// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_TIKV_VERSIONMAP_H_
#define _SPTAG_COMMON_TIKV_VERSIONMAP_H_

#include "IVersionMap.h"
#include "inc/Helper/KeyValueIO.h"
#include <atomic>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>
#include <cstring>
#include <chrono>
#include <algorithm>
#include <list>

namespace SPTAG
{
    namespace COMMON
    {
        /// TiKVVersionMap stores per-VID version bytes in TiKV as chunks.
        ///
        /// TiKV key schema:
        ///   "vc:{layer}"         → uint64 (vector count)
        ///   "v:{layer}:{chunkId}" → uint8_t[chunkSize]
        ///
        /// Each chunk holds chunkSize VIDs' version bytes.
        /// chunk_id = VID / chunkSize, offset = VID % chunkSize.
        class TiKVVersionMap : public IVersionMap
        {
        private:
            std::shared_ptr<Helper::KeyValueIO> m_db;
            int m_layer;
            int m_chunkSize;
            std::atomic<SizeType> m_count{0};
            std::atomic<SizeType> m_deleted{0};

            // Striped mutexes for per-chunk write serialization
            static constexpr int kWriteStripes = 64;
            mutable std::mutex m_chunkWriteMutex[kWriteStripes];
            std::mutex& ChunkMutex(SizeType chunkId) const { return m_chunkWriteMutex[chunkId % kWriteStripes]; }

            // LRU chunk cache: list front = most recently used
            struct CachedChunk {
                SizeType chunkId;
                std::string data;
                std::chrono::steady_clock::time_point fetchTime;
            };
            using LruList = std::list<CachedChunk>;
            mutable std::shared_mutex m_cacheMutex;
            mutable LruList m_lruList;
            mutable std::unordered_map<SizeType, LruList::iterator> m_cacheMap;
            int m_cacheTTLMs{0};        // TTL in ms, 0 = cache disabled
            int m_cacheMaxChunks{10000}; // max cached chunks (~40MB at 4096 chunk size)

            // Insert or update a chunk in the LRU cache. Caller must hold exclusive m_cacheMutex.
            void CachePut(SizeType chunkId, const std::string& data, std::chrono::steady_clock::time_point now) const
            {
                auto it = m_cacheMap.find(chunkId);
                if (it != m_cacheMap.end()) {
                    // Update existing: move to front
                    it->second->data = data;
                    it->second->fetchTime = now;
                    m_lruList.splice(m_lruList.begin(), m_lruList, it->second);
                } else {
                    // Evict LRU entries if at capacity
                    while ((int)m_cacheMap.size() >= m_cacheMaxChunks) {
                        auto& back = m_lruList.back();
                        m_cacheMap.erase(back.chunkId);
                        m_lruList.pop_back();
                    }
                    m_lruList.push_front({chunkId, data, now});
                    m_cacheMap[chunkId] = m_lruList.begin();
                }
            }

            static constexpr auto MaxTimeout = std::chrono::microseconds(60000000); // 60s

            std::string CountKey() const
            {
                return "vc:" + std::to_string(m_layer);
            }

            std::string ChunkKey(SizeType chunkId) const
            {
                return "v:" + std::to_string(m_layer) + ":" + std::to_string(chunkId);
            }

            SizeType ChunkId(SizeType vid) const { return vid / m_chunkSize; }
            int ChunkOffset(SizeType vid) const { return vid % m_chunkSize; }

            // Read a single chunk from TiKV. Returns empty string on miss.
            std::string ReadChunk(SizeType chunkId) const
            {
                std::string value;
                auto ret = m_db->Get(ChunkKey(chunkId), &value, MaxTimeout, nullptr);
                if (ret != ErrorCode::Success || value.empty()) {
                    return std::string();
                }
                return value;
            }

            // Write a chunk to TiKV and update LRU cache.
            ErrorCode WriteChunk(SizeType chunkId, const std::string& data)
            {
                auto ret = m_db->Put(ChunkKey(chunkId), data, MaxTimeout, nullptr);
                if (ret == ErrorCode::Success) {
                    std::unique_lock<std::shared_mutex> lock(m_cacheMutex);
                    CachePut(chunkId, data, std::chrono::steady_clock::now());
                }
                return ret;
            }

            // Read a chunk with pure LRU cache.
            // Uses shared_lock for cache hits (no LRU reorder) to allow concurrent reads.
            // Only takes exclusive lock on cache miss for insertion.
            std::string ReadChunkCached(SizeType chunkId) const
            {
                // Try cache with shared lock — concurrent reads OK
                {
                    std::shared_lock<std::shared_mutex> lock(m_cacheMutex);
                    auto it = m_cacheMap.find(chunkId);
                    if (it != m_cacheMap.end()) {
                        return it->second->data;
                    }
                }
                // Cache miss — fetch from TiKV, then exclusive lock to insert
                std::string data = ReadChunk(chunkId);
                if (!data.empty()) {
                    std::unique_lock<std::shared_mutex> lock(m_cacheMutex);
                    CachePut(chunkId, data, std::chrono::steady_clock::now());
                }
                return data;
            }

            // Read a single byte for a VID using cache. Returns 0xfe on error/miss.
            uint8_t ReadVersionByte(SizeType vid) const
            {
                SizeType cid = ChunkId(vid);
                std::string chunk = ReadChunkCached(cid);
                if (chunk.empty() || (int)chunk.size() <= ChunkOffset(vid)) {
                    return 0xfe;
                }
                return static_cast<uint8_t>(chunk[ChunkOffset(vid)]);
            }

            // Read-modify-write a single byte. Returns the old value.
            // Thread-safe: locks the chunk stripe to prevent concurrent overwrites.
            uint8_t WriteVersionByte(SizeType vid, uint8_t newVal)
            {
                SizeType cid = ChunkId(vid);
                int offset = ChunkOffset(vid);
                std::lock_guard<std::mutex> lock(ChunkMutex(cid));
                std::string chunk = ReadChunk(cid);
                if (chunk.empty()) {
                    // Create new chunk, uninitialized (matching VersionLabel's 0xff)
                    chunk.assign(m_chunkSize, static_cast<char>(0xff));
                }
                uint8_t oldVal = static_cast<uint8_t>(chunk[offset]);
                chunk[offset] = static_cast<char>(newVal);
                WriteChunk(cid, chunk);
                return oldVal;
            }

            void SaveCount()
            {
                SizeType count = m_count.load();
                std::string val(reinterpret_cast<const char*>(&count), sizeof(SizeType));
                m_db->Put(CountKey(), val, MaxTimeout, nullptr);
            }

        public:
            TiKVVersionMap() : m_layer(0), m_chunkSize(4096) {}

            void SetDB(std::shared_ptr<Helper::KeyValueIO> db) { m_db = db; }
            void SetLayer(int layer) { m_layer = layer; }
            void SetChunkSize(int chunkSize) { m_chunkSize = chunkSize; }
            void SetCacheTTL(int ttlMs) { m_cacheTTLMs = ttlMs; }
            void SetCacheMaxChunks(int maxChunks) { m_cacheMaxChunks = maxChunks; }

            std::shared_ptr<Helper::KeyValueIO> GetDB() const { return m_db; }

            void Initialize(SizeType size, SizeType blockSize, SizeType capacity, COMMON::Dataset<SizeType>* globalIDs = nullptr) override
            {
                m_count = size;

                SizeType totalChunks = (size + m_chunkSize - 1) / m_chunkSize;

                if (m_layer > 0 && globalIDs != nullptr && globalIDs->R() > 0) {
                    // Non-leaf layer: only globalIDs are alive, rest deleted
                    std::string defaultChunk(m_chunkSize, static_cast<char>(0xfe));
                    for (SizeType c = 0; c < totalChunks; c++) {
                        WriteChunk(c, defaultChunk);
                    }
                    m_deleted = size;

                    // Mark vectors in globalIDs as version 0 (not deleted)
                    std::unordered_map<SizeType, std::string> dirtyChunks;
                    for (SizeType i = 0; i < globalIDs->R(); i++) {
                        SizeType globalID = *(globalIDs->At(i));
                        SizeType cid = ChunkId(globalID);
                        if (dirtyChunks.find(cid) == dirtyChunks.end()) {
                            dirtyChunks[cid] = ReadChunk(cid);
                            if (dirtyChunks[cid].empty()) {
                                dirtyChunks[cid].assign(m_chunkSize, static_cast<char>(0xfe));
                            }
                        }
                        uint8_t oldVal = static_cast<uint8_t>(dirtyChunks[cid][ChunkOffset(globalID)]);
                        dirtyChunks[cid][ChunkOffset(globalID)] = 0x00;
                        if (oldVal == 0xfe) m_deleted--;
                    }
                    for (auto& [cid, data] : dirtyChunks) {
                        WriteChunk(cid, data);
                    }
                } else {
                    // Leaf layer (layer 0) or no globalIDs: all VIDs start alive (version 0)
                    std::string aliveChunk(m_chunkSize, static_cast<char>(0x00));
                    for (SizeType c = 0; c < totalChunks; c++) {
                        WriteChunk(c, aliveChunk);
                    }
                    m_deleted = 0;
                }

                SaveCount();
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVVersionMap::Initialize: layer=%d, size=%d, totalChunks=%d, deleted=%d, globalIDs=%d\n",
                    m_layer, size, totalChunks, m_deleted.load(), (globalIDs && globalIDs->R() > 0) ? globalIDs->R() : 0);
            }

            void DeleteAll() override
            {
                SizeType totalChunks = (m_count.load() + m_chunkSize - 1) / m_chunkSize;
                std::string deletedChunk(m_chunkSize, static_cast<char>(0xfe));
                for (SizeType c = 0; c < totalChunks; c++) {
                    WriteChunk(c, deletedChunk);
                }
                m_deleted = m_count.load();
            }

            SizeType Count() const override { return m_count.load(); }
            SizeType GetDeleteCount() const override { return m_deleted.load(); }
            SizeType GetVectorNum() override { return m_count.load(); }
            std::uint64_t BufferSize() const override { return static_cast<std::uint64_t>(m_count.load()) + sizeof(SizeType); }

            bool Deleted(const SizeType& key) const override
            {
                if (key < 0 || key >= m_count.load()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVVersionMap::Deleted: invalid key %d (max %d)\n", key, m_count.load());
                    return true;
                }
                return ReadVersionByte(key) == 0xfe;
            }

            bool Delete(const SizeType& key) override
            {
                if (key < 0 || key >= m_count.load()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVVersionMap::Delete: invalid key %d (max %d)\n", key, m_count.load());
                    return false;
                }
                uint8_t oldVal = WriteVersionByte(key, 0xfe);
                if (oldVal == 0xfe) {
                    if (key < 10) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Warning, "TiKVVersionMap::Delete: key %d already deleted (layer=%d, chunk=%d, offset=%d)\n",
                                     key, m_layer, ChunkId(key), ChunkOffset(key));
                    }
                    return false;
                }
                m_deleted++;
                return true;
            }

            uint8_t GetVersion(const SizeType& key) override
            {
                if (key < 0 || key >= m_count.load()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVVersionMap::GetVersion: invalid key %d (max %d)\n", key, m_count.load());
                    return 0xfe;
                }
                return ReadVersionByte(key);
            }

            void SetVersion(const SizeType& key, const uint8_t& version) override
            {
                if (key < 0 || key >= m_count.load()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVVersionMap::SetVersion: invalid key %d (max %d)\n", key, m_count.load());
                    return;
                }
                uint8_t oldVal = WriteVersionByte(key, version);
                if (oldVal == 0xfe && version != 0xfe) m_deleted--;
                else if (oldVal != 0xfe && version == 0xfe) m_deleted++;
            }

            bool IncVersion(const SizeType& key, uint8_t* newVersion, uint8_t expectedOld = 0xff) override
            {
                if (key < 0 || key >= m_count.load()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVVersionMap::IncVersion: invalid key %d (max %d)\n", key, m_count.load());
                    return false;
                }

                const int MAX_RETRIES = 3;
                for (int retry = 0; retry < MAX_RETRIES; retry++) {
                    SizeType cid = ChunkId(key);
                    int offset = ChunkOffset(key);
                    std::lock_guard<std::mutex> lock(ChunkMutex(cid));
                    std::string chunk = ReadChunk(cid);
                    if (chunk.empty()) return false;

                    uint8_t current = static_cast<uint8_t>(chunk[offset]);
                    if (current == 0xfe) return false; // deleted

                    uint8_t target;
                    if (expectedOld != 0xff) {
                        target = (expectedOld + 1) & 0x7f;
                        // If already at target, another node did the same increment → success
                        if (current == target) {
                            *newVersion = target;
                            return true;
                        }
                        // If not at expected old, unexpected state → conflict
                        if (current != expectedOld) {
                            return false;
                        }
                    } else {
                        target = (current + 1) & 0x7f;
                    }

                    chunk[offset] = static_cast<char>(target);
                    // TODO: Replace with RawCompareAndSwap when available in kvproto
                    // for true atomic CAS across nodes. For now, best-effort write.
                    ErrorCode ret = WriteChunk(cid, chunk);
                    if (ret == ErrorCode::Success) {
                        *newVersion = target;
                        return true;
                    }
                    // Write failed, retry
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning, "TiKVVersionMap::IncVersion: write failed for key %d, retry %d\n", key, retry);
                }
                return false;
            }

            ErrorCode AddBatch(SizeType num) override
            {
                SizeType oldCount = m_count.load();
                SizeType newCount = oldCount + num;

                // Create any new chunks needed (init to 0xff = uninitialized, matching VersionLabel)
                SizeType oldLastChunk = (oldCount > 0) ? ChunkId(oldCount - 1) : -1;
                SizeType newLastChunk = ChunkId(newCount - 1);

                for (SizeType c = oldLastChunk + 1; c <= newLastChunk; c++) {
                    std::string newChunk(m_chunkSize, static_cast<char>(0xff));
                    WriteChunk(c, newChunk);
                }

                m_count = newCount;
                SaveCount();
                return ErrorCode::Success;
            }

            void SetR(SizeType num) override
            {
                m_count = num;
                SaveCount();
            }

            // Save/Load: For TiKV mode, data is already persisted in TiKV.
            // These are no-ops for TiKV mode.
            ErrorCode Save(std::shared_ptr<Helper::DiskIO> output) override
            {
                SaveCount();
                return ErrorCode::Success;
            }

            ErrorCode Save(const std::string& filename) override
            {
                SaveCount();
                return ErrorCode::Success;
            }

            ErrorCode Load(std::shared_ptr<Helper::DiskIO> input, SizeType blockSize, SizeType capacity) override
            {
                // Load count from TiKV
                return LoadCountFromTiKV();
            }

            ErrorCode Load(const std::string& filename, SizeType blockSize, SizeType capacity) override
            {
                return LoadCountFromTiKV();
            }

            ErrorCode Load(char* pmemoryFile, SizeType blockSize, SizeType capacity) override
            {
                return LoadCountFromTiKV();
            }

            /// Batch version lookup with local cache support.
            /// Checks cache first, only fetches misses from TiKV via BatchGet.
            void BatchGetVersions(const std::vector<SizeType>& vids, std::vector<uint8_t>& versions) override
            {
                versions.resize(vids.size());
                if (vids.empty()) return;

                SizeType count = m_count.load();
                auto now = std::chrono::steady_clock::now();
                bool cacheEnabled = (m_cacheTTLMs > 0);

                // Group VIDs by chunk
                std::unordered_map<SizeType, std::vector<size_t>> chunkToIndices;
                for (size_t i = 0; i < vids.size(); i++) {
                    if (vids[i] < 0 || vids[i] >= count) {
                        versions[i] = 0xfe;
                    } else {
                        chunkToIndices[ChunkId(vids[i])].push_back(i);
                    }
                }

                // Phase 1: Resolve from cache (shared lock, no LRU reorder), collect misses
                // Copy data out to avoid dangling pointers after lock release.
                std::unordered_map<SizeType, std::string> resolvedChunks;
                std::vector<SizeType> missChunkIds;

                if (cacheEnabled) {
                    std::shared_lock<std::shared_mutex> lock(m_cacheMutex);
                    for (auto& [cid, indices] : chunkToIndices) {
                        auto it = m_cacheMap.find(cid);
                        if (it != m_cacheMap.end()) {
                            auto ageMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second->fetchTime).count();
                            if (ageMs < m_cacheTTLMs) {
                                resolvedChunks[cid] = it->second->data; // copy
                                continue;
                            }
                        }
                        missChunkIds.push_back(cid);
                    }
                } else {
                    for (auto& [cid, indices] : chunkToIndices) {
                        missChunkIds.push_back(cid);
                    }
                }

                // Phase 2: BatchGet cache misses from TiKV
                std::vector<std::string> fetchedValues;
                std::unordered_map<SizeType, std::string> fetchedChunks;
                if (!missChunkIds.empty()) {
                    std::vector<std::string> keys;
                    keys.reserve(missChunkIds.size());
                    for (SizeType cid : missChunkIds) {
                        keys.push_back(ChunkKey(cid));
                    }
                    m_db->MultiGet(keys, &fetchedValues, MaxTimeout, nullptr);

                    for (size_t i = 0; i < missChunkIds.size(); i++) {
                        if (i < fetchedValues.size() && !fetchedValues[i].empty()) {
                            fetchedChunks[missChunkIds[i]] = std::move(fetchedValues[i]);
                        }
                    }

                    // Update LRU cache with fetched chunks (exclusive lock)
                    if (cacheEnabled && !fetchedChunks.empty()) {
                        std::unique_lock<std::shared_mutex> lock(m_cacheMutex);
                        for (auto& [cid, data] : fetchedChunks) {
                            CachePut(cid, data, now);
                        }
                    }
                }

                // Phase 3: Resolve all VIDs
                for (auto& [cid, indices] : chunkToIndices) {
                    const std::string* chunkData = nullptr;

                    // Check resolved (copied from cache)
                    auto rit = resolvedChunks.find(cid);
                    if (rit != resolvedChunks.end()) {
                        chunkData = &rit->second;
                    } else {
                        // Check fetched
                        auto fit = fetchedChunks.find(cid);
                        if (fit != fetchedChunks.end()) {
                            chunkData = &fit->second;
                        }
                    }

                    for (size_t idx : indices) {
                        if (chunkData == nullptr) {
                            versions[idx] = 0xfe;
                        } else {
                            int offset = ChunkOffset(vids[idx]);
                            if (offset < (int)chunkData->size()) {
                                versions[idx] = static_cast<uint8_t>((*chunkData)[offset]);
                            } else {
                                versions[idx] = 0xfe;
                            }
                        }
                    }
                }
            }

        private:
            ErrorCode LoadCountFromTiKV()
            {
                std::string val;
                auto ret = m_db->Get(CountKey(), &val, MaxTimeout, nullptr);
                if (ret == ErrorCode::Success && val.size() >= sizeof(SizeType)) {
                    m_count = *reinterpret_cast<const SizeType*>(val.data());
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVVersionMap: Loaded count=%d from TiKV\n", m_count.load());
                } else {
                    m_count = 0;
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning, "TiKVVersionMap: No count found in TiKV, starting at 0\n");
                }
                // Scan all chunks to compute accurate delete count
                SizeType count = m_count.load();
                SizeType deleted = 0;
                if (count > 0) {
                    SizeType totalChunks = (count + m_chunkSize - 1) / m_chunkSize;
                    for (SizeType c = 0; c < totalChunks; c++) {
                        std::string chunk = ReadChunk(c);
                        if (chunk.empty()) {
                            // Missing chunk — treat all entries as deleted
                            SizeType chunkEntries = (c == totalChunks - 1) ? (count - c * m_chunkSize) : m_chunkSize;
                            deleted += chunkEntries;
                            continue;
                        }
                        SizeType chunkEntries = (c == totalChunks - 1) ? (count - c * m_chunkSize) : m_chunkSize;
                        for (SizeType i = 0; i < chunkEntries; i++) {
                            if (static_cast<uint8_t>(chunk[i]) == 0xfe) deleted++;
                        }
                    }
                }
                m_deleted = deleted;
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVVersionMap: Scanned %d chunks, deleted=%d/%d\n",
                             count > 0 ? (count + m_chunkSize - 1) / m_chunkSize : 0, deleted, count);
                return ErrorCode::Success;
            }
        };
    }
}

#endif // _SPTAG_COMMON_TIKV_VERSIONMAP_H_
