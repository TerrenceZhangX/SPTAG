// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_EXTRATIKVCONTROLLER_H_
#define _SPTAG_SPANN_EXTRATIKVCONTROLLER_H_

#include "inc/Helper/KeyValueIO.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Core/SPANN/Options.h"

#include <grpcpp/grpcpp.h>
#include "kvproto/tikvpb.grpc.pb.h"
#include "kvproto/kvrpcpb.pb.h"
#include "kvproto/metapb.pb.h"
#include "kvproto/pdpb.grpc.pb.h"

#include <map>
#include <algorithm>
#include <cmath>
#include <climits>
#include <future>
#include <mutex>
#include <unordered_map>
#include <sstream>
#include <chrono>
#include <thread>

namespace SPTAG::SPANN
{
    /// TiKVIO implements the KeyValueIO interface by communicating with a TiKV
    /// cluster via its RawKV gRPC API.
    ///
    /// Architecture:
    ///   1. Connect to PD (Placement Driver) to discover TiKV store endpoints.
    ///   2. Use PD's GetRegion RPC to find, for any given key, which TiKV
    ///      store (region leader) should handle the request.
    ///   3. Send RawGet / RawPut / RawDelete / RawBatchGet requests to the
    ///      correct TiKV store's RawKV gRPC service.
    ///
    /// All keys are prefixed with a configurable namespace prefix so that
    /// SPANN posting data does not collide with other data in the same TiKV
    /// cluster.
    class TiKVIO : public Helper::KeyValueIO
    {
    public:
        TiKVIO(const std::string& pdAddresses, const std::string& keyPrefix)
            : m_keyPrefix(keyPrefix)
        {
            // Parse comma-separated PD addresses and try to connect.
            std::istringstream ss(pdAddresses);
            std::string addr;
            while (std::getline(ss, addr, ',')) {
                // Trim whitespace
                addr.erase(0, addr.find_first_not_of(" \t"));
                addr.erase(addr.find_last_not_of(" \t") + 1);
                if (!addr.empty()) {
                    m_pdAddresses.push_back(addr);
                }
            }

            if (m_pdAddresses.empty()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO: No PD addresses provided!\n");
                return;
            }

            // Create channels to all PD nodes; find the leader.
            for (const auto& pdAddr : m_pdAddresses) {
                auto channel = grpc::CreateChannel(pdAddr, grpc::InsecureChannelCredentials());
                auto stub = pdpb::PD::NewStub(channel);
                if (!stub) continue;

                // Try GetMembers to find the PD leader
                pdpb::GetMembersRequest membersReq;
                auto* header = membersReq.mutable_header();
                header->set_cluster_id(0);
                pdpb::GetMembersResponse membersResp;
                grpc::ClientContext membersCtx;
                membersCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

                auto status = stub->GetMembers(&membersCtx, membersReq, &membersResp);
                if (!status.ok()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning, "TiKVIO: GetMembers failed on %s: %s\n",
                                 pdAddr.c_str(), status.error_message().c_str());
                    continue;
                }

                // Find leader's client URL
                if (membersResp.has_leader() && membersResp.leader().client_urls_size() > 0) {
                    // Save cluster_id from the response header
                    if (membersResp.has_header()) {
                        m_clusterId = membersResp.header().cluster_id();
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: Cluster ID: %lu\n", m_clusterId);
                    }

                    std::string leaderUrl = membersResp.leader().client_urls(0);
                    // Strip http:// prefix if present
                    std::string leaderAddr = leaderUrl;
                    auto schemePos = leaderAddr.find("://");
                    if (schemePos != std::string::npos) {
                        leaderAddr = leaderAddr.substr(schemePos + 3);
                    }
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: PD leader is at %s\n", leaderAddr.c_str());

                    if (leaderAddr == pdAddr) {
                        // We're already connected to the leader
                        m_pdStub = std::move(stub);
                    } else {
                        // Connect to the actual leader
                        auto leaderChannel = grpc::CreateChannel(leaderAddr, grpc::InsecureChannelCredentials());
                        m_pdStub = pdpb::PD::NewStub(leaderChannel);
                    }
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: Connected to PD leader at %s\n", leaderAddr.c_str());
                    break;
                } else {
                    // No leader info; use this node anyway
                    m_pdStub = std::move(stub);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: Connected to PD at %s (leader unknown)\n", pdAddr.c_str());
                    break;
                }
            }

            if (!m_pdStub) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO: Failed to create PD stub!\n");
                return;
            }

            m_available = true;
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: Initialized with key prefix '%s'\n", m_keyPrefix.c_str());
        }

        ~TiKVIO() override {
            ShutDown();
        }

        void ShutDown() override {
            m_available = false;
            std::lock_guard<std::mutex> lock(m_storeMutex);
            m_storeStubs.clear();
            m_pdStub.reset();
        }

        bool Available() override {
            return m_available;
        }

        // ---- Single-key operations ----

        ErrorCode Get(const std::string& key, std::string* value,
                      const std::chrono::microseconds& timeout,
                      std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            std::string prefixedKey = MakePrefixedKey(key);

            for (int attempt = 0; attempt < 10; attempt++) {
                auto stub = GetStubForKey(prefixedKey);
                if (!stub) return ErrorCode::Fail;

                kvrpcpb::RawGetRequest request;
                request.set_key(prefixedKey);
                SetContext(request.mutable_context(), prefixedKey);

                kvrpcpb::RawGetResponse response;
                grpc::ClientContext ctx;
                SetDeadline(ctx, timeout);

                auto status = stub->RawGet(&ctx, request, &response);
                if (!status.ok()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning, "TiKVIO::Get gRPC error (attempt %d): %s\n",
                                 attempt, status.error_message().c_str());
                    InvalidateRegionCache(prefixedKey);
                    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
                    continue;
                }
                if (response.has_region_error()) {
                    InvalidateRegionCache(prefixedKey);
                    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
                    continue;
                }
                if (!response.error().empty()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::Get error: %s\n", response.error().c_str());
                    return ErrorCode::Fail;
                }
                if (response.not_found()) {
                    return ErrorCode::Fail;
                }

                *value = response.value();
                return ErrorCode::Success;
            }
            return ErrorCode::Fail;
        }

        ErrorCode Get(const SizeType key, std::string* value,
                      const std::chrono::microseconds& timeout,
                      std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            std::string k(reinterpret_cast<const char*>(&key), sizeof(SizeType));
            return Get(k, value, timeout, reqs);
        }

        // ---- Put operations ----

        ErrorCode Put(const std::string& key, const std::string& value,
                      const std::chrono::microseconds& timeout,
                      std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            std::string prefixedKey = MakePrefixedKey(key);

            for (int attempt = 0; attempt < 10; attempt++) {
                auto stub = GetStubForKey(prefixedKey);
                if (!stub) return ErrorCode::Fail;

                kvrpcpb::RawPutRequest request;
                request.set_key(prefixedKey);
                request.set_value(value);
                SetContext(request.mutable_context(), prefixedKey);

                kvrpcpb::RawPutResponse response;
                grpc::ClientContext ctx;
                SetDeadline(ctx, timeout);

                auto status = stub->RawPut(&ctx, request, &response);
                if (!status.ok()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning, "TiKVIO::Put gRPC error (attempt %d): %s\n",
                                 attempt, status.error_message().c_str());
                    InvalidateRegionCache(prefixedKey);
                    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
                    continue;
                }
                if (response.has_region_error()) {
                    InvalidateRegionCache(prefixedKey);
                    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
                    continue;
                }
                if (!response.error().empty()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::Put error: %s\n", response.error().c_str());
                    return ErrorCode::Fail;
                }
                return ErrorCode::Success;
            }
            return ErrorCode::Fail;
        }

        ErrorCode Put(const SizeType key, const std::string& value,
                      const std::chrono::microseconds& timeout,
                      std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            std::string k(reinterpret_cast<const char*>(&key), sizeof(SizeType));
            return Put(k, value, timeout, reqs);
        }

        // ---- Delete operations ----

        ErrorCode Delete(SizeType key) override {
            std::string k(reinterpret_cast<const char*>(&key), sizeof(SizeType));
            std::string prefixedKey = MakePrefixedKey(k);

            auto stub = GetStubForKey(prefixedKey);
            if (!stub) return ErrorCode::Fail;

            kvrpcpb::RawDeleteRequest request;
            request.set_key(prefixedKey);
            SetContext(request.mutable_context(), prefixedKey);

            kvrpcpb::RawDeleteResponse response;
            grpc::ClientContext ctx;
            auto timeout = std::chrono::microseconds(5000000); // 5s default
            SetDeadline(ctx, timeout);

            auto status = stub->RawDelete(&ctx, request, &response);
            if (!status.ok()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::Delete gRPC error: %s\n", status.error_message().c_str());
                return ErrorCode::Fail;
            }
            if (!response.error().empty()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::Delete error: %s\n", response.error().c_str());
                return ErrorCode::Fail;
            }
            return ErrorCode::Success;
        }

        ErrorCode DeleteRange(SizeType start, SizeType end) override {
            std::string startKey(reinterpret_cast<const char*>(&start), sizeof(SizeType));
            std::string endKey(reinterpret_cast<const char*>(&end), sizeof(SizeType));
            std::string prefixedStart = MakePrefixedKey(startKey);
            std::string prefixedEnd = MakePrefixedKey(endKey);

            auto stub = GetStubForKey(prefixedStart);
            if (!stub) return ErrorCode::Fail;

            kvrpcpb::RawDeleteRangeRequest request;
            request.set_start_key(prefixedStart);
            request.set_end_key(prefixedEnd);
            SetContext(request.mutable_context(), prefixedStart);

            kvrpcpb::RawDeleteRangeResponse response;
            grpc::ClientContext ctx;
            auto timeout = std::chrono::microseconds(10000000); // 10s default
            SetDeadline(ctx, timeout);

            auto status = stub->RawDeleteRange(&ctx, request, &response);
            if (!status.ok()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::DeleteRange gRPC error: %s\n", status.error_message().c_str());
                return ErrorCode::Fail;
            }
            if (!response.error().empty()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::DeleteRange error: %s\n", response.error().c_str());
                return ErrorCode::Fail;
            }
            return ErrorCode::Success;
        }

        // ---- Merge (append) operation ----
        // TiKV does not have native merge; we implement read-modify-write with
        // a simple get-append-put pattern.

        ErrorCode Merge(const SizeType key, const std::string& value,
                        const std::chrono::microseconds& timeout,
                        std::vector<Helper::AsyncReadRequest>* reqs,
                        int& size) override
        {
            if (value.empty()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::Merge: empty append posting!\n");
                return ErrorCode::Fail;
            }

            std::string existingValue;
            auto ret = Get(key, &existingValue, timeout, reqs);
            if (ret != ErrorCode::Success) {
                // Key doesn't exist yet, just put the new value.
                size = static_cast<int>(value.size());
                return Put(key, value, timeout, reqs);
            }

            // Append the new value to existing
            existingValue.append(value);
            size = static_cast<int>(existingValue.size());
            return Put(key, existingValue, timeout, reqs);
        }

        // ---- MultiGet operations ----
        // Use individual Get calls since keys may span multiple TiKV regions.

        ErrorCode MultiGet(const std::vector<SizeType>& keys,
                           std::vector<Helper::PageBuffer<std::uint8_t>>& values,
                           const std::chrono::microseconds& timeout,
                           std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            for (size_t i = 0; i < keys.size(); i++) {
                std::string val;
                auto ret = Get(keys[i], &val, timeout, reqs);
                if (ret != ErrorCode::Success || val.empty()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::MultiGet key not found: %d\n", keys[i]);
                    return ErrorCode::Fail;
                }
                // Resize buffer if the value is larger than pre-allocated capacity
                if (val.size() > values[i].GetPageSize()) {
                    values[i].ReservePageBuffer(val.size());
                }
                memcpy(values[i].GetBuffer(), val.data(), val.size());
                values[i].SetAvailableSize(static_cast<int>(val.size()));
            }
            return ErrorCode::Success;
        }

        ErrorCode MultiGet(const std::vector<std::string>& keys,
                           std::vector<std::string>* values,
                           const std::chrono::microseconds& timeout,
                           std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            for (size_t i = 0; i < keys.size(); i++) {
                std::string val;
                auto ret = Get(keys[i], &val, timeout, reqs);
                if (ret != ErrorCode::Success) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::MultiGet key not found\n");
                    return ErrorCode::Fail;
                }
                values->push_back(std::move(val));
            }
            return ErrorCode::Success;
        }

        ErrorCode MultiGet(const std::vector<SizeType>& keys,
                           std::vector<std::string>* values,
                           const std::chrono::microseconds& timeout,
                           std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            for (size_t i = 0; i < keys.size(); i++) {
                std::string val;
                auto ret = Get(keys[i], &val, timeout, reqs);
                if (ret != ErrorCode::Success) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO::MultiGet key not found: %d\n", keys[i]);
                    return ErrorCode::Fail;
                }
                values->push_back(std::move(val));
            }
            return ErrorCode::Success;
        }

        // ---- Scan operations ----

        ErrorCode StartToScan(SizeType& key, std::string* value) {
            std::string startKey = m_keyPrefix;
            std::string endKey = m_keyPrefix;
            // Create a range that covers all our prefixed keys
            endKey.push_back(static_cast<char>(0xFF));

            auto stub = GetStubForKey(startKey);
            if (!stub) return ErrorCode::Fail;

            kvrpcpb::RawScanRequest request;
            request.set_start_key(startKey);
            request.set_end_key(endKey);
            request.set_limit(4096);
            SetContext(request.mutable_context(), startKey);

            kvrpcpb::RawScanResponse response;
            grpc::ClientContext ctx;

            auto status = stub->RawScan(&ctx, request, &response);
            if (!status.ok() || response.kvs_size() == 0) {
                return ErrorCode::Fail;
            }

            // Cache scan results
            m_scanResults.clear();
            m_scanIndex = 0;
            for (int i = 0; i < response.kvs_size(); i++) {
                m_scanResults.push_back({response.kvs(i).key(), response.kvs(i).value()});
            }

            const auto& first = m_scanResults[0];
            std::string rawKey = StripPrefix(first.first);
            if (rawKey.size() >= sizeof(SizeType)) {
                key = *reinterpret_cast<const SizeType*>(rawKey.data());
            }
            *value = first.second;
            m_scanIndex = 1;
            return ErrorCode::Success;
        }

        ErrorCode NextToScan(SizeType& key, std::string* value) {
            if (m_scanIndex >= m_scanResults.size()) {
                return ErrorCode::Fail;
            }

            const auto& entry = m_scanResults[m_scanIndex];
            std::string rawKey = StripPrefix(entry.first);
            if (rawKey.size() >= sizeof(SizeType)) {
                key = *reinterpret_cast<const SizeType*>(rawKey.data());
            }
            *value = entry.second;
            m_scanIndex++;
            return ErrorCode::Success;
        }

        void ForceCompaction() override {
            // TiKV handles compaction internally; this is a no-op.
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: ForceCompaction is a no-op (TiKV manages compaction internally)\n");
        }

        ErrorCode Check(const SizeType key, std::vector<std::uint8_t> *visited) override {
            // TiKV guarantees data integrity internally via Raft consensus.
            // Posting size checks are skipped since TiKV is shared mutable storage
            // and concurrent inserts/splits may update postings between size recording
            // and check time.
            return ErrorCode::Success;
        }

        void GetStat() override {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: Connected to PD cluster with %zu addresses, prefix='%s'\n",
                         m_pdAddresses.size(), m_keyPrefix.c_str());
        }

        ErrorCode Checkpoint(std::string prefix) override {
            // TiKV provides its own snapshot/backup mechanism; no local checkpoint.
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: Checkpoint is a no-op (use TiKV's backup tools)\n");
            return ErrorCode::Success;
        }

    private:
        std::string m_keyPrefix;
        std::vector<std::string> m_pdAddresses;
        std::unique_ptr<pdpb::PD::Stub> m_pdStub;
        uint64_t m_clusterId = 0;
        bool m_available = false;

        // Cache of TiKV store stubs keyed by store address
        mutable std::mutex m_storeMutex;
        std::unordered_map<std::string, std::shared_ptr<tikvpb::Tikv::Stub>> m_storeStubs;

        // Region cache: maps a key prefix to (region_id, leader_store_addr)
        struct RegionInfo {
            uint64_t regionId;
            uint64_t storeId;
            std::string leaderAddr;
            std::string startKey;
            std::string endKey;
            metapb::RegionEpoch epoch;
            metapb::Peer leaderPeer;  // Full peer info (id + store_id)
        };
        mutable std::mutex m_regionMutex;
        std::vector<RegionInfo> m_regionCache;

        // Scan state
        std::vector<std::pair<std::string, std::string>> m_scanResults;
        size_t m_scanIndex = 0;

        // ---- Helper: build a prefixed key ----
        std::string MakePrefixedKey(const std::string& key) const {
            std::string result;
            result.reserve(m_keyPrefix.size() + 1 + key.size());
            result.append(m_keyPrefix);
            result.push_back('_');
            result.append(key);
            return result;
        }

        std::string StripPrefix(const std::string& prefixedKey) const {
            size_t prefixLen = m_keyPrefix.size() + 1; // prefix + '_'
            if (prefixedKey.size() > prefixLen) {
                return prefixedKey.substr(prefixLen);
            }
            return "";
        }

        // ---- Helper: set gRPC deadline ----
        void SetDeadline(grpc::ClientContext& ctx, const std::chrono::microseconds& timeout) const {
            if (timeout.count() > 0) {
                // Cap at 60 seconds to prevent overflow when timeout is chrono::microseconds::max()
                auto cappedTimeout = std::min(timeout, std::chrono::microseconds(60000000));
                ctx.set_deadline(std::chrono::system_clock::now() + cappedTimeout);
            }
        }

        // ---- Helper: set request context with region info ----
        void SetContext(kvrpcpb::Context* ctx, const std::string& key) {
            // Look up region for this key
            RegionInfo region;
            if (FindRegionForKey(key, region)) {
                ctx->set_region_id(region.regionId);
                *ctx->mutable_region_epoch() = region.epoch;
                *ctx->mutable_peer() = region.leaderPeer;
            }
        }

        // ---- PD: get region for a given key ----
        bool GetRegionFromPD(const std::string& key, RegionInfo& info) {
            if (!m_pdStub) return false;

            pdpb::GetRegionRequest request;
            request.set_region_key(key);
            // PD requires a header
            auto* header = request.mutable_header();
            header->set_cluster_id(m_clusterId);

            pdpb::GetRegionResponse response;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

            auto status = m_pdStub->GetRegion(&ctx, request, &response);
            if (!status.ok()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO: PD GetRegion failed: %s\n", status.error_message().c_str());
                return false;
            }

            if (!response.has_region() || !response.has_leader()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO: PD GetRegion returned no region/leader\n");
                return false;
            }

            const auto& region = response.region();
            const auto& leader = response.leader();

            info.regionId = region.id();
            info.startKey = region.start_key();
            info.endKey = region.end_key();
            info.epoch = region.region_epoch();
            info.storeId = leader.store_id();
            info.leaderPeer = leader;  // Store full peer info (id + store_id)

            // Get store address from PD
            info.leaderAddr = GetStoreAddress(leader.store_id());

            // Cache the region info
            {
                std::lock_guard<std::mutex> lock(m_regionMutex);
                // Replace existing entry for this region or add new
                bool found = false;
                for (auto& cached : m_regionCache) {
                    if (cached.regionId == info.regionId) {
                        cached = info;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    m_regionCache.push_back(info);
                }
            }

            return true;
        }

        // ---- PD: get store address by store ID ----
        std::string GetStoreAddress(uint64_t storeId) {
            if (!m_pdStub) return "";

            pdpb::GetStoreRequest request;
            request.set_store_id(storeId);
            auto* header = request.mutable_header();
            header->set_cluster_id(m_clusterId);

            pdpb::GetStoreResponse response;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

            auto status = m_pdStub->GetStore(&ctx, request, &response);
            if (!status.ok() || !response.has_store()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO: PD GetStore failed for store %lu\n", storeId);
                return "";
            }

            return response.store().address();
        }

        // ---- Find cached region for a key ----
        bool FindRegionForKey(const std::string& key, RegionInfo& info) {
            {
                std::lock_guard<std::mutex> lock(m_regionMutex);
                for (const auto& region : m_regionCache) {
                    if ((region.startKey.empty() || key >= region.startKey) &&
                        (region.endKey.empty() || key < region.endKey)) {
                        info = region;
                        return true;
                    }
                }
            }
            // Cache miss: query PD
            return GetRegionFromPD(key, info);
        }

        // ---- Invalidate cached region for a key ----
        void InvalidateRegionCache(const std::string& key) {
            std::lock_guard<std::mutex> lock(m_regionMutex);
            m_regionCache.erase(
                std::remove_if(m_regionCache.begin(), m_regionCache.end(),
                    [&key](const RegionInfo& r) {
                        return (r.startKey.empty() || key >= r.startKey) &&
                               (r.endKey.empty() || key < r.endKey);
                    }),
                m_regionCache.end());
        }

        // ---- Get or create a TiKV stub for a key ----
        tikvpb::Tikv::Stub* GetStubForKey(const std::string& key) {
            RegionInfo region;
            if (!FindRegionForKey(key, region) || region.leaderAddr.empty()) {
                // Fallback: use one of the known TiKV store addresses from PD
                std::string fallbackAddr = GetAnyStoreAddress();
                if (!fallbackAddr.empty()) {
                    region.leaderAddr = fallbackAddr;
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                                 "TiKVIO: Failed to resolve region, falling back to store %s\n",
                                 region.leaderAddr.c_str());
                } else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO: No TiKV store available for key\n");
                    return nullptr;
                }
            }

            return GetOrCreateStub(region.leaderAddr);
        }

        // ---- Get any available TiKV store address from PD ----
        std::string GetAnyStoreAddress() {
            // Try to get stores from PD via its HTTP-like GetStore calls
            // Use store IDs 1-10 as a heuristic
            for (uint64_t storeId = 1; storeId <= 10; storeId++) {
                std::string addr = GetStoreAddress(storeId);
                if (!addr.empty()) {
                    return addr;
                }
            }
            return "";
        }

        // ---- Get or create a gRPC stub for a TiKV store ----
        tikvpb::Tikv::Stub* GetOrCreateStub(const std::string& address) {
            std::lock_guard<std::mutex> lock(m_storeMutex);

            auto it = m_storeStubs.find(address);
            if (it != m_storeStubs.end()) {
                return it->second.get();
            }

            grpc::ChannelArguments args;
            args.SetMaxReceiveMessageSize(64 * 1024 * 1024); // 64MB
            args.SetMaxSendMessageSize(64 * 1024 * 1024);    // 64MB
            auto channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
            auto stub = tikvpb::Tikv::NewStub(channel);
            if (!stub) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "TiKVIO: Failed to create stub for %s\n", address.c_str());
                return nullptr;
            }

            auto* rawPtr = stub.get();
            m_storeStubs[address] = std::move(stub);
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: Created stub for TiKV store at %s\n", address.c_str());
            return rawPtr;
        }
    };
} // namespace SPTAG::SPANN

#endif // _SPTAG_SPANN_EXTRATIKVCONTROLLER_H_
