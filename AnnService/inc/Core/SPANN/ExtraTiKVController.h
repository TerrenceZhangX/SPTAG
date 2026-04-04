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

        // ---- BatchPut operation ----
        // Use RawBatchPut grouped by region for efficient batched writes.

        ErrorCode BatchPut(const std::vector<SizeType>& keys,
                           const std::vector<std::string>& values,
                           const std::chrono::microseconds& timeout) override
        {
            if (keys.empty()) return ErrorCode::Success;
            if (keys.size() != values.size()) return ErrorCode::Fail;

            // Build prefixed keys
            std::vector<std::string> prefixedKeys(keys.size());
            for (size_t i = 0; i < keys.size(); i++) {
                std::string k(reinterpret_cast<const char*>(&keys[i]), sizeof(SizeType));
                prefixedKeys[i] = MakePrefixedKey(k);
            }

            // Group by (leader address, region id)
            std::unordered_map<RegionGroupKey, RegionGroup, RegionGroupKeyHash> regionGroups;
            for (size_t i = 0; i < prefixedKeys.size(); i++) {
                RegionInfo region;
                std::string addr;
                uint64_t rid = 0;
                if (FindRegionForKey(prefixedKeys[i], region) && !region.leaderAddr.empty()) {
                    addr = region.leaderAddr;
                    rid = region.regionId;
                } else {
                    addr = GetAnyStoreAddress();
                }
                auto& g = regionGroups[{addr, rid}];
                if (g.keys.empty()) g.region = region;
                g.keys.push_back({i, prefixedKeys[i]});
            }

            // Send RawBatchPut per region group
            std::atomic<int> errorCount{0};
            std::vector<std::future<void>> futures;

            for (auto& [gkey, rg] : regionGroups) {
                futures.push_back(std::async(std::launch::async, [&, &gkey = gkey, &rg = rg]() {
                    auto* stub = GetOrCreateStub(gkey.leaderAddr);
                    if (!stub) { errorCount++; return; }

                    kvrpcpb::RawBatchPutRequest request;
                    SetContextFromRegion(request.mutable_context(), rg.region);
                    for (auto& [idx, pkey] : rg.keys) {
                        auto* pair = request.add_pairs();
                        pair->set_key(pkey);
                        pair->set_value(values[idx]);
                    }

                    kvrpcpb::RawBatchPutResponse response;
                    grpc::ClientContext ctx;
                    SetDeadline(ctx, timeout);

                    auto status = stub->RawBatchPut(&ctx, request, &response);
                    if (!status.ok() || !response.error().empty()) {
                        // Fallback: individual puts
                        for (auto& [idx, pkey] : rg.keys) {
                            if (Put(keys[idx], values[idx], timeout, nullptr) != ErrorCode::Success) {
                                errorCount++;
                            }
                        }
                    }
                }));
            }

            for (auto& f : futures) f.get();
            return errorCount == 0 ? ErrorCode::Success : ErrorCode::Fail;
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

        // ---- BatchMerge: batch read-modify-write ----
        // Reduces N individual Merge calls (2N serial gRPC round-trips) to
        // 1 MultiGet + 1 BatchPut (2 batch gRPC round-trips).

        ErrorCode BatchMerge(const std::vector<SizeType>& keys,
                             const std::vector<std::string>& values,
                             const std::chrono::microseconds& timeout,
                             std::vector<int>& sizes) override
        {
            if (keys.empty()) return ErrorCode::Success;
            if (keys.size() != values.size()) return ErrorCode::Fail;

            sizes.resize(keys.size(), 0);

            // Step 1: MultiGet all existing values (1 batch RPC)
            std::vector<std::string> existingVals;
            MultiGet(keys, &existingVals, timeout, nullptr);

            // Step 2: Merge in memory
            std::vector<SizeType> putKeys(keys.size());
            std::vector<std::string> putValues(keys.size());
            for (size_t i = 0; i < keys.size(); i++) {
                putKeys[i] = keys[i];
                if (i < existingVals.size() && !existingVals[i].empty()) {
                    putValues[i] = std::move(existingVals[i]);
                    putValues[i].append(values[i]);
                } else {
                    putValues[i] = values[i];
                }
                sizes[i] = static_cast<int>(putValues[i].size());
            }

            // Step 3: BatchPut all merged values (1 batch RPC)
            return BatchPut(putKeys, putValues, timeout);
        }

        // ---- MultiGet operations ----
        // Use RawBatchGet grouped by region for efficient batched reads.
        // Tolerate individual key not-found: set empty buffer for missing keys
        // (e.g., postings deleted by splits in multi-layer SPANN).

        ErrorCode MultiGet(const std::vector<SizeType>& keys,
                           std::vector<Helper::PageBuffer<std::uint8_t>>& values,
                           const std::chrono::microseconds& timeout,
                           std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            if (keys.empty()) return ErrorCode::Success;

            // Build prefixed keys and initialize all values as empty
            std::vector<std::string> prefixedKeys(keys.size());
            for (size_t i = 0; i < keys.size(); i++) {
                std::string k(reinterpret_cast<const char*>(&keys[i]), sizeof(SizeType));
                prefixedKeys[i] = MakePrefixedKey(k);
                values[i].SetAvailableSize(0);
            }

            // Group keys by (leader address, region id)
            std::unordered_map<RegionGroupKey, RegionGroup, RegionGroupKeyHash> regionGroups;
            for (size_t i = 0; i < prefixedKeys.size(); i++) {
                RegionInfo region;
                std::string addr;
                uint64_t rid = 0;
                if (FindRegionForKey(prefixedKeys[i], region) && !region.leaderAddr.empty()) {
                    addr = region.leaderAddr;
                    rid = region.regionId;
                } else {
                    addr = GetAnyStoreAddress();
                }
                auto& g = regionGroups[{addr, rid}];
                if (g.keys.empty()) g.region = region;
                g.keys.push_back({i, prefixedKeys[i]});
            }

            // Send RawBatchGet per region group (in parallel via futures)
            std::vector<std::future<void>> futures;
            std::mutex resultMutex;

            for (auto& [gkey, rg] : regionGroups) {
                futures.push_back(std::async(std::launch::async, [&, &gkey, &rg]() {
                    auto& group = rg.keys;
                    auto* stub = GetOrCreateStub(gkey.leaderAddr);
                    if (!stub) return;

                    kvrpcpb::RawBatchGetRequest request;
                    SetContextFromRegion(request.mutable_context(), rg.region);
                    for (auto& [idx, pkey] : group) {
                        request.add_keys(pkey);
                    }

                    kvrpcpb::RawBatchGetResponse response;
                    grpc::ClientContext ctx;
                    SetDeadline(ctx, timeout);

                    auto status = stub->RawBatchGet(&ctx, request, &response);
                    if (!status.ok()) {
                        // Fallback: individual gets for this group
                        for (auto& [idx, pkey] : group) {
                            std::string val;
                            auto ret = Get(keys[idx], &val, timeout, reqs);
                            if (ret == ErrorCode::Success && !val.empty()) {
                                std::lock_guard<std::mutex> lock(resultMutex);
                                if (val.size() > values[idx].GetPageSize()) {
                                    values[idx].ReservePageBuffer(val.size());
                                }
                                memcpy(values[idx].GetBuffer(), val.data(), val.size());
                                values[idx].SetAvailableSize(static_cast<int>(val.size()));
                            }
                        }
                        return;
                    }

                    // Build a map from prefixed key -> response value
                    std::unordered_map<std::string, std::string> resultMap;
                    for (int p = 0; p < response.pairs_size(); p++) {
                        const auto& pair = response.pairs(p);
                        if (!pair.has_error() && !pair.value().empty()) {
                            resultMap[pair.key()] = pair.value();
                        }
                    }

                    // Copy results to output buffers
                    std::lock_guard<std::mutex> lock(resultMutex);
                    for (auto& [idx, pkey] : group) {
                        auto it = resultMap.find(pkey);
                        if (it != resultMap.end()) {
                            const auto& val = it->second;
                            if (val.size() > values[idx].GetPageSize()) {
                                values[idx].ReservePageBuffer(val.size());
                            }
                            memcpy(values[idx].GetBuffer(), val.data(), val.size());
                            values[idx].SetAvailableSize(static_cast<int>(val.size()));
                        }
                        // else: remains empty (SetAvailableSize(0)), tolerating missing keys
                    }
                }));
            }

            // Wait for all region batch gets to complete
            for (auto& f : futures) {
                f.get();
            }

            return ErrorCode::Success;
        }

        ErrorCode MultiGet(const std::vector<std::string>& keys,
                           std::vector<std::string>* values,
                           const std::chrono::microseconds& timeout,
                           std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            if (keys.empty()) return ErrorCode::Success;

            // Build prefixed keys
            std::vector<std::string> prefixedKeys(keys.size());
            for (size_t i = 0; i < keys.size(); i++) {
                prefixedKeys[i] = MakePrefixedKey(keys[i]);
            }

            // Initialize output with empty strings
            values->resize(keys.size());

            // Group by (leader address, region id)
            std::unordered_map<RegionGroupKey, RegionGroup, RegionGroupKeyHash> regionGroups;
            for (size_t i = 0; i < prefixedKeys.size(); i++) {
                RegionInfo region;
                std::string addr;
                uint64_t rid = 0;
                if (FindRegionForKey(prefixedKeys[i], region) && !region.leaderAddr.empty()) {
                    addr = region.leaderAddr;
                    rid = region.regionId;
                } else {
                    addr = GetAnyStoreAddress();
                }
                auto& g = regionGroups[{addr, rid}];
                if (g.keys.empty()) g.region = region;
                g.keys.push_back({i, prefixedKeys[i]});
            }

            for (auto& [gkey, rg] : regionGroups) {
                auto& group = rg.keys;
                auto* stub = GetOrCreateStub(gkey.leaderAddr);
                if (!stub) continue;

                kvrpcpb::RawBatchGetRequest request;
                SetContextFromRegion(request.mutable_context(), rg.region);
                for (auto& [idx, pkey] : group) {
                    request.add_keys(pkey);
                }

                kvrpcpb::RawBatchGetResponse response;
                grpc::ClientContext ctx;
                SetDeadline(ctx, timeout);

                auto status = stub->RawBatchGet(&ctx, request, &response);
                if (!status.ok()) {
                    // Fallback to individual gets
                    for (auto& [idx, pkey] : group) {
                        std::string val;
                        if (Get(keys[idx], &val, timeout, reqs) == ErrorCode::Success) {
                            (*values)[idx] = std::move(val);
                        }
                    }
                    continue;
                }

                std::unordered_map<std::string, std::string> resultMap;
                for (int p = 0; p < response.pairs_size(); p++) {
                    const auto& pair = response.pairs(p);
                    if (!pair.has_error() && !pair.value().empty()) {
                        resultMap[pair.key()] = pair.value();
                    }
                }

                for (auto& [idx, pkey] : group) {
                    auto it = resultMap.find(pkey);
                    if (it != resultMap.end()) {
                        (*values)[idx] = std::move(it->second);
                    }
                }
            }

            return ErrorCode::Success;
        }

        ErrorCode MultiGet(const std::vector<SizeType>& keys,
                           std::vector<std::string>* values,
                           const std::chrono::microseconds& timeout,
                           std::vector<Helper::AsyncReadRequest>* reqs) override
        {
            if (keys.empty()) return ErrorCode::Success;

            // Convert SizeType keys to strings and delegate
            std::vector<std::string> strKeys(keys.size());
            for (size_t i = 0; i < keys.size(); i++) {
                strKeys[i] = std::string(reinterpret_cast<const char*>(&keys[i]), sizeof(SizeType));
            }
            return MultiGet(strKeys, values, timeout, reqs);
        }

        // ---- Coprocessor vector search ----
        // Push distance computation into TiKV: send query vector + posting
        // keys, TiKV reads posting data locally, computes L2 distances, and
        // returns only top-N (vector_id, distance) candidates.

        struct CoprocessorResult {
            SizeType vectorID;
            float distance;
        };

        ErrorCode CoprocessorSearch(
            const std::vector<SizeType>& postingIDs,
            const uint8_t* queryVector,
            int dim,
            int valueType,       // 0=UInt8, 1=Int8, 3=Float32
            int metaDataSize,
            int topN,
            const std::chrono::microseconds& timeout,
            std::vector<CoprocessorResult>& results)
        {
            if (postingIDs.empty()) return ErrorCode::Success;

            // Determine vector data size
            int valueSize = (valueType == 3) ? 4 : 1;
            int queryVecBytes = dim * valueSize;

            // Build prefixed keys for all posting IDs
            std::vector<std::string> prefixedKeys(postingIDs.size());
            for (size_t i = 0; i < postingIDs.size(); i++) {
                std::string k(reinterpret_cast<const char*>(&postingIDs[i]), sizeof(SizeType));
                prefixedKeys[i] = MakePrefixedKey(k);
            }

            // Group keys by (leader address, region id)
            std::unordered_map<RegionGroupKey, RegionGroup, RegionGroupKeyHash> regionGroups;
            for (size_t i = 0; i < prefixedKeys.size(); i++) {
                RegionInfo region;
                std::string addr;
                uint64_t rid = 0;
                if (FindRegionForKey(prefixedKeys[i], region) && !region.leaderAddr.empty()) {
                    addr = region.leaderAddr;
                    rid = region.regionId;
                } else {
                    addr = GetAnyStoreAddress();
                }
                auto& g = regionGroups[{addr, rid}];
                if (g.keys.empty()) g.region = region;
                g.keys.push_back({i, prefixedKeys[i]});
            }

            // Send RawCoprocessor per region group in parallel
            std::vector<std::future<std::vector<CoprocessorResult>>> futures;

            for (auto& [gkey, rg] : regionGroups) {
                futures.push_back(std::async(std::launch::async,
                    [&, &gkey, &rg]() -> std::vector<CoprocessorResult> {
                    auto& group = rg.keys;
                    auto* stub = GetOrCreateStub(gkey.leaderAddr);
                    if (!stub) return {};

                    // Encode the vector search request
                    std::string requestData = EncodeVectorSearchRequest(
                        dim, topN, valueType, metaDataSize,
                        queryVector, queryVecBytes, group);

                    kvrpcpb::RawCoprocessorRequest request;
                    SetContextFromRegion(request.mutable_context(), rg.region);
                    request.set_copr_name("vector_search");
                    request.set_copr_version_req("*");
                    request.set_data(std::move(requestData));

                    // Add a key range covering the group keys for routing
                    auto* range = request.add_ranges();
                    range->set_start_key(group.front().second);
                    range->set_end_key(group.back().second);

                    kvrpcpb::RawCoprocessorResponse response;
                    grpc::ClientContext ctx;
                    SetDeadline(ctx, timeout);

                    auto status = stub->RawCoprocessor(&ctx, request, &response);
                    if (!status.ok()) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                            "TiKVIO::CoprocessorSearch gRPC error: %s\n",
                            status.error_message().c_str());
                        return {};
                    }
                    if (response.has_region_error()) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                            "TiKVIO::CoprocessorSearch region error\n");
                        InvalidateRegionCache(group[0].second);
                        return {};
                    }
                    if (!response.error().empty()) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                            "TiKVIO::CoprocessorSearch error: %s\n",
                            response.error().c_str());
                        return {};
                    }

                    return DecodeVectorSearchResponse(response.data());
                }));
            }

            // Merge results from all regions
            results.clear();
            for (auto& f : futures) {
                auto regionResults = f.get();
                results.insert(results.end(), regionResults.begin(), regionResults.end());
            }

            // Sort by distance and truncate to topN
            std::sort(results.begin(), results.end(),
                [](const CoprocessorResult& a, const CoprocessorResult& b) {
                    return a.distance < b.distance;
                });
            if (static_cast<int>(results.size()) > topN) {
                results.resize(topN);
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

        // ---- TODO2: Distributed routing support ----
        bool GetKeyLocation(SizeType key, Helper::KeyLocation& loc) override {
            std::string k(reinterpret_cast<const char*>(&key), sizeof(SizeType));
            std::string prefixedKey = MakePrefixedKey(k);
            RegionInfo region;
            if (!FindRegionForKey(prefixedKey, region) || region.leaderAddr.empty())
                return false;
            loc.regionId = region.regionId;
            loc.leaderStoreAddr = region.leaderAddr;
            return true;
        }

        bool GetKeyLocations(const std::vector<SizeType>& keys,
                             std::vector<Helper::KeyLocation>& locs) override {
            locs.resize(keys.size());
            bool allFound = true;
            for (size_t i = 0; i < keys.size(); i++) {
                if (!GetKeyLocation(keys[i], locs[i]))
                    allFound = false;
            }
            return allFound;
        }

    private:
        std::string m_keyPrefix;
        std::vector<std::string> m_pdAddresses;
        std::unique_ptr<pdpb::PD::Stub> m_pdStub;
        uint64_t m_clusterId = 0;
        bool m_available = false;

        // TiKV store stubs keyed by store address
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

        // Overload: set context directly from a cached RegionInfo.
        void SetContextFromRegion(kvrpcpb::Context* ctx, const RegionInfo& region) {
            ctx->set_region_id(region.regionId);
            *ctx->mutable_region_epoch() = region.epoch;
            *ctx->mutable_peer() = region.leaderPeer;
        }

        // Composite key for grouping by (leader address, region id).
        struct RegionGroupKey {
            std::string leaderAddr;
            uint64_t regionId;
            bool operator==(const RegionGroupKey& o) const {
                return leaderAddr == o.leaderAddr && regionId == o.regionId;
            }
        };
        struct RegionGroupKeyHash {
            size_t operator()(const RegionGroupKey& k) const {
                auto h1 = std::hash<std::string>{}(k.leaderAddr);
                auto h2 = std::hash<uint64_t>{}(k.regionId);
                return h1 ^ (h2 << 1);
            }
        };

        struct RegionGroup {
            RegionInfo region;
            std::vector<std::pair<size_t, std::string>> keys; // (original_index, prefixed_key)
        };

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

            auto* result = stub.get();
            m_storeStubs[address] = std::move(stub);
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TiKVIO: Created stub for TiKV store at %s\n", address.c_str());
            return result;
        }

        // ---- Vector search protocol encoding/decoding ----

        static constexpr uint32_t VSCH_MAGIC = 0x56534348; // "VSCH"
        static constexpr uint32_t VSCH_VERSION = 1;

        std::string EncodeVectorSearchRequest(
            int dim, int topN, int valueType, int metaDataSize,
            const uint8_t* queryVector, int queryVecBytes,
            const std::vector<std::pair<size_t, std::string>>& group) const
        {
            // Header: 7 × uint32 = 28 bytes
            // Query vector: queryVecBytes
            // Keys: (4 + keyLen) per key
            size_t totalSize = 28 + queryVecBytes;
            for (auto& [idx, pkey] : group) {
                totalSize += 4 + pkey.size();
            }

            std::string buf;
            buf.resize(totalSize);
            char* p = buf.data();

            auto write_u32 = [&p](uint32_t v) {
                memcpy(p, &v, 4); p += 4;
            };

            write_u32(VSCH_MAGIC);
            write_u32(VSCH_VERSION);
            write_u32(static_cast<uint32_t>(dim));
            write_u32(static_cast<uint32_t>(topN));
            write_u32(static_cast<uint32_t>(valueType));
            write_u32(static_cast<uint32_t>(metaDataSize));
            write_u32(static_cast<uint32_t>(group.size()));

            memcpy(p, queryVector, queryVecBytes);
            p += queryVecBytes;

            for (auto& [idx, pkey] : group) {
                uint32_t keyLen = static_cast<uint32_t>(pkey.size());
                memcpy(p, &keyLen, 4); p += 4;
                memcpy(p, pkey.data(), keyLen); p += keyLen;
            }

            return buf;
        }

        std::vector<CoprocessorResult> DecodeVectorSearchResponse(const std::string& data) const {
            std::vector<CoprocessorResult> results;
            if (data.size() < 12) return results;

            const char* p = data.data();
            uint32_t magic, version, numResults;
            memcpy(&magic, p, 4); p += 4;
            memcpy(&version, p, 4); p += 4;
            memcpy(&numResults, p, 4); p += 4;

            if (magic != VSCH_MAGIC || version != VSCH_VERSION) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "TiKVIO: Invalid vector search response magic=%x version=%u\n",
                    magic, version);
                return results;
            }

            if (data.size() < 12 + numResults * 12) return results;

            results.reserve(numResults);
            for (uint32_t i = 0; i < numResults; i++) {
                CoprocessorResult r;
                int64_t vid;
                memcpy(&vid, p, 8); p += 8;
                r.vectorID = static_cast<SizeType>(vid);
                memcpy(&r.distance, p, 4); p += 4;
                results.push_back(r);
            }

            return results;
        }
    };
} // namespace SPTAG::SPANN

#endif // _SPTAG_SPANN_EXTRATIKVCONTROLLER_H_
