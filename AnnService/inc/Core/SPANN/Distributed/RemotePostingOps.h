// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once

#include "inc/Core/SPANN/Distributed/DistributedProtocol.h"
#include "inc/Core/SPANN/Distributed/OpId.h"
#include "inc/Core/SPANN/Distributed/OpIdCache.h"
#include <chrono>
#include "inc/Socket/Client.h"
#include "inc/Socket/Server.h"
#include "inc/Socket/Packet.h"
#include "inc/Socket/SimpleSerialization.h"
#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace SPTAG::SPANN {

    /// Handles all node-to-node RPC mechanics for internal posting operations:
    ///   - Append / BatchAppend (forward writes to the correct owner node)
    ///   - HeadSync (broadcast head index changes to peers)
    ///   - RemoteLock (cross-node locking for merge/split)
    ///
    /// This class owns the request/response matching state and serialization
    /// logic. It is independent of routing decisions — WorkerNode decides
    /// *where* to send, RemotePostingOps handles *how*.
    class RemotePostingOps {
    public:
        using AppendCallback = std::function<ErrorCode(
            SizeType headID,
            std::shared_ptr<std::string> headVec,
            int appendNum,
            std::string& appendPosting)>;

        using HeadSyncCallback = std::function<void(const HeadSyncEntry& entry)>;
        using RemoteLockCallback = std::function<bool(SizeType headID, bool lock)>;

        /// Abstract interface for network access (implemented by NetworkNode).
        class NetworkAccess {
        public:
            virtual ~NetworkAccess() = default;
            virtual Socket::ConnectionID GetPeerConnection(int nodeIndex) = 0;
            virtual void InvalidatePeerConnection(int nodeIndex) = 0;
            virtual int GetLocalNodeIndex() const = 0;
            virtual int GetNumNodes() const = 0;
            virtual Socket::Client* GetClient() = 0;
            virtual Socket::Server* GetServer() = 0;

            /// Current local ring epoch. {0,0} means "not yet initialised".
            /// Default is {0,0} so legacy implementers (test doubles) keep
            /// compiling — they will simply trip the sender-startup gate
            /// on any routed send, which is the safe default.
            virtual RingEpoch GetCurrentRingEpoch() const { return RingEpoch{}; }
        };

        RemotePostingOps() = default;

        void SetNetwork(NetworkAccess* net) { m_net = net; }

        void SetAppendCallback(AppendCallback cb) { m_appendCallback = std::move(cb); }
        void SetHeadSyncCallback(HeadSyncCallback cb) { m_headSyncCallback = std::move(cb); }
        void SetRemoteLockCallback(RemoteLockCallback cb) { m_remoteLockCallback = std::move(cb); }

        // Configure idempotency for outbound (sender) and inbound (receiver)
        // Append paths. Must be called before any traffic.
        void ConfigureIdempotency(std::int32_t senderId,
                                  std::uint32_t restartEpoch,
                                  std::size_t cacheCapacity = 4096,
                                  std::chrono::seconds cacheTtl = std::chrono::seconds(60)) {
            m_opIdAllocator.Reset(senderId, restartEpoch);
            m_appendDedupCache = std::make_unique<
                Distributed::OpIdCache<Distributed::AppendDedupResult>>(
                    cacheCapacity ? cacheCapacity : 4096, cacheTtl);
        }

        // Test hooks.
        Distributed::OpIdCache<Distributed::AppendDedupResult>* AppendDedupCacheForTest() {
            return m_appendDedupCache.get();
        }
        Distributed::OpIdAllocator& OpIdAllocatorForTest() { return m_opIdAllocator; }

        // ==================================================================
        //  Owner-failover routing wrapper (FT case: posting-router-owner-failover)
        // ==================================================================
        //
        // Behaviour: when env SPTAG_FAULT_POSTING_ROUTER_OWNER_FAILOVER is set,
        // an in-flight RemoteAppend that fails with StaleRingEpoch / Fail /
        // RingNotReady is retried after re-resolving the owner via the
        // caller-supplied ring lookup. The SAME OpId is carried across
        // attempts so receiver-side dedup collapses duplicates.
        //
        // The wrapper is a no-op when the env is unset (caller short-circuits
        // to the original SendRemoteAppend path). The hot path is therefore
        // unaffected by this code on baseline runs.
        //
        // Counters: m_ownerFailoverRetries / ..StaleEpochFenced.
        // Receiver dedup collapse is observed via m_dedupReplays (existing).
        static bool OwnerFailoverArmed() {
            return std::getenv("SPTAG_FAULT_POSTING_ROUTER_OWNER_FAILOVER") != nullptr;
        }

        // Generic owner-failover loop. Caller provides resolveOwner (called per
        // attempt to pick a target node from the current ring) and sendFn
        // (executes the per-attempt RPC with the supplied OpId). Returns the
        // final ErrorCode of the last attempt.
        //
        // sendFn signature: ErrorCode(int target, const Distributed::OpId& opId)
        // resolveOwner signature: int(SizeType headID)
        template <typename ResolveOwnerFn, typename SendFn>
        ErrorCode SendRemoteAppendWithFailover(
            SizeType headID,
            ResolveOwnerFn resolveOwner,
            SendFn sendFn,
            int maxRetries = 3)
        {
            Distributed::OpId opId{};
            if (m_opIdAllocator.SenderId() >= 0) {
                opId = m_opIdAllocator.Next();
            }

            int target = resolveOwner(headID);
            ErrorCode last = ErrorCode::Fail;
            for (int attempt = 0; attempt <= maxRetries; ++attempt) {
                last = sendFn(target, opId);
                if (last == ErrorCode::Success) return ErrorCode::Success;

                bool fenceable =
                    (last == ErrorCode::StaleRingEpoch) ||
                    (last == ErrorCode::RingNotReady)   ||
                    (last == ErrorCode::Fail);
                if (!fenceable) return last;

                if (last == ErrorCode::StaleRingEpoch) {
                    m_ownerFailoverStaleEpochFenced.fetch_add(1);
                }
                if (attempt == maxRetries) break;

                m_ownerFailoverRetries.fetch_add(1);
                int newTarget = resolveOwner(headID);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: owner-failover retry %d for headID %lld: target %d -> %d (last=%d)\n",
                    attempt + 1, (std::int64_t)headID, target, newTarget, (int)last);
                target = newTarget;
            }
            return last;
        }

        // Test hook: bump dedup-collapsed counter on receiver side. Mirrors
        // existing m_dedupReplays bump path; exposed so cross-instance tests
        // can verify collapse semantics without driving full HandleAppendRequest.
        std::uint64_t DedupReplaysForTest() const { return m_dedupReplays.load(); }

        // ==================================================================
        //  Append — single item, synchronous (waits for response)
        // ==================================================================

        ErrorCode SendRemoteAppend(
            int targetNodeIndex,
            SizeType headID,
            const std::shared_ptr<std::string>& headVec,
            int appendNum,
            std::string& appendPosting)
        {
            // Sender-startup gate: refuse routed RPCs until our ring view is
            // initialised. Eliminates the GetOwner→{isLocal=true,nodeIndex=-1}
            // hazard at the RPC layer (fault-case: routing-table-drift).
            RingEpoch senderEpoch = m_net->GetCurrentRingEpoch();
            if (!senderEpoch.IsInitialised()) {
                m_ringNotReadyRejects.fetch_add(1);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: refusing append to node %d, ring not initialised (sender-startup gate)\n",
                    targetNodeIndex);
                return ErrorCode::RingNotReady;
            }

            Socket::ConnectionID connID = m_net->GetPeerConnection(targetNodeIndex);
            if (connID == Socket::c_invalidConnectionID) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "RemotePostingOps: Cannot connect to node %d for headID %lld\n",
                    targetNodeIndex, (std::int64_t)headID);
                return ErrorCode::Fail;
            }

            RemoteAppendRequest req;
            req.m_headID = headID;
            req.m_headVec = *headVec;
            req.m_appendNum = appendNum;
            req.m_appendPosting = appendPosting;
            if (m_opIdAllocator.SenderId() >= 0) {
                req.m_opId = m_opIdAllocator.Next();
            }
            req.m_senderEpoch = senderEpoch;

            Socket::ResourceID resID = m_nextResourceId.fetch_add(1);
            auto [future, _] = CreatePendingResponse(resID);
            (void)_;

            Socket::Packet packet;
            packet.Header().m_packetType = Socket::PacketType::AppendRequest;
            packet.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
            packet.Header().m_connectionID = Socket::c_invalidConnectionID;
            packet.Header().m_resourceID = resID;

            auto bodySize = static_cast<std::uint32_t>(req.EstimateBufferSize());
            packet.Header().m_bodyLength = bodySize;
            packet.AllocateBuffer(bodySize);
            req.Write(packet.Body());
            packet.Header().WriteBuffer(packet.HeaderBuffer());

            m_net->GetClient()->SendPacket(connID, std::move(packet),
                MakeSendFailHandler(resID));

            auto status = future.wait_for(std::chrono::seconds(30));
            if (status == std::future_status::timeout) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "RemotePostingOps: Timeout waiting for append response for headID %lld from node %d\n",
                    (std::int64_t)headID, targetNodeIndex);
                ErasePending(resID);
                return ErrorCode::Fail;
            }
            return future.get();
        }

        // ==================================================================
        //  Append — batch, synchronous with retry
        // ==================================================================

        ErrorCode SendBatchRemoteAppend(
            int targetNodeIndex,
            std::vector<RemoteAppendRequest>& items)
        {
            if (items.empty()) return ErrorCode::Success;

            // Sender-startup gate (see SendRemoteAppend).
            RingEpoch senderEpoch = m_net->GetCurrentRingEpoch();
            if (!senderEpoch.IsInitialised()) {
                m_ringNotReadyRejects.fetch_add(1);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: refusing batch append (%d items) to node %d, ring not initialised\n",
                    (int)items.size(), targetNodeIndex);
                return ErrorCode::RingNotReady;
            }

            for (int attempt = 0; attempt < 2; attempt++) {
                Socket::ConnectionID connID = m_net->GetPeerConnection(targetNodeIndex);
                if (connID == Socket::c_invalidConnectionID) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                        "RemotePostingOps: Cannot connect to node %d for batch (%d items, attempt %d)\n",
                        targetNodeIndex, (int)items.size(), attempt + 1);
                    if (attempt == 0) continue;
                    return ErrorCode::Fail;
                }

                BatchRemoteAppendRequest batchReq;
                batchReq.m_count = static_cast<std::uint32_t>(items.size());
                batchReq.m_items = std::move(items);
                batchReq.m_senderEpoch = senderEpoch;
                for (auto& it : batchReq.m_items) it.m_senderEpoch = senderEpoch;

                Socket::ResourceID resID = m_nextResourceId.fetch_add(1);
                auto [future, _] = CreatePendingResponse(resID);
                (void)_;

                Socket::Packet packet;
                packet.Header().m_packetType = Socket::PacketType::BatchAppendRequest;
                packet.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
                packet.Header().m_connectionID = Socket::c_invalidConnectionID;
                packet.Header().m_resourceID = resID;

                auto bodySize = static_cast<std::uint32_t>(batchReq.EstimateBufferSize());
                packet.Header().m_bodyLength = bodySize;
                packet.AllocateBuffer(bodySize);
                batchReq.Write(packet.Body());
                items = std::move(batchReq.m_items); // restore for retry

                packet.Header().WriteBuffer(packet.HeaderBuffer());

                SPTAGLIB_LOG(Helper::LogLevel::LL_Debug,
                    "RemotePostingOps: Sending batch of %u appends to node %d (resID=%u, attempt=%d)\n",
                    batchReq.m_count, targetNodeIndex, resID, attempt + 1);

                m_net->GetClient()->SendPacket(connID, std::move(packet),
                    MakeSendFailHandler(resID));

                auto status = future.wait_for(std::chrono::seconds(60));
                if (status == std::future_status::timeout) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                        "RemotePostingOps: Timeout waiting for batch response from node %d\n",
                        targetNodeIndex);
                    ErasePending(resID);
                    m_net->InvalidatePeerConnection(targetNodeIndex);
                    if (attempt == 0) continue;
                    return ErrorCode::Fail;
                }

                ErrorCode result = future.get();
                if (result == ErrorCode::Success) return ErrorCode::Success;

                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: Batch to node %d failed (attempt %d), reconnecting...\n",
                    targetNodeIndex, attempt + 1);
                m_net->InvalidatePeerConnection(targetNodeIndex);
            }
            return ErrorCode::Fail;
        }

        // ==================================================================
        //  HeadSync — fire-and-forget broadcast
        // ==================================================================

        void BroadcastHeadSync(const std::vector<HeadSyncEntry>& entries) {
            if (entries.empty()) return;

            size_t bodySize = sizeof(std::uint32_t);
            for (const auto& e : entries) bodySize += e.EstimateBufferSize();

            int numNodes = m_net->GetNumNodes();
            int localIdx = m_net->GetLocalNodeIndex();

            for (int i = 0; i < numNodes; i++) {
                if (i == localIdx) continue;

                Socket::ConnectionID connID = m_net->GetPeerConnection(i);
                if (connID == Socket::c_invalidConnectionID) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                        "RemotePostingOps: Cannot broadcast head sync to node %d\n", i);
                    continue;
                }

                Socket::Packet pkt;
                pkt.Header().m_packetType = Socket::PacketType::HeadSyncRequest;
                pkt.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
                pkt.Header().m_connectionID = Socket::c_invalidConnectionID;
                pkt.Header().m_resourceID = 0;
                pkt.Header().m_bodyLength = static_cast<std::uint32_t>(bodySize);
                pkt.AllocateBuffer(static_cast<std::uint32_t>(bodySize));

                std::uint8_t* buf = pkt.Body();
                buf = Socket::SimpleSerialization::SimpleWriteBuffer(
                    static_cast<std::uint32_t>(entries.size()), buf);
                for (const auto& e : entries) buf = e.Write(buf);
                pkt.Header().WriteBuffer(pkt.HeaderBuffer());

                m_net->GetClient()->SendPacket(connID, std::move(pkt),
                    [i](bool success) {
                        if (!success) {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Debug,
                                "RemotePostingOps: Head sync send to node %d failed\n", i);
                        }
                    });
            }
        }

        // ==================================================================
        //  RemoteLock — synchronous request/response
        // ==================================================================

        bool SendRemoteLock(int nodeIndex, SizeType headID, bool lock) {
            Socket::ConnectionID connID = m_net->GetPeerConnection(nodeIndex);
            if (connID == Socket::c_invalidConnectionID) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: Cannot send remote lock to node %d\n", nodeIndex);
                return false;
            }

            RemoteLockRequest req;
            req.m_op = lock ? RemoteLockRequest::Op::Lock : RemoteLockRequest::Op::Unlock;
            req.m_headID = headID;

            Socket::ResourceID rid = m_nextResourceId.fetch_add(1);
            auto [future, _] = CreatePendingResponse(rid);
            (void)_;

            Socket::Packet pkt;
            auto bodySize = req.EstimateBufferSize();
            pkt.Header().m_packetType = Socket::PacketType::RemoteLockRequest;
            pkt.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
            pkt.Header().m_connectionID = Socket::c_invalidConnectionID;
            pkt.Header().m_resourceID = rid;
            pkt.Header().m_bodyLength = static_cast<std::uint32_t>(bodySize);
            pkt.AllocateBuffer(static_cast<std::uint32_t>(bodySize));
            req.Write(pkt.Body());
            pkt.Header().WriteBuffer(pkt.HeaderBuffer());

            m_net->GetClient()->SendPacket(connID, std::move(pkt),
                MakeSendFailHandler(rid));

            auto status = future.wait_for(std::chrono::milliseconds(5000));
            if (status != std::future_status::ready) {
                ErasePending(rid);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: Lock timeout for headID %lld on node %d\n",
                    (std::int64_t)headID, nodeIndex);
                return false;
            }
            return future.get() == ErrorCode::Success;
        }

        // ==================================================================
        //  Inbound packet handlers (called by WorkerNode's server/client)
        // ==================================================================

        void HandleAppendRequest(Socket::ConnectionID connID, Socket::Packet packet) {
            if (packet.Header().m_bodyLength == 0) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "RemotePostingOps: Empty AppendRequest\n");
                return;
            }

            if (Socket::c_invalidConnectionID == packet.Header().m_connectionID)
                packet.Header().m_connectionID = connID;

            RemoteAppendRequest req;
            if (req.Read(packet.Body()) == nullptr) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "RemotePostingOps: AppendRequest version mismatch\n");
                SendAppendResponse(packet, RemoteAppendResponse::Status::Failed,
                                   m_net ? m_net->GetCurrentRingEpoch() : RingEpoch{});
                return;
            }

            // Receiver-side ring-epoch fence — runs first so a stale sender
            // is rejected before we touch the dedup cache (split-brain guard).
            RingEpoch local = m_net ? m_net->GetCurrentRingEpoch() : RingEpoch{};
            auto cmp = CompareRingEpoch(req.m_senderEpoch, local);
            if (cmp == RingEpochCompare::SenderStale ||
                cmp == RingEpochCompare::SenderUninitialised) {
                m_staleSenderRejects.fetch_add(1);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: rejecting AppendRequest, stale sender (sender=%u/%u local=%u/%u)\n",
                    req.m_senderEpoch.epoch, req.m_senderEpoch.ringRev,
                    local.epoch, local.ringRev);
                SendAppendResponse(packet, RemoteAppendResponse::Status::StaleRingEpoch, local);
                return;
            }
            if (cmp == RingEpochCompare::ReceiverStale) {
                // Receiver is behind. Continue (fail-open at receiver side):
                // the dispatcher will catch up via RingUpdate retries; bumping
                // the counter lets a refresh hook observe the lag.
                m_receiverStaleObservations.fetch_add(1);
            }

            // Receiver-side idempotency dedup. Replay cached status on hit;
            // bump epoch tracking on a fresh restartEpoch from this sender.
            const Distributed::OpId& incomingOpId = req.m_opId;
            RemoteAppendResponse::Status status = RemoteAppendResponse::Status::Failed;

            if (m_appendDedupCache && incomingOpId.IsValid()) {
                {
                    std::lock_guard<std::mutex> g(m_epochMutex);
                    auto& seen = m_lastSeenEpoch[incomingOpId.senderId];
                    if (incomingOpId.restartEpoch > seen) {
                        m_appendDedupCache->InvalidateOlderEpochs(
                            incomingOpId.senderId, incomingOpId.restartEpoch);
                        seen = incomingOpId.restartEpoch;
                    }
                }

                auto [hit, cached] = m_appendDedupCache->Lookup(incomingOpId);
                if (hit) {
                    m_dedupReplays.fetch_add(1);
                    SendAppendResponse(packet,
                        static_cast<RemoteAppendResponse::Status>(cached.status),
                        local);
                    return;
                }
            }

            ErrorCode result = ErrorCode::Fail;
            if (m_appendCallback) {
                auto headVec = std::make_shared<std::string>(std::move(req.m_headVec));
                result = m_appendCallback(
                    req.m_headID, headVec, req.m_appendNum, req.m_appendPosting);
            }

            status = (result == ErrorCode::Success)
                ? RemoteAppendResponse::Status::Success
                : RemoteAppendResponse::Status::Failed;

            if (m_appendDedupCache && incomingOpId.IsValid()) {
                Distributed::AppendDedupResult cached;
                cached.status = static_cast<std::uint8_t>(status);
                m_appendDedupCache->Insert(incomingOpId, cached);
            }
            SendAppendResponse(packet, status, local);
        }

        void HandleAppendResponse(Socket::ConnectionID connID, Socket::Packet packet) {
            Socket::ResourceID resID = packet.Header().m_resourceID;
            auto promise = TakePendingResponse(resID);
            if (!promise) return;

            if (packet.Header().m_processStatus != Socket::PacketProcessStatus::Ok) {
                promise->set_value(ErrorCode::Fail);
                return;
            }

            RemoteAppendResponse resp;
            if (resp.Read(packet.Body()) == nullptr) {
                promise->set_value(ErrorCode::Fail);
                return;
            }

            if (resp.m_status == RemoteAppendResponse::Status::StaleRingEpoch) {
                m_staleSenderRoundtrips.fetch_add(1);
                promise->set_value(ErrorCode::StaleRingEpoch);
                return;
            }
            promise->set_value(
                resp.m_status == RemoteAppendResponse::Status::Success
                    ? ErrorCode::Success : ErrorCode::Fail);
        }

        void HandleBatchAppendRequest(Socket::ConnectionID connID, Socket::Packet packet) {
            if (packet.Header().m_bodyLength == 0) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "RemotePostingOps: Empty BatchAppendRequest\n");
                return;
            }

            if (Socket::c_invalidConnectionID == packet.Header().m_connectionID)
                packet.Header().m_connectionID = connID;

            BatchRemoteAppendRequest batchReq;
            if (batchReq.Read(packet.Body(), packet.Header().m_bodyLength) == nullptr) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "RemotePostingOps: BatchAppendRequest parse failed\n");
                SendBatchAppendResponse(packet, 0, 1,
                                        BatchRemoteAppendResponse::FenceStatus::Ok,
                                        m_net ? m_net->GetCurrentRingEpoch() : RingEpoch{});
                return;
            }

            // Receiver-side ring-epoch fence (batch envelope).
            RingEpoch local = m_net ? m_net->GetCurrentRingEpoch() : RingEpoch{};
            auto cmp = CompareRingEpoch(batchReq.m_senderEpoch, local);
            if (cmp == RingEpochCompare::SenderStale ||
                cmp == RingEpochCompare::SenderUninitialised) {
                m_staleSenderRejects.fetch_add(1);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: rejecting BatchAppendRequest (%u items), stale sender (sender=%u/%u local=%u/%u)\n",
                    batchReq.m_count,
                    batchReq.m_senderEpoch.epoch, batchReq.m_senderEpoch.ringRev,
                    local.epoch, local.ringRev);
                SendBatchAppendResponse(packet, 0, batchReq.m_count,
                                        BatchRemoteAppendResponse::FenceStatus::StaleRingEpoch,
                                        local);
                return;
            }
            if (cmp == RingEpochCompare::ReceiverStale) {
                m_receiverStaleObservations.fetch_add(1);
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Debug,
                "RemotePostingOps: Received batch of %u appends\n", batchReq.m_count);

            std::atomic<std::uint32_t> successCount(0), failCount(0);
            std::vector<std::thread> workers;
            std::atomic<size_t> nextItem(0);
            int numWorkers = std::min(static_cast<int>(batchReq.m_items.size()), 16);
            workers.reserve(numWorkers);
            for (int w = 0; w < numWorkers; w++) {
                workers.emplace_back([&]() {
                    while (true) {
                        size_t idx = nextItem.fetch_add(1);
                        if (idx >= batchReq.m_items.size()) break;
                        auto& req = batchReq.m_items[idx];
                        ErrorCode result = ErrorCode::Fail;
                        if (m_appendCallback) {
                            auto headVec = std::make_shared<std::string>(std::move(req.m_headVec));
                            result = m_appendCallback(
                                req.m_headID, headVec, req.m_appendNum, req.m_appendPosting);
                        }
                        if (result == ErrorCode::Success) successCount.fetch_add(1);
                        else failCount.fetch_add(1);
                    }
                });
            }
            for (auto& t : workers) t.join();

            SendBatchAppendResponse(packet, successCount, failCount,
                                    BatchRemoteAppendResponse::FenceStatus::Ok,
                                    local);
        }

        void HandleBatchAppendResponse(Socket::ConnectionID connID, Socket::Packet packet) {
            Socket::ResourceID resID = packet.Header().m_resourceID;
            auto promise = TakePendingResponse(resID);
            if (!promise) return;

            if (packet.Header().m_processStatus != Socket::PacketProcessStatus::Ok) {
                promise->set_value(ErrorCode::Fail);
                return;
            }

            BatchRemoteAppendResponse resp;
            if (resp.Read(packet.Body()) == nullptr) {
                promise->set_value(ErrorCode::Fail);
                return;
            }

            if (resp.m_fenceStatus == BatchRemoteAppendResponse::FenceStatus::StaleRingEpoch) {
                m_staleSenderRoundtrips.fetch_add(1);
                promise->set_value(ErrorCode::StaleRingEpoch);
                return;
            }
            promise->set_value(resp.m_failCount == 0 ? ErrorCode::Success : ErrorCode::Fail);
        }

        void HandleHeadSyncRequest(Socket::ConnectionID connID, Socket::Packet packet) {
            if (!m_headSyncCallback) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: HeadSyncRequest but no callback set\n");
                return;
            }

            const std::uint8_t* buf = packet.Body();
            const std::uint8_t* bufEnd = buf + packet.Header().m_bodyLength;
            std::uint32_t entryCount = 0;
            buf = Socket::SimpleSerialization::SimpleReadBuffer(buf, entryCount);

            std::uint32_t bodyLength = packet.Header().m_bodyLength;
            if (bodyLength < sizeof(std::uint32_t) ||
                entryCount > (bodyLength - sizeof(std::uint32_t)) / 8) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "RemotePostingOps: HeadSyncRequest entryCount=%u exceeds bodyLength=%u\n",
                    entryCount, bodyLength);
                return;
            }

            for (std::uint32_t i = 0; i < entryCount; i++) {
                if (buf >= bufEnd) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                        "RemotePostingOps: HeadSync buffer overrun at entry %u/%u\n", i, entryCount);
                    break;
                }
                HeadSyncEntry entry;
                buf = entry.Read(buf);
                if (!buf || buf > bufEnd) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                        "RemotePostingOps: HeadSync parse error at entry %u/%u\n", i, entryCount);
                    break;
                }
                m_headSyncCallback(entry);
            }
        }

        void HandleRemoteLockRequest(Socket::ConnectionID connID, Socket::Packet packet) {
            RemoteLockRequest req;
            if (req.Read(packet.Body()) == nullptr) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "RemotePostingOps: Failed to parse RemoteLockRequest\n");
                return;
            }

            RemoteLockResponse resp;
            resp.m_status = RemoteLockResponse::Status::Denied;

            if (m_remoteLockCallback) {
                bool isLock = (req.m_op == RemoteLockRequest::Op::Lock);
                bool success = m_remoteLockCallback(req.m_headID, isLock);
                if (success) resp.m_status = RemoteLockResponse::Status::Granted;
            }

            Socket::Packet ret;
            auto bodySize = resp.EstimateBufferSize();
            ret.Header().m_packetType = Socket::PacketType::RemoteLockResponse;
            ret.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
            ret.Header().m_connectionID = connID;
            ret.Header().m_resourceID = packet.Header().m_resourceID;
            ret.Header().m_bodyLength = static_cast<std::uint32_t>(bodySize);
            ret.AllocateBuffer(static_cast<std::uint32_t>(bodySize));
            resp.Write(ret.Body());
            ret.Header().WriteBuffer(ret.HeaderBuffer());

            m_net->GetServer()->SendPacket(connID, std::move(ret), nullptr);
        }

        void HandleRemoteLockResponse(Socket::ConnectionID connID, Socket::Packet packet) {
            Socket::ResourceID rid = packet.Header().m_resourceID;
            auto promise = TakePendingResponse(rid);
            if (!promise) return;

            RemoteLockResponse resp;
            if (resp.Read(packet.Body()) == nullptr) {
                promise->set_value(ErrorCode::Fail);
                return;
            }

            promise->set_value(resp.m_status == RemoteLockResponse::Status::Granted
                ? ErrorCode::Success : ErrorCode::Fail);
        }

    private:
        // ---- Response matching helpers ----

        std::pair<std::future<ErrorCode>, bool> CreatePendingResponse(Socket::ResourceID resID) {
            std::promise<ErrorCode> promise;
            auto future = promise.get_future();
            std::lock_guard<std::mutex> lock(m_pendingMutex);
            m_pendingResponses.emplace(resID, std::move(promise));
            return {std::move(future), true};
        }

        void ErasePending(Socket::ResourceID resID) {
            std::lock_guard<std::mutex> lock(m_pendingMutex);
            m_pendingResponses.erase(resID);
        }

        /// Take a pending promise out of the map (returns nullptr if not found).
        std::unique_ptr<std::promise<ErrorCode>> TakePendingResponse(Socket::ResourceID resID) {
            std::lock_guard<std::mutex> lock(m_pendingMutex);
            auto it = m_pendingResponses.find(resID);
            if (it == m_pendingResponses.end()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "RemotePostingOps: Response for unknown resourceID %u\n", resID);
                return nullptr;
            }
            auto p = std::make_unique<std::promise<ErrorCode>>(std::move(it->second));
            m_pendingResponses.erase(it);
            return p;
        }

        /// Create a send-failure callback that resolves the pending promise.
        std::function<void(bool)> MakeSendFailHandler(Socket::ResourceID resID) {
            return [resID, this](bool success) {
                if (!success) {
                    std::lock_guard<std::mutex> lock(m_pendingMutex);
                    auto it = m_pendingResponses.find(resID);
                    if (it != m_pendingResponses.end()) {
                        it->second.set_value(ErrorCode::Fail);
                        m_pendingResponses.erase(it);
                    }
                }
            };
        }

        void SendAppendResponse(Socket::Packet& srcPacket,
                                RemoteAppendResponse::Status status,
                                RingEpoch authoritative = RingEpoch{}) {
            RemoteAppendResponse resp;
            resp.m_status = status;
            resp.m_authoritativeEpoch = authoritative;

            Socket::Packet ret;
            ret.Header().m_packetType = Socket::PacketType::AppendResponse;
            ret.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
            ret.Header().m_connectionID = srcPacket.Header().m_connectionID;
            ret.Header().m_resourceID = srcPacket.Header().m_resourceID;

            auto bodySize = static_cast<std::uint32_t>(resp.EstimateBufferSize());
            ret.Header().m_bodyLength = bodySize;
            ret.AllocateBuffer(bodySize);
            resp.Write(ret.Body());
            ret.Header().WriteBuffer(ret.HeaderBuffer());

            m_net->GetServer()->SendPacket(srcPacket.Header().m_connectionID, std::move(ret), nullptr);
        }

        void SendBatchAppendResponse(Socket::Packet& srcPacket,
            std::uint32_t successCount, std::uint32_t failCount,
            BatchRemoteAppendResponse::FenceStatus fenceStatus = BatchRemoteAppendResponse::FenceStatus::Ok,
            RingEpoch authoritative = RingEpoch{}) {
            BatchRemoteAppendResponse resp;
            resp.m_successCount = successCount;
            resp.m_failCount = failCount;
            resp.m_fenceStatus = fenceStatus;
            resp.m_authoritativeEpoch = authoritative;

            Socket::Packet ret;
            ret.Header().m_packetType = Socket::PacketType::BatchAppendResponse;
            ret.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
            ret.Header().m_connectionID = srcPacket.Header().m_connectionID;
            ret.Header().m_resourceID = srcPacket.Header().m_resourceID;

            auto bodySize = static_cast<std::uint32_t>(resp.EstimateBufferSize());
            ret.Header().m_bodyLength = bodySize;
            ret.AllocateBuffer(bodySize);
            resp.Write(ret.Body());
            ret.Header().WriteBuffer(ret.HeaderBuffer());

            m_net->GetServer()->SendPacket(srcPacket.Header().m_connectionID, std::move(ret), nullptr);
        }

        // ---- State ----

        NetworkAccess* m_net = nullptr;
        AppendCallback m_appendCallback;
        HeadSyncCallback m_headSyncCallback;
        RemoteLockCallback m_remoteLockCallback;

        std::atomic<Socket::ResourceID> m_nextResourceId{1};
        std::mutex m_pendingMutex;
        std::unordered_map<Socket::ResourceID, std::promise<ErrorCode>> m_pendingResponses;

        // Idempotency state.
        Distributed::OpIdAllocator m_opIdAllocator;
        std::unique_ptr<Distributed::OpIdCache<Distributed::AppendDedupResult>>
            m_appendDedupCache;
        std::mutex m_epochMutex;
        std::unordered_map<std::int32_t, std::uint32_t> m_lastSeenEpoch;
        std::atomic<std::uint64_t> m_dedupReplays{0};

        // ---- Ring-epoch fence counters (test + ops observability) ----
        std::atomic<std::uint64_t> m_staleSenderRejects{0};
        std::atomic<std::uint64_t> m_staleSenderRoundtrips{0};
        std::atomic<std::uint64_t> m_receiverStaleObservations{0};
        std::atomic<std::uint64_t> m_ringNotReadyRejects{0};

        // ---- Owner-failover counters (posting-router-owner-failover fault) ----
        std::atomic<std::uint64_t> m_ownerFailoverRetries{0};
        std::atomic<std::uint64_t> m_ownerFailoverStaleEpochFenced{0};
        std::atomic<std::uint64_t> m_ownerFailoverDedupCollapsed{0};

    public:
        struct FenceCounters {
            std::uint64_t staleSenderRejects;        // receiver rejected an inbound stale RPC
            std::uint64_t staleSenderRoundtrips;     // sender saw a StaleRingEpoch reply
            std::uint64_t receiverStaleObservations; // receiver noticed it was behind
            std::uint64_t ringNotReadyRejects;       // sender-startup gate fired
            std::uint64_t dedupReplays;              // receiver replayed cached result for a dup OpId
        };

        FenceCounters GetFenceCounters() const {
            return FenceCounters{
                m_staleSenderRejects.load(),
                m_staleSenderRoundtrips.load(),
                m_receiverStaleObservations.load(),
                m_ringNotReadyRejects.load(),
                m_dedupReplays.load(),
            };
        }

        struct OwnerFailoverCounters {
            std::uint64_t retries;
            std::uint64_t staleEpochFenced;
            std::uint64_t dedupCollapsed;
        };

        OwnerFailoverCounters GetOwnerFailoverCounters() const {
            return OwnerFailoverCounters{
                m_ownerFailoverRetries.load(),
                m_ownerFailoverStaleEpochFenced.load(),
                m_ownerFailoverDedupCollapsed.load(),
            };
        }
    };

} // namespace SPTAG::SPANN
