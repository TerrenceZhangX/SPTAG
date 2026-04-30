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
        std::uint64_t BatchDedupHitsForTest() const {
            return m_batchDedupHits.load(std::memory_order_relaxed);
        }
        std::uint64_t SingleDedupHitsForTest() const {
            return m_singleDedupHits.load(std::memory_order_relaxed);
        }
        // Drives the per-item dedup decision used by the batch receiver,
        // *without* needing a real Socket::Packet / network. Returns the
        // status that the batch handler would emit for this item: a cached
        // status on dedup hit, otherwise the result of running the configured
        // append callback. Mirrors HandleBatchAppendRequest's per-item logic
        // exactly so tests exercise the production path.
        RemoteAppendResponse::Status ProcessBatchItemForTest(RemoteAppendRequest& item) {
            return ProcessOneAppendItem(item, /*isBatch=*/true);
        }
        // Stamp OpIds on a vector of items the same way SendBatchRemoteAppend
        // does, but without sending. Used by tests to observe the contract:
        // every previously-invalid item gets a fresh, valid, monotonic OpId;
        // pre-stamped items are left intact so retries reuse the same id.
        void StampBatchOpIdsForTest(std::vector<RemoteAppendRequest>& items) {
            StampBatchOpIdsLocked(items);
        }
        Distributed::OpIdAllocator& OpIdAllocatorForTest() { return m_opIdAllocator; }

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

            // Idempotency: stamp every item with a fresh OpId on first entry.
            // Items pre-stamped by an upstream caller (e.g. an earlier failed
            // SendBatchRemoteAppend that bubbled the items back up through
            // FlushRemoteAppends) keep their existing id, so the receiver's
            // dedup cache can recognise this as a retry. Items queued via
            // QueueRemoteAppend arrive with an invalid OpId and get one here.
            // The 2-attempt retry loop below preserves m_items across attempts
            // (move-then-restore on success of the Write step), so a retry to
            // the same owner uses the same OpIds.
            StampBatchOpIdsLocked(items);

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
                SendAppendResponse(packet, RemoteAppendResponse::Status::Failed);
                return;
            }

            // Receiver-side idempotency dedup. Replay cached status on hit;
            // bump epoch tracking on a fresh restartEpoch from this sender.
            // online-insert-idempotency-on-retry: shared with the batch path
            // via ProcessOneAppendItem so single + batch follow identical
            // contract semantics.
            RemoteAppendResponse::Status status =
                ProcessOneAppendItem(req, /*isBatch=*/false);
            SendAppendResponse(packet, status);
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
                SendBatchAppendResponse(packet, 0, 1);
                return;
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
                        // Per-item idempotency: dedup hit replays the cached
                        // status without re-invoking the callback; miss
                        // executes the callback and records the result.
                        // online-insert-idempotency-on-retry: this closes the
                        // gap left by the prim/op-id-idempotency primitive,
                        // which only handled the single-item path.
                        RemoteAppendResponse::Status itemStatus =
                            ProcessOneAppendItem(req, /*isBatch=*/true);
                        if (itemStatus == RemoteAppendResponse::Status::Success)
                            successCount.fetch_add(1);
                        else
                            failCount.fetch_add(1);
                    }
                });
            }
            for (auto& t : workers) t.join();

            SendBatchAppendResponse(packet, successCount, failCount);
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

        void SendAppendResponse(Socket::Packet& srcPacket, RemoteAppendResponse::Status status) {
            RemoteAppendResponse resp;
            resp.m_status = status;

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
            std::uint32_t successCount, std::uint32_t failCount) {
            BatchRemoteAppendResponse resp;
            resp.m_successCount = successCount;
            resp.m_failCount = failCount;

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
        // Dedup hit counters split by path so tests can assert which
        // path (single vs batch) actually deduplicated. Bumped from
        // ProcessOneAppendItem.
        std::atomic<std::uint64_t> m_singleDedupHits{0};
        std::atomic<std::uint64_t> m_batchDedupHits{0};

        // Per-item dedup decision used by both HandleAppendRequest and
        // HandleBatchAppendRequest. Returns the status to emit for the
        // item: cached status on dedup hit, otherwise the status produced
        // by running m_appendCallback. On a fresh execution the result is
        // recorded into m_appendDedupCache so a subsequent retry with the
        // same OpId replays it.
        RemoteAppendResponse::Status ProcessOneAppendItem(
            RemoteAppendRequest& req, bool isBatch)
        {
            const Distributed::OpId& incomingOpId = req.m_opId;

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
                    if (isBatch) m_batchDedupHits.fetch_add(1, std::memory_order_relaxed);
                    else         m_singleDedupHits.fetch_add(1, std::memory_order_relaxed);
                    return static_cast<RemoteAppendResponse::Status>(cached.status);
                }
            }

            ErrorCode result = ErrorCode::Fail;
            if (m_appendCallback) {
                auto headVec = std::make_shared<std::string>(std::move(req.m_headVec));
                result = m_appendCallback(
                    req.m_headID, headVec, req.m_appendNum, req.m_appendPosting);
            }
            RemoteAppendResponse::Status status =
                (result == ErrorCode::Success)
                    ? RemoteAppendResponse::Status::Success
                    : RemoteAppendResponse::Status::Failed;

            if (m_appendDedupCache && incomingOpId.IsValid()) {
                Distributed::AppendDedupResult cachedResult;
                cachedResult.status = static_cast<std::uint8_t>(status);
                m_appendDedupCache->Insert(incomingOpId, cachedResult);
            }
            return status;
        }

        // Stamp every item that arrived with an invalid OpId. Items whose
        // OpId is already valid are left as-is so that retries (the second
        // attempt of SendBatchRemoteAppend, or items recycled from a higher
        // level after a failed flush) keep the *same* id, which is the only
        // way the receiver's dedup cache can recognise them as duplicates.
        // No-op if idempotency was never configured (m_opIdAllocator's
        // senderId stays -1; allocator returns invalid ids).
        void StampBatchOpIdsLocked(std::vector<RemoteAppendRequest>& items) {
            if (m_opIdAllocator.SenderId() < 0) return;
            for (auto& it : items) {
                if (!it.m_opId.IsValid()) it.m_opId = m_opIdAllocator.Next();
            }
        }
    };

} // namespace SPTAG::SPANN
