// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_WORKERNODE_H_
#define _SPTAG_SPANN_WORKERNODE_H_

#include "inc/Core/SPANN/Distributed/NetworkNode.h"
#include "inc/Helper/KeyValueIO.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Socket/SimpleSerialization.h"
#include <string>
#include <unordered_map>
#include <map>
#include <set>
#include <functional>
#include <future>

namespace SPTAG::SPANN {

    /// Distributed compute worker node.
    ///
    /// Responsibilities:
    ///   - Route headIDs to owner nodes via consistent hash ring
    ///   - Queue and flush remote appends (batched RPC)
    ///   - HeadSync broadcast and remote locking
    ///   - Register with dispatcher and receive ring updates
    ///   - Handle incoming dispatch commands from the driver
    class WorkerNode : public NetworkNode {
    public:
        using AppendCallback = RemotePostingOps::AppendCallback;
        using DispatchCallback = DispatchCoordinator::DispatchCallback;
        using HeadSyncCallback = RemotePostingOps::HeadSyncCallback;
        using RemoteLockCallback = RemotePostingOps::RemoteLockCallback;

        /// Initialize with separate dispatcher/worker/store addresses.
        /// workerIndex is 0-based (0 = driver/local, 1+ = remote).
        /// Internal node index = workerIndex + 1 (0 is reserved for dispatcher).
        bool Initialize(
            std::shared_ptr<Helper::KeyValueIO> p_db,
            int workerIndex,
            const std::pair<std::string, std::string>& dispatcherAddr,
            const std::vector<std::pair<std::string, std::string>>& workerAddrs,
            const std::vector<std::string>& storeAddrs,
            int vnodeCount = 150)
        {
            if (storeAddrs.empty()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "WorkerNode::Initialize: storeAddrs is empty\n");
                return false;
            }

            // Build combined addr list: [dispatcher, worker0, worker1, ...]
            std::vector<std::pair<std::string, std::string>> allAddrs;
            allAddrs.push_back(dispatcherAddr);
            allAddrs.insert(allAddrs.end(), workerAddrs.begin(), workerAddrs.end());

            int internalIdx = workerIndex + 1;  // 0 = dispatcher, 1..N = workers
            if (!InitializeNetwork(internalIdx, allAddrs, vnodeCount)) return false;

            m_db = p_db;
            m_nodeStores = storeAddrs;

            // Build store → node list mapping (worker internal indices 1..N)
            int numWorkers = static_cast<int>(workerAddrs.size());
            int numStores = static_cast<int>(storeAddrs.size());
            for (int wi = 0; wi < numWorkers; wi++) {
                int storeIdx = wi % numStores;
                m_storeToNodes[storeAddrs[storeIdx]].push_back(wi + 1);
            }
            for (auto& [store, nodes] : m_storeToNodes) {
                std::string nodeList;
                for (int n : nodes) { nodeList += std::to_string(n) + " "; }
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                    "WorkerNode: store %s → nodes [%s]\n", store.c_str(), nodeList.c_str());
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "WorkerNode: initialized (workerIndex=%d, internalIdx=%d, %d stores, %d vnodes/node)\n",
                workerIndex, internalIdx, numStores, vnodeCount);

            m_dispatch.SetNetwork(this);
            m_remoteOps.SetNetwork(this);
            return true;
        }

        bool Start() { return StartNetwork(); }

        // ---- Callbacks ----

        void SetAppendCallback(AppendCallback cb) { m_remoteOps.SetAppendCallback(std::move(cb)); }
        void SetHeadSyncCallback(HeadSyncCallback cb) { m_remoteOps.SetHeadSyncCallback(std::move(cb)); }
        void SetRemoteLockCallback(RemoteLockCallback cb) { m_remoteOps.SetRemoteLockCallback(std::move(cb)); }
        void SetDispatchCallback(DispatchCallback cb) { m_dispatch.SetDispatchCallback(std::move(cb)); }
        void ClearDispatchCallback() { m_dispatch.ClearDispatchCallback(); }

        // ---- Routing ----

        RouteTarget GetOwner(SizeType headID) {
            RouteTarget target;
            target.isLocal = true;
            target.nodeIndex = m_localNodeIndex;

            if (!m_enabled) {
                m_routeStats.disabled++;
                return target;
            }
            {
                auto ring = std::atomic_load(&m_hashRing);
                if (!ring || ring->NodeCount() <= 1) {
                    m_routeStats.local++;
                    return target;
                }
                target.nodeIndex = ring->GetOwner(headID);
            }
            target.isLocal = (target.nodeIndex == m_localNodeIndex);
            if (target.isLocal) m_routeStats.local++;
            else m_routeStats.remote++;
            return target;
        }

        void LogRouteStats(const char* context = "") {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "WorkerNode stats%s: local=%d remote=%d disabled=%d keyMiss=%d noMapping=%d\n",
                context, (int)m_routeStats.local, (int)m_routeStats.remote,
                (int)m_routeStats.disabled, (int)m_routeStats.keyMiss,
                (int)m_routeStats.noMapping);
        }

        void ResetRouteStats() {
            m_routeStats.local.store(0);
            m_routeStats.remote.store(0);
            m_routeStats.disabled.store(0);
            m_routeStats.keyMiss.store(0);
            m_routeStats.noMapping.store(0);
        }

        // ---- Remote posting ops ----

        ErrorCode SendRemoteAppend(int targetNodeIndex, SizeType headID,
            const std::shared_ptr<std::string>& headVec, int appendNum,
            std::string& appendPosting)
        {
            return m_remoteOps.SendRemoteAppend(targetNodeIndex, headID, headVec, appendNum, appendPosting);
        }

        ErrorCode SendBatchRemoteAppend(int targetNodeIndex, std::vector<RemoteAppendRequest>& items) {
            return m_remoteOps.SendBatchRemoteAppend(targetNodeIndex, items);
        }

        void BroadcastHeadSync(const std::vector<HeadSyncEntry>& entries) {
            if (!m_enabled) return;
            m_remoteOps.BroadcastHeadSync(entries);
        }

        bool SendRemoteLock(int nodeIndex, SizeType headID, bool lock) {
            if (!m_enabled) return false;
            return m_remoteOps.SendRemoteLock(nodeIndex, headID, lock);
        }

        // ---- Append queue ----

        void QueueRemoteAppend(int nodeIndex, RemoteAppendRequest req) {
            std::lock_guard<std::mutex> lock(m_appendQueueMutex);
            m_appendQueue[nodeIndex].push_back(std::move(req));
            m_remoteQueueSize.fetch_add(1, std::memory_order_relaxed);
        }

        size_t GetRemoteQueueSize() const {
            return m_remoteQueueSize.load(std::memory_order_relaxed);
        }

        ErrorCode FlushRemoteAppends() {
            std::unordered_map<int, std::vector<RemoteAppendRequest>> toSend;
            {
                std::lock_guard<std::mutex> lock(m_appendQueueMutex);
                toSend.swap(m_appendQueue);
                m_remoteQueueSize.store(0, std::memory_order_relaxed);
            }
            if (toSend.empty()) return ErrorCode::Success;

            std::atomic<int> errors{0};
            std::vector<std::thread> threads;
            for (auto& [nodeIdx, items] : toSend) {
                if (items.empty()) continue;
                threads.emplace_back([this, &errors, nodeIdx, &items]() {
                    ErrorCode ret = SendBatchRemoteAppend(nodeIdx, items);
                    if (ret != ErrorCode::Success) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                            "FlushRemoteAppends: batch to node %d failed (%d items)\n",
                            nodeIdx, (int)items.size());
                        errors++;
                    }
                });
            }
            for (auto& t : threads) t.join();
            return errors > 0 ? ErrorCode::Fail : ErrorCode::Success;
        }

        // ---- Ring protocol (worker side) ----

        bool WaitForRing(int timeoutSec = 120) {
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
            while (std::chrono::steady_clock::now() < deadline) {
                auto ring = std::atomic_load(&m_hashRing);
                if (ring && ring->NodeCount() > 0) return true;
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                "WorkerNode: Timed out waiting for ring (%ds)\n", timeoutSec);
            return false;
        }

        // ---- Data members (public for ExtraDynamicSearcher access) ----

        std::shared_ptr<Helper::KeyValueIO> m_db;
        std::vector<std::string> m_nodeStores;
        std::unordered_map<std::string, std::vector<int>> m_storeToNodes;

        struct RouteStats {
            std::atomic<int> local{0};
            std::atomic<int> remote{0};
            std::atomic<int> disabled{0};
            std::atomic<int> keyMiss{0};
            std::atomic<int> noMapping{0};
        } m_routeStats;

    protected:
        void RegisterServerHandlers(Socket::PacketHandlerMapPtr& handlers) override {
            handlers->emplace(Socket::PacketType::AppendRequest,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_remoteOps.HandleAppendRequest(c, std::move(p)); });
            handlers->emplace(Socket::PacketType::BatchAppendRequest,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_remoteOps.HandleBatchAppendRequest(c, std::move(p)); });
            handlers->emplace(Socket::PacketType::HeadSyncRequest,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_remoteOps.HandleHeadSyncRequest(c, std::move(p)); });
            handlers->emplace(Socket::PacketType::RemoteLockRequest,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_remoteOps.HandleRemoteLockRequest(c, std::move(p)); });
            handlers->emplace(Socket::PacketType::DispatchCommand,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_dispatch.HandleDispatchCommand(c, std::move(p)); });
            handlers->emplace(Socket::PacketType::DispatchResult,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_dispatch.HandleDispatchResult(c, std::move(p)); });
            handlers->emplace(Socket::PacketType::RingUpdate,
                [this](Socket::ConnectionID c, Socket::Packet p) { HandleRingUpdate(c, std::move(p)); });
        }

        void RegisterClientHandlers(Socket::PacketHandlerMapPtr& handlers) override {
            handlers->emplace(Socket::PacketType::AppendResponse,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_remoteOps.HandleAppendResponse(c, std::move(p)); });
            handlers->emplace(Socket::PacketType::BatchAppendResponse,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_remoteOps.HandleBatchAppendResponse(c, std::move(p)); });
            handlers->emplace(Socket::PacketType::RemoteLockResponse,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_remoteOps.HandleRemoteLockResponse(c, std::move(p)); });
            handlers->emplace(Socket::PacketType::DispatchResult,
                [this](Socket::ConnectionID c, Socket::Packet p) { m_dispatch.HandleDispatchResult(c, std::move(p)); });
        }

        void BgProtocolStep() override {
            // Keep sending NodeRegister until ring is populated
            auto ring = std::atomic_load(&m_hashRing);
            if (!ring || ring->NodeCount() == 0) {
                Socket::ConnectionID connID = Socket::c_invalidConnectionID;
                {
                    std::lock_guard<std::mutex> lock(m_connMutex);
                    if (m_dispatcherNodeIndex < (int)m_peerConnections.size())
                        connID = m_peerConnections[m_dispatcherNodeIndex];
                }
                if (connID != Socket::c_invalidConnectionID) {
                    SendNodeRegister();
                }
            }
        }

        bool IsRingSettled() const override {
            auto ring = std::atomic_load(&m_hashRing);
            return ring && ring->NodeCount() > 0;
        }

    private:
        void SendNodeRegister() {
            NodeRegisterMsg msg;
            msg.m_nodeIndex = m_localNodeIndex;
            msg.m_host = m_nodeAddrs[m_localNodeIndex].first;
            msg.m_port = m_nodeAddrs[m_localNodeIndex].second;
            // Worker's 0-based index = m_localNodeIndex - 1 (since 0 is dispatcher)
            int workerIdx = m_localNodeIndex - 1;
            int numStores = static_cast<int>(m_nodeStores.size());
            msg.m_store = (numStores > 0) ? m_nodeStores[workerIdx % numStores] : "";

            std::size_t bodySize = msg.EstimateBufferSize();
            Socket::Packet pkt;
            pkt.Header().m_packetType = Socket::PacketType::NodeRegisterRequest;
            pkt.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
            pkt.Header().m_connectionID = Socket::c_invalidConnectionID;
            pkt.Header().m_resourceID = 0;
            pkt.Header().m_bodyLength = static_cast<std::uint32_t>(bodySize);
            pkt.AllocateBuffer(static_cast<std::uint32_t>(bodySize));
            msg.Write(pkt.Body());
            pkt.Header().WriteBuffer(pkt.HeaderBuffer());

            auto connID = GetPeerConnection(m_dispatcherNodeIndex);
            if (connID != Socket::c_invalidConnectionID) {
                m_client->SendPacket(connID, std::move(pkt), nullptr);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                    "WorkerNode: Sent NodeRegister (node %d) to dispatcher\n", m_localNodeIndex);
            }
        }

        void HandleRingUpdate(Socket::ConnectionID connID, Socket::Packet packet) {
            RingUpdateMsg msg;
            if (!msg.Read(packet.Body())) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "WorkerNode: Failed to parse RingUpdate\n");
                return;
            }

            auto newRing = std::make_shared<ConsistentHashRing>(msg.m_vnodeCount);
            for (auto idx : msg.m_nodeIndices) {
                newRing->AddNode(idx);
            }
            // Tag with the dispatcher-authoritative epoch (epoch = ringVersion,
            // ringRev = optional in-epoch monotonic, 0 today).
            newRing->SetEpoch(msg.AsEpoch());
            {
                std::lock_guard<std::mutex> guard(m_ringWriteMutex);
                std::atomic_store(&m_hashRing,
                    std::shared_ptr<const ConsistentHashRing>(std::move(newRing)));
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "WorkerNode: Ring updated — %d nodes (v%u)\n",
                (int)msg.m_nodeIndices.size(), msg.m_ringVersion);

            SendRingUpdateACK(msg.m_ringVersion);
        }

        void SendRingUpdateACK(std::uint32_t ringVersion) {
            RingUpdateACKMsg msg;
            msg.m_nodeIndex = m_localNodeIndex;
            msg.m_ringVersion = ringVersion;

            std::size_t bodySize = msg.EstimateBufferSize();
            Socket::Packet pkt;
            pkt.Header().m_packetType = Socket::PacketType::RingUpdateACK;
            pkt.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
            pkt.Header().m_connectionID = Socket::c_invalidConnectionID;
            pkt.Header().m_resourceID = 0;
            pkt.Header().m_bodyLength = static_cast<std::uint32_t>(bodySize);
            pkt.AllocateBuffer(static_cast<std::uint32_t>(bodySize));
            msg.Write(pkt.Body());
            pkt.Header().WriteBuffer(pkt.HeaderBuffer());

            auto connID = GetPeerConnection(m_dispatcherNodeIndex);
            if (connID != Socket::c_invalidConnectionID) {
                m_client->SendPacket(connID, std::move(pkt), nullptr);
            }
        }

        int m_dispatcherNodeIndex = 0;
        RemotePostingOps m_remoteOps;
        DispatchCoordinator m_dispatch;

        mutable std::mutex m_appendQueueMutex;
        std::unordered_map<int, std::vector<RemoteAppendRequest>> m_appendQueue;
        std::atomic<size_t> m_remoteQueueSize{0};
    };

} // namespace SPTAG::SPANN

#endif // _SPTAG_SPANN_WORKERNODE_H_
