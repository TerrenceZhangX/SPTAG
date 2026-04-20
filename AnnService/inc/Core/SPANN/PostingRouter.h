// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_POSTINGROUTER_H_
#define _SPTAG_SPANN_POSTINGROUTER_H_

#include "inc/Core/SPANN/DistributedProtocol.h"
#include "inc/Core/SPANN/ConsistentHashRing.h"
#include "inc/Core/SPANN/DispatchCoordinator.h"
#include "inc/Core/SPANN/RemotePostingOps.h"
#include "inc/Helper/KeyValueIO.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Socket/Client.h"
#include "inc/Socket/Server.h"
#include "inc/Socket/Packet.h"
#include "inc/Socket/SimpleSerialization.h"
#include <string>
#include <unordered_map>
#include <map>
#include <set>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <vector>
#include <atomic>
#include <future>
#include <functional>
#include <thread>

namespace SPTAG::SPANN {

    /// Distributed routing layer for SPFRESH posting writes.
    ///
    /// Architecture: N compute nodes, each with a full head index replica,
    /// sharing a TiKV cluster for posting storage. When a write arrives,
    /// PostingRouter determines which compute node should handle it using
    /// consistent hashing on headID, ensuring minimal key remapping on
    /// node join/leave.
    ///
    /// This guarantees that the same headID always routes to the same compute
    /// node, enabling local per-headID locking for Append/Split/Merge correctness.

    class PostingRouter : public DispatchCoordinator::PeerNetwork,
                         public RemotePostingOps::NetworkAccess {
    public:
        using AppendCallback = RemotePostingOps::AppendCallback;

        /// Callback for executing a dispatch command on a worker node.
        /// Delegated to DispatchCoordinator.
        using DispatchCallback = DispatchCoordinator::DispatchCallback;

        using HeadSyncCallback = RemotePostingOps::HeadSyncCallback;
        using RemoteLockCallback = RemotePostingOps::RemoteLockCallback;

        PostingRouter()
            : m_enabled(false), m_localNodeIndex(-1) {}

        ~PostingRouter() {
            m_bgConnectStop.store(true);
            if (m_bgConnectThread.joinable()) m_bgConnectThread.join();
        }

        /// Initialize the router from configuration.
        /// @param p_db         The KeyValueIO backend (for GetKeyLocation).
        /// @param localNodeIdx Index of this node in the compute node list.
        /// @param nodeAddrs    "host:port" pairs for all compute nodes' router ports.
        /// @param nodeStores   TiKV store addresses. Can be >= nodeAddrs.size();
        ///                     stores are assigned to nodes round-robin.
        /// @param vnodeCount   Virtual nodes per physical node for consistent hashing (default 150).
        bool Initialize(
            std::shared_ptr<Helper::KeyValueIO> p_db,
            int localNodeIdx,
            const std::vector<std::pair<std::string, std::string>>& nodeAddrs,
            const std::vector<std::string>& nodeStores,
            int vnodeCount = 150)
        {
            if (nodeAddrs.empty() || localNodeIdx < 0 ||
                localNodeIdx >= static_cast<int>(nodeAddrs.size()) ||
                nodeStores.empty()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter::Initialize invalid config: %d nodes, localIdx=%d, %d stores\n",
                    (int)nodeAddrs.size(), localNodeIdx, (int)nodeStores.size());
                return false;
            }

            m_db = p_db;
            m_localNodeIndex = localNodeIdx;
            m_nodeAddrs = nodeAddrs;
            m_nodeStores = nodeStores;

            // Build store → node list mapping (sub-partitioned)
            // Each node is assigned to one store round-robin, so multiple nodes
            // can share a store. Within a store, headID % nodesPerStore picks the owner.
            int numNodes = static_cast<int>(nodeAddrs.size());
            int numStores = static_cast<int>(nodeStores.size());
            for (int nodeIdx = 0; nodeIdx < numNodes; nodeIdx++) {
                int storeIdx = nodeIdx % numStores;
                m_storeToNodes[nodeStores[storeIdx]].push_back(nodeIdx);
            }

            for (auto& [store, nodes] : m_storeToNodes) {
                std::string nodeList;
                for (int n : nodes) { nodeList += std::to_string(n) + " "; }
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                    "PostingRouter: store %s → nodes [%s] (%d nodes)\n",
                    store.c_str(), nodeList.c_str(), (int)nodes.size());
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "PostingRouter: %d stores mapped to %d nodes (sub-partitioned)\n",
                numStores, numNodes);

            // Build consistent hash ring (lock-free via atomic shared_ptr)
            {
                auto ring = std::make_shared<ConsistentHashRing>(vnodeCount);
                for (int i = 0; i < numNodes; i++) {
                    ring->AddNode(i);
                }
                std::atomic_store(&m_hashRing, std::shared_ptr<const ConsistentHashRing>(std::move(ring)));
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "PostingRouter: Consistent hash ring built with %d nodes, %d vnodes/node\n",
                numNodes, vnodeCount);

            m_enabled = true;

            // Wire up dispatch coordinator and remote posting ops
            m_dispatch.SetNetwork(this);
            m_remoteOps.SetNetwork(this);

            return true;
        }

        /// Set the callback for handling appends locally (called for incoming RPCs).
        void SetAppendCallback(AppendCallback cb) {
            m_remoteOps.SetAppendCallback(std::move(cb));
        }

        /// Set the callback for applying head sync entries on the local head index.
        void SetHeadSyncCallback(HeadSyncCallback cb) {
            m_remoteOps.SetHeadSyncCallback(std::move(cb));
        }

        /// Set the callback for remote lock/unlock of headIDs on this node.
        void SetRemoteLockCallback(RemoteLockCallback cb) {
            m_remoteOps.SetRemoteLockCallback(std::move(cb));
        }

        /// Set the callback for dispatch commands (worker side).
        void SetDispatchCallback(DispatchCallback cb) {
            m_dispatch.SetDispatchCallback(std::move(cb));
        }

        /// Clear the dispatch callback and wait for in-flight dispatch
        /// threads to complete. Call before destroying callback state.
        void ClearDispatchCallback() {
            m_dispatch.ClearDispatchCallback();
        }

        /// Start the router server (listens for incoming remote appends)
        /// and connect client to all peer nodes.
        bool Start() {
            if (!m_enabled) return false;

            // --- Server side: listen for incoming requests ---
            Socket::PacketHandlerMapPtr serverHandlers(new Socket::PacketHandlerMap);
            serverHandlers->emplace(Socket::PacketType::AppendRequest,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_remoteOps.HandleAppendRequest(connID, std::move(packet));
                });
            serverHandlers->emplace(Socket::PacketType::BatchAppendRequest,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_remoteOps.HandleBatchAppendRequest(connID, std::move(packet));
                });
            serverHandlers->emplace(Socket::PacketType::HeadSyncRequest,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_remoteOps.HandleHeadSyncRequest(connID, std::move(packet));
                });
            serverHandlers->emplace(Socket::PacketType::RemoteLockRequest,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_remoteOps.HandleRemoteLockRequest(connID, std::move(packet));
                });
            serverHandlers->emplace(Socket::PacketType::DispatchCommand,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_dispatch.HandleDispatchCommand(connID, std::move(packet));
                });
            serverHandlers->emplace(Socket::PacketType::DispatchResult,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_dispatch.HandleDispatchResult(connID, std::move(packet));
                });

            const auto& localAddr = m_nodeAddrs[m_localNodeIndex];
            m_server.reset(new Socket::Server(
                localAddr.first, localAddr.second, serverHandlers, 2));
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "PostingRouter server listening on %s:%s\n",
                localAddr.first.c_str(), localAddr.second.c_str());

            // --- Client side: handle responses ---
            Socket::PacketHandlerMapPtr clientHandlers(new Socket::PacketHandlerMap);
            clientHandlers->emplace(Socket::PacketType::AppendResponse,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_remoteOps.HandleAppendResponse(connID, std::move(packet));
                });
            clientHandlers->emplace(Socket::PacketType::BatchAppendResponse,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_remoteOps.HandleBatchAppendResponse(connID, std::move(packet));
                });
            clientHandlers->emplace(Socket::PacketType::RemoteLockResponse,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_remoteOps.HandleRemoteLockResponse(connID, std::move(packet));
                });
            clientHandlers->emplace(Socket::PacketType::DispatchResult,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    m_dispatch.HandleDispatchResult(connID, std::move(packet));
                });

            m_client.reset(new Socket::Client(clientHandlers, 2, 30));

            m_peerConnections.resize(m_nodeAddrs.size(), Socket::c_invalidConnectionID);

            // Launch background thread that keeps retrying failed peer connections.
            // This handles the case where workers start before the driver's router
            // is listening — the thread will keep trying until all peers are connected.
            m_bgConnectStop.store(false);
            m_bgConnectThread = std::thread([this]() {
                int numNodes = static_cast<int>(m_nodeAddrs.size());
                int delayMs = 500;
                while (!m_bgConnectStop.load()) {
                    bool allConnected = true;
                    for (int i = 0; i < numNodes; i++) {
                        if (i == m_localNodeIndex) continue;
                        {
                            std::lock_guard<std::mutex> lock(m_connMutex);
                            if (m_peerConnections[i] != Socket::c_invalidConnectionID)
                                continue;
                        }
                        allConnected = false;
                        ConnectToPeer(i, 1, 0);
                    }
                    if (allConnected) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                            "PostingRouter: All peer connections established\n");
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
                    delayMs = std::min(delayMs + 500, 5000);
                }
            });

            return true;
        }

        /// Determine whether a headID should be handled by this node.
        /// Uses consistent hashing: headID → hash ring → Owner Node.
        /// Minimal key remapping when nodes are added/removed (~1/N affected).
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
                "PostingRouter stats%s: local=%d remote=%d disabled=%d keyMiss=%d noMapping=%d\n",
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

        /// Dynamically add a new compute node to the consistent hash ring.
        /// Connects to the new peer and updates the ring. After this call,
        /// some headIDs previously owned by existing nodes will route to the new node.
        /// Use ComputeMigration() to determine which headIDs need to be migrated.
        /// @param nodeIndex  The index for the new node (must not already exist).
        /// @param addr       host:port pair for the new node's router port.
        /// @param store      TiKV store address for the new node.
        /// @return true if the node was added successfully.
        bool AddNode(int nodeIndex, const std::pair<std::string, std::string>& addr,
                     const std::string& store) {
            {
                std::lock_guard<std::mutex> guard(m_ringWriteMutex);
                auto oldRing = std::atomic_load(&m_hashRing);
                if (oldRing && oldRing->HasNode(nodeIndex)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                        "PostingRouter::AddNode: node %d already exists\n", nodeIndex);
                    return false;
                }
                auto newRing = oldRing
                    ? std::make_shared<ConsistentHashRing>(*oldRing)
                    : std::make_shared<ConsistentHashRing>();
                newRing->AddNode(nodeIndex);
                std::atomic_store(&m_hashRing, std::shared_ptr<const ConsistentHashRing>(std::move(newRing)));
            }

            // Expand connection and address arrays if needed
            {
                std::lock_guard<std::mutex> lock(m_connMutex);
                if (nodeIndex >= static_cast<int>(m_nodeAddrs.size())) {
                    m_nodeAddrs.resize(nodeIndex + 1);
                    m_peerConnections.resize(nodeIndex + 1, Socket::c_invalidConnectionID);
                }
                m_nodeAddrs[nodeIndex] = addr;
                if (!store.empty()) {
                    m_storeToNodes[store].push_back(nodeIndex);
                }
            }

            // Connect to the new peer
            if (nodeIndex != m_localNodeIndex) {
                ConnectToPeer(nodeIndex, 5, 1000);
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "PostingRouter::AddNode: node %d (%s:%s) added to ring, total nodes=%d\n",
                nodeIndex, addr.first.c_str(), addr.second.c_str(),
                GetNumNodes());
            return true;
        }

        /// Remove a compute node from the consistent hash ring.
        /// HeadIDs owned by this node will be redistributed to remaining nodes.
        /// No data migration is performed (the node's data is considered lost).
        /// @param nodeIndex  The index of the node to remove.
        void RemoveNode(int nodeIndex) {
            {
                std::lock_guard<std::mutex> guard(m_ringWriteMutex);
                auto oldRing = std::atomic_load(&m_hashRing);
                if (!oldRing || !oldRing->HasNode(nodeIndex)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                        "PostingRouter::RemoveNode: node %d not in ring\n", nodeIndex);
                    return;
                }
                auto newRing = std::make_shared<ConsistentHashRing>(*oldRing);
                newRing->RemoveNode(nodeIndex);
                std::atomic_store(&m_hashRing, std::shared_ptr<const ConsistentHashRing>(std::move(newRing)));
            }

            // Invalidate connection
            InvalidatePeerConnection(nodeIndex);

            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "PostingRouter::RemoveNode: node %d removed from ring, remaining nodes=%d\n",
                nodeIndex, GetNumNodes());
        }

        /// Given a list of headIDs that this node currently owns locally,
        /// compute which ones need to be migrated to other nodes.
        /// Returns a map of targetNodeIndex → vector of headIDs to send there.
        /// Typical usage after AddNode(): enumerate local headIDs, call this,
        /// then send posting data for each headID to its new owner.
        std::unordered_map<int, std::vector<SizeType>> ComputeMigration(
            const std::vector<SizeType>& localHeadIDs) const {
            auto ring = std::atomic_load(&m_hashRing);
            std::unordered_map<int, std::vector<SizeType>> result;
            if (!ring) return result;
            for (SizeType hid : localHeadIDs) {
                int owner = ring->GetOwner(hid);
                if (owner != m_localNodeIndex) {
                    result[owner].push_back(hid);
                }
            }
            return result;
        }

        /// Get a snapshot of the consistent hash ring (for inspection/debugging).
        std::shared_ptr<const ConsistentHashRing> GetHashRing() const {
            return std::atomic_load(&m_hashRing);
        }

        /// Send an append request to a remote compute node (delegated to RemotePostingOps).
        ErrorCode SendRemoteAppend(
            int targetNodeIndex,
            SizeType headID,
            const std::shared_ptr<std::string>& headVec,
            int appendNum,
            std::string& appendPosting)
        {
            return m_remoteOps.SendRemoteAppend(
                targetNodeIndex, headID, headVec, appendNum, appendPosting);
        }

        bool IsEnabled() const { return m_enabled; }

        /// Block until all outbound peer connections are established.
        /// Call from the driver after workers are ready, before dispatching.
        /// Returns true if all peers connected within the timeout.
        bool WaitForAllPeersConnected(int timeoutSec = 120) {
            if (!m_enabled) return true;
            int numNodes = static_cast<int>(m_nodeAddrs.size());
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);

            while (std::chrono::steady_clock::now() < deadline) {
                bool allConnected = true;
                for (int i = 0; i < numNodes; i++) {
                    if (i == m_localNodeIndex) continue;
                    std::lock_guard<std::mutex> lock(m_connMutex);
                    if (m_peerConnections[i] == Socket::c_invalidConnectionID) {
                        allConnected = false;
                        break;
                    }
                }
                if (allConnected) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "PostingRouter: All %d peers connected\n", numNodes - 1);
                    return true;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                "PostingRouter: Timed out waiting for peer connections (%ds)\n", timeoutSec);
            return false;
        }

        /// Send a batch of append requests to a remote node (delegated to RemotePostingOps).
        ErrorCode SendBatchRemoteAppend(
            int targetNodeIndex,
            std::vector<RemoteAppendRequest>& items)
        {
            return m_remoteOps.SendBatchRemoteAppend(targetNodeIndex, items);
        }

        // ---- Dispatch protocol (delegated to DispatchCoordinator) ----

        std::uint64_t BroadcastDispatchCommand(DispatchCommand::Type type, std::uint32_t round) {
            return m_dispatch.BroadcastDispatchCommand(type, round);
        }

        std::vector<double> WaitForAllResults(std::uint64_t dispatchId, int timeoutSec = 300) {
            return m_dispatch.WaitForAllResults(dispatchId, timeoutSec);
        }

        // ---- PeerNetwork interface (used by DispatchCoordinator) ----

        int GetLocalNodeIndex() const override { return m_localNodeIndex; }

        int GetNumNodes() const override {
            auto ring = std::atomic_load(&m_hashRing);
            return ring ? static_cast<int>(ring->NodeCount()) : 0;
        }

        Socket::ConnectionID GetPeerConnection(int nodeIndex) override {
            {
                std::lock_guard<std::mutex> lock(m_connMutex);
                if (m_peerConnections[nodeIndex] != Socket::c_invalidConnectionID)
                    return m_peerConnections[nodeIndex];
            }
            if (ConnectToPeer(nodeIndex, 5, 1000)) {
                std::lock_guard<std::mutex> lock(m_connMutex);
                return m_peerConnections[nodeIndex];
            }
            return Socket::c_invalidConnectionID;
        }

        void SendPacket(Socket::ConnectionID connID, Socket::Packet&& pkt,
                        std::function<void(bool)> callback) override {
            m_client->SendPacket(connID, std::move(pkt), std::move(callback));
        }

        /// Connect to a peer compute node with retry and exponential backoff.
        bool ConnectToPeer(int nodeIndex, int maxRetries = 10, int initialDelayMs = 500) {
            if (nodeIndex == m_localNodeIndex) return true;

            // Snapshot address under lock to avoid racing with AddNode resizing m_nodeAddrs
            std::pair<std::string, std::string> addr;
            {
                std::lock_guard<std::mutex> lock(m_connMutex);
                if (nodeIndex >= static_cast<int>(m_nodeAddrs.size())) return false;
                addr = m_nodeAddrs[nodeIndex];
            }

            int delayMs = initialDelayMs;
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                ErrorCode ec;
                auto connID = m_client->ConnectToServer(addr.first, addr.second, ec);
                if (ec == ErrorCode::Success) {
                    std::lock_guard<std::mutex> lock(m_connMutex);
                    m_peerConnections[nodeIndex] = connID;
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "PostingRouter: Connected to node %d (%s:%s), connID=%u (attempt %d)\n",
                        nodeIndex, addr.first.c_str(), addr.second.c_str(),
                        connID, attempt);
                    return true;
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "PostingRouter: Failed to connect to node %d (%s:%s), attempt %d/%d, retrying in %dms\n",
                    nodeIndex, addr.first.c_str(), addr.second.c_str(),
                    attempt, maxRetries, delayMs);
                std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
                delayMs = std::min(delayMs * 2, 5000);
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                "PostingRouter: All %d connection attempts to node %d (%s:%s) failed\n",
                maxRetries, nodeIndex, addr.first.c_str(), addr.second.c_str());
            return false;
        }

        /// Invalidate a cached peer connection so the next GetPeerConnection reconnects.
        void InvalidatePeerConnection(int nodeIndex) override {
            std::lock_guard<std::mutex> lock(m_connMutex);
            m_peerConnections[nodeIndex] = Socket::c_invalidConnectionID;
        }

        // ---- Internal posting ops (delegated to RemotePostingOps) ----

        void BroadcastHeadSync(const std::vector<HeadSyncEntry>& entries) {
            if (!m_enabled) return;
            m_remoteOps.BroadcastHeadSync(entries);
        }

        bool SendRemoteLock(int nodeIndex, SizeType headID, bool lock) {
            if (!m_enabled) return false;
            return m_remoteOps.SendRemoteLock(nodeIndex, headID, lock);
        }

        // ---- NetworkAccess interface (used by RemotePostingOps) ----

        Socket::Client* GetClient() override { return m_client.get(); }
        Socket::Server* GetServer() override { return m_server.get(); }

        bool m_enabled;
        int m_localNodeIndex;
        std::shared_ptr<Helper::KeyValueIO> m_db;

        // Consistent hash ring for headID → node routing (lock-free RCU)
        // Readers: atomic_load gives a snapshot, zero overhead
        // Writers: copy-on-write under m_ringWriteMutex, then atomic_store
        std::shared_ptr<const ConsistentHashRing> m_hashRing;
        std::mutex m_ringWriteMutex;  // serializes AddNode/RemoveNode (rare)

        // Node configuration
        std::vector<std::pair<std::string, std::string>> m_nodeAddrs;  // host:port per node
        std::vector<std::string> m_nodeStores;  // TiKV store addr per node
        std::unordered_map<std::string, std::vector<int>> m_storeToNodes;  // store addr → node indices (sub-partitioned)

        // Server (receives remote append requests)
        std::unique_ptr<Socket::Server> m_server;

        // Client (sends remote append requests to peers)
        std::unique_ptr<Socket::Client> m_client;
        std::mutex m_connMutex;
        std::vector<Socket::ConnectionID> m_peerConnections;

        // Background peer connection thread
        std::thread m_bgConnectThread;
        std::atomic<bool> m_bgConnectStop{false};

        // Delegated subsystems
        RemotePostingOps m_remoteOps;       // Internal posting RPCs (append/headsync/lock)
        DispatchCoordinator m_dispatch;     // External dispatch (driver↔worker coordination)

        // Batched remote append queue (queue items from multiple AddIndex calls, flush once)
        mutable std::mutex m_appendQueueMutex;
        std::unordered_map<int, std::vector<RemoteAppendRequest>> m_appendQueue;
        std::atomic<size_t> m_remoteQueueSize{0};

        // Routing statistics
        struct RouteStats {
            std::atomic<int> local{0};
            std::atomic<int> remote{0};
            std::atomic<int> disabled{0};
            std::atomic<int> keyMiss{0};
            std::atomic<int> noMapping{0};
        } m_routeStats;

    public:
        /// Queue a remote append for batched sending (thread-safe).
        void QueueRemoteAppend(int nodeIndex, RemoteAppendRequest req) {
            std::lock_guard<std::mutex> lock(m_appendQueueMutex);
            m_appendQueue[nodeIndex].push_back(std::move(req));
            m_remoteQueueSize.fetch_add(1, std::memory_order_relaxed);
        }

        /// Get total number of queued remote appends (lock-free).
        size_t GetRemoteQueueSize() const {
            return m_remoteQueueSize.load(std::memory_order_relaxed);
        }

        /// Flush all queued remote appends, sending one batch per target node in parallel.
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

    };

} // namespace SPTAG::SPANN

#endif // _SPTAG_SPANN_POSTINGROUTER_H_
