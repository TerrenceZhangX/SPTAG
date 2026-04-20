// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_POSTINGROUTER_H_
#define _SPTAG_SPANN_POSTINGROUTER_H_

#include "inc/Core/SPANN/DistributedProtocol.h"
#include "inc/Core/SPANN/ConsistentHashRing.h"
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

    class PostingRouter {
    public:
        /// Callback type for handling a local append.
        using AppendCallback = std::function<ErrorCode(
            SizeType headID,
            std::shared_ptr<std::string> headVec,
            int appendNum,
            std::string& appendPosting)>;

        /// Callback for executing a dispatch command on a worker node.
        /// Called on a dedicated command-execution thread (not the io_context).
        /// Returns the result to send back to the driver.
        using DispatchCallback = std::function<DispatchResult(const DispatchCommand&)>;

        /// Callback to apply a head sync entry on the local head index.
        /// For Add: call AddHeadIndex with (vectorData, headVID, dim).
        /// For Delete: call DeleteIndex with (headVID, layer).
        using HeadSyncCallback = std::function<void(const HeadSyncEntry& entry)>;

        /// Callback for remote lock: try_lock or unlock a headID on this node.
        /// Returns true if lock was acquired (Lock) or released (Unlock).
        using RemoteLockCallback = std::function<bool(SizeType headID, bool lock)>;

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
            return true;
        }

        /// Set the callback for handling appends locally (called for incoming RPCs).
        void SetAppendCallback(AppendCallback cb) {
            m_appendCallback = std::move(cb);
        }

        /// Set the callback for applying head sync entries on the local head index.
        void SetHeadSyncCallback(HeadSyncCallback cb) {
            m_headSyncCallback = std::move(cb);
        }

        /// Set the callback for remote lock/unlock of headIDs on this node.
        void SetRemoteLockCallback(RemoteLockCallback cb) {
            m_remoteLockCallback = std::move(cb);
        }

        /// Set the callback for dispatch commands (worker side).
        void SetDispatchCallback(DispatchCallback cb) {
            m_dispatchCallback = std::move(cb);
        }

        /// Clear the dispatch callback and wait for in-flight dispatch
        /// threads to complete. Call before destroying callback state.
        void ClearDispatchCallback() {
            m_dispatchCallback = nullptr;
            // Wait for all in-flight dispatch threads to finish
            std::unique_lock<std::mutex> lock(m_activeDispatchMutex);
            m_activeDispatchCV.wait(lock, [this]() {
                return m_activeDispatchCount == 0;
            });
        }

        /// Start the router server (listens for incoming remote appends)
        /// and connect client to all peer nodes.
        bool Start() {
            if (!m_enabled) return false;

            // --- Server side: listen for incoming AppendRequests ---
            Socket::PacketHandlerMapPtr serverHandlers(new Socket::PacketHandlerMap);
            serverHandlers->emplace(Socket::PacketType::AppendRequest,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleAppendRequest(connID, std::move(packet));
                });
            serverHandlers->emplace(Socket::PacketType::BatchAppendRequest,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleBatchAppendRequest(connID, std::move(packet));
                });
            serverHandlers->emplace(Socket::PacketType::HeadSyncRequest,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleHeadSyncRequest(connID, std::move(packet));
                });
            serverHandlers->emplace(Socket::PacketType::RemoteLockRequest,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleRemoteLockRequest(connID, std::move(packet));
                });
            serverHandlers->emplace(Socket::PacketType::DispatchCommand,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleDispatchCommand(connID, std::move(packet));
                });
            // DispatchResult arrives on the server when a worker's CLIENT connects
            // to this node's SERVER to send results back.
            serverHandlers->emplace(Socket::PacketType::DispatchResult,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleDispatchResult(connID, std::move(packet));
                });

            const auto& localAddr = m_nodeAddrs[m_localNodeIndex];
            m_server.reset(new Socket::Server(
                localAddr.first, localAddr.second, serverHandlers, 2));
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "PostingRouter server listening on %s:%s\n",
                localAddr.first.c_str(), localAddr.second.c_str());

            // --- Client side: connect to all peer nodes ---
            Socket::PacketHandlerMapPtr clientHandlers(new Socket::PacketHandlerMap);
            clientHandlers->emplace(Socket::PacketType::AppendResponse,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleAppendResponse(connID, std::move(packet));
                });
            clientHandlers->emplace(Socket::PacketType::BatchAppendResponse,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleBatchAppendResponse(connID, std::move(packet));
                });
            clientHandlers->emplace(Socket::PacketType::RemoteLockResponse,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleRemoteLockResponse(connID, std::move(packet));
                });
            clientHandlers->emplace(Socket::PacketType::DispatchResult,
                [this](Socket::ConnectionID connID, Socket::Packet packet) {
                    HandleDispatchResult(connID, std::move(packet));
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

        int GetNumNodes() const {
            auto ring = std::atomic_load(&m_hashRing);
            return ring ? static_cast<int>(ring->NodeCount()) : 0;
        }
        int GetLocalNodeIndex() const { return m_localNodeIndex; }

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

        /// Send an append request to a remote compute node and wait for the response.
        /// Returns ErrorCode::Success on success, or an error code on failure.
        ErrorCode SendRemoteAppend(
            int targetNodeIndex,
            SizeType headID,
            const std::shared_ptr<std::string>& headVec,
            int appendNum,
            std::string& appendPosting)
        {
            // Ensure connection to target
            Socket::ConnectionID connID = GetPeerConnection(targetNodeIndex);
            if (connID == Socket::c_invalidConnectionID) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: Cannot connect to node %d for headID %lld\n",
                    targetNodeIndex, (std::int64_t)headID);
                return ErrorCode::Fail;
            }

            // Serialize request
            RemoteAppendRequest req;
            req.m_headID = headID;
            req.m_headVec = *headVec;
            req.m_appendNum = appendNum;
            req.m_appendPosting = appendPosting;

            Socket::Packet packet;
            packet.Header().m_packetType = Socket::PacketType::AppendRequest;
            packet.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
            packet.Header().m_connectionID = Socket::c_invalidConnectionID;

            // Allocate a ResourceID and create a promise for the response
            Socket::ResourceID resID = m_nextResourceId.fetch_add(1);
            packet.Header().m_resourceID = resID;

            std::promise<ErrorCode> promise;
            std::future<ErrorCode> future = promise.get_future();
            {
                std::lock_guard<std::mutex> lock(m_pendingMutex);
                m_pendingResponses.emplace(resID, std::move(promise));
            }

            // Serialize body
            auto bodySize = static_cast<std::uint32_t>(req.EstimateBufferSize());
            packet.Header().m_bodyLength = bodySize;
            packet.AllocateBuffer(bodySize);
            req.Write(packet.Body());
            packet.Header().WriteBuffer(packet.HeaderBuffer());

            // Send
            m_client->SendPacket(connID, std::move(packet),
                [resID, this](bool success) {
                    if (!success) {
                        // Send failed; complete the promise with error
                        std::lock_guard<std::mutex> lock(m_pendingMutex);
                        auto it = m_pendingResponses.find(resID);
                        if (it != m_pendingResponses.end()) {
                            it->second.set_value(ErrorCode::Fail);
                            m_pendingResponses.erase(it);
                        }
                    }
                });

            // Wait for response (with timeout)
            auto status = future.wait_for(std::chrono::seconds(30));
            if (status == std::future_status::timeout) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: Timeout waiting for append response for headID %lld from node %d\n",
                    (std::int64_t)headID, targetNodeIndex);
                // Clean up pending entry
                std::lock_guard<std::mutex> lock(m_pendingMutex);
                m_pendingResponses.erase(resID);
                return ErrorCode::Fail;
            }

            return future.get();
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

        /// Send a batch of append requests to a remote node in a single round-trip.
        /// Returns ErrorCode::Success if all items succeeded, ErrorCode::Fail otherwise.
        ErrorCode SendBatchRemoteAppend(
            int targetNodeIndex,
            std::vector<RemoteAppendRequest>& items)
        {
            if (items.empty()) return ErrorCode::Success;

            // Retry once on connection failure (handles driver PostingRouter re-init)
            for (int attempt = 0; attempt < 2; attempt++) {
                Socket::ConnectionID connID = GetPeerConnection(targetNodeIndex);
                if (connID == Socket::c_invalidConnectionID) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                        "PostingRouter: Cannot connect to node %d for batch append (%d items, attempt %d)\n",
                        targetNodeIndex, (int)items.size(), attempt + 1);
                    if (attempt == 0) continue; // retry after GetPeerConnection reconnects
                    return ErrorCode::Fail;
                }

                BatchRemoteAppendRequest batchReq;
                batchReq.m_count = static_cast<std::uint32_t>(items.size());
                batchReq.m_items = std::move(items); // move in for serialization

                Socket::Packet packet;
                packet.Header().m_packetType = Socket::PacketType::BatchAppendRequest;
                packet.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
                packet.Header().m_connectionID = Socket::c_invalidConnectionID;

                Socket::ResourceID resID = m_nextResourceId.fetch_add(1);
                packet.Header().m_resourceID = resID;

                std::promise<ErrorCode> promise;
                std::future<ErrorCode> future = promise.get_future();
                {
                    std::lock_guard<std::mutex> lock(m_pendingMutex);
                    m_pendingResponses.emplace(resID, std::move(promise));
                }

                auto bodySize = static_cast<std::uint32_t>(batchReq.EstimateBufferSize());
                packet.Header().m_bodyLength = bodySize;
                packet.AllocateBuffer(bodySize);
                batchReq.Write(packet.Body());
                // Move items back after serialization so they're available for retry
                items = std::move(batchReq.m_items);
                packet.Header().WriteBuffer(packet.HeaderBuffer());

                SPTAGLIB_LOG(Helper::LogLevel::LL_Debug,
                    "PostingRouter: Sending batch of %u appends to node %d (resID=%u, bodySize=%u, attempt=%d)\n",
                    batchReq.m_count, targetNodeIndex, resID, bodySize, attempt + 1);

                m_client->SendPacket(connID, std::move(packet),
                    [resID, this](bool success) {
                        if (!success) {
                            std::lock_guard<std::mutex> lock(m_pendingMutex);
                            auto it = m_pendingResponses.find(resID);
                            if (it != m_pendingResponses.end()) {
                                it->second.set_value(ErrorCode::Fail);
                                m_pendingResponses.erase(it);
                            }
                        }
                    });

                auto status = future.wait_for(std::chrono::seconds(60));
                if (status == std::future_status::timeout) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                        "PostingRouter: Timeout waiting for batch append response from node %d\n",
                        targetNodeIndex);
                    std::lock_guard<std::mutex> lock(m_pendingMutex);
                    m_pendingResponses.erase(resID);
                    InvalidatePeerConnection(targetNodeIndex);
                    if (attempt == 0) continue; // retry
                    return ErrorCode::Fail;
                }

                ErrorCode result = future.get();
                if (result == ErrorCode::Success) return ErrorCode::Success;

                // Send failed — invalidate connection and retry
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "PostingRouter: Batch append to node %d failed (attempt %d), reconnecting...\n",
                    targetNodeIndex, attempt + 1);
                InvalidatePeerConnection(targetNodeIndex);
            }
            return ErrorCode::Fail;
        }

        // ---- Dispatch protocol (driver ↔ worker coordination) ----

        /// Broadcast a dispatch command to all worker nodes (driver side).
        /// Returns the dispatchId assigned to this command.
        std::uint64_t BroadcastDispatchCommand(DispatchCommand::Type type, std::uint32_t round) {
            std::uint64_t dispatchId = m_nextDispatchId.fetch_add(1);

            DispatchCommand cmd;
            cmd.m_type = type;
            cmd.m_dispatchId = dispatchId;
            cmd.m_round = round;

            // Set up pending state for collecting results (not for Stop)
            if (type != DispatchCommand::Type::Stop) {
                int numWorkers = GetNumNodes() - 1;  // exclude self
                if (numWorkers > 0) {
                    auto state = std::make_shared<PendingDispatch>();
                    state->remaining.store(numWorkers);
                    {
                        std::lock_guard<std::mutex> lock(m_dispatchMutex);
                        m_pendingDispatches[dispatchId] = state;
                    }
                }
            }

            auto bodySize = static_cast<std::uint32_t>(cmd.EstimateBufferSize());

            int numNodes;
            {
                std::lock_guard<std::mutex> lock(m_connMutex);
                numNodes = static_cast<int>(m_nodeAddrs.size());
            }

            for (int i = 0; i < numNodes; i++) {
                if (i == m_localNodeIndex) continue;

                Socket::ConnectionID connID = GetPeerConnection(i);
                if (connID == Socket::c_invalidConnectionID) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                        "PostingRouter: Cannot dispatch to node %d (no connection)\n", i);
                    // Count as failed immediately
                    if (type != DispatchCommand::Type::Stop) {
                        std::lock_guard<std::mutex> lock(m_dispatchMutex);
                        auto it = m_pendingDispatches.find(dispatchId);
                        if (it != m_pendingDispatches.end()) {
                            it->second->errors++;
                            if (it->second->remaining.fetch_sub(1) == 1) {
                                it->second->done.set_value();
                            }
                        }
                    }
                    continue;
                }

                Socket::Packet pkt;
                pkt.Header().m_packetType = Socket::PacketType::DispatchCommand;
                pkt.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
                pkt.Header().m_connectionID = Socket::c_invalidConnectionID;
                pkt.Header().m_resourceID = 0;
                pkt.Header().m_bodyLength = bodySize;
                pkt.AllocateBuffer(bodySize);
                cmd.Write(pkt.Body());
                pkt.Header().WriteBuffer(pkt.HeaderBuffer());

                m_client->SendPacket(connID, std::move(pkt), nullptr);
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "PostingRouter: Dispatched %s (id=%llu round=%u) to %d workers\n",
                type == DispatchCommand::Type::Search ? "Search" :
                type == DispatchCommand::Type::Insert ? "Insert" : "Stop",
                (unsigned long long)dispatchId, round, numNodes - 1);

            return dispatchId;
        }

        /// Wait for all workers to report results for a dispatch (driver side).
        /// Returns collected wall times from workers. Empty on timeout.
        std::vector<double> WaitForAllResults(std::uint64_t dispatchId, int timeoutSec = 300) {
            std::shared_ptr<PendingDispatch> state;
            {
                std::lock_guard<std::mutex> lock(m_dispatchMutex);
                auto it = m_pendingDispatches.find(dispatchId);
                if (it == m_pendingDispatches.end()) return {};
                state = it->second;
            }

            auto future = state->done.get_future();
            auto status = future.wait_for(std::chrono::seconds(timeoutSec));

            // Clean up
            {
                std::lock_guard<std::mutex> lock(m_dispatchMutex);
                m_pendingDispatches.erase(dispatchId);
            }

            if (status == std::future_status::timeout) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: Timeout waiting for dispatch results (id=%llu, %d remaining)\n",
                    (unsigned long long)dispatchId, state->remaining.load());
                return {};
            }

            if (state->errors > 0) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "PostingRouter: Dispatch %llu completed with %d errors\n",
                    (unsigned long long)dispatchId, (int)state->errors);
            }

            std::lock_guard<std::mutex> lock(state->mutex);
            return state->wallTimes;
        }

        /// Send a dispatch result back to the driver (worker side).
        void SendDispatchResult(const DispatchResult& result) {
            // Driver is always node 0 in current design
            int driverNode = 0;
            if (driverNode == m_localNodeIndex) return;

            Socket::ConnectionID connID = GetPeerConnection(driverNode);
            if (connID == Socket::c_invalidConnectionID) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: Cannot send dispatch result to driver\n");
                return;
            }

            Socket::Packet pkt;
            auto bodySize = static_cast<std::uint32_t>(result.EstimateBufferSize());
            pkt.Header().m_packetType = Socket::PacketType::DispatchResult;
            pkt.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
            pkt.Header().m_connectionID = Socket::c_invalidConnectionID;
            pkt.Header().m_resourceID = 0;
            pkt.Header().m_bodyLength = bodySize;
            pkt.AllocateBuffer(bodySize);
            result.Write(pkt.Body());
            pkt.Header().WriteBuffer(pkt.HeaderBuffer());

            m_client->SendPacket(connID, std::move(pkt), nullptr);
        }

    private:
        /// Pending dispatch state for collecting worker results (driver side).
        struct PendingDispatch {
            std::atomic<int> remaining{0};
            std::atomic<int> errors{0};
            std::promise<void> done;
            std::mutex mutex;
            std::vector<double> wallTimes;
        };

    public:
        /// Connect to a peer compute node with retry and exponential backoff.
        /// Returns true on success.
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
                delayMs = std::min(delayMs * 2, 5000);  // cap at 5s
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                "PostingRouter: All %d connection attempts to node %d (%s:%s) failed\n",
                maxRetries, nodeIndex, addr.first.c_str(), addr.second.c_str());
            return false;
        }

        /// Get the connection to a peer, reconnecting with retry if necessary.
        Socket::ConnectionID GetPeerConnection(int nodeIndex) {
            {
                std::lock_guard<std::mutex> lock(m_connMutex);
                if (m_peerConnections[nodeIndex] != Socket::c_invalidConnectionID)
                    return m_peerConnections[nodeIndex];
            }
            // Try to reconnect with retry
            if (ConnectToPeer(nodeIndex, 5, 1000)) {
                std::lock_guard<std::mutex> lock(m_connMutex);
                return m_peerConnections[nodeIndex];
            }
            return Socket::c_invalidConnectionID;
        }

        /// Invalidate a cached peer connection so the next GetPeerConnection reconnects.
        void InvalidatePeerConnection(int nodeIndex) {
            std::lock_guard<std::mutex> lock(m_connMutex);
            m_peerConnections[nodeIndex] = Socket::c_invalidConnectionID;
        }

        /// Handle an incoming AppendRequest from a peer node.
        void HandleAppendRequest(Socket::ConnectionID connID, Socket::Packet packet) {
            if (packet.Header().m_bodyLength == 0) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: Empty AppendRequest received\n");
                return;
            }

            if (Socket::c_invalidConnectionID == packet.Header().m_connectionID) {
                packet.Header().m_connectionID = connID;
            }

            // Deserialize request
            RemoteAppendRequest req;
            if (req.Read(packet.Body()) == nullptr) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: AppendRequest version mismatch\n");
                SendAppendResponse(packet, RemoteAppendResponse::Status::Failed);
                return;
            }

            // Execute the append via the registered callback
            ErrorCode result = ErrorCode::Fail;
            if (m_appendCallback) {
                auto headVec = std::make_shared<std::string>(std::move(req.m_headVec));
                result = m_appendCallback(
                    req.m_headID, headVec, req.m_appendNum, req.m_appendPosting);
            }

            auto status = (result == ErrorCode::Success)
                ? RemoteAppendResponse::Status::Success
                : RemoteAppendResponse::Status::Failed;
            SendAppendResponse(packet, status);
        }

        /// Send an AppendResponse back to the requesting peer.
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

            m_server->SendPacket(srcPacket.Header().m_connectionID, std::move(ret), nullptr);
        }

        /// Handle an incoming AppendResponse (client side, matches pending requests).
        void HandleAppendResponse(Socket::ConnectionID connID, Socket::Packet packet) {
            Socket::ResourceID resID = packet.Header().m_resourceID;

            std::promise<ErrorCode> promise;
            {
                std::lock_guard<std::mutex> lock(m_pendingMutex);
                auto it = m_pendingResponses.find(resID);
                if (it == m_pendingResponses.end()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                        "PostingRouter: Received AppendResponse for unknown resourceID %u\n",
                        resID);
                    return;
                }
                promise = std::move(it->second);
                m_pendingResponses.erase(it);
            }

            if (packet.Header().m_processStatus != Socket::PacketProcessStatus::Ok) {
                promise.set_value(ErrorCode::Fail);
                return;
            }

            RemoteAppendResponse resp;
            if (resp.Read(packet.Body()) == nullptr) {
                promise.set_value(ErrorCode::Fail);
                return;
            }

            promise.set_value(
                resp.m_status == RemoteAppendResponse::Status::Success
                    ? ErrorCode::Success
                    : ErrorCode::Fail);
        }

        /// Handle an incoming BatchAppendRequest from a peer node.
        void HandleBatchAppendRequest(Socket::ConnectionID connID, Socket::Packet packet) {
            if (packet.Header().m_bodyLength == 0) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: Empty BatchAppendRequest received\n");
                return;
            }

            if (Socket::c_invalidConnectionID == packet.Header().m_connectionID) {
                packet.Header().m_connectionID = connID;
            }

            BatchRemoteAppendRequest batchReq;
            if (batchReq.Read(packet.Body(), packet.Header().m_bodyLength) == nullptr) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: BatchAppendRequest parse failed\n");
                SendBatchAppendResponse(packet, 0, 1);
                return;
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Debug,
                "PostingRouter: Received batch of %u appends\n", batchReq.m_count);

            // Process appends in parallel using worker threads
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

            SendBatchAppendResponse(packet, successCount, failCount);
        }

        /// Send a BatchAppendResponse back to the requesting peer.
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

            m_server->SendPacket(srcPacket.Header().m_connectionID, std::move(ret), nullptr);
        }

        /// Handle an incoming BatchAppendResponse (client side).
        void HandleBatchAppendResponse(Socket::ConnectionID connID, Socket::Packet packet) {
            Socket::ResourceID resID = packet.Header().m_resourceID;

            std::promise<ErrorCode> promise;
            {
                std::lock_guard<std::mutex> lock(m_pendingMutex);
                auto it = m_pendingResponses.find(resID);
                if (it == m_pendingResponses.end()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                        "PostingRouter: Received BatchAppendResponse for unknown resourceID %u\n",
                        resID);
                    return;
                }
                promise = std::move(it->second);
                m_pendingResponses.erase(it);
            }

            if (packet.Header().m_processStatus != Socket::PacketProcessStatus::Ok) {
                promise.set_value(ErrorCode::Fail);
                return;
            }

            BatchRemoteAppendResponse resp;
            if (resp.Read(packet.Body()) == nullptr) {
                promise.set_value(ErrorCode::Fail);
                return;
            }

            promise.set_value(resp.m_failCount == 0 ? ErrorCode::Success : ErrorCode::Fail);
        }

        /// Handle an incoming head sync request (fire-and-forget, no response sent).
    public:
        void HandleHeadSyncRequest(Socket::ConnectionID connID, Socket::Packet packet) {
            if (!m_headSyncCallback) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "PostingRouter: HeadSyncRequest received but no callback set\n");
                return;
            }

            const std::uint8_t* buf = packet.Body();
            const std::uint8_t* bufEnd = buf + packet.Header().m_bodyLength;
            std::uint32_t entryCount = 0;
            buf = Socket::SimpleSerialization::SimpleReadBuffer(buf, entryCount);

            // Each entry has at minimum ~13 bytes; reject corrupt counts
            std::uint32_t bodyLength = packet.Header().m_bodyLength;
            if (bodyLength < sizeof(std::uint32_t) || entryCount > (bodyLength - sizeof(std::uint32_t)) / 8) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: HeadSyncRequest entryCount=%u exceeds bodyLength=%u\n",
                    entryCount, bodyLength);
                return;
            }

            for (std::uint32_t i = 0; i < entryCount; i++) {
                if (buf >= bufEnd) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                        "PostingRouter: HeadSyncRequest buffer overrun at entry %u/%u\n", i, entryCount);
                    break;
                }
                HeadSyncEntry entry;
                buf = entry.Read(buf);
                if (!buf || buf > bufEnd) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                        "PostingRouter: HeadSyncRequest parse error at entry %u/%u\n", i, entryCount);
                    break;
                }
                m_headSyncCallback(entry);
            }
        }

        /// Broadcast head sync entries to all peer nodes (fire-and-forget).
        void BroadcastHeadSync(const std::vector<HeadSyncEntry>& entries) {
            if (!m_enabled || entries.empty()) return;

            // Compute total body size
            size_t bodySize = sizeof(std::uint32_t); // entry count
            for (const auto& e : entries) bodySize += e.EstimateBufferSize();

            // Snapshot node count under lock to avoid racing with AddNode
            int numNodes;
            {
                std::lock_guard<std::mutex> lock(m_connMutex);
                numNodes = static_cast<int>(m_nodeAddrs.size());
            }

            for (int i = 0; i < numNodes; i++) {
                if (i == m_localNodeIndex) continue;

                Socket::ConnectionID connID = GetPeerConnection(i);
                if (connID == Socket::c_invalidConnectionID) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                        "PostingRouter: Cannot broadcast head sync to node %d (no connection)\n", i);
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

                m_client->SendPacket(connID, std::move(pkt),
                    [i](bool success) {
                        if (!success) {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Debug,
                                "PostingRouter: Head sync send to node %d failed\n", i);
                        }
                    });
            }
        }

        /// Send a remote lock request to a peer node (synchronous, waits for response).
        /// Returns true if lock was granted.
        bool SendRemoteLock(int nodeIndex, SizeType headID, bool lock) {
            if (!m_enabled) return false;

            Socket::ConnectionID connID = GetPeerConnection(nodeIndex);
            if (connID == Socket::c_invalidConnectionID) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "PostingRouter: Cannot send remote lock to node %d\n", nodeIndex);
                return false;
            }

            RemoteLockRequest req;
            req.m_op = lock ? RemoteLockRequest::Op::Lock : RemoteLockRequest::Op::Unlock;
            req.m_headID = headID;

            Socket::ResourceID rid = m_nextResourceId.fetch_add(1);
            std::promise<ErrorCode> promise;
            auto future = promise.get_future();
            {
                std::lock_guard<std::mutex> guard(m_pendingMutex);
                m_pendingResponses.emplace(rid, std::move(promise));
            }

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

            m_client->SendPacket(connID, std::move(pkt),
                [rid, this](bool success) {
                    if (!success) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                            "PostingRouter: RemoteLock send failed for resourceID %u\n", rid);
                        std::lock_guard<std::mutex> guard(m_pendingMutex);
                        auto it = m_pendingResponses.find(rid);
                        if (it != m_pendingResponses.end()) {
                            it->second.set_value(ErrorCode::Fail);
                            m_pendingResponses.erase(it);
                        }
                    }
                });

            auto status = future.wait_for(std::chrono::milliseconds(5000));
            if (status != std::future_status::ready) {
                std::lock_guard<std::mutex> guard(m_pendingMutex);
                m_pendingResponses.erase(rid);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "PostingRouter: Remote lock timeout for headID %lld on node %d\n",
                    (std::int64_t)headID, nodeIndex);
                return false;
            }
            return future.get() == ErrorCode::Success;
        }

        /// Handle an incoming remote lock request.
        void HandleRemoteLockRequest(Socket::ConnectionID connID, Socket::Packet packet) {
            RemoteLockRequest req;
            if (req.Read(packet.Body()) == nullptr) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: Failed to parse RemoteLockRequest\n");
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

            m_server->SendPacket(connID, std::move(ret), nullptr);
        }

        /// Handle remote lock response.
        void HandleRemoteLockResponse(Socket::ConnectionID connID, Socket::Packet packet) {
            Socket::ResourceID rid = packet.Header().m_resourceID;
            std::promise<ErrorCode> promise;
            {
                std::lock_guard<std::mutex> guard(m_pendingMutex);
                auto it = m_pendingResponses.find(rid);
                if (it == m_pendingResponses.end()) return;
                promise = std::move(it->second);
                m_pendingResponses.erase(it);
            }

            RemoteLockResponse resp;
            if (resp.Read(packet.Body()) == nullptr) {
                promise.set_value(ErrorCode::Fail);
                return;
            }

            promise.set_value(resp.m_status == RemoteLockResponse::Status::Granted
                ? ErrorCode::Success : ErrorCode::Fail);
        }

        /// Handle an incoming dispatch command from the driver (worker side).
        /// Offloads execution to a dedicated thread to avoid blocking io_context.
        void HandleDispatchCommand(Socket::ConnectionID connID, Socket::Packet packet) {
            if (packet.Header().m_bodyLength == 0) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: Empty DispatchCommand received\n");
                return;
            }

            DispatchCommand cmd;
            if (cmd.Read(packet.Body()) == nullptr) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: DispatchCommand parse failed\n");
                return;
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "PostingRouter: Received DispatchCommand type=%d id=%llu round=%u\n",
                (int)cmd.m_type, (unsigned long long)cmd.m_dispatchId, cmd.m_round);

            if (!m_dispatchCallback) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "PostingRouter: No dispatch callback set, ignoring command\n");
                return;
            }

            // Execute on a detached thread to avoid blocking the io_context.
            // The callback is expected to be long-running (search/insert).
            auto callback = m_dispatchCallback;
            if (!callback) return;  // re-check after copy (race with ClearDispatchCallback)
            auto self = this;

            // Track active dispatch threads for clean shutdown
            {
                std::lock_guard<std::mutex> lock(m_activeDispatchMutex);
                m_activeDispatchCount++;
            }

            std::thread([self, callback, cmd]() {
                DispatchResult result = callback(cmd);
                result.m_dispatchId = cmd.m_dispatchId;
                result.m_round = cmd.m_round;

                // Don't send result for Stop — the worker is shutting down
                if (cmd.m_type != DispatchCommand::Type::Stop) {
                    self->SendDispatchResult(result);
                }

                // Signal completion
                {
                    std::lock_guard<std::mutex> lock(self->m_activeDispatchMutex);
                    self->m_activeDispatchCount--;
                }
                self->m_activeDispatchCV.notify_all();
            }).detach();
        }

        /// Handle an incoming dispatch result from a worker (driver side).
        void HandleDispatchResult(Socket::ConnectionID connID, Socket::Packet packet) {
            if (packet.Header().m_bodyLength == 0) return;

            DispatchResult result;
            if (result.Read(packet.Body()) == nullptr) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "PostingRouter: DispatchResult parse failed\n");
                return;
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                "PostingRouter: Received DispatchResult id=%llu round=%u status=%d wallTime=%.3f\n",
                (unsigned long long)result.m_dispatchId, result.m_round,
                (int)result.m_status, result.m_wallTime);

            std::shared_ptr<PendingDispatch> state;
            {
                std::lock_guard<std::mutex> lock(m_dispatchMutex);
                auto it = m_pendingDispatches.find(result.m_dispatchId);
                if (it == m_pendingDispatches.end()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                        "PostingRouter: Received result for unknown dispatch %llu (late/expired)\n",
                        (unsigned long long)result.m_dispatchId);
                    return;
                }
                state = it->second;
            }

            if (result.m_status != DispatchResult::Status::Success) {
                state->errors++;
            }

            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->wallTimes.push_back(result.m_wallTime);
            }

            if (state->remaining.fetch_sub(1) == 1) {
                state->done.set_value();  // all workers reported
            }
        }

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

        // Response matching for synchronous sends
        std::atomic<Socket::ResourceID> m_nextResourceId{1};
        std::mutex m_pendingMutex;
        std::unordered_map<Socket::ResourceID, std::promise<ErrorCode>> m_pendingResponses;

        // Append callback (set by ExtraDynamicSearcher)
        AppendCallback m_appendCallback;

        // Head sync callback (set by ExtraDynamicSearcher)
        HeadSyncCallback m_headSyncCallback;

        // Remote lock callback (set by ExtraDynamicSearcher for cross-node Merge)
        RemoteLockCallback m_remoteLockCallback;

        // Dispatch protocol state
        DispatchCallback m_dispatchCallback;
        std::atomic<std::uint64_t> m_nextDispatchId{1};
        std::mutex m_dispatchMutex;
        std::unordered_map<std::uint64_t, std::shared_ptr<PendingDispatch>> m_pendingDispatches;

        // Active dispatch thread tracking (for clean shutdown)
        std::mutex m_activeDispatchMutex;
        std::condition_variable m_activeDispatchCV;
        int m_activeDispatchCount{0};

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
