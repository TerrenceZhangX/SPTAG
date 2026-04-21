// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_NETWORKNODE_H_
#define _SPTAG_SPANN_NETWORKNODE_H_

#include "inc/Core/SPANN/DistributedProtocol.h"
#include "inc/Core/SPANN/ConsistentHashRing.h"
#include "inc/Core/SPANN/DispatchCoordinator.h"
#include "inc/Core/SPANN/RemotePostingOps.h"
#include "inc/Socket/Client.h"
#include "inc/Socket/Server.h"
#include "inc/Socket/Packet.h"
#include <string>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <vector>
#include <atomic>
#include <thread>

namespace SPTAG::SPANN {

    /// Base class providing shared networking infrastructure for all
    /// distributed node roles. Manages server/client sockets, peer
    /// connections, consistent hash ring storage, and a background
    /// connection maintenance thread.
    ///
    /// Subclasses override RegisterHandlers() to wire up their specific
    /// packet handlers, and BgProtocolStep() / IsRingSettled() for
    /// role-specific background work.
    class NetworkNode : public DispatchCoordinator::PeerNetwork,
                        public RemotePostingOps::NetworkAccess {
    public:
        NetworkNode()
            : m_enabled(false), m_localNodeIndex(-1) {}

        virtual ~NetworkNode() {
            m_bgConnectStop.store(true);
            if (m_bgConnectThread.joinable()) m_bgConnectThread.join();
        }

        /// Initialize shared networking state.
        bool InitializeNetwork(
            int localNodeIdx,
            const std::vector<std::pair<std::string, std::string>>& nodeAddrs,
            int vnodeCount = 150)
        {
            if (nodeAddrs.empty() || localNodeIdx < 0 ||
                localNodeIdx >= static_cast<int>(nodeAddrs.size())) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "NetworkNode::Initialize invalid config: %d nodes, localIdx=%d\n",
                    (int)nodeAddrs.size(), localNodeIdx);
                return false;
            }

            m_localNodeIndex = localNodeIdx;
            m_nodeAddrs = nodeAddrs;
            m_vnodeCount = vnodeCount;

            // Start with empty hash ring
            std::atomic_store(&m_hashRing,
                std::shared_ptr<const ConsistentHashRing>(
                    std::make_shared<ConsistentHashRing>(vnodeCount)));

            m_enabled = true;
            return true;
        }

        /// Start server + client + background connection thread.
        /// Subclasses must have called InitializeNetwork() first.
        /// Each node listens on its own address from the combined address list.
        bool StartNetwork() {
            if (!m_enabled) return false;

            // --- Server side ---
            {
                Socket::PacketHandlerMapPtr serverHandlers(new Socket::PacketHandlerMap);
                RegisterServerHandlers(serverHandlers);

                const auto& localAddr = m_nodeAddrs[m_localNodeIndex];
                m_server.reset(new Socket::Server(
                    localAddr.first, localAddr.second, serverHandlers, 2));
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                    "NetworkNode server listening on %s:%s\n",
                    localAddr.first.c_str(), localAddr.second.c_str());
            }

            // --- Client side ---
            Socket::PacketHandlerMapPtr clientHandlers(new Socket::PacketHandlerMap);
            RegisterClientHandlers(clientHandlers);

            m_client.reset(new Socket::Client(clientHandlers, 2, 30));
            m_peerConnections.resize(m_nodeAddrs.size(), Socket::c_invalidConnectionID);

            // --- Background thread ---
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

                    BgProtocolStep();

                    if (allConnected && IsRingSettled()) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                            "NetworkNode: All peers connected and ring synchronized\n");
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
                    delayMs = std::min(delayMs + 500, 5000);
                }
            });

            return true;
        }

        // ---- PeerNetwork + NetworkAccess interface ----

        int GetLocalNodeIndex() const override { return m_localNodeIndex; }

        int GetNumNodes() const override {
            return static_cast<int>(m_nodeAddrs.size());
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

        void InvalidatePeerConnection(int nodeIndex) override {
            std::lock_guard<std::mutex> lock(m_connMutex);
            m_peerConnections[nodeIndex] = Socket::c_invalidConnectionID;
        }

        Socket::Client* GetClient() override { return m_client.get(); }
        Socket::Server* GetServer() override { return m_server.get(); }

        // ---- Shared accessors ----

        bool IsEnabled() const { return m_enabled; }

        std::shared_ptr<const ConsistentHashRing> GetHashRing() const {
            return std::atomic_load(&m_hashRing);
        }

        void SetHashRing(std::shared_ptr<const ConsistentHashRing> ring) {
            std::atomic_store(&m_hashRing, std::move(ring));
        }

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
                if (allConnected) return true;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                "NetworkNode: Timed out waiting for peer connections (%ds)\n", timeoutSec);
            return false;
        }

        bool ConnectToPeer(int nodeIndex, int maxRetries = 10, int initialDelayMs = 500) {
            if (nodeIndex == m_localNodeIndex) return true;
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
                        "NetworkNode: Connected to node %d (%s:%s), connID=%u (attempt %d)\n",
                        nodeIndex, addr.first.c_str(), addr.second.c_str(), connID, attempt);
                    return true;
                }
                if (attempt < maxRetries) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
                    delayMs = std::min(delayMs * 2, 5000);
                }
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                "NetworkNode: All %d connection attempts to node %d failed\n",
                maxRetries, nodeIndex);
            return false;
        }

    protected:
        /// Subclasses register their packet handlers here.
        virtual void RegisterServerHandlers(Socket::PacketHandlerMapPtr& handlers) = 0;
        virtual void RegisterClientHandlers(Socket::PacketHandlerMapPtr& handlers) = 0;

        /// Called each iteration of the bg thread for role-specific protocol work.
        virtual void BgProtocolStep() {}

        /// Return true when ring is fully synchronized for this node's role.
        virtual bool IsRingSettled() const { return true; }

        bool m_enabled;
        int m_localNodeIndex;
        int m_vnodeCount = 150;

        // Consistent hash ring (lock-free RCU: atomic_load to read, copy-on-write to modify)
        std::shared_ptr<const ConsistentHashRing> m_hashRing;
        std::mutex m_ringWriteMutex;

        // Node addresses
        std::vector<std::pair<std::string, std::string>> m_nodeAddrs;

        // Networking
        std::unique_ptr<Socket::Server> m_server;
        std::unique_ptr<Socket::Client> m_client;
        std::mutex m_connMutex;
        std::vector<Socket::ConnectionID> m_peerConnections;

        // Background thread
        std::thread m_bgConnectThread;
        std::atomic<bool> m_bgConnectStop{false};
    };

} // namespace SPTAG::SPANN

#endif // _SPTAG_SPANN_NETWORKNODE_H_
