// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_POSTINGROUTER_H_
#define _SPTAG_SPANN_POSTINGROUTER_H_

#include "inc/Helper/KeyValueIO.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Socket/Client.h"
#include "inc/Socket/Server.h"
#include "inc/Socket/Packet.h"
#include "inc/Socket/SimpleSerialization.h"
#include <string>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <atomic>
#include <future>
#include <functional>

namespace SPTAG::SPANN {

    /// Serializable request for remote Append operations sent between compute nodes.
    struct RemoteAppendRequest {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 0; }

        SizeType m_headID = 0;
        std::string m_headVec;        // raw head vector bytes
        std::int32_t m_appendNum = 0;
        std::string m_appendPosting;  // serialized posting data

        std::size_t EstimateBufferSize() const {
            std::size_t size = 0;
            size += sizeof(std::uint16_t) * 2;  // version fields
            size += sizeof(SizeType);            // headID
            size += sizeof(std::uint32_t) + m_headVec.size();       // headVec (len-prefixed)
            size += sizeof(std::int32_t);        // appendNum
            size += sizeof(std::uint32_t) + m_appendPosting.size(); // appendPosting (len-prefixed)
            return size;
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(m_headID, p_buffer);
            p_buffer = SimpleWriteBuffer(m_headVec, p_buffer);
            p_buffer = SimpleWriteBuffer(m_appendNum, p_buffer);
            p_buffer = SimpleWriteBuffer(m_appendPosting, p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            p_buffer = SimpleReadBuffer(p_buffer, m_headID);
            p_buffer = SimpleReadBuffer(p_buffer, m_headVec);
            p_buffer = SimpleReadBuffer(p_buffer, m_appendNum);
            p_buffer = SimpleReadBuffer(p_buffer, m_appendPosting);
            return p_buffer;
        }
    };

    /// Response for remote Append operations.
    struct RemoteAppendResponse {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 0; }

        enum class Status : std::uint8_t { Success = 0, Failed = 1 };
        Status m_status = Status::Success;

        std::size_t EstimateBufferSize() const {
            return sizeof(std::uint16_t) * 2 + sizeof(std::uint8_t);
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(m_status, p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            p_buffer = SimpleReadBuffer(p_buffer, m_status);
            return p_buffer;
        }
    };

    /// Identifies a compute node target for routing decisions.
    struct RouteTarget {
        int nodeIndex = -1;
        bool isLocal = true;
    };

    /// Batch of remote append requests sent to a single node in one round-trip.
    struct BatchRemoteAppendRequest {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 0; }

        std::uint32_t m_count = 0;
        std::vector<RemoteAppendRequest> m_items;

        std::size_t EstimateBufferSize() const {
            std::size_t size = sizeof(std::uint16_t) * 2;  // version
            size += sizeof(std::uint32_t);  // count
            for (auto& item : m_items) size += item.EstimateBufferSize();
            return size;
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(m_count, p_buffer);
            for (auto& item : m_items) p_buffer = item.Write(p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            p_buffer = SimpleReadBuffer(p_buffer, m_count);
            m_items.resize(m_count);
            for (std::uint32_t i = 0; i < m_count; i++) {
                p_buffer = m_items[i].Read(p_buffer);
                if (!p_buffer) return nullptr;
            }
            return p_buffer;
        }
    };

    /// Response for batch remote append.
    struct BatchRemoteAppendResponse {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 0; }

        std::uint32_t m_successCount = 0;
        std::uint32_t m_failCount = 0;

        std::size_t EstimateBufferSize() const {
            return sizeof(std::uint16_t) * 2 + sizeof(std::uint32_t) * 2;
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(m_successCount, p_buffer);
            p_buffer = SimpleWriteBuffer(m_failCount, p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            p_buffer = SimpleReadBuffer(p_buffer, m_successCount);
            p_buffer = SimpleReadBuffer(p_buffer, m_failCount);
            return p_buffer;
        }
    };

    /// Distributed routing layer for SPFRESH posting writes.
    ///
    /// Architecture: N compute nodes, each with a full head index replica,
    /// sharing a TiKV cluster for posting storage. When a write arrives,
    /// PostingRouter determines which compute node should handle it based
    /// on the TiKV region-to-store mapping (writes go to the compute node
    /// nearest the TiKV store holding that region's leader).
    ///
    /// This guarantees that the same headID always routes to the same compute
    /// node, preserving VersionMap consistency and Merge atomicity.
    class PostingRouter {
    public:
        /// Callback type for handling a local append.
        /// Params: headID, headVec, appendNum, appendPosting
        /// Returns ErrorCode.
        using AppendCallback = std::function<ErrorCode(
            SizeType headID,
            std::shared_ptr<std::string> headVec,
            int appendNum,
            std::string& appendPosting)>;

        PostingRouter()
            : m_enabled(false), m_localNodeIndex(-1) {}

        /// Initialize the router from configuration.
        /// @param p_db         The KeyValueIO backend (for GetKeyLocation).
        /// @param localNodeIdx Index of this node in the compute node list.
        /// @param nodeAddrs    "host:port" pairs for all compute nodes' router ports.
        /// @param nodeStores   TiKV store address nearest each compute node.
        bool Initialize(
            std::shared_ptr<Helper::KeyValueIO> p_db,
            int localNodeIdx,
            const std::vector<std::pair<std::string, std::string>>& nodeAddrs,
            const std::vector<std::string>& nodeStores)
        {
            if (nodeAddrs.empty() || localNodeIdx < 0 ||
                localNodeIdx >= static_cast<int>(nodeAddrs.size()) ||
                nodeAddrs.size() != nodeStores.size()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter::Initialize invalid config: %d nodes, localIdx=%d, %d stores\n",
                    (int)nodeAddrs.size(), localNodeIdx, (int)nodeStores.size());
                return false;
            }

            m_db = p_db;
            m_localNodeIndex = localNodeIdx;
            m_nodeAddrs = nodeAddrs;
            m_nodeStores = nodeStores;

            // Build store address → node index mapping
            for (int i = 0; i < static_cast<int>(nodeStores.size()); i++) {
                m_storeToNode[nodeStores[i]] = i;
            }

            m_enabled = true;
            return true;
        }

        /// Set the callback for handling appends locally (called for incoming RPCs).
        void SetAppendCallback(AppendCallback cb) {
            m_appendCallback = std::move(cb);
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

            m_client.reset(new Socket::Client(clientHandlers, 2, 30));

            m_peerConnections.resize(m_nodeAddrs.size(), Socket::c_invalidConnectionID);
            for (int i = 0; i < static_cast<int>(m_nodeAddrs.size()); i++) {
                if (i == m_localNodeIndex) continue;
                ConnectToPeer(i);
            }

            return true;
        }

        /// Determine whether a headID should be handled by this node.
        RouteTarget GetOwner(SizeType headID) {
            RouteTarget target;
            target.isLocal = true;
            target.nodeIndex = m_localNodeIndex;

            if (!m_enabled || !m_db) return target;

            Helper::KeyLocation loc;
            if (!m_db->GetKeyLocation(headID, loc)) {
                // Storage doesn't support location queries; default to local
                return target;
            }

            auto it = m_storeToNode.find(loc.leaderStoreAddr);
            if (it == m_storeToNode.end()) {
                // Leader is on a store with no nearby compute node; keep local
                SPTAGLIB_LOG(Helper::LogLevel::LL_Debug,
                    "PostingRouter: No node mapped to store %s for headID %lld, using local\n",
                    loc.leaderStoreAddr.c_str(), (std::int64_t)headID);
                return target;
            }

            target.nodeIndex = it->second;
            target.isLocal = (target.nodeIndex == m_localNodeIndex);
            return target;
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

        /// Send a batch of append requests to a remote node in a single round-trip.
        /// Returns ErrorCode::Success if all items succeeded, ErrorCode::Fail otherwise.
        ErrorCode SendBatchRemoteAppend(
            int targetNodeIndex,
            std::vector<RemoteAppendRequest>& items)
        {
            if (items.empty()) return ErrorCode::Success;

            Socket::ConnectionID connID = GetPeerConnection(targetNodeIndex);
            if (connID == Socket::c_invalidConnectionID) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: Cannot connect to node %d for batch append (%d items)\n",
                    targetNodeIndex, (int)items.size());
                return ErrorCode::Fail;
            }

            BatchRemoteAppendRequest batchReq;
            batchReq.m_count = static_cast<std::uint32_t>(items.size());
            batchReq.m_items = std::move(items);

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
            packet.Header().WriteBuffer(packet.HeaderBuffer());

            SPTAGLIB_LOG(Helper::LogLevel::LL_Debug,
                "PostingRouter: Sending batch of %u appends to node %d (resID=%u, bodySize=%u)\n",
                batchReq.m_count, targetNodeIndex, resID, bodySize);

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
                return ErrorCode::Fail;
            }

            return future.get();
        }

    private:
        /// Connect to a peer compute node with retry and exponential backoff.
        /// Returns true on success.
        bool ConnectToPeer(int nodeIndex, int maxRetries = 10, int initialDelayMs = 500) {
            if (nodeIndex == m_localNodeIndex) return true;

            int delayMs = initialDelayMs;
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                ErrorCode ec;
                auto connID = m_client->ConnectToServer(
                    m_nodeAddrs[nodeIndex].first,
                    m_nodeAddrs[nodeIndex].second, ec);
                if (ec == ErrorCode::Success) {
                    std::lock_guard<std::mutex> lock(m_connMutex);
                    m_peerConnections[nodeIndex] = connID;
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "PostingRouter: Connected to node %d (%s:%s), connID=%u (attempt %d)\n",
                        nodeIndex,
                        m_nodeAddrs[nodeIndex].first.c_str(),
                        m_nodeAddrs[nodeIndex].second.c_str(),
                        connID, attempt);
                    return true;
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                    "PostingRouter: Failed to connect to node %d (%s:%s), attempt %d/%d, retrying in %dms\n",
                    nodeIndex,
                    m_nodeAddrs[nodeIndex].first.c_str(),
                    m_nodeAddrs[nodeIndex].second.c_str(),
                    attempt, maxRetries, delayMs);
                std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
                delayMs = std::min(delayMs * 2, 5000);  // cap at 5s
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                "PostingRouter: All %d connection attempts to node %d (%s:%s) failed\n",
                maxRetries, nodeIndex,
                m_nodeAddrs[nodeIndex].first.c_str(),
                m_nodeAddrs[nodeIndex].second.c_str());
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
            if (batchReq.Read(packet.Body()) == nullptr) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "PostingRouter: BatchAppendRequest version mismatch\n");
                SendBatchAppendResponse(packet, 0, 1);
                return;
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Debug,
                "PostingRouter: Received batch of %u appends\n", batchReq.m_count);

            std::uint32_t successCount = 0, failCount = 0;
            for (auto& req : batchReq.m_items) {
                ErrorCode result = ErrorCode::Fail;
                if (m_appendCallback) {
                    auto headVec = std::make_shared<std::string>(std::move(req.m_headVec));
                    result = m_appendCallback(
                        req.m_headID, headVec, req.m_appendNum, req.m_appendPosting);
                }
                if (result == ErrorCode::Success) successCount++;
                else failCount++;
            }

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

    private:
        bool m_enabled;
        int m_localNodeIndex;
        std::shared_ptr<Helper::KeyValueIO> m_db;

        // Node configuration
        std::vector<std::pair<std::string, std::string>> m_nodeAddrs;  // host:port per node
        std::vector<std::string> m_nodeStores;  // TiKV store addr per node
        std::unordered_map<std::string, int> m_storeToNode;  // store addr → node index

        // Server (receives remote append requests)
        std::unique_ptr<Socket::Server> m_server;

        // Client (sends remote append requests to peers)
        std::unique_ptr<Socket::Client> m_client;
        std::mutex m_connMutex;
        std::vector<Socket::ConnectionID> m_peerConnections;

        // Response matching for synchronous sends
        std::atomic<Socket::ResourceID> m_nextResourceId{1};
        std::mutex m_pendingMutex;
        std::unordered_map<Socket::ResourceID, std::promise<ErrorCode>> m_pendingResponses;

        // Append callback (set by ExtraDynamicSearcher)
        AppendCallback m_appendCallback;
    };

} // namespace SPTAG::SPANN

#endif // _SPTAG_SPANN_POSTINGROUTER_H_
