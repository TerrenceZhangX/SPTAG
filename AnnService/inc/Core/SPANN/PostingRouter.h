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
#include <map>
#include <set>
#include <mutex>
#include <memory>
#include <vector>
#include <atomic>
#include <future>
#include <functional>
#include <thread>

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

        const std::uint8_t* Read(const std::uint8_t* p_buffer, std::uint32_t bodyLength = 0) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            p_buffer = SimpleReadBuffer(p_buffer, m_count);
            // Validate count against body length if provided
            if (bodyLength > 0 && m_count > bodyLength / sizeof(RemoteAppendRequest)) {
                return nullptr;
            }
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

    /// Entry in a head sync broadcast: one add or delete of a head node.
    struct HeadSyncEntry {
        enum class Op : std::uint8_t { Add = 0, Delete = 1 };
        Op op;
        SizeType headVID;
        std::string headVector;  // only for Add; empty for Delete

        size_t EstimateBufferSize() const {
            return sizeof(std::uint8_t)   // op
                 + sizeof(SizeType)       // headVID
                 + sizeof(std::uint32_t)  // headVector length
                 + headVector.size();
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(static_cast<std::uint8_t>(op), p_buffer);
            p_buffer = SimpleWriteBuffer(headVID, p_buffer);
            std::uint32_t vecLen = static_cast<std::uint32_t>(headVector.size());
            p_buffer = SimpleWriteBuffer(vecLen, p_buffer);
            if (vecLen > 0) {
                memcpy(p_buffer, headVector.data(), vecLen);
                p_buffer += vecLen;
            }
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint8_t rawOp = 0;
            p_buffer = SimpleReadBuffer(p_buffer, rawOp);
            op = static_cast<Op>(rawOp);
            p_buffer = SimpleReadBuffer(p_buffer, headVID);
            std::uint32_t vecLen = 0;
            p_buffer = SimpleReadBuffer(p_buffer, vecLen);
            if (vecLen > 0) {
                headVector.assign(reinterpret_cast<const char*>(p_buffer), vecLen);
                p_buffer += vecLen;
            } else {
                headVector.clear();
            }
            return p_buffer;
        }
    };

    /// Request to lock/unlock a headID on its owner node (for cross-node Merge).
    struct RemoteLockRequest {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 0; }

        enum class Op : std::uint8_t { Lock = 0, Unlock = 1 };
        Op m_op = Op::Lock;
        SizeType m_headID = 0;

        std::size_t EstimateBufferSize() const {
            return sizeof(std::uint16_t) * 2 + sizeof(std::uint8_t) + sizeof(SizeType);
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(static_cast<std::uint8_t>(m_op), p_buffer);
            p_buffer = SimpleWriteBuffer(m_headID, p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            std::uint8_t rawOp = 0;
            p_buffer = SimpleReadBuffer(p_buffer, rawOp);
            m_op = static_cast<Op>(rawOp);
            p_buffer = SimpleReadBuffer(p_buffer, m_headID);
            return p_buffer;
        }
    };

    /// Response for remote lock operations.
    struct RemoteLockResponse {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 0; }

        enum class Status : std::uint8_t { Granted = 0, Denied = 1 };
        Status m_status = Status::Granted;

        std::size_t EstimateBufferSize() const {
            return sizeof(std::uint16_t) * 2 + sizeof(std::uint8_t);
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(static_cast<std::uint8_t>(m_status), p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            std::uint8_t rawOp = 0;
            p_buffer = SimpleReadBuffer(p_buffer, rawOp);
            m_status = static_cast<Status>(rawOp);
            return p_buffer;
        }
    };

    /// Consistent hash ring for distributing headIDs across compute nodes.
    /// Uses virtual nodes (vnodes) for balanced distribution.
    /// When nodes are added/removed, only ~1/N of keys are remapped.
    class ConsistentHashRing {
    public:
        explicit ConsistentHashRing(int vnodeCount = 150)
            : m_vnodeCount(vnodeCount) {}

        /// Add a physical node to the ring with its virtual nodes.
        void AddNode(int nodeIndex) {
            for (int i = 0; i < m_vnodeCount; i++) {
                uint32_t h = HashVNode(nodeIndex, i);
                m_ring[h] = nodeIndex;
            }
            m_nodes.insert(nodeIndex);
        }

        /// Remove a physical node and all its virtual nodes from the ring.
        void RemoveNode(int nodeIndex) {
            for (int i = 0; i < m_vnodeCount; i++) {
                uint32_t h = HashVNode(nodeIndex, i);
                m_ring.erase(h);
            }
            m_nodes.erase(nodeIndex);
        }

        /// Find the owner node for a given key (headID).
        /// Returns -1 if the ring is empty.
        int GetOwner(SizeType headID) const {
            if (m_ring.empty()) return -1;
            uint32_t h = HashKey(headID);
            auto it = m_ring.lower_bound(h);
            if (it == m_ring.end()) it = m_ring.begin();
            return it->second;
        }

        bool Empty() const { return m_ring.empty(); }
        size_t NodeCount() const { return m_nodes.size(); }
        bool HasNode(int nodeIndex) const { return m_nodes.count(nodeIndex) > 0; }
        const std::set<int>& GetNodes() const { return m_nodes; }
        int GetVNodeCount() const { return m_vnodeCount; }

    private:
        static uint32_t HashKey(SizeType headID) {
            uint32_t hash = 2166136261u; // FNV-1a offset basis
            uint32_t val = static_cast<uint32_t>(headID);
            for (int i = 0; i < 4; i++) {
                hash ^= (val >> (i * 8)) & 0xFF;
                hash *= 16777619u; // FNV prime
            }
            return hash;
        }

        static uint32_t HashVNode(int nodeIndex, int vnodeIdx) {
            uint32_t hash = 2166136261u;
            auto mix = [&](uint32_t v) {
                for (int i = 0; i < 4; i++) {
                    hash ^= (v >> (i * 8)) & 0xFF;
                    hash *= 16777619u;
                }
            };
            mix(static_cast<uint32_t>(nodeIndex));
            mix(static_cast<uint32_t>(vnodeIdx));
            return hash;
        }

        int m_vnodeCount;
        std::map<uint32_t, int> m_ring;  // hash position → nodeIndex
        std::set<int> m_nodes;           // active physical node indices
    };

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

        /// Callback to apply a head sync entry on the local head index.
        /// For Add: call AddHeadIndex with (vectorData, headVID, dim).
        /// For Delete: call DeleteIndex with (headVID, layer).
        using HeadSyncCallback = std::function<void(const HeadSyncEntry& entry)>;

        /// Callback for remote lock: try_lock or unlock a headID on this node.
        /// Returns true if lock was acquired (Lock) or released (Unlock).
        using RemoteLockCallback = std::function<bool(SizeType headID, bool lock)>;

        PostingRouter()
            : m_enabled(false), m_localNodeIndex(-1) {}

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

            m_client.reset(new Socket::Client(clientHandlers, 2, 30));

            m_peerConnections.resize(m_nodeAddrs.size(), Socket::c_invalidConnectionID);
            for (int i = 0; i < static_cast<int>(m_nodeAddrs.size()); i++) {
                if (i == m_localNodeIndex) continue;
                // Try once; lazy reconnect via GetPeerConnection handles failures
                ConnectToPeer(i, 3, 500);
            }

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
            }

            if (!store.empty()) {
                m_storeToNodes[store].push_back(nodeIndex);
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
            std::uint32_t entryCount = 0;
            buf = Socket::SimpleSerialization::SimpleReadBuffer(buf, entryCount);

            for (std::uint32_t i = 0; i < entryCount; i++) {
                HeadSyncEntry entry;
                buf = entry.Read(buf);
                if (!buf) {
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

            for (int i = 0; i < static_cast<int>(m_nodeAddrs.size()); i++) {
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
