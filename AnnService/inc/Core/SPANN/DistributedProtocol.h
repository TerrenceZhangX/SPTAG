// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once

#include "inc/Core/Common.h"
#include "inc/Socket/SimpleSerialization.h"
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

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
            const std::uint8_t* bufEnd = (bodyLength > 0) ? (p_buffer + bodyLength) : nullptr;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            p_buffer = SimpleReadBuffer(p_buffer, m_count);
            // Reject obviously corrupt counts before allocating
            if (bodyLength > 0 && m_count > bodyLength / 8) {
                return nullptr;
            }
            m_items.resize(m_count);
            for (std::uint32_t i = 0; i < m_count; i++) {
                if (bufEnd && p_buffer >= bufEnd) return nullptr;
                p_buffer = m_items[i].Read(p_buffer);
                if (!p_buffer) return nullptr;
                if (bufEnd && p_buffer > bufEnd) return nullptr;
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

    /// Dispatch command from driver to workers (replaces file-based barriers).
    struct DispatchCommand {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 0; }

        enum class Type : std::uint8_t { Search = 0, Insert = 1, Stop = 2 };
        Type m_type = Type::Search;
        std::uint64_t m_dispatchId = 0;   // unique ID from driver
        std::uint32_t m_round = 0;        // search round or insert batch index

        std::size_t EstimateBufferSize() const {
            return sizeof(std::uint16_t) * 2 + sizeof(std::uint8_t)
                 + sizeof(std::uint64_t) + sizeof(std::uint32_t);
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(static_cast<std::uint8_t>(m_type), p_buffer);
            p_buffer = SimpleWriteBuffer(m_dispatchId, p_buffer);
            p_buffer = SimpleWriteBuffer(m_round, p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            std::uint8_t rawType = 0;
            p_buffer = SimpleReadBuffer(p_buffer, rawType);
            m_type = static_cast<Type>(rawType);
            p_buffer = SimpleReadBuffer(p_buffer, m_dispatchId);
            p_buffer = SimpleReadBuffer(p_buffer, m_round);
            return p_buffer;
        }
    };

    /// Result from worker back to driver after executing a dispatch command.
    struct DispatchResult {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 1; }

        enum class Status : std::uint8_t { Success = 0, Failed = 1 };
        Status m_status = Status::Success;
        std::uint64_t m_dispatchId = 0;
        std::uint32_t m_round = 0;
        double m_wallTime = 0.0;
        std::int32_t m_nodeIndex = -1;  // which worker sent this result

        std::size_t EstimateBufferSize() const {
            return sizeof(std::uint16_t) * 2 + sizeof(std::uint8_t)
                 + sizeof(std::uint64_t) + sizeof(std::uint32_t) + sizeof(double)
                 + sizeof(std::int32_t);
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(static_cast<std::uint8_t>(m_status), p_buffer);
            p_buffer = SimpleWriteBuffer(m_dispatchId, p_buffer);
            p_buffer = SimpleWriteBuffer(m_round, p_buffer);
            p_buffer = SimpleWriteBuffer(m_wallTime, p_buffer);
            p_buffer = SimpleWriteBuffer(m_nodeIndex, p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            std::uint8_t rawStatus = 0;
            p_buffer = SimpleReadBuffer(p_buffer, rawStatus);
            m_status = static_cast<Status>(rawStatus);
            p_buffer = SimpleReadBuffer(p_buffer, m_dispatchId);
            p_buffer = SimpleReadBuffer(p_buffer, m_round);
            p_buffer = SimpleReadBuffer(p_buffer, m_wallTime);
            if (mirrorVer >= 1) {
                p_buffer = SimpleReadBuffer(p_buffer, m_nodeIndex);
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

    /// Worker → dispatcher registration message.
    struct NodeRegisterMsg {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 0; }

        std::int32_t m_nodeIndex = 0;
        std::string m_host;
        std::string m_port;
        std::string m_store;

        std::size_t EstimateBufferSize() const {
            std::size_t size = 0;
            size += sizeof(std::uint16_t) * 2;
            size += sizeof(std::int32_t);
            size += sizeof(std::uint32_t) + m_host.size();
            size += sizeof(std::uint32_t) + m_port.size();
            size += sizeof(std::uint32_t) + m_store.size();
            return size;
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(m_nodeIndex, p_buffer);
            p_buffer = SimpleWriteBuffer(m_host, p_buffer);
            p_buffer = SimpleWriteBuffer(m_port, p_buffer);
            p_buffer = SimpleWriteBuffer(m_store, p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            p_buffer = SimpleReadBuffer(p_buffer, m_nodeIndex);
            p_buffer = SimpleReadBuffer(p_buffer, m_host);
            p_buffer = SimpleReadBuffer(p_buffer, m_port);
            p_buffer = SimpleReadBuffer(p_buffer, m_store);
            return p_buffer;
        }
    };

    /// Dispatcher → worker ring update (full node list).
    struct RingUpdateMsg {
        static constexpr std::uint16_t MajorVersion() { return 1; }
        static constexpr std::uint16_t MirrorVersion() { return 0; }

        std::int32_t m_vnodeCount = 150;
        std::vector<std::int32_t> m_nodeIndices;

        std::size_t EstimateBufferSize() const {
            std::size_t size = 0;
            size += sizeof(std::uint16_t) * 2;
            size += sizeof(std::int32_t);       // vnodeCount
            size += sizeof(std::uint32_t);      // numNodes
            size += sizeof(std::int32_t) * m_nodeIndices.size();
            return size;
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(MajorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(MirrorVersion(), p_buffer);
            p_buffer = SimpleWriteBuffer(m_vnodeCount, p_buffer);
            std::uint32_t count = static_cast<std::uint32_t>(m_nodeIndices.size());
            p_buffer = SimpleWriteBuffer(count, p_buffer);
            for (auto idx : m_nodeIndices) {
                p_buffer = SimpleWriteBuffer(idx, p_buffer);
            }
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            std::uint16_t majorVer = 0, mirrorVer = 0;
            p_buffer = SimpleReadBuffer(p_buffer, majorVer);
            p_buffer = SimpleReadBuffer(p_buffer, mirrorVer);
            if (majorVer != MajorVersion()) return nullptr;
            p_buffer = SimpleReadBuffer(p_buffer, m_vnodeCount);
            std::uint32_t count = 0;
            p_buffer = SimpleReadBuffer(p_buffer, count);
            m_nodeIndices.resize(count);
            for (std::uint32_t i = 0; i < count; i++) {
                p_buffer = SimpleReadBuffer(p_buffer, m_nodeIndices[i]);
            }
            return p_buffer;
        }
    };

} // namespace SPTAG::SPANN
