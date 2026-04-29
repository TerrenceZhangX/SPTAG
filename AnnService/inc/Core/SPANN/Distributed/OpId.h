// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// OpId — operation identifier / idempotency token for distributed write paths.
//
// An OpId uniquely identifies a logical write attempt (one client-driven
// Insert call, or one driver-internal retry batch) across the cluster. The
// receiver uses (OpId, cached-result) pairs to deduplicate retries:
//
//   - senderId        — index of the originating compute node (or -1 for
//                       client-side allocation; assigned by router).
//   - restartEpoch    — bumped every time the sender restarts. Distinguishes
//                       "same counter, different process incarnation" so a
//                       receiver can invalidate stale cache entries left
//                       over from a previous incarnation of the same sender.
//   - monotonicCounter — per-sender, per-epoch monotonically increasing
//                       sequence number. Strictly increases within an epoch.
//
// The pair (senderId, restartEpoch) is referred to as the *senderEpoch*; it
// scopes the counter namespace.
//
// Wire encoding is fixed-width little-endian via SimpleSerialization, so the
// struct is safe to embed inside any DistributedProtocol message without
// version drift inside a single major version.

#pragma once

#include "inc/Socket/SimpleSerialization.h"
#include <cstdint>
#include <functional>
#include <atomic>
#include <ostream>

namespace SPTAG::SPANN::Distributed {

    struct OpId {
        std::int32_t  senderId        = -1;
        std::uint32_t restartEpoch    = 0;
        std::uint64_t monotonicCounter = 0;

        constexpr OpId() = default;
        constexpr OpId(std::int32_t s, std::uint32_t e, std::uint64_t c)
            : senderId(s), restartEpoch(e), monotonicCounter(c) {}

        bool IsValid() const { return senderId >= 0; }

        bool operator==(const OpId& o) const {
            return senderId == o.senderId
                && restartEpoch == o.restartEpoch
                && monotonicCounter == o.monotonicCounter;
        }
        bool operator!=(const OpId& o) const { return !(*this == o); }

        // Lexicographic ordering: senderId, then epoch, then counter. Useful
        // for deterministic logging / debug dumps; do not confuse with
        // causality.
        bool operator<(const OpId& o) const {
            if (senderId != o.senderId) return senderId < o.senderId;
            if (restartEpoch != o.restartEpoch) return restartEpoch < o.restartEpoch;
            return monotonicCounter < o.monotonicCounter;
        }

        // Wire layout (16 bytes): senderId(4) | epoch(4) | counter(8).
        // No version prefix: that is the responsibility of the enclosing
        // protocol message.
        static constexpr std::size_t WireSize() {
            return sizeof(std::int32_t) + sizeof(std::uint32_t) + sizeof(std::uint64_t);
        }

        std::uint8_t* Write(std::uint8_t* p) const {
            using namespace Socket::SimpleSerialization;
            p = SimpleWriteBuffer(senderId, p);
            p = SimpleWriteBuffer(restartEpoch, p);
            p = SimpleWriteBuffer(monotonicCounter, p);
            return p;
        }

        const std::uint8_t* Read(const std::uint8_t* p) {
            using namespace Socket::SimpleSerialization;
            p = SimpleReadBuffer(p, senderId);
            p = SimpleReadBuffer(p, restartEpoch);
            p = SimpleReadBuffer(p, monotonicCounter);
            return p;
        }
    };

    inline std::ostream& operator<<(std::ostream& os, const OpId& id) {
        return os << "OpId(" << id.senderId << "@e" << id.restartEpoch
                  << ":#" << id.monotonicCounter << ")";
    }

    // Sender-side allocator. One per local compute node. Thread-safe.
    // restartEpoch is supplied at construction (typically wall-clock seconds
    // at process start, or a persisted counter incremented per boot).
    class OpIdAllocator {
    public:
        OpIdAllocator() = default;
        OpIdAllocator(std::int32_t senderId, std::uint32_t restartEpoch)
            : m_senderId(senderId), m_restartEpoch(restartEpoch) {}

        void Reset(std::int32_t senderId, std::uint32_t restartEpoch) {
            m_senderId = senderId;
            m_restartEpoch = restartEpoch;
            m_counter.store(0, std::memory_order_relaxed);
        }

        OpId Next() {
            return OpId{
                m_senderId,
                m_restartEpoch,
                m_counter.fetch_add(1, std::memory_order_relaxed) + 1};
        }

        std::int32_t  SenderId()     const { return m_senderId; }
        std::uint32_t RestartEpoch() const { return m_restartEpoch; }

    private:
        std::int32_t  m_senderId     = -1;
        std::uint32_t m_restartEpoch = 0;
        std::atomic<std::uint64_t> m_counter{0};
    };

} // namespace SPTAG::SPANN::Distributed

namespace std {
    template <>
    struct hash<SPTAG::SPANN::Distributed::OpId> {
        std::size_t operator()(const SPTAG::SPANN::Distributed::OpId& id) const noexcept {
            // FNV-1a-ish mix; senderId/epoch fit in 64 bits, counter is 64.
            std::uint64_t senderEpoch =
                (static_cast<std::uint64_t>(static_cast<std::uint32_t>(id.senderId)) << 32)
                | static_cast<std::uint64_t>(id.restartEpoch);
            std::uint64_t h = senderEpoch ^ 0x9E3779B97F4A7C15ULL;
            h ^= id.monotonicCounter + 0x9E3779B97F4A7C15ULL + (h << 6) + (h >> 2);
            return static_cast<std::size_t>(h);
        }
    };
}
