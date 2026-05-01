// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Ring-epoch fencing primitive.
//
// Every routed RPC between distributed compute nodes carries the
// sender's view of the cluster ring as a (epoch, ringRev) pair.
// The receiver compares the sender's view against its own and either:
//   * accepts (Equal),
//   * refreshes itself if it is behind (ReceiverStale),
//   * rejects with ErrorCode::StaleRingEpoch if the sender is behind.
//
// epoch     — increments on every membership change (AddNode/RemoveNode).
// ringRev   — within-epoch monotonic for non-membership rebalances.
//
// {0, 0} is reserved as the "uninitialised" sentinel. A node with epoch
// {0, 0} is in the pre-Initialize state and MUST NOT send routed RPCs;
// this kills the GetOwner→{isLocal=true, nodeIndex=-1} hazard at the RPC
// layer (per fault-case: posting-router-routing-table-drift).

#pragma once

#include "inc/Socket/SimpleSerialization.h"
#include <cstdint>

namespace SPTAG::SPANN {

    struct RingEpoch {
        std::uint32_t epoch = 0;
        std::uint32_t ringRev = 0;

        constexpr RingEpoch() = default;
        constexpr RingEpoch(std::uint32_t e, std::uint32_t r) : epoch(e), ringRev(r) {}

        constexpr bool IsInitialised() const { return epoch != 0 || ringRev != 0; }
        constexpr bool IsZero() const { return epoch == 0 && ringRev == 0; }

        constexpr bool operator==(const RingEpoch& o) const {
            return epoch == o.epoch && ringRev == o.ringRev;
        }
        constexpr bool operator!=(const RingEpoch& o) const { return !(*this == o); }
        constexpr bool operator<(const RingEpoch& o) const {
            return epoch < o.epoch || (epoch == o.epoch && ringRev < o.ringRev);
        }

        static constexpr std::size_t WireSize() {
            return sizeof(std::uint32_t) * 2;
        }

        std::uint8_t* Write(std::uint8_t* p_buffer) const {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleWriteBuffer(epoch, p_buffer);
            p_buffer = SimpleWriteBuffer(ringRev, p_buffer);
            return p_buffer;
        }

        const std::uint8_t* Read(const std::uint8_t* p_buffer) {
            using namespace Socket::SimpleSerialization;
            p_buffer = SimpleReadBuffer(p_buffer, epoch);
            p_buffer = SimpleReadBuffer(p_buffer, ringRev);
            return p_buffer;
        }
    };

    /// Result of comparing a sender's ring view against the local view.
    enum class RingEpochCompare : std::uint8_t {
        Equal = 0,            // sender == local
        SenderStale = 1,      // sender < local; sender must refresh + retry
        ReceiverStale = 2,    // sender > local; receiver must refresh
        SenderUninitialised = 3,  // sender is {0,0}; refuse — sender-startup gate
    };

    /// Pure comparison; no I/O, no logging.
    inline RingEpochCompare CompareRingEpoch(const RingEpoch& sender,
                                             const RingEpoch& local) {
        if (sender.IsZero()) return RingEpochCompare::SenderUninitialised;
        if (sender == local) return RingEpochCompare::Equal;
        if (sender < local) return RingEpochCompare::SenderStale;
        return RingEpochCompare::ReceiverStale;
    }

} // namespace SPTAG::SPANN
