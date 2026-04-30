// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Primitive: RingEpochFence
//
// Validates the (epoch, ringRev) versioning + StaleRingEpoch fence
// + sender-startup gate that all routed compute-node RPCs depend on.
//
// Cases covered:
//   1. RingEpoch equality / inequality / ordering.
//   2. {0,0} sentinel detection (uninitialised).
//   3. Monotonic ordering across (epoch bump) and (ringRev bump).
//   4. CompareRingEpoch — Equal / SenderStale / ReceiverStale /
//      SenderUninitialised.
//   5. Empty-ring sender gate: a node whose ring view is {0,0} must
//      be told "uninitialised" by CompareRingEpoch regardless of the
//      receiver's local view.
//   6. Wire round-trip preserves (epoch, ringRev).
//   7. ConsistentHashRing carries an epoch tag end-to-end.
//   8. RingUpdateMsg.AsEpoch() exposes (ringVersion, ringRev).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/RingEpoch.h"
#include "inc/Core/SPANN/Distributed/ConsistentHashRing.h"
#include "inc/Core/SPANN/Distributed/DistributedProtocol.h"

#include <cstdint>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::SPANN;

BOOST_AUTO_TEST_SUITE(RingEpochFenceTest)

// ----- 1. Equality / ordering ------------------------------------------

BOOST_AUTO_TEST_CASE(EpochEquality) {
    RingEpoch a{1, 0};
    RingEpoch b{1, 0};
    RingEpoch c{1, 1};
    RingEpoch d{2, 0};

    BOOST_CHECK(a == b);
    BOOST_CHECK(a != c);
    BOOST_CHECK(a != d);
    BOOST_CHECK(a < c);
    BOOST_CHECK(c < d);
    BOOST_CHECK(a < d);
}

BOOST_AUTO_TEST_CASE(MonotonicRingRevWithinEpoch) {
    RingEpoch e0{5, 0};
    RingEpoch e1{5, 1};
    RingEpoch e2{5, 2};
    BOOST_CHECK(e0 < e1);
    BOOST_CHECK(e1 < e2);
    BOOST_CHECK(!(e2 < e0));
}

BOOST_AUTO_TEST_CASE(EpochBumpResetsRingRevOrdering) {
    // (1, 999) is older than (2, 0): epoch dominates ringRev.
    RingEpoch oldHigh{1, 999};
    RingEpoch newLow{2, 0};
    BOOST_CHECK(oldHigh < newLow);
    BOOST_CHECK(!(newLow < oldHigh));
}

// ----- 2. Zero-sentinel ------------------------------------------------

BOOST_AUTO_TEST_CASE(ZeroSentinelDetected) {
    RingEpoch zero;
    BOOST_CHECK(zero.IsZero());
    BOOST_CHECK(!zero.IsInitialised());

    RingEpoch nonzero{1, 0};
    BOOST_CHECK(!nonzero.IsZero());
    BOOST_CHECK(nonzero.IsInitialised());

    // {0, 1} is also "initialised" — only {0, 0} is the sentinel.
    RingEpoch onlyRev{0, 1};
    BOOST_CHECK(!onlyRev.IsZero());
    BOOST_CHECK(onlyRev.IsInitialised());
}

// ----- 3. CompareRingEpoch -----------------------------------------

BOOST_AUTO_TEST_CASE(CompareEqual) {
    BOOST_CHECK(CompareRingEpoch((RingEpoch(3, 4)), (RingEpoch(3, 4)))
                == RingEpochCompare::Equal);
}

BOOST_AUTO_TEST_CASE(CompareSenderStaleByEpoch) {
    BOOST_CHECK(CompareRingEpoch((RingEpoch(1, 5)), (RingEpoch(2, 0)))
                == RingEpochCompare::SenderStale);
}

BOOST_AUTO_TEST_CASE(CompareSenderStaleByRingRev) {
    BOOST_CHECK(CompareRingEpoch((RingEpoch(2, 1)), (RingEpoch(2, 4)))
                == RingEpochCompare::SenderStale);
}

BOOST_AUTO_TEST_CASE(CompareReceiverStaleTriggersRefresh) {
    // Sender ahead → receiver should refresh itself.
    BOOST_CHECK(CompareRingEpoch((RingEpoch(2, 3)), (RingEpoch(1, 9)))
                == RingEpochCompare::ReceiverStale);
    BOOST_CHECK(CompareRingEpoch((RingEpoch(2, 5)), (RingEpoch(2, 4)))
                == RingEpochCompare::ReceiverStale);
}

BOOST_AUTO_TEST_CASE(CompareSenderUninitialisedAlwaysWins) {
    // Sender {0,0} is rejected regardless of local view, including local {0,0}.
    BOOST_CHECK(CompareRingEpoch(RingEpoch{}, RingEpoch{})
                == RingEpochCompare::SenderUninitialised);
    BOOST_CHECK(CompareRingEpoch(RingEpoch{}, (RingEpoch(1, 0)))
                == RingEpochCompare::SenderUninitialised);
    BOOST_CHECK(CompareRingEpoch(RingEpoch{}, (RingEpoch(99, 99)))
                == RingEpochCompare::SenderUninitialised);
}

// ----- 4. Wire serialization round-trip ---------------------------------

BOOST_AUTO_TEST_CASE(WireRoundTripPreservesFields) {
    RingEpoch original{0xDEADBEEFu, 0x12345678u};
    std::vector<std::uint8_t> buf(RingEpoch::WireSize());
    auto* end = original.Write(buf.data());
    BOOST_CHECK_EQUAL(static_cast<std::size_t>(end - buf.data()),
                      RingEpoch::WireSize());

    RingEpoch decoded;
    auto* rend = decoded.Read(buf.data());
    BOOST_CHECK_EQUAL(static_cast<std::size_t>(rend - buf.data()),
                      RingEpoch::WireSize());
    BOOST_CHECK(decoded == original);
}

BOOST_AUTO_TEST_CASE(WireSizeIsEightBytes) {
    BOOST_CHECK_EQUAL(RingEpoch::WireSize(), 8u);
}

// ----- 5. ConsistentHashRing carries an epoch ---------------------------

BOOST_AUTO_TEST_CASE(ConsistentHashRingEpochTag) {
    ConsistentHashRing ring(8);
    BOOST_CHECK(!ring.IsEpochInitialised());
    BOOST_CHECK(ring.GetEpoch().IsZero());

    ring.AddNode(1);
    ring.AddNode(2);
    ring.SetEpoch((RingEpoch(7, 3)));

    BOOST_CHECK(ring.IsEpochInitialised());
    BOOST_CHECK((ring.GetEpoch() == (RingEpoch(7, 3))));
}

// ----- 6. RingUpdateMsg exposes its epoch ---------------------------

BOOST_AUTO_TEST_CASE(RingUpdateMsgAsEpoch) {
    RingUpdateMsg msg;
    msg.m_ringVersion = 4;
    msg.m_ringRev = 2;
    msg.m_vnodeCount = 16;
    msg.m_nodeIndices = {1, 2, 3};

    auto e = msg.AsEpoch();
    BOOST_CHECK_EQUAL(e.epoch, 4u);
    BOOST_CHECK_EQUAL(e.ringRev, 2u);
}

// ----- 7. Sender-startup gate semantic check ----------------------------
// The sender-startup gate is "if local epoch is zero, refuse routed RPC".
// The pure check is RingEpoch::IsInitialised() (used by RemotePostingOps).
// We cover the full RPC plumbing via the live distributed harness; this
// test pins the predicate that drives the gate.

BOOST_AUTO_TEST_CASE(SenderStartupGatePredicate) {
    RingEpoch preInit;             // pre-Initialize state
    BOOST_CHECK(!preInit.IsInitialised());
    // After dispatcher tags the ring with epoch=1, the gate opens.
    RingEpoch postInit{1, 0};
    BOOST_CHECK(postInit.IsInitialised());
}

// ----- 8. Stale-receiver-self-refresh trigger ---------------------------
// When a receiver sees a sender ahead of itself, CompareRingEpoch
// returns ReceiverStale; that's the signal the receiver uses to ask
// the dispatcher for the latest ring snapshot. We verify the signal
// fires across both epoch and ringRev gaps.

BOOST_AUTO_TEST_CASE(ReceiverStaleAcrossEpochAndRingRev) {
    BOOST_CHECK(CompareRingEpoch((RingEpoch(5, 0)), (RingEpoch(3, 8)))
                == RingEpochCompare::ReceiverStale);
    BOOST_CHECK(CompareRingEpoch((RingEpoch(5, 9)), (RingEpoch(5, 8)))
                == RingEpochCompare::ReceiverStale);
    // Equal must NOT trigger refresh.
    BOOST_CHECK(CompareRingEpoch((RingEpoch(5, 8)), (RingEpoch(5, 8)))
                == RingEpochCompare::Equal);
}

BOOST_AUTO_TEST_SUITE_END()
