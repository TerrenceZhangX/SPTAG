// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: compute-driver-die-mid-dispatch
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/compute-driver-die-mid-dispatch.md
// Branch: fault/compute-driver-die-mid-dispatch
//
// Scenario: the driver/dispatcher process dies mid dispatch round. Workers
// detect the loss via SwimDetector (Suspect → Dead transition) and either
// drain their queued batches (full payload + valid ring) or NACK them
// back to the retry queue. A zombie/stale dispatcher resurfacing under
// an old ring-version is fenced at the worker side via CompareRingEpoch
// (prim/ring-epoch-fence).
//
// Tier 1 invariants asserted by this suite:
//   1. env-off path is dormancy: no behavioural change, all four
//      counters stay at 0.
//   2. env-on baseline (death disabled): dispatch round completes
//      successfully; no batch lost or double-applied.
//   3. env-on death path: m_driverDied bumps once;
//      m_batchesDrained + m_batchesNacked == in-flight remaining;
//      no batch is double-applied.
//   4. zombie path: stale-ring writes are rejected;
//      m_ringFenceRejected > 0; cluster state remains coherent.
//   5. counters reset between cases; SwimDetector observer for the
//      dispatcher wires correctly.
//
// Tier 2 (1M perf): DEFERRED — non hot-path; the wrapper does not run
// inside the SPANN search inner loop.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/DriverDispatchGuard.h"
#include "inc/Core/SPANN/Distributed/RingEpoch.h"
#include "inc/Core/SPANN/Distributed/SwimDetector.h"

#include <atomic>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::SPANN;
using namespace SPTAG::SPANN::Distributed;

namespace {

class ScopedEnv {
public:
    ScopedEnv(const char* name, const char* value) : m_name(name) {
        const char* prior = std::getenv(name);
        m_had = (prior != nullptr);
        if (m_had) m_prior = prior;
        if (value) ::setenv(name, value, 1);
        else        ::unsetenv(name);
    }
    ~ScopedEnv() {
        if (m_had) ::setenv(m_name.c_str(), m_prior.c_str(), 1);
        else       ::unsetenv(m_name.c_str());
    }
private:
    std::string m_name;
    std::string m_prior;
    bool m_had = false;
};

// Construct a deterministic dispatch round of N batches owned by
// round-robin worker ids, all under the same sender ring view.
std::vector<DispatchBatch> MakeBatches(std::size_t n,
                                       const RingEpoch& ring,
                                       int firstWorker = 1,
                                       int numWorkers = 3,
                                       bool payloadComplete = true)
{
    std::vector<DispatchBatch> out;
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        DispatchBatch b;
        b.batchId = static_cast<std::uint64_t>(i + 1);
        b.ownerWorker = firstWorker + static_cast<int>(i % numWorkers);
        b.ringAtSend = ring;
        b.payloadComplete = payloadComplete;
        out.push_back(b);
    }
    return out;
}

// Drive the SwimDetector forward enough ticks to force `peer` to
// Suspect then Dead. Mirrors the helper used in swim-detector-lag.
void DriveDispatcherToDead(SwimDetector& d, int peer,
                           std::uint64_t suspectTicks,
                           std::uint64_t deadTicks)
{
    for (std::uint64_t i = 0; i < suspectTicks + deadTicks + 2; ++i) {
        (void)d.Tick();
    }
    MemberState st;
    BOOST_REQUIRE(d.TryGetMember(peer, st));
    BOOST_REQUIRE_EQUAL(static_cast<int>(st.health),
                        static_cast<int>(MemberHealth::Dead));
}

}  // namespace

BOOST_AUTO_TEST_SUITE(ComputeDriverDieMidDispatchTest)

// ---------------------------------------------------------------------------
// 1) EnvOffDormancy. With SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH unset
//    the wrapper must run the dispatch round to completion regardless of
//    any death schedule the caller would have configured under armed mode.
//    All four counters stay at zero; behaviour is identical to baseline.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH", nullptr);
    BOOST_REQUIRE(!DriverDispatchGuardArmed());

    DriverDispatchGuard guard;
    // Even though we set a death schedule, the env gate is off so the
    // schedule is not honoured.
    guard.SetDeathSchedule(2);

    const RingEpoch ring{3, 1};
    auto batches = MakeBatches(/*n=*/6, ring);

    int sent = 0;
    auto sendOne = [&](const DispatchBatch& b) {
        (void)b;
        ++sent;
        return ErrorCode::Success;
    };

    std::vector<DispatchBatch> remaining;
    ErrorCode rc = guard.RunDispatchRound(batches, sendOne, &remaining);
    BOOST_CHECK(rc == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sent, 6);
    BOOST_CHECK(remaining.empty());

    auto snap = guard.Counters().Get();
    BOOST_CHECK_EQUAL(snap.driverDied, 0u);
    BOOST_CHECK_EQUAL(snap.batchesDrained, 0u);
    BOOST_CHECK_EQUAL(snap.batchesNacked, 0u);
    BOOST_CHECK_EQUAL(snap.ringFenceRejected, 0u);
}

// ---------------------------------------------------------------------------
// 2) BaselineDispatchSucceeds. Env armed but the death schedule is the
//    control sub-toggle (dieAfterN==0 → death disabled). Dispatch round
//    completes; m_driverDied stays 0; all batches delivered exactly once.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineDispatchSucceeds)
{
    ScopedEnv env("SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH", "1");
    BOOST_REQUIRE(DriverDispatchGuardArmed());

    DriverDispatchGuard guard;
    guard.SetDeathSchedule(0);  // explicit no-death control

    const RingEpoch ring{4, 1};
    auto batches = MakeBatches(/*n=*/8, ring);

    std::unordered_set<std::uint64_t> applied;
    auto sendOne = [&](const DispatchBatch& b) {
        // No worker should see the same batchId twice.
        BOOST_CHECK(applied.insert(b.batchId).second);
        return ErrorCode::Success;
    };

    std::vector<DispatchBatch> remaining;
    ErrorCode rc = guard.RunDispatchRound(batches, sendOne, &remaining);
    BOOST_CHECK(rc == ErrorCode::Success);
    BOOST_CHECK_EQUAL(applied.size(), 8u);
    BOOST_CHECK(remaining.empty());

    auto snap = guard.Counters().Get();
    BOOST_CHECK_EQUAL(snap.driverDied, 0u);
    BOOST_CHECK_EQUAL(snap.batchesDrained, 0u);
    BOOST_CHECK_EQUAL(snap.batchesNacked, 0u);
    BOOST_CHECK_EQUAL(snap.ringFenceRejected, 0u);
}

// ---------------------------------------------------------------------------
// 3) DriverDiesQueuedBatchesDrain. Driver dies after batch 3 of 8. The
//    SwimDetector observer fires once the dispatcher node transitions to
//    Suspect / Dead; the worker recovery routine decides drain vs nack
//    per batch. Invariants:
//      * m_driverDied == 1
//      * m_batchesDrained + m_batchesNacked == remaining (5)
//      * no double-apply: applied set contains every batch at most once
//      * no batch lost: applied ∪ nacked == full input set
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(DriverDiesQueuedBatchesDrain)
{
    ScopedEnv env("SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH", "1");
    BOOST_REQUIRE(DriverDispatchGuardArmed());

    DriverDispatchGuard guard;
    constexpr std::uint32_t kDieAfter = 3;
    guard.SetDeathSchedule(kDieAfter);

    const RingEpoch ring{5, 1};
    auto batches = MakeBatches(/*n=*/8, ring);

    std::unordered_set<std::uint64_t> applied;
    auto sendOne = [&](const DispatchBatch& b) {
        BOOST_CHECK(applied.insert(b.batchId).second);
        return ErrorCode::Success;
    };

    // SwimDetector wiring: dispatcher (id=42) is driven to Dead; the
    // observer flips a flag the recovery code waits on.
    SwimDetector det(/*self=*/1, /*peers=*/{42, 2, 3});
    bool dispatcherLossSeen = false;
    DriverDispatchGuard::ObserveDispatcherLoss(
        det, /*dispatcherNodeId=*/42,
        [&]() { dispatcherLossSeen = true; });

    std::vector<DispatchBatch> remaining;
    ErrorCode rc = guard.RunDispatchRound(batches, sendOne, &remaining);
    BOOST_CHECK(rc == ErrorCode::DriverLost);
    BOOST_CHECK_EQUAL(applied.size(), kDieAfter);
    BOOST_CHECK_EQUAL(remaining.size(), batches.size() - kDieAfter);

    // Workers detect dispatcher loss via SWIM.
    DriveDispatcherToDead(det, /*peer=*/42, /*suspect=*/1, /*dead=*/3);
    BOOST_CHECK(dispatcherLossSeen);

    // Mark the first two remaining as payload-complete, last three as
    // not-yet-complete (exercising both drain and NACK paths).
    for (std::size_t i = 0; i < remaining.size(); ++i) {
        remaining[i].payloadComplete = (i < 2);
    }

    auto recovery = guard.WorkerRecoverInFlight(remaining, ring);
    BOOST_REQUIRE_EQUAL(recovery.size(), remaining.size());

    std::size_t drained = 0, nacked = 0;
    for (auto r : recovery) {
        if (r == BatchRecovery::Drained) ++drained;
        else if (r == BatchRecovery::Nacked) ++nacked;
        else BOOST_FAIL("unexpected recovery decision under non-zombie path");
    }
    BOOST_CHECK_EQUAL(drained, 2u);
    BOOST_CHECK_EQUAL(nacked, 3u);

    auto snap = guard.Counters().Get();
    BOOST_CHECK_EQUAL(snap.driverDied, 1u);
    BOOST_CHECK_EQUAL(snap.batchesDrained, drained);
    BOOST_CHECK_EQUAL(snap.batchesNacked, nacked);
    BOOST_CHECK_EQUAL(snap.batchesDrained + snap.batchesNacked,
                      remaining.size());
    // No fence trip on this path: dispatcher's ring matched cluster ring.
    BOOST_CHECK_EQUAL(snap.ringFenceRejected, 0u);

    // No batch lost: applied ∪ remaining (drained+nacked) covers the
    // full input set without duplicates.
    std::unordered_set<std::uint64_t> total(applied);
    for (const auto& b : remaining) total.insert(b.batchId);
    BOOST_CHECK_EQUAL(total.size(), batches.size());
}

// ---------------------------------------------------------------------------
// 4) ZombieDriverFenced. After the new driver bumps the cluster ring to
//    epoch=E+1, a stale (zombie) driver appears alive briefly and tries
//    to push batches under the old ring. The worker-side fence
//    (CompareRingEpoch from prim/ring-epoch-fence) rejects all such
//    writes; m_ringFenceRejected > 0; no zombie write is accepted.
//    Writes from the new driver under the fresh ring are accepted.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ZombieDriverFenced)
{
    ScopedEnv env("SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH", "1");
    BOOST_REQUIRE(DriverDispatchGuardArmed());

    DriverDispatchGuard guard;

    const RingEpoch oldRing{6, 1};       // zombie's cached view
    const RingEpoch clusterRing{7, 1};   // new driver took over

    // Sanity: the comparison underneath is the prim's
    // CompareRingEpoch. Sender(old) vs Local(new) → SenderStale.
    BOOST_REQUIRE(CompareRingEpoch(oldRing, clusterRing) ==
                  RingEpochCompare::SenderStale);

    // Three zombie batches should all be fenced.
    constexpr int kZombieBatches = 3;
    int accepted = 0;
    int rejected = 0;
    for (int i = 0; i < kZombieBatches; ++i) {
        if (guard.AcceptUnderRingFence(oldRing, clusterRing)) ++accepted;
        else                                                  ++rejected;
    }
    BOOST_CHECK_EQUAL(accepted, 0);
    BOOST_CHECK_EQUAL(rejected, kZombieBatches);

    // The new driver, under the fresh ring, is accepted.
    BOOST_CHECK(guard.AcceptUnderRingFence(clusterRing, clusterRing));

    // Uninitialised sender (RingEpoch{}) is rejected per prim contract.
    BOOST_CHECK(!guard.AcceptUnderRingFence(RingEpoch{}, clusterRing));

    auto snap = guard.Counters().Get();
    BOOST_CHECK_EQUAL(snap.driverDied, 0u);
    BOOST_CHECK_EQUAL(snap.batchesDrained, 0u);
    BOOST_CHECK_EQUAL(snap.batchesNacked, 0u);
    // 3 stale + 1 uninit = 4 fence rejections.
    BOOST_CHECK_EQUAL(snap.ringFenceRejected, 4u);
}

// ---------------------------------------------------------------------------
// 5) HarnessSmoke. Counters reset across cases (guarded by per-test
//    DriverDispatchGuard instances), env gate is honoured precisely on
//    [unset, "0", "1"], and SwimDetector observer wiring is stable
//    across multiple Tick() rounds without leaking callbacks.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(HarnessSmoke)
{
    // Env-gate parsing.
    {
        ScopedEnv e("SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH", nullptr);
        BOOST_CHECK(!DriverDispatchGuardArmed());
    }
    {
        ScopedEnv e("SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH", "0");
        BOOST_CHECK(!DriverDispatchGuardArmed());
    }
    {
        ScopedEnv e("SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH", "1");
        BOOST_CHECK(DriverDispatchGuardArmed());
    }

    // Per-case counter independence.
    DriverDispatchGuard a;
    DriverDispatchGuard b;
    a.Counters().m_driverDied.fetch_add(1, std::memory_order_relaxed);
    BOOST_CHECK_EQUAL(a.Counters().Get().driverDied, 1u);
    BOOST_CHECK_EQUAL(b.Counters().Get().driverDied, 0u);
    a.Counters().Reset();
    BOOST_CHECK_EQUAL(a.Counters().Get().driverDied, 0u);

    // SwimDetector observer wiring: many ticks, observer never fires
    // for a peer that stays Alive (we keep marking it via OnAck).
    SwimDetector det(/*self=*/0, /*peers=*/{99});
    int losses = 0;
    DriverDispatchGuard::ObserveDispatcherLoss(
        det, /*dispatcherNodeId=*/99,
        [&]() { ++losses; });
    for (int i = 0; i < 5; ++i) {
        det.OnAck(99, {});
        (void)det.Tick();
    }
    BOOST_CHECK_EQUAL(losses, 0);

    // Now drive the dispatcher Dead and confirm exactly one transition
    // (Suspect → Dead are both reported, so the count is ≥1 not ==1).
    DriveDispatcherToDead(det, /*peer=*/99, /*suspect=*/1, /*dead=*/3);
    BOOST_CHECK_GE(losses, 1);
}

BOOST_AUTO_TEST_SUITE_END()
