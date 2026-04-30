// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: compute-worker-zombie-after-suspect
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//        compute-worker-zombie-after-suspect.md
// Branch: fault/compute-worker-zombie-after-suspect
//
// Sibling of compute-worker-restart-rejoin. Same primitives
// (prim/swim-membership + prim/ring-epoch-fence) but a different
// failure-mode anchor:
//
//   * rejoin   = a worker that truly died, then came back with a
//                strictly-higher SWIM incarnation; the cluster
//                must reinstate it.
//   * zombie   = a worker that the dispatcher has already marked
//                Dead at incarnation D, but is still alive and
//                continuing to issue routed writes carrying its
//                OLD incarnation (≤ D); the receiver must refuse
//                those writes without polluting the posting list.
//
// The implementation hook is RemotePostingOps::SendRemoteAppend
// WithZombieFence. It is env-gated by
//   SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT
// so the production hot path is byte-identical when the env is
// unset.
//
// Tier 1 invariants exercised here (all five spec cases):
//   1. env-off dormancy: wrapper is a straight pass-through; no
//      counters move, no state mutated.
//   2. Zombie write rejected: senderIncarnation ≤ deadIncarnation
//      → ErrorCode::Fenced + zombieFenceRejected bump. Receiver
//      sendFn never invoked.
//   3. Legitimate rejoin succeeds: senderIncarnation > deadInc
//      → wrapper forwards; zombieIncarnationBumped bumped. The
//      observed dead-incarnation is NOT regressed (subsequent
//      stragglers at ≤ D remain fenced).
//   4. Concurrent zombie + rejoin: two simultaneous wrapper calls
//      for the same nodeId — only the rejoin (higher incarnation)
//      reaches sendFn; the zombie is fenced. No interleaving can
//      let the zombie through.
//   5. TiKV harness smoke: real RawPut/RawGet round-trip via
//      ExtraTiKVController (gated on TIKV macro + harness env).
//
// Both env-off (SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT
// unset) and env-armed (=1) execute against the same wrapper
// surface; cases 1 + 5 cover env-off behaviour and the rest cover
// env-armed.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/RemotePostingOps.h"
#include "inc/Core/SPANN/Distributed/RingEpoch.h"
#include "inc/Core/SPANN/Distributed/SwimDetector.h"

#ifdef TIKV
#include "inc/Core/SPANN/ExtraTiKVController.h"
#endif

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::SPANN;

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

bool FaultEnvArmed() {
    return std::getenv("SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT")
           != nullptr;
}

} // namespace

BOOST_AUTO_TEST_SUITE(ComputeWorkerZombieAfterSuspectTest)

// ---------------------------------------------------------------------------
// Invariant #1 — env-off dormancy.
// Spec: production hot path is byte-identical to baseline when the env
// gate is unset. The wrapper short-circuits to sendFn unconditionally;
// no counter is touched, no observation is recorded.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ZombieFenceEnvOffIsDormant)
{
    ScopedEnv off("SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT", nullptr);
    BOOST_REQUIRE(!RemotePostingOps::ZombieFenceArmed());

    RemotePostingOps ops;
    ops.ResetZombieFenceForTest();

    // Even if the receiver "knew" the sender was dead, env-off => no
    // fence is applied. The wrapper must call through.
    ops.ObserveSwimDeadIncarnation(/*senderNodeId=*/2, /*deadInc=*/9);

    int seenSender = -1;
    std::uint64_t seenInc = 0;
    auto sendFn = [&](int s, std::uint64_t i) {
        seenSender = s;
        seenInc = i;
        return ErrorCode::Success;
    };

    ErrorCode rc = ops.SendRemoteAppendWithZombieFence(
        /*senderNodeId=*/2, /*senderIncarnation=*/3, sendFn);
    BOOST_CHECK(rc == ErrorCode::Success);
    BOOST_CHECK_EQUAL(seenSender, 2);
    BOOST_CHECK_EQUAL(seenInc, 3u);

    auto c = ops.GetZombieFenceCounters();
    BOOST_CHECK_EQUAL(c.fenceRejected, 0u);
    BOOST_CHECK_EQUAL(c.incarnationBumped, 0u);

    BOOST_TEST_MESSAGE("env-off: wrapper dormant, hot path byte-identical");
}

// ---------------------------------------------------------------------------
// Invariant #2 — zombie write rejected at the fence.
// Spec "Failure mode" + "Expected behaviour" #1: a sender carrying an
// incarnation ≤ the observed Dead incarnation must be refused; the
// underlying sendFn (which would have hit RawPut on TiKV) MUST NOT be
// invoked. The fenceRejected counter must increment by exactly 1 per
// rejected RPC.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ZombieWriteRejectedWithOldIncarnation)
{
    ScopedEnv on("SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT", "1");
    BOOST_REQUIRE(RemotePostingOps::ZombieFenceArmed());

    RemotePostingOps ops;
    ops.ResetZombieFenceForTest();

    // Cluster has marked node 2 Dead at incarnation 7 (via SWIM). The
    // ring observer pushed that observation into RemotePostingOps.
    ops.ObserveSwimDeadIncarnation(/*senderNodeId=*/2, /*deadInc=*/7);
    BOOST_REQUIRE_EQUAL(ops.GetObservedDeadIncarnationForTest(2), 7u);

    int sendInvocations = 0;
    auto sendFn = [&](int, std::uint64_t) {
        ++sendInvocations;
        return ErrorCode::Success;
    };

    // Strictly older incarnation: classic zombie. Refused.
    BOOST_CHECK(ops.SendRemoteAppendWithZombieFence(2, /*inc=*/3, sendFn)
                == ErrorCode::Fenced);
    // Equal-to-dead incarnation: also refused (the worker we declared
    // Dead at inc=7 IS the worker we are now hearing from at inc=7).
    BOOST_CHECK(ops.SendRemoteAppendWithZombieFence(2, /*inc=*/7, sendFn)
                == ErrorCode::Fenced);

    // sendFn never invoked — no posting-list pollution.
    BOOST_CHECK_EQUAL(sendInvocations, 0);

    auto c = ops.GetZombieFenceCounters();
    BOOST_CHECK_EQUAL(c.fenceRejected, 2u);
    BOOST_CHECK_EQUAL(c.incarnationBumped, 0u);

    // A different sender (nodeId=99) is unrelated; its writes pass.
    BOOST_CHECK(ops.SendRemoteAppendWithZombieFence(99, /*inc=*/1, sendFn)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sendInvocations, 1);
}

// ---------------------------------------------------------------------------
// Invariant #3 — legitimate rejoin succeeds.
// Spec "Recovery semantics" #5: after a rejoin, B's SWIM incarnation
// strictly advances past the observed-Dead incarnation. The wrapper
// must let those writes through (incarnationBumped counter increments)
// while still keeping the deadIncarnation high-water so subsequent
// stragglers at ≤ D remain fenced.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(LegitimateRejoinIncarnationBumpSucceeds)
{
    ScopedEnv on("SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT", "1");
    RemotePostingOps ops;
    ops.ResetZombieFenceForTest();

    ops.ObserveSwimDeadIncarnation(/*senderNodeId=*/2, /*deadInc=*/7);

    int sendInvocations = 0;
    auto sendFn = [&](int, std::uint64_t) {
        ++sendInvocations;
        return ErrorCode::Success;
    };

    // Rejoin: incarnation 8 > observed dead 7. Admitted.
    BOOST_CHECK(ops.SendRemoteAppendWithZombieFence(2, /*inc=*/8, sendFn)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sendInvocations, 1);

    {
        auto c = ops.GetZombieFenceCounters();
        BOOST_CHECK_EQUAL(c.fenceRejected, 0u);
        BOOST_CHECK_EQUAL(c.incarnationBumped, 1u);
    }

    // After legitimate rejoin, a straggler RPC carrying the OLD
    // incarnation (5 ≤ 7) is STILL fenced — the dead-incarnation
    // high-water must not be regressed by the rejoin.
    BOOST_CHECK(ops.SendRemoteAppendWithZombieFence(2, /*inc=*/5, sendFn)
                == ErrorCode::Fenced);
    BOOST_CHECK_EQUAL(sendInvocations, 1);
    {
        auto c = ops.GetZombieFenceCounters();
        BOOST_CHECK_EQUAL(c.fenceRejected, 1u);
        BOOST_CHECK_EQUAL(c.incarnationBumped, 1u);
    }

    // Wire the SwimDetector → RemotePostingOps observation path. After
    // a real rejoin event, prim/swim-membership reports MemberChange::
    // BecameAlive with strictly bumped incarnation; our wrapper has
    // already admitted the rejoin RPC. We assert the public-API path
    // (ObserveSwimDeadIncarnation is monotonic and handles bursts):
    ops.ObserveSwimDeadIncarnation(/*senderNodeId=*/2, /*deadInc=*/4); // older — ignored
    BOOST_CHECK_EQUAL(ops.GetObservedDeadIncarnationForTest(2), 7u);
}

// ---------------------------------------------------------------------------
// Invariant #4 — concurrent zombie + rejoin: only the rejoin succeeds.
// Spec rationale: in the live cluster, a rejoin and a zombie-tail can
// race. The wrapper must enforce the contract under thread-level
// concurrency: two simultaneous calls for senderNodeId=2 with
// incarnations 8 (rejoin) and 5 (zombie) on top of a deadInc=7
// observation must produce { Success, Fenced } in any interleaving —
// never { Success, Success }.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ConcurrentZombieAndRejoinOnlyRejoinSucceeds)
{
    ScopedEnv on("SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT", "1");
    RemotePostingOps ops;
    ops.ResetZombieFenceForTest();

    ops.ObserveSwimDeadIncarnation(/*senderNodeId=*/2, /*deadInc=*/7);

    std::atomic<int> sendCalls{0};
    auto sendFn = [&](int, std::uint64_t) -> ErrorCode {
        sendCalls.fetch_add(1);
        // Tiny pause to widen the interleaving window.
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return ErrorCode::Success;
    };

    constexpr int kPairs = 32;
    std::vector<std::thread> threads;
    std::atomic<int> rejoinSuccess{0};
    std::atomic<int> rejoinFenced{0};
    std::atomic<int> zombieSuccess{0};
    std::atomic<int> zombieFenced{0};

    for (int i = 0; i < kPairs; ++i) {
        threads.emplace_back([&] {
            ErrorCode rc = ops.SendRemoteAppendWithZombieFence(2, 8, sendFn);
            (rc == ErrorCode::Success ? rejoinSuccess : rejoinFenced)
                .fetch_add(1);
        });
        threads.emplace_back([&] {
            ErrorCode rc = ops.SendRemoteAppendWithZombieFence(2, 5, sendFn);
            (rc == ErrorCode::Success ? zombieSuccess : zombieFenced)
                .fetch_add(1);
        });
    }
    for (auto& t : threads) t.join();

    // Every rejoin (inc=8 > 7) is admitted.
    BOOST_CHECK_EQUAL(rejoinSuccess.load(), kPairs);
    BOOST_CHECK_EQUAL(rejoinFenced.load(), 0);
    // Every zombie (inc=5 ≤ 7) is fenced — never reaches sendFn.
    BOOST_CHECK_EQUAL(zombieSuccess.load(), 0);
    BOOST_CHECK_EQUAL(zombieFenced.load(), kPairs);

    auto c = ops.GetZombieFenceCounters();
    BOOST_CHECK_EQUAL(c.fenceRejected, (std::uint64_t)kPairs);
    BOOST_CHECK_EQUAL(c.incarnationBumped, (std::uint64_t)kPairs);
    // Exactly one sendFn invocation per legitimate rejoin attempt.
    BOOST_CHECK_EQUAL(sendCalls.load(), kPairs);

    // Cross-primitive composition: the zombie's stale-inc claim must not
    // be representable as a SWIM "Dead at higher inc" update either,
    // since prim/swim-membership rejects strictly-older incarnation
    // updates. Smoke that property here against SwimDetector to keep
    // this test honest on the prim seam.
    SwimConfig cfg;
    cfg.indirect_fanout = 0;
    cfg.suspect_after_ticks = 1000;
    cfg.dead_after_ticks = 10000;
    SwimDetector peer(0, std::vector<int>{2}, cfg);
    std::vector<std::pair<int, MemberState>> postRejoin{
        {2, MemberState{MemberHealth::Alive, /*inc=*/8, 0, 0}}};
    peer.OnPing(99, postRejoin);
    std::vector<std::pair<int, MemberState>> staleZombie{
        {2, MemberState{MemberHealth::Dead, /*inc=*/3, 0, 0}}};
    peer.OnAck(99, staleZombie);
    MemberState st;
    BOOST_REQUIRE(peer.TryGetMember(2, st));
    BOOST_CHECK(st.health == MemberHealth::Alive);
    BOOST_CHECK_EQUAL(st.incarnation, 8u);
}

// ---------------------------------------------------------------------------
// Invariant #5 — TiKV harness smoke (real KV round-trip).
// Gated on the TIKV build macro AND on TIKV_PD_ADDRESSES env (set by
// the harness wrapper script). This proves the wrapper's caller path
// composes with a real TiKV backend; the wrapper itself is in-process,
// but the spec asks for one full round-trip to confirm no surprise
// linkage. Skipped silently when the harness is not available.
// ---------------------------------------------------------------------------
#ifdef TIKV
BOOST_AUTO_TEST_CASE(TiKVHarnessSmokeWriteRead) {
    const char* pd = std::getenv("TIKV_PD_ADDRESSES");
    if (!pd || std::string(pd).empty()) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping TiKV smoke");
        return;
    }
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string prefix =
        "fault_compute_worker_zombie_after_suspect_smoke_" +
        std::to_string(now) + "_";
    auto io = std::make_shared<TiKVIO>(std::string(pd), prefix);
    if (!io->Available()) {
        BOOST_TEST_MESSAGE("TiKV unreachable; skipping smoke");
        return;
    }
    BOOST_REQUIRE(io->Put("k", "v",
                          std::chrono::microseconds(2000000), nullptr)
                  == ErrorCode::Success);
    std::string out;
    BOOST_REQUIRE(io->Get("k", &out,
                          std::chrono::microseconds(2000000), nullptr)
                  == ErrorCode::Success);
    BOOST_CHECK_EQUAL(out, "v");
}
#endif

BOOST_AUTO_TEST_SUITE_END()
