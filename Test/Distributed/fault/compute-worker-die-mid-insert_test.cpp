// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: compute-worker-die-mid-insert
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//        compute-worker-die-mid-insert.md
// Branch: fault/compute-worker-die-mid-insert
//
// Sibling of compute-worker-restart-rejoin (lifecycle) and
// compute-worker-zombie-after-suspect (process). This case targets
// the *insert hot path*: a compute worker is SIGKILLed mid online-
// insert with in-flight Path-A local appends and Path-B remote-
// appends. Recovery must converge via three orthogonal mechanisms:
//
//   (a) op-id dedup       (prim/op-id-idempotency)
//   (b) ring-epoch fence  (prim/ring-epoch-fence)
//   (c) SWIM membership   (prim/swim-membership)
//
// The implementation hook is RemotePostingOps::SendRemoteAppendWith
// KillResumeFence. It is env-gated by
//   SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT
// so the production hot path is byte-identical when the env is
// unset.
//
// Tier 1 invariants exercised here (all five spec cases):
//   1. EnvOffDormancy        -- wrapper short-circuits to sendFn,
//                               no counter touches, no caching.
//   2. BaselineNoKill        -- env-armed but no kill observed: an
//                               insert flows through; no fence, no
//                               replay; first attempt succeeds.
//   3. KillMidInsertResumes  -- worker B is killed mid-batch, the
//                               op-id resume cache lets a retry of
//                               the same OpId replay the cached
//                               Success without re-invoking sendFn.
//   4. KillThenZombieFenced  -- after the kill is observed (ring
//                               epoch advanced), a sender still on
//                               the pre-kill ring view is rejected
//                               Fenced; m_ringFenceRejected bumps.
//   5. HarnessSmoke          -- the SWIM detector + ring-epoch
//                               primitives compose end-to-end: kill
//                               -> SwimDetector marks B Dead ->
//                               observer bumps RingEpoch -> wrapper
//                               picks up ringEpochAtKill and fences
//                               stale senders; rejoin (legitimate
//                               resume on bumped epoch) is admitted.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/ConsistentHashRing.h"
#include "inc/Core/SPANN/Distributed/OpId.h"
#include "inc/Core/SPANN/Distributed/RemotePostingOps.h"
#include "inc/Core/SPANN/Distributed/RingEpoch.h"
#include "inc/Core/SPANN/Distributed/SwimDetector.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
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
    return std::getenv("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT")
           != nullptr;
}

// In-memory mesh used by the harness-smoke case (mirrors the cousin
// fault test). Kept local to this TU.
struct Mesh {
    std::map<int, SwimDetector*> nodes;
    std::set<std::pair<int,int>> dropped;

    void Add(int id, SwimDetector& d) { nodes[id] = &d; }
    void Replace(int id, SwimDetector& d) { nodes[id] = &d; }
    void Drop(int from, int to) {
        dropped.insert({from, to});
        dropped.insert({to, from});
    }
    void HealAll() { dropped.clear(); }

    std::vector<SwimMessage> DeliverOne(const SwimMessage& m) {
        if (dropped.count({m.from, m.to})) return {};
        auto it = nodes.find(m.to);
        if (it == nodes.end()) return {};
        SwimDetector& dst = *it->second;
        switch (m.kind) {
            case SwimMessage::Kind::Ping:
                return dst.OnPing(m.from, m.updates);
            case SwimMessage::Kind::Ack:
                dst.OnAck(m.from, m.updates);
                return {};
            case SwimMessage::Kind::PingReq:
                return dst.OnPingReq(m.target, m.from, m.updates);
            case SwimMessage::Kind::IndirectAck:
                dst.OnIndirectAck(m.target, m.updates);
                return {};
        }
        return {};
    }

    void Pump(std::uint64_t ticks, int depthCap = 16) {
        for (std::uint64_t t = 0; t < ticks; ++t) {
            std::vector<SwimMessage> q;
            for (auto& [id, d] : nodes) {
                auto out = d->Tick();
                for (auto& m : out) q.push_back(std::move(m));
            }
            int depth = 0;
            while (!q.empty() && depth++ < depthCap) {
                std::vector<SwimMessage> next;
                for (auto& m : q) {
                    auto reply = DeliverOne(m);
                    for (auto& r : reply) next.push_back(std::move(r));
                }
                q.swap(next);
            }
        }
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(ComputeWorkerDieMidInsertTest)

// ---------------------------------------------------------------------------
// Invariant #1 -- env-off dormancy.
// Spec: production hot path is byte-identical when the env gate is unset.
// The wrapper must call sendFn with the same opId argument and never
// touch any counter.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv off("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT", nullptr);
    BOOST_REQUIRE(!RemotePostingOps::KillResumeArmed());

    RemotePostingOps ops;
    ops.ResetKillResumeForTest();

    // ObserveWorkerDied is a pure observability hook -- it records
    // state (and bumps the m_workerDiedMidInsert observability counter)
    // independent of env-arming. The "dormant" property under env-off
    // is that the wrapper itself does NOT fence or replay anything --
    // sendFn is invoked unconditionally with the supplied opId.
    ops.ObserveWorkerDied(/*nodeIndex=*/2,
                          /*killInc=*/9,
                          /*ringEpochAtKill=*/RingEpoch{4, 0});
    BOOST_CHECK_EQUAL(ops.GetKillResumeCounters().workerDiedMidInsert, 1u);

    Distributed::OpId opA{/*sender=*/0, /*epoch=*/1, /*counter=*/1};
    int sendCalls = 0;
    auto sendFn = [&](const Distributed::OpId& id) {
        BOOST_CHECK_EQUAL(id.senderId, 0);
        BOOST_CHECK_EQUAL(id.monotonicCounter, 1u);
        ++sendCalls;
        return ErrorCode::Success;
    };

    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, RingEpoch{1, 0}, opA, sendFn)
                == ErrorCode::Success);
    // Same OpId again: env-off => no replay caching, sendFn called twice.
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, RingEpoch{1, 0}, opA, sendFn)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sendCalls, 2);

    // The wrapper itself touched no counters -- workerDiedMidInsert is
    // 1 from the explicit Observe call above; resumedFromOpId and
    // ringFenceRejected are 0 because the wrapper short-circuits to
    // sendFn under env-off.
    auto c = ops.GetKillResumeCounters();
    BOOST_CHECK_EQUAL(c.workerDiedMidInsert, 1u);
    BOOST_CHECK_EQUAL(c.resumedFromOpId, 0u);
    BOOST_CHECK_EQUAL(c.ringFenceRejected, 0u);

    BOOST_TEST_MESSAGE("env-off: wrapper dormant, hot path byte-identical");
}

// ---------------------------------------------------------------------------
// Invariant #2 -- baseline no-kill: env-armed wrapper passes inserts
// through cleanly when no kill has been observed. Counters stay at 0
// for fence + resume, and a successful insert is cached so an idempotent
// retry replays without invoking sendFn twice.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineNoKill)
{
    ScopedEnv on("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT", "1");
    BOOST_REQUIRE(RemotePostingOps::KillResumeArmed());

    RemotePostingOps ops;
    ops.ResetKillResumeForTest();

    Distributed::OpId opA{/*sender=*/0, /*epoch=*/1, /*counter=*/100};
    int sendCalls = 0;
    auto sendFn = [&](const Distributed::OpId&) {
        ++sendCalls;
        return ErrorCode::Success;
    };

    // No kill observed => no fence, no replay required.
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, RingEpoch{1, 0}, opA, sendFn)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sendCalls, 1);

    // Idempotent retry of the SAME opId replays from the resume cache;
    // sendFn is NOT invoked a second time. Counter bumps.
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, RingEpoch{1, 0}, opA, sendFn)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sendCalls, 1);

    auto c = ops.GetKillResumeCounters();
    BOOST_CHECK_EQUAL(c.workerDiedMidInsert, 0u);
    BOOST_CHECK_EQUAL(c.resumedFromOpId, 1u);
    BOOST_CHECK_EQUAL(c.ringFenceRejected, 0u);
}

// ---------------------------------------------------------------------------
// Invariant #3 -- kill mid-insert: a worker is killed while its own
// insert was in flight. The driver retries with the *same* OpId against
// the new owner (or against the same node after rejoin under a fresh
// ring view). The op-id resume cache replays the cached Success so the
// retry observes at-most-once semantics. sendFn is invoked exactly once
// across the kill+retry boundary.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(KillMidInsertResumes)
{
    ScopedEnv on("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT", "1");
    RemotePostingOps ops;
    ops.ResetKillResumeForTest();

    // Simulate the in-flight insert: original send Succeeds and is
    // cached, but from the driver's perspective the response was lost
    // (it received TIMEOUT(NODE_CRASH) when the TCP went down).
    Distributed::OpId opMid{/*sender=*/0, /*epoch=*/1, /*counter=*/42};
    int firstSendCalls = 0;
    auto firstSend = [&](const Distributed::OpId&) {
        ++firstSendCalls;
        return ErrorCode::Success;  // applied to TiKV before the kill
    };
    BOOST_REQUIRE(ops.SendRemoteAppendWithKillResumeFence(
                      /*targetNodeIndex=*/2, RingEpoch{1, 0}, opMid, firstSend)
                  == ErrorCode::Success);
    BOOST_REQUIRE_EQUAL(firstSendCalls, 1);

    // Now the kill happens. Membership observer publishes the kill
    // event with a bumped ring epoch.
    ops.ObserveWorkerDied(/*nodeIndex=*/2,
                          /*killInc=*/5,
                          /*ringEpochAtKill=*/RingEpoch{2, 0});
    {
        auto k = ops.GetKilledWorkerForTest(2);
        BOOST_REQUIRE_EQUAL(k.killIncarnation, 5u);
        BOOST_REQUIRE_EQUAL(k.ringEpochAtKill.epoch, 2u);
    }
    BOOST_CHECK_EQUAL(ops.GetKillResumeCounters().workerDiedMidInsert, 1u);

    // The driver retries the same OpId. The second sendFn MUST NOT be
    // invoked (would risk double-apply); the wrapper replays Success
    // from the resume cache.
    int retrySendCalls = 0;
    auto retrySend = [&](const Distributed::OpId&) {
        ++retrySendCalls;
        return ErrorCode::Fail;  // would-be failure if reached
    };
    // Retry uses an updated ring view (epoch=2, post-kill).
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, RingEpoch{2, 0}, opMid, retrySend)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(retrySendCalls, 0);

    auto c = ops.GetKillResumeCounters();
    BOOST_CHECK_EQUAL(c.workerDiedMidInsert, 1u);
    BOOST_CHECK_EQUAL(c.resumedFromOpId, 1u);
    BOOST_CHECK_EQUAL(c.ringFenceRejected, 0u);

    // A *different* OpId from the same sender (next batch) flows through
    // normally (no replay collision, no spurious fence on the post-kill
    // ring view).
    Distributed::OpId opNext{/*sender=*/0, /*epoch=*/1, /*counter=*/43};
    int freshSends = 0;
    auto freshSend = [&](const Distributed::OpId&) {
        ++freshSends;
        return ErrorCode::Success;
    };
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, RingEpoch{2, 0}, opNext, freshSend)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(freshSends, 1);
}

// ---------------------------------------------------------------------------
// Invariant #4 -- kill-then-stale-sender fenced.
// After the kill is observed (ring-epoch advanced past ringEpochAtKill),
// any RPC from a sender that has NOT yet refreshed its ring view (still
// carrying the pre-kill epoch) MUST be rejected with ErrorCode::Fenced.
// Counter m_ringFenceRejected increments by exactly one per rejected RPC.
// sendFn is never invoked. A sender that has refreshed (epoch >= kill
// epoch) is admitted.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(KillThenZombieFenced)
{
    ScopedEnv on("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT", "1");
    RemotePostingOps ops;
    ops.ResetKillResumeForTest();

    // Worker B (nodeIndex=2) is killed; ring epoch advances to (3,0).
    ops.ObserveWorkerDied(/*nodeIndex=*/2,
                          /*killInc=*/8,
                          /*ringEpochAtKill=*/RingEpoch{3, 0});

    int sendCalls = 0;
    auto sendFn = [&](const Distributed::OpId&) {
        ++sendCalls;
        return ErrorCode::Success;
    };

    // Stale sender carrying pre-kill epoch (1,0) -> Fenced.
    Distributed::OpId opStale{/*sender=*/9, /*epoch=*/1, /*counter=*/1};
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, RingEpoch{1, 0}, opStale, sendFn)
                == ErrorCode::Fenced);
    BOOST_CHECK_EQUAL(sendCalls, 0);

    // Another stale sender (epoch=2, still < kill epoch=3) -> Fenced.
    Distributed::OpId opStale2{/*sender=*/9, /*epoch=*/1, /*counter=*/2};
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, RingEpoch{2, 9}, opStale2, sendFn)
                == ErrorCode::Fenced);
    BOOST_CHECK_EQUAL(sendCalls, 0);

    // Refreshed sender (epoch=3 == kill) is NOT below the kill epoch
    // (CompareRingEpoch is strict-less); admitted.
    Distributed::OpId opFresh{/*sender=*/9, /*epoch=*/1, /*counter=*/3};
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, RingEpoch{3, 0}, opFresh, sendFn)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sendCalls, 1);

    auto c = ops.GetKillResumeCounters();
    BOOST_CHECK_EQUAL(c.workerDiedMidInsert, 1u);
    BOOST_CHECK_EQUAL(c.ringFenceRejected, 2u);
    // resumedFromOpId stays 0: no two senders shared an OpId here.
    BOOST_CHECK_EQUAL(c.resumedFromOpId, 0u);

    // Targeting a *different* node (not the killed one) is unaffected
    // even with the same stale sender epoch.
    Distributed::OpId opOther{/*sender=*/9, /*epoch=*/1, /*counter=*/4};
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/5, RingEpoch{1, 0}, opOther, sendFn)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sendCalls, 2);
    BOOST_CHECK_EQUAL(ops.GetKillResumeCounters().ringFenceRejected, 2u);

    // Monotonicity: a stale ObserveWorkerDied (older killInc) must NOT
    // regress the recorded ring epoch.
    ops.ObserveWorkerDied(/*nodeIndex=*/2,
                          /*killInc=*/4,
                          /*ringEpochAtKill=*/RingEpoch{1, 0});
    auto k = ops.GetKilledWorkerForTest(2);
    BOOST_CHECK_EQUAL(k.killIncarnation, 8u);
    BOOST_CHECK_EQUAL(k.ringEpochAtKill.epoch, 3u);
}

// ---------------------------------------------------------------------------
// Invariant #5 -- harness smoke: the full primitive composition fires
// against the in-memory SWIM mesh + ring-epoch fence. We model:
//   * three workers (W0, W1, W2)
//   * W2 is killed (network partition stand-in for SIGKILL)
//   * peers' SWIM detectors transition W2 to Dead
//   * an observer wired to RemotePostingOps publishes the kill event,
//     bumping the local ring epoch and recording ringEpochAtKill
//   * a stale-ring sender targeting W2 is then fenced
//   * a refreshed sender is admitted
// This ensures the wrapper composes correctly with the actual SwimDetector
// and ConsistentHashRing types -- not just with hand-fed observations.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(HarnessSmoke)
{
    ScopedEnv on("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT", "1");
    BOOST_TEST_MESSAGE("env-armed=" << (FaultEnvArmed() ? "1" : "0"));

    SwimConfig cfg;
    cfg.indirect_fanout = 0;
    cfg.suspect_after_ticks = 1;
    cfg.dead_after_ticks    = 2;
    cfg.ping_targets_per_tick = 4;

    SwimDetector w0(0, std::vector<int>{1, 2}, cfg);
    SwimDetector w1(1, std::vector<int>{0, 2}, cfg);
    auto w2 = std::make_unique<SwimDetector>(2, std::vector<int>{0, 1}, cfg);

    ConsistentHashRing ring;
    ring.AddNode(0); ring.AddNode(1); ring.AddNode(2);
    std::uint32_t ringEpoch = 1;
    ring.SetEpoch(RingEpoch{ringEpoch, 0});

    RemotePostingOps ops;
    ops.ResetKillResumeForTest();

    std::mutex mu;
    auto observer = [&](int nodeId, MemberChange c, const MemberState& st) {
        std::lock_guard<std::mutex> lk(mu);
        switch (c) {
            case MemberChange::BecameDead:
                if (ring.HasNode(nodeId)) {
                    ring.RemoveNode(nodeId);
                    ++ringEpoch;
                    ring.SetEpoch(RingEpoch{ringEpoch, 0});
                    ops.ObserveWorkerDied(nodeId, st.incarnation,
                                          ring.GetEpoch());
                }
                break;
            case MemberChange::BecameAlive:
            case MemberChange::Refuted:
            case MemberChange::Joined:
                if (!ring.HasNode(nodeId)) {
                    ring.AddNode(nodeId);
                    ++ringEpoch;
                    ring.SetEpoch(RingEpoch{ringEpoch, 0});
                }
                break;
            case MemberChange::BecameSuspect:
                break;
        }
    };
    w0.OnMemberChangeCallback(observer);
    w1.OnMemberChangeCallback(observer);

    Mesh mesh;
    mesh.Add(0, w0);
    mesh.Add(1, w1);
    mesh.Add(2, *w2);
    mesh.Pump(2);

    const RingEpoch ringBefore = ring.GetEpoch();
    BOOST_REQUIRE(ring.HasNode(2));
    BOOST_REQUIRE_EQUAL(ops.GetKillResumeCounters().workerDiedMidInsert, 0u);

    // Kill W2: drop all paths to it.
    mesh.Drop(0, 2);
    mesh.Drop(1, 2);
    mesh.Pump(8);

    BOOST_REQUIRE(!ring.HasNode(2));
    BOOST_REQUIRE(ringBefore < ring.GetEpoch());
    BOOST_REQUIRE_GE(ops.GetKillResumeCounters().workerDiedMidInsert, 1u);
    auto kw = ops.GetKilledWorkerForTest(2);
    BOOST_REQUIRE_EQUAL(kw.ringEpochAtKill.epoch, ring.GetEpoch().epoch);

    // A stale sender (still on ringBefore) targeting W2 is fenced by
    // the wrapper -- composition of SwimDetector -> observer ->
    // ObserveWorkerDied -> ring-epoch fence.
    int sendCalls = 0;
    auto sendFn = [&](const Distributed::OpId&) {
        ++sendCalls;
        return ErrorCode::Success;
    };
    Distributed::OpId opStale{/*sender=*/3, /*epoch=*/1, /*counter=*/1};
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/2, ringBefore, opStale, sendFn)
                == ErrorCode::Fenced);
    BOOST_CHECK_EQUAL(sendCalls, 0);

    // A refreshed sender targeting a *live* node (not W2) goes through.
    Distributed::OpId opLive{/*sender=*/3, /*epoch=*/1, /*counter=*/2};
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/0, ring.GetEpoch(), opLive, sendFn)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sendCalls, 1);

    // Resume path: a previously-cached Success on (sender=3,ctr=2)
    // replays without a second sendFn invocation.
    BOOST_CHECK(ops.SendRemoteAppendWithKillResumeFence(
                    /*targetNodeIndex=*/0, ring.GetEpoch(), opLive, sendFn)
                == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sendCalls, 1);
    BOOST_CHECK_GE(ops.GetKillResumeCounters().resumedFromOpId, 1u);
    BOOST_CHECK_GE(ops.GetKillResumeCounters().ringFenceRejected, 1u);
}

BOOST_AUTO_TEST_SUITE_END()
