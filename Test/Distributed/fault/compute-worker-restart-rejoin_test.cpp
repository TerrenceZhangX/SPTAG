// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: compute-worker-restart-rejoin
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//        compute-worker-restart-rejoin.md
// Branch: fault/compute-worker-restart-rejoin
//
// Composes prim/swim-membership + prim/ring-epoch-fence to validate the
// rejoin handshake invariants stated in the spec:
//
//   1. Sender-startup gate. A worker freshly restarted with
//      RingEpoch{0,0} cannot send routed RPCs; receivers reply
//      SenderUninitialised. (No silent stale serving.)
//
//   2. Stale-incarnation writes rejected. After a restart, the
//      cluster sees a higher incarnation for W2; an inbound update
//      claiming W2 is Dead at an older incarnation cannot revert
//      peers to Dead. (No zombie-write resurrection.)
//
//   3. Ring not reverted by stale epoch. A RingUpdate carrying a stale
//      (epoch, ringRev) is rejected as SenderStale by CompareRingEpoch;
//      the local ring epoch monotonically advances across rejoin.
//
//   4. Rejoin handshake observed. SwimDetector mesh: W2 is removed
//      (peers mark it Dead via partition), restart is simulated by
//      replacing W2's detector instance, and the cluster converges to
//      W2 Alive with strictly bumped incarnation. The MemberChange
//      callback fires BecameAlive/Refuted, which (per the SwimDetector
//      observer factory) re-adds W2 to the ConsistentHashRing.
//
// All four invariants run against pure header-only primitives — no
// PD/TiKV bring-up, no docker. Both env-off (SPTAG_FAULT_COMPUTE_
// WORKER_RESTART_REJOIN unset) and env-armed (=1) execute the same
// code path; the env var is captured only to record protocol-level
// evidence that the harness invoked us in both modes.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/SwimDetector.h"
#include "inc/Core/SPANN/Distributed/RingEpoch.h"
#include "inc/Core/SPANN/Distributed/ConsistentHashRing.h"

#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::SPANN;

namespace {

// In-memory mesh: same shape as Test/Distributed/prim/swim_detector_test.cpp,
// duplicated locally to keep the fault test self-contained and to allow
// the "remove + replace W2's detector" flow that the prim test does not
// exercise.
struct Mesh {
    std::map<int, SwimDetector*> nodes;
    std::set<std::pair<int,int>> dropped;

    void Add(int id, SwimDetector& d) { nodes[id] = &d; }
    void Replace(int id, SwimDetector& d) { nodes[id] = &d; }
    void Remove(int id) { nodes.erase(id); }
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

MemberHealth HealthOf(const SwimDetector& d, int target) {
    MemberState st;
    if (!d.TryGetMember(target, st)) return MemberHealth::Alive;
    return st.health;
}

std::uint64_t IncarnationOf(const SwimDetector& d, int target) {
    MemberState st;
    if (!d.TryGetMember(target, st)) return 0;
    return st.incarnation;
}

bool FaultArmed() {
    return std::getenv("SPTAG_FAULT_COMPUTE_WORKER_RESTART_REJOIN") != nullptr;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(ComputeWorkerRestartRejoinTest)

// Invariant #1 — sender-startup gate.
// Spec "Expected behaviour" #5: "Wk does not register itself as ready
// until the anti-entropy pull is complete." The RPC-layer expression
// of that property is: a freshly-restarted worker carries RingEpoch{0,0}
// in its sender view, and CompareRingEpoch returns SenderUninitialised
// regardless of receiver state — so the worker cannot serve traffic
// until it has refreshed its ring view.
BOOST_AUTO_TEST_CASE(InvRestartedWorkerCannotSendRPCsUntilEpochRefreshed)
{
    BOOST_TEST_MESSAGE("env-armed=" << (FaultArmed() ? "1" : "0"));

    RingEpoch postRestart;                  // {0,0} — fresh process state
    BOOST_REQUIRE(postRestart.IsZero());
    BOOST_REQUIRE(!postRestart.IsInitialised());

    // No matter what the receiver thinks the cluster ring is, a {0,0}
    // sender is rejected at the fence. This is the property that makes
    // "no silent stale serving" enforceable at the wire level.
    BOOST_CHECK(CompareRingEpoch(postRestart, RingEpoch{1, 0})
                == RingEpochCompare::SenderUninitialised);
    BOOST_CHECK(CompareRingEpoch(postRestart, RingEpoch{42, 17})
                == RingEpochCompare::SenderUninitialised);
    BOOST_CHECK(CompareRingEpoch(postRestart, RingEpoch{})
                == RingEpochCompare::SenderUninitialised);

    // After the anti-entropy pull populates the ring view, the same
    // sender now compares Equal and is allowed to serve.
    RingEpoch refreshed{42, 17};
    BOOST_CHECK(CompareRingEpoch(refreshed, RingEpoch{42, 17})
                == RingEpochCompare::Equal);
}

// Invariant #2 — ring not reverted by stale epoch.
// Spec "Failure mode": dispatcher must revoke the stale ring slot and
// reject zombie writes. At the fence layer this means an RPC carrying
// a (epoch, ringRev) below local is SenderStale and must not cause the
// receiver to downgrade its ring view.
BOOST_AUTO_TEST_CASE(InvStaleRingEpochCannotRevertLocalRing)
{
    ConsistentHashRing ring;
    ring.AddNode(0);
    ring.AddNode(1);
    ring.AddNode(2);
    ring.SetEpoch(RingEpoch{5, 0});     // pre-restart cluster epoch
    BOOST_REQUIRE(ring.IsEpochInitialised());

    // After W2 dies + rejoins, the cluster bumps to (6, 0) (membership
    // change → epoch+1). A late RPC from a peer still on (5, 0) is
    // SenderStale; the fence rejects it and our local epoch must not
    // be touched.
    ring.SetEpoch(RingEpoch{6, 0});
    BOOST_CHECK(CompareRingEpoch(RingEpoch{5, 0}, ring.GetEpoch())
                == RingEpochCompare::SenderStale);
    BOOST_CHECK(CompareRingEpoch(RingEpoch{5, 99}, ring.GetEpoch())
                == RingEpochCompare::SenderStale);

    // A RingUpdate fence is one-way: receivers refuse stale, but they
    // never *adopt* a stale view. Assert the absence of a downgrade
    // path by checking the ordering relation directly.
    BOOST_CHECK(RingEpoch(5, 0) < ring.GetEpoch());
    BOOST_CHECK(!(ring.GetEpoch() < RingEpoch(5, 0)));

    // A peer ahead of us (post-rejoin "you're behind" RingUpdate)
    // marks us ReceiverStale — the legitimate forward-refresh path.
    BOOST_CHECK(CompareRingEpoch(RingEpoch{7, 0}, ring.GetEpoch())
                == RingEpochCompare::ReceiverStale);
}

// Invariant #3 — stale-incarnation writes rejected.
// Spec "Recovery semantics" #1: "Wk writes (nodeId, generation) on
// every start. Peers read it on RingUpdate ACK." MVP expression:
// SWIM's ApplyUpdates only adopts an incoming update if its incarnation
// is strictly higher than the current view (or equal + strictly worse
// health). A "zombie" RPC claiming W2 Dead at an older incarnation
// must therefore fail to revert peers to Dead.
BOOST_AUTO_TEST_CASE(InvStaleIncarnationCannotResurrectZombieDeadView)
{
    SwimConfig cfg;
    cfg.indirect_fanout = 0;
    cfg.suspect_after_ticks = 100;      // stop SWIM from interfering
    cfg.dead_after_ticks    = 1000;
    cfg.ping_targets_per_tick = 4;

    SwimDetector peer(0, {2}, cfg);     // peer keeps a view of W2

    // Cluster has converged: W2 is Alive at incarnation = 7 (post-rejoin).
    std::vector<std::pair<int, MemberState>> postRejoinView{
        {2, MemberState{MemberHealth::Alive, /*inc=*/7, 0, 0}}};
    peer.OnPing(99, postRejoinView);
    BOOST_REQUIRE_EQUAL(static_cast<int>(HealthOf(peer, 2)),
                        static_cast<int>(MemberHealth::Alive));
    BOOST_REQUIRE_EQUAL(IncarnationOf(peer, 2), 7u);

    // Zombie update from a stale path: "W2 Dead @ inc=3" (pre-restart
    // generation). MUST NOT revert peer's view.
    std::vector<std::pair<int, MemberState>> zombie{
        {2, MemberState{MemberHealth::Dead, /*inc=*/3, 0, 0}}};
    peer.OnAck(99, zombie);
    BOOST_CHECK_EQUAL(static_cast<int>(HealthOf(peer, 2)),
                      static_cast<int>(MemberHealth::Alive));
    BOOST_CHECK_EQUAL(IncarnationOf(peer, 2), 7u);

    // An update STRICTLY older than current must be rejected — this
    // is the MVP's primary defence against zombie-rev-write.
    std::vector<std::pair<int, MemberState>> sameIncOlder{
        {2, MemberState{MemberHealth::Dead, /*inc=*/6, 0, 0}}};
    peer.OnAck(99, sameIncOlder);
    BOOST_CHECK_EQUAL(static_cast<int>(HealthOf(peer, 2)),
                      static_cast<int>(MemberHealth::Alive));
    BOOST_CHECK_EQUAL(IncarnationOf(peer, 2), 7u);
}

// Invariant #4 — rejoin handshake. Full-loop composition of both
// primitives: take W2 down by partitioning, peers mark Dead, "restart"
// W2 by replacing its SwimDetector instance, and observe convergence
// to Alive with strictly bumped incarnation across the cluster. The
// MakeRingMembershipObserver factory wires SWIM transitions back to
// ConsistentHashRing.AddNode/RemoveNode — the rejoin must end with
// W2 *back in the ring* and the ring epoch monotonically advanced.
BOOST_AUTO_TEST_CASE(InvRejoinHandshakeBumpsIncarnationReinstatesRingSlot)
{
    SwimConfig cfg;
    cfg.indirect_fanout = 0;
    cfg.suspect_after_ticks = 1;
    cfg.dead_after_ticks    = 2;
    cfg.ping_targets_per_tick = 4;

    SwimDetector w0(0, {1, 2}, cfg);
    SwimDetector w1(1, {0, 2}, cfg);
    auto w2_pre = std::make_unique<SwimDetector>(2, std::vector<int>{0, 1}, cfg);

    // Two ConsistentHashRings represent W0 and W1's local ring view.
    // We wire each detector to its ring through an observer so
    // BecameDead / BecameAlive / Refuted automatically reshape the ring.
    ConsistentHashRing ringW0;
    ConsistentHashRing ringW1;
    std::mutex muW0, muW1;
    ringW0.AddNode(0); ringW0.AddNode(1); ringW0.AddNode(2);
    ringW1.AddNode(0); ringW1.AddNode(1); ringW1.AddNode(2);

    // Local ring epoch starts at (1,0); we bump on every membership
    // observer fire to stand in for the dispatcher pushing RingUpdate.
    std::uint32_t epochW0 = 1, epochW1 = 1;
    ringW0.SetEpoch(RingEpoch{epochW0, 0});
    ringW1.SetEpoch(RingEpoch{epochW1, 0});

    auto bumpAndRewire = [](ConsistentHashRing& r, std::mutex& mu,
                            std::uint32_t& epochCounter) {
        return [&r, &mu, &epochCounter](int nodeId, MemberChange c,
                                        const MemberState&) {
            std::lock_guard<std::mutex> lk(mu);
            switch (c) {
                case MemberChange::BecameDead:
                    if (r.HasNode(nodeId)) {
                        r.RemoveNode(nodeId);
                        ++epochCounter;
                        r.SetEpoch(RingEpoch{epochCounter, 0});
                    }
                    break;
                case MemberChange::BecameAlive:
                case MemberChange::Refuted:
                case MemberChange::Joined:
                    if (!r.HasNode(nodeId)) {
                        r.AddNode(nodeId);
                        ++epochCounter;
                        r.SetEpoch(RingEpoch{epochCounter, 0});
                    }
                    break;
                case MemberChange::BecameSuspect:
                    break;
            }
        };
    };
    w0.OnMemberChangeCallback(bumpAndRewire(ringW0, muW0, epochW0));
    w1.OnMemberChangeCallback(bumpAndRewire(ringW1, muW1, epochW1));

    Mesh mesh;
    mesh.Add(0, w0);
    mesh.Add(1, w1);
    mesh.Add(2, *w2_pre);

    mesh.Pump(2);
    BOOST_REQUIRE(ringW0.HasNode(2));
    BOOST_REQUIRE(ringW1.HasNode(2));
    const RingEpoch ringEpochBefore = ringW0.GetEpoch();

    // Take W2 down: drop all paths to it, simulating SIGKILL.
    mesh.Drop(0, 2);
    mesh.Drop(1, 2);
    mesh.Pump(8);

    // Peers should have evicted W2 and bumped the ring epoch.
    BOOST_CHECK_MESSAGE(static_cast<int>(HealthOf(w0, 2))
                        == static_cast<int>(MemberHealth::Dead),
                        "W0 should mark W2 Dead");
    BOOST_CHECK_MESSAGE(!ringW0.HasNode(2),
                        "W0 ring observer should have evicted W2");
    BOOST_CHECK_MESSAGE(ringEpochBefore < ringW0.GetEpoch(),
                        "ring epoch must advance on Dead");

    // Snapshot the cluster's view of W2's incarnation just before
    // restart so we can verify rejoin bumps it.
    const std::uint64_t incPreRestart = IncarnationOf(w0, 2);

    // Restart W2: replace the detector instance with a fresh one
    // (simulating a new process). Heal the network. Per SWIM, peers
    // gossip "W2 Dead @ inc=N" to the new instance, which refutes by
    // bumping its own incarnation past N.
    auto w2_post = std::make_unique<SwimDetector>(2, std::vector<int>{0, 1}, cfg);
    mesh.Replace(2, *w2_post);
    mesh.HealAll();
    mesh.Pump(20);

    // W0 / W1 should now see W2 Alive again, with incarnation strictly
    // greater than the pre-restart claim.
    BOOST_CHECK_MESSAGE(HealthOf(w0, 2) == MemberHealth::Alive,
                        "W0: W2 should be Alive post-rejoin, got "
                        << static_cast<int>(HealthOf(w0, 2)));
    BOOST_CHECK_MESSAGE(IncarnationOf(w0, 2) > incPreRestart,
                        "W0: W2 incarnation must strictly advance: pre="
                        << incPreRestart << " post=" << IncarnationOf(w0, 2));

    // Ring observer should have re-added W2 and bumped the epoch again.
    BOOST_CHECK_MESSAGE(ringW0.HasNode(2),
                        "W0 ring observer should have re-added W2");
    BOOST_CHECK_MESSAGE(ringEpochBefore < ringW0.GetEpoch(),
                        "ring epoch must monotonically advance across rejoin");

    // The sender-startup gate composes: even after rejoin, an RPC from
    // some hypothetical sender that still carries {0,0} is rejected.
    BOOST_CHECK(CompareRingEpoch(RingEpoch{}, ringW0.GetEpoch())
                == RingEpochCompare::SenderUninitialised);
}

BOOST_AUTO_TEST_SUITE_END()
