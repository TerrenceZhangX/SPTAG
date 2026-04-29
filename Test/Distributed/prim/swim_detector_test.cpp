// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Unit tests for SwimDetector primitive (Test/Distributed/prim).
//
// Strategy: in-memory mesh harness. Each test creates N detectors,
// hand-delivers messages between them per tick, and asserts on
// MemberView() transitions. No threading, no IO.

#include "inc/Core/SPANN/Distributed/SwimDetector.h"
#include "inc/Test.h"

#include <map>
#include <set>
#include <string>
#include <vector>

using namespace SPTAG::SPANN;

namespace {

// In-memory mesh wiring detector instances together. The harness
// owns no nodes; callers register detectors and decide which (from,to)
// edges are "broken" so we can simulate partial path failures.
struct Mesh {
    std::map<int, SwimDetector*> nodes;
    // (from,to) pairs for which we drop messages — direct path failure.
    std::set<std::pair<int,int>> dropped;

    void Add(int id, SwimDetector& d) { nodes[id] = &d; }
    void Drop(int from, int to) {
        dropped.insert({from, to});
        dropped.insert({to, from});
    }

    // Deliver one message envelope. Returns reply messages (if any).
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

    // Pump: run `ticks` ticks across all nodes, fully draining the
    // mailbox each tick (BFS until quiescent or a depth cap hits).
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
    if (!d.TryGetMember(target, st)) return MemberHealth::Alive; // unknown
    return st.health;
}

} // namespace

BOOST_AUTO_TEST_SUITE(SwimDetectorTest)

// 1) alive -> suspect -> dead transition on missed pings.
BOOST_AUTO_TEST_CASE(DirectFailureTransitions)
{
    SwimConfig cfg;
    cfg.indirect_fanout = 0;            // disable indirect path
    cfg.suspect_after_ticks = 1;
    cfg.dead_after_ticks = 3;
    cfg.ping_targets_per_tick = 4;      // ping everything every tick

    SwimDetector a(0, {1}, cfg);
    SwimDetector b(1, {0}, cfg);

    Mesh mesh;
    mesh.Add(0, a);
    mesh.Add(1, b);

    // First, baseline: a few healthy ticks. Should remain Alive.
    mesh.Pump(2);
    BOOST_CHECK(HealthOf(a, 1) == MemberHealth::Alive);

    // Sever the direct path.
    mesh.Drop(0, 1);

    // After 1 missed direct ping cycle, b should be Suspect on a.
    mesh.Pump(2);
    BOOST_CHECK_MESSAGE(HealthOf(a, 1) == MemberHealth::Suspect ||
                        HealthOf(a, 1) == MemberHealth::Dead,
                        "expected Suspect/Dead, got "
                        << static_cast<int>(HealthOf(a, 1)));

    // Continue ticking past dead_after_ticks; should be Dead.
    mesh.Pump(5);
    BOOST_CHECK_EQUAL(static_cast<int>(HealthOf(a, 1)),
                      static_cast<int>(MemberHealth::Dead));
}

// 2) Indirect-ping recovers from a flaky direct path: A↔B is dropped,
//    but A↔C and B↔C are healthy, so PingReq via C should keep B alive.
BOOST_AUTO_TEST_CASE(IndirectPingRecoversFlakyDirectPath)
{
    SwimConfig cfg;
    cfg.indirect_fanout = 2;
    cfg.suspect_after_ticks = 1;
    cfg.dead_after_ticks = 5;
    cfg.ping_targets_per_tick = 4;

    SwimDetector a(0, {1, 2}, cfg);
    SwimDetector b(1, {0, 2}, cfg);
    SwimDetector c(2, {0, 1}, cfg);

    Mesh mesh;
    mesh.Add(0, a);
    mesh.Add(1, b);
    mesh.Add(2, c);

    mesh.Pump(2);
    BOOST_REQUIRE(HealthOf(a, 1) == MemberHealth::Alive);

    // Break only the direct A<->B link. C is healthy with both.
    mesh.Drop(0, 1);

    // Run several rounds; A should mark B Suspect once direct ping
    // misses, but indirect ping via C must prevent B from going Dead.
    mesh.Pump(10);

    // B must never have flipped to Dead in A's view.
    MemberState seen;
    BOOST_REQUIRE(a.TryGetMember(1, seen));
    BOOST_CHECK_MESSAGE(seen.health != MemberHealth::Dead,
                        "indirect ping via C should keep B out of Dead, "
                        "got health=" << static_cast<int>(seen.health));
}

// 3) Gossip propagates state across a 3-node ring within bounded ticks.
//    A learns about C (and vice versa) only via B's gossip piggyback.
BOOST_AUTO_TEST_CASE(GossipPropagatesAcrossRing)
{
    SwimConfig cfg;
    cfg.indirect_fanout = 0;
    cfg.suspect_after_ticks = 100;     // don't let anyone go Suspect
    cfg.dead_after_ticks = 1000;
    cfg.ping_targets_per_tick = 4;

    // A only knows B, C only knows B, B knows both. Bridge topology.
    SwimDetector a(0, {1},    cfg);
    SwimDetector b(1, {0, 2}, cfg);
    SwimDetector c(2, {1},    cfg);

    Mesh mesh;
    mesh.Add(0, a);
    mesh.Add(1, b);
    mesh.Add(2, c);

    // Pump a small bounded number of ticks; A and C should learn of
    // each other purely through gossip embedded in pings/acks via B.
    mesh.Pump(4);

    MemberState st;
    BOOST_CHECK_MESSAGE(a.TryGetMember(2, st),
                        "A should have learned about C via gossip");
    BOOST_CHECK_MESSAGE(c.TryGetMember(0, st),
                        "C should have learned about A via gossip");
}

// 4) Dead-then-rejoin via 3-node gossip + incarnation refute.
//    Topology: A, B, C all peers. A<->B is partitioned; C remains
//    healthy with both. After A marks B Dead, A gossips that to C,
//    C echoes "B is Dead@inc=0" to B (since gossip is unconditional
//    in this MVP). B's ApplyUpdates self-branch refutes: bumps its
//    own incarnation past A's claim and re-broadcasts inc=1 Alive.
//    C learns B Alive@inc=1, gossips to A, A's view flips to Alive.
//
//    Note: a pure 2-node Dead<->Dead deadlock cannot self-recover
//    in this MVP — there's no third party to carry the refutation.
//    Lifeguard / explicit rejoin handshake is deferred to Wave-2.
BOOST_AUTO_TEST_CASE(DeadThenRejoinViaIncarnationRefute)
{
    SwimConfig cfg;
    cfg.indirect_fanout = 0;            // direct path only, simpler
    cfg.suspect_after_ticks = 1;
    cfg.dead_after_ticks = 2;
    cfg.ping_targets_per_tick = 4;

    SwimDetector a(0, {1, 2}, cfg);
    SwimDetector b(1, {0, 2}, cfg);
    SwimDetector c(2, {0, 1}, cfg);

    Mesh mesh;
    mesh.Add(0, a);
    mesh.Add(1, b);
    mesh.Add(2, c);

    mesh.Pump(2);

    // Bidirectionally partition A and B; C stays connected to both.
    mesh.Drop(0, 1);
    mesh.Pump(6);
    BOOST_REQUIRE_EQUAL(static_cast<int>(HealthOf(a, 1)),
                        static_cast<int>(MemberHealth::Dead));
    BOOST_REQUIRE_EQUAL(static_cast<int>(HealthOf(b, 0)),
                        static_cast<int>(MemberHealth::Dead));

    // C started with direct contact to both. Note: in this MVP
    // gossip is unconditional ("worse health wins at same incarnation"),
    // so once A starts gossiping "B Dead@inc=0", C may also flip B
    // to Suspect/Dead even though its direct probes still succeed.
    // That's a known MVP weakness; Lifeguard would weight gossip
    // less than direct observation. We don't assert here.

    // Heal. Gossip carries A's "B Dead" claim to C, then to B; B
    // refutes by bumping its incarnation and that propagates back.
    mesh.dropped.clear();
    mesh.Pump(10);

    MemberState st;
    BOOST_REQUIRE(a.TryGetMember(1, st));
    BOOST_CHECK_MESSAGE(st.health == MemberHealth::Alive,
                        "after heal, B should be Alive in A; got "
                        << static_cast<int>(st.health));
    BOOST_CHECK_MESSAGE(st.incarnation > 0,
                        "B should have bumped its incarnation; got "
                        << st.incarnation);
}

BOOST_AUTO_TEST_SUITE_END()
