// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: headsync-ordering-violation
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//        headsync-ordering-violation.md
// Branch: fault/headsync-ordering-violation
//
// Scenario: two head adds applied out-of-order on a follower
// (entry N+1 before N). Required invariants (Tier 1 HARD):
//
//   1. env-off dormancy: the gate is silent on the production hot
//      path - cursor is never consulted, the pull callback is never
//      invoked, counters do not move, and the caller still sees the
//      raw "apply whatever arrives" behaviour (so the production
//      build is byte-equal to baseline; no silent recall regression
//      on baseline runs).
//   2. baseline in-order apply: env-armed; entries arrive in order
//      (sequence 1, 2, 3, ...). All apply immediately; no out-of-
//      order detected; no pull triggered.
//   3. out-of-order refused + scan_range pull fills: env-armed;
//      sequence 2 arrives before sequence 1. The gate refuses to
//      apply 2, triggers a scan_range pull which returns the
//      missing entry 1, then drains 1 then 2 in order. Cursor
//      ends at 2.
//   4. concurrent out-of-order converges: env-armed; multiple
//      origins interleave out-of-order arrivals. At quiesce, every
//      origin's cursor reaches the highest sent sequence and the
//      apply log is per-origin contiguous (no gaps, no duplicates,
//      no reorder).
//   5. harness smoke: env-armed; covers epoch advance with
//      sequence != 1 (must be treated as gap), pull-failure path
//      (entry stays parked, counters reflect refusal but no
//      drain), and the late-arriving missing entry resumes the
//      drain idempotently.
//
// 1M-scale perf gate (Tier 2 SOFT, +/-20%) is DEFERRED; this case
// is non hot-path (the gate only fires when an out-of-order
// arrival is detected, which is a rare anti-entropy event, not on
// the query / insert hot path).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/HeadsyncOrderingGate.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
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

// Per-origin authoritative log: the ordered sequence of payloads
// the owner has emitted. Tests build this log up front; the gate
// drives `apply` against the follower's `appliedLog`.
struct OriginLog {
    // [(epoch, seq)] -> payload.
    std::vector<HeadsyncPullEntry<std::int64_t>> entries;

    void Push(std::uint64_t epoch, std::uint64_t seq, std::int64_t v) {
        entries.push_back({epoch, seq, v});
    }
    bool RangeSince(std::uint64_t fromEpoch, std::uint64_t fromSeq,
                    std::vector<HeadsyncPullEntry<std::int64_t>>& out) const {
        for (const auto& e : entries) {
            bool inRange = (e.epoch  > fromEpoch) ||
                           (e.epoch == fromEpoch && e.sequence >= fromSeq);
            if (inRange) out.push_back(e);
        }
        return true;
    }
};

// Follower harness: the apply log is the per-origin sequence of
// applied payloads in apply order. We assert the apply order is
// per-origin contiguous (entry N+1 follows N).
struct OrderingHarness {
    std::unordered_map<std::int32_t, OriginLog> originLogs; // owner-side
    // Apply log per origin captured by the apply callback.
    std::unordered_map<std::int32_t,
                       std::vector<HeadsyncPullEntry<std::int64_t>>> appliedLog;

    // Counters for callback invocations (test-side; distinct from
    // gate counters which we also assert).
    int applyCalls = 0;
    int pullCalls  = 0;
    bool pullFailNext = false;

    // Convenience: the gate is templated on the payload type.
    HeadsyncOrderingGate<std::int64_t> gate;

    HeadsyncOrderingStatus Send(std::int32_t origin,
                                std::uint64_t epoch,
                                std::uint64_t seq,
                                std::int64_t  payload)
    {
        return gate.TryApply<>(
            origin, epoch, seq, payload,
            // apply
            [this](std::int32_t  o,
                   std::uint64_t e,
                   std::uint64_t s,
                   std::int64_t  v) {
                ++applyCalls;
                appliedLog[o].push_back({e, s, v});
            },
            // pullSince
            [this](std::int32_t  o,
                   std::uint64_t fromEpoch,
                   std::uint64_t fromSeq,
                   std::vector<HeadsyncPullEntry<std::int64_t>>& out) -> bool {
                ++pullCalls;
                if (pullFailNext) { pullFailNext = false; return false; }
                auto it = originLogs.find(o);
                if (it == originLogs.end()) return true;
                return it->second.RangeSince(fromEpoch, fromSeq, out);
            });
    }

    bool AppliedIsContiguous(std::int32_t origin) const {
        auto it = appliedLog.find(origin);
        if (it == appliedLog.end()) return true;
        const auto& log = it->second;
        for (std::size_t i = 1; i < log.size(); ++i) {
            const auto& prev = log[i - 1];
            const auto& cur  = log[i];
            if (cur.epoch == prev.epoch) {
                if (cur.sequence != prev.sequence + 1) return false;
            } else {
                if (cur.epoch != prev.epoch + 1) return false;
                if (cur.sequence != 1)           return false;
            }
        }
        return true;
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(HeadsyncOrderingViolationTest)

// -----------------------------------------------------------------------
// Case 1: env-off dormancy.
// -----------------------------------------------------------------------
//
// Even with the env unset, TryApply must return Dormant and never
// touch cursor / pending / pull / counters. The caller (production
// code) is expected to interpret Dormant as "fall through to the
// pre-fix raw apply" - so this test asserts the gate is a no-op,
// not that the pre-fix apply happens (that lives outside the gate).
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION", nullptr);

    OrderingHarness h;
    // Simulate an out-of-order arrival; the gate should still be
    // silent.
    auto r1 = h.Send(/*origin*/ 1, /*epoch*/ 0, /*seq*/ 5, /*payload*/ 500);
    auto r2 = h.Send(/*origin*/ 1, /*epoch*/ 0, /*seq*/ 1, /*payload*/ 100);

    BOOST_CHECK(r1 == HeadsyncOrderingStatus::Dormant);
    BOOST_CHECK(r2 == HeadsyncOrderingStatus::Dormant);
    BOOST_CHECK_EQUAL(h.applyCalls, 0);
    BOOST_CHECK_EQUAL(h.pullCalls,  0);

    auto c = h.gate.GetCounters();
    BOOST_CHECK_EQUAL(c.outOfOrderApplyDetected, 0u);
    BOOST_CHECK_EQUAL(c.cursorGapRefused,        0u);
    BOOST_CHECK_EQUAL(c.scanRangeFillTriggered,  0u);
    BOOST_CHECK_EQUAL(c.inOrderResumed,          0u);

    BOOST_CHECK_EQUAL(h.gate.PendingSize(1), 0u);
    auto cur = h.gate.GetCursor(1);
    BOOST_CHECK_EQUAL(cur.epoch,    0u);
    BOOST_CHECK_EQUAL(cur.sequence, 0u);
}

// -----------------------------------------------------------------------
// Case 2: baseline in-order apply.
// -----------------------------------------------------------------------
//
// env-armed; entries arrive in (epoch=0, seq=1, 2, 3, ...). All
// apply immediately, no out-of-order detected, no pull triggered.
BOOST_AUTO_TEST_CASE(BaselineInOrderApply)
{
    ScopedEnv env("SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION", "1");

    OrderingHarness h;
    for (std::uint64_t seq = 1; seq <= 10; ++seq) {
        auto r = h.Send(7, 0, seq, static_cast<std::int64_t>(seq * 10));
        BOOST_REQUIRE(r == HeadsyncOrderingStatus::AppliedInOrder);
    }

    BOOST_CHECK_EQUAL(h.applyCalls, 10);
    BOOST_CHECK_EQUAL(h.pullCalls,  0);

    auto c = h.gate.GetCounters();
    BOOST_CHECK_EQUAL(c.outOfOrderApplyDetected, 0u);
    BOOST_CHECK_EQUAL(c.cursorGapRefused,        0u);
    BOOST_CHECK_EQUAL(c.scanRangeFillTriggered,  0u);
    BOOST_CHECK_EQUAL(c.inOrderResumed,          0u);

    auto cur = h.gate.GetCursor(7);
    BOOST_CHECK_EQUAL(cur.epoch,    0u);
    BOOST_CHECK_EQUAL(cur.sequence, 10u);
    BOOST_CHECK_EQUAL(h.gate.PendingSize(7), 0u);
    BOOST_CHECK(h.AppliedIsContiguous(7));
}

// -----------------------------------------------------------------------
// Case 3: out-of-order refused + scan_range pull fills.
// -----------------------------------------------------------------------
//
// env-armed; sequence 2 arrives before sequence 1. The gate refuses
// to apply 2 out of order, triggers a scan_range pull which returns
// the missing entry 1, drains 1 -> 2 in order. Cursor ends at 2,
// applied log is [1, 2] (contiguous, in order).
BOOST_AUTO_TEST_CASE(OutOfOrderRefusedAndPullFills)
{
    ScopedEnv env("SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION", "1");

    OrderingHarness h;
    // Owner has emitted 1 then 2.
    h.originLogs[3].Push(0, 1, 11);
    h.originLogs[3].Push(0, 2, 22);

    // Out-of-order arrival: 2 first.
    auto r2 = h.Send(3, 0, 2, 22);
    BOOST_CHECK(r2 == HeadsyncOrderingStatus::FilledAndResumed);

    auto c = h.gate.GetCounters();
    BOOST_CHECK_EQUAL(c.outOfOrderApplyDetected, 1u);
    BOOST_CHECK_EQUAL(c.cursorGapRefused,        1u);
    BOOST_CHECK_EQUAL(c.scanRangeFillTriggered,  1u);
    BOOST_CHECK_EQUAL(c.inOrderResumed,          1u);

    BOOST_CHECK_EQUAL(h.pullCalls, 1);
    // applyCalls == 2: entry 1 (filled) then entry 2 (was parked).
    BOOST_CHECK_EQUAL(h.applyCalls, 2);

    auto cur = h.gate.GetCursor(3);
    BOOST_CHECK_EQUAL(cur.epoch,    0u);
    BOOST_CHECK_EQUAL(cur.sequence, 2u);
    BOOST_CHECK_EQUAL(h.gate.PendingSize(3), 0u);
    BOOST_CHECK(h.AppliedIsContiguous(3));
    BOOST_REQUIRE_EQUAL(h.appliedLog[3].size(), 2u);
    BOOST_CHECK_EQUAL(h.appliedLog[3][0].sequence, 1u);
    BOOST_CHECK_EQUAL(h.appliedLog[3][1].sequence, 2u);

    // Late-arriving 1: must be detected as duplicate (already seen),
    // counters unchanged, apply log unchanged.
    auto r1 = h.Send(3, 0, 1, 11);
    BOOST_CHECK(r1 == HeadsyncOrderingStatus::AlreadySeen);
    auto c2 = h.gate.GetCounters();
    BOOST_CHECK_EQUAL(c2.outOfOrderApplyDetected, c.outOfOrderApplyDetected);
    BOOST_CHECK_EQUAL(c2.cursorGapRefused,        c.cursorGapRefused);
    BOOST_CHECK_EQUAL(h.applyCalls,               2);
}

// -----------------------------------------------------------------------
// Case 4: concurrent out-of-order across origins converges.
// -----------------------------------------------------------------------
//
// env-armed; two origins each with 6 entries; we shuffle the
// arrivals so several gaps fire, then we send the original
// in-order entries that fill the gaps. At quiesce, every origin's
// cursor reaches 6 and the per-origin apply log is contiguous.
BOOST_AUTO_TEST_CASE(ConcurrentOutOfOrderConverges)
{
    ScopedEnv env("SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION", "1");

    OrderingHarness h;
    constexpr int N = 6;
    for (std::int32_t origin : {10, 20}) {
        for (std::uint64_t s = 1; s <= N; ++s) {
            h.originLogs[origin].Push(0, s,
                static_cast<std::int64_t>(origin * 100 + s));
        }
    }

    // Out-of-order arrivals: skip seq=3 on origin 10 then send 4
    // (gap), and skip seq=2 on origin 20 then send 5 (bigger gap).
    // Each gap triggers a scan_range pull that fills *all* missing
    // entries for that origin (since RangeSince returns from the
    // gap origin onwards). After two such bursts both origins are
    // fully converged.
    BOOST_CHECK(h.Send(10, 0, 1, 101) == HeadsyncOrderingStatus::AppliedInOrder);
    BOOST_CHECK(h.Send(10, 0, 2, 102) == HeadsyncOrderingStatus::AppliedInOrder);
    // Skip 3, send 4: triggers fill (which returns 3,4,5,6 from the log).
    auto r10 = h.Send(10, 0, 4, 104);
    BOOST_CHECK(r10 == HeadsyncOrderingStatus::FilledAndResumed);

    // Origin 20: send 1 then jump to 5.
    BOOST_CHECK(h.Send(20, 0, 1, 2001) == HeadsyncOrderingStatus::AppliedInOrder);
    auto r20 = h.Send(20, 0, 5, 2005);
    BOOST_CHECK(r20 == HeadsyncOrderingStatus::FilledAndResumed);

    // Both origins should be at sequence == 6 (the fill returned all
    // remaining entries up to the end of each origin's log).
    BOOST_CHECK_EQUAL(h.gate.GetCursor(10).sequence, 6u);
    BOOST_CHECK_EQUAL(h.gate.GetCursor(20).sequence, 6u);

    BOOST_CHECK(h.AppliedIsContiguous(10));
    BOOST_CHECK(h.AppliedIsContiguous(20));

    BOOST_CHECK_EQUAL(h.appliedLog[10].size(), 6u);
    BOOST_CHECK_EQUAL(h.appliedLog[20].size(), 6u);

    auto c = h.gate.GetCounters();
    // Two bursts each with one out-of-order arrival.
    BOOST_CHECK_EQUAL(c.outOfOrderApplyDetected, 2u);
    BOOST_CHECK_EQUAL(c.cursorGapRefused,        2u);
    BOOST_CHECK_EQUAL(c.scanRangeFillTriggered,  2u);
    // inOrderResumed bumps once per drain pass that applied >=1 entries.
    BOOST_CHECK_EQUAL(c.inOrderResumed,          2u);
}

// -----------------------------------------------------------------------
// Case 5: harness smoke - epoch advance, pull failure, late resume.
// -----------------------------------------------------------------------
//
// Combines three sub-checks against one gate instance:
//   (a) epoch advance with sequence != 1 is treated as a gap
//       (EpochAdvanced cursor action -> refused). Once entry
//       (epoch=1, seq=1) is parked alongside, the drain resumes.
//   (b) a pull-failure leaves the entry parked and bumps the
//       refusal counters but not the resume counter. The next
//       successful pull / arrival drains it.
//   (c) the spec-mandated invariant - per-origin apply log is
//       contiguous, no gaps, no duplicates, no reorder - holds at
//       quiesce.
BOOST_AUTO_TEST_CASE(HarnessSmoke)
{
    ScopedEnv env("SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION", "1");

    OrderingHarness h;

    // (a) Epoch 0, sequence 1 in order.
    h.originLogs[42].Push(0, 1, 1000);
    BOOST_CHECK(h.Send(42, 0, 1, 1000) == HeadsyncOrderingStatus::AppliedInOrder);

    // (b) Pull failure path: send seq=3 on origin 42 in epoch 0
    //     with no entry 2 in the owner log (simulate "owner does
    //     not yet have it"). pullFailNext forces the pull to fail;
    //     the entry parks, counters reflect refusal.
    h.pullFailNext = true;
    auto rFail = h.Send(42, 0, 3, 1003);
    BOOST_CHECK(rFail == HeadsyncOrderingStatus::PullFailed);

    auto cMid = h.gate.GetCounters();
    BOOST_CHECK_EQUAL(cMid.outOfOrderApplyDetected, 1u);
    BOOST_CHECK_EQUAL(cMid.cursorGapRefused,        1u);
    BOOST_CHECK_EQUAL(cMid.scanRangeFillTriggered,  1u);
    BOOST_CHECK_EQUAL(cMid.inOrderResumed,          0u);
    BOOST_CHECK_EQUAL(h.gate.PendingSize(42),       1u);

    // Late resume: feed the missing seq=2 in order. Drain picks up
    // seq=2 (in-order apply) AND seq=3 (parked) contiguously.
    h.originLogs[42].Push(0, 2, 1002);
    auto rResume = h.Send(42, 0, 2, 1002);
    BOOST_CHECK(rResume == HeadsyncOrderingStatus::AppliedInOrder);

    auto cAfter = h.gate.GetCounters();
    // No new out-of-order detected on the in-order arrival; the
    // drain did fire so inOrderResumed bumps to 1.
    BOOST_CHECK_EQUAL(cAfter.outOfOrderApplyDetected, 1u);
    BOOST_CHECK_EQUAL(cAfter.cursorGapRefused,        1u);
    BOOST_CHECK_EQUAL(cAfter.scanRangeFillTriggered,  1u);
    BOOST_CHECK_EQUAL(cAfter.inOrderResumed,          1u);

    BOOST_CHECK_EQUAL(h.gate.GetCursor(42).sequence, 3u);
    BOOST_CHECK_EQUAL(h.gate.PendingSize(42),        0u);

    // (a, continued) Epoch advance to (epoch=1, seq=2) with no
    // (epoch=1, seq=1) yet -> EpochAdvanced cursor action -> gap.
    // The owner's pull range is asked from (epoch=0, seq=4) (the
    // expected next slot in the current epoch), which the owner's
    // log fulfils empty (no more entries in epoch 0); but we
    // present (epoch=1, seq=1) and (epoch=1, seq=2) in the owner
    // log so the fill returns both, the drain applies both, and
    // the cursor reaches (epoch=1, seq=2). Note: the gate is per-
    // spec spec-compliant only when the pull RPC honours the
    // (fromEpoch, fromSeq) >= bound and serves both epochs in one
    // response. Our owner's RangeSince does that.
    h.originLogs[42].Push(1, 1, 2001);
    h.originLogs[42].Push(1, 2, 2002);

    auto rEpoch = h.Send(42, 1, 2, 2002);
    BOOST_CHECK(rEpoch == HeadsyncOrderingStatus::FilledAndResumed);

    BOOST_CHECK_EQUAL(h.gate.GetCursor(42).epoch,    1u);
    BOOST_CHECK_EQUAL(h.gate.GetCursor(42).sequence, 2u);

    auto cFinal = h.gate.GetCounters();
    // Second out-of-order arrival -> +1 detected/refused/fill.
    BOOST_CHECK_EQUAL(cFinal.outOfOrderApplyDetected, 2u);
    BOOST_CHECK_EQUAL(cFinal.cursorGapRefused,        2u);
    BOOST_CHECK_EQUAL(cFinal.scanRangeFillTriggered,  2u);
    BOOST_CHECK_EQUAL(cFinal.inOrderResumed,          2u);

    // (c) Apply-log invariant: per-origin contiguous, no
    // duplicates, no reorder.
    BOOST_CHECK(h.AppliedIsContiguous(42));

    // Concretely: sequence ran 0:1, 0:2, 0:3, 1:1, 1:2.
    BOOST_REQUIRE_EQUAL(h.appliedLog[42].size(), 5u);
    BOOST_CHECK_EQUAL(h.appliedLog[42][0].epoch,    0u);
    BOOST_CHECK_EQUAL(h.appliedLog[42][0].sequence, 1u);
    BOOST_CHECK_EQUAL(h.appliedLog[42][2].epoch,    0u);
    BOOST_CHECK_EQUAL(h.appliedLog[42][2].sequence, 3u);
    BOOST_CHECK_EQUAL(h.appliedLog[42][3].epoch,    1u);
    BOOST_CHECK_EQUAL(h.appliedLog[42][3].sequence, 1u);
    BOOST_CHECK_EQUAL(h.appliedLog[42][4].epoch,    1u);
    BOOST_CHECK_EQUAL(h.appliedLog[42][4].sequence, 2u);
}

BOOST_AUTO_TEST_SUITE_END()
