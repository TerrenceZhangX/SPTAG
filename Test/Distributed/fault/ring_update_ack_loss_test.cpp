// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: ring-update-ack-loss
//
// Validates the bounded ACK-retransmit + eviction surface that
// the dispatcher arms when SPTAG_FAULT_RING_UPDATE_ACK_LOSS is
// set. With the env unset, the surface is dormant: every counter
// stays zero and the production unbounded-retry path is reused.
//
// Tier 1 (HARD) — 5 cases:
//   1. EnvOffDormancy             — env unset → no work, no counters.
//   2. AckOnceSucceeds            — ACK lands first try; no retransmit.
//   3. AckOnceLostThenAcked       — single retransmit; worker converges.
//   4. AckPersistentLossEvicts    — > maxAttempts → evicted; counters tick.
//   5. HarnessSmoke               — RingUpdateAckLossArmed() reflects env.
//
// Invariants: ring is never partially-applied (no torn ring), and
// worker state converges to either {acked} or {evicted}, never both.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/RingAckRetry.h"

#include <chrono>
#include <cstdlib>
#include <string>
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

// Tight config so the unit test runs in milliseconds.
RingAckRetryConfig FastConfig() {
    RingAckRetryConfig c;
    c.maxAttempts = 3;
    c.initialBackoff = std::chrono::milliseconds(10);
    c.maxBackoff = std::chrono::milliseconds(80);
    return c;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(RingUpdateAckLossTest)

// ---------------------------------------------------------------------------
// 1. Env OFF dormancy — wrapper inactive; counters are zero by
// virtue of *never* having been ticked. Confirms RingUpdateAckLoss
// is a Tier-1 dormant surface.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy) {
    ScopedEnv env("SPTAG_FAULT_RING_UPDATE_ACK_LOSS", nullptr);
    BOOST_CHECK(!RingUpdateAckLossArmedDynamic());

    RingAckRetry r(FastConfig());
    auto c = r.GetCounters();
    BOOST_CHECK_EQUAL(c.retransmits, 0u);
    BOOST_CHECK_EQUAL(c.timeouts, 0u);
    BOOST_CHECK_EQUAL(c.evicted, 0u);

    // Production-style: caller checks armed and skips the state
    // machine entirely. Even without that check, untouched state
    // emits Idle and counters stay at zero.
    auto act = r.Tick(/*workerId=*/1, std::chrono::steady_clock::now());
    BOOST_CHECK(act == RingAckAction::Idle);
    c = r.GetCounters();
    BOOST_CHECK_EQUAL(c.retransmits + c.timeouts + c.evicted, 0u);
}

// ---------------------------------------------------------------------------
// 2. ACK on the first attempt — the most common path. State
// machine emits Idle on every tick and the worker is observably
// acked.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(AckOnceSucceeds) {
    ScopedEnv env("SPTAG_FAULT_RING_UPDATE_ACK_LOSS", "1");
    BOOST_REQUIRE(RingUpdateAckLossArmedDynamic());

    RingAckRetry r(FastConfig());
    auto t0 = std::chrono::steady_clock::now();
    r.OnNewVersion(/*version=*/2, std::vector<int>{1, 2, 3}, t0);

    // Worker 1 acks immediately.
    r.OnAck(/*workerId=*/1, /*version=*/2);

    // Even after the initial backoff window expires, worker 1 stays
    // Idle because it has acked.
    auto act = r.Tick(1, t0 + std::chrono::milliseconds(50));
    BOOST_CHECK(act == RingAckAction::Idle);
    BOOST_CHECK(r.IsAcked(1));
    BOOST_CHECK(!r.IsEvicted(1));

    auto c = r.GetCounters();
    BOOST_CHECK_EQUAL(c.retransmits, 0u);
    BOOST_CHECK_EQUAL(c.timeouts, 0u);
    BOOST_CHECK_EQUAL(c.evicted, 0u);
}

// ---------------------------------------------------------------------------
// 3. ACK lost once → exactly one retransmit, then ACK arrives.
// Worker converges to {acked}; counters: retransmits == 1,
// timeouts == 1, evicted == 0.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(AckOnceLostThenAcked) {
    ScopedEnv env("SPTAG_FAULT_RING_UPDATE_ACK_LOSS", "1");
    BOOST_REQUIRE(RingUpdateAckLossArmedDynamic());

    auto cfg = FastConfig();
    RingAckRetry r(cfg);
    auto t0 = std::chrono::steady_clock::now();
    r.OnNewVersion(/*version=*/3, std::vector<int>{2}, t0);

    // Within the first backoff window: still Idle.
    auto act = r.Tick(2, t0 + cfg.initialBackoff / 2);
    BOOST_CHECK(act == RingAckAction::Idle);

    // After the first window: time-out, emit Retransmit.
    act = r.Tick(2, t0 + cfg.initialBackoff + std::chrono::milliseconds(1));
    BOOST_CHECK(act == RingAckAction::Retransmit);

    // ACK arrives between retransmit and next backoff expiry.
    r.OnAck(/*workerId=*/2, /*version=*/3);

    // Subsequent ticks stay Idle, no further retransmits.
    act = r.Tick(2, t0 + std::chrono::milliseconds(500));
    BOOST_CHECK(act == RingAckAction::Idle);

    BOOST_CHECK(r.IsAcked(2));
    BOOST_CHECK(!r.IsEvicted(2));

    auto c = r.GetCounters();
    BOOST_CHECK_EQUAL(c.retransmits, 1u);
    BOOST_CHECK_EQUAL(c.timeouts, 1u);
    BOOST_CHECK_EQUAL(c.evicted, 0u);
}

// ---------------------------------------------------------------------------
// 4. Persistent ACK loss → after maxAttempts retransmits, worker
// is marked evicted. State machine emits Evict exactly once;
// further ticks are Idle. Counters: retransmits ==
// cfg.maxAttempts, timeouts == cfg.maxAttempts + 1, evicted == 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(AckPersistentLossEvicts) {
    ScopedEnv env("SPTAG_FAULT_RING_UPDATE_ACK_LOSS", "1");
    BOOST_REQUIRE(RingUpdateAckLossArmedDynamic());

    auto cfg = FastConfig();
    RingAckRetry r(cfg);
    auto t0 = std::chrono::steady_clock::now();
    r.OnNewVersion(/*version=*/4, std::vector<int>{5}, t0);

    auto advance = std::chrono::milliseconds(0);
    auto bump = [&](std::chrono::milliseconds d){ advance += d; };

    // First time-out → Retransmit #1.
    bump(cfg.initialBackoff + std::chrono::milliseconds(1));
    BOOST_CHECK(r.Tick(5, t0 + advance) == RingAckAction::Retransmit);

    // Subsequent retransmits with exponential backoff.
    for (std::uint32_t i = 2; i <= cfg.maxAttempts; ++i) {
        bump(cfg.maxBackoff + std::chrono::milliseconds(1));
        BOOST_CHECK(r.Tick(5, t0 + advance) == RingAckAction::Retransmit);
    }

    // Cap exceeded → Evict.
    bump(cfg.maxBackoff + std::chrono::milliseconds(1));
    BOOST_CHECK(r.Tick(5, t0 + advance) == RingAckAction::Evict);

    // Further ticks are Idle.
    bump(cfg.maxBackoff + std::chrono::milliseconds(1));
    BOOST_CHECK(r.Tick(5, t0 + advance) == RingAckAction::Idle);

    // Late ACK arrives — must not un-evict (worker state convergence).
    r.OnAck(5, 4);
    BOOST_CHECK(r.IsEvicted(5));
    BOOST_CHECK(!r.IsAcked(5));

    auto c = r.GetCounters();
    BOOST_CHECK_EQUAL(c.retransmits, cfg.maxAttempts);
    BOOST_CHECK_EQUAL(c.timeouts, cfg.maxAttempts + 1u);
    BOOST_CHECK_EQUAL(c.evicted, 1u);
}

// ---------------------------------------------------------------------------
// 5. Harness smoke — armed-probe responds to env. Includes ACKs
// for stale versions to confirm they don't perturb state.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(HarnessSmoke) {
    {
        ScopedEnv off("SPTAG_FAULT_RING_UPDATE_ACK_LOSS", nullptr);
        BOOST_CHECK(!RingUpdateAckLossArmedDynamic());
    }
    {
        ScopedEnv on("SPTAG_FAULT_RING_UPDATE_ACK_LOSS", "1");
        BOOST_CHECK(RingUpdateAckLossArmedDynamic());
    }
    {
        ScopedEnv zero("SPTAG_FAULT_RING_UPDATE_ACK_LOSS", "0");
        BOOST_CHECK(!RingUpdateAckLossArmedDynamic());
    }

    // Stale-version ACKs are dropped.
    RingAckRetry r(FastConfig());
    auto t0 = std::chrono::steady_clock::now();
    r.OnNewVersion(/*version=*/10, std::vector<int>{1}, t0);
    r.OnAck(1, /*version=*/9);  // stale
    BOOST_CHECK(!r.IsAcked(1));
    r.OnAck(1, /*version=*/10);
    BOOST_CHECK(r.IsAcked(1));

    // Re-publishing a new version resets per-worker state.
    auto t1 = std::chrono::steady_clock::now();
    r.OnNewVersion(/*version=*/11, std::vector<int>{1}, t1);
    BOOST_CHECK(!r.IsAcked(1));
    BOOST_CHECK(!r.IsEvicted(1));
}

BOOST_AUTO_TEST_SUITE_END()
