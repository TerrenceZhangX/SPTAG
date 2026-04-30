// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Unit tests for Helper::RetryBudget.
// No docker needed. Verifies:
//   - attempt cap exhaustion
//   - wall-deadline cap
//   - exponential backoff is monotonic-but-not-equal due to jitter
//   - record_attempt observably sleeps and decrements remaining
//   - PDConfig() and DefaultConfig() match the case-spec numbers

#include "inc/Test.h"
#include "inc/Helper/RetryBudget.h"

#include <chrono>
#include <set>
#include <thread>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::Helper;

BOOST_AUTO_TEST_SUITE(RetryBudgetTest)

BOOST_AUTO_TEST_CASE(DefaultConfigMatchesSpec)
{
    auto& c = RetryBudget::DefaultConfig();
    BOOST_CHECK_EQUAL(c.total_wall.count(), 2000);
    BOOST_CHECK_EQUAL(c.max_attempts, 6);
    BOOST_CHECK_EQUAL(c.base_backoff.count(), 20);
    BOOST_CHECK_EQUAL(c.cap_backoff.count(), 500);
    BOOST_CHECK_CLOSE(c.jitter, 0.5, 1e-6);
}

BOOST_AUTO_TEST_CASE(PDConfigMatchesSpec)
{
    // pd-reconnect-during-retry: pd_total_wall=1s, pd_max_attempts=3
    auto& c = RetryBudget::PDConfig();
    BOOST_CHECK_EQUAL(c.total_wall.count(), 1000);
    BOOST_CHECK_EQUAL(c.max_attempts, 3);
}

BOOST_AUTO_TEST_CASE(AttemptCapExhausts)
{
    // Tiny budget with attempt cap = 3, large wall so wall does not fire.
    RetryBudget::Config cfg;
    cfg.total_wall   = std::chrono::milliseconds(60000);
    cfg.max_attempts = 3;
    cfg.base_backoff = std::chrono::milliseconds(0);  // no real sleep
    cfg.cap_backoff  = std::chrono::milliseconds(0);
    cfg.jitter       = 0.0;
    RetryBudget b(cfg);

    int loops = 0;
    while (b.should_retry()) {
        ++loops;
        if (!b.record_attempt()) break;
    }
    BOOST_CHECK_EQUAL(b.attempts(), 3);
    BOOST_CHECK_EQUAL(loops, 3);
    BOOST_CHECK(b.exhausted());
}

BOOST_AUTO_TEST_CASE(DeadlineCapExhausts)
{
    // Wall cap = 80 ms, attempts huge. Expect to bail on wall, not on count.
    RetryBudget::Config cfg;
    cfg.total_wall   = std::chrono::milliseconds(80);
    cfg.max_attempts = 1000;
    cfg.base_backoff = std::chrono::milliseconds(20);
    cfg.cap_backoff  = std::chrono::milliseconds(20);
    cfg.jitter       = 0.0;
    RetryBudget b(cfg);

    auto t0 = std::chrono::steady_clock::now();
    int loops = 0;
    bool exit_via_budget = false;
    while (b.should_retry()) {
        ++loops;
        if (!b.record_attempt()) { exit_via_budget = true; break; }
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    BOOST_CHECK(exit_via_budget);
    BOOST_CHECK_LT(b.attempts(), 1000);
    // Should not run forever; should be under ~200 ms (wall + 1 backoff slack).
    BOOST_CHECK_LT(elapsed, 250);
    // After record_attempt returned false, the budget is exhausted.
    BOOST_CHECK(b.exhausted());
}

BOOST_AUTO_TEST_CASE(RemainingDecreases)
{
    RetryBudget::Config cfg;
    cfg.total_wall   = std::chrono::milliseconds(500);
    cfg.max_attempts = 100;
    cfg.base_backoff = std::chrono::milliseconds(50);
    cfg.cap_backoff  = std::chrono::milliseconds(50);
    cfg.jitter       = 0.0;
    RetryBudget b(cfg);

    auto r0 = b.remaining();
    b.record_attempt();
    auto r1 = b.remaining();
    BOOST_CHECK_LT(r1.count(), r0.count());
}

BOOST_AUTO_TEST_CASE(BackoffMonotonicButJittered)
{
    // With jitter > 0, repeated draws at the same n should differ but
    // average to roughly base*2^n. Verify (a) nominal monotone growth
    // ignoring jitter, (b) at least 2 distinct values out of 50 draws.
    RetryBudget::Config cfg;
    cfg.total_wall   = std::chrono::milliseconds(60000);
    cfg.max_attempts = 100;
    cfg.base_backoff = std::chrono::milliseconds(20);
    cfg.cap_backoff  = std::chrono::milliseconds(500);
    cfg.jitter       = 0.5;
    RetryBudget b(cfg);

    // Mean grows monotonically until cap.
    auto avg_at = [&](int n) {
        long long s = 0;
        const int K = 200;
        for (int i = 0; i < K; ++i) s += b.compute_backoff(n).count();
        return double(s) / K;
    };
    double a0 = avg_at(0);
    double a1 = avg_at(1);
    double a2 = avg_at(2);
    double a3 = avg_at(3);
    BOOST_CHECK_LT(a0, a1);
    BOOST_CHECK_LT(a1, a2);
    BOOST_CHECK_LT(a2, a3);

    // Jitter: at fixed n, several distinct values should appear.
    std::set<long long> distinct;
    for (int i = 0; i < 50; ++i) {
        distinct.insert(b.compute_backoff(2).count());
    }
    BOOST_CHECK_GT(distinct.size(), 1u);
}

BOOST_AUTO_TEST_CASE(BackoffCappedAtCap)
{
    RetryBudget::Config cfg;
    cfg.total_wall   = std::chrono::milliseconds(60000);
    cfg.max_attempts = 100;
    cfg.base_backoff = std::chrono::milliseconds(20);
    cfg.cap_backoff  = std::chrono::milliseconds(500);
    cfg.jitter       = 0.0; // deterministic: equal to clipped value
    RetryBudget b(cfg);

    // 20 * 2^10 = 20480 >> 500 ms cap. Expect exactly cap (jitter=0).
    auto bo = b.compute_backoff(10);
    BOOST_CHECK_EQUAL(bo.count(), 500);
}

BOOST_AUTO_TEST_CASE(BackoffClampedToRemaining)
{
    // Even without jitter, backoff must not exceed remaining wall.
    RetryBudget::Config cfg;
    cfg.total_wall   = std::chrono::milliseconds(50);
    cfg.max_attempts = 100;
    cfg.base_backoff = std::chrono::milliseconds(500);
    cfg.cap_backoff  = std::chrono::milliseconds(500);
    cfg.jitter       = 0.0;
    RetryBudget b(cfg);
    auto bo = b.compute_backoff(0);
    BOOST_CHECK_LE(bo.count(), 50);
}

BOOST_AUTO_TEST_CASE(RecordAttemptReturnsFalseOnExhaustion)
{
    RetryBudget::Config cfg;
    cfg.total_wall   = std::chrono::milliseconds(60000);
    cfg.max_attempts = 2;
    cfg.base_backoff = std::chrono::milliseconds(0);
    cfg.cap_backoff  = std::chrono::milliseconds(0);
    cfg.jitter       = 0.0;
    RetryBudget b(cfg);

    BOOST_CHECK(b.should_retry());
    bool more = b.record_attempt();
    BOOST_CHECK(more);                  // 1/2
    more = b.record_attempt();
    BOOST_CHECK(!more);                 // 2/2 → no more
    BOOST_CHECK(!b.should_retry());
    BOOST_CHECK(b.exhausted());
}

BOOST_AUTO_TEST_SUITE_END()
