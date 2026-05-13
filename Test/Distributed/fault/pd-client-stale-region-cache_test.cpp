// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: pd-client-stale-region-cache
//
// Tier 1 HARD repro of the pd-client-stale-region-cache
// invariants (PD-client stale-region-cache refresh cap):
//
//   * env-off: wrapper dormant; one pass-through write against
//     the cached route, no stale-cache counter moves.
//   * env-armed + empty pattern: cached route is taken as
//     converged -> wrapper drives one write,
//     ++m_writeSucceededAfterRefresh only.
//   * env-armed + single stale step: one cache miss observed,
//     one PD query, then write lands on the refreshed route.
//   * env-armed + multi stale steps converging: K cache
//     misses, K PD queries, write lands on the final converged
//     route.
//   * env-armed + persistent cache divergence and budget blows:
//     wrapper fails fast with
//     ErrorCode::StaleRegionCacheRetryCapExceeded.
//
// Wrapper under test:  PdClientStaleRegionCacheGated
//                      (header-only; defers to
//                      Helper::RetryBudget + injected per-route
//                      write callable).
// Env-gate:            SPTAG_FAULT_PD_CLIENT_STALE_REGION_CACHE
// Tier 2 (1M perf):    DEFERRED-NO-HOT-PATH -- this case is not
//                      on the strict 5% hot-path list; soft
//                      ±20% review is still due once the gate
//                      stabilizes.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/PdClientStaleRegionCacheGated.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>
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

using ms = std::chrono::milliseconds;
using RegionId = PdClientStaleRegionCacheGated::RegionId;

Helper::RetryBudget::Config StaleCacheBudgetConfig(int maxAttempts, int budgetMs) {
    Helper::RetryBudget::Config c = Helper::RetryBudget::DefaultConfig();
    c.max_attempts = maxAttempts;
    c.total_wall = ms(budgetMs);
    return c;
}

RouteEntry R(std::int32_t store, std::int64_t epoch) {
    RouteEntry e;
    e.leader_store_id = store;
    e.region_epoch    = epoch;
    return e;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(PdClientStaleRegionCacheTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. Wrapper drives one pass-through write
//    against the cached route; the PD pattern is ignored.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_PD_CLIENT_STALE_REGION_CACHE", nullptr);
    BOOST_REQUIRE(!PdClientStaleRegionCacheGated::EnvArmed());

    PdClientStaleRegionCacheGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    RouteEntry lastRoute{};
    PdClientStaleRegionCacheGated client(cfg,
        [&](RegionId, const RouteEntry& r, ms) {
            ++calls;
            lastRoute = r;
            return ErrorCode::Success;
        });

    const RouteEntry cached = R(20, 5);
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithStaleCacheGuard(
            /*regionId=*/42,
            /*cachedRoute=*/cached,
            /*actualRoutePerAttempt=*/{R(21, 7), R(22, 9)},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK_EQUAL(out.stale_cache_hits, 0u);
    BOOST_CHECK_EQUAL(out.pd_queries_triggered, 0u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 0u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK(out.landed_on_route == cached);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK(lastRoute == cached);

    BOOST_CHECK_EQUAL(client.StaleCacheHit(),               0u);
    BOOST_CHECK_EQUAL(client.PdQueryTriggered(),            0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRefresh(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),         0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline fresh cache: env-armed + pattern == cached route ->
//    no cache miss, wrapper writes once, only
//    m_writeSucceededAfterRefresh moves.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineFreshCache)
{
    ScopedEnv env("SPTAG_FAULT_PD_CLIENT_STALE_REGION_CACHE", "1");
    BOOST_REQUIRE(PdClientStaleRegionCacheGated::EnvArmed());

    PdClientStaleRegionCacheGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    RouteEntry lastRoute{};
    PdClientStaleRegionCacheGated client(cfg,
        [&](RegionId, const RouteEntry& r, ms) {
            ++calls;
            lastRoute = r;
            return ErrorCode::Success;
        });

    const RouteEntry cached = R(20, 5);
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithStaleCacheGuard(
            /*regionId=*/42,
            /*cachedRoute=*/cached,
            /*actualRoutePerAttempt=*/{cached},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.stale_cache_hits, 0u);
    BOOST_CHECK_EQUAL(out.pd_queries_triggered, 0u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK(out.landed_on_route == cached);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK(lastRoute == cached);

    BOOST_CHECK_EQUAL(client.StaleCacheHit(),               0u);
    BOOST_CHECK_EQUAL(client.PdQueryTriggered(),            0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRefresh(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),         0u);
}

// ---------------------------------------------------------------------------
// 3) Single stale step. cached={20,5}, pattern={R'} -> one
//    cache miss, one PD query, write lands on R'.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SingleStaleRefreshes)
{
    ScopedEnv env("SPTAG_FAULT_PD_CLIENT_STALE_REGION_CACHE", "1");

    PdClientStaleRegionCacheGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = StaleCacheBudgetConfig(/*maxAttempts=*/4, /*budgetMs=*/1000);

    std::atomic<int> calls{0};
    RouteEntry lastRoute{};
    PdClientStaleRegionCacheGated client(cfg,
        [&](RegionId, const RouteEntry& r, ms) {
            ++calls;
            lastRoute = r;
            return ErrorCode::Success;
        });

    const RouteEntry cached    = R(20, 5);
    const RouteEntry converged = R(21, 7);

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithStaleCacheGuard(
            /*regionId=*/42,
            /*cachedRoute=*/cached,
            /*actualRoutePerAttempt=*/{converged, converged},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.stale_cache_hits, 1u);
    BOOST_CHECK_EQUAL(out.pd_queries_triggered, 1u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK(out.landed_on_route == converged);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK(lastRoute == converged);

    BOOST_CHECK_EQUAL(client.StaleCacheHit(),               1u);
    BOOST_CHECK_EQUAL(client.PdQueryTriggered(),            1u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRefresh(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),         0u);
}

// ---------------------------------------------------------------------------
// 4) Multi stale steps converging. cached={20,5}, pattern walks
//    through three intermediate routes before settling on a
//    converged route.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(MultiStaleConverges)
{
    ScopedEnv env("SPTAG_FAULT_PD_CLIENT_STALE_REGION_CACHE", "1");

    PdClientStaleRegionCacheGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = StaleCacheBudgetConfig(/*maxAttempts=*/6, /*budgetMs=*/1000);

    std::atomic<int> calls{0};
    RouteEntry lastRoute{};
    PdClientStaleRegionCacheGated client(cfg,
        [&](RegionId, const RouteEntry& r, ms) {
            ++calls;
            lastRoute = r;
            return ErrorCode::Success;
        });

    const RouteEntry cached    = R(20, 5);
    const RouteEntry step1     = R(21, 6);
    const RouteEntry step2     = R(22, 7);
    const RouteEntry converged = R(23, 9);

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithStaleCacheGuard(
            /*regionId=*/42,
            /*cachedRoute=*/cached,
            /*actualRoutePerAttempt=*/{step1, step2, converged, converged},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.stale_cache_hits, 3u);
    BOOST_CHECK_EQUAL(out.pd_queries_triggered, 3u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK(out.landed_on_route == converged);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK(lastRoute == converged);

    BOOST_CHECK_EQUAL(client.StaleCacheHit(),               3u);
    BOOST_CHECK_EQUAL(client.PdQueryTriggered(),            3u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRefresh(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),         0u);
}

// ---------------------------------------------------------------------------
// 5) Retry cap exceeded. Persistent stale-cache divergence on a
//    tight budget -> wrapper fails fast with
//    ErrorCode::StaleRegionCacheRetryCapExceeded.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RetryBudgetExhausted)
{
    ScopedEnv env("SPTAG_FAULT_PD_CLIENT_STALE_REGION_CACHE", "1");

    PdClientStaleRegionCacheGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = StaleCacheBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    PdClientStaleRegionCacheGated client(cfg,
        [&](RegionId, const RouteEntry&, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    const RouteEntry cached = R(20, 5);

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithStaleCacheGuard(
            /*regionId=*/42,
            /*cachedRoute=*/cached,
            /*actualRoutePerAttempt=*/{R(21, 6), R(22, 7), R(23, 8), R(24, 9), R(25, 10)},
            budget);

    BOOST_CHECK(out.code == ErrorCode::StaleRegionCacheRetryCapExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 0u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRefresh(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),         1u);

    // Wiring smoke: distinct error code.
    BOOST_CHECK(static_cast<int>(ErrorCode::StaleRegionCacheRetryCapExceeded)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::StaleRegionCacheRetryCapExceeded)
                != static_cast<int>(ErrorCode::PdReconnectRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::StaleRegionCacheRetryCapExceeded)
                != static_cast<int>(ErrorCode::GrpcTimeoutStormRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::StaleRegionCacheRetryCapExceeded)
                != static_cast<int>(ErrorCode::RegionErrorRetryCapExceeded));
}

BOOST_AUTO_TEST_SUITE_END()
