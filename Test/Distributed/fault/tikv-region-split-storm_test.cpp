// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-region-split-storm
//
// Tier 1 HARD repro of the tikv-region-split-storm invariants:
//
//   * env-off: wrapper dormant; each routed write succeeds
//     against the declared region, no counter moves.
//   * env-armed + all regions stable (splitsRemaining=0): wrapper
//     drives one write per key on the declared region, no
//     EpochNotMatch path is taken, no counter moves.
//   * env-armed + one in-flight split: wrapper observes
//     EpochNotMatch, refreshes the region cache, retries on the
//     post-split region, lands the write.
//   * env-armed + storm across N keys: refresh+retry counters
//     scale with total splits-in-flight; every key eventually
//     lands.
//   * env-armed + retries blow past the RetryBudget: wrapper
//     fails fast with ErrorCode::SplitStormRetryCapExceeded,
//     ++m_clientErrorReturned.
//
// Wrapper under test:  TikvRegionSplitStormGated (header-only;
//                      defers to Helper::RetryBudget + injected
//                      per-region write callable).
// Env-gate:            SPTAG_FAULT_TIKV_REGION_SPLIT_STORM
// Tier 2 (1M perf):    DEFERRED -- non hot-path (region-split
//                      retry logic sits on the TiKV write
//                      admission edge, not the SPANN search inner
//                      loop).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvRegionSplitStormGated.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
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
using RegionId = TikvRegionSplitStormGated::RegionId;

// Build a permissive retry budget for the storm tests.
Helper::RetryBudget::Config StormBudgetConfig(int maxAttempts, int budgetMs) {
    Helper::RetryBudget::Config c = Helper::RetryBudget::DefaultConfig();
    c.max_attempts = maxAttempts;
    c.total_wall = ms(budgetMs);
    return c;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvRegionSplitStormTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the gate unset the wrapper drives one
//    write per declared region and never enters the
//    EpochNotMatch/refresh path -- even if splitsRemaining says
//    the region is mid-split, env-off is a pass-through.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_SPLIT_STORM", nullptr);
    BOOST_REQUIRE(!TikvRegionSplitStormGated::EnvArmed());

    TikvRegionSplitStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvRegionSplitStormGated client(cfg,
        [&](RegionId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSplitGuard(
            /*regionRoutes=*/{10, 20, 30},
            /*splitsRemaining=*/{{10, 3}, {20, 3}, {30, 3}},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK_EQUAL(out.epoch_misses, 0u);
    BOOST_CHECK_EQUAL(out.refreshes, 0u);
    BOOST_CHECK_EQUAL(out.new_region_lands, 0u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 3);
    BOOST_CHECK_EQUAL(calls.load(), 3);

    BOOST_CHECK_EQUAL(client.EpochNotMatchObserved(),   0u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(),    0u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToNewRegion(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline: every region is already stable. Env-armed but
//    splitsRemaining=0 everywhere -> wrapper writes once per key,
//    counters stay at zero (no epoch miss, no refresh). The
//    "land" counter does move because env-armed lands count.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineStableRegions)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_SPLIT_STORM", "1");
    BOOST_REQUIRE(TikvRegionSplitStormGated::EnvArmed());

    TikvRegionSplitStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvRegionSplitStormGated client(cfg,
        [&](RegionId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSplitGuard(
            /*regionRoutes=*/{100, 200, 300},
            /*splitsRemaining=*/{{100, 0}, {200, 0}, {300, 0}},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.epoch_misses, 0u);
    BOOST_CHECK_EQUAL(out.refreshes, 0u);
    BOOST_CHECK_EQUAL(out.new_region_lands, 3u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 3);
    BOOST_CHECK_EQUAL(calls.load(), 3);

    BOOST_CHECK_EQUAL(client.EpochNotMatchObserved(),   0u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(),    0u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToNewRegion(),  3u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
}

// ---------------------------------------------------------------------------
// 3) Single in-flight split converges. One key, region 500 has 1
//    split remaining, budget allows 2 attempts -> wrapper sees one
//    EpochNotMatch, refreshes once, retries on the post-split
//    region id (1500), lands.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SingleSplitConverges)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_SPLIT_STORM", "1");

    TikvRegionSplitStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = StormBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    TikvRegionSplitStormGated client(cfg,
        [&](RegionId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSplitGuard(
            /*regionRoutes=*/{500},
            /*splitsRemaining=*/{{500, 1}},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.epoch_misses, 1u);
    BOOST_CHECK_EQUAL(out.refreshes, 1u);
    BOOST_CHECK_EQUAL(out.new_region_lands, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.EpochNotMatchObserved(),   1u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(),    1u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToNewRegion(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
}

// ---------------------------------------------------------------------------
// 4) Storm across multiple keys. 3 keys, each on a region with 2
//    splits remaining; permissive budget -> 6 refreshes total
//    (3 keys * 2 splits), every key eventually lands.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(MultiSplitStorm)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_SPLIT_STORM", "1");

    TikvRegionSplitStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = StormBudgetConfig(/*maxAttempts=*/10, /*budgetMs=*/2000);

    std::atomic<int> calls{0};
    TikvRegionSplitStormGated client(cfg,
        [&](RegionId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSplitGuard(
            /*regionRoutes=*/{11, 22, 33},
            /*splitsRemaining=*/{{11, 2}, {22, 2}, {33, 2}},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.epoch_misses, 6u);
    BOOST_CHECK_EQUAL(out.refreshes, 6u);
    BOOST_CHECK_EQUAL(out.new_region_lands, 3u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 3);
    BOOST_CHECK_EQUAL(calls.load(), 3);

    BOOST_CHECK_EQUAL(client.EpochNotMatchObserved(),   6u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(),    6u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToNewRegion(),  3u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
}

// ---------------------------------------------------------------------------
// 5) Retry cap exceeded. One key on a region with 5 splits
//    remaining, but budget allows only 2 attempts -> wrapper
//    fails fast with ErrorCode::SplitStormRetryCapExceeded,
//    ++m_clientErrorReturned, no land counter.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RetryCapExceeded)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_SPLIT_STORM", "1");

    TikvRegionSplitStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = StormBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    TikvRegionSplitStormGated client(cfg,
        [&](RegionId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSplitGuard(
            /*regionRoutes=*/{77},
            /*splitsRemaining=*/{{77, 5}},
            budget);

    BOOST_CHECK(out.code == ErrorCode::SplitStormRetryCapExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.new_region_lands, 0u);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteRoutedToNewRegion(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     1u);

    // Wiring smoke: distinct error code.
    BOOST_CHECK(static_cast<int>(ErrorCode::SplitStormRetryCapExceeded)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::SplitStormRetryCapExceeded)
                != static_cast<int>(ErrorCode::CompactionStallExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::SplitStormRetryCapExceeded)
                != static_cast<int>(ErrorCode::WriteStall));
}

BOOST_AUTO_TEST_SUITE_END()
