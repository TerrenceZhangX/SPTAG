// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-region-merge-during-writes
//
// Tier 1 HARD repro of the tikv-region-merge-during-writes
// invariants:
//
//   * env-off: wrapper dormant; each routed write succeeds
//     against the declared region, no counter moves.
//   * env-armed + empty mergeMap: every region is stable ->
//     wrapper drives one write per key, no RegionNotFound path is
//     taken, ++m_writeRoutedToMergedRegion only.
//   * env-armed + single-pair merge (r1->r3, r2->r3): write to r1
//     observes RegionNotFound, resolves to r3, lands the write.
//   * env-armed + chained merge (r1->r2, r2->r3): write to r1
//     resolves through two hops to r3 before landing;
//     ++m_regionNotFoundObserved == ++m_mergeTargetResolved == 2.
//   * env-armed + retries blow past the RetryBudget: wrapper
//     fails fast with ErrorCode::MergeRetryCapExceeded,
//     ++m_clientErrorReturned.
//
// Wrapper under test:  TikvRegionMergeDuringWritesGated (header-
//                      only; defers to Helper::RetryBudget +
//                      injected per-region write callable).
// Env-gate:            SPTAG_FAULT_TIKV_REGION_MERGE_DURING_WRITES
// Tier 2 (1M perf):    DEFERRED -- non hot-path (region-merge
//                      re-route logic sits on the TiKV write
//                      admission edge, not the SPANN search inner
//                      loop).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvRegionMergeDuringWritesGated.h"
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
using RegionId = TikvRegionMergeDuringWritesGated::RegionId;

Helper::RetryBudget::Config MergeBudgetConfig(int maxAttempts, int budgetMs) {
    Helper::RetryBudget::Config c = Helper::RetryBudget::DefaultConfig();
    c.max_attempts = maxAttempts;
    c.total_wall = ms(budgetMs);
    return c;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvRegionMergeDuringWritesTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the gate unset the wrapper drives one
//    write per declared region; even if mergeMap declares the
//    region merged, env-off is pass-through.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_MERGE_DURING_WRITES", nullptr);
    BOOST_REQUIRE(!TikvRegionMergeDuringWritesGated::EnvArmed());

    TikvRegionMergeDuringWritesGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvRegionMergeDuringWritesGated client(cfg,
        [&](RegionId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithMergeGuard(
            /*regionRoutes=*/{10, 20, 30},
            /*mergeMap=*/{{10, 99}, {20, 99}, {30, 99}},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK_EQUAL(out.region_not_found, 0u);
    BOOST_CHECK_EQUAL(out.merge_target_resolved, 0u);
    BOOST_CHECK_EQUAL(out.merged_region_lands, 0u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 3);
    BOOST_CHECK_EQUAL(calls.load(), 3);

    BOOST_CHECK_EQUAL(client.RegionNotFoundObserved(),     0u);
    BOOST_CHECK_EQUAL(client.MergeTargetResolved(),        0u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToMergedRegion(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline no-merge: env-armed but mergeMap empty -> every
//    region is stable, wrapper writes once per key, only
//    m_writeRoutedToMergedRegion moves.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineNoMerge)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_MERGE_DURING_WRITES", "1");
    BOOST_REQUIRE(TikvRegionMergeDuringWritesGated::EnvArmed());

    TikvRegionMergeDuringWritesGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvRegionMergeDuringWritesGated client(cfg,
        [&](RegionId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithMergeGuard(
            /*regionRoutes=*/{100, 200, 300},
            /*mergeMap=*/{},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.region_not_found, 0u);
    BOOST_CHECK_EQUAL(out.merge_target_resolved, 0u);
    BOOST_CHECK_EQUAL(out.merged_region_lands, 3u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 3);
    BOOST_CHECK_EQUAL(calls.load(), 3);

    BOOST_CHECK_EQUAL(client.RegionNotFoundObserved(),     0u);
    BOOST_CHECK_EQUAL(client.MergeTargetResolved(),        0u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToMergedRegion(),  3u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 3) Single-pair merge resolved. mergeMap={r1:r3, r2:r3}; write
//    to r1 -> RegionNotFound once, resolve to r3, land. Counters:
//    1 not-found, 1 resolved, 1 land.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SinglePairMergeResolved)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_MERGE_DURING_WRITES", "1");

    TikvRegionMergeDuringWritesGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = MergeBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    std::atomic<RegionId> lastTarget{0};
    TikvRegionMergeDuringWritesGated client(cfg,
        [&](RegionId r, ms) {
            ++calls;
            lastTarget = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithMergeGuard(
            /*regionRoutes=*/{1},
            /*mergeMap=*/{{1, 3}, {2, 3}},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.region_not_found, 1u);
    BOOST_CHECK_EQUAL(out.merge_target_resolved, 1u);
    BOOST_CHECK_EQUAL(out.merged_region_lands, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastTarget.load(), static_cast<RegionId>(3));

    BOOST_CHECK_EQUAL(client.RegionNotFoundObserved(),     1u);
    BOOST_CHECK_EQUAL(client.MergeTargetResolved(),        1u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToMergedRegion(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 4) Chained merge resolved. mergeMap={r1:r2, r2:r3}; write to
//    r1 -> RegionNotFound -> r2 -> RegionNotFound -> r3 (stable).
//    Counters: 2 not-found, 2 resolved, 1 land.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ChainedMergeResolved)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_MERGE_DURING_WRITES", "1");

    TikvRegionMergeDuringWritesGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = MergeBudgetConfig(/*maxAttempts=*/4, /*budgetMs=*/1000);

    std::atomic<int> calls{0};
    std::atomic<RegionId> lastTarget{0};
    TikvRegionMergeDuringWritesGated client(cfg,
        [&](RegionId r, ms) {
            ++calls;
            lastTarget = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithMergeGuard(
            /*regionRoutes=*/{1},
            /*mergeMap=*/{{1, 2}, {2, 3}},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.region_not_found, 2u);
    BOOST_CHECK_EQUAL(out.merge_target_resolved, 2u);
    BOOST_CHECK_EQUAL(out.merged_region_lands, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastTarget.load(), static_cast<RegionId>(3));

    BOOST_CHECK_EQUAL(client.RegionNotFoundObserved(),     2u);
    BOOST_CHECK_EQUAL(client.MergeTargetResolved(),        2u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToMergedRegion(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 5) Retry cap exceeded. Deep chain r1->r2->r3->r4 with budget
//    max_attempts=1 -> wrapper fails fast with
//    ErrorCode::MergeRetryCapExceeded.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RetryCapExceeded)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_MERGE_DURING_WRITES", "1");

    TikvRegionMergeDuringWritesGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = MergeBudgetConfig(/*maxAttempts=*/1, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    TikvRegionMergeDuringWritesGated client(cfg,
        [&](RegionId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithMergeGuard(
            /*regionRoutes=*/{1},
            /*mergeMap=*/{{1, 2}, {2, 3}, {3, 4}},
            budget);

    BOOST_CHECK(out.code == ErrorCode::MergeRetryCapExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.merged_region_lands, 0u);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteRoutedToMergedRegion(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        1u);

    // Wiring smoke: distinct error code.
    BOOST_CHECK(static_cast<int>(ErrorCode::MergeRetryCapExceeded)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::MergeRetryCapExceeded)
                != static_cast<int>(ErrorCode::SplitStormRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::MergeRetryCapExceeded)
                != static_cast<int>(ErrorCode::CompactionStallExceeded));
}

BOOST_AUTO_TEST_SUITE_END()
