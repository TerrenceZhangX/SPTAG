// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-region-error-retry-cap
//
// Tier 1 HARD repro of the tikv-region-error-retry-cap
// invariants (the umbrella retry-cap for any TiKV
// RegionError kind):
//
//   * env-off: wrapper dormant; one pass-through write, no
//     region-error counter moves.
//   * env-armed + empty chain: no errors -> wrapper drives
//     one write, ++m_writeSucceededAfterRecovery only.
//   * env-armed + single error: chain={EpochNotMatch} with
//     budget>=1 -> 1 observed, 1 recovered, write lands.
//   * env-armed + chained errors of mixed kind:
//     chain={EpochNotMatch, NotLeader, RegionNotFound,
//     ServerIsBusy} with budget>=4 -> 4 observed, 4
//     recovered, write lands.
//   * env-armed + retries blow past the RetryBudget: wrapper
//     fails fast with ErrorCode::RegionErrorRetryCapExceeded,
//     ++m_clientErrorReturned.
//
// Wrapper under test:  TikvRegionErrorRetryCapGated
//                      (header-only; defers to
//                      Helper::RetryBudget + injected per-region
//                      write callable).
// Env-gate:            SPTAG_FAULT_TIKV_REGION_ERROR_RETRY_CAP
// Tier 2 (1M perf):    DEFERRED-PENDING-GATE -- hot-path
//                      (region-error retry-cap sits on the TiKV
//                      write admission path and is on the
//                      strict 5% hot-path list), but the 1M
//                      perf gate is currently in the noise
//                      floor.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvRegionErrorRetryCapGated.h"
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
using RegionId = TikvRegionErrorRetryCapGated::RegionId;

Helper::RetryBudget::Config RegionErrorBudgetConfig(int maxAttempts, int budgetMs) {
    Helper::RetryBudget::Config c = Helper::RetryBudget::DefaultConfig();
    c.max_attempts = maxAttempts;
    c.total_wall = ms(budgetMs);
    return c;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvRegionErrorRetryCapTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the gate unset the wrapper drives one
//    pass-through write to regionId; even if a chain is declared,
//    env-off ignores it.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_ERROR_RETRY_CAP", nullptr);
    BOOST_REQUIRE(!TikvRegionErrorRetryCapGated::EnvArmed());

    TikvRegionErrorRetryCapGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    std::atomic<RegionId> lastRegion{0};
    TikvRegionErrorRetryCapGated client(cfg,
        [&](RegionId r, ms) {
            ++calls;
            lastRegion = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithRegionErrorGuard(
            /*regionId=*/77,
            /*regionErrorChain=*/{RegionErrorKind::EpochNotMatch,
                                  RegionErrorKind::NotLeader},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK_EQUAL(out.region_error_obs, 0u);
    BOOST_CHECK_EQUAL(out.region_error_recovered, 0u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after_rec, 0u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastRegion.load(), static_cast<RegionId>(77));

    BOOST_CHECK_EQUAL(client.RegionErrorObserved(),          0u);
    BOOST_CHECK_EQUAL(client.RegionErrorRecovered(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRecovery(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline no region error: env-armed but chain empty ->
//    region is clean, wrapper writes once, no region-error hint
//    observed, only m_writeSucceededAfterRecovery moves.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineNoRegionError)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_ERROR_RETRY_CAP", "1");
    BOOST_REQUIRE(TikvRegionErrorRetryCapGated::EnvArmed());

    TikvRegionErrorRetryCapGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    std::atomic<RegionId> lastRegion{0};
    TikvRegionErrorRetryCapGated client(cfg,
        [&](RegionId r, ms) {
            ++calls;
            lastRegion = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithRegionErrorGuard(
            /*regionId=*/77,
            /*regionErrorChain=*/{},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.region_error_obs, 0u);
    BOOST_CHECK_EQUAL(out.region_error_recovered, 0u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after_rec, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastRegion.load(), static_cast<RegionId>(77));

    BOOST_CHECK_EQUAL(client.RegionErrorObserved(),          0u);
    BOOST_CHECK_EQUAL(client.RegionErrorRecovered(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRecovery(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          0u);
}

// ---------------------------------------------------------------------------
// 3) Single region error recovers. chain={EpochNotMatch} with
//    budget max=2 -> one observed, one recovered, write lands.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SingleRegionErrorRecovers)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_ERROR_RETRY_CAP", "1");

    TikvRegionErrorRetryCapGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = RegionErrorBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    std::atomic<RegionId> lastRegion{0};
    TikvRegionErrorRetryCapGated client(cfg,
        [&](RegionId r, ms) {
            ++calls;
            lastRegion = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithRegionErrorGuard(
            /*regionId=*/77,
            /*regionErrorChain=*/{RegionErrorKind::EpochNotMatch},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.region_error_obs, 1u);
    BOOST_CHECK_EQUAL(out.region_error_recovered, 1u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after_rec, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastRegion.load(), static_cast<RegionId>(77));

    BOOST_CHECK_EQUAL(client.RegionErrorObserved(),          1u);
    BOOST_CHECK_EQUAL(client.RegionErrorRecovered(),         1u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRecovery(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          0u);
}

// ---------------------------------------------------------------------------
// 4) Chained region errors of mixed kind recover. chain=
//    {EpochNotMatch, NotLeader, RegionNotFound, ServerIsBusy}
//    with budget max=6 -> four observed, four recovered, write
//    lands.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ChainedRegionErrorsRecover)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_ERROR_RETRY_CAP", "1");

    TikvRegionErrorRetryCapGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = RegionErrorBudgetConfig(/*maxAttempts=*/6, /*budgetMs=*/1000);

    std::atomic<int> calls{0};
    std::atomic<RegionId> lastRegion{0};
    TikvRegionErrorRetryCapGated client(cfg,
        [&](RegionId r, ms) {
            ++calls;
            lastRegion = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithRegionErrorGuard(
            /*regionId=*/77,
            /*regionErrorChain=*/{RegionErrorKind::EpochNotMatch,
                                  RegionErrorKind::NotLeader,
                                  RegionErrorKind::RegionNotFound,
                                  RegionErrorKind::ServerIsBusy},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.region_error_obs, 4u);
    BOOST_CHECK_EQUAL(out.region_error_recovered, 4u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after_rec, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastRegion.load(), static_cast<RegionId>(77));

    BOOST_CHECK_EQUAL(client.RegionErrorObserved(),          4u);
    BOOST_CHECK_EQUAL(client.RegionErrorRecovered(),         4u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRecovery(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          0u);
}

// ---------------------------------------------------------------------------
// 5) Retry cap exceeded. Deep chain of 5 region errors with
//    budget max_attempts=2 -> wrapper fails fast with
//    ErrorCode::RegionErrorRetryCapExceeded.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RegionErrorRetryCapExceeded)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_ERROR_RETRY_CAP", "1");

    TikvRegionErrorRetryCapGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = RegionErrorBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    TikvRegionErrorRetryCapGated client(cfg,
        [&](RegionId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithRegionErrorGuard(
            /*regionId=*/77,
            /*regionErrorChain=*/{RegionErrorKind::EpochNotMatch,
                                  RegionErrorKind::NotLeader,
                                  RegionErrorKind::RegionNotFound,
                                  RegionErrorKind::KeyNotInRegion,
                                  RegionErrorKind::ServerIsBusy},
            budget);

    BOOST_CHECK(out.code == ErrorCode::RegionErrorRetryCapExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.write_succeeded_after_rec, 0u);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRecovery(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          1u);

    // Wiring smoke: distinct error code.
    BOOST_CHECK(static_cast<int>(ErrorCode::RegionErrorRetryCapExceeded)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::RegionErrorRetryCapExceeded)
                != static_cast<int>(ErrorCode::LeaderTransferRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::RegionErrorRetryCapExceeded)
                != static_cast<int>(ErrorCode::SplitStormRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::RegionErrorRetryCapExceeded)
                != static_cast<int>(ErrorCode::MergeRetryCapExceeded));
}

BOOST_AUTO_TEST_SUITE_END()
