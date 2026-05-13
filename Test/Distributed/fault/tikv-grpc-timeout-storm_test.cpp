// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-grpc-timeout-storm
//
// Tier 1 HARD repro of the tikv-grpc-timeout-storm invariants
// (cross-replica gRPC-timeout retry-cap):
//
//   * env-off: wrapper dormant; one pass-through write against
//     the primary replica, no timeout counter moves.
//   * env-armed + empty pattern: clean run -> wrapper drives
//     one write on the primary, ++m_writeSucceededAfterRetry
//     only.
//   * env-armed + single replica with N>0 timeouts: N
//     observed, N retries with backoff, write lands on the
//     primary.
//   * env-armed + always-times-out primary, healthy secondary:
//     primary contributes one observed/one retry then fails
//     over; write lands on secondary.
//   * env-armed + all replicas always time out and budget
//     blows: wrapper fails fast with
//     ErrorCode::GrpcTimeoutStormRetryCapExceeded.
//
// Wrapper under test:  TikvGrpcTimeoutStormGated
//                      (header-only; defers to
//                      Helper::RetryBudget + injected per-replica
//                      write callable).
// Env-gate:            SPTAG_FAULT_TIKV_GRPC_TIMEOUT_STORM
// Tier 2 (1M perf):    DEFERRED-PENDING-GATE -- hot-path
//                      (tikv-grpc-timeout-storm sits on the
//                      strict 5% hot-path list), but the 1M
//                      perf gate is currently in the noise
//                      floor.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvGrpcTimeoutStormGated.h"
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
using ReplicaId = TikvGrpcTimeoutStormGated::ReplicaId;

Helper::RetryBudget::Config GrpcTimeoutBudgetConfig(int maxAttempts, int budgetMs) {
    Helper::RetryBudget::Config c = Helper::RetryBudget::DefaultConfig();
    c.max_attempts = maxAttempts;
    c.total_wall = ms(budgetMs);
    return c;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvGrpcTimeoutStormTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the gate unset the wrapper drives one
//    pass-through write against the primary replica; the timeout
//    pattern is ignored.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_GRPC_TIMEOUT_STORM", nullptr);
    BOOST_REQUIRE(!TikvGrpcTimeoutStormGated::EnvArmed());

    TikvGrpcTimeoutStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    std::atomic<ReplicaId> lastReplica{-1};
    TikvGrpcTimeoutStormGated client(cfg,
        [&](ReplicaId r, ms) {
            ++calls;
            lastReplica = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithGrpcTimeoutGuard(
            /*replicas=*/{10, 11, 12},
            /*timeoutPatternPerReplica=*/{3, -1, 2},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK_EQUAL(out.grpc_timeout_obs, 0u);
    BOOST_CHECK_EQUAL(out.retry_with_backoff, 0u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 0u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(out.landed_on_replica, 10);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastReplica.load(), static_cast<ReplicaId>(10));

    BOOST_CHECK_EQUAL(client.GrpcTimeoutObserved(),       0u);
    BOOST_CHECK_EQUAL(client.RetryWithBackoff(),          0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRetry(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline no timeouts: env-armed but pattern empty/zero ->
//    primary is clean, wrapper writes once, no timeout observed,
//    only m_writeSucceededAfterRetry moves.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineNoTimeouts)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_GRPC_TIMEOUT_STORM", "1");
    BOOST_REQUIRE(TikvGrpcTimeoutStormGated::EnvArmed());

    TikvGrpcTimeoutStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    std::atomic<ReplicaId> lastReplica{-1};
    TikvGrpcTimeoutStormGated client(cfg,
        [&](ReplicaId r, ms) {
            ++calls;
            lastReplica = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithGrpcTimeoutGuard(
            /*replicas=*/{10, 11, 12},
            /*timeoutPatternPerReplica=*/{0, 0, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.grpc_timeout_obs, 0u);
    BOOST_CHECK_EQUAL(out.retry_with_backoff, 0u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(out.landed_on_replica, 10);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastReplica.load(), static_cast<ReplicaId>(10));

    BOOST_CHECK_EQUAL(client.GrpcTimeoutObserved(),       0u);
    BOOST_CHECK_EQUAL(client.RetryWithBackoff(),          0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRetry(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       0u);
}

// ---------------------------------------------------------------------------
// 3) Single replica recovers after N timeouts. pattern={3,-,-}
//    with budget max>=4 -> three observed, three retries with
//    backoff, write lands on the primary.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SingleReplicaTimeoutRecovers)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_GRPC_TIMEOUT_STORM", "1");

    TikvGrpcTimeoutStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = GrpcTimeoutBudgetConfig(/*maxAttempts=*/6, /*budgetMs=*/1000);

    std::atomic<int> calls{0};
    std::atomic<ReplicaId> lastReplica{-1};
    TikvGrpcTimeoutStormGated client(cfg,
        [&](ReplicaId r, ms) {
            ++calls;
            lastReplica = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithGrpcTimeoutGuard(
            /*replicas=*/{10, 11},
            /*timeoutPatternPerReplica=*/{3, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.grpc_timeout_obs, 3u);
    BOOST_CHECK_EQUAL(out.retry_with_backoff, 3u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(out.landed_on_replica, 10);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastReplica.load(), static_cast<ReplicaId>(10));

    BOOST_CHECK_EQUAL(client.GrpcTimeoutObserved(),       3u);
    BOOST_CHECK_EQUAL(client.RetryWithBackoff(),          3u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRetry(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       0u);
}

// ---------------------------------------------------------------------------
// 4) Primary always times out, secondary is healthy. pattern=
//    {-1, 0} with budget max>=3 -> primary contributes one
//    observed/one retry then exhausts; secondary lands. Write
//    lands on replica 11.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(AllReplicasTimeout)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_GRPC_TIMEOUT_STORM", "1");

    TikvGrpcTimeoutStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = GrpcTimeoutBudgetConfig(/*maxAttempts=*/4, /*budgetMs=*/1000);

    std::atomic<int> calls{0};
    std::atomic<ReplicaId> lastReplica{-1};
    TikvGrpcTimeoutStormGated client(cfg,
        [&](ReplicaId r, ms) {
            ++calls;
            lastReplica = r;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithGrpcTimeoutGuard(
            /*replicas=*/{10, 11},
            /*timeoutPatternPerReplica=*/{-1, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.grpc_timeout_obs, 1u);
    BOOST_CHECK_EQUAL(out.retry_with_backoff, 1u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(out.landed_on_replica, 11);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastReplica.load(), static_cast<ReplicaId>(11));

    BOOST_CHECK_EQUAL(client.GrpcTimeoutObserved(),       1u);
    BOOST_CHECK_EQUAL(client.RetryWithBackoff(),          1u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRetry(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       0u);
}

// ---------------------------------------------------------------------------
// 5) Retry cap exceeded. Deep pattern {5, 5} on two replicas
//    with budget max_attempts=2 -> wrapper fails fast with
//    ErrorCode::GrpcTimeoutStormRetryCapExceeded.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RetryBudgetExhausted)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_GRPC_TIMEOUT_STORM", "1");

    TikvGrpcTimeoutStormGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = GrpcTimeoutBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    TikvGrpcTimeoutStormGated client(cfg,
        [&](ReplicaId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithGrpcTimeoutGuard(
            /*replicas=*/{10, 11},
            /*timeoutPatternPerReplica=*/{5, 5},
            budget);

    BOOST_CHECK(out.code == ErrorCode::GrpcTimeoutStormRetryCapExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 0u);
    BOOST_CHECK_EQUAL(out.landed_on_replica, -1);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteSucceededAfterRetry(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       1u);

    // Wiring smoke: distinct error code.
    BOOST_CHECK(static_cast<int>(ErrorCode::GrpcTimeoutStormRetryCapExceeded)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::GrpcTimeoutStormRetryCapExceeded)
                != static_cast<int>(ErrorCode::RegionErrorRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::GrpcTimeoutStormRetryCapExceeded)
                != static_cast<int>(ErrorCode::LeaderTransferRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::GrpcTimeoutStormRetryCapExceeded)
                != static_cast<int>(ErrorCode::SplitStormRetryCapExceeded));
}

BOOST_AUTO_TEST_SUITE_END()
