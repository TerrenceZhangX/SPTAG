// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: pd-reconnect-during-retry
//
// Tier 1 HARD repro of the pd-reconnect-during-retry invariants
// (PD-client disconnect-during-retry retry-cap):
//
//   * env-off: wrapper dormant; one pass-through write against
//     the primary PD endpoint, no disconnect counter moves.
//   * env-armed + empty pattern: clean run -> wrapper drives
//     one write on the primary, ++m_writeSucceededAfterReconnect
//     only.
//   * env-armed + single endpoint with N>0 disconnects: N
//     observed, N reconnect attempts, write lands on the
//     primary PD endpoint.
//   * env-armed + never-reconnect primary, healthy secondary:
//     primary contributes one observed/one reconnect then
//     fails over; write lands on the secondary endpoint.
//   * env-armed + every endpoint never reconnects and budget
//     blows: wrapper fails fast with
//     ErrorCode::PdReconnectRetryCapExceeded.
//
// Wrapper under test:  PdReconnectDuringRetryGated
//                      (header-only; defers to
//                      Helper::RetryBudget + injected per-endpoint
//                      write callable).
// Env-gate:            SPTAG_FAULT_PD_RECONNECT_DURING_RETRY
// Tier 2 (1M perf):    DEFERRED-PENDING-GATE -- hot-path
//                      (pd-reconnect-during-retry sits on the
//                      strict 5% hot-path list), but the 1M
//                      perf gate is currently in the noise
//                      floor.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/PdReconnectDuringRetryGated.h"
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
using EndpointId = PdReconnectDuringRetryGated::EndpointId;

Helper::RetryBudget::Config PdReconnectBudgetConfig(int maxAttempts, int budgetMs) {
    Helper::RetryBudget::Config c = Helper::RetryBudget::DefaultConfig();
    c.max_attempts = maxAttempts;
    c.total_wall = ms(budgetMs);
    return c;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(PdReconnectDuringRetryTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the gate unset the wrapper drives one
//    pass-through write against the primary PD endpoint; the
//    reconnect pattern is ignored.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_PD_RECONNECT_DURING_RETRY", nullptr);
    BOOST_REQUIRE(!PdReconnectDuringRetryGated::EnvArmed());

    PdReconnectDuringRetryGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    std::atomic<EndpointId> lastEndpoint{-1};
    PdReconnectDuringRetryGated client(cfg,
        [&](EndpointId e, ms) {
            ++calls;
            lastEndpoint = e;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithPdReconnectGuard(
            /*pdEndpoints=*/{20, 21, 22},
            /*reconnectDelayMsPerAttempt=*/{3, -1, 2},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK_EQUAL(out.pd_disconnect_obs, 0u);
    BOOST_CHECK_EQUAL(out.pd_reconnect_attempted, 0u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 0u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(out.landed_on_endpoint, 20);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastEndpoint.load(), static_cast<EndpointId>(20));

    BOOST_CHECK_EQUAL(client.PdDisconnectObserved(),         0u);
    BOOST_CHECK_EQUAL(client.PdReconnectAttempted(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterReconnect(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline stable PD: env-armed but pattern empty/zero ->
//    primary is clean, wrapper writes once, no disconnect
//    observed, only m_writeSucceededAfterReconnect moves.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineStablePd)
{
    ScopedEnv env("SPTAG_FAULT_PD_RECONNECT_DURING_RETRY", "1");
    BOOST_REQUIRE(PdReconnectDuringRetryGated::EnvArmed());

    PdReconnectDuringRetryGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    std::atomic<EndpointId> lastEndpoint{-1};
    PdReconnectDuringRetryGated client(cfg,
        [&](EndpointId e, ms) {
            ++calls;
            lastEndpoint = e;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithPdReconnectGuard(
            /*pdEndpoints=*/{20, 21, 22},
            /*reconnectDelayMsPerAttempt=*/{0, 0, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.pd_disconnect_obs, 0u);
    BOOST_CHECK_EQUAL(out.pd_reconnect_attempted, 0u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(out.landed_on_endpoint, 20);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastEndpoint.load(), static_cast<EndpointId>(20));

    BOOST_CHECK_EQUAL(client.PdDisconnectObserved(),         0u);
    BOOST_CHECK_EQUAL(client.PdReconnectAttempted(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterReconnect(), 1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          0u);
}

// ---------------------------------------------------------------------------
// 3) Single endpoint recovers after N disconnects.
//    pattern={3,0} with budget max>=4 -> three observed, three
//    reconnect attempts, write lands on the primary.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SingleDisconnectRecovers)
{
    ScopedEnv env("SPTAG_FAULT_PD_RECONNECT_DURING_RETRY", "1");

    PdReconnectDuringRetryGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = PdReconnectBudgetConfig(/*maxAttempts=*/6, /*budgetMs=*/1000);

    std::atomic<int> calls{0};
    std::atomic<EndpointId> lastEndpoint{-1};
    PdReconnectDuringRetryGated client(cfg,
        [&](EndpointId e, ms) {
            ++calls;
            lastEndpoint = e;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithPdReconnectGuard(
            /*pdEndpoints=*/{20, 21},
            /*reconnectDelayMsPerAttempt=*/{3, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.pd_disconnect_obs, 3u);
    BOOST_CHECK_EQUAL(out.pd_reconnect_attempted, 3u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(out.landed_on_endpoint, 20);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastEndpoint.load(), static_cast<EndpointId>(20));

    BOOST_CHECK_EQUAL(client.PdDisconnectObserved(),         3u);
    BOOST_CHECK_EQUAL(client.PdReconnectAttempted(),         3u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterReconnect(), 1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          0u);
}

// ---------------------------------------------------------------------------
// 4) Primary never reconnects, secondary is healthy.
//    pattern={-1, 0} with budget max>=3 -> primary contributes
//    one observed/one reconnect then exhausts; secondary lands.
//    Write lands on endpoint 21.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(MultiDisconnectRecovers)
{
    ScopedEnv env("SPTAG_FAULT_PD_RECONNECT_DURING_RETRY", "1");

    PdReconnectDuringRetryGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = PdReconnectBudgetConfig(/*maxAttempts=*/4, /*budgetMs=*/1000);

    std::atomic<int> calls{0};
    std::atomic<EndpointId> lastEndpoint{-1};
    PdReconnectDuringRetryGated client(cfg,
        [&](EndpointId e, ms) {
            ++calls;
            lastEndpoint = e;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithPdReconnectGuard(
            /*pdEndpoints=*/{20, 21},
            /*reconnectDelayMsPerAttempt=*/{-1, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.pd_disconnect_obs, 1u);
    BOOST_CHECK_EQUAL(out.pd_reconnect_attempted, 1u);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(out.landed_on_endpoint, 21);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastEndpoint.load(), static_cast<EndpointId>(21));

    BOOST_CHECK_EQUAL(client.PdDisconnectObserved(),         1u);
    BOOST_CHECK_EQUAL(client.PdReconnectAttempted(),         1u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterReconnect(), 1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          0u);
}

// ---------------------------------------------------------------------------
// 5) Retry cap exceeded. Deep pattern {5, 5} on two endpoints
//    with budget max_attempts=2 -> wrapper fails fast with
//    ErrorCode::PdReconnectRetryCapExceeded.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RetryBudgetExhausted)
{
    ScopedEnv env("SPTAG_FAULT_PD_RECONNECT_DURING_RETRY", "1");

    PdReconnectDuringRetryGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = PdReconnectBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    PdReconnectDuringRetryGated client(cfg,
        [&](EndpointId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithPdReconnectGuard(
            /*pdEndpoints=*/{20, 21},
            /*reconnectDelayMsPerAttempt=*/{5, 5},
            budget);

    BOOST_CHECK(out.code == ErrorCode::PdReconnectRetryCapExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.write_succeeded_after, 0u);
    BOOST_CHECK_EQUAL(out.landed_on_endpoint, -1);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteSucceededAfterReconnect(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),          1u);

    // Wiring smoke: distinct error code.
    BOOST_CHECK(static_cast<int>(ErrorCode::PdReconnectRetryCapExceeded)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::PdReconnectRetryCapExceeded)
                != static_cast<int>(ErrorCode::GrpcTimeoutStormRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::PdReconnectRetryCapExceeded)
                != static_cast<int>(ErrorCode::RegionErrorRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::PdReconnectRetryCapExceeded)
                != static_cast<int>(ErrorCode::LeaderTransferRetryCapExceeded));
}

BOOST_AUTO_TEST_SUITE_END()
