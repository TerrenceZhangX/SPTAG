// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-store-restart-with-stale-data
//
// Tier 1 HARD repro of the tikv-store-restart-with-stale-data
// invariants from
// design-docs/ft-fault-cases/tikv-store-restart-with-stale-data.md:
//
//   * env-off: wrapper dormant; primary read succeeds, no counter
//     moves;
//   * env-armed + no Arm(): no restart synthesised, primary read
//     succeeds, no stale detection, no counter moves;
//   * env-armed + Arm(restartedHead) with at least one fresh peer:
//     stale view is detected on the restarted head, the wrapper
//     falls through to a non-restarted replica, returns Success;
//     m_storeRestarted, m_staleViewDetected, m_freshFetchTriggered
//     all bump and the served replica is the first fresh peer;
//   * env-armed + Arm(N) with N == replicas.size(): every replica
//     stale -> wrapper returns ErrorCode::StaleStoreView,
//     m_clientErrorReturned == 1, m_freshFetchTriggered == 0.
//
// Wrapper under test:  TikvStoreRestartWithStaleDataGated
//                      (header-only; defers to Helper::RetryBudget +
//                      injected per-replica read callable).
// Env-gate:            SPTAG_FAULT_TIKV_STORE_RESTART_WITH_STALE_DATA
// Tier 2 (1M perf):    DEFERRED -- non hot-path (region-epoch stale
//                      check sits on the post-restart recovery edge,
//                      not the SPANN search inner loop or the
//                      per-RPC TiKV retry hot path).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvStoreRestartWithStaleDataGated.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <mutex>
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

constexpr std::uint64_t kExpectedEpoch = 42;

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvStoreRestartWithStaleDataTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the env-gate unset the wrapper is dormant:
//    Arm(...) is irrelevant, the wrapper drives the per-replica read
//    in order, accepts the primary's Success, keeps every counter
//    at zero.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_RESTART_WITH_STALE_DATA", nullptr);
    BOOST_REQUIRE(!TikvStoreRestartWithStaleDataGated::EnvArmed());

    TikvStoreRestartWithStaleDataGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::vector<std::string> seen;
    std::mutex seenMu;
    TikvStoreRestartWithStaleDataGated client(cfg,
        [&](const std::string& replica, std::chrono::milliseconds) {
            std::lock_guard<std::mutex> lk(seenMu);
            seen.push_back(replica);
            return ErrorCode::Success;
        });

    // Even Arm()ed, env-off must stay pass-through.
    client.Arm(2);
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.ReadFromStore({"S0", "S1", "S2"}, kExpectedEpoch, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK(!out.fresh_fetched);
    BOOST_CHECK_EQUAL(out.restarted_count, 0u);
    BOOST_CHECK_EQUAL(out.stale_seen, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_REQUIRE_EQUAL(seen.size(), 1u);
    BOOST_CHECK_EQUAL(seen[0], std::string("S0"));
    BOOST_CHECK_EQUAL(client.StoreRestarted(),       0u);
    BOOST_CHECK_EQUAL(client.StaleViewDetected(),    0u);
    BOOST_CHECK_EQUAL(client.FreshFetchTriggered(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline no-restart. Env-armed but never Arm()ed: wrapper sees
//    no synthetic restart, primary read succeeds, no stale detection,
//    counters stay at zero.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineNoRestart)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_RESTART_WITH_STALE_DATA", "1");
    BOOST_REQUIRE(TikvStoreRestartWithStaleDataGated::EnvArmed());

    TikvStoreRestartWithStaleDataGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::atomic<int> calls{0};
    TikvStoreRestartWithStaleDataGated client(cfg,
        [&](const std::string& replica, std::chrono::milliseconds) {
            ++calls;
            BOOST_CHECK_EQUAL(replica, std::string("S0"));
            return ErrorCode::Success;
        });

    BOOST_REQUIRE(!client.IsArmed());
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.ReadFromStore({"S0", "S1", "S2"}, kExpectedEpoch, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK(!out.fresh_fetched);
    BOOST_CHECK_EQUAL(out.restarted_count, 0u);
    BOOST_CHECK_EQUAL(out.stale_seen, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.StoreRestarted(),       0u);
    BOOST_CHECK_EQUAL(client.StaleViewDetected(),    0u);
    BOOST_CHECK_EQUAL(client.FreshFetchTriggered(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);
}

// ---------------------------------------------------------------------------
// 3) Restart with fresh replica. Env-armed + Arm(1) on a 3-replica
//    region: primary returns from a stale snapshot, wrapper detects
//    it at read time, falls through to the next non-restarted
//    replica which succeeds. m_storeRestarted == 1,
//    m_staleViewDetected == 1, m_freshFetchTriggered == 1, no client
//    error.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RestartWithFreshReplica)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_RESTART_WITH_STALE_DATA", "1");

    TikvStoreRestartWithStaleDataGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(100);
    cfg.budget.total_wall   = std::chrono::milliseconds(500);
    cfg.budget.max_attempts = 6;

    std::atomic<int> calls{0};
    TikvStoreRestartWithStaleDataGated client(cfg,
        [&](const std::string& replica, std::chrono::milliseconds) {
            ++calls;
            // Primary is restarted-stale synthetically; only S1/S2
            // should be hit by the inner read.
            BOOST_CHECK(replica != std::string("S0"));
            return ErrorCode::Success;
        });

    client.Arm(1);
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.ReadFromStore({"S0", "S1", "S2"}, kExpectedEpoch, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(out.fresh_fetched);
    BOOST_CHECK_EQUAL(out.restarted_count, 1u);
    BOOST_CHECK_EQUAL(out.stale_seen, 1u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S1"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.StoreRestarted(),       1u);
    BOOST_CHECK_EQUAL(client.StaleViewDetected(),    1u);
    BOOST_CHECK_EQUAL(client.FreshFetchTriggered(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);
    BOOST_CHECK(!client.IsArmed());
}

// ---------------------------------------------------------------------------
// 4) All replicas stale. Arm(N) where N == replicas.size(): every
//    replica restarted with a stale view -> wrapper returns
//    ErrorCode::StaleStoreView; m_freshFetchTriggered stays 0
//    (nothing to fall through to), m_clientErrorReturned == 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(AllReplicasStale)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_RESTART_WITH_STALE_DATA", "1");

    TikvStoreRestartWithStaleDataGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::atomic<int> calls{0};
    TikvStoreRestartWithStaleDataGated client(cfg,
        [&](const std::string&, std::chrono::milliseconds) {
            ++calls;
            return ErrorCode::Success;
        });

    const std::size_t kN = 3;
    client.Arm(kN);
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.ReadFromStore({"S0", "S1", "S2"}, kExpectedEpoch, budget);

    BOOST_CHECK(out.code == ErrorCode::StaleStoreView);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.fresh_fetched);
    BOOST_CHECK_EQUAL(out.restarted_count, kN);
    BOOST_CHECK_EQUAL(out.stale_seen, kN);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    // No fresh peer remained -> inner read must NOT fire.
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.StoreRestarted(),       static_cast<std::uint64_t>(kN));
    BOOST_CHECK_EQUAL(client.StaleViewDetected(),    static_cast<std::uint64_t>(kN));
    BOOST_CHECK_EQUAL(client.FreshFetchTriggered(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  1u);

    // Wiring smoke: StaleStoreView is well-formed and distinct from
    // its sibling client-visible TiKV error codes.
    BOOST_CHECK(static_cast<int>(ErrorCode::StaleStoreView)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::StaleStoreView)
                != static_cast<int>(ErrorCode::StoreUnavailable));
    BOOST_CHECK(static_cast<int>(ErrorCode::StaleStoreView)
                != static_cast<int>(ErrorCode::MinReplicasViolated));
    BOOST_CHECK(static_cast<int>(ErrorCode::StaleStoreView)
                != static_cast<int>(ErrorCode::RetryBudgetExceeded));
}

BOOST_AUTO_TEST_SUITE_END()
