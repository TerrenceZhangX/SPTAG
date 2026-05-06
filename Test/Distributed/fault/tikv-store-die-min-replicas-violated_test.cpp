// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-store-die-min-replicas-violated
//
// Tier 1 HARD repro of the tikv-store-die-min-replicas-violated
// invariants from
// design-docs/ft-fault-cases/tikv-store-die-min-replicas-violated.md:
//
//   * env-off: wrapper dormant; primary write succeeds and no
//     counter moves;
//   * env-armed + Arm(deadStores) with live_count >= minReplicas:
//     surviving live replicas accept the write, durability invariant
//     is satisfied, no MinReplicasViolated;
//   * env-armed + Arm(deadStores) with live_count < minReplicas:
//     wrapper REJECTS the write before any surviving store is touched
//     and returns ErrorCode::MinReplicasViolated -- m_writeRejected,
//     m_minReplicasViolated, m_clientErrorReturned all bump;
//   * edge: deadStores == replicas.size() -> live_count == 0 -> still
//     MinReplicasViolated, m_storeDied == N.
//
// Wrapper under test:  TikvStoreDieMinReplicasViolatedGated
//                      (header-only; defers to Helper::RetryBudget +
//                      injected per-replica write callable).
// Env-gate:            SPTAG_FAULT_TIKV_STORE_DIE_MIN_REPLICAS_VIOLATED
// Tier 2 (1M perf):    DEFERRED -- non hot-path (durability-floor
//                      pre-check sits outside the SPANN search inner
//                      loop and outside the per-RPC TiKV retry hot
//                      path).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvStoreDieMinReplicasViolatedGated.h"
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

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvStoreDieMinReplicasViolatedTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the env-gate unset the wrapper is dormant:
//    Arm(...) is irrelevant, the wrapper drives the per-replica
//    callable in order, accepts the primary's Success, and keeps every
//    counter at zero.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_DIE_MIN_REPLICAS_VIOLATED", nullptr);
    BOOST_REQUIRE(!TikvStoreDieMinReplicasViolatedGated::EnvArmed());

    TikvStoreDieMinReplicasViolatedGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::vector<std::string> seen;
    std::mutex seenMu;
    TikvStoreDieMinReplicasViolatedGated client(cfg,
        [&](const std::string& replica, std::chrono::milliseconds) {
            std::lock_guard<std::mutex> lk(seenMu);
            seen.push_back(replica);
            return ErrorCode::Success;
        });

    // Even Arm()ed, env-off must stay pass-through.
    client.Arm(2);
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteToReplicaSet({"S0", "S1", "S2"}, 2, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK(!out.rejected);
    BOOST_CHECK_EQUAL(out.dead_count, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_REQUIRE_EQUAL(seen.size(), 1u);
    BOOST_CHECK_EQUAL(seen[0], std::string("S0"));
    BOOST_CHECK_EQUAL(client.StoreDied(),            0u);
    BOOST_CHECK_EQUAL(client.MinReplicasViolated(),  0u);
    BOOST_CHECK_EQUAL(client.WriteRejected(),        0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline min-replicas OK. Env-armed + Arm(1) but
//    replicas.size()-1 >= minReplicas, so durability is preserved:
//    one synthetic store death fires (m_storeDied == 1) but the write
//    is admitted on a surviving live replica, no MinReplicasViolated.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineMinReplicasOk)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_DIE_MIN_REPLICAS_VIOLATED", "1");
    BOOST_REQUIRE(TikvStoreDieMinReplicasViolatedGated::EnvArmed());

    TikvStoreDieMinReplicasViolatedGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(100);
    cfg.budget.total_wall   = std::chrono::milliseconds(500);
    cfg.budget.max_attempts = 6;

    std::atomic<int> calls{0};
    TikvStoreDieMinReplicasViolatedGated client(cfg,
        [&](const std::string& replica, std::chrono::milliseconds) {
            ++calls;
            // Primary is dead synthetically; only S1/S2 should be hit.
            BOOST_CHECK(replica != std::string("S0"));
            return ErrorCode::Success;
        });

    // 3 replicas, 1 dead, minReplicas=2: live_count=2 >= 2 -> OK.
    client.Arm(1);
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteToReplicaSet({"S0", "S1", "S2"}, 2, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.rejected);
    BOOST_CHECK_EQUAL(out.dead_count, 1u);
    BOOST_CHECK_EQUAL(out.live_count, 2u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S1"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.StoreDied(),            1u);
    BOOST_CHECK_EQUAL(client.MinReplicasViolated(),  0u);
    BOOST_CHECK_EQUAL(client.WriteRejected(),        0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);
    BOOST_CHECK(!client.IsArmed());
}

// ---------------------------------------------------------------------------
// 3) Min-replicas violated rejects the write. Env-armed + Arm(2) on a
//    3-replica region with minReplicas=2 yields live_count=1 < 2 ->
//    wrapper REJECTS the write before any surviving store is touched.
//    Returns ErrorCode::MinReplicasViolated; m_writeRejected,
//    m_minReplicasViolated, m_clientErrorReturned all == 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(MinReplicasViolatedRejects)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_DIE_MIN_REPLICAS_VIOLATED", "1");

    TikvStoreDieMinReplicasViolatedGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::atomic<int> calls{0};
    TikvStoreDieMinReplicasViolatedGated client(cfg,
        [&](const std::string&, std::chrono::milliseconds) {
            ++calls;
            return ErrorCode::Success;
        });

    client.Arm(2);
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteToReplicaSet({"S0", "S1", "S2"}, 2, budget);

    BOOST_CHECK(out.code == ErrorCode::MinReplicasViolated);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(out.rejected);
    BOOST_CHECK_EQUAL(out.dead_count, 2u);
    BOOST_CHECK_EQUAL(out.live_count, 1u);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    // The surviving live replica must NOT be touched -- durability
    // pre-check rejects before any RPC.
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.StoreDied(),            2u);
    BOOST_CHECK_EQUAL(client.MinReplicasViolated(),  1u);
    BOOST_CHECK_EQUAL(client.WriteRejected(),        1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  1u);
    BOOST_CHECK(!client.IsArmed());

    // Wiring smoke: MinReplicasViolated is well-formed and distinct
    // from StoreUnavailable / RetryBudgetExceeded / Success.
    BOOST_CHECK(static_cast<int>(ErrorCode::MinReplicasViolated)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::MinReplicasViolated)
                != static_cast<int>(ErrorCode::StoreUnavailable));
    BOOST_CHECK(static_cast<int>(ErrorCode::MinReplicasViolated)
                != static_cast<int>(ErrorCode::RetryBudgetExceeded));
}

// ---------------------------------------------------------------------------
// 4) Edge: every replica dies. deadStores == replicas.size() ->
//    live_count == 0 -> still MinReplicasViolated, m_storeDied == N.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EdgeAllDie)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_DIE_MIN_REPLICAS_VIOLATED", "1");

    TikvStoreDieMinReplicasViolatedGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::atomic<int> calls{0};
    TikvStoreDieMinReplicasViolatedGated client(cfg,
        [&](const std::string&, std::chrono::milliseconds) {
            ++calls;
            return ErrorCode::Success;
        });

    const std::size_t kN = 3;
    client.Arm(kN);
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteToReplicaSet({"S0", "S1", "S2"}, 2, budget);

    BOOST_CHECK(out.code == ErrorCode::MinReplicasViolated);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(out.rejected);
    BOOST_CHECK_EQUAL(out.dead_count, kN);
    BOOST_CHECK_EQUAL(out.live_count, 0u);
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.StoreDied(),            static_cast<std::uint64_t>(kN));
    BOOST_CHECK_EQUAL(client.MinReplicasViolated(),  1u);
    BOOST_CHECK_EQUAL(client.WriteRejected(),        1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  1u);
}

BOOST_AUTO_TEST_SUITE_END()
