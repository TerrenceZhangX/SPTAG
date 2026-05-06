// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-store-die-with-replicas
//
// Tier 1 HARD repro of the tikv-store-die-with-replicas invariants
// from design-docs/ft-fault-cases/tikv-store-die-with-replicas.md:
//
//   * env-off: wrapper dormant; primary write succeeds and no
//     counter moves;
//   * env-armed but no Arm() trigger: wrapper still passes through,
//     primary admits the write, no counters move;
//   * env-armed + Arm()ed: primary store death is injected, wrapper
//     fails over to surviving replicas, write is admitted on the
//     alternate within the retry budget;
//   * env-armed + Arm()ed + every replica also dead: wrapper surfaces
//     ErrorCode::StoreUnavailable cleanly via m_clientErrorReturned;
//   * env-armed + Arm()ed + tight RetryBudget: budget exhausts before
//     any alternate accepts -> ErrorCode::StoreUnavailable, no hang.
//
// Wrapper under test:  TikvStoreDieWithReplicasGated (header-only;
//                      defers to Helper::RetryBudget + injected
//                      per-replica write callable).
// Env-gate:            SPTAG_FAULT_TIKV_STORE_DIE_WITH_REPLICAS (off
//                      -> wrapper dormant, counters stay zero on the
//                      happy path).
// Tier 2 (1M perf):    DEFERRED -- non hot-path (the gated replica-
//                      failover wrapper sits outside the SPANN search
//                      inner loop and outside the per-RPC TiKV retry
//                      hot path; storage-failover semantics).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvStoreDieWithReplicasGated.h"
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

BOOST_AUTO_TEST_SUITE(TikvStoreDieWithReplicasTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With SPTAG_FAULT_TIKV_STORE_DIE_WITH_REPLICAS
//    unset, Arm() is irrelevant: the wrapper drives the per-replica
//    callable in order, accepts the primary's Success, and keeps every
//    counter at zero. Protects baseline perf and keeps the wrapper
//    invisible in production builds.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_DIE_WITH_REPLICAS", nullptr);
    BOOST_REQUIRE(!TikvStoreDieWithReplicasGated::EnvArmed());

    TikvStoreDieWithReplicasGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::vector<std::string> seen;
    std::mutex seenMu;
    TikvStoreDieWithReplicasGated client(cfg,
        [&](const std::string& replica, std::chrono::milliseconds) {
            std::lock_guard<std::mutex> lk(seenMu);
            seen.push_back(replica);
            return ErrorCode::Success;
        });

    // Even Arm()ed, env-off must stay pass-through.
    client.Arm();
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteToReplicaSet({"S0", "S1", "S2"}, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK(!out.failedOver);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_REQUIRE_EQUAL(seen.size(), 1u);
    BOOST_CHECK_EQUAL(seen[0], std::string("S0"));
    BOOST_CHECK_EQUAL(client.StoreDied(),               0u);
    BOOST_CHECK_EQUAL(client.ReplicaFailover(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline healthy cluster. Env-armed but no Arm() trigger fired:
//    wrapper passes through, primary admits the write, no counters
//    move, no alternates touched.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineNoStoreDeath)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_DIE_WITH_REPLICAS", "1");
    BOOST_REQUIRE(TikvStoreDieWithReplicasGated::EnvArmed());

    TikvStoreDieWithReplicasGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(100);

    std::atomic<int> calls{0};
    TikvStoreDieWithReplicasGated client(cfg,
        [&](const std::string& replica, std::chrono::milliseconds) {
            ++calls;
            BOOST_CHECK_EQUAL(replica, std::string("S0"));
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteToReplicaSet({"S0", "S1", "S2"}, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK(!out.failedOver);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(client.StoreDied(),               0u);
    BOOST_CHECK_EQUAL(client.ReplicaFailover(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
}

// ---------------------------------------------------------------------------
// 3) Primary store dies; alternate replica accepts the write within
//    the retry budget. Asserts:
//      * code == Success;
//      * m_storeDied == 1 (exactly one synthesised store death);
//      * m_replicaFailover == 1 (alternate took over);
//      * m_writeSucceededOnReplica == 1 (write durable on alternate);
//      * m_clientErrorReturned == 0;
//      * served_by == first surviving alternate;
//      * primary's inner callable NEVER fires (synthetic death);
//      * wall stays well within budget.total_wall.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(StoreDiesReplicaTakesOver)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_DIE_WITH_REPLICAS", "1");

    TikvStoreDieWithReplicasGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(100);
    cfg.budget.total_wall   = std::chrono::milliseconds(500);
    cfg.budget.max_attempts = 6;

    std::vector<std::string> seen;
    std::mutex seenMu;
    TikvStoreDieWithReplicasGated client(cfg,
        [&](const std::string& replica, std::chrono::milliseconds) {
            {
                std::lock_guard<std::mutex> lk(seenMu);
                seen.push_back(replica);
            }
            // The primary is dead synthetically; we should only see
            // the alternates here.
            BOOST_CHECK(replica != std::string("S0"));
            return ErrorCode::Success;
        });

    client.Arm();
    Helper::RetryBudget budget(cfg.budget);
    auto t0 = std::chrono::steady_clock::now();
    auto out = client.WriteToReplicaSet({"S0", "S1", "S2"}, budget);
    auto wall = std::chrono::steady_clock::now() - t0;

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(out.failedOver);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S1"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_REQUIRE_EQUAL(seen.size(), 1u);
    BOOST_CHECK_EQUAL(seen[0], std::string("S1"));

    BOOST_CHECK_EQUAL(client.StoreDied(),               1u);
    BOOST_CHECK_EQUAL(client.ReplicaFailover(),         1u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
    BOOST_CHECK(!client.IsArmed());
    // No infinite hang.
    BOOST_CHECK_LT(std::chrono::duration_cast<std::chrono::milliseconds>(
                       wall).count(),
                   cfg.budget.total_wall.count() + 200);
}

// ---------------------------------------------------------------------------
// 4) Primary store dies AND every surviving replica also rejects the
//    write (e.g. concurrent network blip across the whole region).
//    Wrapper must surface ErrorCode::StoreUnavailable cleanly via
//    m_clientErrorReturned -- never a hang, never a silent drop.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(AllReplicasDieClientErrorReturned)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_DIE_WITH_REPLICAS", "1");

    TikvStoreDieWithReplicasGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);
    cfg.budget.total_wall   = std::chrono::milliseconds(2000);
    cfg.budget.max_attempts = 8;
    cfg.budget.base_backoff = std::chrono::milliseconds(1);
    cfg.budget.cap_backoff  = std::chrono::milliseconds(5);

    std::atomic<int> calls{0};
    TikvStoreDieWithReplicasGated client(cfg,
        [&](const std::string&, std::chrono::milliseconds) {
            ++calls;
            return ErrorCode::StoreUnavailable;
        });

    client.Arm();
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteToReplicaSet({"S0", "S1", "S2"}, budget);

    BOOST_CHECK(out.code == ErrorCode::StoreUnavailable);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.failedOver);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    // Both alternates should have been attempted (S0 was synthetic).
    BOOST_CHECK_EQUAL(out.attempted_replicas, 2);
    BOOST_CHECK_EQUAL(calls.load(), 2);

    BOOST_CHECK_EQUAL(client.StoreDied(),               1u);
    BOOST_CHECK_EQUAL(client.ReplicaFailover(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     1u);
}

// ---------------------------------------------------------------------------
// 5) Tight RetryBudget exhaustion. Primary dies (synthetic); each
//    alternate is "still warming up" and rejects -- the budget is so
//    tight that we run out of attempts before reaching the last
//    alternate. Must surface ErrorCode::StoreUnavailable via
//    m_clientErrorReturned and not hang.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RetryBudgetExhausted)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_STORE_DIE_WITH_REPLICAS", "1");

    TikvStoreDieWithReplicasGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(20);
    // Budget cap of 2 attempts: the synthesised primary death consumes
    // one, then exactly one alternate gets to try, then exhausted.
    cfg.budget.total_wall   = std::chrono::milliseconds(500);
    cfg.budget.max_attempts = 2;
    cfg.budget.base_backoff = std::chrono::milliseconds(1);
    cfg.budget.cap_backoff  = std::chrono::milliseconds(2);

    std::atomic<int> calls{0};
    TikvStoreDieWithReplicasGated client(cfg,
        [&](const std::string&, std::chrono::milliseconds) {
            ++calls;
            return ErrorCode::StoreUnavailable;
        });

    client.Arm();
    Helper::RetryBudget budget(cfg.budget);
    auto t0 = std::chrono::steady_clock::now();
    auto out = client.WriteToReplicaSet({"S0", "S1", "S2", "S3"}, budget);
    auto wall = std::chrono::steady_clock::now() - t0;

    BOOST_CHECK(out.code == ErrorCode::StoreUnavailable);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.failedOver);
    // With max_attempts=2 and one consumed by the synthetic primary
    // death, only one alternate is invoked before the budget bails.
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(client.StoreDied(),               1u);
    BOOST_CHECK_EQUAL(client.ReplicaFailover(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     1u);
    // No infinite hang.
    BOOST_CHECK_LT(std::chrono::duration_cast<std::chrono::milliseconds>(
                       wall).count(),
                   cfg.budget.total_wall.count() + 200);

    // Wiring smoke: ErrorCode::StoreUnavailable is well-formed and
    // distinct from RetryBudgetExceeded and Success.
    BOOST_CHECK(static_cast<int>(ErrorCode::StoreUnavailable)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::StoreUnavailable)
                != static_cast<int>(ErrorCode::RetryBudgetExceeded));
}

BOOST_AUTO_TEST_SUITE_END()
