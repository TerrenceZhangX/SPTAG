// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: pd-leader-failover
//
// Tier 1 HARD repro of the pd-leader-failover invariants from
// design-docs/ft-fault-cases/pd-leader-failover.md:
//
//   * env-off: wrapper is dormant; baseline PD calls succeed and no
//     counter moves;
//   * env-armed but no Arm() trigger: wrapper still passes through
//     the PD call (no spurious counters);
//   * env-armed + Arm()ed: leader stepdown is injected, leader is
//     re-resolved via the discovery surface, retry against the fresh
//     leader succeeds within the retry budget, region-cache TTL is
//     refreshed;
//   * sequential armings: counters grow monotonically; no leaks /
//     no double-injection per single Arm().
//
// Wrapper under test:  PdLeaderFailoverGated (header-only; defers to
//                      Helper::RetryBudget + injected resolver +
//                      injected region-cache refresher).
// Env-gate:            SPTAG_FAULT_PD_LEADER_FAILOVER (off ->
//                      wrapper dormant, counters stay zero).
// Tier 2 (1M perf):    DEFERRED -- non hot-path (the gated PD path
//                      sits outside the SPANN search inner loop and
//                      outside the per-RPC TiKV retry hot path).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/PdLeaderFailoverGated.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
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

BOOST_AUTO_TEST_SUITE(PdLeaderFailoverTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With SPTAG_FAULT_PD_LEADER_FAILOVER unset,
//    Arm() is irrelevant: the wrapper forwards the inner RPC against
//    the current leader unchanged, never re-resolves, never refreshes
//    region cache, and keeps all four counters at zero. This protects
//    baseline perf and keeps the wrapper invisible in production
//    builds.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_PD_LEADER_FAILOVER", nullptr);
    BOOST_REQUIRE(!PdLeaderFailoverGated::EnvArmed());

    PdLeaderFailoverGated::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(50);

    std::atomic<int> rpcCalls{0};
    std::atomic<int> resolves{0};
    std::atomic<int> refreshes{0};
    PdLeaderFailoverGated client(cfg,
        [&](const std::string& leader, std::chrono::milliseconds) {
            ++rpcCalls;
            BOOST_CHECK_EQUAL(leader, std::string("pd-1"));
            return ErrorCode::Success;
        },
        [&]() { ++resolves; return std::string("pd-2"); },
        [&]() { ++refreshes; },
        /*initial_leader=*/"pd-1");

    // Even Arm()ed, env-off must stay pass-through.
    client.Arm();
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.Call(budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK(!out.reconnected);
    BOOST_CHECK(!out.refreshed);
    BOOST_CHECK_EQUAL(out.served_by, std::string("pd-1"));
    BOOST_CHECK_EQUAL(rpcCalls.load(), 1);
    BOOST_CHECK_EQUAL(resolves.load(), 0);
    BOOST_CHECK_EQUAL(refreshes.load(), 0);
    BOOST_CHECK_EQUAL(client.PdLeaderStepdown(),     0u);
    BOOST_CHECK_EQUAL(client.PdReconnected(),        0u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);
    BOOST_CHECK_EQUAL(client.CurrentLeader(), std::string("pd-1"));
}

// ---------------------------------------------------------------------------
// 2) Baseline healthy PD. Env-armed but no Arm() trigger fired:
//    wrapper passes through the inner RPC against the current leader,
//    no counters move, no resolver/refresher callbacks fire.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineHealthyPd)
{
    ScopedEnv env("SPTAG_FAULT_PD_LEADER_FAILOVER", "1");
    BOOST_REQUIRE(PdLeaderFailoverGated::EnvArmed());

    PdLeaderFailoverGated::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(100);

    std::atomic<int> rpcCalls{0};
    std::atomic<int> resolves{0};
    std::atomic<int> refreshes{0};
    PdLeaderFailoverGated client(cfg,
        [&](const std::string& leader, std::chrono::milliseconds) {
            ++rpcCalls;
            BOOST_CHECK_EQUAL(leader, std::string("pd-A"));
            return ErrorCode::Success;
        },
        [&]() { ++resolves; return std::string("pd-B"); },
        [&]() { ++refreshes; },
        /*initial_leader=*/"pd-A");

    // No Arm() -> baseline pass-through even when env-armed.
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.Call(budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK(!out.reconnected);
    BOOST_CHECK(!out.refreshed);
    BOOST_CHECK_EQUAL(out.served_by, std::string("pd-A"));
    BOOST_CHECK_EQUAL(rpcCalls.load(), 1);
    BOOST_CHECK_EQUAL(resolves.load(), 0);
    BOOST_CHECK_EQUAL(refreshes.load(), 0);
    BOOST_CHECK_EQUAL(client.PdLeaderStepdown(),     0u);
    BOOST_CHECK_EQUAL(client.PdReconnected(),        0u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);
}

// ---------------------------------------------------------------------------
// 3) Leader steps down -> client transparently reconnects to a fresh
//    leader within the retry budget; query succeeds. Asserts:
//      * code == Success;
//      * m_pdLeaderStepdown == 1 (we injected exactly one stepdown);
//      * m_pdReconnected == 1 (retry against fresh leader succeeded);
//      * m_clientErrorReturned == 0 (client never observed an error);
//      * served_by == fresh leader address;
//      * the "in-flight Unavailable" surfaced in well under the
//        budget total_wall (no infinite hang).
//    This is the case-md "client transparently re-resolves leader,
//    retries against the fresh leader" invariant.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(LeaderStepdownReconnects)
{
    ScopedEnv env("SPTAG_FAULT_PD_LEADER_FAILOVER", "1");

    PdLeaderFailoverGated::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(100);
    cfg.budget.total_wall   = std::chrono::milliseconds(500);
    cfg.budget.max_attempts = 6;

    std::atomic<int> resolves{0};
    std::atomic<int> refreshes{0};
    std::vector<std::string> seenLeaders;
    std::mutex seenMu;
    PdLeaderFailoverGated client(cfg,
        [&](const std::string& leader, std::chrono::milliseconds) {
            {
                std::lock_guard<std::mutex> lk(seenMu);
                seenLeaders.push_back(leader);
            }
            // Both old and new leaders accept calls in this case;
            // the injected stepdown is synthetic (no inner RPC fires
            // for the old leader), so the only call we see here is
            // the retry against the fresh leader.
            return ErrorCode::Success;
        },
        [&]() { ++resolves; return std::string("pd-new-leader"); },
        [&]() { ++refreshes; },
        /*initial_leader=*/"pd-old-leader");

    client.Arm();
    Helper::RetryBudget budget(cfg.budget);
    auto t0 = std::chrono::steady_clock::now();
    auto out = client.Call(budget);
    auto wall = std::chrono::steady_clock::now() - t0;

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(out.reconnected);
    BOOST_CHECK(out.refreshed);
    BOOST_CHECK_EQUAL(out.served_by, std::string("pd-new-leader"));
    BOOST_CHECK_EQUAL(resolves.load(), 1);
    BOOST_CHECK_EQUAL(refreshes.load(), 1);

    BOOST_REQUIRE_EQUAL(seenLeaders.size(), 1u);
    BOOST_CHECK_EQUAL(seenLeaders[0], std::string("pd-new-leader"));

    BOOST_CHECK_EQUAL(client.PdLeaderStepdown(),     1u);
    BOOST_CHECK_EQUAL(client.PdReconnected(),        1u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(), 1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);
    BOOST_CHECK_EQUAL(client.CurrentLeader(),
                      std::string("pd-new-leader"));
    BOOST_CHECK(!client.IsArmed());
    // No infinite hang.
    BOOST_CHECK_LT(std::chrono::duration_cast<std::chrono::milliseconds>(
                       wall).count(),
                   cfg.budget.total_wall.count() + 200);
}

// ---------------------------------------------------------------------------
// 4) Region-cache refresh on reconnect. The case-md requires that on
//    successful reconnect to a fresh leader, the region-cache TTL is
//    refreshed (region leaders can have shifted alongside PD leader
//    change). We model TTL as a steady_clock timestamp bumped by the
//    refresher callback and assert the post-reconnect timestamp is
//    strictly later than the pre-reconnect timestamp, plus
//    m_regionCacheRefreshed >= 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RegionCacheRefreshOnReconnect)
{
    ScopedEnv env("SPTAG_FAULT_PD_LEADER_FAILOVER", "1");

    PdLeaderFailoverGated::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(100);

    std::atomic<std::int64_t> ttlNs{0};
    auto preTtl = std::chrono::steady_clock::now().time_since_epoch().count();
    ttlNs.store(preTtl, std::memory_order_relaxed);

    PdLeaderFailoverGated client(cfg,
        [&](const std::string&, std::chrono::milliseconds) {
            return ErrorCode::Success;
        },
        []()       { return std::string("pd-fresh"); },
        [&]()      {
            // Bump TTL: simulate region-cache TTL refresh.
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            ttlNs.store(
                std::chrono::steady_clock::now().time_since_epoch().count(),
                std::memory_order_relaxed);
        },
        /*initial_leader=*/"pd-old");

    client.Arm();
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.Call(budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.refreshed);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(), 1u);
    BOOST_CHECK_GT(ttlNs.load(std::memory_order_relaxed), preTtl);
    BOOST_CHECK_EQUAL(client.PdReconnected(), 1u);
    BOOST_CHECK_EQUAL(client.PdLeaderStepdown(), 1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(), 0u);
}

// ---------------------------------------------------------------------------
// 5) Harness smoke. Two sequential armings on the same wrapper
//    instance: counters grow monotonically (each Arm() injects
//    exactly one stepdown; non-armed pass-through call between them
//    does not bump counters); no leaks / no spurious refresher fires;
//    EnvArmed reflects the env; ErrorCode wiring works.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(HarnessSmoke)
{
    {
        ScopedEnv off("SPTAG_FAULT_PD_LEADER_FAILOVER", nullptr);
        BOOST_CHECK(!PdLeaderFailoverGated::EnvArmed());
    }
    ScopedEnv env("SPTAG_FAULT_PD_LEADER_FAILOVER", "1");
    BOOST_REQUIRE(PdLeaderFailoverGated::EnvArmed());

    PdLeaderFailoverGated::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(50);

    std::atomic<int> resolves{0};
    std::atomic<int> refreshes{0};
    int leaderIdx = 0;
    std::vector<std::string> leaders = {"pd-init", "pd-second", "pd-third"};
    PdLeaderFailoverGated client(cfg,
        [&](const std::string&, std::chrono::milliseconds) {
            return ErrorCode::Success;
        },
        [&]() {
            ++resolves;
            ++leaderIdx;
            return leaders[std::min<size_t>(leaderIdx, leaders.size() - 1)];
        },
        [&]() { ++refreshes; },
        /*initial_leader=*/leaders[0]);

    // First arming.
    client.Arm();
    Helper::RetryBudget budget1(cfg.budget);
    auto o1 = client.Call(budget1);
    BOOST_CHECK(o1.code == ErrorCode::Success);
    BOOST_CHECK(o1.injected);
    BOOST_CHECK(o1.reconnected);
    BOOST_CHECK_EQUAL(client.PdLeaderStepdown(),     1u);
    BOOST_CHECK_EQUAL(client.PdReconnected(),        1u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(), 1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);

    // Pass-through between armings: must NOT bump any counter.
    Helper::RetryBudget budget2(cfg.budget);
    auto oMid = client.Call(budget2);
    BOOST_CHECK(oMid.code == ErrorCode::Success);
    BOOST_CHECK(!oMid.injected);
    BOOST_CHECK_EQUAL(client.PdLeaderStepdown(),     1u);
    BOOST_CHECK_EQUAL(client.PdReconnected(),        1u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(), 1u);

    // Second arming.
    client.Arm();
    Helper::RetryBudget budget3(cfg.budget);
    auto o2 = client.Call(budget3);
    BOOST_CHECK(o2.code == ErrorCode::Success);
    BOOST_CHECK(o2.injected);
    BOOST_CHECK(o2.reconnected);
    BOOST_CHECK_EQUAL(client.PdLeaderStepdown(),     2u);
    BOOST_CHECK_EQUAL(client.PdReconnected(),        2u);
    BOOST_CHECK_EQUAL(client.RegionCacheRefreshed(), 2u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),  0u);
    BOOST_CHECK_EQUAL(resolves.load(), 2);
    BOOST_CHECK_EQUAL(refreshes.load(), 2);

    // Wiring smoke: ErrorCode is well-formed.
    BOOST_CHECK(static_cast<int>(ErrorCode::Success)
                != static_cast<int>(ErrorCode::RetryBudgetExceeded));
}

BOOST_AUTO_TEST_SUITE_END()
