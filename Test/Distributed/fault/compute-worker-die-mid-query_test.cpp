// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: compute-worker-die-mid-query
//
// Tier 1 HARD repro of the compute-worker-die-mid-query invariant
// triple from design-docs/ft-fault-cases/compute-worker-die-mid-query.md:
//
//   * client surfaces a *defined* ErrorCode when the worker dies
//     mid-RawBatchGet (no infinite hang, no silent partial);
//   * once the detector has decided a worker is Dead, subsequent
//     queries fast-fail OwnerSuspect with no RPC;
//   * the dispatcher reroutes future queries through a healthy peer
//     (SWIM suspect + retry-budget) when one is available.
//
// Wrapper under test:  QueryDispatcherGated (header-only; defers to
//                      SwimDetector + Helper::RetryBudget).
// Env-gate:            SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY
//                      (off -> wrapper dormant, counters stay zero).
// Tier 2 (1M perf):    DEFERRED -- non hot-path (the gated dispatcher
//                      sits outside the SPANN search inner loop).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/SwimDetector.h"
#include "inc/Core/SPANN/Distributed/QueryDispatcherGated.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <mutex>
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

// Drive a SwimDetector tick loop until `peer` is observed Dead.
// Mirrors the helper used in swim-detector-lag_test.cpp.
void DriveToDead(SwimDetector& d, int peer, std::uint64_t suspectTicks,
                 std::uint64_t deadTicks) {
    MemberState peek;
    BOOST_REQUIRE(d.TryGetMember(peer, peek));
    const std::uint64_t cap =
        (suspectTicks + deadTicks + 2) * 8 + 32;
    MemberState st;
    for (std::uint64_t i = 0; i < cap; ++i) {
        (void)d.Tick();
        if (d.TryGetMember(peer, st)
            && st.health == MemberHealth::Dead) break;
    }
    BOOST_REQUIRE_EQUAL(static_cast<int>(st.health),
                        static_cast<int>(MemberHealth::Dead));
}

}  // namespace

BOOST_AUTO_TEST_SUITE(ComputeWorkerDieMidQueryTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY unset
//    the wrapper must forward the inner RPC unchanged and keep all three
//    counters at zero -- even when the inner RPC fails. This protects
//    baseline perf and keeps the wrapper invisible in production builds.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY", nullptr);
    BOOST_REQUIRE(!QueryDispatcherGated::EnvArmed());

    SwimDetector det(/*self=*/0, /*peers=*/{1, 2});
    DriveToDead(det, /*peer=*/1, /*suspect=*/1, /*dead=*/3);

    QueryDispatcherGated::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(50);

    std::atomic<int> rpcCalls{0};
    QueryDispatcherGated client(det, cfg,
        [&](int peer, std::chrono::milliseconds /*deadline*/) {
            ++rpcCalls;
            // Even if the (env-off) caller hits the "dead" peer 1, the
            // wrapper does not consult the detector -- it simply forwards.
            // We return Success to assert pure pass-through semantics.
            (void)peer;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.Call(/*primary=*/1, /*reroute=*/{2}, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.primary_dead);
    BOOST_CHECK(!out.primary_failed);
    BOOST_CHECK(!out.rerouted);
    BOOST_CHECK_EQUAL(out.served_by, 1);
    BOOST_CHECK_EQUAL(rpcCalls.load(), 1);
    BOOST_CHECK_EQUAL(client.QueryWorkerDied(),       0u);
    BOOST_CHECK_EQUAL(client.Rerouted(),              0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),   0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline healthy query. Env-armed, primary alive, inner RPC returns
//    Success: wrapper returns Success without bumping any counter (no
//    spurious reroute, no spurious error accounting).
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineHealthyQuery)
{
    ScopedEnv env("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY", "1");
    BOOST_REQUIRE(QueryDispatcherGated::EnvArmed());

    SwimDetector det(/*self=*/0, /*peers=*/{1, 2});
    // Detector ticks not advanced -- peer 1 stays Alive.

    QueryDispatcherGated::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(100);

    std::atomic<int> rpcCalls{0};
    QueryDispatcherGated client(det, cfg,
        [&](int peer, std::chrono::milliseconds /*deadline*/) {
            ++rpcCalls;
            BOOST_CHECK_EQUAL(peer, 1);
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.Call(/*primary=*/1, /*reroute=*/{2}, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.primary_dead);
    BOOST_CHECK(!out.primary_failed);
    BOOST_CHECK(!out.rerouted);
    BOOST_CHECK_EQUAL(out.served_by, 1);
    BOOST_CHECK_EQUAL(rpcCalls.load(), 1);
    BOOST_CHECK_EQUAL(client.QueryWorkerDied(),     0u);
    BOOST_CHECK_EQUAL(client.Rerouted(),            0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(), 0u);
}

// ---------------------------------------------------------------------------
// 3) Worker dies mid-query, no healthy reroute peer -> client gets a
//    *defined* ErrorCode (OwnerDead). Asserts:
//      * code == OwnerDead (not Success, not a bare RPC error code);
//      * m_queryWorkerDied bumped;
//      * m_clientErrorReturned bumped;
//      * m_rerouted untouched.
//    This is the "client gets defined ERROR_CODE" invariant from
//    case-md "Expected behaviour" #2/#4.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(KillMidQueryReturnsErrorCode)
{
    ScopedEnv env("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY", "1");

    SwimDetector det(/*self=*/0, /*peers=*/{1, 2});
    // Mark peer 2 Dead so reroute has no live target -> wrapper must
    // surface OwnerDead instead of silently rerouting.
    DriveToDead(det, /*peer=*/2, /*suspect=*/1, /*dead=*/3);

    QueryDispatcherGated::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(40);
    cfg.budget.total_wall   = std::chrono::milliseconds(300);
    cfg.budget.max_attempts = 4;
    cfg.budget.base_backoff = std::chrono::milliseconds(5);
    cfg.budget.cap_backoff  = std::chrono::milliseconds(20);

    std::atomic<int> rpcCalls{0};
    QueryDispatcherGated client(det, cfg,
        [&](int peer, std::chrono::milliseconds /*deadline*/) {
            ++rpcCalls;
            // Both primary (1) and (defensively) any other peer fail:
            // the SIGKILL has dropped the TCP connection.
            (void)peer;
            return ErrorCode::Socket_FailedConnectToEndPoint;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto t0 = std::chrono::steady_clock::now();
    auto out = client.Call(/*primary=*/1, /*reroute=*/{2}, budget);
    auto wall = std::chrono::steady_clock::now() - t0;

    BOOST_CHECK(out.code == ErrorCode::OwnerDead);
    BOOST_CHECK(out.primary_failed);
    BOOST_CHECK(!out.rerouted);
    BOOST_CHECK_EQUAL(out.served_by, -1);
    // Only the primary is RPC-attempted: peer 2 is Dead per detector
    // and therefore skipped without a wire-call.
    BOOST_CHECK_EQUAL(rpcCalls.load(), 1);
    BOOST_CHECK_EQUAL(client.QueryWorkerDied(),     1u);
    BOOST_CHECK_EQUAL(client.Rerouted(),            0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(), 1u);
    // No infinite hang: we surface the typed error well within the
    // budget total_wall.
    BOOST_CHECK_LT(std::chrono::duration_cast<std::chrono::milliseconds>(
                       wall).count(),
                   cfg.budget.total_wall.count() + 200);
}

// ---------------------------------------------------------------------------
// 4) Worker dies mid-query but a healthy peer exists -> dispatcher
//    reroutes and returns Success. Asserts:
//      * code == Success, served_by == reroute peer, rerouted == true;
//      * m_queryWorkerDied bumped (primary failed);
//      * m_rerouted bumped;
//      * m_clientErrorReturned NOT bumped (client never saw an error).
//    This is the "dispatcher reroutes future queries" invariant
//    (case-md Expected behaviour #3).
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(KillThenRerouteToHealthyPeer)
{
    ScopedEnv env("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY", "1");

    SwimDetector det(/*self=*/0, /*peers=*/{1, 2, 3});
    // peer 1 = primary (will fail mid-RPC);
    // peer 2 = also fails (e.g. transient socket reset);
    // peer 3 = healthy reroute target.

    QueryDispatcherGated::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(50);
    cfg.budget.total_wall   = std::chrono::milliseconds(500);
    cfg.budget.max_attempts = 6;

    std::atomic<int> rpcCalls{0};
    std::vector<int> seenPeers;
    std::mutex seenMu;
    QueryDispatcherGated client(det, cfg,
        [&](int peer, std::chrono::milliseconds /*deadline*/) {
            ++rpcCalls;
            {
                std::lock_guard<std::mutex> lk(seenMu);
                seenPeers.push_back(peer);
            }
            if (peer == 1) return ErrorCode::Socket_FailedConnectToEndPoint;
            if (peer == 2) return ErrorCode::Socket_FailedConnectToEndPoint;
            return ErrorCode::Success;  // peer 3
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.Call(/*primary=*/1, /*reroute=*/{2, 3}, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.primary_failed);
    BOOST_CHECK(out.rerouted);
    BOOST_CHECK_EQUAL(out.served_by, 3);
    // Primary + two reroute attempts (2 fails, 3 succeeds).
    BOOST_CHECK_EQUAL(rpcCalls.load(), 3);
    BOOST_REQUIRE_EQUAL(seenPeers.size(), 3u);
    BOOST_CHECK_EQUAL(seenPeers[0], 1);
    BOOST_CHECK_EQUAL(seenPeers[1], 2);
    BOOST_CHECK_EQUAL(seenPeers[2], 3);

    BOOST_CHECK_EQUAL(client.QueryWorkerDied(),     1u);
    BOOST_CHECK_EQUAL(client.Rerouted(),            1u);
    // Reroute succeeded, so the client never observed an error.
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(), 0u);
}

// ---------------------------------------------------------------------------
// 5) Harness smoke: detector + budget + wrapper all instantiate cleanly,
//    counters start at 0, EnvArmed reflects the env, the new ErrorCode
//    enumerators are distinct/well-formed, and a Success path leaves all
//    counters untouched even when env-armed.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(HarnessSmoke)
{
    BOOST_CHECK(static_cast<int>(ErrorCode::OwnerSuspect)
                != static_cast<int>(ErrorCode::OwnerDead));
    BOOST_CHECK(static_cast<int>(ErrorCode::OwnerDead)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::OwnerDead)
                != static_cast<int>(ErrorCode::RetryBudgetExceeded));

    {
        ScopedEnv off("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY", nullptr);
        BOOST_CHECK(!QueryDispatcherGated::EnvArmed());
    }
    {
        ScopedEnv on("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY", "1");
        BOOST_CHECK(QueryDispatcherGated::EnvArmed());

        SwimDetector det(0, {1, 2});
        QueryDispatcherGated::Config cfg;
        cfg.per_call_timeout = std::chrono::milliseconds(20);

        QueryDispatcherGated client(det, cfg,
            [&](int, std::chrono::milliseconds) {
                return ErrorCode::Success;
            });
        BOOST_CHECK_EQUAL(client.QueryWorkerDied(),     0u);
        BOOST_CHECK_EQUAL(client.Rerouted(),            0u);
        BOOST_CHECK_EQUAL(client.ClientErrorReturned(), 0u);

        Helper::RetryBudget budget(cfg.budget);
        auto out = client.Call(1, {2}, budget);
        BOOST_CHECK(out.code == ErrorCode::Success);
        BOOST_CHECK(!out.primary_failed);
        BOOST_CHECK(!out.rerouted);
        BOOST_CHECK_EQUAL(out.served_by, 1);
        BOOST_CHECK_EQUAL(client.QueryWorkerDied(),     0u);
        BOOST_CHECK_EQUAL(client.Rerouted(),            0u);
        BOOST_CHECK_EQUAL(client.ClientErrorReturned(), 0u);
    }
}

BOOST_AUTO_TEST_SUITE_END()
