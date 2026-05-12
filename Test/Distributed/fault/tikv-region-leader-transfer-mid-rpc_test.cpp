// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-region-leader-transfer-mid-rpc
//
// Tier 1 HARD repro of the tikv-region-leader-transfer-mid-rpc
// invariants:
//
//   * env-off: wrapper dormant; one pass-through write against
//     `currentLeader`, no leader-transfer counter moves.
//   * env-armed + empty chain: leader is stable -> wrapper
//     drives one write, ++m_writeRoutedToNewLeader only.
//   * env-armed + single transfer: chain={p2} with budget>=1 ->
//     1 NotLeader observed, 1 cache update, write lands on p2.
//   * env-armed + chained transfers: chain={p2,p3,p4} with
//     budget>=3 -> 3 NotLeader observed, 3 cache updates, write
//     lands on p4.
//   * env-armed + retries blow past the RetryBudget: wrapper
//     fails fast with ErrorCode::LeaderTransferRetryCapExceeded,
//     ++m_clientErrorReturned.
//
// Wrapper under test:  TikvRegionLeaderTransferMidRpcGated
//                      (header-only; defers to
//                      Helper::RetryBudget + injected per-peer
//                      write callable).
// Env-gate:            SPTAG_FAULT_TIKV_REGION_LEADER_TRANSFER_MID_RPC
// Tier 2 (1M perf):    DEFERRED -- non hot-path (leader-transfer
//                      re-route logic sits on the TiKV write
//                      admission edge, not the SPANN search
//                      inner loop).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvRegionLeaderTransferMidRpcGated.h"
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
using RegionId = TikvRegionLeaderTransferMidRpcGated::RegionId;
using PeerId   = TikvRegionLeaderTransferMidRpcGated::PeerId;

Helper::RetryBudget::Config LeaderBudgetConfig(int maxAttempts, int budgetMs) {
    Helper::RetryBudget::Config c = Helper::RetryBudget::DefaultConfig();
    c.max_attempts = maxAttempts;
    c.total_wall = ms(budgetMs);
    return c;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvRegionLeaderTransferMidRpcTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the gate unset the wrapper drives one
//    pass-through write to currentLeader; even if a chain is
//    declared, env-off ignores it.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_LEADER_TRANSFER_MID_RPC", nullptr);
    BOOST_REQUIRE(!TikvRegionLeaderTransferMidRpcGated::EnvArmed());

    TikvRegionLeaderTransferMidRpcGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    std::atomic<PeerId> lastPeer{0};
    TikvRegionLeaderTransferMidRpcGated client(cfg,
        [&](RegionId, PeerId p, ms) {
            ++calls;
            lastPeer = p;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithLeaderGuard(
            /*regionId=*/77,
            /*currentLeader=*/1,
            /*leaderTransferChain=*/{2, 3, 4},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK_EQUAL(out.leader_transfer_obs, 0u);
    BOOST_CHECK_EQUAL(out.leader_cache_updated, 0u);
    BOOST_CHECK_EQUAL(out.routed_to_new_leader, 0u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastPeer.load(), static_cast<PeerId>(1));

    BOOST_CHECK_EQUAL(client.LeaderTransferObserved(),     0u);
    BOOST_CHECK_EQUAL(client.LeaderCacheUpdated(),         0u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToNewLeader(),     0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline stable leader: env-armed but chain empty -> leader
//    is stable, wrapper writes once, no NotLeader hint observed,
//    only m_writeRoutedToNewLeader moves.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineStableLeader)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_LEADER_TRANSFER_MID_RPC", "1");
    BOOST_REQUIRE(TikvRegionLeaderTransferMidRpcGated::EnvArmed());

    TikvRegionLeaderTransferMidRpcGated::Config cfg;
    cfg.per_write_timeout = ms(50);

    std::atomic<int> calls{0};
    std::atomic<PeerId> lastPeer{0};
    TikvRegionLeaderTransferMidRpcGated client(cfg,
        [&](RegionId, PeerId p, ms) {
            ++calls;
            lastPeer = p;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithLeaderGuard(
            /*regionId=*/77,
            /*currentLeader=*/1,
            /*leaderTransferChain=*/{},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.leader_transfer_obs, 0u);
    BOOST_CHECK_EQUAL(out.leader_cache_updated, 0u);
    BOOST_CHECK_EQUAL(out.routed_to_new_leader, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastPeer.load(), static_cast<PeerId>(1));

    BOOST_CHECK_EQUAL(client.LeaderTransferObserved(),     0u);
    BOOST_CHECK_EQUAL(client.LeaderCacheUpdated(),         0u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToNewLeader(),     1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 3) Single leader transfer. chain={p2} with budget max=2 ->
//    one NotLeader observed, one cache update to p2, write lands
//    on p2.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SingleLeaderTransfer)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_LEADER_TRANSFER_MID_RPC", "1");

    TikvRegionLeaderTransferMidRpcGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = LeaderBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    std::atomic<PeerId> lastPeer{0};
    TikvRegionLeaderTransferMidRpcGated client(cfg,
        [&](RegionId, PeerId p, ms) {
            ++calls;
            lastPeer = p;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithLeaderGuard(
            /*regionId=*/77,
            /*currentLeader=*/1,
            /*leaderTransferChain=*/{2},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.leader_transfer_obs, 1u);
    BOOST_CHECK_EQUAL(out.leader_cache_updated, 1u);
    BOOST_CHECK_EQUAL(out.routed_to_new_leader, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastPeer.load(), static_cast<PeerId>(2));

    BOOST_CHECK_EQUAL(client.LeaderTransferObserved(),     1u);
    BOOST_CHECK_EQUAL(client.LeaderCacheUpdated(),         1u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToNewLeader(),     1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 4) Chained leader transfers. chain={p2,p3,p4} with budget
//    max=5 -> three NotLeader observed, three cache updates,
//    write lands on p4.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ChainedLeaderTransfers)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_LEADER_TRANSFER_MID_RPC", "1");

    TikvRegionLeaderTransferMidRpcGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = LeaderBudgetConfig(/*maxAttempts=*/5, /*budgetMs=*/1000);

    std::atomic<int> calls{0};
    std::atomic<PeerId> lastPeer{0};
    TikvRegionLeaderTransferMidRpcGated client(cfg,
        [&](RegionId, PeerId p, ms) {
            ++calls;
            lastPeer = p;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithLeaderGuard(
            /*regionId=*/77,
            /*currentLeader=*/1,
            /*leaderTransferChain=*/{2, 3, 4},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.leader_transfer_obs, 3u);
    BOOST_CHECK_EQUAL(out.leader_cache_updated, 3u);
    BOOST_CHECK_EQUAL(out.routed_to_new_leader, 1u);
    BOOST_CHECK_EQUAL(out.attempted_writes, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);
    BOOST_CHECK_EQUAL(lastPeer.load(), static_cast<PeerId>(4));

    BOOST_CHECK_EQUAL(client.LeaderTransferObserved(),     3u);
    BOOST_CHECK_EQUAL(client.LeaderCacheUpdated(),         3u);
    BOOST_CHECK_EQUAL(client.WriteRoutedToNewLeader(),     1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 5) Retry cap exceeded. Deep chain {p2,p3,p4,p5,p6} with budget
//    max_attempts=2 -> wrapper fails fast with
//    ErrorCode::LeaderTransferRetryCapExceeded.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(LeaderTransferRetryCapExceeded)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_REGION_LEADER_TRANSFER_MID_RPC", "1");

    TikvRegionLeaderTransferMidRpcGated::Config cfg;
    cfg.per_write_timeout = ms(50);
    cfg.budget = LeaderBudgetConfig(/*maxAttempts=*/2, /*budgetMs=*/500);

    std::atomic<int> calls{0};
    TikvRegionLeaderTransferMidRpcGated client(cfg,
        [&](RegionId, PeerId, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithLeaderGuard(
            /*regionId=*/77,
            /*currentLeader=*/1,
            /*leaderTransferChain=*/{2, 3, 4, 5, 6},
            budget);

    BOOST_CHECK(out.code == ErrorCode::LeaderTransferRetryCapExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.routed_to_new_leader, 0u);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteRoutedToNewLeader(),     0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        1u);

    // Wiring smoke: distinct error code.
    BOOST_CHECK(static_cast<int>(ErrorCode::LeaderTransferRetryCapExceeded)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::LeaderTransferRetryCapExceeded)
                != static_cast<int>(ErrorCode::MergeRetryCapExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::LeaderTransferRetryCapExceeded)
                != static_cast<int>(ErrorCode::SplitStormRetryCapExceeded));
}

BOOST_AUTO_TEST_SUITE_END()
