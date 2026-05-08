// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-slow-disk
//
// Tier 1 HARD repro of the tikv-slow-disk invariants from
// design-docs/ft-fault-cases/tikv-slow-disk.md:
//
//   * env-off: wrapper dormant; primary write succeeds, no counter
//     moves;
//   * env-armed + primary fits the latency budget: write succeeds
//     on the primary, m_writeSucceededOnReplica == 1, no failover;
//   * env-armed + primary slow + replica fast: wrapper observes the
//     slow primary, falls through to the next replica that fits the
//     budget, returns Success; m_slowWriteObserved,
//     m_failoverToFastReplica, m_writeSucceededOnReplica all bump;
//   * env-armed + every replica slow: wrapper returns
//     ErrorCode::SlowDiskBudgetExceeded; m_clientErrorReturned == 1,
//     m_slowWriteObserved == replicas.size(), no inner write fires;
//   * env-armed + retry budget exhausts before any replica accepts:
//     wrapper returns ErrorCode::SlowDiskBudgetExceeded.
//
// Wrapper under test:  TikvSlowDiskGated (header-only; defers to
//                      Helper::RetryBudget + injected per-replica
//                      write callable).
// Env-gate:            SPTAG_FAULT_TIKV_SLOW_DISK
// Tier 2 (1M perf):    DEFERRED -- non hot-path (latency budget
//                      sits on the per-store EWMA / write-batch
//                      admission edge, not the SPANN search inner
//                      loop or the per-RPC TiKV retry hot path).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvSlowDiskGated.h"
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

using ms = std::chrono::milliseconds;

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvSlowDiskTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the env-gate unset the wrapper is dormant:
//    the per-replica slowness vector is irrelevant, the wrapper drives
//    the primary write, accepts Success, every counter stays at zero.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_SLOW_DISK", nullptr);
    BOOST_REQUIRE(!TikvSlowDiskGated::EnvArmed());

    TikvSlowDiskGated::Config cfg;
    cfg.per_replica_timeout = ms(50);

    std::vector<std::string> seen;
    std::mutex seenMu;
    TikvSlowDiskGated client(cfg,
        [&](const std::string& replica, ms, ms) {
            std::lock_guard<std::mutex> lk(seenMu);
            seen.push_back(replica);
            return ErrorCode::Success;
        });

    // Even with primary "slow" in the slowness vector, env-off must
    // stay pass-through against the primary.
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSlowDiskGuard(
            {"S0", "S1", "S2"},
            /*slowness=*/{ms(500), ms(1), ms(1)},
            /*latencyBudgetMs=*/ms(20), budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK(!out.failed_over);
    BOOST_CHECK_EQUAL(out.slow_seen, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_REQUIRE_EQUAL(seen.size(), 1u);
    BOOST_CHECK_EQUAL(seen[0], std::string("S0"));
    BOOST_CHECK_EQUAL(client.SlowWriteObserved(),       0u);
    BOOST_CHECK_EQUAL(client.FailoverToFastReplica(),   0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline fast disk. Env-armed but every replica's observed
//    latency comfortably fits the budget: no failover, no slow
//    op observed, write succeeds on the primary,
//    m_writeSucceededOnReplica == 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineFastDisk)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_SLOW_DISK", "1");
    BOOST_REQUIRE(TikvSlowDiskGated::EnvArmed());

    TikvSlowDiskGated::Config cfg;
    cfg.per_replica_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvSlowDiskGated client(cfg,
        [&](const std::string& replica, ms, ms) {
            ++calls;
            BOOST_CHECK_EQUAL(replica, std::string("S0"));
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSlowDiskGuard(
            {"S0", "S1", "S2"},
            {ms(2), ms(2), ms(2)},
            /*latencyBudgetMs=*/ms(20), budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.failed_over);
    BOOST_CHECK_EQUAL(out.slow_seen, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.SlowWriteObserved(),       0u);
    BOOST_CHECK_EQUAL(client.FailoverToFastReplica(),   0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
}

// ---------------------------------------------------------------------------
// 3) Primary slow, failover succeeds. Env-armed + primary's latency
//    > budget, but a replica fits. Wrapper observes the slow
//    primary, falls through to the next replica which succeeds.
//    m_slowWriteObserved == 1, m_failoverToFastReplica == 1,
//    m_writeSucceededOnReplica == 1, no client error.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(PrimarySlowFailoverSucceeds)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_SLOW_DISK", "1");

    TikvSlowDiskGated::Config cfg;
    cfg.per_replica_timeout = ms(100);
    cfg.budget.total_wall   = ms(500);
    cfg.budget.max_attempts = 6;

    std::atomic<int> calls{0};
    TikvSlowDiskGated client(cfg,
        [&](const std::string& replica, ms, ms) {
            ++calls;
            // Primary is slow; only S1/S2 should be hit by the
            // inner write.
            BOOST_CHECK(replica != std::string("S0"));
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSlowDiskGuard(
            {"S0", "S1", "S2"},
            {ms(500), ms(2), ms(2)},
            /*latencyBudgetMs=*/ms(20), budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(out.failed_over);
    BOOST_CHECK_EQUAL(out.slow_seen, 1u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S1"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.SlowWriteObserved(),       1u);
    BOOST_CHECK_EQUAL(client.FailoverToFastReplica(),   1u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     0u);
}

// ---------------------------------------------------------------------------
// 4) All replicas slow. Every replica's latency > budget -> wrapper
//    returns ErrorCode::SlowDiskBudgetExceeded; m_slowWriteObserved
//    == N, m_clientErrorReturned == 1, no inner write fires.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(AllReplicasSlow)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_SLOW_DISK", "1");

    TikvSlowDiskGated::Config cfg;
    cfg.per_replica_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvSlowDiskGated client(cfg,
        [&](const std::string&, ms, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    const std::size_t kN = 3;
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSlowDiskGuard(
            {"S0", "S1", "S2"},
            {ms(400), ms(500), ms(600)},
            /*latencyBudgetMs=*/ms(20), budget);

    BOOST_CHECK(out.code == ErrorCode::SlowDiskBudgetExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.failed_over);
    BOOST_CHECK_EQUAL(out.slow_seen, kN);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.SlowWriteObserved(),       static_cast<std::uint64_t>(kN));
    BOOST_CHECK_EQUAL(client.FailoverToFastReplica(),   0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     1u);

    // Wiring smoke: SlowDiskBudgetExceeded is well-formed and
    // distinct from sibling client-visible TiKV error codes.
    BOOST_CHECK(static_cast<int>(ErrorCode::SlowDiskBudgetExceeded)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::SlowDiskBudgetExceeded)
                != static_cast<int>(ErrorCode::DiskFull));
    BOOST_CHECK(static_cast<int>(ErrorCode::SlowDiskBudgetExceeded)
                != static_cast<int>(ErrorCode::StoreUnavailable));
    BOOST_CHECK(static_cast<int>(ErrorCode::SlowDiskBudgetExceeded)
                != static_cast<int>(ErrorCode::MinReplicasViolated));
    BOOST_CHECK(static_cast<int>(ErrorCode::SlowDiskBudgetExceeded)
                != static_cast<int>(ErrorCode::StaleStoreView));
    BOOST_CHECK(static_cast<int>(ErrorCode::SlowDiskBudgetExceeded)
                != static_cast<int>(ErrorCode::RetryBudgetExceeded));
}

// ---------------------------------------------------------------------------
// 5) Retry budget exhausted. Tight budget + every replica slow ->
//    wrapper bails out with ErrorCode::SlowDiskBudgetExceeded just
//    as in (4), m_clientErrorReturned == 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RetryBudgetExhausted)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_SLOW_DISK", "1");

    TikvSlowDiskGated::Config cfg;
    cfg.per_replica_timeout  = ms(20);
    cfg.budget.total_wall    = ms(2);
    cfg.budget.max_attempts  = 1;

    std::atomic<int> calls{0};
    TikvSlowDiskGated client(cfg,
        [&](const std::string&, ms, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithSlowDiskGuard(
            {"S0", "S1", "S2"},
            {ms(800), ms(800), ms(800)},
            /*latencyBudgetMs=*/ms(20), budget);

    BOOST_CHECK(out.code == ErrorCode::SlowDiskBudgetExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.failed_over);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(), 0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),     1u);
}

BOOST_AUTO_TEST_SUITE_END()
