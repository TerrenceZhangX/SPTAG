// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-rocksdb-write-stall
//
// Tier 1 HARD repro of the tikv-rocksdb-write-stall invariants
// from design-docs/ft-fault-cases/tikv-rocksdb-write-stall.md:
//
//   * env-off: wrapper dormant; primary write succeeds, no counter
//     moves;
//   * env-armed + no stall: tokens=0 from the start -> wrapper
//     drives the primary write, m_writeSucceededAfterStall == 1,
//     m_writeStallObserved == 0, m_retryAfterBackoff == 0;
//   * env-armed + stall clears after backoff: tokens=2 ->
//     wrapper observes the stall twice, backs off twice, then
//     drives the inner write and succeeds:
//     m_writeStallObserved == 1, m_retryAfterBackoff == 2,
//     m_writeSucceededAfterStall == 1;
//   * env-armed + stall outlasts budget: tokens > attempts left ->
//     wrapper returns ErrorCode::WriteStall,
//     m_clientErrorReturned == 1, no inner write fires;
//   * env-armed + budget=0: wrapper fails fast with
//     ErrorCode::WriteStall before any retry / inner call.
//
// Wrapper under test:  TikvRocksdbWriteStallGated (header-only;
//                      defers to Helper::RetryBudget + injected
//                      per-replica write callable).
// Env-gate:            SPTAG_FAULT_TIKV_ROCKSDB_WRITE_STALL
// Tier 2 (1M perf):    DEFERRED -- non hot-path (RocksDB stall
//                      back-pressure surfaces on the per-RPC TiKV
//                      write admission edge, not the SPANN search
//                      inner loop).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvRocksdbWriteStallGated.h"
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

BOOST_AUTO_TEST_SUITE(TikvRocksdbWriteStallTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the env-gate unset the wrapper is dormant:
//    the stall-token vector is irrelevant, the wrapper drives the
//    primary write, accepts Success, every counter stays at zero.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_WRITE_STALL", nullptr);
    BOOST_REQUIRE(!TikvRocksdbWriteStallGated::EnvArmed());

    TikvRocksdbWriteStallGated::Config cfg;
    cfg.per_replica_timeout = ms(50);

    std::vector<std::string> seen;
    std::mutex seenMu;
    TikvRocksdbWriteStallGated client(cfg,
        [&](const std::string& replica, ms) {
            std::lock_guard<std::mutex> lk(seenMu);
            seen.push_back(replica);
            return ErrorCode::Success;
        });

    // Even with tokens loaded, env-off must stay pass-through.
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithStallGuard(
            {"S0", "S1", "S2"},
            /*stallTokens=*/{5, 5, 5},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK_EQUAL(out.stall_seen, 0u);
    BOOST_CHECK_EQUAL(out.retries, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_REQUIRE_EQUAL(seen.size(), 1u);
    BOOST_CHECK_EQUAL(seen[0], std::string("S0"));
    BOOST_CHECK_EQUAL(client.WriteStallObserved(),        0u);
    BOOST_CHECK_EQUAL(client.RetryAfterBackoff(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterStall(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline no stall. Env-armed but tokens[0]==0 -> wrapper drives
//    the primary write right away, succeeds, no stall observed.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineNoStall)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_WRITE_STALL", "1");
    BOOST_REQUIRE(TikvRocksdbWriteStallGated::EnvArmed());

    TikvRocksdbWriteStallGated::Config cfg;
    cfg.per_replica_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvRocksdbWriteStallGated client(cfg,
        [&](const std::string& replica, ms) {
            ++calls;
            BOOST_CHECK_EQUAL(replica, std::string("S0"));
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithStallGuard(
            {"S0", "S1", "S2"},
            /*stallTokens=*/{0, 0, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.stall_seen, 0u);
    BOOST_CHECK_EQUAL(out.retries, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.WriteStallObserved(),        0u);
    BOOST_CHECK_EQUAL(client.RetryAfterBackoff(),         0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterStall(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       0u);
}

// ---------------------------------------------------------------------------
// 3) Stall clears after backoff. tokens[0]=2 -> wrapper observes
//    the stall twice, backs off twice, then writes succeed on the
//    primary. m_writeStallObserved == 1, m_retryAfterBackoff == 2,
//    m_writeSucceededAfterStall == 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(StallClearsAfterBackoff)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_WRITE_STALL", "1");

    TikvRocksdbWriteStallGated::Config cfg;
    cfg.per_replica_timeout = ms(100);
    cfg.budget.total_wall   = ms(500);
    cfg.budget.max_attempts = 6;

    std::atomic<int> calls{0};
    TikvRocksdbWriteStallGated client(cfg,
        [&](const std::string& replica, ms) {
            ++calls;
            BOOST_CHECK_EQUAL(replica, std::string("S0"));
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithStallGuard(
            {"S0", "S1", "S2"},
            /*stallTokens=*/{2, 0, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.stall_seen, 2u);
    BOOST_CHECK_EQUAL(out.retries, 2u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.WriteStallObserved(),        1u);
    BOOST_CHECK_EQUAL(client.RetryAfterBackoff(),         2u);
    BOOST_CHECK_EQUAL(client.WriteSucceededAfterStall(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       0u);
}

// ---------------------------------------------------------------------------
// 4) Stall exhausts budget. tokens > attempts the budget allows ->
//    wrapper returns ErrorCode::WriteStall;
//    m_clientErrorReturned == 1, inner write never fires.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(StallExhaustsBudget)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_WRITE_STALL", "1");

    TikvRocksdbWriteStallGated::Config cfg;
    cfg.per_replica_timeout  = ms(50);
    cfg.budget.total_wall    = ms(500);
    cfg.budget.max_attempts  = 3;

    std::atomic<int> calls{0};
    TikvRocksdbWriteStallGated client(cfg,
        [&](const std::string&, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithStallGuard(
            {"S0", "S1", "S2"},
            /*stallTokens=*/{10, 0, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::WriteStall);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteSucceededAfterStall(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       1u);
    BOOST_CHECK_EQUAL(client.WriteStallObserved(), 1u);
    BOOST_CHECK(client.RetryAfterBackoff() >= 1u);

    // Wiring smoke: WriteStall is well-formed and distinct from
    // sibling client-visible TiKV error codes.
    BOOST_CHECK(static_cast<int>(ErrorCode::WriteStall)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::WriteStall)
                != static_cast<int>(ErrorCode::DiskFull));
    BOOST_CHECK(static_cast<int>(ErrorCode::WriteStall)
                != static_cast<int>(ErrorCode::SlowDiskBudgetExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::WriteStall)
                != static_cast<int>(ErrorCode::StoreUnavailable));
    BOOST_CHECK(static_cast<int>(ErrorCode::WriteStall)
                != static_cast<int>(ErrorCode::RetryBudgetExceeded));
}

// ---------------------------------------------------------------------------
// 5) Retry budget exhausted. Budget=0 with stall pending ->
//    wrapper bails out with ErrorCode::WriteStall on entry,
//    m_clientErrorReturned == 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RetryBudgetExhausted)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_WRITE_STALL", "1");

    TikvRocksdbWriteStallGated::Config cfg;
    cfg.per_replica_timeout  = ms(20);
    cfg.budget.total_wall    = ms(2);
    cfg.budget.max_attempts  = 1;

    std::atomic<int> calls{0};
    TikvRocksdbWriteStallGated client(cfg,
        [&](const std::string&, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    // Pre-consume to ensure the budget is empty on entry.
    (void)budget.record_attempt();
    auto out = client.WriteWithStallGuard(
            {"S0", "S1", "S2"},
            /*stallTokens=*/{5, 0, 0},
            budget);

    BOOST_CHECK(out.code == ErrorCode::WriteStall);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteSucceededAfterStall(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),       1u);
}

BOOST_AUTO_TEST_SUITE_END()
