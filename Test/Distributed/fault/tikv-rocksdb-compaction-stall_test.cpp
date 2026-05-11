// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-rocksdb-compaction-stall
//
// Tier 1 HARD repro of the tikv-rocksdb-compaction-stall
// invariants from design-docs/ft-fault-cases/
// tikv-rocksdb-compaction-stall.md:
//
//   * env-off: wrapper dormant; primary write succeeds, no counter
//     moves.
//   * env-armed + baseline pressure (pending <= soft): wrapper
//     drives the primary write, counters stay at zero.
//   * env-armed + degraded pressure with budget>0: wrapper accepts
//     N degraded writes (++m_compactionPressureObserved each;
//     ++m_writeAcceptedDegraded each) up to the degraded budget.
//   * env-armed + degraded pressure with budget=0: wrapper fails
//     fast with ErrorCode::CompactionStallExceeded,
//     ++m_clientErrorReturned, no inner write fires (no healthy
//     failover available).
//   * env-armed + hard-limit pressure: wrapper records
//     ++m_writeRejectedHardLimit and fails fast with
//     ErrorCode::CompactionStallExceeded.
//
// Wrapper under test:  TikvRocksdbCompactionStallGated (header-
//                      only; defers to Helper::RetryBudget +
//                      injected per-replica write callable).
// Env-gate:            SPTAG_FAULT_TIKV_ROCKSDB_COMPACTION_STALL
// Tier 2 (1M perf):    DEFERRED -- non hot-path (RocksDB
//                      compaction back-pressure surfaces on the
//                      per-RPC TiKV write admission edge, not the
//                      SPANN search inner loop).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvRocksdbCompactionStallGated.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdint>
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

constexpr std::uint64_t kSoft = 64ULL * 1024ULL * 1024ULL;       //  64 MiB
constexpr std::uint64_t kHard = 256ULL * 1024ULL * 1024ULL;      // 256 MiB

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvRocksdbCompactionStallTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the env-gate unset the wrapper is
//    dormant: the pending-bytes vector is irrelevant, the wrapper
//    drives the primary write, accepts Success, every counter
//    stays at zero.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_COMPACTION_STALL", nullptr);
    BOOST_REQUIRE(!TikvRocksdbCompactionStallGated::EnvArmed());

    TikvRocksdbCompactionStallGated::Config cfg;
    cfg.per_replica_timeout = ms(50);

    std::vector<std::string> seen;
    std::mutex seenMu;
    TikvRocksdbCompactionStallGated client(cfg,
        [&](const std::string& replica, ms) {
            std::lock_guard<std::mutex> lk(seenMu);
            seen.push_back(replica);
            return ErrorCode::Success;
        });

    // Even with primary saturated past hard, env-off must stay
    // pass-through and accept the write.
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithCompactionGuard(
            {"S0", "S1", "S2"},
            /*pending=*/{kHard * 2, 0, 0},
            kSoft, kHard,
            /*degradedBudget=*/0,
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK_EQUAL(out.pressure_seen, 0u);
    BOOST_CHECK_EQUAL(out.hard_seen, 0u);
    BOOST_CHECK_EQUAL(out.degraded_writes, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_REQUIRE_EQUAL(seen.size(), 1u);
    BOOST_CHECK_EQUAL(seen[0], std::string("S0"));
    BOOST_CHECK_EQUAL(client.CompactionPressureObserved(), 0u);
    BOOST_CHECK_EQUAL(client.WriteAcceptedDegraded(),      0u);
    BOOST_CHECK_EQUAL(client.WriteRejectedHardLimit(),     0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline low pressure. Env-armed but pending <= soft on
//    primary -> wrapper drives the primary write right away,
//    succeeds, no counter moves.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselineLowPressure)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_COMPACTION_STALL", "1");
    BOOST_REQUIRE(TikvRocksdbCompactionStallGated::EnvArmed());

    TikvRocksdbCompactionStallGated::Config cfg;
    cfg.per_replica_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvRocksdbCompactionStallGated client(cfg,
        [&](const std::string& replica, ms) {
            ++calls;
            BOOST_CHECK_EQUAL(replica, std::string("S0"));
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithCompactionGuard(
            {"S0", "S1", "S2"},
            /*pending=*/{kSoft / 2, 0, 0},
            kSoft, kHard,
            /*degradedBudget=*/0,
            budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.pressure_seen, 0u);
    BOOST_CHECK_EQUAL(out.hard_seen, 0u);
    BOOST_CHECK_EQUAL(out.degraded_writes, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.CompactionPressureObserved(), 0u);
    BOOST_CHECK_EQUAL(client.WriteAcceptedDegraded(),      0u);
    BOOST_CHECK_EQUAL(client.WriteRejectedHardLimit(),     0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 3) Degraded writes under soft < pending < hard. With
//    degradedBudget=2 the caller can drive two degraded writes
//    before failing fast. Each call is one observation +
//    one degraded acceptance.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(DegradedWritesUnderSoftLimit)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_COMPACTION_STALL", "1");

    TikvRocksdbCompactionStallGated::Config cfg;
    cfg.per_replica_timeout = ms(100);

    std::atomic<int> calls{0};
    TikvRocksdbCompactionStallGated client(cfg,
        [&](const std::string& replica, ms) {
            ++calls;
            BOOST_CHECK_EQUAL(replica, std::string("S0"));
            return ErrorCode::Success;
        });

    // Two independent calls, each with budget room for one
    // degraded write.
    for (int i = 0; i < 2; ++i) {
        Helper::RetryBudget budget(cfg.budget);
        auto out = client.WriteWithCompactionGuard(
                {"S0", "S1", "S2"},
                /*pending=*/{(kSoft + kHard) / 2, 0, 0},
                kSoft, kHard,
                /*degradedBudget=*/1,
                budget);
        BOOST_CHECK(out.code == ErrorCode::Success);
        BOOST_CHECK(out.injected);
        BOOST_CHECK_EQUAL(out.pressure_seen, 1u);
        BOOST_CHECK_EQUAL(out.degraded_writes, 1u);
        BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
        BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    }

    BOOST_CHECK_EQUAL(calls.load(), 2);
    BOOST_CHECK_EQUAL(client.CompactionPressureObserved(), 2u);
    BOOST_CHECK_EQUAL(client.WriteAcceptedDegraded(),      2u);
    BOOST_CHECK_EQUAL(client.WriteRejectedHardLimit(),     0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        0u);
}

// ---------------------------------------------------------------------------
// 4) Degraded budget exhausted. soft < pending < hard on primary,
//    degradedBudget=0, peers also pressured -> no healthy failover
//    -> wrapper returns ErrorCode::CompactionStallExceeded,
//    ++m_compactionPressureObserved, ++m_clientErrorReturned, no
//    inner write fires.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(DegradedBudgetExhausted)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_COMPACTION_STALL", "1");

    TikvRocksdbCompactionStallGated::Config cfg;
    cfg.per_replica_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvRocksdbCompactionStallGated client(cfg,
        [&](const std::string&, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithCompactionGuard(
            {"S0", "S1", "S2"},
            // All replicas pressured -> failover finds no healthy peer.
            /*pending=*/{(kSoft + kHard) / 2,
                          (kSoft + kHard) / 2,
                          (kSoft + kHard) / 2},
            kSoft, kHard,
            /*degradedBudget=*/0,
            budget);

    BOOST_CHECK(out.code == ErrorCode::CompactionStallExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.CompactionPressureObserved(), 1u);
    BOOST_CHECK_EQUAL(client.WriteAcceptedDegraded(),      0u);
    BOOST_CHECK_EQUAL(client.WriteRejectedHardLimit(),     0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        1u);

    // Wiring smoke: CompactionStallExceeded is well-formed and
    // distinct from sibling TiKV error codes.
    BOOST_CHECK(static_cast<int>(ErrorCode::CompactionStallExceeded)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::CompactionStallExceeded)
                != static_cast<int>(ErrorCode::WriteStall));
    BOOST_CHECK(static_cast<int>(ErrorCode::CompactionStallExceeded)
                != static_cast<int>(ErrorCode::SlowDiskBudgetExceeded));
    BOOST_CHECK(static_cast<int>(ErrorCode::CompactionStallExceeded)
                != static_cast<int>(ErrorCode::DiskFull));
    BOOST_CHECK(static_cast<int>(ErrorCode::CompactionStallExceeded)
                != static_cast<int>(ErrorCode::StoreUnavailable));
}

// ---------------------------------------------------------------------------
// 5) Hard-limit rejection. pending >= hard on primary, all peers
//    also at hard -> ++m_writeRejectedHardLimit + fail-fast with
//    ErrorCode::CompactionStallExceeded.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(HardLimitRejection)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_ROCKSDB_COMPACTION_STALL", "1");

    TikvRocksdbCompactionStallGated::Config cfg;
    cfg.per_replica_timeout = ms(50);

    std::atomic<int> calls{0};
    TikvRocksdbCompactionStallGated client(cfg,
        [&](const std::string&, ms) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithCompactionGuard(
            {"S0", "S1", "S2"},
            /*pending=*/{kHard * 2, kHard * 2, kHard * 2},
            kSoft, kHard,
            /*degradedBudget=*/8,
            budget);

    BOOST_CHECK(out.code == ErrorCode::CompactionStallExceeded);
    BOOST_CHECK(out.injected);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.CompactionPressureObserved(), 0u);
    BOOST_CHECK_EQUAL(client.WriteAcceptedDegraded(),      0u);
    BOOST_CHECK_EQUAL(client.WriteRejectedHardLimit(),     1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),        1u);
}

BOOST_AUTO_TEST_SUITE_END()
