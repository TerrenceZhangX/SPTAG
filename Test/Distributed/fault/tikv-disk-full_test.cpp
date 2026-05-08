// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-disk-full
//
// Tier 1 HARD repro of the tikv-disk-full invariants from
// design-docs/ft-fault-cases/tikv-disk-full.md:
//
//   * env-off: wrapper dormant; primary write succeeds, no counter
//     moves;
//   * env-armed + primary has capacity: write succeeds on the
//     primary, m_writeSucceededOnReplica == 1, no failover;
//   * env-armed + primary disk-full + replica has capacity: wrapper
//     observes the disk-full primary, falls through to the next
//     replica with capacity, returns Success; m_diskFullObserved,
//     m_failoverToHealthyReplica, m_writeSucceededOnReplica all
//     bump;
//   * env-armed + every replica disk-full: wrapper returns
//     ErrorCode::DiskFull; m_clientErrorReturned == 1,
//     m_diskFullObserved == replicas.size(), no inner write fires;
//   * env-armed + retry budget exhausts before any replica accepts:
//     wrapper returns ErrorCode::DiskFull.
//
// Wrapper under test:  TikvDiskFullGated (header-only; defers to
//                      Helper::RetryBudget + injected per-replica
//                      write callable).
// Env-gate:            SPTAG_FAULT_TIKV_DISK_FULL
// Tier 2 (1M perf):    DEFERRED -- non hot-path (free-space check
//                      sits on the per-store heartbeat / write-batch
//                      admission edge, not the SPANN search inner
//                      loop or the per-RPC TiKV retry hot path).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/TikvDiskFullGated.h"
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

constexpr std::size_t kPayload = 1 << 20; // 1 MiB

}  // namespace

BOOST_AUTO_TEST_SUITE(TikvDiskFullTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With the env-gate unset the wrapper is dormant:
//    the per-replica free-space vector is irrelevant, the wrapper
//    drives the primary write, accepts Success, every counter stays
//    at zero.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_DISK_FULL", nullptr);
    BOOST_REQUIRE(!TikvDiskFullGated::EnvArmed());

    TikvDiskFullGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::vector<std::string> seen;
    std::mutex seenMu;
    TikvDiskFullGated client(cfg,
        [&](const std::string& replica, std::size_t, std::chrono::milliseconds) {
            std::lock_guard<std::mutex> lk(seenMu);
            seen.push_back(replica);
            return ErrorCode::Success;
        });

    // Even with primary "disk-full" in the free-space vector, env-off
    // must stay pass-through against the primary.
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithDiskGuard(
            {"S0", "S1", "S2"}, kPayload,
            /*free=*/{0, kPayload * 4, kPayload * 4}, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.injected);
    BOOST_CHECK(!out.failed_over);
    BOOST_CHECK_EQUAL(out.disk_full_seen, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_REQUIRE_EQUAL(seen.size(), 1u);
    BOOST_CHECK_EQUAL(seen[0], std::string("S0"));
    BOOST_CHECK_EQUAL(client.DiskFullObserved(),         0u);
    BOOST_CHECK_EQUAL(client.FailoverToHealthyReplica(), 0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),      0u);
}

// ---------------------------------------------------------------------------
// 2) Baseline primary has capacity. Env-armed but primary's free
//    space comfortably exceeds the payload: no failover, no disk-full
//    observed, write succeeds on the primary,
//    m_writeSucceededOnReplica == 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BaselinePrimaryHasCapacity)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_DISK_FULL", "1");
    BOOST_REQUIRE(TikvDiskFullGated::EnvArmed());

    TikvDiskFullGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::atomic<int> calls{0};
    TikvDiskFullGated client(cfg,
        [&](const std::string& replica, std::size_t, std::chrono::milliseconds) {
            ++calls;
            BOOST_CHECK_EQUAL(replica, std::string("S0"));
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithDiskGuard(
            {"S0", "S1", "S2"}, kPayload,
            {kPayload * 4, kPayload * 4, kPayload * 4}, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.failed_over);
    BOOST_CHECK_EQUAL(out.disk_full_seen, 0u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S0"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.DiskFullObserved(),         0u);
    BOOST_CHECK_EQUAL(client.FailoverToHealthyReplica(), 0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),      0u);
}

// ---------------------------------------------------------------------------
// 3) Primary full, failover succeeds. Env-armed + primary's free
//    space < payload, but a replica has capacity. Wrapper observes
//    the disk-full primary, falls through to the next replica which
//    succeeds. m_diskFullObserved == 1, m_failoverToHealthyReplica
//    == 1, m_writeSucceededOnReplica == 1, no client error.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(PrimaryFullFailoverSucceeds)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_DISK_FULL", "1");

    TikvDiskFullGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(100);
    cfg.budget.total_wall   = std::chrono::milliseconds(500);
    cfg.budget.max_attempts = 6;

    std::atomic<int> calls{0};
    TikvDiskFullGated client(cfg,
        [&](const std::string& replica, std::size_t, std::chrono::milliseconds) {
            ++calls;
            // Primary is disk-full; only S1/S2 should be hit by the
            // inner write.
            BOOST_CHECK(replica != std::string("S0"));
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithDiskGuard(
            {"S0", "S1", "S2"}, kPayload,
            {kPayload / 2, kPayload * 4, kPayload * 4}, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(out.failed_over);
    BOOST_CHECK_EQUAL(out.disk_full_seen, 1u);
    BOOST_CHECK_EQUAL(out.served_by, std::string("S1"));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 1);
    BOOST_CHECK_EQUAL(calls.load(), 1);

    BOOST_CHECK_EQUAL(client.DiskFullObserved(),         1u);
    BOOST_CHECK_EQUAL(client.FailoverToHealthyReplica(), 1u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(),  1u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),      0u);
}

// ---------------------------------------------------------------------------
// 4) All replicas disk-full. Every replica's free space < payload ->
//    wrapper returns ErrorCode::DiskFull; m_diskFullObserved == N,
//    m_clientErrorReturned == 1, no inner write fires.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(AllReplicasFull)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_DISK_FULL", "1");

    TikvDiskFullGated::Config cfg;
    cfg.per_replica_timeout = std::chrono::milliseconds(50);

    std::atomic<int> calls{0};
    TikvDiskFullGated client(cfg,
        [&](const std::string&, std::size_t, std::chrono::milliseconds) {
            ++calls;
            return ErrorCode::Success;
        });

    const std::size_t kN = 3;
    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithDiskGuard(
            {"S0", "S1", "S2"}, kPayload,
            {0, kPayload / 4, kPayload / 8}, budget);

    BOOST_CHECK(out.code == ErrorCode::DiskFull);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.failed_over);
    BOOST_CHECK_EQUAL(out.disk_full_seen, kN);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.DiskFullObserved(),         static_cast<std::uint64_t>(kN));
    BOOST_CHECK_EQUAL(client.FailoverToHealthyReplica(), 0u);
    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),      1u);

    // Wiring smoke: DiskFull is well-formed and distinct from sibling
    // client-visible TiKV error codes.
    BOOST_CHECK(static_cast<int>(ErrorCode::DiskFull)
                != static_cast<int>(ErrorCode::Success));
    BOOST_CHECK(static_cast<int>(ErrorCode::DiskFull)
                != static_cast<int>(ErrorCode::StoreUnavailable));
    BOOST_CHECK(static_cast<int>(ErrorCode::DiskFull)
                != static_cast<int>(ErrorCode::MinReplicasViolated));
    BOOST_CHECK(static_cast<int>(ErrorCode::DiskFull)
                != static_cast<int>(ErrorCode::StaleStoreView));
    BOOST_CHECK(static_cast<int>(ErrorCode::DiskFull)
                != static_cast<int>(ErrorCode::RetryBudgetExceeded));
}

// ---------------------------------------------------------------------------
// 5) Retry budget exhausted. Tight budget + every replica disk-full
//    -> wrapper bails out with ErrorCode::DiskFull just as in (4),
//    m_clientErrorReturned == 1.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RetryBudgetExhausted)
{
    ScopedEnv env("SPTAG_FAULT_TIKV_DISK_FULL", "1");

    TikvDiskFullGated::Config cfg;
    cfg.per_replica_timeout  = std::chrono::milliseconds(20);
    cfg.budget.total_wall    = std::chrono::milliseconds(2);
    cfg.budget.max_attempts  = 1;

    std::atomic<int> calls{0};
    TikvDiskFullGated client(cfg,
        [&](const std::string&, std::size_t, std::chrono::milliseconds) {
            ++calls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.WriteWithDiskGuard(
            {"S0", "S1", "S2"}, kPayload,
            {0, 0, 0}, budget);

    BOOST_CHECK(out.code == ErrorCode::DiskFull);
    BOOST_CHECK(out.injected);
    BOOST_CHECK(!out.failed_over);
    BOOST_CHECK_EQUAL(out.served_by, std::string(""));
    BOOST_CHECK_EQUAL(out.attempted_replicas, 0);
    BOOST_CHECK_EQUAL(calls.load(), 0);

    BOOST_CHECK_EQUAL(client.WriteSucceededOnReplica(),  0u);
    BOOST_CHECK_EQUAL(client.ClientErrorReturned(),      1u);
}

BOOST_AUTO_TEST_SUITE_END()
