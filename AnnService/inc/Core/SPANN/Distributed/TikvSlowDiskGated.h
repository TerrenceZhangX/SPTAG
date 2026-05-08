// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-slow-disk
//
// Goal: a TiKV store's local disk is degraded (high write latency,
// not full). Writes don't fail outright but blow the per-write SLO
// budget. The wrapper must OBSERVE the per-replica synthetic
// latency, count slow ops, and FAIL OVER to a healthier replica
// rather than block on a slow disk indefinitely. If every replica
// is slow and the SLO budget is exhausted, return
// ErrorCode::SlowDiskBudgetExceeded so the caller can back off /
// throttle / escalate.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_SLOW_DISK` unset/0): wrapper is
//     dormant -- forwards the write to the first replica in the
//     input list, accepts its response, no counter moves.
//   * env-armed: for each replica, the caller-supplied synthetic
//     latency `slownessMsPerReplica[i]` (no real sleep) is compared
//     against `latencyBudgetMs`:
//       - if slowness > budget: ++m_slowWriteObserved, skip to the
//         next replica (one budget attempt consumed).
//       - if slowness <= budget: drive the inner write against this
//         replica. On Success ++m_writeSucceededOnReplica and, if
//         this was not the primary (i > 0),
//         ++m_failoverToFastReplica.
//     If every replica is slow or the retry budget exhausts before
//     any replica accepts -> ++m_clientErrorReturned and return
//     ErrorCode::SlowDiskBudgetExceeded.
//
// Counters required by case-md:
//   - m_slowWriteObserved
//   - m_failoverToFastReplica
//   - m_writeSucceededOnReplica
//   - m_clientErrorReturned

#pragma once

#include "inc/Core/Common.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <string>
#include <vector>

namespace SPTAG::SPANN::Distributed {

/// One outcome of a gated slow-disk write.
struct TikvSlowDiskOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected             = false; // wrapper actively gated this call
    bool failed_over          = false; // a non-primary replica served it
    std::size_t slow_seen     = 0;     // replicas observed slow
    std::string served_by;             // replica that accepted the write
    int attempted_replicas    = 0;     // inner callable invocations
};

class TikvSlowDiskGated {
public:
    struct Config {
        std::chrono::milliseconds per_replica_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Inner per-replica write. The wrapper hands the callable a
    /// replica address, the synthetic observed latency, and a
    /// deadline; the callable returns Success or any per-replica
    /// error.
    using ReplicaWrite = std::function<ErrorCode(const std::string& replicaAddr,
                                                 std::chrono::milliseconds observedLatency,
                                                 std::chrono::milliseconds deadline)>;

    TikvSlowDiskGated(Config cfg, ReplicaWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_SLOW_DISK");
        return v && *v && std::string(v) != "0";
    }

    /// Write to a replica set with slow-disk detection. The caller
    /// supplies the synthetic per-write latency for each replica
    /// (e.g. derived from an EWMA of recent store p99). Replicas
    /// whose observed latency exceeds latencyBudgetMs are skipped
    /// and counted slow; the wrapper falls through to the next
    /// replica that fits the budget.
    TikvSlowDiskOutcome WriteWithSlowDiskGuard(
            const std::vector<std::string>& replicas,
            const std::vector<std::chrono::milliseconds>& slownessMsPerReplica,
            std::chrono::milliseconds latencyBudgetMs,
            Helper::RetryBudget& budget) {
        TikvSlowDiskOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        // Env-off: pass-through against the primary, no gating.
        if (!envArmed) {
            if (replicas.empty()) {
                o.code = ErrorCode::SlowDiskBudgetExceeded;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            ++o.attempted_replicas;
            const auto observed = !slownessMsPerReplica.empty()
                ? slownessMsPerReplica[0]
                : std::chrono::milliseconds(0);
            ErrorCode rc = m_write(replicas[0], observed,
                                   m_cfg.per_replica_timeout);
            o.code = rc;
            if (rc == ErrorCode::Success) o.served_by = replicas[0];
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        o.injected = true;

        const std::size_t total = replicas.size();
        for (std::size_t i = 0; i < total; ++i) {
            if (!budget.should_retry() || budget.remaining().count() <= 0) {
                break;
            }
            const auto slowness =
                (i < slownessMsPerReplica.size())
                    ? slownessMsPerReplica[i]
                    : std::chrono::milliseconds(0);
            if (slowness > latencyBudgetMs) {
                ++m_slowWriteObserved;
                ++o.slow_seen;
                (void)budget.record_attempt();
                continue;
            }
            auto rem = budget.remaining();
            auto deadline = std::min<std::chrono::milliseconds>(
                m_cfg.per_replica_timeout, rem);
            ++o.attempted_replicas;
            ErrorCode rc = m_write(replicas[i], slowness, deadline);
            if (rc == ErrorCode::Success) {
                ++m_writeSucceededOnReplica;
                if (i > 0) {
                    ++m_failoverToFastReplica;
                    o.failed_over = true;
                }
                o.code = ErrorCode::Success;
                o.served_by = replicas[i];
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            (void)budget.record_attempt();
        }

        ++m_clientErrorReturned;
        o.code = ErrorCode::SlowDiskBudgetExceeded;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    // -- counters -----------------------------------------------------

    std::uint64_t SlowWriteObserved() const {
        return m_slowWriteObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t FailoverToFastReplica() const {
        return m_failoverToFastReplica.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteSucceededOnReplica() const {
        return m_writeSucceededOnReplica.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config       m_cfg;
    ReplicaWrite m_write;

    std::atomic<std::uint64_t> m_slowWriteObserved{0};
    std::atomic<std::uint64_t> m_failoverToFastReplica{0};
    std::atomic<std::uint64_t> m_writeSucceededOnReplica{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
