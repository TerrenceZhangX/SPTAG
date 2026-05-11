// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-rocksdb-compaction-stall
//
// Goal: RocksDB compaction falls behind. pending-compaction-bytes
// grows past the soft-limit: writes are still accepted but read
// amplification grows and tail latency spikes (degraded mode).
// Once pending crosses the hard-limit, RocksDB triggers a full
// write stop and writes must fail fast. The wrapper provides a
// budget-aware compaction-pressure probe before every write.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_ROCKSDB_COMPACTION_STALL`
//     unset/0): wrapper is dormant -- forwards the write to the
//     first replica in the input list, accepts its response,
//     no counter moves.
//   * env-armed: probe primary replica's pendingBytes vs the
//     supplied soft / hard limits:
//       - pending <= softLimit: drive the primary write, return
//         Success (no counter moves).
//       - softLimit < pending < hardLimit:
//           ++m_compactionPressureObserved. If degradedWriteBudget
//           > 0 -> ++m_writeAcceptedDegraded, consume one budget
//           tick, drive the primary write, return Success. If the
//           degraded-write budget is exhausted -> retry against
//           any replica i (i>0) whose pendingBytes < softLimit
//           within the RetryBudget. If no healthy replica is
//           reachable: ++m_clientErrorReturned, return
//           ErrorCode::CompactionStallExceeded.
//       - pending >= hardLimit:
//           ++m_writeRejectedHardLimit. Retry against any replica
//           i (i>0) with pendingBytes < softLimit within the
//           RetryBudget. If no healthy replica:
//           ++m_clientErrorReturned, return
//           ErrorCode::CompactionStallExceeded.
//
// Counters required by case-md:
//   - m_compactionPressureObserved
//   - m_writeAcceptedDegraded
//   - m_writeRejectedHardLimit
//   - m_clientErrorReturned

#pragma once

#include "inc/Core/Common.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <string>
#include <vector>

namespace SPTAG::SPANN::Distributed {

/// One outcome of a gated compaction-stall write.
struct TikvCompactionStallOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected               = false; // wrapper actively gated this call
    std::size_t pressure_seen   = 0;     // soft<pending<hard observations
    std::size_t hard_seen       = 0;     // pending>=hard observations
    std::size_t degraded_writes = 0;     // degraded-budget consumptions
    std::string served_by;               // replica that accepted the write
    int attempted_replicas      = 0;     // inner callable invocations
};

class TikvRocksdbCompactionStallGated {
public:
    struct Config {
        std::chrono::milliseconds per_replica_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    using ReplicaWrite = std::function<ErrorCode(const std::string& replicaAddr,
                                                 std::chrono::milliseconds deadline)>;

    TikvRocksdbCompactionStallGated(Config cfg, ReplicaWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_ROCKSDB_COMPACTION_STALL");
        return v && *v && std::string(v) != "0";
    }

    /// Write to a replica set with RocksDB compaction-pressure detection.
    /// pendingBytesPerReplica[i] is the current pending-compaction-bytes
    /// reported by replica i.
    TikvCompactionStallOutcome WriteWithCompactionGuard(
            const std::vector<std::string>& replicas,
            const std::vector<std::uint64_t>& pendingBytesPerReplica,
            std::uint64_t softLimitBytes,
            std::uint64_t hardLimitBytes,
            int degradedWriteBudget,
            Helper::RetryBudget& budget) {
        TikvCompactionStallOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        // Env-off: pass-through against the primary, no gating.
        if (!envArmed) {
            if (replicas.empty()) {
                o.code = ErrorCode::CompactionStallExceeded;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            ++o.attempted_replicas;
            ErrorCode rc = m_write(replicas[0], m_cfg.per_replica_timeout);
            o.code = rc;
            if (rc == ErrorCode::Success) o.served_by = replicas[0];
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        o.injected = true;

        if (replicas.empty()) {
            ++m_clientErrorReturned;
            o.code = ErrorCode::CompactionStallExceeded;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        auto pendingOf = [&](std::size_t i) -> std::uint64_t {
            return i < pendingBytesPerReplica.size()
                   ? pendingBytesPerReplica[i] : 0ULL;
        };

        const std::uint64_t primaryPending = pendingOf(0);

        // Healthy primary path: pending <= soft.
        if (primaryPending <= softLimitBytes) {
            auto rem = budget.remaining();
            auto deadline = rem.count() > 0
                ? std::min<std::chrono::milliseconds>(m_cfg.per_replica_timeout, rem)
                : m_cfg.per_replica_timeout;
            ++o.attempted_replicas;
            ErrorCode rc = m_write(replicas[0], deadline);
            o.code = rc;
            if (rc == ErrorCode::Success) o.served_by = replicas[0];
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // Degraded path: soft < pending < hard.
        if (primaryPending < hardLimitBytes) {
            ++m_compactionPressureObserved;
            ++o.pressure_seen;
            if (degradedWriteBudget > 0) {
                ++m_writeAcceptedDegraded;
                ++o.degraded_writes;
                auto rem = budget.remaining();
                auto deadline = rem.count() > 0
                    ? std::min<std::chrono::milliseconds>(m_cfg.per_replica_timeout, rem)
                    : m_cfg.per_replica_timeout;
                ++o.attempted_replicas;
                ErrorCode rc = m_write(replicas[0], deadline);
                o.code = rc;
                if (rc == ErrorCode::Success) o.served_by = replicas[0];
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            // Budget exhausted -> try a healthy non-primary replica.
        } else {
            // Hard-limit rejection path.
            ++m_writeRejectedHardLimit;
            ++o.hard_seen;
        }

        // Failover attempt: find a replica i>0 with pending < soft.
        for (std::size_t i = 1; i < replicas.size(); ++i) {
            if (!budget.should_retry() || budget.remaining().count() <= 0) {
                break;
            }
            if (pendingOf(i) >= softLimitBytes) {
                (void)budget.record_attempt();
                continue;
            }
            auto rem = budget.remaining();
            auto deadline = std::min<std::chrono::milliseconds>(
                m_cfg.per_replica_timeout, rem);
            ++o.attempted_replicas;
            ErrorCode rc = m_write(replicas[i], deadline);
            if (rc == ErrorCode::Success) {
                o.code = ErrorCode::Success;
                o.served_by = replicas[i];
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            (void)budget.record_attempt();
        }

        ++m_clientErrorReturned;
        o.code = ErrorCode::CompactionStallExceeded;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    // -- counters -----------------------------------------------------

    std::uint64_t CompactionPressureObserved() const {
        return m_compactionPressureObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteAcceptedDegraded() const {
        return m_writeAcceptedDegraded.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteRejectedHardLimit() const {
        return m_writeRejectedHardLimit.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config       m_cfg;
    ReplicaWrite m_write;

    std::atomic<std::uint64_t> m_compactionPressureObserved{0};
    std::atomic<std::uint64_t> m_writeAcceptedDegraded{0};
    std::atomic<std::uint64_t> m_writeRejectedHardLimit{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
