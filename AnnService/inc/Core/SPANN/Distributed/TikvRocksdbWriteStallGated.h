// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-rocksdb-write-stall
//
// Goal: RocksDB inside TiKV enters write-stall (back-pressure when
// L0 file count or pending-compaction-bytes cross the slowdown /
// stop-writes triggers). Writes don't fail outright but are
// throttled and may surface as `ServerIsBusy`. The wrapper must
// OBSERVE the stall, back off and retry within the caller's retry
// budget, and either ride out the stall (writes resume once
// back-pressure clears) or fail fast with ErrorCode::WriteStall
// rather than block the caller indefinitely.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_ROCKSDB_WRITE_STALL` unset/0):
//     wrapper is dormant -- forwards the write to the first
//     replica in the input list, accepts its response, no counter
//     moves.
//   * env-armed: drive the primary (replicas[0]). The caller-
//     supplied per-replica `stallTokenAvailability[i]` is the
//     number of retries that must elapse before the replica's
//     stall clears:
//       - tokens > 0: ++m_writeStallObserved (once per attempt),
//         consume a retry budget tick, ++m_retryAfterBackoff,
//         decrement tokens, loop.
//       - tokens == 0: drive the inner write; on Success
//         ++m_writeSucceededAfterStall, return Success.
//     If the retry budget exhausts before tokens reach 0:
//     ++m_clientErrorReturned and return ErrorCode::WriteStall.
//
// Counters required by case-md:
//   - m_writeStallObserved
//   - m_retryAfterBackoff
//   - m_writeSucceededAfterStall
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

/// One outcome of a gated write-stall write.
struct TikvWriteStallOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected             = false; // wrapper actively gated this call
    std::size_t stall_seen    = 0;     // per-attempt stalls observed
    std::size_t retries       = 0;     // backoff retries spent
    std::string served_by;             // replica that accepted the write
    int attempted_replicas    = 0;     // inner callable invocations
};

class TikvRocksdbWriteStallGated {
public:
    struct Config {
        std::chrono::milliseconds per_replica_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Inner per-replica write. The wrapper hands the callable a
    /// replica address and a deadline; the callable returns
    /// Success once the stall has cleared.
    using ReplicaWrite = std::function<ErrorCode(const std::string& replicaAddr,
                                                 std::chrono::milliseconds deadline)>;

    TikvRocksdbWriteStallGated(Config cfg, ReplicaWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_ROCKSDB_WRITE_STALL");
        return v && *v && std::string(v) != "0";
    }

    /// Write to a replica set with RocksDB write-stall detection.
    /// stallTokenAvailability[i] is the number of retries that
    /// must elapse before replica i's stall clears (0 == clear).
    TikvWriteStallOutcome WriteWithStallGuard(
            const std::vector<std::string>& replicas,
            std::vector<int> stallTokenAvailability,
            Helper::RetryBudget& budget) {
        TikvWriteStallOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        // Env-off: pass-through against the primary, no gating.
        if (!envArmed) {
            if (replicas.empty()) {
                o.code = ErrorCode::WriteStall;
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
            o.code = ErrorCode::WriteStall;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        const std::string& primary = replicas[0];
        int tokens = stallTokenAvailability.empty() ? 0
                                                    : stallTokenAvailability[0];

        while (true) {
            if (!budget.should_retry() || budget.remaining().count() <= 0) {
                ++m_clientErrorReturned;
                o.code = ErrorCode::WriteStall;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            if (tokens > 0) {
                if (o.stall_seen == 0) ++m_writeStallObserved;
                ++o.stall_seen;
                ++m_retryAfterBackoff;
                ++o.retries;
                --tokens;
                (void)budget.record_attempt();
                continue;
            }
            auto rem = budget.remaining();
            auto deadline = std::min<std::chrono::milliseconds>(
                m_cfg.per_replica_timeout, rem);
            ++o.attempted_replicas;
            ErrorCode rc = m_write(primary, deadline);
            if (rc == ErrorCode::Success) {
                ++m_writeSucceededAfterStall;
                o.code = ErrorCode::Success;
                o.served_by = primary;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            (void)budget.record_attempt();
            ++m_clientErrorReturned;
            o.code = ErrorCode::WriteStall;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }
    }

    // -- counters -----------------------------------------------------

    std::uint64_t WriteStallObserved() const {
        return m_writeStallObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t RetryAfterBackoff() const {
        return m_retryAfterBackoff.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteSucceededAfterStall() const {
        return m_writeSucceededAfterStall.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config       m_cfg;
    ReplicaWrite m_write;

    std::atomic<std::uint64_t> m_writeStallObserved{0};
    std::atomic<std::uint64_t> m_retryAfterBackoff{0};
    std::atomic<std::uint64_t> m_writeSucceededAfterStall{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
