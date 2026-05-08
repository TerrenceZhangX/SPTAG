// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-disk-full
//
// Goal: a TiKV store's local disk has reached capacity. A subsequent
// write either blocks indefinitely (the store keeps the request in
// its write-batch waiting for free space) or, worse, is silently
// dropped at the storage engine layer. The wrapper must SURFACE the
// disk-full condition immediately, attempt a FAILOVER to a replica
// that still has free capacity, and -- if every replica is also
// disk-full -- return ErrorCode::DiskFull so the caller can decide
// whether to back off, throttle, or escalate.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_DISK_FULL` unset/0): wrapper is
//     dormant -- forwards the write to the first replica in the
//     input list, accepts its response, no counter moves.
//   * env-armed: for each replica, compare freeBytesPerReplica[i]
//     against payload_bytes:
//       - if freeBytes < payload_bytes: ++m_diskFullObserved, skip
//         to the next replica (one budget attempt consumed).
//       - if freeBytes >= payload_bytes: drive the inner write
//         against this replica. On Success ++m_writeSucceededOnReplica
//         and, if this was not the primary (i > 0),
//         ++m_failoverToHealthyReplica.
//     If every replica is disk-full or the budget exhausts before any
//     replica accepts -> ++m_clientErrorReturned and return
//     ErrorCode::DiskFull.
//
// Counters required by case-md:
//   - m_diskFullObserved
//   - m_failoverToHealthyReplica
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

/// One outcome of a gated disk-full write.
struct TikvDiskFullOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected             = false; // wrapper actively gated this call
    bool failed_over          = false; // a non-primary replica served it
    std::size_t disk_full_seen = 0;    // replicas observed disk-full
    std::string served_by;             // replica that accepted the write
    int attempted_replicas    = 0;     // inner callable invocations
};

class TikvDiskFullGated {
public:
    struct Config {
        std::chrono::milliseconds per_replica_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Inner per-replica write. The wrapper hands the callable a
    /// replica address, the payload size, and a deadline; the
    /// callable returns Success or any per-replica error.
    using ReplicaWrite = std::function<ErrorCode(const std::string& replicaAddr,
                                                 std::size_t payloadBytes,
                                                 std::chrono::milliseconds deadline)>;

    TikvDiskFullGated(Config cfg, ReplicaWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_DISK_FULL");
        return v && *v && std::string(v) != "0";
    }

    /// Write to a replica set with disk-full detection. The caller
    /// supplies the observed free bytes for each replica (e.g. from
    /// the most recent store heartbeat). Replicas whose free space
    /// is < payload_bytes are skipped and counted disk-full; the
    /// wrapper falls through to the next replica with capacity.
    TikvDiskFullOutcome WriteWithDiskGuard(
            const std::vector<std::string>& replicas,
            std::size_t payload_bytes,
            const std::vector<std::size_t>& freeBytesPerReplica,
            Helper::RetryBudget& budget) {
        TikvDiskFullOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        // Env-off: pass-through against the primary, no gating.
        if (!envArmed) {
            if (replicas.empty()) {
                o.code = ErrorCode::DiskFull;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            ++o.attempted_replicas;
            ErrorCode rc = m_write(replicas[0], payload_bytes,
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
            const std::size_t freeBytes =
                (i < freeBytesPerReplica.size()) ? freeBytesPerReplica[i] : 0;
            if (freeBytes < payload_bytes) {
                ++m_diskFullObserved;
                ++o.disk_full_seen;
                (void)budget.record_attempt();
                continue;
            }
            auto rem = budget.remaining();
            auto deadline = std::min<std::chrono::milliseconds>(
                m_cfg.per_replica_timeout, rem);
            ++o.attempted_replicas;
            ErrorCode rc = m_write(replicas[i], payload_bytes, deadline);
            if (rc == ErrorCode::Success) {
                ++m_writeSucceededOnReplica;
                if (i > 0) {
                    ++m_failoverToHealthyReplica;
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
        o.code = ErrorCode::DiskFull;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    // -- counters -----------------------------------------------------

    std::uint64_t DiskFullObserved() const {
        return m_diskFullObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t FailoverToHealthyReplica() const {
        return m_failoverToHealthyReplica.load(std::memory_order_relaxed);
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

    std::atomic<std::uint64_t> m_diskFullObserved{0};
    std::atomic<std::uint64_t> m_failoverToHealthyReplica{0};
    std::atomic<std::uint64_t> m_writeSucceededOnReplica{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
