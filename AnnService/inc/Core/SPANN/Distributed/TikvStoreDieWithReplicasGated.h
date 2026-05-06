// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-store-die-with-replicas
//
// Goal: when a single TiKV store dies mid-write while the region's
//       remaining replicas (>=2) are still healthy, the compute side
//       must (a) detect the dead store, (b) failover to a surviving
//       replica, (c) admit the write on the alternate, all bounded by
//       Helper::RetryBudget. If every replica is unreachable (or the
//       retry budget is exhausted before any alternate accepts), the
//       client sees ErrorCode::StoreUnavailable -- never a hang and
//       never a silent drop.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_STORE_DIE_WITH_REPLICAS` unset/0):
//     wrapper is dormant -- forwards the per-replica write attempts in
//     order and never increments any counter beyond what the underlying
//     callable returns. (The "primary succeeded" path bumps NO
//     counters; we only count failover events.)
//   * env-armed: while the wrapper is "armed" (Arm() called once,
//     consumed by the next call), the very next WriteToReplicaSet()
//     synthesises a primary-store death BEFORE the inner callable
//     fires for the primary:
//       1. m_storeDied++ (we injected one store death).
//       2. The wrapper proceeds to the next replica in the input list,
//          invoking the per-replica callable for each alternate in
//          turn, bounded by Helper::RetryBudget. Each alternate that
//          fails consumes one record_attempt().
//       3. On the first alternate that returns Success:
//          m_replicaFailover++ and m_writeSucceededOnReplica++; return
//          ErrorCode::Success.
//       4. If every alternate fails OR the retry budget is exhausted
//          before any alternate accepts: m_clientErrorReturned++ and
//          return ErrorCode::StoreUnavailable.
//
// Counters required by case-md:
//   - m_storeDied
//   - m_replicaFailover
//   - m_writeSucceededOnReplica
//   - m_clientErrorReturned
//
// This header is dependency-light by design: callers inject the
// per-replica write via std::function so prod code (TiKVIO::Put /
// ExtraTiKVController write loop) and the in-process fault-case test
// can both drive it.

#pragma once

#include "inc/Core/Common.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <string>
#include <vector>

namespace SPTAG::SPANN::Distributed {

/// One outcome of a gated replica-set write.
struct TikvStoreDieWithReplicasOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected     = false;  // a primary store death was injected
    bool failedOver   = false;  // an alternate replica answered
    std::string served_by;      // replica address that ultimately accepted
    int attempted_replicas = 0; // number of replicas the wrapper actually
                                // invoked the inner callable for
};

class TikvStoreDieWithReplicasGated {
public:
    struct Config {
        std::chrono::milliseconds per_replica_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Per-replica write attempt. Returns Success on durable accept or
    /// any other ErrorCode on failure / timeout / store-unreachable.
    using ReplicaWrite = std::function<ErrorCode(const std::string& replicaAddr,
                                                 std::chrono::milliseconds deadline)>;

    TikvStoreDieWithReplicasGated(Config cfg, ReplicaWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_STORE_DIE_WITH_REPLICAS");
        return v && *v && std::string(v) != "0";
    }

    /// Arm the next call to inject a primary-store death. Idempotent
    /// while armed; consumed by the next WriteToReplicaSet().
    void Arm() { m_armed.store(true, std::memory_order_release); }
    bool IsArmed() const { return m_armed.load(std::memory_order_acquire); }

    /// Issue a write against a replica set. `replicas[0]` is the
    /// primary; `replicas[1..]` are the surviving alternates in the
    /// caller-preferred fallback order. Caller must supply >=1 replica.
    ///
    /// Behaviour:
    ///   * env-off OR env-armed-but-not-Arm()ed: try replicas in order,
    ///     stopping on first Success. No counters move on the
    ///     "primary succeeded" path. If every replica fails,
    ///     m_clientErrorReturned++ and return StoreUnavailable.
    ///   * env-armed + Arm(): the primary is synthetically declared
    ///     dead (no inner call is made for replicas[0]); m_storeDied++.
    ///     Then alternates are attempted in order, bounded by
    ///     Helper::RetryBudget. First alternate Success ->
    ///     m_replicaFailover++ and m_writeSucceededOnReplica++ and
    ///     return Success. Otherwise m_clientErrorReturned++ and
    ///     return StoreUnavailable.
    TikvStoreDieWithReplicasOutcome WriteToReplicaSet(
            const std::vector<std::string>& replicas,
            Helper::RetryBudget& budget) {
        TikvStoreDieWithReplicasOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        if (replicas.empty()) {
            ++m_clientErrorReturned;
            o.code = ErrorCode::StoreUnavailable;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        const bool envArmed = EnvArmed();
        const bool fireInjection =
            envArmed && m_armed.exchange(false, std::memory_order_acq_rel);

        std::size_t startIdx = 0;
        if (fireInjection) {
            // Synthesise primary store death without firing the inner
            // write for the primary -- the store is "gone".
            ++m_storeDied;
            o.injected = true;
            startIdx = 1;
            // The primary failure consumes one retry attempt against
            // the shared budget (mirrors the prod path: a failed RPC to
            // the primary records one attempt).
            (void)budget.record_attempt();
        }

        for (std::size_t i = startIdx; i < replicas.size(); ++i) {
            if (!budget.should_retry() || budget.remaining().count() <= 0) {
                break;
            }
            auto rem = budget.remaining();
            auto deadline = std::min<std::chrono::milliseconds>(
                m_cfg.per_replica_timeout, rem);
            ++o.attempted_replicas;
            ErrorCode rc = m_write(replicas[i], deadline);
            if (rc == ErrorCode::Success) {
                o.code = ErrorCode::Success;
                o.served_by = replicas[i];
                if (fireInjection || i > 0) {
                    ++m_replicaFailover;
                    ++m_writeSucceededOnReplica;
                    o.failedOver = true;
                }
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            // Replica failed -- consume one budget attempt before the
            // next alternate. record_attempt() also enforces wall.
            (void)budget.record_attempt();
        }

        // Either every replica failed or budget exhausted before any
        // alternate accepted -- defined client error.
        ++m_clientErrorReturned;
        o.code = ErrorCode::StoreUnavailable;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    // -- counters -----------------------------------------------------

    std::uint64_t StoreDied() const {
        return m_storeDied.load(std::memory_order_relaxed);
    }
    std::uint64_t ReplicaFailover() const {
        return m_replicaFailover.load(std::memory_order_relaxed);
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

    std::atomic<bool> m_armed{false};

    std::atomic<std::uint64_t> m_storeDied{0};
    std::atomic<std::uint64_t> m_replicaFailover{0};
    std::atomic<std::uint64_t> m_writeSucceededOnReplica{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
