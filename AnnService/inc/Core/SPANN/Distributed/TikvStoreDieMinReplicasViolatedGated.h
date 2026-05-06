// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-store-die-min-replicas-violated
//
// Goal: distinct from the sibling tikv-store-die-with-replicas case
//       (which models a SINGLE store dying while quorum survives), this
//       case fires when the number of dying stores in a region is
//       LARGE ENOUGH to drop live_replica_count below the
//       min-replicas durability threshold. Even if some replicas
//       remain live, the wrapper must REJECT the write and surface
//       ErrorCode::MinReplicasViolated -- never silently degrade
//       durability by writing to a sub-quorum.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_STORE_DIE_MIN_REPLICAS_VIOLATED`
//     unset/0): wrapper is dormant -- forwards the per-replica write
//     attempts in order, no counter moves on the happy path.
//   * env-armed: while the wrapper is "armed" via Arm(deadStores)
//     (idempotent until consumed), the next WriteToReplicaSet() call
//     synthesises death of the first `deadStores` replicas in the
//     input list:
//       1. m_storeDied += deadStores (one per simulated death).
//       2. live_count = replicas.size() - deadStores.
//       3. If live_count < minReplicas:
//            m_minReplicasViolated++; m_writeRejected++;
//            m_clientErrorReturned++; return MinReplicasViolated.
//          (No write is admitted on any surviving live replica --
//          the durability invariant takes precedence.)
//       4. Else: invoke the inner write callable for the live
//          replicas in order, bounded by Helper::RetryBudget.
//          First Success on a live replica returns Success. If every
//          live replica also fails or budget exhausts before any
//          accepts, m_clientErrorReturned++ and return
//          StoreUnavailable (a separate, sibling error).
//
// Counters required by case-md:
//   - m_storeDied
//   - m_minReplicasViolated
//   - m_writeRejected
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

/// One outcome of a gated min-replicas-violation write.
struct TikvStoreDieMinReplicasViolatedOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected           = false;  // at least one synthetic store death fired
    bool rejected           = false;  // wrapper rejected the write pre-RPC
    std::size_t dead_count  = 0;      // synthetic dead replicas
    std::size_t live_count  = 0;      // surviving replicas
    std::string served_by;            // replica that admitted the write (if any)
    int attempted_replicas  = 0;      // inner callable invocations
};

class TikvStoreDieMinReplicasViolatedGated {
public:
    struct Config {
        std::chrono::milliseconds per_replica_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    using ReplicaWrite = std::function<ErrorCode(const std::string& replicaAddr,
                                                 std::chrono::milliseconds deadline)>;

    TikvStoreDieMinReplicasViolatedGated(Config cfg, ReplicaWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_STORE_DIE_MIN_REPLICAS_VIOLATED");
        return v && *v && std::string(v) != "0";
    }

    /// Arm the next call to synthesise `deadStores` simultaneous store
    /// deaths from the head of the replica list. Consumed by the next
    /// WriteToReplicaSet().
    void Arm(std::size_t deadStores) {
        m_armedDead.store(deadStores, std::memory_order_release);
        m_armed.store(true, std::memory_order_release);
    }
    bool IsArmed() const { return m_armed.load(std::memory_order_acquire); }

    /// Issue a write to a replica set with a min-replicas durability
    /// invariant. `replicas` is the ordered replica list; `minReplicas`
    /// is the durability floor (region must have at least this many
    /// live replicas to admit a write).
    TikvStoreDieMinReplicasViolatedOutcome WriteToReplicaSet(
            const std::vector<std::string>& replicas,
            std::size_t minReplicas,
            Helper::RetryBudget& budget) {
        TikvStoreDieMinReplicasViolatedOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();
        const bool fireInjection =
            envArmed && m_armed.exchange(false, std::memory_order_acq_rel);

        std::size_t deadStores = 0;
        if (fireInjection) {
            deadStores = m_armedDead.exchange(0, std::memory_order_acq_rel);
            if (deadStores > replicas.size()) deadStores = replicas.size();
            for (std::size_t i = 0; i < deadStores; ++i) {
                ++m_storeDied;
                // Each dead-store RPC failure mirrors one budget attempt.
                (void)budget.record_attempt();
            }
            o.injected = (deadStores > 0);
            o.dead_count = deadStores;
        }

        const std::size_t total = replicas.size();
        const std::size_t live  = (deadStores >= total) ? 0
                                                        : (total - deadStores);
        o.live_count = live;

        // Durability invariant: if too few live replicas remain,
        // reject the write before touching any surviving store.
        if (live < minReplicas) {
            ++m_minReplicasViolated;
            ++m_writeRejected;
            ++m_clientErrorReturned;
            o.rejected = true;
            o.code = ErrorCode::MinReplicasViolated;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // Enough live replicas remain -- write to them in order
        // (skipping the synthesised dead head), bounded by budget.
        for (std::size_t i = deadStores; i < total; ++i) {
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
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            (void)budget.record_attempt();
        }

        // Every live replica also failed or budget exhausted before
        // any accepted -- a separate, sibling client error.
        ++m_clientErrorReturned;
        o.code = ErrorCode::StoreUnavailable;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    // -- counters -----------------------------------------------------

    std::uint64_t StoreDied() const {
        return m_storeDied.load(std::memory_order_relaxed);
    }
    std::uint64_t MinReplicasViolated() const {
        return m_minReplicasViolated.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteRejected() const {
        return m_writeRejected.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config       m_cfg;
    ReplicaWrite m_write;

    std::atomic<bool>        m_armed{false};
    std::atomic<std::size_t> m_armedDead{0};

    std::atomic<std::uint64_t> m_storeDied{0};
    std::atomic<std::uint64_t> m_minReplicasViolated{0};
    std::atomic<std::uint64_t> m_writeRejected{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
