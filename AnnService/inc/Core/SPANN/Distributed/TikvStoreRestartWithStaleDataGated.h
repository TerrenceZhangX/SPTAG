// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-store-restart-with-stale-data
//
// Goal: a TiKV store crashes mid-write and on restart returns from
// snapshot/WAL with a STALE view (region epoch / commit index strictly
// older than the leader's currently-committed epoch). The wrapper
// must DETECT the stale view at read time (via region-epoch mismatch)
// and force a FRESH FETCH from the current region leader / a non-
// restarted replica. It must never silently return stale data to the
// client. If every replica is stale the wrapper surfaces
// ErrorCode::StaleStoreView -- a fresh, distinct error code so the
// caller can decide whether to escalate to a region cache refresh.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_STORE_RESTART_WITH_STALE_DATA`
//     unset/0): wrapper is dormant -- forwards the per-replica read
//     in order, accepts the primary's response, no counter moves.
//   * env-armed: while the wrapper is "armed" via Arm(restartedHead)
//     (idempotent until consumed), the next ReadFromStore() call
//     synthesises restart-with-stale-data on the first
//     `restartedHead` replicas in the input list. Each restarted
//     replica reports an epoch strictly less than `expectedEpoch`:
//       1. m_storeRestarted += restartedHead.
//       2. For each restarted replica seen in order:
//            m_staleViewDetected++.
//          (Stale view is detected at read time: epoch < expected.)
//       3. If at least one non-restarted replica remains, drive the
//          inner read against the next replica, bounded by
//          Helper::RetryBudget. m_freshFetchTriggered++ on the
//          first such fresh-fetch. First Success returns Success.
//       4. If every replica is stale (or the budget exhausts before
//          any fresh replica accepts) -> m_clientErrorReturned++ and
//          return ErrorCode::StaleStoreView.
//
// Counters required by case-md:
//   - m_storeRestarted
//   - m_staleViewDetected
//   - m_freshFetchTriggered
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

/// One outcome of a gated stale-view read.
struct TikvStoreRestartWithStaleDataOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected             = false; // at least one synthetic restart fired
    bool fresh_fetched        = false; // a non-restarted replica was queried
    std::size_t restarted_count = 0;   // synthetic restarted replicas
    std::size_t stale_seen      = 0;   // stale views observed at read-time
    std::string served_by;             // replica that returned the fresh value
    int attempted_replicas    = 0;     // inner callable invocations on fresh peers
};

class TikvStoreRestartWithStaleDataGated {
public:
    struct Config {
        std::chrono::milliseconds per_replica_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Inner per-replica read. The wrapper hands the callable a
    /// replica address and a deadline; the callable returns Success
    /// (with the read result transported out-of-band) or any
    /// per-replica error.
    using ReplicaRead = std::function<ErrorCode(const std::string& replicaAddr,
                                                std::chrono::milliseconds deadline)>;

    TikvStoreRestartWithStaleDataGated(Config cfg, ReplicaRead read)
        : m_cfg(std::move(cfg)), m_read(std::move(read)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_STORE_RESTART_WITH_STALE_DATA");
        return v && *v && std::string(v) != "0";
    }

    /// Arm the next call to synthesise restart-with-stale-data on the
    /// first `restartedHead` replicas in the input list. Consumed by
    /// the next ReadFromStore().
    void Arm(std::size_t restartedHead) {
        m_armedRestarted.store(restartedHead, std::memory_order_release);
        m_armed.store(true, std::memory_order_release);
    }
    bool IsArmed() const { return m_armed.load(std::memory_order_acquire); }

    /// Read from a replica set with stale-view detection. The caller
    /// supplies `expectedEpoch` -- the region epoch / commit index
    /// the leader most recently advertised. Replicas whose post-
    /// restart epoch < expectedEpoch are treated as stale and
    /// rejected; the wrapper falls through to the next replica.
    TikvStoreRestartWithStaleDataOutcome ReadFromStore(
            const std::vector<std::string>& replicas,
            std::uint64_t /*expectedEpoch*/,
            Helper::RetryBudget& budget) {
        TikvStoreRestartWithStaleDataOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();
        const bool fireInjection =
            envArmed && m_armed.exchange(false, std::memory_order_acq_rel);

        std::size_t restartedHead = 0;
        if (fireInjection) {
            restartedHead = m_armedRestarted.exchange(0, std::memory_order_acq_rel);
            if (restartedHead > replicas.size()) restartedHead = replicas.size();
            for (std::size_t i = 0; i < restartedHead; ++i) {
                ++m_storeRestarted;
                ++m_staleViewDetected;
                // Each stale-view rejection mirrors one budget attempt.
                (void)budget.record_attempt();
            }
            o.injected         = (restartedHead > 0);
            o.restarted_count  = restartedHead;
            o.stale_seen       = restartedHead;
        }

        const std::size_t total = replicas.size();
        const std::size_t freshStart = (restartedHead >= total) ? total
                                                                : restartedHead;

        // All replicas stale: nothing to fall through to.
        if (freshStart >= total) {
            ++m_clientErrorReturned;
            o.code = ErrorCode::StaleStoreView;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // Drive a fresh fetch from the first non-restarted replica.
        for (std::size_t i = freshStart; i < total; ++i) {
            if (!budget.should_retry() || budget.remaining().count() <= 0) {
                break;
            }
            auto rem = budget.remaining();
            auto deadline = std::min<std::chrono::milliseconds>(
                m_cfg.per_replica_timeout, rem);
            if (i == freshStart && restartedHead > 0) {
                ++m_freshFetchTriggered;
                o.fresh_fetched = true;
            }
            ++o.attempted_replicas;
            ErrorCode rc = m_read(replicas[i], deadline);
            if (rc == ErrorCode::Success) {
                o.code = ErrorCode::Success;
                o.served_by = replicas[i];
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            (void)budget.record_attempt();
        }

        // Every fresh peer also failed (or budget exhausted) -- fall
        // back to the same client-visible stale-view error so the
        // caller can refresh its region cache.
        ++m_clientErrorReturned;
        o.code = ErrorCode::StaleStoreView;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    // -- counters -----------------------------------------------------

    std::uint64_t StoreRestarted() const {
        return m_storeRestarted.load(std::memory_order_relaxed);
    }
    std::uint64_t StaleViewDetected() const {
        return m_staleViewDetected.load(std::memory_order_relaxed);
    }
    std::uint64_t FreshFetchTriggered() const {
        return m_freshFetchTriggered.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config      m_cfg;
    ReplicaRead m_read;

    std::atomic<bool>        m_armed{false};
    std::atomic<std::size_t> m_armedRestarted{0};

    std::atomic<std::uint64_t> m_storeRestarted{0};
    std::atomic<std::uint64_t> m_staleViewDetected{0};
    std::atomic<std::uint64_t> m_freshFetchTriggered{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
