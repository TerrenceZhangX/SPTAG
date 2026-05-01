// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   pd-leader-failover
//
// Goal: when the PD leader steps down, a new leader is elected within
//       seconds; TiKV/compute clients tolerate brief Unavailable on
//       in-flight PD calls by re-resolving the fresh leader (via the
//       existing PD member list), retrying the call against the new
//       leader bounded by Helper::RetryBudget, and refreshing the
//       region-cache TTL on successful reconnect.
//
// Contract:
//   * env-off (`SPTAG_FAULT_PD_LEADER_FAILOVER` unset/0):
//     wrapper is dormant -- forwards the inner PD RPC unchanged
//     against the current leader and never increments any counter.
//   * env-armed: while the wrapper is "armed" (Arm() called once,
//     consumed by the next call), the very next call to the PD
//     leader returns gRPC Unavailable / dead-leader synthetically
//     before the inner RPC fires:
//       1. m_pdLeaderStepdown++ (we injected a stepdown).
//       2. Re-resolve leader via ResolveLeader() callback (uses the
//          existing PD leader-discovery surface).
//       3. Bounded retry against the fresh leader, gated by the
//          shared Helper::RetryBudget.
//       4. On retry Success: m_pdReconnected++ and
//          m_regionCacheRefreshed++ (the wrapper calls the
//          RefreshRegionCache() callback to bump TTL on the region
//          cache, since a leader change can shift region leaders).
//       5. On retry budget exhausted (or fresh leader still
//          Unavailable): m_clientErrorReturned++ and return
//          ErrorCode::RetryBudgetExceeded.
//
// Counters required by case-md:
//   - m_pdLeaderStepdown
//   - m_pdReconnected
//   - m_regionCacheRefreshed
//   - m_clientErrorReturned
//
// This header is dependency-light by design: callers inject the
// PD RPC, leader-resolver, and region-cache-refresh by std::function
// so prod code (TiKVIO / ExtraTiKVController) and the in-process
// fault-case test can both drive it.

#pragma once

#include "inc/Core/Common.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <string>

namespace SPTAG::SPANN::Distributed {

/// One outcome of a gated PD call.
struct PdLeaderFailoverOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected     = false;  // a stepdown was synthetically injected
    bool reconnected  = false;  // a reconnect to a fresh leader succeeded
    bool refreshed    = false;  // the region cache refresh callback fired
    std::string served_by;      // PD address that ultimately answered
};

class PdLeaderFailoverGated {
public:
    struct Config {
        std::chrono::milliseconds per_call_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Inner PD RPC. Returns Success on timely completion or any
    /// other ErrorCode on failure / timeout / Unavailable.
    using PdRpc = std::function<ErrorCode(const std::string& leaderAddr,
                                          std::chrono::milliseconds deadline)>;
    /// Re-resolve the current PD leader. Returns the fresh leader
    /// address (post-failover) or empty string if none is known.
    using ResolveLeader = std::function<std::string()>;
    /// Refresh the region-cache TTL after a leader reconnect.
    using RefreshRegionCache = std::function<void()>;

    PdLeaderFailoverGated(Config cfg,
                          PdRpc rpc,
                          ResolveLeader resolver,
                          RefreshRegionCache refresher,
                          std::string initial_leader)
        : m_cfg(std::move(cfg)),
          m_rpc(std::move(rpc)),
          m_resolver(std::move(resolver)),
          m_refresher(std::move(refresher)),
          m_currentLeader(std::move(initial_leader)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_PD_LEADER_FAILOVER");
        return v && *v && std::string(v) != "0";
    }

    /// Arm the next call to inject a leader-stepdown. Idempotent
    /// while armed; consumed by the next Call(). Allows tests to
    /// drive multiple sequential armings (HarnessSmoke).
    void Arm() { m_armed.store(true, std::memory_order_release); }
    bool IsArmed() const { return m_armed.load(std::memory_order_acquire); }

    /// Issue a PD call. If env-armed AND Arm() was called, the next
    /// call synthesises a leader-stepdown, re-resolves, retries
    /// against the fresh leader, refreshes the region-cache TTL on
    /// success, and surfaces RetryBudgetExceeded only if the retry
    /// budget is exhausted before reconnecting.
    PdLeaderFailoverOutcome Call(Helper::RetryBudget& budget) {
        PdLeaderFailoverOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();
        const bool fireInjection =
            envArmed && m_armed.exchange(false, std::memory_order_acq_rel);

        if (!fireInjection) {
            // Pass-through: env-off OR env-armed-but-not-Arm()ed.
            auto rem = budget.remaining();
            auto deadline = std::min<std::chrono::milliseconds>(
                m_cfg.per_call_timeout, rem);
            if (deadline.count() <= 0) {
                ++m_clientErrorReturned;
                o.code = ErrorCode::RetryBudgetExceeded;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            o.code = m_rpc(m_currentLeader, deadline);
            if (o.code == ErrorCode::Success) o.served_by = m_currentLeader;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // Injection: synthesise leader stepdown without firing the
        // inner RPC (the previous leader is "gone").
        ++m_pdLeaderStepdown;
        o.injected = true;
        (void)budget.record_attempt();

        // Re-resolve the current leader via the discovery surface.
        std::string fresh = m_resolver ? m_resolver() : std::string{};
        if (fresh.empty() || !budget.should_retry()
            || budget.remaining().count() <= 0) {
            ++m_clientErrorReturned;
            o.code = ErrorCode::RetryBudgetExceeded;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }
        m_currentLeader = fresh;

        auto rem = budget.remaining();
        auto deadline = std::min<std::chrono::milliseconds>(
            m_cfg.per_call_timeout, rem);
        ErrorCode rc = m_rpc(fresh, deadline);
        if (rc == ErrorCode::Success) {
            ++m_pdReconnected;
            o.reconnected = true;
            // Refresh region cache TTL since region leaders can have
            // shifted alongside the PD leader change.
            if (m_refresher) {
                m_refresher();
                ++m_regionCacheRefreshed;
                o.refreshed = true;
            }
            o.code = ErrorCode::Success;
            o.served_by = fresh;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // Fresh leader still Unavailable -> defined client error.
        ++m_clientErrorReturned;
        o.code = ErrorCode::RetryBudgetExceeded;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    // -- counters -----------------------------------------------------

    std::uint64_t PdLeaderStepdown() const {
        return m_pdLeaderStepdown.load(std::memory_order_relaxed);
    }
    std::uint64_t PdReconnected() const {
        return m_pdReconnected.load(std::memory_order_relaxed);
    }
    std::uint64_t RegionCacheRefreshed() const {
        return m_regionCacheRefreshed.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const std::string& CurrentLeader() const { return m_currentLeader; }
    const Config& config() const { return m_cfg; }

private:
    Config             m_cfg;
    PdRpc              m_rpc;
    ResolveLeader      m_resolver;
    RefreshRegionCache m_refresher;
    std::string        m_currentLeader;

    std::atomic<bool> m_armed{false};

    std::atomic<std::uint64_t> m_pdLeaderStepdown{0};
    std::atomic<std::uint64_t> m_pdReconnected{0};
    std::atomic<std::uint64_t> m_regionCacheRefreshed{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
