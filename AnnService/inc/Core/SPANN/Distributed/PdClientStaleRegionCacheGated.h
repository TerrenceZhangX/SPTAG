// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   pd-client-stale-region-cache
//
// Goal: A TiKV client write resolves its target region via the
// PD-backed client-side region cache. When the cluster has
// already split/merged/transferred-leader the cached route is
// stale; the store rejects the write with a region-mismatch
// signal, the client must (a) detect the cache miss, (b) issue
// a fresh PD query to learn the actual route, and (c) bound
// the chain of stale-cache/refresh cycles by a RetryBudget so
// the client cannot loop forever masking persistent
// PD/region-cache divergence. This wrapper models the
// stale-region-cache invariant: every cache miss observed must
// be answered by one PD query, the cache must converge on the
// route that PD reports for that attempt, and the write only
// lands once the cached route matches the actual route.
//
// Distinct from pd-reconnect-during-retry: that case covers
// the PD client transport flapping (no PD answer at all).
// pd-client-stale-region-cache is the case where PD answers
// promptly but the client's local route cache is behind the
// truth. Distinct from tikv-region-error-retry-cap: that one
// caps generic region errors on a single route; this one is
// specifically the cache/PD divergence loop where the client
// keeps re-resolving until convergence.
//
// Contract:
//   * env-off (`SPTAG_FAULT_PD_CLIENT_STALE_REGION_CACHE`
//     unset/0): wrapper is dormant -- drives one pass-through
//     write against `cachedRoute`; no stale-cache counter
//     moves.
//   * env-armed: `actualRoutePerAttempt` is a vector of
//     `RouteEntry` values returned by PD on successive
//     cache-miss queries. The wrapper compares the local
//     cached route against `actualRoutePerAttempt[i]`; while
//     they diverge it ++m_staleCacheHit, consumes one budget
//     tick, ++m_pdQueryTriggered, refreshes the cache to the
//     reported route and continues. When the cached route
//     matches the reported route the wrapper attempts the
//     write and ++m_writeSucceededAfterRefresh on landing.
//     The last entry of `actualRoutePerAttempt` is the
//     converged route. If the RetryBudget is exhausted before
//     convergence ++m_clientErrorReturned and return
//     ErrorCode::StaleRegionCacheRetryCapExceeded.
//
// Counters required by case-md:
//   - m_staleCacheHit
//   - m_pdQueryTriggered
//   - m_writeSucceededAfterRefresh
//   - m_clientErrorReturned

#pragma once

#include "inc/Core/Common.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <string>
#include <vector>

namespace SPTAG::SPANN::Distributed {

/// One PD-resolved route for a region: opaque identity that
/// the wrapper compares for equality only. Tests use small
/// integers; production code would carry leader-store-id and
/// epoch.
struct RouteEntry {
    std::int32_t leader_store_id = -1;
    std::int64_t region_epoch    = -1;

    bool operator==(const RouteEntry& other) const {
        return leader_store_id == other.leader_store_id
            && region_epoch    == other.region_epoch;
    }
    bool operator!=(const RouteEntry& other) const {
        return !(*this == other);
    }
};

/// One outcome of a gated pd-client-stale-region-cache write.
struct PdClientStaleRegionCacheOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected                         = false;
    std::size_t stale_cache_hits          = 0;
    std::size_t pd_queries_triggered      = 0;
    std::size_t write_succeeded_after     = 0;
    int attempted_writes                  = 0;
    RouteEntry landed_on_route            {};
};

class PdClientStaleRegionCacheGated {
public:
    using RegionId = std::int64_t;

    struct Config {
        std::chrono::milliseconds per_write_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Per-attempt write callable: drive the write against the
    /// region currently routed to `route`. Returns Success on
    /// landing.
    using RouteWrite = std::function<ErrorCode(RegionId regionId,
                                               const RouteEntry& route,
                                               std::chrono::milliseconds deadline)>;

    PdClientStaleRegionCacheGated(Config cfg, RouteWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_PD_CLIENT_STALE_REGION_CACHE");
        return v && *v && std::string(v) != "0";
    }

    /// Drive a single write for `regionId`. The local
    /// `cachedRoute` is the route the client thinks is current;
    /// `actualRoutePerAttempt` is the per-attempt sequence of
    /// routes PD reports back on cache-miss queries. While the
    /// cached route disagrees with the PD-reported route the
    /// wrapper refreshes and consumes one budget tick;
    /// convergence triggers a write attempt. The last entry of
    /// `actualRoutePerAttempt` is treated as the converged
    /// route (the wrapper will only attempt a write once the
    /// cached route matches the actual route).
    PdClientStaleRegionCacheOutcome WriteWithStaleCacheGuard(
            RegionId regionId,
            RouteEntry cachedRoute,
            std::vector<RouteEntry> actualRoutePerAttempt,
            Helper::RetryBudget& budget) {
        PdClientStaleRegionCacheOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        if (!envArmed) {
            ++o.attempted_writes;
            ErrorCode rc = m_write(regionId, cachedRoute, m_cfg.per_write_timeout);
            if (rc == ErrorCode::Success) {
                o.landed_on_route = cachedRoute;
            } else {
                o.code = rc;
            }
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        o.injected = true;

        if (actualRoutePerAttempt.empty()) {
            // Nothing for PD to disagree about; attempt the
            // write against the cached route as-is.
            auto rem = budget.remaining();
            auto deadline = rem.count() > 0
                ? std::min<std::chrono::milliseconds>(m_cfg.per_write_timeout, rem)
                : m_cfg.per_write_timeout;
            ++o.attempted_writes;
            ErrorCode rc = m_write(regionId, cachedRoute, deadline);
            if (rc == ErrorCode::Success) {
                ++m_writeSucceededAfterRefresh;
                ++o.write_succeeded_after;
                o.landed_on_route = cachedRoute;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            ++m_clientErrorReturned;
            o.code = rc;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        for (std::size_t i = 0; i < actualRoutePerAttempt.size(); ++i) {
            const RouteEntry& actual = actualRoutePerAttempt[i];

            if (cachedRoute != actual) {
                ++m_staleCacheHit;
                ++o.stale_cache_hits;

                if (!budget.should_retry() || budget.remaining().count() <= 0) {
                    ++m_clientErrorReturned;
                    o.code = ErrorCode::StaleRegionCacheRetryCapExceeded;
                    o.elapsed = std::chrono::steady_clock::now() - t0;
                    return o;
                }
                (void)budget.record_attempt();

                ++m_pdQueryTriggered;
                ++o.pd_queries_triggered;
                cachedRoute = actual;
                continue;
            }

            // Cache and PD agree -- attempt the write.
            auto rem = budget.remaining();
            auto deadline = rem.count() > 0
                ? std::min<std::chrono::milliseconds>(m_cfg.per_write_timeout, rem)
                : m_cfg.per_write_timeout;
            ++o.attempted_writes;
            ErrorCode rc = m_write(regionId, cachedRoute, deadline);
            if (rc == ErrorCode::Success) {
                ++m_writeSucceededAfterRefresh;
                ++o.write_succeeded_after;
                o.landed_on_route = cachedRoute;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            ++m_clientErrorReturned;
            o.code = rc;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // We drained the actualRoutePerAttempt sequence without
        // converging on a write attempt: PD kept reporting new
        // routes. Cap with the retry-cap-exceeded error.
        ++m_clientErrorReturned;
        o.code = ErrorCode::StaleRegionCacheRetryCapExceeded;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    std::uint64_t StaleCacheHit() const {
        return m_staleCacheHit.load(std::memory_order_relaxed);
    }
    std::uint64_t PdQueryTriggered() const {
        return m_pdQueryTriggered.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteSucceededAfterRefresh() const {
        return m_writeSucceededAfterRefresh.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config     m_cfg;
    RouteWrite m_write;

    std::atomic<std::uint64_t> m_staleCacheHit{0};
    std::atomic<std::uint64_t> m_pdQueryTriggered{0};
    std::atomic<std::uint64_t> m_writeSucceededAfterRefresh{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
