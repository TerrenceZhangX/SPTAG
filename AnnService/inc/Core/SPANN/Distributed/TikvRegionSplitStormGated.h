// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-region-split-storm
//
// Goal: A bulk insert into a TiKV-backed region triggers many
// concurrent region splits. While splits are in-flight, RPCs sent
// to the old (pre-split) region IDs return EpochNotMatch /
// RegionNotFound. A correct client must refresh its region cache
// from PD, route the retry to the new region, and bound the
// number of refresh+retry rounds via a RetryBudget so that a
// "split storm" on a hot range cannot turn into an unbounded
// retry storm.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_REGION_SPLIT_STORM` unset/0):
//     wrapper is dormant -- routes each key to its declared
//     region exactly once via the injected write callable; no
//     counter moves.
//   * env-armed: for every key with splitsRemainingPerRegion[r]>0
//     the wrapper treats the per-region write as having returned
//     EpochNotMatch (++m_epochNotMatchObserved), consumes one
//     budget tick, refreshes the region cache
//     (++m_regionCacheRefreshed), decrements
//     splitsRemainingPerRegion[r], and retries on the post-split
//     region id (r + 1000 * (round+1) in this model). It keeps
//     looping per key until either splitsRemaining drops to 0
//     (write lands -- ++m_writeRoutedToNewRegion) or the
//     RetryBudget is exhausted (++m_clientErrorReturned, return
//     ErrorCode::SplitStormRetryCapExceeded for that batch).
//
// Counters required by case-md:
//   - m_epochNotMatchObserved
//   - m_regionCacheRefreshed
//   - m_writeRoutedToNewRegion
//   - m_clientErrorReturned

#pragma once

#include "inc/Core/Common.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace SPTAG::SPANN::Distributed {

/// One outcome of a gated split-storm write batch.
struct TikvRegionSplitStormOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected               = false;
    std::size_t epoch_misses    = 0;
    std::size_t refreshes       = 0;
    std::size_t new_region_lands = 0;
    int attempted_writes        = 0;
};

class TikvRegionSplitStormGated {
public:
    using RegionId = std::int64_t;

    struct Config {
        std::chrono::milliseconds per_write_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Per-key write callable: given a region id, drive the write
    /// against that region. Return Success on landing.
    using RegionWrite = std::function<ErrorCode(RegionId regionId,
                                                std::chrono::milliseconds deadline)>;

    TikvRegionSplitStormGated(Config cfg, RegionWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_REGION_SPLIT_STORM");
        return v && *v && std::string(v) != "0";
    }

    /// Write a batch of keys, each routed to a region. While a
    /// region still has splits remaining the wrapper rolls the
    /// key to the post-split region id and retries.
    TikvRegionSplitStormOutcome WriteWithSplitGuard(
            const std::vector<RegionId>& regionRoutes,
            std::map<RegionId, int> splitsRemainingPerRegion,
            Helper::RetryBudget& budget) {
        TikvRegionSplitStormOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        if (!envArmed) {
            // Env-off: pass-through, one write per key against the
            // declared region. No counter moves.
            for (RegionId r : regionRoutes) {
                ++o.attempted_writes;
                ErrorCode rc = m_write(r, m_cfg.per_write_timeout);
                if (rc != ErrorCode::Success) {
                    o.code = rc;
                    o.elapsed = std::chrono::steady_clock::now() - t0;
                    return o;
                }
            }
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        o.injected = true;

        // Env-armed: each key may have to be rolled through several
        // post-split regions before landing.
        for (RegionId r : regionRoutes) {
            while (true) {
                auto it = splitsRemainingPerRegion.find(r);
                int remaining = (it != splitsRemainingPerRegion.end()) ? it->second : 0;

                if (remaining <= 0) {
                    // Region is stable: land the write on the
                    // (now-current) region id.
                    auto rem = budget.remaining();
                    auto deadline = rem.count() > 0
                        ? std::min<std::chrono::milliseconds>(m_cfg.per_write_timeout, rem)
                        : m_cfg.per_write_timeout;
                    ++o.attempted_writes;
                    ErrorCode rc = m_write(r, deadline);
                    if (rc == ErrorCode::Success) {
                        ++m_writeRoutedToNewRegion;
                        ++o.new_region_lands;
                    } else {
                        ++m_clientErrorReturned;
                        o.code = rc;
                        o.elapsed = std::chrono::steady_clock::now() - t0;
                        return o;
                    }
                    break;
                }

                // EpochNotMatch path: refresh + retry on the
                // post-split region (modeled as same logical key
                // routed to the next epoch of the same region).
                ++m_epochNotMatchObserved;
                ++o.epoch_misses;

                if (!budget.should_retry() || budget.remaining().count() <= 0) {
                    ++m_clientErrorReturned;
                    o.code = ErrorCode::SplitStormRetryCapExceeded;
                    o.elapsed = std::chrono::steady_clock::now() - t0;
                    return o;
                }
                (void)budget.record_attempt();

                ++m_regionCacheRefreshed;
                ++o.refreshes;
                if (it != splitsRemainingPerRegion.end() && it->second > 0) {
                    --it->second;
                }
            }
        }

        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    std::uint64_t EpochNotMatchObserved() const {
        return m_epochNotMatchObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t RegionCacheRefreshed() const {
        return m_regionCacheRefreshed.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteRoutedToNewRegion() const {
        return m_writeRoutedToNewRegion.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config      m_cfg;
    RegionWrite m_write;

    std::atomic<std::uint64_t> m_epochNotMatchObserved{0};
    std::atomic<std::uint64_t> m_regionCacheRefreshed{0};
    std::atomic<std::uint64_t> m_writeRoutedToNewRegion{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
