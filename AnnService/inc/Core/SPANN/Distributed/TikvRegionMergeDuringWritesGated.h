// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-region-merge-during-writes
//
// Goal: Two (or more) adjacent TiKV regions are merged while
// writes against them are still in flight. After the merge, RPCs
// targeting an old region id return RegionNotFound (the region
// has been tombstoned) or EpochNotMatch (the epoch was bumped on
// the merge target). A correct client must:
//   * refresh the region cache from PD,
//   * resolve the old region id to the merged-region id,
//   * retry the write on the merged region,
//   * bound the retry depth via a RetryBudget so a chained merge
//     cannot turn into an unbounded re-route storm.
//
// Distinct from tikv-region-split-storm: split storms turn 1
// region into N (epoch bump per split round), merge collapses N
// regions into 1 (multiple old region ids all resolve to the
// same merge target). The wrapper exposes the merge as a
// `map<old_region_id, merged_region_id>` -- absent key means the
// region is stable; chained merges (r1->r2->r3) are followed
// transitively.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_REGION_MERGE_DURING_WRITES`
//     unset/0): wrapper is dormant -- routes each key to its
//     declared region exactly once via the injected write
//     callable; no counter moves.
//   * env-armed: for each key, if regionRoutes[k] is a key in
//     mergeMap, the wrapper treats the per-region write as
//     having returned RegionNotFound
//     (++m_regionNotFoundObserved), consumes one budget tick,
//     resolves the merge target (++m_mergeTargetResolved), and
//     retries on the merged region. If the merge target is
//     itself a key in mergeMap, the loop continues (chained
//     merge). When the resolved region is stable, the write
//     lands (++m_writeRoutedToMergedRegion). If the RetryBudget
//     is exhausted, ++m_clientErrorReturned, return
//     ErrorCode::MergeRetryCapExceeded.
//
// Counters required by case-md:
//   - m_regionNotFoundObserved
//   - m_mergeTargetResolved
//   - m_writeRoutedToMergedRegion
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

/// One outcome of a gated merge-during-writes batch.
struct TikvRegionMergeDuringWritesOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected                     = false;
    std::size_t region_not_found      = 0;
    std::size_t merge_target_resolved = 0;
    std::size_t merged_region_lands   = 0;
    int attempted_writes              = 0;
};

class TikvRegionMergeDuringWritesGated {
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

    TikvRegionMergeDuringWritesGated(Config cfg, RegionWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_REGION_MERGE_DURING_WRITES");
        return v && *v && std::string(v) != "0";
    }

    /// Write a batch of keys, each routed to a region. If the
    /// region appears as a key in `mergeMap`, the wrapper rolls
    /// the key to `mergeMap[r]` and retries (chains allowed).
    TikvRegionMergeDuringWritesOutcome WriteWithMergeGuard(
            const std::vector<RegionId>& regionRoutes,
            const std::map<RegionId, RegionId>& mergeMap,
            Helper::RetryBudget& budget) {
        TikvRegionMergeDuringWritesOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        if (!envArmed) {
            // Env-off: pass-through, one write per key against
            // the declared region. No counter moves.
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

        // Env-armed: each key may be rolled through several
        // merge targets before landing.
        for (RegionId r : regionRoutes) {
            RegionId cur = r;
            while (true) {
                auto it = mergeMap.find(cur);
                if (it == mergeMap.end()) {
                    // Region is stable: land the write on the
                    // (now-current) merged region id.
                    auto rem = budget.remaining();
                    auto deadline = rem.count() > 0
                        ? std::min<std::chrono::milliseconds>(m_cfg.per_write_timeout, rem)
                        : m_cfg.per_write_timeout;
                    ++o.attempted_writes;
                    ErrorCode rc = m_write(cur, deadline);
                    if (rc == ErrorCode::Success) {
                        ++m_writeRoutedToMergedRegion;
                        ++o.merged_region_lands;
                    } else {
                        ++m_clientErrorReturned;
                        o.code = rc;
                        o.elapsed = std::chrono::steady_clock::now() - t0;
                        return o;
                    }
                    break;
                }

                // RegionNotFound path: refresh + resolve merge
                // target, retry on the merged region id.
                ++m_regionNotFoundObserved;
                ++o.region_not_found;

                if (!budget.should_retry() || budget.remaining().count() <= 0) {
                    ++m_clientErrorReturned;
                    o.code = ErrorCode::MergeRetryCapExceeded;
                    o.elapsed = std::chrono::steady_clock::now() - t0;
                    return o;
                }
                (void)budget.record_attempt();

                ++m_mergeTargetResolved;
                ++o.merge_target_resolved;
                cur = it->second;
            }
        }

        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    std::uint64_t RegionNotFoundObserved() const {
        return m_regionNotFoundObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t MergeTargetResolved() const {
        return m_mergeTargetResolved.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteRoutedToMergedRegion() const {
        return m_writeRoutedToMergedRegion.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config      m_cfg;
    RegionWrite m_write;

    std::atomic<std::uint64_t> m_regionNotFoundObserved{0};
    std::atomic<std::uint64_t> m_mergeTargetResolved{0};
    std::atomic<std::uint64_t> m_writeRoutedToMergedRegion{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
