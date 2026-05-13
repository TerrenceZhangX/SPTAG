// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-region-error-retry-cap
//
// Goal: A TiKV write can come back with a RegionError carrying
// one of several kinds (EpochNotMatch, NotLeader,
// RegionNotFound, KeyNotInRegion, ServerIsBusy, ...). Each kind
// has its own client-side recovery (refresh region cache,
// re-resolve the leader, re-route to the new region, back off),
// but the *general* client invariant is: every region error
// observed must be recovered exactly once, and the total chain
// of region-error recoveries must be bounded by a RetryBudget
// so a pathological storm of region errors cannot become an
// unbounded retry loop. This wrapper models the *generic*
// retry-cap, independent of the specific RegionErrorKind.
//
// Distinct from tikv-region-leader-transfer-mid-rpc: that case
// is the NotLeader-only specialisation. Distinct from
// tikv-region-split-storm / tikv-region-merge-during-writes:
// those cover EpochNotMatch / RegionNotFound respectively. The
// retry-cap guard is the umbrella invariant: regardless of the
// region-error kind, the chain of recoveries is bounded.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_REGION_ERROR_RETRY_CAP`
//     unset/0): wrapper is dormant -- drives one pass-through
//     write against `regionId`; no region-error counter moves.
//   * env-armed: while `regionErrorChain` is non-empty, the
//     wrapper treats the write as having returned a RegionError
//     of that kind (++m_regionErrorObserved), consumes one
//     budget tick, performs the kind-specific recovery
//     (++m_regionErrorRecovered), and retries. When the chain
//     is empty the write lands successfully
//     (++m_writeSucceededAfterRecovery). If the RetryBudget is
//     exhausted, ++m_clientErrorReturned, return
//     ErrorCode::RegionErrorRetryCapExceeded.
//
// Counters required by case-md:
//   - m_regionErrorObserved
//   - m_regionErrorRecovered
//   - m_writeSucceededAfterRecovery
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

/// Kinds of TiKV RegionError this wrapper models. The wrapper
/// is kind-agnostic on the counter side -- every kind funnels
/// through the same observed/recovered/budget path -- but the
/// enum keeps the chain explicit so tests can mix kinds in one
/// chain.
enum class RegionErrorKind : std::uint8_t {
    EpochNotMatch    = 1,
    NotLeader        = 2,
    RegionNotFound   = 3,
    KeyNotInRegion   = 4,
    ServerIsBusy     = 5,
};

/// One outcome of a gated region-error retry-cap write.
struct TikvRegionErrorRetryCapOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected                         = false;
    std::size_t region_error_obs          = 0;
    std::size_t region_error_recovered    = 0;
    std::size_t write_succeeded_after_rec = 0;
    int attempted_writes                  = 0;
};

class TikvRegionErrorRetryCapGated {
public:
    using RegionId = std::int64_t;

    struct Config {
        std::chrono::milliseconds per_write_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Per-attempt write callable: drive the write to
    /// `regionId`. Returns Success on landing.
    using RegionWrite = std::function<ErrorCode(RegionId regionId,
                                                std::chrono::milliseconds deadline)>;

    TikvRegionErrorRetryCapGated(Config cfg, RegionWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_REGION_ERROR_RETRY_CAP");
        return v && *v && std::string(v) != "0";
    }

    /// Drive a single write to `regionId`. If env-armed and
    /// `regionErrorChain` is non-empty, each entry models one
    /// observed RegionError; the wrapper performs the
    /// per-kind recovery and retries until the chain is drained
    /// or the RetryBudget is spent.
    TikvRegionErrorRetryCapOutcome WriteWithRegionErrorGuard(
            RegionId regionId,
            std::vector<RegionErrorKind> regionErrorChain,
            Helper::RetryBudget& budget) {
        TikvRegionErrorRetryCapOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        if (!envArmed) {
            ++o.attempted_writes;
            ErrorCode rc = m_write(regionId, m_cfg.per_write_timeout);
            if (rc != ErrorCode::Success) {
                o.code = rc;
            }
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        o.injected = true;

        std::size_t idx = 0;
        while (idx < regionErrorChain.size()) {
            ++m_regionErrorObserved;
            ++o.region_error_obs;

            if (!budget.should_retry() || budget.remaining().count() <= 0) {
                ++m_clientErrorReturned;
                o.code = ErrorCode::RegionErrorRetryCapExceeded;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            (void)budget.record_attempt();

            // Kind-specific recovery is a black box at this
            // layer (the specialised wrappers cover it). The
            // umbrella invariant is: every observed error gets
            // exactly one recovery tick.
            (void)regionErrorChain[idx++];
            ++m_regionErrorRecovered;
            ++o.region_error_recovered;
        }

        auto rem = budget.remaining();
        auto deadline = rem.count() > 0
            ? std::min<std::chrono::milliseconds>(m_cfg.per_write_timeout, rem)
            : m_cfg.per_write_timeout;
        ++o.attempted_writes;
        ErrorCode rc = m_write(regionId, deadline);
        if (rc == ErrorCode::Success) {
            ++m_writeSucceededAfterRecovery;
            ++o.write_succeeded_after_rec;
        } else {
            ++m_clientErrorReturned;
            o.code = rc;
        }

        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    std::uint64_t RegionErrorObserved() const {
        return m_regionErrorObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t RegionErrorRecovered() const {
        return m_regionErrorRecovered.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteSucceededAfterRecovery() const {
        return m_writeSucceededAfterRecovery.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config      m_cfg;
    RegionWrite m_write;

    std::atomic<std::uint64_t> m_regionErrorObserved{0};
    std::atomic<std::uint64_t> m_regionErrorRecovered{0};
    std::atomic<std::uint64_t> m_writeSucceededAfterRecovery{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
