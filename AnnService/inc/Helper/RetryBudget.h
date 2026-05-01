// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// RetryBudget — shared per-logical-call retry primitive for the SPANN+TiKV
// stack. Defined by `tikv-region-error-retry-cap.md`. Used by ~10 fault
// cases (pd-leader-failover, pd-quorum-loss, pd-reconnect-during-retry,
// tikv-grpc-timeout-storm, tikv-grpc-stub-channel-broken,
// tikv-region-{split,merge,leader-transfer-mid-rpc}-storm,
// tikv-store-die-with-replicas, tikv-region-error-retry-cap).
//
// Contract (kept deliberately small):
//   1. A single budget instance is constructed at the *outermost* SPANN
//      caller (Append, Split, Merge, Search, MultiGet) and threaded
//      down. All retry loops on that call path share it.
//   2. should_retry() is the only loop guard. It enforces both a wall
//      deadline and an attempt cap.
//   3. record_attempt() is called *after* an attempt fails and *before*
//      the loop sleeps. It also computes the next backoff sleep
//      (exponential + ±50% jitter) and sleeps for that long internally
//      (via sleep_for_backoff()).
//   4. remaining() exposes the wall-clock left so callers can populate
//      gRPC ClientContext deadlines instead of using the static cap.
//   5. exhausted() is shorthand for !should_retry() and is what callers
//      use to decide whether to surface ErrorCode::RetryBudgetExceeded.
//
// Defaults are taken from the case spec:
//   total_wall = 2000 ms, max_attempts = 6,
//   base = 20 ms, cap = 500 ms, jitter = ±50%.
// Several dependent cases want different numbers (pd_total_wall=1s/3
// attempts in pd-reconnect-during-retry; tikv-store-die wants 30s);
// callers pass non-default budgets via the explicit constructor.
//
// Thread-safety: a RetryBudget is owned by a single logical call. It
// must NOT be shared across threads of independent logical calls. Inside
// one logical call (e.g. MultiGet's per-region futures), record_attempt
// IS thread-safe: counters are atomic and the deadline is immutable.

#ifndef _SPTAG_HELPER_RETRY_BUDGET_H_
#define _SPTAG_HELPER_RETRY_BUDGET_H_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>

namespace SPTAG
{
namespace Helper
{

class RetryBudget
{
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = std::chrono::milliseconds;

    struct Config {
        Duration total_wall   = Duration(2000);
        int      max_attempts = 6;
        Duration base_backoff = Duration(20);
        Duration cap_backoff  = Duration(500);
        // jitter is multiplicative: sleep *= U[1-jitter, 1+jitter]
        double   jitter       = 0.5;
    };

    static const Config& DefaultConfig() {
        static const Config c{};
        return c;
    }

    static const Config& PDConfig() {
        // pd-reconnect-during-retry: pd_total_wall=1s, pd_max_attempts=3
        static const Config c{
            std::chrono::milliseconds(1000),
            3,
            std::chrono::milliseconds(20),
            std::chrono::milliseconds(500),
            0.5
        };
        return c;
    }

    RetryBudget()
        : RetryBudget(DefaultConfig()) {}

    explicit RetryBudget(const Config& cfg)
        : m_cfg(cfg)
        , m_deadline(Clock::now() + cfg.total_wall)
        , m_attempts(0)
    {}

    // No copy: a budget is single-owner per logical call. Move OK.
    RetryBudget(const RetryBudget&) = delete;
    RetryBudget& operator=(const RetryBudget&) = delete;
    RetryBudget(RetryBudget&&) = default;

    // Loop guard. Call at top of every retry iteration.
    bool should_retry() const {
        if (m_terminated.load(std::memory_order_relaxed)) return false;
        return m_attempts.load(std::memory_order_relaxed) < m_cfg.max_attempts
               && Clock::now() < m_deadline;
    }

    bool exhausted() const { return !should_retry(); }

    // Number of attempts already recorded.
    int attempts() const { return m_attempts.load(std::memory_order_relaxed); }

    // Wall-clock remaining (clamped at zero).
    Duration remaining() const {
        auto now = Clock::now();
        if (now >= m_deadline) return Duration(0);
        return std::chrono::duration_cast<Duration>(m_deadline - now);
    }

    const Config& config() const { return m_cfg; }

    // Record one failed attempt and sleep for the computed backoff.
    // Returns false if the budget is now exhausted (caller should bail
    // out without retrying further).
    bool record_attempt() {
        int n = m_attempts.fetch_add(1, std::memory_order_relaxed);
        if (n + 1 >= m_cfg.max_attempts) {
            m_terminated.store(true, std::memory_order_relaxed);
            return false;
        }
        if (Clock::now() >= m_deadline) {
            m_terminated.store(true, std::memory_order_relaxed);
            return false;
        }
        auto rem_before = remaining();
        auto bo = compute_backoff(n);
        if (bo >= rem_before) {
            // Planned sleep would consume the rest of our wall: declare
            // the budget terminated so callers can't loop further.
            if (rem_before.count() > 0) {
                std::this_thread::sleep_for(rem_before);
            }
            m_terminated.store(true, std::memory_order_relaxed);
            return false;
        }
        std::this_thread::sleep_for(bo);
        if (Clock::now() >= m_deadline
            || m_attempts.load(std::memory_order_relaxed) >= m_cfg.max_attempts) {
            m_terminated.store(true, std::memory_order_relaxed);
            return false;
        }
        return true;
    }

    // Compute next-attempt backoff deterministically given attempt index n.
    // Public so tests can verify monotonicity & jitter without sleeping.
    Duration compute_backoff(int n) const {
        // base * 2^n, capped, then jittered ±50%.
        double base_ms = static_cast<double>(m_cfg.base_backoff.count());
        double cap_ms  = static_cast<double>(m_cfg.cap_backoff.count());
        double exp_ms  = base_ms * std::pow(2.0, std::min(n, 16));
        double clipped = std::min(exp_ms, cap_ms);
        double j = JitterMultiplier();
        double final_ms = clipped * j;
        // Also do not sleep past our deadline.
        auto rem = remaining();
        long long capped = std::min<long long>(static_cast<long long>(final_ms),
                                               rem.count());
        if (capped < 0) capped = 0;
        return Duration(capped);
    }

private:
    void sleep_for_backoff(int n) {
        auto d = compute_backoff(n);
        if (d.count() > 0) std::this_thread::sleep_for(d);
    }

    double JitterMultiplier() const {
        // Per-thread RNG seeded by (thread_id, monotonic_clock) once so
        // that 2K clients are decorrelated.
        thread_local std::mt19937_64 rng(
            static_cast<uint64_t>(
                std::hash<std::thread::id>{}(std::this_thread::get_id())
            ) ^ static_cast<uint64_t>(Clock::now().time_since_epoch().count())
        );
        std::uniform_real_distribution<double> d(1.0 - m_cfg.jitter,
                                                 1.0 + m_cfg.jitter);
        return d(rng);
    }

    Config              m_cfg;
    TimePoint           m_deadline;
    std::atomic<int>    m_attempts;
    mutable std::atomic<bool> m_terminated{false};
};

} // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_RETRY_BUDGET_H_
