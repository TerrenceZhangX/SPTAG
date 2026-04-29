// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// PLACEHOLDER header for the `RetryBudget` primitive. The full
// implementation (per-call wall-clock + count cap, exponential+jitter
// backoff, circuit-breaker hooks) is being delivered in parallel under
// the `prim/retry-budget` branch. We ship a lightweight in-header
// stand-in here so dependent primitives (coprocessor-error path,
// MultiGet fallback, op-id idempotency) can compile and unit-test
// against the *interface* before the heavyweight implementation lands.
//
// API contract (kept stable when prim/retry-budget supersedes this):
//   - Construct with (maxAttempts, totalBudget):
//       RetryBudget(int maxAttempts, std::chrono::milliseconds totalBudget)
//   - `TryAgain(ctx)` returns true if the caller is still allowed to
//     retry. Internally it (a) decrements the attempt counter,
//     (b) sleeps for an exponential+jittered backoff bounded by the
//     remaining wall-clock budget, and (c) reports false when either
//     budget is exhausted.
//   - `ctx` is an opaque pointer reserved for the real implementation
//     (cancellation token, metrics sink, deadline propagation). The
//     placeholder ignores it.

#ifndef _SPTAG_SPANN_DISTRIBUTED_PRIM_RETRYBUDGET_H_
#define _SPTAG_SPANN_DISTRIBUTED_PRIM_RETRYBUDGET_H_

#include <atomic>
#include <chrono>
#include <random>
#include <thread>

namespace SPTAG { namespace SPANN { namespace prim {

class RetryBudget
{
public:
    RetryBudget(int maxAttempts,
                std::chrono::milliseconds totalBudget)
        : m_remaining(maxAttempts),
          m_deadline(std::chrono::steady_clock::now() + totalBudget),
          m_attempt(0)
    {}

    // Returns true if a retry should be attempted. Sleeps for an
    // exponential backoff with jitter, bounded by the remaining budget.
    // `ctx` is reserved for future cancellation/deadline plumbing.
    bool TryAgain(void* ctx = nullptr)
    {
        (void)ctx;
        if (m_remaining <= 0) return false;
        auto now = std::chrono::steady_clock::now();
        if (now >= m_deadline) return false;

        --m_remaining;
        ++m_attempt;

        // Exponential: 50ms * 2^(attempt-1), capped at remaining budget
        // and at 2s. Jitter ±25%.
        auto base = std::chrono::milliseconds(50) * (1 << std::min(m_attempt - 1, 5));
        if (base > std::chrono::milliseconds(2000)) base = std::chrono::milliseconds(2000);

        // Jitter
        thread_local std::mt19937 rng{std::random_device{}()};
        std::uniform_real_distribution<double> dist(0.75, 1.25);
        auto sleepFor = std::chrono::milliseconds(
            static_cast<long long>(base.count() * dist(rng)));

        auto remainingNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
            m_deadline - now);
        if (remainingNs.count() <= 0) return false;
        auto remainingMs = std::chrono::duration_cast<std::chrono::milliseconds>(remainingNs);
        if (sleepFor > remainingMs) sleepFor = remainingMs;
        if (sleepFor.count() > 0) std::this_thread::sleep_for(sleepFor);

        return true;
    }

    int RemainingAttempts() const { return m_remaining; }

private:
    int m_remaining;
    std::chrono::steady_clock::time_point m_deadline;
    int m_attempt;
};

}}}  // namespace SPTAG::SPANN::prim

#endif  // _SPTAG_SPANN_DISTRIBUTED_PRIM_RETRYBUDGET_H_
