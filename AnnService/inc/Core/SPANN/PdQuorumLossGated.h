// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only gated wrapper for FT case `pd-quorum-loss`.
//
// Contract (see tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
// pd-quorum-loss.md):
//   * Off-by-default. Enable by setting `SPTAG_FAULT_PD_QUORUM_LOSS=1`.
//   * When armed AND the PD client observes fewer than a strict
//     majority of PD members alive, writes are refused with
//     ErrorCode::PdUnavailable (0x6003) and reads are degraded to a
//     TTL-bounded cache lookup; a cache miss / expired entry returns
//     PdUnavailable. No silent commit-without-PD.
//   * Counters are global (process-wide atomics) so test-harnesses can
//     observe deltas without poking TiKVIO internals.
//   * Reuses Helper::RetryBudget so callers' bounded-retry contract is
//     surfaced; an exhausted budget short-circuits to
//     ErrorCode::RetryBudgetExceeded.
//
// This is intentionally a header-only seam. The real PD client layer
// (ExtraTiKVController.h) will call into GuardWrite/GuardRead at the
// request-entry boundary in a follow-up wiring change; the contract
// and Tier 1 repro live here so they cannot bit-rot.

#ifndef _SPTAG_FAULT_PD_QUORUM_LOSS_GATED_H_
#define _SPTAG_FAULT_PD_QUORUM_LOSS_GATED_H_

#include "inc/Core/Common.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <unordered_map>

namespace SPTAG
{
namespace Fault
{
namespace PdQuorumLoss
{

// ---- Counters (process-wide; reset via ResetCountersForTesting) -----------

inline std::atomic<std::uint64_t>& CounterPdQuorumLost()
{
    static std::atomic<std::uint64_t> c{0};
    return c;
}
inline std::atomic<std::uint64_t>& CounterWriteRefused()
{
    static std::atomic<std::uint64_t> c{0};
    return c;
}
inline std::atomic<std::uint64_t>& CounterReadDegraded()
{
    static std::atomic<std::uint64_t> c{0};
    return c;
}
inline std::atomic<std::uint64_t>& CounterClientErrorReturned()
{
    static std::atomic<std::uint64_t> c{0};
    return c;
}

inline void ResetCountersForTesting()
{
    CounterPdQuorumLost().store(0, std::memory_order_relaxed);
    CounterWriteRefused().store(0, std::memory_order_relaxed);
    CounterReadDegraded().store(0, std::memory_order_relaxed);
    CounterClientErrorReturned().store(0, std::memory_order_relaxed);
}

// ---- Env gate -------------------------------------------------------------

inline bool ArmedFromEnv()
{
    const char* v = std::getenv("SPTAG_FAULT_PD_QUORUM_LOSS");
    return v != nullptr && v[0] == '1' && v[1] == '\0';
}

// ---- PD topology snapshot -------------------------------------------------

struct PdState
{
    int liveMembers = 4;
    int totalMembers = 4;

    bool QuorumOk() const
    {
        // Strict majority: live > total / 2.
        return liveMembers * 2 > totalMembers;
    }
};

// ---- TTL cache used for the degraded read path ---------------------------

class TtlCache
{
public:
    using Clock = std::chrono::steady_clock;

    void Put(const std::string& key,
             const std::string& value,
             std::chrono::milliseconds ttl)
    {
        std::lock_guard<std::mutex> g(m_mu);
        m_entries[key] = Entry{value, Clock::now() + ttl};
    }

    bool Get(const std::string& key, std::string* out) const
    {
        std::lock_guard<std::mutex> g(m_mu);
        auto it = m_entries.find(key);
        if (it == m_entries.end()) return false;
        if (Clock::now() >= it->second.expiresAt) return false;
        if (out) *out = it->second.value;
        return true;
    }

    void Clear()
    {
        std::lock_guard<std::mutex> g(m_mu);
        m_entries.clear();
    }

    std::size_t Size() const
    {
        std::lock_guard<std::mutex> g(m_mu);
        return m_entries.size();
    }

private:
    struct Entry
    {
        std::string value;
        Clock::time_point expiresAt;
    };
    mutable std::mutex m_mu;
    std::unordered_map<std::string, Entry> m_entries;
};

// ---- Guards ---------------------------------------------------------------
//
// GuardWrite is called before any PD-coordinated write. When armed and
// quorum is lost, the write is refused with ErrorCode::PdUnavailable;
// the caller MUST NOT then commit. When disarmed (env-off) or quorum is
// healthy, returns ErrorCode::Success and the caller proceeds to commit.
//
// GuardRead is called before any PD-coordinated read. When armed and
// quorum is lost, the wrapper consults the supplied TTL cache:
//   * cache hit + entry valid -> returns Success and writes value to out;
//     bumps `m_readDegraded`.
//   * cache miss / expired    -> returns ErrorCode::PdUnavailable; bumps
//     `m_clientErrorReturned`.
// When disarmed or quorum healthy, returns Success and the caller serves
// from the authoritative path.
//
// Both guards optionally accept a Helper::RetryBudget*; if non-null and
// already exhausted, the guard short-circuits to RetryBudgetExceeded so
// callers do not loop indefinitely.

inline ErrorCode GuardWrite(const PdState& pd,
                            Helper::RetryBudget* budget = nullptr)
{
    if (budget != nullptr && budget->exhausted())
    {
        return ErrorCode::RetryBudgetExceeded;
    }
    if (!ArmedFromEnv()) return ErrorCode::Success;
    if (pd.QuorumOk())   return ErrorCode::Success;

    CounterPdQuorumLost().fetch_add(1, std::memory_order_relaxed);
    CounterWriteRefused().fetch_add(1, std::memory_order_relaxed);
    CounterClientErrorReturned().fetch_add(1, std::memory_order_relaxed);
    return ErrorCode::PdUnavailable;
}

inline ErrorCode GuardRead(const PdState& pd,
                           const std::string& key,
                           const TtlCache& cache,
                           std::string* out,
                           Helper::RetryBudget* budget = nullptr)
{
    if (budget != nullptr && budget->exhausted())
    {
        return ErrorCode::RetryBudgetExceeded;
    }
    if (!ArmedFromEnv()) return ErrorCode::Success;
    if (pd.QuorumOk())   return ErrorCode::Success;

    CounterPdQuorumLost().fetch_add(1, std::memory_order_relaxed);
    if (cache.Get(key, out))
    {
        CounterReadDegraded().fetch_add(1, std::memory_order_relaxed);
        return ErrorCode::Success;
    }
    CounterClientErrorReturned().fetch_add(1, std::memory_order_relaxed);
    return ErrorCode::PdUnavailable;
}

}  // namespace PdQuorumLoss
}  // namespace Fault
}  // namespace SPTAG

#endif  // _SPTAG_FAULT_PD_QUORUM_LOSS_GATED_H_
