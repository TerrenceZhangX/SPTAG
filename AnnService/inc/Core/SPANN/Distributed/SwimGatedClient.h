// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration client wrapper for fault case
//   swim-detector-lag
//
// Goal:  given a SwimDetector + RetryBudget, surface a *well-formed*
//        ErrorCode to the caller during the detector's decision gap and
//        guard against retry pile-up on a dead peer.
//
// Contract:
//   * env-off (`SPTAG_FAULT_SWIM_DETECTOR_LAG` unset/0): the wrapper is
//     dormant — it forwards the inner RPC unmodified and never
//     increments any counter.
//   * env-armed: before each call we consult the detector:
//       - peer Dead   -> immediate ErrorCode::OwnerSuspect (no RPC)
//       - peer Suspect or Alive -> issue the RPC bounded by the
//         shared RetryBudget; on per-call timeout the wrapper
//         counts the well-formed error and returns
//         ErrorCode::RetryBudgetExceeded once the budget is gone.
//     Concurrent callers share a per-peer in-flight cap
//     (`peer_queue_max`); rejected admissions surface the same
//     well-formed error rather than queueing.
//
// This header is dependency-light on purpose: callers inject the inner
// RPC by std::function so prod code (Connection.cpp) and the in-process
// fault-case test can both drive it.

#pragma once

#include "inc/Core/Common.h"
#include "inc/Core/SPANN/Distributed/SwimDetector.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace SPTAG::SPANN::Distributed {

/// One outcome of a gated call attempt.  The caller drives one logical
/// call to completion; the wrapper does NOT loop internally — that is
/// the caller's job (so the budget can be shared across attempts).
struct GatedCallOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool admitted = true;   // false if rejected by per-peer queue cap
    bool dead     = false;  // true if detector reported Dead pre-RPC
};

class SwimGatedClient {
public:
    struct Config {
        std::size_t peer_queue_max = 8;       // bound per-peer in-flight
        std::chrono::milliseconds per_call_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// `inner_rpc` simulates the underlying transport. It receives the
    /// per-call deadline and returns Success on timely completion or
    /// any other ErrorCode if it failed/timed out before the deadline.
    using InnerRpc = std::function<ErrorCode(int peer,
                                             std::chrono::milliseconds deadline)>;

    SwimGatedClient(SwimDetector& detector, Config cfg, InnerRpc rpc)
        : m_detector(detector), m_cfg(std::move(cfg)), m_rpc(std::move(rpc)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_SWIM_DETECTOR_LAG");
        return v && *v && std::string(v) != "0";
    }

    /// Attempt a single bounded call to `peer`. Caller threads the
    /// shared RetryBudget across attempts.
    GatedCallOutcome Call(int peer, Helper::RetryBudget& budget) {
        GatedCallOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        if (!EnvArmed()) {
            // Dormant: just forward.
            o.code = m_rpc(peer, m_cfg.per_call_timeout);
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // 1) Detector veto: peer already known Dead → fast-fail.
        MemberState st;
        if (m_detector.TryGetMember(peer, st)
            && st.health == MemberHealth::Dead) {
            ++m_swimDetectorLagErrors;
            o.code = ErrorCode::OwnerSuspect;
            o.dead = true;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // 2) Per-peer queue admission control: prevents pile-up while
        //    detector is still in the gap window.
        if (!Admit(peer)) {
            ++m_swimDetectorLagErrors;
            ++m_swimDetectorLagBudgetExhausted;
            o.code = ErrorCode::RetryBudgetExceeded;
            o.admitted = false;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }
        struct Releaser {
            SwimGatedClient* self; int p;
            ~Releaser() { self->Release(p); }
        } _r{this, peer};

        // 3) Bounded RPC: deadline = min(per_call_timeout, budget.remaining()).
        auto rem = budget.remaining();
        auto deadline = std::min<std::chrono::milliseconds>(
            m_cfg.per_call_timeout, rem);
        if (deadline.count() <= 0) {
            ++m_swimDetectorLagErrors;
            ++m_swimDetectorLagBudgetExhausted;
            o.code = ErrorCode::RetryBudgetExceeded;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        ErrorCode rc = m_rpc(peer, deadline);
        if (rc != ErrorCode::Success) {
            ++m_swimDetectorLagErrors;
            // The caller drives record_attempt() between Call()s, so
            // budget.attempts() reflects how many *prior* attempts have
            // been recorded. We treat a failure as terminal (and bump
            // the well-formed RetryBudgetExceeded counter) when the
            // caller has no headroom left for another attempt under
            // either the attempt cap or the wall deadline minus one
            // more per_call_timeout window.
            const int prior = budget.attempts();
            const auto rem_after = budget.remaining();
            const bool noAttemptsLeft =
                (prior + 1) >= budget.config().max_attempts;
            const bool noWallLeft =
                rem_after <= m_cfg.per_call_timeout;
            if (budget.exhausted() || !budget.should_retry()
                || noAttemptsLeft || noWallLeft) {
                ++m_swimDetectorLagBudgetExhausted;
                o.code = ErrorCode::RetryBudgetExceeded;
            } else {
                o.code = rc;
            }
        } else {
            o.code = ErrorCode::Success;
        }
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    // -- counters -----------------------------------------------------

    std::uint64_t SwimDetectorLagErrors() const {
        return m_swimDetectorLagErrors.load(std::memory_order_relaxed);
    }
    std::uint64_t SwimDetectorLagBudgetExhausted() const {
        return m_swimDetectorLagBudgetExhausted.load(std::memory_order_relaxed);
    }
    std::size_t InflightFor(int peer) const {
        std::lock_guard<std::mutex> lk(m_mu);
        auto it = m_inflight.find(peer);
        return it == m_inflight.end() ? 0u : it->second;
    }
    const Config& config() const { return m_cfg; }

private:
    bool Admit(int peer) {
        std::lock_guard<std::mutex> lk(m_mu);
        auto& n = m_inflight[peer];
        if (n >= m_cfg.peer_queue_max) return false;
        ++n;
        return true;
    }
    void Release(int peer) {
        std::lock_guard<std::mutex> lk(m_mu);
        auto it = m_inflight.find(peer);
        if (it != m_inflight.end() && it->second > 0) --it->second;
    }

    SwimDetector& m_detector;
    Config        m_cfg;
    InnerRpc      m_rpc;

    mutable std::mutex                 m_mu;
    std::unordered_map<int, std::size_t> m_inflight;

    std::atomic<std::uint64_t> m_swimDetectorLagErrors{0};
    std::atomic<std::uint64_t> m_swimDetectorLagBudgetExhausted{0};
};

}  // namespace SPTAG::SPANN::Distributed
