// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration dispatcher wrapper for fault case
//   compute-worker-die-mid-query
//
// Goal:  when a compute worker dies mid-query (SIGKILL or host crash
//        during an in-flight RawBatchGet), the dispatcher must surface
//        a *defined* ErrorCode to the client and reroute subsequent
//        queries to a healthy peer (per SWIM suspect + retry-budget
//        primitives), instead of hanging on the dead TCP socket.
//
// Contract:
//   * env-off (`SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY` unset/0):
//     wrapper is dormant -- forwards the inner RawBatchGet RPC
//     unchanged and never increments any counter.
//   * env-armed: before each call we consult the detector:
//       - primary Dead   -> immediate ErrorCode::OwnerSuspect (no RPC,
//                           m_clientErrorReturned++).
//       - primary Suspect/Alive -> issue the inner RPC bounded by the
//         shared RetryBudget. On RPC failure (worker died mid-query
//         or RST):
//           1. m_queryWorkerDied++.
//           2. iterate healthy reroute peers (per detector view); on
//              first Success return Success and m_rerouted++.
//           3. if no healthy reroute peer answers, return
//              ErrorCode::OwnerDead and m_clientErrorReturned++.
//
// Counters required by case-md:
//   - m_queryWorkerDied
//   - m_rerouted
//   - m_clientErrorReturned
//
// This header is dependency-light on purpose: callers inject the inner
// RPC by std::function so prod code (DispatcherNode::BatchRouteSearch)
// and the in-process fault-case test can both drive it.

#pragma once

#include "inc/Core/Common.h"
#include "inc/Core/SPANN/Distributed/SwimDetector.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <string>
#include <vector>

namespace SPTAG::SPANN::Distributed {

/// One outcome of a gated query attempt.
struct QueryGatedOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool primary_dead   = false;  // detector reported primary Dead
    bool primary_failed = false;  // primary RPC returned non-Success
    bool rerouted       = false;  // a reroute peer answered Success
    int  served_by      = -1;     // peer that ultimately answered (-1 = none)
};

class QueryDispatcherGated {
public:
    struct Config {
        std::chrono::milliseconds per_call_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Inner per-peer query RPC. Returns Success on timely completion
    /// or any other ErrorCode on failure / timeout / RST.
    using QueryRpc = std::function<ErrorCode(int peer,
                                             std::chrono::milliseconds deadline)>;

    QueryDispatcherGated(SwimDetector& detector, Config cfg, QueryRpc rpc)
        : m_detector(detector), m_cfg(std::move(cfg)), m_rpc(std::move(rpc)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY");
        return v && *v && std::string(v) != "0";
    }

    /// Issue a query against `primary`. If `primary` is dead (or fails
    /// mid-query), fall back through `reroute_peers` in order.
    QueryGatedOutcome Call(int primary,
                           const std::vector<int>& reroute_peers,
                           Helper::RetryBudget& budget) {
        QueryGatedOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        if (!EnvArmed()) {
            o.code = m_rpc(primary, m_cfg.per_call_timeout);
            o.served_by = (o.code == ErrorCode::Success) ? primary : -1;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // 1) Detector veto: primary already known Dead -> fast-fail without RPC.
        MemberState st;
        if (m_detector.TryGetMember(primary, st)
            && st.health == MemberHealth::Dead) {
            ++m_clientErrorReturned;
            o.code = ErrorCode::OwnerSuspect;
            o.primary_dead = true;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // 2) Bounded primary RPC.
        auto rem = budget.remaining();
        auto deadline = std::min<std::chrono::milliseconds>(
            m_cfg.per_call_timeout, rem);
        if (deadline.count() <= 0) {
            ++m_clientErrorReturned;
            o.code = ErrorCode::RetryBudgetExceeded;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        ErrorCode rc = m_rpc(primary, deadline);
        if (rc == ErrorCode::Success) {
            o.code = ErrorCode::Success;
            o.served_by = primary;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // 3) Primary RPC failed -> worker died (or partial result lost).
        ++m_queryWorkerDied;
        o.primary_failed = true;
        (void)budget.record_attempt();

        // 4) Reroute: try each healthy peer in order.
        for (int peer : reroute_peers) {
            MemberState pst;
            bool known = m_detector.TryGetMember(peer, pst);
            if (known && pst.health == MemberHealth::Dead) continue;

            auto rem2 = budget.remaining();
            if (rem2.count() <= 0 || !budget.should_retry()) break;
            auto dl2 = std::min<std::chrono::milliseconds>(
                m_cfg.per_call_timeout, rem2);
            ErrorCode rc2 = m_rpc(peer, dl2);
            if (rc2 == ErrorCode::Success) {
                ++m_rerouted;
                o.code = ErrorCode::Success;
                o.rerouted = true;
                o.served_by = peer;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            (void)budget.record_attempt();
        }

        // 5) No healthy reroute peer answered -> defined client error.
        ++m_clientErrorReturned;
        o.code = ErrorCode::OwnerDead;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    // -- counters -----------------------------------------------------

    std::uint64_t QueryWorkerDied() const {
        return m_queryWorkerDied.load(std::memory_order_relaxed);
    }
    std::uint64_t Rerouted() const {
        return m_rerouted.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    SwimDetector& m_detector;
    Config        m_cfg;
    QueryRpc      m_rpc;

    std::atomic<std::uint64_t> m_queryWorkerDied{0};
    std::atomic<std::uint64_t> m_rerouted{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
