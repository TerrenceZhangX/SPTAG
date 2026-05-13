// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   pd-reconnect-during-retry
//
// Goal: A TiKV client write needs PD for region-route resolution
// and for retry decisions. When the PD client's TCP/gRPC session
// flaps mid-retry the wrapper must (a) detect the disconnect,
// (b) attempt a reconnect with a bounded delay, and (c) bound
// the cross-endpoint chain of disconnect/reconnect cycles by a
// RetryBudget so the client cannot loop forever masking a
// genuine PD outage. This wrapper models the PD-client
// disconnect-during-retry invariant: every disconnect observed
// must be answered by one reconnect attempt against the same
// (or next) endpoint, the cross-endpoint chain is bounded by
// the budget, and the write only lands once a stable PD
// session is in hand.
//
// Distinct from pd-leader-failover: that case covers PD's
// internal Raft leader handoff (a server-side event surfaced
// in-band). pd-reconnect-during-retry is the client-side
// transport failure where the PD session itself drops.
// Distinct from tikv-grpc-timeout-storm: that one is the
// data-plane RPC against a store; this one is the control-plane
// session against PD.
//
// Contract:
//   * env-off (`SPTAG_FAULT_PD_RECONNECT_DURING_RETRY`
//     unset/0): wrapper is dormant -- drives one pass-through
//     write against the primary PD endpoint; no disconnect
//     counter moves.
//   * env-armed: reconnectDelayMsPerAttempt is a per-endpoint
//     vector of "disconnect/reconnect cycles required before
//     this endpoint is stable" (the first entry models the
//     initial disconnect detected on the primary, subsequent
//     entries are the per-endpoint reconnect-delay budgets). A
//     value of N>=0 means N disconnect/reconnect cycles are
//     observed on that endpoint before the write lands; a
//     value of -1 means the endpoint never reconnects (the
//     wrapper must fail over to the next endpoint). On each
//     disconnect the wrapper ++m_pdDisconnectObserved, consumes
//     one budget tick, ++m_pdReconnectAttempted, and continues
//     against the same endpoint until its pattern drains or it
//     exhausts. On endpoint exhaustion the wrapper fails over
//     to the next endpoint. When a write lands successfully
//     ++m_writeSucceededAfterReconnect. If the RetryBudget is
//     exhausted before any endpoint lands,
//     ++m_clientErrorReturned and return
//     ErrorCode::PdReconnectRetryCapExceeded.
//
// Counters required by case-md:
//   - m_pdDisconnectObserved
//   - m_pdReconnectAttempted
//   - m_writeSucceededAfterReconnect
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

/// One outcome of a gated pd-reconnect-during-retry write.
struct PdReconnectDuringRetryOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected                         = false;
    std::size_t pd_disconnect_obs         = 0;
    std::size_t pd_reconnect_attempted    = 0;
    std::size_t write_succeeded_after     = 0;
    int attempted_writes                  = 0;
    int landed_on_endpoint                = -1;
};

class PdReconnectDuringRetryGated {
public:
    using EndpointId = std::int32_t;

    struct Config {
        std::chrono::milliseconds per_write_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Per-attempt write callable: drive the write against the
    /// PD-resolved route currently anchored on `endpointId`.
    /// Returns Success on landing.
    using EndpointWrite = std::function<ErrorCode(EndpointId endpointId,
                                                  std::chrono::milliseconds deadline)>;

    PdReconnectDuringRetryGated(Config cfg, EndpointWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_PD_RECONNECT_DURING_RETRY");
        return v && *v && std::string(v) != "0";
    }

    /// Drive a single write across `pdEndpoints`. Each entry of
    /// `reconnectDelayMsPerAttempt` is the number of
    /// disconnect/reconnect cycles to inject on that endpoint
    /// before the write lands (-1 meaning the endpoint never
    /// reconnects -- failover required). The wrapper retries
    /// within an endpoint's pattern until it stabilizes, fails
    /// over on exhaustion, and caps the cross-endpoint chain by
    /// the RetryBudget.
    PdReconnectDuringRetryOutcome WriteWithPdReconnectGuard(
            std::vector<EndpointId> pdEndpoints,
            std::vector<int> reconnectDelayMsPerAttempt,
            Helper::RetryBudget& budget) {
        PdReconnectDuringRetryOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        if (!envArmed) {
            ++o.attempted_writes;
            EndpointId primary = pdEndpoints.empty()
                                     ? EndpointId{0}
                                     : pdEndpoints.front();
            ErrorCode rc = m_write(primary, m_cfg.per_write_timeout);
            if (rc == ErrorCode::Success) {
                o.landed_on_endpoint = primary;
            } else {
                o.code = rc;
            }
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        o.injected = true;

        for (std::size_t i = 0; i < pdEndpoints.size(); ++i) {
            int pattern = i < reconnectDelayMsPerAttempt.size()
                              ? reconnectDelayMsPerAttempt[i]
                              : 0;

            // -1 sentinel = endpoint never reconnects. Treat as
            // a single disconnect observation that consumes one
            // budget tick, then fail over.
            const bool neverReconnects = (pattern < 0);
            int disconnectsOnThisEndpoint = neverReconnects ? 1 : pattern;

            bool exhaustedEndpoint = false;
            while (disconnectsOnThisEndpoint > 0) {
                ++m_pdDisconnectObserved;
                ++o.pd_disconnect_obs;

                if (!budget.should_retry() || budget.remaining().count() <= 0) {
                    ++m_clientErrorReturned;
                    o.code = ErrorCode::PdReconnectRetryCapExceeded;
                    o.elapsed = std::chrono::steady_clock::now() - t0;
                    return o;
                }
                (void)budget.record_attempt();

                ++m_pdReconnectAttempted;
                ++o.pd_reconnect_attempted;
                --disconnectsOnThisEndpoint;
                if (neverReconnects) {
                    exhaustedEndpoint = true;
                    break;
                }
            }

            if (exhaustedEndpoint) {
                // Failover to the next PD endpoint.
                continue;
            }

            // This endpoint's disconnect cycle has drained --
            // attempt to land the write here.
            auto rem = budget.remaining();
            auto deadline = rem.count() > 0
                ? std::min<std::chrono::milliseconds>(m_cfg.per_write_timeout, rem)
                : m_cfg.per_write_timeout;
            ++o.attempted_writes;
            ErrorCode rc = m_write(pdEndpoints[i], deadline);
            if (rc == ErrorCode::Success) {
                ++m_writeSucceededAfterReconnect;
                ++o.write_succeeded_after;
                o.landed_on_endpoint = pdEndpoints[i];
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            // Non-success non-disconnect: surface as client error.
            ++m_clientErrorReturned;
            o.code = rc;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // Every endpoint disconnected and we failed over off the
        // end of the PD ring.
        ++m_clientErrorReturned;
        o.code = ErrorCode::PdReconnectRetryCapExceeded;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    std::uint64_t PdDisconnectObserved() const {
        return m_pdDisconnectObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t PdReconnectAttempted() const {
        return m_pdReconnectAttempted.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteSucceededAfterReconnect() const {
        return m_writeSucceededAfterReconnect.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config        m_cfg;
    EndpointWrite m_write;

    std::atomic<std::uint64_t> m_pdDisconnectObserved{0};
    std::atomic<std::uint64_t> m_pdReconnectAttempted{0};
    std::atomic<std::uint64_t> m_writeSucceededAfterReconnect{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
