// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-grpc-timeout-storm
//
// Goal: A TiKV write travels over a gRPC channel. Under load
// or a flapping network the per-RPC timeout can fire even when
// the underlying region is healthy and the store is reachable.
// The client-side recovery for a gRPC timeout is straightforward
// -- back off, retry on the same (or freshly resolved) replica
// -- but a *storm* of timeouts across the available replicas
// must be bounded by a RetryBudget so the client cannot loop
// forever masking a real availability outage. This wrapper
// models the cross-replica gRPC-timeout retry-cap invariant:
// every timeout observed must be retried exactly once with a
// backoff, and the total chain of retries is bounded by the
// budget.
//
// Distinct from tikv-region-error-retry-cap: that case covers
// in-band RegionError responses (epoch/leader/region cache
// problems). gRPC timeout is the transport-layer failure where
// no in-band response arrives at all. Distinct from
// tikv-slow-disk / tikv-rocksdb-write-stall: those return
// success-with-latency or a stall token; here the RPC simply
// never returns within the deadline.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_GRPC_TIMEOUT_STORM`
//     unset/0): wrapper is dormant -- drives one pass-through
//     write against the primary replica; no timeout counter
//     moves.
//   * env-armed: timeoutPatternPerReplica is a per-replica
//     vector of "attempts-until-success" counts. A value of N>=0
//     means the first N attempts on that replica time out, the
//     (N+1)th lands. A value of -1 means the replica always
//     times out (failover required). On each timeout the
//     wrapper ++m_grpcTimeoutObserved, consumes one budget
//     tick, ++m_retryWithBackoff, and retries on the same
//     replica until that replica's pattern exhausts or its
//     count drains to zero. On replica-exhaustion the wrapper
//     fails over to the next replica. When a write lands
//     successfully ++m_writeSucceededAfterRetry. If the
//     RetryBudget is exhausted before any replica lands,
//     ++m_clientErrorReturned and return
//     ErrorCode::GrpcTimeoutStormRetryCapExceeded.
//
// Counters required by case-md:
//   - m_grpcTimeoutObserved
//   - m_retryWithBackoff
//   - m_writeSucceededAfterRetry
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

/// One outcome of a gated gRPC-timeout-storm write.
struct TikvGrpcTimeoutStormOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected                         = false;
    std::size_t grpc_timeout_obs          = 0;
    std::size_t retry_with_backoff        = 0;
    std::size_t write_succeeded_after     = 0;
    int attempted_writes                  = 0;
    int landed_on_replica                 = -1;
};

class TikvGrpcTimeoutStormGated {
public:
    using ReplicaId = std::int32_t;

    struct Config {
        std::chrono::milliseconds per_write_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Per-attempt write callable: drive the write against
    /// `replicaId`. Returns Success on landing.
    using ReplicaWrite = std::function<ErrorCode(ReplicaId replicaId,
                                                 std::chrono::milliseconds deadline)>;

    TikvGrpcTimeoutStormGated(Config cfg, ReplicaWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_GRPC_TIMEOUT_STORM");
        return v && *v && std::string(v) != "0";
    }

    /// Drive a single write across `replicas`. Each entry of
    /// `timeoutPatternPerReplica` is the number of timeouts
    /// to inject on that replica before the write lands (-1
    /// meaning the replica always times out -- failover
    /// required). The wrapper retries within a replica's
    /// pattern until it lands, fails over on exhaustion, and
    /// caps the cross-replica chain by the RetryBudget.
    TikvGrpcTimeoutStormOutcome WriteWithGrpcTimeoutGuard(
            std::vector<ReplicaId> replicas,
            std::vector<int> timeoutPatternPerReplica,
            Helper::RetryBudget& budget) {
        TikvGrpcTimeoutStormOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        if (!envArmed) {
            ++o.attempted_writes;
            ReplicaId primary = replicas.empty() ? ReplicaId{0} : replicas.front();
            ErrorCode rc = m_write(primary, m_cfg.per_write_timeout);
            if (rc == ErrorCode::Success) {
                o.landed_on_replica = primary;
            } else {
                o.code = rc;
            }
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        o.injected = true;

        for (std::size_t i = 0; i < replicas.size(); ++i) {
            int pattern = i < timeoutPatternPerReplica.size()
                              ? timeoutPatternPerReplica[i]
                              : 0;

            // -1 sentinel = replica always times out. Treat as
            // a single timeout observation that consumes one
            // budget tick, then fail over.
            const bool alwaysTimesOut = (pattern < 0);
            int timeoutsOnThisReplica = alwaysTimesOut ? 1 : pattern;

            bool exhaustedReplica = false;
            while (timeoutsOnThisReplica > 0) {
                ++m_grpcTimeoutObserved;
                ++o.grpc_timeout_obs;

                if (!budget.should_retry() || budget.remaining().count() <= 0) {
                    ++m_clientErrorReturned;
                    o.code = ErrorCode::GrpcTimeoutStormRetryCapExceeded;
                    o.elapsed = std::chrono::steady_clock::now() - t0;
                    return o;
                }
                (void)budget.record_attempt();

                ++m_retryWithBackoff;
                ++o.retry_with_backoff;
                --timeoutsOnThisReplica;
                if (alwaysTimesOut) {
                    exhaustedReplica = true;
                    break;
                }
            }

            if (exhaustedReplica) {
                // Failover to the next replica.
                continue;
            }

            // This replica's timeout window has drained -- try
            // to land the write here.
            auto rem = budget.remaining();
            auto deadline = rem.count() > 0
                ? std::min<std::chrono::milliseconds>(m_cfg.per_write_timeout, rem)
                : m_cfg.per_write_timeout;
            ++o.attempted_writes;
            ErrorCode rc = m_write(replicas[i], deadline);
            if (rc == ErrorCode::Success) {
                ++m_writeSucceededAfterRetry;
                ++o.write_succeeded_after;
                o.landed_on_replica = replicas[i];
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            // Non-success non-timeout: surface as client error.
            ++m_clientErrorReturned;
            o.code = rc;
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        // Every replica timed out and we failed over off the end.
        ++m_clientErrorReturned;
        o.code = ErrorCode::GrpcTimeoutStormRetryCapExceeded;
        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    std::uint64_t GrpcTimeoutObserved() const {
        return m_grpcTimeoutObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t RetryWithBackoff() const {
        return m_retryWithBackoff.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteSucceededAfterRetry() const {
        return m_writeSucceededAfterRetry.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config       m_cfg;
    ReplicaWrite m_write;

    std::atomic<std::uint64_t> m_grpcTimeoutObserved{0};
    std::atomic<std::uint64_t> m_retryWithBackoff{0};
    std::atomic<std::uint64_t> m_writeSucceededAfterRetry{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
