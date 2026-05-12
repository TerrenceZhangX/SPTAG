// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Header-only test/integration wrapper for fault case
//   tikv-region-leader-transfer-mid-rpc
//
// Goal: A TiKV region leader-transfer happens mid-RPC. The old
// leader returns NotLeader{leader: <new-peer>} (a TiKV-side hint
// indicating which peer is the new leader). A correct client
// must:
//   * observe the NotLeader hint (++m_leaderTransferObserved),
//   * update its leader cache to the new peer
//     (++m_leaderCacheUpdated),
//   * retry the write against the new leader,
//   * bound the retry depth via a RetryBudget so a chain of
//     rapid leader transfers cannot become an unbounded
//     re-route storm.
//
// Distinct from tikv-region-merge-during-writes: merge collapses
// N regions into 1 (region-id remap); leader-transfer keeps the
// region id but rotates the peer that owns the lease. The
// wrapper expresses the transfer chain as a `vector<peerId>` --
// each retry pops the front and treats it as the next observed
// leader hint; an empty chain means the leader is stable.
//
// Contract:
//   * env-off (`SPTAG_FAULT_TIKV_REGION_LEADER_TRANSFER_MID_RPC`
//     unset/0): wrapper is dormant -- drives one pass-through
//     write against `currentLeader`; no leader-transfer counter
//     moves.
//   * env-armed: while leaderTransferChain is non-empty, the
//     wrapper treats the write as having returned NotLeader
//     (++m_leaderTransferObserved), consumes one budget tick,
//     pops the front of the chain into currentLeader
//     (++m_leaderCacheUpdated), and retries. When the chain is
//     empty the write lands against the (now-current) leader
//     (++m_writeRoutedToNewLeader). If the RetryBudget is
//     exhausted, ++m_clientErrorReturned, return
//     ErrorCode::LeaderTransferRetryCapExceeded.
//
// Counters required by case-md:
//   - m_leaderTransferObserved
//   - m_leaderCacheUpdated
//   - m_writeRoutedToNewLeader
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

/// One outcome of a gated leader-transfer-mid-rpc write.
struct TikvRegionLeaderTransferMidRpcOutcome {
    ErrorCode code = ErrorCode::Success;
    std::chrono::steady_clock::duration elapsed{0};
    bool injected                     = false;
    std::size_t leader_transfer_obs   = 0;
    std::size_t leader_cache_updated  = 0;
    std::size_t routed_to_new_leader  = 0;
    int attempted_writes              = 0;
};

class TikvRegionLeaderTransferMidRpcGated {
public:
    using RegionId = std::int64_t;
    using PeerId   = std::int64_t;

    struct Config {
        std::chrono::milliseconds per_write_timeout{1000};
        Helper::RetryBudget::Config budget = Helper::RetryBudget::DefaultConfig();
    };

    /// Per-attempt write callable: given (region, peer), drive
    /// the write to that peer. Return Success on landing.
    using PeerWrite = std::function<ErrorCode(RegionId regionId,
                                              PeerId   peerId,
                                              std::chrono::milliseconds deadline)>;

    TikvRegionLeaderTransferMidRpcGated(Config cfg, PeerWrite write)
        : m_cfg(std::move(cfg)), m_write(std::move(write)) {}

    static bool EnvArmed() {
        const char* v = std::getenv("SPTAG_FAULT_TIKV_REGION_LEADER_TRANSFER_MID_RPC");
        return v && *v && std::string(v) != "0";
    }

    /// Drive a single write to `regionId`. If env-armed and the
    /// `leaderTransferChain` is non-empty, each entry models one
    /// NotLeader hint; the wrapper retries on the popped peer
    /// until the chain is drained or the RetryBudget is spent.
    TikvRegionLeaderTransferMidRpcOutcome WriteWithLeaderGuard(
            RegionId regionId,
            PeerId   currentLeader,
            std::vector<PeerId> leaderTransferChain,
            Helper::RetryBudget& budget) {
        TikvRegionLeaderTransferMidRpcOutcome o;
        const auto t0 = std::chrono::steady_clock::now();

        const bool envArmed = EnvArmed();

        if (!envArmed) {
            // Env-off: one pass-through write against the
            // declared currentLeader. No leader-transfer counter
            // moves.
            ++o.attempted_writes;
            ErrorCode rc = m_write(regionId, currentLeader, m_cfg.per_write_timeout);
            if (rc != ErrorCode::Success) {
                o.code = rc;
            }
            o.elapsed = std::chrono::steady_clock::now() - t0;
            return o;
        }

        o.injected = true;

        // Env-armed: pop each chain entry as a NotLeader hint
        // until the chain is empty, then land the write.
        std::size_t idx = 0;
        while (idx < leaderTransferChain.size()) {
            ++m_leaderTransferObserved;
            ++o.leader_transfer_obs;

            if (!budget.should_retry() || budget.remaining().count() <= 0) {
                ++m_clientErrorReturned;
                o.code = ErrorCode::LeaderTransferRetryCapExceeded;
                o.elapsed = std::chrono::steady_clock::now() - t0;
                return o;
            }
            (void)budget.record_attempt();

            currentLeader = leaderTransferChain[idx++];
            ++m_leaderCacheUpdated;
            ++o.leader_cache_updated;
        }

        // Chain drained: land the write on the (now-current)
        // leader.
        auto rem = budget.remaining();
        auto deadline = rem.count() > 0
            ? std::min<std::chrono::milliseconds>(m_cfg.per_write_timeout, rem)
            : m_cfg.per_write_timeout;
        ++o.attempted_writes;
        ErrorCode rc = m_write(regionId, currentLeader, deadline);
        if (rc == ErrorCode::Success) {
            ++m_writeRoutedToNewLeader;
            ++o.routed_to_new_leader;
        } else {
            ++m_clientErrorReturned;
            o.code = rc;
        }

        o.elapsed = std::chrono::steady_clock::now() - t0;
        return o;
    }

    std::uint64_t LeaderTransferObserved() const {
        return m_leaderTransferObserved.load(std::memory_order_relaxed);
    }
    std::uint64_t LeaderCacheUpdated() const {
        return m_leaderCacheUpdated.load(std::memory_order_relaxed);
    }
    std::uint64_t WriteRoutedToNewLeader() const {
        return m_writeRoutedToNewLeader.load(std::memory_order_relaxed);
    }
    std::uint64_t ClientErrorReturned() const {
        return m_clientErrorReturned.load(std::memory_order_relaxed);
    }
    const Config& config() const { return m_cfg; }

private:
    Config    m_cfg;
    PeerWrite m_write;

    std::atomic<std::uint64_t> m_leaderTransferObserved{0};
    std::atomic<std::uint64_t> m_leaderCacheUpdated{0};
    std::atomic<std::uint64_t> m_writeRoutedToNewLeader{0};
    std::atomic<std::uint64_t> m_clientErrorReturned{0};
};

}  // namespace SPTAG::SPANN::Distributed
