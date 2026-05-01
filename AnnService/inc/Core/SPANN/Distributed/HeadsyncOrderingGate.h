// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// HeadsyncOrderingGate<EntryT> — receive-side primitive for the
// `headsync-ordering-violation` fault case.
//
// Spec:
//   tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//     headsync-ordering-violation.md
//
// Problem (recap from spec):
//   Two head adds applied out-of-order on a follower (entry N+1 before
//   N). Today the receive path applies whatever HeadSyncEntry arrives
//   with no per-(originOwner, epoch) cursor, so a Delete(VID) can land
//   before the matching Add(VID) from a different owner whose batch
//   was reordered by the network. The follower mis-applies, the local
//   head index permanently diverges from the cluster's authoritative
//   set, and queries served from that follower silently regress
//   recall.
//
// Recovery contract (from spec, items 1-6):
//   1. Every HeadSync packet carries (originOwner, epoch, sequence).
//   2. The follower keeps latestApplied[originOwner] -> (epoch, seq)
//      via the existing prim/headsync-pull-rpc::HeadSyncCursor.
//   3. On receive:
//      * sequence == latestApplied + 1  -> apply, advance, drain any
//        contiguous pending entries.
//      * sequence <= latestApplied      -> drop as duplicate.
//      * sequence  > latestApplied + 1  -> park into a per-origin
//        pending-apply buffer, trigger a scan_range-style pull RPC
//        (callback supplied by the caller; wires to
//        HandleHeadSyncPullRequest in production), and retry the
//        ordered drain after the pull returns the missing entries.
//   4. Cross-owner ordering is *not* required to be globally
//      serialised - per-owner cursor is sufficient.
//
// Env-gate:
//   SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION (off by default). When
//   unset Armed() returns false and TryApply returns Status::Dormant
//   without touching the cursor, the pending buffer, the pull
//   callback or the counters - production hot path is byte-identical
//   to merge base. When set the gate is active: gaps are refused,
//   scan_range fills are triggered, and in-order resume is recorded.
//
// Counters (atomic):
//   - outOfOrderApplyDetected - entries that arrived ahead of the
//     cursor (gap or epoch-advance with sequence != 1). One bump per
//     out-of-order arrival.
//   - cursorGapRefused        - entries the gate held back (parked)
//     instead of applying. Tracks the "refuse" leg of the contract.
//   - scanRangeFillTriggered  - number of pull-RPC fills the gate
//     issued. Each fill covers exactly one detected gap window.
//   - inOrderResumed          - number of times the gate drained >=1
//     pending entries after a fill / late arrival. Once per drain
//     pass regardless of how many entries the drain applied.
//
// Defers-to-primitive:
//   - prim/headsync-pull-rpc - HeadSyncCursor (already merged here).
//   - prim/op-id-idempotency - receive-side dedup is orthogonal: even
//     with op-id, an out-of-order Delete carries a fresh op-id and
//     would still mis-apply. The two compose.

#pragma once

#include "inc/Core/SPANN/Distributed/HeadSyncCursor.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace SPTAG::SPANN {

    /// Result of one TryApply call.
    enum class HeadsyncOrderingStatus : std::uint8_t {
        Dormant          = 0, ///< env-off; gate is silent.
        AppliedInOrder   = 1, ///< sequence == cursor + 1 -> applied immediately.
        AlreadySeen      = 2, ///< sequence <= cursor -> dropped as duplicate.
        GapDeferred      = 3, ///< gap remained after pull (or pull empty);
                              ///< entry parked.
        FilledAndResumed = 4, ///< gap detected, pull succeeded, missing
                              ///< entries applied in order, then this entry
                              ///< (and any parked entries that became
                              ///< contiguous) applied.
        PullFailed       = 5, ///< pull RPC reported failure; entry parked.
    };

    /// One entry in the pull-RPC response. Mirrors the on-wire
    /// envelope: (epoch, sequence, opaque payload). Tests typically
    /// use EntryT = SizeType; production wires HeadSyncEntry whole.
    template <typename EntryT>
    struct HeadsyncPullEntry {
        std::uint64_t epoch;
        std::uint64_t sequence;
        EntryT        payload;
    };

    /// Header-only ordering gate. Templated on the payload type so
    /// the pending-buffer can hold typed payloads without type-
    /// erasure machinery. One instance per follower (per payload
    /// type).
    template <typename EntryT>
    class HeadsyncOrderingGate {
    public:
        struct Counters {
            std::uint64_t outOfOrderApplyDetected;
            std::uint64_t cursorGapRefused;
            std::uint64_t scanRangeFillTriggered;
            std::uint64_t inOrderResumed;
        };

        HeadsyncOrderingGate() { m_cursor.Open(""); }

        static bool Armed() {
            return std::getenv("SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION") != nullptr;
        }

        Counters GetCounters() const {
            return Counters{
                m_outOfOrderApplyDetected.load(),
                m_cursorGapRefused.load(),
                m_scanRangeFillTriggered.load(),
                m_inOrderResumed.load(),
            };
        }

        /// Cursor snapshot for an origin (test/inspection helper).
        HeadSyncCursorPos GetCursor(std::int32_t originOwner) const {
            return m_cursor.Get(originOwner);
        }

        /// Pending-buffer size for an origin (test/inspection helper).
        std::size_t PendingSize(std::int32_t originOwner) const {
            std::lock_guard<std::mutex> lock(m_pendingMu);
            auto it = m_pending.find(originOwner);
            return (it == m_pending.end()) ? 0u : it->second.size();
        }

        /// TryApply: per-spec receive-path.
        ///
        /// `apply(originOwner, epoch, sequence, payload)` - actually
        /// mutate the local head index; must be idempotent w.r.t.
        /// concurrent identical entries.
        ///
        /// `pullSince(originOwner, fromEpoch, fromSeq, &outEntries)` -
        /// issue a scan_range-style fill request to the owner. Must
        /// return true on success with outEntries holding every entry
        /// from (fromEpoch, fromSeq) inclusive up to and including
        /// the current gap entry. Returns false on RPC failure - gate
        /// parks the entry and the caller can retry on the next
        /// anti-entropy tick.
        ///
        /// Dormant when env-off: no cursor mutation, no apply, no
        /// pull, no counter writes.
        template <typename ApplyFn, typename PullFn>
        HeadsyncOrderingStatus TryApply(std::int32_t  originOwner,
                                        std::uint64_t epoch,
                                        std::uint64_t sequence,
                                        EntryT        payload,
                                        ApplyFn&&     apply,
                                        PullFn&&      pullSince)
        {
            if (!Armed()) {
                return HeadsyncOrderingStatus::Dormant;
            }

            auto action = m_cursor.Inspect(originOwner, epoch, sequence);
            if (action == HeadSyncCursorAction::AlreadySeen) {
                return HeadsyncOrderingStatus::AlreadySeen;
            }
            if (action == HeadSyncCursorAction::Applied) {
                apply(originOwner, epoch, sequence, payload);
                (void)m_cursor.AdvanceOnApply(originOwner, epoch, sequence);
                if (DrainPending(originOwner, apply)) {
                    m_inOrderResumed.fetch_add(1);
                }
                return HeadsyncOrderingStatus::AppliedInOrder;
            }

            // Gap or epoch-advance: refuse to apply out of order.
            m_outOfOrderApplyDetected.fetch_add(1);
            m_cursorGapRefused.fetch_add(1);
            ParkEntry(originOwner, epoch, sequence, payload);

            HeadSyncCursorPos cur = m_cursor.Get(originOwner);
            std::uint64_t fromEpoch = cur.epoch;
            std::uint64_t fromSeq   = cur.sequence + 1;

            std::vector<HeadsyncPullEntry<EntryT>> filled;
            m_scanRangeFillTriggered.fetch_add(1);
            bool ok = pullSince(originOwner, fromEpoch, fromSeq, filled);
            if (!ok) {
                return HeadsyncOrderingStatus::PullFailed;
            }

            for (auto& e : filled) {
                ParkEntry(originOwner, e.epoch, e.sequence,
                          std::move(e.payload));
            }

            bool any = DrainPending(originOwner, apply);
            if (any) m_inOrderResumed.fetch_add(1);

            HeadSyncCursorPos after = m_cursor.Get(originOwner);
            bool reached =
                (after.epoch  > epoch) ||
                (after.epoch == epoch && after.sequence >= sequence);
            return reached
                ? HeadsyncOrderingStatus::FilledAndResumed
                : HeadsyncOrderingStatus::GapDeferred;
        }

    private:
        struct PendingEntry {
            EntryT payload;
        };
        using PendingMap =
            std::map<std::pair<std::uint64_t, std::uint64_t>, PendingEntry>;

        void ParkEntry(std::int32_t originOwner,
                       std::uint64_t epoch,
                       std::uint64_t sequence,
                       EntryT payload)
        {
            std::lock_guard<std::mutex> lock(m_pendingMu);
            auto& bucket = m_pending[originOwner];
            // Idempotent: same (epoch, seq) overwrites; payload is
            // owner-authoritative.
            bucket[std::make_pair(epoch, sequence)] =
                PendingEntry{std::move(payload)};
        }

        template <typename ApplyFn>
        bool DrainPending(std::int32_t originOwner, ApplyFn& apply) {
            std::lock_guard<std::mutex> lock(m_pendingMu);
            auto it = m_pending.find(originOwner);
            if (it == m_pending.end()) return false;
            auto& bucket = it->second;
            bool any = false;
            while (!bucket.empty()) {
                auto firstIt = bucket.begin();
                std::uint64_t e = firstIt->first.first;
                std::uint64_t s = firstIt->first.second;
                auto act = m_cursor.Inspect(originOwner, e, s);
                if (act == HeadSyncCursorAction::AlreadySeen) {
                    bucket.erase(firstIt);
                    continue;
                }
                if (act != HeadSyncCursorAction::Applied) break;
                apply(originOwner, e, s, firstIt->second.payload);
                (void)m_cursor.AdvanceOnApply(originOwner, e, s);
                bucket.erase(firstIt);
                any = true;
            }
            if (bucket.empty()) m_pending.erase(it);
            return any;
        }

        HeadSyncCursor m_cursor;

        mutable std::mutex                          m_pendingMu;
        std::unordered_map<std::int32_t, PendingMap> m_pending;

        std::atomic<std::uint64_t> m_outOfOrderApplyDetected{0};
        std::atomic<std::uint64_t> m_cursorGapRefused{0};
        std::atomic<std::uint64_t> m_scanRangeFillTriggered{0};
        std::atomic<std::uint64_t> m_inOrderResumed{0};
    };

} // namespace SPTAG::SPANN
