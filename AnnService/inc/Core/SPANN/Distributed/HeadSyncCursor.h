// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// HeadSyncCursor — receive-side primitive for the HeadSync pull-RPC family of
// fault cases (headsync-broadcast-partial, headsync-ordering-violation,
// head-replica-drift, headsync-version-cas-failure).
//
// Tracks the highest contiguously-applied (epoch, sequence) per `originOwner`
// observed by this peer. On a gap (entry.sequence > cursor + 1, same epoch, or
// entry.epoch > cursor.epoch + with non-zero sequence ≠ 0) the caller is
// expected to issue a HeadSyncPullRequest for the missing range.
//
// Persistence: cursor state is written to a small append-only journal file
// (one line per advance). Recovery replays the journal and keeps only the max
// (epoch, sequence) per originOwner. We picked a local file over a TiKV-backed
// store for the primitive because:
//   1. The cursor is per-peer state (so it has a natural local home).
//   2. Pull is idempotent: the worst case after a stale cursor is one extra
//      pull window. Durability is a hint, not a correctness requirement —
//      if the journal is lost, the peer pulls from (epoch=0, sequence=0)
//      and the owner caps the response at m_maxEntries.
// Follow-up: a TiKV-backed cursor (or per-owner Merkle metadata) is the right
// long-term home; that's deferred to the head-replica-drift case.

#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>

namespace SPTAG::SPANN {

    /// Action a caller should take after AdvanceOnApply / Observe.
    enum class HeadSyncCursorAction : std::uint8_t {
        Applied        = 0, ///< entry was the next-expected, cursor advanced.
        AlreadySeen    = 1, ///< entry is at or below the cursor; safe to drop.
        GapDetected    = 2, ///< entry is ahead of cursor; defer + request pull.
        EpochAdvanced  = 3, ///< entry's epoch > cursor.epoch; treat as gap and pull.
    };

    struct HeadSyncCursorPos {
        std::uint64_t epoch    = 0;
        std::uint64_t sequence = 0;
        bool operator==(const HeadSyncCursorPos& o) const noexcept {
            return epoch == o.epoch && sequence == o.sequence;
        }
        bool operator<(const HeadSyncCursorPos& o) const noexcept {
            return epoch < o.epoch || (epoch == o.epoch && sequence < o.sequence);
        }
    };

    class HeadSyncCursor {
    public:
        HeadSyncCursor() = default;
        ~HeadSyncCursor() { Close(); }

        HeadSyncCursor(const HeadSyncCursor&) = delete;
        HeadSyncCursor& operator=(const HeadSyncCursor&) = delete;

        /// Open (or create) a journal-backed cursor. `journalPath` may be empty
        /// for ephemeral mode (tests). On open the journal is replayed and the
        /// in-memory map populated with the max (epoch, sequence) per origin.
        /// Returns true on success.
        bool Open(const std::string& journalPath = "") {
            std::lock_guard<std::mutex> lock(m_mu);
            m_journalPath = journalPath;
            m_cursors.clear();
            if (journalPath.empty()) return true;
            // Replay existing journal (best-effort; corrupt lines are skipped).
            std::ifstream in(journalPath);
            if (in.is_open()) {
                std::int32_t  origin = 0;
                std::uint64_t epoch = 0, seq = 0;
                while (in >> origin >> epoch >> seq) {
                    auto& cur = m_cursors[origin];
                    HeadSyncCursorPos pos{epoch, seq};
                    if (cur < pos) cur = pos;
                }
            }
            // Open append handle for subsequent advances.
            m_journal.open(journalPath, std::ios::app);
            return m_journal.is_open();
        }

        void Close() {
            std::lock_guard<std::mutex> lock(m_mu);
            if (m_journal.is_open()) m_journal.close();
        }

        /// Snapshot the cursor for an origin (default if unseen).
        HeadSyncCursorPos Get(std::int32_t originOwner) const {
            std::lock_guard<std::mutex> lock(m_mu);
            auto it = m_cursors.find(originOwner);
            if (it == m_cursors.end()) return {};
            return it->second;
        }

        /// Decide what should happen with an incoming entry, *without*
        /// advancing the cursor. Used for inspection / dry-run tests.
        HeadSyncCursorAction Inspect(std::int32_t originOwner,
                                     std::uint64_t epoch,
                                     std::uint64_t sequence) const {
            std::lock_guard<std::mutex> lock(m_mu);
            auto it = m_cursors.find(originOwner);
            HeadSyncCursorPos cur = (it == m_cursors.end()) ? HeadSyncCursorPos{} : it->second;
            return Decide(cur, epoch, sequence);
        }

        /// Atomically: if the entry is the next-expected (sequence == cur+1
        /// in same epoch, OR epoch advance with sequence==1), advance the
        /// cursor and return Applied; if it's at-or-below cursor, return
        /// AlreadySeen; otherwise return Gap/EpochAdvanced (caller should
        /// queue + trigger pull).
        HeadSyncCursorAction AdvanceOnApply(std::int32_t originOwner,
                                            std::uint64_t epoch,
                                            std::uint64_t sequence) {
            std::lock_guard<std::mutex> lock(m_mu);
            auto& cur = m_cursors[originOwner];
            HeadSyncCursorAction action = Decide(cur, epoch, sequence);
            if (action == HeadSyncCursorAction::Applied) {
                cur.epoch    = epoch;
                cur.sequence = sequence;
                AppendJournalLocked(originOwner, cur);
            }
            return action;
        }

        /// Force the cursor for an origin to (epoch, sequence) if higher.
        /// Used after a successful pull that catches us up beyond what the
        /// per-entry path would. Idempotent.
        bool ForceAtLeast(std::int32_t originOwner,
                          std::uint64_t epoch,
                          std::uint64_t sequence) {
            std::lock_guard<std::mutex> lock(m_mu);
            auto& cur = m_cursors[originOwner];
            HeadSyncCursorPos pos{epoch, sequence};
            if (cur < pos) {
                cur = pos;
                AppendJournalLocked(originOwner, cur);
                return true;
            }
            return false;
        }

        /// Test/inspection helper.
        std::size_t OriginCount() const {
            std::lock_guard<std::mutex> lock(m_mu);
            return m_cursors.size();
        }

    private:
        static HeadSyncCursorAction Decide(const HeadSyncCursorPos& cur,
                                           std::uint64_t epoch,
                                           std::uint64_t sequence) {
            if (epoch < cur.epoch) return HeadSyncCursorAction::AlreadySeen;
            if (epoch == cur.epoch) {
                if (sequence <= cur.sequence)        return HeadSyncCursorAction::AlreadySeen;
                if (sequence == cur.sequence + 1)    return HeadSyncCursorAction::Applied;
                return HeadSyncCursorAction::GapDetected;
            }
            // epoch > cur.epoch: epoch boundary. We accept sequence==1 as the
            // first entry in the new epoch; anything else implies the peer
            // missed entries at the head of the new epoch.
            if (sequence == 1) return HeadSyncCursorAction::Applied;
            return HeadSyncCursorAction::EpochAdvanced;
        }

        void AppendJournalLocked(std::int32_t origin, const HeadSyncCursorPos& pos) {
            if (!m_journal.is_open()) return;
            m_journal << origin << ' ' << pos.epoch << ' ' << pos.sequence << '\n';
            m_journal.flush();
        }

        mutable std::mutex m_mu;
        std::unordered_map<std::int32_t, HeadSyncCursorPos> m_cursors;
        std::string  m_journalPath;
        std::ofstream m_journal;
    };

} // namespace SPTAG::SPANN
