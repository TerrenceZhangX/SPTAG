// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Primitive: HeadSyncPullRPC
//
// Validates the in-process behaviour of the shared mechanism that the
// HeadSync-related fault cases depend on:
//   - headsync-broadcast-partial    (canonical: pull-on-reconnect)
//   - headsync-ordering-violation   (gap-fill via pull)
//   - head-replica-drift            (Merkle-audit repair via pull RPC)
//   - headsync-version-cas-failure  (replay missing entries)
//
// Scope: cursor + pull request/response *protocol*. We don't exercise the
// real socket layer here (covered separately by docker harness if needed).
// What we cover:
//   1. Cursor monotonicity and idempotent advance.
//   2. Gap detection routes the caller to issue a pull.
//   3. Pull request/response round-trip through serialization.
//   4. Pull provider stub returns Ok with entries; cursor catches up.
//   5. NotAvailable response is surfaced and the caller invokes the
//      scan_range fallback hook (test stub).
//   6. Cursor durability: write to a journal, re-open, recover.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/HeadSyncCursor.h"
#include "inc/Core/SPANN/Distributed/DistributedProtocol.h"

#include <atomic>
#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::SPANN;

namespace {

HeadSyncMetaEntry MakeEntry(std::int32_t origin, std::uint64_t epoch,
                            std::uint64_t seq, SizeType vid,
                            HeadSyncEntry::Op op = HeadSyncEntry::Op::Add) {
    HeadSyncMetaEntry e;
    e.originOwner = origin;
    e.epoch = epoch;
    e.sequence = seq;
    e.entry.op = op;
    e.entry.headVID = vid;
    if (op == HeadSyncEntry::Op::Add) {
        // Synthetic 4-byte vector
        e.entry.headVector.assign(4, '\xAB');
    }
    return e;
}

std::string TmpJournalPath(const std::string& tag) {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto path = std::filesystem::temp_directory_path() /
                ("headsync_pull_test_" + tag + "_" + std::to_string(now) + ".log");
    return path.string();
}

}  // namespace

BOOST_AUTO_TEST_SUITE(HeadSyncPullRPCTest)

// ----- Cursor unit behaviour --------------------------------------------

BOOST_AUTO_TEST_CASE(CursorStartsAtZeroAndAcceptsFirstEntry) {
    HeadSyncCursor cur;
    BOOST_REQUIRE(cur.Open());
    BOOST_CHECK_EQUAL(cur.OriginCount(), 0u);

    auto pos = cur.Get(7);
    BOOST_CHECK_EQUAL(pos.epoch, 0u);
    BOOST_CHECK_EQUAL(pos.sequence, 0u);

    // First entry of epoch 0 is sequence 1.
    BOOST_CHECK(cur.AdvanceOnApply(7, 0, 1) == HeadSyncCursorAction::Applied);
    BOOST_CHECK_EQUAL(cur.Get(7).sequence, 1u);
}

BOOST_AUTO_TEST_CASE(CursorIsMonotonicAndIdempotent) {
    HeadSyncCursor cur;
    cur.Open();

    BOOST_CHECK(cur.AdvanceOnApply(1, 0, 1) == HeadSyncCursorAction::Applied);
    BOOST_CHECK(cur.AdvanceOnApply(1, 0, 2) == HeadSyncCursorAction::Applied);
    // Replay of (0,1) and (0,2) must be no-op.
    BOOST_CHECK(cur.AdvanceOnApply(1, 0, 1) == HeadSyncCursorAction::AlreadySeen);
    BOOST_CHECK(cur.AdvanceOnApply(1, 0, 2) == HeadSyncCursorAction::AlreadySeen);
    // Cursor unchanged.
    BOOST_CHECK_EQUAL(cur.Get(1).sequence, 2u);
}

BOOST_AUTO_TEST_CASE(GapDetectTriggersPullSignal) {
    HeadSyncCursor cur;
    cur.Open();

    BOOST_CHECK(cur.AdvanceOnApply(2, 0, 1) == HeadSyncCursorAction::Applied);
    // Skip sequence 2: entry 3 arrives.
    auto action = cur.AdvanceOnApply(2, 0, 3);
    BOOST_CHECK(action == HeadSyncCursorAction::GapDetected);
    // Cursor must NOT have advanced past 1.
    BOOST_CHECK_EQUAL(cur.Get(2).sequence, 1u);
}

BOOST_AUTO_TEST_CASE(EpochAdvanceWithSeq1IsApplied) {
    HeadSyncCursor cur;
    cur.Open();

    // Walk the cursor up to (0, 5) the legitimate way.
    for (std::uint64_t s = 1; s <= 5; ++s) {
        BOOST_REQUIRE(cur.AdvanceOnApply(3, 0, s) == HeadSyncCursorAction::Applied);
    }
    // New epoch, sequence==1 (first in epoch).
    BOOST_CHECK(cur.AdvanceOnApply(3, 1, 1) == HeadSyncCursorAction::Applied);
    BOOST_CHECK_EQUAL(cur.Get(3).epoch, 1u);
    BOOST_CHECK_EQUAL(cur.Get(3).sequence, 1u);
}

BOOST_AUTO_TEST_CASE(EpochAdvanceWithGapTriggersPull) {
    HeadSyncCursor cur;
    cur.Open();

    BOOST_CHECK(cur.AdvanceOnApply(4, 0, 1) == HeadSyncCursorAction::Applied);
    // New epoch but skipped the head of it.
    auto action = cur.AdvanceOnApply(4, 1, 5);
    BOOST_CHECK(action == HeadSyncCursorAction::EpochAdvanced);
    BOOST_CHECK_EQUAL(cur.Get(4).epoch, 0u);
    BOOST_CHECK_EQUAL(cur.Get(4).sequence, 1u);
}

BOOST_AUTO_TEST_CASE(ForceAtLeastJumpsCursorOnPullCatchup) {
    HeadSyncCursor cur;
    cur.Open();
    BOOST_CHECK(cur.AdvanceOnApply(5, 0, 1) == HeadSyncCursorAction::Applied);
    // Pull returned (epochHi=2, sequenceHi=10) and applied the entries.
    BOOST_CHECK(cur.ForceAtLeast(5, 2, 10));
    BOOST_CHECK_EQUAL(cur.Get(5).epoch, 2u);
    BOOST_CHECK_EQUAL(cur.Get(5).sequence, 10u);
    // Lower call must be no-op.
    BOOST_CHECK(!cur.ForceAtLeast(5, 1, 50));
    BOOST_CHECK(!cur.ForceAtLeast(5, 2, 10));
    BOOST_CHECK_EQUAL(cur.Get(5).sequence, 10u);
}

// ----- Cursor durability --------------------------------------------------

BOOST_AUTO_TEST_CASE(CursorPersistsAcrossReopen) {
    auto path = TmpJournalPath("durable");
    {
        HeadSyncCursor cur;
        BOOST_REQUIRE(cur.Open(path));
        BOOST_CHECK(cur.AdvanceOnApply(11, 0, 1) == HeadSyncCursorAction::Applied);
        BOOST_CHECK(cur.AdvanceOnApply(11, 0, 2) == HeadSyncCursorAction::Applied);
        BOOST_CHECK(cur.AdvanceOnApply(12, 1, 1) == HeadSyncCursorAction::Applied);
    }

    HeadSyncCursor reopened;
    BOOST_REQUIRE(reopened.Open(path));
    BOOST_CHECK_EQUAL(reopened.Get(11).epoch, 0u);
    BOOST_CHECK_EQUAL(reopened.Get(11).sequence, 2u);
    BOOST_CHECK_EQUAL(reopened.Get(12).epoch, 1u);
    BOOST_CHECK_EQUAL(reopened.Get(12).sequence, 1u);

    std::filesystem::remove(path);
}

// ----- Protocol serialization round-trips ---------------------------------

BOOST_AUTO_TEST_CASE(HeadSyncMetaEntryRoundTrip) {
    HeadSyncMetaEntry src = MakeEntry(13, 7, 42, /*vid*/ 0xCAFE);
    std::vector<std::uint8_t> buf(src.EstimateBufferSize());
    src.Write(buf.data());

    HeadSyncMetaEntry dst;
    auto end = dst.Read(buf.data());
    BOOST_REQUIRE(end != nullptr);
    BOOST_CHECK_EQUAL(dst.originOwner, 13);
    BOOST_CHECK_EQUAL(dst.epoch, 7u);
    BOOST_CHECK_EQUAL(dst.sequence, 42u);
    BOOST_CHECK(dst.entry.op == HeadSyncEntry::Op::Add);
    BOOST_CHECK_EQUAL(dst.entry.headVID, 0xCAFE);
    BOOST_CHECK_EQUAL(dst.entry.headVector.size(), 4u);
}

BOOST_AUTO_TEST_CASE(HeadSyncPullRequestRoundTrip) {
    HeadSyncPullRequest src;
    src.m_originOwner   = 4;
    src.m_epochFloor    = 2;
    src.m_sequenceFloor = 17;
    src.m_maxEntries    = 256;

    std::vector<std::uint8_t> buf(src.EstimateBufferSize());
    src.Write(buf.data());

    HeadSyncPullRequest dst;
    BOOST_REQUIRE(dst.Read(buf.data()) != nullptr);
    BOOST_CHECK_EQUAL(dst.m_originOwner, 4);
    BOOST_CHECK_EQUAL(dst.m_epochFloor, 2u);
    BOOST_CHECK_EQUAL(dst.m_sequenceFloor, 17u);
    BOOST_CHECK_EQUAL(dst.m_maxEntries, 256u);
}

BOOST_AUTO_TEST_CASE(HeadSyncPullResponseRoundTripWithEntries) {
    HeadSyncPullResponse src;
    src.m_status     = HeadSyncPullResponse::Status::Ok;
    src.m_originOwner = 9;
    src.m_epochHi    = 3;
    src.m_sequenceHi = 12;
    src.m_entries.push_back(MakeEntry(9, 3, 11, 0x1));
    src.m_entries.push_back(MakeEntry(9, 3, 12, 0x2, HeadSyncEntry::Op::Delete));

    std::vector<std::uint8_t> buf(src.EstimateBufferSize());
    src.Write(buf.data());

    HeadSyncPullResponse dst;
    BOOST_REQUIRE(dst.Read(buf.data()) != nullptr);
    BOOST_CHECK(dst.m_status == HeadSyncPullResponse::Status::Ok);
    BOOST_CHECK_EQUAL(dst.m_originOwner, 9);
    BOOST_CHECK_EQUAL(dst.m_epochHi, 3u);
    BOOST_CHECK_EQUAL(dst.m_sequenceHi, 12u);
    BOOST_REQUIRE_EQUAL(dst.m_entries.size(), 2u);
    BOOST_CHECK_EQUAL(dst.m_entries[0].sequence, 11u);
    BOOST_CHECK(dst.m_entries[1].entry.op == HeadSyncEntry::Op::Delete);
}

BOOST_AUTO_TEST_CASE(HeadSyncPullResponseNotAvailableRoundTrip) {
    HeadSyncPullResponse src;
    src.m_status = HeadSyncPullResponse::Status::NotAvailable;
    src.m_originOwner = 5;

    std::vector<std::uint8_t> buf(src.EstimateBufferSize());
    src.Write(buf.data());

    HeadSyncPullResponse dst;
    BOOST_REQUIRE(dst.Read(buf.data()) != nullptr);
    BOOST_CHECK(dst.m_status == HeadSyncPullResponse::Status::NotAvailable);
    BOOST_CHECK_EQUAL(dst.m_originOwner, 5);
    BOOST_CHECK_EQUAL(dst.m_entries.size(), 0u);
}

// ----- End-to-end: gap detect → pull → catch up --------------------------

namespace {
// In-process pull provider mimic of an owner that retains a sliding window
// of HeadSync entries. Mirrors what the WorkerNode-side provider will do
// once wired into the real outbox.
struct PullProviderStub {
    std::vector<HeadSyncMetaEntry> entries;  // monotonic by (epoch, seq)
    std::uint64_t epochHi = 0, sequenceHi = 0;

    void Push(const HeadSyncMetaEntry& e) {
        entries.push_back(e);
        if (e.epoch > epochHi || (e.epoch == epochHi && e.sequence > sequenceHi)) {
            epochHi = e.epoch;
            sequenceHi = e.sequence;
        }
    }

    void Serve(std::int32_t origin,
               std::uint64_t epochFloor,
               std::uint64_t sequenceFloor,
               std::uint32_t maxEntries,
               HeadSyncPullResponse& out) {
        out.m_originOwner = origin;
        out.m_epochHi = epochHi;
        out.m_sequenceHi = sequenceHi;
        out.m_status = HeadSyncPullResponse::Status::Ok;
        out.m_entries.clear();
        for (const auto& e : entries) {
            if (e.originOwner != origin) continue;
            if (e.epoch < epochFloor) continue;
            if (e.epoch == epochFloor && e.sequence < sequenceFloor) continue;
            out.m_entries.push_back(e);
            if (out.m_entries.size() >= maxEntries) break;
        }
    }
};
}  // namespace

BOOST_AUTO_TEST_CASE(GapDetectThenPullCatchesUp) {
    HeadSyncCursor cur;
    cur.Open();

    PullProviderStub owner;
    for (std::uint64_t s = 1; s <= 5; ++s) {
        owner.Push(MakeEntry(/*origin*/ 1, /*epoch*/ 0, s, /*vid*/ 100 + s));
    }

    // Peer applies seq 1 normally, then misses 2 and 3, then sees 4.
    BOOST_REQUIRE(cur.AdvanceOnApply(1, 0, 1) == HeadSyncCursorAction::Applied);
    BOOST_REQUIRE(cur.AdvanceOnApply(1, 0, 4) == HeadSyncCursorAction::GapDetected);

    // Issue pull from cursor+1.
    auto pos = cur.Get(1);
    HeadSyncPullResponse resp;
    owner.Serve(/*origin*/ 1, pos.epoch, pos.sequence + 1, /*max*/ 1024, resp);
    BOOST_REQUIRE(resp.m_status == HeadSyncPullResponse::Status::Ok);
    BOOST_REQUIRE_GE(resp.m_entries.size(), 2u);

    // Apply each pulled entry through the cursor.
    for (const auto& me : resp.m_entries) {
        auto act = cur.AdvanceOnApply(me.originOwner, me.epoch, me.sequence);
        BOOST_CHECK(act == HeadSyncCursorAction::Applied
                 || act == HeadSyncCursorAction::AlreadySeen);
    }

    // Cursor catches up to at least the pull's high-water mark.
    cur.ForceAtLeast(1, resp.m_epochHi, resp.m_sequenceHi);
    BOOST_CHECK_EQUAL(cur.Get(1).sequence, owner.sequenceHi);
}

// ----- NotAvailable → scan_range fallback --------------------------------

namespace {
// Fallback hook a peer would invoke when the owner has GC'd entries below
// the requested floor. The real implementation will scan the storage range
// for the affected origin/epoch and rebuild the head index from postings;
// here we just record that it was called with the right parameters.
struct ScanRangeFallbackStub {
    std::atomic<std::uint32_t> calls{0};
    std::int32_t lastOrigin = -1;
    std::uint64_t lastEpoch = 0;
    std::uint64_t lastSeq = 0;

    void operator()(std::int32_t origin, std::uint64_t epoch, std::uint64_t seq) {
        calls.fetch_add(1);
        lastOrigin = origin;
        lastEpoch = epoch;
        lastSeq = seq;
    }
};
}  // namespace

BOOST_AUTO_TEST_CASE(NotAvailableTriggersScanRangeFallback) {
    HeadSyncCursor cur;
    cur.Open();
    BOOST_REQUIRE(cur.AdvanceOnApply(2, 0, 1) == HeadSyncCursorAction::Applied);
    BOOST_REQUIRE(cur.AdvanceOnApply(2, 0, 7) == HeadSyncCursorAction::GapDetected);

    // Owner returns NotAvailable (e.g. it GC'd its outbox).
    HeadSyncPullResponse resp;
    resp.m_status = HeadSyncPullResponse::Status::NotAvailable;
    resp.m_originOwner = 2;
    resp.m_epochHi = 0;
    resp.m_sequenceHi = 8;

    ScanRangeFallbackStub fallback;
    if (resp.m_status == HeadSyncPullResponse::Status::NotAvailable) {
        auto pos = cur.Get(2);
        fallback(resp.m_originOwner, pos.epoch, pos.sequence + 1);
    }

    BOOST_CHECK_EQUAL(fallback.calls.load(), 1u);
    BOOST_CHECK_EQUAL(fallback.lastOrigin, 2);
    BOOST_CHECK_EQUAL(fallback.lastEpoch, 0u);
    BOOST_CHECK_EQUAL(fallback.lastSeq, 2u);
}

BOOST_AUTO_TEST_SUITE_END()
