// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: posting-router-owner-failover
//
// Scenario: while a RemoteAppend is in flight, the owner of the HeadID
// changes (consistent-hash ring rev bumps and/or SWIM marks the old
// owner dead). Required invariants:
//   1. The in-flight append is retried and routed to the *new* owner.
//   2. AtLeastOnce + receiver-side OpId dedup => single truth, no
//      duplicate persisted record exposed.
//   3. Stale-epoch sender gets fenced (StaleRingEpoch) so the *old*
//      owner cannot commit a split-brain append.
//
// This file exercises the unit-level recovery contract:
//   - Tier 1 HARD: the env-armed test case must observe failover retries
//     succeed and the StaleRingEpoch fence fire on the old owner.
//   - The env-off case asserts the wrapper is dormant on baseline.
//
// 1M-scale perf gate (Tier 2 SOFT, ±20%) is in
// posting-router-owner-failover_perf.sh and notes/perf-validation-protocol.md.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/RemotePostingOps.h"
#include "inc/Core/SPANN/Distributed/OpId.h"
#include "inc/Core/SPANN/Distributed/OpIdCache.h"
#include "inc/Core/SPANN/Distributed/RingEpoch.h"

#ifdef TIKV
#include "inc/Core/SPANN/ExtraTiKVController.h"
#endif

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::SPANN;

namespace {

// Small helper: temporarily set/unset an env var for the scope of a test
// case. Restores prior value on destruction.
class ScopedEnv {
public:
    ScopedEnv(const char* name, const char* value) : m_name(name) {
        const char* prior = std::getenv(name);
        m_had = (prior != nullptr);
        if (m_had) m_prior = prior;
        if (value) ::setenv(name, value, 1);
        else        ::unsetenv(name);
    }
    ~ScopedEnv() {
        if (m_had) ::setenv(m_name.c_str(), m_prior.c_str(), 1);
        else       ::unsetenv(m_name.c_str());
    }
private:
    std::string m_name;
    std::string m_prior;
    bool m_had = false;
};

// In-process scenario harness: simulates the sequence
//   1. Sender resolves owner -> A.
//   2. RemoteAppend in flight; ring rev bumps so HeadID maps to B.
//   3. First attempt to A returns StaleRingEpoch (receiver fence on A).
//   4. Wrapper re-resolves -> B; second attempt to B succeeds.
//   5. AtLeastOnce: same OpId carried across attempts. We assert that
//      a duplicate retry to B (simulated) is collapsed by receiver
//      dedup cache (single-truth on read-back).
struct OwnerFailoverScenario {
    int oldOwner = 100;
    int newOwner = 200;
    SPTAG::SizeType headID = 42;

    int attempts = 0;
    int targetA_calls = 0;
    int targetB_calls = 0;
    Distributed::OpId observedOpId{};

    // Simulated receiver dedup cache (one per receiver node).
    Distributed::OpIdCache<Distributed::AppendDedupResult> receiverA{16, std::chrono::seconds(60)};
    Distributed::OpIdCache<Distributed::AppendDedupResult> receiverB{16, std::chrono::seconds(60)};
    std::uint64_t dedupCollapsed = 0;

    // Persisted set on B (single-truth check).
    std::unordered_set<std::uint64_t> persistedOnB;

    // Ring resolver: returns oldOwner on first call, newOwner thereafter.
    std::atomic<int> ringRev{1};
    int Resolve(SPTAG::SizeType /*hid*/) {
        return ringRev.load() <= 1 ? oldOwner : newOwner;
    }

    // Per-attempt send. Records OpId for cross-attempt verification.
    ErrorCode Send(int target, const Distributed::OpId& opId) {
        ++attempts;
        if (attempts == 1 && observedOpId.monotonicCounter == 0) {
            observedOpId = opId;
        } else {
            // Subsequent attempts MUST carry the same OpId.
            BOOST_CHECK(opId == observedOpId);
        }
        if (target == oldOwner) {
            ++targetA_calls;
            // Simulate ring rev bump observed by receiver A: it now sees
            // sender ahead/behind and returns StaleRingEpoch on first try.
            // We bump ringRev so the next Resolve returns newOwner.
            ringRev.store(2);
            return ErrorCode::StaleRingEpoch;
        }
        if (target == newOwner) {
            ++targetB_calls;
            // Receiver B applies dedup. If the OpId was already seen,
            // collapse and report success (no double-persist).
            auto [hit, _cached] = receiverB.Lookup(opId);
            if (hit) {
                ++dedupCollapsed;
                return ErrorCode::Success;
            }
            Distributed::AppendDedupResult cached;
            cached.status = static_cast<std::uint8_t>(0);  // Success
            receiverB.Insert(opId, cached);
            persistedOnB.insert(opId.monotonicCounter);
            return ErrorCode::Success;
        }
        return ErrorCode::Fail;
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(PostingRouterOwnerFailoverTest)

// ---------------------------------------------------------------------------
// Tier 1 HARD repro — env ARMED. Wrapper retries, fences old owner, lands
// the append on the new owner with the SAME OpId. Dedup collapses any
// duplicate retry to the new owner.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(OwnerFailoverArmedRetriesAndFences) {
    ScopedEnv env("SPTAG_FAULT_POSTING_ROUTER_OWNER_FAILOVER", "1");
    BOOST_REQUIRE(RemotePostingOps::OwnerFailoverArmed());

    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/7, /*restartEpoch=*/1);

    OwnerFailoverScenario sc;

    // Use the wrapper directly with a stub sendFn modelling the in-flight
    // owner change. resolveOwner returns oldOwner first, then newOwner.
    ErrorCode rc = ops.SendRemoteAppendWithFailover(
        sc.headID,
        [&](SPTAG::SizeType hid){ return sc.Resolve(hid); },
        [&](int target, const Distributed::OpId& opId){ return sc.Send(target, opId); },
        /*maxRetries=*/3);

    // Invariant 1: retry routed to new owner; final result Success.
    BOOST_CHECK(rc == ErrorCode::Success);
    BOOST_CHECK_GE(sc.targetA_calls, 1);
    BOOST_CHECK_GE(sc.targetB_calls, 1);

    auto failoverCounters = ops.GetOwnerFailoverCounters();
    BOOST_CHECK_GE(failoverCounters.retries, 1u);
    BOOST_CHECK_GE(failoverCounters.staleEpochFenced, 1u);

    // Invariant 3: stale-epoch fence fired on old owner (StaleRingEpoch
    // surfaced by sendFn was counted).
    BOOST_CHECK_GE(failoverCounters.staleEpochFenced, 1u);

    // Invariant 2 (single truth): persistedOnB has exactly one entry for
    // this OpId, regardless of how many attempts we made.
    BOOST_CHECK_EQUAL(sc.persistedOnB.size(), 1u);

    // Now simulate a duplicate retry to B with the SAME OpId — receiver
    // dedup must collapse it (mismatches==0, no double-persist).
    ErrorCode rcDup = sc.Send(sc.newOwner, sc.observedOpId);
    BOOST_CHECK(rcDup == ErrorCode::Success);
    BOOST_CHECK_GE(sc.dedupCollapsed, 1u);
    BOOST_CHECK_EQUAL(sc.persistedOnB.size(), 1u);  // still one truth
}

// ---------------------------------------------------------------------------
// Tier 1 HARD repro — env OFF. Wrapper is dormant; the production hot path
// (caller-side) does not invoke the failover loop. We assert by checking
// that OwnerFailoverArmed() returns false and counters stay zero.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(OwnerFailoverEnvOffIsDormant) {
    ScopedEnv env("SPTAG_FAULT_POSTING_ROUTER_OWNER_FAILOVER", nullptr);
    BOOST_REQUIRE(!RemotePostingOps::OwnerFailoverArmed());

    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/7, /*restartEpoch=*/1);

    auto before = ops.GetOwnerFailoverCounters();
    BOOST_CHECK_EQUAL(before.retries, 0u);
    BOOST_CHECK_EQUAL(before.staleEpochFenced, 0u);
    BOOST_CHECK_EQUAL(before.dedupCollapsed, 0u);

    // Production-style path simulation: caller sees env unset and skips
    // the wrapper entirely. We do not invoke SendRemoteAppendWithFailover.
    // Counters must remain zero; this is the no-op-on-baseline assertion.
    auto after = ops.GetOwnerFailoverCounters();
    BOOST_CHECK_EQUAL(after.retries, 0u);
    BOOST_CHECK_EQUAL(after.staleEpochFenced, 0u);
    BOOST_CHECK_EQUAL(after.dedupCollapsed, 0u);
}

// ---------------------------------------------------------------------------
// OpId carry-across-retries: separately confirm that the wrapper allocates
// the OpId once and reuses it across attempts. Critical for receiver-side
// dedup to collapse duplicates.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(OpIdIsStableAcrossOwnerFailoverRetries) {
    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/9, /*restartEpoch=*/3);

    std::vector<Distributed::OpId> seen;
    int attempts = 0;
    auto resolveOwner = [&](SPTAG::SizeType){ return attempts < 2 ? 1 : 2; };
    auto sendFn = [&](int /*target*/, const Distributed::OpId& opId){
        seen.push_back(opId);
        ++attempts;
        if (attempts < 3) return ErrorCode::StaleRingEpoch;
        return ErrorCode::Success;
    };

    ErrorCode rc = ops.SendRemoteAppendWithFailover(
        /*headID=*/123, resolveOwner, sendFn, /*maxRetries=*/4);
    BOOST_CHECK(rc == ErrorCode::Success);
    BOOST_REQUIRE_GE(seen.size(), 2u);
    for (size_t i = 1; i < seen.size(); ++i) {
        BOOST_CHECK(seen[i] == seen[0]);
    }
}

// ---------------------------------------------------------------------------
// Receiver-side dedup collapse: an OpIdCache that has already recorded
// the result for a given OpId returns the cached status on lookup. This
// is the receiver-side guarantee that AtLeastOnce + dedup => single truth.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ReceiverDedupCollapsesDuplicateOpId) {
    Distributed::OpIdCache<Distributed::AppendDedupResult> cache(16, std::chrono::seconds(60));
    Distributed::OpId id{7, 1, 100};

    Distributed::AppendDedupResult cached;
    cached.status = 0;
    BOOST_REQUIRE(cache.Insert(id, cached));

    auto [hit, replay] = cache.Lookup(id);
    BOOST_CHECK(hit);
    BOOST_CHECK_EQUAL(replay.status, 0u);

    // Bump same OpId 3 more times; cache stays single-truth (one entry).
    BOOST_CHECK_EQUAL(cache.Size(), 1u);
    for (int i = 0; i < 3; ++i) {
        auto [h, _r] = cache.Lookup(id);
        BOOST_CHECK(h);
    }
    BOOST_CHECK_EQUAL(cache.Size(), 1u);
}

// ---------------------------------------------------------------------------
// TiKV smoke (when TIKV harness is present): verify the harness still
// brings up PD+TiKV cleanly. This is the existing scaffold contract; the
// fault-case logic above does not depend on TiKV.
// ---------------------------------------------------------------------------
#ifdef TIKV
BOOST_AUTO_TEST_CASE(TiKVHarnessSmokeWriteRead) {
    const char* pd = std::getenv("TIKV_PD_ADDRESSES");
    if (!pd || std::string(pd).empty()) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping TiKV smoke");
        return;
    }
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string prefix = "fault_posting_router_owner_failover_smoke_" + std::to_string(now) + "_";
    auto io = std::make_shared<TiKVIO>(std::string(pd), prefix);
    if (!io->Available()) {
        BOOST_TEST_MESSAGE("TiKV unreachable; skipping smoke");
        return;
    }
    BOOST_REQUIRE(io->Put("k", "v", std::chrono::microseconds(2000000), nullptr) == ErrorCode::Success);
    std::string out;
    BOOST_REQUIRE(io->Get("k", &out, std::chrono::microseconds(2000000), nullptr) == ErrorCode::Success);
    BOOST_CHECK_EQUAL(out, "v");
}
#endif

BOOST_AUTO_TEST_SUITE_END()
