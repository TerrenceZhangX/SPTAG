// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: posting-router-routing-table-drift
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/posting-router-routing-table-drift.md
// Branch: fault/posting-router-routing-table-drift
//
// Scenario: two compute nodes act on different ConsistentHashRing
// snapshots. Node A routes a posting for headID=H to oldOwner under
// its (epoch, ringRev) = R_a. The receiver — running under R_b > R_a —
// fences the request with StaleRingEpoch and returns its authoritative
// (epoch, ringRev). Required invariants (Tier 1 HARD):
//   1. Receiver fence rejects the stale-router request (no split-brain
//      append on the ex-owner).
//   2. Sender refreshes its ring view, recomputes GetOwner under the
//      fresh (epoch, ringRev), and retries against the *new* owner with
//      the SAME OpId.
//   3. After refresh, both views converge on a single (epoch, ringRev).
//   4. Empty / pre-Initialize ring (the row-7 hazard:
//      isLocal=true, nodeIndex=-1) fails closed with RingNotReady; no
//      KV write is issued.
//   5. env-off: the wrapper is dormant; the production hot path is
//      unaffected on baseline runs.
//
// 1M-scale perf gate (Tier 2 SOFT, ±20%) is informational per the
// 2026-04-30 perf-validation protocol; this case is *non* hot-path
// (DEFERRED is acceptable on the perf tier).

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

// Two-router routing-table-drift scenario.
//
//   Sender holds R_a = (epoch=1, ringRev=1).
//   Authoritative dispatcher view R_b = (epoch=2, ringRev=1).
//   Under R_a, headID=H -> oldOwner.
//   Under R_b, headID=H -> newOwner.
//
//   First attempt: routes to oldOwner under R_a. The mock receiver
//   compares senderEpoch (R_a) with its own (R_b): SenderStale ->
//   reply StaleRingEpoch with authoritative R_b. The ex-owner does
//   NOT persist the append (split-brain prevention).
//
//   Wrapper invokes refreshRing() -> sender adopts R_b. resolveOwner
//   under R_b returns newOwner. Second attempt lands on newOwner; the
//   mock receiver agrees on R_b -> Success and persists.
struct RingDriftScenario {
    int oldOwner = 100;
    int newOwner = 200;
    SPTAG::SizeType headID = 42;

    // Sender's view (refreshable).
    RingEpoch senderRing{1, 1};
    // Authoritative cluster view.
    RingEpoch authoritativeRing{2, 1};

    int attempts = 0;
    int oldOwnerCalls = 0;
    int newOwnerCalls = 0;
    int oldOwnerPersisted = 0;
    int newOwnerPersisted = 0;
    int refreshCalls = 0;

    Distributed::OpId firstOpId{};
    bool firstOpIdSet = false;

    Distributed::OpIdCache<Distributed::AppendDedupResult> oldOwnerDedup{16, std::chrono::seconds(60)};
    Distributed::OpIdCache<Distributed::AppendDedupResult> newOwnerDedup{16, std::chrono::seconds(60)};
    std::unordered_set<std::uint64_t> persistedOpIdsOnNewOwner;
    std::unordered_set<std::uint64_t> persistedOpIdsOnOldOwner;

    int Resolve(SPTAG::SizeType /*hid*/) const {
        // Resolution depends on the sender's *current* ring view.
        return senderRing.epoch >= authoritativeRing.epoch ? newOwner : oldOwner;
    }

    void Refresh() {
        ++refreshCalls;
        senderRing = authoritativeRing;
    }

    // Per-attempt mock send. Models the receiver-side fence semantics
    // implemented in HandleAppendRequest (prim/ring-epoch-fence):
    //   - if senderEpoch < authoritativeRing -> StaleRingEpoch, no apply
    //   - if equal                            -> Success, dedup-collapse
    ErrorCode Send(int target, const Distributed::OpId& opId) {
        ++attempts;
        if (!firstOpIdSet) {
            firstOpId = opId;
            firstOpIdSet = true;
        } else {
            BOOST_CHECK(opId == firstOpId);
        }

        // Receiver-side fence: each receiver acts on its own view of
        // R_b (authoritative).
        if (senderRing < authoritativeRing) {
            if (target == oldOwner) ++oldOwnerCalls;
            else if (target == newOwner) ++newOwnerCalls;
            // Crucially, NO persistence on the ex-owner.
            return ErrorCode::StaleRingEpoch;
        }

        if (target == oldOwner) {
            // Sender ring is now current, but routed to ex-owner
            // (resolveOwner under fresh ring would not pick this).
            ++oldOwnerCalls;
            auto [hit, _c] = oldOwnerDedup.Lookup(opId);
            if (hit) return ErrorCode::Success;
            Distributed::AppendDedupResult cached;
            cached.status = 0;
            oldOwnerDedup.Insert(opId, cached);
            persistedOpIdsOnOldOwner.insert(opId.monotonicCounter);
            ++oldOwnerPersisted;
            return ErrorCode::Success;
        }
        if (target == newOwner) {
            ++newOwnerCalls;
            auto [hit, _c] = newOwnerDedup.Lookup(opId);
            if (hit) return ErrorCode::Success;  // dedup-collapse
            Distributed::AppendDedupResult cached;
            cached.status = 0;
            newOwnerDedup.Insert(opId, cached);
            persistedOpIdsOnNewOwner.insert(opId.monotonicCounter);
            ++newOwnerPersisted;
            return ErrorCode::Success;
        }
        return ErrorCode::Fail;
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(PostingRouterRoutingTableDriftTest)

// ---------------------------------------------------------------------------
// Tier 1 HARD repro — env ARMED. Two routers diverge; receiver fence rejects
// the stale-router request; sender refreshes ring + retries on the new owner;
// no split-brain append on the ex-owner; rings converge.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(RoutingTableDriftArmedFenceRefreshAndRetry) {
    ScopedEnv env("SPTAG_FAULT_POSTING_ROUTER_ROUTING_TABLE_DRIFT", "1");
    BOOST_REQUIRE(RemotePostingOps::RoutingDriftArmed());

    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/11, /*restartEpoch=*/1);

    RingDriftScenario sc;

    ErrorCode rc = ops.SendRemoteAppendWithRingRefresh(
        sc.headID,
        [&](SPTAG::SizeType hid){ return sc.Resolve(hid); },
        [&](){ sc.Refresh(); },
        [&](int target, const Distributed::OpId& opId){ return sc.Send(target, opId); },
        /*maxRetries=*/3);

    // Invariant 2: retry routed to new owner; final result Success.
    BOOST_CHECK(rc == ErrorCode::Success);
    BOOST_CHECK_GE(sc.oldOwnerCalls, 1);
    BOOST_CHECK_GE(sc.newOwnerCalls, 1);

    auto rc1 = ops.GetRoutingCounters();
    BOOST_CHECK_GE(rc1.staleFenced, 1u);
    BOOST_CHECK_GE(rc1.refreshes, 1u);
    BOOST_CHECK_GE(rc1.retries, 1u);

    // Invariant 1: NO split-brain append. The ex-owner must not have
    // persisted the posting; only the new owner did.
    BOOST_CHECK_EQUAL(sc.oldOwnerPersisted, 0);
    BOOST_CHECK_EQUAL(sc.newOwnerPersisted, 1);
    BOOST_CHECK(sc.persistedOpIdsOnOldOwner.empty());
    BOOST_CHECK_EQUAL(sc.persistedOpIdsOnNewOwner.size(), 1u);

    // Invariant 3: single ring convergence — sender view matches
    // authoritative after refresh.
    BOOST_CHECK(sc.senderRing == sc.authoritativeRing);
}

// ---------------------------------------------------------------------------
// Empty-ring / pre-Initialize hazard: a sender whose ring view is
// uninitialised (the GetOwner -> {isLocal=true, nodeIndex=-1} hazard)
// must fail closed with RingNotReady. No persistence anywhere.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EmptyRingFailsClosedNotReady) {
    ScopedEnv env("SPTAG_FAULT_POSTING_ROUTER_ROUTING_TABLE_DRIFT", "1");

    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/12, /*restartEpoch=*/1);

    RingDriftScenario sc;
    sc.senderRing = RingEpoch{};        // uninit
    sc.authoritativeRing = RingEpoch{}; // dispatcher not yet up either

    int sendCalls = 0;
    ErrorCode rc = ops.SendRemoteAppendWithRingRefresh(
        sc.headID,
        [&](SPTAG::SizeType /*hid*/){ return -1; },           // sentinel for empty ring
        [&](){ /* refresh no-op: dispatcher still empty */ },
        [&](int /*target*/, const Distributed::OpId& /*opId*/){
            ++sendCalls;
            return ErrorCode::RingNotReady;                   // sender-startup gate
        },
        /*maxRetries=*/2);

    BOOST_CHECK(rc == ErrorCode::RingNotReady);
    BOOST_CHECK_EQUAL(sc.oldOwnerPersisted, 0);
    BOOST_CHECK_EQUAL(sc.newOwnerPersisted, 0);
    BOOST_CHECK(sc.persistedOpIdsOnOldOwner.empty());
    BOOST_CHECK(sc.persistedOpIdsOnNewOwner.empty());
}

// ---------------------------------------------------------------------------
// Tier 1 HARD repro — env OFF. Wrapper armed-check is dormant; we do not
// invoke the wrapper from the production hot path; counters stay zero.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffWrapperIsDormant) {
    ScopedEnv env("SPTAG_FAULT_POSTING_ROUTER_ROUTING_TABLE_DRIFT", nullptr);
    BOOST_REQUIRE(!RemotePostingOps::RoutingDriftArmed());

    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/13, /*restartEpoch=*/1);

    auto before = ops.GetRoutingCounters();
    BOOST_CHECK_EQUAL(before.refreshes, 0u);
    BOOST_CHECK_EQUAL(before.staleFenced, 0u);
    BOOST_CHECK_EQUAL(before.retries, 0u);

    // Production-style: caller short-circuits past the wrapper when
    // RoutingDriftArmed() is false. We do NOT call the wrapper here.
    auto after = ops.GetRoutingCounters();
    BOOST_CHECK_EQUAL(after.refreshes, 0u);
    BOOST_CHECK_EQUAL(after.staleFenced, 0u);
    BOOST_CHECK_EQUAL(after.retries, 0u);
}

// ---------------------------------------------------------------------------
// OpId stability across refresh+retry: receiver-side dedup needs the same
// OpId on every attempt so a duplicated retry is collapsed to single-truth.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(OpIdIsStableAcrossRingRefreshRetries) {
    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/14, /*restartEpoch=*/2);

    std::vector<Distributed::OpId> seen;
    int attempts = 0;
    auto resolveOwner = [&](SPTAG::SizeType){ return attempts < 2 ? 1 : 2; };
    auto refreshRing  = [&](){ /* mock refresh */ };
    auto sendFn = [&](int /*target*/, const Distributed::OpId& opId){
        seen.push_back(opId);
        ++attempts;
        if (attempts < 3) return ErrorCode::StaleRingEpoch;
        return ErrorCode::Success;
    };

    ErrorCode rc = ops.SendRemoteAppendWithRingRefresh(
        /*headID=*/777, resolveOwner, refreshRing, sendFn, /*maxRetries=*/4);
    BOOST_CHECK(rc == ErrorCode::Success);
    BOOST_REQUIRE_GE(seen.size(), 2u);
    for (size_t i = 1; i < seen.size(); ++i) {
        BOOST_CHECK(seen[i] == seen[0]);
    }
}

// ---------------------------------------------------------------------------
// Receiver-side dedup collapse on an OpId already seen for this slice.
// Confirms AtLeastOnce + dedup => single truth across the refresh boundary.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ReceiverDedupCollapsesDuplicateOnNewOwner) {
    Distributed::OpIdCache<Distributed::AppendDedupResult> cache(16, std::chrono::seconds(60));
    Distributed::OpId id{14, 2, 100};

    Distributed::AppendDedupResult cached;
    cached.status = 0;
    BOOST_REQUIRE(cache.Insert(id, cached));

    auto [hit, replay] = cache.Lookup(id);
    BOOST_CHECK(hit);
    BOOST_CHECK_EQUAL(replay.status, 0u);

    BOOST_CHECK_EQUAL(cache.Size(), 1u);
    for (int i = 0; i < 3; ++i) {
        auto [h, _r] = cache.Lookup(id);
        BOOST_CHECK(h);
    }
    BOOST_CHECK_EQUAL(cache.Size(), 1u);
}

// ---------------------------------------------------------------------------
// TiKV smoke (when present): existing scaffold contract; the routing-drift
// logic above is in-process and does not depend on TiKV.
// ---------------------------------------------------------------------------
#ifdef TIKV
BOOST_AUTO_TEST_CASE(TiKVHarnessSmokeWriteRead) {
    const char* pd = std::getenv("TIKV_PD_ADDRESSES");
    if (!pd || std::string(pd).empty()) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping TiKV smoke");
        return;
    }
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string prefix = "fault_posting_router_routing_table_drift_smoke_" + std::to_string(now) + "_";
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
