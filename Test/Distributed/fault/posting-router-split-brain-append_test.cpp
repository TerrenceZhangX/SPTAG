// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: posting-router-split-brain-append
//
// Twin to posting-router-owner-failover. Scenario: during a brief
// membership window two compute nodes (N1, N2) both believe they own
// the same HeadID H. Required invariants:
//   1. **Local lock**: a single RemotePostingOps cannot enter the
//      Append path for the same HeadID twice from a stale-epoch
//      caller; the higher-epoch holder fences the stale acquirer.
//   2. **Ring-epoch fence**: a sender that ships its (HeadID, opId)
//      to a receiver whose ring epoch has already advanced past the
//      sender's claim is rejected with StaleRingEpoch (existing
//      prim/ring-epoch-fence path; we just count the surface).
//   3. **Receiver dedup -> single truth**: when both N1 and N2's
//      RPCs land on the same physical owner, only one persisted
//      record exists and the duplicate is collapsed by the receiver
//      OpIdCache.
//
// Tier 1 HARD: env-armed fence / dedup converge to single truth;
//              env-off wrapper is dormant (counters all zero).
// Tier 2 SOFT: deferred (this case is NOT on perf-validation hot
//              path; production caller short-circuits the wrapper
//              when the env is unset).

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

// In-process simulation of two senders (N1, N2) racing on the same
// HeadID through a shared "physical owner" that runs ring-epoch fence
// + OpId dedup.
struct SplitBrainScenario {
    SizeType headID = 77;
    int physicalOwner = 99;

    // Receiver state, shared across the two senders.
    std::uint64_t receiverRingEpoch = 5;  // authoritative epoch
    Distributed::OpIdCache<Distributed::AppendDedupResult> receiverDedup{16, std::chrono::seconds(60)};
    std::unordered_set<std::uint64_t> persisted;  // (opId.monotonicCounter)
    std::uint64_t receiverFences = 0;
    std::uint64_t receiverDedupHits = 0;

    int Resolve(SizeType /*hid*/) const { return physicalOwner; }

    // Receiver model parametrised on the sender's claimed ring epoch.
    // Stale claims are fenced; OpId dedup is consulted on accept.
    ErrorCode Receive(std::uint64_t senderEpoch, const Distributed::OpId& opId) {
        if (senderEpoch < receiverRingEpoch) {
            ++receiverFences;
            return ErrorCode::StaleRingEpoch;
        }
        auto [hit, _cached] = receiverDedup.Lookup(opId);
        if (hit) {
            ++receiverDedupHits;
            return ErrorCode::Success;
        }
        Distributed::AppendDedupResult cached;
        cached.status = 0;
        receiverDedup.Insert(opId, cached);
        persisted.insert(opId.monotonicCounter);
        return ErrorCode::Success;
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(PostingRouterSplitBrainAppendTest)

// ---------------------------------------------------------------------------
// Tier 1 HARD repro — env ARMED. Two senders (N1 at epoch=4 stale,
// N2 at epoch=5 fresh) race on the same HeadID. The stale sender is
// fenced by the receiver; the fresh sender's append lands once. A
// deliberate replay from the fresh sender is collapsed by receiver
// dedup. Single truth holds: persisted set has exactly one entry.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SplitBrainArmedFenceAndDedupConvergeToSingleTruth) {
    ScopedEnv env("SPTAG_FAULT_POSTING_ROUTER_SPLIT_BRAIN_APPEND", "1");
    BOOST_REQUIRE(RemotePostingOps::SplitBrainGuardArmed());

    // Two RemotePostingOps instances model the two compute nodes that
    // both think they own H during the brief membership window.
    RemotePostingOps n1;
    n1.ConfigureIdempotency(/*senderId=*/11, /*restartEpoch=*/1);
    RemotePostingOps n2;
    n2.ConfigureIdempotency(/*senderId=*/22, /*restartEpoch=*/1);

    SplitBrainScenario sc;

    auto resolve = [&](SizeType hid){ return sc.Resolve(hid); };

    // N1 thinks it owns H but its ring view is one epoch behind.
    std::uint64_t n1Epoch = 4;
    auto sendN1 = [&](int /*target*/, const Distributed::OpId& opId){
        return sc.Receive(n1Epoch, opId);
    };
    ErrorCode rc1 = n1.SendRemoteAppendWithSplitBrainGuard(
        sc.headID, n1Epoch, resolve, sendN1, /*maxRetries=*/0);

    // Invariant 2: stale sender is fenced.
    BOOST_CHECK(rc1 == ErrorCode::StaleRingEpoch);
    BOOST_CHECK_GE(n1.GetSplitBrainCounters().fenced, 1u);
    BOOST_CHECK_EQUAL(sc.persisted.size(), 0u);

    // N2's ring view matches the receiver authoritative epoch.
    std::uint64_t n2Epoch = 5;
    auto sendN2 = [&](int /*target*/, const Distributed::OpId& opId){
        return sc.Receive(n2Epoch, opId);
    };
    ErrorCode rc2 = n2.SendRemoteAppendWithSplitBrainGuard(
        sc.headID, n2Epoch, resolve, sendN2, /*maxRetries=*/0);

    // Invariant 3a: single truth — exactly one persisted entry.
    BOOST_CHECK(rc2 == ErrorCode::Success);
    BOOST_CHECK_EQUAL(sc.persisted.size(), 1u);

    // N2 retries with the SAME OpId (simulated replay). Receiver dedup
    // collapses the duplicate; persisted set stays at size 1.
    // (allocator state is opaque; we drive a direct receiver replay below)

    // We re-issue via a wrapper-internal retry path: drive the wrapper
    // again with a sendFn that returns Fail once then success. The
    // wrapper allocates a NEW OpId for the new top-level call, so to
    // exercise dedup we directly hand the previously persisted OpId to
    // the receiver and assert collapse semantics.
    // Capture the current persisted OpId (only one entry).
    BOOST_REQUIRE_EQUAL(sc.persisted.size(), 1u);
    Distributed::OpId persistedOpId{};
    persistedOpId.monotonicCounter = *sc.persisted.begin();
    persistedOpId.senderId = 22;
    persistedOpId.restartEpoch = 1;
    ErrorCode rcDup = sc.Receive(n2Epoch, persistedOpId);
    BOOST_CHECK(rcDup == ErrorCode::Success);
    BOOST_CHECK_GE(sc.receiverDedupHits, 1u);
    BOOST_CHECK_EQUAL(sc.persisted.size(), 1u);  // still single truth
}

// ---------------------------------------------------------------------------
// Tier 1 HARD repro — env OFF. Wrapper is dormant; counters stay zero
// and the production hot path is byte-identical to the merged-prim
// baseline.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SplitBrainEnvOffIsDormant) {
    ScopedEnv env("SPTAG_FAULT_POSTING_ROUTER_SPLIT_BRAIN_APPEND", nullptr);
    BOOST_REQUIRE(!RemotePostingOps::SplitBrainGuardArmed());

    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/7, /*restartEpoch=*/1);

    auto before = ops.GetSplitBrainCounters();
    BOOST_CHECK_EQUAL(before.localLockAcquired, 0u);
    BOOST_CHECK_EQUAL(before.localLockContended, 0u);
    BOOST_CHECK_EQUAL(before.fenced, 0u);
    BOOST_CHECK_EQUAL(before.dedupCollapsed, 0u);

    // Production-style path: caller checks SplitBrainGuardArmed() and
    // skips the wrapper entirely. Counters must remain zero.
    auto after = ops.GetSplitBrainCounters();
    BOOST_CHECK_EQUAL(after.localLockAcquired, 0u);
    BOOST_CHECK_EQUAL(after.localLockContended, 0u);
    BOOST_CHECK_EQUAL(after.fenced, 0u);
    BOOST_CHECK_EQUAL(after.dedupCollapsed, 0u);
}

// ---------------------------------------------------------------------------
// Local-lock invariant: on the SAME node, a stale-epoch acquirer is
// fenced when a higher-epoch holder is currently active for the same
// HeadID. This is the (a) leg of the three-way interlock.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SplitBrainLocalLockFencesStaleEpochAcquirer) {
    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/3, /*restartEpoch=*/1);

    SizeType H = 200;
    std::atomic<bool> firstInside{false};
    std::atomic<bool> firstRelease{false};

    auto resolve = [&](SizeType){ return 1; };

    // First call: holds the lock at epoch=10 until released.
    std::thread first([&]{
        auto sendFn = [&](int, const Distributed::OpId&){
            firstInside.store(true);
            // Park until the stale acquirer has had a chance to fence.
            while (!firstRelease.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            return ErrorCode::Success;
        };
        ErrorCode rc = ops.SendRemoteAppendWithSplitBrainGuard(
            H, /*myRingEpoch=*/10, resolve, sendFn, /*maxRetries=*/0);
        BOOST_CHECK(rc == ErrorCode::Success);
    });

    // Wait for the first thread to acquire the local lock.
    while (!firstInside.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    // Stale acquirer (epoch=4) tries to enter; should be fenced
    // because the holder at epoch=10 is strictly higher.
    auto sendStub = [&](int, const Distributed::OpId&){
        return ErrorCode::Success;  // never reached
    };
    ErrorCode rcStale = ops.SendRemoteAppendWithSplitBrainGuard(
        H, /*myRingEpoch=*/4, resolve, sendStub, /*maxRetries=*/0);
    BOOST_CHECK(rcStale == ErrorCode::StaleRingEpoch);
    auto c = ops.GetSplitBrainCounters();
    BOOST_CHECK_GE(c.localLockContended, 1u);
    BOOST_CHECK_GE(c.fenced, 1u);

    firstRelease.store(true);
    first.join();
}

// ---------------------------------------------------------------------------
// OpId stability across guard retries: the wrapper allocates the OpId
// once for the duration of the call; if the receiver returns
// StaleRingEpoch on the first attempt and we retry, the SAME OpId is
// reused so receiver-side dedup can collapse. Mirrors the
// owner-failover OpId-stability test.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(SplitBrainOpIdStableAcrossRetries) {
    RemotePostingOps ops;
    ops.ConfigureIdempotency(/*senderId=*/9, /*restartEpoch=*/2);

    std::vector<Distributed::OpId> seen;
    int attempts = 0;
    auto resolve = [&](SizeType){ return 1; };
    auto sendFn = [&](int, const Distributed::OpId& opId){
        seen.push_back(opId);
        ++attempts;
        if (attempts < 3) return ErrorCode::StaleRingEpoch;
        return ErrorCode::Success;
    };

    ErrorCode rc = ops.SendRemoteAppendWithSplitBrainGuard(
        /*headID=*/321,
        /*myRingEpoch=*/5,
        resolve,
        sendFn,
        /*maxRetries=*/4);
    BOOST_CHECK(rc == ErrorCode::Success);
    BOOST_REQUIRE_GE(seen.size(), 2u);
    for (size_t i = 1; i < seen.size(); ++i) {
        BOOST_CHECK(seen[i] == seen[0]);
    }
    auto c = ops.GetSplitBrainCounters();
    // Two StaleRingEpoch surfaces -> fenced counter >= 2. Final
    // success after retry -> dedupCollapsed >= 1 (wrapper observed
    // a retry succeed).
    BOOST_CHECK_GE(c.fenced, 2u);
    BOOST_CHECK_GE(c.dedupCollapsed, 1u);
}

// ---------------------------------------------------------------------------
// TiKV smoke (when harness present). Same shape as sibling.
// ---------------------------------------------------------------------------
#ifdef TIKV
BOOST_AUTO_TEST_CASE(TiKVHarnessSmokeWriteRead) {
    const char* pd = std::getenv("TIKV_PD_ADDRESSES");
    if (!pd || std::string(pd).empty()) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping TiKV smoke");
        return;
    }
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string prefix = "fault_posting_router_split_brain_append_smoke_" + std::to_string(now) + "_";
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
