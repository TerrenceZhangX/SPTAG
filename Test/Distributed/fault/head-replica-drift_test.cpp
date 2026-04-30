// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: head-replica-drift
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/head-replica-drift.md
// Branch: fault/head-replica-drift
//
// Scenario: per-node head-index full replicas diverge after long-running
// inserts. Required invariants (Tier 1 HARD):
//
//   1. env-off dormancy: the audit/reconcile wrapper is silent on the
//      production hot path; counters do not move and applyDelta is never
//      invoked, even when drift is present. (no silent recall regression
//      because the hot path is byte-equal to baseline.)
//   2. baseline sync: env-armed audit over equal sets reports InSync; no
//      reconcile is triggered; entry counts are byte-equal pre/post.
//   3. drift detected: env-armed audit over divergent sets reports
//      Repaired with m_headDriftDetected ≥ 1 and
//      m_headDriftEntryDelta == |symmetric difference|.
//   4. reconcile to zero: a follow-up audit after Repaired returns
//      InSync; the local set converges byte-equal to authoritative
//      (entry-count convergence invariant).
//   5. harness smoke: a multi-pass loop with bidirectional drift
//      (adds + deletes) keeps counters monotonic, never spuriously fires
//      on a no-drift pass, and ends in convergence.
//
// 1M-scale perf gate (Tier 2 SOFT, ±20%) is DEFERRED; this case is
// non hot-path (audit is a periodic background task, not a query- or
// insert-path mutator).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/RemotePostingOps.h"
#include "inc/Core/SPANN/Distributed/DistributedProtocol.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <string>
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

// In-memory peer head-index replica. Mirrors the design-doc model: a
// per-peer VID set that the audit wrapper compares against authoritative.
struct PeerHeadSet {
    std::unordered_set<SizeType> vids;

    void Add(SizeType v)    { vids.insert(v); }
    void Delete(SizeType v) { vids.erase(v); }

    std::vector<SizeType> Snapshot() const {
        std::vector<SizeType> out(vids.begin(), vids.end());
        std::sort(out.begin(), out.end());
        return out;
    }

    void ApplyDelta(const std::vector<SizeType>& adds,
                    const std::vector<SizeType>& deletes) {
        for (auto v : adds)    vids.insert(v);
        for (auto v : deletes) vids.erase(v);
    }
};

// Fluent helper: wires the four callbacks the wrapper needs against an
// in-memory `local` and `authoritative` set.
struct AuditHarness {
    PeerHeadSet local;
    PeerHeadSet authoritative;

    int localScans = 0;
    int ownerDigests = 0;
    int ownerScans = 0;
    int applyCalls = 0;
    bool digestFailNext = false;
    bool ownerScanFailNext = false;

    RemotePostingOps::HeadDriftAuditResult Run(RemotePostingOps& ops) {
        return ops.AuditAndReconcileHeadDrift(
            // localScan
            [this]() {
                ++localScans;
                return local.Snapshot();
            },
            // ownerDigest
            [this](std::uint64_t& outDigest) {
                ++ownerDigests;
                if (digestFailNext) { digestFailNext = false; return false; }
                outDigest = RemotePostingOps::HeadSetDigest(authoritative.Snapshot());
                return true;
            },
            // ownerScan (heavyweight; only invoked on digest mismatch)
            [this](std::vector<SizeType>& out) {
                ++ownerScans;
                if (ownerScanFailNext) { ownerScanFailNext = false; return false; }
                out = authoritative.Snapshot();
                return true;
            },
            // applyDelta
            [this](const std::vector<SizeType>& adds,
                   const std::vector<SizeType>& deletes) {
                ++applyCalls;
                local.ApplyDelta(adds, deletes);
            });
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(HeadReplicaDriftTest)

// -----------------------------------------------------------------------
// Case 1: env-off dormancy.
// -----------------------------------------------------------------------
//
// Even though local and authoritative differ, with the env unset the
// wrapper must be a no-op: ownerDigest / ownerScan / applyDelta are
// never invoked, counters do not move, and the local set is unchanged.
// This guarantees the production hot path is byte-equal to baseline
// (no silent recall regression invariant).
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_HEAD_REPLICA_DRIFT", nullptr);

    RemotePostingOps ops;
    auto preCounters = ops.GetHeadDriftCounters();

    AuditHarness h;
    for (SizeType v : {1, 2, 3, 4}) h.local.Add(v);
    for (SizeType v : {1, 2, 3, 4, 5, 6}) h.authoritative.Add(v); // drift!

    auto result = h.Run(ops);

    BOOST_CHECK(result == RemotePostingOps::HeadDriftAuditResult::Dormant);
    BOOST_CHECK_EQUAL(h.localScans,   0);
    BOOST_CHECK_EQUAL(h.ownerDigests, 0);
    BOOST_CHECK_EQUAL(h.ownerScans,   0);
    BOOST_CHECK_EQUAL(h.applyCalls,   0);

    auto post = ops.GetHeadDriftCounters();
    BOOST_CHECK_EQUAL(post.detected,   preCounters.detected);
    BOOST_CHECK_EQUAL(post.reconciled, preCounters.reconciled);
    BOOST_CHECK_EQUAL(post.entryDelta, preCounters.entryDelta);

    // Local set unchanged.
    BOOST_CHECK_EQUAL(h.local.vids.size(), 4u);
}

// -----------------------------------------------------------------------
// Case 2: synced baseline.
// -----------------------------------------------------------------------
//
// env-armed but local == authoritative. Audit fires (digest computed)
// but reports InSync. ownerScan / applyDelta are NOT invoked (the
// design's two-tier audit cost guarantee). Counters do not move.
BOOST_AUTO_TEST_CASE(SyncedBaseline)
{
    ScopedEnv env("SPTAG_FAULT_HEAD_REPLICA_DRIFT", "1");

    RemotePostingOps ops;
    AuditHarness h;
    for (SizeType v : {10, 20, 30, 40, 50}) {
        h.local.Add(v);
        h.authoritative.Add(v);
    }

    auto result = h.Run(ops);

    BOOST_CHECK(result == RemotePostingOps::HeadDriftAuditResult::InSync);
    BOOST_CHECK_EQUAL(h.localScans,   1);
    BOOST_CHECK_EQUAL(h.ownerDigests, 1);
    BOOST_CHECK_EQUAL(h.ownerScans,   0);
    BOOST_CHECK_EQUAL(h.applyCalls,   0);

    auto post = ops.GetHeadDriftCounters();
    BOOST_CHECK_EQUAL(post.detected,   0u);
    BOOST_CHECK_EQUAL(post.reconciled, 0u);
    BOOST_CHECK_EQUAL(post.entryDelta, 0u);
}

// -----------------------------------------------------------------------
// Case 3: drift detected, delta recorded.
// -----------------------------------------------------------------------
//
// env-armed; local missing K=50 entries (exact drop count from the
// design-doc repro). Audit must detect mismatch, invoke ownerScan,
// apply the symmetric difference, and surface |Δ| in counters.
BOOST_AUTO_TEST_CASE(DriftDetectedAndDelta)
{
    ScopedEnv env("SPTAG_FAULT_HEAD_REPLICA_DRIFT", "1");

    RemotePostingOps ops;
    AuditHarness h;

    constexpr int N = 200;
    constexpr int K = 50;
    for (SizeType v = 0; v < N; ++v) {
        h.authoritative.Add(v);
        if (v >= K) h.local.Add(v); // local missing first K
    }
    BOOST_REQUIRE_EQUAL(h.local.vids.size(),         (std::size_t)(N - K));
    BOOST_REQUIRE_EQUAL(h.authoritative.vids.size(), (std::size_t)N);

    auto result = h.Run(ops);

    BOOST_CHECK(result == RemotePostingOps::HeadDriftAuditResult::Repaired);
    BOOST_CHECK_EQUAL(h.localScans,   1);
    BOOST_CHECK_EQUAL(h.ownerDigests, 1);
    BOOST_CHECK_EQUAL(h.ownerScans,   1);
    BOOST_CHECK_EQUAL(h.applyCalls,   1);

    auto post = ops.GetHeadDriftCounters();
    BOOST_CHECK_EQUAL(post.detected,   1u);
    BOOST_CHECK_EQUAL(post.reconciled, 1u);
    BOOST_CHECK_EQUAL(post.entryDelta, (std::uint64_t)K);

    // Convergence invariant: local now equals authoritative byte-for-byte.
    BOOST_CHECK(h.local.Snapshot() == h.authoritative.Snapshot());
}

// -----------------------------------------------------------------------
// Case 4: reconcile to zero.
// -----------------------------------------------------------------------
//
// After a repaired pass, a follow-up audit must observe equal digests
// and skip ownerScan / applyDelta entirely. Demonstrates monotonic
// convergence: counters do NOT bump on the second pass.
BOOST_AUTO_TEST_CASE(ReconcileToZero)
{
    ScopedEnv env("SPTAG_FAULT_HEAD_REPLICA_DRIFT", "1");

    RemotePostingOps ops;
    AuditHarness h;
    for (SizeType v = 0; v < 32; ++v) {
        h.authoritative.Add(v);
        if ((v & 1) == 0) h.local.Add(v); // local has only evens; missing 16 odds.
    }

    // Pass 1: repair.
    auto r1 = h.Run(ops);
    BOOST_REQUIRE(r1 == RemotePostingOps::HeadDriftAuditResult::Repaired);
    auto mid = ops.GetHeadDriftCounters();
    BOOST_CHECK_EQUAL(mid.detected,   1u);
    BOOST_CHECK_EQUAL(mid.reconciled, 1u);
    BOOST_CHECK_EQUAL(mid.entryDelta, 16u);

    // Pass 2: should be a no-op (digest equal, no reconcile).
    auto r2 = h.Run(ops);
    BOOST_CHECK(r2 == RemotePostingOps::HeadDriftAuditResult::InSync);
    BOOST_CHECK_EQUAL(h.ownerScans, 1); // unchanged from pass 1
    BOOST_CHECK_EQUAL(h.applyCalls, 1); // unchanged from pass 1

    auto post = ops.GetHeadDriftCounters();
    BOOST_CHECK_EQUAL(post.detected,   mid.detected);
    BOOST_CHECK_EQUAL(post.reconciled, mid.reconciled);
    BOOST_CHECK_EQUAL(post.entryDelta, mid.entryDelta);

    // Final convergence: local == authoritative byte-for-byte.
    BOOST_CHECK(h.local.Snapshot() == h.authoritative.Snapshot());
}

// -----------------------------------------------------------------------
// Case 5: harness smoke — bidirectional drift over multiple passes.
// -----------------------------------------------------------------------
//
// Combines adds + deletes (local has ghosts that authoritative dropped,
// and is missing fresh entries the owner has) and runs three audit
// passes back-to-back. Validates:
//   - first pass repairs the symmetric difference and counters reflect
//     |adds| + |deletes|.
//   - second pass is a no-op (no drift).
//   - injecting fresh drift between passes makes the third pass repair
//     again, with counters monotonic.
BOOST_AUTO_TEST_CASE(HarnessSmoke)
{
    ScopedEnv env("SPTAG_FAULT_HEAD_REPLICA_DRIFT", "1");

    RemotePostingOps ops;
    AuditHarness h;

    // Initial state: 10 shared, 5 ghosts (local-only), 7 missing (auth-only).
    for (SizeType v = 0; v < 10; ++v) { h.local.Add(v); h.authoritative.Add(v); }
    for (SizeType v = 100; v < 105; ++v) h.local.Add(v);          // ghosts
    for (SizeType v = 200; v < 207; ++v) h.authoritative.Add(v);  // missing

    // Pass 1: 5 deletes + 7 adds = 12.
    auto r1 = h.Run(ops);
    BOOST_REQUIRE(r1 == RemotePostingOps::HeadDriftAuditResult::Repaired);
    auto c1 = ops.GetHeadDriftCounters();
    BOOST_CHECK_EQUAL(c1.detected,   1u);
    BOOST_CHECK_EQUAL(c1.reconciled, 1u);
    BOOST_CHECK_EQUAL(c1.entryDelta, 12u);
    BOOST_CHECK(h.local.Snapshot() == h.authoritative.Snapshot());

    // Pass 2: no drift introduced -> InSync, counters frozen.
    auto r2 = h.Run(ops);
    BOOST_CHECK(r2 == RemotePostingOps::HeadDriftAuditResult::InSync);
    auto c2 = ops.GetHeadDriftCounters();
    BOOST_CHECK_EQUAL(c2.detected,   c1.detected);
    BOOST_CHECK_EQUAL(c2.reconciled, c1.reconciled);
    BOOST_CHECK_EQUAL(c2.entryDelta, c1.entryDelta);

    // Inject fresh drift: 3 new auth-side adds.
    for (SizeType v = 500; v < 503; ++v) h.authoritative.Add(v);

    // Pass 3: detect + repair the +3.
    auto r3 = h.Run(ops);
    BOOST_REQUIRE(r3 == RemotePostingOps::HeadDriftAuditResult::Repaired);
    auto c3 = ops.GetHeadDriftCounters();
    BOOST_CHECK_EQUAL(c3.detected,   c1.detected + 1);
    BOOST_CHECK_EQUAL(c3.reconciled, c1.reconciled + 1);
    BOOST_CHECK_EQUAL(c3.entryDelta, c1.entryDelta + 3);

    // Final convergence invariant.
    BOOST_CHECK(h.local.Snapshot() == h.authoritative.Snapshot());

    // Sanity: Failed-path is reachable but does NOT bump reconciled.
    {
        AuditHarness h2;
        for (SizeType v = 0; v < 4; ++v) { h2.local.Add(v); h2.authoritative.Add(v); }
        h2.authoritative.Add(99); // induce mismatch
        h2.ownerScanFailNext = true;
        auto r = h2.Run(ops);
        BOOST_CHECK(r == RemotePostingOps::HeadDriftAuditResult::Drift);
        auto cF = ops.GetHeadDriftCounters();
        // detected bumped (mismatch observed), but no reconcile / delta.
        BOOST_CHECK_EQUAL(cF.detected,   c3.detected + 1);
        BOOST_CHECK_EQUAL(cF.reconciled, c3.reconciled);
        BOOST_CHECK_EQUAL(cF.entryDelta, c3.entryDelta);
    }
}

BOOST_AUTO_TEST_SUITE_END()
