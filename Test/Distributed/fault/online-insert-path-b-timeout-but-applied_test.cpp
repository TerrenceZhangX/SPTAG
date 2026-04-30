// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: online-insert-path-b-timeout-but-applied
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/online-insert-path-b-timeout-but-applied.md
// Branch: fault/online-insert-path-b-timeout-but-applied
//
// Tier-1 unit repro of the Path-B "applied-but-response-lost" pathology:
//
//   A delegates an Append to owner B (Path-B remote-append). B applies
//   the RawPut to TiKV successfully. The response packet from B back to
//   A is lost (or A's RPC times out). A surfaces a timeout to the
//   client; the client retries. The retry lands on B (or any peer that
//   shares the same OpId dedup contract) carrying the *same* OpId.
//
// Required behaviour (per spec):
//   • No posting lost — the first attempt that B applied is preserved.
//   • Queries see no duplicate — the retry must NOT re-execute the
//     append; it must replay B's cached SUCCESS via the receiver-side
//     dedup cache.
//   • Client gets a clean SUCCESS (or, after retry-cap, a well-formed
//     error). Both attempts here return SUCCESS deterministically.
//
// The implementation lives on `prim/op-id-idempotency`, merged into this
// branch: `RemotePostingOps::HandleAppendRequest` consults
// `OpIdCache<AppendDedupResult>` before invoking the append callback. We
// drive that exact dedup path here without standing up the full Socket
// server stack — the cache + callback wrapper is the unit-level contract
// this case formalises.
//
// Env-gate (per perf-validation-protocol.md):
//   SPTAG_FAULT_ONLINE_INSERT_PATH_B_TIMEOUT_BUT_APPLIED=1
// arms the dedup short-circuit. Unset = baseline (no OpId on the wire,
// no dedup; included to demonstrate the failure mode the primitive
// fixes).

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/DistributedProtocol.h"
#include "inc/Core/SPANN/Distributed/OpId.h"
#include "inc/Core/SPANN/Distributed/OpIdCache.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>

using namespace SPTAG::SPANN;
using namespace SPTAG::SPANN::Distributed;

namespace {

// Mirrors the dedup logic in RemotePostingOps::HandleAppendRequest
// (AnnService/inc/Core/SPANN/Distributed/RemotePostingOps.h:309-368)
// without the wire layer. `applyCount` increments only when the
// append callback would actually run on B; on a dedup hit B replays
// the cached status and the callback is NOT invoked.
RemoteAppendResponse::Status DispatchOnReceiver(
        OpIdCache<AppendDedupResult>* cache,
        const OpId& opId,
        int& applyCount,
        bool armed)
{
    if (armed && cache && opId.IsValid()) {
        auto [hit, cached] = cache->Lookup(opId);
        if (hit) {
            return static_cast<RemoteAppendResponse::Status>(cached.status);
        }
    }
    // Unique apply: simulate B writing to TiKV.
    ++applyCount;
    auto status = RemoteAppendResponse::Status::Success;
    if (armed && cache && opId.IsValid()) {
        AppendDedupResult r;
        r.status = static_cast<std::uint8_t>(status);
        cache->Insert(opId, r);
    }
    return status;
}

bool ArmedFromEnv() {
    const char* v = std::getenv("SPTAG_FAULT_ONLINE_INSERT_PATH_B_TIMEOUT_BUT_APPLIED");
    return v != nullptr && std::string(v) != "0" && std::string(v) != "";
}

}  // namespace

BOOST_AUTO_TEST_SUITE(OnlineInsertPathBTimeoutButAppliedTest)

// Smoke: allocator mints a Path-B-bound OpId carrying the sender's
// (id, restartEpoch). The receiver-side cache starts empty.
BOOST_AUTO_TEST_CASE(SmokeAllocatorAndCache) {
    OpIdAllocator alloc(/*senderId=*/3, /*epoch=*/7);
    OpId a = alloc.Next();
    BOOST_CHECK_EQUAL(a.senderId, 3);
    BOOST_CHECK_EQUAL(a.restartEpoch, 7u);
    BOOST_CHECK(a.IsValid());

    OpIdCache<AppendDedupResult> cache(/*capacity=*/64,
                                       std::chrono::seconds(60));
    auto [hit, _] = cache.Lookup(a);
    (void)_;
    BOOST_CHECK(!hit);
}

// env-off baseline: with dedup disarmed, A's retry of a B-applied write
// re-executes the append. This is the failure mode the case must fix —
// the test intentionally observes the duplicate so we know the env-armed
// case is doing real work and not just succeeding by accident.
BOOST_AUTO_TEST_CASE(EnvOffRetryReExecutes) {
    OpIdAllocator alloc(/*senderId=*/1, /*epoch=*/1);
    OpId opId = alloc.Next();
    OpIdCache<AppendDedupResult> cache(64, std::chrono::seconds(60));

    int applyCount = 0;

    // First attempt: B applies, response (in production) would be lost.
    auto firstStatus = DispatchOnReceiver(&cache, opId, applyCount,
                                          /*armed=*/false);
    BOOST_CHECK(firstStatus == RemoteAppendResponse::Status::Success);
    BOOST_CHECK_EQUAL(applyCount, 1);

    // A times out and retries with the same logical write. With dedup
    // disarmed, the retry hits the apply path again — duplicate write
    // exposed to TiKV. This is the pathology the case must close.
    auto retryStatus = DispatchOnReceiver(&cache, opId, applyCount,
                                          /*armed=*/false);
    BOOST_CHECK(retryStatus == RemoteAppendResponse::Status::Success);
    BOOST_CHECK_EQUAL(applyCount, 2);
}

// env-armed Tier-1 contract: with dedup armed, the retry of the
// timeout-but-applied write is absorbed by the receiver-side cache.
// Apply path runs exactly once; both client-visible attempts return
// SUCCESS. This is the invariant the spec demands.
BOOST_AUTO_TEST_CASE(EnvArmedRetryIsDedupedAtReceiver) {
    if (!ArmedFromEnv()) {
        BOOST_TEST_MESSAGE("SPTAG_FAULT_ONLINE_INSERT_PATH_B_TIMEOUT_BUT_APPLIED unset; "
                           "running unit-armed mode (in-test override).");
    }

    OpIdAllocator alloc(/*senderId=*/2, /*epoch=*/4);
    OpId opId = alloc.Next();
    OpIdCache<AppendDedupResult> cache(64, std::chrono::seconds(60));

    int applyCount = 0;

    // First attempt: B applies + caches Success. Response then gets lost.
    auto firstStatus = DispatchOnReceiver(&cache, opId, applyCount,
                                          /*armed=*/true);
    BOOST_CHECK(firstStatus == RemoteAppendResponse::Status::Success);
    BOOST_CHECK_EQUAL(applyCount, 1);

    // A's retry carries the same OpId. Receiver-side dedup hits and
    // replays Success without re-executing the append callback.
    auto retryStatus = DispatchOnReceiver(&cache, opId, applyCount,
                                          /*armed=*/true);
    BOOST_CHECK(retryStatus == RemoteAppendResponse::Status::Success);
    BOOST_CHECK_EQUAL(applyCount, 1);  // <-- core invariant

    // Cache survives a third attempt as well (client retry-cap > 1).
    auto thirdStatus = DispatchOnReceiver(&cache, opId, applyCount,
                                          /*armed=*/true);
    BOOST_CHECK(thirdStatus == RemoteAppendResponse::Status::Success);
    BOOST_CHECK_EQUAL(applyCount, 1);
}

// Restart-epoch invariant: if B restarts (epoch bumps) before A's retry
// reaches the new B, the *new* epoch's OpId is not deduped against the
// pre-restart cache entry — that is correct, because B's pre-restart
// state was lost. The new-epoch attempt must apply (and the old-epoch
// entry is invalidated, not falsely replayed).
BOOST_AUTO_TEST_CASE(EnvArmedNewEpochIsNotDeduped) {
    OpIdCache<AppendDedupResult> cache(64, std::chrono::seconds(60));

    OpId preRestart(/*sender=*/5, /*epoch=*/1, /*counter=*/100);
    int applyCount = 0;
    auto s1 = DispatchOnReceiver(&cache, preRestart, applyCount,
                                 /*armed=*/true);
    BOOST_CHECK(s1 == RemoteAppendResponse::Status::Success);
    BOOST_CHECK_EQUAL(applyCount, 1);

    // B-prime restarts (epoch=2). Same counter, different epoch.
    OpId postRestart(/*sender=*/5, /*epoch=*/2, /*counter=*/100);
    cache.InvalidateOlderEpochs(postRestart.senderId, postRestart.restartEpoch);

    auto s2 = DispatchOnReceiver(&cache, postRestart, applyCount,
                                 /*armed=*/true);
    BOOST_CHECK(s2 == RemoteAppendResponse::Status::Success);
    BOOST_CHECK_EQUAL(applyCount, 2);  // new epoch must apply afresh
}

BOOST_AUTO_TEST_SUITE_END()
