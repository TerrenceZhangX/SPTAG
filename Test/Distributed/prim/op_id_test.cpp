// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Unit tests for the OpId / OpIdCache idempotency primitive (prim/op-id).
//
// Scope: cover the four contract points from the design note —
//   1. dedup        — same OpId twice, second call replays cached result.
//   2. eviction     — exceeding cache capacity drops the LRU tail.
//   3. TTL          — expired entries are treated as misses.
//   4. restartEpoch — a sender restart invalidates same-counter, old-epoch
//                     entries.

#include "inc/Core/SPANN/Distributed/OpId.h"
#include "inc/Core/SPANN/Distributed/OpIdCache.h"
#include "inc/Test.h"

#include <chrono>
#include <thread>

using namespace SPTAG::SPANN::Distributed;

namespace {

struct CountedResult {
    int value = 0;
};

OpId Make(std::int32_t sender, std::uint32_t epoch, std::uint64_t counter) {
    return OpId(sender, epoch, counter);
}

}  // namespace

BOOST_AUTO_TEST_SUITE(OpIdIdempotencyTest)

BOOST_AUTO_TEST_CASE(AllocatorMintsMonotonicIds)
{
    OpIdAllocator alloc(/*senderId=*/7, /*epoch=*/1);
    OpId a = alloc.Next();
    OpId b = alloc.Next();
    OpId c = alloc.Next();
    BOOST_CHECK_EQUAL(a.senderId, 7);
    BOOST_CHECK_EQUAL(a.restartEpoch, 1u);
    BOOST_CHECK(a.monotonicCounter < b.monotonicCounter);
    BOOST_CHECK(b.monotonicCounter < c.monotonicCounter);
    BOOST_CHECK(a != b);
}

BOOST_AUTO_TEST_CASE(DedupSameIdReplaysCachedResult)
{
    OpIdCache<CountedResult> cache(/*capacity=*/16, std::chrono::seconds(60));
    OpId id = Make(1, 1, 100);

    auto [hit0, _0] = cache.Lookup(id);
    BOOST_CHECK(!hit0);

    CountedResult first;
    first.value = 42;
    BOOST_CHECK(cache.Insert(id, first));

    auto [hit1, cached1] = cache.Lookup(id);
    BOOST_CHECK(hit1);
    BOOST_CHECK_EQUAL(cached1.value, 42);

    // A second arrival with the same OpId is a hit — the receiver must
    // replay the cached value, not re-execute.
    auto [hit2, cached2] = cache.Lookup(id);
    BOOST_CHECK(hit2);
    BOOST_CHECK_EQUAL(cached2.value, 42);
}

BOOST_AUTO_TEST_CASE(EvictionDropsLruTailWhenCapacityExceeded)
{
    OpIdCache<CountedResult> cache(/*capacity=*/3, std::chrono::seconds(60));
    OpId a = Make(1, 1, 1);
    OpId b = Make(1, 1, 2);
    OpId c = Make(1, 1, 3);
    OpId d = Make(1, 1, 4);

    cache.Insert(a, CountedResult{1});
    cache.Insert(b, CountedResult{2});
    cache.Insert(c, CountedResult{3});
    BOOST_CHECK_EQUAL(cache.Size(), 3u);

    // Touch `a` so it is most-recently-used; `b` becomes LRU tail.
    auto [hitA, _] = cache.Lookup(a);
    BOOST_CHECK(hitA);

    // Overflow: `b` should evict.
    cache.Insert(d, CountedResult{4});
    BOOST_CHECK_EQUAL(cache.Size(), 3u);

    auto [hitB, __] = cache.Lookup(b);
    BOOST_CHECK(!hitB);  // evicted

    // a, c, d still present.
    BOOST_CHECK(cache.Lookup(a).first);
    BOOST_CHECK(cache.Lookup(c).first);
    BOOST_CHECK(cache.Lookup(d).first);

    // The op that was evicted is now eligible for re-execution: a fresh
    // Insert with the same OpId should succeed and report a *new* entry.
    BOOST_CHECK(cache.Insert(b, CountedResult{99}));
    auto [hitB2, cachedB2] = cache.Lookup(b);
    BOOST_CHECK(hitB2);
    BOOST_CHECK_EQUAL(cachedB2.value, 99);
}

BOOST_AUTO_TEST_CASE(TtlExpiryAllowsReExecution)
{
    OpIdCache<CountedResult> cache(/*capacity=*/16, std::chrono::milliseconds(50));
    OpId id = Make(1, 1, 7);
    cache.Insert(id, CountedResult{55});

    auto [hit1, _] = cache.Lookup(id);
    BOOST_CHECK(hit1);

    std::this_thread::sleep_for(std::chrono::milliseconds(120));

    auto [hit2, __] = cache.Lookup(id);
    BOOST_CHECK(!hit2);  // expired -> miss, re-execution allowed.

    // After expiry we can record a new result for the same OpId.
    BOOST_CHECK(cache.Insert(id, CountedResult{77}));
    auto [hit3, cached3] = cache.Lookup(id);
    BOOST_CHECK(hit3);
    BOOST_CHECK_EQUAL(cached3.value, 77);
}

BOOST_AUTO_TEST_CASE(RestartEpochInvalidatesSameCounterOldEpochEntries)
{
    OpIdCache<CountedResult> cache(/*capacity=*/16, std::chrono::seconds(60));

    // Pre-restart op: sender=1, epoch=1, counter=42.
    OpId pre = Make(1, 1, 42);
    cache.Insert(pre, CountedResult{1});
    BOOST_CHECK(cache.Lookup(pre).first);

    // Sender restarts -> epoch bumps to 2. Receiver MUST drop epoch=1
    // entries for sender 1 so a new op (epoch=2, counter=42) cannot be
    // mistaken for a retry.
    std::size_t removed = cache.InvalidateOlderEpochs(/*sender=*/1, /*newEpoch=*/2);
    BOOST_CHECK_EQUAL(removed, 1u);

    BOOST_CHECK(!cache.Lookup(pre).first);

    // Same-counter, new-epoch entry is a *different* OpId and is allowed.
    OpId post = Make(1, 2, 42);
    BOOST_CHECK(!cache.Lookup(post).first);
    cache.Insert(post, CountedResult{2});
    auto [hitPost, cachedPost] = cache.Lookup(post);
    BOOST_CHECK(hitPost);
    BOOST_CHECK_EQUAL(cachedPost.value, 2);

    // Other senders must be unaffected.
    OpId other = Make(/*sender=*/2, /*epoch=*/1, /*counter=*/42);
    cache.Insert(other, CountedResult{9});
    BOOST_CHECK_EQUAL(cache.InvalidateOlderEpochs(1, 2), 0u);
    BOOST_CHECK(cache.Lookup(other).first);
}

BOOST_AUTO_TEST_SUITE_END()
