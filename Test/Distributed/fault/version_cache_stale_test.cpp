// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: version-cache-stale
//
// Spec:   tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/version-cache-stale.md
// Branch: fault/version-cache-stale
//
// Tier-1 unit repro of the TiKVVersionMap LRU-cache-vs-authoritative
// staleness window. Two writers race CAS-incrementing the same VID; the
// FT scenario is that the loser's read came out of the LRU cache and is
// stale, so a naive `current != expectedOld → return false` lets the
// caller silently lose the update or, worse, derive a target from the
// stale value and double-increment.
//
// The fix lives in TiKVVersionMap::IncVersionFaultPath (env-gated):
//   1) Read via the (stale-prone) LRU cache.
//   2) On `current != expectedOld` mismatch, bump
//      `FaultVersionCacheCasMismatches`, drop the stale cache entry,
//      re-read authoritative chunk via ReadChunk (bypassing cache).
//   3) If authoritative agrees with caller's expectedOld → bump
//      `FaultVersionCacheCasRetries` and retry CAS with the fresh chunk.
//      If authoritative disagrees → genuine CAS conflict, return false.
//   4) Cap CAS-loser retries (8); on cap, degrade to
//      IncVersionAuthoritative — the env-off, no-cache path.
//
// Env-gate (per perf-validation-protocol.md):
//   SPTAG_FAULT_VERSION_CACHE_STALE=1 arms the fault-aware path.
// Unset / "" / "0" → original code verbatim, both counters dormant.
//
// Invariants asserted:
//   * env-off path is fully dormant (counters stay 0, no retry-loop entry)
//   * no lost update: a CAS-loser whose stale cache disagreed with TiKV
//     ultimately converges (either succeeds with target = expectedOld+1
//     or returns false on a genuine conflict)
//   * no double-increment: target is always (expectedOld+1) & 0x7f, never
//     derived from the stale cached `current`

#include "inc/Test.h"

#include "inc/Core/Common/TiKVVersionMap.h"
#include "inc/Helper/KeyValueIO.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::COMMON;

namespace {

// Minimal in-memory KeyValueIO for unit-driving TiKVVersionMap without
// standing up a real TiKV. Implements only the string Get/Put/MultiGet
// surface that TiKVVersionMap actually consults.
class InMemoryKV : public Helper::KeyValueIO
{
public:
    void ShutDown() override {}

    ErrorCode Get(const std::string& key, std::string* value,
                  const std::chrono::microseconds&,
                  std::vector<Helper::AsyncReadRequest>*) override
    {
        std::lock_guard<std::mutex> lk(m_mu);
        auto it = m_store.find(key);
        if (it == m_store.end()) { value->clear(); return ErrorCode::Fail; }
        *value = it->second;
        return ErrorCode::Success;
    }

    ErrorCode Get(const SizeType, std::string*,
                  const std::chrono::microseconds&,
                  std::vector<Helper::AsyncReadRequest>*) override
    {
        return ErrorCode::Undefined;
    }

    ErrorCode Put(const std::string& key, const std::string& value,
                  const std::chrono::microseconds&,
                  std::vector<Helper::AsyncReadRequest>*) override
    {
        std::lock_guard<std::mutex> lk(m_mu);
        m_store[key] = value;
        return ErrorCode::Success;
    }

    ErrorCode Put(const SizeType, const std::string&,
                  const std::chrono::microseconds&,
                  std::vector<Helper::AsyncReadRequest>*) override
    {
        return ErrorCode::Undefined;
    }

    ErrorCode MultiGet(const std::vector<std::string>& keys,
                       std::vector<std::string>* values,
                       const std::chrono::microseconds&,
                       std::vector<Helper::AsyncReadRequest>*) override
    {
        std::lock_guard<std::mutex> lk(m_mu);
        values->clear();
        values->reserve(keys.size());
        for (const auto& k : keys) {
            auto it = m_store.find(k);
            values->push_back(it == m_store.end() ? std::string() : it->second);
        }
        return ErrorCode::Success;
    }

    ErrorCode MultiGet(const std::vector<SizeType>&,
                       std::vector<std::string>*,
                       const std::chrono::microseconds&,
                       std::vector<Helper::AsyncReadRequest>*) override
    {
        return ErrorCode::Undefined;
    }

    ErrorCode Merge(const SizeType, const std::string&,
                    const std::chrono::microseconds&,
                    std::vector<Helper::AsyncReadRequest>*, int&) override
    {
        return ErrorCode::Undefined;
    }

    ErrorCode Delete(SizeType) override { return ErrorCode::Undefined; }

    bool Available() override { return true; }

    // Test-only: directly mutate authoritative store (bypasses TiKVVersionMap
    // cache). Used to simulate "another node updated TiKV while our LRU
    // cache still holds the previous value".
    void TEST_Force(const std::string& key, const std::string& value)
    {
        std::lock_guard<std::mutex> lk(m_mu);
        m_store[key] = value;
    }

    std::string TEST_Read(const std::string& key)
    {
        std::lock_guard<std::mutex> lk(m_mu);
        auto it = m_store.find(key);
        return it == m_store.end() ? std::string() : it->second;
    }

private:
    std::mutex m_mu;
    std::map<std::string, std::string> m_store;
};

bool ArmedFromEnv()
{
    const char* v = std::getenv("SPTAG_FAULT_VERSION_CACHE_STALE");
    return v != nullptr && v[0] != '\0' && std::string(v) != "0";
}

// Helpers for test plumbing.
std::shared_ptr<InMemoryKV> MakeKV() { return std::make_shared<InMemoryKV>(); }

// chunkId for the Test/CMake-friendly default chunkSize used below.
constexpr int kChunkSize = 16;
constexpr int kLayer = 0;

std::unique_ptr<TiKVVersionMap> MakeMap(const std::shared_ptr<InMemoryKV>& kv,
                                        SizeType vidCount = 4)
{
    auto m = std::unique_ptr<TiKVVersionMap>(new TiKVVersionMap());
    m->SetDB(kv);
    m->SetLayer(kLayer);
    m->SetChunkSize(kChunkSize);
    m->SetCacheTTL(60000);
    m->SetCacheMaxChunks(8);
    // Initialize allocates default (0xfe) chunks, so reset to 0x00 (alive)
    // after Initialize() to give us a clean canvas.
    m->Initialize(vidCount, 0, vidCount, nullptr);
    // Replace the all-deleted default with all-zero so our VIDs are alive.
    std::string aliveChunk(kChunkSize, static_cast<char>(0x00));
    kv->TEST_Force("v:0:0", aliveChunk);
    m->TEST_ONLY_InvalidateCachedChunk(0);
    return m;
}

uint8_t AuthoritativeByte(InMemoryKV& kv, SizeType vid)
{
    std::string chunk = kv.TEST_Read("v:0:0");
    return chunk.empty() ? 0xff : static_cast<uint8_t>(chunk[vid % kChunkSize]);
}

}  // namespace


BOOST_AUTO_TEST_SUITE(VersionCacheStaleTest)

// ---------------------------------------------------------------------------
// Case 1: env-off dormancy.
// With SPTAG_FAULT_VERSION_CACHE_STALE unset, the FT path must not run.
// Counters stay at 0 across an arbitrary IncVersion sequence including a
// genuine CAS rejection.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy) {
    if (ArmedFromEnv()) {
        BOOST_TEST_MESSAGE("[skip] env-armed run; EnvOffDormancy is env-off-only");
        return;
    }
    BOOST_REQUIRE(!TiKVVersionMap::IsFaultVersionCacheStaleEnabled());
    TiKVVersionMap::ResetFaultVersionCacheCounters();

    auto kv = MakeKV();
    auto m = MakeMap(kv);

    uint8_t v = 0xff;
    BOOST_CHECK(m->IncVersion(0, &v, /*expectedOld=*/0));
    BOOST_CHECK_EQUAL(v, 1);
    BOOST_CHECK(m->IncVersion(0, &v, 1));
    BOOST_CHECK_EQUAL(v, 2);

    // Trigger a genuine CAS rejection (expectedOld stale on purpose).
    BOOST_CHECK(!m->IncVersion(0, &v, 0));

    BOOST_CHECK_EQUAL(TiKVVersionMap::FaultVersionCacheCasMismatches(), 0u);
    BOOST_CHECK_EQUAL(TiKVVersionMap::FaultVersionCacheCasRetries(), 0u);
}

// ---------------------------------------------------------------------------
// Case 2: single-thread baseline under env-on.
// Without any planted staleness, the cache reflects authoritative TiKV at
// every step, so neither counter increments — env-on must not penalise the
// happy path.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOnSingleThreadNoMismatches) {
    if (!ArmedFromEnv()) {
        BOOST_TEST_MESSAGE("[skip] env-off run; EnvOnSingleThreadNoMismatches is env-armed-only");
        return;
    }
    BOOST_REQUIRE(TiKVVersionMap::IsFaultVersionCacheStaleEnabled());
    TiKVVersionMap::ResetFaultVersionCacheCounters();

    auto kv = MakeKV();
    auto m = MakeMap(kv);

    uint8_t v = 0xff;
    for (uint8_t i = 0; i < 10; ++i) {
        BOOST_REQUIRE(m->IncVersion(0, &v, i));
        BOOST_CHECK_EQUAL(v, i + 1);
    }
    BOOST_CHECK_EQUAL(AuthoritativeByte(*kv, 0), 10);
    BOOST_CHECK_EQUAL(TiKVVersionMap::FaultVersionCacheCasMismatches(), 0u);
    BOOST_CHECK_EQUAL(TiKVVersionMap::FaultVersionCacheCasRetries(), 0u);
}

// ---------------------------------------------------------------------------
// Case 3: concurrent CAS, two writers — one wins, one retries and converges.
// We deterministically construct the "stale cache window" by:
//   1) Writer-A succeeds a CAS 0→1 (chunk now holds 1, both authoritative
//      and cache).
//   2) Writer-B's view of authoritative is forcibly rolled back via the
//      LRU cache: we plant a STALE cached chunk where VID 0 reads as 0.
//   3) Writer-B issues IncVersion(expectedOld=1). Cached read returns 0;
//      `current != expectedOld` triggers the FT path: re-read authoritative
//      finds 1 (== expectedOld); CAS retried; success with target = 2.
// Invariants:
//   * Authoritative final value == 2 (no double-increment).
//   * Mismatches >= 1, retries >= 1.
//   * No lost update: Writer-A's prior write is preserved in the final
//     value (authoritative went 0 → 1 → 2).
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOnConcurrentCasOneWinnerOneRetries) {
    if (!ArmedFromEnv()) {
        BOOST_TEST_MESSAGE("[skip] env-off run; EnvOnConcurrentCasOneWinnerOneRetries is env-armed-only");
        return;
    }
    BOOST_REQUIRE(TiKVVersionMap::IsFaultVersionCacheStaleEnabled());
    TiKVVersionMap::ResetFaultVersionCacheCounters();

    auto kv = MakeKV();
    auto m = MakeMap(kv);

    // Step 1: Writer-A wins.
    uint8_t va = 0xff;
    BOOST_REQUIRE(m->IncVersion(0, &va, 0));
    BOOST_CHECK_EQUAL(va, 1);
    BOOST_CHECK_EQUAL(AuthoritativeByte(*kv, 0), 1);

    // Step 2: Plant a stale cached chunk that disagrees with TiKV.
    // Stale chunk shows VID 0 as 0 even though authoritative says 1.
    std::string staleChunk(kChunkSize, static_cast<char>(0x00));
    m->TEST_ONLY_PrimeStaleCache(/*chunkId=*/0, staleChunk);

    // Step 3: Writer-B issues CAS(expectedOld=1). Cached read shows 0 →
    // mismatch → FT path → re-read authoritative (1) → retry CAS → success.
    uint8_t vb = 0xff;
    BOOST_CHECK(m->IncVersion(0, &vb, 1));
    BOOST_CHECK_EQUAL(vb, 2);

    // Invariants.
    BOOST_CHECK_EQUAL(AuthoritativeByte(*kv, 0), 2);  // no double-increment
    BOOST_CHECK_GE(TiKVVersionMap::FaultVersionCacheCasMismatches(), 1u);
    BOOST_CHECK_GE(TiKVVersionMap::FaultVersionCacheCasRetries(), 1u);
}

// ---------------------------------------------------------------------------
// Case 4: LRU evict then CAS (stale-axis variant).
// The stale-cache fault must also catch the case where an entry was
// evicted between the read and the CAS write and a *resurrected stale*
// chunk re-enters the cache before the CAS resolves. We model that by:
//   1) Bumping authoritative TiKV directly (simulating another node's write).
//   2) Planting the previous (now-stale) chunk back into the cache.
//   3) Issuing IncVersion: cache hit returns stale; FT path detects
//      mismatch, drops the stale entry, re-reads authoritative.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOnLruEvictThenCasReReadsAuthoritative) {
    if (!ArmedFromEnv()) {
        BOOST_TEST_MESSAGE("[skip] env-off run; EnvOnLruEvictThenCasReReadsAuthoritative is env-armed-only");
        return;
    }
    BOOST_REQUIRE(TiKVVersionMap::IsFaultVersionCacheStaleEnabled());
    TiKVVersionMap::ResetFaultVersionCacheCounters();

    auto kv = MakeKV();
    auto m = MakeMap(kv);

    // Pump the chunk to authoritative version 5 by going through the map
    // (cache and authoritative agree at 5).
    uint8_t v = 0xff;
    for (uint8_t i = 0; i < 5; ++i) {
        BOOST_REQUIRE(m->IncVersion(0, &v, i));
    }
    BOOST_REQUIRE_EQUAL(AuthoritativeByte(*kv, 0), 5);

    // Simulate "another node bumped to 7", then a stale chunk at VID0=5
    // re-entered the cache (e.g. a sibling read after eviction got the
    // pre-update value via a slow follower).
    std::string authoritativeAt7 = kv->TEST_Read("v:0:0");
    authoritativeAt7[0] = static_cast<char>(7);
    kv->TEST_Force("v:0:0", authoritativeAt7);

    std::string staleAt5(kChunkSize, static_cast<char>(0x00));
    staleAt5[0] = static_cast<char>(5);
    m->TEST_ONLY_PrimeStaleCache(/*chunkId=*/0, staleAt5);

    // Caller sees authoritative 7, requests CAS(7→8). FT path will read
    // cache (5) → mismatch → re-read authoritative (7) → retry → 8.
    uint8_t out = 0xff;
    BOOST_CHECK(m->IncVersion(0, &out, 7));
    BOOST_CHECK_EQUAL(out, 8);
    BOOST_CHECK_EQUAL(AuthoritativeByte(*kv, 0), 8);

    BOOST_CHECK_GE(TiKVVersionMap::FaultVersionCacheCasMismatches(), 1u);
    BOOST_CHECK_GE(TiKVVersionMap::FaultVersionCacheCasRetries(), 1u);
}

// ---------------------------------------------------------------------------
// Case 5: harness smoke (env-on, trivial workload, counters monotonic).
// Asserts the counter API is callable, monotonic, and that resetting
// works as advertised.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOnHarnessSmokeCountersMonotonic) {
    if (!ArmedFromEnv()) {
        BOOST_TEST_MESSAGE("[skip] env-off run; EnvOnHarnessSmokeCountersMonotonic is env-armed-only");
        return;
    }
    BOOST_REQUIRE(TiKVVersionMap::IsFaultVersionCacheStaleEnabled());
    TiKVVersionMap::ResetFaultVersionCacheCounters();
    BOOST_CHECK_EQUAL(TiKVVersionMap::FaultVersionCacheCasMismatches(), 0u);
    BOOST_CHECK_EQUAL(TiKVVersionMap::FaultVersionCacheCasRetries(), 0u);

    auto kv = MakeKV();
    auto m = MakeMap(kv);

    uint8_t v = 0xff;
    BOOST_REQUIRE(m->IncVersion(0, &v, 0));

    std::string staleChunk(kChunkSize, static_cast<char>(0x00));
    m->TEST_ONLY_PrimeStaleCache(0, staleChunk);
    BOOST_REQUIRE(m->IncVersion(0, &v, 1));
    auto m1 = TiKVVersionMap::FaultVersionCacheCasMismatches();
    auto r1 = TiKVVersionMap::FaultVersionCacheCasRetries();
    BOOST_CHECK_GE(m1, 1u);
    BOOST_CHECK_GE(r1, 1u);

    m->TEST_ONLY_PrimeStaleCache(0, staleChunk);
    BOOST_REQUIRE(m->IncVersion(0, &v, 2));
    BOOST_CHECK_GE(TiKVVersionMap::FaultVersionCacheCasMismatches(), m1 + 1);
    BOOST_CHECK_GE(TiKVVersionMap::FaultVersionCacheCasRetries(), r1 + 1);

    // Reset returns counters to 0 even with env armed.
    TiKVVersionMap::ResetFaultVersionCacheCounters();
    BOOST_CHECK_EQUAL(TiKVVersionMap::FaultVersionCacheCasMismatches(), 0u);
    BOOST_CHECK_EQUAL(TiKVVersionMap::FaultVersionCacheCasRetries(), 0u);
}

BOOST_AUTO_TEST_SUITE_END()
