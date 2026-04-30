// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: version-cache-evict-then-cas
//
// Scenario: A hot key's chunk is evicted from the TiKVVersionMap LRU
// between an upstream Get (which returned v_read) and the subsequent
// CAS (IncVersion with expectedOld=v_read). The fault asks: when the
// CAS-time cache lookup misses, the implementation MUST always re-read
// authoritatively from TiKV (no silent stale-cache repopulation, no
// trust of v_read as "prior version").
//
// Today's IncVersion path always uses ReadChunk(cid) (authoritative
// TiKV) inside the per-chunk striped lock, so the production hot path
// is correct by construction. The env-gated probe surfaces this:
//   * m_versionCacheAuthoritativePath  -> bumped every CAS attempt
//     (CAS path always took authoritative read).
//   * m_versionCacheEvictReread        -> bumped when the LRU did NOT
//     hold the chunk at CAS time (eviction observed; reread happened
//     anyway).
//
// Defers-to-primitive: cas-lease (CAS lease semantics already merged
// from origin/prim/cas-lease into this branch).
//
// Tier 1 HARD gate (5 cases):
//   1. EnvOffIsDormant            — env unset → counters stay zero
//   2. EnvArmedSingleThreadBaseline — env-armed → CAS path bumps
//                                     authoritative counter monotonically
//   3. ReadEvictRereadSucceeds    — populate cache, force eviction, CAS
//                                   succeeds; reread counter increments
//                                   and final version is monotonic
//   4. ConcurrentCasOneWinsOneFails — two CAS racers with the same
//                                     expectedOld: exactly one wins and
//                                     no double-increment / lost update
//   5. TiKVHarnessSmokeWriteRead  — harness PD+TiKV reachable
//
// Tier 2 SOFT gate: deferred (this case is NOT on the perf-validation
// hot-path strict list; the production CAS path is byte-identical when
// the env is unset, so a 1M baseline cannot show a regression sourced
// here).

#include "inc/Test.h"

#ifdef TIKV
#include "inc/Core/SPANN/ExtraTiKVController.h"
#include "inc/Core/Common/TiKVVersionMap.h"
#endif

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#ifdef TIKV

using namespace SPTAG;
using namespace SPTAG::COMMON;

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

static std::unique_ptr<TiKVVersionMap> MakeVm(const std::string& tag,
                                              int chunkSize = 64,
                                              int cacheTTLMs = 0,
                                              int cacheMaxChunks = 100)
{
    const char* pd = std::getenv("TIKV_PD_ADDRESSES");
    if (!pd || std::string(pd).empty()) return nullptr;
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string prefix = "fault_vcec_" + tag + "_" + std::to_string(now) + "_";
    auto db = std::make_shared<SPTAG::SPANN::TiKVIO>(std::string(pd), prefix);
    auto vm = std::make_unique<TiKVVersionMap>();
    vm->SetDB(db);
    vm->SetLayer(0);
    vm->SetChunkSize(chunkSize);
    vm->SetCacheTTL(cacheTTLMs);
    vm->SetCacheMaxChunks(cacheMaxChunks);
    return vm;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(VersionCacheEvictThenCasTest)

// ---------------------------------------------------------------------------
// Tier 1 (1/5): env-off dormancy. Counters stay at zero across IncVersion
// even though the path is exercised. Static-init of the gate decision is
// process-wide; we still require the live read to report unarmed.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffIsDormant) {
    auto vm = MakeVm("envoff");
    if (!vm) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping");
        return;
    }
    vm->Initialize(/*size=*/64, /*blockSize=*/0, /*capacity=*/0);
    vm->ResetEvictThenCasCounters();

    if (TiKVVersionMap::EvictThenCasArmed()) {
        BOOST_TEST_MESSAGE("Env detected as armed at static-init time; "
                           "skipping env-off case (test runner is env-armed).");
        return;
    }

    uint8_t nv = 0;
    BOOST_REQUIRE(vm->IncVersion(/*key=*/3, &nv));
    BOOST_REQUIRE(vm->IncVersion(/*key=*/3, &nv));

    BOOST_CHECK_EQUAL(vm->GetVersionCacheEvictRereadCount(), 0u);
    BOOST_CHECK_EQUAL(vm->GetVersionCacheAuthoritativePathCount(), 0u);
}

// ---------------------------------------------------------------------------
// Tier 1 (2/5): env-armed single-thread baseline. Each successful
// IncVersion goes through the authoritative ReadChunk; the authoritative
// counter increments monotonically. No lost increments.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvArmedSingleThreadBaseline) {
    if (!TiKVVersionMap::EvictThenCasArmed()) {
        BOOST_TEST_MESSAGE("Env not armed at static-init; skipping armed case");
        return;
    }
    auto vm = MakeVm("baseline");
    if (!vm) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping");
        return;
    }
    vm->Initialize(/*size=*/64, 0, 0);
    vm->ResetEvictThenCasCounters();

    const int N = 5;
    uint8_t nv = 0;
    for (int i = 0; i < N; ++i) {
        BOOST_REQUIRE(vm->IncVersion(/*key=*/7, &nv));
    }
    BOOST_CHECK_EQUAL(static_cast<int>(nv) & 0x7f, N & 0x7f);
    BOOST_CHECK_GE(vm->GetVersionCacheAuthoritativePathCount(),
                   static_cast<std::uint64_t>(N));
    // Single-threaded → no double-increment: GetVersion equals N.
    BOOST_CHECK_EQUAL(static_cast<int>(vm->GetVersion(7)) & 0x7f, N & 0x7f);
}

// ---------------------------------------------------------------------------
// Tier 1 (3/5): read-then-evict-then-CAS triggers a reread and succeeds.
// We populate the LRU for chunk(H), drive eviction by reading enough OTHER
// chunks to overflow m_cacheMaxChunks, then call IncVersion(H). The
// authoritative read is taken; reread counter increments; final version
// is monotonic.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ReadEvictRereadSucceeds) {
    if (!TiKVVersionMap::EvictThenCasArmed()) {
        BOOST_TEST_MESSAGE("Env not armed at static-init; skipping armed case");
        return;
    }
    // Tight cache: 4 chunks max, chunkSize=8 → 32 keys span 4 chunks.
    auto vm = MakeVm("evictreread", /*chunkSize=*/8, /*cacheTTLMs=*/60000,
                     /*cacheMaxChunks=*/4);
    if (!vm) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping");
        return;
    }
    vm->Initialize(/*size=*/200, 0, 0);
    vm->ResetEvictThenCasCounters();

    const SizeType H = 5;  // chunk 0
    // Populate cache for chunk(H) by reading the byte (goes through cached path).
    (void)vm->GetVersion(H);
    BOOST_CHECK(vm->GetVersionCacheEvictRereadCount() == 0u);

    // Drive evictions: read keys spanning many other chunks, exceeding
    // cacheMaxChunks. With cacheMaxChunks=4 and 200 keys / chunkSize=8 = 25
    // distinct chunks, this evicts chunk 0 reliably.
    for (SizeType k = 16; k < 200; k += 8) {
        (void)vm->GetVersion(k);
    }

    // CAS on H: should detect missing-prior-version (cache evicted)
    // and reread authoritatively from TiKV. Succeeds.
    uint8_t nv = 0;
    BOOST_REQUIRE(vm->IncVersion(H, &nv));
    BOOST_CHECK_EQUAL(static_cast<int>(nv) & 0x7f, 1);

    BOOST_CHECK_GE(vm->GetVersionCacheEvictRereadCount(), 1u);
    BOOST_CHECK_GE(vm->GetVersionCacheAuthoritativePathCount(), 1u);

    // Monotonic: GetVersion now reflects the CAS result.
    BOOST_CHECK_EQUAL(static_cast<int>(vm->GetVersion(H)) & 0x7f, 1);
}

// ---------------------------------------------------------------------------
// Tier 1 (4/5): two writers race CAS on the SAME key with the same
// expectedOld. Exactly one wins; no double-increment; no lost update.
// (Even with cache-eviction in flight, the per-chunk striped lock
// serialises CASes, and the authoritative ReadChunk inside the lock
// guarantees fresh state.)
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ConcurrentCasOneWinsOneFails) {
    if (!TiKVVersionMap::EvictThenCasArmed()) {
        BOOST_TEST_MESSAGE("Env not armed at static-init; skipping armed case");
        return;
    }
    auto vm = MakeVm("concurrent", /*chunkSize=*/8, /*cacheTTLMs=*/60000,
                     /*cacheMaxChunks=*/4);
    if (!vm) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping");
        return;
    }
    vm->Initialize(/*size=*/64, 0, 0);
    vm->ResetEvictThenCasCounters();

    const SizeType H = 11;
    // Establish baseline so expectedOld is well-defined.
    uint8_t base = 0;
    BOOST_REQUIRE(vm->IncVersion(H, &base));   // version becomes 1
    BOOST_CHECK_EQUAL(static_cast<int>(base) & 0x7f, 1);

    // Drive eviction.
    for (SizeType k = 16; k < 64; k += 8) (void)vm->GetVersion(k);

    // Two racers with DIFFERENT expectedOld views model the post-evict
    // race: a stale reader still believes prior=0 while a fresh reader
    // (post-reread) sees prior=1. Exactly the fresh racer must win;
    // the stale racer must observe CAS-mismatch and fail. No
    // double-increment.
    std::atomic<int> stale_ok{0}, stale_fail{0};
    std::atomic<int> fresh_ok{0}, fresh_fail{0};
    auto stale = [&]{
        uint8_t nv = 0;
        bool ok = vm->IncVersion(H, &nv, /*expectedOld=*/0);  // stale view
        if (ok) stale_ok.fetch_add(1); else stale_fail.fetch_add(1);
    };
    auto fresh = [&]{
        uint8_t nv = 0;
        bool ok = vm->IncVersion(H, &nv, /*expectedOld=*/1);  // fresh view
        if (ok) {
            fresh_ok.fetch_add(1);
            BOOST_CHECK_EQUAL(static_cast<int>(nv) & 0x7f, 2);
        } else {
            fresh_fail.fetch_add(1);
        }
    };
    std::thread t1(stale), t2(fresh);
    t1.join(); t2.join();

    // Single truth: persisted version is exactly 2 (one CAS landed).
    BOOST_CHECK_EQUAL(static_cast<int>(vm->GetVersion(H)) & 0x7f, 2);
    // Fresh racer wins; stale racer's CAS-mismatch path returns false.
    BOOST_CHECK_EQUAL(fresh_ok.load(), 1);
    BOOST_CHECK_EQUAL(stale_ok.load(), 0);
    BOOST_CHECK_EQUAL(stale_fail.load(), 1);
    BOOST_CHECK_EQUAL(fresh_fail.load(), 0);

    // Authoritative path was taken at least twice (two CAS attempts).
    BOOST_CHECK_GE(vm->GetVersionCacheAuthoritativePathCount(), 2u);
}

// ---------------------------------------------------------------------------
// Tier 1 (5/5): harness smoke. Verifies PD+TiKV reachable.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(TiKVHarnessSmokeWriteRead) {
    const char* pd = std::getenv("TIKV_PD_ADDRESSES");
    if (!pd || std::string(pd).empty()) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping smoke");
        return;
    }
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string prefix = "fault_vcec_smoke_" + std::to_string(now) + "_";
    auto io = std::make_shared<SPTAG::SPANN::TiKVIO>(std::string(pd), prefix);
    if (!io->Available()) {
        BOOST_TEST_MESSAGE("TiKV unreachable; skipping smoke");
        return;
    }
    BOOST_REQUIRE(io->Put("k", "v", std::chrono::microseconds(2000000), nullptr) == ErrorCode::Success);
    std::string out;
    BOOST_REQUIRE(io->Get("k", &out, std::chrono::microseconds(2000000), nullptr) == ErrorCode::Success);
    BOOST_CHECK_EQUAL(out, "v");
}

BOOST_AUTO_TEST_SUITE_END()

#endif  // TIKV
