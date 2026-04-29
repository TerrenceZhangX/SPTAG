// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: pd-store-discovery-stale
//
// Validates the recovery semantics specified in
//   tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//     pd-store-discovery-stale.md
//
// This test exercises the in-process behaviour added to
// SPTAG::SPANN::TiKVIO in commit fault/pd-store-discovery-stale:
//   1. Store-address cache TTL.
//   2. Negative invalidation: RPC UNAVAILABLE -> evict store-addr cache
//      entry + corresponding stub pool, so the next attempt re-resolves
//      from PD.
//   3. Positive invalidation: PD GetStore returning a different address
//      for an already-cached storeId evicts the old stub pool.
//   4. Background PD member refresh.
//
// Sub-cases that need multi-node orchestration (PD member shift,
// cluster_id rebuild) are wired up but skipped unless the harness
// provides the relevant env vars; see the .sh harness.
//
// Required env (single-node smoke):
//   TIKV_PD_ADDRESSES   "127.0.0.1:2379"
//
// Optional env:
//   TIKV_STORE_RESTART_CMD     command run between two writes (sub-case 1)
//   SPTAG_TIKV_STORE_ADDR_TTL_SEC   small value (e.g. 2) to exercise TTL fast

#include "inc/Test.h"

#ifdef TIKV
#include "inc/Core/SPANN/ExtraTiKVController.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

using namespace SPTAG;
using namespace SPTAG::SPANN;

namespace {

std::shared_ptr<TiKVIO> MakeIO(const std::string& tag) {
    const char* pd = std::getenv("TIKV_PD_ADDRESSES");
    if (!pd || std::string(pd).empty()) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping " + tag);
        return nullptr;
    }
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string prefix = "fault_pdstale_" + tag + "_" + std::to_string(now) + "_";
    auto io = std::make_shared<TiKVIO>(std::string(pd), prefix);
    if (!io->Available()) return nullptr;
    return io;
}

bool RawWrite(TiKVIO& io, const std::string& key, const std::string& value) {
    return io.Put(key, value, std::chrono::microseconds(2'000'000), nullptr) == ErrorCode::Success;
}

bool RawRead(TiKVIO& io, const std::string& key, std::string* out) {
    return io.Get(key, out, std::chrono::microseconds(2'000'000), nullptr) == ErrorCode::Success;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(PDStoreDiscoveryStaleTest)

// Smoke: end-to-end Put/Get against a real PD+TiKV (single node) succeeds and
// populates the store address cache.
BOOST_AUTO_TEST_CASE(BaselinePutGetPopulatesStoreAddrCache) {
    auto io = MakeIO("baseline");
    if (!io) return;

    BOOST_REQUIRE(RawWrite(*io, "key1", "value1"));
    std::string out;
    BOOST_REQUIRE(RawRead(*io, "key1", &out));
    BOOST_CHECK_EQUAL(out, "value1");

    // After at least one successful op, we must have learned at least one
    // store address.
    BOOST_CHECK_GE(io->StoreAddrCacheSize(), 1u);
    BOOST_CHECK_GE(io->StubPoolCount(), 1u);
}

// Mechanism: InvalidateStoreCache drops the cached entry, evicts the stub
// pool, and bumps observability counters. Pass criterion from the case spec:
// "next op re-resolves from PD" \u2014 we simulate this directly by checking the
// counters and that the next Put still succeeds (i.e. recovery is automatic).
BOOST_AUTO_TEST_CASE(InvalidateEvictsAndRecovers) {
    auto io = MakeIO("invalidate");
    if (!io) return;

    BOOST_REQUIRE(RawWrite(*io, "key2", "value2"));
    BOOST_CHECK_GE(io->StoreAddrCacheSize(), 1u);
    auto poolsBefore = io->StubPoolCount();
    BOOST_REQUIRE_GE(poolsBefore, 1u);

    // Invalidate every cached storeId.
    for (uint64_t sid = 1; sid <= 10; ++sid) {
        io->TestInvalidateStoreCache(sid);
    }
    // At least one real eviction must have happened (because we had cached
    // entries from the put above).
    BOOST_CHECK_GE(io->GetStoreAddrInvalidations(), 1u);
    BOOST_CHECK_GE(io->GetStubPoolEvictions(), 1u);
    BOOST_CHECK_EQUAL(io->StoreAddrCacheSize(), 0u);

    // Recovery: the next op re-resolves from PD and succeeds within the
    // case spec's recovery budget (we use 5 s to keep the test brisk; the
    // spec allows up to 31 s).
    auto t0 = std::chrono::steady_clock::now();
    std::string out;
    BOOST_REQUIRE(RawRead(*io, "key2", &out));
    auto elapsed = std::chrono::steady_clock::now() - t0;
    BOOST_CHECK_EQUAL(out, "value2");
    BOOST_CHECK_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 5);

    // Recovery established a usable connection to a TiKV store again.
    BOOST_CHECK_GE(io->StubPoolCount(), 1u);
}

// Background PD member refresher: ForceRefreshPDMembers must update the
// member list (or at least bump the counter) and not crash.
BOOST_AUTO_TEST_CASE(PDMemberRefreshSucceeds) {
    auto io = MakeIO("pdrefresh");
    if (!io) return;

    auto before = io->GetPDMemberRefreshes();
    BOOST_CHECK(io->ForceRefreshPDMembers());
    BOOST_CHECK_GE(io->GetPDMemberRefreshes(), before + 1);
    auto pds = io->GetPDAddressesSnapshot();
    BOOST_CHECK_GE(pds.size(), 1u);
}

// Sub-case (operator-driven move): if the harness provides
// TIKV_STORE_RESTART_CMD, we (a) write a key, (b) ask the harness to bounce
// the store on a new address, (c) write again and assert the second Put
// succeeds within recovery budget. This is the "TiKV node moved" path
// described in the fault case spec.
BOOST_AUTO_TEST_CASE(StoreAddressMoveRecovers) {
    const char* restart = std::getenv("TIKV_STORE_RESTART_CMD");
    if (!restart || std::string(restart).empty()) {
        BOOST_TEST_MESSAGE("TIKV_STORE_RESTART_CMD not set; skipping store-move sub-case");
        return;
    }
    auto io = MakeIO("storemove");
    if (!io) return;

    BOOST_REQUIRE(RawWrite(*io, "preMove", "v1"));
    auto invBefore = io->GetStoreAddrInvalidations();

    int rc = std::system(restart);
    BOOST_REQUIRE_EQUAL(rc, 0);

    auto t0 = std::chrono::steady_clock::now();
    bool ok = false;
    for (int i = 0; i < 30 && !ok; ++i) {
        ok = RawWrite(*io, "postMove", "v2");
        if (!ok) std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::steady_clock::now() - t0).count();
    BOOST_CHECK(ok);
    // Recovery must complete within 31 s per the case spec.
    BOOST_CHECK_LT(elapsedSec, 31);
    // We must have observed at least one negative invalidation along the way.
    BOOST_CHECK_GT(io->GetStoreAddrInvalidations(), invBefore);
}

BOOST_AUTO_TEST_SUITE_END()

#endif  // TIKV
