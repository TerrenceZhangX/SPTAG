// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: tikv-grpc-stub-channel-broken
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/tikv-grpc-stub-channel-broken.md
// Branch: fault/tikv-grpc-stub-channel-broken
//
// Each fault case test file follows the pilot pattern from
// pd-store-discovery-stale: a few small Boost.Test cases that exercise
// the *unit-level* recovery contract against a real (single-node)
// PD + TiKV brought up by the matching tikv-grpc-stub-channel-broken_test.sh harness.
//
// Multi-node and chaos sub-cases are guarded by env vars and skipped
// unless the harness exports them; see tikv-grpc-stub-channel-broken_test.sh.
//
// The 1M-scale perf gate (mandatory before closure) lives in
// tikv-grpc-stub-channel-broken_perf.sh. Unit repro and perf gate are independent; both
// must pass for the case to be DONE.

#include "inc/Test.h"

#ifdef TIKV
#include "inc/Core/SPANN/ExtraTiKVController.h"
#include "lib/test_hooks.h"
#include "lib/counters_delta.h"

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

using namespace SPTAG;
using namespace SPTAG::SPANN;
using SPTAG::SPANN::test::TiKVIOTestHook;
using SPTAG::SPANN::test::CountersDelta;

namespace {

std::shared_ptr<TiKVIO> MakeIO(const std::string& tag) {
    const char* pd = std::getenv("TIKV_PD_ADDRESSES");
    if (!pd || std::string(pd).empty()) {
        BOOST_TEST_MESSAGE("TIKV_PD_ADDRESSES not set; skipping " + tag);
        return nullptr;
    }
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string prefix = "fault_tikv-grpc-stub-channel-broken_" + tag + "_" + std::to_string(now) + "_";
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

BOOST_AUTO_TEST_SUITE(TikvGrpcStubChannelBrokenTest)

// Smoke: real PD+TiKV reachable, write/read round-trip works.
BOOST_AUTO_TEST_CASE(Baseline) {
    auto io = MakeIO("baseline");
    if (!io) return;
    BOOST_REQUIRE(RawWrite(*io, "k", "v"));
    std::string out;
    BOOST_REQUIRE(RawRead(*io, "k", &out));
    BOOST_CHECK_EQUAL(out, "v");
}

// TODO: replace with the case-specific recovery assertion. Pattern:
//   1. Make the IO and exercise the happy path.
//   2. Snapshot counters with CountersDelta.
//   3. Trigger the fault (via env-provided harness command, in-process call,
//      or test hook).
//   4. Assert recovery: next op succeeds within budget; counters bumped
//      as the spec says they should.
//
// Per the perf-validation protocol the impl-side recovery path lives
// behind an env-var gate so baseline runs see the unmodified hot path:
//
//   if (std::getenv("SPTAG_FAULT_TIKV_GRPC_STUB_CHANNEL_BROKEN") != nullptr) { /* armed */ }
//
// The harness sets SPTAG_FAULT_TIKV_GRPC_STUB_CHANNEL_BROKEN=1 for the "fault" perf tier
// and the unit repro. Baseline tier runs with it unset.
BOOST_AUTO_TEST_CASE(BrokenChannelTriggersRebuild) {
    auto io = MakeIO("broken_channel");
    if (!io) return;

    // Prime: write+read to populate region cache and at least one stub pool.
    BOOST_REQUIRE(RawWrite(*io, "prime", "v0"));
    std::string out;
    BOOST_REQUIRE(RawRead(*io, "prime", &out));
    BOOST_CHECK_EQUAL(out, "v0");

    auto addrs = TiKVIOTestHook::known_store_addresses(*io);
    BOOST_REQUIRE_MESSAGE(!addrs.empty(), "stub pool not populated after prime ops");
    const std::string& store = addrs.front();
    size_t pool_size = TiKVIOTestHook::stub_pool_size_for(*io, store);
    BOOST_CHECK_EQUAL(pool_size, 48u);

    CountersDelta delta(*io);

    // Mark several slots broken; subsequent RPCs that round-robin onto them
    // must lazily rebuild instead of black-holing.
    for (size_t i = 0; i < 8 && i < pool_size; ++i) {
        BOOST_CHECK(TiKVIOTestHook::force_evict_stub_slot(*io, store, i));
    }

    // Drive enough traffic to hit every slot at least once (round-robin
    // over 48 slots; 200 ops gives ~4x coverage).
    int ok = 0;
    for (int i = 0; i < 200; ++i) {
        std::string k = "chan_" + std::to_string(i);
        if (RawWrite(*io, k, "v") && RawRead(*io, k, &out) && out == "v") ++ok;
    }
    BOOST_CHECK_GE(ok, 190);  // tolerate a few transient retries
    delta.expect_ge("evictions", 1);
}

// Sub-case: armed via SPTAG_FAULT_TIKV_GRPC_STUB_CHANNEL_BROKEN=1 (env gate).
// When the env var is set the harness intentionally injects more breakage
// to validate behaviour under load. Off by default so baseline runs are clean.
BOOST_AUTO_TEST_CASE(EnvGatedHeavyEviction) {
    if (!std::getenv("SPTAG_FAULT_TIKV_GRPC_STUB_CHANNEL_BROKEN")) {
        BOOST_TEST_MESSAGE("SPTAG_FAULT_TIKV_GRPC_STUB_CHANNEL_BROKEN not set; skipping");
        return;
    }
    auto io = MakeIO("heavy");
    if (!io) return;
    BOOST_REQUIRE(RawWrite(*io, "prime", "v0"));
    auto addrs = TiKVIOTestHook::known_store_addresses(*io);
    BOOST_REQUIRE(!addrs.empty());
    const std::string& store = addrs.front();
    size_t pool_size = TiKVIOTestHook::stub_pool_size_for(*io, store);

    CountersDelta delta(*io);
    // Break half the pool.
    for (size_t i = 0; i < pool_size / 2; ++i) {
        TiKVIOTestHook::force_evict_stub_slot(*io, store, i);
    }
    int ok = 0;
    std::string out;
    for (int i = 0; i < 400; ++i) {
        std::string k = "heavy_" + std::to_string(i);
        if (RawWrite(*io, k, "v") && RawRead(*io, k, &out) && out == "v") ++ok;
    }
    BOOST_CHECK_GE(ok, 380);
    delta.expect_ge("evictions", static_cast<uint64_t>(pool_size / 4));
}

BOOST_AUTO_TEST_SUITE_END()

#endif  // TIKV
