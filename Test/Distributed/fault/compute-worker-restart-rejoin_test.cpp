// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: compute-worker-restart-rejoin
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/compute-worker-restart-rejoin.md
// Branch: fault/compute-worker-restart-rejoin
//
// Each fault case test file follows the pilot pattern from
// pd-store-discovery-stale: a few small Boost.Test cases that exercise
// the *unit-level* recovery contract against a real (single-node)
// PD + TiKV brought up by the matching compute-worker-restart-rejoin_test.sh harness.
//
// Multi-node and chaos sub-cases are guarded by env vars and skipped
// unless the harness exports them; see compute-worker-restart-rejoin_test.sh.
//
// The 1M-scale perf gate (mandatory before closure) lives in
// compute-worker-restart-rejoin_perf.sh. Unit repro and perf gate are independent; both
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
    std::string prefix = "fault_compute-worker-restart-rejoin_" + tag + "_" + std::to_string(now) + "_";
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

BOOST_AUTO_TEST_SUITE(ComputeWorkerRestartRejoinTest)

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
//   if (std::getenv("SPTAG_FAULT_COMPUTE_WORKER_RESTART_REJOIN") != nullptr) { /* armed */ }
//
// The harness sets SPTAG_FAULT_COMPUTE_WORKER_RESTART_REJOIN=1 for the "fault" perf tier
// and the unit repro. Baseline tier runs with it unset.
BOOST_AUTO_TEST_CASE(RecoveryContract) {
    auto io = MakeIO("recovery");
    if (!io) return;

    BOOST_REQUIRE(RawWrite(*io, "k", "v"));
    CountersDelta delta(*io);

    // ── trigger fault here (TODO) ──

    std::string out;
    BOOST_CHECK(RawRead(*io, "k", &out));
    // delta.expect_ge("invalidations", 1);   // example
    // delta.expect_eq("mismatches", 0);      // example
}

BOOST_AUTO_TEST_SUITE_END()

#endif  // TIKV
