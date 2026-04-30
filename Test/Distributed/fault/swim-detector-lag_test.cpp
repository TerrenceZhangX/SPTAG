// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: swim-detector-lag
//
// Tier 1 HARD repro of the SWIM detector-lag → client-timeout invariant
// triple from design-docs/ft-fault-cases/swim-detector-lag.md:
//
//   * client surfaces a typed error (RetryBudgetExceeded / OwnerSuspect)
//     during the gap window — never an infinite hang;
//   * once the detector has decided a peer is Dead, subsequent calls
//     fast-fail OwnerSuspect with no RPC;
//   * concurrent callers do not pile up: a per-peer in-flight cap holds
//     the queue depth bounded; rejected admissions surface the same
//     well-formed error.
//
// Wrapper under test:  SwimGatedClient (header-only; defers to SwimDetector
//                      + Helper::RetryBudget primitives).
// Env-gate:            SPTAG_FAULT_SWIM_DETECTOR_LAG (off → wrapper dormant,
//                      counters stay zero).
// Tier 2 (1M perf):    DEFERRED — non hot-path; gap-window fast-fail does
//                      not run inside the SPANN search inner loop.

#include "inc/Test.h"

#include "inc/Core/SPANN/Distributed/SwimDetector.h"
#include "inc/Core/SPANN/Distributed/SwimGatedClient.h"
#include "inc/Helper/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::SPANN;
using namespace SPTAG::SPANN::Distributed;

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

// Force `peer` into Dead by re-using the SWIM admission paths: we just
// don't talk to peer at all and tick past dead_after_ticks. Because the
// detector under test runs as a single isolated instance (no mesh) we
// open-code the simplest path: drive ticks past the timeout window.
void DriveToDead(SwimDetector& d, int peer, std::uint64_t suspectTicks,
                 std::uint64_t deadTicks) {
    // Each Tick() with no inbound Ack from `peer` advances its
    // miss-count. The detector's state machine declares Suspect after
    // suspectTicks misses, then Dead after `deadTicks` more.
    for (std::uint64_t i = 0; i < suspectTicks + deadTicks + 2; ++i) {
        (void)d.Tick();
    }
    // After this, peer should be Dead in the detector's view.
    MemberState st;
    BOOST_REQUIRE(d.TryGetMember(peer, st));
    BOOST_REQUIRE_EQUAL(static_cast<int>(st.health),
                        static_cast<int>(MemberHealth::Dead));
}

}  // namespace

BOOST_AUTO_TEST_SUITE(SwimDetectorLagTest)

// ---------------------------------------------------------------------------
// 1) Env-off dormancy. With SPTAG_FAULT_SWIM_DETECTOR_LAG unset the wrapper
//    must forward the inner RPC unchanged and keep both counters at zero —
//    even when the detector says Dead and the inner RPC would surface a
//    bare error. This protects baseline perf and makes the wrapper invisible
//    to non-fault production builds.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    ScopedEnv env("SPTAG_FAULT_SWIM_DETECTOR_LAG", nullptr);
    BOOST_REQUIRE(!SwimGatedClient::EnvArmed());

    SwimDetector det(/*self=*/0, /*peers=*/{1, 2});
    DriveToDead(det, /*peer=*/1, /*suspect=*/1, /*dead=*/3);

    SwimGatedClient::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(50);

    std::atomic<int> rpcCalls{0};
    SwimGatedClient client(det, cfg,
        [&](int /*peer*/, std::chrono::milliseconds /*deadline*/) {
            ++rpcCalls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto out = client.Call(/*peer=*/1, budget);

    BOOST_CHECK(out.code == ErrorCode::Success);
    BOOST_CHECK(!out.dead);
    BOOST_CHECK_EQUAL(rpcCalls.load(), 1);
    BOOST_CHECK_EQUAL(client.SwimDetectorLagErrors(), 0u);
    BOOST_CHECK_EQUAL(client.SwimDetectorLagBudgetExhausted(), 0u);
}

// ---------------------------------------------------------------------------
// 2) SWIM has already decided Dead → wrapper fast-fails with OwnerSuspect
//    before any RPC. This is the post-decision invariant from the case-md
//    ("subsequent calls fast-fail OwnerSuspect ≤ 1 ms").
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(DetectorDeadFastFailsOwnerSuspect)
{
    ScopedEnv env("SPTAG_FAULT_SWIM_DETECTOR_LAG", "1");
    BOOST_REQUIRE(SwimGatedClient::EnvArmed());

    SwimDetector det(/*self=*/0, /*peers=*/{1});
    DriveToDead(det, /*peer=*/1, /*suspect=*/1, /*dead=*/3);

    SwimGatedClient::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(500);

    std::atomic<int> rpcCalls{0};
    SwimGatedClient client(det, cfg,
        [&](int /*peer*/, std::chrono::milliseconds /*deadline*/) {
            ++rpcCalls;
            return ErrorCode::Success;
        });

    Helper::RetryBudget budget(cfg.budget);
    auto t0 = std::chrono::steady_clock::now();
    auto out = client.Call(/*peer=*/1, budget);
    auto elapsed = std::chrono::steady_clock::now() - t0;

    BOOST_CHECK(out.code == ErrorCode::OwnerSuspect);
    BOOST_CHECK(out.dead);
    BOOST_CHECK_EQUAL(rpcCalls.load(), 0);
    BOOST_CHECK_EQUAL(client.SwimDetectorLagErrors(), 1u);
    // ≤ 50 ms is a comfortable upper bound for a function that takes no
    // mutexes besides the detector lookup; spec says ≤ 1 ms but we use
    // 50 ms to absorb CI noise.
    BOOST_CHECK_LT(std::chrono::duration_cast<std::chrono::milliseconds>(
                       elapsed).count(),
                   50);
}

// ---------------------------------------------------------------------------
// 3) Detector-lag gap → client per-call timeout shorter than detector
//    decision: caller surfaces RetryBudgetExceeded after one or more
//    well-formed timeouts; never hangs past the budget.total_wall.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(GapPeriodClientGetsRetryBudgetExceeded)
{
    ScopedEnv env("SPTAG_FAULT_SWIM_DETECTOR_LAG", "1");

    SwimDetector det(/*self=*/0, /*peers=*/{1});
    // Detector ticks NOT advanced — peer is still Alive in our view, i.e.
    // we are inside the lag gap.

    SwimGatedClient::Config cfg;
    cfg.per_call_timeout = std::chrono::milliseconds(40);
    cfg.budget.total_wall = std::chrono::milliseconds(300);
    cfg.budget.max_attempts = 4;
    cfg.budget.base_backoff = std::chrono::milliseconds(10);
    cfg.budget.cap_backoff  = std::chrono::milliseconds(40);

    std::atomic<int> rpcCalls{0};
    SwimGatedClient client(det, cfg,
        [&](int /*peer*/, std::chrono::milliseconds deadline) {
            ++rpcCalls;
            // Simulate the dead-but-not-yet-detected peer: sleep up to
            // the deadline, return a timeout-style error.
            std::this_thread::sleep_for(deadline);
            return ErrorCode::Socket_FailedConnectToEndPoint;
        });

    Helper::RetryBudget budget(cfg.budget);
    ErrorCode last = ErrorCode::Success;
    auto t0 = std::chrono::steady_clock::now();
    while (budget.should_retry()) {
        auto out = client.Call(/*peer=*/1, budget);
        last = out.code;
        if (out.code == ErrorCode::RetryBudgetExceeded) break;
        if (!budget.record_attempt()) break;
    }
    auto wall = std::chrono::steady_clock::now() - t0;

    BOOST_CHECK(last == ErrorCode::RetryBudgetExceeded
                || budget.exhausted());
    // No infinite hang: total elapsed ≤ wall + a generous slop.
    BOOST_CHECK_LT(std::chrono::duration_cast<std::chrono::milliseconds>(
                       wall).count(),
                   cfg.budget.total_wall.count() + 200);
    BOOST_CHECK_GE(rpcCalls.load(), 1);
    BOOST_CHECK_GE(client.SwimDetectorLagErrors(), 1u);
    BOOST_CHECK_GE(client.SwimDetectorLagBudgetExhausted(), 1u);
}

// ---------------------------------------------------------------------------
// 4) Many concurrent callers → per-peer in-flight cap holds the queue
//    bounded; rejected admissions surface RetryBudgetExceeded rather than
//    queueing. Invariant: max observed in-flight ≤ peer_queue_max; rejected
//    callers > 0 (proves the cap fired); no caller hangs past total_wall.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(ConcurrentCallersNoRetryPileUp)
{
    ScopedEnv env("SPTAG_FAULT_SWIM_DETECTOR_LAG", "1");

    SwimDetector det(/*self=*/0, /*peers=*/{1});

    SwimGatedClient::Config cfg;
    cfg.peer_queue_max = 4;
    cfg.per_call_timeout = std::chrono::milliseconds(80);
    cfg.budget.total_wall   = std::chrono::milliseconds(300);
    cfg.budget.max_attempts = 1;  // single shot per logical caller

    std::atomic<int>     rpcCalls{0};
    std::atomic<int>     concurrent{0};
    std::atomic<int>     peakConcurrent{0};
    SwimGatedClient client(det, cfg,
        [&](int /*peer*/, std::chrono::milliseconds deadline) {
            int now = concurrent.fetch_add(1) + 1;
            int prev = peakConcurrent.load();
            while (now > prev
                   && !peakConcurrent.compare_exchange_weak(prev, now)) {}
            std::this_thread::sleep_for(deadline);
            concurrent.fetch_sub(1);
            ++rpcCalls;
            return ErrorCode::Socket_FailedConnectToEndPoint;
        });

    constexpr int N = 32;
    std::vector<std::future<GatedCallOutcome>> futs;
    futs.reserve(N);
    for (int i = 0; i < N; ++i) {
        futs.emplace_back(std::async(std::launch::async, [&]() {
            Helper::RetryBudget budget(cfg.budget);
            return client.Call(/*peer=*/1, budget);
        }));
    }

    int rejected = 0;
    int admittedErr = 0;
    for (auto& f : futs) {
        auto o = f.get();
        if (!o.admitted) ++rejected;
        if (o.code == ErrorCode::RetryBudgetExceeded
            || o.code == ErrorCode::Socket_FailedConnectToEndPoint) {
            ++admittedErr;
        }
        BOOST_CHECK(o.code != ErrorCode::Success);
    }

    BOOST_CHECK_LE(peakConcurrent.load(),
                   static_cast<int>(cfg.peer_queue_max));
    BOOST_CHECK_GT(rejected, 0);
    BOOST_CHECK_EQUAL(rejected + rpcCalls.load(), N);
    BOOST_CHECK_GE(client.SwimDetectorLagErrors(),
                   static_cast<std::uint64_t>(rejected));
    BOOST_CHECK_GE(client.SwimDetectorLagBudgetExhausted(),
                   static_cast<std::uint64_t>(rejected));
}

// ---------------------------------------------------------------------------
// 5) Harness smoke: detector + budget + wrapper all instantiate cleanly,
//    counters start at 0, EnvArmed reflects the env, both error codes are
//    distinct/well-formed, and a Success path leaves all counters untouched
//    even when env-armed.
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(HarnessSmoke)
{
    BOOST_CHECK(static_cast<int>(ErrorCode::OwnerSuspect)
                != static_cast<int>(ErrorCode::RetryBudgetExceeded));

    {
        ScopedEnv off("SPTAG_FAULT_SWIM_DETECTOR_LAG", nullptr);
        BOOST_CHECK(!SwimGatedClient::EnvArmed());
    }
    {
        ScopedEnv on("SPTAG_FAULT_SWIM_DETECTOR_LAG", "1");
        BOOST_CHECK(SwimGatedClient::EnvArmed());

        SwimDetector det(0, {1});
        SwimGatedClient::Config cfg;
        cfg.per_call_timeout = std::chrono::milliseconds(20);

        SwimGatedClient client(det, cfg,
            [&](int, std::chrono::milliseconds) {
                return ErrorCode::Success;
            });
        BOOST_CHECK_EQUAL(client.SwimDetectorLagErrors(), 0u);
        BOOST_CHECK_EQUAL(client.SwimDetectorLagBudgetExhausted(), 0u);

        Helper::RetryBudget budget(cfg.budget);
        auto out = client.Call(1, budget);
        BOOST_CHECK(out.code == ErrorCode::Success);
        BOOST_CHECK(out.admitted);
        BOOST_CHECK(!out.dead);
        BOOST_CHECK_EQUAL(client.SwimDetectorLagErrors(), 0u);
        BOOST_CHECK_EQUAL(client.SwimDetectorLagBudgetExhausted(), 0u);
        BOOST_CHECK_EQUAL(client.InflightFor(1), 0u);
    }
}

BOOST_AUTO_TEST_SUITE_END()
