// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: pd-quorum-loss
//
// Spec:  tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//        pd-quorum-loss.md
// Branch: fault/pd-quorum-loss
//
// Tier 1 repro: exercises the header-only gated wrapper
// `PdQuorumLossGated.h` directly. The wrapper is the contract surface
// the PD client layer will call into; testing it in isolation gives
// deterministic env-off / env-armed dormancy + degraded-mode evidence
// without standing up a real 4-PD cluster.

#include "inc/Test.h"

#ifdef TIKV
#include "inc/Core/SPANN/PdQuorumLossGated.h"

#include <boost/test/unit_test.hpp>

#include <cstdlib>
#include <string>
#include <thread>
#include <chrono>

using namespace SPTAG;
using namespace SPTAG::Fault::PdQuorumLoss;

namespace {

// RAII guard that overrides SPTAG_FAULT_PD_QUORUM_LOSS for the duration
// of a test case so the outer process invocation (env-off vs env-armed)
// does not change which assertions a case exercises.
class EnvGuard
{
public:
    EnvGuard(const char* name, const char* value) : m_name(name)
    {
        const char* prev = std::getenv(name);
        if (prev != nullptr)
        {
            m_hadPrev = true;
            m_prev = prev;
        }
        if (value == nullptr) ::unsetenv(name);
        else                  ::setenv(name, value, 1);
    }

    ~EnvGuard()
    {
        if (m_hadPrev) ::setenv(m_name.c_str(), m_prev.c_str(), 1);
        else           ::unsetenv(m_name.c_str());
    }

private:
    std::string m_name;
    std::string m_prev;
    bool m_hadPrev = false;
};

struct FakeWriteSink
{
    int commits = 0;
    ErrorCode Commit() { ++commits; return ErrorCode::Success; }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(PdQuorumLossTest)

// (1) env-off: wrapper is fully dormant — no counters move, all calls
// pass through with Success regardless of (mocked) PD quorum state.
BOOST_AUTO_TEST_CASE(EnvOffDormancy)
{
    EnvGuard g("SPTAG_FAULT_PD_QUORUM_LOSS", nullptr);
    ResetCountersForTesting();
    BOOST_REQUIRE(!ArmedFromEnv());

    PdState healthy{4, 4};
    PdState quorumLost{2, 4};
    TtlCache cache;
    cache.Put("k", "v", std::chrono::milliseconds(60'000));
    FakeWriteSink sink;

    // Even with mocked quorum loss, env-off must not refuse anything.
    for (const PdState& pd : {healthy, quorumLost})
    {
        BOOST_CHECK(GuardWrite(pd) == ErrorCode::Success);
        sink.Commit();
        std::string out = "untouched";
        BOOST_CHECK(GuardRead(pd, "k", cache, &out) == ErrorCode::Success);
    }

    BOOST_CHECK_EQUAL(sink.commits, 2);
    BOOST_CHECK_EQUAL(CounterPdQuorumLost().load(), 0u);
    BOOST_CHECK_EQUAL(CounterWriteRefused().load(), 0u);
    BOOST_CHECK_EQUAL(CounterReadDegraded().load(), 0u);
    BOOST_CHECK_EQUAL(CounterClientErrorReturned().load(), 0u);
}

// (2) env-armed but quorum healthy: wrapper does not inject; counters
// stay at zero; reads/writes proceed authoritatively.
BOOST_AUTO_TEST_CASE(BaselineQuorumOk)
{
    EnvGuard g("SPTAG_FAULT_PD_QUORUM_LOSS", "1");
    ResetCountersForTesting();
    BOOST_REQUIRE(ArmedFromEnv());

    PdState pd{4, 4};
    TtlCache cache;
    FakeWriteSink sink;

    BOOST_CHECK(GuardWrite(pd) == ErrorCode::Success);
    sink.Commit();
    std::string out;
    BOOST_CHECK(GuardRead(pd, "k", cache, &out) == ErrorCode::Success);

    BOOST_CHECK_EQUAL(sink.commits, 1);
    BOOST_CHECK_EQUAL(CounterPdQuorumLost().load(), 0u);
    BOOST_CHECK_EQUAL(CounterWriteRefused().load(), 0u);
    BOOST_CHECK_EQUAL(CounterReadDegraded().load(), 0u);
    BOOST_CHECK_EQUAL(CounterClientErrorReturned().load(), 0u);
}

// (3) env-armed + quorum lost (2/4 live): write must be refused with
// ErrorCode::PdUnavailable; commit must NOT happen; counters bumped.
BOOST_AUTO_TEST_CASE(QuorumLostWriteRefused)
{
    EnvGuard g("SPTAG_FAULT_PD_QUORUM_LOSS", "1");
    ResetCountersForTesting();

    PdState pd{2, 4};
    BOOST_REQUIRE(!pd.QuorumOk());

    FakeWriteSink sink;
    ErrorCode rc = GuardWrite(pd);
    BOOST_REQUIRE(rc == ErrorCode::PdUnavailable);
    if (rc == ErrorCode::Success) sink.Commit();   // contract: caller
                                                   // skips commit on
                                                   // refusal

    // No silent commit observable.
    BOOST_CHECK_EQUAL(sink.commits, 0);
    BOOST_CHECK_GE(CounterPdQuorumLost().load(), 1u);
    BOOST_CHECK_GE(CounterWriteRefused().load(), 1u);
    BOOST_CHECK_GE(CounterClientErrorReturned().load(), 1u);
    BOOST_CHECK_EQUAL(CounterReadDegraded().load(), 0u);

    // RetryBudget integration: an exhausted budget short-circuits to
    // RetryBudgetExceeded so the caller can surface bounded retries.
    Helper::RetryBudget::Config cfg;
    cfg.max_attempts = 1;
    cfg.total_wall = std::chrono::milliseconds(0);
    Helper::RetryBudget budget(cfg);
    budget.record_attempt();
    BOOST_REQUIRE(budget.exhausted());
    BOOST_CHECK(GuardWrite(pd, &budget) == ErrorCode::RetryBudgetExceeded);
}

// (4) env-armed + quorum lost: read served from cache iff TTL valid,
// else PdUnavailable. Two phases:
//   (a) valid cache hit -> Success + m_readDegraded bumped.
//   (b) expired cache   -> PdUnavailable + m_clientErrorReturned bumped.
BOOST_AUTO_TEST_CASE(QuorumLostReadFromCache)
{
    EnvGuard g("SPTAG_FAULT_PD_QUORUM_LOSS", "1");
    ResetCountersForTesting();

    PdState pd{2, 4};
    TtlCache cache;
    cache.Put("hot", "cached-value", std::chrono::milliseconds(60'000));
    cache.Put("cold", "stale", std::chrono::milliseconds(1));

    // (a) fresh entry -> degraded read serves from cache.
    std::string out = "untouched";
    BOOST_CHECK(GuardRead(pd, "hot", cache, &out) == ErrorCode::Success);
    BOOST_CHECK_EQUAL(out, "cached-value");
    BOOST_CHECK_GE(CounterReadDegraded().load(), 1u);
    std::uint64_t errAfterHit = CounterClientErrorReturned().load();

    // (b) wait for the cold entry to expire, then read.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    out = "untouched";
    ErrorCode rc = GuardRead(pd, "cold", cache, &out);
    BOOST_CHECK(rc == ErrorCode::PdUnavailable);
    BOOST_CHECK_EQUAL(out, "untouched");
    BOOST_CHECK_GT(CounterClientErrorReturned().load(), errAfterHit);

    // (c) unknown key -> also PdUnavailable.
    out = "untouched";
    rc = GuardRead(pd, "unknown", cache, &out);
    BOOST_CHECK(rc == ErrorCode::PdUnavailable);
    BOOST_CHECK_EQUAL(out, "untouched");

    BOOST_CHECK_GE(CounterPdQuorumLost().load(), 3u);
    BOOST_CHECK_EQUAL(CounterWriteRefused().load(), 0u);
}

// (5) Toggle injection on/off within one process. Verifies counters
// reset cleanly and no leaked state crosses the toggle boundary.
BOOST_AUTO_TEST_CASE(HarnessSmoke)
{
    PdState quorumLost{1, 4};
    PdState healthy{4, 4};

    // Phase A: armed, quorum lost -> refusal.
    {
        EnvGuard g("SPTAG_FAULT_PD_QUORUM_LOSS", "1");
        ResetCountersForTesting();
        BOOST_REQUIRE(GuardWrite(quorumLost) == ErrorCode::PdUnavailable);
        BOOST_REQUIRE_GE(CounterWriteRefused().load(), 1u);
    }
    // Phase B: env unset -> dormant; previously-bumped counters are not
    // consulted by the wrapper.
    {
        EnvGuard g("SPTAG_FAULT_PD_QUORUM_LOSS", nullptr);
        ResetCountersForTesting();
        BOOST_REQUIRE(!ArmedFromEnv());
        BOOST_CHECK(GuardWrite(quorumLost) == ErrorCode::Success);
        BOOST_CHECK(GuardWrite(healthy) == ErrorCode::Success);
        BOOST_CHECK_EQUAL(CounterWriteRefused().load(), 0u);
        BOOST_CHECK_EQUAL(CounterPdQuorumLost().load(), 0u);
    }
    // Phase C: re-arm, quorum healthy -> still no injection.
    {
        EnvGuard g("SPTAG_FAULT_PD_QUORUM_LOSS", "1");
        ResetCountersForTesting();
        BOOST_CHECK(GuardWrite(healthy) == ErrorCode::Success);
        BOOST_CHECK_EQUAL(CounterWriteRefused().load(), 0u);
    }
}

BOOST_AUTO_TEST_SUITE_END()

#endif  // TIKV
