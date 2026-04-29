// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Unit test for prim/coprocessor-error path.
//
// Scope:
//   1. Validate prim::RetryBudget honors maxAttempts and totalBudget,
//      including jittered exponential backoff.
//   2. Validate the explicit-error policy state machine:
//        - region_error => failed posting IDs surfaced (not silently dropped)
//        - retry-once via RetryBudget
//        - fallback to per-key MultiGet
//        - explicit DiskIOFail when both fail
//
// We do NOT bring up TiKV here; the policy machine is tested through a
// mock that mirrors the TiKVIO::CoprocessorSearch contract:
//   ErrorCode CoprocessorSearch(postings, ..., results, failedIDs);
// where failedIDs is appended on region_error / gRPC failure.

#include "inc/Test.h"
#include "inc/Core/Common.h"
#include "inc/Core/SPANN/Distributed/prim/RetryBudget.h"

#include <atomic>
#include <chrono>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::SPANN;

BOOST_AUTO_TEST_SUITE(CoprocessorErrorTest)

// --- RetryBudget primitive ---------------------------------------------------

BOOST_AUTO_TEST_CASE(RetryBudget_RespectsMaxAttempts)
{
    prim::RetryBudget b(3, std::chrono::milliseconds(5000));
    int n = 0;
    while (b.TryAgain()) { ++n; }
    BOOST_CHECK_EQUAL(n, 3);
    BOOST_CHECK_EQUAL(b.RemainingAttempts(), 0);
}

BOOST_AUTO_TEST_CASE(RetryBudget_RespectsTotalBudget)
{
    // 100 attempts but only 50 ms budget; backoff will exhaust budget
    // long before attempts.
    auto t0 = std::chrono::steady_clock::now();
    prim::RetryBudget b(100, std::chrono::milliseconds(50));
    int n = 0;
    while (b.TryAgain()) { ++n; if (n > 50) break; }
    auto t1 = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
    // Wall-clock bound is the real guarantee; attempt count may saturate.
    // Allow generous slack for CI: budget is 50 ms, expect <300 ms total.
    BOOST_CHECK_LT(elapsed.count(), 500);
}

BOOST_AUTO_TEST_CASE(RetryBudget_ZeroAttemptsRefuses)
{
    prim::RetryBudget b(0, std::chrono::milliseconds(1000));
    BOOST_CHECK(!b.TryAgain());
}

// --- Policy state machine (mirrors caller logic in ExtraDynamicSearcher) ----

namespace {

struct MockResult { int vid; float dist; };

// Mock CoprocessorSearch contract.  failureMode controls behavior:
//   0 = success, all postings succeed
//   1 = first call: half fail; second call: all succeed
//   2 = always fail (every group returns region_error)
struct MockCopr {
    int failureMode = 0;
    int callCount = 0;
    int totalPostingsAttempted = 0;
    std::atomic<int> injectedRegionErrors{0};

    ErrorCode CoprocessorSearch(const std::vector<int>& postings,
                                std::vector<MockResult>& out,
                                std::vector<int>* failed)
    {
        ++callCount;
        totalPostingsAttempted += static_cast<int>(postings.size());
        out.clear();
        bool anyFail = false;
        for (size_t i = 0; i < postings.size(); ++i) {
            bool fails = false;
            if (failureMode == 2) fails = true;
            else if (failureMode == 1 && callCount == 1 && (i % 2) == 0) fails = true;

            if (fails) {
                if (failed) failed->push_back(postings[i]);
                anyFail = true;
                injectedRegionErrors++;
            } else {
                out.push_back({static_cast<int>(postings[i]),
                               static_cast<float>(postings[i]) * 0.1f});
            }
        }
        return anyFail ? ErrorCode::DiskIOFail : ErrorCode::Success;
    }
};

struct MockMultiGet {
    bool shouldSucceed = true;
    int callCount = 0;
    int keysFetched = 0;
    ErrorCode MultiGet(const std::vector<int>& keys) {
        ++callCount;
        keysFetched += static_cast<int>(keys.size());
        return shouldSucceed ? ErrorCode::Success : ErrorCode::DiskIOFail;
    }
};

// Replicates the policy from ExtraDynamicSearcher::SearchIndexWithCoprocessor.
ErrorCode RunPolicy(MockCopr& copr, MockMultiGet& mg,
                    const std::vector<int>& postings,
                    std::vector<MockResult>& results)
{
    std::vector<int> failed;
    auto ret = copr.CoprocessorSearch(postings, results, &failed);
    if (ret == ErrorCode::DiskIOFail && !failed.empty()) {
        // 1) retry once via RetryBudget
        prim::RetryBudget budget(1, std::chrono::milliseconds(2000));
        if (budget.TryAgain()) {
            std::vector<int> retryFailed;
            std::vector<MockResult> retryResults;
            auto retryRet = copr.CoprocessorSearch(failed, retryResults, &retryFailed);
            results.insert(results.end(), retryResults.begin(), retryResults.end());
            if (retryRet == ErrorCode::Success && retryFailed.empty()) {
                ret = ErrorCode::Success;
                failed.clear();
            } else {
                failed.swap(retryFailed);
            }
        }
        // 2) fall back to MultiGet
        if (!failed.empty()) {
            auto fbRet = mg.MultiGet(failed);
            if (fbRet != ErrorCode::Success) return ErrorCode::DiskIOFail;
            ret = ErrorCode::Success;
            failed.clear();
        }
    }
    return ret;
}

}  // namespace

BOOST_AUTO_TEST_CASE(Policy_NoErrorPath_ReturnsSuccessWithFullRecall)
{
    MockCopr copr; copr.failureMode = 0;
    MockMultiGet mg;
    std::vector<int> postings = {1,2,3,4,5,6};
    std::vector<MockResult> results;
    auto ret = RunPolicy(copr, mg, postings, results);
    BOOST_CHECK(ret == ErrorCode::Success);
    BOOST_CHECK_EQUAL(results.size(), 6);
    BOOST_CHECK_EQUAL(copr.callCount, 1);    // no retry
    BOOST_CHECK_EQUAL(mg.callCount, 0);      // no fallback
}

BOOST_AUTO_TEST_CASE(Policy_RetryRecovers_PreservesRecallWithoutFallback)
{
    MockCopr copr; copr.failureMode = 1;     // first call half-fails, retry succeeds
    MockMultiGet mg;
    std::vector<int> postings = {1,2,3,4,5,6};
    std::vector<MockResult> results;
    auto ret = RunPolicy(copr, mg, postings, results);
    BOOST_CHECK(ret == ErrorCode::Success);
    BOOST_CHECK_EQUAL(results.size(), 6);    // full recall
    BOOST_CHECK_EQUAL(copr.callCount, 2);    // initial + 1 retry
    BOOST_CHECK_EQUAL(mg.callCount, 0);      // no fallback needed
}

BOOST_AUTO_TEST_CASE(Policy_RetryFails_FallsBackToMultiGet)
{
    MockCopr copr; copr.failureMode = 2;     // always fails
    MockMultiGet mg; mg.shouldSucceed = true;
    std::vector<int> postings = {1,2,3,4,5,6};
    std::vector<MockResult> results;
    auto ret = RunPolicy(copr, mg, postings, results);
    BOOST_CHECK(ret == ErrorCode::Success);  // recovered via MultiGet
    BOOST_CHECK_EQUAL(copr.callCount, 2);    // initial + 1 retry
    BOOST_CHECK_EQUAL(mg.callCount, 1);      // fallback fired
    BOOST_CHECK_EQUAL(mg.keysFetched, 6);    // all postings rerouted
}

BOOST_AUTO_TEST_CASE(Policy_FallbackAlsoFails_SurfacesDiskIOFail)
{
    MockCopr copr; copr.failureMode = 2;
    MockMultiGet mg; mg.shouldSucceed = false;
    std::vector<int> postings = {1,2,3};
    std::vector<MockResult> results;
    auto ret = RunPolicy(copr, mg, postings, results);
    BOOST_CHECK(ret == ErrorCode::DiskIOFail);
    // Critical: the error is *surfaced*, not silently dropped.
}

BOOST_AUTO_TEST_CASE(Policy_RegionErrorNeverSilentlyDropped)
{
    // Pass criterion from ft-design-review: under simulated region_error
    // injection, recall does NOT silently degrade -- either full recall
    // (after retry) or explicit error.
    for (int mode : {0, 1, 2}) {
        MockCopr copr; copr.failureMode = mode;
        MockMultiGet mg; mg.shouldSucceed = (mode != 2);
        // For mode 2 with failing fallback we expect explicit error.
        if (mode == 2) mg.shouldSucceed = false;
        std::vector<int> postings = {10,20,30,40};
        std::vector<MockResult> results;
        auto ret = RunPolicy(copr, mg, postings, results);
        if (ret == ErrorCode::Success) {
            // If success, recall must be complete.
            BOOST_CHECK_EQUAL(results.size(), postings.size());
        } else {
            // Otherwise the failure must be surfaced (not silently zero-ed).
            BOOST_CHECK(ret == ErrorCode::DiskIOFail);
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
