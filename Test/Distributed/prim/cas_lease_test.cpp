// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// CasLease primitive unit tests.
//
// Covers:
//   - Acquire success on empty key.
//   - Acquire conflict against live lease.
//   - Heartbeat extends TTL.
//   - TTL loss: lease becomes acquirable by another owner after expiry,
//     prior holder's heartbeat returns Lost.
//   - Fencing token monotonicity across re-acquires.
//   - Validate distinguishes current vs stale fencing tokens.

#include "inc/Core/SPANN/Distributed/CasLease.h"
#include "inc/Test.h"

#include <chrono>
#include <thread>

using namespace SPTAG::SPANN::Distributed;

namespace {

std::shared_ptr<CasLease> MakeLease() {
    auto backend = std::make_shared<InMemoryKvBackend>();
    return std::make_shared<CasLease>(backend);
}

constexpr auto kLongTtl  = std::chrono::milliseconds(2000);
constexpr auto kShortTtl = std::chrono::milliseconds(50);

}  // namespace

BOOST_AUTO_TEST_SUITE(CasLeaseTest)

BOOST_AUTO_TEST_CASE(AcquireGrantsLeaseOnEmptyKey)
{
    auto lease = MakeLease();
    auto r = lease->Acquire("head/42", "node-A", kLongTtl);
    BOOST_CHECK(r.status == AcquireStatus::Granted);
    BOOST_CHECK(r.token.valid());
    BOOST_CHECK_EQUAL(r.token.fencingToken, 1u);
    BOOST_CHECK_EQUAL(r.token.owner, "node-A");
}

BOOST_AUTO_TEST_CASE(AcquireConflictsWithLiveLease)
{
    auto lease = MakeLease();
    auto r1 = lease->Acquire("head/42", "node-A", kLongTtl);
    BOOST_REQUIRE(r1.status == AcquireStatus::Granted);

    auto r2 = lease->Acquire("head/42", "node-B", kLongTtl);
    BOOST_CHECK(r2.status == AcquireStatus::Conflict);
    BOOST_CHECK_EQUAL(r2.token.fencingToken, r1.token.fencingToken);
}

BOOST_AUTO_TEST_CASE(HeartbeatExtendsTtl)
{
    auto lease = MakeLease();
    auto r = lease->Acquire("head/42", "node-A", kShortTtl);
    BOOST_REQUIRE(r.status == AcquireStatus::Granted);

    auto firstExpiry = r.token.expiresAt;
    // Sleep past initial TTL but heartbeat first to extend.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto hb = lease->Heartbeat(r.token, kShortTtl);
    BOOST_CHECK(hb == HeartbeatStatus::Renewed);
    BOOST_CHECK(r.token.expiresAt > firstExpiry);

    // After heartbeat, sleep past the *original* deadline; since we
    // extended, validate should still be Current.
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    BOOST_CHECK(lease->Validate(r.token, r.token.fencingToken)
                == ValidateStatus::Current);
}

BOOST_AUTO_TEST_CASE(TtlLossLeasesBecomeStealable)
{
    auto lease = MakeLease();
    auto r1 = lease->Acquire("head/42", "node-A", kShortTtl);
    BOOST_REQUIRE(r1.status == AcquireStatus::Granted);

    // Wait past TTL without heartbeat.
    std::this_thread::sleep_for(kShortTtl + std::chrono::milliseconds(20));

    // Old holder's heartbeat should now report Lost (lease still in
    // store, but if a stealer wins next CAS, old leaseId mismatches).
    auto r2 = lease->Acquire("head/42", "node-B", kLongTtl);
    BOOST_CHECK(r2.status == AcquireStatus::Granted);
    BOOST_CHECK_GT(r2.token.fencingToken, r1.token.fencingToken);

    auto hb = lease->Heartbeat(r1.token, kShortTtl);
    BOOST_CHECK(hb == HeartbeatStatus::Lost);
}

BOOST_AUTO_TEST_CASE(FencingTokenIsMonotonicAcrossReAcquires)
{
    auto lease = MakeLease();
    uint64_t prev = 0;
    for (int i = 0; i < 4; ++i) {
        auto r = lease->Acquire("head/77", "node-A", kShortTtl);
        BOOST_REQUIRE(r.status == AcquireStatus::Granted);
        BOOST_CHECK_GT(r.token.fencingToken, prev);
        prev = r.token.fencingToken;
        // Force expiry so next Acquire sees stale record.
        std::this_thread::sleep_for(kShortTtl + std::chrono::milliseconds(10));
    }
}

BOOST_AUTO_TEST_CASE(ValidateStaleVsCurrent)
{
    auto lease = MakeLease();
    auto r1 = lease->Acquire("head/9", "node-A", kShortTtl);
    BOOST_REQUIRE(r1.status == AcquireStatus::Granted);

    // Current.
    BOOST_CHECK(lease->Validate(r1.token, r1.token.fencingToken)
                == ValidateStatus::Current);

    // FenceStale: caller observed a different fencing token under the
    // same lease. (e.g. stale survivor write attempting to commit.)
    BOOST_CHECK(lease->Validate(r1.token, r1.token.fencingToken + 999)
                == ValidateStatus::FenceStale);

    // Expire and have node-B steal. Now r1 must validate as
    // FenceStale or Expired.
    std::this_thread::sleep_for(kShortTtl + std::chrono::milliseconds(10));
    auto r2 = lease->Acquire("head/9", "node-B", kLongTtl);
    BOOST_REQUIRE(r2.status == AcquireStatus::Granted);

    auto v = lease->Validate(r1.token, r1.token.fencingToken);
    BOOST_CHECK(v == ValidateStatus::FenceStale || v == ValidateStatus::Expired);

    // r2 still current.
    BOOST_CHECK(lease->Validate(r2.token, r2.token.fencingToken)
                == ValidateStatus::Current);
}

BOOST_AUTO_TEST_CASE(ReleaseIsIdempotent)
{
    auto lease = MakeLease();
    auto r = lease->Acquire("head/3", "node-A", kLongTtl);
    BOOST_REQUIRE(r.status == AcquireStatus::Granted);

    auto rel1 = lease->Release(r.token);
    BOOST_CHECK(rel1 == ReleaseStatus::Released);
    auto rel2 = lease->Release(r.token);
    BOOST_CHECK(rel2 == ReleaseStatus::NotFound);
}

BOOST_AUTO_TEST_SUITE_END()
