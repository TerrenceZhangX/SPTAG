// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// CasLease.h — TTL lease + fencing-token primitive for cross-node head
// locks (split/merge). Solves the "remote lock with no TTL" failure mode
// described in
//   tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//     split-merge-cross-node-lock-loss.md
//
// Semantics:
//   * Acquire returns a LeaseToken{leaseId, fencingToken, expiresAt}.
//     The fencing token is monotonically increasing per leaseKey across
//     successive lease grants — stale writes from an expired prior
//     lease holder are rejected by Validate().
//   * Heartbeat extends expiresAt by ttl (only if the token still owns
//     the lease — otherwise returns Lost).
//   * Release is CAS-delete on (leaseKey, fencingToken); idempotent.
//   * Validate(token, currentFencingToken) lets the survivor write
//     refuse to commit when the current owner has changed.
//
// Backend: this header ships an in-memory KvBackend implementation
// suitable for tests and single-process simulation. The on-wire TiKV
// binding is deferred — `KvBackend` is the injection point. A real
// TiKV implementation must back Acquire/Heartbeat/Release with TiKV
// transactional CAS (`txn->prewrite` with version check) on a key like
// `lease/<leaseKey>` carrying serialized {leaseId, fencingToken,
// expiresAtMs, owner}. See the deferred-impl note at bottom.
//
// Thread-safety: all public methods are safe under concurrent callers.

#ifndef _SPTAG_SPANN_DISTRIBUTED_CASLEASE_H_
#define _SPTAG_SPANN_DISTRIBUTED_CASLEASE_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace SPTAG {
namespace SPANN {
namespace Distributed {

using SteadyClock = std::chrono::steady_clock;
using TimePoint   = SteadyClock::time_point;

// ---------------------------------------------------------------------
// LeaseToken
// ---------------------------------------------------------------------
struct LeaseToken {
    std::string leaseKey;          // logical resource id (e.g. headID)
    std::string owner;             // requester identity (node id, etc.)
    uint64_t    leaseId      = 0;  // unique per Acquire
    uint64_t    fencingToken = 0;  // monotonic per leaseKey
    TimePoint   expiresAt{};

    bool valid() const { return leaseId != 0; }
};

// Operation results.
enum class AcquireStatus  : uint8_t { Granted, Conflict, Error };
enum class HeartbeatStatus: uint8_t { Renewed, Lost,    Error };
enum class ReleaseStatus  : uint8_t { Released, NotFound, Stale };
enum class ValidateStatus : uint8_t { Current, FenceStale, Expired, Unknown };

struct AcquireResult {
    AcquireStatus status = AcquireStatus::Error;
    LeaseToken    token{};
};

// ---------------------------------------------------------------------
// KvBackend — replace with TiKV adapter for production.
//
// Contract: CompareAndSwap atomically:
//   * If the key is absent OR caller-supplied predicate `expect` matches
//     the stored serialized record, write `desired` and return true.
//   * Otherwise return false and load the current record into `actual`.
// ---------------------------------------------------------------------
struct KvRecord {
    bool        present = false;
    uint64_t    leaseId = 0;
    uint64_t    fencingToken = 0;
    TimePoint   expiresAt{};
    std::string owner;
};

class KvBackend {
public:
    virtual ~KvBackend() = default;

    // Read current record for `key`. Returns whether key exists.
    virtual bool Load(const std::string& key, KvRecord& out) = 0;

    // CAS write: succeed iff stored record == `expect` (compared as
    // (present, leaseId, fencingToken)). On success store `desired`.
    // On failure populate `actual` with whatever is currently stored.
    virtual bool CompareAndSwap(const std::string& key,
                                const KvRecord& expect,
                                const KvRecord& desired,
                                KvRecord& actual) = 0;

    // CAS delete: succeed iff stored record matches expect.
    virtual bool CompareAndDelete(const std::string& key,
                                  const KvRecord& expect) = 0;
};

// In-memory backend. Single-process; used by tests and the in-tree
// simulation harness. Production must inject a TiKV-backed KvBackend.
class InMemoryKvBackend : public KvBackend {
public:
    bool Load(const std::string& key, KvRecord& out) override {
        std::lock_guard<std::mutex> lk(m_mu);
        auto it = m_store.find(key);
        if (it == m_store.end()) { out.present = false; return false; }
        out = it->second;
        out.present = true;
        return true;
    }

    bool CompareAndSwap(const std::string& key,
                        const KvRecord& expect,
                        const KvRecord& desired,
                        KvRecord& actual) override {
        std::lock_guard<std::mutex> lk(m_mu);
        auto it = m_store.find(key);
        bool present = (it != m_store.end());
        if (expect.present != present) {
            if (present) { actual = it->second; actual.present = true; }
            else         { actual.present = false; }
            return false;
        }
        if (present &&
            (it->second.leaseId      != expect.leaseId ||
             it->second.fencingToken != expect.fencingToken)) {
            actual = it->second;
            actual.present = true;
            return false;
        }
        m_store[key] = desired;
        m_store[key].present = true;
        return true;
    }

    bool CompareAndDelete(const std::string& key,
                          const KvRecord& expect) override {
        std::lock_guard<std::mutex> lk(m_mu);
        auto it = m_store.find(key);
        if (it == m_store.end()) return false;
        if (it->second.leaseId      != expect.leaseId ||
             it->second.fencingToken != expect.fencingToken) {
            return false;
        }
        m_store.erase(it);
        return true;
    }

private:
    std::mutex m_mu;
    std::unordered_map<std::string, KvRecord> m_store;
};

// ---------------------------------------------------------------------
// CasLease — public API
// ---------------------------------------------------------------------
class CasLease {
public:
    explicit CasLease(std::shared_ptr<KvBackend> backend)
        : m_backend(std::move(backend)) {}

    // Acquire a TTL lease on `leaseKey` for `owner`. If the key is
    // already held by an unexpired lease, returns Conflict. If the
    // existing lease has expired, Acquire steals it with a strictly
    // greater fencing token (CAS).
    AcquireResult Acquire(const std::string& leaseKey,
                          const std::string& owner,
                          std::chrono::milliseconds ttl) {
        AcquireResult result;
        if (!m_backend) { return result; /* Error */ }

        // Up to a small bounded number of CAS retries against races.
        for (int attempt = 0; attempt < 8; ++attempt) {
            KvRecord cur{};
            bool present = m_backend->Load(leaseKey, cur);
            cur.present = present;

            const TimePoint now = SteadyClock::now();
            if (present && now < cur.expiresAt) {
                // Live lease — conflict.
                result.status = AcquireStatus::Conflict;
                result.token.leaseKey     = leaseKey;
                result.token.fencingToken = cur.fencingToken;
                result.token.expiresAt    = cur.expiresAt;
                result.token.owner        = cur.owner;
                return result;
            }

            KvRecord desired{};
            desired.present      = true;
            desired.leaseId      = NextLeaseId();
            desired.fencingToken = (present ? cur.fencingToken : 0) + 1;
            desired.expiresAt    = now + ttl;
            desired.owner        = owner;

            KvRecord actual{};
            // If absent, expect.present == false; if expired, expect
            // current record so we can CAS-replace it.
            KvRecord expect = present ? cur : KvRecord{};
            expect.present = present;

            if (m_backend->CompareAndSwap(leaseKey, expect, desired, actual)) {
                result.status              = AcquireStatus::Granted;
                result.token.leaseKey      = leaseKey;
                result.token.owner         = owner;
                result.token.leaseId       = desired.leaseId;
                result.token.fencingToken  = desired.fencingToken;
                result.token.expiresAt     = desired.expiresAt;
                return result;
            }
            // CAS race — retry. (actual reflects observed state.)
        }
        result.status = AcquireStatus::Error;
        return result;
    }

    // Extend `expiresAt` of an owned lease by `ttl`. Returns Lost if
    // the underlying record no longer matches the token (expired and
    // re-granted, deleted, or CAS lost to a thief).
    HeartbeatStatus Heartbeat(LeaseToken& token,
                              std::chrono::milliseconds ttl) {
        if (!m_backend || !token.valid()) return HeartbeatStatus::Error;

        for (int attempt = 0; attempt < 4; ++attempt) {
            KvRecord cur{};
            if (!m_backend->Load(token.leaseKey, cur)) {
                return HeartbeatStatus::Lost;
            }
            cur.present = true;
            if (cur.leaseId      != token.leaseId ||
                cur.fencingToken != token.fencingToken) {
                return HeartbeatStatus::Lost;
            }

            KvRecord desired = cur;
            desired.expiresAt = SteadyClock::now() + ttl;

            KvRecord actual{};
            if (m_backend->CompareAndSwap(token.leaseKey, cur, desired, actual)) {
                token.expiresAt = desired.expiresAt;
                return HeartbeatStatus::Renewed;
            }
            // Lost the CAS — someone else changed it. Re-check.
            if (actual.leaseId      != token.leaseId ||
                actual.fencingToken != token.fencingToken) {
                return HeartbeatStatus::Lost;
            }
        }
        return HeartbeatStatus::Error;
    }

    // Idempotent unlock. Releases iff the stored record still matches
    // the token.
    ReleaseStatus Release(const LeaseToken& token) {
        if (!m_backend || !token.valid()) return ReleaseStatus::NotFound;

        KvRecord expect{};
        expect.present      = true;
        expect.leaseId      = token.leaseId;
        expect.fencingToken = token.fencingToken;

        if (m_backend->CompareAndDelete(token.leaseKey, expect)) {
            return ReleaseStatus::Released;
        }
        // Either gone (expired & swept) or replaced — both are fine.
        KvRecord cur{};
        if (!m_backend->Load(token.leaseKey, cur)) {
            return ReleaseStatus::NotFound;
        }
        return ReleaseStatus::Stale;
    }

    // Validate that the survivor write may proceed: token must still
    // own the lease AND the caller-observed fencing token must equal
    // the current one. Used at the CAS boundary on KV writes done
    // under the lease.
    ValidateStatus Validate(const LeaseToken& token,
                            uint64_t observedFencingToken) {
        if (!m_backend || !token.valid()) return ValidateStatus::Unknown;
        KvRecord cur{};
        if (!m_backend->Load(token.leaseKey, cur)) {
            return ValidateStatus::Expired;
        }
        if (SteadyClock::now() >= cur.expiresAt) return ValidateStatus::Expired;
        if (cur.fencingToken != token.fencingToken ||
            cur.leaseId      != token.leaseId) {
            return ValidateStatus::FenceStale;
        }
        if (observedFencingToken != cur.fencingToken) {
            return ValidateStatus::FenceStale;
        }
        return ValidateStatus::Current;
    }

private:
    static uint64_t NextLeaseId() {
        static std::atomic<uint64_t> s_seq{1};
        return s_seq.fetch_add(1, std::memory_order_relaxed);
    }

    std::shared_ptr<KvBackend> m_backend;
};

// ---------------------------------------------------------------------
// Deferred TiKV impl note
// ---------------------------------------------------------------------
// The current `ExtraTiKVController` does not expose a CAS primitive
// usable without a non-trivial refactor (its interface is Get / Put /
// Scan over postings and it embeds its own `txnkv` client). Wiring CAS
// here would require either: (a) hoisting `txnkv::Transaction` out of
// `TiKVIO` and giving CasLease its own client handle, or (b) extending
// `TiKVIO` with `CompareAndSwap(key, expectedVersion, value)` and a
// matching `CompareAndDelete`.
//
// We ship the API + an in-memory KvBackend now so callers can be wired,
// the failure-mode test can run, and the on-wire binding is a localized
// follow-up (single new `KvBackend` subclass).

}  // namespace Distributed
}  // namespace SPANN
}  // namespace SPTAG

#endif  // _SPTAG_SPANN_DISTRIBUTED_CASLEASE_H_
