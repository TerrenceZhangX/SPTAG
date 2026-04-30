// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// HeadSyncVersionGate — stage-then-CAS loop for the per-(originOwner, epoch)
// `headsync:version` handle. Implements the recovery contract specified by
//   tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//     headsync-version-cas-failure.md
//
// Contract:
//   1. Build a HeadSync batch tagged with (originOwner, epoch, sequence) and
//      a stable entryUUID per entry (caller-supplied).
//   2. Read authoritative version V for the (originOwner, epoch) handle.
//   3. CAS V -> V+1 (transactionally with the per-entry writes in production).
//   4. On success -> broadcast the batch.
//   5. On CAS failure -> re-read V, recompute sequence, re-stage entries,
//      retry. Bounded by m_retryCap; on exhaustion -> RetryCapExceeded.
//
// Stale-owner fence: a writer whose claimed epoch is below expectedEpoch is
// rejected with FenceStale before any CAS.
//
// env-gate: caller checks `Armed()` (SPTAG_FAULT_HEADSYNC_VERSION_CAS_FAILURE)
// to decide whether to invoke the gate. With env unset the production hot
// path is byte-identical to the merge base.
//
// Backend: reuses Distributed::KvBackend from CasLease.h. Tests use
// InMemoryKvBackend; production wires the same TiKV adapter as CasLease.

#ifndef _SPTAG_SPANN_DISTRIBUTED_HEADSYNC_VERSION_GATE_H_
#define _SPTAG_SPANN_DISTRIBUTED_HEADSYNC_VERSION_GATE_H_

#include "CasLease.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

namespace SPTAG {
namespace SPANN {
namespace Distributed {

struct HeadSyncStagedEntry {
    std::uint64_t sequence    = 0;   // (re)assigned by Stage on retry
    std::uint64_t version     = 0;   // version this entry was committed under
    std::string   entryUUID;         // stable across retries
    std::string   payload;           // opaque to the gate
};

enum class HeadSyncStageStatus : std::uint8_t {
    Disabled,           // env-gate off — caller falls back to pre-FT path
    Committed,          // CAS succeeded; entries[].version == final version
    RetryCapExceeded,   // CAS lost m_retryCap times; surface as well-formed ERROR
    FenceStale,         // caller's epoch < expectedEpoch — drop & let new owner pick up
    BackendError,       // KvBackend missing
};

struct HeadSyncStageOutcome {
    HeadSyncStageStatus status = HeadSyncStageStatus::BackendError;
    std::uint64_t       finalVersion = 0;
    std::uint32_t       attempts     = 0;
    std::uint32_t       casFailures  = 0;
};

class HeadSyncVersionGate {
public:
    static constexpr const char* kEnvGate =
        "SPTAG_FAULT_HEADSYNC_VERSION_CAS_FAILURE";

    HeadSyncVersionGate(std::shared_ptr<KvBackend> backend,
                        std::string ownerId,
                        std::uint32_t retryCap = 8)
        : m_backend(std::move(backend))
        , m_ownerId(std::move(ownerId))
        , m_retryCap(retryCap == 0 ? 1 : retryCap) {}

    void SetRetryCap(std::uint32_t cap) { m_retryCap = cap == 0 ? 1 : cap; }

    bool Armed() const {
        const char* v = std::getenv(kEnvGate);
        return v != nullptr && v[0] != '\0' && v[0] != '0';
    }

    static std::string VersionKey(std::int32_t originOwner,
                                  std::uint64_t epoch) {
        return "headsync:version:" + std::to_string(originOwner)
             + ":epoch:" + std::to_string(epoch);
    }

    HeadSyncStageOutcome StageThenCas(std::int32_t originOwner,
                                      std::uint64_t epoch,
                                      std::uint64_t expectedEpoch,
                                      std::uint64_t baseSequence,
                                      std::vector<HeadSyncStagedEntry>& entries) {
        HeadSyncStageOutcome out;

        if (!Armed()) {
            out.status = HeadSyncStageStatus::Disabled;
            return out;
        }
        if (!m_backend) {
            out.status = HeadSyncStageStatus::BackendError;
            return out;
        }
        if (epoch < expectedEpoch) {
            out.status = HeadSyncStageStatus::FenceStale;
            m_metrics.fenceStaleTotal.fetch_add(1, std::memory_order_relaxed);
            return out;
        }

        const std::string key = VersionKey(originOwner, epoch);

        for (std::uint32_t attempt = 1; attempt <= m_retryCap; ++attempt) {
            out.attempts = attempt;
            m_metrics.casAttemptsTotal.fetch_add(1, std::memory_order_relaxed);

            // Step 1: read current version record.
            KvRecord cur{};
            bool present = m_backend->Load(key, cur);
            cur.present = present;

            // Stale-owner fence: stranger holds the slot and we know our
            // expectedEpoch is below the current authority epoch.
            if (present && !cur.owner.empty() && cur.owner != m_ownerId
                && expectedEpoch < epoch) {
                out.status = HeadSyncStageStatus::FenceStale;
                m_metrics.fenceStaleTotal.fetch_add(1, std::memory_order_relaxed);
                return out;
            }

            const std::uint64_t observedVersion = present ? cur.fencingToken : 0;
            const std::uint64_t newVersion      = observedVersion + 1;

            // Step 2: re-stage entries against (epoch, newVersion).
            std::uint64_t seq = baseSequence;
            for (auto& e : entries) {
                e.sequence = ++seq;
                e.version  = newVersion;
            }

            // Step 3: CAS V -> V+1.
            KvRecord desired{};
            desired.present      = true;
            desired.leaseId      = NextBatchId();
            desired.fencingToken = newVersion;
            desired.expiresAt    = SteadyClock::now() + std::chrono::hours(1);
            desired.owner        = m_ownerId;

            KvRecord expect = present ? cur : KvRecord{};
            expect.present = present;

            KvRecord actual{};
            if (m_backend->CompareAndSwap(key, expect, desired, actual)) {
                out.status       = HeadSyncStageStatus::Committed;
                out.finalVersion = newVersion;
                m_metrics.casCommitsTotal.fetch_add(1, std::memory_order_relaxed);
                return out;
            }

            // CAS race — count, observe, rebuild.
            ++out.casFailures;
            m_metrics.casFailuresTotal.fetch_add(1, std::memory_order_relaxed);
        }

        out.status = HeadSyncStageStatus::RetryCapExceeded;
        m_metrics.casRetriesExceededTotal.fetch_add(1, std::memory_order_relaxed);
        return out;
    }

    struct Metrics {
        std::atomic<std::uint64_t> casAttemptsTotal{0};
        std::atomic<std::uint64_t> casCommitsTotal{0};
        std::atomic<std::uint64_t> casFailuresTotal{0};
        std::atomic<std::uint64_t> casRetriesExceededTotal{0};
        std::atomic<std::uint64_t> fenceStaleTotal{0};
    };

    const Metrics& metrics() const { return m_metrics; }

private:
    static std::uint64_t NextBatchId() {
        static std::atomic<std::uint64_t> s_seq{1};
        return s_seq.fetch_add(1, std::memory_order_relaxed);
    }

    std::shared_ptr<KvBackend> m_backend;
    std::string                m_ownerId;
    std::uint32_t              m_retryCap;
    Metrics                    m_metrics;
};

}  // namespace Distributed
}  // namespace SPANN
}  // namespace SPTAG

#endif  // _SPTAG_SPANN_DISTRIBUTED_HEADSYNC_VERSION_GATE_H_
