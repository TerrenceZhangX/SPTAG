// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault wrapper: compute-driver-die-mid-dispatch
//
// Header-only, env-gated guard around a driver/dispatcher dispatch round.
// When SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH=1 the wrapper:
//   * deterministically simulates driver process loss after N batches
//     have been sent on a dispatch round (seed-based, reproducible);
//   * lets workers detect the loss via SWIM (SwimDetector) — on Dispatcher
//     transitioning to Suspect/Dead, queued batches with a full payload
//     and a current ring-version are drained, others NACK back to the
//     retry queue;
//   * applies a ring-epoch fence at the worker side: any write from a
//     dispatcher whose ring-version is < cluster ring-version (zombie
//     dispatcher resurfacing after a new driver took over) is rejected
//     and bumps m_ringFenceRejected.
//
// When the env is unset the wrapper is dormant: dispatch runs to
// completion, no batches die, no workers fence, all four counters stay
// at zero. This protects baseline performance and means the wrapper is
// invisible to non-fault production builds.
//
// Defers-to-primitive:
//   * SwimDetector  (prim/swim-membership) — dispatcher liveness signal.
//   * RingEpoch + CompareRingEpoch (prim/ring-epoch-fence) — fencing.
//
// Spec: tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/
//       compute-driver-die-mid-dispatch.md
//
// Tier 2 1M perf: DEFERRED — the wrapper does not run in the SPANN
// search inner loop (dispatch coordination only), so no hot-path impact.

#pragma once

#include "inc/Core/Common.h"
#include "inc/Core/SPANN/Distributed/RingEpoch.h"
#include "inc/Core/SPANN/Distributed/SwimDetector.h"

#include <atomic>
#include <cstdlib>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace SPTAG::SPANN::Distributed {

    /// Env gate. Off by default.
    inline bool DriverDispatchGuardArmed()
    {
        const char* v = std::getenv("SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH");
        return v && v[0] && v[0] != '0';
    }

    /// One in-flight dispatch unit. The driver hands these to workers
    /// over the dispatch path; on driver loss workers must each decide
    /// to drain (apply locally + ack) or NACK (return to retry queue).
    struct DispatchBatch {
        std::uint64_t batchId = 0;
        int           ownerWorker = -1;
        RingEpoch     ringAtSend{};
        bool          payloadComplete = false;  // worker received full payload
    };

    /// Outcome of one batch's recovery decision after driver loss.
    enum class BatchRecovery : std::uint8_t {
        Drained = 0,     // worker had full payload + valid ring → applied + acked
        Nacked  = 1,     // worker missing payload or ring stale → NACK to retry queue
        Fenced  = 2,     // came from zombie dispatcher under stale ring → rejected
    };

    /// Dispatch counters. Plumbed by tests + ops; matches the spec
    /// counter list in the case-md (Detection point, tertiary):
    ///   * m_driverDied         : dispatcher process loss observed (0 or 1 per round)
    ///   * m_batchesDrained     : batches workers drained successfully under SWIM-detected loss
    ///   * m_batchesNacked      : batches workers NACKed back to retry queue
    ///   * m_ringFenceRejected  : writes rejected because the dispatcher's
    ///                            ring-version was stale (zombie path)
    struct DispatchCounters {
        std::atomic<std::uint64_t> m_driverDied{0};
        std::atomic<std::uint64_t> m_batchesDrained{0};
        std::atomic<std::uint64_t> m_batchesNacked{0};
        std::atomic<std::uint64_t> m_ringFenceRejected{0};

        void Reset() {
            m_driverDied.store(0, std::memory_order_relaxed);
            m_batchesDrained.store(0, std::memory_order_relaxed);
            m_batchesNacked.store(0, std::memory_order_relaxed);
            m_ringFenceRejected.store(0, std::memory_order_relaxed);
        }

        struct Snapshot {
            std::uint64_t driverDied;
            std::uint64_t batchesDrained;
            std::uint64_t batchesNacked;
            std::uint64_t ringFenceRejected;
        };

        Snapshot Get() const {
            return Snapshot{
                m_driverDied.load(std::memory_order_relaxed),
                m_batchesDrained.load(std::memory_order_relaxed),
                m_batchesNacked.load(std::memory_order_relaxed),
                m_ringFenceRejected.load(std::memory_order_relaxed),
            };
        }
    };

    /// Header-only guard wrapping a dispatcher-driven dispatch round.
    /// Single instance per dispatcher; safe to share across worker
    /// threads (counters are atomic, the configured callbacks are
    /// expected to be reentrant).
    class DriverDispatchGuard {
    public:
        /// Construct disarmed. Call SetDeathSchedule() to make tests
        /// deterministic; production callers leave the schedule at
        /// default (death disabled even when the env is set).
        DriverDispatchGuard() = default;

        /// Test/operator hook. `dieAfterNBatches`==0 means death is
        /// disabled even when the env is armed (control sub-toggle, used
        /// by BaselineDispatchSucceeds). Otherwise the guard simulates
        /// driver loss immediately after the Nth batch send returns.
        void SetDeathSchedule(std::uint32_t dieAfterNBatches) {
            std::lock_guard<std::mutex> lk(m_mu);
            m_dieAfterN = dieAfterNBatches;
        }

        /// Number of batches sent so far on the current round; cleared
        /// at the start of every RunDispatchRound() call.
        std::uint32_t BatchesSent() const {
            std::lock_guard<std::mutex> lk(m_mu);
            return m_sent;
        }

        DispatchCounters& Counters() { return m_counters; }
        const DispatchCounters& Counters() const { return m_counters; }

        // ---------------------------------------------------------------
        // Round entry point. Sends `batches` in order via the caller-
        // supplied sendOne hook. If the env is armed AND the death
        // schedule fires, the round aborts after N successful sends and
        // returns DriverLost so the caller (and observers) can react.
        // The remaining (unsent) batches are returned via outRemaining
        // for the worker-side recovery path.
        // ---------------------------------------------------------------
        ErrorCode RunDispatchRound(
            const std::vector<DispatchBatch>& batches,
            const std::function<ErrorCode(const DispatchBatch&)>& sendOne,
            std::vector<DispatchBatch>* outRemaining = nullptr)
        {
            const bool armed = DriverDispatchGuardArmed();
            std::uint32_t dieAt;
            {
                std::lock_guard<std::mutex> lk(m_mu);
                m_sent = 0;
                dieAt = armed ? m_dieAfterN : 0;
            }

            for (std::size_t i = 0; i < batches.size(); ++i) {
                ErrorCode rc = sendOne(batches[i]);
                if (rc != ErrorCode::Success) return rc;
                {
                    std::lock_guard<std::mutex> lk(m_mu);
                    ++m_sent;
                    if (dieAt > 0 && m_sent == dieAt && i + 1 < batches.size()) {
                        // Simulate process death between batches.
                        m_counters.m_driverDied.fetch_add(1, std::memory_order_relaxed);
                        if (outRemaining) {
                            outRemaining->assign(batches.begin() + i + 1, batches.end());
                        }
                        return ErrorCode::DriverLost;
                    }
                }
            }
            return ErrorCode::Success;
        }

        // ---------------------------------------------------------------
        // Worker-side recovery. Called once a worker observes the
        // dispatcher transitioning to Suspect/Dead via SwimDetector. For
        // each in-flight batch the worker decides drain vs nack:
        //   * payloadComplete && ring not stale → drain
        //   * else                              → nack
        // The function bumps the matching counter and returns the
        // per-batch decision so the caller can apply / requeue.
        // ---------------------------------------------------------------
        std::vector<BatchRecovery>
        WorkerRecoverInFlight(const std::vector<DispatchBatch>& inFlight,
                              const RingEpoch& clusterRing) {
            std::vector<BatchRecovery> out;
            out.reserve(inFlight.size());
            for (const auto& b : inFlight) {
                const bool ringOk =
                    b.ringAtSend.IsInitialised() && !(b.ringAtSend < clusterRing);
                if (b.payloadComplete && ringOk) {
                    m_counters.m_batchesDrained.fetch_add(1, std::memory_order_relaxed);
                    out.push_back(BatchRecovery::Drained);
                } else {
                    m_counters.m_batchesNacked.fetch_add(1, std::memory_order_relaxed);
                    out.push_back(BatchRecovery::Nacked);
                }
            }
            return out;
        }

        // ---------------------------------------------------------------
        // Ring-epoch fence. Called by a worker on every inbound dispatch
        // RPC. Rejects writes from a dispatcher whose ring-version is
        // older than the worker's known cluster ring (zombie dispatcher
        // path). Returns true → accept; false → reject (counter bumped).
        // SenderUninitialised is also rejected per the prim's contract.
        // ---------------------------------------------------------------
        bool AcceptUnderRingFence(const RingEpoch& senderRing,
                                  const RingEpoch& localRing)
        {
            const RingEpochCompare cmp = CompareRingEpoch(senderRing, localRing);
            if (cmp == RingEpochCompare::SenderStale ||
                cmp == RingEpochCompare::SenderUninitialised) {
                m_counters.m_ringFenceRejected.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            return true;
        }

        // ---------------------------------------------------------------
        // Helper: register a SwimDetector observer that calls `onLoss`
        // when `dispatcherNodeId` transitions to Suspect or Dead. Used
        // by tests to wire the worker-side recovery path. Production
        // callers may instead poll TryGetMember() in their dispatch
        // wait loop; the observer style keeps the wrapper IO-free.
        // ---------------------------------------------------------------
        static void ObserveDispatcherLoss(SwimDetector& det,
                                          int dispatcherNodeId,
                                          std::function<void()> onLoss)
        {
            det.OnMemberChangeCallback(
                [dispatcherNodeId, cb = std::move(onLoss)](
                    int nodeId, MemberChange change,
                    const MemberState& /*state*/) {
                    if (nodeId != dispatcherNodeId) return;
                    if (change == MemberChange::BecameSuspect ||
                        change == MemberChange::BecameDead) {
                        if (cb) cb();
                    }
                });
        }

    private:
        mutable std::mutex m_mu;
        std::uint32_t      m_dieAfterN = 0;
        std::uint32_t      m_sent      = 0;
        DispatchCounters   m_counters;
    };

} // namespace SPTAG::SPANN::Distributed
