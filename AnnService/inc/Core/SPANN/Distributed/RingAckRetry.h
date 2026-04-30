// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Fault case: ring-update-ack-loss
//
// Per-worker RingUpdate ACK retransmit + bounded-retry +
// eviction state machine. The dispatcher feeds it ticks; the
// machine emits Retransmit / Evict directives.
//
// Production behaviour (env unset) is byte-identical to the
// merged primitive: this state machine is a no-op and the
// caller falls back to the unbounded BgProtocolStep retry path.
//
// Env gate: SPTAG_FAULT_RING_UPDATE_ACK_LOSS=1 arms the cap +
// backoff + eviction surface and the counters tick.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace SPTAG::SPANN {

    struct RingAckRetryConfig {
        std::uint32_t maxAttempts = 3;
        std::chrono::milliseconds initialBackoff{200};
        std::chrono::milliseconds maxBackoff{2000};
    };

    enum class RingAckAction {
        Idle,
        Retransmit,
        Evict,
    };

    inline bool RingUpdateAckLossArmed() {
        static const bool armed = []{
            const char* v = std::getenv("SPTAG_FAULT_RING_UPDATE_ACK_LOSS");
            if (!v || !*v) return false;
            std::string_view sv(v);
            return !(sv == "0" || sv == "false" || sv == "FALSE");
        }();
        return armed;
    }

    inline bool RingUpdateAckLossArmedDynamic() {
        const char* v = std::getenv("SPTAG_FAULT_RING_UPDATE_ACK_LOSS");
        if (!v || !*v) return false;
        std::string_view sv(v);
        return !(sv == "0" || sv == "false" || sv == "FALSE");
    }

    class RingAckRetry {
    public:
        struct Counters {
            std::uint64_t retransmits = 0;
            std::uint64_t timeouts = 0;
            std::uint64_t evicted = 0;
        };

        explicit RingAckRetry(RingAckRetryConfig cfg = {}) : m_cfg(cfg) {}

        void SetConfig(RingAckRetryConfig cfg) {
            std::lock_guard<std::mutex> lk(m_mu);
            m_cfg = cfg;
        }

        RingAckRetryConfig GetConfig() const {
            std::lock_guard<std::mutex> lk(m_mu);
            return m_cfg;
        }

        void OnNewVersion(std::uint32_t version,
                          const std::vector<int>& workerIds,
                          std::chrono::steady_clock::time_point now)
        {
            std::lock_guard<std::mutex> lk(m_mu);
            m_currentVersion = version;
            m_state.clear();
            for (int w : workerIds) {
                State s;
                s.attempts = 0;
                s.lastSendAt = now;
                s.evicted = false;
                s.acked = false;
                m_state.emplace(w, s);
            }
        }

        void OnAck(int workerId, std::uint32_t version) {
            std::lock_guard<std::mutex> lk(m_mu);
            if (version < m_currentVersion) return;
            auto it = m_state.find(workerId);
            if (it == m_state.end()) return;
            if (it->second.evicted) return;  // late ACKs do not un-evict
            it->second.acked = true;
        }

        RingAckAction Tick(int workerId, std::chrono::steady_clock::time_point now) {
            std::lock_guard<std::mutex> lk(m_mu);
            auto it = m_state.find(workerId);
            if (it == m_state.end()) return RingAckAction::Idle;
            State& s = it->second;
            if (s.acked || s.evicted) return RingAckAction::Idle;

            if (s.attempts == 0) {
                if (now - s.lastSendAt < m_cfg.initialBackoff) {
                    return RingAckAction::Idle;
                }
                ++m_counters.timeouts;
                ++s.attempts;
                s.lastSendAt = now;
                ++m_counters.retransmits;
                return RingAckAction::Retransmit;
            }

            auto backoff = BackoffFor(s.attempts);
            if (now - s.lastSendAt < backoff) return RingAckAction::Idle;

            ++m_counters.timeouts;
            if (s.attempts >= m_cfg.maxAttempts) {
                s.evicted = true;
                ++m_counters.evicted;
                return RingAckAction::Evict;
            }
            ++s.attempts;
            s.lastSendAt = now;
            ++m_counters.retransmits;
            return RingAckAction::Retransmit;
        }

        Counters GetCounters() const {
            std::lock_guard<std::mutex> lk(m_mu);
            return m_counters;
        }

        bool IsEvicted(int workerId) const {
            std::lock_guard<std::mutex> lk(m_mu);
            auto it = m_state.find(workerId);
            return it != m_state.end() && it->second.evicted;
        }

        bool IsAcked(int workerId) const {
            std::lock_guard<std::mutex> lk(m_mu);
            auto it = m_state.find(workerId);
            return it != m_state.end() && it->second.acked;
        }

        std::uint32_t CurrentVersion() const {
            std::lock_guard<std::mutex> lk(m_mu);
            return m_currentVersion;
        }

    private:
        struct State {
            std::uint32_t attempts = 0;
            std::chrono::steady_clock::time_point lastSendAt{};
            bool evicted = false;
            bool acked = false;
        };

        std::chrono::milliseconds BackoffFor(std::uint32_t attempt) const {
            auto ms = m_cfg.initialBackoff;
            for (std::uint32_t i = 1; i < attempt; ++i) {
                ms *= 2;
                if (ms > m_cfg.maxBackoff) { ms = m_cfg.maxBackoff; break; }
            }
            return ms;
        }

        mutable std::mutex m_mu;
        RingAckRetryConfig m_cfg{};
        std::uint32_t m_currentVersion = 0;
        std::unordered_map<int, State> m_state;
        Counters m_counters{};
    };

} // namespace SPTAG::SPANN
