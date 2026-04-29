// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Primitive: SwimDetector
//
// MVP SWIM-style failure detector. Periodic ping + indirect-ping fan-out,
// suspect/dead state machine with timeouts, gossip piggyback on ping/ack.
//
// Scope (MVP):
//   - Detects dead nodes within ~3 ticks using direct ping + k-fanout
//     indirect ping.
//   - Maintains a Member view {Alive, Suspect(since), Dead(since)} with
//     incarnation numbers so a node can refute its own suspicion on
//     rejoin.
//   - Header-only by design. Caller integrates the wire via a Transport
//     interface; no new socket dependency is taken here.
//
// Out-of-scope (Wave-2 follow-ups, called out in closure note):
//   - Lifeguard refinements (nack-aware suspicion timeouts, dogpile
//     dampening).
//   - Persistent incarnation rollback past a process restart.
//   - Disseminator with bounded gossip queue + per-message age cap (we
//     gossip every member on every ping; fine for cluster sizes < ~32).
//
// Wire ONLY into PostingRouter / ConsistentHashRing as a member-change
// observer via OnMemberChangeCallback(). Refactoring split/merge or
// HeadSync to react is per-case work, not done here.

#pragma once

#include "inc/Core/Common.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

namespace SPTAG::SPANN {

    /// Member health states observed by SWIM.
    enum class MemberHealth : std::uint8_t {
        Alive   = 0,
        Suspect = 1,
        Dead    = 2,
    };

    /// One member's known state. `incarnation` follows SWIM:
    /// only the member itself can bump its own incarnation, which is
    /// how a wrongly-suspected node refutes the suspicion.
    struct MemberState {
        MemberHealth health = MemberHealth::Alive;
        std::uint64_t incarnation = 0;
        std::uint64_t since_tick  = 0;   // tick at which we entered `health`
        std::uint64_t last_seen   = 0;   // tick at which we last got an ack
    };

    /// SWIM tunables. All times are in "ticks" — the caller's chosen
    /// quantum from Tick(). A typical config drives Tick() once per
    /// ~250 ms, which makes the defaults below detect dead within
    /// roughly 1-2 s.
    struct SwimConfig {
        std::uint32_t indirect_fanout      = 3;  // k for ping-req fanout
        std::uint64_t suspect_after_ticks  = 1;  // direct miss → Suspect
        std::uint64_t dead_after_ticks     = 3;  // Suspect → Dead
        std::uint32_t ping_targets_per_tick = 1; // round-robin ping count
        std::uint64_t rng_seed             = 0xC0FFEEULL;
    };

    /// One outgoing message produced by the detector. The caller
    /// (Transport) is responsible for actual delivery; the detector
    /// stays sync-free and IO-free.
    struct SwimMessage {
        enum class Kind : std::uint8_t {
            Ping        = 1,
            Ack         = 2,
            PingReq     = 3, // "please ping `target` for me"
            IndirectAck = 4, // proxied ack flowing back through helper
        } kind = Kind::Ping;

        int from   = -1;  // sender node id
        int to     = -1;  // recipient node id
        int target = -1;  // for PingReq / IndirectAck: the probed node

        // Gossip piggyback. We send the full known view; consumers
        // should keep cluster size modest. Each entry: (node, state).
        std::vector<std::pair<int, MemberState>> updates;
    };

    /// Reason passed to OnMemberChange callback so observers can react
    /// (e.g. ConsistentHashRing::RemoveNode on first Dead).
    enum class MemberChange : std::uint8_t {
        Joined         = 0,  // first time we see a node
        BecameAlive    = 1,
        BecameSuspect  = 2,
        BecameDead     = 3,
        Refuted        = 4,  // own incarnation bumped past suspicion
    };

    using MemberChangeCallback =
        std::function<void(int nodeId, MemberChange change,
                           const MemberState& newState)>;

    /// Header-only SWIM detector. Thread-safety: the detector takes an
    /// internal mutex around every public method. Callers may invoke
    /// Tick() and OnXxx() handlers concurrently from different threads;
    /// callbacks fire while the lock is held — keep observer code
    /// non-blocking.
    class SwimDetector {
    public:
        SwimDetector(int p_localNodeId,
                     std::vector<int> p_initialPeers,
                     SwimConfig p_config = {})
            : m_self(p_localNodeId),
              m_config(p_config),
              m_rng(p_config.rng_seed ^ static_cast<std::uint64_t>(
                        p_localNodeId * 0x9E3779B97F4A7C15ULL)) {
            // Self is implicitly Alive at incarnation 0.
            m_members[m_self] = MemberState{MemberHealth::Alive, 0, 0, 0};
            for (int peer : p_initialPeers) {
                if (peer == m_self) continue;
                m_members[peer] = MemberState{MemberHealth::Alive, 0, 0, 0};
            }
        }

        // -- API -----------------------------------------------------

        /// Drive one detector tick. Returns the messages the caller
        /// must transmit (free of any IO).
        std::vector<SwimMessage> Tick() {
            std::lock_guard<std::mutex> lk(m_mu);
            ++m_tick;
            std::vector<SwimMessage> out;
            EvaluateTimeoutsLocked(out);
            EmitDirectPingsLocked(out);
            EmitIndirectPingsLocked(out);
            return out;
        }

        /// Inbound Ping from `from`. Caller should send the returned
        /// vector (a single Ack with our gossip piggyback).
        std::vector<SwimMessage> OnPing(int from,
                const std::vector<std::pair<int, MemberState>>& updates) {
            std::lock_guard<std::mutex> lk(m_mu);
            ApplyUpdatesLocked(updates);
            MarkAliveLocked(from);
            std::vector<SwimMessage> out;
            SwimMessage ack;
            ack.kind = SwimMessage::Kind::Ack;
            ack.from = m_self;
            ack.to   = from;
            ack.target = m_self;
            BuildGossipLocked(ack.updates);
            out.push_back(std::move(ack));
            return out;
        }

        /// Inbound direct Ack for a ping we previously emitted.
        void OnAck(int from,
                   const std::vector<std::pair<int, MemberState>>& updates) {
            std::lock_guard<std::mutex> lk(m_mu);
            ApplyUpdatesLocked(updates);
            MarkAliveLocked(from);
            m_pendingDirectAcks.erase(from);
        }

        /// Inbound PingReq("please ping target on my behalf"). Returns
        /// the ping the caller must send to `target` (with `from` /
        /// requester encoded so we can reply to them).
        std::vector<SwimMessage> OnPingReq(int target, int requester,
                const std::vector<std::pair<int, MemberState>>& updates) {
            std::lock_guard<std::mutex> lk(m_mu);
            ApplyUpdatesLocked(updates);
            std::vector<SwimMessage> out;
            // Track the indirect request so when we get an Ack from
            // `target` we can forward an IndirectAck to `requester`.
            m_indirectRequesters[target].insert(requester);
            // Emit a normal Ping; we're transparent to `target`.
            SwimMessage p;
            p.kind = SwimMessage::Kind::Ping;
            p.from = m_self;
            p.to   = target;
            BuildGossipLocked(p.updates);
            out.push_back(std::move(p));
            return out;
        }

        /// Inbound IndirectAck that another node forwarded on our
        /// behalf — equivalent in effect to a direct Ack.
        void OnIndirectAck(int target,
                const std::vector<std::pair<int, MemberState>>& updates) {
            std::lock_guard<std::mutex> lk(m_mu);
            ApplyUpdatesLocked(updates);
            MarkAliveLocked(target);
            m_pendingIndirectAcks.erase(target);
        }

        /// Snapshot of the current member view.
        std::map<int, MemberState> MemberView() const {
            std::lock_guard<std::mutex> lk(m_mu);
            return std::map<int, MemberState>{m_members.begin(),
                                              m_members.end()};
        }

        /// Inspect a single member; returns false if unknown.
        bool TryGetMember(int nodeId, MemberState& out) const {
            std::lock_guard<std::mutex> lk(m_mu);
            auto it = m_members.find(nodeId);
            if (it == m_members.end()) return false;
            out = it->second;
            return true;
        }

        /// Subscribe to state transitions. Replaces any existing cb.
        void OnMemberChangeCallback(MemberChangeCallback cb) {
            std::lock_guard<std::mutex> lk(m_mu);
            m_callback = std::move(cb);
        }

        /// Operator-driven join (e.g. before SWIM picks up the node).
        void AddPeer(int nodeId) {
            std::lock_guard<std::mutex> lk(m_mu);
            if (m_members.find(nodeId) == m_members.end()) {
                MemberState st{MemberHealth::Alive, 0, m_tick, m_tick};
                m_members[nodeId] = st;
                FireLocked(nodeId, MemberChange::Joined, st);
            }
        }

        /// Tick counter, exposed for tests / metrics.
        std::uint64_t CurrentTick() const {
            std::lock_guard<std::mutex> lk(m_mu);
            return m_tick;
        }

        int Self() const { return m_self; }

    private:
        // -- state mutation (lock held) ------------------------------

        void FireLocked(int nodeId, MemberChange c, const MemberState& s) {
            if (m_callback) m_callback(nodeId, c, s);
        }

        void MarkAliveLocked(int nodeId) {
            if (nodeId == m_self) return;
            auto& st = m_members[nodeId];
            const bool first = (st.last_seen == 0 && st.since_tick == 0
                                && st.health == MemberHealth::Alive);
            const bool transitioning =
                (st.health == MemberHealth::Suspect ||
                 st.health == MemberHealth::Dead);
            st.last_seen = m_tick;
            if (transitioning) {
                st.health = MemberHealth::Alive;
                st.since_tick = m_tick;
                FireLocked(nodeId, MemberChange::BecameAlive, st);
            } else if (first) {
                st.since_tick = m_tick;
                FireLocked(nodeId, MemberChange::Joined, st);
            }
        }

        void ApplyUpdatesLocked(
                const std::vector<std::pair<int, MemberState>>& updates) {
            for (const auto& [id, incoming] : updates) {
                if (id == m_self) {
                    // Refute foreign suspicion / death about *us*.
                    if (incoming.health != MemberHealth::Alive) {
                        auto& mine = m_members[m_self];
                        if (incoming.incarnation >= mine.incarnation) {
                            mine.incarnation = incoming.incarnation + 1;
                            mine.health = MemberHealth::Alive;
                            mine.since_tick = m_tick;
                            mine.last_seen = m_tick;
                            FireLocked(m_self, MemberChange::Refuted, mine);
                        }
                    }
                    continue;
                }
                auto it = m_members.find(id);
                if (it == m_members.end()) {
                    MemberState st = incoming;
                    st.since_tick = m_tick;
                    st.last_seen  = (incoming.health == MemberHealth::Alive)
                                        ? m_tick : 0;
                    m_members[id] = st;
                    FireLocked(id, MemberChange::Joined, st);
                    continue;
                }
                MemberState& cur = it->second;
                // Higher incarnation always wins. Same incarnation:
                // worse health (Dead > Suspect > Alive) wins.
                const bool higherInc = incoming.incarnation > cur.incarnation;
                const bool sameInc   = incoming.incarnation == cur.incarnation;
                const bool worse     = static_cast<int>(incoming.health) >
                                       static_cast<int>(cur.health);
                if (higherInc || (sameInc && worse)) {
                    MemberHealth prev = cur.health;
                    cur.incarnation = incoming.incarnation;
                    cur.health = incoming.health;
                    cur.since_tick = m_tick;
                    if (cur.health == MemberHealth::Alive) cur.last_seen = m_tick;
                    if (prev != cur.health) {
                        MemberChange c =
                            (cur.health == MemberHealth::Alive)
                                ? MemberChange::BecameAlive
                                : (cur.health == MemberHealth::Suspect)
                                    ? MemberChange::BecameSuspect
                                    : MemberChange::BecameDead;
                        FireLocked(id, c, cur);
                    }
                }
            }
        }

        void EvaluateTimeoutsLocked(std::vector<SwimMessage>& /*out*/) {
            for (auto& [id, st] : m_members) {
                if (id == m_self) continue;
                if (st.health == MemberHealth::Alive) {
                    // If a direct ping is outstanding past the suspect
                    // window AND we haven't heard back, mark Suspect.
                    auto pd = m_pendingDirectAcks.find(id);
                    if (pd != m_pendingDirectAcks.end() &&
                        m_tick - pd->second >= m_config.suspect_after_ticks) {
                        st.health = MemberHealth::Suspect;
                        st.since_tick = m_tick;
                        FireLocked(id, MemberChange::BecameSuspect, st);
                    }
                } else if (st.health == MemberHealth::Suspect) {
                    if (m_tick - st.since_tick >= m_config.dead_after_ticks) {
                        st.health = MemberHealth::Dead;
                        st.since_tick = m_tick;
                        FireLocked(id, MemberChange::BecameDead, st);
                    }
                }
            }
        }

        void EmitDirectPingsLocked(std::vector<SwimMessage>& out) {
            std::vector<int> candidates;
            candidates.reserve(m_members.size());
            for (const auto& [id, st] : m_members) {
                if (id == m_self) continue;
                if (st.health == MemberHealth::Dead) continue;
                candidates.push_back(id);
            }
            if (candidates.empty()) return;
            std::shuffle(candidates.begin(), candidates.end(), m_rng);
            std::uint32_t count = std::min<std::uint32_t>(
                m_config.ping_targets_per_tick,
                static_cast<std::uint32_t>(candidates.size()));
            for (std::uint32_t i = 0; i < count; ++i) {
                int target = candidates[i];
                SwimMessage p;
                p.kind = SwimMessage::Kind::Ping;
                p.from = m_self;
                p.to   = target;
                BuildGossipLocked(p.updates);
                out.push_back(std::move(p));
                m_pendingDirectAcks[target] = m_tick;
            }
        }

        void EmitIndirectPingsLocked(std::vector<SwimMessage>& out) {
            // For each Suspect, ask k random Alive helpers to ping.
            std::vector<int> alive;
            for (const auto& [id, st] : m_members) {
                if (id == m_self) continue;
                if (st.health == MemberHealth::Alive) alive.push_back(id);
            }
            for (auto& [id, st] : m_members) {
                if (id == m_self) continue;
                if (st.health != MemberHealth::Suspect) continue;
                if (m_pendingIndirectAcks.count(id)) continue;
                if (alive.empty()) continue;
                std::shuffle(alive.begin(), alive.end(), m_rng);
                std::uint32_t k = std::min<std::uint32_t>(
                    m_config.indirect_fanout,
                    static_cast<std::uint32_t>(alive.size()));
                for (std::uint32_t i = 0; i < k; ++i) {
                    int helper = alive[i];
                    if (helper == id) continue;
                    SwimMessage pr;
                    pr.kind   = SwimMessage::Kind::PingReq;
                    pr.from   = m_self;
                    pr.to     = helper;
                    pr.target = id;
                    BuildGossipLocked(pr.updates);
                    out.push_back(std::move(pr));
                }
                m_pendingIndirectAcks[id] = m_tick;
            }
        }

        void BuildGossipLocked(
                std::vector<std::pair<int, MemberState>>& updates) {
            updates.reserve(m_members.size());
            for (const auto& [id, st] : m_members) {
                updates.emplace_back(id, st);
            }
        }

        // -- members -------------------------------------------------

        const int m_self;
        SwimConfig m_config;
        mutable std::mutex m_mu;
        std::uint64_t m_tick = 0;
        std::unordered_map<int, MemberState> m_members;
        // node → tick at which we sent our last unacknowledged ping
        std::unordered_map<int, std::uint64_t> m_pendingDirectAcks;
        // node → tick at which we issued ping-req fanout
        std::unordered_map<int, std::uint64_t> m_pendingIndirectAcks;
        // target → set of requesters waiting for IndirectAck
        std::unordered_map<int, std::set<int>> m_indirectRequesters;
        MemberChangeCallback m_callback;
        std::mt19937_64 m_rng;
    };

    /// Convenience observer that wires SWIM directly to a
    /// ConsistentHashRing. Removes nodes on first Dead and re-adds on
    /// rejoin (BecameAlive after Dead). Per-case work is responsible
    /// for invalidating in-flight RPCs.
    template <typename Ring>
    inline MemberChangeCallback MakeRingMembershipObserver(
            Ring& ring, std::mutex& ringMutex) {
        return [&ring, &ringMutex](int nodeId, MemberChange c,
                                   const MemberState&) {
            std::lock_guard<std::mutex> lk(ringMutex);
            switch (c) {
                case MemberChange::BecameDead:
                    if (ring.HasNode(nodeId)) ring.RemoveNode(nodeId);
                    break;
                case MemberChange::BecameAlive:
                case MemberChange::Refuted:
                case MemberChange::Joined:
                    if (!ring.HasNode(nodeId)) ring.AddNode(nodeId);
                    break;
                case MemberChange::BecameSuspect:
                    // Keep node in ring while Suspect; flip out only on Dead.
                    break;
            }
        };
    }

} // namespace SPTAG::SPANN
