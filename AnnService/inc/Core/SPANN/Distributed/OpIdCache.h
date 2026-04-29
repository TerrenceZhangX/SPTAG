// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// OpIdCache — receiver-side idempotency dedup cache.
//
// When a receiver handles a write request stamped with an OpId, it does:
//
//   auto [hit, cached] = cache.Lookup(opId);
//   if (hit) return cached;            // sender retry; replay cached result
//   ErrorCode result = DoWrite(...);
//   cache.Insert(opId, result);
//   return result;
//
// Concurrency: a coarse mutex protects the LRU list + index. The cache is
// only consulted on write paths (≤ a few thousand QPS per node), so this is
// not a hot point. Replace with a sharded map if needed later.
//
// Eviction:
//   - Capacity bound: when size > capacity, evict LRU tail.
//   - TTL: entries older than ttl are treated as misses on Lookup and
//     reaped on next Insert / explicit Sweep().
//
// Restart-epoch semantics:
//   When InvalidateOlderEpochs(senderId, currentEpoch) is called (typically
//   on receiving a write from sender S whose epoch jumped, or on an
//   explicit out-of-band notification), all entries for S with
//   restartEpoch < currentEpoch are dropped. Same-counter / old-epoch
//   entries cannot collide with the new epoch's counters.

#pragma once

#include "inc/Core/SPANN/Distributed/OpId.h"
#include "inc/Core/Common.h"
#include <chrono>
#include <list>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace SPTAG::SPANN::Distributed {

    template <typename TResult>
    class OpIdCache {
    public:
        using Clock     = std::chrono::steady_clock;
        using TimePoint = Clock::time_point;
        using Duration  = Clock::duration;

        struct Entry {
            OpId      id;
            TResult   result;
            TimePoint expiresAt;
        };

        OpIdCache(std::size_t capacity,
                  Duration ttl)
            : m_capacity(capacity ? capacity : 1)
            , m_ttl(ttl) {}

        // Returns {true, result} if a non-expired entry exists; {false, _}
        // otherwise. A hit refreshes recency (LRU touch) but does not
        // refresh expiry — TTL is bound to first-insert, so a slow retry
        // still gets a deterministic answer right up until expiry.
        std::pair<bool, TResult> Lookup(const OpId& id) {
            std::lock_guard<std::mutex> g(m_mu);
            auto it = m_index.find(id);
            if (it == m_index.end()) return {false, TResult{}};
            auto listIt = it->second;
            if (listIt->expiresAt <= Clock::now()) {
                m_lru.erase(listIt);
                m_index.erase(it);
                return {false, TResult{}};
            }
            // LRU touch: move to front.
            m_lru.splice(m_lru.begin(), m_lru, listIt);
            return {true, listIt->result};
        }

        // Insert or overwrite. Returns true if a new entry was inserted,
        // false if an existing entry was overwritten.
        bool Insert(const OpId& id, TResult result) {
            std::lock_guard<std::mutex> g(m_mu);
            auto now = Clock::now();
            auto it = m_index.find(id);
            if (it != m_index.end()) {
                it->second->result = std::move(result);
                it->second->expiresAt = now + m_ttl;
                m_lru.splice(m_lru.begin(), m_lru, it->second);
                return false;
            }
            m_lru.push_front(Entry{id, std::move(result), now + m_ttl});
            m_index.emplace(id, m_lru.begin());
            EvictLocked();
            return true;
        }

        // Drops any cached entries for `senderId` whose restartEpoch is
        // strictly less than `currentEpoch`. Returns count removed.
        std::size_t InvalidateOlderEpochs(std::int32_t senderId,
                                         std::uint32_t currentEpoch) {
            std::lock_guard<std::mutex> g(m_mu);
            std::size_t removed = 0;
            for (auto it = m_lru.begin(); it != m_lru.end(); ) {
                if (it->id.senderId == senderId
                    && it->id.restartEpoch < currentEpoch) {
                    m_index.erase(it->id);
                    it = m_lru.erase(it);
                    ++removed;
                } else {
                    ++it;
                }
            }
            return removed;
        }

        // Reaps expired entries. Called opportunistically.
        std::size_t Sweep() {
            std::lock_guard<std::mutex> g(m_mu);
            auto now = Clock::now();
            std::size_t removed = 0;
            for (auto it = m_lru.begin(); it != m_lru.end(); ) {
                if (it->expiresAt <= now) {
                    m_index.erase(it->id);
                    it = m_lru.erase(it);
                    ++removed;
                } else {
                    ++it;
                }
            }
            return removed;
        }

        std::size_t Size() const {
            std::lock_guard<std::mutex> g(m_mu);
            return m_lru.size();
        }

        std::size_t Capacity() const { return m_capacity; }
        Duration    Ttl()      const { return m_ttl; }

    private:
        void EvictLocked() {
            while (m_lru.size() > m_capacity) {
                auto& last = m_lru.back();
                m_index.erase(last.id);
                m_lru.pop_back();
            }
        }

        mutable std::mutex m_mu;
        std::size_t        m_capacity;
        Duration           m_ttl;
        std::list<Entry>   m_lru;
        std::unordered_map<OpId, typename std::list<Entry>::iterator,
                           std::hash<OpId>> m_index;
    };

} // namespace SPTAG::SPANN::Distributed
