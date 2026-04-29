// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Test-only friend hook for SPTAG::SPANN::TiKVIO.
//
// Goal: avoid sprinkling Test* pass-through methods across the public API
// of TiKVIO every time a fault-case test needs to poke an internal path.
// Instead, declare a single `friend class TiKVIOTestHook` in the controller
// (already done, unconditionally), and centralise the test-only entry
// points here.
//
// Each fault case's *_test.cpp can `#include "lib/test_hooks.h"` and call
// `TiKVIOTestHook::...`. As fault branches add real impl (new private
// methods or counters on TiKVIO), they extend this header at the same time.
//
// IMPORTANT: every accessor in this baseline must compile against today's
// TiKVIO. Speculative accessors for not-yet-implemented hooks return 0 so
// downstream tests can be written ahead of impl; once a fault branch adds
// the real member/method, that branch updates the accessor body in the
// same commit.

#pragma once

#ifdef TIKV

#include "inc/Core/SPANN/ExtraTiKVController.h"

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace SPTAG::SPANN::test {

/// Centralised test-only access to TiKVIO internals.
///
/// Declared `friend` of `TiKVIO` (see ExtraTiKVController.h, unconditional).
/// All methods are static; this class never needs an instance.
class TiKVIOTestHook {
public:
    // ---- Cache control ---------------------------------------------------
    // Real impl lands on the relevant fault branches (e.g. pd-store-discovery-stale
    // adds InvalidateStoreCache / InvalidateStoreCacheForKey). Until then these
    // are no-ops so the harness header stays compile-clean for *every* branch.
    static void invalidate_store_cache(TiKVIO& /*io*/, uint64_t /*storeId*/) {
        // no-op until fault branch lands real method
    }
    static void invalidate_store_cache_for_key(TiKVIO& /*io*/, const std::string& /*key*/) {
        // no-op until fault branch lands real method
    }

    // Region-level cache invalidation already exists upstream.
    static void invalidate_region_cache_for_key(TiKVIO& io, const std::string& key) {
        io.InvalidateRegionCache(key);
    }

    // ---- PD member refresh ----------------------------------------------
    // Real impl pending; returns false until the headsync-pull-rpc / discovery
    // fault branch lands RefreshPDMembers().
    static bool force_refresh_pd_members(TiKVIO& /*io*/) {
        return false;
    }

    // ---- Counters --------------------------------------------------------
    // All four return 0 today. Fault branches that add the underlying
    // std::atomic<uint64_t> members on TiKVIO update the accessor body here
    // in the same commit (one-line change).
    static uint64_t store_addr_invalidations(const TiKVIO& /*io*/) { return 0; }
    static uint64_t pd_member_refreshes(const TiKVIO& /*io*/)      { return 0; }
    static uint64_t cluster_id_mismatches(const TiKVIO& /*io*/)    { return 0; }
    static uint64_t stub_pool_evictions(const TiKVIO& io)          { return io.m_stubPoolEvictions.load(); }

    // ---- Cache shape -----------------------------------------------------
    // These read existing private members via friend access; safe today.
    static size_t store_addr_cache_size(const TiKVIO& io) {
        std::lock_guard<std::mutex> lock(io.m_storeAddrMutex);
        return io.m_storeAddrCache.size();
    }
    static size_t stub_pool_count(const TiKVIO& io) {
        std::lock_guard<std::mutex> lock(io.m_storeMutex);
        return io.m_storeStubs.size();
    }
    static std::vector<std::string> pd_addresses_snapshot(const TiKVIO& io) {
        return io.m_pdAddresses;
    }
    static uint64_t cluster_id(const TiKVIO& io) {
        return io.m_clusterId;
    }
    // ---- Stub-pool fault injection ----------------------------------------
    // Force a slot in the stub pool for `address` to be marked broken.
    // Returns true if a slot was flipped. The next GetNext() landing on it
    // rebuilds the channel and bumps stub_pool_evictions.
    static bool force_evict_stub_slot(TiKVIO& io, const std::string& address, size_t idx) {
        std::lock_guard<std::mutex> lock(io.m_storeMutex);
        auto it = io.m_storeStubs.find(address);
        if (it == io.m_storeStubs.end()) return false;
        auto& pool = *it->second;
        if (idx >= pool.slots.size()) return false;
        pool.slots[idx]->broken.store(true, std::memory_order_release);
        return true;
    }
    static size_t stub_pool_size_for(TiKVIO& io, const std::string& address) {
        std::lock_guard<std::mutex> lock(io.m_storeMutex);
        auto it = io.m_storeStubs.find(address);
        if (it == io.m_storeStubs.end()) return 0;
        return it->second->slots.size();
    }
    static std::vector<std::string> known_store_addresses(const TiKVIO& io) {
        std::lock_guard<std::mutex> lock(io.m_storeMutex);
        std::vector<std::string> r; r.reserve(io.m_storeStubs.size());
        for (auto& kv : io.m_storeStubs) r.push_back(kv.first);
        return r;
    }
};

}  // namespace SPTAG::SPANN::test

#endif  // TIKV
