// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Test-only friend hook for SPTAG::SPANN::TiKVIO.
//
// Goal: avoid sprinkling `Test*` pass-through methods across the public API
// of TiKVIO (and similar controllers) every time a fault-case test needs to
// poke an internal path. Instead, declare a single `friend class
// TiKVIOTestHook` in the controller, and centralise the test-only entry
// points here.
//
// Each fault case's *_test.cpp can `#include "lib/test_hooks.h"` and call
// `TiKVIOTestHook::invalidate_store_cache(io, storeId)` etc.
//
// New hooks land here as fault cases need them. Keep this file
// dependency-light: it must compile with TIKV defined and the controller
// header included; no other heavy headers.
//
// NOTE: The friend declaration in `ExtraTiKVController.h` is unconditional
// at compile time but the methods are guarded by TIKV so the hook only has
// real bodies in TIKV builds.

#pragma once

#ifdef TIKV

#include "inc/Core/SPANN/ExtraTiKVController.h"

#include <cstdint>
#include <string>
#include <vector>

namespace SPTAG::SPANN::test {

/// Centralised test-only access to TiKVIO internals.
///
/// The class is declared `friend` of `TiKVIO` (see ExtraTiKVController.h).
/// All methods are static; this class never needs an instance.
class TiKVIOTestHook {
public:
    // ---- Cache control ----
    static void invalidate_store_cache(TiKVIO& io, uint64_t storeId) {
        io.InvalidateStoreCache(storeId);
    }
    static void invalidate_store_cache_for_key(TiKVIO& io, const std::string& key) {
        io.InvalidateStoreCacheForKey(key);
    }

    // ---- PD member refresh ----
    static bool force_refresh_pd_members(TiKVIO& io) {
        return io.RefreshPDMembers();
    }

    // ---- Counters ----
    static uint64_t store_addr_invalidations(const TiKVIO& io) {
        return io.m_storeAddrInvalidations.load();
    }
    static uint64_t pd_member_refreshes(const TiKVIO& io) {
        return io.m_pdMemberRefreshes.load();
    }
    static uint64_t cluster_id_mismatches(const TiKVIO& io) {
        return io.m_clusterIdMismatches.load();
    }
    static uint64_t stub_pool_evictions(const TiKVIO& io) {
        return io.m_stubPoolEvictions.load();
    }

    // ---- Cache shape ----
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
};

}  // namespace SPTAG::SPANN::test

#endif  // TIKV
