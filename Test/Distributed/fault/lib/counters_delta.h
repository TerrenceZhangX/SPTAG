// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// CountersDelta: RAII helper that snapshots a named set of TiKVIO counters
// at construction and provides ergonomic delta assertions afterwards.
//
// Usage in a fault-case test:
//
//   #include "lib/counters_delta.h"
//   ...
//   {
//       CountersDelta delta(*io);
//       ...do something that should bump invalidations...
//       delta.expect_ge("invalidations", 1);
//       delta.expect_eq("mismatches", 0);
//   }
//
// The standard counter set is the pilot's:
//   invalidations  -> TiKVIOTestHook::store_addr_invalidations
//   evictions      -> TiKVIOTestHook::stub_pool_evictions
//   refreshes      -> TiKVIOTestHook::pd_member_refreshes
//   mismatches     -> TiKVIOTestHook::cluster_id_mismatches
//
// `retries` and `fallbacks` are reserved names for future counters that the
// retry-budget / fallback work will add; they currently return 0 but we
// accept asserts against them so tests can be written ahead of impl.

#pragma once

#ifdef TIKV

#include "test_hooks.h"

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <string>
#include <unordered_map>

namespace SPTAG::SPANN::test {

class CountersDelta {
public:
    explicit CountersDelta(const TiKVIO& io) : m_io(io) {
        m_baseline = snapshot();
    }

    /// Refresh baseline (e.g. after a setup phase).
    void rebase() { m_baseline = snapshot(); }

    /// Assert delta(name) >= n.
    void expect_ge(const std::string& name, uint64_t n) const {
        uint64_t d = delta(name);
        BOOST_CHECK_MESSAGE(
            d >= n,
            "CountersDelta: " << name << " expected >= " << n
                              << ", got " << d);
    }

    /// Assert delta(name) > n (strictly).
    void expect_gt(const std::string& name, uint64_t n) const {
        uint64_t d = delta(name);
        BOOST_CHECK_MESSAGE(
            d > n,
            "CountersDelta: " << name << " expected > " << n
                              << ", got " << d);
    }

    /// Assert delta(name) == n.
    void expect_eq(const std::string& name, uint64_t n) const {
        uint64_t d = delta(name);
        BOOST_CHECK_MESSAGE(
            d == n,
            "CountersDelta: " << name << " expected == " << n
                              << ", got " << d);
    }

    /// Read the current delta without asserting.
    uint64_t delta(const std::string& name) const {
        uint64_t now = read_counter(name);
        auto it = m_baseline.find(name);
        uint64_t was = it == m_baseline.end() ? 0 : it->second;
        return now >= was ? now - was : 0;
    }

private:
    const TiKVIO& m_io;
    std::unordered_map<std::string, uint64_t> m_baseline;

    std::unordered_map<std::string, uint64_t> snapshot() const {
        return {
            {"invalidations", TiKVIOTestHook::store_addr_invalidations(m_io)},
            {"evictions",     TiKVIOTestHook::stub_pool_evictions(m_io)},
            {"refreshes",     TiKVIOTestHook::pd_member_refreshes(m_io)},
            {"mismatches",    TiKVIOTestHook::cluster_id_mismatches(m_io)},
            // Reserved names for future counters.
            {"retries",       0},
            {"fallbacks",     0},
        };
    }
    uint64_t read_counter(const std::string& name) const {
        if (name == "invalidations") return TiKVIOTestHook::store_addr_invalidations(m_io);
        if (name == "evictions")     return TiKVIOTestHook::stub_pool_evictions(m_io);
        if (name == "refreshes")     return TiKVIOTestHook::pd_member_refreshes(m_io);
        if (name == "mismatches")    return TiKVIOTestHook::cluster_id_mismatches(m_io);
        if (name == "retries")       return 0;  // reserved
        if (name == "fallbacks")     return 0;  // reserved
        BOOST_FAIL("CountersDelta: unknown counter '" << name << "'");
        return 0;
    }
};

}  // namespace SPTAG::SPANN::test

#endif  // TIKV
