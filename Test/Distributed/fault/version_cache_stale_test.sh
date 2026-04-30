#!/usr/bin/env bash
# Per-case test harness: version-cache-stale (Tier 1)
#
# Tier-1 unit repro is a Boost.Test in-process drive of the
# TiKVVersionMap LRU-vs-authoritative skew contract — no PD/TiKV
# bring-up required (uses an in-memory KeyValueIO fake). The shell
# harness exists so the case fits the protocol shape; it runs the
# SPTAGTest binary twice (env-off + env-armed) and captures both
# logs into results/<slug>/tier1/.
#
# Usage:
#   ./version_cache_stale_test.sh
#
# Env overrides:
#   BUILD_DIR  cmake build dir holding Release/SPTAGTest
#              default: $HOME/workspace/sptag-ft/build/version-cache-stale
#
# Exit 0 = both env-off and env-armed runs PASSED.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SLUG="version-cache-stale"

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/${SLUG}}"
TEST_BIN="${WT_DIR}/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || TEST_BIN="${BUILD_DIR}/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || { echo "[harness] SPTAGTest not found at $TEST_BIN"; exit 2; }

OUT_DIR="${WT_DIR}/results/${SLUG}/tier1"
mkdir -p "$OUT_DIR"

run_tier() {
    local label="$1"
    local logfile="${OUT_DIR}/${label}.log"
    echo "[harness] tier1/${label}"
    "$TEST_BIN" --run_test=VersionCacheStaleTest \
                --log_level=test_suite --report_level=short \
                > "$logfile" 2>&1
    if grep -qE "has passed with" "$logfile"; then
        echo "[harness] tier1/${label} GREEN"
        tail -4 "$logfile"
    else
        echo "[harness] tier1/${label} FAILED — see $logfile"
        tail -40 "$logfile"
        return 1
    fi
}

unset SPTAG_FAULT_VERSION_CACHE_STALE
run_tier env-off

export SPTAG_FAULT_VERSION_CACHE_STALE=1
run_tier env-armed

echo "[harness] Tier-1 PASS for ${SLUG}"
