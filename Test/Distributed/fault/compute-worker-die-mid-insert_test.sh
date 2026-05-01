#!/usr/bin/env bash
# Per-case test harness: compute-worker-die-mid-insert
#
# Tier-1 invariants for this case live in
# Test/Distributed/fault/compute-worker-die-mid-insert_test.cpp
# and exercise prim/op-id-idempotency + prim/ring-epoch-fence +
# prim/swim-membership composed via the env-gated wrapper added in
# AnnService/inc/Core/SPANN/Distributed/RemotePostingOps.h
# (SendRemoteAppendWithKillResumeFence).
#
# Per the perf-validation protocol (locked 2026-04-30 06:02 UTC):
#   * Tier 1 hard gate: this script must exit 0 with both
#     SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT unset (env-off) and
#     =1 (env-armed). Both invocations are scripted here.
#   * Tier 2 (1M perf triple): NOT on the hot-path strict list ->
#     deferred per protocol; rationale recorded in
#     results/compute-worker-die-mid-insert/STATUS.md.
#
# Usage:
#   ./compute-worker-die-mid-insert_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FAULT_SLUG="compute-worker-die-mid-insert"
SUITE="ComputeWorkerDieMidInsertTest"

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/${FAULT_SLUG}}"
SRC_DIR="${SRC_DIR:-$HOME/workspace/sptag-ft/wt/${FAULT_SLUG}}"

if [[ -n "${TEST_BIN:-}" && -x "${TEST_BIN}" ]]; then
    :
elif [[ -x "${SRC_DIR}/Release/SPTAGTest" ]]; then
    TEST_BIN="${SRC_DIR}/Release/SPTAGTest"
elif [[ -x "${BUILD_DIR}/Release/SPTAGTest" ]]; then
    TEST_BIN="${BUILD_DIR}/Release/SPTAGTest"
elif [[ -x "${BUILD_DIR}/SPTAGTest" ]]; then
    TEST_BIN="${BUILD_DIR}/SPTAGTest"
else
    echo "[harness] SPTAGTest not found (searched ${SRC_DIR}/Release, ${BUILD_DIR}/Release, ${BUILD_DIR})"
    exit 2
fi

run_tier1() {
    local mode="$1"
    echo "[harness] Tier-1 ${mode}: SPTAGTest --run_test=${SUITE}"
    "$TEST_BIN" --run_test="${SUITE}" --log_level=test_suite --report_level=short
}

unset SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT
run_tier1 env-off

export SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_INSERT=1
run_tier1 env-armed

echo "[harness] Tier-1 PASS (env-off + env-armed) for ${FAULT_SLUG}"
