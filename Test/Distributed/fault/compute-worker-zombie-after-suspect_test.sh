#!/usr/bin/env bash
# Per-case test harness: compute-worker-zombie-after-suspect
#
# Tier-1 invariants for this case live entirely in
# Test/Distributed/fault/compute-worker-zombie-after-suspect_test.cpp
# and exercise the prim/swim-membership + prim/ring-epoch-fence headers
# plus the env-gated zombie-fence wrapper added in
# AnnService/inc/Core/SPANN/Distributed/RemotePostingOps.h.
# No PD/TiKV/docker bring-up is required by Tier 1 (the optional TiKV
# smoke test self-skips when TIKV_PD_ADDRESSES is unset).
#
# Per the perf-validation protocol (locked 2026-04-30 06:02 UTC):
#   * Tier 1 hard gate: this script must exit 0 with both
#     SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT unset (env-off)
#     and =1 (env-armed). Both invocations are scripted here.
#   * Tier 2 (1M perf triple): NOT on the hot-path strict list →
#     deferred per protocol; rationale recorded in
#     results/compute-worker-zombie-after-suspect/STATUS.md.
#
# Usage:
#   ./compute-worker-zombie-after-suspect_test.sh
#
# Env overrides:
#   BUILD_DIR  path to cmake build dir holding Release/SPTAGTest
#              default: $HOME/workspace/sptag-ft/build/compute-worker-zombie-after-suspect

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FAULT_SLUG="compute-worker-zombie-after-suspect"
SUITE="ComputeWorkerZombieAfterSuspectTest"

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

# env-off
unset SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT
run_tier1 env-off

# env-armed
export SPTAG_FAULT_COMPUTE_WORKER_ZOMBIE_AFTER_SUSPECT=1
run_tier1 env-armed

echo "[harness] Tier-1 PASS (env-off + env-armed) for ${FAULT_SLUG}"
