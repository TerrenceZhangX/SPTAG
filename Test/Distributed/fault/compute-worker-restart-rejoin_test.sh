#!/usr/bin/env bash
# Per-case test harness: compute-worker-restart-rejoin
#
# Tier-1 invariants for this case live entirely in
# Test/Distributed/fault/compute-worker-restart-rejoin_test.cpp and exercise
# the prim/swim-membership + prim/ring-epoch-fence headers in-process.
# No PD/TiKV/docker bring-up is required (this is not a TiKV-IO case).
#
# Per the perf-validation protocol (locked 2026-04-30 06:02 UTC):
#   * Tier 1 hard gate: this script must exit 0 with both
#     SPTAG_FAULT_COMPUTE_WORKER_RESTART_REJOIN unset (env-off) and =1
#     (env-armed). Both are run by default; the env var is observability
#     only — the rejoin invariants are unconditional, not gated.
#   * Tier 2 (1M perf triple): NOT on the hot-path strict list →
#     deferred per protocol; rationale recorded in
#     results/compute-worker-restart-rejoin/1M/STATUS.md.
#
# Usage:
#   ./compute-worker-restart-rejoin_test.sh
#
# Env overrides:
#   BUILD_DIR  path to cmake build dir holding Release/SPTAGTest
#              default: $HOME/workspace/sptag-ft/build/compute-worker-restart-rejoin

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FAULT_SLUG="compute-worker-restart-rejoin"
SUITE="ComputeWorkerRestartRejoinTest"

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/${FAULT_SLUG}}"
SRC_DIR="${SRC_DIR:-$HOME/workspace/sptag-ft/wt/${FAULT_SLUG}}"
# CMake places the linked binary under <src>/Release/SPTAGTest; fall back to
# <build>/Release/SPTAGTest and <build>/SPTAGTest for portability.
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
unset SPTAG_FAULT_COMPUTE_WORKER_RESTART_REJOIN
run_tier1 env-off

# env-armed
export SPTAG_FAULT_COMPUTE_WORKER_RESTART_REJOIN=1
run_tier1 env-armed

echo "[harness] Tier-1 PASS (env-off + env-armed) for ${FAULT_SLUG}"
