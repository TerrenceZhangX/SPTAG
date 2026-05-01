#!/usr/bin/env bash
# Per-case test harness: compute-worker-die-mid-query
#
# Tier 1 entry-point. Runs the ComputeWorkerDieMidQueryTest BOOST suite
# both env-off (dormancy) and env-armed (defined ErrorCode + reroute).
# The wrapper is deliberately in-process -- no PD/TiKV bring-up needed
# for Tier 1.
#
# Usage:  ./compute-worker-die-mid-query_test.sh
#
# Env:
#   BUILD_DIR  cmake build dir holding Release/SPTAGTest
#              default: $HOME/workspace/sptag-ft/build/compute-worker-die-mid-query

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export FAULT_SLUG="compute-worker-die-mid-query"

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/${FAULT_SLUG}}"
WT_DIR="${WT_DIR:-$HOME/workspace/sptag-ft/wt/compute-worker-die-mid-query}"
TEST_BIN="${WT_DIR}/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || TEST_BIN="${BUILD_DIR}/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || TEST_BIN="${BUILD_DIR}/SPTAGTest"
[[ -x "$TEST_BIN" ]] || { echo "SPTAGTest not found under $BUILD_DIR"; exit 2; }

echo "[harness] env-off baseline (dormancy)"
unset SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY
"$TEST_BIN" --run_test=ComputeWorkerDieMidQueryTest/EnvOffDormancy --log_level=test_suite --report_level=short

echo "[harness] env-armed Tier 1 cases"
export SPTAG_FAULT_COMPUTE_WORKER_DIE_MID_QUERY=1
"$TEST_BIN" --run_test=ComputeWorkerDieMidQueryTest --log_level=test_suite --report_level=short
echo "[harness] test exit=$?"
