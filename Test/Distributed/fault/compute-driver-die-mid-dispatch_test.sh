#!/usr/bin/env bash
# Per-case test harness: compute-driver-die-mid-dispatch
#
# Tier 1 protocol: run env-off then env-armed; both must be 100% green.
# This case is in-process / non hot-path — no PD/TiKV bring-up needed.
#
# Usage:
#   ./compute-driver-die-mid-dispatch_test.sh [--with-<subcase>] [...]
#
# Env overrides:
#   BUILD_DIR    path to cmake build dir holding Release/SPTAGTest
#                default: $HOME/workspace/sptag-ft/build/compute-driver-die-mid-dispatch
#   TEST_BIN     full path to SPTAGTest (overrides BUILD_DIR resolution)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FAULT_SLUG="compute-driver-die-mid-dispatch"

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/${FAULT_SLUG}}"
TEST_BIN="${TEST_BIN:-${BUILD_DIR}/../../wt/${FAULT_SLUG}/Release/SPTAGTest}"
[[ -x "$TEST_BIN" ]] || { echo "SPTAGTest not found at $TEST_BIN"; exit 2; }

echo "[harness] === ENV-OFF ==="
( unset SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH; \
  "$TEST_BIN" --run_test=ComputeDriverDieMidDispatchTest \
              --log_level=test_suite --report_level=short )
echo "[harness] env-off exit=$?"

echo "[harness] === ENV-ARMED ==="
( SPTAG_FAULT_COMPUTE_DRIVER_DIE_MID_DISPATCH=1 \
  "$TEST_BIN" --run_test=ComputeDriverDieMidDispatchTest \
              --log_level=test_suite --report_level=short )
echo "[harness] env-armed exit=$?"
