#!/usr/bin/env bash
# Per-case test harness: swim-detector-lag
#
# Tier 1 entry-point. Runs the SwimDetectorLagTest BOOST suite both env-off
# (dormancy) and env-armed (gap → typed error / no pile-up). The wrapper is
# deliberately in-process — no PD/TiKV bring-up needed for Tier 1.
#
# Usage:  ./swim-detector-lag_test.sh
#
# Env:
#   BUILD_DIR  cmake build dir holding Release/SPTAGTest
#              default: $HOME/workspace/sptag-ft/build/swim-detector-lag

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export FAULT_SLUG="swim-detector-lag"

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/${FAULT_SLUG}}"
WT_DIR="${WT_DIR:-$HOME/workspace/sptag-ft/wt/swim-detector-lag}"
TEST_BIN="${WT_DIR}/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || TEST_BIN="${BUILD_DIR}/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || TEST_BIN="${BUILD_DIR}/SPTAGTest"
[[ -x "$TEST_BIN" ]] || { echo "SPTAGTest not found under $BUILD_DIR"; exit 2; }

echo "[harness] env-off baseline (dormancy)"
unset SPTAG_FAULT_SWIM_DETECTOR_LAG
"$TEST_BIN" --run_test=SwimDetectorLagTest/EnvOffDormancy --log_level=test_suite --report_level=short

echo "[harness] env-armed Tier 1 cases"
export SPTAG_FAULT_SWIM_DETECTOR_LAG=1
"$TEST_BIN" --run_test=SwimDetectorLagTest --log_level=test_suite --report_level=short
echo "[harness] test exit=$?"
