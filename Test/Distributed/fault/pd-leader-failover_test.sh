#!/usr/bin/env bash
# Per-case test harness: pd-leader-failover
#
# Tier 1 entry-point. Runs the PdLeaderFailoverTest BOOST suite both
# env-off (dormancy) and env-armed (leader-stepdown injection +
# reconnect + region-cache refresh). The wrapper is in-process; no
# real PD/TiKV bring-up is needed for Tier 1 (the leader-resolver and
# region-cache-refresher are injected via std::function).
#
# Usage:  ./pd-leader-failover_test.sh
#
# Env:
#   BUILD_DIR  cmake build dir holding Release/SPTAGTest
#              default: $HOME/workspace/sptag-ft/build/pd-leader-failover

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export FAULT_SLUG="pd-leader-failover"

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/${FAULT_SLUG}}"
WT_DIR="${WT_DIR:-$HOME/workspace/sptag-ft/wt/pd-leader-failover}"
TEST_BIN="${WT_DIR}/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || TEST_BIN="${BUILD_DIR}/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || TEST_BIN="${BUILD_DIR}/SPTAGTest"
[[ -x "$TEST_BIN" ]] || { echo "SPTAGTest not found under $BUILD_DIR"; exit 2; }

echo "[harness] env-off baseline (dormancy)"
unset SPTAG_FAULT_PD_LEADER_FAILOVER
"$TEST_BIN" --run_test=PdLeaderFailoverTest/EnvOffDormancy --log_level=test_suite --report_level=short

echo "[harness] env-armed Tier 1 cases"
export SPTAG_FAULT_PD_LEADER_FAILOVER=1
"$TEST_BIN" --run_test=PdLeaderFailoverTest --log_level=test_suite --report_level=short
echo "[harness] test exit=$?"
