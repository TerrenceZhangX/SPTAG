#!/usr/bin/env bash
# Per-case test harness: headsync-ordering-violation
#
# Drives the Boost suite HeadsyncOrderingViolationTest under both
# env-off and env-armed per the 2026-04-30 perf-validation protocol
# Tier 1 contract:
#   - SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION unset -> gate dormant.
#   - SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION=1     -> gate armed.
#
# This case is a pure in-memory primitive test: no PD, no TiKV, no
# docker. The gate (HeadsyncOrderingGate.h) wraps prim/headsync-pull-rpc's
# HeadSyncCursor and the only dependency is the Boost.Test runner.
#
# Usage:
#   ./headsync-ordering-violation_test.sh [--with-<subcase>] [...]
#
# Env overrides:
#   BUILD_DIR  path to cmake build dir holding Release/SPTAGTest
#              default: $HOME/workspace/sptag-ft/build/headsync-ordering-violation
#   TEST_BIN   override the binary path explicitly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FAULT_SLUG="headsync-ordering-violation"

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/${FAULT_SLUG}}"
TEST_BIN="${TEST_BIN:-$HOME/workspace/sptag-ft/wt/${FAULT_SLUG}/Release/SPTAGTest}"
[[ -x "$TEST_BIN" ]] || { echo "SPTAGTest not found at $TEST_BIN"; exit 2; }

run_suite() {
    local label="$1"
    echo "[harness] === ${label} ==="
    "$TEST_BIN" --run_test=HeadsyncOrderingViolationTest \
                --log_level=test_suite \
                --report_level=short
}

# Tier 1 (HARD): both invocations must be GREEN.

# env-off: gate dormant; case 1 (EnvOffDormancy) is the only case
# that asserts dormant behaviour explicitly. The remaining cases
# (which require Armed=true) skip themselves under env-off via the
# in-test ScopedEnv that arms only the case that needs it.
unset SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION
run_suite "env-off"

# env-armed: gate active; ScopedEnv inside each case overrides
# locally. The outer env serves as the "default armed" so any
# future case that omits ScopedEnv inherits Armed=true.
export SPTAG_FAULT_HEADSYNC_ORDERING_VIOLATION=1
run_suite "env-armed"

echo "[harness] both invocations GREEN"
