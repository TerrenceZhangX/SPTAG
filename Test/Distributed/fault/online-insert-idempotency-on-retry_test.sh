#!/usr/bin/env bash
# Per-case test harness: online-insert-idempotency-on-retry
#
# Tier-1 evidence per notes/perf-validation-protocol.md (locked
# 2026-04-30 06:02 UTC): runs the case's Boost suite twice, once with
# the fault hook env-off and once env-armed. Both must exit zero.
#
# The case's repro tests live in
#   Test/Distributed/fault/online-insert-idempotency-on-retry_test.cpp
# and target SPTAG::SPANN::RemotePostingOps in-process — no PD/TiKV
# bringup required. We therefore do NOT source docker_pd_tikv.sh.
#
# Usage:
#   ./online-insert-idempotency-on-retry_test.sh
#
# Env overrides:
#   BUILD_DIR     path to build dir holding Release/SPTAGTest
#                 default: $HOME/workspace/sptag-ft/build/online-insert-idempotency-on-retry
#   TEST_BIN      explicit override for SPTAGTest path

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SLUG="online-insert-idempotency-on-retry"
ENV_VAR="SPTAG_FAULT_ONLINE_INSERT_IDEMPOTENCY_ON_RETRY"

DEFAULT_BUILD="$HOME/workspace/sptag-ft/build/${SLUG}"
BUILD_DIR="${BUILD_DIR:-$DEFAULT_BUILD}"

# Hunt for SPTAGTest in the conventional spots.
if [[ -z "${TEST_BIN:-}" ]]; then
    for cand in \
        "${BUILD_DIR}/Release/SPTAGTest" \
        "${BUILD_DIR}/SPTAGTest" \
        "${BUILD_DIR}/Test/SPTAGTest" \
        "$HOME/workspace/sptag-ft/wt/${SLUG}/Release/SPTAGTest"; do
        if [[ -x "$cand" ]]; then TEST_BIN="$cand"; break; fi
    done
fi

if [[ -z "${TEST_BIN:-}" || ! -x "$TEST_BIN" ]]; then
    echo "[harness] SPTAGTest binary not found. Set BUILD_DIR or TEST_BIN." >&2
    echo "[harness]   tried: ${BUILD_DIR}/Release/SPTAGTest and friends" >&2
    exit 2
fi

run_suite() {
    local mode="$1"
    local val="$2"
    echo
    echo "[harness] === pass: ${mode} (${ENV_VAR}=${val}) ==="
    env "${ENV_VAR}=${val}" \
        "$TEST_BIN" \
            --run_test=OnlineInsertIdempotencyOnRetryTest \
            --log_level=test_suite \
            --report_level=short
    echo "[harness] pass ${mode} OK"
}

run_suite "env-off"   0
run_suite "env-armed" 1

echo
echo "[harness] online-insert-idempotency-on-retry: Tier-1 PASS (env-off + env-armed)"
