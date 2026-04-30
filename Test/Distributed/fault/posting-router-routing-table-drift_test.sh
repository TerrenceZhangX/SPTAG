#!/usr/bin/env bash
# Per-case test harness scaffold: posting-router-routing-table-drift
#
# Brings up PD + TiKV, runs the SPTAGTest cases for this fault slug,
# tears down. Multi-store / chaos sub-cases gated by --with-* flags.
#
# Usage:
#   ./posting-router-routing-table-drift_test.sh [--with-<subcase>] [...]
#
# Env overrides (see lib/docker_pd_tikv.sh for full list):
#   BUILD_DIR         path to cmake build dir holding Release/SPTAGTest
#                     default: $HOME/workspace/sptag-ft/build/posting-router-routing-table-drift
#   PD_PORT           default 12379
#   TIKV_PORT         default 20160
#
# Per-case hooks: edit the EXTRA_ENV section below to add env vars the
# SPTAGTest case reads (e.g. SPTAG_TIKV_STORE_ADDR_TTL_SEC=2).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export FAULT_SLUG="posting-router-routing-table-drift"
# shellcheck disable=SC1090
source "${SCRIPT_DIR}/lib/docker_pd_tikv.sh"

WITH_FLAGS=()
for a in "$@"; do
    case "$a" in --with-*) WITH_FLAGS+=("$a") ;; esac
done

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/${FAULT_SLUG}}"
TEST_BIN="${BUILD_DIR}/../../wt/${FAULT_SLUG}/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || { echo "SPTAGTest not found at $TEST_BIN"; exit 2; }

trap pd_tikv_teardown EXIT
pd_tikv_bringup

# ── EXTRA_ENV (case-specific) ──
# Tighten timing knobs so the unit cases finish inside the spec budget.
# Customise per case; remove if irrelevant.
export SPTAG_TIKV_STORE_ADDR_TTL_SEC="${SPTAG_TIKV_STORE_ADDR_TTL_SEC:-2}"
export SPTAG_TIKV_PD_REFRESH_SEC="${SPTAG_TIKV_PD_REFRESH_SEC:-5}"

# ── Sub-case hooks: each --with-* flag should set up the env the test
#    reads, then the test will exercise the sub-case. Examples:
#
# for f in "${WITH_FLAGS[@]}"; do
#   case "$f" in
#     --with-move) export TIKV_STORE_RESTART_CMD=/tmp/${FAULT_SLUG}_move.sh ;;
#   esac
# done

# Tier 1 protocol: run env-off and env-armed; both must be 100% green.
echo "[harness] === ENV-OFF ==="
( unset SPTAG_FAULT_POSTING_ROUTER_ROUTING_TABLE_DRIFT; "$TEST_BIN" --run_test=PostingRouterRoutingTableDriftTest --log_level=test_suite --report_level=short )
echo "[harness] env-off exit=$?"
echo "[harness] === ENV-ARMED ==="
( SPTAG_FAULT_POSTING_ROUTER_ROUTING_TABLE_DRIFT=1 "$TEST_BIN" --run_test=PostingRouterRoutingTableDriftTest --log_level=test_suite --report_level=short )
echo "[harness] env-armed exit=$?"
