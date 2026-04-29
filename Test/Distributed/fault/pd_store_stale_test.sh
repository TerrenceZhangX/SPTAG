#!/usr/bin/env bash
# Fault-case harness: pd-store-discovery-stale
#
# Brings up a single-node PD + single TiKV using docker, runs SPTAGTest
# limited to the PDStoreDiscoveryStaleTest suite, then tears down. End-to-end
# wall clock target: < 5 minutes per the fault-case spec.
#
# Usage:
#   ./pd_store_stale_test.sh                 # baseline (3 sub-cases)
#   ./pd_store_stale_test.sh --with-move     # include the store-move sub-case
#
# Env overrides:
#   BUILD_DIR        path to the cmake build dir holding Release/SPTAGTest
#   PD_IMAGE         default pingcap/pd:v8.1.0
#   TIKV_IMAGE       default pingcap/tikv:v8.1.0
#   PD_PORT          default 12379
#   TIKV_PORT        default 20160
#   TIKV_PORT2       default 20170 (used by --with-move)
#   NET_NAME         default fault-pdstale-net

set -euo pipefail

WITH_MOVE=0
if [[ "${1:-}" == "--with-move" ]]; then
    WITH_MOVE=1
fi

BUILD_DIR="${BUILD_DIR:-$HOME/workspace/sptag-ft/build/pd-store-discovery-stale}"
TEST_BIN="${BUILD_DIR}/../../wt/pd-store-discovery-stale/Release/SPTAGTest"
[[ -x "$TEST_BIN" ]] || { echo "SPTAGTest not found at $TEST_BIN"; exit 2; }

PD_IMAGE="${PD_IMAGE:-pingcap/pd:v8.1.0}"
TIKV_IMAGE="${TIKV_IMAGE:-pingcap/tikv:v8.1.0}"
PD_PORT="${PD_PORT:-12379}"
TIKV_PORT="${TIKV_PORT:-20160}"
TIKV_PORT2="${TIKV_PORT2:-20170}"
NET_NAME="${NET_NAME:-fault-pdstale-net}"

PD_NAME="pdstale-pd"
TIKV_NAME="pdstale-tikv"
TIKV_NAME2="pdstale-tikv2"

cleanup() {
    set +e
    docker rm -f "$TIKV_NAME" "$TIKV_NAME2" "$PD_NAME" >/dev/null 2>&1 || true
    docker network rm "$NET_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
docker network create "$NET_NAME" >/dev/null

HOST_IP=$(hostname -I | awk '{print $1}')

echo "[harness] starting PD on $HOST_IP:$PD_PORT"
docker run -d --name "$PD_NAME" --network "$NET_NAME" \
    -p "${PD_PORT}:2379" \
    "$PD_IMAGE" \
    --name=pd1 \
    --client-urls=http://0.0.0.0:2379 \
    --advertise-client-urls=http://${PD_NAME}:2379 \
    --peer-urls=http://0.0.0.0:2380 \
    --advertise-peer-urls=http://${PD_NAME}:2380 \
    --initial-cluster=pd1=http://${PD_NAME}:2380 >/dev/null

echo "[harness] starting TiKV on $HOST_IP:$TIKV_PORT"
docker run -d --name "$TIKV_NAME" --network "$NET_NAME" \
    -p "${TIKV_PORT}:20160" \
    "$TIKV_IMAGE" \
    --pd-endpoints=${PD_NAME}:2379 \
    --addr=0.0.0.0:20160 \
    --advertise-addr=${TIKV_NAME}:20160 \
    --status-addr=0.0.0.0:20180 >/dev/null

# Wait for PD + TiKV to be ready.
echo "[harness] waiting for cluster..."
for i in $(seq 1 60); do
    if docker exec "$PD_NAME" /pd-ctl --pd=http://127.0.0.1:2379 store 2>/dev/null | grep -q "address"; then
        ready=1
        break
    fi
    sleep 1
done
[[ "${ready:-0}" == "1" ]] || { echo "[harness] cluster failed to come up"; docker logs "$PD_NAME" | tail -20; docker logs "$TIKV_NAME" | tail -20; exit 3; }

# The host reaches PD on the published port, but advertise-addr points to
# the docker hostname. Tests run on the host; we need them to talk to PD on
# the host port. We therefore run inside the docker network using a one-shot
# container. But our SPTAGTest binary lives on the host; simplest is to
# advertise to host IPs.
docker rm -f "$TIKV_NAME" "$PD_NAME" >/dev/null 2>&1
docker run -d --name "$PD_NAME" --network "$NET_NAME" \
    -p "${PD_PORT}:${PD_PORT}" \
    "$PD_IMAGE" \
    --name=pd1 \
    --client-urls=http://0.0.0.0:${PD_PORT} \
    --advertise-client-urls=http://${HOST_IP}:${PD_PORT} \
    --peer-urls=http://0.0.0.0:2380 \
    --advertise-peer-urls=http://${PD_NAME}:2380 \
    --initial-cluster=pd1=http://${PD_NAME}:2380 >/dev/null

docker run -d --name "$TIKV_NAME" --network "$NET_NAME" \
    -p "${TIKV_PORT}:${TIKV_PORT}" \
    "$TIKV_IMAGE" \
    --pd-endpoints=${HOST_IP}:${PD_PORT} \
    --addr=0.0.0.0:${TIKV_PORT} \
    --advertise-addr=${HOST_IP}:${TIKV_PORT} \
    --status-addr=0.0.0.0:20180 >/dev/null

echo "[harness] waiting for TiKV store registration..."
for i in $(seq 1 90); do
    out=$(docker exec "$PD_NAME" /pd-ctl -u http://127.0.0.1:${PD_PORT} store 2>/dev/null || true)
    if echo "$out" | grep -q '"state_name": "Up"'; then
        ready=1
        break
    fi
    sleep 1
done
[[ "${ready:-0}" == "1" ]] || { echo "[harness] TiKV did not register"; docker logs "$TIKV_NAME" | tail -30; exit 4; }
echo "[harness] cluster ready"

export TIKV_PD_ADDRESSES="${HOST_IP}:${PD_PORT}"
# Short TTL so the cache-refresh path runs inside the test budget.
export SPTAG_TIKV_STORE_ADDR_TTL_SEC=2
export SPTAG_TIKV_PD_REFRESH_SEC=5

if [[ "$WITH_MOVE" == "1" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    cat >/tmp/pd_store_stale_move.sh <<EOF
#!/bin/bash
set -e
docker rm -f ${TIKV_NAME} >/dev/null 2>&1 || true
sleep 1
docker run -d --name ${TIKV_NAME2} --network ${NET_NAME} \\
    -p ${TIKV_PORT2}:${TIKV_PORT2} \\
    ${TIKV_IMAGE} \\
    --pd-endpoints=${HOST_IP}:${PD_PORT} \\
    --addr=0.0.0.0:${TIKV_PORT2} \\
    --advertise-addr=${HOST_IP}:${TIKV_PORT2} \\
    --status-addr=0.0.0.0:20181 >/dev/null
for i in \$(seq 1 60); do
    out=\$(docker exec ${PD_NAME} /pd-ctl -u http://127.0.0.1:${PD_PORT} store 2>/dev/null || true)
    if echo "\$out" | grep -q "${HOST_IP}:${TIKV_PORT2}"; then exit 0; fi
    sleep 1
done
exit 1
EOF
    chmod +x /tmp/pd_store_stale_move.sh
    export TIKV_STORE_RESTART_CMD=/tmp/pd_store_stale_move.sh
fi

echo "[harness] running test..."
"$TEST_BIN" --run_test=PDStoreDiscoveryStaleTest --log_level=test_suite --report_level=short
rc=$?
echo "[harness] test exit=$rc"
exit $rc
