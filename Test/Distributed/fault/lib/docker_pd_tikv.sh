#!/usr/bin/env bash
# Reusable docker-based PD + TiKV bring-up / tear-down for fault-case
# repro tests. Source this from a per-case <slug>_test.sh.
#
# Public functions:
#   pd_tikv_bringup           bring up a single-node PD + 1 TiKV, wait for
#                             readiness. Exports:
#                               TIKV_PD_ADDRESSES   "<HOST_IP>:<PD_PORT>"
#                               PDTIKV_HOST_IP      detected host IP
#                               PDTIKV_PD_NAME      pd container name
#                               PDTIKV_TIKV_NAME    tikv container name
#                               PDTIKV_NET_NAME     docker network name
#   pd_tikv_addtikv N         start an additional TiKV (N=1..) on
#                             ${TIKV_PORT}+10*N, advertise on host IP.
#                             Container named ${TIKV_NAME}${N}.
#   pd_tikv_kill_store NAME   kill a tikv container by name.
#   pd_tikv_teardown          remove all known containers + network.
#                             Idempotent; safe to call from EXIT trap.
#   pd_tikv_pdctl ARGS...     run `pd-ctl <args>` against the PD.
#
# Required env (with sensible defaults):
#   FAULT_SLUG       short slug; used to namespace container names so
#                    parallel cases don't collide. Default: "fault".
#   PD_IMAGE         default pingcap/pd:v8.1.0
#   TIKV_IMAGE       default pingcap/tikv:v8.1.0
#   PD_PORT          default 12379 (host-side)
#   TIKV_PORT        default 20160 (host-side)
#
# Each case's harness should set FAULT_SLUG to its case slug *before*
# sourcing this file so containers/networks are uniquely named.

set -o pipefail

: "${FAULT_SLUG:=fault}"
: "${PD_IMAGE:=pingcap/pd:v8.1.0}"
: "${TIKV_IMAGE:=pingcap/tikv:v8.1.0}"
: "${PD_PORT:=12379}"
: "${TIKV_PORT:=20160}"

PDTIKV_NET_NAME="${FAULT_SLUG}-net"
PDTIKV_PD_NAME="${FAULT_SLUG}-pd"
PDTIKV_TIKV_NAME="${FAULT_SLUG}-tikv"

PDTIKV_HOST_IP=""

_log() { echo "[pd_tikv:${FAULT_SLUG}] $*" >&2; }

pd_tikv_pdctl() {
    docker exec "$PDTIKV_PD_NAME" /pd-ctl -u "http://127.0.0.1:${PD_PORT}" "$@"
}

pd_tikv_teardown() {
    set +e
    # Kill every container starting with "${FAULT_SLUG}-tikv" plus PD.
    local ids
    ids=$(docker ps -aq --filter "name=^${FAULT_SLUG}-")
    [[ -n "$ids" ]] && docker rm -f $ids >/dev/null 2>&1
    docker network rm "$PDTIKV_NET_NAME" >/dev/null 2>&1
    rm -f /tmp/${FAULT_SLUG}_*.sh 2>/dev/null
    return 0
}

pd_tikv_bringup() {
    pd_tikv_teardown
    docker network create "$PDTIKV_NET_NAME" >/dev/null

    PDTIKV_HOST_IP=$(hostname -I | awk '{print $1}')
    [[ -n "$PDTIKV_HOST_IP" ]] || { _log "ERROR: cannot detect host IP"; return 2; }

    _log "starting PD on $PDTIKV_HOST_IP:$PD_PORT"
    docker run -d --name "$PDTIKV_PD_NAME" --network "$PDTIKV_NET_NAME" \
        -p "${PD_PORT}:${PD_PORT}" \
        "$PD_IMAGE" \
        --name=pd1 \
        --client-urls=http://0.0.0.0:${PD_PORT} \
        --advertise-client-urls=http://${PDTIKV_HOST_IP}:${PD_PORT} \
        --peer-urls=http://0.0.0.0:2380 \
        --advertise-peer-urls=http://${PDTIKV_PD_NAME}:2380 \
        --initial-cluster=pd1=http://${PDTIKV_PD_NAME}:2380 >/dev/null \
        || { _log "ERROR: docker run pd failed"; return 3; }

    _log "starting TiKV on $PDTIKV_HOST_IP:$TIKV_PORT"
    docker run -d --name "$PDTIKV_TIKV_NAME" --network "$PDTIKV_NET_NAME" \
        -p "${TIKV_PORT}:${TIKV_PORT}" \
        "$TIKV_IMAGE" \
        --pd-endpoints=${PDTIKV_HOST_IP}:${PD_PORT} \
        --addr=0.0.0.0:${TIKV_PORT} \
        --advertise-addr=${PDTIKV_HOST_IP}:${TIKV_PORT} \
        --status-addr=0.0.0.0:20180 >/dev/null \
        || { _log "ERROR: docker run tikv failed"; return 4; }

    _log "waiting for TiKV store registration (Up)..."
    local ready=0
    for i in $(seq 1 90); do
        local out
        out=$(pd_tikv_pdctl store 2>/dev/null || true)
        if echo "$out" | grep -q '"state_name": "Up"'; then
            ready=1
            break
        fi
        sleep 1
    done
    if [[ "$ready" != "1" ]]; then
        _log "ERROR: TiKV did not register in 90s"
        docker logs "$PDTIKV_TIKV_NAME" 2>&1 | tail -30 >&2
        return 5
    fi
    _log "cluster ready"

    export TIKV_PD_ADDRESSES="${PDTIKV_HOST_IP}:${PD_PORT}"
    return 0
}

pd_tikv_addtikv() {
    local N="${1:-2}"
    local name="${PDTIKV_TIKV_NAME}${N}"
    local port=$(( TIKV_PORT + 10 * N ))
    local status=$(( 20180 + N ))
    _log "starting extra TiKV $name on ${PDTIKV_HOST_IP}:${port}"
    docker run -d --name "$name" --network "$PDTIKV_NET_NAME" \
        -p "${port}:${port}" \
        "$TIKV_IMAGE" \
        --pd-endpoints=${PDTIKV_HOST_IP}:${PD_PORT} \
        --addr=0.0.0.0:${port} \
        --advertise-addr=${PDTIKV_HOST_IP}:${port} \
        --status-addr=0.0.0.0:${status} >/dev/null \
        || { _log "ERROR: docker run $name failed"; return 1; }
    for i in $(seq 1 60); do
        if pd_tikv_pdctl store 2>/dev/null | grep -q "${PDTIKV_HOST_IP}:${port}"; then
            return 0
        fi
        sleep 1
    done
    _log "ERROR: $name did not register"
    return 2
}

pd_tikv_kill_store() {
    local name="$1"
    docker rm -f "$name" >/dev/null 2>&1
}
