#!/bin/bash
# 2-node smoke test for the unified shared-PD topology.
#
# Brings up a 2-node unified TiKV cluster (one PD + one TiKV per host, all PDs
# in ONE Raft group, max-replicas=3 -> falls back to actual replica count for
# 2 stores; we still verify the cluster is wired correctly), runs
# verify-topology, then cleans up.
#
# Inputs:
#   $NODES   "<host1>,<host2>"   (optional; otherwise read from $CLUSTER_CONF)
#   $CLUSTER_CONF  path to cluster.conf (default: configs/cluster.conf)
#
# This script is authored but NOT run by default. It is designed for CI / a
# dedicated 2-node smoke rig. DO NOT execute on production hardware without
# review.
#
# Usage:
#   NODES="10.0.0.1,10.0.0.2" ./smoke_unified_pd_2node.sh
#   CLUSTER_CONF=/path/to/cluster.conf ./smoke_unified_pd_2node.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN="$SCRIPT_DIR/run_distributed.sh"
CLUSTER_CONF="${CLUSTER_CONF:-$SCRIPT_DIR/configs/cluster.conf}"

if [ -n "${NODES:-}" ]; then
    # Build a throwaway cluster.conf from $NODES env var.
    TMP_CONF="$(mktemp -t cluster.conf.smoke.XXXXXX)"
    trap 'rm -f "$TMP_CONF"' EXIT

    IFS=',' read -ra HOSTS <<< "$NODES"
    if [ "${#HOSTS[@]}" -ne 2 ]; then
        echo "ERROR: NODES must contain exactly 2 comma-separated hosts (got ${#HOSTS[@]})" >&2
        exit 1
    fi

    cat > "$TMP_CONF" <<EOF
[cluster]
ssh_user=${SSH_USER:-$(whoami)}
sptag_dir=${SPTAG_DIR:-/tmp/sptag-smoke}
data_dir=${DATA_DIR:-/tmp/sptag-smoke-data}
tikv_version=v8.5.1
pd_version=v8.5.1
ssh_key=${SSH_KEY:-~/.ssh/id_ed25519}

[nodes]
${HOSTS[0]}    30001
${HOSTS[1]}    30002

[tikv]
${HOSTS[0]}    2379    2380    20161
${HOSTS[1]}    2379    2380    20161
EOF
    CLUSTER_CONF="$TMP_CONF"
fi

if [ ! -f "$CLUSTER_CONF" ]; then
    echo "ERROR: cluster.conf not found at $CLUSTER_CONF" >&2
    echo "Provide \$NODES=host1,host2 or \$CLUSTER_CONF=/path/to/cluster.conf" >&2
    exit 1
fi

export TIKV_TOPOLOGY=unified
export TIKV_MAX_REPLICAS=3   # PD will hold the setting; with 2 stores it cannot
                             # actually replicate to 3, but the config write must
                             # succeed and verify-topology must read it back.

echo "=== smoke_unified_pd_2node: bring up unified cluster ==="
"$RUN" start-tikv "$CLUSTER_CONF" 2

echo ""
echo "=== smoke_unified_pd_2node: verify topology ==="
if ! "$RUN" verify-topology "$CLUSTER_CONF" 2; then
    echo "verify-topology FAILED, tearing down"
    "$RUN" stop-tikv "$CLUSTER_CONF" 2 || true
    exit 1
fi

echo ""
echo "=== smoke_unified_pd_2node: cleanup ==="
"$RUN" stop-tikv "$CLUSTER_CONF" 2

echo ""
echo "smoke_unified_pd_2node: PASS"
