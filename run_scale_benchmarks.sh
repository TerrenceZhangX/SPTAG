#!/bin/bash
# No set -e: worker exit codes are non-zero by design, must not abort script
#
# Usage:
#   ./run_scale_benchmarks.sh <scale> [scale2] [scale3] ...
#   ./run_scale_benchmarks.sh 100k
#   ./run_scale_benchmarks.sh 100k 1m
#   ./run_scale_benchmarks.sh 100k 1m 10m 100m
#   ./run_scale_benchmarks.sh all          # run all available scales

SPTAG_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SPTAG_DIR"
BINARY=./Release/SPTAGTest
TIKV_DIR="$SPTAG_DIR/docker/tikv"
LOGDIR=benchmark_logs
IDXROOT="/mnt/nvme_striped/zhangt"
mkdir -p "$LOGDIR"

# Cleanup on script exit/interrupt to prevent orphan processes
trap 'echo ""; echo "Interrupted, cleaning up..."; pkill -9 -f "SPTAGTest.*WorkerNode" 2>/dev/null; pkill -9 -f "SPTAGTest.*BenchmarkFromConfig" 2>/dev/null; cd "$TIKV_DIR" && docker compose down 2>/dev/null; exit 1' INT TERM

# ─── Usage ───
if [ $# -eq 0 ]; then
    echo "Usage: $0 <scale> [scale2] ..."
    echo ""
    echo "Scales: 100k, 1m, 10m, 100m, or 'all'"
    echo ""
    echo "Available configs:"
    for s in 100k 1m 10m 100m; do
        if ls Test/benchmark_${s}_*.ini &>/dev/null; then
            configs=$(ls -1 Test/benchmark_${s}_*.ini 2>/dev/null | wc -l)
            vectors=$(grep BaseVectorCount "Test/benchmark_${s}_1node.ini" 2>/dev/null | cut -d= -f2 || echo "?")
            echo "  $s  (base=$vectors, $configs configs)"
        fi
    done
    exit 1
fi

# ─── Helper Functions ───

# Helper: stop all SPTAGTest worker processes
stop_workers() {
    echo "Stopping all SPTAGTest workers..."
    pkill -9 -f "SPTAGTest.*WorkerNode" 2>/dev/null || true
    sleep 2
    if pgrep -f "SPTAGTest.*WorkerNode" >/dev/null 2>&1; then
        echo "  WARNING: Some workers still alive, retrying..."
        pkill -9 -f SPTAGTest 2>/dev/null || true
        sleep 1
    fi
    echo "  All workers stopped"
}

restart_tikv() {
    echo "Restarting TiKV cluster (clean data)..."
    pkill -f SPTAGTest 2>/dev/null || true
    sleep 1
    cd "$TIKV_DIR"
    docker compose down --remove-orphans --timeout 30 2>&1
    # Wait for all containers to fully stop
    for i in $(seq 1 10); do
        if ! docker ps --filter "name=tikv-" --format '{{.Names}}' | grep -q .; then
            break
        fi
        echo "  Waiting for containers to stop..."
        sleep 2
    done
    docker run --rm -v /mnt/nvme_striped/zhangt/tikv:/data alpine sh -c "rm -rf /data/*"
    sleep 2
    docker compose up -d 2>&1
    echo "Waiting for TiKV to be ready..."
    # Wait for all 3 PD nodes to be healthy first
    for i in $(seq 1 30); do
        PD1_OK=$(curl -sf -o /dev/null http://127.0.0.1:23791/pd/api/v1/health 2>/dev/null && echo "1" || echo "0")
        PD2_OK=$(curl -sf -o /dev/null http://127.0.0.1:23792/pd/api/v1/health 2>/dev/null && echo "1" || echo "0")
        PD3_OK=$(curl -sf -o /dev/null http://127.0.0.1:23793/pd/api/v1/health 2>/dev/null && echo "1" || echo "0")
        if [ "$PD1_OK" = "1" ] && [ "$PD2_OK" = "1" ] && [ "$PD3_OK" = "1" ]; then
            break
        fi
        sleep 2
    done
    # Wait for 3 TiKV stores to register
    for i in $(seq 1 30); do
        STORES=$(curl -s http://127.0.0.1:23791/pd/api/v1/stores 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(1 for s in d.get('stores',[]) if s.get('store',{}).get('state_name')=='Up'))" 2>/dev/null || echo "0")
        if [ "$STORES" = "3" ]; then
            break
        fi
        sleep 2
    done
    if [ "$STORES" != "3" ]; then
        echo "ERROR: Expected 3 TiKV stores Up, got $STORES"
        exit 1
    fi
    # Verify all PDs report same cluster ID
    CID1=$(curl -s http://127.0.0.1:23791/pd/api/v1/cluster 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
    CID2=$(curl -s http://127.0.0.1:23792/pd/api/v1/cluster 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
    CID3=$(curl -s http://127.0.0.1:23793/pd/api/v1/cluster 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
    if [ "$CID1" != "$CID2" ] || [ "$CID1" != "$CID3" ]; then
        echo "ERROR: PD cluster ID mismatch: $CID1 vs $CID2 vs $CID3"
        exit 1
    fi
    echo "TiKV cluster ready: $STORES stores, cluster=$CID1"
    cd "$SPTAG_DIR"
}

# Helper: rebalance TiKV region leaders evenly across stores
rebalance_tikv_leaders() {
    # Usage: rebalance_tikv_leaders [target_store_addrs...]
    # If target store addresses are given (e.g. 127.0.0.1:20161 127.0.0.1:20162),
    # only those stores get leaders; others are drained to 0.
    # If no args, balance across all stores.
    local TARGET_STORES="$*"
    echo "Rebalancing TiKV region leaders... targets=[${TARGET_STORES:-all}]"
    python3 -c "
import json, urllib.request, time, sys

target_addrs = '${TARGET_STORES}'.split() if '${TARGET_STORES}' else []

def api_get(path):
    return json.loads(urllib.request.urlopen('http://127.0.0.1:23791/pd/api/v1/' + path).read())

def api_post(path, data):
    req = urllib.request.Request('http://127.0.0.1:23791/pd/api/v1/' + path,
                                data=json.dumps(data).encode(),
                                headers={'Content-Type': 'application/json'})
    return json.loads(urllib.request.urlopen(req).read())

stores_data = api_get('stores')
store_info = {}
for s in stores_data.get('stores', []):
    si = s['store']
    if si.get('state_name') == 'Up':
        store_info[si['id']] = si['address']
store_ids = sorted(store_info.keys())
print(f'  Stores: {store_info}')

# Determine target store IDs (those that should hold leaders)
if target_addrs:
    target_ids = [sid for sid in store_ids if store_info[sid] in target_addrs]
    print(f'  Target stores (by address): {[store_info[s] for s in target_ids]}')
else:
    target_ids = list(store_ids)

if len(target_ids) < 1:
    print(f'  No target stores found, skipping')
    sys.exit(0)

regions = api_get('regions')['regions']
if not regions:
    print('  No regions')
    sys.exit(0)

from collections import Counter
leader_counts = Counter({sid: 0 for sid in store_ids})
for r in regions:
    sid = r.get('leader', {}).get('store_id')
    if sid in leader_counts: leader_counts[sid] += 1

print(f'  Before: {dict(leader_counts)} ({len(regions)} regions)')
target_per_store = len(regions) // len(target_ids)

transfers = 0
expected = dict(leader_counts)
for r in regions:
    src = r.get('leader', {}).get('store_id')
    # Transfer if src is NOT a target store, or if src has too many leaders
    if src in target_ids and expected.get(src, 0) <= target_per_store:
        continue
    dst = min(target_ids, key=lambda s: expected[s])
    if dst == src:
        continue
    if expected[dst] >= target_per_store + 1:
        continue
    peers = [p.get('store_id') for p in r.get('peers', [])]
    if dst not in peers:
        continue
    try:
        api_post('operators', {'name': 'transfer-leader', 'region_id': r['id'], 'to_store_id': dst})
        expected[src] -= 1; expected[dst] += 1; transfers += 1
    except Exception as e:
        print(f'  Transfer region {r[\"id\"]} failed: {e}')

if transfers == 0:
    print(f'  Already balanced')
    sys.exit(0)

print(f'  Expected: {expected} ({transfers} transfers)')

for attempt in range(15):
    time.sleep(2)
    regions2 = api_get('regions')['regions']
    lc2 = Counter({sid: 0 for sid in store_ids})
    for r in regions2:
        sid = r.get('leader', {}).get('store_id')
        if sid in lc2: lc2[sid] += 1
    if dict(lc2) == expected:
        print(f'  Verified after {(attempt+1)*2}s: {dict(lc2)}')
        sys.exit(0)

print(f'  Warning: after 30s actual={dict(lc2)}, expected={expected}')
" 2>&1
}

run_1node() {
    local SCALE=$1
    local INI="Test/benchmark_${SCALE}_1node.ini"
    if [ ! -f "$INI" ]; then
        echo "  SKIP 1-node: $INI not found"
        return
    fi

    echo ""
    echo "--- ${SCALE}: 1-node baseline ---"
    echo "Start: $(date)"

    restart_tikv

    rm -rf "$IDXROOT/proidx_${SCALE}_1node" "truth_${SCALE}_1node" "output_${SCALE}_1node.json"
    mkdir -p "$IDXROOT/proidx_${SCALE}_1node"

    BENCHMARK_CONFIG="$INI" \
    BENCHMARK_OUTPUT="output_${SCALE}_1node.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_1node.log"

    echo "${SCALE} 1-node done: $(date)"
}

run_2node() {
    local SCALE=$1
    local INI_N0="Test/benchmark_${SCALE}_2node_n0.ini"
    local INI_N1="Test/benchmark_${SCALE}_2node_n1.ini"
    if [ ! -f "$INI_N0" ] || [ ! -f "$INI_N1" ]; then
        echo "  SKIP 2-node: configs not found ($INI_N0, $INI_N1)"
        return
    fi

    echo ""
    echo "--- ${SCALE}: 2-node distributed ---"
    echo "Start: $(date)"

    restart_tikv

    # Build index with n0 (router disabled - worker not started yet)
    rm -rf "$IDXROOT/proidx_${SCALE}_2node_n0" "$IDXROOT/proidx_${SCALE}_2node_n1" "truth_${SCALE}_2node" "output_${SCALE}_2node"*.json
    mkdir -p "$IDXROOT/proidx_${SCALE}_2node_n0"

    # Disable router during build: worker isn't running yet
    sed 's/^RouterEnabled=true/RouterEnabled=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_2node_n0_build.ini"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_2node_n0_build.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_2node_build.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_2node_build.log"

    echo "${SCALE} 2-node build done: $(date)"
    # Only balance leaders to the 2 stores mapped by RouterNodeStores
    rebalance_tikv_leaders

    # Clear checkpoint so driver re-runs all insert batches with routing
    rm -f "$IDXROOT/proidx_${SCALE}_2node_n0/spann_index/checkpoint.txt"

    # Copy head index to n1
    echo "Copying head index to n1..."
    mkdir -p "$IDXROOT/proidx_${SCALE}_2node_n1/spann_index"
    cp -r "$IDXROOT/proidx_${SCALE}_2node_n0/spann_index/"* "$IDXROOT/proidx_${SCALE}_2node_n1/spann_index/"

    # Start worker n1
    echo "Starting worker n1..."
    BENCHMARK_CONFIG="$INI_N1" \
      $BINARY --run_test=SPFreshTest/WorkerNode \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_2node_worker1.log" &
    WORKER1_PID=$!
    sleep 12

    # Run driver n0 with Rebuild=false
    sed 's/^Rebuild=true/Rebuild=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_2node_n0_run.ini"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_2node_n0_run.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_2node.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_2node_driver.log"

    # Stop workers
    stop_workers
    echo "${SCALE} 2-node done: $(date)"
}

run_3node() {
    local SCALE=$1
    local INI_N0="Test/benchmark_${SCALE}_3node_n0.ini"
    local INI_N1="Test/benchmark_${SCALE}_3node_n1.ini"
    local INI_N2="Test/benchmark_${SCALE}_3node_n2.ini"
    if [ ! -f "$INI_N0" ] || [ ! -f "$INI_N1" ] || [ ! -f "$INI_N2" ]; then
        echo "  SKIP 3-node: configs not found"
        return
    fi

    echo ""
    echo "--- ${SCALE}: 3-node distributed ---"
    echo "Start: $(date)"

    restart_tikv

    # Build index with n0 (router disabled - workers not started yet)
    rm -rf "$IDXROOT/proidx_${SCALE}_3node_n0" "$IDXROOT/proidx_${SCALE}_3node_n1" "$IDXROOT/proidx_${SCALE}_3node_n2" "truth_${SCALE}_3node" "output_${SCALE}_3node"*.json
    mkdir -p "$IDXROOT/proidx_${SCALE}_3node_n0"

    # Disable router during build: workers aren't running yet
    sed 's/^RouterEnabled=true/RouterEnabled=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_3node_n0_build.ini"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_3node_n0_build.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_3node_build.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_3node_build.log"

    echo "${SCALE} 3-node build done: $(date)"
    rebalance_tikv_leaders

    # Clear checkpoint so driver re-runs all insert batches with routing
    rm -f "$IDXROOT/proidx_${SCALE}_3node_n0/spann_index/checkpoint.txt"

    # Copy head index to n1, n2
    echo "Copying head index to n1, n2..."
    mkdir -p "$IDXROOT/proidx_${SCALE}_3node_n1/spann_index"
    mkdir -p "$IDXROOT/proidx_${SCALE}_3node_n2/spann_index"
    cp -r "$IDXROOT/proidx_${SCALE}_3node_n0/spann_index/"* "$IDXROOT/proidx_${SCALE}_3node_n1/spann_index/"
    cp -r "$IDXROOT/proidx_${SCALE}_3node_n0/spann_index/"* "$IDXROOT/proidx_${SCALE}_3node_n2/spann_index/"

    # Start workers n1, n2
    echo "Starting workers n1, n2..."

    BENCHMARK_CONFIG="$INI_N1" \
      $BINARY --run_test=SPFreshTest/WorkerNode \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_3node_worker1.log" &
    WORKER1_PID=$!

    BENCHMARK_CONFIG="$INI_N2" \
      $BINARY --run_test=SPFreshTest/WorkerNode \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_3node_worker2.log" &
    WORKER2_PID=$!
    sleep 12

    # Run driver n0 with Rebuild=false
    sed 's/^Rebuild=true/Rebuild=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_3node_n0_run.ini"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_3node_n0_run.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_3node.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_3node_driver.log"

    # Stop workers
    stop_workers
    echo "${SCALE} 3-node done: $(date)"
}

run_6node() {
    local SCALE=$1
    local INI_N0="Test/benchmark_${SCALE}_6node_n0.ini"
    # Check all 6 ini files exist
    for i in 0 1 2 3 4 5; do
        if [ ! -f "Test/benchmark_${SCALE}_6node_n${i}.ini" ]; then
            echo "  SKIP 6-node: Test/benchmark_${SCALE}_6node_n${i}.ini not found"
            return
        fi
    done

    echo ""
    echo "--- ${SCALE}: 6-node distributed ---"
    echo "Start: $(date)"

    restart_tikv

    # Build index with n0 (router disabled)
    rm -rf "$IDXROOT/proidx_${SCALE}_6node_n"{0,1,2,3,4,5} "truth_${SCALE}_6node" "output_${SCALE}_6node"*.json
    mkdir -p "$IDXROOT/proidx_${SCALE}_6node_n0"

    sed 's/^RouterEnabled=true/RouterEnabled=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_6node_n0_build.ini"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_6node_n0_build.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_6node_build.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_6node_build.log"

    echo "${SCALE} 6-node build done: $(date)"
    rebalance_tikv_leaders

    # Clear checkpoint so driver re-runs all insert batches with routing
    rm -f "$IDXROOT/proidx_${SCALE}_6node_n0/spann_index/checkpoint.txt"

    # Copy head index to n1-n5
    echo "Copying head index to n1-n5..."
    for i in 1 2 3 4 5; do
        mkdir -p "$IDXROOT/proidx_${SCALE}_6node_n${i}/spann_index"
        cp -r "$IDXROOT/proidx_${SCALE}_6node_n0/spann_index/"* "$IDXROOT/proidx_${SCALE}_6node_n${i}/spann_index/"
    done

    # Start workers n1-n5
    echo "Starting workers n1-n5..."
    for i in 1 2 3 4 5; do
        BENCHMARK_CONFIG="Test/benchmark_${SCALE}_6node_n${i}.ini" \
          $BINARY --run_test=SPFreshTest/WorkerNode \
          2>&1 | tee "$LOGDIR/benchmark_${SCALE}_6node_worker${i}.log" &
    done
    sleep 15

    # Run driver n0 with Rebuild=false
    sed 's/^Rebuild=true/Rebuild=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_6node_n0_run.ini"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_6node_n0_run.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_6node.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_6node_driver.log"

    stop_workers
    echo "${SCALE} 6-node done: $(date)"
}

# ─── Run all phases for one scale ───

run_scale() {
    local SCALE=$1
    echo ""
    echo "=========================================="
    echo " SPTAG ${SCALE} Distributed Routing Scale Test"
    echo " $(date)"
    echo "=========================================="

    run_1node "$SCALE"
    run_2node "$SCALE"
    run_3node "$SCALE"

    echo ""
    echo "------------------------------------------"
    echo " ${SCALE} complete: $(date)"
    echo " Results:"
    for f in "output_${SCALE}_1node.json" "output_${SCALE}_2node.json" "output_${SCALE}_3node.json"; do
        if [ -f "$f" ]; then
            echo "   $f"
        fi
    done
    echo "------------------------------------------"
}

# ─── Main ───

# Expand "all" to all available scales
SCALES=()
for arg in "$@"; do
    if [ "$arg" = "all" ]; then
        for s in 100k 1m 10m 100m; do
            if ls Test/benchmark_${s}_*.ini &>/dev/null; then
                SCALES+=("$s")
            fi
        done
    else
        SCALES+=("$arg")
    fi
done

# Validate
for SCALE in "${SCALES[@]}"; do
    if ! ls Test/benchmark_${SCALE}_*.ini &>/dev/null; then
        echo "ERROR: No configs found for scale '$SCALE' (expected Test/benchmark_${SCALE}_*.ini)"
        exit 1
    fi
done

echo "=========================================="
echo " SPTAG Distributed Routing Scale Test"
echo " Scales: ${SCALES[*]}"
echo " $(date)"
echo "=========================================="

for SCALE in "${SCALES[@]}"; do
    run_scale "$SCALE"
done

echo ""
echo "Shutting down TiKV cluster..."
cd "$TIKV_DIR" && docker compose down 2>&1
cd "$SPTAG_DIR"

echo ""
echo "=========================================="
echo " ALL DONE: ${SCALES[*]}"
echo " $(date)"
echo "=========================================="
