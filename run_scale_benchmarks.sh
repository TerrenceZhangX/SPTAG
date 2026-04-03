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
mkdir -p "$LOGDIR"

# Cleanup on script exit/interrupt to prevent orphan processes
trap 'echo ""; echo "Interrupted, cleaning up..."; pkill -9 -f "SPTAGTest.*WorkerNode" 2>/dev/null; pkill -9 -f "SPTAGTest.*BenchmarkFromConfig" 2>/dev/null; exit 1' INT TERM

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
    docker compose down 2>&1
    docker run --rm -v /mnt/nvme/tikv:/data alpine sh -c "rm -rf /data/*"
    docker compose up -d 2>&1
    echo "Waiting for TiKV to be ready..."
    for i in $(seq 1 30); do
        STORES=$(curl -s http://127.0.0.1:23791/pd/api/v1/stores 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])" 2>/dev/null || echo "0")
        if [ "$STORES" = "3" ]; then
            break
        fi
        sleep 2
    done
    if [ "$STORES" != "3" ]; then
        echo "ERROR: Expected 3 TiKV stores, got $STORES"
        exit 1
    fi
    echo "TiKV cluster ready: $STORES stores"
    cd "$SPTAG_DIR"
}

# ─── Phase Functions ───

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

    rm -rf "proidx_${SCALE}_1node" "truth_${SCALE}_1node" "output_${SCALE}_1node.json"
    mkdir -p "proidx_${SCALE}_1node/spann_index"

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
    rm -rf "proidx_${SCALE}_2node_n0" "proidx_${SCALE}_2node_n1" "truth_${SCALE}_2node" "output_${SCALE}_2node"*.json
    mkdir -p "proidx_${SCALE}_2node_n0/spann_index"

    # Disable router during build: worker isn't running yet
    sed 's/^RouterEnabled=true/RouterEnabled=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_2node_n0_build.ini"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_2node_n0_build.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_2node_build.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_2node_build.log"

    echo "${SCALE} 2-node build done: $(date)"

    # Copy head index to n1
    echo "Copying head index to n1..."
    mkdir -p "proidx_${SCALE}_2node_n1/spann_index"
    cp -r "proidx_${SCALE}_2node_n0/spann_index/"* "proidx_${SCALE}_2node_n1/spann_index/"

    # Start worker n1
    echo "Starting worker n1..."
    BENCHMARK_CONFIG="$INI_N1" \
      $BINARY --run_test=SPFreshTest/WorkerNode \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_2node_worker1.log" &
    WORKER1_PID=$!
    sleep 8

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
    rm -rf "proidx_${SCALE}_3node_n0" "proidx_${SCALE}_3node_n1" "proidx_${SCALE}_3node_n2" "truth_${SCALE}_3node" "output_${SCALE}_3node"*.json
    mkdir -p "proidx_${SCALE}_3node_n0/spann_index"

    # Disable router during build: workers aren't running yet
    sed 's/^RouterEnabled=true/RouterEnabled=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_3node_n0_build.ini"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_3node_n0_build.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_3node_build.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_3node_build.log"

    echo "${SCALE} 3-node build done: $(date)"

    # Copy head index to n1, n2
    echo "Copying head index to n1, n2..."
    mkdir -p "proidx_${SCALE}_3node_n1/spann_index"
    mkdir -p "proidx_${SCALE}_3node_n2/spann_index"
    cp -r "proidx_${SCALE}_3node_n0/spann_index/"* "proidx_${SCALE}_3node_n1/spann_index/"
    cp -r "proidx_${SCALE}_3node_n0/spann_index/"* "proidx_${SCALE}_3node_n2/spann_index/"

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
    sleep 8

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
echo "=========================================="
echo " ALL DONE: ${SCALES[*]}"
echo " $(date)"
echo "=========================================="
