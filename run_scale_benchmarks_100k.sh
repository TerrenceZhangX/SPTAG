#!/bin/bash
# No set -e: worker exit codes are non-zero by design, must not abort script

cd /home/zhangt/workspace/SPTAG
BINARY=./Release/SPTAGTest
LOGDIR=benchmark_logs
mkdir -p "$LOGDIR"

# Helper: stop a worker by touching stop file, then wait up to 15s before killing
stop_worker() {
    local stop_file=$1
    local pid=$2
    touch "$stop_file"
    for i in $(seq 1 15); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "  Worker (PID $pid) exited"
            return
        fi
        sleep 1
    done
    echo "  Worker (PID $pid) did not exit, killing..."
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

echo "=========================================="
echo " SPTAG 100K Distributed Routing Scale Test"
echo " $(date)"
echo "=========================================="

###############################################
# Phase 1: 1-node baseline (no router)
###############################################
echo ""
echo "=== Phase 1: 1-node baseline ==="
echo "Start: $(date)"

rm -rf proidx_100k_1node
mkdir -p proidx_100k_1node/spann_index

BENCHMARK_CONFIG=Test/benchmark_100k_1node.ini \
BENCHMARK_OUTPUT=output_100k_1node.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_100k_1node.log"

echo "1-node done: $(date)"

###############################################
# Phase 2: 2-node (driver n0 + worker n1)
###############################################
echo ""
echo "=== Phase 2: 2-node distributed ==="
echo "Start: $(date)"

# Step 1: Build index with n0 (Rebuild=true)
rm -rf proidx_100k_2node_n0 proidx_100k_2node_n1
mkdir -p proidx_100k_2node_n0/spann_index

BENCHMARK_CONFIG=Test/benchmark_100k_2node_n0.ini \
BENCHMARK_OUTPUT=output_100k_2node_build.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_100k_2node_build.log"

echo "2-node build done: $(date)"

# Step 2: Copy head index to n1
echo "Copying head index to n1..."
mkdir -p proidx_100k_2node_n1/spann_index
cp -r proidx_100k_2node_n0/spann_index/* proidx_100k_2node_n1/spann_index/

# Step 3: Start worker n1 in background
echo "Starting worker n1..."
rm -f worker_stop_1
BENCHMARK_CONFIG=Test/benchmark_100k_2node_n1.ini \
  $BINARY --run_test=SPFreshTest/WorkerNode \
  2>&1 | tee "$LOGDIR/benchmark_100k_2node_worker1.log" &
WORKER1_PID=$!
sleep 3

# Step 4: Run driver n0 with Rebuild=false
sed 's/^Rebuild=true/Rebuild=false/' Test/benchmark_100k_2node_n0.ini > /tmp/benchmark_100k_2node_n0_run.ini

BENCHMARK_CONFIG=/tmp/benchmark_100k_2node_n0_run.ini \
BENCHMARK_OUTPUT=output_100k_2node.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_100k_2node_driver.log"

# Step 5: Stop worker
echo "Stopping worker n1..."
stop_worker worker_stop_1 $WORKER1_PID
echo "2-node done: $(date)"

###############################################
# Phase 3: 3-node (driver n0 + worker n1 + n2)
###############################################
echo ""
echo "=== Phase 3: 3-node distributed ==="
echo "Start: $(date)"

# Step 1: Build index with n0
rm -rf proidx_100k_3node_n0 proidx_100k_3node_n1 proidx_100k_3node_n2
mkdir -p proidx_100k_3node_n0/spann_index

BENCHMARK_CONFIG=Test/benchmark_100k_3node_n0.ini \
BENCHMARK_OUTPUT=output_100k_3node_build.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_100k_3node_build.log"

echo "3-node build done: $(date)"

# Step 2: Copy head index to n1 and n2
echo "Copying head index to n1, n2..."
mkdir -p proidx_100k_3node_n1/spann_index
mkdir -p proidx_100k_3node_n2/spann_index
cp -r proidx_100k_3node_n0/spann_index/* proidx_100k_3node_n1/spann_index/
cp -r proidx_100k_3node_n0/spann_index/* proidx_100k_3node_n2/spann_index/

# Step 3: Start workers n1 and n2
echo "Starting workers n1, n2..."
rm -f worker_stop_1 worker_stop_2

BENCHMARK_CONFIG=Test/benchmark_100k_3node_n1.ini \
  $BINARY --run_test=SPFreshTest/WorkerNode \
  2>&1 | tee "$LOGDIR/benchmark_100k_3node_worker1.log" &
WORKER1_PID=$!

BENCHMARK_CONFIG=Test/benchmark_100k_3node_n2.ini \
  $BINARY --run_test=SPFreshTest/WorkerNode \
  2>&1 | tee "$LOGDIR/benchmark_100k_3node_worker2.log" &
WORKER2_PID=$!
sleep 3

# Step 4: Run driver n0 with Rebuild=false
sed 's/^Rebuild=true/Rebuild=false/' Test/benchmark_100k_3node_n0.ini > /tmp/benchmark_100k_3node_n0_run.ini

BENCHMARK_CONFIG=/tmp/benchmark_100k_3node_n0_run.ini \
BENCHMARK_OUTPUT=output_100k_3node.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_100k_3node_driver.log"

# Step 5: Stop workers
echo "Stopping workers..."
stop_worker worker_stop_1 $WORKER1_PID
stop_worker worker_stop_2 $WORKER2_PID
echo "3-node done: $(date)"

###############################################
echo ""
echo "=========================================="
echo " All 100K benchmarks complete: $(date)"
echo " Results: output_100k_1node.json"
echo "          output_100k_2node.json"
echo "          output_100k_3node.json"
echo "=========================================="
