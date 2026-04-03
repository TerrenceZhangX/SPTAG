#!/bin/bash
set -e

cd /home/zhangt/workspace/SPTAG
BINARY=./Release/SPTAGTest
LOGDIR=benchmark_logs
mkdir -p "$LOGDIR"

echo "=========================================="
echo " SPTAG Distributed Routing Scale Benchmark"
echo " $(date)"
echo "=========================================="

###############################################
# Phase 1: 1-node baseline (no router)
###############################################
echo ""
echo "=== Phase 1: 1-node baseline ==="
echo "Start: $(date)"

mkdir -p proidx_1m_1node/spann_index

BENCHMARK_CONFIG=Test/benchmark_1m_1node.ini \
BENCHMARK_OUTPUT=output_1m_1node.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_1m_1node.log"

echo "1-node done: $(date)"

###############################################
# Phase 2: 2-node (driver n0 + worker n1)
###############################################
echo ""
echo "=== Phase 2: 2-node distributed ==="
echo "Start: $(date)"

# Step 1: Build index with n0 (Rebuild=true)
mkdir -p proidx_1m_2node_n0/spann_index

BENCHMARK_CONFIG=Test/benchmark_1m_2node_n0.ini \
BENCHMARK_OUTPUT=output_1m_2node_build.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_1m_2node_build.log"

echo "2-node build done: $(date)"

# Step 2: Copy head index to n1
echo "Copying head index to n1..."
mkdir -p proidx_1m_2node_n1/spann_index
cp -r proidx_1m_2node_n0/spann_index/* proidx_1m_2node_n1/spann_index/

# Step 3: Start worker n1 in background
echo "Starting worker n1..."
rm -f worker_stop_1
BENCHMARK_CONFIG=Test/benchmark_1m_2node_n1.ini \
  $BINARY --run_test=SPFreshTest/WorkerNode \
  2>&1 | tee "$LOGDIR/benchmark_1m_2node_worker1.log" &
WORKER1_PID=$!
sleep 3

# Step 4: Run driver n0 with Rebuild=false (insert + query only)
# Create a temp config for n0 with Rebuild=false
sed 's/^Rebuild=true/Rebuild=false/' Test/benchmark_1m_2node_n0.ini > /tmp/benchmark_1m_2node_n0_run.ini

BENCHMARK_CONFIG=/tmp/benchmark_1m_2node_n0_run.ini \
BENCHMARK_OUTPUT=output_1m_2node.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_1m_2node_driver.log"

# Step 5: Stop worker
echo "Stopping worker n1..."
touch worker_stop_1
wait $WORKER1_PID 2>/dev/null || true
echo "2-node done: $(date)"

###############################################
# Phase 3: 3-node (driver n0 + worker n1 + n2)
###############################################
echo ""
echo "=== Phase 3: 3-node distributed ==="
echo "Start: $(date)"

# Step 1: Build index with n0
mkdir -p proidx_1m_3node_n0/spann_index

BENCHMARK_CONFIG=Test/benchmark_1m_3node_n0.ini \
BENCHMARK_OUTPUT=output_1m_3node_build.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_1m_3node_build.log"

echo "3-node build done: $(date)"

# Step 2: Copy head index to n1 and n2
echo "Copying head index to n1, n2..."
mkdir -p proidx_1m_3node_n1/spann_index
mkdir -p proidx_1m_3node_n2/spann_index
cp -r proidx_1m_3node_n0/spann_index/* proidx_1m_3node_n1/spann_index/
cp -r proidx_1m_3node_n0/spann_index/* proidx_1m_3node_n2/spann_index/

# Step 3: Start workers n1 and n2
echo "Starting workers n1, n2..."
rm -f worker_stop_1 worker_stop_2

BENCHMARK_CONFIG=Test/benchmark_1m_3node_n1.ini \
  $BINARY --run_test=SPFreshTest/WorkerNode \
  2>&1 | tee "$LOGDIR/benchmark_1m_3node_worker1.log" &
WORKER1_PID=$!

BENCHMARK_CONFIG=Test/benchmark_1m_3node_n2.ini \
  $BINARY --run_test=SPFreshTest/WorkerNode \
  2>&1 | tee "$LOGDIR/benchmark_1m_3node_worker2.log" &
WORKER2_PID=$!
sleep 3

# Step 4: Run driver n0 with Rebuild=false
sed 's/^Rebuild=true/Rebuild=false/' Test/benchmark_1m_3node_n0.ini > /tmp/benchmark_1m_3node_n0_run.ini

BENCHMARK_CONFIG=/tmp/benchmark_1m_3node_n0_run.ini \
BENCHMARK_OUTPUT=output_1m_3node.json \
  $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
  2>&1 | tee "$LOGDIR/benchmark_1m_3node_driver.log"

# Step 5: Stop workers
echo "Stopping workers..."
touch worker_stop_1 worker_stop_2
wait $WORKER1_PID 2>/dev/null || true
wait $WORKER2_PID 2>/dev/null || true
echo "3-node done: $(date)"

###############################################
echo ""
echo "=========================================="
echo " All benchmarks complete: $(date)"
echo " Results: output_1m_1node.json"
echo "          output_1m_2node.json"
echo "          output_1m_3node.json"
echo "=========================================="
