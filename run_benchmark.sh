#!/bin/bash
set -e

# Usage: ./run_benchmark.sh <config_ini> [output_json]
# Example:
#   ./run_benchmark.sh Test/benchmark_100k_1node.ini
#   ./run_benchmark.sh Test/benchmark_1m_l2_batchget.ini output_1m.json
#   ./run_benchmark.sh Test/benchmark_10m_l2_batchget.ini output_10m.json

SPTAG_DIR="$(cd "$(dirname "$0")" && pwd)"
RELEASE_DIR="$SPTAG_DIR/Release"
TIKV_DIR="$SPTAG_DIR/docker/tikv"

if [ -z "$1" ]; then
    echo "Usage: $0 <config_ini> [output_json]"
    echo ""
    echo "Available configs:"
    ls -1 "$SPTAG_DIR/Test/benchmark_*.ini" 2>/dev/null | while read f; do
        base=$(basename "$f")
        vectors=$(grep BaseVectorCount "$f" | cut -d= -f2)
        echo "  $base  (base=$vectors)"
    done
    exit 1
fi

CONFIG="$SPTAG_DIR/$1"
if [ ! -f "$CONFIG" ]; then
    CONFIG="$1"
fi
if [ ! -f "$CONFIG" ]; then
    echo "ERROR: Config not found: $1"
    exit 1
fi

# Extract scale name from config filename
SCALE=$(basename "$CONFIG" .ini | sed 's/benchmark_//')
OUTPUT_JSON="${2:-output_${SCALE}.json}"
LOG="$RELEASE_DIR/benchmark_${SCALE}_$(date +%Y%m%d_%H%M%S).log"

# Extract IndexPath from config
INDEX_PATH=$(grep "^IndexPath=" "$CONFIG" | cut -d= -f2 | sed 's|/spann_index||')
TRUTH_PATH=$(grep "^TruthPath=" "$CONFIG" | cut -d= -f2)

echo "============================================"
echo "  SPTAG Benchmark: $SCALE"
echo "============================================"
echo "Config:  $CONFIG"
echo "Output:  $RELEASE_DIR/$OUTPUT_JSON"
echo "Log:     $LOG"
echo ""

# 1. Kill old benchmark
echo "[1/5] Killing old SPTAGTest..."
pkill -f SPTAGTest 2>/dev/null || true
sleep 1

# 2. Stop TiKV, clean data, restart
echo "[2/5] Restarting TiKV cluster (clean data)..."
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
echo "TiKV cluster ready: $STORES stores (v8.5.5)"

# 3. Clean old index
echo "[3/5] Cleaning old index..."
cd "$RELEASE_DIR"
rm -rf "$INDEX_PATH" "$TRUTH_PATH" "$OUTPUT_JSON"
mkdir -p "$INDEX_PATH/spann_index"

# 4. Show config
echo "[4/5] Config:"
cat "$CONFIG"
echo ""

# 5. Run benchmark
echo "[5/5] Running benchmark (this may take a while)..."
echo "  100K: ~5 min, 1M: ~16 min, 10M: ~3 hours"
echo ""
BENCHMARK_CONFIG="$CONFIG" \
BENCHMARK_OUTPUT="$OUTPUT_JSON" \
./SPTAGTest --run_test=SPFreshTest/BenchmarkFromConfig --log_level=message 2>&1 | tee "$LOG"

echo ""
echo "============================================"
echo "  Benchmark Complete: $SCALE"
echo "============================================"
echo "Log:  $LOG"
echo "JSON: $RELEASE_DIR/$OUTPUT_JSON"
echo ""

# Parse results
if [ -f "$RELEASE_DIR/$OUTPUT_JSON" ]; then
    python3 -c "
import json, sys
with open('$RELEASE_DIR/$OUTPUT_JSON') as f:
    data = json.load(f)
r = data['results']

print('=== Query Before Insert (warm cache) ===')
for key in ['benchmark0b_query_before_insert_round2']:
    if key in r:
        v = r[key]
        print(f'  Mean: {v[\"meanLatency\"]:.2f}ms  P50: {v[\"p50\"]:.2f}ms  P99: {v[\"p99\"]:.2f}ms  QPS: {v[\"qps\"]:.0f}  Recall@5: {v[\"recall\"][\"recallAtK\"]:.3f}')
print()

print('=== Insert Batches ===')
print(f'{\"Batch\":<8} {\"Insert vec/s\":>12} {\"MeanLat\":>8} {\"P99\":>8} {\"QPS\":>6} {\"Recall\":>7}')
print('-' * 55)
for i in range(1, 11):
    bk = f'batch_{i}'
    if bk in r.get('benchmark1_insert', {}):
        bv = r['benchmark1_insert'][bk]
        s = bv.get('search_round2', bv.get('search_round1', {}))
        tp = bv.get('insert throughput', 0)
        print(f'{bk:<8} {tp:>12.1f} {s.get(\"meanLatency\",0):>7.2f}ms {s.get(\"p99\",0):>7.2f}ms {s.get(\"qps\",0):>5.0f} {s.get(\"recall\",{}).get(\"recallAtK\",0):>7.3f}')
"
fi
