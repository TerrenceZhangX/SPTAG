# SPTAG Distributed Insert — Scale Benchmark Results

## Overview

SPTAG SPFresh insert + query benchmark with distributed compute nodes sharing a TiKV
cluster. Each node holds a full head-index replica; the driver partitions the insert
batch across nodes for independent **head search + posting append**. After splits, head
index changes are broadcast to all peers via fire-and-forget **HeadSyncRequest** packets.

**Latest run**: with PostingRouter `AdoptRouter` fix (reuses server socket across batches,
eliminates per-batch reconnection overhead) + `InvalidatePeerConnection` retry (defensive
reconnect on send failure). Clean TiKV restart per scale/node-count phase.

---

## 1. Insert Throughput Summary

### 100K (base 99,000, insert 100/batch)

| Nodes | Avg (vec/s) | Avg Speedup | Max (vec/s) | Max Speedup | B1 | B2 | B3 | B4 | B5 | B6 | B7 | B8 | B9 | B10 |
|-------|-------------|-------------|-------------|-------------|------|------|------|------|------|------|------|------|------|------|
| 1 | **93.9** | 1.00× | 107.2 | 1.00× | 91.0 | 85.3 | 102.5 | 94.9 | 95.5 | 94.1 | 107.2 | 84.9 | 94.2 | 89.4 |
| 2 | **185.3** | 1.97× | 240.1 | 2.24× | 194.8 | 224.8 | 240.1 | 158.2 | 153.5 | 207.8 | 194.1 | 181.6 | 159.4 | 138.1 |
| 3 | **223.2** | 2.38× | 266.4 | 2.49× | 238.5 | 125.1 | 264.2 | 191.7 | 266.3 | 230.2 | 256.0 | 266.4 | 206.0 | 187.2 |

### 1M (base 990,000, insert 1,000/batch) — *running, pending update*

### 10M (base 9,900,000, insert 10,000/batch) — *pending re-run with fix*

---

## 2. 100K Detail

### Query Before Insert

| | 1-node | | 2-node | | 3-node | |
|---|--------|----------|--------|----------|--------|----------|
| **Metric** | **Cold** | **Warm** | **Cold** | **Warm** | **Cold** | **Warm** |
| QPS | 568.8 | 593.5 | 370.6 | 444.1 | 502.3 | 665.5 |
| Mean (ms) | 27.5 | 26.4 | 41.5 | 34.4 | 30.5 | 22.8 |
| P50 (ms) | 27.2 | 26.0 | 41.0 | 35.2 | 29.4 | 22.5 |
| P99 (ms) | 35.4 | 37.1 | 64.7 | 44.1 | 50.2 | 33.4 |
| Recall@5 | 0.665 | 0.665 | 0.662 | 0.662 | 0.671 | 0.671 |

### Insert Throughput Comparison

| Batch | 1-node (vec/s) | 2-node (vec/s) | 2n Speedup | 3-node (vec/s) | 3n Speedup |
|-------|----------------|----------------|------------|----------------|------------|
| 1 | 91.0 | 194.8 | 2.14× | 238.5 | 2.62× |
| 2 | 85.3 | 224.8 | 2.64× | 125.1 | 1.47× |
| 3 | 102.5 | 240.1 | 2.34× | 264.2 | 2.58× |
| 4 | 94.9 | 158.2 | 1.67× | 191.7 | 2.02× |
| 5 | 95.5 | 153.5 | 1.61× | 266.3 | 2.79× |
| 6 | 94.1 | 207.8 | 2.21× | 230.2 | 2.45× |
| 7 | 107.2 | 194.1 | 1.81× | 256.0 | 2.39× |
| 8 | 84.9 | 181.6 | 2.14× | 266.4 | 3.14× |
| 9 | 94.2 | 159.4 | 1.69× | 206.0 | 2.19× |
| 10 | 89.4 | 138.1 | 1.54× | 187.2 | 2.09× |
| **Avg** | **93.9** | **185.3** | **1.97×** | **223.2** | **2.38×** |

### Search After Insert (per batch, warm cache)

| Batch | 1n QPS | 1n P50 | 1n P99 | 1n Recall | 2n QPS | 2n P50 | 2n P99 | 2n Recall | 3n QPS | 3n P50 | 3n P99 | 3n Recall |
|-------|--------|--------|--------|-----------|--------|--------|--------|-----------|--------|--------|--------|-----------|
| 1 | 607.2 | 25.5 | 40.2 | 0.667 | 626.6 | 24.3 | 37.7 | 0.664 | 835.7 | 18.4 | 27.7 | 0.671 |
| 2 | 607.1 | 25.2 | 36.6 | 0.665 | 645.8 | 24.1 | 38.3 | 0.664 | 878.1 | 17.5 | 27.3 | 0.671 |
| 3 | 623.5 | 24.8 | 34.1 | 0.665 | 650.2 | 24.5 | 34.5 | 0.663 | 866.5 | 17.8 | 27.5 | 0.671 |
| 4 | 530.0 | 28.6 | 40.5 | 0.665 | 593.0 | 25.9 | 37.9 | 0.661 | 898.5 | 17.3 | 26.4 | 0.671 |
| 5 | 577.9 | 26.7 | 41.4 | 0.667 | 397.4 | 39.2 | 55.9 | 0.661 | 529.3 | 20.2 | 176.9 | 0.672 |
| 6 | 535.0 | 28.8 | 40.1 | 0.668 | 332.0 | 47.5 | 65.3 | 0.662 | 571.5 | 26.5 | 45.3 | 0.673 |
| 7 | 496.8 | 32.1 | 44.7 | 0.667 | 367.4 | 42.7 | 64.3 | 0.661 | 492.7 | 31.1 | 46.9 | 0.673 |
| 8 | 473.0 | 33.7 | 40.7 | 0.666 | 337.1 | 44.0 | 66.7 | 0.660 | 534.7 | 28.2 | 51.3 | 0.673 |
| 9 | 435.0 | 34.8 | 55.6 | 0.667 | 298.7 | 54.3 | 68.8 | 0.658 | 695.9 | 21.9 | 35.3 | 0.672 |
| 10 | 420.1 | 36.1 | 55.6 | 0.668 | 318.6 | 48.8 | 68.1 | 0.658 | 436.4 | 32.3 | 94.7 | 0.672 |

### Search After Insert (per batch, cold cache)

| Batch | 1n QPS | 1n P50 | 1n P99 | 1n Recall | 2n QPS | 2n P50 | 2n P99 | 2n Recall | 3n QPS | 3n P50 | 3n P99 | 3n Recall |
|-------|--------|--------|--------|-----------|--------|--------|--------|-----------|--------|--------|--------|-----------|
| 1 | 541.5 | 28.2 | 41.5 | 0.667 | 654.7 | 23.2 | 38.9 | 0.664 | 800.6 | 19.1 | 33.1 | 0.671 |
| 2 | 639.1 | 23.8 | 38.9 | 0.665 | 631.8 | 24.3 | 34.9 | 0.664 | 884.9 | 17.4 | 24.8 | 0.671 |
| 3 | 584.7 | 26.1 | 42.9 | 0.665 | 667.6 | 23.1 | 42.2 | 0.663 | 897.6 | 17.1 | 29.5 | 0.671 |
| 4 | 554.1 | 27.6 | 46.3 | 0.665 | 631.3 | 23.5 | 65.8 | 0.661 | 868.8 | 17.8 | 25.2 | 0.671 |
| 5 | 598.1 | 24.9 | 46.0 | 0.667 | 463.2 | 33.4 | 45.5 | 0.661 | 897.5 | 17.2 | 26.2 | 0.672 |
| 6 | 570.8 | 26.8 | 38.3 | 0.668 | 351.5 | 43.6 | 63.9 | 0.662 | 589.0 | 26.4 | 35.7 | 0.673 |
| 7 | 528.7 | 28.7 | 40.9 | 0.667 | 363.3 | 41.8 | 58.0 | 0.661 | 561.7 | 26.4 | 46.6 | 0.673 |
| 8 | 569.2 | 27.7 | 34.8 | 0.666 | 362.3 | 42.6 | 62.2 | 0.660 | 587.6 | 25.7 | 65.1 | 0.673 |
| 9 | 457.6 | 33.0 | 54.5 | 0.667 | 307.0 | 50.4 | 76.6 | 0.658 | 616.4 | 23.9 | 43.1 | 0.672 |
| 10 | 466.6 | 32.8 | 50.3 | 0.668 | 301.5 | 51.1 | 76.2 | 0.658 | 616.8 | 23.8 | 43.8 | 0.672 |

---

## 3. 1M Detail — *pending re-run with AdoptRouter fix*

## 4. 10M Detail — *pending re-run with AdoptRouter fix*

---

## 5. Key Observations

1. **100k: ~2× scaling** — 1.97× avg at 2-node, 2.38× at 3-node. The previous run
   showed inflated 3-node numbers (2.99×) because the PostingRouter reconnection bug
   silently dropped ~50% of worker→driver remote appends from batch 2 onwards. With
   the `AdoptRouter` fix, all appends are properly delivered, giving true scaling numbers.

2. **3-node sub-linear scaling**: With 3 TiKV stores round-robin mapped to 3 nodes,
   each vector's ~8 replica heads span all 3 stores. Each node must send ~2/3 of its
   appends to remote nodes (double-hop problem). This overhead limits 3-node scaling
   to ~2.4× rather than the theoretical 3×.

3. **Query performance stable**: Recall values consistent across node counts (±0.01).
   3-node shows better search QPS (800–900) than 1-node (450–600) due to TiKV cache
   warming from the build phase across all stores.

4. **1M and 10M**: Previous results were collected with the PostingRouter reconnection
   bug (worker→driver appends silently lost from batch 2+). Re-run pending with the
   `AdoptRouter` fix to get accurate scaling numbers.

---

## 6. Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Driver (Node 0)                                         │
│  ┌─────────────────────────────┐                         │
│  │ InsertVectors()             │                         │
│  │  ├─ partition vectors       │                         │
│  │  ├─ local: head search +    │                         │
│  │  │         AddIndex (16 thr)│                         │
│  │  └─ remote: SendInsertBatch │                         │
│  └────────────┬────────────────┘                         │
│               │ TCP (InsertBatchRequest)                  │
│  ┌────────────▼────────────────┐  ┌────────────────────┐ │
│  │ Worker 1                    │  │ Worker 2           │ │
│  │  head search + AddIndex     │  │  head search +     │ │
│  │  (16 threads)               │  │  AddIndex (16 thr) │ │
│  └──────┬──────────────────────┘  └──────┬─────────────┘ │
│         │  HeadSyncRequest (broadcast)   │               │
│         └────────────────────────────────┘               │
│                         │                                │
│  ┌──────────────────────▼──────────────────────────────┐ │
│  │              Shared TiKV Cluster                     │ │
│  │  Store 1 (20161)  Store 2 (20162)  Store 3 (20163)  │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

---

## 7. How to Reproduce

```bash
cd /mnt/nvme/SPTAG/build && make -j8 SPTAGTest
./run_scale_benchmarks.sh 100k   # ~20 min
./run_scale_benchmarks.sh 1m     # ~90 min
./run_scale_benchmarks.sh 10m    # ~8 hours
```
