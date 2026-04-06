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

### 100K (base 99,000, insert 100/batch, NumThreads=8, sub-partitioned stores)

| Nodes | Avg (vec/s) | Avg Speedup | Max (vec/s) | Max Speedup | B1 | B2 | B3 | B4 | B5 | B6 | B7 | B8 | B9 | B10 |
|-------|-------------|-------------|-------------|-------------|------|------|------|------|------|------|------|------|------|------|
| 1 | **98.1** | 1.00× | 122.8 | 1.00× | 104.9 | 98.7 | 100.1 | 111.8 | 89.5 | 122.8 | 105.0 | 85.0 | 83.2 | 80.4 |
| 2 | **192.1** | 1.96× | 228.2 | 1.86× | 179.8 | 209.3 | 206.4 | 223.5 | 154.4 | 94.2 | 226.8 | 188.0 | 210.0 | 228.2 |
| 3 | **274.9** | 2.80× | 339.2 | 2.76× | 130.3 | 286.4 | 339.2 | 302.1 | 304.2 | 232.9 | 305.4 | 255.8 | 320.8 | 271.7 |
| 6 | **302.5** | 3.08× | 380.4 | 3.10× | 290.3 | 380.4 | 373.3 | 296.5 | 300.6 | 307.0 | 263.6 | 211.3 | 316.0 | 285.6 |

### 1M (base 990,000, insert 1,000/batch, NumThreads=8, sub-partitioned stores)

| Nodes | Avg (vec/s) | Avg Speedup | Max (vec/s) | Max Speedup | B1 | B2 | B3 | B4 | B5 | B6 | B7 | B8 | B9 | B10 |
|-------|-------------|-------------|-------------|-------------|------|------|------|------|------|------|------|------|------|------|
| 1 | **140.1** | 1.00× | 160.3 | 1.00× | 142.5 | 146.5 | 150.3 | 142.8 | 148.7 | 149.9 | 110.9 | 151.2 | 98.3 | 160.3 |
| 2 | **101.9** | 0.73× | 111.3 | 0.69× | 98.9 | 111.3 | 106.9 | 81.5 | 99.2 | 106.1 | 101.6 | 105.8 | 105.6 | 102.2 |
| 3 | **149.4** | 1.07× | 173.4 | 1.08× | 127.0 | 131.7 | 142.8 | 147.8 | 168.1 | 173.4 | 140.3 | 149.7 | 160.7 | 152.0 |
| 6 | **135.6** | 0.97× | 171.3 | 1.07× | 171.3 | 147.8 | 141.7 | 125.0 | 147.0 | 139.2 | 145.3 | 75.6 | 134.5 | 128.5 |

### 10M (base 9,900,000, insert 10,000/batch) — *pending*

---

## 2. 100K Detail

### Query Before Insert

| | 1-node | | 2-node | | 3-node | | 6-node | |
|---|--------|----------|--------|----------|--------|----------|--------|----------|
| **Metric** | **Cold** | **Warm** | **Cold** | **Warm** | **Cold** | **Warm** | **Cold** | **Warm** |
| QPS | 568.8 | 593.5 | 370.6 | 444.1 | 502.3 | 665.5 | 562.0 | 573.0 |
| Mean (ms) | 27.5 | 26.4 | 41.5 | 34.4 | 30.5 | 22.8 | 14.0 | 13.4 |
| Recall@5 | 0.665 | 0.665 | 0.662 | 0.662 | 0.671 | 0.671 | 0.682 | 0.682 |

### Insert Throughput Comparison

| Batch | 1-node | 2-node | 2n× | 3-node | 3n× | 6-node | 6n× |
|-------|--------|--------|-----|--------|-----|--------|-----|
| 1 | 104.9 | 179.8 | 1.71 | 130.3 | 1.24 | 290.3 | 2.77 |
| 2 | 98.7 | 209.3 | 2.12 | 286.4 | 2.90 | 380.4 | 3.85 |
| 3 | 100.1 | 206.4 | 2.06 | 339.2 | 3.39 | 373.3 | 3.73 |
| 4 | 111.8 | 223.5 | 2.00 | 302.1 | 2.70 | 296.5 | 2.65 |
| 5 | 89.5 | 154.4 | 1.73 | 304.2 | 3.40 | 300.6 | 3.36 |
| 6 | 122.8 | 94.2 | 0.77 | 232.9 | 1.90 | 307.0 | 2.50 |
| 7 | 105.0 | 226.8 | 2.16 | 305.4 | 2.91 | 263.6 | 2.51 |
| 8 | 85.0 | 188.0 | 2.21 | 255.8 | 3.01 | 211.3 | 2.49 |
| 9 | 83.2 | 210.0 | 2.52 | 320.8 | 3.86 | 316.0 | 3.80 |
| 10 | 80.4 | 228.2 | 2.84 | 271.7 | 3.38 | 285.6 | 3.55 |
| **Avg** | **98.1** | **192.1** | **1.96** | **274.9** | **2.80** | **302.5** | **3.08** |

### Search After Insert (batch 10, warm cache)

| | 1-node | 2-node | 3-node | 6-node |
|---|--------|--------|--------|--------|
| QPS | 420.1 | 318.6 | 436.4 | 354.7 |
| P50 (ms) | 36.1 | 48.8 | 32.3 | 22.0 |
| P99 (ms) | 55.6 | 68.1 | 94.7 | 28.6 |
| Recall@5 | 0.668 | 0.658 | 0.672 | 0.680 |

---

## 3. 1M Detail

### Query Before Insert

| | 1-node | | 2-node | | 3-node | | 6-node | |
|---|--------|----------|--------|----------|--------|----------|--------|----------|
| **Metric** | **Cold** | **Warm** | **Cold** | **Warm** | **Cold** | **Warm** | **Cold** | **Warm** |
| QPS | 445.4 | 471.1 | 298.7 | 338.1 | 375.7 | 409.8 | 324.0 | 363.8 |
| Mean (ms) | 17.5 | 16.6 | 26.4 | 24.0 | 20.7 | 19.7 | 24.6 | 22.1 |
| Recall@5 | 0.448 | 0.448 | 0.438 | 0.438 | 0.426 | 0.426 | 0.446 | 0.446 |

### Insert Throughput Comparison

| Batch | 1-node | 2-node | 2n× | 3-node | 3n× | 6-node | 6n× |
|-------|--------|--------|-----|--------|-----|--------|-----|
| 1 | 142.5 | 98.9 | 0.69 | 127.0 | 0.89 | 171.3 | 1.20 |
| 2 | 146.5 | 111.3 | 0.76 | 131.7 | 0.90 | 147.8 | 1.01 |
| 3 | 150.3 | 106.9 | 0.71 | 142.8 | 0.95 | 141.7 | 0.94 |
| 4 | 142.8 | 81.5 | 0.57 | 147.8 | 1.04 | 125.0 | 0.88 |
| 5 | 148.7 | 99.2 | 0.67 | 168.1 | 1.13 | 147.0 | 0.99 |
| 6 | 149.9 | 106.1 | 0.71 | 173.4 | 1.16 | 139.2 | 0.93 |
| 7 | 110.9 | 101.6 | 0.92 | 140.3 | 1.27 | 145.3 | 1.31 |
| 8 | 151.2 | 105.8 | 0.70 | 149.7 | 0.99 | 75.6 | 0.50 |
| 9 | 98.3 | 105.6 | 1.07 | 160.7 | 1.63 | 134.5 | 1.37 |
| 10 | 160.3 | 102.2 | 0.64 | 152.0 | 0.95 | 128.5 | 0.80 |
| **Avg** | **140.1** | **101.9** | **0.73** | **149.4** | **1.07** | **135.6** | **0.97** |

### 1M Routing Statistics (per batch, driver node)

| Config | Local Appends | Remote Appends | Remote % |
|--------|--------------|----------------|----------|
| 3-node | ~775 | ~1740 | 69% |
| 6-node | ~195 | ~1045 | 84% |

## 4. 10M Detail — *pending*

---

## 5. Key Observations

1. **100k scales well**: With sub-partitioned store ownership, 100k achieves 1.96× (2-node),
   2.80× (3-node), and 3.08× (6-node). At this scale the insert batch (100 vectors) is
   small enough that compute dominates over TiKV I/O, so distributing compute across nodes
   provides near-linear scaling.

2. **1M does NOT scale**: Multi-node configurations show no throughput improvement over
   1-node (140 vec/s). 2-node drops to 102 vec/s (0.73×), 3-node is flat at 149 vec/s
   (1.07×), and 6-node at 136 vec/s (0.97×). The distributed overhead negates any
   parallelism benefit at this scale.

3. **1M bottleneck analysis**: Each vector has ~7.4 replica heads spread across all 3
   TiKV stores. Total Append work (~7400 per batch) is the same regardless of node count.
   Distributing across N nodes means each node does 1/N of the head search but then sends
   (N-1)/N of its appends as RPCs. At 6-node, 84% of appends are remote. The RPC
   overhead (serialization, TCP, HandleBatchAppendRequest spawning 16 threads per incoming
   batch) adds latency without reducing total work. Workers also call FlushRemoteAppends
   which sends RPCs back to other nodes, creating cross-node synchronization barriers.

4. **2-node store imbalance**: Round-robin 3 stores → 2 nodes gives uneven distribution.
   With sub-partitioning, each store maps to both nodes (headID % 2), but the overhead of
   routing 2/3 of appends remotely still dominates. This effect is worse at 1M where each
   RPC carries ~500+ items requiring TiKV writes.

5. **Query performance**: Recall is ~0.45 at 1M (vs 0.67 at 100k) due to SearchK=TopK=5
   being too low for the larger index. Query QPS is lower at multi-node configs because
   the build phase runs single-node (no routing), so TiKV cache is less warm.

6. **Scaling requires reducing remote append ratio**: The fundamental challenge is that
   each vector's replica heads span all TiKV stores. To achieve scaling, the system would
   need to either (a) co-locate replicas on fewer stores, (b) pipeline RPC with compute
   to hide latency, or (c) use a lightweight append handler that avoids spawning threads.

---

## 6. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Driver (Node 0)              Sub-partitioned ownership:     │
│  ┌─────────────────────────┐  Store 0 → nodes {0, 3}        │
│  │ InsertVectors()         │  Store 1 → nodes {1, 4}        │
│  │  ├─ partition vectors   │  Store 2 → nodes {2, 5}        │
│  │  ├─ local: head search  │  headID % nodesPerStore picks   │
│  │  │   + AddIndex (8 thr) │  the owner within a store       │
│  │  └─ remote: SendInsert  │                                 │
│  │     Batch to workers    │                                 │
│  └────────────┬────────────┘                                 │
│               │ TCP (InsertBatchRequest)                      │
│  ┌────────────▼────────────────────────────────────────────┐ │
│  │ Workers 1-5: head search + AddIndex + FlushRemoteAppends│ │
│  │ Each worker routes appends to the correct owner node    │ │
│  └─────────────────────────┬───────────────────────────────┘ │
│                            │                                  │
│  ┌─────────────────────────▼──────────────────────────────┐  │
│  │              Shared TiKV Cluster                        │  │
│  │  Store 1 (20161)  Store 2 (20162)  Store 3 (20163)     │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## 7. How to Reproduce

```bash
cd /mnt/nvme/SPTAG/build && make -j8 SPTAGTest
./run_scale_benchmarks.sh 100k   # ~20 min
./run_scale_benchmarks.sh 1m     # ~90 min
./run_scale_benchmarks.sh 10m    # ~8 hours
```
