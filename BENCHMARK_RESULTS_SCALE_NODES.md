# SPTAG Distributed Insert — Scale Benchmark Results

## Overview

SPTAG SPFresh insert + query benchmark with distributed compute nodes sharing a TiKV
cluster. Each node holds a full head-index replica; the driver partitions the insert
batch across nodes for independent **head search + posting append**. After splits, head
index changes are broadcast to all peers via fire-and-forget **HeadSyncRequest** packets.

All data collected with MultiGet region error retry fix, clean TiKV restart per phase.

---

## 1. Throughput Summary (All Scales)

| Scale | Base Vectors | Insert/Batch | 1-node (vec/s) | 2-node (vec/s) | 2n Speedup | 3-node (vec/s) | 3n Speedup |
|-------|-------------|-------------|----------------|----------------|------------|----------------|------------|
| 100k | 99,000 | 100 | 103.6 | 202.9 | 1.96× | 310.1 | 2.99× |
| 1M | 990,000 | 1,000 | 142.4 | 134.5 | 0.94× | 184.2 | 1.29× |
| 10M | 9,900,000 | 10,000 | 145.8 | 147.7 | 1.01× | 306.5 | 2.10× |

---

## 2. 100k Detail

### Configuration

| Parameter | Value |
|---|---|
| Base vectors | 99,000 |
| Insert vectors / batch | 100 |
| Batches | 10 |
| PostingPageLimit | 12 |
| BufferLength | 8 |

### Query Before Insert (Baseline)

| Nodes | QPS | Mean (ms) | P50 | P99 | Recall@5 |
|-------|-----|-----------|-----|-----|----------|
| 1 | 621 | 24.6 | 24.6 | 35.5 | 0.677 |
| 2 | 373 | 40.4 | 40.4 | 56.0 | 0.666 |
| 3 | 632 | 24.0 | 22.6 | 39.2 | 0.671 |

### Per-Batch — 1 Node (avg 103.6 vec/s)

| Batch | Throughput | Recall@5 | Search2 QPS |
|-------|------------|----------|-------------|
| 1 | 109.0 | 0.679 | 686 |
| 2 | 91.3 | 0.679 | 654 |
| 3 | 86.5 | 0.677 | 637 |
| 4 | 91.7 | 0.677 | 481 |
| 5 | 116.9 | 0.677 | 545 |
| 6 | 96.9 | 0.681 | 557 |
| 7 | 117.3 | 0.681 | 548 |
| 8 | 101.5 | 0.679 | 464 |
| 9 | 128.3 | 0.679 | 501 |
| 10 | 96.8 | 0.681 | 494 |

### Per-Batch — 2 Nodes (avg 202.9 vec/s, 1.96×)

| Batch | Throughput | Recall@5 | Search2 QPS |
|-------|------------|----------|-------------|
| 1 | 183.5 | 0.665 | 560 |
| 2 | 170.3 | 0.666 | 591 |
| 3 | 248.7 | 0.665 | 613 |
| 4 | 188.2 | 0.665 | 600 |
| 5 | 238.2 | 0.665 | 593 |
| 6 | 155.6 | 0.665 | 352 |
| 7 | 190.8 | 0.666 | 357 |
| 8 | 238.7 | 0.665 | 366 |
| 9 | 193.7 | 0.665 | 353 |
| 10 | 221.7 | 0.665 | 348 |

### Per-Batch — 3 Nodes (avg 310.1 vec/s, 2.99×)

| Batch | Throughput | Recall@5 | Search2 QPS |
|-------|------------|----------|-------------|
| 1 | 166.9 | 0.672 | 894 |
| 2 | 301.3 | 0.672 | 911 |
| 3 | 348.0 | 0.671 | 948 |
| 4 | 397.3 | 0.670 | 920 |
| 5 | 364.0 | 0.671 | 949 |
| 6 | 275.3 | 0.671 | 422 |
| 7 | 247.1 | 0.671 | 536 |
| 8 | 272.9 | 0.670 | 531 |
| 9 | 348.7 | 0.671 | 484 |
| 10 | 379.8 | 0.671 | 483 |

> Recall stable across all batches and node counts. Zero anomalies.
> MultiGet region error retry triggered 6 times during 3-node — fix working as designed.

---

## 3. 1M Detail

### Configuration

| Parameter | Value |
|---|---|
| Base vectors | 990,000 |
| Insert vectors / batch | 1,000 |
| Batches | 10 |
| PostingPageLimit | 12 |
| BufferLength | 8 |

### Per-Batch — 1 Node (avg 142.4 vec/s)

| Batch | Throughput | Time (s) | Recall@5 | Search2 QPS |
|-------|------------|----------|----------|-------------|
| 1 | 152.5 | 6.56 | 0.444 | 374 |
| 2 | 171.6 | 5.83 | 0.444 | 410 |
| 3 | 155.0 | 6.45 | 0.443 | 380 |
| 4 | 158.1 | 6.33 | 0.441 | 393 |
| 5 | 140.1 | 7.14 | 0.442 | 350 |
| 6 | 121.6 | 8.22 | 0.444 | 399 |
| 7 | 146.5 | 6.83 | 0.443 | 365 |
| 8 | 140.9 | 7.10 | 0.441 | 328 |
| 9 | 93.1 | 10.75 | 0.440 | 368 |
| 10 | 144.7 | 6.91 | 0.436 | 389 |

### Per-Batch — 2 Nodes (avg 134.5 vec/s, 0.94×)

| Batch | Throughput | Time (s) | Recall@5 | Search2 QPS |
|-------|------------|----------|----------|-------------|
| 1 | 94.6 | 10.57 | 0.405 | 286 |
| 2 | 164.4 | 6.08 | 0.404 | 313 |
| 3 | 87.7 | 11.40 | 0.401 | 344 |
| 4 | 149.6 | 6.69 | 0.401 | 371 |
| 5 | 122.1 | 8.19 | 0.401 | 363 |
| 6 | 173.2 | 5.77 | 0.402 | 364 |
| 7 | 145.6 | 6.87 | 0.403 | 353 |
| 8 | 133.3 | 7.50 | 0.402 | 380 |
| 9 | 140.1 | 7.14 | 0.402 | 379 |
| 10 | 134.3 | 7.44 | 0.404 | 370 |

### Per-Batch — 3 Nodes (avg 184.2 vec/s, 1.29×)

| Batch | Throughput | Time (s) | Recall@5 | Search2 QPS |
|-------|------------|----------|----------|-------------|
| 1 | 93.3 | 10.72 | 0.428 | 283 |
| 2 | 223.7 | 4.47 | 0.428 | 326 |
| 3 | 232.3 | 4.31 | 0.429 | 272 |
| 4 | 137.4 | 7.28 | 0.428 | 356 |
| 5 | 170.1 | 5.88 | 0.429 | 390 |
| 6 | 168.7 | 5.93 | 0.429 | 408 |
| 7 | 167.9 | 5.96 | 0.428 | 355 |
| 8 | 214.8 | 4.66 | 0.426 | 374 |
| 9 | 204.9 | 4.88 | 0.427 | 334 |
| 10 | 228.7 | 4.37 | 0.426 | 345 |

> Scaling significantly weaker at 1M. 2-node shows NO speedup (0.94×).
> 3-node shows modest 1.29× gain. Suggests TiKV I/O becomes the bottleneck
> at this scale, limiting parallelism gains from additional compute nodes.
> Recall also lower in multi-node configs (1-node 0.44 vs 2-node 0.40 vs 3-node 0.43).

---

## 4. 10M Detail

### Configuration

| Parameter | Value |
|---|---|
| Base vectors | 9,900,000 |
| Insert vectors / batch | 10,000 |
| Batches | 10 |
| PostingPageLimit | 12 |
| BufferLength | 8 |

### Per-Batch — 1 Node (avg 145.8 vec/s)

| Batch | Throughput | Time (s) | Recall@5 | Search2 QPS |
|-------|------------|----------|----------|-------------|
| 1 | 155.6 | 64.3 | 0.240 | 289 |
| 2 | 133.5 | 74.9 | 0.239 | 288 |
| 3 | 150.5 | 66.4 | 0.241 | 301 |
| 4 | 152.1 | 65.7 | 0.241 | 298 |
| 5 | 139.8 | 71.5 | 0.242 | 265 |
| 6 | 151.0 | 66.2 | 0.243 | 249 |
| 7 | 122.3 | 81.8 | 0.242 | 284 |
| 8 | 152.1 | 65.7 | 0.242 | 255 |
| 9 | 146.9 | 68.1 | 0.242 | 266 |
| 10 | 154.3 | 64.8 | 0.243 | 262 |

### Per-Batch — 2 Nodes (avg 147.7 vec/s, 1.01×)

| Batch | Throughput | Time (s) | Recall@5 | Search2 QPS |
|-------|------------|----------|----------|-------------|
| 1 | 143.4 | 69.8 | 0.278 | 259 |
| 2 | 206.0 | 48.5 | 0.276 | 279 |
| 3 | 336.3 | 29.7 | 0.278 | 282 |
| 4 | 246.1 | 40.6 | 0.277 | 185 |
| 5 | 236.6 | 42.3 | 0.277 | 173 |
| 6 | 111.1 | 90.0 | 0.278 | 169 |
| 7 | **44.6** | 224.0 | 0.277 | 185 |
| 8 | **52.6** | 189.9 | 0.278 | 188 |
| 9 | **48.2** | 207.3 | 0.277 | 182 |
| 10 | **52.5** | 190.4 | 0.277 | 183 |

> ⚠️ Severe throughput degradation in batches 7–10 (~50 vec/s vs ~250 early).
> Likely TiKV contention or split cascade at 2-node scale with 10M dataset.

### Per-Batch — 3 Nodes (avg 306.5 vec/s, 2.10×)

| Batch | Throughput | Time (s) | Recall@5 | Search2 QPS |
|-------|------------|----------|----------|-------------|
| 1 | 226.7 | 44.1 | 0.250 | 269 |
| 2 | 327.8 | 30.5 | 0.249 | 229 |
| 3 | 335.9 | 29.8 | 0.251 | 187 |
| 4 | 262.1 | 38.2 | 0.251 | 169 |
| 5 | 363.2 | 27.5 | 0.252 | 174 |
| 6 | 281.2 | 35.6 | 0.251 | 171 |
| 7 | 323.2 | 30.9 | 0.251 | 172 |
| 8 | 346.6 | 28.9 | 0.252 | 164 |
| 9 | 310.7 | 32.2 | 0.251 | 164 |
| 10 | 287.8 | 34.7 | 0.253 | 165 |

> 3-node shows consistent throughput across all batches — no degradation.
> Recall stable at 0.249–0.253.

---

## 5. Key Observations

1. **100k: Near-linear scaling** — 1.96× at 2-node, 2.99× at 3-node. At this scale
   the bottleneck is head search + posting append (CPU-bound), which parallelizes well.

2. **1M: Scaling collapses** — 2-node shows no speedup (0.94×), 3-node only 1.29×.
   At 1M the bottleneck shifts to TiKV I/O (larger postings, more splits/merges).
   Adding compute nodes can't help when all nodes contend on the same TiKV cluster.

3. **10M: Mixed results** — 2-node shows no average speedup (1.01×) with severe
   late-batch degradation (batches 7–10 drop to ~50 vec/s). 3-node shows good
   scaling (2.10×) with consistent throughput. The 2-node degradation pattern
   (fine early, collapses later) suggests resource exhaustion or split cascading
   under sustained load with exactly 2 nodes.

4. **Recall stability**: Zero anomalies across all scales after MultiGet region error
   fix. Recall values differ by scale (100k: 0.67, 1M: 0.44, 10M: 0.24) due to
   index structure and posting density.

5. **1M/10M multi-node recall variation**: 2-node recall sometimes higher than 1-node
   at 10M (0.277 vs 0.242). Different TiKV data layout after distributed inserts
   may affect posting quality.

6. **Root cause of previous recall anomaly**: `TiKVIO::MultiGet` silently returned
   empty buffers on region errors. Fixed by falling back to individual `Get` calls
   with 10-retry logic.

7. **Scaling bottleneck analysis**:
   - 100k (100 vec/batch): Compute-bound → scales linearly
   - 1M (1k vec/batch): TiKV I/O starts to dominate → scaling limited
   - 10M (10k vec/batch): Split cascades dominate at 2-node; 3-node distributes
     split load more evenly, recovering scaling

---

## Architecture

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

## How to Reproduce

```bash
cd /mnt/nvme/SPTAG/build && make -j8 SPTAGTest
./run_scale_benchmarks.sh 100k   # ~20 min
./run_scale_benchmarks.sh 1m     # ~90 min
./run_scale_benchmarks.sh 10m    # ~8 hours
```
