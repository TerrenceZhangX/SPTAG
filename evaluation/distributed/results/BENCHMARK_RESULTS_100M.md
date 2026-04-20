# SPTAG Distributed Benchmark Results — 100M Scale

**Date:** 2026-04-19
**Dataset:** SIFT1B (ANN_SIFT1B), UInt8, 128 dimensions
**Base vectors:** 99,000,000 | **Insert vectors:** 1,000,000 (10 batches × 100,000)
**Queries:** 200 | **TopK:** 5 | **Distance:** L2
**Layers:** 2 | **Search threads:** 8 | **Insert threads:** 8
**TiKV Config:** tikv.toml (memory-usage-limit=300GB, block-cache=200GB, region-max-size=512MB)

## Cluster Topology

| Config | Compute Nodes | TiKV Instances | Driver Machine |
|--------|--------------|----------------|----------------|
| 1-node | 1 (10.11.0.7) | 1 (10.11.0.7) | 10.11.0.7 |
| 2-node | 2 (10.11.0.9, 10.11.0.7) | 2 (10.11.0.9, 10.11.0.7) | 10.11.0.9 |

**Note:** 1-node ran on 10.11.0.7 (6.9TB SSD) due to local disk constraints.

## Summary

| Metric | 1-Node | 2-Node | 2N Scaling |
|--------|--------|--------|------------|
| **Baseline Search QPS (warm)** | 129.4 | 235.9 | **1.82x** |
| **Avg Insert Throughput (vec/s)** | 194.3 | 280.2 | **1.44x** |
| **Avg Search QPS (warm, during insert)** | 126.8 | 236.4 | **1.86x** |
| **Avg Search Latency (warm, ms)** | 62.5 | 66.4 | ~same |
| **Build Time** | 26,519s (7.4h) | 27,393s (7.6h) | — |

**2-Node note:** Batches 1–6 ran with both workers active. Worker 1 timed out during checkpoint save
(default `WorkerTimeout=3600s`). Batches 7–10 ran with driver only, but QPS remained ~same because
TiKV data distribution (the main source of QPS scaling) was unaffected. Averages use batches 1–6
for insert throughput. The timeout has been fixed to 36000s for future runs.

## Per-Batch Insert Throughput

| Batch | 1-Node (vec/s) | 2-Node (vec/s) | 2N Ratio |
|-------|----------------|----------------|----------|
| 1 | 193.9 | 274.2 | 1.41x |
| 2 | 194.0 | 275.5 | 1.42x |
| 3 | 194.9 | 273.6 | 1.40x |
| 4 | 192.8 | 275.6 | 1.43x |
| 5 | 193.2 | 277.4 | 1.44x |
| 6 | 195.7 | 305.0 | 1.56x |
| 7 | 194.7 | 288.5 | 1.48x |
| 8 | 194.4 | 284.5 | 1.46x |
| 9 | 195.2 | 288.6 | 1.48x |
| 10 | 194.6 | 289.5 | 1.49x |

## Per-Batch Search QPS (round 2, warm, post-insert)

| Batch | 1-Node (QPS) | 2-Node (QPS) | 2N Ratio |
|-------|--------------|--------------|----------|
| 1 | 125.8 | 237.6 | 1.89x |
| 2 | 123.2 | 239.6 | 1.94x |
| 3 | 128.8 | 233.7 | 1.81x |
| 4 | 123.2 | 232.4 | 1.88x |
| 5 | 126.7 | 235.0 | 1.86x |
| 6 | 125.6 | 240.0 | 1.91x |
| 7 | 129.6 | 237.4 | 1.83x |
| 8 | 128.1 | 234.6 | 1.83x |
| 9 | 128.6 | 242.2 | 1.88x |
| 10 | 128.7 | 241.3 | 1.87x |

## Cross-Scale Comparison (1M → 10M → 100M)

| Metric | 1M 1N | 1M 2N | 10M 1N | 10M 2N | 100M 1N | 100M 2N |
|--------|-------|-------|--------|--------|---------|---------|
| Insert (vec/s) | 225.9 | 436.1 | 224.5 | 395.2 | 194.3 | 280.2 |
| Search QPS (warm) | 738.7 | 1107.2 | 242.6 | 467.2 | 126.8 | 236.4 |
| Search Latency (ms) | 10.7 | 10.9 | 32.7 | 33.5 | 62.5 | 66.4 |
| 2N Insert Scaling | — | 1.93x | — | 1.76x | — | 1.44x |
| 2N Search Scaling | — | 1.50x | — | 1.93x | — | 1.86x |

## Observations

1. **Search QPS scales ~1.86x at 2 nodes for 100M** — good scaling from TiKV data distribution.

2. **Insert throughput scaling decreases with data size**: 1M 1.93x → 10M 1.76x → 100M 1.44x.
   At larger scales, per-insert TiKV cost increases due to Read-Modify-Write pattern on growing postings.

3. **QPS unaffected by worker crash** — batches 7–10 (worker dead) show ~same QPS as batches 1–6.
   This proves the QPS improvement comes from TiKV distributing data across 2 instances, which
   remain online regardless of worker status.

4. **Search latency doubles from 10M to 100M** (~33ms → ~63ms) — expected with 10x index size.
   Consistent latency between 1-node and 2-node (~63ms vs ~66ms) shows latency is dominated
   by posting scan time, not network overhead.

5. **Insert throughput shows slight degradation at scale** — 225→194 vec/s (1-node) from 1M→100M.
   At 100M, each insert touches more/larger postings, adding overhead.

6. **Build time: 7.4h for 100M** — scales roughly linearly from 10M (40min). Build runs on a
   single driver node before distribution.

7. **TiKV disk usage: ~170GB per node** for 2-node (340GB total). The 200GB block-cache provides
   ~59% cache coverage, a significant improvement over the previous 40GB config (~12%).
