# SPTAG Distributed Benchmark Results — 10M Scale

**Date:** 2026-04-18
**Dataset:** SIFT1B (ANN_SIFT1B), UInt8, 128 dimensions
**Base vectors:** 9,900,000 | **Insert vectors:** 100,000 (10 batches × 10,000)
**Queries:** 200 | **TopK:** 5 | **Distance:** L2
**Layers:** 2 | **Search threads:** 8 | **Insert threads:** 8
**TiKV Config:** tikv.toml (memory-usage-limit=80GB, block-cache=40GB, region-max-size=512MB)

## Cluster Topology

| Config | Compute Nodes | TiKV Instances | Driver Machine |
|--------|--------------|----------------|----------------|
| 1-node | 1 (10.11.0.9) | 1 (10.11.0.9) | 10.11.0.9 |
| 2-node | 2 (10.11.0.9, 10.11.0.7) | 2 (10.11.0.9, 10.11.0.7) | 10.11.0.9 |

## Summary

| Metric | 1-Node | 2-Node | 2N Scaling |
|--------|--------|--------|------------|
| **Baseline Search QPS (warm)** | 233.9 | 453.8 | **1.94x** |
| **Avg Insert Throughput (vec/s)** | 224.5 | 395.2 | **1.76x** |
| **Avg Search QPS (warm, during insert)** | 242.6 | 467.2 | **1.93x** |
| **Avg Search Latency (warm, ms)** | 32.7 | 33.5 | ~same |
| **Build Time** | 2384s (39.7min) | 2595s (43.2min) | — |

## Per-Batch Insert Throughput

| Batch | 1-Node (vec/s) | 2-Node (vec/s) | 2N Ratio |
|-------|----------------|----------------|----------|
| 1 | 228.7 | 400.5 | 1.75x |
| 2 | 228.3 | 409.7 | 1.79x |
| 3 | 206.8 | 369.8 | 1.79x |
| 4 | 229.2 | 377.7 | 1.65x |
| 5 | 219.7 | 400.2 | 1.82x |
| 6 | 225.6 | 413.1 | 1.83x |
| 7 | 232.8 | 402.7 | 1.73x |
| 8 | 219.7 | 413.3 | 1.88x |
| 9 | 225.0 | 375.0 | 1.67x |
| 10 | 229.5 | 389.5 | 1.70x |

## Per-Batch Search QPS (round 2, warm, post-insert)

| Batch | 1-Node (QPS) | 2-Node (QPS) | 2N Ratio |
|-------|--------------|--------------|----------|
| 1 | 241.7 | 464.1 | 1.92x |
| 2 | 251.9 | 466.1 | 1.85x |
| 3 | 243.1 | 473.3 | 1.95x |
| 4 | 236.0 | 465.4 | 1.97x |
| 5 | 244.6 | 467.6 | 1.91x |
| 6 | 235.1 | 462.2 | 1.97x |
| 7 | 244.4 | 469.8 | 1.92x |
| 8 | 238.9 | 468.5 | 1.96x |
| 9 | 239.4 | 466.4 | 1.95x |
| 10 | 251.1 | 468.4 | 1.87x |

## Observations

1. **Search QPS scales ~1.93x at 2 nodes** — near-linear.
2. **Insert throughput scales ~1.76x** — good but sub-linear, limited by TiKV Read-Modify-Write overhead.
3. **Search latency stable** (~33ms) across topologies — dominated by posting scan time.
4. **Zero TiKV splits** during all insert phases.
5. **Build time ~same across topologies** — build runs on single driver node.
6. **Consistent scaling across batches** — no degradation over time at 10M scale.
