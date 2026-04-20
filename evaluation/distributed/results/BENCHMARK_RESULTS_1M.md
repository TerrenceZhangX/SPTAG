# SPTAG Distributed Benchmark Results — 1M Scale

**Date:** 2026-04-18
**Dataset:** SIFT1B (ANN_SIFT1B), UInt8, 128 dimensions
**Base vectors:** 990,000 | **Insert vectors:** 10,000 (10 batches × 1,000)
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
| **Baseline Search QPS (warm)** | 770.8 | 1135.0 | **1.47x** |
| **Avg Insert Throughput (vec/s)** | 225.9 | 436.1 | **1.93x** |
| **Avg Search QPS (warm, during insert)** | 738.7 | 1107.2 | **1.50x** |
| **Avg Search Latency (warm, ms)** | 10.7 | 10.9 | ~same |

## Observations

1. **Insert throughput scales ~1.93x** with 2 nodes — near-linear scaling.
2. **Search QPS scales ~1.50x at 2 nodes** — moderate scaling.
3. **Zero TiKV splits** during insert phase — tikv.toml region-max-size=512MB prevents unnecessary region splitting at 1M scale.
