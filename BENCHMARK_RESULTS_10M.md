# SPTAG/SPFresh + TiKV Benchmark Results (10M Scale)

## Configuration

- **Scale**: 10M vectors (base: 9.9M, insert: 100K = 10K per batch x 10)
- **Vector**: UInt8, dim=128, L2 distance
- **Queries**: 200 queries, TopK=5, 16 search threads
- **Index**: 2-layer SPANN BKT, PostingPageLimit=12, BufferLength=8
- **Storage**: TiKV (3 PD + 3 TiKV, Docker nightly, host network)
- **TiKV Config**: region-max-size=512MB, region-split-size=384MB, grpc-concurrency=8
- **Optimization**: RawBatchGet (region-grouped async batch RPC)
- **Build Time**: 11070.6s (~3.1 hours)

## Query Latency (ms)

| Phase | Mean | P50 | P90 | P95 | P99 | Min | Max | QPS |
|-------|------|-----|-----|-----|-----|-----|-----|-----|
| Pre-insert | 52.0 | 52.1 | 58.3 | 59.6 | 65.0 | 28.4 | 65.4 | 302 |
| Batch 1 | 46.5 | 46.4 | 54.2 | 55.7 | 58.5 | 25.1 | 62.7 | 339 |
| Batch 2 | 46.8 | 46.8 | 53.8 | 55.2 | 60.9 | 25.1 | 62.4 | 336 |
| Batch 3 | 46.7 | 46.8 | 53.9 | 56.5 | 59.2 | 24.5 | 62.4 | 337 |
| Batch 4 | 46.8 | 47.0 | 54.6 | 56.3 | 59.6 | 24.7 | 63.9 | 337 |
| Batch 5 | 46.4 | 46.4 | 54.1 | 55.5 | 59.7 | 25.1 | 63.9 | 340 |
| Batch 6 | 46.3 | 46.4 | 53.7 | 54.8 | 59.0 | 25.0 | 61.3 | 340 |
| Batch 7 | 46.9 | 47.2 | 53.9 | 56.3 | 59.3 | 25.8 | 61.5 | 336 |
| Batch 8 | 46.4 | 46.3 | 53.5 | 55.9 | 59.9 | 24.7 | 60.4 | 339 |
| Batch 9 | 46.7 | 46.7 | 54.0 | 55.7 | 59.1 | 25.5 | 63.8 | 338 |
| Batch 10 | 46.9 | 46.8 | 54.7 | 55.9 | 59.6 | 25.6 | 63.4 | 336 |

## Recall@5

| Phase | Recall@5 |
|-------|----------|
| Pre-insert | 0.6920 |
| Batch 1 | 0.6920 |
| Batch 2 | 0.6890 |
| Batch 3 | 0.6860 |
| Batch 4 | 0.6840 |
| Batch 5 | 0.6830 |
| Batch 6 | 0.6840 |
| Batch 7 | 0.6820 |
| Batch 8 | 0.6820 |
| Batch 9 | 0.6800 |
| Batch 10 | 0.6780 |

## Insert Throughput

| Batch | Throughput (vec/s) | Time (s) |
|-------|-------------------|----------|
| Batch 1 | 483.6 | 20.7 |
| Batch 2 | 508.4 | 19.7 |
| Batch 3 | 500.4 | 20.0 |
| Batch 4 | 501.5 | 19.9 |
| Batch 5 | 505.4 | 19.8 |
| Batch 6 | 506.5 | 19.7 |
| Batch 7 | 491.2 | 20.4 |
| Batch 8 | 505.8 | 19.8 |
| Batch 9 | 516.0 | 19.4 |
| Batch 10 | 510.9 | 19.6 |

## Summary

- **Avg Latency (B1-B10)**: 46.6ms
- **Avg Recall@5 (B1-B10)**: 0.6840
- **Pre-insert Latency**: 52.0ms (cold cache)
- **Steady-state Latency**: ~46.6ms (B3-B10, warmed cache)
- **Recall Degradation**: 0.6920 -> 0.6780 over 10 batches
- **Build Time**: 3.1 hours

## Cross-Scale Comparison

| Scale | Avg Latency (B1-10) | Pre-insert Latency | Recall@5 (Pre) | Recall@5 (Final) | Build Time |
|-------|--------------------|--------------------|----------------|-----------------|------------|
| 100K | 7.6ms | 7.1ms | 0.924 | 0.918 | 93s |
| 1M | 55.2ms | 68.4ms | 0.800 | 0.767 | 240s |
| 10M | 46.6ms | 52.0ms | 0.692 | 0.678 | 3.1h |

---

## Distributed Routing Scale Test (10M, Float32/dim64)

### Configuration

- **Scale**: 10M vectors (base: 9.9M, insert: 100K = 10K per batch × 10)
- **Vector**: Float32, dim=64, L2 distance
- **Queries**: 200 queries, TopK=5, 8 search threads, 8 insert threads
- **Index**: 2-layer SPANN BKT, PostingPageLimit=12, BufferLength=8
- **Storage**: TiKV (3 PD + 3 TiKV, v8.5.5, Docker, host network)
- **Routing**: Hash-based posting router, gRPC between nodes
- **Hardware**: 2× Intel Xeon Gold 6530 (64 cores / 128 threads), 128GB DDR5, 1× 894GB NVMe SSD
- **Note**: All compute nodes co-located on same machine; TiKV data on SSD via `/mnt/nvme_striped`

### Insert Throughput Scaling

| Config | Avg Throughput (vec/s) | Min | Max | Scaling |
|--------|----------------------|-----|-----|---------|
| 1-node | 93.8 | 87.0 | 102.4 | 1.00x |
| 2-node | 200.0 | 168.1 | 234.1 | 2.13x |
| 3-node | 271.4 | 207.0 | 349.6 | 2.89x |

Near-linear scaling: 2.13x at 2 nodes, 2.89x at 3 nodes.

### Query Throughput Scaling

| Config | Mean Latency (ms) | P90 (ms) | P99 (ms) | QPS | Scaling |
|--------|-------------------|----------|----------|-----|---------|
| 1-node | 50.2 | 55.1 | 66.2 | 158 | 1.00x |
| 2-node | 23.0 | 48.9 | 57.4 | 338 | 2.14x |
| 3-node | 17.8 | 55.3 | 74.4 | 438 | 2.77x |

Measured on pre-insert queries (warmed, round 2). Multi-node queries are dispatched
round-robin: each node searches its local head index + shared TiKV postings in parallel.

> **Note**: Per-query latency stats for remote queries are incomplete (min/p50=0 for remote
> portion) due to SearchStats not being propagated via gRPC. QPS is measured end-to-end
> and is accurate.

### Recall@5 (per configuration)

| Config | Pre-insert | Batch 1 | Batch 5 | Batch 10 |
|--------|-----------|---------|---------|----------|
| 1-node | 0.622 | 0.609 | 0.622 | 0.622 |
| 2-node routed | 0.556 | 0.556 | 0.556 | 0.558 |
| 3-node routed | 0.564 | 0.564 | 0.564 | 0.569 |

Recall is stable within each configuration across all insert batches.
Cross-config variation is from independent BKT builds (different random trees).

### Build Phase Baseline (single-node, no routing)

| Config | Build Time (s) | Avg Throughput (vec/s) | Pre-insert Recall@5 |
|--------|---------------|----------------------|---------------------|
| 1-node | 3293 | 93.8 | 0.622 |
| 2-node | 2837 | 100.9 | 0.566 |
| 3-node | 2822 | 103.2 | 0.574 |

### Bug Fixes Applied

1. **Double-sort in FullSearchCallback** (`SPANNIndex.cpp`): Removed redundant
   `SortResult()` on already-sorted results from `SearchDiskIndex`. Heapsort
   extraction on ascending data corrupted remote query results (lost best match).
2. **EvaluateRecall stride** (`TestDataGenerator.cpp`): Fixed truth NN access
   from stride=1 to stride=K via `GetVector()` instead of raw pointer arithmetic.
