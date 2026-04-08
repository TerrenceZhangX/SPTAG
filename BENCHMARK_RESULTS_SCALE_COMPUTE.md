# SPTAG/SPFresh + TiKV Distributed Routing Scale Benchmark Results

## Configuration

- **Vector**: UInt8, dim=128, L2 distance
- **Queries**: 200 queries, TopK=5
- **Threads**: 8 search threads + 8 insert threads per node (driver node only; worker nodes handle routed requests)
- **Index**: 2-layer SPANN BKT, PostingPageLimit=12, BufferLength=8
- **Storage**: TiKV (3 PD + 3 TiKV, Docker v8.5.5, host network)
- **Data**: `/mnt/data_disk/sift1b/base.1B.u8bin`, `query.public.10K.u8bin`
- **TiKV Config**: region-max-size=512MB, block-cache=40GB, grpc-concurrency=8
- **Date**: 2026-04-08

| Scale | Base Vectors | Insert Vectors | Batch Size |
|-------|-------------|---------------|------------|
| 100K | 99,000 | 1,000 | 100 × 10 |
| 1M | 990,000 | 10,000 | 1,000 × 10 |
| 10M | 9,900,000 | 100,000 | 10,000 × 10 |

| Topology | Nodes | Router | Description |
|----------|-------|--------|-------------|
| 1-node | 1 | Disabled | Single compute node, baseline |
| 2-node | 2 | Enabled | n0 (driver) + n1 (worker), hash routing |
| 3-node | 3 | Enabled | n0 (driver) + n1, n2 (workers), hash routing |

---

## 1. Build Time

### Total Build Time

| Scale | 1-node | 2-node | 3-node |
|-------|--------|--------|--------|
| 100K | 12.8s | 12.7s | 12.7s |
| 1M | 76.6s | 77.4s | 77.1s |
| 10M | 989s (16.5min) | 1008s (16.8min) | 1005s (16.7min) |

### Per-Layer Build Time (SelectHead + BuildHead + BuildSSD)

| Scale | Layer 0 | Layer 1 |
|-------|---------|---------|
| 100K | 0s + 5s + 1s = **6s** | 0s + 3s + 0s = **3s** |
| 1M | 6s + 33s + 22s = **61s** | 1s + 8s + 3s = **12s** |
| 10M | ~120s + ~495s + ~247s = **~850s** | ~13s + ~76s + ~45s = **~134s** |

---

## 2. Query Latency — Pre-insert

| Scale | Topo | Mean (ms) | P50 | P95 | P99 | QPS | Recall@5 |
|-------|------|-----------|-----|-----|-----|-----|----------|
| 100K | 1-node | 3.09 | 3.07 | 3.73 | 4.35 | 2532 | 0.989 |
| 100K | 2-node | 3.25 | 3.09 | 4.02 | 7.54 | 2401 | 0.979 |
| 100K | 3-node | 3.21 | 3.07 | 3.76 | 7.17 | 2428 | 0.980 |
| 1M | 1-node | 3.41 | 3.39 | 4.03 | 5.36 | 2299 | 0.977 |
| 1M | 2-node | 6.21 | 6.08 | 8.04 | 11.84 | 1271 | 0.969 |
| 1M | 3-node | 6.31 | 5.98 | 8.62 | 12.14 | 1257 | 0.975 |
| 10M | 1-node | 14.69 | 14.58 | 17.75 | 18.66 | 540 | 0.954 |
| 10M | 2-node | 14.60 | 14.66 | 17.06 | 19.22 | 541 | 0.933 |
| 10M | 3-node | 15.54 | 15.47 | 18.54 | 21.41 | 508 | 0.946 |

---

## 3. Query Latency — Avg B1-B10 (search round 1)

| Scale | Topo | Mean (ms) | P50 | P95 | P99 | QPS | Recall@5 |
|-------|------|-----------|-----|-----|-----|-----|----------|
| 100K | 1-node | 3.11 | 3.09 | 3.78 | 4.45 | 2517 | 0.989 |
| 100K | 2-node | 3.16 | 3.15 | 3.78 | 4.58 | 2481 | 0.984 |
| 100K | 3-node | 3.13 | 3.11 | 3.77 | 4.69 | 2506 | 0.985 |
| 1M | 1-node | 4.66 | 4.60 | 5.78 | 6.49 | 1778 | 0.977 |
| 1M | 2-node | 6.44 | 6.40 | 8.04 | 9.80 | 1233 | 0.973 |
| 1M | 3-node | 6.90 | 6.80 | 8.52 | 10.28 | 1156 | 0.980 |
| 10M | 1-node | 13.78 | 13.71 | 16.36 | 18.76 | 575 | 0.954 |
| 10M | 2-node | 14.23 | 14.17 | 17.12 | 19.79 | 557 | 0.940 |
| 10M | 3-node | 15.42 | 15.36 | 18.28 | 21.05 | 514 | 0.954 |

---

## 4. Query Latency — Per Batch Detail (Mean ms, search round 1)

### 1-node

| Batch | 100K | 1M | 10M |
|-------|------|----|-----|
| B1 | 3.12 | 3.37 | 13.55 |
| B2 | 3.18 | 3.49 | 14.00 |
| B3 | 3.09 | 4.02 | 13.99 |
| B4 | 3.10 | 4.04 | 13.70 |
| B5 | 3.02 | 3.89 | 13.82 |
| B6 | 3.11 | 4.23 | 13.74 |
| B7 | 3.14 | 5.31 | 13.63 |
| B8 | 3.15 | 5.51 | 13.86 |
| B9 | 3.09 | 6.12 | 13.66 |
| B10 | 3.14 | 6.62 | 13.89 |

### 2-node

| Batch | 100K | 1M | 10M |
|-------|------|----|-----|
| B1 | 3.10 | 5.82 | 13.99 |
| B2 | 3.08 | 5.53 | 14.17 |
| B3 | 3.21 | 6.76 | 14.21 |
| B4 | 3.11 | 6.74 | 14.09 |
| B5 | 3.25 | 6.45 | 14.41 |
| B6 | 3.10 | 6.60 | 14.27 |
| B7 | 3.10 | 6.65 | 14.39 |
| B8 | 3.32 | 6.45 | 14.24 |
| B9 | 3.24 | 6.38 | 14.29 |
| B10 | 3.13 | 7.03 | 14.26 |

### 3-node

| Batch | 100K | 1M | 10M |
|-------|------|----|-----|
| B1 | 3.08 | 5.86 | 15.08 |
| B2 | 3.08 | 5.97 | 15.42 |
| B3 | 3.12 | 6.16 | 15.90 |
| B4 | 3.12 | 7.19 | 15.38 |
| B5 | 3.17 | 7.17 | 15.58 |
| B6 | 3.10 | 7.37 | 15.41 |
| B7 | 3.10 | 7.28 | 15.27 |
| B8 | 3.19 | 7.24 | 15.33 |
| B9 | 3.23 | 7.31 | 15.59 |
| B10 | 3.11 | 7.48 | 15.20 |

---

## 5. Insert Throughput (avg vec/s)

| Scale | 1-node | 2-node | 3-node | 2-node vs 1 | 3-node vs 1 |
|-------|--------|--------|--------|-------------|-------------|
| 100K | 358 | 411 | 427 | +15% | +19% |
| 1M | 421 | 456 | 475 | +8% | +13% |
| 10M | 412 | 459 | 460 | +11% | +12% |

### Per-Batch Detail

| Batch | 100K 1n | 100K 2n | 100K 3n | 1M 1n | 1M 2n | 1M 3n | 10M 1n | 10M 2n | 10M 3n |
|-------|---------|---------|---------|-------|-------|-------|--------|--------|--------|
| B1 | 364 | 411 | 418 | 438 | 465 | 483 | 391 | 439 | 462 |
| B2 | 355 | 414 | 435 | 446 | 470 | 486 | 397 | 464 | 466 |
| B3 | 367 | 408 | 441 | 446 | 465 | 469 | 410 | 464 | 457 |
| B4 | 362 | 413 | 428 | 440 | 452 | 475 | 417 | 457 | 462 |
| B5 | 353 | 414 | 422 | 442 | 453 | 473 | 416 | 460 | 463 |
| B6 | 362 | 409 | 419 | 440 | 451 | 472 | 414 | 465 | 456 |
| B7 | 354 | 415 | 424 | 413 | 450 | 470 | 418 | 460 | 459 |
| B8 | 361 | 406 | 429 | 399 | 450 | 476 | 417 | 461 | 468 |
| B9 | 353 | 417 | 421 | 349 | 448 | 471 | 411 | 462 | 451 |
| B10 | 354 | 401 | 429 | 400 | 451 | 472 | 424 | 460 | 456 |

---

## 6. Recall@5

### Avg Recall@5 (B1-B10)

| Scale | 1-node | 2-node | 3-node |
|-------|--------|--------|--------|
| 100K | 0.989 | 0.984 | 0.985 |
| 1M | 0.977 | 0.973 | 0.980 |
| 10M | 0.954 | 0.940 | 0.954 |

### Recall@5 Trend (Pre → B10)

| Scale | Topo | Pre | B1 | B5 | B10 |
|-------|------|-----|----|----|-----|
| 100K | 1-node | 0.989 | 0.989 | 0.989 | 0.989 |
| 100K | 2-node | 0.979 | 0.980 | 0.983 | 0.988 |
| 100K | 3-node | 0.980 | 0.981 | 0.984 | 0.988 |
| 1M | 1-node | 0.977 | 0.977 | 0.977 | 0.978 |
| 1M | 2-node | 0.969 | 0.970 | 0.974 | 0.975 |
| 1M | 3-node | 0.975 | 0.976 | 0.981 | 0.982 |
| 10M | 1-node | 0.954 | 0.954 | 0.954 | 0.954 |
| 10M | 2-node | 0.933 | 0.936 | 0.942 | 0.938 |
| 10M | 3-node | 0.946 | 0.949 | 0.955 | 0.950 |

---

## 7. Router Overhead (Δ latency vs 1-node, avg B1-B10)

| Scale | 1-node (ms) | 2-node (ms) | Δ2 | 3-node (ms) | Δ3 |
|-------|-------------|-------------|-----|-------------|-----|
| 100K | 3.11 | 3.16 | +0.05 (+2%) | 3.13 | +0.02 (+1%) |
| 1M | 4.66 | 6.44 | +1.78 (+38%) | 6.90 | +2.24 (+48%) |
| 10M | 13.78 | 14.23 | +0.45 (+3%) | 15.42 | +1.64 (+12%) |

---

## Key Observations

1. **Build time dominated by Layer 0 BuildHead**: Layer 0 accounts for ~85% of total build time. BuildHead (BKT graph construction) is the bottleneck: 5s (100K) → 33s (1M) → 495s (10M). Build time is identical across topologies since it runs on a single node.
2. **Build scales ~13x per 10x data**: 100K 13s → 1M 77s (6x) → 10M 1000s (13x).
3. **100K — Router overhead negligible**: ~3.1ms across all topologies. Data fits entirely in block cache.
4. **1M — Router adds ~2ms**: 1-node 4.66ms → 2/3-node 6.4-6.9ms. Latency also grows across batches (3.4ms→6.6ms at 1-node) as posting lists expand.
5. **10M — Latency stable, router overhead small**: 1-node 13.8ms flat across batches. Router adds only +0.5ms (2-node) to +1.6ms (3-node) — TiKV RPC dominates.
6. **Insert throughput scales with nodes**: Consistent +10-19% improvement with multi-node across all scales.
7. **Recall improves with distributed inserts**: Multi-node recall increases over batches at all scales, converging toward 1-node levels.
8. **P99 tail latency**: 100K ~4-5ms, 1M ~6-12ms, 10M ~17-23ms. Multi-node adds 1-3ms to P99.
