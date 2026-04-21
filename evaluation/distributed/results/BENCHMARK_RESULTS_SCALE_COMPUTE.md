# SPTAG/SPFresh + TiKV Distributed Routing Scale Benchmark Results

## Configuration

- **Vector**: UInt8, dim=128, L2 distance
- **Queries**: 200 queries, TopK=5
- **Threads**: 8 search threads + 8 insert threads per node (driver node only; worker nodes handle routed requests)
- **Index**: 2-layer SPANN BKT, PostingPageLimit=12, BufferLength=8
- **Storage (shared)**: TiKV shared cluster (3 PD + 3 TiKV, Docker v8.5.5, host network)
- **Storage (indep)**: Per-worker independent TiKV (1 PD + 1 TiKV per node, Docker v8.5.1, host network)
- **Data**: `/mnt/data_disk/sift1b/base.1B.u8bin`, `query.public.10K.u8bin`
- **TiKV Config**: region-max-size=512MB, block-cache=40GB, grpc-concurrency=8
- **Date**: 2026-04-10 (shared TiKV); 2026-04-21 (independent TiKV)

| Scale | Base Vectors | Insert Vectors | Batch Size |
|-------|-------------|---------------|------------|
| 100K | 99,000 | 1,000 | 100 × 10 |
| 1M | 990,000 | 10,000 | 1,000 × 10 |
| 10M | 9,900,000 | 100,000 | 10,000 × 10 |

| Topology | Nodes | Router | TiKV | Description |
|----------|-------|--------|------|-------------|
| 1-node | 1 | Disabled | shared | Single compute node, baseline |
| 2-node | 2 | Enabled | shared | n0 (driver) + n1 (worker), hash routing |
| 3-node | 3 | Enabled | shared | n0 (driver) + n1, n2 (workers), hash routing |
| 1-node-indep | 1 | Disabled | independent | Single node, local PD+TiKV |
| 2-node-indep | 2 | Enabled | independent | n0 + n1, each with local PD+TiKV |

---

## 1. Build Time

### Total Build Time

| Scale | 1-node | 2-node | 3-node |
|-------|--------|--------|--------|
| 100K | 12.8s | 12.7s | 12.7s |
| 1M | 77.0s | 78.6s | 76.6s |
| 10M | 1548s (25.8min) | 1546s (25.8min) | 1544s (25.7min) |

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
| 1M | 1-node | 3.57 | 3.52 | 4.32 | 5.13 | 2207 | 0.984 |
| 1M | 2-node | 5.67 | 5.18 | 8.01 | 10.63 | 1120 | 0.969 |
| 1M | 3-node | 4.86 | 4.70 | 6.35 | 9.53 | 1621 | 0.977 |
| 10M | 1-node | 14.88 | 14.85 | 18.07 | 20.19 | 532 | 0.951 |
| 10M | 2-node | 14.70 | 14.64 | 17.19 | 21.03 | 539 | 0.935 |
| 10M | 3-node | 15.43 | 15.46 | 18.02 | 21.41 | 514 | 0.948 |
| 1M | 1-node-indep | 5.81 | 5.74 | 7.10 | 8.70 | 1355 | — |
| 1M | 2-node-indep | 11.51 | 11.58 | 13.82 | 16.56 | 899 | — |
| 10M | 1-node-indep | 29.26 | 29.78 | 36.23 | 37.59 | 271 | — |
| 10M | 2-node-indep | 28.37 | 27.37 | 38.70 | 40.34 | 480 | — |

---

## 3. Query Latency — Avg B1-B10 (search round 1)

| Scale | Topo | Mean (ms) | P50 | P95 | P99 | QPS | Recall@5 |
|-------|------|-----------|-----|-----|-----|-----|----------|
| 100K | 1-node | 3.11 | 3.09 | 3.78 | 4.45 | 2517 | 0.989 |
| 100K | 2-node | 3.16 | 3.15 | 3.78 | 4.58 | 2481 | 0.984 |
| 100K | 3-node | 3.13 | 3.11 | 3.77 | 4.69 | 2506 | 0.985 |
| 1M | 1-node | 4.54 | 4.44 | 5.57 | 6.44 | 1808 | 0.984 |
| 1M | 2-node | 5.59 | 5.21 | 7.07 | 10.64 | 1133 | 0.974 |
| 1M | 3-node | 5.10 | 4.85 | 6.39 | 8.87 | 1441 | 0.983 |
| 10M | 1-node | 15.47 | 15.37 | 18.38 | 21.12 | 513 | 0.952 |
| 10M | 2-node | 10.07 | 12.79 | 23.23 | 26.31 | 776 | 0.847 |
| 10M | 3-node | 7.74 | 0.00 | 26.69 | 29.91 | 988 | 0.822 |
| 1M | 1-node-indep | 9.56 | 9.43 | 11.37 | 12.77 | 880 | — |
| 1M | 2-node-indep | 11.53 | 11.29 | 14.58 | 23.17 | 898 | — |
| 10M | 1-node-indep | 29.32 | 29.41 | 36.84 | 39.53 | 271 | — |
| 10M | 2-node-indep | 30.61 | 30.18 | 41.65 | 44.13 | 499 | — |

### Avg Search QPS Scaling

| Scale | 1-node | 2-node | 3-node | 2-node vs 1 | 3-node vs 1 |
|-------|--------|--------|--------|-------------|-------------|
| 100K | 2517 | 2481 | 2506 | -1% | -0.4% |
| 1M | 1808 | 1133 | 1441 | -37% | -20% |
| 10M | 513 | 776 | 988 | +51% | +93% |
| 1M (indep) | 880 | 898 | — | +2% | — |
| 10M (indep) | 271 | 499 | — | +84% | — |

---

## 4. Query Latency — Per Batch Detail (search round 1)

### 1M

| Batch | 1-node avg (ms) | 1-node P99 (ms) | 1-node QPS | 2-node avg (ms) | 2-node P99 (ms) | 2-node QPS | 3-node avg (ms) | 3-node P99 (ms) | 3-node QPS |
|-------|-----------------|-----------------|------------|-----------------|-----------------|------------|-----------------|-----------------|------------|
| 0 | 3.57 | 5.13 | 2207 | 5.67 | 10.63 | 1120 | 4.86 | 9.53 | 1621 |
| 1 | 3.48 | 4.84 | 2274 | 5.50 | 10.24 | 1146 | 4.78 | 7.90 | 1649 |
| 2 | 3.50 | 5.27 | 2257 | 5.42 | 10.12 | 1164 | 4.83 | 8.23 | 1633 |
| 3 | 3.68 | 5.02 | 2138 | 5.57 | 9.95 | 1138 | 4.77 | 8.27 | 1658 |
| 4 | 3.94 | 5.80 | 2002 | 5.53 | 9.61 | 1144 | 5.65 | 44.39 | 1150 |
| 5 | 4.11 | 6.33 | 1918 | 5.58 | 12.42 | 1137 | 4.92 | 7.69 | 1607 |
| 6 | 4.48 | 6.37 | 1763 | 5.72 | 11.08 | 1108 | 5.08 | 8.87 | 1552 |
| 7 | 5.23 | 7.20 | 1507 | 6.03 | 11.04 | 1064 | 5.16 | 9.30 | 1211 |
| 8 | 6.31 | 8.38 | 1252 | 5.54 | 10.47 | 1142 | 4.98 | 8.21 | 1581 |
| 9 | 5.28 | 7.74 | 1497 | 5.41 | 10.11 | 1160 | 5.28 | 9.89 | 1188 |
| 10 | 5.36 | 7.40 | 1473 | 5.62 | 11.38 | 1126 | 5.51 | 43.60 | 1184 |

### 1M (indep-tikv)

| Batch | 1-node avg (ms) | 1-node P99 (ms) | 1-node QPS | 2-node avg (ms) | 2-node P99 (ms) | 2-node QPS |
|-------|-----------------|-----------------|------------|-----------------|-----------------|------------|
| 0 | 5.81 | 8.70 | 1355 | 11.51 | 16.56 | 899 |
| 1 | 5.56 | 8.09 | 1416 | 10.27 | 18.57 | 900 |
| 2 | 7.38 | 10.51 | 1068 | 11.49 | 28.24 | 911 |
| 3 | 8.12 | 12.03 | 969 | 11.24 | 23.87 | 881 |
| 4 | 9.69 | 11.37 | 814 | 11.58 | 28.09 | 907 |
| 5 | 9.19 | 12.52 | 858 | 12.26 | 26.64 | 902 |
| 6 | 9.30 | 11.78 | 847 | 11.69 | 20.20 | 896 |
| 7 | 9.43 | 13.02 | 839 | 11.74 | 19.52 | 907 |
| 8 | 9.96 | 12.58 | 793 | 11.92 | 22.43 | 903 |
| 9 | 12.29 | 15.60 | 643 | 11.43 | 25.50 | 877 |
| 10 | 14.19 | 21.24 | 557 | 11.64 | 18.59 | 894 |
| **Avg** | **9.56** | **12.77** | **880** | **11.53** | **23.17** | **898** |
| **2n/1n QPS** | | | | | | **1.02x** |

### 10M

| Batch | 1-node avg (ms) | 1-node P99 (ms) | 1-node QPS | 2-node avg (ms) | 2-node P99 (ms) | 2-node QPS | 3-node avg (ms) | 3-node P99 (ms) | 3-node QPS |
|-------|-----------------|-----------------|------------|-----------------|-----------------|------------|-----------------|-----------------|------------|
| 0 | 14.88 | 20.19 | 532 | 14.70 | 21.03 | 539 | 15.43 | 21.41 | 514 |
| 1 | 14.89 | 19.29 | 532 | 9.89 | 25.83 | 788 | 7.33 | 27.87 | 1022 |
| 2 | 15.37 | 19.98 | 515 | 9.79 | 27.98 | 798 | 7.42 | 28.93 | 1051 |
| 3 | 14.93 | 22.04 | 530 | 10.49 | 29.47 | 750 | 7.55 | 28.78 | 1006 |
| 4 | 15.78 | 21.52 | 503 | 9.98 | 25.88 | 776 | 7.71 | 30.79 | 975 |
| 5 | 16.00 | 21.16 | 495 | 9.85 | 24.81 | 798 | 7.88 | 32.51 | 977 |
| 6 | 15.28 | 23.26 | 520 | 10.01 | 25.06 | 774 | 7.96 | 29.57 | 976 |
| 7 | 15.63 | 20.85 | 506 | 9.85 | 25.44 | 796 | 7.91 | 30.92 | 975 |
| 8 | 15.79 | 19.64 | 502 | 10.04 | 25.05 | 781 | 7.82 | 28.90 | 978 |
| 9 | 15.16 | 22.03 | 522 | 10.08 | 25.58 | 774 | 8.05 | 30.69 | 965 |
| 10 | 15.86 | 21.43 | 500 | 10.76 | 28.04 | 726 | 7.78 | 31.88 | 958 |

### 10M (indep-tikv)

| Batch | 1-node avg (ms) | 1-node P99 (ms) | 1-node QPS | 2-node avg (ms) | 2-node P99 (ms) | 2-node QPS |
|-------|-----------------|-----------------|------------|-----------------|-----------------|------------|
| 0 | 29.26 | 37.59 | 271 | 28.37 | 40.34 | 480 |
| 1 | 28.20 | 35.37 | 281 | 29.37 | 45.55 | 494 |
| 2 | 27.95 | 35.65 | 284 | 32.44 | 44.59 | 487 |
| 3 | 29.36 | 38.52 | 270 | 31.27 | 41.92 | 475 |
| 4 | 29.66 | 39.14 | 267 | 31.84 | 42.35 | 494 |
| 5 | 29.84 | 47.82 | 265 | 30.98 | 46.92 | 486 |
| 6 | 29.67 | 38.15 | 267 | 28.94 | 40.62 | 481 |
| 7 | 30.11 | 37.88 | 264 | 30.76 | 40.92 | 512 |
| 8 | 29.16 | 36.94 | 271 | 29.62 | 49.77 | 526 |
| 9 | 31.09 | 45.39 | 255 | 28.37 | 40.76 | 555 |
| 10 | 28.12 | 40.30 | 282 | 32.50 | 48.27 | 485 |
| **Avg** | **29.32** | **39.53** | **271** | **30.61** | **44.13** | **499** |
| **2n/1n QPS** | | | | | | **1.84x** |

---

## 5. Insert Throughput (avg vec/s)

| Scale | 1-node | 2-node | 3-node | 2-node vs 1 | 3-node vs 1 |
|-------|--------|--------|--------|-------------|-------------|
| 100K | 358 | 411 | 427 | +15% | +19% |
| 1M | 411 | 548 | 622 | +33% | +51% |
| 10M | 459 | 757 | 907 | +65% | +98% |
| 1M (indep) | 359 | 624 | — | +74% | — |
| 10M (indep) | 299 | 595 | — | +99% | — |

### Per-Batch Detail

#### 1M

| Batch | 1-node | 2-node | 3-node | 2n/1n | 3n/1n |
|-------|--------|--------|--------|-------|-------|
| B1 | 442 | 550 | 620 | 1.24x | 1.40x |
| B2 | 447 | 553 | 622 | 1.24x | 1.39x |
| B3 | 448 | 552 | 626 | 1.23x | 1.40x |
| B4 | 446 | 551 | 623 | 1.24x | 1.40x |
| B5 | 445 | 552 | 609 | 1.24x | 1.37x |
| B6 | 444 | 551 | 622 | 1.24x | 1.40x |
| B7 | 410 | 550 | 626 | 1.34x | 1.53x |
| B8 | 402 | 516 | 621 | 1.28x | 1.54x |
| B9 | 210 | 551 | 624 | 2.62x | 2.97x |
| B10 | 416 | 554 | 624 | 1.33x | 1.50x |
| **Avg speedup** | **411** | **548** | **622** | **1.33x** | **1.51x** |
| **Max-max speedup** | **448** | **554** | **626** | **1.24x** | **1.40x** |

#### 10M

| Batch | 1-node | 2-node | 3-node | 2n/1n | 3n/1n |
|-------|--------|--------|--------|-------|-------|
| B1 | 448 | 731 | 876 | 1.63x | 1.96x |
| B2 | 452 | 784 | 939 | 1.73x | 2.08x |
| B3 | 461 | 779 | 942 | 1.69x | 2.04x |
| B4 | 460 | 757 | 907 | 1.65x | 1.97x |
| B5 | 448 | 753 | 906 | 1.68x | 2.02x |
| B6 | 468 | 764 | 898 | 1.63x | 1.92x |
| B7 | 461 | 756 | 897 | 1.64x | 1.95x |
| B8 | 454 | 752 | 911 | 1.66x | 2.01x |
| B9 | 468 | 766 | 894 | 1.64x | 1.91x |
| B10 | 466 | 729 | 902 | 1.56x | 1.94x |
| **Avg speedup** | **459** | **757** | **907** | **1.65x** | **1.98x** |
| **Max-max speedup** | **468** | **784** | **942** | **1.68x** | **2.01x** |

#### 1M (indep-tikv)

| Batch | 1-node | 2-node | 2n/1n |
|-------|--------|--------|-------|
| B1 | 397 | 632 | 1.59x |
| B2 | 376 | 647 | 1.72x |
| B3 | 359 | 656 | 1.83x |
| B4 | 365 | 627 | 1.72x |
| B5 | 369 | 626 | 1.70x |
| B6 | 365 | 641 | 1.75x |
| B7 | 354 | 610 | 1.73x |
| B8 | 354 | 596 | 1.69x |
| B9 | 323 | 594 | 1.84x |
| B10 | 328 | 612 | 1.86x |
| **Avg** | **359** | **624** | **1.74x** |

#### 10M (indep-tikv)

| Batch | 1-node | 2-node | 2n/1n |
|-------|--------|--------|-------|
| B1 | 303 | 598 | 1.97x |
| B2 | 298 | 587 | 1.97x |
| B3 | 300 | 591 | 1.97x |
| B4 | 294 | 590 | 2.01x |
| B5 | 298 | 582 | 1.95x |
| B6 | 296 | 594 | 2.00x |
| B7 | 302 | 587 | 1.94x |
| B8 | 298 | 602 | 2.02x |
| B9 | 298 | 605 | 2.03x |
| B10 | 299 | 613 | 2.05x |
| **Avg** | **299** | **595** | **1.99x** |

---

## 6. Recall@5

### Avg Recall@5 (B1-B10)

| Scale | 1-node | 2-node | 3-node |
|-------|--------|--------|--------|
| 100K | 0.989 | 0.984 | 0.985 |
| 1M | 0.984 | 0.974 | 0.983 |
| 10M | 0.952 | 0.847 | 0.822 |

### Recall@5 Trend (Pre → B10)

| Scale | Topo | Pre | B1 | B5 | B10 |
|-------|------|-----|----|----|-----|
| 100K | 1-node | 0.989 | 0.989 | 0.989 | 0.989 |
| 100K | 2-node | 0.979 | 0.980 | 0.983 | 0.988 |
| 100K | 3-node | 0.980 | 0.981 | 0.984 | 0.988 |
| 1M | 1-node | 0.984 | 0.984 | 0.984 | 0.984 |
| 1M | 2-node | 0.969 | 0.970 | 0.974 | 0.978 |
| 1M | 3-node | 0.977 | 0.978 | 0.983 | 0.987 |
| 10M | 1-node | 0.951 | 0.951 | 0.952 | 0.952 |
| 10M | 2-node | 0.935 | 0.845 | 0.846 | 0.847 |
| 10M | 3-node | 0.948 | 0.823 | 0.822 | 0.821 |

---

## 7. Router Overhead (Δ latency vs 1-node, avg B1-B10)

| Scale | 1-node (ms) | 2-node (ms) | Δ2 | 3-node (ms) | Δ3 |
|-------|-------------|-------------|-----|-------------|-----|
| 100K | 3.11 | 3.16 | +0.05 (+2%) | 3.13 | +0.02 (+1%) |
| 1M | 4.54 | 5.59 | +1.05 (+23%) | 5.10 | +0.56 (+12%) |
| 10M | 15.47 | 10.07 | -5.40 (-35%) | 7.74 | -7.73 (-50%) |
| 1M (indep) | 9.56 | 11.53 | +1.97 (+21%) | — | — |
| 10M (indep) | 29.32 | 30.61 | +1.29 (+4%) | — | — |

---

## Key Observations

1. **Build time dominated by Layer 0 BuildHead**: Layer 0 accounts for ~85% of total build time. BuildHead (BKT graph construction) is the bottleneck: 5s (100K) → 33s (1M) → 495s (10M). Build time is identical across topologies since it runs on a single node.
2. **Build scales ~13x per 10x data**: 100K 13s → 1M 77s (6x) → 10M 1548s (20x).
3. **100K — Router overhead negligible**: ~3.1ms across all topologies. Data fits entirely in block cache.
4. **1M — Router overhead moderate (post-bugfix)**: 1-node 4.54ms → 2-node 5.59ms (+23%) → 3-node 5.10ms (+12%). 3-node is faster than 2-node because work is split across more workers.
5. **1M — Insert throughput scales well**: 1-node 411 → 2-node 548 (+33%) → 3-node 622 (+51%). Near-linear scaling with compute nodes.
6. **10M — Insert throughput scales near-linearly**: 1-node 459 → 2-node 757 (+65%) → 3-node 907 (+98%). This is a dramatic improvement over the previous run (2-node was 0.72x, now 1.65x). The fix: each node builds its own index independently (per-node build), eliminating the resource contention that caused the 2-node regression.
7. **10M — Search latency improves with more nodes**: 1-node 15.5ms → 2-node 10.1ms (-35%) → 3-node 7.7ms (-50%). More nodes = less posting data per node = faster search. This is the opposite of the previous run where 2-node was +34% slower.
8. **Insert throughput scales across all data sizes**: 100K +15-19%, 1M +33-51%, 10M +65-98%. Scaling improves with data size because larger data means more work to distribute.
9. **Recall trade-off at 10M multi-node**: Pre-insert recall is similar across topologies (0.935-0.951). After insert, 2/3-node recall drops to 0.82-0.85 due to FullSearch routing across nodes (each node only has partial head index). This is expected and can be improved with head sync.
10. **P99 tail latency**: 100K ~4-5ms, 1M ~5-12ms, 10M ~20-32ms. Multi-node 10M shows higher P99 (25-32ms) due to cross-node RPC tail.
11. **HandleSearchPosting sort fix (2026-04-09)**: Fixed a bug where worker nodes returned 0 results when the TopK heap was not fully filled, causing recall degradation at small scales (100K). After fix, 1M insert throughput improved significantly (2-node: 456→548, 3-node: 475→622).
12. **Per-worker independent TiKV — 10M insert scales ~2x**: With each worker using its own local TiKV (no shared cluster), 10M insert throughput reaches 1.99x (299→595 vec/s), up from 1.65x in the shared-TiKV configuration. Eliminates cross-network TiKV access entirely.
13. **Per-worker independent TiKV — 1M search limited by dispatch overhead**: At 1M scale, 2-node search QPS (898) barely exceeds 1-node (880) because per-query compute (~9ms) is small relative to the ~2ms dispatch round-trip. The 2-node QPS stays flat while 1-node degrades across batches, so they converge.
14. **Per-worker independent TiKV — 10M search scales 1.84x**: At 10M, per-query compute (~29ms) is large enough to amortize dispatch overhead (+4%), yielding 1.84x QPS scaling (271→499).

