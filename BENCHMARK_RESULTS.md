# SPTAG Distributed Insert — Scale Benchmark Results

## Overview

We benchmark **SPTAG SPFresh** insert throughput with distributed compute nodes sharing a
TiKV cluster. Each node holds a full head-index replica; the driver partitions
the insert batch across nodes so that every node performs its own **head search +
posting append** independently (distributed insert architecture).

| Parameter | 100k | 1M | 10M |
|---|---|---|---|
| Base vectors | 99,000 | 990,000 | 9,900,000 |
| Insert vectors / batch | 100 | 1,000 | 3,333 |
| Batches | 10 | 3 | 3 |
| PostingPageLimit | 12 | 48 | 48 |
| BufferLength | 8 | 32 | 32 |
| ValueType | UInt8 | UInt8 | UInt8 |
| Dimension | 128 | 128 | 128 |
| Insert threads / node | 16 | 16 | 16 |
| TiKV stores | 3 | 3 | 3 |

---

## Insert Throughput (ops/s)

### 100k Scale

| Nodes | Avg Throughput | Speedup (avg) | Max Throughput | Speedup (max-to-max) | Batch 1 | Batch 2 | Batch 3 | Batch 4 | Batch 5 | Batch 6 | Batch 7 | Batch 8 | Batch 9 | Batch 10 |
|-------|---------------|---------------|----------------|---------------|---------|---------|---------|---------|---------|---------|---------|---------|---------|----------|
| 1 | **156.1** | 1.00× | **175** | 1.00× | 145 | 168 | 144 | 166 | 148 | 145 | 151 | 143 | 175 | 175 |
| 2 | **271.5** | 1.74× | **323** | 1.85× | 252 | 283 | 195 | 323 | 207 | 288 | 301 | 238 | 311 | 316 |
| 3 | **382.4** | 2.45× | **453** | 2.59× | 298 | 376 | 324 | 451 | 374 | 401 | 436 | 453 | 284 | 427 |

### 1M Scale

| Nodes | Avg Throughput | Speedup | Batch 1 | Batch 2 | Batch 3 |
|-------|---------------|---------|---------|---------|---------|
| 1 | **154.3** | 1.00× | 157 | 182 | 123 |
| 2 | **244.1** | 1.58× | 117 | 305 | 310 |
| 3 | **314.5** | 2.04× | 307 | 329 | 308 |

> Note: 2-node batch 1 shows a cold-start effect (117 ops/s) — the worker's
> first TiKV stub creation adds latency. Subsequent batches reach 305–310 ops/s.

### 10M Scale

| Nodes | Avg Throughput | Speedup | Batch 1 | Batch 2 | Batch 3 |
|-------|---------------|---------|---------|---------|---------|
| 1 | **123.5** | 1.00× | 133 | 122 | 116 |
| 2 | **320.4** | 2.59× | 289 | 340 | 332 |
| 3 | _(pending)_ | — | — | — | — |

> 10M 3-node data is being re-collected — the initial run failed during the build
> phase due to split lock contention (retry limit increased from 20→100 to fix).

---

## Summary

```
Scale     1-node      2-node (speedup)    3-node (speedup)
──────    ──────      ────────────────    ────────────────
100k      156.1       271.5  (1.74×)      382.4  (2.45×)
1M        154.3       244.1  (1.58×)      314.5  (2.04×)
10M       123.5       320.4  (2.59×)      (pending)
```

### Key Observations

1. **Near-linear scaling at 100k**: 2-node reaches 1.74× and 3-node reaches
   2.45× — close to ideal 2× / 3× speedup, demonstrating that the distributed
   insert architecture effectively parallelizes both head search and posting
   writes.

2. **Good scaling at 1M**: 2-node 1.58×, 3-node 2.04×. Slightly sub-linear
   due to the cold-start overhead on the first batch and increased TiKV
   contention at larger index sizes.

3. **Strong 2-node scaling at 10M**: 2.59× speedup — actually super-linear,
   likely because distributing the workload also reduces per-node memory
   pressure and TiKV contention, leading to better cache utilization.

4. **1-node throughput decreases with scale**: 156→154→124 ops/s as index
   size grows, because head search over a larger graph takes more time and
   TiKV operations become heavier.

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
│  └────────────┬────────────────┘  └──────┬─────────────┘ │
│               │                          │               │
│  ┌────────────▼──────────────────────────▼─────────────┐ │
│  │              Shared TiKV Cluster                     │ │
│  │  Store 1 (20161)  Store 2 (20162)  Store 3 (20163)  │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

Each node has a full head-index replica for independent head search.
PostingRouter handles cross-node append routing when a posting's TiKV
region leader is on a different node.

---

## Fixes Applied

| Fix | File | Description |
|-----|------|-------------|
| Socket crash on disconnect | `Connection.cpp` | Used `error_code` overloads for `remote_endpoint()`/`local_endpoint()` |
| Port rebind failure | `Server.cpp` | Changed `reuse_address(false)` → `reuse_address(true)` |
| TiKV key format mismatch | `ExtraTiKVController.h` | Fixed `GetKeyLocation` to use raw bytes instead of `std::to_string` |
| Split lock contention | `ExtraDynamicSearcher.h` | Increased retry limit 20→100 with less verbose logging |
| Distributed insert | `PostingRouter.h`, `ExtraDynamicSearcher.h`, `Index.h`, `SPFreshTest.cpp` | Full distributed head search + append architecture |
| Batched remote appends | `PostingRouter.h`, `ExtraDynamicSearcher.h` | Queue remote appends and flush in batch (eliminates per-vector TCP) |
| Parallel worker appends | `PostingRouter.h` | `HandleBatchAppendRequest` uses 16 parallel threads |
| TiKV leader rebalancing | `run_scale_benchmarks.sh` | Rebalance region leaders evenly; 2-node drains 3rd store |

---

## Configuration

All benchmarks run on a single physical machine with TiKV stores on local
ports. This measures CPU scaling (head search parallelism), not network or
storage locality benefits.

- **TiKV PD**: ports 23791–23793
- **TiKV Stores**: ports 20161–20163
- **Router Ports**: 30001–30003
- **OS**: Linux
- **Build**: CMake Release, `-O2`

---

## How to Reproduce

```bash
# Build
cd /mnt/nvme/SPTAG/build && make -j8 SPTAGTest

# Run all scales
./run_scale_benchmarks.sh 100k 1m 10m

# Run a single scale
./run_scale_benchmarks.sh 100k
```
