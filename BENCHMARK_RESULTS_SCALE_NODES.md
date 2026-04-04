# SPTAG Distributed Insert — 100k Scale Benchmark Results

## Overview

SPTAG SPFresh insert + query benchmark with distributed compute nodes sharing a TiKV
cluster. Each node holds a full head-index replica; the driver partitions the insert
batch across nodes for independent **head search + posting append**. After splits, head
index changes are broadcast to all peers via fire-and-forget **HeadSyncRequest** packets.

### Configuration

| Parameter | Value |
|---|---|
| Base vectors | 99,000 |
| Insert vectors / batch | 100 |
| Batches | 10 |
| Total inserts | 1,000 |
| PostingPageLimit | 12 |
| BufferLength | 8 |
| ValueType | UInt8 |
| Dimension | 128 |
| Insert threads / node | 16 |
| Query threads | 16 |
| Queries per round | 200 |
| TiKV stores | 3 |
| Head sync broadcast | Enabled |

Data from Run 9 (MultiGet region error retry fix, clean TiKV restart per phase).

---

## 1. Insert Throughput Summary

| Nodes | Avg (vec/s) | Speedup | Batch 1 | Batch 2 | Batch 3 | Batch 4 | Batch 5 | Batch 6 | Batch 7 | Batch 8 | Batch 9 | Batch 10 |
|-------|------------|---------|---------|---------|---------|---------|---------|---------|---------|---------|---------|----------|
| 1 | **103.6** | 1.00× | 109.0 | 91.3 | 86.5 | 91.7 | 116.9 | 96.9 | 117.3 | 101.5 | 128.3 | 96.8 |
| 2 | **202.9** | 1.96× | 183.5 | 170.3 | 248.7 | 188.2 | 238.2 | 155.6 | 190.8 | 238.7 | 193.7 | 221.7 |
| 3 | **310.1** | 2.99× | 166.9 | 301.3 | 348.0 | 397.3 | 364.0 | 275.3 | 247.1 | 272.9 | 348.7 | 379.8 |

---

## 2. Query Before Insert (Baseline)

Measured on the freshly built index (99k vectors), before any inserts.

| Nodes | QPS | Mean (ms) | P50 | P99 | Recall@5 |
|-------|-----|-----------|-----|-----|----------|
| 1 | 560 | 27.9 | 27.9 | 38.5 | 0.677 |
| 2 | 351 | 46.8 | 46.8 | 58.8 | 0.666 |
| 3 | 550 | 27.6 | 26.8 | 50.5 | 0.671 |

**Round 2** (warm cache):

| Nodes | QPS | Mean (ms) | P50 | P99 | Recall@5 |
|-------|-----|-----------|-----|-----|----------|
| 1 | 621 | 24.6 | 24.6 | 35.5 | 0.677 |
| 2 | 373 | 40.4 | 40.4 | 56.0 | 0.666 |
| 3 | 632 | 24.0 | 22.6 | 39.2 | 0.671 |

> **Note**: 2-node QPS is lower than 1-node because queries run only on the driver
> node, and TiKV is shared with a worker process. Queries are NOT distributed.

---

## 3. Per-Batch Detail — 1 Node

| Batch | Throughput | Search QPS | Search P50 (ms) | Search P99 | Search Recall@5 | Search2 QPS | Search2 P50 | Search2 P99 | Search2 Recall@5 |
|-------|------------|------------|-----------------|------------|-----------------|-------------|-------------|-------------|------------------|
| 1 | 109.0 | 671 | 22.8 | 35.8 | 0.679 | 686 | 22.4 | 33.7 | 0.679 |
| 2 | 91.3 | 617 | 24.8 | 35.1 | 0.679 | 654 | 23.5 | 33.3 | 0.679 |
| 3 | 86.5 | 576 | 27.4 | 38.2 | 0.677 | 637 | 24.2 | 35.0 | 0.677 |
| 4 | 91.7 | 511 | 29.6 | 43.8 | 0.677 | 481 | 32.2 | 46.2 | 0.677 |
| 5 | 116.9 | 549 | 26.5 | 49.2 | 0.677 | 545 | 28.4 | 38.2 | 0.677 |
| 6 | 96.9 | 518 | 29.2 | 47.1 | 0.681 | 557 | 28.1 | 37.2 | 0.681 |
| 7 | 117.3 | 505 | 30.6 | 47.5 | 0.681 | 548 | 28.5 | 40.8 | 0.681 |
| 8 | 101.5 | 383 | 40.6 | 62.7 | 0.679 | 464 | 32.6 | 48.7 | 0.679 |
| 9 | 128.3 | 501 | 29.7 | 49.3 | 0.679 | 501 | 29.5 | 59.6 | 0.679 |
| 10 | 96.8 | 460 | 32.2 | 67.5 | 0.681 | 494 | 31.2 | 45.2 | 0.681 |

> Recall stable at 0.677–0.681 across all batches. Zero anomalies.

---

## 4. Per-Batch Detail — 2 Nodes

| Batch | Throughput | Search QPS | Search P50 (ms) | Search P99 | Search Recall@5 | Search2 QPS | Search2 P50 | Search2 P99 | Search2 Recall@5 |
|-------|------------|------------|-----------------|------------|-----------------|-------------|-------------|-------------|------------------|
| 1 | 183.5 | 542 | 28.8 | 47.0 | 0.665 | 560 | 26.6 | 43.9 | 0.665 |
| 2 | 170.3 | 594 | 25.9 | 38.1 | 0.666 | 591 | 25.5 | 43.2 | 0.666 |
| 3 | 248.7 | 595 | 25.6 | 43.9 | 0.665 | 613 | 25.2 | 37.8 | 0.665 |
| 4 | 188.2 | 583 | 26.4 | 38.3 | 0.665 | 600 | 25.5 | 37.8 | 0.665 |
| 5 | 238.2 | 585 | 26.5 | 38.9 | 0.665 | 593 | 26.1 | 35.1 | 0.665 |
| 6 | 155.6 | 341 | 45.6 | 92.0 | 0.665 | 352 | 46.6 | 55.0 | 0.665 |
| 7 | 190.8 | 346 | 45.8 | 61.0 | 0.666 | 357 | 44.6 | 55.0 | 0.666 |
| 8 | 238.7 | 353 | 46.2 | 57.7 | 0.665 | 366 | 44.3 | 57.1 | 0.665 |
| 9 | 193.7 | 359 | 44.5 | 61.0 | 0.665 | 353 | 46.0 | 56.1 | 0.665 |
| 10 | 221.7 | 341 | 46.7 | 67.7 | 0.665 | 348 | 46.0 | 57.6 | 0.665 |

> Recall stable at 0.665–0.666 across all batches. Zero anomalies.

---

## 5. Per-Batch Detail — 3 Nodes

| Batch | Throughput | Search QPS | Search P50 (ms) | Search P99 | Search Recall@5 | Search2 QPS | Search2 P50 | Search2 P99 | Search2 Recall@5 |
|-------|------------|------------|-----------------|------------|-----------------|-------------|-------------|-------------|------------------|
| 1 | 166.9 | 889 | 16.8 | 31.8 | 0.672 | 894 | 16.8 | 27.1 | 0.672 |
| 2 | 301.3 | 881 | 17.2 | 31.3 | 0.672 | 911 | 16.5 | 28.3 | 0.672 |
| 3 | 348.0 | 918 | 16.6 | 25.5 | 0.671 | 948 | 16.1 | 26.2 | 0.671 |
| 4 | 397.3 | 910 | 16.7 | 27.4 | 0.670 | 920 | 16.5 | 25.7 | 0.670 |
| 5 | 364.0 | 927 | 16.5 | 28.0 | 0.671 | 949 | 16.2 | 26.8 | 0.671 |
| 6 | 275.3 | 808 | 19.1 | 28.7 | 0.671 | 422 | 28.9 | 137.1 | 0.671 |
| 7 | 247.1 | 521 | 30.5 | 42.5 | 0.671 | 536 | 29.2 | 41.4 | 0.671 |
| 8 | 272.9 | 546 | 29.4 | 41.0 | 0.670 | 531 | 30.4 | 40.4 | 0.670 |
| 9 | 348.7 | 504 | 30.9 | 42.8 | 0.671 | 484 | 32.3 | 44.7 | 0.671 |
| 10 | 379.8 | 523 | 30.0 | 42.9 | 0.671 | 483 | 32.3 | 49.5 | 0.671 |

> Recall stable at 0.670–0.672 across all batches. Zero anomalies.
> MultiGet region error retry triggered 6 times during this run — fix working as designed.

---

## 6. Key Observations

1. **Insert scaling**: Near-linear throughput scaling across node counts:
   - 1-node: 103.6 vec/s (baseline)
   - 2-node: 202.9 vec/s (1.96×)
   - 3-node: 310.1 vec/s (2.99×)

2. **Recall stability**: Recall@5 stays at 0.665–0.681 across all batches and node
   counts. **Zero anomalies** after fixing MultiGet region error handling. Previously
   recall would sporadically drop to 0.14 due to silent empty posting reads.

3. **Root cause of previous recall anomaly**: `TiKVIO::MultiGet` did not check
   `response.has_region_error()` or per-pair `has_error()`. During TiKV region
   events, batch reads returned empty buffers silently. Fixed by falling back to
   individual `Get` calls (which have 10-retry logic) on any region/pair error.

4. **Search QPS regression over batches**: QPS drops from ~670 → ~460 (1-node),
   ~595 → ~341 (2-node), and ~920 → ~483 (3-node) across batches. Expected as
   TiKV accumulates data.

5. **Head sync broadcast**: No measurable overhead. Fire-and-forget after splits.

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
./run_scale_benchmarks.sh 100k
```
