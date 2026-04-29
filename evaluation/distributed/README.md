# Distributed Benchmark Evaluation

Scripts and configs for running SPTAG SPANN distributed benchmarks — single-machine (multi-process) and multi-machine.

## Component metrics (post-run)

After a run completes, run `collect_component_metrics.sh` to merge router /
HeadSync / PD / per-node resource counters into the bench JSON under a new
`component_metrics` block. See [`COMPONENT_METRICS_SCHEMA.md`](./COMPONENT_METRICS_SCHEMA.md)
for the field list and origins.

```bash
evaluation/distributed/collect_component_metrics.sh \
    cluster.conf results/output_10m_4node.json results/logs/ [duration_sec]
```

The helper does not modify `run_distributed.sh` or the benchmark hot path;
it is safe to re-run on archived result directories.

## Directory Structure

```
evaluation/distributed/
├── README.md                          # This file
├── cluster.conf.example               # Multi-machine cluster topology config
├── run_scale_benchmarks.sh            # Single-machine multi-process benchmark
├── run_distributed.sh                 # Multi-machine benchmark orchestrator
└── configs/
    ├── benchmark_1m_template.ini      # Template for auto-generating per-node configs
    ├── benchmark_1m_1node.ini         # 1M scale, single-node
    ├── benchmark_1m_2node_*.ini       # 1M scale, 2-node configs
    ├── benchmark_1m_3node_*.ini       # 1M scale, 3-node configs
    ├── benchmark_10m_1node.ini        # 10M scale, single-node
    ├── benchmark_10m_2node_*.ini      # 10M scale, 2-node configs
    └── benchmark_10m_3node_*.ini      # 10M scale, 3-node configs
```

## Architecture

```
                    ┌──────────────┐
                    │   Driver     │  (node 0)
                    │  RunBenchmark│
                    │   + Router   │
                    └──┬───┬───┬──┘
           TCP Dispatch│   │   │
              ┌────────┘   │   └────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Worker 1 │ │ Worker 2 │ │ Worker N │
        │ WorkerNode│ │ WorkerNode│ │ WorkerNode│
        │ + Router │ │ + Router │ │ + Router │
        └────┬─────┘ └────┬─────┘ └────┬─────┘
             │             │             │
             ▼             ▼             ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │  TiKV 1  │ │  TiKV 2  │ │  TiKV 3  │
        └──────────┘ └──────────┘ └──────────┘
```

- **Driver** (node 0): Builds the index, sends Search/Insert/Stop commands via TCP dispatch
- **Workers** (nodes 1..N): Receive commands, execute locally, report results back
- **TiKV**: Distributed KV store for posting lists (shared across all nodes)
- **PostingRouter**: Handles hash-based routing, remote append, head sync, and dispatch protocol

## Single-Machine Benchmark

For running on one machine with multiple processes simulating distributed nodes.

### Prerequisites

```bash
# Build SPTAG
cd /path/to/SPTAG
mkdir -p build && cd build
cmake .. && cmake --build . --target SPTAGTest -- -j$(nproc)

# Start TiKV (docker compose)
cd docker/tikv && docker compose up -d
```

### Usage

```bash
# Run specific scales
./evaluation/distributed/run_scale_benchmarks.sh 1m
./evaluation/distributed/run_scale_benchmarks.sh 1m 10m

# Run all available scales
./evaluation/distributed/run_scale_benchmarks.sh all
```

The script automatically:
1. Runs 1-node baseline (single process, no routing)
2. Runs 2-node distributed (driver + 1 worker, hash-based routing)
3. Runs 3-node distributed (driver + 2 workers)

Results are saved to `output_<scale>_<N>node.json` and logs to `benchmark_logs/`.

### Config Files

Each scale needs configs in `configs/`. Naming convention:
- `benchmark_<scale>_1node.ini` — single-node baseline
- `benchmark_<scale>_<N>node_n0.ini` — driver config (node 0)
- `benchmark_<scale>_<N>node_n1.ini` — worker 1 config
- `benchmark_<scale>_<N>node_build.ini` — build-only config (router disabled)
- `benchmark_<scale>_<N>node_driver.ini` — run-only config (Rebuild=false)

Key config sections:
```ini
[Benchmark]
IndexPath=/mnt/nvme/proidx_1m_2node_n0/spann_index
Storage=TIKVIO
TiKVPDAddresses=127.0.0.1:23791,127.0.0.1:23792,127.0.0.1:23793

[Router]
RouterEnabled=true
RouterLocalNodeIndex=0
RouterNodeAddrs=127.0.0.1:30001,127.0.0.1:30002
RouterNodeStores=127.0.0.1:20161,127.0.0.1:20162
```

## Multi-Machine Benchmark

For running across multiple physical machines.

### Step 1: Configure Cluster

```bash
cp evaluation/distributed/cluster.conf.example cluster.conf
# Edit with your actual IPs, ports, and paths
vim cluster.conf
```

Example `cluster.conf`:
```ini
[cluster]
ssh_user=zhangt
sptag_dir=/mnt/nvme/SPTAG
data_dir=/mnt/nvme
tikv_version=v7.5.1
pd_version=v7.5.1

[nodes]
# host          router_port
10.0.1.1        30001         # driver (always first)
10.0.1.2        30002         # worker 1
10.0.1.3        30003         # worker 2

[tikv]
# host          pd_client  pd_peer  tikv_port
10.0.1.1        2379       2380     20161
10.0.1.2        2379       2380     20162
10.0.1.3        2379       2380     20163
```

### Step 2: Create Template INI

Create `configs/benchmark_<scale>_template.ini` for each scale. The script auto-fills `IndexPath`, `TiKVPDAddresses`, and the entire `[Router]` section.

### Step 3: Deploy & Run

```bash
# Deploy binary + data files to all nodes
./evaluation/distributed/run_distributed.sh deploy cluster.conf

# Start TiKV/PD cluster across all machines
./evaluation/distributed/run_distributed.sh start-tikv cluster.conf

# Run benchmark
./evaluation/distributed/run_distributed.sh run cluster.conf 1m

# Stop TiKV when done
./evaluation/distributed/run_distributed.sh stop-tikv cluster.conf

# Clean up remote files
./evaluation/distributed/run_distributed.sh cleanup cluster.conf
```

### What `run` Does

1. **Build**: Driver builds index locally with router disabled
2. **Distribute**: rsync head index + perftest data to all workers
3. **Workers**: Start worker processes on remote machines via SSH
4. **Driver**: Run driver with routing enabled; sends TCP dispatch commands
5. **Collect**: Driver sends Stop; waits for workers; collects logs

### Prerequisites for Multi-Machine

- Passwordless SSH from driver to all worker nodes
- Docker installed on all nodes (for TiKV)
- Same directory structure on all nodes (`sptag_dir`, `data_dir`)
- Network connectivity between all nodes (router ports + TiKV ports)

## Dispatch Protocol

The TCP dispatch protocol replaces file-based barriers. Communication flows through PostingRouter's existing TCP transport:

| Packet | Direction | Purpose |
|--------|-----------|---------|
| `DispatchCommand (0x09)` | Driver → Worker | Search/Insert/Stop command with dispatchId + round |
| `DispatchResult (0x89)` | Worker → Driver | Status + wallTime for aggregation |

- **Search**: Driver broadcasts to workers, runs local queries in parallel, collects wall times for percentile stats
- **Insert**: Driver broadcasts batch index, workers insert their shard, driver waits for all to finish
- **Stop**: Driver sends at end of benchmark; workers exit gracefully

Each command has a unique `dispatchId` (monotonic uint64) to avoid round collisions between search and insert operations.

## Troubleshooting

**Workers don't connect**: Check that `RouterNodeAddrs` ports are reachable between all nodes. The router uses TCP with 2 io_context threads.

**TiKV timeout on multi-machine**: Ensure PD advertise URLs use reachable IPs (not 127.0.0.1). Check `docker logs sptag-pd-0`.

**Worker exits prematurely**: Check worker logs in `benchmark_logs/`. Common causes: TiKV not ready, index path wrong, or router connection failure.

**Build fails on Java wrapper**: Known issue — build only the needed targets:
```bash
cmake --build . --target SPTAGLibStatic --target SPTAGTest -- -j$(nproc)
```
