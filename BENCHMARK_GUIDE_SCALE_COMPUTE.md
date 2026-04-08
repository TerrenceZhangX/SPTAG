# SPTAG Distributed Scale Compute Benchmark Guide

This document records the complete steps for running distributed scale benchmarks
on the current machine (`spfresh48core1`), including building SPTAG with TiKV support,
deploying a TiKV cluster, preparing data, and executing benchmarks.

## 1. Environment

| Item | Value |
|------|-------|
| Machine | spfresh48core1 |
| OS | Ubuntu 24.04 |
| CPU | 48 cores |
| Memory | 377 GB |
| Working directory | `/home/azureuser/zhangt/SPTAG` |
| Data directory | `/mnt/data_disk/sift1b` |
| TiKV data directory | `/mnt/nvme_striped/zhangt/tikv` |

## 2. Data Files

Data is located at `/mnt/data_disk/sift1b/`:

| File | Description |
|------|-------------|
| `base.1B.u8bin` | Vector file (UInt8, dim=128) |
| `query.public.10K.u8bin` | Query vector file (10K queries, UInt8, dim=128) |

All benchmark ini files have `VectorPath` and `QueryPath` set to:
```ini
VectorPath=/mnt/data_disk/sift1b/base.1B.u8bin
QueryPath=/mnt/data_disk/sift1b/query.public.10K.u8bin
```

## 3. Building SPTAG (with TiKV Support)

**Important**: `-DTIKV=ON` must be enabled, otherwise the binary cannot connect to TiKV.

```bash
cd /home/azureuser/zhangt/SPTAG

# Generate kvproto stubs if not already done
cd ThirdParty/kvproto && ./generate_cpp.sh && cd ../..

# Create Release directory and build
mkdir -p Release && cd Release
cmake .. -DTIKV=ON -DTBB=ON -DCMAKE_BUILD_TYPE=Release -DGPU=OFF
make -j$(nproc)
```

Build output: `Release/SPTAGTest`

Verify TiKV support is enabled (cmake output should contain):
```
-- Found gRPC for TiKV backend
-- Found kvproto generated sources: ...
```

## 4. TiKV Cluster Deployment

### 4.1 Configuration Files

Docker Compose file: `docker/tikv/docker-compose.yml`

Cluster composition:
- **3 PD nodes** (Docker): ports 23791, 23792, 23793
- **3 TiKV nodes** (Docker): ports 20161, 20162, 20163

TiKV configuration file: `docker/tikv/tikv.toml`

```toml
memory-usage-limit = "80GB"

[server]
grpc-concurrency = 8
grpc-memory-pool-quota = "4GB"

[raftstore]
region-max-size = "512MB"
region-split-size = "384MB"

[storage.block-cache]
capacity = "40GB"
```

### 4.2 Start Cluster

```bash
cd /home/azureuser/zhangt/SPTAG/docker/tikv
docker compose up -d

# Verify PD health
curl http://127.0.0.1:23791/pd/api/v1/health

# Verify all 3 TiKV stores are Up
curl http://127.0.0.1:23791/pd/api/v1/stores | python3 -m json.tool
```

### 4.3 Stop Cluster

```bash
cd /home/azureuser/zhangt/SPTAG/docker/tikv
docker compose down
```

### 4.4 Clean Data (Fresh Start)

`restart_tikv()` in `run_scale_benchmarks.sh` handles this automatically:
```bash
docker compose down --remove-orphans --timeout 30
docker run --rm -v /mnt/nvme_striped/zhangt/tikv:/data alpine sh -c "rm -rf /data/*"
docker compose up -d
```

## 5. Benchmark Configuration Files

All configuration files are under `Test/`.

### 5.1 Scale Dimension

| Scale | BaseVectorCount | InsertVectorCount | Search / Insert Threads | TiKVKeyPrefix |
|-------|----------------|-------------------|------------------------|---------------|
| 100k | 99,000 | 1,000 | 8 / 8 | `bench100k_` |
| 1m | 990,000 | 10,000 | 8 / 8 | `bench_` / `bench1m_` |
| 10m | 9,900,000 | 100,000 | 8 / 8 | `bench10m_` |
| 100m | 99,000,000 | 1,000,000 | 8 / 8 | `bench100m_` |

### 5.2 Node Topology

Each scale supports 1-node, 2-node, and 3-node configurations:

| Topology | Config files | Distributed routing |
|----------|-------------|---------------------|
| 1-node | `benchmark_{scale}_1node.ini` | None |
| 2-node | `benchmark_{scale}_2node_n0.ini`, `..._n1.ini` | RouterEnabled=true |
| 3-node | `benchmark_{scale}_3node_n0.ini`, `..._n1.ini`, `..._n2.ini` | RouterEnabled=true |

### 5.3 Multi-node Router Parameters (3-node example)

```ini
[BuildSSDIndex]
PostingPageLimit=12
BufferLength=8
VersionCacheTTLMs=60000
VersionCacheMaxChunks=10000        # 100k/1m; 100000 for 10m/100m
RouterEnabled=true
RouterLocalNodeIndex=0              # n0=0, n1=1, n2=2
RouterNodeAddrs=127.0.0.1:30001,127.0.0.1:30002,127.0.0.1:30003
RouterNodeStores=127.0.0.1:20161,127.0.0.1:20162,127.0.0.1:20163
```

- **RouterNodeAddrs**: Communication addresses between compute nodes
- **RouterNodeStores**: TiKV store address corresponding to each node
- **RouterLocalNodeIndex**: Index of the current node in the list

## 6. Running Benchmarks

### 6.1 Using the Automation Script (Recommended)

```bash
cd /home/azureuser/zhangt/SPTAG

# Run a single scale
./run_scale_benchmarks.sh 100k

# Run multiple scales
./run_scale_benchmarks.sh 100k 1m

# Run all scales
./run_scale_benchmarks.sh all
```

The script automatically executes the following workflow for each scale:

#### 1-node Workflow
1. `restart_tikv` — restart TiKV and clear all data
2. Run `SPTAGTest --run_test=SPFreshTest/BenchmarkFromConfig` (Rebuild=true)
3. Output: `output_{scale}_1node.json`

#### 2-node / 3-node Workflow
1. `restart_tikv` — restart TiKV and clear all data
2. **Build phase**: Run BenchmarkFromConfig as n0 (RouterEnabled temporarily disabled) to build the index into TiKV
3. `rebalance_tikv_leaders` — rebalance region leader distribution across TiKV stores
4. Copy head index to other node directories
5. **Start workers**: Run `SPTAGTest --run_test=SPFreshTest/WorkerNode` as n1/n2
6. **Query phase**: Run BenchmarkFromConfig as n0 (Rebuild=false), queries are distributed via the router
7. Stop all worker processes
8. Output: `output_{scale}_{topo}.json`

### 6.2 Manual Single-Config Run

```bash
cd /home/azureuser/zhangt/SPTAG

BENCHMARK_CONFIG=Test/benchmark_1m_1node.ini \
BENCHMARK_OUTPUT=output_1m_1node.json \
./Release/SPTAGTest --run_test=SPFreshTest/BenchmarkFromConfig --log_level=message
```

### 6.3 Logs and Output

| Artifact | Path |
|----------|------|
| Run logs | `benchmark_logs/benchmark_{scale}_{topo}*.log` |
| Result JSON | `output_{scale}_{topo}.json` |
| Index data | `proidx_{scale}_{topo}/spann_index/` |
| Truth file | `truth_{scale}_{topo}` |

## 7. Quick Reference

### Kill All Related Processes

```bash
# SPFreshTest / WorkerNode
pkill -9 -f SPTAGTest

# TiKV cluster (Docker)
cd /home/azureuser/zhangt/SPTAG/docker/tikv && docker compose down

# If TiKV runs as host binary (non-Docker)
sudo pkill -9 -f tikv-server
sudo pkill -9 -f pd-server
```

### Check Cluster Status

```bash
# PD health check
curl http://127.0.0.1:23791/pd/api/v1/health

# TiKV store status
curl -s http://127.0.0.1:23791/pd/api/v1/stores | python3 -c "
import sys, json
d = json.load(sys.stdin)
for s in d.get('stores', []):
    si = s['store']
    print(f\"  Store {si['id']}: {si['address']} - {si.get('state_name')}\")"
```

### Resume from Interruption

Edit the corresponding ini file and set `Resume` to the last completed batch number:
```ini
Rebuild=true
Resume=3     # resumes from batch 4 (0-based)
```

## 8. Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| `No such file or directory: ./Release/SPTAGTest` | Not built or wrong build directory | `mkdir Release && cd Release && cmake .. -DTIKV=ON -DTBB=ON -DCMAKE_BUILD_TYPE=Release -DGPU=OFF && make -j$(nproc)` |
| TiKV connection failure | PD/TiKV not running | `cd docker/tikv && docker compose up -d` |
| TiKV OOM | `memory-usage-limit` or `block-cache.capacity` too large | Edit `tikv.toml`, total memory should be ≤ 80% of physical memory |
| Build interrupted | TiKV crashed or process killed | Use `Resume` parameter to continue |
| Recall = 0 | Truth file doesn't match actual storage | Delete the truth file to let the program regenerate it, or set `TruthPath=none` |
| gRPC/kvproto build errors | Missing grpc dependencies or kvproto not generated | `sudo apt install libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc` + `cd ThirdParty/kvproto && ./generate_cpp.sh` |
