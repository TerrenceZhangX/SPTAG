# SPANN TiKV Storage Backend

This document explains how to build and test the TiKV storage backend for SPANN. This feature allows SPANN posting data to be stored in a TiKV distributed KV cluster, replacing local files (FILEIO) or RocksDB (ROCKSDBIO).

## Architecture Overview

```
SPANN Index
    │
    ▼
ExtraDynamicSearcher (KeyValueIO interface)
    │
    ├── ExtraFileController     (FILEIO - local files)
    ├── ExtraRocksDBController  (ROCKSDBIO - RocksDB)
    └── ExtraTiKVController     (TIKVIO - TiKV cluster)  ← NEW
            │
            ├── PD (Placement Driver) ── Region/Store routing
            └── TiKV Store ── RawPut / RawGet / RawDelete
```

## Prerequisites

### System Dependencies

```bash
# Ubuntu/Debian
sudo apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    libgrpc++-dev \
    protobuf-compiler-grpc
```

### TiKV Cluster

A running TiKV cluster is required. You can deploy one quickly with Docker Compose:

```bash
# Deploy 3 PD + 3 TiKV nodes using docker-compose.yml
cd /path/to/tikv
docker compose up -d

# Verify cluster health
curl http://127.0.0.1:2379/pd/api/v1/stores
```

Make sure at least one TiKV store has status `Up`.

## Building

### 1. Generate protobuf/gRPC C++ stubs (pre-generated; usually no need to re-run)

```bash
cd SPTAG/ThirdParty/kvproto
./generate_cpp.sh
```

### 2. CMake Build

```bash
mkdir -p Release && cd Release
cmake .. -DCMAKE_BUILD_TYPE=Release -DTIKV=ON
make -j$(nproc)
```

`-DTIKV=ON` enables TiKV backend compilation and automatically links gRPC and Protobuf libraries.

Build artifacts:
- `SPTAGTest` — Full test suite including SPFresh tests
- `spfresh` — Standalone SPFresh executable

## Running Tests

### Option 1: Built-in SPFreshTest Cases (auto-generated data)

These tests automatically generate random vector data, require no external data files, and use the default FILEIO backend:

```bash
cd Release
./SPTAGTest --run_test=SPFreshTest/TestLoadAndSave --log_level=message
```

### Option 2: BenchmarkFromConfig with TiKV Backend

#### 1. Create benchmark.ini

```ini
[Benchmark]
VectorPath=sift1b/base.100M.u8bin
QueryPath=sift1b/query.public.10K.u8bin
TruthPath=none
IndexPath=proidx/spann_index
ValueType=UInt8
Dimension=128
BaseVectorCount=10000
InsertVectorCount=20000
DeleteVectorCount=0
BatchNum=2
TopK=5
NumThreads=8
NumQueries=1000
DistMethod=L2
Rebuild=true
Resume=-1

Storage=TIKVIO
TiKVPDAddresses=127.0.0.1:2379
TiKVKeyPrefix=spann_test
```

**Key Configuration Parameters:**

| Parameter | Description |
|-----------|-------------|
| `Storage` | Set to `TIKVIO` to enable the TiKV backend |
| `TiKVPDAddresses` | PD node addresses, comma-separated (e.g., `127.0.0.1:2379,127.0.0.1:2380`) |
| `TiKVKeyPrefix` | Key namespace prefix to avoid conflicts with other data. **Use a different prefix for each fresh test run** |
| `BaseVectorCount` | Number of vectors for initial index build |
| `InsertVectorCount` | Total number of vectors to insert after initial build |
| `BatchNum` | Number of batches for insertion |
| `Rebuild` | `true` = build index from scratch; `false` = load existing index |

> **Note**: When `VectorPath`/`QueryPath` point to non-existent files, the test will automatically generate random data.

#### 2. Create the Index Directory

```bash
mkdir -p proidx/spann_index
```

#### 3. Run the Test

```bash
cd Release
BENCHMARK_CONFIG=benchmark.ini ./SPTAGTest \
    --run_test=SPFreshTest/BenchmarkFromConfig \
    --log_level=message
```

#### 4. Expected Output

On a successful run, you should see output similar to:

```
[1] ExtraDynamicSearcher:UseTiKV
[1] TiKVIO: Cluster ID: 7613649808172763460
[1] TiKVIO: PD leader is at 127.0.0.1:2379
[1] TiKVIO: Connected to PD leader at 127.0.0.1:2379
[1] TiKVIO: Initialized with key prefix 'spann_test'
...
[1] TiKVIO: Created stub for TiKV store at 127.0.0.1:20160
...
=== Benchmark 0: Query Before Insertions ===
  QPS:          504.589
...
=== Benchmark 1: Insert Performance ===
  Inserted: 10000 vectors
...
=== Benchmark 2: Query After Insertions and Deletions ===
  QPS:          510.177
...
*** No errors detected
```

## Notes

1. **Key Prefix Isolation**: Use a different `TiKVKeyPrefix` for each fresh test run; otherwise stale data may affect results.

2. **TiKV API Version**: The current implementation uses the RawKV API V1, compatible with the TiKV default configuration (`api-version: 1`).

3. **Region Routing**: The client automatically discovers Region Leaders via PD and caches routing information. When regions split or migrate, it retries with cache invalidation (up to 3 attempts).

4. **Concurrency Safety**: The Merge operation (read-modify-write) is not atomic. The SPANN framework itself guarantees posting concurrency safety through upper-level locking.

5. **Data Durability**: Posting data stored in TiKV is durably replicated via Raft. Index metadata (head index, posting sizes, etc.) is still stored on the local filesystem.

## Modified Files

| File | Changes |
|------|---------|
| `AnnService/inc/Core/SPANN/ExtraTiKVController.h` | **NEW** — TiKV KeyValueIO implementation |
| `AnnService/inc/Core/DefinitionList.h` | Added `TIKVIO` storage enum value |
| `AnnService/inc/Core/SPANN/Options.h` | Added `m_tikvPDAddresses` and `m_tikvKeyPrefix` config fields |
| `AnnService/inc/Core/SPANN/ParameterDefinitionList.h` | Registered TiKV parameter defaults |
| `AnnService/inc/Core/SPANN/ExtraDynamicSearcher.h` | Added TIKVIO initialization and LoadIndex branch |
| `AnnService/CMakeLists.txt` | Added TiKV proto sources and library linking |
| `CMakeLists.txt` | Added TIKV build option and gRPC/Protobuf find_package |
| `Test/src/SPFreshTest.cpp` | BenchmarkFromConfig now supports Storage backend configuration |
| `ThirdParty/kvproto/` | Proto definitions and generated C++ gRPC stubs |
