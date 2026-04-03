# SPANN Distributed Write Routing Layer

本文档描述 SPFRESH/SPANN 分布式写路由层的设计与实现。该层在 [TiKV 存储后端](TiKV-Integration.md) 基础上，实现多计算节点共享 TiKV 集群的水平扩展写入。

## 1. 问题背景

单节点 SPFRESH 架构中，所有 Insert → Append → Split → Merge → Reassign 操作在同一进程内完成。TiKV 后端将 posting 数据从本地磁盘解耦到分布式 KV，但写入仍然集中在单个计算节点。

当 posting 写入成为瓶颈时（例如大规模数据集下 Split/Merge 密集），需要将写入负载分散到多个计算节点。

## 2. 架构总览

```
Compute Node 0 (Driver)          Compute Node 1 (Worker)        Compute Node 2 (Worker)
┌─────────────────────────┐      ┌─────────────────────┐       ┌─────────────────────┐
│ BenchmarkFromConfig     │      │ WorkerNode          │       │ WorkerNode          │
│   ↓                     │      │   RPC Server        │       │   RPC Server        │
│ PostingRouter           │      │   ↓                 │       │   ↓                 │
│   GetOwner → PD         │─RPC─▶│ Append/Split/Merge  │       │ Append/Split/Merge  │
│   ↓                     │      │ Reassign            │       │ Reassign            │
│ Append/Split/Merge      │      │   ↓                 │       │   ↓                 │
│ Reassign                │      │ Head Index (副本)    │       │ Head Index (副本)    │
│   ↓                     │      │ TiKVVersionMap      │       │ TiKVVersionMap      │
│ Head Index (副本)        │      └─────────┬───────────┘       └──────────┬──────────┘
│ TiKVVersionMap          │                │                              │
└────────────┬────────────┘                │                              │
             │                             │                              │
             ▼                             ▼                              ▼
      ┌──────────────────────────────────────────────────────────────────────┐
      │                     TiKV 集群 (共享存储)                              │
      │   PD (调度中心)   │   Store 1   │   Store 2   │   Store 3           │
      └──────────────────────────────────────────────────────────────────────┘
```

> 架构图详见 [arch.mmd](arch.mmd)，数据流时序图详见 [flow.mmd](flow.mmd)。

### 核心思路

每个计算节点绑定一个 TiKV Store。写入时，PostingRouter 查询 PD 获取 headID 所在的 Region Leader 位于哪个 Store，然后将写请求路由到绑定该 Store 的计算节点执行。

**不变量**：同一个 headID 的所有写操作（Append / Split / Merge）总是路由到同一个计算节点，保证 per-posting 的并发安全由本地锁 `m_rwLocks[headID]` 保护。

## 3. 组件说明

### 3.1 PostingRouter (`PostingRouter.h`)

路由层的核心类，嵌入在 `ExtraDynamicSearcher` 中。

| 方法 | 作用 |
|------|------|
| `Initialize(db, localNodeIdx, nodeAddrs, nodeStores)` | 配置节点列表，建立 Store→Node 映射表 |
| `Start()` | 启动 RPC Server 监听 + 连接所有 peer 节点 |
| `GetOwner(headID)` | 查 PD 获取 key 所在 Region Leader 的 Store 地址，匹配到对应 Node |
| `SendRemoteAppend(nodeIdx, headID, headVec, appendNum, posting)` | 序列化请求通过 Socket 发送到远端，同步等待 Response（30s 超时）|
| `HandleAppendRequest()` | 收到远端请求后调用本地 `appendCallback` 执行 Append |

**路由决策流程**：
```
GetOwner(headID)
  → db->GetKeyLocation(headID)     // 查 TiKV PD
  → KeyLocation{regionId, leaderStoreAddr}
  → m_storeToNode[leaderStoreAddr] // Store→Node 映射
  → RouteTarget{nodeIndex, isLocal}
```

### 3.2 ExtraDynamicSearcher 集成

路由层在 `ExtraDynamicSearcher` 中的接入点：

**初始化** (`EnableRouter` → `InitializeRouter`)：
- 解析配置参数 (`RouterNodeAddrs`, `RouterNodeStores`)
- 创建 PostingRouter 实例
- 注册 `appendCallback`：收到远端请求时调用本地 `Append()`
- 调用 `PostingRouter::Start()` 启动 Server + Client

**写入路由** (`AddIndex` 中)：
```cpp
if (m_router && m_router->IsEnabled()) {
    auto target = m_router->GetOwner(selections[i].VID);
    if (!target.isLocal) {
        ret = m_router->SendRemoteAppend(
            target.nodeIndex, selections[i].VID, headVec, 1, appendPosting);
        continue;
    }
}
// 本地路径: AsyncAppend 或 Append
```

### 3.3 WorkerNode (`SPFreshTest.cpp`)

独立的 Boost.Test 用例，作为 Worker 进程运行：
1. 加载预构建的 Head Index
2. 连接 TiKV 集群
3. 调用 `ApplyRouterParams()` 启用 PostingRouter
4. 阻塞等待，通过 PostingRouter Server 处理来自 Driver 的远端 Append 请求
5. 收到停止信号文件（`worker_stop_N`）或超时后退出

### 3.4 配置参数

在 `[BuildSSDIndex]` 节中设置：

| 参数 | 类型 | 说明 |
|------|------|------|
| `RouterEnabled` | bool | 是否启用分布式路由 |
| `RouterLocalNodeIndex` | int | 本节点在节点列表中的索引 (0-based) |
| `RouterNodeAddrs` | string | 所有节点的路由 RPC 地址，逗号分隔：`host1:port1,host2:port2,...` |
| `RouterNodeStores` | string | 每个节点绑定的 TiKV Store 地址，逗号分隔：`storeAddr1,storeAddr2,...` |

**示例** (2 节点)：
```ini
# Node 0 (Driver)
[BuildSSDIndex]
RouterEnabled=true
RouterLocalNodeIndex=0
RouterNodeAddrs=127.0.0.1:30001,127.0.0.1:30002
RouterNodeStores=127.0.0.1:20161,127.0.0.1:20162

# Node 1 (Worker) — 只改 RouterLocalNodeIndex
[BuildSSDIndex]
RouterEnabled=true
RouterLocalNodeIndex=1
RouterNodeAddrs=127.0.0.1:30001,127.0.0.1:30002
RouterNodeStores=127.0.0.1:20161,127.0.0.1:20162
```

## 4. 数据流

### 4.1 Insert 全路径

```
Client Insert(vector, VID)
  → SPANN::Index::AddIndex()
    → ExtraDynamicSearcher::AddIndex()
      → RNGSelection() → 选出 head replicas [headID_1, headID_2, ...]
      → 对每个 headID:
          PostingRouter::GetOwner(headID) →  查 PD
          ├── isLocal=true  → 本地 Append(headID, posting)
          └── isLocal=false → SendRemoteAppend() → 远端 Worker 执行 Append()
```

### 4.2 Append 级联操作

Append 完成后，根据 posting 大小触发异步维护操作：

| 条件 | 操作 | 说明 |
|------|------|------|
| `size > postingSizeLimit + bufferSizeLimit` | 同步 Split | 紧急分裂，阻塞当前线程 |
| `size > postingSizeLimit` | 异步 SplitAsync | 提交给 splitThreadPool |
| `size < mergeThreshold` | 异步 MergeAsync | 提交给 splitThreadPool |

**Split 流程**：
1. 读取完整 posting
2. KMeans 聚类分成 2 个新 posting
3. 写入新 posting，删除旧 posting
4. 更新 Head Index（AddIndex / DeleteIndex）
5. 对散落向量执行 ReassignAsync

**Reassign 流程**：
1. 对离新 head 太远的向量重新 RNGSelection
2. 找到更近的 head 后 IncVersion + Append 到新 head

### 4.3 Checkpoint

`SaveIndex()` 时调用 `Checkpoint()`，循环等待 `AllFinished()`（即 splitThreadPool + reassignThreadPool 全部清空），然后持久化 VersionMap 和 TiKV 数据。

## 5. 运行方法

### 前置条件

-  按 [TiKV-Integration.md](TiKV-Integration.md) 部署 TiKV 集群
- `cmake -DTIKV=ON -DTBB=ON -DCMAKE_BUILD_TYPE=Release` 构建

### 2 节点测试

```bash
# 1) 构建初始索引 (Driver 节点)
mkdir -p proidx_2node_n0/spann_index
BENCHMARK_CONFIG=Test/benchmark_100k_2node_n0.ini \
BENCHMARK_OUTPUT=output_2node_build.json \
  ./Release/SPTAGTest --run_test=SPFreshTest/BenchmarkFromConfig

# 2) 复制 Head Index 给 Worker
mkdir -p proidx_2node_n1/spann_index
cp -r proidx_2node_n0/spann_index/* proidx_2node_n1/spann_index/

# 3) 启动 Worker (后台)
BENCHMARK_CONFIG=Test/benchmark_100k_2node_n1.ini \
  ./Release/SPTAGTest --run_test=SPFreshTest/WorkerNode &
WORKER_PID=$!
sleep 3

# 4) 启动 Driver (Rebuild=false，使用已有索引)
sed 's/^Rebuild=true/Rebuild=false/' Test/benchmark_100k_2node_n0.ini > /tmp/run.ini
BENCHMARK_CONFIG=/tmp/run.ini \
BENCHMARK_OUTPUT=output_2node.json \
  ./Release/SPTAGTest --run_test=SPFreshTest/BenchmarkFromConfig

# 5) 停止 Worker
touch worker_stop_1
wait $WORKER_PID
```

## 6. 修改文件清单

| 文件 | 变更 |
|------|------|
| `AnnService/inc/Core/SPANN/PostingRouter.h` | **新增** — 路由层核心：GetOwner、SendRemoteAppend、RPC Server/Client |
| `AnnService/inc/Core/SPANN/ExtraDynamicSearcher.h` | AddIndex 加入路由判断；InitializeRouter / EnableRouter 方法 |
| `AnnService/inc/Core/SPANN/Index.h` | EnableRouter() 转发到 layer 0 extra searcher |
| `AnnService/inc/Core/SPANN/Options.h` | 新增 m_routerEnabled / m_routerLocalNodeIndex / m_routerNodeAddrs / m_routerNodeStores |
| `AnnService/inc/Core/SPANN/ParameterDefinitionList.h` | 注册 RouterEnabled / RouterLocalNodeIndex / RouterNodeAddrs / RouterNodeStores |
| `Test/src/SPFreshTest.cpp` | ApplyRouterParams() 辅助函数；WorkerNode 测试用例；BenchmarkFromConfig 中调用 |
| `AnnService/inc/Socket/Packet.h` | 新增 AppendRequest/AppendResponse PacketType |

## 7. 设计考量与已知问题

### 7.1 当前 Prototype 的局限性

本实现为功能验证原型。以下是已识别的需要生产化改进的问题：

#### (1) Reassign 不经过 Router

**现状**：Split/Merge 后的 Reassign 调用 `Append()` 时直接走本地路径，不查 Router。
```cpp
// Reassign() L1449:
ErrorCode tmp = Append(p_exWorkSpace, selections[i].VID, ..., 1, *vectorInfo, 3);
// ↑ 直接调用本地 Append，没有经过 Router 路由
```

**影响**：如果 Reassign 选出的新 headID 本应路由到其他 Node，向量会被写到错误的 Node，破坏 "同一 headID 的写操作由同一 Node 处理" 的不变量。

**修复方案**：Reassign 中的 Append 调用需增加 Router 检查，与 AddIndex 中相同的路由 逻辑。

#### (2) Head Index 无跨节点同步

**现状**：Split 产生新 head 时调用 `AddHeadIndex()`，Merge 删除 head 时调 `DeleteIndex()`，这些操作只改变本地 Head Index。其他 Node 不知道 Head 集合变化了。

**影响**：
- **不会丢数据**：checkDeleted 机制会把找不到 head 的向量重新 Reassign
- **影响查询召回率**：Worker 创建的新 head 对 Driver 不可见，Driver 的 Query 搜索不到这些新 head 对应的 posting

**修复方案**：Split/Merge 后异步广播 Head 变更到所有 peer Node。Fire-and-forget 即可，不影响写入吞吐。

#### (3) VersionMap IncVersion 非原子操作

**现状**：`TiKVVersionMap::IncVersion()` 使用 Read → Modify → Write 模式，没有 CAS 保证。

**影响**：多 Node 并发对同一 VID 执行 IncVersion 时，可能出现 lost update。实际场景中同一 VID 并发写概率较低（路由保证同一 headID 归同一 Node），但 Reassign 可能导致 VID 跨 Node。

**修复方案**：使用 TiKV 的 `RawCompareAndSwap` 替换 Read-Modify-Write。

#### (4) SendRemoteAppend 同步阻塞

**现状**：`SendRemoteAppend()` 发送请求后同步 `future.wait_for(30s)` 阻塞调用线程。

**影响**：Driver 的 insert 线程在等待远端响应期间完全阻塞，严重降低吞吐。这是当前 throughput 仅 ~7 vec/s 的主要原因之一。

**修复方案**：
- 短期：增加 insert 并发线程数
- 长期：改为异步 RPC，SendRemoteAppend 提交后立即返回，响应通过回调处理

#### (5) TiKV Region 迁移后路由失效

**现状**：`GetOwner()` 的路由决策基于当前 Region Leader 所在的 Store。如果 TiKV 发生 Region 迁移（Leader Transfer / Region Split / Region Merge），同一 key 的 owner 可能变化。

**影响**：路由切换瞬间，可能有两个 Node 同时在操作同一 headID 的 posting，破坏并发安全。

**修复方案**：
- 方案 A：路由版本号 — 每次 Region 变更递增版本，操作前后校验版本一致
- 方案 B：依赖 TiKV 事务 — 使用 TxnKV 代替 RawKV，由 TiKV 保证并发安全

#### (6) Merge 跨 Node 问题

**现状**：Merge 需要原子操作两个 posting（读取 neighbor + 合并写入 survivor + 删除 victim）。如果两个 posting 分别路由到不同 Node，Merge 操作无法简单执行。

**修复方案**：Merge 操作强制在同一 Node 执行；如果 neighbor 在远端，先将 neighbor 数据拉取到本地再合并。

### 7.2 性能分析

**100K 基准测试结果**（UInt8, 128 维, 90K base + 10K insert, 10 批次）：

| 指标 | 1 节点 (无路由) |
|------|-----------------|
| Insert throughput | 7.25 vec/s (平均) |
| Insert throughput 范围 | 6.01 ~ 9.97 vec/s |
| Query latency (p50) | ~136 ms |
| Query QPS | ~114 |
| Recall@5 | 0.707 ~ 0.719 |
| Build time | 2109.9 s |

**对比历史数据**：之前单机测试 throughput 可达数百至上千 vec/s，latency ~25ms。当前 7 vec/s 的性能退化需要排查。

**初步原因分析**：
- TiKV 网络往返开销（本地磁盘 vs 远端 KV）
- BenchmarkFromConfig 的 Clone/Reload 逻辑可能引入额外开销
- Split/Merge 密集时 ThreadPool 竞争
- `InsertThreadNum=1` 限制了并发（配置中只有 1 个 insert 线程）
- 需要进一步 profile 确认具体瓶颈

### 7.3 生产化路线图

| 优先级 | 改进项 | 复杂度 | 影响 |
|--------|--------|--------|------|
| P0 | 排查 throughput 退化 (7 vs 1000 vec/s) | 中 | 基准性能 |
| P0 | Reassign 经过 Router | 低 | 数据正确性 |
| P1 | Head Index 跨节点广播 | 中 | 查询召回率 |
| P1 | VersionMap CAS 原子操作 | 低 | 并发安全 |
| P2 | 异步 RPC 替换同步等待 | 高 | 写入吞吐 |
| P2 | Region 迁移安全 | 高 | 运行时正确性 |
| P3 | Merge 跨 Node 协调 | 中 | 功能完整性 |
