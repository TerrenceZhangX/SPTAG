# 对等模式架构设计：2000节点 SPTAG 分布式 Insert/Search

## Key Design Principles

```
  ┌───────────────────────────────────────────────────────────────────────────┐
  │                                                                           │
  │  P1  Symmetric Peers — 无特殊角色                                         │
  │      每个节点角色完全相同: 接收 client 请求 + 持有 posting 分区            │
  │      无 Driver/Master, 消除单点故障                                       │
  │                                                                           │
  │  P2  Shared Storage (TiKV) — Owner ≠ 数据所在地                           │
  │      所有 posting 存 TiKV (Raft 3 副本), 任何节点可读写任何 headID        │
  │      "Owner" 仅指 "谁负责处理请求", 不指 "数据在哪"                       │
  │      → 扩缩容零数据迁移, ring 不一致期间新旧 owner 都能正确读写           │
  │                                                                           │
  │  P3  Synchronous Write Semantics — 诚实、不骗 client                      │
  │      SUCCESS = 数据 100% 持久化在 TiKV                                    │
  │      FAIL / TIMEOUT = client 明确知道失败, 自行 retry                     │
  │      绝不出现: client 收到 SUCCESS 但数据没写进去                          │
  │      无 Outbox、无后台异步补偿、无隐式修复                                 │
  │                                                                           │
  │  P4  Idempotent Retry — retry 永远安全                                    │
  │      同 Ingress + idempotency_key → 缓存命中, 不重复执行                  │
  │      不同 Ingress → 新 VID, 旧的变孤儿 (无害) 或重复 (去重)              │
  │      Append 层 VID+version check → 同 VID 写两次自动 skip                │
  │                                                                           │
  │  P5  Local-First — 热路径零网络                                            │
  │      VID 分配: 静态分段 + 本地 atomic++ (零网络)                           │
  │      路由决策: 本地 hash ring 查 GetOwner() (零网络)                       │
  │      Head search: 本地 BKT index (零网络)                                 │
  │      唯一需要网络的热路径: remote Append 到 Owner                          │
  │                                                                           │
  │  P6  Consistent Hashing — 增删节点最小影响                                 │
  │      FNV-1a + 150 vnodes/node → 增删 1 节点只影响 ~1/N 的 headID         │
  │      RCU lock-free ring 更新 → 读路径无锁                                 │
  │      TiKV 共享存储 → ring 变更期间读写仍然正确                             │
  │                                                                           │
  │  P7  Gossip Membership (SWIM) — 去中心化故障发现                           │
  │      每秒 Ping 1 个 peer, 怀疑时 PingReq 间接确认                        │
  │      O(log N) 轮收敛 (~11s @ 2000 nodes)                                  │
  │      自动 AddNode/RemoveNode → ring 自愈                                  │
  │                                                                           │
  │  P8  Fast Failure Detection — 不反复等超时                                 │
  │      Owner Blacklist (15s TTL): 第一次超时后标记, 后续请求 ms 级快速失败   │
  │      与 Gossip SWIM (~8.5s) 互补: blacklist 桥接 gossip 检测空窗期        │
  │                                                                           │
  │  P9  Graceful Degradation — Search 永远可用                                │
  │      Owner 不可达 → TiKV 直读 fallback (任何 node 都能读)                 │
  │      TiKV 也失败 → 跳过该 headID (recall 降低, 不报错)                    │
  │      标注 degraded=true → client 知道结果可能不完整                        │
  │                                                                           │
  │  P10 Crash Recovery — 重启即恢复, 无手动干预                               │
  │      VID 计数器: 扫 TiKV 恢复 m_next (防冲突)                             │
  │      孤儿 posting: 无害 (数据正确, 上层过滤/GC 清理)                       │
  │      幂等缓存: 丢失无影响 (最多多一次重复写入)                             │
  │      FreeList: 下次 GC 扫描重建                                           │
  │                                                                           │
  └───────────────────────────────────────────────────────────────────────────┘
```

---

## 0. 现状分析

当前 `PostingRouter` **路由层已经是对称的**——每个节点同时运行 Server 和 Client，一致性哈希决定 headID owner，`AddNode()`/`RemoveNode()` 支持环变更。"Driver vs Worker"仅是测试入口的区别（node 0 执行 Rebuild + Insert，其余 node 等待停止信号）。

### 不可直接扩到 2000 节点的瓶颈

| 瓶颈 | 根因 | 量级（2000 节点） |
|------|------|--------------------|
| **Full-mesh TCP** | `Start()` 中每个 node 连接所有 peer | 2000×1999 = ~4M 半连接 |
| **VID 单调递增** | `m_iCurAssignedVID` 在发起 insert 的节点上自增 | 只有发起者分配 VID，其他节点需要全局锁或分段预分配 |
| **Head index 全量广播** | `BroadcastHeadSync()` fire-and-forget 到 N-1 个 peer | 一次 split/merge → 1999 条 TCP 广播 |
| **静态成员配置** | `RouterNodeAddrs` INI 写死 | 加节点需改配置重启全集群 |
| **无自动数据迁移** | `ComputeMigration()` 仅计算列表，不执行搬迁 | 扩容后手动分发 posting |

---

## 1. 整体架构

```
                    ┌──────────────────┐
                    │  Seed Registry   │   (etcd / ZooKeeper / TiKV-PD)
                    │  - membership    │
                    │  - VID counters  │
                    │  - epoch / ring  │
                    └────────┬─────────┘
                             │ heartbeat + watch
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                   ▼
    ┌──────────┐       ┌──────────┐        ┌──────────┐
    │  Node 0  │◄─────►│  Node 1  │◄──────►│ Node 1999│
    │ (peer)   │       │  (peer)  │        │  (peer)  │
    │          │◄──────►          │◄──────►│          │
    └──────────┘       └──────────┘        └──────────┘
        同一角色：接收 client 请求、本地 RNGSelection、点对点 Append
```

### 核心原则

1. **每个节点角色完全相同**——既接收 client insert/search，又持有自己的 posting 分区
2. **连接拓扑不再是 full-mesh**——改为按需连接 + gossip 转发
3. **VID 分段预分配**——每个 node 从注册中心领取互不重叠的 VID range，本地自增无锁
4. **Head index 分层同步**——gossip 增量传播 + 定期全量校验
5. **数据迁移自动化**——ring 变更后各 node 自主推送受影响 headID 的 posting

---

## 2. 数据链路（Insert 路径）

### 2.1 Client → 任意 Node（负载均衡层）

```
Client ──(HTTP/gRPC)──► LB ──► Node K
```

负载均衡器（L4 或 DNS 轮询）将请求随机分配到任意 node。Node K 成为这批向量的**入口节点（Ingress Node）**。

### 2.2 Ingress Node：VID 分配 + 本地 RNGSelection

```
Node K 收到 batch of M vectors:

for each vector v[i]:
  1. VID = localVIDCounter++       // 从本 node 预分配的 range 中取
  2. heads[] = RNGSelection(v[i])  // 本地 BKT head index 副本
  3. for each head h in heads[]:
       owner = hashRing.GetOwner(h.headID)
       if owner == K:
           localAppend(h.headID, VID, v[i])
       else:
           appendBuffer[owner].push(h.headID, VID, v[i])
  4. Flush appendBuffer  // 点对点批量发送
```

**关键改动**（对比现有代码）：
- VID 不再需要 `VID % numNodes` 来路由到"负责 insert 的 node"——**每个 node 自己分配 VID、自己做 RNGSelection**
- `QueueRemoteInsert()` / `InsertBatchRequest` 路径被移除（不再需要把原始向量转发给"VID owner"做 RNG）
- `QueueRemoteAppend()` + `FlushRemoteAppends()` 保留——这是纯粹的点对点 posting append

### 2.3 数据流图

```
Client
  │
  ▼  (1) Insert(vectors)
Node K (Ingress)
  │
  ├─ (2) Allocate VIDs from local range [K*block .. K*block+block-1]
  │
  ├─ (3) RNGSelection (local head index replica)
  │      → headID=42 (owner=Node 7), headID=99 (owner=Node K)
  │
  ├─ (4a) headID=99 → localAppend()
  │
  └─ (4b) headID=42 → BatchAppendRequest → Node 7
                                              │
                                              ▼
                                         Node 7: Append(headID=42, posting)
                                              │
                                              ▼
                                         TiKV Store (Node 7's partition)
```

### 2.4 与现有代码的映射

| 现有组件 | 对等模式改动 |
|----------|-------------|
| `ExtraDynamicSearcher::AddIndex()` L2621 | 删除 `VID % numNodes` 分支，所有 VID 直接本地 RNGSelection + route by headID |
| `PostingRouter::QueueRemoteInsert()` | **删除**——不再需要 |
| `PostingRouter::SendInsertBatch()` | **删除**——不再需要 |
| `PostingRouter::HandleInsertBatchRequest()` | **删除**——不再需要 |
| `PostingRouter::QueueRemoteAppend()` | **保留**，这是点对点 append 的基础 |
| `PostingRouter::FlushRemoteAppends()` | **保留** |
| `PostingRouter::GetOwner()` | **保留**，一致性哈希核心 |

---

## 3. 数据链路（Search 路径）

### 3.1 两层搜索

```
Client ──► Node K (Ingress)
  │
  ├─ (1) Head Search: BKT search on local head index replica → candidate headIDs
  │
  ├─ (2) Group headIDs by owner node:
  │      Node 7 owns [headID=42, headID=55]
  │      Node K owns [headID=99]
  │      Node 312 owns [headID=1001]
  │
  ├─ (3a) Local: SearchPosting(headID=99)
  │
  ├─ (3b) Parallel fan-out:
  │       SearchPostingRequest → Node 7   (headIDs=[42,55])
  │       SearchPostingRequest → Node 312 (headIDs=[1001])
  │
  └─ (4) Merge results from all nodes, return top-K to client
```

### 3.2 扇出优化（2000 节点关键）

Head search 通常返回 32~64 个 candidate headIDs。在 2000 节点下，一致性哈希会把它们分散到 ~32~64 个不同 node（极端情况）。但实际上：

- **Posting 数据的 locality**：150 个 vnode/node 意味着每个 node 持有 ring 上 ~0.05% 的 headID 空间。32 个 candidate 平均只命中 ~32 个不同 node。
- **优化**：增大 vnode 数量不影响扇出；重要的是**批量合并同一 node 的请求**（已有 `BatchSearchRequest`）。

对于 2000 节点的搜索扇出问题：
- 实际扇出 = min(candidateCount, numNodes) ≈ 32~64
- **不是 O(N) 扇出**，因此可接受

---

## 4. 连接拓扑：从 Full-Mesh 改为按需连接

### 4.1 问题

Full-mesh 在 2000 节点下 = 2000 × 1999 / 2 ≈ 2M 条 TCP 连接，不可行。

### 4.2 方案：**按需连接池 + LRU 淘汰**

```cpp
class ConnectionPool {
    // Max concurrent peer connections per node
    static constexpr int MAX_ACTIVE_CONNECTIONS = 200;

    struct PeerConn {
        Socket::ConnectionID connID;
        std::chrono::steady_clock::time_point lastUsed;
    };

    // LRU map: nodeIndex → connection
    std::unordered_map<int, PeerConn> m_activeConns;
    std::mutex m_connMutex;

    ConnectionID GetOrConnect(int nodeIndex) {
        std::lock_guard<std::mutex> lock(m_connMutex);
        auto it = m_activeConns.find(nodeIndex);
        if (it != m_activeConns.end()) {
            it->second.lastUsed = now();
            return it->second.connID;
        }
        // Evict LRU if at capacity
        if (m_activeConns.size() >= MAX_ACTIVE_CONNECTIONS) {
            EvictLRU();
        }
        auto connID = m_client->ConnectToServer(
            m_nodeAddrs[nodeIndex].first,
            m_nodeAddrs[nodeIndex].second);
        m_activeConns[nodeIndex] = {connID, now()};
        return connID;
    }
};
```

### 4.3 对现有代码的改动

| 现有 | 改为 |
|------|------|
| `m_peerConnections` (vector, 全部预连) | `ConnectionPool` (LRU map, 按需连) |
| `Start()` 中 `for (i: allNodes) ConnectToPeer(i)` | 删除。延迟到首次发送时连接 |
| `GetPeerConnection(nodeIndex)` | 改调 `ConnectionPool::GetOrConnect(nodeIndex)` |
| `InvalidatePeerConnection(nodeIndex)` | 同时从 LRU 中移除 |

### 4.4 连接数估算

- 每个 node 在 steady state 下活跃连接 ≈ 50~200（取决于 headID 分布热度）
- 全集群维持的 TCP 连接 ≈ 2000 × 100 = 200K（vs full-mesh 的 4M）
- LRU 淘汰设 60s idle timeout → 冷节点连接自动释放

---

## 5. 成员管理：Gossip + Seed Registry

### 5.1 三种角色不再区分

```
所有节点 = Peer Node
  ├─ 接受 client 请求 (Ingress)
  ├─ 持有 posting 分区 (Storage Owner)
  ├─ 持有 head index 副本 (Search)
  └─ 参与 gossip 协议 (Membership)
```

### 5.2 成员发现协议

使用已有的 TiKV-PD 作为 Seed Registry（不引入新依赖）：

```
启动流程:
1. Node 启动，向 PD 注册自己: PUT /nodes/{nodeID} = {addr, port, status=joining}
2. 从 PD 读取当前成员列表: GET /nodes/ → [{node0, addr0}, {node1, addr1}, ...]
3. 构建本地 ConsistentHashRing
4. 进入 gossip 循环 (增量成员变更通过 gossip 传播)
5. 状态改为 active

退出/故障:
1. 正常退出: DEL /nodes/{nodeID}, gossip LeaveMessage
2. 故障检测: PD lease 过期(TTL=30s) + gossip SuspectMessage → 确认后 RemoveNode
```

### 5.3 Gossip 协议（SWIM 变体）

```cpp
// 新增 PacketType
MembershipPing    = 0x10  // 心跳探测
MembershipPingReq = 0x11  // 间接探测（A 怀疑 B 挂了，请 C 帮忙 ping B）
MembershipAck     = 0x12  // 心跳回复
MembershipUpdate  = 0x13  // 成员变更广播（piggyback on any message）

struct MembershipEntry {
    int nodeIndex;
    std::string addr;
    std::string port;
    uint64_t epoch;           // 全局递增 epoch（from PD）
    MemberState state;        // Alive | Suspect | Dead | Left
    uint32_t incarnation;     // 递增计数，用于 refute suspect
};
```

**Gossip 循环**（每个 node 独立运行）：

```
every 1 second:
  1. 随机选 1 个 peer → send MembershipPing
     - piggyback 最近 K 条 MembershipUpdate (K=10)
  2. if no Ack in 500ms:
     - 随机选 3 个 peer → send MembershipPingReq(target=suspect)
     - if still no Ack in 2s → mark target as Suspect
  3. if Suspect for > 5s → mark as Dead → gossip DeadMessage
  4. 处理收到的 MembershipUpdate:
     - Alive with higher incarnation → update local state
     - Suspect 指向自己 → refute: incarnation++ → gossip Alive
     - Dead → RemoveNode() from ring, 触发数据迁移
```

**扩散速度**：SWIM gossip 在 N=2000 节点下，一条消息传播到所有节点需 ~O(log N) 轮 ≈ 11 轮 ≈ 11 秒。

### 5.4 对现有代码的改动

```
新增文件:
  AnnService/inc/Core/SPANN/GossipProtocol.h
  AnnService/src/Core/SPANN/GossipProtocol.cpp

修改 PostingRouter.h:
  - Start() 中不再读 RouterNodeAddrs INI → 改为从 PD 拉成员列表
  - 新增 StartGossip() 方法
  - AddNode()/RemoveNode() 由 gossip 回调触发，不再手动调用
  
修改 Socket/Packet.h:
  - 新增 MembershipPing/PingReq/Ack/Update PacketType
```

---

## 6. VID 分段预分配

### 6.1 方案

VID 唯一要求：全局唯一。不需要有序，不需要连续，只要不重叠。

```
VID 空间: [0, 2^31)  ≈ 2.1 billion

静态分段 (启动时本地计算, 零网络):
  BLOCK_SIZE = 2^31 / MAX_NODES        // MAX_NODES = 4096 (预留扩容空间)
            = 524,288

  Node 0:     [0,        524,288)
  Node 1:     [524,288,  1,048,576)
  ...
  Node 1999:  [1,047,527,424,  1,048,051,712)

运行时:
  VID = m_rangeStart + m_localCounter++    // 本地 atomic, 零网络

VID 回收 (GC):
  Delete 操作标记 VID 为 deleted (versionMap)
  GC 将 deleted VID 放回空闲列表, 新 insert 优先复用
  活跃 VID 数 = inserted - deleted, 只要活跃数 < 524,288 就永远不会用完
  正常场景下 overflow 不会触发

Block 用完 (安全兜底, 正常不会触发):
  从 TiKV 原子抢下一个空闲 block
  key = "__vid_overflow__"
  value = 下一个可分配的 block 起始地址 (初始 = MAX_NODES * BLOCK_SIZE)
  操作 = TiKV 事务 CAS (走该 key 所在 region 的 Raft leader, 不是 PD)
```

为什么不找 PD：
- PD 是 TiKV 的 region 调度器，不负责业务 key
- VID 分配只是对一个普通 TiKV key 做原子递增
- 这跟任何 TiKV 读写一样，走 Raft leader，不经过 PD
- 但 99.9% 的 VID 分配是纯本地 atomic++，根本不涉及网络

### 6.2 实现

```cpp
class DistributedVIDAllocator {
    SizeType m_rangeStart;              // 当前 block 起始
    SizeType m_rangeEnd;                // 当前 block 结束
    std::atomic<SizeType> m_next;       // 下一个可分配 VID

    static constexpr int MAX_NODES = 4096;
    static constexpr SizeType BLOCK_SIZE = (1u << 31) / MAX_NODES;  // 524,288

    /// 启动时 Phase 1: 计算 range 边界 (纯本地, 零网络)
    void InitBlock(int nodeIndex) {
        m_rangeStart = static_cast<SizeType>(nodeIndex) * BLOCK_SIZE;
        m_rangeEnd = m_rangeStart + BLOCK_SIZE;
        m_next.store(m_rangeStart);  // 临时值, Phase 2 会覆盖
    }

    /// 启动时 Phase 2: 扫 TiKV 恢复真实计数器 (防止 crash 后 VID 冲突)
    void RecoverVIDCounter() {
        SizeType maxUsed = m_tikvClient->RangeScanMaxKey(
            "__vid__", m_rangeStart, m_rangeEnd);
        if (maxUsed >= m_rangeStart) {
            m_next.store(maxUsed + 1);
        }
        // else: 该 range 无已用 VID, m_next 保持 m_rangeStart
    }

    /// 热路径: 本地 atomic, 零网络
    SizeType AllocateVID() {
        // 优先从回收列表取 (GC 后的 deleted VID)
        if (!m_freeList.empty()) {
            return m_freeList.pop();
        }
        SizeType vid = m_next.fetch_add(1);
        if (vid < m_rangeEnd) [[likely]] {
            return vid;
        }
        // 安全兜底: block 用完 (正常有 GC 回收不会到这)
        return AllocateOverflowVID();
    }

    /// GC 回收: Delete 后把 VID 放回空闲列表
    void RecycleVID(SizeType vid) {
        m_freeList.push(vid);
    }

    /// 冷路径: TiKV CAS 抢新 block (每 52 万次 insert 最多一次)
    SizeType AllocateOverflowVID() {
        std::lock_guard<std::mutex> lock(m_overflowMutex);
        // double check
        if (m_next.load() < m_rangeEnd) {
            return m_next.fetch_add(1);
        }
        // TiKV 原子递增: 普通 TiKV 事务, 走 Raft leader
        auto txn = m_tikvClient->Begin();
        std::string val = txn->Get("__vid_overflow__");
        SizeType current = val.empty()
            ? static_cast<SizeType>(MAX_NODES) * BLOCK_SIZE  // 初始溢出地址
            : DecodeInt(val);
        txn->Put("__vid_overflow__", EncodeInt(current + BLOCK_SIZE));
        if (!txn->Commit()) {
            // CAS 冲突 → 重试 (极低概率, 另一个 node 同时在抢)
            return AllocateOverflowVID();
        }
        m_rangeStart = current;
        m_rangeEnd = current + BLOCK_SIZE;
        m_next.store(m_rangeStart);
        return m_next.fetch_add(1);
    }
};
```

### 6.3 对现有代码的改动

| 现有 | 改为 |
|------|------|
| `SPANN::Index::m_iCurAssignedVID` (单调自增) | `DistributedVIDAllocator::AllocateVID()` |
| `VID % numNodes` 确定处理节点 | **删除**——VID 只是一个 ID，不再决定处理节点 |
| `QueueRemoteInsert(targetNode, ...)` | **删除**——所有 insert 都在 ingress node 本地完成 RNGSelection |

---

## 7. Head Index 同步：从全量广播到 Gossip 增量传播

### 7.1 现有问题

`BroadcastHeadSync()` 向 N-1=1999 个节点逐一发送 `HeadSyncRequest`，fan-out = O(N)。

### 7.2 方案：Gossip 式增量传播 + Epoch 校验

```
Head Sync Entry:
  { epoch, op(Add/Delete), headVID, headVector, originNode }

传播方式:
  1. 当 node K 执行 split/merge 产生 head 变更:
     - 写入本地 HeadSyncLog (append-only, keyed by epoch)
     - 将变更条目 piggyback 到下一轮 gossip 消息
  2. 收到 gossip 的 node 检查 epoch:
     - if epoch > local_applied_epoch → apply + continue gossip
     - if epoch <= local_applied_epoch → skip (已应用过)
  3. 全量同步 (catch-up):
     - 新加入的 node 或 落后太多的 node
     - 向任意 peer 请求 HeadSyncLog[from_epoch..latest_epoch]
     - 或直接从 TiKV 拉取完整 head index snapshot

扩散时间: O(log N) 轮 ≈ 11 秒传遍 2000 节点
```

### 7.3 一致性保证

Head index replica **允许短暂不一致**：
- Insert 路径：RNGSelection 用的是本地 head index，轻微过时只影响 head 选择质量（选到一个旧 head），不影响正确性
- Search 路径：同理，miss 一个新 head 只降低 recall 约 0.1%
- **最终一致**：gossip 保证所有节点最终同步到相同 epoch

### 7.4 对现有代码的改动

```
修改 PostingRouter.h:
  - BroadcastHeadSync() → GossipHeadSync()
    不再 for-loop 发给所有 peer
    改为写入本地 HeadSyncLog + 标记为待传播

新增:
  - HeadSyncLog: append-only log in TiKV, key = "head_sync_{epoch}"
  - GetHeadSyncLog(fromEpoch, toEpoch) RPC for catch-up
```

---

## 8. 弹性伸缩

### 8.1 扩容流程（新增 Node 2000）

```
时序图:

t=0   Node 2000 启动
      ├─ 注册到 PD: PUT /nodes/2000 = {addr, port, status=joining}
      ├─ 读取成员列表 → 构建 hash ring (不含 self)
      ├─ 从 PD 领取 VID block
      └─ 从 peer 拉取 head index snapshot (全量)

t=5s  Head index ready
      ├─ 加入 gossip 网络，开始接收 MembershipUpdate
      ├─ 状态改为 status=syncing
      └─ 本地 AddNode(2000) → 新 hash ring 生效

t=5s  Ring 变更通过 gossip 传播到所有节点 (~11轮)

t=16s 所有节点的 ring 已包含 Node 2000
      各节点执行:
      ├─ ComputeMigration(localHeadIDs) → 找到需要迁移给 Node 2000 的 headIDs
      └─ 启动 MigrationSender 后台任务:
          for each headID to migrate:
            1. 读取 posting data from local TiKV
            2. BatchAppendRequest → Node 2000 (复用现有 RPC)
            3. 确认成功后本地标记为 "migrated"

t=?   Migration 完成
      ├─ Node 2000 状态改为 status=active
      └─ 开始接受 client 请求
```

### 8.2 缩容流程（Node 500 下线）

```
正常下线 (Graceful):
t=0   Node 500 收到 drain signal
      ├─ 状态改为 status=draining
      ├─ 停止接受新 client 请求
      ├─ 等待所有 in-flight append 完成
      └─ gossip LeaveMessage

t=1s  其他节点收到 Leave
      ├─ RemoveNode(500) from ring
      └─ 不需要数据迁移! posting data 在 TiKV 中
          新 owner 通过 hash ring 自动路由，TiKV 数据仍可访问

t=3s  Node 500 shutdown

故障下线 (Crash):
t=0    Node 500 crash
t=1s   Gossip peer 发送 Ping → 无 Ack
t=1.5s PingReq 间接探测 → 无 Ack
t=3.5s 标记为 Suspect → gossip SuspectMessage
t=8.5s 标记为 Dead → gossip DeadMessage → RemoveNode(500)
       posting data 在 TiKV 中，新 owner 自动接管
```

### 8.3 数据迁移协议

```cpp
// 新增 RPC PacketType
MigrationDataRequest  = 0x14  // "请把这些 headID 的 posting 发给我"
MigrationDataResponse = 0x94  // 批量 posting data
MigrationComplete     = 0x15  // "我已完成迁移，你可以清理"

struct MigrationTask {
    int sourceNode;
    int targetNode;
    std::vector<SizeType> headIDs;
    uint64_t ringEpoch;  // 防止和更新的 ring 冲突
};
```

**Pull 模式**（新节点主动拉取）：

```
新 Node 2000 加入后:
1. 计算自己应该 own 的 headID 范围 (consistent hash ring)
2. 遍历所有 node，找到当前持有这些 headID 的 old owner
3. 向 old owner 发送 MigrationDataRequest
4. 收到 posting data 后写入本地 TiKV partition
5. 完成后发送 MigrationComplete → old owner 可清理冗余副本

关键: 迁移期间读写仍可用
  - 写: ring 已更新，新写入直接到 Node 2000
  - 读: 如果 Node 2000 还没迁完,
        返回 "redirect" 或由 Node 2000 从 old owner 代理读取
```

### 8.4 TiKV 数据访问

**关键设计决策**：posting data 存在 TiKV（共享存储），不在本地磁盘。

这意味着：
- **缩容不需要数据迁移**：posting data 在 TiKV 中，任何 node 可以直接读取任意 headID 的 posting（通过 TiKV key prefix）
- **扩容的"迁移"实质是 routing 变更**：新 owner 只需要知道自己 own 哪些 headID，然后直接从 TiKV 读写
- **真正需要迁移的是 in-memory cache**——如果 node 在内存中缓存了 posting data，这些 cache 需要 warm up

```
简化后的扩容:
1. Node 2000 加入 ring → routing 立即生效
2. Node 2000 收到 append for headID X → 直接写 TiKV (key = prefix + headID)
3. Node 2000 收到 search for headID X → 直接读 TiKV
4. 无需显式数据搬迁!

唯一的 warm-up:
  - Node 2000 的 posting cache 是冷的 → 前几次读走 TiKV (稍慢)
  - old owner 的 cache 有旧数据 → 自然 LRU 淘汰
```

---

## 9. 数据链路容错设计

本节纯粹从数据链路架构角度出发，画出 5 条核心链路的完整流程，逐段标注故障点和容错机制。

---

### 9.1 链路总览

对等模式共 5 条数据链路：

```
链路 ①  Insert (写入):      Client → Ingress → Owner → TiKV
链路 ②  Search (查询):      Client → Ingress → Owner(s) → TiKV → Ingress → Client
链路 ③  Head Sync (索引同步): Split/Merge Node → Gossip → All Nodes
链路 ④  Merge Lock (跨节点锁): Merge Initiator → Lock Owner → Merge Initiator
链路 ⑤  Ring Change (拓扑变更): Gossip → All Nodes → Re-route
```

---

### 9.2 链路 ① Insert（写入链路）

#### 9.2.1 核心语义

```
  SUCCESS 返回给 client ⟺ 所有 posting 已写入 TiKV（local + remote 均已 ACK）
  FAIL 返回给 client    ⟺ 有至少一条 posting 未确认（client 需 retry）
  绝无静默丢失：不会说成功实际上没写进去
```

#### 9.2.2 完整数据流图

```
 Client                Ingress (Node K)                Owner (Node J)            TiKV
   │                        │                               │                      │
   │  ① Insert(vectors)     │                               │                      │
   │───────────────────────►│                               │                      │
   │                        │                               │                      │
   │                 ② AllocateVID()                        │                      │
   │                   (本地 range, 无网络)                   │                      │
   │                        │                               │                      │
   │                 ③ RNGSelection(vec)                    │                      │
   │                   (本地 head index)                     │                      │
   │                   → headID=42 (owner=J)                │                      │
   │                   → headID=99 (owner=K)                │                      │
   │                        │                               │                      │
   │                 ④a Local head (99):                    │                      │
   │                   Append(99, posting) ─────────────────────────────────────────►│
   │                   ◄──────────────────────────────────────────────── TiKV ACK ──│
   │                        │                               │                      │
   │                 ④b Remote head (42):                   │                      │
   │                   BatchAppendRequest ─────────────────►│                      │
   │                        │                               │                      │
   │                        │                        ⑤ Append(42, posting) ────────►│
   │                        │                               │◄──── TiKV ACK ──────│
   │                        │                               │                      │
   │                        │  ⑥ BatchAppendResponse(OK) ◄──│                      │
   │                        │                               │                      │
   │                 ⑦ ALL ACKs received?                   │                      │
   │                   ├─ YES → return SUCCESS              │                      │
   │  ⑧ SUCCESS/FAIL   ├─ NO  → return FAIL               │                      │
   │◄───────────────────────│                               │                      │
   │                        │                               │                      │

  ═══ Ingress Crash: 任意时刻均可发生 ═══

  Crash 时刻           TiKV 中已写入            孤儿数据          VID 已消耗
 ──────────────────────────────────────────────────────────────────────────────
  C1  ①→② 之间         无                       无                无
      (收到请求, 还没分配 VID)

  C2  ②→③ 之间         无                       无                VID 已分配但未使用
      (VID 已分配, 还没选 head)                                   → 重启扫 TiKV 时发现
                                                                    未写入, 变成 gap
                                                                  → GC 回收进 freeList

  C3  ③→④a 之间        无                       无                VID 已分配
      (已选 head, 还没开始写)                                     同 C2

  C4  ④a 进行中         部分 local posting       已写入的变孤儿    VID 已分配
      (local append 写了一半)

  C5  ④a 完成,          全部 local posting       local 孤儿       VID 已分配
      ④b 未发出         remote 无

  C6  ④b 发送中         全部 local posting       local 孤儿       VID 已分配
      (请求在网络上)     + Owner 可能收到部分     + Owner 侧可能
                                                  有部分孤儿

  C7  ⑤ 进行中          local posting            local 孤儿       VID 已分配  
      (Owner 在写 TiKV)  + Owner TiKV 写入中      + Owner 侧
                                                   部分孤儿

  C8  ⑥ 返回途中         全部 local + remote      无!              VID 已分配
      (Owner 已成功,      都已写入 TiKV            (数据完整,
       response 在路上)                             只是 client
                                                   不知道成功了)

  C9  ⑦→⑧ 之间          全部已写入               无!              VID 已分配
      (Ingress 已收到     (同 C8)                  (同 C8, 数据
       所有 ACK, 还没                               完整, client
       回复 client)                                  不知道)
 ──────────────────────────────────────────────────────────────────────────────

  所有 Ingress crash 点, Client 统一行为:
    TCP RST / read timeout → 没收到 SUCCESS → retry 到其他 Ingress

  关键观察:
  ┌──────────────────────────────────────────────────────────────┐
  │  C1~C7: 数据不完整 → retry 写入新 VID → 旧的变孤儿 (无害)  │
  │  C8~C9: 数据已完整 → retry 写入新 VID → 旧的也完整 (重复)  │
  │                                                              │
  │  两种情况 retry 都安全:                                      │
  │    ・孤儿: 搜索可命中但 external ID 不存在 → 上层过滤        │
  │    ・重复: 同一向量两组 VID → 搜索结果去重                    │
  │    ・后台 GC 可选清理 (crash 稀有, 量极小)                   │
  │                                                              │
  │  VID 计数器: 重启时扫 TiKV 恢复 (详见 §9.2.6 问题 1)       │
  └──────────────────────────────────────────────────────────────┘


  ═══ Owner Crash: ④b 发出后, Owner 侧任意时刻均可发生 ═══

  Owner crash 只影响 remote append 路径 (④b→⑤→⑥), Ingress 仍然活着。

  Crash 时刻           TiKV 中 Owner 侧已写入    Ingress 感知         后续
 ──────────────────────────────────────────────────────────────────────────────
  O1  ④b 到达前         无                        TCP 断连 / 超时      Ingress → FAIL
      (Owner 还没收到    (Owner 侧零影响)          → return FAIL       client retry
       请求就挂了)                                  to client           → 新 Owner 处理

  O2  ④b 到达,          无                        同 O1               同 O1
      ⑤ 未开始          (收到请求但还没
      (解析请求中)        开始写 TiKV)

  O3  ⑤ 进行中          部分 posting 已写入       Ingress 等 ⑥ 超时   Ingress → FAIL
      (Owner 写 TiKV     TiKV                     → return FAIL       client retry
       写了一半)                                   to client           已写入的: 孤儿
                                                                       未写入的: 重做

  O4  ⑤ 完成,           全部 posting 已写入       Ingress 等 ⑥ 超时   Ingress → FAIL
      ⑥ 未发出           TiKV                     → return FAIL       client retry
      (TiKV ACK 了,                               to client           → 数据实际完整
       response 没发出)                                                但 Ingress 不知道
                                                                       → retry 产生重复

  O5  ⑥ 发送中           全部 posting 已写入       可能收到 / 可能     收到: SUCCESS
      (response 在        TiKV                     TCP 断连两种情况     没收到: 同 O4
       网络上)
 ──────────────────────────────────────────────────────────────────────────────

  Owner crash 对 Ingress 的统一表现:
    ④b 发出后超时 (30s) 没收到 ⑥ → Ingress 认为 remote append 失败

  Ingress 行为:
    ┌───────────────────────────────────────────────────────────────┐
    │  ④a (local) 可能已成功 + ④b (remote) 超时                    │
    │  → return FAIL to client                                     │
    │  → client retry 到同一个或其他 Ingress                       │
    │  → retry 时:                                                 │
    │    ・④a 已写的: 同 VID 幂等 skip (同 Ingress + idempotency) │
    │                 或新 VID → 旧的变孤儿 (不同 Ingress)         │
    │    ・④b 未写的 (O1~O2): 新 Owner 重新写入                    │
    │    ・④b 已写的 (O3~O5): 变孤儿或重复 (无害)                  │
    └───────────────────────────────────────────────────────────────┘

  关键区别 vs Ingress crash:
    ・Ingress crash → Client 收到 TIMEOUT (TCP 断连)
    ・Owner crash   → Client 收到明确的 FAIL (Ingress 还活着, 主动返回)
    ・两者对 client 处理方式一样: retry


  ═══ 双重 Crash: Ingress + Owner 同时挂 ═══

  极端场景, 但需要覆盖:
    ・Ingress 和 Owner 同时 crash → Client 收到 TIMEOUT → retry 到其他 Ingress
    ・等价于 Ingress crash (C4~C7), 因为 Ingress 挂了 Owner 状态已不重要
    ・恢复方式完全一致: client retry, 孤儿无害, VID 重启恢复

  每个请求独立等待, 不阻塞同一 Client 的其他并发请求
  三种结果: SUCCESS (确认成功) / FAIL (明确失败) / TIMEOUT (Ingress 可能 crash)
  后两者对 Client 处理方式一样: retry 到其他 Ingress
```

#### 9.2.3 故障点 × 容错矩阵

```
  故障点          位置              后果                     容错机制
 ─────────────────────────────────────────────────────────────────────────────
  F1  Client→     ① LB/网络断      请求未到达               Client timeout → retry
      Ingress                                              (LB 切换到其他 node)

  F2  VID分配     ② PD 不可用      无法领取新 block          本地 block 余量~100 万
                                                            PD 恢复后继续

  F3  RNG         ③ head index     选到稍旧的 head          最终一致, 不影响正确性
      Selection   过时                                      只影响选头质量

  F4  Local       ④a TiKV 写失败   posting 未持久化          → return FAIL to client
      Append                                                client retry

  F5  Remote      ④b TCP 断连      append 未送达 Owner      → 超时 → return FAIL
      发送失败    或 Owner 不可达                              client retry

  F6  Owner       ⑤ TiKV 写失败    Owner 端 Append 失败     Owner 返回 FAIL response
      写入失败                                              → Ingress return FAIL
                                                            client retry

  F7  Owner       ⑤ 进程 crash     部分 item 写入 TiKV      → Ingress 超时 → FAIL
      Crash       部分完成         部分 item 丢失            client retry
                                                            已写入的: 幂等 skip
                                                            未写入的: 重新写入

  F8  Response    ⑥ TCP 断连       Ingress 不知道结果       → 超时 → return FAIL
      丢失        (Owner 其实成功了)                          client retry
                                                            retry 时 Owner 幂等 skip
                                                            (VID+version check)

  F9  Ingress     ④a~⑧ 之间     部分 posting 可能              → Client TCP 超时 → retry
      Crash       任意时刻       已写入 TiKV                    到其他 Ingress (新 VID)
                                client 无 response              已写入的: 变孤儿 (无害)
                                VID 计数器丢失                    未写入的: 新 Ingress 重做
                                                                VID 恢复: 重启时扫 TiKV
                                                                详见 §9.2.6

  F10 Ring        ④b owner 已变   发给了旧 owner            旧 owner 仍可写 TiKV
      变更中                       (TiKV 是共享存储)         → 写入成功, 不影响正确性
                                                            → 仅 steady state 后
                                                              新 owner 接管后续写入

  F11 Partial     ④a OK           local 成了,               → return FAIL
      失败        ④b FAIL          remote 失败               client retry 整个 batch
                                                            ④a 已写的: 幂等 skip
                                                            ④b 未写的: 重新写入
```

#### 9.2.4 端到端保证

```
  ┌───────────────────────────────────────────────────────────────┐
  │                    写入语义契约 (三态)                          │
  │                                                               │
  │  Client                  Ingress                 TiKV         │
  │    │                       │                       │          │
  │    │───Insert(batch)──────►│                       │          │
  │    │                       │──local append────────►│          │
  │    │                       │──remote append via────►│ (Owner) │
  │    │                       │◄─────── ALL ACK ──────│          │
  │    │◄──── SUCCESS ─────────│                       │          │
  │    │                       │                       │          │
  │    │      ═══ OR ═══       │                       │          │
  │    │                       │                       │          │
  │    │                       │──remote append────────►│          │
  │    │                       │         ✗ timeout/fail │          │
  │    │◄──── FAIL ────────────│                       │          │
  │    │                       │                       │          │
  │    │      ═══ OR ═══       │                       │          │
  │    │                       │                       │          │
  │    │                       │──local append────────►│          │
  │    │                       ████ CRASH ████         │          │
  │    │         (TCP RST / read timeout)              │          │
  │    │◄──── TIMEOUT ─────────×                       │          │
  │                                                               │
  │  三态语义:                                                     │
  │    SUCCESS → 数据 100% 在 TiKV (Raft 3 副本)                  │
  │              client 可以确认写入成功                            │
  │                                                               │
  │    FAIL    → Ingress 明确告知失败                              │
  │              数据可能部分写入或完全没写入                       │
  │              client 知道了, 可以 retry                         │
  │              retry 时幂等机制防止重复                           │
  │                                                               │
  │    TIMEOUT → Ingress 可能 crash, 无明确 response               │
  │              数据可能部分写入 (孤儿 posting, 无害)              │
  │              client 看到 TCP 断连/超时                         │
  │              处理方式: 与 FAIL 一致, retry 到其他 Ingress      │
  │              已写入的变孤儿 (后台 GC 可选清理)                  │
  │              新 Ingress 分配新 VID 重新写入                     │
  │                                                               │
  │  Client 决策:                                                  │
  │    if response == SUCCESS  → 确认成功, 不用管                  │
  │    if response == FAIL     → 明确失败, retry                   │
  │    if response == 超时/断连 → 也是失败, retry                  │
  │    → 后两者处理方式完全一样                                     │
  │                                                               │
  │  绝不出现: client 收到 SUCCESS 但数据没写进去                  │
  │  简单、诚实、无后台魔法                                        │
  └───────────────────────────────────────────────────────────────┘
```

#### 9.2.5 幂等性保证（Client Retry 安全性）

```
Client retry 后同一批向量会重新 insert:
  - 新 VID? 还是同 VID?

分情况:

  ┌───────────────────────────────────────────────────────────────────────┐
  │                                                                       │
  │  情况 1: retry 到同一 Ingress (没 crash), 有 idempotency_key         │
  │    → key 命中缓存                                                    │
  │    → 只缓存 SUCCESS (不缓存 FAIL!)                                   │
  │    → 缓存命中: 返回 SUCCESS, 同 VID, 不重新执行                      │
  │    → 缓存未命中 (上次是 FAIL): 重新执行, 同 Ingress 分配新 VID       │
  │       → 上次部分写入的变孤儿 (无害)                                   │
  │                                                                       │
  │  情况 2: retry 到同一 Ingress (没 crash), 无 idempotency_key         │
  │    → 每次都重新执行, 分配新 VID                                       │
  │    → 上次部分写入的变孤儿 (无害)                                      │
  │    → 如果上次其实全部成功了 (F8: response 丢失)                       │
  │       → 同向量两组 VID → 重复, 搜索去重即可                           │
  │                                                                       │
  │  情况 3: retry 到不同 Ingress                                         │
  │    → 不同 Ingress 没有缓存 → 重新执行                                │
  │    → 从不同 node 的 VID range 分配 → 新 VID                          │
  │    → 上次部分写入的变孤儿 (无害)                                      │
  │                                                                       │
  │  情况 4: retry 到同一 Ingress, 但它刚 crash 重启过                    │
  │    → 缓存丢失 → 等同情况 2                                           │
  │    → VID 计数器已通过 RecoverVIDCounter() 恢复                       │
  │    → 分配新 VID (从 max_used+1 继续)                                  │
  └───────────────────────────────────────────────────────────────────────┘

idempotency_key 缓存规则:

  ┌─────────────────────────────────────────────┐
  │  Client Request:                             │
  │    { idempotency_key: "abc123",              │
  │      vectors: [...] }                        │
  │                                              │
  │  Ingress:                                    │
  │    if key in cache AND cached == SUCCESS:    │
  │      return SUCCESS (不重新执行)              │
  │    else:                                     │
  │      process normally                        │
  │      if result == SUCCESS:                   │
  │        cache.put(key, SUCCESS)  // 只缓存成功 │
  │      return result                           │
  │                                              │
  │  为什么不缓存 FAIL:                           │
  │    缓存 FAIL → retry 命中 → 返回 FAIL        │
  │    → client 再 retry → 又 FAIL → 死循环!     │
  │    FAIL 说明这次没写成功, retry 应该重新执行   │
  └─────────────────────────────────────────────┘

如果 client 没有发 idempotency_key (向后兼容):
  每次 retry 分配新 VID → Append 层 VID 不重复 → 不会冲突
  代价: posting list 中可能有多条 VID 对应同一个向量
        → 搜索结果中出现重复 → 上层去重即可
        → 不影响正确性, 只浪费少量存储

Append 层的天然幂等 (无论是否有 idempotency_key):
  Posting list entry = (VID, version, vector)
  同一个 (VID, version) 写两次 → 第二次检查 version → skip
  这保护了 "Ingress 成功但 response 丢失 → client retry → 同 Ingress 处理" 场景
```

#### 9.2.6 Ingress Crash 深入分析

Ingress（执行节点）在处理 Insert 过程中任意时刻 crash，需要解决 3 个问题：

##### 问题 1: VID 计数器丢失 → 重启后 VID 冲突

```
  崩溃前: m_next = m_rangeStart + 37,000  (已分配 37,000 个 VID)
  崩溃后: InitBlock() → m_next = m_rangeStart  (重置到起点!)
  后果:   重新分配 VID 0 ~ 36,999 → 和 TiKV 中已有的 posting 的 VID 冲突

  解决: 重启时扫描 TiKV 恢复 m_next (一次性, 仅在启动时)

  void RecoverVIDCounter(int nodeIndex) {
      SizeType rangeStart = nodeIndex * BLOCK_SIZE;
      SizeType rangeEnd   = rangeStart + BLOCK_SIZE;

      // TiKV scan: 找到本节点 VID 范围内的最大已用 VID
      // 扫描 versionMap (key = VID, value = version)
      // 这是一个 range scan, 不是全表扫描
      SizeType maxUsed = TiKVRangeScan("__vid__", rangeStart, rangeEnd).maxKey();

      m_next.store(maxUsed + 1);  // 从最大已用 VID 之后继续分配
      // gap 中可能有未使用的 VID → 留给 GC 扫描后放入 freeList
  }

  启动流程:
    1. InitBlock(nodeIndex)              // 计算 range 边界
    2. RecoverVIDCounter(nodeIndex)       // 扫 TiKV 恢复 m_next
    3. RebuildFreeList(nodeIndex)         // 可选: 扫描 deleted VID 重建 freeList
    4. StartServing()                     // 开始接受请求

  代价:
    - 一次 TiKV range scan, 范围 = 524,288 个 key
    - 正常节点重启 ~1-2 秒, 可接受
    - 启动期间不接受写请求, 不会产生不一致
```

##### 问题 2: 孤儿 Posting（Crash 导致的部分写入）

```
  场景:
    t=0  Client → Ingress: Insert(vec_A, vec_B)
    t=1  Ingress: AllocateVID → VID=100(vec_A), VID=101(vec_B)
    t=2  RNGSelection → headID=42(owner=J), headID=99(owner=K,local)
    t=3  ④a: Append(99, VID=100) → TiKV ✓  (成功写入)
    t=4  ████ Ingress CRASH ████  (④b 还没发出去)

  结果:
    - TiKV 中: headID=99 的 posting list 包含 VID=100 ← 孤儿!
    - headID=42 没有收到 VID=101 ← 丢失但无害
    - Client: TCP 超时, retry 到其他 node
    - 新 Ingress: 分配 VID=500(vec_A), VID=501(vec_B), 全部重新写入
    - 最终: headID=99 中有 VID=100(孤儿) 和 VID=500(有效)
            → VID=100 指向的向量和 VID=500 完全相同

  孤儿的影响:
    - 搜索时可能命中孤儿 VID → 返回正确的向量数据 (因为数据是对的)
    - 但它在 client 的 external ID mapping 中不存在
      → client 认为是 "unknown" → 上层过滤即可
    - 浪费少量 TiKV 存储空间

  孤儿清理 (后台 GC, 非关键路径):
    // 定期扫描: posting list 中的 VID 是否仍然 "活跃"
    // "活跃" = versionMap 中存在且未被 delete
    for each headID in my_owned_headIDs:
      posting_list = TiKV.Get(headID)
      for each entry in posting_list:
        if !versionMap.exists(entry.VID):
          // 孤儿: 没有 client 知道这个 VID
          posting_list.remove(entry)
      TiKV.Put(headID, posting_list)

    频率: 每小时或每天, crash 是稀有事件, 孤儿量极小
    也可以不清理: 孤儿不影响正确性, 只浪费少量空间
```

##### 问题 3: Client 收到的不是 FAIL，是 TCP 超时

```
  区分:
    正常 FAIL: Ingress 活着, 但写入失败 → 返回明确的 FAIL response
    Crash:     Ingress 进程死了 → TCP 连接断开 → Client 看到的是:
               - TCP RST (如果 OS 还在) → 表现为 "connection reset"
               - TCP 超时 (如果整机挂了) → 表现为 "read timeout"

  Client 行为:
    对 client 来说, "TCP 断开/超时" 和 "FAIL response" 的处理方式完全一样:
    → retry (发到其他 Ingress node)

    但 client 确实知道 "这次 insert 没成功":
    ┌─────────────────────────────────────────────────────────┐
    │  if response == SUCCESS → 确认成功, 不用管               │
    │  if response == FAIL   → 明确失败, retry                │
    │  if response == 超时/断连 → 也是失败, retry              │
    │                                                         │
    │  三种情况 client 都有明确的预期:                          │
    │    SUCCESS = 一定写进去了                                 │
    │    非 SUCCESS (包括超时) = 可能没写进去, retry            │
    │    绝不会出现: client 以为成功了但实际上没有               │
    └─────────────────────────────────────────────────────────┘

  更新 9.2.4 的写入语义为三态:
    SUCCESS → 数据 100% 在 TiKV
    FAIL    → Ingress 明确告诉你失败了
    TIMEOUT → Ingress 可能 crash 了, 数据可能部分写入
    → 后两者对 client 的处理方式一样: retry
    → retry 安全性由幂等保证 (同 Ingress = idempotency_key cache;
                              不同 Ingress = 新 VID, 旧数据变孤儿, 无害)
```

##### 总结: Crash ≠ 灾难

```
  ┌───────────────────────────────────────────────────────────────┐
  │  Ingress Crash 的完整恢复故事                                  │
  │                                                               │
  │  Client 侧:                                                   │
  │    感知: TCP 超时/断连 (不是静默丢失!)                         │
  │    动作: retry 到其他 Ingress                                  │
  │    结果: 新 VID, 重新写入, 最终成功                             │
  │                                                               │
  │  崩溃节点侧 (重启后):                                          │
  │    VID 恢复: 扫 TiKV 拿到 m_next → 不会分配已用 VID           │
  │    FreeList:  空的 → 下次 GC 扫描后重建                        │
  │    幂等缓存: 空的 → 不影响正确性 (只可能多一次重复写入)         │
  │                                                               │
  │  数据侧:                                                      │
  │    孤儿 posting: 存在但无害                                    │
  │    → 搜索可命中 (数据正确, 上层过滤 unknown VID)               │
  │    → 后台 GC 定期清理 (可选)                                   │
  │    → crash 是稀有事件, 孤儿量 ≈ batch_size × crash次数 ≈ 极小  │
  │                                                               │
  │  代价:                                                        │
  │    1. Client 多一次 retry (~30s timeout + retry 时间)          │
  │    2. TiKV 中几百条孤儿 posting (不影响正确性)                 │
  │    3. 重启时 1-2 秒 VID 恢复扫描                               │
  │                                                               │
  │  保证:                                                        │
  │    ✓ 不会静默丢数据 — client 知道没成功                        │
  │    ✓ 不会 VID 冲突 — 重启时恢复计数器                          │
  │    ✓ 不会阻塞系统 — 其他 node 正常服务, client retry 即可      │
  └───────────────────────────────────────────────────────────────┘
```

#### 9.2.7 Batch 内 Partial Failure 处理

```
一个 batch (10 个向量) 的 RNGSelection 可能散到 3 个 owner:

  local(K):  headIDs [99, 200, 301]      → ④a 全部 OK
  Node J:    headIDs [42, 55]             → ④b OK
  Node M:    headIDs [1001, 1002, 1003]   → ④b FAIL (Node M 超时)

处理策略:
  方案 A: 全部失败 → return FAIL (简单, 事务性)
    ┌─ 优: client 逻辑简单, 要么全成功要么全重试
    └─ 劣: 已成功的 8 条白做了 (虽然幂等不会出错)

  方案 B: 部分成功 → return PARTIAL_FAIL + 失败列表
    ┌─ 优: client 可以只 retry 失败的 vector
    └─ 劣: client 需要更复杂的 retry 逻辑

  选择: 方案 A (全部失败)
    理由:
    1. 简单 — client 不需要追踪哪些成功了哪些没有
    2. 幂等 — 全部 retry 不会重复写入
    3. 故障是稀有路径 — 不值得为之复杂化 API
    4. batch 通常只涉及 2-3 个 owner — 单个失败概率低

  实现:
    for each owner in remote_owners:
      resp = SendBatchAppend(owner, items, timeout=30s)
      if resp != SUCCESS:
        return FAIL to client   // 快速失败, 不等其他 owner
```

#### 9.2.8 Insert 后半段: Split / Merge / Reassign

Append 只是 Insert 的前半段。当 posting list 超过阈值时触发 Split，过短时触发 Merge。
这些操作改变 head 拓扑，涉及跨节点协调，是 Insert 路径中最复杂的部分。

##### 9.2.8.1 触发条件

```
  Append(headID, vectors) → 写入 TiKV → 返回新 postingSize

  postingSize 决定后续动作:

  ┌─────────────────────────────────────────────────────────────────────┐
  │                                                                     │
  │  postingSize ≤ postingSizeLimit                                     │
  │    → 正常, 无后续操作                                               │
  │                                                                     │
  │  postingSizeLimit < postingSize ≤ postingSizeLimit + bufferLimit    │
  │    → SplitAsync(headID) — 异步提交到 splitThreadPool               │
  │    → Append 立即返回, 不阻塞 client                                │
  │                                                                     │
  │  postingSize > postingSizeLimit + bufferLimit                       │
  │    → Split(headID) — 同步执行! 阻塞当前 Append 直到 split 完成     │
  │    → 完成后 retry Append (posting 已拆分, 新的 postingSize 正常)    │
  │                                                                     │
  │  postingSize ≤ mergeThreshold (通常在 GC/RefineIndex 后)           │
  │    → MergeAsync(headID) — 异步提交到 splitThreadPool               │
  │                                                                     │
  └─────────────────────────────────────────────────────────────────────┘

  关键: 正常情况 split 是异步的, 不影响 Append 的延迟
        只有 posting 爆满 (overflow) 才同步 split
```

##### 9.2.8.2 Split 完整流程

```
  Owner Node (headID 的 owner 在本地执行 split)

  ┌─ splitThreadPool worker (或同步调用) ─────────────────────────────┐
  │                                                                    │
  │  ① Lock: unique_lock(m_rwLocks[headID])                           │
  │     → 该 headID 的 Append/Search 暂时阻塞                         │
  │                                                                    │
  │  ② Read: db->Get(headID) → 读取完整 posting list                  │
  │                                                                    │
  │  ③ GC: 过滤掉 deleted/stale 的向量 (version 不匹配)               │
  │     → if GC 后 postingSize ≤ limit → 只写回压缩后的 posting → 返回│
  │                                                                    │
  │  ④ K-means(k=2): 把 posting 向量分成两个 cluster                  │
  │     → cluster_A (包含原 head 向量)                                 │
  │     → cluster_B (中心点离原 head 较远的那组)                       │
  │                                                                    │
  │  ⑤ 写 cluster_A:                                                  │
  │     db->Put(headID, cluster_A_posting)                             │
  │     → 复用原 headID, head index 不变                               │
  │                                                                    │
  │  ⑥ 写 cluster_B (分两种情况):                                      │
  │     ┌─ 6a: cluster_B 中心 VID 已经是 head → 合并到那个 head       │
  │     │   db->Get(existingHead) → merge → db->Put(existingHead)     │
  │     │   if merged > limit → SplitAsync(existingHead) 递归!         │
  │     │                                                              │
  │     └─ 6b: cluster_B 中心 VID 不是 head → 创建新 head             │
  │         newHeadID = cluster_B 中心 VID                             │
  │         db->Put(newHeadID, cluster_B_posting)    ← TiKV 写入      │
  │         headIndex.Add(newHeadID, vector)          ← 本地 head 更新 │
  │                                                                    │
  │  ⑦ HeadSync 广播:                                                 │
  │     BroadcastHeadSync([                                            │
  │       {Op::Add, newHeadID, vector},  // 新 head (6b 的情况)       │
  │     ])                                                             │
  │     → gossip piggyback, 最终一致                                   │
  │                                                                    │
  │  ⑧ Unlock: 释放 m_rwLocks[headID]                                │
  │                                                                    │
  │  ⑨ (可选) Reassign: 检查分出的向量是否有更近的 head               │
  │     → 把"分错了"的向量 ReassignAsync 到更合适的 head               │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘

  TiKV 写入 (非事务, 逐步执行):
    1. db->Put(headID, cluster_A)     — 覆写原 posting
    2. db->Put(newHeadID, cluster_B)  — 新 posting
   (3. db->Delete(oldHead)            — 仅当原 head 被废弃时)

  注意: 这些 TiKV 写不是原子事务!
        crash 可能导致部分完成 → 见下面容错分析
```

##### 9.2.8.3 Merge 完整流程

```
  Owner Node (headID 的 owner 在本地执行 merge)

  ┌─ splitThreadPool worker ──────────────────────────────────────────┐
  │                                                                    │
  │  ① Lock: unique_lock(m_rwLocks[headID])                           │
  │                                                                    │
  │  ② Read + GC: db->Get(headID) → 过滤 → if 恢复到 > threshold → 返回│
  │                                                                    │
  │  ③ 找 merge 对象: headIndex.Search(headVector)                    │
  │     → 找到最近邻 partner headID                                    │
  │                                                                    │
  │  ④ Lock partner:                                                   │
  │     ┌─ partner 在本地 → try_lock(m_rwLocks[partner])              │
  │     │   失败 → 重新入队, 稍后重试                                  │
  │     │                                                              │
  │     └─ partner 在远程 Node J → SendRemoteLock(J, partner, true)   │
  │         → RPC 请求跨节点锁 (30s lease)                             │
  │         → 被拒 → 重新入队, 稍后重试                                │
  │                                                                    │
  │  ⑤ Read partner: db->Get(partner) → 读取 partner posting          │
  │                                                                    │
  │  ⑥ 去重 + 大小检查:                                               │
  │     if (headID.size + partner.size ≥ postingSizeLimit)             │
  │       → partner 太大, 合不进去 → 跳过, 试下一个 neighbor           │
  │                                                                    │
  │  ⑦ 合并: 较大的 posting 吸收较小的                                 │
  │     winner = larger posting's headID                                │
  │     loser  = smaller posting's headID                               │
  │     db->Put(winner, merged_posting)                                │
  │     db->Delete(loser)                                              │
  │     headIndex.Delete(loser)                                        │
  │                                                                    │
  │  ⑧ HeadSync 广播:                                                 │
  │     BroadcastHeadSync([{Op::Delete, loser}])                       │
  │                                                                    │
  │  ⑨ Unlock partner:                                                │
  │     本地 → unlock(m_rwLocks[partner])                              │
  │     远程 → SendRemoteLock(J, partner, false)                       │
  │                                                                    │
  │  ⑩ (可选) Reassign 被 merge 的向量到更近的 head                    │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘

  Merge 的跨节点锁场景:
    headID=42 在 Node K, partner headID=99 在 Node J
    Node K 需要锁住 Node J 上的 headID=99 → RemoteLockReq
    → 这就是 §9.5 Merge Lock 链路所描述的
```

##### 9.2.8.4 Reassign 完整流程

```
  Split/Merge 后, 部分向量可能不再"属于"最近的 head
  Reassign 把它们移到更合适的 head

  ┌─ splitThreadPool worker ──────────────────────────────────────────┐
  │                                                                    │
  │  ① 对每个需要 reassign 的 VID:                                    │
  │     RNGSelection(vector) → 找到当前最佳 headID 列表               │
  │                                                                    │
  │  ② 如果新 headID 和旧的不同:                                      │
  │     version++ (bump version, 旧 posting entry 变 stale)           │
  │     Append(newHeadID, {VID, newVersion, vector})                   │
  │                                                                    │
  │  ③ Append 走正常路径:                                              │
  │     newHeadID 的 owner 在本地 → 直接写 TiKV                       │
  │     newHeadID 的 owner 在远程 → QueueRemoteAppend → FlushRemote   │
  │                                                                    │
  │  ④ 旧 posting 中的 stale entry:                                   │
  │     不主动删除! 惰性清理 — 下次 GC/Split 读 posting 时过滤掉      │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘

  Reassign 本质上就是 "delete from old head + insert to new head"
  但通过 version 机制实现了无锁 "逻辑删除":
    旧 entry version 不匹配 → GC 时自动清理
    新 entry version 正确 → 搜索返回新的
```

##### 9.2.8.5 Split/Merge Crash 容错

```
  Split 和 Merge 的 TiKV 写不是原子事务, crash 可能导致中间状态。
  逐一分析:

  ═══ Split Crash ═══

  Crash 时刻         已完成的 TiKV 写        后果              恢复
 ──────────────────────────────────────────────────────────────────────
  S1  ②之后④之前      无                     posting 未变       无影响
      (读完还没写)                            splitList 丢失    → 下次 Append 重新触发

  S2  ⑤之后⑥之前      headID 写了 cluster_A  cluster_B 的向量   向量暂时"消失"
      (原 posting 已     但 cluster_B 没写      不在任何 posting   → 但 versionMap 中仍存在
       覆写为 cluster_A)                        中! (被截断了)    → RefineIndex/GC 扫描时
                                                                   发现 version 有效但不在
                                                                   任何 posting → Reassign
                                                                 → 最终恢复 (分钟级)

  S3  ⑥之后⑦之前      两个 posting 都写了     数据完整!          head index 本地已更新
      (TiKV 写完,       headIndex 本地已更新    但其他节点        → 但 HeadSync 没广播
       HeadSync 没发)                           不知道新 head    → gossip 最终会传播
                                                                 → 或 peer 做 catch-up

  S4  步骤 6a 递归     cluster_B 合并到        可能触发了另一个   同 S2 分析
      split 中 crash    已有 head, 但递归       split 的 S1~S3
                        split 写到一半

  ═══ Merge Crash ═══

  Crash 时刻         已完成的 TiKV 写        后果              恢复
 ──────────────────────────────────────────────────────────────────────
  M1  ④之后⑤之前      无 TiKV 写             无影响            远程锁 lease 30s 自动过期
      (拿到锁, 还没写)                         partner 暂时锁住  → 不会死锁

  M2  ⑦ Put 完成      winner 有合并数据       两个 head 都存在   搜索可能返回重复
      Delete 未完成   loser 没删              → loser 数据 和     → 上层去重
                                               winner 中有重叠   → 下次 GC 会发现 loser
                                                                   posting 中向量 version
                                                                   已 stale → 清理

  M3  headIndex       loser 的 TiKV posting   headIndex 没有     posting key 孤立在 TiKV
      Delete 完成     没删                    loser 了, 但       → 不会被搜索命中
      db->Delete 没   headIndex 已删          TiKV 还有它        → 空间浪费, 无正确性问题
                                                                 → 后台 key GC 可清理

  M4  ⑧ HeadSync      本地操作全部完成        其他节点不知道      gossip 最终传播
      没广播                                   loser 被删了       → 或 peer catch-up
                                               → 可能路由到       → Append 检查到 head
                                                  已删 head         已删 → Reassign

  M5  ⑨ Unlock        远程锁没释放            lease 30s 过期     §9.5 的 lease 机制
      crash                                    → 不会死锁

  ═══ 恢复机制总结 ═══

  ┌────────────────────────────────────────────────────────────────────┐
  │                                                                    │
  │  1. splitList / mergeList 是内存结构, crash 后丢失                 │
  │     → Append 时 postingSize 检查会重新触发 split                   │
  │     → RefineIndex 定期扫描会重新触发 merge                         │
  │     → 不需要持久化这些 list                                        │
  │                                                                    │
  │  2. TiKV 写不需要事务:                                            │
  │     → Split: 最坏情况 (S2) 是部分向量暂时"消失"                   │
  │       → versionMap 中仍有记录 → RefineIndex/Reassign 修复         │
  │     → Merge: 最坏情况 (M2) 是重复数据                             │
  │       → GC 清理 stale version → 天然修复                          │
  │                                                                    │
  │  3. Lease 保证无死锁:                                              │
  │     → 跨节点 merge lock 30s 自动过期                               │
  │     → crash 后无需手动清理                                         │
  │                                                                    │
  │  4. HeadSync 丢失不影响正确性:                                     │
  │     → 只影响 head 选择质量                                         │
  │     → Append 到已删 head → 检测到 deleted → 自动 Reassign         │
  │     → catch-up 机制最终恢复 head index 一致性                      │
  │                                                                    │
  │  核心不变量: TiKV 中 posting data 是 ground truth                  │
  │    head index 是加速结构, 可从 TiKV 完全重建                       │
  │    versionMap 是权威版本记录, split/merge 都据此判断 stale         │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘
```

##### 9.2.8.6 Split/Merge 与 Client 的关系

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                                                                    │
  │  Client 完全不感知 Split/Merge!                                    │
  │                                                                    │
  │  Client 的 Insert 请求:                                            │
  │    ① → ⑧ 全部完成 → return SUCCESS                                │
  │    这里的 "完成" = Append 写入 TiKV + 所有 Owner ACK              │
  │    Split/Merge 是 Append 之后的异步操作, 不在 client 等待路径上    │
  │                                                                    │
  │  唯一例外: posting 爆满 (overflow), 同步 Split 阻塞 Append         │
  │    → Append 等 Split 完成 → 重新 Append → 然后才返回 Client       │
  │    → 对 Client 表现为: 这一次 Insert 延迟较高 (多了 split 时间)   │
  │    → 但仍然是 SUCCESS/FAIL 语义, 不需要 client 特殊处理           │
  │                                                                    │
  │  Split/Merge 失败不影响 Client 已收到的 SUCCESS:                   │
  │    Client 收到 SUCCESS = Append 已经写入 TiKV                     │
  │    Split 失败 = posting list 暂时过长 → 搜索变慢, 但数据不丢      │
  │    Merge 失败 = posting list 暂时过短 → 搜索 recall 略低, 但可用  │
  │    → 都是性能问题, 不是正确性问题                                  │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘
```

---

### 9.3 链路 ② Search（查询链路）

#### 9.3.1 完整数据流图

```
 Client               Ingress (Node K)           Owner A (Node J)     Owner B (Node M)
   │                       │                          │                     │
   │  ① Search(query, K)   │                          │                     │
   │──────────────────────►│                          │                     │
   │                       │                          │                     │
   │                ② Head Search (local BKT)         │                     │
   │                  → candidates:                   │                     │
   │                    headID=42 (owner=J)           │                     │
   │                    headID=55 (owner=J)           │                     │
   │                    headID=99 (owner=K, local)    │                     │
   │                    headID=1001 (owner=M)         │                     │
   │                       │                          │                     │
   │                ③ Group by owner:                 │                     │
   │                  local: [99]                     │                     │
   │                  Node J: [42, 55]                │                     │
   │                  Node M: [1001]                  │                     │
   │                       │                          │                     │
   │                ④a SearchPosting(99)              │                     │
   │                  (本地 TiKV 读取)                 │                     │
   │                       │                          │                     │
   │                ④b ────SearchPostingReq([42,55])──►│                     │
   │                       │                          │                     │
   │                ④c ────SearchPostingReq([1001])───────────────────────►│
   │                       │                          │                     │
   │                       │  ⑤a SearchPostingResp    │                     │
   │                       │◄─────(results for 42,55)─│                     │
   │                       │                          │                     │
   │                       │  ⑤b SearchPostingResp    │                     │
   │                       │◄────────(results for 1001)──────────────────│
   │                       │                          │                     │
   │                ⑥ Merge all results               │                     │
   │                  → global top-K                  │                     │
   │                       │                          │                     │
   │  ⑦ Return top-K       │                          │                     │
   │◄──────────────────────│                          │                     │
```

#### 9.3.2 故障点 × 容错矩阵

```
  故障点           位置               后果                  容错机制
 ────────────────────────────────────────────────────────────────────────────
  F1  Client→      ① 网络断         请求未到               Client 重试到其他 node
      Ingress

  F2  Head Search  ② head index     candidate 不完整       head index 最终一致;
                   过时                                    只影响 recall ~0.1%

  F3  Local        ④a TiKV 读失败   缺少 local results     标记为降级结果;
      Search                                              仍返回 remote 部分给 client

  F4  Remote       ④b/④c TCP 断连   某个 owner 不可达      超时机制 (见下):
      Search       或 owner crash                         5s timeout → local fallback
      不可达                                              → 降级但可用

  F5  Remote       ⑤a/⑤b 超时      等太久拖慢整体查询       带超时收集:
      Search                                              for each future:
      慢响应                                                wait_for(5s)
                                                            timeout → local fallback

  F6  Partial      ④b OK,          部分 owner 返回         Merge 所有已收到的结果;
      Failure      ④c 失败         部分 owner 超时          缺失部分用 local fallback 补
                                                           标记 response.degraded=true

  F7  Ingress      ⑥ 之中          merge 未完成            Client 超时 → 重试到其他 node
      Crash                                                (search 是纯只读, 无副作用)
```

#### 9.3.3 降级策略示意

```
4 个 candidate headIDs 分配到 3 个 owner:

  local(K): [99]          → ✅ 成功
  Node J:   [42, 55]      → ✅ 成功
  Node M:   [1001]        → ❌ 超时!

降级流程:
  1. 已有: results(99) + results(42) + results(55)
  2. 缺失: results(1001)
  3. Local fallback: Ingress 自己去 TiKV 读 headID=1001 的 posting
     (任何 node 都可以读 TiKV 中的任何 headID, 因为 TiKV 是共享存储)
  4. 如果 TiKV 读也失败 → 用已有的 3 个 head 的结果返回
     (recall 略降, 但查询不阻塞)

  ┌───────────────────────────────────────┐
  │         Degradation Ladder            │
  │                                       │
  │  Level 0: 所有 owner 正常返回          │ → 最佳 recall
  │  Level 1: 部分 owner 超时,            │
  │           fallback 到 local TiKV 读    │ → recall 略降, 延迟略增
  │  Level 2: owner 超时 + TiKV 读也失败, │
  │           跳过该 headID               │ → recall 再降, 但返回可用
  │  Level 3: 所有 remote 失败,           │
  │           只用 local heads             │ → 单节点 recall, 仍可用
  │                                       │
  │  每一级都标记 response.degraded=true   │
  │  让 client 知道结果质量               │
  └───────────────────────────────────────┘
```

#### 9.3.4 Hedged Request（长尾优化）

```
对延迟敏感场景，同时发给 primary + secondary owner:

  primary   = hashRing.GetOwner(headID)        // 一致性哈希主 owner
  secondary = hashRing.GetNextOwner(headID)     // ring 上顺时针下一个物理 node

  ┌────────┐      SearchReq       ┌─────────┐
  │Ingress │─────────────────────►│ Primary │
  │        │                      └─────────┘
  │        │      SearchReq       ┌───────────┐
  │        │─────────────────────►│ Secondary │
  │        │                      └───────────┘
  │        │
  │        │◄── 先到的 Response ── 使用
  │        │    后到的 Response ── 丢弃
  └────────┘

  启用条件: 仅当 fan-out ≤ 3 个 remote node (避免放大流量)
  效果:     p99 延迟降低 ~30-50% (消除单 node 慢响应)
  代价:     流量×2 (仅在低 fan-out 时可接受)
```

---

### 9.4 链路 ③ Head Sync（索引同步链路）

#### 9.4.1 完整数据流图

```
 Split/Merge Node           TiKV               Gossip Network         Other Nodes
      (Node J)                │                      │                     │
        │                     │                      │                     │
 ① Split posting →            │                      │                     │
   new headID=500 born        │                      │                     │
        │                     │                      │                     │
 ② epoch = globalEpoch++      │                      │                     │
   (从 PD 原子递增)            │                      │                     │
        │                     │                      │                     │
 ③ Write HeadSyncLog ────────►│                      │                     │
   key: __hs__/{epoch}        │                      │                     │
   val: {Add, 500, vec}       │                      │                     │
        │◄──── ACK ───────────│                      │                     │
        │                     │                      │                     │
 ④ Apply locally:             │                      │                     │
   headIndex.Add(500, vec)    │                      │                     │
        │                     │                      │                     │
 ⑤ Piggyback on next gossip:  │                      │                     │
   MembershipPing {           │                      │                     │
     ...,                     │                      │                     │
     headsync: [{epoch,       │                      │                     │
       Add, 500, vec}]        │                      │                     │
   }  ─────────────────────────────────────────────►│                     │
        │                     │                      │                     │
        │                     │               ⑥ Gossip peer (Node A)       │
        │                     │                 Apply HeadSyncEntry         │
        │                     │                 headIndex.Add(500, vec)     │
        │                     │                      │                     │
        │                     │               ⑦ Node A's next gossip ─────►│
        │                     │                      │              Node B: Apply
        │                     │                      │              Node B gossip──►...
        │                     │                      │                     │
```

```
传播速度: O(log₂ N) 轮 gossip
  N=2000 → ~11 轮 × 1s/轮 = ~11s 全部传遍

传播保证: 最终一致 (eventual consistency)
  但如果 gossip 消息丢失... → 见 gap 检测
```

#### 9.4.2 故障点 × 容错矩阵

```
  故障点          位置                后果                  容错机制
 ────────────────────────────────────────────────────────────────────────
  F1  Epoch      ② PD 不可用        无法分配 epoch          退避重试; PD 恢复后继续
      分配                                                 (split 操作阻塞, 不丢数据)

  F2  Log        ③ TiKV 写失败      log 未持久化            split 操作 rollback
      写入                                                 (不 split, 下次重试)

  F3  Gossip     ⑤ 消息丢失         某些 node 没收到        Gap 检测机制:
      丢失       (网络/节点忙)       head sync entry         (见下)

  F4  Gossip     ⑤-⑦ 传播慢        部分 node 过时 >11s     可接受: head index
      延迟                                                 短暂过时只影响选头质量

  F5  新节点     加入时             head index 为空          全量 catch-up:
      加入                                                 从 TiKV scan 所有 epoch
                                                           或从 peer 拉 snapshot
```

#### 9.4.3 Gap 检测 + Catch-Up

```
每个 node 维护: m_appliedEpoch (已应用到的最新 epoch)

收到 gossip 中的 HeadSyncEntry:

  ┌─ entry.epoch == m_appliedEpoch + 1 ─────────────────┐
  │  顺序到达 → 直接 Apply → m_appliedEpoch++            │
  │  → 检查 out-of-order buffer 中是否有后续 entry       │
  └──────────────────────────────────────────────────────┘

  ┌─ entry.epoch > m_appliedEpoch + 1 ──────────────────┐
  │  检测到 Gap! 中间缺了 entry                          │
  │  → 缓存到 out-of-order buffer                       │
  │  → if gap > 100: 触发 catch-up                      │
  └──────────────────────────────────────────────────────┘

  ┌─ entry.epoch <= m_appliedEpoch ─────────────────────┐
  │  已经应用过 → skip                                   │
  └──────────────────────────────────────────────────────┘

  Catch-Up 流程:
    1. 从 TiKV scan: key range [__hs__/{applied+1}, __hs__/{latest}]
    2. 逐条 Apply
    3. 更新 m_appliedEpoch

  定期校验 (每 10 分钟):
    1. 从 TiKV 读 max epoch
    2. if max_epoch > m_appliedEpoch → catch-up
    3. 防止 gossip 长期丢失导致悄悄过时
```

---

### 9.5 链路 ④ Merge Lock（跨节点锁链路）

#### 9.5.1 完整数据流图

```
Merge Initiator (Node K)                    Lock Owner (Node J)
       │                                          │
 ① 发现 headID=42 需要 merge                      │
   (posting list 过短)                             │
       │                                          │
 ② owner(42) = Node J                             │
       │                                          │
 ③ RemoteLockReq ─────────────────────────────────►│
   {headID=42, lock=true,                         │
    leaseID=uuid, duration=30s}                   │
       │                                          │
       │                                   ④ TryLock(42):
       │                                     if 未锁 → 授予, 记录 lease
       │                                     if 已锁且未过期 → 拒绝
       │                                     if 已锁且已过期 → 强制释放 → 授予
       │                                          │
       │  ⑤ RemoteLockResp(granted=true) ◄────────│
       │                                          │
 ⑥ 执行 merge 操作                                 │
   (读取 posting, 合并,                            │
    写回 TiKV, 更新 head index)                    │
       │                                          │
   ┌─ if merge 超过 15s ──────────────────────────┐│
   │ ⑥' LeaseRenewReq(headID=42, leaseID=uuid) ──►│
   │     → 续约 30s                                ││
   └──────────────────────────────────────────────┘│
       │                                          │
 ⑦ RemoteLockReq ─────────────────────────────────►│
   {headID=42, lock=false, leaseID=uuid}          │
       │                                   ⑧ Unlock(42, leaseID):
       │                                     leaseID 匹配 → 释放
       │                                     leaseID 不匹配 → 忽略
       │                                       (说明中途过期被别人抢了)
       │                                          │
       │  ⑨ RemoteLockResp(released) ◄────────────│
       │                                          │
```

#### 9.5.2 故障点 × 容错矩阵

```
  故障点           位置               后果                  容错机制
 ────────────────────────────────────────────────────────────────────────
  F1  Lock        ③ 网络断           lock 未获得            Initiator 超时(5s) →
      请求失败    或 Owner 忙                               defer merge → async job
                                                           → 稍后重试

  F2  Initiator   ⑥ merge 中间      持有 lock 但           Lease 自动过期 (30s):
      Crash       进程 crash        无法 unlock             Lock Owner 定期扫描
                                                           过期 lock → 强制释放
                                                           → 不会永久死锁

  F3  Lock        ⑤-⑦ 之间         lock 已授予但           Lock Owner 检测 gossip:
      Owner       Owner crash       Owner 内存中的           新 owner = ring 重分配
      Crash                         lock 状态丢失           → merge initiator 重试
                                                           向新 owner 请求 lock

  F4  Unlock      ⑦ 网络断          unlock 未送达           30s 后 lease 自动过期
      失败                                                 → 不会死锁

  F5  Merge       ⑥ 超过 30s       lease 可能过期           每 15s 发 LeaseRenew
      太慢                          别人可能拿到 lock        若 renew 失败 →
                                                           merge 必须 abort + rollback
                                                           (TiKV 事务保证原子性)

  F6  Lease       ⑥' renew 失败    lease 已过期,            Initiator 检测 renew
      续约失败    (Owner crash      别人可能已拿锁            返回 false → abort merge
                  或网络断)                                  → 放弃本次 merge
```

#### 9.5.3 Lease 生命周期状态机

```
             TryLock(OK)                 Unlock(leaseID match)
  FREE ──────────────────► HELD ────────────────────────────────► FREE
                             │                                      ▲
                             │ time > expiresAt                     │
                             │                                      │
                             ▼                                      │
                          EXPIRED ──────────────────────────────────┘
                             │       (reap thread 清理)
                             │
                             │ 另一个 node TryLock
                             ▼
                          HELD (by new owner)

  关键: EXPIRED → FREE 由后台 reap 线程每 5s 扫描执行
       保证: 即使 reap 还没运行, 新的 TryLock 也会检测到过期并强制释放
```

---

### 9.6 链路 ⑤ Ring Change（拓扑变更链路）

#### 9.6.1 完整数据流图 — 节点加入

```
 New Node (2000)          PD (Seed Registry)        Gossip Network          Existing Nodes
       │                        │                        │                       │
 ① Register ───────────────────►│                        │                       │
   {addr, port,                 │                        │                       │
    status=joining}             │                        │                       │
       │◄──── Membership List ──│                        │                       │
       │                        │                        │                       │
 ② Build hash ring             │                        │                       │
   (from membership list)       │                        │                       │
       │                        │                        │                       │
 ③ Pull head index snapshot ─────────────────────────────────────────────────────►│
   (from any existing peer)     │                        │                       │
       │◄───── snapshot ─────────────────────────────────────────────────────────│
       │                        │                        │                       │
 ④ AddNode(2000) locally       │                        │                       │
   Ring now includes self       │                        │                       │
       │                        │                        │                       │
 ⑤ Join gossip ──────────────────────────────────────────►│                       │
   MembershipUpdate:            │                        │                       │
   {node=2000, state=Alive}     │                       ⑥ Gossip propagation     │
       │                        │                        │──────────────────────►│
       │                        │                        │                       │
       │                        │                        │              ⑦ Each node:
       │                        │                        │                AddNode(2000)
       │                        │                        │                ring updated
       │                        │                        │                       │
       │                        │                        │              ⑧ New writes to
       │                        │                        │                headIDs now owned
       │                        │                        │                by 2000 → route
       │                        │                        │                to 2000
       │                        │                        │                       │
 ⑨ Ready to serve              │                        │                       │
   status = active              │                        │                       │
       │                        │                        │                       │
```

#### 9.6.2 完整数据流图 — 节点故障

```
 Failed Node (500)         Gossip Peers              All Other Nodes           TiKV
       │                       │                          │                     │
   ╳ CRASH                     │                          │                     │
   (t=0)                       │                          │                     │
                               │                          │                     │
                        ① Ping(500) → no ACK              │                     │
                          (t=1s)                          │                     │
                               │                          │                     │
                        ② PingReq to 3 peers:             │                     │
                          "please ping 500"               │                     │
                          (t=1.5s)───────────────────────►│                     │
                               │                          │                     │
                               │              ③ Peers ping 500 → no ACK        │
                               │◄───── all report fail ───│                     │
                               │  (t=3.5s)                │                     │
                               │                          │                     │
                        ④ Mark 500 = Suspect              │                     │
                          Gossip SuspectMsg ─────────────►│                     │
                          (t=3.5s)                        │                     │
                               │                          │                     │
                          (wait 5s)                       │                     │
                               │                          │                     │
                        ⑤ Mark 500 = Dead                 │                     │
                          Gossip DeadMsg ────────────────►│                     │
                          (t=8.5s)                        │                     │
                               │                   ⑥ All nodes:                 │
                               │                     RemoveNode(500)            │
                               │                     Ring re-hash               │
                               │                          │                     │
                               │                   ⑦ headIDs owned by 500      │
                               │                     now map to other nodes     │
                               │                     New owners serve directly  │
                               │                     from TiKV (shared storage) │
                               │                          │─── Read/Write ─────►│
                               │                          │                     │
```

#### 9.6.3 Ring Change 期间的读写一致性

```
问题: Ring 在 gossip 传播的 ~11s 内, 不同 node 看到的 ring 版本不同

  t=0   Node 2000 joins. Node A 已更新 ring, Node B 还未更新.
  t=1   Client insert via Node A → headID=42 route to Node 2000 (new owner) ✅
  t=2   Client insert via Node B → headID=42 route to Node 500 (old owner) ❓

这不是问题, 因为:

  ┌──────────────────────────────────────────────────────────────────┐
  │  TiKV 是共享存储, posting data 按 headID key 存储               │
  │                                                                 │
  │  无论 Append(headID=42) 从 Node 500 还是 Node 2000 发起:       │
  │    → 都写入同一个 TiKV key: {prefix}/posting/42                 │
  │    → TiKV Raft 保证串行化                                      │
  │                                                                 │
  │  "Owner" 的含义不是 "数据存在哪里"                               │
  │  而是 "由谁负责处理 Append/Search for this headID"              │
  │                                                                 │
  │  所以 ring 不一致期间:                                          │
  │    - 写: 旧 owner 和新 owner 都可以写 TiKV → 正确              │
  │    - 读: 旧 owner 和新 owner 都可以读 TiKV → 正确              │
  │    - 唯一代价: 旧 owner 可能多做了一些不必要的工作               │
  │      (处理了按新 ring 不属于它的 headID)                         │
  │    - 11s 后所有 node 收敛到同一个 ring → steady state           │
  └──────────────────────────────────────────────────────────────────┘

  补充: Split/Merge 操作需要精确的 owner 判定
    → 使用 ring epoch: merge 前检查 ring epoch
    → 如果 epoch 变了 → 重新判定 owner → 如果不是自己 → abort merge
```

---

### 9.7 五条链路容错汇总

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        数据链路容错全景                                       │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────┐         │
│  │  链路 ① Insert                                                  │         │
│  │                                                                 │         │
│  │  Client ──► Ingress ──► local Append ──► TiKV                  │         │
│  │                │                                                │         │
│  │                ├──► BatchAppendReq ──► Owner ──► TiKV           │         │
│  │                │          │                                     │         │
│  │                │    wait ACK (30s timeout)                      │         │
│  │                │          │                                     │         │
│  │                ├─ ALL OK ─────► return SUCCESS to client        │         │
│  │                └─ ANY FAIL ──► return FAIL to client            │         │
│  │                                  ↓                              │         │
│  │                            client retry (幂等安全)              │         │
│  │                                                                 │         │
│  │  保证: SUCCESS = 100% durable; FAIL = client 知情可 retry       │         │
│  │  延迟: 正常 ~ms (1 RTT to owner + 1 TiKV write)                │         │
│  └─────────────────────────────────────────────────────────────────┘         │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────┐         │
│  │  链路 ② Search                                                  │         │
│  │                                                                 │         │
│  │  Client ──► Ingress ──► Owner(s) ──► TiKV                      │         │
│  │                │  5s timeout │                                   │         │
│  │                │     ▼       │                                   │         │
│  │                │  Local      Level 0: 全部成功 (最佳 recall)     │         │
│  │                │  Fallback   Level 1: 部分超时, TiKV 直读补       │         │
│  │                │     │       Level 2: TiKV 也失败, 跳过该 head   │         │
│  │                │     ▼       Level 3: 全部失败, 仅 local heads   │         │
│  │                │  Hedged Req (optional, low fan-out only)        │         │
│  │                                                                 │         │
│  │  保证: Always available (degraded recall when partial failure)   │         │
│  │  延迟: 正常 ~ms; 降级时 max(5s timeout, local TiKV read)        │         │
│  └─────────────────────────────────────────────────────────────────┘         │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────┐         │
│  │  链路 ③ Head Sync                                               │         │
│  │                                                                 │         │
│  │  Origin ──► TiKV(Log) ──► Gossip ──► All Nodes                 │         │
│  │               ▲              │                                   │         │
│  │    持久化边界 ─┘              │ O(logN) 轮传播                    │         │
│  │                              │                                   │         │
│  │    Gap 检测 + TiKV Catch-Up  │ 定期全量校验                      │         │
│  │                                                                 │         │
│  │  保证: Eventually consistent; gap auto-healed                   │         │
│  │  延迟: ~11s (2000 nodes); catch-up ~seconds                     │         │
│  └─────────────────────────────────────────────────────────────────┘         │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────┐         │
│  │  链路 ④ Merge Lock                                              │         │
│  │                                                                 │         │
│  │  Initiator ──► Lock Owner ──► HELD (30s lease)                  │         │
│  │      │                            │                              │         │
│  │      │ lease renew (15s)          │ auto-expire                  │         │
│  │      │ abort on renew fail        │ reap thread (5s)             │         │
│  │                                                                 │         │
│  │  保证: No deadlock (lease auto-expire); at-most-once merge      │         │
│  │  延迟: lock acquire ~ms; expire recovery 30s                    │         │
│  └─────────────────────────────────────────────────────────────────┘         │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────┐         │
│  │  链路 ⑤ Ring Change                                             │         │
│  │                                                                 │         │
│  │  Node Join/Leave ──► PD Register ──► Gossip ──► All Nodes      │         │
│  │                                                                 │         │
│  │  Node Crash ──► SWIM Detection (~8s) ──► RemoveNode            │         │
│  │                                                                 │         │
│  │  TiKV shared storage → 无需数据迁移                              │         │
│  │  Ring 不一致窗口 (~11s) → 对正确性无影响                          │         │
│  │                                                                 │         │
│  │  保证: Routing convergence in O(logN) rounds                    │         │
│  │  延迟: join ~5s; crash detection ~8.5s; convergence ~11s        │         │
│  └─────────────────────────────────────────────────────────────────┘         │
│                                                                              │
│  脑裂防护:                                                                   │
│    node 必须 (连接到 PD) OR (gossip 可达 > N/2 peers) 才能 serve writes      │
│    否则自动降级 read-only → 最终 shutdown                                    │
└──────────────────────────────────────────────────────────────────────────────┘
```
        while (!m_stopped) {
            {
                std::lock_guard<std::mutex> guard(m_mgr_mutex);
                auto now = NowMs();
                for (auto it = m_locks.begin(); it != m_locks.end(); ) {
                    if (now > it->second.expiresAt) {
                        SPTAGLIB_LOG(LL_Warning,
                            "Reaping expired lock: headID=%d node=%d\n",
                            it->first, it->second.holderNode);
                        it->second.lock.unlock();
                        it = m_locks.erase(it);
                    } else {
                        ++it;
                    }
                }
---

## 9.8 Job Routing 容错

本节专门讨论 **routing 决策路径上的故障处理**——Ingress 怎么把请求路由到正确的 Owner，以及 Owner 不可达、Ring 不一致时的容错策略。

### 9.8.1 路由决策流程

```
  Ingress 收到 Insert/Search 请求:

  ① RNGSelection / Head Search → 得到 headID 列表
  ② for each headID:
       owner = hashRing.GetOwner(headID)
       if owner == self → 本地处理
       else → 路由到 owner
  ③ 等待所有 owner 回复 (timeout = 30s Insert / 5s Search)
  ④ 汇总结果 → 返回 client

  路由决策的唯一依据: 本地 hash ring (内存, 无网络)
  hash ring 通过 gossip 最终一致 (收敛 ~11s)
```

### 9.8.2 Owner 不可达: Blacklist + 快速失败

```
  问题: 一个 Owner crash 后, gossip 需要 ~8.5s 才能检测到并 RemoveNode
        在这 8.5s 内, 所有路由到该 Owner 的请求都会等 30s 超时
        → 大量请求堆积, Ingress 线程池耗尽

  解决: Ingress 维护本地 Owner Blacklist (短期记忆)

  struct OwnerBlacklist {
      // owner nodeIndex → 失败时间
      // key 存在 && now - failTime < BLACKLIST_TTL → 该 owner 视为不可达
      std::unordered_map<int, uint64_t> m_failed;
      static constexpr uint64_t BLACKLIST_TTL = 15'000;  // 15 秒

      bool IsBlacklisted(int owner) {
          auto it = m_failed.find(owner);
          if (it == m_failed.end()) return false;
          if (NowMs() - it->second > BLACKLIST_TTL) {
              m_failed.erase(it);  // 过期, 允许重试
              return false;
          }
          return true;
      }

      void MarkFailed(int owner) {
          m_failed[owner] = NowMs();
      }
  };

  路由逻辑变为:

  for each headID:
    owner = hashRing.GetOwner(headID)
    if blacklist.IsBlacklisted(owner):
      // 快速失败, 不等 30s 超时
      → Insert: 该 headID 标记失败 → 整个 batch FAIL (快速返回)
      → Search: 跳过该 headID 或 fallback 到 TiKV 直读 (降级)
    else:
      send request to owner
      if timeout / TCP error:
        blacklist.MarkFailed(owner)

  效果:
    第 1 个请求: 等 30s 超时 → MarkFailed(owner)
    后续请求:    检测到 blacklist → 立即 FAIL (ms 级) → client 快速 retry
    15s 后:      blacklist 过期 → 若 gossip 已 RemoveNode, ring 已更新, 新 owner 接管
                                → 若 owner 恢复了, 重新路由到它
```

### 9.8.3 Owner 收到不属于自己的 headID

```
  场景: Ring 变更传播中, Ingress 的 ring 是旧的, 把 headID 发给了旧 Owner

  旧 Owner 怎么处理?

  方案 A: 拒绝 + 返回 "NOT_OWNER" 错误
    → 需要 Ingress 重新查 ring → 复杂, 可能 Ingress ring 也没更新
    → 多一次网络往返

  方案 B: 直接处理 (当前设计)
    → TiKV 共享存储, 旧 Owner 写同一个 key → 正确性不受影响
    → 唯一代价: 旧 Owner 做了一次"不必要"的工作
    → ring 收敛后 (~11s) 自动修复

  选择: 方案 B (直接处理)

  理由:
    1. 简单 — 不需要 ownership 校验 + 重试协议
    2. 正确 — TiKV key = headID, 谁写都一样
    3. 短暂 — ring 不一致窗口只有 ~11s
    4. 无害 — "多做了一次工作" vs "引入复杂的转发/重试" → 前者代价更低

  例外: Merge 操作需要精确 ownership
    → Merge 前用 ring epoch 二次校验
    → epoch 变了 → abort merge → 等 ring 收敛后重试
    → 这是已有设计 (§9.5 Merge Lock)
```

### 9.8.4 Batch 内多 Owner 的路由重试策略

```
  场景: 一个 batch 的 10 个向量散到 3 个 Owner:
    local(K):  [headID=99, 200, 301]   → ④a OK
    Node J:    [headID=42, 55]         → ④b OK
    Node M:    [headID=1001, 1002]     → ④b FAIL (blacklisted / 超时)

  选择 (已有设计 §9.2.7): 全部 FAIL, client retry 整个 batch

  但这里有一个优化空间:

  问题: 是否值得 Ingress 在 batch 内对单个 Owner 做 retry?

  方案 A: 不 retry, 直接 FAIL (当前设计)
    Ingress:  Node M 超时 → return FAIL to client
    Client:   retry 整个 batch
    优:  Ingress 逻辑简单, 快速释放线程
    劣:  已成功的 8 条白做了 (幂等保证安全, 但浪费)

  方案 B: Ingress 对失败的 Owner 做一次 retry (可选优化)
    Ingress:  Node M 超时 → MarkFailed(M) → 查 ring 找 M 的 successor
              → 如果 gossip 已 RemoveNode(M), ring 有新 Owner → retry 到新 Owner
              → 如果 ring 还没更新 → GetOwner 仍返回 M → 放弃, return FAIL
    优:  gossip 快的话, 一次 retry 就能成功, 不用麻烦 client
    劣:  多一次网络等待, 复杂度增加

  决策: 默认方案 A, 方案 B 作为后期优化

  理由:
    1. Owner 故障是稀有路径 — 不值得在热路径上增加复杂度
    2. Client retry 整个 batch 是幂等安全的 — 正确性有保证
    3. Blacklist 保证后续请求不会再等 30s — 第一次 FAIL 后 client 秒级 retry
    4. 方案 B 依赖 "gossip 已经更新了 ring" — 如果没更新, retry 也会失败
```

### 9.8.5 Search 路由降级策略

```
  Search 和 Insert 的路由容错不同:
    Insert: 必须全部成功 → 任何 Owner 失败 = FAIL
    Search: 可以降级 → Owner 失败 = 跳过该 headID, recall 降低但仍可用

  完整降级梯度:

  Level 0 (正常):
    所有 Owner 可达, 所有 headID 的 posting 都读到
    → 最佳 recall

  Level 1 (单 Owner 不可达):
    headID=42 的 Owner J 超时
    → blacklist.MarkFailed(J)
    → 尝试 fallback: Ingress 自己从 TiKV 直读 headID=42 的 posting
       (任何 node 都能读 TiKV, 不必经过 Owner)
    → recall 不受影响, 延迟略增 (多一次 TiKV 读)

  Level 2 (TiKV 直读也失败):
    headID=42 的 posting 在 TiKV 中也读不到 (TiKV region 不可用)
    → 跳过 headID=42
    → recall 降低: 少了 headID=42 贡献的 candidates
    → 仍然返回结果, 不报错 (best-effort)

  Level 3 (多个 Owner 不可达):
    只剩 local heads 可用
    → recall 显著降低
    → 仍然返回结果 + 在 response 中标注 degraded=true
    → 让 client 知道结果可能不完整

  Search 路由伪代码:

  SearchResult Ingress::HandleSearch(query, K) {
      candidates = HeadSearch(query)   // 本地 BKT
      groups = GroupByOwner(candidates)
      results = {}

      // 本地 heads: 直接处理
      for hid in groups[self]:
          results += SearchPosting(hid, query)

      // 远程 heads: 并发发送
      for (owner, hids) in groups:
          if owner == self: continue
          if blacklist.IsBlacklisted(owner):
              // 降级: 直接 TiKV 读 (不经过 Owner)
              for hid in hids:
                  try: results += TiKVDirectRead(hid, query)
                  catch: skip  // Level 2
              continue

          resp = SendSearchRequest(owner, hids, timeout=5s)
          if resp.ok:
              results += resp.candidates
          else:
              blacklist.MarkFailed(owner)
              // fallback: TiKV 直读
              for hid in hids:
                  try: results += TiKVDirectRead(hid, query)
                  catch: skip

      return TopK(results, K)
  }
```

### 9.8.6 Split Job 的路由容错

```
  Split 在 Owner 本地执行, 但有两个子操作涉及路由:

  ┌────────────────────────────────────────────────────────────────────┐
  │  子操作 1: Split 产生的新 headID 可能属于另一个 Owner             │
  │                                                                    │
  │  场景:                                                             │
  │    Node K 对 headID=42 做 Split → K-means 产生 cluster_B          │
  │    cluster_B 的中心 VID=newHead                                    │
  │    GetOwner(newHead) = Node J (不是 Node K!)                       │
  │                                                                    │
  │  当前代码行为: Split 不路由! 它直接写 TiKV (shared storage)       │
  │    db->Put(newHead, cluster_B_posting) ← Node K 直接写            │
  │    headIndex.Add(newHead, vector) ← Node K 本地                   │
  │    BroadcastHeadSync ← gossip 传播给所有人 (包括 Node J)          │
  │                                                                    │
  │  为什么可以这样做:                                                  │
  │    "Owner" 不是 "TiKV 数据在哪", 而是 "谁处理请求"               │
  │    Node K 写 TiKV 的 newHead key = Node J 写是完全一样的          │
  │    TiKV Raft 保证原子性                                            │
  │                                                                    │
  │  Split 之后:                                                       │
  │    newHead 的后续 Append/Search 由 Node J 处理 (ring 路由)         │
  │    但 Split 本身不需要跟 Node J 协调                               │
  │                                                                    │
  │  故障: Node J 在 split 期间 crash?                                 │
  │    → 对 split 操作零影响! split 不依赖 Node J                     │
  │    → Node J 恢复后, ring 重新路由, newHead 由新 owner 接管        │
  └────────────────────────────────────────────────────────────────────┘

  ┌────────────────────────────────────────────────────────────────────┐
  │  子操作 2: Split 后 Reassign 产生的远程 Append                     │
  │                                                                    │
  │  Split 完成后, Reassign 把"分错了"的向量移到更合适的 head          │
  │  → RNGSelection → 新 headID → GetOwner(newHeadID) → 可能是远程   │
  │  → QueueRemoteAppend → FlushRemoteAppends                         │
  │                                                                    │
  │  这走的是正常 Append 路由路径, 受 Blacklist 保护:                  │
  │    Owner 不可达 → MarkFailed → Reassign 失败 → 向量留在旧 head   │
  │    → 不影响正确性, 只影响搜索质量 (向量在次优 head 中)             │
  │    → 后续 RefineIndex 会重新尝试 Reassign                          │
  │                                                                    │
  │  Reassign 失败不需要 rollback:                                     │
  │    version 已经 bump → 旧 entry 变 stale → 但向量还在旧 posting   │
  │    搜索仍然能找到 (stale entry 直到 GC 前都还在旧 posting 里)     │
  │    → 惰性清理, 最终一致                                            │
  └────────────────────────────────────────────────────────────────────┘
```

### 9.8.7 Merge Job 的路由容错

```
  Merge 是路由最复杂的 job: 需要跨节点锁 + 读写两个 headID 的 posting

  ┌────────────────────────────────────────────────────────────────────┐
  │  完整路由链:                                                       │
  │                                                                    │
  │  Node K: headID=42 的 owner, 发现 posting 过短, 要 merge          │
  │                                                                    │
  │  ① 找 merge partner:                                               │
  │     headIndex.Search(headVector) → partner = headID=99             │
  │     GetOwner(99) = Node J (远程!)                                  │
  │                                                                    │
  │  ② 跨节点加锁:                                                    │
  │     SendRemoteLock(J, headID=99, lock=true) → 30s lease           │
  │     (这就是 §9.5 的 Merge Lock 链路)                               │
  │                                                                    │
  │  ③ 读 partner posting:                                            │
  │     db->Get(99) → 从 TiKV 直接读 (不经过 Node J)                  │
  │     (TiKV 共享存储, 任何 node 都能读)                              │
  │                                                                    │
  │  ④ 合并 + 写回:                                                    │
  │     db->Put(winner, merged) + db->Delete(loser) → 直接写 TiKV     │
  │                                                                    │
  │  ⑤ 释放远程锁:                                                    │
  │     SendRemoteLock(J, headID=99, lock=false)                       │
  │                                                                    │
  │  ⑥ HeadSync 广播:                                                 │
  │     BroadcastHeadSync([{Op::Delete, loser}])                       │
  └────────────────────────────────────────────────────────────────────┘

  路由故障点:

  ┌────────────────────────────────────────────────────────────────────┐
  │  故障                    后果                恢复                   │
  │ ─────────────────────────────────────────────────────────────────  │
  │                                                                    │
  │  ② 加锁: Node J 不可达   lock 失败           重新入队, 稍后重试    │
  │  (blacklisted / crash)   merge 不执行         → merge 延迟但不丢数据│
  │                                                                    │
  │  ② 加锁: Node J 拒绝     headID=99 已被       重新入队, 稍后重试   │
  │  (另一个 merge 在进行)   别人锁住              → 排队等待            │
  │                                                                    │
  │  ③ 读 TiKV: 读失败       没拿到 partner       abort merge         │
  │  (TiKV region 不可用)    的 posting            释放锁, 稍后重试    │
  │                                                                    │
  │  ④ 写 TiKV: 写失败       合并数据没写进去      abort merge         │
  │                                                释放锁, 稍后重试    │
  │                                                                    │
  │  ⑤ 解锁: Node J 不可达   lock 没释放          lease 30s 自动过期   │
  │                                               → 不会死锁           │
  │                                                                    │
  │  ②~⑤ 之间               持有锁但 merge       lease 30s 过期       │
  │  Node K 自己 crash       未完成               → 不会死锁           │
  │                          可能部分写入 TiKV     → M2/M3 恢复机制    │
  │                                                  (见 §9.2.8.5)    │
  │                                                                    │
  │  Ring 变更:              Node J 不再是         Merge abort         │
  │  ② 时 ring epoch 变了   headID=99 的 owner    → 重新查 ring       │
  │                          RPC 可能发给错人       → 找新 owner 重试   │
  │                                                                    │
  │  Blacklist 的作用:                                                  │
  │    merge 发现 partner 的 owner 在 blacklist 中                     │
  │    → 不尝试加锁 → 直接跳过该 partner → 选下一个 neighbor          │
  │    → 避免 merge 线程阻塞在不可达的 node 上                        │
  └────────────────────────────────────────────────────────────────────┘
```

### 9.8.8 Reassign Job 的路由容错

```
  Reassign = "delete from old head + insert to new head"
  本质是一次小型的跨节点 Append, 走正常路由路径

  ┌────────────────────────────────────────────────────────────────────┐
  │  路由链:                                                           │
  │                                                                    │
  │  Node K: Split/Merge 后决定把 VID=200 从 headID=42 移到 headID=99│
  │                                                                    │
  │  ① version++ (VID=200 的 version bump)                            │
  │     → 旧 posting entry (headID=42 中的 VID=200) 变 stale         │
  │                                                                    │
  │  ② GetOwner(99) = Node J                                          │
  │     → QueueRemoteAppend(J, {headID=99, VID=200, newVersion, vec}) │
  │     → FlushRemoteAppends(J)                                       │
  │                                                                    │
  │  ③ Node J: Append(99, {VID=200, ...}) → TiKV                     │
  └────────────────────────────────────────────────────────────────────┘

  路由故障:

  故障                    后果                    恢复
 ──────────────────────────────────────────────────────────────────────
  Node J 不可达           Append 失败              向量留在旧 head
  (blacklist/crash)       Reassign 放弃            → 搜索仍能找到 (旧 entry)
                                                   → version 已 bump 但新 entry
                                                     没写成功
                                                   → 问题: version 不匹配!

  *** 这里有一个微妙的问题 ***

  Reassign 先 bump version (①) 再 Append (②):
    如果 ② 失败:
      旧 entry: headID=42 中 VID=200, version=old → stale!
      新 entry: 不存在 (没写进去)
      → VID=200 在搜索中"消失"了? (旧 entry 被 GC 认为 stale 会清理掉)

  实际分析:
    GC 不是立即执行的 — 旧 entry 在 headID=42 的 posting 中还存在
    直到下一次 Split/GC 扫描 headID=42 时才会清理
    在清理之前, 搜索仍然能命中 VID=200 (从 headID=42 的 posting 中)
    只是 version 不匹配, 需要从 versionMap check — 如果 search 不检查则正常返回

  修复策略:
    方案 A: Reassign 失败时 rollback version (version--)
      → 旧 entry 变回有效, 不丢失
      → 但 version-- 不是原子的, 可能有 race

    方案 B (推荐): Reassign 先 Append 再 bump version
      → ② FlushRemoteAppends → 成功后 ① version++
      → 失败: version 没变, 旧 entry 仍有效, 新 entry 不存在 → 无影响
      → 成功: 新 entry 在, 旧 entry 变 stale → 正确
      → 代价: 短暂窗口内新旧 entry 同时有效 → 搜索可能返回重复 → 去重即可

    方案 C: 不管它
      → GC 是惰性的, 旧 entry 在 posting 中存在几小时
      → 搜索返回时检查 metadata 即可过滤
      → Reassign 本来就是可选优化, 不是正确性关键路径
      → 下次 RefineIndex 会再次尝试 Reassign

  决策: 方案 B — 先写后 bump, 简单安全
```

### 9.8.9 四种 Job 的路由容错汇总

```
  ┌────────────────────────────────────────────────────────────────────┐
  │              Job Routing 容错全景 (所有 Job 类型)                   │
  │                                                                    │
  │  Job 类型      路由目标        故障行为         Blacklist 作用      │
  │ ─────────────────────────────────────────────────────────────────  │
  │                                                                    │
  │  Append        Owner           FAIL → client    快速失败           │
  │  (client 路径) (headID owner)  retry             不等 30s          │
  │                                                                    │
  │  Search        Owner(s)        TiKV 直读        跳过 blacklisted   │
  │  (client 路径) (多 headID)     fallback          Owner, 直读 TiKV  │
  │                                                                    │
  │  Split         无远程路由!     N/A               N/A               │
  │  (后台 job)    直接写 TiKV                                         │
  │                HeadSync 广播                                       │
  │                                                                    │
  │  Split→        Owner(s)        放弃 reassign    跳过 blacklisted  │
  │  Reassign      (新 headID      向量留旧 head    Owner, 放弃该     │
  │  (后台 job)      owner)        惰性修复         向量的 reassign    │
  │                                                                    │
  │  Merge         Lock Owner      重新入队重试     跳过 blacklisted  │
  │  (后台 job)    (partner        lease 自动过期   partner, 试下一个  │
  │                 headID owner)  不死锁           neighbor           │
  │                                                                    │
  │  Merge→        同 Reassign     同 Reassign      同 Reassign       │
  │  Reassign                                                          │
  │                                                                    │
  │ ─────────────────────────────────────────────────────────────────  │
  │                                                                    │
  │  核心原则:                                                         │
  │    Client 路径 (Append/Search): 快速失败, 让 client retry          │
  │    后台路径 (Split/Merge/Reassign): 重新入队, 系统自动重试         │
  │    Blacklist 统一保护: 所有路由决策共享同一个 blacklist             │
  │    正确性不依赖路由: TiKV 是 ground truth, head index 可重建      │
  └────────────────────────────────────────────────────────────────────┘
```

---

## 10. 新增 & 修改文件清单

### 新增文件

| 文件 | 职责 |
|------|------|
| `AnnService/inc/Core/SPANN/GossipProtocol.h` | SWIM gossip 协议实现：Ping/PingReq/Ack/Update 处理、故障检测 FSM、piggyback 成员变更 |
| `AnnService/inc/Core/SPANN/ConnectionPool.h` | 按需连接池：LRU 淘汰、自动重连、idle timeout |
| `AnnService/inc/Core/SPANN/DistributedVIDAllocator.h` | VID 分段预分配：从 TiKV 原子领取 block、本地无锁自增 |
| `AnnService/inc/Core/SPANN/HeadSyncLog.h` | Epoch-based head index 增量同步日志、catch-up 协议 |
| `AnnService/inc/Core/SPANN/MigrationManager.h` | 扩缩容数据迁移管理：cache warm-up、进度跟踪 |

### 修改文件

| 文件 | 改动点 |
|------|--------|
| **PostingRouter.h** | 1. `Start()`: 从 PD 拉成员列表替代 INI 静态配置<br>2. 新增 `StartGossip()`<br>3. `m_peerConnections` → `ConnectionPool`<br>4. 删除 `QueueRemoteInsert()`/`SendInsertBatch()`/`HandleInsertBatchRequest()`<br>5. `BroadcastHeadSync()` → `GossipHeadSync()`<br>6. `SendBatchRemoteAppend()` 增加 ring-aware 重试 |
| **ExtraDynamicSearcher.h** | 1. `AddIndex()`: 删除 `VID % numNodes` 分支，所有向量本地 RNGSelection<br>2. `InitializeRouter()`: 注册 gossip 回调 |
| **SPANNIndex.h/cpp** | 1. `m_iCurAssignedVID` → `DistributedVIDAllocator`<br>2. 删除 `FlushRemoteInserts()` 调用 |
| **Socket/Packet.h** | 新增 PacketType: MembershipPing/PingReq/Ack/Update, MigrationData* |
| **ParameterDefinitionList.h** | 删除 `RouterNodeAddrs`/`RouterNodeStores` 参数<br>新增 `SeedRegistryAddr`/`NodePort`/`VIDBlockSize` |
| **SPFreshTest.cpp** | 统一 WorkerNode 和 DriverNode 为同一个 `PeerNode` test case |

---

## 11. 配置变更

### 旧配置 (INI)
```ini
[BuildSSDIndex]
RouterEnabled=true
RouterLocalNodeIndex=0
RouterNodeAddrs=host1:30001,host2:30002,...,host1999:32000  # 2000 个地址!
RouterNodeStores=tikv1:20161,...,tikv1999:21999              # 2000 个 store!
```

### 新配置 (INI)
```ini
[BuildSSDIndex]
RouterEnabled=true
NodeListenPort=30001                # 本 node 监听端口（host 自动检测或指定 NodeListenAddr）
SeedRegistryAddr=pd1:2379           # PD 地址（用于成员注册 + VID 分配）
TiKVPDAddresses=pd1:2379,pd2:2379,pd3:2379
VIDBlockSize=1000000                # 每次领取的 VID block 大小
GossipIntervalMs=1000               # Gossip 心跳间隔
GossipSuspectTimeoutMs=5000         # 从 Suspect 到 Dead 的超时
MaxActiveConnections=200             # 每 node 最大活跃对等连接数
HeadSyncMode=gossip                  # gossip | broadcast (向后兼容)
```

---

## 12. 性能估算（2000 节点）

| 指标 | 当前 (Driver 协调) | 对等模式 |
|------|-------------------|----------|
| Insert 吞吐 | 受限于 Driver CPU/带宽 | 线性扩展：2000× single-node |
| Insert 延迟 | Client → Driver → Worker → TiKV (3 hop) | Client → Ingress → Owner → TiKV (3 hop, 但无 Driver 排队) |
| Search 扇出 | Driver → 所有 node (O(N)) 或 round-robin | Ingress → ~32 node (O(candidateCount)) |
| 连接数/node | 1999 (full mesh) | ~100~200 (按需连接) |
| VID 分配延迟 | 全局锁 (~1ms 含网络) | 本地原子操作 (~10ns)；每 100 万次才需 1 次 TiKV txn |
| 成员变更传播 | 需重新配置 INI + 重启 | Gossip ~11s 传遍 2000 节点 |
| Head sync 传播 | 1999 次 TCP 发送 | Gossip O(log N) 轮 ~11s |
| 故障检测 | 无（Driver 挂全停） | SWIM ~8s 检测 + 自动 ring 调整 |

---

## 13. 分阶段实施路线

### Phase 1: 去中心化 Insert（最高优先级）
- 实现 `DistributedVIDAllocator`
- 修改 `AddIndex()` 删除 VID-based routing，统一为 headID-based routing
- 删除 `QueueRemoteInsert`/`SendInsertBatch`/`HandleInsertBatchRequest`
- **效果**：立即消除 Driver 瓶颈，每个 node 可独立接受 insert

### Phase 2: 按需连接池
- 实现 `ConnectionPool` 替代 full-mesh
- 修改 `Start()` 为延迟连接
- **效果**：连接数从 O(N²) 降到 O(N × K)，K ≈ 200

### Phase 3: Gossip 成员管理
- 实现 `GossipProtocol`（SWIM 变体）
- 从 PD 做 seed discovery
- 自动 `AddNode()`/`RemoveNode()`
- **效果**：支持动态扩缩容，无需重启

### Phase 4: Head Sync 改造
- 实现 `HeadSyncLog` epoch-based 增量日志
- `BroadcastHeadSync()` → gossip piggyback
- Catch-up 协议
- **效果**：head sync 从 O(N) fan-out 变为 O(log N) gossip

### Phase 5: 容错加固
- ring-aware 重试逻辑
- 脑裂防护
- 健康检查 + 自动降级
- **效果**：无单点故障，任意 node crash 自动恢复
