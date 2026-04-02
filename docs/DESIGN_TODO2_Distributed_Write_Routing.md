# TODO 2: Distributed Write Routing for SPFRESH

## 1. Problem Background

### 1.1 Context: TODO 1 (TiKV Storage Backend)

TODO 1 (`users/qiazh/merge-spfresh-tikv`) replaced the local posting storage with a
shared TiKV cluster:

- `ExtraTiKVController` implements `KeyValueIO` over TiKV's RawKV API
- Posting data is distributed across TiKV Regions (~96 MB each, Raft-replicated)
- PD (Placement Driver) manages Region→Store mapping
- Head index is loaded into memory on each compute node (full replica)

**What TODO 1 does not do**: distribute *compute*. All inserts still run on a single
node, which becomes the bottleneck.

### 1.2 What TODO 2 Solves

With shared TiKV storage, we can run N compute nodes, each with a full head index
replica. The problem: when a vector arrives at any node for insertion, `AddIndex` calls
`RNGSelection` (local, fast) then `Append(headID, ...)` which does `db->Merge(headID, posting)`.

If two nodes concurrently Append to the same `headID`:
1. **VersionMap divergence** — each node's local `m_versionMap` gets out of sync
2. **Merge atomicity** — `MergePostings` reads then writes a posting; concurrent writes
   corrupt it
3. **Split collision** — two nodes Split the same posting simultaneously

**Solution**: route all writes for a given `headID` to a single designated compute node.
If we guarantee that the same `headID` always routes to the same node, all three
problems disappear without distributed locking.

### 1.3 Bug Fixes (Ported from TODO 2 Lock-Routing Branch)

Six bugs were fixed and ported to the TODO 1 codebase:

| # | Bug | Fix |
|---|-----|-----|
| 1 | `SplitAsync` deadlock: `shared_lock(m_splitListLock)` + `operator[]` | Changed to `unique_lock` |
| 2 | Split with k=1: `GetKMeans` returns `cut > localIndices.size()` → OOB | Clamp `cut` to `localIndices.size()` |
| 3 | `AsyncAppend` queue drain: holds `m_asyncAppendLock` during I/O | Drain to local vector, release lock, then process |
| 4 | Append `goto checkDeleted` infinite loop on error | Return error immediately |
| 5 | Reassign shares `m_splitThreadPool`: no parallelism | Enable `m_reassignThreadPool` when `m_reassignThreadNum > 0` |
| 6 | No backpressure on background tasks | Add `m_maxBackgroundTaskQueueSize` check |

---

## 2. Architecture

### 2.1 Deployment Topology

```
                    ┌─────────────────────────┐
                    │       Load Balancer      │
                    │  (round-robin inserts)   │
                    └──────┬──────────┬────────┘
                           │          │
              ┌────────────▼──┐  ┌────▼───────────┐
              │  Compute      │  │  Compute        │
              │  Node 0       │  │  Node 1         │   ...N
              │               │  │                 │
              │ [Head Index]  │  │ [Head Index]    │  ← full replica
              │ [VersionMap]  │  │ [VersionMap]    │  ← local only
              │ [PostingRouter│  │ [PostingRouter] │  ← routes writes
              │  ↕ Socket RPC]│  │  ↕ Socket RPC]  │
              └───────┬───────┘  └───────┬─────────┘
                      │                  │
                      └─────────┬────────┘
                                │  TiKV RawKV API
                    ┌───────────▼───────────┐
                    │      TiKV Cluster      │
                    │  PD ← Region mapping   │
                    │  Store 0   Store 1 ... │
                    └────────────────────────┘
```

### 2.2 Routing Principle

```
headID → db->GetKeyLocation(headID) → KeyLocation { regionId, leaderStoreAddr }
       → storeToNode[leaderStoreAddr] → target compute node index
       → if local: Append() directly
         if remote: SendRemoteAppend() via Socket RPC
```

**Why store-based routing?**  TiKV already distributes data by key range into Regions,
and each Region has a leader on a specific Store. By routing writes to the compute node
*nearest* the TiKV Store holding the leader, we minimize network hops for the
`db->Merge()` call.

**Consistency guarantee**: Same headID → same Region → same leader Store → same compute
node. Routing only changes when TiKV rebalances Regions (rare, handled by cache
invalidation).

### 2.3 Key Invariant

> For any headID H, at any point in time, all Append/Split/Merge operations on H
> execute on exactly one compute node.

This makes VersionMap, Merge atomicity, and Split correctness trivially local problems.

---

## 3. Implementation Details

### 3.1 KeyLocation Interface (`KeyValueIO.h`)

```cpp
struct KeyLocation {
    uint64_t regionId = 0;
    std::string leaderStoreAddr;
};

// Added to KeyValueIO base class (default returns false for non-distributed backends):
virtual bool GetKeyLocation(SizeType key, KeyLocation& loc) { return false; }
virtual bool GetKeyLocations(const std::vector<SizeType>& keys,
                             std::vector<KeyLocation>& locs) { return false; }
```

`ExtraTiKVController` overrides these using `FindRegionForKey()` from the TiKV client.

### 3.2 PostingRouter (`PostingRouter.h`)

Core class that manages distributed write routing:

```cpp
class PostingRouter {
    // Configuration
    bool m_enabled;
    int m_localNodeIndex;
    std::shared_ptr<Helper::KeyValueIO> m_db;
    std::vector<std::pair<std::string, std::string>> m_nodeAddrs;  // host:port per node
    std::vector<std::string> m_nodeStores;  // TiKV store addr per node
    std::unordered_map<std::string, int> m_storeToNode;

    // Server: listens for incoming remote AppendRequests
    std::unique_ptr<Socket::Server> m_server;

    // Client: sends remote AppendRequests to peer nodes
    std::unique_ptr<Socket::Client> m_client;
    std::vector<Socket::ConnectionID> m_peerConnections;

    // Response matching (synchronous RPC via promise/future)
    std::unordered_map<Socket::ResourceID, std::promise<ErrorCode>> m_pendingResponses;

    // Callback for handling local appends (set by ExtraDynamicSearcher)
    AppendCallback m_appendCallback;
};
```

**Key methods**:
- `GetOwner(headID) → RouteTarget { nodeIndex, isLocal }` — queries `db->GetKeyLocation`
- `SendRemoteAppend(nodeIdx, headID, headVec, appendNum, posting) → ErrorCode` —
  serializes `RemoteAppendRequest`, sends via Socket, waits for response (30s timeout)
- `HandleAppendRequest(connID, packet)` — deserializes, calls `m_appendCallback` for
  local Append, sends `RemoteAppendResponse` back

### 3.3 Packet Types (`Packet.h`)

```cpp
AppendRequest  = 0x04,
AppendResponse = ResponseMask | AppendRequest  // = 0x84
```

### 3.4 RemoteAppendRequest/Response Wire Format

```
RemoteAppendRequest:
  [uint16 majorVersion][uint16 mirrorVersion]
  [SizeType headID]
  [uint32 headVecLen][bytes headVec]
  [int32 appendNum]
  [uint32 postingLen][bytes appendPosting]

RemoteAppendResponse:
  [uint16 majorVersion][uint16 mirrorVersion]
  [uint8 status]  // 0=Success, 1=Failed
```

### 3.5 AddIndex Integration (`ExtraDynamicSearcher.h`)

The routing decision is inserted in the `AddIndex` loop, between `RNGSelection` and
`Append`:

```cpp
for (int i = 0; i < replicaCount; i++) {
    ErrorCode ret;
    auto headVec = make_shared<string>(selections[i].Vec.Data(), m_vectorDataSize);

    // Distributed routing: check if this headID should go to a remote node
    if (m_router && m_router->IsEnabled()) {
        auto target = m_router->GetOwner(selections[i].VID);
        if (!target.isLocal) {
            ret = m_router->SendRemoteAppend(
                target.nodeIndex, selections[i].VID, headVec, 1, appendPosting);
            if (ret != ErrorCode::Success) return ret;
            continue;  // skip local Append
        }
    }

    // Local path (unchanged)
    if (m_opt->m_asyncAppendQueueSize > 0) {
        ret = AsyncAppend(...);
    } else {
        ret = Append(...);
    }
}
```

### 3.6 Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `RouterEnabled` | bool | false | Enable distributed write routing |
| `RouterLocalNodeIndex` | int | 0 | This node's index in the cluster |
| `RouterNodeAddrs` | string | "" | Comma-separated `host:port` for each node's router |
| `RouterNodeStores` | string | "" | Comma-separated TiKV store addresses, one per node |

Example configuration:
```ini
RouterEnabled=true
RouterLocalNodeIndex=0
RouterNodeAddrs=10.0.0.1:9100,10.0.0.2:9100,10.0.0.3:9100
RouterNodeStores=tikv-store1:20160,tikv-store2:20160,tikv-store3:20160
```

### 3.7 Initialization Flow

1. `ExtraDynamicSearcher` constructor checks `m_opt->m_routerEnabled`
2. Parses `RouterNodeAddrs` and `RouterNodeStores` into vectors
3. Creates `PostingRouter`, calls `Initialize(db, localNodeIdx, addrs, stores)`
4. Sets `AppendCallback` → creates local `ExtraWorkSpace` and calls `Append()`
5. Calls `Start()` → launches Socket Server + connects Socket Client to all peers

---

## 4. Design Decisions

### 4.1 Why Store-Based Routing (not Hash-Based)?

| Approach | Pros | Cons |
|----------|------|------|
| Hash(headID) % N | Simple, deterministic | Ignores data locality; writes cross the network twice (compute→TiKV store on different rack) |
| Store-based | Write goes to compute node near TiKV leader → minimal network hops | Depends on TiKV Region placement; slightly more complex |

We chose store-based because TiKV already co-locates related keys in the same Region,
and Region leaders are on specific Stores. Routing to the nearest compute node
minimizes the `db->Merge()` latency.

### 4.2 Why Synchronous RPC (not Fire-and-Forget)?

`AddIndex` must know if the Append succeeded (for error propagation and WAL ordering).
We use `promise/future` with a 30-second timeout. This is acceptable because:
- Append latency is typically <10ms
- The insert thread is already blocked on Append anyway
- Fire-and-forget would require a distributed WAL, which is out of scope

### 4.3 Why Not Distributed VersionMap?

VersionMap is only consulted locally (during Append and Search). Since routing ensures
all writes for a headID go to the same node, each node's VersionMap is consistent for
the headIDs it owns. Search reads are snapshot-consistent (search reads stale data at
worst, which is acceptable for approximate nearest neighbor).

---

## 5. Files Changed

| File | Changes |
|------|---------|
| `AnnService/inc/Core/SPANN/PostingRouter.h` | **NEW** — PostingRouter, RemoteAppendRequest/Response |
| `AnnService/inc/Socket/Packet.h` | Added `AppendRequest=0x04`, `AppendResponse=0x84` |
| `AnnService/inc/Helper/KeyValueIO.h` | Added `KeyLocation` struct, `GetKeyLocation`/`GetKeyLocations` virtuals |
| `AnnService/inc/Core/SPANN/ExtraTiKVController.h` | Added `GetKeyLocation`/`GetKeyLocations` override |
| `AnnService/inc/Core/SPANN/ExtraDynamicSearcher.h` | Router init, AddIndex routing, 6 bug fixes |
| `AnnService/inc/Core/SPANN/Options.h` | 5 new params (router + backpressure) |
| `AnnService/inc/Core/SPANN/ParameterDefinitionList.h` | 5 new param definitions |

---

## 6. Future Work

### 6.1 Connection Resilience
- Reconnect on connection failure with exponential backoff
- Fall back to local Append if remote node is unreachable (accept temporary inconsistency)
- Health-check via Heartbeat: mark node as down after N missed heartbeats

### 6.2 Batch Routing Optimization
- Group multiple appends for the same target node into a single RPC
- Reduces per-vector RPC overhead when inserting large batches
- Requires buffering in AddIndex loop with flush at end

### 6.3 Region Migration Handling
- TiKV may rebalance Regions (leader transfer, Region split/merge)
- Cache `headID → nodeIndex` mapping with TTL
- On cache miss or stale routing, re-query `GetKeyLocation`
- Worst case: two nodes briefly both process the same headID → detect via VersionMap
  mismatch and re-route

### 6.4 Split/Merge Routing
- Currently only Append is routed; Split/Merge still execute locally
- Since Split/Merge are triggered by Append (same headID), they naturally execute on
  the correct node
- Cross-node Split (splitting a posting whose new head routes to a different node)
  may need explicit handling in the future

### 6.5 Per-Posting Job Queue (from Original Design)
- The original design proposed per-posting job queues with priority scheduling
  (Split > Merge > Reassign). This remains relevant for single-node optimization
  and can be layered on top of the distributed routing.
