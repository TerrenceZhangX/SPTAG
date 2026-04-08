# Distributed Insert for SPFresh + TiKV

---

## Slide 1: Problem — Single Node Bottleneck

**SPFresh insert is IO-heavy, not CPU-heavy.**

Single-node insert pipeline:
```
 Vector ──→ RNGSelection ──→ headID ──→ Append(headID, posting)
                (CPU)                         (IO: TiKV Put)
                                               │
                                    ┌──────────┤
                                    ▼          ▼
                               SplitAsync   MergeAsync
                              (IO: Get+Put) (IO: Get+Put)
```

- **RNGSelection** = search head index → find nearest headID. Pure CPU, fast (~μs)
- **Append** = read posting, append vector, write back to TiKV. IO-bound (~ms)
- **Split/Merge** = read posting, reorganize, write multiple postings. IO-heavy

**Bottleneck**: Append + Split + Merge are serialized per headID (needs lock).  
With 1 node, all IO goes through one process → throughput limited by IO concurrency.

---

## Slide 2: Solution — Distribute IO, Keep RNG Local

**Key insight: Don't distribute RNGSelection. Distribute the IO writes.**

Single Driver does all RNGSelection, then routes IO to owner nodes:

```
                        ┌─────────────────────────────────────────────────┐
                        │              Driver (Node 0)                    │
                        │                                                 │
 Vectors ──→ for each vector:                                             │
                        │   1. headID = RNGSelection(vector)              │
                        │   2. owner  = hash(headID) % NumNodes           │
                        │   3. if owner == me:                            │
                        │        Append(headID, vector)   ← local IO     │
                        │      else:                                      │
                        │        QueueRemoteAppend(owner, headID, vector) │
                        │                                                 │
                        │   4. FlushRemoteAppends()  ← batch send RPCs   │
                        └──────────┬──────────────────┬───────────────────┘
                                   │                  │
                          BatchAppend RPC      BatchAppend RPC
                                   │                  │
                          ┌────────▼────────┐ ┌──────▼──────────┐
                          │   Worker 1      │ │   Worker 2      │
                          │   Append(h,v)   │ │   Append(h,v)   │
                          │   → TiKV Put    │ │   → TiKV Put    │
                          │   → SplitAsync? │ │   → SplitAsync? │
                          │   → MergeAsync? │ │   → MergeAsync? │
                          └─────────────────┘ └─────────────────┘
```

**Why hash routing?**
- `hash(headID) % N` → deterministic, zero network lookup
- Same headID always → same node → **local per-headID lock suffices**
- No distributed lock needed (except Merge, see Slide 6)

---

## Slide 3: Single Machine vs Distributed — Full Flow Comparison

### Single Machine (1 Node)
```
 Vector
   │
   ▼
 RNGSelection(vector) ──→ headID
   │
   ▼
 lock(headID)
   │
   ▼
 Append(headID, vector) ──→ TiKV Put
   │
   ├──→ if posting > softLimit:  SplitAsync(headID)
   │      └─ K-means k=2 → new headID₁, headID₂
   │      └─ Put(headID₁), Put(headID₂), Delete(headID)
   │      └─ Update Head Index (Add new, Delete old)
   │
   ├──→ if posting < mergeThreshold:  MergeAsync(headID)
   │      └─ find neighbor in Head Index
   │      └─ merge two postings → Put combined
   │      └─ Update Head Index (Delete absorbed head)
   │
   └──→ unlock(headID)
```

### Distributed (N Nodes)
```
 Vector
   │
   ▼
 RNGSelection(vector) ──→ headID         ← Driver only
   │
   ▼
 owner = hash(headID) % N                ← deterministic
   │
   ├── owner == local ──→ Append(headID, vector)        [same as single machine]
   │                        │
   │                        ├──→ SplitAsync(headID)
   │                        │      └─ new headIDs: Put locally (no contention yet)
   │                        │      └─ BroadcastHeadSync(Add/Delete) → all peers
   │                        │
   │                        ├──→ MergeAsync(headID)
   │                        │      └─ neighbor owner == local?
   │                        │           Yes → lock locally, merge
   │                        │           No  → SendRemoteLock(neighbor) → try_lock
   │                        │                  success → merge, unlock remote
   │                        │                  fail    → retry via MergeAsync
   │                        │      └─ BroadcastHeadSync(Delete) → all peers
   │                        │
   │                        └──→ ReassignAsync(vector)
   │                               └─ new headID = RNGSelection(vector)
   │                               └─ new owner = hash(new headID) % N
   │                               └─ route to new owner (local or remote Append)
   │
   └── owner == remote ──→ QueueRemoteAppend(owner, headID, vector)
                             └─ buffered, sent as BatchAppend RPC
                             └─ remote node does Append + Split/Merge locally
```

---

## Slide 4: Implementation ① — Hash Routing (Owner Assignment)

```cpp
// PostingRouter::GetOwner — O(1), no network
int GetOwner(SizeType headID) {
    return headID % m_numNodes;   // deterministic hash
}
```

**Properties:**
- Same headID → always same owner → local lock is safe
- Uniform distribution across nodes
- Unaffected by TiKV Region splits / compaction / leader transfer
- Trade-off: no data locality awareness (acceptable on same machine)

**Where GetOwner is called:**
| Operation | Call site | Purpose |
|-----------|----------|---------|
| Append    | `AddIndex()` | Route vector write to owner |
| Split     | Implicit (runs on owner) | Owner already holds lock |
| Merge     | `MergePostings()` | Check if neighbor is local/remote |
| Reassign  | `ReassignAsync()` | Route re-assigned vector to new owner |

---

## Slide 5: Implementation ② — Append Write Path

**Local Append** (owner == me):
```
 Driver: Append(headID, vector)
   └─ lock(headID)
   └─ posting = TiKV.Get(headID)
   └─ posting.append(vector)
   └─ TiKV.Put(headID, posting)
   └─ check split/merge thresholds
   └─ unlock(headID)
```

**Remote Append** (owner != me):
```
 Driver:
   └─ QueueRemoteAppend(owner, headID, vector)  ← buffer locally
   └─ ... more vectors ...
   └─ FlushRemoteAppends()                      ← batch send

 Network: BatchAppend RPC ──→ owner node

 Worker (owner):
   └─ for each (headID, vector) in batch:
       └─ Append(headID, vector)     ← same as local path
       └─ triggers Split/Merge if needed
```

**Batching**: vectors are buffered per-owner, flushed once after all RNGSelection completes for the insert batch. Reduces RPC count from O(vectors) to O(nodes).

---

## Slide 6: Implementation ③ — Split / Merge / Reassign Routing

### Split (always local)
```
 Owner node: SplitAsync(headID)
   └─ lock(headID)
   └─ posting = TiKV.Get(headID)
   └─ K-means(k=2) on posting vectors
   └─ headID₁ = existing VID from cluster 1
   └─ headID₂ = existing VID from cluster 2
   └─ TiKV.Put(headID₁, posting₁)
   └─ TiKV.Put(headID₂, posting₂)
   └─ TiKV.Delete(headID)
   └─ Head Index: Add(headID₁), Add(headID₂), Delete(headID)
   └─ BroadcastHeadSync({Add headID₁, Add headID₂, Delete headID})
   └─ unlock(headID)
```
- Split runs on the owner of headID (who holds the lock)
- New postings Put locally — no contention on brand-new headIDs
- HeadSync broadcast updates all peers' head indexes

### Merge (may need cross-node lock)
```
 Owner of headID_small: MergeAsync(headID_small)
   └─ neighbor = HeadIndex.Search(headID_small)  ← nearest head
   └─ neighbor_owner = hash(neighbor) % N
   │
   ├─ neighbor_owner == me:
   │    └─ lock(headID_small), lock(neighbor)
   │    └─ merge postings
   │    └─ unlock both
   │
   └─ neighbor_owner != me:
        └─ SendRemoteLock(neighbor_owner, neighbor)  ← RPC try_lock
             ├─ success: merge, SendRemoteUnlock, BroadcastHeadSync
             └─ fail:    enqueue MergeAsync for retry (no deadlock)
```
- **Deadlock prevention**: `try_lock` is non-blocking. On failure, retry asynchronously.
- Cross-node Merge lock is the **only** distributed lock in the system.

### Reassign (route to new owner)
```
 Owner of old_headID: ReassignAsync(vector)
   └─ new_headID = RNGSelection(vector)      ← fresh search
   └─ new_owner = hash(new_headID) % N
   ├─ new_owner == me: Append(new_headID, vector)
   └─ new_owner != me: QueueRemoteAppend(new_owner, new_headID, vector)
                        FlushRemoteAppends()
```

---

## Slide 7: Implementation ④ — HeadSync (Consistency Broadcast)

**Problem**: After Split/Merge, head index changes. Other nodes have stale head indexes.

**Solution**: Synchronous TCP broadcast after every Split/Merge.

```
 Node 0 (Split完成):
   └─ BroadcastHeadSync([
        {op: Add,    headVID: 1001, headVector: [...]},
        {op: Add,    headVID: 1002, headVector: [...]},
        {op: Delete, headVID: 42}
      ])
        │
        ├──→ Node 1: HeadIndex.Add(1001), Add(1002), Delete(42)
        └──→ Node 2: HeadIndex.Add(1001), Add(1002), Delete(42)
```

**Timing**: HeadSync runs inside SplitAsync/MergeAsync (background threads).
Not blocking the insert path. Other nodes' RNGSelection uses the stale head  
index until sync arrives — vectors may route to deleted headID, handled by  
`ContainSample` check → ReassignAsync.

---

## Slide 8: 3-Node Sequence Diagram — Insert 1 Vector

```
  Driver(N0)              Worker(N1)             Worker(N2)
     │                       │                       │
     │  vector arrives       │                       │
     │                       │                       │
     ├─ RNGSelection(vec)    │                       │
     │  → headID = 42        │                       │
     │                       │                       │
     ├─ owner = 42%3 = 0     │                       │
     │  (local!)             │                       │
     │                       │                       │
     ├─ lock(42)             │                       │
     ├─ Append(42, vec)      │                       │
     │  → TiKV.Put(42)      │                       │
     │                       │                       │
     ├─ posting > limit!     │                       │
     ├─ SplitAsync(42)       │                       │
     │  K-means → 1001,1002  │                       │
     │  Put(1001), Put(1002) │                       │
     │  Delete(42)           │                       │
     │                       │                       │
     ├─ BroadcastHeadSync ──►├─ Add(1001,1002)       │
     │                  ────►│  Delete(42)            │
     │                       │        ──────────────►├─ Add(1001,1002)
     │                       │                       │  Delete(42)
     ├─ unlock(42)           │                       │
     │                       │                       │
     │  next vector          │                       │
     ├─ RNGSelection(vec2)   │                       │
     │  → headID = 1001      │                       │
     ├─ owner = 1001%3 = 2   │                       │
     │  (remote! → N2)       │                       │
     ├─ QueueRemote(N2,1001) │                       │
     │  ...                  │                       │
     ├─ FlushRemoteAppends() │                       │
     │  ──── BatchAppend RPC ───────────────────────►│
     │                       │                       ├─ Append(1001, vec2)
     │                       │                       │  → TiKV.Put(1001)
     │◄──── Response ────────────────────────────────┤
     │                       │                       │
```

---

## Slide 9: Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| What to distribute | IO (Append/Split/Merge) | IO is bottleneck, not RNG (CPU) |
| Routing function | `hash(headID) % N` | Deterministic, zero lookup, uniform |
| Locking | Local per-headID | Same headID → same node always |
| Cross-node lock | Only for Merge neighbor | try_lock + async retry = no deadlock |
| HeadSync | Synchronous broadcast | Simple, correct. Stale reads handled by ContainSample → Reassign |
| Split on new headID | Local Put (no route) | No contention on brand-new key |
| Batch RPC | Buffer per-owner, flush once | O(nodes) RPCs instead of O(vectors) |

---

## Slide 10: Network Protocol

**5 RPC Types** (TCP, custom binary protocol):

| Packet Type | Direction | Purpose |
|------------|-----------|---------|
| `BatchAppendRequest` | Driver → Worker | Send buffered appends |
| `BatchAppendResponse` | Worker → Driver | Acknowledge completion |
| `HeadSyncRequest` | Any → All peers | Broadcast head index changes |
| `RemoteLockRequest` | Node → Node | try_lock / unlock for Merge |
| `RemoteLockResponse` | Node → Node | Lock success/failure |

**Connection**: Each node maintains persistent TCP connections to all other nodes.  
Worker nodes start a TCP server; driver connects to all workers at startup.

---

## Slide 11: Configuration

```ini
[BuildSSDIndex]
RouterEnabled=true
RouterLocalNodeIndex=0          # this node's index (0, 1, 2, ...)
RouterNodeAddrs=                # TCP addresses for routing RPCs
  127.0.0.1:30001,              #   node 0
  127.0.0.1:30002,              #   node 1
  127.0.0.1:30003               #   node 2
RouterNodeStores=               # TiKV store addresses (for sub-partitioning)
  127.0.0.1:20161,
  127.0.0.1:20162,
  127.0.0.1:20163
```

---

## Slide 12: 100K Benchmark (Validation)

| Config | Steady-State | Routing Distribution | Recall@5 |
|--------|-------------|---------------------|----------|
| 1 node | 94.4 vps | — | 0.782 |
| 2 node | 80.5 vps | local 48% / remote 52% | 0.782 |
| 3 node | 78.4 vps | local 33% / remote 67% | 0.786 |

- ✅ Routing works correctly (verified by per-batch stats)
- ✅ Recall maintained across all configurations
- ❌ No throughput scaling at 100K (100 vectors/batch too small, shared TiKV on localhost)
- Next: 1M / 10M scale where IO workload is large enough for parallelism to help
