# Implementation Pilot Report

## Primitive: OpIdIdempotency

**Branch:** `prim/op-id-idempotency`
**Commits:**
- `0ba1343` — `prim/op-id: OpId + OpIdCache primitive` (header-only types)
- `980e4b7` — `prim/op-id: wire OpId into RemoteAppend path with receiver dedup`
- `7d0294b` — `prim/op-id: add op_id_test.cpp + wire into Test/CMakeLists.txt`

**Build:** `make -j16 SPTAGTest` from `~/workspace/sptag-ft/build/op-id-idempotency`
with `-DTIKV=ON` (matches `_template`'s cache).

**Tests (all green, 5/5):**
```
Suite: OpIdIdempotencyTest
  AllocatorMintsMonotonicIds
  DedupSameIdReplaysCachedResult
  EvictionDropsLruTailWhenCapacityExceeded
  TtlExpiryAllowsReExecution
  RestartEpochInvalidatesSameCounterOldEpochEntries
*** No errors detected
```

### What got wired

Single canonical write path: `RemoteAppendRequest` / `HandleAppendRequest`
in `RemotePostingOps`.

1. **Wire format.** `RemoteAppendRequest` bumped to `MajorVersion = 2`, gained
   an embedded `Distributed::OpId m_opId` (16 bytes: `int32 senderId | uint32
   restartEpoch | uint64 monotonicCounter`) serialized via `OpId::Write/Read`
   right after the version prefix. `EstimateBufferSize` updated.
2. **Sender stamping.** `RemotePostingOps::SendRemoteAppend` now stamps
   `req.m_opId = m_opIdAllocator.Next()` whenever
   `m_opIdAllocator.SenderId() >= 0`. If `ConfigureIdempotency` was never
   called, the stamping is a no-op and the receiver path falls through to
   the legacy behavior.
3. **Receiver dedup.** `HandleAppendRequest` consults
   `m_appendDedupCache->Lookup(opId)` before invoking the append callback.
   On hit it replays the cached `Status` (encoded as `uint8_t`) without
   re-executing. On miss it executes the callback and inserts the resulting
   status into the cache.
4. **Restart-epoch handling.** A per-sender `m_lastSeenEpoch` map (guarded
   by a small mutex) tracks the highest epoch seen. When an incoming op
   carries an epoch greater than the previously-seen value, the cache calls
   `InvalidateOlderEpochs(senderId, newEpoch)` so a same-counter entry
   left over from before the restart cannot mask the new op.
5. **Configuration.** `RemotePostingOps::ConfigureIdempotency(senderId,
   epoch, capacity=4096, ttl=60s)` constructs the cache with the desired
   capacity/TTL and seeds the allocator. Test hooks
   (`AppendDedupCacheForTest`, `OpIdAllocatorForTest`) exposed for unit
   tests.

### Files touched

- `AnnService/inc/Core/SPANN/Distributed/OpId.h` — added `AppendDedupResult`
  POD used as the cached value type.
- `AnnService/inc/Core/SPANN/Distributed/DistributedProtocol.h` —
  versioned `RemoteAppendRequest` to v2, embedded `OpId` field.
- `AnnService/inc/Core/SPANN/Distributed/RemotePostingOps.h` — added
  idempotency state, stamp + dedup logic, configure entry point.
- `Test/Distributed/prim/op_id_test.cpp` — 5 unit tests (4 contract +
  1 allocator monotonicity).
- `Test/CMakeLists.txt` — added a `GLOB Test/Distributed/prim/*.cpp` rule
  alongside the existing `Test/src/*.cpp` glob (`chore/test-glob-prim-fault`
  was not yet on `main` at the time of this work, so the manual edit was
  still needed; once that lands, this glob can be reconciled).

### Deferred / out of scope

- `BatchRemoteAppendRequest` does NOT yet carry per-item OpIds. The wire
  protocol there is unchanged. Each batch item still re-runs through the
  callback. Wiring batch dedup is a follow-up: it requires deciding whether
  to allocate one OpId per item or one per batch and how the per-item
  status maps back into `BatchRemoteAppendResponse`. (Callers today retry
  by individual headID, so the missing batch dedup is degraded retry-cost,
  not a correctness gap.)
- No integration test that drives two real `RemotePostingOps` instances
  over loopback sockets with retry. The unit tests cover the cache contract
  and the allocator; the wiring (Write/Read symmetry, sender stamp, receiver
  Lookup/Insert) is covered by code review only. A loopback retry test is
  cheap to add later but was out of the 75-minute budget.
- `ConfigureIdempotency` is not yet called from `WorkerNode`. The plumbing
  is there; bootstrap code needs one line passing the local node's stable
  ID + a per-process epoch (e.g. wall-clock seconds at start). Without it
  every Append goes out with `senderId = -1` (`!IsValid()`), the receiver
  treats it as "not idempotency-tagged", and behavior is identical to
  before this primitive landed. This is the right default for staged
  rollout.
- Cache size / TTL tuning is left at conservative defaults (4096 entries,
  60 s TTL). Should be revisited under load once batch dedup lands.

### Notes for the next pilot

- The `Distributed::` namespace already contained `OpId`/`OpIdAllocator`/
  `OpIdCache`; the new `AppendDedupResult` lives there too. If the next
  protocol gains its own dedup cache (e.g. `RemoteLockRequest`), follow
  the same `XxxDedupResult` pattern rather than reusing `AppendDedupResult`.
- The receiver-side `m_lastSeenEpoch` map currently grows unbounded with
  the number of distinct sender IDs observed. For SPANN's fixed-cardinality
  cluster this is a non-issue; if/when senders churn, swap for a bounded
  LRU.
