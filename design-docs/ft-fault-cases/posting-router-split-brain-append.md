# FT fault case: posting-router-split-brain-append

**Status:** repro-green (Tier 1 PASS env-off + env-armed; Tier 2 deferred — no hot-path impact)
**Branch:** `fault/posting-router-split-brain-append`
**Closure date:** 2026-04-30 UTC

## Scenario

During a brief membership transition window, two compute nodes
N1 and N2 both believe they own the same HeadID `H`:
1. Old owner N1 has not yet learned about the re-assignment.
2. New owner N2 has just been promoted via ring update.
3. Both accept `Append(H, vid)` requests during the window.

The window is bounded by ring-update propagation; the case is
twin to `posting-router-owner-failover`, which covers the
*steady-state* retry path (in-flight RemoteAppend re-routed to
the new owner). This case covers the *brief-window correctness
invariant* (single truth across the overlap).

## Invariants (must hold under the fault)

1. **Local lock + ring-version fencing.** Each
   `RemotePostingOps` instance gates Append on a per-HeadID
   slot stamped with its own ring-epoch. A second acquirer
   with a STALE epoch is fenced locally with
   `StaleRingEpoch` (no send is issued).
2. **Ring-epoch fence on RPC ingress.** When the wrapper
   does ship the RPC, an out-of-date ring epoch is rejected
   by the receiver via the existing
   `prim/ring-epoch-fence` path (counted on the sender
   surface as `splitBrainFenced`).
3. **Receiver dedup → single truth.** When both N1 and N2
   manage to land RPCs on the same physical owner, the
   receiver-side OpIdCache collapses duplicates; only one
   logical record is persisted. Wrapper retries with the
   SAME OpId so dedup observes them as duplicates of one
   logical operation, not as distinct ops.

## Env gate

`SPTAG_FAULT_POSTING_ROUTER_SPLIT_BRAIN_APPEND`

When unset, `SendRemoteAppend` and `SendBatchRemoteAppend`
retain their pre-fault control flow byte-for-byte. The
split-brain wrapper
(`RemotePostingOps::SendRemoteAppendWithSplitBrainGuard`) is
opt-in and only invoked by tests and (in future production
wiring) by a caller that consults
`RemotePostingOps::SplitBrainGuardArmed()`.

## Counters

Surfaced via `RemotePostingOps::GetSplitBrainCounters()`:

| counter             | semantics                                                              |
| ------------------- | ---------------------------------------------------------------------- |
| `localLockAcquired` | wrapper successfully entered the local-lock slot for a headID          |
| `localLockContended`| another acquirer was already active for the same headID                |
| `fenced`            | sender saw `StaleRingEpoch` (local-lock fence OR receiver fence reply) |
| `dedupCollapsed`    | wrapper retry succeeded after a fence — receiver collapsed via OpId    |

Receiver-side dedup hits also land on the existing
`RemotePostingOps::FenceCounters::dedupReplays` counter.

## Implementation

- `SendRemoteAppendWithSplitBrainGuard` (templated):
  performs a non-blocking try-acquire on a per-HeadID slot.
  Blocking is forbidden so a stale-epoch acquirer is fenced
  rather than queued behind the legitimate holder. After
  admission, allocates a single OpId and runs the same
  retry loop the owner-failover wrapper uses, reusing the
  SAME OpId across attempts so receiver dedup collapses
  duplicates.
- Production hot path is unchanged when the env is unset;
  the wrapper is dormant.
- `OpId.restartEpoch` is preserved across retries so receiver
  dedup collapses duplicates without confusing them with
  post-restart ops.

## Tier 1 evidence (HARD gate, both PASS)

Logs:
- env-off: `results/posting-router-split-brain-append/1M/posting-router-split-brain-append_t1_envoff.log`
- env-armed: `results/posting-router-split-brain-append/1M/posting-router-split-brain-append_t1_envarmed.log`

Both runs report:

```
Test module "Main" has passed with:
  5 test cases out of 211 passed
  29 assertions out of 29 passed
```

| case                                                | invariant covered                              | result |
| --------------------------------------------------- | ---------------------------------------------- | ------ |
| `SplitBrainArmedFenceAndDedupConvergeToSingleTruth` | (2) ring-epoch fence + (3) dedup single truth  | PASS   |
| `SplitBrainEnvOffIsDormant`                         | hot path untouched on baseline                 | PASS   |
| `SplitBrainLocalLockFencesStaleEpochAcquirer`       | (1) local-lock fence on stale-epoch acquirer   | PASS   |
| `SplitBrainOpIdStableAcrossRetries`                 | (3) OpId stability across wrapper retries      | PASS   |
| `TiKVHarnessSmokeWriteRead` (TIKV)                  | harness PD+TiKV reachable (smoke)              | PASS   |

## Tier 2 (SOFT gate / informational): deferred

This case is NOT on the perf-validation-protocol hot-path
strict list. The production hot path is byte-identical to
the merge of the two primitive branches when
`SPTAG_FAULT_POSTING_ROUTER_SPLIT_BRAIN_APPEND` is unset
(the wrapper is not wired into any production call site by
this branch). Therefore no measurable perf delta is plausible
from the fault-case code itself.

Closure proceeds under `repro-green` status, mirroring
`posting-router-owner-failover`. Rationale recorded in
`results/posting-router-split-brain-append/1M/STATUS.md`.

## Sibling / twin

This case is the brief-window correctness twin of
`posting-router-owner-failover`:

- `owner-failover` covers steady-state retry to the *new*
  owner after a ring rev bump on a single sender.
- `split-brain-append` (this case) covers two-sender brief
  window where both senders independently believe they own
  the head; the local-lock + epoch-fence + dedup interlock
  collapses to a single truth.

The wrapper here reuses the same OpId-stable retry shape as
`SendRemoteAppendWithFailover` so the two paths are
semantically composable when wired into production.
