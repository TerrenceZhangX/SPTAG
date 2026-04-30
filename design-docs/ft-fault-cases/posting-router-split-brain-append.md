# FT fault case: posting-router-split-brain-append

**Status:** repro-green (Tier 1 PASS env-off + env-armed; Tier 2 deferred — no hot-path impact)
**Branch:** `fault/posting-router-split-brain-append`
**Closure date:** 2026-04-30 UTC

## Scenario

During a brief membership-transition window two compute nodes `N1`
and `N2` both believe they own the same `HeadID H`:

1. Old owner `N1` has not yet learned about the re-assignment.
2. New owner `N2` has just been promoted via ring update.
3. Both accept `Append(H, vid)` requests in the overlap window.

Without fencing, both nodes can drive split / merge against `H`
concurrently → posting state corruption.

## Invariants (must hold under the fault)

1. **Local lock + ring-epoch fence.** A stale-epoch acquirer on the
   same node is rejected before it ever reaches the send path.
2. **Ring-epoch fence on RPC ingress.** A receiver whose ring epoch
   has advanced past the sender's claim rejects the inbound RPC
   with `StaleRingEpoch` (existing prim/ring-epoch-fence path).
3. **Receiver dedup → single truth.** When two senders' RPCs land
   on the same physical owner, the receiver `OpIdCache` collapses
   the duplicate; the persisted set has exactly one entry per
   logical op (existing prim/op-id-idempotency path).

## Env gate

`SPTAG_FAULT_POSTING_ROUTER_SPLIT_BRAIN_APPEND`

When unset, `SendRemoteAppend` / `SendBatchRemoteAppend` retain
their pre-fault control flow byte-for-byte. The wrapper
(`RemotePostingOps::SendRemoteAppendWithSplitBrainGuard`) is opt-in
and only invoked by tests and (in future production wiring) by a
caller that consults `RemotePostingOps::SplitBrainGuardArmed()`.

## Counters

Surfaced via `RemotePostingOps::GetSplitBrainCounters()`:

| counter             | semantics                                                                |
| ------------------- | ------------------------------------------------------------------------ |
| `localLockAcquired` | local-lock slot acquired for a HeadID (entry into the guarded region)    |
| `localLockContended`| concurrent admittance was observed for the same HeadID                   |
| `fenced`            | wrapper rejected via local-lock-stale or receiver `StaleRingEpoch`       |
| `dedupCollapsed`    | wrapper observed a retry success after a fence — receiver dedup hit      |

Receiver-side dedup hits that propagate through the wider RPC path
continue to land on the existing
`RemotePostingOps::FenceCounters::dedupReplays` counter.

## Implementation

- `SendRemoteAppendWithSplitBrainGuard` (templated): non-blocking
  acquire of a per-HeadID local-lock slot keyed on ring-epoch. A
  strictly-higher-epoch holder fences a stale acquirer at slot
  acquire time without entering the send path. Equal-or-newer
  epoch concurrent admittance is permitted (single-truth is then
  guaranteed by receiver dedup). On send failure (StaleRingEpoch /
  RingNotReady / Fail), the wrapper retries with the **same** OpId
  and a freshly-resolved owner so receiver dedup can collapse.
- Production hot path is unchanged when the env is unset; the
  wrapper is dormant.
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
  5 test cases out of <total> passed
*** No errors detected
```

| case                                                  | invariant covered                                       | result |
| ----------------------------------------------------- | ------------------------------------------------------- | ------ |
| `SplitBrainArmedFenceAndDedupConvergeToSingleTruth`   | (1)+(2)+(3): fence stale sender, dedup converges truth  | PASS   |
| `SplitBrainEnvOffIsDormant`                           | hot path untouched on baseline                          | PASS   |
| `SplitBrainLocalLockFencesStaleEpochAcquirer`         | (1): same-node stale-epoch fenced before send           | PASS   |
| `SplitBrainOpIdStableAcrossRetries`                   | (3): same OpId across attempts                          | PASS   |
| `TiKVHarnessSmokeWriteRead` (TIKV)                    | harness PD+TiKV reachable (smoke)                       | PASS   |

## Tier 2 (SOFT gate / informational): deferred

Not on the perf-validation hot-path strict list. Production hot
path is byte-identical to the prim merge when the env is unset.
Closure under `repro-green`, mirroring the sibling
`posting-router-owner-failover`. STATUS in
`results/posting-router-split-brain-append/1M/STATUS.md`.

## Twin relationship

Twin to `posting-router-owner-failover`. The owner-failover case
covers the steady-state retry path when ownership has fully moved;
this case covers the brief overlap window where two nodes both
believe they own the HeadID. Both share the same primitive deps
(`prim/op-id-idempotency` + `prim/ring-epoch-fence`); each adds a
small wrapper on top. Production wiring (when added) should pick
the guard based on the membership signal: while ring epoch is in
flux, route through the split-brain guard; once stable,
owner-failover is sufficient.
