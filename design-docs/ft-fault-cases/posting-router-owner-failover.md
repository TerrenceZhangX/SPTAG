# FT fault case: posting-router-owner-failover

**Status:** repro-green (Tier 1 PASS env-off + env-armed; Tier 2 deferred — no hot-path impact)
**Branch:** `fault/posting-router-owner-failover`
**Closure date:** 2026-04-30 UTC

## Scenario

The owner of a HeadID changes (consistent-hash ring rev bump and/or
SWIM marks the previous owner dead) while a `RemoteAppend` is in
flight from the dispatcher to the previous owner.

## Invariants (must hold under the fault)

1. **Retry to new owner.** The in-flight append is retried and
   routed to the *new* owner, picked up via the post-bump ring view.
2. **Single truth.** AtLeastOnce send + receiver-side OpId dedup =>
   no duplicate persisted record exposed; the same logical op
   appears once.
3. **Stale-epoch fence.** A retry that lands on the *old* owner
   after its ring rev has advanced is rejected with
   `StaleRingEpoch`; the old owner cannot commit a split-brain
   append.

## Env gate

`SPTAG_FAULT_POSTING_ROUTER_OWNER_FAILOVER`

When unset, `SendRemoteAppend` and `SendBatchRemoteAppend` retain
their pre-fault control flow byte-for-byte. The owner-failover
wrapper (`RemotePostingOps::SendRemoteAppendWithFailover`) is opt-in
and only invoked by tests and (in future production wiring) by a
caller that consults `RemotePostingOps::OwnerFailoverArmed()`.

## Counters

Surfaced via `RemotePostingOps::GetOwnerFailoverCounters()`:

| counter            | semantics                                                        |
| ------------------ | ---------------------------------------------------------------- |
| `retries`          | wrapper re-resolved owner and re-sent with the same OpId         |
| `staleEpochFenced` | sender saw `StaleRingEpoch` reply (old owner's fence fired)      |
| `dedupCollapsed`   | reserved; receiver-side collapse observable via `m_dedupReplays` |

Receiver-side dedup hits land on the existing
`RemotePostingOps::FenceCounters::dedupReplays` counter.

## Implementation

- `SendRemoteAppendWithFailover` (templated): allocates an OpId
  once, then loops up to `maxRetries` attempts. Each attempt:
  invoke caller-supplied `resolveOwner(headID)` then `sendFn(target,
  opId)`. On `StaleRingEpoch` / `Fail` / `RingNotReady`, bumps
  `retries` (and `staleEpochFenced` on `StaleRingEpoch`),
  re-resolves owner, retries with the same OpId.
- Production hot path is unchanged when the env is unset; the
  wrapper is dormant.
- `OpId.restartEpoch` is preserved across retries so receiver
  dedup collapses duplicates without confusing them with
  post-restart ops.

## Tier 1 evidence (HARD gate, both PASS)

Logs:
- env-off: `/tmp/posting-router-owner-failover_t1_envoff.log`
- env-armed: `/tmp/posting-router-owner-failover_t1_envarmed.log`

Both runs report:

```
Test module "Main" has passed with:
  5 test cases out of 211 passed
  35 assertions out of 35 passed
[harness] test exit=0
```

| case                                       | invariant covered                         | result |
| ------------------------------------------ | ----------------------------------------- | ------ |
| `OwnerFailoverArmedRetriesAndFences`       | (1), (2), (3): retry, single truth, fence | PASS   |
| `OwnerFailoverEnvOffIsDormant`             | hot path untouched on baseline            | PASS   |
| `OpIdIsStableAcrossOwnerFailoverRetries`   | (2): same OpId across attempts            | PASS   |
| `ReceiverDedupCollapsesDuplicateOpId`      | (2): receiver dedup collapse semantics    | PASS   |
| `TiKVHarnessSmokeWriteRead` (TIKV)         | harness PD+TiKV reachable (smoke)         | PASS   |

## Tier 2 (SOFT gate / informational): deferred

This case is NOT on the perf-validation-protocol hot-path strict
list. The production hot path is byte-identical to the merge of the
two primitive branches when
`SPTAG_FAULT_POSTING_ROUTER_OWNER_FAILOVER` is unset (the wrapper is
not wired into any production call site by this branch). Therefore
no measurable perf delta is plausible from the fault-case code
itself.

Per the protocol's `pd-store-discovery-stale` (I-pilot) precedent,
Tier 2 is deferred. Rationale recorded in
`results/posting-router-owner-failover/1M/STATUS.md`. Closure under
the new protocol is `repro-green`.
