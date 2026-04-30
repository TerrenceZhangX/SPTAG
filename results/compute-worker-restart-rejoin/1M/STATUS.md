# Tier 2 — 1M perf triple — DEFERRED

> Status: deferred per `notes/perf-validation-protocol.md` (locked
> 2026-04-30 06:02 UTC).

## Rationale

`compute-worker-restart-rejoin` is **not** on the hot-path strict
list (`retry-budget`, `coprocessor-error`,
`tikv-region-error-retry-cap`, `tikv-grpc-timeout-storm`,
`pd-reconnect-during-retry`). The change introduced by this case is
header-only, additive, and does not touch the synchronous RPC retry
hot path:

* `prim/swim-membership` adds a header-only failure detector. It is
  not yet wired into `PostingRouter` / `TiKVIO` (the SwimDetector
  observer factory `MakeRingMembershipObserver` is referenced from
  the rejoin test but not instantiated on the hot path).
* `prim/ring-epoch-fence` adds the (epoch, ringRev) tag + the
  `CompareRingEpoch` fence; receivers pay one branch on each routed
  RPC. The fence sits *outside* the TiKV / gRPC retry loop, so any
  perf shift would be measurable only at the inter-worker
  Append/Search edge, which the 1M sift INI already heavily caches.
* No new lock, no new allocation, no new gRPC call.

Per protocol, Tier 2 for non-hot-path cases is **soft / informational**
— `INFO` (within bounds) or `REVIEW-FLAG` (≥20 % swing) verdicts do
not block closure, and the case may close on Tier 1 alone.

## What would trigger us to capture Tier 2 anyway

If a follow-up wires `SwimDetector` and `RingEpoch` into the actual
`PostingRouter::DispatchAppend` / `WorkerNode::OnAppendRequest` call
sites (Wave-2 "make rejoin a first-class observer"), capture the
1M triple at that point because the change becomes hot-path-adjacent
and the soft gate is the cheapest way to flag accidental noise from
the new branches.

## Closure status

Closed on Tier 1 only — see closure note in
`design-docs/ft-fault-cases/compute-worker-restart-rejoin.md`.
