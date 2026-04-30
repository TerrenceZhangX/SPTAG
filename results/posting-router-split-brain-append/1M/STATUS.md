# results/posting-router-split-brain-append/1M — Tier 2 STATUS

**Verdict:** deferred (no hot-path impact)
**Date:** 2026-04-30 UTC
**Branch:** `fault/posting-router-split-brain-append`

## Rationale

This case is not on the
`notes/perf-validation-protocol.md` hot-path strict list. The
production `SendRemoteAppend` / `SendBatchRemoteAppend` hot path is
byte-identical to the merge of `origin/prim/op-id-idempotency` and
`origin/prim/ring-epoch-fence` when
`SPTAG_FAULT_POSTING_ROUTER_SPLIT_BRAIN_APPEND` is unset. The
split-brain guard wrapper (`SendRemoteAppendWithSplitBrainGuard`)
is opt-in, not wired into any production call site by this branch,
and therefore cannot introduce a measurable baseline regression.

The Tier 1 hard gate (both env-off and env-armed) is green; closure
proceeds under the protocol's `repro-green` status, mirroring the
twin `posting-router-owner-failover` precedent.

If a future change wires the wrapper into the dispatcher's
production append loop, that change must re-run Tier 2 with a fresh
1M triple.
