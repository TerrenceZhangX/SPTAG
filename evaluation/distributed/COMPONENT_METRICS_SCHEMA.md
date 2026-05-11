# Component-Metrics Schema (v1)

Defines fields added to `evaluation/distributed/results/output_<scale>_<N>node.json`
under the new top-level `component_metrics` object. Populated by
`collect_component_metrics.sh` (post-run helper) and intended to be consumed by
the projection model in
`tasks/distributed-index-scale-2k/notes/projection/model.md`.

The benchmark itself (`Test/src/SPFreshTest.cpp::RunBenchmark`) is **not**
modified by this change. Component metrics are merged into the existing JSON
file post-run, so the schema can evolve without disturbing the hot benchmark
loop.

## Top-level field

```jsonc
"component_metrics": {
  "schema_version": 1,
  "collected_at_unix": 1714398000,
  "duration_sec": 612.5,

  "posting_router_rps":      { "mean": 0, "p50": 0, "p99": 0, "samples": 0 },
  "headsync_writes_per_sec": 0,
  "tikv_region_count":       0,
  "tikv_region_error_retries": 0,
  "pd_heartbeat_rate":       0,

  "per_node_cpu_pct":        { "mean": 0, "max": 0, "samples": 0 },
  "per_node_rss_mb":         { "mean": 0, "max": 0, "samples": 0 },
  "per_node_net_in_mb_s":    { "mean": 0, "max": 0, "samples": 0 },
  "per_node_net_out_mb_s":   { "mean": 0, "max": 0, "samples": 0 },

  "collection_warnings": []
}
```

Any individual field MAY be `null` if collection failed; in that case a short
human-readable reason is appended to `collection_warnings[]`.

## Field origins

| field | source | sampling |
|-------|--------|----------|
| `posting_router_rps` | `[PostingRouter] rps=… p50=… p99=…` lines emitted by the dispatcher / router (driver+worker logs). Falls back to `AppendRequest`/`BatchAppendRequest` log counts ÷ duration. | aggregate over run |
| `headsync_writes_per_sec` | `HeadSyncRequest` / `BroadcastHeadSync` log line count ÷ duration. | aggregate |
| `tikv_region_count` | `GET http://<pd0>/pd/api/v1/regions` (`.count`), equivalent to `pd-ctl region`. | one sample, end of run |
| `tikv_region_error_retries` | `RegionError` / `EpochNotMatch` / `NotLeader.*retry` log lines, driver+workers. | aggregate |
| `pd_heartbeat_rate` | PD `/metrics`, `pd_scheduler_handle_region_heartbeat_*_count` deltas across two samples 2 s apart. | rate at end of run |
| `per_node_cpu_pct` | `/proc/stat` CPU jiffy delta over 2 s, every compute node. | per-N sampling |
| `per_node_rss_mb` | `/proc/meminfo` `(MemTotal-MemAvailable)/1024`. | per-N sampling |
| `per_node_net_in/out_mb_s` | `/proc/net/dev` byte deltas over 2 s, summed across non-`lo` interfaces. | per-N sampling |

## Producing it

```bash
# After a run completes:
evaluation/distributed/collect_component_metrics.sh \
    cluster.conf \
    evaluation/distributed/results/output_10m_4node.json \
    evaluation/distributed/results/logs/ \
    [duration_sec]
```

The helper is **read-only** with respect to the cluster (HTTP GETs + SSH
`/proc` reads); it can safely be re-run on an archived results dir as long as
the PD endpoints and compute hosts are still reachable. If they are not, the
non-log fields (`tikv_region_count`, `pd_heartbeat_rate`, `per_node_*`) will
remain `null` while the log-derived fields still populate.

## Forward path

To make these metrics first-class (instead of post-merged), the future change
in `Test/src/SPFreshTest.cpp::RunBenchmark` would:

1. Add a `Json::Value componentMetrics` block written alongside
   the existing latency/recall stats.
2. Wire counters from
   - `AnnService/inc/Core/SPANN/Distributed/RemotePostingOps.h`
     (router send/recv counters, HeadSync broadcast counter), and
   - `WorkerNode` retry path (region-error counter).
3. Sample PD/TiKV from the driver via the same HTTP API as this script.

That refactor is intentionally **out of scope** for this PR: the helper
provides the same schema today without touching the benchmark hot path.
