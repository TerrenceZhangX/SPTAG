#!/usr/bin/env bash
# collect_component_metrics.sh — gather distributed-benchmark component metrics
# and merge them into the bench JSON produced by run_distributed.sh.
#
# Usage:
#   collect_component_metrics.sh <cluster.conf> <bench.json> <log_dir> [duration_sec]
#
# Inputs:
#   cluster.conf   Same file consumed by run_distributed.sh.
#                  We parse [cluster], [nodes], [tikv] for ssh_user/key,
#                  compute hosts, and pd_client_port.
#   bench.json     The JSON file emitted by SPFreshTest BenchmarkFromConfig
#                  (e.g. results/output_<scale>_<N>node.json). We *merge in*
#                  a new top-level object "component_metrics".
#   log_dir        Directory containing benchmark_*_driver.log /
#                  benchmark_*_workerN_remote.log produced by run_distributed.sh.
#   duration_sec   (Optional) Wall clock duration of the run, used to
#                  convert log-derived counts into per-second rates.
#                  Falls back to bench.json wall_time / total_runtime_sec
#                  if available, else 1.0.
#
# Outputs (merged under bench.json -> "component_metrics"):
#   posting_router_rps              { mean, p50, p99 }
#   headsync_writes_per_sec         number
#   tikv_region_count               integer (single sample, end of run)
#   tikv_region_error_retries       integer (count from logs)
#   pd_heartbeat_rate               number  (heartbeats/sec, sampled)
#   per_node_cpu_pct                { mean, max }
#   per_node_rss_mb                 { mean, max }
#   per_node_net_in_mb_s            { mean, max }
#   per_node_net_out_mb_s           { mean, max }
#
# Design constraints:
#   - Read-only with respect to the cluster (sampling APIs only).
#   - Independent from run_distributed.sh control flow; safe to run post-hoc.
#   - Best-effort: any field that cannot be collected is set to null with a
#     short reason in component_metrics.collection_warnings[].
#   - No bash -e: we explicitly check exit codes per-section so a single
#     missing PD endpoint or unreachable host does not abort the whole script.
#
# Required tools on driver:  bash, curl, ssh, awk, sed, grep, python3.
# Required tools on each node: top OR /proc/stat, /proc/meminfo, /proc/net/dev.

set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CONF="${1:-}"
BENCH_JSON="${2:-}"
LOG_DIR="${3:-}"
DURATION_SEC="${4:-}"

if [ -z "$CONF" ] || [ -z "$BENCH_JSON" ] || [ -z "$LOG_DIR" ]; then
    echo "Usage: $0 <cluster.conf> <bench.json> <log_dir> [duration_sec]" >&2
    exit 2
fi
[ -f "$CONF" ]       || { echo "ERROR: cluster.conf not found: $CONF"  >&2; exit 2; }
[ -f "$BENCH_JSON" ] || { echo "ERROR: bench.json not found: $BENCH_JSON" >&2; exit 2; }
[ -d "$LOG_DIR" ]    || { echo "ERROR: log_dir not found: $LOG_DIR"    >&2; exit 2; }

# ─── Parse cluster.conf (subset of run_distributed.sh logic) ────────────────
SSH_USER=""
SSH_KEY=""
declare -a NODE_HOSTS=()
declare -a TIKV_HOSTS=()
declare -a TIKV_PD_CLIENT_PORTS=()

SECTION=""
while IFS= read -r line || [ -n "$line" ]; do
    line="${line%%#*}"
    line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [ -z "$line" ] && continue
    if [[ "$line" =~ ^\[(.+)\]$ ]]; then
        SECTION="${BASH_REMATCH[1]}"
        continue
    fi
    case "$SECTION" in
        cluster)
            k="${line%%=*}"; v="${line#*=}"
            [ "$k" = "ssh_user" ] && SSH_USER="$v"
            [ "$k" = "ssh_key" ]  && SSH_KEY="${v/#\~/$HOME}"
            ;;
        nodes)
            read -r host _rport <<< "$line"
            [ -n "$host" ] && NODE_HOSTS+=("$host")
            ;;
        tikv)
            read -r host pdc _pdp _kvp <<< "$line"
            [ -n "$host" ] && TIKV_HOSTS+=("$host") && TIKV_PD_CLIENT_PORTS+=("$pdc")
            ;;
    esac
done < "$CONF"

SSH_USER="${SSH_USER:-$(whoami)}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes"
[ -n "$SSH_KEY" ] && SSH_OPTS="$SSH_OPTS -i $SSH_KEY"

declare -a WARN=()
warn() { WARN+=("$1"); echo "  WARN: $1" >&2; }

# ─── Resolve duration ───────────────────────────────────────────────────────
if [ -z "$DURATION_SEC" ]; then
    DURATION_SEC=$(python3 - "$BENCH_JSON" <<'PY' 2>/dev/null || echo ""
import json, sys
d = json.load(open(sys.argv[1]))
for k in ("total_runtime_sec","wall_time_sec","wall_time","duration_sec"):
    v = d.get(k)
    if isinstance(v,(int,float)) and v>0:
        print(v); break
PY
)
fi
[ -z "$DURATION_SEC" ] && { DURATION_SEC=1.0; warn "duration not found, using 1.0s"; }

# ─── 1. PostingRouter RPS (mean, p50, p99) ──────────────────────────────────
# Strategy: per-second router request counts are emitted by PostingRouter
# stats logs of the form:
#   [PostingRouter] rps=<float> p50=<ms> p99=<ms>
# We grep all driver+worker logs and aggregate. If absent, derive from
# AppendRequest counts in driver log over DURATION_SEC.
PR_RPS_JSON='null'
PR_VALUES=$(grep -hE "PostingRouter.*rps=" "$LOG_DIR"/benchmark_*_driver.log "$LOG_DIR"/benchmark_*_worker*_remote.log 2>/dev/null \
            | sed -nE 's/.*rps=([0-9.]+).*/\1/p')
if [ -n "$PR_VALUES" ]; then
    PR_RPS_JSON=$(python3 - <<PY
import statistics as s, json
vals = [float(x) for x in """$PR_VALUES""".split() if x]
vals.sort()
def pct(p):
    if not vals: return None
    i = max(0, min(len(vals)-1, int(round((p/100.0)*(len(vals)-1)))))
    return vals[i]
print(json.dumps({"mean": (sum(vals)/len(vals)) if vals else None,
                  "p50": pct(50), "p99": pct(99),
                  "samples": len(vals)}))
PY
)
else
    APPEND_COUNT=$(grep -chE "AppendRequest|BatchAppendRequest" "$LOG_DIR"/benchmark_*_driver.log 2>/dev/null | awk -F: '{s+=$NF} END{print s+0}')
    if [ "${APPEND_COUNT:-0}" -gt 0 ]; then
        PR_RPS_JSON=$(python3 -c "import json; r=$APPEND_COUNT/float($DURATION_SEC); print(json.dumps({'mean':r,'p50':r,'p99':None,'samples':1,'note':'derived from AppendRequest log count / duration'}))")
    else
        warn "posting_router_rps: no PostingRouter stats lines and no AppendRequest log entries"
    fi
fi

# ─── 2. HeadSync writes/sec ─────────────────────────────────────────────────
HEADSYNC_WPS='null'
HS_COUNT=$(grep -chE "HeadSyncRequest|BroadcastHeadSync|head[_ ]?sync.*entries=" "$LOG_DIR"/benchmark_*_driver.log "$LOG_DIR"/benchmark_*_worker*_remote.log 2>/dev/null \
           | awk -F: '{s+=$NF} END{print s+0}')
if [ "${HS_COUNT:-0}" -gt 0 ]; then
    HEADSYNC_WPS=$(python3 -c "print(${HS_COUNT}/float(${DURATION_SEC}))")
else
    warn "headsync_writes_per_sec: no HeadSync log lines found"
fi

# ─── 3. TiKV region count + 4. region-error retries + 5. PD heartbeat rate ──
TIKV_REGION_COUNT='null'
PD_HEARTBEAT_RATE='null'
PD0_HOST="${TIKV_HOSTS[0]:-}"
PD0_PORT="${TIKV_PD_CLIENT_PORTS[0]:-2379}"
if [ -n "$PD0_HOST" ]; then
    # Region count via PD HTTP API (equivalent to `pd-ctl region`).
    RC=$(curl -fsS --max-time 5 "http://${PD0_HOST}:${PD0_PORT}/pd/api/v1/regions" 2>/dev/null \
         | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',''))" 2>/dev/null)
    if [ -n "$RC" ]; then
        TIKV_REGION_COUNT="$RC"
    else
        warn "tikv_region_count: PD /pd/api/v1/regions unreachable at ${PD0_HOST}:${PD0_PORT}"
    fi
    # PD heartbeat rate: sample /metrics twice 2s apart, diff pd_scheduler_handle_region_heartbeat_duration_seconds_count
    M1=$(curl -fsS --max-time 5 "http://${PD0_HOST}:${PD0_PORT}/metrics" 2>/dev/null)
    sleep 2
    M2=$(curl -fsS --max-time 5 "http://${PD0_HOST}:${PD0_PORT}/metrics" 2>/dev/null)
    if [ -n "$M1" ] && [ -n "$M2" ]; then
        PD_HEARTBEAT_RATE=$(python3 - <<PY 2>/dev/null
import re,sys
def total(blob):
    s=0.0; found=False
    for line in blob.splitlines():
        if line.startswith("#"): continue
        if "region_heartbeat" in line and ("_count" in line or "_total" in line):
            m=re.search(r'(\S+)\s+([0-9.eE+-]+)\s*$', line)
            if m:
                try: s+=float(m.group(2)); found=True
                except: pass
    return s if found else None
a=total("""$M1"""); b=total("""$M2""")
print((b-a)/2.0 if (a is not None and b is not None) else "null")
PY
)
        [ -z "$PD_HEARTBEAT_RATE" ] && PD_HEARTBEAT_RATE='null'
    else
        warn "pd_heartbeat_rate: PD /metrics unreachable at ${PD0_HOST}:${PD0_PORT}"
    fi
else
    warn "no TiKV hosts in cluster.conf; skipping PD-derived metrics"
fi

# Region-error-induced retries: count from driver+worker logs.
RE_RETRIES=$(grep -chE "region.error|RegionError|EpochNotMatch|NotLeader.*retry" "$LOG_DIR"/benchmark_*_driver.log "$LOG_DIR"/benchmark_*_worker*_remote.log 2>/dev/null \
             | awk -F: '{s+=$NF} END{print s+0}')
TIKV_REGION_ERROR_RETRIES="${RE_RETRIES:-0}"

# ─── 6–9. Per-node CPU%, RSS MB, net in/out MB/s ────────────────────────────
# Sample twice (2s apart) on each compute node and compute deltas.
CPU_VALS=()
RSS_VALS=()
NETIN_VALS=()
NETOUT_VALS=()

sample_node() {
    local host="$1"
    # Returns: cpu_pct rss_mb net_in_mb_s net_out_mb_s   (or empty on failure)
    ssh $SSH_OPTS "$SSH_USER@$host" 'bash -s' <<'REMOTE' 2>/dev/null
set -u
read_stat() { awk '/^cpu /{u=$2+$3+$4+$7+$8; t=$2+$3+$4+$5+$6+$7+$8+$9; print u" "t}' /proc/stat; }
read_net() { awk 'BEGIN{rx=0;tx=0} /:/ && $1!="lo:" {gsub(":","",$1); rx+=$2; tx+=$10} END{print rx" "tx}' /proc/net/dev; }
read_rss() { awk '/MemTotal:/{t=$2}/MemAvailable:/{a=$2} END{print (t-a)/1024}' /proc/meminfo; }
read S1 < <(read_stat); read N1 < <(read_net)
sleep 2
read S2 < <(read_stat); read N2 < <(read_net)
RSS=$(read_rss)
awk -v s1="$S1" -v s2="$S2" -v n1="$N1" -v n2="$N2" -v rss="$RSS" '
BEGIN {
    split(s1,a," "); split(s2,b," "); split(n1,c," "); split(n2,d," ");
    du = b[1]-a[1]; dt = b[2]-a[2]; cpu = (dt>0) ? (100.0*du/dt) : 0;
    rxbps = (d[1]-c[1])/2.0; txbps = (d[2]-c[2])/2.0;
    printf("%.2f %.1f %.3f %.3f\n", cpu, rss, rxbps/1048576.0, txbps/1048576.0);
}'
REMOTE
}

for host in "${NODE_HOSTS[@]}"; do
    OUT=$(sample_node "$host")
    if [ -n "$OUT" ]; then
        # shellcheck disable=SC2206
        arr=($OUT)
        CPU_VALS+=("${arr[0]}")
        RSS_VALS+=("${arr[1]}")
        NETIN_VALS+=("${arr[2]}")
        NETOUT_VALS+=("${arr[3]}")
    else
        warn "per-node sample failed for $host"
    fi
done

agg() {
    # mean+max JSON for a space-separated list of floats; "null" if empty
    local vals="$1"
    if [ -z "$vals" ]; then echo 'null'; return; fi
    python3 -c "
import json, sys
vs=[float(x) for x in '''$vals'''.split() if x]
print(json.dumps({'mean': sum(vs)/len(vs) if vs else None, 'max': max(vs) if vs else None, 'samples': len(vs)}))
"
}

CPU_J=$(agg "${CPU_VALS[*]:-}")
RSS_J=$(agg "${RSS_VALS[*]:-}")
NIN_J=$(agg "${NETIN_VALS[*]:-}")
NOUT_J=$(agg "${NETOUT_VALS[*]:-}")

# ─── Merge into bench JSON ──────────────────────────────────────────────────
WARN_JSON=$(python3 -c "import json,sys; print(json.dumps(sys.argv[1:]))" "${WARN[@]:-}")

python3 - "$BENCH_JSON" <<PY
import json, sys, os, time
path = sys.argv[1]
try:
    with open(path) as f: doc = json.load(f)
except Exception:
    doc = {}
doc["component_metrics"] = {
    "schema_version": 1,
    "collected_at_unix": int(time.time()),
    "duration_sec": float($DURATION_SEC),
    "posting_router_rps":         json.loads('''$PR_RPS_JSON''') if '''$PR_RPS_JSON''' != 'null' else None,
    "headsync_writes_per_sec":    None if '''$HEADSYNC_WPS'''=='null' else float('''$HEADSYNC_WPS'''),
    "tikv_region_count":          None if '''$TIKV_REGION_COUNT'''=='null' or '''$TIKV_REGION_COUNT'''=='' else int('''$TIKV_REGION_COUNT'''),
    "tikv_region_error_retries":  int('''$TIKV_REGION_ERROR_RETRIES'''),
    "pd_heartbeat_rate":          None if '''$PD_HEARTBEAT_RATE'''=='null' else float('''$PD_HEARTBEAT_RATE'''),
    "per_node_cpu_pct":           json.loads('''$CPU_J'''),
    "per_node_rss_mb":            json.loads('''$RSS_J'''),
    "per_node_net_in_mb_s":       json.loads('''$NIN_J'''),
    "per_node_net_out_mb_s":      json.loads('''$NOUT_J'''),
    "collection_warnings":        json.loads('''$WARN_JSON''') or [],
}
tmp = path + ".tmp"
with open(tmp, "w") as f: json.dump(doc, f, indent=2)
os.replace(tmp, path)
print(f"Merged component_metrics into {path}")
PY
