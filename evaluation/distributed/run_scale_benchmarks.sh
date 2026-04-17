#!/bin/bash
# No set -e: worker exit codes are non-zero by design, must not abort script
#
# Usage:
#   ./run_scale_benchmarks.sh <scale> [scale2] [scale3] ...
#   ./run_scale_benchmarks.sh 100k
#   ./run_scale_benchmarks.sh 100k 1m
#   ./run_scale_benchmarks.sh 100k 1m 10m 100m
#   ./run_scale_benchmarks.sh all          # run all available scales

SPTAG_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$SPTAG_DIR"
SCRIPT_DIR="$SPTAG_DIR/evaluation/distributed"
BINARY=./Release/SPTAGTest
TIKV_DIR="$SPTAG_DIR/docker/tikv"
LOGDIR=benchmark_logs
IDXROOT="/mnt/nvme"
PERF_MONITOR=false
mkdir -p "$LOGDIR"

# Cleanup on script exit/interrupt to prevent orphan processes
cleanup_on_exit() {
    echo ""
    echo "Interrupted, cleaning up..."
    stop_perf_collectors
    stop_workers 5  # short timeout on interrupt
    kill -9 $(pgrep -f "SPTAGTest.*BenchmarkFromConfig") 2>/dev/null || true
    cd "$TIKV_DIR" && docker compose down 2>/dev/null
    exit 1
}
trap cleanup_on_exit INT TERM

WORKER_PIDS=()

# ─── Perf Monitor Helpers ───

PERF_COLLECTOR_PIDS=()
PERF_START_TIME=""
PERF_OUTDIR=""

start_perf_collectors() {
    # Usage: start_perf_collectors <phase_label>
    $PERF_MONITOR || return 0
    local LABEL=$1
    PERF_OUTDIR="$LOGDIR/perf_diag_${LABEL}_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$PERF_OUTDIR"
    PERF_START_TIME=$(date +%s)
    PERF_COLLECTOR_PIDS=()

    echo "[PERF] Starting collectors for phase: $LABEL -> $PERF_OUTDIR"

    # 1. iostat - per-device IO stats every 1s
    iostat -xdmt 1 > "$PERF_OUTDIR/iostat.log" 2>&1 &
    PERF_COLLECTOR_PIDS+=($!)

    # 2. vmstat - system CPU/IO overview every 1s
    vmstat 1 > "$PERF_OUTDIR/vmstat.log" 2>&1 &
    PERF_COLLECTOR_PIDS+=($!)

    # 3. mpstat - per-CPU iowait every 2s
    mpstat -P ALL 2 > "$PERF_OUTDIR/mpstat.log" 2>&1 &
    PERF_COLLECTOR_PIDS+=($!)

    # 4. pidstat IO + CPU every 2s
    pidstat -d -u 2 > "$PERF_OUTDIR/pidstat.log" 2>&1 &
    PERF_COLLECTOR_PIDS+=($!)

    # 5. Process /proc/PID/io auto-discovery poller
    (
        TRACKED_PIDS=""
        while true; do
            NEW_PIDS=$(pgrep -f "SPTAGTest\|SPFresh\|server\|sptag" 2>/dev/null || true)
            for PID in $NEW_PIDS; do
                if [[ ! " $TRACKED_PIDS " =~ " $PID " ]] && [ -f "/proc/$PID/io" ]; then
                    TRACKED_PIDS="$TRACKED_PIDS $PID"
                fi
            done
            TS=$(date +%s)
            for PID in $TRACKED_PIDS; do
                if [ -f "/proc/$PID/io" ]; then
                    COMM=$(cat "/proc/$PID/comm" 2>/dev/null || echo "unknown")
                    echo "--- ts=$TS pid=$PID comm=$COMM ---"
                    cat "/proc/$PID/io" 2>/dev/null || true
                fi
            done
            sleep 1
        done
    ) > "$PERF_OUTDIR/proc_io.log" 2>&1 &
    PERF_COLLECTOR_PIDS+=($!)

    # 6. perf record - on-CPU call stacks (sample all SPTAGTest threads)
    #    Runs system-wide at low frequency to avoid overhead
    perf record -a -g -F 99 -o "$PERF_OUTDIR/perf_oncpu.data" -- sleep 999999 > /dev/null 2>&1 &
    PERF_COLLECTOR_PIDS+=($!)

    # 7. Periodic perf-based off-CPU snapshot via /proc/PID/stack + wchan
    (
        while true; do
            TS=$(date +%s)
            for PID in $(pgrep -f "SPTAGTest" 2>/dev/null || true); do
                # Sample all thread stacks
                TIDS=$(ls "/proc/$PID/task/" 2>/dev/null || true)
                for TID in $TIDS; do
                    WCHAN=$(cat "/proc/$PID/task/$TID/wchan" 2>/dev/null || echo "?")
                    STATUS=$(grep '^State:' "/proc/$PID/task/$TID/status" 2>/dev/null | awk '{print $2}' || echo "?")
                    if [[ "$STATUS" == "S"* ]] || [[ "$STATUS" == "D"* ]]; then
                        STACK=$(head -5 "/proc/$PID/task/$TID/stack" 2>/dev/null | tr '\n' '|' || echo "?")
                        echo "ts=$TS pid=$PID tid=$TID state=$STATUS wchan=$WCHAN stack=$STACK"
                    fi
                done
            done
            sleep 2
        done
    ) > "$PERF_OUTDIR/thread_stacks.log" 2>&1 &
    PERF_COLLECTOR_PIDS+=($!)

    echo "[PERF]   iostat, vmstat, mpstat, pidstat, proc_io, perf_record, thread_stacks"
}

stop_perf_collectors() {
    # Stop all collectors and generate summary
    $PERF_MONITOR || return 0
    [ ${#PERF_COLLECTOR_PIDS[@]} -eq 0 ] && return 0

    local END_TIME=$(date +%s)
    local ELAPSED=$((END_TIME - PERF_START_TIME))

    echo "[PERF] Stopping collectors (ran ${ELAPSED}s)..."
    for pid in "${PERF_COLLECTOR_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait "${PERF_COLLECTOR_PIDS[@]}" 2>/dev/null || true
    PERF_COLLECTOR_PIDS=()

    # Generate perf report from recorded data
    if [ -f "$PERF_OUTDIR/perf_oncpu.data" ]; then
        echo "[PERF] Generating perf report..."
        perf report -i "$PERF_OUTDIR/perf_oncpu.data" --stdio --no-children \
            -g fractal,0.5 --comm SPTAGTest 2>/dev/null | head -200 > "$PERF_OUTDIR/perf_report.txt" || true
    fi

    # Generate Python summary
    generate_perf_summary "$ELAPSED"
}

generate_perf_summary() {
    local ELAPSED=$1
    local ODIR="$PERF_OUTDIR"
    [ -z "$ODIR" ] && return
    [ ! -d "$ODIR" ] && return

    python3 - "$ODIR" "$ELAPSED" << 'PYANALYSIS'
import re, os, sys
from collections import Counter, defaultdict

ODIR = sys.argv[1]
ELAPSED = int(sys.argv[2])
out_lines = []
def P(s=""): out_lines.append(s)

# ─── iostat parser (correct column mapping) ───
def parse_iostat(filepath):
    records = []
    current_ts = None
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            m = re.match(r'\d+/\d+/\d+\s+(\d+:\d+:\d+)', line)
            if m:
                current_ts = m.group(1)
                continue
            if not current_ts or line.startswith('Device') or line.startswith('Linux') or not line:
                continue
            parts = line.split()
            if len(parts) < 23: continue
            try:
                records.append({
                    'ts': current_ts, 'dev': parts[0],
                    'r_s': float(parts[1]), 'rMB_s': float(parts[2]), 'r_await': float(parts[5]),
                    'w_s': float(parts[7]), 'wMB_s': float(parts[8]), 'w_await': float(parts[11]),
                    'aqu_sz': float(parts[21]), 'util': float(parts[22]),
                })
            except: pass
    return records

def s(vals): return (sum(vals)/len(vals), max(vals)) if vals else (0,0)

P("=" * 90)
P(f"  PERF MONITOR SUMMARY — Duration: {ELAPSED}s — {__import__('datetime').datetime.now()}")
P("=" * 90)

# ─── Section 1: IO (active periods only) ───
iostat_path = os.path.join(ODIR, "iostat.log")
if os.path.exists(iostat_path):
    recs = parse_iostat(iostat_path)
    ts_data = {}
    for r in recs:
        ts_data.setdefault(r['ts'], {})[r['dev']] = r
    timestamps = sorted(ts_data.keys())[1:]  # skip cumulative

    active = {'md0': [], 'sda': [], 'sdb': [], 'nvme_total': []}
    for ts in timestamps:
        d = ts_data[ts]
        md0 = d.get('md0', {})
        sda = d.get('sda', {})
        sdb = d.get('sdb', {})
        busy = (md0.get('r_s',0)>10 or md0.get('wMB_s',0)>0.5 or md0.get('util',0)>5 or
                sda.get('r_s',0)>2 or sda.get('rMB_s',0)>0.5 or sda.get('util',0)>3 or
                sdb.get('r_s',0)>50 or sdb.get('rMB_s',0)>1)
        if not busy: continue
        active['md0'].append(md0)
        active['sda'].append(sda)
        active['sdb'].append(sdb)
        nvme_r = sum(d.get(f'nvme{i}n1',{}).get('r_s',0) for i in range(6))
        nvme_rmb = sum(d.get(f'nvme{i}n1',{}).get('rMB_s',0) for i in range(6))
        nvme_w = sum(d.get(f'nvme{i}n1',{}).get('w_s',0) for i in range(6))
        nvme_wmb = sum(d.get(f'nvme{i}n1',{}).get('wMB_s',0) for i in range(6))
        nvme_u = sum(d.get(f'nvme{i}n1',{}).get('util',0) for i in range(6))/6
        active['nvme_total'].append({'r_s':nvme_r,'rMB_s':nvme_rmb,'w_s':nvme_w,'wMB_s':nvme_wmb,'util':nvme_u})

    n = len(active['md0'])
    P(f"\n=== IO (active periods: {n}/{len(timestamps)} samples) ===")
    if n > 0:
        for label, key in [("md0 (TiKV RAID)", 'md0'), ("sda (HDD /data_disk)", 'sda'),
                           ("sdb (OS disk)", 'sdb'), ("NVMe 6x total", 'nvme_total')]:
            samples = active[key]
            if not samples: continue
            avg_rs, peak_rs = s([x.get('r_s',0) for x in samples])
            avg_rmb, peak_rmb = s([x.get('rMB_s',0) for x in samples])
            avg_ws, peak_ws = s([x.get('w_s',0) for x in samples])
            avg_wmb, peak_wmb = s([x.get('wMB_s',0) for x in samples])
            avg_rawait, _ = s([x.get('r_await',0) for x in samples])
            avg_wawait, peak_wawait = s([x.get('w_await',0) for x in samples])
            avg_util, peak_util = s([x.get('util',0) for x in samples])
            avg_aq, peak_aq = s([x.get('aqu_sz',0) for x in samples])
            P(f"  {label}:")
            P(f"    AVG:  r/s={avg_rs:8.0f}  rMB/s={avg_rmb:7.1f}  w/s={avg_ws:8.0f}  wMB/s={avg_wmb:7.1f}  r_await={avg_rawait:.2f}ms  w_await={avg_wawait:.2f}ms  util={avg_util:.1f}%  aq={avg_aq:.1f}")
            P(f"    PEAK: r/s={peak_rs:8.0f}  rMB/s={peak_rmb:7.1f}  w/s={peak_ws:8.0f}  wMB/s={peak_wmb:7.1f}  w_await={peak_wawait:.2f}ms  util={peak_util:.1f}%  aq={peak_aq:.1f}")
    else:
        P("  NO ACTIVE IO DETECTED")

# ─── Section 2: CPU (vmstat, active periods) ───
vmstat_path = os.path.join(ODIR, "vmstat.log")
if os.path.exists(vmstat_path):
    P(f"\n=== CPU ===")
    vals = []
    with open(vmstat_path) as f:
        for line in f.readlines()[2:]:
            parts = line.split()
            if len(parts) >= 17:
                try:
                    vals.append({'r':int(parts[0]),'b':int(parts[1]),'bi':int(parts[8]),'bo':int(parts[9]),
                                 'us':int(parts[12]),'sy':int(parts[13]),'id':int(parts[14]),'wa':int(parts[15])})
                except: pass
    if vals:
        n = len(vals)
        avg = lambda k: sum(v[k] for v in vals)/n
        peak = lambda k: max(v[k] for v in vals)
        P(f"  vmstat ({n} samples):")
        P(f"    AVG:  r={avg('r'):.1f}  b={avg('b'):.1f}  us={avg('us'):.1f}%  sy={avg('sy'):.1f}%  wa={avg('wa'):.1f}%  id={avg('id'):.1f}%")
        P(f"    PEAK: r={peak('r')}  b={peak('b')}  us={peak('us')}%  sy={peak('sy')}%  wa={peak('wa')}%")

# ─── Section 3: Thread wait analysis (off-CPU) ───
stacks_path = os.path.join(ODIR, "thread_stacks.log")
if os.path.exists(stacks_path):
    P(f"\n=== THREAD WAIT ANALYSIS (off-CPU) ===")
    wchan_counts = Counter()
    state_counts = Counter()
    stack_counts = Counter()
    total_sleeping = 0
    with open(stacks_path) as f:
        for line in f:
            m = re.match(r'ts=\d+ pid=\d+ tid=\d+ state=(\S+) wchan=(\S+) stack=(.*)', line)
            if m:
                state, wchan, stack = m.group(1), m.group(2), m.group(3)
                state_counts[state] += 1
                total_sleeping += 1
                if wchan != '0' and wchan != '?':
                    wchan_counts[wchan] += 1
                # Simplify stack for grouping
                frames = [f.strip() for f in stack.split('|') if f.strip() and '+0x' in f]
                if frames:
                    top3 = ' <- '.join(f.split('+')[0] for f in frames[:3])
                    stack_counts[top3] += 1

    if total_sleeping > 0:
        P(f"  Total sleeping thread samples: {total_sleeping}")
        P(f"\n  Thread states:")
        for st, cnt in state_counts.most_common():
            P(f"    {st}: {cnt} ({100*cnt/total_sleeping:.1f}%)")

        P(f"\n  Top wait channels (wchan):")
        for wc, cnt in wchan_counts.most_common(15):
            P(f"    {wc}: {cnt} ({100*cnt/total_sleeping:.1f}%)")

        if stack_counts:
            P(f"\n  Top sleeping call stacks:")
            for st, cnt in stack_counts.most_common(15):
                P(f"    [{cnt:5d}] {st}")

# ─── Section 4: perf on-CPU hotspots ───
perf_report_path = os.path.join(ODIR, "perf_report.txt")
if os.path.exists(perf_report_path):
    P(f"\n=== ON-CPU HOTSPOTS (perf) ===")
    with open(perf_report_path) as f:
        lines = f.readlines()
    # Extract top functions
    func_lines = [l.rstrip() for l in lines if '%' in l and ('SPTAGTest' in l or 'libSPTAG' in l or 'libc' in l or 'kernel' in l)]
    if func_lines:
        for l in func_lines[:20]:
            P(f"  {l}")
    else:
        P("  (no SPTAGTest samples found - try longer run)")

# ─── Section 5: Process IO rates ───
proc_io_path = os.path.join(ODIR, "proc_io.log")
if os.path.exists(proc_io_path):
    P(f"\n=== PROCESS IO RATES ===")
    with open(proc_io_path) as f:
        content = f.read()
    pid_data = defaultdict(list)
    current = {}
    for line in content.split('\n'):
        m = re.match(r'--- ts=(\d+) pid=(\d+) comm=(\S+) ---', line)
        if m:
            current = {'ts': int(m.group(1)), 'pid': m.group(2), 'comm': m.group(3)}
            continue
        m2 = re.match(r'(read_bytes|write_bytes):\s+(\d+)', line)
        if m2 and current:
            current[m2.group(1)] = int(m2.group(2))
            if 'read_bytes' in current and 'write_bytes' in current:
                pid_data[current['pid']].append(dict(current))
                current = {}

    for pid, samples in pid_data.items():
        if len(samples) < 2: continue
        first, last = samples[0], samples[-1]
        dt = last['ts'] - first['ts']
        if dt <= 0: continue
        dr = (last['read_bytes'] - first['read_bytes']) / dt / 1048576
        dw = (last['write_bytes'] - first['write_bytes']) / dt / 1048576
        tr = (last['read_bytes'] - first['read_bytes']) / 1073741824
        tw = (last['write_bytes'] - first['write_bytes']) / 1073741824
        P(f"  PID {pid} ({first['comm']}): {dt}s  read={dr:.1f} MB/s  write={dw:.1f} MB/s  total_rd={tr:.2f} GB  total_wr={tw:.2f} GB")

# ─── Verdict ───
P(f"\n{'='*90}")
P(f"  AUTO-VERDICT")
P(f"{'='*90}")

# Gather key metrics
io_util = 0; io_wawait = 0; cpu_us = 0; cpu_sy = 0; cpu_wa = 0; cpu_id = 0
if active.get('md0'):
    io_util = sum(x.get('util',0) for x in active['md0'])/len(active['md0'])
    io_wawait = sum(x.get('w_await',0) for x in active['md0'])/len(active['md0'])
if vals:
    cpu_us = sum(v['us'] for v in vals)/len(vals)
    cpu_sy = sum(v['sy'] for v in vals)/len(vals)
    cpu_wa = sum(v['wa'] for v in vals)/len(vals)
    cpu_id = sum(v['id'] for v in vals)/len(vals)

verdict = "UNKNOWN"
if io_util > 80 or cpu_wa > 10:
    verdict = "IO BOUND"
    P(f"  >>> IO BOUND: md0 util={io_util:.1f}%, iowait={cpu_wa:.1f}%")
elif cpu_us + cpu_sy > 80:
    verdict = "CPU BOUND"
    P(f"  >>> CPU BOUND: us+sy={cpu_us+cpu_sy:.1f}%")
elif cpu_id > 50 and cpu_wa < 5 and io_util < 30:
    # Check what they're waiting on
    top_wchan = wchan_counts.most_common(1)[0] if wchan_counts else ("?", 0)
    verdict = "WAIT/CONTENTION BOUND"
    P(f"  >>> WAIT/CONTENTION BOUND: idle={cpu_id:.1f}%, io_util={io_util:.1f}%, iowait={cpu_wa:.1f}%")
    P(f"      Top wait channel: {top_wchan[0]} ({top_wchan[1]} samples)")
    P(f"      Threads are sleeping, neither CPU nor IO is saturated.")
    P(f"      Likely cause: synchronous RPC waits, lock contention, or insufficient parallelism.")
else:
    P(f"  >>> MIXED: us={cpu_us:.1f}% sy={cpu_sy:.1f}% wa={cpu_wa:.1f}% id={cpu_id:.1f}% io_util={io_util:.1f}%")

P(f"\n  Reference thresholds:")
P(f"    IO BOUND:   md0 util>80%, await>1ms, iowait>10%")
P(f"    CPU BOUND:  us+sy>80%")
P(f"    WAIT BOUND: idle>50%, io_util<30%, iowait<5%")
P(f"    NVMe 6x RAID0 limits: ~3M IOPS, ~18 GB/s seq read")

# Write summary
summary_path = os.path.join(ODIR, "summary.txt")
with open(summary_path, 'w') as f:
    f.write('\n'.join(out_lines) + '\n')

# Print quick verdict to stdout
print(f"[PERF] ─── {verdict} ───")
print(f"[PERF]   IO: md0 util={io_util:.1f}%  w_await={io_wawait:.2f}ms")
print(f"[PERF]   CPU: us={cpu_us:.1f}%  sy={cpu_sy:.1f}%  wa={cpu_wa:.1f}%  id={cpu_id:.1f}%")
if wchan_counts:
    top3 = ', '.join(f"{w}({c})" for w,c in wchan_counts.most_common(3))
    print(f"[PERF]   Top waits: {top3}")
print(f"[PERF]   Full report: {summary_path}")
print(f"[PERF] ────────────────────")
PYANALYSIS
}

# ─── Usage ───
if [ $# -eq 0 ]; then
    echo "Usage: $0 [--perf-monitor] <scale> [scale2] ..."
    echo ""
    echo "Scales: 100k, 1m, 10m, 100m, or 'all'"
    echo ""
    echo "Options: --perf-monitor  Enable IO + CPU + thread profiling per phase"
    echo ""
    echo "Available configs:"
    for s in 100k 1m 10m 100m; do
        if ls evaluation/distributed/configs/benchmark_${s}_*.ini &>/dev/null; then
            configs=$(ls -1 evaluation/distributed/configs/benchmark_${s}_*.ini 2>/dev/null | wc -l)
            vectors=$(grep BaseVectorCount "evaluation/distributed/configs/benchmark_${s}_1node.ini" 2>/dev/null | cut -d= -f2 || echo "?")
            echo "  $s  (base=$vectors, $configs configs)"
        fi
    done
    exit 1
fi

# ─── Helper Functions ───

# Helper: stop all SPTAGTest worker processes
stop_workers() {
    # Wait for workers to self-exit via TCP Stop command, then force-kill if needed.
    # Usage: stop_workers [timeout_sec]  (default: 30)
    local TIMEOUT=${1:-30}
    if [ ${#WORKER_PIDS[@]} -eq 0 ]; then
        echo "  No worker PIDs tracked"
        return
    fi
    echo "Waiting for ${#WORKER_PIDS[@]} workers to exit (timeout: ${TIMEOUT}s)..."
    for pid in "${WORKER_PIDS[@]}"; do
        local elapsed=0
        while kill -0 "$pid" 2>/dev/null && [ $elapsed -lt $TIMEOUT ]; do
            sleep 1
            elapsed=$((elapsed + 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "  WARNING: Worker PID $pid still alive after ${TIMEOUT}s, force killing"
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        else
            echo "  Worker PID $pid exited gracefully"
        fi
    done
    WORKER_PIDS=()
    echo "  All workers stopped"
}

restart_tikv() {
    echo "Restarting TiKV cluster (clean data)..."
    pkill -f SPTAGTest 2>/dev/null || true
    sleep 1
    cd "$TIKV_DIR"
    docker compose down --remove-orphans --timeout 30 2>&1
    # Wait for all containers to fully stop
    for i in $(seq 1 10); do
        if ! docker ps --filter "name=tikv-" --format '{{.Names}}' | grep -q .; then
            break
        fi
        echo "  Waiting for containers to stop..."
        sleep 2
    done
        # Clean all TiKV and PD data directories (owned by root from containers)
    docker run --rm -v /mnt/nvme_striped/zhangt/tikv:/data alpine sh -c \
        "rm -rf /data/tikv*-data/* /data/pd*-data/* /data/tikv*-logs/* /data/pd*-logs/*"
    sleep 2
    docker compose up -d 2>&1
    echo "Waiting for TiKV to be ready..."
    # Wait for all 3 PD nodes to be healthy first
    for i in $(seq 1 30); do
        PD1_OK=$(curl -sf -o /dev/null http://127.0.0.1:23791/pd/api/v1/health 2>/dev/null && echo "1" || echo "0")
        PD2_OK=$(curl -sf -o /dev/null http://127.0.0.1:23792/pd/api/v1/health 2>/dev/null && echo "1" || echo "0")
        PD3_OK=$(curl -sf -o /dev/null http://127.0.0.1:23793/pd/api/v1/health 2>/dev/null && echo "1" || echo "0")
        if [ "$PD1_OK" = "1" ] && [ "$PD2_OK" = "1" ] && [ "$PD3_OK" = "1" ]; then
            break
        fi
        sleep 2
    done
    # Wait for 3 TiKV stores to register (up to 120s)
    for i in $(seq 1 60); do
        STORES=$(curl -s http://127.0.0.1:23791/pd/api/v1/stores 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(1 for s in d.get('stores',[]) if s.get('store',{}).get('state_name')=='Up'))" 2>/dev/null || echo "0")
        if [ "$STORES" = "3" ]; then
            break
        fi
        sleep 2
    done
    if [ "$STORES" != "3" ]; then
        echo "ERROR: Expected 3 TiKV stores Up, got $STORES"
        exit 1
    fi
    # Verify all PDs report same cluster ID
    CID1=$(curl -s http://127.0.0.1:23791/pd/api/v1/cluster 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
    CID2=$(curl -s http://127.0.0.1:23792/pd/api/v1/cluster 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
    CID3=$(curl -s http://127.0.0.1:23793/pd/api/v1/cluster 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
    if [ "$CID1" != "$CID2" ] || [ "$CID1" != "$CID3" ]; then
        echo "ERROR: PD cluster ID mismatch: $CID1 vs $CID2 vs $CID3"
        exit 1
    fi
    echo "TiKV cluster ready: $STORES stores, cluster=$CID1"
    cd "$SPTAG_DIR"
}

# Helper: rebalance TiKV region leaders evenly across stores
rebalance_tikv_leaders() {
    # Usage: rebalance_tikv_leaders [target_store_addrs...]
    # If target store addresses are given (e.g. 127.0.0.1:20161 127.0.0.1:20162),
    # only those stores get leaders; others are drained to 0.
    # If no args, balance across all stores.
    local TARGET_STORES="$*"
    echo "Rebalancing TiKV region leaders... targets=[${TARGET_STORES:-all}]"
    python3 -c "
import json, urllib.request, time, sys

target_addrs = '${TARGET_STORES}'.split() if '${TARGET_STORES}' else []

def api_get(path):
    return json.loads(urllib.request.urlopen('http://127.0.0.1:23791/pd/api/v1/' + path).read())

def api_post(path, data):
    req = urllib.request.Request('http://127.0.0.1:23791/pd/api/v1/' + path,
                                data=json.dumps(data).encode(),
                                headers={'Content-Type': 'application/json'})
    return json.loads(urllib.request.urlopen(req).read())

stores_data = api_get('stores')
store_info = {}
for s in stores_data.get('stores', []):
    si = s['store']
    if si.get('state_name') == 'Up':
        store_info[si['id']] = si['address']
store_ids = sorted(store_info.keys())
print(f'  Stores: {store_info}')

# Determine target store IDs (those that should hold leaders)
if target_addrs:
    target_ids = [sid for sid in store_ids if store_info[sid] in target_addrs]
    print(f'  Target stores (by address): {[store_info[s] for s in target_ids]}')
else:
    target_ids = list(store_ids)

if len(target_ids) < 1:
    print(f'  No target stores found, skipping')
    sys.exit(0)

regions = api_get('regions')['regions']
if not regions:
    print('  No regions')
    sys.exit(0)

from collections import Counter
leader_counts = Counter({sid: 0 for sid in store_ids})
for r in regions:
    sid = r.get('leader', {}).get('store_id')
    if sid in leader_counts: leader_counts[sid] += 1

print(f'  Before: {dict(leader_counts)} ({len(regions)} regions)')
target_per_store = len(regions) // len(target_ids)

transfers = 0
expected = dict(leader_counts)
for r in regions:
    src = r.get('leader', {}).get('store_id')
    # Transfer if src is NOT a target store, or if src has too many leaders
    if src in target_ids and expected.get(src, 0) <= target_per_store:
        continue
    dst = min(target_ids, key=lambda s: expected[s])
    if dst == src:
        continue
    if expected[dst] >= target_per_store + 1:
        continue
    peers = [p.get('store_id') for p in r.get('peers', [])]
    if dst not in peers:
        continue
    try:
        api_post('operators', {'name': 'transfer-leader', 'region_id': r['id'], 'to_store_id': dst})
        expected[src] -= 1; expected[dst] += 1; transfers += 1
    except Exception as e:
        print(f'  Transfer region {r[\"id\"]} failed: {e}')

if transfers == 0:
    print(f'  Already balanced')
    sys.exit(0)

print(f'  Expected: {expected} ({transfers} transfers)')

for attempt in range(15):
    time.sleep(2)
    regions2 = api_get('regions')['regions']
    lc2 = Counter({sid: 0 for sid in store_ids})
    for r in regions2:
        sid = r.get('leader', {}).get('store_id')
        if sid in lc2: lc2[sid] += 1
    if dict(lc2) == expected:
        print(f'  Verified after {(attempt+1)*2}s: {dict(lc2)}')
        sys.exit(0)

print(f'  Warning: after 30s actual={dict(lc2)}, expected={expected}')
" 2>&1
}

run_1node() {
    local SCALE=$1
    local INI="evaluation/distributed/configs/benchmark_${SCALE}_1node.ini"
    if [ ! -f "$INI" ]; then
        echo "  SKIP 1-node: $INI not found"
        return
    fi

    echo ""
    echo "--- ${SCALE}: 1-node baseline ---"
    echo "Start: $(date)"

    restart_tikv

    rm -rf "$IDXROOT/proidx_${SCALE}_1node" "truth_${SCALE}_1node" "output_${SCALE}_1node.json"
    mkdir -p "$IDXROOT/proidx_${SCALE}_1node"

    start_perf_collectors "${SCALE}_1node"

    BENCHMARK_CONFIG="$INI" \
    BENCHMARK_OUTPUT="output_${SCALE}_1node.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_1node.log"

    stop_perf_collectors
    echo "${SCALE} 1-node done: $(date)"
}

run_2node() {
    local SCALE=$1
    local INI_N0="evaluation/distributed/configs/benchmark_${SCALE}_2node_n0.ini"
    local INI_N1="evaluation/distributed/configs/benchmark_${SCALE}_2node_n1.ini"
    if [ ! -f "$INI_N0" ] || [ ! -f "$INI_N1" ]; then
        echo "  SKIP 2-node: configs not found ($INI_N0, $INI_N1)"
        return
    fi

    echo ""
    echo "--- ${SCALE}: 2-node distributed ---"
    echo "Start: $(date)"

    restart_tikv

    # Build index with n0 (router disabled - worker not started yet)
    rm -rf "$IDXROOT/proidx_${SCALE}_2node_n0" "$IDXROOT/proidx_${SCALE}_2node_n1" "truth_${SCALE}_2node" "output_${SCALE}_2node"*.json
    mkdir -p "$IDXROOT/proidx_${SCALE}_2node_n0"

    # Disable router during build: worker isn't running yet
    sed 's/^RouterEnabled=true/RouterEnabled=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_2node_n0_build.ini"

    start_perf_collectors "${SCALE}_2node_build"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_2node_n0_build.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_2node_build.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_2node_build.log"

    stop_perf_collectors
    echo "${SCALE} 2-node build done: $(date)"
    # Only balance leaders to the 2 stores mapped by RouterNodeStores
    rebalance_tikv_leaders

    # Clear checkpoint so driver re-runs all insert batches with routing
    rm -f "$IDXROOT/proidx_${SCALE}_2node_n0/spann_index/checkpoint.txt"

    # Copy head index to n1
    echo "Copying head index to n1..."
    mkdir -p "$IDXROOT/proidx_${SCALE}_2node_n1/spann_index"
    cp -r "$IDXROOT/proidx_${SCALE}_2node_n0/spann_index/"* "$IDXROOT/proidx_${SCALE}_2node_n1/spann_index/"

    # Start worker n1
    echo "Starting worker n1..."
    BENCHMARK_CONFIG="$INI_N1" \
      $BINARY --run_test=SPFreshTest/WorkerNode \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_2node_worker1.log" &
    WORKER_PIDS=($!)
    echo "  Waiting for worker n1 to be ready..."
    for i in $(seq 1 120); do
        if grep -q "WorkerNode.*Ready" "$LOGDIR/benchmark_${SCALE}_2node_worker1.log" 2>/dev/null; then
            echo "  Worker n1 ready (${i}s)"
            break
        fi
        if ! kill -0 ${WORKER_PIDS[0]} 2>/dev/null; then
            echo "  ERROR: Worker n1 (PID ${WORKER_PIDS[0]}) exited prematurely"
            return
        fi
        sleep 1
    done

    # Run driver n0 with Rebuild=false
    sed 's/^Rebuild=true/Rebuild=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_2node_n0_run.ini"

    start_perf_collectors "${SCALE}_2node_query"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_2node_n0_run.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_2node.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_2node_driver.log"

    stop_perf_collectors

    # Stop workers
    stop_workers
    echo "${SCALE} 2-node done: $(date)"
}

run_3node() {
    local SCALE=$1
    local INI_N0="evaluation/distributed/configs/benchmark_${SCALE}_3node_n0.ini"
    local INI_N1="evaluation/distributed/configs/benchmark_${SCALE}_3node_n1.ini"
    local INI_N2="evaluation/distributed/configs/benchmark_${SCALE}_3node_n2.ini"
    if [ ! -f "$INI_N0" ] || [ ! -f "$INI_N1" ] || [ ! -f "$INI_N2" ]; then
        echo "  SKIP 3-node: configs not found"
        return
    fi

    echo ""
    echo "--- ${SCALE}: 3-node distributed ---"
    echo "Start: $(date)"

    restart_tikv

    # Build index with n0 (router disabled - workers not started yet)
    rm -rf "$IDXROOT/proidx_${SCALE}_3node_n0" "$IDXROOT/proidx_${SCALE}_3node_n1" "$IDXROOT/proidx_${SCALE}_3node_n2" "truth_${SCALE}_3node" "output_${SCALE}_3node"*.json
    mkdir -p "$IDXROOT/proidx_${SCALE}_3node_n0"

    # Disable router during build: workers aren't running yet
    sed 's/^RouterEnabled=true/RouterEnabled=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_3node_n0_build.ini"

    start_perf_collectors "${SCALE}_3node_build"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_3node_n0_build.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_3node_build.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_3node_build.log"

    stop_perf_collectors
    echo "${SCALE} 3-node build done: $(date)"
    rebalance_tikv_leaders

    # Clear checkpoint so driver re-runs all insert batches with routing
    rm -f "$IDXROOT/proidx_${SCALE}_3node_n0/spann_index/checkpoint.txt"

    # Copy head index to n1, n2
    echo "Copying head index to n1, n2..."
    mkdir -p "$IDXROOT/proidx_${SCALE}_3node_n1/spann_index"
    mkdir -p "$IDXROOT/proidx_${SCALE}_3node_n2/spann_index"
    cp -r "$IDXROOT/proidx_${SCALE}_3node_n0/spann_index/"* "$IDXROOT/proidx_${SCALE}_3node_n1/spann_index/"
    cp -r "$IDXROOT/proidx_${SCALE}_3node_n0/spann_index/"* "$IDXROOT/proidx_${SCALE}_3node_n2/spann_index/"

    # Start workers n1, n2
    echo "Starting workers n1, n2..."

    BENCHMARK_CONFIG="$INI_N1" \
      $BINARY --run_test=SPFreshTest/WorkerNode \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_3node_worker1.log" &
    WORKER_PIDS=($!)

    BENCHMARK_CONFIG="$INI_N2" \
      $BINARY --run_test=SPFreshTest/WorkerNode \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_3node_worker2.log" &
    WORKER_PIDS+=($!)

    echo "  Waiting for workers n1, n2 to be ready..."
    for i in $(seq 1 120); do
        W1_READY=$(grep -q "WorkerNode.*Ready" "$LOGDIR/benchmark_${SCALE}_3node_worker1.log" 2>/dev/null && echo 1 || echo 0)
        W2_READY=$(grep -q "WorkerNode.*Ready" "$LOGDIR/benchmark_${SCALE}_3node_worker2.log" 2>/dev/null && echo 1 || echo 0)
        if [ "$W1_READY" = "1" ] && [ "$W2_READY" = "1" ]; then
            echo "  Both workers ready (${i}s)"
            break
        fi
        if ! kill -0 ${WORKER_PIDS[0]} 2>/dev/null; then
            echo "  ERROR: Worker n1 (PID ${WORKER_PIDS[0]}) exited prematurely"
            return
        fi
        if ! kill -0 ${WORKER_PIDS[1]} 2>/dev/null; then
            echo "  ERROR: Worker n2 (PID ${WORKER_PIDS[1]}) exited prematurely"
            return
        fi
        sleep 1
    done

    # Run driver n0 with Rebuild=false
    sed 's/^Rebuild=true/Rebuild=false/' "$INI_N0" > "/tmp/benchmark_${SCALE}_3node_n0_run.ini"

    start_perf_collectors "${SCALE}_3node_query"

    BENCHMARK_CONFIG="/tmp/benchmark_${SCALE}_3node_n0_run.ini" \
    BENCHMARK_OUTPUT="output_${SCALE}_3node.json" \
      $BINARY --run_test=SPFreshTest/BenchmarkFromConfig \
      2>&1 | tee "$LOGDIR/benchmark_${SCALE}_3node_driver.log"

    stop_perf_collectors

    # Stop workers
    stop_workers
    echo "${SCALE} 3-node done: $(date)"
}

# ─── Run all phases for one scale ───

run_scale() {
    local SCALE=$1
    echo ""
    echo "=========================================="
    echo " SPTAG ${SCALE} Distributed Routing Scale Test"
    echo " $(date)"
    echo "=========================================="

    run_1node "$SCALE"
    run_2node "$SCALE"
    run_3node "$SCALE"

    echo ""
    echo "------------------------------------------"
    echo " ${SCALE} complete: $(date)"
    echo " Results:"
    for f in "output_${SCALE}_1node.json" "output_${SCALE}_2node.json" "output_${SCALE}_3node.json"; do
        if [ -f "$f" ]; then
            echo "   $f"
        fi
    done
    echo "------------------------------------------"
}

# ─── Main ───

# Expand "all" to all available scales
SCALES=()
for arg in "$@"; do
    if [ "$arg" = "all" ]; then
        for s in 100k 1m 10m 100m; do
            if ls evaluation/distributed/configs/benchmark_${s}_*.ini &>/dev/null; then
                SCALES+=("$s")
            fi
        done
    elif [ "$arg" = "--perf-monitor" ]; then
        PERF_MONITOR=true
    else
        SCALES+=("$arg")
    fi
done

# Validate
for SCALE in "${SCALES[@]}"; do
    if ! ls evaluation/distributed/configs/benchmark_${SCALE}_*.ini &>/dev/null; then
        echo "ERROR: No configs found for scale '$SCALE' (expected evaluation/distributed/configs/benchmark_${SCALE}_*.ini)"
        exit 1
    fi
done

echo "=========================================="
echo " SPTAG Distributed Routing Scale Test"
echo " Scales: ${SCALES[*]}"
$PERF_MONITOR && echo " Perf Monitor: ENABLED"
echo " $(date)"
echo "=========================================="

for SCALE in "${SCALES[@]}"; do
    run_scale "$SCALE"
done

echo ""
echo "Shutting down TiKV cluster..."
cd "$TIKV_DIR" && docker compose down 2>&1
cd "$SPTAG_DIR"

echo ""
echo "=========================================="
echo " ALL DONE: ${SCALES[*]}"
echo " $(date)"
echo "=========================================="
