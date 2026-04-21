#!/bin/bash
# Multi-machine distributed benchmark orchestrator for SPTAG.
#
# Usage:
#   ./run_distributed.sh deploy     <cluster.conf>                Deploy binary + data to all nodes
#   ./run_distributed.sh start-tikv <cluster.conf> [node_count]   Start independent TiKV/PD instances
#   ./run_distributed.sh stop-tikv  <cluster.conf> [node_count]   Stop TiKV/PD instances
#   ./run_distributed.sh run        <cluster.conf> <scale> <node_count>  Run benchmark
#   ./run_distributed.sh bench      <cluster.conf> <scale> [scale...]    Run 1-node + N-node for each scale
#   ./run_distributed.sh cleanup    <cluster.conf>                Remove deployed files from remote nodes
#
# Prerequisites:
#   - Passwordless SSH from driver to all nodes (configure ssh_key in cluster.conf)
#   - Docker installed on all nodes (for TiKV)
#   - cluster.conf configured (see cluster.conf.example)
#
# The driver (first node in [nodes]) orchestrates everything.
# Each machine runs its own independent PD + TiKV (NOT a shared Raft cluster).

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOGDIR="$(cd "$SCRIPT_DIR/../.." && pwd)/benchmark_logs"
mkdir -p "$LOGDIR"

# ─── Config Parsing ───

declare -a NODE_HOSTS NODE_ROUTER_PORTS
declare -a TIKV_HOSTS TIKV_PD_CLIENT_PORTS TIKV_PD_PEER_PORTS TIKV_PORTS
declare SSH_USER SPTAG_DIR DATA_DIR TIKV_VERSION PD_VERSION SSH_KEY
TOTAL_NODES=0

parse_config() {
    local CONF="$1"
    if [ ! -f "$CONF" ]; then
        echo "ERROR: Config file not found: $CONF"
        exit 1
    fi

    local SECTION=""

    while IFS= read -r line || [ -n "$line" ]; do
        # Strip comments and whitespace
        line="${line%%#*}"
        line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
        [ -z "$line" ] && continue

        # Section header
        if [[ "$line" =~ ^\[(.+)\]$ ]]; then
            SECTION="${BASH_REMATCH[1]}"
            continue
        fi

        case "$SECTION" in
            cluster)
                local key="${line%%=*}"
                local val="${line#*=}"
                case "$key" in
                    ssh_user)     SSH_USER="$val" ;;
                    sptag_dir)    SPTAG_DIR="$val" ;;
                    data_dir)     DATA_DIR="$val" ;;
                    tikv_version) TIKV_VERSION="$val" ;;
                    pd_version)   PD_VERSION="$val" ;;
                    ssh_key)      SSH_KEY="$val" ;;
                esac
                ;;
            nodes)
                read -r host rport <<< "$line"
                NODE_HOSTS+=("$host")
                NODE_ROUTER_PORTS+=("$rport")
                ;;
            tikv)
                read -r host pd_client pd_peer tikv_port <<< "$line"
                TIKV_HOSTS+=("$host")
                TIKV_PD_CLIENT_PORTS+=("$pd_client")
                TIKV_PD_PEER_PORTS+=("$pd_peer")
                TIKV_PORTS+=("$tikv_port")
                ;;
        esac
    done < "$CONF"

    # Defaults
    SSH_USER="${SSH_USER:-$(whoami)}"
    TIKV_VERSION="${TIKV_VERSION:-v8.5.1}"
    PD_VERSION="${PD_VERSION:-v8.5.1}"

    # Expand ~ in ssh_key path
    if [ -n "$SSH_KEY" ]; then
        SSH_KEY="${SSH_KEY/#\~/$HOME}"
    fi

    TOTAL_NODES=${#NODE_HOSTS[@]}

    if [ "$TOTAL_NODES" -lt 1 ]; then
        echo "ERROR: No compute nodes defined in [nodes]"
        exit 1
    fi
    if [ ${#TIKV_HOSTS[@]} -lt 1 ]; then
        echo "ERROR: No TiKV instances defined in [tikv]"
        exit 1
    fi

    echo "Cluster config loaded:"
    echo "  Compute nodes: $TOTAL_NODES (driver: ${NODE_HOSTS[0]})"
    echo "  TiKV instances: ${#TIKV_HOSTS[@]}"
    echo "  SSH user: $SSH_USER"
    echo "  SSH key: ${SSH_KEY:-(none)}"
    echo "  SPTAG dir: $SPTAG_DIR"
    echo "  Data dir: $DATA_DIR"
}

# ─── SSH Helpers ───

# Build SSH options string (key + host checking)
_ssh_opts() {
    local opts="-o StrictHostKeyChecking=no -o ConnectTimeout=10"
    if [ -n "$SSH_KEY" ]; then
        opts+=" -i $SSH_KEY"
    fi
    echo "$opts"
}

# Run command on remote host (or locally if it's the driver)
remote_exec() {
    local host="$1"; shift
    if [ "$host" = "${NODE_HOSTS[0]}" ] || [ "$host" = "localhost" ] || [ "$host" = "127.0.0.1" ]; then
        eval "$@"
    else
        ssh $(_ssh_opts) "$SSH_USER@$host" "$@"
    fi
}

# rsync files to remote host
remote_sync() {
    local host="$1"
    local src="$2"
    local dst="$3"
    if [ "$host" = "${NODE_HOSTS[0]}" ] || [ "$host" = "localhost" ]; then
        # Local copy — skip if same path
        if [ "$(realpath "$src")" != "$(realpath "$dst")" ]; then
            rsync -az --progress "$src" "$dst"
        fi
    else
        rsync -az --progress -e "ssh $(_ssh_opts)" "$src" "$SSH_USER@$host:$dst"
    fi
}

# ─── Deploy ───

cmd_deploy() {
    echo ""
    echo "=== Deploying SPTAG to ${#NODE_HOSTS[@]} nodes ==="
    echo ""

    # Validate SSH connectivity
    for host in "${NODE_HOSTS[@]}"; do
        if [ "$host" = "${NODE_HOSTS[0]}" ]; then continue; fi
        echo -n "  Checking SSH to $host... "
        if remote_exec "$host" "echo ok" >/dev/null 2>&1; then
            echo "OK"
        else
            echo "FAILED"
            echo "ERROR: Cannot SSH to $SSH_USER@$host"
            exit 1
        fi
    done

    # Deploy binary to all remote nodes
    echo ""
    echo "Deploying binary..."
    local BINARY="$SPTAG_DIR/Release/SPTAGTest"
    if [ ! -f "$BINARY" ]; then
        echo "ERROR: Binary not found: $BINARY (run cmake build first)"
        exit 1
    fi

    for host in "${NODE_HOSTS[@]}"; do
        if [ "$host" = "${NODE_HOSTS[0]}" ]; then continue; fi
        echo "  → $host:$SPTAG_DIR/Release/"
        remote_exec "$host" "mkdir -p $SPTAG_DIR/Release"
        remote_sync "$host" "$BINARY" "$SPTAG_DIR/Release/SPTAGTest"
        # Also deploy any shared libraries
        if ls "$SPTAG_DIR/Release/"*.so 2>/dev/null; then
            remote_sync "$host" "$SPTAG_DIR/Release/*.so" "$SPTAG_DIR/Release/"
        fi
    done

    # Deploy data files (perftest_* vectors, queries)
    echo ""
    echo "Deploying data files..."
    for host in "${NODE_HOSTS[@]}"; do
        if [ "$host" = "${NODE_HOSTS[0]}" ]; then continue; fi
        echo "  → $host:$SPTAG_DIR/ (perftest_* files)"
        remote_exec "$host" "mkdir -p $SPTAG_DIR"
        rsync -az --progress \
            --include='perftest_*' --exclude='*' \
            -e "ssh $(_ssh_opts)" \
            "$SPTAG_DIR/" "$SSH_USER@$host:$SPTAG_DIR/"
    done

    echo ""
    echo "Deploy complete."
}

# ─── TiKV Management (Independent Mode) ───

tikv_start() {
    # Start the first <node_count> independent PD+TiKV pairs.
    local node_count="${1:-${#TIKV_HOSTS[@]}}"
    echo ""
    echo "=== Starting $node_count independent TiKV instances ==="

    # Start PD instances (each standalone — initial-cluster references only itself)
    echo "Starting PD instances..."
    for (( i=0; i<node_count; i++ )); do
        local host="${TIKV_HOSTS[$i]}"
        local client_port="${TIKV_PD_CLIENT_PORTS[$i]}"
        local peer_port="${TIKV_PD_PEER_PORTS[$i]}"
        local pd_name="pd${i}"
        local initial_cluster="${pd_name}=http://${host}:${peer_port}"
        echo "  PD $i on $host:$client_port (standalone)"

        remote_exec "$host" "docker rm -f sptag-pd-$i 2>/dev/null; \
            docker run -d --name sptag-pd-$i --net host \
            -v $DATA_DIR/tikv-data/pd-$i:/data \
            pingcap/pd:${PD_VERSION} \
            --name=${pd_name} \
            --data-dir=/data \
            --client-urls=http://0.0.0.0:${client_port} \
            --advertise-client-urls=http://${host}:${client_port} \
            --peer-urls=http://0.0.0.0:${peer_port} \
            --advertise-peer-urls=http://${host}:${peer_port} \
            --initial-cluster=${initial_cluster}"
    done

    echo "Waiting for PD instances to start..."
    sleep 5

    # Check each PD health independently
    for (( i=0; i<node_count; i++ )); do
        local host="${TIKV_HOSTS[$i]}"
        local pd_port="${TIKV_PD_CLIENT_PORTS[$i]}"
        for attempt in $(seq 1 30); do
            if curl -sf "http://${host}:${pd_port}/pd/api/v1/members" >/dev/null 2>&1; then
                echo "  PD $i ($host:$pd_port) healthy"
                break
            fi
            if [ "$attempt" -eq 30 ]; then
                echo "  ERROR: PD $i ($host:$pd_port) not healthy after 30s"
                return 1
            fi
            sleep 1
        done
    done

    # Set max-replicas=1 for each independent PD (single-node Raft)
    echo "Setting max-replicas=1 on each PD..."
    for (( i=0; i<node_count; i++ )); do
        local host="${TIKV_HOSTS[$i]}"
        local pd_port="${TIKV_PD_CLIENT_PORTS[$i]}"
        if curl -sf "http://${host}:${pd_port}/pd/api/v1/config/replicate" \
            -X POST -d '{"max-replicas": 1}' >/dev/null 2>&1; then
            echo "  PD $i: max-replicas=1 set"
        else
            echo "  WARNING: Failed to set max-replicas=1 on PD $i"
        fi
    done

    # Start TiKV instances (each connects to its own PD only)
    echo "Starting TiKV instances..."
    for (( i=0; i<node_count; i++ )); do
        local host="${TIKV_HOSTS[$i]}"
        local tikv_port="${TIKV_PORTS[$i]}"
        local pd_port="${TIKV_PD_CLIENT_PORTS[$i]}"
        echo "  TiKV $i on $host:$tikv_port → PD $host:$pd_port"

        # Deploy tikv.toml to remote host
        local TIKV_TOML="$SCRIPT_DIR/configs/tikv.toml"
        if [[ -f "$TIKV_TOML" ]]; then
            remote_exec "$host" "docker run --rm -v $DATA_DIR/tikv-data:/data alpine mkdir -p /data/conf"
            if [ "$host" = "${NODE_HOSTS[0]}" ] || [ "$host" = "localhost" ] || [ "$host" = "127.0.0.1" ]; then
                sg docker -c "docker run --rm -v $DATA_DIR/tikv-data/conf:/conf -v $(realpath "$TIKV_TOML"):/src/tikv.toml:ro alpine cp /src/tikv.toml /conf/tikv.toml"
            else
                scp $(_ssh_opts) "$TIKV_TOML" "${SSH_USER}@${host}:${SPTAG_DIR}/tikv.toml"
                remote_exec "$host" "docker run --rm -v $DATA_DIR/tikv-data/conf:/conf -v ${SPTAG_DIR}/tikv.toml:/src/tikv.toml:ro alpine cp /src/tikv.toml /conf/tikv.toml"
            fi
        fi

        remote_exec "$host" "docker rm -f sptag-tikv-$i 2>/dev/null; \
            docker run -d --name sptag-tikv-$i --net host \
            --ulimit nofile=1048576:1048576 \
            -v $DATA_DIR/tikv-data/tikv-$i:/data \
            -v $DATA_DIR/tikv-data/conf:/conf \
            pingcap/tikv:${TIKV_VERSION} \
            --config=/conf/tikv.toml \
            --addr=0.0.0.0:${tikv_port} \
            --advertise-addr=${host}:${tikv_port} \
            --data-dir=/data \
            --pd-endpoints=http://${host}:${pd_port}"
    done

    echo "Waiting for TiKV stores to register..."
    sleep 5

    # Check each TiKV store registered with its own PD
    for (( i=0; i<node_count; i++ )); do
        local host="${TIKV_HOSTS[$i]}"
        local pd_port="${TIKV_PD_CLIENT_PORTS[$i]}"
        for attempt in $(seq 1 60); do
            local store_count
            store_count=$(curl -sf "http://${host}:${pd_port}/pd/api/v1/stores" 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null || echo 0)
            if [ "$store_count" -ge 1 ]; then
                echo "  TiKV $i registered with PD $i"
                break
            fi
            if [ "$attempt" -eq 60 ]; then
                echo "  WARNING: TiKV $i not registered after 60s"
            fi
            sleep 1
        done
    done

    echo "TiKV instances started ($node_count)."
}

tikv_stop() {
    # Stop the first <node_count> TiKV+PD instances.
    local node_count="${1:-${#TIKV_HOSTS[@]}}"
    echo ""
    echo "=== Stopping $node_count TiKV instances ==="

    for (( i=0; i<node_count; i++ )); do
        local host="${TIKV_HOSTS[$i]}"
        echo "  Stopping TiKV $i and PD $i on $host..."
        remote_exec "$host" "docker rm -f sptag-tikv-$i sptag-pd-$i 2>/dev/null || true"
    done

    echo "TiKV instances stopped."
}

tikv_clean() {
    # Clean TiKV data for the first <node_count> instances.
    local node_count="${1:-${#TIKV_HOSTS[@]}}"
    echo ""
    echo "=== Cleaning TiKV data ($node_count instances) ==="

    for (( i=0; i<node_count; i++ )); do
        local host="${TIKV_HOSTS[$i]}"
        echo "  Cleaning TiKV data on $host..."
        remote_exec "$host" "docker run --rm -v $DATA_DIR/tikv-data:/data alpine \
            rm -rf /data/tikv-$i /data/pd-$i 2>/dev/null || true"
    done
}

# Legacy wrappers for the main case block
cmd_start_tikv() { tikv_start "${1:-${#TIKV_HOSTS[@]}}"; }
cmd_stop_tikv()  { tikv_stop  "${1:-${#TIKV_HOSTS[@]}}"; }

# ─── INI Generation ───

generate_ini() {
    # Generate a benchmark INI from a template, filling in [Distributed] fields.
    # Usage: generate_ini <scale> <node_count> [overrides...]
    local SCALE="$1"
    local NODE_COUNT="$2"
    shift 2

    local IDX_PATH="$DATA_DIR/proidx_${SCALE}_${NODE_COUNT}node/spann_index"
    local KEY_PREFIX="bench${SCALE}_${NODE_COUNT}node"

    # Build comma-separated address lists from the first node_count entries
    local dispatcher_addr="${NODE_HOSTS[0]}:30001"
    local worker_addrs="" store_addrs="" pd_addrs=""
    for (( i=0; i<NODE_COUNT; i++ )); do
        [ -n "$worker_addrs" ] && worker_addrs+=","
        worker_addrs+="${NODE_HOSTS[$i]}:${NODE_ROUTER_PORTS[$i]}"
        [ -n "$store_addrs" ] && store_addrs+=","
        store_addrs+="${TIKV_HOSTS[$i]}:${TIKV_PORTS[$i]}"
        [ -n "$pd_addrs" ] && pd_addrs+=","
        pd_addrs+="${TIKV_HOSTS[$i]}:${TIKV_PD_CLIENT_PORTS[$i]}"
    done

    # Load the base INI template
    local BASE_INI="$SCRIPT_DIR/configs/benchmark_${SCALE}_template.ini"
    if [ ! -f "$BASE_INI" ]; then
        echo "ERROR: Template INI not found: $BASE_INI" >&2
        return 1
    fi

    local OUT="$SCRIPT_DIR/configs/benchmark_${SCALE}_${NODE_COUNT}node.ini"
    cp "$BASE_INI" "$OUT"

    # Fill in placeholder fields
    sed -i "s|^IndexPath=.*|IndexPath=${IDX_PATH}|" "$OUT"
    sed -i "s|^TiKVKeyPrefix=.*|TiKVKeyPrefix=${KEY_PREFIX}|" "$OUT"
    sed -i "s|^DispatcherAddr=.*|DispatcherAddr=${dispatcher_addr}|" "$OUT"
    sed -i "s|^WorkerAddrs=.*|WorkerAddrs=${worker_addrs}|" "$OUT"
    sed -i "s|^StoreAddrs=.*|StoreAddrs=${store_addrs}|" "$OUT"
    sed -i "s|^PDAddrs=.*|PDAddrs=${pd_addrs}|" "$OUT"

    # Apply extra overrides (key=value pairs)
    for override in "$@"; do
        local key="${override%%=*}"
        local val="${override#*=}"
        if grep -q "^${key}=" "$OUT"; then
            sed -i "s|^${key}=.*|${key}=${val}|" "$OUT"
        else
            # Append to [Benchmark] section
            sed -i "/^\[Benchmark\]/a ${key}=${val}" "$OUT"
        fi
    done

    echo "$OUT"
}

# ─── Worker Management ───

WORKER_SSH_PIDS=()

start_remote_worker() {
    # Start a worker on a remote node. Returns immediately; worker runs in background.
    local NODE_IDX="$1"
    local INI="$2"
    local SCALE="$3"
    local NODE_COUNT="$4"
    local host="${NODE_HOSTS[$NODE_IDX]}"
    local LOG="$LOGDIR/benchmark_${SCALE}_${NODE_COUNT}node_worker${NODE_IDX}.log"

    # Copy INI + binary to remote
    remote_sync "$host" "$INI" "$SPTAG_DIR/worker_n${NODE_IDX}.ini"

    # Start worker via SSH (foreground on remote, background locally)
    ssh $(_ssh_opts) "$SSH_USER@$host" \
        "cd $SPTAG_DIR && WORKER_INDEX=${NODE_IDX} BENCHMARK_CONFIG=worker_n${NODE_IDX}.ini \
         ./Release/SPTAGTest --run_test=SPFreshTest/BenchmarkFromConfig 2>&1" \
        > "$LOG" 2>&1 &
    local ssh_pid=$!
    WORKER_SSH_PIDS+=($ssh_pid)
    echo "  Worker n${NODE_IDX} on $host (SSH PID: $ssh_pid, log: $LOG)"
}

wait_workers_ready() {
    local SCALE="$1"
    local NODE_COUNT="$2"
    local TIMEOUT=120

    echo "Waiting for ${#WORKER_SSH_PIDS[@]} workers to be ready..."
    for attempt in $(seq 1 $TIMEOUT); do
        local all_ready=true
        for i in $(seq 1 $((NODE_COUNT - 1))); do
            local LOG="$LOGDIR/benchmark_${SCALE}_${NODE_COUNT}node_worker${i}.log"
            if ! grep -q "Worker.*[Rr]eady\|Waiting for dispatch" "$LOG" 2>/dev/null; then
                all_ready=false
            fi
        done
        if $all_ready; then
            echo "  All workers ready (${attempt}s)"
            return 0
        fi
        # Check if any worker SSH process died
        for idx in "${!WORKER_SSH_PIDS[@]}"; do
            if ! kill -0 "${WORKER_SSH_PIDS[$idx]}" 2>/dev/null; then
                echo "  ERROR: Worker SSH PID ${WORKER_SSH_PIDS[$idx]} exited prematurely"
                return 1
            fi
        done
        sleep 1
    done
    echo "  WARNING: Not all workers ready after ${TIMEOUT}s"
    return 1
}

stop_remote_workers() {
    # Wait for workers to self-exit (driver sends TCP Stop), then force-kill.
    local TIMEOUT=${1:-30}
    if [ ${#WORKER_SSH_PIDS[@]} -eq 0 ]; then return; fi

    echo "Waiting for ${#WORKER_SSH_PIDS[@]} remote workers to exit (${TIMEOUT}s timeout)..."
    for pid in "${WORKER_SSH_PIDS[@]}"; do
        local elapsed=0
        while kill -0 "$pid" 2>/dev/null && [ $elapsed -lt $TIMEOUT ]; do
            sleep 1
            elapsed=$((elapsed + 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "  WARNING: SSH PID $pid still alive, force killing"
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        else
            echo "  Worker (SSH PID $pid) exited gracefully"
        fi
    done
    WORKER_SSH_PIDS=()
}

# ─── Benchmark Run ───

distribute_head_index() {
    # Copy the head index from driver to all worker nodes.
    local SCALE="$1"
    local NODE_COUNT="$2"
    local SRC="$DATA_DIR/proidx_${SCALE}_${NODE_COUNT}node/spann_index"

    echo "Distributing head index to $((NODE_COUNT - 1)) workers..."
    for (( i=1; i<NODE_COUNT; i++ )); do
        local host="${NODE_HOSTS[$i]}"
        local DST="$DATA_DIR/proidx_${SCALE}_${NODE_COUNT}node/spann_index"
        echo "  → n${i} ($host)"
        remote_exec "$host" "mkdir -p $DST"
        remote_sync "$host" "$SRC/" "$DST/"
    done
}

distribute_perftest_files() {
    # rsync generated perftest_* files from driver to workers.
    local NODE_COUNT="$1"
    echo "Distributing perftest_* data files to workers..."
    for (( i=1; i<NODE_COUNT; i++ )); do
        local host="${NODE_HOSTS[$i]}"
        echo "  → $host"
        rsync -az --progress \
            --include='perftest_*' --exclude='*' \
            -e "ssh $(_ssh_opts)" \
            "$SPTAG_DIR/" "$SSH_USER@$host:$SPTAG_DIR/"
    done
}

cmd_run() {
    local SCALE="$1"
    local NODE_COUNT="$2"
    if [ -z "$SCALE" ] || [ -z "$NODE_COUNT" ]; then
        echo "Usage: $0 run <cluster.conf> <scale> <node_count>"
        exit 1
    fi

    local BINARY="$SPTAG_DIR/Release/SPTAGTest"

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ${SCALE}: ${NODE_COUNT}-node benchmark"
    echo "  Start: $(date)"
    echo "═══════════════════════════════════════════════════"

    if [ "$NODE_COUNT" -eq 1 ]; then
        # ─── Single-node flow ───
        echo ""
        echo "--- Phase 0: Prepare TiKV (1 instance) ---"
        tikv_stop 1
        tikv_clean 1
        tikv_start 1

        echo ""
        echo "--- Phase 1: Single-node run ---"
        local INI
        INI=$(generate_ini "$SCALE" 1 "Rebuild=true") || exit 1

        # Clean old index dir
        rm -rf "$DATA_DIR/proidx_${SCALE}_1node"
        mkdir -p "$DATA_DIR/proidx_${SCALE}_1node"

        echo "Starting driver on ${NODE_HOSTS[0]}..."
        BENCHMARK_CONFIG="$INI" \
        BENCHMARK_OUTPUT="output_${SCALE}_1node.json" \
            "$BINARY" --run_test=SPFreshTest/BenchmarkFromConfig \
            2>&1 | tee "$LOGDIR/benchmark_${SCALE}_1node_driver.log"

        echo "Done: $(date)"
        tikv_stop 1
    else
        # ─── Multi-node flow ───
        echo ""
        echo "--- Phase 0: Prepare TiKV ($NODE_COUNT instances) ---"
        tikv_stop "$NODE_COUNT"
        tikv_clean "$NODE_COUNT"
        tikv_start "$NODE_COUNT"

        # --- Phase 1: Build index on driver ---
        echo ""
        echo "--- Phase 1: Build index on driver ---"
        local BUILD_INI
        BUILD_INI=$(generate_ini "$SCALE" "$NODE_COUNT" "Rebuild=true" "BuildOnly=true") || exit 1

        # Clean old index dir
        for (( i=0; i<NODE_COUNT; i++ )); do
            local host="${NODE_HOSTS[$i]}"
            remote_exec "$host" "rm -rf $DATA_DIR/proidx_${SCALE}_${NODE_COUNT}node"
        done
        mkdir -p "$DATA_DIR/proidx_${SCALE}_${NODE_COUNT}node"

        BENCHMARK_CONFIG="$BUILD_INI" \
        BENCHMARK_OUTPUT="output_${SCALE}_${NODE_COUNT}node_build.json" \
            "$BINARY" --run_test=SPFreshTest/BenchmarkFromConfig \
            2>&1 | tee "$LOGDIR/benchmark_${SCALE}_${NODE_COUNT}node_build.log"

        echo "Build done: $(date)"

        # --- Phase 2: Distribute data ---
        echo ""
        echo "--- Phase 2: Distribute head index + data ---"
        rm -f "$DATA_DIR/proidx_${SCALE}_${NODE_COUNT}node/spann_index/checkpoint.txt"

        distribute_head_index "$SCALE" "$NODE_COUNT"
        distribute_perftest_files "$NODE_COUNT"

        # Also distribute binary + INI to workers
        for (( i=1; i<NODE_COUNT; i++ )); do
            local host="${NODE_HOSTS[$i]}"
            remote_exec "$host" "mkdir -p $SPTAG_DIR/Release"
            remote_sync "$host" "$SPTAG_DIR/Release/SPTAGTest" "$SPTAG_DIR/Release/SPTAGTest"
        done

        # --- Phase 3: Start driver first (contains dispatcher), then workers ---
        echo ""
        echo "--- Phase 3: Distributed run ---"

        local RUN_INI
        RUN_INI=$(generate_ini "$SCALE" "$NODE_COUNT" "Rebuild=false") || exit 1

        # Start driver in background first — it contains the dispatcher that
        # workers need to connect to for ring registration.
        local DRIVER_LOG="$LOGDIR/benchmark_${SCALE}_${NODE_COUNT}node_driver.log"
        echo "Starting driver (dispatcher+worker0) on ${NODE_HOSTS[0]}..."
        BENCHMARK_CONFIG="$RUN_INI" \
        BENCHMARK_OUTPUT="output_${SCALE}_${NODE_COUNT}node.json" \
            "$BINARY" --run_test=SPFreshTest/BenchmarkFromConfig \
            > "$DRIVER_LOG" 2>&1 &
        local DRIVER_PID=$!
        echo "  Driver PID: $DRIVER_PID"

        # Wait for dispatcher to start listening before launching workers
        local DISP_PORT=30001
        echo "  Waiting for dispatcher to listen on port $DISP_PORT..."
        for attempt in $(seq 1 60); do
            if ss -tlnp 2>/dev/null | grep -q ":${DISP_PORT} " || \
               netstat -tlnp 2>/dev/null | grep -q ":${DISP_PORT} "; then
                echo "  Dispatcher listening (${attempt}s)"
                break
            fi
            if ! kill -0 "$DRIVER_PID" 2>/dev/null; then
                echo "  ERROR: Driver exited prematurely"
                cat "$DRIVER_LOG"
                return 1
            fi
            if [ "$attempt" -eq 60 ]; then
                echo "  WARNING: Dispatcher not detected on port $DISP_PORT after 60s, proceeding anyway"
            fi
            sleep 1
        done

        # Now start remote workers — they can connect to the dispatcher
        WORKER_SSH_PIDS=()
        for (( i=1; i<NODE_COUNT; i++ )); do
            start_remote_worker "$i" "$RUN_INI" "$SCALE" "$NODE_COUNT"
        done

        # Wait for driver to complete (it runs the full benchmark)
        echo "  Waiting for driver to complete..."
        wait "$DRIVER_PID"
        local DRIVER_EXIT=$?
        echo "Driver done (exit=$DRIVER_EXIT): $(date)"
        # Show driver output
        tail -20 "$DRIVER_LOG"

        # Driver sends TCP Stop to workers; wait for graceful exit
        stop_remote_workers 60

        # Collect remote logs
        echo "Collecting remote logs..."
        for (( i=1; i<NODE_COUNT; i++ )); do
            local host="${NODE_HOSTS[$i]}"
            local REMOTE_LOG="$SPTAG_DIR/worker_n${i}.log"
            scp $(_ssh_opts) "$SSH_USER@$host:$REMOTE_LOG" \
                "$LOGDIR/benchmark_${SCALE}_${NODE_COUNT}node_worker${i}_remote.log" 2>/dev/null || true
        done

        tikv_stop "$NODE_COUNT"
    fi

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ${SCALE} ${NODE_COUNT}-node done: $(date)"
    echo "  Results: output_${SCALE}_${NODE_COUNT}node.json"
    echo "  Logs:    $LOGDIR/benchmark_${SCALE}_${NODE_COUNT}node_*.log"
    echo "═══════════════════════════════════════════════════"
}

cmd_bench() {
    # Run 1-node baseline + N-node distributed for each specified scale.
    # Usage: cmd_bench <scale> [scale...]
    # Special scale "all" expands to all scales with templates in configs/.
    local scales=()
    for arg in "$@"; do
        if [ "$arg" = "all" ]; then
            for tmpl in "$SCRIPT_DIR"/configs/benchmark_*_template.ini; do
                local name
                name="$(basename "$tmpl")"
                name="${name#benchmark_}"
                name="${name%_template.ini}"
                scales+=("$name")
            done
        else
            scales+=("$arg")
        fi
    done

    if [ ${#scales[@]} -eq 0 ]; then
        echo "Usage: $0 bench <cluster.conf> <scale> [scale...] | all"
        echo "Available scales:"
        for tmpl in "$SCRIPT_DIR"/configs/benchmark_*_template.ini; do
            local name
            name="$(basename "$tmpl")"
            name="${name#benchmark_}"
            name="${name%_template.ini}"
            echo "  $name"
        done
        exit 1
    fi

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  Benchmark suite: ${scales[*]}"
    echo "  Cluster: $TOTAL_NODES nodes"
    echo "  Start: $(date)"
    echo "═══════════════════════════════════════════════════"

    for scale in "${scales[@]}"; do
        echo ""
        echo "▶▶▶ Scale: $scale — 1-node baseline"
        cmd_run "$scale" 1

        if [ "$TOTAL_NODES" -gt 1 ]; then
            echo ""
            echo "▶▶▶ Scale: $scale — ${TOTAL_NODES}-node distributed"
            cmd_run "$scale" "$TOTAL_NODES"
        else
            echo "  (Skipping multi-node: cluster has only 1 node)"
        fi
    done

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  Benchmark suite complete: $(date)"
    echo "═══════════════════════════════════════════════════"
}

# ─── Cleanup ───

cmd_cleanup() {
    echo ""
    echo "=== Cleaning up remote nodes ==="

    for i in $(seq 1 $((${#NODE_HOSTS[@]} - 1))); do
        local host="${NODE_HOSTS[$i]}"
        echo "  Cleaning $host..."
        remote_exec "$host" "rm -rf $SPTAG_DIR/Release/SPTAGTest $SPTAG_DIR/perftest_* $SPTAG_DIR/worker_*.ini"
        # Clean index directories
        remote_exec "$host" "rm -rf $DATA_DIR/proidx_*"
    done
    echo "Cleanup complete."
}

# ─── Main ───

CMD="$1"
CONF="$2"

if [ -z "$CMD" ] || [ -z "$CONF" ]; then
    echo "Usage: $0 <command> <cluster.conf> [args...]"
    echo ""
    echo "Commands:"
    echo "  deploy      Deploy binary and data to all nodes"
    echo "  start-tikv  Start independent TiKV/PD instances"
    echo "  stop-tikv   Stop TiKV/PD instances"
    echo "  run         Run benchmark: $0 run cluster.conf <scale> <node_count>"
    echo "  bench       Run full benchmark suite: $0 bench cluster.conf <scale> [scale...] | all"
    echo "  cleanup     Remove deployed files from remote nodes"
    exit 1
fi

parse_config "$CONF"

# Trap for cleanup on interrupt
trap 'echo ""; echo "Interrupted!"; stop_remote_workers 5; cmd_stop_tikv; exit 1' INT TERM

case "$CMD" in
    deploy)
        cmd_deploy
        ;;
    start-tikv)
        cmd_start_tikv "${3:-}"
        ;;
    stop-tikv)
        cmd_stop_tikv "${3:-}"
        ;;
    run)
        cmd_run "$3" "$4"
        ;;
    bench)
        shift 2  # skip cmd and conf
        cmd_bench "$@"
        ;;
    cleanup)
        cmd_cleanup
        ;;
    *)
        echo "Unknown command: $CMD"
        echo "Valid commands: deploy, start-tikv, stop-tikv, run, bench, cleanup"
        exit 1
        ;;
esac
