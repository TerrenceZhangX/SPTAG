#!/bin/bash
# Multi-machine distributed benchmark orchestrator for SPTAG.
#
# Usage:
#   ./run_distributed.sh deploy  <cluster.conf>              Deploy binary + data to all nodes
#   ./run_distributed.sh start-tikv <cluster.conf>           Start TiKV/PD cluster across machines
#   ./run_distributed.sh stop-tikv  <cluster.conf>           Stop TiKV/PD cluster
#   ./run_distributed.sh run     <cluster.conf> <scale> ...  Run benchmark (build + distribute + run)
#   ./run_distributed.sh cleanup <cluster.conf>              Remove deployed files from remote nodes
#
# Prerequisites:
#   - Passwordless SSH from driver to all nodes
#   - Docker installed on all nodes (for TiKV)
#   - cluster.conf configured (see cluster.conf.example)
#
# The driver (first node in [nodes]) orchestrates everything.
# Workers receive commands via TCP dispatch protocol.

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPTAG_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOGDIR="$SPTAG_DIR/benchmark_logs"
mkdir -p "$LOGDIR"

# ─── Config Parsing ───

declare -a NODE_HOSTS NODE_ROUTER_PORTS
declare -a TIKV_HOSTS TIKV_PD_CLIENT_PORTS TIKV_PD_PEER_PORTS TIKV_PORTS
declare SSH_USER SPTAG_DIR DATA_DIR TIKV_VERSION PD_VERSION

parse_config() {
    local CONF="$1"
    if [ ! -f "$CONF" ]; then
        echo "ERROR: Config file not found: $CONF"
        exit 1
    fi

    local SECTION=""
    local NODE_IDX=0
    local TIKV_IDX=0

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
    TIKV_VERSION="${TIKV_VERSION:-v7.5.1}"
    PD_VERSION="${PD_VERSION:-v7.5.1}"

    if [ ${#NODE_HOSTS[@]} -lt 1 ]; then
        echo "ERROR: No compute nodes defined in [nodes]"
        exit 1
    fi
    if [ ${#TIKV_HOSTS[@]} -lt 1 ]; then
        echo "ERROR: No TiKV instances defined in [tikv]"
        exit 1
    fi

    echo "Cluster config loaded:"
    echo "  Compute nodes: ${#NODE_HOSTS[@]} (driver: ${NODE_HOSTS[0]})"
    echo "  TiKV instances: ${#TIKV_HOSTS[@]}"
    echo "  SSH user: $SSH_USER"
    echo "  SPTAG dir: $SPTAG_DIR"
    echo "  Data dir: $DATA_DIR"
}

# ─── SSH Helpers ───

# Run command on remote host (or locally if it's the driver)
remote_exec() {
    local host="$1"; shift
    if [ "$host" = "${NODE_HOSTS[0]}" ] || [ "$host" = "localhost" ] || [ "$host" = "127.0.0.1" ]; then
        eval "$@"
    else
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$SSH_USER@$host" "$@"
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
        rsync -az --progress -e "ssh -o StrictHostKeyChecking=no" "$src" "$SSH_USER@$host:$dst"
    fi
}

# ─── Computed Helpers ───

# Build RouterNodeAddrs string: host1:port1,host2:port2,...
get_router_node_addrs() {
    local addrs=""
    for i in "${!NODE_HOSTS[@]}"; do
        [ -n "$addrs" ] && addrs+=","
        addrs+="${NODE_HOSTS[$i]}:${NODE_ROUTER_PORTS[$i]}"
    done
    echo "$addrs"
}

# Build TiKVPDAddresses string: host1:port1,host2:port2,...
get_tikv_pd_addrs() {
    local addrs=""
    for i in "${!TIKV_HOSTS[@]}"; do
        [ -n "$addrs" ] && addrs+=","
        addrs+="${TIKV_HOSTS[$i]}:${TIKV_PD_CLIENT_PORTS[$i]}"
    done
    echo "$addrs"
}

# Build RouterNodeStores string: host1:port1,host2:port2,...
get_router_node_stores() {
    local addrs=""
    for i in "${!TIKV_HOSTS[@]}"; do
        [ -n "$addrs" ] && addrs+=","
        addrs+="${TIKV_HOSTS[$i]}:${TIKV_PORTS[$i]}"
    done
    echo "$addrs"
}

# PD initial cluster string for bootstrap: pd0=http://host:peer_port,...
get_pd_initial_cluster() {
    local cluster=""
    for i in "${!TIKV_HOSTS[@]}"; do
        [ -n "$cluster" ] && cluster+=","
        cluster+="pd${i}=http://${TIKV_HOSTS[$i]}:${TIKV_PD_PEER_PORTS[$i]}"
    done
    echo "$cluster"
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
        # Use rsync with include/exclude to only send perftest_* files
        rsync -az --progress \
            --include='perftest_*' --exclude='*' \
            -e "ssh -o StrictHostKeyChecking=no" \
            "$SPTAG_DIR/" "$SSH_USER@$host:$SPTAG_DIR/"
    done

    echo ""
    echo "Deploy complete."
}

# ─── TiKV Management ───

cmd_start_tikv() {
    echo ""
    echo "=== Starting TiKV cluster (${#TIKV_HOSTS[@]} instances) ==="

    local PD_ENDPOINTS=$(get_tikv_pd_addrs)
    local INITIAL_CLUSTER=$(get_pd_initial_cluster)

    # Start PD instances
    echo "Starting PD instances..."
    for i in "${!TIKV_HOSTS[@]}"; do
        local host="${TIKV_HOSTS[$i]}"
        local client_port="${TIKV_PD_CLIENT_PORTS[$i]}"
        local peer_port="${TIKV_PD_PEER_PORTS[$i]}"
        echo "  PD $i on $host:$client_port"

        remote_exec "$host" "docker rm -f sptag-pd-$i 2>/dev/null; \
            docker run -d --name sptag-pd-$i --net host \
            -v $DATA_DIR/tikv-data/pd-$i:/data \
            pingcap/pd:${PD_VERSION} \
            --name=pd${i} \
            --data-dir=/data \
            --client-urls=http://0.0.0.0:${client_port} \
            --advertise-client-urls=http://${host}:${client_port} \
            --peer-urls=http://0.0.0.0:${peer_port} \
            --advertise-peer-urls=http://${host}:${peer_port} \
            --initial-cluster=${INITIAL_CLUSTER}"
    done

    echo "Waiting for PD cluster to elect leader..."
    sleep 5

    # Check PD health
    local pd_host="${TIKV_HOSTS[0]}"
    local pd_port="${TIKV_PD_CLIENT_PORTS[0]}"
    for attempt in $(seq 1 30); do
        if curl -sf "http://${pd_host}:${pd_port}/pd/api/v1/members" >/dev/null 2>&1; then
            echo "  PD cluster healthy"
            break
        fi
        if [ "$attempt" -eq 30 ]; then
            echo "  ERROR: PD cluster not healthy after 30s"
            return 1
        fi
        sleep 1
    done

    # Start TiKV instances
    echo "Starting TiKV instances..."
    for i in "${!TIKV_HOSTS[@]}"; do
        local host="${TIKV_HOSTS[$i]}"
        local tikv_port="${TIKV_PORTS[$i]}"
        echo "  TiKV $i on $host:$tikv_port"

        remote_exec "$host" "docker rm -f sptag-tikv-$i 2>/dev/null; \
            docker run -d --name sptag-tikv-$i --net host \
            --ulimit nofile=1048576:1048576 \
            -v $DATA_DIR/tikv-data/tikv-$i:/data \
            pingcap/tikv:${TIKV_VERSION} \
            --addr=0.0.0.0:${tikv_port} \
            --advertise-addr=${host}:${tikv_port} \
            --data-dir=/data \
            --pd=${PD_ENDPOINTS}"
    done

    echo "Waiting for TiKV stores to register..."
    sleep 5

    # Check TiKV store count
    for attempt in $(seq 1 60); do
        local store_count
        store_count=$(curl -sf "http://${pd_host}:${pd_port}/pd/api/v1/stores" 2>/dev/null \
            | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null || echo 0)
        if [ "$store_count" -ge "${#TIKV_HOSTS[@]}" ]; then
            echo "  All ${store_count} TiKV stores registered"
            break
        fi
        if [ "$attempt" -eq 60 ]; then
            echo "  WARNING: Only $store_count/${#TIKV_HOSTS[@]} stores registered after 60s"
        fi
        sleep 1
    done

    echo "TiKV cluster started."
}

cmd_stop_tikv() {
    echo ""
    echo "=== Stopping TiKV cluster ==="

    for i in "${!TIKV_HOSTS[@]}"; do
        local host="${TIKV_HOSTS[$i]}"
        echo "  Stopping TiKV $i and PD $i on $host..."
        remote_exec "$host" "docker rm -f sptag-tikv-$i sptag-pd-$i 2>/dev/null || true"
    done

    echo "TiKV cluster stopped."
}

cmd_clean_tikv_data() {
    echo ""
    echo "=== Cleaning TiKV data ==="

    for i in "${!TIKV_HOSTS[@]}"; do
        local host="${TIKV_HOSTS[$i]}"
        echo "  Cleaning TiKV data on $host..."
        remote_exec "$host" "rm -rf $DATA_DIR/tikv-data/tikv-$i $DATA_DIR/tikv-data/pd-$i"
    done
}

# ─── INI Generation ───

generate_ini() {
    # Generate a benchmark INI file for a specific node and scale.
    # Usage: generate_ini <scale> <node_index> <extra_overrides...>
    local SCALE="$1"
    local NODE_IDX="$2"
    shift 2

    local ROUTER_ADDRS=$(get_router_node_addrs)
    local PD_ADDRS=$(get_tikv_pd_addrs)
    local STORE_ADDRS=$(get_router_node_stores)
    local NUM_NODES=${#NODE_HOSTS[@]}
    local HOST="${NODE_HOSTS[$NODE_IDX]}"
    local IDX_PATH="$DATA_DIR/proidx_${SCALE}_${NUM_NODES}node_n${NODE_IDX}/spann_index"

    # Load the base INI template if it exists
    local BASE_INI="$SCRIPT_DIR/configs/benchmark_${SCALE}_template.ini"
    if [ ! -f "$BASE_INI" ]; then
        echo "ERROR: Template INI not found: $BASE_INI"
        echo "Create a template or use an existing single-node INI as base."
        return 1
    fi

    # Generate node-specific INI
    local OUT="/tmp/benchmark_${SCALE}_${NUM_NODES}node_n${NODE_IDX}.ini"
    cp "$BASE_INI" "$OUT"

    # Rewrite key fields
    sed -i "s|^IndexPath=.*|IndexPath=${IDX_PATH}|" "$OUT"
    sed -i "s|^TiKVPDAddresses=.*|TiKVPDAddresses=${PD_ADDRS}|" "$OUT"

    # Ensure [Router] section exists
    if ! grep -q '^\[Router\]' "$OUT"; then
        echo "" >> "$OUT"
        echo "[Router]" >> "$OUT"
    fi

    # Set router params (remove old values first, then append)
    sed -i '/^RouterEnabled=/d; /^RouterLocalNodeIndex=/d; /^RouterNodeAddrs=/d; /^RouterNodeStores=/d' "$OUT"
    sed -i "/^\[Router\]/a RouterEnabled=true\nRouterLocalNodeIndex=${NODE_IDX}\nRouterNodeAddrs=${ROUTER_ADDRS}\nRouterNodeStores=${STORE_ADDRS}" "$OUT"

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
    # The SSH connection stays open — killing the local SSH PID kills the remote process.
    local NODE_IDX="$1"
    local INI="$2"
    local SCALE="$3"
    local host="${NODE_HOSTS[$NODE_IDX]}"
    local NUM_NODES=${#NODE_HOSTS[@]}
    local LOG="$LOGDIR/benchmark_${SCALE}_${NUM_NODES}node_worker${NODE_IDX}.log"

    # Copy INI to remote
    remote_sync "$host" "$INI" "$SPTAG_DIR/worker_n${NODE_IDX}.ini"

    # Start worker via SSH (foreground on remote, background locally)
    ssh -o StrictHostKeyChecking=no "$SSH_USER@$host" \
        "cd $SPTAG_DIR && BENCHMARK_CONFIG=worker_n${NODE_IDX}.ini \
         ./Release/SPTAGTest --run_test=SPFreshTest/WorkerNode 2>&1" \
        > "$LOG" 2>&1 &
    local ssh_pid=$!
    WORKER_SSH_PIDS+=($ssh_pid)
    echo "  Worker n${NODE_IDX} on $host (SSH PID: $ssh_pid, log: $LOG)"
}

wait_workers_ready() {
    local SCALE="$1"
    local NUM_NODES=${#NODE_HOSTS[@]}
    local TIMEOUT=120

    echo "Waiting for ${#WORKER_SSH_PIDS[@]} workers to be ready..."
    for attempt in $(seq 1 $TIMEOUT); do
        local all_ready=true
        for i in $(seq 1 $((NUM_NODES - 1))); do
            local LOG="$LOGDIR/benchmark_${SCALE}_${NUM_NODES}node_worker${i}.log"
            if ! grep -q "WorkerNode.*Ready" "$LOG" 2>/dev/null; then
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
    # Copy the head index from driver (n0) to all worker nodes.
    local SCALE="$1"
    local NUM_NODES=${#NODE_HOSTS[@]}
    local SRC="$DATA_DIR/proidx_${SCALE}_${NUM_NODES}node_n0/spann_index"

    echo "Distributing head index to ${NUM_NODES}-1 workers..."
    for i in $(seq 1 $((NUM_NODES - 1))); do
        local host="${NODE_HOSTS[$i]}"
        local DST="$DATA_DIR/proidx_${SCALE}_${NUM_NODES}node_n${i}/spann_index"
        echo "  → n${i} ($host)"
        remote_exec "$host" "mkdir -p $DST"
        remote_sync "$host" "$SRC/" "$DST/"
    done
}

distribute_perftest_files() {
    # rsync generated perftest_* files from driver to all workers.
    local host
    echo "Distributing perftest_* data files to workers..."
    for i in $(seq 1 $((${#NODE_HOSTS[@]} - 1))); do
        host="${NODE_HOSTS[$i]}"
        echo "  → $host"
        rsync -az --progress \
            --include='perftest_*' --exclude='*' \
            -e "ssh -o StrictHostKeyChecking=no" \
            "$SPTAG_DIR/" "$SSH_USER@$host:$SPTAG_DIR/"
    done
}

cmd_run() {
    local SCALE="$1"
    if [ -z "$SCALE" ]; then
        echo "Usage: $0 run <cluster.conf> <scale>"
        exit 1
    fi

    local NUM_NODES=${#NODE_HOSTS[@]}
    local BINARY="$SPTAG_DIR/Release/SPTAGTest"

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ${SCALE}: ${NUM_NODES}-node distributed benchmark"
    echo "  Start: $(date)"
    echo "═══════════════════════════════════════════════════"

    # --- Phase 1: Build index on driver (router disabled) ---
    echo ""
    echo "--- Phase 1: Build index on driver ---"

    local BUILD_INI
    BUILD_INI=$(generate_ini "$SCALE" 0 "Rebuild=true" "RouterEnabled=false") || exit 1

    # Clean old index dirs
    for i in $(seq 0 $((NUM_NODES - 1))); do
        local host="${NODE_HOSTS[$i]}"
        remote_exec "$host" "rm -rf $DATA_DIR/proidx_${SCALE}_${NUM_NODES}node_n${i}"
    done
    mkdir -p "$DATA_DIR/proidx_${SCALE}_${NUM_NODES}node_n0"

    BENCHMARK_CONFIG="$BUILD_INI" \
    BENCHMARK_OUTPUT="output_${SCALE}_${NUM_NODES}node_build.json" \
        "$BINARY" --run_test=SPFreshTest/BenchmarkFromConfig \
        2>&1 | tee "$LOGDIR/benchmark_${SCALE}_${NUM_NODES}node_build.log"

    echo "Build done: $(date)"

    # --- Phase 2: Distribute data ---
    echo ""
    echo "--- Phase 2: Distribute head index + data ---"

    # Clear checkpoint so driver re-runs insert batches with routing
    rm -f "$DATA_DIR/proidx_${SCALE}_${NUM_NODES}node_n0/spann_index/checkpoint.txt"

    distribute_head_index "$SCALE"
    distribute_perftest_files

    # --- Phase 3: Start workers, then run driver ---
    echo ""
    echo "--- Phase 3: Distributed run ---"

    # Generate per-node INI files
    WORKER_SSH_PIDS=()
    for i in $(seq 1 $((NUM_NODES - 1))); do
        local WORKER_INI
        WORKER_INI=$(generate_ini "$SCALE" "$i" "Rebuild=false") || exit 1
        start_remote_worker "$i" "$WORKER_INI" "$SCALE"
    done

    wait_workers_ready "$SCALE" || {
        echo "ERROR: Workers not ready, aborting"
        stop_remote_workers 5
        return 1
    }

    # Run driver with routing enabled, rebuild disabled
    local DRIVER_INI
    DRIVER_INI=$(generate_ini "$SCALE" 0 "Rebuild=false") || exit 1

    echo ""
    echo "Starting driver on ${NODE_HOSTS[0]}..."
    BENCHMARK_CONFIG="$DRIVER_INI" \
    BENCHMARK_OUTPUT="output_${SCALE}_${NUM_NODES}node.json" \
        "$BINARY" --run_test=SPFreshTest/BenchmarkFromConfig \
        2>&1 | tee "$LOGDIR/benchmark_${SCALE}_${NUM_NODES}node_driver.log"

    echo "Driver done: $(date)"

    # Driver sends TCP Stop to workers; wait for graceful exit
    stop_remote_workers 60

    # Collect remote logs
    echo "Collecting remote logs..."
    for i in $(seq 1 $((NUM_NODES - 1))); do
        local host="${NODE_HOSTS[$i]}"
        local REMOTE_LOG="$SPTAG_DIR/worker_n${i}.log"
        scp -o StrictHostKeyChecking=no "$SSH_USER@$host:$REMOTE_LOG" \
            "$LOGDIR/benchmark_${SCALE}_${NUM_NODES}node_worker${i}_remote.log" 2>/dev/null || true
    done

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ${SCALE} ${NUM_NODES}-node done: $(date)"
    echo "  Results: output_${SCALE}_${NUM_NODES}node.json"
    echo "  Logs:    $LOGDIR/benchmark_${SCALE}_${NUM_NODES}node_*.log"
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
    echo "  start-tikv  Start TiKV/PD cluster"
    echo "  stop-tikv   Stop TiKV/PD cluster"
    echo "  run         Run benchmark: $0 run cluster.conf <scale>"
    echo "  cleanup     Remove deployed files from remote nodes"
    exit 1
fi

parse_config "$CONF"

# Trap for cleanup on interrupt
trap 'echo ""; echo "Interrupted!"; stop_remote_workers 5; exit 1' INT TERM

case "$CMD" in
    deploy)
        cmd_deploy
        ;;
    start-tikv)
        cmd_start_tikv
        ;;
    stop-tikv)
        cmd_stop_tikv
        ;;
    run)
        shift 2  # skip cmd and conf
        for scale in "$@"; do
            cmd_run "$scale"
        done
        ;;
    cleanup)
        cmd_cleanup
        ;;
    *)
        echo "Unknown command: $CMD"
        echo "Valid commands: deploy, start-tikv, stop-tikv, run, cleanup"
        exit 1
        ;;
esac
