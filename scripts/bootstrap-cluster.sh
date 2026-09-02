#!/usr/bin/env bash
# Stand up a three-node multi-master cluster on this host, natively, in one command.
#
# What this is for: an operator or an evaluator who wants a working cluster to talk to. It is not a
# debugging tool — `scripts/mm_harness.py` is that, and it kills nodes, blocks links and counts rows
# to reproduce specific defects. This script's job is finished the moment the mesh is up and it has
# told you how to connect.
#
# Native processes, no container. Every node gets its own port set, its own data directory and its
# own log file, so the three do not collide and a failure names which one.
#
# **Single host only, deliberately.** The roadmap item also asks for "across hosts over SSH", and
# that half is a documented procedure in docs/operations.md rather than a script: it could not be
# verified on the machine this was written on — sshd is installed but inactive and no key is set up,
# and standing one up would be a change to the developer's system rather than a test. A deployment
# script nobody has run is worse than a procedure someone has read.
set -euo pipefail

BASE_PORT="${BASE_PORT:-19090}"
STATE_DIR="${STATE_DIR:-${TMPDIR:-/tmp}/ob-cluster}"
NODES="${NODES:-3}"
SERVER="${OB_SERVER_BINARY:-}"
ETCD="${OB_ETCD_BINARY:-$(command -v etcd || true)}"
READY_TIMEOUT="${READY_TIMEOUT:-45}"

die() { echo "error: $*" >&2; exit 1; }
note() { echo "  $*"; }

# ── Prerequisites, named before anything is started ──────────────────────────
if [ -z "$SERVER" ]; then
    for candidate in /usr/bin/ob_tcp_server ./build/ob_tcp_server ./build-release/ob_tcp_server; do
        [ -x "$candidate" ] && { SERVER="$candidate"; break; }
    done
fi
[ -n "$SERVER" ] && [ -x "$SERVER" ] || die "no ob_tcp_server. Install the package, build the tree, or set OB_SERVER_BINARY."
[ -n "$ETCD" ] && [ -x "$ETCD" ] || die "no etcd on PATH. Multi-master needs one for peer discovery; set OB_ETCD_BINARY if it lives elsewhere."

echo "cluster: $NODES nodes, ports from $BASE_PORT, state in $STATE_DIR"
note "server: $SERVER"
note "etcd:   $ETCD"

case "${1:-}" in
  stop|down)
    if [ -f "$STATE_DIR/pids" ]; then
        pids=$(cat "$STATE_DIR/pids")
        for pid in $pids; do kill "$pid" 2>/dev/null || true; done

        # Wait, and say so if they do not go. The first version printed "stopped" and returned
        # immediately, while all three nodes were still running — a graceful shutdown drains and
        # flushes, so SIGTERM is a request rather than an event. A script that reports a state it
        # has not confirmed is the defect this repository keeps finding in its own code.
        deadline=$(( $(date +%s) + 15 ))
        while :; do
            alive=""
            for pid in $pids; do kill -0 "$pid" 2>/dev/null && alive="$alive $pid"; done
            [ -z "$alive" ] && break
            if [ "$(date +%s)" -ge "$deadline" ]; then
                echo "warning: still running after 15s:$alive — sending SIGKILL." >&2
                echo "         A node that will not drain in fifteen seconds has something to say;" >&2
                echo "         its log is in $STATE_DIR/node*/node.log." >&2
                for pid in $alive; do kill -9 "$pid" 2>/dev/null || true; done
                break
            fi
            sleep 0.5
        done
        rm -f "$STATE_DIR/pids"
        echo "stopped. State is left in $STATE_DIR — it holds the databases, so removing it is your call."
    else
        echo "nothing recorded as running in $STATE_DIR"
    fi
    exit 0
    ;;
esac

[ -f "$STATE_DIR/pids" ] && die "$STATE_DIR/pids exists; something may still be running. Run '$0 stop' first."

mkdir -p "$STATE_DIR"
: > "$STATE_DIR/pids"

ETCD_CLIENT=$((BASE_PORT + 100))
ETCD_PEER=$((BASE_PORT + 101))

# ── etcd ─────────────────────────────────────────────────────────────────────
rm -rf "$STATE_DIR/etcd"
mkdir -p "$STATE_DIR/etcd"
"$ETCD" --name ob-bootstrap \
        --data-dir "$STATE_DIR/etcd/data" \
        --listen-client-urls "http://127.0.0.1:$ETCD_CLIENT" \
        --advertise-client-urls "http://127.0.0.1:$ETCD_CLIENT" \
        --listen-peer-urls "http://127.0.0.1:$ETCD_PEER" \
        --initial-advertise-peer-urls "http://127.0.0.1:$ETCD_PEER" \
        --initial-cluster "ob-bootstrap=http://127.0.0.1:$ETCD_PEER" \
        --initial-cluster-state new \
        > "$STATE_DIR/etcd.log" 2>&1 &
echo $! >> "$STATE_DIR/pids"

deadline=$(( $(date +%s) + 30 ))
until curl -sf "http://127.0.0.1:$ETCD_CLIENT/version" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || die "etcd did not answer within 30s; see $STATE_DIR/etcd.log"
    sleep 0.5
done
note "etcd ready on 127.0.0.1:$ETCD_CLIENT"

# ── Nodes ────────────────────────────────────────────────────────────────────
#
# A configuration file per node rather than a command line, so the thing an operator ends up editing
# on a real host is the thing this script writes here. `--config` plus one flag is also what the
# systemd unit does.
for i in $(seq 1 "$NODES"); do
    tcp=$((BASE_PORT + (i - 1) * 10))
    mm=$((tcp + 1))
    metrics=$((tcp + 2))
    dir="$STATE_DIR/node$i"
    mkdir -p "$dir"

    cat > "$dir/ob.conf" <<CONF
# Node $i of a local $NODES-node multi-master cluster, written by scripts/bootstrap-cluster.sh.
port                  = $tcp
data-dir              = $dir/data
metrics-port          = $metrics
log-level             = INFO
node-id               = node-$i
coordinator-endpoints = http://127.0.0.1:$ETCD_CLIENT
multi-master          = true
mm-node-id            = $i
mm-replication-port   = $mm
CONF

    "$SERVER" --config "$dir/ob.conf" > "$dir/node.log" 2>&1 &
    echo $! >> "$STATE_DIR/pids"
    note "node-$i: client $tcp, peer $mm, metrics $metrics, log $dir/node.log"
done

# ── Wait for the mesh, and say what "ready" means ────────────────────────────
#
# Readiness is every node answering MM_PEERS with the other two. A node that is merely listening is
# not a member: it can accept a write and have nobody to send it to, which is the state this wait
# exists to keep an operator out of.
expected=$((NODES - 1))
deadline=$(( $(date +%s) + READY_TIMEOUT ))
while :; do
    ready=0
    for i in $(seq 1 "$NODES"); do
        tcp=$((BASE_PORT + (i - 1) * 10))
        # Count rows that say `connected`, not rows mentioning `node_id`. The first version counted
        # the latter and always got 1: `node_id` appears in the *header* line and never in a peer
        # row, which carries values. Counting `connected` is also the stronger condition — #84 made
        # MM_PEERS list connections still in their handshake, and a peer that is listed but not
        # connected cannot receive a write.
        peers=$( (printf 'MM_PEERS\nQUIT\n'; sleep 0.4) | timeout 5 nc -q1 127.0.0.1 "$tcp" 2>/dev/null | grep -c "connected" || true)
        [ "$peers" -ge "$expected" ] && ready=$((ready + 1))
    done
    [ "$ready" -eq "$NODES" ] && break
    if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "error: only $ready of $NODES nodes see $expected peers after ${READY_TIMEOUT}s." >&2
        echo "       Logs: $STATE_DIR/node*/node.log — the peer discovery chain is" >&2
        echo "       etcd -> PeerRegistry::start_watch -> handle_topology_change -> connect_to_peer" >&2
        echo "       -> send_handshake, and breaking any link there fails quietly." >&2
        exit 1
    fi
    sleep 1
done

cat <<DONE

cluster ready: $NODES nodes, every one seeing $expected peers.

  write to any node   — they all accept writes; conflicts resolve by HLC and last-writer-wins
  read from any node

    printf 'INSERT BTCUSDT binance bid 100000 5\nQUIT\n' | nc 127.0.0.1 $BASE_PORT
    printf "SELECT * FROM 'BTCUSDT'.'binance'\nQUIT\n" | nc 127.0.0.1 $((BASE_PORT + 10))

  metrics             http://127.0.0.1:$((BASE_PORT + 2))/metrics
  logs                $STATE_DIR/node*/node.log
  stop                $0 stop

The wire protocol has no authentication or encryption (roadmap #30), so this is bound to 127.0.0.1
and belongs on a machine you trust.
DONE
