#!/usr/bin/env python3
"""
Binance Live Failover Test — stream real data, kill a node, restart, verify & plot.

This is the "ultimate" integration test: real market data + multi-master failover
+ visual verification via plot.

Sequence:
  1. Start 2-node MM cluster (native etcd + 2 ob_tcp_server --multi-master)
  2. Connect to Binance WebSocket depth stream (btcusdt@depth)
  3. Phase A (~20s): stream data to both nodes alternately
  4. Phase B: KILL node 1 (SIGKILL)
  5. Phase C (~20s): continue streaming to node 0 only
  6. Phase D: RESTART node 1, wait for re-sync
  7. Phase E (~20s): stream to both nodes again
  8. Query both nodes, compare state, generate plot

The plot shows best bid/ask over time with vertical lines marking kill/restart events.
Saved to /tmp/btc_failover_test.png

Usage:
    python3 scripts/binance_failover_test.py [--phase-duration 20] [--output /tmp/btc_failover_test.png]

Exit codes:
    0 — test passed (both nodes converged after failover)
    1 — test failed (nodes diverged or other error)

Requirements: native etcd binary on PATH (or OB_ETCD_BINARY), compiled ob_tcp_server,
internet access, websockets, matplotlib
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── Configuration ──────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVER_BINARY = str(PROJECT_ROOT / "build" / "ob_tcp_server")
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@depth"
SYMBOL = "BTCUSDT"
EXCHANGE = "BINANCE"
PEER_DISCOVERY_WAIT = 3.0


# ── ANSI Colors ────────────────────────────────────────────────────────────────

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
BOLD = "\033[1m"
RESET = "\033[0m"


def info(msg: str):
    print(f"{GREEN}▶{RESET} {msg}")


def warn(msg: str):
    print(f"{YELLOW}⚠{RESET} {msg}")


def error(msg: str):
    print(f"{RED}✗{RESET} {msg}")


def success(msg: str):
    print(f"{GREEN}✓{RESET} {msg}")


# ── TCP Client ─────────────────────────────────────────────────────────────────

class TCPClient:
    """Minimal TCP client for ob_tcp_server wire protocol."""

    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._buf = b""
        self._connect()

    def _connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self._timeout)
        self._sock.connect((self._host, self._port))
        self._buf = b""
        self._read_banner()

    def _read_banner(self):
        while True:
            decoded = self._buf.decode("utf-8", errors="replace")
            end = decoded.find("\n\n")
            if end != -1:
                consumed = decoded[:end + 2].encode("utf-8")
                self._buf = self._buf[len(consumed):]
                return
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed before banner")
            self._buf += chunk

    def execute(self, command: str) -> str:
        if not command.endswith("\n"):
            command += "\n"
        self._sock.sendall(command.encode("utf-8"))
        return self._recv_response()

    def _recv_response(self) -> str:
        while True:
            decoded = self._buf.decode("utf-8", errors="replace")
            if decoded.startswith("ERR "):
                nl = decoded.find("\n")
                if nl != -1:
                    resp = decoded[:nl + 1]
                    self._buf = self._buf[len(resp.encode("utf-8")):]
                    return resp
            if decoded.startswith("PONG"):
                nl = decoded.find("\n")
                if nl != -1:
                    resp = decoded[:nl + 1]
                    self._buf = self._buf[len(resp.encode("utf-8")):]
                    return resp
            if any(decoded.startswith(r) for r in
                   ("PRIMARY", "REPLICA", "STANDALONE", "MULTI_MASTER")):
                nl = decoded.find("\n")
                if nl != -1:
                    resp = decoded[:nl + 1]
                    self._buf = self._buf[len(resp.encode("utf-8")):]
                    return resp
            if decoded.startswith("OK"):
                pos = decoded.find("\n\n")
                if pos != -1:
                    resp = decoded[:pos + 2]
                    self._buf = self._buf[len(resp.encode("utf-8")):]
                    return resp
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("TCP connection closed by server")
            self._buf += chunk

    def close(self):
        if self._sock:
            try:
                self._sock.sendall(b"QUIT\n")
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None


# ── Cluster Manager ────────────────────────────────────────────────────────────

@dataclass
class NodeInfo:
    index: int
    process: Optional[subprocess.Popen]
    tcp_port: int
    replication_port: int
    metrics_port: int
    data_dir: str
    node_id: str


class MMCluster:
    """2-node multi-master cluster with kill/restart support."""

    def __init__(self):
        self.etcd_client_port: int = 0
        self.etcd_peer_port: int = 0
        self.etcd_binary: str = os.environ.get("OB_ETCD_BINARY") or "etcd"
        self._logs: list = []
        self.etcd_data_dir: str = ""
        self.etcd_process = None
        self._etcd_log = None
        self.nodes: List[NodeInfo] = []
        self.temp_dirs: List[str] = []

    def start(self):
        self._check_prerequisites()
        self._start_etcd()
        self._wait_for_etcd()

        for i in range(2):
            node = self._start_node(i)
            self.nodes.append(node)
            self._wait_for_node(node)

        info(f"Waiting {PEER_DISCOVERY_WAIT}s for peer discovery...")
        time.sleep(PEER_DISCOVERY_WAIT)

    def kill_node(self, index: int):
        node = self.nodes[index]
        if node.process and node.process.poll() is None:
            node.process.kill()
            node.process.wait(timeout=5)
        warn(f"Node {index} killed (port {node.tcp_port})")

    def restart_node(self, index: int):
        old = self.nodes[index]
        if old.process and old.process.poll() is None:
            old.process.kill()
            old.process.wait(timeout=5)

        etcd_url = f"http://127.0.0.1:{self.etcd_client_port}"
        cmd = [
            SERVER_BINARY,
            "--port", str(old.tcp_port),
            "--data-dir", old.data_dir,
            "--metrics-port", str(old.metrics_port),
            "--coordinator-endpoints", etcd_url,
            "--node-id", old.node_id,
            "--multi-master",
            "--mm-node-id", str(old.index + 1),
            "--mm-replication-port", str(old.replication_port),
            "--anti-entropy-interval-seconds", "1",
            "--log-level", "WARN",
        ]
        log = self._open_log(old.data_dir)
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        self.nodes[index] = NodeInfo(
            index=old.index, process=proc, tcp_port=old.tcp_port,
            replication_port=old.replication_port, metrics_port=old.metrics_port,
            data_dir=old.data_dir, node_id=old.node_id,
        )
        self._wait_for_node(self.nodes[index])
        info(f"Node {index} restarted (port {old.tcp_port})")

    def shutdown(self):
        for node in self.nodes:
            try:
                if node.process and node.process.poll() is None:
                    node.process.send_signal(signal.SIGTERM)
                    try:
                        node.process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        node.process.kill()
                        node.process.wait(timeout=5)
            except Exception:
                pass
        # After the nodes are gone: a live node writing into a closed handle gets EBADF.
        for log in self._logs:
            try:
                log.close()
            except Exception:
                pass
        self._logs = []
        try:
            self._stop_etcd()
        except Exception:
            pass
        for d in self.temp_dirs:
            shutil.rmtree(d, ignore_errors=True)

    # ── Internal ──────────────────────────────────────────────────

    def _check_prerequisites(self):
        if not os.path.isfile(SERVER_BINARY):
            raise RuntimeError(f"Server binary not found: {SERVER_BINARY}\nBuild first.")
        resolved = shutil.which(self.etcd_binary)
        if resolved is None:
            raise RuntimeError(
                f"etcd binary not found: '{self.etcd_binary}'. Install it natively "
                "(see docs/cli.md) or set OB_ETCD_BINARY."
            )
        self.etcd_binary = resolved

    def _start_etcd(self):
        """Start etcd as a native process. No containers anywhere in this harness."""
        self.etcd_client_port = self._find_free_port()
        self.etcd_peer_port = self._find_free_port()
        self.etcd_data_dir = tempfile.mkdtemp(prefix="ob_etcd_live_")
        self.temp_dirs.append(self.etcd_data_dir)
        self._etcd_log = open(os.path.join(self.etcd_data_dir, "etcd.log"), "wb")

        cmd = [
            self.etcd_binary,
            "--name", "ob-live-etcd",
            "--data-dir", os.path.join(self.etcd_data_dir, "data"),
            "--advertise-client-urls", f"http://127.0.0.1:{self.etcd_client_port}",
            "--listen-client-urls", f"http://127.0.0.1:{self.etcd_client_port}",
            "--listen-peer-urls", f"http://127.0.0.1:{self.etcd_peer_port}",
            "--initial-advertise-peer-urls", f"http://127.0.0.1:{self.etcd_peer_port}",
            "--initial-cluster", f"ob-live-etcd=http://127.0.0.1:{self.etcd_peer_port}",
            "--initial-cluster-state", "new",
        ]
        self.etcd_process = subprocess.Popen(
            cmd, stdout=self._etcd_log, stderr=subprocess.STDOUT,
        )
        time.sleep(0.2)
        if self.etcd_process.poll() is not None:
            raise RuntimeError(
                f"etcd exited immediately with code {self.etcd_process.returncode}; "
                f"see {self.etcd_data_dir}/etcd.log"
            )

    def _stop_etcd(self):
        proc = getattr(self, "etcd_process", None)
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        self.etcd_process = None
        log = getattr(self, "_etcd_log", None)
        if log is not None:
            try:
                log.close()
            except OSError:
                pass
            self._etcd_log = None

    def _wait_for_etcd(self, timeout: float = 30.0):
        url = f"http://127.0.0.1:{self.etcd_client_port}/v3/maintenance/status"
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                req = urllib.request.Request(url, data=b"{}", method="POST")
                req.add_header("Content-Type", "application/json")
                resp = urllib.request.urlopen(req, timeout=2)
                if resp.status == 200:
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError("etcd not ready")

    def _start_node(self, index: int) -> NodeInfo:
        tcp_port = self._find_free_port()
        repl_port = self._find_free_port()
        metrics_port = self._find_free_port()
        data_dir = tempfile.mkdtemp(prefix=f"ob_failover_live_{index}_")
        self.temp_dirs.append(data_dir)
        node_id = f"failover-live-node-{index}"
        etcd_url = f"http://127.0.0.1:{self.etcd_client_port}"

        cmd = [
            SERVER_BINARY,
            "--port", str(tcp_port),
            "--data-dir", data_dir,
            "--metrics-port", str(metrics_port),
            "--coordinator-endpoints", etcd_url,
            "--node-id", node_id,
            "--multi-master",
            "--mm-node-id", str(index + 1),
            "--mm-replication-port", str(repl_port),
            "--anti-entropy-interval-seconds", "1",
            "--log-level", "WARN",
        ]
        log = self._open_log(data_dir)
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        return NodeInfo(index=index, process=proc, tcp_port=tcp_port,
                        replication_port=repl_port, metrics_port=metrics_port,
                        data_dir=data_dir, node_id=node_id)

    def _open_log(self, data_dir: str):
        """A node's log as a file, kept open until shutdown.

        Not `subprocess.PIPE`: nothing here reads one, and a pipe nobody drains fills at 64 KB and
        stops the node inside `write()`. `--log-level WARN` below makes that slow to arrive, not
        impossible — and this script runs against a live feed for minutes. A failover script that
        freezes the node whose failover it measures reports a false result, which is worse than a
        hang. The integration fixture had the same defect at DEBUG level, where it arrived in
        seconds; see `open_node_log()` in `tests/integration/conftest.py`.
        """
        log = open(os.path.join(data_dir, "node.log"), "w", encoding="utf-8", buffering=1)
        self._logs.append(log)
        return log

    def _wait_for_node(self, node: NodeInfo, timeout: float = 15.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", node.tcp_port), timeout=2) as s:
                    s.recv(4096)
                    s.sendall(b"PING\n")
                    data = s.recv(1024)
                    if b"PONG" in data:
                        return
            except Exception:
                pass
            time.sleep(0.3)
        raise RuntimeError(f"Node {node.node_id} not ready after {timeout}s")

    @staticmethod
    def _find_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]


# ── Data collection ────────────────────────────────────────────────────────────

@dataclass
class Snapshot:
    timestamp: float
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    phase: str = ""


def stream_binance_data(
    ws,
    clients: List[Optional[TCPClient]],
    duration: float,
    phase_name: str,
    snapshots: List[Snapshot],
    orderbook_bids: Dict[float, float],
    orderbook_asks: Dict[float, float],
) -> int:
    """Stream Binance depth data to available clients for `duration` seconds.

    Returns number of updates processed.
    """
    deadline = time.time() + duration
    update_count = 0

    while time.time() < deadline:
        try:
            raw_msg = ws.recv(timeout=1.0)
        except Exception:
            continue

        try:
            msg = json.loads(raw_msg)
        except (json.JSONDecodeError, TypeError):
            continue

        # Parse depth update
        bids_raw = msg.get("b", [])
        asks_raw = msg.get("a", [])
        if not bids_raw and not asks_raw:
            continue

        # Update local orderbook state
        for price_str, qty_str in bids_raw:
            price = float(price_str)
            qty = float(qty_str)
            if qty > 0:
                orderbook_bids[price] = qty
            else:
                orderbook_bids.pop(price, None)

        for price_str, qty_str in asks_raw:
            price = float(price_str)
            qty = float(qty_str)
            if qty > 0:
                orderbook_asks[price] = qty
            else:
                orderbook_asks.pop(price, None)

        # Record snapshot
        best_bid = max(orderbook_bids.keys()) if orderbook_bids else None
        best_ask = min(orderbook_asks.keys()) if orderbook_asks else None
        snapshots.append(Snapshot(
            timestamp=time.time(), best_bid=best_bid,
            best_ask=best_ask, phase=phase_name,
        ))

        # Send to available clients (alternating)
        active_clients = [c for c in clients if c is not None]
        if not active_clients:
            continue

        target = active_clients[update_count % len(active_clients)]

        # Send bid updates
        for price_str, qty_str in bids_raw:
            price_int = int(float(price_str) * 100)
            qty_int = int(float(qty_str) * 100000)
            if qty_int > 0:
                try:
                    target.execute(f"INSERT {SYMBOL} {EXCHANGE} bid {price_int} {qty_int}")
                except Exception:
                    pass

        # Send ask updates
        for price_str, qty_str in asks_raw:
            price_int = int(float(price_str) * 100)
            qty_int = int(float(qty_str) * 100000)
            if qty_int > 0:
                try:
                    target.execute(f"INSERT {SYMBOL} {EXCHANGE} ask {price_int} {qty_int}")
                except Exception:
                    pass

        update_count += 1

        # Periodic flush
        if update_count % 30 == 0:
            for c in active_clients:
                try:
                    c.execute("FLUSH")
                except Exception:
                    pass

    # Final flush
    for c in [c for c in clients if c is not None]:
        try:
            c.execute("FLUSH")
        except Exception:
            pass

    return update_count


# ── Query & Compare ────────────────────────────────────────────────────────────

def query_node_state(port: int) -> List[Tuple[str, int, int]]:
    """Query all orderbook rows from a node. Returns [(side, price, qty), ...]."""
    client = TCPClient("127.0.0.1", port)
    try:
        resp = client.execute(
            f"SELECT * FROM '{SYMBOL}'.'{EXCHANGE}' WHERE timestamp BETWEEN 0 AND 9999999999999999999"
        )
    finally:
        client.close()

    rows = []
    if not resp.startswith("OK"):
        return rows
    lines = resp.split("\n")
    for line in lines[2:]:
        line = line.strip()
        if not line:
            break
        parts = line.split("\t")
        if len(parts) >= 5:
            try:
                price = int(parts[1])
                qty = int(parts[2])
                side = "bid" if int(parts[4]) == 0 else "ask"
                rows.append((side, price, qty))
            except (ValueError, IndexError):
                continue
    return rows


# ── Plot ───────────────────────────────────────────────────────────────────────

def generate_plot(snapshots: List[Snapshot], kill_time: float, restart_time: float,
                  output_path: str, node0_rows: int, node1_rows: int):
    """Generate best bid/ask plot with kill/restart markers."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    times = [datetime.fromtimestamp(s.timestamp) for s in snapshots]
    bids = [s.best_bid for s in snapshots]
    asks = [s.best_ask for s in snapshots]

    fig, ax = plt.subplots(figsize=(16, 7))

    ax.plot(times, bids, color="green", linewidth=0.8, label="Best Bid", alpha=0.9)
    ax.plot(times, asks, color="red", linewidth=0.8, label="Best Ask", alpha=0.9)
    ax.fill_between(times, bids, asks, alpha=0.08, color="gray")

    # Mark kill and restart events
    kill_dt = datetime.fromtimestamp(kill_time)
    restart_dt = datetime.fromtimestamp(restart_time)

    ax.axvline(x=kill_dt, color="red", linestyle="--", linewidth=2,
               label="Node 1 KILLED", alpha=0.8)
    ax.axvline(x=restart_dt, color="blue", linestyle="--", linewidth=2,
               label="Node 1 RESTARTED", alpha=0.8)

    # Shade the downtime period
    ax.axvspan(kill_dt, restart_dt, alpha=0.05, color="red",
               label="Node 1 downtime")

    ax.set_xlabel("Time", fontsize=11)
    ax.set_ylabel("Price (USD)", fontsize=11)
    ax.set_title(
        f"BTC/USDT — Binance Live Failover Test\n"
        f"Node 0: {node0_rows} rows | Node 1: {node1_rows} rows | "
        f"{'CONVERGED ✓' if node0_rows == node1_rows else 'DIVERGED ✗'}",
        fontsize=12,
    )
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    # Fix Y-axis: show full price without scientific offset, format as $XX,XXX.XX
    ax.ticklabel_format(axis='y', useOffset=False, style='plain')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"${x:,.2f}"))

    fig.autofmt_xdate()

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    info(f"Plot saved to: {output_path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Binance Live Failover Test")
    parser.add_argument("--phase-duration", type=int, default=20,
                        help="Duration of each phase in seconds (default: 20)")
    parser.add_argument("--output", type=str, default="/tmp/btc_failover_test.png",
                        help="Output plot path")
    args = parser.parse_args()

    phase_dur = args.phase_duration

    # Check prerequisites
    try:
        import websockets.sync.client  # noqa: F401
    except ImportError:
        error("websockets not installed. Run: pip3 install websockets")
        sys.exit(1)
    try:
        import matplotlib  # noqa: F401
    except ImportError:
        error("matplotlib not installed. Run: pip3 install matplotlib")
        sys.exit(1)

    # ── Start cluster ──────────────────────────────────────────────────────────
    cluster = MMCluster()
    try:
        info("Starting 2-node MM cluster (etcd + 2 ob_tcp_server)...")
        cluster.start()
        success(f"Cluster ready: node0={cluster.nodes[0].tcp_port}, "
                f"node1={cluster.nodes[1].tcp_port}")

        # ── Connect to Binance ─────────────────────────────────────────────────
        import websockets.sync.client as ws_sync

        info("Connecting to Binance WebSocket (btcusdt@depth)...")
        try:
            ws = ws_sync.connect(BINANCE_WS_URL, open_timeout=5.0, close_timeout=2.0)
        except Exception as exc:
            error(f"Cannot connect to Binance: {exc}")
            sys.exit(1)
        success("Connected to Binance")

        # State tracking
        snapshots: List[Snapshot] = []
        orderbook_bids: Dict[float, float] = {}
        orderbook_asks: Dict[float, float] = {}

        # ── Phase A: Stream to both nodes ──────────────────────────────────────
        info(f"Phase A: Streaming to both nodes for {phase_dur}s...")
        client0 = TCPClient("127.0.0.1", cluster.nodes[0].tcp_port)
        client1 = TCPClient("127.0.0.1", cluster.nodes[1].tcp_port)

        updates_a = stream_binance_data(
            ws, [client0, client1], phase_dur, "A: both nodes",
            snapshots, orderbook_bids, orderbook_asks,
        )
        success(f"Phase A done: {updates_a} updates")

        # ── Phase B: Kill node 1 ──────────────────────────────────────────────
        kill_time = time.time()
        cluster.kill_node(1)
        client1.close()
        client1 = None

        # ── Phase C: Stream to node 0 only ────────────────────────────────────
        info(f"Phase C: Streaming to node 0 only for {phase_dur}s (node 1 is dead)...")
        updates_c = stream_binance_data(
            ws, [client0, None], phase_dur, "C: node 1 down",
            snapshots, orderbook_bids, orderbook_asks,
        )
        success(f"Phase C done: {updates_c} updates (node 0 only)")

        # ── Phase D: Restart node 1 ───────────────────────────────────────────
        restart_time = time.time()
        cluster.restart_node(1)
        info("Waiting for peer re-discovery...")
        time.sleep(PEER_DISCOVERY_WAIT)

        # Reconnect client to node 1
        client1 = TCPClient("127.0.0.1", cluster.nodes[1].tcp_port)

        # ── Phase E: Stream to both nodes again ────────────────────────────────
        info(f"Phase E: Streaming to both nodes for {phase_dur}s (after restart)...")
        updates_e = stream_binance_data(
            ws, [client0, client1], phase_dur, "E: both nodes (post-restart)",
            snapshots, orderbook_bids, orderbook_asks,
        )
        success(f"Phase E done: {updates_e} updates")

        # Cleanup connections
        client0.close()
        client1.close()
        ws.close()

        # ── Query & Compare ────────────────────────────────────────────────────
        info("Querying both nodes...")
        state0 = query_node_state(cluster.nodes[0].tcp_port)
        state1 = query_node_state(cluster.nodes[1].tcp_port)

        info(f"Node 0: {len(state0)} rows")
        info(f"Node 1: {len(state1)} rows")

        # Check convergence
        set0 = set(state0)
        set1 = set(state1)
        converged = set0 == set1

        if converged:
            success(f"CONVERGED — both nodes have identical state ({len(state0)} rows)")
        else:
            only_0 = set0 - set1
            only_1 = set1 - set0
            warn(f"DIVERGED — node 0 has {len(only_0)} unique rows, "
                 f"node 1 has {len(only_1)} unique rows")
            warn(f"  (This may be expected if anti-entropy hasn't fully synced yet)")

        # ── Generate plot ──────────────────────────────────────────────────────
        info("Generating plot...")
        generate_plot(snapshots, kill_time, restart_time, args.output,
                      len(state0), len(state1))

        # ── Summary ────────────────────────────────────────────────────────────
        total_updates = updates_a + updates_c + updates_e
        print(f"\n{BOLD}═══ Summary ═══{RESET}")
        print(f"  Total updates: {total_updates}")
        print(f"  Total snapshots: {len(snapshots)}")
        print(f"  Node 0 rows: {len(state0)}")
        print(f"  Node 1 rows: {len(state1)}")
        print(f"  Converged: {'YES ✓' if converged else 'NO ✗'}")
        print(f"  Plot: {args.output}")

        sys.exit(0 if converged else 1)

    except KeyboardInterrupt:
        warn("Interrupted by user")
        sys.exit(130)
    except Exception as exc:
        error(f"Test failed: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cluster.shutdown()
        info("Cluster stopped, resources cleaned up.")


if __name__ == "__main__":
    main()
