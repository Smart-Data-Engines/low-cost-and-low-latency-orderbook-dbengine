#!/usr/bin/env python3
"""
A node bootstraps by snapshot while its peer is being fed live Binance depth.

This is the multi-master half of scripts/binance_collect_and_plot.py, and it exists to test one
claim on real data rather than on a fixture: since roadmap #79 a snapshot is created on a worker
thread, so the peer producing one keeps consuming a live feed while it does.

Usage:
    OB_SERVER_BINARY=./build-release/ob_tcp_server \
        python3 scripts/binance_live_bootstrap.py [--duration 90] [--join-after 30]

Scenario:
  1. Native etcd, then node 1 with multi-master enabled and an empty data directory.
  2. Live Binance BTC/USDT depth into node 1, continuously, timing every single write.
  3. After --join-after seconds, node 2 starts with an empty data directory. Holding nothing, it
     asks node 1 for a snapshot as soon as node 1's version vector reaches it.
  4. The feed keeps running through the bootstrap and past it.
  5. Both nodes are read back and compared, and node 1's snapshot metrics are printed.

What it measures: the slowest write to node 1 inside the bootstrap window against the slowest
outside it. With snapshot creation on the io loop, the first would have to include a whole
flush-and-checksum pass over the store — which is the thing #79 removed.

etcd runs as a native process. This engine does not use containers, in tests either.
"""

import argparse
import json
import os
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from binance_collect_and_plot import TCPClient  # noqa: E402

SERVER = os.environ.get("OB_SERVER_BINARY", str(PROJECT_ROOT / "build" / "ob_tcp_server"))
ETCD = os.environ.get("OB_ETCD_BINARY", shutil.which("etcd") or "/usr/local/bin/etcd")
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@depth"
SYMBOL, EXCHANGE = "BTCUSDT", "BINANCE"
PRICE_SCALE, QTY_SCALE = 100, 100_000_000


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def metrics_of(port: int) -> dict:
    """Scrape one node's /metrics into a name → float map."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as r:
            body = r.read().decode()
    except OSError as exc:
        print(f"  (metrics on :{port} unreachable: {exc})")
        return {}
    out = {}
    for line in body.splitlines():
        if line.startswith("#") or " " not in line:
            continue
        name, _, value = line.rpartition(" ")
        try:
            out[name.strip()] = float(value)
        except ValueError:
            pass
    return out


class Node:
    def __init__(self, index: int, etcd_url: str, root: str, mm_port: int, peer_mm_port: int):
        self.index = index
        self.dir = os.path.join(root, f"node{index}")
        os.makedirs(self.dir, exist_ok=True)
        self.tcp, self.metrics = free_port(), free_port()
        self.repl = free_port()
        self.mm = mm_port
        self.log_path = os.path.join(root, f"node{index}.log")
        self._log = open(self.log_path, "ab")
        self.proc = subprocess.Popen([
            SERVER, "--port", str(self.tcp), "--data-dir", self.dir,
            "--metrics-port", str(self.metrics), "--replication-port", str(self.repl),
            "--coordinator-endpoints", etcd_url, "--node-id", f"node-{index}",
            "--multi-master", "--mm-node-id", str(index),
            "--mm-replication-port", str(self.mm),
            "--log-level", "DEBUG",
            # Three seconds so the peer's version vector — which is what makes an empty node ask
            # for a snapshot — arrives inside this run rather than in half a minute.
            "--anti-entropy-interval-seconds", "3",
        ], stdout=self._log, stderr=subprocess.STDOUT)
        if not wait_for_port(self.tcp, 30):
            raise RuntimeError(f"node {index} never opened :{self.tcp} — see {self.log_path}")

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self._log.close()


def levels(msg: dict, side: str) -> list:
    key = "b" if side == "bid" else "a"
    out = []
    for price_str, qty_str in msg.get(key, []):
        qty = float(qty_str)
        if qty <= 0:            # Binance sends 0 to mean "level gone"; the engine stores rows
            continue            # so a zero-quantity row would record a size that never existed
        out.append((int(round(float(price_str) * PRICE_SCALE)),
                    max(1, int(round(qty * QTY_SCALE)))))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=90, help="total seconds of streaming")
    ap.add_argument("--join-after", type=int, default=30, help="when node 2 joins")
    args = ap.parse_args()

    for path, what in ((SERVER, "server binary"), (ETCD, "etcd binary")):
        if not path or not os.path.isfile(path):
            print(f"ERROR: {what} not found: {path}")
            return 1
    try:
        import websockets.sync.client  # noqa: F401
    except ImportError:
        print("ERROR: pip install websockets")
        return 1

    root = tempfile.mkdtemp(prefix="ob_binance_bootstrap_")
    etcd_client, etcd_peer = free_port(), free_port()
    etcd_url = f"http://127.0.0.1:{etcd_client}"
    etcd_log = open(os.path.join(root, "etcd.log"), "ab")
    etcd_proc = subprocess.Popen([
        ETCD, "--name", "blb", "--data-dir", os.path.join(root, "etcd"),
        "--advertise-client-urls", etcd_url, "--listen-client-urls", etcd_url,
        "--listen-peer-urls", f"http://127.0.0.1:{etcd_peer}",
        "--initial-advertise-peer-urls", f"http://127.0.0.1:{etcd_peer}",
        "--initial-cluster", f"blb=http://127.0.0.1:{etcd_peer}",
    ], stdout=etcd_log, stderr=subprocess.STDOUT)

    nodes: list = []
    stop_feed = threading.Event()
    writes: list = []          # (monotonic_time, latency_ms)
    counters = {"updates": 0, "inserts": 0, "errors": 0}

    try:
        if not wait_for_port(etcd_client, 30):
            print("ERROR: etcd never came up")
            return 1
        print(f"etcd on :{etcd_client}")

        mm1, mm2 = free_port(), free_port()
        node1 = Node(1, etcd_url, root, mm1, mm2)
        nodes.append(node1)
        print(f"node 1 up: tcp :{node1.tcp} metrics :{node1.metrics} mm :{node1.mm}")

        def feed():
            import websockets.sync.client as wsc
            client = TCPClient("127.0.0.1", node1.tcp)
            with wsc.connect(BINANCE_WS_URL, open_timeout=15) as ws:
                while not stop_feed.is_set():
                    try:
                        raw = ws.recv(timeout=2.0)
                    except TimeoutError:
                        continue
                    except Exception:
                        break
                    try:
                        msg = json.loads(raw)
                    except (TypeError, ValueError):
                        continue
                    if msg.get("e") != "depthUpdate":
                        continue
                    counters["updates"] += 1
                    ts = int(time.time() * 1_000_000_000)
                    for side, side_word in (("bid", "BID"), ("ask", "ASK")):
                        lv = levels(msg, side)
                        if not lv:
                            continue
                        payload = " ".join(f"{p} {q}" for p, q in lv)
                        cmd = (f"MINSERT {SYMBOL} {EXCHANGE} {ts} {side_word} "
                               f"{len(lv)} {payload}")
                        t0 = time.perf_counter()
                        try:
                            resp = client.execute(cmd)
                        except Exception:
                            counters["errors"] += 1
                            break
                        dt_ms = (time.perf_counter() - t0) * 1000.0
                        writes.append((time.monotonic(), dt_ms))
                        if resp.startswith("ERR"):
                            counters["errors"] += 1
                        else:
                            counters["inserts"] += len(lv)

        t_feed = threading.Thread(target=feed, daemon=True)
        t_feed.start()
        print(f"streaming live BTC/USDT depth into node 1 for {args.duration}s "
              f"(node 2 joins at {args.join_after}s)")

        t_start = time.monotonic()
        time.sleep(args.join_after)

        before = metrics_of(node1.metrics)
        join_t0 = time.monotonic()
        node2 = Node(2, etcd_url, root, mm2, mm1)
        nodes.append(node2)
        print(f"[{join_t0 - t_start:5.1f}s] node 2 up (empty): tcp :{node2.tcp} "
              f"metrics :{node2.metrics} mm :{node2.mm}")

        # Wait for the snapshot to land, reading it from node 1's own counter.
        bootstrap_done = None
        while time.monotonic() - t_start < args.duration:
            m = metrics_of(node1.metrics)
            if m.get("ob_mm_snapshot_sent_total", 0) > before.get("ob_mm_snapshot_sent_total", 0):
                bootstrap_done = time.monotonic()
                print(f"[{bootstrap_done - t_start:5.1f}s] node 1 reports a snapshot sent "
                      f"({bootstrap_done - join_t0:.1f}s after node 2 started)")
                break
            time.sleep(0.5)
        else:
            print("node 1 never reported a snapshot sent — see the logs in", root)

        remaining = args.duration - (time.monotonic() - t_start)
        if remaining > 0:
            time.sleep(remaining)
        stop_feed.set()
        t_feed.join(timeout=15)

        print(f"\nfeed: {counters['updates']} depth updates, {counters['inserts']} levels "
              f"inserted, {counters['errors']} errors, {len(writes)} MINSERTs")

        # ── The measurement this script exists for ─────────────────────────────
        if bootstrap_done and writes:
            window = [ms for (t, ms) in writes if join_t0 <= t <= bootstrap_done]
            outside = [ms for (t, ms) in writes if t < join_t0 or t > bootstrap_done]
            print("\nMINSERT round-trip on node 1, the loop that also produced the snapshot:")
            for label, xs in (("during the bootstrap", window), ("outside it", outside)):
                if not xs:
                    print(f"  {label:22}: no samples")
                    continue
                xs_sorted = sorted(xs)
                p50 = statistics.median(xs_sorted)
                p99 = xs_sorted[min(len(xs_sorted) - 1, int(len(xs_sorted) * 0.99))]
                print(f"  {label:22}: n={len(xs):4d}  p50={p50:6.2f} ms  "
                      f"p99={p99:6.2f} ms  max={max(xs):6.2f} ms")

        after = metrics_of(node1.metrics)
        print("\nnode 1 snapshot metrics:")
        for key in ("ob_mm_snapshot_sent_total", "ob_mm_snapshot_failed_total",
                    "ob_mm_snapshot_refused_total", "ob_mm_snapshot_discarded_total",
                    "ob_mm_snapshot_create_ms", "ob_mm_snapshot_prepare_ms",
                    "ob_mm_snapshot_bytes_sent_total"):
            print(f"  {key:38} = {after.get(key, float('nan')):.0f}")

        # ── Read the data back out of both nodes ───────────────────────────────
        print("\nreading back from the engine:")
        for node in nodes:
            c = TCPClient("127.0.0.1", node.tcp)
            rows = c.execute(f"SELECT {SYMBOL} {EXCHANGE} 0 9999999999999999999 LIMIT 5")
            count = sum(1 for ln in rows.splitlines() if ln and not ln.startswith(("OK", "ERR")))
            vwap = c.execute(f"SELECT VWAP({SYMBOL}) {EXCHANGE} 0 9999999999999999999").strip()
            status = c.execute("STATUS")
            total_rows = next((ln for ln in status.splitlines() if "rows" in ln.lower()), "").strip()
            print(f"  node {node.index}: first rows returned={count}  VWAP={vwap!r}")
            if total_rows:
                print(f"          {total_rows}")
        return 0

    finally:
        stop_feed.set()
        for n in reversed(nodes):
            n.stop()
        if etcd_proc.poll() is None:
            etcd_proc.terminate()
            try:
                etcd_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                etcd_proc.kill()
        etcd_log.close()
        print(f"\nlogs and data left in {root}")


if __name__ == "__main__":
    sys.exit(main())
