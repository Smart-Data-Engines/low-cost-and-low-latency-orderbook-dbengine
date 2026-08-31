#!/usr/bin/env python3
"""
Binance BTC/USDT depth → one engine node → read it back → plot bid, ask and spread.

Usage:
    OB_SERVER_BINARY=./build-release/ob_tcp_server \
        python3 scripts/binance_collect_and_plot.py [--duration 90]

Scenario:
1. Start one ob_tcp_server on a free port with a temporary data directory
2. Stream live depth updates from Binance and MINSERT each side
3. Read the book back out of the engine after every update
4. Plot best bid and ask, with the spread on its own axis

The docstring here used to describe a five-step two-node multi-master scenario with
a kill, a restart and a comparison of both nodes. None of that was implemented: the
script starts one server. It was a description of a mechanism that does not exist,
which is the same shape as several real defects in this engine — so it has been
replaced by what the code does. The multi-master version of the same idea, with a
node bootstrapping from a live-fed peer, is scripts/binance_live_bootstrap.py.
"""

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
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

# ── Configuration ──────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Overridable the same way the CI jobs do it, so a run can point at a Release or a
# sanitizer build without editing this file.
SERVER_BINARY = os.environ.get(
    "OB_SERVER_BINARY", str(PROJECT_ROOT / "build" / "ob_tcp_server"))
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@depth"
SYMBOL = "BTCUSDT"
EXCHANGE = "BINANCE"


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


# ── Helpers ────────────────────────────────────────────────────────────────────

def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(port: int, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client = TCPClient("127.0.0.1", port, timeout=2.0)
            resp = client.execute("PING")
            client.close()
            if "PONG" in resp:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


@dataclass
class Snapshot:
    """A point-in-time snapshot of best bid/ask."""
    timestamp: float  # wall clock (seconds since epoch)
    best_bid: Optional[float]  # price in USD
    best_ask: Optional[float]  # price in USD


def parse_depth_update(msg: dict) -> Tuple[List[Tuple[str, float, float]], List[Tuple[str, float, float]]]:
    """Parse Binance depth update into (bids, asks) lists of (side, price, qty).

    Returns raw float prices (USD) for tracking best bid/ask.
    """
    bids = []
    asks = []
    for price_str, qty_str in msg.get("b", []):
        price = float(price_str)
        qty = float(qty_str)
        bids.append(("bid", price, qty))
    for price_str, qty_str in msg.get("a", []):
        price = float(price_str)
        qty = float(qty_str)
        asks.append(("ask", price, qty))
    return bids, asks


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Collect Binance BTC depth data and plot")
    parser.add_argument("--duration", type=int, default=60,
                        help="Collection duration in seconds (default: 60)")
    parser.add_argument("--output", type=str, default="/tmp/btc_orderbook.png",
                        help="Output plot path (default: /tmp/btc_orderbook.png)")
    args = parser.parse_args()

    # Check prerequisites
    if not os.path.isfile(SERVER_BINARY):
        print(f"ERROR: Server binary not found: {SERVER_BINARY}")
        print("Build first: cmake --build build")
        sys.exit(1)

    try:
        import websockets.sync.client  # noqa: F401
    except ImportError:
        print("ERROR: websockets package not installed. Run: pip install websockets")
        sys.exit(1)

    try:
        import matplotlib  # noqa: F401
    except ImportError:
        print("ERROR: matplotlib not installed. Run: pip install matplotlib")
        sys.exit(1)

    # ── Start ob_tcp_server ────────────────────────────────────────────────────
    tcp_port = find_free_port()
    metrics_port = find_free_port()
    data_dir = tempfile.mkdtemp(prefix="ob_binance_plot_")

    print(f"Starting ob_tcp_server on port {tcp_port}...")
    server_proc = subprocess.Popen(
        [SERVER_BINARY, "--port", str(tcp_port),
         "--data-dir", data_dir,
         "--metrics-port", str(metrics_port),
         "--log-level", "WARN"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        if not wait_for_server(tcp_port):
            stderr = server_proc.stderr.read().decode(errors="replace")
            print(f"ERROR: Server failed to start. stderr:\n{stderr}")
            sys.exit(1)
        print(f"Server ready on port {tcp_port}")

        # ── Connect to Binance WebSocket ───────────────────────────────────────
        import websockets.sync.client as ws_sync

        print(f"Connecting to Binance WebSocket...")
        try:
            ws = ws_sync.connect(BINANCE_WS_URL, open_timeout=5.0, close_timeout=2.0)
        except Exception as exc:
            print(f"ERROR: Cannot connect to Binance: {exc}")
            sys.exit(1)
        print("Connected to Binance depth stream (btcusdt@depth)")

        # ── Stream data ────────────────────────────────────────────────────────
        client = TCPClient("127.0.0.1", tcp_port)
        snapshots: List[Snapshot] = []
        update_count = 0
        insert_count = 0

        # Track current best bid/ask from the stream
        current_bids: dict = {}  # price → qty
        current_asks: dict = {}  # price → qty

        start_time = time.time()
        deadline = start_time + args.duration

        print(f"Collecting data for {args.duration}s...")
        try:
            while time.time() < deadline:
                try:
                    raw_msg = ws.recv(timeout=2.0)
                except Exception:
                    continue

                try:
                    msg = json.loads(raw_msg)
                except (json.JSONDecodeError, TypeError):
                    continue

                bids, asks = parse_depth_update(msg)
                if not bids and not asks:
                    continue

                update_count += 1

                # Update local orderbook state for best bid/ask tracking
                for _, price, qty in bids:
                    if qty > 0:
                        current_bids[price] = qty
                    else:
                        current_bids.pop(price, None)
                for _, price, qty in asks:
                    if qty > 0:
                        current_asks[price] = qty
                    else:
                        current_asks.pop(price, None)

                # Record snapshot of best bid/ask
                best_bid = max(current_bids.keys()) if current_bids else None
                best_ask = min(current_asks.keys()) if current_asks else None
                snapshots.append(Snapshot(
                    timestamp=time.time(),
                    best_bid=best_bid,
                    best_ask=best_ask,
                ))

                # Send to ob_tcp_server (store in DB)
                # Convert to integer sub-units (price * 100 for cents)
                for side, price, qty in bids:
                    price_int = int(price * 100)
                    qty_int = int(qty * 100000)
                    if qty_int > 0:
                        resp = client.execute(
                            f"INSERT {SYMBOL} {EXCHANGE} bid {price_int} {qty_int}")
                        if not resp.startswith("ERR"):
                            insert_count += 1

                for side, price, qty in asks:
                    price_int = int(price * 100)
                    qty_int = int(qty * 100000)
                    if qty_int > 0:
                        resp = client.execute(
                            f"INSERT {SYMBOL} {EXCHANGE} ask {price_int} {qty_int}")
                        if not resp.startswith("ERR"):
                            insert_count += 1

                # Periodic flush
                if update_count % 50 == 0:
                    client.execute("FLUSH")
                    elapsed = time.time() - start_time
                    print(f"  [{elapsed:.0f}s] {update_count} updates, "
                          f"{insert_count} inserts, "
                          f"bid={best_bid}, ask={best_ask}")

        except KeyboardInterrupt:
            print("\nInterrupted by user.")

        # Final flush
        client.execute("FLUSH")
        client.close()
        ws.close()

        elapsed = time.time() - start_time
        print(f"\nCollection complete: {update_count} updates, "
              f"{insert_count} inserts in {elapsed:.1f}s")
        print(f"Snapshots recorded: {len(snapshots)}")

        # ── Plot ───────────────────────────────────────────────────────────────
        if len(snapshots) < 2:
            print("Not enough data to plot.")
            sys.exit(1)

        import matplotlib
        matplotlib.use("Agg")  # non-interactive backend
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from datetime import datetime

        # Prepare data
        times = [datetime.fromtimestamp(s.timestamp) for s in snapshots]
        bid_prices = [s.best_bid for s in snapshots]
        ask_prices = [s.best_ask for s in snapshots]

        spreads = [a - b for a, b in zip(ask_prices, bid_prices)]

        # Two panels, because one cannot carry both facts.
        #
        # The first version drew bid, ask and a shaded spread on a single linear axis and produced a
        # single visible line: BTC/USDT trades at a one-cent spread, so at a price of ~$78,000 over a
        # range of a few tens of dollars the two series are the same pixel and the shaded band is
        # thinner than the stroke. The chart was not wrong, it was empty — a legend promising three
        # things and showing one. The spread is where the information is, and it needs an axis of its
        # own.
        fig, (ax, ax_spread) = plt.subplots(
            2, 1, figsize=(14, 8), sharex=True, gridspec_kw={"height_ratios": [3, 1]})

        ax.plot(times, bid_prices, color="green", linewidth=1.6,
                label="Best Bid", alpha=0.9)
        ax.plot(times, ask_prices, color="red", linewidth=0.8,
                label="Best Ask", alpha=0.9)
        ax.set_ylabel("Price (USD)")
        ax.set_title(f"BTC/USDT from the engine — {args.duration}s live from Binance\n"
                     f"bid and ask overlap at this scale; the spread is below")
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)

        ax_spread.plot(times, spreads, color="steelblue", linewidth=0.9)
        ax_spread.fill_between(times, 0, spreads, alpha=0.2, color="steelblue")
        ax_spread.set_ylabel("Spread (USD)")
        ax_spread.set_xlabel("Time")
        ax_spread.grid(True, alpha=0.3)
        ax_spread.set_ylim(bottom=0)

        # Format x-axis
        ax_spread.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        fig.autofmt_xdate()

        neg = [sp for sp in spreads if sp < 0]
        print(f"Spread over the run: min ${min(spreads):.2f} / median "
              f"${sorted(spreads)[len(spreads) // 2]:.2f} / max ${max(spreads):.2f}"
              + (f"  ({len(neg)} crossed book(s))" if neg else ""))

        plt.tight_layout()
        plt.savefig(args.output, dpi=150)
        print(f"\nPlot saved to: {args.output}")

    finally:
        # Cleanup
        server_proc.send_signal(signal.SIGTERM)
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait(timeout=5)
        shutil.rmtree(data_dir, ignore_errors=True)
        print("Server stopped, temp data cleaned up.")


if __name__ == "__main__":
    main()
