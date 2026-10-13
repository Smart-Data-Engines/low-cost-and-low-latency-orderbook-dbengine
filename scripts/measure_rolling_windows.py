#!/usr/bin/env python3
"""What a rolling window costs, asked of the server or computed by the client (#44 step 3).

One fresh node, one symbol of ROWS rows with event times spread over two hours, loaded through the
Python client and FLUSHed - so they are in segments. Then, alternating, ROUNDS rounds of each mode:

  - `rolling`: `SELECT COUNT(*), SUM(quantity), MIN(price), MAX(price), VWAP(price) ...
    GROUP BY TIME_BUCKET(1m) ROLLING 15m`, the server's answer parsed into numbers;
  - `buckets`: the same without `ROLLING` - what the same query cost before step 3, its windows a
    minute wide;
  - `client`: `SELECT *` of the same rows, and the fifteen-minute windows of every minute computed
    from them in Python - what a client does without step 3.

Each round runs QUERIES queries over one connection after three to warm up. Printed per round: the
server's CPU time a query (from /proc/<pid>/stat), the engine's time a query (from
`ob_query_latency_seconds`), the client's wall time a query including its own computation, p50 and
min, and the reply's bytes. The server's windows and the client's are checked against each other
before anything is timed.

    scripts/measure_rolling_windows.py <ob_tcp_server> [rows] [rounds] [queries] [port]
"""
import json
import os
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "python"))
from orderbook_engine import OrderbookEngine  # noqa: E402

SERVER = sys.argv[1]
ROWS = int(sys.argv[2]) if len(sys.argv) > 2 else 1_000_000
ROUNDS = int(sys.argv[3]) if len(sys.argv) > 3 else 4
QUERIES = int(sys.argv[4]) if len(sys.argv) > 4 else 10
PORT = int(sys.argv[5]) if len(sys.argv) > 5 else 21996  # below the ephemeral range
METRICS = PORT + 1
SEC = 1_000_000_000
MIN = 60 * SEC
BASE = 20_719 * 86_400 * SEC          # a whole UTC day
SPAN = 2 * 3600 * SEC                 # two hours of rows: 120 minute buckets
WINDOW = 15                           # minutes
PER_TIME = 100                        # levels written at one event time

SELECT = (b"SELECT COUNT(*), SUM(quantity), MIN(price), MAX(price), VWAP(price) FROM 'RW'.'BENCH' "
          b"WHERE timestamp BETWEEN " + str(BASE).encode() + b" AND " + str(BASE + SPAN - 1).encode())
ROLLING_SQL = SELECT + b" GROUP BY TIME_BUCKET(1m) ROLLING 15m\n"
BUCKET_SQL = SELECT + b" GROUP BY TIME_BUCKET(1m)\n"
ROWS_SQL = (b"SELECT * FROM 'RW'.'BENCH' WHERE timestamp BETWEEN " + str(BASE).encode() + b" AND " +
            str(BASE + SPAN - 1).encode() + b"\n")


def proc_stat(pid):
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])


def histogram(name):
    text = urllib.request.urlopen(f"http://127.0.0.1:{METRICS}/metrics", timeout=10).read().decode()
    s = float(re.search(rf"^{name}_sum(?:\{{[^}}]*\}})? (\S+)$", text, re.M).group(1))
    c = float(re.search(rf"^{name}_count(?:\{{[^}}]*\}})? (\S+)$", text, re.M).group(1))
    return s, c


def from_buckets(reply):
    """The server's answer as {start: (count, sum, min, max, vwap_scaled)}."""
    lines = reply.decode().split("\n")
    assert lines[0] == "OK" and lines[1].startswith("bucket_ns"), lines[:2]
    out = {}
    for line in lines[2:]:
        if not line:
            break
        f = line.split("\t")
        out[int(f[0])] = tuple(None if x == "NULL" else int(x) for x in f[1:])
    return out


def windows_from_rows(reply):
    """The same five over each minute's fifteen-minute window, from the rows, as a client computes
    them: per-minute sums first, then each window rolled from them."""
    lines = reply.decode().split("\n")
    assert lines[0] == "OK", lines[:1]
    minutes = {}
    for line in lines[2:]:
        if not line:
            break
        f = line.split("\t")
        ts, price, qty = int(f[0]), int(f[1]), int(f[2])
        m = minutes.get(ts - ts % MIN)
        if m is None:
            minutes[ts - ts % MIN] = [1, qty, price, price, price * qty]
            continue
        m[0] += 1
        m[1] += qty
        m[2] = min(m[2], price)
        m[3] = max(m[3], price)
        m[4] += price * qty
    out = {}
    for start in range(BASE, BASE + SPAN, MIN):
        inside = [minutes[s] for s in range(start - (WINDOW - 1) * MIN, start + MIN, MIN) if s in minutes]
        if not inside:
            out[start] = (0, None, None, None, None)
            continue
        n, q = sum(m[0] for m in inside), sum(m[1] for m in inside)
        pq = sum(m[4] for m in inside)
        out[start] = (n, q, min(m[2] for m in inside), max(m[3] for m in inside),
                      None if q == 0 else pq * 1_000_000 // q)
    return out


def main():
    data = tempfile.mkdtemp(prefix="measure_rolling_windows_", dir=os.path.expanduser("~"))
    log = open(os.path.join(data, "server.log"), "w")
    server = subprocess.Popen([SERVER, "--port", str(PORT), "--metrics-port", str(METRICS),
                               "--data-dir", os.path.join(data, "d"), "--log-level", "WARN"],
                              stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.time() + 30
        while True:
            try:
                socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
                break
            except OSError:
                if server.poll() is not None or time.time() > deadline:
                    raise SystemExit(f"the server did not start; its log is in {data}")
                time.sleep(0.1)

        client = OrderbookEngine(host="127.0.0.1", port=PORT, timeout=120)
        times = ROWS // PER_TIME
        step = SPAN // times
        for k in range(times):
            client.insert("RW", "BENCH", "bid", [1_000_000 + (k * 7 + i) % 10_000 for i in range(PER_TIME)],
                          [1 + (k + i) % 50 for i in range(PER_TIME)], timestamp_ns=BASE + k * step)
        client.flush()
        client.close()

        sock = socket.create_connection(("127.0.0.1", PORT), timeout=120)
        buf = b""

        def reply():
            nonlocal buf
            seen = 0
            while True:
                pos = buf.find(b"\n\n", max(0, seen - 1))
                if pos != -1:
                    resp, buf = buf[:pos + 2], buf[pos + 2:]
                    return resp
                seen = len(buf)
                chunk = sock.recv(1 << 20)
                if not chunk:
                    raise SystemExit("the server closed the connection")
                buf += chunk

        def ask(sql):
            sock.sendall(sql)
            return reply()

        reply()  # the banner
        # The same windows both ways, before anything is timed.
        served, computed = from_buckets(ask(ROLLING_SQL)), windows_from_rows(ask(ROWS_SQL))
        if served != computed:
            differing = [s for s in computed if served.get(s) != computed[s]]
            raise SystemExit(f"the windows differ at {len(differing)} of {len(computed)} minutes, the first "
                             f"{differing[:1]}: {served.get(differing[0]) if differing else None} served, "
                             f"{computed[differing[0]] if differing else None} computed")
        if len(served) != SPAN // MIN:
            raise SystemExit(f"expected {SPAN // MIN} windows, got {len(served)}")

        modes = {"rolling": (ROLLING_SQL, from_buckets), "buckets": (BUCKET_SQL, from_buckets),
                 "client": (ROWS_SQL, windows_from_rows)}
        rounds = []
        for r in range(ROUNDS):
            for mode, (sql, work) in modes.items():   # ABCABC
                for _ in range(3):
                    work(ask(sql))
                s0, c0 = histogram("ob_query_latency_seconds")
                cpu0 = proc_stat(server.pid)
                walls, size = [], 0
                for _ in range(QUERIES):
                    t = time.perf_counter()
                    resp = ask(sql)
                    work(resp)
                    walls.append(time.perf_counter() - t)
                    size = len(resp)
                cpu1 = proc_stat(server.pid)
                s1, c1 = histogram("ob_query_latency_seconds")
                ticks = os.sysconf("SC_CLK_TCK")
                rounds.append({
                    "round": r, "mode": mode,
                    "engine_ms_per_query": round((s1 - s0) / (c1 - c0) * 1000, 3),
                    "server_cpu_ms_per_query": round((cpu1 - cpu0) / ticks * 1000 / QUERIES, 3),
                    "client_wall_ms_p50": round(statistics.median(walls) * 1000, 3),
                    "client_wall_ms_min": round(min(walls) * 1000, 3),
                    "reply_bytes": size,
                })
                print(json.dumps(rounds[-1]), flush=True)
        print(json.dumps({"server": SERVER, "rows": ROWS, "windows": len(served), "window_min": WINDOW,
                          "rounds": ROUNDS, "queries_per_round": QUERIES}))
    finally:
        server.terminate()
        try:
            server.wait(timeout=60)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=30)
        shutil.rmtree(data, ignore_errors=True)


if __name__ == "__main__":
    main()
