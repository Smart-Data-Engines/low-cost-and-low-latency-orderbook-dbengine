#!/usr/bin/env python3
"""What a minute's bars cost, asked of the server or computed by the client (#44).

One fresh node, one symbol of ROWS rows with event times spread over two hours, loaded through the
Python client and FLUSHed - so they are in segments. Then, alternating, ROUNDS rounds of each:

  - `buckets`: `SELECT FIRST(price), MAX(price), MIN(price), LAST(price), COUNT(*), VWAP(price)
    ... GROUP BY TIME_BUCKET(1m)`, the server's answer parsed into numbers;
  - `client`: `SELECT *` of the same rows over the same range, and the same six aggregates computed
    from them in Python - what a client did before #44.

Each round runs QUERIES queries over one connection after three to warm up. Printed per round: the
server's CPU time a query (from /proc/<pid>/stat), the engine's time a query (from
`ob_query_latency_seconds`), the client's wall time a query including its own aggregation, p50 and
min, and the reply's bytes. The two answers are checked against each other before anything is timed.

    scripts/measure_time_buckets.py <ob_tcp_server> [rows] [rounds] [queries] [port]
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
PORT = int(sys.argv[5]) if len(sys.argv) > 5 else 21994  # below the ephemeral range
METRICS = PORT + 1
SEC = 1_000_000_000
BASE = 20_719 * 86_400 * SEC          # a whole UTC day
SPAN = 2 * 3600 * SEC                 # two hours of rows: 120 minute bars
PER_TIME = 100                        # levels written at one event time

BUCKET_SQL = (b"SELECT FIRST(price), MAX(price), MIN(price), LAST(price), COUNT(*), VWAP(price) "
              b"FROM 'BKT'.'BENCH' GROUP BY TIME_BUCKET(1m)\n")
ROWS_SQL = b"SELECT * FROM 'BKT'.'BENCH' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n"


def proc_stat(pid):
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])


def histogram(name):
    text = urllib.request.urlopen(f"http://127.0.0.1:{METRICS}/metrics", timeout=10).read().decode()
    s = float(re.search(rf"^{name}_sum(?:\{{[^}}]*\}})? (\S+)$", text, re.M).group(1))
    c = float(re.search(rf"^{name}_count(?:\{{[^}}]*\}})? (\S+)$", text, re.M).group(1))
    return s, c


def bars_from_buckets(reply):
    """The server's answer as {start: (first, max, min, last, count, vwap_scaled)}."""
    lines = reply.decode().split("\n")
    assert lines[0] == "OK" and lines[1].startswith("bucket_ns"), lines[:2]
    out = {}
    for line in lines[2:]:
        if not line:
            break
        f = line.split("\t")
        out[int(f[0])] = tuple(None if x == "NULL" else int(x) for x in f[1:])
    return out


def bars_from_rows(reply):
    """The same six from the rows, as a client computed them: {start: (first, max, min, last, count, vwap)}."""
    lines = reply.decode().split("\n")
    assert lines[0] == "OK", lines[:1]
    acc = {}
    for line in lines[2:]:
        if not line:
            break
        f = line.split("\t")
        ts, price, qty = int(f[0]), int(f[1]), int(f[2])
        start = ts - ts % (60 * SEC)
        b = acc.get(start)
        if b is None:
            acc[start] = [ts, price, ts, price, price, price, 1, qty, price * qty]
            continue
        if ts < b[0]:
            b[0], b[1] = ts, price
        if ts >= b[2]:
            b[2], b[3] = ts, price
        b[4] = max(b[4], price)
        b[5] = min(b[5], price)
        b[6] += 1
        b[7] += qty
        b[8] += price * qty
    return {s: (b[1], b[4], b[5], b[3], b[6], None if b[7] == 0 else b[8] * 1_000_000 // b[7])
            for s, b in acc.items()}


def main():
    data = tempfile.mkdtemp(prefix="measure_time_buckets_", dir=os.path.expanduser("~"))
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
            client.insert("BKT", "BENCH", "bid", [1_000_000 + (k * 7 + i) % 10_000 for i in range(PER_TIME)],
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
        # The same answer both ways, before anything is timed.
        served, computed = bars_from_buckets(ask(BUCKET_SQL)), bars_from_rows(ask(ROWS_SQL))
        if served != computed:
            raise SystemExit(f"the bars differ: {len(served)} served, {len(computed)} computed")
        if len(served) != SPAN // (60 * SEC):
            raise SystemExit(f"expected {SPAN // (60 * SEC)} bars, got {len(served)}")

        rounds = []
        for r in range(ROUNDS):
            for mode in ("buckets", "client"):   # ABAB
                sql = BUCKET_SQL if mode == "buckets" else ROWS_SQL
                work = bars_from_buckets if mode == "buckets" else bars_from_rows
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
        print(json.dumps({"server": SERVER, "rows": ROWS, "bars": len(served), "rounds": ROUNDS,
                          "queries_per_round": QUERIES}))
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
