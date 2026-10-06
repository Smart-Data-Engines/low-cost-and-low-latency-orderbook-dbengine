#!/usr/bin/env python3
"""What a minute's bars of the mid cost, asked of the server or computed by the client (#44 step 2).

One fresh node, one symbol: at each of ROWS / 200 event times spread over two hours, both sides of a
100-level book - so ROWS rows - loaded through the Python client and FLUSHed. Then, alternating,
ROUNDS rounds of each:

  - `series`: `SELECT OPEN(mid), HIGH(mid), LOW(mid), CLOSE(mid), TWAP(mid) ... GROUP BY
    TIME_BUCKET(1m)`, the server's answer parsed into numbers;
  - `client`: `SELECT * ... WHERE level = 0` - the rows the series is made of, the least a client
    can fetch for it - and the same five computed from them in Python: the book at each instant, a
    tie to the row returned later, swept through each minute.

Each round runs QUERIES queries over one connection after three to warm up. Printed per round: the
server's CPU time a query (from /proc/<pid>/stat), the engine's time a query (from
`ob_query_latency_seconds`), the client's wall time a query including its own computation, p50 and
min, and the reply's bytes. The two answers are checked against each other before anything is timed.

    scripts/measure_book_series.py <ob_tcp_server> [rows] [rounds] [queries] [port]
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
PORT = int(sys.argv[5]) if len(sys.argv) > 5 else 21992  # below the ephemeral range
METRICS = PORT + 1
SEC = 1_000_000_000
MINUTE = 60 * SEC
BASE = 20_719 * 86_400 * SEC          # a whole UTC day
SPAN = 2 * 3600 * SEC                 # two hours: 120 minute bars
LEVELS = 100                          # a side's levels at one event time

SERIES_SQL = (b"SELECT OPEN(mid), HIGH(mid), LOW(mid), CLOSE(mid), TWAP(mid) "
              b"FROM 'BKS'.'BENCH' GROUP BY TIME_BUCKET(1m)\n")
ROWS_SQL = b"SELECT * FROM 'BKS'.'BENCH' WHERE level = 0\n"


def proc_stat(pid):
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])


def histogram(name):
    text = urllib.request.urlopen(f"http://127.0.0.1:{METRICS}/metrics", timeout=10).read().decode()
    s = float(re.search(rf"^{name}_sum(?:\{{[^}}]*\}})? (\S+)$", text, re.M).group(1))
    c = float(re.search(rf"^{name}_count(?:\{{[^}}]*\}})? (\S+)$", text, re.M).group(1))
    return s, c


def bars_from_series(reply):
    """The server's answer as {start: (open, high, low, close, twap)}, the mid at 10^6."""
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
    """The same five from the level-0 rows, as a client would compute them."""
    lines = reply.decode().split("\n")
    assert lines[0] == "OK", lines[:1]
    rows = []
    for order, line in enumerate(lines[2:]):
        if not line:
            break
        f = line.split("\t")
        rows.append((int(f[0]), order, int(f[4]), int(f[1])))   # time, order, side, price
    if not rows:
        return {}
    rows.sort()
    lo, hi = rows[0][0], rows[-1][0]
    bid = ask = None
    out = {}
    i = 0
    start = lo - lo % MINUTE
    while start <= hi:
        ws, we = max(start, lo), min(start + MINUTE - 1, hi)

        def mid():
            return None if bid is None or ask is None else (bid + ask) * 500_000

        # The rows at the window's first instant make its open.
        while i < len(rows) and rows[i][0] <= ws:
            if rows[i][2] == 0:
                bid = rows[i][3]
            else:
                ask = rows[i][3]
            i += 1
        opened = mid()
        high = low = opened
        since, weighted, held = ws, 0, 0
        while i < len(rows) and rows[i][0] <= we:
            t = rows[i][0]
            value = mid()
            if value is not None:
                weighted += value * (t - since)
                held += t - since
            while i < len(rows) and rows[i][0] == t:
                if rows[i][2] == 0:
                    bid = rows[i][3]
                else:
                    ask = rows[i][3]
                i += 1
            value = mid()
            if value is not None:
                high = value if high is None else max(high, value)
                low = value if low is None else min(low, value)
            since = t
        value = mid()
        if value is not None:
            weighted += value * (we - since + 1)
            held += we - since + 1
        out[start] = (opened, high, low, value, weighted // held if held else None)
        start += MINUTE
    return out


def main():
    data = tempfile.mkdtemp(prefix="measure_book_series_", dir=os.path.expanduser("~"))
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
        times = ROWS // (2 * LEVELS)
        step = SPAN // times
        for k in range(times):
            bid = 1_000_000 - (k * 7919) % 5_000
            ask = bid + 1 + (k * 104_729) % 50
            ts = BASE + k * step
            client.insert("BKS", "BENCH", "bid", [bid - i for i in range(LEVELS)],
                          [1 + (k + i) % 50 for i in range(LEVELS)], timestamp_ns=ts)
            client.insert("BKS", "BENCH", "ask", [ask + i for i in range(LEVELS)],
                          [1 + (k + i) % 50 for i in range(LEVELS)], timestamp_ns=ts)
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
        served, computed = bars_from_series(ask(SERIES_SQL)), bars_from_rows(ask(ROWS_SQL))
        if served != computed:
            differ = [s for s in served if served[s] != computed.get(s)]
            raise SystemExit(f"the bars differ: {len(served)} served, {len(computed)} computed, "
                             f"first at {differ[:1]}: {served.get(differ[0]) if differ else None} "
                             f"against {computed.get(differ[0]) if differ else None}")
        if len(served) != SPAN // MINUTE:
            raise SystemExit(f"expected {SPAN // MINUTE} bars, got {len(served)}")

        rounds = []
        for r in range(ROUNDS):
            for mode in ("series", "client"):   # ABAB
                sql = SERIES_SQL if mode == "series" else ROWS_SQL
                work = bars_from_series if mode == "series" else bars_from_rows
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
