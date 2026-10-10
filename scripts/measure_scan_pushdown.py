#!/usr/bin/env python3
"""What a query's conditions save when they narrow the read rather than the answer (#47 step 2).

    scripts/measure_scan_pushdown.py <repo with python/> --variant LABEL=SERVER[,FLAG,...] ...
                                     [--rounds 1] [--instants 5000] [--port N]

One symbol of a book over two hours: `--instants` instants, each the 100 levels of both sides
(1,000,000 rows at 5000), the mid drifting linearly up by a tenth, levels 10 ticks apart. Per round,
each variant in turn - the order reversed every other round: a fresh node on a fresh directory under
$HOME (a disk, not a tmpfs), loaded, FLUSHed, left 2 s; then each query asked once cold - the first
time after the load - then three times to warm up and ten times timed over one connection. Printed,
one JSON line a query: the variant, the cold time, p50 and min of the ten in ms, the server's CPU
over the ten (utime + stime from /proc), and the answer's lines, which every variant must give the
same.

The queries, requirement 8 of kiro-workspace/specs/conditions-narrow-the-read/:
- `level0`: SELECT * of level 0 - 2 rows an instant of 200;
- `band`: SELECT * in a band of prices the mid passes through once, around 30% of the history;
- `limit`: SELECT * ... LIMIT 100 over the whole symbol;
- `top`: FIRST, MAX, MIN, LAST and COUNT of the best bid's prices per minute;
- `mid`: OPEN, HIGH, LOW and CLOSE of the mid per minute - a series, which reads level 0;
- `slice`: SELECT * of one minute with no other condition - the read no condition narrows.
"""
import argparse
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time

ap = argparse.ArgumentParser()
ap.add_argument("repo")
ap.add_argument("--variant", action="append", required=True,
                help="LABEL=SERVER[,FLAG,...]: a server binary and the flags it runs with")
ap.add_argument("--rounds", type=int, default=1)
ap.add_argument("--instants", type=int, default=5000)
ap.add_argument("--port", type=int, default=21991)
args = ap.parse_args()
sys.path.insert(0, os.path.join(args.repo, "python"))
from orderbook_engine import OrderbookEngine  # noqa: E402

SEC = 1_000_000_000
BASE = 20_719 * 86_400 * SEC
LEVELS = 100
TICK = 10
MID0 = 1_000_000
SYMBOL, EXCHANGE = "PUSH", "BENCH"

variants = []
for spec in args.variant:
    label, rest = spec.split("=", 1)
    parts = rest.split(",")
    variants.append((label, parts[0], parts[1:]))

step = 2 * 3600 * SEC // args.instants


def mid(k):
    return MID0 + k * (MID0 // 10) // args.instants


def cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def load(c):
    for k in range(args.instants):
        m, ts = mid(k), BASE + k * step
        qty = [1 + (k + i) % 50 for i in range(LEVELS)]
        c.insert(SYMBOL, EXCHANGE, "bid", [m - (i + 1) * TICK for i in range(LEVELS)], qty, timestamp_ns=ts)
        c.insert(SYMBOL, EXCHANGE, "ask", [m + (i + 1) * TICK for i in range(LEVELS)], qty, timestamp_ns=ts)


def run(label, server, flags):
    data = tempfile.mkdtemp(prefix="scan_pushdown_", dir=os.path.expanduser("~"))
    log = open(os.path.join(data, "server.log"), "w")
    srv = subprocess.Popen([server, "--port", str(args.port), "--metrics-port", "0",
                            "--data-dir", os.path.join(data, "d"), "--log-level", "WARN", *flags],
                           stdout=log, stderr=subprocess.STDOUT)
    try:
        for _ in range(300):
            try:
                socket.create_connection(("127.0.0.1", args.port), timeout=1).close()
                break
            except OSError:
                time.sleep(0.1)
        c = OrderbookEngine(host="127.0.0.1", port=args.port, timeout=300)
        load(c)
        c.flush()
        c.close()
        time.sleep(2)
        lo, hi = BASE, BASE + (args.instants - 1) * step
        at30 = mid(args.instants * 3 // 10)
        minute = lo + (hi - lo) // 2
        src = f"'{SYMBOL}'.'{EXCHANGE}'"
        queries = {
            "level0": f"SELECT * FROM {src} WHERE level = 0",
            "band": f"SELECT * FROM {src} WHERE price BETWEEN {at30 - 1000} AND {at30 + 1000}",
            "limit": f"SELECT * FROM {src} LIMIT 100",
            "top": (f"SELECT FIRST(price), MAX(price), MIN(price), LAST(price), COUNT(*) FROM {src} "
                    f"WHERE side = 0 AND level = 0 GROUP BY TIME_BUCKET(1m)"),
            "mid": (f"SELECT OPEN(mid), HIGH(mid), LOW(mid), CLOSE(mid) FROM {src} "
                    f"WHERE timestamp BETWEEN {lo} AND {hi} GROUP BY TIME_BUCKET(1m)"),
            "slice": f"SELECT * FROM {src} WHERE timestamp BETWEEN {minute} AND {minute + 60 * SEC}",
        }
        s = socket.create_connection(("127.0.0.1", args.port), timeout=300)
        f = s.makefile("rb")
        f.readline()
        f.readline()   # the banner

        def ask(sql):
            s.sendall(sql.encode() + b"\n")
            lines = 0
            while True:
                line = f.readline()
                if line.startswith(b"ERR"):
                    raise SystemExit(f"{sql}: {line!r}")
                if line in (b"\n", b""):
                    return lines
                lines += 1

        for name, sql in queries.items():
            t0 = time.perf_counter()
            ask(sql)
            cold = (time.perf_counter() - t0) * 1000
            for _ in range(3):
                ask(sql)
            took = []
            cpu0 = cpu_seconds(srv.pid)
            for _ in range(10):
                t0 = time.perf_counter()
                lines = ask(sql)
                took.append((time.perf_counter() - t0) * 1000)
            cpu = cpu_seconds(srv.pid) - cpu0
            print(json.dumps({"variant": label, "query": name, "cold_ms": round(cold, 3),
                              "p50_ms": round(statistics.median(took), 3),
                              "min_ms": round(min(took), 3), "server_cpu_s_10": round(cpu, 3),
                              "lines": lines}), flush=True)
        s.close()
    finally:
        srv.terminate()
        try:
            srv.wait(timeout=60)
        except subprocess.TimeoutExpired:
            srv.kill()
            srv.wait()
        shutil.rmtree(data, ignore_errors=True)


for r in range(args.rounds):
    order = variants if r % 2 == 0 else list(reversed(variants))
    for label, server, flags in order:
        print(json.dumps({"round": r + 1, "variant": label, "instants": args.instants,
                          "server": server, "flags": flags}), flush=True)
        run(label, server, flags)
