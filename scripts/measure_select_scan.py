#!/usr/bin/env python3
"""What a server's SELECT of one symbol's rows costs it, and how many pages it faults doing so (#49).

One fresh node, one symbol of ROWS rows loaded through the Python client and FLUSHed - so they are
in segments - then QUERIES full-range SELECTs over one connection, after ten to warm up. Printed:

  - the engine's time a query, from `ob_query_latency_seconds` - the read and the rows' hand-over,
    without the formatting and the send;
  - the server's minor page faults a query, from /proc/<pid>/stat;
  - the client's wall time a query, p50 and min, over a socket read to the reply's blank line - so
    the Python client's parsing is not in it.

A query's cost depends on what the process's heap did before it, which is why this runs a server
rather than the engine in a benchmark's loop: the benchmark's process was never trimmed between its
scans, and a server faulted thousands of pages a query back in (#49's step 2).

    scripts/measure_select_scan.py <ob_tcp_server> [rows] [queries] [port]
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
ROWS = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
QUERIES = int(sys.argv[3]) if len(sys.argv) > 3 else 200
PORT = int(sys.argv[4]) if len(sys.argv) > 4 else 21996  # below the ephemeral range
METRICS = PORT + 1
BATCH = 500


def minor_faults(pid):
    return int(open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()[7])


def histogram(name):
    text = urllib.request.urlopen(f"http://127.0.0.1:{METRICS}/metrics", timeout=10).read().decode()
    # Labelled, `{node_role="..."}`, as every series the registry writes.
    s = float(re.search(rf"^{name}_sum(?:\{{[^}}]*\}})? (\S+)$", text, re.M).group(1))
    c = float(re.search(rf"^{name}_count(?:\{{[^}}]*\}})? (\S+)$", text, re.M).group(1))
    return s, c


def main():
    data = tempfile.mkdtemp(prefix="measure_select_scan_", dir=os.path.expanduser("~"))
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

        client = OrderbookEngine(host="127.0.0.1", port=PORT, timeout=60)
        for sent in range(0, ROWS, BATCH):
            count = min(BATCH, ROWS - sent)
            client.insert("SCAN", "BENCH", "bid", [1_000_000 + sent + i for i in range(count)],
                          [10 + (sent + i) % 5000 for i in range(count)])
        client.flush()
        client.close()

        sql = b"SELECT * FROM 'SCAN'.'BENCH' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n"
        sock = socket.create_connection(("127.0.0.1", PORT), timeout=60)
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

        def ask():
            sock.sendall(sql)
            return reply()

        reply()  # the banner, which ends with a blank line as a reply does
        lines = ask().count(b"\n")
        if lines != ROWS + 3:  # OK, the header, the rows, the blank line
            raise SystemExit(f"a SELECT answered {lines - 3} rows of {ROWS}")
        for _ in range(10):
            ask()
        s0, c0 = histogram("ob_query_latency_seconds")
        f0 = minor_faults(server.pid)
        walls = []
        for _ in range(QUERIES):
            t = time.perf_counter()
            ask()
            walls.append(time.perf_counter() - t)
        f1 = minor_faults(server.pid)
        s1, c1 = histogram("ob_query_latency_seconds")
        print(json.dumps({
            "server": SERVER, "rows": ROWS, "queries": QUERIES,
            "engine_ms_per_query": round((s1 - s0) / (c1 - c0) * 1000, 3),
            "server_minor_faults_per_query": (f1 - f0) / QUERIES,
            "client_wall_ms_p50": round(statistics.median(walls) * 1000, 3),
            "client_wall_ms_min": round(min(walls) * 1000, 3),
        }, indent=1))
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
