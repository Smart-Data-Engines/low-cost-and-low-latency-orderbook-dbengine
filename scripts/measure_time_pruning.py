#!/usr/bin/env python3
"""Does a one-second time-range query read only the segments its range needs? (#166)

16 symbols, one 20-level MINSERT each per round, 20 rounds a second, for SECONDS; then three segment
directory names of one symbol - their names are their ranges - and a one-second SELECT of that
symbol at several distances back, five times each, with the rows it returned. The rows are the same
at every distance, a second of writes, so a latency that grows with the distance is segments read
that the range did not need.

Until #166 every segment of a period claimed to start at the period's boundary, so pruning kept every
segment of the hour written after the window: measured on the m9g.xlarge, 2.0 ms two seconds back and
7.0 ms 115 seconds back. With #166 each segment's range is the span of its own rows: 1.22, 1.19, 1.15
and 1.16 ms at 10, 30, 60 and 115 seconds back. What is left is the index walk of #165.

    scripts/measure_time_pruning.py <ob_tcp_server> <data-root> 120
"""
import os
import socket
import statistics
import subprocess
import sys
import time

SERVER, ROOT, SECONDS = sys.argv[1], sys.argv[2], int(sys.argv[3])
PORT, METRICS = 21994, 21995  # below the ephemeral range
SYMBOLS = [f"S{i:03d}" for i in range(16)]
DATA = os.path.join(ROOT, "prune")


def request_lines(cmd, timeout=60.0):
    with socket.create_connection(("127.0.0.1", PORT), timeout=timeout) as s:
        r = s.makefile("rb")
        while r.readline().strip():
            pass
        s.sendall((cmd + "\n").encode())
        out = []
        while True:
            line = r.readline()
            if not line or not line.strip():
                return out
            if not out and line.startswith(b"ERR"):
                return [line.decode()]
            out.append(line.decode())


def ping_ok():
    try:
        with socket.create_connection(("127.0.0.1", PORT), timeout=1.0) as s:
            r = s.makefile("rb")
            while r.readline().strip():
                pass
            s.sendall(b"PING\n")
            return b"PONG" in r.readline()
    except OSError:
        return False


subprocess.run(["rm", "-rf", DATA])
os.makedirs(DATA)
log = open(os.path.join(ROOT, "prune-server.log"), "wb")
node = subprocess.Popen([SERVER, "--port", str(PORT), "--metrics-port", str(METRICS),
                         "--data-dir", DATA, "--drain-timeout-ms", "2000"],
                        stdout=log, stderr=subprocess.STDOUT)
t0 = time.monotonic()
while not ping_ok():
    if time.monotonic() - t0 > 60:
        raise SystemExit("the node never answered")
    time.sleep(0.05)

w = socket.create_connection(("127.0.0.1", PORT), timeout=30)
wr = w.makefile("rb")
while wr.readline().strip():
    pass
levels = "\n".join(f"{1000 + i} 1 1" for i in range(20))
payload = "".join(f"MINSERT {s} EX bid 20\n{levels}\n" for s in SYMBOLS).encode()
start = time.monotonic()
next_round = start
while time.monotonic() - start < SECONDS:
    now = time.monotonic()
    if now < next_round:
        time.sleep(next_round - now)
    next_round += 1 / 20
    w.sendall(payload)
    for _ in SYMBOLS:
        assert wr.readline().strip() == b"OK"
        wr.readline()
w.close()
time.sleep(1.0)  # one more tick, so the last rounds are in segments

sym_dir = os.path.join(DATA, "S000", "EX")
names = sorted(os.listdir(sym_dir))
print(f"S000 has {len(names)} segment directories; first, middle and last:")
for n in (names[0], names[len(names) // 2], names[-1]):
    print("  ", n)

wall = time.time_ns()
print(f"\n{'seconds back':>12} {'rows':>6} {'median ms':>10} {'min ms':>8} {'max ms':>8}")
for back in (2, 10, 30, 60, SECONDS - 5):
    lo = wall - back * 10**9
    hi = lo + 10**9
    times, rows = [], None
    for _ in range(5):
        q0 = time.monotonic()
        out = request_lines(f"SELECT price FROM 'S000'.'EX' WHERE timestamp BETWEEN {lo} AND {hi}")
        times.append((time.monotonic() - q0) * 1000)
        # The first line is the header.
        rows = max(0, len(out) - 1)
        first = out[0].strip() if out else ""
    print(f"{back:12d} {rows:6d} {statistics.median(times):10.2f} {min(times):8.2f} {max(times):8.2f}"
          f"   first line: {first!r}", flush=True)

node.terminate()
node.wait(timeout=60)
subprocess.run(["rm", "-rf", DATA])
