#!/usr/bin/env python3
"""What a node pays as its segment count grows (#165).

Every flush tick closes the active segment of every symbol that received rows since the last one,
so a node gains one segment per active symbol per tick. This writes 256 symbols, one 20-level
MINSERT each per round, 20 rounds a second (102 400 levels a second, far below what one connection
can write), so every tick sees every symbol. Every 15 s it reports the segment directories on disk,
the server's resident memory, a narrow SELECT's latency, a PING's, and the writer's round latency over
the window; at the end it restarts the node and times it to its first answer.

Measured for #165 on the m9g.xlarge before its index change: 105 472 segments after 90 s, a round's
p99 14.7 -> 104 ms along the way, the narrow SELECT 2.85 -> 10.66 ms, PING flat, and a warm restart
2.70 s. The port is below the ephemeral range, so the connect loop cannot connect to itself.

    scripts/measure_segment_growth.py <ob_tcp_server> <data-root> <seconds>
"""
import os
import socket
import statistics
import subprocess
import sys
import time

SERVER, ROOT, SECONDS = sys.argv[1], sys.argv[2], int(sys.argv[3])
PORT, METRICS = 21992, 21993
SYMBOLS = [f"S{i:03d}" for i in range(256)]
DATA = os.path.join(ROOT, "soak")


def start():
    log = open(os.path.join(ROOT, "soak-server.log"), "ab")
    p = subprocess.Popen([SERVER, "--port", str(PORT), "--metrics-port", str(METRICS),
                          "--data-dir", DATA, "--drain-timeout-ms", "2000"],
                         stdout=log, stderr=subprocess.STDOUT)
    t0 = time.monotonic()
    while True:
        try:
            if "PONG" in request("PING", 1.0):
                return p, time.monotonic() - t0
        except OSError:
            pass
        if time.monotonic() - t0 > 600:
            raise SystemExit("the node never answered")
        time.sleep(0.05)


def request(cmd, timeout=30.0):
    with socket.create_connection(("127.0.0.1", PORT), timeout=timeout) as s:
        r = s.makefile("rb")
        while r.readline().strip():
            pass
        s.sendall((cmd + "\n").encode())
        if cmd == "PING":
            return r.readline().decode()
        out = []
        while True:
            line = r.readline()
            if not line:
                return "".join(out)
            if not line.strip():
                return "".join(out)
            if not out and line.startswith(b"ERR"):
                return line.decode()
            out.append(line.decode())


def rss_mib(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return 0.0


def segment_dirs():
    n = 0
    if not os.path.isdir(DATA):
        return 0
    for sym in os.scandir(DATA):
        if not sym.is_dir() or not sym.name.startswith("S"):
            continue
        for ex in os.scandir(sym.path):
            if ex.is_dir():
                n += sum(1 for _ in os.scandir(ex.path))
    return n


subprocess.run(["rm", "-rf", DATA])
os.makedirs(DATA)
node, _ = start()
w = socket.create_connection(("127.0.0.1", PORT), timeout=30)
wr = w.makefile("rb")
while wr.readline().strip():
    pass
levels = "\n".join(f"{1000 + i} 1 1" for i in range(20))
round_payload = "".join(f"MINSERT {s} EX bid 20\n{levels}\n" for s in SYMBOLS).encode()

print(f"{'t_s':>5} {'segments':>9} {'rss_mib':>8} {'select_ms':>10} {'ping_ms':>8} "
      f"{'round_p50_ms':>12} {'round_p99_ms':>12} {'round_max_ms':>12}", flush=True)
start_t = time.monotonic()
next_report = start_t + 15
window = []
period = 1 / 20
next_round = time.monotonic()
while time.monotonic() - start_t < SECONDS:
    now = time.monotonic()
    if now < next_round:
        time.sleep(next_round - now)
    next_round += period
    a = time.monotonic()
    w.sendall(round_payload)
    for _ in SYMBOLS:
        assert wr.readline().strip() == b"OK"
        wr.readline()
    window.append((time.monotonic() - a) * 1000)
    if time.monotonic() >= next_report:
        next_report += 15
        wall = time.time_ns()
        q0 = time.monotonic()
        reply = request(f"SELECT price FROM 'S000'.'EX' WHERE timestamp BETWEEN {wall - 10**9} AND {wall}")
        select_ms = (time.monotonic() - q0) * 1000
        p0 = time.monotonic()
        request("PING")
        ping_ms = (time.monotonic() - p0) * 1000
        window.sort()
        print(f"{time.monotonic() - start_t:5.0f} {segment_dirs():9d} {rss_mib(node.pid):8.1f} "
              f"{select_ms:10.2f} {ping_ms:8.2f} "
              f"{statistics.median(window):12.2f} {window[int(len(window) * 0.99)]:12.2f} "
              f"{window[-1]:12.2f}", flush=True)
        window = []
        # The report took time the writer did not use; starting the schedule again here keeps the
        # next window from being a burst that catches up on it.
        next_round = time.monotonic()
w.close()
node.terminate()
node.wait(timeout=120)
n = segment_dirs()
node, startup = start()
wall = time.time_ns()
q0 = time.monotonic()
request(f"SELECT price FROM 'S000'.'EX' WHERE timestamp BETWEEN {wall - 10**9} AND {wall}")
print(f"restart with {n} segments: first answer after {startup:.2f} s; narrow SELECT "
      f"{(time.monotonic() - q0) * 1000:.2f} ms", flush=True)
node.terminate()
node.wait(timeout=120)
