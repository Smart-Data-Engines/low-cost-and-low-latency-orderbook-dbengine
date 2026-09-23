#!/usr/bin/env python3
"""What the #166 repair costs at the first start after an upgrade, and that the next start pays nothing.

A build from before #166 writes a node's worth of segments - 256 symbols, one 20-level MINSERT each
per round, 20 rounds a second, for SECONDS - and stops. Then, on that directory, in this order:

  before    the old build restarted: the time to a first answer without any repair (the baseline);
  after-1   the #166 build: every segment is of the old format, so it reads each ts.col once and
            rewrites its meta.json - the time to a first answer, and the repair's own log line;
  after-2   the #166 build again: nothing left to repair.

Each start is timed from the fork to the first PONG, with the page cache dropped before it (`sudo`),
so the first start does not read what the writing left in memory and the others do not inherit it.
The port is below the ephemeral range on purpose: a connect loop to a port inside it can be given
that same port as its source and connect to itself, and the node then fails to bind - which is how
the first run of this measured nothing (#109 is the same lesson from the test suite's side).

Measured for #166 on the m9g.xlarge, data on EBS: 106 752 segments; 82.31 s, 170.02 s (the repair
87.1 s of it), 82.34 s.

    scripts/measure_segment_range_repair.py <old ob_tcp_server> <new ob_tcp_server> <data-root> 90
"""
import os
import re
import socket
import subprocess
import sys
import time

OLD, NEW, ROOT, SECONDS = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
PORT, METRICS = 21990, 21991  # below the ephemeral range: a tight connect loop to a port inside it can connect to itself
DATA = os.path.join(ROOT, "repair")
SYMBOLS = [f"S{i:03d}" for i in range(256)]


def ping(timeout=1.0):
    with socket.create_connection(("127.0.0.1", PORT), timeout=timeout) as s:
        r = s.makefile("rb")
        while r.readline().strip():
            pass
        s.sendall(b"PING\n")
        return b"PONG" in r.readline()


def start(binary, log_name):
    log = open(os.path.join(ROOT, log_name), "wb")
    t0 = time.monotonic()
    p = subprocess.Popen([binary, "--port", str(PORT), "--metrics-port", str(METRICS),
                          "--data-dir", DATA, "--drain-timeout-ms", "2000"],
                         stdout=log, stderr=subprocess.STDOUT)
    while True:
        try:
            if ping():
                return p, time.monotonic() - t0
        except OSError:
            pass
        if time.monotonic() - t0 > 900:
            raise SystemExit(f"{binary} never answered")
        time.sleep(0.2)


def stop(p):
    p.terminate()
    p.wait(timeout=300)


def drop_caches():
    subprocess.run(["sync"])
    subprocess.run(["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=True)


def segment_dirs():
    n = 0
    for sym in os.scandir(DATA):
        if sym.is_dir() and sym.name.startswith("S"):
            for ex in os.scandir(sym.path):
                if ex.is_dir():
                    n += sum(1 for _ in os.scandir(ex.path))
    return n


subprocess.run(["rm", "-rf", DATA])
os.makedirs(DATA)
node, _ = start(OLD, "repair-write.log")
w = socket.create_connection(("127.0.0.1", PORT), timeout=30)
wr = w.makefile("rb")
while wr.readline().strip():
    pass
levels = "\n".join(f"{1000 + i} 1 1" for i in range(20))
payload = "".join(f"MINSERT {s} EX bid 20\n{levels}\n" for s in SYMBOLS).encode()
t_start = time.monotonic()
next_round = t_start
while time.monotonic() - t_start < SECONDS:
    now = time.monotonic()
    if now < next_round:
        time.sleep(next_round - now)
    next_round += 1 / 20
    w.sendall(payload)
    for _ in SYMBOLS:
        assert wr.readline().strip() == b"OK"
        wr.readline()
w.close()
stop(node)
print(f"written by the old build: {segment_dirs()} segment directories", flush=True)

for label, binary in (("before", OLD), ("after-1", NEW), ("after-2", NEW)):
    drop_caches()
    node, took = start(binary, f"repair-{label}.log")
    stop(node)
    with open(os.path.join(ROOT, f"repair-{label}.log"), errors="replace") as f:
        text = f.read()
    m = re.search(r'"msg":"([0-9]+ segment\(s\) written before #166[^"]*)"', text)
    print(f"{label:8s} first answer after {took:6.2f} s   {m.group(1) if m else '(no repair line)'}",
          flush=True)

subprocess.run(["rm", "-rf", DATA])
