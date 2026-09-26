#!/usr/bin/env python3
"""What a mesh catch-up costs the node that serves it, and how long it takes (#178).

A catch-up read the retained WAL on the io loop under the manager's lock, and every local write
broadcasts under that lock - so a node catching a peer up held its own writes for the length of the
scan. Since #178 a catch-up is rounds that read without the lock. This measures both builds the same
way: three nodes; one killed; `--records` single-level writes to another while it is down; the killed
node restarted; and all along, a probe on the writing node sending one `INSERT` at a time and timing
each answer. It reports the probe's latency before the kill and while the returning node is caught
up, how long the catch-up took from the writer's own log line, and whether the returning node ended
with every row.

    scripts/measure_mesh_catchup.py --server before=<ob_tcp_server> --server after=<ob_tcp_server> \\
        [--records 300000] [--rounds 2]

Builds are alternated ABAB... over `--rounds`. Needs a native etcd on PATH (or ETCD).
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time

ETCD = os.environ.get("ETCD", shutil.which("etcd") or "/usr/local/bin/etcd")
SYMBOLS = [f"MC{i:02d}" for i in range(10)]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Conn:
    """One client connection, one request at a time."""

    def __init__(self, port: int):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=60)
        self.r = self.s.makefile("rb")
        while self.r.readline().strip():
            pass

    def line(self, text: str) -> bytes:
        """A command answered by one line: PING."""
        self.s.sendall(text.encode())
        return self.r.readline().rstrip(b"\n")

    def request(self, text: str) -> list[bytes]:
        """A command answered by lines and a blank one: OK, a header, rows."""
        self.s.sendall(text.encode())
        lines = []
        while True:
            line = self.r.readline()
            if not line:
                raise ConnectionError("closed")
            line = line.rstrip(b"\n")
            if not line:
                return lines
            lines.append(line)

    def close(self) -> None:
        self.s.close()


def start_etcd(root: str) -> tuple[subprocess.Popen, str]:
    peer, client = free_port(), free_port()
    url = f"http://127.0.0.1:{client}"
    proc = subprocess.Popen([
        ETCD, "--name", "mc", "--data-dir", os.path.join(root, "etcd"),
        "--advertise-client-urls", url, "--listen-client-urls", url,
        "--listen-peer-urls", f"http://127.0.0.1:{peer}",
        "--initial-advertise-peer-urls", f"http://127.0.0.1:{peer}",
        "--initial-cluster", f"mc=http://127.0.0.1:{peer}",
    ], stdout=open(os.path.join(root, "etcd.log"), "wb"), stderr=subprocess.STDOUT)
    time.sleep(3)
    return proc, url


class Node:
    def __init__(self, server: str, root: str, index: int, etcd: str):
        self.server, self.index, self.etcd = server, index, etcd
        self.dir = os.path.join(root, f"node{index}")
        self.log = os.path.join(root, f"node{index}.log")
        os.makedirs(self.dir, exist_ok=True)
        self.ports = (free_port(), free_port(), free_port(), free_port())
        self.proc: subprocess.Popen | None = None

    @property
    def tcp(self) -> int:
        return self.ports[0]

    def start(self) -> None:
        tcp, metrics, repl, mm = self.ports
        self.proc = subprocess.Popen([
            self.server, "--port", str(tcp), "--data-dir", self.dir, "--metrics-port", str(metrics),
            "--replication-port", str(repl), "--coordinator-endpoints", self.etcd,
            "--node-id", f"node-{self.index}", "--multi-master", "--mm-node-id", str(self.index + 1),
            "--mm-replication-port", str(mm), "--log-level", "INFO",
        ], stdout=open(self.log, "ab"), stderr=subprocess.STDOUT)
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                c = Conn(tcp)
                ok = c.line("PING\n")
                c.close()
                if b"PONG" in ok:
                    return
            except OSError:
                pass
            time.sleep(0.2)
        raise RuntimeError(f"node{self.index} did not come up")

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=10)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def write(port: int, first_ts: int, per_symbol: int, batch: int = 256) -> None:
    """`per_symbol` single-level MINSERTs of every symbol, pipelined `batch` at a time."""
    c = Conn(port)
    try:
        cmds = []
        for k in range(per_symbol):
            for s, sym in enumerate(SYMBOLS):
                ts = first_ts + k * len(SYMBOLS) + s
                cmds.append(f"MINSERT {sym} EX bid 1 {ts}\n{100_000 + k % 1000} 5 1\n")
        for i in range(0, len(cmds), batch):
            chunk = cmds[i:i + batch]
            c.s.sendall("".join(chunk).encode())
            for _ in chunk:
                line = c.r.readline()
                if not line.startswith(b"OK"):
                    raise RuntimeError(f"write refused: {line!r}")
                c.r.readline()
    finally:
        c.close()


def rows(port: int) -> int:
    c = Conn(port)
    try:
        c.request("FLUSH\n")
        total = 0
        for sym in SYMBOLS:
            reply = c.request(f"SELECT price FROM '{sym}'.'EX'\n")
            total += max(0, len(reply) - 2)     # "OK" and the header
        return total
    finally:
        c.close()


class Probe(threading.Thread):
    """One INSERT at a time on the writing node, each answer timed."""

    def __init__(self, port: int):
        super().__init__(daemon=True)
        self.port, self.samples, self.stop_flag = port, [], threading.Event()

    def run(self) -> None:
        c = Conn(self.port)
        k = 0
        while not self.stop_flag.is_set():
            k += 1
            t0 = time.monotonic()
            c.s.sendall(f"INSERT PROBE EX bid {100_000 + k % 1000} 1 1\n".encode())
            c.r.readline()
            c.r.readline()
            self.samples.append((t0, (time.monotonic() - t0) * 1000.0))
            time.sleep(0.001)
        c.close()


def summary(samples: list[tuple[float, float]], a: float, b: float) -> str:
    xs = sorted(ms for t, ms in samples if a <= t < b)
    if not xs:
        return "no samples"
    return (f"n={len(xs)} p50={statistics.median(xs):.2f} p99={xs[int(len(xs) * 0.99)]:.2f} "
            f"max={xs[-1]:.2f} ms")


def one_run(label: str, server: str, records: int, root: str) -> None:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    etcd, url = start_etcd(root)
    nodes = [Node(server, root, i, url) for i in range(3)]
    try:
        for n in nodes:
            n.start()
        time.sleep(3)
        writer, returning = nodes[0], nodes[2]
        write(writer.tcp, 1_700_000_000_000_000_000, 10)
        time.sleep(3)
        probe = Probe(writer.tcp)
        probe.start()
        time.sleep(5)
        base_a, base_b = time.monotonic() - 5, time.monotonic()

        returning.kill()
        write(writer.tcp, 1_700_000_001_000_000_000, records // len(SYMBOLS))
        # The writer's vector is refreshed at a checkpoint (#180), so without this the catch-up at
        # the reconnect could find nothing to send and wait for a reconciliation - in either build.
        c = Conn(writer.tcp)
        c.request("FLUSH\n")
        c.close()
        time.sleep(2)
        before_restart = os.path.getsize(writer.log)
        restart = time.monotonic()
        returning.start()

        expected = rows(writer.tcp)
        got, deadline = -1, time.monotonic() + 300
        while time.monotonic() < deadline:
            got = rows(returning.tcp)
            if got >= expected:
                break
            time.sleep(1.0)
        caught_up = time.monotonic()
        time.sleep(2)
        probe.stop_flag.set()
        probe.join(timeout=5)

        with open(writer.log, "rb") as f:
            f.seek(before_restart)
            log = f.read().decode(errors="replace")
        finished = [m.group(0) for m in re.finditer(r"Catch-up to peer 3 finished[^\"]*", log)]
        print(f"{label}: returning node holds {got} of {expected} rows, "
              f"{caught_up - restart:.1f} s after its restart", flush=True)
        print(f"{label}: probe before the kill   {summary(probe.samples, base_a, base_b)}", flush=True)
        print(f"{label}: probe during catch-up   {summary(probe.samples, restart, caught_up)}",
              flush=True)
        for line in finished[:2]:
            print(f"{label}: writer said: {line[:220]}", flush=True)
        slowest = sorted((ms, t) for t, ms in probe.samples if restart <= t < caught_up)[-5:]
        print(f"{label}: slowest probes, ms at s after the restart: "
              + ", ".join(f"{ms:.1f} at {t - restart:.2f}" for ms, t in reversed(slowest)), flush=True)
    finally:
        for n in nodes:
            n.stop()
        etcd.terminate()
        etcd.wait(timeout=10)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", action="append", required=True, help="label=path")
    ap.add_argument("--records", type=int, default=300_000)
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--root", default=os.path.join(os.getcwd(), "mesh-catchup-runs"))
    args = ap.parse_args()
    servers = [s.split("=", 1) for s in args.server]
    for r in range(args.rounds):
        for label, path in (servers if r % 2 == 0 else list(reversed(servers))):
            print(f"== round {r + 1} {label}", flush=True)
            one_run(label, path, args.records, os.path.join(args.root, label))
    return 0


if __name__ == "__main__":
    sys.exit(main())
