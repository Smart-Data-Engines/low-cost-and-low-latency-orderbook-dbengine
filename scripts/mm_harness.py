#!/usr/bin/env python3
"""Three multi-master nodes, killed and restarted in a loop, with logs kept on disk.

Written to reproduce roadmap #61, and kept because the integration suite cannot replace it.
`tests/integration/conftest.py` keeps each node's stdout in a pipe, so the line that mattered —
the catch-up decision — was invisible: the suite could tell you rows were missing, not why. Here
every node logs at DEBUG into its own file, which is how the cause was found:

    cycle 0: Peer 3 is behind (peer: file=0 off=174, local: file=0 off=522) — starting catch-up
    cycle 1: Peer 3 is behind (peer: file=0 off=846, local: file=0 off=870) — starting catch-up

The other thing this harness exists for is *repetition*. One outage recovered by luck for months,
because the two nodes' byte offsets happened to line up; the defect only showed from the second
outage on. Any test of catch-up that runs a single outage proves very little.

Row counts are exact on purpose. The first attempt at fixing #61 replaced the missing rows with
duplicated ones — storage is append-only — and a check that only looks for absent prices calls
that a pass.

Usage:
    MMH_CYCLES=4 python3 scripts/mm_harness.py

Needs a built build/ob_tcp_server and a native etcd on PATH (or ETCD env var).
"""
from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.environ.get("OB_SERVER_BINARY", os.path.join(REPO, "build", "ob_tcp_server"))
ETCD = os.environ.get("ETCD", shutil.which("etcd") or "/usr/local/bin/etcd")
ROOT = os.environ.get("MMH_ROOT", "/tmp/ob_mm_harness")
SYMBOL = "MMH-CATCHUP"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def command(port: int, text: str, settle: float = 0.4, timeout: float = 15.0) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.settimeout(timeout)
        s.recv(4096)  # banner
        s.sendall(text.encode())
        time.sleep(settle)
        try:
            return s.recv(1 << 20).decode(errors="replace")
        except socket.timeout:
            return ""


def prices(port: int, symbol: str = SYMBOL) -> list[int]:
    reply = command(
        port,
        f"SELECT * FROM '{symbol}'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n",
        settle=0.8)
    out = []
    for line in reply.strip().splitlines():
        fields = line.split("\t")
        if fields and fields[0].isdigit() and len(fields) > 1:
            out.append(int(fields[1]))
    return sorted(out)


class Node:
    def __init__(self, index: int, etcd_url: str):
        self.index = index
        self.etcd_url = etcd_url
        self.dir = os.path.join(ROOT, f"node{index}")
        os.makedirs(self.dir, exist_ok=True)
        self.ports: tuple[int, int, int, int] = (0, 0, 0, 0)
        self.proc: subprocess.Popen | None = None

    def start(self, reuse_ports: bool = False) -> None:
        # Ports are reused across a restart so the peer registry sees the same node coming
        # back rather than a new one.
        if not reuse_ports or self.ports == (0, 0, 0, 0):
            self.ports = (free_port(), free_port(), free_port(), free_port())
        tcp, metrics, repl, mm = self.ports
        log = open(os.path.join(ROOT, f"node{self.index}.log"), "ab")
        self.proc = subprocess.Popen([
            SERVER, "--port", str(tcp), "--data-dir", self.dir,
            "--metrics-port", str(metrics), "--replication-port", str(repl),
            "--coordinator-endpoints", self.etcd_url, "--node-id", f"node-{self.index}",
            "--multi-master", "--mm-node-id", str(self.index + 1),
            "--mm-replication-port", str(mm), "--log-level", "DEBUG",
        ], stdout=log, stderr=subprocess.STDOUT)

        deadline = time.time() + 25
        while time.time() < deadline:
            try:
                if "PONG" in command(tcp, "PING\n", settle=0.1, timeout=2):
                    return
            except OSError:
                time.sleep(0.3)
        raise RuntimeError(f"node{self.index} did not come up on port {tcp}")

    @property
    def tcp(self) -> int:
        return self.ports[0]

    def kill(self) -> None:
        """SIGKILL: no drain, no flush — an outage, not a shutdown."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=10)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def start_etcd() -> tuple[subprocess.Popen, str]:
    client_port, peer_port = free_port(), free_port()
    url = f"http://127.0.0.1:{client_port}"
    log = open(os.path.join(ROOT, "etcd.log"), "wb")
    proc = subprocess.Popen([
        ETCD, "--name", "mmh", "--data-dir", os.path.join(ROOT, "etcd"),
        "--advertise-client-urls", url, "--listen-client-urls", url,
        "--listen-peer-urls", f"http://127.0.0.1:{peer_port}",
        "--initial-advertise-peer-urls", f"http://127.0.0.1:{peer_port}",
        "--initial-cluster", f"mmh=http://127.0.0.1:{peer_port}",
    ], stdout=log, stderr=subprocess.STDOUT)
    time.sleep(3)
    return proc, url


def dump_stacks(nodes: list[Node]) -> None:
    """Thread stacks of whatever is still alive.

    A hang here has twice been a lock problem inside the engine, and both times the stacks
    named it in seconds while reasoning about it produced wrong answers. `sudo` is needed
    because ptrace_scope blocks attaching to a sibling process.
    """
    for node in nodes:
        if node.proc and node.proc.poll() is None:
            path = os.path.join(ROOT, f"stacks_node{node.index}.txt")
            try:
                out = subprocess.run(
                    ["sudo", "gdb", "-p", str(node.proc.pid), "-batch",
                     "-ex", "thread apply all bt"],
                    capture_output=True, text=True, timeout=120)
                with open(path, "w") as fh:
                    fh.write(out.stdout + out.stderr)
                print(f"  thread stacks for node{node.index}: {path}")
            except Exception as exc:  # noqa: BLE001 - diagnostics must not mask the failure
                print(f"  could not dump stacks for node{node.index}: {exc!r}")


def main() -> int:
    if not os.path.isfile(SERVER):
        print(f"server binary not built: {SERVER}")
        return 2

    cycles = int(os.environ.get("MMH_CYCLES", "4"))
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT)

    etcd, etcd_url = start_etcd()
    nodes = [Node(i, etcd_url) for i in range(3)]
    failures = 0

    try:
        for node in nodes:
            node.start()
        time.sleep(6)  # let the mesh form

        command(nodes[0].tcp, f"INSERT {SYMBOL} EX bid 700000 1 1\n")
        time.sleep(3)
        expected = [700_000]
        print("after the first write:", [prices(n.tcp) for n in nodes])

        victim, writer = nodes[2], nodes[0]
        for cycle in range(cycles):
            victim.kill()
            time.sleep(2)

            base = 710_000 + cycle * 10_000
            for k in range(2):
                price = base + k * 1000
                command(writer.tcp, f"INSERT {SYMBOL} EX bid {price} 2 1\n", settle=0.3)
                expected.append(price)
            time.sleep(2)

            victim.start(reuse_ports=True)

            deadline = time.time() + 40
            got: list[int] = []
            while time.time() < deadline:
                got = prices(victim.tcp)
                if len(got) >= len(expected):
                    break
                time.sleep(1.5)

            missing = sorted(set(expected) - set(got))
            duplicates = len(got) - len(set(got))
            unexpected = sorted(set(got) - set(expected))
            problems = []
            if missing:
                problems.append(f"MISSING {missing}")
            if duplicates:
                problems.append(f"{duplicates} DUPLICATE rows")
            if unexpected:
                problems.append(f"UNEXPECTED {unexpected}")

            verdict = "OK" if not problems else " + ".join(problems)
            print(f"cycle {cycle}: node2 holds {len(got)} rows, {len(set(got))} distinct, "
                  f"expected {len(expected)} — {verdict}")
            if problems:
                failures += 1
                writer_rows = prices(writer.tcp)
                print(f"  writer holds {len(writer_rows)} rows, "
                      f"{len(set(writer_rows))} distinct")
                break
    except Exception as exc:  # noqa: BLE001 - a hang is the interesting case
        print("HANG or ERROR:", repr(exc)[:160])
        dump_stacks(nodes)
        failures += 1
    finally:
        for node in nodes:
            node.stop()
        etcd.terminate()
        try:
            etcd.wait(timeout=10)
        except subprocess.TimeoutExpired:
            etcd.kill()

    print(f"\nlogs: {ROOT}/node*.log")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
