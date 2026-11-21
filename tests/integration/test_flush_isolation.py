"""#261: a FLUSH answered off the client loop, on a live node.

A FLUSH wrote the rows waiting into segments on its client loop, and every session on that loop - its
`PING`, its writes, its reads - waited for as long as that took: on the ARM test host 112-180 ms for
500 000 rows of 1 000 books. Past its refusals a FLUSH is answered on the threads of #260
(`--large-read-threads`) now, and its loop goes on serving the others; the session that asked waits for
its answer, and its later commands and its socket wait with it.

One client loop (`--io-threads 1`), so every session is on it, and no flush in the background
(`--flush-interval-ms 600000`), so that the FLUSH writes every row. The control is the same node with
`--large-read-threads 0`, as every node was before #261.
"""
from __future__ import annotations

import json
import re
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

from conftest import free_port, patience, server_binary_path

pytestmark = pytest.mark.large_response

BOOKS = 1000
LEVELS = 300

# A PING every half millisecond on a session of its own, until a line comes on stdin; then each PING's
# send time and round trip, in nanoseconds of the monotonic clock every process on the machine reads.
PINGER = r"""
import json, select, socket, sys, time
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=float(sys.argv[2]))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
buf = b""
while b"\n\n" not in buf:
    buf += s.recv(65536)
print("ready", flush=True)
samples = []
while not select.select([sys.stdin], [], [], 0)[0]:
    sent = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    s.sendall(b"PING\n")
    got = b""
    while not got.endswith(b"\n"):
        got += s.recv(64)
    samples.append((sent, time.clock_gettime_ns(time.CLOCK_MONOTONIC) - sent))
    time.sleep(0.0005)
print(json.dumps(samples), flush=True)
"""


class Node:
    def __init__(self, tmp: Path, *extra: str) -> None:
        self.port = free_port()
        self.metrics_port = free_port()
        self.log = open(tmp / "node.log", "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port), "--data-dir", str(tmp / "data"),
             "--metrics-port", str(self.metrics_port), "--io-threads", "1",
             "--flush-interval-ms", "600000", *extra],
            stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + patience(30)
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"the node exited with {self.proc.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=1):
                    return
            except OSError:
                time.sleep(0.1)
        raise AssertionError("the node never accepted a connection")

    def metric(self, name: str) -> float:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.metrics_port}/metrics",
                                    timeout=patience(10)) as resp:
            body = resp.read().decode(errors="replace")
        match = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)$", body, re.M)
        assert match, f"{name} is not exported"
        return float(match.group(1))

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=patience(30))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()


def connect(port: int) -> socket.socket:
    s = socket.create_connection(("127.0.0.1", port), timeout=patience(120))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    buf = b""
    while b"\n\n" not in buf:
        buf += s.recv(65536)
    return s


def answers(s: socket.socket, n: int) -> list[bytes]:
    """The next `n` answers: `OK` through its empty line, or one line of anything else."""
    out: list[bytes] = []
    buf = bytearray()
    looked = 0
    while len(out) < n:
        chunk = s.recv(1 << 20)
        assert chunk, "the session was closed"
        buf += chunk
        while len(out) < n and buf:
            if buf.startswith(b"OK"):
                end = buf.find(b"\n\n", max(0, looked - 1))
                if end < 0:
                    looked = len(buf)
                    break
                out.append(bytes(buf[:end + 2]))
                del buf[:end + 2]
            else:
                end = buf.find(b"\n")
                if end < 0:
                    break
                out.append(bytes(buf[:end + 1]))
                del buf[:end + 1]
            looked = 0
    return out


def write_books(s: socket.socket, tag: str) -> None:
    """LEVELS levels in each of BOOKS books, waiting for the FLUSH to write them: a MINSERT a book, 50
    in flight at a time."""
    levels = "".join(f"{1_000_000 + i} 10 1\n" for i in range(LEVELS))
    for first in range(0, BOOKS, 50):
        batch = "".join(f"MINSERT {tag}{b} EX bid {LEVELS}\n{levels}" for b in range(first, first + 50))
        s.sendall(batch.encode())
        got = answers(s, 50)
        assert all(a == b"OK\n\n" for a in got), got[:3]


def longest_ping_beside_a_flush(node: Node, tag: str) -> float:
    """The longest round trip, in seconds, of a PING sent on a second session while a FLUSH of
    BOOKS x LEVELS rows is answered on the first. The PINGs come from a process of their own (pitfall
    642), and only those sent while the FLUSH was answered count."""
    writer = connect(node.port)
    try:
        write_books(writer, tag)
        clock = time.CLOCK_MONOTONIC
        pinger = subprocess.Popen([sys.executable, "-c", PINGER, str(node.port), str(patience(120))],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            assert pinger.stdout is not None
            assert pinger.stdout.readline().strip() == "ready", "the pinger did not connect"
            time.sleep(0.05)
            began = time.clock_gettime_ns(clock)
            writer.sendall(b"FLUSH\n")
            (answer,) = answers(writer, 1)
            ended = time.clock_gettime_ns(clock)
            out, _ = pinger.communicate("stop\n", timeout=patience(30))
        finally:
            if pinger.poll() is None:
                pinger.kill()
                pinger.wait()
        assert answer == b"OK\n\n", answer
        during = [rtt for sent, rtt in json.loads(out) if began <= sent <= ended]
        assert during, "no PING was sent while the FLUSH was answered"
        return max(during) / 1e9
    finally:
        writer.close()


@pytest.fixture(scope="module")
def nodes(tmp_path_factory):
    off = Node(tmp_path_factory.mktemp("flush_off_the_loop"))
    on = Node(tmp_path_factory.mktemp("flush_on_the_loop"), "--large-read-threads", "0")
    try:
        yield off, on
    finally:
        off.stop()
        on.stop()


def test_a_flush_does_not_hold_the_sessions_beside_it(nodes):
    off, on = nodes
    held = longest_ping_beside_a_flush(on, "H")
    flushes = off.metric("ob_off_loop_flushes_total")
    waits = off.metric("ob_off_loop_flush_wait_seconds_count")
    beside = longest_ping_beside_a_flush(off, "B")
    # On the loop a PING waits for the whole FLUSH; off it, for no more than a tenth of that.
    assert beside < held / 10, f"a PING beside the FLUSH waited {beside * 1000:.1f} ms, " \
                               f"{held * 1000:.1f} ms with the FLUSH on the loop"
    assert off.metric("ob_off_loop_flushes_total") == flushes + 1
    assert off.metric("ob_off_loop_flush_wait_seconds_count") == waits + 1
    assert off.metric("ob_off_loop_flushes_in_flight") == 0
    assert on.metric("ob_off_loop_flushes_total") == 0, "a FLUSH left a loop with no threads"


def test_a_sessions_answers_come_in_the_order_it_asked_around_a_flush(nodes):
    off, _ = nodes
    s = connect(off.port)
    try:
        write_books(s, "O")
        before = off.metric("ob_off_loop_flushes_total")
        # A row, and the FLUSH that makes it visible, in one write...
        s.sendall(b"INSERT FX EX bid 4242 1 1\nFLUSH\n")
        deadline = time.monotonic() + patience(10)
        while off.metric("ob_off_loop_flushes_total") < before + 1:
            assert time.monotonic() < deadline, "the FLUSH did not leave the loop"
            time.sleep(0.001)
        assert off.metric("ob_off_loop_flushes_in_flight") >= 1, "the FLUSH was over before the test"
        # ...and, while the FLUSH is written off the loop, a read of the row and a PING. The socket
        # keeps them until the FLUSH's answer is queued, so the read sees what the FLUSH wrote.
        s.sendall(b"SELECT price FROM 'FX'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n"
                  b"PING\n")
        got = answers(s, 4)
        assert got[0] == b"OK\n\n" and got[1] == b"OK\n\n", got[:2]
        assert got[2].startswith(b"OK\nprice\n") and b"4242" in got[2], got[2][:80]
        assert got[3] == b"PONG\n", got[3]
    finally:
        s.close()


def test_a_read_only_node_refuses_a_flush_on_its_loop(tmp_path):
    node = Node(tmp_path, "--read-only")
    s = connect(node.port)
    try:
        s.sendall(b"FLUSH\n")
        (answer,) = answers(s, 1)
        assert answer.startswith(b"ERR") and b"read-only" in answer, answer
        assert node.metric("ob_off_loop_flushes_total") == 0, "a refused FLUSH left the loop"
    finally:
        s.close()
        node.stop()
