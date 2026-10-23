"""#246, stage 2: a query's segments read on the scan pool, on live nodes.

The pool is off under every profile, so nothing else in this battery reads through it: these tests
start a node with `--scan-threads 2` beside one without, write both the same rows, and hold every
answer of the first to the second's bytes - rows, narrowed rows, a LIMIT that ends inside a
segment and buckets of every row aggregate, both buckets inside one segment and buckets gathering
two or all of them, whose aggregates the parts' are merged into. The pool node's /metrics then has
to say the pool read something, or the comparison was of two nodes reading the same way.

The rows are eight segments of 6 000, each in an hour of its own and with compaction off, so that no
merge folds them into one: a scan the pool divides needs 32 768 rows in range and no segment with
three quarters of them.
"""

from __future__ import annotations

import re
import signal
import socket
import subprocess
import time
import urllib.request

import pytest

from conftest import free_port, patience, send_command, server_binary_path

pytestmark = pytest.mark.smoke

HOUR = 3_600_000_000_000
BASE = 20_718 * 86_400 * 1_000_000_000          # a whole UTC day
SEGMENTS = 8
UPDATES = 300                                   # each 20 levels: 6 000 rows a segment
LEVELS = 20


class Node:
    def __init__(self, tmp_path, *args: str) -> None:
        self.port = free_port()
        self.metrics_port = free_port()
        tmp_path.mkdir(parents=True, exist_ok=True)
        self.log = open(tmp_path / "node.log", "w", encoding="utf-8")
        self.process = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port), "--data-dir", str(tmp_path / "data"),
             "--metrics-port", str(self.metrics_port), "--drain-timeout-ms", "2000",
             "--compaction", "off", *args],
            stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + patience(30)
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"node exited with {self.process.returncode}: {self.tail()}")
            try:
                if "PONG" in send_command(self.port, "PING", timeout=1.0):
                    return
            except Exception:
                time.sleep(0.1)
        raise AssertionError(f"node never answered PING: {self.tail()}")

    def tail(self) -> str:
        self.log.flush()
        with open(self.log.name, "r", encoding="utf-8") as handle:
            return handle.read()[-2000:]

    def metric(self, name: str) -> float:
        text = urllib.request.urlopen(f"http://127.0.0.1:{self.metrics_port}/metrics",
                                      timeout=10).read().decode()
        found = re.search(rf"^{name}(?:\{{[^}}]*\}})? (\S+)$", text, re.M)
        assert found, f"{name} is not in /metrics"
        return float(found.group(1))

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=patience(15))
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=patience(15))
        self.log.close()


def ask(port: int, sql: str) -> bytes:
    """One answer, whole: to its blank line, or its one line when it is an error."""
    with socket.create_connection(("127.0.0.1", port), timeout=patience(60)) as sock:
        def reply() -> bytes:
            data = bytearray()
            while True:
                chunk = sock.recv(1 << 20)
                assert chunk, "the node closed the connection"
                data += chunk
                if data.startswith(b"ERR") and b"\n" in data:
                    return bytes(data)
                if data.endswith(b"\n\n"):
                    return bytes(data)
        reply()                                   # the banner
        sock.sendall(sql.encode() + b"\n")
        return reply()


def load(port: int) -> None:
    """Eight segments, each its own hour and its own FLUSH, prices and quantities moving - and each
    segment's from its own start, or a bucket's FIRST and LAST would read the same from any of
    them and the merge of parts could take the wrong one unseen."""
    with socket.create_connection(("127.0.0.1", port), timeout=patience(60)) as sock:
        sock.recv(4096)                           # the banner
        for segment in range(SEGMENTS):
            lines = []
            for u in range(UPDATES):
                ts = BASE + segment * HOUR + u * 1_000_000_000
                side = "bid" if u % 2 == 0 else "ask"
                lines.append(f"MINSERT SP EX {side} {LEVELS} {ts}")
                for level in range(LEVELS):
                    price = (100_000 + segment * 100 + (u % 37) * 10
                             + (-level if side == "bid" else level + 1))
                    lines.append(f"{price} {1 + (u * 7 + level + segment * 3) % 50}")
            sock.sendall(("\n".join(lines) + "\nFLUSH\n").encode())
            want = b"OK\n\n" * (UPDATES + 1)
            got = bytearray()
            while len(got) < len(want):
                chunk = sock.recv(len(want) - len(got))
                assert chunk, "the node closed the connection during the load"
                got += chunk
            assert bytes(got) == want, bytes(got)[:200]


@pytest.fixture(scope="module")
def two_nodes(tmp_path_factory):
    plain = Node(tmp_path_factory.mktemp("plain"))
    pooled = None
    try:
        pooled = Node(tmp_path_factory.mktemp("pooled"), "--scan-threads", "2")
        load(plain.port)
        load(pooled.port)
        yield plain, pooled
    finally:
        plain.close()
        if pooled is not None:
            pooled.close()


ALL = f"FROM 'SP'.'EX' WHERE timestamp BETWEEN {BASE} AND {BASE + SEGMENTS * HOUR}"


AGGREGATES = ("COUNT(*), FIRST(price), LAST(price), MIN(quantity), MAX(price), SUM(quantity), "
              "AVG(price), VWAP(price)")


@pytest.mark.parametrize("sql", [
    f"SELECT * {ALL}",
    f"SELECT price, quantity, price {ALL}",
    f"SELECT timestamp, level {ALL} AND side = 1 AND price BETWEEN 100100 AND 100300",
    f"SELECT * {ALL} LIMIT 7000",
    f"SELECT sequence_number {ALL} LIMIT 25000",
    f"SELECT {AGGREGATES} {ALL} GROUP BY TIME_BUCKET(10m)",
    f"SELECT {AGGREGATES} {ALL} GROUP BY TIME_BUCKET(2h)",
    f"SELECT {AGGREGATES} {ALL} AND side = 0 GROUP BY TIME_BUCKET(1d)",
])
def test_an_answer_read_on_the_pool_is_the_bytes_of_one_read_without_it(two_nodes, sql) -> None:
    plain, pooled = two_nodes
    expected = ask(plain.port, sql)
    assert expected.startswith(b"OK"), expected[:200]
    assert ask(pooled.port, sql) == expected


def test_the_pool_read_the_parts(two_nodes) -> None:
    """A node with `--scan-threads 2` says so, and its scans were divided: without that, the
    comparison above held two nodes reading the same way to each other."""
    plain, pooled = two_nodes
    assert pooled.metric("ob_scan_threads") == 2
    assert plain.metric("ob_scan_threads") == 0
    ask(pooled.port, f"SELECT * {ALL}")
    parts = pooled.metric("ob_scan_parts_pooled_total") + pooled.metric("ob_scan_parts_inline_total")
    assert parts >= SEGMENTS, f"{parts} parts read on a node whose every whole-range scan divides"
    assert plain.metric("ob_scan_parts_pooled_total") == 0
