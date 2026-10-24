"""#246, stage 2: a query's segments read on the scan pool, on live nodes.

Since #249 `boost`, the default profile, gives the pool a thread per usable CPU, so the battery's
nodes read through it wherever their machine has two CPUs or more. These tests start a node with
`--scan-threads 2` beside one with `--scan-threads 0`, write both the same rows, and hold every
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
import threading
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
    plain = Node(tmp_path_factory.mktemp("plain"), "--scan-threads", "0")
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


def test_the_default_profile_sizes_the_pool_to_the_machine(tmp_path) -> None:
    """#249: a node started with no `--scan-threads` has the pool `--print-config` says the profile
    chose for this machine - a thread per usable CPU, none on one - and where it has one, a scan of
    many rows with every other loop idle divides."""
    printed = subprocess.run([server_binary_path(), "--print-config"], capture_output=True, text=True,
                             timeout=patience(30), check=True).stdout
    chosen = re.search(r"^\s*scan-threads\s+(\d+)\s+\(profile\)", printed, re.MULTILINE)
    assert chosen, printed
    node = Node(tmp_path / "default")
    try:
        assert node.metric("ob_scan_threads") == int(chosen.group(1))
        if int(chosen.group(1)) >= 2:
            load(node.port)
            ask(node.port, f"SELECT * {ALL}")
            parts = node.metric("ob_scan_parts_pooled_total") + node.metric("ob_scan_parts_inline_total")
            assert parts >= SEGMENTS, f"{parts} parts read on a default node, its other loops idle"
            assert node.metric("ob_scan_undivided_total") == 0
    finally:
        node.close()


def test_a_node_whose_every_loop_serves_keeps_its_scans_on_their_loops(tmp_path) -> None:
    """#249: with a connection on each of a default node's client loops, every one asking the bucket
    query back to back, most scans find no CPU to spare and read on their own loop - counted - and
    every answer is still the bytes of one read without the pool."""
    printed = subprocess.run([server_binary_path(), "--print-config"], capture_output=True, text=True,
                             timeout=patience(30), check=True).stdout
    loops = int(re.search(r"^\s*io-threads\s+(\d+)", printed, re.MULTILINE).group(1))
    pool = int(re.search(r"^\s*scan-threads\s+(\d+)", printed, re.MULTILINE).group(1))
    if pool < 2:
        pytest.skip(f"a scan pool of {pool} thread(s) on this machine: nothing to keep from dividing")
    node = Node(tmp_path / "busy")
    try:
        load(node.port)
        sql = f"SELECT {AGGREGATES} {ALL} GROUP BY TIME_BUCKET(10m)"
        expected = ask(node.port, sql)
        assert expected.startswith(b"OK"), expected[:200]
        stop_at = time.monotonic() + 3
        wrong: list[bytes] = []

        def asker() -> None:
            while time.monotonic() < stop_at:
                got = ask(node.port, sql)
                if got != expected:
                    wrong.append(got[:200])

        threads = [threading.Thread(target=asker) for _ in range(loops)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not wrong, wrong[0]
        assert node.metric("ob_scan_undivided_total") > 0, \
            f"{loops} loops each serving a connection, and no scan was kept on its loop"
    finally:
        node.close()
