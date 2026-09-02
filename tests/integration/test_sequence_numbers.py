"""Sequence numbers assigned by a real server, read back off disk and off the wire.

What consumes these numbers is the WAL and the columnar segments, so most of these tests look
there: `meta.json` publishes the highest number in a segment, which is also what the engine reads
at startup to avoid handing one out twice.

Since #65 a client can see them too — `SELECT` returns `sequence_number` as a seventh column — so
the last tests here read the same numbers the two ways and require them to agree. Before that the
wire carried six columns and asserting on `SELECT` output would have proved nothing.

Until August 2026 every one of these numbers was 0: `tcp_server.cpp` set the field to zero
with a comment saying the engine assigned it, and the engine copied the zero through.
"""
from __future__ import annotations

import glob
import json
import os
import socket
import subprocess
import tempfile
import time

import pytest
from conftest import server_binary_path

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Node:
    """A single ob_tcp_server on its own data dir.

    Its own node rather than the shared cluster fixture: these tests read the data
    directory directly and restart the process, and both would disturb everyone else.
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.port = 0
        self.proc: subprocess.Popen | None = None

    def start(self, timeout: float = 20.0) -> None:
        self.port = free_port()
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", self.data_dir,
             "--flush-interval-ms", "600000"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=2) as s:
                    s.settimeout(2)
                    s.recv(4096)
                    s.sendall(b"PING\n")
                    if b"PONG" in s.recv(1024):
                        return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(f"server on port {self.port} did not come up")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def command(self, text: str, settle: float = 0.5, timeout: float = 15.0) -> str:
        """Send text, wait `settle`, read whatever is there.

        `timeout` is configurable because FLUSH is synchronous: when the whole integration suite is
        running on a slow machine, a flush can take longer than a default socket timeout allows, and
        the failure then looks like an unreachable server rather than a slow one.
        """
        with socket.create_connection(("127.0.0.1", self.port), timeout=timeout) as s:
            s.settimeout(timeout)
            s.recv(4096)  # banner
            s.sendall(text.encode())
            time.sleep(settle)
            try:
                return s.recv(1 << 20).decode(errors="replace")
            except socket.timeout:
                return ""

    def max_sequence_in_segments(self, symbol: str, exchange: str = "EX") -> int:
        """Highest sequence number published by any segment of this symbol."""
        pattern = os.path.join(self.data_dir, symbol, exchange, "*", "meta.json")
        best = 0
        for path in glob.glob(pattern):
            with open(path, encoding="utf-8") as f:
                meta = json.load(f)
            # A missing key means a segment written before numbers were assigned.
            best = max(best, int(meta.get("max_sequence_number", 0)))
        return best

    def segment_count(self, symbol: str, exchange: str = "EX") -> int:
        return len(glob.glob(os.path.join(self.data_dir, symbol, exchange, "*", "meta.json")))


@pytest.fixture
def node():
    if not os.path.isfile(SERVER):
        pytest.skip(f"server binary not built: {SERVER}")
    data_dir = tempfile.mkdtemp(prefix="ob_seq_")
    n = Node(data_dir)
    yield n
    n.stop()


def insert(node: Node, symbol: str, price: int) -> None:
    reply = node.command(f"INSERT {symbol} EX bid {price} 10 1\n", settle=0.3)
    assert reply.startswith("OK"), f"INSERT refused: {reply!r}"


def test_the_server_numbers_the_writes_it_accepts(node):
    node.start()
    for i in range(3):
        insert(node, "SEQINT", 100_000 + i)
    node.command("FLUSH\n")

    assert node.segment_count("SEQINT") > 0, "nothing was flushed, so there is nothing to read"
    assert node.max_sequence_in_segments("SEQINT") == 3, (
        f"three writes produced a highest sequence number of "
        f"{node.max_sequence_in_segments('SEQINT')}; every production write used to carry 0")


def test_numbers_do_not_restart_from_one_after_a_restart(node):
    node.start()
    for i in range(2):
        insert(node, "SEQRESTART", 200_000 + i)
    node.command("FLUSH\n")
    assert node.max_sequence_in_segments("SEQRESTART") == 2

    node.stop()
    node.start()

    insert(node, "SEQRESTART", 210_000)
    node.command("FLUSH\n")

    assert node.max_sequence_in_segments("SEQRESTART") == 3, (
        "the write after the restart reused a number already on disk, so two rows claim the "
        "same position in the symbol's stream")


def test_two_symbols_are_numbered_separately(node):
    node.start()
    for i in range(3):
        insert(node, "SEQ-ONE", 300_000 + i)
    insert(node, "SEQ-TWO", 400_000)
    node.command("FLUSH\n")

    assert node.max_sequence_in_segments("SEQ-ONE") == 3
    assert node.max_sequence_in_segments("SEQ-TWO") == 1, (
        "the second symbol continued the first symbol's numbering, so one shared counter is "
        "being used for both")


# ── The wire view (#65) ──────────────────────────────────────────────────────


def _rows_from_select(response: str) -> tuple[list[str], list[list[str]]]:
    """Split a SELECT response into (header columns, data rows)."""
    lines = [ln for ln in response.split("\n") if ln]
    assert lines and lines[0] == "OK", f"query failed: {response!r}"
    header = lines[1].split("\t")
    rows = [ln.split("\t") for ln in lines[2:]]
    return header, rows


def test_select_reports_the_sequence_number_as_its_last_column(node):
    """The column exists, is named, and comes last so positional readers do not shift."""
    node.start()
    # One connection for all three writes: the protocol pipelines commands, and a connection per
    # write is load the test does not need.
    node.command("".join(f"INSERT SEQWIRE EX bid {p} 5 1\n" for p in (100_000, 100_100, 100_200)))
    # A timestamp-range SELECT is served from segments, so a write is only visible to it once
    # flushed. The node runs with a ten-minute flush interval, hence the explicit FLUSH.
    node.command("FLUSH\n", settle=1.5, timeout=60)

    header, rows = _rows_from_select(node.command(
        "SELECT * FROM 'SEQWIRE'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n"))

    assert header == ["timestamp_ns", "price", "quantity", "order_count", "side", "level",
                      "sequence_number"], header
    assert rows, "no rows came back"
    for row in rows:
        assert len(row) == 7, f"expected seven columns, got {row}"
        assert int(row[6]) > 0, f"sequence number is unassigned in {row}"


def test_the_numbers_a_client_reads_are_the_numbers_on_disk(node):
    """Two views of the same counter must not disagree.

    The wire value comes from `QueryResult`, the disk value from `meta.json`. A mismatch would mean
    the column shows something other than what the engine stored — a worse outcome than not showing
    it at all.
    """
    node.start()
    node.command("".join(f"INSERT SEQCHECK EX ask {p} 2 1\n" for p in range(100_000, 100_010)))
    node.command("FLUSH\n", settle=1.5, timeout=60)

    on_disk = node.max_sequence_in_segments("SEQCHECK")
    assert on_disk > 0, "the segment published no sequence number"

    _, rows = _rows_from_select(node.command(
        "SELECT * FROM 'SEQCHECK'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n"))
    on_wire = max(int(r[6]) for r in rows)
    assert on_wire == on_disk, (
        f"the client sees {on_wire} as the highest sequence number, the segment says {on_disk}")


def test_sequence_numbers_a_client_sees_have_no_holes_in_them(node):
    """The point of exposing the column: a reader can check its own completeness.

    Ten writes to one symbol are ten consecutive numbers. If the set a client reads back had a gap,
    the client would be right to conclude it is missing a row — so the test asserts there is none.
    """
    node.start()
    writes = 10
    node.command("".join(f"INSERT SEQGAP EX bid {100_000 + i} 1 1\n" for i in range(writes)))
    node.command("FLUSH\n", settle=1.5, timeout=60)

    _, rows = _rows_from_select(node.command(
        "SELECT * FROM 'SEQGAP'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n"))
    seen = sorted({int(r[6]) for r in rows})
    assert len(seen) == writes, f"expected {writes} distinct numbers, got {seen}"
    assert seen == list(range(seen[0], seen[0] + writes)), f"gap in {seen}"
