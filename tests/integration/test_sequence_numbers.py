"""Sequence numbers assigned by a real server, read back off disk.

The wire protocol does not carry a row's sequence number — `format_query_response()` sends
six columns and none of them is it — so a client cannot see these numbers, and asserting on
`SELECT` output would prove nothing. What consumes them is the WAL and the columnar
segments, so that is where these tests look: `meta.json` publishes the highest number in a
segment, which is also what the engine reads at startup to avoid handing one out twice.

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

pytestmark = pytest.mark.smoke

SERVER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "build", "ob_tcp_server")


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

    def command(self, text: str, settle: float = 0.5) -> str:
        with socket.create_connection(("127.0.0.1", self.port), timeout=15) as s:
            s.settimeout(15)
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
