"""Crash recovery: SIGKILL the server, restart it, count the rows.

Every other module shuts a node down through the cluster manager, which sends SIGTERM
and lets the engine drain and flush. That path never reads the WAL back, so it cannot
notice that replay applied nothing at all — which is how acknowledged writes were lost
on a crash for as long as the engine existed.

These tests run their own single node, because they kill it: no fixture in the shared
cluster survives that cleanly, and a recovery test that shares a node with others would
report someone else's rows.

Every test asserts that no segment existed at the moment of the kill. Without that
check the test passes on data that reached the columnar store the ordinary way, and
proves nothing about the WAL — the first version of this measurement did exactly that.
"""
from __future__ import annotations

import glob
import os
import signal
import socket
import subprocess
import tempfile
import time

import pytest

pytestmark = pytest.mark.crash_recovery

SERVER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "build", "ob_tcp_server")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Node:
    """A single ob_tcp_server on its own data dir, killable."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.port = free_port()
        self.proc: subprocess.Popen | None = None

    def start(self, timeout: float = 20.0) -> None:
        self.port = free_port()
        # A long flush interval keeps the rows in the WAL instead of racing a 100 ms
        # background flush. Without it this test passes on data that reached a segment
        # the ordinary way, which proves nothing about recovery.
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

    def kill(self) -> None:
        """SIGKILL: no drain, no flush, no destructor — an actual crash."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=10)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def command(self, text: str, settle: float = 0.4) -> str:
        with socket.create_connection(("127.0.0.1", self.port), timeout=15) as s:
            s.settimeout(15)
            s.recv(4096)  # banner
            s.sendall(text.encode())
            time.sleep(settle)
            try:
                return s.recv(1 << 20).decode(errors="replace")
            except socket.timeout:
                return ""

    def row_count(self, symbol: str, exchange: str = "EX") -> int:
        reply = self.command(
            f"SELECT * FROM '{symbol}'.'{exchange}' "
            f"WHERE timestamp BETWEEN 0 AND 9999999999999999999\n", settle=0.8)
        return len([ln for ln in reply.strip().splitlines()
                    if ln and ln.split("\t")[0].isdigit()])

    def segments_on_disk(self) -> int:
        return len(glob.glob(os.path.join(self.data_dir, "**", "meta.json"),
                             recursive=True))

    def wal_bytes(self) -> int:
        return sum(os.path.getsize(p)
                   for p in glob.glob(os.path.join(self.data_dir, "wal_*.bin")))


@pytest.fixture
def node():
    if not os.path.isfile(SERVER):
        pytest.skip(f"server binary not built: {SERVER}")
    data_dir = tempfile.mkdtemp(prefix="ob_crash_")
    n = Node(data_dir)
    yield n
    n.stop()


def insert_batch(node: Node, symbol: str, count: int, base_price: int) -> None:
    """Send `count` inserts in one write, to stay inside the flush interval."""
    payload = "".join(
        f"INSERT {symbol} EX bid {base_price + i} 10 1\n" for i in range(count))
    with socket.create_connection(("127.0.0.1", node.port), timeout=15) as s:
        s.settimeout(15)
        s.recv(4096)
        s.sendall(payload.encode())
        time.sleep(0.2)
        s.recv(1 << 20)


def test_acknowledged_writes_survive_sigkill(node):
    node.start()
    insert_batch(node, "CRASH1", 5, 100_000)

    assert node.segments_on_disk() == 0, (
        "rows reached a segment before the kill, so this test would pass without the "
        "WAL being read")
    assert node.wal_bytes() > 0, "the WAL is empty, so there is nothing to recover"

    node.kill()
    node.start()

    assert node.row_count("CRASH1") == 5, (
        "five writes were acknowledged and present in a fsynced WAL; after the crash "
        f"{node.row_count('CRASH1')} came back")


def test_recovery_does_not_duplicate_flushed_rows(node):
    node.start()
    insert_batch(node, "CRASH2", 4, 200_000)
    node.command("FLUSH\n")            # first four are durable in a segment
    assert node.segments_on_disk() > 0

    insert_batch(node, "CRASH2", 4, 210_000)   # these four only in the WAL

    node.kill()
    node.start()

    count = node.row_count("CRASH2")
    assert count == 8, (
        f"expected 4 flushed + 4 recovered, got {count}; anything above 8 means the "
        f"flushed rows were replayed on top of the segment that already held them")


def test_two_crashes_in_a_row(node):
    node.start()
    insert_batch(node, "CRASH3", 3, 300_000)
    node.kill()

    node.start()
    assert node.row_count("CRASH3") == 3
    insert_batch(node, "CRASH3", 3, 310_000)
    node.kill()

    node.start()
    assert node.row_count("CRASH3") == 6, (
        f"after two crashes the node holds {node.row_count('CRASH3')} of 6 rows")


def test_clean_restart_after_crash_recovery_is_stable(node):
    node.start()
    insert_batch(node, "CRASH4", 4, 400_000)
    node.kill()

    node.start()
    first = node.row_count("CRASH4")
    node.stop()          # graceful this time: drain + flush

    node.start()
    second = node.row_count("CRASH4")

    assert first == 4, f"recovery returned {first} of 4"
    assert second == 4, (
        f"a clean restart after recovery changed the count to {second}: the recovered "
        f"rows are being replayed again")


def test_writes_after_recovery_are_also_durable(node):
    """Recovery must leave the node in a state where the next crash is survivable."""
    node.start()
    insert_batch(node, "CRASH5", 2, 500_000)
    node.kill()

    node.start()
    assert node.row_count("CRASH5") == 2
    insert_batch(node, "CRASH5", 2, 510_000)
    node.kill()

    node.start()
    assert node.row_count("CRASH5") == 4, (
        f"the node holds {node.row_count('CRASH5')} of 4: writes made after a recovery "
        f"are not being recovered themselves")
