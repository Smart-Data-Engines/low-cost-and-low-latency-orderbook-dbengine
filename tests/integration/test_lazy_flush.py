"""A flush tick seals a store only when it is due, and a crash loses and duplicates nothing (#165 2a).

A tick used to write a segment for every symbol that had received a row since the last one: at 256
symbols about two thousand files and a syncfs() every tick, and a node gained one segment per active
symbol per tick. Rows now wait in blocks, readable, until their store is due - and the checkpoint
claims only what no block still holds, so a crash replays the rest from the WAL.

These run their own node, because they kill it - the same reason as `test_crash_recovery.py`. And
like those, each asserts what was on the disk at the moment of the kill: without that, a test that
passes on rows that reached a segment proves nothing about the ones that had not.
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
from conftest import ClusterManager, server_binary_path

pytestmark = pytest.mark.crash_recovery

SERVER = server_binary_path()
SEAL_ROWS = 65_536   # Engine::kSealRows


class Node:
    """One ob_tcp_server on its own data dir, with the flush tick running, killable."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.port = 0
        self.proc: subprocess.Popen | None = None

    def start(self, timeout: float = 30.0) -> None:
        self.port = ClusterManager.find_free_port()
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", self.data_dir,
             "--metrics-port", "0", "--flush-interval-ms", "50"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + timeout
        while time.time() < deadline:
            # Not ask(): `PONG` is one line with no blank one after it, and ask() reads to a blank
            # line - the first version of this waited out its socket timeout on every attempt.
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=2) as s:
                    f = s.makefile("rb")
                    while f.readline().strip():
                        pass
                    s.sendall(b"PING\n")
                    if f.readline().strip() == b"PONG":
                        return
            except OSError:
                time.sleep(0.2)
        # A start that failed leaves nothing running, whoever called it: the first version of the
        # fixture called this outside its `try`, and three servers outlived their run by 11 hours.
        self.proc.kill()
        self.proc.wait(timeout=10)
        raise RuntimeError(f"server on port {self.port} did not come up")

    def ask(self, command: str, timeout: float = 30.0) -> list[str]:
        with socket.create_connection(("127.0.0.1", self.port), timeout=timeout) as s:
            f = s.makefile("rb")
            while f.readline().strip():
                pass
            s.sendall((command + "\n").encode())
            out: list[str] = []
            while True:
                line = f.readline().decode().rstrip("\n")
                if line == "":
                    return out
                out.append(line)
                if len(out) == 1 and line.startswith("ERR"):
                    return out

    def minsert(self, symbol: str, levels: int, first_price: int = 1000) -> None:
        body = "\n".join(f"{first_price + i} 1 1" for i in range(levels))
        reply = self.ask(f"MINSERT {symbol} EX bid {levels}\n{body}")
        assert reply and reply[0].startswith("OK"), reply

    def rows(self, symbol: str) -> list[str]:
        lines = self.ask(f"SELECT * FROM '{symbol}'.'EX'")
        assert lines and lines[0] == "OK", lines
        return lines[2:]

    def kill(self) -> None:
        """SIGKILL: no drain, no seal, no destructor - an actual crash."""
        assert self.proc is not None and self.proc.poll() is None, "the node died before the kill"
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=10)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=60)


def segments(data_dir: str, symbol: str) -> int:
    return len(glob.glob(os.path.join(data_dir, symbol, "EX", "*", "meta.json")))


def eventually(done, within: float = 20.0) -> bool:
    deadline = time.time() + within
    while time.time() < deadline:
        if done():
            return True
        time.sleep(0.1)
    return done()


@pytest.fixture
def node():
    with tempfile.TemporaryDirectory(prefix="ob_lazy_flush_") as d:
        n = Node(d)
        try:
            n.start()
            yield n
        finally:
            n.stop()


def test_rows_waiting_in_blocks_survive_a_kill(node):
    for symbol in ("LZA", "LZB", "LZC"):
        node.minsert(symbol, 20)
    # Readable from the blocks the ticks drained them into, and not written: the premise.
    assert eventually(lambda: all(len(node.rows(s)) == 20 for s in ("LZA", "LZB", "LZC")))
    time.sleep(0.5)   # ten ticks at 50 ms, none of which may seal a store this small
    for symbol in ("LZA", "LZB", "LZC"):
        assert segments(node.data_dir, symbol) == 0, (
            f"{symbol} was sealed before the kill, so this would prove nothing about blocks")
    node.kill()
    node.start()
    for symbol in ("LZA", "LZB", "LZC"):
        assert len(node.rows(symbol)) == 20, f"{symbol}'s rows in blocks were lost in the crash"


def test_a_kill_between_one_store_sealed_and_another_waiting_loses_and_repeats_nothing(node):
    # LZS goes over the rows threshold and a tick seals it; LZW stays under and waits. The checkpoint
    # after that seal may claim only up to LZW's first block, so a restart replays LZW - and LZS's
    # segment, sealed past that claim, is rebuilt from the WAL rather than kept beside its replay.
    levels = 1000
    for i in range(SEAL_ROWS // levels + 1):
        node.minsert("LZS", levels, first_price=1000 + i)
    node.minsert("LZW", 30)
    assert eventually(lambda: segments(node.data_dir, "LZS") > 0), "LZS was never sealed"
    assert segments(node.data_dir, "LZW") == 0, "LZW was sealed, so the kill tests nothing waiting"
    sealed_rows = len(node.rows("LZS"))
    assert sealed_rows == (SEAL_ROWS // levels + 1) * levels
    node.kill()
    node.start()
    assert len(node.rows("LZW")) == 30, "the waiting store's rows were lost"
    assert len(node.rows("LZS")) == sealed_rows, "the sealed store's rows were lost or doubled"


def test_flush_seals_every_store(node):
    for symbol in ("LZF", "LZG"):
        node.minsert(symbol, 10)
    assert eventually(lambda: len(node.rows("LZF")) == 10 and len(node.rows("LZG")) == 10)
    assert node.ask("FLUSH")[0] == "OK"
    assert segments(node.data_dir, "LZF") == 1
    assert segments(node.data_dir, "LZG") == 1
    assert len(node.rows("LZF")) == 10, "a sealed row was answered twice or not at all"
