"""A node started with `--read-only` refuses writes from the first one (#241).

The flag is configuration on the way in and the engine's state from then on: the engine owns it
since #241, because the engine is what sets it again at every role change, and the server hands it
`--read-only` before the engine opens. Before #241 the engine held a pointer to a member of the
server instead.

Nothing in this battery started a node with the flag until this module. The replicas the fixtures
start are refused writes by their role, in the same words, so a node whose `--read-only` went
nowhere passed every test. Here the node is standalone, so no role refuses a write and the refusal
can only be the flag's - and the control, the same node without the flag, takes the write.
"""

from __future__ import annotations

import socket
import subprocess
import time

import pytest

from conftest import free_port, open_node_log, patience, server_binary_path

pytestmark = pytest.mark.smoke


class Wire:
    """One connection speaking the line protocol by hand.

    An answer that begins `OK` ends in a blank line; any other - `PONG`, `ERR ...`, `STANDALONE` - at
    its first line. Read to the blank line, an `ERR` would wait out the socket's timeout.
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buf = b""
        self.banner = self.answer()

    def answer(self) -> str:
        while True:
            ok = self.buf.startswith(b"OK")
            end = self.buf.find(b"\n\n") if ok else self.buf.find(b"\n")
            if end >= 0:
                size = end + (2 if ok else 1)
                out, self.buf = self.buf[:size], self.buf[size:]
                return out.decode()
            chunk = self.sock.recv(4096)
            if not chunk:
                raise AssertionError(f"the node closed the connection with {self.buf!r} unanswered")
            self.buf += chunk

    def ask(self, line: str) -> str:
        self.sock.sendall(line.encode() + b"\n")
        return self.answer()

    def close(self) -> None:
        self.sock.close()


class Node:
    """A standalone node on a port of its own, with its log in its data directory."""

    def __init__(self, data_dir: str, extra: list[str]):
        self.data_dir = data_dir
        self.port = free_port()
        log = open_node_log(data_dir)
        self.proc = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port), "--data-dir", data_dir,
             "--metrics-port", "0", "--drain-timeout-ms", "1000", *extra],
            stdout=log, stderr=subprocess.STDOUT)
        log.close()

    def log_tail(self) -> str:
        with open(f"{self.data_dir}/node.log", encoding="utf-8", errors="replace") as f:
            return f.read()[-1500:]

    def connect(self) -> Wire:
        deadline = time.monotonic() + patience(30)
        while True:
            try:
                sock = socket.create_connection(("127.0.0.1", self.port), timeout=patience(5))
                return Wire(sock)
            except OSError:
                if self.proc.poll() is not None or time.monotonic() > deadline:
                    raise AssertionError(
                        f"the node never answered on port {self.port}:\n{self.log_tail()}")
                time.sleep(0.1)

    def stop(self) -> None:
        self.proc.terminate()
        try:
            self.proc.wait(timeout=patience(10))
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)


@pytest.fixture
def start_node(tmp_path):
    """Start nodes for one test; every one it started is stopped, whatever the test did."""
    nodes: list[Node] = []
    wires: list[Wire] = []

    def start(name: str, *extra: str) -> Wire:
        data_dir = tmp_path / name
        data_dir.mkdir()
        node = Node(str(data_dir), list(extra))
        nodes.append(node)
        wire = node.connect()
        wires.append(wire)
        return wire

    try:
        yield start
    finally:
        for wire in wires:
            wire.close()
        for node in nodes:
            node.stop()


def test_a_node_started_read_only_refuses_writes_and_answers_reads(start_node):
    wire = start_node("read-only", "--read-only")
    # Standalone, so no role refuses a write here: a refusal below is the flag's.
    assert wire.ask("ROLE") == "STANDALONE\n"
    assert wire.ask("INSERT RO-SYM EX bid 100 5 1") == "ERR read-only replica\n"
    assert wire.ask("FLUSH") == "ERR read-only replica\n"
    assert wire.ask("STATUS").startswith("OK"), "a read-only node still answers reads"


def test_the_same_node_without_the_flag_takes_the_write(start_node):
    # The control the refusal above is read against: same node, same commands, no flag.
    wire = start_node("writable")
    assert wire.ask("ROLE") == "STANDALONE\n"
    assert wire.ask("INSERT RO-SYM EX bid 100 5 1") == "OK\n\n"
    assert wire.ask("FLUSH") == "OK\n\n"
