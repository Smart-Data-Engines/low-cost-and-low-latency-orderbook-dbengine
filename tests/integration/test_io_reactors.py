"""More than one client event loop: `--io-threads N` (stage 2 of using the whole machine).

The reactor holding the listening socket accepts every connection and deals it to one of N reactors
in turn; a connection stays on the reactor it was dealt to for its whole life, because a `Session`
is not thread-safe. What this module holds is the part of that design a client can observe:

- the deal is exact rather than statistical — N consecutive connections land on N reactors;
- every reactor serves writes and reads, and a pipelined batch keeps its order on any of them;
- `--max-sessions` is one limit for the server, not one per reactor;
- a subscription made on one reactor is fed by a write made on another, which is the one path
  where two reactors' work meets;
- a drain with sessions on several reactors ends at one deadline, measured from one moment.

Which reactor a connection went to is read from the node's log, where each adoption is one INFO
line naming the reactor and the `conn_id`. That is the operator's view of the same fact, so it is
the one worth pinning.
"""

from __future__ import annotations

import re
import signal
import socket
import tempfile
import time
import urllib.request
from pathlib import Path

import pytest

from conftest import patience
from test_auth import Node
from test_pipelined_answers import commands_for, connect, read_exactly, read_response

pytestmark = pytest.mark.smoke

ADOPTED = re.compile(r"Reactor (\d+) adopted fd=\d+ conn_id=(\d+)")


class ReactorNode(Node):
    """`Node` with the flags this module is about."""

    def __init__(self, tmp: Path, extra: list[str], *, metrics: bool = False):
        super().__init__(tmp, metrics=metrics)
        self.extra = extra

    def argv(self) -> list[str]:
        return super().argv() + self.extra

    def adoptions(self) -> dict[int, int]:
        """conn_id -> reactor, from the node's own log."""
        return {int(conn): int(reactor) for reactor, conn in ADOPTED.findall(self.log_text())}

    def active_sessions(self) -> int:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.metrics_port}/metrics",
                                    timeout=patience(10)) as r:
            body = r.read().decode()
        for line in body.splitlines():
            if line.startswith("ob_active_sessions") and not line.startswith("#"):
                return int(float(line.rsplit(" ", 1)[1]))
        raise AssertionError("ob_active_sessions is not in /metrics, so this test cannot wait on it")

    def wait_for_sessions(self, n: int, limit: float = 10.0) -> None:
        deadline = time.monotonic() + patience(limit)
        seen = self.active_sessions()
        while seen != n and time.monotonic() < deadline:
            time.sleep(0.02)
            seen = self.active_sessions()
        assert seen == n, f"ob_active_sessions stayed at {seen}, never {n}"


def started(extra: list[str], metrics: bool = False):
    d = tempfile.TemporaryDirectory(prefix="ob_reactors_")
    node = ReactorNode(Path(d.name), extra, metrics=metrics)
    node.start()
    return d, node


@pytest.fixture
def four_reactors():
    d, node = started(["--io-threads", "4"], metrics=True)
    try:
        yield node
    finally:
        node.stop()
        d.cleanup()


def test_consecutive_connections_are_dealt_to_every_reactor_in_turn(four_reactors):
    node = four_reactors
    node.wait_for_sessions(0)   # the readiness probe has gone
    socks = [connect(node.port) for _ in range(8)]
    try:
        for s in socks:
            s.sendall(b"PING\n")
            assert read_response(s, bytearray()) == b"PONG\n"
        dealt = node.adoptions()
        newest = sorted(dealt)[-8:]
        reactors = [dealt[c] for c in newest]
        assert sorted(reactors) == [0, 0, 1, 1, 2, 2, 3, 3], (
            f"eight consecutive connections went to reactors {reactors}; dealt in turn over four, "
            f"each must have two")
        assert all(reactors[i] != reactors[i + 1] for i in range(7)), (
            f"two consecutive connections shared a reactor: {reactors}")
    finally:
        for s in socks:
            s.close()


def test_every_reactor_serves_writes_and_reads_and_keeps_pipelined_order(four_reactors):
    node = four_reactors
    # The reference, one command at a time on one connection.
    ref = connect(node.port)
    pending = bytearray()
    expected = b""
    for command in commands_for("REF"):
        ref.sendall((command + "\n").encode())
        expected += read_response(ref, pending)
    ref.close()
    assert b"\t100\t" in expected, expected   # the control: the book it wrote is in the answer

    socks = [connect(node.port) for _ in range(4)]
    try:
        for i, s in enumerate(socks):
            s.sendall(("\n".join(commands_for(f"R{i}")) + "\n").encode())
        for i, s in enumerate(socks):
            got = read_exactly(s, len(expected), bytearray())
            assert got == expected, f"connection {i} answered differently from one at a time"
        reactors = {node.adoptions()[c] for c in sorted(node.adoptions())[-4:]}
        assert reactors == {0, 1, 2, 3}, f"the four connections were served by {reactors}"
    finally:
        for s in socks:
            s.close()


def test_the_session_limit_is_one_limit_for_the_server():
    d, node = started(["--io-threads", "4", "--max-sessions", "3"], metrics=True)
    held = []
    try:
        node.wait_for_sessions(0)
        for _ in range(3):
            held.append(connect(node.port))
        node.wait_for_sessions(3)
        s = socket.create_connection(("127.0.0.1", node.port), timeout=patience(10))
        s.settimeout(patience(10))
        answer = s.recv(4096)
        s.close()
        assert answer == b"ERR server full\n", (
            f"a fourth connection against --max-sessions 3 and four reactors got {answer!r}. A "
            f"limit counted per reactor would admit twelve")
        # The control: one released place is one admitted connection.
        held.pop().close()
        node.wait_for_sessions(2)
        held.append(connect(node.port))
    finally:
        for s in held:
            s.close()
        node.stop()
        d.cleanup()


def test_a_subscription_is_fed_by_a_write_made_on_another_reactor(four_reactors):
    node = four_reactors
    node.wait_for_sessions(0)
    sub = connect(node.port)
    writer = connect(node.port)
    try:
        dealt = node.adoptions()
        sub_conn, writer_conn = sorted(dealt)[-2:]
        assert dealt[sub_conn] != dealt[writer_conn], (
            "the subscriber and the writer landed on one reactor, so this test would not cross")
        sub.sendall(b"SUBSCRIBE * FROM 'XREACT'.'EX'\n")
        ack = read_response(sub, bytearray())
        assert ack.startswith(b"OK SUB "), ack
        writer.sendall(b"INSERT XREACT EX bid 4242 7 1\n")
        assert read_response(writer, bytearray()) == b"OK\n\n"
        sub.settimeout(patience(10))
        pushed = b""
        deadline = time.monotonic() + patience(10)
        while b"\t4242\t" not in pushed and time.monotonic() < deadline:
            pushed += sub.recv(65536)
        assert b"PUSH " in pushed and b"\t4242\t" in pushed, (
            f"a write on reactor {dealt[writer_conn]} did not reach a subscriber on reactor "
            f"{dealt[sub_conn]}: {pushed!r}")
    finally:
        sub.close()
        writer.close()


def test_a_drain_with_sessions_on_several_reactors_ends_at_one_deadline():
    d, node = started(["--io-threads", "4", "--drain-timeout-ms", "1500"], metrics=True)
    idle = []
    try:
        node.wait_for_sessions(0)
        idle = [connect(node.port) for _ in range(4)]
        reactors = {node.adoptions()[c] for c in sorted(node.adoptions())[-4:]}
        assert reactors == {0, 1, 2, 3}, reactors
        started_at = time.monotonic()
        node.proc.send_signal(signal.SIGTERM)
        node.proc.wait(timeout=patience(15))
        took = time.monotonic() - started_at
        assert node.proc.returncode == 0, node.log_text()[-2000:]
        # One deadline for every reactor: 1.5 s plus one wait of each loop, not four in a row.
        assert took < patience(4.0), f"the drain took {took:.2f} s against a 1.5 s deadline"
        assert "Drain deadline of 1500 ms reached with 4 session(s)" in node.log_text(), (
            node.log_text()[-2000:])
    finally:
        for s in idle:
            s.close()
        if node.proc and node.proc.poll() is None:
            node.stop()
        d.cleanup()
