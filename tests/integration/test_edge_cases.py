"""Edge cases: what the server does with input it should refuse.

The rule these tests encode is that a bad command produces an error and leaves the
session usable. Two failure modes are worse than an error: accepting nonsense
silently, and dropping a connection that could have carried the next valid
command.
"""
from __future__ import annotations

import socket
import time

import pytest

from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.edge_cases


class Conn:
    """A session held open across several commands, so state can be observed."""

    def __init__(self, port: int, timeout: float = 6.0):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.banner = self.sock.recv(4096).decode(errors="replace")

    def send(self, command: str, settle: float = 0.3) -> str:
        self.sock.sendall(command.encode())
        time.sleep(settle)
        try:
            return self.sock.recv(1 << 20).decode(errors="replace")
        except socket.timeout:
            return ""

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def test_query_for_unknown_symbol_is_not_an_error(cluster):
    """An empty result is the right answer; an error would be wrong."""
    conn = Conn(cluster.primary().tcp_port)
    try:
        reply = conn.send("SELECT * FROM 'NO-SUCH-SYM'.'NOWHERE' "
                          "WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")
    finally:
        conn.close()

    # Either an OK with no data rows, or a clearly-worded not-found error. What
    # matters is that it is deliberate and not a crash or a hang.
    assert reply.strip(), "server said nothing at all"
    data_lines = [ln for ln in reply.strip().splitlines()
                  if ln and ln.split("\t")[0].isdigit()]
    assert not data_lines, f"unknown symbol returned rows: {reply!r}"


def test_malformed_insert_is_rejected(cluster):
    conn = Conn(cluster.primary().tcp_port)
    try:
        # Missing quantity.
        reply = conn.send("INSERT EDGE-BAD BINANCE bid 100000\n")
    finally:
        conn.close()
    assert "ERR" in reply.upper(), f"malformed INSERT was accepted: {reply!r}"


def test_unknown_command_is_rejected(cluster):
    conn = Conn(cluster.primary().tcp_port)
    try:
        reply = conn.send("FROBNICATE everything\n")
    finally:
        conn.close()
    assert "ERR" in reply.upper(), f"unknown command was accepted: {reply!r}"


def test_invalid_side_is_rejected(cluster):
    conn = Conn(cluster.primary().tcp_port)
    try:
        reply = conn.send("INSERT EDGE-SIDE BINANCE sideways 100000 10 1\n")
    finally:
        conn.close()
    assert "ERR" in reply.upper(), (
        f"a side that is neither bid nor ask was accepted: {reply!r}")


def test_non_numeric_price_is_rejected(cluster):
    conn = Conn(cluster.primary().tcp_port)
    try:
        reply = conn.send("INSERT EDGE-PRICE BINANCE bid abc 10 1\n")
    finally:
        conn.close()
    assert "ERR" in reply.upper(), f"a non-numeric price was accepted: {reply!r}"


def test_session_survives_a_rejected_command(cluster):
    """One bad command must not cost the client its connection."""
    conn = Conn(cluster.primary().tcp_port)
    try:
        bad = conn.send("INSERT EDGE-SURV BINANCE bid\n")
        assert "ERR" in bad.upper(), f"expected rejection, got {bad!r}"

        good = conn.send("PING\n")
        assert "PONG" in good.upper(), (
            f"session was unusable after one bad command: {good!r}")
    finally:
        conn.close()


def test_oversized_line_is_refused(cluster):
    """max_line_length is 256KB; beyond it the server closes the session.

    Closing is the documented behaviour here, unlike for a merely malformed
    command, because the server cannot know where the oversized line ends.
    """
    conn = Conn(cluster.primary().tcp_port)
    try:
        huge = "INSERT EDGE-HUGE BINANCE bid 100000 10 " + ("9" * 300_000) + "\n"
        reply = conn.send(huge, settle=0.6)
        # Either an explicit error or a closed connection, but not silent success.
        assert "OK" not in reply.upper() or "ERR" in reply.upper(), (
            f"an oversized line was accepted: {reply[:200]!r}")
    finally:
        conn.close()


def test_empty_line_does_not_break_the_session(cluster):
    conn = Conn(cluster.primary().tcp_port)
    try:
        conn.send("\n")
        reply = conn.send("PING\n")
        assert "PONG" in reply.upper(), (
            f"an empty line left the session unusable: {reply!r}")
    finally:
        conn.close()


def test_zero_quantity_is_handled_deliberately(cluster):
    """A zero quantity means "remove this level" in L2 feeds, so it must not crash."""
    conn = Conn(cluster.primary().tcp_port)
    try:
        reply = conn.send("INSERT EDGE-ZERO BINANCE bid 100000 0 1\n")
        assert reply.strip(), "server said nothing to a zero-quantity insert"
        follow_up = conn.send("PING\n")
        assert "PONG" in follow_up.upper(), "zero quantity destabilised the session"
    finally:
        conn.close()


def test_write_to_replica_raises_through_the_client(cluster):
    """The Python client should surface the refusal, not swallow it."""
    replica = cluster.replica()
    engine = OrderbookEngine(host="127.0.0.1", port=replica.tcp_port)
    try:
        with pytest.raises(OrderbookError):
            engine.insert("EDGE-RO", "BINANCE", "bid", [100_000], [10])
    finally:
        engine.close()
