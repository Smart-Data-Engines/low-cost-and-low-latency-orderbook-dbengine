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


def _rows_for(conn: Conn, symbol: str) -> int:
    """How many rows the store holds for a symbol, over the wire."""
    reply = conn.send(f"SELECT * FROM '{symbol}'.'BINANCE' "
                      f"WHERE timestamp BETWEEN 0 AND 9999999999999999999\n", settle=0.5)
    return len([ln for ln in reply.strip().splitlines()
                if ln and ln.split("\t")[0].isdigit()])


def test_a_token_the_grammar_has_no_place_for_is_refused_and_stores_nothing(cluster):
    """The defect this module's own docstring names, measured on the wire (#107).

    Before the fix, on this cluster: `OK`, and one row. The parser read the fields it knew and
    ignored the rest — pitfall 27 on the wire protocol, and the same class #36 closed for
    command-line flags, where `--prot 5599` was silently skipped.

    The row count is the assertion that matters. `OK` and *a row was stored* are different claims,
    and an operator whose typo was discarded has the second one.
    """
    conn = Conn(cluster.primary().tcp_port)
    try:
        refused = conn.send("INSERT EDGE-EXTRA BINANCE bid 100000 10 1 notanumber\n")
        assert "ERR" in refused.upper(), (
            f"a token nobody reads was accepted: {refused!r}")

        # The control, in the same session: the same line without the extra token is stored. Without
        # it this test would pass against a server that refuses every INSERT there is.
        accepted = conn.send("INSERT EDGE-EXTRA BINANCE bid 100000 10 1\n")
        assert "ERR" not in accepted.upper(), f"the canonical form was refused too: {accepted!r}"

        conn.send("FLUSH\n", settle=0.6)
        assert _rows_for(conn, "EDGE-EXTRA") == 1, (
            "the refused line left a row behind, or the accepted one did not - and either way the "
            "count is what an operator would have to discover by querying")
    finally:
        conn.close()


def test_the_refusal_names_the_token_rather_than_counting_arguments(cluster):
    """`too many arguments` sends an operator counting spaces (#107).

    The message also has to say what the command does accept, because the next thing the reader
    needs is the grammar — and it borrows the query parser's own words for the same situation
    (`unexpected token 'garbage'`), so the protocol says this one thing one way.
    """
    conn = Conn(cluster.primary().tcp_port)
    try:
        reply = conn.send("INSERT EDGE-NAME BINANCE bid 100000 10 1 surprise\n")
        assert "unexpected token 'surprise'" in reply, (
            f"the refusal does not name the token that caused it: {reply!r}")
        assert "INSERT takes:" in reply, (
            f"the refusal does not say what INSERT accepts: {reply!r}")

        # And a word that is not a command stays `unknown command`: the parser has nothing specific
        # to say about it, and inventing something would be worse than the truth.
        unknown = conn.send("FROBNICATE surprise\n")
        assert "unknown command" in unknown, f"expected the plain answer, got {unknown!r}"
    finally:
        conn.close()


def test_a_batch_header_carrying_an_invented_time_field_is_refused(cluster):
    """The shape #105 will add, sent to a server that does not know it yet.

    This is why #107 is its own item rather than a paragraph inside #105: an upgraded client sending
    a timestamp to an older server used to get `OK` with the value dropped, which is exactly the
    defect #105 is about, one layer out. Somebody has already written this line by assumption —
    `binance_live_bootstrap.py` once sent `MINSERT` with an invented time argument, and the only
    reason that failed loudly was a second mistake on the same line.
    """
    conn = Conn(cluster.primary().tcp_port)
    try:
        refused = conn.send("MINSERT EDGE-TIME BINANCE bid 1 1700000000000000000\n"
                            "100000\t10\t1\n")
        assert "ERR" in refused.upper(), f"an invented header field was accepted: {refused!r}"
        assert "1700000000000000000" in refused, (
            f"the refusal does not name the field that is not understood: {refused!r}")

        # The control: the same batch without it, and a level line with its optional count.
        accepted = conn.send("MINSERT EDGE-TIME BINANCE bid 2\n100000\t10\t1\n99999\t11\t2\n")
        assert "ERR" not in accepted.upper(), f"the canonical batch was refused: {accepted!r}"

        conn.send("FLUSH\n", settle=0.6)
        assert _rows_for(conn, "EDGE-TIME") == 2, (
            "the refused batch stored levels, or the accepted one did not")
    finally:
        conn.close()
