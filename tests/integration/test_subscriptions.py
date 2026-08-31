"""Pushed streaming subscriptions over the wire (#45).

Until this landed, the README listed "streaming subscriptions" among the features and the wire
protocol had no way to ask for one: `Engine::subscribe()` and `ob_subscribe()` delivered rows to an
**embedded** callback, and a network client polled. These tests are the part that proves the other
half exists, and there is one of them that no unit test can replace — a delta arriving from a peer
reaches a subscriber, which is the path that crosses a thread boundary.

Read over a bare socket rather than through the client library, deliberately. The library would hide
exactly what is under test: that a pushed row is distinguishable from a response to a command, and
that the two can interleave.
"""
from __future__ import annotations

import socket
import time

import pytest

pytestmark = pytest.mark.subscriptions

SYMBOL, EXCHANGE = "SUBSYM", "SUBEX"


class WireClient:
    """One connection, with the banner consumed and a buffer that survives partial reads."""

    def __init__(self, port: int, timeout: float = 8.0) -> None:
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""
        self.sock.recv(4096)  # banner

    def send(self, line: str) -> None:
        self.sock.sendall((line + "\n").encode())

    def read_more(self, timeout: float = 1.0) -> bytes:
        """Whatever has arrived within `timeout`. Empty on nothing, never an exception."""
        self.sock.settimeout(timeout)
        try:
            chunk = self.sock.recv(1 << 20)
        except socket.timeout:
            return b""
        self.buf += chunk
        return chunk

    def drain(self, seconds: float = 1.5) -> str:
        deadline = time.time() + seconds
        while time.time() < deadline:
            if not self.read_more(0.3):
                continue
        return self.buf.decode(errors="replace")

    def pushes(self) -> list[str]:
        return [line for line in self.buf.decode(errors="replace").splitlines()
                if line.startswith("PUSH ")]

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def minsert(client: WireClient, side: str, levels: list[tuple[int, int, int]],
            symbol: str = SYMBOL, exchange: str = EXCHANGE) -> None:
    """One MINSERT: a header line, then `price qty count` per level.

    The form matters and was got wrong once already — a made-up single-line variant produced 182
    `ERR` replies and a summary that read like success.
    """
    header = f"MINSERT {symbol} {exchange} {side} {len(levels)}"
    body = "\n".join(f"{p} {q} {c}" for p, q, c in levels)
    client.send(header + "\n" + body)


@pytest.mark.subscriptions
def test_a_subscriber_receives_rows_without_asking_for_them(cluster) -> None:
    """The whole point of the feature: no polling."""
    port = cluster.primary().tcp_port
    subscriber = WireClient(port)
    writer = WireClient(port)
    try:
        subscriber.send(f"SUBSCRIBE * FROM '{SYMBOL}'.'{EXCHANGE}'")
        ack = subscriber.drain(1.0)
        assert "OK SUB" in ack, f"subscribe was not acknowledged: {ack!r}"
        sub_id = ack.split("OK SUB")[1].split()[0]

        subscriber.buf = b""
        minsert(writer, "bid", [(100_000, 5, 1), (99_900, 7, 1), (99_800, 9, 1)])
        writer.drain(0.6)

        pushed = subscriber.drain(2.0)
        rows = subscriber.pushes()
        assert len(rows) == 3, f"expected three pushed rows, got {len(rows)}: {pushed!r}"
        for row in rows:
            fields = row.split("\t")
            assert fields[0] == f"PUSH {sub_id}", row
            assert len(fields) == 8, f"seven columns after the prefix, as in SELECT: {row!r}"
    finally:
        subscriber.close()
        writer.close()


@pytest.mark.subscriptions
def test_a_session_that_did_not_subscribe_receives_nothing(cluster) -> None:
    """Every existing client keeps working, and that is checked rather than assumed."""
    port = cluster.primary().tcp_port
    subscriber = WireClient(port)
    bystander = WireClient(port)
    writer = WireClient(port)
    try:
        subscriber.send(f"SUBSCRIBE * FROM '{SYMBOL}'.'{EXCHANGE}'")
        subscriber.drain(1.0)
        bystander.buf = b""

        for seq in range(4):
            minsert(writer, "ask", [(101_000 + seq, 3, 1)] * 5)
            writer.drain(0.3)

        subscriber.drain(1.5)
        assert subscriber.pushes(), "the subscriber got nothing, so this proves nothing about the bystander"

        assert bystander.read_more(1.0) == b"", (
            "a session with no subscription received bytes it did not ask for"
        )
    finally:
        subscriber.close()
        bystander.close()
        writer.close()


@pytest.mark.subscriptions
def test_a_push_can_interleave_with_a_reply_and_both_parse(cluster) -> None:
    """The protocol allows it, so a client has to be able to tell them apart."""
    port = cluster.primary().tcp_port
    client = WireClient(port)
    writer = WireClient(port)
    try:
        client.send(f"SUBSCRIBE * FROM '{SYMBOL}'.'{EXCHANGE}'")
        client.drain(1.0)
        client.buf = b""

        # Traffic and a query at the same time, from the same connection that is subscribed.
        minsert(writer, "bid", [(100_100 + i, 4, 1) for i in range(8)])
        client.send(f"SELECT * FROM '{SYMBOL}'.'{EXCHANGE}' LIMIT 2")
        text = client.drain(2.5)

        assert client.pushes(), f"no pushed rows arrived: {text!r}"
        assert "OK" in text, f"the reply to SELECT did not arrive: {text!r}"
        # Every line is either a push or part of the reply; nothing is a hybrid.
        for line in text.splitlines():
            assert not (line.startswith("PUSH ") and "\t" not in line), f"truncated push: {line!r}"
    finally:
        client.close()
        writer.close()


@pytest.mark.subscriptions
def test_unsubscribe_stops_the_stream(cluster) -> None:
    port = cluster.primary().tcp_port
    client = WireClient(port)
    writer = WireClient(port)
    try:
        client.send(f"SUBSCRIBE * FROM '{SYMBOL}'.'{EXCHANGE}'")
        ack = client.drain(1.0)
        sub_id = ack.split("OK SUB")[1].split()[0]

        client.buf = b""
        minsert(writer, "bid", [(100_200, 1, 1)])
        client.drain(1.5)
        assert client.pushes(), "nothing arrived before the unsubscribe, so this proves nothing"

        client.send(f"UNSUBSCRIBE {sub_id}")
        client.drain(1.0)
        client.buf = b""

        for seq in range(3):
            minsert(writer, "bid", [(100_300 + seq, 1, 1)])
            writer.drain(0.3)
        client.drain(1.5)

        # One row may still arrive: a notification already in flight when the cancellation landed.
        # Documented on QueryEngine::unsubscribe(), so the assertion is "the stream stopped", not
        # "not a single byte".
        assert len(client.pushes()) <= 1, (
            f"the stream did not stop after UNSUBSCRIBE: {client.pushes()}"
        )
    finally:
        client.close()
        writer.close()


@pytest.mark.subscriptions
def test_a_subscriber_that_disconnects_mid_stream_does_not_disturb_the_writer(cluster) -> None:
    """The failure that would matter operationally: one consumer leaving takes the node with it."""
    port = cluster.primary().tcp_port
    subscriber = WireClient(port)
    writer = WireClient(port)
    try:
        subscriber.send(f"SUBSCRIBE * FROM '{SYMBOL}'.'{EXCHANGE}'")
        subscriber.drain(1.0)

        minsert(writer, "ask", [(102_000, 2, 1)] * 4)
        writer.drain(0.4)
        subscriber.close()          # gone mid-stream, without UNSUBSCRIBE

        for seq in range(6):
            minsert(writer, "ask", [(102_100 + seq, 2, 1)] * 4)
            reply = writer.drain(0.4)
            assert "ERR" not in reply, f"the writer was disturbed by the subscriber leaving: {reply!r}"

        writer.buf = b""
        writer.send("PING")
        assert "PONG" in writer.drain(1.0), "the node stopped answering"
    finally:
        subscriber.close()
        writer.close()


# ── The one test the whole item exists for ────────────────────────────────────────────────────────
#
# `notify_subscribers()` is called from three places, and only one of them crosses a thread
# boundary: `Engine::apply_remote_delta`, which runs on `MultiMasterManager::io_loop` rather than on
# the server's epoll loop. That is why the subscription list needed synchronising and why a
# notification may not touch a Session — it has to enqueue and wake the loop instead.
#
# Every other test here exercises the client path, where the notification and the socket happen to
# be on the same thread and would work even with the wrong design. This one does not.

@pytest.mark.subscriptions
def test_a_delta_from_a_peer_reaches_a_subscriber_on_another_node(healthy_mm_cluster) -> None:
    writer_node = healthy_mm_cluster.nodes[0]
    reader_node = healthy_mm_cluster.nodes[1]

    subscriber = WireClient(reader_node.tcp_port)
    writer = WireClient(writer_node.tcp_port)
    try:
        subscriber.send(f"SUBSCRIBE * FROM '{SYMBOL}'.'{EXCHANGE}'")
        ack = subscriber.drain(1.5)
        assert "OK SUB" in ack, f"subscribe on the reader node was not acknowledged: {ack!r}"
        subscriber.buf = b""

        # Written on node 0. It reaches node 1 through the multi-master mesh, is applied by
        # apply_remote_delta() on io_loop, and has to arrive at a subscriber that node 1's epoll
        # loop owns.
        minsert(writer, "bid", [(105_000, 11, 1), (104_900, 12, 1)])
        writer.drain(0.6)

        text = subscriber.drain(6.0)
        rows = subscriber.pushes()
        assert rows, (
            "a delta written on another node did not reach the subscriber. This is the path that "
            f"crosses a thread boundary, so the failure is in the hand-off, not in the wire: {text!r}"
        )
        for row in rows:
            fields = row.split("\t")
            assert len(fields) == 8, f"seven columns after the prefix: {row!r}"
    finally:
        subscriber.close()
        writer.close()


# ── The Python client ─────────────────────────────────────────────────────────────────────────────
#
# A feature that exists on the wire and not in the client is a feature nobody uses. And this one
# needed a fix in the client rather than only an addition: `_recv_plain_response()` matches on the
# *front* of the buffer, so a pushed row arriving before the reply to a command matched none of its
# branches and the loop read until it timed out. Allowing an unsolicited server-to-client message
# breaks every client written that way, ours included.

@pytest.mark.subscriptions
def test_the_python_client_receives_pushed_rows(cluster) -> None:
    from orderbook_engine import OrderbookEngine

    port = cluster.primary().tcp_port
    reader = OrderbookEngine(host="127.0.0.1", port=port, timeout=8.0)
    writer = OrderbookEngine(host="127.0.0.1", port=port, timeout=8.0)
    try:
        sub_id = reader.subscribe("PYSYM", "PYEX")
        assert sub_id > 0

        writer.insert("PYSYM", "PYEX", "bid", [100_000, 99_900], [5, 6])

        rows = []
        deadline = time.time() + 6.0
        while len(rows) < 2 and time.time() < deadline:
            rows.extend(reader.poll(1.0))
        assert len(rows) >= 2, f"the client received {len(rows)} pushed rows"
        for got_id, row in rows:
            assert got_id == sub_id
            assert row.price in (100_000, 99_900), row

        assert reader.unsubscribe(sub_id) == 1
    finally:
        reader.close()
        writer.close()


@pytest.mark.subscriptions
def test_a_command_still_works_while_rows_are_being_pushed(cluster) -> None:
    """The interleaving case, through the client. Without the fix this times out rather than fails.

    A pushed row sitting in front of a reply matched none of `_recv_plain_response()`'s branches, so
    the loop kept reading and raised "TCP recv timeout" — with the reply already in the buffer.
    """
    from orderbook_engine import OrderbookEngine

    port = cluster.primary().tcp_port
    client = OrderbookEngine(host="127.0.0.1", port=port, timeout=8.0)
    writer = OrderbookEngine(host="127.0.0.1", port=port, timeout=8.0)
    try:
        client.subscribe("MIXSYM", "MIXEX")
        for level in range(6):
            writer.insert("MIXSYM", "MIXEX", "bid", [100_000 - level], [3])
        time.sleep(0.5)   # let the pushes arrive and sit in front of the next reply

        # A command on the subscribed connection, with pushes already queued in its buffer.
        rows = client.query_all("MIXSYM", "MIXEX", limit=3)
        assert isinstance(rows, list), rows

        pushed = client.poll(1.5)
        assert pushed, "the pushed rows were consumed but not kept; poll() returned nothing"
    finally:
        client.close()
        writer.close()
