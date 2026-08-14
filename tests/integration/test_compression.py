"""LZ4 session compression: negotiation, and data that survives it unchanged.

Compression is the kind of feature that looks fine until the data comes back
subtly different, so these tests care less about the handshake succeeding and more
about what the rows look like afterwards.
"""
from __future__ import annotations

import socket
import time

import pytest

import orderbook_engine
from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.compression


def negotiate(port: int, timeout: float = 6.0) -> str:
    """Open a session and ask for LZ4, returning the raw reply."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.recv(4096)  # banner
        sock.sendall(b"COMPRESS LZ4\n")
        time.sleep(0.3)
        return sock.recv(4096).decode(errors="replace")


def test_lz4_negotiation_is_accepted(cluster):
    reply = negotiate(cluster.primary().tcp_port)
    assert "OK" in reply.upper(), f"server refused LZ4: {reply!r}"
    assert "ERR" not in reply.upper(), f"got an error: {reply!r}"


def test_unknown_codec_is_refused(cluster):
    """Accepting a codec the server cannot speak would desynchronise the stream."""
    with socket.create_connection(("127.0.0.1", cluster.primary().tcp_port),
                                  timeout=6) as sock:
        sock.settimeout(6)
        sock.recv(4096)
        sock.sendall(b"COMPRESS SNAPPY\n")
        time.sleep(0.3)
        reply = sock.recv(4096).decode(errors="replace")
    assert "ERR" in reply.upper(), (
        f"server accepted a codec it does not implement: {reply!r}")


def test_missing_lz4_fails_before_negotiating(cluster, monkeypatch):
    """Without the lz4 extra the client must refuse, not half-negotiate.

    COMPRESS LZ4 switches the server's framing for the whole session. A client
    that sends it and then cannot compress leaves the connection desynchronised
    for good, so the check has to happen before the command goes out.
    """
    monkeypatch.setattr(orderbook_engine, "_lz4_frame", None)

    sent: list[str] = []
    original_send = orderbook_engine._TcpBackend._send

    def spy(self, line: str):
        sent.append(line)
        return original_send(self, line)

    monkeypatch.setattr(orderbook_engine._TcpBackend, "_send", spy)

    with pytest.raises(OrderbookError) as excinfo:
        OrderbookEngine(host="127.0.0.1", port=cluster.primary().tcp_port,
                        compress=True)

    assert not any("COMPRESS" in line for line in sent), (
        f"client switched the server's framing it cannot speak: {sent}")

    message = str(excinfo.value)
    assert "lz4" in message.lower(), f"error should name the missing package: {message}"
    assert "compression" in message, (
        f"error should name the extra that fixes it: {message}")


def test_compressed_client_really_is_compressed(compressed_client: OrderbookEngine):
    """Guard against the whole category passing over a plain session.

    If the fixture ever fell back to uncompressed framing, every test below would
    still pass and prove nothing about compression. Assert the flag once here.
    """
    backend = compressed_client._tcp
    assert backend is not None, "compressed_client is not in TCP mode"
    assert backend._compressed is True, (
        "session negotiated no compression, so this category tests nothing")


def test_round_trip_over_compressed_session(compressed_client: OrderbookEngine):
    prices = [500_000, 499_900, 499_800]
    qtys = [10, 20, 30]

    compressed_client.insert("LZ4-RT", "BINANCE", "bid", prices, qtys)
    compressed_client.flush()

    rows = compressed_client.query_all("LZ4-RT", "BINANCE")
    assert sorted(r.price for r in rows) == sorted(prices)
    assert sorted(r.quantity for r in rows) == sorted(qtys)


def test_compressed_session_preserves_side(compressed_client: OrderbookEngine):
    compressed_client.insert("LZ4-SIDES", "BINANCE", "bid", [600_000], [10])
    compressed_client.insert("LZ4-SIDES", "BINANCE", "ask", [601_000], [20])
    compressed_client.flush()

    rows = compressed_client.query_all("LZ4-SIDES", "BINANCE")
    sides = {r.side for r in rows}
    assert len(rows) == 2
    assert len(sides) == 2, f"compressed round trip flattened the sides: {sides}"


def test_bulk_insert_over_compressed_session(compressed_client: OrderbookEngine):
    """Larger payloads are where a framing bug in the codec shows up."""
    count = 400
    prices = [700_000 + i for i in range(count)]
    qtys = [5 + (i % 25) for i in range(count)]

    compressed_client.insert("LZ4-BULK", "BINANCE", "ask", prices, qtys)
    compressed_client.flush()

    rows = compressed_client.query_all("LZ4-BULK", "BINANCE")
    assert len(rows) == count, f"expected {count} rows, got {len(rows)}"
    assert sorted(r.price for r in rows) == sorted(prices)


def test_compressed_and_plain_sessions_agree(cluster,
                                             compressed_client: OrderbookEngine):
    """A row written compressed must read back identically uncompressed."""
    compressed_client.insert("LZ4-AGREE", "BINANCE", "bid", [800_000], [42])
    compressed_client.flush()

    plain = OrderbookEngine(host="127.0.0.1", port=cluster.primary().tcp_port)
    try:
        rows = plain.query_all("LZ4-AGREE", "BINANCE")
    finally:
        plain.close()

    assert len(rows) == 1, f"plain session sees {len(rows)} rows, expected 1"
    assert rows[0].price == 800_000
    assert rows[0].quantity == 42
