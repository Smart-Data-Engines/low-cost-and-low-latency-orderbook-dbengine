"""Smoke tests: the basics have to work before anything else is worth checking.

PING, an INSERT/query round trip, STATUS fields and ROLE. If these fail, the
remaining categories will fail too and their output is noise.
"""
from __future__ import annotations

import socket

import pytest

from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.smoke


def raw_command(port: int, command: str, timeout: float = 5.0) -> str:
    """Send one command over a bare socket and return the raw reply.

    Used where the point is the wire protocol itself rather than the client:
    the Python client hides banners, framing and error strings, which is exactly
    what some of these tests need to see.
    """
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        banner = sock.recv(4096)  # "OK ob_tcp_server v0.1.0\n"
        assert banner, "server sent no banner"
        sock.sendall(command.encode())
        chunks = []
        while True:
            try:
                data = sock.recv(4096)
            except socket.timeout:
                break
            if not data:
                break
            chunks.append(data)
            joined = b"".join(chunks)
            # Responses end with a blank line, or are a single line.
            if joined.endswith(b"\n\n") or joined.count(b"\n") >= 1 and b"ERR" in joined:
                break
            if joined.endswith(b"\n") and not joined.startswith(b"OK\n"):
                break
        return b"".join(chunks).decode(errors="replace")


def test_banner_on_connect(cluster):
    """A fresh connection is greeted, so a client can identify the server."""
    node = cluster.primary()
    with socket.create_connection(("127.0.0.1", node.tcp_port), timeout=5) as sock:
        sock.settimeout(5)
        banner = sock.recv(4096).decode(errors="replace")
    assert banner.startswith("OK ob_tcp_server"), f"unexpected banner: {banner!r}"


def test_ping_returns_pong(primary_client: OrderbookEngine):
    assert primary_client.ping().strip() == "PONG"


def test_insert_flush_query_round_trip(primary_client: OrderbookEngine):
    """The core promise of the engine: what goes in comes back out."""
    prices = [6_500_000, 6_499_000, 6_498_000]
    qtys = [150, 200, 250]

    primary_client.insert("SMOKE-BTC", "BINANCE", "bid", prices, qtys)
    primary_client.flush()

    rows = primary_client.query_all("SMOKE-BTC", "BINANCE")
    got_prices = sorted(r.price for r in rows)

    assert len(rows) == len(prices), f"expected {len(prices)} rows, got {len(rows)}"
    assert got_prices == sorted(prices)


def test_query_returns_both_sides(primary_client: OrderbookEngine):
    primary_client.insert("SMOKE-SIDES", "BINANCE", "bid", [100_000], [10])
    primary_client.insert("SMOKE-SIDES", "BINANCE", "ask", [101_000], [20])
    primary_client.flush()

    rows = primary_client.query_all("SMOKE-SIDES", "BINANCE")
    sides = {r.side for r in rows}

    assert len(rows) == 2, f"expected one row per side, got {len(rows)}"
    assert len(sides) == 2, f"both sides should be stored, got sides={sides}"


def test_status_reports_counters(primary_client: OrderbookEngine):
    """STATUS has to answer with counters, not just not fail."""
    primary_client.insert("SMOKE-STATUS", "BINANCE", "bid", [123_000], [7])
    primary_client.flush()

    status = primary_client.status()

    assert status.get("mode") == "tcp"
    for field in ("sessions", "queries", "inserts"):
        assert field in status, f"STATUS is missing {field}: {status}"
        assert isinstance(status[field], int)
    assert status["inserts"] >= 1, "an insert was just performed"


def test_role_reports_primary_and_replica(cluster):
    """Exactly one node holds the primary role; the other follows it."""
    primary_reply = raw_command(cluster.primary().tcp_port, "ROLE\n")
    replica_reply = raw_command(cluster.replica().tcp_port, "ROLE\n")

    assert "PRIMARY" in primary_reply.upper(), f"got {primary_reply!r}"
    assert "REPLICA" in replica_reply.upper(), f"got {replica_reply!r}"


def test_exactly_one_primary(cluster):
    """Two primaries mean split brain, which is worse than none."""
    roles = [raw_command(n.tcp_port, "ROLE\n").upper() for n in cluster.nodes]
    primaries = [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]
    assert len(primaries) == 1, f"expected exactly one primary, roles={roles}"


def test_flush_is_idempotent(primary_client: OrderbookEngine):
    """Flushing twice must not duplicate rows or fail."""
    primary_client.insert("SMOKE-FLUSH", "BINANCE", "bid", [999_000], [1])
    primary_client.flush()
    before = len(primary_client.query_all("SMOKE-FLUSH", "BINANCE"))

    primary_client.flush()
    after = len(primary_client.query_all("SMOKE-FLUSH", "BINANCE"))

    assert before == after == 1, f"row count changed on second flush: {before} -> {after}"
