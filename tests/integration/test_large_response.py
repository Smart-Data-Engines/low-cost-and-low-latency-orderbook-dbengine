"""Responses larger than the socket send buffer.

A non-blocking socket returns EAGAIN when its send buffer is full. That means "come
back later", not "the client is gone" — but the server used to read it as the latter
and close the session in the middle of the response. Measured before the fix:
50 000 rows came back fine, 100 000 killed the connection.

The slow-reader test is the important one. The plain large-response test depends on
the server outpacing the client, which is a race; reading 4 KB at a time with pauses
fills the send buffer every run.
"""
from __future__ import annotations

import socket
import struct
import time

import pytest

from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.large_response

ROWS = 100_000
BATCH = 500


@pytest.fixture(scope="module")
def big_symbol(heavy_cluster) -> str:
    """One symbol with ROWS rows, flushed. Built once for the whole module."""
    symbol = "BIGRESP"
    client = OrderbookEngine(host="127.0.0.1", port=heavy_cluster.primary().tcp_port,
                             timeout=60)
    try:
        sent = 0
        while sent < ROWS:
            count = min(BATCH, ROWS - sent)
            client.insert(symbol, "BINANCE", "bid",
                          [1_000_000 + sent + i for i in range(count)],
                          [10] * count)
            sent += count
        client.flush()
    finally:
        client.close()
    return symbol


def test_full_result_set_comes_back(heavy_cluster, big_symbol):
    """100 000 rows in one response. The row count is the whole assertion."""
    client = OrderbookEngine(host="127.0.0.1", port=heavy_cluster.primary().tcp_port,
                             timeout=60)
    try:
        rows = client.query_all(big_symbol, "BINANCE")
    finally:
        client.close()

    assert len(rows) == ROWS, (
        f"expected {ROWS} rows, got {len(rows)} — a response larger than the socket "
        f"send buffer must not truncate")


def test_slow_reader_is_not_disconnected(heavy_cluster, big_symbol):
    """Read 4 KB at a time with pauses, so the send buffer is certainly full.

    This is the deterministic version of the defect: the server cannot possibly
    write 4 MB into a socket nobody is draining, so it hits EAGAIN every run.
    """
    port = heavy_cluster.primary().tcp_port
    sql = (f"SELECT * FROM '{big_symbol}'.'BINANCE' "
           f"WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")

    with socket.create_connection(("127.0.0.1", port), timeout=90) as sock:
        sock.settimeout(90)
        sock.recv(4096)  # banner
        sock.sendall(sql.encode())

        chunks: list[bytes] = []
        total = 0
        closed_early = False
        # The response ends with a blank line: "...\n\n".
        while True:
            time.sleep(0.01)  # keep the reader slower than the writer
            data = sock.recv(4096)
            if not data:
                closed_early = True
                break
            chunks.append(data)
            total += len(data)
            if b"".join(chunks[-2:]).endswith(b"\n\n"):
                break

    body = b"".join(chunks).decode(errors="replace")
    data_lines = [ln for ln in body.splitlines()
                  if ln and ln.split("\t")[0].isdigit()]

    assert not closed_early, (
        f"server closed the connection after {total} bytes and "
        f"{len(data_lines)} rows; a slow reader is not a dead reader")
    assert len(data_lines) == ROWS, (
        f"expected {ROWS} rows, got {len(data_lines)} after {total} bytes")


def test_session_survives_a_large_response(heavy_cluster, big_symbol):
    """The connection must still be usable once a big response has drained."""
    client = OrderbookEngine(host="127.0.0.1", port=heavy_cluster.primary().tcp_port,
                             timeout=60)
    try:
        rows = client.query_all(big_symbol, "BINANCE")
        assert len(rows) == ROWS

        # Same session, another command.
        assert client.ping().strip() == "PONG"
        status = client.status()
        assert status.get("mode") == "tcp"
    finally:
        client.close()


def test_two_large_responses_on_one_session(heavy_cluster, big_symbol):
    """Back-to-back big reads: the second must not inherit the first one's tail."""
    client = OrderbookEngine(host="127.0.0.1", port=heavy_cluster.primary().tcp_port,
                             timeout=60)
    try:
        first = client.query_all(big_symbol, "BINANCE")
        second = client.query_all(big_symbol, "BINANCE")
    finally:
        client.close()

    assert len(first) == ROWS, f"first read got {len(first)}"
    assert len(second) == ROWS, (
        f"second read got {len(second)} — leftover bytes from the first response "
        f"would show up here")


def test_limit_still_bounds_the_response(heavy_cluster, big_symbol):
    """LIMIT is the documented way to ask for less; it must actually apply."""
    client = OrderbookEngine(host="127.0.0.1", port=heavy_cluster.primary().tcp_port,
                             timeout=60)
    try:
        rows = client.query_all(big_symbol, "BINANCE", limit=25)
    finally:
        client.close()

    assert len(rows) == 25, f"LIMIT 25 returned {len(rows)} rows"


def test_server_survives_a_client_vanishing_mid_response(heavy_cluster, big_symbol):
    """Abandon a large response half-read, then check the server is still there.

    Writing to a socket whose peer has gone raises SIGPIPE, and nothing in the
    process ignored it: one disconnecting client killed the whole server, taking
    every other session with it. Buffering the response over several event-loop
    iterations widens that window, so this test guards the fix rather than the
    feature.
    """
    port = heavy_cluster.primary().tcp_port
    sql = (f"SELECT * FROM '{big_symbol}'.'BINANCE' "
           f"WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")

    for attempt in range(3):
        sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        try:
            sock.settimeout(30)
            sock.recv(4096)  # banner
            sock.sendall(sql.encode())
            # Take a little, then walk away while the rest is still queued.
            sock.recv(4096)
            # SO_LINGER 0 makes close() send RST instead of FIN, which is the
            # harshest version of what a crashed client does.
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                            struct.pack("ii", 1, 0))
        finally:
            sock.close()
        time.sleep(0.2)

    # The server must still be serving. A fresh session proves the process lives.
    client = OrderbookEngine(host="127.0.0.1", port=port, timeout=30)
    try:
        assert client.ping().strip() == "PONG", "server stopped answering"
        rows = client.query_all(big_symbol, "BINANCE", limit=10)
        assert len(rows) == 10
    finally:
        client.close()

    # And the node process itself is still the one we started.
    node = heavy_cluster.primary()
    assert node.process is None or node.process.poll() is None, (
        "the server process died when a client vanished mid-response")


def test_many_abandoned_responses_do_not_leak_sessions(heavy_cluster, big_symbol):
    """Sessions abandoned mid-response must be reaped, not accumulate."""
    port = heavy_cluster.primary().tcp_port
    sql = (f"SELECT * FROM '{big_symbol}'.'BINANCE' "
           f"WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")

    for _ in range(10):
        sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        try:
            sock.settimeout(30)
            sock.recv(4096)
            sock.sendall(sql.encode())
            sock.recv(4096)
        finally:
            sock.close()

    # Give the server a moment to notice the closures.
    deadline = time.time() + 15
    sessions = None
    while time.time() < deadline:
        client = OrderbookEngine(host="127.0.0.1", port=port, timeout=30)
        try:
            sessions = client.status().get("sessions")
        finally:
            client.close()
        if sessions is not None and sessions <= 2:
            break
        time.sleep(0.5)

    assert sessions is not None, "STATUS did not report a session count"
    assert sessions <= 2, (
        f"{sessions} sessions still registered after ten clients walked away; "
        f"abandoned sessions are leaking")


def test_pending_bytes_gauge_reacts_to_a_slow_reader(heavy_cluster, big_symbol):
    """The gauge exists so an operator can see a slow client. It has to move.

    A metric that never leaves zero is worse than a missing one: a dashboard built
    on it looks calm while output piles up.
    """
    import re
    import urllib.request

    port = heavy_cluster.primary().tcp_port
    metrics_port = heavy_cluster.primary().metrics_port
    sql = (f"SELECT * FROM '{big_symbol}'.'BINANCE' "
           f"WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")

    def pending_bytes() -> float:
        with urllib.request.urlopen(f"http://127.0.0.1:{metrics_port}/metrics",
                                    timeout=6) as resp:
            body = resp.read().decode(errors="replace")
        match = re.search(r"^ob_session_pending_bytes(?:\{[^}]*\})?\s+([0-9.eE+-]+)$",
                          body, re.M)
        assert match, "ob_session_pending_bytes is not exposed"
        return float(match.group(1))

    sock = socket.create_connection(("127.0.0.1", port), timeout=60)
    peak = 0.0
    try:
        sock.settimeout(60)
        sock.recv(4096)  # banner
        sock.sendall(sql.encode())

        # Do not read. The server fills the socket buffer, then queues the rest.
        deadline = time.time() + 10
        while time.time() < deadline:
            peak = max(peak, pending_bytes())
            if peak > 0:
                break
            time.sleep(0.3)
    finally:
        sock.close()

    assert peak > 0, (
        "ob_session_pending_bytes stayed at zero while a client refused to read "
        "megabytes of response")

    # And it must come back down once the client is gone.
    deadline = time.time() + 15
    while time.time() < deadline:
        if pending_bytes() == 0:
            break
        time.sleep(0.5)
    assert pending_bytes() == 0, "queued bytes never went back to zero"
