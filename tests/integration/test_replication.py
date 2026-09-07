"""Replication: what the primary accepts, the replica has to end up holding.

The interesting part is not that data arrives but that it arrives *intact*. A
replica that receives rows with the wrong side, the wrong level or a truncated
tail is worse than one that receives nothing, because nothing is obviously broken.
"""
from __future__ import annotations

import socket
import time

import pytest

from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.replication


def raw_command(port: int, command: str, timeout: float = 6.0,
                settle: float = 0.4) -> str:
    """Send one command over a bare socket and return the raw reply."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.recv(4096)  # banner
        sock.sendall(command.encode())
        time.sleep(settle)
        try:
            return sock.recv(1 << 20).decode(errors="replace")
        except socket.timeout:
            return ""


def wait_for_rows(port: int, symbol: str, exchange: str, expected: int,
                  timeout: float = 15.0) -> int:
    """Poll a node until it reports the expected row count, or time out.

    Returns the last count seen, so a failure message can state what was actually
    there rather than just that it timed out.
    """
    sql = (f"SELECT * FROM '{symbol}'.'{exchange}' "
           f"WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")
    deadline = time.monotonic() + timeout
    seen = -1
    while time.monotonic() < deadline:
        reply = raw_command(port, sql, settle=0.2)
        # Data lines: skip the "OK" line and the header line.
        lines = [ln for ln in reply.strip().splitlines() if ln.strip()]
        seen = max(0, len(lines) - 2)
        if seen >= expected:
            return seen
        time.sleep(0.3)
    return seen


def count_rows(port: int, symbol: str, exchange: str) -> int:
    """Count the rows a node holds for one symbol, right now.

    Separate from `wait_for_rows` on purpose: that one returns as soon as it has seen enough, so it
    cannot see too many. Duplication is exactly the failure this module needs to be able to see -
    the same lesson a surviving mutation taught the C++ side of #93.
    """
    sql = (f"SELECT * FROM \'{symbol}\'.\'{exchange}\' "
           f"WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")
    reply = raw_command(port, sql, settle=0.4)
    lines = [ln for ln in reply.strip().splitlines() if ln.strip()]
    return max(0, len(lines) - 2)

def test_replica_reports_replica_role(cluster):
    reply = raw_command(cluster.replica().tcp_port, "ROLE\n")
    assert "REPLICA" in reply.upper(), f"got {reply!r}"


def test_replica_rejects_writes(cluster):
    """A replica accepting a write would diverge from the primary silently."""
    reply = raw_command(cluster.replica().tcp_port,
                        "INSERT REPL-RO BINANCE bid 100000 10 1\n")
    assert "ERR" in reply.upper(), (
        f"the replica accepted a write, which would fork the data: {reply!r}")


def test_single_row_reaches_replica(cluster, primary_client: OrderbookEngine):
    primary_client.insert("REPL-ONE", "BINANCE", "bid", [100_000], [10])
    primary_client.flush()

    got = wait_for_rows(cluster.replica().tcp_port, "REPL-ONE", "BINANCE", 1)
    assert got >= 1, f"replica never received the row (saw {got})"


def test_replicated_rows_keep_their_side(cluster, primary_client: OrderbookEngine):
    """Regression guard for the columnar format bug.

    Format version 1 dropped the order side on flush, so a replica received rows
    that all looked like bids. Checking arrival without checking content would
    have passed throughout.
    """
    primary_client.insert("REPL-SIDES", "BINANCE", "bid", [100_000], [10])
    primary_client.insert("REPL-SIDES", "BINANCE", "ask", [101_000], [20])
    primary_client.flush()

    replica_port = cluster.replica().tcp_port
    assert wait_for_rows(replica_port, "REPL-SIDES", "BINANCE", 2) >= 2

    sql = ("SELECT * FROM 'REPL-SIDES'.'BINANCE' "
           "WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")
    reply = raw_command(replica_port, sql)

    sides = set()
    for line in reply.strip().splitlines():
        parts = line.split("\t")
        if len(parts) >= 6 and parts[0].isdigit():
            sides.add(parts[4])

    assert sides == {"0", "1"}, (
        f"replica should hold both sides, saw side values {sorted(sides)}")


def test_bulk_replication_preserves_every_row(cluster,
                                              primary_client: OrderbookEngine):
    """Volume matters: a truncated tail is the failure mode worth catching."""
    count = 500
    prices = [200_000 + i for i in range(count)]
    qtys = [10 + (i % 50) for i in range(count)]

    primary_client.insert("REPL-BULK", "BINANCE", "bid", prices, qtys)
    primary_client.flush()

    got = wait_for_rows(cluster.replica().tcp_port, "REPL-BULK", "BINANCE",
                        count, timeout=30.0)
    assert got == count, f"expected {count} rows on the replica, saw {got}"


def test_primary_status_lists_the_replica(cluster):
    """The primary has to know its replica exists, or lag monitoring is blind."""
    reply = raw_command(cluster.primary().tcp_port, "STATUS\n")
    assert "replicas:" in reply, f"STATUS has no replicas section: {reply!r}"


def test_replica_status_reports_its_position(cluster):
    reply = raw_command(cluster.replica().tcp_port, "STATUS\n")
    # The replica section is only emitted when the node is in replica mode.
    assert "replication" in reply.lower() or "confirmed" in reply.lower(), (
        f"STATUS on the replica says nothing about replication: {reply!r}")


def test_replica_catches_up_after_more_writes(cluster,
                                              primary_client: OrderbookEngine):
    """Replication is a stream, not a one-off: a second batch must arrive too."""
    primary_client.insert("REPL-CATCH", "BINANCE", "bid", [300_000], [10])
    primary_client.flush()
    assert wait_for_rows(cluster.replica().tcp_port, "REPL-CATCH", "BINANCE", 1) >= 1

    primary_client.insert("REPL-CATCH", "BINANCE", "bid",
                          [300_001, 300_002, 300_003], [11, 12, 13])
    primary_client.flush()

    got = wait_for_rows(cluster.replica().tcp_port, "REPL-CATCH", "BINANCE", 4)
    assert got == 4, f"replica stopped following after the first batch (saw {got})"


def test_a_restarted_replica_resumes_instead_of_replaying(cluster,
                                                          primary_client: OrderbookEngine):
    """A replica that restarts must ask for what it has not got, not for the file it was in.

    This is the consequence #98 was about. The `WAL <file> <offset>` line is what a replica saves,
    and the offset used to be a literal zero on the live path - so a restarted replica recorded one
    record's worth however many it had received, asked for the whole current WAL file again, and
    re-applied it. Storage is append-only and the replication path has no sequence check, so the
    rows land a second time and a `SELECT` returns each of them twice.

    The row count is read exactly rather than waited for: waiting for `n` cannot fail upwards.
    """
    symbol, exchange = "REPL-RESUME", "BINANCE"
    rows = 40
    for i in range(rows):
        primary_client.insert(symbol, exchange, "bid", [400_000 + i], [i + 1])
    primary_client.flush()

    replica = cluster.replica()
    assert wait_for_rows(replica.tcp_port, symbol, exchange, rows) == rows, (
        "the replica never received the first batch, so this test cannot say anything about a "
        "restart")

    # The position the replica is about to persist. A real one is far past a single record; the
    # defect made this the length of one.
    status = raw_command(replica.tcp_port, "STATUS\n")
    line = next((ln for ln in status.splitlines() if ln.startswith("replication:")), "")
    assert line, f"STATUS on the replica has no replication line: {status!r}"
    offset = int(line.split("offset=")[1].split()[0])
    assert offset > 4096, (
        f"the replica's confirmed offset is {offset}, which is about one record - it is not "
        f"tracking what it has received ({line!r})")

    before = count_rows(replica.tcp_port, symbol, exchange)
    assert before == rows, f"expected {rows} rows before the restart, found {before}"

    cluster.restart_node(replica.index)

    # Long enough for the reconnect, the handshake and whatever catch-up it asks for. If it asks
    # for the whole file, this is more than long enough to receive it: the batch above is small.
    time.sleep(4.0)

    after = count_rows(replica.tcp_port, symbol, exchange)
    assert after == rows, (
        f"the restarted replica holds {after} rows where {rows} were written: it replayed records "
        f"it already had, and append-only storage kept both copies")
