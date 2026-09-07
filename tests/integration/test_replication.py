"""Replication: what the primary accepts, the replica has to end up holding.

The interesting part is not that data arrives but that it arrives *intact*. A
replica that receives rows with the wrong side, the wrong level or a truncated
tail is worse than one that receives nothing, because nothing is obviously broken.
"""
from __future__ import annotations

import os
import socket
import time

import pytest
from conftest import patience

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


def test_the_replica_persists_the_position_of_what_it_received(cluster,
                                                               primary_client: OrderbookEngine):
    """The saved position has to be the stream's position, not one record's worth.

    This is the consequence #98 was about. The `WAL <file> <offset>` line is what a replica records:
    it computes `confirmed_offset = byte_offset + total_len`, persists it and resumes from it. The
    live path used to send a literal zero for the offset, so a replica that had received forty
    records persisted the length of **one** - measured, 136 bytes against a WAL of 5472 - and asked
    for the whole current file again on reconnect.

    Checked against the primary's WAL rather than against a constant: the property is that the
    replica knows how far along the stream it is, and the only thing that knows the answer is the
    file the primary is writing.

    What this deliberately does not assert is a restart, and the reason has changed. It used to be
    that a failover-managed replica - which is what this fixture builds - cleared its local data and
    re-synced from zero whenever it was told its primary, including on its own restart, so a restart
    here measured the wipe rather than the resume. Since #101 it does not: `demote_to_replica()`
    discards nothing, and the replication client discards only when the primary it reached serves a
    different stream from the one the saved position belongs to. The restart is worth asserting and
    is asserted on its own, because it is a different property from this one - this test is about
    the position tracking the stream, that one about the position being believed.
    """
    symbol, exchange = "REPL-POS", "BINANCE"
    rows = 40
    for i in range(rows):
        primary_client.insert(symbol, exchange, "bid", [400_000 + i], [i + 1])
    primary_client.flush()

    replica = cluster.replica()
    assert wait_for_rows(replica.tcp_port, symbol, exchange, rows) == rows, (
        "the replica never received the batch, so this test cannot say anything about its position")

    status = raw_command(replica.tcp_port, "STATUS\n")
    line = next((ln for ln in status.splitlines() if ln.startswith("replication:")), "")
    assert line, f"STATUS on the replica has no replication line: {status!r}"
    reported = int(line.split("offset=")[1].split()[0])
    reported_file = int(line.split("file=")[1].split()[0])

    # The primary's WAL is the only thing that knows how long the stream is.
    wal_path = os.path.join(cluster.primary().data_dir, f"wal_{reported_file:06d}.bin")
    assert os.path.exists(wal_path), f"the primary has no {wal_path}"
    wal_size = os.path.getsize(wal_path)

    # The remaining difference is whatever the primary appended after the last delta - a checkpoint
    # from its own flush loop, at most a few records. A few kilobytes of slack, against a stream
    # that the defect left 5428 bytes behind on a 5564-byte WAL.
    slack = 4096
    assert wal_size - reported <= slack, (
        f"the replica reports offset {reported} in file {reported_file} while the primary's WAL is "
        f"{wal_size} bytes - it is {wal_size - reported} bytes behind its own stream, which is what "
        f"a position that does not track what arrived looks like ({line!r})")

    # And what a restart would read has to move as records arrive. Asserted as *advancement* rather
    # than as agreement with the line above, because `save_state()` runs on a ten-second timer: a
    # state file lagging the live position by a window of writes is correct, and a state file pinned
    # to one record's worth for ever is the defect. Comparing the two numbers directly passed here
    # and failed in CI, where the session cluster had already written a file.
    state_path = os.path.join(replica.data_dir, "repl_state.txt")
    deadline = time.monotonic() + patience(30)
    while time.monotonic() < deadline and not os.path.exists(state_path):
        time.sleep(0.5)
    assert os.path.exists(state_path), (
        f"the replica never wrote {state_path}; the position it holds in memory is the only copy "
        f"and a restart would replay the whole WAL")

    def saved_offset() -> int:
        parts = dict(kv.split("=", 1) for kv in open(state_path).read().split())
        return int(parts["byte_offset"])

    first_saved = saved_offset()
    for i in range(rows):
        primary_client.insert(symbol, exchange, "bid", [500_000 + i], [i + 1])
    primary_client.flush()
    assert wait_for_rows(replica.tcp_port, symbol, exchange, rows * 2) == rows * 2

    deadline = time.monotonic() + patience(30)
    while time.monotonic() < deadline and saved_offset() <= first_saved:
        time.sleep(0.5)
    assert saved_offset() > first_saved, (
        f"the persisted position stayed at {first_saved} while another {rows} records arrived - "
        f"what a restart reads is not tracking what the replica received")
