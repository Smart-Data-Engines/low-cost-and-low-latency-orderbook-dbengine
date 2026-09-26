"""What a multi-master node holds after a restart, and how soon it gets what it missed (#179, #180).

Found measuring #178. Two defects, both older than it - the same numbers on the build before it:

- #179: a node that replays records a peer sent it gives them its **own** origin. The replay seeds
  the tracker with `mm_config_.node_id` for every record, whatever the record's header says, so a
  node restarted before its version vector reached the WAL does not know it holds those records from
  their origin, says so to its peers, and is sent them again - and storage is append-only. Measured:
  after the restart, every row the node had replayed was stored twice.
- #180: a node's version vector is refreshed when a checkpoint is written, and since part 2a of
  #165 that can be ten seconds after a write. So a node that comes back within that window is
  compared against a vector without the writes it missed, judged to hold everything, and caught up
  only at the next reconciliation. Measured: the rows it missed arrived 20 s after its restart.

A mesh of its own for each test, reconciling every 5 s: both kill and restart a node, and what
the first leaves behind - rows stored twice, a vector claiming them for the wrong origin - would
start the catch-up the second one is about.
"""
from __future__ import annotations

import time

import pytest

from conftest import ClusterManager, node_log_since, node_log_size
from orderbook_engine import BookUpdate, OrderbookEngine

pytestmark = pytest.mark.multi_master

EXCHANGE = "RST"
SYMBOLS = [f"R{i:02d}" for i in range(10)]
BASE_TS = 1_700_000_000_000_000_000
INTERVAL_S = 5


class Duplicated(AssertionError):
    """The restarted node holds a row more than once."""


class Late(AssertionError):
    """The restarted node did not get what it missed when it reconnected."""


def client_for(node, timeout: float = 30.0) -> OrderbookEngine:
    return OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=timeout)


def write(node, first_ts: int, per_symbol: int) -> None:
    client = client_for(node)
    try:
        updates = [BookUpdate(sym, EXCHANGE, "bid", [100_000 + k], [5],
                              timestamp_ns=first_ts + s * per_symbol + k)
                   for s, sym in enumerate(SYMBOLS) for k in range(per_symbol)]
        for start in range(0, len(updates), 512):
            outcomes = client.insert_batch(updates[start:start + 512])
            assert all(o.ok for o in outcomes), [o for o in outcomes if not o.ok][:3]
    finally:
        client.close()


def rows(node) -> int:
    client = client_for(node)
    try:
        client.flush()
        return sum(len(client.query_all(sym, EXCHANGE)) for sym in SYMBOLS)
    finally:
        client.close()


def wait_for(node, at_least: int, timeout: float) -> int:
    deadline = time.monotonic() + timeout
    got = rows(node)
    while got < at_least and time.monotonic() < deadline:
        time.sleep(0.5)
        got = rows(node)
    return got


@pytest.fixture
def mesh():
    cm = ClusterManager()
    cm.extra_node_args = ["--anti-entropy-interval-seconds", str(INTERVAL_S)]
    cm.start_multi_master(node_count=3)
    cm.wait_for_mm_mesh(timeout=45)
    yield cm
    cm.shutdown()


@pytest.mark.xfail(strict=True, raises=Duplicated,
                   reason="#179: a replayed record is seeded with this node's origin, so a node "
                          "restarted before its vector reached the WAL is sent its rows again")
def test_a_node_restarted_before_its_vector_was_written_holds_each_row_once(mesh):
    writer, restarted = mesh.nodes[0], mesh.nodes[2]
    write(writer, BASE_TS, 10)
    # Not counted on the node itself: a count flushes, and a flush writes the vector down. The mesh
    # delivers in milliseconds here; the restart's own log says below what it held.
    time.sleep(1.5)

    # Killed at once: a vector reaches the WAL with a checkpoint, and nothing has sealed yet.
    mesh.kill_node(2)
    offset = node_log_size(restarted)
    mesh.restart_node(2)
    mesh.wait_for_mm_mesh(timeout=90)
    said = node_log_since(mesh.nodes[2], offset)
    assert "No version vector in the WAL" in said, (
        "the premise: the node came back without its vector, which is the case this is about")
    assert "WAL replay: records=100 applied=100" in said, (
        "the premise: the node replayed the 100 rows it had received")

    # Reconciliation every 5 s: three of them, and the writer's own checkpoint, are room.
    deadline = time.monotonic() + 4 * INTERVAL_S + 12
    got = rows(mesh.nodes[2])
    while time.monotonic() < deadline:
        time.sleep(1.0)
        got = rows(mesh.nodes[2])
    expected = rows(writer)
    if got != expected:
        raise Duplicated(f"the restarted node holds {got} rows where the writer holds {expected}")


@pytest.mark.xfail(strict=True, raises=Late,
                   reason="#180: a node's vector is refreshed at a checkpoint, so a peer that comes "
                          "back within the seal interval is judged to hold what it missed")
def test_a_node_restarted_after_missing_writes_gets_them_when_it_reconnects(mesh):
    writer = mesh.nodes[0]
    write(writer, BASE_TS, 10)
    assert wait_for(mesh.nodes[2], 100, 30) == 100, "the premise: the mesh holds the first rows"
    # What every node holds is written down, so the restart is not the one #179 is about.
    for node in mesh.nodes:
        client = client_for(node)
        try:
            client.flush()
        finally:
            client.close()
    before = rows(writer)

    mesh.kill_node(2)
    write(writer, BASE_TS + 1_000_000, 200)
    # Not counted on the writer until the end: a count flushes it, and a flush writes the
    # checkpoint that refreshes its vector - which is the refresh this test is about.
    expected = before + 200 * len(SYMBOLS)

    mesh.restart_node(2)
    mesh.wait_for_mm_mesh(timeout=90)
    # A catch-up at the reconnect sends what the peer lacks in well under a second here; the
    # three seconds are room, and far less than a seal interval and a reconciliation.
    got = wait_for(mesh.nodes[2], expected, 3.0)
    assert rows(writer) == expected, "the premise: the writer holds every row it wrote"
    if got < expected:
        raise Late(f"3 s after it reconnected the node holds {got} of {expected} rows")
