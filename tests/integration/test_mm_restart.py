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

Both fixed together: replay seeds each record under the origin its header names, the vector is
written before the checkpoint rather than after it, and every flush tick refreshes the vector peers
are told whenever the tracker moved, sealing or not. The single-engine account of each part is
`tests/test_mm_restart_origins.cpp`.

And a third, found in the fix's own CI run (#180 part D): "a tick late" was still too late for the
one decision that reads the copy the tick refreshes. A node back within a tick of the writes it
missed was compared with a copy of its peers' vectors that did not have them yet, judged to hold
everything, and sent them at the next reconciliation - 100 of 2 100 rows 3 s after it reconnected,
there once, and with a tick of 1 s in every run. The decision now waits for the tick
(`tests/test_mm_catchup_rounds.cpp`).

A mesh of its own for each test, reconciling every 5 s: both kill and restart a node, and what
the first leaves behind - rows stored twice, a vector claiming them for the wrong origin - would
start the catch-up the second one is about.
"""
from __future__ import annotations

import re
import time
import urllib.request

import pytest

from conftest import ClusterManager, node_log_since, node_log_size
from orderbook_engine import BookUpdate, OrderbookEngine

pytestmark = pytest.mark.multi_master

EXCHANGE = "RST"
SYMBOLS = [f"R{i:02d}" for i in range(10)]
BASE_TS = 1_700_000_000_000_000_000
INTERVAL_S = 5
# For the node back within a tick: wide enough that the writes and the restart after one of the
# writer's ticks land before the next - together they take about half a second here - and a
# reconciliation interval far wider, so that the only thing to send the rows within the wait after
# the tick is the tick.
SLOW_TICK_MS = 5000
SLOW_TICK_INTERVAL_S = 30


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


def metric(node, name: str) -> float:
    with urllib.request.urlopen(f"http://127.0.0.1:{node.metrics_port}/metrics",
                                timeout=6.0) as resp:
        body = resp.read().decode(errors="replace")
    match = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)$", body, re.M)
    return float(match.group(1)) if match else 0.0


def wait_for(node, at_least: int, timeout: float) -> int:
    deadline = time.monotonic() + timeout
    got = rows(node)
    while got < at_least and time.monotonic() < deadline:
        time.sleep(0.5)
        got = rows(node)
    return got


def start_mesh(*extra: str, nodes: int = 3, interval_s: int = INTERVAL_S) -> ClusterManager:
    cm = ClusterManager()
    cm.extra_node_args = ["--anti-entropy-interval-seconds", str(interval_s), *extra]
    cm.start_multi_master(node_count=nodes)
    cm.wait_for_mm_mesh(timeout=45)
    return cm


@pytest.fixture
def mesh():
    cm = start_mesh()
    yield cm
    cm.shutdown()


@pytest.fixture
def slow_tick_pair():
    cm = start_mesh("--flush-interval-ms", str(SLOW_TICK_MS), nodes=2,
                    interval_s=SLOW_TICK_INTERVAL_S)
    yield cm
    cm.shutdown()


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


def held_after_missing_writes(mesh, returning: int, just_after_a_tick: bool = False,
                              wait_s: float = 3.0) -> tuple[int, int]:
    """What node `returning` holds `wait_s` after it reconnected, and what the writer does: it was
    killed with every node's rows written down, 200 rows a symbol were written to node 0
    meanwhile, and it was restarted."""
    writer = mesh.nodes[0]
    write(writer, BASE_TS, 10)
    assert wait_for(mesh.nodes[returning], 100, 30) == 100, (
        "the premise: the mesh holds the first rows")
    # What every node holds is written down, so the restart is not the one #179 is about.
    for node in mesh.nodes:
        client = client_for(node)
        try:
            client.flush()
        finally:
            client.close()
    before = rows(writer)

    if just_after_a_tick:
        # Straight after one of the writer's ticks, so that what follows lands before its next one.
        # The counter rises as a tick starts; the pause is for that tick to end.
        ticks = metric(writer, "ob_flush_ticks_total")
        deadline = time.monotonic() + 3 * SLOW_TICK_MS / 1000
        while metric(writer, "ob_flush_ticks_total") == ticks and time.monotonic() < deadline:
            time.sleep(0.01)
        assert metric(writer, "ob_flush_ticks_total") > ticks, "the premise: the writer ticks"
        time.sleep(0.1)
    started = time.monotonic()

    mesh.kill_node(returning)
    write(writer, BASE_TS + 1_000_000, 200)
    # Not counted on the writer until the end: a count flushes it, and a flush writes the
    # checkpoint that refreshes its vector - which is the refresh this test is about.
    expected = before + 200 * len(SYMBOLS)

    mesh.restart_node(returning)
    if just_after_a_tick:
        took = time.monotonic() - started
        assert took < SLOW_TICK_MS / 1000 - 1.0, (
            f"the premise: the writes and the restart fit in a tick, with a second to spare "
            f"({took:.2f} s)")
    mesh.wait_for_mm_mesh(timeout=90)
    # A catch-up at the reconnect sends what the peer lacks in well under a second here; the
    # three seconds are room, and far less than a seal interval and a reconciliation.
    got = wait_for(mesh.nodes[returning], expected, wait_s)
    assert rows(writer) == expected, "the premise: the writer holds every row it wrote"
    return got, expected


def test_a_node_restarted_after_missing_writes_gets_them_when_it_reconnects(mesh):
    got, expected = held_after_missing_writes(mesh, returning=2)
    if got < expected:
        raise Late(f"3 s after it reconnected the node holds {got} of {expected} rows")


def test_a_node_back_within_a_tick_of_the_writes_it_missed_gets_them_when_it_reconnects(
        slow_tick_pair):
    # The same, with the writes and the restart inside one of the writer's ticks: the copy of its
    # vector the returning node is compared with has none of the writes. They come with the tick
    # that brings it up to date, at most a tick after the reconnect, and the wait is that and two
    # seconds - which end long before the returning node's first reconciliation, the only other
    # thing that would send them. Two nodes: a third would hold the writes too, in a copy its own
    # tick refreshes on a clock this test does not follow.
    wait_s = SLOW_TICK_MS / 1000 + 2
    got, expected = held_after_missing_writes(slow_tick_pair, returning=1, just_after_a_tick=True,
                                              wait_s=wait_s)
    if got < expected:
        raise Late(f"{wait_s:.0f} s after it reconnected within a tick the node holds {got} of "
                   f"{expected} rows")
