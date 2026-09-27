"""A symbol more than one mesh node writes, and a node that missed some of it (#184); and a restarted
node that claims numbers it did not write (#185).

A sequence number belongs to the origin that minted it, and a frontier is "every number up to here
from that origin". But the counter that mints them is one per symbol, raised by every origin's
numbers - so when two nodes take turns writing one symbol, neither origin's numbers for it are
contiguous: node 0 writes 1-500, node 1 then 501-1 000, node 0 1 001-1 500. Every node's frontier
for each origin stops at the first number the other took, so every node states the same frontiers,
and a node that missed writes during an outage is compared against a vector that says it lacks
nothing. Measured with the probe this module is built from: after an outage and a restart, 10 000 of
11 000 rows for good on the branch that fixed #179 - and 12 308 on the build before it, where the
replay's own-origin seeding made the node look empty and every number above the held set's cap
(4 096 per origin) was stored twice.

The single-writer case is the control: the same outage, one writer, and the node catches up.

Fixed together: in a mesh another origin's number no longer raises this node's counter, and a
segment records the highest number of this node's own rows, which a restart continues from and
declares its own frontier up to - not every origin's highest, which also made a node restarted with
segments of a symbol only its peers write claim their numbers as its own (#185): every
reconciliation then scanned its WAL for records nobody wrote.
"""
from __future__ import annotations

import re
import time
import urllib.request

import pytest

from conftest import ClusterManager, node_log_since, node_log_size
from orderbook_engine import BookUpdate, OrderbookEngine

pytestmark = pytest.mark.multi_master

EXCHANGE = "MWR"
SYMBOL = "MW01"
BASE_TS = 1_700_000_000_000_000_000
ROUNDS = 20
PER_ROUND = 500
INTERVAL_S = 5


class Diverged(AssertionError):
    """A node that missed writes does not hold what the writers hold once it is back."""


def metric(node, name: str) -> float:
    with urllib.request.urlopen(f"http://127.0.0.1:{node.metrics_port}/metrics", timeout=6) as resp:
        body = resp.read().decode(errors="replace")
    match = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)$", body, re.M)
    return float(match.group(1)) if match else 0.0


def client_for(node, timeout: float = 30.0) -> OrderbookEngine:
    return OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=timeout)


def write(node, first_ts: int, count: int) -> None:
    client = client_for(node)
    try:
        updates = [BookUpdate(SYMBOL, EXCHANGE, "bid", [100_000 + (first_ts + k) % 997], [5],
                              timestamp_ns=first_ts + k) for k in range(count)]
        for start in range(0, len(updates), 500):
            outcomes = client.insert_batch(updates[start:start + 500])
            assert all(o.ok for o in outcomes), [o for o in outcomes if not o.ok][:3]
    finally:
        client.close()


def rows(node) -> int:
    client = client_for(node)
    try:
        client.flush()
        return len(client.query_all(SYMBOL, EXCHANGE))
    finally:
        client.close()


@pytest.fixture
def mesh():
    cm = ClusterManager()
    cm.extra_node_args = ["--anti-entropy-interval-seconds", str(INTERVAL_S)]
    cm.start_multi_master(node_count=3)
    cm.wait_for_mm_mesh(timeout=45)
    yield cm
    cm.shutdown()


def outage(mesh, writers: list[int]) -> tuple[int, int]:
    """Rounds on `writers` in turn, node 2 killed, two more rounds, node 2 back: what the writers
    hold and what node 2 holds once it has had six reconciliations."""
    ts = BASE_TS
    for r in range(ROUNDS):
        write(mesh.nodes[writers[r % len(writers)]], ts, PER_ROUND)
        ts += PER_ROUND
        time.sleep(0.3)          # delivered before the other writer's numbers depend on it
    deadline = time.monotonic() + 30
    while rows(mesh.nodes[2]) < ROUNDS * PER_ROUND and time.monotonic() < deadline:
        time.sleep(0.5)
    assert rows(mesh.nodes[2]) == ROUNDS * PER_ROUND, "the premise: the mesh delivered everything"

    mesh.kill_node(2)
    for r in range(2):
        write(mesh.nodes[writers[r % len(writers)]], ts, PER_ROUND)
        ts += PER_ROUND
        time.sleep(0.3)
    mesh.restart_node(2)
    mesh.wait_for_mm_mesh(timeout=90)
    expected = (ROUNDS + 2) * PER_ROUND
    deadline = time.monotonic() + 6 * INTERVAL_S
    got = rows(mesh.nodes[2])
    while got != expected and time.monotonic() < deadline:
        time.sleep(1.0)
        got = rows(mesh.nodes[2])
    held = [rows(mesh.nodes[w]) for w in sorted(set(writers))]
    assert held == [expected] * len(held), f"the premise: the writers hold every row ({held})"
    return expected, got


def test_a_node_that_missed_one_writers_rows_gets_them_back(mesh):
    expected, got = outage(mesh, writers=[0])
    assert got == expected, f"node 2 holds {got} rows where the writer holds {expected}"


def test_a_node_that_missed_two_writers_rows_gets_them_back(mesh):
    expected, got = outage(mesh, writers=[0, 1])
    if got != expected:
        raise Diverged(f"node 2 holds {got} rows where the writers hold {expected}")


def test_a_restarted_node_claims_nothing_it_did_not_write(mesh):
    """#185: a clean restart of a node holding segments of a symbol only node 0 writes. It used to
    declare its own frontier for the symbol from the highest number in them - node 0's - so it said
    it held records of its own origin nobody wrote; its peers lacked them, and every reconciliation
    started a catch-up that read its whole WAL for them and counted them unfillable. Measured with
    the probe this is built from: 14 catch-ups in 30 s, 140 ranges counted."""
    writer, restarted = mesh.nodes[0], mesh.nodes[2]
    write(writer, BASE_TS, PER_ROUND)
    deadline = time.monotonic() + 30
    while rows(restarted) < PER_ROUND and time.monotonic() < deadline:
        time.sleep(0.5)
    assert rows(restarted) == PER_ROUND, "the premise: the restarted node holds the rows"
    for node in mesh.nodes:
        client = client_for(node)
        try:
            client.flush()     # in segments, which is what the start read the claim from
        finally:
            client.close()

    mesh._stop_node(restarted)          # clean: its vector and its checkpoint are written
    offset = node_log_size(restarted)
    mesh.restart_node(2)
    mesh.wait_for_mm_mesh(timeout=90)
    time.sleep(3 * INTERVAL_S + 2)      # three reconciliations
    said = node_log_since(mesh.nodes[2], offset)
    started = len(re.findall(r"Starting catch-up to peer", said))
    unfillable = metric(mesh.nodes[2], "ob_mm_catchup_unfillable_total")
    assert started == 0 and unfillable == 0, (
        f"the restarted node started {started} catch-up(s) and counted {unfillable:.0f} unfillable "
        "range(s): it claims records of its own origin that nobody wrote")
