"""A mesh node that missed more than a catch-up round reads gets back what it missed (#178).

`--mm-max-catchup-bytes` bounds how much of its WAL a node reads for one peer at a time, because
the reading shares the io loop with live traffic. It used to bound the whole catch-up: past it the
catch-up stopped sending, said it was "falling back to snapshot sync", and nothing ever sent a
snapshot to a node that holds data - nor may anything, since installing one discards what is
there. The next catch-up read from the first record again and stopped at the same place. Measured
before the fix with the ceiling at 1 MiB: the returning node stopped at 6 990 of 20 100 rows, and
every catch-up after the first read the whole WAL and sent nothing.

A node is killed, its peers write more than the ceiling, and it comes back with data of its own.
Its own mesh, because the ceiling is set for every node and one node is killed.
"""
from __future__ import annotations

import time

import pytest

from conftest import ClusterManager, node_log_since, node_log_size
from orderbook_engine import BookUpdate, OrderbookEngine

pytestmark = pytest.mark.multi_master

EXCHANGE = "CEIL"
BASE_TS = 1_700_000_000_000_000_000
CEILING = 1 << 20                  # 1 MiB of WAL a catch-up may scan
SYMBOLS = 10
MISSED = 2_000                     # a symbol: 20 000 records, well past the ceiling


class NotCaughtUp(AssertionError):
    """The returning node holds fewer rows than the node that wrote them."""


def symbol(i: int) -> str:
    return f"C{i:02d}"


def client_for(node, timeout: float = 30.0) -> OrderbookEngine:
    return OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=timeout)


def write(node, first_ts: int, per_symbol: int) -> None:
    client = client_for(node)
    try:
        updates = [BookUpdate(symbol(s), EXCHANGE, "bid", [100_000 + k], [5],
                              timestamp_ns=first_ts + s * per_symbol + k)
                   for s in range(SYMBOLS) for k in range(per_symbol)]
        for start in range(0, len(updates), 512):
            outcomes = client.insert_batch(updates[start:start + 512])
            assert all(o.ok for o in outcomes), [o for o in outcomes if not o.ok][:3]
    finally:
        client.close()


def rows_per_symbol(node) -> dict:
    client = client_for(node)
    try:
        client.flush()
        return {symbol(s): len(client.query_all(symbol(s), EXCHANGE)) for s in range(SYMBOLS)}
    finally:
        client.close()


@pytest.fixture
def ceiling_mesh():
    cm = ClusterManager()
    cm.extra_node_args = ["--mm-max-catchup-bytes", str(CEILING)]
    cm.start_multi_master(node_count=3)
    cm.wait_for_mm_mesh(timeout=45)
    yield cm
    cm.shutdown()


def test_a_node_that_missed_more_than_the_ceiling_gets_it_all_back(ceiling_mesh):
    cm = ceiling_mesh
    writer, returning = cm.nodes[0], cm.nodes[2]

    # Data of its own first, so the returning node is one a snapshot may not replace.
    write(writer, BASE_TS, 10)
    deadline = time.monotonic() + 60
    while rows_per_symbol(returning) != rows_per_symbol(writer):
        assert time.monotonic() < deadline, "the premise: the mesh converges on the first rows"
        time.sleep(1.0)

    cm.kill_node(2)
    write(writer, BASE_TS + 1_000_000, MISSED)
    offsets = {n.index: node_log_size(n) for n in cm.nodes}
    cm.restart_node(2)
    cm.wait_for_mm_mesh(timeout=90)

    expected = rows_per_symbol(writer)
    assert all(v == 10 + MISSED for v in expected.values()), (
        f"the premise: the writer holds every row it wrote, and it holds {expected}")
    # Done, or stuck: a count that has not moved for 30 s - two catch-up rounds at the default
    # pace would have moved it - is an answer, and 120 s bounds a slow machine either way.
    got, last_change, last_total = {}, time.monotonic(), -1
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        got = rows_per_symbol(returning)
        if got == expected:
            break
        if sum(got.values()) != last_total:
            last_total, last_change = sum(got.values()), time.monotonic()
        elif time.monotonic() - last_change > 30:
            break
        time.sleep(2.0)
    if got != expected:
        said = []
        for n in cm.nodes:
            lines = [line for line in node_log_since(n, offsets.get(n.index, 0)).splitlines()
                     if "atch-up" in line or "snapshot" in line.lower()]
            said += [f"node {n.index}: {line[:300]}" for line in lines[:3] + lines[-3:]]
        raise NotCaughtUp(
            f"the returning node stopped at {sum(got.values())} of "
            f"{sum(expected.values())} rows ({got}); what each node said about catching up, "
            f"first and last three lines:\n" + "\n".join(said))

    # What it took, from the peers' own summary lines: a catch-up of more than the ceiling is
    # several rounds of it now, not one that stops.
    for n in cm.nodes[:2]:
        for line in node_log_since(n, offsets.get(n.index, 0)).splitlines():
            if "Catch-up to peer 3 finished" in line:
                print(f"node {n.index}: {line[line.find('Catch-up'):][:200]}")
