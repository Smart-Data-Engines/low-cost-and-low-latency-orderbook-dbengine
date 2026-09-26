"""What a mesh does once a node's version vector passes what one frame can carry (#177).

A version vector travels in one record whose `payload_len` is a `uint16_t`, so above 1 561
(symbol, origin) entries - 42 bytes each - it is replaced by the "send everything" marker. The
architecture document says that costs bandwidth and never data. This module measures what it costs,
on one three-node mesh, first below the limit and then just above it:

- a vector exchange - reconciliation sends one to every peer each interval - makes the receiving
  node scan its retained WAL and send the peer every record in it, which the peer then drops as a
  duplicate;
- a node that joins holding nothing cannot ask for a snapshot, because a peer that "wants
  everything" says nothing about what it holds.

Measured before the fix on the i3-7100U with the default 60 s interval and 70 s windows: 0, 0 and
0 duplicates at 1 500 entries, 19 200, 14 400 and 9 600 at 1 600, and a joiner that asked for no
snapshot. Here the interval is 5 s, so a window of 16 s holds three of them.

Its own mesh, because it sets the interval, writes 1 600 symbols and adds a node for good.
"""
from __future__ import annotations

import re
import time
import urllib.request

import pytest

from conftest import ClusterManager
from orderbook_engine import BookUpdate, OrderbookEngine

pytestmark = pytest.mark.multi_master

EXCHANGE = "VVS"
BASE_TS = 1_700_000_000_000_000_000
BELOW = 1_500          # (symbol, origin) entries a vector can carry: 1 561
ABOVE = 1_600
INTERVAL_S = 5
WINDOW = 16.0          # three reconciliation intervals


def symbol(i: int) -> str:
    return f"V{i:05d}"


def client_for(node, timeout: float = 30.0) -> OrderbookEngine:
    return OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=timeout)


def scrape(port: int, timeout: float = 6.0) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=timeout) as resp:
        return resp.read().decode(errors="replace")


def metric_value(body: str, name: str) -> float:
    match = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)$", body, re.M)
    return float(match.group(1)) if match else 0.0


def metric(node, name: str) -> float:
    return metric_value(scrape(node.metrics_port), name)


def write_symbols(node, first: int, last: int) -> None:
    client = client_for(node)
    try:
        updates = [BookUpdate(symbol(i), EXCHANGE, "bid", [100_000 + i], [5],
                              timestamp_ns=BASE_TS + i)
                   for i in range(first, last)]
        for start in range(0, len(updates), 512):
            outcomes = client.insert_batch(updates[start:start + 512])
            assert all(o.ok for o in outcomes), [o for o in outcomes if not o.ok][:3]
    finally:
        client.close()


def wait_until_every_node_has(nodes, last: int, timeout: float = 60.0) -> None:
    """Every node answers for the last symbol written - the mesh delivered the batch."""
    deadline = time.monotonic() + timeout
    for node in nodes:
        client = client_for(node)
        try:
            while len(client.query_all(symbol(last - 1), EXCHANGE)) < 1:
                assert time.monotonic() < deadline, f"node {node.index} never got {symbol(last - 1)}"
                time.sleep(0.5)
        finally:
            client.close()


def flush_everywhere(nodes) -> None:
    """What a node tells its peers it holds is refreshed when a checkpoint is written, and since
    part 2a of #165 that can be ten seconds after a write: a reconciliation in between reads a
    vector without the batch, and its peers send the batch back. That costs duplicates below the
    limit too, so it would make this module measure the wrong thing; a flush writes the checkpoint.
    """
    for node in nodes:
        client = client_for(node)
        try:
            client.flush()
        finally:
            client.close()


def duplicates_over_window(nodes) -> dict:
    before = {n.index: metric(n, "ob_mm_duplicates_dropped") for n in nodes}
    time.sleep(WINDOW)
    return {n.index: metric(n, "ob_mm_duplicates_dropped") - before[n.index] for n in nodes}


@pytest.fixture(scope="module")
def vv_mesh():
    cm = ClusterManager()
    cm.extra_node_args = ["--anti-entropy-interval-seconds", str(INTERVAL_S)]
    cm.start_multi_master(node_count=3)
    cm.wait_for_mm_mesh(timeout=45)
    yield cm
    cm.shutdown()


def test_below_the_limit_a_converged_mesh_resends_nothing(vv_mesh):
    """The control: 1 500 entries fit one frame, and reconciliation finds nothing to send."""
    write_symbols(vv_mesh.nodes[0], 0, BELOW)
    wait_until_every_node_has(vv_mesh.nodes, BELOW)
    flush_everywhere(vv_mesh.nodes)
    time.sleep(2 * INTERVAL_S)                      # the delivery's own tail
    dropped = duplicates_over_window(vv_mesh.nodes)
    print(f"below the limit ({BELOW} entries): duplicates dropped over {WINDOW:.0f} s: {dropped}")
    assert all(v < BELOW // 10 for v in dropped.values()), dropped


@pytest.mark.xfail(strict=True, reason="#177: a vector past 1 561 entries is sent as 'send "
                                       "everything', so every reconciliation resends the WAL")
def test_above_the_limit_a_converged_mesh_resends_nothing(vv_mesh):
    write_symbols(vv_mesh.nodes[0], BELOW, ABOVE)
    wait_until_every_node_has(vv_mesh.nodes, ABOVE)
    flush_everywhere(vv_mesh.nodes)
    time.sleep(2 * INTERVAL_S)
    dropped = duplicates_over_window(vv_mesh.nodes)
    print(f"above the limit ({ABOVE} entries): duplicates dropped over {WINDOW:.0f} s: {dropped}")
    assert all(v < ABOVE // 10 for v in dropped.values()), dropped


@pytest.mark.xfail(strict=True, reason="#177: a node that joins a mesh whose vectors are past "
                                       "1 561 entries never asks for a snapshot")
def test_a_node_that_joins_above_the_limit_asks_for_a_snapshot(vv_mesh):
    if len(vv_mesh.nodes) != 3:
        pytest.skip("runs on the mesh the two tests above filled; select the whole module")
    joiner = vv_mesh.add_multi_master_node(timeout=60)
    vv_mesh.wait_for_mm_mesh(timeout=90)
    # A joiner asks when the first peer's vector arrives, within the handshake's two-second grace:
    # four intervals is room, not a guess at a slow machine.
    deadline = time.monotonic() + 4 * INTERVAL_S
    requested = 0.0
    while time.monotonic() < deadline:
        requested = metric(joiner, "ob_mm_snapshot_requested_total")
        if requested >= 1.0:
            break
        time.sleep(0.5)
    print(f"the joiner asked for {requested:.0f} snapshot(s)")
    assert requested >= 1.0
