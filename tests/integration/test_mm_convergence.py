"""Multi-master: three nodes that all accept writes, and converge.

A different topology from the rest of the suite — no primary, no election, peers
found through the etcd topology watch. The promise being tested is that a write to
any node becomes visible on every node, and that MM_PEERS describes the mesh
truthfully, because that command is what an operator has to trust.
"""
from __future__ import annotations

import time

import pytest

from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.multi_master

# Replication is asynchronous; this is how long a test waits for a row to travel.
CONVERGE_TIMEOUT = 20.0


def client_for(node, timeout: float = 20.0) -> OrderbookEngine:
    return OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=timeout)


def peers_of(cluster, node) -> list[list[str]]:
    """MM_PEERS rows as split columns: node_id, address, status, hlc, lag."""
    reply = cluster._send(node, "MM_PEERS")
    lines = [ln for ln in reply.strip().splitlines() if ln]
    return [ln.split("\t") for ln in lines[1:]]


def wait_for_rows(node, symbol: str, exchange: str, expected: int,
                  timeout: float = CONVERGE_TIMEOUT) -> list:
    """Poll one node until it has the expected row count, or give up loudly."""
    deadline = time.monotonic() + timeout
    rows: list = []
    while time.monotonic() < deadline:
        client = client_for(node)
        try:
            client.flush()
            rows = client.query_all(symbol, exchange)
        finally:
            client.close()
        if len(rows) >= expected:
            return rows
        time.sleep(0.5)
    return rows


def test_every_node_reports_multi_master(mm_cluster):
    for node in mm_cluster.nodes:
        role = mm_cluster._send(node, "ROLE").strip().upper()
        assert "MULTI_MASTER" in role, f"{node.node_id} reports {role!r}"


def test_every_node_sees_every_peer_connected(mm_cluster):
    for node in mm_cluster.nodes:
        rows = peers_of(mm_cluster, node)
        assert len(rows) == len(mm_cluster.nodes) - 1, (
            f"{node.node_id} sees {len(rows)} peers")
        for row in rows:
            assert row[2] == "connected", f"{node.node_id} → peer {row[0]}: {row[2]}"


def test_mm_peers_reports_an_address_for_every_peer(mm_cluster):
    """A peer list without addresses tells an operator nothing.

    A peer that dialled us arrives over an accepted socket whose source port is
    ephemeral, so the connection has no usable address of its own. Until the address
    was taken from the registry, every inbound peer — half the mesh — showed a blank
    column.
    """
    for node in mm_cluster.nodes:
        for row in peers_of(mm_cluster, node):
            address = row[1]
            assert address, (
                f"{node.node_id} reports no address for peer {row[0]}")
            assert ":" in address, f"address {address!r} is not host:port"


def test_hlc_reflects_what_was_received_from_each_peer(mm_cluster):
    """The hlc_timestamp column has to mean something.

    PeerConnection::last_hlc was printed by MM_PEERS and written nowhere, so this
    column read 0.0.0 for every peer no matter how much data had flowed. Its
    definition is "the last HLC received from this peer", so a node that has received
    nothing from a peer should still read zero — which is what makes the assertion
    below meaningful rather than just non-zero-hunting.
    """
    writer_node = mm_cluster.nodes[0]
    reader_node = mm_cluster.nodes[1]

    writer = client_for(writer_node)
    try:
        writer.insert("MM-HLC", "BINANCE", "bid", [100_000], [10])
        writer.flush()
    finally:
        writer.close()

    deadline = time.monotonic() + CONVERGE_TIMEOUT
    hlc_for_writer = "0.0.0"
    while time.monotonic() < deadline:
        for row in peers_of(mm_cluster, reader_node):
            if row[0] == str(writer_node.index + 1):
                hlc_for_writer = row[3]
        if hlc_for_writer != "0.0.0":
            break
        time.sleep(0.5)

    assert hlc_for_writer != "0.0.0", (
        f"{reader_node.node_id} received data from {writer_node.node_id} but reports "
        f"hlc {hlc_for_writer} for it")


def test_a_write_on_one_node_reaches_the_others(mm_cluster):
    source = mm_cluster.nodes[0]
    client = client_for(source)
    try:
        client.insert("MM-ONE", "BINANCE", "bid", [200_000, 199_000], [10, 20])
        client.flush()
    finally:
        client.close()

    for node in mm_cluster.nodes[1:]:
        rows = wait_for_rows(node, "MM-ONE", "BINANCE", expected=2)
        assert len(rows) == 2, (
            f"{node.node_id} has {len(rows)} of 2 rows after {CONVERGE_TIMEOUT}s")
        assert sorted(r.price for r in rows) == [199_000, 200_000]


def test_writes_to_all_three_nodes_converge(mm_cluster):
    """Every node accepts writes, and every node ends up with all of them."""
    for index, node in enumerate(mm_cluster.nodes):
        client = client_for(node)
        try:
            client.insert("MM-ALL", "BINANCE", "bid",
                          [300_000 + index * 1_000], [5 + index])
            client.flush()
        finally:
            client.close()

    expected_prices = [300_000 + i * 1_000 for i in range(len(mm_cluster.nodes))]

    for node in mm_cluster.nodes:
        rows = wait_for_rows(node, "MM-ALL", "BINANCE",
                             expected=len(expected_prices))
        got = sorted(r.price for r in rows)
        assert got == sorted(expected_prices), (
            f"{node.node_id} converged to {got}, expected {sorted(expected_prices)}")


def test_no_node_rejects_a_write_as_read_only(mm_cluster):
    """There is no primary here, so read-only rejection would be a bug."""
    for index, node in enumerate(mm_cluster.nodes):
        client = client_for(node)
        try:
            client.insert("MM-WRITABLE", "BINANCE", "ask",
                          [400_000 + index], [1])
            client.flush()
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"{node.node_id} rejected a write: {exc!r}")
        finally:
            client.close()


def test_mm_conflicts_is_queryable(mm_cluster):
    """The conflict log must answer with its documented columns, empty or not."""
    reply = mm_cluster._send(mm_cluster.nodes[0], "MM_CONFLICTS")
    header = reply.strip().splitlines()[0]
    assert header.split("\t")[:4] == ["symbol", "exchange", "side", "price"], header


def test_concurrent_writes_to_the_same_level_resolve_consistently(mm_cluster):
    """Same symbol, side and price on two nodes at once: one value must win.

    Last-writer-wins by HLC means the nodes must agree on *which* value, not merely
    that they each kept one. Disagreement here is a permanent split.
    """
    price = 500_000
    for index, node in enumerate(mm_cluster.nodes[:2]):
        client = client_for(node)
        try:
            client.insert("MM-LWW", "BINANCE", "bid", [price], [100 + index])
            client.flush()
        finally:
            client.close()

    time.sleep(3.0)

    seen: dict[str, list[int]] = {}
    for node in mm_cluster.nodes:
        client = client_for(node)
        try:
            client.flush()
            rows = client.query_all("MM-LWW", "BINANCE")
        finally:
            client.close()
        seen[node.node_id] = sorted(r.quantity for r in rows if r.price == price)

    values = list(seen.values())
    assert all(v == values[0] for v in values), (
        f"nodes disagree about the level after concurrent writes: {seen}")
