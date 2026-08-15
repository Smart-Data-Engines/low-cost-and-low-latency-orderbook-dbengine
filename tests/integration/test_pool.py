"""Client pool mode: discovery and routing.

A pool takes several addresses and works out for itself which node accepts writes.
The failure that matters is not "an error was raised" but "the write went to the
wrong node and nobody noticed", so these tests check where the data landed rather
than whether the call returned.

Tests that kill a node live in test_failover.py, which restores the cluster
afterwards. Nothing here changes the topology.
"""
from __future__ import annotations

import pytest

from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.pool


def direct(node) -> OrderbookEngine:
    """A single-node client, to check what a specific node actually holds."""
    return OrderbookEngine(host="127.0.0.1", port=node.tcp_port)


def test_pool_discovers_the_primary(pool_client: OrderbookEngine, cluster):
    """The pool must identify the primary itself, from ROLE, not from ordering."""
    pool = pool_client._pool
    assert pool is not None, "pool_client is not in pool mode"

    expected = f"127.0.0.1:{cluster.primary().tcp_port}"
    assert pool._primary_key == expected, (
        f"pool thinks the primary is {pool._primary_key}, cluster says {expected}")


def test_pool_connects_to_every_node(pool_client: OrderbookEngine, cluster):
    pool = pool_client._pool
    connected = {key for key, backend in pool._connections.items() if backend}
    expected = {f"127.0.0.1:{n.tcp_port}" for n in cluster.nodes}
    assert connected == expected, (
        f"pool connected to {connected}, cluster has {expected}")


def test_pool_write_lands_on_the_primary(pool_client: OrderbookEngine, cluster):
    """Routing is only proven by finding the row on the primary."""
    pool_client.insert("POOL-ROUTE", "BINANCE", "bid", [410_000], [11])
    pool_client.flush()

    primary = direct(cluster.primary())
    try:
        rows = primary.query_all("POOL-ROUTE", "BINANCE")
    finally:
        primary.close()

    assert len(rows) == 1, f"the write did not reach the primary: {rows}"
    assert rows[0].price == 410_000
    assert rows[0].quantity == 11


def test_pool_read_returns_rows(pool_client: OrderbookEngine):
    pool_client.insert("POOL-READ", "BINANCE", "ask", [420_000, 421_000], [5, 6])
    pool_client.flush()

    rows = pool_client.query_all("POOL-READ", "BINANCE")

    assert len(rows) == 2
    assert sorted(r.price for r in rows) == [420_000, 421_000]


def test_pool_ping_and_status(pool_client: OrderbookEngine):
    assert pool_client.ping().strip() == "PONG"

    status = pool_client.status()
    assert status.get("mode") == "pool"
    assert "inserts" in status


def test_pool_reports_role_of_the_node_it_read_from(pool_client: OrderbookEngine):
    """STATUS through a pool must describe a real node, not a merged fiction."""
    status = pool_client.status()
    role = status.get("role")
    assert role is not None, f"STATUS carries no role: {sorted(status)}"
    assert role.lower() in ("primary", "replica", "standalone"), role


def test_pool_aggregates_are_routed_and_scaled(pool_client: OrderbookEngine):
    """query_agg() goes through execute_read, so the pool path needs its own test."""
    pool_client.insert("POOL-AGG", "BINANCE", "bid", [500_000], [40])
    pool_client.insert("POOL-AGG", "BINANCE", "ask", [501_000], [20])

    aggs = pool_client.query_agg("POOL-AGG", "BINANCE", "SPREAD(*)", "MID_PRICE(*)")

    assert aggs["SPREAD(*)"].value == 1_000
    assert aggs["MID_PRICE(*)"].scale == 1_000_000
    assert aggs["MID_PRICE(*)"].real == pytest.approx(500_500.0)


def test_pool_with_a_single_host_still_works(cluster):
    """A one-address pool is a legitimate configuration, not a degenerate case."""
    engine = OrderbookEngine(hosts=[f"127.0.0.1:{cluster.primary().tcp_port}"],
                             timeout=10.0)
    try:
        assert engine.ping().strip() == "PONG"
        engine.insert("POOL-SINGLE", "BINANCE", "bid", [430_000], [7])
        engine.flush()
        rows = engine.query_all("POOL-SINGLE", "BINANCE")
        assert len(rows) == 1
    finally:
        engine.close()


def test_pool_with_an_unreachable_address_still_finds_the_primary(cluster):
    """A stale address in the config must not take the pool down with it.

    Deployment configs outlive clusters: an address that no longer answers is the
    normal case, not the exceptional one.
    """
    free_port = cluster.find_free_port()
    hosts = [f"127.0.0.1:{free_port}"] + [
        f"127.0.0.1:{n.tcp_port}" for n in cluster.nodes
    ]

    engine = OrderbookEngine(hosts=hosts, timeout=5.0)
    try:
        assert engine._pool._primary_key == f"127.0.0.1:{cluster.primary().tcp_port}"
        engine.insert("POOL-STALE", "BINANCE", "bid", [440_000], [3])
        engine.flush()
        assert len(engine.query_all("POOL-STALE", "BINANCE")) == 1
    finally:
        engine.close()


def test_pool_with_no_reachable_node_fails_clearly(cluster):
    """Every address dead should say so, not hang or return an empty result."""
    dead = [f"127.0.0.1:{cluster.find_free_port()}" for _ in range(2)]

    with pytest.raises((OrderbookError, OSError, ConnectionError)):
        engine = OrderbookEngine(hosts=dead, timeout=3.0)
        try:
            engine.insert("POOL-DEAD", "BINANCE", "bid", [1], [1])
        finally:
            engine.close()
