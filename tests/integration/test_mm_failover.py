"""Multi-master with a node missing.

There is no election here and no primary to lose, so "failover" means something
narrower and more useful: the cluster has no single point of failure. Losing a node
must not stop the others from accepting writes or from converging with each other,
and a node that comes back must end up with what it missed.

Every test takes `healthy_mm_cluster`, which restarts dead nodes and waits for the
mesh afterwards. Multi-master has no election, so a node left dead is not replaced —
it just hands the next test a smaller mesh and no explanation.
"""
from __future__ import annotations

import time

import pytest

from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.multi_master

CONVERGE_TIMEOUT = 25.0


def client_for(node, timeout: float = 20.0) -> OrderbookEngine:
    return OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=timeout)


def write(node, symbol: str, prices: list[int], qtys: list[int]) -> None:
    client = client_for(node)
    try:
        client.insert(symbol, "BINANCE", "bid", prices, qtys)
        client.flush()
    finally:
        client.close()


def wait_for_prices(node, symbol: str, expected: list[int],
                    timeout: float = CONVERGE_TIMEOUT) -> list[int]:
    """Poll a node until it holds the expected prices, then return what it has."""
    wanted = sorted(expected)
    deadline = time.monotonic() + timeout
    got: list[int] = []
    while time.monotonic() < deadline:
        client = client_for(node)
        try:
            client.flush()
            got = sorted(r.price for r in client.query_all(symbol, "BINANCE"))
        finally:
            client.close()
        if got == wanted:
            return got
        time.sleep(0.5)
    return got


def peer_status(cluster, node) -> dict[str, str]:
    reply = cluster._send(node, "MM_PEERS")
    rows = [ln.split("\t") for ln in reply.strip().splitlines()[1:] if ln]
    return {row[0]: row[2] for row in rows}


def test_losing_a_node_leaves_the_others_writable(healthy_mm_cluster):
    """No primary means no single point of failure. That has to be true in practice."""
    cluster = healthy_mm_cluster
    victim = cluster.nodes[2]
    survivors = cluster.nodes[:2]

    cluster.kill_node(victim.index)
    time.sleep(2.0)

    for index, node in enumerate(survivors):
        write(node, "MMF-WRITABLE", [600_000 + index], [7])

    for node in survivors:
        got = wait_for_prices(node, "MMF-WRITABLE", [600_000, 600_001])
        assert got == [600_000, 600_001], (
            f"{node.node_id} has {got} while one of three nodes is down")


def test_a_dead_peer_is_reported_as_disconnected(healthy_mm_cluster):
    """A peer that is gone must not keep showing as connected."""
    cluster = healthy_mm_cluster
    victim = cluster.nodes[2]
    observer = cluster.nodes[0]
    victim_key = str(victim.index + 1)

    cluster.kill_node(victim.index)

    deadline = time.monotonic() + 30
    status = ""
    while time.monotonic() < deadline:
        status = peer_status(cluster, observer).get(victim_key, "absent")
        if status != "connected":
            break
        time.sleep(0.5)

    assert status != "connected", (
        f"{observer.node_id} still reports peer {victim_key} as connected after it "
        f"was killed")


@pytest.mark.xfail(
    strict=True,
    reason="roadmap #61: catch-up decides what to send by comparing byte offsets in "
           "two independent WALs. In multi-master each node writes its own records "
           "and the remote ones it applies, so the same set of records yields "
           "different offsets, and after an earlier outage the rejoining node can "
           "look 'not behind' while still missing records. Measured: cycle 0 and 1 "
           "recover, cycle 2 loses a row. Passes in isolation because a single "
           "outage happens to line the offsets up favourably; the two tests above "
           "have each already restarted this node. strict=True so the marker cannot "
           "outlive the fix.")
def test_a_restarted_node_catches_up_on_what_it_missed(healthy_mm_cluster):
    """Rows written during an outage must reach the node that was away.

    This asserts the outcome, not the route: whether the data arrives through
    catch-up streaming from a peer's WAL position or through re-broadcast is an
    implementation detail, and the node's own data directory cannot be the source
    because it was not running when these rows were written.
    """
    cluster = healthy_mm_cluster
    victim = cluster.nodes[2]
    writer = cluster.nodes[0]

    write(writer, "MMF-CATCHUP", [700_000], [1])
    assert wait_for_prices(victim, "MMF-CATCHUP", [700_000]) == [700_000], (
        "the cluster was not converged before the test began")

    cluster.kill_node(victim.index)
    time.sleep(1.5)

    # Written while the third node is not running.
    write(writer, "MMF-CATCHUP", [701_000, 702_000], [2, 3])
    time.sleep(1.0)

    cluster.restart_node(victim.index)

    got = wait_for_prices(victim, "MMF-CATCHUP", [700_000, 701_000, 702_000],
                          timeout=45)
    assert got == [700_000, 701_000, 702_000], (
        f"the restarted node holds {got}; rows written during its outage did not "
        f"reach it")


def test_the_mesh_reconverges_after_a_restart(healthy_mm_cluster):
    cluster = healthy_mm_cluster
    victim = cluster.nodes[1]

    cluster.kill_node(victim.index)
    time.sleep(2.0)
    cluster.restart_node(victim.index)

    cluster.wait_for_mm_mesh(timeout=45)

    for node in cluster.nodes:
        statuses = peer_status(cluster, node)
        assert len(statuses) == len(cluster.nodes) - 1, (
            f"{node.node_id} sees {statuses}")
        assert all(s == "connected" for s in statuses.values()), (
            f"{node.node_id} after reconvergence: {statuses}")


def test_a_restarted_node_accepts_writes_again(healthy_mm_cluster):
    cluster = healthy_mm_cluster
    victim = cluster.nodes[2]

    cluster.kill_node(victim.index)
    time.sleep(1.5)
    cluster.restart_node(victim.index)
    cluster.wait_for_mm_mesh(timeout=45)

    write(victim, "MMF-REWRITE", [800_000], [4])

    for node in cluster.nodes:
        got = wait_for_prices(node, "MMF-REWRITE", [800_000])
        assert got == [800_000], (
            f"{node.node_id} did not receive the write made on the restarted node: "
            f"{got}")


def test_writes_during_an_outage_are_not_lost_by_the_survivors(healthy_mm_cluster):
    """The two nodes that stayed up must agree with each other regardless."""
    cluster = healthy_mm_cluster
    victim = cluster.nodes[0]
    survivors = cluster.nodes[1:]

    cluster.kill_node(victim.index)
    time.sleep(2.0)

    write(survivors[0], "MMF-SURVIVE", [900_000, 901_000], [1, 2])

    for node in survivors:
        got = wait_for_prices(node, "MMF-SURVIVE", [900_000, 901_000])
        assert got == [900_000, 901_000], f"{node.node_id} has {got}"
