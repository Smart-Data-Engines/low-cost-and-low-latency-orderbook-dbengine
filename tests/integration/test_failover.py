"""Failover: handing the primary role over, and losing it unexpectedly.

Every test here changes the cluster's topology, so they all take `healthy_cluster`
rather than `cluster`: its teardown restarts whatever is not running and refuses to
finish unless exactly one node holds the primary role. Without that, one killed node
turns the rest of the session red for reasons unrelated to the code being tested.

Two things are being checked, and they are different:

- **Graceful handover** (`FAILOVER <node_id>`) must land the role on the node that
  was named. Before roadmap #29 it ignored the target and the outgoing primary won
  the race back roughly half the time.
- **An unplanned loss** (SIGKILL) must promote the survivor, keep the data that was
  acknowledged, and accept writes again.
"""
from __future__ import annotations

import socket
import time

import pytest

from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.failover

# Read by the console report; failover_time_sec is rendered specially.
custom_metrics: dict = {}


def send_command(port: int, command: str, timeout: float = 10.0) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.recv(4096)  # banner
        sock.sendall((command + "\n").encode())
        time.sleep(0.3)
        try:
            return sock.recv(65536).decode(errors="replace")
        except socket.timeout:
            return ""


def role_of(port: int, timeout: float = 5.0) -> str:
    try:
        return send_command(port, "ROLE", timeout=timeout).strip().upper()
    except OSError:
        return "UNREACHABLE"


def wait_for_role(port: int, expected: str, timeout: float = 30.0) -> float:
    """Wait until a node reports the expected role. Returns how long it took."""
    start = time.monotonic()
    deadline = start + timeout
    last = ""
    while time.monotonic() < deadline:
        last = role_of(port)
        if expected in last:
            return time.monotonic() - start
        time.sleep(0.25)
    raise AssertionError(
        f"node on port {port} did not report {expected} within {timeout}s; "
        f"last ROLE was {last!r}")


# ── Graceful handover ─────────────────────────────────────────────────────────

@pytest.mark.xfail(
    strict=True,
    reason="roadmap #60: FAILOVER <target> validates the target against "
           "get_published_positions(), and no production code path ever publishes a "
           "position — publish_wal_position() is called only from tests and from one "
           "connectivity check in shard_coordinator.cpp. So every graceful handover "
           "on a real cluster answers ERR unknown_target. strict=True on purpose: "
           "when the fix lands, this test starts passing and the suite fails until "
           "the marker is removed.")
def test_handover_lands_on_the_named_target(healthy_cluster):
    """The role must go to the node named in the command, not to whoever polls first."""
    primary = healthy_cluster.primary()
    target = healthy_cluster.replica()

    reply = send_command(primary.tcp_port, f"FAILOVER {target.node_id}")
    assert reply.strip().startswith("OK"), f"handover refused: {reply!r}"

    elapsed = wait_for_role(target.tcp_port, "PRIMARY", timeout=30)
    custom_metrics["failover_time_sec"] = elapsed
    custom_metrics["failover_kind"] = "graceful"

    # And the node that handed the role over must not have taken it back.
    assert "PRIMARY" not in role_of(primary.tcp_port), (
        "the outgoing primary reacquired the role it handed over — this is the race "
        "roadmap #29 fixed, and it used to happen about half the time")


def test_handover_to_an_unknown_node_is_refused(healthy_cluster):
    primary = healthy_cluster.primary()

    reply = send_command(primary.tcp_port, "FAILOVER node-that-does-not-exist")

    assert reply.startswith("ERR"), f"an unknown target was accepted: {reply!r}"
    assert "unknown_target" in reply, reply
    # A rejected handover is never a partial one: the node keeps its role.
    assert "PRIMARY" in role_of(primary.tcp_port), (
        "the primary lost its role to a handover it had rejected")


def test_handover_to_itself_is_refused(healthy_cluster):
    primary = healthy_cluster.primary()

    reply = send_command(primary.tcp_port, f"FAILOVER {primary.node_id}")

    assert reply.startswith("ERR"), f"self-handover was accepted: {reply!r}"
    assert "invalid_target" in reply, reply
    assert "PRIMARY" in role_of(primary.tcp_port)


def test_handover_from_a_replica_is_refused(healthy_cluster):
    replica = healthy_cluster.replica()
    primary = healthy_cluster.primary()

    reply = send_command(replica.tcp_port, f"FAILOVER {primary.node_id}")

    assert reply.startswith("ERR"), f"a replica accepted a handover: {reply!r}"
    assert "not_primary" in reply, reply


# ── Unplanned loss ───────────────────────────────────────────────────────────

def test_killing_the_primary_promotes_the_survivor(healthy_cluster):
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    start = time.monotonic()
    healthy_cluster.kill_node(primary.index)

    elapsed = wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)
    custom_metrics["failover_time_sec"] = elapsed
    custom_metrics["failover_kind"] = "kill"

    # Exactly one primary: two would be split brain, which is worse than none.
    roles = [role_of(n.tcp_port) for n in healthy_cluster.nodes]
    primaries = [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]
    assert len(primaries) == 1, f"expected one primary, roles={roles}"


def test_acknowledged_data_survives_a_kill(healthy_cluster):
    """Rows the server said OK to must be there after the primary is gone."""
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    writer = OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=20)
    try:
        writer.insert("FAILOVER-DATA", "BINANCE", "bid",
                      [700_000, 699_000, 698_000], [11, 12, 13])
        writer.flush()
    finally:
        writer.close()

    # Give replication a moment to carry the rows across before pulling the plug.
    time.sleep(1.5)
    healthy_cluster.kill_node(primary.index)
    wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)

    reader = OrderbookEngine(host="127.0.0.1", port=survivor.tcp_port, timeout=20)
    try:
        rows = reader.query_all("FAILOVER-DATA", "BINANCE")
    finally:
        reader.close()

    prices = sorted(r.price for r in rows)
    assert prices == [698_000, 699_000, 700_000], (
        f"rows acknowledged before the kill did not survive it: {prices}")


def test_the_promoted_node_accepts_writes(healthy_cluster):
    """A promoted replica that still rejects writes is not a primary."""
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    healthy_cluster.kill_node(primary.index)
    wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)

    client = OrderbookEngine(host="127.0.0.1", port=survivor.tcp_port, timeout=20)
    try:
        client.insert("FAILOVER-WRITE", "BINANCE", "ask", [800_000], [9])
        client.flush()
        rows = client.query_all("FAILOVER-WRITE", "BINANCE")
    finally:
        client.close()

    assert len(rows) == 1, (
        "the promoted node did not accept a write; read_only_flag_ is probably still "
        "set after the role transition")


def test_a_pool_client_follows_the_new_primary(healthy_cluster):
    """The pool must re-discover the primary on its own after one disappears."""
    hosts = [f"127.0.0.1:{n.tcp_port}" for n in healthy_cluster.nodes]
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    pool = OrderbookEngine(hosts=hosts, timeout=20.0, health_check_interval=1.0)
    try:
        pool.insert("FAILOVER-POOL", "BINANCE", "bid", [900_000], [4])
        pool.flush()

        healthy_cluster.kill_node(primary.index)
        wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)

        # No reconnect call: the pool is supposed to work this out itself.
        pool.insert("FAILOVER-POOL", "BINANCE", "bid", [901_000], [5])
        pool.flush()
        rows = pool.query_all("FAILOVER-POOL", "BINANCE")
    finally:
        pool.close()

    prices = sorted(r.price for r in rows)
    assert 901_000 in prices, (
        f"the write after failover did not land anywhere readable: {prices}")


def test_a_dead_node_is_reported_as_unreachable_not_as_a_replica(healthy_cluster):
    """A node that is gone must not be counted as a healthy follower."""
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    healthy_cluster.kill_node(primary.index)
    wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)

    assert role_of(primary.tcp_port, timeout=3) == "UNREACHABLE", (
        "a killed node still answered ROLE")

    client = OrderbookEngine(host="127.0.0.1", port=survivor.tcp_port, timeout=20)
    try:
        status = client.status()
    finally:
        client.close()

    replicas = status.get("replicas")
    assert replicas is not None, f"STATUS carries no replica count: {sorted(status)}"
    assert replicas == 0, (
        f"the new primary reports {replicas} replicas while the only other node is "
        f"dead")
