"""#201: a primary that gives the role up has to replicate from whoever takes it.

A node that stops being primary demotes itself to REPLICA, and starts a replication client - but
only towards a primary it can already read in the coordinator. When nobody has taken the role yet it
starts none: "Not knowing where to point the replication client is a reason to start no client"
(`src/failover.cpp`). And from then on its monitor loop, as a REPLICA, records the new primary's
address when one appears and does nothing else - on purpose, so that an unchanged leader does not
restart replication every second (#104) - while adoption happens only on a transition this node no
longer makes. So the node never replicates again. Its `ROLE` names the new primary all the same,
because that answer is the recorded address.

Found in the integration battery under load: the coordinator timed out, the primary lost its
lease and demoted with nobody published, the replica was elected ten seconds later, and every
test after that which read the replica failed - eleven of them, for the rest of the session.

Two ways in, both here: the primary losing its lease with no successor published (an outage of the
coordinator long enough to cost it the lease), and a planned `FAILOVER`, whose outgoing primary
demotes before the target has noticed the empty leader key.

Each test owns its `ClusterManager`: they move roles and stop etcd.
"""
from __future__ import annotations

import time

import pytest

from conftest import (ClusterManager, node_log_since, node_log_size, patience, raw_query, role_of,
                      send_command, wait_for_role)

pytestmark = pytest.mark.failover

LEASE_TTL_SECONDS = 10   # the engine's default (docs/cli.md); a step-down waits on it


@pytest.fixture
def cluster2():
    mgr = ClusterManager()
    mgr.start()
    try:
        yield mgr
    finally:
        mgr.shutdown()


def primary_index(mgr: ClusterManager, timeout: float) -> int:
    deadline = time.monotonic() + timeout
    last: list = []
    while time.monotonic() < deadline:
        last = [role_of(n.tcp_port) for n in mgr.nodes]
        primaries = [i for i, r in enumerate(last) if r.startswith("PRIMARY")]
        if len(primaries) == 1:
            return primaries[0]
        time.sleep(0.25)
    raise AssertionError(f"no single PRIMARY within {timeout}s; roles were {last}")


def write_rows(port: int, symbol: str, n: int) -> None:
    for i in range(n):
        reply = send_command(port, f"INSERT {symbol} EX bid {100_000 + i} 1 1")
        assert reply.startswith("OK"), f"the new primary refused a write: {reply!r}"


def rows_on(port: int, symbol: str) -> int:
    lines = raw_query(port, f"SELECT * FROM '{symbol}'.'EX' WHERE timestamp >= 0")
    if not lines or lines[0] != "OK":
        return 0
    return max(0, len(lines) - 2)


def assert_replicates(mgr: ClusterManager, new_primary: int, old_primary: int, symbol: str) -> None:
    """Rows written to the new primary reach the old one, which is the whole of following it."""
    write_rows(mgr.nodes[new_primary].tcp_port, symbol, 20)
    deadline = time.monotonic() + patience(30)
    seen = 0
    while time.monotonic() < deadline:
        seen = rows_on(mgr.nodes[old_primary].tcp_port, symbol)
        if seen >= 20:
            break
        time.sleep(0.5)
    role = role_of(mgr.nodes[old_primary].tcp_port)
    status = send_command(mgr.nodes[old_primary].tcp_port, "STATUS")
    assert seen >= 20, (
        f"the node that gave the role up holds {seen} of the 20 rows written to its successor "
        f"{patience(30):.0f} s later: it does not replicate. Its ROLE says {role!r} - the "
        f"successor's address, recorded - and its STATUS "
        f"{'has' if 'replication' in status.lower() else 'has no'} replication section")


def test_a_primary_demoted_with_no_successor_follows_the_one_elected_after_it(cluster2):
    mgr = cluster2
    old = primary_index(mgr, timeout=patience(60))
    new = 1 - old

    # The coordinator goes away for long enough to cost the primary its lease; nobody can be
    # elected meanwhile, so the primary demotes with no successor to follow.
    mgr.stop_etcd()
    deadline = time.monotonic() + patience(LEASE_TTL_SECONDS * 3)
    while time.monotonic() < deadline and role_of(mgr.nodes[old].tcp_port).startswith("PRIMARY"):
        time.sleep(0.25)
    assert not role_of(mgr.nodes[old].tcp_port).startswith("PRIMARY"), "the primary never stepped down"

    # The other node is the one elected: the old primary is held still until it is, so the order
    # of the two nodes' election waits cannot decide the test.
    mgr.pause_node(old)
    try:
        mgr.start_etcd()
        wait_for_role(mgr.nodes[new].tcp_port, "PRIMARY", timeout=patience(90))
    finally:
        mgr.resume_node(old)
    wait_for_role(mgr.nodes[old].tcp_port, "REPLICA", timeout=patience(30))

    assert_replicates(mgr, new, old, "FOLLOW-DEMOTED")


def test_a_handed_over_primary_replicates_from_its_successor(cluster2):
    mgr = cluster2
    old = primary_index(mgr, timeout=patience(60))
    new = 1 - old
    target = mgr.nodes[new]

    try:
        reply = send_command(mgr.nodes[old].tcp_port, f"FAILOVER {target.node_id}")
    except Exception as exc:  # noqa: BLE001 - the step-down can race the reply (#86)
        reply = f"(no reply: {exc})"
    assert not reply.strip().startswith("ERR"), f"handover refused: {reply!r}"
    wait_for_role(target.tcp_port, "PRIMARY", timeout=patience(30))
    wait_for_role(mgr.nodes[old].tcp_port, "REPLICA", timeout=patience(30))

    assert_replicates(mgr, new, old, "FOLLOW-HANDOVER")


def test_a_replica_of_an_unchanged_leader_does_not_restart_replication(cluster2):
    """The other half, and #104's: replication restarts for a leader this node does not follow, and
    for nothing else. A replica that started following the primary at startup says nothing more
    about whom it follows for as long as the primary stays."""
    mgr = cluster2
    primary = primary_index(mgr, timeout=patience(60))
    replica = mgr.nodes[1 - primary]
    offset = node_log_size(replica)
    write_rows(mgr.nodes[primary].tcp_port, "FOLLOW-STEADY", 5)
    time.sleep(patience(5))   # five monitor ticks
    log = node_log_since(replica, offset)
    assert "this replica followed" not in log, (
        "the replica restarted replication for the leader it already follows:\n" + log[-2000:])
