"""What a node does when the lease holding its mesh registration is gone — #132 and #133.

`PeerRegistry::lease_loop()` refreshes an etcd lease every TTL/3, and that lease is what keeps
`<prefix>mm_peers/<node_id>` alive. Until #132 the registration was written **once**, at start, so a
lease etcd had forgotten took the node out of the registry for the life of the process: measured,
the key was gone at once and still gone 22.5 s later while the node answered `PING`, with its row in
every peer's `MM_PEERS` carrying an empty address. The only recovery was a restart.

The two tests below are the two halves of that, and they need **different premises** — which is the
part worth reading.

**#132 needs a lease that is gone while etcd is reachable.** The lease is revoked through etcd's own
API, which isolates one thing: stopping etcd would make the coordinator unreachable and every other
mechanism in the node react at the same time. The key comes back within an interval, and the address
comes back with it.

**#133 needs a condition that is actually permanent, and after #132 a revoked lease is not one.**
The registration is rewritten, the next refresh succeeds, and the episode closes — so counting log
lines over a revoke would be counting a condition that happened once, which is the control failure
that test exists to avoid. An unreachable coordinator is the permanent version: the key's state is
`Unavailable` rather than `Absent`, so nothing is rewritten and nothing is attempted, and the refusal
repeats every interval for as long as it lasts. That also makes it the test for the branch #132
added to keep itself from flooding.
"""

from __future__ import annotations

import time

import pytest

from conftest import ClusterManager, node_log_since, node_log_size, patience, send_command

pytestmark = pytest.mark.multi_master

# Four refresh intervals and more at the default 10 s TTL (TTL/3, floored at one second), so "one
# line" is a claim about a repeating condition rather than about a single tick.
WATCH_SECONDS = 14.0


@pytest.fixture
def converged_mesh():
    """A two-node mesh whose peers have found each other, with node 1's log offset recorded.

    The offset is what makes every assertion below about this test rather than about the node's
    whole history.
    """
    mgr = ClusterManager()
    mgr.start_multi_master(node_count=2)
    try:
        deadline = time.monotonic() + patience(30)
        while time.monotonic() < deadline:
            if "connected" in send_command(mgr.nodes[0].tcp_port, "MM_PEERS"):
                break
            time.sleep(0.5)
        assert mgr.peer_registration(1) is not None, (
            "node 2 never registered, so neither test below has its premise")
        yield mgr, mgr.nodes[1], node_log_size(mgr.nodes[1])
    finally:
        mgr.shutdown()


def test_a_revoked_registration_comes_back_within_an_interval(converged_mesh):
    """#132. The key returns, the address returns with it, and the node never stops serving."""
    mgr, node, offset = converged_mesh
    mgr.revoke_peer_lease(1)

    # Gone first, because a test that only ever sees the key present proves nothing about a
    # recovery: it would pass against an engine that never lost it.
    assert mgr.peer_registration(1) is None, (
        "the registration was still there immediately after its lease was revoked, so this test "
        "would have measured a recovery from nothing")

    # Both halves are polled, and the wait is on "has it happened yet" rather than on a snapshot:
    # the key lands in etcd before the line lands in the log, so reading the log once at the moment
    # the key appears is a race the first version of this test lost.
    back_at = None
    said_so = False
    start = time.monotonic()
    deadline = start + patience(WATCH_SECONDS)
    while time.monotonic() < deadline:
        assert send_command(node.tcp_port, "PING").strip() == "PONG", (
            "the node stopped answering while re-registering, which would be a worse defect than "
            "the one #132 was about")
        if back_at is None and mgr.peer_registration(1) is not None:
            back_at = time.monotonic() - start
        if "has registered again" in node_log_since(node, offset):
            said_so = True
        if back_at is not None and said_so:
            break
        time.sleep(0.5)

    assert back_at is not None, (
        f"node 2's registration did not come back within {WATCH_SECONDS}s. Before #132 it never "
        f"came back at all and a restart was the only recovery; the rewrite fires on a key "
        f"confirmed **absent**, so read `PeerRegistry::read_self_key()` if this fails")
    assert said_so, (
        f"the key came back after {back_at:.1f}s and the node never said it wrote it, so "
        f"something else did. Log:\n{node_log_since(node, offset)[-1500:]}")

    # The second half of the defect, and the half an operator sees: before #132 the peer's row
    # carried an empty address for the life of the process, because the address a peer prints comes
    # from the registry and nothing put it back.
    peers = send_command(mgr.nodes[0].tcp_port, "MM_PEERS")
    rows = [r for r in peers.strip().splitlines()[1:] if r.startswith("2\t")]
    assert rows, f"node 1 no longer lists peer 2 at all:\n{peers}"
    assert rows[0].split("\t")[1], (
        f"peer 2's row still carries an empty address, so the entry came back without the one "
        f"field anything looking a node up needs:\n{peers}")


def test_an_unreachable_coordinator_is_reported_once_not_once_per_interval(converged_mesh):
    """#133, and the `Unavailable` branch #132 added to keep itself quiet."""
    mgr, node, offset = converged_mesh
    mgr.stop_etcd()

    # The control, and it comes first: "one line" is a weak claim unless the condition was
    # continuous. With the coordinator gone the refusal repeats every interval for the whole
    # window, and the node has to keep serving throughout.
    checks = 0
    deadline = time.monotonic() + patience(WATCH_SECONDS)
    while time.monotonic() < deadline:
        checks += 1
        assert send_command(node.tcp_port, "PING").strip() == "PONG", (
            "the node stopped answering while its coordinator was away; losing etcd costs writes "
            "and role changes, not processes (#54 stage B)")
        time.sleep(1.0)
    assert checks >= 4, f"only {checks} samples; this measured almost nothing"

    written = node_log_since(node, offset)
    refusals = written.count("Lease refresh failed")

    # One line for a condition that held across four-plus refresh intervals. Before #133 this
    # arrived once per interval from each of two components - measured at eleven of each in 33 s.
    assert refusals == 1, (
        f"the peer registry wrote {refusals} 'Lease refresh failed' lines for one condition that "
        f"held across {checks} seconds; #133 is one line per episode, and #95 is why. Log:\n"
        f"{written[-2000:]}")

    # And nothing was rewritten, because the key's state is unknown rather than absent. This is the
    # branch that keeps #132's recovery from becoming #133 in a new place: a grant attempt per
    # interval against an unreachable coordinator would be two more WARN lines per interval.
    assert "has registered again" not in written, (
        f"the node rewrote its registration while etcd was unreachable, which it cannot have "
        f"confirmed was missing. Log:\n{written[-2000:]}")

    # What that branch says, it says at DEBUG, and asserting on it here would be asserting on a
    # line this node does not write at its default level - the mistake #115's mutation table paid
    # for from the other side. The branch is **quiet by design**: the loud line above already tells
    # an operator the entry will be rewritten once the key is confirmed gone, and a second line per
    # interval saying "still cannot tell" is exactly #133. What is observable is the absence of the
    # rewrite, which is asserted above.
    assert refusals == 1

    mgr.start_etcd()
