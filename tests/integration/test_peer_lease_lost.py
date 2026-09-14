"""What a node does when the lease holding its mesh registration is gone — #132 and #133.

`PeerRegistry::lease_loop()` refreshes an etcd lease every TTL/3, and that lease is what keeps
`<prefix>mm_peers/<node_id>` alive. `register_self()` is the only writer of `lease_id_` and runs
exactly once, at start (`src/multi_master.cpp:351`) — so if the lease goes, the registration goes
with it and nothing puts either back.

**The premise is constructed rather than waited for.** The lease is revoked directly through etcd's
own API, which isolates one thing: stopping etcd would make the coordinator unreachable and every
other mechanism in the node react at the same time. The revoke needs no engine change, for the same
reason `redirect_peer()` needs none.

Two claims, and they are on opposite sides of the same measurement.

**#132 is pinned as it behaves today, not as it should behave.** The registration does not come
back, and the node keeps serving throughout. That is a defect with a decision attached — re-granting
a lease from this loop would also overwrite the entry `redirect_peer()` writes, which ten tests in
this battery depend on — so it is filed, and this test is what will fail on the day it is fixed.

**#133 is the fix.** Both lines this condition used to write at loop frequency now arrive once.
Measured before it: eleven `Lease refresh failed` and eleven `keepalive returned no TTL` in 33 s,
one of each per interval, unbounded. After: one of each. The control is in the same test — the
condition has to be *continuous* across the window, or "one line" is satisfied by a condition that
only happened once.
"""

from __future__ import annotations

import base64
import json
import time

import pytest

from conftest import ClusterManager, node_log_since, node_log_size, patience, send_command

pytestmark = pytest.mark.multi_master

# Long enough to cover at least four refresh intervals at the default 10 s TTL (TTL/3, floored at
# one second), so "one line" is a claim about a repeating condition rather than about a single tick.
WATCH_SECONDS = 14.0


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def _peer_key(mgr: ClusterManager, node_index: int) -> str:
    return f"/ob/mm_peers/{node_index + 1}"


def _registration(mgr: ClusterManager, node_index: int) -> dict | None:
    key = _b64(_peer_key(mgr, node_index))
    kvs = (mgr._etcd_call("/v3/kv/range", {"key": key}).get("kvs")) or []
    return kvs[0] if kvs else None


@pytest.fixture
def mesh_with_a_revoked_lease():
    """A two-node mesh in which node 1's registration lease has been revoked.

    Yields the manager, the node, and where its log had got to when the lease went — so every
    assertion below is about what this test caused rather than about the node's whole history.
    """
    mgr = ClusterManager()
    mgr.start_multi_master(node_count=2)
    try:
        # Converged first: "the key is there" has to be the premise, not a race with registration.
        deadline = time.monotonic() + patience(30)
        while time.monotonic() < deadline:
            if "connected" in send_command(mgr.nodes[0].tcp_port, "MM_PEERS"):
                break
            time.sleep(0.5)

        entry = _registration(mgr, 1)
        assert entry is not None, (
            f"no registration at {_peer_key(mgr, 1)} to revoke. The node must be up and registered "
            f"first; this is a one-shot registration, so there is nothing to take away before it")
        lease = entry.get("lease")
        assert lease and lease != "0", (
            f"{_peer_key(mgr, 1)} carries no lease ({lease!r}), so revoking one would prove "
            f"nothing about what keeps that key alive")

        offset = node_log_size(mgr.nodes[1])
        mgr._etcd_call("/v3/lease/revoke", {"ID": int(lease)})
        yield mgr, mgr.nodes[1], offset
    finally:
        mgr.shutdown()


def test_the_registration_does_not_come_back_and_the_node_keeps_serving(mesh_with_a_revoked_lease):
    """#132, pinned as it is. This test fails on the day the loop learns to re-register."""
    mgr, node, _ = mesh_with_a_revoked_lease

    absent = 0
    samples = 0
    deadline = time.monotonic() + patience(WATCH_SECONDS)
    while time.monotonic() < deadline:
        samples += 1
        if _registration(mgr, 1) is None:
            absent += 1
        assert send_command(node.tcp_port, "PING").strip() == "PONG", (
            "the node stopped answering after its registration lease was revoked. That would be a "
            "different and worse defect than #132, which is about a node that keeps serving while "
            "being absent from the registry")
        time.sleep(1.0)

    assert samples >= 4, f"only {samples} samples in the window; this measured almost nothing"
    assert absent == samples, (
        f"the registration came back: absent on {absent} of {samples} samples. If that is "
        f"deliberate, #132 is fixed and this test is the one to rewrite - it pins today's "
        f"behaviour so the fix cannot land unnoticed")


def test_a_lease_that_is_gone_for_good_is_reported_once_not_once_per_interval(
        mesh_with_a_revoked_lease):
    """#133. The condition is permanent; the log is not."""
    mgr, node, offset = mesh_with_a_revoked_lease

    # The control, and it has to come first: "one line" is a weak claim unless the condition was
    # continuous across the window. Four-plus refresh intervals with the key absent every time.
    checks = 0
    deadline = time.monotonic() + patience(WATCH_SECONDS)
    while time.monotonic() < deadline:
        checks += 1
        assert _registration(mgr, 1) is None, (
            "the registration reappeared mid-window, so this test no longer measures a permanent "
            "condition and its line count means nothing")
        time.sleep(1.0)
    assert checks >= 4

    written = node_log_since(node, offset)
    refusals = written.count("Lease refresh failed")
    gone = written.count("keepalive returned no TTL")

    # One of each, not one per interval. Before #133 these arrived at loop frequency for the life
    # of the process - measured at eleven of each in 33 s.
    assert refusals == 1, (
        f"the peer registry wrote {refusals} 'Lease refresh failed' lines for one permanent "
        f"condition across {checks} refresh intervals; #133 is one line per episode, and #95 is "
        f"why. Log:\n{written[-2000:]}")
    assert gone == 1, (
        f"the coordinator wrote {gone} 'keepalive returned no TTL' lines for one permanent "
        f"condition; the episode belongs to the client and covers every one of its four owners. "
        f"Log:\n{written[-2000:]}")

    # And the line that opened the episode says what it costs, because an operator reading one
    # WARN instead of hundreds needs it to carry the consequence.
    assert "nothing re-registers it" in written, (
        f"the WARN does not say that the registration is gone for good, which is the whole reason "
        f"one line is enough. Log:\n{written[-2000:]}")
