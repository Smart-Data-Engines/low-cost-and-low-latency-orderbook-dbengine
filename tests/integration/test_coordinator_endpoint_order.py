"""Which configured coordinator endpoint the peer registry talks to — #135.

`--coordinator-endpoints` is a **comma-separated list**; `--help` says so, the parser splits it, and
`CliArgs.SplitsCoordinatorEndpointsAndDropsEmptyOnes` pins three endpoints plus the empty ones it
drops. `CoordinatorClient::connect()` probes them **in order** and keeps the first that answers
`/v3/maintenance/status`; every lease call then goes through that one.

`PeerRegistry` did not. All three of its own etcd calls addressed `config_.endpoints[0]` directly:
the PUT that publishes this node's address, the range that reads its own key back (#132), and the
**topology watch** — which is how every peer is learned. Measured before the fix, with the dead
endpoint first: `connect()` and `grant_lease()` both succeeded through the live one, the PUT failed,
and the node ran unregistered for the life of the process while election, keepalive and failover all
worked. Zero `Lease refresh failed` lines, so #132's recovery could not see the condition either —
the lease was alive.

Two tests, and the second is the control rather than a second case: the same unreachable endpoint
in the **harmless** position, which passed before the fix and has to keep passing. Without it, a
harness that quietly stopped prepending anything would leave the first test green and meaningless.
"""

from __future__ import annotations

import time

import pytest

from conftest import ClusterManager, patience, send_command

pytestmark = pytest.mark.multi_master

# Port 1 refuses instantly rather than hanging, so a node configured with it pays a failed
# connection and nothing else (#111 measured what an address that black-holes SYNs costs instead).
UNREACHABLE = "http://127.0.0.1:1"


def _first_endpoint_of(node) -> str:
    """The first endpoint on the **running process's** command line.

    Read from `Popen.args` rather than from the `ClusterManager` attribute that put it there: the
    premise of both tests below is what the node was actually told, and an attribute is what the
    harness meant to tell it. A harness change that stopped prepending would leave the attribute
    set and this assertion failing, which is the direction that costs nothing.
    """
    argv = list(node.process.args)
    return argv[argv.index("--coordinator-endpoints") + 1].split(",")[0]


def _mesh_with(unreachable_position: str):
    mgr = ClusterManager()
    if unreachable_position == "before":
        mgr.extra_endpoints_before = [UNREACHABLE]
    else:
        mgr.extra_endpoints_after = [UNREACHABLE]
    mgr.start_multi_master(node_count=2)
    return mgr


def _both_register_and_find_each_other(mgr) -> None:
    registrations = {}
    peers = ""
    deadline = time.monotonic() + patience(45)
    while time.monotonic() < deadline:
        for index in (0, 1):
            if index not in registrations:
                entry = mgr.peer_registration(index)
                if entry is not None:
                    registrations[index] = entry
        if len(registrations) == 2:
            peers = send_command(mgr.nodes[0].tcp_port, "MM_PEERS")
            if "connected" in peers:
                return
        time.sleep(0.5)

    missing = [i + 1 for i in (0, 1) if i not in registrations]
    assert not missing, (
        f"node(s) {missing} never registered. Before #135 the PUT went to `endpoints[0]` while the "
        f"lease was granted through whichever endpoint answered, so a dead first entry meant the "
        f"node published no address at all — and its refresh kept succeeding, so nothing reported "
        f"it. Read `CoordinatorClient::endpoint()` if this fails")
    assert "connected" in peers, (
        f"both nodes registered and the mesh still did not form, so the topology watch is reading "
        f"a different endpoint from the one that answered — the third of #135's three call "
        f"sites:\n{peers}")


def test_the_registry_uses_the_endpoint_that_answered(mm_two_nodes_unreachable_first):
    """#135: an unreachable endpoint **first** must not cost the node its registration."""
    mgr = mm_two_nodes_unreachable_first
    for index in (0, 1):
        assert _first_endpoint_of(mgr.nodes[index]) == UNREACHABLE, (
            "the unreachable endpoint is not first on this node's command line, so this test would "
            "pass for the trivial reason")
    _both_register_and_find_each_other(mgr)


def test_the_same_endpoint_last_is_the_control(mm_two_nodes_unreachable_last):
    """The harmless order, which passed before #135's fix and has to keep passing.

    It is what says the assertions above are about **order**: the configuration differs from the
    first test by nothing except where the dead entry sits.
    """
    mgr = mm_two_nodes_unreachable_last
    for index in (0, 1):
        assert _first_endpoint_of(mgr.nodes[index]) != UNREACHABLE, (
            "the unreachable endpoint is first here too, so this is not the control it claims to be")
    _both_register_and_find_each_other(mgr)


@pytest.fixture
def mm_two_nodes_unreachable_first():
    mgr = _mesh_with("before")
    try:
        yield mgr
    finally:
        mgr.shutdown()


@pytest.fixture
def mm_two_nodes_unreachable_last():
    mgr = _mesh_with("after")
    try:
        yield mgr
    finally:
        mgr.shutdown()
