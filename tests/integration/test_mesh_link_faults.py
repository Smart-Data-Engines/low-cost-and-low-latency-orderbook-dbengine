"""#54 stage C: what a mesh node does when the link to a peer breaks under it.

The link is broken by a `MeshProxy` inserted between the two nodes, which needs **no change in the
engine**: `PeerRegistry::register_self()` runs once at start, and overwriting the address it
published makes peers dial the proxy instead (`ClusterManager.redirect_peer`).

**Getting the proxy into the path is most of the work, and the reasons are all about ordering.**

- Nodes connect **directly** within milliseconds of registering, and overwriting the key does not
  move a connection that already exists. So the redirect has to land in the window between one
  node registering and the other discovering it — which is workable only because #96 measured the
  etcd topology watch as orders of magnitude slower than a loopback connect.
- A **restart** undoes a redirect, because registration is part of starting. Nothing here restarts
  a node for that reason.
- Both addresses must be redirected, not one. The mesh resolves a double link by keeping the one
  the **lower-numbered** node dialled (`src/multi_master.cpp:1023`), so redirecting only the
  higher-numbered node leaves the surviving link direct. With both redirected, whichever link wins
  is proxied.

Because all three of those are timing arguments rather than guarantees, the fixture **asserts that
the proxies carried bytes** before any test relies on them. A stage C test that ran against a
direct link would pass while measuring nothing, which is the failure mode this whole module exists
to avoid.
"""

from __future__ import annotations

import pathlib
import re
import socket
import time

import pytest

from conftest import ClusterManager, patience, tail_node_log
from mesh_proxy import MeshProxy

pytestmark = pytest.mark.multi_master

custom_metrics: dict = {}

SYMBOL = "MESH"
EXCHANGE = "PROXY"


def raw(port: int, payload: str, settle: float = 0.25) -> str:
    """One command on a fresh connection, read until the socket goes quiet."""
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.sendall((payload if payload.endswith("\n") else payload + "\n").encode())
        time.sleep(settle)
        sock.settimeout(3.0)
        buf = b""
        try:
            while True:
                chunk = sock.recv(1 << 16)
                if not chunk:
                    break
                buf += chunk
        except socket.timeout:
            pass
    return buf.decode(errors="replace")


def metric(port: int, name: str) -> float:
    """One gauge or counter from /metrics, by name, ignoring its label set.

    Labels matter here: the exposition is `name{node_role="..."} value`, and a lookup by bare name
    plus a space finds nothing — which reads as zero and once made a scraper announce that a
    snapshot had never been sent (pitfall 66).
    """
    import urllib.request
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        body = response.read().decode()
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        key, _, value = line.rpartition(" ")
        if key.split("{")[0] == name:
            return float(value)
    raise AssertionError(f"{name} is not in /metrics at all, which is not the same as zero")


# The column header `SELECT` prints between its `OK` and its rows. Dropped by name rather than by
# position, and it matters more than it looks: the first version of `rows()` stripped only the `OK`
# line, so a node holding **nothing** returned a one-element list containing this header — which is
# truthy, so `assert rows(node_b)` ("the mesh replicated something") could never fail, and every
# row count in this file was one too many. Pitfall 110's shape: an assertion satisfied for a reason
# that has nothing to do with what it claims.
QUERY_HEADER = "timestamp_ns\tprice\tquantity\torder_count\tside\tlevel\tsequence_number"


def rows(port: int) -> list:
    """Every stored row for the test symbol, as text lines, so comparison is by content."""
    reply = raw(port, f"SELECT * FROM '{SYMBOL}'.'{EXCHANGE}'")
    return [line for line in reply.splitlines()
            if line and not line.startswith("OK") and line != QUERY_HEADER]


def test_the_query_header_this_module_drops_is_the_one_the_engine_prints():
    """A constant copied out of `src/response_formatter.cpp`, so it is checked against it.

    If the header gains a column, `rows()` stops recognising it, every list here grows a phantom
    entry, and the assertions that count rows go quietly wrong in the lenient direction. Cheap to
    pin, and the alternative — dropping "the second line" by position — would break the moment a
    response gains or loses a line for any other reason.
    """
    source = (pathlib.Path(__file__).resolve().parents[2]
              / "src" / "response_formatter.cpp").read_text()
    assert len(source) > 1000, "response_formatter.cpp was not read, so this checked nothing"
    # Compared in the source's own spelling: a tab is `\t` there, two characters, and the first
    # version of this check compared a string with real tabs against a file that has none. Same
    # class as the phrase that does not exist contiguously because of implicit concatenation
    # (pitfall 238) - the source is not the runtime value, and a check has to say which it wants.
    as_written = QUERY_HEADER.replace("\t", "\\t")
    joined = re.sub(r'"\s*"', "", source)
    assert as_written in joined, (
        "the column header this module drops is not the one the engine prints any more, so every "
        f"row list here has a phantom entry in it. Looking for: {as_written!r}")


def _build_proxied_mesh(mgr: ClusterManager, proxies: list, recv_buffer=None):
    """Start two mm nodes with a proxy in front of each, in the only order that works.

    Start one node, proxy and redirect it **before** the second exists, then add the second and
    redirect it too. Starting both first loses the race to a direct connection, and nothing can
    move a connection once it is up.
    """
    mgr.start_multi_master(1)
    first = MeshProxy("127.0.0.1", mgr.nodes[0].mm_replication_port, name="to-node-1",
                      recv_buffer=recv_buffer)
    mgr.redirect_peer(0, first.start())
    proxies.append(first)

    mgr.add_multi_master_node(timeout=patience(20))
    second = MeshProxy("127.0.0.1", mgr.nodes[1].mm_replication_port, name="to-node-2",
                       recv_buffer=recv_buffer)
    mgr.redirect_peer(1, second.start())
    proxies.append(second)

    mgr.wait_for_mm_mesh(timeout=patience(45))

    # The precondition, asserted rather than assumed: if the surviving link went direct, every
    # fault below would be injected into a path carrying nothing.
    deadline = time.monotonic() + patience(20)
    while time.monotonic() < deadline:
        if any(sum(p.forwarded()) > 0 for p in proxies):
            return
        time.sleep(0.25)
    carried = [p.forwarded() for p in proxies]
    raise AssertionError(
        f"the mesh converged without either proxy carrying a byte, so the surviving link is "
        f"direct and nothing would be testing the engine: {carried}")


@pytest.fixture
def narrow_proxied_mesh():
    """A proxied mesh whose accepted sockets have a 4 kB receive buffer.

    So a partition reaches the engine's own send queue in kilobytes instead of the 2.6 MB the
    default buffers absorb. Separate from `proxied_mesh` rather than a parameter on it: the
    convergence test must run through ordinary buffers, because a narrowed window changes what it
    is measuring.
    """
    mgr = ClusterManager()
    proxies: list = []
    try:
        _build_proxied_mesh(mgr, proxies, recv_buffer=4096)
        yield mgr, proxies
    finally:
        for proxy in proxies:
            proxy.stop()
        mgr.shutdown()


@pytest.fixture
def proxied_mesh():
    """Two multi-master nodes whose link runs through proxies, with that established, not hoped for.

    The order is the design: start one node, put a proxy in front of it and redirect it **before**
    the second node exists, then add the second and redirect it too. Starting both first loses the
    race to a direct connection, and there is no way to move a connection once it is up.
    """
    mgr = ClusterManager()
    proxies: list = []
    try:
        _build_proxied_mesh(mgr, proxies)
        yield mgr, proxies
    finally:
        for proxy in proxies:
            proxy.stop()
        mgr.shutdown()


def test_a_partitioned_peer_is_not_replicated_to_and_the_mesh_converges_on_heal(proxied_mesh):
    """C3, first clause: the writes do not cross, and after `heal()` the mesh converges.

    Renamed from "...and the node says so", which it does not check: that clause has no passing
    test and the comment at the foot of this file says why (#117). A test name promising a
    guarantee its body does not assert is the defect this repository found by mutation once
    already — the mutation survived, and the answer was to fix the name.

    Converged **by row content**, not by row count, and that distinction is requirement 1.4's:
    a count survives a row replaced by a different row, and a mesh that agreed on how many rows
    it had while disagreeing on what they were would pass a count-based test for ever.
    """
    mgr, proxies = proxied_mesh
    node_a, node_b = mgr.nodes[0], mgr.nodes[1]

    assert raw(node_a.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid 100 1 1").startswith("OK")
    deadline = time.monotonic() + patience(30)
    while time.monotonic() < deadline and not rows(node_b.tcp_port):
        time.sleep(0.25)
    assert rows(node_b.tcp_port), "the mesh never replicated the first write, so nothing is set up"

    for proxy in proxies:
        proxy.partition()
    before = rows(node_b.tcp_port)

    for price in (200, 300, 400):
        assert raw(node_a.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid {price} 2 1").startswith("OK")

    # The writes do not cross. Watched for long enough that a link still pumping would show it.
    settle = time.monotonic() + patience(6)
    while time.monotonic() < settle:
        assert rows(node_b.tcp_port) == before, (
            "a write crossed a partitioned link, so the fault was not injected into the live path")
        time.sleep(0.5)

    # What this volume can NOT show, stated because the first version of this test asserted it and
    # was wrong: `ob_mm_peer_send_buf_bytes` is **the engine's own queue**, which stays empty while
    # the kernel socket buffer absorbs the writes - measured at 2.6 MB for a loopback pair in #93.
    # Three small rows produce a queue of 0, so at this volume a partitioned peer is
    # indistinguishable from a healthy one in the metrics. The gauge whose name is exactly the
    # signal requirement 1.4 wants, `ob_mm_replication_lag_bytes`, is registered and fed by
    # nothing. Both halves are roadmap #117; the test below this one shows the volume at which the
    # engine can see it today.
    custom_metrics["small_partition_send_buf_bytes"] = metric(
        node_a.metrics_port, "ob_mm_peer_send_buf_bytes")

    for proxy in proxies:
        proxy.heal()

    deadline = time.monotonic() + patience(60)
    while time.monotonic() < deadline:
        if sorted(rows(node_b.tcp_port)) == sorted(rows(node_a.tcp_port)):
            break
        time.sleep(0.5)
    a_rows, b_rows = sorted(rows(node_a.tcp_port)), sorted(rows(node_b.tcp_port))
    assert a_rows == b_rows, (
        "the mesh did not converge by content after the partition lifted.\n"
        f"node A has {len(a_rows)} rows, node B has {len(b_rows)}\n"
        f"only on A: {[r for r in a_rows if r not in b_rows]}\n"
        f"only on B: {[r for r in b_rows if r not in a_rows]}")
    # Exactly four, not "at least": the phantom header made the earlier `>= 4` satisfiable by
    # three real rows, and an exact count is also what catches a record applied twice - which is
    # the append-only storage failure #101's dedup exists to prevent.
    assert len(a_rows) == 4, f"expected exactly the four writes, got {len(a_rows)}: {a_rows}"
    assert len(set(a_rows)) == len(a_rows), f"a row was stored twice: {a_rows}"


# The second clause of requirement 1.4 — "no node accepts writes it cannot replicate **without
# saying so in STATUS/metrics**" — has **no passing test here, and that is roadmap #117 rather
# than an omission.** Measured while trying to write one:
#
#   * `ob_mm_peer_send_buf_bytes` is the engine's own queue, set from `peer.send_buf.size()`. It
#     grows only once `send()` has returned `EAGAIN`, which needs the **sender's** socket buffer
#     full first — and the engine sets no `SO_SNDBUF`, so that is `tcp_wmem`'s maximum: **4 MB** on
#     this machine. Up to 4 MB of writes a node has accepted and cannot replicate are reported by
#     nothing.
#   * Narrowing the proxy's receive buffer does not help, and finding that out was the useful part:
#     TCP accumulates the unsent data in the **sender's** buffer, which belongs to the engine.
#   * `ob_mm_replication_lag_bytes` — registered, described as "Replication lag in bytes (max
#     across peers)", which is exactly the signal this clause wants — is **fed by nothing**. It is
#     one of five such metrics; `scripts/check_metrics.py` now checks that direction too.
#
# The convergence test above records `small_partition_send_buf_bytes` so the number is in the run's
# report rather than only in this comment. A test asserting the defect would have to be an xfail,
# and this battery's zero-xfail property is worth more than pinning a gauge that reads zero for two
# different reasons.


def _wait_for_rows(port: int, count: int, timeout: float) -> list:
    deadline = time.monotonic() + timeout
    seen: list = []
    while time.monotonic() < deadline:
        seen = rows(port)
        if len(seen) >= count:
            return seen
        time.sleep(0.25)
    return seen


def test_a_frame_cut_in_half_is_not_applied_in_part(proxied_mesh):
    """C4: the link dies mid-frame, and the peer must store nothing from the half it got.

    The budget is **20 bytes**, chosen against the format rather than for looking small: a mesh
    frame is a `WALRecordV2` header of 38 bytes followed by its payload, so 20 cannot complete even
    the header. `partition()` could not produce this — it stops at whatever boundary a `recv`
    happened to land on, which is why the proxy has a byte budget at all.

    The pairing is what makes it a *cut* rather than a stall: `stall_after()` puts the receiver in
    the middle of a frame, `close_connections()` guarantees the rest is never coming.
    """
    mgr, proxies = proxied_mesh
    node_a, node_b = mgr.nodes[0], mgr.nodes[1]

    assert raw(node_a.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid 100 1 1").startswith("OK")
    settled = _wait_for_rows(node_b.tcp_port, 1, patience(30))
    assert settled, "the mesh never replicated the first write, so nothing is set up"
    before = sorted(settled)

    for proxy in proxies:
        proxy.stall_after(20)
    assert raw(node_a.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid 200 2 1").startswith("OK")
    # The prefix has to actually be delivered, or this test is a partition with extra steps.
    deadline = time.monotonic() + patience(15)
    while time.monotonic() < deadline and not any(p.held_bytes() for p in proxies):
        time.sleep(0.2)
    held = sum(p.held_bytes() for p in proxies)
    custom_metrics["bytes_withheld_after_the_cut"] = held
    assert held > 0, (
        "the budget let everything through, so no frame was cut and this test measured nothing")

    for proxy in proxies:
        proxy.close_connections()

    # The peer may legitimately gain the row later, by reconnecting and catching up - that is the
    # repair path and it is the next test's subject. What it may never have is a row assembled from
    # a partial frame, so the assertion is on *content*: whatever node B holds must be a subset of
    # what node A holds, at every moment.
    deadline = time.monotonic() + patience(20)
    while time.monotonic() < deadline:
        a_rows, b_rows = sorted(rows(node_a.tcp_port)), sorted(rows(node_b.tcp_port))
        assert all(row in a_rows for row in b_rows), (
            "node B holds a row node A never wrote, which is a frame applied in part.\n"
            f"only on B: {[r for r in b_rows if r not in a_rows]}")
        assert len(b_rows) >= len(before), "node B lost a row it already had"
        time.sleep(0.5)
    assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()


def test_a_reconnect_redelivers_records_and_none_are_stored_twice(proxied_mesh):
    """C4: over-delivery is the normal repair path, and dedup must not lose what arrived once.

    The sequence produces the overlap rather than simulating it: two writes cross live, the link is
    partitioned for two more, and then it is cut. The peer reconnects and catch-up streams from the
    position it last acknowledged — which covers records it already has. That is #61's design and
    #101's dedup, and the failure this guards against is the one measured before dedup existed:
    four outage cycles stored 25 rows where 9 were written, because storage is append-only and
    re-applying a record appends its rows again.
    """
    mgr, proxies = proxied_mesh
    node_a, node_b = mgr.nodes[0], mgr.nodes[1]

    for price in (100, 110):
        assert raw(node_a.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid {price} 1 1").startswith("OK")
    assert _wait_for_rows(node_b.tcp_port, 2, patience(30)), "the live writes never crossed"

    for proxy in proxies:
        proxy.partition()
    for price in (120, 130):
        assert raw(node_a.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid {price} 1 1").startswith("OK")

    # Cut rather than heal: a heal would deliver the withheld bytes on the same connection, and
    # the redelivery this test is about comes from a *reconnect*.
    for proxy in proxies:
        proxy.close_connections()
        proxy.heal()

    a_rows = sorted(rows(node_a.tcp_port))
    deadline = time.monotonic() + patience(90)
    while time.monotonic() < deadline:
        if sorted(rows(node_b.tcp_port)) == a_rows:
            break
        time.sleep(0.5)

    b_rows = sorted(rows(node_b.tcp_port))
    assert b_rows == a_rows, (
        "the mesh did not converge after the reconnect.\n"
        f"A has {len(a_rows)}, B has {len(b_rows)}\n"
        f"only on A: {[r for r in a_rows if r not in b_rows]}\n"
        f"only on B: {[r for r in b_rows if r not in a_rows]}")
    assert len(b_rows) == len(set(b_rows)), (
        f"a record was stored twice, so dedup did not hold across the reconnect: "
        f"{len(b_rows)} rows, {len(set(b_rows))} distinct")
    custom_metrics["rows_after_reconnect"] = len(b_rows)
    assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()
