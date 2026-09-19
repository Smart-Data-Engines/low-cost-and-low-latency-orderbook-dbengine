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

# The per-peer queue ceiling `small_queue_proxied_mesh` runs with, and the shape of the writes the
# test behind it uses. A `MINSERT` of 1000 levels is one mesh frame of about 24 kB (an 88-byte
# `DeltaUpdate` plus 1000 levels of 24 bytes, behind a 38-byte header), so the volume needed arrives
# in a few hundred round trips instead of tens of thousands.
QUEUE_CEILING = 256 * 1024
LEVELS_PER_WRITE = 1000
# The cap is a statement about Linux, not about the engine: the queue cannot grow until the sender's
# own socket buffer is full, and that is `tcp_wmem`'s maximum - 4 MB on this machine. Twelve
# megabytes leaves room for it and for the ceiling above, and reaching the cap means the kernel
# absorbed more than that, which the failure message says rather than blaming the engine.
VOLUME_CAP_BYTES = 12 * 1024 * 1024


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
    """A constant checked against the table the engine builds the header from.

    If the header gains a column, `rows()` stops recognising it, every list here grows a phantom
    entry, and the assertions that count rows go quietly wrong in the lenient direction. Cheap to
    pin, and the alternative — dropping "the second line" by position — would break the moment a
    response gains or loses a line for any other reason.

    It used to look for the literal in `src/response_formatter.cpp`, which was right until #139
    replaced that literal with a loop over the column table and #152 deleted the leftover — so the
    string this test wanted stopped existing anywhere, and the test said so. **An anchor has to
    move to whatever decides the value**, and for the header that is now the spellings table in
    `src/query_columns.cpp`, read in its own order. That is the stronger of the two: the old check
    would have passed against a dead literal that agreed with a header nothing printed.
    """
    source = (pathlib.Path(__file__).resolve().parents[2]
              / "src" / "query_columns.cpp").read_text()
    assert len(source) > 500, "query_columns.cpp was not read, so this checked nothing"
    table = re.search(r"kSpellings\{\{(.*?)\}\};", source, re.S)
    assert table, "the spellings table is not where this test looks for it any more"
    names = re.findall(r'\{\s*"([^"]+)"', table.group(1))
    assert len(names) == 7, f"expected seven columns in the table, read {names}"
    assert "\t".join(names) == QUERY_HEADER, (
        "the column header this module drops is not the one the engine prints any more, so every "
        f"row list here has a phantom entry in it. The table says: {names}")


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


# There was a `narrow_proxied_mesh` fixture here, with a 4 kB receive buffer on the accepted
# sockets, added on the expectation that it would bring the engine's own send queue within reach in
# kilobytes instead of megabytes. **It does not, and the same session measured why**: TCP
# accumulates unsent data in the *sender's* buffer, which is the engine's socket and not anything a
# proxy can narrow, so the queue only starts growing after that buffer is full. The fixture was left
# behind with no users — a fixture built for a test nobody then wrote, which is the shape of a knob
# nothing turns. `small_queue_proxied_mesh` below is what the test that needed it uses instead: it
# shortens the engine's half of the wait, which is the half a flag can reach.


@pytest.fixture
def small_queue_proxied_mesh():
    """A proxied mesh whose per-peer send queue is capped at 256 kB instead of 64 MB.

    `--mm-max-peer-send-buffer` is the ceiling #69 added, and this is the flag its own field comment
    in `ServerConfig` recommends lowering in tests. It shortens the **engine's** half of what a
    "peer that stopped reading" test has to push; the kernel's half cannot be shortened from here
    and is measured by the test rather than assumed.
    """
    mgr = ClusterManager()
    mgr.extra_node_args = ["--mm-max-peer-send-buffer", str(QUEUE_CEILING),
                           # The records lag is recomputed by the anti-entropy pass and nowhere
                           # else, so its freshness *is* this interval - the same reason
                           # `reconciling_proxied_mesh` lowers it.
                           "--anti-entropy-interval-seconds", "2"]
    proxies: list = []
    try:
        _build_proxied_mesh(mgr, proxies)
        yield mgr, proxies
    finally:
        for proxy in proxies:
            proxy.stop()
        mgr.shutdown()


@pytest.fixture
def reconciling_proxied_mesh():
    """A proxied mesh whose anti-entropy pass runs every two seconds instead of every thirty.

    `ob_mm_replication_lag_records` is recomputed by that pass and nowhere else, so its freshness
    **is** the interval — and a test that waited out the production default would spend half a
    minute per assertion. The flag goes on `ClusterManager.extra_node_args` rather than into a
    start-method parameter so that a restarted node keeps it, which is the same reason
    `cluster_secret_file` lives there.
    """
    mgr = ClusterManager()
    mgr.extra_node_args = ["--anti-entropy-interval-seconds", "2"]
    proxies: list = []
    try:
        _build_proxied_mesh(mgr, proxies)
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
# saying so in STATUS/metrics**" — has a passing test now, and getting there took closing two
# roadmap items rather than writing an assertion.
#
# What it looked like from here first (#117): `ob_mm_peer_send_buf_bytes` is the engine's own
# queue, taken from `peer.send_buf.size()`, and it grows only once `send()` has returned `EAGAIN` —
# which needs the **sender's** socket buffer full first, and the engine sets no `SO_SNDBUF`, so
# that is `tcp_wmem`'s maximum: 4 MB on this machine. Narrowing the proxy's receive buffer does not
# shorten it, and finding that out was the useful part: TCP accumulates unsent data in the
# *sender's* buffer, which belongs to the engine and not to anything a test can reach.
#
# `ob_mm_replication_lag_bytes` was registered, described as exactly the signal this clause wants,
# and fed by nothing. Then #118 measured why it could not be fed: the only per-peer position the
# mesh has is a byte offset into that peer's **own** WAL, recorded once at handshake, so the
# subtraction produced this node's own WAL size. The replacement is in **records**, from the
# per-origin sequence vectors — the one position two nodes can compare — and the test below is the
# first thing in this battery able to assert it.


def test_a_partition_is_visible_in_the_records_lag_and_clears_on_heal(reconciling_proxied_mesh):
    """C3, second clause: a node that cannot replicate its writes says so in a metric.

    Three readings, and the first is the control that makes the other two mean anything. A gauge
    only ever observed as zero is what #117 was about, so a test that only checked the partitioned
    case would be pinning a number it never saw move; one that only checked the converged case
    would pass against a gauge hard-wired to zero.

    The pair is asserted throughout, never the lag alone. `compare_vectors()` reports a peer that
    has said nothing as holding nothing, on purpose — sending it everything is the safe direction
    for a repair — so `ob_mm_replication_lag_records` excludes those peers and
    `ob_mm_peers_position_unknown` counts them. Zero lag with a nonzero unknown count means "we do
    not know", which is a different answer from "converged" and would otherwise share its number
    (#84's defect, where a connection mid-handshake read as a peer that had fallen over).

    A partition rather than a disconnect, and that is load-bearing: `MeshProxy.partition()`
    buffers and stops reading without closing, so the engine still has the peer's **last** vector.
    A closed link would eventually leave the peer's position unknown, which is honest and is a
    different test.
    """
    mgr, proxies = reconciling_proxied_mesh
    writer, follower = mgr.nodes[0], mgr.nodes[1]

    def lag_pair(node) -> tuple:
        return (metric(node.metrics_port, "ob_mm_replication_lag_records"),
                metric(node.metrics_port, "ob_mm_peers_position_unknown"))

    # ── Reading one: converged. The control. ──
    assert raw(writer.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid 10000 2 1").startswith("OK")
    _wait_for_rows(follower.tcp_port, 1, timeout=patience(30))

    converged = _await_lag(writer, lag_pair, want_zero=True, timeout=patience(30))
    assert converged == (0.0, 0.0), (
        f"a converged mesh reports lag_records={converged[0]} with "
        f"{converged[1]} peer(s) of unknown position; the second number is why the first is not "
        f"enough on its own")

    # ── Reading two: partitioned, and the writes cannot cross. ──
    for proxy in proxies:
        proxy.partition()

    for price in (20_000, 20_001, 20_002, 20_003, 20_004, 20_005):
        assert raw(writer.tcp_port,
                   f"INSERT {SYMBOL} {EXCHANGE} bid {price} 2 1").startswith("OK")

    behind = _await_lag(writer, lag_pair, want_zero=False, timeout=patience(40))
    assert behind[0] > 0, (
        "the writer accepted six writes its peer cannot have and reported a lag of zero — which "
        "is the whole of requirement 1.4's second clause")
    assert behind[1] == 0.0, (
        f"the peer's position became unknown ({behind[1]} peers), so the lag above excluded it and "
        f"the assertion measured the wrong thing: partition() must buffer, not close")

    # ── Reading three: healed, and the number comes back down. ──
    for proxy in proxies:
        proxy.heal()
    _wait_for_rows(follower.tcp_port, 7, timeout=patience(60))

    healed = _await_lag(writer, lag_pair, want_zero=True, timeout=patience(60))
    assert healed == (0.0, 0.0), (
        f"the mesh converged by row content and the lag stayed at {healed[0]} with "
        f"{healed[1]} unknown — a gauge that goes up and never comes down is a gauge that gets "
        f"ignored")


def _await_lag(node, read, *, want_zero: bool, timeout: float) -> tuple:
    """Poll the (lag, unknown) pair until it is zero, or is not, or time runs out.

    Polling rather than sleeping a fixed interval: the pass that recomputes this runs every two
    seconds in this fixture and the node decides when, so a single sleep would be asserting on
    scheduling. Returns the last pair either way, so the assertion can report what it actually saw
    — which is the difference between "the lag stayed at 4" and "the lag was wrong".
    """
    deadline = time.monotonic() + timeout
    pair = read(node)
    while time.monotonic() < deadline:
        pair = read(node)
        if (pair[0] == 0.0) == want_zero:
            return pair
        time.sleep(0.5)
    return pair


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


class _Writer:
    """One connection, one reply line per write — because `raw()` cannot be used in a loop.

    `raw()` reads until the socket goes **quiet**, which costs its full 3 s read timeout per call.
    That is right for a one-off command and wrong for a few hundred: the first version of the test
    below spent 3 s per `MINSERT` and hit pytest's 120 s limit inside the control phase, before it
    had injected any fault at all.
    """

    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.sock.settimeout(30)
        self.buf = b""
        self._response()                  # the banner

    def _line(self) -> str:
        while b"\n" not in self.buf:
            chunk = self.sock.recv(1 << 16)
            if not chunk:
                raise AssertionError("the server closed the connection mid-write")
            self.buf += chunk
        line, _, self.buf = self.buf.partition(b"\n")
        return line.decode(errors="replace")

    def _response(self) -> str:
        """One response: its lines up to the blank one that ends it.

        A response here is **terminated by an empty line** - `format_ok()` returns `"OK\n\n"` and
        the banner is `"OK ob_tcp_server v0.1.0\n\n"`. Reading one line per command left that blank
        line in the stream, so the *next* write read it as its own reply and saw `''`. That is what
        the first run of this helper reported as "the write was refused".
        """
        lines = []
        while True:
            line = self._line()
            if line == "":
                return "\n".join(lines)
            lines.append(line)

    def minsert(self, first_price: int, levels: int = LEVELS_PER_WRITE) -> int:
        """One `MINSERT`. Returns the mesh bytes it is worth, by the format rather than by guess."""
        body = "\n".join(f"{first_price + i} {1 + (i % 97)} 1" for i in range(levels))
        self.sock.sendall(f"MINSERT {SYMBOL} {EXCHANGE} bid {levels}\n{body}\n".encode())
        reply = self._response()
        assert reply.startswith("OK"), f"the write was refused: {reply!r}"
        # 38-byte WALRecordV2 header, an 88-byte DeltaUpdate and 24 bytes per level.
        return 38 + 88 + levels * 24

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def test_a_peer_that_stops_reading_is_dropped_rather_than_buffered_for_ever(small_queue_proxied_mesh):
    """C4, the last clause: the queue ceiling #69 added has to **drop** the peer.

    Before that ceiling existed one unreachable peer grew the writer at about 113 MB/s with nothing
    to stop it, because `check_backpressure()` only ever ran inside the catch-up loop. The ceiling
    is a flag and the drop is a counter, and neither had a test that reached them: this is the clause
    C4 recorded as blocked, and what blocked it was **volume, not visibility**.

    **Why the volume is what it is, measured rather than argued.** `ob_mm_peer_send_buf_bytes` is
    the engine's own queue — `peer.send_buf.size()` — and it cannot grow until `send()` returns
    `EAGAIN`, which needs the **sender's** socket buffer full. The engine sets no `SO_SNDBUF`, so
    that is `tcp_wmem`'s maximum, 4 MB on this machine, and narrowing the proxy's receive buffer does
    not shorten it: TCP holds unsent data on the sender's side. The kernel's half of the wait is
    therefore out of a test's reach and the engine's half is not, which is why this runs with
    `--mm-max-peer-send-buffer` at 256 kB. The test writes until the drop counter moves, records how
    much that took, and fails against a cap that says what hitting it would mean.

    Three claims, and the first is the control: with the link **healthy**, the same shape of writes
    must leave the counter at zero, or "the partition caused the drop" would be a claim about volume.
    Then the drop. Then the recovery, because the ceiling's whole point is that dropping is *cheaper*
    than buffering — the peer reconnects and catches up.

    **Convergence is read from `ob_mm_replication_lag_records` here rather than by comparing rows**,
    and this is the one place in this module where that is the right trade: half a million rows means
    pulling tens of megabytes through two `SELECT`s. That gauge is the mesh's honest lag in records
    from the per-origin version vectors (#118), it is recomputed by the anti-entropy pass this
    fixture runs every two seconds, and zero means node B holds every record node A has. Convergence
    **by content** is asserted by the first test in this file, at three rows, where it costs nothing.
    """
    mgr, proxies = small_queue_proxied_mesh
    node_a, node_b = mgr.nodes[0], mgr.nodes[1]

    def dropped():
        return metric(node_a.metrics_port, "ob_mm_peer_dropped_slow_total")

    def queued():
        return metric(node_a.metrics_port, "ob_mm_peer_send_buf_bytes")

    assert dropped() == 0, "a peer was already dropped before this test wrote anything"

    writer = _Writer(node_a.tcp_port)
    try:
        # ── The control: a healthy link drains, so the queue never reaches the ceiling ─────────
        healthy_bytes = 0
        while healthy_bytes < QUEUE_CEILING * 4:
            healthy_bytes += writer.minsert(100_000 + healthy_bytes)
        assert dropped() == 0, (
            f"the peer was dropped over {healthy_bytes} bytes through a link that was draining, so "
            f"the ceiling is reacting to volume rather than to a peer that stopped reading")
        custom_metrics["healthy_link_queue_bytes"] = queued()

        assert _wait_for_rows(node_b.tcp_port, 1, patience(60)), (
            "nothing crossed the healthy link, so the fault below has no live path to break")

        # ── The fault: the peer stops reading, and the queue has nowhere to go ────────────────
        for proxy in proxies:
            proxy.partition()

        pushed = 0
        while pushed < VOLUME_CAP_BYTES and dropped() == 0:
            pushed += writer.minsert(500_000 + pushed)
    finally:
        writer.close()

    assert dropped() >= 1, (
        f"{pushed} bytes went to a peer that stopped reading and it was never dropped; the engine's "
        f"queue reads {queued()} against a {QUEUE_CEILING}-byte ceiling. If that queue is still near "
        f"zero the kernel absorbed more than the cap — a fact about `tcp_wmem` rather than about the "
        f"engine — and the cap is what needs raising")
    custom_metrics["bytes_to_drop_a_stalled_peer"] = pushed
    assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()

    # ── The recovery: dropping is cheaper than buffering only if the peer comes back ──────────
    for proxy in proxies:
        proxy.close_connections()
        proxy.heal()

    lag, unknown = -1.0, -1.0
    deadline = time.monotonic() + patience(240)
    while time.monotonic() < deadline:
        lag = metric(node_a.metrics_port, "ob_mm_replication_lag_records")
        unknown = metric(node_a.metrics_port, "ob_mm_peers_position_unknown")
        if lag == 0 and unknown == 0:
            break
        time.sleep(2.0)
    assert lag == 0 and unknown == 0, (
        f"after the drop and the heal, node A still reports {lag} record(s) of lag with {unknown} "
        f"peer(s) whose position it cannot tell. Dropping a peer is only the right answer if the "
        f"catch-up that follows is complete:\n{tail_node_log(node_b, 20)}")
    assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()
