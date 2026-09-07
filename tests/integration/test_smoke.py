"""Smoke tests: the basics have to work before anything else is worth checking.

PING, an INSERT/query round trip, STATUS fields and ROLE. If these fail, the
remaining categories will fail too and their output is noise.
"""
from __future__ import annotations

import socket

import pytest

from orderbook_engine import OrderbookEngine
import pathlib
import re

pytestmark = pytest.mark.smoke


def raw_command(port: int, command: str, timeout: float = 5.0) -> str:
    """Send one command over a bare socket and return the raw reply.

    Used where the point is the wire protocol itself rather than the client:
    the Python client hides banners, framing and error strings, which is exactly
    what some of these tests need to see.
    """
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        banner = sock.recv(4096)  # "OK ob_tcp_server v0.1.0\n"
        assert banner, "server sent no banner"
        sock.sendall(command.encode())
        chunks = []
        while True:
            try:
                data = sock.recv(4096)
            except socket.timeout:
                break
            if not data:
                break
            chunks.append(data)
            joined = b"".join(chunks)
            # Responses end with a blank line, or are a single line.
            if joined.endswith(b"\n\n") or joined.count(b"\n") >= 1 and b"ERR" in joined:
                break
            if joined.endswith(b"\n") and not joined.startswith(b"OK\n"):
                break
        return b"".join(chunks).decode(errors="replace")


def test_banner_on_connect(cluster):
    """A fresh connection is greeted, so a client can identify the server."""
    node = cluster.primary()
    with socket.create_connection(("127.0.0.1", node.tcp_port), timeout=5) as sock:
        sock.settimeout(5)
        banner = sock.recv(4096).decode(errors="replace")
    assert banner.startswith("OK ob_tcp_server"), f"unexpected banner: {banner!r}"


def test_ping_returns_pong(primary_client: OrderbookEngine):
    assert primary_client.ping().strip() == "PONG"


def test_insert_flush_query_round_trip(primary_client: OrderbookEngine):
    """The core promise of the engine: what goes in comes back out."""
    prices = [6_500_000, 6_499_000, 6_498_000]
    qtys = [150, 200, 250]

    primary_client.insert("SMOKE-BTC", "BINANCE", "bid", prices, qtys)
    primary_client.flush()

    rows = primary_client.query_all("SMOKE-BTC", "BINANCE")
    got_prices = sorted(r.price for r in rows)

    assert len(rows) == len(prices), f"expected {len(prices)} rows, got {len(rows)}"
    assert got_prices == sorted(prices)


def test_query_returns_both_sides(primary_client: OrderbookEngine):
    primary_client.insert("SMOKE-SIDES", "BINANCE", "bid", [100_000], [10])
    primary_client.insert("SMOKE-SIDES", "BINANCE", "ask", [101_000], [20])
    primary_client.flush()

    rows = primary_client.query_all("SMOKE-SIDES", "BINANCE")
    sides = {r.side for r in rows}

    assert len(rows) == 2, f"expected one row per side, got {len(rows)}"
    assert len(sides) == 2, f"both sides should be stored, got sides={sides}"


def test_repeated_flush_does_not_duplicate_rows(primary_client: OrderbookEngine):
    """Flushing between reads must not multiply the rows.

    This is the assertion that caught the flush race: a segment registered twice in
    the query index returned every row in it twice. Count the rows — comparing
    sets of prices would have passed on the duplicated data.
    """
    prices = [7_100_000, 7_099_000]
    primary_client.insert("SMOKE-DUP", "BINANCE", "bid", prices, [10, 20])

    for round_no in range(4):
        primary_client.flush()
        rows = primary_client.query_all("SMOKE-DUP", "BINANCE")
        assert len(rows) == len(prices), (
            f"round {round_no}: expected {len(prices)} rows, got {len(rows)}")

    # No default on the get: with one, this assertion passed while the field was
    # missing from the client's STATUS parsing entirely, which is no assertion at all.
    status = primary_client.status()
    assert "segment_merge_refused" in status, (
        f"STATUS does not report segment_merge_refused; keys: {sorted(status)}")
    assert status["segment_merge_refused"] == 0, (
        "the server refused a duplicate segment merge, so two flush paths raced: "
        f"segment_merge_refused={status['segment_merge_refused']}")


def test_status_reports_counters(primary_client: OrderbookEngine):
    """STATUS has to answer with counters, not just not fail."""
    primary_client.insert("SMOKE-STATUS", "BINANCE", "bid", [123_000], [7])
    primary_client.flush()

    status = primary_client.status()

    assert status.get("mode") == "tcp"
    for field in ("sessions", "queries", "inserts"):
        assert field in status, f"STATUS is missing {field}: {status}"
        assert isinstance(status[field], int)
    assert status["inserts"] >= 1, "an insert was just performed"


def test_role_reports_primary_and_replica(cluster):
    """Exactly one node holds the primary role; the other follows it."""
    primary_reply = raw_command(cluster.primary().tcp_port, "ROLE\n")
    replica_reply = raw_command(cluster.replica().tcp_port, "ROLE\n")

    assert "PRIMARY" in primary_reply.upper(), f"got {primary_reply!r}"
    assert "REPLICA" in replica_reply.upper(), f"got {replica_reply!r}"


def test_exactly_one_primary(cluster):
    """Two primaries mean split brain, which is worse than none."""
    roles = [raw_command(n.tcp_port, "ROLE\n").upper() for n in cluster.nodes]
    primaries = [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]
    assert len(primaries) == 1, f"expected exactly one primary, roles={roles}"


def test_flush_is_idempotent(primary_client: OrderbookEngine):
    """Flushing twice must not duplicate rows or fail."""
    primary_client.insert("SMOKE-FLUSH", "BINANCE", "bid", [999_000], [1])
    primary_client.flush()
    before = len(primary_client.query_all("SMOKE-FLUSH", "BINANCE"))

    primary_client.flush()
    after = len(primary_client.query_all("SMOKE-FLUSH", "BINANCE"))

    assert before == after == 1, f"row count changed on second flush: {before} -> {after}"


def test_no_module_allocates_its_own_port() -> None:
    """Static, over the integration modules. The fix existed and five modules did not use it.

    `conftest.free_port()` remembers every port it has handed out, because binding to port 0 tells
    you a port that was free *at that instant* and the socket is closed before the caller can use
    it - so two calls in quick succession can return the same number. Its docstring has described
    that failure, with the engine's exact message, since the day it was written for
    `ClusterManager`.

    Five modules kept a naive two-line copy anyway, and on 7 September 2026 one of them produced
    the failure verbatim: `test_auth.py` handed the same ephemeral port to `--port` and
    `--metrics-port`, the metrics server took it, the client port could not, and the node aborted -
    reported as `node exited with -6`, which reads like a crash rather than a port conflict.

    Same shape as `test_no_module_builds_its_own_server_path` above, and the same reason for being
    a guard over the source: a module with its own copy fails somewhere else, rarely, on a loaded
    runner.
    """
    # Anchored to a real call - `<name>.bind((..., 0))` as a statement - rather than to the text
    # `.bind((` anywhere on the line. The first version flagged this test's own matcher, which is
    # the ordinary hazard of a static test that has to describe what it forbids (the sibling guard
    # above hit the same thing with its docstring).
    port_zero_bind = re.compile(r"^\s*\w+\.bind\(\([^)]*,\s*0\)\)")
    here = pathlib.Path(__file__).resolve().parent
    offenders = []
    for module in sorted(here.glob("test_*.py")):
        lines = module.read_text(encoding="utf-8").splitlines()
        for number, line in enumerate(lines, start=1):
            if port_zero_bind.match(line):
                offenders.append(f"{module.name}:{number}")
    assert not offenders, (
        "these lines allocate a port by binding to zero instead of calling conftest.free_port(), "
        "so they can hand out a port this process has already given to a node: "
        f"{offenders}"
    )

    # The pair: the shared allocator has to still be the thing that remembers, or this guard is
    # forcing every module to call a function that is no better than the copies it replaced.
    conftest = (here / "conftest.py").read_text(encoding="utf-8")
    assert "_ports_handed_out" in conftest, (
        "conftest.free_port() no longer tracks what it has handed out, so calling it buys nothing"
    )


def test_the_startup_budget_is_scaled_in_one_place() -> None:
    """Static, over `conftest.py`. A scaling rule applied by hand is applied once too few.

    `patience()` triples every timeout under a sanitizer, because every wait in this suite was
    chosen against an uninstrumented build. Five of the six `_wait_for_node()` call sites wrapped
    their budget in it; the sixth - the multi-master fixture, which starts three nodes and so has
    the most to lose - passed a flat 20. Under `sanitizers-integration (tsan)` that fixture had a
    20-second budget where every other had 45, and the job failed with "node-2 not ready after 20s"
    on a process that was alive with its mesh already up.

    The scaling now happens inside `_wait_for_node()`, so no call site can forget it. This is the
    guard for that: the function has to scale, and no call site may scale again, because double
    scaling reads as a budget somebody measured.
    """
    conftest = pathlib.Path(__file__).resolve().parent / "conftest.py"
    lines = conftest.read_text(encoding="utf-8").splitlines()

    body_start = next(
        (n for n, line in enumerate(lines) if "def _wait_for_node(" in line), None
    )
    assert body_start is not None, "conftest has no _wait_for_node; this guard checks nothing"
    body = "\n".join(lines[body_start:body_start + 25])
    assert "timeout = patience(timeout)" in body, (
        "_wait_for_node no longer scales its own budget, so every call site is back to remembering "
        "to do it - which is the defect this guard exists for"
    )

    doubled = [
        f"conftest.py:{n}"
        for n, line in enumerate(lines, start=1)
        if "_wait_for_node(" in line and "patience(" in line
    ]
    assert not doubled, (
        f"these call sites scale a budget that _wait_for_node already scales: {doubled}. The number "
        "at a call site should be the one that was measured on an uninstrumented build."
    )


def test_no_module_builds_its_own_server_path() -> None:
    """Static, over the integration modules. The rule, not the four instances of it.

    Four modules had grown their own `os.path.join(REPO, "build", "ob_tcp_server")` and none of them
    honoured `OB_SERVER_BINARY`. They start their own nodes rather than using `ClusterManager` -
    simultaneous starts, crash recovery, multi-master stats, all things the shared fixture
    deliberately serialises or shares - so each one grew the path and none grew the override.

    The CI failure that exposed it was the cheap part. The expensive part: running any of them
    against a sanitizer tree silently tested the **plain** build, because a stale
    `build/ob_tcp_server` was there to be found. `test_mm_stats.py` is one of the three modules
    `sanitizers-integration (tsan)` had been running since the job was created, so part of a
    required check had been measuring an uninstrumented binary all along. A check that quietly
    measures the wrong artefact is worse than no check, because it is believed.

    So the guard is the source, and it has to be: the symptom of a module ignoring the override is a
    green run, which no assertion inside that run can see.
    """
    # Matched on the *shape* of the defect - an assignment naming the binary - rather than on any
    # mention of "ob_tcp_server". The first version flagged this test's own docstring, which is the
    # ordinary hazard of a static test that has to describe what it forbids.
    assignment = re.compile(r"^\s*(SERVER|SERVER_BINARY)\s*=")
    here = pathlib.Path(__file__).resolve().parent
    offenders = []
    for module in sorted(here.glob("test_*.py")):
        lines = module.read_text(encoding="utf-8").splitlines()
        for number, line in enumerate(lines, start=1):
            if not assignment.match(line):
                continue
            # The join can span lines - one module wrote it over three - so look at the statement,
            # not the line.
            statement = " ".join(lines[number - 1:number + 3])
            if "ob_tcp_server" in statement and "server_binary_path" not in statement:
                offenders.append(f"{module.name}:{number}")
    assert not offenders, (
        "these lines build a path to the server instead of calling "
        "conftest.server_binary_path(), so they ignore OB_SERVER_BINARY and will silently test the "
        f"wrong build: {offenders}"
    )

    # The same rule for the C++ client harness, added after it went wrong the same way: this module
    # and test_auth.py each derived `ob_integration_test` themselves, and the second copy found
    # nothing when the suite ran against a TSan tree. Any line naming that binary must go through
    # conftest.cpp_client_binary_path().
    client_offenders = []
    for module in sorted(here.glob("test_*.py")):
        for number, line in enumerate(module.read_text(encoding="utf-8").splitlines(), start=1):
            if "ob_integration_test" not in line:
                continue
            if "cpp_client_binary_path" in line:
                continue
            # Prose is fine; a path expression is not.
            if "/" in line or "join" in line or "Path(" in line:
                client_offenders.append(f"{module.name}:{number}")
    assert not client_offenders, (
        "these lines build a path to ob_integration_test instead of calling "
        "conftest.cpp_client_binary_path(), so they ignore OB_SERVER_BINARY and will look in the "
        f"wrong build tree: {client_offenders}"
    )

