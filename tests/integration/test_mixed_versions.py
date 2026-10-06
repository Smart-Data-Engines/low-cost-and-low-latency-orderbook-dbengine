"""The previous version and this one, side by side (#56).

Every question here is answered by running the two binaries: the previous version's server, built
from the revision `scripts/previous_version.txt` names and handed in as `OB_PREVIOUS_SERVER_BINARY`,
and this tree's. What they are asked is what a rolling upgrade does - a data directory written by
one and opened by the other, each direction; a primary of one with a replica of the other, each way;
a failover from one to the other; a mesh of both; this tree's clients against the previous server -
and each answer is a row of the matrix in `docs/upgrading.md`, which the last test holds to this
module's tests, both ways.

Without the previous server the module skips on a developer's machine and fails in CI (`CI=true`):
a skip in the job that should run it would read as a pass (#85).
"""
from __future__ import annotations

import os
import re
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from conftest import (ClusterManager, free_port, patience, role_of, send_command, server_binary_path,
                      wait_for_role)

pytestmark = pytest.mark.replication

REPO = Path(__file__).resolve().parents[2]
CURRENT = server_binary_path()
PREVIOUS = os.environ.get("OB_PREVIOUS_SERVER_BINARY", "")
BACKUP_TOOL = str(Path(CURRENT).resolve().with_name("ob_backup"))
LEVELS = 50

MISSING = not PREVIOUS or not Path(PREVIOUS).is_file()
WHY = ("no previous server: build the revision scripts/previous_version.txt names and set "
       "OB_PREVIOUS_SERVER_BINARY to its ob_tcp_server")
if MISSING and os.environ.get("CI") != "true":
    pytestmark = [pytest.mark.replication, pytest.mark.skip(reason=WHY)]


@pytest.fixture(autouse=True)
def _the_previous_server():
    """In CI, where the job builds it, a missing previous server fails every test with the reason."""
    if MISSING:
        pytest.fail(WHY)


class Node:
    """One server of either version on a data directory the caller owns, logging into it."""

    def __init__(self, binary: str, data_dir: str, extra: list[str] | None = None):
        self.binary = binary
        self.data_dir = data_dir
        self.extra = list(extra or [])
        self.port = 0
        self.proc: subprocess.Popen | None = None

    def start(self, timeout: float = 30.0) -> None:
        self.port = free_port()
        os.makedirs(self.data_dir, exist_ok=True)
        log = open(os.path.join(self.data_dir, "node.log"), "a", encoding="utf-8")
        self.proc = subprocess.Popen(
            [self.binary, "--port", str(self.port), "--data-dir", self.data_dir,
             "--metrics-port", "0", *self.extra], stdout=log, stderr=subprocess.STDOUT)
        log.close()
        deadline = time.time() + patience(timeout)
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.binary} exited with {self.proc.returncode}:\n"
                                   f"{self.log()[-2000:]}")
            try:
                if ask(self.port, "PING").startswith("PONG"):
                    return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError(f"{self.binary} never answered on {self.port}:\n{self.log()[-2000:]}")

    def run_to_exit(self, timeout: float = 30.0) -> tuple[int, str]:
        """Start it expecting it to refuse: the exit status and its log."""
        self.port = free_port()
        run = subprocess.run([self.binary, "--port", str(self.port), "--data-dir", self.data_dir,
                              "--metrics-port", "0", *self.extra],
                             capture_output=True, text=True, timeout=patience(timeout))
        return run.returncode, run.stdout + run.stderr

    def log(self) -> str:
        path = os.path.join(self.data_dir, "node.log")
        return open(path, encoding="utf-8", errors="replace").read() if os.path.exists(path) else ""

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=10)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=patience(60))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


def ask(port: int, line: str, timeout: float = 30.0) -> str:
    """One command on a fresh connection, its whole answer."""
    with socket.create_connection(("127.0.0.1", port), timeout=patience(timeout)) as s:
        s.settimeout(patience(timeout))
        buf = b""
        while b"\n\n" not in buf:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
        s.sendall((line if line.endswith("\n") else line + "\n").encode())
        out = b""
        while True:
            if out.startswith(b"ERR") and out.endswith(b"\n"):
                break
            if out.startswith(b"PONG\n") or out.endswith(b"\n\n"):
                break
            chunk = s.recv(1 << 20)
            if not chunk:
                break
            out += chunk
        return out.decode(errors="replace")


def write(port: int, symbol: str, first: int, count: int) -> None:
    for i in range(first, first + count):
        body = "\n".join(f"{1_000_000 + i * 1000 + j} 1 1" for j in range(LEVELS))
        answer = ask(port, f"MINSERT {symbol} EX bid {LEVELS}\n{body}")
        assert answer.startswith("OK"), answer


def prices(port: int, symbol: str) -> list[int] | None:
    """Every price the node answers, sorted; None while it installs a snapshot."""
    answer = ask(port, f"SELECT * FROM '{symbol}'.'EX'")
    if answer.startswith("ERR"):
        if "bootstrapping" in answer.lower():
            return None
        if "not found" in answer.lower():
            return []
        raise AssertionError(answer)
    lines = answer.strip("\n").split("\n")
    column = lines[1].split("\t").index("price")
    return sorted(int(line.split("\t")[column]) for line in lines[2:])


def expected(first: int, count: int) -> list[int]:
    return sorted(1_000_000 + i * 1000 + j for i in range(first, first + count) for j in range(LEVELS))


def eventually(done, within: float = 60.0) -> bool:
    deadline = time.time() + patience(within)
    while time.time() < deadline:
        if done():
            return True
        time.sleep(0.2)
    return done()


def written_then_killed(binary: str, data_dir: str, symbol: str,
                        extra: list[str] | None = None) -> list[int]:
    """Rows in segments and more in the WAL alone, then a SIGKILL: what an upgrade finds."""
    n = Node(binary, data_dir, ["--flush-interval-ms", "600000", *(extra or [])])
    n.start()
    try:
        write(n.port, symbol, 0, 20)
        assert ask(n.port, "FLUSH").startswith("OK")
        write(n.port, symbol, 20, 10)         # only in the WAL: no tick for ten minutes
    finally:
        n.kill()
    return expected(0, 30)


# ── The data directory ────────────────────────────────────────────────────────

def test_an_upgrade_in_place_keeps_every_row_once():
    with tempfile.TemporaryDirectory(prefix="ob_mixed_up_") as tmp:
        want = written_then_killed(PREVIOUS, f"{tmp}/data", "UPG")
        n = Node(CURRENT, f"{tmp}/data")
        n.start()
        try:
            assert prices(n.port, "UPG") == want, "the upgraded node lost rows or holds some twice"
            write(n.port, "UPG", 30, 1)
            assert ask(n.port, "FLUSH").startswith("OK")
            assert prices(n.port, "UPG") == expected(0, 31)
        finally:
            n.stop()


def test_a_downgrade_keeps_every_row_once_or_refuses_to_start():
    # Segment format 2, which the previous version reads: what a node keeping the way back runs.
    with tempfile.TemporaryDirectory(prefix="ob_mixed_down_") as tmp:
        want = written_then_killed(CURRENT, f"{tmp}/data", "DWN", ["--segment-format", "2"])
        n = Node(PREVIOUS, f"{tmp}/data")
        try:
            n.start()
        except RuntimeError as refused:
            # A refusal is an answer, with a reason; a start with part of the rows is not.
            assert "exited with" in str(refused), refused
            print("downgrade: the previous version refused to start")
            return
        try:
            assert prices(n.port, "DWN") == want, (
                "the previous version started on this version's data directory and holds part of it")
            print("downgrade: the previous version started and holds every row")
        finally:
            n.stop()


def test_a_downgrade_after_format_3_hides_its_segments_and_removes_nothing():
    """A data directory this version wrote in segment format 3, the default, opened by the previous
    one: a start with part of the rows, which this matrix calls a failure, and why it is the answer
    since format 3 - the previous version cannot decode those segments. It says so for each and
    removes none, and this version started again holds every row once (docs/upgrading.md)."""
    with tempfile.TemporaryDirectory(prefix="ob_mixed_down3_") as tmp:
        want = written_then_killed(CURRENT, f"{tmp}/data", "DW3")
        previous = Node(PREVIOUS, f"{tmp}/data")
        previous.start()
        try:
            # What only the WAL held, the previous version replays into segments of its own; what
            # this version sealed is format 3.
            assert prices(previous.port, "DW3") == expected(20, 10), (
                "the previous version read rows of a format-3 segment, or lost a replayed one")
            assert "unsupported format_version=3" in previous.log(), (
                "the previous version left format-3 segments out without saying so")
        finally:
            previous.stop()
        again = Node(CURRENT, f"{tmp}/data")
        again.start()
        try:
            assert prices(again.port, "DW3") == want, (
                "this version, started again after the previous one, lost rows or holds some twice")
        finally:
            again.stop()


# ── Replication ───────────────────────────────────────────────────────────────

def _replicate(primary_binary: str, replica_binary: str, symbol: str) -> None:
    with tempfile.TemporaryDirectory(prefix="ob_mixed_repl_") as tmp:
        repl_port = free_port()
        p = Node(primary_binary, f"{tmp}/p", ["--replication-port", str(repl_port)])
        r = Node(replica_binary, f"{tmp}/r", ["--primary-host", "127.0.0.1",
                                              "--primary-port", str(repl_port)])
        try:
            p.start()
            write(p.port, symbol, 0, 20)
            r.start()
            write(p.port, symbol, 20, 20)
            assert ask(p.port, "FLUSH").startswith("OK")
            want = expected(0, 40)
            assert prices(p.port, symbol) == want
            assert eventually(lambda: prices(r.port, symbol) == want), (
                f"the replica holds {len(prices(r.port, symbol) or [])} of {len(want)} rows")
        finally:
            r.stop()
            p.stop()


def test_a_primary_of_the_previous_version_replicates_to_this_one():
    _replicate(PREVIOUS, CURRENT, "RPN")


def test_a_primary_of_this_version_replicates_to_the_previous_one():
    _replicate(CURRENT, PREVIOUS, "RNP")


class MixedCluster(ClusterManager):
    """The shared cluster harness, with a binary chosen per node."""

    def __init__(self, binaries: dict[int, str]):
        super().__init__()
        self.binaries = binaries

    def _node_argv(self, *, node_index: int, **kw) -> list:
        cmd = super()._node_argv(node_index=node_index, **kw)
        cmd[0] = self.binaries.get(node_index, cmd[0])
        return cmd


def test_a_failover_from_the_previous_primary_to_this_versions_replica_loses_no_write():
    cluster = MixedCluster({0: PREVIOUS, 1: CURRENT})
    cluster.start()
    try:
        primary, replica = cluster.primary(), cluster.replica()
        assert primary.index == 0 and replica.index == 1, "the harness started them the other way"
        write(primary.tcp_port, "FOV", 0, 30)
        assert ask(primary.tcp_port, "FLUSH").startswith("OK")
        want = expected(0, 30)
        assert eventually(lambda: prices(replica.tcp_port, "FOV") == want), "the replica never caught up"
        cluster.kill_node(primary.index)
        wait_for_role(replica.tcp_port, "PRIMARY", timeout=patience(60))
        write(replica.tcp_port, "FOV", 30, 5)
        assert ask(replica.tcp_port, "FLUSH").startswith("OK")
        assert prices(replica.tcp_port, "FOV") == expected(0, 35)
    finally:
        cluster.shutdown()


def _hand_over(binaries: dict[int, str], symbol: str) -> None:
    """A planned FAILOVER from node 0 to node 1 of the versions given: it completes, and every write
    acknowledged before it is on the new primary, which takes writes (#204)."""
    cluster = MixedCluster(binaries)
    cluster.start()
    try:
        primary, replica = cluster.primary(), cluster.replica()
        assert primary.index == 0 and replica.index == 1, "the harness started them the other way"
        write(primary.tcp_port, symbol, 0, 30)
        want = expected(0, 30)
        assert eventually(lambda: prices(replica.tcp_port, symbol) == want), "the replica never caught up"
        reply = send_command(primary.tcp_port, f"FAILOVER {replica.node_id}").strip()
        assert reply.startswith("OK"), f"the handover was refused: {reply!r}"
        # Either side may be the one that waits the election delay - the previous version's target
        # waits it whatever it is told, and so does this one's without a statement from the
        # previous version - so within three of them.
        wait_for_role(replica.tcp_port, "PRIMARY", timeout=patience(30))
        write(replica.tcp_port, symbol, 30, 5)
        assert ask(replica.tcp_port, "FLUSH").startswith("OK")
        assert prices(replica.tcp_port, symbol) == expected(0, 35)
        assert role_of(primary.tcp_port).startswith("REPLICA"), "the outgoing node kept the role"
    finally:
        cluster.shutdown()


def test_a_handover_from_the_previous_primary_to_this_versions_replica_loses_no_write():
    _hand_over({0: PREVIOUS, 1: CURRENT}, "HPN")


def test_a_handover_from_this_versions_primary_to_the_previous_replica_loses_no_write():
    _hand_over({0: CURRENT, 1: PREVIOUS}, "HNP")


# ── The mesh ──────────────────────────────────────────────────────────────────

def test_a_mesh_of_both_versions_converges():
    cluster = MixedCluster({0: PREVIOUS, 1: CURRENT})
    cluster.start_multi_master(node_count=2)
    try:
        cluster.wait_for_mm_mesh(timeout=patience(60))
        a, b = cluster.nodes[0], cluster.nodes[1]
        write(a.tcp_port, "MMA", 0, 10)
        write(b.tcp_port, "MMB", 0, 10)
        for n in (a, b):
            assert ask(n.tcp_port, "FLUSH").startswith("OK")
        for n in (a, b):
            for symbol in ("MMA", "MMB"):
                assert eventually(lambda: prices(n.tcp_port, symbol) == expected(0, 10)), (
                    f"node {n.index} holds {len(prices(n.tcp_port, symbol) or [])} rows of {symbol}")
    finally:
        cluster.shutdown()


# ── Clients ───────────────────────────────────────────────────────────────────

def test_this_versions_clients_work_against_the_previous_server():
    with tempfile.TemporaryDirectory(prefix="ob_mixed_client_") as tmp:
        n = Node(PREVIOUS, f"{tmp}/data")
        n.start()
        try:
            import sys
            sys.path.insert(0, str(REPO / "python"))
            from orderbook_engine import OrderbookEngine
            engine = OrderbookEngine(host="127.0.0.1", port=n.port, timeout=patience(30))
            try:
                engine.insert("CLI", "EX", "bid", [100, 101], [1, 2])
                engine.flush()
                assert [r.price for r in engine.query("SELECT * FROM 'CLI'.'EX'")] == [100, 101]
                caps = engine.server_capabilities()
                assert "backup" not in caps, caps   # the previous version takes no backups
            finally:
                engine.close()
            refused = subprocess.run([BACKUP_TOOL, "--host", "127.0.0.1", "--port", str(n.port)],
                                     capture_output=True, text=True, timeout=patience(30))
            assert refused.returncode == 2 and "does not take backups" in refused.stderr, refused
        finally:
            n.stop()


# ── The matrix ────────────────────────────────────────────────────────────────

def test_every_scenario_has_a_row_in_the_upgrade_matrix_and_every_row_a_scenario():
    doc = (REPO / "docs" / "upgrading.md").read_text()
    rows = set(re.findall(r"`(test_[a-z0-9_]+)`", doc))
    here = {name for name in globals() if name.startswith("test_") and
            name != "test_every_scenario_has_a_row_in_the_upgrade_matrix_and_every_row_a_scenario"}
    assert here - rows == set(), f"scenarios with no row in docs/upgrading.md: {sorted(here - rows)}"
    assert rows - here == set(), f"rows naming no test here: {sorted(rows - here)}"
