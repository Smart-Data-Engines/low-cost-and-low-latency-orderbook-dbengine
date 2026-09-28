"""A replica that bootstrapped from a snapshot, and then became a primary, gives every row it holds
to a replica of its own (#197).

Measured before the fix: 0 of 18 000. A snapshot's rows are in the installed segments and in no
record of the replica's own WAL, which began at file 0 before the install - and a primary sends a
snapshot only when the WAL file a replica asks from is gone, so a new replica asking from the start
was streamed a log without them, and held nothing, without an error. After a failover to such a
replica every other replica sees a new identity, starts over, and asks from the start: the same
thing, on a real cluster. An install begins a new WAL lineage now - a new identity, a first file
after it - and both tests here are about what the next replica holds.
"""
from __future__ import annotations

import os
import socket
import tempfile

import pytest

from conftest import ClusterManager, free_port, patience, wait_for_role
from test_compaction import Node, eventually, lowest_wal_file

pytestmark = pytest.mark.replication

LEVELS = 300
ROTATE = "65573"   # the smallest the server takes: the WAL rotates every few writes


def fill(node_port: int, symbol: str, batches: int, first: int = 0) -> None:
    """`batches` MINSERTs of LEVELS levels on one connection, each answered, then a FLUSH."""
    with socket.create_connection(("127.0.0.1", node_port), timeout=patience(60)) as s:
        s.settimeout(patience(60))
        buf = b""
        while b"\n\n" not in buf:
            buf += s.recv(65536)
        for i in range(first, first + batches):
            body = "\n".join(f"{1_000_000 + i * 1000 + j} 1 1" for j in range(LEVELS))
            s.sendall(f"MINSERT {symbol} EX bid {LEVELS}\n{body}\n".encode())
            answer = b""
            while not answer.endswith(b"\n\n") and not (answer.startswith(b"ERR") and answer.endswith(b"\n")):
                answer += s.recv(65536)
            assert answer.startswith(b"OK"), answer
        s.sendall(b"FLUSH\n")
        answer = b""
        while not answer.endswith(b"\n\n") and not (answer.startswith(b"ERR") and answer.endswith(b"\n")):
            answer += s.recv(65536)
        assert answer.startswith(b"OK"), answer


def serving(node: Node, symbol: str) -> list[int] | None:
    """Every price, sorted; None while the node installs a snapshot."""
    rows = node.rows_once_serving(symbol)
    return None if rows is None else sorted(node.prices(symbol))


def test_a_replica_made_a_primary_after_a_snapshot_gives_a_new_replica_every_row():
    with tempfile.TemporaryDirectory(prefix="ob_lineage_a_") as adir, \
            tempfile.TemporaryDirectory(prefix="ob_lineage_r_") as rdir, \
            tempfile.TemporaryDirectory(prefix="ob_lineage_q_") as qdir:
        p1 = free_port()
        a = Node(adir, ["--replication-port", str(p1), "--wal-rotate-bytes", ROTATE])
        r = Node(rdir, ["--primary-host", "127.0.0.1", "--primary-port", str(p1)])
        r2 = q = None
        try:
            a.start()
            first = lowest_wal_file(adir)
            fill(a.port, "LIN", 60)
            assert eventually(lambda: lowest_wal_file(adir) > first), (
                "the primary still holds its first WAL file, so the replica would stream the log "
                "rather than bootstrap from a snapshot")
            want = sorted(a.prices("LIN"))
            r.start()
            assert eventually(lambda: serving(r, "LIN") == want, within=60), "no bootstrap"
            assert "Snapshot installed" in r.log(), "the replica streamed the log after all"
            r.stop()
            a.stop()

            # R is a primary now, as a failover makes one; Q is a new replica of it.
            p2 = free_port()
            r2 = Node(rdir, ["--replication-port", str(p2)])
            r2.start()
            assert sorted(r2.prices("LIN")) == want
            q = Node(qdir, ["--primary-host", "127.0.0.1", "--primary-port", str(p2)])
            q.start()
            assert eventually(lambda: serving(q, "LIN") == want, within=60), (
                f"a new replica of the promoted node holds {len(serving(q, 'LIN') or [])} of "
                f"{len(want)} rows")
            assert "Snapshot installed" in q.log(), (
                "the new replica was streamed the log: rows the log does not hold would be lost")
            assert "A new WAL lineage (a snapshot was installed)" in r.log()
        finally:
            for n in (q, r2, r, a):
                if n is not None:
                    n.stop()


def test_after_a_failover_to_a_replica_from_a_snapshot_the_other_replica_holds_every_row():
    cluster = ClusterManager()
    cluster.extra_node_args = ["--wal-rotate-bytes", ROTATE]
    cluster.start()
    try:
        primary, first_replica = cluster.primary(), cluster.replica()
        before = lowest_wal_file(primary.data_dir)
        fill(primary.tcp_port, "LFO", 60)
        assert eventually(lambda: lowest_wal_file(primary.data_dir) > before, within=60), (
            "the primary kept its first WAL file, so a new replica would stream it")

        late = cluster.add_replica(timeout=patience(60))
        # What the primary holds, and the late replica bootstrapped from a snapshot of it.
        probe = _Reader(primary.tcp_port)
        want = probe.prices("LFO")
        assert want, "the primary stored nothing"
        late_reader = _Reader(late.tcp_port)
        assert eventually(lambda: late_reader.prices_or_none("LFO") == want, within=90), (
            "the late replica did not catch up")
        assert "Snapshot installed" in _log(late.data_dir), "the late replica streamed the log"

        # The late replica alone survives, and the election makes it the primary.
        cluster.kill_node(first_replica.index)
        cluster.kill_node(primary.index)
        wait_for_role(late.tcp_port, "PRIMARY", timeout=patience(60))

        # The first replica comes back and follows it: it starts over, and must get every row.
        cluster.restart_node(first_replica.index)
        back = _Reader(cluster.nodes[first_replica.index].tcp_port)
        assert eventually(lambda: back.prices_or_none("LFO") == want, within=120), (
            f"after the failover the other replica holds {len(back.prices_or_none('LFO') or [])} "
            f"of {len(want)} rows")
    finally:
        cluster.shutdown()


class _Reader:
    """SELECT over a bare connection, for a cluster node."""

    def __init__(self, port: int):
        self.port = port

    def _select(self, symbol: str) -> list[str]:
        with socket.create_connection(("127.0.0.1", self.port), timeout=patience(30)) as s:
            s.settimeout(patience(30))
            buf = b""
            while b"\n\n" not in buf:
                buf += s.recv(65536)
            s.sendall(f"SELECT * FROM '{symbol}'.'EX'\n".encode())
            out = b""
            while not out.endswith(b"\n\n") and not (out.startswith(b"ERR") and out.endswith(b"\n")):
                chunk = s.recv(1 << 20)
                if not chunk:
                    break
                out += chunk
        return out.decode(errors="replace").strip("\n").split("\n")

    def prices_or_none(self, symbol: str) -> list[int] | None:
        lines = self._select(symbol)
        if lines and lines[0].startswith("ERR"):
            if "bootstrapping" in lines[0].lower():
                return None
            if "not found" in lines[0].lower():
                return []
            raise AssertionError(lines[0])
        column = lines[1].split("\t").index("price")
        return sorted(int(line.split("\t")[column]) for line in lines[2:])

    def prices(self, symbol: str) -> list[int]:
        got = self.prices_or_none(symbol)
        assert got is not None, "the node is bootstrapping"
        return got


def _log(data_dir: str) -> str:
    path = os.path.join(data_dir, "node.log")
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()
