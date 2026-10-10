"""A replica that is joining does not stand for election (#215).

A replica whose store was replaced to follow a stream - here, one whose position names no stream,
which discards and replays - holds part of that stream until its primary says the catch-up ended.
Before #215 a primary lost in that window handed the role to it, and the node returning after it
discarded its own copy: every write the joining node had not received was gone.

The window is held open by the primary itself: the test pauses it the moment the replica says it is
joining, and the catch-up it was serving is far larger than what a socket and a send queue hold, so
the end of the catch-up cannot already be on its way. Then the primary dies. The premise is checked
rather than assumed: the replica must say it is joining and must not have said the catch-up ended.
"""
from __future__ import annotations

import os
import socket
import time

import pytest

from conftest import (ClusterManager, node_log_size, node_log_since, patience, role_of,
                      send_command, wait_for_role)

pytestmark = pytest.mark.failover

LEVELS = 500
BATCHES = 1500                     # 750 000 levels, about 24 MB of WAL: far beyond a queue and a socket
LEASE_TTL = "5"


def fill(port: int, symbol: str, batches: int, first: int = 0) -> None:
    """`batches` MINSERTs of LEVELS levels on one connection, each answered, then a FLUSH."""
    with socket.create_connection(("127.0.0.1", port), timeout=patience(60)) as s:
        s.settimeout(patience(60))
        buf = b""
        while b"\n\n" not in buf:
            buf += s.recv(65536)
        for i in range(first, first + batches):
            body = "\n".join(f"{1_000_000 + i * LEVELS + j} 1 1" for j in range(LEVELS))
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


def count(port: int, symbol: str) -> int | None:
    """Rows of `symbol` the node holds, or None while it cannot answer (bootstrapping, down).

    `COUNT(*)` counts stored rows only per time bucket, so one bucket of a year holds them all:
    every row here was written within a minute or two, by arrival time. Summed over the buckets
    anyway, because the answer is a list."""
    try:
        reply = send_command(port, f"SELECT COUNT(*) FROM '{symbol}'.'EX' GROUP BY TIME_BUCKET(366d)",
                             timeout=patience(20))
    except Exception:
        return None
    if not reply.startswith("OK"):
        return None
    total = 0
    for line in reply.splitlines()[1:]:
        fields = line.split()
        if fields and fields[-1].isdigit() and fields[0].isdigit():
            total += int(fields[-1])
    return total


def wait_for(predicate, within: float, every: float = 0.2) -> bool:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(every)
    return predicate()


@pytest.mark.timeout(600)
def test_a_primary_lost_while_its_replica_joins_loses_no_write():
    cluster = ClusterManager()
    cluster.extra_node_args = ["--coordinator-lease-ttl", LEASE_TTL]
    cluster.start()
    try:
        primary, replica = cluster.primary(), cluster.replica()
        fill(primary.tcp_port, "JOIN", BATCHES)
        want = BATCHES * LEVELS
        assert count(primary.tcp_port, "JOIN") == want

        # The replica comes back holding a position that names no stream: it discards its store and
        # replays, which is a join - the same act as a node rejoining after a failover.
        cluster.kill_node(replica.index)
        os.remove(os.path.join(replica.data_dir, "repl_state.txt"))
        offset = node_log_size(replica)
        cluster.restart_node(replica.index)
        replica = cluster.nodes[replica.index]

        # The moment it decides to discard, the primary stops: whatever it has queued is all the
        # replica can still get, and 24 MB of catch-up do not fit there. The decision's own line,
        # which a build from before #215 writes too - so the same test run against one fails where
        # the defect is, and not on a line it never had.
        deadline = time.monotonic() + patience(60)
        while time.monotonic() < deadline:
            if "discarding and replaying from zero" in node_log_since(replica, offset):
                break
            time.sleep(0.005)
        else:
            pytest.fail("the replica never discarded:\n" + node_log_since(replica, offset)[-3000:])
        cluster.pause_node(primary.index)
        time.sleep(1.0)
        assert "no longer joining" not in node_log_since(replica, offset), (
            "the catch-up ended before the test could hold the window open; the premise is lost")

        cluster.kill_node(primary.index)

        # Without #215 the replica takes the role once the lease and the election wait have gone:
        # about ten seconds here. Watched for three times that.
        became_primary = False
        watch_until = time.monotonic() + 3 * 2 * int(LEASE_TTL)
        while time.monotonic() < watch_until:
            try:
                if role_of(replica.tcp_port).upper().startswith("PRIMARY"):
                    became_primary = True
                    break
            except Exception:
                pass
            time.sleep(0.2)
        log = node_log_since(replica, offset)
        assert not became_primary, (
            "a replica joining a stream it does not hold took the primary role:\n" + log[-3000:])
        assert "joining stream" in log, log[-3000:]
        assert "does not stand for election (#215)" in log, log[-3000:]
        status = send_command(replica.tcp_port, "STATUS")
        assert "joining: stream=" in status, status
        assert os.path.exists(os.path.join(replica.data_dir, "repl_joining.txt"))

        # The node that holds the stream returns and takes the role; the replica finishes joining.
        cluster.restart_node(primary.index)
        primary = cluster.nodes[primary.index]
        wait_for_role(primary.tcp_port, "PRIMARY", timeout=patience(60))
        assert count(primary.tcp_port, "JOIN") == want, "the primary lost rows of its own"

        assert wait_for(lambda: count(replica.tcp_port, "JOIN") == want, within=patience(120)), (
            f"the replica holds {count(replica.tcp_port, 'JOIN')} of {want} rows")
        assert wait_for(lambda: not os.path.exists(os.path.join(replica.data_dir, "repl_joining.txt")),
                        within=patience(30)), "the replica caught up and is still recorded as joining"
        assert "joining:" not in send_command(replica.tcp_port, "STATUS")
        assert role_of(replica.tcp_port).upper().startswith("REPLICA")
    finally:
        cluster.shutdown()
