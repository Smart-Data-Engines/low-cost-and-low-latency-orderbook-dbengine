"""A replica that restarts keeps what it holds (#101).

Measured before the fix, on i3-7100U with a 23.07 MB primary log: the restart deleted 2261 columnar
files, re-streamed the whole log, doubled the replica's own WAL (x2.00 in every one of four runs,
because each replayed record is appended to the replica's log again), and left the node answering
`SELECT` with **one row out of 23501** for 6.4 seconds - `OK`, not a refusal, so a reader could not
tell. It had all 23501 on disk a second earlier.

The window need not exist: `engine_->open()`, WAL tail replay included, runs in `TcpServer::run()`
before `::bind()`, so a node that keeps its data answers completely from its first reply.
"""

from __future__ import annotations

import os
import re
import socket
import time

import pytest

from conftest import ClusterManager, node_log_size, node_log_since, patience

pytestmark = pytest.mark.replication

# Reported rather than only asserted: the numbers are the argument for this item, and an assertion
# that passes prints nothing. `custom_metrics` puts them in the run's report, where the next person
# to change this path can compare against them.
custom_metrics: dict = {}

SYMBOLS = [f"RST{i:02d}" for i in range(8)]
EXCH = "BINANCE"
LEVELS = 300
BATCHES = 8


def raw(port: int, payload: str, settle: float = 0.2) -> str:
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


def rows(port: int, symbol: str) -> int:
    reply = raw(port, f"SELECT * FROM '{symbol}'.'{EXCH}' "
                      f"WHERE timestamp BETWEEN 0 AND 9999999999999999999", settle=0.4)
    lines = [line for line in reply.strip().splitlines() if line.strip()]
    if lines and "ERR" in lines[0].upper():
        pytest.fail(f"the replica refused the read: {lines[0]}")
    return max(0, len(lines) - 2)


def store_shape(data_dir: str) -> tuple[int, int]:
    """(columnar files, WAL bytes). Two numbers because the defect moved both."""
    cols = 0
    wal = 0
    for root, _dirs, names in os.walk(data_dir):
        for name in names:
            if name.endswith(".col"):
                cols += 1
            elif name.startswith("wal_") and name.endswith(".bin"):
                wal += os.path.getsize(os.path.join(root, name))
    return cols, wal


def role_epoch(port: int) -> int:
    """The epoch a node reports for itself: `PRIMARY <epoch>` or `REPLICA <addr> <epoch>`."""
    reply = raw(port, "ROLE", settle=0.2).strip()
    parts = reply.split()
    assert parts and parts[-1].isdigit(), f"the node answered {reply!r} to ROLE, with no epoch in it"
    return int(parts[-1])


def replication_state(data_dir: str) -> str:
    path = os.path.join(data_dir, "repl_state.txt")
    if not os.path.exists(path):
        return "(no repl_state.txt)"
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def replayed_count(port: int) -> int:
    """What the node itself says it has replayed, from `STATUS`.

    Per process: the counter lives in the `ReplicationClient` object, which a restart replaces. So
    after a restart this is what the new process pulled, which is the quantity #101 is about.
    """
    status = raw(port, "STATUS", settle=0.3)
    line = next((ln for ln in status.splitlines() if ln.startswith("replication:")), "")
    assert line, f"STATUS has no replication line, so this node is not a replica: {status!r}"
    return int(line.split("replayed=")[1].split()[0])


def load_primary(port: int, batches: int = BATCHES) -> None:
    with socket.create_connection(("127.0.0.1", port), timeout=60) as sock:
        sock.settimeout(60.0)
        for batch in range(batches):
            for symbol in SYMBOLS:
                base = 100_000 + batch * 1000
                body = "\n".join(f"{base - i} {10 + i} 1" for i in range(LEVELS))
                sock.sendall((f"MINSERT {symbol} {EXCH} bid {LEVELS}\n" + body + "\n").encode())
            time.sleep(0.05)
            try:
                sock.recv(1 << 20)
            except socket.timeout:
                pass


def test_a_restarted_replica_keeps_what_it_holds() -> None:
    """Three assertions at once, because each one passes on its own while the others are broken.

    A file count alone passes for a node that kept its files and re-streamed anyway. A WAL size
    alone passes for a node that wiped its columnar files and re-streamed into a log that happened
    not to grow. And a log assertion alone passes for a node that says nothing and does it anyway -
    which is why the log check is the third and not a substitute: the previous test of this
    behaviour asserted the opposite of what the node was doing and passed, and the node's own log
    was where that came out.
    """
    cluster = ClusterManager()
    cluster.start()
    try:
        primary, replica = cluster.primary(), cluster.replica()

        load_primary(primary.tcp_port)
        raw(primary.tcp_port, "FLUSH", settle=1.5)

        want = rows(primary.tcp_port, SYMBOLS[0])
        assert want > 0, "the primary stored nothing, so this test has no subject"

        deadline = time.time() + patience(120)
        while time.time() < deadline and rows(replica.tcp_port, SYMBOLS[0]) < want:
            time.sleep(0.5)
        assert rows(replica.tcp_port, SYMBOLS[0]) == want, "the replica never caught up"
        time.sleep(3.0)   # let the replica's own flush timer put the rows in columnar files

        cols_before, wal_before = store_shape(replica.data_dir)
        assert cols_before > 0, "the replica has no columnar files, so nothing here is measurable"
        _, primary_wal = store_shape(primary.data_dir)
        log_at = node_log_size(replica)

        cluster.restart_node(replica.index)
        replica = cluster.nodes[replica.index]

        # The node answers, so its own WAL replay has finished - that runs before the bind. What it
        # answers with is the whole question.
        first_answer = rows(replica.tcp_port, SYMBOLS[0])

        cols_after, wal_after = store_shape(replica.data_dir)
        log = node_log_since(replica, log_at)

        assert cols_after >= cols_before, (
            f"the restart deleted columnar files: {cols_before} before, {cols_after} after. "
            f"Nothing about a restart requires discarding the store."
        )
        # The discriminator is a fraction of a measured quantity rather than a byte count. Before
        # the fix the growth was one whole copy of the primary's log - x2.00 of the replica's own WAL
        # in four runs out of four; after it, the only growth is what the primary appended during the
        # restart itself. Half a copy sits between the two with room on both sides.
        grew_by = wal_after - wal_before
        assert grew_by < primary_wal * 0.5, (
            f"the replica's WAL grew by {grew_by} B, which is {grew_by / max(1, primary_wal):.2f} of "
            f"the primary's {primary_wal} B log: it re-appended the stream it already had. Each "
            f"restart adds another copy, so the cost is cumulative and bounded only by retention."
        )
        assert "clearing local data" not in log, (
            "the node wiped its store on a restart with no role change; the saved position and the "
            "stream identity are what make that unnecessary"
        )
        assert "REPLICATE 0 0 0" not in log, (
            "the node asked for the whole log from zero, so it discarded the position it had"
        )
        custom_metrics["restart_columnar_files_before"] = cols_before
        custom_metrics["restart_columnar_files_after"] = cols_after
        custom_metrics["restart_replica_wal_growth_bytes"] = grew_by
        custom_metrics["restart_primary_wal_bytes"] = primary_wal
        custom_metrics["restart_first_answer_rows"] = first_answer
        custom_metrics["restart_expected_rows"] = want

        assert first_answer == want, (
            f"the replica's first answer after the restart had {first_answer} of {want} rows. It is "
            f"reachable and incomplete, and it reports OK rather than refusing - so a reader cannot "
            f"tell. Its own WAL replay runs before the bind, so a node that keeps its data has no "
            f"such window at all."
        )
    finally:
        cluster.shutdown()


def test_a_different_wal_at_the_same_address_makes_the_replica_start_over() -> None:
    """The control, and requirement 4.3 on a real cluster.

    A test that proves a replica does not discard proves nothing without one that proves it does
    when it should - and this is the case the identity exists for rather than a case an address
    could have caught. The primary is killed, **its data directory is emptied**, and it is started
    again on the same host and the same port. Nothing observable about the endpoint changed; the
    stream did.

    Before the identity, "resume from the saved position" here means resuming inside a WAL that has
    never existed, and the replica would keep serving rows no primary holds - reporting `OK` for
    data that cannot be reconciled with anything. So the assertion is that the rows go away.
    """
    cluster = ClusterManager()
    cluster.start()
    try:
        primary, replica = cluster.primary(), cluster.replica()

        load_primary(primary.tcp_port)
        raw(primary.tcp_port, "FLUSH", settle=1.5)
        want = rows(primary.tcp_port, SYMBOLS[0])
        assert want > 0, "the primary stored nothing, so this test has no subject"

        deadline = time.time() + patience(120)
        while time.time() < deadline and rows(replica.tcp_port, SYMBOLS[0]) < want:
            time.sleep(0.5)
        assert rows(replica.tcp_port, SYMBOLS[0]) == want, "the replica never caught up"
        time.sleep(3.0)

        log_at = node_log_size(replica)

        # Same address, new stream: the identity file lives in the data directory, so emptying it
        # is exactly what a primary rebuilt from a bare disk looks like.
        cluster.kill_node(primary.index)
        for root, dirs, names in os.walk(primary.data_dir, topdown=False):
            for name in names:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        cluster.restart_node(primary.index)

        # The replica reconnects on its own - the connection died with the node - so no restart of
        # it is involved. Its reconnect backoff is five seconds.
        deadline = time.time() + patience(90)
        discarded = False
        while time.time() < deadline:
            log = node_log_since(replica, log_at)
            if "discarding and replaying from zero" in log:
                discarded = True
                break
            time.sleep(1.0)

        log = node_log_since(replica, log_at)
        assert discarded, (
            "the replica reconnected to a primary serving a different WAL and resumed from its old "
            f"position, so it is serving rows that primary never had. Its log since the kill:\n{log}"
        )
        assert "a different WAL at the same address" in log, (
            "the discard happened for some other reason than the one this test arranged, so it is "
            f"not measuring requirement 4.3:\n{log}"
        )

        # And what it serves afterwards is the new primary's content, which is nothing.
        settled = time.time() + patience(20)
        while time.time() < settled and rows(replica.tcp_port, SYMBOLS[0]) != 0:
            time.sleep(0.5)
        assert rows(replica.tcp_port, SYMBOLS[0]) == 0, (
            "the replica still answers with rows from the WAL it stopped following"
        )
    finally:
        cluster.shutdown()


def test_a_promotion_deletes_the_position_it_had_in_the_old_primarys_stream() -> None:
    """Requirement 3.1 on the real failover path, which is the half a unit test cannot reach.

    `promote_to_primary()` deleting the file is pinned in C++; that it is *this* function the
    failover manager reaches, with the state file the server actually configured
    (`<data_dir>/repl_state.txt`), is not. The distinction has bitten before: the deletion used to
    rebuild the path from `base_dir_` instead of reading the configured one, which was right
    wherever the server set it and deleted nothing at all in every unit test.

    It matters because everything else about this item leans on it. With the position gone there is
    no identity to match, so a node that accepted writes starts over without anybody checking
    whether it accepted writes (requirement 2.2).
    """
    cluster = ClusterManager()
    cluster.start()
    try:
        primary, replica = cluster.primary(), cluster.replica()
        state_file = os.path.join(replica.data_dir, "repl_state.txt")

        load_primary(primary.tcp_port)
        raw(primary.tcp_port, "FLUSH", settle=1.5)
        want = rows(primary.tcp_port, SYMBOLS[0])
        assert want > 0

        deadline = time.time() + patience(120)
        while time.time() < deadline and rows(replica.tcp_port, SYMBOLS[0]) < want:
            time.sleep(0.5)
        assert rows(replica.tcp_port, SYMBOLS[0]) == want, "the replica never caught up"

        # The replica writes it on a ten-second timer and on the way out, so wait for the artifact
        # rather than assuming it: an assertion on a file that was never there passes for the wrong
        # reason.
        deadline = time.time() + patience(30)
        while time.time() < deadline and not os.path.exists(state_file):
            time.sleep(0.5)
        assert os.path.exists(state_file), (
            "the replica never saved a position, so the deletion below cannot be observed")

        cluster.kill_node(primary.index)

        deadline = time.time() + patience(90)
        while time.time() < deadline:
            role = raw(replica.tcp_port, "ROLE", settle=0.2)
            if "PRIMARY" in role:
                break
            time.sleep(1.0)
        assert "PRIMARY" in raw(replica.tcp_port, "ROLE", settle=0.2), (
            "the surviving node was never promoted, so nothing here is about a promotion")

        assert not os.path.exists(state_file), (
            "a promoted node kept the position it had in the old primary's stream. After a restart "
            "it would resume from there, with the records it accepted as primary sitting above it - "
            "and the identity would match, so nothing would object."
        )
    finally:
        cluster.shutdown()


def _stream_and_restart(cluster: ClusterManager, batches: int) -> tuple[int, int]:
    """Load `batches` batches, wait for the replica, restart it, and report what it re-streamed.

    Returns (records the primary wrote, records the replica replayed after the restart). Both come
    from the nodes themselves - the primary's WAL and the replica's own `STATUS` counter - rather
    than from anything this process timed.
    """
    primary, replica = cluster.primary(), cluster.replica()

    load_primary(primary.tcp_port, batches=batches)
    raw(primary.tcp_port, "FLUSH", settle=1.5)
    want = rows(primary.tcp_port, SYMBOLS[0])
    assert want > 0, "the primary stored nothing"

    deadline = time.time() + patience(180)
    while time.time() < deadline and rows(replica.tcp_port, SYMBOLS[0]) < want:
        time.sleep(0.5)
    assert rows(replica.tcp_port, SYMBOLS[0]) == want, "the replica never caught up"
    time.sleep(3.0)

    written = replayed_count(replica.tcp_port)
    assert written > 0, "the replica reports replaying nothing, so it received nothing"

    cluster.restart_node(replica.index)
    replica = cluster.nodes[replica.index]

    # Let the new process finish its handshake and whatever catch-up follows it.
    time.sleep(patience(8))
    return written, replayed_count(replica.tcp_port)


def test_what_a_restart_costs_does_not_grow_with_the_store() -> None:
    """Requirement 1.3, and the quantity asserted is not the one the task first named.

    The property an operator feels is time: before this item a restarted replica was reachable and
    incomplete for as long as it took to re-stream everything it already had - measured at 6.4
    seconds for 23 MB, and growing with the store. Timing it here would measure this harness: the
    first attempt at a store-size comparison reported the same number for a three-times store,
    because what it was resolving was its own sampling interval.

    So the assertion is on what made the time grow: how many records the replica pulls from the
    primary after a restart. Before, that was the whole store. After, it is whatever the primary
    appended while the node was down, and this test appends nothing - so it does not scale, and the
    discriminator needs no threshold of its own: **the larger store's replay is compared against
    the smaller store's size**. Anything proportional fails that; anything constant passes it with
    room to spare.
    """
    small, large = 4, 12

    cluster = ClusterManager()
    cluster.start()
    try:
        small_written, small_replayed = _stream_and_restart(cluster, small)
    finally:
        cluster.shutdown()

    cluster = ClusterManager()
    cluster.start()
    try:
        large_written, large_replayed = _stream_and_restart(cluster, large)
    finally:
        cluster.shutdown()

    # The two loads really are different sizes, or the comparison below is between equals.
    assert large_written > small_written * 2, (
        f"the two runs streamed {small_written} and {large_written} records, which are not two "
        f"different store sizes - so this test cannot say anything about scaling"
    )

    assert large_replayed < small_written, (
        f"after a restart the replica pulled {large_replayed} records for a store of "
        f"{large_written}, while the smaller store was {small_written}: the work is proportional "
        f"to what the replica already holds rather than to what it is missing. That is the whole "
        f"of #101 - a restart costs a full re-sync"
    )
    assert small_replayed < small_written, (
        f"even at the smaller size the restart pulled {small_replayed} of {small_written} records"
    )

    custom_metrics["restart_records_held_small"] = small_written
    custom_metrics["restart_records_restreamed_small"] = small_replayed
    custom_metrics["restart_records_held_large"] = large_written
    custom_metrics["restart_records_restreamed_large"] = large_replayed


def test_a_replica_keeps_the_epoch_it_fences_with_across_a_crash() -> None:
    """#103: the number both epoch guards stand on, on a real cluster and across a `SIGKILL`.

    Before the fix it lived in the `ReplicationClient` object and started at zero, so the first
    `REPLICATE` of every connection carried 0 - which is every connection after a restart or a role
    change. Zero is never greater than anything, so the primary's `ERR STALE_PRIMARY` had nothing to
    refuse and the replica's own record filter had nothing below it.

    The assertion that decides this is the **first handshake after the restart**, not the node's
    `ROLE` a while later: a heartbeat arrives within five seconds and would supply the number by
    itself. The handshake goes out milliseconds after the process starts, so a non-zero epoch on it
    came from what the node had written down.
    """
    cluster = ClusterManager()
    cluster.start()
    try:
        primary, replica = cluster.primary(), cluster.replica()

        want = role_epoch(primary.tcp_port)
        assert want > 0, "the cluster elected nobody, so there is no epoch for a replica to learn"

        # The replica learns it from the stream: a heartbeat every five seconds, or the first record.
        deadline = time.time() + patience(30)
        while time.time() < deadline and role_epoch(replica.tcp_port) != want:
            time.sleep(0.5)
        assert role_epoch(replica.tcp_port) == want, (
            f"the replica reports epoch {role_epoch(replica.tcp_port)} while following a primary in "
            f"epoch {want}: it is answering ROLE with a number it does not have"
        )

        state = replication_state(replica.data_dir)
        assert f"epoch={want}" in state, (
            f"the epoch is not in the file a restart reads: {state!r}. Promotions write EPOCH "
            f"records to a WAL and nothing writes one to a replica's own, so this file is the only "
            f"place this node can learn it from again."
        )

        log_at = node_log_size(replica)
        cluster.kill_node(replica.index)          # SIGKILL: no shutdown, no save on the way out
        cluster.restart_node(replica.index)
        replica = cluster.nodes[replica.index]

        # Wait for the node to answer at all before reading its log, so a slow start is not read as
        # a missing handshake.
        deadline = time.time() + patience(30)
        while time.time() < deadline:
            try:
                role_epoch(replica.tcp_port)
                break
            except (OSError, AssertionError):
                time.sleep(0.5)

        log = node_log_since(replica, log_at)
        handshakes = re.findall(r"handshake sent: REPLICATE (\d+) (\d+) (\d+)", log)
        assert handshakes, (
            "the restarted replica logged no handshake, so this test measured nothing; "
            f"tail: {log[-800:]!r}"
        )
        first_epoch = int(handshakes[0][2])
        custom_metrics["restart_epoch_on_first_handshake"] = first_epoch
        custom_metrics["restart_epoch_expected"] = want
        assert first_epoch == want, (
            f"the first handshake after the crash carried epoch {first_epoch} rather than {want}, so "
            f"a superseded primary would have been served on the one connection the guard exists "
            f"for. Handshakes seen: {handshakes[:3]}"
        )
    finally:
        cluster.shutdown()
