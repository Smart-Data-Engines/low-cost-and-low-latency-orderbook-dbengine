"""#124: the first tests in this battery that cross a WAL file boundary.

Until `--wal-rotate-bytes` existed, the threshold was a literal in `Engine`'s constructor
(`WALWriter(base_dir, 512ULL << 20, fsync_policy)`), so reaching a rotation from the outside meant
writing **512 MB** — two orders of magnitude past anything this battery does in its nineteen
minutes. Every integration test that had ever looked at a WAL position therefore stayed inside one
file, and three behaviours that only happen at a rotation were covered by unit tests driving a
`WALWriter` directly, which cannot express a reconnect or a retention pass:

* **retention**, where `safe_truncate` is the smallest `confirmed_file` across connected replicas,
* **catch-up across files**, which is where #98's arithmetic was wrong,
* and the **replica lag** #123 fixed, whose defect was invisible *within* a file by construction.

That is why #123 could only be pinned at the unit level, and it is what these tests are for.

The threshold used here is the refusal floor of the flag itself — 65573 bytes, a 38-byte header
plus the 64 KiB payload limit. Nothing chose it for convenience: it is the smallest value the
parser accepts, so if that floor ever rises these nodes stop starting and this module fails at
setup rather than quietly measuring something else.

**Records, not rows, are what fill a WAL file**, and the two are worth keeping apart when reading
the volumes below. A single-level `INSERT` is 136 bytes on disk — a 24-byte header, an 88-byte
`DeltaUpdate` and one 24-byte level — so a thousand-odd of them cross three boundaries while
leaving only a thousand-odd rows to count. A `MINSERT` would reach the same byte count in a tenth
of the round trips and leave ten times the rows.
"""

from __future__ import annotations

import os
import socket
import time
import urllib.request

import pytest

from conftest import ClusterManager, patience, tail_node_log

from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.replication

# The parser's floor for --wal-rotate-bytes: sizeof(WALRecordV2) + WAL_MAX_PAYLOAD_LEN.
ROTATE_BYTES = 65573

# 136 bytes per single-level record, so 1500 of them are 204 000 bytes: three rotations past the
# threshold above, with the fourth file open. Asserted rather than assumed everywhere it matters,
# because the record size is a fact about the engine's layout and not about this file.
RECORDS = 1500
RECORD_BYTES = 136

EXCHANGE = "ROTATE"


def insert_records(client: OrderbookEngine, symbol: str, count: int, base: int = 100_000) -> None:
    """`count` single-level inserts, one WAL record each."""
    for i in range(count):
        client.insert(symbol, EXCHANGE, "bid", [base + i], [1 + (i % 97)])


def data_rows(port: int, symbol: str, timeout: float = 20.0) -> list:
    """Every stored row for one symbol, as text lines.

    Rows are recognised **positively** — a data line starts with a timestamp — rather than by
    dropping the `OK` and the column header by position. The header is what made
    `assert rows(node)` unfailable in the mesh module: it is a line, so a node holding nothing
    returned a list of length one, and every count was one too many.
    """
    sql = (f"SELECT * FROM '{symbol}'.'{EXCHANGE}' "
           f"WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(5.0)
        sock.recv(4096)  # banner
        sock.sendall(sql.encode())
        buf = b""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(1 << 16)
            except socket.timeout:
                break
            if not chunk:
                break
            buf += chunk
            # The reply is one OK line, one header line and one line per row, with no terminator,
            # so the read ends when the socket goes quiet. Fifteen hundred rows do not arrive in
            # one segment, hence the loop rather than a single `recv`.
            sock.settimeout(0.4)
    out = []
    for line in buf.decode(errors="replace").splitlines():
        first, _, _ = line.partition("\t")
        if first.isdigit():
            out.append(line)
    return out


def wait_for_rows(port: int, symbol: str, expected: int, timeout: float) -> int:
    """Poll until a node holds `expected` rows, returning the last count seen."""
    deadline = time.monotonic() + timeout
    seen = -1
    while time.monotonic() < deadline:
        seen = len(data_rows(port, symbol))
        if seen >= expected:
            return seen
        time.sleep(0.5)
    return seen


def wal_files(data_dir: str) -> list:
    """The WAL files a node currently has, sorted by index.

    **How many there are is a retention fact, not a rotation fact**, and the difference cost this
    module its first assertion: after fifteen hundred records the primary had rotated three times
    and held `['wal_000002.bin', 'wal_000003.bin']`, because the replica had confirmed into file 2
    while the writing was still going on and the flush tick had already freed what was below it.
    For "did it rotate", read the highest index or the gauge; for "which files are still here",
    read this.
    """
    return sorted(name for name in os.listdir(data_dir)
                  if name.startswith("wal_") and name.endswith(".bin"))


def wal_index(name: str) -> int:
    """The file index in a WAL filename: `wal_000003.bin` -> 3."""
    return int(name[len("wal_"):-len(".bin")])


def metric(port: int, name: str) -> float:
    """One gauge or counter from /metrics, by name, ignoring its label set.

    Labels matter: the exposition is `name{node_role="..."} value`, and a lookup by bare name plus
    a space finds nothing — which reads as zero and once made a scraper announce that a snapshot
    had never been sent (pitfall 66).
    """
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        body = response.read().decode()
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        key, _, value = line.rpartition(" ")
        if key.split("{")[0] == name:
            return float(value)
    raise AssertionError(f"{name} is not in /metrics at all, which is not the same as zero")


def replica_confirmed_file(port: int) -> int:
    """The WAL file the primary's first replica has acknowledged into.

    The file to watch has to be **this** one rather than the oldest file on disk. Retention runs
    every flush tick, so the oldest file present at any moment may simply be one the pass has not
    reached yet, and a test watching it would fail for a reason that is not a defect. The file the
    slowest connected replica is in is exactly the one `safe_truncate` promises to keep.
    """
    line = replica_line(port)
    return int(line.split("file=")[1].split()[0])


def replica_line(port: int, index: int = 0) -> str:
    """The primary's `replica[i]:` line from STATUS, which carries `file=`, `offset=` and `lag=`."""
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.settimeout(10)
        sock.recv(4096)
        sock.sendall(b"STATUS\n")
        time.sleep(0.4)
        reply = sock.recv(1 << 20).decode(errors="replace")
    for line in reply.splitlines():
        if line.startswith(f"replica[{index}]:"):
            return line
    raise AssertionError(f"STATUS on the primary has no replica[{index}] line:\n{reply}")




def wait_until(predicate, timeout: float, interval: float = 0.3) -> bool:
    """Poll `predicate` until it is true or the budget runs out. Returns what it last saw."""
    deadline = time.monotonic() + timeout
    last = bool(predicate())
    while not last and time.monotonic() < deadline:
        time.sleep(interval)
        last = bool(predicate())
    return last


@pytest.fixture(scope="module")
def rotating_cluster():
    """A primary and a replica that rotate every 65573 bytes and flush every 200 ms.

    The short flush interval is not about flushing. **WAL truncation runs in the flush tick**, so it
    is what makes retention observable inside a test instead of once every production interval; and
    `ob_wal_file_index` is published from the same tick, which is what lets a test say "a retention
    pass has run since the rotation" rather than hope one has. It matters for a second reason on the
    replica: a replica **refuses `FLUSH`** — it is read-only — so its own tick is the only thing
    that turns what replication delivered into rows a `SELECT` can see.

    Module-scoped, because a cluster that has written a couple of hundred kilobytes across four WAL
    files is not a neighbour the session cluster's other tests deserve — the same reason
    `heavy_cluster` exists.
    """
    mgr = ClusterManager()
    mgr.extra_node_args = ["--wal-rotate-bytes", str(ROTATE_BYTES),
                           "--flush-interval-ms", "200"]
    mgr.start()
    try:
        yield mgr
    finally:
        mgr.shutdown()


@pytest.fixture(scope="module")
def held_cluster():
    """The same, plus a third replica that is stopped for the whole module.

    A test of catch-up **across** WAL files needs the primary to keep the files the reconnecting
    replica will ask for, and that is not a timer: `safe_truncate` is the smallest `confirmed_file`
    across connected replicas, so one stopped replica pins it in place. The alternative — a flush
    interval longer than the test — was measured and rejected: it stops retention, and it also stops
    the **replica's** rows from ever becoming queryable, because a replica refuses `FLUSH`. The first
    version of this module did exactly that and read zero rows out of a replica that had received
    every one of them.

    `nodes[2]` is the keeper and answers nothing while paused. `nodes[1]` is the replica under test.
    """
    mgr = ClusterManager()
    mgr.extra_node_args = ["--wal-rotate-bytes", str(ROTATE_BYTES),
                           "--flush-interval-ms", "200"]
    mgr.start()
    try:
        keeper = mgr.add_replica()
        mgr.pause_node(keeper.index)
        try:
            yield mgr
        finally:
            mgr.resume_node(keeper.index)
    finally:
        mgr.shutdown()


def test_a_replica_follows_the_primary_across_wal_rotations(rotating_cluster):
    """Every row survives a stream that crosses three file boundaries — and the boundaries happened.

    The second half is the control. "The replica has 1500 rows" passes just as well against a WAL
    that never rotated, which is what every replication test in this battery has measured until now,
    so the rotation is asserted from the primary's own directory and from its own gauge.
    """
    primary = rotating_cluster.primary()
    replica = rotating_cluster.replica()
    symbol = "ROT-STREAM"

    client = OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=60.0)
    try:
        insert_records(client, symbol, RECORDS)
        client.flush()
    finally:
        client.close()

    files = wal_files(primary.data_dir)
    highest = wal_index(files[-1])
    assert highest >= 2, (
        f"{RECORDS} records of {RECORD_BYTES} bytes against a {ROTATE_BYTES}-byte threshold should "
        f"have rotated at least twice; the primary's highest WAL file is {files[-1]} and it holds "
        f"{files}. Either the flag did not reach the writer or a record is not the size this "
        f"module assumes")

    # The same fact from the engine's own instrument rather than from its directory, and it is
    # published from the flush tick - so this is also how the test knows a retention pass has run
    # since the rotation, which is what the next test needs to be able to say.
    assert wait_until(
        lambda: metric(primary.metrics_port, "ob_wal_file_index") >= highest,
        timeout=patience(30)), (
        f"ob_wal_file_index says {metric(primary.metrics_port, 'ob_wal_file_index')} while the "
        f"primary's WAL is at {highest}")

    got = wait_for_rows(replica.tcp_port, symbol, RECORDS, timeout=patience(60))
    assert got == RECORDS, (
        f"the replica holds {got} of {RECORDS} rows after a stream across "
        f"{highest + 1} WAL files")


def test_retention_keeps_the_files_a_stopped_replica_still_needs(rotating_cluster):
    """`safe_truncate` is the slowest connected replica's file, and a stopped replica is connected.

    A killed replica leaves `replicas_`, so `safe_truncate` becomes the current file index and
    retention frees everything below it. A **stopped** one is still there and still counted, with
    its `confirmed_file` frozen, which is the only way to ask from the outside whether the promise
    holds. Three claims share that state:

    1. the WAL file the replica is in is **kept**, though retention is demonstrably running,
    2. the lag reported is **bigger than a whole file** — the number the subtraction #123 replaced
       could not produce, because it ignored the file index and clamped at zero,
    3. and no replica's distance is **unknown**, which is the same fact from the other side: the
       distance is measurable precisely because the files are still there.

    Claim 1 needs a control, and it is taken **before** the fault rather than after: retention is
    watched actually freeing a file in this cluster, with the replica keeping up. Without it, "the
    file is still here" also passes for a node whose retention never runs at all.

    **This test deliberately stops at the resume, and the reason is a defect it found.** A pause long
    enough to observe anything outlives the replica's socket timeout, so the replica reconnects; in
    that window it is not connected, retention advances, and its saved position is refused with
    `ERR WAL_TRUNCATED` — correct, and documented in `docs/operations.md`. What is not correct is
    what happens next: the snapshot bootstrap that refusal sends it to **cannot succeed**, because
    the manifest CRC covers two fields the wire does not carry. That is **#125**, and the test that
    a truncated replica comes back with every row belongs to it. What this one asserts instead is
    that the primary is unharmed, which is the other half of the promise.
    """
    primary = rotating_cluster.primary()
    replica = rotating_cluster.replica()          # taken before the pause: a stopped node answers
    symbol = "ROT-HELD"                           # nothing, so `replica()` cannot find it

    client = OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=60.0)
    try:
        # The control: retention is running here. One rotation's worth of records with the replica
        # keeping up, and the oldest file on disk has to move.
        oldest = wal_files(primary.data_dir)[0]
        insert_records(client, symbol, 600, base=400_000)
        assert wait_until(lambda: wal_files(primary.data_dir)[0] != oldest,
                          timeout=patience(30)), (
            f"the oldest WAL file is still {oldest} after a rotation with the replica keeping up, "
            f"so retention is not running in this cluster and the assertion below would prove "
            f"nothing. Files: {wal_files(primary.data_dir)}")

        held = f"wal_{replica_confirmed_file(primary.tcp_port):06d}.bin"
        rotating_cluster.pause_node(replica.index)

        insert_records(client, symbol, RECORDS, base=500_000)
        client.flush()
    finally:
        client.close()

    try:
        # A flush tick has to have run *after* the rotation, or "the files are still here" says
        # nothing about retention. The gauge is published from that tick.
        assert wait_until(lambda: metric(primary.metrics_port, "ob_wal_file_index") >= 2,
                          timeout=patience(30)), (
            f"ob_wal_file_index never passed 2 (saw "
            f"{metric(primary.metrics_port, 'ob_wal_file_index')})")

        connected = metric(primary.metrics_port, "ob_replicas_connected")
        assert connected == 1, (
            f"the paused replica is no longer counted as connected ({connected}), so retention is "
            f"free to remove everything below the current file and the claims below would pass or "
            f"fail for a reason this test is not about")

        line = replica_line(primary.tcp_port)
        kept = wal_files(primary.data_dir)
        assert held in kept, (
            f"retention removed {held}, which the replica still needs: it reports {line!r}. "
            f"Files now: {kept}")

        lag = metric(primary.metrics_port, "ob_replication_lag_bytes")
        assert lag > ROTATE_BYTES, (
            f"ob_replication_lag_bytes is {lag} for a replica more than a whole "
            f"{ROTATE_BYTES}-byte file behind ({line!r}). A lag that stops at a file boundary is "
            f"#123: the subtraction ignored the file index, so it clamped at zero")

        unknown = metric(primary.metrics_port, "ob_replicas_lag_unknown")
        assert unknown == 0, (
            f"{unknown} replica(s) report an unmeasurable distance while every file is still on "
            f"disk ({kept}). Unknown means a file between the two positions is gone, which says "
            f"that replica needs a snapshot")
    finally:
        rotating_cluster.resume_node(replica.index)

    # The other half of the promise: a replica that cannot be served does not cost the primary its
    # data. Read from the primary, which has been taking writes throughout.
    on_primary = len(data_rows(primary.tcp_port, symbol))
    assert on_primary == 600 + RECORDS, (
        f"the primary holds {on_primary} of {600 + RECORDS} rows it acknowledged")


def test_a_reconnecting_replica_is_caught_up_across_file_boundaries(held_cluster):
    """The catch-up scan itself crosses files, and the primary's own log is the evidence.

    This is #98's arithmetic end to end. A killed replica reconnects and asks `REPLICATE` from the
    position it saved; the primary streams from there to wherever its WAL is now, which here is
    three files further on. Before #98 the position on the wire was not the record's position at
    all, so a replica that had received forty records persisted the length of **one** — and no
    integration test could reach the case, because crossing a file needed 512 MB.

    Asserted on the log line `handle_catchup()` writes, not only on the row count, and the reason is
    the failure this module exists to prevent: a row count is also satisfied by a snapshot, by a
    scan that stayed inside one file, and by a replica that never lost anything. The line names
    `from_file` and `through_file`, so it says which of those happened.
    """
    primary = held_cluster.primary()
    replica = held_cluster.nodes[1]               # nodes[2] is the keeper, stopped by the fixture
    symbol = "ROT-CATCHUP"

    client = OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=60.0)
    try:
        # A first batch, so the replica has a position in file 0 to come back to.
        insert_records(client, symbol, 100, base=700_000)
        client.flush()
        assert wait_for_rows(replica.tcp_port, symbol, 100, timeout=patience(45)) == 100, (
            "the replica never received the first batch, so there is no position to resume from")

        # The position has to be on disk before the kill, or there is nothing to resume from:
        # `save_state()` runs on a ten-second timer, and a replica killed before its first save
        # comes back with no position at all - a different path from the one under test.
        state_path = os.path.join(replica.data_dir, "repl_state.txt")
        assert wait_until(lambda: os.path.exists(state_path), timeout=patience(30)), (
            f"the replica never wrote {state_path}, so a crash would leave it with no position to "
            f"resume from and the primary would be answering a different question")

        held_cluster.kill_node(replica.index)

        insert_records(client, symbol, RECORDS, base=800_000)
        client.flush()
    finally:
        client.close()

    files = wal_files(primary.data_dir)
    assert wal_index(files[-1]) >= 2, (
        f"the primary should have rotated at least twice while the replica was down; it has {files}")
    assert files[0] == "wal_000000.bin", (
        f"the file the replica will ask for is gone, so this test would measure the snapshot path "
        f"instead of the scan. The stopped keeper exists to prevent exactly that: {files}")

    held_cluster.restart_node(replica.index)
    replica = held_cluster.nodes[replica.index]

    total = 100 + RECORDS
    got = wait_for_rows(replica.tcp_port, symbol, total, timeout=patience(120))
    assert got == total, (
        f"the replica holds {got} of {total} rows after catching up across {len(files)} WAL "
        f"files:\n{tail_node_log(replica, 25)}")

    log = open(os.path.join(primary.data_dir, "node.log"), encoding="utf-8",
               errors="replace").read()
    assert "ERR WAL_TRUNCATED" not in log, (
        "the primary refused the replica's position, so what arrived came from a snapshot and this "
        "test measured nothing about the scan")

    crossing = []
    for line in log.splitlines():
        if "catchup for fd=" not in line:
            continue
        start = int(line.split("from_file=")[1].split(",")[0])
        through = int(line.split("through_file=")[1].split(",")[0])
        if through > start:
            crossing.append((start, through))
    assert crossing, (
        "no catch-up in the primary's log spans more than one WAL file, so the arithmetic this "
        "test is about was never exercised. Catch-ups seen: "
        + "; ".join(ln for ln in log.splitlines() if "catchup for fd=" in ln))
    assert max(t - s for s, t in crossing) >= 2, (
        f"the widest catch-up spanned one boundary, not two: {crossing}")
