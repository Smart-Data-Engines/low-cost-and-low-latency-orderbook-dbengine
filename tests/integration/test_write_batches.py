"""The writes of one read are one batch — stage 2b of #151, roadmap #155.

Every `INSERT` and `MINSERT` a client sends in one read is applied under **one** acquisition of the
engine's lock, and their WAL records reach the file with one `write()` per run between rotations.
A client that pipelines 64 writes used to cost the engine 64 acquisitions, 64 `write()` calls and,
under `--fsync-policy every`, 64 `fsync` calls; with four reactors that was a convoy on the engine's
mutex (futex calls 1 128 at one reactor, 1 485 158 at four).

What this module holds is that the batch is **invisible except in its cost**: that whatever the
disk does to the one `write()` a batch now is, each write gets the answer it would have got as a
command of its own, and that replicas, peers and the metrics see what they saw.

The fault tests aim the injector at the batch itself: `OB_FAULT_SIZE` is the byte count of the
whole batch's records, so the fault fires only if the batch really reached the WAL in one call -
which is the premise, asserted through the injector's own count rather than assumed.
"""

from __future__ import annotations

import os
import socket
import threading
import time

import pytest
from conftest import patience
from orderbook_engine import BookUpdate, OrderbookEngine
from test_storage_faults import DELTA_BYTES, FaultNode, WAL_SEGMENT

pytestmark = pytest.mark.smoke

custom_metrics: dict = {}

# One batch of this many one-level inserts is this many bytes of WAL, and that is the size the
# injector is told to fail.
BATCH = 8
BATCH_BYTES = str(BATCH * int(DELTA_BYTES))


def pipelined(port: int, commands: list[str]) -> list[str]:
    """Every command in one `sendall`, so one read on the node and one batch, then one reply per
    command read by its own terminator: `OK` ends in a blank line, `ERR` in one newline."""
    replies: list[str] = []
    with socket.create_connection(("127.0.0.1", port), timeout=patience(15)) as sock:
        reader = sock.makefile("rb")
        while reader.readline().strip():   # the banner ends in a blank line
            pass
        sock.sendall(("\n".join(commands) + "\n").encode())
        for _ in commands:
            line = reader.readline().decode(errors="replace").rstrip("\n")
            if line == "OK":
                reader.readline()
            replies.append(line or "<the node closed the connection>")
    return replies


def inserts(prices: list[int]) -> list[str]:
    return [f"INSERT SYM EX bid {price} 1 1" for price in prices]


def stored_prices(port: int, symbol: str = "SYM") -> list[int]:
    with OrderbookEngine(host="127.0.0.1", port=port, timeout=patience(30)) as client:
        rows = client.query(f"SELECT * FROM '{symbol}'.'EX' "
                            f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")
    return sorted(row.price for row in rows)


def test_a_batch_whose_write_is_refused_costs_the_write_it_stopped_at():
    """A `write()` refused with nothing written refuses the record it stopped at - here the first -
    and the records behind it are written by the next `write()`, so each write is answered as it
    would have been as a command of its own: the next command after a refused one was tried."""
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_SIZE=BATCH_BYTES, OB_FAULT_COUNT="1",
                     OB_FAULT_FLUSH_MS="3600000")
    try:
        node.wait_until_answering()
        prices = [100 * (i + 1) for i in range(BATCH)]
        replies = pipelined(node.port, inserts(prices))

        # The premise: the injector fails only a write of exactly BATCH records, so one injection
        # is the proof that the batch reached the WAL in one call.
        assert node.injections() == 1, (
            f"no write of {BATCH_BYTES} bytes was made, so the batch was not one write: "
            f"{replies}\n{node.fault_log_text()}")
        assert replies[0].startswith("ERR") and "No space left" in replies[0], replies
        assert all(r == "OK" for r in replies[1:]), (
            f"the writes behind the refused one were not tried again: {replies}")

        node.kill_and_restart_without_faults()
        assert stored_prices(node.port) == prices[1:], (
            "what the restart replays is not what was acknowledged")
    finally:
        node.cleanup()


def test_a_batch_torn_in_the_middle_costs_only_the_record_that_tore():
    """#126 inside a batch: the `write()` is cut part way through its third record and the rest is
    refused. The two records before the cut are in the file and acknowledged, the cut one is
    refused, the file is abandoned, and the five behind it are written into the next file."""
    cut = 2 * int(DELTA_BYTES) + 50
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_SIZE=BATCH_BYTES, OB_FAULT_COUNT="1",
                     OB_FAULT_SHORT=str(cut), OB_FAULT_SHORT_THEN_FAIL="1",
                     OB_FAULT_FLUSH_MS="3600000")
    try:
        node.wait_until_answering()
        prices = [100 * (i + 1) for i in range(BATCH)]
        replies = pipelined(node.port, inserts(prices))

        assert node.injections() == 2, f"the batch was not cut and left cut: {replies}"
        assert "action=fail-remainder" in node.fault_log_text(), node.fault_log_text()
        assert replies[:2] == ["OK", "OK"], f"a record wholly in the file was refused: {replies}"
        assert replies[2].startswith("ERR"), f"the record the write tore was acknowledged: {replies}"
        assert all(r == "OK" for r in replies[3:]), (
            f"the records behind the tear were not written to the next file: {replies}")

        node.kill_and_restart_without_faults()
        assert stored_prices(node.port) == prices[:2] + prices[3:], (
            f"what the restart replays is not what was acknowledged:\n{node.log()[-2000:]}")
        assert node.wal_files() == ["wal_000000.bin", "wal_000001.bin"], (
            f"the writer did not abandon the file it tore: {node.wal_files()}")
    finally:
        node.cleanup()


def test_a_failed_sync_of_a_batch_is_not_answered_with_ok():
    """#113 for a batch: under `every`, one `fsync` covers the batch's run, so when it fails no
    write of the run is answered `OK` - and the batch after it, whose sync succeeds, is."""
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="fsync", OB_FAULT_ERRNO="EIO",
                     OB_FAULT_COUNT="1", OB_FAULT_FLUSH_MS="3600000")
    try:
        node.wait_until_answering()
        first = [100 * (i + 1) for i in range(BATCH)]
        second = [10_000 + 100 * (i + 1) for i in range(BATCH)]
        refused = pipelined(node.port, inserts(first))
        accepted = pipelined(node.port, inserts(second))

        assert node.injections() == 1, f"no sync was made to fail: {refused}"
        assert all(r.startswith("ERR") and "fsync failed" in r for r in refused), (
            f"a write whose sync failed was answered as durable under fsync-policy=every: "
            f"{refused}")
        assert all(r == "OK" for r in accepted), f"the next batch was refused: {accepted}"

        node.kill_and_restart_without_faults()
        prices = stored_prices(node.port)
        assert all(p in prices for p in second), "an acknowledged write did not survive"
        # Not asserted either way, and reported: the refused records are in the WAL, because a
        # record is written before it is synced - the cost `docs/operations.md` states for a
        # failed sync, where a client that resends stores the write twice.
        custom_metrics["refused_by_sync_and_replayed"] = sum(1 for p in first if p in prices)
    finally:
        node.cleanup()


def test_every_acknowledged_write_of_a_stream_of_batches_survives_a_kill():
    """`SIGKILL` in the middle of a stream of pipelined batches, then a replay: every write that was
    answered `OK` is back. Under `every` an `OK` is a synced record, so this is the promise the
    batch has to keep while it is being kept once per run instead of once per write."""
    node = FaultNode(OB_FAULT_POLICY="every", OB_FAULT_FLUSH_MS="3600000")
    acknowledged: list[int] = []
    try:
        node.wait_until_answering()
        stop = threading.Event()

        def stream() -> None:
            price = 1
            try:
                with socket.create_connection(("127.0.0.1", node.port),
                                              timeout=patience(15)) as sock:
                    reader = sock.makefile("rb")
                    while reader.readline().strip():
                        pass
                    while not stop.is_set():
                        batch = list(range(price, price + 64))
                        sock.sendall(("\n".join(inserts(batch)) + "\n").encode())
                        for p in batch:
                            line = reader.readline().decode(errors="replace").rstrip("\n")
                            if not line:
                                return
                            if line == "OK":
                                reader.readline()
                                acknowledged.append(p)
                        price += 64
            except OSError:
                return

        writer = threading.Thread(target=stream, daemon=True)
        writer.start()
        deadline = time.time() + patience(30)
        while len(acknowledged) < 2_000 and time.time() < deadline and writer.is_alive():
            time.sleep(0.05)
        assert len(acknowledged) >= 2_000, f"the stream stalled at {len(acknowledged)} writes"

        node.proc.kill()        # mid-stream: the writer is in the middle of some batch
        node.proc.wait(timeout=10)
        stop.set()
        writer.join(timeout=patience(10))

        node.kill_and_restart_without_faults()
        prices = set(stored_prices(node.port))
        missing = [p for p in acknowledged if p not in prices]
        custom_metrics["acknowledged_before_the_kill"] = len(acknowledged)
        assert not missing, (
            f"{len(missing)} acknowledged write(s) were not replayed, first {missing[:5]}")
        assert "checksum mismatch" not in node.log(), node.log()[-2000:]
    finally:
        node.cleanup()


def test_a_batch_counts_each_write_in_the_metrics():
    """The metrics mean what they meant per write: one batch of sixteen is sixteen inserts and
    sixteen latency observations, updated once rather than sixteen times."""
    node = FaultNode(OB_FAULT_POLICY="interval")
    try:
        node.wait_until_answering()
        before = node.counter("ob_total_inserts")
        before_obs = node.counter("ob_insert_latency_seconds_count")
        replies = pipelined(node.port, inserts([100 * (i + 1) for i in range(16)]))
        assert replies == ["OK"] * 16, replies
        assert node.counter("ob_total_inserts") - before == 16
        assert node.counter("ob_insert_latency_seconds_count") - before_obs == 16
    finally:
        node.cleanup()


def test_a_replica_receives_every_record_of_every_batch_in_order(cluster):
    """Replication with batches: each record goes to the replicas with its own position, in WAL
    order, under the batch's lock - so the replica holds the primary's rows and the primary's
    numbers, row for row."""
    symbol = f"WB{os.getpid()}R"
    primary = cluster.primary()
    replica = cluster.replica()
    with OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=patience(30)) as client:
        updates = [BookUpdate(symbol=symbol, exchange="EX", side="bid" if i % 2 else "ask",
                              prices=[1_000 + i], qtys=[1 + i % 7])
                   for i in range(640)]
        for start in range(0, len(updates), 64):
            outcomes = client.insert_batch(updates[start:start + 64])
            assert all(o.ok for o in outcomes), [o for o in outcomes if not o.ok][:3]
        # A query reads what has been flushed, and a replica refuses FLUSH - it flushes on its own
        # interval, which is what the wait below is for.
        client.flush()
        on_primary = client.query(f"SELECT * FROM '{symbol}'.'EX' "
                                  f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")
    assert len(on_primary) == len(updates)

    deadline = time.time() + patience(30)
    on_replica: list = []
    while time.time() < deadline:
        with OrderbookEngine(host="127.0.0.1", port=replica.tcp_port,
                             timeout=patience(30)) as client:
            on_replica = client.query(f"SELECT * FROM '{symbol}'.'EX' "
                                      f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")
        if len(on_replica) >= len(on_primary):
            break
        time.sleep(0.3)

    key = lambda r: (r.sequence_number, r.timestamp_ns, r.price, r.quantity, r.side)  # noqa: E731
    assert sorted(map(key, on_replica)) == sorted(map(key, on_primary)), (
        f"the replica holds {len(on_replica)} rows against the primary's {len(on_primary)}")
    assert sorted(r.sequence_number for r in on_primary) == list(range(1, len(updates) + 1)), (
        "the batch did not number its writes one after another")


def test_peers_receive_every_write_of_every_batch(healthy_mm_cluster):
    """Multi-master with batches: each write gets its own HLC tick and its own number, and the peers
    are told after the lock is released, in the order of the writes (#80). Every peer ends up with
    every row, under the writer's numbers."""
    symbol = f"WB{os.getpid()}M"
    writer = healthy_mm_cluster.nodes[0]
    with OrderbookEngine(host="127.0.0.1", port=writer.tcp_port, timeout=patience(30)) as client:
        updates = [BookUpdate(symbol=symbol, exchange="EX", side="bid",
                              prices=[2_000 + i, 1_000 + i], qtys=[1, 2]) for i in range(256)]
        for start in range(0, len(updates), 64):
            outcomes = client.insert_batch(updates[start:start + 64])
            assert all(o.ok for o in outcomes), [o for o in outcomes if not o.ok][:3]
        client.flush()   # a query reads what has been flushed; the peers flush on their interval
        written = client.query(f"SELECT * FROM '{symbol}'.'EX' "
                               f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")
    assert len(written) == 2 * len(updates)

    key = lambda r: (r.sequence_number, r.timestamp_ns, r.price, r.quantity)  # noqa: E731
    for peer in healthy_mm_cluster.nodes[1:]:
        deadline = time.time() + patience(45)
        held: list = []
        while time.time() < deadline:
            with OrderbookEngine(host="127.0.0.1", port=peer.tcp_port,
                                 timeout=patience(30)) as client:
                held = client.query(f"SELECT * FROM '{symbol}'.'EX' "
                                    f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")
            if len(held) >= len(written):
                break
            time.sleep(0.5)
        assert sorted(map(key, held)) == sorted(map(key, written)), (
            f"a peer holds {len(held)} rows against the writer's {len(written)}")
