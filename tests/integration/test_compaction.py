"""A node merges a symbol's small segments, and a crash in any step keeps each row once (#165 2b).

The flush tick merges eight sealed segments of a symbol into one: written into a working directory
(`<start>_<end>.compacting`), synced, published by a rename under the index's lock in place of its
inputs, synced again, and only then are the inputs removed. A crash between any two of those leaves
the inputs, the merged segment naming them, or both - and a start keeps each row once: it removes a
working directory, and the inputs it finds beside the merged segment that names them.

These run their own node because they kill it, like `test_lazy_flush.py`, and each asserts what was
on the disk at the moment of the kill: a test that passes whatever the kill interrupted proves
nothing about the state it meant to interrupt. No fault injector: the states are waited for on the
disk, one tick wide each at `--flush-interval-ms 50`.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import tempfile
import threading
import time

import pytest
from conftest import free_port, patience, server_binary_path

pytestmark = pytest.mark.crash_recovery

SERVER = server_binary_path()
FAN_IN = 8       # compaction::kFanIn
LEVELS = 40


def request(port: int, command: str, timeout: float = 30.0) -> list[str]:
    """One command and its answer, a line at a time up to the blank line that ends it."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        reader = s.makefile("rb")
        while reader.readline().strip():   # the greeting
            pass
        s.sendall((command + "\n").encode())
        out: list[str] = []
        while True:
            line = reader.readline()
            if not line:
                raise AssertionError(f"the node closed the connection inside the answer to "
                                     f"{command.splitlines()[0]!r}")
            text = line.decode(errors="replace").rstrip("\n")
            if text == "":
                return out
            out.append(text)
            if len(out) == 1 and (text.startswith("ERR") or text == "PONG"):
                return out


class Node:
    """One standalone server on a data directory the caller owns, logging to a file in it."""

    def __init__(self, data_dir: str, extra: list[str] | None = None):
        self.data_dir = data_dir
        self.extra = extra or []
        self.port = 0
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        self.port = free_port()
        log = open(os.path.join(self.data_dir, "node.log"), "a", encoding="utf-8")
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", self.data_dir, "--metrics-port", "0",
             "--flush-interval-ms", "50", "--drain-timeout-ms", "1000", *self.extra],
            stdout=log, stderr=subprocess.STDOUT)
        log.close()
        deadline = time.time() + patience(30)
        while time.time() < deadline:
            try:
                if request(self.port, "PING", timeout=2) == ["PONG"]:
                    return
            except OSError:
                pass
            time.sleep(0.1)
        self.proc.kill()
        self.proc.wait(timeout=10)
        pytest.fail(f"the node never answered on {self.port}; log tail:\n{self.log()[-1500:]}")

    def log(self) -> str:
        with open(os.path.join(self.data_dir, "node.log"), encoding="utf-8", errors="replace") as f:
            return f.read()

    def minsert(self, symbol: str, first_price: int, levels: int = LEVELS) -> None:
        body = "\n".join(f"{first_price + i} 1 1" for i in range(levels))
        reply = request(self.port, f"MINSERT {symbol} EX bid {levels}\n{body}")
        assert reply and reply[0].startswith("OK"), reply

    def flush(self) -> None:
        reply = request(self.port, "FLUSH")
        assert reply and reply[0].startswith("OK"), reply

    def rows(self, symbol: str) -> list[str]:
        reply = request(self.port, f"SELECT * FROM '{symbol}'.'EX'")
        if reply and reply[0].startswith("ERR") and "not found" in reply[0]:
            return []
        assert reply and reply[0] == "OK", reply
        return reply[2:]

    def rows_once_serving(self, symbol: str) -> list[str] | None:
        """The rows, or None while the node answers `ERR bootstrapping` - a replica installing a
        snapshot says so rather than answer from a store it is replacing."""
        reply = request(self.port, f"SELECT * FROM '{symbol}'.'EX'")
        if reply and reply[0].startswith("ERR") and "bootstrapping" in reply[0]:
            return None
        if reply and reply[0].startswith("ERR") and "not found" in reply[0]:
            return []
        assert reply and reply[0] == "OK", reply
        return reply[2:]

    def prices(self, symbol: str) -> list[int]:
        """The price of every row a `SELECT *` answers, in its order - the column found by the
        header's name rather than by a position this test would have to keep in step."""
        reply = request(self.port, f"SELECT * FROM '{symbol}'.'EX'")
        if reply and reply[0].startswith("ERR") and "not found" in reply[0]:
            return []
        assert reply and reply[0] == "OK" and len(reply) >= 2, reply
        column = reply[1].split("\t").index("price")
        return [int(line.split("\t")[column]) for line in reply[2:]]

    def kill(self) -> None:
        """SIGKILL: no drain, no seal, no destructor - an actual crash."""
        assert self.proc is not None and self.proc.poll() is None, "the node died before the kill"
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


def symbol_dir(data_dir: str, symbol: str) -> str:
    return os.path.join(data_dir, symbol, "EX")


def segment_dirs(data_dir: str, symbol: str) -> list[str]:
    """The published segments of a symbol, by directory name."""
    try:
        names = os.listdir(symbol_dir(data_dir, symbol))
    except FileNotFoundError:
        return []
    return sorted(n for n in names if not n.endswith(".compacting")
                  and os.path.exists(os.path.join(symbol_dir(data_dir, symbol), n, "meta.json")))


def working_dirs(data_dir: str, symbol: str) -> list[str]:
    try:
        return sorted(n for n in os.listdir(symbol_dir(data_dir, symbol)) if n.endswith(".compacting"))
    except FileNotFoundError:
        return []


def merged(data_dir: str, symbol: str) -> dict[str, list[str]]:
    """Each published merged segment, by name, with the inputs its meta.json names."""
    out: dict[str, list[str]] = {}
    for name in segment_dirs(data_dir, symbol):
        try:
            with open(os.path.join(symbol_dir(data_dir, symbol), name, "meta.json")) as f:
                meta = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            continue
        if "compacted_from" in meta:
            out[name] = [entry["dir"] for entry in meta["compacted_from"]]
    return out


def eventually(done, within: float = 20.0, step: float = 0.05) -> bool:
    deadline = time.time() + patience(within)
    while time.time() < deadline:
        if done():
            return True
        time.sleep(step)
    return done()


def seal_n(node: Node, symbol: str, n: int, first: int = 0, levels: int = LEVELS) -> None:
    """`n` seals of one symbol: a MINSERT of `levels` rows and a FLUSH, each."""
    for i in range(first, first + n):
        node.minsert(symbol, first_price=1_000_000 + i * 1000, levels=levels)
        node.flush()


def lowest_wal_file(data_dir: str) -> int:
    """The index of the oldest WAL file a node still holds."""
    indices = [int(n[len("wal_"):-len(".bin")]) for n in os.listdir(data_dir)
               if n.startswith("wal_") and n.endswith(".bin") and n[len("wal_"):-len(".bin")].isdigit()]
    assert indices, f"no WAL file in {data_dir}: {sorted(os.listdir(data_dir))}"
    return min(indices)


@pytest.fixture
def node():
    with tempfile.TemporaryDirectory(prefix="ob_compaction_") as d:
        n = Node(d)
        try:
            n.start()
            yield n
        finally:
            n.stop()


def test_eight_flushes_merge_into_one_segment_and_every_row_answers_once_in_order(node):
    seal_n(node, "CMA", FAN_IN)
    answered = node.rows("CMA")
    assert len(answered) == FAN_IN * LEVELS
    assert eventually(lambda: len(segment_dirs(node.data_dir, "CMA")) == 1), (
        f"{segment_dirs(node.data_dir, 'CMA')}: the eight segments were not merged, or their "
        "directories were not removed")
    assert not working_dirs(node.data_dir, "CMA")
    [(name, inputs)] = merged(node.data_dir, "CMA").items()
    assert len(inputs) == FAN_IN, inputs
    assert node.rows("CMA") == answered, "the merge changed what a SELECT answers, or its order"


def test_a_kill_while_a_merge_waits_for_its_sync_keeps_every_row_once(node):
    seal_n(node, "CMW", FAN_IN)
    answered = node.rows("CMW")
    # Written and not yet published: a tick wide.
    assert eventually(lambda: bool(working_dirs(node.data_dir, "CMW")), within=10, step=0.001), (
        "the merge's working directory was never seen")
    node.kill()
    assert working_dirs(node.data_dir, "CMW"), "the kill came after the merge was published"
    assert len(segment_dirs(node.data_dir, "CMW")) == FAN_IN, "an input went before its merge was"
    node.start()
    # Said by the start, not read off the directory: the first ticks after it merge the same eight
    # inputs again, into a working directory of the same name.
    assert "removed 1 working director(ies) nothing published" in node.log(), (
        "the start did not remove the unpublished merge")
    assert node.rows("CMW") == answered, "a row was lost, doubled or reordered by the crash"
    # And the inputs merge again, once the node is running.
    assert eventually(lambda: len(segment_dirs(node.data_dir, "CMW")) == 1
                      and not working_dirs(node.data_dir, "CMW"))
    assert node.rows("CMW") == answered


def test_a_kill_after_a_merge_is_published_and_before_its_inputs_go_keeps_every_row_once(node):
    seal_n(node, "CMP", FAN_IN)
    answered = node.rows("CMP")

    def published_beside_inputs() -> bool:
        m = merged(node.data_dir, "CMP")
        present = set(segment_dirs(node.data_dir, "CMP"))
        return any(any(i in present for i in inputs) for inputs in m.values())

    assert eventually(published_beside_inputs, within=10, step=0.001), (
        "the merged segment was never seen beside its inputs")
    node.kill()
    m = merged(node.data_dir, "CMP")
    present = set(segment_dirs(node.data_dir, "CMP"))
    left = [i for inputs in m.values() for i in inputs if i in present]
    assert m and left, "the kill came after the inputs were removed, so this tests nothing"
    node.start()
    assert set(segment_dirs(node.data_dir, "CMP")) == set(m), (
        "the start kept an input its merged segment holds the rows of")
    assert f"and {len(left)} segment(s) a merged segment beside them had replaced" in node.log()
    assert node.rows("CMP") == answered, "a row was lost, doubled or reordered by the crash"


def test_queries_while_merges_run_see_each_row_once_and_in_order(node):
    batches = 5 * FAN_IN
    # Batches whose FLUSH has answered: what every later read must hold. A write alone is not - a
    # SELECT reads what the flush tick has drained, which an acknowledged write may not yet be.
    flushed = [0]
    failures: list[str] = []

    def writer() -> None:
        try:
            for i in range(batches):
                node.minsert("CMQ", first_price=1_000_000 + i * 1000)
                node.flush()
                flushed[0] = i + 1
        except Exception as e:   # reported below, not lost on this thread
            failures.append(repr(e))

    expected = [1_000_000 + i * 1000 + j for i in range(batches) for j in range(LEVELS)]
    t = threading.Thread(target=writer)
    t.start()
    reads = 0
    while t.is_alive() or reads == 0:
        before = flushed[0]
        prices = node.prices("CMQ")
        reads += 1
        assert len(prices) == len(set(prices)), f"read {reads}: a row answered twice"
        assert prices == expected[:len(prices)], f"read {reads}: rows out of their order"
        assert len(prices) >= before * LEVELS, (
            f"read {reads}: {len(prices)} rows where {before * LEVELS} were flushed before it")
    t.join()
    assert not failures, failures
    assert eventually(lambda: node.rows("CMQ") and
                      len(segment_dirs(node.data_dir, "CMQ")) < batches // 2)
    assert node.prices("CMQ") == expected


def test_a_replica_bootstrapped_while_the_primary_merges_holds_every_row_once():
    with tempfile.TemporaryDirectory(prefix="ob_compaction_p_") as pdir, \
            tempfile.TemporaryDirectory(prefix="ob_compaction_r_") as rdir:
        repl_port = free_port()
        # Small WAL files, so retention has taken the first ones by the time the replica asks, and
        # the primary answers WAL_TRUNCATED: the replica bootstraps from a snapshot.
        primary = Node(pdir, ["--replication-port", str(repl_port), "--wal-rotate-bytes", "65573",
                              "--log-level", "DEBUG"])
        replica = Node(rdir, ["--primary-host", "127.0.0.1", "--primary-port", str(repl_port)])
        try:
            primary.start()
            symbols = [f"CMR{i}" for i in range(4)]
            first_file = lowest_wal_file(pdir)
            for s in symbols:
                # A thousand rows a record, so the WAL rotates many times over.
                seal_n(primary, s, FAN_IN + 3, levels=1000)
            assert eventually(lambda: all(merged(pdir, s) for s in symbols)), "nothing merged"
            assert eventually(lambda: lowest_wal_file(pdir) > first_file), (
                "the primary still holds its first WAL file, so the replica would stream the log "
                "rather than bootstrap from a snapshot")
            stop = threading.Event()

            def keep_writing() -> None:
                i = FAN_IN + 3
                while not stop.is_set():
                    for s in symbols:
                        primary.minsert(s, first_price=1_000_000 + i * 1000, levels=1000)
                        primary.flush()
                    i += 1

            writer = threading.Thread(target=keep_writing)
            writer.start()
            try:
                replica.start()
                time.sleep(2)
            finally:
                stop.set()
                writer.join()
            for s in symbols:
                want = sorted(primary.rows(s))
                assert eventually(lambda: sorted(replica.rows_once_serving(s) or []) == want,
                                  within=60), (
                    f"{s}: the replica holds {len(replica.rows_once_serving(s) or [])} row(s) of "
                    f"the primary's {len(want)}")
                assert len(set(want)) == len(want)
            log = primary.log()
            assert "segment files pinned for a snapshot" in log, "no snapshot was taken"
            assert "released its pin on the segment files" in log
            assert "SNAPSHOT_FAILED" not in log and "file_read_error" not in log
        finally:
            replica.stop()
            primary.stop()
