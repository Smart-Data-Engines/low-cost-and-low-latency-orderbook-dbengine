"""A segment's time range is the range of its rows (#166), through the wire.

Until #166 a segment recorded `[the start of the period its first row fell in, the time of its last
row]`, and queries prune by that range while retention deletes by its end. Rows arrive in the order
they were written, which is time order only while the server stamps every one: a client giving its
own event times (#105) can send them in any order. Measured on the m9g.xlarge before the fix, on
master and on #164's branch alike:

- rows at T+2 s, then T+1 s: the segment ended at T+1 s, and a `SELECT` for T+1.5..3 s answered
  `OK` with **nothing**, while a `SELECT` of everything returned both rows;
- a row five seconds into an hour, then one from a minute before it: the segment's directory was
  `1790186400000000000_1790186340000000000` - it started after it ended - and a `SELECT` of the
  minute before the hour answered nothing;
- under `--ttl-hours 24`, a row written now and then one dated two days back: **0 of 2** after the
  first sweep, and the log said `its newest row is 24.0 h past the retention` of a segment holding
  a row one second old.

And one thing the fix must not change, pinned here because only a restart can reach it: replay's
fallback for a symbol with no trusted WAL position compares a record's time with the time of each
segment's **last** row, as it always did.

Standalone nodes, because every test here restarts the process it measures or needs a flag the
shared cluster does not have; the binary from `server_binary_path()` (pitfall 77).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import pytest
from conftest import fault_injector_path, free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()
SECOND = 10**9
HOUR = 3600 * SECOND


def request(port: int, command: str, timeout: float = 10.0) -> str:
    """One command and its whole response, which ends with an empty line - except `PONG` and an
    `ERR`, which are one line and nothing after it."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        reader = s.makefile("rb")
        while reader.readline().strip():   # the greeting
            pass
        s.sendall((command + "\n").encode())
        if command == "PING":
            return reader.readline().decode(errors="replace")
        lines = []
        while True:
            line = reader.readline()
            if not line:
                raise AssertionError(f"the node closed the connection inside the answer to {command!r}")
            if not line.strip():
                return "".join(lines)
            if not lines and line.startswith(b"ERR "):
                return line.decode(errors="replace")
            lines.append(line.decode(errors="replace"))


class Node:
    """One standalone server on a data directory the caller owns, logging to a file in it."""

    def __init__(self, data_dir: str, extra: list[str] | None = None,
                 fault: dict[str, str] | None = None):
        self.data_dir = data_dir
        self.port = free_port()
        self._log = open(os.path.join(data_dir, "node.log"), "a", encoding="utf-8", buffering=1)
        env = {k: v for k, v in os.environ.items() if not k.startswith("OB_FAULT_")}
        env.pop("LD_PRELOAD", None)
        if fault:
            injector = fault_injector_path()
            assert injector is not None, (
                "libobfault.so was not built. Failing rather than skipping: a fault test that "
                "injects nothing is indistinguishable from an engine that survives the fault")
            env["LD_PRELOAD"] = injector
            env.update(fault)
            env["OB_FAULT_LOG"] = os.path.join(data_dir, "fault.log")
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", data_dir, "--metrics-port", "0",
             "--drain-timeout-ms", "1000", *(extra or [])],
            stdout=self._log, stderr=subprocess.STDOUT, env=env)
        deadline = time.time() + patience(20)
        while time.time() < deadline:
            try:
                if "PONG" in request(self.port, "PING", timeout=2):
                    return
            except OSError:
                pass
            time.sleep(0.2)
        pytest.fail(f"the node never answered on {self.port}; log tail:\n{self.log()[-800:]}")

    def log(self) -> str:
        with open(os.path.join(self.data_dir, "node.log"), encoding="utf-8", errors="replace") as f:
            return f.read()

    def insert(self, symbol: str, price: int, event_time_ns: int) -> None:
        reply = request(self.port, f"INSERT {symbol} EX bid {price} 1 1 {event_time_ns}")
        assert reply.strip() == "OK", reply

    def flush(self) -> None:
        assert request(self.port, "FLUSH", timeout=30).startswith("OK")

    def prices(self, symbol: str, lo: int = 0, hi: int = 2**63) -> list[int]:
        """The prices a time-range `SELECT` returns, in the order it returns them. A symbol this
        node does not know answers `ERR ... not found`, which is no rows here."""
        reply = request(self.port, f"SELECT timestamp, price FROM '{symbol}'.'EX' "
                                   f"WHERE timestamp BETWEEN {lo} AND {hi}")
        if reply.startswith("ERR ") and "not found" in reply:
            return []
        assert not reply.startswith("ERR "), reply
        out = []
        for line in reply.splitlines():
            fields = line.split("\t")
            if len(fields) == 2 and fields[0].isdigit():
                out.append(int(fields[1]))
        return out

    def status(self, field: str) -> int:
        match = re.search(rf"^{field}: (\d+)", request(self.port, "STATUS"), re.M)
        assert match, f"STATUS has no {field}"
        return int(match.group(1))

    def stop(self) -> None:
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=patience(20))
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self._log.close()

    def kill(self) -> None:
        """`SIGKILL`: a clean stop ends in a checkpoint, and a replay test needs a WAL tail."""
        self.proc.kill()
        self.proc.wait()
        self._log.close()


@pytest.fixture
def data_dir():
    path = tempfile.mkdtemp(prefix="ob_time_range_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def ten_minutes_into_an_hour_back(hours: int) -> int:
    """An event time well inside an hour, `hours` hours before now - so nothing here is near a
    period's boundary unless a test puts it there."""
    return (time.time_ns() // HOUR - hours) * HOUR + 600 * SECOND


def segment_dirs(data_dir: str, symbol: str) -> list[str]:
    root = os.path.join(data_dir, symbol, "EX")
    return sorted(os.listdir(root)) if os.path.isdir(root) else []


def meta_path(data_dir: str, symbol: str) -> str:
    dirs = segment_dirs(data_dir, symbol)
    assert len(dirs) == 1, f"{symbol} was expected to be one segment: {dirs}"
    return os.path.join(data_dir, symbol, "EX", dirs[0], "meta.json")


def written_before_the_fix(meta_file: str, start: int, end: int) -> None:
    """Rewrite a segment's `meta.json` into what the writer before #166 produced: no
    `last_row_ts_ns`, no `time_range`, and the range it recorded - the start of the first row's
    period and the last row's time."""
    with open(meta_file, encoding="utf-8") as f:
        meta = json.load(f)
    assert meta.get("time_range") == "rows", f"premise: this segment was written by the fix: {meta}"
    meta.pop("time_range")
    meta.pop("last_row_ts_ns")
    meta["start_ts_ns"] = start
    meta["end_ts_ns"] = end
    with open(meta_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(meta, separators=(",", ":")))


def test_a_row_written_after_a_later_one_is_found_by_a_query_for_its_time(data_dir):
    base = ten_minutes_into_an_hour_back(2)
    node = Node(data_dir)
    try:
        node.insert("AAA", 100, base + 2 * SECOND)
        node.insert("AAA", 101, base + 1 * SECOND)
        node.flush()
        assert sorted(node.prices("AAA")) == [100, 101], "control: both rows are stored"
        assert node.prices("AAA", base + SECOND + SECOND // 2, base + 3 * SECOND) == [100], (
            f"a query for the row written first, and later in time, did not find it: "
            f"segments {segment_dirs(data_dir, 'AAA')}")
    finally:
        node.stop()


def test_a_late_row_from_the_hour_before_is_found_by_a_query_for_its_time(data_dir):
    boundary = ten_minutes_into_an_hour_back(2) // HOUR * HOUR + HOUR
    node = Node(data_dir)
    try:
        node.insert("BBB", 200, boundary + 5 * SECOND)
        node.insert("BBB", 201, boundary - 60 * SECOND)
        node.flush()
        assert sorted(node.prices("BBB")) == [200, 201], "control: both rows are stored"
        assert node.prices("BBB", boundary - 61 * SECOND, boundary - 1) == [201], (
            f"a query for the minute before the hour did not find the row from it: "
            f"segments {segment_dirs(data_dir, 'BBB')}")
        start, end = (int(x) for x in segment_dirs(data_dir, "BBB")[0].split("_")[:2])
        assert start <= end, "a segment's directory says it starts after it ends"
    finally:
        node.stop()


def test_retention_keeps_a_current_row_written_before_an_old_one(data_dir):
    """The measured loss, and its control on the same node in the same sweep.

    `NEW` gets a row now and then one from two days ago: one segment, because only a later period
    rolls a segment over. `OLD` gets them the other way round, so the old row starts a period of its
    own, the current one rolls it over, and retention deletes that segment - which is what says a
    sweep ran after the flush; without it, finding both of `NEW`'s rows proves nothing.
    """
    now = time.time_ns()
    old = now - 48 * HOUR
    node = Node(data_dir, ["--ttl-hours", "24", "--ttl-scan-interval-seconds", "1"])
    try:
        node.insert("NEW", 100, now)
        node.insert("NEW", 101, old)
        node.insert("OLD", 300, old)
        node.insert("OLD", 301, now)
        node.flush()
        deadline = time.time() + patience(20)
        while node.status("ttl_segments_deleted") < 1 and time.time() < deadline:
            time.sleep(0.2)
        assert node.status("ttl_segments_deleted") >= 1, "premise: no sweep deleted anything"
        assert node.prices("OLD") == [301], "control: retention deletes a segment past the window"
        assert sorted(node.prices("NEW")) == [100, 101], (
            "retention deleted a row younger than the retention, with an older one written after it")
    finally:
        node.stop()


def test_a_segment_written_before_the_fix_is_repaired_at_start(data_dir):
    base = ten_minutes_into_an_hour_back(2)
    node = Node(data_dir)
    node.insert("AAA", 100, base + 2 * SECOND)
    node.insert("AAA", 101, base + 1 * SECOND)
    node.flush()
    node.stop()
    meta_file = meta_path(data_dir, "AAA")
    written_before_the_fix(meta_file, base // HOUR * HOUR, base + SECOND)

    node = Node(data_dir)
    try:
        assert node.prices("AAA", base + SECOND + SECOND // 2, base + 3 * SECOND) == [100], (
            "a segment written before the fix still hides the row outside its recorded range")
        assert re.search(r"1 segment\(s\) written before #166 were read for their time range in "
                         r"\d+ ms: 1 held rows outside the range they recorded .*1 repaired on disk",
                         node.log()), node.log()[-1500:]
        with open(meta_file, encoding="utf-8") as f:
            meta = json.load(f)
        assert meta["time_range"] == "rows", meta
        assert (meta["start_ts_ns"], meta["end_ts_ns"]) == (base + SECOND, base + 2 * SECOND), meta
        assert meta["last_row_ts_ns"] == base + SECOND, (
            "the repair moved the number replay's fallback reads", meta)
    finally:
        node.stop()


def test_a_repair_whose_sync_fails_publishes_nothing_and_still_answers(data_dir):
    """A failed `syncfs()` means the corrected files may not be on the device, so none of them may
    replace a `meta.json`: the old ones stay, the index is corrected anyway, and the next start
    repairs them again - this time on disk."""
    base = ten_minutes_into_an_hour_back(2)
    node = Node(data_dir)
    node.insert("AAA", 100, base + 2 * SECOND)
    node.insert("AAA", 101, base + 1 * SECOND)
    node.flush()
    node.stop()
    meta_file = meta_path(data_dir, "AAA")
    written_before_the_fix(meta_file, base // HOUR * HOUR, base + SECOND)
    with open(meta_file, encoding="utf-8") as f:
        before = f.read()

    node = Node(data_dir, fault={"OB_FAULT_PATH": data_dir, "OB_FAULT_OP": "syncfs",
                                 "OB_FAULT_ERRNO": "EIO", "OB_FAULT_COUNT": "1"})
    try:
        with open(os.path.join(data_dir, "fault.log"), encoding="utf-8", errors="replace") as f:
            assert "action=fail" in f.read(), "premise: the injector failed no syncfs"
        assert "the range repair could not sync 1 corrected meta.json file(s)" in node.log(), (
            node.log()[-1500:])
        assert node.prices("AAA", base + SECOND + SECOND // 2, base + 3 * SECOND) == [100], (
            "the index was not corrected when the disk could not be")
        with open(meta_file, encoding="utf-8") as f:
            assert f.read() == before, "a corrected meta.json was published without its sync"
        assert not os.path.exists(os.path.join(os.path.dirname(meta_file), "meta.json.range"))
    finally:
        node.stop()

    node = Node(data_dir)
    try:
        with open(meta_file, encoding="utf-8") as f:
            assert json.load(f)["time_range"] == "rows", "the next start did not repair it"
    finally:
        node.stop()


def test_replay_compares_with_the_last_row_as_it_always_did(data_dir):
    """What #166 must not change. A symbol whose only segment carries a WAL identity other than
    this node's - as one received in a snapshot does - has no trusted position, so replay decides
    by time: a record at or before the time of the segment's last row is taken to be stored. The
    segment here holds rows at +2 s and then +1 s; the record in the WAL tail is at +1.5 s and is
    stored nowhere. Compared with the last row it is replayed; compared with the newest - which is
    what a range fix could quietly have made it read - it is skipped, and lost."""
    base = ten_minutes_into_an_hour_back(2)
    node = Node(data_dir, ["--flush-interval-ms", "3600000"])
    node.insert("REP", 100, base + 2 * SECOND)
    node.insert("REP", 101, base + 1 * SECOND)
    node.flush()
    node.insert("REP", 102, base + SECOND + SECOND // 2)
    node.kill()

    meta_file = meta_path(data_dir, "REP")
    with open(meta_file, encoding="utf-8") as f:
        meta = json.load(f)
    assert meta["wal_identity"] != 0, "premise: the segment carries this node's WAL identity"
    meta["wal_identity"] = meta["wal_identity"] ^ 0x5A5A5A5A   # another node's WAL
    with open(meta_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(meta, separators=(",", ":")))

    node = Node(data_dir, ["--flush-interval-ms", "3600000"])
    try:
        assert sorted(node.prices("REP")) == [100, 101, 102], (
            f"replay skipped a record the segment does not hold:\n{node.log()[-1500:]}")
    finally:
        node.stop()
