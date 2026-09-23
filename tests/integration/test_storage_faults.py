"""What the engine does when the disk refuses a WAL write (#54).

These assert the half that already holds, and the injector is what made it assertable: a write
refused on a client's own thread is **reported to that client and not stored**, and no write that
was acknowledged is lost when a later one fails. Neither could be tested before, because a healthy
machine never returns ENOSPC.

`OB_FAULT_SIZE` matters more than it looks. This engine's WAL takes a 136-byte delta record on the
session thread and, from the flush loop, a 32-byte checkpoint and a 68-byte version vector. Failing
"the fourth write" is a different call on every run; failing "the 136-byte write" is the same one
every time. It is also what keeps these tests away from #112 — an ENOSPC on either background
record aborts the process, and that is filed rather than asserted here.
"""

from __future__ import annotations

import os
import re
import signal
import socket
import stat
import subprocess
import tempfile
import threading
import time

import pytest
from conftest import fault_injector_path, free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()
WAL_SEGMENT = "wal_000000.bin"   # what the first WAL file is called; the delta records go here
DELTA_BYTES = "136"              # one INSERT of one level, header included
CHECKPOINT_BYTES = "32"          # the header and, since #159, the 8-byte position it covers

# Read by the report plugin out of `sys.modules`, so the numbers a fault measures are printed with
# the run rather than living in a comment.
custom_metrics: dict = {}


class FaultNode:
    """A standalone node with the storage fault injector preloaded.

    Logging to a file rather than a pipe nobody reads, for the reason `test_shutdown.py` gives:
    a pipe fills at 64 KB and the node then blocks inside `write()`, which looks alive to `poll()`
    and answers nothing.
    """

    def __init__(self, **fault: str):
        # OB_FAULT_POLICY is this class's own argument, not the injector's: the fsync tests need a
        # node started with `interval` so the failure lands where a client asked for it.
        self.policy = fault.pop("OB_FAULT_POLICY", "every")
        # And so is OB_FAULT_FLUSH_MS. A test about what **replay** can reach needs the flush tick
        # out of the way: a tick writes the rows into segments and a checkpoint into the WAL, and
        # replay starts *after* the last checkpoint. The torn-record test measured [500] stranded
        # instead of [400, 500] on its first run for exactly that reason - a flush had rescued the
        # row before the kill, so the number was about timing rather than about the WAL.
        self.flush_ms = fault.pop("OB_FAULT_FLUSH_MS", "500")
        # And OB_FAULT_ROTATE_BYTES, for the tests about what happens when a file ends: the default
        # threshold is 512 MB, and a file that never fills never rotates.
        self.rotate_bytes = fault.pop("OB_FAULT_ROTATE_BYTES", None)
        # And OB_FAULT_EXTRA_ARGS, for the tests that need a second node: a replication port on a
        # primary, or the primary's address on a replica. Space-separated, appended as given.
        self.extra_args = fault.pop("OB_FAULT_EXTRA_ARGS", "").split()
        injector = fault_injector_path()
        assert injector is not None, (
            "libobfault.so was not built. Failing rather than skipping: a fault-injection test "
            "that injects nothing is indistinguishable from an engine that survives the fault"
        )
        self.data_dir = tempfile.mkdtemp(prefix="ob_storage_fault_")
        self.port = free_port()
        self.metrics_port = free_port()
        self.fault_log = os.path.join(self.data_dir, "fault.log")
        self._log_path = os.path.join(self.data_dir, "node.log")
        self._log = open(self._log_path, "a", encoding="utf-8", buffering=1)

        env = dict(os.environ)
        env["LD_PRELOAD"] = injector
        for key in list(env):
            if key.startswith("OB_FAULT_"):
                del env[key]
        if fault:
            env.update(fault)
            env["OB_FAULT_LOG"] = self.fault_log

        self.proc = subprocess.Popen(self._argv(), env=env, stdout=self._log,
                                     stderr=subprocess.STDOUT)

    def _argv(self) -> list[str]:
        """The node's command line, built in one place for the start and every restart.

        There were two copies of it, and a flag added to one of them is a restart that comes back
        as a different node - pitfall 77, which is how `restart_node()` once lost
        `--cluster-secret-file`.
        """
        argv = [SERVER, "--port", str(self.port), "--data-dir", self.data_dir,
                "--metrics-port", str(self.metrics_port), "--drain-timeout-ms", "2000",
                "--fsync-policy", self.policy, "--flush-interval-ms", self.flush_ms]
        if self.rotate_bytes is not None:
            argv += ["--wal-rotate-bytes", self.rotate_bytes]
        return argv + self.extra_args

    def counter(self, name: str) -> int:
        """One counter from /metrics, or 0 if it has never been incremented.

        The name is followed by a label set, not a space: this engine exposes
        `ob_flush_errors_total{node_role="standalone"} 1`. A first version of this matched
        `name + " "` and therefore read every counter as zero - which would have turned a working
        fix into a failing test, and reading the raw exposition is what found it.
        """
        with socket.create_connection(("127.0.0.1", self.metrics_port), timeout=patience(10)) as s:
            s.sendall(b"GET /metrics HTTP/1.0\r\n\r\n")
            chunks = []
            while True:
                data = s.recv(65536)
                if not data:
                    break
                chunks.append(data)
        for line in b"".join(chunks).decode(errors="replace").splitlines():
            if line.startswith("#"):
                continue
            head = line.split()[0] if line.split() else ""
            if head == name or head.startswith(name + "{"):
                return int(float(line.split()[1]))
        return 0

    def wait_until_answering(self, timeout: float = 20.0) -> None:
        deadline = time.time() + patience(timeout)
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=2) as probe:
                    probe.recv(4096)
                    probe.sendall(b"PING\n")
                    if b"PONG" in probe.recv(4096):
                        return
            except OSError:
                time.sleep(0.3)
        pytest.fail(f"the node never answered on {self.port}; log tail:\n{self.log()[-800:]}")

    def talk(self, *commands: str) -> list[str]:
        """One session, one reply per command. A closed connection is reported, not raised."""
        replies: list[str] = []
        with socket.create_connection(("127.0.0.1", self.port), timeout=patience(15)) as sock:
            sock.settimeout(patience(15))
            sock.recv(4096)
            for command in commands:
                sock.sendall((command + "\n").encode())
                time.sleep(0.2)
                try:
                    data = sock.recv(65536)
                    replies.append(data.decode(errors="replace").strip() if data
                                   else "<the node closed the connection>")
                except OSError as exc:
                    replies.append(f"<{type(exc).__name__}: {exc}>")
                    break
        return replies

    def insert_each(self, prices) -> dict[int, str]:
        """One `INSERT` per price on one session, each answered before the next is sent.

        Not `talk()`: that sleeps 0.2 s per command, and the rotation tests need five hundred of
        them. The reply is read by its own terminator - `OK` ends in a blank line, `ERR` in one
        newline - so no command's answer is ever read as the next one's.
        """
        replies: dict[int, str] = {}
        with socket.create_connection(("127.0.0.1", self.port), timeout=patience(15)) as sock:
            reader = sock.makefile("rb")
            while reader.readline().strip():   # the banner ends in a blank line
                pass
            for price in prices:
                sock.sendall(f"INSERT SYM EX bid {price} 1 1\n".encode())
                line = reader.readline().decode(errors="replace").rstrip("\n")
                if line == "OK":
                    reader.readline()
                replies[price] = line or "<the node closed the connection>"
        return replies

    def wal_files(self) -> list[str]:
        return sorted(f for f in os.listdir(self.data_dir)
                      if f.startswith("wal_") and f.endswith(".bin"))

    def injections(self) -> int:
        """How many faults actually fired. Zero means this test measured nothing."""
        if not os.path.exists(self.fault_log):
            return 0
        with open(self.fault_log, encoding="utf-8", errors="replace") as handle:
            return sum(line.count("action=fail") + line.count("action=short") for line in handle)

    def fault_log_text(self) -> str:
        """The injector's own log, which is where a composite fault says both halves happened."""
        if not os.path.exists(self.fault_log):
            return "(no fault log)"
        with open(self.fault_log, encoding="utf-8", errors="replace") as handle:
            return handle.read()

    def log(self) -> str:
        self._log.flush()
        with open(self._log_path, encoding="utf-8", errors="replace") as handle:
            return handle.read()

    def kill_and_restart_without_faults(self) -> None:
        """`SIGKILL`, then restart with no injector — for what **replay** can reach.

        A clean stop ends in a checkpoint, which is the whole point of a clean stop and the wrong
        thing entirely for a test about a WAL the engine has to read back: after a checkpoint there
        is nothing to replay. So this one does not ask politely.
        """
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self._restart_clean()

    def restart_without_faults(self) -> None:
        self.stop()
        self._restart_clean()

    def _restart_clean(self) -> None:
        self._log = open(self._log_path, "a", encoding="utf-8", buffering=1)
        env = {k: v for k, v in os.environ.items() if not k.startswith("OB_FAULT_")}
        env.pop("LD_PRELOAD", None)
        self.proc = subprocess.Popen(self._argv(), env=env, stdout=self._log,
                                     stderr=subprocess.STDOUT)
        self.wait_until_answering()

    def stop(self) -> int | None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=patience(20))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        return self.proc.returncode

    def cleanup(self) -> None:
        self.stop()
        self._log.close()


def prices_in(select_reply: str) -> list[int]:
    """The price column of every row a SELECT returned."""
    prices = []
    for line in select_reply.splitlines():
        parts = line.split("\t")
        if len(parts) >= 7 and parts[0].isdigit():
            prices.append(int(parts[1]))
    return prices


SELECT_ALL = "SELECT * FROM 'SYM'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999"


def test_a_refused_write_reaches_the_client_instead_of_vanishing():
    """ENOSPC on one delta record: the client is told, the row is not there, the node stays up.

    The row matters more than the wording. Asserting the message would pin `strerror`'s phrasing;
    what the client is owed is that the write it was refused did not happen.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_SIZE=DELTA_BYTES, OB_FAULT_SKIP="1", OB_FAULT_COUNT="1")
    try:
        node.wait_until_answering()
        replies = node.talk("INSERT SYM EX bid 100 1 1",
                            "INSERT SYM EX bid 200 2 1",
                            "INSERT SYM EX bid 300 3 1",
                            "FLUSH")
        assert node.injections() == 1, (
            f"nothing was injected, so this test measured nothing: {replies}")
        assert replies[0] == "OK", replies
        assert replies[1].startswith("ERR"), f"the refused write was not reported: {replies}"
        assert replies[2] == "OK", "the node did not accept writes again once the fault was spent"

        prices = prices_in(node.talk(SELECT_ALL)[0])
        assert 200 not in prices, f"a refused write was stored anyway: {prices}"
        assert {100, 300} <= set(prices), f"an acknowledged write is missing: {prices}"
        assert node.proc.poll() is None, "the node died on a refused client write"
    finally:
        node.cleanup()


def test_a_disk_that_stays_full_refuses_every_write_and_keeps_answering():
    """Every delta record fails, for ever. The node must refuse rather than fall over."""
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_SIZE=DELTA_BYTES)
    try:
        node.wait_until_answering()
        replies = node.talk(*[f"INSERT SYM EX bid {100 + i} {i + 1} 1" for i in range(3)])
        assert node.injections() >= 3, f"fewer faults fired than writes attempted: {replies}"
        assert all(r.startswith("ERR") for r in replies), replies
        assert node.talk("PING")[0] == "PONG", "a full disk left the node unable to answer at all"

        # And it must not have started a new WAL file per refusal. #126 abandons a file whose record
        # was **torn** - some bytes written, the rest failed - and these writes fail atomically, so
        # nothing was stranded and there is nothing to abandon. Without that condition a disk that
        # stays full would produce one empty WAL file per refused write, which is a pathology the
        # fix would have introduced rather than removed.
        files = sorted(f for f in os.listdir(node.data_dir)
                       if f.startswith("wal_") and f.endswith(".bin"))
        assert files == ["wal_000000.bin"], (
            f"a write that failed atomically abandoned its file anyway; WAL files are {files}")

        assert node.stop() == 0, "a node on a full disk did not shut down cleanly"
    finally:
        node.cleanup()


def test_no_acknowledged_write_survives_less_than_a_restart():
    """The durability claim, under a fault: what was acknowledged is what comes back.

    The refused write is in the middle on purpose. A node that lost everything after the first
    failure would still pass a test where the refusal came last.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_SIZE=DELTA_BYTES, OB_FAULT_SKIP="1", OB_FAULT_COUNT="1")
    try:
        node.wait_until_answering()
        replies = node.talk("INSERT SYM EX bid 100 1 1",
                            "INSERT SYM EX bid 200 2 1",
                            "INSERT SYM EX bid 300 3 1",
                            "FLUSH")
        assert node.injections() == 1, f"nothing was injected: {replies}"
        acknowledged = {100, 300}
        assert replies[1].startswith("ERR"), replies

        node.restart_without_faults()
        prices = set(prices_in(node.talk(SELECT_ALL)[0]))
        assert acknowledged <= prices, f"an acknowledged write did not survive the restart: {prices}"
        assert 200 not in prices, f"a refused write appeared after the restart: {prices}"
    finally:
        node.cleanup()


def test_a_failing_flush_tick_does_not_take_the_node_with_it():
    """#112: an ENOSPC on the flush thread's own WAL write used to abort the process.

    The 32-byte record is the checkpoint, and nothing but the flush loop writes it — which is why
    the size filter is the whole instrument here. It was 24 bytes until #159 gave it the position
    it covers, and the first run after that change failed on "no fault fired": the size names the
    call, so a record that changes size moves the injection point, and this test's own guard is
    what said so rather than a pass over nothing. Measured before the fix, by naming that call:
    SIGABRT with one injection fired, an **idle** node dead 0.5 s into idleness with nobody
    connected, and three consecutive restarts on a disk that stays full each coming up and dying
    unattended after 1.5–2.0 s. The client-facing path handled the same condition correctly the
    whole time, which is what made it look survivable.

    The counter is asserted as well as the node being alive. Alive alone would also pass if the
    injection never fired, and "the fault did not happen" and "the fault was handled" are the two
    outcomes this test exists to separate.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_SIZE=CHECKPOINT_BYTES)
    try:
        node.wait_until_answering()
        assert node.talk("INSERT SYM EX bid 100 1 1")[0] == "OK", "the write path itself refused"

        # Several flush intervals with nobody connected: the shape that killed an idle node.
        deadline = time.time() + patience(25)
        while time.time() < deadline and "flushing again" not in node.log():
            if node.proc.poll() is not None:
                pytest.fail(
                    f"the node died on a failing flush tick, exit {node.proc.returncode}; "
                    f"log tail:\n{node.log()[-900:]}")
            time.sleep(0.5)

        assert node.proc.poll() is None, "the node died on a failing flush tick"
        assert node.injections() >= 1, "no fault fired, so this test measured nothing"
        assert node.counter("ob_flush_errors_total") >= 1, (
            "the tick failed but nothing counted it, so an operator watching a full disk would see "
            "a node that looks healthy")

        # That the *next* tick ran is the property, and the recovery line is where it is
        # observable. Waiting for a second failure instead would be wrong: one insert produces one
        # checkpoint, so the injector can only fire once - a count asserted rather than measured,
        # which is how the first version of this test failed against a working fix.
        assert "flushing again" in node.log(), (
            f"no tick ran after the failing one; log tail:\n{node.log()[-900:]}")

        assert node.talk("PING")[0] == "PONG", "the node stopped answering"
        assert node.stop() == 0, "a node whose flushes are failing did not shut down cleanly"
    finally:
        node.cleanup()


def test_a_failed_fsync_is_not_answered_with_ok():
    """#113: `--fsync-policy every` promises the record is on the disk before the client is told.

    Measured before the fix: **eleven `fsync` calls returned `EIO` and all three `INSERT`s were
    still answered `OK`**, because all seven `::fsync()` calls in src/wal.cpp discarded their
    result. The node stayed up and exited 0 — from inside the process nothing was missing, which is
    exactly why the return value has to be read.

    A retry cannot repair it and the fix does not pretend otherwise: Linux reports the error once
    and marks the pages clean, so the next `fsync` on that descriptor returns 0 with the data gone.
    What the engine can honestly do is stop claiming the write is durable, which is what this
    asserts.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="fsync", OB_FAULT_ERRNO="EIO")
    try:
        node.wait_until_answering()
        replies = node.talk("INSERT SYM EX bid 100 1 1", "INSERT SYM EX bid 200 2 1")
        assert node.injections() >= 1, f"no fsync was made to fail: {replies}"
        assert all(r.startswith("ERR") for r in replies), (
            f"a write whose fsync failed was answered as durable under fsync-policy=every: {replies}")

        # The node is still a node: a disk that cannot sync is a refusal, not a crash (#112's rule
        # applied to the other half of the same file).
        assert node.proc.poll() is None, "the node died on a failed fsync"
        assert node.talk("PING")[0] == "PONG"

        deadline = time.time() + patience(15)
        while time.time() < deadline and node.counter("ob_wal_fsync_errors_total") < 1:
            time.sleep(0.5)
        assert node.counter("ob_wal_fsync_errors_total") >= 1, (
            "the sync failed and nothing counted it, so an operator cannot tell a disk that is full "
            "from one that is failing — and those ask for different actions")
    finally:
        node.cleanup()


def test_a_flush_command_does_not_report_success_over_a_failed_sync():
    """`FLUSH` answering `OK` over a failed fsync is the same lie as `INSERT` doing it.

    Written with the interval policy, so the failure happens where a client asked for it rather
    than on the write path — the two go through different call sites in src/wal.cpp and only one of
    them was covered by the test above.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="fsync", OB_FAULT_ERRNO="EIO",
                     OB_FAULT_POLICY="interval")
    try:
        node.wait_until_answering()
        replies = node.talk("INSERT SYM EX bid 100 1 1", "FLUSH")
        assert replies[0] == "OK", f"the write itself should not be refused here: {replies}"
        assert node.injections() >= 1, f"no fsync was made to fail: {replies}"
        assert replies[1].startswith("ERR"), (
            f"FLUSH reported success over a sync that failed: {replies}")
        assert node.proc.poll() is None, "the node died on a failed fsync"
    finally:
        node.cleanup()


SEGMENT_COLUMN = "price.col"   # one column file every segment has; a flush writes it for each

def select_prices(node: FaultNode) -> list[int]:
    """Every price `SYM.EX` holds, read to the end of the answer.

    Not `talk()`: that takes one `recv()`, and the rows the retention test counts are tens of
    kilobytes - an answer cut at a segment boundary would read as rows that were lost.
    """
    with socket.create_connection(("127.0.0.1", node.port), timeout=patience(15)) as sock:
        reader = sock.makefile("rb")
        while reader.readline().strip():   # the banner ends in a blank line
            pass
        sock.sendall((SELECT_ALL + "\n").encode())
        lines = []
        while True:
            line = reader.readline()
            # `OK` ends in a blank line and `ERR` in one newline: a symbol with nothing left is
            # `ERR ... not found`, which a reader waiting for a blank line would hang on.
            if not line or line == b"\n" or line.startswith(b"ERR"):
                break
            lines.append(line.decode(errors="replace"))
    return prices_in("".join(lines))


def delays_injected(node: FaultNode) -> int:
    """How many calls the injector has made slow so far."""
    return node.fault_log_text().count("action=delay")


def wait_for_delays(node: FaultNode, count: int, timeout: float = 20.0) -> None:
    deadline = time.time() + patience(timeout)
    while delays_injected(node) < count and time.time() < deadline:
        time.sleep(0.02)
    assert delays_injected(node) >= count, (
        f"the injector never slowed segment write number {count}: {node.fault_log_text()}")


@pytest.mark.parametrize("policy", ["every", "interval"])
def test_rows_written_while_a_tick_writes_segments_survive_a_crash_after_it(policy):
    """#159: the checkpoint claims what its flush drained, not what the log held when it wrote it.

    A flush drains the queued rows under the engine's lock and writes their segments without it,
    so writers go on while it does - and only then appends the checkpoint that replay starts from.
    That checkpoint said "everything before me", so it covered the records written during the
    segment I/O, whose rows were still queued. A crash before they reached a segment then lost
    every one, **each answered `OK` and under every fsync policy** - `every` included, where the
    record was on the disk before the client heard anything.

    The injector slows two segment writes by two seconds each. Row 300 goes in before the first
    tick, rows 301-303 while that tick is writing, and the kill comes while the **next** tick is
    writing them - after the first tick's checkpoint, before any segment holds them.

    A tick rather than `FLUSH`, which runs on the connection's event loop: with one loop it holds
    every other connection's writes back for the whole segment write, and the first version of this
    test measured exactly that - 1.99 s for three `INSERT`s, then a `FLUSH` that had already
    answered. The premise is checked, not assumed: a row written before the drain survives on the
    old build too, and this test would then prove nothing.
    """
    node = FaultNode(OB_FAULT_PATH=SEGMENT_COLUMN, OB_FAULT_OP="write", OB_FAULT_DELAY_MS="2000",
                     OB_FAULT_COUNT="2", OB_FAULT_POLICY=policy, OB_FAULT_FLUSH_MS="2000")
    try:
        node.wait_until_answering()
        assert node.insert_each([300]) == {300: "OK"}
        wait_for_delays(node, 1)

        started = time.monotonic()
        replies = node.insert_each([301, 302, 303])
        took = time.monotonic() - started
        assert all(r == "OK" for r in replies.values()), replies
        assert node.counter("ob_segment_count") == 0, (
            f"the slow tick had merged its segment before 301-303 were acknowledged ({took:.3f} s), "
            "so they were not written during its segment I/O")
        custom_metrics[f"writes_during_segment_io_s_{policy}"] = round(took, 3)

        wait_for_delays(node, 2)
        assert node.counter("ob_segment_count") >= 1, "the first tick never merged its segment"

        node.kill_and_restart_without_faults()
        assert sorted(select_prices(node)) == [300, 301, 302, 303], (
            "a row acknowledged while a tick wrote its segments was lost (or doubled) by the "
            f"replay after a crash:\n{node.log()[-1500:]}")
        # And the restart says so: the one line that tells an operator this happened is the count
        # of records the checkpoint's position gave back, which here is exactly the three.
        given_back = re.findall(r"of which (\d+) written before the checkpoint", node.log())
        assert given_back and given_back[-1] == "3", (
            f"the replay did not report the three records it gave back: {given_back}")
    finally:
        node.cleanup()


def test_a_wal_file_rotated_away_while_a_tick_wrote_segments_is_kept():
    """#159, the second reader of the same boundary: WAL retention.

    The flush tick deleted every WAL file before the **current** one once its segments were
    written. Writers append while those segments are written, and a rotation in that window leaves
    their records in a file before the current one - with their rows still queued. The tick deleted
    it, and a crash before those rows reached a segment had neither the segment nor the record.

    So: row 300, a tick whose segment write is slowed, six hundred rows during it - enough to fill
    the smallest WAL file the flag allows and rotate - and a kill while the **next** tick is writing
    them, that is after the first tick's retention ran and before anything else saved them. The
    checkpoint fix alone does not pass this: it gives back what is in the log, and the log had lost
    the file.
    """
    node = FaultNode(OB_FAULT_PATH=SEGMENT_COLUMN, OB_FAULT_OP="write", OB_FAULT_DELAY_MS="6000",
                     OB_FAULT_COUNT="2", OB_FAULT_POLICY="every", OB_FAULT_FLUSH_MS="2000",
                     OB_FAULT_ROTATE_BYTES="65573")
    burst = list(range(301, 901))
    try:
        node.wait_until_answering()
        assert node.insert_each([300]) == {300: "OK"}
        wait_for_delays(node, 1)

        started = time.monotonic()
        replies = node.insert_each(burst)
        took = time.monotonic() - started
        assert all(r == "OK" for r in replies.values()), [r for r in replies.values() if r != "OK"][:3]
        assert "wal_000001.bin" in node.wal_files(), (
            f"the burst did not rotate the WAL, so there is no earlier file to keep: {node.wal_files()}")
        assert node.counter("ob_segment_count") == 0, (
            f"the slow tick had finished before the burst did ({took:.3f} s), so the burst was not "
            "written during its segment I/O")
        custom_metrics["retention_burst_s"] = round(took, 3)

        # The first tick finishes, retention runs, and the next tick drains the burst and is slowed
        # in turn: killed while it writes, the burst is in no segment and only the WAL has it.
        wait_for_delays(node, 2, timeout=30.0)
        assert node.counter("ob_segment_count") >= 1, "the first tick never merged its segment"
        custom_metrics["retention_wal_files_at_kill"] = len(node.wal_files())

        node.kill_and_restart_without_faults()
        assert sorted(select_prices(node)) == [300] + burst, (
            f"rows whose WAL file the tick deleted did not come back: {node.log()[-1500:]}")
    finally:
        node.cleanup()


def test_a_torn_record_costs_only_the_write_that_tore():
    """#126: the write that tore is refused, and every other acknowledged write survives.

    The shape needs both halves and that is the first finding. `WALWriter::write_record()` loops
    `while (remaining > 0)`, so a short write on its own is **resumed** and the record completes —
    correctly. It takes a short write plus a failed retry to leave a partial record in the file, and
    the injector grew `OB_FAULT_SHORT_THEN_FAIL` for exactly that: the chosen write is cut and the
    remainder fails, whatever its size, because a size filter that names a 136-byte record cannot
    name the 116-byte retry.

    **This test measured the defect before it asserted the guarantee**, which is why it reads the
    way it does. #54's A2.2 asked how many records *after* the torn one stop being readable: the
    answer was **2 of 2**, because the writes that follow are answered `OK`, land behind the
    stranded bytes, and `WALReplayer::replay()` returned at the first checksum mismatch — for the
    whole directory rather than for one file. #126 fixed both halves, and neither is useful alone:
    the writer abandons a file whose record it tore (without a ROTATE marker, since it has just
    established the file cannot be written to), and replay treats a mismatch in a file that is not
    the last one as a tear and continues with the next.

    So the assertion is now the guarantee: the cut write is refused, and **everything else that was
    acknowledged comes back**. The evidence that this is the new path rather than luck is the second
    WAL file: the node was configured never to rotate on size, so `wal_000001.bin` exists only
    because the tear put it there.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_SIZE=DELTA_BYTES, OB_FAULT_SKIP="2", OB_FAULT_COUNT="1",
                     OB_FAULT_SHORT="20", OB_FAULT_SHORT_THEN_FAIL="1",
                     # An hour, so no flush tick runs during this test. What a tick does is
                     # exactly what this test must not measure: it moves rows into segments and
                     # writes a checkpoint, and replay begins after the last checkpoint. With the
                     # 500 ms default the first run of this test read one stranded record instead
                     # of two, because a tick had rescued the other one.
                     OB_FAULT_FLUSH_MS="3600000")
    try:
        node.wait_until_answering()
        replies = node.talk("INSERT SYM EX bid 100 1 1",   # before the tear
                            "INSERT SYM EX bid 200 2 1",   # before the tear
                            "INSERT SYM EX bid 300 3 1",   # cut at 20 bytes, remainder refused
                            "INSERT SYM EX bid 400 4 1",   # after the tear, acknowledged
                            "INSERT SYM EX bid 500 5 1")   # after the tear, acknowledged
        # Two, not one: the cut and the refused remainder are the two halves of one fault, and
        # `injections()` counts `action=` lines. Asserting one was the first version of this test
        # and it failed on its own instrument rather than on the engine.
        assert node.injections() == 2, f"the record was not cut and left cut: {replies}"
        assert "action=fail-remainder" in node.fault_log_text(), (
            f"the short write was not followed by a failed retry, so no record was torn:\n"
            f"{node.fault_log_text()}")

        assert replies[2].startswith("ERR"), (
            f"the write whose record was cut was acknowledged: {replies}")
        acknowledged = {100, 200, 400, 500}
        for i, price in ((0, 100), (1, 200), (3, 400), (4, 500)):
            assert replies[i].startswith("OK"), f"the write of {price} was refused: {replies}"

        # No FLUSH: this is about what **replay** can reach, and a flush would move the rows into
        # segments where the WAL no longer matters. That is also why the node is killed rather than
        # stopped - a clean stop ends in a checkpoint.
        node.kill_and_restart_without_faults()
        prices = set(prices_in(node.talk(SELECT_ALL)[0]))

        assert 300 not in prices, (
            f"the refused write came back after the restart, which would make the refusal a lie: "
            f"{sorted(prices)}")
        stranded = sorted(p for p in acknowledged if p not in prices)
        custom_metrics["records_stranded_behind_a_torn_one"] = len(stranded)
        assert not stranded, (
            f"{len(stranded)} acknowledged write(s) did not survive the restart: {stranded}. "
            f"Before #126 this was 2 of 2, because the writer kept appending behind the stranded "
            f"bytes and replay stopped at the first checksum mismatch in the directory:\n"
            f"{node.log()}")

        # And the tear really did move the writer on, which is what makes the assertion above about
        # the fix rather than about a fault that failed to fire. The node never rotates on size
        # here - the default threshold is 512 MB and this test writes five records - so a second
        # WAL file exists only because the torn one was abandoned.
        files = sorted(f for f in os.listdir(node.data_dir)
                       if f.startswith("wal_") and f.endswith(".bin"))
        assert files == ["wal_000000.bin", "wal_000001.bin"], (
            f"the writer did not abandon the file it tore; WAL files are {files}")
        assert "torn record in" in node.log(), (
            f"nothing in the node's log names the torn record, so an operator would have no way to "
            f"know one happened:\n{node.log()}")
        # And the replay is **silent** about the tear, which is worth asserting because an earlier
        # version of this test expected the opposite. Once the writer abandons the file, that file
        # ends mid-record - 20 bytes of a 24-byte header - and every reader here has always treated
        # a short header as the end of that file, so no checksum is ever compared. The replayer's
        # half of #126 exists for a WAL an **older build** left behind, where records sit *behind*
        # the tear and a garbled header does parse; `WalTornRecord.*` covers that, and
        # `tears_skipped()` is how the engine reports it.
        assert "checksum mismatch" not in node.log(), (
            f"the replay compared a checksum inside the abandoned file, which means the writer left "
            f"something behind the tear:\n{node.log()}")
    finally:
        node.cleanup()


# ── #153 and #154: what happens when a WAL file ends ──────────────────────────
#
# The smallest threshold `--wal-rotate-bytes` accepts (a WAL header and the largest payload), so a
# file ends after a few hundred one-level inserts rather than after 512 MB of them.
ROTATE_BYTES = "65573"
# The insert that carries the first file past it: 482 of them reach 65 552 bytes, the 483rd 65 688.
ROTATING_INSERT = -(-int(ROTATE_BYTES) // int(DELTA_BYTES))


def test_a_failed_sync_of_a_file_the_wal_rotated_away_from_costs_no_acknowledged_write():
    """#153, where stage 5 of #151 moved the sync it was about: the file a rotation leaves.

    Replay and catch-up both stop reading a file at its ROTATE record. The marker used to be written
    through the path that syncs under `every`, and a failed sync threw **before** the writer moved
    to the next file - so the next record went into the same file, behind the marker. Measured
    before #153's fix, failing exactly that sync: the insert that crossed the threshold was answered
    `ERR` although its record was on the disk, and **the one after it was answered `OK` and was gone
    after a restart** - 487 acknowledged, 487 back, and the one missing was acknowledged.

    Since stage 5 the rotation syncs nothing: the file it leaves is synced later, by the flush tick
    without the engine's lock, or by a `FLUSH` - which is what makes the failure deterministic here,
    with the tick an hour away. Under `every` each insert syncs the file once, so syncs 0..482 are
    the 483 inserts that fill `wal_000000.bin`, and **no** sync of it follows the rotation until
    the `FLUSH`: the injector has fired zero times when the inserts are done, which is the premise
    that the rotation no longer syncs. The `FLUSH`'s sync of the left file fails and it says so; no
    insert was answered anything but `OK`; and after a restart every acknowledged write is back.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="fsync", OB_FAULT_ERRNO="EIO",
                     OB_FAULT_SKIP=str(ROTATING_INSERT), OB_FAULT_COUNT="1",
                     OB_FAULT_ROTATE_BYTES=ROTATE_BYTES, OB_FAULT_FLUSH_MS="3600000")
    try:
        node.wait_until_answering()
        prices = [1000 + i for i in range(1, ROTATING_INSERT + 6)]
        replies = node.insert_each(prices)
        refused = {p: r for p, r in replies.items() if r != "OK"}
        assert not refused, f"an insert around the rotation was refused: {refused}"
        assert node.wal_files() == ["wal_000000.bin", "wal_000001.bin"], (
            f"the writer did not move to the next file; WAL files are {node.wal_files()}")
        assert node.injections() == 0, (
            "a sync of the left file ran before anything asked for one - the rotation syncs it "
            f"itself again, under the engine's lock:\n{node.fault_log_text()}")

        assert node.talk("FLUSH")[0].startswith("ERR"), (
            "the FLUSH whose sync of the file the WAL rotated away from failed answered OK")
        assert node.injections() == 1, f"no sync was made to fail:\n{node.fault_log_text()}"

        node.kill_and_restart_without_faults()
        back = set(prices_in(node.talk(SELECT_ALL)[0]))
        lost = sorted(p for p in prices if p not in back)
        custom_metrics["acknowledged_writes_lost_to_a_failed_rotation_sync"] = len(lost)
        assert not lost, (
            f"{len(lost)} acknowledged write(s) did not survive the restart: {lost}. Before #153 "
            f"this was the insert right after the rotating one, written behind the ROTATE marker "
            f"where replay stops reading")
    finally:
        node.cleanup()


def test_a_rotation_that_cannot_write_its_marker_does_not_refuse_the_write_that_crossed_it():
    """#153's other half: the disk refuses the ROTATE marker, and no client is told it failed.

    The marker is the first 24-byte write this node makes: inserts are 136 bytes, a checkpoint has
    been 32 since #159, and the other 24-byte record - a GAP - needs a sequence number skipped, which
    nothing here does. The flush tick is an hour away regardless. Before the fix
    the refusal of the marker threw out of the `INSERT` that asked for the rotation, whose own record
    was already in the file: answered `ERR`, present after a restart, so a client that sent it again
    stored it twice. Now the file is left as it was - readable to its end, since nothing of the
    marker reached it - the next record goes into it, and the rotation is tried again after that.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_SIZE="24", OB_FAULT_COUNT="1",
                     OB_FAULT_ROTATE_BYTES=ROTATE_BYTES, OB_FAULT_FLUSH_MS="3600000")
    try:
        node.wait_until_answering()
        prices = [2000 + i for i in range(1, ROTATING_INSERT + 6)]
        replies = node.insert_each(prices)
        assert node.injections() == 1, f"the marker was not refused:\n{node.fault_log_text()}"
        assert "arg=24 " in node.fault_log_text(), node.fault_log_text()
        refused = {p: r for p, r in replies.items() if r != "OK"}
        assert not refused, (
            f"a refused ROTATE marker was reported as the failure of a client's write: {refused}")
        assert node.log().count("with a ROTATE record") == 1, node.log()[-2000:]
        # Tried again after the next record, and that time it worked.
        assert node.wal_files() == ["wal_000000.bin", "wal_000001.bin"], node.wal_files()

        node.kill_and_restart_without_faults()
        back = set(prices_in(node.talk(SELECT_ALL)[0]))
        lost = sorted(p for p in prices if p not in back)
        assert not lost, f"acknowledged writes did not survive the restart: {lost}"
    finally:
        node.cleanup()


def test_a_wal_that_could_not_open_its_next_file_opens_it_once_it_can():
    """#154: the next WAL file cannot be created for a moment, and the node recovers from it.

    Produced without the injector: the data directory is made read-only just before the insert that
    rotates, so creating the next file fails with EACCES, and writable again one insert later.
    `open_current()` gives up the old descriptor before it opens the new one - it closed it then,
    and since stage 5 of #151 it leaves it for the flush tick to sync and close - and before the fix
    nothing opened a file again: measured, every write after the rotation was refused with
    `Bad file descriptor` for the rest of the process - after the directory was writable again.

    Three things are asserted and each is a separate claim: the insert that crossed the threshold
    is `OK` (its record is written; the rotation is what failed, #153), the one while the directory
    is still read-only is refused **with the reason** rather than with a bad descriptor, and the
    ones after it are written to the next file. It happens twice, at the end of two files, because
    an outage the log reports once has to be reported again the next time it starts. A test run as
    root would chmod nothing and prove nothing, so it refuses to run as root rather than pass.
    """
    assert os.geteuid() != 0, (
        "run as root, a read-only directory refuses nothing, so this test would measure nothing")
    node = FaultNode(OB_FAULT_ROTATE_BYTES=ROTATE_BYTES, OB_FAULT_FLUSH_MS="3600000")
    mode = os.stat(node.data_dir).st_mode
    replies: dict[int, str] = {}
    refused: list[int] = []

    def through_a_rotation_that_cannot_open(first_price: int, already_in_file: int) -> int:
        """Fill the current file to its rotating insert, with the directory read-only for that
        insert and the next one. Returns the next unused price."""
        before = [first_price + i for i in range(ROTATING_INSERT - already_in_file - 1)]
        replies.update(node.insert_each(before))
        crossing = first_price + len(before)
        while_read_only = crossing + 1
        os.chmod(node.data_dir, mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
        try:
            replies.update(node.insert_each([crossing, while_read_only]))
        finally:
            os.chmod(node.data_dir, mode)
        after = [while_read_only + i for i in range(1, 4)]
        replies.update(node.insert_each(after))

        assert replies[crossing] == "OK", (
            f"the insert whose record was written was refused because the rotation after it "
            f"failed: {replies[crossing]}")
        refusal = replies[while_read_only]
        assert refusal.startswith("ERR") and ".bin" in refusal and "denied" in refusal, (
            f"a write with no WAL file to go to was not refused with the reason: {refusal}")
        assert "Bad file descriptor" not in refusal, refusal
        refused.append(while_read_only)
        refused_after = {p: replies[p] for p in after if replies[p] != "OK"}
        assert not refused_after, (
            f"the WAL did not open its next file once it could, so every write stays refused until "
            f"the process restarts: {refused_after}")
        return after[-1] + 1

    try:
        node.wait_until_answering()
        nxt = through_a_rotation_that_cannot_open(3000, already_in_file=0)
        assert node.wal_files() == ["wal_000000.bin", "wal_000001.bin"], node.wal_files()
        # The second file already holds the three inserts written after the first recovery.
        through_a_rotation_that_cannot_open(nxt, already_in_file=3)
        assert node.wal_files() == ["wal_000000.bin", "wal_000001.bin", "wal_000002.bin"], (
            node.wal_files())
        others = {p: r for p, r in replies.items() if r != "OK" and p not in refused}
        assert not others, f"writes outside the two read-only moments were refused: {others}"

        # One line when the writer lost its file and one when it got one back - per outage, not
        # per write, and again for the second outage.
        assert node.log().count("has no WAL file to write to") == 2, node.log()[-3000:]
        assert node.log().count("the writer had no WAL file for 2 attempt(s)") == 2, (
            node.log()[-3000:])

        node.kill_and_restart_without_faults()
        back = set(prices_in(node.talk(SELECT_ALL)[0]))
        acknowledged = [p for p, r in replies.items() if r == "OK"]
        lost = sorted(p for p in acknowledged if p not in back)
        assert not lost, f"acknowledged writes did not survive the restart: {lost}"
        assert not [p for p in refused if p in back], "a refused write came back after the restart"
    finally:
        os.chmod(node.data_dir, mode)
        node.cleanup()


DATA_DIR_PREFIX = "ob_storage_fault_"   # FaultNode's data directory, which is what syncfs() names


def replay_line(node: FaultNode) -> str:
    """The restarted node's account of its WAL replay: what it applied and what segments held."""
    lines = [l for l in node.log().splitlines() if "WAL replay: records=" in l]
    return lines[-1] if lines else ""


def skipped_by_position(node: FaultNode) -> int:
    m = re.search(r"skipped_by_position=(\d+)", replay_line(node))
    return int(m.group(1)) if m else -1


def applied(node: FaultNode) -> int:
    m = re.search(r"applied=(\d+)", replay_line(node))
    return int(m.group(1)) if m else -1


@pytest.mark.parametrize("policy", ["every", "interval"])
def test_a_flush_whose_segments_did_not_sync_claims_nothing(policy):
    """#160: segments that could not be made durable are not claimed by a checkpoint.

    The first `syncfs()` is made to fail. `FLUSH` answers `ERR` - a client that asked is told - and the
    counter moves, while the rows are merged and readable, because they *are* written. What says no
    checkpoint claimed them is the restart after a kill: the segment no checkpoint vouches for is
    **removed**, and the replay **applies** both records, rebuilding it. A checkpoint would have
    covered them, and the replay would have trusted a segment a power cut may have left empty.
    """
    node = FaultNode(OB_FAULT_PATH=DATA_DIR_PREFIX, OB_FAULT_OP="syncfs", OB_FAULT_ERRNO="EIO",
                     OB_FAULT_COUNT="1", OB_FAULT_POLICY=policy, OB_FAULT_FLUSH_MS="3600000")
    try:
        node.wait_until_answering()
        assert node.insert_each([100, 101]) == {100: "OK", 101: "OK"}
        reply = node.talk("FLUSH")[0]
        assert "action=fail" in node.fault_log_text(), node.fault_log_text()
        assert reply.startswith("ERR") and "segment sync failed" in reply, (
            f"FLUSH did not report the failed segment sync: {reply!r}")
        assert node.counter("ob_segment_sync_errors_total") == 1
        assert node.counter("ob_checkpoints_frozen") == 1
        assert node.log().count("A flush's segment sync failed (Input/output error)") == 1
        assert sorted(select_prices(node)) == [100, 101], "the written rows are not readable"

        node.kill_and_restart_without_faults()
        assert sorted(select_prices(node)) == [100, 101], (
            f"the rows did not come back exactly once:\n{node.log()[-1500:]}")
        assert node.log().count("1 segment(s) written after the last checkpoint that survived "
                                "were removed") == 1, node.log()[-2000:]
        assert applied(node) == 2 and skipped_by_position(node) == 0, (
            f"the replay did not rebuild the removed segment's two rows: {replay_line(node)!r}")
        # And it says which of the three states the log was in: no checkpoint at all, which the
        # replay line used to render as an older build's checkpoint that says nothing.
        assert "resuming from the start of the log, which holds no checkpoint" in node.log()
    finally:
        node.cleanup()


def test_a_failed_segment_sync_freezes_the_claim_until_a_restart():
    """#160: after a failed segment sync, no later sync vouches for what that one held.

    Linux reports a failed sync once and marks the pages it could not write clean, so the next
    `syncfs()` succeeds **without writing them again**: a checkpoint after it that claimed the first
    flush's segments would claim files the device may not have. So the second `FLUSH` - whose own
    sync succeeds, and which answers `OK` for that - claims nothing either, and after a kill the
    restart removes **both** segments and rebuilds all three rows from the WAL.

    The restart is what ends it: its own flush syncs and claims, so a second restart replays
    nothing. This test asserted the opposite at first - that the second flush's checkpoint claimed
    both - and it was checking the design against the kernel's semantics that turned it round.
    """
    node = FaultNode(OB_FAULT_PATH=DATA_DIR_PREFIX, OB_FAULT_OP="syncfs", OB_FAULT_ERRNO="EIO",
                     OB_FAULT_COUNT="1", OB_FAULT_POLICY="every", OB_FAULT_FLUSH_MS="3600000")
    try:
        node.wait_until_answering()
        assert node.insert_each([100, 101]) == {100: "OK", 101: "OK"}
        assert node.talk("FLUSH")[0].startswith("ERR")
        assert node.insert_each([102]) == {102: "OK"}
        assert node.talk("FLUSH")[0] == "OK", "a flush whose own sync succeeded was refused"
        assert "action=pass-spent" in node.fault_log_text(), (
            f"the second flush's sync was not the one after the failure:\n{node.fault_log_text()}")
        assert node.counter("ob_segment_sync_errors_total") == 1
        assert node.counter("ob_checkpoints_frozen") == 1
        assert sorted(select_prices(node)) == [100, 101, 102]

        node.kill_and_restart_without_faults()
        assert sorted(select_prices(node)) == [100, 101, 102]
        assert node.log().count("2 segment(s) written after the last checkpoint that survived "
                                "were removed") == 1, node.log()[-2000:]
        assert applied(node) == 3 and skipped_by_position(node) == 0, (
            f"the replay did not rebuild both segments: {replay_line(node)!r}")
        assert node.counter("ob_checkpoints_frozen") == 0

        node.kill_and_restart_without_faults()
        assert sorted(select_prices(node)) == [100, 101, 102]
        assert applied(node) == 0, (
            f"the restart's own flush claimed nothing, so the freeze outlived the process that saw "
            f"the failure: {replay_line(node)!r}")
        assert node.log().count("segment(s) written after the last checkpoint that survived") == 1
    finally:
        node.cleanup()


def test_retention_does_not_pass_a_segment_that_did_not_sync():
    """#160: while no flush can sync, no WAL file is deleted - the segments are not on the device.

    Every `syncfs()` fails, and the WAL rotates under six hundred writes. The ticks that write those
    rows' segments fail to sync them; the ticks after them have nothing to write and so nothing to
    sync - only a flush with segments syncs - but retention runs on every tick, and it must not move.
    Before this change retention followed the drain and deleted `wal_000000.bin` at the first tick
    after the rotation; a power cut then had neither the unsynced segment nor the record.
    """
    node = FaultNode(OB_FAULT_PATH=DATA_DIR_PREFIX, OB_FAULT_OP="syncfs", OB_FAULT_ERRNO="EIO",
                     OB_FAULT_POLICY="interval", OB_FAULT_FLUSH_MS="300",
                     OB_FAULT_ROTATE_BYTES="65573")
    try:
        node.wait_until_answering()
        prices = list(range(1000, 1600))
        replies = node.insert_each(prices)
        assert all(r == "OK" for r in replies.values())
        assert "wal_000001.bin" in node.wal_files(), node.wal_files()
        deadline = time.time() + patience(20)
        while node.counter("ob_segment_sync_errors_total") < 1 and time.time() < deadline:
            time.sleep(0.05)
        assert node.counter("ob_segment_sync_errors_total") >= 1, "no segment sync was refused"
        # Three more ticks, each of which runs retention over the same state.
        ticks = node.counter("ob_flush_ticks_total")
        while node.counter("ob_flush_ticks_total") < ticks + 3 and time.time() < deadline:
            time.sleep(0.05)
        assert node.counter("ob_flush_ticks_total") >= ticks + 3, "the ticks stopped"
        assert "wal_000000.bin" in node.wal_files(), (
            f"retention deleted a WAL file whose segment never synced: {node.wal_files()}")
        assert node.log().count("A flush's segment sync failed") == 1, "loud more than once"

        node.kill_and_restart_without_faults()
        assert sorted(select_prices(node)) == prices
    finally:
        node.cleanup()



def test_retention_does_not_pass_a_checkpoint_a_failed_wal_sync_may_have_lost():
    """#160: a WAL sync that fails after a checkpoint leaves that checkpoint unvouched for.

    A checkpoint is appended without a sync and the next tick's WAL sync is what puts it on the
    device - the retention floor waits for that sync. **When it fails, a later one proves nothing**:
    Linux reports a failed sync once and marks the pages it could not write clean, so the tick after
    syncs successfully without writing the checkpoint at all. Taking that as the checkpoint being
    durable let retention delete a file whose records a power cut would then need.

    Deterministic without a clock: under `interval` a rotation leaves a sync owed, so the first sync
    of `wal_000002.bin` is the phase-A sync of the first tick after the rotation, and that tick
    drains at least the row that crossed into it - its checkpoint claims a position in file 2. The
    second sync of the file, the next tick's, is the one that fails.
    """
    node = FaultNode(OB_FAULT_PATH="wal_000002.bin", OB_FAULT_OP="fsync", OB_FAULT_ERRNO="EIO",
                     OB_FAULT_SKIP="1", OB_FAULT_COUNT="1", OB_FAULT_POLICY="interval",
                     OB_FAULT_FLUSH_MS="300", OB_FAULT_ROTATE_BYTES="65573")
    try:
        node.wait_until_answering()
        prices = list(range(1000, 2100))
        replies = node.insert_each(prices)
        assert all(r == "OK" for r in replies.values())
        files = node.wal_files()
        assert "wal_000002.bin" in files and "wal_000003.bin" not in files, files

        deadline = time.time() + patience(20)
        while node.injections() < 1 and time.time() < deadline:
            time.sleep(0.05)
        assert node.injections() == 1, f"the WAL sync never failed:\n{node.fault_log_text()}"
        assert "seen=0 action=pass-skip" in node.fault_log_text(), node.fault_log_text()
        ticks = node.counter("ob_flush_ticks_total")
        while node.counter("ob_flush_ticks_total") < ticks + 3 and time.time() < deadline:
            time.sleep(0.05)
        assert node.counter("ob_flush_ticks_total") >= ticks + 3, "the ticks stopped"
        assert "wal_000001.bin" in node.wal_files(), (
            f"retention deleted a WAL file on the word of a checkpoint whose sync failed: "
            f"{node.wal_files()}\n{node.fault_log_text()}")
        assert node.counter("ob_checkpoints_frozen") == 1

        # And it is frozen rather than slow: a row written now, flushed and synced, moves nothing.
        assert node.insert_each([5000]) == {5000: "OK"}
        ticks = node.counter("ob_flush_ticks_total")
        while node.counter("ob_flush_ticks_total") < ticks + 3 and time.time() < deadline:
            time.sleep(0.05)
        assert "wal_000001.bin" in node.wal_files(), node.wal_files()

        # The restart is what ends it: its own flush claims what it rebuilt, a sync covers that,
        # and retention moves past the file it held - the other half of this test, without which
        # a retention that never deletes anything would pass it.
        node.kill_and_restart_without_faults()
        assert sorted(select_prices(node)) == prices + [5000]
        deadline = time.time() + patience(20)
        while "wal_000001.bin" in node.wal_files() and time.time() < deadline:
            time.sleep(0.05)
        assert "wal_000001.bin" not in node.wal_files(), (
            f"retention did not move after the restart: {node.wal_files()}")
        assert node.counter("ob_checkpoints_frozen") == 0
    finally:
        node.cleanup()


def test_under_none_a_failed_wal_sync_freezes_nothing():
    """`--fsync-policy none` makes no promise a failed sync could break, so it freezes nothing (#160).

    The WAL syncs the file a rotation leaves whatever the policy - since stage 5 of #151 at the next
    flush tick rather than at the rotation, which is why the failure is waited for rather than
    expected by the time the inserts return - and that sync is made to fail. Under a policy that
    promises something, that freezes the checkpoints until a restart; under `none` the checkpoints go
    on, and retention deletes the file the rotation left.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="fsync", OB_FAULT_ERRNO="EIO",
                     OB_FAULT_COUNT="1", OB_FAULT_POLICY="none", OB_FAULT_FLUSH_MS="300",
                     OB_FAULT_ROTATE_BYTES="65573")
    try:
        node.wait_until_answering()
        replies = node.insert_each(range(1000, 1600))
        assert all(r == "OK" for r in replies.values())
        deadline = time.time() + patience(10)
        while node.injections() == 0 and time.time() < deadline:
            time.sleep(0.05)
        assert node.injections() == 1, (
            f"the sync of the file the rotation left did not fail:\n{node.fault_log_text()}")
        deadline = time.time() + patience(20)
        while WAL_SEGMENT in node.wal_files() and time.time() < deadline:
            time.sleep(0.05)
        assert WAL_SEGMENT not in node.wal_files(), (
            f"retention stopped under a policy that promises nothing: {node.wal_files()}")
        assert node.counter("ob_wal_fsync_errors_total") == 1
        assert node.counter("ob_checkpoints_frozen") == 0
    finally:
        node.cleanup()


def test_a_snapshot_whose_install_could_not_sync_is_installed_again():
    """#162: a replica records a snapshot's position only once the install synced it.

    The install renames the staged files into place and then syncs the data directory; the position
    that says "the store is this snapshot" is saved only after that. Here the sync fails - the first
    `syncfs()` of a fresh replica is its install's, because a node with nothing to flush syncs
    nothing - so the install is reported failed, the position is not saved, the checkpoints freeze
    (#160), and the replica asks again, is refused again and installs the snapshot a second time,
    whose sync succeeds. Every row arrives, and the log says the whole story once.

    The primary writes past two rotations with nobody connected, so retention has deleted the file
    a new replica asks for first: that is what makes its first request a snapshot.
    """
    repl_port = free_port()
    primary = FaultNode(OB_FAULT_POLICY="every", OB_FAULT_FLUSH_MS="200",
                        OB_FAULT_ROTATE_BYTES="65573",
                        OB_FAULT_EXTRA_ARGS=f"--replication-port {repl_port}")
    replica = None
    try:
        primary.wait_until_answering()
        prices = list(range(1000, 2100))
        assert all(r == "OK" for r in primary.insert_each(prices).values())
        deadline = time.time() + patience(30)
        while WAL_SEGMENT in primary.wal_files() and time.time() < deadline:
            time.sleep(0.1)
        assert WAL_SEGMENT not in primary.wal_files(), (
            f"retention kept the first WAL file, so the replica would catch up from the log: "
            f"{primary.wal_files()}")

        replica = FaultNode(OB_FAULT_PATH=DATA_DIR_PREFIX, OB_FAULT_OP="syncfs", OB_FAULT_ERRNO="EIO",
                            OB_FAULT_COUNT="1", OB_FAULT_POLICY="every", OB_FAULT_FLUSH_MS="200",
                            OB_FAULT_EXTRA_ARGS=f"--primary-host 127.0.0.1 --primary-port {repl_port}")
        replica.wait_until_answering()
        state = os.path.join(replica.data_dir, "repl_state.txt")

        def position_recorded() -> bool:
            text = open(state).read() if os.path.exists(state) else ""
            return bool(re.search(r"file_index=[1-9]", text) or re.search(r"byte_offset=[1-9]", text))

        # The failed install is followed by the replica's reconnect backoff (five seconds), and the
        # second bootstrap after it. Waited for as that, rather than for the rows: the first install
        # replaced the store, so every row is readable before the position is ever recorded - the
        # first version of this test waited for the rows and counted one install.
        deadline = time.time() + patience(60)
        while time.time() < deadline and not position_recorded():
            time.sleep(0.2)
        assert position_recorded(), f"the replica never recorded a position:\n{replica.log()[-2000:]}"
        assert node_count(replica, "action=fail", fault=True) == 1, replica.fault_log_text()
        assert replica.log().count("The installed snapshot could not be synced") == 1, (
            replica.log()[-2000:])
        assert replica.log().count("Installing a snapshot of") == 2, (
            f"the replica recorded the snapshot's position over an install whose sync failed, so it "
            f"never installed it again:\n{replica.log()[-2000:]}")
        assert sorted(select_prices(replica)) == prices, (
            f"the replica did not end with every row:\n{replica.log()[-2000:]}")
        assert replica.counter("ob_checkpoints_frozen") == 1
    finally:
        if replica is not None:
            replica.cleanup()
        primary.cleanup()


def node_count(node: FaultNode, needle: str, fault: bool = False) -> int:
    """How often `needle` appears in the node's log, or in its injector's log with `fault`."""
    return (node.fault_log_text() if fault else node.log()).count(needle)

HOUR_NS = 3600 * 1_000_000_000   # the columnar store's segment length


def insert_at(node: FaultNode, symbol: str, price: int, event_time_ns: int) -> str:
    """One INSERT with its event time, for a test that needs rows in two different segments."""
    return node.talk(f"INSERT {symbol} EX bid {price} 1 1 {event_time_ns}")[0]


def partial_segment_dirs(node: FaultNode) -> list[str]:
    """Segment directories with no `meta.json` - what a refused write leaves if nothing removes it.

    Segments live at `<data dir>/<symbol>/<exchange>/<start>_<end>[_n]`, and `meta.json` is written
    last, so a directory without one is a write that stopped part-way.
    """
    out = []
    root = node.data_dir
    for symbol in sorted(os.listdir(root)):
        for exchange in sorted(os.listdir(os.path.join(root, symbol))
                               if os.path.isdir(os.path.join(root, symbol)) else []):
            parent = os.path.join(root, symbol, exchange)
            if not os.path.isdir(parent):
                continue
            for span in sorted(os.listdir(parent)):
                seg = os.path.join(parent, span)
                if (re.fullmatch(r"\d+_\d+(_\d+)?", span) and os.path.isdir(seg)
                        and not os.path.exists(os.path.join(seg, "meta.json"))):
                    out.append(os.path.relpath(seg, root))
    return out


def prices_of(node: FaultNode, symbol: str) -> list[int]:
    reply = node.talk(f"SELECT * FROM '{symbol}'.'EX' WHERE timestamp BETWEEN 0 AND "
                      "9999999999999999999")[0]
    return [] if reply.startswith("ERR") else prices_in(reply)


def test_a_rollover_the_disk_refuses_mid_drain_appends_no_row_twice():
    """#160: a drain stopped by a segment write keeps only the rows it has not appended queued.

    A row whose event time crosses the segment's hour rolls the segment over, and the rollover
    writes it - from inside the drain. Since the segment writes are checked, a refused one throws
    there, part-way through the queue. Rows already appended must leave the queue, or the next
    drain appends them a second time into storage that never removes a row.

    Row 100 is in hour 1, rows 200 and 300 in hour 2, so appending 200 rolls hour 1 over, and the
    injector refuses that first `price.col`. The first `FLUSH` fails; the second writes both hours;
    every row is there once, before a restart and after one.
    """
    node = FaultNode(OB_FAULT_PATH="price.col", OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_COUNT="1", OB_FAULT_FLUSH_MS="3600000")
    base = 1_700_000_000 * 1_000_000_000 // HOUR_NS * HOUR_NS
    try:
        node.wait_until_answering()
        assert insert_at(node, "ROLL", 100, base + 1) == "OK"
        assert insert_at(node, "ROLL", 200, base + HOUR_NS + 1) == "OK"
        assert insert_at(node, "ROLL", 300, base + HOUR_NS + 2) == "OK"
        assert node.talk("FLUSH")[0].startswith("ERR"), "the refused rollover was not reported"
        assert node.injections() == 1, node.fault_log_text()
        assert node.talk("FLUSH")[0] == "OK"
        assert sorted(prices_of(node, "ROLL")) == [100, 200, 300], (
            f"a row was lost or appended twice by the drain the rollover stopped:\n"
            f"{node.log()[-1500:]}")
        node.kill_and_restart_without_faults()
        assert sorted(prices_of(node, "ROLL")) == [100, 200, 300]
    finally:
        node.cleanup()


def test_a_symbol_the_disk_refuses_leaves_the_others_written_and_readable():
    """#160: one store's refused segment does not take the flush's other segments with it.

    Two symbols in one flush, and the first `price.col` the flush writes is refused - whichever
    symbol that is. The other's segment is written **and merged**: an exception out of the loop
    used to skip the merge, and a written segment outside the index is rows no query returns until
    a restart finds them. The refused one's rows stay in memory and the next flush writes them.
    """
    node = FaultNode(OB_FAULT_PATH="price.col", OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC",
                     OB_FAULT_COUNT="1", OB_FAULT_FLUSH_MS="3600000")
    try:
        node.wait_until_answering()
        assert node.talk("INSERT AAA EX bid 111 1 1")[0] == "OK"
        assert node.talk("INSERT BBB EX bid 222 1 1")[0] == "OK"
        assert node.talk("FLUSH")[0].startswith("ERR"), "the refused segment was not reported"
        visible = {s: prices_of(node, s) for s in ("AAA", "BBB")}
        assert sorted(len(v) for v in visible.values()) == [0, 1], (
            f"exactly one symbol's segment should be readable after the first flush: {visible}")
        # And the refused one left nothing behind: a directory it began is removed, or a disk that
        # refused a write because it was full would keep what the write managed to put there.
        assert partial_segment_dirs(node) == [], partial_segment_dirs(node)
        assert node.talk("FLUSH")[0] == "OK"
        assert prices_of(node, "AAA") == [111] and prices_of(node, "BBB") == [222]
        node.kill_and_restart_without_faults()
        assert prices_of(node, "AAA") == [111] and prices_of(node, "BBB") == [222]
    finally:
        node.cleanup()


def test_a_writer_does_not_wait_for_the_flush_ticks_sync():
    """Stage 5 of #151: the flush tick syncs the WAL without the engine's lock.

    The tick's `fsync` held every writer for as long as it took - 7.9 ms at p50 and 25.8 ms at
    worst on the m9g.xlarge - and nothing that does not time a writer *while a sync runs* can see
    that. So the sync is made to last three seconds (the injector's delay mode) and ten writes are
    timed inside it: with the sync under the lock, the first of them waits for all of it. Then what
    the delay must not cost: every row, the one the slow sync covered and the ten written while it
    ran, comes back once.
    """
    stall_ms = 3000
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="fsync",
                     OB_FAULT_DELAY_MS=str(stall_ms), OB_FAULT_COUNT="1",
                     OB_FAULT_POLICY="interval", OB_FAULT_FLUSH_MS="200")
    try:
        node.wait_until_answering()
        assert node.insert_each([100]) == {100: "OK"}
        deadline = time.time() + patience(10)
        while "action=delay" not in node.fault_log_text() and time.time() < deadline:
            time.sleep(0.02)
        assert "action=delay" in node.fault_log_text(), (
            "the flush tick never synced the WAL, so nothing was measured:\n"
            + node.fault_log_text())

        started = time.monotonic()
        replies = node.insert_each(range(101, 111))
        took = time.monotonic() - started
        custom_metrics["ten_writes_during_a_3s_tick_sync_s"] = round(took, 3)
        assert all(r == "OK" for r in replies.values()), replies
        assert took < stall_ms / 1000 / 3, (
            f"ten writes took {took:.2f} s while the flush tick's sync was made to last "
            f"{stall_ms} ms - they waited for it, so the tick syncs under the engine's lock")

        time.sleep(stall_ms / 1000 + 0.5)   # the slow sync ends, and the next tick syncs the rest
        reply = node.talk("FLUSH", "SELECT * FROM 'SYM'.'EX' WHERE timestamp BETWEEN 0 AND "
                                   "9999999999999999999")[-1]
        assert sorted(prices_in(reply)) == list(range(100, 111)), reply
    finally:
        node.cleanup()


def test_a_failed_tick_sync_drains_nothing_and_the_next_one_drains_it_all():
    """The failure half of stage 5 of #151.

    The tick now takes its rows out of the queue before it syncs, so a sync that fails has to put
    them back - in front of anything written meanwhile - because draining them would move them out
    of the only place that still knows they were never synced. Five rows, the tick's first sync
    fails, and then every row exactly once: none lost with the failed sync, none twice from being
    queued again. Read without a `FLUSH`, so what drains them is the next tick and nothing else.
    """
    # A tick every two seconds, so "the failed tick drained nothing" is a window wide enough to look
    # into: rows visible between the failed tick and the next one were drained over a failed sync.
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="fsync", OB_FAULT_ERRNO="EIO",
                     OB_FAULT_COUNT="1", OB_FAULT_POLICY="interval", OB_FAULT_FLUSH_MS="2000")
    try:
        node.wait_until_answering()
        replies = node.insert_each(range(200, 205))
        assert all(r == "OK" for r in replies.values()), replies
        deadline = time.time() + patience(10)
        while node.injections() == 0 and time.time() < deadline:
            time.sleep(0.05)
        assert node.injections() == 1, node.fault_log_text()
        after_failure = node.talk("SELECT * FROM 'SYM'.'EX' WHERE timestamp BETWEEN 0 AND "
                                  "9999999999999999999")[-1]
        assert prices_in(after_failure) == [], (
            "the tick whose sync failed drained its rows anyway - into segments, over records that "
            f"never reached the disk: {after_failure}")
        # Published at the start of the next tick, as a delta (#113), so it is waited for rather
        # than read at the instant the sync failed.
        deadline = time.time() + patience(10)
        while node.counter("ob_wal_fsync_errors_total") == 0 and time.time() < deadline:
            time.sleep(0.05)
        assert node.counter("ob_wal_fsync_errors_total") == 1

        deadline = time.time() + patience(10)
        reply = ""
        while time.time() < deadline:
            reply = node.talk("SELECT * FROM 'SYM'.'EX' WHERE timestamp BETWEEN 0 AND "
                              "9999999999999999999")[-1]
            if len(prices_in(reply)) >= 5:
                break
            time.sleep(0.2)
        assert sorted(prices_in(reply)) == list(range(200, 205)), reply
        assert node.counter("ob_flush_errors_total") >= 1, "the failed tick was not counted"
    finally:
        node.cleanup()


def test_rows_written_during_a_tick_sync_survive_a_crash_after_it():
    """The position a tick stamps into its segments, through a crash (stage 5 of #151).

    A segment records how far into the WAL its rows go, and replay starts past it (#63). The tick
    now syncs without the lock, so rows keep arriving while it does, and their records are *after*
    the position its sync covers. Stamped with the log's position at drain time instead, a segment
    would claim records whose rows are still queued - and a crash before the next tick would lose
    them to a replay that skips them, with every row still answered `OK`.

    So: row 300 before a sync made to last two seconds, rows 301-303 during it, a kill after that
    tick has drained 300 and before the next one drains the rest, and a restart. All four must come
    back, each once.
    """
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="fsync", OB_FAULT_DELAY_MS="2000",
                     OB_FAULT_COUNT="1", OB_FAULT_POLICY="interval", OB_FAULT_FLUSH_MS="4000")
    select = "SELECT * FROM 'SYM'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999"
    try:
        node.wait_until_answering()
        assert node.insert_each([300]) == {300: "OK"}
        deadline = time.time() + patience(15)
        while "action=delay" not in node.fault_log_text() and time.time() < deadline:
            time.sleep(0.02)
        assert "action=delay" in node.fault_log_text(), node.fault_log_text()
        replies = node.insert_each([301, 302, 303])
        assert all(r == "OK" for r in replies.values()), replies

        # The slow tick drains 300 when its sync ends; the rest wait four seconds for the next.
        deadline = time.time() + patience(10)
        seen = []
        while time.time() < deadline:
            seen = prices_in(node.talk(select)[-1])
            if seen:
                break
            time.sleep(0.05)
        assert seen == [300], f"the slow tick drained {seen}, not the one row its sync covered"

        node.kill_and_restart_without_faults()
        assert sorted(prices_in(node.talk(select)[-1])) == [300, 301, 302, 303], (
            "a row written while the tick synced was lost or doubled by the replay after a crash")
    finally:
        node.cleanup()


def test_retention_moves_under_every_while_writes_flow():
    """The retention floor rises under `--fsync-policy every` while writes keep arriving (#160).

    Under `every` each write syncs the WAL itself, which also takes the checkpoint the last tick
    appended to the device - so a tick usually finds **nothing owed**, and that is the path on which
    the floor has to move. Stage 5 of #151 split the tick's sync out of the lock, and its first
    version promoted the floor only on the path that synced: with writes flowing, retention under
    `every` never moved and the WAL grew for as long as they did.

    The directory is read the moment the writes stop, which is the window that decides it: with the
    floor stuck, the first promotion comes two ticks later - one to append a checkpoint nobody's
    write has synced, and one to find it owed.
    """
    node = FaultNode(OB_FAULT_POLICY="every", OB_FAULT_FLUSH_MS="300", OB_FAULT_ROTATE_BYTES="65573")
    try:
        node.wait_until_answering()
        replies = node.insert_each(range(1000, 4000))
        files = node.wal_files()
        assert all(r == "OK" for r in replies.values())
        assert any(f >= "wal_000003.bin" for f in files), (
            f"the writes did not rotate the WAL past its third file, so there was nothing for "
            f"retention to delete: {files}")
        assert WAL_SEGMENT not in files, (
            f"retention kept every WAL file while writes flowed under `every`: {files}")
    finally:
        node.cleanup()


def test_a_writer_does_not_wait_for_a_segment_the_ticks_drain_writes():
    """Stage 5 of #151: the flush tick drains its rows without the engine's lock.

    The drain appends each row to its store, and a row whose event time crosses its segment's hour
    rolls the segment over - which **writes** it, from inside the drain. With the drain under the
    lock, every writer waited for that write, and with only the tick's sync taken out of the lock
    the drain was what was left: 11 ms at p99.9 on the m9g.xlarge. Here row 100 is in hour 1 and
    row 200 in hour 2, both queued before the first tick, so that tick's drain rolls hour 1 over and
    the injector makes that write - the first `price.col` this node writes - last three seconds.
    Ten writes timed inside it must not wait for it; and after it, every row is there once.
    """
    stall_ms = 3000
    node = FaultNode(OB_FAULT_PATH="price.col", OB_FAULT_OP="write",
                     OB_FAULT_DELAY_MS=str(stall_ms), OB_FAULT_COUNT="1",
                     OB_FAULT_POLICY="interval", OB_FAULT_FLUSH_MS="3000",
                     OB_FAULT_EXTRA_ARGS="--log-level DEBUG")
    base = 1_700_000_000 * 1_000_000_000 // HOUR_NS * HOUR_NS
    try:
        node.wait_until_answering()
        assert insert_at(node, "ROLL", 100, base + 1) == "OK"
        assert insert_at(node, "ROLL", 200, base + HOUR_NS + 1) == "OK"
        deadline = time.time() + patience(15)
        while "action=delay" not in node.fault_log_text() and time.time() < deadline:
            time.sleep(0.02)
        assert "action=delay" in node.fault_log_text(), node.fault_log_text()

        started = time.monotonic()
        replies = node.insert_each(range(101, 111))
        took = time.monotonic() - started
        custom_metrics["ten_writes_during_a_3s_drain_rollover_s"] = round(took, 3)
        assert all(r == "OK" for r in replies.values()), replies
        assert took < stall_ms / 1000 / 3, (
            f"ten writes took {took:.2f} s while a segment the flush tick's drain rolled over took "
            f"{stall_ms} ms to write - they waited for it, so the tick drains under the engine's lock")

        time.sleep(stall_ms / 1000 + 0.5)
        # The premise, read once the slow write is over, because the rollover says so after it: the
        # slow write was the drain's rollover rather than a segment the tick writes after the drain,
        # which never held the lock. Nothing was flushed before this tick, so the first `price.col`
        # this node wrote is the one that rolled over.
        assert "Segment rolled over" in node.log(), (
            "the first segment write was not a rollover inside the drain, so this measured nothing")
        assert node.talk("FLUSH")[0] == "OK"
        assert sorted(prices_of(node, "ROLL")) == [100, 200]
        assert sorted(prices_of(node, "SYM")) == list(range(101, 111))
    finally:
        node.cleanup()


def test_rows_written_during_a_ticks_drain_survive_a_crash_after_it():
    """The drain without the lock, through a crash (stage 5 of #151).

    Rows keep arriving while a tick drains now, and they are queued for the next tick rather than
    drained into this one's segments - which carry the position of the tick's sync, before those
    rows' records. Rows 100 and 200 are drained by a tick whose rollover write is made to last two
    seconds, rows 101-103 are written during it, the node is killed after that tick has made its
    segments readable and before the next one runs, and after a restart all five are back, each once.
    """
    node = FaultNode(OB_FAULT_PATH="price.col", OB_FAULT_OP="write", OB_FAULT_DELAY_MS="2000",
                     OB_FAULT_COUNT="1", OB_FAULT_POLICY="interval", OB_FAULT_FLUSH_MS="4000")
    base = 1_700_000_000 * 1_000_000_000 // HOUR_NS * HOUR_NS
    try:
        node.wait_until_answering()
        assert insert_at(node, "ROLL", 100, base + 1) == "OK"
        assert insert_at(node, "ROLL", 200, base + HOUR_NS + 1) == "OK"
        deadline = time.time() + patience(15)
        while "action=delay" not in node.fault_log_text() and time.time() < deadline:
            time.sleep(0.02)
        assert "action=delay" in node.fault_log_text(), node.fault_log_text()
        replies = node.insert_each([101, 102, 103])
        assert all(r == "OK" for r in replies.values()), replies

        # The slow tick merges ROLL's segments when its write ends; SYM's rows wait for the next.
        deadline = time.time() + patience(10)
        while sorted(prices_of(node, "ROLL")) != [100, 200] and time.time() < deadline:
            time.sleep(0.05)
        assert sorted(prices_of(node, "ROLL")) == [100, 200], node.log()[-1500:]
        assert prices_of(node, "SYM") == [], (
            "the rows written during the drain were drained by it - they have to wait for the next "
            "tick, whose sync is the one that covers their records")

        node.kill_and_restart_without_faults()
        assert sorted(prices_of(node, "ROLL")) == [100, 200]
        assert sorted(prices_of(node, "SYM")) == [101, 102, 103], (
            "a row written while the tick drained was lost or doubled by the replay after a crash")
    finally:
        node.cleanup()


def test_a_writer_does_not_wait_for_the_sync_of_a_file_a_rotation_left():
    """Stage 5 of #151: a file the WAL rotated away from is synced by the flush tick, unlocked.

    `open_current()` synced the file it was leaving before closing it - on the thread of the writer
    whose record crossed the threshold, under the engine's lock. Under `--fsync-policy none` nothing
    else syncs the WAL, so that was the whole file at once: 295 and 323 ms measured on the
    m9g.xlarge at 512 MB, and a pipelining writer's worst batch was exactly that `fsync`. Here the
    threshold is the smallest the flag takes, and the first `fsync` of `wal_000000.bin` - under
    `none` the one that settles it after the rotation, since nothing else syncs that file - is made
    to last three seconds. A second session fills the file; ten writes on this one are timed inside
    the stall.
    """
    stall_ms = 3000
    node = FaultNode(OB_FAULT_PATH=WAL_SEGMENT, OB_FAULT_OP="fsync",
                     OB_FAULT_DELAY_MS=str(stall_ms), OB_FAULT_COUNT="1",
                     OB_FAULT_POLICY="none", OB_FAULT_FLUSH_MS="200",
                     OB_FAULT_ROTATE_BYTES=ROTATE_BYTES)
    try:
        node.wait_until_answering()
        filled: dict[int, str] = {}
        filler = threading.Thread(
            target=lambda: filled.update(node.insert_each(range(1, ROTATING_INSERT + 2))))
        filler.start()
        deadline = time.time() + patience(20)
        while "action=delay" not in node.fault_log_text() and time.time() < deadline:
            time.sleep(0.02)
        assert "action=delay" in node.fault_log_text(), (
            "nothing synced the first WAL file, so nothing was measured:\n" + node.fault_log_text())

        started = time.monotonic()
        replies = node.insert_each(range(9001, 9011))
        took = time.monotonic() - started
        filler.join(timeout=patience(30))
        custom_metrics["ten_writes_during_a_3s_rotation_sync_s"] = round(took, 3)
        assert all(r == "OK" for r in replies.values()), replies
        assert all(r == "OK" for r in filled.values()) and len(filled) == ROTATING_INSERT + 1, filled
        assert took < stall_ms / 1000 / 3, (
            f"ten writes took {took:.2f} s while the sync of a file the WAL rotated away from took "
            f"{stall_ms} ms - they waited for it, so it runs under the engine's lock")
        assert node.wal_files() == ["wal_000000.bin", "wal_000001.bin"], node.wal_files()

        time.sleep(stall_ms / 1000 + 0.5)
        assert node.talk("FLUSH")[0] == "OK"
        back = set(prices_in(node.talk(SELECT_ALL)[0]))
        assert back >= set(range(1, ROTATING_INSERT + 2)) | set(range(9001, 9011))
    finally:
        node.cleanup()
