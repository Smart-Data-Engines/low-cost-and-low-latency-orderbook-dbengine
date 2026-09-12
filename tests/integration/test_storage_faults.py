"""What the engine does when the disk refuses a WAL write (#54).

These assert the half that already holds, and the injector is what made it assertable: a write
refused on a client's own thread is **reported to that client and not stored**, and no write that
was acknowledged is lost when a later one fails. Neither could be tested before, because a healthy
machine never returns ENOSPC.

`OB_FAULT_SIZE` matters more than it looks. This engine's WAL takes a 136-byte delta record on the
session thread and, from the flush loop, a 24-byte checkpoint and a 68-byte version vector. Failing
"the fourth write" is a different call on every run; failing "the 136-byte write" is the same one
every time. It is also what keeps these tests away from #112 — an ENOSPC on either background
record aborts the process, and that is filed rather than asserted here.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import tempfile
import time

import pytest
from conftest import fault_injector_path, free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()
WAL_SEGMENT = "wal_000000.bin"   # what the first WAL file is called; the delta records go here
DELTA_BYTES = "136"              # one INSERT of one level, header included


class FaultNode:
    """A standalone node with the storage fault injector preloaded.

    Logging to a file rather than a pipe nobody reads, for the reason `test_shutdown.py` gives:
    a pipe fills at 64 KB and the node then blocks inside `write()`, which looks alive to `poll()`
    and answers nothing.
    """

    def __init__(self, **fault: str):
        injector = fault_injector_path()
        assert injector is not None, (
            "libobfault.so was not built. Failing rather than skipping: a fault-injection test "
            "that injects nothing is indistinguishable from an engine that survives the fault"
        )
        self.data_dir = tempfile.mkdtemp(prefix="ob_storage_fault_")
        self.port = free_port()
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

        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", self.data_dir,
             "--metrics-port", "0", "--drain-timeout-ms", "2000", "--fsync-policy", "every"],
            env=env, stdout=self._log, stderr=subprocess.STDOUT)

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

    def injections(self) -> int:
        """How many faults actually fired. Zero means this test measured nothing."""
        if not os.path.exists(self.fault_log):
            return 0
        with open(self.fault_log, encoding="utf-8", errors="replace") as handle:
            return sum(line.count("action=fail") + line.count("action=short") for line in handle)

    def log(self) -> str:
        self._log.flush()
        with open(self._log_path, encoding="utf-8", errors="replace") as handle:
            return handle.read()

    def restart_without_faults(self) -> None:
        self.stop()
        self._log = open(self._log_path, "a", encoding="utf-8", buffering=1)
        env = {k: v for k, v in os.environ.items() if not k.startswith("OB_FAULT_")}
        env.pop("LD_PRELOAD", None)
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", self.data_dir,
             "--metrics-port", "0", "--drain-timeout-ms", "2000", "--fsync-policy", "every"],
            env=env, stdout=self._log, stderr=subprocess.STDOUT)
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
