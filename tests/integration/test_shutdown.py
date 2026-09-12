"""A node stops when it is asked to, even with a client attached (#106).

Measured before the bound existed, on this machine: `SIGTERM` with nothing connected exits in
**0.11 s**; with one **idle** client attached the process was **still running after 60 s**.

Not a hang, which is what made it an item rather than a crash report: on `SIGTERM` the listener
closes at once - a new connection is refused immediately - and the loop then waits for every
existing session to end, exiting 0.00 s after the last client disconnects. An idle client never
disconnects, and a long-lived client is the normal case for a database: a connection pool, a
`SUBSCRIBE` stream, a monitoring probe. So a supervisor reaches its own timeout and sends `SIGKILL`,
and the flush and checkpoint the graceful path exists for are exactly what does not run.

Standalone nodes rather than a fixture's, because these tests end the process they measure - and the
binary comes from `server_binary_path()` rather than from a path built here, which is pitfall 77.
"""

from __future__ import annotations

import os
import re
import signal
import socket
import subprocess
import tempfile
import time

import pytest
from conftest import free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()

custom_metrics: dict = {}


class Node:
    """One standalone server, logging to a file in its own data directory.

    Never a `subprocess.PIPE` nobody reads: it fills at 64 KB and the node then blocks inside
    `write()`, which looks alive to `poll()` and answers nothing (pitfall 91). The file is also
    where the drain line is read from below.
    """

    def __init__(self, drain_timeout_ms: int | None = None):
        self.data_dir = tempfile.mkdtemp(prefix="ob_shutdown_")
        self.port = free_port()
        self._log_path = os.path.join(self.data_dir, "node.log")
        self._log = open(self._log_path, "a", encoding="utf-8", buffering=1)
        argv = [SERVER, "--port", str(self.port), "--data-dir", self.data_dir,
                "--metrics-port", "0"]
        if drain_timeout_ms is not None:
            argv += ["--drain-timeout-ms", str(drain_timeout_ms)]
        self.proc = subprocess.Popen(argv, stdout=self._log, stderr=subprocess.STDOUT)

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

    def sigterm_and_time(self, allowance: float) -> float | None:
        """Seconds until the process left, or None if it was still running at `allowance`."""
        started = time.monotonic()
        self.proc.send_signal(signal.SIGTERM)
        deadline = started + allowance
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return time.monotonic() - started
            time.sleep(0.1)
        return None

    def log(self) -> str:
        self._log.flush()
        with open(self._log_path, encoding="utf-8", errors="replace") as handle:
            return handle.read()

    def cleanup(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self._log.close()


def test_a_node_with_a_client_attached_still_stops() -> None:
    """Three assertions, because each one passes on its own while the others are broken.

    A timing alone passes for a node that exits whatever happens. An exit code alone passes for a
    node whose client happened to leave first. And the log line alone passes for a node that says
    it cut a session and then sits there.
    """
    node = Node(drain_timeout_ms=2000)
    client = None
    try:
        node.wait_until_answering()
        client = socket.create_connection(("127.0.0.1", node.port), timeout=10)
        client.recv(4096)                      # the banner
        client.sendall(b"PING\n")
        time.sleep(0.3)
        assert b"PONG" in client.recv(4096), "the node was not answering before the shutdown"

        took = node.sigterm_and_time(allowance=patience(20))
        custom_metrics["shutdown_with_client_seconds"] = None if took is None else round(took, 2)
        assert took is not None, (
            "the node did not exit after SIGTERM while a client held a connection. Before #106 "
            "that was the ordinary outcome, and a supervisor's answer to it is SIGKILL - which is "
            f"when the flush the graceful path exists for does not happen. Log:\n{node.log()[-800:]}")
        assert node.proc.returncode == 0, (
            f"the node left with {node.proc.returncode} rather than 0. A supervisor reads the exit "
            f"mode, which is what #102 was about from the other side")

        assert re.search(r"Drain deadline of \d+ ms reached with 1 session\(s\) still open",
                         node.log()), (
            "the node exited without saying it cut a session, so this run does not distinguish the "
            "bound firing from the client having left first:\n" + node.log()[-800:])
    finally:
        if client is not None:
            client.close()
        node.cleanup()


def test_a_node_with_nobody_connected_stops_at_once() -> None:
    """The control that makes the test above mean something.

    If an empty node took its whole drain budget, the bound would be a sleep rather than a deadline -
    and the assertion above would pass for a server that ignores its sessions entirely. Measured
    before and after the change: 0.11 s, unchanged.
    """
    node = Node(drain_timeout_ms=2000)
    try:
        node.wait_until_answering()
        took = node.sigterm_and_time(allowance=patience(20))
        custom_metrics["shutdown_idle_seconds"] = None if took is None else round(took, 2)
        assert took is not None, "an empty node did not exit at all"
        assert took < 1.5, (
            f"an empty node took {took:.2f} s to stop, which is inside its 2 s drain budget rather "
            f"than below it: the bound is being waited out instead of checked")
        assert node.proc.returncode == 0
    finally:
        node.cleanup()


def test_zero_means_wait_for_ever_and_has_to_be_asked_for() -> None:
    """The other half of the flag, and the reason the default is not this.

    `--drain-timeout-ms 0` keeps the behaviour #106 is about: wait for the client, however long that
    takes. It stays available because a deployment that would rather hang than cut a session is a
    legitimate choice - it just cannot be what a supervisor meets by default. Asserting it also
    stops the fix from being "the flag is ignored and the node always exits", which every assertion
    above would pass.
    """
    node = Node(drain_timeout_ms=0)
    client = None
    try:
        node.wait_until_answering()
        client = socket.create_connection(("127.0.0.1", node.port), timeout=10)
        client.recv(4096)

        # Deliberately short: the claim is "it is still there", and waiting longer only makes a
        # passing test slower. The node is killed in the teardown, which is what an operator's
        # supervisor does too.
        took = node.sigterm_and_time(allowance=4.0)
        custom_metrics["shutdown_unbounded_still_running"] = took is None
        assert took is None, (
            f"a node asked to wait indefinitely exited after {took:.2f} s, so the 0 case is not "
            f"honoured - and the default bound is then the only behaviour there is")

        # And it leaves the moment the client does, which is what "waiting" means.
        client.close()
        client = None
        left = node.sigterm_and_time(allowance=patience(15))
        assert left is not None and node.proc.returncode == 0, (
            "with the client gone the node still did not exit, so it was not waiting for the "
            "session at all")
    finally:
        if client is not None:
            client.close()
        node.cleanup()
