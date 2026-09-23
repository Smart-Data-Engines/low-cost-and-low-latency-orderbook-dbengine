"""A node restarted with `--ttl-hours` expires what is older than the retention, and nothing else (#163).

Measured before the fix, on the i3-7100U, a node restarted with `--ttl-hours 24` on a machine up for
21.5 hours held **0 of 200 rows and none of their two segment directories** after its first sweep,
where the same restart without the flag held all 200. The sweep compared segment times - event
times, nanoseconds since the Unix epoch - with a reading of `steady_clock`, which counts from boot:
on a machine up for less than the retention the subtraction wrapped to a cutoff past every
timestamp. On a machine up for longer the cutoff was a few hours into 1970, and rows dated two
hours ago under `--ttl-hours 1` were never expired (also measured: 200 of 200 left after the
sweeps).

The retention here is taken from this machine's own uptime, so the test holds the premise under
which the old sweep deleted everything on any machine - a CI runner has been up for minutes - and
the rows it must expire are what shows a sweep ran at all.

A standalone node rather than a fixture's, because the test restarts the process it measures, and
the binary comes from `server_binary_path()` (pitfall 77).
"""

from __future__ import annotations

import math
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import pytest
from conftest import free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()
ROWS = 100
NS_PER_HOUR = 3600 * 10**9


def request(port: int, command: str, timeout: float = 10.0) -> str:
    """One command and its whole response, which ends with an empty line - except `PONG` and an
    `ERR`, which are one line and nothing after it.

    Not `send_command()`, which returns at the first newline: enough for `PING`, and for `SELECT`
    a count of whatever the first read happened to hold.
    """
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

    def __init__(self, data_dir: str, extra: list[str]):
        self.data_dir = data_dir
        self.port = free_port()
        self._log = open(os.path.join(data_dir, "node.log"), "a", encoding="utf-8", buffering=1)
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", data_dir, "--metrics-port", "0",
             "--drain-timeout-ms", "1000", *extra],
            stdout=self._log, stderr=subprocess.STDOUT)
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

    def rows(self, symbol: str) -> int:
        """Rows this node returns for `symbol`. A symbol whose every segment has expired is one the
        node no longer knows, and it says so: zero rows rather than a failure here."""
        reply = request(self.port, f"SELECT * FROM '{symbol}'.'EX' WHERE timestamp BETWEEN 0 "
                                   "AND 9999999999999999999")
        if reply.startswith("ERR ") and "not found" in reply:
            return 0
        assert not reply.startswith("ERR "), reply
        return sum(1 for line in reply.splitlines() if line.split("\t")[0].isdigit())

    def status(self, field: str) -> int:
        match = re.search(rf"^{field}: (\d+)", request(self.port, "STATUS"), re.M)
        assert match, f"STATUS has no {field}"
        return int(match.group(1))

    def write(self, symbol: str, event_time_ns: int | None) -> None:
        """ROWS single-level inserts; with an event time, each dated a nanosecond after the last."""
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as s:
            reader = s.makefile("rb")
            while reader.readline().strip():   # the greeting
                pass
            for i in range(ROWS):
                tail = "" if event_time_ns is None else f" {event_time_ns + i}"
                s.sendall(f"INSERT {symbol} EX bid {1000 + i} 1 1{tail}\n".encode())
                assert reader.readline().strip() == b"OK"
                reader.readline()   # the blank line that ends a response
        assert request(self.port, "FLUSH", timeout=30).startswith("OK")

    def stop(self) -> None:
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=patience(20))
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self._log.close()


def machine_uptime_seconds() -> float:
    with open("/proc/uptime", encoding="ascii") as f:
        return float(f.read().split()[0])


def test_a_restart_with_a_retention_longer_than_the_machine_has_been_up_expires_only_older_rows():
    up = machine_uptime_seconds()
    ttl_hours = math.ceil(up / 3600) + 1
    assert ttl_hours * 3600 > up, "the premise is a retention longer than this machine's uptime"

    data_dir = tempfile.mkdtemp(prefix="ob_ttl_")
    try:
        first = Node(data_dir, [])
        first.write("OLD", time.time_ns() - (ttl_hours + 2) * NS_PER_HOUR)
        first.write("FRESH", None)
        assert (first.rows("OLD"), first.rows("FRESH")) == (ROWS, ROWS)
        first.stop()

        second = Node(data_dir, ["--ttl-hours", str(ttl_hours), "--ttl-scan-interval-seconds", "1"])
        deadline = time.time() + patience(15)
        while second.status("ttl_segments_deleted") == 0 and time.time() < deadline:
            time.sleep(0.2)
        deleted = second.status("ttl_segments_deleted")
        old, fresh = second.rows("OLD"), second.rows("FRESH")
        log_tail = second.log()[-1500:]
        second.stop()

        # A sweep ran - rows dated past the retention are what it was asked to expire...
        assert deleted >= 1, f"no sweep deleted anything within 15 s; log tail:\n{log_tail}"
        assert old == 0, f"{old} rows dated {ttl_hours + 2} h ago survived a {ttl_hours} h retention"
        # ...and it expired nothing younger than the retention.
        assert fresh == ROWS, (
            f"{fresh} of {ROWS} rows written a moment ago survived a sweep with a {ttl_hours} h "
            f"retention on a machine up {up / 3600:.1f} h; log tail:\n{log_tail}")
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
