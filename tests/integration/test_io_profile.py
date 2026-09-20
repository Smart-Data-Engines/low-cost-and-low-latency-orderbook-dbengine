"""The latency profiles, on a live node — #144.

`--profile boost` (or `--io-spin-us` on its own) keeps the io thread polling for a bounded window
after the last event instead of blocking immediately. What it buys is the kernel wake-up, measured
on an m9g.xlarge over loopback at 1.6 µs of an 8.0 µs round trip — the same amount off p50 and p99,
with the minimum unchanged, which is what says wake-up rather than tail.

**What this module adds over the unit tests, which is the only reason it exists.** `io_wait_ms()`
is pinned exhaustively in C++, the provenance of a value the profile chose is pinned against
`format_config()`, and the refusal of an unknown name is a death test. None of those starts a node.
Two questions are left, and both are about a process:

  * a node with the mode on **serves normally** — the window is inside the event loop, so a
    mistake there costs missed events rather than a wrong answer, and a wrong answer is what a
    client sees;
  * the window **closes**. The figures above come from a bare `epoll_wait(..., 0)`, which on an
    idle node burns a core for ever; the shipped mode spins for one window after the last event and
    then blocks. That is the difference between "up to one core while traffic flows" and "one
    core", and it is the only claim here that nothing else checks.

These tests spawn their own nodes, so they go through `conftest.server_binary_path()` and
`conftest.free_port()` — the two things a module that starts its own nodes gets wrong by growing a
copy of them (see the static guards in `test_smoke.py`).
"""

from __future__ import annotations

import os
import signal
import subprocess
import time

import pytest

from conftest import free_port, patience, send_command, server_binary_path

pytestmark = pytest.mark.smoke

CLOCK_TICKS = os.sysconf("SC_CLK_TCK")


def cpu_seconds(pid: int) -> float:
    """utime + stime of one process, from `/proc`.

    Fields 14 and 15 of `/proc/<pid>/stat`, counted from the closing parenthesis of the command
    name rather than by splitting the whole line: a command name may contain a space, and every
    field index after it then shifts.
    """
    with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as handle:
        after_name = handle.read().rsplit(") ", 1)[1].split()
    return (int(after_name[11]) + int(after_name[12])) / CLOCK_TICKS


class Node:
    def __init__(self, tmp_path, *args: str) -> None:
        self.port = free_port()
        tmp_path.mkdir(parents=True, exist_ok=True)   # the second test gives a subdirectory per profile
        self.log = open(tmp_path / "node.log", "w", encoding="utf-8")
        self.process = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port),
             "--data-dir", str(tmp_path / "data"),
             "--metrics-port", str(free_port()),
             "--drain-timeout-ms", "2000", *args],
            stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + patience(30)
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"node exited with {self.process.returncode}: {self.tail()}")
            try:
                if "PONG" in send_command(self.port, "PING", timeout=1.0):
                    return
            except Exception:
                time.sleep(0.1)
        raise AssertionError(f"node never answered PING: {self.tail()}")

    def tail(self) -> str:
        self.log.flush()
        with open(self.log.name, "r", encoding="utf-8") as handle:
            return handle.read()[-2000:]

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=patience(15))
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=patience(15))
        self.log.close()


@pytest.mark.parametrize("profile", ["eco", "boost"])
def test_a_node_serves_the_same_answers_under_either_profile(tmp_path, profile) -> None:
    """The claim that a profile is a named set of the knobs and not a second code path.

    Parametrised rather than written twice, and `eco` is not decoration: it is the control that
    says the assertions below are about the mode rather than about whether a node works at all.
    """
    node = Node(tmp_path, "--profile", profile)
    try:
        assert "PONG" in send_command(node.port, "PING")
        symbol = f"PROF{profile.upper()}"
        for price in (100, 101, 102):
            reply = send_command(node.port, f"INSERT {symbol} EX bid {price} 5 1")
            assert reply.startswith("OK"), reply
        assert send_command(node.port, "FLUSH").startswith("OK")

        rows = [line for line in send_command(node.port, f"SELECT * FROM '{symbol}'.'EX'")
                .splitlines()[2:] if line.strip()]
        assert len(rows) == 3, f"{len(rows)} rows under --profile {profile}: {rows}"

        printed = send_command(node.port, "STATUS")
        assert printed, "a node under --profile boost answered STATUS with nothing"
    finally:
        node.close()


def test_the_spin_window_closes_so_an_idle_node_does_not_hold_a_core(tmp_path) -> None:
    """The bound on the cost, and the one claim in this item nothing else can check.

    Three orders of magnitude clear of the value, deliberately (#129's shape): with a 50 µs window
    and a 100 ms blocking timeout an idle node spins about ten times a second, which is half a
    millisecond of CPU per second. A bare timeout of zero — the variant the published figures were
    measured on — spends the whole interval. So the threshold separates the two by a factor of
    roughly a thousand and is not a duration anyone has to tune.

    The control is in the same test rather than a second one: an `eco` node over the same idle
    window, so a threshold that had become satisfiable by any node at all would fail here.
    """
    idle = 3.0
    measured = {}
    for profile in ("eco", "boost"):
        node = Node(tmp_path / profile, "--profile", profile)
        try:
            before = cpu_seconds(node.process.pid)
            time.sleep(idle)
            measured[profile] = cpu_seconds(node.process.pid) - before
            # And it is still a working node afterwards, so the window closing is not the loop
            # having stopped looking.
            assert "PONG" in send_command(node.port, "PING"), node.tail()
        finally:
            node.close()

    assert measured["boost"] < idle * 0.3, (
        f"an idle --profile boost node spent {measured['boost']:.3f}s of CPU over {idle}s of doing "
        f"nothing (eco: {measured['eco']:.3f}s). The spin window is not closing, so the public "
        "cost is a core rather than 'up to one core while traffic flows'"
    )
    assert measured["eco"] < idle * 0.3, (
        f"the control spent {measured['eco']:.3f}s of CPU idling, so this threshold no longer "
        "distinguishes anything"
    )
