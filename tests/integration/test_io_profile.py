"""The profiles, on a live node — #144, and stage 4 of #151, which made `boost` the default.

`boost` gives the node one client event loop per usable CPU, and keeps each polling for a bounded
window after its last event instead of blocking immediately, where the process has a CPU to spare.
What the window buys is the kernel wake-up, measured on an m9g.xlarge over loopback at about 1.4 µs
of a 7.6 µs round trip. `eco` is the engine as it was before either knob: one loop, blocking.

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

Stage 4 adds a third: a node started with **no flags** is sized to the machine it runs on, and
the loops its log says it chose are the loops it runs — counted as threads, not read back from the
same sentence that claimed them.

These tests spawn their own nodes, so they go through `conftest.server_binary_path()` and
`conftest.free_port()` — the two things a module that starts its own nodes gets wrong by growing a
copy of them (see the static guards in `test_smoke.py`).
"""

from __future__ import annotations

import os
import re
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


def io_loops(pid: int) -> list[str]:
    """The node's client event loops, as the kernel names its threads: `ob-io-0` ... `ob-io-N-1`.

    The main thread runs the first loop and carries its name, so this is the whole set.
    """
    names = []
    for tid in os.listdir(f"/proc/{pid}/task"):
        with open(f"/proc/{pid}/task/{tid}/comm", "r", encoding="utf-8") as handle:
            names.append(handle.read().strip())
    return sorted(name for name in names if name.startswith("ob-io-"))


class Node:
    def __init__(self, tmp_path, *args: str, prefix: tuple[str, ...] = ()) -> None:
        self.port = free_port()
        tmp_path.mkdir(parents=True, exist_ok=True)   # the second test gives a subdirectory per profile
        self.log = open(tmp_path / "node.log", "w", encoding="utf-8")
        self.process = subprocess.Popen(
            [*prefix, server_binary_path(), "--port", str(self.port),
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
        return self.text()[-2000:]

    def text(self) -> str:
        """The whole log. The lines a node writes while resolving its configuration are its
        first, and a node's startup is a few kilobytes, so `tail()` does not reach them."""
        self.log.flush()
        with open(self.log.name, "r", encoding="utf-8") as handle:
            return handle.read()

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

    Three orders of magnitude clear of the value, deliberately (#129's shape): a loop spins for one
    window after an event and then blocks for 100 ms at a time, so an idle node spends next to
    nothing, while a bare timeout of zero spends the whole interval on every loop. The node measured
    is the **default** one, since stage 4 of #151: one loop per usable CPU and the window where the
    rule gives one - requirement 4.4, "100% of the cores" means under load, not heating an idle
    machine.

    The control is in the same test rather than a second one: an `eco` node over the same idle
    window, so a threshold that had become satisfiable by any node at all would fail here.
    """
    idle = 3.0
    measured = {}
    for profile in ("eco", "default"):
        node = Node(tmp_path / profile, *(("--profile", "eco") if profile == "eco" else ()))
        try:
            before = cpu_seconds(node.process.pid)
            time.sleep(idle)
            measured[profile] = cpu_seconds(node.process.pid) - before
            # And it is still a working node afterwards, so the window closing is not the loop
            # having stopped looking.
            assert "PONG" in send_command(node.port, "PING"), node.tail()
        finally:
            node.close()

    assert measured["default"] < idle * 0.3, (
        f"an idle node on the default profile spent {measured['default']:.3f}s of CPU over {idle}s "
        f"of doing nothing (eco: {measured['eco']:.3f}s). A spin window is not closing, so the "
        "public cost is a core per loop rather than 'up to one core while traffic flows'"
    )
    assert measured["eco"] < idle * 0.3, (
        f"the control spent {measured['eco']:.3f}s of CPU idling, so this threshold no longer "
        "distinguishes anything"
    )


def profile_line(log: str) -> tuple[int, int, int]:
    """(loops the reason names, io-threads, io-spin-us) from the node's startup line."""
    found = re.search(r"io profile boost: (\d+) client event loops?, one per usable CPU.*?"
                      r"\(io-threads=(\d+) io-spin-us=(\d+)\)", log)
    assert found, f"no boost profile line in the node's log:\n{log}"
    return int(found.group(1)), int(found.group(2)), int(found.group(3))


def test_a_node_with_no_flags_runs_one_loop_per_usable_cpu(tmp_path) -> None:
    """Stage 4 of #151: `boost` is the default, and its loops are the loops the node runs.

    Three sources that must agree and do not share a code path: the mask this test process has
    (the node inherits it), the `machine:` and `io profile` lines the node logs, and the threads
    the kernel lists for it.
    """
    node = Node(tmp_path)
    try:
        log = node.text()
        machine = re.search(r"machine: (\d+) usable CPUs?: affinity (\d+)", log)
        assert machine, f"no machine line in the node's log:\n{log}"
        usable, affinity = int(machine.group(1)), int(machine.group(2))
        assert affinity == len(os.sched_getaffinity(0)), (
            f"the node counted {affinity} CPUs in a mask this process sees as "
            f"{len(os.sched_getaffinity(0))}")
        said, threads, _ = profile_line(log)
        assert said == threads == min(usable, 64), log
        loops = io_loops(node.process.pid)
        assert len(loops) == threads, f"the log says {threads} loops, the kernel lists {loops}"
        # And every loop is serving: a connection per loop, each answered.
        for _ in range(threads):
            assert "PONG" in send_command(node.port, "PING"), node.tail()
    finally:
        node.close()


def test_a_node_on_one_cpu_runs_one_loop_and_does_not_spin(tmp_path) -> None:
    """The case the rule refuses the spin in, on a live node: one CPU in the mask.

    Measured before the rule was written: a client sharing the only CPU had a p99 of 5.9 µs with
    no spin and 13.7 µs with a 10 µs window. The node must also say why, because a node that quietly
    does less than its profile's name promises is the weaker mode nobody can see.
    """
    cpu = min(os.sched_getaffinity(0))
    node = Node(tmp_path, prefix=("taskset", "-c", str(cpu)))
    try:
        log = node.text()
        said, threads, spin = profile_line(log)
        assert (said, threads, spin) == (1, 1, 0), log
        assert "no spin: the only CPU this process may run on" in log, log
        assert io_loops(node.process.pid) == ["ob-io-0"]
        assert "PONG" in send_command(node.port, "PING"), node.tail()
    finally:
        node.close()
