"""The fault injector has its own tests, because an injector that does nothing looks like success.

`libobfault.so` exists to make a chosen `write`, `fsync`, `fdatasync` or `ftruncate` fail on a
chosen file after a chosen number of successful calls (#54). Every test that uses it concludes
something from the engine's behaviour **under** an injected fault — so if the injection silently
does not happen, those tests pass and report that the engine survives a failure it never saw. That
is the same shape as a required check whose scope quietly shrank, and it is why requirement 2.5
asks for a control run with no fault alongside every run with one.

So these tests are about the instrument rather than the engine. The workload is a Python
subprocess: `os.write` and `os.fsync` go through the same libc symbols the engine's WAL does, and
using the interpreter avoids adding a second C artefact to the repository whose own correctness
would then need establishing.

Two of the six are negative on purpose. A check that passes by finding nothing needs a pair - this
hits, that does not - or it can stop looking and stay green.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from conftest import fault_injector_path

pytestmark = pytest.mark.smoke

RECORD = 20
WRITES = 5

# Writes WRITES records of RECORD bytes, fsyncing after each, and reports what every call returned.
# `os.write` and `os.fsync` rather than a file object: buffering would decide when the syscall
# happens, and the injector counts syscalls.
PROBE = r"""
import errno, os, sys
path = sys.argv[1]
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_APPEND, 0o644)
for i in range(%d):
    try:
        n = os.write(fd, bytes([65 + i]) * %d)
        print("w %%d ok %%d" %% (i, n))
    except OSError as exc:
        print("w %%d err %%s" %% (i, errno.errorcode.get(exc.errno, exc.errno)))
    try:
        os.fsync(fd)
        print("s %%d ok 0" %% i)
    except OSError as exc:
        print("s %%d err %%s" %% (i, errno.errorcode.get(exc.errno, exc.errno)))
os.close(fd)
print("size %%d" %% os.path.getsize(path))
""" % (WRITES, RECORD)


def run_probe(tmp_path, **fault_env) -> dict:
    """Run the probe with the injector preloaded, returning what each call did."""
    injector = fault_injector_path()
    assert injector is not None, (
        "libobfault.so was not built, so nothing can be injected. Failing rather than skipping: a "
        "fault-injection test that does not inject is indistinguishable from an engine that "
        "survives the fault (requirement 4.2)"
    )
    target = tmp_path / "probe_target.bin"
    env = dict(os.environ)
    env["LD_PRELOAD"] = injector
    for key in ("OB_FAULT_PATH", "OB_FAULT_OP", "OB_FAULT_ERRNO", "OB_FAULT_SKIP",
                "OB_FAULT_COUNT", "OB_FAULT_SHORT", "OB_FAULT_LOG", "OB_FAULT_DELAY_MS"):
        env.pop(key, None)
    env.update({k: str(v) for k, v in fault_env.items() if v is not None})

    done = subprocess.run([sys.executable, "-c", PROBE, str(target)],
                          env=env, capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, f"probe failed: {done.stderr}"

    writes, syncs, size = {}, {}, None
    for line in done.stdout.splitlines():
        parts = line.split()
        if parts[0] == "w":
            writes[int(parts[1])] = (parts[2], parts[3])
        elif parts[0] == "s":
            syncs[int(parts[1])] = (parts[2], parts[3])
        elif parts[0] == "size":
            size = int(parts[1])
    return {"writes": writes, "syncs": syncs, "size": size, "target": target}


def log_actions(path) -> list[str]:
    """The action of every decision the injector recorded, in order."""
    if not os.path.exists(path):
        return []
    actions = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            for field in line.split():
                if field.startswith("action="):
                    actions.append(field[len("action="):])
    return actions


def test_the_injector_was_built(tmp_path):
    """Its absence is a failure, not a skip — everything else here would pass without it."""
    assert fault_injector_path() is not None


def test_a_disarmed_injector_changes_nothing(tmp_path):
    """Preloaded with no configuration at all: the control every other test is measured against."""
    result = run_probe(tmp_path)
    assert all(v == ("ok", str(RECORD)) for v in result["writes"].values()), result["writes"]
    assert all(v == ("ok", "0") for v in result["syncs"].values()), result["syncs"]
    assert result["size"] == WRITES * RECORD


def test_a_path_that_matches_nothing_changes_nothing(tmp_path):
    """The other half of the pair: armed, but pointed at a file this probe never opens.

    Without this, the test above could pass because the injector is broken rather than because it
    is disarmed.
    """
    log = tmp_path / "fault.log"
    result = run_probe(tmp_path, OB_FAULT_PATH="a_file_this_probe_never_opens",
                       OB_FAULT_OP="write", OB_FAULT_ERRNO="ENOSPC", OB_FAULT_LOG=log)
    assert result["size"] == WRITES * RECORD
    assert log_actions(log) == [], "the injector decided something about a file it should not match"


def test_only_the_chosen_write_fails(tmp_path):
    """ENOSPC on the third write, and on nothing else."""
    log = tmp_path / "fault.log"
    result = run_probe(tmp_path, OB_FAULT_PATH="probe_target", OB_FAULT_OP="write",
                       OB_FAULT_ERRNO="ENOSPC", OB_FAULT_SKIP=2, OB_FAULT_COUNT=1,
                       OB_FAULT_LOG=log)
    assert result["writes"][2] == ("err", "ENOSPC"), result["writes"]
    for i in (0, 1, 3, 4):
        assert result["writes"][i] == ("ok", str(RECORD)), (i, result["writes"])
    assert result["size"] == (WRITES - 1) * RECORD, "exactly one record should be missing"

    actions = log_actions(log)
    assert actions == ["pass-skip", "pass-skip", "fail", "pass-spent", "pass-spent"], actions


def test_a_short_write_leaves_the_bytes_it_claims(tmp_path):
    """The torn-record shape: the bytes it reports really do reach the file.

    A short write that reported a count without writing anything would model nothing — the caller's
    loop would resend from the wrong offset and the file would be consistent by accident. The point
    of this mode is a file that ends in a partial record.
    """
    log = tmp_path / "fault.log"
    short = 7
    result = run_probe(tmp_path, OB_FAULT_PATH="probe_target", OB_FAULT_OP="write",
                       OB_FAULT_SKIP=1, OB_FAULT_COUNT=1, OB_FAULT_SHORT=short, OB_FAULT_LOG=log)
    assert result["writes"][1] == ("ok", str(short)), result["writes"]
    assert result["size"] == (WRITES - 1) * RECORD + short
    assert "short" in log_actions(log)


def test_fsync_fails_while_every_write_succeeds(tmp_path):
    """The shape that loses data without any write ever failing.

    On Linux a failed `fsync` marks the pages clean, so a retry reports success over data that is
    gone. Here the writes all succeed and the file is the full size — from inside the process
    nothing is missing, which is exactly why the return value of `fsync` has to be read.
    """
    log = tmp_path / "fault.log"
    result = run_probe(tmp_path, OB_FAULT_PATH="probe_target", OB_FAULT_OP="fsync",
                       OB_FAULT_ERRNO="EIO", OB_FAULT_LOG=log)
    assert all(v == ("ok", str(RECORD)) for v in result["writes"].values()), result["writes"]
    assert all(v == ("err", "EIO") for v in result["syncs"].values()), result["syncs"]
    assert result["size"] == WRITES * RECORD
    assert log_actions(log) == ["fail"] * WRITES


TIMED_PROBE = r"""
import os, sys, time
path = sys.argv[1]
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_APPEND, 0o644)
for i in range(3):
    os.write(fd, b"x" * 64)
    started = time.monotonic()
    os.fsync(fd)
    print("s %d %.3f" % (i, time.monotonic() - started))
os.close(fd)
"""


def test_a_delayed_fsync_is_slow_and_succeeds(tmp_path):
    """`OB_FAULT_DELAY_MS`, the slow sync stage 5 of #151 needs: the chosen call sleeps, then syncs.

    Only the chosen one. The control is the other two calls in the same run, which must not wait -
    a delay that leaked to every sync would make every test built on it measure the injector.
    """
    injector = fault_injector_path()
    assert injector is not None, "libobfault.so was not built"
    log = tmp_path / "fault.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith("OB_FAULT_")}
    env.update(LD_PRELOAD=injector, OB_FAULT_PATH="timed_target", OB_FAULT_OP="fsync",
               OB_FAULT_DELAY_MS="400", OB_FAULT_SKIP="1", OB_FAULT_COUNT="1",
               OB_FAULT_LOG=str(log))
    done = subprocess.run([sys.executable, "-c", TIMED_PROBE, str(tmp_path / "timed_target.bin")],
                          env=env, capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, f"a delayed fsync failed instead of succeeding: {done.stderr}"
    took = {int(p[1]): float(p[2]) for p in (line.split() for line in done.stdout.splitlines())}
    assert took[1] >= 0.4, f"the chosen fsync took {took[1]:.3f}s, not the 0.4 s it was given"
    assert took[0] < 0.2 and took[2] < 0.2, f"a sync that was not chosen waited: {took}"
    assert log_actions(log) == ["pass-skip", "delay", "pass-spent"], log_actions(log)


TIMED_WRITE_PROBE = r"""
import os, sys, time
path = sys.argv[1]
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_APPEND, 0o644)
for i in range(3):
    started = time.monotonic()
    written = os.write(fd, b"y" * 64)
    print("w %d %d %.3f" % (i, written, time.monotonic() - started))
os.close(fd)
print("size %d" % os.path.getsize(path))
"""


def test_a_delayed_write_is_slow_and_writes_its_bytes(tmp_path):
    """`OB_FAULT_DELAY_MS` on a write, the fault #159 needed: a segment write that takes its time.

    Slow and **real**: the chosen write returns its full count and its bytes are in the file, since a
    delay that dropped the bytes would be a different fault wearing this one's name. And only the
    chosen one - the other two writes in the same run are the control.
    """
    injector = fault_injector_path()
    assert injector is not None, "libobfault.so was not built"
    log = tmp_path / "fault.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith("OB_FAULT_")}
    env.update(LD_PRELOAD=injector, OB_FAULT_PATH="timed_write", OB_FAULT_OP="write",
               OB_FAULT_DELAY_MS="400", OB_FAULT_SKIP="1", OB_FAULT_COUNT="1",
               OB_FAULT_LOG=str(log))
    done = subprocess.run([sys.executable, "-c", TIMED_WRITE_PROBE, str(tmp_path / "timed_write.bin")],
                          env=env, capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, f"a delayed write failed instead of succeeding: {done.stderr}"
    rows = [line.split() for line in done.stdout.splitlines()]
    took = {int(r[1]): float(r[3]) for r in rows if r[0] == "w"}
    written = {int(r[1]): int(r[2]) for r in rows if r[0] == "w"}
    size = next(int(r[1]) for r in rows if r[0] == "size")
    assert took[1] >= 0.4, f"the chosen write took {took[1]:.3f}s, not the 0.4 s it was given"
    assert took[0] < 0.2 and took[2] < 0.2, f"a write that was not chosen waited: {took}"
    assert written == {0: 64, 1: 64, 2: 64} and size == 192, (
        f"the delayed write did not write its bytes: {written}, file size {size}")
    assert log_actions(log) == ["pass-skip", "delay", "pass-spent"], log_actions(log)
