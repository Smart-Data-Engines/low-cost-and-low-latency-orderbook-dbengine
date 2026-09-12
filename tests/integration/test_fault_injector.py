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
                "OB_FAULT_COUNT", "OB_FAULT_SHORT", "OB_FAULT_LOG"):
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
