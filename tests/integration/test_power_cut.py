"""What an acknowledged write survives when the power goes (#160).

A power cut with the kernel doing the losing: the node's data directory is on dm-flakey over a loop
device, and the device is switched to drop every write at the chosen moment - the way xfstests
simulate one. After an unmount (while writes are still being dropped) and a remount, the filesystem
shows what had reached the device at the switch, and nothing more. A killed process cannot show
this, because the page cache survives a kill - and every other crash test in this battery kills.

**Before #160 this lost every row a flush had claimed:** segment files were never synced, the
checkpoint claiming them was, and the cut brought back empty segment files under a checkpoint that
said the rows were in them. `scripts/power_cut.sh` measured 1 row of 201 with one flush before the
cut; this test, with a second flush after the last write, measured **0 of 201 under `every` and
under `interval`** against the build before the fix - no segment survived and the last checkpoint
covered every record. The control is the same run without the cut - the same kill, unmount and
remount - and it must bring back everything, or the procedure rather than the cut lost the rows.

**Opt-in here, mandatory in CI.** It needs root (losetup, dmsetup, mount) and the dm-flakey module,
so it runs only with `OB_POWER_CUT_TESTS=1` - and with that set, a missing prerequisite **fails**
rather than skips, because a power-cut test that skips looks exactly like code that survives the
cut. Both integration jobs in CI set it, and the job's skip gate accepts no skip but Binance's, so
losing the variable is a red job rather than a quiet one.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
from conftest import free_port, patience, server_binary_path

pytestmark = pytest.mark.skipif(
    os.environ.get("OB_POWER_CUT_TESTS") != "1",
    reason="the power-cut tests need root and dm-flakey: set OB_POWER_CUT_TESTS=1 to run them")

SERVER = server_binary_path()
ROWS = 200


def sudo(*argv: str, check: bool = True, input: str | None = None) -> subprocess.CompletedProcess:
    done = subprocess.run(["sudo", "-n", *argv], input=input, capture_output=True, text=True,
                          timeout=60)
    if check and done.returncode != 0:
        pytest.fail(f"`sudo {' '.join(argv)}` failed ({done.returncode}): {done.stderr.strip()}")
    return done


def require_prerequisites() -> None:
    """Fail, not skip, when the run was asked for and cannot happen - see the module docstring."""
    if subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode != 0:
        pytest.fail("OB_POWER_CUT_TESTS=1 but `sudo -n` asks for a password; this test needs root")
    for tool in ("losetup", "dmsetup", "mkfs.ext4", "blockdev"):
        if shutil.which(tool) is None and not Path(f"/usr/sbin/{tool}").exists():
            pytest.fail(f"OB_POWER_CUT_TESTS=1 but `{tool}` is not installed")
    probe = sudo("modprobe", "dm-flakey", check=False)
    if probe.returncode != 0:
        pytest.fail("OB_POWER_CUT_TESTS=1 but the dm-flakey module does not load: "
                    f"{probe.stderr.strip()} (on a cloud kernel it is in linux-modules-extra)")


class FlakeyDisk:
    """A filesystem on dm-flakey over a loop device, with a switch that drops every write."""

    def __init__(self, work: Path):
        self.work = work
        self.image = work / "flakey.img"
        self.mount = work / "mnt"
        self.name = f"ob-power-cut-{os.getpid()}-{free_port()}"
        self.loop = ""
        self.mount.mkdir()
        with open(self.image, "wb") as f:
            f.truncate(1 << 30)
        self.loop = sudo("losetup", "--find", "--show", str(self.image)).stdout.strip()
        self.sectors = sudo("blockdev", "--getsz", self.loop).stdout.strip()
        sudo("dmsetup", "create", self.name, input=self._table(drop=False) + "\n")
        sudo("mkfs.ext4", "-q", f"/dev/mapper/{self.name}")
        self._mount()

    def _table(self, drop: bool) -> str:
        # Up for 180 s and never down, or down for 180 s dropping every write: the two tables
        # xfstests load for FLAKEY_ALLOW_WRITES and FLAKEY_DROP_WRITES.
        if drop:
            return f"0 {self.sectors} flakey {self.loop} 0 0 180 1 drop_writes"
        return f"0 {self.sectors} flakey {self.loop} 0 180 0"

    def _load(self, drop: bool) -> None:
        # --nolockfs, or the suspend freezes the filesystem - which writes back every dirty page
        # first and turns the power cut into a clean shutdown.
        sudo("dmsetup", "suspend", "--nolockfs", self.name)
        sudo("dmsetup", "load", self.name, input=self._table(drop) + "\n")
        sudo("dmsetup", "resume", self.name)

    def _mount(self) -> None:
        sudo("mount", f"/dev/mapper/{self.name}", str(self.mount))
        sudo("chown", f"{os.getuid()}:{os.getgid()}", str(self.mount))

    def cut_power(self) -> None:
        self._load(drop=True)

    def remount_after(self, kill) -> None:
        """Kill whatever is using the disk, unmount while writes are still dropped, remount."""
        kill()
        sudo("umount", str(self.mount))
        self._load(drop=False)
        self._mount()

    def remove(self) -> None:
        if subprocess.run(["mountpoint", "-q", str(self.mount)]).returncode == 0:
            sudo("umount", str(self.mount), check=False)
        sudo("dmsetup", "remove", self.name, check=False)
        if self.loop:
            sudo("losetup", "-d", self.loop, check=False)
        self.image.unlink(missing_ok=True)


class Node:
    def __init__(self, data_dir: Path, log: Path, policy: str, flush_ms: int):
        self.data_dir, self.log_path, self.policy, self.flush_ms = data_dir, log, policy, flush_ms
        self.proc: subprocess.Popen | None = None
        self.port = self.metrics_port = 0

    def start(self) -> None:
        self.port, self.metrics_port = free_port(), free_port()
        with open(self.log_path, "ab") as log:
            self.proc = subprocess.Popen(
                [SERVER, "--port", str(self.port), "--metrics-port", str(self.metrics_port),
                 "--data-dir", str(self.data_dir), "--fsync-policy", self.policy,
                 "--flush-interval-ms", str(self.flush_ms), "--drain-timeout-ms", "2000"],
                stdout=log, stderr=subprocess.STDOUT)
        deadline = time.time() + patience(30)
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=2) as s:
                    s.recv(4096)
                    s.sendall(b"PING\n")
                    if b"PONG" in s.recv(4096):
                        return
            except OSError:
                time.sleep(0.1)
        pytest.fail(f"the node never answered; log tail:\n{self.log()[-1500:]}")

    def kill(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)

    def log(self) -> str:
        return self.log_path.read_text(errors="replace") if self.log_path.exists() else ""

    def insert(self, prices) -> int:
        acknowledged = 0
        with socket.create_connection(("127.0.0.1", self.port), timeout=patience(15)) as s:
            r = s.makefile("rb")
            while r.readline().strip():
                pass
            for p in prices:
                s.sendall(f"INSERT SYM EX bid {p} 1 1\n".encode())
                line = r.readline().strip()
                if line == b"OK":
                    r.readline()
                    acknowledged += 1
        return acknowledged

    def prices(self) -> list[int]:
        with socket.create_connection(("127.0.0.1", self.port), timeout=patience(15)) as s:
            r = s.makefile("rb")
            while r.readline().strip():
                pass
            s.sendall(b"SELECT * FROM 'SYM'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")
            out = []
            while True:
                line = r.readline()
                # `OK` ends in a blank line and `ERR` in one newline. A symbol with no rows left at
                # all is answered `ERR ... not found`, and waiting for a blank line after it hung
                # this reader for its whole timeout - on the build that lost everything.
                if not line or line == b"\n" or line.startswith(b"ERR"):
                    break
                parts = line.decode(errors="replace").split("\t")
                if len(parts) >= 7 and parts[0].isdigit():
                    out.append(int(parts[1]))
        return out

    def metric(self, name: str) -> int:
        with socket.create_connection(("127.0.0.1", self.metrics_port), timeout=patience(10)) as s:
            s.sendall(b"GET /metrics HTTP/1.0\r\n\r\n")
            body = b""
            while chunk := s.recv(65536):
                body += chunk
        for line in body.decode(errors="replace").splitlines():
            head = line.split()[0] if line.split() else ""
            if head == name or head.startswith(name + "{"):
                return int(float(line.split()[1]))
        return 0

    def wait_for(self, name: str, at_least: int) -> None:
        deadline = time.time() + patience(30)
        while self.metric(name) < at_least and time.time() < deadline:
            time.sleep(0.05)
        assert self.metric(name) >= at_least, f"{name} never reached {at_least}"


@pytest.fixture
def disk(tmp_path):
    require_prerequisites()
    d = FlakeyDisk(tmp_path)
    yield d
    d.remove()


@pytest.mark.parametrize("policy", ["every", "interval"])
@pytest.mark.parametrize("cut", [True, False], ids=["cut", "control"])
def test_an_acknowledged_write_survives_a_power_cut_after_a_flush(disk, tmp_path, policy, cut):
    """200 rows, a tick that puts them in a segment and appends its checkpoint, one more row, the
    next tick - which syncs the WAL, the checkpoint in it and the last row under either policy - and
    then the cut. Every one of the 201 has been acknowledged and synced, so every one comes back.
    """
    node = Node(disk.mount / "data", tmp_path / "node.log", policy, flush_ms=1000)
    try:
        node.start()
        assert node.insert(range(1000, 1000 + ROWS)) == ROWS
        node.wait_for("ob_segment_count", 1)
        assert node.insert([5000]) == 1
        # A full tick after the last write: its WAL sync takes the checkpoint and row 5000 to the
        # device under `interval` as well, so the cut cannot be excused as an unsynced write.
        ticks = node.metric("ob_flush_ticks_total")
        node.wait_for("ob_flush_ticks_total", ticks + 2)

        if cut:
            disk.cut_power()
        disk.remount_after(node.kill)
        node.start()
        back = sorted(node.prices())
        expected = list(range(1000, 1000 + ROWS)) + [5000]
        assert back == expected, (
            f"{len(expected) - len(set(back) & set(expected))} of {len(expected)} acknowledged rows "
            f"did not survive {'the power cut' if cut else 'the control run'} under {policy}; "
            f"{len(back)} came back.\n{node.log()[-2000:]}")
        # And the node is still the one that wrote them: a cut once brought wal_identity back
        # empty, and a new identity makes every position the segments recorded foreign.
        assert "wal_identity file present but unusable" not in node.log(), (
            "the WAL identity did not survive the cut")
    finally:
        node.kill()
