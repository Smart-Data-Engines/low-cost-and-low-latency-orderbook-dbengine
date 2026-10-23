"""A restore to a moment after the backup (#34, point-in-time recovery; spec `point-in-time-recovery`).

A node with `--wal-archive-dir` copies every WAL file it closes into the archive, and rotates a file
that has held a record for `--wal-archive-interval-s`. `ob_restore --wal-archive A --until T` restores
a backup and replays the archived records from its cut to the last checkpoint at or before T. What
only a running node shows: that the archive it leaves takes a restore to each of several moments to
exactly the rows written before that moment, `--until end` to every row, and that a restore refuses -
before it creates anything - an archive of another lineage, a hole, a file that is not what its sum
says and a moment before the backup.

One node writes the phases, and the tests restore from what it left, each into its own directories.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from conftest import patience
from test_backup import RESTORE, Node, Wire, backup_and_wait

pytestmark = pytest.mark.smoke

SYMBOL = "PITR"
EXCH = "EX"
PER_PHASE = 40


def price_of(phase: int, i: int) -> int:
    return phase * 1_000_000 + i


def write_phase(port: int, phase: int) -> None:
    wire = Wire(port)
    try:
        for i in range(PER_PHASE):
            answer = wire.ask(f"MINSERT {SYMBOL} {EXCH} bid 1\n{price_of(phase, i)} 1 1")
            assert answer.startswith("OK"), answer
        flushed = wire.ask("FLUSH")
        assert flushed.startswith("OK"), flushed
    finally:
        wire.close()


def wal_indexes(directory: Path) -> list[int]:
    return sorted(int(p.name[4:10]) for p in directory.glob("wal_[0-9][0-9][0-9][0-9][0-9][0-9].bin"))


@dataclass
class Archived:
    tmp: Path
    backup: Path
    archive: Path
    identity: str
    cut_ns: int
    cut_file: int
    moments: list[int] = field(default_factory=list)   # after phase 1, 2 and 3


@pytest.fixture(scope="module")
def archived(tmp_path_factory) -> Archived:
    tmp = tmp_path_factory.mktemp("ob_pitr_")
    data = tmp / "data"
    node = Node(str(data), ["--backup-dir", str(tmp / "backups"), "--wal-archive-dir", str(tmp / "archive"),
                            "--wal-archive-interval-s", "1", "--flush-interval-ms", "50"])
    node.start()
    moments: list[int] = []
    try:
        write_phase(node.port, 0)
        name, st = backup_and_wait(node.port)
        assert st["state"] == "done", st
        for phase in (1, 2, 3):
            # A second apart, so that each phase's records reach the archive in a file of their own
            # and no checkpoint of one phase shares a moment with the next.
            time.sleep(1.1)
            write_phase(node.port, phase)
            moments.append(time.time_ns())
        # The last phase's file is rotated a second after its records and archived after that: wait
        # until the archive holds it, its sum written last. The last file with anything in it - one a
        # rotation has just opened stays empty, and is never rotated.
        identity = (data / "wal_identity").read_text().strip()
        lineage = tmp / "archive" / identity
        last = max(i for i in wal_indexes(data) if (data / f"wal_{i:06d}.bin").stat().st_size > 0)
        deadline = time.time() + patience(30)
        while not (lineage / f"wal_{last:06d}.bin.crc32c").exists():
            assert time.time() < deadline, f"the archive never took wal_{last:06d}.bin:\n{node.log_text()}"
            time.sleep(0.1)
    finally:
        node.stop()
    description = json.loads((tmp / "backups" / name / "backup.json").read_text())
    assert str(description["wal"]["identity"]) == identity, description
    return Archived(tmp=tmp, backup=tmp / "backups" / name, archive=tmp / "archive", identity=identity,
                    cut_ns=int(description["cut_at_ns"]), cut_file=int(description["wal"]["file_index"]),
                    moments=moments)


def restore_to(a: Archived, target: Path, until: str, archive: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run([RESTORE, "--backup", str(a.backup), "--data-dir", str(target),
                           "--wal-archive", str(archive or a.archive), "--until", until],
                          capture_output=True, text=True, timeout=patience(300))


def phases_held(data_dir: Path) -> dict[int, int]:
    node = Node(str(data_dir))
    node.start()
    try:
        prices = node.prices(SYMBOL)
    finally:
        node.stop()
    held: dict[int, int] = {}
    for price in prices or []:
        held[price // 1_000_000] = held.get(price // 1_000_000, 0) + 1
    return held


def replayed(stdout: str) -> int:
    found = re.search(r"replayed (\d+) record\(s\)", stdout)
    assert found, stdout
    return int(found.group(1))


def test_each_moment_restores_exactly_the_rows_written_before_it(archived):
    for phase, moment in enumerate(archived.moments, start=1):
        target = archived.tmp / f"restored_{phase}"
        done = restore_to(archived, target, str(moment))
        assert done.returncode == 0, done.stderr
        assert "to the checkpoint at" in done.stdout, done.stdout
        # The phases after the backup's cut, and no record of the next.
        assert replayed(done.stdout) == PER_PHASE * phase, done.stdout
        held = phases_held(target)
        assert held == {p: PER_PHASE for p in range(phase + 1)}, (phase, held, done.stdout)


def test_until_end_restores_every_row_the_archive_holds(archived):
    target = archived.tmp / "restored_end"
    done = restore_to(archived, target, "end")
    assert done.returncode == 0, done.stderr
    assert "every record the archive holds" in done.stdout, done.stdout
    assert replayed(done.stdout) == PER_PHASE * 3, done.stdout
    assert phases_held(target) == {p: PER_PHASE for p in range(4)}


def test_a_moment_as_backups_are_named_is_read_to_the_millisecond(archived):
    # The second phase's moment in the form a backup's name gives a time, to the millisecond - rounded
    # up, so that it is not before the checkpoint of the phase's FLUSH, which answered before the
    # moment was read; the next phase begins a second later.
    ms = archived.moments[1] // 1_000_000 + 1
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime(ms // 1000)) + ".%03dZ" % (ms % 1000)
    target = archived.tmp / "restored_named"
    done = restore_to(archived, target, stamp)
    assert done.returncode == 0, done.stderr
    assert phases_held(target) == {p: PER_PHASE for p in range(3)}


def copy_of_archive(a: Archived, name: str) -> Path:
    copy = a.tmp / name
    shutil.copytree(a.archive, copy)
    return copy


@pytest.mark.parametrize("case", ["another lineage", "a hole", "a file unlike its sum", "before the cut"])
def test_a_restore_refuses_before_it_creates_anything(archived, case):
    lineage_files = wal_indexes(archived.archive / archived.identity)
    until = str(archived.moments[2])
    archive = None
    if case == "another lineage":
        archive = archived.tmp / "archive_of_another"
        archive.mkdir(exist_ok=True)
        (archive / "12345").mkdir(exist_ok=True)
        expected = f"has no {archived.identity}"
    elif case == "a hole":
        # A file after the cut's and before the last, which the moment after phase 3 needs.
        archive = copy_of_archive(archived, "archive_with_a_hole")
        after_cut = [i for i in lineage_files if i > archived.cut_file]
        assert len(after_cut) >= 2, lineage_files
        middle = after_cut[0]
        (archive / archived.identity / f"wal_{middle:06d}.bin").unlink()
        expected = f"has no wal_{middle:06d}.bin"
    elif case == "a file unlike its sum":
        # The file the cut is in, which every restore of this backup reads.
        archive = copy_of_archive(archived, "archive_with_a_changed_file")
        changed = archive / archived.identity / f"wal_{archived.cut_file:06d}.bin"
        raw = bytearray(changed.read_bytes())
        raw[len(raw) // 2] ^= 0x01
        changed.write_bytes(bytes(raw))
        expected = "and its sum says"
    else:
        until = str(archived.cut_ns - 1)
        expected = "is before the backup's cut"
    target = archived.tmp / f"refused_{case.replace(' ', '_')}"
    done = restore_to(archived, target, until, archive)
    assert done.returncode == 1, (done.returncode, done.stdout, done.stderr)
    assert expected in done.stderr, done.stderr
    assert not target.exists() or not any(target.iterdir()), f"{case}: the refusal created {list(target.iterdir())}"


def test_until_goes_with_the_archive_and_takes_end_a_number_or_a_time(archived):
    lone = subprocess.run([RESTORE, "--backup", str(archived.backup), "--data-dir", str(archived.tmp / "x"),
                           "--until", "end"], capture_output=True, text=True, timeout=patience(60))
    assert lone.returncode == 2 and "go together" in lone.stderr, lone.stderr
    bad = restore_to(archived, archived.tmp / "y", "yesterday")
    assert bad.returncode == 2 and "is not 'end'" in bad.stderr, bad.stderr
    assert not (archived.tmp / "x").exists() and not (archived.tmp / "y").exists()
