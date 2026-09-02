"""What a published number needs beside it to be checkable.

Every field here is read from the machine rather than from configuration, because the whole point of
requirement 2.1 is that a reader can compare their hardware to ours. A description we typed is a
description we can get wrong, and the one that matters most - the build type - is the one a directory
name lies about most convincingly.
"""
from __future__ import annotations

import hashlib
import os
import re
import subprocess
from dataclasses import dataclass, asdict
from pathlib import Path


class NotReleaseBuild(RuntimeError):
    """The engine was not built with -DCMAKE_BUILD_TYPE=Release, so the numbers mean nothing.

    A Debug build of this engine reaches roughly a third of Release throughput, and that gap is not
    a regression - it is a different measurement wearing the same name.

    The wording here avoids the comparative verbs on purpose. The static guard in
    `tests/test_resolution_rules.py` allows exactly one file to produce such a verdict, and it fired
    on this message: a true sentence about build types, which is not a claim about two systems. An
    exception for this file was the wrong repair - a guard with exceptions is the substring trap
    again - so the sentence moved instead. That keeps the rule absolute, which is the only reason it
    is worth having.
    """


@dataclass(frozen=True)
class Hardware:
    cpu_model: str
    cores: int
    mhz: float
    ram_mib: int
    disk_model: str
    disk_stack: str
    disk_rotational: bool
    filesystem: str
    kernel: str
    compiler: str

    def digest(self) -> str:
        """Eight hex characters over the canonical description.

        It goes in the results filename so that two runs on different machines cannot land in one
        file. Two runs on the *same* machine should collide - that is the point.
        """
        canonical = "|".join(f"{k}={v}" for k, v in sorted(asdict(self).items()))
        return hashlib.sha256(canonical.encode()).hexdigest()[:8]


def _first(pattern: str, text: str, default: str = "unknown") -> str:
    match = re.search(pattern, text, re.MULTILINE)
    return match.group(1).strip() if match else default


def _run(cmd: list[str]) -> str:
    """Best effort: a missing tool is a missing field, not a failed run."""
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return ""


def describe(data_dir: Path) -> Hardware:
    cpuinfo = Path("/proc/cpuinfo").read_text(errors="replace")
    meminfo = Path("/proc/meminfo").read_text(errors="replace")

    # `lsblk` on the device holding the data directory, not on the first disk in the system: a run
    # against an SSD-backed path on a machine with a spinning root disk would otherwise be described
    # by the disk it never touched.
    source = _run(["findmnt", "-no", "SOURCE", "--target", str(data_dir)]).strip()
    fstype = _run(["findmnt", "-no", "FSTYPE", "--target", str(data_dir)]).strip() or "unknown"
    # `--inverse` walks down to the physical device. `-d` on the mount source returns the mapper or
    # partition itself, whose MODEL is empty - which is how this reported "unknown" on a perfectly
    # ordinary laptop: the root filesystem is LVM over LUKS over an NVMe partition, and only the
    # bottom of that stack knows it is an ADATA SX7000NP. Encryption also costs I/O, so the layers
    # are worth reporting rather than flattening.
    stack = _run(["lsblk", "-no", "NAME,MODEL,ROTA", "--inverse", source]) if source else ""
    disk: list[str] = []
    for line in stack.splitlines():
        fields = line.split()
        if len(fields) >= 3:          # a name, a model and a rotational flag
            disk = fields[1:]
    layers = [line.split()[0].strip("`|-└─ ") for line in stack.splitlines() if line.split()]

    return Hardware(
        cpu_model=_first(r"^model name\s*:\s*(.+)$", cpuinfo),
        cores=cpuinfo.count("processor\t:"),
        mhz=float(_first(r"^cpu MHz\s*:\s*([0-9.]+)$", cpuinfo, "0")),
        ram_mib=int(_first(r"^MemTotal:\s+(\d+) kB$", meminfo, "0")) // 1024,
        disk_model=" ".join(disk[:-1]) if len(disk) > 1 else "unknown",
        disk_stack=" → ".join(layers) if layers else "unknown",
        disk_rotational=(disk[-1] == "1") if disk else False,
        filesystem=fstype,
        kernel=os.uname().release,
        compiler=_run(["g++", "--version"]).splitlines()[0] if _run(["g++", "--version"]) else "unknown",
    )


def build_type(build_dir: Path) -> str:
    """The build type as CMake recorded it, from the cache of the tree that produced the binary.

    Read from `CMakeCache.txt` and never from the directory name. `build-release/` configured in
    Debug reads correctly from its name and wrongly from every number it produces - and that is the
    failure mode this function exists for, because nobody checks a name that already looks right.
    """
    cache = build_dir / "CMakeCache.txt"
    if not cache.is_file():
        raise NotReleaseBuild(f"no CMakeCache.txt in {build_dir}, so the build type is unknown")
    value = _first(r"^CMAKE_BUILD_TYPE:\w+=(.*)$", cache.read_text(errors="replace"), "")
    return value or "(empty)"


def require_release(build_dir: Path) -> str:
    actual = build_type(build_dir)
    if actual != "Release":
        raise NotReleaseBuild(
            f"{build_dir} was configured as {actual!r}, not Release. A Debug build of this engine "
            f"reaches roughly a third of Release throughput, and that gap would be published as a "
            f"result")
    return actual
