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


class VolatileStorage(RuntimeError):
    """The engine's data directory is in RAM, so its write path is not comparable to the others.

    The harness used to put the engine's data directory in `tempfile.mkdtemp()`, which is `/tmp`.
    On the machine this benchmark was written on, `/tmp` is part of an ext4 root filesystem, so the
    engine wrote to the same disk as ClickHouse and PostgreSQL and the numbers were comparable - by
    luck, not by design. On the first machine where `/tmp` is a tmpfs, the engine's WAL and columnar
    segments would go to memory while both competitors wrote to NVMe, and the report would print the
    NVMe next to all three numbers.

    The two servers keep their data in their own directories, which this harness does not choose,
    so it cannot move everybody into RAM to make the comparison fair that way. It refuses instead.
    """


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
    platform: str
    cpu_model: str
    cores: int
    mhz: float
    clock_source: str
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


def _read(path: str) -> str:
    try:
        return Path(path).read_text(errors="replace").strip()
    except OSError:
        return ""


def _platform() -> str:
    """What the firmware says this machine is, from DMI - no network call, no credentials.

    On a cloud instance this is the single most useful line in the whole record: "Amazon EC2
    m9g.xlarge" tells a reader more about reproducing a number than any CPU string does, because it
    is the thing they can rent. The instance metadata service would also answer it, and is not asked:
    it is a network endpoint that serves credentials, which a benchmark harness has no business
    talking to.
    """
    vendor = _read("/sys/class/dmi/id/sys_vendor")
    product = _read("/sys/class/dmi/id/product_name")
    joined = " ".join(part for part in (vendor, product) if part)
    return joined or "unknown (no DMI product identifiers)"


def _cpu_model(cpuinfo: str) -> str:
    """The CPU, named from whatever this architecture actually publishes.

    x86 puts a marketing string in `model name` and this read it directly, which is why the field
    said "unknown" on the first aarch64 run: there is no `model name` line there, and `lscpu` on
    this distribution did not decode the part number either. What aarch64 does publish is the
    implementer, part, variant and revision from MIDR_EL1, and those are reported verbatim.

    Deliberately without a lookup table from part number to marketing name. Such a table answers
    "unknown" for every CPU released after it was written, which is the failure this function just
    had; the hex identifiers never go stale, a reader with the same part number knows it is the same
    silicon, and `platform` above carries the name a human recognises.
    """
    model = _first(r"^model name\s*:\s*(.+)$", cpuinfo, "")
    if model:
        return model
    implementer = _first(r"^CPU implementer\s*:\s*(\S+)$", cpuinfo, "")
    part = _first(r"^CPU part\s*:\s*(\S+)$", cpuinfo, "")
    if implementer and part:
        variant = _first(r"^CPU variant\s*:\s*(\S+)$", cpuinfo, "0x0")
        revision = _first(r"^CPU revision\s*:\s*(\S+)$", cpuinfo, "0")
        return (f"{os.uname().machine} implementer {implementer} part {part} "
                f"r{int(variant, 16)}p{revision}")
    return "unknown"


def _clock(cpuinfo: str, cpufreq_max_khz: str) -> tuple[float, str]:
    """The clock, with where it came from - or an admission that nobody published one.

    This returned 0.0 on aarch64, because `cpu MHz` is an x86 line and there is no cpufreq driver on
    this instance to read a maximum from. Zero renders as "0.0 MHz", which is a measurement nobody
    took presented as one; the source travels with the number so the report can say "not published"
    instead.

    The cpufreq reading arrives as an argument rather than being read here, so that both of this
    function's inputs are visible to its caller and to a test. The first version read the sysfs file
    itself, and the test that hands it an aarch64 `/proc/cpuinfo` got 2400 MHz back - the clock of
    the x86 machine the test was running on, arriving from behind the fixture.

    `BogoMIPS` is right there in the same file and is not used. On aarch64 it is the architected
    timer's frequency, not the core clock - on this machine it reads 2000.00 while the part is
    nothing like a 2 GHz design. A field filled with a number that means something else is worse
    than a field left empty.
    """
    from_cpuinfo = _first(r"^cpu MHz\s*:\s*([0-9.]+)$", cpuinfo, "")
    if from_cpuinfo:
        return float(from_cpuinfo), "/proc/cpuinfo cpu MHz"
    if cpufreq_max_khz.isdigit():
        return (float(cpufreq_max_khz) / 1000.0,
                "cpufreq cpuinfo_max_freq (maximum, not the running clock)")
    return 0.0, "not published by this platform"


def _compiler(build_dir: Path) -> str:
    """The compiler that built the binary, asked of the binary's own build cache.

    This ran `g++ --version` and reported that. On the machine the benchmark was written on, `g++`
    *is* what built it, so the field was correct by coincidence - and on the first machine where it
    was not, there is no `g++` on PATH at all and the field read "unknown" while a perfectly
    identified GCC 14 sat in the cache. The worse shape is the one in between: a system `g++` that
    exists, answers, and is not the compiler whose output is being timed.
    """
    cache = build_dir / "CMakeCache.txt"
    if cache.is_file():
        compiler = _first(r"^CMAKE_CXX_COMPILER:\w+=(.*)$", cache.read_text(errors="replace"), "")
        if compiler:
            version = _run([compiler, "--version"]).splitlines()
            if version:
                return version[0].strip()
            return f"{compiler} (did not answer --version)"
    return "unknown"


def describe(data_dir: Path, build_dir: Path) -> Hardware:
    """Two directories, because the two questions have different answers.

    The disk is the one under the **engine's data directory** - the device whose write latency ends
    up in the ingest number. The compiler and build type come from the **build directory**, which is
    where CMake recorded them. This took one argument before, named `data_dir`, and the caller passed
    the build directory: on a one-disk machine that is the same device and the field was right, and
    on a machine whose `/tmp` is a tmpfs it described a disk the engine never touched.
    """
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

    mhz, clock_source = _clock(
        cpuinfo, _read("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq"))

    return Hardware(
        platform=_platform(),
        cpu_model=_cpu_model(cpuinfo),
        cores=cpuinfo.count("processor\t:"),
        mhz=mhz,
        clock_source=clock_source,
        ram_mib=int(_first(r"^MemTotal:\s+(\d+) kB$", meminfo, "0")) // 1024,
        disk_model=" ".join(disk[:-1]) if len(disk) > 1 else "unknown",
        disk_stack=" → ".join(layers) if layers else "unknown",
        disk_rotational=(disk[-1] == "1") if disk else False,
        filesystem=fstype,
        kernel=os.uname().release,
        compiler=_compiler(build_dir),
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


def require_durable_storage(data_dir: Path) -> str:
    """Refuse before the run if the engine would be writing to memory. Returns the filesystem.

    Checked here rather than reported in the results, because a run that has already happened is a
    number somebody will read. The competitors' data directories belong to their servers and are not
    ours to move, so there is no arrangement in which a tmpfs is fair - only one in which it is
    hidden.
    """
    fstype = _run(["findmnt", "-no", "FSTYPE", "--target", str(data_dir)]).strip()
    if fstype in ("tmpfs", "ramfs"):
        raise VolatileStorage(
            f"{data_dir} is on {fstype}, which is memory. The engine would write its WAL and "
            f"segments there while ClickHouse and PostgreSQL write to their own data directories on "
            f"disk, and every ingest number in the report would be that difference. Pass "
            f"--data-dir pointing somewhere on the same storage the servers use.")
    return fstype or "unknown"


def require_release(build_dir: Path) -> str:
    actual = build_type(build_dir)
    if actual != "Release":
        raise NotReleaseBuild(
            f"{build_dir} was configured as {actual!r}, not Release. A Debug build of this engine "
            f"reaches roughly a third of Release throughput, and that gap would be published as a "
            f"result")
    return actual
