"""What the hardware description says on a machine that is not the one it was written on.

Every field in `hardware.py` exists so that a reader can compare their machine to ours. Three of
them were read in a way that only x86 answers, and the module said so about none of it: on the first
aarch64 run the CPU was "unknown", the clock was 0.0 and the compiler was "unknown" - while the
machine was perfectly willing to identify all three.

The tests below feed both architectures' `/proc/cpuinfo` to the same functions, because a fallback
that shadows the good path is the other way to get this wrong.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from .. import hardware

# A real aarch64 /proc/cpuinfo block, from the Amazon Linux 2023 m9g.xlarge this was written on.
# There is no `model name` line and no `cpu MHz` line; that is the whole point of the fixture.
AARCH64 = """processor\t: 0
BogoMIPS\t: 2000.00
Features\t: fp asimd aes crc32 atomics sve sve2 i8mm bf16
CPU implementer\t: 0x41
CPU architecture: 8
CPU variant\t: 0x0
CPU part\t: 0xd84
CPU revision\t: 1

processor\t: 1
BogoMIPS\t: 2000.00
CPU implementer\t: 0x41
CPU part\t: 0xd84
CPU variant\t: 0x0
CPU revision\t: 1
"""

X86 = """processor\t: 0
vendor_id\t: GenuineIntel
model name\t: Intel(R) Core(TM) i3-7100U CPU @ 2.40GHz
cpu MHz\t\t: 2399.989
"""


def test_the_cpu_is_named_on_an_architecture_with_no_model_name_line():
    named = hardware._cpu_model(AARCH64)
    assert named != "unknown"
    # The identifiers a reader compares, and no invented marketing name: a part-number table would
    # answer "unknown" for every CPU released after it was written, which is the bug being fixed.
    assert "0x41" in named and "0xd84" in named and "r0p1" in named


def test_the_x86_model_name_still_wins_over_the_fallback():
    # A fallback that shadows the path that already worked is the other way to break this.
    assert hardware._cpu_model(X86) == "Intel(R) Core(TM) i3-7100U CPU @ 2.40GHz"


def test_bogomips_is_not_reported_as_a_clock():
    # BogoMIPS sits in the aarch64 fixture above and reads 2000.00, which is the architected timer's
    # frequency and nothing like the core clock. A field filled with a number that means something
    # else is worse than an empty one.
    # "" for the cpufreq reading, because that is what the measured machine gave: no cpufreq
    # driver at all on this instance family.
    mhz, source = hardware._clock(AARCH64, "")
    assert mhz == 0.0
    assert "not published" in source
    assert "2000" not in source


def test_a_cpufreq_maximum_is_used_when_cpuinfo_has_no_clock_and_is_labelled_a_maximum():
    # The control for the test above: without it, a function that always answers "not published"
    # passes. The label matters too - a maximum is not the clock the run happened at.
    mhz, source = hardware._clock(AARCH64, "2400000")
    assert mhz == pytest.approx(2400.0)
    assert "maximum" in source


def test_an_x86_clock_is_reported_with_where_it_came_from():
    mhz, source = hardware._clock(X86, "2400000")
    assert mhz == pytest.approx(2399.989)
    assert "cpuinfo" in source


def test_the_compiler_comes_from_the_build_cache_not_from_path(tmp_path: Path):
    # The interesting case is a build whose compiler is *not* the one PATH resolves. Any binary that
    # answers --version proves the mechanism: the field is read from the cache and that program is
    # the one asked.
    (tmp_path / "CMakeCache.txt").write_text(
        f"CMAKE_BUILD_TYPE:STRING=Release\nCMAKE_CXX_COMPILER:FILEPATH={sys.executable}\n")
    assert sys.version.split()[0] in hardware._compiler(tmp_path)


def test_a_compiler_that_cannot_answer_is_named_rather_than_called_unknown(tmp_path: Path):
    (tmp_path / "CMakeCache.txt").write_text(
        "CMAKE_CXX_COMPILER:FILEPATH=/nonexistent/g++-from-another-machine\n")
    described = hardware._compiler(tmp_path)
    assert "g++-from-another-machine" in described
    assert "did not answer" in described


def test_no_build_cache_is_unknown(tmp_path: Path):
    assert hardware._compiler(tmp_path) == "unknown"


def test_storage_in_memory_is_refused_against_a_real_tmpfs():
    # /dev/shm is a tmpfs on every Linux this runs on, so the refusal is tested against the real
    # thing rather than a mocked filesystem type.
    if hardware._run(["findmnt", "-no", "FSTYPE", "--target", "/dev/shm"]).strip() != "tmpfs":
        pytest.skip("/dev/shm is not a tmpfs here, so there is nothing to refuse")
    with pytest.raises(hardware.VolatileStorage) as refusal:
        hardware.require_durable_storage(Path("/dev/shm"))
    # The message has to say what to do, because the person reading it is mid-run.
    assert "--data-dir" in str(refusal.value)


def test_storage_on_a_disk_is_accepted_and_names_the_filesystem(tmp_path: Path):
    # The control: without it, a refusal that fires on everything passes the test above.
    if hardware._run(["findmnt", "-no", "FSTYPE", "--target", str(tmp_path)]).strip() == "tmpfs":
        pytest.skip("this machine's temporary directory is itself a tmpfs")
    assert hardware.require_durable_storage(tmp_path) not in ("tmpfs", "ramfs", "")


def test_the_platform_is_whatever_dmi_says_or_an_admission():
    # Not asserting a value: the point is that the field never comes back empty, so a published
    # record always says something about the machine it came from.
    assert hardware._platform()
