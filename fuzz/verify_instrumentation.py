#!/usr/bin/env python3
"""Refuse a fuzzing build whose instrumentation did not reach the parsers it claims to cover.

This exists because the same failure already happened once in this repository, in the ordinary
sanitizer build (#83): `add_compile_options()` only affects targets created *after* the call, and
the ASan block sat below all twenty-eight libraries. The sanitizers instrumented `ob_tcp_server`
and the test executables and none of the engine, and every job stayed green while doing it. A
fuzzer built that way explores `LLVMFuzzerTestOneInput` and reports nothing about the parser.

So this reads the compiler's own command lines rather than CMakeLists.txt. What CMake *says* and
what the compiler *received* are different claims, and only the second one decides anything.

Two blindnesses are possible and this script is written against both:

  - **The instrument reads a real flag as missing.** The first version of this check tested
    `'-fsanitize=undefined' in command`, which never matches: clang is handed
    `-fsanitize=address,undefined` as one argument. It reported every parser as uninstrumented in a
    build that was in fact fully instrumented. A sanitizer list is therefore parsed, not searched.
  - **The instrument reads nothing as fine.** A typo in a path, a renamed file, an empty
    `compile_commands.json` - each produces "no problems found". So absence is failure here: every
    expected translation unit must be *present* and instrumented, and the count of what was
    actually inspected is printed so a silent zero cannot pass for success.

Usage:

    python3 fuzz/verify_instrumentation.py <build-dir>
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Which production translation units each harness is claiming to fuzz. Deliberately explicit: this
# is the claim the fuzz job makes, so it is written down rather than inferred from link order, and
# a harness that stops reaching its parser has to change this file to stay green.
COVERS = {
    "fuzz_command_parser": ["src/command_parser.cpp"],
    "fuzz_mm_frames":      ["src/mm_framing.cpp"],
    "fuzz_wal_replay":     ["src/wal.cpp"],
}

REQUIRED = {"address", "undefined", "fuzzer-no-link"}


def sanitizers(command: str) -> set[str]:
    """Every sanitizer named in the command, however the lists were spelled.

    `-fsanitize=address,undefined` is one argument naming two sanitizers, and
    `-fno-sanitize-recover=undefined` names none: it changes what a finding does, not what is
    instrumented. Counting it would let a build pass with recovery configured and instrumentation
    absent.
    """
    found: set[str] = set()
    for arg in command.split():
        if arg.startswith("-fsanitize=") and "recover" not in arg:
            found.update(arg.split("=", 1)[1].split(","))
    return found


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__)
        return 2
    build = Path(argv[1])
    database = build / "compile_commands.json"
    if not database.is_file():
        print(f"::error::{database} does not exist; configure with -DOB_BUILD_FUZZERS=ON")
        return 1

    entries = json.loads(database.read_text())
    if not entries:
        print(f"::error::{database} is empty, so nothing was inspected")
        return 1

    repo = Path(__file__).resolve().parent.parent
    problems: list[str] = []
    inspected = 0

    fuzz_sources = sorted(p.name[:-4] for p in (repo / "fuzz").glob("*.cpp"))
    expected_harnesses = sorted(name[len("fuzz_"):] for name in COVERS)
    if fuzz_sources != expected_harnesses:
        problems.append(
            f"fuzz/ holds drivers {fuzz_sources} but this check knows {expected_harnesses}; "
            "a new harness has to declare which parser it covers")

    for harness, sources in sorted(COVERS.items()):
        name = harness[len("fuzz_"):]

        corpus = repo / "fuzz" / "corpus" / name
        seeds = sorted(p for p in corpus.glob("*") if p.is_file()) if corpus.is_dir() else []
        if not seeds:
            problems.append(f"{harness}: corpus {corpus} is missing or empty")

        for source in sources + [f"fuzz/{name}.cpp"]:
            matches = [e for e in entries if e["file"].endswith("/" + source)]
            if not matches:
                problems.append(f"{harness}: {source} was not compiled in this build")
                continue
            for entry in matches:
                inspected += 1
                missing = REQUIRED - sanitizers(entry["command"])
                if missing:
                    problems.append(
                        f"{harness}: {source} is missing {','.join(sorted(missing))}")

        print(f"{harness:22} {len(seeds):3} seeds   covers {', '.join(sources)}")

    # The build-wide number is the control. Every translation unit here is ours - FetchContent
    # dependencies are header-only or excluded - so a partial count means the flag was added after
    # some targets were created, which is exactly the shape #83 had.
    uninstrumented = [e["file"] for e in entries if REQUIRED - sanitizers(e["command"])]
    print(f"\ntranslation units in build   : {len(entries)}")
    print(f"inspected by name            : {inspected}")
    print(f"without full instrumentation : {len(uninstrumented)}")
    for path in uninstrumented[:10]:
        print(f"    {path}")

    if inspected != sum(len(v) + 1 for v in COVERS.values()):
        problems.append(f"inspected {inspected} translation units, expected "
                        f"{sum(len(v) + 1 for v in COVERS.values())}")

    if problems:
        for problem in problems:
            print(f"::error::{problem}")
        return 1

    print("\nevery parser and every harness carries " + "+".join(sorted(REQUIRED)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
