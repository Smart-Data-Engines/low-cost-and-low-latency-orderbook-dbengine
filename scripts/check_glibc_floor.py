#!/usr/bin/env python3
"""What a packaged binary asks of the system it runs on, against the oldest system the packages
promise (#212): glibc 2.34 - Amazon Linux 2023, and RHEL 9 and its rebuilds - and no libstdc++ of
the system's at all, since the packaging build links its own in (OB_STATIC_LIBSTDCXX).

    scripts/check_glibc_floor.py [--floor 2.34] <ELF file>...

For each file, the symbol versions its undefined dynamic symbols require, read from `objdump -T`:
the newest GLIBC_ among them has to be at or below the floor, and none may be GLIBCXX_ or CXXABI_.
The binaries of a build on Ubuntu 24.04 asked for GLIBC_2.38 - glibc 2.38's headers turn strtol,
strtoul, strtoll, strtoull and sscanf into their C23 variants, `__isoc23_strtol` and the rest,
wherever _GNU_SOURCE is defined, and g++ always defines it - and for GLIBCXX_3.4.31, GCC 13's.

Exit status 0 when every file holds, 1 when one does not - naming the symbols that pass the floor -
and 2 when a file cannot be read.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys

# An undefined symbol's line: `0000000000000000      DF *UND*  0000000000000000 (GLIBC_2.34) pthread_create`.
# A version objdump prints without parentheses is one the symbol is defined at, not one it needs.
REQUIRED = re.compile(r"\*UND\*\s+\S+\s+\(([A-Z][A-Z0-9_]*?)_([0-9][0-9.]*)\)\s+(\S+)")
NOT_ALLOWED = ("GLIBCXX", "CXXABI")


def version_tuple(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split("."))


def requirements(path: str) -> list[tuple[str, str, str]]:
    """(version node, version, symbol) for every undefined dynamic symbol with a version."""
    out = subprocess.run(["objdump", "-T", path], capture_output=True, text=True)
    if out.returncode != 0:
        raise OSError(f"objdump -T {path}: {out.stderr.strip() or f'exit {out.returncode}'}")
    return [m.groups() for m in map(REQUIRED.search, out.stdout.splitlines()) if m]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--floor", default="2.34", help="the newest glibc a file may need (default: 2.34)")
    ap.add_argument("files", nargs="+")
    args = ap.parse_args()
    floor = version_tuple(args.floor)

    failed = False
    for path in args.files:
        try:
            needs = requirements(path)
        except OSError as exc:
            print(f"FAIL: {exc}")
            return 2
        glibc = [(version_tuple(ver), ver, sym) for node, ver, sym in needs if node == "GLIBC"]
        newest = max(glibc)[1] if glibc else "none"
        past = sorted({(ver, sym) for v, ver, sym in glibc if v > floor})
        cxx = sorted({(f"{node}_{ver}", sym) for node, ver, sym in needs if node in NOT_ALLOWED})
        if past or cxx:
            failed = True
            print(f"FAIL: {path} needs glibc {newest} (the floor is {args.floor})"
                  + (f" and the system's libstdc++ ({len(cxx)} symbols)" if cxx else ""))
            for ver, sym in past[:10]:
                print(f"    GLIBC_{ver}  {sym}")
            for node_ver, sym in cxx[:5]:
                print(f"    {node_ver}  {sym}")
        else:
            others = sorted({node for node, _ver, _sym in needs if node != "GLIBC"})
            print(f"  ok: {path} needs glibc {newest} at most, no libstdc++ of the system's"
                  + (f"; versioned symbols also from {', '.join(others)}" if others else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
