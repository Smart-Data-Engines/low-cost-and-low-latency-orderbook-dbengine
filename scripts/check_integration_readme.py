#!/usr/bin/env python3
"""The integration battery's README lists every file of tests/integration/, and nothing else.

The list in tests/integration/README.md said it was complete as of #101, and by #281 it named 31 of
the directory's 93 files: a module added without a line there is invisible to anyone deciding from
the README what the battery covers, and nothing failed when one was. This holds the list to the
directory in both directions - a file not listed, a line naming a file that is not there, and a
file listed twice.

    python3 scripts/check_integration_readme.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIRECTORY = ROOT / "tests" / "integration"
README = DIRECTORY / "README.md"


def listed(text: str) -> list[str]:
    """The names in the README's tree: the lines of its `tests/integration/` block that start with
    a branch, in their order."""
    block = re.search(r"^```\ntests/integration/\n(.*?)^```", text, re.M | re.S)
    if block is None:
        raise SystemExit(f"{README.relative_to(ROOT)}: no ```tests/integration/ block to read")
    return re.findall(r"^[├└]── (\S+)", block.group(1), re.M)


def main() -> int:
    names = listed(README.read_text(encoding="utf-8"))
    present = {p.name for p in DIRECTORY.iterdir() if p.is_file() and not p.name.startswith(".")}
    problems = []
    seen = set()
    for name in names:
        if name in seen:
            problems.append(f"{name} is listed twice")
        seen.add(name)
    problems += [f"{name} is in the directory and not in the list" for name in sorted(present - seen)]
    problems += [f"{name} is listed and not in the directory" for name in sorted(seen - present)]
    if problems:
        for p in problems:
            print(f"{README.relative_to(ROOT)}: {p}", file=sys.stderr)
        return 1
    print(f"{README.relative_to(ROOT)}: every one of the directory's {len(present)} files is listed, "
          "once")
    return 0


if __name__ == "__main__":
    sys.exit(main())
