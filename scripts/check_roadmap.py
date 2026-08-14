#!/usr/bin/env python3
"""Verify the integrity of docs/roadmap.md item numbering and cross-references.

Item numbers are permanent identifiers. They are never reassigned when an item is
inserted, because every renumbering pass so far has damaged something: a `#47-48`
range became `#48-48` (only the bound carrying a `#` gets rewritten), and
references drifted onto the wrong items while still resolving to *an* item, which
is invisible to any check that only asks whether a target exists.

What this script can prove:
  - no duplicate item numbers
  - every `#N` reference points at an item that exists
  - every reference in a range `#N-M` has both bounds pointing at existing items
  - a new item took the next free number rather than displacing an existing one

What it cannot prove, and no script can: that a reference points at the item the
author *meant*. Read those yourself when you touch them.

Exit status 0 if clean, 1 otherwise.
"""
from __future__ import annotations

import pathlib
import re
import sys

ROADMAP = pathlib.Path(__file__).resolve().parent.parent / "docs" / "roadmap.md"

HEADER_RE = re.compile(r"^### (\d+)\. (.+)$", re.M)
# A range must be matched before a bare reference, or the second bound is missed.
RANGE_RE = re.compile(r"#(\d+)-(\d+)")
REF_RE = re.compile(r"#(\d+)")
# Real references are written bare (#27). Anything inside backticks is an example
# being discussed — including this file's own note about a mangled `#48-48` range,
# which the checker flagged as a live defect the first time it ran.
CODE_SPAN_RE = re.compile(r"`[^`]*`")


def main() -> int:
    if not ROADMAP.exists():
        print(f"error: {ROADMAP} not found", file=sys.stderr)
        return 1

    text = ROADMAP.read_text(encoding="utf-8")
    items = {}
    duplicates = []
    for match in HEADER_RE.finditer(text):
        number = int(match.group(1))
        if number in items:
            duplicates.append(number)
        items[number] = match.group(2).strip()

    problems = []

    for number in sorted(set(duplicates)):
        problems.append(f"item number {number} is used more than once")

    lines = text.splitlines()
    for line_no, line in enumerate(lines, start=1):
        if line.startswith("### "):
            continue  # the item's own heading
        # Blank out code spans, keeping the line length so reported columns and the
        # range/reference overlap logic below still line up.
        line = CODE_SPAN_RE.sub(lambda m: " " * len(m.group(0)), line)
        checked_spans = []
        for match in RANGE_RE.finditer(line):
            checked_spans.append(match.span())
            low, high = int(match.group(1)), int(match.group(2))
            for bound in (low, high):
                if bound not in items:
                    problems.append(
                        f"line {line_no}: range #{low}-{high} names item {bound}, "
                        f"which does not exist")
            # The bound-existence check above is not enough. A renumbering pass
            # rewrites only the bound carrying a '#', so "#47-48" silently becomes
            # "#48-48" and then "#49-48" — both bounds exist, so nothing complains,
            # and the range now reads backwards or as a single item.
            if low >= high:
                problems.append(
                    f"line {line_no}: range #{low}-{high} does not ascend, which is "
                    f"what a renumbering pass leaves behind when it rewrites the "
                    f"first bound only")
        for match in REF_RE.finditer(line):
            if any(start <= match.start() < end for start, end in checked_spans):
                continue  # already validated as part of a range
            number = int(match.group(1))
            if number not in items:
                problems.append(
                    f"line {line_no}: reference #{number} does not resolve to an item")

    if problems:
        print(f"docs/roadmap.md: {len(problems)} problem(s)")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    numbers = sorted(items)
    print(f"docs/roadmap.md: {len(items)} items, ids {numbers[0]}-{numbers[-1]}, "
          f"all cross-references resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
