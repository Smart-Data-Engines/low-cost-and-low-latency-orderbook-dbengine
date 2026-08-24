#!/usr/bin/env python3
"""Verify that every metric written by name is registered in MetricsRegistry.

A write to an unregistered counter is discarded and `/metrics` reports a flat zero
for it for ever. The registry logs an ERROR when it happens, but nothing fails: the
server starts, the tests pass, and the dashboard shows a metric that never moves.

Two of them lived in the tree at once — `ob_mm_duplicates_dropped`, the number of
remote records refused as duplicates, and `ob_sequence_gaps_detected` — so the two
numbers describing whether multi-master deduplication works at all were both
invisible. Both were introduced with the mechanisms they measure, which is exactly
when nobody is looking at the dashboard yet.

What this script proves: every string literal handed to increment_counter(),
add_to_counter(), set_gauge() or observe_histogram() in src/ and tools/ appears in a
make_counter/make_gauge/make_histogram call in src/metrics.cpp.

What it cannot prove: that a metric name is spelled the way the dashboard expects,
or that a registered metric is ever written. Read those yourself.

Exit status 0 if clean, 1 otherwise.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
REGISTRY = REPO / "src" / "metrics.cpp"

REGISTERED = re.compile(r'make_(?:counter|gauge|histogram)\(\s*"([^"]+)"')
WRITTEN = re.compile(
    r'(?:increment_counter|add_to_counter|set_gauge|observe_histogram)\(\s*"([^"]+)"')


def main() -> int:
    if not REGISTRY.is_file():
        print(f"{REGISTRY}: not found", file=sys.stderr)
        return 1

    registered = set(REGISTERED.findall(REGISTRY.read_text(encoding="utf-8")))

    written: dict[str, set[str]] = {}
    for directory in ("src", "tools"):
        for path in sorted((REPO / directory).glob("*.cpp")):
            for name in WRITTEN.findall(path.read_text(encoding="utf-8")):
                written.setdefault(name, set()).add(f"{directory}/{path.name}")

    missing = {name: files for name, files in written.items() if name not in registered}
    if missing:
        print("Metrics written by name but never registered:", file=sys.stderr)
        for name, files in sorted(missing.items()):
            print(f"  {name} — written in {', '.join(sorted(files))}", file=sys.stderr)
        print("\nEvery write to one of these is discarded and /metrics reports a flat zero.",
              file=sys.stderr)
        print("Register it in MetricsRegistry::MetricsRegistry() in src/metrics.cpp.",
              file=sys.stderr)
        return 1

    print(f"src/metrics.cpp: {len(registered)} metrics registered, "
          f"{len(written)} written by name, all resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
