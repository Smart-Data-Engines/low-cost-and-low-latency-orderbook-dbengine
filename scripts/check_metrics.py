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
increment_gauge(), set_gauge() or observe_histogram() in src/ and tools/ appears in a
make_counter/make_gauge/make_histogram call in src/metrics.cpp.

Two of those four were wrong until #113. It scanned `add_to_counter`, which
`MetricsRegistry` does not have - a dead branch that could never match - and did **not**
scan `increment_gauge`, which it does have and which eight sites use to move
`ob_active_sessions`. So a metric written only through `increment_gauge` escaped the check
entirely, and the claim in the sentence above was false for one of the four ways this
engine writes a metric. Nothing was actually unregistered; the gap was found by using the
script rather than by it firing.

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
    r'(?:increment_counter|increment_gauge|set_gauge|observe_histogram)\(\s*"([^"]+)"')

# The reverse direction, added by #54 stage C, which needed a "replication lag" gauge and found one
# that is registered and fed by nothing. The check above cannot see that: it walks from writes to
# registrations, so a registration with no write is invisible to it, and the docstring above says
# so in as many words. A gauge nobody sets reads as **zero**, which is the one value an operator
# cannot tell from good news - #23's lesson about a counter that means two things, one level up.
#
# "Written" is deliberately looser here than above: the name appearing as a string literal anywhere
# in src/ or tools/ counts. The three subscription counters are written through a local `publish()`
# lambda, so the literal sits next to `publish` rather than `increment_counter`, and a strict scan
# calls them dead. For finding *dead* metrics a loose test is the right direction to be wrong in:
# it can miss a name that is only mentioned, never a name that is genuinely fed.
ANY_LITERAL = re.compile(r'"(ob_[a-z0-9_]+)"')

# Registered, fed by nothing, and known. Each line is a claim that the gap is understood rather
# than unnoticed; #117 carries the work. Removing a name from here without feeding the metric puts
# this check back to red, which is the point.
NOT_YET_WRITTEN = {
    # One left, and it is the one that cannot be fed honestly rather than the one nobody got to.
    # #118 measured why: the only per-peer position the mesh has is a byte offset into the peer's
    # own WAL, frozen at handshake, so subtracting it from ours yields this node's own WAL size.
    # The honest mesh answer is in records, from compare_vectors(), which is a different metric
    # with a different name - so this entry is waiting to be *removed*, not filled.
    "ob_mm_replication_lag_bytes": "#118 - a byte lag the mesh cannot state; the fix renames it",
}


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

    mentioned: set = set()
    for directory in ("src", "tools"):
        for path in sorted((REPO / directory).rglob("*.cpp")):
            if path == REGISTRY:
                continue
            mentioned |= set(ANY_LITERAL.findall(path.read_text(encoding="utf-8")))

    dead = sorted(name for name in registered
                  if name not in mentioned and name not in NOT_YET_WRITTEN)
    stale_allowlist = sorted(name for name in NOT_YET_WRITTEN
                             if name in mentioned or name not in registered)

    problems = False
    if dead:
        print("Metrics registered but fed by nothing, so they report zero for ever:",
              file=sys.stderr)
        for name in dead:
            print(f"  {name}", file=sys.stderr)
        print("  Feed it, or add it to NOT_YET_WRITTEN with the item that will.", file=sys.stderr)
        problems = True
    if stale_allowlist:
        print("NOT_YET_WRITTEN names that are no longer accurate:", file=sys.stderr)
        for name in stale_allowlist:
            why = "now written" if name in mentioned else "no longer registered"
            print(f"  {name}: {why} - remove the entry", file=sys.stderr)
        problems = True

    missing = {name: files for name, files in written.items() if name not in registered}
    if missing:
        print("Metrics written by name but never registered:", file=sys.stderr)
        for name, files in sorted(missing.items()):
            print(f"  {name} — written in {', '.join(sorted(files))}", file=sys.stderr)
        print("\nEvery write to one of these is discarded and /metrics reports a flat zero.",
              file=sys.stderr)
        print("Register it in MetricsRegistry::MetricsRegistry() in src/metrics.cpp.",
              file=sys.stderr)
        problems = True

    if problems:
        return 1

    print(f"src/metrics.cpp: {len(registered)} metrics registered, "
          f"{len(written)} written by name, all resolve; "
          f"{len(NOT_YET_WRITTEN)} registered and knowingly fed by nothing "
          f"({', '.join(sorted(NOT_YET_WRITTEN))})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
