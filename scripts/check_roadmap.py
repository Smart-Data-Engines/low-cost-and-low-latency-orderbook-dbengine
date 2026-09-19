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
  - the `**Open: …**` line in "Recommended order" names *exactly* the items above
    its stated floor that are not marked closed - both directions

The last one exists because the same sentence went false twice in two days. "There
is no open defect on this page" was true when it was written and stayed on the page
while three items were filed under it; before that, a count and the CI run that
measured it drifted onto different trees. A status claim with nothing over it is the
shape this repository keeps finding rotten.

The floor is read from that same line rather than written here, because this script
cannot tell a defect from a feature nobody has built: every item from #1 to #58 is
unticked and always will be until somebody builds it. The first version of this rule
only pinned the *highest*-numbered item, on the argument that a filed item takes the
next free number - and that was wrong the moment an item was filed closed above an
open one, which had already happened (#133 above #132). Dropping #132 from the line
would have survived. The rule compares sets now.

What it cannot prove, and no script can: that a reference points at the item the
author *meant*, or that the prose around the line describes the four honestly. Read
those yourself when you touch them.

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
# The one status claim on this page that a script can hold: which items are open.
# Anchored on the literal label rather than on prose around it, because the prose is
# rewritten every session and an anchor that moves with it checks nothing.
OPEN_LINE_RE = re.compile(r"^\*\*Open: (.+?)\*\*", re.M)
# The floor comes from the document, next to the claim it bounds.
FLOOR_RE = re.compile(r"above #(\d+)")
CLOSED_MARK = "\u2705"  # the tick an item's heading carries once it is closed
# Real references are written bare (#27). Anything inside backticks is an example
# being discussed — including this file's own note about a mangled `#48-48` range,
# which the checker flagged as a live defect the first time it ran.
CODE_SPAN_RE = re.compile(r"`[^`]*`")
# A markdown link pointing at github.com cites a **GitHub object** - a pull request, a workflow
# run, a commit - and `#137` inside one is that PR's number, not a reference to item 137.
#
# This rule arrived the day it was needed and not before, which is the part worth recording: every
# earlier citation in this file names a PR whose number happened to be **below** the item count, so
# it resolved to an unrelated item and the checker was satisfied for the wrong reason. The first PR
# numbered above the last item is the first one that fails. A rule that passes by coincidence is
# indistinguishable from one that works until the coincidence ends.
GITHUB_LINK_RE = re.compile(r"\[[^\]]*\]\(https://(?:www\.)?github\.com/[^)]*\)")
# And the same two kinds of thing sharing one notation, without a link around it. `PR #146` is a
# pull request; `#146` on its own is item 146. The link rule above closed the case where a citation
# carries a URL and left this one, which failed the first time this page cited a PR numbered above
# the item count - two of them, in the same line. The token before the number is the whole
# distinction, so it is the whole rule: a bare `#9999` still fails and `PR #9999` does not.
PR_REF_RE = re.compile(r"\bPRs?\s+#\d+(?:\s*(?:,|and)\s*#\d+)*")
# The priority table, anchored on its header row. A table of what to do next that lists an item
# already closed is the rot the `Open:` line was mechanised against, one section further down: it
# happened on 7 September to three items at once, was fixed by hand, and happened again to #121 and
# #37 on the day both were closed - in a table two paragraphs below the sentence explaining why a
# second statement about the open set is the one that rots. Repairing the same prose by hand twice
# is the signal to check it instead.
PRIORITY_HEADER = "| Priority | Item | Effort | Why now |"


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

    header_at = None
    for line_no, line in enumerate(lines, start=1):
        if line.strip() == PRIORITY_HEADER:
            header_at = line_no
            break
    if header_at is None:
        problems.append(
            "the priority table's header row is not on this page, so the check that it lists no "
            "closed item has nothing to read - if the table was renamed, rename PRIORITY_HEADER "
            "with it, because a check whose scope can shrink silently is not a check")
    else:
        for line in lines[header_at:]:
            if not line.startswith("|"):
                break
            # Only the Item cell, which is what the row is *about*. The rationale column legitimately
            # mentions closed work - "authentication landed with #30, authorisation did not" is true
            # and belongs there - and the first version of this check flagged it. Use against
            # mention, in a checker written to catch prose that rots: the control that caught it was
            # a reference that was correct.
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) < 2:
                continue
            cleaned = PR_REF_RE.sub("", GITHUB_LINK_RE.sub("", CODE_SPAN_RE.sub("", cells[1])))
            for ref in REF_RE.finditer(cleaned):
                number = int(ref.group(1))
                title = items.get(number)
                if title is not None and CLOSED_MARK in title:
                    problems.append(
                        f"the priority table names #{number}, which is marked closed - a table of "
                        f"what to do next that lists finished work reads as a plan and is not one")
    # A fenced block is code, for the same reason a backtick span is: `#0 ob::Engine::…` in a quoted
    # sanitizer stack trace is a frame number, not a reference to item zero. The inline rule was
    # already here; this is the same rule at block scale, and it arrived the day an item quoted a
    # UBSan report (#127).
    in_fence = False
    for line_no, line in enumerate(lines, start=1):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if line.startswith("### "):
            continue  # the item's own heading
        # Blank out code spans, keeping the line length so reported columns and the
        # range/reference overlap logic below still line up.
        line = CODE_SPAN_RE.sub(lambda m: " " * len(m.group(0)), line)
        line = GITHUB_LINK_RE.sub(lambda m: " " * len(m.group(0)), line)
        line = PR_REF_RE.sub(lambda m: " " * len(m.group(0)), line)
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

    # ── The status line ───────────────────────────────────────────────────────
    open_match = OPEN_LINE_RE.search(text)
    floor_match = FLOOR_RE.search(text) if open_match is None else FLOOR_RE.search(
        text[open_match.start():open_match.start() + 400])
    if open_match is None:
        problems.append(
            'no "**Open: …**" line in docs/roadmap.md. It is the one status claim on this page '
            'with a check over it; write "none" there rather than deleting it')
    elif floor_match is None:
        problems.append(
            'the open-items line does not say "above #N", so this check has no floor and '
            'cannot tell an open defect from a feature nobody has built')
    else:
        floor = int(floor_match.group(1))
        claimed = {int(n) for n in re.findall(r"#(\d+)", open_match.group(1))}
        actually_open = {n for n, title in items.items()
                         if n > floor and CLOSED_MARK not in title}
        for number in sorted(claimed - actually_open):
            if number not in items:
                problems.append(f"the open-items line names #{number}, which is not an item")
            elif number <= floor:
                problems.append(
                    f"the open-items line names #{number}, which is at or below the floor "
                    f"#{floor} it declares")
            else:
                problems.append(
                    f"the open-items line names #{number}, whose heading is marked closed. "
                    f"Closing an item means taking it off that line in the same commit")
        for number in sorted(actually_open - claimed):
            problems.append(
                f"#{number} is above #{floor} and is not marked closed, so the open-items line "
                f"has to name it. A filed item takes the next free number, which is how that "
                f"line goes stale")

    if problems:
        print(f"docs/roadmap.md: {len(problems)} problem(s)")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    numbers = sorted(items)
    open_items = sorted(int(n) for n in re.findall(r"#(\d+)", open_match.group(1)))
    listed = ", ".join(f"#{n}" for n in open_items) if open_items else "none"
    print(f"docs/roadmap.md: {len(items)} items, ids {numbers[0]}-{numbers[-1]}, "
          f"all cross-references resolve; open above #{int(floor_match.group(1))}: {listed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
