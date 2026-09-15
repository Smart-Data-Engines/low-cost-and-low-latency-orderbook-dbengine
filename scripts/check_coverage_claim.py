#!/usr/bin/env python3
"""Verify that what this repository says about its line coverage matches what CI enforces.

Two different claims live in the documents, and they rot in different ways.

The **floor** is derivable from the tree: `.github/workflows/ci.yml` fails the required `coverage`
job below `FLOOR`. Four documents quote that number in prose, so raising the gate by one line of
YAML leaves four pages asserting the old one - the same one-directional rot that made
`docs/github-security.md` say "ten required checks" under a ruleset requiring eleven. That is what
this script is mostly for, and it scans **every** document that states a floor rather than one of
them, because a mechanism guarding one page is how the second page quietly learns a different
number (#117 in the roadmap, from the other side).

The **measured figure** is not derivable here: it is produced by the `coverage` job on a GitHub
runner, and this script cannot re-measure it. What it can hold is that the figure is stated in a
falsifiable shape - a percentage together with the covered and total line counts it came from - and
that the two halves agree with each other. That shape is not decoration. Before #83 this repository
published "59.0% of 2387 lines" while the instrumentation reached 6 of 34 source files: the
percentage was plausible, and the denominator was the whole defect.

What this script proves:
  1. every floor quoted in prose equals FLOOR in the coverage job;
  2. every measured figure quoted in those documents carries its denominator, the percentage matches
     the division to one decimal place, and it sits at or above the floor - a citation of a run that
     would have failed the gate cannot be the run we cite.

What it cannot prove: that the cited run measured the cited tree, or that the figure is this week's.
Both are facts about somebody else's machine; the citation names the run so a reader can check.

Exit status 0 if clean, 1 otherwise.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKFLOW = REPO / ".github" / "workflows" / "ci.yml"

# Documents that state a floor. Adding one here is cheaper than finding out it disagreed.
DOCUMENTS = ["README.md", "docs/github-security.md", "docs/roadmap.md", "CLAUDE.md"]

FLOOR_IN_WORKFLOW = re.compile(r"^\s*FLOOR\s*=\s*(\d+(?:\.\d+)?)\s*$", re.MULTILINE)

# A floor claim, in the two shapes this tree writes it: "a 58% line floor" and "at least 58% line
# coverage". Bold markers are optional because both are emphasised somewhere.
#
# The word "floor" on its own is not the anchor, and the first version of this check proved why in
# one run: README.md states a **noise** floor for the comparative benchmark ("7% apart against a
# 21.2% floor"), and a pattern anchored on the bare word reported the benchmark's floor as a false
# claim about coverage. Two unrelated floors, one page - the shape of `rds` matching "records".
FLOOR_CLAIMS = [
    re.compile(r"(\d+(?:\.\d+)?)\s*%\*{0,2}\s+line\s+(?:coverage\s+)?floor"),
    re.compile(r"at least\s+\*{0,2}(\d+(?:\.\d+)?)\s*%\s+line\s+coverage"),
]

# The measured figure, in the shape this check exists to require: percentage, then the division it
# came from. "66.2% of 14,562 lines (9,640 covered)" and "66.2% (9,640 of 14,562 lines)" both read
# well; only the first is accepted, so there is one shape to parse and one to write. The pre-#83
# figures in the roadmap's comparison table are written "59.0% of **2387**", without the counts,
# and are deliberately outside this shape: they are the history of the defect rather than a claim
# about the tree.
MEASURED = re.compile(
    r"(\d+(?:\.\d+)?)\s*%\s+of\s+([\d,]+)\s+lines\s+\((?:\*{0,2})?([\d,]+)(?:\*{0,2})?\s+covered\)")

# Fenced blocks are skipped for the **measured figure** only: a percentage with counts beside it
# inside a fence is most likely a paste of a run's output, which is history rather than a claim, and
# check_roadmap.py learned the same thing about `#0` in a stack trace.
#
# Floors are scanned everywhere, fences included, and that is not laziness - it is where the claim
# turned out to live. `docs/github-security.md` states the floor in its checklist, which is fenced
# for monospace rather than because it quotes anything, so skipping fences made the one page whose
# job is to be audited the one page not audited. It is safe because neither shape below is a shape
# the gate itself prints: its output is `line coverage 66.2% (floor 58.0%)`, and the workflow says
# `FLOOR = 58.0`.
FENCE = re.compile(r"^```", re.MULTILINE)


def strip_fenced(text: str) -> str:
    out, fenced = [], False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        out.append("" if fenced else line)
    return "\n".join(out)


def main() -> int:
    problems: list[str] = []

    yaml = WORKFLOW.read_text(encoding="utf-8")
    floors = FLOOR_IN_WORKFLOW.findall(yaml)
    if len(floors) != 1:
        print(f"::error::expected exactly one FLOOR assignment in {WORKFLOW.name}, found {len(floors)}")
        return 1
    floor = float(floors[0])
    print(f"{WORKFLOW.relative_to(REPO)}: the coverage job fails below {floor}%")

    claims: dict[str, int] = {}
    for rel in DOCUMENTS:
        path = REPO / rel
        if not path.exists():
            problems.append(f"{rel}: listed as a document that states the floor, and is not there")
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in FLOOR_CLAIMS:
            for match in pattern.finditer(text):
                claims[rel] = claims.get(rel, 0) + 1
                quoted = float(match.group(1))
                if quoted != floor:
                    problems.append(
                        f"{rel}: claims a {quoted}% line-coverage floor; the job enforces {floor}%"
                        f"  ({match.group(0).strip()!r})")

    if not claims:
        problems.append(
            "no document states the line-coverage floor; the gate would be invisible to a reader"
            " - and this check would pass by finding nothing, which is the failure it has to refuse")
    elif "README.md" not in claims:
        # The front page specifically, not just somewhere. The floor is what makes the CI badge at
        # the top of README.md mean anything, and a reader who is going to check one page checks
        # that one.
        problems.append(
            "README.md does not state the line-coverage floor; it carries the CI badge, and the"
            " badge means nothing to a reader who is not told what the job refuses")

    # Every document, not just the one this rule was written for. The same reasoning as the floor
    # scan above: a figure checked on one page is how a second page keeps an older one.
    figures = 0
    for rel in DOCUMENTS:
        path = REPO / rel
        if not path.exists():
            continue
        prose = strip_fenced(path.read_text(encoding="utf-8"))
        for pct, total, covered in MEASURED.findall(prose):
            figures += 1
            total_n, covered_n = int(total.replace(",", "")), int(covered.replace(",", ""))
            derived = round(100.0 * covered_n / total_n, 1)
            if abs(derived - float(pct)) > 0.05:
                problems.append(
                    f"{rel}: {pct}% does not follow from {covered_n}/{total_n} lines, which is"
                    f" {derived}%")
            elif float(pct) < floor:
                problems.append(
                    f"{rel}: cites {pct}%, below the {floor}% floor - that run would have failed the"
                    " gate")
            else:
                print(f"{rel}: {pct}% of {total_n} lines ({covered_n} covered) -> {derived}%, at or"
                      f" above the {floor}% floor")

    if figures == 0:
        problems.append(
            "no document states a measured line coverage in the form"
            " '<pct>% of <total> lines (<covered> covered)'; a bare percentage is the shape that"
            " survived #83, and this check passing by finding nothing is the failure it refuses")

    for problem in problems:
        print(f"::error::{problem}")
    print(f"\n{sum(claims.values())} floor claim(s) checked across {len(claims)} of"
          f" {len(DOCUMENTS)} documents; {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
