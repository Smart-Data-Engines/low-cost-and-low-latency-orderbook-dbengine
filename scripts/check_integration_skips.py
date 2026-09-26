#!/usr/bin/env python3
"""The integration job's gate on every test that did not simply pass or fail (#48, PR #187).

A skip is a test that did not run, and the only ones the job accepts are the two Binance tests that
need a live feed. Anything else - a binary the build step forgot, a fixture that gave up, a marker
added without thinking - would otherwise pass as green.

A strict xfail is not one. The test ran, and failed the way a defect the roadmap files makes it
fail, and `strict=True` turns it red the day it passes. pytest's JUnit report files it as
`<skipped type="pytest.xfail">` all the same - which is how the battery's first xfails since this
gate was written, five of them in PR #187, turned the job red over 407 passing tests. So an xfail is
accepted when its reason names an item the roadmap's `**Open: ...**` line lists, and refused when it
names none, or only closed ones: a marker that outlived its item is a skip with a better excuse.

    scripts/check_integration_skips.py integration-report.xml [docs/roadmap.md]
    scripts/check_integration_skips.py --self-test

Exit status 0 when every test that did not pass is accounted for, 1 otherwise.
"""
from __future__ import annotations

import pathlib
import re
import sys
import xml.etree.ElementTree as ET

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from check_roadmap import OPEN_LINE_RE  # noqa: E402  - that line has one parser, and it is there

ROADMAP = pathlib.Path(__file__).resolve().parent.parent / "docs" / "roadmap.md"


def open_items(roadmap: str) -> set[int]:
    match = OPEN_LINE_RE.search(roadmap)
    return {int(n) for n in re.findall(r"#(\d+)", match.group(1))} if match else set()


def check(report_xml: str, roadmap: str) -> tuple[list[str], list[str]]:
    """What was accepted and what was not, one line each."""
    still_open = open_items(roadmap)
    accepted, problems = [], []
    for case in ET.fromstring(report_xml).iter("testcase"):
        skipped = case.find("skipped")
        if skipped is None:
            continue
        where = f"{case.get('classname', '')}::{case.get('name', '')}"
        why = (skipped.get("message") or "").splitlines()[0][:160] if skipped.get("message") else ""
        if skipped.get("type") == "pytest.xfail":
            named = {int(n) for n in re.findall(r"#(\d+)", why)}
            if named & still_open:
                accepted.append(f"xfail: {where} - {why}")
            elif named:
                problems.append(f"xfail naming only items the roadmap does not list as open "
                                f"({', '.join(f'#{n}' for n in sorted(named))}): {where} - {why}")
            else:
                problems.append(f"xfail naming no roadmap item: {where} - {why}")
            continue
        # Matched on classname *and* name: a module skipped at collection time - which is how the
        # Binance ones skip, on a marker - lands in the report with an empty classname.
        if "binance" in where.lower():
            accepted.append(f"skipped: {where} - {why}")
        else:
            problems.append(f"skipped: {where} - {why}")
    return accepted, problems


def self_test() -> int:
    """The gate's own cases, so that a change to it cannot quietly accept everything."""
    roadmap = "**Open: #7, #9.** Every other item above #5 is marked closed\n"

    def report(*cases: str) -> str:
        return "<testsuites><testsuite>" + "".join(cases) + "</testsuite></testsuites>"

    def case(name: str, inner: str = "", classname: str = "test_m") -> str:
        return f'<testcase classname="{classname}" name="{name}">{inner}</testcase>'

    xfail = '<skipped type="pytest.xfail" message="{}" />'
    skip = '<skipped type="pytest.skip" message="{}" />'
    cases = [
        ("a pass", report(case("t")), True),
        ("an xfail naming an open item", report(case("t", xfail.format("#7: why"))), True),
        ("an xfail naming a closed item", report(case("t", xfail.format("#6: why"))), False),
        ("an xfail naming no item", report(case("t", xfail.format("broken"))), False),
        ("an xfail naming a closed and an open item",
         report(case("t", xfail.format("#6 and #9"))), True),
        ("a Binance skip at collection", report(case("test_binance_live", skip.format("c"), "")),
         True),
        ("any other skip", report(case("t", skip.format("no binary"))), False),
        ("a skip whose reason names an open item", report(case("t", skip.format("#7"))), False),
    ]
    failed = 0
    for what, xml, ok in cases:
        _, problems = check(xml, roadmap)
        if (not problems) != ok:
            print(f"self-test: {what}: expected {'accepted' if ok else 'refused'}, got "
                  f"{'refused' if problems else 'accepted'}")
            failed += 1
    print(f"self-test: {len(cases) - failed} of {len(cases)} cases as expected")
    return 1 if failed else 0


def main(argv: list[str]) -> int:
    if argv[1:] == ["--self-test"]:
        return self_test()
    if len(argv) not in (2, 3):
        print(__doc__)
        return 2
    report_xml = pathlib.Path(argv[1]).read_text()
    roadmap = pathlib.Path(argv[2] if len(argv) == 3 else ROADMAP).read_text()
    accepted, problems = check(report_xml, roadmap)
    for line in accepted:
        print(line)
    for line in problems:
        print(f"NOT ACCEPTED - {line}")
    if problems:
        print(f"\n{len(problems)} test(s) neither passed nor are accounted for: only the Binance "
              "tests may skip in this job, and an xfail must name an item the roadmap lists as open")
        return 1
    print(f"\n{len(accepted)} accounted for: Binance opt-ins, and xfails of open roadmap items")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
