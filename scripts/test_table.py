#!/usr/bin/env python3
"""Print the test-suite table's numbers **and** the citation that measured them, together.

Why a script rather than a rule. The table in `docs/roadmap.md` carries three counts and a link to
the CI run that produced them, and those two halves have drifted apart twice — once with #129, and
then again in the very commit that added a paragraph telling the next person not to let them drift.
A sentence asking a human to keep two numbers in step is the shape this repository keeps finding
rotten: a claim with nothing over it.

Nothing offline can verify these numbers, because they are measured on someone else's machine. What
*can* be removed is the possibility of updating one half without the other: this reads the job logs
of one pull request's run and emits both halves in one block, so the drift needs someone to ignore
the tool rather than merely forget a line.

    scripts/test_table.py 127

Reads `gh` from PATH and needs no build. Exits non-zero if any of the three jobs is missing or its
log does not contain a count - a silent zero here would be worse than an error, because it would be
pasted into the table.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys

REPO = "Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine"
JOBS = ("build-and-test", "integration-tests", "sanitizers-integration (tsan)")
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def gh(*args: str) -> str:
    out = subprocess.run(["gh", *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"gh {' '.join(args)} failed: {out.stderr.strip()}")
    return out.stdout


def main() -> int:
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        sys.exit(__doc__)
    pr = sys.argv[1]

    head = json.loads(gh("pr", "view", pr, "--repo", REPO, "--json", "headRefOid"))["headRefOid"]
    checks = json.loads(gh("api", f"repos/{REPO}/commits/{head}/check-runs?per_page=100"))
    urls = {c["name"]: c.get("html_url", "") for c in checks["check_runs"]}
    if "integration-tests" not in urls:
        sys.exit(f"PR #{pr}'s head {head[:8]} has no integration-tests check run")
    run = re.search(r"/runs/(\d+)", urls["integration-tests"])
    if not run:
        sys.exit("could not read the run id out of the check run's url")
    run_id = run.group(1)

    jobs = json.loads(gh("api", f"repos/{REPO}/actions/runs/{run_id}/jobs?per_page=100"))["jobs"]
    by_name = {j["name"]: j["id"] for j in jobs}

    found: dict[str, str] = {}
    for name in JOBS:
        if name not in by_name:
            sys.exit(f"run {run_id} has no job called {name!r} - the table's row for it would be a "
                     f"number from somewhere else")
        log = gh("api", f"repos/{REPO}/actions/jobs/{by_name[name]}/logs")
        # Colour codes sit between the word and the number in a CI terminal, which is how a gate
        # in the SDK once announced "ran no tests at all" directly under sixteen passing ones.
        log = ANSI.sub("", log)
        if name == "build-and-test":
            hits = re.findall(r"tests passed, \d+ tests failed out of (\d+)", log)
            found[name] = hits[-1] if hits else ""
        else:
            # The **last** match, not the first: this log holds per-test timings and a report
            # plugin's own summaries, and the first version of this script read one of those and
            # printed "17 in 0:00" for a job that ran 265 tests in 20:54. A number a tool is about
            # to hand to a document has to come from the line that is the verdict.
            hits = re.findall(r"(\d+) passed[^\r\n]*?in (\d+(?:\.\d+)?)s", log)
            if hits:
                count, seconds = hits[-1]
                total = int(float(seconds))
                found[name] = f"{count} in {total // 60}:{total % 60:02d}"
            else:
                found[name] = ""
        if not found[name]:
            sys.exit(f"{name} produced no count in its log. A blank here must not become a table "
                     f"entry: read the job before editing the row")

    print(f"Verified by [the full CI run for PR #{pr}]"
          f"(https://github.com/{REPO}/actions/runs/{run_id}),")
    print()
    print(f"  C++ (GTest + RapidCheck)        {found['build-and-test']}")
    print(f"  Python integration              {found['integration-tests']}")
    print(f"  Python integration under TSan   {found['sanitizers-integration (tsan)']}")
    print()
    print("Both halves come out of this one command. Paste the citation and the counts in the same")
    print("edit; they describe a tree that only exists together.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
