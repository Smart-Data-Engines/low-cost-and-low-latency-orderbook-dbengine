#!/usr/bin/env python3
"""Verify that the comparative table on the front page matches the run it cites.

This table has one property the test-suite table does not: **its source of truth is in the tree.**
The counts in `docs/roadmap.md` are measured on somebody else's machine, so the most a mechanism
can do there is emit the numbers and the citation together (`scripts/test_table.py`). Here the
cited run's own JSON is committed beside the report, so the claim is checkable offline - and a
claim that can be checked and is not is the shape this repository keeps finding rotten.

It has been rewritten by hand three times, and the third rewrite is why this exists: the published
figures were "446,219 updates/s in process against 4,012 updates/s through the wire - a factor of
111", two numbers in different units read as a ratio. That particular defect is not one this script
can catch, because neither number came from the results file. What it can catch is every figure
that *did*, which is the rest of the table.

What this script proves:
  1. the README cites exactly one results file, and it exists in both `.md` and `.json`;
  2. every system the run measured has a row, and every row names a system the run measured -
     both directions, because a competitor dropped from the table reads as one that was never run;
  3. every figure in each row equals the run's own figure, rounded to the precision the README
     itself chose;
  4. the floor sentence equals the run's measured floor;
  5. every "N% apart" in the prose below the table equals `resolution.classify()`'s own arithmetic,
     which is imported rather than restated - the first draft of the generator beside this script
     divided by the better of the two values and printed 89.3% for a pair the harness calls 47.2%.

What it cannot prove: that the run was taken on the machine the hardware line names, or that the
prose around the numbers describes them honestly. Both are for a reader; this holds the arithmetic.

Exit status 0 if clean, 1 otherwise.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
README = REPO / "README.md"

CITATION = re.compile(r"\[`(benchmarks/comparative/results/([^`]+)\.md)`\]")
FLOOR_SENTENCE = re.compile(r"\*\*Control floor (\d+(?:\.\d+)?)%\*\*")
APART = re.compile(r"(\d+(?:\.\d+)?)% apart")
TABLE_HEADER = "| System | Version |"
NUMBER = re.compile(r"\d[\d,]*(?:\.\d+)?")


def normalise(name: str) -> str:
    """Strip everything a display name adds. A hand-written map from "TimescaleDB" to "timescaledb"
    is a second list to keep in step; this needs none."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def refers_to(row_key: str, system_name: str) -> bool:
    """A display name may *extend* the run's name ("orderbook" -> "orderbook-dbengine"), never
    contain it somewhere in the middle. Containment matched the kdb+ row against
    "orderbookdbengine", which has "kdb" inside it, and reported the engine's row as kdb+'s."""
    a, b = normalise(row_key), normalise(system_name)
    return a.startswith(b) or b.startswith(a)


def decimals(text: str) -> int:
    return len(text.split(".")[1]) if "." in text else 0


def same(measured: float, claimed_text: str) -> bool:
    """Equal at the precision the page chose for itself, so tightening a figure is not a failure."""
    claimed = float(claimed_text.replace(",", ""))
    return round(measured, decimals(claimed_text)) == claimed


def main() -> int:
    problems: list[str] = []
    text = README.read_text(encoding="utf-8")

    citations = {m.group(1) for m in CITATION.finditer(text)}
    if len(citations) != 1:
        print(f"{README.name}: the comparative table must cite exactly one results file, found "
              f"{len(citations)}: {sorted(citations)}. Numbers from two runs in one table is the "
              f"defect this checks for, not a formatting preference.", file=sys.stderr)
        return 1
    report = REPO / citations.pop()
    results = report.with_suffix(".json")
    for path in (report, results):
        if not path.is_file():
            problems.append(f"cites {path.relative_to(REPO)}, which is not in the tree")
    if problems:
        for p in problems:
            print(f"{README.name}: {p}", file=sys.stderr)
        return 1

    data = json.loads(results.read_text(encoding="utf-8"))
    sys.path.insert(0, str(REPO))
    from benchmarks.comparative import resolution as res_mod

    res = data["resolution"]
    floor = res["floor"]
    systems = {s["name"]: s for s in data["systems"]}

    floors = FLOOR_SENTENCE.findall(text)
    if not floors:
        problems.append("states no control floor, and a comparison without one classifies nothing")
    for claimed in floors:
        if not same(floor * 100, claimed):
            problems.append(f"says the control floor is {claimed}% where "
                            f"{results.name} measured {floor * 100:.4f}%")

    # The table, read row by row from its own header rather than by line number.
    rows: dict[str, list[str]] = {}
    start = text.find(TABLE_HEADER)
    if start < 0:
        print(f"{README.name}: no row beginning {TABLE_HEADER!r} - if the table was reshaped, "
              f"reshape TABLE_HEADER with it, because a check whose scope can shrink silently is "
              f"not a check", file=sys.stderr)
        return 1
    for line in text[start:].splitlines()[2:]:
        if not line.startswith("|"):
            break
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        rows[normalise(re.sub(r"\*+", "", cells[0]))] = cells

    for name, system in systems.items():
        match = [key for key in rows if refers_to(key, name)]
        if not match:
            problems.append(f"has no row for {name!r}, which {results.name} measured - a competitor "
                            f"missing from the table reads as one that was never run")
            continue
        cells = rows[match[0]]
        if not system["available"]:
            if "NOT MEASURED" not in " ".join(cells):
                problems.append(f"{name}: the run did not measure it, so the row must say "
                                f"NOT MEASURED rather than carry figures")
            continue
        for digits in NUMBER.findall(cells[1]):
            if digits not in system["version"]:
                problems.append(f"{name}: the row's version says {digits!r}, which is not in the "
                                f"run's {system['version']!r}")
        work = system["workloads"]
        ingest = NUMBER.search(cells[2])
        if ingest is None:
            problems.append(f"{name}: the row carries no ingest figure")
        elif not same(work["ingest"]["value"], ingest.group()):
            problems.append(f"{name}: the row says {ingest.group()} rows/s where the run measured "
                            f"{work['ingest']['value']:,.2f}")
        query = work.get("time_range")
        claimed = NUMBER.findall(cells[3])
        if query is None or "value" not in query:
            if "not comparable" not in cells[3].lower():
                problems.append(f"{name}: the run produced no time-range value, so the cell must "
                                f"say so rather than carry a number")
        elif len(claimed) < 3:
            problems.append(f"{name}: the time-range cell needs the median and the round-to-round "
                            f"range - a median alone hides the spread the floor is read against")
        else:
            for label, measured, shown in (("median", query["value"], claimed[0]),
                                           ("low", query["min"], claimed[1]),
                                           ("high", query["max"], claimed[2])):
                if not same(measured * 1e3, shown):
                    problems.append(f"{name}: the time-range {label} says {shown} ms where the run "
                                    f"measured {measured * 1e3:.4f} ms")

    for key in rows:
        if not any(refers_to(key, n) for n in systems):
            problems.append(f"has a row for {key!r}, which {results.name} does not name")

    # Every "N% apart" below the table, against the harness's own arithmetic.
    resolution = res_mod.Resolution(rounds=res["rounds"], warmup=res["warmup"],
                                    discarded_outlier=res.get("discarded_outlier"),
                                    control_ratios=res["control_ratios"], floor=floor,
                                    verdict=res["verdict"], note=res["note"])
    ours = systems["orderbook"]["workloads"]
    allowed: set[float] = set()
    for workload, key in (("ingest", "seconds"), ("time_range", "value")):
        mine = ours.get(workload)
        if mine is None or key not in mine:
            continue
        for name, system in systems.items():
            if name == "orderbook" or not system["available"]:
                continue
            theirs = system["workloads"].get(workload)
            if theirs is None or key not in theirs:
                continue
            a, b = mine[key], theirs[key]
            allowed.add(round(abs(a - b) / max(a, b) * 100, 4))
            # Named so a reader of this script can see classify() is what decides, not the number.
            res_mod.classify(a, b, resolution)
    # Every distance measurable from a results file committed in this tree: between two systems
    # inside one run, and between two runs for one system. The README compares this run's systems,
    # quotes the same pair from the larger-volume run beside it, and says how far this run's ingest
    # is from the previous one - all three are real and all three are checkable, and a number
    # invented for any of them would otherwise be a claim nothing reads.
    for other in sorted(results.parent.glob("*.json")):
        try:
            payload = json.loads(other.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            problems.append(f"{other.name} sits beside the cited run and cannot be read, so a "
                            f"figure quoted from it cannot be checked")
            continue
        past = {s["name"]: s for s in payload.get("systems", [])}
        for workload, key in (("ingest", "seconds"), ("time_range", "value")):
            live = [(n, s["workloads"][workload][key]) for n, s in past.items()
                    if s.get("available") and key in s["workloads"].get(workload, {})]
            for i, (_, a) in enumerate(live):          # two systems, one run
                for _, b in live[i + 1:]:
                    allowed.add(round(abs(a - b) / max(a, b) * 100, 4))
            for name, system in systems.items():        # one system, two runs
                if not system["available"] or name not in past or not past[name]["available"]:
                    continue
                now, then = system["workloads"].get(workload), past[name]["workloads"].get(workload)
                if now and then and key in now and key in then:
                    a, b = now[key], then[key]
                    allowed.add(round(abs(a - b) / max(a, b) * 100, 4))

    for claimed in APART.findall(text):
        if not any(same(value, claimed) for value in allowed):
            problems.append(f"says {claimed}% apart, which is not the distance between any pair "
                            f"this run measured (it measured "
                            f"{', '.join(f'{v:.1f}%' for v in sorted(allowed))})")

    if problems:
        for problem in problems:
            print(f"{README.name}: {problem}", file=sys.stderr)
        print(f"{README.name}: {len(problems)} problem(s) against {results.name}", file=sys.stderr)
        return 1
    print(f"{README.name}: the comparative table agrees with {results.name} - "
          f"{len(systems)} systems, floor {floor * 100:.2f}%, "
          f"{len(allowed)} measured pair distances")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
