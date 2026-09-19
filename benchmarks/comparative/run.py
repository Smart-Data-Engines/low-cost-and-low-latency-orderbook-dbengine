"""One command for the whole comparative run.

    python -m benchmarks.comparative.run --rows 200000 --seed 7 --rounds 6

What it will not do, and each refusal is in the spec for a reason that cost somebody a wrong number:

* measure a build that is not Release — read from `CMakeCache.txt`, not from the directory name;
* run a comparison without first measuring **this machine's** noise floor, and report any difference
  smaller than that floor as a win;
* time a workload before checking the two systems return the same rows;
* accept a competitor that declares no tuning;
* write a report with no `limitations`, or with an empty `losses` and nothing saying how one was
  looked for;
* install anything. `install_competitors.md` is commands to read before pasting.

A missing system is a loud row in the table, never a blank cell: a skip nobody can see reads as a
pass, which is the lesson the CI skip gate came from.
"""
from __future__ import annotations

import argparse
import socket
import sys
from datetime import datetime, timezone
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Callable

from . import dataset, equivalence, hardware, report, resolution
from .systems.base import NoTuningDeclared, QueryResult, require_tuning
from .systems.clickhouse import ClickHouseSystem
from .systems.kdb import KdbSystem
from .systems.orderbook import OrderbookSystem
from .systems.timescaledb import TimescaleDbSystem

REPO = Path(__file__).resolve().parents[2]

# What this engine cannot do, stated because the comparison is uneven in its favour. A specialised
# engine beating general databases at its one workload is its whole thesis - and a number that does
# not say what it gives up promises a replacement.
# Figures in the prose below that justify a design choice rather than interpret the table are
# attributed rather than re-measured every run: what a fresh `clickhouse-client` costs is an
# argument for holding a connection open, and it does not change what this run measured. They said
# "measured" with no machine beside them, which on any other machine is a claim about the wrong one.
PROSE_FIGURES_MACHINE = ("an Intel i3-7100U workstation, ext4 over LUKS on NVMe - the machine this "
                         "harness was written on")

ENGINE_LIMITATIONS = [
    # The first entry of this list used to be the event-time limitation, and it is gone rather than
    # softened: `INSERT` and `MINSERT` take a trailing `event_time_ns` since #105, the Python client
    # sends it and refuses rather than dropping it when a server cannot store one, and the
    # time-range workload therefore holds all four systems to every column of its rows. Kept as a
    # comment for one release so that a reader comparing two published tables can see why the
    # exclusion disappeared: the number changed because the engine did, not because the harness
    # stopped looking.
    "orderbook: no bulk-load path over the wire, so the ingest row still measures the protocol's "
    "shape as much as the engine's speed - but less of it than it did. Since #141 this harness "
    "pipelines 64 updates per round trip, so it sends rows/64 requests where the SQL systems "
    "receive the whole CSV in one. 64 is the knee #141 measured (1.59x against one update per "
    "round trip, and 512 gave the same 1.59x), chosen from that measurement rather than by trying "
    "several here and keeping the best. {in_process}",
    "orderbook: no general-purpose SQL - a fixed set of commands, not a query language",
    "orderbook: no joins, and no cross-symbol queries",
    "orderbook: the schema is imposed, not derived from a model",
    "orderbook: append-only; nothing deletes rows except TTL retention",
    "orderbook: aggregates run over the live book, so a VWAP over a historical time range is not "
    "the same question the SQL equivalents answer",
]


def parse_cost(rows: int, samples: int = 9) -> tuple[float, float]:
    """Seconds to turn `rows` lines of tab-separated text into tuples, seven columns and three.

    Every query figure in the table includes this, for all three systems, because each adapter
    receives text. It was stated as "about 4.8 ms" - measured once, on the workstation this harness
    was written on, and then printed inside every report generated anywhere since.

    On the first machine that was not that one, the same table's fastest query median came out at
    1.47 ms: a constant declared to be *included* in every figure while being larger than the
    smallest of them. A reader can see that contradiction; the harness could not, because the
    constant was prose and the medians were measurements.

    Both costs are still measured, and since #139 only the narrow one is paid: the engine answers
    this query with three columns like the others, so the wide figure survives here to say what
    the engine used to pay alone rather than to describe the run. Build the lines, split them,
    convert three fields, take the fastest of several passes.
    """
    wide = ["\t".join(("1700000000000000000", "SYM0001", "EX", "B", str(i % 20),
                        str(5_000_000 - i), str(1_000 + i))) for i in range(rows)]
    narrow = ["\t".join(("1700000000000000000", str(5_000_000 - i), str(1_000 + i)))
              for i in range(rows)]

    def timed_parse(lines: list[str], price_at: int, size_at: int) -> float:
        best = float("inf")
        for _ in range(samples):
            started = time.perf_counter()
            out = []
            for line in lines:
                fields = line.split("\t")
                out.append((int(fields[0]), int(fields[price_at]), int(fields[size_at])))
            elapsed = time.perf_counter() - started
            if len(out) != len(lines):          # the loop must not be optimised into nothing
                raise RuntimeError("the parse loop did not produce a row per line")
            best = min(best, elapsed)
        return best

    return timed_parse(wide, 5, 6), timed_parse(narrow, 1, 2)


def in_process_sentence(build_dir: Path, wire_levels_per_second: float | None,
                        levels_per_update: int) -> str:
    """The in-process figure, measured by this run rather than quoted from another machine.

    This sentence used to carry three numbers as literals - 446,219 updates/s in process, 4,012
    over the wire, a factor of 111 - under the words "Measured on this machine". They were measured
    on *a* machine, and then travelled into every report generated anywhere else: the first aarch64
    run printed them inside a header reading "Amazon EC2 m9g.xlarge", where the same benchmark
    measures about 946,000 levels/s.

    Staleness was the smaller half. The two figures were in **different units**:
    `BM_IngestionThroughput` applies one level per call, this harness sends twenty levels per round
    trip, so "updates/s" meant two things a factor of twenty apart and the factor of 111 was mostly
    that. Both halves are levels per second now, and the in-process half comes from
    `BM_IngestionThroughputBatched`, which uses the wire's shape.
    """
    binary = build_dir / "benchmarks" / "bench_engine"
    if not binary.is_file():
        return (f"The in-process figure is not measured in this run: there is no bench_engine in "
                f"{build_dir / 'benchmarks'}. Build it and run again rather than reading a number "
                f"from another machine")

    with tempfile.TemporaryDirectory() as scratch:
        out = Path(scratch) / "inproc.json"
        completed = subprocess.run(
            [str(binary), "--benchmark_filter=BM_IngestionThroughputBatched",
             "--benchmark_format=json", f"--benchmark_out={out}"],
            capture_output=True, text=True, timeout=600, check=False,
            # The engine writes its storage where this run put everything else, so the in-process
            # figure is measured against the same filesystem as the wire figure beside it.
            env={**os.environ, "TMPDIR": str(build_dir / "bench-data")})
        if completed.returncode != 0 or not out.is_file():
            return (f"The in-process figure is not measured in this run: bench_engine exited "
                    f"{completed.returncode}. Its output is not quoted from elsewhere")
        payload = json.loads(out.read_text())

    rows = [b for b in payload.get("benchmarks", [])
            if b.get("real_time") and b.get("cpu_time") and "Batched" in b.get("name", "")
            and b.get("run_type") != "aggregate"]
    if not rows:
        return ("The in-process figure is not measured in this run: bench_engine reported no "
                "timed run of the batched ingestion benchmark")

    # **Not** `items_per_second`, which Google Benchmark computes from **CPU** time - verified on
    # this benchmark: 20 levels / 1336 ns of CPU is the 14.97M/s it prints, where the wall figure
    # is 20 / 20284 ns. The ingest column beside this is wall-clock, so quoting the counter here
    # would compare a CPU rate with a wall rate and call the quotient a factor. That is the defect
    # this whole sentence was rewritten to remove, one unit along.
    real = sorted(r["real_time"] for r in rows)[len(rows) // 2]
    cpu = sorted(r["cpu_time"] for r in rows)[len(rows) // 2]
    iterations = max(r.get("iterations", 0) for r in rows)
    levels_per_second = levels_per_update / (real * 1e-9)
    levels_per_cpu_second = levels_per_update / (cpu * 1e-9)
    storage = payload.get("context", {}).get("engine_storage_fs", "unknown filesystem")

    measured = (
        f"Measured by this run: the engine applies {levels_per_second:,.0f} levels/s in process "
        f"by the wall clock and {levels_per_cpu_second:,.0f} levels/s of CPU (bench_engine "
        f"BM_IngestionThroughputBatched, {levels_per_update} levels per update, "
        f"{iterations:,} iterations, storage on {storage})")
    rows_written = iterations * levels_per_update
    gap = (f" Its wall clock is {real / cpu:.1f} times its CPU time, so that run spent most of its "
           f"wall clock waiting, and it wrote {rows_written:,} rows - a different volume from this "
           f"table's dataset, which the benchmark's registration fixes by setting its own minimum "
           f"time, so the count cannot be varied from the command line.")
    # **No ratio between these two.** They are measured at volumes two orders of magnitude apart
    # and by clients written in different languages, and dividing one by the other would produce
    # exactly the kind of figure this sentence exists to have stopped printing. The comparison that
    # holds the volume and the client is in benchmarks/on-a-bigger-machine.md, and it finds the two
    # within a few per cent of each other - which is a different conclusion from any quotient of
    # the two numbers here.
    if not wire_levels_per_second:
        return measured + "." + gap + " The figure over the wire is in the ingest column above"
    return (f"{measured}; the same twenty-level update over the wire, in the ingest column above, "
            f"is {wire_levels_per_second:,.0f} levels/s. **These two are not divided here**: they "
            f"are at different volumes and behind different clients, and a quotient of them would "
            f"be the same mistake as the factor of 111 this sentence replaced.{gap}")


def timed(call: Callable[[], QueryResult], rounds: int) -> dict:
    """One warm call, then `rounds` samples, reported as a median with the range beside it.

    A median rather than a mean because one scheduler hiccup on this machine moves a mean and not a
    median - the same reason `resolution.measure()` discards one extreme - and the range is published
    rather than summarised away, because a number without its spread cannot be argued with.
    """
    call()
    samples = sorted(call().seconds for _ in range(rounds))
    return {
        "value": samples[len(samples) // 2],
        "unit": "s",
        "min": samples[0],
        "max": samples[-1],
        "samples": samples,
    }


def compare_systems(entries: list[dict], reference_name: str,
                    floor: resolution.Resolution) -> tuple[list[str], list[str], int, float]:
    """Every comparable workload, classified by the one function allowed to say "faster".

    Returns the losses, the wins, how many were indistinguishable, and the largest relative
    difference seen - which is what `verdict_for()` needs to downgrade the verdict when the floor
    swallows everything the run measured.
    """
    reference = next((e for e in entries if e.get("name") == reference_name and e.get("available")),
                     None)
    losses: list[str] = []
    wins: list[str] = []
    ties = 0
    largest = 0.0
    if reference is None:
        return losses, wins, ties, largest

    for entry in entries:
        if entry is reference or not entry.get("available"):
            continue
        for name, theirs in entry.get("workloads", {}).items():
            ours = reference.get("workloads", {}).get(name)
            if not ours or "value" not in ours or "value" not in theirs:
                continue
            if ours["value"] <= 0 or theirs["value"] <= 0:
                continue

            # Ingest is rows per second, so more is better; a workload timed in seconds is the
            # other way round. Getting this backwards would report every loss as a win, which is
            # the one direction nobody double-checks.
            if ours.get("unit") == "rows/s":
                ours_seconds, theirs_seconds = 1.0 / ours["value"], 1.0 / theirs["value"]
            else:
                ours_seconds, theirs_seconds = ours["value"], theirs["value"]

            verdict = resolution.classify(ours_seconds, theirs_seconds, floor)
            difference = (abs(ours_seconds - theirs_seconds) / max(ours_seconds, theirs_seconds))
            largest = max(largest, difference)
            def show(entry: dict) -> str:
                """The same formatting the table uses, because a sentence quoting `0.0088493 s`
                next to a table saying `8.85 ms` reads as two different measurements."""
                if entry.get("unit") == "rows/s":
                    return f"{entry['value']:,.0f} rows/s"
                return f"{entry['value'] * 1000:.2f} ms"

            sentence = (f"{name}: orderbook {show(ours)} against {entry['name']} {show(theirs)} "
                        f"({difference * 100:.1f}% apart, floor {floor.floor * 100:.1f}%)")
            if verdict == resolution.INDISTINGUISHABLE:
                ties += 1
            elif verdict == resolution.FASTER:
                wins.append(sentence)
            else:
                losses.append(sentence)
    return losses, wins, ties, largest


def losses_search(entries: list[dict], reference_name: str, floor: resolution.Resolution,
                  wins: list[str], ties: int) -> str:
    """How a loss was looked for, in the words of what this run actually did.

    Required rather than optional: an empty losses list on its own is a selected table, and a
    sentence that describes the search is what lets a reader decide whether it was a real one.
    """
    compared = [e["name"] for e in entries
                if e.get("available") and e.get("name") != reference_name]
    absent = [f"{e['name']} ({e.get('reason', 'unavailable')})" for e in entries
              if not e.get("available")]
    return (
        f"Every workload was run against every system that answered, and each pair was classified "
        f"by resolution.classify() against this machine's measured floor of {floor.floor:.4f} - "
        f"not by inspection. Compared against: {', '.join(compared) or 'nothing'}. "
        f"{len(wins)} workload(s) came out {resolution.FASTER}, {ties} inside the floor and "
        f"therefore reported as indistinguishable rather than as a win. Not measured: "
        f"{'; '.join(absent) or 'none'}.")


# Properties of the measurement, not of any system in it. Kept apart from ENGINE_LIMITATIONS
# because a constant every system pays is not something the engine cannot do.
MEASUREMENT_NOTES = [
    "{parsing}",
    "each adapter holds one connection open for every timed request. That is not a courtesy: "
    "measured on {prose_machine}, a fresh `clickhouse-client` costs 80 ms and a fresh `psql` "
    "40-60 ms, against queries of a few milliseconds - and the first version of this harness "
    "charged both of those to the competitor",
    "the noise floor is measured inside the run it governs, by timing the reference system's own "
    "query twice per round. Across runs the same workload varies more than that floor: on "
    "{prose_machine}, two consecutive runs of this table gave 9.69 ms and 10.97 ms for the same "
    "query, which is why a comparison is only made between numbers from one run",
]


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--symbols", type=int, default=50)
    parser.add_argument("--levels", type=int, default=20)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--build-dir", type=Path, default=REPO / "build-release")
    parser.add_argument("--results", type=Path, default=Path(__file__).parent / "results")
    # Where the engine keeps its WAL and segments. Defaults under the build directory rather than
    # /tmp, because /tmp is a tmpfs on a good share of machines and the servers this is compared
    # against always write to a disk. hardware.require_durable_storage() refuses the rest.
    parser.add_argument("--data-dir", type=Path, default=None,
                        help="engine storage location (default: <build-dir>/bench-data)")
    args = parser.parse_args(argv)
    data_dir = args.data_dir or (args.build_dir / "bench-data")
    data_dir.mkdir(parents=True, exist_ok=True)

    # Refuse before doing any work: a two-hour run that turns out to have measured Debug is worse
    # than a message.
    build_type = hardware.require_release(args.build_dir)
    engine_fs = hardware.require_durable_storage(data_dir)
    hw = hardware.describe(data_dir, args.build_dir)
    print(f"Platform: {hw.platform}")
    clock = f"{hw.mhz:.0f} MHz ({hw.clock_source})" if hw.mhz else hw.clock_source
    print(f"Hardware: {hw.cpu_model}, {hw.cores} cores, {hw.ram_mib} MiB, {hw.filesystem}, "
          f"clock {clock}, kernel {hw.kernel} (digest {hw.digest()})")
    print(f"Build: {build_type} from {args.build_dir}")
    print(f"Engine storage: {data_dir} on {engine_fs}")

    csv_path = args.results / f"dataset-{args.rows}-{args.seed}.csv"
    manifest = dataset.generate(csv_path, rows=args.rows, symbols=args.symbols,
                               levels=args.levels, seed=args.seed)
    print(f"Dataset: {manifest.rows} rows, {manifest.symbols} symbols, sha256 "
          f"{manifest.sha256[:16]}…")

    # Ours first, and that position is load-bearing: it is the reference every other system's rows
    # are compared against, and the control the noise floor is measured on.
    systems = [
        OrderbookSystem(args.build_dir / "ob_tcp_server", free_port(), data_dir),
        ClickHouseSystem(),
        TimescaleDbSystem(),
        KdbSystem(),
    ]

    span_start = manifest.start_ns
    span_end = manifest.start_ns + manifest.rows * manifest.interval_ns
    # One symbol, because the engine's `SELECT` addresses a single book and the comparison has to
    # ask all four systems the same question.
    vwap_symbol = "SYM0000"

    # Which columns of a workload's rows every system can be held to, when any of them has to be
    # dropped. **Empty since #105**, and that is the point of keeping the mechanism: `time_range`
    # used to drop the timestamp because the engine could not be given event time at all, so its
    # rows carried arrival time and only price and size could be compared. The field exists now, all
    # four systems answer the same question, and every column of it is held to the same value.
    #
    # The dictionary stays because the next incomparable column should be declared here with its
    # reason rather than arranged by an adapter quietly returning fewer columns - and a test below
    # refuses to publish a limitation row for a limitation nobody declared.
    COMPARABLE_COLUMNS: dict[str, tuple[tuple[int, ...], str]] = {}

    def projected(workload: str, rows: list[tuple]) -> list[tuple]:
        keep = COMPARABLE_COLUMNS.get(workload)
        if keep is None:
            return rows
        columns, _ = keep
        return [tuple(row[i] for i in columns) for row in rows]

    def workload_calls(system) -> dict[str, Callable[[], QueryResult]]:
        return {
            "time_range": lambda: system.query_time_range(span_start, span_end),
            "vwap": lambda: system.query_vwap(vwap_symbol, span_end),
        }

    entries: list[dict] = []
    reference_rows: dict[str, list[tuple]] = {}
    reference_name = systems[0].name
    measured = 0
    try:
        for system in systems:
            ok, why = system.available()
            if not ok:
                print(f"{system.name}: NOT MEASURED ({why})")
                entries.append({"name": system.name, "available": False, "reason": why})
                continue

            try:
                tuning = require_tuning(system)
            except NoTuningDeclared as exc:
                print(f"{system.name}: refused — {exc}")
                entries.append({"name": system.name, "available": False, "reason": str(exc)})
                continue

            load = system.load(csv_path)
            rows_per_second = load.rows_loaded / load.seconds if load.seconds > 0 else 0.0
            print(f"{system.name}: loaded {load.rows_loaded} rows in {load.seconds:.2f}s "
                  f"({rows_per_second:,.0f} rows/s)")

            workloads: dict[str, dict] = {
                "ingest": {"value": rows_per_second, "unit": "rows/s",
                           "rows_loaded": load.rows_loaded, "seconds": load.seconds},
            }

            # Rows first, then time. A workload is run once for what it returns, compared against
            # the reference, and only timed if the two systems answered the same question - because
            # two different queries time just as cleanly as two equivalent ones and the faster one
            # wins.
            for name, call in workload_calls(system).items():
                try:
                    first = call()
                except Exception as exc:                       # noqa: BLE001 - reported, not raised
                    workloads[name] = {"note": f"FAILED: {exc}"}
                    print(f"{system.name}: {name} failed — {exc}")
                    continue

                if system.name == reference_name:
                    reference_rows[name] = first.rows
                else:
                    try:
                        equivalence.require_equivalent(
                            reference_name, projected(name, reference_rows.get(name, [])),
                            system.name, projected(name, first.rows))
                    except equivalence.EquivalenceError as exc:
                        excluded = COMPARABLE_COLUMNS.get(name)
                        why = f"; {excluded[1]}" if excluded else ""
                        workloads[name] = {"note": f"NOT COMPARABLE: {exc}{why}"}
                        print(f"{system.name}: {name} not comparable — {exc}")
                        continue

                workloads[name] = timed(call, args.rounds) | {"rows": len(first.rows)}
                print(f"{system.name}: {name} {workloads[name]['value'] * 1000:.3f} ms median "
                      f"over {args.rounds} rounds ({len(first.rows)} rows)")

            measured += 1
            entries.append({
                "name": system.name,
                "available": True,
                "version": system.version(),
                "tuning_applied": tuning,
                "config": system.config_dump(),
                "workloads": workloads,
            })

        # The floor, measured by **running a real workload twice per round** on the reference
        # system, interleaved. Done after the loads so the page cache is in the state the
        # comparison saw.
        #
        # The first version of this passed a constant to `measure()`, which returns a floor of
        # exactly 0.0 - so every difference would have cleared it and `classify()` would have
        # called noise a win. A mechanism that produces a number without measuring anything is the
        # failure this whole module exists to prevent, and I put it in the glue rather than in the
        # module. The sampler has to be work.
        if measured == 0:
            print("No system produced a measurement, so there is no floor to measure against.")
            return 1
        reference = systems[0]

        def control_sample() -> float:
            return reference.query_time_range(span_start, span_end).seconds

        floor = resolution.measure(control_sample, rounds=args.rounds)
        print(f"Resolution: {floor.note}")

        # The comparison, and this is the first run in which it can happen: `classify()` and
        # `verdict_for()` were written with part one and had no caller outside the tests, because
        # one system cannot be compared with anything. Same shape as roadmap #104 - a mechanism
        # whose reachability arrives with the case it was written for.
        losses, wins, ties, largest = compare_systems(entries, reference_name, floor)
        floor = resolution.verdict_for(floor, largest)
        print(f"Comparison: {len(wins)} {resolution.FASTER}, {len(losses)} {resolution.SLOWER}, "
              f"{ties} inside the floor (largest difference {largest:.4f})")
    finally:
        for system in systems:
            system.teardown()

    # After every server is down, so the in-process figure is measured on a quiet machine, and from
    # this build rather than from prose. `entries` carries the wire figure this is put beside.
    wire_levels = next(
        (e["workloads"]["ingest"]["value"] for e in entries
         if e.get("name") == reference_name and isinstance(e.get("workloads"), dict)
         and isinstance(e["workloads"].get("ingest"), dict)
         and e["workloads"]["ingest"].get("value")),
        None)
    # The row count the time-range query actually returned for the reference system, so the parse
    # measurement below is over the same number of lines the table's query column carried. A
    # constant here would be the same mistake one size smaller.
    query_rows = next(
        (e["workloads"]["time_range"]["rows"] for e in entries
         if e.get("name") == reference_name and isinstance(e.get("workloads"), dict)
         and isinstance(e["workloads"].get("time_range"), dict)
         and e["workloads"]["time_range"].get("rows")),
        0)
    wide_s, narrow_s = parse_cost(rows=query_rows) if query_rows else (0.0, 0.0)
    fills = {
        "in_process": in_process_sentence(args.build_dir, wire_levels, args.levels),
        "parsing": ("the query column's Python-side parsing cost is not measured in this run: "
                    "the time-range query returned no rows")
        if not query_rows else (
            f"every figure in the query column includes a measured {narrow_s * 1000:.3f} ms of "
            f"Python-side parsing for {query_rows} three-column rows, and since #139 that really "
            f"is the same work for all three systems - the engine used to answer this query with "
            f"seven columns and pay {wide_s * 1000:.3f} ms for it, which made the constant ours "
            f"alone. What separates the systems is what is left after it, stated here rather than "
            f"subtracted from the table"),
        "prose_machine": PROSE_FIGURES_MACHINE,
    }

    def fill(entry: str) -> str:
        return entry.format(**fills) if "{" in entry else entry

    limitations = [fill(entry) for entry in ENGINE_LIMITATIONS]
    measurement_notes = [fill(note) for note in MEASUREMENT_NOTES]

    document = report.Report(
        run={"timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
             "seed": args.seed, "rounds": args.rounds, "build_type": build_type},
        hardware=hw.__dict__ | {"digest": hw.digest()},
        dataset=manifest.as_dict(),
        resolution=floor.as_dict(),
        systems=entries,
        limitations=limitations,
        measurement_notes=measurement_notes,
        losses=losses,
        losses_search=losses_search(entries, reference_name, floor, wins, ties),
    )
    path = report.write(document, args.results, hw.digest())
    print(f"Wrote {path} and {path.with_suffix('.md')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
