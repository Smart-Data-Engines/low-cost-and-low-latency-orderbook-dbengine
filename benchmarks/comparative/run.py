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
from pathlib import Path

from . import dataset, hardware, report, resolution
from .systems.base import NoTuningDeclared, require_tuning
from .systems.orderbook import OrderbookSystem

REPO = Path(__file__).resolve().parents[2]

# What this engine cannot do, stated because the comparison is uneven in its favour. A specialised
# engine beating general databases at its one workload is its whole thesis - and a number that does
# not say what it gives up promises a replacement.
ENGINE_LIMITATIONS = [
    "orderbook: no general-purpose SQL - a fixed set of commands, not a query language",
    "orderbook: no joins, and no cross-symbol queries",
    "orderbook: the schema is imposed, not derived from a model",
    "orderbook: append-only; nothing deletes rows except TTL retention",
    "orderbook: aggregates run over the live book, so a VWAP over a historical time range is not "
    "the same question the SQL equivalents answer",
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
    args = parser.parse_args(argv)

    # Refuse before doing any work: a two-hour run that turns out to have measured Debug is worse
    # than a message.
    build_type = hardware.require_release(args.build_dir)
    hw = hardware.describe(args.build_dir)
    print(f"Hardware: {hw.cpu_model}, {hw.cores} cores, {hw.ram_mib} MiB, {hw.filesystem}, "
          f"kernel {hw.kernel} (digest {hw.digest()})")
    print(f"Build: {build_type} from {args.build_dir}")

    csv_path = args.results / f"dataset-{args.rows}-{args.seed}.csv"
    manifest = dataset.generate(csv_path, rows=args.rows, symbols=args.symbols,
                               levels=args.levels, seed=args.seed)
    print(f"Dataset: {manifest.rows} rows, {manifest.symbols} symbols, sha256 "
          f"{manifest.sha256[:16]}…")

    systems = [OrderbookSystem(args.build_dir / "ob_tcp_server", free_port())]
    # ClickHouse, TimescaleDB and kdb+ are added here as their adapters land (tasks 6.2-6.4). Until
    # then the run is honest about being one system wide rather than pretending otherwise.

    entries: list[dict] = []
    reference_load: float | None = None
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
            if reference_load is None:
                reference_load = load.seconds
            print(f"{system.name}: loaded {load.rows_loaded} rows in {load.seconds:.2f}s "
                  f"({rows_per_second:,.0f} rows/s)")

            entries.append({
                "name": system.name,
                "available": True,
                "version": system.version(),
                "tuning_applied": tuning,
                "config": system.config_dump(),
                "workloads": {"ingest": {"value": rows_per_second, "unit": "rows/s"}},
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
        if reference_load is None:
            print("No system produced a measurement, so there is no floor to measure against.")
            return 1
        reference = systems[0]
        span_start = manifest.start_ns
        span_end = manifest.start_ns + manifest.rows * manifest.interval_ns

        def control_sample() -> float:
            return reference.query_time_range(span_start, span_end).seconds

        floor = resolution.measure(control_sample, rounds=args.rounds)
        print(f"Resolution: {floor.note}")
    finally:
        for system in systems:
            system.teardown()

    document = report.Report(
        run={"timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
             "seed": args.seed, "rounds": args.rounds, "build_type": build_type},
        hardware=hw.__dict__ | {"digest": hw.digest()},
        dataset=manifest.as_dict(),
        resolution=floor.as_dict(),
        systems=entries,
        limitations=ENGINE_LIMITATIONS,
        losses=[],
        losses_search=(
            "Only one system was measured in this run, so no comparison was possible and no loss "
            "could be found. This sentence is required rather than optional: an empty losses list "
            "with nothing beside it is a selected table."),
    )
    path = report.write(document, args.results, hw.digest())
    print(f"Wrote {path} and {path.with_suffix('.md')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
