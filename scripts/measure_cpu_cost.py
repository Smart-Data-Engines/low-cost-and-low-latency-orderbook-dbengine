#!/usr/bin/env python3
"""CPU-seconds each system burns to ingest the same rows, beside the wall clock it took.

Wall clock says who finishes first. CPU time says what it cost, and this engine's name contains a
claim about cost. The distinction is not academic on a small box: the engine runs a fixed set of
threads with fixed roles - `grep -rn 'hardware_concurrency\\|_SC_NPROCESSORS' src include` returns
nothing - while ClickHouse sizes itself to the cores it can see and `timescaledb-tune` sizes
PostgreSQL to the machine. A wall-clock comparison there conflates "faster per core" with "uses
more cores".

Three things about the arithmetic, each of which was wrong in the first version of this script and
each of which moved a published column:

**Server CPU includes reaped children** (`cutime`/`cstime`, fields 16 and 17 of `/proc/pid/stat`).
PostgreSQL forks a backend per connection, and that backend can exit before the closing sample is
taken; without the children's time its CPU is lost, and lost in the direction that flatters it.
ClickHouse and this engine are threaded single processes, where the children's fields are zero and
including them costs nothing.

**Client CPU includes children** (`RUSAGE_CHILDREN`). Checked rather than assumed, because the
three adapters do not agree: this engine's and ClickHouse's are in-process Python (a socket and
`http.client`), and the PostgreSQL one drives a `psql` subprocess. `getrusage(RUSAGE_SELF)` excludes
subprocesses, so TimescaleDB's client column read as 0.003 s - its client's cost, wherever it went,
was not in it. The first version of this docstring claimed ClickHouse shelled out too; it does not,
and its 0.043 s was always real.

**`comm` is truncated to fifteen characters**, so `clickhouse-server` appears as `clickhouse-serv`.
Matching on the untruncated name silently matches nothing, and nothing looks like zero CPU.

It does not constrain anyone's parallelism: what is measured is what each system chose to do with
the machine, which is the honest comparison and is why the "server cores" column is reported.

    scripts/measure_cpu_cost.py --rows 2000000

Needs the competitors installed natively and running (benchmarks/install_competitors.md) and a
Release build. Prints one row per system; a system that is not available says so rather than being
omitted, because an absent row reads as a system nobody tried.
"""
from __future__ import annotations

import argparse
import os
import resource
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from benchmarks.comparative import dataset  # noqa: E402
from benchmarks.comparative.systems.clickhouse import ClickHouseSystem  # noqa: E402
from benchmarks.comparative.systems.orderbook import OrderbookSystem  # noqa: E402
from benchmarks.comparative.systems.timescaledb import TimescaleDbSystem  # noqa: E402

CLK = 100.0  # USER_HZ; the kernel reports these fields in clock ticks.

# `comm` is truncated to TASK_COMM_LEN-1 = 15 characters. These are the truncated forms, which is
# what /proc actually holds - matching the full name matches nothing and reads as zero CPU.
COMMS = {
    "orderbook": {"ob_tcp_server"},
    "clickhouse": {"clickhouse-serv"},
    "timescaledb": {"postgres"},
}


def server_cpu(comms: set[str]) -> float:
    """utime+stime+cutime+cstime over every live process whose truncated comm matches.

    The children's fields are what make this correct for PostgreSQL, which forks a backend per
    connection; they are zero for the two threaded servers.
    """
    total = 0.0
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().split()
        except OSError:
            continue  # the process exited between listing and reading, which is normal
        if len(fields) < 17 or fields[1].strip("()") not in comms:
            continue
        try:
            total += sum(int(fields[i]) for i in (13, 14, 15, 16)) / CLK
        except ValueError:
            continue
    return total


def live_descendants(pid: int) -> list[int]:
    """Every process below this one, found by walking `/proc/<pid>/task/*/children`."""
    found: list[int] = []
    stack = [pid]
    while stack:
        parent = stack.pop()
        for task in Path(f"/proc/{parent}/task").glob("*/children"):
            try:
                kids = [int(x) for x in task.read_text().split()]
            except (OSError, ValueError):
                continue
            found.extend(kids)
            stack.extend(kids)
    return found


def client_cpu() -> float:
    """This process, its reaped children, **and** its children that are still running.

    `RUSAGE_CHILDREN` only accounts for children that have been **waited for**, and the PostgreSQL
    adapter keeps one `psql` alive for the whole load - so the first version of this reported its
    client cost as 0.003 s, which is the harness's own Python and none of the client. The live
    descendants are read from `/proc` the same way the servers are.
    """
    me = resource.getrusage(resource.RUSAGE_SELF)
    reaped = resource.getrusage(resource.RUSAGE_CHILDREN)
    total = me.ru_utime + me.ru_stime + reaped.ru_utime + reaped.ru_stime
    for pid in live_descendants(os.getpid()):
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().split()
        except OSError:
            continue
        if len(fields) >= 17:
            try:
                total += sum(int(fields[i]) for i in (13, 14, 15, 16)) / CLK
            except ValueError:
                continue
    return total


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--symbols", type=int, default=50)
    ap.add_argument("--levels", type=int, default=20)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--build-dir", type=Path, default=REPO / "build-release")
    ap.add_argument("--data-dir", type=Path, default=None,
                    help="where the engine's own storage goes. Defaults beside the build, because "
                         "/tmp is a tmpfs on a good share of machines and timing one system "
                         "against RAM while the others write to disk is not a comparison.")
    args = ap.parse_args()

    from benchmarks.comparative import hardware
    data_dir = args.data_dir or (args.build_dir / "bench-data")
    data_dir.mkdir(parents=True, exist_ok=True)
    engine_fs = hardware.require_durable_storage(data_dir)

    results = REPO / "benchmarks" / "comparative" / "results"
    csv_path = results / f"dataset-{args.rows}-{args.seed}.csv"
    manifest = dataset.generate(csv_path, rows=args.rows, symbols=args.symbols,
                                levels=args.levels, seed=args.seed)
    print(f"dataset: {manifest.rows:,} rows, one level each, in "
          f"{manifest.rows // args.levels:,} updates of {args.levels} levels")
    print(f"engine storage: {data_dir} on {engine_fs}\n")

    cases = [
        ("orderbook", OrderbookSystem(args.build_dir / "ob_tcp_server", 19311, data_dir)),
        ("clickhouse", ClickHouseSystem()),
        ("timescaledb", TimescaleDbSystem()),
    ]
    header = ("system", "wall s", "levels/s", "srv CPU s", "cli CPU s", "levels/srv-CPU-s",
              "srv cores")
    print("{:13}{:>9}{:>13}{:>12}{:>12}{:>18}{:>12}".format(*header))
    try:
        for name, system in cases:
            ok, note = system.available()
            if not ok:
                print(f"{name:13}unavailable: {note[:70]}")
                continue
            srv_before, cli_before = server_cpu(COMMS[name]), client_cpu()
            loaded = system.load(csv_path)
            srv_cpu = server_cpu(COMMS[name]) - srv_before
            cli = client_cpu() - cli_before
            per_cpu = manifest.rows / srv_cpu if srv_cpu > 0 else float("nan")
            print("{:13}{:9.3f}{:13,.0f}{:12.3f}{:12.3f}{:18,.0f}{:12.2f}".format(
                name, loaded.seconds, manifest.rows / loaded.seconds, srv_cpu, cli, per_cpu,
                srv_cpu / loaded.seconds))
    finally:
        # `run.py` has had this since it was written and this script did not, so a run that raised
        # - mine did, on an import - left a node holding a port and a data directory with nobody
        # to stop it. Measured: one survived for an hour. Teardown is idempotent and safe on a
        # system that never started.
        for _, system in cases:
            system.teardown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
