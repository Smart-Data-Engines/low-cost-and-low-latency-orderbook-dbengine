#!/usr/bin/env python3
"""Pipelined ingest over the wire, compared across server builds (roadmap #146).

Runs `benchmarks/pipelined_ingest` against a fresh node per run, for every build named with
`--server NAME=PATH`, in rounds whose order rotates — so every build runs first, in the middle and
last, and a warm-cache or turbo effect that favours a position cannot read as a difference between
builds. Prints one JSON line per run and a table of medians.

With `--count-syscalls` each run is also counted by the kernel: `perf stat` on the
`sys_enter_sendto` and `sys_enter_read` tracepoints, divided by the number of batches. That is how
#146 was measured — one send per batch where there had been 64 — and it needs `sudo` and `perf`.
The counts are process-wide, so they include the io loop's own descriptors as well as the clients'
sockets: the read of the subscription eventfd on every pass of the loop is one per batch in every
build.

    scripts/measure_pipelined_ingest.py \\
        --server before=../before/build-release/ob_tcp_server \\
        --server after=build-release/ob_tcp_server \\
        --probe build-release/benchmarks/pipelined_ingest --rounds 5 --connections 1 \\
        --count-syscalls

One binary under several configurations is several `--server` entries with the same path and a
`--flags NAME=...` each, which is how the multi-reactor stage compared `--io-threads 1, 2, 4`:

    scripts/measure_pipelined_ingest.py \\
        --server r1=build-release/ob_tcp_server --flags "r1=--io-threads 1" \\
        --server r2=build-release/ob_tcp_server --flags "r2=--io-threads 2" \\
        --server-cpus 0-1 --probe-cpus 2-3 --connections 2

`--server-cpus` and `--probe-cpus` pin the two with `taskset`, so a client that needs a core does
not take it from the server it is measuring - on a four-core machine, the difference between a
reactor that has a core and one that shares it with the load generator.

Two refusals before any run, both borrowed from the comparative harness rather than restated
(`benchmarks/comparative/hardware.py`): a server whose build directory is not Release, and a data
directory on memory. The second is not hypothetical here. This script's first version put each
node's data under `tempfile`'s default, which is `/tmp`, and on the m9g.xlarge it was written for
`/tmp` is a **tmpfs**: the same three builds read 3.46, 5.26 and 5.57 million levels a second there
against 3.14, 4.59 and 4.85 on the instance's disk - ten to fifteen per cent of every figure was the
WAL going to memory (pitfall 326, met again by a script written a week after it was recorded).
"""
from __future__ import annotations

import argparse
import json
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks" / "comparative"))
import hardware  # noqa: E402  - one definition of "Release" and of "durable", shared

EVENTS = ("syscalls:sys_enter_sendto", "syscalls:sys_enter_read")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def loadavg() -> str:
    return " ".join(Path("/proc/loadavg").read_text().split()[:3])


def pinned(cpus: str | None, argv: list[str]) -> list[str]:
    return ["taskset", "-c", cpus, *argv] if cpus else argv


def start_node(server: Path, data_dir: Path, flags: list[str],
               cpus: str | None) -> tuple[subprocess.Popen, int]:
    port = free_port()
    log = open(data_dir / "node.log", "wb")
    proc = subprocess.Popen(
        pinned(cpus, [str(server), "--port", str(port), "--metrics-port", str(free_port()),
                      "--data-dir", str(data_dir / "data"), "--drain-timeout-ms", "500",
                      *flags]),
        stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise SystemExit(f"node exited: {(data_dir / 'node.log').read_text()[-400:]}")
        if b"listening on port" in (data_dir / "node.log").read_bytes():
            return proc, port
        time.sleep(0.05)
    proc.kill()
    raise SystemExit("node did not start within 15 s")


def perf_counts(path: Path) -> dict[str, int]:
    counts = {}
    for line in path.read_text().splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[1] in EVENTS:
            counts[fields[1].split("sys_enter_")[1]] = int(fields[0].replace(",", ""))
    missing = [e for e in EVENTS if e.split("sys_enter_")[1] not in counts]
    if missing:
        raise SystemExit(f"perf did not count {missing}: {path.read_text()[-400:]}")
    return counts


def one_run(name: str, server: Path, args: argparse.Namespace) -> dict:
    flags = args.flags_by_name.get(name, [])
    with tempfile.TemporaryDirectory(prefix="pipelined-ingest-", dir=args.data_root) as tmp:
        node, port = start_node(server, Path(tmp), flags, args.server_cpus)
        perf = None
        try:
            if args.count_syscalls:
                perf = subprocess.Popen(
                    ["sudo", "perf", "stat", "-e", ",".join(EVENTS), "-p", str(node.pid),
                     "-o", str(Path(tmp) / "perf.txt")],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(0.3)
            out = subprocess.run(
                pinned(args.probe_cpus,
                       [str(args.probe), str(port), str(node.pid), str(args.connections),
                        str(args.batches // args.connections), str(args.levels),
                        str(args.batch)]),
                capture_output=True, text=True)
            if out.returncode != 0:
                raise SystemExit(f"probe failed ({out.returncode}) against {name}: "
                                 f"{out.stderr.strip()}")
            row = {"server": name, "flags": flags, "loadavg": loadavg(),
                   "probe": json.loads(out.stdout)}
            if perf is not None:
                subprocess.run(["sudo", "kill", "-INT", str(perf.pid)], check=False)
                perf.wait(timeout=30)
                batches = (args.batches // args.connections) * args.connections
                row["per_batch"] = {k: v / batches
                                    for k, v in perf_counts(Path(tmp) / "perf.txt").items()}
            return row
        finally:
            node.send_signal(signal.SIGTERM)
            try:
                node.wait(timeout=10)
            except subprocess.TimeoutExpired:
                node.kill()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--server", action="append", required=True, metavar="NAME=PATH",
                    help="a build to measure; repeat it to compare builds")
    ap.add_argument("--probe", type=Path,
                    default=REPO / "build-release" / "benchmarks" / "pipelined_ingest")
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--connections", type=int, default=1)
    ap.add_argument("--batches", type=int, default=12000, help="in total, across connections")
    ap.add_argument("--levels", type=int, default=20)
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--count-syscalls", action="store_true")
    ap.add_argument("--flags", action="append", default=[], metavar="NAME=FLAGS",
                    help="server flags for the --server of that name, space-separated")
    ap.add_argument("--server-cpus", help="taskset list for the server, e.g. 0-1")
    ap.add_argument("--probe-cpus", help="taskset list for the probe, e.g. 2-3")
    ap.add_argument("--data-root", type=Path, default=REPO / "build-release" / "bench-data",
                    help="where each run's node keeps its data; refused if it is memory")
    args = ap.parse_args()
    args.data_root.mkdir(parents=True, exist_ok=True)
    try:
        fstype = hardware.require_durable_storage(args.data_root)
    except hardware.VolatileStorage:
        raise SystemExit(
            f"{args.data_root} is memory, so a node's WAL would cost nothing to write there - "
            f"which was 10-15% of every figure on the machine this was written for. Pass "
            f"--data-root on the disk the engine would use.") from None

    args.flags_by_name = {}
    for spec in args.flags:
        name, sep, flags = spec.partition("=")
        if not sep or not name:
            raise SystemExit(f"--flags wants NAME=FLAGS, got {spec!r}")
        args.flags_by_name[name] = flags.split()

    servers = []
    for spec in args.server:
        name, sep, path = spec.partition("=")
        if not sep or not name or not Path(path).is_file():
            raise SystemExit(f"--server wants NAME=PATH to a binary, got {spec!r}")
        try:
            hardware.require_release(Path(path).resolve().parent)
        except hardware.NotReleaseBuild as refusal:
            raise SystemExit(f"--server {name}: {refusal}") from None
        servers.append((name, Path(path)))
    unknown = set(args.flags_by_name) - {name for name, _ in servers}
    if unknown:
        raise SystemExit(f"--flags names no --server: {sorted(unknown)}")

    hw = hardware.describe(args.data_root, Path(servers[0][1]).resolve().parent)
    print(f"storage: {args.data_root} on {fstype}, {hw.disk_model} ({hw.disk_stack}); "
          f"{hw.cpu_model}, {hw.cores} cores", file=sys.stderr)

    print(f"loadavg at start: {loadavg()}", file=sys.stderr)
    runs: dict[str, list[dict]] = {name: [] for name, _ in servers}
    for r in range(args.rounds):
        for k in range(len(servers)):
            name, path = servers[(r + k) % len(servers)]
            row = one_run(name, path, args)
            row["round"] = r + 1
            runs[name].append(row)
            print(json.dumps(row), flush=True)
    print(f"loadavg at end: {loadavg()}", file=sys.stderr)

    counted = args.count_syscalls
    pins = (f"; server on CPUs {args.server_cpus or 'any'}, probe on {args.probe_cpus or 'any'}"
            if args.server_cpus or args.probe_cpus else "")
    print(f"\n{args.connections} connection(s), batches of {args.batch} x {args.levels}-level "
          f"MINSERT, medians of {args.rounds} rounds{pins}\n")
    head = "| build | levels/s | range | batch p50 | batch p99 | server CPU |"
    rule = "|---|---|---|---|---|---|"
    if counted:
        head += " sends per batch | reads per batch |"
        rule += "---|---|"
    print(head)
    print(rule)
    for name, rows in runs.items():
        rate = [x["probe"]["levels_per_s"] for x in rows]
        line = (f"| {name} | {statistics.median(rate):,.0f} | {min(rate):,.0f}-{max(rate):,.0f} "
                f"| {statistics.median(x['probe']['batch_p50_us'] for x in rows):.1f} µs "
                f"| {statistics.median(x['probe']['batch_p99_us'] for x in rows):.1f} µs "
                f"| {statistics.median(x['probe']['server_cpu_s'] for x in rows):.2f} s |")
        if counted:
            line += (f" {statistics.median(x['per_batch']['sendto'] for x in rows):.2f} "
                     f"| {statistics.median(x['per_batch']['read'] for x in rows):.2f} |")
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
