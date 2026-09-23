#!/usr/bin/env python3
"""What the default profile should be on a machine of N CPUs (stage 4 of roadmap #151).

`boost` sizes the client event loops and the spin window from the CPUs the process can use
(`detect_machine()`, stage 3), and requirement 4.1 says the rule comes from a measurement rather
than from a paragraph. This is that measurement: every **placement** (where the server may run and
how much CPU time it gets) against every **configuration** (server flags) against every
**workload**, a fresh node per run, in rounds whose order rotates.

A placement is `NAME:KEY=VALUE,...` with any of

* `server=CPUS` - the server under `taskset -c CPUS`;
* `probe=CPUS` - the load generator under `taskset -c CPUS`, which may be the same CPUs as the
  server's: that is the case of a client sharing the machine's only core with the engine;
* `quota=PCT` - the server in a transient systemd scope with `CPUQuota=PCT`, which is what a
  container's CPU limit is. The run reads the scope's `cpu.stat` before and after and reports how
  often the kernel throttled it, because CPU time spent spinning is CPU time the quota no longer
  has, and a throttled cgroup stops **every** thread in it until the next period.

A workload is one of

* `ping` - `benchmarks/command_latency`, one connection, `PING` round trips: what the spin buys;
* `writes:C` and `reads:C` - `benchmarks/pipelined_ingest` over C connections, batches of
  20-level `MINSERT`s or of `BOOK` reads: what the loops buy;
* `ping+writes:C` - a `PING` probe while C connections pipeline writes into the same node, which
  is where a spinning loop and the work it competes with meet. The writer is stopped when the
  probe ends, so only the `PING` is reported.

    scripts/measure_profile_rule.py --server build-release/ob_tcp_server \\
        --pipelined build-release/benchmarks/pipelined_ingest \\
        --latency build-release/benchmarks/command_latency \\
        --placement "shared1:server=3,probe=3" --placement "quota1:quota=100%" \\
        --config "eco=--io-threads 1 --io-spin-us 0" --config "spin=--io-threads 1 --io-spin-us 50" \\
        --workload ping --workload ping+writes:1 --rounds 3

Two refusals before any run, the same two the ingest script makes (`benchmarks/comparative/
hardware.py`): a server whose build directory is not Release, and a data directory on memory. And
two after the node starts, because a wrapper that did not do what it was asked measures a
different placement under this one's name: the process behind the PID must be the server (so
`taskset` and `systemd-run` exec'd rather than forked), and under a quota the scope's `cpu.max`
must say the quota that was asked for.
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

CGROUP_ROOT = Path("/sys/fs/cgroup")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def loadavg() -> str:
    return " ".join(Path("/proc/loadavg").read_text().split()[:3])


def parse_placement(spec: str) -> tuple[str, dict[str, str]]:
    name, sep, rest = spec.partition(":")
    if not name:
        raise SystemExit(f"--placement wants NAME:KEY=VALUE,..., got {spec!r}")
    keys: dict[str, str] = {}
    for part in filter(None, rest.split(",") if sep else []):
        key, eq, value = part.partition("=")
        if not eq or key not in ("server", "probe", "quota"):
            raise SystemExit(f"--placement {name}: unknown or empty part {part!r}")
        keys[key] = value
    return name, keys


def parse_workload(spec: str) -> tuple[str, int]:
    kind, _, count = spec.partition(":")
    if kind == "ping" and not count:
        return kind, 1
    if kind in ("writes", "reads", "ping+writes") and count.isdigit() and int(count) > 0:
        return kind, int(count)
    raise SystemExit(f"--workload wants ping, writes:C, reads:C or ping+writes:C, got {spec!r}")


def pinned(cpus: str | None, argv: list[str]) -> list[str]:
    return ["taskset", "-c", cpus, *argv] if cpus else argv


def scope_of(pid: int) -> Path:
    for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines():
        if line.startswith("0::"):
            return CGROUP_ROOT / line[3:].lstrip("/")
    raise SystemExit(f"pid {pid} has no cgroup v2 line in /proc/{pid}/cgroup")


def cpu_stat(scope: Path) -> dict[str, int]:
    stat = {}
    for line in (scope / "cpu.stat").read_text().splitlines():
        key, _, value = line.partition(" ")
        stat[key] = int(value)
    return stat


def start_node(server: Path, data_dir: Path, flags: list[str],
               place: dict[str, str]) -> tuple[subprocess.Popen, int]:
    port = free_port()
    argv = [str(server), "--port", str(port), "--metrics-port", str(free_port()),
            "--data-dir", str(data_dir / "data"), "--drain-timeout-ms", "500", *flags]
    if "quota" in place:
        argv = ["systemd-run", "--user", "--scope", "--quiet", "-p", f"CPUQuota={place['quota']}",
                "--", *argv]
    log = open(data_dir / "node.log", "wb")
    proc = subprocess.Popen(pinned(place.get("server"), argv), stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise SystemExit(f"node exited: {(data_dir / 'node.log').read_text()[-400:]}")
        if b"listening on port" in (data_dir / "node.log").read_bytes():
            break
        time.sleep(0.05)
    else:
        proc.kill()
        raise SystemExit("node did not start within 15 s")
    # The executable, not the name: the server's main thread runs the first event loop and is
    # called `ob-io-0`, so /proc/PID/comm does not say which program is behind the PID.
    exe = Path(f"/proc/{proc.pid}/exe").resolve()
    if exe != server.resolve():
        proc.kill()
        raise SystemExit(f"pid {proc.pid} runs {exe}, not {server.resolve()}: a wrapper forked "
                         f"instead of exec'ing, and every number would be about the wrapper")
    if "quota" in place:
        want = float(place["quota"].rstrip("%")) / 100.0
        quota, period = (scope_of(proc.pid) / "cpu.max").read_text().split()
        got = float(quota) / float(period) if quota != "max" else None
        if got is None or abs(got - want) > 1e-6:
            proc.kill()
            raise SystemExit(f"asked for CPUQuota={place['quota']}, the scope says {quota} {period}")
    return proc, port


def run_probe(argv: list[str], what: str) -> dict:
    out = subprocess.run(argv, capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(f"{what} failed ({out.returncode}): {out.stderr.strip()[-400:]}")
    return json.loads(out.stdout)


def one_run(args: argparse.Namespace, pname: str, place: dict[str, str], cname: str,
            flags: list[str], workload: tuple[str, int]) -> dict:
    kind, count = workload
    with tempfile.TemporaryDirectory(prefix="profile-rule-", dir=args.data_root) as tmp:
        node, port = start_node(args.server, Path(tmp), flags, place)
        try:
            scope = scope_of(node.pid) if "quota" in place else None
            before = cpu_stat(scope) if scope else None
            row: dict = {"placement": pname, "config": cname, "workload": f"{kind}:{count}",
                         "flags": flags, "loadavg": loadavg()}
            probe_cpus = place.get("probe")
            if kind == "ping":
                row["ping"] = run_probe(
                    pinned(probe_cpus, [str(args.latency), str(port), str(args.iterations),
                                        str(node.pid), "PING"]), "command_latency")
            elif kind in ("writes", "reads"):
                row["ingest"] = run_probe(
                    pinned(probe_cpus, [str(args.pipelined), str(port), str(node.pid), str(count),
                                        str(args.batches // count), "20", "64",
                                        *(["book"] if kind == "reads" else [])]),
                    "pipelined_ingest")
            else:
                # The writer is given far more than it can finish and stopped once the PING probe
                # is done, so that writes run for the whole of the probe by construction rather than
                # because a batch count happened to be large enough - under a quota or on a shared
                # core the probe can take many times longer than it does alone. Its throughput is
                # not reported: it was cut off, and the question this workload answers is the PING.
                writer = subprocess.Popen(
                    pinned(probe_cpus, [str(args.pipelined), str(port), str(node.pid), str(count),
                                        str(100 * args.batches // count), "20", "64"]),
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
                time.sleep(0.3)
                if writer.poll() is not None:
                    raise SystemExit(f"the writer ended before the PING probe started "
                                     f"({writer.returncode}): {writer.stderr.read()[-400:]}")
                row["ping"] = run_probe(
                    pinned(probe_cpus, [str(args.latency), str(port), str(args.iterations),
                                        str(node.pid), "PING"]), "command_latency")
                if writer.poll() is not None:
                    raise SystemExit(f"the writer ended while the PING probe ran "
                                     f"({writer.returncode}): {writer.stderr.read()[-400:]}")
                writer.terminate()
                writer.wait(timeout=30)
            if scope:
                after = cpu_stat(scope)
                row["cgroup"] = {k: after[k] - before[k]
                                 for k in ("usage_usec", "nr_periods", "nr_throttled",
                                           "throttled_usec") if k in after}
            return row
        finally:
            node.send_signal(signal.SIGTERM)
            try:
                node.wait(timeout=10)
            except subprocess.TimeoutExpired:
                node.kill()


def median(rows: list[dict], *path: str) -> float | None:
    values = []
    for row in rows:
        value = row
        for key in path:
            value = value.get(key) if isinstance(value, dict) else None
        if value is not None:
            values.append(value)
    return statistics.median(values) if values else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--server", type=Path, required=True)
    ap.add_argument("--pipelined", type=Path, required=True, help="benchmarks/pipelined_ingest")
    ap.add_argument("--latency", type=Path, required=True, help="benchmarks/command_latency")
    ap.add_argument("--placement", action="append", required=True, metavar="NAME:KEY=VALUE,...")
    ap.add_argument("--config", action="append", required=True, metavar="NAME=FLAGS")
    ap.add_argument("--workload", action="append", required=True)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--iterations", type=int, default=20000, help="PING round trips per run")
    ap.add_argument("--batches", type=int, default=9600, help="write or read batches per run")
    ap.add_argument("--data-root", type=Path, default=REPO / "build-release" / "bench-data",
                    help="where each run's node keeps its data; refused if it is memory")
    args = ap.parse_args()

    args.data_root.mkdir(parents=True, exist_ok=True)
    try:
        fstype = hardware.require_durable_storage(args.data_root)
    except hardware.VolatileStorage:
        raise SystemExit(f"{args.data_root} is memory; pass --data-root on a disk") from None
    try:
        hardware.require_release(args.server.resolve().parent)
    except hardware.NotReleaseBuild as refusal:
        raise SystemExit(f"--server: {refusal}") from None

    placements = [parse_placement(p) for p in args.placement]
    configs = []
    for spec in args.config:
        name, sep, flags = spec.partition("=")
        if not sep or not name:
            raise SystemExit(f"--config wants NAME=FLAGS, got {spec!r}")
        configs.append((name, flags.split()))
    workloads = [parse_workload(w) for w in args.workload]

    hw = hardware.describe(args.data_root, args.server.resolve().parent)
    print(f"storage: {args.data_root} on {fstype}, {hw.disk_model} ({hw.disk_stack}); "
          f"{hw.cpu_model}, {hw.cores} cores", file=sys.stderr)
    print(f"loadavg at start: {loadavg()}", file=sys.stderr)

    cells = [(p, c, w) for w in workloads for p in placements for c in configs]
    runs: dict[tuple, list[dict]] = {}
    for r in range(args.rounds):
        # Rotate within each workload, so every cell of it runs first, in the middle and last.
        for w in workloads:
            group = [cell for cell in cells if cell[2] == w]
            for k in range(len(group)):
                (pname, place), (cname, flags), work = group[(r + k) % len(group)]
                row = one_run(args, pname, place, cname, flags, work)
                row["round"] = r + 1
                runs.setdefault((pname, cname, work), []).append(row)
                print(json.dumps(row), flush=True)
    print(f"loadavg at end: {loadavg()}", file=sys.stderr)

    for w in workloads:
        kind, count = w
        print(f"\n{kind}:{count}, medians of {args.rounds} rounds\n")
        print("| placement | config | PING p50 | p99 | max | levels/s | batch p99 | server CPU "
              "| throttled periods | throttled ms |")
        print("|---|---|---|---|---|---|---|---|---|---|")
        for (pname, _), (cname, _) in ((p, c) for p in placements for c in configs):
            rows = runs[(pname, cname, w)]

            def fmt(v: float | None, spec: str) -> str:
                return "-" if v is None else format(v, spec)

            cpu = median(rows, "ingest", "server_cpu_s") if kind != "ping" else \
                median(rows, "ping", "server_cpu_s")
            throttled_us = median(rows, "cgroup", "throttled_usec")
            print(f"| {pname} | {cname} "
                  f"| {fmt(median(rows, 'ping', 'p50_ns'), '.0f')} ns "
                  f"| {fmt(median(rows, 'ping', 'p99_ns'), '.0f')} ns "
                  f"| {fmt(median(rows, 'ping', 'max_ns'), '.0f')} ns "
                  f"| {fmt(median(rows, 'ingest', 'levels_per_s'), ',.0f')} "
                  f"| {fmt(median(rows, 'ingest', 'batch_p99_us'), '.1f')} µs "
                  f"| {fmt(cpu, '.2f')} s "
                  f"| {fmt(median(rows, 'cgroup', 'nr_throttled'), '.0f')} "
                  f"| {fmt(None if throttled_us is None else throttled_us / 1000, '.1f')} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
