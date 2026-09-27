#!/usr/bin/env python3
"""What refreshing the version vector at every flush tick costs a mesh node (#180).

Until #180 the vector a peer is told was exported only at a checkpoint, and since part 2a of #165 a
tick that seals nothing writes none - so a peer could be told, for up to ten seconds, that this node
lacked what it held. Since the fix every tick exports it again whenever the sequence tracker moved:
under the engine's lock, one pass over every (symbol, origin) the node holds - at most 4 096 entries
until #177. This measured that at the largest vector a node stated then. Three nodes; `--symbols` symbols written
once on the first, so every node holds that many entries; then `benchmarks/pipelined_ingest`
against the first node, whose writes move the tracker between any two ticks. Each run prints the
ingest's rate in levels per second and its batch round trip at p50, p99, p99.9 and max - a lock
held once a tick is a tail, not a median.

**On a build before #177, keep `--symbols` under 1 561** for the rounds of a mesh: past that a
vector did not fit the record that carries it and was sent as "send everything", so every
reconciliation resent the whole WAL and that, not the refresh, was what the ingest paid. Since #177
a vector of any size goes in parts. `--nodes 1` measures the refresh alone: a mesh node with no peer
broadcasts and resends nothing, so what differs between builds is the tick.

    scripts/measure_vector_refresh.py --server before=<ob_tcp_server> --server after=<ob_tcp_server> \\
        --probe build-release/benchmarks/pipelined_ingest [--symbols 1500] [--rounds 4]

`--probe-seconds S` measures a write's latency instead of the ceiling: one `INSERT` at a time on the
writer for S seconds, beside a trickle writing `--trickle-symbols` of the prefilled symbols every
100 ms, so that every tick has that many frontiers to bring the vector up to date with - the load
at which a lock the tick holds shows, rather than a ceiling at which the flush is the bottleneck.

Builds alternate ABAB... over `--rounds`. `--refresh-log LABEL` adds one run of that build with its
nodes at DEBUG and reports what each refresh took from the engine's own line - the cost itself,
rather than its effect on a client; DEBUG costs throughput, so that run is not one of the rounds.
Needs a native etcd on PATH (or ETCD).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "scripts"))

import measure_mesh_catchup as M  # noqa: E402

BASE_TS = 1_700_000_000_000_000_000


def prefill(port: int, symbols: int, batch: int = 256) -> None:
    """One single-level MINSERT for each of `symbols` symbols, pipelined."""
    c = M.Conn(port)
    try:
        cmds = [f"MINSERT V{i:04d} EX bid 1 {BASE_TS + i}\n100000 5 1\n" for i in range(symbols)]
        for i in range(0, len(cmds), batch):
            chunk = cmds[i:i + batch]
            c.s.sendall("".join(chunk).encode())
            for _ in chunk:
                line = c.r.readline()
                if not line.startswith(b"OK"):
                    raise RuntimeError(f"write refused: {line!r}")
                c.r.readline()
    finally:
        c.close()


class Trickle(threading.Thread):
    """`count` of the prefilled symbols written once each every 100 ms, pipelined: so that every
    tick has that many frontiers to bring the vector up to date with."""

    def __init__(self, port: int, symbols: int, count: int):
        super().__init__(daemon=True)
        self.port, self.symbols, self.count = port, symbols, count
        self.stop_flag = threading.Event()

    def run(self) -> None:
        c = M.Conn(self.port)
        rng, ts = random.Random(180), BASE_TS + 10_000_000
        while not self.stop_flag.is_set():
            started = time.monotonic()
            cmds = []
            for s in rng.sample(range(self.symbols), self.count):
                ts += 1
                cmds.append(f"MINSERT V{s:04d} EX bid 1 {ts}\n{100_000 + ts % 997} 5 1\n")
            c.s.sendall("".join(cmds).encode())
            for _ in cmds:
                c.r.readline()
                c.r.readline()
            self.stop_flag.wait(max(0.0, 0.1 - (time.monotonic() - started)))
        c.close()


def probe_run(writer, symbols: int, args) -> dict:
    """One INSERT at a time on the writer for `--probe-seconds`, with the trickle running - from
    `benchmarks/command_latency` when `--latency-probe` names it, whose clock has no interpreter
    in it, and from Python otherwise."""
    trickle = Trickle(writer.tcp, symbols, args.trickle_symbols)
    trickle.start()
    time.sleep(1)
    if args.latency_probe:
        out = subprocess.run([args.latency_probe, str(writer.tcp), str(args.probe_iterations),
                              str(writer.proc.pid), "INSERT", "PROBE", "EX", "bid", "100000", "1",
                              "1"], capture_output=True, text=True, timeout=900)
        trickle.stop_flag.set()
        trickle.join(timeout=5)
        if out.returncode != 0:
            return {"failed": (out.stderr.strip().splitlines() or ["?"])[-1][:160]}
        r = json.loads(out.stdout.strip().splitlines()[-1])
        return {"probe_n": r["iterations"], "probe_p50_ms": r["p50_ns"] / 1e6,
                "probe_p99_ms": r["p99_ns"] / 1e6, "probe_p999_ms": r["p999_ns"] / 1e6,
                "probe_max_ms": r["max_ns"] / 1e6, "probe_wall_s": r["wall_s"]}
    probe = M.Probe(writer.tcp)
    probe.start()
    time.sleep(args.probe_seconds)
    probe.stop_flag.set()
    probe.join(timeout=5)
    trickle.stop_flag.set()
    trickle.join(timeout=5)
    xs = sorted(ms for _, ms in probe.samples)
    return {"probe_n": len(xs), "probe_p50_ms": round(statistics.median(xs), 3),
            "probe_p99_ms": round(xs[int(len(xs) * 0.99)], 3),
            "probe_p999_ms": round(xs[int(len(xs) * 0.999)], 3), "probe_max_ms": round(xs[-1], 3),
            "probe_over_1ms": sum(1 for x in xs if x > 1.0)}


def one_run(label: str, server: str, probe: str, symbols: int, args, root: str,
            log_level: str = "INFO") -> dict:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    etcd, url = M.start_etcd(root)
    nodes = [M.Node(server, root, i, url, log_level) for i in range(args.nodes)]
    try:
        for n in nodes:
            n.start()
        time.sleep(3)
        writer = nodes[0]
        prefill(writer.tcp, symbols)
        last = f"V{symbols - 1:04d}"
        deadline = time.monotonic() + 120
        while not all(M.has_row_at(n.tcp, last, BASE_TS + symbols - 1) for n in nodes[1:]):
            if time.monotonic() > deadline:
                raise RuntimeError("the mesh did not deliver the prefill")
            time.sleep(0.5)
        time.sleep(2)
        before = os.path.getsize(writer.log)
        if args.probe_seconds:
            # Past the prefill's seals: its stores come due ten seconds after their row.
            time.sleep(12)
            before = os.path.getsize(writer.log)
            result = probe_run(writer, symbols, args)
        else:
            out = subprocess.run([probe, str(writer.tcp), str(writer.proc.pid),
                                  str(args.connections), str(args.batches), str(args.levels),
                                  str(args.batch)], capture_output=True, text=True, timeout=900)
            if out.returncode == 0:
                result = json.loads(out.stdout.strip().splitlines()[-1])
            else:
                result = {"failed": (out.stderr.strip().splitlines() or ["?"])[-1][:160]}
        result["label"] = label
        with open(writer.log, "rb") as f:
            f.seek(before)
            text = f.read().decode(errors="replace")
        # What the writer said while the ingest ran: a write waiting for room in the pending queue,
        # one refused because no room came, and a peer dropped for not draining its send buffer.
        result["queue_waits"] = text.count("is waiting for room in the pending queue")
        result["refused"] = text.count("did not free room in 5 s")
        result["peer_drops"] = text.count("is not draining")
        if log_level == "DEBUG":
            took = [(int(m.group(1)), int(m.group(2))) for m in re.finditer(
                r"Version vector cache refreshed at tracker generation \d+: (\d+) entries in "
                r"(\d+) us", text)]
            result["refreshes"] = len(took)
            if took:
                us = sorted(t for _, t in took)
                result["refresh_entries_max"] = max(e for e, _ in took)
                result["refresh_us_p50"] = statistics.median(us)
                result["refresh_us_p99"] = us[int(len(us) * 0.99)]
                result["refresh_us_max"] = us[-1]
        return result
    finally:
        for n in nodes:
            n.stop()
        etcd.terminate()
        etcd.wait(timeout=10)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", action="append", required=True, help="label=path")
    ap.add_argument("--probe", required=True, help="benchmarks/pipelined_ingest")
    ap.add_argument("--symbols", type=int, default=1500)
    ap.add_argument("--rounds", type=int, default=4)
    ap.add_argument("--connections", type=int, default=2)
    ap.add_argument("--batches", type=int, default=2000, help="per connection")
    ap.add_argument("--levels", type=int, default=20)
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--probe-seconds", type=float, default=0.0,
                    help="instead of the ingest: one INSERT at a time on the writer for this long, "
                         "beside a trickle of --trickle-symbols writes every 100 ms")
    ap.add_argument("--trickle-symbols", type=int, default=500)
    ap.add_argument("--latency-probe", metavar="PATH",
                    help="benchmarks/command_latency, to time the probe's INSERTs from C++")
    ap.add_argument("--probe-iterations", type=int, default=300_000)
    ap.add_argument("--nodes", type=int, default=3,
                    help="1 measures the refresh alone: no peer, so no broadcast and no resend")
    ap.add_argument("--refresh-log", metavar="LABEL")
    ap.add_argument("--root", default=os.path.join(os.getcwd(), "vector-refresh-runs"))
    args = ap.parse_args()
    servers = [s.split("=", 1) for s in args.server]
    keys = ("levels_per_s", "batch_p50_us", "batch_p99_us", "batch_p999_us", "batch_max_us",
            "server_cores", "probe_n", "probe_p50_ms", "probe_p99_ms", "probe_p999_ms",
            "probe_max_ms", "probe_over_1ms", "probe_wall_s", "queue_waits", "refused",
            "peer_drops", "failed")
    for r in range(args.rounds):
        for label, path in (servers if r % 2 == 0 else list(reversed(servers))):
            # A directory per run, not per build: an outlier is read in the log of the run it
            # happened in, and the next run of the same build used to remove it first.
            res = one_run(label, path, args.probe, args.symbols, args,
                          os.path.join(args.root, f"{label}-round{r + 1}"))
            print(f"round {r + 1} {label}: " + " ".join(f"{k}={res[k]}" for k in keys if k in res),
                  flush=True)
    if args.refresh_log:
        path = dict(servers)[args.refresh_log]
        res = one_run(args.refresh_log, path, args.probe, args.symbols, args,
                      os.path.join(args.root, args.refresh_log + "-debug"), "DEBUG")
        extra = ("refreshes", "refresh_entries_max", "refresh_us_p50", "refresh_us_p99",
                 "refresh_us_max")
        print(f"debug {args.refresh_log}: " + " ".join(
            f"{k}={res.get(k, '-')}" for k in keys + extra), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
