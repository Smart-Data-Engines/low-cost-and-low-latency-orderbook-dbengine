#!/usr/bin/env python3
"""What refreshing the version vector at every flush tick costs a mesh node (#180).

Until #180 the vector a peer is told was exported only at a checkpoint, and since part 2a of #165 a
tick that seals nothing writes none - so a peer could be told, for up to ten seconds, that this node
lacked what it held. Since the fix every tick exports it again whenever the sequence tracker moved:
under the engine's lock, one pass over every (symbol, origin) the node holds, at most 4 096 entries.
This measures that at the largest vector a node states. Three nodes; `--symbols` symbols written
once on the first, so every node holds that many entries; then `benchmarks/pipelined_ingest`
against the first node, whose writes move the tracker between any two ticks. Each run prints the
ingest's rate in levels per second and its batch round trip at p50, p99, p99.9 and max - a lock
held once a tick is a tail, not a median.

    scripts/measure_vector_refresh.py --server before=<ob_tcp_server> --server after=<ob_tcp_server> \\
        --probe build-release/benchmarks/pipelined_ingest [--symbols 4000] [--rounds 3]

Builds alternate ABAB... over `--rounds`. `--refresh-log LABEL` adds one run of that build with its
nodes at DEBUG and reports what each refresh took from the engine's own line - the cost itself,
rather than its effect on a client; DEBUG costs throughput, so that run is not one of the rounds.
Needs a native etcd on PATH (or ETCD).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
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


def one_run(label: str, server: str, probe: str, symbols: int, args, root: str,
            log_level: str = "INFO") -> dict:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    etcd, url = M.start_etcd(root)
    nodes = [M.Node(server, root, i, url, log_level) for i in range(3)]
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
        out = subprocess.run([probe, str(writer.tcp), str(writer.proc.pid), str(args.connections),
                              str(args.batches), str(args.levels), str(args.batch)],
                             capture_output=True, text=True, timeout=900, check=True)
        result = json.loads(out.stdout.strip().splitlines()[-1])
        result["label"] = label
        if log_level == "DEBUG":
            with open(writer.log, "rb") as f:
                f.seek(before)
                text = f.read().decode(errors="replace")
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
    ap.add_argument("--symbols", type=int, default=4000)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--connections", type=int, default=2)
    ap.add_argument("--batches", type=int, default=2000, help="per connection")
    ap.add_argument("--levels", type=int, default=20)
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--refresh-log", metavar="LABEL")
    ap.add_argument("--root", default=os.path.join(os.getcwd(), "vector-refresh-runs"))
    args = ap.parse_args()
    servers = [s.split("=", 1) for s in args.server]
    keys = ("levels_per_s", "batch_p50_us", "batch_p99_us", "batch_p999_us", "batch_max_us",
            "server_cores")
    for r in range(args.rounds):
        for label, path in (servers if r % 2 == 0 else list(reversed(servers))):
            res = one_run(label, path, args.probe, args.symbols, args,
                          os.path.join(args.root, label))
            print(f"round {r + 1} {label}: " + " ".join(f"{k}={res[k]}" for k in keys), flush=True)
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
