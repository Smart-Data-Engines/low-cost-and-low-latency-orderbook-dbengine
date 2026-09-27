#!/usr/bin/env python3
"""What a mesh logs and counts as conflicts (#182).

Three nodes at the default log level. Node 0 writes `--records` single-level updates to 1 000 price
levels of ten symbols - the shape of #178's measurement, where the node that received them logged
301 985 lines of "Conflict detected" - and, with `--second-writer`, node 1 then writes as many to the
same levels, later, which is what a conflict is: two origins writing one level. When every node holds
the last row of each writer, the run reads every node's log for the resolver's lines at INFO, its
`ob_mm_conflicts_total` and the log's size.

    scripts/measure_conflict_log.py --server before=build-a/ob_tcp_server \\
                                    --server after=build-b/ob_tcp_server [--second-writer]

The servers run in turn, each on a mesh of its own under `--root`; nothing here judges the numbers.
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import measure_mesh_catchup as M  # noqa: E402  (the same mesh, writes and reads)

BASE_TS = 1_700_000_000_000_000_000
SECOND_TS = BASE_TS + 1_000_000_000_000   # the second writer's rows, after the first's


def metric(port: int, name: str) -> float:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as resp:
        for line in resp.read().decode(errors="replace").splitlines():
            if line.startswith(name + " ") or line.startswith(name + "{"):
                return float(line.split()[-1])
    return 0.0


def resolver_lines(log: str) -> int:
    """The resolver's lines at INFO: one per conflict before #182, one per window after it."""
    n = 0
    with open(log, "rb") as f:
        for raw in f:
            if b'"component":"conflict"' in raw and b'"level":"INFO"' in raw:
                n += 1
    return n


def wait_for(nodes, symbol: str, ts: int, label: str) -> None:
    deadline = time.monotonic() + 600
    for node in nodes:
        while not M.has_row_at(node.tcp, symbol, ts):
            if time.monotonic() > deadline:
                raise RuntimeError(f"{label}: node{node.index} never got {symbol} at {ts}")
            time.sleep(0.25)


def one_run(label: str, server: str, records: int, root: str, second_writer: bool) -> None:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    etcd, url = M.start_etcd(root)
    nodes = [M.Node(server, root, i, url) for i in range(3)]
    per_symbol = records // len(M.SYMBOLS)
    last = lambda first: first + (per_symbol - 1) * len(M.SYMBOLS) + len(M.SYMBOLS) - 1  # noqa: E731
    try:
        for n in nodes:
            n.start()
        time.sleep(3)
        started = time.monotonic()
        M.write(nodes[0].tcp, BASE_TS, per_symbol)
        wait_for(nodes[1:], M.SYMBOLS[-1], last(BASE_TS), label)
        if second_writer:
            M.write(nodes[1].tcp, SECOND_TS, per_symbol)
            wait_for([nodes[0], nodes[2]], M.SYMBOLS[-1], last(SECOND_TS), label)
        took = time.monotonic() - started
        time.sleep(2)                             # the last tick's drain, and the logs' last lines
        for n in nodes:
            conflicts = metric(n.ports[1], "ob_mm_conflicts_total")
            lines = resolver_lines(n.log)
            size = os.path.getsize(n.log)
            print(f"{label}: node{n.index}: ob_mm_conflicts_total={conflicts:.0f} "
                  f"resolver lines at INFO={lines} log={size / 1e6:.1f} MB", flush=True)
        print(f"{label}: {records} record(s) from node0"
              + (f" and {records} from node1 to the same levels" if second_writer else "")
              + f", delivered everywhere in {took:.1f} s", flush=True)
    finally:
        for n in nodes:
            n.stop()
        etcd.terminate()
        etcd.wait(timeout=10)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", action="append", required=True, help="label=path")
    ap.add_argument("--records", type=int, default=300_000)
    ap.add_argument("--second-writer", action="store_true")
    ap.add_argument("--root", default=os.path.join(os.getcwd(), "conflict-log-runs"))
    args = ap.parse_args()
    for spec in args.server:
        label, path = spec.split("=", 1)
        one_run(label, path, args.records, os.path.join(args.root, label), args.second_writer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
