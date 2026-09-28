#!/usr/bin/env python3
"""How long moving a symbol between shards refuses its writes (#196), and what the move costs.

Two shards on a native etcd, a symbol with a history of `--history` updates of 5 levels, and two
writers through it the whole time - the Python pool and the C++ one, each routing by the map. Each
run moves the symbol s0 -> s1 and reports what SHARD_INFO says of the move - its rounds and the
freeze, the window in which the symbol's writes were refused SYMBOL_MOVING - with how long the move
took, the rows it copied a second, and whether any write failed. Build the server in Release: a
window measured on a Debug build is not a number to quote.

    OB_SERVER_BINARY=build-release/ob_tcp_server \\
        python3 benchmarks/shard_migration.py --runs 10 --history 50000

Reuses the integration suite's cluster (tests/integration), so run it from the repository root.
"""
from __future__ import annotations

import argparse
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
sys.path.insert(0, str(ROOT / "tests" / "integration"))

from conftest import cpp_client_binary_path  # noqa: E402
from orderbook_engine import OrderbookEngine  # noqa: E402
from test_shard_control_plane import EXCHANGE, Cluster, command  # noqa: E402
from test_shard_migration import (T1, migration_ended, moved_symbol, rows_of,  # noqa: E402
                                  write_history)


def one_run(history: int) -> dict:
    cluster = Cluster()
    try:
        sym, key = moved_symbol(cluster)
        pool = OrderbookEngine(hosts=[cluster.address("s0")], coordinator_endpoints=[cluster.etcd],
                               health_check_interval=0.5)
        failed, acked = [], [0]
        stop = threading.Event()

        def writer() -> None:
            i = 0
            while not stop.is_set():
                try:
                    pool.insert(sym, EXCHANGE, "bid", [20_000 + i], [1], timestamp_ns=T1 + 1_000 * i)
                    acked[0] += 1
                except Exception as e:   # noqa: BLE001 - every failure is the finding
                    failed.append(repr(e))
                i += 1

        with tempfile.TemporaryDirectory() as tmp:
            until, out = Path(tmp) / "stop", Path(tmp) / "acked"
            try:
                write_history(pool, sym, history)
                held = len(rows_of(cluster, "s0", sym))
                thread = threading.Thread(target=writer)
                thread.start()
                cpp = subprocess.Popen([cpp_client_binary_path(), "--test", "shard_writer",
                                        "--coordinator", cluster.etcd, "--symbols", sym,
                                        "--until", str(until), "--out", str(out)],
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                try:
                    time.sleep(0.5)
                    started = time.monotonic()
                    assert command(cluster, "s0", f"MIGRATE {key} s1") == "OK"
                    info = migration_ended(cluster, "s0")
                    took = time.monotonic() - started
                    time.sleep(0.5)
                finally:
                    stop.set()
                    thread.join()
                    until.touch()
                    cpp_out = cpp.communicate(timeout=60)[0]
            finally:
                pool.close()
            cpp_acked = len(out.read_text().split()) if out.exists() else 0
        return {
            "phase": info.get("migration_phase"),
            "rounds": int(info.get("migration_rounds", 0)),
            "freeze_ms": float(info.get("migration_freeze_ms", 0)),
            "rows": int(info.get("migration_rows", 0)),
            "held": held,
            "seconds": took,
            "rows_per_s": int(info.get("migration_rows", 0)) / took if took > 0 else 0.0,
            "writes": acked[0] + cpp_acked,
            "failed": len(failed) + (0 if cpp.returncode == 0 else 1),
            "cpp": cpp_out.strip().splitlines()[-1] if cpp_out.strip() else "",
            "error": info.get("migration_error", ""),
        }
    finally:
        cluster.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--history", type=int, default=50_000)
    args = ap.parse_args()
    if not os.environ.get("OB_SERVER_BINARY"):
        sys.exit("OB_SERVER_BINARY names the server to measure: build it in Release")
    runs = []
    for i in range(args.runs):
        r = one_run(args.history)
        runs.append(r)
        print(f"run {i + 1}: {r['phase']}, {r['rounds']} round(s), freeze {r['freeze_ms']:.1f} ms, "
              f"{r['rows']} row(s) of {r['held']} held in {r['seconds']:.2f} s "
              f"({r['rows_per_s']:.0f} rows/s), {r['writes']} writes during, {r['failed']} failed"
              + (f" - {r['error']}" if r['error'] else ""), flush=True)
    done = [r for r in runs if r["phase"] == "done"]
    if done:
        freeze = sorted(r["freeze_ms"] for r in done)
        print(f"\n{len(done)} of {len(runs)} done; freeze ms min {freeze[0]:.1f} median "
              f"{statistics.median(freeze):.1f} max {freeze[-1]:.1f}; rows/s median "
              f"{statistics.median(r['rows_per_s'] for r in done):.0f}; failed writes "
              f"{sum(r['failed'] for r in runs)}")
    return 0 if len(done) == len(runs) and not any(r["failed"] for r in runs) else 1


if __name__ == "__main__":
    sys.exit(main())
