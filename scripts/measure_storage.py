#!/usr/bin/env python3
"""Bytes on disk for one order book dataset - the engine, ClickHouse and TimescaleDB.

    scripts/measure_storage.py [--rows 200000] [--seed 7] [--build-dir build-release]

Each system loads the comparative harness's dataset through the harness's own adapter
(`benchmarks/comparative/systems`), with the schema and tuning each adapter declares. Then:

- orderbook: the files of its sealed segments - every file in a directory holding a `meta.json` -
  and its WAL beside them, after the adapter's FLUSH and a pause for a background merge;
- ClickHouse: `sum(bytes_on_disk)` of the table's active parts in `system.parts` after
  `OPTIMIZE TABLE ... FINAL`, with the compressed and uncompressed column bytes beside it;
- TimescaleDB: `hypertable_size()` as loaded, and again with native compression enabled
  (segmentby symbol, orderby ts_ns) and every chunk compressed. Both are TimescaleDB; the adapter
  loads without compression.

One JSON object on stdout. Bytes on disk do not depend on the machine's load, so this needs no quiet
machine and no rounds: run twice, the numbers repeat. A competitor that is not running is reported
as not measured, with the reason.

Where the engine stands, and why (measured 6 October 2026, `README.md`, "What it costs"): its
segments write the timestamp, the order count, the side and the level raw - fifteen of its
twenty-five bytes a row - and encode only the price, the quantity and the sequence number.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "python"))
from benchmarks.comparative import dataset  # noqa: E402
from benchmarks.comparative.systems.clickhouse import ClickHouseSystem  # noqa: E402
from benchmarks.comparative.systems.orderbook import OrderbookSystem  # noqa: E402
from benchmarks.comparative.systems.timescaledb import TimescaleDbSystem  # noqa: E402


def tree_bytes(root: Path) -> tuple[int, int, int]:
    """Segment files, WAL files and everything else under the engine's data directory."""
    seg, wal, other = 0, 0, 0
    for dirpath, _dirs, files in os.walk(root):
        d = Path(dirpath)
        is_segment = (d / "meta.json").exists()
        for f in files:
            size = (d / f).stat().st_size
            if is_segment:
                seg += size
            elif f.endswith(".wal") or f.startswith("wal_"):
                wal += size
            else:
                other += size
    return seg, wal, other


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--build-dir", type=Path, default=REPO / "build-release")
    ap.add_argument("--port", type=int, default=21986)
    ap.add_argument("--work-dir", type=Path, default=Path.home() / "measure-storage",
                    help="the dataset and the engine's data directory; not a tmpfs, which would "
                         "hold the engine's files in RAM while the others write to disk")
    args = ap.parse_args()

    args.work_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.work_dir / f"dataset-{args.rows}-{args.seed}.csv"
    if not csv_path.exists():
        dataset.generate(csv_path, rows=args.rows, seed=args.seed)
    out: dict = {"rows": args.rows, "seed": args.seed, "csv_bytes": csv_path.stat().st_size}

    ob = OrderbookSystem(args.build_dir / "ob_tcp_server", args.port, args.work_dir)
    try:
        loaded = ob.load(csv_path)
        time.sleep(3)   # the adapter's FLUSH sealed everything; let a background merge settle
        seg, wal, other = tree_bytes(Path(ob._data_dir))
        out["orderbook"] = {"rows_loaded": loaded.rows_loaded, "segment_bytes": seg,
                            "wal_bytes": wal, "other_bytes": other, "version": ob.version(),
                            "tuning": ob.tuning_applied()}
    finally:
        ob.teardown()

    ch = ClickHouseSystem()
    ok, why = ch.available()
    if ok:
        try:
            loaded = ch.load(csv_path)
            ch._ask("OPTIMIZE TABLE ob_bench.book FINAL")
            row = ch._ask("SELECT sum(bytes_on_disk), sum(data_compressed_bytes), "
                          "sum(data_uncompressed_bytes), count() FROM system.parts "
                          "WHERE database = 'ob_bench' AND table = 'book' AND active")
            on_disk, compressed, uncompressed, parts = (int(x) for x in row.split())
            out["clickhouse"] = {"rows_loaded": loaded.rows_loaded, "bytes_on_disk": on_disk,
                                 "data_compressed_bytes": compressed,
                                 "data_uncompressed_bytes": uncompressed, "parts": parts,
                                 "version": ch.version(), "tuning": ch.tuning_applied()}
        finally:
            ch.teardown()
    else:
        out["clickhouse"] = {"not_measured": why}

    ts = TimescaleDbSystem()
    ok, why = ts.available()
    if ok:
        try:
            loaded = ts.load(csv_path)
            before = int(ts._ask("SELECT hypertable_size('book')")[0].strip())
            ts._ask("ALTER TABLE book SET (timescaledb.compress, "
                    "timescaledb.compress_segmentby = 'symbol', "
                    "timescaledb.compress_orderby = 'ts_ns')")
            chunks = ts._ask("SELECT count(compress_chunk(c)) FROM show_chunks('book') c")
            after = int(ts._ask("SELECT hypertable_size('book')")[0].strip())
            out["timescaledb"] = {"rows_loaded": loaded.rows_loaded,
                                  "hypertable_size_as_loaded": before,
                                  "hypertable_size_compressed": after,
                                  "chunks_compressed": chunks, "version": ts.version(),
                                  "tuning": ts.tuning_applied()}
        finally:
            ts.teardown()
    else:
        out["timescaledb"] = {"not_measured": why}

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
