#!/usr/bin/env python3
"""Bytes on disk for one order book dataset - the engine, ClickHouse and TimescaleDB.

    scripts/measure_storage.py [--rows 200000] [--seed 7] [--csv FILE] [--build-dir build-release]

Each system loads the comparative harness's dataset - or `--csv`, a file in its format, such as one
`scripts/binance_capture_csv.py` recorded - through the harness's own adapter
(`benchmarks/comparative/systems`), with the schema and tuning each adapter declares. Then:

- orderbook: the files of its sealed segments - every file in a directory holding a `meta.json` -
  and its WAL beside them, after the adapter's FLUSH and `--settle-seconds` for its merges; the
  segment files by name; and the same bytes as the file system allocates them, block by block. Once
  for each of `--segment-formats` (3, 2 or both), so that one build's two formats are compared on
  one dataset;
- ClickHouse: `sum(bytes_on_disk)` of the table's active parts in `system.parts` after
  `OPTIMIZE TABLE ... FINAL`, with the compressed and uncompressed column bytes beside it and each
  column's own from `system.columns`, and the table's directories as `du` sees them. Then the same
  rows in tables whose columns carry the codecs ClickHouse's documentation recommends for them, at
  three settings, and two more with `GCD` for the fixed-point columns (`clickhouse_codecs`): the
  harness's table is ClickHouse as installed, and a claim about storage has to hold against
  ClickHouse as an expert would set it up too;
- TimescaleDB: `hypertable_size()` as loaded, and again with native compression enabled
  (segmentby symbol, orderby ts_ns) and every chunk compressed, the hypertable in one chunk that
  holds the whole dataset - TimescaleDB compresses a chunk at a time. Both are TimescaleDB; the
  adapter loads without compression.

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
import subprocess
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


def tree_bytes(root: Path) -> tuple[int, int, int, dict[str, int], int, int]:
    """Segment files, WAL files and everything else under the engine's data directory; the
    segment files' bytes by file name; the number of segments; and the segment files' bytes as the
    file system allocates them - in blocks, which for a file smaller than one is the block."""
    seg, wal, other, segments, seg_allocated = 0, 0, 0, 0, 0
    by_file: dict[str, int] = {}
    for dirpath, _dirs, files in os.walk(root):
        d = Path(dirpath)
        is_segment = (d / "meta.json").exists()
        segments += is_segment
        for f in files:
            st = (d / f).stat()
            size = st.st_size
            if is_segment:
                seg += size
                seg_allocated += st.st_blocks * 512
                by_file[f] = by_file.get(f, 0) + size
            elif f.endswith(".wal") or f.startswith("wal_"):
                wal += size
            else:
                other += size
    return seg, wal, other, dict(sorted(by_file.items())), segments, seg_allocated


def allocated_bytes(path: str) -> int | None:
    """What `du` says a directory owned by another service takes, in bytes as allocated and as
    apparent; None where it cannot be asked (no sudo)."""
    run = subprocess.run(["sudo", "-n", "du", "-sB1", path], capture_output=True, text=True)
    if run.returncode != 0:
        return None
    return int(run.stdout.split()[0])


def dataset_span_ns(csv_path: Path) -> int:
    """The time the dataset spans, from its first column: what a TimescaleDB chunk holding all of
    it has to cover."""
    lo, hi = None, None
    with csv_path.open() as handle:
        next(handle)
        for line in handle:
            ts = int(line[:line.index(",")])
            lo = ts if lo is None or ts < lo else lo
            hi = ts if hi is None or ts > hi else hi
    return 0 if lo is None else hi - lo


# The columns ClickHouse's documentation recommends codecs for, as an expert would declare them:
# DoubleDelta for a timestamp that rises, Delta for a value that moves by small steps, T64 for
# integers far narrower than their type - each followed by ZSTD, at three levels.
CLICKHOUSE_CODECS = {
    "zstd1": {"ts_ns": "DoubleDelta, ZSTD(1)", "level": "ZSTD(1)", "price_ticks": "ZSTD(1)",
              "size_lots": "ZSTD(1)"},
    "specialised_zstd3": {"ts_ns": "DoubleDelta, ZSTD(3)", "level": "T64, ZSTD(3)",
                          "price_ticks": "Delta, ZSTD(3)", "size_lots": "T64, ZSTD(3)"},
    "specialised_zstd9": {"ts_ns": "DoubleDelta, ZSTD(9)", "level": "T64, ZSTD(9)",
                          "price_ticks": "Delta, ZSTD(9)", "size_lots": "T64, ZSTD(9)"},
    # GCD first: fixed-point prices and quantities share a divisor - a tick, a lot - and ClickHouse
    # has the codec an expert would reach for to take it out (since 24.2).
    "gcd_zstd1": {"ts_ns": "DoubleDelta, ZSTD(1)", "level": "ZSTD(1)", "price_ticks": "GCD, ZSTD(1)",
                  "size_lots": "GCD, ZSTD(1)"},
    "gcd_specialised_zstd3": {"ts_ns": "DoubleDelta, ZSTD(3)", "level": "T64, ZSTD(3)",
                              "price_ticks": "GCD, Delta, ZSTD(3)", "size_lots": "GCD, T64, ZSTD(3)"},
}


CLICKHOUSE_TYPES = {"ts_ns": "Int64", "level": "UInt16", "price_ticks": "Int64", "size_lots": "Int64"}


def clickhouse_columns(ch: ClickHouseSystem, table: str) -> dict[str, int]:
    out = ch._ask("SELECT name, data_compressed_bytes FROM system.columns "
                  f"WHERE database = 'ob_bench' AND table = '{table}'")
    return {name: int(b) for name, b in (line.split("\t") for line in out.strip().splitlines())}


def clickhouse_allocated(ch: ClickHouseSystem, table: str) -> int | None:
    """The table's active parts as the file system allocates them, asked of `du`. Active only: a
    merge leaves the parts it replaced on the disk for `old_parts_lifetime` (eight minutes by
    default), and the table's directory counted them too."""
    paths = ch._ask("SELECT path FROM system.parts "
                    f"WHERE database = 'ob_bench' AND table = '{table}' AND active").split()
    sizes = [allocated_bytes(p) for p in paths]
    return None if not sizes or any(x is None for x in sizes) else sum(x for x in sizes if x)


def clickhouse_parts(ch: ClickHouseSystem, table: str) -> tuple[int, int, int, int]:
    row = ch._ask("SELECT sum(bytes_on_disk), sum(data_compressed_bytes), "
                  "sum(data_uncompressed_bytes), count() FROM system.parts "
                  f"WHERE database = 'ob_bench' AND table = '{table}' AND active")
    on_disk, compressed, uncompressed, parts = (int(x) for x in row.split())
    return on_disk, compressed, uncompressed, parts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--build-dir", type=Path, default=REPO / "build-release")
    ap.add_argument("--csv", type=Path, default=None,
                    help="load this file, in the harness's format, instead of generating the dataset")
    ap.add_argument("--segment-formats", default="3",
                    help="the engine's segment formats to load the dataset in, one run each: 3, 2 or "
                         "3,2 - one build's two settings, measured side by side")
    ap.add_argument("--settle-seconds", type=float, default=3.0,
                    help="how long to leave the engine after its FLUSH before measuring: a merge "
                         "waits for a period sealed into for 60 s, so 90 measures the store after "
                         "its merges")
    ap.add_argument("--port", type=int, default=21986)
    ap.add_argument("--work-dir", type=Path, default=Path.home() / "measure-storage",
                    help="the dataset and the engine's data directory; not a tmpfs, which would "
                         "hold the engine's files in RAM while the others write to disk")
    args = ap.parse_args()

    args.work_dir.mkdir(parents=True, exist_ok=True)
    if args.csv is not None:
        csv_path = args.csv.resolve()
        with csv_path.open("rb") as handle:
            rows = sum(1 for _ in handle) - 1
        out: dict = {"csv": str(csv_path), "rows": rows, "csv_bytes": csv_path.stat().st_size}
    else:
        csv_path = args.work_dir / f"dataset-{args.rows}-{args.seed}.csv"
        if not csv_path.exists():
            dataset.generate(csv_path, rows=args.rows, seed=args.seed)
        out = {"rows": args.rows, "seed": args.seed, "csv_bytes": csv_path.stat().st_size}

    out["orderbook"] = []
    for fmt in [f.strip() for f in args.segment_formats.split(",") if f.strip()]:
        ob = OrderbookSystem(args.build_dir / "ob_tcp_server", args.port, args.work_dir,
                             extra_args=("--segment-format", fmt))
        try:
            loaded = ob.load(csv_path)
            time.sleep(args.settle_seconds)   # the adapter's FLUSH sealed everything; merges settle
            seg, wal, other, by_file, segments, seg_allocated = tree_bytes(Path(ob._data_dir))
            out["orderbook"].append({"segment_format": int(fmt), "rows_loaded": loaded.rows_loaded,
                                     "segment_bytes": seg, "segment_bytes_allocated": seg_allocated,
                                     "segments": segments, "segment_bytes_by_file": by_file,
                                     "wal_bytes": wal, "other_bytes": other,
                                     "settle_seconds": args.settle_seconds, "version": ob.version(),
                                     "tuning": ob.tuning_applied()})
        finally:
            ob.teardown()

    ch = ClickHouseSystem()
    ok, why = ch.available()
    if ok:
        try:
            loaded = ch.load(csv_path)
            ch._ask("OPTIMIZE TABLE ob_bench.book FINAL")
            on_disk, compressed, uncompressed, parts = clickhouse_parts(ch, "book")
            out["clickhouse"] = {"rows_loaded": loaded.rows_loaded, "bytes_on_disk": on_disk,
                                 "bytes_allocated": clickhouse_allocated(ch, "book"),
                                 "data_compressed_bytes": compressed,
                                 "data_uncompressed_bytes": uncompressed, "parts": parts,
                                 "column_compressed_bytes": clickhouse_columns(ch, "book"),
                                 "version": ch.version(), "tuning": ch.tuning_applied()}
            variants = {}
            for variant, codecs in CLICKHOUSE_CODECS.items():
                table = f"book_{variant}"
                ch._ask(f"CREATE TABLE ob_bench.{table} AS ob_bench.book")
                for column, codec in codecs.items():
                    ch._ask(f"ALTER TABLE ob_bench.{table} MODIFY COLUMN {column} "
                            f"{CLICKHOUSE_TYPES[column]} CODEC({codec})")
                ch._ask(f"INSERT INTO ob_bench.{table} SELECT * FROM ob_bench.book")
                ch._ask(f"OPTIMIZE TABLE ob_bench.{table} FINAL")
                v_disk, v_comp, _v_uncomp, v_parts = clickhouse_parts(ch, table)
                variants[variant] = {"codecs": codecs, "bytes_on_disk": v_disk,
                                     "bytes_allocated": clickhouse_allocated(ch, table),
                                     "data_compressed_bytes": v_comp, "parts": v_parts,
                                     "column_compressed_bytes": clickhouse_columns(ch, table)}
            out["clickhouse_codecs"] = variants
        finally:
            ch.teardown()
    else:
        out["clickhouse"] = {"not_measured": why}

    # One chunk for the whole dataset: TimescaleDB compresses a chunk at a time, and the harness's
    # one-second chunks, chosen for its queries over ten seconds, compress a ten-minute recording a
    # second at a time.
    chunk_ns = max(1_000_000_000, dataset_span_ns(csv_path) + 1)
    ts = TimescaleDbSystem(chunk_interval_ns=chunk_ns)
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
