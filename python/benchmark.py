#!/usr/bin/env python3
"""
orderbook-dbengine benchmark script.

Measures insert throughput, flush latency, and query performance.
Supports both local (ctypes) and TCP modes.

Usage:
    # Local mode (default)
    python benchmark.py

    # TCP mode (requires running ob_tcp_server)
    python benchmark.py --mode tcp --host 127.0.0.1 --port 5555

    # Custom parameters
    python benchmark.py --rows 500000 --symbols 10 --levels 20 --rounds 3
"""

import argparse
import os
import shutil
import statistics
import sys
import tempfile
import time
from typing import List, Tuple

from orderbook_engine import OrderbookEngine, OrderbookRow


# ── Helpers ────────────────────────────────────────────────────────────────────

def fmt_rate(count: int, elapsed: float) -> str:
    rate = count / elapsed if elapsed > 0 else float("inf")
    if rate >= 1_000_000:
        return f"{rate / 1_000_000:.2f}M ops/s"
    elif rate >= 1_000:
        return f"{rate / 1_000:.2f}K ops/s"
    return f"{rate:.0f} ops/s"


def fmt_latency(seconds: float) -> str:
    us = seconds * 1_000_000
    if us < 1:
        return f"{seconds * 1_000_000_000:.0f} ns"
    elif us < 1000:
        return f"{us:.1f} µs"
    else:
        return f"{us / 1000:.2f} ms"


def percentile(data: List[float], p: float) -> float:
    data_sorted = sorted(data)
    k = (len(data_sorted) - 1) * (p / 100)
    f = int(k)
    c = f + 1
    if c >= len(data_sorted):
        return data_sorted[f]
    return data_sorted[f] + (k - f) * (data_sorted[c] - data_sorted[f])


def print_header(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def print_row(label: str, value: str):
    print(f"  {label:<36} {value}")


# ── Benchmarks ─────────────────────────────────────────────────────────────────

def bench_insert_throughput(engine: OrderbookEngine,
                            n_rows: int,
                            n_symbols: int,
                            levels_per_insert: int) -> Tuple[float, int]:
    """Insert n_rows levels and measure throughput."""
    symbols = [f"SYM-{i}" for i in range(n_symbols)]
    exchange = "BENCH"
    total_levels = 0

    prices_bid = [5000_00 - i * 100 for i in range(levels_per_insert)]
    prices_ask = [5001_00 + i * 100 for i in range(levels_per_insert)]
    qtys = [100 + i * 10 for i in range(levels_per_insert)]

    start = time.perf_counter()

    inserts_done = 0
    while total_levels < n_rows:
        sym = symbols[inserts_done % n_symbols]
        side = "bid" if (inserts_done % 2 == 0) else "ask"
        p = prices_bid if side == "bid" else prices_ask
        batch = min(levels_per_insert, n_rows - total_levels)
        engine.insert(sym, exchange, side,
                      prices=p[:batch], qtys=qtys[:batch])
        total_levels += batch
        inserts_done += 1

    elapsed = time.perf_counter() - start
    return elapsed, total_levels


def bench_flush(engine: OrderbookEngine) -> float:
    """Measure flush latency."""
    start = time.perf_counter()
    engine.flush()
    return time.perf_counter() - start


def bench_query_latency(engine: OrderbookEngine,
                        n_symbols: int,
                        rounds: int) -> List[float]:
    """Measure per-query latency across symbols."""
    latencies = []
    for r in range(rounds):
        for i in range(n_symbols):
            sym = f"SYM-{i}"
            sql = (f"SELECT * FROM '{sym}'.'BENCH' "
                   f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")
            start = time.perf_counter()
            rows = engine.query(sql)
            elapsed = time.perf_counter() - start
            latencies.append(elapsed)
    return latencies


def bench_query_with_limit(engine: OrderbookEngine,
                           n_symbols: int,
                           limit: int,
                           rounds: int) -> List[float]:
    """Measure query latency with LIMIT clause."""
    latencies = []
    for r in range(rounds):
        for i in range(n_symbols):
            sym = f"SYM-{i}"
            sql = (f"SELECT * FROM '{sym}'.'BENCH' "
                   f"WHERE timestamp BETWEEN 0 AND 9999999999999999999 "
                   f"LIMIT {limit}")
            start = time.perf_counter()
            rows = engine.query(sql)
            elapsed = time.perf_counter() - start
            latencies.append(elapsed)
    return latencies


# ── Main ───────────────────────────────────────────────────────────────────────

def run_benchmark(args):
    mode = args.mode
    n_rows = args.rows
    n_symbols = args.symbols
    levels = args.levels
    rounds = args.rounds

    print(f"\n  orderbook-dbengine benchmark")
    print(f"  mode={mode}, rows={n_rows:,}, symbols={n_symbols}, "
          f"levels/insert={levels}, rounds={rounds}")

    # Create engine
    tmp_dir = None
    if mode == "local":
        tmp_dir = tempfile.mkdtemp(prefix="ob_bench_")
        engine = OrderbookEngine(tmp_dir)
    else:
        engine = OrderbookEngine(host=args.host, port=args.port,
                                 timeout=args.timeout)

    try:
        # Warmup
        engine.insert("WARMUP", "BENCH", "bid", prices=[100], qtys=[1])
        engine.flush()
        engine.query_all("WARMUP", "BENCH")

        # ── INSERT throughput ──────────────────────────────────────────
        print_header("INSERT throughput")
        elapsed, total = bench_insert_throughput(engine, n_rows, n_symbols, levels)
        print_row("Total levels inserted", f"{total:,}")
        print_row("Time", f"{elapsed:.3f} s")
        print_row("Throughput", fmt_rate(total, elapsed))
        if mode == "tcp":
            # In TCP mode each level is a separate command
            print_row("TCP commands sent", f"{total:,}")
            print_row("Avg per command", fmt_latency(elapsed / total))

        # ── FLUSH ──────────────────────────────────────────────────────
        print_header("FLUSH")
        flush_time = bench_flush(engine)
        print_row("Flush latency", fmt_latency(flush_time))

        # ── QUERY full scan ────────────────────────────────────────────
        print_header(f"QUERY full scan ({n_symbols} symbols x {rounds} rounds)")
        latencies = bench_query_latency(engine, n_symbols, rounds)
        total_queries = len(latencies)
        total_query_time = sum(latencies)
        print_row("Total queries", f"{total_queries}")
        print_row("Total time", f"{total_query_time:.3f} s")
        print_row("Avg latency", fmt_latency(statistics.mean(latencies)))
        print_row("p50 latency", fmt_latency(percentile(latencies, 50)))
        print_row("p95 latency", fmt_latency(percentile(latencies, 95)))
        print_row("p99 latency", fmt_latency(percentile(latencies, 99)))
        print_row("Throughput", fmt_rate(total_queries, total_query_time))

        # ── QUERY with LIMIT ──────────────────────────────────────────
        limit = 100
        print_header(f"QUERY LIMIT {limit} ({n_symbols} symbols x {rounds} rounds)")
        lat_limit = bench_query_with_limit(engine, n_symbols, limit, rounds)
        total_lq = len(lat_limit)
        total_lt = sum(lat_limit)
        print_row("Total queries", f"{total_lq}")
        print_row("Avg latency", fmt_latency(statistics.mean(lat_limit)))
        print_row("p50 latency", fmt_latency(percentile(lat_limit, 50)))
        print_row("p99 latency", fmt_latency(percentile(lat_limit, 99)))
        print_row("Throughput", fmt_rate(total_lq, total_lt))

        # ── Summary ───────────────────────────────────────────────────
        print_header("Summary")
        print_row("Insert throughput", fmt_rate(total, elapsed))
        print_row("Flush latency", fmt_latency(flush_time))
        print_row("Query avg latency (full)", fmt_latency(statistics.mean(latencies)))
        print_row(f"Query avg latency (LIMIT {limit})", fmt_latency(statistics.mean(lat_limit)))
        print()

    finally:
        engine.close()
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(
        description="orderbook-dbengine benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--mode", choices=["local", "tcp"], default="local",
                        help="Engine mode (default: local)")
    parser.add_argument("--host", default="127.0.0.1",
                        help="TCP server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5555,
                        help="TCP server port (default: 5555)")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="TCP timeout in seconds (default: 30)")
    parser.add_argument("--rows", type=int, default=100_000,
                        help="Number of levels to insert (default: 100000)")
    parser.add_argument("--symbols", type=int, default=5,
                        help="Number of distinct symbols (default: 5)")
    parser.add_argument("--levels", type=int, default=10,
                        help="Levels per insert call (default: 10)")
    parser.add_argument("--rounds", type=int, default=3,
                        help="Query rounds per benchmark (default: 3)")
    args = parser.parse_args()
    run_benchmark(args)


if __name__ == "__main__":
    main()
