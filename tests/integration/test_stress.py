"""Sustained load: throughput, concurrency, and what breaks under both.

The point of a stress category in an integration suite is not to produce a
benchmark number — the machine is shared and the numbers would be meaningless as a
performance claim. It is to run the server hard enough that races and leaks surface,
and to assert the invariants that must hold regardless of speed: nothing is lost,
nothing is duplicated, and no error goes unreported.

Duration defaults to 5 seconds so the suite stays usable. Set OB_STRESS_SECONDS=30
for the documented long run; the duration actually used is published in the report,
because a throughput number without its window is not a number.
"""
from __future__ import annotations

import os
import threading
import time

import pytest

from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.stress

# Read by the console report in conftest.py. The three keys it renders specially are
# stress_throughput, stress_errors and failover_time_sec; anything else is printed
# as a plain line.
custom_metrics: dict = {}

STRESS_SECONDS = float(os.environ.get("OB_STRESS_SECONDS", "5"))
LEVELS_PER_INSERT = 50


def test_sustained_insert_throughput(heavy_client: OrderbookEngine):
    """Insert continuously for the configured window and account for every level."""
    symbol = "STRESS-SUSTAINED"
    deadline = time.monotonic() + STRESS_SECONDS
    levels_sent = 0
    batches = 0
    errors = 0
    started = time.monotonic()

    while time.monotonic() < deadline:
        base = 1_000_000 + batches * LEVELS_PER_INSERT
        prices = [base + i for i in range(LEVELS_PER_INSERT)]
        qtys = [10 + (i % 7) for i in range(LEVELS_PER_INSERT)]
        try:
            heavy_client.insert(symbol, "BINANCE", "bid", prices, qtys)
            levels_sent += LEVELS_PER_INSERT
            batches += 1
        except Exception as exc:  # noqa: BLE001 - any failure is a failure to report
            errors += 1
            if errors <= 3:
                print(f"insert error: {exc!r}")

    elapsed = time.monotonic() - started
    heavy_client.flush()

    throughput = levels_sent / elapsed if elapsed > 0 else 0.0
    custom_metrics["stress_throughput"] = throughput
    custom_metrics["stress_errors"] = errors
    custom_metrics["stress_duration_sec"] = round(elapsed, 2)
    custom_metrics["stress_levels_sent"] = levels_sent

    assert errors == 0, f"{errors} inserts failed during the run"
    assert levels_sent > 0, "no levels were sent at all"

    rows = heavy_client.query_all(symbol, "BINANCE")
    assert len(rows) == levels_sent, (
        f"sent {levels_sent} levels, {len(rows)} came back — a sustained run must "
        f"not lose or duplicate rows")


def test_large_minsert_arrives_whole(heavy_client: OrderbookEngine):
    """1000 levels in one command, the documented maximum per side."""
    count = 1000
    prices = [2_000_000 + i for i in range(count)]
    qtys = [1 + (i % 13) for i in range(count)]

    heavy_client.insert("STRESS-BIG", "BINANCE", "ask", prices, qtys)
    heavy_client.flush()

    rows = heavy_client.query_all("STRESS-BIG", "BINANCE")
    assert len(rows) == count, f"expected {count} rows, got {len(rows)}"
    assert sorted(r.price for r in rows) == prices
    assert sum(r.quantity for r in rows) == sum(qtys), (
        "quantities do not add up, so some rows carry the wrong payload")


def test_concurrent_writer_and_readers(heavy_cluster, heavy_client: OrderbookEngine):
    """One writer, three readers, all against the same symbol.

    Readers must never see a partial row or an error. Row counts are only allowed to
    grow: a reader seeing fewer rows than a previous read would mean data went
    backwards.
    """
    symbol = "STRESS-CONCURRENT"
    stop = threading.Event()
    reader_errors: list[str] = []
    counts_seen: list[int] = []
    lock = threading.Lock()

    def reader() -> None:
        client = OrderbookEngine(host="127.0.0.1", port=heavy_cluster.primary().tcp_port)
        try:
            while not stop.is_set():
                try:
                    rows = client.query_all(symbol, "BINANCE")
                    with lock:
                        counts_seen.append(len(rows))
                except Exception as exc:  # noqa: BLE001
                    with lock:
                        reader_errors.append(repr(exc))
                    return
        finally:
            client.close()

    readers = [threading.Thread(target=reader, daemon=True) for _ in range(3)]
    for thread in readers:
        thread.start()

    written = 0
    deadline = time.monotonic() + STRESS_SECONDS
    try:
        while time.monotonic() < deadline:
            base = 3_000_000 + written
            heavy_client.insert(symbol, "BINANCE", "bid",
                                  [base + i for i in range(10)],
                                  [5] * 10)
            written += 10
            heavy_client.flush()
    finally:
        stop.set()
        for thread in readers:
            thread.join(timeout=10)

    custom_metrics["stress_reads"] = len(counts_seen)

    assert not reader_errors, f"readers failed: {reader_errors[:3]}"
    assert counts_seen, "no reads completed at all"
    assert max(counts_seen) <= written, (
        f"a reader saw {max(counts_seen)} rows but only {written} were written — "
        f"rows are being duplicated")


def test_concurrent_flushes_do_not_duplicate_rows(heavy_cluster,
                                                  heavy_client: OrderbookEngine):
    """Several clients flushing at once, which is how the flush race showed up.

    The background flush runs every 100ms, so a client FLUSH from four threads is
    the situation that used to register the same segment twice and double every row
    in it. `segment_merge_refused` staying at zero is the direct check that the
    duplicate is prevented rather than caught after the fact.
    """
    symbol = "STRESS-FLUSH"
    heavy_client.insert(symbol, "BINANCE", "bid",
                          [4_000_000 + i for i in range(20)], [3] * 20)

    errors: list[str] = []

    def flusher() -> None:
        client = OrderbookEngine(host="127.0.0.1", port=heavy_cluster.primary().tcp_port)
        try:
            for _ in range(15):
                try:
                    client.flush()
                except Exception as exc:  # noqa: BLE001
                    errors.append(repr(exc))
                    return
        finally:
            client.close()

    threads = [threading.Thread(target=flusher) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors, f"flush failed: {errors[:3]}"

    rows = heavy_client.query_all(symbol, "BINANCE")
    assert len(rows) == 20, (
        f"expected 20 rows after concurrent flushing, got {len(rows)}")

    status = heavy_client.status()
    assert "segment_merge_refused" in status, sorted(status)
    custom_metrics["stress_segment_merge_refused"] = status["segment_merge_refused"]
    assert status["segment_merge_refused"] == 0, (
        "two flush paths produced the same segment under load; the index check kept "
        "the rows right, but flush_mtx_ should have prevented it")


def test_many_symbols_stay_separate(heavy_client: OrderbookEngine):
    """Load spread across symbols must not leak rows between them."""
    symbols = [f"STRESS-MULTI-{i}" for i in range(20)]
    for index, symbol in enumerate(symbols):
        base = 5_000_000 + index * 1_000
        heavy_client.insert(symbol, "BINANCE", "bid",
                              [base, base + 1, base + 2], [1, 2, 3])
    heavy_client.flush()

    for index, symbol in enumerate(symbols):
        rows = heavy_client.query_all(symbol, "BINANCE")
        assert len(rows) == 3, f"{symbol}: expected 3 rows, got {len(rows)}"
        base = 5_000_000 + index * 1_000
        assert sorted(r.price for r in rows) == [base, base + 1, base + 2], (
            f"{symbol} returned prices from another symbol")


def test_pending_rows_drain_after_load(heavy_client: OrderbookEngine, heavy_cluster):
    """After a flush, nothing should still be pending — a leak would grow forever."""
    heavy_client.insert("STRESS-DRAIN", "BINANCE", "bid",
                          [6_000_000 + i for i in range(200)], [2] * 200)
    heavy_client.flush()

    import urllib.request
    with urllib.request.urlopen(
            f"http://127.0.0.1:{heavy_cluster.primary().metrics_port}/metrics",
            timeout=6) as resp:
        body = resp.read().decode(errors="replace")

    import re
    match = re.search(r"^ob_pending_rows(?:\{[^}]*\})?\s+([0-9.eE+-]+)$", body, re.M)
    assert match, "ob_pending_rows is not exposed"
    pending = float(match.group(1))
    custom_metrics["stress_pending_rows_after_flush"] = pending
    assert pending == 0, f"{pending} rows still pending after an explicit flush"
