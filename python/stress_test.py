"""
Feature: stress-testing
Python TCP stress tests for orderbook-dbengine.

Tests:
  - test_tcp_insert_latency  (Req 4.1–4.5) — INSERT × 60s, latency per-request
  - test_tcp_minsert_latency (Req 5.1–5.5) — MINSERT 1000 levels × 60s, latency per-batch

Gating:
  - OB_STRESS_TESTS env var must be set
  - ob_tcp_server must be running at 127.0.0.1:5555

Property 1: Kompletność danych po ingestion + flush
  After inserting N rows and flushing, query SELECT returns exactly N rows.
"""

import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from orderbook_engine import OrderbookEngine, OrderbookError

# ── Constants ──────────────────────────────────────────────────────────────────

STRESS_HOST = "127.0.0.1"
STRESS_PORT = 5555
STRESS_DURATION_S = 60
MINSERT_BATCH_SIZE = 1000


# ── Helpers ────────────────────────────────────────────────────────────────────

def percentile(sorted_data: list, p: float) -> float:
    """Compute percentile from a sorted list. p in [0.0, 1.0]."""
    if not sorted_data:
        return 0.0
    idx = int(p * (len(sorted_data) - 1))
    return sorted_data[idx]


def fmt_num(n: int) -> str:
    """Format integer with comma separators."""
    return f"{n:,}"


def print_report(test_name: str, duration: float, total_ops: int,
                 throughput_label: str, throughput_val: float,
                 latencies_s: list, expected_rows: int, actual_rows: int,
                 extra_lines: list = None):
    """Print a formatted stress test report to stdout."""
    latencies_s_sorted = sorted(latencies_s)
    p50 = percentile(latencies_s_sorted, 0.50) * 1000
    p95 = percentile(latencies_s_sorted, 0.95) * 1000
    p99 = percentile(latencies_s_sorted, 0.99) * 1000
    avg = (sum(latencies_s) / len(latencies_s) * 1000) if latencies_s else 0.0
    max_lat = (max(latencies_s) * 1000) if latencies_s else 0.0

    match = "✓" if actual_rows == expected_rows else "✗"

    print(f"\n=== STRESS TEST: {test_name} ===")
    print(f"Duration:        {duration:.2f}s")
    print(f"Total inserts:   {fmt_num(total_ops)}")
    print(f"Throughput:      {throughput_val:.0f} {throughput_label}")
    print(f"Latency p50:     {p50:.2f} ms")
    print(f"Latency p95:     {p95:.2f} ms")
    print(f"Latency p99:     {p99:.2f} ms")
    print(f"Latency avg:     {avg:.2f} ms")
    print(f"Latency max:     {max_lat:.2f} ms")
    if extra_lines:
        for line in extra_lines:
            print(line)
    print(f"Query rows:      {fmt_num(actual_rows)} "
          f"(expected: {fmt_num(expected_rows)}) {match}")
    print("PASS" if actual_rows == expected_rows else "FAIL")


# ── Gating ─────────────────────────────────────────────────────────────────────

def _check_server_available():
    """Try connecting to the TCP server. Raises SkipTest if unavailable."""
    import socket as _socket
    try:
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect((STRESS_HOST, STRESS_PORT))
        s.close()
    except (ConnectionRefusedError, OSError):
        raise unittest.SkipTest(
            f"Server not running at {STRESS_HOST}:{STRESS_PORT}")


# ── Test class ─────────────────────────────────────────────────────────────────

class StressTestTCP(unittest.TestCase):
    """TCP stress tests — require running ob_tcp_server and OB_STRESS_TESTS=1."""

    @classmethod
    def setUpClass(cls):
        if not os.environ.get("OB_STRESS_TESTS"):
            raise unittest.SkipTest(
                "Stress tests disabled (set OB_STRESS_TESTS=1)")
        _check_server_available()

    def test_tcp_insert_latency(self):
        """
        Req 4.1–4.5: INSERT single levels for 60s, measure per-request latency.

        **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
        Feature: stress-testing, Property 1: Kompletność danych po ingestion + flush
        """
        symbol = "TCPINS"
        exchange = "STRESS"

        engine = OrderbookEngine(host=STRESS_HOST, port=STRESS_PORT,
                                 timeout=30.0)
        try:
            latencies = []
            total_inserts = 0
            seq = 0

            start_time = time.perf_counter()
            deadline = start_time + STRESS_DURATION_S

            while time.perf_counter() < deadline:
                seq += 1
                price = 5_000_00 + (seq % 500)
                qty = 1000 + (seq % 500)

                t0 = time.perf_counter()
                engine.insert(
                    symbol, exchange, "bid",
                    prices=[price], qtys=[qty], counts=[1],
                    timestamp_ns=int(time.time_ns()),
                    seq=seq,
                )
                t1 = time.perf_counter()

                latencies.append(t1 - t0)
                total_inserts += 1

            elapsed = time.perf_counter() - start_time

            # Flush and verify data completeness
            engine.flush()
            rows = engine.query_all(symbol, exchange)
            actual_rows = len(rows)

            throughput = total_inserts / elapsed if elapsed > 0 else 0

            print_report(
                test_name="test_tcp_insert_latency",
                duration=elapsed,
                total_ops=total_inserts,
                throughput_label="inserts/s",
                throughput_val=throughput,
                latencies_s=latencies,
                expected_rows=total_inserts,
                actual_rows=actual_rows,
            )

            self.assertEqual(
                actual_rows, total_inserts,
                f"Data completeness: expected {total_inserts} rows, "
                f"got {actual_rows}")
        finally:
            engine.close()

    def test_tcp_minsert_latency(self):
        """
        Req 5.1–5.5: MINSERT 1000 levels per batch for 60s, measure per-batch latency.

        **Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5**
        Feature: stress-testing, Property 1: Kompletność danych po ingestion + flush
        """
        symbol = "TCPMIN"
        exchange = "STRESS"
        batch_size = MINSERT_BATCH_SIZE

        engine = OrderbookEngine(host=STRESS_HOST, port=STRESS_PORT,
                                 timeout=30.0)
        try:
            latencies = []
            total_levels = 0
            total_batches = 0
            seq = 0

            # Pre-generate batch data (reused each iteration with varying base)
            base_prices = [5_000_00 + i * 100 for i in range(batch_size)]
            base_qtys = [1000 + (i % 500) for i in range(batch_size)]
            base_counts = [1] * batch_size

            start_time = time.perf_counter()
            deadline = start_time + STRESS_DURATION_S

            while time.perf_counter() < deadline:
                seq += 1
                # Use same prices each batch (update existing levels, not create new)
                prices = list(base_prices)

                t0 = time.perf_counter()
                engine.insert(
                    symbol, exchange, "bid",
                    prices=prices, qtys=base_qtys, counts=base_counts,
                    timestamp_ns=int(time.time_ns()),
                    seq=seq,
                )
                t1 = time.perf_counter()

                latencies.append(t1 - t0)
                total_levels += batch_size
                total_batches += 1

            elapsed = time.perf_counter() - start_time

            # Flush and verify data completeness
            engine.flush()
            rows = engine.query_all(symbol, exchange)
            actual_rows = len(rows)

            levels_per_sec = total_levels / elapsed if elapsed > 0 else 0
            batches_per_sec = total_batches / elapsed if elapsed > 0 else 0

            extra = [
                f"Batches:         {fmt_num(total_batches)}",
                f"Batch size:      {fmt_num(batch_size)} levels",
                f"Batches/s:       {batches_per_sec:.0f}",
            ]

            print_report(
                test_name="test_tcp_minsert_latency",
                duration=elapsed,
                total_ops=total_levels,
                throughput_label="levels/s",
                throughput_val=levels_per_sec,
                latencies_s=latencies,
                expected_rows=total_levels,
                actual_rows=actual_rows,
                extra_lines=extra,
            )

            self.assertEqual(
                actual_rows, total_levels,
                f"Data completeness: expected {total_levels} rows, "
                f"got {actual_rows}")
        finally:
            engine.close()


if __name__ == "__main__":
    unittest.main()
