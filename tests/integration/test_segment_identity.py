"""#136 at the level it was measured: the same event-time span written twice.

A segment's directory used to be named for its event-time range alone, so two flushes covering
one span wrote to one directory and the second destroyed the first. Before #105 no client could
reach that state — the wire dropped the client's timestamp and the server stamped arrival — so
putting event time on the wire, which is what makes a backfill expressible, is also what made
re-running one destructive.

Both cases are asserted here because they fail differently and the second is the worse one: with
a shorter second write the index kept the first write's row count over the second write's bytes
and the reader refused the whole segment, so the symbol returned **nothing**. Every write was
acknowledged and every FLUSH returned OK in both.

Each test uses its own symbol. Storage is append-only and this module runs against the session
cluster, so a shared symbol would make these assertions depend on test order — which is exactly
how #139's column-projection fixture went wrong.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from orderbook_engine import OrderbookEngine

BASE_NS = 1_700_000_000_000_000_000
UPDATES = 50


def _write_span(client: OrderbookEngine, symbol: str, levels: int, first_price: int) -> None:
    """Write UPDATES updates of `levels` levels over one fixed span, then flush them."""
    for i in range(UPDATES):
        client.insert(symbol, "EX", "bid",
                      [first_price + j for j in range(levels)], [10] * levels,
                      timestamp_ns=BASE_NS + i * 1000)
    client.flush()


def test_rewriting_a_span_with_different_values_keeps_both(cluster):
    """The first case: same span, same shape, different prices.

    Measured before the fix: 4000 of an expected 8000 — the first write's values gone, the rows
    read back carrying the second write's prices under the first write's index entry.
    """
    symbol = "SEGID-SAME-SHAPE"
    client = OrderbookEngine(host="127.0.0.1", port=cluster.primary().tcp_port, timeout=60.0)
    try:
        _write_span(client, symbol, 20, 500)
        _write_span(client, symbol, 20, 900)
        rows = client.query(f"SELECT * FROM '{symbol}'.'EX'")
    finally:
        client.close()

    assert len(rows) == UPDATES * 20 * 2, (
        f"{len(rows)} rows for two acknowledged writes of {UPDATES * 20} each. A segment's "
        f"directory is named for its span, so before #136 the second flush wrote over the first")


def test_rewriting_a_span_with_fewer_rows_does_not_strand_the_first_write(cluster):
    """The second case, and the one that returned nothing at all.

    The index kept `row_count` from the first write while the directory held the second write's
    shorter columns, and the reader — correctly — refused to serve a segment whose columns are
    shorter than its count says. Two acknowledged writes, and the symbol answered with zero rows.
    """
    symbol = "SEGID-SHORTER"
    client = OrderbookEngine(host="127.0.0.1", port=cluster.primary().tcp_port, timeout=60.0)
    try:
        _write_span(client, symbol, 20, 500)
        _write_span(client, symbol, 5, 900)
        rows = client.query(f"SELECT * FROM '{symbol}'.'EX'")
    finally:
        client.close()

    assert len(rows) == UPDATES * 20 + UPDATES * 5, (
        f"{len(rows)} rows, where both writes together are {UPDATES * 25}. Zero here is the "
        f"failure this test exists for: the whole symbol unreadable after two writes the client "
        f"was told had succeeded")
