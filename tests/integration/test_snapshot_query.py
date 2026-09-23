"""What a SNAPSHOT answers over the wire (#167, #168).

From #139 on a `SNAPSHOT` answered `OK`, an empty header and one empty line per row: the shape the
wire formatter prints was assigned below its branch's return, so the rows were there and every
column was gone (#167). Measured with the probe these tests are made of, on the tree before #139
and at it. And the book it answered was the last row delivered for each level rather than the
latest at or before its time, so a correction for an earlier instant that arrived later replaced
the book it came after (#168) - visible over the wire only on the tree before #139, because after
it nothing was.

Over the raw protocol, for the same reason as `test_column_projection.py`: the shape of the answer
is the thing being asked about, and our clients read a row by position.
"""
from __future__ import annotations

import hashlib

import pytest

from orderbook_engine import OrderbookEngine

from conftest import raw_query

pytestmark = pytest.mark.smoke

EXCHANGE = "BINANCE"
BASE = 1_790_000_000 * 10**9
SEC = 10**9
HEADER = ["timestamp_ns", "price", "quantity", "order_count", "side", "level", "sequence_number"]


@pytest.fixture
def book(cluster, primary_client: OrderbookEngine, request) -> str:
    """One bid level written twice in one flush, the later event time first.

    A symbol of this test's own: storage is append-only and `cluster` is session-scoped, so a
    shared symbol would carry the rows of every test before it.
    """
    symbol = f"SNAP-{hashlib.sha1(request.node.name.encode()).hexdigest()[:8]}"
    primary_client.insert(symbol, EXCHANGE, "bid", [100], [5], [1], timestamp_ns=BASE + 5 * SEC)
    primary_client.insert(symbol, EXCHANGE, "bid", [101], [5], [1], timestamp_ns=BASE + 3 * SEC)
    primary_client.flush()
    return symbol


def test_a_snapshot_answers_its_columns_and_the_latest_row(cluster, book):
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT * FROM '{book}'.'{EXCHANGE}' WHERE AT {BASE + 10 * SEC}")
    # Under #167 the empty header and the empty row are blank lines, which raw_query drops.
    assert lines[:2] == ["OK", "\t".join(HEADER)], f"{lines}: a SNAPSHOT answered no columns (#167)"
    assert len(lines) == 3, lines
    row = dict(zip(HEADER, lines[2].split("\t")))
    assert row["timestamp_ns"] == str(BASE + 5 * SEC), (
        "the correction for 3 s replaced the book written at 5 s (#168)")
    assert row["price"] == "100"


def test_a_snapshot_before_the_later_row_answers_the_earlier(cluster, book):
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT * FROM '{book}'.'{EXCHANGE}' WHERE AT {BASE + 4 * SEC}")
    assert lines[:2] == ["OK", "\t".join(HEADER)], lines
    assert len(lines) == 3, lines
    assert dict(zip(HEADER, lines[2].split("\t")))["price"] == "101"


def test_a_snapshot_answers_the_columns_it_names(cluster, book):
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT price, level FROM '{book}'.'{EXCHANGE}' WHERE AT {BASE + 10 * SEC}")
    assert lines == ["OK", "price\tlevel", "100\t0"], lines
