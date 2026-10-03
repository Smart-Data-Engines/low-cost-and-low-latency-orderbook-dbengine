"""Aggregations over the wire protocol.

Every aggregate query used to answer `OK` plus a row of zeros, because the response
formatter had one fixed row header and never read `agg_values`. The engine computed
the values correctly the whole time; nothing carried them to a client.

Expected numbers here are computed by hand in the test, so a wrong implementation
cannot make the test agree with it.
"""
from __future__ import annotations

import socket
import time

import pytest

from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.aggregations

# One book used by most tests: best bid 100000×50, best ask 101000×30.
#   spread     = 101000 - 100000            = 1000            (scale 1)
#   mid price  = (101000 + 100000) / 2      = 100500          (scale 10^6)
#   imbalance  = (50 - 30) * 10^9 / (50+30) = 250_000_000     (scale 10^9)
BID_PRICE, BID_QTY = 100_000, 50
ASK_PRICE, ASK_QTY = 101_000, 30


def raw_agg(port: int, sql: str, timeout: float = 6.0) -> str:
    """Send one query over a bare socket and return the raw reply."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.recv(4096)  # banner
        sock.sendall((sql + "\n").encode())
        time.sleep(0.4)
        try:
            return sock.recv(1 << 20).decode(errors="replace")
        except socket.timeout:
            return ""


@pytest.fixture
def book(primary_client: OrderbookEngine) -> str:
    """A symbol with one level on each side. Returns the symbol name."""
    symbol = "AGG-WIRE"
    primary_client.insert(symbol, "BINANCE", "bid", [BID_PRICE], [BID_QTY])
    primary_client.insert(symbol, "BINANCE", "ask", [ASK_PRICE], [ASK_QTY])
    return symbol


def test_spread_comes_back_over_the_raw_protocol(cluster, book):
    reply = raw_agg(cluster.primary().tcp_port,
                    f"SELECT SPREAD(*) FROM '{book}'.'BINANCE'")

    lines = [ln for ln in reply.strip().splitlines() if ln]
    assert lines[0] == "OK", reply
    assert lines[1].split("\t") == ["name", "value", "scale"], (
        f"aggregate responses must use their own header, got {lines[1]!r}")

    name, value, scale = lines[2].split("\t")
    assert name == "SPREAD(*)"
    assert int(value) == ASK_PRICE - BID_PRICE == 1000
    assert int(scale) == 1


def test_several_aggregates_in_one_query(cluster, book):
    reply = raw_agg(
        cluster.primary().tcp_port,
        f"SELECT SPREAD(*), MID_PRICE(*), IMBALANCE(10) FROM '{book}'.'BINANCE'")

    rows = [ln.split("\t") for ln in reply.strip().splitlines()[2:] if ln]
    assert len(rows) == 3, f"expected one row per aggregate, got {rows}"
    assert [r[0] for r in rows] == ["SPREAD(*)", "MID_PRICE(*)", "IMBALANCE(10)"]


def test_scales_are_reported_not_assumed(cluster, book):
    """A client must be able to divide rather than know that mid-price is ×10^6."""
    reply = raw_agg(cluster.primary().tcp_port,
                    f"SELECT MID_PRICE(*), IMBALANCE(10) FROM '{book}'.'BINANCE'")
    rows = {r[0]: r for r in
            (ln.split("\t") for ln in reply.strip().splitlines()[2:] if ln)}

    mid = rows["MID_PRICE(*)"]
    assert int(mid[2]) == 1_000_000, "mid-price scale missing from the response"
    assert int(mid[1]) // int(mid[2]) == (ASK_PRICE + BID_PRICE) // 2 == 100_500

    imb = rows["IMBALANCE(10)"]
    assert int(imb[2]) == 1_000_000_000, "imbalance scale missing from the response"
    assert int(imb[1]) == (BID_QTY - ASK_QTY) * 10**9 // (BID_QTY + ASK_QTY)


def test_one_sided_book_reports_null_not_zero(cluster, primary_client):
    """A spread with no ask side is absent. Zero would read as a tight market."""
    primary_client.insert("AGG-ONESIDE", "BINANCE", "bid", [200_000], [10])

    reply = raw_agg(cluster.primary().tcp_port,
                    "SELECT SPREAD(*) FROM 'AGG-ONESIDE'.'BINANCE'")

    row = reply.strip().splitlines()[2].split("\t")
    assert row[1] == "NULL", (
        f"expected NULL for a book with no ask side, got {row[1]!r}")
    assert int(row[2]) == 1, "the scale should still be reported for a NULL"


def test_mixing_aggregates_and_columns_is_refused(cluster, book):
    reply = raw_agg(cluster.primary().tcp_port,
                    f"SELECT price, SPREAD(*) FROM '{book}'.'BINANCE'")

    assert reply.startswith("ERR"), f"'price' was accepted and dropped: {reply!r}"
    assert "AGG_WITH_COLUMNS" in reply, reply


def test_timestamp_filter_with_an_aggregate_is_refused(cluster, book):
    reply = raw_agg(
        cluster.primary().tcp_port,
        f"SELECT SPREAD(*) FROM '{book}'.'BINANCE' "
        "WHERE timestamp BETWEEN 0 AND 9999999999999999999")

    assert reply.startswith("ERR"), (
        f"a timestamp filter an aggregate cannot honour was accepted: {reply!r}")
    assert "AGG_TIME_FILTER" in reply, reply


def test_unknown_aggregate_function_is_refused(cluster, book):
    reply = raw_agg(cluster.primary().tcp_port,
                    f"SELECT NONSENSE(*) FROM '{book}'.'BINANCE'")
    assert reply.startswith("ERR"), reply


# ── Python client ─────────────────────────────────────────────────────────────

def test_client_query_agg_returns_values_and_scales(primary_client, book):
    aggs = primary_client.query_agg(book, "BINANCE",
                                    "SPREAD(*)", "MID_PRICE(*)", "IMBALANCE(10)")

    assert set(aggs) == {"SPREAD(*)", "MID_PRICE(*)", "IMBALANCE(10)"}
    assert aggs["SPREAD(*)"].value == 1000
    assert aggs["SPREAD(*)"].scale == 1
    assert aggs["MID_PRICE(*)"].scale == 1_000_000
    assert aggs["MID_PRICE(*)"].real == pytest.approx(100_500.0)
    assert aggs["IMBALANCE(10)"].real == pytest.approx(0.25)


def test_client_reports_an_empty_aggregate_as_none(primary_client):
    primary_client.insert("AGG-ONESIDE-CLI", "BINANCE", "bid", [300_000], [10])

    aggs = primary_client.query_agg("AGG-ONESIDE-CLI", "BINANCE", "SPREAD(*)")

    spread = aggs["SPREAD(*)"]
    assert spread.is_empty
    assert spread.value is None
    assert spread.real is None


def test_client_query_refuses_to_parse_an_aggregate_response(primary_client, book):
    """query() would have skipped all three columns and returned an empty list."""
    with pytest.raises(OrderbookError) as excinfo:
        primary_client.query(f"SELECT SPREAD(*) FROM '{book}'.'BINANCE'")

    assert "query_agg" in str(excinfo.value), str(excinfo.value)


def test_client_query_agg_rejects_an_empty_expression_list(primary_client, book):
    with pytest.raises(OrderbookError):
        primary_client.query_agg(book, "BINANCE")


# ── #200: every function of one side read the bids, and nothing could ask for the asks ──────────

SIDES = "AGG-SIDES"


@pytest.fixture
def two_sided(primary_client: OrderbookEngine) -> str:
    """Bids 100 x 5 and 99 x 6, asks 101 x 7 and 102 x 8: the book #200 was measured on."""
    primary_client.insert(SIDES, "BINANCE", "bid", [100, 99], [5, 6])
    primary_client.insert(SIDES, "BINANCE", "ask", [101, 102], [7, 8])
    return SIDES


def test_one_query_answers_both_sides_and_the_spread(primary_client, two_sided):
    aggs = primary_client.query_agg(two_sided, "BINANCE", "VWAP(bid)", "VWAP(ask)", "SPREAD(*)",
                                    "DEPTH(101)", "DEPTH_RANGE(99, 102)", "SUM(ask)", "MAX(ask)")
    assert aggs["VWAP(bid)"].value == (100 * 5 + 99 * 6) * 10**6 // 11
    assert aggs["VWAP(ask)"].value == (101 * 7 + 102 * 8) * 10**6 // 15
    assert aggs["SPREAD(*)"].value == 1
    assert aggs["DEPTH(101)"].value == 7, "an ask's price answered 0 while every function read the bids"
    assert aggs["DEPTH_RANGE(99, 102)"].value == 26, "the asks in the range were left out"
    assert aggs["SUM(ask)"].value == 15
    assert aggs["MAX(ask)"].value == 102


def test_the_spellings_that_meant_the_bids_are_refused(cluster, two_sided):
    for expr in ("SUM(quantity)", "VWAP(*)", "MAX(price)", "CUMULATIVE_VOLUME(2)"):
        reply = raw_agg(cluster.primary().tcp_port, f"SELECT {expr} FROM '{two_sided}'.'BINANCE'")
        assert reply.startswith("ERR") and "AGG_NEEDS_SIDE" in reply, f"{expr}: {reply!r}"
