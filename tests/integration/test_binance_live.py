"""Live Binance depth data through the engine.

The rest of the suite writes numbers the test made up. This one writes a real L2
feed, which is the workload the engine exists for, and checks the things synthetic
data cannot: that real prices survive the round trip at the right scale, that both
sides of a real book arrive, and that aggregates over live data are sane.

Opt-in and hard-skipped — see binance_support.require_binance(). A third-party
exchange being unreachable is not a defect in this engine, and must never fail a
suite run offline.
"""
from __future__ import annotations

import pytest

from binance_support import (
    PLAUSIBLE_BTC_MAX,
    PLAUSIBLE_BTC_MIN,
    levels_from,
    price_to_usd,
    require_binance,
    stream_depth_updates,
)
from orderbook_engine import OrderbookEngine

require_binance()

pytestmark = pytest.mark.binance

SYMBOL = "BTC-USDT"
EXCHANGE = "BINANCE-LIVE"
STREAM_SECONDS = 8.0


@pytest.fixture(scope="module")
def ingested(mm_cluster) -> dict:
    """Stream live depth for a few seconds into one node. Returns what was sent.

    Uses the multi-master cluster because the failover-sync module needs the same
    topology, and a live feed is worth opening once per session rather than per test.
    """
    node = mm_cluster.nodes[0]
    client = OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=30)

    sent = {"bid": 0, "ask": 0, "updates": 0,
            "bid_prices": [], "ask_prices": []}
    try:
        for msg in stream_depth_updates(STREAM_SECONDS):
            sent["updates"] += 1
            for side in ("bid", "ask"):
                levels = levels_from(msg, side)
                if not levels:
                    continue
                client.insert(SYMBOL, EXCHANGE, side,
                              [p for p, _ in levels], [q for _, q in levels])
                sent[side] += len(levels)
                sent[f"{side}_prices"].extend(p for p, _ in levels)
        client.flush()
    finally:
        client.close()

    if sent["updates"] == 0:
        pytest.skip("no depth updates arrived within the streaming window")
    return sent


def test_live_updates_were_received(ingested):
    """If the feed produced nothing, everything below would pass on an empty book."""
    assert ingested["updates"] > 0
    assert ingested["bid"] + ingested["ask"] > 0, "no levels were extracted"


def test_ingested_rows_are_queryable(mm_cluster, ingested):
    client = OrderbookEngine(host="127.0.0.1", port=mm_cluster.nodes[0].tcp_port,
                             timeout=30)
    try:
        rows = client.query_all(SYMBOL, EXCHANGE)
    finally:
        client.close()

    expected = ingested["bid"] + ingested["ask"]
    assert len(rows) == expected, (
        f"sent {expected} live levels, {len(rows)} came back")


def test_live_prices_survive_at_the_right_scale(mm_cluster, ingested):
    """A scaling mistake would show up here and nowhere else in the suite."""
    client = OrderbookEngine(host="127.0.0.1", port=mm_cluster.nodes[0].tcp_port,
                             timeout=30)
    try:
        rows = client.query_all(SYMBOL, EXCHANGE)
    finally:
        client.close()

    usd_prices = [price_to_usd(r.price) for r in rows]
    assert usd_prices, "no rows to check"
    out_of_band = [p for p in usd_prices
                   if not (PLAUSIBLE_BTC_MIN <= p <= PLAUSIBLE_BTC_MAX)]
    assert not out_of_band, (
        f"{len(out_of_band)} prices outside a plausible BTC band, e.g. "
        f"{out_of_band[:5]} — a scale or parsing error")


def test_both_sides_of_a_real_book_arrive(mm_cluster, ingested):
    """Real feeds carry both sides; a stored book with one side is a lost `side`."""
    if ingested["bid"] == 0 or ingested["ask"] == 0:
        pytest.skip("the streaming window happened to carry only one side")

    client = OrderbookEngine(host="127.0.0.1", port=mm_cluster.nodes[0].tcp_port,
                             timeout=30)
    try:
        rows = client.query_all(SYMBOL, EXCHANGE)
    finally:
        client.close()

    sides = {r.side for r in rows}
    assert sides == {"bid", "ask"}, (
        f"stored sides are {sides}; both were sent "
        f"({ingested['bid']} bids, {ingested['ask']} asks)")


def test_aggregates_over_live_data_are_sane(mm_cluster, ingested):
    """Spread and mid-price on a real book: the numbers have to make sense."""
    client = OrderbookEngine(host="127.0.0.1", port=mm_cluster.nodes[0].tcp_port,
                             timeout=30)
    try:
        aggs = client.query_agg(SYMBOL, EXCHANGE, "SPREAD(*)", "MID_PRICE(*)")
    finally:
        client.close()

    spread = aggs["SPREAD(*)"]
    mid = aggs["MID_PRICE(*)"]

    if spread.is_empty or mid.is_empty:
        pytest.skip("the live book had only one side at the moment of the query")

    # A negative spread means bid above ask: either the data is wrong or the store is.
    assert spread.value >= 0, f"negative spread on live data: {spread.value}"
    mid_usd = mid.real / 100  # MID_PRICE is in engine sub-units, already unscaled
    assert PLAUSIBLE_BTC_MIN <= mid_usd <= PLAUSIBLE_BTC_MAX, (
        f"mid price {mid_usd} USD is outside a plausible band")
