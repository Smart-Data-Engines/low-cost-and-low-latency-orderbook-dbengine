"""`insert_batch()`: several updates in one round trip, and a word about each of them (#141).

The engine has always been pipelineable — `Session::feed()` returns every complete command from
one read — and until #140 no client could benefit, because the server's second answer sat behind
Nagle waiting for an acknowledgement the client had no reason to send. This is the other half:
without it the measurement in #140 describes a client nobody has.

The module is deliberately two halves. The ones that need no server pin the **wire spelling** and
the refusals that happen before a byte goes out; the live ones pin what the server does with a
batch, including a batch that is partly refused — because a result list that cannot distinguish
"all fine" from "the third one bounced" would be the defect this API exists to avoid.
"""
from __future__ import annotations

import pytest

from orderbook_engine import (BatchOutcome, BookUpdate, OrderbookEngine, OrderbookError,
                              _insert_command)

pytestmark = pytest.mark.smoke

SYMBOL = "BATCH-WIRE"
EXCHANGE = "BINANCE"


# ── the wire spelling, with no server in it ──────────────────────────────────

@pytest.mark.parametrize("prices,qtys,counts,ts,expected", [
    ([100], [5], [1], None, "INSERT S E bid 100 5 1"),
    ([100], [5], [1], 1_700_000_000_000_000_000,
     "INSERT S E bid 100 5 1 1700000000000000000"),
    ([100, 99], [5, 6], [1, 2], None, "MINSERT S E bid 2\n100 5 1\n99 6 2"),
    ([100, 99], [5, 6], [1, 2], 1_700_000_000_000_000_000,
     "MINSERT S E bid 2 1700000000000000000\n100 5 1\n99 6 2"),
])
def test_the_command_is_spelled_one_way(prices, qtys, counts, ts, expected):
    """One definition, because `insert()` spelled this twice and a batch would have made three.

    Two copies of a protocol's syntax is how two clients of one server start saying different
    things — the same reason this engine has one identifier quoter and one query header.
    """
    assert _insert_command("S", "E", "bid", prices, qtys, counts, ts) == expected


def test_an_empty_batch_sends_nothing_and_says_so(cluster):
    client = OrderbookEngine(host="127.0.0.1", port=cluster.primary().tcp_port)
    try:
        assert client.insert_batch([]) == []
    finally:
        client.close()


def test_a_mismatched_update_is_refused_with_its_index(cluster):
    """Before a byte goes out, and naming which update — a partial send after rejecting update k
    would be a write nobody can find afterwards."""
    client = OrderbookEngine(host="127.0.0.1", port=cluster.primary().tcp_port)
    try:
        with pytest.raises(ValueError) as refused:
            client.insert_batch([
                BookUpdate(SYMBOL, EXCHANGE, "bid", [100], [5]),
                BookUpdate(SYMBOL, EXCHANGE, "bid", [100, 99], [5]),
            ])
        assert "update 1" in str(refused.value), str(refused.value)
    finally:
        client.close()


def test_a_batch_past_the_ceiling_is_refused_before_anything_is_sent(cluster):
    client = OrderbookEngine(host="127.0.0.1", port=cluster.primary().tcp_port)
    try:
        # One update whose body alone is past the cap, so the refusal is about the batch rather
        # than about how many entries it has.
        levels = OrderbookEngine.MAX_BATCH_BYTES // 8
        huge = BookUpdate(SYMBOL, EXCHANGE, "bid",
                          list(range(1_000_000, 1_000_000 + levels)), [1] * levels)
        with pytest.raises(OrderbookError) as refused:
            client.insert_batch([huge])
        assert "MAX_BATCH_BYTES" in str(refused.value), str(refused.value)
        assert "nothing was sent" in str(refused.value).lower(), str(refused.value)
    finally:
        client.close()


# ── against a real server ────────────────────────────────────────────────────

def test_a_batch_lands_and_every_value_comes_back(cluster):
    """Values, not a count: a batch that stored the right number of wrong rows would pass a
    count."""
    port = cluster.primary().tcp_port
    client = OrderbookEngine(host="127.0.0.1", port=port)
    book = f"{SYMBOL}-LAND"
    try:
        updates = [BookUpdate(book, EXCHANGE, "bid",
                              [6_000_000 + i], [100 + i], [1])
                   for i in range(100)]
        outcomes = client.insert_batch(updates)
        assert len(outcomes) == 100
        assert all(o.ok for o in outcomes), [o for o in outcomes if not o.ok][:3]
        assert [o.index for o in outcomes] == list(range(100))
        client.flush()

        rows = client.query(f"SELECT * FROM '{book}'.'{EXCHANGE}' "
                            f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")
        got = {(r.price, r.quantity) for r in rows}
        assert got == {(6_000_000 + i, 100 + i) for i in range(100)}, len(got)
    finally:
        client.close()


def test_one_refusal_does_not_hide_the_others(cluster):
    """The server refuses the middle update; the outcome list says which, and the other two are
    stored. A single exception would have thrown that away."""
    port = cluster.primary().tcp_port
    client = OrderbookEngine(host="127.0.0.1", port=port)
    book = f"{SYMBOL}-MIXED"
    try:
        good_a = BookUpdate(book, EXCHANGE, "bid", [7_000_001], [11], [1])
        # An empty symbol is refused by the parser, and the refusal is the server's rather than
        # the client's — which is the point: the client cannot pre-screen every rule the server
        # has, so the per-update answer has to come back from the wire.
        bad = BookUpdate("", EXCHANGE, "bid", [7_000_002], [12], [1])
        good_b = BookUpdate(book, EXCHANGE, "bid", [7_000_003], [13], [1])

        outcomes = client.insert_batch([good_a, bad, good_b])
        assert [o.ok for o in outcomes] == [True, False, True], outcomes
        assert outcomes[1].index == 1 and outcomes[1].message, outcomes[1]
        client.flush()

        rows = client.query(f"SELECT * FROM '{book}'.'{EXCHANGE}' "
                            f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")
        assert {r.price for r in rows} == {7_000_001, 7_000_003}, sorted(r.price for r in rows)
    finally:
        client.close()


def test_a_batch_and_a_loop_of_inserts_store_the_same_thing(cluster):
    """The control that makes the rest of this module mean something: whatever `insert_batch()`
    is faster at, it has to be the same writes."""
    port = cluster.primary().tcp_port
    client = OrderbookEngine(host="127.0.0.1", port=port)
    batched, looped = f"{SYMBOL}-B", f"{SYMBOL}-L"
    try:
        levels = [(8_000_000 + i, 50 + i) for i in range(25)]
        client.insert_batch([BookUpdate(batched, EXCHANGE, "ask", [p], [q], [1])
                             for p, q in levels])
        for p, q in levels:
            client.insert(looped, EXCHANGE, "ask", [p], [q], [1])
        client.flush()

        where = "WHERE timestamp BETWEEN 0 AND 9999999999999999999"
        a = sorted((r.price, r.quantity, r.side, r.order_count)
                   for r in client.query(f"SELECT * FROM '{batched}'.'{EXCHANGE}' {where}"))
        b = sorted((r.price, r.quantity, r.side, r.order_count)
                   for r in client.query(f"SELECT * FROM '{looped}'.'{EXCHANGE}' {where}"))
        assert a == b and len(a) == 25
    finally:
        client.close()
