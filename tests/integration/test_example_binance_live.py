"""The live market data example, `examples/binance-live/`, without the network (#43).

The example writes Binance's depth20 stream into a node and reads it back two ways, and both are what
this module checks, against messages made here from a seed - the same shape as the stream's, with
nothing of the exchange's data in the repository:

- **history**: the minute bars `bars.py` reads (`OPEN`, `HIGH`, `LOW`, `CLOSE` of the mid, the
  time-weighted spread, the rows) against the same bars computed here from the messages, to the
  last digit of the engine's scale; and `WHERE AT` the last instant against the last message;
- **the live book**: `BOOK` against each symbol's last message, every level of both sides - which is
  what the example's removals are for (`market.py`, roadmap #233): without them the live book keeps
  every price it was ever written - and the exporter's `/metrics` against the best prices.

And the two refusals the example promises: a value that is not a whole number of steps is refused
before anything is written, and a node that does not answer is reported down with no market.
"""
from __future__ import annotations

import random
import secrets
import sys
import threading
import urllib.request
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest

from conftest import free_port
from orderbook_engine import OrderbookEngine

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "binance-live"
sys.path.insert(0, str(EXAMPLE))

import bars  # noqa: E402
import exporter  # noqa: E402
import market  # noqa: E402

pytestmark = pytest.mark.smoke

MINUTE = 60_000_000_000
STEPS = market.Steps(price=Decimal("0.01"), qty=Decimal("0.00001"))
T0 = 1_791_800_000 * 1_000_000_000 // MINUTE * MINUTE   # a whole minute, 2026-10-12


Message = Tuple[str, dict, int]


def synthetic_depth20(symbols: List[str], seed: int, minutes: int = 3) -> List[Message]:
    """Messages shaped like `<symbol>@depth20@100ms`'s `data`, every 50 - 150 ms per symbol, from a
    random walk of the mid; the first of each symbol at T0 exactly, so every minute has an OPEN.

    The walk moves by up to three steps, so prices leave the top twenty and come back - the case the
    removals are for. One message in eight repeats the bids unchanged, and one in eight keeps the best
    level of both sides and changes the quantities behind it."""
    rng = random.Random(seed)
    out: List[Message] = []
    for n, symbol in enumerate(symbols):
        mid = 6_000_000 + 100_000 * n          # 60 000.00 and up, in steps of 0.01
        t = T0
        last: Optional[dict] = None
        while t < T0 + minutes * MINUTE:
            draw = rng.random()
            if last is not None and draw < 0.125:
                # The best level of each side as it was, the quantities behind it changed.
                data = {"lastUpdateId": t, "bids": [], "asks": []}
                for key in ("bids", "asks"):
                    levels = [list(level) for level in last[key]]
                    for level in levels[1:]:
                        level[1] = f"{Decimal(rng.randint(1, 500_000)) / 100_000:.5f}"
                    data[key] = levels
            elif last is not None and draw < 0.25:
                # The bids as they were; the asks as they were or all a step higher.
                shift = Decimal("0.01") * rng.randint(0, 1)
                data = {"lastUpdateId": t, "bids": last["bids"],
                        "asks": [[f"{Decimal(p) + shift:.2f}", q] for p, q in last["asks"]]}
            else:
                mid += rng.randint(-3, 3)
                half = rng.randint(1, 3)
                bids, asks = [], []
                bid, ask = mid - half, mid + half
                for _ in range(market.DEPTH):
                    bids.append([f"{Decimal(bid) / 100:.2f}", f"{Decimal(rng.randint(1, 500_000)) / 100_000:.5f}"])
                    asks.append([f"{Decimal(ask) / 100:.2f}", f"{Decimal(rng.randint(1, 500_000)) / 100_000:.5f}"])
                    bid -= rng.randint(1, 4)
                    ask += rng.randint(1, 4)
                data = {"lastUpdateId": t, "bids": bids, "asks": asks}
            out.append((symbol, data, t))
            last = data
            t += rng.randint(50, 150) * 1_000_000
    out.sort(key=lambda m: (m[2], m[0]))
    return out


def best(data: dict, key: str) -> int:
    return market.to_steps(data[key][0][0], STEPS.price, "price", "test")


def expected_bars(messages: List[Message], symbol: str, written_rows: Dict[int, int],
                  from_ns: int, to_ns: int) -> List[Dict[str, Optional[Decimal]]]:
    """The bars of `symbol` as `docs/query-language.md` defines them, from the messages.

    The series' value is the mid's (bid + ask) * 500 000 - MID_PRICE's 10^6 scale - and the spread's
    ask - bid, in force from the instant of a message to the next; `OPEN` is the value at a window's
    first instant, `CLOSE` at its last, `HIGH` and `LOW` over every value in force in it, and `TWAP`
    the time-weighted mean over the window, `(sum of v * held * 10^6) // time with a value`."""
    changes = [(at, best(d, "bids"), best(d, "asks")) for s, d, at in messages if s == symbol]
    out: List[Dict[str, Optional[Decimal]]] = []
    start = from_ns
    while start <= to_ns:
        end = min(start + MINUTE - 1, to_ns)          # the window's last instant
        in_force = [c for c in changes if c[0] <= start][-1:]   # carried in from before
        inside = [c for c in changes if start < c[0] <= end]
        values = in_force + inside
        mids = [(bid + ask) * 500_000 for _, bid, ask in values]
        weighted = held_total = 0
        for i, (at, bid, ask) in enumerate(values):
            begin = max(at, start)
            finish = values[i + 1][0] if i + 1 < len(values) else end + 1
            weighted += (ask - bid) * (finish - begin)
            held_total += finish - begin
        last_bid, last_ask = values[-1][1], values[-1][2]
        out.append({
            "start_ns": Decimal(start),
            "OPEN(mid)": market.from_steps(mids[0], STEPS.price, 1_000_000) if in_force else None,
            "HIGH(mid)": market.from_steps(max(mids), STEPS.price, 1_000_000),
            "LOW(mid)": market.from_steps(min(mids), STEPS.price, 1_000_000),
            "CLOSE(mid)": market.from_steps((last_bid + last_ask) * 500_000, STEPS.price, 1_000_000),
            "TWAP(spread)": market.from_steps(weighted * 1_000_000 // held_total, STEPS.price,
                                              1_000_000),
            "rows": Decimal(sum(n for at, n in written_rows.items() if start <= at <= end)),
        })
        start += MINUTE
    return out


@pytest.fixture(scope="module")
def written(cluster):
    """Three symbols' three minutes, written the example's way through the module's own connection,
    with the rows each instant wrote per symbol."""
    node = cluster.primary()
    engine = OrderbookEngine(host="127.0.0.1", port=node.tcp_port)
    tag = secrets.token_hex(3).upper()
    symbols = [f"T{tag}{n}" for n in range(3)]
    writer = market.BookWriter({s: STEPS for s in symbols})
    messages = synthetic_depth20(symbols, seed=43)
    rows: Dict[str, Dict[int, int]] = {s: {} for s in symbols}
    unchanged = 0
    for symbol, data, at in messages:
        updates, skipped = writer.updates(symbol, data, at)
        unchanged += skipped
        if not updates:
            continue
        outcomes = engine.insert_batch(updates)
        assert all(o.ok for o in outcomes), [o.message for o in outcomes if not o.ok]
        rows[symbol][at] = rows[symbol].get(at, 0) + sum(len(u.prices) for u in updates)
    engine.flush()
    yield {"node": node, "engine": engine, "symbols": symbols, "messages": messages, "rows": rows,
           "unchanged": unchanged}
    engine.close()


def test_the_bars_from_the_engine_are_the_bars_of_the_messages(written):
    engine = written["engine"]
    from_ns, to_ns = T0, T0 + 3 * MINUTE - 1
    for symbol in written["symbols"]:
        got = bars.minute_bars(engine, symbol, from_ns, to_ns, STEPS)
        want = expected_bars(written["messages"], symbol, written["rows"][symbol], from_ns, to_ns)
        assert len(got) == 3, got
        for g, w in zip(got, want):
            assert g == w, f"{symbol} minute {(int(w['start_ns']) - T0) // MINUTE}: {g} != {w}"
    assert written["unchanged"] > 0, "no message repeated a side: the case was never exercised"


def _levels(symbol: str, data: dict) -> Dict[str, List[Tuple[int, int]]]:
    return {side: [(market.to_steps(p, STEPS.price, "price", symbol),
                    market.to_steps(q, STEPS.qty, "qty", symbol)) for p, q in data[key]]
            for side, key in (("bid", "bids"), ("ask", "asks"))}


def _book_at(engine: OrderbookEngine, symbol: str, at: int) -> Dict[str, List[Tuple[int, int]]]:
    then = engine.query_rows(f"SELECT side, level, price, quantity FROM '{symbol}'.'BINANCE' "
                             f"WHERE AT {at} AND level < {market.DEPTH}")
    return {side: [(p, q) for s, _, p, q in sorted(then.rows) if s == n]
            for n, side in ((0, "bid"), (1, "ask"))}


def test_the_live_book_is_each_symbols_last_message_level_by_level(written):
    engine = written["engine"]
    for symbol in written["symbols"]:
        _, data, _ = [m for m in written["messages"] if m[0] == symbol][-1]
        want = _levels(symbol, data)
        book = engine.book(symbol, market.EXCHANGE)
        live = {side: [(r.price, r.quantity) for r in book if r.side == side] for side in want}
        assert live == want, f"{symbol}: the live book is not the last message - a stale level?"


def test_history_at_the_instants_of_the_messages_is_those_messages(written):
    # Every sixth message of one symbol and its last: some 300 reads `WHERE AT`, which the TSan
    # battery has to fit in a test's timeout too. A sixth of the changes behind the best level -
    # the case a writer that judged a side by its best level would leave out - is still dozens.
    engine = written["engine"]
    symbol = written["symbols"][0]
    mine = [m for m in written["messages"] if m[0] == symbol]
    for n, (_, data, at) in enumerate(mine):
        if n % 6 and n != len(mine) - 1:
            continue
        assert _book_at(engine, symbol, at) == _levels(symbol, data), \
            f"{symbol}: the book AT message {n} of {len(mine)} is not that message"


def test_bars_top_of_book_reads_the_live_book(written):
    symbol = written["symbols"][0]
    _, data, at = [m for m in written["messages"] if m[0] == symbol][-1]
    top = bars.top_of_book(written["engine"], symbol, STEPS)
    assert top["bid"] == (Decimal(data["bids"][0][0]), Decimal(data["bids"][0][1]))
    assert top["ask"] == (Decimal(data["asks"][0][0]), Decimal(data["asks"][0][1]))
    assert top["as_of_ns"] == at


def _scrape(port: int) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as reply:
        assert reply.headers["Content-Type"].startswith("text/plain")
        return reply.read().decode()


def _value(text: str, name: str, symbol: Optional[str] = None) -> Optional[float]:
    want = f'{name}{{symbol="{symbol}"}} ' if symbol else f"{name} "
    for line in text.splitlines():
        if line.startswith(want):
            return float(line[len(want):])
    return None


def test_the_exporter_serves_the_engines_top_of_book(written):
    node = written["node"]
    collector = exporter.Collector(lambda: OrderbookEngine(host="127.0.0.1", port=node.tcp_port),
                                   written["symbols"] + ["NEVERWRITTEN"],
                                   {s: STEPS for s in written["symbols"] + ["NEVERWRITTEN"]}, None)
    server = exporter.serve(collector, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        text = _scrape(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
    assert _value(text, "market_engine_up") == 1.0, text
    for symbol in written["symbols"]:
        _, data, _ = [m for m in written["messages"] if m[0] == symbol][-1]
        bid, ask = Decimal(data["bids"][0][0]), Decimal(data["asks"][0][0])
        assert _value(text, "market_best_bid", symbol) == float(bid), text
        assert _value(text, "market_best_ask", symbol) == float(ask), text
        assert _value(text, "market_spread", symbol) == float(ask - bid), text
    assert 'symbol="NEVERWRITTEN"' not in text, "a symbol with no book got a series"
    for name in exporter.METRICS:
        if name in text:
            assert f"# TYPE {name} gauge" in text, name


def test_the_exporter_says_down_and_draws_no_market_without_a_node():
    closed = free_port()
    collector = exporter.Collector(lambda: OrderbookEngine(host="127.0.0.1", port=closed, timeout=2),
                                   ["BTCUSDT"], {"BTCUSDT": STEPS}, None)
    text = collector.scrape()
    assert _value(text, "market_engine_up") == 0.0, text
    assert "market_best_bid" not in text, text


def test_a_value_off_the_step_is_refused_before_anything_is_written():
    writer = market.BookWriter({"BTCUSDT": STEPS})
    good = {"bids": [["100.00", "1.00000"]], "asks": [["100.01", "1.00000"]]}
    updates, _ = writer.updates("BTCUSDT", good, 1)
    assert len(updates) == 2
    bad = {"bids": [["100.02", "1.00000"]], "asks": [["100.015", "1.00000"]]}
    with pytest.raises(ValueError, match="BTCUSDT: ask price 100.015"):
        writer.updates("BTCUSDT", bad, 2)
    # The refused message changed nothing: the bid side is still what the first one wrote.
    assert writer.written[("BTCUSDT", "bid")] == [(10000, 100000)]
    with pytest.raises(ValueError, match="not a non-negative multiple"):
        market.to_steps("-1.00", STEPS.price, "price", "BTCUSDT")
