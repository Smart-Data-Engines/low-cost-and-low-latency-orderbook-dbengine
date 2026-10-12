#!/usr/bin/env python3
"""Minute bars of one symbol and the top of its book, read from the engine.

    python bars.py [--host 127.0.0.1] [--port 9090] [--symbol BTCUSDT] [--minutes 15] [--json]

Two questions, both answered by the node rather than by anything this program kept:
- the bars: `OPEN`, `HIGH`, `LOW` and `CLOSE` of the mid and the time-weighted spread of every minute
  of the last `--minutes`, one `SELECT ... GROUP BY TIME_BUCKET(1m)`, with the rows each minute holds;
- the top of the book now: `BOOK` at depth 1, the best bid and ask with their quantities and the time
  of the update they are as of.

The engine answers counts of the symbol's steps (`market.py`); a series of the mid arrives scaled by
10^6 and a TWAP too, and the answer says so. Everything printed is in the exchange's units.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

import market
from orderbook_engine import OrderbookEngine

MINUTE_NS = 60_000_000_000
COLUMNS = ("OPEN(mid)", "HIGH(mid)", "LOW(mid)", "CLOSE(mid)", "TWAP(spread)", "COUNT(*)")


def bars_sql(symbol: str, from_ns: int, to_ns: int) -> str:
    return (f"SELECT {', '.join(COLUMNS)} FROM '{symbol}'.'{market.EXCHANGE}' "
            f"WHERE timestamp BETWEEN {from_ns} AND {to_ns} GROUP BY TIME_BUCKET(1m)")


def minute_bars(engine: OrderbookEngine, symbol: str, from_ns: int, to_ns: int,
                steps: market.Steps) -> List[Dict[str, Optional[Decimal]]]:
    """Every minute from `from_ns` to `to_ns`: its start and the five values, in exchange units
    (None where the series had none), and the rows the minute holds."""
    out: List[Dict[str, Optional[Decimal]]] = []
    for bucket in engine.query_buckets(bars_sql(symbol, from_ns, to_ns)):
        row: Dict[str, Optional[Decimal]] = {"start_ns": Decimal(bucket.start_ns)}
        for name in COLUMNS:
            value = bucket.values[name]
            if name == "COUNT(*)":
                row["rows"] = Decimal(value.value)
            elif value.is_empty:
                row[name] = None
            else:
                row[name] = market.from_steps(value.value, steps.price, value.scale)
        out.append(row)
    return out


def top_of_book(engine: OrderbookEngine, symbol: str, steps: market.Steps) -> Dict[str, object]:
    """The best bid and ask now, from the live book, in exchange units."""
    top: Dict[str, object] = {}
    for row in engine.book(symbol, market.EXCHANGE, depth=1):
        top[row.side] = (market.from_steps(row.price, steps.price),
                         market.from_steps(row.quantity, steps.qty))
        top["as_of_ns"] = row.timestamp_ns
    return top


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--minutes", type=int, default=15)
    ap.add_argument("--price-step", help="the symbol's tickSize, instead of asking Binance")
    ap.add_argument("--qty-step", help="the symbol's stepSize, instead of asking Binance")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    symbol = args.symbol.upper()
    if args.price_step and args.qty_step:
        steps = market.Steps(price=Decimal(args.price_step), qty=Decimal(args.qty_step))
    else:
        steps = market.fetch_steps([symbol])[symbol]
    now_ns = time.time_ns()
    from_ns = (now_ns // MINUTE_NS - args.minutes + 1) * MINUTE_NS
    engine = OrderbookEngine(host=args.host, port=args.port)
    try:
        bars = minute_bars(engine, symbol, from_ns, now_ns, steps)
        top = top_of_book(engine, symbol, steps)
    finally:
        engine.close()

    if args.json:
        print(json.dumps({"symbol": symbol, "bars": bars, "top": top}, default=str))
        return 0
    print(f"{symbol} on {market.EXCHANGE}, the last {args.minutes} minutes, from the engine")
    print(f"{'minute (UTC)':<17} {'open mid':>14} {'high':>14} {'low':>14} {'close':>14} "
          f"{'TWAP spread':>12} {'rows':>7}")
    for bar in bars:
        start = datetime.fromtimestamp(int(bar["start_ns"] or 0) / 1e9, tz=timezone.utc)
        cells = [bar.get(name) for name in COLUMNS[:5]]
        text = ["-" if v is None else f"{v.normalize():f}" for v in cells]
        print(f"{start:%Y-%m-%d %H:%M} {text[0]:>14} {text[1]:>14} {text[2]:>14} {text[3]:>14} "
              f"{text[4]:>12} {int(bar['rows'] or 0):>7}")
    if "bid" in top and "ask" in top:
        (bid, bid_qty), (ask, ask_qty) = top["bid"], top["ask"]  # type: ignore[misc]
        as_of = datetime.fromtimestamp(int(top["as_of_ns"]) / 1e9, tz=timezone.utc)  # type: ignore[arg-type]
        print(f"book now: bid {bid:f} x {bid_qty:f}, ask {ask:f} x {ask_qty:f}, "
              f"spread {ask - bid:f}, as of {as_of:%H:%M:%S.%f} UTC")
    else:
        print("book now: empty - nothing has been written for this symbol")
    return 0


if __name__ == "__main__":
    sys.exit(main())
