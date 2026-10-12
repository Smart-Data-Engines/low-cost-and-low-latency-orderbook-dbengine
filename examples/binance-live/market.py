"""What the three programs of this example share: an instrument's steps, a depth20 message as the
engine's updates, and the numbers a window of writes reports.

The engine stores integers. Binance sends prices and quantities as decimal text, and every symbol has
its own price step (`tickSize`) and quantity step (`stepSize`), so a value is written as a count of
steps - `Decimal(text) / step` - and read back as `count * step`. No float is involved either way, and
a value that is not a whole number of steps is refused with the symbol named rather than rounded:
it would mean the steps are not what the exchange said.

How an update is written. `<symbol>@depth20@100ms` sends the best twenty levels of each side every
100 ms. The engine keeps two views of what is written, and both have to come out right:
- the live book (`BOOK`, `SPREAD(*)`, `MID_PRICE(*)`) applies an update level by level **by price**,
  and a quantity of 0 removes the price;
- history (`SELECT ... WHERE AT`, `OPEN(mid)` and the other series) keys a row by its **position in
  the update**: level 0 is the first level written.

So a side is written as its current levels, best first - positions 0 to 19 are the book's levels -
followed by every price that was in the side the last time it was written and is not now, with a
quantity of 0, so that the live book drops it. Without those the live book keeps every price it was
ever sent, and its best bid stops being the market's. The removals come after the current levels -
at position 20 and up while the side is full, which is where no reader of the top twenty looks
(`level < 20`). Roadmap #233 is that disagreement between the two views; this example writes the one
encoding on which they agree.

A side whose levels are the same as the last time is not written again: its rows would say nothing
new, and history carries the value in force forward on its own.
"""
from __future__ import annotations

import json
import logging
import math
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from orderbook_engine import BookUpdate

log = logging.getLogger("binance-live")

EXCHANGE = "BINANCE"
DEPTH = 20
EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
DEFAULT_SYMBOLS = ("btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt,dogeusdt,adausdt,trxusdt,linkusdt,"
                   "avaxusdt")


@dataclass(frozen=True)
class Steps:
    """One instrument's steps: a price is a count of `price`, a quantity a count of `qty`."""
    price: Decimal
    qty: Decimal


def parse_symbols(text: str) -> List[str]:
    """`btcusdt, ethusdt` -> `["BTCUSDT", "ETHUSDT"]`, in order and without repeats."""
    out: List[str] = []
    for part in text.split(","):
        name = part.strip().upper()
        if name and name not in out:
            out.append(name)
    if not out:
        raise ValueError("no symbol given")
    return out


def steps_from_exchange_info(info: dict, symbols: Sequence[str]) -> Dict[str, Steps]:
    """Each symbol's steps from an `exchangeInfo` answer, refusing a symbol the answer lacks."""
    found: Dict[str, Steps] = {}
    for entry in info.get("symbols", []):
        name = entry.get("symbol", "")
        if name not in symbols:
            continue
        filters = {f.get("filterType"): f for f in entry.get("filters", [])}
        try:
            price = Decimal(filters["PRICE_FILTER"]["tickSize"]).normalize()
            qty = Decimal(filters["LOT_SIZE"]["stepSize"]).normalize()
        except (KeyError, InvalidOperation) as exc:
            raise ValueError(f"{name}: exchangeInfo has no usable PRICE_FILTER/LOT_SIZE ({exc})") from exc
        if price <= 0 or qty <= 0:
            raise ValueError(f"{name}: exchangeInfo gives a step of zero (price {price}, qty {qty})")
        found[name] = Steps(price=price, qty=qty)
    missing = [s for s in symbols if s not in found]
    if missing:
        raise ValueError(f"exchangeInfo does not list {', '.join(missing)}")
    return found


def fetch_steps(symbols: Sequence[str], timeout: float = 10.0) -> Dict[str, Steps]:
    """The steps of `symbols` from Binance's public `exchangeInfo`, once, without a key."""
    query = urllib.parse.urlencode({"symbols": json.dumps(list(symbols), separators=(",", ":"))})
    with urllib.request.urlopen(f"{EXCHANGE_INFO_URL}?{query}", timeout=timeout) as reply:
        info = json.load(reply)
    steps = steps_from_exchange_info(info, symbols)
    for name in symbols:
        log.info("steps of %s: price %s, quantity %s", name, steps[name].price, steps[name].qty)
    return steps


def to_steps(text: str, step: Decimal, what: str, symbol: str) -> int:
    """`text` as a whole number of `step`s; anything else is a ValueError naming the symbol."""
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"{symbol}: {what} {text!r} is not a number") from exc
    count = value / step
    if value < 0 or count != count.to_integral_value():
        log.warning("refused %s %s %r: not a non-negative multiple of %s", symbol, what, text, step)
        raise ValueError(f"{symbol}: {what} {text} is not a non-negative multiple of {step}")
    return int(count)


def from_steps(count: int, step: Decimal, scale: int = 1) -> Decimal:
    """A count of steps - divided by an answer's scale first - back in the exchange's units."""
    return (Decimal(count) / Decimal(scale)) * step


Levels = List[Tuple[int, int]]   # (price, quantity), both in steps, best first


def side_levels(raw: Iterable[Sequence[str]], steps: Steps, symbol: str, side: str) -> Levels:
    """A side of a depth20 message in steps, best first, without levels of quantity 0."""
    levels: Levels = []
    for entry in raw:
        price = to_steps(entry[0], steps.price, f"{side} price", symbol)
        qty = to_steps(entry[1], steps.qty, f"{side} quantity", symbol)
        if qty > 0:
            levels.append((price, qty))
    return levels


@dataclass
class BookWriter:
    """Turns depth20 messages into the engine's updates, remembering what each side was last written
    as - which is what a removal is computed against."""
    steps: Dict[str, Steps]
    written: Dict[Tuple[str, str], Levels] = field(default_factory=dict)
    _before: Dict[Tuple[str, str], Levels] = field(default_factory=dict)

    def updates(self, symbol: str, data: dict, at_ns: int) -> Tuple[List[BookUpdate], int]:
        """The updates for one message, and how many of its sides were unchanged and left out.

        Both sides are read before anything is remembered, so a message refused on its ask side
        leaves the bid side as it was. `written` is advanced as if the updates landed; a caller whose
        write failed calls `unsure()` with the same symbol before the next message."""
        steps = self.steps[symbol]
        sides = [(side, side_levels(data.get(key, []), steps, symbol, side))
                 for side, key in (("bid", "bids"), ("ask", "asks"))]
        out: List[BookUpdate] = []
        unchanged = 0
        self._before.pop((symbol, "bid"), None)
        self._before.pop((symbol, "ask"), None)
        for side, now in sides:
            before = self.written.get((symbol, side), [])
            if now == before:
                unchanged += 1
                continue
            present = {price for price, _ in now}
            gone = sorted((price for price, _ in before if price not in present),
                          reverse=(side == "bid"))
            prices = [price for price, _ in now] + gone
            qtys = [qty for _, qty in now] + [0] * len(gone)
            out.append(BookUpdate(symbol, EXCHANGE, side, prices, qtys, timestamp_ns=at_ns))
            self._before[(symbol, side)] = before
            self.written[(symbol, side)] = now
        return out, unchanged

    def unsure(self, symbol: str) -> None:
        """After a write of `symbol` that may or may not have landed: each side it wrote counts as
        holding the prices of both, so the next update removes whichever of them it no longer has.
        Removing a price the book does not hold changes nothing, and is not refused."""
        for side in ("bid", "ask"):
            before = self._before.pop((symbol, side), None)
            if before is None:
                continue
            now = self.written.get((symbol, side), [])
            prices = {price for price, _ in now}
            merged = now + [(price, qty) for price, qty in before if price not in prices]
            self.written[(symbol, side)] = sorted(merged, reverse=(side == "bid"))


def percentile(sorted_values: Sequence[float], q: float) -> Optional[float]:
    """The nearest-rank percentile of an already sorted sample, None when it is empty."""
    if not sorted_values:
        return None
    rank = max(1, min(len(sorted_values), math.ceil(q * len(sorted_values))))
    return sorted_values[rank - 1]


@dataclass
class Window:
    """What the writes of one reporting window did."""
    started: float = field(default_factory=time.monotonic)
    messages: int = 0
    updates: int = 0
    levels: int = 0
    unchanged_sides: int = 0
    refused: int = 0
    superseded: int = 0
    failed_batches: int = 0
    latencies: List[float] = field(default_factory=list)

    def report(self) -> dict:
        seconds = max(time.monotonic() - self.started, 1e-9)
        lat = sorted(self.latencies)

        def ms(value: Optional[float]) -> Optional[float]:
            return None if value is None else round(value * 1000, 3)

        return {"seconds": round(seconds, 1), "messages": self.messages,
                "messages_per_s": round(self.messages / seconds, 1),
                "levels_per_s": round(self.levels / seconds, 1), "updates": self.updates,
                "unchanged_sides": self.unchanged_sides, "refused": self.refused,
                "superseded": self.superseded, "failed_batches": self.failed_batches,
                "batch_ms_p50": ms(percentile(lat, 0.50)), "batch_ms_p99": ms(percentile(lat, 0.99)),
                "batch_ms_max": ms(lat[-1] if lat else None)}
