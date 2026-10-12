#!/usr/bin/env python3
"""Binance's best twenty levels of each side, every 100 ms, written into an engine node as they come.

    python ingest.py [--host 127.0.0.1] [--port 9090] [--symbols btcusdt,ethusdt,...]
                     [--duration SECONDS]

One websocket carries `<symbol>@depth20@100ms` for every symbol. Each message is one `insert_batch`
of the sides that changed (`market.py` says how a side is written and why), stamped with the time the
message arrived here, since the stream carries none. Every ten seconds one line says what the window
did: messages and levels a second, the batch's latency at p50, p99 and its worst, sides left out as
unchanged, refusals, and failed batches. The last line, at the end of `--duration` or on Ctrl-C, is
the run's totals as JSON.

Receiving and writing are two threads. A write does not hold up the socket: what waits for the writer
is the newest message of each symbol, and a newer message replaces an older one not yet written -
depth20 is the whole top of the book, so the older one says nothing the newer does not. That is
counted (`superseded`), and it stays 0 while the node keeps up.

A dropped websocket is reconnected, and so is the node, each after 1, 2, 4 ... up to 30 seconds. A
batch whose connection failed is sent once more on the next connection; it may already have landed,
and writing a depth20 side again only repeats rows of the same instant. Nothing is dropped without a
count: a value that is not a whole number of steps refuses its message, and the node's refusal of an
update is logged with the reason it gave.
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
import time
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

import market
from orderbook_engine import OrderbookEngine, OrderbookError

log = logging.getLogger("binance-live")

STREAM_URL = "wss://stream.binance.com:9443/stream?streams="
REPORT_EVERY = 10.0
BACKOFF_MAX = 30.0


class Pending:
    """The newest unwritten message of each symbol, symbols in the order they first waited."""

    def __init__(self) -> None:
        self._cv = threading.Condition()
        self._latest: Dict[str, Tuple[dict, int]] = {}
        self._order: Deque[str] = deque()
        self.superseded = 0

    def put(self, symbol: str, data: dict, at_ns: int) -> None:
        with self._cv:
            if symbol in self._latest:
                self.superseded += 1
            else:
                self._order.append(symbol)
            self._latest[symbol] = (data, at_ns)
            self._cv.notify()

    def take(self, timeout: float) -> Optional[Tuple[str, dict, int]]:
        with self._cv:
            if not self._order:
                self._cv.wait(timeout)
            if not self._order:
                return None
            symbol = self._order.popleft()
            data, at_ns = self._latest.pop(symbol)
            return symbol, data, at_ns

    def take_superseded(self) -> int:
        with self._cv:
            count, self.superseded = self.superseded, 0
            return count


class Totals:
    """The run's sums, kept beside each window's report."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.window = market.Window()
        self.sums: Dict[str, int] = {}
        self.ws_reconnects = 0
        self.engine_reconnects = 0

    def close_window(self, pending: Pending) -> dict:
        with self.lock:
            self.window.superseded += pending.take_superseded()
            report = self.window.report()
            for key in ("messages", "updates", "unchanged_sides", "refused", "superseded",
                        "failed_batches"):
                self.sums[key] = self.sums.get(key, 0) + report[key]
            self.sums["levels"] = self.sums.get("levels", 0) + self.window.levels
            self.window = market.Window()
            return report


def connect_engine(host: str, port: int, stop: threading.Event) -> Optional[OrderbookEngine]:
    """A connection to the node, retried with backoff until it opens or `stop` is set."""
    backoff = 1.0
    while not stop.is_set():
        try:
            engine = OrderbookEngine(host=host, port=port)
            status = engine.status()
            log.info("connected to the node at %s:%d: version %s, %s", host, port,
                     status.get("version", "?"), status.get("role", "?"))
            return engine
        except (OrderbookError, OSError) as exc:
            log.warning("cannot reach the node at %s:%d (%s); again in %.0f s", host, port, exc,
                        backoff)
            stop.wait(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
    return None


def write_loop(host: str, port: int, book: market.BookWriter, pending: Pending, totals: Totals,
               stop: threading.Event) -> None:
    """Takes each symbol's newest message and writes the sides that changed, until `stop`."""
    engine = connect_engine(host, port, stop)
    while engine is not None and not stop.is_set():
        item = pending.take(timeout=0.2)
        if item is None:
            continue
        symbol, data, at_ns = item
        try:
            updates, unchanged = book.updates(symbol, data, at_ns)
        except ValueError:
            with totals.lock:
                totals.window.messages += 1
                totals.window.refused += 1
            continue
        with totals.lock:
            totals.window.messages += 1
            totals.window.unchanged_sides += unchanged
        if not updates:
            continue
        for attempt in (1, 2):
            started = time.perf_counter()
            try:
                outcomes = engine.insert_batch(updates)
            except (OrderbookError, OSError) as exc:
                log.warning("a batch of %s failed on its connection (%s); attempt %d of 2", symbol,
                            exc, attempt)
                with totals.lock:
                    totals.window.failed_batches += 1
                    totals.engine_reconnects += 1
                try:
                    engine.close()
                except (OrderbookError, OSError):
                    pass
                engine = connect_engine(host, port, stop)
                if engine is None:
                    return
                continue
            elapsed = time.perf_counter() - started
            refused = [o for o in outcomes if not o.ok]
            with totals.lock:
                totals.window.latencies.append(elapsed)
                totals.window.updates += len(updates) - len(refused)
                totals.window.levels += sum(len(u.prices) for u in updates)
                totals.window.refused += len(refused)
            for outcome in refused[:5]:
                update = updates[outcome.index]
                log.warning("the node refused %s %s: %s", update.symbol, update.side, outcome.message)
            break
        else:
            book.unsure(symbol)
    if engine is not None:
        engine.close()


def receive_loop(symbols: List[str], pending: Pending, totals: Totals, stop: threading.Event) -> None:
    """The websocket, reconnected with backoff, until `stop`."""
    from websockets.sync.client import connect   # imported here: the tests never open a socket

    url = STREAM_URL + "/".join(s.lower() + "@depth20@100ms" for s in symbols)
    backoff = 1.0
    while not stop.is_set():
        try:
            with connect(url, open_timeout=15, close_timeout=2, max_size=2**22) as ws:
                log.info("receiving %s from Binance", ", ".join(symbols))
                backoff = 1.0
                while not stop.is_set():
                    try:
                        raw = ws.recv(timeout=1.0)
                    except TimeoutError:
                        continue
                    at_ns = time.time_ns()
                    msg = json.loads(raw)
                    symbol = msg.get("stream", "").split("@")[0].upper()
                    if symbol in symbols:
                        pending.put(symbol, msg.get("data", {}), at_ns)
        except Exception as exc:  # noqa: BLE001 - any failure of the socket ends in a reconnect
            if stop.is_set():
                break
            with totals.lock:
                totals.ws_reconnects += 1
            log.warning("the websocket dropped (%s: %s); reconnecting in %.0f s",
                        type(exc).__name__, exc, backoff)
            stop.wait(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--symbols", default=market.DEFAULT_SYMBOLS)
    ap.add_argument("--duration", type=float, default=0.0, help="seconds; 0 runs until Ctrl-C")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    symbols = market.parse_symbols(args.symbols)
    book = market.BookWriter(market.fetch_steps(symbols))
    pending, totals, stop = Pending(), Totals(), threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    writer = threading.Thread(target=write_loop, name="writer",
                              args=(args.host, args.port, book, pending, totals, stop))
    receiver = threading.Thread(target=receive_loop, name="receiver",
                                args=(symbols, pending, totals, stop), daemon=True)
    writer.start()
    receiver.start()
    started = time.monotonic()
    while not stop.wait(REPORT_EVERY):
        log.info("window %s", json.dumps(totals.close_window(pending)))
        if args.duration and time.monotonic() - started >= args.duration:
            stop.set()
    writer.join(timeout=10)
    totals.close_window(pending)
    seconds = time.monotonic() - started
    print(json.dumps({"seconds": round(seconds, 1), "symbols": len(symbols),
                      "levels_per_s": round(totals.sums.get("levels", 0) / max(seconds, 1e-9), 1),
                      "ws_reconnects": totals.ws_reconnects,
                      "engine_reconnects": totals.engine_reconnects, **totals.sums}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
