#!/usr/bin/env python3
"""The market as the engine holds it, for Prometheus: what the dashboard's market panels draw.

    python exporter.py [--host 127.0.0.1] [--port 9090] [--listen 127.0.0.1:9101]
                       [--symbols btcusdt,...] [--data-dir /var/lib/orderbook]

Every scrape of `/metrics` asks the node for the top of each symbol's live book (`BOOK`, depth 1) and
answers it in the exchange's units: the best bid and ask, the mid and the spread, the quantities at
the best prices, and how old the update is that the book is as of. Nothing is kept between scrapes -
a value on the dashboard was read from the engine at most one scrape interval earlier, which is the
point of drawing the market from the engine rather than from the feed.

`--data-dir`, given the node's data directory on the same host, adds the bytes it holds on disk
(allocated blocks, recursively), the panel that shows what the stream costs to keep.

A node that does not answer is `market_engine_up 0` and no symbol's series: a last value repeated
would draw a market that stopped as one that is quiet.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, List, Optional, Tuple

import market
from orderbook_engine import OrderbookEngine, OrderbookError

log = logging.getLogger("binance-live")

# Every metric this exporter writes, with its help text: `/metrics` writes `# HELP` and `# TYPE` from
# here, and scripts/check_dashboards.py reads these names to hold the dashboard to them.
METRICS: Dict[str, str] = {
    "market_best_bid": "Best bid of the symbol's live book in the engine, in the exchange's units.",
    "market_best_ask": "Best ask of the symbol's live book in the engine, in the exchange's units.",
    "market_best_bid_qty": "Quantity at the best bid, in the exchange's units.",
    "market_best_ask_qty": "Quantity at the best ask, in the exchange's units.",
    "market_mid": "Mid of the best bid and ask, in the exchange's units.",
    "market_spread": "Best ask minus best bid, in the exchange's units.",
    "market_book_age_seconds": "Seconds since the update the live book is as of.",
    "market_engine_up": "1 when the node answered this scrape, 0 when it did not.",
    "market_scrape_seconds": "Seconds this scrape took, the node's answers included.",
    "market_data_dir_bytes": "Bytes allocated on disk under the node's data directory.",
}


def data_dir_bytes(path: str) -> int:
    """Allocated bytes under `path`, files that vanish while it is walked left out."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_blocks * 512
            except FileNotFoundError:
                continue
    return total


Sample = Tuple[str, Dict[str, str], float]


def market_samples(engine: OrderbookEngine, symbols: List[str], steps: Dict[str, market.Steps],
                   now_ns: int) -> List[Sample]:
    """The market metrics of every symbol with both sides in the node's live book."""
    out: List[Sample] = []
    for symbol in symbols:
        try:
            rows = engine.book(symbol, market.EXCHANGE, depth=1)
        except OrderbookError as exc:
            if "not found" in str(exc).lower():   # nothing written for it yet: no series, as yet
                continue
            raise
        best = {row.side: row for row in rows}
        if "bid" not in best or "ask" not in best:
            continue
        s = steps[symbol]
        bid = market.from_steps(best["bid"].price, s.price)
        ask = market.from_steps(best["ask"].price, s.price)
        labels = {"symbol": symbol}
        out += [
            ("market_best_bid", labels, float(bid)),
            ("market_best_ask", labels, float(ask)),
            ("market_best_bid_qty", labels, float(market.from_steps(best["bid"].quantity, s.qty))),
            ("market_best_ask_qty", labels, float(market.from_steps(best["ask"].quantity, s.qty))),
            ("market_mid", labels, float((bid + ask) / Decimal(2))),
            ("market_spread", labels, float(ask - bid)),
            ("market_book_age_seconds", labels, max(0, now_ns - best["bid"].timestamp_ns) / 1e9),
        ]
    return out


def exposition(samples: List[Sample]) -> str:
    """Prometheus's text format, a metric's HELP and TYPE once before its samples."""
    lines: List[str] = []
    for name in METRICS:
        mine = [s for s in samples if s[0] == name]
        if not mine:
            continue
        lines.append(f"# HELP {name} {METRICS[name]}")
        lines.append(f"# TYPE {name} gauge")
        for _, labels, value in mine:
            text = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
            lines.append(f"{name}{{{text}}} {value!r}" if text else f"{name} {value!r}")
    return "\n".join(lines) + "\n"


class Collector:
    """One scrape's answer, over a connection to the node that is reopened after a failure."""

    def __init__(self, connect: Callable[[], OrderbookEngine], symbols: List[str],
                 steps: Dict[str, market.Steps], data_dir: Optional[str]) -> None:
        self._connect = connect
        self._symbols = symbols
        self._steps = steps
        self._data_dir = data_dir
        self._engine: Optional[OrderbookEngine] = None
        self._was_up: Optional[bool] = None
        self._lock = threading.Lock()

    def scrape(self) -> str:
        started = time.perf_counter()
        with self._lock:
            samples: List[Sample] = []
            up = True
            try:
                if self._engine is None:
                    self._engine = self._connect()
                samples = market_samples(self._engine, self._symbols, self._steps, time.time_ns())
            except (OrderbookError, OSError) as exc:
                up = False
                samples = []
                if self._engine is not None:
                    try:
                        self._engine.close()
                    except (OrderbookError, OSError):
                        pass
                self._engine = None
                if self._was_up is not False:
                    log.warning("the node does not answer (%s); reporting it down", exc)
            if up and self._was_up is False:
                log.info("the node answers again")
            self._was_up = up
            samples.append(("market_engine_up", {}, 1.0 if up else 0.0))
            if self._data_dir:
                samples.append(("market_data_dir_bytes", {}, float(data_dir_bytes(self._data_dir))))
            samples.append(("market_scrape_seconds", {}, time.perf_counter() - started))
            return exposition(samples)


def serve(collector: Collector, host: str, port: int) -> ThreadingHTTPServer:
    """`/metrics` on host:port; the server is returned unstarted, for `serve_forever()`."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - the name http.server calls
            if self.path.split("?")[0] != "/metrics":
                self.send_error(404, "only /metrics is served here")
                return
            body = collector.scrape().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: object) -> None:
            log.debug("scrape from %s: " + fmt, self.address_string(), *args)

    return ThreadingHTTPServer((host, port), Handler)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--listen", default="127.0.0.1:9101")
    ap.add_argument("--symbols", default=market.DEFAULT_SYMBOLS)
    ap.add_argument("--data-dir", help="the node's data directory, for market_data_dir_bytes")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    symbols = market.parse_symbols(args.symbols)
    steps = market.fetch_steps(symbols)
    collector = Collector(lambda: OrderbookEngine(host=args.host, port=args.port), symbols, steps,
                          args.data_dir)
    listen_host, _, listen_port = args.listen.rpartition(":")
    server = serve(collector, listen_host, int(listen_port))
    log.info("serving /metrics on %s for %s, from the node at %s:%d", args.listen,
             ", ".join(symbols), args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
