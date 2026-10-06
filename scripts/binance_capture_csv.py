#!/usr/bin/env python3
"""Record Binance order book streams into the comparative harness's CSV, for a storage measurement
on data nobody generated.

    scripts/binance_capture_csv.py <out.csv> [--kind diff|top20] [--minutes 10]
                                   [--symbols btcusdt,ethusdt,...]

`benchmarks/comparative/dataset.py` writes a dataset described by parameters, which is what makes it
reproducible - and a format, or a codec, chosen on it is chosen on a random walk with fixed
twenty-level updates. This writes the same columns (`ts_ns, symbol, exchange, side, level,
price_ticks, size_lots`) from a live exchange, so that the harness's adapters load it unchanged and
every system stores the same rows:

- `diff`: `<symbol>@depth@100ms`, the changes to the book every 100 ms. One update is one side of
  one message, its levels the changed price levels in the order Binance sends them; a level whose
  quantity is 0 (removed) is left out, as `tests/integration/binance_support.py` does. The time is
  the message's event time.
- `top20`: `<symbol>@depth20@100ms`, the best twenty levels of each side every 100 ms. One update is
  one side of one message, level i the i-th best price. The payload carries no time, so the time is
  the arrival's, from this machine's clock.

Prices and quantities are fixed point with eight decimals for every symbol - the scale of a system
holding many instruments in one integer column, not one chosen per symbol to look small.

The capture is a measurement input, not a fixture: a file recorded today differs from tomorrow's, so
a result names the file by its SHA-256, which this prints with the row count at the end.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import time
from decimal import Decimal
from pathlib import Path

import websockets

SCALE = Decimal(100_000_000)
DEFAULT_SYMBOLS = "btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt,dogeusdt,adausdt,trxusdt,linkusdt,avaxusdt"


def fixed(value: str) -> int:
    return int(Decimal(value) * SCALE)


async def capture(out: Path, kind: str, minutes: float, symbols: list[str]) -> tuple[int, int]:
    suffix = "@depth@100ms" if kind == "diff" else "@depth20@100ms"
    url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(s + suffix for s in symbols)
    deadline = time.monotonic() + minutes * 60
    rows = messages = 0
    with out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ts_ns", "symbol", "exchange", "side", "level", "price_ticks", "size_lots"])
        async with websockets.connect(url, open_timeout=15, max_size=2**22) as ws:
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.monotonic()))
                except asyncio.TimeoutError:
                    break
                arrival_ns = time.time_ns()
                msg = json.loads(raw)
                data = msg.get("data", {})
                symbol = msg.get("stream", "").split("@")[0].upper()
                if kind == "diff":
                    if data.get("e") != "depthUpdate":
                        continue
                    ts = int(data["E"]) * 1_000_000
                    sides = (("bid", data.get("b", [])), ("ask", data.get("a", [])))
                else:
                    ts = arrival_ns
                    sides = (("bid", data.get("bids", [])), ("ask", data.get("asks", [])))
                messages += 1
                for side, levels in sides:
                    level = 0
                    for price, qty in levels:
                        if Decimal(qty) == 0:
                            continue
                        writer.writerow([ts, symbol, "BINANCE", side, level, fixed(price), fixed(qty)])
                        level += 1
                        rows += 1
    return rows, messages


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("out", type=Path)
    ap.add_argument("--kind", choices=("diff", "top20"), default="diff")
    ap.add_argument("--minutes", type=float, default=10.0)
    ap.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    args = ap.parse_args()
    symbols = [s.strip().lower() for s in args.symbols.split(",") if s.strip()]
    rows, messages = asyncio.run(capture(args.out, args.kind, args.minutes, symbols))
    digest = hashlib.sha256(args.out.read_bytes()).hexdigest()
    print(json.dumps({"file": str(args.out), "kind": args.kind, "minutes": args.minutes,
                      "symbols": len(symbols), "messages": messages, "rows": rows, "sha256": digest}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
