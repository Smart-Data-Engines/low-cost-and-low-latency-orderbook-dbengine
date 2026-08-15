"""Shared gate and helpers for the Binance live tests.

Live tests against a third-party exchange must never be able to fail a suite that
someone runs offline, on a locked-down network, or without the optional websockets
dependency. So they are opt-in and skip at module level with a reason that names
which precondition is missing — a skip that says only "skipped" is indistinguishable
from a test nobody wrote.

Not a test module: no test_ prefix, so pytest does not collect it.
"""
from __future__ import annotations

import json
import os
import socket
import time
from typing import Iterator

import pytest

BINANCE_HOST = "stream.binance.com"
BINANCE_PORT = 9443
BINANCE_WS_URL = f"wss://{BINANCE_HOST}:{BINANCE_PORT}/ws/btcusdt@depth"

# Binance quotes BTC in USD with 2 decimals; the engine stores integer sub-units.
PRICE_SCALE = 100
QTY_SCALE = 100_000_000  # 8 decimals, enough for BTC quantities

# A sanity band for BTC/USDT, wide enough to stay valid for years and narrow enough
# that a parsing or scaling mistake fails the test rather than passing quietly.
PLAUSIBLE_BTC_MIN = 1_000.0
PLAUSIBLE_BTC_MAX = 10_000_000.0


def require_binance() -> None:
    """Skip the whole module unless a live run was asked for and is possible."""
    if not os.environ.get("OB_BINANCE_TESTS"):
        pytest.skip(
            "live Binance tests are opt-in: set OB_BINANCE_TESTS=1 to run them "
            "(they need outbound network access to stream.binance.com:9443)",
            allow_module_level=True)

    try:
        import websockets.sync.client  # noqa: F401
    except ImportError:
        pytest.skip(
            "the websockets package is not installed, so a live depth stream cannot "
            'be opened: pip install websockets',
            allow_module_level=True)

    try:
        with socket.create_connection((BINANCE_HOST, BINANCE_PORT), timeout=5):
            pass
    except OSError as exc:
        pytest.skip(
            f"{BINANCE_HOST}:{BINANCE_PORT} is unreachable ({exc}); a live exchange "
            f"being down or firewalled is not a defect in this engine",
            allow_module_level=True)


def stream_depth_updates(duration_s: float,
                         max_updates: int = 500) -> Iterator[dict]:
    """Yield parsed depth updates from Binance for up to `duration_s` seconds."""
    import websockets.sync.client

    deadline = time.monotonic() + duration_s
    seen = 0
    with websockets.sync.client.connect(BINANCE_WS_URL, open_timeout=15) as ws:
        while time.monotonic() < deadline and seen < max_updates:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                raw = ws.recv(timeout=min(10.0, remaining))
            except TimeoutError:
                break
            try:
                msg = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if msg.get("e") != "depthUpdate":
                continue
            seen += 1
            yield msg


def levels_from(msg: dict, side: str) -> list[tuple[int, int]]:
    """Convert one side of a depth update into (price, qty) in engine units.

    Binance sends a quantity of 0 to mean "this level is gone". Those are dropped:
    the engine stores rows, not a mutable book, so writing a zero-quantity row would
    record a level that never existed at that size.
    """
    key = "b" if side == "bid" else "a"
    out: list[tuple[int, int]] = []
    for price_str, qty_str in msg.get(key, []):
        price = float(price_str)
        qty = float(qty_str)
        if qty <= 0:
            continue
        out.append((int(round(price * PRICE_SCALE)),
                    max(1, int(round(qty * QTY_SCALE)))))
    return out


def price_to_usd(engine_price: int) -> float:
    return engine_price / PRICE_SCALE
