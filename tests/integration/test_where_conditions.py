"""What a WHERE condition answers, over the wire (#199).

Every comparison set one end of a range and nothing else: `=` meant `>=`, `>` and `<` kept the value
they exclude, a second condition on a column replaced the first, a SNAPSHOT ignored its price and
time conditions, and a subscription ignored `AT` and pushed every row. `tests/test_query_conditions.cpp`
runs the engine over a store; these ask a running server, through the command line a client sends.
"""
from __future__ import annotations

import hashlib
import socket
import time

import pytest

from orderbook_engine import OrderbookEngine

from conftest import raw_query

pytestmark = pytest.mark.smoke

EXCHANGE = "BINANCE"
LOW, MID, HIGH = 1_000_000, 2_000_000, 3_000_000


def own_symbol(request) -> str:
    """A book of this test's own: storage is append-only and `cluster` is session-scoped."""
    return f"WHERE-{hashlib.sha1(request.node.name.encode()).hexdigest()[:8]}"


@pytest.fixture
def three(cluster, primary_client: OrderbookEngine, request) -> str:
    """Three flushed rows at three prices, written one at a time, so each has a time of its own."""
    symbol = own_symbol(request)
    for price in (LOW, MID, HIGH):
        primary_client.insert(symbol, EXCHANGE, "bid", [price], [5])
    primary_client.flush()
    return symbol


def rows(cluster, symbol: str, where: str) -> list[tuple[int, int]]:
    """(timestamp_ns, price) of each row a `SELECT timestamp, price ... WHERE <where>` answers."""
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT timestamp, price FROM '{symbol}'.'{EXCHANGE}' WHERE {where}")
    assert lines and lines[0] == "OK", f"{where}: {lines}"
    assert lines[1].split("\t") == ["timestamp_ns", "price"], lines
    return [(int(ts), int(px)) for ts, px in (ln.split("\t") for ln in lines[2:])]


def prices(cluster, symbol: str, where: str) -> list[int]:
    return sorted(px for _, px in rows(cluster, symbol, where))


def test_equals_is_one_price(cluster, three):
    assert prices(cluster, three, f"price = {MID}") == [MID], "= answered as >="


def test_a_strict_comparison_leaves_its_bound_out(cluster, three):
    assert prices(cluster, three, f"price > {MID}") == [HIGH]
    assert prices(cluster, three, f"price < {MID}") == [LOW]


def test_after_the_last_row_read_is_not_that_row(cluster, three):
    """`timestamp > t` with `t` the time of a row already read: how a client polls for what is new.
    Keeping `t` handed that row back on every poll."""
    every = sorted(rows(cluster, three, "timestamp >= 0"))
    assert len(every) == 3 and len({ts for ts, _ in every}) == 3, (
        f"three rows with three times are this test's premise: {every}")
    first_ts = every[0][0]
    after = rows(cluster, three, f"timestamp > {first_ts}")
    assert sorted(after) == every[1:], f"timestamp > {first_ts} answered {after}"


def test_conditions_on_one_column_narrow_each_other(cluster, three):
    assert prices(cluster, three, f"price >= {HIGH} AND price >= {LOW}") == [HIGH]
    assert prices(cluster, three, f"price BETWEEN {LOW} AND {MID} AND price BETWEEN {MID} AND {HIGH}") == [MID]
    assert prices(cluster, three, f"price = {LOW} AND price = {HIGH}") == []


def test_a_snapshot_keeps_the_levels_of_its_book_priced_in_range(cluster, primary_client, request):
    symbol = own_symbol(request)
    primary_client.insert(symbol, EXCHANGE, "bid", [HIGH, MID, LOW], [5, 6, 7])
    primary_client.flush()
    at = "AT 18446744073709551615"
    every = raw_query(cluster.primary().tcp_port, f"SELECT price FROM '{symbol}'.'{EXCHANGE}' WHERE {at}")
    assert every[0] == "OK" and sorted(int(p) for p in every[2:]) == [LOW, MID, HIGH], every
    band = raw_query(cluster.primary().tcp_port,
                     f"SELECT price FROM '{symbol}'.'{EXCHANGE}' WHERE {at} AND price BETWEEN {LOW + 1} AND {HIGH - 1}")
    assert band[0] == "OK" and [int(p) for p in band[2:]] == [MID], f"the price condition was ignored: {band}"


def test_a_snapshot_refuses_a_timestamp_condition(cluster, three):
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT * FROM '{three}'.'{EXCHANGE}' WHERE AT 18446744073709551615 AND timestamp >= 0")
    assert lines and lines[0].startswith("ERR") and "SNAPSHOT_TIME_FILTER" in lines[0], lines


def subscribe_and_write(port: int, symbol: str, where: str, prices_written: list[int]) -> tuple[str, list[str]]:
    """Subscribe with `where`, write one MINSERT of `prices_written` from another connection, and
    return the acknowledgement and every PUSH line that arrived."""
    sub = socket.create_connection(("127.0.0.1", port), timeout=8)
    writer = socket.create_connection(("127.0.0.1", port), timeout=8)
    try:
        sub.recv(4096)
        writer.recv(4096)
        sub.sendall(f"SUBSCRIBE * FROM '{symbol}'.'{EXCHANGE}' WHERE {where}\n".encode())
        sub.settimeout(2.0)
        ack = sub.recv(4096).decode(errors="replace")
        if not ack.startswith("OK SUB"):
            return ack, []
        body = "\n".join(f"{p} 5 1" for p in prices_written)
        writer.sendall(f"MINSERT {symbol} {EXCHANGE} bid {len(prices_written)}\n{body}\n".encode())
        received = b""
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            sub.settimeout(0.3)
            try:
                chunk = sub.recv(1 << 20)
            except socket.timeout:
                continue
            if not chunk:
                break
            received += chunk
        return ack, [ln for ln in received.decode(errors="replace").splitlines() if ln.startswith("PUSH ")]
    finally:
        sub.close()
        writer.close()


def test_a_subscription_pushes_only_what_its_condition_allows(cluster, request):
    ack, pushed = subscribe_and_write(cluster.primary().tcp_port, own_symbol(request),
                                      f"price > {MID}", [HIGH, MID, LOW])
    assert ack.startswith("OK SUB"), ack
    assert [int(line.split("\t")[2]) for line in pushed] == [HIGH], f"pushed {pushed}"


def test_a_subscription_refuses_at(cluster, request):
    ack, pushed = subscribe_and_write(cluster.primary().tcp_port, own_symbol(request), "AT 5", [MID])
    assert ack.startswith("ERR"), f"a subscription with AT was accepted: {ack!r}"
    assert pushed == []


def test_side_and_level_narrow_rows(cluster, primary_client, request):
    """#200: `side` and `level` are conditions as time and price are."""
    symbol = own_symbol(request)
    primary_client.insert(symbol, EXCHANGE, "bid", [HIGH, MID], [5, 6])
    primary_client.insert(symbol, EXCHANGE, "ask", [HIGH + 1, HIGH + 2], [7, 8])
    primary_client.flush()
    assert prices(cluster, symbol, "side = 1") == [HIGH + 1, HIGH + 2]
    assert prices(cluster, symbol, "side = 0 AND level = 0") == [HIGH], "the top of the bids"
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT price FROM '{symbol}'.'{EXCHANGE}' WHERE AT 18446744073709551615 AND level = 0")
    assert lines[0] == "OK" and [int(p) for p in lines[2:]] == [HIGH, HIGH + 1], lines


def test_a_side_past_its_type_is_refused(cluster, three):
    lines = raw_query(cluster.primary().tcp_port, f"SELECT * FROM '{three}'.'{EXCHANGE}' WHERE side = 256")
    assert lines and lines[0].startswith("ERR") and "out of range for side" in lines[0], lines
