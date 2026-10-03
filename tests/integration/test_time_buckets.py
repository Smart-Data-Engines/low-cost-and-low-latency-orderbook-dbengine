"""#44, step 1: aggregates over time buckets through the server and the Python client.

`SELECT <aggregates> FROM ... GROUP BY TIME_BUCKET(<interval>)` answers one row per bucket that holds
a row: its start, then each aggregate with its scale in the header. These compare every bucket with
the same aggregation done on the client over the rows a plain SELECT returns - which is what a
client had to do before, fetching every row to get a few hundred numbers.
"""
from __future__ import annotations

import subprocess
import time

import pytest

from conftest import free_port, patience, raw_query, send_command, server_binary_path
from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.aggregations

SEC = 1_000_000_000
BASE = 20_719 * 86_400 * SEC   # a whole UTC day


@pytest.fixture
def written(primary_client: OrderbookEngine):
    """Rows of one symbol over 30 seconds, both sides and two levels, quantities from zero."""
    symbol = f"BKT-{free_port()}"
    rows = []
    for i in range(120):
        ts = BASE + i * 250_000_000
        side = "bid" if i % 3 else "ask"
        price = 1_000 + (i * 37) % 200 - 50
        qty = i % 7
        primary_client.insert(symbol, "EX", side, [price], [qty], timestamp_ns=ts)
        rows.append((ts, price, qty, 0 if side == "bid" else 1))
    primary_client.flush()
    return symbol, rows


def by_client(rows, width):
    """The buckets the client used to compute itself: start -> (count, first, last, sum_qty, vwap)."""
    out = {}
    for ts, price, qty, _side in sorted(rows, key=lambda r: r[0]):
        start = ts - ts % width
        b = out.setdefault(start, {"n": 0, "first": price, "last": price, "q": 0, "pq": 0})
        b["n"] += 1
        b["last"] = price
        b["q"] += qty
        b["pq"] += price * qty
    return out


@pytest.mark.parametrize("interval,width", [("1s", SEC), ("7s", 7 * SEC), ("1m", 60 * SEC)])
def test_every_bucket_is_the_clients_own_aggregation(primary_client, written, interval, width):
    symbol, rows = written
    buckets = primary_client.query_buckets(
        f"SELECT COUNT(*), FIRST(price), LAST(price), SUM(quantity), VWAP(price) "
        f"FROM '{symbol}'.'EX' GROUP BY TIME_BUCKET({interval})")
    want = by_client(rows, width)
    assert [b.start_ns for b in buckets] == sorted(want), "buckets missing, extra or out of order"
    for b in buckets:
        w = want[b.start_ns]
        assert b.values["COUNT(*)"].value == w["n"]
        assert b.values["FIRST(price)"].value == w["first"]
        assert b.values["LAST(price)"].value == w["last"]
        assert b.values["SUM(quantity)"].value == w["q"]
        vwap = b.values["VWAP(price)"]
        assert vwap.scale == 1_000_000
        if w["q"] == 0:
            assert vwap.value is None, "a bucket with nothing to weigh by answered a VWAP"
        else:
            # Truncated at the scale; every price here is positive, so that is the floor.
            assert vwap.value == (w["pq"] * 1_000_000) // w["q"], (b.start_ns, vwap, w)


def test_the_top_of_the_book_in_minute_bars(primary_client, written):
    """What #44 is for: conditions narrow the rows - the bids at the best level - instead of being
    refused as they are beside a function of the live book."""
    symbol, rows = written
    bars = primary_client.query_buckets(
        f"SELECT COUNT(*), MAX(price), MIN(price) FROM '{symbol}'.'EX' "
        f"WHERE side = 0 AND level = 0 GROUP BY TIME_BUCKET(1m)")
    bids = [r for r in rows if r[3] == 0]
    assert len(bars) == 1
    assert bars[0].values["COUNT(*)"].value == len(bids)
    assert bars[0].values["MAX(price)"].value == max(r[1] for r in bids)
    assert bars[0].values["MIN(price)"].value == min(r[1] for r in bids)


def test_limit_answers_the_first_buckets(primary_client, written):
    symbol, _rows = written
    first = primary_client.query_buckets(
        f"SELECT COUNT(*) FROM '{symbol}'.'EX' GROUP BY TIME_BUCKET(1s) LIMIT 3")
    assert [b.start_ns for b in first] == [BASE, BASE + SEC, BASE + 2 * SEC]


def test_the_wire_answer_carries_each_columns_scale(cluster, written):
    symbol, _rows = written
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT COUNT(*), AVG(quantity) FROM '{symbol}'.'EX' GROUP BY TIME_BUCKET(10s)")
    assert lines[0] == "OK"
    assert lines[1] == "bucket_ns\tCOUNT(*)/1\tAVG(quantity)/1000000"
    assert len(lines) == 2 + 3, lines   # 30 s of rows in 10 s buckets


def test_a_query_that_is_not_a_bucket_query_says_what_is_wrong(cluster, written):
    symbol, _rows = written
    port = cluster.primary().tcp_port
    cases = {
        f"SELECT COUNT(*) FROM '{symbol}'.'EX'": "AGG_NEEDS_BUCKET",
        f"SELECT price FROM '{symbol}'.'EX' GROUP BY TIME_BUCKET(1s)": "is not one",
        f"SELECT SPREAD(*) FROM '{symbol}'.'EX' GROUP BY TIME_BUCKET(1s)": "aggregates the live book",
        f"SELECT COUNT(*) FROM '{symbol}'.'EX' GROUP BY TIME_BUCKET(1y)": "unknown unit 'y'",
    }
    for sql, says in cases.items():
        answer = send_command(port, sql).strip()
        assert answer.startswith("ERR") and says in answer, (sql, answer)


def test_the_row_api_refuses_a_bucket_answer_by_name(primary_client, written):
    symbol, _rows = written
    with pytest.raises(OrderbookError, match="query_buckets"):
        primary_client.query(f"SELECT COUNT(*) FROM '{symbol}'.'EX' GROUP BY TIME_BUCKET(1s)")


def test_past_the_ceiling_a_query_is_refused_not_cut_short(tmp_path):
    port = free_port()
    proc = subprocess.Popen([server_binary_path(), "--port", str(port), "--data-dir", str(tmp_path / "d"),
                             "--metrics-port", "0", "--max-query-buckets", "3"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + patience(20)
        while True:
            try:
                if send_command(port, "PING").startswith("PONG"):
                    break
            except OSError:
                pass
            assert proc.poll() is None, f"the node exited with {proc.returncode}"
            assert time.monotonic() < deadline, "the node never answered PING"
            time.sleep(0.1)
        engine = OrderbookEngine(host="127.0.0.1", port=port, timeout=patience(20))
        try:
            for i in range(4):
                engine.insert("BKT-CEIL", "EX", "bid", [100], [1], timestamp_ns=BASE + i * SEC)
            engine.flush()
            assert len(engine.query_buckets(
                "SELECT COUNT(*) FROM 'BKT-CEIL'.'EX' GROUP BY TIME_BUCKET(2s)")) == 2
            with pytest.raises(OrderbookError, match="BUCKETS_TOO_MANY: more than 3 buckets"):
                engine.query_buckets("SELECT COUNT(*) FROM 'BKT-CEIL'.'EX' GROUP BY TIME_BUCKET(1s)")
        finally:
            engine.close()
    finally:
        proc.terminate()
        proc.wait(timeout=patience(20))


def test_the_ceiling_is_in_the_printed_configuration(tmp_path):
    out = subprocess.run([server_binary_path(), "--data-dir", str(tmp_path / "d"), "--max-query-buckets", "7",
                          "--print-config"], capture_output=True, text=True, timeout=patience(20))
    assert out.returncode == 0, out.stderr
    line = next((l for l in out.stdout.splitlines() if l.split()[:1] == ["max-query-buckets"]), "")
    assert line.split()[1:2] == ["7"], out.stdout
