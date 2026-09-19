"""The response carries the columns the query asked for (#139).

The row path used to parse a select list, check the names in it, and never read it again, so
every row query was `SELECT *`. A client that asked for one column got seven and had no way to
tell, because the header it was handed named seven and was correct about the bytes under it.

Over the raw protocol on purpose: both of our clients read a row **by position**, so neither can
read a narrowed answer, and the tests for that are the refusals at the bottom of this file.
"""
from __future__ import annotations

import socket
import time

import pytest

from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.smoke

SYMBOL = "PROJ-WIRE"
EXCHANGE = "BINANCE"
PRICE, QTY = 6_500_000, 150


def raw_query(port: int, sql: str, timeout: float = 6.0) -> list[str]:
    """Send one query over a bare socket and return its non-empty lines."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.recv(4096)  # banner
        sock.sendall((sql + "\n").encode())
        buffered = b""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            chunk = sock.recv(1 << 20)
            if not chunk:
                break
            buffered += chunk
            # `OK` bodies end in a blank line; `ERR` is a single line.
            if b"\n\n" in buffered or buffered.startswith(b"ERR "):
                break
        return [ln for ln in buffered.decode(errors="replace").strip().splitlines() if ln]


@pytest.fixture
def book(cluster, primary_client: OrderbookEngine) -> str:
    """One flushed row, so the answer comes from the columnar store rather than a live buffer."""
    primary_client.insert(SYMBOL, EXCHANGE, "bid", [PRICE], [QTY])
    primary_client.flush()
    return SYMBOL


WHERE = "WHERE timestamp BETWEEN 0 AND 9999999999999999999"


def test_select_star_still_answers_seven_columns(cluster, book):
    """The control, and the one that matters most: every existing client sends this."""
    lines = raw_query(cluster.primary().tcp_port, f"SELECT * FROM '{book}'.'{EXCHANGE}' {WHERE}")
    assert lines[0] == "OK", lines
    assert lines[1].split("\t") == [
        "timestamp_ns", "price", "quantity", "order_count", "side", "level", "sequence_number"]
    assert len(lines[2].split("\t")) == 7


def test_a_narrowed_query_answers_only_those_columns(cluster, book):
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT price, quantity FROM '{book}'.'{EXCHANGE}' {WHERE}")
    assert lines[0] == "OK", lines
    assert lines[1].split("\t") == ["price", "quantity"]
    assert lines[2].split("\t") == [str(PRICE), str(QTY)]


def test_the_order_is_the_order_the_query_asked_for(cluster, book):
    """Not the canonical one. Answering the right columns in the wrong order is still a different
    answer from the question, and a client reading them positionally would be silently wrong."""
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT quantity, price FROM '{book}'.'{EXCHANGE}' {WHERE}")
    assert lines[1].split("\t") == ["quantity", "price"]
    assert lines[2].split("\t") == [str(QTY), str(PRICE)]


def test_the_timestamp_answers_to_the_name_the_header_gives_it(cluster, book):
    """`timestamp` is what the lexer always took; `timestamp_ns` is what a client copies out of a
    header. Both parse, and the header says `timestamp_ns` either way."""
    for spelling in ("timestamp", "timestamp_ns"):
        lines = raw_query(cluster.primary().tcp_port,
                          f"SELECT {spelling}, price FROM '{book}'.'{EXCHANGE}' {WHERE}")
        assert lines[1].split("\t") == ["timestamp_ns", "price"], spelling


def test_a_column_asked_for_twice_is_answered_twice(cluster, book):
    lines = raw_query(cluster.primary().tcp_port,
                      f"SELECT price, price FROM '{book}'.'{EXCHANGE}' {WHERE}")
    assert lines[1].split("\t") == ["price", "price"]
    assert lines[2].split("\t") == [str(PRICE), str(PRICE)]


def test_a_filter_reads_a_column_the_answer_does_not_carry(cluster, book):
    """`price` is not in the answer and the predicate still works, which is the whole reason the
    read set is wider than the output list. Both directions, so a filter that silently matched
    everything would fail the second."""
    port = cluster.primary().tcp_port
    hit = raw_query(port, f"SELECT quantity FROM '{book}'.'{EXCHANGE}' {WHERE} "
                          f"AND price BETWEEN {PRICE - 1} AND {PRICE + 1}")
    assert hit[1].split("\t") == ["quantity"]
    assert hit[2].split("\t") == [str(QTY)]

    miss = raw_query(port, f"SELECT quantity FROM '{book}'.'{EXCHANGE}' {WHERE} "
                           f"AND price BETWEEN {PRICE + 1000} AND {PRICE + 2000}")
    assert miss[0] == "OK", miss
    assert miss[1].split("\t") == ["quantity"]
    assert len(miss) == 2, f"the filter matched nothing, so there is no row: {miss}"


def test_the_python_client_refuses_an_answer_it_would_read_wrongly(cluster, book):
    """It converts fields by position, so a two-column answer would come back as an empty list —
    a silent wrong answer in our own client, which is the defect this removes from the server."""
    client = OrderbookEngine(host="127.0.0.1", port=cluster.primary().tcp_port)
    try:
        with pytest.raises(OrderbookError) as refused:
            client.query(f"SELECT price, quantity FROM '{book}'.'{EXCHANGE}' {WHERE}")
        assert "price" in str(refused.value), str(refused.value)

        # The control: the same client, the same book, the query it can read.
        rows = client.query(f"SELECT * FROM '{book}'.'{EXCHANGE}' {WHERE}")
        assert len(rows) == 1
        assert rows[0].price == PRICE and rows[0].quantity == QTY
    finally:
        client.close()
