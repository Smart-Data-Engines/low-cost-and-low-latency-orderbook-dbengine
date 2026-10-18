"""The live book over the wire, through the client somebody has (#145).

`SoABuffer` is the engine's current book, and until this item `read_snapshot()` had exactly one
caller - the aggregate branch - so no wire command returned its levels. A client's two routes were
both indirect and both made it hold what the server already holds: `SUBSCRIBE` and rebuild from the
stream, or `SELECT` history and replay it.

What this module adds over the unit tests is the pair of questions only a wire can answer: that a
`BOOK` answer is **not** a `SELECT` answer over the same writes, and that our own Python client
parses it. The first is the one nobody checks until both answers arrive in one run, and it is the
whole claim of the item: `SELECT` reads history and returns every version of a level, `BOOK` reads
the current state and returns one row per level.
"""

from __future__ import annotations

import socket
import subprocess
import time

import pytest

from conftest import free_port, open_node_log, patience, server_binary_path
from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.smoke


@pytest.fixture(scope="module")
def unflushed_client(tmp_path_factory):
    """A node of its own whose flush tick does not come round while a test runs.

    The shared cluster flushes every 100 ms, the server's default, so whether a tick falls between a
    write and a history read depends on how long the round trips take. Under TSan they take longest:
    in PR #249's run a tick fell between the two inserts of `test_both_sides_come_back_bids_first`,
    and history held the three bids before any flush the test asked for. Ten minutes is longer than
    any test here, so nothing reaches history unless a test flushes - the precedent is
    `test_crash_recovery.py`'s node.
    """
    data_dir = str(tmp_path_factory.mktemp("live_book_unflushed"))
    port = free_port()
    log = open_node_log(data_dir)
    proc = subprocess.Popen(
        [server_binary_path(), "--port", str(port), "--data-dir", data_dir, "--metrics-port", "0",
         "--flush-interval-ms", "600000", "--drain-timeout-ms", "1000"],
        stdout=log, stderr=subprocess.STDOUT)
    log.close()
    deadline = time.time() + patience(30)
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
                s.settimeout(2)
                s.recv(4096)
                s.sendall(b"PING\n")
                if b"PONG" in s.recv(1024):
                    break
        except OSError:
            pass
        if proc.poll() is not None or time.time() > deadline:
            proc.kill()
            proc.wait(timeout=10)
            with open(f"{data_dir}/node.log", encoding="utf-8", errors="replace") as f:
                pytest.fail(f"the node never answered on port {port}:\n{f.read()[-1500:]}")
        time.sleep(0.1)
    client = OrderbookEngine(host="127.0.0.1", port=port, timeout=patience(10))
    try:
        yield client
    finally:
        client.close()
        proc.terminate()
        try:
            proc.wait(timeout=patience(10))
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


def _history_rows(engine: OrderbookEngine, symbol: str) -> int:
    """How many rows the *columnar store* has for a symbol.

    A symbol with a live buffer and no segments can answer either zero rows or a refusal depending
    on what the store knows, and both mean the same thing here: nothing has been flushed. Written
    as a helper for the same reason `test_event_time.py` has one - a bare `pytest.raises` would pin
    which of the two arrives, which is not what any test in this file is about.
    """
    try:
        return len(engine.query_all(symbol, "EX"))
    except OrderbookError as exc:
        if "not found" in str(exc).lower():
            return 0
        raise


def test_the_book_is_the_current_state_and_select_is_the_history(primary_client):
    # One price written three times. History keeps all three; the book keeps the last.
    for qty in (10, 20, 30):
        primary_client.insert("BOOK-HIST", "EX", "bid", [100_000], [qty])
    primary_client.flush()

    history = primary_client.query_all("BOOK-HIST", "EX")
    assert len(history) == 3, f"history should hold every version: {history}"

    book = primary_client.book("BOOK-HIST", "EX")
    assert len(book) == 1, f"the book should hold one row per level: {book}"
    assert book[0].quantity == 30, "the book returned a superseded version"
    assert book[0].side == "bid"
    assert book[0].level == 0


def test_both_sides_come_back_bids_first(unflushed_client):
    unflushed_client.insert("BOOK-SIDES", "EX", "bid", [100, 102, 101], [5, 7, 6])
    unflushed_client.insert("BOOK-SIDES", "EX", "ask", [105, 103], [8, 9])

    rows = unflushed_client.book("BOOK-SIDES", "EX")
    assert [r.side for r in rows] == ["bid", "bid", "bid", "ask", "ask"], rows
    assert [r.price for r in rows] == [102, 101, 100, 103, 105], rows
    # `level` is a position within a side, so the best ask is level 0 and not row 3.
    assert [r.level for r in rows] == [0, 1, 2, 0, 1], rows

    # No flush anywhere above, and none from the node's tick (`unflushed_client`): the book is the
    # live structure, so it answers before anything is a segment. The control is that history has
    # nothing yet - whether that arrives as zero rows or as a refusal is the store's business and
    # not what this test is about.
    assert _history_rows(unflushed_client, "BOOK-SIDES") == 0, (
        "the history answered before a flush, so this test is not contrasting the two paths")


def test_depth_is_per_side_and_its_refusals_name_the_token(primary_client):
    primary_client.insert("BOOK-DEPTH", "EX", "bid", [100, 99, 98, 97], [1, 2, 3, 4])
    primary_client.insert("BOOK-DEPTH", "EX", "ask", [110, 111, 112], [1, 2, 3])

    two = primary_client.book("BOOK-DEPTH", "EX", depth=2)
    assert [r.price for r in two] == [100, 99, 110, 111], two
    assert len(primary_client.book("BOOK-DEPTH", "EX")) == 7

    # Refused by the server, naming the token, rather than clamped: a client asking for five
    # thousand levels is asking for something this engine cannot store (#107).
    with pytest.raises(OrderbookError) as over:
        primary_client.book("BOOK-DEPTH", "EX", depth=5000)
    assert "5000" in str(over.value), over.value

    # Refused by the client before the wire, because `None` already means "everything" and an empty
    # answer on request is indistinguishable from a book that is not there.
    with pytest.raises(OrderbookError):
        primary_client.book("BOOK-DEPTH", "EX", depth=0)


def test_every_row_carries_the_same_snapshot_identity(primary_client):
    primary_client.insert("BOOK-IDENT", "EX", "bid", [100, 99], [5, 6])
    primary_client.insert("BOOK-IDENT", "EX", "ask", [101], [7])

    rows = primary_client.book("BOOK-IDENT", "EX")
    assert len({r.sequence_number for r in rows}) == 1, (
        f"the sequence number is a property of the read, not of a level: {rows}")
    assert len({r.timestamp_ns for r in rows}) == 1, rows
    assert rows[0].sequence_number > 0, "the identity of the read must not be zero-by-accident"


def test_a_symbol_the_node_has_never_seen_is_refused_by_name(primary_client):
    with pytest.raises(OrderbookError) as absent:
        primary_client.book("BOOK-NOSUCH", "EX")
    assert "BOOK-NOSUCH" in str(absent.value), absent.value


# #233: one write, read two ways. The live book applies an update by price - a price it does not get
# again stays - and history keyed a row by its position in the update, so the two answered different
# best bids for the same writes. History reads by price now (decision B'), so both kinds of writer get
# one best bid; these were strict xfails until it did.
T233 = 1_791_900_000_000_000_000


def _best_bids(engine: OrderbookEngine, symbol: str, at: int) -> tuple[int, int]:
    """The best bid of the live book now, and of history at `at`."""
    engine.flush()
    live = [r.price for r in engine.book(symbol, "EX", depth=1) if r.side == "bid"][0]
    then = engine.query_rows(f"SELECT price FROM '{symbol}'.'EX' WHERE AT {at} AND side = 0 "
                             f"AND level = 0").rows[0][0]
    return live, then


def test_a_writer_of_top_levels_gets_one_best_bid_from_the_book_and_history(primary_client):
    # Each update is the side's best two levels, as a depth stream sends them.
    primary_client.insert("BOOK-233-TOP", "EX", "bid", [100, 99], [1, 1], timestamp_ns=T233)
    primary_client.insert("BOOK-233-TOP", "EX", "ask", [101, 102], [1, 1], timestamp_ns=T233)
    primary_client.insert("BOOK-233-TOP", "EX", "bid", [98, 97], [2, 2],
                          timestamp_ns=T233 + 1_000_000_000)
    live, then = _best_bids(primary_client, "BOOK-233-TOP", T233 + 1_000_000_000)
    assert live == then, f"the live book's best bid is {live}, history's {then}"


def test_a_writer_of_changed_levels_gets_one_best_bid_from_the_book_and_history(primary_client):
    # The first update is the book; the second changes one level behind the best, as a diff does.
    primary_client.insert("BOOK-233-DIFF", "EX", "bid", [1000, 990], [1, 1], timestamp_ns=T233)
    primary_client.insert("BOOK-233-DIFF", "EX", "ask", [1010], [1], timestamp_ns=T233)
    primary_client.insert("BOOK-233-DIFF", "EX", "bid", [995], [5],
                          timestamp_ns=T233 + 1_000_000_000)
    live, then = _best_bids(primary_client, "BOOK-233-DIFF", T233 + 1_000_000_000)
    assert live == then, f"the live book's best bid is {live}, history's {then}"
