"""`MREPLACE`: replace one side of a book, over the wire (#233, the incremental books).

An update by price sets the quantity of each price it names, so a writer of whole books that sends
the top N levels left every price that dropped out of them in the book - and in history, where an
`AT` read assembles the book by price. `MREPLACE` says "the side is exactly these levels", and the
server adds a quantity 0 for each price the side holds and the block does not name, before the
update is numbered and logged.

What this module checks is what only a wire and a second process can show: the command in both of
its shapes and in both transports, what history keeps, what a replica, a peer and a restart hold,
and the Python client's side of the contract - asking for `replace_side` before sending. The C++
client's is in `test_cpp_client.py`, with the binary it runs.
"""

from __future__ import annotations

import signal
import socket
import subprocess
import time

import pytest

from conftest import free_port, open_node_log, patience, send_command, server_binary_path
from orderbook_engine import BookUpdate, OrderbookEngine, OrderbookError

pytestmark = pytest.mark.smoke

T0 = 1_700_000_000_000_000_000


def side_of(client: OrderbookEngine, symbol: str, side: str) -> list[tuple[int, int]]:
    """One side of the live book, best first, as (price, quantity)."""
    return [(r.price, r.quantity) for r in client.book(symbol, "EX") if r.side == side]


def eventually(read, expected, within: float) -> object:
    """Read until the answer is `expected` or the time is up; return the last answer."""
    deadline = time.monotonic() + patience(within)
    got = read()
    while got != expected and time.monotonic() < deadline:
        time.sleep(0.1)
        got = read()
    return got


# ── the command ───────────────────────────────────────────────────────────────

def test_the_server_announces_replace_side(primary_client):
    assert "replace_side" in primary_client.server_capabilities()


def test_the_side_becomes_exactly_the_replacement(primary_client):
    c = primary_client
    c.insert("RS-EXACT", "EX", "bid", [100, 99, 98], [1, 2, 3])
    c.insert("RS-EXACT", "EX", "ask", [110, 111], [4, 5])

    c.replace_side("RS-EXACT", "EX", "bid", [101, 99], [5, 7])

    assert side_of(c, "RS-EXACT", "bid") == [(101, 5), (99, 7)], (
        "a price the replacement did not name was kept, or one it named was lost")
    assert side_of(c, "RS-EXACT", "ask") == [(110, 4), (111, 5)], "replacing the bids moved the asks"


def test_no_levels_empties_the_side(primary_client):
    c = primary_client
    c.insert("RS-EMPTY", "EX", "ask", [110, 111], [4, 5])
    c.insert("RS-EMPTY", "EX", "bid", [100], [1])
    c.replace_side("RS-EMPTY", "EX", "ask", [], [])
    assert side_of(c, "RS-EMPTY", "ask") == []
    assert side_of(c, "RS-EMPTY", "bid") == [(100, 1)]


def test_a_compressed_connection_takes_both_shapes(compressed_client):
    # A compressed frame is one command: a block of levels, or the one line of an empty side.
    c = compressed_client
    c.insert("RS-LZ4", "EX", "bid", [100, 99], [1, 2])
    c.replace_side("RS-LZ4", "EX", "bid", [98], [3])
    assert side_of(c, "RS-LZ4", "bid") == [(98, 3)]
    c.replace_side("RS-LZ4", "EX", "bid", [], [])
    assert side_of(c, "RS-LZ4", "bid") == []


def test_the_wire_takes_the_block_and_the_one_line_and_names_what_it_refuses(cluster):
    port = cluster.primary().tcp_port
    assert send_command(port, "INSERT RS-WIRE EX bid 100 1").startswith("OK")
    assert send_command(port, "MREPLACE RS-WIRE EX bid 2\n101 5 1\n99 6 2").startswith("OK")
    assert send_command(port, "MREPLACE RS-WIRE EX ask 0").startswith("OK")

    # The session frames the lines a header announces before anything parses it, so the refusal
    # of 1001 comes once the 1001 lines have arrived - as MINSERT's always has.
    lines = "\n".join(f"{1000 + i} 1 1" for i in range(1001))
    over = send_command(port, "MREPLACE RS-WIRE EX bid 1001\n" + lines)
    assert over.startswith("ERR"), over
    assert "MREPLACE number of levels is not 0 to 1000: '1001'" in over, over
    # The same count on MINSERT is named too now, rather than answered `unknown command` (#222).
    none = send_command(port, "MINSERT RS-WIRE EX bid 0")
    assert "MINSERT number of levels is not 1 to 1000: '0'" in none, none


def test_history_holds_the_update_and_then_its_removals(primary_client):
    c = primary_client
    c.insert("RS-HIST", "EX", "bid", [100, 99, 98], [1, 2, 3], timestamp_ns=T0 + 1)
    c.replace_side("RS-HIST", "EX", "bid", [101, 99], [5, 7], timestamp_ns=T0 + 2)
    c.flush()
    rows = [(r.price, r.quantity) for r in c.query_all("RS-HIST", "EX") if r.timestamp_ns == T0 + 2]
    # The removals best first, then the update's levels: what the WAL record holds and what a
    # replica, a restart and an `AT` read are given.
    assert sorted(rows) == sorted([(101, 5), (99, 7), (100, 0), (98, 0)]), rows


def test_a_batch_carries_replacements_in_order(primary_client):
    c = primary_client
    outcomes = c.insert_batch([
        BookUpdate("RS-BATCH", "EX", "bid", [100, 99], [1, 2]),
        BookUpdate("RS-BATCH", "EX", "bid", [98], [3], replace=True),
        BookUpdate("RS-BATCH", "EX", "bid", [97], [4]),
        BookUpdate("RS-BATCH", "EX", "ask", [], [], replace=True),
    ])
    assert [o.ok for o in outcomes] == [True, True, True, True], outcomes
    assert side_of(c, "RS-BATCH", "bid") == [(98, 3), (97, 4)], (
        "the replacement did not see the write before it in the batch, or removed the one after")


def test_the_client_refuses_before_sending_to_a_server_that_cannot_replace(primary_client):
    c = primary_client
    c.insert("RS-OLD", "EX", "bid", [100, 99], [1, 2])
    # What an older server's STATUS says: no `replace_side`. The answer is cached per connection,
    # so this is the connection's view of that server.
    c._capabilities = {"insert_event_time", "strict_args", "backup"}
    with pytest.raises(OrderbookError) as refused:
        c.replace_side("RS-OLD", "EX", "bid", [98], [3])
    assert "Nothing was sent" in str(refused.value), refused.value
    with pytest.raises(OrderbookError):
        c.insert_batch([BookUpdate("RS-OLD", "EX", "bid", [97], [1], replace=True)])
    c._capabilities = None
    assert side_of(c, "RS-OLD", "bid") == [(100, 1), (99, 2)], "a refused replacement was sent"


def test_a_misspelt_side_is_refused_before_anything_is_sent(primary_client):
    with pytest.raises(ValueError):
        primary_client.replace_side("RS-SIDE", "EX", "buy", [1], [1])


# ── what other processes hold ─────────────────────────────────────────────────

def test_a_replica_holds_the_replaced_side(primary_client, replica_client):
    primary_client.insert("RS-REPL", "EX", "bid", [100, 99, 98], [1, 2, 3])
    primary_client.replace_side("RS-REPL", "EX", "bid", [97], [9])
    expected = [(97, 9)]
    assert side_of(primary_client, "RS-REPL", "bid") == expected
    got = eventually(lambda: side_of(replica_client, "RS-REPL", "bid"), expected, within=15)
    assert got == expected, "the replica kept prices the primary's replacement removed"


@pytest.fixture
def own_node(tmp_path):
    """A node of its own, killable, whose flush tick never comes: the replacement stays in the WAL,
    so a restart has to replay it rather than read it back from a segment."""
    (tmp_path / "node").mkdir()
    data_dir = str(tmp_path / "node")
    state: dict = {}

    def start() -> OrderbookEngine:
        port = free_port()
        log = open_node_log(data_dir)
        proc = subprocess.Popen(
            [server_binary_path(), "--port", str(port), "--data-dir", data_dir,
             "--metrics-port", "0", "--flush-interval-ms", "600000"],
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
                pytest.fail(f"the node never answered on port {port}")
            time.sleep(0.1)
        state["proc"] = proc
        client = OrderbookEngine(host="127.0.0.1", port=port, timeout=patience(10))
        state.setdefault("clients", []).append(client)
        return client

    def kill() -> None:
        proc = state.pop("proc")
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)

    yield start, kill
    for client in state.get("clients", []):
        client.close()
    proc = state.get("proc")
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=patience(10))
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


def test_after_sigkill_the_replaced_side_comes_back(own_node):
    start, kill = own_node
    c = start()
    c.insert("RS-KILL", "EX", "bid", [100, 99, 98], [1, 2, 3])
    c.replace_side("RS-KILL", "EX", "bid", [101, 98], [5, 4])
    before = side_of(c, "RS-KILL", "bid")
    assert before == [(101, 5), (98, 4)]
    kill()
    after = side_of(start(), "RS-KILL", "bid")
    assert after == before, "a restart from the WAL brought back a price the replacement removed"


@pytest.mark.multi_master
def test_every_peer_holds_the_replaced_side(mm_cluster):
    writer, *peers = mm_cluster.nodes
    w = OrderbookEngine(host="127.0.0.1", port=writer.tcp_port, timeout=20)
    try:
        w.insert("RS-MESH", "EX", "ask", [110, 111, 112], [1, 2, 3])
        w.replace_side("RS-MESH", "EX", "ask", [109, 111], [7, 8])
        expected = side_of(w, "RS-MESH", "ask")
        assert expected == [(109, 7), (111, 8)]
    finally:
        w.close()
    for node in peers:
        p = OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=20)
        try:
            got = eventually(lambda: side_of(p, "RS-MESH", "ask"), expected, within=20)
            assert got == expected, f"{node.node_id} kept prices the writer's replacement removed"
        finally:
            p.close()

