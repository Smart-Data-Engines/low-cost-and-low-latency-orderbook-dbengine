"""A reply goes to the command that asked for it (#170, #171).

The Python client reads replies off the front of one buffer per connection, so a reply belongs to
whichever command reads it next. Two things broke that pairing, and in both the wrong rows came
back as a normal answer:

**#171, an exchange that did not finish.** A command whose reply did not arrive in full - a
timeout, most often - left the rest of that reply on its way, and the next command read it as its
own. Measured: the query after a timed-out one got 100 000 rows of another symbol with no error,
and every reply after that was one behind, for the life of the connection. The connection is now
closed at the first failure, and every later call on it says why.

**#170, two exchanges at once.** A pool keeps one connection per node, used by every thread that
calls it and by its own health check, which sends ROLE down each connection every couple of
seconds, and nothing made an exchange one thread's at a time. Measured on a standalone node, two
threads each asking for their own symbol got each other's rows 10 216 times in 26 104, and with one
caller and the health check at 10 ms the first FLUSH got the health check's STANDALONE.

Every assertion is on what a caller got back, never only on whether it raised: the worst form of
both defects returns normally.
"""
from __future__ import annotations

import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time

import pytest

from conftest import free_port, server_binary_path
from orderbook_engine import BookUpdate, OrderbookEngine, OrderbookError

SERVER = server_binary_path()
RACE_SECONDS = 2.0
# Enough answers that the old client, wrong about two times in five, cannot produce none of them by
# luck, and few enough for a server running under a sanitizer.
MIN_ANSWERS = 100


class PausableNode:
    """One standalone ob_tcp_server of this module's own, which SIGSTOP can freeze.

    A stopped server keeps its sockets: a command sent to it waits in the kernel, and the reply is
    written the moment the process is continued - which is exactly a reply arriving late. The
    session cluster cannot be frozen without stalling every other test's node, so this is a process
    of its own.
    """

    def __init__(self) -> None:
        self.data_dir = tempfile.mkdtemp(prefix="ob_reply_")
        self.port = free_port()
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", self.data_dir,
             "--metrics-port", "0"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=2):
                    return
            except OSError:
                time.sleep(0.1)
        self.stop()
        raise RuntimeError(f"server on port {self.port} did not come up")

    def pause(self) -> None:
        self.proc.send_signal(signal.SIGSTOP)

    def resume(self) -> None:
        self.proc.send_signal(signal.SIGCONT)

    def stop(self) -> None:
        if self.proc.poll() is None:
            # A stopped process does not act on SIGTERM until it is continued.
            self.resume()
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        shutil.rmtree(self.data_dir, ignore_errors=True)


@pytest.fixture
def node():
    n = PausableNode()
    try:
        yield n
    finally:
        n.stop()


def own_rows(rows) -> list[tuple[int, int]]:
    return [(r.price, r.quantity) for r in rows]


def seed(client: OrderbookEngine, rows: dict[str, tuple[int, int]]) -> None:
    for symbol, (price, qty) in rows.items():
        client.insert(symbol, "EX", "bid", [price], [qty])
    client.flush()


def wait_until_answering(client: OrderbookEngine, rows: dict[str, tuple[int, int]],
                         within: float = 20.0) -> None:
    """Every symbol answers with its own row through `client` - the premise of every race below.

    Reads may go to a replica, and a row still on its way there would read as the defect."""
    deadline = time.time() + within
    while time.time() < deadline:
        if all(own_rows(client.query(f"SELECT * FROM '{s}'.'EX'")) == [r] for s, r in rows.items()):
            return
        time.sleep(0.1)
    raise AssertionError(f"not every symbol answered with its own row within {within} s: {rows}")


# ── #171: an exchange that did not finish ────────────────────────────────────────────────────────


# What times out: one command, or a pipelined batch - the other way an exchange is made.
TIMED_OUT = {
    "a query": lambda c: c.query("SELECT * FROM 'RA-LATE'.'EX'"),
    "a pipelined batch": lambda c: c.insert_batch(
        [BookUpdate("RA-LATE-BATCH", "EX", "bid", [10 + i], [1]) for i in range(8)]),
}


@pytest.mark.edge_cases
@pytest.mark.parametrize("timed_out", sorted(TIMED_OUT))
def test_a_reply_that_timed_out_is_not_read_as_the_next_commands(node, timed_out) -> None:
    writer = OrderbookEngine(host="127.0.0.1", port=node.port, timeout=30.0)
    try:
        seed(writer, {"RA-LATE": (100, 1), "RA-NEXT": (200, 2)})
    finally:
        writer.close()

    client = OrderbookEngine(host="127.0.0.1", port=node.port, timeout=0.5)
    try:
        node.pause()
        try:
            with pytest.raises(OrderbookError, match="timeout"):
                TIMED_OUT[timed_out](client)
        finally:
            node.resume()

        # The late reply is written as soon as the server runs again. Wait until a connection of
        # its own is answered, so the old behaviour - reading that reply now - is what the next
        # call would meet rather than a second timeout.
        control = OrderbookEngine(host="127.0.0.1", port=node.port, timeout=30.0)
        try:
            assert own_rows(control.query("SELECT * FROM 'RA-LATE'.'EX'")) == [(100, 1)]
            time.sleep(0.3)
            # The control: the server answers the next question correctly on a connection that
            # did not time out. Without it the refusal below proves nothing about the client.
            assert own_rows(control.query("SELECT * FROM 'RA-NEXT'.'EX'")) == [(200, 2)]
        finally:
            control.close()

        for name, call in (("a query", lambda: client.query("SELECT * FROM 'RA-NEXT'.'EX'")),
                           ("a PING", client.ping)):
            with pytest.raises(OrderbookError) as refused:
                call()
            assert "was closed because" in str(refused.value), (
                f"{name} on the connection that timed out did not say why it was refused: "
                f"{refused.value}")
    finally:
        client.close()


@pytest.mark.pool
def test_a_pool_replaces_a_connection_that_timed_out_and_never_answers_from_it(node) -> None:
    writer = OrderbookEngine(host="127.0.0.1", port=node.port, timeout=30.0)
    try:
        seed(writer, {"RA-PLATE": (300, 3), "RA-PNEXT": (400, 4)})
    finally:
        writer.close()

    pool = OrderbookEngine(hosts=[f"127.0.0.1:{node.port}"], timeout=0.5,
                           health_check_interval=0.05)
    try:
        node.pause()
        try:
            with pytest.raises(OrderbookError):
                pool.query("SELECT * FROM 'RA-PLATE'.'EX'")
        finally:
            node.resume()

        # Until the health check replaces the connection a call may be refused. It may never be
        # answered with anything but its own row.
        deadline = time.time() + 20.0
        answered = None
        while time.time() < deadline:
            try:
                answered = own_rows(pool.query("SELECT * FROM 'RA-PNEXT'.'EX'"))
            except OrderbookError:
                time.sleep(0.05)
                continue
            assert answered == [(400, 4)], f"the pool answered RA-PNEXT with {answered}"
            break
        assert answered == [(400, 4)], "the pool never replaced the connection that timed out"
    finally:
        pool.close()


# ── #170: two exchanges at once ──────────────────────────────────────────────────────────────────


def pool_over(cluster, health_check_interval: float) -> OrderbookEngine:
    hosts = [f"127.0.0.1:{n.tcp_port}" for n in cluster.nodes]
    return OrderbookEngine(hosts=hosts, timeout=10.0, health_check_interval=health_check_interval)


@pytest.mark.pool
def test_two_threads_of_one_pool_each_get_their_own_answers(cluster) -> None:
    rows = {"RA-THREAD-A": (100, 1), "RA-THREAD-B": (200, 2)}
    # No health check traffic: the other party is the other caller and nothing else.
    pool = pool_over(cluster, health_check_interval=1000.0)
    try:
        seed(pool, rows)
        wait_until_answering(pool, rows)

        answers = {s: 0 for s in rows}
        wrong: dict[str, list] = {s: [] for s in rows}
        start = threading.Barrier(len(rows))

        def ask(symbol: str) -> None:
            start.wait()
            stop = time.time() + RACE_SECONDS
            while time.time() < stop:
                try:
                    got = own_rows(pool.query(f"SELECT * FROM '{symbol}'.'EX'"))
                except OrderbookError as e:
                    wrong[symbol].append(f"raised: {e}")
                    continue
                answers[symbol] += 1
                if got != [rows[symbol]]:
                    wrong[symbol].append(got)

        threads = [threading.Thread(target=ask, args=(s,)) for s in rows]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=RACE_SECONDS + 60)
        assert not any(t.is_alive() for t in threads), "a caller never came back"

        for symbol in rows:
            assert not wrong[symbol], (
                f"{symbol}: {len(wrong[symbol])} answers were not its own row, of "
                f"{answers[symbol]}; first {wrong[symbol][:3]}")
            assert answers[symbol] >= MIN_ANSWERS, (
                f"{symbol} was answered {answers[symbol]} times, too few to have raced the other "
                f"thread; this run proves nothing")
    finally:
        pool.close()


@pytest.mark.pool
def test_the_health_check_never_answers_a_callers_command(cluster) -> None:
    pool = pool_over(cluster, health_check_interval=0.01)
    try:
        first = pool._pool._nodes[0]
        checks_seen = set()
        stop = time.time() + RACE_SECONDS
        calls = 0
        while time.time() < stop:
            pong = pool.ping()
            assert pong.strip() == "PONG", f"PING was answered with {pong!r}"
            pool.flush()   # raises on a reply that is not FLUSH's
            calls += 2
            checks_seen.add(first.last_check)
        assert len(checks_seen) >= 20, (
            f"the health check answered {len(checks_seen)} times during {calls} calls, too few to "
            f"have raced them; this run proves nothing")
    finally:
        pool.close()


@pytest.mark.pool
def test_a_long_exchange_on_one_node_does_not_hold_up_a_write_to_another(cluster) -> None:
    """The fix's own hazard, and why the health check asks without the pool's lock.

    Once an exchange holds its connection, the health check's ROLE waits for whatever a caller has
    in flight there - and the first version of this fix asked while holding the pool's lock, which
    every call through the pool takes to choose a node. One slow read on a replica then stalled
    every write to the primary. The test holds a replica's connection the way a long read does and
    asks for a write.
    """
    primary = cluster.primary()
    replica = next(n for n in cluster.nodes if n.tcp_port != primary.tcp_port)
    pool = pool_over(cluster, health_check_interval=0.01)
    try:
        inner = pool._pool
        held = inner._connections[f"127.0.0.1:{replica.tcp_port}"]
        replica_state = next(n for n in inner._nodes if n.port == replica.tcp_port)
        before = replica_state.last_check
        time.sleep(0.2)
        assert replica_state.last_check != before, "the health check is not running: no premise"

        outcome: dict = {}
        writer = None

        def write() -> None:
            try:
                pool.insert("RA-HELD", "EX", "bid", [500], [5])
                outcome["ok"] = True
            except Exception as e:   # noqa: BLE001 - reported below
                outcome["error"] = e

        held._io_lock.acquire()
        try:
            time.sleep(0.3)   # thirty health-check intervals: it is waiting on this connection now
            frozen = replica_state.last_check
            writer = threading.Thread(target=write)
            writer.start()
            writer.join(timeout=5.0)
            assert not writer.is_alive(), (
                "a write to the primary waited for an exchange on the replica's connection")
            assert outcome.get("ok"), f"the write failed: {outcome.get('error')}"
            assert replica_state.last_check == frozen, (
                "the health check answered from the held connection, so this ran without its "
                "premise")
        finally:
            held._io_lock.release()
            if writer is not None and writer.is_alive():
                writer.join(timeout=30)
    finally:
        pool.close()

    direct = OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=10.0)
    try:
        direct.flush()   # the write answered OK; a row is read from a segment
        assert own_rows(direct.query_all("RA-HELD", "EX")) == [(500, 5)]
    finally:
        direct.close()


@pytest.mark.pool
def test_an_answer_about_a_connection_the_pool_replaced_changes_nothing(cluster) -> None:
    """The health check asks without the pool's lock, so the answer can come back about a
    connection another thread has since replaced - a write's retry reconnects on its own thread. An
    answer that dropped the replacement would leave the pool without a node it can reach."""
    pool = pool_over(cluster, health_check_interval=0.01)
    try:
        inner = pool._pool
        key = f"127.0.0.1:{cluster.nodes[0].tcp_port}"
        old = inner._connections[key]
        old._io_lock.acquire()
        try:
            time.sleep(0.3)   # the health check is waiting on `old`
            replacement = type(old)(old._host, old._port, 10.0)
            with inner._lock:
                inner._connections[key] = replacement
            old.close()   # its ROLE, when the health check gets it, fails
        finally:
            old._io_lock.release()
        time.sleep(0.3)   # thirty more intervals: the failed answer has been written, or not
        assert inner._connections.get(key) is replacement, (
            "a failed answer about the old connection dropped the one that replaced it")
    finally:
        pool.close()


@pytest.mark.subscriptions
def test_a_poll_does_not_shorten_another_threads_wait_for_a_reply(node) -> None:
    """poll() waits by setting the socket's timeout to what is left of its own wait - a property of
    the socket, so of whatever another thread is reading from it at the time. A command whose reply
    took longer than the poll's wait failed as a timeout it never had, and since #171 a timeout
    closes the connection. The poll and the command take turns on it now.

    Which reply is whose was never the danger here: both take from the front of one buffer, and a
    command has one reply outstanding. Measured, the version of this test that only raced them
    passed against the client before the fix.
    """
    writer = OrderbookEngine(host="127.0.0.1", port=node.port, timeout=30.0)
    try:
        seed(writer, {"RA-POLLED": (800, 8)})
    finally:
        writer.close()

    client = OrderbookEngine(host="127.0.0.1", port=node.port, timeout=10.0)
    stop = threading.Event()

    def keep_polling() -> None:
        while not stop.is_set():
            client.poll(0.05)
            # Outside the connection: a lock released and taken straight back is rarely the other
            # thread's, and a query that never gets a turn tests nothing.
            time.sleep(0.001)

    poller = threading.Thread(target=keep_polling)
    try:
        client.subscribe("RA-SUBSCRIBED", "EX")
        poller.start()
        time.sleep(0.2)
        # Frozen for half a second: longer than the poll's wait, far shorter than the client's.
        node.pause()
        resumer = threading.Timer(0.5, node.resume)
        resumer.start()
        try:
            got = own_rows(client.query("SELECT * FROM 'RA-POLLED'.'EX'"))
        finally:
            resumer.join()
        assert got == [(800, 8)], f"the query was answered with {got}"
    finally:
        stop.set()
        if poller.is_alive():
            poller.join(timeout=30)
        client.close()


@pytest.mark.subscriptions
def test_close_does_not_wait_for_a_poll_in_another_thread(node) -> None:
    """close() and an exchange take turns on a connection (#170), and a poll's turn lasts as long as
    it was asked to wait: a close that queued behind one would wait that long too. The socket is
    shut under the poll instead, which ends it at once, and the poll says why."""
    client = OrderbookEngine(host="127.0.0.1", port=node.port, timeout=10.0)
    client.subscribe("RA-CLOSED", "EX")
    outcome: dict = {}

    def long_poll() -> None:
        try:
            outcome["rows"] = client.poll(30.0)
        except OrderbookError as e:
            outcome["error"] = e

    poller = threading.Thread(target=long_poll)
    poller.start()
    try:
        time.sleep(0.3)   # the poll is waiting on the socket
        started = time.monotonic()
        client.close()
        took = time.monotonic() - started
        poller.join(timeout=5.0)
        assert took < 2.0, f"close() took {took:.2f} s behind a poll of 30 s"
        assert not poller.is_alive(), "the poll went on waiting after close()"
        assert "closed while this call was in flight" in str(outcome.get("error", "")), (
            f"the poll ended with {outcome}")
    finally:
        client.close()
        poller.join(timeout=35.0)
