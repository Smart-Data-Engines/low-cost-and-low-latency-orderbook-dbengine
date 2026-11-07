"""#260: a large read answered off the client loop, on a live node.

A client loop ran a `SELECT` to its last byte before it served another session: every session on it
waited - its `PING`, its writes - for as long as the read took. A read whose scan the index puts past
100 000 rows is answered on threads of its own now (`--large-read-threads`), and its loop goes on
serving the others; the session that asked waits for its answer, and its later commands and its socket
wait with it, so a session's answers still come in the order it asked.

One client loop (`--io-threads 1`), so every session is on it; 600 000 rows of one book. The control is
the same node with `--large-read-threads 0`, as every node was before #260. A session closed while its
read is answered off the loop - a reload that revokes its identity (#258) - is checked on a node of its
own, with client authentication.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import signal
import socket
import struct
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import lz4.frame
import pytest

from conftest import free_port, patience, server_binary_path
from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.large_response

ROWS = 600_000
ALICE = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1-alice"
BOB = "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1-bob"
WHOLE = "SELECT * FROM 'ISO'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999"

# A PING every half millisecond on a session of its own, until a line comes on stdin; then each PING's
# send time and round trip, in nanoseconds of the monotonic clock every process on the machine reads.
PINGER = r"""
import json, select, socket, sys, time
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=float(sys.argv[2]))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
buf = b""
while b"\n\n" not in buf:
    buf += s.recv(65536)
print("ready", flush=True)
samples = []
while not select.select([sys.stdin], [], [], 0)[0]:
    sent = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    s.sendall(b"PING\n")
    got = b""
    while not got.endswith(b"\n"):
        got += s.recv(64)
    samples.append((sent, time.clock_gettime_ns(time.CLOCK_MONOTONIC) - sent))
    time.sleep(0.0005)
print(json.dumps(samples), flush=True)
"""


class Node:
    def __init__(self, tmp: Path, *extra: str, clients: str | None = None) -> None:
        self.port = free_port()
        self.metrics_port = free_port()
        self.clients = tmp / "clients"
        auth: list[str] = []
        if clients is not None:
            self.write_clients(clients)
            auth = ["--auth-secret-file", str(self.clients)]
        self.log_path = tmp / "node.log"
        self.log = open(self.log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port), "--data-dir", str(tmp / "data"),
             "--metrics-port", str(self.metrics_port), "--io-threads", "1", *auth, *extra],
            stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + patience(30)
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"the node exited with {self.proc.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=1):
                    return
            except OSError:
                time.sleep(0.1)
        raise AssertionError("the node never accepted a connection")

    def write_clients(self, text: str) -> None:
        self.clients.write_text(text)
        self.clients.chmod(0o600)

    def reload(self) -> None:
        self.proc.send_signal(signal.SIGHUP)

    def logged(self, pattern: str, count: int = 1) -> list[str]:
        """The groups of `pattern` in the node's log, once it holds `count` lines that match."""
        deadline = time.monotonic() + patience(10)
        while True:
            self.log.flush()
            found = re.findall(pattern, self.log_path.read_text(errors="replace"))
            if len(found) >= count:
                return found
            assert time.monotonic() < deadline, f"the node never logged {pattern!r} {count} time(s)"
            time.sleep(0.01)

    def load(self, auth: tuple[str, str] | None = None) -> None:
        client = OrderbookEngine(host="127.0.0.1", port=self.port, timeout=patience(120), auth=auth)
        for sent in range(0, ROWS, 500):
            count = min(500, ROWS - sent)
            client.insert("ISO", "EX", "bid", [1_000_000 + (sent + i) % 900_000 for i in range(count)],
                          [10 + (sent + i) % 5000 for i in range(count)])
        client.flush()
        client.close()

    def metric(self, name: str) -> float:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.metrics_port}/metrics",
                                    timeout=patience(10)) as resp:
            body = resp.read().decode(errors="replace")
        match = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)$", body, re.M)
        assert match, f"{name} is not exported"
        return float(match.group(1))

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=patience(30))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()


def connect(port: int) -> socket.socket:
    s = socket.create_connection(("127.0.0.1", port), timeout=patience(120))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    buf = b""
    while b"\n\n" not in buf:
        buf += s.recv(65536)
    return s


def response(secret: str, identity: str, nonce: str) -> str:
    message = b"\x00".join([b"ob-auth-v1", b"client", b"initiator", identity.encode(), nonce.encode()])
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def authenticated(port: int, identity: str, secret: str) -> socket.socket:
    s = connect(port)
    s.sendall(b"AUTH\n")
    (challenge,) = answers(s, 1)
    assert challenge.startswith(b"OK CHALLENGE "), challenge
    nonce = challenge.split()[2].decode()
    s.sendall(f"AUTH {identity} {response(secret, identity, nonce)}\n".encode())
    (ok,) = answers(s, 1)
    assert ok.startswith(b"OK AUTH " + identity.encode()), ok
    return s


def answers(s: socket.socket, n: int) -> list[bytes]:
    """The next `n` answers: `OK` through its empty line, or one line of anything else."""
    out: list[bytes] = []
    buf = bytearray()
    while len(out) < n:
        chunk = s.recv(1 << 20)
        assert chunk, "the session was closed"
        buf += chunk
        while len(out) < n and buf:
            if buf.startswith(b"OK"):
                end = buf.find(b"\n\n")
                if end < 0:
                    break
                out.append(bytes(buf[:end + 2]))
                del buf[:end + 2]
            else:
                end = buf.find(b"\n")
                if end < 0:
                    break
                out.append(bytes(buf[:end + 1]))
                del buf[:end + 1]
    return out


def longest_ping_beside(port: int, query: str) -> tuple[float, int]:
    """The longest round trip, in seconds, of a PING sent on a second session while `query` is
    answered on the first - and the bytes of that answer.

    The PINGs come from a process of their own, and only those sent while the read was answered
    count. A thread of this process waited for the interpreter's lock while this one took the
    answer in, so the round trips it measured were the client's, not the node's.
    """
    clock = time.CLOCK_MONOTONIC
    pinger = subprocess.Popen([sys.executable, "-c", PINGER, str(port), str(patience(120))],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert pinger.stdout is not None
        assert pinger.stdout.readline().strip() == "ready", "the pinger did not connect"
        reader = connect(port)
        try:
            time.sleep(0.05)
            began = time.clock_gettime_ns(clock)
            reader.sendall((query + "\n").encode())
            (answer,) = answers(reader, 1)
            ended = time.clock_gettime_ns(clock)
        finally:
            reader.close()
        out, _ = pinger.communicate("stop\n", timeout=patience(30))
    finally:
        if pinger.poll() is None:
            pinger.kill()
            pinger.wait()
    assert answer.startswith(b"OK\n"), answer[:100]
    during = [rtt for sent, rtt in json.loads(out) if began <= sent <= ended]
    assert len(during) >= 3, f"{len(during)} PING(s) sent while the read was answered"
    return max(during) / 1e9, len(answer)


@pytest.fixture(scope="module")
def nodes(tmp_path_factory):
    off = Node(tmp_path_factory.mktemp("isolated"))
    on = Node(tmp_path_factory.mktemp("on_the_loop"), "--large-read-threads", "0")
    try:
        off.load()
        on.load()
        yield off, on
    finally:
        off.stop()
        on.stop()


def test_a_large_read_does_not_hold_the_sessions_beside_it(nodes):
    off, on = nodes
    # Once each, so that both read the range's columns warm.
    longest_ping_beside(off.port, WHOLE)
    longest_ping_beside(on.port, WHOLE)
    held, size_on = longest_ping_beside(on.port, WHOLE)
    beside, size_off = longest_ping_beside(off.port, WHOLE)
    assert size_off == size_on, "the answer off the loop is not the one on it"
    # On the loop a PING waits for the whole read; off it, for no more than a tenth of that.
    assert beside < held / 10, f"a PING beside the read waited {beside * 1000:.1f} ms, " \
                               f"{held * 1000:.1f} ms with the read on the loop"


def test_a_read_leaves_the_loop_when_the_index_puts_its_scan_at_the_threshold(nodes):
    off, _ = nodes
    s = connect(off.port)
    try:
        before = off.metric("ob_off_loop_reads_total")
        waits = off.metric("ob_off_loop_read_wait_seconds_count")
        # A row query reads no more than its LIMIT: one row short of the threshold stays on the loop.
        s.sendall((WHOLE + " LIMIT 99999\n").encode())
        (short,) = answers(s, 1)
        assert short.startswith(b"OK\n"), short[:100]
        assert off.metric("ob_off_loop_reads_total") == before, "a read below the threshold left the loop"
        s.sendall((WHOLE + " LIMIT 100000\n").encode())
        (at,) = answers(s, 1)
        assert at.startswith(b"OK\n"), at[:100]
        assert at.count(b"\n") == short.count(b"\n") + 1
        assert off.metric("ob_off_loop_reads_total") == before + 1, "a read at the threshold stayed on it"
        assert off.metric("ob_off_loop_reads_in_flight") == 0
        assert off.metric("ob_off_loop_read_wait_seconds_count") == waits + 1, "its wait was not observed"
    finally:
        s.close()


def test_a_sessions_answers_come_in_the_order_it_asked(nodes):
    off, _ = nodes
    ahead = [connect(off.port) for _ in range(4)]
    s = connect(off.port)
    try:
        before = off.metric("ob_off_loop_reads_total")
        # Four reads ahead of this session's on the two threads off the loop, so that its own waits
        # there long enough for a second write to come while it does.
        for a in ahead:
            a.sendall((WHOLE + "\n").encode())
        # A row made visible, the large read and a row made visible after it, in one write...
        s.sendall(b"INSERT ISO EX ask 7770001 1 1\nFLUSH\n" + (WHOLE + "\n").encode() +
                  b"INSERT ISO EX ask 7770002 1 1\nFLUSH\n")
        deadline = time.monotonic() + patience(10)
        while off.metric("ob_off_loop_reads_total") < before + 5:
            assert time.monotonic() < deadline, "the read did not leave the loop"
            time.sleep(0.001)
        assert off.metric("ob_off_loop_reads_in_flight") >= 1
        # ...and, while the read waits off the loop, a second one: a read of both rows and a PING. The
        # socket keeps them until the large read's answer is queued.
        s.sendall(b"SELECT price FROM 'ISO'.'EX' WHERE side = 1 AND timestamp BETWEEN 0 AND "
                  b"9999999999999999999\nPING\n")
        got = answers(s, 7)
        assert got[0] == b"OK\n\n" and got[1] == b"OK\n\n"
        assert got[2].startswith(b"OK\ntimestamp_ns\t"), got[2][:80]
        assert b"\t7770001\t" in got[2], "the large read did not see a write sent before it"
        assert b"\t7770002\t" not in got[2], "the large read saw a write sent after it"
        assert got[3] == b"OK\n\n" and got[4] == b"OK\n\n"
        assert b"7770001" in got[5] and b"7770002" in got[5], got[5]
        assert got[6] == b"PONG\n", got[6][:80]
    finally:
        s.close()
        for a in ahead:
            a.close()


def test_a_compressed_sessions_large_read_is_the_plain_answer_framed(nodes):
    off, _ = nodes
    plain = connect(off.port)
    framed = connect(off.port)
    try:
        query = WHOLE + " LIMIT 400000"
        plain.sendall((query + "\n").encode())
        (expected,) = answers(plain, 1)
        framed.sendall(b"COMPRESS LZ4\n")
        (ok,) = answers(framed, 1)
        assert ok == b"OK COMPRESS LZ4\n\n"
        frame = lz4.frame.compress((query + "\n").encode())
        framed.sendall(struct.pack(">I", len(frame)) + frame)
        head = b""
        while len(head) < 4:
            head += framed.recv(4 - len(head))
        (length,) = struct.unpack(">I", head)
        body = bytearray()
        while len(body) < length:
            chunk = framed.recv(min(1 << 20, length - len(body)))
            assert chunk
            body += chunk
        assert lz4.frame.decompress(bytes(body)) == expected
    finally:
        plain.close()
        framed.close()


def test_an_answer_whose_session_was_closed_goes_to_no_session_after_it(tmp_path):
    """A reload that revokes an identity closes its session while the session's read waits off the
    loop. The answer, when it comes, is dropped: the session the node accepted next has the closed
    one's descriptor - the kernel gives the lowest free one - and is given its own answers only."""
    node = Node(tmp_path, "--large-read-threads", "1", clients=f"alice {ALICE}\nbob {BOB}\n")
    sessions: list[socket.socket] = []
    try:
        node.load(auth=("alice", ALICE))
        # One read first, so that the reads after it find the columns held and open no file, which
        # would take a descriptor.
        warm = authenticated(node.port, "alice", ALICE)
        sessions.append(warm)
        warm.sendall((WHOLE + "\n").encode())
        answers(warm, 1)
        before = node.metric("ob_off_loop_reads_total")
        # Eight reads ahead of bob's on the one thread off the loop: the window the reload falls in.
        ahead = [authenticated(node.port, "alice", ALICE) for _ in range(8)]
        sessions += ahead
        bob = authenticated(node.port, "bob", BOB)
        sessions.append(bob)
        for s in ahead:
            s.sendall((WHOLE + "\n").encode())
        bob.sendall((WHOLE + "\n").encode())
        deadline = time.monotonic() + patience(10)
        while node.metric("ob_off_loop_reads_total") < before + 9:
            assert time.monotonic() < deadline, "the reads did not leave the loop"
            time.sleep(0.001)

        node.write_clients(f"alice {ALICE}\n")
        node.reload()
        got = b""
        while True:
            try:
                chunk = bob.recv(1 << 20)
            except OSError:
                break
            if not chunk:
                break
            got += chunk
        assert got == b"", "bob's read was answered before the reload closed its session: no window"
        (closed,) = node.logged(r"closing fd=(\d+) of identity 'bob'")

        adopted = len(node.logged(r"adopted fd=(\d+) conn_id=\d+"))
        after = authenticated(node.port, "alice", ALICE)
        sessions.append(after)
        given = node.logged(r"adopted fd=(\d+) conn_id=\d+", adopted + 1)[adopted]
        assert given == closed, f"the next session has fd={given}, not bob's {closed}: it shows nothing"

        deadline = time.monotonic() + patience(60)
        while node.metric("ob_off_loop_reads_in_flight") != 0:
            assert time.monotonic() < deadline, "the reads off the loop never came back"
            time.sleep(0.01)
        after.sendall(b"PING\n")
        (first,) = answers(after, 1)
        assert first == b"PONG\n", f"the session after bob's was given {first[:60]!r}"
    finally:
        for s in sessions:
            s.close()
        node.stop()
