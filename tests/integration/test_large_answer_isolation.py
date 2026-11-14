"""#260: a large read answered off the client loop, on a live node.

A client loop ran a `SELECT` to its last byte before it served another session: every session on it
waited - its `PING`, its writes - for as long as the read took. A read whose scan the index puts past
100 000 rows is answered on threads of its own now (`--large-read-threads`), and its loop goes on
serving the others; the session that asked waits for its answer, and its later commands and its socket
wait with it, so a session's answers still come in the order it asked.

One client loop (`--io-threads 1`), so every session is on it; 600 000 rows of one book. The control is
the same node with `--large-read-threads 0`, as every node was before #260. A session closed while its
read is answered off the loop - a client gone with an answer still queued for it - is checked on a node
of its own.
"""
from __future__ import annotations

import json
import re
import socket
import ssl
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
    def __init__(self, tmp: Path, *extra: str) -> None:
        self.port = free_port()
        self.metrics_port = free_port()
        self.log_path = tmp / "node.log"
        self.log = open(self.log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port), "--data-dir", str(tmp / "data"),
             "--metrics-port", str(self.metrics_port), "--io-threads", "1", *extra],
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

    def load(self, unix_socket: Path | None = None) -> None:
        if unix_socket is not None:
            client = OrderbookEngine(unix_socket=str(unix_socket), timeout=patience(120))
        else:
            client = OrderbookEngine(host="127.0.0.1", port=self.port, timeout=patience(120))
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


def banner(s: socket.socket) -> socket.socket:
    buf = b""
    while b"\n\n" not in buf:
        buf += s.recv(65536)
    return s


def connect(port: int) -> socket.socket:
    s = socket.create_connection(("127.0.0.1", port), timeout=patience(120))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    buf = b""
    while b"\n\n" not in buf:
        buf += s.recv(65536)
    return s


def answers(s: socket.socket, n: int) -> list[bytes]:
    """The next `n` answers: `OK` through its empty line, or one line of anything else. An answer's
    end is looked for in what came since the last look, so taking megabytes in is not quadratic."""
    out: list[bytes] = []
    buf = bytearray()
    looked = 0
    while len(out) < n:
        chunk = s.recv(1 << 20)
        assert chunk, "the session was closed"
        buf += chunk
        while len(out) < n and buf:
            if buf.startswith(b"OK"):
                end = buf.find(b"\n\n", max(0, looked - 1))
                if end < 0:
                    looked = len(buf)
                    break
                out.append(bytes(buf[:end + 2]))
                del buf[:end + 2]
            else:
                end = buf.find(b"\n")
                if end < 0:
                    break
                out.append(bytes(buf[:end + 1]))
                del buf[:end + 1]
            looked = 0
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
    """A session closed while its read waits off the loop is given no answer, and neither is the session
    that has its descriptor next. Bob pipelines three reads answered on the loop - 99 999 rows each,
    more than its socket and the node's take in - and a large read behind them, and resets the
    connection without reading: the loop's next flush of the answers queued fails, and closes the
    session while the large read is answered off the loop. That answer, when it comes, finds the
    session the node accepted next on bob's descriptor - the kernel gives the lowest free one - and is
    dropped by the connection's id."""
    node = Node(tmp_path, "--large-read-threads", "1", "--log-level", "DEBUG")
    sessions: list[socket.socket] = []
    try:
        node.load()
        # One read first, so that the reads after it find the columns held and open no file, which
        # would take a descriptor.
        warm = connect(node.port)
        sessions.append(warm)
        warm.sendall((WHOLE + "\n").encode())
        answers(warm, 1)
        before = node.metric("ob_off_loop_reads_total")
        # Eight reads ahead of bob's on the one thread off the loop: the window the reset falls in.
        ahead = [connect(node.port) for _ in range(8)]
        sessions += ahead
        # Bob takes little in: a receive buffer of a few kB, set before the connection says its window.
        bob = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sessions.append(bob)
        bob.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        bob.settimeout(patience(120))
        bob.connect(("127.0.0.1", node.port))
        banner(bob)
        port = bob.getsockname()[1]
        ((fd, conn),) = node.logged(rf"adopted fd=(\d+) conn_id=(\d+) from 127\.0\.0\.1:{port}\b")
        for s in ahead:
            s.sendall((WHOLE + "\n").encode())
        bob.sendall((f"{WHOLE} LIMIT 99999\n" * 3 + f"{WHOLE}\n").encode())
        deadline = time.monotonic() + patience(10)
        while node.metric("ob_off_loop_reads_total") < before + 9:
            assert time.monotonic() < deadline, "the reads did not leave the loop"
            time.sleep(0.001)

        # A reset, not a FIN that would wait behind what the node has queued for bob.
        closing = rf"Closing session: fd={fd} reason=([a-z ]+) pending_bytes=(\d+)"
        closed_before = len(re.findall(closing, node.log_path.read_text(errors="replace")))
        bob.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        bob.close()
        sessions.remove(bob)
        reason, pending = node.logged(closing, closed_before + 1)[closed_before]
        assert reason == "flush failed" and int(pending) > 0, \
            f"bob's session closed as {reason!r} with {pending} byte(s) queued: not by a failed flush"

        adopted = len(node.logged(r"adopted fd=(\d+) conn_id=\d+"))
        after = connect(node.port)
        sessions.append(after)
        given, given_conn = node.logged(r"adopted fd=(\d+) conn_id=(\d+)", adopted + 1)[adopted]
        assert given == fd, f"the next session has fd={given}, not bob's {fd}: it shows nothing"

        dropped = rf"an answer off the loop for fd={fd} conn_id={conn}, whose session has gone: dropped"
        node.logged(dropped)
        log = node.log_path.read_text(errors="replace")
        assert log.index(f"adopted fd={fd} conn_id={given_conn} ") < re.search(dropped, log).start(), \
            "bob's answer came back before the next session had its descriptor: the case is not made"
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


def test_a_tls_sessions_large_read_is_the_answer_a_plain_session_is_given(tmp_path):
    """Off the loop and through TLS: the bytes a plain session on the node's Unix socket is given, 6.7 MB
    to a reader that keeps up - in slices of a megabyte, the loop serving its other sessions between
    them (#260). A write that stops at a full socket and is asked again for more than it asked is
    tests/test_tls_session.cpp's: on the loopback the node's send buffer grows to megabytes, and only a
    buffer made small there makes such a write certain."""
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-keyout", str(key),
                    "-out", str(cert), "-days", "1", "-nodes", "-subj", "/CN=127.0.0.1",
                    "-addext", "subjectAltName=IP:127.0.0.1"],
                   check=True, capture_output=True, timeout=patience(60))
    key.chmod(0o600)
    path = tmp_path / "ob.sock"
    node = Node(tmp_path, "--tls-client", "--tls-cert-file", str(cert), "--tls-key-file", str(key),
                "--unix-socket", str(path))
    try:
        node.load(unix_socket=path)
        query = (WHOLE + " LIMIT 150000\n").encode()
        before = node.metric("ob_off_loop_reads_total")

        plain = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        plain.settimeout(patience(120))
        plain.connect(str(path))
        try:
            banner(plain).sendall(query)
            (expected,) = answers(plain, 1)
        finally:
            plain.close()
        assert expected.startswith(b"OK\ntimestamp_ns\t"), expected[:80]
        assert len(expected) > 4 * (1 << 20), "the answer is not several slices"

        context = ssl.create_default_context(cafile=str(cert))
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        s = context.wrap_socket(socket.create_connection(("127.0.0.1", node.port), timeout=patience(120)),
                                server_hostname="127.0.0.1")
        try:
            banner(s).sendall(query)
            (got,) = answers(s, 1)
        finally:
            s.close()
        assert got == expected, f"through TLS: {len(got)} bytes, {len(expected)} expected"
        assert node.metric("ob_off_loop_reads_total") == before + 2, "a read stayed on the loop"
    finally:
        node.stop()
