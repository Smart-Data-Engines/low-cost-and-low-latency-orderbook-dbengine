"""#260: a large read answered off the client loop, on a live node.

A client loop ran a `SELECT` to its last byte before it served another session: every session on it
waited - its `PING`, its writes - for as long as the read took. A read whose scan the index puts past
100 000 rows is answered on threads of its own now (`--large-read-threads`), and its loop goes on
serving the others; the session that asked waits for its answer, and its later commands and its socket
wait with it, so a session's answers still come in the order it asked.

One client loop (`--io-threads 1`), so every session is on it; 600 000 rows of one book. The control is
the same node with `--large-read-threads 0`, as every node was before #260.
"""
from __future__ import annotations

import socket
import struct
import subprocess
import threading
import time
from pathlib import Path

import lz4.frame
import pytest

from conftest import free_port, patience, server_binary_path
from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.large_response

ROWS = 600_000
WHOLE = "SELECT * FROM 'ISO'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999"


class Node:
    def __init__(self, tmp: Path, *extra: str) -> None:
        self.port = free_port()
        self.log = open(tmp / "node.log", "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port), "--data-dir", str(tmp / "data"),
             "--metrics-port", "0", "--io-threads", "1", *extra],
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

    def load(self) -> None:
        client = OrderbookEngine(host="127.0.0.1", port=self.port, timeout=patience(120))
        for sent in range(0, ROWS, 500):
            count = min(500, ROWS - sent)
            client.insert("ISO", "EX", "bid", [1_000_000 + (sent + i) % 900_000 for i in range(count)],
                          [10 + (sent + i) % 5000 for i in range(count)])
        client.flush()
        client.close()

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
    """The longest PING round trip, in seconds, on a second session while `query` is answered on the
    first - and the bytes of that answer."""
    reader, pinger = connect(port), connect(port)
    try:
        done = threading.Event()
        rtts: list[float] = []

        def ping() -> None:
            while not done.is_set():
                t = time.perf_counter()
                pinger.sendall(b"PING\n")
                got = b""
                while not got.endswith(b"\n"):
                    got += pinger.recv(64)
                rtts.append(time.perf_counter() - t)
                time.sleep(0.002)

        th = threading.Thread(target=ping)
        th.start()
        time.sleep(0.1)
        reader.sendall((query + "\n").encode())
        (answer,) = answers(reader, 1)
        time.sleep(0.05)
        done.set()
        th.join()
        assert answer.startswith(b"OK\n"), answer[:100]
        return max(rtts), len(answer)
    finally:
        reader.close()
        pinger.close()


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


def test_a_sessions_answers_come_in_the_order_it_asked(nodes):
    off, _ = nodes
    s = connect(off.port)
    try:
        # One write: a row made visible, the large read, a row made visible after it, and a small
        # read. The large read sees the first row and not the second; the small one sees both.
        s.sendall(b"INSERT ISO EX ask 7770001 1 1\nFLUSH\n" + (WHOLE + "\n").encode() +
                  b"INSERT ISO EX ask 7770002 1 1\nFLUSH\n" +
                  b"SELECT price FROM 'ISO'.'EX' WHERE side = 1 AND timestamp BETWEEN 0 AND 9999999999999999999\n")
        got = answers(s, 6)
        assert got[0] == b"OK\n\n" and got[1] == b"OK\n\n"
        assert got[2].startswith(b"OK\ntimestamp_ns\t"), got[2][:80]
        assert b"\t7770001\t" in got[2], "the large read did not see a write sent before it"
        assert b"\t7770002\t" not in got[2], "the large read saw a write sent after it"
        assert got[3] == b"OK\n\n" and got[4] == b"OK\n\n"
        assert b"7770001" in got[5] and b"7770002" in got[5], got[5]
    finally:
        s.close()


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
