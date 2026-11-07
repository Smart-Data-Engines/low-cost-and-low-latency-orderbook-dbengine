"""#257: a row answer measured against what its session may be sent while it is built.

Since #152 a session is refused an answer larger than the 64 MiB it may have queued, and stays open -
but the answer was built in full to be measured: on the m9g.xlarge a `SELECT *` of eight million rows
formatted 367 MB of text and took the node's resident memory up by 912 MB to be told no, holding its
event loop for 225 ms. The answer is measured as it is built now, and the scan stops where it passed
the ceiling; on a session that compresses, what is measured is the LZ4 frame, built as the rows come.

One node and 1 200 000 rows of one book with sixteen-digit prices and quantities: about 84 MB of
`SELECT *` text, past the ceiling, and an LZ4 frame of it under the ceiling. Wide rows, so that the
battery loads as few of them as can pass the ceiling - it runs under ThreadSanitizer too.
"""

from __future__ import annotations

import re
import socket
import struct
import subprocess
import time

import lz4.frame
import pytest

from conftest import free_port, instrumented_run, patience, server_binary_path
from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.large_response

ROWS = 1_200_000
CEILING = 64 * 1024 * 1024
WHOLE = "SELECT * FROM 'CEIL'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999"
TOO_LARGE = re.compile(r"^ERR answer is larger than the 67108864 bytes a session may have queued, "
                       r"past its first (\d+) rows; narrow the query or add LIMIT\n$")


class Node:
    def __init__(self, tmp_path) -> None:
        self.port = free_port()
        self.log = open(tmp_path / "node.log", "w", encoding="utf-8")
        self.process = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port), "--data-dir", str(tmp_path / "data"),
             "--metrics-port", "0", "--drain-timeout-ms", "2000"],
            stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + patience(30)
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"node exited with {self.process.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=1):
                    return
            except OSError:
                time.sleep(0.1)
        raise AssertionError("node never accepted a connection")

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=patience(30))
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.log.close()

    def status_kb(self, key: str) -> int:
        with open(f"/proc/{self.process.pid}/status") as f:
            for line in f:
                if line.startswith(key + ":"):
                    return int(line.split()[1])
        raise KeyError(key)

    def reset_peak(self) -> None:
        with open(f"/proc/{self.process.pid}/clear_refs", "w") as f:
            f.write("5")


@pytest.fixture(scope="module")
def node(tmp_path_factory):
    n = Node(tmp_path_factory.mktemp("answer_ceiling"))
    client = OrderbookEngine(host="127.0.0.1", port=n.port, timeout=patience(120))
    for sent in range(0, ROWS, 500):
        count = min(500, ROWS - sent)
        client.insert("CEIL", "EX", "bid",
                      [1_000_000_000_000_000 + (sent + i) * 7_919 for i in range(count)],
                      [1_000_000_000_000_000 + (sent + i) * 104_729 for i in range(count)])
    client.flush()
    client.close()
    yield n
    n.stop()


def connect(port: int) -> socket.socket:
    s = socket.create_connection(("127.0.0.1", port), timeout=patience(120))
    buf = b""
    while b"\n\n" not in buf:
        buf += s.recv(65536)
    return s


def answer(s: socket.socket) -> bytes:
    """One answer: an `ERR` line, or `OK` through its empty line."""
    buf = bytearray()
    while True:
        chunk = s.recv(1 << 20)
        assert chunk, "the session was closed"
        buf += chunk
        if buf.startswith(b"ERR ") and b"\n" in buf:
            return bytes(buf)
        if buf.startswith(b"OK") and buf.endswith(b"\n\n"):
            return bytes(buf)


def framed_answer(s: socket.socket) -> tuple[int, bytes]:
    """One answer of a compressed session: its frame's length and the text it holds."""
    def exactly(n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = s.recv(min(1 << 20, n - len(buf)))
            assert chunk, "the session was closed"
            buf += chunk
        return bytes(buf)
    (length,) = struct.unpack(">I", exactly(4))
    return length, lz4.frame.decompress(exactly(length))


def test_an_answer_past_the_ceiling_is_refused_where_it_passed_it(node):
    s = connect(node.port)
    try:
        node.reset_peak()
        before = node.status_kb("VmRSS")
        s.sendall((WHOLE + "\n").encode())
        got = answer(s).decode()
        peak = node.status_kb("VmHWM")
        m = TOO_LARGE.match(got)
        assert m, got
        within = int(m.group(1))
        # About 70 bytes a row: the ceiling holds a little under a million of them.
        assert 800_000 < within < ROWS, within
        if not instrumented_run():
            # Built to the ceiling and a batch past it, not to the 84 MB of the whole answer - which
            # the answer's own doubling took past twice the ceiling in resident memory before #257.
            grew = (peak - before) * 1024
            assert grew < 2 * CEILING, f"the refused answer took the node up by {grew} bytes"

        # The session is open, and a LIMIT of the rows within the ceiling is answered whole.
        s.sendall(b"PING\n")
        assert s.recv(64) == b"PONG\n"
        s.sendall(f"{WHOLE} LIMIT {within}\n".encode())
        rows = answer(s)
        assert rows.startswith(b"OK\ntimestamp_ns\t"), rows[:80]
        assert rows.count(b"\n") == within + 3, "not the rows asked for, the header and the terminator"
        assert len(rows) <= CEILING
    finally:
        s.close()


def test_a_compressed_session_is_sent_a_frame_whose_text_is_past_the_ceiling(node):
    """The frame is what has to fit: 84 MB of text in a frame under 64 MiB is an answer, as it was
    before #257 - built now as the rows come, never the whole text beside its frame."""
    s = connect(node.port)
    try:
        s.sendall(b"COMPRESS LZ4\n")
        assert answer(s) == b"OK COMPRESS LZ4\n\n"
        frame = lz4.frame.compress((WHOLE + "\n").encode())
        s.sendall(struct.pack(">I", len(frame)) + frame)
        length, text = framed_answer(s)
        assert length <= CEILING
        assert len(text) > CEILING, "the text should be past the ceiling for this to hold anything"
        assert text.startswith(b"OK\ntimestamp_ns\t") and text.endswith(b"\n\n")
        assert text.count(b"\n") == ROWS + 3

        # The same rows as a session that does not compress is sent, as far as it can be sent them.
        plain = connect(node.port)
        try:
            plain.sendall(f"{WHOLE} LIMIT 100000\n".encode())
            first = answer(plain)
        finally:
            plain.close()
        assert text.startswith(first[:-1]), "the frame's text is not the plain answer's rows"
    finally:
        s.close()
