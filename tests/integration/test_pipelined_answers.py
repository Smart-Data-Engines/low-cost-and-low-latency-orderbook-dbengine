"""Every command from one read is answered with one send — #146.

The epoll loop used to send each response the moment it was produced, so a client that pipelined
64 commands got 64 `send()` calls and, with `TCP_NODELAY` on every accepted socket (#140), 64
segments. Measured on an m9g.xlarge that was 32.4% of the io thread. Now the responses to every
command parsed from one read are queued and go out together.

**What this module holds is that nothing a client can observe changed, except the count.** The
bytes and their order are the same as one command at a time, in the clear, compressed and over TLS;
`QUIT` and a line too long still deliver what came before them; and — the one that matters for
security — a failed `AUTH` still ends the batch where it stands. One attempt per connection is the
whole rate limit on authentication (#30), and "queue and keep executing" would let one read carry
a failed attempt and a second one behind it.

The performance half is not a test: a mutation that sends per response again passes everything
here, which is why a static test in `tests/test_tcp_server.cpp` pins the loop's shape and the
roadmap entry carries the measurement.
"""

from __future__ import annotations

import socket
import struct
import tempfile
import time
from pathlib import Path

import pytest

import lz4.frame
from conftest import patience
from test_auth import ALICE_SECRET, Node, client_response, write_secret_file
from test_tls import TlsNode

pytestmark = pytest.mark.smoke

T0 = 1_700_000_000_000_000_000


def commands_for(symbol: str) -> list[str]:
    """A mixed batch whose answers are deterministic: event times are given, so the book's rows
    carry the same timestamps on any connection, and sequence numbers are per symbol."""
    return [
        "PING",
        f"INSERT {symbol} EX bid 100 5 1 {T0 + 1}",
        f"INSERT {symbol} EX ask 110 7 2 {T0 + 2}",
        f"MINSERT {symbol} EX bid 3 {T0 + 3}\n99 1 1\n98 2 1\n97 3 1",
        f"BOOK {symbol} EX",
        "NOT_A_COMMAND",
        f"BOOK {symbol} EX 2",
        "PING",
    ]


def read_response(sock: socket.socket, pending: bytearray) -> bytes:
    """One response off a plaintext socket: a single line for PONG and ERR, a blank line otherwise."""
    while True:
        end = -1
        if pending.startswith(b"PONG") or pending.startswith(b"ERR"):
            end = pending.find(b"\n")
            end = end + 1 if end >= 0 else -1
        else:
            end = pending.find(b"\n\n")
            end = end + 2 if end >= 0 else -1
        if end > 0:
            out = bytes(pending[:end])
            del pending[:end]
            return out
        chunk = sock.recv(1 << 16)
        if not chunk:
            raise AssertionError(f"connection closed mid-response: {bytes(pending)!r}")
        pending += chunk


def read_until_eof(sock, limit: float = 10.0) -> bytes:
    sock.settimeout(patience(limit))
    got = bytearray()
    while True:
        chunk = sock.recv(1 << 16)
        if not chunk:
            return bytes(got)
        got += chunk


def read_exactly(sock, n: int, pending: bytearray, limit: float = 10.0) -> bytes:
    """Exactly `n` bytes, keeping whatever arrived behind them in `pending`.

    Keeping the rest is the point: with one send per read the answers to a batch arrive
    together, so a reader that returns "everything from the recv that crossed n" hands the next
    answer to nobody - which is what the first version of this helper did.
    """
    sock.settimeout(patience(limit))
    while len(pending) < n:
        chunk = sock.recv(1 << 16)
        if not chunk:
            break
        pending += chunk
    out = bytes(pending[:n])
    del pending[:n]
    return out


def connect(port: int) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=patience(10))
    banner = bytearray()
    while not banner.endswith(b"\n\n"):
        banner += sock.recv(4096)
    return sock


@pytest.fixture
def open_node():
    with tempfile.TemporaryDirectory(prefix="ob_pipelined_") as d:
        node = Node(Path(d))
        node.start()
        try:
            yield node
        finally:
            node.stop()


@pytest.fixture
def authed_node():
    with tempfile.TemporaryDirectory(prefix="ob_pipelined_auth_") as d:
        tmp = Path(d)
        secrets = write_secret_file(tmp / "clients", f"alice {ALICE_SECRET}\n")
        node = Node(tmp, auth_file=secrets)
        node.start()
        try:
            yield node
        finally:
            node.stop()


def test_a_pipelined_batch_is_answered_byte_for_byte_as_one_at_a_time(open_node):
    sequential = connect(open_node.port)
    pending = bytearray()
    one_at_a_time = b""
    for command in commands_for("SEQ"):
        sequential.sendall((command + "\n").encode())
        one_at_a_time += read_response(sequential, pending)
    sequential.close()

    pipelined = connect(open_node.port)
    pipelined.sendall(("\n".join(commands_for("PIP")) + "\n").encode())
    together = read_exactly(pipelined, len(one_at_a_time), bytearray())
    pipelined.close()

    # The control, so a pair of empty or error-only answers cannot agree with each other: the
    # sequential run has to contain the book it wrote, levels and all.
    assert b"\t100\t" in one_at_a_time and b"\t97\t" in one_at_a_time, one_at_a_time
    assert together == one_at_a_time


def test_a_batch_spanning_several_reads_is_answered_in_full_and_in_order(open_node):
    sock = connect(open_node.port)
    # The server reads 64 KiB at a time (#146) and answers each read with its own send, so the
    # order across those sends is what is being held - which needs a batch several reads long.
    # The first version of this comment called 2000 of these "about 10 kB"; they were 96 kB, so
    # the premise was true by accident and would have become false at the next read-size change.
    # It is asserted now instead of described.
    writes = 5999
    lines = [f"INSERT SPAN EX bid {1000 + i} 1 1 {T0 + i}" for i in range(writes)] + ["BOOK SPAN EX 3"]
    payload = ("\n".join(lines) + "\n").encode()
    assert len(payload) > 4 * 64 * 1024, f"{len(payload)} bytes is not several 64 KiB reads"
    sock.sendall(payload)
    pending = bytearray()
    answers = [read_response(sock, pending) for _ in lines]
    sock.close()
    assert answers[:-1] == [b"OK\n\n"] * writes
    book = answers[-1].decode()
    # The best three bids of the ones written, in the side's own order, and nothing else.
    rows = [row for row in book.splitlines()[2:] if row]   # the terminating blank line is not a row
    assert [row.split("\t")[1] for row in rows] == ["6998", "6997", "6996"], book


def test_quit_behind_a_batch_delivers_every_answer_before_it(open_node):
    sock = connect(open_node.port)
    sock.sendall(b"PING\n" * 20 + b"QUIT\n" + b"PING\n")
    assert read_until_eof(sock) == b"PONG\n" * 20
    sock.close()


def test_quit_behind_answers_still_draining_delivers_all_of_them(open_node):
    """`QUIT` behind answers too big for the socket closes once they have drained, not at once.

    The test above cannot reach this branch: twenty PONGs fit the socket buffer, so the flush
    leaves nothing pending and the deferred close never runs - a mutation closing at once passed
    it. Here a hundred full books are queued behind a receive window clamped to 64 KiB, which is
    more than the kernel will hold for this connection, so `QUIT` arrives while the session still
    has megabytes of answers to send.
    """
    levels = 1000
    seeder = connect(open_node.port)
    bids = "\n".join(f"{6_500_000 - i} {i + 1} 1" for i in range(levels))
    asks = "\n".join(f"{6_501_000 + i} {i + 1} 1" for i in range(levels))
    seeder.sendall(f"MINSERT DEEP EX bid {levels} {T0}\n{bids}\n"
                   f"MINSERT DEEP EX ask {levels} {T0 + 1}\n{asks}\n".encode())
    pending = bytearray()
    assert read_response(seeder, pending) == b"OK\n\n"
    assert read_response(seeder, pending) == b"OK\n\n"
    seeder.sendall(b"BOOK DEEP EX\n")
    one_book = read_response(seeder, pending)
    seeder.close()
    assert one_book.count(b"\n") == 2 * levels + 3, "the seed did not produce the book this needs"

    books = 100
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 16)
    sock.settimeout(patience(10))
    sock.connect(("127.0.0.1", open_node.port))
    banner = bytearray()
    while not banner.endswith(b"\n\n"):
        banner += sock.recv(4096)
    sock.sendall(b"BOOK DEEP EX\n" * books + b"QUIT\nPING\n")
    got = read_until_eof(sock, limit=60)
    sock.close()
    assert len(got) == books * len(one_book), (
        f"{len(got)} bytes arrived of {books * len(one_book)} queued before QUIT: the session "
        f"closed with answers still to send")
    assert got == one_book * books, "the answers arrived whole but not as the books that were asked"


def test_a_line_too_long_behind_a_batch_delivers_the_answers_before_it(open_node):
    sock = connect(open_node.port)
    # Over max_line_length (256 KiB) and under max_unparsed_bytes (512 KiB), so the line arrives
    # whole and is refused as a line rather than as unparsed input.
    sock.sendall(b"PING\n" * 5 + b"X" * 300_000 + b"\n")
    assert read_until_eof(sock) == b"PONG\n" * 5 + b"ERR line too long\n"
    sock.close()


def test_compression_negotiated_inside_a_batch_frames_everything_after_it(open_node):
    sock = connect(open_node.port)
    sock.sendall(b"COMPRESS LZ4\nPING\nPING\n")
    ack = b"OK COMPRESS LZ4\n\n"
    pending = bytearray()
    assert read_exactly(sock, len(ack), pending) == ack, "the ack must go out in the clear"
    frames = []
    for _ in range(2):
        (length,) = struct.unpack(">I", read_exactly(sock, 4, pending))
        frames.append(lz4.frame.decompress(read_exactly(sock, length, pending)))
    sock.close()
    assert frames == [b"PONG\n", b"PONG\n"]


def test_a_failed_auth_ends_its_batch_where_it_stands(authed_node):
    sock = connect(authed_node.port)
    sock.sendall(b"AUTH\n")
    challenge = read_response(sock, bytearray()).decode()
    assert challenge.startswith("OK CHALLENGE "), challenge
    nonce = challenge.split()[2]
    wrong = client_response("not-the-secret-not-the-secret-xx", "alice", nonce)
    right = client_response(ALICE_SECRET, "alice", nonce)
    # A second attempt, with the right answer, in the same read as the failed one, and a write
    # behind it that only an admitted session could make.
    sock.sendall(f"AUTH alice {wrong}\nAUTH alice {right}\nINSERT RATELIM EX bid 100 1 1\nPING\n"
                 .encode())
    assert read_until_eof(sock) == b"ERR auth_failed\n"
    sock.close()

    def book_of(symbol: str) -> str:
        admitted = connect(authed_node.port)
        admitted.sendall(b"AUTH\n")
        again = read_response(admitted, bytearray()).decode().split()[2]
        admitted.sendall(f"AUTH alice {client_response(ALICE_SECRET, 'alice', again)}\n".encode())
        pending = bytearray()
        assert read_response(admitted, pending).startswith(b"OK AUTH alice")
        admitted.sendall(f"BOOK {symbol} EX\n".encode())
        answer = read_response(admitted, pending).decode()
        admitted.close()
        return answer

    assert book_of("RATELIM").startswith("ERR"), "a command behind a failed AUTH was executed"

    # The control: the same second attempt and write, without a failure in front of them, is
    # admitted and writes - so the absence above is the stop, not a write this script cannot make.
    control = connect(authed_node.port)
    control.sendall(b"AUTH\n")
    nonce = read_response(control, bytearray()).decode().split()[2]
    control.sendall(f"AUTH alice {client_response(ALICE_SECRET, 'alice', nonce)}\n"
                    f"INSERT CONTROL EX bid 100 1 1\n".encode())
    pending = bytearray()
    assert read_response(control, pending).startswith(b"OK AUTH alice")
    assert read_response(control, pending) == b"OK\n\n"
    control.close()
    assert "\t100\t" in book_of("CONTROL")


def test_a_pipelined_batch_over_tls_arrives_whole_and_in_order():
    with tempfile.TemporaryDirectory(prefix="ob_pipelined_tls_") as d:
        node = TlsNode(Path(d))
        node.start()
        try:
            sock = node.connect()
            lines = [f"INSERT TLSPIPE EX bid {1000 + i} 1 1 {T0 + i}" for i in range(500)]
            sock.sendall(("\n".join(lines + ["BOOK TLSPIPE EX 1", "PING"]) + "\n").encode())
            expected_head = b"OK\n\n" * 500
            pending = bytearray()
            head = read_exactly(sock, len(expected_head), pending)
            assert head == expected_head
            book = read_response(sock, pending).decode()
            assert book.splitlines()[2].split("\t")[1] == "1499", book
            assert read_response(sock, pending) == b"PONG\n"
            sock.close()
        finally:
            node.stop()
