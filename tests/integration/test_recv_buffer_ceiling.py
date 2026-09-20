"""#143: one session may not hold unbounded unparsed input.

`--max-line-length` bounds a **line**, and the check runs on a line `Session::feed()` has already
assembled. A client that never sends a newline never assembles one, so that check never runs for it
and the receive buffer grew without limit. Measured before the fix, on the m9g.xlarge: 227 MiB sent
on one connection took the server's resident memory to **257 MiB**, and the connection needs no
authentication to get there.

There are two routes to the same accumulation and both are tested, because they are bounded by
different code and a fix for one would leave the other:

1. bytes with no newline at all, which sit in the receive buffer;
2. a `MINSERT` that announces levels and then sends oversized payload lines — those lines **do**
   end in newlines, but they never come back from `feed()`, so the per-line check cannot see them
   either.

The control matters more than either: a legitimate large `MINSERT` has to keep working. A ceiling
set low enough to break real traffic would pass both refusals above and be a worse defect than the
one it closes.
"""
from __future__ import annotations

import os
import socket
import sys

sys.path.insert(0, os.path.dirname(__file__))

from conftest import patience
from orderbook_engine import OrderbookEngine

CEILING = 512 * 1024          # TcpServerConfig::max_unparsed_bytes, twice max_line_length


def _metric(port: int, name: str) -> float:
    import urllib.request
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as r:
        body = r.read().decode()
    for line in body.splitlines():
        if line.startswith("#") or not line.startswith(name):
            continue
        # The exposition carries a label set: `name{node_role="standalone"} 3` (pitfall 66).
        return float(line.rsplit(" ", 1)[1])
    return 0.0


def _read_banner(sock: socket.socket) -> None:
    sock.settimeout(patience(20))
    sock.recv(4096)


def _rss_kib(node) -> int:
    """The node's resident memory, which is the quantity this defect was about."""
    with open(f"/proc/{node.process.pid}/status", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("VmRSS"):
                return int(line.split()[1])
    raise AssertionError("no VmRSS for the node, so this test cannot measure what it is about")


def test_a_client_that_never_sends_a_newline_is_dropped(cluster):
    """Route one: no newline, so no line, so nothing per-line can refuse it.

    Asserted on **resident memory**, because that is the defect rather than a proxy for it: before
    the fix 227 MiB on the wire took the server from 1.9 MiB to 257 MiB. The counter is asserted
    too, so a run where something unrelated closed the connection says so instead of passing.
    """
    primary = cluster.primary()
    before_rss = _rss_kib(primary)
    before = _metric(primary.metrics_port, "ob_sessions_unparsed_overflow_total")

    sock = socket.create_connection(("127.0.0.1", primary.tcp_port), timeout=patience(20))
    try:
        _read_banner(sock)
        chunk = b"A" * (64 * 1024)
        sent = 0
        for _ in range(64):                       # up to 4 MiB, eight times the ceiling
            try:
                sock.sendall(chunk)
                sent += len(chunk)
            except OSError:
                break                             # refused, which is the expected end
    finally:
        sock.close()

    after = _metric(primary.metrics_port, "ob_sessions_unparsed_overflow_total")
    assert after > before, (
        f"ob_sessions_unparsed_overflow_total did not move ({before} -> {after}) after {sent} "
        f"bytes of one unterminated line, so whatever ended that connection was not this "
        f"ceiling — and a test that passes on an unrelated close is the one failure this cannot "
        f"afford")

    grew = _rss_kib(primary) - before_rss
    assert grew < 4 * 1024, (
        f"the node grew {grew} KiB while being sent {sent} bytes of one unterminated line. The "
        f"ceiling is {CEILING} bytes, so growth of the order of what was sent means the bytes "
        f"were accumulated rather than refused")

    # And the refusal is per session: the node has to keep serving everyone else.
    client = OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=patience(30))
    try:
        pong = client.ping()
        assert pong == "PONG", (
            f"the node answered {pong!r} rather than PONG after dropping one abusive session; "
            f"the refusal has to end that session and nothing else")
    finally:
        client.close()


def test_a_minsert_that_never_completes_is_dropped(cluster):
    """Route two: the payload lines end in newlines and still never reach the per-line check."""
    port = cluster.primary().tcp_port
    before = _metric(cluster.primary().metrics_port, "ob_sessions_unparsed_overflow_total")

    sock = socket.create_connection(("127.0.0.1", port), timeout=patience(20))
    try:
        _read_banner(sock)
        # Announce a thousand levels, then send payload lines that are each far too long. They are
        # newline-terminated, so `feed()` consumes them into the pending block and returns nothing.
        sock.sendall(b"MINSERT RECVCEIL EX bid 1000\n")
        line = b"1 " + b"9" * (32 * 1024) + b"\n"
        for _ in range(40):                        # ~1.3 MiB of payload, well past the ceiling
            try:
                sock.sendall(line)
            except OSError:
                break
    finally:
        sock.close()

    after = _metric(cluster.primary().metrics_port, "ob_sessions_unparsed_overflow_total")
    assert after > before, (
        f"a MINSERT collecting oversized payload lines was not refused ({before} -> {after}); "
        f"those lines never come back from feed(), so the per-line length check cannot see them")


def test_a_legitimate_large_minsert_still_works(cluster):
    """The control, and it is the one that would catch a ceiling set too low.

    A thousand levels is what `max_line_length`'s own comment says the protocol supports, so it has
    to go through. Without this, a ceiling below real traffic would pass both refusals above.
    """
    port = cluster.primary().tcp_port
    client = OrderbookEngine(host="127.0.0.1", port=port, timeout=patience(60))
    try:
        prices = list(range(100_000, 100_000 + 1000))
        client.insert("RECVCEIL-OK", "EX", "bid", prices, [10] * 1000)
        client.flush()
        rows = client.query("SELECT * FROM 'RECVCEIL-OK'.'EX'")
    finally:
        client.close()

    assert len(rows) == 1000, (
        f"a 1000-level MINSERT returned {len(rows)} rows; the ceiling is refusing traffic the "
        f"protocol documents as supported")
