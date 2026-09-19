"""A pipelining client does not pay a delayed-ACK timer per round trip (#140).

The engine sets TCP_NODELAY on every socket it dials and, until #140, on none it accepted. Nagle
holds a small write while an earlier byte is unacknowledged; a client that sends several commands
before reading gives the server two writes and has no reason to acknowledge the first, so the
second sat in the server's kernel until the client's delayed-ACK timer fired. Measured on one
m9g.xlarge: **52.75 ms per round trip at batch 8**, and 51.68 and 52.15 at 64 and 512 — the same
figure at three batch sizes, which is a timer rather than a cost. The same 250 round trips took
12.963 s before and 0.021 s once the client acknowledged immediately, the server unchanged.

A socket option cannot be read from the other end of a connection, so this asks about the
consequence instead. The two numbers are three orders of magnitude apart, so the threshold is not
a tuning exercise: it separates "a kernel timer fired" from "it did not", and the failure it has
to survive is a busy runner making a two-command round trip take a few milliseconds rather than
tens of microseconds. The static half of this guarantee — that the option is set at every accept
site — is tests/test_socket_options.cpp, because nothing here can see it directly.
"""
from __future__ import annotations

import socket
import statistics
import time

import pytest

pytestmark = pytest.mark.smoke

# Three times the server's own cost under instrumentation and a third of the delayed-ACK floor
# this exists to detect. The defect produces ~51.5 ms on every round trip, not occasionally.
MAX_MEDIAN_MS = 15.0
ROUND_TRIPS = 25
PER_BATCH = 2


def test_a_pipelining_client_is_not_held_for_a_delayed_ack(cluster) -> None:
    port = cluster.primary().tcp_port

    with socket.create_connection(("127.0.0.1", port), timeout=30.0) as sock:
        # On the client's own socket, so this measures the server's behaviour rather than ours.
        # TCP_QUICKACK is deliberately *not* set: the client's delayed ACK is the other half of
        # the mechanism under observation, and arming it here would hide exactly the defect.
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(30.0)
        sock.recv(4096)  # banner

        buffered = b""
        done = 0

        def await_responses(expected: int) -> None:
            nonlocal buffered
            # One buffer for the whole test rather than one per round trip: a terminator can
            # straddle two reads, and clearing between them loses it. `PING` is answered `PONG\n`
            # with no blank line, so that is what gets counted.
            while buffered.count(b"PONG\n") < expected:
                chunk = sock.recv(1 << 16)
                assert chunk, "the server closed the connection mid-measurement"
                buffered += chunk

        batch = b"PING\n" * PER_BATCH
        sock.sendall(batch)          # one warm-up round trip, not measured
        done += PER_BATCH
        await_responses(done)

        samples = []
        for _ in range(ROUND_TRIPS):
            start = time.perf_counter()
            sock.sendall(batch)
            done += PER_BATCH
            await_responses(done)
            samples.append((time.perf_counter() - start) * 1000.0)

    median = statistics.median(samples)
    assert median < MAX_MEDIAN_MS, (
        f"a {PER_BATCH}-command pipelined round trip took a median of {median:.1f} ms "
        f"(min {min(samples):.1f}, max {max(samples):.1f}) over {ROUND_TRIPS} round trips. "
        "A figure near the delayed-ACK timer means the accepted socket is back under Nagle."
    )
