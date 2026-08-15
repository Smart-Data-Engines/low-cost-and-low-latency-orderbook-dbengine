"""Live Binance data through a multi-master cluster losing and regaining a node.

This is the scenario `scripts/binance_collect_and_plot.py` draws as a chart: stream a
real feed into one node, take another node down mid-stream, bring it back, and see
whether the two nodes agree afterwards. A gap on the restarted node is exactly what
the chart would show as a flat line.

Synthetic data cannot substitute here. Real depth updates arrive at their own rate,
in their own sizes, and keep arriving during the outage — which is the part that
makes catch-up something other than a formality.

Opt-in and hard-skipped; see binance_support.require_binance().
"""
from __future__ import annotations

import time

import pytest

from binance_support import levels_from, require_binance, stream_depth_updates
from orderbook_engine import OrderbookEngine

require_binance()

pytestmark = pytest.mark.binance

SYMBOL = "BTC-USDT"

# One exchange label per test. Sharing a label across tests in a module-scoped
# cluster means the second test counts the first one's rows: the original version of
# this file asserted 72 and found 255.
EXCHANGE_CATCHUP = "BINANCE-SYNC-CATCHUP"
EXCHANGE_SURVIVE = "BINANCE-SYNC-SURVIVE"


def stream_into(node, seconds: float, exchange: str) -> int:
    """Stream live depth into one node for a while. Returns levels written."""
    client = OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=30)
    written = 0
    try:
        for msg in stream_depth_updates(seconds):
            for side in ("bid", "ask"):
                levels = levels_from(msg, side)
                if not levels:
                    continue
                client.insert(SYMBOL, exchange, side,
                              [p for p, _ in levels], [q for _, q in levels])
                written += len(levels)
        client.flush()
    finally:
        client.close()
    return written


def row_count(node, exchange: str, timeout: float = 30.0) -> int:
    client = OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=timeout)
    try:
        client.flush()
        return len(client.query_all(SYMBOL, exchange))
    finally:
        client.close()


def wait_for_count(node, exchange: str, expected: int, timeout: float = 60.0) -> int:
    deadline = time.monotonic() + timeout
    count = 0
    while time.monotonic() < deadline:
        count = row_count(node, exchange)
        if count >= expected:
            return count
        time.sleep(1.0)
    return count


def test_a_restarted_node_catches_up_on_live_data(healthy_mm_cluster):
    """Kill a node mid-stream, restart it, and the two must end up agreeing."""
    cluster = healthy_mm_cluster
    writer = cluster.nodes[0]
    victim = cluster.nodes[1]

    written_before = stream_into(writer, seconds=5.0,
                                 exchange=EXCHANGE_CATCHUP)
    if written_before == 0:
        pytest.skip("no live levels arrived in the first window")

    # The victim should already have the first batch before it goes down, otherwise
    # the test proves nothing about what it missed versus what it never had.
    assert wait_for_count(victim, EXCHANGE_CATCHUP, written_before,
                          timeout=45) >= written_before, (
        "the cluster was not converged before the outage began")

    cluster.kill_node(victim.index)
    time.sleep(1.0)

    written_during = stream_into(writer, seconds=5.0,
                                 exchange=EXCHANGE_CATCHUP)
    if written_during == 0:
        pytest.skip("no live levels arrived during the outage window")

    cluster.restart_node(victim.index)
    # A node that has been away from a live feed replays a WAL on the way up.
    cluster.wait_for_mm_mesh(timeout=90)

    total = written_before + written_during
    on_writer = wait_for_count(writer, EXCHANGE_CATCHUP, total, timeout=45)
    on_victim = wait_for_count(victim, EXCHANGE_CATCHUP, total, timeout=90)

    assert on_writer == total, (
        f"the node that stayed up holds {on_writer} of {total} levels it accepted")
    assert on_victim == total, (
        f"the restarted node holds {on_victim} of {total}; it is missing "
        f"{total - on_victim} levels written while it was down")


def test_live_stream_survives_a_peer_disappearing(healthy_mm_cluster):
    """Writes must not fail because another node died mid-stream."""
    cluster = healthy_mm_cluster
    writer = cluster.nodes[0]
    victim = cluster.nodes[2]

    cluster.kill_node(victim.index)
    time.sleep(1.0)

    written = stream_into(writer, seconds=5.0, exchange=EXCHANGE_SURVIVE)
    if written == 0:
        pytest.skip("no live levels arrived in the window")

    assert row_count(writer, EXCHANGE_SURVIVE) == written, (
        "levels the server acknowledged while a peer was down are not all there")
