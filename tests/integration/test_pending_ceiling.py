"""Crossing the pending-row ceiling costs a flush, not a flush interval (#137).

`apply_delta_impl()` blocks a writer at `MAX_PENDING_ROWS` and waits on a condition variable that
only the flush notifies — and nothing asked the flush to run on account of a writer waiting, so
the wait was for `--flush-interval-ms` to elapse. The work being waited for is a flush of a
million rows, measured at **73 ms** on an m9g.xlarge. At the twenty seconds this module sets, the
old code paid the twenty; the new code pays the 73 ms.

Over the wire on purpose, and with a **bound rather than a comparison**: the writer here is the
node's epoll thread, so what the defect costs is not "the write is slow" but "the node is not
answering anybody", and a bound is the honest shape for that. Twenty seconds of interval against
a fifteen-second budget means the pre-fix code cannot pass however fast the machine is, and the
post-fix code has an order of magnitude of room.
"""
from __future__ import annotations

import time

import pytest

from conftest import ClusterManager, patience
from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.smoke

SYMBOL = "CEILING"
EXCHANGE = "EX"
LEVELS_PER_WRITE = 1000
WRITES = 1100                       # 1,100,000 rows: past MAX_PENDING_ROWS with room to spare
FLUSH_INTERVAL_MS = 20_000
BUDGET_SECONDS = 15.0


@pytest.fixture(scope="module")
def slow_flush_cluster():
    """A cluster whose flush interval is long enough that waiting for it is unmistakable.

    Module-scoped: this writes a million rows, which is not a neighbour the session cluster's
    other tests deserve — the same reason `heavy_cluster` and `rotating_cluster` exist.
    """
    mgr = ClusterManager()
    mgr.extra_node_args = ["--flush-interval-ms", str(FLUSH_INTERVAL_MS)]
    mgr.start()
    try:
        yield mgr
    finally:
        mgr.shutdown()


def test_crossing_the_ceiling_costs_a_flush_and_not_an_interval(slow_flush_cluster):
    node = slow_flush_cluster.primary()
    client = OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=120.0)
    prices = list(range(9_000_000, 9_000_000 + LEVELS_PER_WRITE))
    qtys = [1 + (i % 97) for i in range(LEVELS_PER_WRITE)]
    try:
        started = time.perf_counter()
        for _ in range(WRITES):
            client.insert(SYMBOL, EXCHANGE, "bid", prices, qtys)
        elapsed = time.perf_counter() - started
    finally:
        client.close()

    budget = patience(BUDGET_SECONDS)
    assert elapsed < budget, (
        f"{WRITES * LEVELS_PER_WRITE} rows took {elapsed:.1f} s against a {budget:.0f} s budget "
        f"and a {FLUSH_INTERVAL_MS} ms flush interval. Past the pending-row ceiling a writer is "
        f"supposed to ask for a flush and wait for that; a figure near the interval means it is "
        f"waiting for the interval, and the thread doing the waiting is the one serving every "
        f"client."
    )

    # The control the bound needs: a run that wrote nothing would also be fast. These rows have
    # to be readable, which is also the assertion that the refusal path did not fire quietly.
    client = OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=120.0)
    try:
        client.flush()
        rows = client.query(f"SELECT * FROM '{SYMBOL}'.'{EXCHANGE}' "
                            f"WHERE timestamp BETWEEN 0 AND 9999999999999999999 LIMIT 5")
        assert len(rows) == 5, f"the rows are not there, so the timing above measured nothing: {rows}"
    finally:
        client.close()
