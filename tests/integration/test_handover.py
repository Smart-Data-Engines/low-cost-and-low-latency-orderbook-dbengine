"""#204: a planned FAILOVER loses no acknowledged write, and leaves no primary for less than a TTL.

Measured before the fix, through the server on the m9g.xlarge: the target was primary 10.73 s after
the FAILOVER's OK - the lease TTL, because the target, like every candidate since #82, waited for
the previous holder "to have certainly stepped down" - and in nine runs of ten it lacked the last
acknowledged writes: 1 to 4 on Release, 4308 once on Debug. The outgoing primary stopped its
replication stream before it stopped taking writes, and whatever the target had not received yet
had nowhere left to come from.

Now the outgoing primary closes writes first, keeps streaming, and says in its intent that it has
stepped down and where its stream ends; the target stands as soon as it holds that much.

Each test owns its `ClusterManager`: they move the role.
"""
from __future__ import annotations

import threading
import time

import pytest

from conftest import ClusterManager, patience, role_of, send_command
from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.failover

LEASE_TTL_SECONDS = 10   # the engine's default (docs/cli.md)


@pytest.fixture
def cluster2():
    mgr = ClusterManager()
    mgr.start()
    try:
        yield mgr
    finally:
        mgr.shutdown()


def primary_index(mgr: ClusterManager, timeout: float) -> int:
    deadline = time.monotonic() + timeout
    roles: list = []
    while time.monotonic() < deadline:
        roles = [role_of(n.tcp_port) for n in mgr.nodes]
        primaries = [i for i, r in enumerate(roles) if r.startswith("PRIMARY")]
        if len(primaries) == 1:
            return primaries[0]
        time.sleep(0.25)
    raise AssertionError(f"no single PRIMARY within {timeout:.0f} s; roles were {roles}")


class Writer:
    """Inserts one level per INSERT into a node until it refuses, recording each price it was
    answered OK for."""

    def __init__(self, port: int, symbol: str, first_price: int):
        self.port, self.symbol, self.next_price = port, symbol, first_price
        self.acknowledged: list[int] = []
        self.refusal = ""
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        client = OrderbookEngine(host="127.0.0.1", port=self.port, timeout=20)
        try:
            while True:
                try:
                    client.insert(self.symbol, "EX", "bid", [self.next_price], [1])
                except Exception as exc:  # noqa: BLE001 - the first refusal ends the writer
                    self.refusal = str(exc)
                    return
                self.acknowledged.append(self.next_price)
                self.next_price += 1
        finally:
            client.close()

    def start(self) -> "Writer":
        self._thread.start()
        return self

    def join(self) -> None:
        self._thread.join(timeout=patience(30))
        assert not self._thread.is_alive(), "the writer was never refused by the node it wrote to"


def prices_on(port: int, symbol: str) -> set[int]:
    reader = OrderbookEngine(host="127.0.0.1", port=port, timeout=30)
    try:
        return {row.price for row in reader.query_all(symbol, "EX")}
    finally:
        reader.close()


def hand_over(mgr: ClusterManager, old: int, new: int) -> float:
    """FAILOVER from `old` to `new`; returns the seconds from its answer to `new` answering PRIMARY."""
    try:
        reply = send_command(mgr.nodes[old].tcp_port, f"FAILOVER {mgr.nodes[new].node_id}").strip()
    except Exception as exc:  # noqa: BLE001 - the step-down can race the reply (#86)
        reply = f"(no reply: {exc})"
    answered = time.monotonic()
    assert not reply.startswith("ERR"), f"handover refused: {reply!r}"
    deadline = answered + patience(LEASE_TTL_SECONDS * 3)
    while time.monotonic() < deadline:
        if role_of(mgr.nodes[new].tcp_port).startswith("PRIMARY"):
            return time.monotonic() - answered
        time.sleep(0.05)
    raise AssertionError(f"{mgr.nodes[new].node_id} never became PRIMARY after the handover")


def test_a_planned_failover_loses_no_acknowledged_write(cluster2):
    """Three handovers, there and back and there again, each under a writer that never pauses:
    every price the outgoing primary answered OK for is on its successor."""
    mgr = cluster2
    symbol = "HANDOVER-ACKED"
    old = primary_index(mgr, timeout=patience(60))
    acknowledged: list[int] = []
    first_price = 1_000_000
    for round_no in range(3):
        new = 1 - old
        # The target has caught up with the rounds before - after a handover the old primary
        # re-syncs from its successor (#201) - so what is measured is the handover, not a bootstrap.
        deadline = time.monotonic() + patience(60)
        while acknowledged and time.monotonic() < deadline:
            if set(acknowledged) <= prices_on(mgr.nodes[new].tcp_port, symbol):
                break
            time.sleep(0.5)
        writer = Writer(mgr.nodes[old].tcp_port, symbol, first_price).start()
        time.sleep(2.0)
        hand_over(mgr, old, new)
        writer.join()
        assert writer.acknowledged, f"round {round_no}: nothing was written before the handover"
        assert "read-only" in writer.refusal, (
            f"round {round_no}: the outgoing primary refused a write with {writer.refusal!r}")
        acknowledged += writer.acknowledged
        first_price = writer.next_price + 1

        have = prices_on(mgr.nodes[new].tcp_port, symbol)
        lost = [p for p in acknowledged if p not in have]
        assert not lost, (
            f"round {round_no}: {len(lost)} of {len(acknowledged)} acknowledged writes are not on "
            f"the new primary {mgr.nodes[new].node_id}, the first {lost[:5]}")
        old = new


def test_a_planned_failover_leaves_no_primary_for_under_half_the_ttl(cluster2):
    mgr = cluster2
    old = primary_index(mgr, timeout=patience(60))
    took = hand_over(mgr, old, 1 - old)
    # A tick to see the vacant key and a round trip to take it. The election wait is the TTL.
    assert took < patience(LEASE_TTL_SECONDS / 2), (
        f"the target was PRIMARY {took:.2f} s after the handover's OK - the election wait of "
        f"{LEASE_TTL_SECONDS} s, which a target holding the outgoing stream has no reason to wait")
