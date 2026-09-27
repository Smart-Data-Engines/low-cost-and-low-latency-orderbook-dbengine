"""A mesh node holding more files than a 16-bit index can name bootstraps a peer that joins (#176).

A mesh snapshot named each file by a `uint16_t` in its chunk header, and 0xFFFF was the metadata
blob's, so `begin_snapshot_send()` refused a manifest of 65 535 files or more with
`too_many_files`. A segment is eight files, so a node of 8 192 segments could bootstrap no peer -
not one that joined, and not one too far behind to catch up. And that is not a store a mesh
reaches only by neglect: part 2b of #165 merges a symbol's segments, but not below one a symbol an
hour, so 8 192 instruments are past it whatever merging does.

Measured before the fix, the joiner of this test never got as far as the refusal: at 8 200
(symbol, origin) entries a version vector is sent as "send everything", and a joiner asks for a
snapshot only from a peer whose vector says what it holds (#177). It caught up from the WAL
instead, which its peers still held. So the marker names both, and the test says which one it met.

The module has its own three-node mesh, as `test_mm_snapshot_bootstrap.py` does, because the test
adds a fourth node for good and fills every node with 8 200 segments.
"""
from __future__ import annotations

import re
import time
import urllib.request

import pytest

from conftest import node_log_size, node_log_since
from orderbook_engine import BookUpdate, OrderbookEngine

pytestmark = pytest.mark.multi_master

# Past 65 535 files at eight a segment, with room: one segment per symbol after one flush.
SYMBOLS = 8_200
EXCHANGE = "MANY"
BASE_TS = 1_700_000_000_000_000_000
SEGMENTS_TIMEOUT = 120.0
BOOTSTRAP_TIMEOUT = 180.0


class NotBootstrapped(AssertionError):
    """The joiner received no snapshot. The one failure the #176 marker is about.

    Its own type so the strict xfail below covers only it: a premise that did not hold - a node
    with fewer segments than the test needs - fails as itself rather than as the defect.
    """


def symbol(i: int) -> str:
    return f"F{i:05d}"


def client_for(node, timeout: float = 30.0) -> OrderbookEngine:
    return OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=timeout)


def scrape(port: int, timeout: float = 6.0) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=timeout) as resp:
        return resp.read().decode(errors="replace")


def metric_value(body: str, name: str) -> float:
    match = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)$", body, re.M)
    return float(match.group(1)) if match else 0.0


def metric(node, name: str) -> float:
    try:
        return metric_value(scrape(node.metrics_port), name)
    except Exception:  # noqa: BLE001 - a node still coming up has no answer yet
        return -1.0


@pytest.mark.xfail(strict=True, raises=NotBootstrapped,
                   reason="#177: past 1 561 vector entries a joiner never asks for a snapshot; "
                          "#176: a snapshot of 65 535 files or more is refused")
def test_a_node_of_more_than_8192_segments_bootstraps_a_joiner(mm_cluster):
    writer = mm_cluster.nodes[0]
    client = client_for(writer)
    try:
        updates = [BookUpdate(symbol(i), EXCHANGE, "bid", [100_000 + i], [5],
                              timestamp_ns=BASE_TS + i)
                   for i in range(SYMBOLS)]
        for start in range(0, SYMBOLS, 512):
            outcomes = client.insert_batch(updates[start:start + 512])
            refused = [o for o in outcomes if not o.ok]
            assert not refused, f"writes refused: {refused[:3]}"
    finally:
        client.close()

    # Every node holds every symbol as a segment of its own, so whichever peer serves the joiner
    # serves a store past the old limit. A flush seals what a node holds; one that had not yet
    # received every record seals the rest at the next, so flush until the count says so.
    deadline = time.monotonic() + SEGMENTS_TIMEOUT
    counts = {}
    for node in mm_cluster.nodes:
        while True:
            c = client_for(node)
            try:
                c.flush()
            finally:
                c.close()
            counts[node.index] = metric(node, "ob_segment_count")
            if counts[node.index] >= SYMBOLS or time.monotonic() > deadline:
                break
            time.sleep(1.0)
    assert all(v >= SYMBOLS for v in counts.values()), (
        f"the premise: every node holds {SYMBOLS} segments or more, and they hold {counts}")

    offsets = {n.index: node_log_size(n) for n in mm_cluster.nodes}
    started = time.monotonic()
    joiner = mm_cluster.add_multi_master_node(timeout=60)
    mm_cluster.wait_for_mm_mesh(timeout=90)

    # Three ways out, and each says which: the snapshot arrives; a peer refuses it (#176); or the
    # joiner never asks (#177) - which it would have done within the handshake's two-second grace,
    # so twenty seconds of silence is an answer rather than a slow machine.
    peers = mm_cluster.nodes[:-1]
    failed_before = sum(metric(n, "ob_mm_snapshot_failed_total") for n in peers)
    formed = time.monotonic()
    deadline = formed + BOOTSTRAP_TIMEOUT
    outcome = ""
    while time.monotonic() < deadline:
        if metric(joiner, "ob_mm_snapshot_received_total") >= 1.0:
            break
        if sum(metric(n, "ob_mm_snapshot_failed_total") for n in peers) > failed_before:
            outcome = "a peer refused the snapshot (#176)"
            break
        if (metric(joiner, "ob_mm_snapshot_requested_total") < 1.0
                and time.monotonic() - formed > 20.0):
            outcome = "the joiner never asked for a snapshot (#177)"
            break
        time.sleep(0.5)
    else:
        outcome = f"no snapshot arrived within {BOOTSTRAP_TIMEOUT:.0f} s"
    if outcome:
        said = []
        for n in mm_cluster.nodes:
            for line in node_log_since(n, offsets.get(n.index, 0)).splitlines():
                if re.search(r"Refusing snapshot|Asked peer|Bootstrap|snapshot for peer", line):
                    said.append(f"node {n.index}: {line[:300]}")
        raise NotBootstrapped(outcome + "; what the nodes said about it:\n" + "\n".join(said[-12:]))
    elapsed = time.monotonic() - started

    served = sum(metric(n, "ob_mm_snapshot_sent_total") for n in mm_cluster.nodes[:-1])
    assert served >= 1.0, "a snapshot arrived that nobody recorded sending"

    # What the joiner holds is every segment's rows - the first symbol, the last, one between.
    joiner_client = client_for(joiner)
    try:
        for i in (0, SYMBOLS // 2, SYMBOLS - 1):
            rows = joiner_client.query_all(symbol(i), EXCHANGE)
            assert len(rows) == 1, f"{symbol(i)}: {len(rows)} rows on the joiner, 1 written"
        assert metric(joiner, "ob_segment_count") >= SYMBOLS
        # And it takes writes, which a node that never left its bootstrap would refuse.
        joiner_client.insert(symbol(0), EXCHANGE, "ask", [999_000], [1], timestamp_ns=BASE_TS - 1)
    finally:
        joiner_client.close()
    print(f"joined and bootstrapped from a node of {min(counts.values()):.0f} segments "
          f"in {elapsed:.1f} s")
