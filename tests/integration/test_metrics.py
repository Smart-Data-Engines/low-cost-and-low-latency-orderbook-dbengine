"""Prometheus endpoint and STATUS: the numbers an operator would page on.

A metric that exists but never moves is worse than a missing one, because a
dashboard built on it looks healthy. So these tests check that counters respond to
work, not merely that the endpoint answers.
"""
from __future__ import annotations

import re
import urllib.error
import urllib.request

import pytest

from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.metrics


def scrape(port: int, timeout: float = 6.0) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics",
                                timeout=timeout) as resp:
        return resp.read().decode(errors="replace")


def metric_value(body: str, name: str) -> float | None:
    """Read a bare counter or gauge, ignoring labelled variants."""
    match = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)$",
                      body, re.M)
    return float(match.group(1)) if match else None


def test_metrics_endpoint_responds(cluster):
    body = scrape(cluster.primary().metrics_port)
    assert body.strip(), "metrics endpoint returned an empty body"
    assert "# HELP" in body or "# TYPE" in body, (
        f"body is not in Prometheus text format: {body[:200]!r}")


def test_metrics_endpoint_on_every_node(cluster):
    for node in cluster.nodes:
        body = scrape(node.metrics_port)
        assert body.strip(), f"{node.node_id} served no metrics"


def test_unknown_path_is_not_served_as_metrics(cluster):
    """A typo in a scrape config should fail loudly, not return metrics."""
    port = cluster.primary().metrics_port
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/nope",
                                    timeout=6) as resp:
            body = resp.read().decode(errors="replace")
    except urllib.error.HTTPError as exc:
        assert exc.code in (404, 400), f"unexpected status {exc.code}"
        return
    assert "# HELP" not in body, "an unknown path served the metrics body"


def test_insert_counter_advances(primary_client: OrderbookEngine, cluster):
    """The counter has to follow real work, not just exist."""
    port = cluster.primary().metrics_port

    start = metric_value(scrape(port), "ob_total_inserts")
    assert start is not None, "ob_total_inserts is not exposed"

    for i in range(5):
        primary_client.insert("METRICS-INS", "BINANCE", "bid", [900_000 + i], [10])
    primary_client.flush()

    after = metric_value(scrape(port), "ob_total_inserts")
    assert after is not None and after >= start + 5, (
        f"ob_total_inserts moved by {None if after is None else after - start} "
        f"after 5 inserts: {start} -> {after}")


def test_flush_counter_advances(primary_client: OrderbookEngine, cluster):
    port = cluster.primary().metrics_port

    start = metric_value(scrape(port), "ob_total_flushes")
    assert start is not None, "ob_total_flushes is not exposed"

    primary_client.insert("METRICS-FLUSH", "BINANCE", "bid", [905_000], [10])
    primary_client.flush()

    after = metric_value(scrape(port), "ob_total_flushes")
    assert after is not None and after > start, (
        f"ob_total_flushes did not move across an explicit FLUSH: {start} -> {after}")


def test_latency_histograms_record_observations(primary_client: OrderbookEngine,
                                                cluster):
    """A histogram with a zero count means the instrumentation is not wired."""
    port = cluster.primary().metrics_port

    primary_client.insert("METRICS-HIST", "BINANCE", "bid", [906_000], [10])
    primary_client.flush()
    primary_client.query_all("METRICS-HIST", "BINANCE")

    body = scrape(port)
    for histogram in ("ob_insert_latency_seconds_count",
                      "ob_query_latency_seconds_count",
                      "ob_flush_latency_seconds_count"):
        count = metric_value(body, histogram)
        assert count is not None, f"{histogram} is missing"
        assert count > 0, f"{histogram} recorded no observations"


def test_status_and_metrics_agree_on_node_role(primary_client: OrderbookEngine,
                                               cluster):
    status = primary_client.status()
    body = scrape(cluster.primary().metrics_port)

    assert status.get("mode") == "tcp"
    # The role appears as a label rather than a value, so match the text.
    assert "primary" in body.lower(), (
        "metrics do not mention the primary role while STATUS reports it")


def test_segment_count_gauge_tracks_flushed_segments(primary_client: OrderbookEngine,
                                                     cluster):
    """STATUS has no segment count, so the gauge is the only view an operator gets."""
    port = cluster.primary().metrics_port
    before = metric_value(scrape(port), "ob_segment_count")
    assert before is not None, "ob_segment_count is not exposed"

    primary_client.insert("METRICS-SEG", "BINANCE", "ask", [910_000], [11])
    primary_client.flush()

    after = metric_value(scrape(port), "ob_segment_count")
    assert after is not None and after >= before, (
        f"segment count went backwards: {before} -> {after}")
    assert after > 0, "no segments registered after an explicit flush"


def test_status_reports_no_refused_segment_merges(primary_client: OrderbookEngine):
    """Non-zero here means two flush paths raced over the same segment."""
    status = primary_client.status()
    refused = status.get("segment_merge_refused")
    assert refused is not None, (
        "STATUS no longer reports segment_merge_refused; the flush race guard is "
        "invisible to operators")
    assert refused == 0, f"segment_merge_refused={refused}"
