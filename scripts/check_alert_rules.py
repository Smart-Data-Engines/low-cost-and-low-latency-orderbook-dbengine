#!/usr/bin/env python3
"""Run the shipped alert rules against series that should fire them, and one that should not (#35).

`promtool check rules` says a rule parses; it cannot say that `OrderbookFailover` fires when the epoch
steps, or that a healthy node pages nobody. `promtool test rules` can, from series written out below:
each scenario feeds one alert the shape of the fault it is for and asserts it fires - with its labels
and with the annotations the rules file gives it, read from that file rather than written here twice -
and a healthy node's series assert that none fires.

    PROMTOOL=/path/to/promtool python3 scripts/check_alert_rules.py

Exit status 0 when every test passes, 1 otherwise, 2 without promtool.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
RULES = REPO / "packaging" / "prometheus" / "orderbook-engine-alerts.yml"
LABELS = 'instance="n1", job="orderbook", node_role="primary"'

# alert -> (input series, minutes at which it must be firing). One minute between samples.
FAULTS = {
    "OrderbookCheckpointsFrozen": ([("ob_checkpoints_frozen", "0 0 1 1 1 1")], 5),
    "OrderbookSyncErrors": ([("ob_wal_fsync_errors_total", "0 0 0 1 1 1"),
                             ("ob_segment_sync_errors_total", "0 0 0 0 0 0")], 4),
    "OrderbookWritesRefused": ([("ob_writer_backpressure_refusals_total", "0 0 0 2 2 2")], 4),
    "OrderbookWritersWaiting": ([("ob_writer_backpressure_waits_total", "0+10x30")], 25),
    "OrderbookFlushErrors": ([("ob_flush_errors_total", "0 0 1 1 1")], 3),
    "OrderbookLoopErrors": ([("ob_loop_errors_total", "0 0 0 1 1")], 4),
    "OrderbookReplicaDisconnected": ([("ob_replicas_connected", "2 2 2 2 2 1 1 1 1 1 1 1 1")], 12),
    "OrderbookReplicationLagHigh": ([("ob_replication_lag_bytes", "0 100000000x10")], 8),
    "OrderbookReplicaLagUnknown": ([("ob_replicas_lag_unknown", "0 1x10")], 8),
    "OrderbookFailover": ([("ob_current_epoch", "1 1 1 2 2 2")], 4),
    "OrderbookNoPrimaryWhileJoining": ([("ob_failover_abstaining", "0 0 1 1 1 1")], 5),
    "OrderbookMeshPeerDown": ([("ob_mm_peers_connected", "2 2 2 2 2 1 1 1 1 1 1 1 1")], 12),
    "OrderbookMeshLagHigh": ([("ob_mm_replication_lag_records", "0 200000x10")], 8),
    "OrderbookMeshPeersDropped": ([("ob_mm_peer_dropped_slow_total", "0 0 0 1 1 1"),
                                   ("ob_mm_peer_dropped_clock_total", "0 0 0 0 0 0")], 4),
    "OrderbookAuthFailures": ([("ob_auth_failures_total", "0+120x20")], 15),
    "OrderbookSubscribersDisconnected": ([("ob_subscription_overflow_disconnects_total", "0 0 0 3 3 3")], 4),
    "OrderbookBackupFailed": ([("ob_backup_failures_total", "0 0 0 1 1 1")], 4),
    # A backup at t=60 s and none since: 27 hours on, time() is past the cut by more than 26.
    "OrderbookBackupStale": ([("ob_backup_last_success_timestamp_seconds", "60x1700")], 27 * 60),
}

# alert -> (input series, minute) at which it must **not** fire: the cases a fault's shape could be
# mistaken for, beyond the healthy node below.
QUIET = {
    # A node that never took a backup exports 0, and is not paged for a schedule it does not have.
    "OrderbookBackupStale": ([("ob_backup_last_success_timestamp_seconds", "0x1700")], 27 * 60),
}

# What a healthy primary with two replicas and two mesh peers exports for twenty minutes.
HEALTHY = [
    ("ob_checkpoints_frozen", "0x20"), ("ob_wal_fsync_errors_total", "0x20"),
    ("ob_segment_sync_errors_total", "0x20"), ("ob_writer_backpressure_refusals_total", "0x20"),
    ("ob_writer_backpressure_waits_total", "0x20"), ("ob_flush_errors_total", "0x20"),
    ("ob_loop_errors_total", "0x20"), ("ob_replicas_connected", "2x20"),
    ("ob_replication_lag_bytes", "1000x20"), ("ob_replicas_lag_unknown", "0x20"),
    ("ob_current_epoch", "3x20"), ("ob_failover_abstaining", "0x20"), ("ob_mm_peers_connected", "2x20"),
    ("ob_mm_replication_lag_records", "10x20"), ("ob_mm_peer_dropped_slow_total", "0x20"),
    ("ob_mm_peer_dropped_clock_total", "0x20"), ("ob_auth_failures_total", "0x20"),
    ("ob_subscription_overflow_disconnects_total", "0x20"),
    ("ob_backup_failures_total", "0x20"), ("ob_backup_last_success_timestamp_seconds", "600x20"),
]


def rendered(text: str) -> str:
    return re.sub(r"\{\{\s*\$labels\.instance\s*\}\}", "n1", text)


def main() -> int:
    promtool = os.environ.get("PROMTOOL") or shutil.which("promtool")
    if not promtool:
        print("promtool not found: set PROMTOOL or put it on PATH")
        return 2
    rules = yaml.safe_load(RULES.read_text())
    alerts = {r["alert"]: r for g in rules["groups"] for r in g["rules"]}
    missing = sorted(set(alerts) - set(FAULTS))
    stale = sorted(set(FAULTS) - set(alerts))
    if missing or stale:
        print(f"alerts without a fault scenario: {missing}; scenarios without an alert: {stale}")
        return 1

    tests = []
    for name, (series, at) in FAULTS.items():
        rule = alerts[name]
        expected_labels = {"instance": "n1", **rule.get("labels", {})}
        # An alert aggregated `by (instance)` carries the instance and the rule's labels, no others.
        tests.append({
            "interval": "1m",
            "input_series": [{"series": f"{m}{{{LABELS}}}", "values": v} for m, v in series],
            "alert_rule_test": [{
                "eval_time": f"{at}m",
                "alertname": name,
                "exp_alerts": [{
                    "exp_labels": expected_labels,
                    "exp_annotations": {k: rendered(v) for k, v in rule["annotations"].items()},
                }],
            }],
        })
    for name, (series, at) in QUIET.items():
        assert name in alerts, f"a quiet scenario for {name}, which is not an alert"
        tests.append({
            "interval": "1m",
            "input_series": [{"series": f"{m}{{{LABELS}}}", "values": v} for m, v in series],
            "alert_rule_test": [{"eval_time": f"{at}m", "alertname": name, "exp_alerts": []}],
        })
    tests.append({
        "interval": "1m",
        "input_series": [{"series": f"{m}{{{LABELS}}}", "values": v} for m, v in HEALTHY],
        "alert_rule_test": [{"eval_time": "20m", "alertname": name, "exp_alerts": []}
                            for name in alerts],
    })

    with tempfile.TemporaryDirectory() as tmp:
        shutil.copy(RULES, Path(tmp) / RULES.name)
        test_file = Path(tmp) / "alerts.test.yml"
        test_file.write_text(yaml.safe_dump({"rule_files": [RULES.name], "evaluation_interval": "1m",
                                             "tests": tests}, sort_keys=False))
        out = subprocess.run([promtool, "test", "rules", str(test_file)], capture_output=True, text=True)
    print(out.stdout.strip())
    if out.returncode != 0:
        print(out.stderr.strip())
        return 1
    print(f"{len(FAULTS)} alerts each fire on their fault, {len(QUIET)} stay quiet where they must, "
          f"and none fires on a healthy node")
    return 0


if __name__ == "__main__":
    sys.exit(main())
