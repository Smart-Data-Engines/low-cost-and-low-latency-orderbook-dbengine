"""What the failover monitor does when the disk refuses the record a promotion writes — #112.

#112 closed the flush thread's half of "an escaping exception ends a thread rather than the
process" and named `monitor_loop` as one of three it had not reached. That was a prediction. With
#54's injector and an `ENOSPC` on the 32-byte `EPOCH` record a promotion writes (a 24-byte legacy
header and eight bytes of payload), measured before the boundary existed:

    "msg":"monitor_loop ended on an exception and that thread is gone: WALWriter: write failed: ..."

The process stayed alive, answered `PING`, and `ROLE` reported `REPLICA <its own replication port>`
for the forty seconds observed — a half-finished promotion had taken the leader key and nothing was
left running to finish it or take it back.

**What this module asserts is the thread, not the role, and the difference is the point.** The
boundary makes the next tick run; it does not unwind a promotion that stopped halfway, and the node
in that state still reports itself as a replica of itself. That remains a defect and has its own
roadmap item — asserting the role here would be asserting something this change does not do.

**The injector reaches one node through its own path filter.** `ClusterManager.start()` starts both
nodes together, so the shim is preloaded for the cluster and `OB_FAULT_PATH=ob_node1_` matches only
the replica's data directory — `mkdtemp(prefix="ob_node{index}_")` for nodes and `ob_etcd_` for the
coordinator, so neither the primary nor etcd can match. The alternative, arming the environment
between two starts, is not available here because this fixture starts both at once.
"""

from __future__ import annotations

import contextlib
import os
import time

import pytest

from conftest import (ClusterManager, fault_injector_path, patience, tail_node_log,
                      wait_for_role)

pytestmark = pytest.mark.failover

# The EPOCH record `promote_to_primary()` writes: a 24-byte legacy header and an 8-byte payload.
EPOCH_RECORD_BYTES = "32"


@contextlib.contextmanager
def _armed(fault_log: str, size: str):
    lib = fault_injector_path()
    assert lib, ("libobfault.so is not built, so this module would inject nothing and pass. Build "
                 "the `obfault` target; both integration jobs in CI do (#54).")
    keys = ("LD_PRELOAD", "OB_FAULT_PATH", "OB_FAULT_OP", "OB_FAULT_ERRNO", "OB_FAULT_SIZE",
            "OB_FAULT_LOG")
    before = {k: os.environ.get(k) for k in keys}
    os.environ.update({
        "LD_PRELOAD": lib,
        # The second node's data directory and nothing else: `ob_node1_…` for the replica,
        # `ob_node0_…` for the primary, `ob_etcd_…` for the coordinator.
        "OB_FAULT_PATH": "ob_node1_",
        "OB_FAULT_OP": "write",
        "OB_FAULT_ERRNO": "ENOSPC",
        "OB_FAULT_SIZE": size,
        "OB_FAULT_LOG": fault_log,
    })
    try:
        yield
    finally:
        for key, value in before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _injections(fault_log: str) -> list:
    if not os.path.exists(fault_log):
        return []
    with open(fault_log) as handle:
        return [line for line in handle.read().splitlines() if line.strip()]


def _metric(port: int, name: str) -> float:
    import urllib.request
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        body = response.read().decode()
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        key, _, value = line.rpartition(" ")
        if key.split("{")[0] == name:
            return float(value)
    raise AssertionError(f"{name} is not in /metrics at all, which is not the same as zero")


def _cluster_with_faulted_replica(size: str, tmp_path):
    fault_log = str(tmp_path / "fault.log")
    mgr = ClusterManager()
    with _armed(fault_log, size):
        mgr.start()
    # The premise, asserted rather than relied on: `OB_FAULT_PATH=ob_node1_` is the right filter
    # only because `start()` waits for node-0 to hold PRIMARY *before* it starts node-1, so the
    # faulted node is the replica by construction. If that ordering ever changes, the injector
    # would refuse the **primary's** startup promotion instead — which exits the process, as
    # measured — and this module would fail somewhere downstream of the reason.
    assert mgr.replica().index == 1, (
        f"the faulted node is not the replica: replica is index {mgr.replica().index}. Read "
        f"`ClusterManager.start()` before changing OB_FAULT_PATH — the filter names a data "
        f"directory, and which node gets it is the fixture's ordering, not this test's choice")
    return mgr, fault_log


def test_a_refused_epoch_record_costs_a_tick_and_not_the_monitor_thread(tmp_path):
    """The recovery line is the assertion, because it can only be written by a *later* tick.

    "The process is alive" was true when the thread was gone — it answered `PING` throughout. The
    only outward difference between a live monitor loop and a dead one is whether anything it does
    happens again, and the line that closes the episode is exactly that: the tick after the one
    that threw.
    """
    mgr, fault_log = _cluster_with_faulted_replica(EPOCH_RECORD_BYTES, tmp_path)
    try:
        replica = mgr.replica()
        mgr.kill_node(mgr.primary().index)

        deadline = time.monotonic() + patience(60)
        while time.monotonic() < deadline and not _injections(fault_log):
            time.sleep(0.5)
        fired = _injections(fault_log)
        assert any("action=fail" in line for line in fired), (
            f"nothing was refused, so this test measured an engine with no fault to survive. "
            f"Injector decisions: {fired}")

        deadline = time.monotonic() + patience(60)
        log = ""
        while time.monotonic() < deadline:
            log = tail_node_log(replica, 600)
            if "the monitor loop is running again after" in log:
                break
            time.sleep(0.5)

        assert "this monitor tick failed and the next one will be attempted" in log, (
            f"the failed tick was not reported at all:\n{log[-2000:]}")
        assert "the monitor loop is running again after" in log, (
            "no tick ran after the one that threw, which is what an ended monitor thread looks "
            f"like — and it looks identical from outside, because the node answers PING either "
            f"way (#112):\n{log[-2000:]}")
        assert "ended on an exception and that thread is gone" not in log, (
            "the monitor thread ended despite the boundary, which is the defect this test is for")

        assert _metric(replica.metrics_port, "ob_monitor_errors_total") >= 1, (
            "ob_monitor_errors_total is zero, so an operator watching a full disk would have "
            "nothing to alarm on: the node answers PING, and ROLE reports a role it half took")
    finally:
        mgr.shutdown()


def test_the_same_failover_without_a_matching_write_promotes_normally(tmp_path):
    """The control, separate so that it can fail on its own.

    Same cluster, same kill, a size no write in this engine has. Zero injections and a promotion
    that completes — so the test above says something about the boundary rather than about an
    injector that matched nothing.
    """
    mgr, fault_log = _cluster_with_faulted_replica("999", tmp_path)
    try:
        replica = mgr.replica()
        mgr.kill_node(mgr.primary().index)
        elapsed = wait_for_role(replica.tcp_port, "PRIMARY", timeout=patience(60))
        assert elapsed > 0
        assert _injections(fault_log) == [], (
            f"the injector matched something at a size nothing writes: {_injections(fault_log)}")
        assert _metric(replica.metrics_port, "ob_monitor_errors_total") == 0
        assert "this monitor tick failed" not in tail_node_log(replica, 600)
    finally:
        mgr.shutdown()
