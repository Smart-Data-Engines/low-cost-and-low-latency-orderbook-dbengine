"""Failover on a cluster nobody started politely.

Every other failover test here runs on the shared fixture, whose `start()` says out loud why:
"Nodes are started sequentially: node-0 starts first and becomes PRIMARY, then node-1 starts and
discovers the leader via etcd... This avoids a race condition where both nodes start simultaneously
and one fails to transition from STANDALONE."

That workaround was a bug report nobody filed. A node that lost the startup CAS stayed at
STANDALONE, `monitor_loop()` branched only on PRIMARY and REPLICA, so the loser never campaigned,
never replicated and never took over — the primary could die and the cluster simply had no primary
any more. The same dead end swallowed a node that booted while etcd was down, because `start()`
returned before the monitor thread existed (roadmap #73).

So this module starts its cluster the way a `systemd` unit, an Ansible play or a start-all script
starts one: everything at once, and sometimes before the coordinator answers.
"""
from __future__ import annotations

import base64
import json
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

import pytest

pytestmark = pytest.mark.failover

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SERVER = os.path.join(REPO, "build", "ob_tcp_server")
ETCD = os.environ.get("OB_ETCD_BINARY") or shutil.which("etcd") or "/usr/local/bin/etcd"

LEASE_TTL = 5
# Lease expiry, then the deference window (#70), then the promotion itself. Generous on purpose:
# the assertion is that a promotion happens at all, not that it is quick.
PROMOTION_BUDGET = 45.0


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def command(port: int, text: str, settle: float = 0.4) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=10) as s:
        s.settimeout(10)
        s.recv(4096)  # banner
        s.sendall(text.encode())
        time.sleep(settle)
        try:
            return s.recv(1 << 16).decode(errors="replace")
        except socket.timeout:
            return ""


def etcd_positions(etcd_port: int) -> list[str]:
    """Node ids that currently have a WAL position published in etcd.

    A local copy on purpose: every module in this suite stands on its own, and `test_failover.py`
    has its own for the shared-cluster tests.
    """
    payload = {"key": base64.b64encode(b"/ob/nodes/").decode(),
               "range_end": base64.b64encode(b"/ob/nodes0").decode()}
    req = urllib.request.Request(
        f"http://127.0.0.1:{etcd_port}/v3/kv/range", data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
    return sorted(base64.b64decode(kv["key"]).decode().rsplit("/", 1)[-1]
                  for kv in json.loads(body).get("kvs", []))


def role_of(port: int) -> str:
    """First line of ROLE, or a marker when the node cannot be reached."""
    try:
        return command(port, "ROLE\n").strip().split("\n")[0]
    except OSError as exc:
        return f"<unreachable: {type(exc).__name__}>"


class Cluster:
    """etcd plus N nodes, started in whatever order the test asks for."""

    def __init__(self, name: str):
        self.root = tempfile.mkdtemp(prefix=f"ob_{name}_")
        self.etcd: subprocess.Popen | None = None
        self.etcd_port = 0
        self.procs: list[subprocess.Popen] = []
        self.ports: list[list[int]] = []
        self._logs: list = []

    # ── lifecycle ────────────────────────────────────────────────

    def reserve_etcd_port(self) -> str:
        """Claim the coordinator's port before it exists, so a node can be told where to look."""
        self.etcd_port = free_port()
        return f"http://127.0.0.1:{self.etcd_port}"

    def start_etcd(self, wait: float = 3.0) -> str:
        url = self.reserve_etcd_port() if not self.etcd_port else f"http://127.0.0.1:{self.etcd_port}"
        peer = free_port()
        log = open(os.path.join(self.root, "etcd.log"), "wb")
        self._logs.append(log)
        self.etcd = subprocess.Popen(
            [ETCD, "--name", "deadstate", "--data-dir", os.path.join(self.root, "etcd"),
             "--advertise-client-urls", url, "--listen-client-urls", url,
             "--listen-peer-urls", f"http://127.0.0.1:{peer}",
             "--initial-advertise-peer-urls", f"http://127.0.0.1:{peer}",
             "--initial-cluster", f"deadstate=http://127.0.0.1:{peer}"],
            stdout=log, stderr=subprocess.STDOUT)
        time.sleep(wait)
        return url

    def spawn(self, index: int, etcd_url: str) -> None:
        """Launch a node without waiting for it to settle — the point is the order, not the pace."""
        data_dir = os.path.join(self.root, f"node{index}")
        os.makedirs(data_dir, exist_ok=True)
        ports = [free_port() for _ in range(3)]
        self.ports.append(ports)
        log = open(os.path.join(self.root, f"node{index}.log"), "wb")
        self._logs.append(log)
        self.procs.append(subprocess.Popen(
            [SERVER, "--port", str(ports[0]), "--data-dir", data_dir,
             "--metrics-port", str(ports[1]), "--replication-port", str(ports[2]),
             "--coordinator-endpoints", etcd_url, "--node-id", f"node-{index}",
             "--coordinator-lease-ttl", str(LEASE_TTL)],
            stdout=log, stderr=subprocess.STDOUT))

    def wait_until_listening(self, index: int, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.ports[index][0]), timeout=2) as s:
                    s.settimeout(2)
                    s.recv(4096)
                    s.sendall(b"PING\n")
                    if b"PONG" in s.recv(1024):
                        return
            except OSError:
                time.sleep(0.3)
        raise RuntimeError(f"node-{index} never accepted connections")

    def roles(self) -> list[str]:
        return [role_of(p[0]) for p in self.ports]

    def wait_for_role(self, index: int, wanted: str, timeout: float) -> float | None:
        start = time.time()
        while time.time() - start < timeout:
            if wanted in role_of(self.ports[index][0]):
                return time.time() - start
            time.sleep(0.5)
        return None

    def node_log(self, index: int) -> str:
        path = os.path.join(self.root, f"node{index}.log")
        with open(path, errors="replace") as fh:
            return fh.read()

    def shutdown(self) -> None:
        for proc in self.procs + ([self.etcd] if self.etcd else []):
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
        for log in self._logs:
            try:
                log.close()
            except OSError:
                pass
        shutil.rmtree(self.root, ignore_errors=True)


@pytest.fixture
def simultaneous_cluster():
    cluster = Cluster("simul")
    try:
        yield cluster
    finally:
        cluster.shutdown()


def test_no_node_is_left_without_a_role_after_a_simultaneous_start(simultaneous_cluster):
    """Both nodes launched at once: one leads, the other follows. Neither idles."""
    cluster = simultaneous_cluster
    url = cluster.start_etcd()
    cluster.spawn(0, url)
    cluster.spawn(1, url)
    cluster.wait_until_listening(0)
    cluster.wait_until_listening(1)

    # Both nodes read "no leader" and both ran the CAS; one of them lost it.
    deadline = time.time() + 30
    roles: list[str] = []
    while time.time() < deadline:
        roles = cluster.roles()
        if sum("PRIMARY" in r for r in roles) == 1 and sum("REPLICA" in r for r in roles) == 1:
            break
        time.sleep(0.5)

    assert sum("PRIMARY" in r for r in roles) == 1, f"expected exactly one primary, got {roles}"
    assert not any("STANDALONE" in r for r in roles), (
        f"a node holds no cluster role and will never campaign again: {roles}")


def test_the_survivor_of_a_simultaneous_start_can_still_take_over(simultaneous_cluster):
    """The consequence that matters: kill the primary and someone must promote.

    Before #73 this failed outright — the loser of the startup race was inert, so the cluster stayed
    without a primary until an operator restarted it.
    """
    cluster = simultaneous_cluster
    url = cluster.start_etcd()
    cluster.spawn(0, url)
    cluster.spawn(1, url)
    cluster.wait_until_listening(0)
    cluster.wait_until_listening(1)
    assert cluster.wait_for_role(0, "PRIMARY", 30) is not None or \
           cluster.wait_for_role(1, "PRIMARY", 30) is not None, "no primary was elected at all"

    roles = cluster.roles()
    primary = 0 if "PRIMARY" in roles[0] else 1
    survivor = 1 - primary

    cluster.procs[primary].send_signal(signal.SIGKILL)
    cluster.procs[primary].wait(timeout=10)

    took = cluster.wait_for_role(survivor, "PRIMARY", PROMOTION_BUDGET)
    assert took is not None, (
        f"node-{survivor} never promoted within {PROMOTION_BUDGET}s; roles now {cluster.roles()}")


def test_a_node_that_boots_before_its_coordinator_still_joins_the_cluster(simultaneous_cluster):
    """Deterministic version of the same dead end, with no race to lose.

    The node starts while nothing answers on the coordinator's port. It must report STANDALONE and
    then join once etcd appears — `start()` used to return before creating the monitor thread, so
    the node stayed outside its own cluster for the rest of its life.
    """
    cluster = simultaneous_cluster
    url = cluster.reserve_etcd_port()          # port claimed, nothing listening on it yet
    cluster.spawn(0, url)
    cluster.wait_until_listening(0)

    assert "STANDALONE" in role_of(cluster.ports[0][0]), (
        "a node with an unreachable coordinator should hold no cluster role")

    cluster.start_etcd(wait=0.0)
    took = cluster.wait_for_role(0, "PRIMARY", PROMOTION_BUDGET)
    assert took is not None, (
        f"the node never joined after etcd came up; role is {role_of(cluster.ports[0][0])}")

    log = cluster.node_log(0)
    assert "cannot reach the coordinator at startup" in log, (
        "the retry path should say why it is standing by")


def test_a_node_stopped_on_purpose_stops_being_a_deference_target_at_once(simultaneous_cluster):
    """A clean shutdown should not leave an election waiting on the lease TTL.

    Position keys expire on their own since #72, which covers a node that died. A node that was
    stopped deliberately can do better than expire: it revokes the lease on the way out, so the
    remaining node sees a shorter list immediately rather than in ten seconds.
    """
    cluster = simultaneous_cluster
    url = cluster.start_etcd()
    cluster.spawn(0, url)
    cluster.spawn(1, url)
    cluster.wait_until_listening(0)
    cluster.wait_until_listening(1)

    deadline = time.time() + 20
    while time.time() < deadline:
        if {"node-0", "node-1"} <= set(etcd_positions(cluster.etcd_port)):
            break
        time.sleep(0.5)
    assert {"node-0", "node-1"} <= set(etcd_positions(cluster.etcd_port)), (
        f"both nodes should publish a position; etcd has {etcd_positions(cluster.etcd_port)}")

    # SIGTERM, not SIGKILL: the difference between the two is the whole point here.
    cluster.procs[1].terminate()
    cluster.procs[1].wait(timeout=15)

    # Well inside the lease TTL, so expiry cannot be what removed it.
    gone_within = None
    start = time.time()
    while time.time() - start < 4:
        if "node-1" not in etcd_positions(cluster.etcd_port):
            gone_within = time.time() - start
            break
        time.sleep(0.2)

    assert gone_within is not None, (
        "a node stopped with SIGTERM left its position behind, so the survivor would wait out the "
        "lease TTL before it could stop deferring")
    assert "position lease" in cluster.node_log(1), (
        "the shutdown path should say what it did with the position lease")
