"""STATUS and /metrics on a multi-master node, which nothing here used to combine.

The multi-master modules exercise INSERT, SELECT, ROLE and MM_PEERS; the metrics module runs
STATUS and /metrics but against the plain cluster fixture, with multi-master off. Each path was
covered on its own, and a null dereference lived at their intersection: `Engine::stats()` read
`mm_mgr_->anti_entropy()`, which handed out a reference to a manager nothing had constructed. A
single STATUS command killed the node with SIGSEGV, and 640 unit plus 117 integration tests
missed it by one crossing.

This module runs its own node with its own etcd, because it needs a one-second anti-entropy
interval to see the scheduler run, and because the point of the first test is that the node is
still alive afterwards — not something to find out on a shared fixture.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

import pytest

pytestmark = pytest.mark.multi_master

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SERVER = os.path.join(REPO, "build", "ob_tcp_server")
ETCD = os.environ.get("OB_ETCD_BINARY") or shutil.which("etcd") or "/usr/local/bin/etcd"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class MmNode:
    """One multi-master node with its own etcd, and a one-second anti-entropy interval."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.tcp = self.metrics = 0
        self.proc: subprocess.Popen | None = None
        self.etcd: subprocess.Popen | None = None

    def start(self, timeout: float = 30.0) -> None:
        client_port, peer_port = free_port(), free_port()
        etcd_url = f"http://127.0.0.1:{client_port}"
        self.etcd = subprocess.Popen(
            [ETCD, "--name", "mmstats", "--data-dir", os.path.join(self.data_dir, "etcd"),
             "--advertise-client-urls", etcd_url, "--listen-client-urls", etcd_url,
             "--listen-peer-urls", f"http://127.0.0.1:{peer_port}",
             "--initial-advertise-peer-urls", f"http://127.0.0.1:{peer_port}",
             "--initial-cluster", f"mmstats=http://127.0.0.1:{peer_port}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)

        self.tcp, self.metrics = free_port(), free_port()
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.tcp), "--data-dir", self.data_dir,
             "--metrics-port", str(self.metrics),
             "--replication-port", str(free_port()),
             "--coordinator-endpoints", etcd_url, "--node-id", "mm-stats-0",
             "--multi-master", "--mm-node-id", "1",
             "--mm-replication-port", str(free_port()),
             "--anti-entropy-interval-seconds", "1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.tcp), timeout=2) as s:
                    s.settimeout(2)
                    s.recv(4096)
                    s.sendall(b"PING\n")
                    if b"PONG" in s.recv(1024):
                        return
            except OSError:
                time.sleep(0.3)
        raise RuntimeError("multi-master node did not come up")

    def stop(self) -> None:
        for proc in (self.proc, self.etcd):
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def exit_code(self):
        return None if self.proc is None else self.proc.poll()

    def command(self, text: str, settle: float = 0.6) -> str:
        with socket.create_connection(("127.0.0.1", self.tcp), timeout=15) as s:
            s.settimeout(15)
            s.recv(4096)  # banner
            s.sendall(text.encode())
            time.sleep(settle)
            try:
                return s.recv(1 << 20).decode(errors="replace")
            except socket.timeout:
                return ""

    def scrape(self) -> str:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.metrics}/metrics", timeout=10) as r:
            return r.read().decode(errors="replace")


@pytest.fixture
def mm_node():
    if not os.path.isfile(SERVER):
        pytest.skip(f"server binary not built: {SERVER}")
    if not (os.path.isfile(ETCD) or shutil.which(ETCD)):
        pytest.skip(f"etcd not available: {ETCD}")
    data_dir = tempfile.mkdtemp(prefix="ob_mm_stats_")
    node = MmNode(data_dir)
    yield node
    node.stop()


def status_field(reply: str, name: str) -> str | None:
    for line in reply.splitlines():
        if line.strip().startswith(f"{name}:"):
            return line.split(":", 1)[1].strip()
    return None


def test_status_on_a_multi_master_node_does_not_kill_it(mm_node):
    mm_node.start()

    reply = mm_node.command("STATUS\n")
    assert reply, "STATUS returned nothing, which is what a dead session looks like"

    time.sleep(0.5)
    assert mm_node.alive(), (
        f"the node died answering STATUS (exit={mm_node.exit_code()}); before the fix this was "
        f"SIGSEGV, because Engine::stats() dereferenced an anti-entropy manager that nothing "
        f"had constructed")
    assert status_field(reply, "anti_entropy_runs") is not None, (
        "STATUS no longer reports anti_entropy_runs")


def test_the_anti_entropy_scheduler_runs_on_a_node_with_peers(mm_node):
    mm_node.start()

    # The roadmap described the scheduler as working and only reconciliation as missing. It was
    # never constructed, so this counter sat at zero — which reads as "checked, nothing to
    # repair" rather than "never ran".
    runs = 0
    deadline = time.time() + 20
    while time.time() < deadline:
        value = status_field(mm_node.command("STATUS\n", settle=0.4), "anti_entropy_runs")
        runs = int(value) if value and value.isdigit() else 0
        if runs > 0:
            break
        time.sleep(1.0)

    assert runs > 0, (
        "no anti-entropy run was recorded in twenty seconds with a one-second interval, so the "
        "scheduler is not running")
    assert mm_node.alive()


def test_metrics_scrape_on_a_multi_master_node_does_not_kill_it(mm_node):
    mm_node.start()

    body = mm_node.scrape()
    assert "ob_mm_anti_entropy_runs_total" in body, (
        "the anti-entropy counters are missing from /metrics")

    time.sleep(0.5)
    assert mm_node.alive(), (
        f"the node died answering a metrics scrape (exit={mm_node.exit_code()}) — the same "
        f"stats() path STATUS uses, and the one a monitoring system hits every few seconds")
