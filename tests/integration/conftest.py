"""
Integration test infrastructure for orderbook-dbengine.

Provides:
- NodeInfo dataclass — metadata about a running server node
- ClusterManager — full lifecycle management of etcd + 2 ob_tcp_server nodes
- Environment gate (OB_INTEGRATION_TESTS)
- Category filtering (OB_INTEGRATION_FILTER)
- Marker registration for all test categories
"""

from __future__ import annotations

import atexit
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, Optional

import pytest
from orderbook_engine import OrderbookEngine


# ---------------------------------------------------------------------------
# NodeInfo
# ---------------------------------------------------------------------------

@dataclass
class NodeInfo:
    """Metadata about a running ob_tcp_server node."""

    index: int                              # 0 or 1
    process: Optional[subprocess.Popen]     # subprocess handle
    tcp_port: int                           # TCP server port
    replication_port: int                   # WAL replication port
    metrics_port: int                       # Prometheus metrics port
    data_dir: str                           # temporary data directory
    node_id: str                            # e.g. "node-0"
    read_only: bool = False                 # started with --read-only


# ---------------------------------------------------------------------------
# ClusterManager
# ---------------------------------------------------------------------------

class ClusterManager:
    """Manage the full lifecycle of an integration-test cluster:
    etcd Docker container + 2 ob_tcp_server nodes."""

    _PROJECT_ROOT = Path(__file__).resolve().parents[2]  # …/low-cost-and-low-latency-orderbook-dbengine
    _SERVER_BINARY = "build/ob_tcp_server"
    _ETCD_IMAGE = "quay.io/coreos/etcd:v3.5.9"

    def __init__(self, server_binary: Optional[str] = None):
        self.server_binary: str = server_binary or str(
            self._PROJECT_ROOT / self._SERVER_BINARY
        )
        self.etcd_client_port: int = 0
        self.etcd_peer_port: int = 0
        self.etcd_container_id: str = ""
        self.nodes: list[NodeInfo] = []
        self.temp_dirs: list[str] = []
        self._started = False

    # ── Public lifecycle ──────────────────────────────────────────

    def start(self) -> None:
        """Launch etcd + 2 server nodes. Raises RuntimeError on failure.

        Nodes are started sequentially: node-0 starts first and becomes PRIMARY,
        then node-1 starts and discovers the leader via etcd, becoming REPLICA.
        This avoids a race condition where both nodes start simultaneously and
        one fails to transition from STANDALONE.
        """
        self._check_prerequisites()
        self._start_etcd()
        self._wait_for_etcd(timeout=30)

        # Start node-0 first and wait for it to become PRIMARY
        node0 = self._start_node(0)
        self.nodes.append(node0)
        self._wait_for_node(node0, timeout=15)
        self._wait_for_primary(node0, timeout=15)

        # Now start node-1 — it will discover node-0 as leader via etcd
        node1 = self._start_node(1)
        self.nodes.append(node1)
        self._wait_for_node(node1, timeout=15)

        self._wait_for_election(timeout=15)
        self._started = True

        # Safety net — clean up even on unhandled exit
        atexit.register(self.shutdown)

    def shutdown(self) -> None:
        """Stop nodes, remove etcd container, clean temp dirs.
        Each step is wrapped in try/except so one failure doesn't block the rest."""
        if not self._started:
            return
        self._started = False

        for node in self.nodes:
            try:
                self._stop_node(node)
            except Exception:
                pass

        try:
            self._stop_etcd()
        except Exception:
            pass

        for d in self.temp_dirs:
            try:
                shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass

        self.nodes.clear()
        self.temp_dirs.clear()

    # ── etcd management ───────────────────────────────────────────

    def _start_etcd(self) -> None:
        self.etcd_client_port = self.find_free_port()
        self.etcd_peer_port = self.find_free_port()
        self._etcd_container_name = f"ob-etcd-test-{self.etcd_client_port}"

        # Remove any stale container with the same name (from a previous crashed run)
        subprocess.run(
            ["docker", "rm", "-f", self._etcd_container_name],
            capture_output=True, timeout=10,
        )

        cmd = [
            "docker", "run", "-d",
            "--name", self._etcd_container_name,
            "--net=host",
            self._ETCD_IMAGE,
            "etcd",
            "--advertise-client-urls", f"http://127.0.0.1:{self.etcd_client_port}",
            "--listen-client-urls", f"http://0.0.0.0:{self.etcd_client_port}",
            "--listen-peer-urls", f"http://0.0.0.0:{self.etcd_peer_port}",
            "--initial-advertise-peer-urls", f"http://127.0.0.1:{self.etcd_peer_port}",
            "--initial-cluster", f"default=http://127.0.0.1:{self.etcd_peer_port}",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to start etcd container: {result.stderr.strip()}"
            )
        self.etcd_container_id = result.stdout.strip()

    def _wait_for_etcd(self, timeout: float = 30.0) -> None:
        url = f"http://127.0.0.1:{self.etcd_client_port}/v3/maintenance/status"
        deadline = time.monotonic() + timeout
        last_err: Optional[Exception] = None

        while time.monotonic() < deadline:
            try:
                req = urllib.request.Request(url, data=b"{}", method="POST")
                req.add_header("Content-Type", "application/json")
                resp = urllib.request.urlopen(req, timeout=2)
                if resp.status == 200:
                    return
            except Exception as exc:
                last_err = exc
            time.sleep(0.5)

        raise RuntimeError(
            f"etcd not ready after {timeout}s: {last_err}"
        )

    def _stop_etcd(self) -> None:
        # Remove by container ID
        if self.etcd_container_id:
            subprocess.run(
                ["docker", "rm", "-f", self.etcd_container_id],
                capture_output=True, timeout=15,
            )
            self.etcd_container_id = ""
        # Also remove by name as safety net
        if hasattr(self, '_etcd_container_name') and self._etcd_container_name:
            subprocess.run(
                ["docker", "rm", "-f", self._etcd_container_name],
                capture_output=True, timeout=15,
            )

    # ── Node management ───────────────────────────────────────────

    def _start_node(self, node_index: int, read_only: bool = False) -> NodeInfo:
        tcp_port = self.find_free_port()
        replication_port = self.find_free_port()
        metrics_port = self.find_free_port()
        data_dir = tempfile.mkdtemp(prefix=f"ob_node{node_index}_")
        self.temp_dirs.append(data_dir)
        node_id = f"node-{node_index}"

        etcd_url = f"http://127.0.0.1:{self.etcd_client_port}"

        cmd = [
            self.server_binary,
            "--port", str(tcp_port),
            "--data-dir", data_dir,
            "--metrics-port", str(metrics_port),
            "--replication-port", str(replication_port),
            "--coordinator-endpoints", etcd_url,
            "--node-id", node_id,
        ]
        if read_only:
            cmd.append("--read-only")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        return NodeInfo(
            index=node_index,
            process=proc,
            tcp_port=tcp_port,
            replication_port=replication_port,
            metrics_port=metrics_port,
            data_dir=data_dir,
            node_id=node_id,
            read_only=read_only,
        )

    def _wait_for_node(self, node: NodeInfo, timeout: float = 15.0) -> None:
        """Poll TCP connect + PING until the node responds with PONG."""
        deadline = time.monotonic() + timeout
        last_err: Optional[Exception] = None

        while time.monotonic() < deadline:
            try:
                with socket.create_connection(
                    ("127.0.0.1", node.tcp_port), timeout=2
                ) as sock:
                    # Read and discard the welcome banner first
                    banner = sock.recv(4096)
                    sock.sendall(b"PING\n")
                    data = sock.recv(1024)
                    if b"PONG" in data:
                        return
            except Exception as exc:
                last_err = exc
            time.sleep(0.5)

        # Grab stderr for diagnostics
        stderr_text = ""
        if node.process and node.process.poll() is not None:
            stderr_text = (node.process.stderr.read() or b"").decode(errors="replace")

        raise RuntimeError(
            f"Node {node.node_id} (port {node.tcp_port}) not ready after "
            f"{timeout}s: {last_err}\nstderr: {stderr_text}"
        )

    def _stop_node(self, node: NodeInfo) -> None:
        """SIGTERM → wait 5s → SIGKILL."""
        proc = node.process
        if proc is None or proc.poll() is not None:
            return

        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

    def _wait_for_primary(self, node: NodeInfo, timeout: float = 15.0) -> None:
        """Poll ROLE on a single node until it reports PRIMARY."""
        deadline = time.monotonic() + timeout
        last_role = ""
        while time.monotonic() < deadline:
            try:
                last_role = self._query_role(node).strip()
                if "PRIMARY" in last_role.upper():
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError(
            f"Node {node.node_id} did not become PRIMARY after {timeout}s. "
            f"Last ROLE: {last_role!r}"
        )

    def _wait_for_election(self, timeout: float = 15.0) -> None:
        """Poll ROLE on both nodes until one is PRIMARY and the other is REPLICA."""
        deadline = time.monotonic() + timeout
        roles = {}

        while time.monotonic() < deadline:
            roles = {}
            for node in self.nodes:
                try:
                    roles[node.node_id] = self._query_role(node).strip().upper()
                except Exception:
                    roles[node.node_id] = "UNKNOWN"

            has_primary = any("PRIMARY" in r for r in roles.values())
            has_replica = any("REPLICA" in r for r in roles.values())
            if has_primary and has_replica:
                return
            if has_primary and len(self.nodes) == 1:
                return
            time.sleep(0.5)

        raise RuntimeError(
            f"Cluster not converged after {timeout}s. Roles: {roles}"
        )

    # ── Restart / kill (for failover tests) ───────────────────────

    def restart_node(self, node_index: int) -> None:
        """Restart a node keeping the same ports and data-dir."""
        old = self.nodes[node_index]
        self._stop_node(old)

        new = NodeInfo(
            index=old.index,
            process=None,
            tcp_port=old.tcp_port,
            replication_port=old.replication_port,
            metrics_port=old.metrics_port,
            data_dir=old.data_dir,
            node_id=old.node_id,
            read_only=old.read_only,
        )

        etcd_url = f"http://127.0.0.1:{self.etcd_client_port}"
        cmd = [
            self.server_binary,
            "--port", str(new.tcp_port),
            "--data-dir", new.data_dir,
            "--metrics-port", str(new.metrics_port),
            "--replication-port", str(new.replication_port),
            "--coordinator-endpoints", etcd_url,
            "--node-id", new.node_id,
        ]
        if new.read_only:
            cmd.append("--read-only")

        new.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.nodes[node_index] = new
        self._wait_for_node(new, timeout=15)

    def kill_node(self, node_index: int) -> None:
        """SIGKILL a node (simulate crash)."""
        node = self.nodes[node_index]
        if node.process and node.process.poll() is None:
            node.process.kill()
            node.process.wait(timeout=5)

    # ── Helpers ───────────────────────────────────────────────────

    def primary(self) -> NodeInfo:
        """Return the node currently holding the PRIMARY role."""
        for node in self.nodes:
            try:
                role = self._query_role(node)
                if "PRIMARY" in role.upper():
                    return node
            except Exception:
                continue
        raise RuntimeError("No node with PRIMARY role found")

    def replica(self) -> NodeInfo:
        """Return the node currently holding the REPLICA role."""
        for node in self.nodes:
            try:
                role = self._query_role(node)
                if "REPLICA" in role.upper():
                    return node
            except Exception:
                continue
        raise RuntimeError("No node with REPLICA role found")

    @staticmethod
    def find_free_port() -> int:
        """Bind to port 0 and return the OS-assigned port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    # ── Internal ──────────────────────────────────────────────────

    def _query_role(self, node: NodeInfo) -> str:
        """Send ROLE command via TCP and return the raw response."""
        with socket.create_connection(
            ("127.0.0.1", node.tcp_port), timeout=2
        ) as sock:
            # Read and discard the server welcome banner ("OK ob_tcp_server v0.1.0\n")
            banner = sock.recv(4096)
            # Now send ROLE
            sock.sendall(b"ROLE\n")
            return sock.recv(4096).decode(errors="replace")

    def _check_prerequisites(self) -> None:
        """Verify Docker and server binary are available."""
        # Check server binary
        if not os.path.isfile(self.server_binary):
            raise RuntimeError(
                f"Server binary not found: {self.server_binary}\n"
                "Please compile the project first (cmake --build build)."
            )

        # Check Docker
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True, timeout=10,
        )
        if result.returncode != 0:
            raise RuntimeError(
                "Docker is not available. Please install and start Docker.\n"
                f"stderr: {result.stderr.decode(errors='replace')}"
            )


# ---------------------------------------------------------------------------
# pytest hooks — marker registration & report plugin
# ---------------------------------------------------------------------------

_CATEGORIES = [
    "smoke", "replication", "failover", "compression",
    "stress", "edge_cases", "metrics", "pool", "cpp_client",
]


# ---------------------------------------------------------------------------
# IntegrationReportPlugin — colored console report
# ---------------------------------------------------------------------------

class IntegrationReportPlugin:
    """Pytest plugin that generates a colored console report after all tests."""

    CATEGORIES = [
        "smoke", "replication", "failover", "compression",
        "stress", "edge_cases", "metrics", "pool", "cpp_client",
    ]

    # ANSI color codes
    _GREEN = "\033[32m"
    _RED = "\033[31m"
    _YELLOW = "\033[33m"
    _RESET = "\033[0m"

    def __init__(self) -> None:
        self.results: list[dict] = []
        self.start_time: float = time.monotonic()

    # ── hooks ─────────────────────────────────────────────────────

    def pytest_runtest_logreport(self, report) -> None:
        """Collect test results — only the 'call' phase."""
        if report.when != "call":
            return

        # Determine category from markers
        category = "uncategorized"
        for cat in self.CATEGORIES:
            if hasattr(report, "keywords") and cat in report.keywords:
                category = cat
                break

        if report.passed:
            outcome = "passed"
        elif report.failed:
            outcome = "failed"
        else:
            outcome = "skipped"

        self.results.append({
            "name": report.nodeid,
            "category": category,
            "outcome": outcome,
            "duration": getattr(report, "duration", 0.0),
            "message": str(report.longrepr) if report.failed else "",
        })

    def pytest_sessionfinish(self, session, exitstatus) -> None:
        """Print the full colored integration report."""
        total_time = time.monotonic() - self.start_time
        custom = self._collect_custom_metrics()

        G = self._GREEN
        R = self._RED
        Y = self._YELLOW
        RST = self._RESET

        print()
        print(f"{G}{'=' * 70}{RST}")
        print(f"{G}  INTEGRATION TEST REPORT{RST}")
        print(f"{G}{'=' * 70}{RST}")

        # ── Per-category sections ─────────────────────────────────
        for cat in self.CATEGORIES:
            cat_results = [r for r in self.results if r["category"] == cat]
            if not cat_results:
                continue

            print(f"\n  [{cat.upper()}]")
            for r in cat_results:
                if r["outcome"] == "passed":
                    icon = f"{G}✓{RST}"
                elif r["outcome"] == "failed":
                    icon = f"{R}✗{RST}"
                else:
                    icon = f"{Y}⚠{RST}"

                short_name = r["name"].split("::")[-1] if "::" in r["name"] else r["name"]
                dur = r["duration"]
                print(f"    {icon} {short_name}  ({dur:.2f}s)")
                if r["outcome"] == "failed" and r["message"]:
                    first_line = r["message"].split("\n")[0][:120]
                    print(f"      {R}{first_line}{RST}")

        # ── Summary ───────────────────────────────────────────────
        passed = sum(1 for r in self.results if r["outcome"] == "passed")
        failed = sum(1 for r in self.results if r["outcome"] == "failed")
        skipped = sum(1 for r in self.results if r["outcome"] == "skipped")

        print(f"\n{'─' * 70}")
        print(f"  Summary: {G}{passed} passed{RST}, {R}{failed} failed{RST}, {Y}{skipped} skipped{RST}")
        print(f"  Total time: {total_time:.2f}s")

        # ── Environment info ──────────────────────────────────────
        print(f"\n  Environment:")
        # Try to get cluster info from the session-scoped fixture
        cluster_fixture = session.config._ob_cluster if hasattr(session.config, "_ob_cluster") else None
        if cluster_fixture is None:
            # Fallback: try to find it via the fixture manager
            try:
                for item in session.items:
                    if "cluster" in item.funcargs:
                        cluster_fixture = item.funcargs["cluster"]
                        break
            except Exception:
                pass

        if cluster_fixture and hasattr(cluster_fixture, "server_binary"):
            print(f"    Server binary: {cluster_fixture.server_binary}")
            for node in cluster_fixture.nodes:
                print(f"    {node.node_id}: tcp={node.tcp_port} repl={node.replication_port} metrics={node.metrics_port}")
                print(f"      data-dir: {node.data_dir}")
            print(f"    etcd port: {cluster_fixture.etcd_client_port}")
        else:
            print(f"    (cluster info not available)")

        # ── Custom metrics ────────────────────────────────────────
        if custom:
            print(f"\n  Custom Metrics:")
            if "failover_time_sec" in custom:
                ft = custom["failover_time_sec"]
                print(f"    Failover time: {ft:.3f}s")
            if "stress_throughput" in custom:
                tp = custom["stress_throughput"]
                print(f"    Stress throughput: {tp:.0f} levels/s")
            if "stress_errors" in custom:
                errs = int(custom["stress_errors"])
                color = G if errs == 0 else R
                print(f"    Stress errors: {color}{errs}{RST}")

        print(f"\n{G}{'=' * 70}{RST}")
        print()

    # ── helpers ───────────────────────────────────────────────────

    @staticmethod
    def _collect_custom_metrics() -> dict:
        """Try to import custom_metrics from test_failover and test_stress modules."""
        merged: dict = {}
        try:
            from tests.integration.test_failover import custom_metrics as fm
            merged.update(fm)
        except Exception:
            try:
                from test_failover import custom_metrics as fm2
                merged.update(fm2)
            except Exception:
                pass
        try:
            from tests.integration.test_stress import custom_metrics as sm
            merged.update(sm)
        except Exception:
            try:
                from test_stress import custom_metrics as sm2
                merged.update(sm2)
            except Exception:
                pass
        return merged


def pytest_configure(config):
    """Register integration-test category markers and report plugin."""
    for cat in _CATEGORIES:
        config.addinivalue_line(
            "markers", f"{cat}: {cat} integration tests"
        )
    config.pluginmanager.register(IntegrationReportPlugin(), "integration_report")


def pytest_collection_modifyitems(config, items):
    """Environment gate and category filtering for integration tests."""
    # 1. Environment gate
    if not os.environ.get("OB_INTEGRATION_TESTS"):
        skip = pytest.mark.skip(reason="OB_INTEGRATION_TESTS not set")
        for item in items:
            item.add_marker(skip)
        return

    # 2. Category filtering
    filter_str = os.environ.get("OB_INTEGRATION_FILTER", "")
    if not filter_str:
        return
    allowed = {c.strip() for c in filter_str.split(",")}
    deselected = []
    selected = []
    for item in items:
        markers = {m.name for m in item.iter_markers()}
        if markers & allowed:
            selected.append(item)
        else:
            deselected.append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)


# ---------------------------------------------------------------------------
# pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def cluster() -> Generator[ClusterManager, None, None]:
    """Start the integration-test cluster (etcd + 2 nodes), yield, shutdown."""
    cm = ClusterManager()
    cm.start()
    yield cm
    cm.shutdown()


@pytest.fixture
def primary_client(cluster: ClusterManager) -> Generator[OrderbookEngine, None, None]:
    """Fresh TCP connection to the current PRIMARY node."""
    node = cluster.primary()
    engine = OrderbookEngine(host="127.0.0.1", port=node.tcp_port)
    yield engine
    engine.close()


@pytest.fixture
def replica_client(cluster: ClusterManager) -> Generator[OrderbookEngine, None, None]:
    """Fresh TCP connection to the current REPLICA node."""
    node = cluster.replica()
    engine = OrderbookEngine(host="127.0.0.1", port=node.tcp_port)
    yield engine
    engine.close()


@pytest.fixture
def compressed_client(cluster: ClusterManager) -> Generator[OrderbookEngine, None, None]:
    """TCP connection with LZ4 compression to the PRIMARY node."""
    node = cluster.primary()
    engine = OrderbookEngine(host="127.0.0.1", port=node.tcp_port, compress=True)
    yield engine
    engine.close()


@pytest.fixture
def pool_client(cluster: ClusterManager) -> Generator[OrderbookEngine, None, None]:
    """Pool-mode client with addresses of all cluster nodes."""
    hosts = [f"127.0.0.1:{n.tcp_port}" for n in cluster.nodes]
    engine = OrderbookEngine(hosts=hosts, timeout=10.0, health_check_interval=2.0)
    yield engine
    engine.close()
