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
import sys
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
    mm_replication_port: int = 0            # multi-master peer port (0 = not MM)


# ---------------------------------------------------------------------------
# ClusterManager
# ---------------------------------------------------------------------------

class ClusterManager:
    """Manage the full lifecycle of an integration-test cluster:
    a native etcd process + 2 ob_tcp_server nodes.

    Everything runs directly on the host. The engine has no containerised
    deployment path, so the test harness does not depend on one either.
    """

    _PROJECT_ROOT = Path(__file__).resolve().parents[2]  # …/low-cost-and-low-latency-orderbook-dbengine
    _SERVER_BINARY = "build/ob_tcp_server"

    def __init__(self, server_binary: Optional[str] = None,
                 etcd_binary: Optional[str] = None):
        self.server_binary: str = server_binary or str(
            self._PROJECT_ROOT / self._SERVER_BINARY
        )
        self.etcd_binary: str = (
            etcd_binary or os.environ.get("OB_ETCD_BINARY") or "etcd"
        )
        self.etcd_client_port: int = 0
        self.etcd_peer_port: int = 0
        self.etcd_data_dir: str = ""
        self.etcd_process: Optional[subprocess.Popen] = None
        self._etcd_log = None
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
        """Start etcd as a native process.

        The engine is a native-deployment database and the test harness follows the
        same rule: no containers anywhere in the loop. etcd must be on PATH, or its
        location given via the OB_ETCD_BINARY environment variable.
        """
        self.etcd_client_port = self.find_free_port()
        self.etcd_peer_port = self.find_free_port()
        self.etcd_data_dir = tempfile.mkdtemp(prefix="ob_etcd_")
        self.temp_dirs.append(self.etcd_data_dir)

        etcd_log_path = os.path.join(self.etcd_data_dir, "etcd.log")
        self._etcd_log = open(etcd_log_path, "wb")

        cmd = [
            self.etcd_binary,
            "--name", "ob-test-etcd",
            "--data-dir", os.path.join(self.etcd_data_dir, "data"),
            "--advertise-client-urls", f"http://127.0.0.1:{self.etcd_client_port}",
            "--listen-client-urls", f"http://127.0.0.1:{self.etcd_client_port}",
            "--listen-peer-urls", f"http://127.0.0.1:{self.etcd_peer_port}",
            "--initial-advertise-peer-urls", f"http://127.0.0.1:{self.etcd_peer_port}",
            "--initial-cluster", f"ob-test-etcd=http://127.0.0.1:{self.etcd_peer_port}",
            "--initial-cluster-state", "new",
            # Keep the store small: these are short-lived test clusters.
            "--quota-backend-bytes", str(256 * 1024 * 1024),
        ]

        try:
            self.etcd_process = subprocess.Popen(
                cmd, stdout=self._etcd_log, stderr=subprocess.STDOUT,
            )
        except OSError as exc:
            raise RuntimeError(
                f"Failed to launch etcd binary '{self.etcd_binary}': {exc}"
            ) from exc

        # Fail fast if etcd died on startup (bad flags, port taken, corrupt data dir).
        time.sleep(0.2)
        if self.etcd_process.poll() is not None:
            log_tail = ""
            try:
                with open(etcd_log_path, "r", errors="replace") as fh:
                    log_tail = "".join(fh.readlines()[-15:])
            except OSError:
                pass
            raise RuntimeError(
                f"etcd exited immediately with code {self.etcd_process.returncode}.\n"
                f"Log tail:\n{log_tail}"
            )

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
        proc = self.etcd_process
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        self.etcd_process = None

        if self._etcd_log is not None:
            try:
                self._etcd_log.close()
            except OSError:
                pass
            self._etcd_log = None

    # ── Node management ───────────────────────────────────────────

    def start_multi_master(self, node_count: int = 3) -> None:
        """Launch etcd plus `node_count` multi-master nodes.

        A different topology, not a variation on the primary/replica one: every node
        accepts writes, there is no election, and peers find each other through the
        etcd topology watch rather than through a leader key. Sharing this with
        start() would mean a method that means two things depending on a flag.

        Nodes are started one at a time and each is only waited for on PING; peer
        discovery is asynchronous, so a caller that needs a converged mesh should
        wait for MM_PEERS to report the others.
        """
        self._check_prerequisites()
        self._start_etcd()
        self._wait_for_etcd(timeout=30)

        for index in range(node_count):
            node = self._start_node(index, multi_master_id=index + 1)
            self.nodes.append(node)
            self._wait_for_node(node, timeout=20)

        self._started = True
        atexit.register(self.shutdown)

    def wait_for_mm_mesh(self, timeout: float = 30.0) -> None:
        """Wait until every node sees all the others in MM_PEERS.

        Without this, a test writing to node 0 and reading from node 2 can run before
        the two have connected, and the failure looks like lost data rather than a
        test that started too early.
        """
        expected_peers = len(self.nodes) - 1
        deadline = time.monotonic() + timeout
        counts: list[int] = []
        errors: dict[str, str] = {}

        while time.monotonic() < deadline:
            counts = []
            errors = {}
            for node in self.nodes:
                try:
                    reply = self._send(node, "MM_PEERS")
                    # One header line, then one line per peer.
                    rows = [ln for ln in reply.strip().splitlines()[1:]
                            if ln and not ln.startswith("OK")]
                    counts.append(len(rows))
                except Exception as exc:  # noqa: BLE001
                    # Recorded, not swallowed: "saw [2, -1, 2]" says a node did not
                    # answer and nothing about why, which is the difference between a
                    # diagnosis and a shrug.
                    counts.append(-1)
                    alive = (node.process is None or node.process.poll() is None)
                    errors[node.node_id] = f"{exc!r} (process alive: {alive})"
            if all(c >= expected_peers for c in counts):
                return
            time.sleep(0.5)

        detail = "; ".join(f"{k}: {v}" for k, v in errors.items()) or "no errors"
        raise RuntimeError(
            f"multi-master mesh did not converge in {timeout}s: each node should see "
            f"{expected_peers} peers, saw {counts}. {detail}")

    def _send(self, node: NodeInfo, command: str, timeout: float = 5.0) -> str:
        """Send one command to a node over a fresh connection."""
        with socket.create_connection(("127.0.0.1", node.tcp_port),
                                      timeout=timeout) as sock:
            sock.settimeout(timeout)
            sock.recv(4096)  # banner
            sock.sendall((command + "\n").encode())
            time.sleep(0.3)
            try:
                return sock.recv(1 << 20).decode(errors="replace")
            except socket.timeout:
                return ""

    def _start_node(self, node_index: int, read_only: bool = False,
                    multi_master_id: Optional[int] = None) -> NodeInfo:
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
        mm_replication_port = 0
        if multi_master_id is not None:
            # The server refuses --mm-replication-port equal to --replication-port:
            # they are two different listeners, and colliding them would have one
            # silently shadow the other.
            mm_replication_port = self.find_free_port()
            cmd += [
                "--multi-master",
                "--mm-node-id", str(multi_master_id),
                "--mm-replication-port", str(mm_replication_port),
            ]

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
            mm_replication_port=mm_replication_port,
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
            mm_replication_port=old.mm_replication_port,
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
        if old.mm_replication_port:
            # Without this, restarting a multi-master node brings it back as an
            # ordinary primary/replica node: same ports, same data dir, silently a
            # different topology. The test that noticed would look like a
            # convergence bug.
            cmd += [
                "--multi-master",
                "--mm-node-id", str(old.index + 1),
                "--mm-replication-port", str(old.mm_replication_port),
            ]

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
        """Verify the server binary and a native etcd binary are available."""
        if not os.path.isfile(self.server_binary):
            raise RuntimeError(
                f"Server binary not found: {self.server_binary}\n"
                "Please compile the project first (cmake --build build)."
            )

        resolved = shutil.which(self.etcd_binary) if os.path.sep not in self.etcd_binary \
            else (self.etcd_binary if os.access(self.etcd_binary, os.X_OK) else None)
        if resolved is None:
            raise RuntimeError(
                f"etcd binary not found: '{self.etcd_binary}'.\n"
                "Install etcd natively (no container needed):\n"
                "  ETCD_VER=v3.5.17\n"
                "  curl -L https://github.com/etcd-io/etcd/releases/download/"
                "$ETCD_VER/etcd-$ETCD_VER-linux-amd64.tar.gz | tar xz\n"
                "  sudo install -m755 etcd-$ETCD_VER-linux-amd64/etcd"
                " etcd-$ETCD_VER-linux-amd64/etcdctl /usr/local/bin/\n"
                "Or point OB_ETCD_BINARY at an existing binary."
            )
        self.etcd_binary = resolved


# ---------------------------------------------------------------------------
# pytest hooks — marker registration & report plugin
# ---------------------------------------------------------------------------

_CATEGORIES = [
    "smoke", "replication", "failover", "compression",
    "stress", "edge_cases", "metrics", "pool", "cpp_client",
    # Added with the categories themselves. A marker missing from this list is not
    # an error: those tests fall into "uncategorized" in the report and quietly stop
    # being counted as coverage of anything.
    "aggregations", "multi_master", "large_response", "binance",
]


# ---------------------------------------------------------------------------
# IntegrationReportPlugin — colored console report
# ---------------------------------------------------------------------------

class IntegrationReportPlugin:
    """Pytest plugin that generates a colored console report after all tests."""

    CATEGORIES = _CATEGORIES

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

            # Anything a test chose to publish beyond the three keys above. Without
            # this, a new metric is collected and then dropped, which looks exactly
            # like a metric that stayed at zero.
            known = {"failover_time_sec", "stress_throughput", "stress_errors"}
            for key in sorted(k for k in custom if k not in known):
                print(f"    {key}: {custom[key]}")

        print(f"\n{G}{'=' * 70}{RST}")
        print()

    # ── helpers ───────────────────────────────────────────────────

    @staticmethod
    def _collect_custom_metrics() -> dict:
        """Read custom_metrics from whichever test modules were loaded.

        Reads sys.modules rather than importing. The previous version tried four
        import paths wrapped in bare `except Exception: pass`, and every one of them
        failed for reasons nobody could see — so a stress run published its
        throughput and the report printed nothing at all. Whatever pytest called the
        module, it is already in sys.modules; importing it again cannot be more
        reliable than looking it up.
        """
        merged: dict = {}
        for name, module in list(sys.modules.items()):
            if module is None:
                continue
            base = name.rsplit(".", 1)[-1]
            if not base.startswith("test_"):
                continue
            metrics = getattr(module, "custom_metrics", None)
            if isinstance(metrics, dict):
                merged.update(metrics)
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
def cluster(request) -> Generator[ClusterManager, None, None]:
    """Start the integration-test cluster (etcd + 2 nodes), yield, shutdown."""
    cm = ClusterManager()
    cm.start()
    # The report reads this at session finish. It used to look for
    # session.config._ob_cluster, which nothing ever set, and then fall back to
    # item.funcargs — already cleared by teardown — so the environment section always
    # printed "(cluster info not available)".
    request.config._ob_cluster = cm
    yield cm
    cm.shutdown()


@pytest.fixture(scope="module")
def heavy_cluster(request) -> Generator[ClusterManager, None, None]:
    """A cluster of its own, for modules that push hundreds of thousands of rows.

    The session-scoped `cluster` is shared by every module, and load tests poison it
    for whatever runs next: half a million rows leave the replica replaying a backlog,
    and later tests fail on ROLE and replication-position timeouts that have nothing
    to do with the code they are testing. Diagnosing that costs far more than the two
    seconds it takes to start a second cluster.

    Module-scoped rather than per-test: the point is to contain the load, not to pay
    for a cluster per assertion.
    """
    cm = ClusterManager()
    cm.start()
    # Only if the session cluster has not registered itself: when both exist, the
    # shared one describes the run better. Registering nothing at all is how the
    # report ends up saying "(cluster info not available)" for a run that had a
    # perfectly good cluster.
    if getattr(request.config, "_ob_cluster", None) is None:
        request.config._ob_cluster = cm
    yield cm
    cm.shutdown()


@pytest.fixture
def heavy_client(heavy_cluster: ClusterManager) -> Generator[OrderbookEngine, None, None]:
    """TCP connection to the PRIMARY of the isolated heavy cluster."""
    node = heavy_cluster.primary()
    engine = OrderbookEngine(host="127.0.0.1", port=node.tcp_port, timeout=60.0)
    yield engine
    engine.close()


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


@pytest.fixture(scope="module")
def mm_cluster(request) -> Generator[ClusterManager, None, None]:
    """Three multi-master nodes, converged, on their own etcd.

    A separate topology from the session cluster: every node accepts writes and there
    is no primary. Module-scoped because bringing three nodes and a mesh up is worth
    doing once, and because nothing else should have to share it.
    """
    cm = ClusterManager()
    cm.start_multi_master(node_count=3)
    cm.wait_for_mm_mesh(timeout=45)
    if getattr(request.config, "_ob_cluster", None) is None:
        request.config._ob_cluster = cm
    yield cm
    cm.shutdown()


@pytest.fixture
def healthy_mm_cluster(mm_cluster: ClusterManager) -> Generator[ClusterManager, None, None]:
    """The multi-master cluster, with the mesh restored after each test.

    Multi-master has no election, so a node that stays dead does not get replaced —
    it simply leaves the next test with a two-node mesh and no explanation.
    """
    yield mm_cluster

    for index, node in enumerate(mm_cluster.nodes):
        if node.process is None or node.process.poll() is not None:
            mm_cluster.restart_node(index)

    # 90s rather than 45: a restarted node replays its WAL before it serves, and a
    # module that has been streaming live market data leaves a WAL worth replaying.
    # A tighter timeout here failed once with the node still coming up.
    mm_cluster.wait_for_mm_mesh(timeout=90)


@pytest.fixture(scope="module")
def failover_cluster(request) -> Generator[ClusterManager, None, None]:
    """A cluster of its own for tests that kill nodes or move the primary.

    Restoring the shared cluster after each destructive test is not enough, and this
    was measured rather than assumed: teardown saw one primary and one replica, and
    minutes later ten unrelated tests failed with "No node with REPLICA role found".
    Lease TTLs and the election cooldown keep the roles moving after any single check
    says they have settled, so a topology test and a shared cluster cannot coexist.
    """
    cm = ClusterManager()
    cm.start()
    if getattr(request.config, "_ob_cluster", None) is None:
        request.config._ob_cluster = cm
    yield cm
    cm.shutdown()


@pytest.fixture
def healthy_cluster(failover_cluster: ClusterManager) -> Generator[ClusterManager, None, None]:
    """The failover cluster, verified healthy again once each test is done.

    Restoration still matters *within* the module: each failover test should start
    from one primary and one replica rather than inheriting whatever the previous
    one left. Teardown restarts whatever is not running and waits for exactly one
    primary; if it cannot get there it raises, because a silently half-restored
    cluster makes the next test's red point at the wrong code.
    """
    cluster = failover_cluster
    yield cluster

    for index, node in enumerate(cluster.nodes):
        if node.process is None or node.process.poll() is not None:
            cluster.restart_node(index)

    cluster._wait_for_election(timeout=30.0)

    roles = [cluster._query_role(n).strip().upper() for n in cluster.nodes]
    primaries = [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]
    if len(primaries) != 1:
        raise RuntimeError(
            f"cluster not restored after the test: expected exactly one primary, "
            f"roles={roles}"
        )


@pytest.fixture
def pool_client(cluster: ClusterManager) -> Generator[OrderbookEngine, None, None]:
    """Pool-mode client with addresses of all cluster nodes."""
    hosts = [f"127.0.0.1:{n.tcp_port}" for n in cluster.nodes]
    engine = OrderbookEngine(hosts=hosts, timeout=10.0, health_check_interval=2.0)
    yield engine
    engine.close()
