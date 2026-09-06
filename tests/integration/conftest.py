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


@dataclass
class NodeTls:
    """TLS on the node links, for a whole cluster (#30 part three, series D).

    On the manager rather than a per-call argument, for the same reason `cluster_secret_file` is:
    it has to apply to restarts too. A node that came back without TLS would be refused by its
    peers, and the test would read that as a replication defect.

    `cert_dir` holds `node-<i>.pem` and `node-<i>-key.pem`, one per node index, plus `ca.pem`. One
    certificate per node rather than one shared: sharing would work and would make the mesh unable
    to tell its members apart, which is the property `--tls-peer-names` is about.
    """

    cert_dir: str
    ca: str
    peer_names: str = ""        # comma separated; empty = any identity this CA signed
    replication: bool = True
    multi_master: bool = True

    def files_for(self, index: int) -> tuple[str, str]:
        return (f"{self.cert_dir}/node-{index}.pem", f"{self.cert_dir}/node-{index}-key.pem")


# ---------------------------------------------------------------------------
# ClusterManager
# ---------------------------------------------------------------------------

def instrumented_run() -> bool:
    """Whether the server under test is running under a sanitizer.

    Read from the environment rather than from the binary, because the environment is what makes it
    true: the sanitizer job exports `TSAN_OPTIONS`, and instrumentation without those options set is
    not a configuration this suite has.
    """
    return bool(os.environ.get("TSAN_OPTIONS") or os.environ.get("ASAN_OPTIONS"))


def patience(seconds: float) -> float:
    """A timeout, tripled where a sanitizer is instrumenting the server.

    ThreadSanitizer costs five to fifteen times the run time, so every wait in this suite was chosen
    against an uninstrumented build and then applied to an instrumented one. That is how
    `sanitizers-integration (tsan)` failed with "node-1 never accepted connections" on a branch
    whose only changes were Python files and documentation: a 30-second startup budget on a loaded
    shared runner, for a node that takes two seconds here.

    It is scaling, not silencing. A node that cannot start inside the scaled window is a defect, and
    the failure still says so — this only stops an honest slow start from being reported as one.
    """
    return seconds * (3.0 if instrumented_run() else 1.0)


def open_node_log(data_dir: str) -> "io.TextIOWrapper":
    """A file for a node's stdout and stderr, in its own data directory.

    **Not `subprocess.PIPE`.** Nothing here ever read those pipes, and a pipe nobody drains fills at
    64 KB — after which the node blocks inside `write()`, stops serving, and still looks alive to
    `poll()`. Measured on this machine (i3-7100U, Release, default log level, which is INFO and not
    DEBUG as the workspace notes claimed): 2000 writes cost **153 bytes** in total, because writes
    are not logged at INFO — but **each client connection costs ~153 bytes**. That puts the ceiling
    at roughly **418 connections per node**, and the `cluster` fixture is session-scoped across 145
    tests, each opening a connection per command. The battery goes past that.

    Honest about what it does *not* explain: the failure that sent me looking reported the outgoing
    primary as `UNREACHABLE`, which `role_of()` returns for a refused connection and not for a
    timeout. A node blocked in `write()` keeps its listening socket, so it times out rather than
    refusing. So this is a real hazard removed, not the cause of that run — and the same change is
    what makes the cause findable, because a file survives to be read.

    `scripts/mm_harness.py` — the harness written *to find* defects — logs each node to its own file
    and says in its docstring that this is how #61 was found. The pytest fixture did not, and the
    difference stayed invisible until the pipes mattered.

    A file also gives the diagnosis roadmap #86 asked for and did not have: the node's own log at the
    moment it stopped answering.
    """
    path = os.path.join(data_dir, "node.log")
    exists = os.path.exists(path)
    # Append: `restart_node()` reuses the data directory, so "w" would truncate the log of the node
    # that had just died — deleting the evidence in the act of repairing the cluster.
    handle = open(path, "a", encoding="utf-8", buffering=1)
    if exists:
        handle.write("\n──────── node restarted, same data directory ────────\n")
    return handle


def node_log_size(node: "NodeInfo") -> int:
    """Where a node's log has got to, so a test can read only what it caused.

    The whole log of a shared session cluster is other tests' output; an assertion over it would
    pass or fail on history. Recording the offset first turns "the log does not contain X" into "the
    log did not gain X during this test", which is the claim worth making.
    """
    try:
        return os.path.getsize(os.path.join(node.data_dir, "node.log"))
    except OSError:
        return 0


def node_log_since(node: "NodeInfo", offset: int) -> str:
    """Everything a node logged after `offset`."""
    path = os.path.join(node.data_dir, "node.log")
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            handle.seek(offset)
            return handle.read()
    except OSError as exc:
        return f"(no log at {path}: {exc})"


def tail_node_log(node: "NodeInfo", lines: int = 40) -> str:
    """The end of a node's log, for an assertion that needs to say why rather than what."""
    path = os.path.join(node.data_dir, "node.log")
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            return "".join(handle.readlines()[-lines:])
    except OSError as exc:
        return f"(no log at {path}: {exc})"


def server_binary_path() -> str:
    """The server this run tests, honouring OB_SERVER_BINARY.

    One function, because four modules had grown their own copy of
    `os.path.join(REPO, "build", "ob_tcp_server")` and none of them honoured the override. Those
    modules start their own nodes rather than using `ClusterManager` - they are about simultaneous
    starts, crash recovery and multi-master stats, which the shared fixture deliberately serialises
    or shares - so each grew the path and none grew the override.
    
    What that cost is worse than the CI failure that exposed it: running any of them against a
    sanitizer tree silently tested the **plain** build, because a stale `build/ob_tcp_server` was
    there to be found. `test_mm_stats.py` is one of the three modules the TSan job had been running
    since it was created, so part of that job had been checking an uninstrumented binary all along.
    A required check that quietly measures the wrong artefact is worse than no check.

    `test_binary_path_is_shared` in `test_smoke.py` refuses a module that builds its own.
    """
    from_env = os.environ.get("OB_SERVER_BINARY")
    if from_env:
        return from_env
    return str(Path(__file__).resolve().parents[2] / "build" / "ob_tcp_server")


def cpp_client_binary_path() -> Optional[str]:
    """The `ob_integration_test` harness, from the same build tree as the server under test.

    Derived from `OB_SERVER_BINARY` rather than guessed: the harness lives at
    `<build>/tests/ob_integration_test` beside the server's `<build>/ob_tcp_server`. Without that
    derivation the sanitizer job pointed at `build-tsan/` while a module looked in `build/`, so its
    tests skipped - and a skipped integration test is indistinguishable from a passing one in a
    summary line (#85).

    Here rather than in a module, and this is the second time: `test_cpp_client.py` had its own
    copy, and `test_auth.py` grew a third one that found nothing under TSan and failed. One place,
    the same rule as `server_binary_path()`.

    Returns None when the binary was not built. Callers decide between skipping and failing -
    the TSan job builds it deliberately, so failing there is the honest choice.
    """
    from_env = os.environ.get("OB_SERVER_BINARY")
    if from_env:
        derived = Path(from_env).resolve().parent / "tests" / "ob_integration_test"
        if derived.is_file():
            return str(derived)
    for candidate in (ClusterManager._PROJECT_ROOT / "build" / "tests" / "ob_integration_test",
                      ClusterManager._PROJECT_ROOT / "build-release" / "tests" / "ob_integration_test"):
        if candidate.is_file():
            return str(candidate)
    return None


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
        # OB_SERVER_BINARY lets the whole suite run against a different build — a sanitizer
        # tree, most usefully. A lock-order inversion or a data race in the io loop only shows up
        # when real clients and real peers are driving it, which no unit test arranges.
        self.server_binary: str = (
            server_binary
            or os.environ.get("OB_SERVER_BINARY")
            or str(self._PROJECT_ROOT / self._SERVER_BINARY)
        )
        self.etcd_binary: str = (
            etcd_binary or os.environ.get("OB_ETCD_BINARY") or "etcd"
        )
        # Open log files, closed on shutdown. Held by the manager rather than by NodeInfo because a
        # restart replaces the NodeInfo and the old handle still needs closing.
        self._node_logs: list = []
        # Path to a cluster secret file, or None. Set before start()/start_multi_master() by the
        # fixture that wants an authenticated cluster (#30 part two). On the manager rather than a
        # per-call argument because it applies to every node including restarts: a node that came
        # back without the secret would be refused by its peers, and the test would read that as a
        # replication defect.
        self.cluster_secret_file: Optional[str] = None
        # TLS on the node links, or None. Same placement and the same reason as the secret file
        # above: it must survive a restart.
        self.node_tls: Optional[NodeTls] = None
        # Indices this harness killed on purpose. Without it, teardown cannot tell a test that
        # killed a node from a node that died on its own, and it silently restarts both — which is
        # how a crashing node stays invisible for as long as the tests around it pass.
        self._deliberately_killed: set = set()
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
        self._wait_for_etcd(timeout=patience(30))

        # Start node-0 first and wait for it to become PRIMARY
        node0 = self._start_node(0)
        self.nodes.append(node0)
        self._wait_for_node(node0, timeout=patience(15))
        self._wait_for_primary(node0, timeout=patience(15))

        # Now start node-1 — it will discover node-0 as leader via etcd
        node1 = self._start_node(1)
        self.nodes.append(node1)
        self._wait_for_node(node1, timeout=patience(15))

        self._wait_for_election(timeout=patience(15))
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

        # After the nodes are stopped, not before: a live node writing into a closed handle gets
        # EBADF, which is the freeze this replaced wearing a different hat.
        for log in self._node_logs:
            try:
                log.close()
            except Exception:
                pass
        self._node_logs = []

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
        self._wait_for_etcd(timeout=patience(30))

        for index in range(node_count):
            node = self._start_node(index, multi_master_id=index + 1)
            self.nodes.append(node)
            self._wait_for_node(node, timeout=20)

        self._started = True
        atexit.register(self.shutdown)

    def add_multi_master_node(self, timeout: float = 20.0) -> NodeInfo:
        """Add one fresh multi-master node to a cluster that is already running.

        The harness could not express this before, and that is why roadmap #67 went
        untested for as long as it did: every fixture starts all nodes at once, so no
        test ever had a node joining a cluster with data already in it — the only
        situation in which a node cannot establish a contiguous frontier by following
        its peers' streams.
        """
        index = len(self.nodes)
        node = self._start_node(index, multi_master_id=index + 1)
        self.nodes.append(node)
        self._wait_for_node(node, timeout=timeout)
        return node

    def wait_for_mm_mesh(self, timeout: float = 30.0) -> None:
        """Wait until every node is **connected** to all the others in MM_PEERS.

        Without this, a test writing to node 0 and reading from node 2 can run before
        the two have connected, and the failure looks like lost data rather than a
        test that started too early.

        Counting listed rows was not enough for that job: a peer discovered through the
        etcd topology watch gets a row before its connection is up, and one whose link
        has dropped keeps it. Both read as converged while the write has nowhere to go -
        the same distinction series D needed to tell a refused peer from a live one.
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
                    counts.append(len(self.mm_connected_peers(node)))
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

    def mm_peer_rows(self, node: NodeInfo) -> list:
        """MM_PEERS rows as split columns: node_id, address, status, hlc, lag_bytes.

        The one place these rows are parsed. Two modules had their own copy before, which is
        how the substring below survived in a third.
        """
        reply = self._send(node, "MM_PEERS")
        lines = [ln for ln in reply.strip().splitlines()[1:]
                 if ln and not ln.startswith("OK")]
        return [ln.split("\t") for ln in lines]

    def mm_connected_peers(self, node: NodeInfo) -> list:
        """The node_ids of the peers whose connection is up, by the status column.

        **The word `connected` cannot be counted**, because the column carries `connected` or
        `disconnected` and `"disconnected".count("connected") == 1`. The series D allowlist test
        counted the word and read a mesh with one live peer and one refused one as two peers -
        an assertion that would have passed against a node that connected to nobody at all.
        Same family as pitfall 87: the token that answers the question is a suffix of the token
        that answers the opposite one.
        """
        return [row[0] for row in self.mm_peer_rows(node)
                if len(row) > 2 and row[2] == "connected"]

    def _node_argv(self, *, node_index: int, node_id: str, data_dir: str, tcp_port: int,
                   replication_port: int, metrics_port: int, read_only: bool,
                   multi_master_id: Optional[int], mm_replication_port: int) -> list:
        """The command line for one node. The **only** place it is built.

        `restart_node()` used to construct its own copy, and it drifted exactly the way a duplicated
        list does: it never learned about `--cluster-secret-file`, so a restarted node in an
        authenticated cluster came back **without the cluster secret** and was refused by its peers -
        which a test reads as a replication defect rather than as a harness defect. Series D found it
        by the same route with `--tls-*`: the restarted replica connected in plaintext and the log
        said `Connection reset by peer` with no TLS line above it.

        The comment that survived next to the multi-master flags in the old copy is the tell - it
        warns about precisely this failure for one flag while three others were missing. Same family
        as pitfall 77, where four modules built their own path to the server binary.
        """
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
        if self.cluster_secret_file:
            cmd += ["--cluster-secret-file", self.cluster_secret_file]
        if self.node_tls is not None:
            cert, key = self.node_tls.files_for(node_index)
            cmd += ["--tls-cert-file", cert, "--tls-key-file", key,
                    "--tls-ca-file", self.node_tls.ca]
            if self.node_tls.replication:
                cmd.append("--tls-replication")
            if self.node_tls.multi_master:
                cmd.append("--tls-multi-master")
            if self.node_tls.peer_names:
                cmd += ["--tls-peer-names", self.node_tls.peer_names]
        if multi_master_id is not None:
            # The server refuses --mm-replication-port equal to --replication-port:
            # they are two different listeners, and colliding them would have one
            # silently shadow the other.
            cmd += [
                "--multi-master",
                "--mm-node-id", str(multi_master_id),
                "--mm-replication-port", str(mm_replication_port),
            ]
        return cmd

    def _start_node(self, node_index: int, read_only: bool = False,
                    multi_master_id: Optional[int] = None) -> NodeInfo:
        tcp_port = self.find_free_port()
        replication_port = self.find_free_port()
        metrics_port = self.find_free_port()
        data_dir = tempfile.mkdtemp(prefix=f"ob_node{node_index}_")
        self.temp_dirs.append(data_dir)
        node_id = f"node-{node_index}"

        mm_replication_port = self.find_free_port() if multi_master_id is not None else 0
        cmd = self._node_argv(node_index=node_index, node_id=node_id, data_dir=data_dir,
                              tcp_port=tcp_port, replication_port=replication_port,
                              metrics_port=metrics_port, read_only=read_only,
                              multi_master_id=multi_master_id,
                              mm_replication_port=mm_replication_port)

        log = open_node_log(data_dir)
        self._node_logs.append(log)
        proc = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=subprocess.STDOUT,
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

        # Why, not just what - and this path had erased the why twice over. `node.process.stderr`
        # is None, because nodes log to a file in their data directory since the unread-pipe fix, so
        # reading it raised `AttributeError: 'NoneType' object has no attribute 'read'` and replaced
        # the diagnosis with a traceback about the diagnosis. And it only looked when the process had
        # **exited**, which is the case that needs it least: a node that is alive and not answering
        # is exactly the one whose log has to be read. Third instance of the same class as the CI
        # step that printed a TSan report only when there was none.
        alive = node.process is None or node.process.poll() is None
        raise RuntimeError(
            f"Node {node.node_id} (port {node.tcp_port}) not ready after {timeout}s: {last_err}\n"
            f"process alive: {alive}\n" + tail_node_log(node, lines=60)
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
        self._deliberately_killed.discard(node_index)
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

        # Through `_node_argv()`, not a second copy of it. A restarted node must come back with
        # *every* flag it had - the cluster secret and the TLS files included - or its peers refuse
        # it and the test reads that as a defect in the engine.
        cmd = self._node_argv(
            node_index=old.index, node_id=new.node_id, data_dir=new.data_dir,
            tcp_port=new.tcp_port, replication_port=new.replication_port,
            metrics_port=new.metrics_port, read_only=new.read_only,
            multi_master_id=(old.index + 1) if old.mm_replication_port else None,
            mm_replication_port=old.mm_replication_port)

        log = open_node_log(new.data_dir)
        self._node_logs.append(log)
        new.process = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.nodes[node_index] = new
        self._wait_for_node(new, timeout=patience(15))

    def unexplained_deaths(self) -> list:
        """Nodes that are not running and were not killed by a test, with their own last words.

        Read **before** restarting anything: `restart_node()` reuses the data directory, and the
        point of this list is the evidence rather than the count.

        This exists because the opposite existed for months. Both `healthy_cluster` and
        `healthy_mm_cluster` restart whatever is not running, and neither could distinguish a
        deliberate `kill_node()` from a crash — so a node that died of its own accord was repaired
        in silence and the suite stayed green. It is the harness-workaround lesson in its purest
        form: the fixture was not hiding a defect it knew about, it was unable to see one.
        """
        deaths = []
        for index, node in enumerate(self.nodes):
            if index in self._deliberately_killed:
                continue
            if node.process is None:
                continue
            code = node.process.poll()
            if code is None:
                continue
            how = f"signal {-code}" if code < 0 else f"exit code {code}"
            deaths.append(
                f"{node.node_id} (index {index}, port {node.tcp_port}) is not running and no test "
                f"killed it: {how}.\n--- tail of its own log ---\n{tail_node_log(node)}")
        return deaths

    def kill_node(self, node_index: int) -> None:
        """SIGKILL a node (simulate crash), and record that this was on purpose."""
        self._deliberately_killed.add(node_index)
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

    # Every port number this process has handed out, so it is never handed out twice.
    _ports_handed_out: set = set()

    @staticmethod
    def find_free_port() -> int:
        """Bind to port 0 and return the OS-assigned port, never the same one twice.

        Binding to port 0 tells you a port that was free *at that instant*. The socket is closed
        before the caller can use it, so two calls in quick succession can return the same number,
        and the second node to start dies with `bind() failed on port N: Address already in use`.
        A single cluster fixture calls this six or more times back to back.

        On a development machine the nodes bind fast enough to hide it. A loaded CI runner widens
        the gap, which is how it appeared with 134 tests passing around it — the same shape as
        pitfall 55 in CLAUDE.md, and the engine was right: it refused to bind and said exactly why.

        This does not defend against something outside this process taking the port in between,
        which is unfixable with this API. It does close the collision the suite was causing itself.
        """
        for _ in range(200):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", 0))
                port = s.getsockname()[1]
            if port not in ClusterManager._ports_handed_out:
                ClusterManager._ports_handed_out.add(port)
                return port
        raise RuntimeError(
            "200 attempts and every port the OS offered had already been handed out in this "
            "process — something is leaking ports or the ephemeral range is exhausted")

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
    "aggregations", "multi_master", "large_response", "binance", "crash_recovery",
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

    # Before the repair, not after: restarting reuses the data directory.
    deaths = mm_cluster.unexplained_deaths()

    for index, node in enumerate(mm_cluster.nodes):
        if node.process is None or node.process.poll() is not None:
            mm_cluster.restart_node(index)

    # 90s rather than 45: a restarted node replays its WAL before it serves, and a
    # module that has been streaming live market data leaves a WAL worth replaying.
    # A tighter timeout here failed once with the node still coming up.
    mm_cluster.wait_for_mm_mesh(timeout=90)

    # Raised after the mesh is whole, so the next test starts from a good cluster and this reads as
    # a defect rather than as a cascade.
    if deaths:
        raise AssertionError(
            "A multi-master node died during this test and no test killed it:\n\n"
            + "\n\n".join(deaths))


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

    # Before the repair, not after: restarting reuses the data directory. These modules kill nodes
    # on purpose all the time, so the distinction is the whole value — `unexplained_deaths()` skips
    # anything `kill_node()` recorded.
    deaths = cluster.unexplained_deaths()

    for index, node in enumerate(cluster.nodes):
        if node.process is None or node.process.poll() is not None:
            cluster.restart_node(index)

    # The timeout has to clear the handover cooldown, not just an election. A node that has just
    # handed the role away refuses to stand for election for --handover-cooldown-seconds (15 by
    # default), so a 30-second budget was only barely enough and the suite flickered once graceful
    # handover started working for real (roadmap #60): before that, these tests were no-ops that
    # answered ERR unknown_target and never moved a role at all.
    try:
        cluster._wait_for_election(timeout=45.0)
    except Exception:
        # One restart of everything, then insist. A silently half-restored cluster makes the next
        # module's red point at the wrong code — which is exactly what happened: a smoke test
        # failed with "No node with PRIMARY role found" because a failover test left no primary.
        for index in range(len(cluster.nodes)):
            cluster.restart_node(index)
        cluster._wait_for_election(timeout=45.0)

    roles = [cluster._query_role(n).strip().upper() for n in cluster.nodes]
    primaries = [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]
    if len(primaries) != 1:
        raise RuntimeError(
            f"cluster not restored after the test: expected exactly one primary, "
            f"roles={roles}"
        )

    # Last, so the cluster is whole first: a death report that leaves the next module a broken
    # cluster turns one finding into a page of unrelated red.
    if deaths:
        raise AssertionError(
            "A node died during this test and no test killed it:\n\n" + "\n\n".join(deaths))


@pytest.fixture
def pool_client(cluster: ClusterManager) -> Generator[OrderbookEngine, None, None]:
    """Pool-mode client with addresses of all cluster nodes."""
    hosts = [f"127.0.0.1:{n.tcp_port}" for n in cluster.nodes]
    engine = OrderbookEngine(hosts=hosts, timeout=10.0, health_check_interval=2.0)
    yield engine
    engine.close()
