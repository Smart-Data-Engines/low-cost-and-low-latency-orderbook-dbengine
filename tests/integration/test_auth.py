"""Wire authentication against a live server (#30, part one).

What only a live node can decide: that the process **refuses to start** on a bad secret file, that
the secret appears in no artefact an operator will read, and that our own two clients - Python and
C++ - authenticate against the real protocol rather than against a mock of it.

These nodes are started here rather than through the shared cluster fixture because each one needs
a different secret file, and half of them are expected not to start at all. The binary comes from
`conftest.server_binary_path()`; a module that builds its own path silently tests whichever binary
happens to be lying around, which is what #85 found in four modules at once.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
from conftest import server_binary_path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
from orderbook_engine import OrderbookEngine, OrderbookError  # noqa: E402

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()

ALICE_SECRET = "0123456789abcdef0123456789abcdef-alice"
CLUSTER_SECRET = "fedcba9876543210fedcba9876543210-cluster"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def client_response(secret: str, identity: str, nonce: str, surface: str = "client") -> str:
    """The response the server expects, computed independently of the client library.

    Written out here rather than imported so that this test would fail if the client and the
    server agreed with each other on a construction different from the documented one.
    """
    message = b"\x00".join([b"ob-auth-v1", surface.encode(),
                            identity.encode(), nonce.encode()])
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def write_secret_file(path: Path, contents: str, mode: int = 0o600) -> Path:
    path.write_text(contents)
    path.chmod(mode)
    return path


class Node:
    """One ob_tcp_server with its own data dir, secret files and log file.

    stderr goes to a file rather than to DEVNULL or a pipe: a pipe nobody reads blocks the node at
    64 kB (#86), and DEVNULL would throw away the log these tests grep for a secret.
    """

    def __init__(self, tmp: Path, *, auth_file: Path | None = None,
                 cluster_file: Path | None = None, metrics: bool = False):
        self.tmp = tmp
        self.port = 0
        self.metrics_port = 0
        self.proc: subprocess.Popen | None = None
        self.log_path = tmp / "node.log"
        self.auth_file = auth_file
        self.cluster_file = cluster_file
        self.metrics = metrics

    def argv(self) -> list[str]:
        args = [SERVER, "--port", str(self.port), "--data-dir", str(self.tmp / "data"),
                "--flush-interval-ms", "600000"]
        if self.auth_file:
            args += ["--auth-secret-file", str(self.auth_file)]
        if self.cluster_file:
            args += ["--cluster-secret-file", str(self.cluster_file)]
        if self.metrics:
            args += ["--metrics-port", str(self.metrics_port), "--metrics-bind", "127.0.0.1"]
        return args

    def start(self, timeout: float = 20.0) -> None:
        self.port = free_port()
        if self.metrics:
            self.metrics_port = free_port()
        self.log = open(self.log_path, "a", buffering=1)
        self.proc = subprocess.Popen(self.argv(), stdout=self.log, stderr=self.log)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"node exited with {self.proc.returncode}:\n{self.log_text()}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=2) as s:
                    s.settimeout(2)
                    s.recv(4096)
                    s.sendall(b"PING\n")
                    if b"PONG" in s.recv(1024):
                        return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(f"node on port {self.port} never answered:\n{self.log_text()}")

    def run_and_wait(self, timeout: float = 20.0) -> subprocess.CompletedProcess:
        """Start the node expecting it *not* to stay up, and return the finished process."""
        self.port = free_port()
        return subprocess.run(self.argv(), capture_output=True, text=True, timeout=timeout)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if getattr(self, "log", None):
            self.log.close()

    def log_text(self) -> str:
        return self.log_path.read_text(errors="replace") if self.log_path.exists() else ""


class Wire:
    """A bare socket, so the tests can see the protocol rather than the client's view of it."""

    def __init__(self, port: int, timeout: float = 10.0):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.sock.recv(4096)  # banner

    def send(self, text: str, settle: float = 0.3) -> str:
        self.sock.sendall((text + "\n").encode())
        time.sleep(settle)
        try:
            return self.sock.recv(1 << 20).decode(errors="replace")
        except socket.timeout:
            return ""

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


@pytest.fixture
def authed_node():
    with tempfile.TemporaryDirectory(prefix="ob_auth_") as d:
        tmp = Path(d)
        secrets = write_secret_file(tmp / "clients", f"alice {ALICE_SECRET}\n")
        node = Node(tmp, auth_file=secrets, metrics=True)
        node.start()
        try:
            yield node
        finally:
            node.stop()


@pytest.fixture
def open_node():
    with tempfile.TemporaryDirectory(prefix="ob_noauth_") as d:
        node = Node(Path(d))
        node.start()
        try:
            yield node
        finally:
            node.stop()


# ── The protocol on the wire ──────────────────────────────────────────────────

def test_an_unauthenticated_session_can_only_ping_and_authenticate(authed_node):
    w = Wire(authed_node.port)
    try:
        assert "PONG" in w.send("PING"), "a health check must not need credentials"
        assert "ERR unauthenticated" in w.send("SELECT * FROM orderbook")
        assert "ERR unauthenticated" in w.send("STATUS")
        assert "ERR unauthenticated" in w.send("COMPRESS LZ4")
    finally:
        w.close()


def test_a_full_challenge_response_admits_the_session(authed_node):
    w = Wire(authed_node.port)
    try:
        challenge = w.send("AUTH")
        assert challenge.startswith("OK CHALLENGE "), challenge
        nonce = challenge.split()[2]
        digest = client_response(ALICE_SECRET, "alice", nonce)
        assert "OK AUTH alice" in w.send(f"AUTH alice {digest}")
        # And the session works from here.
        assert "ERR unauthenticated" not in w.send("SELECT * FROM orderbook")
        status = w.send("STATUS")
        assert "identity: alice" in status, status
    finally:
        w.close()


def test_a_wrong_response_closes_the_connection(authed_node):
    w = Wire(authed_node.port)
    try:
        challenge = w.send("AUTH")
        nonce = challenge.split()[2]
        wrong = client_response("not-the-secret-not-the-secret-xx", "alice", nonce)
        assert "ERR auth_failed" in w.send(f"AUTH alice {wrong}")
        # One attempt per connection is the entire rate limit, so the close is the mechanism.
        w.sock.sendall(b"PING\n")
        time.sleep(0.3)
        assert w.sock.recv(4096) == b"", "the server kept the session after a failed attempt"
    finally:
        w.close()


def test_a_response_for_another_surface_does_not_admit_a_client(authed_node):
    # Domain separation on the wire: the cluster links use the same HMAC with a different label.
    w = Wire(authed_node.port)
    try:
        nonce = w.send("AUTH").split()[2]
        digest = client_response(ALICE_SECRET, "alice", nonce, surface="replication")
        assert "ERR auth_failed" in w.send(f"AUTH alice {digest}")
    finally:
        w.close()


def test_auth_is_refused_when_the_server_does_not_authenticate(open_node):
    # The client must find out. An OK here would be an assurance with nothing behind it.
    w = Wire(open_node.port)
    try:
        assert "ERR auth_disabled" in w.send("AUTH")
    finally:
        w.close()


def test_the_counters_move(authed_node):
    w = Wire(authed_node.port)
    try:
        nonce = w.send("AUTH").split()[2]
        w.send("AUTH alice " + client_response(ALICE_SECRET, "alice", nonce))
    finally:
        w.close()
    bad = Wire(authed_node.port)
    try:
        nonce = bad.send("AUTH").split()[2]
        bad.send("AUTH alice " + client_response("wrong-secret-wrong-secret-wrong!", "alice", nonce))
    finally:
        bad.close()

    with socket.create_connection(("127.0.0.1", authed_node.metrics_port), timeout=5) as s:
        s.sendall(b"GET /metrics HTTP/1.0\r\n\r\n")
        body = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            body += chunk
    text = body.decode(errors="replace")
    # Values carry a node_role label, so match on the name and read the trailing number.
    def value(name: str) -> int:
        for line in text.splitlines():
            if line.startswith(name + "{"):
                return int(line.rsplit(" ", 1)[1])
        raise AssertionError(f"{name} absent from /metrics")
    assert value("ob_auth_challenges_total") >= 2
    assert value("ob_auth_success_total") >= 1
    assert value("ob_auth_failures_total") >= 1


# ── Refusals at startup ───────────────────────────────────────────────────────

def test_the_server_refuses_to_start_on_a_world_readable_secret_file():
    with tempfile.TemporaryDirectory(prefix="ob_auth_mode_") as d:
        tmp = Path(d)
        secrets = write_secret_file(tmp / "clients", f"alice {ALICE_SECRET}\n", mode=0o644)
        done = Node(tmp, auth_file=secrets).run_and_wait()
        assert done.returncode == 1, done.stdout + done.stderr
        combined = done.stdout + done.stderr
        assert "readable beyond its owner" in combined, combined
        assert "0644" in combined, combined
        assert ALICE_SECRET not in combined, "the refusal printed the secret"


def test_the_server_refuses_to_start_when_the_cluster_secret_is_also_a_client_secret():
    # Sharing it would mean client authentication grants node privileges: a client presenting
    # itself as a replica streams the whole write-ahead log.
    with tempfile.TemporaryDirectory(prefix="ob_auth_share_") as d:
        tmp = Path(d)
        clients = write_secret_file(tmp / "clients", f"alice {ALICE_SECRET}\n")
        cluster = write_secret_file(tmp / "cluster", f"{ALICE_SECRET}\n")
        done = Node(tmp, auth_file=clients, cluster_file=cluster).run_and_wait()
        assert done.returncode == 1, done.stdout + done.stderr
        combined = done.stdout + done.stderr
        assert "is also a client secret" in combined, combined
        assert ALICE_SECRET not in combined, "the refusal printed the secret"


def test_the_server_refuses_a_short_secret():
    with tempfile.TemporaryDirectory(prefix="ob_auth_short_") as d:
        tmp = Path(d)
        secrets = write_secret_file(tmp / "clients", "alice hunter2\n")
        done = Node(tmp, auth_file=secrets).run_and_wait()
        assert done.returncode == 1
        assert "hunter2" not in done.stdout + done.stderr


# ── The secret appears in no artefact ─────────────────────────────────────────

def test_the_secret_is_absent_from_the_node_log_and_from_print_config(authed_node):
    w = Wire(authed_node.port)
    try:
        nonce = w.send("AUTH").split()[2]
        w.send("AUTH alice " + client_response(ALICE_SECRET, "alice", nonce))
        w.send("AUTH bad-identity " + "0" * 64)
    finally:
        w.close()
    log = authed_node.log_text()
    assert "authenticated as identity=alice" in log, log[-2000:]
    assert ALICE_SECRET not in log, "the secret reached the node's log"

    printed = subprocess.run(authed_node.argv() + ["--print-config"],
                             capture_output=True, text=True, timeout=30)
    assert printed.returncode == 0, printed.stderr
    assert str(authed_node.auth_file) in printed.stdout
    assert ALICE_SECRET not in printed.stdout, "--print-config printed the secret"


def test_a_claimed_identity_cannot_inject_a_line_into_the_log(authed_node):
    # A newline in the claimed identity would let a peer write its own log lines. The parser bounds
    # the shape, and sanitise_for_log() is the second layer; this checks the pair, end to end.
    w = Wire(authed_node.port)
    try:
        w.send("AUTH")
        w.send("AUTH alice\\nINFO forged " + "0" * 64)
    finally:
        w.close()
    assert "INFO forged" not in authed_node.log_text()


# ── Our own clients ───────────────────────────────────────────────────────────

def test_the_python_client_authenticates(authed_node):
    eng = OrderbookEngine(host="127.0.0.1", port=authed_node.port,
                          auth=("alice", ALICE_SECRET))
    try:
        assert eng.ping()
        eng.insert("BTCUSD", "EX", "bid", [50_000], [10])
    finally:
        eng.close()


def test_the_python_client_without_credentials_cannot_work(authed_node):
    eng = OrderbookEngine(host="127.0.0.1", port=authed_node.port)
    try:
        with pytest.raises(OrderbookError):
            eng.query("SELECT * FROM orderbook")
    finally:
        eng.close()


def test_the_python_client_with_credentials_against_an_open_server_fails_loudly(open_node):
    # Not silently continuing: a client that believes it authenticated against a server that
    # authenticates nobody has a deployment problem worth an exception.
    with pytest.raises(OrderbookError):
        OrderbookEngine(host="127.0.0.1", port=open_node.port,
                        auth=("alice", ALICE_SECRET))


def test_the_cpp_client_authenticates(authed_node):
    binary = Path(SERVER).parent / "tests" / "ob_integration_test"
    if not binary.exists():
        binary = Path(SERVER).parent / "ob_integration_test"
    assert binary.exists(), f"ob_integration_test not built at {binary}"
    env = dict(os.environ, OB_AUTH_IDENTITY="alice", OB_AUTH_SECRET=ALICE_SECRET)
    done = subprocess.run([str(binary), "--host", "127.0.0.1",
                           "--port", str(authed_node.port), "--test", "ping"],
                          capture_output=True, text=True, timeout=60, env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert '"pass"' in done.stdout, done.stdout


def test_the_cpp_client_without_credentials_is_refused(authed_node):
    binary = Path(SERVER).parent / "tests" / "ob_integration_test"
    if not binary.exists():
        binary = Path(SERVER).parent / "ob_integration_test"
    env = {k: v for k, v in os.environ.items()
           if k not in ("OB_AUTH_IDENTITY", "OB_AUTH_SECRET")}
    done = subprocess.run([str(binary), "--host", "127.0.0.1",
                           "--port", str(authed_node.port), "--test", "insert_query"],
                          capture_output=True, text=True, timeout=60, env=env)
    assert done.returncode != 0, done.stdout + done.stderr
