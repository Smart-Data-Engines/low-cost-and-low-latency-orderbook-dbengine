"""#52, step 1: the server's Unix socket, through the binary and the clients.

`--unix-socket <path>` makes the server listen on a Unix stream socket beside its TCP port, for
clients on the same host: the same protocol without TCP. `tests/test_unix_socket.cpp` runs a server
in its own process for the accept path, the hand-off and the drain under the sanitizers; this module
covers what only the binary and the clients can show:
- the Python client through the socket, a subscription's pushes included;
- AUTH through it, and TLS that stays on the port;
- the file's mode, a stale file replaced, a live one or a file that is not a socket refused, and the
  file removed at SIGTERM;
- `ob_backup --unix-socket`;
- `ob_unix_connections_total`.

A socket's path holds 107 bytes, more than a pytest temporary directory leaves room for, so every
test works in a short directory of its own.
"""
from __future__ import annotations

import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import pytest

from conftest import free_port, patience, server_binary_path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
from orderbook_engine import OrderbookEngine, OrderbookError, OrderbookTlsError  # noqa: E402

pytestmark = pytest.mark.smoke

OPS_SECRET = "0123456789abcdef0123456789abcdef-ops"
EVERYTHING = "WHERE timestamp BETWEEN 0 AND 9999999999999999999"


@pytest.fixture
def workdir():
    d = Path(tempfile.mkdtemp(prefix="obu-"))
    assert len(str(d / "ob.sock")) < 100, f"the temporary directory {d} is too long for a socket"
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


class Node:
    """One ob_tcp_server on a port of its own and a Unix socket, with its log in a file."""

    def __init__(self, workdir: Path, extra: list[str] | None = None, sock: Path | None = None):
        self.workdir = workdir
        self.sock = sock or workdir / "ob.sock"
        self.port = free_port()
        self.extra = list(extra or [])
        self.log_path = workdir / f"node-{self.port}.log"
        self.proc: subprocess.Popen | None = None

    def argv(self) -> list[str]:
        return [server_binary_path(), "--port", str(self.port), "--bind", "127.0.0.1",
                "--data-dir", str(self.workdir / f"data-{self.port}"),
                "--unix-socket", str(self.sock), *self.extra]

    def start(self) -> None:
        log = open(self.log_path, "a", buffering=1)
        self.proc = subprocess.Popen(self.argv(), stdout=log, stderr=log)
        deadline = time.monotonic() + patience(20)
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"the node exited with {self.proc.returncode}:\n{self.log()}")
            if greeted_through(self.sock):
                return
            time.sleep(0.1)
        self.proc.kill()
        raise AssertionError(f"nothing answered through {self.sock}:\n{self.log()}")

    def refused_start(self) -> subprocess.CompletedProcess:
        """Run a node expected to refuse its start, to its exit."""
        return subprocess.run(self.argv(), capture_output=True, text=True, timeout=patience(30))

    def stop(self) -> int | None:
        if self.proc is None or self.proc.poll() is not None:
            return None if self.proc is None else self.proc.returncode
        self.proc.terminate()
        try:
            return self.proc.wait(timeout=patience(20))
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return self.proc.wait()

    def log(self) -> str:
        return self.log_path.read_text(errors="replace") if self.log_path.exists() else ""


def greeted_through(path: Path) -> bool:
    """Whether a connection through `path` is given the banner."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect(str(path))
            data = b""
            while not data.endswith(b"\n\n"):
                chunk = s.recv(4096)
                if not chunk:
                    return False
                data += chunk
            return data.startswith(b"OK ob_tcp_server v")
    except OSError:
        return False


def counter(metrics_port: int, name: str) -> int:
    with urllib.request.urlopen(f"http://127.0.0.1:{metrics_port}/metrics", timeout=5) as r:
        text = r.read().decode()
    m = re.search(rf"^{name}(?:\{{[^}}]*\}})? (\d+)$", text, re.M)
    assert m, f"{name} is not in /metrics"
    return int(m.group(1))


def test_the_python_client_speaks_the_whole_protocol_through_the_socket(workdir):
    node = Node(workdir)
    node.start()
    try:
        with OrderbookEngine(unix_socket=str(node.sock)) as writer, \
             OrderbookEngine(unix_socket=str(node.sock)) as reader:
            assert writer.mode == "tcp"
            assert writer.ping() == "PONG"
            sid = reader.subscribe("UDS", "EX")

            writer.insert("UDS", "EX", "bid", [10000, 9990], [5, 6])
            writer.insert("UDS", "EX", "ask", [10010], [7])

            book = writer.book("UDS", "EX")
            assert sorted((r.side, r.price, r.quantity) for r in book) == [
                ("ask", 10010, 7), ("bid", 9990, 6), ("bid", 10000, 5)]

            pushed = []
            deadline = time.monotonic() + patience(10)
            while len(pushed) < 3 and time.monotonic() < deadline:
                pushed += reader.poll(timeout=0.5)
            assert sorted((r.side, r.price) for _, r in pushed) == [
                ("ask", 10010), ("bid", 9990), ("bid", 10000)], pushed
            assert {s for s, _ in pushed} == {sid}

            writer.flush()
            assert len(writer.query(f"SELECT * FROM 'UDS'.'EX' {EVERYTHING}")) == 3
    finally:
        assert node.stop() == 0

    # Each connection is logged with the process at its other end, in place of an address (2.4).
    me = f"from unix pid={os.getpid()} uid={os.getuid()} gid={os.getgid()}"
    assert node.log().count(me) >= 2, node.log()[-3000:]


def test_the_socket_asks_for_auth_like_the_port(workdir):
    secrets = workdir / "clients.secret"
    secrets.write_text(f"ops {OPS_SECRET}\n")
    secrets.chmod(0o600)
    node = Node(workdir, ["--auth-secret-file", str(secrets)])
    node.start()
    try:
        with OrderbookEngine(unix_socket=str(node.sock)) as anonymous:
            with pytest.raises(OrderbookError, match="unauthenticated"):
                anonymous.insert("UDS-A", "EX", "bid", [100], [1])
        with OrderbookEngine(unix_socket=str(node.sock), auth=("ops", OPS_SECRET)) as ops:
            ops.insert("UDS-A", "EX", "bid", [100], [1])
            assert [r.price for r in ops.book("UDS-A", "EX")] == [100]
        with pytest.raises(OrderbookError):
            OrderbookEngine(unix_socket=str(node.sock), auth=("ops", "x" * 40))
    finally:
        node.stop()


def test_tls_on_the_port_leaves_the_socket_plain(workdir):
    cert, key = workdir / "cert.pem", workdir / "key.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-keyout", str(key),
                    "-out", str(cert), "-days", "1", "-nodes", "-subj", "/CN=uds",
                    "-addext", "subjectAltName=IP:127.0.0.1"], check=True, capture_output=True)
    key.chmod(0o600)
    node = Node(workdir, ["--tls-client", "--tls-cert-file", str(cert), "--tls-key-file", str(key)])
    node.start()
    try:
        with OrderbookEngine(unix_socket=str(node.sock)) as local:
            assert local.ping() == "PONG"
        with OrderbookEngine(host="127.0.0.1", port=node.port, tls=True,
                             tls_ca_file=str(cert)) as remote:
            assert remote.ping() == "PONG"
        assert "not encrypted: the file's permissions decide who may connect" in node.log()
    finally:
        node.stop()


def test_the_socket_file_takes_the_mode_it_is_given(workdir):
    default = Node(workdir)
    narrow = Node(workdir, ["--unix-socket-mode", "0600"], sock=workdir / "narrow.sock")
    default.start()
    narrow.start()
    try:
        for node, mode in ((default, 0o660), (narrow, 0o600)):
            st = os.lstat(node.sock)
            assert stat.S_ISSOCK(st.st_mode)
            assert stat.S_IMODE(st.st_mode) == mode, (node.sock, oct(st.st_mode))
    finally:
        default.stop()
        narrow.stop()


def test_what_a_killed_server_left_is_replaced_and_a_live_socket_is_refused(workdir):
    killed = Node(workdir)
    killed.start()
    killed.proc.kill()
    killed.proc.wait()
    assert killed.sock.exists(), "SIGKILL left no socket file, so this test replaces nothing"

    successor = Node(workdir, sock=killed.sock)
    successor.start()
    try:
        assert f"removed a stale unix socket at {killed.sock}" in successor.log()

        intruder = Node(workdir, sock=killed.sock)
        run = intruder.refused_start()
        assert run.returncode == 1, run
        assert f"unix socket {killed.sock}: another process listens on it" in run.stderr, run.stderr
        assert greeted_through(successor.sock), "the refused start disturbed the running node"
    finally:
        assert successor.stop() == 0
    assert not killed.sock.exists()


def test_a_file_that_is_not_a_socket_stops_the_start_and_is_kept(workdir):
    path = workdir / "ob.sock"
    path.write_text("somebody's notes\n")
    run = Node(workdir).refused_start()
    assert run.returncode == 1, run
    assert "the path exists and is not a socket" in run.stderr, run.stderr
    assert path.read_text() == "somebody's notes\n"


def test_sigterm_closes_the_socket_and_removes_its_file(workdir):
    node = Node(workdir)
    node.start()
    with OrderbookEngine(unix_socket=str(node.sock)) as client:
        assert client.ping() == "PONG"
    assert node.stop() == 0
    assert not node.sock.exists(), "the socket file outlived the server"
    log = node.log()
    assert "Drain requested: closing the unix socket" in log, log[-3000:]
    assert f"removed unix socket {node.sock}" in log, log[-3000:]


def test_ob_backup_goes_through_the_socket(workdir):
    tool = str(Path(server_binary_path()).resolve().with_name("ob_backup"))
    node = Node(workdir, ["--backup-dir", str(workdir / "backups")])
    node.start()
    try:
        run = subprocess.run([tool, "--unix-socket", str(node.sock)], capture_output=True,
                             text=True, timeout=patience(120))
        assert run.returncode == 0, run.stderr
        name = re.match(r"backup (\S+) done", run.stdout).group(1)
        assert (workdir / "backups" / name / "backup.json").is_file()

        both = subprocess.run([tool, "--unix-socket", str(node.sock), "--port", str(node.port)],
                              capture_output=True, text=True, timeout=patience(30))
        assert both.returncode == 2, both
        assert "--unix-socket replaces --host and --port" in both.stderr, both.stderr

        tls = subprocess.run([tool, "--unix-socket", str(node.sock), "--tls"], capture_output=True,
                             text=True, timeout=patience(30))
        assert tls.returncode == 2, tls
        assert "unix socket has no TLS" in tls.stderr, tls.stderr
    finally:
        node.stop()


def test_the_metric_counts_connections_through_the_socket(workdir):
    metrics_port = free_port()
    node = Node(workdir, ["--metrics-port", str(metrics_port), "--metrics-bind", "127.0.0.1"])
    node.start()
    try:
        before = counter(metrics_port, "ob_unix_connections_total")
        assert before >= 1, "the start's own probe came through the socket"
        for _ in range(3):
            assert greeted_through(node.sock)
        with socket.create_connection(("127.0.0.1", node.port), timeout=5) as s:
            assert s.recv(4096).startswith(b"OK ob_tcp_server")   # TCP: not counted
        assert counter(metrics_port, "ob_unix_connections_total") == before + 3
    finally:
        node.stop()


def test_the_python_client_refuses_what_a_socket_cannot_be():
    """Before any connection: the path is never dialled."""
    with pytest.raises(OrderbookTlsError, match="unix socket has no TLS"):
        OrderbookEngine(unix_socket="/nonexistent/ob.sock", tls=True)
    with pytest.raises(ValueError, match="a pool dials every node at its address"):
        OrderbookEngine(unix_socket="/nonexistent/ob.sock", hosts=["127.0.0.1:1"])
    with pytest.raises(ValueError, match="unix_socket replaces host and port"):
        OrderbookEngine(unix_socket="/nonexistent/ob.sock", host="127.0.0.1")
