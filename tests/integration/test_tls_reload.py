"""The certificate read again on SIGHUP, against a live node (#238).

A renewal is a file replaced under a running node and a signal. What it has to do is about
connections, so every claim here is read off a handshake: a connection opened after the reload is
shown the new certificate, on the client port and on the replication link alike, and one opened
before it goes on working. A renewal that goes wrong - a key that does not match - leaves the old
certificate in force and the node up, and says so in its log and its metrics. A node without TLS,
which until #238 died of the signal's default action, logs that there is nothing to reload.
"""
from __future__ import annotations

import shutil
import signal
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import pytest
from conftest import free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()


class Pki:
    """A private CA and the certificates it signs, the way `docs/operations.md` makes them."""

    def __init__(self, d: Path):
        d.mkdir(parents=True, exist_ok=True)
        self.d = d
        self.ca = d / "ca.pem"
        self.ca_key = d / "ca-key.pem"
        # keyUsage on the CA and an authority key identifier on what it signs, because Python's
        # default context verifies strictly since 3.13 and refuses a chain without them.
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(self.ca_key),
             "-out", str(self.ca), "-days", "1", "-subj", "/CN=orderbook reload CA",
             "-addext", "keyUsage=critical,keyCertSign,cRLSign"],
            check=True, capture_output=True, timeout=120)

    def sign(self, cn: str, days: int) -> tuple[Path, Path]:
        """A certificate for 127.0.0.1 named `cn`, valid for `days`: what a renewal changes."""
        cert, key, csr, ext = (self.d / f"{cn}.pem", self.d / f"{cn}-key.pem",
                               self.d / f"{cn}.csr", self.d / f"{cn}.ext")
        ext.write_text("subjectAltName=IP:127.0.0.1\nauthorityKeyIdentifier=keyid\n")
        subprocess.run(
            ["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key),
             "-out", str(csr), "-subj", f"/CN={cn}"],
            check=True, capture_output=True, timeout=120)
        subprocess.run(
            ["openssl", "x509", "-req", "-in", str(csr), "-CA", str(self.ca), "-CAkey",
             str(self.ca_key), "-CAcreateserial", "-out", str(cert), "-days", str(days),
             "-extfile", str(ext)],
            check=True, capture_output=True, timeout=120)
        key.chmod(0o600)
        return cert, key


def not_after(cert: Path) -> int:
    """The certificate's end of validity as Unix time, read by openssl rather than by the engine."""
    out = subprocess.run(["openssl", "x509", "-in", str(cert), "-noout", "-enddate"],
                         check=True, capture_output=True, text=True, timeout=30).stdout
    return int(ssl.cert_time_to_seconds(out.strip().split("=", 1)[1]))


class Node:
    """One ob_tcp_server reading its certificate from paths a test replaces under it.

    stderr to a file rather than a pipe: a pipe nobody reads blocks the node at 64 kB (#86), and
    these tests read the log for what the reload said.
    """

    def __init__(self, tmp: Path, *, pki: Pki | None, replication: bool = False):
        self.tmp = tmp
        self.pki = pki
        self.port = free_port()
        self.metrics_port = free_port()
        self.replication_port = free_port() if replication else 0
        self.cert = tmp / "node.pem"
        self.key = tmp / "node-key.pem"
        self.log_path = tmp / "node.log"
        self.proc: subprocess.Popen | None = None

    def install(self, cert: Path, key: Path) -> None:
        """Put a certificate and its key where the node reads them: what a renewal does."""
        shutil.copyfile(cert, self.cert)
        shutil.copyfile(key, self.key)
        self.key.chmod(0o600)

    def start(self) -> None:
        args = [SERVER, "--port", str(self.port), "--data-dir", str(self.tmp / "data"),
                "--metrics-port", str(self.metrics_port), "--flush-interval-ms", "600000"]
        if self.pki is not None:
            args += ["--tls-client", "--tls-cert-file", str(self.cert),
                     "--tls-key-file", str(self.key), "--tls-ca-file", str(self.pki.ca)]
        if self.replication_port:
            args += ["--replication-port", str(self.replication_port)]
            if self.pki is not None:
                args.append("--tls-replication")
        self.log = open(self.log_path, "a", buffering=1)
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        self.wait_for_log("listening on port", what="the node to listen")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        if getattr(self, "log", None):
            self.log.close()

    def log_text(self) -> str:
        return self.log_path.read_text(errors="replace") if self.log_path.exists() else ""

    def wait_for_log(self, needle: str, *, what: str, count: int = 1) -> None:
        """Until `needle` is in the log `count` times; bounded, and a node that exited ends it."""
        deadline = time.monotonic() + patience(20)
        while time.monotonic() < deadline:
            if self.log_text().count(needle) >= count:
                return
            if self.proc is not None and self.proc.poll() is not None:
                raise AssertionError(f"node exited {self.proc.returncode} waiting for {what}:\n"
                                     f"{self.log_text()[-3000:]}")
            time.sleep(0.1)
        raise AssertionError(f"timed out waiting for {what}:\n{self.log_text()[-3000:]}")

    def reload(self, outcome: str, count: int) -> None:
        """SIGHUP, and the `count`-th line saying `outcome`: the monitor polls every 100 ms."""
        assert self.proc is not None
        self.proc.send_signal(signal.SIGHUP)
        self.wait_for_log(outcome, what=f"'{outcome}' after SIGHUP", count=count)

    def metrics(self) -> dict[str, float]:
        """/metrics by bare name: the exposition carries a label set on every line (pitfall 66)."""
        with urllib.request.urlopen(f"http://127.0.0.1:{self.metrics_port}/metrics",
                                    timeout=5) as r:
            body = r.read().decode()
        out: dict[str, float] = {}
        for line in body.splitlines():
            if line and not line.startswith("#"):
                name, _, value = line.partition(" ")
                out[name.split("{", 1)[0]] = float(value)
        return out

    def client_port(self) -> ssl.SSLSocket:
        """A verifying TLS client on the client port, past the banner."""
        assert self.pki is not None
        ctx = ssl.create_default_context(cafile=str(self.pki.ca))
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        raw = socket.create_connection(("127.0.0.1", self.port), timeout=patience(10))
        s = ctx.wrap_socket(raw, server_hostname="127.0.0.1")
        s.recv(4096)
        return s

    def replication_link(self, peer: tuple[Path, Path]) -> ssl.SSLSocket:
        """A handshake on the replication port as a node with `peer`'s certificate: the link is
        mutual, so the port asks for one, and the handshake is all this needs of it."""
        assert self.pki is not None
        ctx = ssl.create_default_context(cafile=str(self.pki.ca))
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.load_cert_chain(str(peer[0]), str(peer[1]))
        raw = socket.create_connection(("127.0.0.1", self.replication_port), timeout=patience(10))
        return ctx.wrap_socket(raw, server_hostname="127.0.0.1")


def common_name(s: ssl.SSLSocket) -> str:
    cert = s.getpeercert()
    assert cert, "the server presented no certificate"
    return dict(x[0] for x in cert["subject"])["commonName"]


def ping(s: ssl.SSLSocket) -> str:
    s.sendall(b"PING\n")
    s.settimeout(patience(5))
    return s.recv(4096).decode(errors="replace")


@pytest.fixture
def tmp():
    with tempfile.TemporaryDirectory(prefix="ob_tlsr_") as d:
        yield Path(d)


@pytest.fixture
def renewal(tmp):
    """A node with TLS on the client port and the replication link, on certificate A, with B - a
    renewal valid for longer - signed by the same CA and waiting beside it."""
    pki = Pki(tmp / "pki")
    a = pki.sign("reload-a", 1)
    b = pki.sign("reload-b", 30)
    peer = pki.sign("reload-peer", 1)
    node = Node(tmp, pki=pki, replication=True)
    node.install(*a)
    node.start()
    try:
        yield node, a, b, peer
    finally:
        node.stop()


def test_a_new_connection_is_shown_the_new_certificate_and_an_open_one_goes_on(renewal) -> None:
    node, a, b, peer = renewal
    before = node.client_port()
    assert common_name(before) == "reload-a"
    assert node.metrics()["ob_tls_certificate_not_after_seconds"] == not_after(a[0])

    node.install(*b)
    node.reload("TLS reloaded", count=1)

    after = node.client_port()
    assert common_name(after) == "reload-b"
    link = node.replication_link(peer)
    assert common_name(link) == "reload-b", "every surface reloads, the node links too"
    link.close()
    assert "PONG" in ping(after)
    # Opened on A, and still served: a reload touches no connection already open.
    assert "PONG" in ping(before)
    assert common_name(before) == "reload-a"
    before.close()
    after.close()

    m = node.metrics()
    assert m["ob_tls_reloads_total"] == 1
    assert m["ob_tls_reload_failures_total"] == 0
    assert m["ob_tls_certificate_not_after_seconds"] == not_after(b[0])
    assert "subject /CN=reload-b, valid until" in node.log_text()


def test_files_it_refuses_leave_the_old_certificate_in_force_and_the_node_up(renewal) -> None:
    node, a, b, peer = renewal
    # Half a renewal: B's certificate, A's key.
    shutil.copyfile(b[0], node.cert)
    node.reload("TLS reload refused", count=1)

    assert node.proc is not None and node.proc.poll() is None, "a bad renewal stopped the node"
    log = node.log_text()
    assert "does not match" in log or "key values mismatch" in log, log[-2000:]
    s = node.client_port()
    assert common_name(s) == "reload-a"
    assert "PONG" in ping(s)
    s.close()
    link = node.replication_link(peer)
    assert common_name(link) == "reload-a"
    link.close()

    m = node.metrics()
    assert m["ob_tls_reload_failures_total"] == 1
    assert m["ob_tls_reloads_total"] == 0
    assert m["ob_tls_certificate_not_after_seconds"] == not_after(a[0])

    # The renewal finished: the same signal now takes it.
    node.install(*b)
    node.reload("TLS reloaded", count=1)
    s = node.client_port()
    assert common_name(s) == "reload-b"
    s.close()


def test_a_node_without_tls_lives_through_sighup(tmp) -> None:
    """Until #238 nothing handled SIGHUP and its default action ended the process."""
    node = Node(tmp, pki=None)
    node.start()
    try:
        node.reload("nothing to reload", count=1)
        node.reload("nothing to reload", count=2)
        assert node.proc is not None and node.proc.poll() is None
        with socket.create_connection(("127.0.0.1", node.port), timeout=patience(5)) as s:
            s.settimeout(patience(5))
            s.recv(4096)
            s.sendall(b"PING\n")
            assert b"PONG" in s.recv(1024)
        m = node.metrics()
        assert m["ob_tls_reloads_total"] == 0
        assert m["ob_tls_certificate_not_after_seconds"] == 0
    finally:
        node.stop()
    # And SIGHUP did not replace the ordinary stop: SIGTERM still drains and exits 0.
    assert node.proc is not None and node.proc.returncode == 0, node.log_text()[-2000:]


def test_sighup_reaches_the_signal_thread_and_no_other(tmp) -> None:
    """A signal sent to a process runs its handler on any thread that does not block it, and the
    call that thread was in - `epoll_wait`, a `recv` with a receive timeout - fails with EINTR. The
    replica's stream reader counts that as a disconnect. So every thread blocks SIGHUP but the one
    that handles it, which `/proc` shows per thread: SigBlk, bit 0 for signal 1."""
    node = Node(tmp, pki=None)
    node.start()
    try:
        assert node.proc is not None
        open_to_sighup, threads = [], []
        for task in Path(f"/proc/{node.proc.pid}/task").iterdir():
            try:
                status = (task / "status").read_text()
            except FileNotFoundError:   # a thread that ended between the listing and the read
                continue
            fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
            threads.append(fields["Name"].strip())
            if not int(fields["SigBlk"].strip(), 16) & 1:
                open_to_sighup.append(fields["Name"].strip())
        assert len(threads) > 2, threads
        assert open_to_sighup == ["ob-signals"], f"open to SIGHUP: {open_to_sighup} of {threads}"
        status = Path(f"/proc/{node.proc.pid}/status").read_text()
        caught = next(line for line in status.splitlines() if line.startswith("SigCgt:"))
        assert int(caught.split(":", 1)[1].strip(), 16) & 1, "SIGHUP has no handler"
    finally:
        node.stop()
