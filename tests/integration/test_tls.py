"""TLS on the client port against a live server (#30 part three).

Everything before this test is startup behaviour: contexts that load, refusals that fire. This is
the only part that proves the byte path works, and the case that matters most is the **large
response** — because that is where `SSL_write` returns partial writes and `send_buf_.erase(0, n)`
moves the pending bytes to a different address, which OpenSSL refuses without
`SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER` (measured in `benchmarks/tls/ssl_write_retry.c`).

A response that fits in one record would pass with either mode set or unset, so a small-response
test here would be the kind that looks like coverage and is not.
"""
from __future__ import annotations

import os
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
from conftest import cpp_client_binary_path, patience, server_binary_path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_cert(tmp: Path, san: str = "IP:127.0.0.1", stem: str = "cert") -> tuple[Path, Path]:
    """A self-signed certificate for 127.0.0.1, generated the way operations.md tells an operator.

    `san` is a parameter because the interesting test is a certificate with a *good chain* and the
    wrong name: self-signed means the file is its own trust anchor, so pointing a client at it
    isolates the name check from every question about trust.
    """
    cert, key = tmp / f"{stem}.pem", tmp / f"{stem}-key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-keyout", str(key),
         "-out", str(cert), "-days", "1", "-nodes", "-subj", "/CN=127.0.0.1",
         "-addext", f"subjectAltName={san}"],
        check=True, capture_output=True, timeout=60)
    key.chmod(0o600)
    return cert, key


class TlsNode:
    """One ob_tcp_server with TLS on the client port.

    stderr to a file rather than a pipe: a pipe nobody reads blocks the node at 64 kB (#86), and
    these tests read the log to check what the handshake reported.
    """

    def __init__(self, tmp: Path, *, tls: bool = True, san: str = "IP:127.0.0.1",
                 auth_secret: str | None = None):
        self.tmp = tmp
        self.port = 0
        self.proc: subprocess.Popen | None = None
        self.log_path = tmp / "node.log"
        self.cert, self.key = make_cert(tmp, san)
        self.tls = tls
        self.secret_file: Path | None = None
        if auth_secret is not None:
            self.secret_file = tmp / "client.secret"
            self.secret_file.write_text(f"alice {auth_secret}\n")
            self.secret_file.chmod(0o600)

    def argv(self) -> list[str]:
        args = [SERVER, "--port", str(self.port), "--data-dir", str(self.tmp / "data"),
                "--flush-interval-ms", "600000"]
        if self.tls:
            args += ["--tls-client", "--tls-cert-file", str(self.cert),
                     "--tls-key-file", str(self.key)]
        if self.secret_file is not None:
            args += ["--auth-secret-file", str(self.secret_file)]
        return args

    def start(self, timeout: float = 20.0) -> None:
        timeout = patience(timeout)
        self.port = free_port()
        self.log = open(self.log_path, "a", buffering=1)
        self.proc = subprocess.Popen(self.argv(), stdout=self.log, stderr=self.log)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"node exited {self.proc.returncode}:\n{self.log_text()}")
            try:
                if self.tls:
                    # verify=False for the *readiness* probe, deliberately. It asks whether the
                    # node is up; whether the certificate is the right one is what the tests below
                    # ask. With verification on, a node deliberately issued a certificate for
                    # another address reported "node never answered" while its own log said
                    # `listening` - a probe that answers two questions with one word.
                    with self.connect(verify=False) as s:
                        s.sendall(b"PING\n")
                        if b"PONG" in s.recv(4096):
                            return
                else:
                    with socket.create_connection(("127.0.0.1", self.port), timeout=2) as s:
                        s.settimeout(2)
                        s.recv(4096)
                        s.sendall(b"PING\n")
                        if b"PONG" in s.recv(1024):
                            return
            except (OSError, ssl.SSLError):
                time.sleep(0.2)
        raise RuntimeError(f"node never answered:\n{self.log_text()}")

    def context(self, verify: bool = True) -> ssl.SSLContext:
        ctx = ssl.create_default_context(cafile=str(self.cert))
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        if not verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def connect(self, verify: bool = True, timeout: float = 20.0, rcvbuf: int = 0):
        """A TLS connection. `rcvbuf` shrinks SO_RCVBUF, which is how a partial write is made certain.

        Without it the loopback socket buffer is megabytes, so a response of a few hundred kilobytes
        goes out in one `SSL_write` and the two modes this exercises are never reached — measured:
        both mutations survived a 36 kB response.
        """
        raw = socket.create_connection(("127.0.0.1", self.port), timeout=patience(timeout))
        if rcvbuf:
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        raw.settimeout(patience(timeout))
        s = self.context(verify).wrap_socket(raw, server_hostname="127.0.0.1")
        s.recv(4096)  # banner
        return s

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


def send(sock, text: str, settle: float = 0.4) -> str:
    sock.sendall((text + "\n").encode())
    time.sleep(settle)
    try:
        return sock.recv(1 << 22).decode(errors="replace")
    except (socket.timeout, ssl.SSLError):
        return ""


@pytest.fixture
def tls_node():
    with tempfile.TemporaryDirectory(prefix="ob_tls_") as d:
        node = TlsNode(Path(d))
        node.start()
        try:
            yield node
        finally:
            node.stop()


# ── The handshake and the ordinary path ───────────────────────────────────────

def test_a_verifying_client_completes_the_handshake_and_gets_the_banner(tls_node):
    s = tls_node.connect()
    try:
        assert s.version() == "TLSv1.3", s.version()
        assert "PONG" in send(s, "PING")
    finally:
        s.close()
    log = tls_node.log_text()
    assert "handshake complete" in log, log[-2000:]
    assert "TLSv1.3" in log, log[-2000:]


def test_a_plaintext_client_does_not_get_served(tls_node):
    """A plaintext PING against a TLS port must not answer PONG.

    The point is not that it errors — it is that no command is ever executed. A server that
    answered here would be one where the handshake gate leaked bytes into feed().
    """
    with socket.create_connection(("127.0.0.1", tls_node.port), timeout=5) as raw:
        raw.settimeout(3)
        raw.sendall(b"PING\n")
        try:
            data = raw.recv(4096)
        except socket.timeout:
            data = b""
    assert b"PONG" not in data, data


def test_a_client_refusing_tls_one_two_still_connects(tls_node):
    # The floor is 1.3 on the server, so a client offering only 1.2 must fail rather than be
    # silently downgraded.
    ctx = ssl.create_default_context(cafile=str(tls_node.cert))
    ctx.maximum_version = ssl.TLSVersion.TLSv1_2
    raw = socket.create_connection(("127.0.0.1", tls_node.port), timeout=5)
    with pytest.raises(ssl.SSLError):
        ctx.wrap_socket(raw, server_hostname="127.0.0.1")
    raw.close()


# ── The case the modes exist for ──────────────────────────────────────────────

def test_a_response_far_larger_than_the_socket_buffer_arrives_intact(tls_node):
    """The load-bearing test, and it took two attempts to make it load-bearing.

    Each partial `SSL_write` is followed by `send_buf_.erase(0, n)`, which moves the pending bytes
    to a different address — refused by OpenSSL without `SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER`, and
    stalled outright without `SSL_MODE_ENABLE_PARTIAL_WRITE` (`benchmarks/tls/ssl_write_retry.c`).

    **The first version of this test did not detect either mutation.** It sent 900 rows, about
    36 kB, and a loopback socket buffer is megabytes — so the whole reply went out in one
    `SSL_write` and no retry ever happened. A test that never provokes the condition passes for the
    same reason a correct server does.

    Made deterministic the way #59 did it: a receive buffer shrunk to 4 kB and a reader that takes
    4 kB at a time with a pause, so the server cannot avoid EAGAIN. Asserted on the **content**,
    because the two mutations fail differently — one with an error, one by silently making no
    progress — and only the bytes tell both apart from a working server.
    """
    s = tls_node.connect(rcvbuf=4096)
    try:
        # 20 batches of 1000 levels: about 800 kB of response against a 4 kB window.
        for batch in range(20):
            base = 50_000 + batch * 1000
            prices = "\n".join(f"{base + i}\t10\t1" for i in range(1000))
            reply = send(s, f"MINSERT BTCUSD BINANCE bid 1000\n{prices}", settle=0.3)
            assert "OK" in reply, f"batch {batch}: {reply[:200]}"
        assert "OK" in send(s, "FLUSH", settle=3.0)

        s.sendall(b"SELECT * FROM 'BTCUSD'.'BINANCE'\n")
        body = ""
        deadline = time.time() + patience(120)
        while time.time() < deadline:
            time.sleep(0.01)          # keep the reader slower than the writer
            try:
                chunk = s.recv(4096).decode(errors="replace")
            except (socket.timeout, ssl.SSLError):
                break
            if not chunk:
                break
            body += chunk
            if body.endswith("\n\n"):
                break

        rows = [l for l in body.splitlines() if l.count("\t") >= 6]
        assert body.startswith("OK"), body[:200]
        assert len(rows) >= 20_000, (
            f"expected at least 20000 rows through TLS, got {len(rows)} - a response larger than "
            f"the socket buffer must not truncate or stall")
        # Content, so a truncated or reordered stream fails rather than counting right.
        assert "\t50000\t" in body or "50000\t" in body
        assert "69999\t" in body, "the last price never arrived"
    finally:
        s.close()


def test_the_engine_still_authenticates_underneath_tls(tls_node):
    """TLS does not replace authentication, and part one's gate must still be there.

    Both together is the deployment this is for: TLS gives the channel, AUTH gives the identity.
    """
    # This node runs without --auth-secret-file, so AUTH is refused with auth_disabled - which is
    # the honest answer and proves the gate is still consulted through the encrypted transport.
    s = tls_node.connect()
    try:
        assert "ERR auth_disabled" in send(s, "AUTH")
    finally:
        s.close()


# ── The clients (#30 part three, series C) ────────────────────────────────────
#
# Everything above drives the server with a raw `ssl` socket, which proves the server. These prove
# the two shipped clients, and the case worth the most is a certificate with a **good chain and the
# wrong name**: `SSL_VERIFY_PEER` accepts it, so a private CA signing a cluster would make every
# node's certificate good for every other one, and the verification would read as done.

@pytest.fixture
def wrong_name_node():
    """A node whose certificate is signed for 10.0.0.2 and served on 127.0.0.1."""
    with tempfile.TemporaryDirectory(prefix="ob_tls_wn_") as d:
        node = TlsNode(Path(d), san="IP:10.0.0.2")
        node.start()
        try:
            yield node
        finally:
            node.stop()


@pytest.fixture
def tls_authed_node():
    with tempfile.TemporaryDirectory(prefix="ob_tls_auth_") as d:
        node = TlsNode(Path(d), auth_secret="0123456789abcdef0123456789abcdef-alice")
        node.start()
        try:
            yield node
        finally:
            node.stop()


def test_the_python_client_round_trips_a_row_over_tls(tls_node):
    from orderbook_engine import OrderbookEngine

    eng = OrderbookEngine(host="127.0.0.1", port=tls_node.port,
                          tls=True, tls_ca_file=str(tls_node.cert), timeout=patience(20.0))
    try:
        # A row, not a successful connect: a negative assertion about one error string passes for a
        # dozen wrong reasons, and so does a connect that never carries application bytes.
        eng.insert("BTC-USD", "BINANCE", "bid", [100_500], [3])
        eng.flush()
        rows = eng.query("SELECT * FROM 'BTC-USD'.'BINANCE'")
        assert len(rows) == 1, rows
        assert rows[0].price == 100_500 and rows[0].quantity == 3, rows[0]
    finally:
        eng.close()


def test_the_python_client_refuses_a_certificate_for_another_address(wrong_name_node):
    from orderbook_engine import OrderbookTlsError

    from orderbook_engine import OrderbookEngine

    # The chain is perfect - this is the exact certificate handed to the client as its trust anchor.
    # Only the name is wrong. Remove `server_hostname=` from the wrap and this passes.
    with pytest.raises(OrderbookTlsError) as caught:
        OrderbookEngine(host="127.0.0.1", port=wrong_name_node.port,
                        tls=True, tls_ca_file=str(wrong_name_node.cert), timeout=patience(20.0))
    assert "hostname" in str(caught.value).lower() or "match" in str(caught.value).lower(), \
        f"failed for some other reason: {caught.value}"


def test_the_python_client_refuses_a_certificate_its_ca_did_not_sign(tls_node, tmp_path):
    from orderbook_engine import OrderbookEngine, OrderbookTlsError

    stranger, _ = make_cert(tmp_path, "IP:127.0.0.1", stem="stranger")
    assert stranger.read_text() != tls_node.cert.read_text()
    with pytest.raises(OrderbookTlsError) as caught:
        OrderbookEngine(host="127.0.0.1", port=tls_node.port,
                        tls=True, tls_ca_file=str(stranger), timeout=patience(20.0))
    assert "verify" in str(caught.value).lower(), caught.value


def test_a_python_client_without_tls_waits_out_its_timeout_against_a_tls_port(tls_node):
    """The reverse misconfiguration, and it fails by *timeout* rather than by refusal.

    Measured, and the name says so because the behaviour is worth knowing before an operator meets
    it: this client speaks second - it waits for the banner - and the server speaks second too, it
    waits for a ClientHello. Neither sends anything, so the connection sits there until the
    client's own timeout expires. Nothing can shorten it: until a byte arrives the server cannot
    tell a plaintext client from a slow one.

    So the symptom of a forgotten `tls=True` is a hang for `timeout` seconds and then an error,
    while the server's log records nothing at all. The forgotten-`--tls-client` direction fails
    immediately (`test_a_plaintext_server_is_refused...` in the unit tests) because there the
    plaintext banner arrives where a ServerHello was expected.
    """
    from orderbook_engine import OrderbookEngine, OrderbookError

    budget = patience(3.0)
    started = time.time()
    with pytest.raises((OrderbookError, OSError)):
        OrderbookEngine(host="127.0.0.1", port=tls_node.port, timeout=budget)
    waited = time.time() - started
    assert waited >= budget * 0.5, (
        f"failed after {waited:.2f}s, faster than the {budget}s timeout - if the server learned to "
        "refuse this eagerly, that is better news than this test and the docstring is now wrong")
    # And nothing was served: the node must not have accepted a plaintext session behind our back.
    assert "PONG" not in tls_node.log_text()


def test_tls_and_authentication_compose_through_the_python_client(tls_authed_node):
    from orderbook_engine import OrderbookEngine

    # The deployment this item is for: TLS gives the channel, AUTH gives the identity. Neither
    # replaces the other, and the proof is a row rather than a connection.
    eng = OrderbookEngine(host="127.0.0.1", port=tls_authed_node.port,
                          tls=True, tls_ca_file=str(tls_authed_node.cert),
                          auth=("alice", "0123456789abcdef0123456789abcdef-alice"),
                          timeout=patience(20.0))
    try:
        eng.insert("ETH-USD", "BINANCE", "ask", [4_200], [7])
        eng.flush()
        rows = eng.query("SELECT * FROM 'ETH-USD'.'BINANCE'")
        assert len(rows) == 1 and rows[0].quantity == 7, rows
    finally:
        eng.close()


def test_the_python_client_still_refuses_an_unauthenticated_command_under_tls(tls_authed_node):
    from orderbook_engine import OrderbookEngine, OrderbookError

    # Encryption is not an identity. Without credentials the gate from part one must still refuse,
    # which is the assertion that stops TLS from being read as authentication.
    eng = OrderbookEngine(host="127.0.0.1", port=tls_authed_node.port,
                          tls=True, tls_ca_file=str(tls_authed_node.cert), timeout=patience(20.0))
    try:
        with pytest.raises(OrderbookError) as caught:
            eng.insert("BTC-USD", "BINANCE", "bid", [1], [1])
        assert "unauthenticated" in str(caught.value), caught.value
    finally:
        eng.close()


def test_the_cpp_client_round_trips_over_tls(tls_node):
    found = cpp_client_binary_path()
    assert found is not None, ("ob_integration_test not built; build that target in the same tree "
                               "as the server under test")
    env = dict(os.environ, OB_TLS="1", OB_TLS_CA_FILE=str(tls_node.cert))
    done = subprocess.run([found, "--host", "127.0.0.1", "--port", str(tls_node.port),
                           "--test", "insert_query"],
                          capture_output=True, text=True, timeout=patience(90.0), env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert '"pass"' in done.stdout, done.stdout


def test_the_cpp_client_refuses_a_certificate_for_another_address(wrong_name_node):
    found = cpp_client_binary_path()
    assert found is not None, "ob_integration_test not built"
    env = dict(os.environ, OB_TLS="1", OB_TLS_CA_FILE=str(wrong_name_node.cert))
    done = subprocess.run([found, "--host", "127.0.0.1", "--port", str(wrong_name_node.port),
                           "--test", "ping"],
                          capture_output=True, text=True, timeout=patience(90.0), env=env)
    assert done.returncode != 0, "a certificate for 10.0.0.2 was accepted for 127.0.0.1: " + \
                                 done.stdout + done.stderr
