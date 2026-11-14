"""#265: the client port's TLS handshakes on their own threads, off the client loops, on a live node.

A client loop drove each TLS session's handshake itself, between the events of the other sessions it
serves: two processes opening connections back to back raised the median PING of a session on the
same loop 31-36 times with an RSA-2048 certificate (evidence/2026-11-14-tls-handshake-probe/). The
handshakes run on --tls-handshake-threads now, and a loop is given a session whose handshake is done.

One client loop (`--io-threads 1`), so every session is on it; the PINGs go through the node's Unix
socket, which is plaintext and served by the same loop. The control is the same node with
`--tls-handshake-threads 0`, as every node was before #265.
"""
from __future__ import annotations

import json
import re
import shutil
import signal
import socket
import ssl
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

from conftest import free_port, instrumented_run, patience, server_binary_path

pytestmark = pytest.mark.smoke

# A PING every half millisecond through the Unix socket, until a line comes on stdin; then each
# PING's send time and round trip, in nanoseconds of the monotonic clock every process reads.
PINGER = r"""
import json, select, socket, sys, time
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.settimeout(float(sys.argv[2]))
s.connect(sys.argv[1])
buf = b""
while b"\n\n" not in buf:
    buf += s.recv(65536)
print("ready", flush=True)
samples = []
while not select.select([sys.stdin], [], [], 0)[0]:
    sent = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    s.sendall(b"PING\n")
    got = b""
    while not got.endswith(b"\n"):
        got += s.recv(64)
    samples.append((sent, time.clock_gettime_ns(time.CLOCK_MONOTONIC) - sent))
    time.sleep(0.0005)
print(json.dumps(samples), flush=True)
"""

# TLS connections back to back until the given monotonic time: each a full handshake and the banner.
STORM = r"""
import socket, ssl, sys, time
port, end = int(sys.argv[1]), int(sys.argv[2])
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
made = 0
while time.clock_gettime_ns(time.CLOCK_MONOTONIC) < end:
    s = ctx.wrap_socket(socket.create_connection(("127.0.0.1", port), timeout=30))
    buf = b""
    while b"\n\n" not in buf:
        chunk = s.recv(65536)
        if not chunk:
            break
        buf += chunk
    s.close()
    made += 1
print(made, flush=True)
"""


def make_cert(directory: Path, stem: str) -> tuple[Path, Path]:
    cert, key = directory / f"{stem}.pem", directory / f"{stem}.key"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-keyout", str(key),
                    "-out", str(cert), "-days", "1", "-nodes", "-subj", "/CN=127.0.0.1",
                    "-addext", "subjectAltName=IP:127.0.0.1"],
                   check=True, capture_output=True, timeout=patience(60))
    key.chmod(0o600)
    return cert, key


def tls_client() -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class Node:
    def __init__(self, tmp: Path, *extra: str) -> None:
        self.tmp = tmp
        self.port = free_port()
        self.metrics_port = free_port()
        self.unix = tmp / "ob.sock"
        self.cert, self.key = make_cert(tmp, "node")
        self.log_path = tmp / "node.log"
        self.log = open(self.log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port), "--data-dir", str(tmp / "data"),
             "--metrics-port", str(self.metrics_port), "--io-threads", "1",
             "--unix-socket", str(self.unix), "--tls-client", "--tls-cert-file", str(self.cert),
             "--tls-key-file", str(self.key), *extra],
            stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + patience(30)
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"the node exited with {self.proc.returncode}")
            if self.unix.exists():
                try:
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                        s.settimeout(1)
                        s.connect(str(self.unix))
                        return
                except OSError:
                    pass
            time.sleep(0.1)
        raise AssertionError("the node never accepted a connection")

    def metric(self, name: str) -> float:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.metrics_port}/metrics",
                                    timeout=patience(10)) as resp:
            body = resp.read().decode(errors="replace")
        match = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)$", body, re.M)
        assert match, f"{name} is not exported"
        return float(match.group(1))

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=patience(30))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()


def tls_session(port: int) -> ssl.SSLSocket:
    s = tls_client().wrap_socket(socket.create_connection(("127.0.0.1", port), timeout=patience(30)))
    return s


def ping_median_beside_a_storm(node: Node, seconds: float = 2.0, workers: int = 2) -> tuple[float, int]:
    """The median round trip, in seconds, of the PINGs sent while `workers` processes open TLS
    connections to `node` back to back for `seconds`; and the handshakes they made."""
    pinger = subprocess.Popen([sys.executable, "-c", PINGER, str(node.unix), str(patience(60))],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert pinger.stdout is not None
        assert pinger.stdout.readline().strip() == "ready", "the pinger did not connect"
        clock = time.CLOCK_MONOTONIC
        began = time.clock_gettime_ns(clock)
        end = began + int(seconds * 1e9)
        storm = [subprocess.Popen([sys.executable, "-c", STORM, str(node.port), str(end)],
                                  stdout=subprocess.PIPE, text=True) for _ in range(workers)]
        made = sum(int(w.communicate(timeout=patience(60) + seconds)[0].strip() or 0) for w in storm)
        out, _ = pinger.communicate("stop\n", timeout=patience(60))
    finally:
        if pinger.poll() is None:
            pinger.kill()
            pinger.wait()
    during = [rtt for sent, rtt in json.loads(out) if began <= sent <= end]
    assert during, "no PING was sent while the handshakes went on"
    return statistics.median(during) / 1e9, made


@pytest.fixture(scope="module")
def nodes(tmp_path_factory):
    if shutil.which("openssl") is None:
        pytest.skip("the openssl tool makes the certificates")
    off = Node(tmp_path_factory.mktemp("tls_off_the_loop"))
    on = Node(tmp_path_factory.mktemp("tls_on_the_loop"), "--tls-handshake-threads", "0")
    try:
        yield off, on
    finally:
        off.stop()
        on.stop()


def test_a_handshake_storm_does_not_hold_the_sessions_beside_it(nodes):
    off, on = nodes
    held, made_on = ping_median_beside_a_storm(on)
    before = off.metric("ob_tls_handshakes_total")
    beside, made_off = ping_median_beside_a_storm(off)
    assert made_on > 0 and made_off > 0, "the storm made no handshake"
    # On the loop a PING waits behind the handshakes; off it, for a fraction of that: under a twentieth
    # in #265's measurement (Release builds, RSA-2048), and a quarter is asked. Under a sanitizer the
    # server's own code, a PING's path with it, runs several times slower while the system's OpenSSL,
    # which is not instrumented, signs as fast, so half is asked there - the scaling patience() does
    # for the waits, applied to a ratio.
    fraction = 2 if instrumented_run() else 4
    assert beside < held / fraction, (f"the median PING beside the handshakes was {beside * 1e6:.0f} us, "
                                      f"{held * 1e6:.0f} us with them on the loop")
    assert off.metric("ob_tls_handshakes_total") >= before + made_off
    assert off.metric("ob_tls_handshakes_in_flight") == 0
    assert on.metric("ob_tls_handshakes_total") == 0, "a handshake left a loop with no threads"


def test_a_command_sent_right_after_the_handshake_is_answered(nodes):
    off, _ = nodes
    s = tls_session(off.port)
    try:
        # Before reading anything: what the client sends behind its Finished may reach the node while
        # the session is handed from the handshake thread to its loop.
        s.sendall(b"PING\n")
        buf = b""
        deadline = time.monotonic() + patience(10)
        while b"PONG\n" not in buf:
            assert time.monotonic() < deadline, f"no PONG: {buf[:80]!r}"
            chunk = s.recv(4096)
            assert chunk, f"the session was closed: {buf[:80]!r}"
            buf += chunk
        assert buf.startswith(b"OK ob_tcp_server"), buf[:40]
    finally:
        s.close()


def test_a_connection_that_never_starts_its_handshake_is_closed_at_its_deadline(tmp_path):
    if shutil.which("openssl") is None:
        pytest.skip("the openssl tool makes the certificates")
    node = Node(tmp_path, "--tls-handshake-timeout-ms", "500")
    try:
        sessions = node.metric("ob_active_sessions")
        raw = socket.create_connection(("127.0.0.1", node.port), timeout=patience(10))
        started = time.monotonic()
        try:
            got = raw.recv(64)
        finally:
            raw.close()
        waited = time.monotonic() - started
        assert got == b"", f"a connection that sent nothing was sent {got!r}"
        assert waited >= 0.45, f"closed after {waited:.2f} s, before its 0.5 s"
        deadline = time.monotonic() + patience(10)
        while node.metric("ob_active_sessions") != sessions:
            assert time.monotonic() < deadline, "its place in --max-sessions was not given back"
            time.sleep(0.05)
        assert node.metric("ob_tls_handshake_timeouts_total") == 1
    finally:
        node.stop()


def test_a_certificate_replaced_on_sighup_is_the_one_the_next_handshake_presents(tmp_path):
    if shutil.which("openssl") is None:
        pytest.skip("the openssl tool makes the certificates")
    node = Node(tmp_path)
    try:
        def presented() -> bytes:
            s = tls_session(node.port)
            try:
                return s.getpeercert(binary_form=True)
            finally:
                s.close()

        first = presented()
        reloads = node.metric("ob_tls_reloads_total")
        replacement_cert, replacement_key = make_cert(tmp_path, "replacement")
        shutil.copyfile(replacement_key, node.key)
        shutil.copyfile(replacement_cert, node.cert)
        node.proc.send_signal(signal.SIGHUP)
        deadline = time.monotonic() + patience(10)
        while node.metric("ob_tls_reloads_total") < reloads + 1:
            assert time.monotonic() < deadline, "the reload never happened"
            time.sleep(0.05)
        second = presented()
        assert second != first, "the handshake after the reload presented the certificate before it"
        assert second == ssl.PEM_cert_to_DER_cert(replacement_cert.read_text())
    finally:
        node.stop()
