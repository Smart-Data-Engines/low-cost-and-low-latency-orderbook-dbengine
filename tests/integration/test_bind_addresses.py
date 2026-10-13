"""#203: the address each listener binds, read from the kernel rather than from the node's log.

The client port, the replication port and the multi-master port listened on every interface, and
only the metrics endpoint could be told otherwise (`--metrics-bind`): a deployment that wanted its
client port on loopback needed a firewall rule - reported from a host where the port, TLS and
authentication on it, was reachable from outside. `--bind`, `--replication-bind` and `--mm-bind`
name the address, and these tests read where each socket listens out of `/proc/net/tcp` and
`/proc/net/tcp6`.

A port with no bind flag listens on `::`, both families on one socket, since #236; `0.0.0.0` is how
an operator keeps it to IPv4.
"""
from __future__ import annotations

import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from conftest import free_port, has_ipv6_loopback, patience, server_binary_path

pytestmark = pytest.mark.smoke


# Every interface, as a port with no bind flag listens on it: both families on `::`, or `0.0.0.0` on
# a host without IPv6.
EVERY_INTERFACE = "::" if has_ipv6_loopback() else "0.0.0.0"


def _words_in_host_order(raw: bytes) -> bytes:
    """/proc/net/tcp{,6} print an address as 32-bit words, each in the host's byte order."""
    if sys.byteorder == "big":
        return raw
    return b"".join(raw[i:i + 4][::-1] for i in range(0, len(raw), 4))


def listening_address(port: int) -> str | None:
    """The address a socket listens on for `port`, from /proc/net/tcp and /proc/net/tcp6, or None."""
    for table, family in (("/proc/net/tcp", socket.AF_INET), ("/proc/net/tcp6", socket.AF_INET6)):
        path = Path(table)
        if not path.exists():
            continue
        for line in path.read_text().splitlines()[1:]:
            fields = line.split()
            local, state = fields[1], fields[3]
            if state != "0A":   # TCP_LISTEN
                continue
            addr_hex, port_hex = local.rsplit(":", 1)
            if int(port_hex, 16) != port:
                continue
            return socket.inet_ntop(family, _words_in_host_order(bytes.fromhex(addr_hex)))
    return None


def answers(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def start_node(tmp_path: Path, extra: list[str], port: int) -> subprocess.Popen:
    proc = subprocess.Popen([server_binary_path(), "--port", str(port), "--data-dir", str(tmp_path / "data"),
                             "--metrics-port", "0", *extra],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + patience(20)
    while time.monotonic() < deadline:
        if listening_address(port) is not None:
            return proc
        if proc.poll() is not None:
            raise AssertionError(f"the node exited with {proc.returncode} before listening")
        time.sleep(0.1)
    proc.kill()
    raise AssertionError(f"nothing listened on port {port} within {patience(20):.0f} s")


def stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=patience(20))
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def test_the_client_port_listens_on_the_address_bind_names(tmp_path):
    port = free_port()
    proc = start_node(tmp_path, ["--bind", "127.0.0.1"], port)
    try:
        assert listening_address(port) == "127.0.0.1"
    finally:
        stop(proc)


def test_without_bind_the_client_port_listens_on_every_interface(tmp_path):
    """The control, and the default every deployment runs on: both families, on one socket."""
    port = free_port()
    proc = start_node(tmp_path, [], port)
    try:
        assert listening_address(port) == EVERY_INTERFACE
        assert answers("127.0.0.1", port), "a port with no bind did not take IPv4"
    finally:
        stop(proc)


def test_bind_to_every_ipv4_interface_keeps_the_port_off_ipv6(tmp_path):
    """The way back to the listener a port had before #236, for a host whose firewall covers IPv4
    alone: `--bind 0.0.0.0` is every IPv4 interface and nothing else."""
    port = free_port()
    proc = start_node(tmp_path, ["--bind", "0.0.0.0"], port)
    try:
        assert listening_address(port) == "0.0.0.0"
        if has_ipv6_loopback():
            assert not answers("::1", port), "a port bound to 0.0.0.0 took an IPv6 connection"
    finally:
        stop(proc)


def test_the_replication_port_listens_on_the_address_replication_bind_names(tmp_path):
    port, repl = free_port(), free_port()
    proc = start_node(tmp_path, ["--replication-port", str(repl), "--replication-bind", "127.0.0.1"], port)
    try:
        deadline = time.monotonic() + patience(10)
        while listening_address(repl) is None and time.monotonic() < deadline:
            time.sleep(0.1)
        assert listening_address(repl) == "127.0.0.1"
        assert listening_address(port) == EVERY_INTERFACE, "--replication-bind moved the client port too"
    finally:
        stop(proc)


def test_the_mesh_port_listens_on_the_address_mm_bind_names(tmp_path):
    port, mesh = free_port(), free_port()
    # A mesh node needs a coordinator to be named, not to be reached, to open its port: one where
    # nothing listens keeps this test free of etcd.
    nobody = f"http://127.0.0.1:{free_port()}"
    proc = start_node(tmp_path, ["--multi-master", "--mm-node-id", "1", "--mm-replication-port", str(mesh),
                                 "--mm-bind", "127.0.0.1", "--coordinator-endpoints", nobody], port)
    try:
        deadline = time.monotonic() + patience(10)
        while listening_address(mesh) is None and time.monotonic() < deadline:
            time.sleep(0.1)
        assert listening_address(mesh) == "127.0.0.1"
    finally:
        stop(proc)


def test_an_address_that_does_not_parse_is_a_refusal_to_start(tmp_path):
    result = subprocess.run([server_binary_path(), "--port", str(free_port()), "--data-dir",
                             str(tmp_path / "data"), "--bind", "127.0.0.256"],
                            capture_output=True, text=True, timeout=patience(20))
    assert result.returncode == 1, result
    assert "--bind expects an IPv4 or IPv6 address" in result.stderr, result.stderr
