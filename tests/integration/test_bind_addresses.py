"""#203: the address each listener binds, read from the kernel rather than from the node's log.

The client port, the replication port and the multi-master port listened on every interface, and
only the metrics endpoint could be told otherwise (`--metrics-bind`): a deployment that wanted its
client port on loopback needed a firewall rule - reported from a host where the port, TLS and
authentication on it, was reachable from outside. `--bind`, `--replication-bind` and `--mm-bind`
name the address, and these tests read where each socket listens out of `/proc/net/tcp` and
`/proc/net/tcp6`.

A port with no bind flag listens on `::`, both families on one socket, since #236; `0.0.0.0` is how
an operator keeps it to IPv4.

Since #256 that is so only where the surface authenticates: with no secret and no bind flag a port
listens on `127.0.0.1`, as PostgreSQL, MongoDB, ClickHouse and Redis do until told otherwise - and a
node announced through a coordinator at another host, with a port it needs on loopback by that rule,
does not start.
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


def secrets(tmp_path: Path) -> tuple[Path, Path]:
    """A client secret file and a cluster secret file, mode 600, the two secrets different."""
    clients = tmp_path / "clients"
    clients.write_text("ops " + secrets_hex(1) + "\n")
    clients.chmod(0o600)
    cluster = tmp_path / "cluster"
    cluster.write_text(secrets_hex(2) + "\n")
    cluster.chmod(0o600)
    return clients, cluster


def secrets_hex(seed: int) -> str:
    return (f"{seed:02x}" * 32)[:64]


def address_off_loopback() -> str | None:
    """An IPv4 address of this host that is not loopback - the one a route out would leave from -
    or None on a host with no route. A UDP socket's connect() sends nothing."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("192.0.2.1", 9))   # TEST-NET-1: never answered, and nothing is sent
            address = s.getsockname()[0]
    except OSError:
        return None
    return None if address.startswith("127.") or address == "0.0.0.0" else address


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


def test_without_authentication_or_bind_the_client_port_listens_on_this_host_only(tmp_path):
    """#256: the package's configuration has no secret and no bind, and it used to listen
    everywhere. Read from the kernel, and asked at this host's address off loopback where it has one."""
    port = free_port()
    proc = start_node(tmp_path, [], port)
    try:
        assert listening_address(port) == "127.0.0.1"
        assert answers("127.0.0.1", port), "a node with no bind did not take a client on this host"
        elsewhere = address_off_loopback()
        if elsewhere is not None:
            assert not answers(elsewhere, port), f"a node with no authentication took a client at {elsewhere}"
    finally:
        stop(proc)


def test_with_authentication_and_without_bind_the_client_port_listens_on_every_interface(tmp_path):
    """The control: a port that authenticates listens where it did, both families on one socket."""
    clients, _ = secrets(tmp_path)
    port = free_port()
    proc = start_node(tmp_path, ["--auth-secret-file", str(clients)], port)
    try:
        assert listening_address(port) == EVERY_INTERFACE
        assert answers("127.0.0.1", port), "a port with no bind did not take IPv4"
    finally:
        stop(proc)


def test_bind_star_without_authentication_is_the_way_back(tmp_path):
    """An operator who wants the old listener names it: `--bind *`, every interface of both families
    on one socket - which `::` is not, being IPv6 alone since #236, and `0.0.0.0` IPv4 alone."""
    port = free_port()
    proc = start_node(tmp_path, ["--bind", "*"], port)
    try:
        assert listening_address(port) == EVERY_INTERFACE
        assert answers("127.0.0.1", port), "--bind * did not take IPv4"
        if has_ipv6_loopback():
            assert answers("::1", port), "--bind * did not take IPv6"
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
        assert listening_address(port) == "127.0.0.1", "--replication-bind moved the client port too"
    finally:
        stop(proc)


@pytest.mark.parametrize("with_cluster_secret", [False, True])
def test_the_replication_port_listens_on_this_host_only_without_a_cluster_secret(tmp_path, with_cluster_secret):
    """#256 for the node links: no cluster secret and no --replication-bind is 127.0.0.1, and the
    secret is every interface - a client secret does not open the replication port."""
    clients, cluster = secrets(tmp_path)
    extra = ["--auth-secret-file", str(clients)]
    if with_cluster_secret:
        extra += ["--cluster-secret-file", str(cluster)]
    port, repl = free_port(), free_port()
    proc = start_node(tmp_path, ["--replication-port", str(repl), *extra], port)
    try:
        deadline = time.monotonic() + patience(10)
        while listening_address(repl) is None and time.monotonic() < deadline:
            time.sleep(0.1)
        assert listening_address(repl) == (EVERY_INTERFACE if with_cluster_secret else "127.0.0.1")
        assert listening_address(port) == EVERY_INTERFACE, "the client secret did not open the client port"
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


def test_a_node_announced_where_its_client_port_does_not_listen_is_refused(tmp_path):
    """#256, requirement 3: announced through a coordinator at another host, with its client port on
    loopback by the rule, a node does not start - a node registered where nobody can reach it would be
    a cluster failing in silence. Refused before a coordinator is asked, so none needs to run."""
    nobody = f"http://127.0.0.1:{free_port()}"
    result = subprocess.run([server_binary_path(), "--port", str(free_port()), "--data-dir",
                             str(tmp_path / "data"), "--coordinator-endpoints", nobody,
                             "--advertise-host", "192.0.2.10"],
                            capture_output=True, text=True, timeout=patience(20))
    assert result.returncode == 1, result
    assert ("--advertise-host 192.0.2.10 announces this node to clients and peers, but with no "
            "--auth-secret-file and no --bind its client port listens on 127.0.0.1 only") in result.stderr, \
        result.stderr
