"""IPv6, on every listener and every dialler of a node (spec ipv6-everywhere).

Until this module the engine was IPv4 alone: `socket(AF_INET)` and `INADDR_ANY` on the client port,
the replication port, the mesh port and the metrics endpoint, `inet_pton(AF_INET)` for a bind
address, a replica's primary and a C++ client's server, and `AF_INET` in the Python client - which is
how the C++ SDK port found it, with `[::1]` failing in the Python client (its finding 28). A node on a
host with only IPv6 could neither listen nor dial.

Nodes of their own, because these tests are about where a node listens and the shared cluster
listens where every other module expects it. A host without `::1` skips the module by name - and the
integration job's skip gate refuses a skip it does not know, so in CI this runs.
"""
from __future__ import annotations

import base64
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request

import pytest

from conftest import cpp_client_binary_path, free_port, has_ipv6_loopback, patience, server_binary_path
import orderbook_engine as ob
from orderbook_engine import OrderbookEngine

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()
ETCD = os.environ.get("OB_ETCD_BINARY") or shutil.which("etcd") or "/usr/local/bin/etcd"


if not has_ipv6_loopback():
    pytestmark = [pytest.mark.smoke, pytest.mark.skip(reason="this host has no ::1")]


def _answers(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2) as s:
            s.settimeout(2)
            banner = b""
            while b"\n\n" not in banner:
                chunk = s.recv(4096)
                if not chunk:
                    return False
                banner += chunk
            s.sendall(b"PING\n")
            return b"PONG" in s.recv(1024)
    except OSError:
        return False


def _status(host: str, port: int) -> str:
    """One STATUS, asked over `host`."""
    with socket.create_connection((host, port), timeout=5) as s:
        s.settimeout(5)
        banner = b""
        while b"\n\n" not in banner:
            chunk = s.recv(4096)
            if not chunk:
                return ""
            banner += chunk
        s.sendall(b"STATUS\n")
        reply = b""
        while not reply.endswith(b"\n\n"):
            chunk = s.recv(1 << 16)
            if not chunk:
                break
            reply += chunk
        return reply.decode(errors="replace")


def _replica_line(host: str, port: int) -> str:
    """The primary's `replica[0]:` line from STATUS, once a replica has connected."""
    deadline = time.time() + patience(30)
    reply = ""
    while time.time() < deadline:
        reply = _status(host, port)
        for line in reply.splitlines():
            if line.startswith("replica[0]:"):
                return line
        time.sleep(0.3)
    raise AssertionError(f"no replica in the primary's STATUS:\n{reply}")


class Node:
    """A standalone server on a data directory of its own, asked over `host`."""

    def __init__(self, data_dir: str, host: str, extra: list[str] | None = None):
        self.data_dir = data_dir
        self.host = host
        self.extra = extra or []
        self.port = 0
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        self.port = free_port()
        log = open(os.path.join(self.data_dir, "node.log"), "a", encoding="utf-8")
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", self.data_dir, "--metrics-port", "0",
             "--flush-interval-ms", "50", "--drain-timeout-ms", "1000", *self.extra],
            stdout=log, stderr=subprocess.STDOUT)
        log.close()
        deadline = time.time() + patience(30)
        while time.time() < deadline:
            if _answers(self.host, self.port):
                return
            time.sleep(0.1)
        self.stop()
        pytest.fail(f"the node never answered on {self.host} port {self.port}; log tail:\n"
                    f"{self.log()[-1500:]}")

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)

    def log(self) -> str:
        with open(os.path.join(self.data_dir, "node.log"), encoding="utf-8", errors="replace") as f:
            return f.read()

    def client(self, host: str | None = None) -> OrderbookEngine:
        return OrderbookEngine(host=host or self.host, port=self.port, timeout=patience(10))


def _write_and_read(engine: OrderbookEngine, symbol: str) -> list[int]:
    engine.insert(symbol, "EX", "bid", [100, 99], [1, 2])
    engine.flush()
    return sorted(row.price for row in engine.query_all(symbol, "EX"))


def test_a_node_bound_to_ipv6_loopback_serves_the_python_client():
    with tempfile.TemporaryDirectory(prefix="ob_ipv6_bound_") as d:
        node = Node(d, "::1", ["--bind", "::1"])
        node.start()
        try:
            for host, symbol in (("::1", "V6BARE"), ("[::1]", "V6BRACKETS")):
                engine = node.client(host)
                try:
                    assert _write_and_read(engine, symbol) == [99, 100], host
                finally:
                    engine.close()
            assert not _answers("127.0.0.1", node.port), "a node bound to ::1 answered on IPv4"
            assert "listening on [::1]:" in node.log(), node.log()[-1500:]
        finally:
            node.stop()


def test_a_node_with_no_bind_takes_both_families():
    with tempfile.TemporaryDirectory(prefix="ob_ipv6_dual_") as d:
        node = Node(d, "127.0.0.1")
        node.start()
        try:
            for host, symbol in (("127.0.0.1", "DUAL4"), ("::1", "DUAL6")):
                engine = node.client(host)
                try:
                    assert _write_and_read(engine, symbol) == [99, 100], host
                finally:
                    engine.close()
            assert "(IPv6 and IPv4)" in node.log(), node.log()[-1500:]
        finally:
            node.stop()


def test_the_cpp_client_reaches_a_node_over_ipv6():
    binary = cpp_client_binary_path()
    if binary is None:
        pytest.skip("ob_integration_test not built")
    with tempfile.TemporaryDirectory(prefix="ob_ipv6_cpp_") as d:
        node = Node(d, "::1", ["--bind", "::1"])
        node.start()
        try:
            for host in ("::1", "[::1]"):
                proc = subprocess.run([binary, "--host", host, "--port", str(node.port), "--test", "ping"],
                                      capture_output=True, text=True, timeout=patience(30))
                results = [json.loads(line) for line in proc.stdout.splitlines()
                           if line.strip().startswith("{") and '"test"' in line]
                assert results and results[-1].get("status") == "pass", \
                    (host, proc.stdout[-800:], proc.stderr[-400:])
                assert proc.returncode == 0, host
        finally:
            node.stop()


def test_a_replica_follows_a_primary_over_ipv6():
    with tempfile.TemporaryDirectory(prefix="ob_ipv6_primary_") as pd, \
            tempfile.TemporaryDirectory(prefix="ob_ipv6_replica_") as rd:
        repl_port = free_port()
        primary = Node(pd, "::1", ["--bind", "::1", "--replication-bind", "::1",
                                   "--replication-port", str(repl_port)])
        replica = Node(rd, "::1", ["--bind", "::1", "--primary-host", "::1",
                                   "--primary-port", str(repl_port)])
        primary.start()
        replica.start()
        try:
            engine = primary.client()
            try:
                want = _write_and_read(engine, "V6REPL")
            finally:
                engine.close()
            deadline = time.time() + patience(30)
            got: list[int] = []
            while time.time() < deadline:
                reader = replica.client()
                try:
                    try:
                        got = sorted(row.price for row in reader.query_all("V6REPL", "EX"))
                    except Exception:  # noqa: BLE001 - not there yet
                        got = []
                finally:
                    reader.close()
                if got == want:
                    break
                time.sleep(0.2)
            assert got == want, (f"the replica holds {got}, the primary {want}; replica log:\n"
                                 f"{replica.log()[-1500:]}")
            assert "new replica connection from [::1]:" in primary.log(), primary.log()[-1500:]
            line = _replica_line("::1", primary.port)
            assert line.startswith("replica[0]: [::1]:"), line
        finally:
            replica.stop()
            primary.stop()


def test_an_ipv4_replica_of_a_port_with_no_bind_is_shown_as_ipv4():
    """A port with no bind takes IPv4 as addresses mapped into IPv6, `::ffff:127.0.0.1`; the log and
    STATUS show the replica as the IPv4 address it dialled from."""
    with tempfile.TemporaryDirectory(prefix="ob_ipv6_mapped_p_") as pd, \
            tempfile.TemporaryDirectory(prefix="ob_ipv6_mapped_r_") as rd:
        repl_port = free_port()
        primary = Node(pd, "127.0.0.1", ["--replication-port", str(repl_port)])
        replica = Node(rd, "127.0.0.1", ["--bind", "127.0.0.1", "--primary-host", "127.0.0.1",
                                         "--primary-port", str(repl_port)])
        primary.start()
        replica.start()
        try:
            line = _replica_line("127.0.0.1", primary.port)
            assert line.startswith("replica[0]: 127.0.0.1:"), line
            assert "ffff" not in primary.log(), primary.log()[-1500:]
            assert "new replica connection from 127.0.0.1:" in primary.log(), primary.log()[-1500:]
        finally:
            replica.stop()
            primary.stop()


def _node_certificates(d: str, san: str) -> tuple[str, str, str, str, str]:
    """A CA and two node certificates signed by it, both carrying `san`: (ca, cert0, key0, cert1,
    key1). The dialling end checks the host it was given against the SAN, so `IP:::1` is what a
    replica given `::1` - or `[::1]` - needs to find in its primary's certificate."""
    ca, ca_key = os.path.join(d, "ca.pem"), os.path.join(d, "ca-key.pem")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", ca_key,
                    "-out", ca, "-days", "1", "-subj", "/CN=ipv6 test CA"],
                   check=True, capture_output=True, timeout=120)
    out = [ca]
    for i in (0, 1):
        key, csr = os.path.join(d, f"node-{i}-key.pem"), os.path.join(d, f"node-{i}.csr")
        cert, ext = os.path.join(d, f"node-{i}.pem"), os.path.join(d, f"node-{i}.ext")
        with open(ext, "w", encoding="utf-8") as f:
            f.write(f"subjectAltName={san}\n")
        subprocess.run(["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", csr,
                        "-subj", f"/CN=node-{i}"], check=True, capture_output=True, timeout=120)
        subprocess.run(["openssl", "x509", "-req", "-in", csr, "-CA", ca, "-CAkey", ca_key,
                        "-CAcreateserial", "-out", cert, "-days", "1", "-extfile", ext],
                       check=True, capture_output=True, timeout=120)
        os.chmod(key, 0o600)
        out += [cert, key]
    return tuple(out)  # type: ignore[return-value]


def test_a_replica_given_its_primary_in_brackets_checks_the_certificate_against_the_address():
    """TLS on the replication link over ::1, with the replica given `--primary-host [::1]`. The name
    check used to take the brackets as part of a DNS name and refuse a certificate for `IP:::1`."""
    with tempfile.TemporaryDirectory(prefix="ob_ipv6_tls_") as root:
        certs = os.path.join(root, "tls")
        os.makedirs(certs)
        ca, cert0, key0, cert1, key1 = _node_certificates(certs, "IP:::1")
        pd, rd = os.path.join(root, "primary"), os.path.join(root, "replica")
        os.makedirs(pd)
        os.makedirs(rd)
        repl_port = free_port()
        tls = ["--tls-replication", "--tls-ca-file", ca]
        primary = Node(pd, "::1", ["--bind", "::1", "--replication-bind", "::1",
                                   "--replication-port", str(repl_port),
                                   "--tls-cert-file", cert0, "--tls-key-file", key0, *tls])
        replica = Node(rd, "::1", ["--bind", "::1", "--primary-host", "[::1]",
                                   "--primary-port", str(repl_port),
                                   "--tls-cert-file", cert1, "--tls-key-file", key1, *tls])
        primary.start()
        replica.start()
        try:
            engine = primary.client()
            try:
                want = _write_and_read(engine, "V6TLS")
            finally:
                engine.close()
            deadline = time.time() + patience(30)
            got: list[int] = []
            while time.time() < deadline:
                reader = replica.client()
                try:
                    try:
                        got = sorted(row.price for row in reader.query_all("V6TLS", "EX"))
                    except Exception:  # noqa: BLE001 - not there yet
                        got = []
                finally:
                    reader.close()
                if got == want:
                    break
                time.sleep(0.2)
            assert got == want, (f"the replica holds {got}, the primary {want}; replica log:\n"
                                 f"{replica.log()[-2000:]}")
            assert _replica_line("::1", primary.port).startswith("replica[0]: [::1]:")
        finally:
            replica.stop()
            primary.stop()


def _start_etcd_on_ipv6(root: str) -> tuple[subprocess.Popen, str]:
    """A native etcd on [::1], and its client URL. Its log is `etcd.log` in `root`."""
    client_port, peer_port = free_port(), free_port()
    url = f"http://[::1]:{client_port}"
    with open(os.path.join(root, "etcd.log"), "ab") as log:
        proc = subprocess.Popen(
            [ETCD, "--name", "ipv6", "--data-dir", os.path.join(root, "etcd"),
             "--advertise-client-urls", url, "--listen-client-urls", url,
             "--listen-peer-urls", f"http://[::1]:{peer_port}",
             "--initial-advertise-peer-urls", f"http://[::1]:{peer_port}",
             "--initial-cluster", f"ipv6=http://[::1]:{peer_port}"],
            stdout=log, stderr=subprocess.STDOUT)
    try:
        _wait_for_etcd(url)
    except BaseException:
        _stop(proc)
        raise
    return proc, url


def _stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def _wait_for_etcd(url: str) -> None:
    deadline = time.time() + patience(30)
    last: Exception | None = None
    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"{url}/v3/maintenance/status", data=b"{}", method="POST")
            req.add_header("Content-Type", "application/json")
            if urllib.request.urlopen(req, timeout=2).status == 200:
                return
        except Exception as exc:  # noqa: BLE001 - not up yet
            last = exc
        time.sleep(0.3)
    pytest.fail(f"etcd at {url} never answered: {last}")


def test_a_mesh_pair_meets_over_ipv6_through_an_etcd_on_ipv6():
    """Two multi-master nodes on ::1, finding each other through an etcd on ::1: each publishes
    `[::1]:<port>`, the other reads it back, splits it and dials it."""
    with tempfile.TemporaryDirectory(prefix="ob_ipv6_mesh_") as root:
        etcd, etcd_url = _start_etcd_on_ipv6(root)
        nodes: list[Node] = []
        try:
            for i in (1, 2):
                d = os.path.join(root, f"node-{i}")
                os.makedirs(d)
                node = Node(d, "::1", ["--bind", "::1", "--multi-master", "--mm-node-id", str(i),
                                       "--mm-replication-port", str(free_port()), "--mm-bind", "::1",
                                       "--replication-port", str(free_port()), "--replication-bind", "::1",
                                       "--advertise-host", "::1", "--coordinator-endpoints", etcd_url,
                                       "--node-id", f"v6-mesh-{i}"])
                nodes.append(node)
                node.start()
            writer = nodes[0].client()
            try:
                want = _write_and_read(writer, "V6MESH")
            finally:
                writer.close()
            deadline = time.time() + patience(30)
            got: list[int] = []
            while time.time() < deadline:
                reader = nodes[1].client()
                try:
                    try:
                        got = sorted(row.price for row in reader.query_all("V6MESH", "EX"))
                    except Exception:  # noqa: BLE001 - not there yet
                        got = []
                finally:
                    reader.close()
                if got == want:
                    break
                time.sleep(0.2)
            logs = "\n".join(n.log() for n in nodes)
            assert got == want, f"node 2 holds {got}, node 1 wrote {want}; logs:\n{logs[-3000:]}"
            assert " at [::1]:" in logs and "Connected to peer" in logs, logs[-3000:]
            assert "Accepted peer connection fd=" in logs and " from [::1]:" in logs, logs[-3000:]
        finally:
            for node in reversed(nodes):
                node.stop()
            _stop(etcd)


def test_a_pool_keeps_its_connection_to_a_shard_at_an_ipv6_address():
    """A shard map names a shard by the address its node publishes: `[::1]:<port>` for IPv6. The
    pool compared that with its connection's host and port written without brackets, so the
    comparison never held, and every refresh of the map replaced the connection to that shard."""
    with tempfile.TemporaryDirectory(prefix="ob_ipv6_shards_") as root:
        etcd, etcd_url = _start_etcd_on_ipv6(root)
        node: Node | None = None
        eng: OrderbookEngine | None = None
        try:
            d = os.path.join(root, "s0")
            os.makedirs(d)
            node = Node(d, "::1", ["--bind", "::1"])
            node.start()
            address = ob._join_host_port("::1", node.port)
            shard_map = {"active_migrations": [], "assignments": {"V6SHARD.EX": "s0"},
                         "pinned_symbols": [], "version": 1,
                         "shards": {"s0": {"address": address, "mm_nodes": [], "shard_id": "s0",
                                           "status": "active", "vnodes": 150}}}
            body = json.dumps({"key": base64.b64encode(b"/ob/shard_map").decode(),
                               "value": base64.b64encode(json.dumps(shard_map).encode()).decode()})
            req = urllib.request.Request(f"{etcd_url}/v3/kv/put", data=body.encode(),
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                assert resp.status == 200

            eng = OrderbookEngine(hosts=[address], coordinator_endpoints=[etcd_url],
                                  health_check_interval=3600)
            first = eng._pool._routing.connections.get("s0")
            assert first is not None, "the pool opened no connection to the shard the map names"
            eng._pool._refresh_routing(eng._pool._routing.shard_map)
            assert eng._pool._routing.connections.get("s0") is first, (
                f"a refresh of the same map replaced the connection to {address}")
            eng.insert("V6SHARD", "EX", "bid", [100], [1], [1])
            reader = node.client()
            try:
                reader.flush()
                assert [r.price for r in reader.query_all("V6SHARD", "EX")] == [100]
            finally:
                reader.close()
        finally:
            if eng is not None:
                eng.close()
            if node is not None:
                node.stop()
            _stop(etcd)
