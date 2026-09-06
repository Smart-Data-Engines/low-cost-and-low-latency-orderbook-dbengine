"""TLS on the replication link and the multi-master mesh, against live clusters (#30 part three,
series D).

Two kinds of test here, and they carry different halves of the claim.

The **positive** ones prove the byte path: a replica that replicates over TLS, a mesh that
converges over TLS, a catch-up larger than a socket buffer. Those are the things a unit test cannot
reach, because they need two processes and a real handshake between them.

The **negative** one is the mutation-critical test of this series and it is here rather than only in
`test_tls_node_link.cpp` because the accepting side's identity allowlist is a *deployment* control:
its whole purpose is to stop a certificate your CA signed for something that is not a cluster node,
and a certificate is a per-node artefact. `--tls-peer-names` naming two of three nodes means the
third cannot join, and the log says which identity was refused.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest
from conftest import ClusterManager, NodeTls, patience, server_binary_path, tail_node_log

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

from orderbook_engine import OrderbookEngine  # noqa: E402

pytestmark = pytest.mark.smoke


# ---------------------------------------------------------------------------
# Certificates
# ---------------------------------------------------------------------------

def free_port() -> int:
    import socket as _socket
    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_cluster_certs(tmp: Path, node_count: int) -> NodeTls:
    """A cluster CA and one certificate per node, the way `docs/operations.md` says to make them.

    Each certificate carries **two** SAN entries and both are load-bearing:

    * `IP:127.0.0.1`, because the *dialling* end verifies the address it dialled and the
      replication client dials an address and never a name — it resolves nothing;
    * `DNS:node-<i>`, so `--tls-peer-names` has a per-node identity to match on the accepting end.

    Getting that split wrong is the mistake this whole area is about: verification matches the SAN,
    while the identity in a log line is the common name.
    """
    d = tmp / "tls"
    d.mkdir(parents=True, exist_ok=True)
    ca = d / "ca.pem"
    ca_key = d / "ca-key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", str(ca_key), "-out", str(ca), "-days", "1",
         "-subj", "/CN=orderbook test CA"],
        check=True, capture_output=True, timeout=120)

    for i in range(node_count):
        key = d / f"node-{i}-key.pem"
        csr = d / f"node-{i}.csr"
        ext = d / f"node-{i}.ext"
        cert = d / f"node-{i}.pem"
        ext.write_text(f"subjectAltName=IP:127.0.0.1,DNS:node-{i}\n")
        subprocess.run(
            ["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key),
             "-out", str(csr), "-subj", f"/CN=node-{i}"],
            check=True, capture_output=True, timeout=120)
        subprocess.run(
            ["openssl", "x509", "-req", "-in", str(csr), "-CA", str(ca), "-CAkey", str(ca_key),
             "-CAcreateserial", "-out", str(cert), "-days", "1", "-extfile", str(ext)],
            check=True, capture_output=True, timeout=120)
        key.chmod(0o600)

    return NodeTls(cert_dir=str(d), ca=str(ca))


def scrape(port: int) -> dict[str, float]:
    """Read /metrics into a dict, stripping the label set from each name.

    The label set is why a lookup by bare name finds nothing: the exposition is
    `ob_mm_peers_connected{node_role="standalone"} 3`, so splitting on whitespace gives keys that
    all end in `{...}` and every lookup misses — which reads as "the counter is zero" (pitfall 66).
    """
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
        body = r.read().decode()
    out: dict[str, float] = {}
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, value = line.partition(" ")
        out[name.split("{", 1)[0]] = float(value)
    return out


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def tls_ha_cluster(tmp_path_factory) -> ClusterManager:
    """Two nodes, primary and replica, with TLS and mutual verification on the replication link."""
    tmp = tmp_path_factory.mktemp("tls_ha")
    cm = ClusterManager()
    cm.node_tls = make_cluster_certs(tmp, node_count=2)
    cm.start()
    yield cm
    cm.shutdown()


@pytest.fixture(scope="module")
def tls_mm_cluster(tmp_path_factory) -> ClusterManager:
    """Three multi-master nodes with TLS and mutual verification on the mesh, and **no cluster
    secret** — mTLS as the alternative to the shared secret (part one, requirement 8.4)."""
    tmp = tmp_path_factory.mktemp("tls_mm")
    cm = ClusterManager()
    cm.node_tls = make_cluster_certs(tmp, node_count=3)
    cm.start_multi_master(node_count=3)
    cm.wait_for_mm_mesh(timeout=patience(60))
    yield cm
    cm.shutdown()


# ---------------------------------------------------------------------------
# The byte path
# ---------------------------------------------------------------------------

def test_a_replica_replicates_over_tls(tls_ha_cluster: ClusterManager) -> None:
    primary = tls_ha_cluster.primary()
    replica = tls_ha_cluster.replica()

    with OrderbookEngine(host="127.0.0.1", port=primary.tcp_port) as eng:
        eng.insert("TLSREPL", "BINANCE", "bid", [100_500], [3])
        eng.flush()

    deadline = time.time() + patience(20)
    rows: list = []
    while time.time() < deadline:
        with OrderbookEngine(host="127.0.0.1", port=replica.tcp_port) as eng:
            rows = eng.query_all("TLSREPL", "BINANCE")
        if rows:
            break
        time.sleep(0.3)

    assert rows, ("the row never reached the replica over TLS.\nprimary log:\n"
                  + tail_node_log(primary) + "\nreplica log:\n" + tail_node_log(replica))
    assert rows[0].price == 100_500


def test_the_replication_handshake_names_the_certificate_identity(
        tls_ha_cluster: ClusterManager) -> None:
    """The field requirement 8.4 of part one asked for, on a link that had no identity at all.

    Before mTLS a node's identity was its `node_id`, which arrives in a handshake that
    authentication precedes — so the cluster form of a secret file carries no name. This asserts the
    log line, because that is where an operator reads it and because #31's ACLs will read the same
    field.
    """
    primary = tls_ha_cluster.primary()
    log = Path(primary.data_dir, "node.log").read_text(errors="replace")
    assert "authenticated by certificate: node-" in log, (
        "the primary never logged a certificate identity for its replica:\n"
        + tail_node_log(primary))


def test_a_catchup_larger_than_a_socket_buffer_completes_over_tls(
        tls_ha_cluster: ClusterManager) -> None:
    """Catch-up is the path where a TLS write meets a full socket, so it is where
    `SSL_ERROR_WANT_WRITE` has to have somewhere to go.

    The same question in plaintext had no answer until series D: `send_to_replica()` called a
    blocking `send_all()` on a non-blocking socket, so the first EAGAIN dropped the replica mid
    catch-up. Measured then: 17 270 of 40 000 records. Here the replica is stopped, a few megabytes
    are written, and it is brought back — so the whole range goes out in one catch-up, encrypted.
    """
    primary = tls_ha_cluster.primary()
    replica_index = tls_ha_cluster.replica().index

    tls_ha_cluster.kill_node(replica_index)

    # ~4 MB on the wire: 2000 updates of 100 levels each. Well past the ~2.6 MB this loopback pair
    # absorbs before the sender first sees EAGAIN.
    with OrderbookEngine(host="127.0.0.1", port=primary.tcp_port) as eng:
        prices = list(range(50_000, 50_100))
        qtys = [10] * 100
        for i in range(2000):
            eng.insert("TLSCATCH", "BINANCE", "bid", [p + i for p in prices], qtys)
        eng.flush()

    tls_ha_cluster.restart_node(replica_index)
    replica = tls_ha_cluster.nodes[replica_index]

    deadline = time.time() + patience(60)
    rows: list = []
    while time.time() < deadline:
        try:
            with OrderbookEngine(host="127.0.0.1", port=replica.tcp_port) as eng:
                rows = eng.query_all("TLSCATCH", "BINANCE")
        except Exception:  # noqa: BLE001 - the node is still coming up
            rows = []
        if rows:
            break
        time.sleep(0.5)

    assert rows, ("catch-up over TLS never delivered the rows.\nprimary log:\n"
                  + tail_node_log(primary) + "\nreplica log:\n" + tail_node_log(replica))


def test_a_mesh_converges_over_tls_without_a_cluster_secret(
        tls_mm_cluster: ClusterManager) -> None:
    """mTLS as the alternative to the shared secret, which is what requirement 8.4 asked for.

    This cluster runs with no `--cluster-secret-file` at all: the only thing establishing that a
    peer belongs to the mesh is the certificate the CA signed for it — and unlike a shared secret,
    that is bound to the connection, which is the whole point (`SECURITY.md`).
    """
    origin = tls_mm_cluster.nodes[0]
    with OrderbookEngine(host="127.0.0.1", port=origin.tcp_port) as eng:
        eng.insert("TLSMESH", "BINANCE", "bid", [77_000], [5])
        eng.flush()

    for node in tls_mm_cluster.nodes[1:]:
        deadline = time.time() + patience(30)
        rows: list = []
        while time.time() < deadline:
            with OrderbookEngine(host="127.0.0.1", port=node.tcp_port) as eng:
                rows = eng.query_all("TLSMESH", "BINANCE")
            if rows:
                break
            time.sleep(0.3)
        assert rows, (f"the row never reached {node.node_id} over the TLS mesh:\n"
                      + tail_node_log(node))
        assert rows[0].price == 77_000


def test_metrics_report_the_replica_as_verified(tls_ha_cluster: ClusterManager) -> None:
    """The same guarantee as the mesh one below, on the link that has a direction.

    Both numbers are exported so the comparison is one scrape: a verified count on its own says
    nothing, and the count it is measured against used to live only in `STATUS`, where an alert
    cannot reach it. The verified gauge is also published on every pass of the run loop rather than
    only where a handshake succeeds, which is what makes a dropped replica leave the count.
    """
    primary = tls_ha_cluster.primary()
    deadline = time.time() + patience(20)
    m: dict = {}
    while time.time() < deadline:
        m = scrape(primary.metrics_port)
        if m.get("ob_replicas_connected", 0) >= 1:
            break
        time.sleep(0.5)

    assert m.get("ob_replicas_connected", 0) >= 1, (
        "the primary reports no connected replica, so this test would assert nothing:\n"
        + tail_node_log(primary))
    assert m["ob_replicas_tls_verified"] == m["ob_replicas_connected"], (
        f"{m['ob_replicas_tls_verified']} of {m['ob_replicas_connected']} replicas are verified, "
        f"so one is talking plaintext:\n" + tail_node_log(primary))


def test_a_dropped_replica_leaves_both_replica_gauges(tmp_path) -> None:
    """A count that only goes up is not a count.

    The verified gauge was published from one place - the end of a successful handshake - so it was
    correct until the first disconnection and then reported a link that no longer existed. Worse
    than a wrong number: `verified` could exceed `connected`, which reads as impossible and sends
    an operator looking for the wrong fault. Its own cluster rather than the module fixture,
    because the point of the test is to take a node away.
    """
    cm = ClusterManager()
    cm.node_tls = make_cluster_certs(tmp_path, node_count=2)
    try:
        cm.start()
        primary = cm.primary()

        deadline = time.time() + patience(20)
        while time.time() < deadline:
            m = scrape(primary.metrics_port)
            if m.get("ob_replicas_connected", 0) >= 1 and m.get("ob_replicas_tls_verified", 0) >= 1:
                break
            time.sleep(0.5)
        assert m.get("ob_replicas_tls_verified", 0) >= 1, (
            "no verified replica to lose, so the second half asserts nothing:\n"
            + tail_node_log(primary))

        replica_index = next(n.index for n in cm.nodes if n.node_id != primary.node_id)
        cm.kill_node(replica_index)

        deadline = time.time() + patience(30)
        while time.time() < deadline:
            m = scrape(primary.metrics_port)
            if m.get("ob_replicas_connected", 0) == 0:
                break
            time.sleep(0.5)

        assert m.get("ob_replicas_connected", 1) == 0, (
            f"the primary still reports {m.get('ob_replicas_connected')} connected replicas after "
            f"the replica was killed:\n" + tail_node_log(primary))
        assert m.get("ob_replicas_tls_verified", 1) == 0, (
            f"the replica is gone and {m.get('ob_replicas_tls_verified')} links are still counted "
            f"as verified:\n" + tail_node_log(primary))
    finally:
        cm.shutdown()


def test_metrics_report_every_peer_as_verified(tls_mm_cluster: ClusterManager) -> None:
    """A guarantee whose state cannot be read on a live node is a guarantee on our word.

    So the assertion is not "TLS is configured" — that is in the config file — but that every peer
    this node believes it is connected to also presented a certificate it verified. A gap between
    these two numbers is a peer talking plaintext.
    """
    for node in tls_mm_cluster.nodes:
        m = scrape(node.metrics_port)
        assert "ob_mm_peers_tls_verified" in m, f"{node.node_id} does not export the gauge"
        assert m["ob_mm_peers_tls_verified"] == m["ob_mm_peers_connected"], (
            f"{node.node_id}: {m['ob_mm_peers_tls_verified']} of "
            f"{m['ob_mm_peers_connected']} peers are verified")
        assert m["ob_mm_peers_connected"] >= 2


# ---------------------------------------------------------------------------
# The refusal that is the deployment control
# ---------------------------------------------------------------------------

def test_a_peer_outside_the_name_allowlist_cannot_join_the_mesh(tmp_path) -> None:
    """The mutation-critical test of series D, and the reason it is an integration test.

    Every certificate here is signed by the trust anchor every node was given, so the **chain check
    passes for all three**. What stops node-2 is its identity: `--tls-peer-names` names node-0 and
    node-1 only. Without that check a corporate CA signing every host in an organisation would mean
    every host in the organisation may join the mesh and stream the write-ahead log, and nothing
    about that reads as wrong.

    The mesh is expected **not** to converge, and the log is expected to say which identity was
    refused — a refusal an operator cannot diagnose is a cluster that does not form for no visible
    reason.
    """
    cm = ClusterManager()
    tls = make_cluster_certs(tmp_path, node_count=3)
    tls.peer_names = "node-0,node-1"
    cm.node_tls = tls
    try:
        cm.start_multi_master(node_count=3)

        # Counting `connected`, not rows. `wait_for_mm_mesh()` counts MM_PEERS rows, and since #84
        # that view lists connections still in their handshake - so a peer being refused can still
        # appear, and the mesh reads as converged. Pitfall 87 is the same mistake with a different
        # token.
        deadline = time.time() + patience(20)
        while time.time() < deadline:
            time.sleep(1.0)
        for node in cm.nodes[:2]:
            connected = cm._send(node, "MM_PEERS").count("connected")
            assert connected <= 1, (
                f"{node.node_id} connected to {connected} peers; node-2 is not in "
                f"--tls-peer-names and should have been refused:\n" + tail_node_log(node))

        refusals = [tail_node_log(n, lines=400) for n in cm.nodes[:2]]
        assert any("is not in --tls-peer-names" in log for log in refusals), (
            "the mesh did not converge and no node said why:\n" + "\n---\n".join(refusals))
        assert any("node-2" in log for log in refusals), (
            "the refusal did not name the identity it refused:\n" + "\n---\n".join(refusals))

        # And the other half, which is what makes the first half a statement about the *name*:
        # node-0 and node-1 are in the allowlist and do connect to each other.
        assert cm._send(cm.nodes[0], "MM_PEERS").count("connected") == 1, (
            "the two allowed nodes did not connect to each other either, so this test would also "
            "pass if TLS refused everything:\n" + tail_node_log(cm.nodes[0]))
    finally:
        # Two of the three nodes are healthy and the third is being refused on purpose, which the
        # normal teardown would report as an unexplained state.
        cm.shutdown()


# ---------------------------------------------------------------------------
# Startup refusals — no cluster needed
# ---------------------------------------------------------------------------

def run_server(args: list[str], data_dir: Path) -> subprocess.CompletedProcess:
    """Start a server for real and wait for it to exit, or kill it after a moment.

    Deliberately **not** `--print-config`. That flag prints and exits before any context is built,
    so every refusal here would report success - which the first version of these tests did, with
    the whole resolved configuration as the failure message. `--print-config` not validating TLS is
    consistent (it does not load the secret files either) and is the right behaviour for a flag
    whose job is to answer "what did you resolve"; it just cannot be the thing a refusal test runs.
    """
    proc = subprocess.Popen(
        [server_binary_path(), "--port", str(free_port()), "--data-dir", str(data_dir)] + args,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        out, err = proc.communicate(timeout=patience(6))
        return subprocess.CompletedProcess(proc.args, proc.returncode, out, err)
    except subprocess.TimeoutExpired:
        # It did not refuse, which for the positive half of these tests is the expected outcome.
        proc.kill()
        out, err = proc.communicate(timeout=10)
        return subprocess.CompletedProcess(proc.args, 0, out, err)


def test_a_node_link_without_a_trust_anchor_refuses_to_start(tmp_path) -> None:
    """The one refusal the client port does not have.

    A node link verifies its peer in both directions; without a trust anchor it would encrypt and
    authenticate nobody, which leaves the relay in `SECURITY.md` open **and looks like protection**.
    So this is a refusal rather than a warning, and it fires before a port is opened.
    """
    tls = make_cluster_certs(tmp_path, node_count=1)
    cert, key = tls.files_for(0)

    r = run_server(["--tls-replication", "--tls-cert-file", cert, "--tls-key-file", key],
                   tmp_path / "d1")
    assert r.returncode != 0, r.stdout
    assert "--tls-ca-file" in r.stderr, r.stderr

    r = run_server(["--tls-multi-master", "--tls-cert-file", cert, "--tls-key-file", key],
                   tmp_path / "d2")
    assert r.returncode != 0, r.stdout
    assert "--tls-ca-file" in r.stderr, r.stderr

    # And the accepting half of the same statement: with the anchor it starts and stays up. Without
    # this the test would also pass if a node-link flag refused unconditionally.
    r = run_server(["--tls-replication", "--tls-cert-file", cert, "--tls-key-file", key,
                    "--tls-ca-file", tls.ca], tmp_path / "d3")
    assert r.returncode == 0, r.stderr
    assert "replication link: TLS 1.3, mutual certificate verification" in r.stderr, r.stderr


def test_a_node_link_without_a_certificate_refuses_to_start(tmp_path) -> None:
    tls = make_cluster_certs(tmp_path, node_count=1)
    r = run_server(["--tls-replication", "--tls-ca-file", tls.ca], tmp_path / "d4")
    assert r.returncode != 0, r.stdout
    assert "--tls-cert-file" in r.stderr, r.stderr


def test_print_config_shows_every_node_link_setting(tmp_path) -> None:
    """`--print-config` exists to be pasted into a ticket, so it has to carry these four - and it
    must carry paths rather than contents, which is why the key files are paths in `ServerConfig`."""
    tls = make_cluster_certs(tmp_path, node_count=1)
    cert, key = tls.files_for(0)
    r = subprocess.run(
        [server_binary_path(), "--port", "0", "--data-dir", str(tmp_path / "d5"), "--print-config",
         "--tls-replication", "--tls-multi-master", "--tls-cert-file", cert,
         "--tls-key-file", key, "--tls-ca-file", tls.ca, "--tls-peer-names", "node-0,node-1"],
        capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    for expected in ("tls-replication", "tls-multi-master", "tls-ca-file", "tls-peer-names",
                     "node-0,node-1"):
        assert expected in r.stdout, f"{expected} missing from --print-config:\n{r.stdout}"
