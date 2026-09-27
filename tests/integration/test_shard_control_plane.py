"""Shards that find each other in etcd, and clients that find them (#175).

Sharding by symbol had its parts - the hash ring, the map's format, ownership, `SYMBOL_MIGRATED` - and
nothing joining them: a node started with `--shard-id` wrote neither itself nor the map to etcd, put
itself alone in its own map and so owned every symbol, and two of them on one etcd shared `/ob/leader`,
so the second became the first one's replica and refused every write. The C++ router read nothing,
and the Python pool read a key nobody wrote. Measured against a native etcd before the fix, each test
here fails; `test_sharded_pool.py` writes the map itself, because it had to.

Each test starts its own etcd and servers: what is under test is how they start.
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

from conftest import cpp_client_binary_path, free_port, patience, server_binary_path
import orderbook_engine as ob
from orderbook_engine import BookUpdate, OrderbookEngine

pytestmark = pytest.mark.pool
SERVER = server_binary_path()
PREFIX = "/ob/"
EXCHANGE = "EX"


class Cluster:
    """A native etcd and servers started with `--shard-id`, each its own failover group of one."""

    def __init__(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="ob_shard_cp_")
        self.procs: dict[str, subprocess.Popen] = {}
        self.ports: dict[str, int] = {}
        self.repl_ports: dict[str, int] = {}
        self._etcd_proc = None
        try:
            client, peer = free_port(), free_port()
            self.etcd = f"http://127.0.0.1:{client}"
            self._etcd_proc = subprocess.Popen(
                [os.environ.get("OB_ETCD_BINARY") or "etcd", "--data-dir", f"{self.dir}/etcd",
                 "--listen-client-urls", self.etcd, "--advertise-client-urls", self.etcd,
                 "--listen-peer-urls", f"http://127.0.0.1:{peer}",
                 "--initial-advertise-peer-urls", f"http://127.0.0.1:{peer}",
                 "--initial-cluster", f"default=http://127.0.0.1:{peer}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self._wait_port(client)
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _wait_port(port: int) -> None:
        deadline = time.time() + patience(20)
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f"nothing listens on port {port}")

    def start(self, sid: str, wait: bool = True, host: str = "127.0.0.1",
              shard: str | None = None) -> None:
        """Node `sid`, of shard `shard` - its own name unless a second node of a shard's group."""
        port = free_port()
        self.ports[sid] = port
        self.repl_ports[sid] = free_port()
        log = open(f"{self.dir}/{sid}.log", "ab")
        self.procs[sid] = subprocess.Popen(
            [SERVER, "--port", str(port), "--metrics-port", "0", "--data-dir", f"{self.dir}/{sid}",
             "--shard-id", shard or sid, "--node-id", f"node-{sid}",
             "--coordinator-endpoints", self.etcd,
             "--replication-port", str(self.repl_ports[sid]), "--advertise-host", host],
            stdout=log, stderr=subprocess.STDOUT)
        if wait:
            self._wait_port(port)

    def address(self, sid: str) -> str:
        return f"127.0.0.1:{self.ports[sid]}"

    def get(self, key: str):
        body = json.dumps({"key": base64.b64encode(key.encode()).decode()}).encode()
        req = urllib.request.Request(f"{self.etcd}/v3/kv/range", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            kvs = json.loads(resp.read()).get("kvs") or []
        return base64.b64decode(kvs[0]["value"]).decode() if kvs else None

    def shard_map(self) -> dict:
        raw = self.get(f"{PREFIX}shard_map")
        return json.loads(raw) if raw else {}

    def put(self, key: str, value: str) -> None:
        body = json.dumps({"key": base64.b64encode(key.encode()).decode(),
                           "value": base64.b64encode(value.encode()).decode()}).encode()
        req = urllib.request.Request(f"{self.etcd}/v3/kv/put", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200

    def wait_for_map(self, sids: tuple[str, ...]) -> dict:
        deadline = time.time() + patience(20)
        doc = {}
        while time.time() < deadline:
            doc = self.shard_map()
            if set(doc.get("shards", {})) >= set(sids):
                return doc
            time.sleep(0.2)
        raise AssertionError(f"the shard map in etcd does not name {sids}: {doc!r}")

    def log(self, sid: str) -> str:
        with open(f"{self.dir}/{sid}.log", errors="replace") as f:
            return f.read()

    def close(self) -> None:
        for p in list(self.procs.values()) + ([self._etcd_proc] if self._etcd_proc else []):
            p.terminate()
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        shutil.rmtree(self.dir, ignore_errors=True)


@pytest.fixture
def cluster():
    c = Cluster()
    try:
        yield c
    finally:
        c.close()


def ring_of(sids) -> ob._ConsistentHashRing:
    ring = ob._ConsistentHashRing()
    for sid in sids:
        ring.add_shard(sid, 150)
    return ring


def symbols_of(owner: str, sids, n: int = 3) -> list[str]:
    ring = ring_of(sids)
    out = [f"S{i}" for i in range(2000) if ring.lookup(f"S{i}.{EXCHANGE}") == owner]
    assert len(out) >= n, f"not {n} symbols hash to {owner}"
    return out[:n]


def command(cluster: Cluster, sid: str, line: str) -> str:
    """One command straight to node `sid`: the first line it answers."""
    with socket.create_connection(("127.0.0.1", cluster.ports[sid]), timeout=10) as s:
        f = s.makefile("rb")
        while f.readline().strip():       # the greeting
            pass
        s.sendall(f"{line}\n".encode())
        return f.readline().decode(errors="replace").strip()


def insert(cluster: Cluster, sid: str, symbol: str) -> str:
    """One INSERT straight to shard `sid`: "OK", or the error it answers."""
    return command(cluster, sid, f"INSERT {symbol} {EXCHANGE} bid 100 1 1")


def count_rows(cluster: Cluster, sid: str, symbol: str) -> int:
    """How many rows of `symbol` shard `sid` holds, after a FLUSH of it."""
    with socket.create_connection(("127.0.0.1", cluster.ports[sid]), timeout=10) as s:
        f = s.makefile("rb")
        while f.readline().strip():
            pass
        s.sendall(b"FLUSH\n")
        assert f.readline().strip() == b"OK"
        f.readline()
        s.sendall(f"SELECT price FROM '{symbol}'.'{EXCHANGE}' WHERE timestamp BETWEEN 0 AND "
                  f"9999999999999999999\n".encode())
        n = 0
        while True:
            line = f.readline().decode(errors="replace").strip()
            if not line or line.startswith("ERR"):
                return n
            if line.isdigit():
                n += 1


def wait_until_writable(cluster: Cluster, sid: str, symbol: str) -> str:
    deadline = time.time() + patience(20)
    answer = ""
    while time.time() < deadline:
        answer = insert(cluster, sid, symbol)
        if answer == "OK":
            return answer
        time.sleep(0.3)
    return answer


def test_two_shards_starting_together_are_both_in_the_map(cluster):
    cluster.start("s0", wait=False)
    cluster.start("s1", wait=False)
    doc = cluster.wait_for_map(("s0", "s1"))
    assert doc["shards"]["s0"]["address"] == cluster.address("s0")
    assert doc["shards"]["s1"]["address"] == cluster.address("s1")


def refused_as_not_owned(cluster: Cluster, sid: str, symbol: str) -> str:
    """What `sid` answers a write of `symbol` once it has read the map the other shard joined: a
    shard reads the map every 2 s, and one that started first learns of the second at its next read."""
    deadline = time.time() + patience(20)
    answer = ""
    while time.time() < deadline:
        answer = insert(cluster, sid, symbol)
        if answer.startswith("ERR NOT_OWNER"):
            return answer
        time.sleep(0.3)
    return answer


def test_each_symbol_is_owned_by_the_one_shard_the_ring_gives_it(cluster):
    for sid in ("s0", "s1"):
        cluster.start(sid)
    cluster.wait_for_map(("s0", "s1"))
    for owner, other in (("s0", "s1"), ("s1", "s0")):
        for sym in symbols_of(owner, ("s0", "s1")):
            assert refused_as_not_owned(cluster, other, sym) == f"ERR NOT_OWNER {sym}.{EXCHANGE}", (
                f"{other} takes {sym}, which the ring gives {owner}")
            assert wait_until_writable(cluster, owner, sym) == "OK", f"{owner} refused {sym}"


def test_each_shard_elects_its_own_primary(cluster):
    # Two shards shared /ob/leader: the second became the first one's replica and refused writes.
    for sid in ("s0", "s1"):
        cluster.start(sid)
    cluster.wait_for_map(("s0", "s1"))
    deadline = time.time() + patience(20)
    while time.time() < deadline and not all(cluster.get(f"{PREFIX}shards/{s}/leader")
                                             for s in ("s0", "s1")):
        time.sleep(0.2)
    for sid in ("s0", "s1"):
        leader = cluster.get(f"{PREFIX}shards/{sid}/leader")
        assert leader and json.loads(leader)["node_id"] == f"node-{sid}", (sid, leader)
    assert cluster.get(f"{PREFIX}leader") is None, "a shard campaigned for the cluster-wide key"
    for sid in ("s0", "s1"):
        sym = symbols_of(sid, ("s0", "s1"), 1)[0]
        assert wait_until_writable(cluster, sid, sym) == "OK", f"{sid} is not writable"


def test_the_python_pool_writes_each_symbol_to_its_owner(cluster):
    for sid in ("s0", "s1"):
        cluster.start(sid)
    cluster.wait_for_map(("s0", "s1"))
    wanted = {sid: symbols_of(sid, ("s0", "s1")) for sid in ("s0", "s1")}
    for sid, syms in wanted.items():
        refused_as_not_owned(cluster, "s1" if sid == "s0" else "s0", syms[0])
        wait_until_writable(cluster, sid, syms[0])
    pool = OrderbookEngine(hosts=[cluster.address("s0")], coordinator_endpoints=[cluster.etcd],
                           health_check_interval=0.5)
    try:
        for syms in wanted.values():
            for sym in syms:
                pool.insert(sym, EXCHANGE, "ask", [200], [2], timestamp_ns=1_700_000_000_000_000_001)
    finally:
        pool.close()
    for sid, syms in wanted.items():
        for i, sym in enumerate(syms):
            # The first of each was also the probe that waited for the shard to take writes.
            assert count_rows(cluster, sid, sym) == (2 if i == 0 else 1), (
                f"{sym} did not reach its owner {sid} through the pool")


def test_a_shard_that_joins_later_is_in_every_ring(cluster):
    for sid in ("s0", "s1"):
        cluster.start(sid)
    cluster.wait_for_map(("s0", "s1"))
    cluster.start("s2")
    cluster.wait_for_map(("s0", "s1", "s2"))
    sym = symbols_of("s2", ("s0", "s1", "s2"), 1)[0]
    assert wait_until_writable(cluster, "s2", sym) == "OK"
    deadline = time.time() + patience(20)
    answers = {}
    while time.time() < deadline:
        answers = {sid: insert(cluster, sid, sym) for sid in ("s0", "s1")}
        if all(a.startswith("ERR NOT_OWNER") for a in answers.values()):
            break
        time.sleep(0.5)
    assert all(a.startswith("ERR NOT_OWNER") for a in answers.values()), (
        f"the first two shards did not take the third into their ring: {answers}")


def test_the_cpp_pool_writes_each_symbol_to_its_owner(cluster):
    # The C++ router read nothing from etcd and answered success, so a pool built from coordinator
    # endpoints routed every symbol to the empty shard id.
    harness = cpp_client_binary_path()
    # Not a skip: both integration jobs build the harness beside the server, and a skip reads as a
    # pass in a summary line.
    assert harness, "ob_integration_test was not built beside the server"
    for sid in ("s0", "s1"):
        cluster.start(sid)
    cluster.wait_for_map(("s0", "s1"))
    wanted = {sid: symbols_of(sid, ("s0", "s1")) for sid in ("s0", "s1")}
    for sid, syms in wanted.items():
        refused_as_not_owned(cluster, "s1" if sid == "s0" else "s0", syms[0])
        wait_until_writable(cluster, sid, syms[0])
    out = subprocess.run([harness, "--test", "shard_pool", "--coordinator", cluster.etcd,
                          "--symbols", ",".join(s for syms in wanted.values() for s in syms)],
                         capture_output=True, text=True, timeout=patience(60))
    assert out.returncode == 0, out.stdout + out.stderr
    for sid, syms in wanted.items():
        for i, sym in enumerate(syms):
            assert count_rows(cluster, sid, sym) == (2 if i == 0 else 1), (
                f"{sym} did not reach its owner {sid} through the C++ pool: {out.stdout}")


def test_a_node_publishes_the_host_it_is_reached_by(cluster):
    # #195: the leader key a replica dials, and the map a client routes by, said 127.0.0.1 whatever
    # the host - a replica on another machine dialled itself.
    cluster.start("s0", host="127.0.0.2")
    doc = cluster.wait_for_map(("s0",))
    assert doc["shards"]["s0"]["address"] == f"127.0.0.2:{cluster.ports['s0']}"
    deadline = time.time() + patience(20)
    leader = None
    while time.time() < deadline and not leader:
        leader = cluster.get(f"{PREFIX}shards/s0/leader")
        time.sleep(0.2)
    assert leader, "shard s0 elected no primary"
    assert json.loads(leader)["address"] == f"127.0.0.2:{cluster.repl_ports['s0']}", leader


def test_a_replica_of_a_shards_group_leaves_the_map_naming_its_primary(cluster):
    # The map names the node a client writes to: a shard's primary. Its replica starts as one - the
    # group's leader key is there - and writes nothing into the map.
    cluster.start("s0a", shard="s0")
    doc = cluster.wait_for_map(("s0",))
    assert doc["shards"]["s0"]["address"] == cluster.address("s0a")
    cluster.start("s0b", shard="s0")
    deadline = time.time() + patience(10)
    seen = set()
    while time.time() < deadline:          # several of each node's reads of the map
        seen.add(cluster.shard_map()["shards"]["s0"]["address"])
        time.sleep(0.25)
    assert seen == {cluster.address("s0a")}, f"the map named {seen} for shard s0"
    assert insert(cluster, "s0b", "ANY") == "ERR read-only replica"


def test_migrate_is_refused_and_the_symbol_stays_writable(cluster):
    # #196: MIGRATE marked the symbol migrated and moved none of its rows, and the map still named
    # the source - so the symbol could be written nowhere. Refused, until it moves data.
    for sid in ("s0", "s1"):
        cluster.start(sid)
    doc = cluster.wait_for_map(("s0", "s1"))
    doc["assignments"] = {f"PIN.{EXCHANGE}": "s0"}
    doc["version"] += 1
    cluster.put(f"{PREFIX}shard_map", json.dumps(doc))
    answer = ""
    deadline = time.time() + patience(20)
    while time.time() < deadline:          # until s0 has read the assignment
        answer = command(cluster, "s0", f"MIGRATE PIN.{EXCHANGE} s1")
        if "not implemented" in answer:
            break
        time.sleep(0.3)
    assert answer.startswith("ERR MIGRATE is not implemented"), answer
    assert wait_until_writable(cluster, "s0", "PIN") == "OK", "the symbol MIGRATE was refused for is not writable"


def test_the_pool_reads_a_bare_symbol_migrated():
    # The server says it bare; the pool matched only the form with a detail, so the refresh and
    # the retry it exists for never happened against a real server.
    assert ob._parse_shard_error("ERR SYMBOL_MIGRATED\n") == ("SYMBOL_MIGRATED", "")
    assert ob._parse_shard_error("ERR SYMBOL_MIGRATED s1") == ("SYMBOL_MIGRATED", "s1")
    assert ob._parse_shard_error(f"ERR NOT_OWNER A.{EXCHANGE}") == ("NOT_OWNER", f"A.{EXCHANGE}")


def test_a_mesh_node_registers_the_host_it_is_reached_by(cluster):
    # #195 in the mesh: a node registered 127.0.0.1 for its peers, so a mesh across hosts dialled
    # itself and never formed.
    port, mm_port = free_port(), free_port()
    cluster.ports["mm1"] = port
    log = open(f"{cluster.dir}/mm1.log", "ab")
    cluster.procs["mm1"] = subprocess.Popen(
        [SERVER, "--port", str(port), "--metrics-port", "0", "--data-dir", f"{cluster.dir}/mm1",
         "--multi-master", "--mm-node-id", "1", "--mm-replication-port", str(mm_port),
         "--node-id", "mm-1", "--coordinator-endpoints", cluster.etcd, "--advertise-host", "127.0.0.2"],
        stdout=log, stderr=subprocess.STDOUT)
    cluster._wait_port(port)
    deadline = time.time() + patience(20)
    registered = None
    while time.time() < deadline and not registered:
        registered = cluster.get(f"{PREFIX}mm_peers/1")
        time.sleep(0.2)
    assert registered, "the mesh node registered nothing"
    assert json.loads(registered)["address"] == f"127.0.0.2:{mm_port}", registered
