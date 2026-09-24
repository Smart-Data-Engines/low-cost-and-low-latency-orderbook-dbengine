"""The Python client's sharded pool, against a shard map in a real etcd (#172).

Nothing constructed a pool with `coordinator_endpoints` before these, so what the pool does while its
health check replaces its routing under its callers had no instrument. Three things it did, each
held here by a test that fails against the pool before #172:

- a lookup during a map refresh routed by a **partial hash ring** - `_rebuild_hash_ring()` published
  an empty ring and then filled it;
- a fan-out query raised `RuntimeError: dictionary changed size during iteration` when a refresh
  deleted a shard from the dictionary it was iterating;
- a shard connection #171 closed after a timeout stayed closed until the map changed, which it may
  never do.

**The shards are two plain nodes, and the map is this module's to write.** A node started with
`--shard-id` writes neither itself nor the shard map to etcd, and two of them on one etcd share one
leader key, so the second becomes the first one's replica and refuses writes (#175). So the nodes run
standalone, and the map is written here in the shape `ShardMap::to_json()` produces - what is under
test is the client.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

import pytest
from conftest import free_port, patience, server_binary_path

import orderbook_engine as ob
from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.pool

SERVER = server_binary_path()
PREFIX = "/ob/"


class Shards:
    """A native etcd and two standalone nodes, `s0` and `s1`, and the shard map naming them."""

    def __init__(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="ob_sharded_pool_")
        self.procs: list[subprocess.Popen] = []
        self.ports: list[int] = []
        self.version = 0
        # Whatever started is stopped if the rest does not: a fixture that never gets its object
        # never reaches its teardown (pitfall 443).
        try:
            client, peer = free_port(), free_port()
            self.etcd = f"http://127.0.0.1:{client}"
            self._spawn([os.environ.get("OB_ETCD_BINARY") or "etcd", "--data-dir", f"{self.dir}/etcd",
                         "--listen-client-urls", self.etcd, "--advertise-client-urls", self.etcd,
                         "--listen-peer-urls", f"http://127.0.0.1:{peer}",
                         "--initial-advertise-peer-urls", f"http://127.0.0.1:{peer}",
                         "--initial-cluster", f"default=http://127.0.0.1:{peer}"], client)
            for sid in ("s0", "s1"):
                port = free_port()
                self._spawn([SERVER, "--port", str(port), "--metrics-port", "0",
                             "--data-dir", f"{self.dir}/{sid}"], port)
                self.ports.append(port)
        except BaseException:
            self.close()
            raise

    def _spawn(self, argv: list[str], port: int) -> None:
        self.procs.append(subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        deadline = time.time() + patience(20)
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f"{argv[0]} on port {port} did not come up")

    def address(self, index: int) -> str:
        return f"127.0.0.1:{self.ports[index]}"

    def put_map(self, shards: tuple[str, ...] = ("s0", "s1"),
                assignments: dict[str, str] | None = None) -> None:
        """A new version of `<prefix>shard_map`, naming `shards` at their nodes' addresses."""
        self.version += 1
        doc = {"active_migrations": [], "assignments": assignments or {}, "pinned_symbols": [],
               "shards": {sid: {"address": self.address(int(sid[1])), "mm_nodes": [],
                                "shard_id": sid, "status": "active", "vnodes": 150}
                          for sid in shards},
               "version": self.version}
        body = json.dumps({"key": base64.b64encode(f"{PREFIX}shard_map".encode()).decode(),
                           "value": base64.b64encode(json.dumps(doc).encode()).decode()}).encode()
        req = urllib.request.Request(f"{self.etcd}/v3/kv/put", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200

    def prices(self, index: int, symbol: str) -> list[int]:
        """The prices of `symbol`'s rows on node `index`, after a FLUSH of that node."""
        with socket.create_connection(("127.0.0.1", self.ports[index]), timeout=10) as s:
            f = s.makefile("rb")
            while f.readline().strip():
                pass
            s.sendall(b"FLUSH\n")
            assert f.readline().strip() == b"OK"
            f.readline()
            s.sendall(f"SELECT price FROM '{symbol}'.'EX' WHERE timestamp BETWEEN 0 AND "
                      f"9999999999999999999\n".encode())
            out = []
            while True:
                line = f.readline().decode(errors="replace").strip()
                if not line:
                    return out
                if line.startswith("ERR"):
                    return []
                if line.isdigit():
                    out.append(int(line))

    def close(self) -> None:
        for p in reversed(self.procs):
            p.terminate()
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                p.kill()
        shutil.rmtree(self.dir, ignore_errors=True)


@pytest.fixture
def shards():
    s = Shards()
    try:
        yield s
    finally:
        s.close()


def engine(shards: Shards, interval: float) -> OrderbookEngine:
    return OrderbookEngine(hosts=[shards.address(0)], coordinator_endpoints=[shards.etcd],
                           health_check_interval=interval)


def full_ring_owner(symbol: str) -> str:
    ring = ob._ConsistentHashRing()
    ring.add_shard("s0", 150)
    ring.add_shard("s1", 150)
    return ring.lookup(f"{symbol}.EX")


def symbol_owned_by(owner: str) -> str:
    """A symbol the whole ring gives to `owner`; the ring with `s0` alone gives every one to `s0`."""
    for n in range(1000):
        sym = f"S{n}"
        if full_ring_owner(sym) == owner:
            return sym
    raise AssertionError("no symbol hashes to " + owner)


def shard_backend(pool, sid: str):
    """The pool's connection to shard `sid`, wherever this version of the pool keeps it."""
    routing = getattr(pool, "_routing", None)
    if routing is not None:
        return routing.connections.get(sid)
    return pool._shard_connections.get(sid)


def test_the_pool_writes_each_symbol_to_the_shard_its_map_names(shards):
    # The instrument's own premise: a map in etcd, two nodes behind it, and a write per symbol that
    # lands on the shard the map assigns it - or, unassigned, the one the whole ring gives it.
    unassigned = symbol_owned_by("s1")
    shards.put_map(assignments={"AAA.EX": "s0", "BBB.EX": "s1"})
    eng = engine(shards, 3600)
    try:
        eng.insert("AAA", "EX", "bid", [111], [1], [1])
        eng.insert("BBB", "EX", "bid", [222], [1], [1])
        eng.insert(unassigned, "EX", "bid", [333], [1], [1])
    finally:
        eng.close()
    assert (shards.prices(0, "AAA"), shards.prices(1, "AAA")) == ([111], [])
    assert (shards.prices(0, "BBB"), shards.prices(1, "BBB")) == ([], [222])
    assert (shards.prices(0, unassigned), shards.prices(1, unassigned)) == ([], [333])


def test_a_write_during_a_map_refresh_is_routed_by_a_whole_ring(shards, monkeypatch):
    # The refresh is made to stop after it has added s0 to the ring it is building, and a symbol the
    # whole ring gives to s1 is written meanwhile. Routed by the ring being built - s0 alone - it
    # lands on s0.
    sym = symbol_owned_by("s1")
    shards.put_map()
    eng = engine(shards, 0.2)
    paused, resume = threading.Event(), threading.Event()
    original = ob._ConsistentHashRing.add_shard

    def add_shard_then_pause(self, shard_id, vnodes=150):
        original(self, shard_id, vnodes)
        if shard_id == "s0" and not paused.is_set():
            paused.set()
            resume.wait(patience(10))

    try:
        monkeypatch.setattr(ob._ConsistentHashRing, "add_shard", add_shard_then_pause)
        shards.put_map()   # a new version, the same shards: the health check rebuilds
        assert paused.wait(patience(10)), "the health check never rebuilt the ring"
        eng.insert(sym, "EX", "bid", [444], [1], [1])
    finally:
        resume.set()
        monkeypatch.undo()
        eng.close()
    assert (shards.prices(0, sym), shards.prices(1, sym)) == ([], [444]), (
        "a write during the refresh was routed by the half-built ring")


def test_a_fan_out_query_does_not_raise_when_a_refresh_removes_a_shard(shards):
    # The query is held on its first shard until the health check has taken s1 out of the map; then
    # it goes on to the next. Iterating the dictionary the refresh deleted from, it raised.
    shards.put_map()
    eng = engine(shards, 0.2)
    pool = eng._pool
    first = shard_backend(pool, "s0")
    held, refreshed = threading.Event(), threading.Event()
    real_execute = first.execute

    def held_execute(command, *args, **kwargs):
        if command.startswith("SELECT") and not held.is_set():
            held.set()
            refreshed.wait(patience(10))
            return "ERR held by the test"
        return real_execute(command, *args, **kwargs)

    first.execute = held_execute
    outcome: list = []

    def fan_out():
        try:
            outcome.append(eng.query("SELECT price FROM everything"))
        except Exception as exc:  # noqa: BLE001 - which exception is the question
            outcome.append(exc)

    caller = threading.Thread(target=fan_out)
    try:
        caller.start()
        assert held.wait(patience(10)), "the fan-out never reached the first shard"
        shards.put_map(shards=("s0",))
        deadline = time.time() + patience(10)
        while shard_backend(pool, "s1") is not None and time.time() < deadline:
            time.sleep(0.02)
        assert shard_backend(pool, "s1") is None, "the refresh never took s1 out"
        refreshed.set()
        caller.join(patience(20))
    finally:
        refreshed.set()
        eng.close()
    assert outcome, "the fan-out never returned"
    assert not isinstance(outcome[0], RuntimeError), (
        f"a map refresh raised in the fan-out's caller: {outcome[0]!r}")


def test_a_shard_connection_a_timeout_closed_comes_back_at_the_next_health_check(shards):
    # #171 closes a connection whose exchange did not finish, and the next command on it says so.
    # The pool is to replace it at its health check - not wait for a map change that may never come.
    shards.put_map(assignments={"CCC.EX": "s1"})
    eng = engine(shards, 0.2)
    try:
        shard_backend(eng._pool, "s1")._abandon("the test timed it out", quietly=True)
        deadline = time.time() + patience(10)
        last = None
        while time.time() < deadline:
            try:
                eng.insert("CCC", "EX", "bid", [555], [1], [1])
                last = None
                break
            except OrderbookError as exc:
                last = exc
                time.sleep(0.1)
        assert last is None, f"the closed shard connection never came back: {last}"
    finally:
        eng.close()
    assert shards.prices(1, "CCC") == [555]


def test_a_pool_closed_during_a_refresh_leaves_no_shard_connection_open(shards, monkeypatch):
    # close() while the health check is opening a shard's connection: the refresh must not publish
    # connections after close() has closed the last ones, which nothing would ever close again.
    shards.put_map(shards=("s0",))
    eng = engine(shards, 0.2)
    pool = eng._pool
    opening, closing = threading.Event(), threading.Event()
    real_open = pool._open_shard

    def slow_open(sid, info):
        if sid == "s1":
            opening.set()
            closing.wait(patience(10))
        return real_open(sid, info)

    monkeypatch.setattr(pool, "_open_shard", slow_open)
    shards.put_map()   # s1 joins: the next health check opens it
    assert opening.wait(patience(10)), "the health check never opened s1"
    closer = threading.Thread(target=eng.close)
    closer.start()
    time.sleep(0.2)   # close() is waiting for the refresh, or has already closed what it saw
    closing.set()
    closer.join(patience(20))
    time.sleep(0.5)   # the health check's refresh, if it outlived close(), has published by now
    left = [sid for sid in ("s0", "s1")
            if (b := shard_backend(pool, sid)) is not None and not pool._shard_connection_closed(b)]
    assert left == [], f"shard connections open after close(): {left}"


class MigratedShard:
    """A shard connection that answers every write as a shard the symbol has moved away from."""

    _host, _port, _closed_because, _sock = "127.0.0.1", 0, None, object()

    def execute(self, command, *args, **kwargs):
        return "ERR SYMBOL_MIGRATED AAA.EX s1"

    def close(self):
        pass


def test_a_migrated_symbol_with_etcd_unreachable_keeps_the_routing_there_is(shards):
    # SYMBOL_MIGRATED makes the pool fetch the map and retry once. With etcd gone the fetch is
    # empty, and routing by an empty map would leave every shard unreachable until the next fetch.
    shards.put_map(assignments={"AAA.EX": "s0"})
    eng = engine(shards, 3600)
    pool = eng._pool
    try:
        routing = pool._routing
        connections = dict(routing.connections)
        connections["s0"] = MigratedShard()
        pool._routing = ob._ShardRouting(routing.shard_map, routing.ring, connections)
        shards.procs[0].terminate()
        shards.procs[0].wait(timeout=20)
        try:
            eng.insert("AAA", "EX", "bid", [666], [1], [1])
        except OrderbookError:
            pass   # the retry met the same shard; what matters is what the pool routes by after
        assert shard_backend(pool, "s1") is not None, (
            "an empty map fetched while etcd was unreachable replaced the routing")
    finally:
        eng.close()
