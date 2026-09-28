"""A symbol moved between shards with its rows (#196).

`MIGRATE` marked a symbol migrated on its shard and moved none of its rows, and since #175 - the map in
etcd, which every client routes by - it was refused. Here it moves them: the target holds every row the
source held and every write acknowledged during the move, once each, with the same times, sides,
levels, prices, quantities and counts; the map names the target; the source refuses the symbol; and a
pool writing throughout does not see one failure. A target that dies during the copy leaves the
symbol where it was and writable, and the move tried again - over what the dead attempt left, once an
operator has dropped it - stores nothing twice.

Each test starts its own etcd and two servers, as `test_shard_control_plane.py` does.
"""
from __future__ import annotations

import collections
import json
import socket
import subprocess
import threading
import time

import pytest

import orderbook_engine as ob
from conftest import cpp_client_binary_path, free_port, patience, server_binary_path
from orderbook_engine import OrderbookEngine
from test_shard_control_plane import (EXCHANGE, PREFIX, Cluster, command, insert,
                                      refused_as_not_owned, symbols_of, wait_until_writable)

pytestmark = pytest.mark.pool
SERVER = server_binary_path()
APP_SECRET = "0123456789abcdef0123456789abcdef-app"
MOVER_SECRET = "fedcba9876543210fedcba9876543210-mover"
LEVELS = 5
T0 = 1_760_000_000_000_000_000   # the history's event times
T1 = 1_760_000_100_000_000_000   # the writer's, after it


@pytest.fixture
def cluster():
    c = Cluster()
    try:
        yield c
    finally:
        c.close()


def rows_of(cluster: Cluster, sid: str, symbol: str) -> list[tuple]:
    """Every row of `symbol` on `sid` after a FLUSH: (time, side, level, price, quantity, count)."""
    with socket.create_connection(("127.0.0.1", cluster.ports[sid]), timeout=30) as s:
        f = s.makefile("rb")
        while f.readline().strip():
            pass
        s.sendall(b"FLUSH\n")
        assert f.readline().strip() == b"OK"
        f.readline()
        s.sendall(f"SELECT * FROM '{symbol}'.'{EXCHANGE}' WHERE timestamp BETWEEN 0 AND "
                  f"9999999999999999999\n".encode())
        first = f.readline().decode().strip()
        if first.startswith("ERR"):
            return []
        assert first == "OK", first
        header = f.readline().decode().strip().split("\t")
        col = {name: i for i, name in enumerate(header)}
        out = []
        while True:
            line = f.readline().decode().strip()
            if not line:
                return out
            v = line.split("\t")
            out.append((int(v[col["timestamp_ns"]]), v[col["side"]], int(v[col["level"]]),
                        int(v[col["price"]]), int(v[col["quantity"]]), int(v[col["order_count"]])))


def shard_info(cluster: Cluster, sid: str) -> dict:
    with socket.create_connection(("127.0.0.1", cluster.ports[sid]), timeout=10) as s:
        f = s.makefile("rb")
        while f.readline().strip():
            pass
        s.sendall(b"SHARD_INFO\n")
        assert f.readline().strip() == b"OK"
        info = {}
        while True:
            line = f.readline().decode().rstrip("\n")
            if not line:
                return info
            key, _, value = line.partition("\t")
            info[key] = value


def migration_ended(cluster: Cluster, sid: str) -> dict:
    deadline = time.time() + patience(120)
    info = {}
    while time.time() < deadline:
        info = shard_info(cluster, sid)
        if info.get("migration_phase") in ("done", "failed", "unknown"):
            return info
        time.sleep(0.05)
    raise AssertionError(f"the migration did not end: {info}")


def write_history(pool: OrderbookEngine, symbol: str, n: int) -> None:
    """`n` updates of LEVELS levels each, at their own event times, on both sides."""
    for i in range(n):
        pool.insert(symbol, EXCHANGE, "bid" if i % 2 == 0 else "ask",
                    [10_000 + 10 * i + k for k in range(LEVELS)], [1 + k for k in range(LEVELS)],
                    timestamp_ns=T0 + 1_000 * i)


def moved_symbol(cluster: Cluster) -> tuple[str, str]:
    for sid in ("s0", "s1"):
        cluster.start(sid)
    cluster.wait_for_map(("s0", "s1"))
    # Until each shard has read the map with the other in it: MIGRATE names a shard of the map the
    # source holds, which it reads every 2 s.
    for sid, other in (("s0", "s1"), ("s1", "s0")):
        theirs = symbols_of(other, ("s0", "s1"), 1)[0]
        assert refused_as_not_owned(cluster, sid, theirs).startswith("ERR NOT_OWNER")
    sym = symbols_of("s0", ("s0", "s1"), 1)[0]
    assert wait_until_writable(cluster, "s0", sym) == "OK"
    return sym, f"{sym}.{EXCHANGE}"


def test_a_symbol_moves_with_every_row_while_it_is_written(cluster, tmp_path):
    # Written throughout by two writers, the Python pool and the C++ one, each routing by the map
    # and neither seeing a failure.
    harness = cpp_client_binary_path()
    assert harness, "ob_integration_test was not built beside the server"
    sym, key = moved_symbol(cluster)
    pool = OrderbookEngine(hosts=[cluster.address("s0")], coordinator_endpoints=[cluster.etcd],
                           health_check_interval=0.5)
    acked: list[int] = []
    failed: list[str] = []
    stop = threading.Event()

    def writer() -> None:
        i = 0
        while not stop.is_set():
            ts = T1 + 1_000 * i
            try:
                pool.insert(sym, EXCHANGE, "bid", [20_000 + i], [1], timestamp_ns=ts)
                acked.append(ts)
            except Exception as e:   # noqa: BLE001 - every failure is the finding
                failed.append(f"{ts}: {e!r}")
            i += 1

    until, cpp_acked_file = tmp_path / "stop", tmp_path / "cpp-acked"
    try:
        write_history(pool, sym, 4_000)
        thread = threading.Thread(target=writer)
        thread.start()
        cpp = subprocess.Popen([harness, "--test", "shard_writer", "--coordinator", cluster.etcd,
                                "--symbols", sym, "--until", str(until), "--out", str(cpp_acked_file)],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            time.sleep(0.5)
            assert command(cluster, "s0", f"MIGRATE {key} s1") == "OK"
            info = migration_ended(cluster, "s0")
            time.sleep(1.0)   # writes after the move, which both pools send to the target
        finally:
            stop.set()
            thread.join()
            until.touch()
            cpp_out = cpp.communicate(timeout=patience(60))[0]
    finally:
        pool.close()
    assert cpp.returncode == 0, cpp_out
    cpp_acked = [int(ts) for ts in cpp_acked_file.read_text().split()]
    assert cpp_acked, cpp_out

    assert info["migration_phase"] == "done", info
    assert failed == [], f"{len(failed)} write(s) failed during the move: {failed[:3]}"
    assert int(info["migration_rounds"]) >= 2, (
        f"the history alone is more than a round's worth, and one round copied it all: {info}")
    acked = acked + cpp_acked
    assert cluster.shard_map()["assignments"].get(key) == "s1", "the map does not name the target"
    source = collections.Counter(rows_of(cluster, "s0", sym))
    target = collections.Counter(rows_of(cluster, "s1", sym))
    missing = source - target
    assert not missing, f"{sum(missing.values())} row(s) the source held are not on the target"
    extra = target - source
    # What the target holds besides: the writes acknowledged after the switch, once each.
    after = set(acked) - {row[0] for row in source}
    assert all(count == 1 and row[0] in after for row, count in extra.items()), (
        "the target holds rows twice, or rows nobody wrote")
    assert {row[0] for row in extra} == after, "a write acknowledged after the switch is not on the target"
    assert insert(cluster, "s0", sym).startswith(("ERR SYMBOL_MIGRATED", "ERR NOT_OWNER"))
    print(f"\nmoved {sum(source.values())} row(s) in {info['migration_rounds']} round(s); writes "
          f"refused SYMBOL_MOVING for {info['migration_freeze_ms']} ms; {len(acked)} written during "
          f"and after the move ({len(cpp_acked)} by the C++ pool), {len(after)} of them to the "
          f"target")


def test_a_switch_the_map_refuses_leaves_nothing_on_a_live_target(cluster):
    # The map changed under the move - an operator assigned the symbol elsewhere - so the switch is
    # refused: the source abandons the adoption, and the target, alive, drops what it stored. The
    # move tried again needs no operator to clean up after it.
    sym, key = moved_symbol(cluster)
    pool = OrderbookEngine(hosts=[cluster.address("s0")], coordinator_endpoints=[cluster.etcd],
                           health_check_interval=0.5)
    try:
        write_history(pool, sym, 20_000)
    finally:
        pool.close()
    held = collections.Counter(rows_of(cluster, "s0", sym))

    assert command(cluster, "s0", f"MIGRATE {key} s1") == "OK"
    deadline = time.time() + patience(60)
    while time.time() < deadline:
        info = shard_info(cluster, "s0")
        if info.get("migration_phase") == "copying" and int(info.get("migration_updates", 0)) > 0:
            break
        assert info.get("migration_phase") in ("adopting", "copying"), info
        time.sleep(0.01)
    doc = cluster.shard_map()
    doc["assignments"][key] = "s9"   # nobody's
    doc["version"] += 1
    cluster.put(f"{PREFIX}shard_map", json.dumps(doc))
    info = migration_ended(cluster, "s0")
    assert info["migration_phase"] == "failed", info
    assert "names shard s9" in info["migration_error"], info
    assert rows_of(cluster, "s1", sym) == [], "the target kept what the abandoned move stored"

    doc = cluster.shard_map()
    doc["assignments"][key] = "s0"
    doc["version"] += 1
    cluster.put(f"{PREFIX}shard_map", json.dumps(doc))
    assert wait_until_writable(cluster, "s0", sym) == "OK", "the symbol stayed refused after a failure"
    assert command(cluster, "s0", f"MIGRATE {key} s1") == "OK"
    info = migration_ended(cluster, "s0")
    assert info["migration_phase"] == "done", info
    source = collections.Counter(rows_of(cluster, "s0", sym))
    assert not held - source
    assert collections.Counter(rows_of(cluster, "s1", sym)) == source


def test_a_symbol_moves_between_shards_that_authenticate_their_clients(cluster, tmp_path):
    # With client authentication the source is a client of the target like any other, and signs in
    # as its --migration-identity, from its own secret file.
    secrets = tmp_path / "clients"
    secrets.write_text(f"app {APP_SECRET}\nmover {MOVER_SECRET}\n")
    secrets.chmod(0o600)
    auth = ("--auth-secret-file", str(secrets))
    cluster.start("s0", extra=auth + ("--migration-identity", "mover"))
    cluster.start("s1", extra=auth)
    cluster.wait_for_map(("s0", "s1"))
    sym = symbols_of("s0", ("s0", "s1"), 1)[0]
    key = f"{sym}.{EXCHANGE}"
    pool = OrderbookEngine(hosts=[cluster.address("s0")], coordinator_endpoints=[cluster.etcd],
                           health_check_interval=0.5, auth=("app", APP_SECRET))
    admin = ob._TcpBackend("127.0.0.1", cluster.ports["s0"], auth=("app", APP_SECRET))
    try:
        deadline = time.time() + patience(20)
        while time.time() < deadline:   # until s0 has read the map with s1 in it
            try:
                write_history(pool, sym, 200)
                answer = admin.execute(f"MIGRATE {key} s1")
            except ob.OrderbookError as e:
                answer = str(e)
            if answer.startswith("OK"):
                break
            time.sleep(0.3)
        assert answer.startswith("OK"), answer
        deadline = time.time() + patience(60)
        info = {}
        while time.time() < deadline:
            raw = admin.execute("SHARD_INFO")
            info = dict(line.split("\t", 1) for line in raw.splitlines()[1:] if "\t" in line)
            if info.get("migration_phase") in ("done", "failed", "unknown"):
                break
            time.sleep(0.05)
        assert info.get("migration_phase") == "done", info
        pool.insert(sym, EXCHANGE, "bid", [1], [1], timestamp_ns=T1)   # to the target, by the map
    finally:
        admin.close()
        pool.close()
    assert cluster.shard_map()["assignments"].get(key) == "s1"


def test_a_migration_identity_the_secret_file_lacks_refuses_to_start(tmp_path):
    secrets = tmp_path / "clients"
    secrets.write_text(f"app {APP_SECRET}\n")
    secrets.chmod(0o600)
    out = subprocess.run([SERVER, "--port", str(free_port()), "--metrics-port", "0",
                          "--data-dir", str(tmp_path / "data"), "--auth-secret-file", str(secrets),
                          "--migration-identity", "mover"],
                         capture_output=True, text=True, timeout=patience(30))
    assert out.returncode == 1, out.stdout + out.stderr
    assert "--migration-identity 'mover' is not an identity in --auth-secret-file" in out.stderr, (
        out.stderr)


def test_a_target_that_dies_during_the_copy_leaves_the_symbol_where_it_was(cluster):
    sym, key = moved_symbol(cluster)
    pool = OrderbookEngine(hosts=[cluster.address("s0")], coordinator_endpoints=[cluster.etcd],
                           health_check_interval=0.5)
    try:
        write_history(pool, sym, 20_000)
    finally:
        pool.close()
    held = collections.Counter(rows_of(cluster, "s0", sym))

    assert command(cluster, "s0", f"MIGRATE {key} s1") == "OK"
    deadline = time.time() + patience(60)
    info = {}
    while time.time() < deadline:   # until the copy has begun: the target holds some of it
        info = shard_info(cluster, "s0")
        if info.get("migration_phase") == "copying" and int(info.get("migration_updates", 0)) > 0:
            break
        assert info.get("migration_phase") in ("adopting", "copying"), info
        time.sleep(0.01)
    cluster.procs["s1"].kill()
    cluster.procs["s1"].wait(timeout=10)
    info = migration_ended(cluster, "s0")
    assert info["migration_phase"] == "failed", info
    assert "s1" in cluster.shard_map()["shards"] and key not in cluster.shard_map()["assignments"], (
        "a failed move changed the map")
    assert insert(cluster, "s0", sym) == "OK", "the symbol a failed move left is not writable"

    # Back, with what the copy stored before it died: not adopted over.
    cluster.start("s1")
    cluster.wait_for_map(("s0", "s1"))
    deadline = time.time() + patience(20)
    while time.time() < deadline:   # until s0 has read s1's new address
        assert command(cluster, "s0", f"MIGRATE {key} s1") == "OK"
        info = migration_ended(cluster, "s0")
        if "cannot be reached" not in info.get("migration_error", ""):
            break
        time.sleep(0.5)
    assert info["migration_phase"] == "failed", info
    assert "holds rows" in info["migration_error"], info
    answer = command(cluster, "s1", f"ADOPT {key} ABANDON")
    assert answer.startswith("OK"), answer

    assert command(cluster, "s0", f"MIGRATE {key} s1") == "OK"
    info = migration_ended(cluster, "s0")
    assert info["migration_phase"] == "done", info
    source = collections.Counter(rows_of(cluster, "s0", sym))
    assert not held - source and sum((source - held).values()) == 1, (
        "the source holds other rows than the history and the one write after the failure")
    assert collections.Counter(rows_of(cluster, "s1", sym)) == source, (
        "the target does not hold exactly the source's rows: a row lost, or stored twice")
