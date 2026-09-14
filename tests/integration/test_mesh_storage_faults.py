"""What a mesh node does when the disk refuses the WAL write a peer's record needs — #112.

#112 closed the flush thread's half of "an escaping exception ends a thread rather than the
process" and named three loops it had not reached, `io_loop` among them. That was a prediction, so
this module starts from a measurement instead. With #54's injector armed on one node's WAL and an
`ENOSPC` on the 150-byte record a mesh delta writes (a 38-byte V2 header plus 112 bytes of payload,
where a client's own `INSERT` is the same payload behind the 24-byte legacy header):

    "msg":"io_loop ended on an exception and that thread is gone: WALWriter: write failed: ..."

The node stayed **alive**, answered `PING`, and its own `MM_PEERS` still called the link
`connected` with a fresh HLC and an empty send queue — while three later records written on the peer
never arrived. A node out of the mesh whose every outward signal says the mesh is fine, which is
this engine's name for a guarantee absent in production.

**The injector reaches exactly one node and nothing here had to change to allow it.** `_start_node`
inherits the environment of the pytest process, so arming it between starting the first node and
`add_multi_master_node()` puts the shim in the second node's process and no other — not the first
node, not etcd, not pytest. The alternative, `OB_FAULT_PATH` matching one node's data directory by
substring, would have worked too and is worse: it loads a shim into every process to have it match
in one.

The control is a second test rather than a second assertion, because the thing that must be
measured is that the same run with a size nothing writes injects **nothing** — a fault-injection
test that quietly failed to inject reads exactly like an engine that survived the fault.
"""

from __future__ import annotations

import contextlib
import os
import socket
import time

import pytest

from conftest import ClusterManager, fault_injector_path, patience, tail_node_log

pytestmark = pytest.mark.multi_master

SYMBOL = "MESHFAULT"
EXCHANGE = "EX"

# One mesh delta of one level as the receiving WAL writes it: a 38-byte V2 header (origin and HLC
# included, which is why this is not the client's 136) plus 112 bytes of payload.
MESH_DELTA_BYTES = "150"
QUERY_HEADER = "timestamp_ns\tprice\tquantity\torder_count\tside\tlevel\tsequence_number"


def raw(port: int, payload: str, settle: float = 0.25) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.sendall((payload if payload.endswith("\n") else payload + "\n").encode())
        time.sleep(settle)
        sock.settimeout(3.0)
        buf = b""
        try:
            while True:
                chunk = sock.recv(1 << 16)
                if not chunk:
                    break
                buf += chunk
        except socket.timeout:
            pass
    return buf.decode(errors="replace")


def metric(port: int, name: str) -> float:
    """One counter from /metrics by name, ignoring its label set (pitfall 66)."""
    import urllib.request
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        body = response.read().decode()
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        key, _, value = line.rpartition(" ")
        if key.split("{")[0] == name:
            return float(value)
    raise AssertionError(f"{name} is not in /metrics at all, which is not the same as zero")


def rows(port: int) -> list:
    """Stored rows for the test symbol. A mesh node may flush its own buffers, unlike a replica."""
    raw(port, "FLUSH", settle=0.4)
    reply = raw(port, f"SELECT * FROM '{SYMBOL}'.'{EXCHANGE}'")
    return [line for line in reply.splitlines()
            if line and not line.startswith("OK") and line != QUERY_HEADER]


def await_rows(port: int, count: int, timeout: float) -> list:
    deadline = time.monotonic() + timeout
    seen: list = []
    while time.monotonic() < deadline:
        seen = rows(port)
        if len(seen) >= count:
            return seen
        time.sleep(0.3)
    return seen


@contextlib.contextmanager
def _armed(fault_log: str, size: str):
    """Arm the injector for whatever this process spawns next, and only for that."""
    lib = fault_injector_path()
    assert lib, ("libobfault.so is not built, so this module would inject nothing and pass. "
                 "Build the `obfault` target; both integration jobs in CI do (#54).")
    before = {k: os.environ.get(k) for k in
              ("LD_PRELOAD", "OB_FAULT_PATH", "OB_FAULT_OP", "OB_FAULT_ERRNO",
               "OB_FAULT_SIZE", "OB_FAULT_SKIP", "OB_FAULT_COUNT", "OB_FAULT_LOG")}
    os.environ.update({
        "LD_PRELOAD": lib,
        "OB_FAULT_PATH": "wal_000000.bin",
        "OB_FAULT_OP": "write",
        "OB_FAULT_ERRNO": "ENOSPC",
        "OB_FAULT_SIZE": size,
        # Let the first matching write through: the mesh has to be carrying records before the
        # refusal means anything, and a node that never received one proves nothing about a node
        # that stopped.
        "OB_FAULT_SKIP": "1",
        "OB_FAULT_COUNT": "1",
        "OB_FAULT_LOG": fault_log,
    })
    try:
        yield
    finally:
        for key, value in before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _mesh_with_faulted_receiver(size: str, tmp_path):
    """Two mesh nodes; the second one's WAL refuses one write of `size` bytes."""
    fault_log = str(tmp_path / "fault.log")
    mgr = ClusterManager()
    mgr.start_multi_master(node_count=1)
    with _armed(fault_log, size):
        mgr.add_multi_master_node(timeout=patience(30))
    writer, receiver = mgr.nodes[0], mgr.nodes[1]

    deadline = time.monotonic() + patience(30)
    while time.monotonic() < deadline:
        if "connected" in raw(receiver.tcp_port, "MM_PEERS"):
            break
        time.sleep(0.25)
    assert "connected" in raw(receiver.tcp_port, "MM_PEERS"), (
        "the two nodes never formed a mesh, so nothing below measures a mesh")
    return mgr, writer, receiver, fault_log


def _injections(fault_log: str) -> list:
    if not os.path.exists(fault_log):
        return []
    with open(fault_log) as handle:
        return [line for line in handle.read().splitlines() if line.strip()]


def test_a_refused_wal_write_costs_one_mesh_record_and_not_the_io_thread(tmp_path):
    """The loop survives, and the proof is the records that arrive *after* the refusal.

    Asserted on later records rather than on the thread being alive, because "alive" is what the
    node looked like when the thread was gone: it answered `PING`, and `MM_PEERS` called the link
    `connected`. The only outward difference between a mesh node and a mesh node with no io thread
    is whether anything else ever arrives.
    """
    mgr, writer, receiver, fault_log = _mesh_with_faulted_receiver(MESH_DELTA_BYTES, tmp_path)
    try:
        raw(writer.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid 1001 5 1")
        assert len(await_rows(receiver.tcp_port, 1, patience(30))) == 1, (
            "the first record never reached the second node, so the refusal below would be the "
            "first thing this mesh ever did")

        # The record the disk refuses.
        raw(writer.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid 1002 5 1")
        deadline = time.monotonic() + patience(20)
        while time.monotonic() < deadline and not _injections(fault_log):
            time.sleep(0.25)
        fired = _injections(fault_log)
        assert any("action=fail" in line for line in fired), (
            f"nothing was refused, so this test measured an engine that had no fault to survive. "
            f"Injector decisions so far: {fired}")

        # And the point: three more records, written after the refusal, still arrive.
        for price in (2001, 2002, 2003):
            raw(writer.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid {price} 5 1")
        after = await_rows(receiver.tcp_port, 4, patience(45))
        assert len(after) >= 4, (
            f"the second node holds {len(after)} rows, so records written after the refused one "
            f"never arrived — which is what an ended io thread looks like from outside (#112)")

        errors = metric(receiver.metrics_port, "ob_mm_io_errors_total")
        assert errors >= 1, (
            "ob_mm_io_errors_total is zero, so an operator watching a full disk would see a node "
            "that looks entirely healthy — the counter is the only outward sign, because PING, "
            "MM_PEERS and the peer's `connected` row all stay fine")

        log = tail_node_log(receiver, 400)
        opened = log.count("handling a mesh event threw and this event is abandoned")
        assert opened >= 1, f"the abandoned event was not reported at all:\n{log[-2000:]}"
        assert "ended on an exception and that thread is gone" not in log, (
            "the io thread ended despite the boundary, which is the defect this test is for")
    finally:
        mgr.shutdown()


def test_the_same_run_without_a_matching_write_injects_nothing(tmp_path):
    """The control, and it is a separate test because it has to be able to fail on its own.

    Same fixture, same traffic, a size no write in this engine has. Zero injections and a mesh that
    keeps up — so a green run of the test above says something about the boundary rather than about
    an injector that never matched.
    """
    mgr, writer, receiver, fault_log = _mesh_with_faulted_receiver("999", tmp_path)
    try:
        for price in (3001, 3002, 3003, 3004):
            raw(writer.tcp_port, f"INSERT {SYMBOL} {EXCHANGE} bid {price} 5 1")
        seen = await_rows(receiver.tcp_port, 4, patience(45))
        assert len(seen) >= 4, f"the mesh did not converge with no fault armed: {len(seen)} rows"
        assert _injections(fault_log) == [], (
            f"the injector matched something at a size nothing writes: {_injections(fault_log)}")
        assert metric(receiver.metrics_port, "ob_mm_io_errors_total") == 0
        assert "handling a mesh event threw" not in tail_node_log(receiver, 400)
    finally:
        mgr.shutdown()
