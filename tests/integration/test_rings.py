"""#52, step 2: a session through the Unix socket moved to rings in shared memory, through the binary.

`tests/test_unix_socket.cpp` and `tests/test_ring.cpp` run the server and the rings in one process;
this module runs the C++ client harness against a real node, every one of its subcommands through
the socket and then through the rings, with the client sleeping on its bell and polling. Under the
sanitizer job the same subcommands run against an instrumented server.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import pytest

from conftest import cpp_client_binary_path, free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke

SUBCOMMANDS = ("ping", "insert_query", "minsert", "query_agg", "query_buckets", "query_named")


@pytest.fixture(scope="module")
def harness() -> Path:
    found = cpp_client_binary_path()
    if found is None:
        pytest.skip("ob_integration_test not built (build with -DOB_BUILD_TESTS=ON); "
                    "note that the sanitizer job builds it on purpose, so a skip there fails")
    return Path(found)


@pytest.fixture
def workdir():
    d = Path(tempfile.mkdtemp(prefix="obr-"))
    assert len(str(d / "ob.sock")) < 100, f"the temporary directory {d} is too long for a socket"
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


class Node:
    """One ob_tcp_server with a Unix socket, its log in a file."""

    def __init__(self, workdir: Path, extra: list[str] | None = None):
        self.workdir = workdir
        self.sock = workdir / "ob.sock"
        self.port = free_port()
        self.metrics_port = free_port()
        self.log_path = workdir / "node.log"
        self.proc = subprocess.Popen(
            [server_binary_path(), "--port", str(self.port), "--bind", "127.0.0.1",
             "--data-dir", str(workdir / "data"), "--unix-socket", str(self.sock),
             "--metrics-port", str(self.metrics_port), "--metrics-bind", "127.0.0.1",
             *(extra or [])],
            stdout=open(self.log_path, "w"), stderr=subprocess.STDOUT)
        deadline = time.monotonic() + patience(20)
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"the node exited with {self.proc.returncode}:\n{self.log()}")
            if "not encrypted: the file's permissions decide" in self.log():
                return
            time.sleep(0.1)
        self.proc.kill()
        raise AssertionError(f"the node did not listen on its socket:\n{self.log()}")

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=patience(20))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def log(self) -> str:
        return self.log_path.read_text(errors="replace") if self.log_path.exists() else ""

    def metric(self, name: str) -> int:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.metrics_port}/metrics", timeout=5) as r:
            text = r.read().decode()
        m = re.search(rf"^{name}(?:\{{[^}}]*\}})? (-?\d+)$", text, re.M)
        assert m, f"{name} is not in /metrics"
        return int(m.group(1))


def run_harness(harness: Path, node: Node, name: str, env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run([str(harness), "--host", "127.0.0.1", "--port", str(node.port), "--test", name],
                          capture_output=True, text=True, timeout=patience(60),
                          env={**os.environ, **env})


@pytest.mark.parametrize("spin_us", ["0", "50"], ids=["client-sleeps", "client-polls"])
def test_every_subcommand_runs_through_rings(harness, workdir, spin_us):
    # A node a transport: the subcommands write fixed symbols and check exact answers, so a second
    # run on the same node would read the first one's rows.
    for label, env in (("socket", {}), ("rings", {"OB_RINGS": "1", "OB_RING_SPIN_US": spin_us})):
        node_dir = workdir / label
        node_dir.mkdir()
        node = Node(node_dir)
        try:
            for name in SUBCOMMANDS:
                run = run_harness(harness, node, name, {"OB_UNIX_SOCKET": str(node.sock), **env})
                assert run.returncode == 0, f"{name} through the {label}:\n{run.stdout}\n{run.stderr}"
                assert '"status":"pass"' in run.stdout, f"{name} through the {label}: {run.stdout}"
            moved = node.log().count("moved to rings of 1048576 bytes a direction")
            expected = len(SUBCOMMANDS) if label == "rings" else 0
            assert moved == expected, f"{moved} sessions moved through the {label}, not {expected}"
            assert node.metric("ob_ring_sessions_total") == expected
            # Every session closed, and its memory with it.
            deadline = time.monotonic() + patience(10)
            while node.metric("ob_ring_bytes") != 0 and time.monotonic() < deadline:
                time.sleep(0.1)
            assert node.metric("ob_ring_bytes") == 0, "the rings of closed sessions are still held"
            if label == "rings" and spin_us == "0":
                # A client that sleeps is rung for its answers.
                assert node.metric("ob_ring_bells_total") > 0
        finally:
            node.stop()
        assert node.proc.returncode == 0, node.log()[-3000:]


def test_a_node_with_rings_off_keeps_sessions_on_the_socket(harness, workdir):
    node = Node(workdir, ["--rings", "off"])
    try:
        run = run_harness(harness, node, "ping", {"OB_UNIX_SOCKET": str(node.sock), "OB_RINGS": "1"})
        assert run.returncode == 1, run.stdout
        assert "switched off on this node (--rings off)" in run.stdout, run.stdout
        ok = run_harness(harness, node, "ping", {"OB_UNIX_SOCKET": str(node.sock)})
        assert ok.returncode == 0, ok.stdout
    finally:
        node.stop()


def test_rings_are_asked_for_through_the_socket_only(harness, workdir):
    node = Node(workdir)
    try:
        run = run_harness(harness, node, "ping", {"OB_RINGS": "1"})
        assert run.returncode == 1, run.stdout
        assert "rings without unix_socket" in run.stdout, run.stdout
    finally:
        node.stop()
