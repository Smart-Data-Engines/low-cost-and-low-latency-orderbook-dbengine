"""How a node refuses to start (#102).

A node that cannot listen has to leave by the door marked *refused*, not the one marked *crashed*.
Both were reachable and the engine used the wrong one: `TcpServer::run()` throws for every startup
condition it cannot proceed past, nothing caught it, and the process left through the default
terminate handler — SIGABRT, exit -6, possibly a core file. The message was already correct. Only
the exit mode was wrong, and the exit mode is the half a supervisor reads: systemd logs a crash,
applies its crash restart policy, and an operator looking at `Main process exited, code=dumped`
starts debugging the engine instead of freeing the port.

It was measured here rather than reasoned about — CI, PR #91, a node whose client port a previous
test still held:

    terminate called after throwing an instance of 'std::runtime_error'
      what():  bind() failed on port 40739: Address already in use

reported by the harness as `node exited with -6`. The confusion is exactly #88's: an abort message
mentioning `terminate` sends the reader looking for an uncaught exception, and the mechanism is a
`std::thread` destroyed while still joinable. Here both were true at once, and a `catch` alone would
not have been enough — the catch block returns past the very destructor that aborts.

The contrast is the argument for the shape of the fix: `load_secrets_or_exit()` and
`load_tls_or_exit()` are named for what they do and print `Error: <what>` before exiting 1. The
listen path was the one that did not.

These tests spawn their own node, so they use `conftest.server_binary_path()` and
`conftest.free_port()` — the two things a module that starts its own nodes gets wrong by growing a
copy (see the static guards in `test_smoke.py`).
"""

from __future__ import annotations

import signal
import socket
import subprocess
import time

import pytest

from conftest import free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke


def hold_port() -> tuple[socket.socket, int]:
    """A listening socket the caller keeps, and the port it occupies.

    The port comes from the shared allocator and is then bound by number, so this module neither
    binds to zero nor hands a node a port some other test is about to be given.
    """
    port = free_port()
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.bind(("127.0.0.1", port))
    holder.listen(1)
    return holder, port


def run_node(argv: list[str], timeout: float) -> subprocess.CompletedProcess:
    return subprocess.run([server_binary_path(), *argv], capture_output=True, text=True,
                          timeout=timeout)


def test_a_taken_client_port_is_a_refusal_not_a_crash(tmp_path) -> None:
    """The assertion that failed before #102: exit 1, not -6.

    Deterministic, because the port is held by this test rather than raced for.
    """
    holder, port = hold_port()
    try:
        done = run_node(["--port", str(port), "--data-dir", str(tmp_path),
                         "--metrics-port", str(free_port())],
                        timeout=patience(60))
    finally:
        holder.close()

    assert done.returncode == 1, (
        "a node that cannot bind its client port must exit 1. "
        f"got {done.returncode} (-6 is SIGABRT, which a supervisor reads as a crash). "
        f"stderr tail: {done.stderr.strip().splitlines()[-3:]}"
    )
    # The message was never the defect, so it is pinned unchanged: anyone grepping their logs for it
    # after the fix has to keep finding it.
    assert f"bind() failed on port {port}" in done.stderr, (
        f"the refusal must name the port it could not take: {done.stderr.strip()[-400:]}"
    )
    assert "Address already in use" in done.stderr, (
        f"the refusal must name the reason: {done.stderr.strip()[-400:]}"
    )
    assert "Error:" in done.stderr, (
        "the refusal must use the same prefix as the other startup refusals, so one grep finds a "
        f"refused start whichever condition caused it: {done.stderr.strip()[-400:]}"
    )
    assert "terminate called" not in done.stderr, (
        f"the process still left through the terminate handler: {done.stderr.strip()[-400:]}"
    )


def test_a_taken_replication_port_leaves_the_same_way(tmp_path) -> None:
    """A second throw site, reached through a different call, to say the contract is the process's.

    `ReplicationManager::start()` binds from inside `Engine::open()`, which runs inside
    `TcpServer::run()` — so this exception crosses two more frames of construction than the client
    port's does, and unwinds a partially opened engine. Neither destructor had ever run in that
    state before the fix, because the process aborted instead.

    Without this test, the fix could have been a `catch` around one call rather than a contract
    about how the program ends.
    """
    holder, repl_port = hold_port()
    try:
        done = run_node(["--port", str(free_port()), "--data-dir", str(tmp_path),
                         "--metrics-port", str(free_port()),
                         "--replication-port", str(repl_port)],
                        timeout=patience(60))
    finally:
        holder.close()

    assert done.returncode == 1, (
        "a node that cannot bind its replication port must exit 1 too. "
        f"got {done.returncode}. stderr tail: {done.stderr.strip().splitlines()[-3:]}"
    )
    assert f"bind() failed on port {repl_port}" in done.stderr, (
        f"the refusal must name the port: {done.stderr.strip()[-400:]}"
    )
    assert "terminate called" not in done.stderr, (
        f"the process still left through the terminate handler: {done.stderr.strip()[-400:]}"
    )


def test_a_node_that_can_bind_starts_and_exits_zero(tmp_path) -> None:
    """The control, and the two tests above are worth nothing without it.

    Both of them pass for a binary that cannot start at all: `exit 1` and a message about a port are
    exactly what a thoroughly broken server produces. This one takes the same binary with ports
    nobody holds, gets a PONG out of it, and requires the ordinary shutdown path to still be worth
    zero — which is also the half most easily broken by adding a catch, since a joinable thread left
    behind on the success path aborts just as readily.
    """
    port, metrics_port = free_port(), free_port()
    node = subprocess.Popen(
        [server_binary_path(), "--port", str(port), "--data-dir", str(tmp_path),
         "--metrics-port", str(metrics_port)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = time.time() + patience(30)
        pong = ""
        while time.time() < deadline and "PONG" not in pong:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
                    sock.sendall(b"PING\n")
                    time.sleep(0.2)
                    pong = sock.recv(256).decode(errors="replace")
            except OSError:
                time.sleep(0.1)
        assert "PONG" in pong, f"the node never answered on a port nobody held: {pong!r}"

        node.send_signal(signal.SIGTERM)
        out, _ = node.communicate(timeout=patience(60))
    finally:
        if node.poll() is None:
            node.kill()
            node.communicate(timeout=30)

    assert node.returncode == 0, (
        f"an ordinary shutdown must still exit 0, got {node.returncode}. tail: "
        f"{out.strip().splitlines()[-3:]}"
    )
    assert "terminate called" not in out, f"the successful path aborted on the way out: {out[-400:]}"
