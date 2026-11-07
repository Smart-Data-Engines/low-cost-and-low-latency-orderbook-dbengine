"""#258: the client secrets and permissions read again on SIGHUP, on a live node.

Until #258 both files were read at the start, and a change - a client added, a secret rotated, a
compromised one revoked - took a restart. SIGHUP reads them again now: a file the start would refuse
is refused and the credentials in force stay; one it takes is in force for the next AUTH, and the open
sessions of the identities it took away or changed are closed, between commands and with no answer
they did not ask for. Every other session goes on, on the same connection.

Bare sockets and the challenge-response computed here, so the tests see the protocol rather than a
client that would reconnect on its own.
"""
from __future__ import annotations

import hashlib
import hmac
import signal
import socket
import subprocess
import time
from pathlib import Path

import pytest

from conftest import free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke

ALICE = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1-alice"
ALICE_NEW = "a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2-alice"
BOB = "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1-bob"


def response(secret: str, identity: str, nonce: str) -> str:
    message = b"\x00".join([b"ob-auth-v1", b"client", b"initiator", identity.encode(), nonce.encode()])
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


class Node:
    def __init__(self, tmp: Path, acl: str | None = None) -> None:
        self.clients = tmp / "clients"
        self.acl = tmp / "permissions" if acl is not None else None
        self.write_clients(f"alice {ALICE}\nbob {BOB}\n")
        if acl is not None:
            self.write_acl(acl)
        self.port = free_port()
        self.log_path = tmp / "node.log"
        self.log = open(self.log_path, "w", encoding="utf-8")
        argv = [server_binary_path(), "--port", str(self.port), "--data-dir", str(tmp / "data"),
                "--metrics-port", "0", "--auth-secret-file", str(self.clients)]
        if self.acl is not None:
            argv += ["--auth-acl-file", str(self.acl)]
        self.proc = subprocess.Popen(argv, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + patience(30)
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"the node exited with {self.proc.returncode}:\n{self.log_text()}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=1):
                    return
            except OSError:
                time.sleep(0.1)
        raise AssertionError("the node never accepted a connection")

    def write_clients(self, text: str, mode: int = 0o600) -> None:
        self.clients.write_text(text)
        self.clients.chmod(mode)

    def write_acl(self, text: str) -> None:
        assert self.acl is not None
        self.acl.write_text(text)
        self.acl.chmod(0o644)

    def reload(self) -> None:
        self.proc.send_signal(signal.SIGHUP)

    def log_text(self) -> str:
        self.log.flush()
        return self.log_path.read_text(errors="replace")

    def wait_for_log(self, text: str) -> None:
        deadline = time.monotonic() + patience(10)
        while text not in self.log_text():
            assert time.monotonic() < deadline, f"the node never logged {text!r}:\n{self.log_text()[-3000:]}"
            time.sleep(0.05)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=patience(20))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()


class Wire:
    def __init__(self, port: int) -> None:
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=patience(10))
        buf = b""
        while b"\n\n" not in buf:
            buf += self.sock.recv(4096)

    def ask(self, line: str) -> str:
        """One command and its whole answer - `OK` through its empty line, any other one line - or
        what came before the node closed the connection."""
        self.sock.sendall((line + "\n").encode())
        buf = b""
        while True:
            try:
                chunk = self.sock.recv(4096)
            except OSError:
                chunk = b""
            if not chunk:
                return buf.decode(errors="replace")
            buf += chunk
            if buf.startswith(b"OK") and buf.endswith(b"\n\n"):
                return buf.decode(errors="replace")
            if not buf.startswith(b"OK") and buf.endswith(b"\n"):
                return buf.decode(errors="replace")

    def authenticate(self, identity: str, secret: str) -> str:
        challenge = self.ask("AUTH")
        assert challenge.startswith("OK CHALLENGE "), challenge
        nonce = challenge.split()[2]
        return self.ask(f"AUTH {identity} {response(secret, identity, nonce)}")

    def closed_by_the_node(self) -> bool:
        """Whether the node has closed this connection, waited for within the patience given."""
        deadline = time.monotonic() + patience(10)
        self.sock.settimeout(0.2)
        try:
            while time.monotonic() < deadline:
                try:
                    if self.sock.recv(4096) == b"":
                        return True
                except socket.timeout:
                    continue
                except OSError:
                    return True
            return False
        finally:
            self.sock.settimeout(patience(10))

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


@pytest.fixture
def node(tmp_path):
    n = Node(tmp_path)
    yield n
    n.stop()


@pytest.fixture
def node_with_permissions(tmp_path):
    n = Node(tmp_path, acl="alice role=admin\nbob role=write\n")
    yield n
    n.stop()


def test_a_reload_closes_the_session_it_revokes_and_keeps_the_others(node):
    alice, bob = Wire(node.port), Wire(node.port)
    try:
        assert alice.authenticate("alice", ALICE).startswith("OK AUTH alice")
        assert bob.authenticate("bob", BOB).startswith("OK AUTH bob")

        node.write_clients(f"alice {ALICE}\n")
        node.reload()
        assert bob.closed_by_the_node(), "a revoked identity's session stayed open"
        node.wait_for_log("of identity 'bob': credential revoked by a reload")
        assert alice.ask("PING") == "PONG\n", "a session whose identity the reload kept was touched"

        again = Wire(node.port)
        try:
            assert not again.authenticate("bob", BOB).startswith("OK"), "a revoked identity authenticated"
        finally:
            again.close()
    finally:
        alice.close()
        bob.close()


def test_a_rotated_secret_closes_its_sessions_and_only_the_new_one_admits(node):
    alice, bob = Wire(node.port), Wire(node.port)
    try:
        assert alice.authenticate("alice", ALICE).startswith("OK AUTH alice")
        assert bob.authenticate("bob", BOB).startswith("OK AUTH bob")

        node.write_clients(f"alice {ALICE_NEW}\nbob {BOB}\n")
        node.reload()
        assert alice.closed_by_the_node(), "the session of a rotated secret stayed open"
        node.wait_for_log("of identity 'alice': credential changed by a reload")
        assert bob.ask("PING") == "PONG\n"

        old, new = Wire(node.port), Wire(node.port)
        try:
            assert not old.authenticate("alice", ALICE).startswith("OK"), "the old secret still admits"
            assert new.authenticate("alice", ALICE_NEW).startswith("OK AUTH alice")
        finally:
            old.close()
            new.close()
    finally:
        alice.close()
        bob.close()


def test_a_changed_role_closes_the_session_and_the_next_one_has_it(node_with_permissions):
    node = node_with_permissions
    bob = Wire(node.port)
    try:
        assert bob.authenticate("bob", BOB).startswith("OK AUTH bob")
        assert bob.ask("INSERT CRSYM EX bid 100 1 1").startswith("OK")

        node.write_acl("alice role=admin\nbob role=read\n")
        node.reload()
        assert bob.closed_by_the_node(), "the session of a changed role stayed open"
    finally:
        bob.close()
    again = Wire(node.port)
    try:
        assert again.authenticate("bob", BOB).startswith("OK AUTH bob")
        refused = again.ask("INSERT CRSYM EX bid 101 1 1")
        assert refused.startswith("ERR"), f"a reader wrote: {refused!r}"
    finally:
        again.close()


def test_a_refused_file_leaves_every_credential_in_force(node):
    alice = Wire(node.port)
    try:
        assert alice.authenticate("alice", ALICE).startswith("OK AUTH alice")
        node.write_clients(f"alice {ALICE_NEW}\n", mode=0o644)   # others may read it: refused
        node.reload()
        node.wait_for_log("credential reload refused, every session keeps the credentials it had")
        assert alice.ask("PING") == "PONG\n"
        bob = Wire(node.port)
        try:
            assert bob.authenticate("bob", BOB).startswith("OK AUTH bob"), "a refused file took effect"
        finally:
            bob.close()
    finally:
        alice.close()
