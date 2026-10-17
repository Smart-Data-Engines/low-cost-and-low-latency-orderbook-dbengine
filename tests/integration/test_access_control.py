"""Permissions per identity over the wire (#31).

`--auth-acl-file` gives each identity of the secret file a role - read, write or admin - and, if it
says so, the symbols and exchanges it reaches. These run a node with three identities and hold what
each may do through the Python client and through a bare socket, the batch of pipelined writes
included: the event loop holds writes back to apply them together, and that path must not go
around the check. And without the file, every identity may still do everything.
"""
from __future__ import annotations

import re
import socket
import tempfile
import time
import urllib.request
from pathlib import Path

import pytest

from conftest import patience
from orderbook_engine import OrderbookEngine, OrderbookError
from test_auth import Node, client_response, write_secret_file

pytestmark = pytest.mark.smoke

SECRETS = {
    "reader": "0123456789abcdef0123456789abcdef-reader",
    "writer": "0123456789abcdef0123456789abcdef-writer",
    "boss": "0123456789abcdef0123456789abcdef-boss",
}
ACL = ("# what each may do\n"
       "reader role=read\n"
       "writer role=write symbols=BTC-*\n"
       "boss   role=admin\n")
T = 1_700_000_000_000_000_000


class AclNode(Node):
    """test_auth's node, with a permissions file beside its secret file."""

    def __init__(self, tmp: Path, acl: Path | None, **kw):
        super().__init__(tmp, **kw)
        self.acl = acl

    def argv(self) -> list[str]:
        args = super().argv()
        return args + ["--auth-acl-file", str(self.acl)] if self.acl else args


def _secret_file(tmp: Path) -> Path:
    return write_secret_file(tmp / "clients", "".join(f"{k} {v}\n" for k, v in SECRETS.items()))


def _permissions_file(tmp: Path, text: str, mode: int = 0o644) -> Path:
    """The mode is set rather than left to the umask: a group-writable file is refused, and a
    umask of 0002 makes one."""
    path = tmp / "permissions"
    path.write_text(text)
    path.chmod(mode)
    return path


@pytest.fixture
def node():
    with tempfile.TemporaryDirectory(prefix="ob_acl_") as d:
        tmp = Path(d)
        acl = _permissions_file(tmp, ACL)
        n = AclNode(tmp, acl, auth_file=_secret_file(tmp), metrics=True)
        n.start()
        try:
            yield n
        finally:
            n.stop()


def client(n: Node, who: str) -> OrderbookEngine:
    return OrderbookEngine(host="127.0.0.1", port=n.port, timeout=patience(10), auth=(who, SECRETS[who]))


class Session:
    """A bare authenticated socket, to send a batch in one write and read every answer."""

    def __init__(self, port: int, who: str):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=patience(10))
        self.buf = b""
        self.read_answer()   # the banner
        self.sock.sendall(b"AUTH\n")
        nonce = self.read_answer().split()[2]
        self.sock.sendall(f"AUTH {who} {client_response(SECRETS[who], who, nonce)}\n".encode())
        assert self.read_answer().startswith("OK AUTH"), who

    def read_answer(self) -> str:
        deadline = time.time() + patience(10)
        while time.time() < deadline:
            if self.buf.startswith((b"ERR ", b"PONG")) and b"\n" in self.buf:
                end = self.buf.index(b"\n") + 1
            elif b"\n\n" in self.buf:
                end = self.buf.index(b"\n\n") + 2
            else:
                self.buf += self.sock.recv(65536)
                continue
            answer, self.buf = self.buf[:end], self.buf[end:]
            return answer.decode()
        raise AssertionError(f"no answer; have {self.buf!r}")

    def close(self) -> None:
        self.sock.close()


def test_a_reader_reads_and_may_not_write(node):
    boss = client(node, "boss")
    boss.insert("BTC-USD", "EX", "bid", [100], [1], timestamp_ns=T)
    boss.flush()
    boss.close()
    reader = client(node, "reader")
    try:
        assert [r.price for r in reader.query_all("BTC-USD", "EX")] == [100]
        with pytest.raises(OrderbookError, match=r"forbidden: reader \(read\) may not run INSERT"):
            reader.insert("BTC-USD", "EX", "bid", [101], [1], timestamp_ns=T + 1)
        # The session is kept: a refused permission is not a failed authentication.
        assert [r.price for r in reader.query_all("BTC-USD", "EX")] == [100]
    finally:
        reader.close()


def test_a_writer_reaches_its_symbols_alone(node):
    writer = client(node, "writer")
    try:
        writer.insert("BTC-USD", "EX", "bid", [100], [1], timestamp_ns=T)
        with pytest.raises(OrderbookError, match=r"forbidden: writer may not INSERT 'ETH-USD'\.'EX'"):
            writer.insert("ETH-USD", "EX", "bid", [3000], [1], timestamp_ns=T)
        with pytest.raises(OrderbookError, match=r"forbidden: writer may not SELECT 'ETH-USD'\.'EX'"):
            writer.query_all("ETH-USD", "EX")
        # The raw answer: the client's execute() hands back what the server said, ERR included.
        assert writer._tcp.execute("BACKUP") == "ERR forbidden: writer (write) may not run BACKUP\n"
        # Refused by role before anything asks whether this node is in a cluster at all.
        assert writer._tcp.execute("FAILOVER node-2") == "ERR forbidden: writer (write) may not run FAILOVER\n"
    finally:
        writer.close()


def test_a_list_holds_for_a_subscription_too(node):
    s = Session(node.port, "writer")
    try:
        s.sock.sendall(b"SUBSCRIBE * FROM 'ETH-USD'.'EX'\nSUBSCRIBE * FROM 'BTC-USD'.'EX'\n")
        refused, accepted = s.read_answer(), s.read_answer()
    finally:
        s.close()
    assert refused == "ERR forbidden: writer may not SUBSCRIBE 'ETH-USD'.'EX'\n", refused
    assert re.fullmatch(r"OK SUB \d+\n\n", accepted), accepted


def test_a_pipelined_batch_of_writes_does_not_go_around_the_check(node):
    """The event loop holds the writes of one read and applies them together, through a function
    that checks nothing; a write the permissions refuse must be answered where it stands."""
    s = Session(node.port, "writer")
    try:
        s.sock.sendall(f"INSERT BTC-A EX bid 1 1 1 {T}\nINSERT ETH-B EX bid 2 1 1 {T}\n"
                       f"INSERT BTC-C EX bid 3 1 1 {T}\nFLUSH\n".encode())
        answers = [s.read_answer() for _ in range(4)]
    finally:
        s.close()
    assert answers[0] == "OK\n\n" and answers[2] == "OK\n\n" and answers[3] == "OK\n\n", answers
    assert answers[1] == "ERR forbidden: writer may not INSERT 'ETH-B'.'EX'\n", answers
    boss = client(node, "boss")
    try:
        with pytest.raises(OrderbookError):
            boss.query_all("ETH-B", "EX")   # never written
        assert [r.price for r in boss.query_all("BTC-C", "EX")] == [3]
    finally:
        boss.close()


def test_status_says_the_role_and_the_refusals_are_counted(node):
    s = Session(node.port, "reader")
    try:
        s.sock.sendall(b"STATUS\n")
        status = s.read_answer()
        s.sock.sendall(b"FLUSH\n")
        assert s.read_answer().startswith("ERR forbidden: reader (read) may not run FLUSH")
    finally:
        s.close()
    assert "identity: reader\nrole: read\n" in status, status
    metrics = urllib.request.urlopen(f"http://127.0.0.1:{node.metrics_port}/metrics", timeout=10).read().decode()
    assert re.search(r"^ob_auth_forbidden_total(\{[^}]*\})? [1-9]", metrics, re.M), \
        [l for l in metrics.splitlines() if "forbidden" in l]
    assert "forbidden: reader (read) may not run FLUSH" in node.log_text()


def test_without_the_file_every_identity_may_do_everything():
    with tempfile.TemporaryDirectory(prefix="ob_acl_none_") as d:
        tmp = Path(d)
        n = AclNode(tmp, None, auth_file=_secret_file(tmp))
        n.start()
        try:
            reader = client(n, "reader")
            try:
                reader.insert("ETH-USD", "EX", "bid", [3000], [1], timestamp_ns=T)
                reader.flush()
                assert [r.price for r in reader.query_all("ETH-USD", "EX")] == [3000]
                status = reader._tcp.execute("STATUS")
                assert "identity: reader\nrole: admin\n" in status, status
            finally:
                reader.close()
        finally:
            n.stop()


@pytest.mark.parametrize("acl,mode,says", [
    ("reader role=read\nwriter role=write\n", 0o644, "has no line for identity 'boss'"),
    (ACL + "carol role=read\n", 0o644, "identity 'carol' is not in the secret file"),
    ("reader role=reader\nwriter role=write\nboss role=admin\n", 0o644,
     "role 'reader' - a role is read, write or admin"),
    # Whoever can write the file decides what every identity may do.
    (ACL, 0o664, "is writable beyond its owner (mode 0664)"),
])
def test_a_file_that_does_not_decide_every_identity_is_a_refusal_to_start(acl, mode, says):
    with tempfile.TemporaryDirectory(prefix="ob_acl_bad_") as d:
        tmp = Path(d)
        path = _permissions_file(tmp, acl, mode)
        result = AclNode(tmp, path, auth_file=_secret_file(tmp)).run_and_wait()
    assert result.returncode == 1, result
    assert says in result.stderr, result.stderr
    assert "0123456789abcdef" not in result.stderr, "a refusal printed a secret"
