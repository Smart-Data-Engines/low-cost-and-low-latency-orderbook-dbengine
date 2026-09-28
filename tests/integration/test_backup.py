"""A node's backup, taken on a running server and restored offline (#34).

What only a live node can decide: that `BACKUP` under two writers is a cut in which every write
acknowledged before it is held, once, and the restored node serves exactly that; that a primary
restored from a backup makes the replica that followed it start over rather than resume inside a WAL
that no longer exists; that `ob_backup` authenticates like a client and says by its exit status what
happened; that a server refuses to start with its backups inside its data directory; and that a
failed backup is counted and leaves nothing that looks like one.

Each test runs its own nodes: they are stopped, restored and started again on other directories,
which no shared fixture survives.
"""
from __future__ import annotations

import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

from conftest import free_port, patience, server_binary_path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
from orderbook_engine import OrderbookEngine, OrderbookError  # noqa: E402

pytestmark = pytest.mark.smoke

SERVER = server_binary_path()
RESTORE = str(Path(SERVER).resolve().with_name("ob_restore"))
BACKUP_TOOL = str(Path(SERVER).resolve().with_name("ob_backup"))

EXCH = "EX"
SYMBOLS = [f"BKP{i}" for i in range(4)]
LEVELS = 3
OPS_SECRET = "0123456789abcdef0123456789abcdef-ops"


class Node:
    """One ob_tcp_server with its own data directory and a log file."""

    def __init__(self, data_dir: str, extra: list[str] | None = None):
        self.data_dir = data_dir
        self.extra = list(extra or [])
        self.port = 0
        self.proc: subprocess.Popen | None = None
        self.log_path = Path(data_dir).with_name(Path(data_dir).name + ".log")

    def argv(self) -> list[str]:
        return [SERVER, "--port", str(self.port), "--data-dir", self.data_dir, *self.extra]

    def start(self, timeout: float = 20.0) -> None:
        timeout = patience(timeout)
        self.port = free_port()
        self.log = open(self.log_path, "a", buffering=1)
        self.proc = subprocess.Popen(self.argv(), stdout=self.log, stderr=self.log)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"node exited with {self.proc.returncode}:\n{self.log_text()}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=2) as s:
                    s.settimeout(2)
                    s.recv(4096)
                    s.sendall(b"PING\n")
                    if b"PONG" in s.recv(1024):
                        return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(f"node on port {self.port} never answered:\n{self.log_text()}")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=patience(30))
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if getattr(self, "log", None):
            self.log.close()

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)

    def log_text(self) -> str:
        return self.log_path.read_text(errors="replace") if self.log_path.exists() else ""

    def prices(self, symbol: str) -> list[int]:
        """Every row's price; none for a symbol the node does not hold (yet)."""
        engine = OrderbookEngine(host="127.0.0.1", port=self.port, timeout=30)
        try:
            return [r.price for r in engine.query(f"SELECT * FROM '{symbol}'.'{EXCH}'")]
        except OrderbookError as e:
            if "NOT_FOUND" in str(e):
                return []
            raise
        finally:
            engine.close()


class Wire:
    """A bare connection, reading whole answers: `OK ...` to its blank line, `ERR ...` to its end."""

    def __init__(self, port: int, timeout: float = 30.0):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""
        self._until(b"\n\n")   # the banner

    def _until(self, end: bytes) -> bytes:
        while end not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError(f"closed after {self.buf!r}")
            self.buf += chunk
        at = self.buf.index(end) + len(end)
        out, self.buf = self.buf[:at], self.buf[at:]
        return out

    def ask(self, line: str) -> str:
        self.sock.sendall((line + "\n").encode())
        while True:
            if self.buf.startswith(b"ERR") and b"\n" in self.buf:
                return self._until(b"\n").decode()
            if b"\n\n" in self.buf:
                return self._until(b"\n\n").decode()
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError(f"closed after {line!r}: {self.buf!r}")
            self.buf += chunk

    def close(self) -> None:
        self.sock.close()


def status_of(answer: str) -> dict[str, str]:
    return dict(line.split(": ", 1) for line in answer.splitlines()[1:] if ": " in line)


def backup_and_wait(port: int, timeout: float = 120.0) -> tuple[str, dict[str, str]]:
    wire = Wire(port)
    try:
        started = wire.ask("BACKUP")
        assert started.startswith("OK BACKUP "), started
        name = started.split()[2]
        deadline = time.time() + patience(timeout)
        while time.time() < deadline:
            st = status_of(wire.ask("BACKUP STATUS"))
            assert st["name"] == name, st
            if st["state"] in ("done", "failed"):
                return name, st
            time.sleep(0.1)
        raise AssertionError(f"backup {name} did not end: {st}")
    finally:
        wire.close()


def restore(backup: str, data_dir: str) -> str:
    verified = subprocess.run([RESTORE, "--verify", backup], capture_output=True, text=True,
                              timeout=patience(120))
    assert verified.returncode == 0, verified.stderr
    done = subprocess.run([RESTORE, "--backup", backup, "--data-dir", data_dir],
                          capture_output=True, text=True, timeout=patience(300))
    assert done.returncode == 0, done.stderr
    return done.stdout


def price_of(writer: int, seq: int, level: int) -> int:
    return writer * 10_000_000 + seq * 10 + level


def write_id(price: int) -> tuple[int, int, int]:
    return price // 10_000_000, (price % 10_000_000) // 10, price % 10


class Writer(threading.Thread):
    """MINSERTs of three levels, one answer at a time, each price naming (writer, seq, level)."""

    def __init__(self, port: int, writer: int):
        super().__init__(daemon=True)
        self.port = port
        self.writer = writer
        self.stop = threading.Event()
        self.acked: list[tuple[int, float]] = []   # (seq, when its OK arrived)
        self.sent: set[int] = set()
        self.failure = ""

    def run(self) -> None:
        try:
            wire = Wire(self.port)
            seq = 0
            while not self.stop.is_set():
                symbol = SYMBOLS[seq % len(SYMBOLS)]
                body = "\n".join(f"{price_of(self.writer, seq, lv)} {lv + 1} 1" for lv in range(LEVELS))
                self.sent.add(seq)
                answer = wire.ask(f"MINSERT {symbol} {EXCH} bid {LEVELS}\n{body}")
                if not answer.startswith("OK"):
                    self.failure = f"write {seq}: {answer!r}"
                    return
                self.acked.append((seq, time.monotonic()))
                seq += 1
            wire.close()
        except Exception as e:  # reported by the test, not lost on this thread
            self.failure = repr(e)


def test_a_backup_under_two_writers_restores_every_write_acknowledged_before_it():
    with tempfile.TemporaryDirectory(prefix="ob_backup_it_") as tmp:
        node = Node(f"{tmp}/data", ["--backup-dir", f"{tmp}/backups", "--flush-interval-ms", "50"])
        node.start()
        writers = [Writer(node.port, w) for w in (1, 2)]
        try:
            for w in writers:
                w.start()
            time.sleep(1.5)
            asked_at = time.monotonic()
            name, st = backup_and_wait(node.port)
            assert st["state"] == "done", st
            time.sleep(0.5)            # writes after the backup, which it must not hold
        finally:
            for w in writers:
                w.stop.set()
            for w in writers:
                w.join(timeout=30)
            node.stop()
        for w in writers:
            assert not w.failure, f"writer {w.writer}: {w.failure}"
        before = {(w.writer, seq) for w in writers for seq, at in w.acked if at < asked_at}
        after = {(w.writer, seq) for w in writers for seq, at in w.acked if at > asked_at + 5}
        sent = {(w.writer, seq) for w in writers for seq in w.sent}
        assert len(before) > 100, f"only {len(before)} writes before the backup: nothing to test"
        assert st["method"] == "linked", st

        out = restore(f"{tmp}/backups/{name}", f"{tmp}/restored")
        assert "sequence state restored" in out, out
        back = Node(f"{tmp}/restored")
        back.start()
        try:
            held: dict[tuple[int, int], list[int]] = {}
            for symbol in SYMBOLS:
                for price in back.prices(symbol):
                    writer, seq, level = write_id(price)
                    held.setdefault((writer, seq), []).append(level)
        finally:
            back.stop()
        missing = before - set(held)
        assert not missing, (f"{len(missing)} write(s) acknowledged before BACKUP are not in the "
                             f"restored node, e.g. {sorted(missing)[:5]}")
        twice = {k: v for k, v in held.items() if sorted(v) != list(range(LEVELS))}
        assert not twice, (f"{len(twice)} write(s) held other than once and whole, e.g. "
                           f"{list(twice.items())[:5]}")
        foreign = set(held) - sent
        assert not foreign, f"rows nobody wrote: {sorted(foreign)[:5]}"
        assert not (set(held) & after), "the backup holds writes acknowledged long after its cut"


def test_a_primary_restored_from_a_backup_makes_its_replica_start_over():
    with tempfile.TemporaryDirectory(prefix="ob_backup_repl_") as tmp:
        repl_port = free_port()
        primary_args = ["--replication-port", str(repl_port), "--backup-dir", f"{tmp}/backups"]
        primary = Node(f"{tmp}/primary", primary_args)
        replica = Node(f"{tmp}/replica", ["--primary-host", "127.0.0.1", "--primary-port",
                                          str(repl_port)])
        restored = Node(f"{tmp}/restored", ["--replication-port", str(repl_port)])
        try:
            primary.start()
            replica.start()
            wire = Wire(primary.port)
            for seq in range(200):
                body = "\n".join(f"{price_of(1, seq, lv)} 1 1" for lv in range(LEVELS))
                assert wire.ask(f"MINSERT {SYMBOLS[0]} {EXCH} bid {LEVELS}\n{body}").startswith("OK")
            name, st = backup_and_wait(primary.port)
            assert st["state"] == "done", st
            at_backup = sorted(primary.prices(SYMBOLS[0]))
            for seq in range(200, 300):
                body = "\n".join(f"{price_of(1, seq, lv)} 1 1" for lv in range(LEVELS))
                assert wire.ask(f"MINSERT {SYMBOLS[0]} {EXCH} bid {LEVELS}\n{body}").startswith("OK")
            # SELECT reads what is sealed: FLUSH first, or the count is of a moment's seals.
            assert wire.ask("FLUSH").startswith("OK")
            wire.close()
            everything = sorted(primary.prices(SYMBOLS[0]))
            assert len(everything) == 300 * LEVELS, len(everything)
            deadline = time.time() + patience(60)
            while time.time() < deadline and sorted(replica.prices(SYMBOLS[0])) != everything:
                time.sleep(0.5)
            assert sorted(replica.prices(SYMBOLS[0])) == everything, "the replica never caught up"

            # The primary's disk is lost; it comes back from the backup, at the same address.
            primary.kill()
            restore(f"{tmp}/backups/{name}", f"{tmp}/restored")
            restored.start()
            deadline = time.time() + patience(90)
            while time.time() < deadline and sorted(replica.prices(SYMBOLS[0])) != at_backup:
                time.sleep(0.5)
            assert sorted(restored.prices(SYMBOLS[0])) == at_backup
            assert sorted(replica.prices(SYMBOLS[0])) == at_backup, (
                "the replica still serves rows the restored primary does not have: it resumed "
                "inside the WAL of the primary that was lost")
            assert "a different WAL at the same address" in replica.log_text(), (
                "the replica caught up for a reason other than the new WAL's identity")
        finally:
            replica.stop()
            restored.stop()
            primary.stop()


def test_ob_backup_authenticates_and_says_what_happened_by_its_exit_status():
    with tempfile.TemporaryDirectory(prefix="ob_backup_auth_") as tmp:
        secrets = Path(tmp) / "clients"
        secrets.write_text(f"ops {OPS_SECRET}\n")
        secrets.chmod(0o600)
        node = Node(f"{tmp}/data", ["--backup-dir", f"{tmp}/backups",
                                    "--auth-secret-file", str(secrets)])
        plain = Node(f"{tmp}/plain")
        node.start()
        plain.start()
        try:
            wire = Wire(node.port)
            refused = wire.ask("BACKUP")
            assert refused.startswith("ERR") and "auth" in refused.lower(), refused
            wire.close()
            assert os.listdir(f"{tmp}/backups") == [], "an unauthenticated BACKUP took a backup"

            run = subprocess.run([BACKUP_TOOL, "--host", "127.0.0.1", "--port", str(node.port),
                                  "--auth-identity", "ops", "--auth-secret-file", str(secrets)],
                                 capture_output=True, text=True, timeout=patience(120))
            assert run.returncode == 0, run.stderr
            name = re.match(r"backup (\S+) done", run.stdout).group(1)
            assert (Path(tmp) / "backups" / name / "backup.json").is_file()

            wrong = subprocess.run([BACKUP_TOOL, "--host", "127.0.0.1", "--port", str(node.port),
                                    "--auth-identity", "nobody", "--auth-secret-file", str(secrets)],
                                   capture_output=True, text=True, timeout=patience(60))
            assert wrong.returncode == 2, wrong
            assert "no secret for 'nobody'" in wrong.stderr, wrong.stderr

            unconfigured = subprocess.run([BACKUP_TOOL, "--host", "127.0.0.1", "--port",
                                           str(plain.port)],
                                          capture_output=True, text=True, timeout=patience(60))
            assert unconfigured.returncode == 2, unconfigured
            assert "not configured" in unconfigured.stderr, unconfigured.stderr
        finally:
            plain.stop()
            node.stop()


def test_backups_inside_the_data_directory_are_refused_at_start():
    with tempfile.TemporaryDirectory(prefix="ob_backup_nested_") as tmp:
        node = Node(f"{tmp}/data", ["--backup-dir", f"{tmp}/data/backups"])
        node.port = free_port()
        run = subprocess.run(node.argv(), capture_output=True, text=True, timeout=patience(30))
        assert run.returncode == 1, run
        assert "is in the data directory" in run.stderr, run.stderr
        # Control: beside it, it starts.
        ok = Node(f"{tmp}/data", ["--backup-dir", f"{tmp}/backups"])
        ok.start()
        ok.stop()


def test_a_failed_backup_is_counted_and_leaves_nothing_that_looks_like_one():
    if os.geteuid() == 0:
        pytest.skip("root writes to a read-only directory")
    with tempfile.TemporaryDirectory(prefix="ob_backup_fail_") as tmp:
        metrics_port = free_port()
        node = Node(f"{tmp}/data", ["--backup-dir", f"{tmp}/backups", "--metrics-port",
                                    str(metrics_port), "--metrics-bind", "127.0.0.1"])
        node.start()
        try:
            wire = Wire(node.port)
            body = "\n".join(f"{price_of(1, 0, lv)} 1 1" for lv in range(LEVELS))
            assert wire.ask(f"MINSERT {SYMBOLS[0]} {EXCH} bid {LEVELS}\n{body}").startswith("OK")
            wire.close()
            os.chmod(f"{tmp}/backups", 0o500)
            try:
                _, st = backup_and_wait(node.port)
            finally:
                os.chmod(f"{tmp}/backups", 0o700)
            assert st["state"] == "failed" and "cannot create" in st["error"], st
            assert os.listdir(f"{tmp}/backups") == []
            name, st = backup_and_wait(node.port)
            assert st["state"] == "done", st

            import urllib.request
            text = urllib.request.urlopen(f"http://127.0.0.1:{metrics_port}/metrics",
                                          timeout=10).read().decode()
            value = {m.group(1): float(m.group(2)) for m in
                     re.finditer(r"^(ob_backup\w*)(?:\{[^}]*\})? (\S+)$", text, re.M)}
            assert value.get("ob_backup_failures_total") == 1, value
            assert value.get("ob_backups_total") == 1, value
            assert value.get("ob_backup_running") == 0, value
            for gauge in ("ob_backup_last_success_timestamp_seconds", "ob_backup_last_duration_ms",
                          "ob_backup_last_bytes", "ob_backup_last_pinned_ms"):
                assert gauge in value, f"{gauge} is not served: {sorted(value)}"
            assert value["ob_backup_last_success_timestamp_seconds"] > 1.7e9
            assert value["ob_backup_last_bytes"] > 0
        finally:
            node.stop()
