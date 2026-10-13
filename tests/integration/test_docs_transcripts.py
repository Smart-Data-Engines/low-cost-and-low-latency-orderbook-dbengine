"""What the documentation shows a server answering is what the server answers (#40).

`docs/protocol.md` and `docs/tutorial.md` mark the exchanges they show with
`<!-- transcript: checked -->`. Each such block is replayed here against a node of its own, byte for
byte:
- every `C:` line is sent;
- every `S:` line is one line of what must come back, and a bare `S:` is an empty line;
- `<version>` stands for the server's version.

The tutorial's Python block (`<!-- tutorial: python -->`) is run in a subprocess, and must print
the block marked `<!-- tutorial: python output -->`.

A page that shows an exchange the server no longer has fails here, rather than in the hands of the
first reader who types it. The README's quick start had shown `OK` answers without the empty line
that ends them, and a timestamp no server writes: what a page nothing checks comes to look like.

The last tests cover the site build's link hook (`scripts/mkdocs_hooks.py`), which has no MkDocs to
run under in this job.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import time

import pytest

from conftest import free_port, patience, server_binary_path

pytestmark = pytest.mark.smoke

REPO = pathlib.Path(__file__).resolve().parents[2]
SERVER = server_binary_path()
CHECKED = "<!-- transcript: checked -->"
PAGES = ("docs/protocol.md", "docs/tutorial.md")


def _block_after(lines: list[str], at: int, page: str) -> tuple[int, list[str]]:
    """The fenced block that starts on the line after `at`: its first line number and its lines."""
    fence = at + 1
    assert fence < len(lines) and lines[fence].startswith("```"), (
        f"{page}:{at + 1}: a marker not followed by a fenced block marks nothing")
    end = fence + 1
    while end < len(lines) and not lines[end].startswith("```"):
        end += 1
    assert end < len(lines), f"{page}:{fence + 1}: the block is never closed"
    return fence + 2, lines[fence + 1:end]


def _marked_blocks(page: str, marker: str) -> list[tuple[int, list[str]]]:
    lines = (REPO / page).read_text(encoding="utf-8").split("\n")
    return [_block_after(lines, i, page) for i, line in enumerate(lines) if line.strip() == marker]


def _version() -> str:
    out = subprocess.run([SERVER, "--version"], capture_output=True, text=True, timeout=30).stdout
    return out.split()[-1]   # "ob_tcp_server 0.1.1.dev0"


def _exchange(body: list[str], where: str, version: str) -> tuple[bytes, bytes]:
    """What a block sends, and what it shows coming back."""
    sent, shown = b"", b""
    for line in body:
        if line.startswith("C: "):
            sent += line[3:].encode() + b"\n"
        elif line == "S:":
            shown += b"\n"
        elif line.startswith("S: "):
            shown += line[3:].replace("<version>", version).encode() + b"\n"
        else:
            raise AssertionError(f"{where}: {line!r} is neither `C: ` nor `S: `")
    return sent, shown


class _Node:
    """A standalone server on a data directory of its own, with no flush tick (pitfall 570): a
    transcript writes and then shows what `FLUSH` made of it, and a tick in between would make the
    answer depend on timing."""

    def __init__(self) -> None:
        self.dir = tempfile.TemporaryDirectory(prefix="ob_docs_")
        self.port = free_port()
        self.log = open(os.path.join(self.dir.name, "node.log"), "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [SERVER, "--port", str(self.port), "--data-dir", os.path.join(self.dir.name, "data"),
             "--metrics-port", "0", "--flush-interval-ms", "600000", "--drain-timeout-ms", "1000"],
            stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.time() + patience(30)
        while True:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=1).close()
                return
            except OSError:
                if self.proc.poll() is not None or time.time() > deadline:
                    self.close()
                    pytest.fail(f"the node on port {self.port} never listened")
                time.sleep(0.05)

    def replay(self, sent: bytes, shown: bytes) -> bytes:
        """Send `sent` and read until `shown`'s length has come back and the node has nothing more
        to say: a further half second of silence, or the connection closed."""
        with socket.create_connection(("127.0.0.1", self.port), timeout=patience(10)) as s:
            s.sendall(sent)
            got = b""
            deadline = time.time() + patience(10)
            while time.time() < deadline:
                s.settimeout(0.5 if len(got) >= len(shown) else patience(5))
                try:
                    chunk = s.recv(65536)
                except socket.timeout:
                    if len(got) >= len(shown):
                        break
                    continue
                if not chunk:
                    break
                got += chunk
            return got

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=patience(10))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.log.close()
        self.dir.cleanup()


@pytest.mark.parametrize("page", PAGES)
def test_every_checked_exchange_is_what_the_server_answers(page: str) -> None:
    blocks = _marked_blocks(page, CHECKED)
    # The control: a page whose markers were lost would pass by checking nothing.
    assert blocks, f"{page} marks no exchange as checked"
    version = _version()
    for first_line, body in blocks:
        where = f"{page}:{first_line}"
        sent, shown = _exchange(body, where, version)
        node = _Node()
        try:
            got = node.replay(sent, shown)
        finally:
            node.close()
        assert got == shown, (
            f"{where}: the page shows\n{shown.decode(errors='replace')}\n"
            f"and the server answers\n{got.decode(errors='replace')}")


def test_the_tutorials_python_prints_what_the_tutorial_says() -> None:
    [(code_line, code)] = _marked_blocks("docs/tutorial.md", "<!-- tutorial: python -->")
    [(_, output)] = _marked_blocks("docs/tutorial.md", "<!-- tutorial: python output -->")
    source = "\n".join(code)
    assert "port=9090" in source, f"docs/tutorial.md:{code_line}: the block no longer names port 9090"
    node = _Node()
    try:
        # The tutorial's node listens on 9090; this one is on a port of its own.
        run = subprocess.run(
            [sys.executable, "-c", source.replace("port=9090", f"port={node.port}")],
            capture_output=True, text=True, timeout=patience(60),
            env={**os.environ, "PYTHONPATH": str(REPO / "python")})
    finally:
        node.close()
    assert run.returncode == 0, run.stderr[-2000:]
    assert run.stdout.splitlines() == output, (
        f"docs/tutorial.md:{code_line}: the block prints\n{run.stdout}\nand the page says\n"
        + "\n".join(output))


def _hooks():
    spec = importlib.util.spec_from_file_location("mkdocs_hooks", REPO / "scripts" / "mkdocs_hooks.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_site_points_a_link_out_of_docs_at_github() -> None:
    hooks = _hooks()
    text = "See [SECURITY.md](../SECURITY.md#reporting) and [the CLI](cli.md).\n"
    out = hooks.rewrite_links(text, "operations.md")
    assert f"]({hooks.BLOB}SECURITY.md#reporting)" in out, out
    assert "](cli.md)" in out, "a link inside docs/ is the site's own and must stay as it was"
    fenced = "```\n[x](../SECURITY.md)\n```\n"
    assert hooks.rewrite_links(fenced, "operations.md") == fenced, "a fenced block is code, not a link"


def test_the_site_refuses_a_link_to_a_file_that_is_not_there() -> None:
    hooks = _hooks()
    with pytest.raises(ValueError) as missing:
        hooks.rewrite_links("[gone](../no-such-file.md)\n", "operations.md")
    assert "operations.md" in str(missing.value) and "no-such-file.md" in str(missing.value)
    with pytest.raises(ValueError):
        hooks.rewrite_links("[out](../../outside.md)\n", "operations.md")
