"""Books on huge pages (#51), over a real node.

The unit tests hold the arena and the engine's use of it. These hold the server's flag - that
`--book-huge-pages` reaches the engine, on unless set off - through what an operator can see: the
`ob_book_arena_regions` gauge and the region's log line.
"""
from __future__ import annotations

import re
import tempfile
import urllib.request
from pathlib import Path

import pytest

from conftest import patience
from orderbook_engine import OrderbookEngine
from test_auth import Node

pytestmark = pytest.mark.smoke

T = 1_700_000_000_000_000_000


class BooksNode(Node):
    """test_auth's node, with metrics and, if given, --book-huge-pages."""

    def __init__(self, tmp: Path, flag: str | None):
        super().__init__(tmp, metrics=True)
        self.flag = flag

    def argv(self) -> list[str]:
        args = super().argv()
        return args + ["--book-huge-pages", self.flag] if self.flag else args


def regions(node: Node) -> int:
    text = urllib.request.urlopen(f"http://127.0.0.1:{node.metrics_port}/metrics",
                                  timeout=patience(10)).read().decode()
    m = re.search(r"^ob_book_arena_regions(\{[^}]*\})? (\d+)$", text, re.M)
    assert m, [line for line in text.splitlines() if "arena" in line]
    return int(m.group(2))


@pytest.mark.parametrize("flag,in_regions", [(None, True), ("on", True), ("off", False)])
def test_the_books_are_made_where_the_flag_says(flag, in_regions):
    with tempfile.TemporaryDirectory(prefix="ob_books_") as d:
        node = BooksNode(Path(d), flag)
        node.start()
        try:
            assert regions(node) == 0, "no book yet, no region"
            client = OrderbookEngine(host="127.0.0.1", port=node.port, timeout=patience(10))
            try:
                client.insert("HP-1", "EX", "bid", [100], [1], timestamp_ns=T)
            finally:
                client.close()
            log = node.log_text()
            if in_regions:
                assert regions(node) == 1, "the first book opens the first region"
                assert re.search(r"region 1 at 0x[0-9a-f]+: \d+ books of \d+ bytes, transparent huge "
                                 r"pages (asked for|refused)", log), log[-2000:]
            else:
                assert regions(node) == 0, "off: the book is the heap's"
                assert "books are made on the heap, each its own allocation" in log, log[-2000:]
                assert "region 1 at" not in log
        finally:
            node.stop()
