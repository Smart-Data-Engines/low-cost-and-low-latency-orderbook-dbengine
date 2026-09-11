"""Our own engine, driven the way a client drives it: over TCP, through the Python client.

Not embedded. The competitors are measured over their own wire protocols, so measuring ours in
process would compare a function call against a socket — the largest difference in the table would
be the transport, and it would be ours to keep.

Writing this adapter is also what found roadmap #90: it has to record the version of every system it
measures, it can read ClickHouse's from `SELECT version()`, and ours could not be asked at all. The
engine reports it now and `version()` below reads it.

One thing here is a limitation rather than a setting, and it is stated instead of smoothed: this
engine's aggregates run over the **live book**, while the SQL equivalents in `benchmarks/README.md`
compute VWAP over rows selected by timestamp. Those are different questions. `equivalence.py` will
refuse the pair rather than time it, and the refusal is the honest outcome: a specialised engine
answering a narrower question faster is not news, and reporting it as a win would be the kind of
selected table requirement 5.1 exists to prevent.
"""
from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from .base import LoadResult, QueryResult

# Raised for bulk loading, and named because requirement 4.2 asks the same of every competitor: a
# system nobody tuned measures our effort rather than its engine.
FLUSH_INTERVAL_MS = 1000


class OrderbookSystem:
    name = "orderbook"

    def __init__(self, binary: Path, port: int):
        self._binary = binary
        self._port = port
        self._proc: subprocess.Popen | None = None
        self._data_dir = tempfile.mkdtemp(prefix="ob_bench_")
        self._log = open(os.path.join(self._data_dir, "node.log"), "a",
                         encoding="utf-8", buffering=1)
        self._engine = None
        self._raw: socket.socket | None = None

    # ── Availability and identity ────────────────────────────────────────────

    def available(self) -> tuple[bool, str]:
        if not self._binary.is_file():
            return False, f"{self._binary} is not built"
        return True, ""

    def version(self) -> str:
        """Asked of the running node, which is now a question it can answer.

        `STATUS` reports a `version:` line, and the client keeps every `key: value` field the server
        sends, so this is one lookup. It was not always: writing this adapter is what found that no
        running node could be asked its version at all — not `--print-config`, not `STATUS`, not
        `/metrics` — and this method returned a sentence saying so. Roadmap #90 fixed the engine, and
        this reads the fix rather than keeping the complaint.

        Still no fallback literal. A version this file invents is a version that disagrees with the
        binary, which is the defect #90 was about; if the field is missing the report says so.
        """
        self._ensure_running()
        assert self._engine is not None
        reported = self._engine.status().get("version")
        if reported:
            return str(reported)
        return "unreported (STATUS carried no version field)"

    def config_dump(self) -> str:
        """`--print-config`, which reports the provenance of every value and opens no port."""
        out = subprocess.run(
            [str(self._binary), "--print-config", "--data-dir", self._data_dir,
             "--flush-interval-ms", str(FLUSH_INTERVAL_MS), "--port", str(self._port)],
            capture_output=True, text=True, timeout=10)
        return out.stdout

    def tuning_applied(self) -> list[str]:
        return [
            f"--flush-interval-ms {FLUSH_INTERVAL_MS} (default 100): fewer, larger segment writes "
            f"during a bulk load",
            "one MINSERT per update rather than one INSERT per level: a round trip per book change "
            "instead of per price level, which is what the wire protocol is shaped for",
            "the timed query is read from the socket and parsed into tuples rather than through "
            "`OrderbookEngine.query()`: measured on 4000 rows, the client's row objects cost p50 "
            "15.5 ms against 5.3 ms for the same bytes parsed as tuples, and no other adapter here "
            "pays that - it is a real cost to a Python user and not a property of the engine",
        ]

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def _ensure_running(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            return
        self._proc = subprocess.Popen(
            [str(self._binary), "--port", str(self._port), "--data-dir", self._data_dir,
             "--metrics-port", "0", "--flush-interval-ms", str(FLUSH_INTERVAL_MS)],
            stdout=self._log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            from orderbook_engine import OrderbookEngine, OrderbookError
            try:
                engine = OrderbookEngine(host="127.0.0.1", port=self._port, timeout=10.0)
                engine.ping()
                self._engine = engine
                return
            except (OrderbookError, OSError):
                time.sleep(0.3)
        raise RuntimeError(f"{self.name} did not come up on port {self._port}")

    # ── The wire, without the client's row objects ───────────────────────────
    #
    # Measured, 4000 rows of one symbol on this machine: `OrderbookEngine.query()` p50 **15.5 ms**,
    # the same rows read from the socket and parsed into tuples p50 **5.3 ms**. Two thirds of what
    # the first comparative run reported as our query time was the Python client constructing 4000
    # `OrderbookRow` dataclasses - work no other adapter in this harness pays, because each of them
    # parses text into tuples. So the table was charging us for our client's convenience and calling
    # it engine speed: 18.2 ms against ClickHouse's 6.3, where the comparable figure is 5.3.
    #
    # The cost is real for a Python user and it is reported rather than hidden - `tuning_applied()`
    # carries both numbers, and they are in the published notes column.

    def _raw_socket(self) -> socket.socket:
        if self._raw is None:
            sock = socket.create_connection(("127.0.0.1", self._port), timeout=60)
            banner = sock.recv(4096)                       # `OK ob_tcp_server vX` and a newline
            if not banner:
                raise RuntimeError("the server closed the connection before its banner")
            self._raw = sock
        return self._raw

    def _raw_rows(self, query: str) -> list[tuple]:
        """Send one query, read to the blank line that ends an `OK` response, return tuples.

        The column indices come from the header the server sends rather than from constants: a
        protocol that grows a column - as it did in #65, which added `sequence_number` - must make
        this fail rather than silently read the wrong field.
        """
        sock = self._raw_socket()
        sock.sendall((query + "\n").encode())
        buf = b""
        while b"\n\n" not in buf:
            chunk = sock.recv(1 << 20)
            if not chunk:
                raise RuntimeError("the server closed the connection mid-response")
            buf += chunk

        lines = buf.decode().split("\n")
        if not lines or lines[0] != "OK":
            raise RuntimeError(f"the server refused the query: {lines[0] if lines else '(nothing)'}")
        columns = lines[1].split("\t")
        try:
            want = [columns.index(name) for name in ("timestamp_ns", "price", "quantity")]
        except ValueError as exc:
            raise RuntimeError(f"the response header does not name the columns this adapter reads "
                               f"({columns}): {exc}") from exc
        rows = []
        for line in lines[2:]:
            if not line:
                break
            fields = line.split("\t")
            rows.append(tuple(int(fields[i]) for i in want))
        return rows

    def teardown(self) -> None:
        if self._raw is not None:
            self._raw.close()
            self._raw = None
        if self._engine is not None:
            self._engine.close()
            self._engine = None
        if self._proc is not None and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        self._log.close()

    # ── Workloads ────────────────────────────────────────────────────────────

    def load(self, csv_path: Path) -> LoadResult:
        """One `MINSERT` per update: the rows sharing a timestamp go in a single round trip.

        Grouped on `(ts_ns, symbol, side)` rather than on `(symbol, side)`, because the client takes
        one `timestamp_ns` per call and batching across timestamps would store the wrong ones - which
        the time-range query then selects on. Found by running this, not by reading it.
        """
        import csv as csv_module

        self._ensure_running()
        assert self._engine is not None

        started = time.perf_counter()
        rows = 0
        current: tuple[int, str, str] | None = None
        prices: list[int] = []
        sizes: list[int] = []

        with csv_path.open(encoding="utf-8") as handle:
            for row in csv_module.DictReader(handle):
                key = (int(row["ts_ns"]), row["symbol"], row["side"])
                if current is not None and key != current:
                    self._send_update(current, prices, sizes)
                    prices, sizes = [], []
                current = key
                prices.append(int(row["price_ticks"]))
                sizes.append(int(row["size_lots"]))
                rows += 1
        if current is not None and prices:
            self._send_update(current, prices, sizes)

        self._engine.flush()
        return LoadResult(rows_loaded=rows, seconds=time.perf_counter() - started)

    def _send_update(self, key: tuple[int, str, str], prices: list[int],
                     sizes: list[int]) -> None:
        ts_ns, symbol, side = key
        assert self._engine is not None
        self._engine.insert(symbol, "EX", side, prices, sizes, timestamp_ns=ts_ns)

    # The dataset's span cannot be used as a predicate here, and the reason is roadmap #105 rather
    # than a choice: `insert(timestamp_ns=...)` is accepted by the Python client, used in embedded
    # mode, and **silently dropped over TCP** - `INSERT` and `MINSERT` carry no timestamp field, so
    # the server stamps arrival time. Measured: rows loaded with the dataset's timestamps came back
    # at `1789153200757060030`, and the dataset's own span selected **0 of 400 rows** while the same
    # load into ClickHouse and TimescaleDB selected 400.
    #
    # So this asks for everything the store holds for one symbol, which is the same *rows* the SQL
    # systems select from their range - and the driver compares price and size, naming the time
    # column as incomparable. Widening the predicate here rather than hiding the defect: the
    # published table says the engine cannot be given event time, which is the most useful thing
    # this comparison produced.
    WIDEST_RANGE = (0, 9_999_999_999_999_999_999)

    def query_time_range(self, start_ns: int, end_ns: int) -> QueryResult:
        """`start_ns` and `end_ns` are accepted and **deliberately not used** - see above.

        Accepted rather than removed from the signature because the interface is what makes the
        four systems comparable, and because the day #105 lands this method has to start using
        them. A parameter ignored in silence is what #105 *is*, so it is ignored in writing.
        """
        self._ensure_running()
        assert self._engine is not None
        low, high = self.WIDEST_RANGE
        started = time.perf_counter()
        rows = self._raw_rows(
            f"SELECT * FROM 'SYM0000'.'EX' WHERE timestamp BETWEEN {low} AND {high}")
        elapsed = time.perf_counter() - started
        # The first version of this mapped `r.timestamp` and `r.size`, which do not exist -
        # `OrderbookRow` names them `timestamp_ns` and `quantity`. It raised `AttributeError` from
        # the day it was written and nobody saw it, because the query selected on timestamps the
        # server had never stored (#105) and returned an empty list, so the comprehension never
        # touched a row. Two defects in three lines, each hiding the other, and part one's
        # published noise floor was measured through this method.
        return QueryResult(rows=rows, seconds=elapsed)

    def query_vwap(self, symbol: str, at_ns: int) -> QueryResult:
        """Over the live book, which is a **different question** from the SQL equivalents.

        Reported as such rather than timed against them. `at_ns` is accepted and ignored, and that
        is exactly why this pair must not be compared: the server refuses a timestamp range on an
        aggregate rather than accepting one and ignoring it, so the two workloads cannot be made to
        answer the same thing without changing what one of them means.
        """
        self._ensure_running()
        assert self._engine is not None
        started = time.perf_counter()
        aggs = self._engine.query_agg(symbol, "EX", "VWAP(*)")
        elapsed = time.perf_counter() - started
        value = aggs["VWAP(*)"].real
        return QueryResult(rows=[(value,)], seconds=elapsed)
