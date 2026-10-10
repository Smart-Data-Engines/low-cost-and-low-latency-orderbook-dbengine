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

    def __init__(self, binary: Path, port: int, data_parent: Path, extra_args: tuple[str, ...] = ()):
        """`data_parent` is where the engine's storage goes, and the caller picks it.

        This was `tempfile.mkdtemp()` with no argument, which means `/tmp`. Where `/tmp` is a tmpfs
        the engine writes to RAM while both competitors write to disk, and nothing in the run says
        so - see hardware.VolatileStorage. The caller now names the filesystem, and the same path is
        what the report describes.
        """
        self._binary = binary
        self._port = port
        # Server options beyond the harness's own, named in the report by tuning_applied() - for a
        # measurement of one build's two settings, such as `--segment-format 2` against 3.
        self._extra_args = tuple(extra_args)
        self._proc: subprocess.Popen | None = None
        self._data_dir = tempfile.mkdtemp(prefix="ob_bench_", dir=str(data_parent))
        self._log = open(os.path.join(self._data_dir, "node.log"), "a",
                         encoding="utf-8", buffering=1)
        self._engine = None
        self._raw: socket.socket | None = None

    # ── Availability and identity ────────────────────────────────────────────

    def available(self) -> tuple[bool, str]:
        if not self._binary.is_file():
            return False, f"{self._binary} is not built"
        return True, ""

    def client_available(self) -> tuple[bool, str]:
        """This adapter is the engine's fastest Python client: the socket read and parsed into
        tuples, which `tuning_applied()` measures against the shipped client's row objects."""
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
            "the timed query is read by the client's `query_rows()`, which answers tuples, rather "
            "than by `query()`, which builds a row object a row and takes `SELECT *` only: measured "
            "on an Amazon EC2 m8a.xlarge, 4000 rows of `SELECT *` in 1.17 ms against 2.66 (#229)",
            "`query_rows()` converts the answer with `split()`, `map(int)` and `zip`: the fastest of "
            "three pure-Python parses measured on that machine with this harness's dataset on "
            "9 October 2026 - 0.64 ms for 4000 rows, against 0.72 for a loop reading fixed indices "
            "and 1.48 for the loop this adapter had until #226, when the competitors' clients were "
            "chosen the same way",
        ] + ([f"{' '.join(self._extra_args)}: set for this measurement"] if self._extra_args else [])

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def _ensure_running(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            return
        self._proc = subprocess.Popen(
            [str(self._binary), "--port", str(self._port), "--data-dir", self._data_dir,
             "--metrics-port", "0", "--flush-interval-ms", str(FLUSH_INTERVAL_MS),
             *self._extra_args],
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

    # ── The wire, read and not parsed ────────────────────────────────────────
    #
    # The timed query goes through the client's `query_rows()` (#229). This socket of its own is for
    # `reply_seconds()`, which reads the same answer and parses nothing, to say how much of the
    # query's figure is the engine and the wire and how much the client.
    #
    # It used to carry the query itself. Measured on the workstation this harness was written on,
    # 4000 rows of one symbol: `OrderbookEngine.query()` p50 **15.5 ms**, the same rows read from a
    # socket and parsed into tuples p50 **5.3 ms** - two thirds of what the first comparative run
    # reported as our query time was the client building `OrderbookRow`s. The client has a method
    # that answers tuples now, and it is the one the query is timed through.

    def _raw_socket(self) -> socket.socket:
        if self._raw is None:
            sock = socket.create_connection(("127.0.0.1", self._port), timeout=60)
            banner = sock.recv(4096)                       # `OK ob_tcp_server vX` and a newline
            if not banner:
                raise RuntimeError("the server closed the connection before its banner")
            self._raw = sock
        return self._raw

    def _reply(self, query: str) -> bytes:
        """Send one query and read to the blank line that ends an `OK` response."""
        sock = self._raw_socket()
        sock.sendall((query + "\n").encode())
        buf = b""
        while b"\n\n" not in buf:
            chunk = sock.recv(1 << 20)
            if not chunk:
                raise RuntimeError("the server closed the connection mid-response")
            buf += chunk
        return buf

    def _raw_rows(self, query: str) -> list[tuple]:
        """The query's rows as the engine's own Python client reads them, through `query_rows()`.

        The client reads the header's names and converts the whole answer at once - split, `map(int)`
        and `zip`, each loop in C (#229). That is the fastest of three pure-Python parses measured on
        an Amazon EC2 m8a.xlarge, 9 October 2026, over this harness's 4000 rows: 0.64 ms, against
        0.72 for a loop reading fixed indices and 1.48 for the loop building each tuple from a
        generator, which is what this adapter did itself until #226 - while the competitors were
        asked through their slowest clients. The columns compared are picked by the header's names,
        so a protocol that grows a column fails here rather than being read wrong.
        """
        assert self._engine is not None
        answer = self._engine.query_rows(query)
        try:
            want = [answer.columns.index(name) for name in ("timestamp_ns", "price", "quantity")]
        except ValueError as exc:
            raise RuntimeError(f"the response header does not name the columns this adapter reads "
                               f"({list(answer.columns)}): {exc}") from exc
        if want == list(range(len(answer.columns))):
            return answer.rows
        return [tuple(row[i] for i in want) for row in answer.rows]

    def server_pid(self) -> int | None:
        """The engine's process while it runs: started here, so its CPU is read by pid rather than
        by a name in /proc that the engine has changed once already (#218)."""
        if self._proc is not None and self._proc.poll() is None:
            return self._proc.pid
        return None

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

    #: Updates per round trip. #141 measured the knee at 64 (1.59x against a loop of `insert()`,
    #: and 512 gave the same 1.59x), so this is the number that measurement already argued for
    #: rather than the best of several tried here — picking it by trying is picking the flattering
    #: one. It is stated in the limitations line beside the ingest figure, because what each system
    #: is allowed to send per request is part of what the row means.
    BATCH_UPDATES = 64

    def load(self, csv_path: Path) -> LoadResult:
        """`insert_batch()` of BATCH_UPDATES updates per round trip, since #141.

        Each update is still the rows sharing one `(ts_ns, symbol, side)`, grouped on the timestamp
        as well as the symbol and side because every row in an update carries the call's single
        `timestamp_ns` — batching across timestamps would store the wrong ones, which the
        time-range query then selects on. Found by running this, not by reading it.

        What changed with #141 is only how many of those updates cross the wire per round trip. It
        is still not what the SQL systems get: they receive the whole CSV in **one** request, and
        this sends `rows / BATCH_UPDATES` of them.
        """
        import csv as csv_module

        self._ensure_running()
        assert self._engine is not None

        from orderbook_engine import BookUpdate

        started = time.perf_counter()
        rows = 0
        batch: list = []
        current: tuple[int, str, str] | None = None
        prices: list[int] = []
        sizes: list[int] = []

        def queue(key: tuple[int, str, str], px: list[int], sz: list[int]) -> None:
            ts_ns, symbol, side = key
            batch.append(BookUpdate(symbol=symbol, exchange="EX", side=side,
                                    prices=px, qtys=sz, timestamp_ns=ts_ns))

        with csv_path.open(encoding="utf-8") as handle:
            for row in csv_module.DictReader(handle):
                key = (int(row["ts_ns"]), row["symbol"], row["side"])
                if current is not None and key != current:
                    queue(current, prices, sizes)
                    prices, sizes = [], []
                    if len(batch) >= self.BATCH_UPDATES:
                        self._send_batch(batch)
                        batch = []
                current = key
                prices.append(int(row["price_ticks"]))
                sizes.append(int(row["size_lots"]))
                rows += 1
        if current is not None and prices:
            queue(current, prices, sizes)
        if batch:
            self._send_batch(batch)

        self._engine.flush()
        return LoadResult(rows_loaded=rows, seconds=time.perf_counter() - started)

    def _send_batch(self, batch: list) -> None:
        """One round trip for many updates, with every refusal surfaced rather than counted.

        `insert_batch()` answers **per update** — a batch is N independent writes in one journey,
        not a transaction — so a partly-refused batch is a silently short load if only the call's
        return is checked. A load that did not store what it was given must not be timed.
        """
        assert self._engine is not None
        refused = [r for r in self._engine.insert_batch(batch) if not r.ok]
        if refused:
            raise RuntimeError(
                f"the engine refused {len(refused)} of {len(batch)} updates in a batch; "
                f"first was update {refused[0].index}: {refused[0].message}")

    def _send_update(self, key: tuple[int, str, str], prices: list[int],
                     sizes: list[int]) -> None:
        ts_ns, symbol, side = key
        assert self._engine is not None
        # Since #105 this timestamp reaches the server; before it, the client accepted the argument
        # and the wire dropped it. The client now refuses rather than dropping, so a run against a
        # server too old to store it fails here instead of producing a table whose time column means
        # arrival.
        self._engine.insert(symbol, "EX", side, prices, sizes, timestamp_ns=ts_ns)

    # This method asked for everything the store held, because until #105 the predicate could not
    # be honoured: `insert(timestamp_ns=...)` was accepted by the Python client, used in embedded
    # mode, and **silently dropped over TCP**, so the server stamped arrival time. Measured then:
    # rows loaded with the dataset's timestamps came back at `1789153200757060030`, and the
    # dataset's own span selected **0 of 400 rows** while the same load into ClickHouse and
    # TimescaleDB selected 400.
    #
    # #105 added the field, so the span is a real predicate here now and the four systems are asked
    # the same question. `WIDEST_RANGE` stays for the aggregate path below, which is a different
    # question for a different reason.
    WIDEST_RANGE = (0, 9_999_999_999_999_999_999)

    def query_time_range(self, start_ns: int, end_ns: int) -> QueryResult:
        """The dataset's own span, as a predicate, since #105.

        The returned row count is what makes this comparable rather than merely timed: an engine
        that answered instantly with nothing would look fastest, and the driver compares the rows.
        """
        self._ensure_running()
        assert self._engine is not None
        started = time.perf_counter()
        rows = self._raw_rows(self._time_range_query(start_ns, end_ns))
        elapsed = time.perf_counter() - started
        # The first version of this mapped `r.timestamp` and `r.size`, which do not exist -
        # `OrderbookRow` names them `timestamp_ns` and `quantity`. It raised `AttributeError` from
        # the day it was written and nobody saw it, because the query selected on timestamps the
        # server had never stored (#105) and returned an empty list, so the comprehension never
        # touched a row. Two defects in three lines, each hiding the other, and part one's
        # published noise floor was measured through this method.
        return QueryResult(rows=rows, seconds=elapsed)

    @staticmethod
    def _time_range_query(start_ns: int, end_ns: int) -> str:
        # Three columns since #139, which is what makes the question the **same** as the others:
        # TimescaleDB already asks for `ts_ns, price_ticks, size_lots` and we were the only system
        # receiving seven. Not an optimisation of ours — the removal of an asymmetry that was
        # costing us.
        return (f"SELECT timestamp, price, quantity FROM 'SYM0000'.'EX' "
                f"WHERE timestamp BETWEEN {start_ns} AND {end_ns}")

    def reply_seconds(self, start_ns: int, end_ns: int) -> float:
        """The time-range query's reply, read and not parsed: the part of the engine's figure that
        is the engine and the wire rather than its client parsing text in Python (#226). No other
        adapter can say this of itself - a driver builds its rows as it reads - so it is reported
        beside the table rather than as a column of it."""
        self._ensure_running()
        started = time.perf_counter()
        self._reply(self._time_range_query(start_ns, end_ns))
        return time.perf_counter() - started

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
        # The bids' VWAP: the side is the function's argument since #200, when VWAP(*) - which
        # read the bids without saying so - became a refusal.
        aggs = self._engine.query_agg(symbol, "EX", "VWAP(bid)")
        elapsed = time.perf_counter() - started
        value = aggs["VWAP(bid)"].real
        return QueryResult(rows=[(value,)], seconds=elapsed)
