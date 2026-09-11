"""ClickHouse, native install, over one kept-alive HTTP connection.

Three decisions here were made by measuring rather than by reading, and each of them would have
produced a wrong number the other way.

**The endpoint is proved, not assumed.** This workstation already runs a *containerised* ClickHouse
for the flagship product's test engines, published on 58123. A run that reached that one would have
measured the container layer, which requirement 1.2 forbids, and nothing in the numbers would have
looked wrong. So `available()` asks the server for its version and puts it in the report: the native
install answers `26.8.2.7` on 8123, the container answers `24.8.14.39` on 58123. The trap is real
and the check is a measurement.

**Nothing in the timed path spawns a process.** The first version of this adapter drove
`clickhouse-client` per query and reported 88.6 ms for a query returning 2000 rows. Measured: the
client costs **80 ms** of `fork`/`exec`/connect on `SELECT 1`, so ~78 ms of that was process startup
charged to ClickHouse — against our own adapter, which holds one socket open. A comparison shaped
like that measures the harness.

**HTTP rather than the native protocol, and that is measured too.** The worry was that HTTP's text
framing would disadvantage ClickHouse on a 2000-row result. It does not: with `clickhouse-driver`
installed for the experiment, native was p50 **5.777 ms** and HTTP p50 **4.980 ms** (mins 4.660 and
4.439), so HTTP is if anything faster here — the Python driver's row-tuple construction costs more
than TSV parsing. The transport is therefore chosen for the property that survives: the harness
needs **no driver dependency at all**, which is what lets a reader reproduce the run with the server
and a Python interpreter.

The schema and tuning below are what a ClickHouse user would write for this workload; requirement
4.2 exists because an untuned competitor produces a flattering number that looks exactly like a fair
one.
"""
from __future__ import annotations

import http.client
import time
from pathlib import Path
from urllib.parse import quote

from .base import LoadResult, QueryResult

HTTP_PORT = 8123
CONTAINER_PORT = 58123          # the flagship product's test engine; named so it stays named
DATABASE = "ob_bench"
TABLE = "book"

# One insert rather than a hundred: the CSV is handed over as a single block.
MAX_INSERT_BLOCK_SIZE = 1_000_000

DDL = f"""
CREATE DATABASE IF NOT EXISTS {DATABASE};
DROP TABLE IF EXISTS {DATABASE}.{TABLE};
CREATE TABLE {DATABASE}.{TABLE}
(
    ts_ns       Int64  CODEC(DoubleDelta, LZ4),
    symbol      LowCardinality(String),
    exchange    LowCardinality(String),
    side        LowCardinality(String),
    level       UInt16,
    price_ticks Int64,
    size_lots   Int64
)
ENGINE = MergeTree
ORDER BY (symbol, ts_ns, side, level)
"""


class ClickHouseSystem:
    name = "clickhouse"

    def __init__(self, host: str = "127.0.0.1", port: int = HTTP_PORT):
        self._host = host
        self._port = port
        self._conn: http.client.HTTPConnection | None = None
        self._prepared = False
        self._version = ""

    # ── The one transport ────────────────────────────────────────────────────

    def _connection(self) -> http.client.HTTPConnection:
        if self._conn is None:
            self._conn = http.client.HTTPConnection(self._host, self._port, timeout=600)
        return self._conn

    def _ask(self, query: str, *, body: Path | None = None, settings: str = "") -> str:
        """One request on the kept-alive connection. A failed response closes it, because
        `http.client` will not reuse a connection whose body was not read."""
        conn = self._connection()
        try:
            if body is None:
                conn.request("POST", "/" + settings, body=query.encode())
            else:
                path = f"/?query={quote(query)}" + (("&" + settings.lstrip("?")) if settings else "")
                with body.open("rb") as handle:
                    conn.request("POST", path, body=handle,
                                 headers={"Content-Length": str(body.stat().st_size)})
            response = conn.getresponse()
            payload = response.read().decode(errors="replace")
            if response.status != 200:
                raise RuntimeError(f"ClickHouse answered {response.status}: {payload.strip()[:400]}")
            return payload
        except (http.client.HTTPException, OSError):
            self._conn = None
            raise

    # ── Availability and identity ────────────────────────────────────────────

    def available(self) -> tuple[bool, str]:
        # The port is refused **before** anything is asked of it, so the reason in the table names
        # the container rather than whatever that server happens to answer first. Measured: the
        # container refuses on authentication, which is a true sentence about the wrong problem.
        if self._port == CONTAINER_PORT:
            return False, (f"port {CONTAINER_PORT} is this machine's containerised ClickHouse; "
                           f"requirement 1.2 asks for a native install")
        try:
            self._version = self._ask("SELECT version()").strip()
        except OSError as exc:
            return False, (f"no ClickHouse on {self._host}:{self._port} ({exc}) - see "
                           f"benchmarks/install_competitors.md")
        except RuntimeError as exc:
            return False, str(exc)
        return True, ""

    def version(self) -> str:
        """Read from the running server. The number also says *which* server answered: the native
        install on this machine reports 26.8.x and the container on 58123 reports 24.8.x."""
        return self._version or self._ask("SELECT version()").strip()

    def config_dump(self) -> str:
        self._prepare()
        table = self._ask(f"SHOW CREATE TABLE {DATABASE}.{TABLE}")
        changed = self._ask(
            "SELECT name || ' = ' || value FROM system.settings WHERE changed ORDER BY name")
        return (f"endpoint: http://{self._host}:{self._port} (kept-alive, one connection)\n"
                f"{table}\nchanged settings:\n{changed}")

    def tuning_applied(self) -> list[str]:
        return [
            "ORDER BY (symbol, ts_ns, side, level): the time-range query filters on symbol and "
            "ts_ns, so the primary key makes it a range scan instead of a full scan",
            "CODEC(DoubleDelta, LZ4) on ts_ns: the codec ClickHouse's own documentation names for a "
            "monotonically increasing timestamp, which is what this dataset's time column is",
            f"max_insert_block_size = {MAX_INSERT_BLOCK_SIZE:,}: the CSV arrives as one block",
            "LowCardinality(String) for symbol, exchange and side: three columns with at most fifty "
            "distinct values between them",
            "one kept-alive HTTP connection for every timed request: measured, a fresh "
            "`clickhouse-client` process costs 80 ms, which is 40 times the query it carries",
        ]

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def _prepare(self) -> None:
        if self._prepared:
            return
        for statement in DDL.strip().split(";"):
            if statement.strip():
                self._ask(statement)
        self._prepared = True

    def teardown(self) -> None:
        """The database goes, the server stays. This adapter did not start the server and must not
        stop it: it is a system service here, and a benchmark that leaves the machine different from
        how it found it is one nobody runs twice."""
        try:
            self._ask(f"DROP DATABASE IF EXISTS {DATABASE}")
        except (RuntimeError, OSError):
            pass
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ── Workloads ────────────────────────────────────────────────────────────

    def load(self, csv_path: Path) -> LoadResult:
        """`INSERT … FORMAT CSVWithNames`, the file streamed as the request body.

        Over the same connection as the queries, so the load is not charged a process start either:
        at 200 000 rows the client's 80 ms would have been about 5% of the answer.
        """
        self._prepare()
        started = time.perf_counter()
        self._ask(f"INSERT INTO {DATABASE}.{TABLE} FORMAT CSVWithNames", body=csv_path,
                  settings=f"max_insert_block_size={MAX_INSERT_BLOCK_SIZE}")
        elapsed = time.perf_counter() - started
        rows = int(self._ask(f"SELECT count() FROM {DATABASE}.{TABLE}").strip())
        return LoadResult(rows_loaded=rows, seconds=elapsed)

    def query_time_range(self, start_ns: int, end_ns: int) -> QueryResult:
        """The same three columns the reference adapter returns, in the same units.

        No `FINAL`: this is a plain `MergeTree`, so nothing is collapsed and a row is a row. The
        flagship product pays that tax because its tables have a key to enforce; charging ClickHouse
        for a guarantee this workload never asked for would be the opposite of requirement 4.2.
        """
        self._prepare()
        query = (f"SELECT ts_ns, price_ticks, size_lots FROM {DATABASE}.{TABLE} "
                 f"WHERE symbol = 'SYM0000' AND ts_ns BETWEEN {start_ns} AND {end_ns}")
        started = time.perf_counter()
        out = self._ask(query)
        elapsed = time.perf_counter() - started
        rows = [tuple(int(cell) for cell in line.split("\t"))
                for line in out.strip().splitlines() if line]
        return QueryResult(rows=rows, seconds=elapsed)

    def query_vwap(self, symbol: str, at_ns: int) -> QueryResult:
        """VWAP over rows selected by timestamp — which is **not** the question our engine answers.

        Kept because the workload definition in `benchmarks/README.md` includes it, and because
        `equivalence.py` is what should refuse the pair rather than this file quietly omitting a
        method and the report showing a system unable to do something it does well.
        """
        self._prepare()
        query = (f"SELECT sum(price_ticks * size_lots) / sum(size_lots) FROM {DATABASE}.{TABLE} "
                 f"WHERE symbol = '{symbol}' AND ts_ns <= {at_ns}")
        started = time.perf_counter()
        out = self._ask(query)
        elapsed = time.perf_counter() - started
        return QueryResult(rows=[(out.strip(),)], seconds=elapsed)
