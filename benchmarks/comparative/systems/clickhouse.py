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

**The native protocol for the query, and that is measured too.** The first version of this adapter
measured the native protocol through `clickhouse-driver` at p50 **5.777 ms** against HTTP's
**4.980 ms** on a 2000-row result, and chose HTTP, which needed no driver. The two clocks were not
the same clock: HTTP's stopped before its TSV was parsed and the driver's after its rows were built
(#226). With every clock stopped at rows of Python ints, on an Amazon EC2 m8a.xlarge with this
harness's dataset on 9 October 2026: `clickhouse-driver` **1.29 ms**, `clickhouse-connect` 1.98 and
HTTP with the TSV parsed in Python 2.59. So the query goes through the fastest, and HTTP stays for
the load, which streams the CSV in one request. The price is a dependency: `client_available()`
names it when it is missing, and ClickHouse is then not measured rather than measured slower.

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
NATIVE_PORT = 9000              # the native protocol, which the time-range query is asked over (#226)
CONTAINER_PORT = 58123          # the flagship product's test engine; named so it stays named
DATABASE = "ob_bench"
TABLE = "book"

# One insert rather than a hundred: the CSV is handed over as a single block.
MAX_INSERT_BLOCK_SIZE = 1_000_000

# Decompressed blocks kept between queries: the cache ClickHouse documents for short queries asked
# again, and what the engine has done with its decoded columns since #220. Off by default, so raised
# here in ClickHouse's favour (#226). Measured on the m8a.xlarge with this harness's dataset and
# query: 2.51 ms with it and 2.58 ms without, every round of ten faster with it.
TIMED_QUERY_SETTINGS = {"use_uncompressed_cache": 1}

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

    def __init__(self, host: str = "127.0.0.1", port: int = HTTP_PORT, native_port: int = NATIVE_PORT):
        self._host = host
        self._port = port
        self._native_port = native_port
        self._conn: http.client.HTTPConnection | None = None
        self._client = None             # clickhouse_driver.Client, for the time-range query
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

    def client_available(self) -> tuple[bool, str]:
        """The time-range query is asked through clickhouse-driver, ClickHouse's fastest Python
        client measured here (#226), so a run without it does not time ClickHouse at all rather than
        time it through a slower one. Imported here and not at the top: the harness's tests run
        without it."""
        try:
            import clickhouse_driver  # noqa: F401
        except ImportError:
            return False, ("clickhouse-driver is not installed, and the time-range query is timed "
                           "through ClickHouse's fastest Python client - see "
                           "benchmarks/install_competitors.md")
        return True, ""

    def _native(self):
        if self._client is None:
            from clickhouse_driver import Client
            self._client = Client(host=self._host, port=self._native_port)
        return self._client

    def version(self) -> str:
        """Read from the running server. The number also says *which* server answered: the native
        install on this machine reports 26.8.x and the container on 58123 reports 24.8.x."""
        return self._version or self._ask("SELECT version()").strip()

    def config_dump(self) -> str:
        self._prepare()
        table = self._ask(f"SHOW CREATE TABLE {DATABASE}.{TABLE}")
        changed = self._ask(
            "SELECT name || ' = ' || value FROM system.settings WHERE changed ORDER BY name")
        import clickhouse_driver
        return (f"endpoint: http://{self._host}:{self._port} (kept-alive, one connection) for the "
                f"load; the native protocol on {self._host}:{self._native_port} for the time-range "
                f"query, through clickhouse-driver {clickhouse_driver.__version__} with "
                f"{TIMED_QUERY_SETTINGS}\n"
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
            "one connection kept open for every timed request - HTTP for the load, the native "
            "protocol for the query: measured, a fresh `clickhouse-client` process costs 80 ms, "
            "which is 40 times the query it carries",
            "use_uncompressed_cache = 1 for the time-range query: decompressed blocks kept between "
            "queries, the counterpart of the engine's decoded columns held between queries, and off "
            "by default. The query cache is not set: it keeps whole answers, and the engine keeps "
            "none",
            "the time-range query through clickhouse-driver, over the native protocol, to rows of "
            "Python ints: the fastest of three Python clients measured on an Amazon EC2 m8a.xlarge "
            "with this harness's dataset on 9 October 2026 - 1.29 ms, against 1.98 through "
            "clickhouse-connect and 2.59 over HTTP with the TSV parsed in Python, which is how this "
            "adapter asked it until #226",
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
        if self._client is not None:
            self._client.disconnect()
            self._client = None

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
        client = self._native()
        started = time.perf_counter()
        # Rows of Python ints, which is where every adapter's clock stops (#226). Until then this
        # one asked over HTTP and stopped before parsing the TSV, so the parse the note under the
        # table says every figure includes was in the engine's figure alone - and the transport
        # it was asked over was the slowest of three measured.
        rows = client.execute(query, settings=TIMED_QUERY_SETTINGS)
        elapsed = time.perf_counter() - started
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
