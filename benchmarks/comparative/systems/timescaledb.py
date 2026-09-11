"""TimescaleDB on a native PostgreSQL, driven through one long-lived `psql` session.

**Why a session rather than `psql -c` per query, measured:** a fresh `psql` costs 40-60 ms on this
machine, and the queries here are single-digit milliseconds. Per-invocation would have charged
TimescaleDB roughly ten times the work it was asked to do - the same defect the ClickHouse adapter
started with, where `clickhouse-client` added 80 ms to a 5 ms query. So one process is started, fed
statements on stdin, and each answer is terminated by a marker this adapter echoes itself. No
driver, no dependency: a reader reproduces this with the server and a Python interpreter.

**Why port 5433 and not 5432.** The cluster chose it: this workstation publishes the landing page's
containerised PostgreSQL on 5432, so `pg_createcluster` took the next free port. That is worth
naming rather than hiding behind a constant, because a run that reached 5432 would have measured a
container - which requirement 1.2 forbids - and the numbers would have looked ordinary.

**What this comparison does not show, and it is a property of the dataset rather than a setting:**
200 000 rows advance one millisecond per update, so the whole dataset spans about ten seconds of
market time. Time partitioning - the feature TimescaleDB exists for - has almost nothing to work
with at that span. The chunk interval below is one second, which gives the ten chunks such a dataset
can support; on a real feed it would be hours or days. A benchmark that reported this as "measured
TimescaleDB's partitioning" would be measuring a claim it never made.
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path

from .base import LoadResult, QueryResult

PORT = 5433                     # the cluster's own choice; 5432 is a container on this machine
CONTAINER_PORT = 5432
DATABASE = "ob_bench"
TABLE = "book"
MARKER = "__END_OF_ANSWER__"

# One second of market time per chunk. The dataset spans about ten seconds, so this is the interval
# that gives TimescaleDB more than one chunk to plan over; see the module docstring for why that is
# a statement about the dataset and not a tuning trick.
CHUNK_INTERVAL_NS = 1_000_000_000

DDL = f"""
DROP TABLE IF EXISTS {TABLE};
CREATE TABLE {TABLE} (
    ts_ns       BIGINT   NOT NULL,
    symbol      TEXT     NOT NULL,
    exchange    TEXT     NOT NULL,
    side        TEXT     NOT NULL,
    level       SMALLINT NOT NULL,
    price_ticks BIGINT   NOT NULL,
    size_lots   BIGINT   NOT NULL
);
SELECT create_hypertable('{TABLE}', 'ts_ns', chunk_time_interval => {CHUNK_INTERVAL_NS});
CREATE INDEX {TABLE}_symbol_ts ON {TABLE} (symbol, ts_ns DESC);
"""


class TimescaleDbSystem:
    name = "timescaledb"

    def __init__(self, port: int = PORT, database: str = DATABASE):
        self._port = port
        self._database = database
        self._session: subprocess.Popen | None = None
        self._prepared = False
        self._version = ""

    # ── The one session ──────────────────────────────────────────────────────

    def _start_session(self) -> subprocess.Popen:
        if self._session is not None and self._session.poll() is None:
            return self._session
        self._session = subprocess.Popen(
            ["psql", "-p", str(self._port), "-d", self._database,
             "--no-psqlrc", "-q", "-A", "-t", "-F", "\t"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1)
        return self._session

    def _ask(self, statement: str) -> list[str]:
        """Send one statement, read until the marker. Two things here are the whole protocol and
        both were found by it breaking.

        **The statement is terminated before the marker is written.** `\\echo` is a meta-command
        psql runs the moment it reads it, so an SQL statement without its semicolon sits in psql's
        buffer while the marker is printed - and then the *next* statement is appended to the
        unterminated one. Measured: `available()` sent `SELECT current_setting('server_version')`
        without a semicolon, read an empty answer, and the following query came back as
        `syntax error at or near "extversion"`. One missing character desynchronises every answer
        after it.

        **An error drains to the marker before it raises.** Otherwise the marker stays in the pipe
        and the next call reads it immediately as an empty answer - the same "two events, one
        value" shape that cost three CI runs in roadmap #86.
        """
        session = self._start_session()
        assert session.stdin is not None and session.stdout is not None
        terminated = statement if statement.lstrip().startswith("\\") or statement.rstrip().endswith(";") \
            else statement.rstrip() + ";"
        session.stdin.write(f"{terminated}\n\\echo {MARKER}\n")
        session.stdin.flush()

        lines: list[str] = []
        failure: str | None = None
        while True:
            line = session.stdout.readline()
            if not line:
                raise RuntimeError(f"the psql session ended while answering: {statement[:120]}")
            text = line.rstrip("\n")
            if text == MARKER:
                break
            if text.startswith(("ERROR:", "FATAL:", "psql:")) and failure is None:
                failure = text
                continue
            if text:
                lines.append(text)
        if failure is not None:
            raise RuntimeError(f"{failure} (while running {statement[:120]})")
        return lines

    # ── Availability and identity ────────────────────────────────────────────

    def available(self) -> tuple[bool, str]:
        if self._port == CONTAINER_PORT:
            return False, (f"port {CONTAINER_PORT} is this machine's containerised PostgreSQL; "
                           f"requirement 1.2 asks for a native install")
        try:
            server = self._ask("SELECT current_setting('server_version')")
            extension = self._ask(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        except FileNotFoundError:
            return False, "psql is not installed (see benchmarks/install_competitors.md)"
        except (RuntimeError, OSError) as exc:
            return False, f"no PostgreSQL on port {self._port}: {exc}"
        if not extension:
            return False, (f"PostgreSQL {server[0] if server else '?'} answers on {self._port} but "
                           f"the timescaledb extension is not installed in {self._database}")
        self._version = f"TimescaleDB {extension[0]} on PostgreSQL {server[0]}"
        return True, ""

    def version(self) -> str:
        """Both halves, read from the server: the extension's number is the one under test and the
        server's is what it runs on. Neither is a constant in this file."""
        if not self._version:
            self.available()
        return self._version or "unreported"

    def config_dump(self) -> str:
        self._prepare()
        settings = self._ask(
            "SELECT name || ' = ' || setting FROM pg_settings WHERE source <> 'default' "
            "AND name NOT LIKE 'log%' ORDER BY name")
        chunks = self._ask(f"SELECT count(*) FROM timescaledb_information.chunks "
                           f"WHERE hypertable_name = '{TABLE}'")
        return ("endpoint: 127.0.0.1:%d/%s (one psql session)\n%s\nchunks after load: %s\n"
                "non-default settings:\n%s" % (
                    self._port, self._database, DDL.strip(),
                    chunks[0] if chunks else "?", "\n".join(settings)))

    def tuning_applied(self) -> list[str]:
        return [
            f"create_hypertable(chunk_time_interval => {CHUNK_INTERVAL_NS:,} ns): one second of "
            f"market time per chunk, which is what a ten-second dataset can support - the module "
            f"docstring says why that is the dataset's limit and not a setting chosen to hurt",
            "index on (symbol, ts_ns DESC): the time-range query filters on both columns",
            "synchronous_commit = off for the bulk load only: the documented setting for loading, "
            "and restored afterwards so the query workloads run under the server's own default",
            "timescaledb-tune applied to the cluster: shared_buffers, work_mem, max_worker_processes "
            "and the rest set by Timescale's own tool for this machine's cores and RAM",
            "one long-lived psql session for every timed statement: measured, a fresh psql costs "
            "40-60 ms, which is ten times the query it carries",
        ]

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def _prepare(self) -> None:
        if self._prepared:
            return
        for statement in DDL.strip().split(";"):
            if statement.strip():
                self._ask(statement + ";")
        self._prepared = True

    def teardown(self) -> None:
        """The table goes, the cluster stays: this adapter did not start the server."""
        try:
            if self._session is not None and self._session.poll() is None:
                self._ask(f"DROP TABLE IF EXISTS {TABLE}")
        except (RuntimeError, OSError):
            pass
        if self._session is not None:
            try:
                if self._session.stdin is not None:
                    self._session.stdin.close()
                self._session.wait(timeout=10)
            except (subprocess.TimeoutExpired, OSError):
                self._session.kill()
            self._session = None

    # ── Workloads ────────────────────────────────────────────────────────────

    def load(self, csv_path: Path) -> LoadResult:
        """`\\copy` rather than server-side `COPY`, and the reason is the filesystem rather than a
        preference: the dataset lives under `/home/km`, which the `postgres` user cannot traverse,
        so a server-side read would fail with a permission error that looks like a bug in the
        harness. `\\copy` streams the file from the client, which is also what a person loading a
        CSV actually types."""
        self._prepare()
        self._ask("SET synchronous_commit = off")
        started = time.perf_counter()
        self._ask(f"\\copy {TABLE} FROM '{csv_path}' WITH (FORMAT csv, HEADER true)")
        elapsed = time.perf_counter() - started
        self._ask("SET synchronous_commit = on")
        rows = self._ask(f"SELECT count(*) FROM {TABLE}")
        return LoadResult(rows_loaded=int(rows[0]), seconds=elapsed)

    def query_time_range(self, start_ns: int, end_ns: int) -> QueryResult:
        self._prepare()
        query = (f"SELECT ts_ns, price_ticks, size_lots FROM {TABLE} "
                 f"WHERE symbol = 'SYM0000' AND ts_ns BETWEEN {start_ns} AND {end_ns}")
        started = time.perf_counter()
        out = self._ask(query)
        elapsed = time.perf_counter() - started
        rows = [tuple(int(cell) for cell in line.split("\t")) for line in out]
        return QueryResult(rows=rows, seconds=elapsed)

    def query_vwap(self, symbol: str, at_ns: int) -> QueryResult:
        """VWAP over rows selected by timestamp - a different question from the one our engine
        answers over the live book. `equivalence.py` is what refuses the pair."""
        self._prepare()
        query = (f"SELECT sum(price_ticks::numeric * size_lots) / sum(size_lots) FROM {TABLE} "
                 f"WHERE symbol = '{symbol}' AND ts_ns <= {at_ns}")
        started = time.perf_counter()
        out = self._ask(query)
        elapsed = time.perf_counter() - started
        return QueryResult(rows=[(out[0] if out else None,)], seconds=elapsed)
