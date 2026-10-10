"""Every adapter stops its query clock at the same point: once the answer is rows of Python ints, each
system's through the fastest Python client measured for it (#226).

The engine's adapter parsed its answer inside the timed window and the three others outside it,
while the note under the published table said every figure included that parse. Each adapter read
correctly on its own; nothing compared where their clocks stopped. And the two competitors were
asked through their slowest clients, which the unequal clocks had hidden.

Here building the rows is the only thing that moves the clock. `time.perf_counter` reads a counter
that advances one tick a cell: through `int`, shadowed in the modules that parse text in Python, and
through the drivers, faked, where a driver builds the rows. A window that holds the rows reads one
tick a cell and a window that stops before them reads none. No transport is opened.
"""
from __future__ import annotations

import builtins
import subprocess
import sys
import time
from pathlib import Path

import pytest

import orderbook_engine
from benchmarks.comparative.systems import clickhouse, kdb, orderbook, timescaledb

ANSWER = [(1_700_000_000_000_000_000, 100_241, 7), (1_700_000_000_001_000_000, 100_242, 9)]
CELLS = sum(len(row) for row in ANSWER)


def tsv(rows: list[tuple]) -> str:
    return "".join("\t".join(str(cell) for cell in row) + "\n" for row in rows)


class Clock:
    """Time that passes only while a cell of the answer becomes a Python int."""

    def __init__(self) -> None:
        self.ticks = 0

    def now(self) -> float:
        return float(self.ticks)

    def counting_int(self, value):
        self.ticks += 1
        return builtins.int(value)

    def rows(self) -> list[tuple]:
        """What a driver hands back, and the time it takes building it."""
        self.ticks += CELLS
        return list(ANSWER)


class FakeSocket:
    """The engine's answer to one query, as its server sends it: one chunk ending in a blank line."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def sendall(self, data: bytes) -> None:
        pass

    def recv(self, size: int) -> bytes:
        chunk, self._payload = self._payload, b""
        return chunk


class FakeEngine:
    """The engine's Python client, answering a query with the reply its server sends, read by the
    client's own `query_rows()` parse."""

    def __init__(self, reply: str) -> None:
        self.reply = reply

    def query_rows(self, sql: str) -> orderbook_engine.QueryRows:
        return orderbook_engine._parse_query_rows(self.reply)

    def close(self) -> None:
        pass


class FakeClickHouseDriver:
    """clickhouse_driver.Client: `execute()` answers rows of Python ints."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.settings: list[dict | None] = []

    def execute(self, query: str, settings: dict | None = None) -> list[tuple]:
        self.settings.append(settings)
        return self.clock.rows()

    def disconnect(self) -> None:
        pass


class FakePsycopgCursor:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        pass

    def execute(self, query: str) -> None:
        pass

    def fetchall(self) -> list[tuple]:
        return self.clock.rows()


class FakePsycopg:
    """psycopg.Connection: rows are built in `fetchall()`, which is where its time goes."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.binary: list[bool] = []

    def cursor(self, binary: bool = False) -> FakePsycopgCursor:
        self.binary.append(binary)
        return FakePsycopgCursor(self.clock)

    def close(self) -> None:
        pass


def the_engine(clock, tmp_path, monkeypatch):
    system = orderbook.OrderbookSystem(tmp_path / "ob_tcp_server", 1, tmp_path)
    monkeypatch.setattr(system, "_ensure_running", lambda: None)
    system._engine = FakeEngine("OK\ntimestamp_ns\tprice\tquantity\n" + tsv(ANSWER) + "\n")
    # The client converts the answer, so `int` is counted in its module (#229).
    monkeypatch.setattr(orderbook_engine, "int", clock.counting_int, raising=False)
    return system


def clickhouse_through_its_driver(clock, tmp_path, monkeypatch):
    system = clickhouse.ClickHouseSystem()
    system._prepared = True
    system._client = FakeClickHouseDriver(clock)
    monkeypatch.setattr(system, "_ask", lambda query, **kwargs: "")
    return system


def timescaledb_through_psycopg(clock, tmp_path, monkeypatch):
    system = timescaledb.TimescaleDbSystem()
    system._prepared = True
    system._conn = FakePsycopg(clock)
    return system


def kdb_through_q(clock, tmp_path, monkeypatch):
    system = kdb.KdbSystem()
    monkeypatch.setattr(system, "_refuse_if_absent", lambda: None)
    monkeypatch.setattr(kdb.subprocess, "run", lambda args, **kwargs: subprocess.CompletedProcess(
        args, 0, stdout=tsv(ANSWER), stderr=""))
    monkeypatch.setattr(kdb, "int", clock.counting_int, raising=False)
    return system


@pytest.mark.parametrize("adapter", [the_engine, clickhouse_through_its_driver,
                                     timescaledb_through_psycopg, kdb_through_q],
                         ids=lambda adapter: adapter.__name__)
def test_every_adapter_times_the_rows_of_its_answer(adapter, tmp_path, monkeypatch):
    clock = Clock()
    system = adapter(clock, tmp_path, monkeypatch)
    monkeypatch.setattr(time, "perf_counter", clock.now)
    try:
        result = system.query_time_range(ANSWER[0][0], ANSWER[-1][0])
    finally:
        system.teardown()
    assert result.rows == ANSWER
    assert result.seconds == CELLS, (
        f"{system.name}'s query clock read {result.seconds:g} ticks over an answer of {CELLS} "
        f"cells, where every adapter's reads one a cell: its clock does not stop where the others' "
        f"do, so building the rows is charged to some systems' figures and not to others'")


def test_clickhouse_keeps_decompressed_blocks_as_the_engine_keeps_decoded_columns(tmp_path,
                                                                                  monkeypatch):
    # The engine holds a segment's decoded columns between queries by default since #220. ClickHouse
    # has the same kind of cache, off by default; a time-range query asked again without it would
    # time ClickHouse decompressing what the engine keeps.
    system = clickhouse_through_its_driver(Clock(), tmp_path, monkeypatch)
    driver = system._client
    system.query_time_range(ANSWER[0][0], ANSWER[-1][0])
    assert driver.settings == [{"use_uncompressed_cache": 1}], (
        f"ClickHouse's time-range query was sent with settings {driver.settings}, not with its "
        f"uncompressed cache on")
    assert any(line.startswith("use_uncompressed_cache = 1") for line in system.tuning_applied()), (
        "the setting is applied but not declared, and the report lists what was raised in each "
        "system's favour")


def test_timescaledb_answers_in_binary_as_its_fastest_client_measured(tmp_path, monkeypatch):
    system = timescaledb_through_psycopg(Clock(), tmp_path, monkeypatch)
    driver = system._conn
    system.query_time_range(ANSWER[0][0], ANSWER[-1][0])
    assert driver.binary == [True], (
        "the time-range query asked psycopg for text results, measured slower than binary ones")


@pytest.mark.parametrize("system, module", [(clickhouse.ClickHouseSystem, "clickhouse_driver"),
                                            (timescaledb.TimescaleDbSystem, "psycopg")])
def test_a_missing_client_is_named_rather_than_replaced_by_a_slower_one(system, module,
                                                                        monkeypatch):
    monkeypatch.setitem(sys.modules, module, None)    # `import` raises ImportError
    ok, why = system().client_available()
    assert not ok and module.replace("_", "-") in why and "install_competitors.md" in why, why


def test_the_runner_reports_a_system_without_its_client_instead_of_timing_it():
    # The same shape as the refusal of an untuned system: the reason reaches the system's entry,
    # and the loop goes on to the next system.
    source = (Path(__file__).resolve().parents[1] / "run.py").read_text()
    check = source.split("system.client_available()", 1)
    assert len(check) == 2, "run.py no longer asks each system for its client"
    body = check[1].split("\n\n", 1)[0]
    assert "entries.append" in body and '"available": False' in body and "why" in body, (
        "a system whose client is missing is not recorded as not measured, with the reason")
    assert "continue" in body, "a system whose client is missing is still timed"


def test_the_engine_adapter_reads_its_columns_by_the_names_the_client_reads(tmp_path):
    # The client answers the columns in the order the server sent them, and the adapter picks the
    # three it compares by name - so a header in another order is read right and one without them
    # is refused rather than read by position. The client's own refusals are its tests' (#229).
    system = orderbook.OrderbookSystem(tmp_path / "ob_tcp_server", 1, tmp_path)
    system._engine = FakeEngine("OK\nprice\ttimestamp_ns\tquantity\n"
                                + "".join(f"{p}\t{t}\t{q}\n" for t, p, q in ANSWER) + "\n")
    assert system._raw_rows("SELECT") == ANSWER
    system._engine = FakeEngine("OK\ntimestamp_ns\tprice\n" + tsv([row[:2] for row in ANSWER]) + "\n")
    with pytest.raises(RuntimeError, match="does not name the columns"):
        system._raw_rows("SELECT")
    system.teardown()


def test_timed_calls_run_with_the_garbage_collector_held_off():
    # #226: a collection triggered inside a timed call is the harness's pause and not the system's,
    # and it fell on every system's samples and on the floor's control pairs. A run of samples is
    # taken with the collector off, as `timeit` takes one, and it is on again afterwards.
    import gc
    from benchmarks.comparative import run
    from benchmarks.comparative.systems.base import QueryResult

    seen: list[bool] = []

    def call() -> QueryResult:
        seen.append(gc.isenabled())
        return QueryResult(rows=[], seconds=0.001)

    assert gc.isenabled()
    run.timed(call, rounds=5)
    assert seen and not any(seen), f"the collector was on during {sum(seen)} of {len(seen)} timed calls"
    assert gc.isenabled(), "the collector was left off"
    with run.collector_held_off():
        assert not gc.isenabled()
    assert gc.isenabled()
    # The floor's control samples and the engine's unparsed replies are timed in main(), which needs
    # servers, so this reads that they are taken inside the same block.
    source = (Path(__file__).resolve().parents[1] / "run.py").read_text()
    assert "with collector_held_off():\n            floor = resolution.measure(" in source, (
        "the floor's control samples are timed with the collector on")
    assert "with collector_held_off():\n                replies = sorted(reference.reply_seconds(" in source, (
        "the engine's unparsed replies are timed with the collector on")
