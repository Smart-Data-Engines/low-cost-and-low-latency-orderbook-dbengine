"""Every adapter stops its query clock at the same point: once the answer is rows (#226).

The engine's adapter parsed its answer inside the timed window and the three others outside it,
while the note under the published table said every figure included that parse. Each adapter read
correctly on its own; nothing compared where their clocks stopped.

Here the parse is the only thing that moves the clock. `time.perf_counter` reads a counter, and
`int` in each adapter's module is shadowed by one that advances it, so a window that holds the
parse reads one tick a cell and a window that stops before it reads none. No transport is opened:
each adapter is handed its own system's answer to the query, in that system's format.
"""
from __future__ import annotations

import builtins
import subprocess
import time

import pytest

from benchmarks.comparative.systems import clickhouse, kdb, orderbook, timescaledb

ANSWER = [(1_700_000_000_000_000_000, 100_241, 7), (1_700_000_000_001_000_000, 100_242, 9)]
CELLS = sum(len(row) for row in ANSWER)


def tsv(rows: list[tuple]) -> str:
    return "".join("\t".join(str(cell) for cell in row) + "\n" for row in rows)


class Clock:
    """Time that passes only while a cell of the answer is converted."""

    def __init__(self) -> None:
        self.ticks = 0

    def now(self) -> float:
        return float(self.ticks)

    def counting_int(self, value):
        self.ticks += 1
        return builtins.int(value)


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
    def close(self) -> None:
        pass


def the_engine(tmp_path, monkeypatch):
    system = orderbook.OrderbookSystem(tmp_path / "ob_tcp_server", 1, tmp_path)
    monkeypatch.setattr(system, "_ensure_running", lambda: None)
    system._engine = FakeEngine()
    payload = ("OK\ntimestamp_ns\tprice\tquantity\n" + tsv(ANSWER) + "\n").encode()
    monkeypatch.setattr(system, "_raw_socket", lambda: FakeSocket(payload))
    return system, orderbook


def clickhouse_over_http(tmp_path, monkeypatch):
    system = clickhouse.ClickHouseSystem()
    system._prepared = True
    monkeypatch.setattr(system, "_ask", lambda query, **kwargs: tsv(ANSWER))
    return system, clickhouse


def timescaledb_through_psql(tmp_path, monkeypatch):
    system = timescaledb.TimescaleDbSystem()
    system._prepared = True
    monkeypatch.setattr(system, "_ask", lambda statement: tsv(ANSWER).splitlines())
    return system, timescaledb


def kdb_through_q(tmp_path, monkeypatch):
    system = kdb.KdbSystem()
    monkeypatch.setattr(system, "_refuse_if_absent", lambda: None)
    monkeypatch.setattr(kdb.subprocess, "run", lambda args, **kwargs: subprocess.CompletedProcess(
        args, 0, stdout=tsv(ANSWER), stderr=""))
    return system, kdb


@pytest.mark.parametrize("adapter", [the_engine, clickhouse_over_http, timescaledb_through_psql,
                                     kdb_through_q], ids=lambda adapter: adapter.__name__)
def test_every_adapter_times_the_parse_of_its_answer(adapter, tmp_path, monkeypatch):
    system, module = adapter(tmp_path, monkeypatch)
    clock = Clock()
    monkeypatch.setattr(time, "perf_counter", clock.now)
    monkeypatch.setattr(module, "int", clock.counting_int, raising=False)
    try:
        result = system.query_time_range(ANSWER[0][0], ANSWER[-1][0])
    finally:
        system.teardown()
    assert result.rows == ANSWER
    assert result.seconds == CELLS, (
        f"{system.name}'s query clock read {result.seconds:g} ticks over an answer of {CELLS} "
        f"cells, where every adapter's reads one a cell: its clock does not stop where the others' "
        f"do, so the parse is charged to some systems' figures and not to others'")
