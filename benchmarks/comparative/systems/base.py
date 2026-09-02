"""One adapter per system, and the differences named in it rather than in the measurement.

The same reasoning as the engine adapters in the flagship product: a `if system == "clickhouse"`
branch inside the timing loop spreads each system's quirks across the code that is supposed to be
identical for all of them, and then the quirks are what gets measured.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


class NoTuningDeclared(RuntimeError):
    """A competitor that declares no tuning measures our effort, not its engine.

    Refused rather than defaulted, and refused loudly: an untuned competitor produces a flattering
    number that looks exactly like a fair one. Requirement 4.2 exists because a table where we win
    everywhere reads as selected, and usually is.
    """


@dataclass(frozen=True)
class LoadResult:
    rows_loaded: int
    seconds: float


@dataclass(frozen=True)
class QueryResult:
    """Rows *and* time, and the rows are not optional.

    Without them there is nothing to check equivalence against, and two different queries time just
    as cleanly as two equivalent ones. `equivalence.py` compares these before anything is timed.
    """
    rows: list[tuple]
    seconds: float


@runtime_checkable
class System(Protocol):
    name: str

    def available(self) -> tuple[bool, str]:
        """(True, "") or (False, reason). The reason is part of the answer: it goes in the table.

        A system that is absent must appear as `NOT MEASURED (reason)` and never as a blank cell -
        a skip nobody can see reads as a pass, which is the lesson the CI skip gate came from.
        """

    def version(self) -> str:
        """Read from the running system, never from documentation or a constant."""

    def config_dump(self) -> str:
        """The configuration this run actually used, published with the results."""

    def tuning_applied(self) -> list[str]:
        """What we raised in its favour. An empty list raises NoTuningDeclared."""

    def load(self, csv_path: Path) -> LoadResult: ...

    def query_time_range(self, start_ns: int, end_ns: int) -> QueryResult: ...

    def query_vwap(self, symbol: str, at_ns: int) -> QueryResult: ...

    def teardown(self) -> None: ...


def require_tuning(system: System) -> list[str]:
    tuning = system.tuning_applied()
    if not tuning:
        raise NoTuningDeclared(
            f"system {system.name} declares no tuning; an untuned competitor measures our effort, "
            f"not its engine")
    return tuning
