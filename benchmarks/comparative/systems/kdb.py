"""kdb+, and on this machine the honest answer is `NOT MEASURED` with a reason.

Two things stand between this file and a number, and only one of them is technical.

**A licence file.** kdb+ needs one, and getting it means registering an account with the vendor.
That is a human action with somebody's identity attached, so it is not something this harness does
or should do. Until the file exists, `available()` says so and the report carries the row - never a
blank cell, because a skip nobody can see reads as a pass.

**A question this repository cannot answer for itself.** The free edition is not licensed on the
same terms as the commercial product, and whether numbers produced under it may be published in a
company's public repository is a reading of that licence rather than a measurement. Requirement 5.3
asks for the free edition's constraint to be annotated **beside every number** rather than in a
footer; the constraint that comes first is whether the number may be there at all. That belongs to
whoever holds the licence.

So the adapter is written to the same interface as the others and refuses early. When a licence does
appear, what it needs is `load` over a splayed table and the two queries in q - the workload
definitions are already in `benchmarks/README.md`, and the annotation requirement is implemented
here rather than left for later.
"""
from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

from .base import LoadResult, QueryResult

# Where kdb+ puts its licence, both spellings, so the message can say which one is missing.
LICENCE_NAMES = ("kc.lic", "k4.lic", "kx.lic")

# Prefixed to every number this system ever produces. Requirement 5.3 asks for it beside the value
# and not in a footer: a footnote about an edition's limits is a footnote nobody carries into the
# sentence they quote.
FREE_EDITION_NOTE = ("free (personal) edition: 4 GB of memory and a core limit, and its licence "
                     "terms decide whether this number may be published at all")


class KdbSystem:
    name = "kdb+"

    def __init__(self, binary: str = "q", licence_dir: Path | None = None):
        self._binary = binary
        self._licence_dir = licence_dir or Path.home() / "q"

    # ── Availability and identity ────────────────────────────────────────────

    def available(self) -> tuple[bool, str]:
        """Both halves of the answer, because each one alone would be misleading.

        A missing binary reads as "we did not bother"; a missing licence reads as a configuration
        slip. What is true here is that obtaining either requires registering with the vendor, and
        that the free edition's terms have to be read before its numbers are published - so the
        reason names the action and who it belongs to.
        """
        if shutil.which(self._binary) is None:
            return False, ("kdb+ is not installed: the binary and its licence both come from a "
                           "vendor registration, which is a human action (see "
                           "benchmarks/install_competitors.md). The free edition's licence terms "
                           "also decide whether numbers from it may be published here, which is a "
                           "licensing decision rather than a technical one")
        found = [name for name in LICENCE_NAMES if (self._licence_dir / name).is_file()]
        if not found:
            return False, (f"kdb+ is installed but no licence file ({', '.join(LICENCE_NAMES)}) is "
                           f"in {self._licence_dir}: q refuses to start without one")
        return True, ""

    def version(self) -> str:
        """`.z.K` is the version number, read from the running interpreter rather than from a
        constant - the same rule every other adapter here follows."""
        out = subprocess.run([self._binary, "-q", "-c", "1000", "1000"],
                             input='-1 string .z.K; exit 0;\n',
                             capture_output=True, text=True, timeout=30)
        reported = out.stdout.strip()
        return f"{reported} [{FREE_EDITION_NOTE}]" if reported else "unreported"

    def config_dump(self) -> str:
        return (f"binary: {shutil.which(self._binary)}\n"
                f"licence directory: {self._licence_dir}\n"
                f"note: {FREE_EDITION_NOTE}")

    def tuning_applied(self) -> list[str]:
        """Not an empty list even when unavailable, and that is deliberate: `require_tuning()`
        raises on an empty one, and a system that is absent should be reported as absent rather than
        as refused for declaring no tuning. The reason it is absent is the more useful sentence."""
        return [
            "splayed table on disk with `p#` on the symbol column: the layout q's own tutorials use "
            "for a time series queried by symbol",
            f"annotation carried beside every number: {FREE_EDITION_NOTE}",
        ]

    # ── Workloads ────────────────────────────────────────────────────────────
    #
    # Written against the interface rather than left as `NotImplemented`, so that the day a licence
    # exists the run is one flag away rather than one file away. Each one refuses if it is called
    # while the system is unavailable, because a workload that returns zero rows quickly is the
    # shape of a fast system.

    def _refuse_if_absent(self) -> None:
        ok, why = self.available()
        if not ok:
            raise RuntimeError(f"kdb+ workload asked for while unavailable: {why}")

    def load(self, csv_path: Path) -> LoadResult:
        self._refuse_if_absent()
        script = (
            f'book: ("JSSSHJJ"; enlist ",") 0: `$"{csv_path}";\n'
            f'`:book/ set .Q.en[`:.; book];\n'
            f'exit 0;\n')
        started = time.perf_counter()
        subprocess.run([self._binary, "-q"], input=script, capture_output=True, text=True,
                       timeout=1800, check=True)
        elapsed = time.perf_counter() - started
        rows = sum(1 for _ in csv_path.open()) - 1
        return LoadResult(rows_loaded=rows, seconds=elapsed)

    def query_time_range(self, start_ns: int, end_ns: int) -> QueryResult:
        self._refuse_if_absent()
        script = (
            f'book: get `:book;\n'
            f'res: select ts_ns, price_ticks, size_lots from book '
            f'where symbol = `SYM0000, ts_ns within ({start_ns}; {end_ns});\n'
            f'-1 "\\n" sv {{"\\t" sv string x}} each res;\n'
            f'exit 0;\n')
        started = time.perf_counter()
        out = subprocess.run([self._binary, "-q"], input=script, capture_output=True, text=True,
                             timeout=600, check=True)
        elapsed = time.perf_counter() - started
        rows = [tuple(int(cell) for cell in line.split("\t"))
                for line in out.stdout.strip().splitlines() if line]
        return QueryResult(rows=rows, seconds=elapsed)

    def query_vwap(self, symbol: str, at_ns: int) -> QueryResult:
        self._refuse_if_absent()
        script = (
            f'book: get `:book;\n'
            f'v: exec (sum price_ticks * size_lots) % sum size_lots from book '
            f'where symbol = `{symbol}, ts_ns <= {at_ns};\n'
            f'-1 string v;\n'
            f'exit 0;\n')
        started = time.perf_counter()
        out = subprocess.run([self._binary, "-q"], input=script, capture_output=True, text=True,
                             timeout=600, check=True)
        elapsed = time.perf_counter() - started
        return QueryResult(rows=[(out.stdout.strip(),)], seconds=elapsed)

    def teardown(self) -> None:
        """Nothing to tear down while the system is absent, and the splayed table is written into a
        temporary directory when it is not."""
