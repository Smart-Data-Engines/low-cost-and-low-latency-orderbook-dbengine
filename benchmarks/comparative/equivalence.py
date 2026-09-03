"""Do two systems answer the same question before we time how fast they answer it.

Two different queries time just as cleanly as two equivalent ones, and the fast one wins. So every
workload is run once for its rows, compared against the reference system, and only then timed. A
system whose rows differ is dropped from that workload with the difference named - not silently
excluded, and not measured anyway.

Values rather than checksums, and for the reason the flagship product's migration verification
settled on the same thing: a checksum says "different", a value says **which column**.
"""
from __future__ import annotations

from dataclasses import dataclass


class EquivalenceError(RuntimeError):
    """Named difference, so the message is a diagnosis rather than a verdict."""


@dataclass(frozen=True)
class Difference:
    row: int
    column: int
    reference: object
    candidate: object

    def describe(self, reference_name: str, candidate_name: str) -> str:
        return (f"row {self.row}, column {self.column}: {reference_name} has "
                f"{self.reference!r}, {candidate_name} has {self.candidate!r}")


def canonical(rows: list[tuple]) -> list[tuple]:
    """Sort rows so that a system free to return them in any order is not penalised for it.

    Sorted on the whole tuple as strings: the systems disagree about integer widths and about
    whether a timestamp comes back aware or naive, and a sort key that depends on the type would
    order two equivalent result sets differently. The comparison below still compares values, so
    normalising the *order* here does not hide a difference in the *content*.
    """
    return sorted(rows, key=lambda row: tuple(str(cell) for cell in row))


def compare(reference: list[tuple], candidate: list[tuple]) -> Difference | None:
    """The first difference, or None. Row count first, because it is the cheaper answer."""
    if len(reference) != len(candidate):
        return Difference(row=-1, column=-1, reference=len(reference), candidate=len(candidate))

    for row_index, (left, right) in enumerate(zip(canonical(reference), canonical(candidate))):
        if len(left) != len(right):
            return Difference(row=row_index, column=-1, reference=len(left), candidate=len(right))
        for column_index, (a, b) in enumerate(zip(left, right)):
            # Compared as text on purpose. The same value arrives as int, Decimal or str depending
            # on the driver, and `1 != Decimal("1")` is false in Python but true for `int` vs `str`.
            # Text is the one representation all four systems can produce without loss, which is
            # also why timestamps travel as integer nanoseconds everywhere.
            if str(a) != str(b):
                return Difference(row=row_index, column=column_index, reference=a, candidate=b)
    return None


def require_equivalent(
    reference_name: str, reference: list[tuple],
    candidate_name: str, candidate: list[tuple],
) -> None:
    difference = compare(reference, candidate)
    if difference is None:
        return
    if difference.row == -1:
        raise EquivalenceError(
            f"{candidate_name} returned {difference.candidate} rows against {reference_name}'s "
            f"{difference.reference}; a query that answers a different question is not comparable")
    raise EquivalenceError(
        f"{candidate_name} disagrees with {reference_name} at "
        f"{difference.describe(reference_name, candidate_name)}")
