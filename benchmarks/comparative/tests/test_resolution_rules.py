"""Tests for the rules the harness refuses on, including the one that is static.

The static test is here rather than in a review checklist because "do not call a difference below
the noise floor faster" broken in some other file looks exactly like the rule not existing.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from benchmarks.comparative import equivalence, resolution

PACKAGE = Path(__file__).resolve().parent.parent

# The one file allowed to produce a comparative claim. An allowlist over the whole package rather
# than a list of files to check: a static test naming four files and silent about a fifth is a
# mistake this workspace has already made.
CLAIM_WORDS = re.compile(r"\b(faster|slower|szybszy|wolniejszy)\b", re.IGNORECASE)
CLAIM_OWNER = "resolution.py"


def _docstring_nodes(tree: ast.AST) -> set[int]:
    """Ids of the Constant nodes that are docstrings, so prose is exempt from the rule.

    The first version of this test matched the *words* anywhere in the file and fired immediately on
    `hardware.py`, which says "Debug is 3-4x slower on this engine" - a true statement about build
    types and not a claim about two systems. Adding an exception for that file would have been the
    wrong repair: it is the same substring trap as matching `composite_keys` while looking for
    credentials, and the fix is structural. What the rule actually forbids is *producing* a verdict,
    so the check looks at string literals that reach output and skips docstrings. Comments never
    appear in the AST at all, so a comment explaining the rule cannot trip it.
    """
    ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", None)
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                ids.add(id(body[0].value))
    return ids


def _files_under_review() -> list[Path]:
    """Every module that can produce a report, which is the package minus two categories.

    `resolution.py` owns the verdict. `tests/` is excluded as a *category* rather than as a file
    exception - a test asserting that `classify()` returns "faster" is doing its job, and one file
    exempted by name is how an allowlist starts drifting. The two exclusions are asserted below, so
    widening them is a visible change rather than a quiet one.
    """
    return [path for path in sorted(PACKAGE.rglob("*.py"))
            if path.name != CLAIM_OWNER and "tests" not in path.parts]


def test_only_resolution_may_produce_a_faster_verdict():
    reviewed = _files_under_review()

    # A static test that scans nothing passes. This package has five modules plus two `__init__`
    # files, so a path mistake that empties the list has to fail here rather than read as clean -
    # the same reason the CI skip gate counts skips instead of looking for the word.
    assert len(reviewed) >= 5, f"the scan found only {len(reviewed)} files, so it is not scanning"
    assert (PACKAGE / CLAIM_OWNER).is_file(), "the file that owns the verdict is missing"

    offenders = []
    for path in reviewed:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        exempt = _docstring_nodes(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if id(node) in exempt:
                continue
            if CLAIM_WORDS.search(node.value):
                offenders.append(
                    f"{path.relative_to(PACKAGE)}:{node.lineno}: {node.value.strip()[:70]!r}")
    assert not offenders, (
        "only resolution.classify() may produce a verdict calling one system faster than another, "
        "because it is the only place that knows the noise floor:\n" + "\n".join(offenders))


def test_the_historical_control_run_refuses_a_thirty_percent_difference():
    """A control BM_VwapLatency once reported -40.6% for an identical function, 8 rounds of 8.

    This asserts the justification for the mechanism rather than its arithmetic: with that floor, a
    30% difference must not be called faster, however tempting the number looks in a table.
    """
    historical = resolution.Resolution(
        rounds=8, warmup=0, discarded_outlier=None, control_ratios=[0.594] * 8, floor=0.406,
        verdict=resolution.USABLE, note="historical i3-7100U control")

    assert resolution.classify(0.70, 1.00, historical) == resolution.INDISTINGUISHABLE
    assert resolution.classify(0.50, 1.00, historical) == "faster"


def test_measure_interleaves_rather_than_blocking():
    """Blocks measure the machine drifting; interleaving measures round-to-round noise.

    A blocked control would report a smaller floor than the comparison is exposed to, and a
    flattered floor is precisely the failure this module exists to prevent. Asserted by giving the
    sampler a value that alternates: under interleaving the ratio is off 1.0 every round, under
    blocking it would be 1.0 for all but the seam.
    """
    # `warmup=0` because this test is about the ratio arithmetic, not the warm-up - which has its
    # own test below. Passing it explicitly rather than relying on the default keeps the two
    # properties separable.
    values = iter([1.0, 2.0] * 6)
    result = resolution.measure(lambda: next(values), rounds=6, warmup=0)
    assert result.rounds == 6
    assert all(abs(ratio - 0.5) < 1e-9 for ratio in result.control_ratios)
    assert abs(result.floor - 0.5) < 1e-9


def test_verdict_downgrades_when_the_floor_swallows_the_run():
    # A varying sampler, because a constant one is now refused - this test used a constant and the
    # new guard caught it, which is the guard working on its author.
    values = iter([1.0, 1.02] * 3)
    res = resolution.measure(lambda: next(values), rounds=3, warmup=0)
    assert res.verdict == resolution.USABLE
    swallowed = resolution.verdict_for(
        resolution.Resolution(3, 0, None, [1.1, 0.9, 1.0], 0.10, resolution.USABLE, "n"), 0.05)
    assert swallowed.verdict == resolution.CANNOT_RESOLVE
    assert "inside that floor" in swallowed.note


def test_a_row_count_difference_is_named_as_such():
    with pytest.raises(equivalence.EquivalenceError, match="returned 2 rows against"):
        equivalence.require_equivalent("orderbook", [(1,), (2,), (3,)], "clickhouse", [(1,), (2,)])


def test_a_value_difference_names_the_column():
    """The reason values are compared rather than checksummed: a checksum cannot say which column."""
    with pytest.raises(equivalence.EquivalenceError, match="column 1"):
        equivalence.require_equivalent(
            "orderbook", [(1, 100), (2, 200)], "timescaledb", [(1, 100), (2, 999)])


def test_row_order_is_not_a_difference():
    equivalence.require_equivalent(
        "orderbook", [(2, 200), (1, 100)], "clickhouse", [(1, 100), (2, 200)])


def test_a_control_run_that_times_nothing_is_refused():
    """A floor of 0.0 lets every difference count as real, so a constant sampler is refused.

    Written after doing it: `run.py` passed a cached load time to `measure()` instead of re-running
    the workload, and a floor of exactly 0.0 made `classify()` report a 1% difference as faster.
    The module was correct and the glue was not, which is where this mistake lives.
    """
    with pytest.raises(ValueError, match="not timing work"):
        resolution.measure(lambda: 1.0, rounds=4, warmup=0)

    # And a sampler that does vary is not refused, so the guard is not just "any small floor".
    values = iter([1.0, 1.0001] * 4)
    result = resolution.measure(lambda: next(values), rounds=4, warmup=0)
    assert result.floor > 0.0


def test_the_warmup_is_discarded_and_recorded():
    """A cold first call must not set the floor, and the count must reach the report.

    Measured on the harness's first real run: control ratios [1.6785, 0.9837, 0.9993, 0.9701]. One
    cold call put the floor at 0.68, which would report every real difference as indistinguishable -
    the "always says cannot resolve" failure, arrived at from the opposite direction. With the cold
    call discarded the floor is the 3% the warm pairs actually show.

    Warming only the control would be worse than not warming at all, because it flatters the floor.
    The number is recorded so a reader can see which regime it describes.
    """
    calls: list[int] = []

    def sample() -> float:
        calls.append(len(calls))
        if len(calls) <= 2:
            return 3.0          # the expensive cold calls, which must not reach a ratio
        # Slightly uneven afterwards, because a sampler returning a constant is refused outright by
        # the guard above - the two rules meeting here is them agreeing, not conflicting.
        return 1.0 if len(calls) % 2 else 1.001

    result = resolution.measure(sample, rounds=3, warmup=2)

    assert len(calls) == 2 + 3 * 2, "warm-up calls are not being made, or rounds are not paired"
    assert result.warmup == 2
    assert result.floor == 0.0 or result.floor < 0.01, (
        f"a discarded cold call still reached the floor: {result.control_ratios}")


def test_one_outlier_is_discarded_at_six_rounds_and_published():
    """The floor is the worst deviation with one extreme dropped, and the extreme is not hidden.

    Plain `max` was the first version and is too sharp for a shared machine: a real run of eight
    rounds gave [1.214, 1.05, 0.478, 1.035, 1.003, 1.019, 0.994, 1.001] — five pairs within 3.5%
    and one scheduler hiccup setting the floor at 0.52. A harness that then refuses every comparison
    is the "always says cannot resolve" job the spec warns about, reached from the other side.
    """
    observed = [1.214, 1.05, 0.478, 1.035, 1.003, 1.019, 0.994, 1.001]
    values: list[float] = []
    for ratio in observed:
        values.extend([ratio, 1.0])          # each round is first/second, so ratio = first
    supply = iter(values)

    result = resolution.measure(lambda: next(supply), rounds=8, warmup=0)

    assert result.discarded_outlier == pytest.approx(0.522, abs=1e-3)
    assert result.floor == pytest.approx(0.214, abs=1e-3)
    assert result.control_ratios == pytest.approx(observed), "every ratio must reach the record"
    assert "discarded as an outlier" in result.note


def test_nothing_is_discarded_below_six_rounds():
    """Too few samples to afford a discard, so the worst deviation stands.

    The rounds threshold is what keeps the discard from being a knob: two discards, or a discard at
    four rounds, would be flattery rather than robustness.
    """
    values = [1.5, 1.0, 1.01, 1.0, 1.0, 1.005]
    supply = iter(values)

    result = resolution.measure(lambda: next(supply), rounds=3, warmup=0)

    assert result.discarded_outlier is None
    assert result.floor == pytest.approx(0.5, abs=1e-6)
    assert "discarded" not in result.note
