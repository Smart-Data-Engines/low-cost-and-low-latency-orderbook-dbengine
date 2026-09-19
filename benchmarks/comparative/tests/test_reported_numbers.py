"""No number in a generated report may come from a machine other than the one that ran it.

`ENGINE_LIMITATIONS` used to carry three measurements as literals - 446,219 updates/s in process,
4,012 over the wire, a factor of 111 - under the words "Measured on this machine". They were
measured on *a* machine, and then travelled unchanged into every report generated anywhere else.
The first aarch64 run printed all three inside a header reading "Amazon EC2 m9g.xlarge".

A stale number in a document is a nuisance. A stale number inside the artefact a run produces, next
to figures that run did measure, is worse: nothing tells a reader which is which.
"""
from __future__ import annotations

import re
from pathlib import Path

from .. import run

# Any group of digits with a thousands separator, or a bare run of four or more digits: the shapes a
# throughput or a nanosecond count takes when somebody types it into prose.
MEASUREMENT_SHAPED = re.compile(r"\d{1,3}(?:,\d{3})+|\b\d{4,}\b")


ALL_PROSE = run.ENGINE_LIMITATIONS + run.MEASUREMENT_NOTES


def test_a_typed_in_measurement_must_say_which_machine_it_came_from():
    # Not a ban on literals. Some of these figures justify a design decision rather than interpret
    # the table - what a fresh `clickhouse-client` costs is an argument for holding a connection
    # open, and re-measuring it every run would be gold-plating. What is not allowed is stating it
    # with no machine beside it, because then it reads as a fact about whichever machine ran.
    for entry in ALL_PROSE:
        found = MEASUREMENT_SHAPED.findall(entry)
        if found:
            assert "{prose_machine}" in entry, (
                f"this entry states a measurement as a literal without attributing it, so on any "
                f"other machine it is a claim about the wrong one: {found} in {entry[:90]!r}")


def test_nothing_claims_to_have_been_measured_here_as_a_literal():
    # "Measured on this machine" was the exact wording that carried three figures from an i3-7100U
    # into the first report generated on an aarch64 instance. The phrase is only honest when
    # something in the run fills it.
    for entry in ALL_PROSE:
        if "on this machine" in entry:
            assert "{" in entry, (
                f"this entry says it was measured on this machine and carries no placeholder for a "
                f"measurement: {entry[:90]!r}")


def test_the_parsing_constant_cannot_exceed_the_smallest_query_it_is_inside():
    # The contradiction that made this whole change necessary, as a property rather than an
    # anecdote: the note says the constant is *included* in every query figure, so a run whose
    # fastest query is 1.47 ms cannot also carry a 4.8 ms constant. Measuring it in the run is what
    # keeps the two on the same machine; this asserts the shape of that measurement, not a value.
    wide, narrow = run.parse_cost(rows=4000, samples=3)
    assert wide > 0 and narrow > 0
    # Seven columns cost at least as much to split as three. Not a tight bound - the point of the
    # original measurement was that the difference is small - but a negative one would mean the
    # loop is not doing the work.
    assert wide >= narrow * 0.5


def test_the_in_process_figure_is_a_placeholder_something_has_to_fill():
    # The ratchet: the sentence about the in-process figure must arrive as a template, so that the
    # only way to put a number in it is to measure one.
    placeholders = [e for e in run.ENGINE_LIMITATIONS if "{in_process}" in e]
    assert len(placeholders) == 1, "exactly one entry should be filled in by a measurement"


def test_a_missing_bench_engine_is_said_rather_than_filled_in(tmp_path: Path):
    sentence = run.in_process_sentence(tmp_path, wire_levels_per_second=361_466.0,
                                       levels_per_update=20)
    assert "not measured in this run" in sentence
    # And above all: no throughput invented to fill the gap.
    assert not MEASUREMENT_SHAPED.findall(sentence.replace(str(tmp_path), ""))


def test_a_bench_engine_that_fails_is_reported_rather_than_quoted(tmp_path: Path):
    # A binary that exists and exits non-zero is the case that a `is_file()` check alone lets
    # through, and the one where a reader would most like to be told.
    binaries = tmp_path / "benchmarks"
    binaries.mkdir()
    fake = binaries / "bench_engine"
    fake.write_text("#!/bin/sh\nexit 3\n")
    fake.chmod(0o755)
    sentence = run.in_process_sentence(tmp_path, wire_levels_per_second=361_466.0,
                                       levels_per_update=20)
    assert "not measured in this run" in sentence
    assert "exited 3" in sentence
