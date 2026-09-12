"""Every capability this server announces has something that reads it (#105).

`capabilities:` is a published vocabulary, and a name in it is a promise: a client reads the name,
concludes the server can take a field, and sends it. A name that nothing reads is the shape this
workspace has now paid for five times — `provisional`, `basis`, `in_use`, `key_id`, `partition_by` —
each one computed, carried, and consumed by nobody, and the fifth cost an item of its own (#104).

This is a static test: it reads `include/orderbook/capabilities.hpp` and the file each name claims
as its reader. No server, no sockets — the subject is the tree.
"""

from __future__ import annotations

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[2]

# What depends on each announced name, and the marker that proves the dependency exists. Written
# out rather than inferred, because "somebody greps for this string somewhere" is satisfied by a
# comment — and a check satisfied by a cross-reference is worse than no check, since the next
# reader trusts it (the lesson from the Kotlin row that matched the word "Java").
READERS = {
    # The client asks this before sending an event time, and refuses rather than dropping it.
    "insert_event_time": ("python/orderbook_engine/__init__.py",
                          '"insert_event_time" not in self.server_capabilities()'),
    # What makes the answer above worth trusting: a server that refuses an unknown token cannot
    # silently accept a field it does not understand. Pinned by the tests of that refusal.
    "strict_args": ("tests/test_command_arity.cpp",
                    "EveryBoundedCommandRefusesOneTokenTooMany"),
}


def announced_capabilities() -> list:
    """The names the server actually puts on the wire, read from the array it builds them from."""
    source = (REPO / "include/orderbook/capabilities.hpp").read_text()
    body = source.split("kCapabilities[] = {", 1)[1].split("};", 1)[0]
    # String literals only: the array is heavily commented, and a comment mentioning a name is not
    # a name being announced.
    return re.findall(r'"([a-z_]+)"', body)


def test_the_header_announces_at_least_what_this_release_added():
    names = announced_capabilities()
    assert "insert_event_time" in names, (
        f"the parser accepts an event time and the server does not say so: {names}")
    # The guard, mutated with the failure it stands for: with a broken reader above, `names` would
    # be empty and every assertion in this file would pass by checking nothing.
    assert len(names) >= 2, f"only {len(names)} capabilities parsed out of the header - the reader " \
                            f"is not reading it"


def test_every_announced_capability_has_a_reader_and_every_reader_a_capability():
    announced = set(announced_capabilities())
    claimed = set(READERS)

    assert announced - claimed == set(), (
        f"announced and read by nothing: {sorted(announced - claimed)}. A name in this list is a "
        f"promise a client acts on; one that nothing reads is the fifth-instance shape #104 was an "
        f"item about. Either give it a reader or do not announce it yet.")
    assert claimed - announced == set(), (
        f"claimed as read but no longer announced: {sorted(claimed - announced)}")


def test_each_reader_file_really_contains_what_it_claims():
    for name, (relative, marker) in READERS.items():
        path = REPO / relative
        assert path.exists(), f"{name} names {relative}, which does not exist"
        text = path.read_text()
        assert marker in text, (
            f"{name} claims to be read by {relative}, and the marker {marker!r} is not there any "
            f"more - so either the reader moved or the dependency is gone, and the difference "
            f"matters to a client that is trusting this name")
