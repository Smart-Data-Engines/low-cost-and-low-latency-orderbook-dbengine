"""#54 stage B: what a node does when the coordinator is unreachable.

Three windows, and they exercise different branches of `monitor_loop()`, which is why they are
three tests rather than one parameterised one:

- **absent before registration** — a node comes up into a world with no coordinator at all
- **absent during operation** — a healthy cluster loses etcd underneath it
- **absent in the election window** — the leader key is gone *and* nobody can read that it is gone

The assertions are about **refusals, not timings**, because the promise this stage checks is a
refusal. #82 is the reason it is worth checking at all: `get_cluster_state()` used to answer
`std::nullopt` for a coordinator that could not be reached *and* for a leader key that genuinely
was not there, so a replica campaigned because a read had failed. Two of those branches now exist
to say "no information", and a test that only watched roles could not tell a node that declined to
campaign from a node that had nothing to campaign for.

**Every refusal here is paired with a control that must open.** A test asserting "nobody promoted
itself" passes against a cluster that can never promote anybody, which is the one outcome that
would make this whole module worthless — so each test brings etcd back and requires the election
to happen. That pairing is the shape this repository has paid for repeatedly: an assertion of
absence needs a run in which the thing is present.

Each test owns its `ClusterManager`. The session fixtures cannot be used: stopping etcd under a
session-scoped cluster poisons every test that runs after it, and a module-scoped one would make
the three windows depend on each other's order.
"""

from __future__ import annotations

import pathlib
import re
import time

import pytest

from conftest import (ClusterManager, node_log_since, node_log_size, patience, role_of,
                      send_command, wait_for_role)

pytestmark = pytest.mark.failover


# The engine's own default, read from `docs/cli.md` rather than guessed: a holder that cannot
# confirm ownership for one whole TTL steps down, so every "did it step down" wait is derived from
# this rather than from a number that looked generous.
LEASE_TTL_SECONDS = 10

# Every sentence `src/failover.cpp` can log when the holder gives the role up, because there is
# more than one route and this test must not care which. Written down rather than reduced to one
# because a test that pins a single phrase fails when a *better* route is taken - which is what
# happened here: the first version required "could not confirm the leader key", the TTL branch,
# while an unreachable coordinator fails the lease keepalive and demotes on the next tick.
STEP_DOWN_REASONS = (
    "refresh_lease failed",                  # the keepalive could not be sent or was refused
    "could not confirm the leader key",      # reads unavailable for a whole TTL
    "the leader key is gone",                # a confirmed absence
)

# Reported as well as asserted: "how long does a holder keep the role after its coordinator
# vanishes" is a number an operator plans around, and an assertion that passes prints nothing.
custom_metrics: dict = {}


def one_primary(mgr: ClusterManager, timeout: float) -> int:
    """Index of the single node reporting PRIMARY, or fail saying what each one said.

    Exactly one, asserted rather than "the first one found": #61's lesson is that "exactly one
    primary" is true before a transition as well as after it, so a helper that breaks on the first
    sighting declares success while nothing has happened.
    """
    deadline = time.monotonic() + timeout
    last: list = []
    while time.monotonic() < deadline:
        last = [role_of(node.tcp_port) for node in mgr.nodes]  # may legitimately be a non-answer mid-transition
        primaries = [i for i, r in enumerate(last) if "PRIMARY" in r]
        if len(primaries) == 1:
            return primaries[0]
        time.sleep(0.25)
    raise AssertionError(f"no single PRIMARY within {timeout}s; roles were {last}")


# `ROLE` is answered from two atomics with no coordinator round trip, so a node always has one of
# these four to give however etcd is doing. That matters for the assertions below: `role_of()`
# returns `NO_ANSWER_YET`, `UNREACHABLE` or `CLOSED_WITHOUT_REPLY` when there was no reply, and
# `"PRIMARY" not in "NO_ANSWER_YET"` is true — so an assertion written only as "not PRIMARY" is
# satisfied by a node that stopped answering, which is the *worse* outcome. Pitfall 110's shape,
# and it was in the first draft of this file.
REAL_ROLES = ("PRIMARY", "REPLICA", "MULTI_MASTER", "STANDALONE")


def role_now(port: int) -> str:
    """The node's role, insisting on a real answer rather than a word meaning "no answer"."""
    answer = role_of(port)
    first = answer.split()[0] if answer.split() else ""
    assert first in REAL_ROLES, (
        f"the node on port {port} answered ROLE with {answer!r}, which is not a role — a node "
        f"that stopped answering is a worse outcome than the one this test watches for, and an "
        f"assertion written as 'not PRIMARY' would have passed on it")
    return answer


def serves_reads(port: int) -> bool:
    """Whether the node still answers a read. A refusal to *write* is not a refusal to serve."""
    reply = send_command(port, "SELECT * FROM 'NOSUCH'.'NOSUCH' LIMIT 1", timeout=10)
    # Either an OK with no rows or a named error is an answer; a timeout or a close is not, and
    # `send_command` raises for those.
    return bool(reply.strip())


@pytest.fixture
def coordinator_cluster():
    """A two-node cluster this test may break, torn down whatever the test did to it.

    **At the server's own default level, and that is the point rather than a detail.** This module
    ran at DEBUG for one commit, because the decision it exists to check - a replica declining to
    campaign because the read failed, #82's fix - was a DEBUG-only line while the default is INFO.
    That gap was the finding, filed as #115 and fixed: the refusal is INFO on the tick the episode
    opens. So these tests now assert on what an operator actually gets, which is a stronger
    statement than asserting on what the engine could be made to say.

    The first version of this fixture called `mgr.stop()`, which does not exist - the method is
    `shutdown()` - so all three tests errored in teardown. Nothing leaked, because `start()`
    registers `shutdown` with `atexit`, which is the safety net earning its keep.
    """
    mgr = ClusterManager()
    mgr.start()
    try:
        yield mgr
    finally:
        mgr.shutdown()


def failover_source_as_the_logger_sees_it() -> str:
    """`src/failover.cpp` with adjacent string literals joined, the way the compiler joins them.

    A phrase that a node writes on one line of its log is written in the source across two or three
    C++ literals, so searching the raw file for it finds nothing: measured here, `"staying REPLICA
    rather than campaigning"` exists in every log line that says it and in **no** contiguous run of
    the source, because the literal breaks after `REPLICA `. Implicit concatenation hiding a phrase
    from a grep is a trap this workspace has paid for in three repositories; this is the first time
    it has been on the *searching* side.

    `STEP_DOWN_REASONS` passed the same check without this, because those phrases happen to fit
    inside one literal each — which is precisely why the list that failed is the useful one.

    The limit, named rather than assumed: joining `" "` boundaries is not a C++ preprocessor. It
    would mis-join a `"` inside a comment written to look like two literals, and it does not expand
    `%s`. Both are fine for asking "can the engine say this sentence"; neither would be fine for
    deciding what the engine says.
    """
    source = (pathlib.Path(__file__).resolve().parents[2] / "src" / "failover.cpp").read_text()
    assert len(source) > 10_000, "failover.cpp was not read, so nothing below checked anything"
    return re.sub(r'"\s*"', "", source)


def test_every_step_down_reason_this_module_accepts_is_one_the_engine_can_say():
    """`STEP_DOWN_REASONS` is a list I wrote, so it is checked against the source.

    A list written by hand is not evidence about the code - #32 deleted three things built on one,
    and #112 found eleven thread entry points by hand where the source has seventeen. This checks
    the direction that actually drifts: a message gets reworded, the phrase stops matching, and the
    assertion above starts passing because it never matches anything.

    What it deliberately does **not** check is the other direction - a new step-down route added
    without a phrase here. That would need the set derived from the call sites of
    `handle_primary_lease_lost()` and `demote_to_replica()`, which is a bigger instrument than this
    test needs; the limit is written down rather than left to be assumed.
    """
    source = failover_source_as_the_logger_sees_it()
    missing = [reason for reason in STEP_DOWN_REASONS if reason not in source]
    assert not missing, (
        f"these phrases are no longer in src/failover.cpp, so any assertion matching them can "
        f"only pass by matching nothing: {missing}")


def test_a_node_that_cannot_reach_the_coordinator_does_not_declare_itself_primary(
        coordinator_cluster: ClusterManager):
    """Window one: the coordinator is absent before the node ever registers.

    This is the start-up shape of #82's conflation and it is next door to #73, where losing the
    CAS race left a node inert in STANDALONE for the rest of its life. A node that cannot read the
    leader key has learned nothing about who holds it, so taking the role would be taking it on
    silence — and the node it would be taking it from may be alive and serving.

    What it must still do is be a node: answer, and stay up.
    """
    mgr = coordinator_cluster
    # Empty the cluster first, so "before registration" means what it says: no key to read, no
    # peer to hear from, and no coordinator to ask.
    for index in range(len(mgr.nodes)):
        mgr.kill_node(index)
    mgr.stop_etcd()

    mgr.restart_node(0)
    node = mgr.nodes[0]
    offset = node_log_size(node)

    # It is up and answering. This is the half that #73 did not have.
    assert node.process.poll() is None, "the node exited because it could not reach etcd"
    assert serves_reads(node.tcp_port), "the node came up and does not answer reads"

    # And it does not claim the role. Watched for a whole TTL, because the branch that would take
    # the role on silence runs once per monitor tick.
    deadline = time.monotonic() + patience(LEASE_TTL_SECONDS + 5)
    seen = set()
    while time.monotonic() < deadline:
        seen.add(role_now(node.tcp_port))
        assert node.process.poll() is None, (
            f"the node exited while the coordinator was away: {mgr.unexplained_deaths()}")
        time.sleep(0.5)

    assert not any("PRIMARY" in r for r in seen), (
        f"a node promoted itself while it could not read the leader key; roles seen: {sorted(seen)}")

    # The control: with the coordinator back, it *can* take the role. Without this the assertion
    # above is satisfied by a node that is simply incapable of being primary.
    mgr.start_etcd()
    took = wait_for_role(node.tcp_port, "PRIMARY", timeout=patience(60))
    assert took >= 0
    assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()
    logged = node_log_since(node, offset)
    assert "etcd" in logged.lower() or "coordinator" in logged.lower(), (
        "the node said nothing about the coordinator through an outage and a recovery, so an "
        "operator reading this log could not tell that either happened")


def test_a_replica_does_not_campaign_because_a_read_failed(coordinator_cluster: ClusterManager):
    """Window two: a healthy cluster loses etcd underneath it.

    Two different promises, and both are checked because each is satisfiable while the other is
    broken. The replica **must not** campaign: it cannot read, and a read that failed is not a
    vacant key (#82). The primary **must** step down once it has been unable to confirm ownership
    for a whole TTL: it is holding a claim it cannot support, and by then its lease has had time to
    expire wherever etcd is.

    Neither node may exit. That is the third promise and it is the one #112 made keepable.
    """
    mgr = coordinator_cluster
    primary = one_primary(mgr, timeout=patience(60))
    replica = 1 - primary
    offsets = {i: node_log_size(node) for i, node in enumerate(mgr.nodes)}

    outage_began = time.monotonic()
    mgr.stop_etcd()

    # One pass, two observations, because they are about the same window and a second pass would
    # start after the first had already ended. The first version measured the holder's step-down in
    # a loop that ran *after* the replica watch, so it reported 0.00s - a duration that comes out
    # exactly zero is a measurement taken after the event, not a fast mechanism.
    gave_up_after = None
    deadline = outage_began + patience(LEASE_TTL_SECONDS * 2 + 5)
    while time.monotonic() < deadline:
        assert "PRIMARY" not in role_now(mgr.nodes[replica].tcp_port), (
            "the replica took the role while the coordinator was unreachable, which is #82: "
            "campaigning because the read failed rather than because there is no leader")
        if gave_up_after is None and "PRIMARY" not in role_now(mgr.nodes[primary].tcp_port):
            gave_up_after = time.monotonic() - outage_began
        assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()
        time.sleep(0.25)

    # The mechanism, not just the outcome: the replica says why it is not campaigning.
    replica_log = node_log_since(mgr.nodes[replica], offsets[replica])
    assert "rather than campaigning" in replica_log, (
        "the replica never logged the decision not to campaign, so this test cannot tell a node "
        "that declined from a node that never looked:\n" + replica_log[-2000:])

    # And the holder gave the role up. Asserted as the **property** with a bounded wait, then as
    # "the log names a reason", from the set of reasons this code can emit - because the first
    # version of this pinned one sentence, "could not confirm the leader key", and the engine
    # takes a different and better route. That branch is for a coordinator that answers keepalives
    # but not reads; an unreachable one fails the *keepalive*, and the holder demotes on the very
    # next tick rather than waiting out a TTL. Pitfall 212: quoting a message pins the wording when
    # the property is one clause of it.
    assert gave_up_after is not None, (
        "the holder still answers ROLE with PRIMARY after two lease TTLs without a coordinator, "
        "so it is holding a claim it has had no way to confirm")
    # Measured from the moment etcd was stopped, which is the number an operator plans around.
    custom_metrics["primary_step_down_without_coordinator_sec"] = round(gave_up_after, 2)
    assert gave_up_after > 0.0, (
        "the step-down was recorded as taking no time at all, which means this loop started after "
        "it had already happened rather than that the mechanism is instant")

    primary_log = node_log_since(mgr.nodes[primary], offsets[primary])
    assert any(reason in primary_log for reason in STEP_DOWN_REASONS), (
        "the holder gave the role up and its log names no reason from the set this code can "
        "emit, so an operator cannot tell which mechanism took it:\n" + primary_log[-2000:])

    # Reads survive the outage on both nodes: losing the coordinator costs writes, not service.
    for index in (primary, replica):
        assert serves_reads(mgr.nodes[index].tcp_port), (
            f"node index {index} stopped answering reads because etcd was away")

    # The control that makes every refusal above mean something.
    mgr.start_etcd()
    one_primary(mgr, timeout=patience(90))
    assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()


def test_nobody_is_promoted_into_an_election_nobody_can_observe(
        coordinator_cluster: ClusterManager):
    """Window three: the leader is gone *and* nobody can read that it is gone.

    The order of the two faults is the whole test. etcd goes first: with the coordinator stopped
    afterwards, the survivor would have had a chance to read the still-present key and start its
    election wait against real information. Stopping it first means the survivor's every read fails
    from the moment the leader dies, which is the window this is about — and it is the window in
    which #82's old code would have promoted, because an unreadable coordinator and a vacant key
    were the same answer.
    """
    mgr = coordinator_cluster
    primary = one_primary(mgr, timeout=patience(60))
    survivor = 1 - primary
    offset = node_log_size(mgr.nodes[survivor])

    mgr.stop_etcd()
    mgr.kill_node(primary)

    deadline = time.monotonic() + patience(LEASE_TTL_SECONDS * 2 + 5)
    while time.monotonic() < deadline:
        assert "PRIMARY" not in role_now(mgr.nodes[survivor].tcp_port), (
            "the survivor promoted itself into an election it could not observe")
        assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()
        time.sleep(0.5)

    survivor_log = node_log_since(mgr.nodes[survivor], offset)
    assert "rather than campaigning" in survivor_log, (
        "the survivor never recorded the decision, so this test proves only that nothing "
        "happened:\n" + survivor_log[-2000:])

    # The control, and here it is load-bearing twice: it shows the survivor *can* take a vacated
    # role, so the refusal above was a decision rather than an incapacity.
    mgr.start_etcd()
    wait_for_role(mgr.nodes[survivor].tcp_port, "PRIMARY", timeout=patience(90))
    assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()


# The sentences this test counts, each the opening line of an episode rather than a per-tick one.
# Taken from `src/failover.cpp` and checked against it below, for the same reason
# `STEP_DOWN_REASONS` is.
EPISODE_OPENINGS = (
    "cannot publish this node's WAL position",              # #116, the publish
    "the coordinator will not grant a lease",               # #116, the lease
    "this is a decision, not a stall",                      # #115, the decision — see below
)
# That third phrase is a clause of the INFO line and appears in **nothing else**, which is the
# point. The obvious choice, "staying REPLICA rather than campaigning", is in the INFO line *and*
# in the per-tick DEBUG line beside it — so a mutation rewording the INFO line survived the source
# check, satisfied by the DEBUG line. #115 is about the **level**, so a phrase both levels share
# cannot test it: a check a cross-reference satisfies is worse than no check, because the next
# reader trusts it. Found by giving each mutation the verdict it was supposed to produce and
# noticing one disagree.
RECOVERY_LINES = (
    "publishing this node's WAL position works again",
    "granted a position lease again",
    "the coordinator answers again",
)


def test_the_episode_phrases_this_module_counts_are_ones_the_engine_can_say():
    """Same reason as the step-down list: a phrase I wrote is not evidence about the code.

    The direction that drifts is a reworded message, after which every "appears at most once"
    assertion below passes by matching nothing at all — which is the failure mode those assertions
    exist to prevent, arriving through the test rather than through the engine.
    """
    source = failover_source_as_the_logger_sees_it()
    missing = [p for p in EPISODE_OPENINGS + RECOVERY_LINES if p not in source]
    assert not missing, f"these phrases are no longer in src/failover.cpp: {missing}"


def test_a_coordinator_outage_is_one_episode_in_the_log_not_one_line_a_second(
        coordinator_cluster: ClusterManager):
    """#115 and #116: what the default log level carries through an outage, and how much of it.

    **Measured before the fix, over 30 s with the coordinator stopped:** 65 and 67 lines on the two
    nodes, of which **60 each** were two sentences repeated once a second, and **0** mentioned the
    replica's decision not to campaign. So the log was ~92% two repeated sentences and was missing
    the one fact that answers "is the engine deciding or stuck?".

    Both halves are checked here, and the window is long enough that the difference cannot be
    luck: a per-tick line would appear about twenty times.
    """
    mgr = coordinator_cluster
    one_primary(mgr, timeout=patience(60))
    offsets = {i: node_log_size(node) for i, node in enumerate(mgr.nodes)}

    outage = patience(20.0)
    mgr.stop_etcd()
    settle = time.monotonic() + outage
    while time.monotonic() < settle:
        assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()
        time.sleep(0.5)

    for index, node in enumerate(mgr.nodes):
        logged = node_log_since(node, offsets[index])
        lines = [line for line in logged.splitlines() if line.strip()]
        custom_metrics[f"outage_log_lines_node_{index}"] = len(lines)
        custom_metrics[f"outage_log_lines_per_sec_node_{index}"] = round(len(lines) / outage, 2)

        for phrase in EPISODE_OPENINGS[:2]:
            seen = logged.count(phrase)
            assert seen <= 1, (
                f"node index {index} logged {seen} copies of {phrase!r} in {outage:.0f}s. One per "
                f"episode is the whole of #116; once per monitor tick is what it replaced")

        # #115: the decision is present at the **default** level, and present once.
        decision = logged.count(EPISODE_OPENINGS[2])
        assert decision == 1, (
            f"node index {index} logged the decision not to campaign {decision} times at the level "
            f"an operator reads. Zero is #115, the absence this assertion exists to keep closed; "
            f"more than one is #116 arriving in a new place. The phrase counted here belongs to "
            f"the INFO line alone, so this number is about the level and not only the sentence")

    # The control, and it is what makes "at most once" mean something: the episodes end, and say so.
    mgr.start_etcd()
    one_primary(mgr, timeout=patience(90))
    recovered = "".join(node_log_since(node, offsets[i]) for i, node in enumerate(mgr.nodes))
    assert any(line in recovered for line in RECOVERY_LINES), (
        "the coordinator came back and no node said so, which means an operator watching a "
        "recovery sees the noise stop and gets no confirmation that it stopped for the right "
        "reason")
    assert not mgr.unexplained_deaths(), mgr.unexplained_deaths()
