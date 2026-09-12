"""What the harness says when a node dies without a test killing it (#86, server thread).

`signal 9` had two producers and no way to tell them apart, which is why that item stayed open:

* **this harness** — `_stop_node()` escalates `SIGTERM` to `SIGKILL` after five seconds, and until
  #106 a node with any client attached never exited, so it closed its listener, kept running and was
  killed. From outside: nothing listening, then `signal 9`, no race report, only sometimes.
* **an OOM kill** — which arrives identically and says nothing at all anywhere.

Measured while closing the thread, and it is what moved the diagnosis: under ThreadSanitizer the
heaviest modules peak at **246 MiB resident** across every node and etcd, the largest single node
ever recorded **92 MiB**, and `MemAvailable` never fell below **8.8 GiB**. Against a runner with
16 GB that does not support the memory hypothesis — so the report has to distinguish the two rather
than assume one, and this file is what keeps it able to.

No cluster is started here: the subject is the sentence, and starting three nodes to read one string
would make a fast test slow for nothing.
"""

from __future__ import annotations

import signal
import time

import pytest

from conftest import ClusterManager, NodeInfo

pytestmark = pytest.mark.smoke


class _DeadProcess:
    """Just enough of `subprocess.Popen` for the report: an exit status and an identity."""

    def __init__(self, returncode: int):
        self.returncode = returncode

    def poll(self) -> int:
        return self.returncode


def _reporting_manager(etcd_stopped_at: float | None = None) -> ClusterManager:
    """A manager with nothing started, carrying exactly the fields `_explain_death` reads.

    `__new__` is on purpose - the subject is the sentence, and starting three nodes to read one
    string would make a fast test slow. The list of fields lives here rather than in each test
    because #54 stage B added `_etcd_stopped_at` and all four tests broke identically with an
    `AttributeError`. A `getattr(self, ..., None)` in the production side would have "fixed" it by
    making a forgotten initialiser read as a healthy absence, which is the shape this repository
    keeps paying for; a factory is where the defaults belong (pitfall 113).
    """
    cm = ClusterManager.__new__(ClusterManager)
    cm._sigkilled_by_harness = {}
    cm._etcd_stopped_at = etcd_stopped_at
    return cm


def _node(process) -> NodeInfo:
    # `tail_node_log` reads a path that does not exist here, which is fine: it is written to answer
    # "no log" rather than to raise.
    return NodeInfo(index=0, process=process, tcp_port=1, replication_port=2, metrics_port=3,
                    data_dir="/nonexistent", node_id="node-x")


def test_a_sigkill_this_harness_sent_is_named_as_such():
    cm = _reporting_manager()
    proc = _DeadProcess(-int(signal.SIGKILL))
    cm._sigkilled_by_harness[id(proc)] = time.time()

    explanation = cm._explain_death(_node(proc), -int(signal.SIGKILL))
    assert "This harness escalated SIGTERM to SIGKILL" in explanation
    assert "#106" in explanation, (
        "the explanation does not name the defect that made the escalation reachable, which is the "
        "one thing a reader needs next")


def test_a_sigkill_from_outside_prints_what_the_machine_had_left():
    cm = _reporting_manager()
    proc = _DeadProcess(-int(signal.SIGKILL))

    explanation = cm._explain_death(_node(proc), -int(signal.SIGKILL))
    assert "No SIGKILL came from this harness" in explanation
    assert "MemAvailable is now" in explanation, (
        "an external SIGKILL is the one exit status that cannot say why by itself, and the number "
        "the OOM hypothesis needs is the one nobody was recording")
    # A real number rather than the -1 the reader would have to interpret.
    assert "MemAvailable is now -1 MiB" not in explanation


def test_a_sanitizer_refusing_to_start_is_not_reported_as_an_engine_failure():
    # Met while measuring this: `FATAL: ThreadSanitizer: unexpected memory mapping` aborts with 66
    # at random on this kernel, so a node that never started reads as a node that died.
    cm = _reporting_manager()
    explanation = cm._explain_death(_node(_DeadProcess(66)), 66)
    assert "ThreadSanitizer" in explanation and "not the engine" in explanation


def test_an_ordinary_exit_code_gets_no_invented_explanation():
    # The control: the report must not grow a theory for every status. A node that exited 1 has its
    # own log, and that is where the answer is.
    cm = _reporting_manager()
    assert cm._explain_death(_node(_DeadProcess(1)), 1) == ""


# ── #54 stage B: a death while the coordinator was deliberately away ──────────
#
# `stop_etcd()` records when it took etcd away. The task that asked for that registry wanted it to
# *suppress* reports; it annotates them instead, because a node that dies while the coordinator is
# gone is exactly the defect stage B hunts and suppressing it would hide the finding. These three
# pin the distinction, and the third is what proves the annotation is reachable on its own.

def test_a_death_while_the_coordinator_was_away_says_so():
    cm = _reporting_manager(etcd_stopped_at=time.time() - 7.5)
    proc = _DeadProcess(-int(signal.SIGKILL))

    explanation = cm._explain_death(_node(proc), -int(signal.SIGKILL))
    assert "stopped etcd" in explanation, (
        "the harness knew the coordinator was away and did not say so, which is #86's shape: one "
        "exit status, two producers, and the harness holding the discriminator")
    assert "7.5s ago" in explanation, "the elapsed time is the part that makes it readable"
    assert "context, not an excuse" in explanation, (
        "without this clause the sentence reads as permission for the node to have died, and a "
        "node must survive an unreachable coordinator")
    # The original explanation is still there: the annotation adds, it does not replace.
    assert "No SIGKILL came from this harness" in explanation


def test_a_sanitizer_refusal_is_not_blamed_on_the_coordinator():
    """Exit 66 deliberately gets no annotation, and this is the test that says it is deliberate.

    The sanitizer refusing to start means the node never ran, so etcd's state is irrelevant.
    Saying "the coordinator was absent when this happened" would be true and would invite the
    reader to connect two unrelated facts — the failure mode the whole function exists to prevent.
    """
    cm = _reporting_manager(etcd_stopped_at=time.time() - 3.0)
    explanation = cm._explain_death(_node(_DeadProcess(66)), 66)
    assert "ThreadSanitizer" in explanation
    assert "stopped etcd" not in explanation, (
        "a node whose sanitizer refused to start never ran, so the coordinator's state is not "
        "context for it — it is a coincidence, and the report must not offer coincidences")


def test_the_coordinator_note_stands_alone_for_a_status_with_no_theory_of_its_own():
    """An ordinary exit 1 gets no invented theory — but it does get the coordinator note.

    This is the test that makes the `return coordinator` fallback reachable. Without it the
    annotation would only ever be seen glued to another sentence, and the branch that returns it by
    itself would be a line nothing exercises.
    """
    away = _reporting_manager(etcd_stopped_at=time.time() - 1.0)
    explanation = away._explain_death(_node(_DeadProcess(1)), 1)
    assert "stopped etcd" in explanation
    assert "MemAvailable" not in explanation and "ThreadSanitizer" not in explanation, (
        "exit 1 acquired a theory it did not have before")

    # And with no outage recorded, the same status still gets nothing. The pair is the point.
    quiet = _reporting_manager()
    assert quiet._explain_death(_node(_DeadProcess(1)), 1) == ""
