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


def _node(process) -> NodeInfo:
    # `tail_node_log` reads a path that does not exist here, which is fine: it is written to answer
    # "no log" rather than to raise.
    return NodeInfo(index=0, process=process, tcp_port=1, replication_port=2, metrics_port=3,
                    data_dir="/nonexistent", node_id="node-x")


def test_a_sigkill_this_harness_sent_is_named_as_such():
    cm = ClusterManager.__new__(ClusterManager)
    cm._sigkilled_by_harness = {}
    proc = _DeadProcess(-int(signal.SIGKILL))
    cm._sigkilled_by_harness[id(proc)] = time.time()

    explanation = cm._explain_death(_node(proc), -int(signal.SIGKILL))
    assert "This harness escalated SIGTERM to SIGKILL" in explanation
    assert "#106" in explanation, (
        "the explanation does not name the defect that made the escalation reachable, which is the "
        "one thing a reader needs next")


def test_a_sigkill_from_outside_prints_what_the_machine_had_left():
    cm = ClusterManager.__new__(ClusterManager)
    cm._sigkilled_by_harness = {}
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
    cm = ClusterManager.__new__(ClusterManager)
    cm._sigkilled_by_harness = {}
    explanation = cm._explain_death(_node(_DeadProcess(66)), 66)
    assert "ThreadSanitizer" in explanation and "not the engine" in explanation


def test_an_ordinary_exit_code_gets_no_invented_explanation():
    # The control: the report must not grow a theory for every status. A node that exited 1 has its
    # own log, and that is where the answer is.
    cm = ClusterManager.__new__(ClusterManager)
    cm._sigkilled_by_harness = {}
    assert cm._explain_death(_node(_DeadProcess(1)), 1) == ""
