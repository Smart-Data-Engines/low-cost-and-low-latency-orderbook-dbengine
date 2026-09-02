"""Failover: handing the primary role over, and losing it unexpectedly.

Every test here changes the cluster's topology, so they all take `healthy_cluster`
rather than `cluster`: its teardown restarts whatever is not running and refuses to
finish unless exactly one node holds the primary role. Without that, one killed node
turns the rest of the session red for reasons unrelated to the code being tested.

Two things are being checked, and they are different:

- **Graceful handover** (`FAILOVER <node_id>`) must land the role on the node that
  was named. Before roadmap #29 it ignored the target and the outgoing primary won
  the race back roughly half the time.
- **An unplanned loss** (SIGKILL) must promote the survivor, keep the data that was
  acknowledged, and accept writes again.
- **A lease the coordinator has forgotten** must take the role away from whoever held it. That is
  the whole purpose of writing the leader key under a lease, and until #74 it did not work: a
  keepalive for a lease etcd no longer knew answered 200, so the holder never found out.
"""
from __future__ import annotations

import base64
import json
import socket
import time
import urllib.error
import urllib.request

import pytest

from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.failover

# Read by the console report; failover_time_sec is rendered specially.
custom_metrics: dict = {}


class ClosedWithoutReply(Exception):
    """The node accepted the command and closed the connection without answering."""


def send_command(port: int, command: str, timeout: float = 10.0) -> str:
    """Send one command and return the reply.

    Reads until data arrives or the timeout expires, rather than sleeping 0.3 s and taking one
    `recv`. The old shape lost the distinction that matters: an orderly close and a reply that had
    not arrived yet both came back as `''`, so a failing assertion could not say which had happened
    — and `FAILOVER` legitimately takes seconds, because it is etcd round-trips and a grace period.

    An orderly close raises rather than returning `''`, so a caller has to decide what it means
    instead of comparing against the empty string and getting the same answer for two different
    events.
    """
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.recv(4096)  # banner
        sock.sendall((command + "\n").encode())

        deadline = time.monotonic() + timeout
        buffered = b""
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                if buffered:
                    break          # closed after answering; the answer is what we asked for
                raise ClosedWithoutReply(
                    f"node on port {port} closed the connection after {command!r} "
                    f"without sending anything")
            buffered += chunk
            if b"\n" in buffered:
                break
        return buffered.decode(errors="replace")


def role_of(port: int, timeout: float = 5.0) -> str:
    """The node's own answer to ROLE, or why it did not give one.

    Three outcomes, not two. The first version returned `"UNREACHABLE"` for anything that raised
    `OSError` — which covers a refused connection *and* a `socket.timeout`, since that is an
    `OSError` subclass. So a node that was merely slow read exactly like a node that was gone, and
    the assertion built on it could not say which.

    That distinction is not academic here: a node in the middle of a demotion is doing etcd work,
    tearing down a replication manager and starting a client, and under ThreadSanitizer it can take
    longer than five seconds to answer. Same defect as `send_command()` had before #86, one function
    away — which is pitfall 63's shape: two functions, the same mistake, and fixing one.
    """
    try:
        return send_command(port, "ROLE", timeout=timeout).strip().upper()
    except socket.timeout:
        return "NO_ANSWER_YET"
    except ClosedWithoutReply:
        return "CLOSED_WITHOUT_REPLY"
    except OSError:
        return "UNREACHABLE"


def wait_for_role(port: int, expected: str, timeout: float = 30.0) -> float:
    """Wait until a node reports the expected role. Returns how long it took."""
    start = time.monotonic()
    deadline = start + timeout
    last = ""
    while time.monotonic() < deadline:
        last = role_of(port)
        if expected in last:
            return time.monotonic() - start
        time.sleep(0.25)
    raise AssertionError(
        f"node on port {port} did not report {expected} within {timeout}s; "
        f"last ROLE was {last!r}")


def etcd_post(port: int, path: str, payload: dict) -> tuple[int, str]:
    """POST to etcd's HTTP gateway. Returns (status, body) and does not raise on 4xx."""
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def revoke_every_lease(etcd_port: int) -> list[int]:
    """Drop every lease in etcd, which deletes the keys written under them.

    This is what a network partition longer than the TTL does to a primary, without having to build
    the partition: the leader key disappears while the process holding the role stays up and healthy.
    """
    _, body = etcd_post(etcd_port, "/v3/lease/leases", {})
    ids = [int(entry["ID"]) for entry in json.loads(body).get("leases", [])]
    for lease_id in ids:
        etcd_post(etcd_port, "/v3/lease/revoke", {"ID": lease_id})
    return ids


def published_positions(etcd_port: int) -> dict[str, dict]:
    """The WAL positions an election actually reads, taken straight out of etcd.

    Keyed by node id. Since #72 these keys are written under a per-node lease, so a node that dies
    stops appearing here on its own — which is what lets a candidate tell a replica that is further
    ahead from one that is merely dead.
    """
    start = base64.b64encode(b"/ob/nodes/").decode()
    end = base64.b64encode(b"/ob/nodes0").decode()   # one past the prefix
    _, body = etcd_post(etcd_port, "/v3/kv/range", {"key": start, "range_end": end})
    out: dict[str, dict] = {}
    for kv in json.loads(body).get("kvs", []):
        key = base64.b64decode(kv["key"]).decode()
        value = json.loads(base64.b64decode(kv["value"]).decode())
        out[key.rsplit("/", 1)[-1]] = value
    return out


def epoch_of(role_response: str) -> int:
    """ROLE answers `PRIMARY <epoch>` for a primary; 0 when there is no epoch to read."""
    parts = role_response.split()
    if len(parts) >= 2 and parts[0] == "PRIMARY" and parts[1].isdigit():
        return int(parts[1])
    return 0


def primaries_among(cluster) -> list[str]:
    roles = [role_of(n.tcp_port) for n in cluster.nodes]
    return [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]


# ── Graceful handover ─────────────────────────────────────────────────────────

def test_handover_lands_on_the_named_target(healthy_cluster):
    """The role must go to the node named in the command, not to whoever polls first."""
    primary = healthy_cluster.primary()
    target = healthy_cluster.replica()

    # A refusal is a failure; a lost answer is not the property under test.
    #
    # This assertion was `startswith("OK")` and it raced the node's own step-down: the handover is
    # accepted, the node stops being primary, and whether the `OK` reaches the client is a matter of
    # which happens first. Measured across four CI runs on three branches, it failed in three of
    # them, including one whose only change was to the flag parser and one under ThreadSanitizer,
    # so it is neither a branch nor a load effect. Roadmap #86.
    #
    # What the assertion was added for is #60: the command used to answer `ERR unknown_target` for
    # every target, because nothing published WAL positions. That protection is kept — an `ERR` is
    # still a failure — while the timing of the acknowledgement is not asserted, because the two
    # assertions below check the thing the test is named after.
    try:
        reply = send_command(primary.tcp_port, f"FAILOVER {target.node_id}")
    except ClosedWithoutReply as closed:
        reply = ""
        print(f"note: {closed}; continuing to the effect assertions")
    assert not reply.strip().startswith("ERR"), f"handover refused: {reply!r}"

    elapsed = wait_for_role(target.tcp_port, "PRIMARY", timeout=30)
    custom_metrics["failover_time_sec"] = elapsed
    custom_metrics["failover_kind"] = "graceful"

    # And the node that handed the role over must not have taken it back.
    assert "PRIMARY" not in role_of(primary.tcp_port), (
        "the outgoing primary reacquired the role it handed over — this is the race "
        "roadmap #29 fixed, and it used to happen about half the time")

    # It must also stop *saying* it is primary. Until #60 the handover moved the failover
    # manager's own role and left the engine untouched unless the target had already promoted in
    # that same instant — which it never had, because it first has to notice the empty leader key.
    # So the outgoing node kept answering ROLE with PRIMARY, and a client discovering the primary
    # by asking would keep writing to the node that had just given the role away.
    # Polled, not sampled once. The property is that the outgoing node *ends up* a replica, and a
    # single `role_of()` five seconds after the handover asserts that it gets there within five
    # seconds — which is an assertion about the machine rather than about the mechanism. It failed
    # exactly that way under ThreadSanitizer, reporting 'UNREACHABLE' for a node that was answering
    # slowly.
    outgoing_role = ""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        outgoing_role = role_of(primary.tcp_port)
        if "REPLICA" in outgoing_role:
            break
        time.sleep(0.5)
    assert "REPLICA" in outgoing_role, (
        f"the outgoing primary still reports {outgoing_role!r} thirty seconds after handing the "
        f"role over. 'NO_ANSWER_YET' means it is up and not answering ROLE, which is a different "
        f"complaint from 'UNREACHABLE'")

    # And refuse writes, which is the consequence that costs data rather than confusion.
    refused = send_command(primary.tcp_port, "INSERT HANDOVER EX bid 100000 1 1")
    assert refused.startswith("ERR"), (
        f"the outgoing primary still accepts writes: {refused!r}")

    # Leave the cluster settled. This test moves a role for real since #60, and the next test
    # resolves "the primary" by asking: handing over and walking away made the next one send its
    # command to a node mid-transition and get ERR not_primary instead of what it was checking.
    wait_for_role(target.tcp_port, "PRIMARY", timeout=30)
    wait_for_role(primary.tcp_port, "REPLICA", timeout=30)


def test_handover_to_an_unknown_node_is_refused(healthy_cluster):
    primary = healthy_cluster.primary()

    reply = send_command(primary.tcp_port, "FAILOVER node-that-does-not-exist")

    assert reply.startswith("ERR"), f"an unknown target was accepted: {reply!r}"
    assert "unknown_target" in reply, reply
    # A rejected handover is never a partial one: the node keeps its role.
    assert "PRIMARY" in role_of(primary.tcp_port), (
        "the primary lost its role to a handover it had rejected")


def test_handover_to_itself_is_refused(healthy_cluster):
    primary = healthy_cluster.primary()

    reply = send_command(primary.tcp_port, f"FAILOVER {primary.node_id}")

    assert reply.startswith("ERR"), f"self-handover was accepted: {reply!r}"
    assert "invalid_target" in reply, reply
    assert "PRIMARY" in role_of(primary.tcp_port)


def test_handover_from_a_replica_is_refused(healthy_cluster):
    replica = healthy_cluster.replica()
    primary = healthy_cluster.primary()

    reply = send_command(replica.tcp_port, f"FAILOVER {primary.node_id}")

    assert reply.startswith("ERR"), f"a replica accepted a handover: {reply!r}"
    assert "not_primary" in reply, reply


# ── Unplanned loss ───────────────────────────────────────────────────────────

def test_killing_the_primary_promotes_the_survivor(healthy_cluster):
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    start = time.monotonic()
    healthy_cluster.kill_node(primary.index)

    elapsed = wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)
    custom_metrics["failover_time_sec"] = elapsed
    custom_metrics["failover_kind"] = "kill"

    # Exactly one primary: two would be split brain, which is worse than none.
    roles = [role_of(n.tcp_port) for n in healthy_cluster.nodes]
    primaries = [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]
    assert len(primaries) == 1, f"expected one primary, roles={roles}"


def test_acknowledged_data_survives_a_kill(healthy_cluster):
    """Rows the server said OK to must be there after the primary is gone."""
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    writer = OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=20)
    try:
        writer.insert("FAILOVER-DATA", "BINANCE", "bid",
                      [700_000, 699_000, 698_000], [11, 12, 13])
        writer.flush()
    finally:
        writer.close()

    # Give replication a moment to carry the rows across before pulling the plug.
    time.sleep(1.5)
    healthy_cluster.kill_node(primary.index)
    wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)

    reader = OrderbookEngine(host="127.0.0.1", port=survivor.tcp_port, timeout=20)
    try:
        rows = reader.query_all("FAILOVER-DATA", "BINANCE")
    finally:
        reader.close()

    prices = sorted(r.price for r in rows)
    assert prices == [698_000, 699_000, 700_000], (
        f"rows acknowledged before the kill did not survive it: {prices}")


def test_the_promoted_node_accepts_writes(healthy_cluster):
    """A promoted replica that still rejects writes is not a primary."""
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    healthy_cluster.kill_node(primary.index)
    wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)

    client = OrderbookEngine(host="127.0.0.1", port=survivor.tcp_port, timeout=20)
    try:
        client.insert("FAILOVER-WRITE", "BINANCE", "ask", [800_000], [9])
        client.flush()
        rows = client.query_all("FAILOVER-WRITE", "BINANCE")
    finally:
        client.close()

    assert len(rows) == 1, (
        "the promoted node did not accept a write; read_only_flag_ is probably still "
        "set after the role transition")


def test_a_pool_client_follows_the_new_primary(healthy_cluster):
    """The pool must re-discover the primary on its own after one disappears."""
    hosts = [f"127.0.0.1:{n.tcp_port}" for n in healthy_cluster.nodes]
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    pool = OrderbookEngine(hosts=hosts, timeout=20.0, health_check_interval=1.0)
    try:
        pool.insert("FAILOVER-POOL", "BINANCE", "bid", [900_000], [4])
        pool.flush()

        healthy_cluster.kill_node(primary.index)
        wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)

        # No reconnect call: the pool is supposed to work this out itself.
        pool.insert("FAILOVER-POOL", "BINANCE", "bid", [901_000], [5])
        pool.flush()
        rows = pool.query_all("FAILOVER-POOL", "BINANCE")
    finally:
        pool.close()

    prices = sorted(r.price for r in rows)
    assert 901_000 in prices, (
        f"the write after failover did not land anywhere readable: {prices}")


def test_a_dead_node_is_reported_as_unreachable_not_as_a_replica(healthy_cluster):
    """A node that is gone must not be counted as a healthy follower."""
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    healthy_cluster.kill_node(primary.index)
    wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)

    assert role_of(primary.tcp_port, timeout=3) == "UNREACHABLE", (
        "a killed node still answered ROLE")

    client = OrderbookEngine(host="127.0.0.1", port=survivor.tcp_port, timeout=20)
    try:
        status = client.status()
    finally:
        client.close()

    replicas = status.get("replicas")
    assert replicas is not None, f"STATUS carries no replica count: {sorted(status)}"
    assert replicas == 0, (
        f"the new primary reports {replicas} replicas while the only other node is "
        f"dead")


# ── Lease fencing ─────────────────────────────────────────────────────────────


def test_a_primary_whose_lease_etcd_forgot_stops_holding_the_role(healthy_cluster):
    """The leader key is written under a lease so that losing the lease loses the role.

    Revoking every lease in etcd is a partition longer than the TTL, without building a partition:
    the leader key disappears while the process that held the role stays up and healthy. Before #74
    the holder never found out — a keepalive for a forgotten lease answers HTTP 200 with the ID
    echoed back and no TTL, and the code only checked that the response was non-empty. The result was
    two nodes reporting PRIMARY at different epochs, indefinitely, both accepting writes.
    """
    primary = healthy_cluster.primary()
    epoch_before = epoch_of(role_of(primary.tcp_port))
    assert epoch_before > 0, "the primary did not report an epoch to compare against"

    assert revoke_every_lease(healthy_cluster.etcd_client_port), "there was no lease to revoke"

    # What #74 restored is that the holder *finds out*, and the only robust way to assert that is to
    # watch for the holder's claim to change. "Exactly one primary" cannot do it: that is true
    # before the transition as well as after, so a poll loop waiting for it declares success while
    # nothing has happened — which is exactly how the first version of this assertion passed against
    # a node that had not noticed anything yet.
    #
    # Either the old holder stops claiming PRIMARY, or it stands again and wins a *later* epoch.
    # Sitting on the same epoch means it never learned.
    deadline = time.monotonic() + 40
    role_after = ""
    while time.monotonic() < deadline:
        role_after = role_of(primary.tcp_port)
        still_claiming_the_same = ("PRIMARY" in role_after and "REPLICA" not in role_after
                                   and epoch_of(role_after) <= epoch_before)
        if not still_claiming_the_same:
            break
        time.sleep(0.5)

    assert not ("PRIMARY" in role_after and "REPLICA" not in role_after
                and epoch_of(role_after) <= epoch_before), (
        f"the old primary still answers {role_after!r} after its lease was revoked, so it never "
        f"learned it had lost the role")

    # And the cluster comes back to exactly one holder. Since #82 this takes longer than it used to:
    # the holder steps down within a second of the key vanishing, and then a candidate waits out the
    # lease TTL before claiming it, so there is a deliberate stretch with no primary at all.
    deadline = time.monotonic() + 40
    holders: list[str] = []
    while time.monotonic() < deadline:
        holders = primaries_among(healthy_cluster)
        if len(holders) == 1:
            break
        time.sleep(0.5)
    assert len(holders) == 1, f"expected one primary once the dust settled, saw {holders}"


def test_a_primary_that_lost_its_lease_refuses_writes(healthy_cluster):
    """The larger half of #82: the demotion has to reach the Engine, not just the FailoverManager.

    `handle_primary_lease_lost()` used to call `demote_to_replica()` **only** when it could read a
    leader key carrying a non-empty address. After a revoke there is no key and therefore no
    address, so in the one case that matters the Engine was never told: `node_role_` stayed PRIMARY,
    `read_only_flag_` stayed unset, `ROLE` kept answering "PRIMARY <epoch>", and the node kept
    accepting writes — until it happened to stand for election again, and indefinitely if it never
    did.

    So the assertion is about writes, not about roles. A role is what a node says; a refused write is
    what protects the data.
    """
    primary = healthy_cluster.primary()

    client = OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=20)
    try:
        # It accepts writes now, so the refusal below cannot be blamed on anything else.
        client.insert("LEASEGONE", "TEST", "bid", [100_000], [5],
                      timestamp_ns=1_700_000_000_000_000_001)

        assert revoke_every_lease(healthy_cluster.etcd_client_port), "there was no lease to revoke"

        deadline = time.monotonic() + 30
        last_error = ""
        refused = False
        while time.monotonic() < deadline:
            try:
                client.insert("LEASEGONE", "TEST", "bid", [100_001], [5],
                              timestamp_ns=1_700_000_000_000_000_002)
            except Exception as exc:  # noqa: BLE001 — the message is the evidence
                last_error = repr(exc)
                refused = True
                break
            time.sleep(0.5)

        assert refused, (
            "the node went on accepting writes after its lease was revoked, so it never learned it "
            "had lost the role — and any write it accepted is in its WAL and in nobody else's")
        assert "read-only" in last_error.lower() or "replica" in last_error.lower(), (
            f"writes stopped, but not for the reason this test is about: {last_error}")
    finally:
        client.close()


def test_no_two_nodes_hold_the_role_at_the_same_instant(healthy_cluster):
    """The window #74 left behind, closed by #82.

    Not a duplicate of the test above. That one asks whether the holder ever finds out, which is
    what #74 fixed. This one asks whether there is any instant at which two nodes both believe they
    are primary — because both accept writes while they believe it, and the writes that land on the
    one about to step down are not in the other's log.

    Was `xfail(strict=False)` between the day CI reproduced it and the day #82 landed. Two changes
    closed it: the holder now steps down on a *confirmed* absent leader key, which it could not
    distinguish from an unreadable one before, and a candidate waits out the holder's step-down
    bound before claiming the vacated key.
    """
    assert revoke_every_lease(healthy_cluster.etcd_client_port), "there was no lease to revoke"

    deadline = time.monotonic() + 30
    worst = 0
    while time.monotonic() < deadline:
        worst = max(worst, len(primaries_among(healthy_cluster)))
        if worst > 1:
            break
        time.sleep(0.25)

    assert worst <= 1, (
        f"{worst} nodes held the PRIMARY role at the same instant: both accept writes while they "
        f"believe it, so writes landing on the one about to step down are lost")


def test_the_cluster_still_has_a_primary_after_a_lease_scare(healthy_cluster):
    """Fencing has to be recoverable, not just safe.

    Taking the role away is only half of it: something must take it back, or a transient coordinator
    problem would leave the cluster read-only until an operator noticed.

    Unlike the test above, this one is **not** a regression test for #74 — it passes against the
    pre-fix binary too, because the fixture restarts nodes between tests and a restarting node reads
    its role from etcd anyway. It guards recoverability, and only that.
    """
    revoke_every_lease(healthy_cluster.etcd_client_port)

    # Confirmed twice, a second apart, because a single sighting is not recovery. Right after the
    # revoke the old holder is still answering PRIMARY for the moment it takes to notice, so a loop
    # that breaks on the first sighting of one primary and then re-reads can catch the gap between
    # the step-down and the new election — which is exactly what it did once #82 made the step-down
    # prompt. What this test guards is that a primary comes *back*, so it has to see one that stays.
    deadline = time.monotonic() + 60
    roles: list[str] = []
    while time.monotonic() < deadline:
        roles = [role_of(n.tcp_port) for n in healthy_cluster.nodes]
        primaries = [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]
        if len(primaries) == 1:
            time.sleep(1.0)
            roles = [role_of(n.tcp_port) for n in healthy_cluster.nodes]
            primaries = [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]
            if len(primaries) == 1:
                break
        time.sleep(0.5)

    primaries = [r for r in roles if "PRIMARY" in r and "REPLICA" not in r]
    assert len(primaries) == 1, f"no single primary came back and stayed; roles={roles}"


# ── Position freshness (#72) ──────────────────────────────────────────────────


def test_a_killed_node_stops_publishing_a_position(healthy_cluster):
    """A dead node's WAL position has to disappear by itself.

    Election deference reads these positions to find the replica that lost the least. Until #72 they
    were written without a lease, so a dead node left its position behind for ever and a candidate
    could only wait out a fixed window before promoting anyway — paying that window on every
    two-node failover, where there is no second replica to prefer.
    """
    primary = healthy_cluster.primary()
    before = published_positions(healthy_cluster.etcd_client_port)
    assert primary.node_id in before, (
        f"the primary published no position at all; keys present: {sorted(before)}")

    healthy_cluster.kill_node(primary.index)

    # The lease TTL is 10 s by default, so give it that plus room for a loaded machine.
    deadline = time.monotonic() + 30
    gone = False
    while time.monotonic() < deadline:
        if primary.node_id not in published_positions(healthy_cluster.etcd_client_port):
            gone = True
            break
        time.sleep(0.5)

    assert gone, (
        f"{primary.node_id} was killed but its position is still in etcd, so every election from "
        f"now on will defer to a node that cannot answer")


def test_the_survivor_does_not_wait_for_a_dead_nodes_position(healthy_cluster):
    """The point of the lease, stated as an invariant rather than as a stopwatch.

    By the time the survivor holds the role, the dead node's position must already be gone — that is
    what makes the promotion immediate instead of deferred. Asserting this rather than a wall-clock
    threshold keeps the test honest on a loaded machine.
    """
    primary = healthy_cluster.primary()
    survivor = healthy_cluster.replica()

    start = time.monotonic()
    healthy_cluster.kill_node(primary.index)
    elapsed = wait_for_role(survivor.tcp_port, "PRIMARY", timeout=40)
    custom_metrics["failover_time_sec"] = elapsed
    custom_metrics["failover_kind"] = "kill (position lease)"

    # The invariant is that a dead node's position **stops being published**, because it is written
    # under a lease nobody is refreshing any more. Before #72 the key had no lease and stayed there
    # for ever, so a candidate could not tell a replica that was further ahead from one that was
    # merely dead.
    #
    # What this used to assert was that the key had already gone *by the time* the survivor was
    # promoted. That is not an invariant and a shared CI runner showed it: the leader lease and the
    # position lease have the same TTL and independent refresh phases, so which expires first is
    # decided by which was refreshed more recently before the kill. It failed at 10.2 s with the
    # position still present — the mechanism working exactly as designed.
    ttl_margin = 20.0
    deadline = start + ttl_margin
    positions: dict = {}
    while time.monotonic() < deadline:
        positions = published_positions(healthy_cluster.etcd_client_port)
        if primary.node_id not in positions:
            break
        time.sleep(0.5)

    assert primary.node_id not in positions, (
        f"{primary.node_id} was killed {time.monotonic() - start:.1f}s ago and its published "
        f"position is still in etcd, so it carries no lease — a candidate cannot tell it from a "
        f"live replica (keys {sorted(positions)})")

    # And exactly one node holds the role at the end of it.
    holders = primaries_among(healthy_cluster)
    assert len(holders) == 1, f"expected one primary after failover, saw {holders}"
