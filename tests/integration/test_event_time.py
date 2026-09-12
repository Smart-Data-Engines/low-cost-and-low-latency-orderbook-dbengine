"""A write keeps the time its sender gave it, over the wire (#105).

Measured before this existed, by #39 part two: the dataset's own span selected **0 of 400 rows**
through this protocol while the same load into ClickHouse and TimescaleDB selected 400. The embedded
path had honoured `timestamp_ns` since the first version, so the value was accepted by the client,
dropped at the wire, and the row stored with its arrival time — a write nobody could find by the
time they went looking for it.

Every assertion here has a control beside it. A server that stamped *every* row with the sender's
time would pass the first test and fail the second, and that is the pair that means something.
"""

from __future__ import annotations

import pytest

from orderbook_engine import OrderbookEngine, OrderbookError

pytestmark = pytest.mark.smoke

# Far enough from now that arrival time cannot be mistaken for it, and narrow enough that "the row
# is somewhere in the store" does not pass: the window below is two nanoseconds wide.
T = 1_700_000_000_000_000_000


def _rows_in_span(engine: OrderbookEngine, symbol: str, lo: int, hi: int) -> int:
    try:
        return len(engine.query(f"SELECT * FROM '{symbol}'.'EX' "
                                f"WHERE timestamp BETWEEN {lo} AND {hi}"))
    except OrderbookError as exc:
        if "not found" in str(exc).lower():
            return 0                      # the symbol never got a row at all
        raise


def test_a_write_over_the_wire_keeps_its_own_event_time(primary_client):
    primary_client.insert("EVT-WIRE", "EX", "bid", [100_000], [10], timestamp_ns=T)
    primary_client.insert("EVT-ARRIVAL", "EX", "bid", [100_000], [10])
    primary_client.insert("EVT-BATCH", "EX", "bid", [100_000, 99_999], [10, 11], timestamp_ns=T)
    primary_client.flush()

    assert _rows_in_span(primary_client, "EVT-WIRE", T - 1, T + 1) == 1, (
        "the row is not inside the span its sender named, so the wire dropped the time and the "
        "server stamped arrival — which is the defect this test exists for, and it answers OK")
    assert _rows_in_span(primary_client, "EVT-BATCH", T - 1, T + 1) == 2, (
        "a batch carries one event time for every level in it")

    # The control. Without it this file would pass against a server that stamped everything 2023.
    assert _rows_in_span(primary_client, "EVT-ARRIVAL", T - 1, T + 1) == 0, (
        "a row nobody gave a time to landed in 2023")
    assert _rows_in_span(primary_client, "EVT-ARRIVAL", 0, 9_999_999_999_999_999_999) == 1, (
        "the row without a time was not stored at all, which is a different defect")


def test_the_server_says_what_it_can_do_and_the_client_asks_before_writing(primary_client):
    """Sending the field is not a test for it, so the question is separate (#105, #107).

    Measured: a server without #107 answers `OK` to `INSERT AAA EX bid 100 5 1 notanumber` and
    stores the row with the token discarded. A client that inferred support from a successful write
    would be inferring it from the very defect it is trying to avoid.
    """
    caps = primary_client.server_capabilities()
    assert "insert_event_time" in caps, f"this build should announce the field it accepts: {caps}"
    assert "strict_args" in caps, f"and the refusal that makes the answer trustworthy: {caps}"

    # `STATUS` is where it comes from, and the line is unconditional so that "none of them" cannot
    # be confused with "the field is missing".
    assert "capabilities" in primary_client.status()


def test_a_client_refuses_rather_than_dropping_what_the_server_cannot_take(primary_client):
    """The half that matters when a cluster is part-way through an upgrade.

    The capability set is replaced with an older server's answer — an empty one — because the
    alternative is keeping a pre-#105 binary around to test against, and the client cannot tell the
    difference between the two.
    """
    original = primary_client.server_capabilities()
    try:
        primary_client._capabilities = set()        # what a server that predates the list says
        with pytest.raises(OrderbookError) as refused:
            primary_client.insert("EVT-REFUSED", "EX", "bid", [100_000], [10], timestamp_ns=T)
        assert "event time" in str(refused.value).lower()
        assert "nothing was sent" in str(refused.value).lower(), (
            f"the refusal does not say whether anything reached the server: {refused.value}")
    finally:
        primary_client._capabilities = original

    # And nothing was sent: the symbol does not exist, which is a stronger statement than zero rows.
    primary_client.flush()
    assert _rows_in_span(primary_client, "EVT-REFUSED", 0, 9_999_999_999_999_999_999) == 0

    # The control: with the capability back, the same call works.
    primary_client.insert("EVT-REFUSED", "EX", "bid", [100_000], [10], timestamp_ns=T)
    primary_client.flush()
    assert _rows_in_span(primary_client, "EVT-REFUSED", T - 1, T + 1) == 1


def test_a_sequence_number_cannot_be_chosen_over_the_wire(primary_client):
    """The same class as the timestamp, and it was dropped just as quietly.

    A sequence number belongs to the origin, and over the wire the origin is the server — it assigns
    one per symbol (pitfall 16). The client accepted the argument and discarded it, which is the
    defect this whole item is about, so it is refused rather than honoured.
    """
    with pytest.raises(OrderbookError) as refused:
        primary_client.insert("EVT-SEQ", "EX", "bid", [100_000], [10], seq=7)
    assert "seq" in str(refused.value).lower()

    # The control: without it, the write goes through and the server numbers it.
    primary_client.insert("EVT-SEQ", "EX", "bid", [100_000], [10])
    primary_client.flush()
    assert _rows_in_span(primary_client, "EVT-SEQ", 0, 9_999_999_999_999_999_999) == 1
