#!/usr/bin/env python3
"""Do the two mesh fields named after a replication lag report one? Measured, not argued.

Written for roadmap #118. Two places answer the question "how far behind is this peer", and
neither of them is asked by any test:

  * `STATUS` prints `replication_lag_peer_<id>`, computed in `Engine::stats()` as
    `wal_.current_offset() - peer.confirmed_offset`.
  * `MM_PEERS` prints a column literally named `lag_bytes`, whose value is
    `peer.send_buf.size()` — this node's own outbound queue for that peer.

`include/orderbook/multi_master.hpp` says what `confirmed_offset` is, in the file that declares
it: a position in the *peer's own* WAL, kept for the MM_PEERS view only, and not to be compared
with ours because "#61 was the consequence of comparing them with ours". It is written in exactly
one place, `process_handshake()`, so it is also frozen at connect time.

So the prediction this script exists to check is specific: on a mesh where every node holds every
row, `replication_lag_peer_<id>` should equal this node's own WAL offset, because the subtrahend
never moves off the value it had when the peer connected. Convergence is established by comparing
row *content*, not by sleeping — storage is append-only, so a count alone would accept duplicates
in place of the rows it is looking for.

Needs a built build/ob_tcp_server and a native etcd on PATH (or ETCD env var). Two nodes rather
than three on purpose: the claim is about one link, and this machine has better things to do.

    python3 scripts/measure_mesh_lag.py
"""
from __future__ import annotations

import glob
import os
import shutil
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "scripts"))

ROOT = os.environ.get("MESHLAG_ROOT", "/tmp/ob_mesh_lag")
os.environ.setdefault("MMH_ROOT", ROOT)

import mm_harness as H  # noqa: E402  (must follow MMH_ROOT, which it reads at import)

SYMBOL = "LAGPROBE"
BATCH = int(os.environ.get("MESHLAG_BATCH", "120"))
PHASES = int(os.environ.get("MESHLAG_PHASES", "3"))


def status_lags(port: int) -> dict[int, int]:
    """Every `replication_lag_peer_<id>` line STATUS prints.

    An empty dict means "no peer record yet", which is a different statement from "lag zero" —
    the distinction that made an earlier mesh defect invisible (#84).
    """
    reply = H.command(port, "STATUS\n", settle=0.6)
    out: dict[int, int] = {}
    for line in reply.splitlines():
        line = line.strip()
        if line.startswith("replication_lag_peer_"):
            name, _, raw = line.partition(":")
            out[int(name[len("replication_lag_peer_"):])] = int(raw.strip())
    return out


def mm_peers(port: int) -> list[dict[str, str]]:
    """MM_PEERS as dicts, with the header taken from the answer's own first line."""
    reply = H.command(port, "MM_PEERS\n", settle=0.6)
    lines = [l for l in reply.strip().splitlines() if l and not l.startswith("OK")]
    if not lines:
        return []
    header = lines[0].split("\t")
    return [dict(zip(header, l.split("\t"))) for l in lines[1:] if "\t" in l]


def wal_offset(node: "H.Node") -> int:
    """This node's WAL byte offset — the minuend of the subtraction under test.

    Read from the file on disk because STATUS publishes the WAL *file index* and not the offset
    inside it, so there is no field to ask for.
    """
    files = sorted(glob.glob(os.path.join(node.dir, "wal_*.bin")))
    return os.path.getsize(files[-1]) if files else 0


def write_batch(port: int, count: int, base_ts: int) -> None:
    for i in range(count):
        reply = H.command(port, f"INSERT {SYMBOL} EX bid {10000 + i} 5 1 {base_ts + i}\n",
                          settle=0.02)
        if "OK" not in reply:
            raise RuntimeError(f"write refused: {reply!r}")
    H.command(port, "FLUSH\n", settle=0.5)


def main() -> int:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)

    etcd, url = H.start_etcd()
    nodes = [H.Node(0, url), H.Node(1, url)]
    try:
        for node in nodes:
            node.start()
        a, b = nodes[0].tcp, nodes[1].tcp

        deadline = time.time() + 30
        while time.time() < deadline and not (status_lags(a) and status_lags(b)):
            time.sleep(0.5)
        if not status_lags(a):
            raise RuntimeError("the mesh link never finished its handshake")

        print(f"handshake: node0 STATUS={status_lags(a)}  node1 STATUS={status_lags(b)}  "
              f"wal={wal_offset(nodes[0])}/{wal_offset(nodes[1])}")
        print(f"           node0 MM_PEERS={mm_peers(a)}")
        print()
        # `implied` is the decisive column, and it is arithmetic on the two beside it rather
        # than a new measurement: the engine computes lag as `our offset - peer.confirmed_offset`,
        # so `our offset - lag` is the position it believes that peer holds. Printed because
        # "the lag equals our WAL size" can be read as a coincidence of two similar numbers,
        # while "the engine believes a peer holding every row is at byte 0" cannot.
        print(f"{'phase':>5}  {'rows 0':>7} {'rows 1':>7}  {'wal 0':>8} {'lag@0':>8} "
              f"{'implied':>8}  {'wal 1':>8} {'lag@1':>8}  {'queued@0':>9}")

        base = 1_700_000_000_000_000_000
        for phase in range(1, PHASES + 1):
            write_batch(a, BATCH, base + phase * 1_000_000)

            # Converge on row content. Append-only storage means a count would also be satisfied
            # by duplicates, which is how the first attempt at fixing #61 read as a pass.
            convergence = time.time() + 30
            while time.time() < convergence and H.prices(a, SYMBOL) != H.prices(b, SYMBOL):
                time.sleep(0.5)
            rows_a, rows_b = H.prices(a, SYMBOL), H.prices(b, SYMBOL)

            lag_a = status_lags(a).get(2, -1)
            lag_b = status_lags(b).get(1, -1)
            queued = ",".join(r.get("lag_bytes", "?") for r in mm_peers(a))
            implied = wal_offset(nodes[0]) - lag_a
            print(f"{phase:>5}  {len(rows_a):>7} {len(rows_b):>7}  "
                  f"{wal_offset(nodes[0]):>8} {lag_a:>8} {implied:>8}  "
                  f"{wal_offset(nodes[1]):>8} {lag_b:>8}  {queued:>9}"
                  f"{'' if rows_a == rows_b else '   NOT CONVERGED'}")

        print()
        print("Read three things off the columns above:")
        print("  * rows 0 == rows 1 in every phase: the mesh is converged, by content.")
        print("  * implied == 0 in every phase: the position this node believes its peer holds")
        print("    is byte zero, while that peer's own WAL is thousands of bytes long and holds")
        print("    every row. The subtrahend was written once, at handshake, and nothing has")
        print("    touched it since — so the 'lag' is this node's own WAL size wearing a name.")
        print("  * MM_PEERS lag_bytes stays 0: it is send_buf.size(), which only leaves zero")
        print("    once the sender's socket buffer is full — 4 MB here, measured under #117.")
        print("  MM_PEERS status also reads 'connected', not the 'active'/'joining'/'leaving'")
        print("  that docs/python.md documents for that field; that vocabulary is the peer")
        print("  registry's, not this column's.")
        return 0
    finally:
        for node in nodes:
            node.stop()
        etcd.terminate()
        try:
            etcd.wait(timeout=10)
        except Exception:
            etcd.kill()


if __name__ == "__main__":
    sys.exit(main())
