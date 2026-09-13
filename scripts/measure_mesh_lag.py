#!/usr/bin/env python3
"""What the mesh reports about how far behind its peers are — before and after #118.

Written to answer the question #117 needed and could not get: `ob_mm_replication_lag_bytes` was
registered and written nowhere, and the two fields that *were* fed turned out not to be lags.

`STATUS` printed `replication_lag_peer_<id>` as this node's WAL offset minus a mesh peer's
`confirmed_offset`, which `include/orderbook/multi_master.hpp` says is a position in the peer's
**own** WAL, written once in `process_handshake()`. On a converged mesh that made the printed lag
equal this node's own WAL offset to the byte — the engine reporting a peer that held every row as
sitting at byte zero. `MM_PEERS`' column was named `lag_bytes` and held `peer.send_buf.size()`,
this node's send queue, flat through the 4 MB the sender's socket buffer holds.

Neither field exists in that form any more. This script now measures the replacement, and the
acceptance is the pair: on a converged mesh `ob_mm_replication_lag_records` is **0** with
`ob_mm_peers_position_unknown` also 0 — the second half matters, because a peer that has not
stated what it holds is reported by the comparison as holding nothing, so a lag of zero alone
cannot tell "converged" from "no idea".

Needs a built build/ob_tcp_server and a native etcd on PATH (or ETCD env var). Two nodes rather
than three: the claim is about one link.

    python3 scripts/measure_mesh_lag.py
"""
from __future__ import annotations

import glob
import os
import urllib.request
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


def metrics(port: int) -> dict[str, float]:
    """The metrics endpoint as a name -> value map, comments dropped."""
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as fh:
        body = fh.read().decode()
    out: dict[str, float] = {}
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, value = line.partition(" ")
        try:
            out[name.split("{")[0]] = float(value)
        except ValueError:
            continue
    return out


def status_has_byte_lag(port: int) -> bool:
    """Whether STATUS still prints a per-peer byte lag, which it must not (#118)."""
    return "replication_lag_peer_" in H.command(port, "STATUS\n", settle=0.6)


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
        ma, mb = nodes[0].ports[1], nodes[1].ports[1]

        # Wait for the single mesh link to finish its handshake. MM_PEERS lists peers, meaning
        # connections whose handshake has named them, so an empty answer is "not connected yet"
        # rather than "no lag".
        deadline = time.time() + 30
        while time.time() < deadline and not (mm_peers(a) and mm_peers(b)):
            time.sleep(0.5)
        if not mm_peers(a):
            raise RuntimeError("the mesh link never finished its handshake")

        # The two fields #118 removed must be gone, and this is the cheap half of the check.
        for port, name in ((a, "node0"), (b, "node1")):
            if status_has_byte_lag(port):
                print(f"REGRESSION: {name} STATUS still prints replication_lag_peer_")
            columns = set(mm_peers(port)[0])
            if "lag_bytes" in columns:
                print(f"REGRESSION: {name} MM_PEERS still has a lag_bytes column")
        print(f"MM_PEERS columns: {sorted(set(mm_peers(a)[0]))}")
        print()
        print(f"{'phase':>5}  {'rows 0':>7} {'rows 1':>7}  {'wal 0':>8}  "
              f"{'lag_records':>11} {'unknown':>8}  {'queue_bytes':>11}")

        base = 1_700_000_000_000_000_000
        worst_lag = 0.0
        for phase in range(1, PHASES + 1):
            write_batch(a, BATCH, base + phase * 1_000_000)

            # Converge on row content. Append-only storage means a count would also be satisfied
            # by duplicates, which is how the first attempt at fixing #61 read as a pass.
            convergence = time.time() + 40
            while time.time() < convergence and H.prices(a, SYMBOL) != H.prices(b, SYMBOL):
                time.sleep(0.5)
            rows_a, rows_b = H.prices(a, SYMBOL), H.prices(b, SYMBOL)

            # The anti-entropy pass is what recomputes the records lag, so give it one interval
            # to run before reading — a gauge that is only as fresh as that pass has to be read
            # that way, and reading it sooner would measure the interval rather than the mesh.
            time.sleep(float(os.environ.get("MMH_AE_INTERVAL", "3")) + 1.0)
            m = metrics(mb)
            lag = m.get("ob_mm_replication_lag_records", -1.0)
            unknown = m.get("ob_mm_peers_position_unknown", -1.0)
            worst_lag = max(worst_lag, lag)
            queue = ",".join(r.get("send_queue_bytes", "?") for r in mm_peers(a))

            print(f"{phase:>5}  {len(rows_a):>7} {len(rows_b):>7}  "
                  f"{wal_offset(nodes[0]):>8}  {lag:>11.0f} {unknown:>8.0f}  {queue:>11}"
                  f"{'' if rows_a == rows_b else '   NOT CONVERGED'}")

        print()
        print("Read the pair, not either half:")
        print("  * lag_records 0 with unknown 0 is the only reading that means converged. A lag of")
        print("    zero on its own cannot tell that from a peer that has not said what it holds,")
        print("    because the comparison reports silence as holding nothing.")
        print("  * queue_bytes is the old `lag_bytes` column under the name of what it holds —")
        print("    this node's send queue, zero on a healthy link and zero through the 4 MB the")
        print("    sender's socket buffer absorbs (#117). It was never a lag.")
        print(f"  * worst lag_records seen across the run: {worst_lag:.0f}")
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
