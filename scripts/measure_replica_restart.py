"""What does a replica's own restart cost, and what does it answer while it costs it?

This is the instrument behind roadmap #101's numbers. It exists in the repository rather than in
somebody's scratch directory because the item's public claims are measurements, and a measurement
whose instrument cannot be re-run is an assertion.

Three things per run, because each of them passes on its own while the others are broken:

  * what the restart deletes and rewrites - columnar files, segment directories, and the replica's
    **own** WAL, which used to double on every restart because each replayed record is appended to
    it again;
  * which stream the replica thinks it is on - the identity the primary announced, the identity of
    the replica's own WAL, and the position file, printed together because the whole design turns
    on those first two being different;
  * what it answers while it is short of the data, sampled every 100 ms with the **raw reply**
    rather than a row count.

Measured on i3-7100U, Debug, two-node cluster, restart with no role change.

Before #101 (the defect), 60 batches:

    t from restart   reply                        rows
    0.72 s           OK ob_tcp_server v0.1.0      1
    3.92 s           OK ...                       22 501
    7.13 s           OK ...                       23 501 (complete)

Zero refusals: for 6.4 s the node served reads missing 99.996 % of the data and reported success,
with all 23 501 rows on its disk a second earlier. Silent incompleteness, not an outage a client
notices. The replica's WAL went 1.797 -> 3.590 MB (x2.00) at 12 batches and 5.465 -> 10.915 MB
(x2.00) at 36, so the cost was cumulative: N restarts, N+1 copies of the primary's log.

After #101, same machine, same loads:

    12 batches   707 .col files before -> 707 after, 0 deleted, WAL x1.00, REPLICATE 0 1675350 0
    36 batches   2268 before -> 2268 after, 0 deleted, WAL x1.00, REPLICATE 0 5645932 0
    60 batches   first reply at 0.82 s holds 24 001 of 24 001 rows; the sampling loop breaks on its
                 first probe because there is nothing to wait for

Three notes about the instrument, each of which cost something to learn:

  * **Do not time this from here.** An earlier version reported 6.32 s and 6.33 s for stores
    differing threefold - agreement to the third decimal is the polling quantum, not a measurement.
    What this prints is the node's own `PING` time and the sample times of the raw replies; the
    quantity that made the time grow is how much gets re-streamed, and that is countable.
  * **A label derived from the "before" snapshot describes the defect, not the run.** "Deleted
    bytes" computed as `before.total - before.wal_bytes` was right only while the wipe existed. The
    evidence here is the before/after counters printed side by side.
  * A `SELECT` probe costs its settle plus a read to timeout, so probing every 100 ms does not mean
    sampling every 100 ms. The reply table prints the time each sample was taken.

    OB_INTEGRATION_TESTS=1 .venv/bin/python scripts/measure_replica_restart.py
    OB_MRR_BATCHES=60 OB_INTEGRATION_TESTS=1 .venv/bin/python scripts/measure_replica_restart.py
"""
import os
import socket
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "tests" / "integration"))
os.environ.setdefault("OB_INTEGRATION_TESTS", "1")

from conftest import ClusterManager  # noqa: E402

SYMBOLS = [f"MRR{i:02d}" for i in range(20)]
EXCH = "BINANCE"
LEVELS = 500
BATCHES = int(os.environ.get("OB_MRR_BATCHES", "12"))
SQL = (f"SELECT * FROM '{SYMBOLS[0]}'.'{EXCH}' "
       f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")


def send(port: int, payload: str, settle: float = 0.3, timeout: float = 2.0) -> str:
    """One request, one connection. Returns whatever arrived, including an error line."""
    try:
        sock = socket.create_connection(("127.0.0.1", port), timeout=5.0)
    except OSError as exc:
        return f"<<CONNECT FAILED: {exc}>>"
    try:
        sock.sendall(payload.encode() if payload.endswith("\n") else (payload + "\n").encode())
        time.sleep(settle)
        sock.settimeout(timeout)
        buf = b""
        try:
            while True:
                chunk = sock.recv(1 << 16)
                if not chunk:
                    break
                buf += chunk
        except socket.timeout:
            pass
        return buf.decode(errors="replace")
    finally:
        sock.close()


def classify(reply: str) -> tuple[str, int, str]:
    """Kind, rows, and the first line - so a refusal cannot be read as an empty result.

    The first pass at #101 mapped both onto a row count and could not tell "the replica quietly
    returns less than it has" from "the replica refuses the read". Those are different defects.
    """
    if reply.startswith("<<"):
        return "connect-refused", 0, reply[:70]
    lines = [ln for ln in reply.strip().splitlines() if ln.strip()]
    if not lines:
        return "empty", 0, ""
    if "ERR" in lines[0].upper():
        return "REFUSAL", 0, lines[0][:80]
    return "result", max(0, len(lines) - 2), lines[0][:60]


def rows(port: int) -> int:
    return classify(send(port, SQL, settle=0.5))[1]


def store_shape(path: str) -> dict:
    """Columnar files, segment directories and WAL bytes, counted separately.

    Separately because the defect moved all three and any one of them can look innocent: a node
    that kept its files and re-streamed anyway has the same file count.
    """
    cols = wal_files = wal_bytes = total = 0
    segdirs = 0
    for root, dirs, names in os.walk(path):
        if root == path:
            segdirs = len([d for d in dirs if not d.startswith("wal_")])
        for name in names:
            size = os.path.getsize(os.path.join(root, name))
            total += size
            if name.endswith(".col"):
                cols += 1
            elif name.startswith("wal_") and name.endswith(".bin"):
                wal_files += 1
                wal_bytes += size
    return {"col": cols, "segdirs": segdirs, "wal_files": wal_files,
            "wal_bytes": wal_bytes, "total": total}


def load(port: int) -> None:
    """One connection, many MINSERTs. Volume is the point, not latency."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=60)
    sock.settimeout(60.0)
    try:
        for batch in range(BATCHES):
            for symbol in SYMBOLS:
                base = 100_000 + batch * 1000
                body = "\n".join(f"{base - i} {10 + i} 1" for i in range(LEVELS))
                sock.sendall((f"MINSERT {symbol} {EXCH} bid {LEVELS}\n" + body + "\n").encode())
            time.sleep(0.05)
            try:
                sock.recv(1 << 20)
            except socket.timeout:
                pass
    finally:
        sock.close()


def read_state(data_dir: str) -> str:
    path = Path(data_dir) / "repl_state.txt"
    if not path.exists():
        return "(no repl_state.txt)"
    return " ".join(path.read_text(errors="replace").split())


def identity(data_dir: str) -> str:
    path = Path(data_dir) / "wal_identity"
    return path.read_text(errors="replace").strip() if path.exists() else "(none)"


def main() -> int:
    cluster = ClusterManager()
    cluster.start()
    try:
        primary, replica = cluster.primary(), cluster.replica()
        print(f"batches={BATCHES} symbols={len(SYMBOLS)} levels={LEVELS}")

        started = time.time()
        load(primary.tcp_port)
        send(primary.tcp_port, "FLUSH", settle=1.5)
        want = rows(primary.tcp_port)
        print(f"primary holds {want} rows/symbol after {time.time() - started:.1f}s")
        if want == 0:
            print("the primary stored nothing - nothing here is measurable")
            return 1

        deadline = time.time() + 240
        while time.time() < deadline and rows(replica.tcp_port) < want:
            time.sleep(1.0)
        if rows(replica.tcp_port) != want:
            print("the replica never caught up - nothing here is measurable")
            return 1
        time.sleep(3.0)   # let the replica's own flush timer put the rows into segments

        before = store_shape(replica.data_dir)
        print(f"replica store BEFORE : {before}")
        print(f"primary store        : {store_shape(primary.data_dir)}")
        print(f"replica repl_state   : {read_state(replica.data_dir)}")
        print(f"replica wal_identity : {identity(replica.data_dir)}")
        print(f"primary wal_identity : {identity(primary.data_dir)}")
        print("  ^ these two differ, and the saved stream_id is the *primary's*. A replica that")
        print("    compared its own identity - as #101's first design proposed - would never match.")

        t0 = time.time()
        cluster.restart_node(replica.index)
        replica = cluster.nodes[replica.index]
        print(f"\nnode answers PING after {time.time() - t0:.2f}s "
              f"(its own WAL replay runs before ::bind(), so this is already a complete node "
              f"unless something discarded its store)")

        print(f"\n{'t(s)':>7} {'kind':>16} {'rows':>7}  first line")
        samples = []
        deadline = time.time() + 120
        while time.time() < deadline:
            at = time.time() - t0
            kind, got, head = classify(send(replica.tcp_port, SQL, settle=0.1, timeout=3.0))
            samples.append((round(at, 2), kind, got))
            print(f"{at:7.2f} {kind:>16} {got:7d}  {head}")
            if got >= want:
                break
            time.sleep(0.1)

        after = store_shape(replica.data_dir)
        print(f"\nreplica store AFTER  : {after}")
        print(f"replica repl_state   : {read_state(replica.data_dir)}")
        print(f"col files  {before['col']} -> {after['col']}   "
              f"segment dirs {before['segdirs']} -> {after['segdirs']}")
        ratio = after["wal_bytes"] / before["wal_bytes"] if before["wal_bytes"] else 0.0
        print(f"own WAL    {before['wal_bytes']} -> {after['wal_bytes']} B  (x{ratio:.2f})")

        partial = [(t, r) for t, k, r in samples if k == "result" and 0 < r < want]
        print(f"\nkinds seen: {sorted({k for _, k, _ in samples})}")
        print(f"successful replies short of the primary's row count: {len(partial)}"
              f"{'  e.g. ' + str(partial[:3]) if partial else ''}")
        print(f"refusals: {len([1 for _, k, _ in samples if k == 'REFUSAL'])}")
        print(f"samples until complete: {len(samples)}")

        # What the node said about the stream it decided it was on.
        log = Path(replica.data_dir) / "node.log"
        if log.exists():
            for line in log.read_text(errors="replace").splitlines():
                if ("stream" in line and ("resuming" in line or "discarding" in line)) \
                        or "REPLICATE" in line or "clearing local data" in line:
                    print("LOG:", line[:220])
        return 0
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    sys.exit(main())
