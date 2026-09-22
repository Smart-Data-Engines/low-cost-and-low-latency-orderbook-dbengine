#!/usr/bin/env python3
"""What one `BOOK` answer costs, read against the `PING` it travels beside (roadmap #145).

On loopback the round trip is most of a small answer - about 8.0 µs of a ten-level book's 9.7 µs on
an m9g.xlarge - so a `BOOK` latency quoted alone would be read as the cost of the read when it is
mostly the cost of the socket. Every round therefore measures both, on the same node and the same
connection shape, and the table reports the difference and the difference per level.

Three things the script does because an earlier measurement of this exact thing got them wrong:

* **The answer is checked against the question.** A book of N levels per side must come back as
  2N rows plus the `OK`, the header and the terminating blank line. The first probe timed
  `ERR unknown command` in both columns and reported "the read is free"; the probe now refuses an
  error answer, and this script refuses an answer of the wrong shape.
* **A fresh node and data directory per round**, because the book is the only thing that should
  differ between rounds.
* **The order alternates between rounds**, so a warm-cache or turbo-state effect that favours
  whichever runs first cannot masquerade as a difference between the two commands.

    scripts/measure_book_latency.py --server build-release/ob_tcp_server \\
        --probe build-release/benchmarks/command_latency --rounds 3 --iterations 20000

Needs a Release build: Debug is three to four times slower and says nothing about the engine.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def loadavg() -> str:
    return " ".join(Path("/proc/loadavg").read_text().split()[:3])


def start_node(server: Path, data_dir: Path) -> tuple[subprocess.Popen, int]:
    port = free_port()
    log = open(data_dir / "node.log", "wb")
    proc = subprocess.Popen(
        [str(server), "--port", str(port), "--metrics-port", str(free_port()),
         "--data-dir", str(data_dir / "data"), "--flush-interval-ms", "3600000",
         "--drain-timeout-ms", "500"],
        stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise SystemExit(f"node exited: {(data_dir / 'node.log').read_text()[-400:]}")
        if b"listening on port" in (data_dir / "node.log").read_bytes():
            return proc, port
        time.sleep(0.05)
    proc.kill()
    raise SystemExit("node did not start within 15 s")


def seed_book(port: int, levels: int) -> None:
    """Bids descending from 6,500,000 and asks ascending from 6,501,000, one MINSERT per side."""
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.recv(4096)
        for side, base, step in (("bid", 6_500_000, -1), ("ask", 6_501_000, 1)):
            body = "".join(f"{base + step * i} {10 + i} 1\n" for i in range(levels))
            s.sendall(f"MINSERT SYM EX {side} {levels}\n{body}".encode())
            got = b""
            while not got.endswith(b"\n"):
                chunk = s.recv(65536)
                if not chunk:
                    raise SystemExit("seed connection closed")
                got += chunk
            if not got.startswith(b"OK"):
                raise SystemExit(f"seed refused: {got[:200]!r}")


def probe(binary: Path, port: int, iterations: int, pid: int, *command: str) -> dict:
    out = subprocess.run([str(binary), str(port), str(iterations), str(pid), *command],
                         capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(f"probe refused or failed ({out.returncode}): {out.stderr.strip()}")
    return json.loads(out.stdout)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--server", type=Path, default=REPO / "build-release" / "ob_tcp_server")
    ap.add_argument("--probe", type=Path,
                    default=REPO / "build-release" / "benchmarks" / "command_latency")
    ap.add_argument("--iterations", type=int, default=20000)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--levels", default="1,10,100,1000")
    args = ap.parse_args()

    print(f"loadavg at start: {loadavg()}", file=sys.stderr)
    results: dict[int, list[tuple[dict, dict]]] = {}
    for levels in (int(x) for x in args.levels.split(",")):
        for r in range(args.rounds):
            with tempfile.TemporaryDirectory(prefix="book-latency-") as tmp:
                node, port = start_node(args.server, Path(tmp))
                try:
                    seed_book(port, levels)
                    order = ("PING", "BOOK") if r % 2 == 0 else ("BOOK", "PING")
                    got = {}
                    for which in order:
                        cmd = ("PING",) if which == "PING" else ("BOOK", "SYM", "EX")
                        got[which] = probe(args.probe, port, args.iterations, node.pid, *cmd)
                    expected = 2 * levels + 3
                    if got["BOOK"]["answer_lines"] != expected:
                        raise SystemExit(
                            f"REFUSED: a book of {levels} levels per side must be {expected} "
                            f"lines, the answer was {got['BOOK']['answer_lines']}")
                    results.setdefault(levels, []).append((got["PING"], got["BOOK"]))
                    print(json.dumps({"levels_per_side": levels, "round": r + 1,
                                      "loadavg": loadavg(), **{k.lower(): v
                                                               for k, v in got.items()}}))
                finally:
                    node.send_signal(signal.SIGTERM)
                    try:
                        node.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        node.kill()
    print(f"loadavg at end: {loadavg()}", file=sys.stderr)

    print("\n| levels per side | `PING` p50 | `BOOK` p50 | difference | `BOOK` p99 | answer "
          "| per level |")
    print("|---|---|---|---|---|---|---|")
    for levels, rounds in results.items():
        ping = statistics.median(p["p50_ns"] for p, _ in rounds)
        book = statistics.median(b["p50_ns"] for _, b in rounds)
        p99 = statistics.median(b["p99_ns"] for _, b in rounds)
        size = rounds[0][1]["answer_bytes"]
        diff = book - ping
        print(f"| {levels} | {ping:.0f} ns | {book:.0f} ns | {diff:.0f} ns | {p99:.0f} ns "
              f"| {size} B | {diff / (2 * levels):.0f} ns |")
    return 0


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    sys.exit(main())
