#!/usr/bin/env python3
"""What refreshing the version vector at every flush tick costs a mesh node (#180).

Until #180 the vector a peer is told was exported only at a checkpoint, and since part 2a of #165 a
tick that seals nothing writes none - so a peer could be told, for up to ten seconds, that this node
lacked what it held. Since the fix every tick exports it again whenever the sequence tracker moved:
under the engine's lock, one pass over every (symbol, origin) the node holds - at most 4 096 entries
until #177. This measured that at the largest vector a node stated then. Three nodes; `--symbols` symbols written
once on the first, so every node holds that many entries; then `benchmarks/pipelined_ingest`
against the first node, whose writes move the tracker between any two ticks. Each run prints the
ingest's rate in levels per second and its batch round trip at p50, p99, p99.9 and max - a lock
held once a tick is a tail, not a median.

**On a build before #177, keep `--symbols` under 1 561** for the rounds of a mesh: past that a
vector did not fit the record that carries it and was sent as "send everything", so every
reconciliation resent the whole WAL and that, not the refresh, was what the ingest paid. Since #177
a vector of any size goes in parts. `--nodes 1` measures the refresh alone: a mesh node with no peer
broadcasts and resends nothing, so what differs between builds is the tick.

    scripts/measure_vector_refresh.py --server before=<ob_tcp_server> --server after=<ob_tcp_server> \\
        --probe build-release/benchmarks/pipelined_ingest [--symbols 1500] [--rounds 4]

`--probe-seconds S` measures a write's latency instead of the ceiling: one `INSERT` at a time on the
writer for S seconds, beside a trickle writing `--trickle-symbols` of the prefilled symbols every
100 ms, so that every tick has that many frontiers to bring the vector up to date with - the load
at which a lock the tick holds shows, rather than a ceiling at which the flush is the bottleneck.

Builds alternate ABAB... over `--rounds`. `--refresh-log LABEL` adds one run of that build with its
nodes at DEBUG and reports what each refresh took from the engine's own line - the cost itself,
rather than its effect on a client; DEBUG costs throughput, so that run is not one of the rounds.
Needs a native etcd on PATH (or ETCD).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "scripts"))

import measure_mesh_catchup as M  # noqa: E402

BASE_TS = 1_700_000_000_000_000_000

# What a round costs the device, beside what it delivered (#190 step 9): the bytes the server wrote,
# and the ticks, seals and merges it ran, over the ingest alone - and what held the writers back:
# admission's delays and their time, and the writes that waited for a sync or for the queue.
COSTS = ("ob_flush_ticks_total", "ob_seals_total", "ob_compactions_total", "ob_compaction_rows_total",
         "ob_writer_admission_delays_total", "ob_writer_admission_delay_us_total",
         "ob_writer_wal_budget_waits_total", "ob_writer_backpressure_waits_total")


def written_bytes(pid: int) -> int:
    """`write_bytes` of /proc/<pid>/io: what the process sent towards the storage layer."""
    with open(f"/proc/{pid}/io") as f:
        for line in f:
            if line.startswith("write_bytes:"):
                return int(line.split()[1])
    raise RuntimeError(f"no write_bytes in /proc/{pid}/io")


def device_stat(device: str) -> tuple[int, int]:
    """Sectors written and milliseconds busy, from /sys/block/<device>/stat (fields 7 and 10)."""
    fields = open(f"/sys/block/{device}/stat").read().split()
    return int(fields[6]), int(fields[9])


def xfs_log_stat(fs: str) -> tuple[int, int, int]:
    """The XFS journal's writes, the 512-byte blocks they wrote and its forces, from the `log` line
    of /sys/fs/xfs/<fs>/stats/stats (xs_log_writes, xs_log_blocks, xs_log_force)."""
    for line in open(f"/sys/fs/xfs/{fs}/stats/stats"):
        if line.startswith("log "):
            fields = line.split()
            return int(fields[1]), int(fields[2]), int(fields[4])
    raise RuntimeError(f"no log line in /sys/fs/xfs/{fs}/stats/stats")


def charged_bytes(device: str) -> int | None:
    """What this harness's cgroup - the scope that caps the writes, the server and etcd in it - wrote
    to <device>: `wbytes` of its io.stat, counted as each write passes the cap. Beside the cap and the
    ingest's length it says how much of the cap the round used. None without an io.stat."""
    major_minor = open(f"/sys/block/{device}/dev").read().strip()
    group = open("/proc/self/cgroup").read().strip().split("::", 1)[-1]
    try:
        for line in open(f"/sys/fs/cgroup{group}/io.stat"):
            if line.startswith(major_minor + " "):
                return int(dict(kv.split("=", 1) for kv in line.split()[1:])["wbytes"])
    except FileNotFoundError:
        return None
    return 0


def counters(metrics_port: int) -> dict:
    """The COSTS counters from a node's /metrics, each summed over its label sets (pitfall 66)."""
    import urllib.request
    text = urllib.request.urlopen(f"http://127.0.0.1:{metrics_port}/metrics", timeout=10).read().decode()
    out = {k: 0 for k in COSTS}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        name = line.split("{", 1)[0].split(" ", 1)[0]
        if name in out:
            out[name] += int(float(line.rsplit(" ", 1)[1]))
    return out


def prefill(port: int, symbols: int, batch: int = 256) -> None:
    """One single-level MINSERT for each of `symbols` symbols, pipelined."""
    c = M.Conn(port)
    try:
        cmds = [f"MINSERT V{i:04d} EX bid 1 {BASE_TS + i}\n100000 5 1\n" for i in range(symbols)]
        for i in range(0, len(cmds), batch):
            chunk = cmds[i:i + batch]
            c.s.sendall("".join(chunk).encode())
            for _ in chunk:
                line = c.r.readline()
                if not line.startswith(b"OK"):
                    raise RuntimeError(f"write refused: {line!r}")
                c.r.readline()
    finally:
        c.close()


class Trickle(threading.Thread):
    """`count` of the prefilled symbols written once each every 100 ms, pipelined: so that every
    tick has that many frontiers to bring the vector up to date with."""

    def __init__(self, port: int, symbols: int, count: int):
        super().__init__(daemon=True)
        self.port, self.symbols, self.count = port, symbols, count
        self.stop_flag = threading.Event()

    def run(self) -> None:
        c = M.Conn(self.port)
        rng, ts = random.Random(180), BASE_TS + 10_000_000
        while not self.stop_flag.is_set():
            started = time.monotonic()
            cmds = []
            for s in rng.sample(range(self.symbols), self.count):
                ts += 1
                cmds.append(f"MINSERT V{s:04d} EX bid 1 {ts}\n{100_000 + ts % 997} 5 1\n")
            c.s.sendall("".join(cmds).encode())
            for _ in cmds:
                c.r.readline()
                c.r.readline()
            self.stop_flag.wait(max(0.0, 0.1 - (time.monotonic() - started)))
        c.close()


def probe_run(writer, symbols: int, args) -> dict:
    """One INSERT at a time on the writer for `--probe-seconds`, with the trickle running - from
    `benchmarks/command_latency` when `--latency-probe` names it, whose clock has no interpreter
    in it, and from Python otherwise."""
    trickle = Trickle(writer.tcp, symbols, args.trickle_symbols)
    trickle.start()
    time.sleep(1)
    if args.latency_probe:
        out = subprocess.run([args.latency_probe, str(writer.tcp), str(args.probe_iterations),
                              str(writer.proc.pid), "INSERT", "PROBE", "EX", "bid", "100000", "1",
                              "1"], capture_output=True, text=True, timeout=900)
        trickle.stop_flag.set()
        trickle.join(timeout=5)
        if out.returncode != 0:
            return {"failed": (out.stderr.strip().splitlines() or ["?"])[-1][:160]}
        r = json.loads(out.stdout.strip().splitlines()[-1])
        return {"probe_n": r["iterations"], "probe_p50_ms": r["p50_ns"] / 1e6,
                "probe_p99_ms": r["p99_ns"] / 1e6, "probe_p999_ms": r["p999_ns"] / 1e6,
                "probe_max_ms": r["max_ns"] / 1e6, "probe_wall_s": r["wall_s"]}
    probe = M.Probe(writer.tcp)
    probe.start()
    time.sleep(args.probe_seconds)
    probe.stop_flag.set()
    probe.join(timeout=5)
    trickle.stop_flag.set()
    trickle.join(timeout=5)
    xs = sorted(ms for _, ms in probe.samples)
    return {"probe_n": len(xs), "probe_p50_ms": round(statistics.median(xs), 3),
            "probe_p99_ms": round(xs[int(len(xs) * 0.99)], 3),
            "probe_p999_ms": round(xs[int(len(xs) * 0.999)], 3), "probe_max_ms": round(xs[-1], 3),
            "probe_over_1ms": sum(1 for x in xs if x > 1.0)}


def one_run(label: str, server: str, probe: str, symbols: int, args, root: str,
            log_level: str = "INFO") -> dict:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    etcd, url = M.start_etcd(root)
    nodes = [M.Node(server, root, i, url, log_level) for i in range(args.nodes)]
    try:
        for n in nodes:
            n.start()
        time.sleep(3)
        writer = nodes[0]
        prefill(writer.tcp, symbols)
        last = f"V{symbols - 1:04d}"
        deadline = time.monotonic() + 120
        while not all(M.has_row_at(n.tcp, last, BASE_TS + symbols - 1) for n in nodes[1:]):
            if time.monotonic() > deadline:
                raise RuntimeError("the mesh did not deliver the prefill")
            time.sleep(0.5)
        time.sleep(2)
        before = os.path.getsize(writer.log)
        if args.probe_seconds:
            # Past the prefill's seals: its stores come due ten seconds after their row.
            time.sleep(12)
            before = os.path.getsize(writer.log)
            result = probe_run(writer, symbols, args)
        else:
            bytes_before = written_bytes(writer.proc.pid)
            costs_before = counters(writer.ports[1])
            dev_before = device_stat(args.block_device) if args.block_device else None
            charged_before = charged_bytes(args.block_device) if args.block_device else None
            log_before = xfs_log_stat(args.xfs_stats) if args.xfs_stats else None
            began = time.monotonic()
            out = subprocess.run([probe, str(writer.tcp), str(writer.proc.pid),
                                  str(args.connections), str(args.batches), str(args.levels),
                                  str(args.batch)], capture_output=True, text=True, timeout=900)
            if out.returncode == 0:
                result = json.loads(out.stdout.strip().splitlines()[-1])
            else:
                result = {"failed": (out.stderr.strip().splitlines() or ["?"])[-1][:160]}
            result["written_mb"] = round((written_bytes(writer.proc.pid) - bytes_before) / 1e6, 1)
            if dev_before:
                # What reached the device, the journal and every other writer on it included, and
                # the share of the ingest it was busy - a device below its cap and not busy is one
                # the engine left idle.
                sectors, busy_ms = device_stat(args.block_device)
                elapsed = time.monotonic() - began
                result["device_mb"] = round((sectors - dev_before[0]) * 512 / 1e6, 1)
                result["device_busy_pct"] = round(100.0 * (busy_ms - dev_before[1]) / 1000 / elapsed, 1)
                result["ingest_s"] = round(elapsed, 2)
                if charged_before is not None:
                    # What passed the cap: over the ingest's length, the share of the cap the round
                    # used, which the device's busy time cannot say when the cap holds the writes
                    # above its queue.
                    result["charged_mb"] = round(
                        (charged_bytes(args.block_device) - charged_before) / 1e6, 1)
            if log_before:
                # The share of those bytes that was the filesystem's journal, and how often it was
                # forced to the device: every fsync and syncfs that finds the journal holding
                # something forces it, and each force writes at least the log's stripe unit.
                writes, blocks, forces = xfs_log_stat(args.xfs_stats)
                result["xfs_log_mb"] = round((blocks - log_before[1]) * 512 / 1e6, 1)
                result["xfs_log_writes"] = writes - log_before[0]
                result["xfs_log_forces"] = forces - log_before[2]
            costs_after = counters(writer.ports[1])
            for k in COSTS:
                result[k.replace("ob_", "").replace("_total", "")] = costs_after[k] - costs_before[k]
        result["label"] = label
        with open(writer.log, "rb") as f:
            f.seek(before)
            text = f.read().decode(errors="replace")
        # What the writer said while the ingest ran: a write waiting for room in the pending queue,
        # or - since #190 step 9 - for the WAL to reach the device, one refused because no room
        # came, and a peer dropped for not draining its send buffer. A refusal is counted by the
        # clause both of its lines share: step 9 gave a refusal during a WAL sync a line of its own,
        # and counting the old line alone would have read every such refusal as none.
        result["queue_waits"] = text.count("is waiting for room in the pending queue")
        result["budget_waits"] = text.count("is waiting for the WAL to reach the device")
        result["refused"] = text.count("so this write is refused rather than accepted")
        result["peer_drops"] = text.count("is not draining")
        if log_level == "DEBUG":
            took = [(int(m.group(1)), int(m.group(2))) for m in re.finditer(
                r"Version vector cache refreshed at tracker generation \d+: (\d+) entries in "
                r"(\d+) us", text)]
            result["refreshes"] = len(took)
            if took:
                us = sorted(t for _, t in took)
                result["refresh_entries_max"] = max(e for e, _ in took)
                result["refresh_us_p50"] = statistics.median(us)
                result["refresh_us_p99"] = us[int(len(us) * 0.99)]
                result["refresh_us_max"] = us[-1]
        return result
    finally:
        for n in nodes:
            n.stop()
        etcd.terminate()
        etcd.wait(timeout=10)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", action="append", required=True, help="label=path")
    ap.add_argument("--probe", required=True, help="benchmarks/pipelined_ingest")
    ap.add_argument("--symbols", type=int, default=1500)
    ap.add_argument("--rounds", type=int, default=4)
    ap.add_argument("--connections", type=int, default=2)
    ap.add_argument("--batches", type=int, default=2000, help="per connection")
    ap.add_argument("--levels", type=int, default=20)
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--probe-seconds", type=float, default=0.0,
                    help="instead of the ingest: one INSERT at a time on the writer for this long, "
                         "beside a trickle of --trickle-symbols writes every 100 ms")
    ap.add_argument("--trickle-symbols", type=int, default=500)
    ap.add_argument("--latency-probe", metavar="PATH",
                    help="benchmarks/command_latency, to time the probe's INSERTs from C++")
    ap.add_argument("--probe-iterations", type=int, default=300_000)
    ap.add_argument("--nodes", type=int, default=3,
                    help="1 measures the refresh alone: no peer, so no broadcast and no resend")
    ap.add_argument("--refresh-log", metavar="LABEL")
    ap.add_argument("--root", default=os.path.join(os.getcwd(), "vector-refresh-runs"))
    ap.add_argument("--block-device", metavar="NAME",
                    help="also record what reached /sys/block/NAME and how busy it was")
    ap.add_argument("--xfs-stats", metavar="FS",
                    help="also record what the XFS journal of /sys/fs/xfs/FS wrote, and its forces")
    args = ap.parse_args()
    servers = [s.split("=", 1) for s in args.server]
    keys = ("levels_per_s", "batch_p50_us", "batch_p99_us", "batch_p999_us", "batch_max_us",
            "server_cores", "probe_n", "probe_p50_ms", "probe_p99_ms", "probe_p999_ms",
            "probe_max_ms", "probe_over_1ms", "probe_wall_s", "queue_waits", "budget_waits",
            "refused", "written_mb", "device_mb", "device_busy_pct", "ingest_s", "charged_mb",
            "xfs_log_mb", "xfs_log_writes", "xfs_log_forces", "flush_ticks", "seals", "compactions",
            "compaction_rows", "writer_admission_delays", "writer_admission_delay_us",
            "writer_wal_budget_waits", "writer_backpressure_waits",
            "peer_drops", "failed")
    for r in range(args.rounds):
        for label, path in (servers if r % 2 == 0 else list(reversed(servers))):
            # A directory per run, not per build: an outlier is read in the log of the run it
            # happened in, and the next run of the same build used to remove it first.
            res = one_run(label, path, args.probe, args.symbols, args,
                          os.path.join(args.root, f"{label}-round{r + 1}"))
            print(f"round {r + 1} {label}: " + " ".join(f"{k}={res[k]}" for k in keys if k in res),
                  flush=True)
    if args.refresh_log:
        path = dict(servers)[args.refresh_log]
        res = one_run(args.refresh_log, path, args.probe, args.symbols, args,
                      os.path.join(args.root, args.refresh_log + "-debug"), "DEBUG")
        extra = ("refreshes", "refresh_entries_max", "refresh_us_p50", "refresh_us_p99",
                 "refresh_us_max")
        print(f"debug {args.refresh_log}: " + " ".join(
            f"{k}={res.get(k, '-')}" for k in keys + extra), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
