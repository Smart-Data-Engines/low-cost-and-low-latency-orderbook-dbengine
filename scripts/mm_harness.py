#!/usr/bin/env python3
"""Three multi-master nodes, killed and restarted in a loop, with logs kept on disk.

Written to reproduce roadmap #61, and kept because the integration suite cannot replace it.
`tests/integration/conftest.py` keeps each node's stdout in a pipe, so the line that mattered —
the catch-up decision — was invisible: the suite could tell you rows were missing, not why. Here
every node logs at DEBUG into its own file, which is how the cause was found:

    cycle 0: Peer 3 is behind (peer: file=0 off=174, local: file=0 off=522) — starting catch-up
    cycle 1: Peer 3 is behind (peer: file=0 off=846, local: file=0 off=870) — starting catch-up

The other thing this harness exists for is *repetition*. One outage recovered by luck for months,
because the two nodes' byte offsets happened to line up; the defect only showed from the second
outage on. Any test of catch-up that runs a single outage proves very little.

Row counts are exact on purpose. The first attempt at fixing #61 replaced the missing rows with
duplicated ones — storage is append-only — and a check that only looks for absent prices calls
that a pass.

Two scenarios:

    MMH_CYCLES=4 python3 scripts/mm_harness.py            # outage: kill a node, restart it
    MMH_MODE=partition python3 scripts/mm_harness.py      # partition: block the link, keep the
                                                          # node running, let reconciliation fix it

The partition mode is what proves anti-entropy (roadmap #57) rather than catch-up: the node stays
up, so nothing reconnects and no handshake happens. Only the periodic vector exchange can close the
difference. It needs `sudo iptables`, which is why it is a local tool and not part of the pytest
suite.

Needs a built build/ob_tcp_server and a native etcd on PATH (or ETCD env var).
"""
from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.environ.get("OB_SERVER_BINARY", os.path.join(REPO, "build", "ob_tcp_server"))
ETCD = os.environ.get("ETCD", shutil.which("etcd") or "/usr/local/bin/etcd")
ROOT = os.environ.get("MMH_ROOT", "/tmp/ob_mm_harness")
SYMBOL = "MMH-CATCHUP"
MESH_PORTS: set[int] = set()   # every node's multi-master port, filled in as nodes start


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def command(port: int, text: str, settle: float = 0.4, timeout: float = 15.0) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.settimeout(timeout)
        s.recv(4096)  # banner
        s.sendall(text.encode())
        time.sleep(settle)
        try:
            return s.recv(1 << 20).decode(errors="replace")
        except socket.timeout:
            return ""


def prices(port: int, symbol: str = SYMBOL) -> list[int]:
    reply = command(
        port,
        f"SELECT * FROM '{symbol}'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n",
        settle=0.8)
    out = []
    for line in reply.strip().splitlines():
        fields = line.split("\t")
        if fields and fields[0].isdigit() and len(fields) > 1:
            out.append(int(fields[1]))
    return sorted(out)


class Node:
    def __init__(self, index: int, etcd_url: str):
        self.index = index
        self.etcd_url = etcd_url
        self.dir = os.path.join(ROOT, f"node{index}")
        os.makedirs(self.dir, exist_ok=True)
        self.ports: tuple[int, int, int, int] = (0, 0, 0, 0)
        self.proc: subprocess.Popen | None = None

    def start(self, reuse_ports: bool = False) -> None:
        # Ports are reused across a restart so the peer registry sees the same node coming
        # back rather than a new one.
        if not reuse_ports or self.ports == (0, 0, 0, 0):
            self.ports = (free_port(), free_port(), free_port(), free_port())
        tcp, metrics, repl, mm = self.ports
        MESH_PORTS.add(mm)
        log = open(os.path.join(ROOT, f"node{self.index}.log"), "ab")
        self.proc = subprocess.Popen([
            SERVER, "--port", str(tcp), "--data-dir", self.dir,
            "--metrics-port", str(metrics), "--replication-port", str(repl),
            "--coordinator-endpoints", self.etcd_url, "--node-id", f"node-{self.index}",
            "--multi-master", "--mm-node-id", str(self.index + 1),
            "--mm-replication-port", str(mm), "--log-level", "DEBUG",
            # Short enough that a reconciliation pass happens inside a test rather than in half
            # a minute; the production default is 30 s.
            "--anti-entropy-interval-seconds", os.environ.get("MMH_AE_INTERVAL", "3"),
            # A small catch-up ceiling on purpose: the partition scenario needs backpressure to
            # discard a peer's backlog, which is the one divergence TCP cannot undo by itself.
            "--mm-max-catchup-bytes", os.environ.get("MMH_MAX_CATCHUP", "8192"),
            "--mm-max-peer-send-buffer", os.environ.get("MMH_MAX_SEND_BUF", "262144"),
        ], stdout=log, stderr=subprocess.STDOUT)

        deadline = time.time() + 25
        while time.time() < deadline:
            try:
                if "PONG" in command(tcp, "PING\n", settle=0.1, timeout=2):
                    return
            except OSError:
                time.sleep(0.3)
        raise RuntimeError(f"node{self.index} did not come up on port {tcp}")

    @property
    def tcp(self) -> int:
        return self.ports[0]

    def kill(self) -> None:
        """SIGKILL: no drain, no flush — an outage, not a shutdown."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=10)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def start_etcd(client_port: int | None = None) -> tuple[subprocess.Popen, str]:
    """Start etcd and return (process, client_url).

    ``client_port`` lets a caller reserve the port before etcd exists, which is how you test a node
    that boots while its coordinator is still down.
    """
    peer_port = free_port()
    if client_port is None:
        client_port = free_port()
    url = f"http://127.0.0.1:{client_port}"
    log = open(os.path.join(ROOT, "etcd.log"), "wb")
    proc = subprocess.Popen([
        ETCD, "--name", "mmh", "--data-dir", os.path.join(ROOT, "etcd"),
        "--advertise-client-urls", url, "--listen-client-urls", url,
        "--listen-peer-urls", f"http://127.0.0.1:{peer_port}",
        "--initial-advertise-peer-urls", f"http://127.0.0.1:{peer_port}",
        "--initial-cluster", f"mmh=http://127.0.0.1:{peer_port}",
    ], stdout=log, stderr=subprocess.STDOUT)
    time.sleep(3)
    return proc, url


def dump_stacks(nodes: list[Node]) -> None:
    """Thread stacks of whatever is still alive.

    A hang here has twice been a lock problem inside the engine, and both times the stacks
    named it in seconds while reasoning about it produced wrong answers. `sudo` is needed
    because ptrace_scope blocks attaching to a sibling process.
    """
    for node in nodes:
        if node.proc and node.proc.poll() is None:
            path = os.path.join(ROOT, f"stacks_node{node.index}.txt")
            try:
                out = subprocess.run(
                    ["sudo", "gdb", "-p", str(node.proc.pid), "-batch",
                     "-ex", "thread apply all bt"],
                    capture_output=True, text=True, timeout=120)
                with open(path, "w") as fh:
                    fh.write(out.stdout + out.stderr)
                print(f"  thread stacks for node{node.index}: {path}")
            except Exception as exc:  # noqa: BLE001 - diagnostics must not mask the failure
                print(f"  could not dump stacks for node{node.index}: {exc!r}")


def victim_ports(pid: int, mm_port: int) -> list[int]:
    """Every TCP port the victim uses for multi-master, listening and ephemeral.

    Blocking only the listening port does not partition a full mesh: each node also *dials* its
    peers, and those connections carry an ephemeral local port that no port-based rule on the
    listening side touches. Measured the hard way — the first version of this scenario reported a
    partition while every write still arrived.

    On loopback every address is 127.0.0.1, so the peers cannot be told apart by address; the
    victim's own socket list is what makes the cut precise.
    """
    ports = {mm_port}
    out = subprocess.run(["ss", "-tnpH", "state", "established"],
                         capture_output=True, text=True).stdout
    for line in out.splitlines():
        if f"pid={pid}," not in line:
            continue
        fields = line.split()
        if len(fields) < 4:
            continue
        local, remote = fields[2], fields[3]
        try:
            local_port, remote_port = int(local.rsplit(":", 1)[1]), int(remote.rsplit(":", 1)[1])
        except (IndexError, ValueError):
            continue
        # Only the multi-master mesh: leave the client port and the metrics port alone, or the
        # harness would cut the connection it uses to ask questions.
        if remote_port in MESH_PORTS or local_port == mm_port:
            ports.add(local_port)
    return sorted(ports)


def block_link(ports: list[int]) -> None:
    """Drop traffic on the victim's mesh sockets, both directions.

    A partition, not a pause: `kill -STOP` would leave the sender filling socket buffers and the
    records would arrive on resume, which proves nothing about reconciliation.
    """
    for port in ports:
        for chain, flag in (("INPUT", "--dport"), ("INPUT", "--sport"),
                            ("OUTPUT", "--dport"), ("OUTPUT", "--sport")):
            subprocess.run(["sudo", "iptables", "-I", chain, "-p", "tcp", flag, str(port),
                            "-j", "DROP"], check=True, capture_output=True)


def unblock_link(ports: list[int]) -> None:
    for port in ports:
        for chain, flag in (("INPUT", "--dport"), ("INPUT", "--sport"),
                            ("OUTPUT", "--dport"), ("OUTPUT", "--sport")):
            subprocess.run(["sudo", "iptables", "-D", chain, "-p", "tcp", flag, str(port),
                            "-j", "DROP"], check=False, capture_output=True)


def status_value(port: int, field: str) -> int:
    reply = command(port, "STATUS\n", settle=0.5)
    for line in reply.splitlines():
        if line.strip().startswith(f"{field}:"):
            raw = line.split(":", 1)[1].strip()
            return int(raw) if raw.isdigit() else 0
    return 0


def rss_mb(pid: int) -> float:
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return 0.0


def count_drops(writer: "Node", since: str) -> int:
    """How many times the writer dropped a peer for not draining, since `since`."""
    path = os.path.join(ROOT, f"node{writer.index}.log")
    if not os.path.isfile(path):
        return 0
    n = 0
    with open(path, errors="replace") as fh:
        for line in fh:
            if "is not draining" in line and line_after(line, since):
                n += 1
    return n


def run_slow_peer(nodes: list[Node], writer: Node, victim: Node) -> int:
    """Stop a peer from reading and keep writing: is the writer's queued output bounded?

    Before roadmap #69 nothing capped `peer.send_buf` on the live path — `check_backpressure()` ran
    only inside the catch-up loop — so a peer that stopped reading grew the writer with no limit. The
    ceiling is `--mm-max-peer-send-buffer`, and on overflow the connection is dropped rather than the
    buffer cleared: after a partial write the buffer can begin mid-frame, and abandoning half a frame
    desynchronises the peer's parser.

    SIGSTOP rather than iptables, deliberately. A DROP rule leaves the kernel buffers to fill at
    whatever rate autotuning picks, and the ceiling then trips somewhere between 240k and never — one
    run tripped, another did not at 320k levels. A stopped process reliably stops reading, so the
    buffers fill in order and the trip point is repeatable at about 160k levels with a 256 KB ceiling.

    RSS is not the assertion here. A control run with the same writes and no stopped peer grows by
    about the same amount: that growth is the writer's own pending rows and columnar buffers, and the
    peer buffer contributes a fraction of a megabyte before the kernel buffers saturate.
    """
    block_started = time.strftime("%H:%M:%S")
    base_rss = rss_mb(writer.proc.pid)
    print(f"stopping node{victim.index} so it cannot read; writer RSS {base_rss:.1f} MB")
    victim.proc.send_signal(signal.SIGSTOP)

    drops = 0
    try:
        with socket.create_connection(("127.0.0.1", writer.tcp), timeout=60) as sock:
            sock.settimeout(60)
            sock.recv(4096)
            for batch in range(10):
                block = ""
                for k in range(40):
                    start = 900_000 + batch * 40_000 + k * 1000
                    block += "MINSERT SLOWPEER EX bid 1000\n" + "".join(
                        f"{start + i} 7 1\n" for i in range(1000))
                sock.sendall(block.encode())
                time.sleep(1.0)
                try:
                    sock.recv(1 << 20)      # drain replies so the session buffer stays small
                except socket.timeout:
                    pass
                drops = count_drops(writer, block_started)
                print(f"  after {(batch + 1) * 40}k levels: peer dropped {drops} time(s), "
                      f"writer RSS {rss_mb(writer.proc.pid):.1f} MB", flush=True)
                if drops:
                    break
    finally:
        victim.proc.send_signal(signal.SIGCONT)
        print(f"node{victim.index} resumed")

    # The rows still have to arrive, by reconnect and catch-up.
    deadline = time.time() + 120
    rows, target = 0, 0
    while time.time() < deadline:
        rows = len(prices(victim.tcp, "SLOWPEER"))
        target = len(prices(writer.tcp, "SLOWPEER"))
        if target and rows >= target:
            break
        time.sleep(3)

    print(f"peer dropped for not draining: {drops} time(s)")
    print(f"victim holds {rows} of the writer's {target} SLOWPEER rows after resuming")
    ok = drops > 0 and target > 0 and rows >= target
    if drops == 0:
        print("RESULT: NO DROP OBSERVED — the ceiling never tripped, so this proves nothing")
    elif rows < target:
        print("RESULT: DROPPED BUT DID NOT RECOVER — the peer never caught up after reconnecting")
    else:
        print("RESULT: OK")
    return 0 if ok else 1


def line_after(line: str, since: str) -> bool:
    """Is this JSON log line's timestamp at or after `since` (an ISO time-of-day)?"""
    marker = '"ts":"'
    at = line.find(marker)
    if at < 0:
        return False
    return line[at + len(marker) + 11:at + len(marker) + 19] >= since


def run_partition(nodes: list[Node], writer: Node, victim: Node) -> int:
    """Isolate the victim, write enough to make the sender discard its backlog, then let
    reconciliation close the difference.

    Blocking the link alone proves nothing: an iptables DROP does not reset the connection, so TCP
    retransmits everything once the rule is gone and the rows arrive with no help from anti-entropy
    at all. That version of this scenario reported success while a mutation that disabled
    reconciliation entirely still passed it.

    What cannot be undone by retransmission is backpressure: once the queued frames exceed
    max_catchup_bytes, MultiMasterManager clears the send buffer and sets needs_snapshot, which
    nothing acts on. Those records are then permanently absent from the peer, the connection is
    healthy, and no reconnect will ever happen — so the periodic vector exchange is the only thing
    left that can repair it. Hence the deliberately small ceiling and the write volume below.
    """
    expected = [700_000]
    mm_port = victim.ports[3]
    ports = victim_ports(victim.proc.pid, mm_port)
    block_started = time.strftime("%H:%M:%S")
    print(f"isolating node{victim.index} on mesh ports {ports}")
    block_link(ports)
    try:
        time.sleep(3)
        writes = int(os.environ.get("MMH_PARTITION_WRITES", "150"))
        with socket.create_connection(("127.0.0.1", writer.tcp), timeout=20) as sock:
            sock.settimeout(20)
            sock.recv(4096)
            payload = "".join(f"INSERT {SYMBOL} EX bid {730_000 + i} 2 1\n" for i in range(writes))
            sock.sendall(payload.encode())
            time.sleep(2)
            try:
                sock.recv(1 << 20)
            except socket.timeout:
                pass
        expected.extend(730_000 + i for i in range(writes))
        time.sleep(3)

        during = prices(victim.tcp)
        held_before_writes = len(expected) - writes
        print(f"during the partition node{victim.index} holds {len(during)} rows "
              f"(expected {held_before_writes} — the writes cannot reach it)")
        if len(during) != held_before_writes:
            print("  WARNING: the partition did not hold, so this run proves nothing about "
                  "reconciliation")
    finally:
        unblock_link(ports)
        print("link restored; the node was never restarted")

    runs_before = status_value(victim.tcp, "anti_entropy_runs")
    deadline = time.time() + 120
    got: list[int] = []
    while time.time() < deadline:
        got = prices(victim.tcp)
        if sorted(got) == sorted(expected):
            break
        time.sleep(2)

    runs_after = status_value(victim.tcp, "anti_entropy_runs")

    # Attribute the repair instead of assuming it. Two mechanisms could have done this: a
    # reconnect, whose handshake exchanges vectors, or the periodic reconciliation pass. An
    # iptables DROP does not reset the connection — TCP just retransmits — so a clean run shows
    # no handshake at all, and then only anti-entropy can have closed the gap. If a handshake
    # does appear, this run proves catch-up, not reconciliation, and says so.
    handshakes = 0
    log_path = os.path.join(ROOT, f"node{victim.index}.log")
    if os.path.isfile(log_path):
        with open(log_path, errors="replace") as fh:
            for line in fh:
                if "Handshake complete" in line and line_after(line, block_started):
                    handshakes += 1

    # Evidence that the gap was real: the writer discarded a backlog for this peer, so the
    # records were gone from the wire rather than waiting in a retransmission queue.
    discards = 0
    writer_log = os.path.join(ROOT, f"node{writer.index}.log")
    if os.path.isfile(writer_log):
        with open(writer_log, errors="replace") as fh:
            for line in fh:
                if "Backpressure" in line and line_after(line, block_started):
                    discards += 1

    ok = sorted(got) == sorted(expected) and len(got) == len(set(got))
    print(f"after reconciliation node{victim.index} holds {len(got)} rows, "
          f"{len(set(got))} distinct, expected {len(expected)}; "
          f"anti_entropy runs {runs_before} -> {runs_after}")
    print(f"  evidence: backpressure discards on the writer = {discards}, "
          f"handshakes on the victim after the block = {handshakes}")
    if discards == 0:
        print("  WARNING: nothing was discarded, so TCP retransmission alone could explain this "
              "run — it proves nothing about reconciliation")
    elif handshakes:
        print("  NOTE: a handshake happened, so reconnect-driven catch-up may have done the work")
    else:
        print("  attributed to reconciliation: the backlog was discarded, no reconnect happened, "
              "and the rows arrived anyway")
    if ok and discards == 0:
        print("RESULT: RECONVERGED, but by TCP retransmission — an iptables DROP does not reset "
              "the connection, so this says nothing about anti-entropy. A conclusive run needs a "
              "bounded send buffer (roadmap #69) so the backlog is actually discarded.")
    else:
        print("RESULT:", "OK" if ok else "STILL DIVERGED")
    return 0 if ok else 1


def main() -> int:
    if not os.path.isfile(SERVER):
        print(f"server binary not built: {SERVER}")
        return 2

    cycles = int(os.environ.get("MMH_CYCLES", "4"))
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT)

    etcd, etcd_url = start_etcd()
    nodes = [Node(i, etcd_url) for i in range(3)]
    failures = 0

    try:
        for node in nodes:
            node.start()
        time.sleep(6)  # let the mesh form

        command(nodes[0].tcp, f"INSERT {SYMBOL} EX bid 700000 1 1\n")
        time.sleep(3)
        expected = [700_000]
        print("after the first write:", [prices(n.tcp) for n in nodes])

        victim, writer = nodes[2], nodes[0]

        mode = os.environ.get("MMH_MODE")
        if mode == "partition":
            failures += run_partition(nodes, writer, victim)
            return 1 if failures else 0
        if mode == "slowpeer":
            failures += run_slow_peer(nodes, writer, victim)
            return 1 if failures else 0
        for cycle in range(cycles):
            victim.kill()
            time.sleep(2)

            base = 710_000 + cycle * 10_000
            for k in range(2):
                price = base + k * 1000
                command(writer.tcp, f"INSERT {SYMBOL} EX bid {price} 2 1\n", settle=0.3)
                expected.append(price)
            time.sleep(2)

            victim.start(reuse_ports=True)

            deadline = time.time() + 40
            got: list[int] = []
            while time.time() < deadline:
                got = prices(victim.tcp)
                if len(got) >= len(expected):
                    break
                time.sleep(1.5)

            missing = sorted(set(expected) - set(got))
            duplicates = len(got) - len(set(got))
            unexpected = sorted(set(got) - set(expected))
            problems = []
            if missing:
                problems.append(f"MISSING {missing}")
            if duplicates:
                problems.append(f"{duplicates} DUPLICATE rows")
            if unexpected:
                problems.append(f"UNEXPECTED {unexpected}")

            verdict = "OK" if not problems else " + ".join(problems)
            print(f"cycle {cycle}: node2 holds {len(got)} rows, {len(set(got))} distinct, "
                  f"expected {len(expected)} — {verdict}")
            if problems:
                failures += 1
                writer_rows = prices(writer.tcp)
                print(f"  writer holds {len(writer_rows)} rows, "
                      f"{len(set(writer_rows))} distinct")
                break
    except Exception as exc:  # noqa: BLE001 - a hang is the interesting case
        print("HANG or ERROR:", repr(exc)[:160])
        dump_stacks(nodes)
        failures += 1
    finally:
        for node in nodes:
            node.stop()
        etcd.terminate()
        try:
            etcd.wait(timeout=10)
        except subprocess.TimeoutExpired:
            etcd.kill()

    print(f"\nlogs: {ROOT}/node*.log")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
