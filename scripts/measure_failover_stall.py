"""How long does a FAILOVER command freeze the outgoing primary's client port?

The server half of roadmap #86 asked whether a node closes a client session while stepping down, so
that an operator cannot tell success from a refusal. Reading the server says it does not: nothing on
the failover path touches a session or the listener, and `draining_` - the only thing that closes
`listen_fd_` - has one writer, reachable only from the signal handler.

What is true is that the whole handover runs inside `execute_command` on the epoll thread: three
coordinator round-trips, `repl_mgr_->stop()` joining threads, and `demote_to_replica()` wiping every
columnar segment directory on disk. So the question worth measuring is not whether one session is
closed but what happens to all the others, and that is what this script measures: one connection
issues FAILOVER while another PINGs and records the round-trip.

Measured on i3-7100U with etcd on loopback, Debug build:

    columnar files   FAILOVER answered in   worst concurrent PING   PING baseline p50
    280              69.3 ms                19.4 ms                 0.053 ms
    2800             75.1 ms                73.2 ms                 0.057 ms

So the stall is the command's own duration and is dominated by the coordinator round-trips rather
than by the local wipe - ten times the segment files cost 6 ms more. Tens of milliseconds is a cost
to write down, not a reason to restructure the io loop the way #79 did for snapshot creation, where
the figure was 1.7 s and grew with the store. Re-run it before believing that: a coordinator that is
not on loopback moves the first column.

    OB_INTEGRATION_TESTS=1 .venv/bin/python scripts/measure_failover_stall.py
"""
import os, socket, statistics, sys, time
from pathlib import Path

REPO = "/home/km/projects/smart-data-engine/low-cost-and-low-latency-orderbook-dbengine"
sys.path.insert(0, os.path.join(REPO, "tests", "integration"))
sys.path.insert(0, os.path.join(REPO, "python"))
os.environ["OB_INTEGRATION_TESTS"] = "1"

import conftest
from orderbook_engine import OrderbookEngine

cm = conftest.ClusterManager()
try:
    cm.start()
    primary = cm.primary()
    replica = cm.replica()
    print(f"primary={primary.node_id} replica={replica.node_id}")

    # Enough data that the demotion's segment wipe has something to delete.
    with OrderbookEngine(host="127.0.0.1", port=primary.tcp_port, timeout=30) as eng:
        for i in range(400):
            eng.insert(f"SYM{i:03d}", "EX", "bid", list(range(100, 300)), [5] * 200)
        eng.flush()
    segs = sum(1 for _ in Path(primary.data_dir).rglob("*.col"))
    print(f"columnar files on the primary: {segs}")

    # The prober: PING on its own connection, every 50 ms, recording round-trip time.
    probe = socket.create_connection(("127.0.0.1", primary.tcp_port), timeout=60)
    probe.settimeout(60)
    probe.recv(4096)  # banner

    def ping() -> float:
        t0 = time.perf_counter()
        probe.sendall(b"PING\n")
        probe.recv(1024)
        return (time.perf_counter() - t0) * 1000.0

    warmup = [ping() for _ in range(20)]
    print(f"PING before: p50={statistics.median(warmup):.3f} ms  max={max(warmup):.3f} ms")

    # The command, on its own connection, from a thread so the prober keeps going.
    import threading
    reply = {}
    def do_failover():
        s = socket.create_connection(("127.0.0.1", primary.tcp_port), timeout=120)
        s.settimeout(120)
        s.recv(4096)
        t0 = time.perf_counter()
        s.sendall(f"FAILOVER {replica.node_id}\n".encode())
        try:
            reply["text"] = s.recv(4096).decode(errors="replace").strip()
        except Exception as exc:
            reply["text"] = f"<{exc!r}>"
        reply["ms"] = (time.perf_counter() - t0) * 1000.0
        s.close()

    t = threading.Thread(target=do_failover)
    t.start()
    during = []
    while t.is_alive():
        try:
            during.append(ping())
        except Exception as exc:
            during.append(float("inf"))
            print(f"probe failed: {exc!r}")
            break
        time.sleep(0.002)
    t.join()

    print(f"FAILOVER answered in {reply.get('ms', -1):.1f} ms: {reply.get('text')!r}")
    if during:
        print(f"PING during: n={len(during)} p50={statistics.median(during):.3f} ms "
              f"max={max(during):.1f} ms")
        stalled = [x for x in during if x > 100]
        print(f"probes over 100 ms: {len(stalled)}  worst={max(during):.1f} ms")
    probe.close()
finally:
    cm.shutdown()
