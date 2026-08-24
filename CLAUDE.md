# orderbook-dbengine — working notes for AI assistants

Context for anyone (human or AI) working on this repository. Read this before touching the code.

## What this is

A C++20 database engine specialised for Level 2 orderbook data in HFT environments. ~1.35M
updates/sec and ~2.8µs p50 update latency on the reference machine, from WAL group commit, SoA
buffers with seqlock concurrency, and columnar storage with delta+zigzag+Simple8b compression.

**The engine runs natively on the host. There is no containerised deployment path and there will not
be one** — a container layer between the engine and the hardware defeats the point of an engine
tuned for specific hardware. This applies to the test harness too: etcd runs as a native process.

## Non-negotiable rules

1. **`ctest -j1`. Always sequential.** Network tests (replication, failover, multi-master) bind fixed
   ports and fail under parallel execution. This is a correctness requirement, not a preference.
2. **Benchmarks only mean anything in a Release build.** Debug is 3-4x slower; that is not a
   regression. And never quote a number without stating the hardware it came from.
3. **Every new function gets logging.** No exceptions. See "Logging" below.
4. **Build must be warning-free.** `-Wall -Wextra -Werror` is on, so a warning is a build failure.
5. **When debugging: add logs first, analyse second.** Guessing wastes time on this codebase.

## Build and test

```bash
# Debug (development default)
cmake -S . -B build
cmake --build build -j$(nproc)

# Release (benchmarks, production)
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j$(nproc)

# Tests — 673 of them, ~2.7 minutes
ctest --test-dir build --output-on-failure -j1
```

First configure pulls googletest, google/benchmark, rapidcheck and nlohmann/json via `FetchContent`,
which needs network access and a few minutes.

System dependencies: `liblz4-dev`, `libcurl4-openssl-dev`, `liburing-dev`.

Tests that touch coordination need a native `etcd` on PATH (or `OB_ETCD_BINARY`). Installation
instructions are in [tests/integration/README.md](tests/integration/README.md) and
[docs/cli.md](docs/cli.md).

Property tests use RapidCheck with `RC_PARAMS=max_success=100` for multi-master networking and `=25`
elsewhere, set in `tests/CMakeLists.txt`.

## Architecture

`Engine` (`src/engine.cpp`) is a facade delegating to WAL, SoA buffer, columnar store, replication and
multi-master. Full component map: [docs/architecture.md](docs/architecture.md). Wire protocol:
[docs/cli.md](docs/cli.md).

The largest and most intricate component is `MultiMasterManager` (`src/multi_master.cpp`, ~1000
lines): a unified epoll io_loop handling accept, recv and send, plus reconnect with exponential
backoff, catch-up streaming from a peer's WAL position, and backpressure that falls back to snapshot
sync above 512MB.

Peer discovery pipeline, worth memorising because breaking any link fails silently:
`etcd → PeerRegistry::start_watch() → handle_topology_change() → connect_to_peer() → send_handshake()`

## Coding conventions

- C++20, namespace `ob::`, headers in `include/orderbook/`, sources in `src/`, tests in `tests/`
- Code and comments in English
- One test file per component; property-based tests where the invariant is worth stating
- GCC quirk: `auto wr = ::write(...); (void)wr;` — not `(void)::write(...)`
- Never name a local `rc`: it shadows the RapidCheck namespace and the resulting error is unreadable
- Aggregate initialisation: list every field (`-Wmissing-field-initializers`)
- Value-init `Type var{}` rather than `memset`
- Storage is append-only; nothing deletes rows except TTL retention

## Logging

Default level is DEBUG. Do not economise on logs.

| Level | Use for |
|-------|---------|
| `OB_LOG_INFO` | lifecycle: start/stop, connections, role transitions, handshake, catch-up |
| `OB_LOG_DEBUG` | detail: parsing, processing, internal state |
| `OB_LOG_WARN` | timeouts, retries, backpressure, unexpected states |
| `OB_LOG_ERROR` | data loss, corruption, protocol errors |

The component string must be specific (`"mm"`, `"engine"`, `"repl_client"`, `"failover"`,
`"peer_registry"`, `"shard_router"`, `"tcp_server"`), and every line must carry context: fd, node_id,
peer_id, epoch, addresses, sizes. Rule of thumb: if it can go wrong, it must be logged.

## Known pitfalls

Learned the hard way. Check here before debugging.

1. **Port conflicts in tests** — always `ctest -j1`.
2. **AF_INET vs AF_INET6** — `socket()` must match the `sockaddr` struct in use (we use AF_INET).
3. **`read_only_flag_` in multi-master mode** — must be reset after `FailoverManager` initialisation,
   or the node rejects writes it should accept.
4. **`peer_registry_` wiring order** — init in the constructor, `register_self()` + `start_watch()` in
   `start()`, `deregister()` in `stop()`. Miss one and peer discovery fails silently.
5. **EPOLLOUT busy-loop** — arm EPOLLOUT only after `send()` returns EAGAIN, disarm when `send_buf`
   empties. Otherwise epoll spins and burns a core.
6. **`parse_frames` offsets** point into the buffer *before* erasure. Snapshot them if you need them
   afterwards.
7. **`WALRecordV2.payload_len`** must equal `frame_len - 38`. A mismatch means desync or corruption:
   disconnect the peer.
8. **LZ4 and Nagle** — small compressed frames need `TCP_NODELAY`, or INSERT latency multiplies.
9. **`FLUSH` is `flush_incremental()`**, not close+open. Getting this wrong costs 82ms instead of 2ms.
10. **Lock order in `Engine` is `flush_mtx_` → `mtx_` → `ColumnarStore::index_mtx_`.** Never the
    reverse. `flush_mtx_` serialises everything that writes segments or mutates `stores_`; `mtx_` is
    released across segment I/O so writers are not blocked. Two unsynchronised flushes each wrote the
    same segment and each merged its meta, so `SELECT` returned every row in it twice.
    `demote_to_replica()` must not hold `flush_mtx_` across `repl_mgr_->stop()`: a replication thread
    can be inside `create_snapshot()` waiting for it.
11. **`EAGAIN` means "come back later", never "the client is gone".** `Session` queues response bytes
    in `send_buf_` and the epoll loop arms `EPOLLOUT` after a partial write; treating a full socket
    buffer as failure truncated every response above ~2 MB. Socket writes use
    `::send(..., MSG_NOSIGNAL)`: with plain `::write()`, a client disconnecting mid-response raised
    SIGPIPE and killed the whole process. Queued output is capped at 64 MB per session.
12. **An aggregate result is not a row.** `QueryResult::agg_values` needs `format_agg_response()`;
    passing it to `format_query_response()` is what made every aggregate query answer a network client
    with a row of zeros. Each value carries its own `scale` (10⁶ for VWAP and MID_PRICE, 10⁹ for
    IMBALANCE) and an `empty` flag that must reach the wire as `NULL`, never `0`.
13. **`ColumnarStore::flush_segment()` returns a `SegmentMeta` that must be merged.** `QueryEngine`
    reads `combined_store_` only, never the live SoA buffer, so a dropped meta means rows sit on disk
    invisible to every query until the next `open_existing()`. Same for the metas parked by an
    `append()` rollover — collect them with `take_rolled_segments()`. The same asymmetry bites WAL
    recovery from the other side: replaying records into the SoA buffer recovers nothing a `SELECT`
    can see, so `open()` flushes immediately after a non-empty replay.
14. **A test that ends in `close()` does not test crash recovery.** `close()` drains and flushes, so
    the rows come back from the columnar store and replay is never the thing under test. This is why
    585 passing tests missed `Engine::open()` replaying into a callback that discarded every record —
    acknowledged writes were lost on every crash, always (roadmap #62). Abandon the engine instead:
    `Engine::release()` in C++, a real `SIGKILL` in Python, plus an assertion that no segment existed
    at the moment of the kill.
15. **A field nobody fills in disables the mechanism that reads it, and looks like a working
    feature while doing so.** `tcp_server.cpp` set `sequence_number = 0` and said the engine
    assigned it; the engine copied the zero through. Gap detection tests
    `prev_seq != 0 && seq != prev_seq + 1`, so it never fired; `append_gap()` was dead code with
    a passing unit test; the `sequence_number` column in every segment was zeros (roadmap #64).
    The sentinel that was meant to mean "no history yet" ended up meaning "never check".
16. **Sequence numbers belong to the origin, not to the node holding the record.** `0` means
    unassigned and `Engine::stamp_sequence()` fills it in; a non-zero number passes through
    untouched. The discriminator has to be the value rather than the caller, because the replica
    path (`replication.cpp`) shares `apply_delta()` with client writes and must keep the
    primary's numbering. Gap detection is per origin in `SequenceTracker` — one counter per
    buffer reports every multi-master interleave as a hole.
17. **A "how far did I get" cursor must live in the sender's space, not the receiver's.**
    Multi-master catch-up compared the peer's WAL byte offset with the local one and streamed from
    that offset in the *local* log. Every node writes its own records plus copies of foreign ones, so
    the same data yields different offsets: 846 at the peer against 870 locally read as "behind by 24
    bytes" and shipped one empty checkpoint while the missing rows sat earlier (roadmap #61). A
    sequence number minted by an origin means the same thing on every node that received it.
18. **"Highest seen" is not "I have everything up to".** A peer can receive live record 7 before
    catch-up delivers 6, and a maximum would report 6 as delivered. The state is a contiguous
    frontier: a record above it is applied, but the frontier stays put.
19. **Over-delivery is not free, and Last-Writer-Wins does not make it so.** Storage is append-only,
    so re-applying a record appends its rows again: four outage cycles stored 25 rows where 9 were
    written. LWW does refuse the repeat, but its HLC state is in memory and does not survive a
    restart, and the columnar store's duplicate-path refusal only hides it while the re-flushed
    segment covers the same timestamp range. Dedup belongs on the sequence number, before the WAL
    append (`SequenceTracker::has_seen`).
20. **`std::mutex` is not recursive, so a helper that locks cannot be called from a section that
    already holds that lock.** `persist_version_vector_if_changed()` took `mtx_` and was called from
    inside the flush block that held it: the flush thread deadlocked against itself and every client
    write queued behind it. The symptom looked like an ABBA cycle between the engine and
    multi-master. `sudo gdb -p <pid> -batch -ex "thread apply all bt"` settled it in two minutes;
    without sudo, `ptrace_scope` blocks the attach.
21. **Two covered paths are not a covered crossing.** `Engine::stats()` dereferenced a null
    `unique_ptr` on every multi-master node, so `STATUS` and every `/metrics` scrape killed the
    process (roadmap #68). The multi-master tests never send `STATUS`; the metrics tests never
    enable multi-master. 640 unit and 117 integration tests missed it by one combination, and
    nothing was missing from the list of things to test — only from the list of pairs.
22. **Never hand out a reference to an optional component.** `anti_entropy()` was
    `return *anti_entropy_;` while `stop()` checked the same pointer for null, and nothing
    constructed it. A pointer return makes the caller face the question; a reference makes the
    bad call look correct. If a member can legitimately be absent, its accessor must say so in
    its type.
23. **A metric that reports zero must distinguish "nothing happened" from "nobody ran".**
    `ob_mm_anti_entropy_runs_total` sat at zero because the scheduler was never constructed, and
    the roadmap read that as "runs fine, only reconciliation is missing" for months. Where a
    counter can be zero for two reasons, report the second one separately.
24. **An `iptables DROP` does not reset a TCP connection, so a partition test proves nothing
    about any repair mechanism.** "Cut the node off, write elsewhere, restore the link" looks like
    a test of anti-entropy and is actually a test of retransmission: the frames sit in the
    sender's buffer and arrive when the rule goes away. A mutation disabling reconciliation
    entirely still passed that scenario. For such a test to decide anything, the divergence has to
    be one TCP cannot undo — a record the sender discarded, or one the receiver refused.
25. **Clearing a partially-sent buffer corrupts the peer's framing.** `try_drain_send_buf()`
    erases the sent prefix after a partial write, so `peer.send_buf` can begin mid-frame.
    `check_backpressure()` used to `clear()` it and keep the socket, which left the peer waiting
    for the rest of a frame nobody would send and reading the next frames as its tail. Dropping
    the connection is the only answer that does not lie about the stream; reconnect and catch-up
    then repair it (roadmap #69).
26. **RSS is not a measurement of the thing you changed.** A partitioned peer looked like it grew
    the writer by 17.8 MB per 120k levels — until a control run with the same writes and no
    partition grew by 17.4 MB. The growth was the writer's own pending rows and columnar buffers;
    the peer buffer contributed 0.2 MB, because the kernel socket buffer absorbs the first few
    megabytes. Measure the thing (`ob_mm_peer_send_buf_bytes`), not a proxy that moves for a dozen
    other reasons.
27. **A parser that ignores what it does not understand hides operator mistakes.**
    `parse_cli_args()` accepted `--prot 5599` and `--port` with no value by silently skipping
    both, and cast `--port 99999` down to 34463. It also had no tests, which is how that survived
    (roadmap #36). Unknown flag, missing value, non-numeric value and out-of-range value are all
    errors now. If a config parser can be wrong in silence, it will be.
28. **When a role moves, every component that answers questions about it has to be told.** The
    graceful handover moved `FailoverManager`'s own role and called `demote_to_replica()` only if
    the coordinator already showed a new leader in that instant — which it never does, because the
    target has to notice the empty leader key first. So the outgoing node answered `ROLE` with
    `PRIMARY` and kept accepting writes after giving the role away (roadmap #60). The same branch
    recorded a new leader's address and told the engine nothing, so a replica kept replicating from
    whoever was primary when its client started. Both were invisible while `FAILOVER <target>`
    could not work at all: fixing the first link in a chain is what exposes the rest.
29. **The checkpoint goes after the flush, never before.** A `CHECKPOINT` record claiming more than
    is durable turns a crash into data loss; claiming less costs a replay that gets skipped anyway.
    For the crash window between writing the segment files and appending the checkpoint,
    `replay_wal_tail()` skips records at or below the highest `end_ts_ns` already on disk — without
    that, replay rewrites a durable segment from a WAL tail that may hold fewer rows than the segment
    does, because truncation only follows the replica-confirmed position.
30. **A state machine with a branch missing is a state machine with a trap.** `monitor_loop()`
    handled `PRIMARY` and `REPLICA`; a node at `STANDALONE` matched neither and sat there for the rest
    of its life — no lease, no leader poll, no campaign, no replication. Losing a race is not a role:
    every path that declines to promote must say what the node *is* instead. Enumerate the roles and
    check each one has an active branch, rather than trusting that the interesting ones do.
31. **A workaround in the test harness is a bug report nobody filed.** The integration fixture started
    nodes sequentially and its docstring said why: "This avoids a race condition where both nodes start
    simultaneously and one fails to transition from STANDALONE." The defect was known, described
    precisely, and worked around in the harness for months. When a fixture explains that it avoids a
    scenario, that scenario is a filed bug — go read the engine, not around it.
32. **A retry loop exposes leaks that one-shot code hides.** `connect()` called `curl_easy_init()`
    unconditionally and stored the result over the previous handle. Called once at startup it looked
    fine; called once a second while a coordinator was down it leaked a handle per attempt. Before
    turning a one-shot call into a retried one, read it as if it runs a thousand times.
33. **A control that is not built like the subject measures the harness.** Timing the seventh column
    of a query response against a six-column copy looked simple until the copy was moved out of
    `main`'s translation unit, where it had been inlinable while the real formatter — in
    `response_formatter.cpp` — was not. That move changed the *control* by 28 ns, most of the 41 ns
    effect under test. Build the control the way the subject is built: same translation unit shape,
    same linkage, same optimisation opportunities.
34. **An HTTP 200 is not a success — find the field that proves it.** etcd answers a keepalive for a
    lease it has forgotten with **200**, the same envelope as a live one, and the lease id echoed
    back; the only difference is that `TTL` is missing. `refresh_lease()` tested `!resp.empty()`, so
    it could not fail, and the lease fenced nothing. When a call's answer decides whether this node
    still owns a role, parse the field that carries the answer, and make the failing case a log line.
35. **Ask the server what it answers; do not infer it from the client code.** Both halves of #74 came
    out of ten lines of Python against a scratch etcd: keepalive on a revoked lease returns 200
    without `TTL`, and `put` under an unknown lease returns **404** with a JSON error body that
    `http_post()` handed back as though it were a result. Neither is visible by reading our own code,
    and neither is documented where you would look.

## Current state and open problems

Roadmap phases 1-6 are complete; 7-11 are planned in [docs/roadmap.md](docs/roadmap.md). Item numbers
below refer to that file. **Those numbers are permanent ids — never renumber them.** A new item takes
the next free number wherever it sits on the page; `scripts/check_roadmap.py` (run in CI) checks ids,
references and ranges. The rule exists because three renumbering passes each broke something, and
because commit messages and specs cite these numbers.

**Where the suites stand:** 681 C++ tests (`ctest -j1`, ~170 s) and 124 integration tests plus 2
opt-in skips (`pytest tests/integration/`, ~3.8 min), all green, and **no `xfail` left** — the two
that marked known defects are gone because both defects are fixed.

Things a newcomer should know, because they are real limits rather than bugs to file again:

- **The wire protocol has no authentication or encryption.** Roadmap #30. Do not expose a node
  outside a trusted network.
- **Deference on election cannot tell a further replica from a dead one.** Positions carry no lease
  and no timestamp, so a candidate waits out a bounded window (`--election-deference-ms`) instead of
  knowing. Roadmap #72 has the fix and the reasoning for it.
- **A node that joins an origin's stream mid-way never establishes a contiguous frontier.** It can
  catch up, but it cannot prove it has everything. Roadmap #67.
- `rapidcheck` is pinned to `master` rather than a commit SHA, unlike every other dependency.

## Before you call a change done

1. Build clean, no warnings
2. `ctest -j1` green
3. New behaviour covered by a test; new server functionality also covered in `tests/integration/`
4. Logging added
5. Hot-path changes (WAL, SoA, columnar, codec, aggregation, query engine, engine facade): run
   `bench_engine` in Release and compare against the previous run **on the same machine**
6. Conventional commit message, in English
