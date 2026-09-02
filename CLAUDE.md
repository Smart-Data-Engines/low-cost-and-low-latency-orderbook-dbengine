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

# Tests — 744 of them, ~2 minutes
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

The default level is **INFO** (`TcpServerConfig::log_level{"INFO"}`); `--log-level DEBUG`
raises it. Do not economise on logs — but read the default from the header rather than from a
document about it: this line said DEBUG for months, and it is what made a log-volume estimate
wrong by an order of magnitude while chasing pitfall 91.

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
36. **Read the log a passing test prints.** Every run of the dedup tests printed `Write to
    unregistered counter 'ob_mm_duplicates_dropped'` — the registry saying, on every run, that the
    metric was discarded and `/metrics` would report a flat zero. Nothing failed, so nobody read it. A
    sweep found a second one. `scripts/check_metrics.py` now fails CI for the class.
37. **A mutation that survives means the test is measuring something else.** Persisting the held
    sequence set was verified by a test that passed with the persistence disabled: the re-flushed
    segment landed on the same directory path, `ColumnarStore` refused the merge as a duplicate, and
    the row count came out right for a reason unrelated to dedup. The neighbouring test warned about
    exactly this. Always disable the fix and watch the test fail before believing it.
38. **Prefer a fact you recorded to an inference from data you kept for another purpose.**
    Recovery decided "is this record already stored?" by comparing its timestamp against a segment's
    `end_ts_ns` — a field kept for time-range pruning, which is the *last* row's timestamp, not the
    highest. That made the guard exact only while a symbol's timestamps increase, which one node
    guarantees and multi-master does not. Segments now record the WAL position their rows came from,
    and the question is answered by comparison rather than inference (#63).
39. **A number written by another node is not a number about you.** Snapshot transfer and shard
    migration ship whole segment directories, `meta.json` included, so a received segment carries the
    sender's WAL position. Trusting it would skip records this node never stored. Any position,
    offset or counter that can arrive from elsewhere needs to say whose it is — `wal_identity` here,
    kept outside every segment directory so it cannot travel with one.
40. **A test whose setup the system cannot produce will fight the correct fix.** The crash-window test
    built its state by re-appending copies of durable records, putting them at WAL positions above the
    segment holding them — impossible in the engine, where a record is written before the row it
    produces is stored. When the guard became a position comparison, that test failed and the code was
    right. Build the state the mechanism actually leaves behind.
41. **Closing a descriptor does not wake a thread blocked on it.** `stop()` closed the epoll
    descriptor "to unblock threads"; Linux does not wake `epoll_wait()` on close, so shutdown really
    waited out the 500 ms timeout while the loop could call `epoll_wait()` on a number the kernel had
    already reassigned. Wake the loop through something it is watching — an `eventfd` in the epoll
    set — join the thread, and only then close what it was using.
42. **Packing a struct for the wire makes every in-memory use of it misaligned.** `HLCTimestamp` was
    `#pragma pack(1)` to match its 12-byte wire form, which put a `uint64_t` on a 4-byte boundary
    inside any struct holding it; binding a reference to that field is undefined behaviour, and UBSan
    said so. Serialisation was already field-by-field at fixed offsets, so the packing bought nothing.
    Keep the CPU layout natural and let the serialiser own the wire layout.

43. **An index into a list means nothing unless both sides order the list the same way.** A snapshot
    chunk names its file by index into the manifest, and `SnapshotManifest::to_json()` sorts entries
    by path for deterministic output — so index 0 on the sender was a different file from index 0 on
    the receiver, and the first chunk was refused for exceeding a size that belonged to another file.
    Serialisation that normalises order turns an index into a different identifier on the other side.
    The first end-to-end test caught it on its first run, which is the argument for writing that test
    before believing the feature.
44. **A width in a header is a limit on every producer, whether or not they check it.**
    `payload_len` is a `uint16_t`, and two WAL appenders cast a `size_t` into it. `write_record()`
    writes the bytes it is handed, so a payload over 65535 produced a record claiming to be shorter
    than it is — and replay then read the middle of that payload as the next header, making the WAL
    tail unreadable from there. Same field on the wire, different symptom: the peer compares it with
    the frame, disagrees, and disconnects for ever. Reachable at 1561 (symbol, origin) pairs (#78).
45. **A guard duplicated for defence in depth cannot be mutation-tested on its own.** Disabling the
    `SNAPSHOT_END` completeness check left the test passing, because the install pre-flight caught the
    same thing. That is not a useless test and not a useless guard — it means the *test* has to assert
    the invariant, not the branch. It now checks that an incomplete transfer leaves no `.col` file in
    the data directory at all, and disabling both guards together fails it.
46. **An in-memory "am I empty" check says nothing about the directory.** `holds_no_data()` reads the
    sequence tracker and the store index, so a half-installed snapshot — files renamed into place,
    nothing loaded — reads as "clean". Any assertion about what an aborted operation left behind has
    to look at the filesystem.
47. **A blocking socket turns `try_drain_send_buf()` into a deadlock in a single-threaded test.** Real
    peer sockets are non-blocking, so EAGAIN arms EPOLLOUT and the call returns; a `socketpair()`
    without `O_NONBLOCK` blocks inside `send()` waiting for a reader that only runs after the call
    returns. While diagnosing it: `sent == 0` fell through both branches of that loop and spun
    silently — now treated as "come back later".

48. **Two mutexes taken in two orders by two threads is a deadlock, and the only reliable way to
    find it is to run the real thing under ThreadSanitizer.** The client write path held
    `Engine::mtx_` across `broadcast_local()`, which takes `MultiMasterManager::mtx_`; the io loop
    held `MM::mtx_` across `apply_remote_delta()`, which takes `Engine::mtx_`. Both are ordinary
    operations on every multi-master node. Thirteen seconds of the integration suite against a TSan
    build reported the cycle on all three nodes; the unit suite under the same sanitizer had been
    green for weeks, because no unit test starts a server with real clients and real peers. Fix the
    smaller side: `stats()` and `apply_delta_mm()` now gather what they need without the lock, so
    `MM::mtx_ → Engine::mtx_` is the only order left (#80).
49. **"Close the descriptor to unblock the loop" is wrong every time it is written.** It was wrong
    in `MultiMasterManager::stop()` (pitfall 41) and it was wrong twice more:
    `TcpServer::shutdown()` closed `listen_fd_` from the signal thread while `run()` was reading and
    closing the same field, and `MetricsServer::stop()` closed its listen socket before joining its
    own thread. Both loops already had timeouts and already re-checked their flags, so both needed
    nothing but the flag. The rule: the thread that owns a descriptor closes it; every other thread
    raises a flag.
50. **A comment explaining why something is not tested is a hypothesis, and it can be wrong.** The
    sanitizer job carried a note saying the integration suite under instrumentation would "multiply
    the runtime without adding coverage of anything the unit tests do not reach". It found a
    lock-order inversion and seventeen data races in thirteen seconds. When a comment justifies a gap
    in coverage, it deserves the same scepticism as a claim in code.

51. **A checksum can be a hot-path cost, and a flat MB/s figure is the tell.** CRC32C ran at 295 MB/s
    at every size, because it was a table walk one byte per iteration on a CPU with a `crc32`
    instruction for exactly this polynomial. 361 ns per 112-byte WAL record; the instruction does it in
    24. Measured end to end: +17.6% ingestion throughput (#81). Two things made it safe to swap:
    runtime detection with the old code as the fallback, so no build-time assumption about the CPU,
    and a test that compares both paths at every length from 0 to 300 and every alignment — because
    these checksums are written into WAL headers and replication frames, so a build that computed them
    differently would reject its own files.
52. **A benchmark whose input never changes measures nothing, and the number will be absurd enough to
    notice only if you are lucky.** The first CRC32C measurement reported 82 TB/s and a 3.2× speedup:
    the buffer was loop-invariant and the function pure, so the compiler hoisted both. Mutate the
    input per iteration, feed the result forward, and keep an `asm volatile` barrier in the loop. A
    smaller error in the same direction would have looked like a result.

53. **A convenience function in `<filesystem>` can touch the filesystem once per path component.**
    `fs::relative(path, base)` costs **~21 µs per call** in libstdc++, because it goes through
    `weakly_canonical()`, which resolves every component of both arguments against the disk. Called
    once per file while building a snapshot manifest that was about half of the whole operation —
    3.9 ms of 8.2 ms across 184 files. Stripping the base prefix produces the same string inside the
    noise of the bare directory walk. Three hypotheses about where that time went were wrong before
    this one was measured, which is the real lesson: profile the loop, do not reason about it (#79).

54. **A test that asserts an ordering between two independent timers asserts a coincidence.**
    `test_the_survivor_does_not_wait_for_a_dead_nodes_position` required a killed node's position key
    to be gone *by the time* the survivor was promoted. The leader lease and the position lease share
    a TTL and have independent refresh phases, so which expires first depends on which was refreshed
    more recently before the kill. It passed for weeks on one machine and failed at 10.2 s on a
    slower one, with the mechanism working exactly as designed. Assert the property the mechanism
    provides — the key *does* disappear within TTL plus a margin — not the race you happened to win.
55. **A slow runner is a fuzzer for orderings.** Two failover tests failed the first time the suite
    ran in CI, and only one was the test's fault: the other reproduced a real window in which two
    nodes both hold the role and both accept writes, because a revoked lease is noticed on the
    holder's next refresh (`lease_ttl/3`) while a candidate can win the vacated key immediately
    (#82). It needs the unlucky poll order, which an idle laptop rarely produces. When a test fails
    only under load, decide which of the two is wrong before touching either.
56. **`xfail(strict=True)` is wrong for a defect whose reproduction is probabilistic.** The repo's
    habit is strict markers, and it is a good habit: a strict xfail that starts passing is a signal.
    But #82's window fails on a loaded runner and passes on an idle one, so strict would turn the
    idle case into a false failure. Non-strict is the honest statement there — and it is the only
    place in this suite where it is.
57. **A test that skips itself because the harness did not build its binary reports green.**
    `test_cpp_client.py` skips when `ob_integration_test` is absent, and the first version of the
    integration CI job built only `ob_tcp_server` — seven tests quietly did not run. Same failure
    mode as a check that runs and gates nothing. The job now fails if anything skips except the two
    opt-in Binance tests.

58. **A value computed and never read is the mirror image of a field nobody writes, and one
    compiler sees it while the other does not.** Clang's `-Wunused-but-set-variable` found two on its
    first build of this tree, neither of which GCC reports: `base64_decode()` counted padding
    characters into a variable nothing used, under a comment claiming the padding was stripped; and
    `handle_catchup_request()` advanced a `file_offset` counter that nothing read. Both were harmless,
    and both were a mechanism that looks present and is not — which is pitfall 15 from the other
    side. Build with the other compiler occasionally; the README promised Clang support for months
    before anything checked it (#37).

59. **An answer that means four different things cannot be acted on.** `get_cluster_state()`
    returned `std::nullopt` for not-connected, an empty HTTP response, a key that genuinely was not
    there, and a body that would not parse. A primary reading that as "the leader key is gone" would
    step down on every transient etcd error, so it read it as "no information" — and therefore could
    not react to the key actually disappearing. Same conflation on the replica side made an
    unreachable coordinator look exactly like a vacant key, so replicas campaigned because a read
    failed. `read_leader()` answers `Present` / `Absent` / `Unavailable` and both branches can now
    say what they mean (#82). Same shape as pitfall 23 and #34: find the field that carries the
    answer, or add one.
60. **A demotion that depends on knowing the successor does not happen when there is no successor.**
    `handle_primary_lease_lost()` told the Engine to demote **only** if it could read a leader key
    with a non-empty address. After a revoke there is no key, so in the one case that matters the
    Engine was never told: it kept `node_role_ == PRIMARY`, `read_only_flag_` unset, and went on
    accepting writes indefinitely, while the FailoverManager privately believed it was a replica.
    Not knowing where to point the replication client is a reason to start no client; it is not a
    reason to keep claiming a role. Pitfall 28 in the path pitfall 28 was written about.
61. **"Exactly one primary" is true before a transition as well as after it.** A test that polls for
    it and breaks on the first sighting declares success while nothing has happened — which is how a
    rewritten assertion passed against a node that had not yet noticed it lost the role. Watch for
    the thing that *changes*: the holder's claim, or the epoch. And when a test breaks on a condition
    and then re-reads state to assert on it, the gap between the two reads is a race the fix can
    widen.

62. **`add_compile_options()` only affects targets created after the call, and CMake will not tell
    you.** `OB_ENABLE_ASAN`, `OB_ENABLE_TSAN` and `OB_ENABLE_COVERAGE` sat below all twenty-eight
    `add_library()` calls, so they instrumented `ob_tcp_server` and the tests and nothing else. Two
    required CI jobs and a coverage number all looked like they covered the tree and covered a sixth
    of it. What gave it away was a coverage report naming 6 of 34 source files — a number of the wrong
    order of magnitude — not the sanitizers, which kept passing. The proof is one grep of
    `flags.make`; do that after touching any global flag (#83).

63. **A mutex held across `join()` deadlocks whenever the thread being joined still needs that
    mutex.** `AsyncSnapshotBuilder::shutdown()` took the object's mutex and then joined the snapshot
    worker — and the worker's last act is to take the same mutex to publish its result. What makes
    this worth a pitfall rather than a bug is that `take_result()`, one function away, has the
    identical shape and is *safe*: it only joins once the result is published, so the mutex is
    already free. Two functions, same shape, one deadlock. The rule has to be blanket — move the
    thread object out under the lock, release it, then join — because the case-by-case version is
    correct reasoning that the next edit invalidates. The hang printed nothing at all, so
    `sudo gdb -p <pid> -batch -ex "thread apply all bt"` was the log (pitfall 20 again).
64. **Publish the result, then notify — and a test for that ordering is probably racing.** Reversing
    the two loses the wake-up: the owner looks, finds nothing, and no second notification is coming.
    The first test for it was worthless and looked fine: it woke a collector from a condition
    variable and raced it against the worker's very next line, which the worker won on every run, so
    swapping the two statements under test did not fail it once. Making the notification sleep after
    announcing itself makes the check decisive — and note which way the residual timing risk points,
    because the correct order then passes regardless of load and only a mutation can survive.
65. **A test for a race between two writers sees nothing if the write fits in one buffer.** The
    manifest race test passed against a completely unsynchronised `ofstream` on the target path,
    because a two-file manifest is a few hundred bytes: one `write()`, nothing to catch half-way.
    Thirty symbols made it tens of kilobytes and the same mutation failed immediately. The second
    half of the same fix: count an *empty* read as a failure once the file has been seen non-empty,
    since `trunc` empties the target before the replacement arrives and a manifest describing
    nothing is precisely the corruption at issue.

66. **`/metrics` names carry a label set, so a lookup by bare name finds nothing — and "absent"
    reads as "zero".** The exposition is
    `ob_mm_snapshot_sent_total{node_role="standalone"} 0`. A harness that split on whitespace and
    used the first field as the key got a map whose keys all ended in `{node_role="..."}`, so every
    lookup missed, every counter came back absent, and the script announced that the snapshot it was
    testing had never been sent. It had — the log said so. Strip from `{`, and make a scraper say
    "not found" rather than return 0.
67. **A `snprintf` into a fixed buffer can build clean at `-O0` and fail the sanitizer job.**
    `char name[8]` with `"SYM%02d"` passes a Debug build and fails Debug-plus-`-O1`, because
    `-Wformat-truncation` needs optimisation to run its value-range analysis and then cannot narrow
    a loop variable. Same shape as pitfall 58 with a different second toolchain: **a sanitizer job
    is a second set of compiler flags before it is a sanitizer.**

68. **A callback invoked under a lock it may itself need is a deadlock, and `shared_mutex` does not
    save you: it is not recursive.** The first cut of the subscription fix called `sub.cb()` under
    the *shared* lock, with a comment asserting that a callback cancelling its own subscription was
    safe "because it only marks the entry dead". Marking takes the *exclusive* lock, and a thread
    holding `std::shared_mutex` in any mode that asks again is undefined behaviour — including a
    second *shared* acquisition. The comment described the intent and not the code. Unconditional
    rule, exactly like pitfall 63 about `join()`: **collect under the lock, release it, then call.**
    Pointer validity comes from a separate in-flight counter that compaction refuses to run against.

69. **A test for a data race may not need a sanitizer — run it without one before you claim it
    does.** The concurrent `subscribe()`/`notify_subscribers()` test was written on the assumption
    that only TSan would show anything. It aborts on **every** run of a plain Debug build:
    `std::bad_function_call`, exit 134, because the notifier holds a reference into the vector,
    `push_back` relocates it, and the `std::function` it then invokes has been moved from. This
    matters beyond tidiness — a test that fails only under a sanitizer runs only in the jobs that
    build one, and this one gates ordinary `ctest`.

70. **A "how many are there" counter kept next to a collection drifts from it the first time the
    collection is tidied.** `has_subscribers()` reads an atomic so the no-subscriber path takes no
    lock. Incrementing on register and decrementing on cancel looks obvious and, with deferred
    removal, gives two sources of truth: marking dead and compacting both count the same event. It
    is recounted from the vector under the lock instead. **Write the safe direction into the code:**
    too high costs one pointless lock acquisition, too low drops a row — so it may only ever be too
    high.

71. **Sanitising a computation at the point of arrival leaves the poisoned value in the state, and
    the next caller trips over it.** #83 found `static_cast<int64_t>(new_physical) -
    static_cast<int64_t>(now)` overflowing in `HybridLogicalClock::update()` — reachable from the
    network, because `physical_ns` comes from a peer — and fixed it there. The identical expression
    in `tick_local()`, three lines away, was left alone and kept failing UBSan on the ASan job,
    because `update()` stores `max(now, last, remote)` into `last_`: the absurd value stays in the
    clock and the next local tick reads it back. **Fixing where the reproducer points is not the same
    as fixing the expression** — grep the expression, not the stack trace.

72. **A running maximum fed by two functions cannot isolate either of them.** `tick_receive()` and
    `tick_local()` fold their drift into one `max_drift_ns_`, and in the scenario that exposes the
    bug they compute the *same* distance — so whichever function is still correct supplies the
    expected value and masks the other. Two versions of the test survived reverting the fix before
    this was noticed. A behavioural assertion on that counter fails only when **both** sites lose the
    pattern; per-site protection comes from UBSan. **Establish what a test detects by reverting the
    fix, not by reading the test.**

73. **Name a test whose only detector is a sanitizer, instead of dressing it in an assertion that
    passes either way.** For the overflow in `tick_local()` no assertion can distinguish the two
    implementations — two's complement wrap lands on almost the correct magnitude — so the test says
    UBSan is the detector. The other half of the same rule: **compute the poison value, do not pick
    one that looks extreme.** `0xF000…` looks extreme and does *not* overflow (as int64 it is only
    −1.15e18); `0x9000…` does, because it is −8.07e18 against a floor of −9.22e18.

74. **A local array of a type with any default member initialiser is constructed unconditionally,
    and the declaration is not inside your `if`.** `SnapshotRow subscriber_rows[MAX_LEVELS]` looked
    free because it was only *filled* when something was subscribed. It is not: `SnapshotRow` carries
    `{}` on three padding members, so it is not trivially default constructible, and every
    `apply_delta` ran a thousand default constructors and touched 48 KB of stack. Measured on
    i3-7100U, Release, `BM_IngestionThroughput`: **2559 → 4511 ns/op, +76%, 6/6 interleaved rounds.**
    Ask `std::is_trivially_default_constructible` rather than reading the struct — those three `{}`s
    are on *padding*, which is exactly where nobody looks. And the general form: a benchmark run is
    what turned a change that read as free into a number.

75. **A comment justifying a gap in coverage is a hypothesis, and this is the third time.** The
    `sanitizers-integration (tsan)` job ran three multi-master modules, and the note explaining why
    said the modules that kill nodes were excluded "because their fixtures wait on timings that
    instrumentation makes unreliable". When #85 ran the whole battery under TSan, **all nineteen
    modules passed with zero reports**, the node-killing three included. The narrow scope had a
    price: none of those modules starts the failover monitor, so the WAL position race lived in
    `publish_position_if_due()` for months with a required check standing over it. #80 was this
    lesson about the job existing at all; this is the same lesson about its *scope*.

76. **Fix the pair, not the field: two loads of related state compose a value from two moments.**
    `current_file_index()` and `current_offset()` were separately correct and read together at five
    sites — including two snapshot manifests, which is where a joining peer is told to catch up
    from. Measured rate of an incoherent pair: **one in 150 million reads**, so no behavioural test
    will find it; the guard is a static test over `src/` that refuses the shape. And when the fix
    itself published `(N+1, previous offset)` as an intermediate state, the cross-thread test caught
    it at 96 in 4.3 million — **the reproduction rate of the reintroduced bug was six orders of
    magnitude higher than the original**, because a deliberate two-store sequence is a much wider
    window than a compiler-scheduled one.

77. **A test module that builds its own path to the artefact under test silently measures the wrong
    one.** Four integration modules had their own `os.path.join(REPO, "build", "ob_tcp_server")` and
    ignored `OB_SERVER_BINARY`. They start their own nodes instead of using `ClusterManager` —
    simultaneous starts, crash recovery, multi-master stats — so each grew the path and none grew the
    override. In CI that made three of them **skip** (14 tests) and the fourth crash. Locally it was
    worse: a stale `build/ob_tcp_server` was there to be found, so "this module is clean under TSan"
    got reported for runs in which **TSan was not present**. `test_mm_stats.py` is one of the three
    modules `sanitizers-integration (tsan)` had run since the job was created, so part of a required
    check had been measuring an uninstrumented binary from day one. One `server_binary_path()` in
    `conftest.py`, plus a static test that refuses a module building its own.

78. **A skip in a sanitizer job is a failure.** Fourteen tests reported as skips while the job stayed
    green and claimed to speak for the battery — a summary line makes a skip and a pass look the
    same, which the SDE repository already had a CI step for. The job now greps its own output and
    exits non-zero on any skip. The general rule: **a check whose scope can shrink silently is not a
    check**, and every mechanism that lets it shrink — a missing artefact, an unset variable, a
    hard-coded path — needs something that notices.

    And the guard itself got this wrong on its first run: it matched `[0-9]+ skipped`, the summary
    line reads `0 skipped`, so it failed the job with the battery green underneath. **A check that
    fires on the presence of a word rather than on a count is the same class of mistake as the thing
    it was added to catch.** `[1-9][0-9]* skipped`, and both cases exercised against a fixture line
    before pushing.

79. **A list you wrote yourself is not evidence about the code.** Adding config-file support needed
    to know which flags take no value, so I wrote the list — and put `failover-enabled` on it,
    because its default is true and I reasoned from the default. It takes a value:
    `--failover-enabled false` had always worked. On that false premise I added a
    `--no-failover-enabled` negation, a table mapping keys to negations, and a test asserting the
    negation was emitted. **The static test comparing the list against the parser's own branches
    deleted all three.** Derive the list from the source, and when the derivation disagrees with you,
    it is right.

    The same branch turned out to map anything unrecognised to *false*, so `--failover-enabled tru`
    silently disabled failover — pitfall 27 again, in a flag that had a value all along.

80. **One green run is not a measurement, and I used it as one.** Two failover tests failed on a PR
    and passed locally three times including under build load, so I pushed an empty commit off
    master to see whether master failed too. It passed — once — and I concluded the branch was at
    fault. It was not: a bisect branch carrying **only** the server change passed the plain
    integration job and failed the same test under ThreadSanitizer *on the same commit*. Four job
    executions across three branches, failing in three, is the measurement; the single baseline was
    the anecdote that pointed the wrong way. Design the experiment to distinguish, and if the answer
    rests on n=1, run it again before acting on it.
81. **A helper that returns the same value for two different events makes every failure
    undiagnosable.** `send_command()` slept 0.3 s and took one `recv`, so an orderly close and a
    reply that had not arrived yet both came back as `''`. The assertion could then only say "no
    OK", which is true of a server that refused, a server that closed, and a server that was still
    thinking — and `FAILOVER` legitimately takes seconds. Most of the diagnosis time went on
    re-deriving from CI logs what the helper had thrown away. **Where two outcomes need different
    responses, they need different return values**, and an exception is the cheapest way to stop a
    caller from conflating them by accident.

82. **An absolute install destination makes the archive generator write to the build host.** CPack's
    TGZ generator honours an absolute `DESTINATION` literally, so `install(FILES ... DESTINATION
    /etc/orderbook)` made `cpack` try to create `/etc/orderbook` on the machine doing the build. It
    failed here only for want of privileges; a build as root, or in a container, would have written
    into the host's `/etc` **while producing a package**. Relative destinations plus
    `CPACK_PACKAGING_INSTALL_PREFIX` give the .deb and the tarball identical layouts and touch
    nothing. The near miss before it is the same family: `${CMAKE_INSTALL_SYSCONFDIR}` is *relative*,
    so with the prefix at `/usr` the config went to `/usr/etc/...` while `conffiles` declared
    `/etc/...` — a conffile mark naming a path the package does not contain marks nothing, and the
    first upgrade reverts every local edit in silence.
83. **A CPack component does not filter anything unless component install is on.** The Python
    wheel's `install(TARGETS orderbook_shared DESTINATION orderbook_engine)` appeared inside the
    .deb at a path that means nothing on a system, because `CPACK_DEB_COMPONENT_INSTALL OFF` takes
    every rule and `CPACK_COMPONENTS_ALL` is then decoration. Guard the rule out of the build —
    `if(SKBUILD)` — rather than asking the packager to filter it afterwards. And `dpkg-deb -c` is
    how this was found: read the artefact, not the configuration that produced it.
84. **Writing the operations document is what proved the knob missing.** `docs/operations.md` had a
    table telling an operator to choose `--fsync-policy` per storage device. The flag did not exist:
    `FsyncPolicy` is in the engine and `tcp_server.cpp` passed `FsyncPolicy::INTERVAL` as a literal,
    so the most consequential setting in a database was unreachable. Documentation written for a
    reader rather than from the code is a test of the code — and this is the third time in this
    repository that a document and the tree disagreed, with the document right about what should
    exist.

85. **Two consumers of one tree need the guard on both sides.** Guarding the Python wheel's
    `install()` with `if(SKBUILD)` was half a separation: the system rules stayed unconditional, so
    scikit-build-core ran them too — and the wheel build compiles only `orderbook_shared`, so
    `install(TARGETS ob_tcp_server)` looked for a binary that build never produced. One missing
    `if(NOT SKBUILD)` turned into **two** red required checks, because both integration jobs install
    the package with `pip install -e`. When a rule exists for one consumer, ask what the other does
    with it.

86. **A new CI job is a ruleset change, and the repository checks that for you.** `docs-integrity`
    failed with `produced but not required: 'package'` twelve seconds into the run. A job nobody
    requires looks like coverage, so `check_contexts.py` refuses the drift in either direction.
    Adding a job means: add the context to `.github/rulesets/master.json`, `PUT` it to the live
    ruleset, and **read the ruleset back** — this API answers 200 for writes that change nothing.
    Then fix the count wherever prose states it; it was in two documents.

87. **A readiness check that counts the wrong token always answers the same thing.** The bootstrap
    script waited for `MM_PEERS` to show two peers by counting lines containing `node_id` — which
    appears in the *header* and never in a peer row, so the count was always 1 and the wait always
    timed out against a cluster that was up and healthy. Counting `connected` is also the stronger
    condition, because #84 made `MM_PEERS` list connections still in their handshake, and a peer
    that is listed but not connected cannot receive a write. Third instance of this shape today: a
    guard matching `[0-9]+ skipped` fired on `0 skipped`, and one matching `LimitMEMLOCK` matched the
    comment explaining its absence.
88. **`SIGTERM` is a request, so a script that reports "stopped" without waiting reports a state it
    has not confirmed.** `stop` killed three nodes and printed success while all three were still
    draining and flushing — which is what a graceful shutdown does. It now polls `kill -0`, and on
    timeout escalates *and says so*, pointing at the log: a node that will not drain in fifteen
    seconds has something to say.
89. **Writing an operator-facing tool is how operator-facing defects get found.** Running
    `scripts/bootstrap-cluster.sh` and reading the metrics endpoint it prints showed every metric on
    a multi-master node labelled `node_role="standalone"` — `set_node_role()` is called only from
    `promote_to_primary()` and `demote_to_replica()`, and a multi-master node runs neither. So a
    three-node mesh reported three nodes each claiming to be alone, the one thing that label exists
    to distinguish, while `ROLE` on the wire answered `MULTI_MASTER` correctly. Two operator-facing
    signals disagreeing; the metric was the wrong one.

90. **A CI step placed after a step that can fail is skipped, so the step that explains a failure
    only runs when there is nothing to explain.** `Fail on any ThreadSanitizer report` sat behind
    the pytest step and reported `skipped` on three consecutive red runs — checked against the API,
    not assumed. Every race report ThreadSanitizer wrote was deleted with the runner, unread, while
    I patched the test three times. The rule, so `always()` does not get sprinkled everywhere:
    **`if: always()` belongs on a step that surfaces evidence existing only on the runner.** A race
    report from a loaded shared runner is that; a coverage percentage from a failed suite is not a
    measurement, and a `.deb` that failed verification rebuilds locally in a minute. Same family as
    a required job nobody requires: **a mechanism that exists only when it is redundant.**

91. **A `subprocess.PIPE` nobody reads freezes the process; it does not merely lose the logs.** The
    pipe fills at 64 KB and the node blocks inside `write()` — it stops serving and still looks
    alive to `poll()`. Measured (i3-7100U, Release, default level): 2000 writes cost **153 bytes in
    total**, because writes are not logged at INFO, but **each client connection costs ~153 bytes**,
    putting the ceiling at roughly **418 connections per node**. The `cluster` fixture is
    session-scoped across 145 tests, so the battery goes past it. Second half, in the diagnostic
    path itself: `server_proc.stderr.read()` in a "server failed to start" handler **waits for EOF**,
    so with a process that is alive and not answering — the case it was written for — it hangs
    instead of printing.

92. **A refused connection and a timeout are different failures, and the difference turned a test
    problem into a server finding.** A process that is slow **or blocked** keeps its listening
    socket, so it times out. **A refusal means nothing is listening** — the node died or closed its
    listener. While the helper returned one word for both, three red runs read as "the test
    flickers"; once separated, the same run said "the outgoing primary stopped listening for thirty
    seconds", which is a claim about the engine and can be falsified.

93. **A fixture that repairs a dead node without distinguishing a deliberate kill from a crash
    cannot see a crash, and stays green.** `healthy_cluster` and `healthy_mm_cluster` restarted
    whatever was not running, and the failover modules kill nodes constantly, so a node that died on
    its own was repaired in silence. Not a workaround for a known defect — an inability to see one.
    `kill_node()` now records the intent and `unexplained_deaths()` reports the rest with its exit
    status and log tail. Verified by mutation: a node killed behind the harness's back yields
    `node-1 ... is not running and no test killed it: signal 9`.

94. **`open(path, "w")` on restart deletes the evidence you just decided to keep.**
    `restart_node()` reuses the data directory, so repairing the cluster truncated the log of the
    node that had died a second earlier. Append, with a separator — and gather the death report
    **before** the repair, because the value of that list is the evidence rather than the count.

95. **`terminate called without an active exception` is not an uncaught exception.** It is
    libstdc++'s message for a **joinable `std::thread` being destroyed or reassigned**, and reading
    it as an exception sent me looking for a missing `catch` — `failover.cpp` has none, which looked
    like the answer and was a red herring. The actual owner of that message is a destructor.

96. **A guard whose early return means "a stop has begun" reads at every call site as "stopped".**
    `ReplicationManager::stop()` was `if (!running_) return;`, then `running_ = false`, then the
    join. The second caller saw `false` and returned having joined nothing, then destroyed the
    object — whose destructor calls `stop()` and hits the same guard. A joinable thread reached
    `~thread` and the node died with `SIGABRT` on a *planned* `FAILOVER` (#88). The fix is that the
    guard becomes true: serialise `stop()` on its own mutex, hold it **across** the join, and make
    the check an `exchange`. Releasing the mutex before the join reintroduces the same hazard with a
    smaller window.

97. **Releasing a lock around `stop()` while leaving the pointer in place invites a second caller
    into the window.** `demote_to_replica()` read `repl_mgr_`, unlocked, stopped, relocked, reset —
    so two demotions both passed the null check and the second worked on an object the first was
    destroying. Take ownership under the lock (`std::move` out of the `unique_ptr`) and the second
    caller sees `nullptr`. `AsyncSnapshotBuilder::shutdown()` already did exactly this, from #79:
    the pattern was in the tree, one file away from the defect.

98. **Audit the operation with the safest name first.** The command that killed the outgoing node
    was `FAILOVER` — planned, deliberate, the one an operator reaches for *in order to* be careful.
    Same shape as `abort()` in the flagship product's migration machine claiming the source still
    had everything. Ask of every reassuring name: what does it do that the alarming ones do not?

99. **Measure what the mutation does; do not assume it fails loudly.** I expected the reverted fix
    to abort, so a plain regression test would have been enough. Twelve runs: twelve **hangs**, no
    aborts — one join succeeds and the other waits on a thread id that will never be signalled. A
    hanging test detects a defect and reports nothing, and CTest's default timeout is **1500
    seconds**, so in CI it reads as a stuck runner. Every test now has a 300-second `TIMEOUT`,
    without which that regression test proves nothing.

100. **Building while `ctest` is running invalidates the run, and the failures look real.** A
    targeted `cmake --build --target X` relinked binaries that a full `ctest` was in the middle of
    executing, and the run came back **7 failed out of 806**. A clean sequential repeat: 811 of 811.
    Nothing was wrong with the tree. If a suite fails after concurrent building, repeat it before
    reading it — and prefer one build-then-test command over two overlapping ones.

101. **Filtering a long run's output to a summary throws away the diagnosis you will need if it
    fails.** I piped `ctest` through `grep -E "tests passed|tests failed"`, which reported seven
    failures and **not one name**, so the next step was a rerun rather than a look. Capture
    everything to a file and filter when reading it. Same shape as the sanitizer report step that
    only ran on success: the information exists exactly until the moment it matters.

## Current state and open problems

Roadmap phases 1-6 are complete; 7-11 are planned in [docs/roadmap.md](docs/roadmap.md). Item numbers
below refer to that file. **Those numbers are permanent ids — never renumber them.** A new item takes
the next free number wherever it sits on the page; `scripts/check_roadmap.py` (run in CI) checks ids,
references and ranges. The rule exists because three renumbering passes each broke something, and
because commit messages and specs cite these numbers.

**Where the suites stand:** 811 C++ tests (`ctest -j1`, ~2 min) and 146 integration tests plus 2
opt-in Binance skips (`pytest tests/integration/`, ~8 min on i3-7100U), all green, and **no `xfail` left** —
every marker that recorded a known defect went with the defect. Both suites run in CI on every pull
request, the **whole** integration battery a second time under ThreadSanitizer with a step that
fails the job on any skip, and the tree also builds and tests under Clang. Twelve required checks on
`master` since #33 added `package`, which builds the .deb, the tarball and the RPM and verifies them
— including that the packaged binary accepts the packaged configuration.

Read the sanitizer claims with #83 in mind: until it landed, `OB_ENABLE_ASAN`, `OB_ENABLE_TSAN` and
`OB_ENABLE_COVERAGE` instrumented the test binaries and the server but **none of the static
libraries**, because `add_compile_options()` only affects targets declared after it and those blocks
sat below all of them.

Things a newcomer should know, because they are real limits rather than bugs to file again:

- **The wire protocol has no authentication or encryption.** Roadmap #30. Do not expose a node
  outside a trusted network.
- **The whole integration battery runs under ThreadSanitizer**, not a subset — since #85. 145 tests,
  **zero skips**, zero reports. Before that the job ran three multi-master modules, and the reason
  given for the narrow scope was a hypothesis that turned out to be false (pitfall 75). Widening it
  also revealed that four modules built their own path to the server and ignored `OB_SERVER_BINARY`,
  so part of that job had been testing an *uninstrumented* binary since it was written (pitfall 77).
  A skip in that job now fails it.
- **A graceful `FAILOVER` used to be able to abort the outgoing primary, and does not now** (#88).
  The outgoing node revokes its own lease, so #82's unconditional lease-lost demotion runs alongside
  the handover's own — two callers into `demote_to_replica()`, whose `stop()` guard returned early
  meaning *stopping* rather than *stopped*, leaving a joinable thread to be destroyed. If you add a
  lifecycle `stop()` here, serialise it and hold the mutex across the join; the early return has to
  mean finished.
- **Every test has a `TIMEOUT`** — 300 s, 900 s where a sanitizer is on (`tests/CMakeLists.txt`).
  CTest's default is 1500, and the regression test for #88 **hangs** under the defect it guards
  rather than aborting, so without a timeout it detects and reports nothing.
- **Failover takes about twice as long as it used to, on purpose.** Since #82 a candidate waits one
  lease TTL after the leader key goes absent, so the previous holder has certainly stepped down.
  Measured: 10.2 s → 20.1 s after a `kill -9`. The alternative that costs no latency makes a primary
  read-only during a brief etcd hiccup, so the cost was moved to latency deliberately;
  `--election-lease-wait-ms` is the knob.
- **Creating a snapshot happens on a worker thread, not on either io loop** (#79). One at a time; a
  second request during creation is refused as busy, and a finished snapshot whose requester has gone
  is discarded rather than sent — matched on `conn_id`, because the case that `node_id` cannot see is
  the same node reconnecting.
- **A subscriber that stops reading is disconnected, not throttled** (#45). Each subscription has an
  8 MB queue ceiling, about 140 000 rows, and past it the session is closed. There is no flow control
  and no resumption; a consumer needing continuity re-reads with `SELECT` from a known sequence
  number (#65).
- `rapidcheck` is pinned to `master` rather than a commit SHA, unlike every other dependency.

*Entries used to sit here and no longer describe the code, and the list is kept because the pattern
matters more than any one of them.* "Deference on election cannot tell a further replica from a dead
one" was true until #72 gave published positions per-node leases. "A node that joins an origin's
stream mid-way never establishes a contiguous frontier" was true until #76 made snapshot bootstrap
real and #67 closed on it. "Streaming subscriptions work embedded and not over TCP", plus a whole
in-flight section describing them as unbuilt, were true until #45 merged — and survived two commits
past it, which is the ordinary half-life of a status note nothing checks.

## Before you call a change done

1. Build clean, no warnings
2. `ctest -j1` green
3. New behaviour covered by a test; new server functionality also covered in `tests/integration/`
4. Logging added
5. Hot-path changes (WAL, SoA, columnar, codec, aggregation, query engine, engine facade): run
   `bench_engine` in Release and compare against the previous run **on the same machine**
6. Conventional commit message, in English
