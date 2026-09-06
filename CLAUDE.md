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
    session-scoped across 146 tests, so the battery goes past it. Second half, in the diagnostic
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

100. **The build directory is shared mutable state; a background build or test run holds a lock on
    the whole tree.** A targeted `cmake --build --target X` relinked binaries that a full `ctest`
    was in the middle of executing, and the run came back **7 failed out of 806**. A clean sequential
    repeat: 811 of 811. Nothing was wrong with the tree. If a suite fails after concurrent building, repeat it before
    reading it — and prefer one build-then-test command over two overlapping ones.
    Three variants of the same mistake in one session: building during a test run, **switching
    branches during a build** (which produced a binary made of two branches), and `pkill -f "cmake
    --build"` matching the replacement build it was clearing the way for.

101. **Filtering a long run's output to a summary throws away the diagnosis you will need if it
    fails.** I piped `ctest` through `grep -E "tests passed|tests failed"`, which reported seven
    failures and **not one name**, so the next step was a rerun rather than a look. Capture
    everything to a file and filter when reading it. Same shape as the sanitizer report step that
    only ran on success: the information exists exactly until the moment it matters.

102. **A flag that suppresses a safety check must be impossible to leave set.** `handing_over_`
    tells the monitor loop that a missing leader key is expected, which means a stuck `true` makes
    the node ignore a genuine lease loss for ever — a worse defect than the double demotion it
    fixes. `initiate_graceful_failover()` has **seven** return paths, one of which keeps the role
    when the revoke fails, so the flag is set by a scope guard rather than by a pair of stores. The
    test is not "did I remember every path": if the handover dies after revoking, the guard clears
    on unwind and the next pass demotes.

103. **A pipeline's exit code is the last command's, and `grep -c` reports 1 for no matches.**
    `cmake --build … | grep -cE " error"` exited 1 on a build that reached 100% with nothing wrong,
    and the harness reported it as a failed command. Check the build's own status, or put the grep
    in a separate step. Twice in one session, both times reading a filter's verdict as the thing it
    was filtering.

104. **A loop that reads state at the top and acts on it after a network call is acting on stale
    state, and a flag that covers "the change is in flight" does not cover "the change finished
    while I was asking".** `monitor_loop()` reads `role_` at the start of an iteration and the
    leader key later in the same one, with an etcd round trip between them. My first fix for #89 was
    a `handing_over_` flag, which covers a handover *in flight* — and a handover that starts and
    finishes inside that gap clears the flag before the branch runs, leaving the branch to act on a
    `current` that still says PRIMARY. Two windows, two guards, neither subsuming the other: the
    flag, and a **re-read of the role immediately before acting**. Found by reasoning about what the
    mutation would show rather than by waiting for it — a flag suppressing a symptom is worth
    re-examining when the symptom has more than one cause.

105. **Every timeout in a test suite was chosen against an uninstrumented build, and the sanitizer
    job applies all of them.** `sanitizers-integration (tsan)` failed with `node-1 never accepted
    connections` on a branch containing no C++ at all: a 30-second startup budget for a node that
    starts in two, under an instrumentation that costs five to fifteen times the run time, on a
    runner also running the rest of the battery. `patience()` in `conftest.py` triples startup waits
    when `TSAN_OPTIONS` or `ASAN_OPTIONS` is set. Scaling, not silencing — a node that cannot start
    inside the scaled window still fails, and the same reasoning gave the ctest `TIMEOUT` its 900 s
    sanitizer variant (pitfall 99). When a limit and an instrumentation meet, ask which one was
    measured.

106. **`printf` to `stdout` is block-buffered the moment it is not a terminal, so the line
    confirming a start is the last one to arrive.** The server's banner reached a redirected log
    file at **process exit**, while every JSON line was on time — because the logger writes to
    `stderr`, which is unbuffered. Measured: absent from the file while the node was up and
    answering. Worse, the banner also ran *before* the bind, so it announced listening that had not
    happened and a failed start printed the claim with the error underneath. If a line's purpose is
    to tell an operator something is true, log it after it becomes true, through the logger, and
    flush anything that is not.

107. **One fact typed in three places is two chances to drift, and the version was the fact.**
    `project(... VERSION)`, `pyproject.toml`, and a literal in `tools/ob_tcp_server.cpp` — with
    nothing comparing them, and the literal being the only version a running node could show. The
    C++ copy is gone (a compile definition on `orderbook_core`, so `ob::version()` is the only way
    to get it); the Python one cannot be, because a wheel's metadata is not a C++ macro, so a test
    holds the two in step and another refuses the version as a literal in any source that reports
    it. Both mutation-checked. A literal that agrees today drifts at the first bump, and the symptom
    is an operator told the wrong build is running — worse than being told nothing.

108. **A flag is a mechanism only where something reads it in the path that matters.**
    `request_close_after_flush()` was consulted in exactly one place: the EPOLLOUT drain, which runs
    only after a *partial* write. So a response small enough to fit the socket buffer left the
    session open with the flag set and nothing reading it — and `ERR auth_failed` is eighteen bytes,
    with closing on the first failure being the entire rate limit for authentication (#30). The
    io_uring loop read the flag **nowhere at all**. What makes this worth keeping is which test
    found it: the unit test asserted `close_requested()` was true (it was), and the integration test
    asserted the connection was gone (it was not). Assert the effect, not the branch — pitfall 45
    from the other side.

109. **A gate written per-case is a gate the next case misses.** The authentication check sits
    *before* `execute_command`'s switch, because a branch in each `case` means the eighteenth
    command added without one is reachable unauthenticated and nothing fails. The enforcement is the
    compiler: the classifier's switch has **no `default:`**, so `-Wswitch` makes a new
    `CommandType` a build failure — and a test refuses a `default:` being added, because that one
    label silently turns the exhaustiveness check off and hands every future command whatever the
    default says.

110. **A test asserting the absence of one error message passes for a dozen wrong reasons.**
    `EXPECT_EQ(response.find("ERR unauthenticated"), npos)` was meant to prove a command reached the
    engine after authenticating. It passed while the engine answered
    `ERR OB_ERR_NOT_FOUND: symbol 'BTCUSD' ... not found`, and then again when the query returned
    `OK` with a column header and no rows. Only `INSERT`, `FLUSH`, `SELECT`, and an assertion on the
    **row** made it decisive. A negative assertion about one string is satisfied by every other
    failure.

111. **Do not trim a secret; remove the line terminator and nothing else.** A general trim makes a
    file containing `"abc "` and a file containing `"abc"` the same secret, and for a secret
    "silently the same" is a security property rather than a convenience. The flagship product's
    `read_bytes().strip()` on a random salt removed bytes in about 5% of files, so the process that
    generated it used 32 bytes and every later process used the remainder.

112. **A log line announcing a guarantee the code does not provide is worse than no log line, and I
    shipped one for an hour.** `--cluster-secret-file` initially loaded and validated the file while
    nothing on the replication or multi-master links required it, and the startup log said
    `cluster authentication enabled`. An operator greps exactly that line to confirm exactly that
    guarantee. Found while **writing the operations document** — the third time in this repository
    that writing for a reader tested the code (pitfall 84), and the reason the fix was to implement
    the enforcement rather than to reword the line.

113. **Positional aggregate initialisation of a config struct breaks call sites that have nothing to
    do with the change.** One new field in `ReplicationConfig` produced six `-Wmissing-field-`
    `initializers` errors in `test_replication.cpp` and two more in `test_tcp_server.cpp`, none of
    them about what those tests check. **Designated initialisers do not help** — verified, GCC still
    warns for the omitted members in C++20. A factory helper (`primary_config()`,
    `simple_command()`) does, and it is where the defaults belong anyway.

114. **A benchmark difference above the control's own variation is not automatically a regression —
    read the instruction stream.** `BM_IngestionThroughput` came out 1.8% slower on median across
    six interleaved rounds (master cv 0.75%, branch cv 1.36%), which is outside noise by that
    measure. `objdump` with mnemonics only: `Engine::apply_delta` 1761 instructions, `write_record`
    536, `WAL::append` 863, **byte-identical in both builds**. So no work was added and the
    difference is code placement — on the machine that once reported −40.6% in 8/8 rounds for an
    identical function (pitfall 33). Diffing the disassembly of the measured path is faster than
    arguing about the number.

115. **`getpeername()` succeeds on an `AF_UNIX` socket, and a `sockaddr_in` cast then reads
    whatever followed the path.** The unit tests use `socketpair(AF_UNIX, ...)`, so the address in
    an authentication log line would have been arbitrary bytes rendered as an IP. Check
    `sin_family` before formatting; the cost is one comparison and the alternative is a log that
    lies about where a connection came from.

116. **A metric label fed by an unauthenticated peer is an unbounded label set an attacker
    controls.** The claimed identity in a failed `AUTH` is peer-supplied, so
    `ob_auth_failures_total{identity="..."}` would let anyone who can reach the port grow the
    registry without limit. The counters carry no identity label at all; the identity goes to the
    log and to `STATUS`, both of which are bounded by the connection. Same rule for #31 when
    identities gain permissions.

117. **Anything a peer sends that reaches a log must be length-bounded and stripped of
    non-printables.** One newline in a claimed identity and the log says whatever the peer wants it
    to, in the format an operator's tooling parses. `sanitise_for_log()` is the funnel, and the
    integration test sends `alice\nINFO forged` and greps for `INFO forged`.

118. **The shell's working directory survives between commands, so a `cd` inside one leaks into the
    next.** A `cd` in an unrelated heredoc left the session in `kiro-workspace`, and the next
    `git checkout -b` created the feature branch **in the shared context repository** rather than in
    the engine. Nothing was lost because the branch had no commits, but that repository is the one
    every session writes to. Absolute paths, or `git -C <repo>`, on every git command.

119. **Two ends of a link holding the same key and computing the same function of a nonce are each
    other's oracle, and my first cut of cluster authentication was bypassable with no knowledge of
    the secret.** Answering a challenge cannot require authentication — the peer has not proved
    itself either — so an attacker could receive `CHALLENGE n`, send `CHALLENGE n` straight back,
    be handed `AUTH H(n)`, and replay it. Four messages, zero secrets.

    The trap inside the trap: **binding both nonces does not fix it.** With the nonce reflected,
    `H(theirs, mine)` and `H(mine, theirs)` are the same pair, so the orderings collapse. The fix
    has to make the two directions compute *different values*, which means a **role** in the MAC
    input (`initiator` / `acceptor`). In a symmetric mesh that role has to be recorded explicitly
    (`PeerConnection::we_accepted`), and it resets with every reconnect because it is a property of
    the connection.

    Two tests, failing in different directions: one replays the four steps against a live socket,
    and one pins the inequality the defence rests on — because the first also fails for unrelated
    reasons, and the second says *which* property broke.

    And the honest limit, in `SECURITY.md` rather than assumed away: the role stops **reflection**
    and not an active **man-in-the-middle**. Nothing binds the exchange to the connection, so an
    attacker who can redirect a replica relays both directions and both ends are satisfied. A relay
    can forward any value bound only to a nonce; stopping it needs a channel with an identity, which
    is TLS.

120. **A reference to another object's container is a lock you did not take.** `QueryEngine` held
    `const std::unordered_map<std::string, SoABuffer*>&` into `Engine::live_ptrs_` and read it with
    no lock, while every write path — a client write, the replication apply path, the multi-master io
    loop — inserted into it under `Engine::mtx_`. An `unordered_map` insertion rehashes, so a query
    concurrent with the **first** write for a symbol followed a bucket that had moved (#91).
    Reachable from a plain `SELECT`; invisible for as long as the tests wrote first and read after.

    Measured, because "only a sanitizer sees it" is a claim: plain Debug, **5 of 5 runs pass**;
    under TSan, **exit 66 and 20 reports**, the first naming `_M_rehash_aux`; with the fix, zero.
    A reader who assumed `ctest` covers this would have been wrong five times out of five.

    The fix is a **lookup callable** rather than the map, so `Engine` takes `mtx_` for one map read
    and releases it before the query runs — one uncontended lock per query instead of holding the
    write path's mutex across a scan, which in this engine is the worse trade. Two tests, failing in
    different directions: one drives the race, one refuses the shape, because a behavioural test for
    a rehash is probabilistic and a shape test is not.

    Two things worth carrying forward. **The identical defect was one file away**, in `c_api.cpp`,
    which is the embedded path the Python client uses locally: `ob_insert` creates buffers under its
    mutex and `ob_query` read the map without it. Fixing the server alone would have left it, so the
    shape test asserts that *both* suppliers of the lookup take a lock. And **the fix does not close
    the lifetime problem**: `buffers_` owns the `SoABuffer`s and a snapshot install clears them, so a
    query holding a resolved pointer can read freed memory. Filed as #92 rather than commented on,
    with the tempting answer — hold `mtx_` across the query — named as the one to avoid.

121. **A local ThreadSanitizer binary segfaults before `main` unless ASLR entropy is lowered.**
    `sudo sysctl -w vm.mmap_rnd_bits=28`, which is what the `sanitizers-integration (tsan)` job does
    in its own step. Without it the failure is a `Segmentation fault` from
    `GoogleTestAddTests.cmake` during test discovery, at *build* time, with no message about
    sanitizers at all — so it reads as a broken test rather than a missing sysctl. The value on this
    machine is 32 by default; set it back afterwards if you care about the entropy.
    Under the **integration** battery the symptom is different and reads worse: every node dies at
    startup, so the harness says `not ready after 45s: Connection refused` and the sanitizer's own
    line — `FATAL: ThreadSanitizer: unexpected memory mapping` — is in the node's log, which is why
    `_wait_for_node()` now tails that log instead of reading a pipe that is not there.

122. **`until ! pgrep -f X; do sleep; done` never terminates when the waiting shell's own command
    line contains X.** The wait is spawned as `bash -c '... until ! pgrep -f "pytest
    tests/integration" ...; then cat the log'` — so the pattern appears in the waiter's own
    `/proc/self/cmdline`, `pgrep` finds it, and the loop sleeps forever. Two of these sat wedged for
    **nine hours**, plus a third on `pgrep -f "cmake --build build"`.

    The `[m]atch` bracket trick does not save you: it stops `grep` matching *its own grep process*,
    and the process it matches here is the enclosing shell. A neighbouring session had three wedged
    the same way on `ps aux | grep '[m]ut107sdk'`, where the pattern was in the `cat …/mut107sdk.out`
    part after the loop.

    Two rules. **Wait on a condition that cannot describe the waiter** — a marker line the job
    itself appends (`until grep -q DONE file`), or an exit code, never a process name the wait
    mentions. And **for harness-tracked work, do not poll at all**: a background command re-invokes
    you when it exits, so a waiter loop is both unnecessary and a chance to wedge. Pitfall 100 is
    the same family in a smaller form (`pkill -f "cmake --build"` matching its replacement build);
    this is what it looks like when it costs a day.

    Diagnosing it has the same trap: `pgrep -c -f "cp_c2.out"` reports 1 for a pattern with nothing
    running, because the diagnostic matches itself. Split the literal — `pgrep -f 'd86414''dc'` — so
    the searching command's own line cannot contain it.

123. **A probe that does not reproduce the shape says "no defect" in the same voice as a probe under
    which there genuinely is none.** The design for #30 part three claims that `SSL_write` retried
    after `WANT_WRITE` fails unless `SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER` is set, because
    `Session::flush_output()` does `send_buf_.erase(0, n)`. Two probes failed to reproduce it and
    nearly got the claim deleted from the document.

    Both retried from an **advanced offset in the same allocation** — which presents the *same
    address* for the still-pending bytes, and is legal. `erase(0, n)` does something else: it moves
    the same bytes to a **different address**. Doing that produces
    `error:0A00007F:SSL routines::bad write retry` immediately, and with both modes set the identical
    sequence returns `WANT_WRITE`.

    So the rule is not "write a probe" but **"write a probe that produces the same bytes at the same
    addresses as the code you are asking about"**, and treat a negative result from a probe you just
    wrote as a claim about the probe until the positive control exists. Same family as pitfall 37
    (a surviving mutation means the test measures something else) and pitfall 24 (an `iptables DROP`
    that proves nothing about a repair mechanism), from the diagnostic side.

    A third row came free: with neither mode, `SSL_write` accepts **nothing** until the whole buffer
    fits — first `WANT` at offset 0 with 4 MB pending. That is a worse failure than the one being
    hunted, and it looks like a slow client rather than a defect.

124. **Chain verification is not peer verification, and `SSL_VERIFY_PEER` only does the chain.** It
    answers "was this certificate signed by a CA I trust" and says nothing about whether the
    certificate belongs to the host you dialled. With a private CA that signs a whole cluster —
    which is how anyone deploys this — node B's certificate is therefore perfectly acceptable for
    node A, the man-in-the-middle relay works again *between two holders of legitimate
    certificates*, and every verification reports success. The name check is a separate call:
    `X509_VERIFY_PARAM_set1_host`, or `set1_ip_asc` for a literal address, and the two branches
    differ in both halves — no SNI for an IP (RFC 6066 §3 forbids it) and matching against
    `iPAddress` rather than `dNSName`, so `set1_host("127.0.0.1")` hunts for a DNS entry spelled
    that way and fails against a *correct* certificate.

    The test that carries this has a **good chain and the wrong name**: the certificate is handed to
    the client as its own trust anchor, issued for `10.0.0.2`, served on `127.0.0.1`. Deleting the
    name check makes exactly that test fail and leaves the trust test passing, which is the
    discrimination worth having. Both clients have it at unit and integration level.

    And a smaller trap inside it: the failed `set1_ip_asc` on a host name **queues OpenSSL errors on
    a call that then succeeds**, so the next real failure reports this one. A function that succeeds
    still has to drain.

125. **A permanent configuration error has to be reported before the transient transport one.**
    `OrderbookClient::connect()` built its TLS context after `::connect()`, so a CA file that does
    not exist was masked by `connection refused` whenever the server was also down — sending the
    operator to debug the network for a typo in a path. The trust anchor loads first now: it is
    knowable without a socket, and it will not fix itself. Found by a test written for the *message*
    rather than for the ordering, which is the usual way this class shows up.

126. **A readiness probe that also verifies trust answers two questions with one word.** The TLS
    integration fixture decided a node was up by opening a **verifying** connection, so a node
    deliberately issued a certificate for another address reported `node never answered` while its
    own log said `listening` two lines above. Liveness and trust are different questions and the
    tests below the fixture ask the second one. Pitfall 92's shape, in a fixture.

127. **Three sites hand-copying a config's fields is how a field arrives that nothing carries.**
    `PoolConfig` and `ShardRouterConfig` carried neither credentials nor transport, so from #30 part
    one the C++ pool and the sharded client could not reach an authenticated node **at all** —
    `auth_identity` existed on `ClientConfig`, three call sites copied the fields each happened to
    know about, and none of them knew about that one. The symptom is `ERR unauthenticated` from a
    configuration that reads as complete. One `copy_client_access()` template carries them now, and
    a static test derives `ClientConfig`'s field list **from the header** and refuses a field that
    neither the template nor every construction site mentions. Deriving both sides from the source
    matters: a list written by hand is not evidence about the code (pitfall 79).

128. **A protocol whose server speaks first cannot diagnose a plaintext client.** Forget `tls=True`
    against a TLS port and the connection **hangs until the client's own timeout**, with nothing in
    the server's log: the client waits for the banner, the server waits for a ClientHello, and until
    a byte arrives the server cannot tell a plaintext client from a slow one. The opposite mistake
    fails instantly — a forgotten `--tls-client` sends the plaintext banner where a ServerHello was
    expected, and OpenSSL says `wrong version number`. Neither is fixable; both are now in
    `docs/operations.md` as a two-row table, and the test is **named after the behaviour** so a
    reader does not file the hang as a bug.

129. **On a blocking socket with `SO_RCVTIMEO`, OpenSSL reports the timeout as `WANT_READ`.** The
    socket BIO maps `EAGAIN` to "should retry" whether the descriptor is non-blocking or merely
    impatient, so the synchronous clients get the same code a non-blocking event loop gets — and a
    helper that treats a want as "come back later" waits out another full timeout, for ever, against
    a peer that has stopped talking, with the caller never told. The blocking helpers translate a
    want into a timeout *error*; the event-loop path still treats it as the four-way `IoWant`
    question. Same code, opposite meaning, decided by how the descriptor was configured.

130. **A comment I wrote described the mechanism I intended, in a file where the mechanism was
    absent, and the code worked anyway for an unrelated reason.** The accept path said the banner
    "goes out with the first flush after the handshake completes … `SSL_write` is not reached until
    `tls_handshaking_` clears". `send_response()` ends in `flush_output()`, so `SSL_write` **was**
    reached at accept — and it worked, because OpenSSL lets a write drive a handshake and
    `SSL_accept` then continues whatever it began. Two functions advancing one state machine while
    a flag said the handshake had not started. Found by reading the accept path to answer an
    unrelated question, not by any test, because there is nothing here for a test to catch: the
    bytes arrive either way. Pitfall 68's lesson with the polarity reversed — there the comment
    excused unsafe code, here it described safe code that did not exist yet. "Works because both
    callers are reentrant into the same state machine" is not a property to build on, so the
    handshake now has one owner and the comment is true.

131. **Six hand-written `SSL_CTX_free` calls on six throw paths were all correct, and the leak was
    on the seventh — the one that throws through a helper.** `TlsContext::client()` allocated the
    context and *then* called `check_file_or_throw()` on the CA bundle, so nobody wrote a free there
    because nobody wrote a `throw` there either. `sanitizers (asan)` found it on a required check,
    after review had passed over it three times: 1616 bytes direct plus about 3 kB indirect, 242
    allocations, in the three tests that exercise a refusal.

    It was a real leak and not a test artefact, which is the part worth carrying:
    `OrderbookClient::ensure_tls_context()` turns that exception into an error and leaves `tls_ctx_`
    null, so a pool retrying `connect()` against a mistyped CA path leaks a context **per attempt,
    for ever** — once per health-check interval. Pitfall 32 exactly: a retry loop exposes leaks that
    one-shot code hides.

    The fix is RAII and not a seventh free, because cleanup written per throw path is correct right
    up until a `throw` arrives from a function you did not write. A static test refuses an
    `SSL_CTX_new` whose result lands anywhere but a `CtxGuard`, so the class is impossible rather
    than fixed once — a leak has no assertion without an allocator hook, but its *shape* does.

132. **A disassembly comparison that matches the symbol by substring reports the sum of two
    functions as the size of one, and the number is plausible enough to act on.** `flush_output_tls`
    contains `flush_output`, so the awk "am I inside the function" flag never cleared and the
    extractor reported **310, then 335**, for a function that is 148. On the strength of that I
    nearly restructured the send path a second time to fix a regression that existed only in my own
    instrument. Match `<demangled signature>:` exactly.

    Same family as the `/metrics` lookup that missed because the key carried a label set
    (pitfall 66): the instrument had the defect, and it answered in the same voice it uses when
    there is nothing wrong. The real numbers, once it was fixed: `feed()` and `send_response()`
    byte-identical, `flush_output_plain()` 135 → 147, and those twelve are the `io_want_`
    bookkeeping that fixes the stalled-response regression — not TLS dispatch, which is 178
    instructions in a function of its own reached through one compare.

133. **A test whose *precondition* is a race fails having exercised nothing, and its message is
    indistinguishable from the real defect's.** `TlsSession.ALargeResponse…` needs a partial
    `SSL_write` to happen at all, and it arranged that by having the reader sleep 400 ms "and then
    drain". Under ThreadSanitizer the server's handshake and its 40 000-string payload took longer
    than the sleep, so the reader was already draining when the first `SSL_write` ran, the socket
    kept accepting, and 1.2 MB went out in one call. `distinct_pending` came back **1**.

    The expensive part is that **1 is also what the real defect produces**: dropping
    `SSL_MODE_ENABLE_PARTIAL_WRITE` prints the identical line. So the flake and the defect are the
    same message, and a required check went red saying something true about a run that had tested
    nothing. Pitfall 105 in a new place — every timing in this suite was chosen against an
    uninstrumented build.

    The fix is not a longer sleep. The reader now waits on an atomic the server sets **after
    asserting** `has_pending_output()`, so the condition is established rather than hoped for, and
    the failure mode when the buffers are too big is a precondition assertion naming that. Same
    lesson as the notify-ordering test in #79 (pitfall 64): do not race the thing you are asserting.

    **And that fix was only half of it**, which is the part worth carrying. With the precondition
    established the test still failed 1 in 6 under TSan, now reporting `3 vs 3`: the assertion was
    `distinct_pending > 3`, a count of *samples*, and how many times a drain loop gets scheduled is
    not a property of the code. Threshold pinned to a count → fails on legitimate variation → gets
    re-run until green. What actually separates the two SSL modes is whether the gauge is ever seen
    **between** the full response and zero: without `ENABLE_PARTIAL_WRITE` pending is only ever
    `payload.size()` and then 0, so no intermediate exists to observe. One intermediate is the whole
    signal and it does not move with the scheduler. Both mutations still fail the rebuilt test, and
    through different assertions — the gauge for one, `bad write retry` for the other.

134. **`send_all()` on a non-blocking socket reads the first `EAGAIN` as a dead peer, and it
    dropped a replica in the middle of every catch-up bigger than a socket buffer.** The helper is
    correct on the *replica's* blocking socket, where `EAGAIN` means the `SO_RCVTIMEO` deadline
    expired; on the primary's accepted sockets it means "come back later". `send_to_replica()` -
    the only sender in the catch-up path - used it, so the replica was removed, reconnected, asked
    for the same range and was removed again. **Measured: 17 270 of 40 000 records delivered**, then
    `send_to_replica failed for fd=7, marking disconnected`.

    Not a TLS defect and not introduced by series D - **found** by it, because "where does this code
    put `SSL_ERROR_WANT_WRITE`" has the same answer as "where does it put `EAGAIN`": nowhere. Every
    write to a replica now goes through `enqueue_send()` and the EPOLLOUT drain, which is the only
    shape in which a socket saying "later" has somewhere to say it. The ceiling still drops a replica
    that is not draining, at 16 MB of queued output instead of one socket buffer; #93 is the cursor
    that removes the reconnects.

135. **A test that needs a full socket buffer needs that buffer's *measured* capacity, not a
    generous-looking number.** The first version of the catch-up test wrote 2 MB and **passed
    against the defect**: with neither side setting a buffer size, this loopback pair absorbs
    **2.6 MB** before the sender first sees `EAGAIN` (539 of 1600 records got through). Shrinking the
    receiver to `SO_RCVBUF=4096` reproduced it reliably and made the test take **49 seconds**, because
    2 MB through a 2 kB window is one delayed ACK per few kilobytes. 8 MB of WAL and no window tricks
    reproduces it in 0.66 s. Pitfall 123 from the other side: a probe that does not reproduce the
    shape says "no defect" in the same voice as one under which there is none.

    Second half, on setup cost: **wide records, not many records.** 40 000 one-level appends took
    ~60 s of the test's runtime; 1600 appends of 200 levels put the same bytes on the wire in a
    fortieth of the time.

136. **On a mutual TLS link the accepting end has no name to expect, and that is a design question
    rather than an omission.** After `accept()` the only fact about the peer is its source address.
    Matching the certificate against *that* sounds strong and breaks on the first `DNS:`-only
    certificate, behind NAT and behind a proxy - it turns "TLS on" into "the cluster does not form" -
    and puts a reverse lookup in the accept path. Chain-only is genuinely sufficient **when the CA
    signs nothing but this cluster**, because every holder of a signed certificate then already has
    the cluster secret and the whole WAL; with a corporate CA the same sentence means every host in
    the organisation may become a replica.

    So the answer is a mechanism, not a paragraph: `--tls-peer-names` is an identity allowlist, empty
    means chain-only, and **the startup log says which of the two is in force**. A weaker mode that
    is not visible is the one that ends up on production - part one paid for the mirror image of
    this, a log line claiming a guarantee nothing enforced (pitfall 112).

137. **A verification check placed after the handshake runs after OpenSSL has already buffered the
    peer's decrypted bytes.** The peer-name allowlist could have been checked by the caller; that
    would be four call sites across two event loops, each needing to refuse *before* touching the
    receive buffer, and one forgotten `if` means a peer whose certificate we rejected feeding frames
    to the parser. It lives inside `TlsChannel::continue_handshake()`'s success path instead and
    fails the handshake, which makes the gate impossible to forget rather than merely present -
    the same move as part one putting the client gate before the `switch` instead of in every
    `case` (pitfall 109).

138. **A harness that builds one command line in two places drifts, and the comment warning about
    one flag is the tell.** `ClusterManager.restart_node()` constructed its own argv and had never
    learned about `--cluster-secret-file`, so a restarted node in an authenticated cluster came back
    **without the cluster secret** and was refused by its peers - which reads as a replication
    defect. Series D found it with `--tls-*`: the restarted replica connected in plaintext and its
    log said `Connection reset by peer` with no TLS line above it. Sitting next to the old copy was a
    comment explaining that `--multi-master` had to be repeated there "or the test that noticed would
    look like a convergence bug" - correct about one flag while three others were missing. One
    `_node_argv()` now; same family as pitfall 77.

139. **Per-connection state belongs behind a `shared_ptr` when its record lives in a container that
    moves.** `replicas_` is a `std::vector<ReplicaInfo>` whose `push_back` moves its elements;
    `peers_` is a map in which a `PeerConnection` **changes key** after the handshake by
    erase-and-move; and `replica_states()` / `peer_states()` return *copies* for `STATUS`. A
    by-value TLS member holding any pointer into its own record - the reader pointing at the
    channel, say - dangles from the first reallocation, and the symptom is corrupt bytes on the
    sixth replica rather than anything that reads as a lifetime bug. One heap object with a
    reference count survives all three, and a copy made for `STATUS` reports on the connection it is
    actually about.

140. **On an edge-triggered loop, read until the TLS layer says it has nothing - not until the
    socket does.** OpenSSL reads a whole record, up to 16 kB, decrypts it into its own buffer and
    returns only what was asked for, so a socket-level `EAGAIN` can arrive with decrypted bytes still
    pending and no further epoll event coming. `SSL_read` returning `WANT_READ` cannot. The
    replication reader keeps `::recv` semantics for its two callers by mapping a want onto
    `errno == EAGAIN`, and exposes `io_want()` separately - because the thing errno cannot carry is
    *which* want, and a read waiting to **write** needs EPOLLOUT rather than readability.

141. **`-Werror` is not the same set of errors in Debug and Release.** `maybe-uninitialized` is an
    optimisation-time analysis, so it does not exist in a Debug build: the series D branch had 930
    green tests and **did not compile** in Release. The specific shape is worth knowing on its own -
    a `switch` over an enum is **not exhaustive to the compiler** even when every enumerator is
    covered, because a value outside the enumerator set is representable. Initialise the variable to
    the failure case rather than trusting the switch to fill it, so "impossible" means "disconnect
    this peer" and not "whatever was on the stack".
142. **A stale build directory reports a perfect measurement.** A mnemonic diff of the branch
    against master said *identical* for every function, including two that had been rewritten. The
    Release build had failed on the error above; the compound command ended in `echo` and `tail`, so
    the shell's exit status was 0 and the task reported success. Archives dated two days earlier were
    compared against master and agreed with it, because they **were** master. The check that catches
    this costs one line: grep the head artefact for a symbol that exists only on the branch.
143. **A gauge published only where it goes up cannot come down.** `ob_replicas_tls_verified` was
    set at the end of a successful handshake and nowhere else, so a replica that dropped left its
    contribution behind — and `verified` could exceed `connected`, which reads as impossible and
    sends an operator after the wrong fault. The same defect had the other shape on the mesh side
    (roadmap #94): `ob_mm_peers_connected` was recomputed inline at three sites and none of them was
    `accept()`. The fix is not more call sites: recompute both counts from the connection table on
    every pass of the loop that owns it, and let the call sites be latency only.
144. **Every failure branch in a retry loop has to move the next-attempt time.** One branch in the
    multi-master reconnect loop did not, so a failure that would never clear was retried at loop
    frequency and logged at loop frequency: `Reconnect: invalid peer address:` every 100 ms for the
    life of the process (roadmap #95). Backoff is what makes a permanent failure legible; a log line
    at 10 Hz is a log an operator cannot read.
145. **A record that cannot become a peer has to be erased, not retried.** A connection this node
    *accepts* is stored with no node id and no address, because the port it arrived on is the peer's
    ephemeral source port. Once it closes before its handshake names a node there is nothing to dial
    and nothing for it to become - it was one dead entry per refused inbound connection, kept
    forever, and the thing that dialled us will dial again by itself.
146. **`"disconnected".count("connected") == 1`.** An integration test counted the word `connected`
    in the `MM_PEERS` view and read one live peer plus one refused peer as two peers - an assertion
    that would also have passed against a node connected to nobody. The token that answers the
    question was a suffix of the token that answers its opposite. Parse the column; and when two
    modules already have their own copy of the parser, the fix is one helper on the harness, not a
    third copy.
147. **Alignment NOPs and call-target addresses make two identical functions read as different.**
    A disassembly diff reported `broadcast` as 173 against 170 instructions with the first
    divergence `nopl` against `cs nopw`, and every `call` site as a difference because the
    section-relative address moved. Both are code layout, which is the thing the tool exists to see
    through: drop padding, normalise `ADDR <target>` as a unit, and keep matching symbols by their
    **exact** demangled signature (pitfall 132). What is left over is real - and here it was member
    offsets, +48 bytes into `ReplicaInfo` and `PeerConnection`, which is the honest reading of "the
    same instructions in the same order, reading fields that moved".

148. **On an unfinished GitHub check run `conclusion` is the empty string, not null** - so
    `jq '.conclusion // .status'` prints nothing rather than falling through to `IN_PROGRESS`, and a
    poll loop that decides "pending" by matching a status word at end of line sees `name: ` and
    counts zero. Mine then announced `ALL CHECKS SETTLED (12 reporting)` with six checks still
    running. Ask jq for the branch explicitly - `if .status == "COMPLETED" then .conclusion else
    .status end` - or count with `select(.status != "COMPLETED") | length`, and make the loop report
    the states it did **not** recognise instead of treating them as done. Second instance in one
    session of the same shape: a compound shell command ending in `echo`/`tail` returned 0 while the
    build inside it had failed. **A verification loop that cannot say "I do not know" says "fine".**

149. **A test that resolves a pointer and never dereferences it proves nothing about that
    pointer.** The driver for #92's use-after-free used `SELECT *`, which resolves the live buffer
    for an **existence check** and reads through it never; three AddressSanitizer runs came back
    clean against a defect that reproduces 3 of 3 with `SELECT VWAP(price)`, because only the
    aggregation branch calls `read_snapshot(*buf, ...)`. #91's test picked VWAP for the same
    reason. Before believing a clean sanitizer run, check that the code under test reaches the
    instruction you are trying to catch.
150. **`grep -E " error |warning:"` does not match `foo.cpp:52:11: error: ...`** - there is no space
    after the word. Two `cmake --build` invocations reported nothing while two test files failed to
    compile, and `ctest` then ran the previously-built binaries and reported 934/934 passing. The
    stale binary also failed a static test about a type it had been compiled *before*, which read
    as the fix not working. **Check the build's exit status, not its output**, and when a test
    result surprises you, compare the binary's timestamp against the source's.

151. **A container's key type is a claim about what may be a key, and a map cannot tell a
    descriptor number from the node id it is standing in for.** An accepted mesh connection was
    inserted into `peers_` — keyed by node id — under `static_cast<uint16_t>(client_fd)`, so a
    connection landing on descriptor N replaced the live record of peer N: its send buffer, its
    backoff and its advertised address went with the assignment, and its descriptor stayed armed in
    the epoll set with nothing behind it (#96). The reserved range the comment above it described
    (`fd + 10000`) is not the fix — it is still a node id, in the same space, one arithmetic slip
    from a live record. A **separate container** is, keyed by something no other subsystem gives
    meaning to; `conn_id` is minted once and never reused, which is what a key needs. It also
    removed six `node_id == 0` tests that stood in for "is this record real?".

152. **A record that changes container leaves every pointer to it dangling, including the one the
    caller is still using.** The re-key was `peers_.erase(key); peers_[real_id] = std::move(rec);`
    and the io loop kept the pointer it had taken into the erased record — the EPOLLOUT branch a
    few lines below reads `peer_ptr->connected` and drains through it, so one event carrying both
    EPOLLIN and EPOLLOUT read freed memory. Same class as #92 and #120. The shape that makes it
    impossible rather than fixed: the function that relocates a record **returns the new location**
    and the caller has nothing stale to reach for. `nullptr` then means "dropped, not moved", which
    the caller must also handle — the third state a bool cannot carry.

153. **A tie-break that two peers evaluate independently must be a function of the values both of
    them have, and nothing else.** A symmetric mesh can end up with two links to one node, and if
    each end resolves that by preferring its own dial, each closes the link the other kept and the
    pair is left with none. The surviving link is the one the **lower-numbered node** dialled, which
    each end can evaluate from its own id, the peer's, and which of the two it accepted. The two
    tests for it are the same situation seen from the two ends, because a rule that is consistent
    is exactly the one whose two views agree — and flipping the comparison fails both.

154. **A sentinel meaning "not known yet" has to be refused when a peer claims it as a value.** The
    handshake sets `node_id` from the peer's own message, and a peer claiming **0** — the value that
    means "this connection has not identified itself" — would have stayed in the unidentified
    container for ever: connected, counted nowhere, never adoptable. Claiming *our own* id is the
    other one, keying a record as us, which broadcast then sends our own records to. `--mm-node-id`
    refuses zero at startup, so neither is a well-behaved peer; that is an argument for refusing
    them, not for assuming they cannot arrive.

155. **A defect a live cluster does not reach cannot be measured by the harness that builds live
    clusters — so build the coincidence instead of waiting for it.** Reaching #96 needs a node id
    equal to a descriptor number, and the integration fixture numbers its nodes 1..3: measured over
    three multi-master modules, **zero** collisions, which is a fact about that fixture and not
    about the code. A unit test installed a peer record for **every** descriptor number the accepted
    socket could get; the connection arrived on 8 and the record of peer 8 was gone, deterministically.
    The live measurements were still worth taking, because they say which consequences are reached
    today (14 records replaced, all of them idle) and which are not (0 orphaned descriptors,
    0 duplicate links even with all nodes launched at once — the etcd watch is slower than a
    loopback connect by orders of magnitude).

156. **A blocking `connect()` under a lock turns one unreachable peer into a node-wide stall, and a
    refused connection hides it perfectly.** The multi-master reconnect loop held `mtx_` across its
    whole pass, dial included, and `set_nonblocking()` came *after* the connect rather than before.
    That is the mutex the io loop takes for every peer event and `broadcast_local()` takes on the
    client write path, so a SYN that goes nowhere — a firewalled peer, a vanished host, a registry
    entry pointing nowhere — stopped everything for `tcp_syn_retries` doublings. **Measured with
    `tcp_syn_retries = 6`: an inbound mesh connection waited 132.5 s for its first byte, and one
    client write blocked 135.7 s** (#97). Every healthy cluster and every existing test missed it
    because a peer that is merely *down* refuses, and a refusal returns in microseconds. Put the
    socket in non-blocking mode before `connect()`, bound the wait yourself, and do it with no lock
    held: the kernel's own answer is two minutes, which is never the answer a mesh wants.

157. **`poll()` reporting `POLLOUT` on a connecting socket does not mean the connection
    succeeded** — a refused connection is also reported as writable. `getsockopt(SO_ERROR)` is the
    authority, so read it even on the path where `poll()` says ready. Same family as pitfall 34
    (an HTTP 200 that means nothing): find the field that carries the answer.

158. **When a decision and its action are split by a lock release, whatever stops a second actor
    has to be written down before the lock goes.** Dialling outside `mtx_` means the reconnect loop
    comes round 100 ms later and sees the same peer still disconnected, so the attempt and its
    backoff are claimed *before* the release — otherwise the loop opens a fresh connection to a peer
    it is already dialling, every 100 ms, for as long as the dial lasts. Two more states appear in
    that window and both need an answer under the lock afterwards: the peer may have left the
    topology, and it may already be connected through a link somebody else opened. This is
    pitfall 144 (#95) applied to a branch that now spans a lock release.

159. **Measure the entry point the caller actually uses, and run the load concurrently with the
    thing you claim it is blocked by.** Two of my own measurements of #97 came back clean before
    one came back right. The first issued the writes and started the dial *in sequence* and reported
    0 ms — every write landed after the connect had already returned. The second ran them
    concurrently and still reported 1 ms, because it called `Engine::apply_delta()`, while a
    multi-master node's server calls `apply_delta_mm()` — the overload that broadcasts, and
    broadcasting is what takes the mutex. **A measurement of the wrong entry point exonerates the
    code in the same voice it would use if the code were fine.** Pitfall 149 from the other side:
    there the test never reached the instruction, here the measurement never reached the function.

160. **`git checkout <path>` is a destructive command, and I ran it as cleanup.** After killing a
    hung mutation run I reverted `src/multi_master.cpp` "for tidiness" and deleted a finished,
    unpushed fix — the header and the tests were untouched, so the loss was invisible until a grep
    for the new container came back empty. Two rules, and the second is the real one: reach for
    `git stash` rather than `checkout` when a file may hold work, and **commit the fix before
    mutation-testing it**, not after. A mutation harness that edits the tree is a good reason for
    the tree to be committed.

## Current state and open problems

Roadmap phases 1-6 are complete; 7-11 are planned in [docs/roadmap.md](docs/roadmap.md). Item numbers
below refer to that file. **Those numbers are permanent ids — never renumber them.** A new item takes
the next free number wherever it sits on the page; `scripts/check_roadmap.py` (run in CI) checks ids,
references and ranges. The rule exists because three renumbering passes each broke something, and
because commit messages and specs cite these numbers.

**Where the suites stand:** 943 C++ tests (`ctest -j1`, ~2.5 min) and 189 integration tests plus 2
opt-in Binance skips (`pytest tests/integration/`, ~10.5 min on i3-7100U), all green, and **no `xfail` left** —
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

- **The wire protocol has authentication and no encryption.** All three surfaces authenticate since
  #30 parts one and two — client sessions with `--auth-secret-file`, the replication link and the
  multi-master mesh with `--cluster-secret-file` — by challenge-response over HMAC-SHA256, so a
  secret never crosses the wire. **Nothing is encrypted**, so every query and every row is readable
  by anything on the path: do not expose a node outside a trusted network. Both files are off by
  default, and the startup log WARNs for each disabled surface rather than leaving "default open" in
  a document. Three things to know before touching it: the client gate sits *before*
  `execute_command`'s switch and its classifier has no `default:` (pitfall 109); the surface label
  is inside the HMAC input because replication and multi-master share one secret; and the two secret
  files must differ, which the start enforces, because a client holding the cluster secret can
  present itself as a replica and stream the whole write-ahead log.
- **The whole integration battery runs under ThreadSanitizer**, not a subset — since #85. 146 tests,
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
- **Integration nodes log to a file in their own data directory, and a node that dies without a test
  killing it fails that test.** Both came from #86. Node output used to go to a `subprocess.PIPE`
  nothing read, which loses the evidence and — past roughly 418 connections per node — blocks the
  node inside `write()`. And `healthy_cluster` restarted anything not running with no way to tell a
  deliberate `kill_node()` from a crash, so a crashing node was repaired in silence. If you add a
  fixture that stops a node on purpose, record it the way `kill_node()` does, or
  `unexplained_deaths()` will report your own teardown as a defect.
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
