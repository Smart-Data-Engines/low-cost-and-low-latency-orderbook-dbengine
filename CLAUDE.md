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

## Current state and open problems

Roadmap phases 1-6 are complete; 7-11 are planned in [docs/roadmap.md](docs/roadmap.md). Item numbers
below refer to that file. **Those numbers are permanent ids — never renumber them.** A new item takes
the next free number wherever it sits on the page; `scripts/check_roadmap.py` (run in CI) checks ids,
references and ranges. The rule exists because three renumbering passes each broke something, and
because commit messages and specs cite these numbers.

**Where the suites stand:** 744 C++ tests (`ctest -j1`, ~2 min) and 135 integration tests plus 2
opt-in skips (`pytest tests/integration/`, ~6 min), all green, and **no `xfail` left** — every marker
that recorded a known defect went with the defect. Both suites run in CI on every pull request, the
multi-master modules a second time under ThreadSanitizer, and the tree also builds and tests under
Clang.

Things a newcomer should know, because they are real limits rather than bugs to file again:

- **The wire protocol has no authentication or encryption.** Roadmap #30. Do not expose a node
  outside a trusted network.
- **Failover takes about twice as long as it used to, on purpose.** Since #82 a candidate waits one
  lease TTL after the leader key goes absent, so the previous holder has certainly stepped down.
  Measured: 10.2 s → 20.1 s after a `kill -9`. The alternative that costs no latency makes a primary
  read-only during a brief etcd hiccup, so the cost was moved to latency deliberately;
  `--election-lease-wait-ms` is the knob.
- `rapidcheck` is pinned to `master` rather than a commit SHA, unlike every other dependency.

*Two entries used to sit here and no longer describe the code.* "Deference on election cannot tell a
further replica from a dead one" was true until #72 gave published positions per-node leases. "A node
that joins an origin's stream mid-way never establishes a contiguous frontier" was true until #76
made snapshot bootstrap real and #67 closed on it.

## Before you call a change done

1. Build clean, no warnings
2. `ctest -j1` green
3. New behaviour covered by a test; new server functionality also covered in `tests/integration/`
4. Logging added
5. Hot-path changes (WAL, SoA, columnar, codec, aggregation, query engine, engine facade): run
   `bench_engine` in Release and compare against the previous run **on the same machine**
6. Conventional commit message, in English
