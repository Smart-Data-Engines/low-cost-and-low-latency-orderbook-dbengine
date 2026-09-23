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

# Tests — counts and measured runtimes are in docs/roadmap.md
ctest --test-dir build --output-on-failure -j1
```

First configure pulls googletest, google/benchmark, rapidcheck and nlohmann/json via `FetchContent`,
which needs network access and a few minutes.

System dependencies: `liblz4-dev`, `libcurl4-openssl-dev`.

Tests that touch coordination need a native `etcd` on PATH (or `OB_ETCD_BINARY`). Installation
instructions are in [tests/integration/README.md](tests/integration/README.md) and
[docs/cli.md](docs/cli.md).

Property tests use RapidCheck with `RC_PARAMS=max_success=100` for multi-master networking and `=25`
elsewhere, set in `tests/CMakeLists.txt`.

Fuzzing is opt-in, separate from CTest, and needs Clang: `-DOB_BUILD_FUZZERS=ON -DOB_ENABLE_ASAN=ON`
builds three libFuzzer harnesses over the parsers that read untrusted bytes. The build refuses GCC
and refuses to run without ASan, so an incomplete request fails at configure time.
[fuzz/README.md](fuzz/README.md) has the commands, the corpus layout and the measured limits.

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
29. **The checkpoint goes after the flush, never before - and claims the drain, not itself.** A
    `CHECKPOINT` record claiming more than is durable turns a crash into data loss; claiming less
    costs a replay that gets skipped anyway. "After the flush" was not enough: the segments are
    written without `mtx_`, so a checkpoint meaning "everything before me" claimed the records
    appended during the segment write, whose rows were still queued (#159). It carries the drain's
    position now, and WAL retention reads the same value. For the crash window between writing the
    segment files and appending the checkpoint, `replay_wal_tail()` skips a record its symbol's
    segment already holds, by the WAL position the segment recorded (#63; `end_ts_ns` is only the
    fallback for older segments) — without that, replay rewrites a durable segment from a WAL tail
    that may hold fewer rows than the segment does, because truncation only follows the
    replica-confirmed position.
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

    **Same lesson, a year of entries later, and this time it cost two required checks.** #139
    replaced the row header literal with a generator over the column table, which left
    `kQueryHeader` dead - a `static constexpr std::string_view` at namespace scope, which GCC
    ignores and clang refuses under `-Wunused-const-variable`. Both `clang-build` and `fuzz` failed
    on it (the fuzz harnesses are the only other clang build in CI), on a branch whose local
    verification had been GCC-only from the first commit to the pull request. When a generator
    takes over from a literal, the literal is dead by construction: grep for it in the same edit.

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
    Adding a job means: put the workflow and its context in `.github/rulesets/master.json` in one
    PR, merge it, then `PUT` the live ruleset and **read it back**. Applying it before the branch
    can produce the new context blocks that branch indefinitely (#108). This API can answer 200
    for writes that change nothing.
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
    that removed the reconnects, and #98 is what it found about the position on the wire.

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

161. **A test that stops when it has seen enough cannot see too many.** The mutation that made the
    catch-up cursor chase the live WAL end — which delivers a record twice, once from the file and
    once from the queue that held it — **survived** its first run. The test read until the expected
    1001 records and returned, so the duplicate was sitting in the reader's own 64 kB buffer,
    discarded on return, indistinguishable from a record that was never sent. It reads until the
    socket goes quiet now and the assertion is exact in both directions. An assertion of the form
    `read(n); expect n` cannot fail for "too many" — it has to be `read until quiet; expect exactly
    n`.

162. **Turning a synchronous pass into a resumable one gives up an ordering guarantee that was
    never written down.** `handle_catchup()` held `mtx_` from the first record to the last, so
    `broadcast()` could not interleave; nothing said so, because nothing had to. The moment the pass
    could stop, a live write landed in front of the history it comes after. Before making a pass
    resumable, ask what the pass was holding and who else needs it — the answer is the design, not a
    detail.

163. **"Chase the live end" and "fix the end" differ by a duplicate you can predict.** A record is
    appended and then broadcast under one engine lock, and `::write()` makes it visible in the WAL
    file immediately — so while a cursor holds `mtx_`, that `broadcast()` is *waiting*. A cursor that
    re-reads the WAL's live position therefore reads the record, sends it, releases the lock, and the
    waiting call sends it again. Not a race: the outcome of every handoff under write load. Fixing
    the end at creation makes the split by offset instead of by timing, and a record cannot be on
    both sides of an offset.

164. **A number in a `WAL <file> <offset>` line was never the record's offset, and nothing said
    so for four phases of replication work.** `send_to_replica()` sends the replica's last
    *acknowledged* position, `broadcast()` sends a literal `0`. Measured: all 112 records of a
    catch-up carried `file=0 offset=0`, so the replica saved one record's worth however many it
    received. The roadmap item that described the reconnect loop as "monotonic progress in 16 MB
    steps" was quoting the mechanism it assumed rather than the field it could have read. When a
    wire field carries a position, print it in a test and look at it — the assumption that a field
    named `offset` holds an offset is not a measurement (#98).

165. **A bound whose worth you have not measured may be worth half of what you assumed, and
    that is still worth saying.** `kCatchupBatchBytes` bounds how long one catch-up pass holds the
    mutex the write path needs. Measured, i3-7100U: removing it moves the longest `broadcast()` wait
    from 12.0 ms to 25.3 ms for a **draining** receiver and **not at all** for a receiver that reads
    nothing — because there the queue ceiling already bounds the pass. A factor of two on one of the
    two cases, not the order of magnitude the reasoning suggested. The mutation for it survives, and
    it is recorded as surviving: a test would have to be a wall-clock gate.

166. **A worst-case latency needs a control window in the same run, on the same machine.** The
    first read of the catch-up measurement said "max 25 ms" and looked like a lock being held. A
    window with nothing running at all, in the same process seconds earlier, reached **5.1 ms** on
    one run of this two-core machine — so the max was part scheduler. p999 (0.048 ms) and the
    control's own max are the honest pair; a max without a control is a number about Linux.

167. **A function that writes has to return where it wrote, because rotation makes the position
    underivable.** `WALWriter::append()` checks the rotate threshold **after** the write, and
    `rotate()` publishes `{next_index, next_offset}` in one store — so for the one record per WAL
    file that crosses the threshold, `current_position()` is already in the next file while the
    record sits at the end of the previous one. `current_position() - total_len` therefore names a
    file the record is not in, and can underflow. `append()` and `append_with_origin()` return a
    `WalPosition` now. The five internal `append_*` deliberately do not: nothing needs their
    position and one of them can refuse to write, so it has none to give — a returned value nobody
    reads is the `provisional`/`basis`/`in_use`/`key_id`/`partition_by` shape again (#98).

168. **Tests that drive a manager directly say nothing about which arguments its caller passes.**
    Every behavioural test for the replication wire drives `ReplicationManager`, so a mutation that
    derived the position at the *engine's* call site survived all 951 of them. The behavioural test
    for it would need `Engine`'s hardcoded 512 MB rotate threshold to become configurable for a
    test's sake, which is the wrong trade. A static test over `Engine::apply_delta` is the mechanism
    instead, and for this claim it is stronger: the engine may not *compute* a position at all
    (#98). The threshold **is** configurable since #124, and that does not change this entry: the
    flag was added because an operator wants it, and the static test is still the stronger mechanism
    for this claim. A knob arriving later does not retroactively justify adding one for a test.

169. **A test whose premise is wrong can pass for a reason unrelated to its name, and the node's
    log is where you find out.** An integration test asserting that a restarted replica resumes
    rather than replays passed — while the node logged `clearing local data before starting
    replication` and `REPLICATE 0 0 0`. A failover-managed replica reaches `demote_to_replica()` on
    its own restart, which wipes the store and deletes the saved position, so the test measured the
    wipe. Read the log of the thing you restarted before believing the assertion about it (#98,
    #101).

170. **`accept()` is not the moment a replica exists, and `broadcast()` did not know that.**
    `replicas_` gets an entry as soon as the socket is accepted, so a live write between the accept
    and the `REPLICATE` line is queued to a replica that has not asked for anything — and then the
    catch-up, whose range ends past that record, sends it again. Measured: ten records broadcast in
    that window, twenty received, in the pattern 1..10 then 1..10. Found because a live-path test
    for something else saw fourteen where twelve were sent (#100).

171. **A pure function with a contract gets a test for the contract, not for the one caller that
    has one.** A mutation dropping the file index from `wal_position_before()` passed every
    behavioural test in the replication suite: it only shows for a record in an earlier file whose
    offset is larger than the boundary it is compared against, and no cursor range those tests
    build straddles a boundary at that moment. Nothing about the ordering needs a socket, so it is
    pinned directly — strict, file first, and a walk across boundaries required to move forwards
    (#100).

172. **A scaling rule applied by hand is applied once too few.** `patience()` triples every wait in
    the integration battery under a sanitizer, and five of the six `_wait_for_node()` call sites
    wrapped their budget in it. The sixth was the multi-master fixture — three nodes, so the one
    with the most to lose — which had a flat 20 s where every other fixture had 45, and
    `sanitizers-integration (tsan)` went red on a loaded runner. Measured before touching it,
    because a raised timeout is the standard way to hide a hang: under TSan on this machine a node
    answers PING in **0.39 s**, and 0.39/0.46/0.54 s for three started together. The scaling lives
    inside `_wait_for_node()` now, with a static guard that forbids a call site scaling again.

173. **A test that asserts an absence needs a control, and mine failed the moment it had one.** The
    #99 measurement splices a live record into a snapshot stream and requires the bootstrap to be
    abandoned. Its control — the same stream without the record — **also** failed, because
    `SNAPSHOT_END` carries the CRC32C of the manifest the replica assembles and the mock primary
    sent a bare `SNAPSHOT_END`. Without the control the spliced test would have passed by not
    finding anything (#99).

174. **A window you cannot observe is a window that was not there.** The primary-side #99 test
    needs a snapshot transfer to be *in progress*. `continue_snapshot_transfer()` backs off at half
    of `MAX_SEND_BUF_SIZE`, so a 7.3 MB snapshot streamed in one pass and `snapshot_active()` was
    never true; 14.6 MB across 208 files stalls. Poll the state the code publishes
    (`snapshot_active()`) rather than sleeping, and check the window existed before asserting about
    what happens inside it (#99).

175. **A mutation can survive because a comment overstates the code.** Swapping `st.active = false`
    with the release of deferred bytes survived, and the comment claiming that ordering was
    load-bearing was simply wrong: `release_deferred_live()` goes through `enqueue_send()` and could
    not re-defer its own bytes whatever the order. The comment changed, not the code (#99).

176. **A defect on a five-second timer needs a structural guard, not a slower test.** The heartbeat
    loop splices into a snapshot stream exactly as a live record does, with no client write
    involved. A behavioural test would have to wait out the interval inside a stalled transfer, or
    make the interval configurable for a test's sake. Instead: `run_loop()` may not call
    `enqueue_send()` or `enqueue_and_flush()` at all, because `queue_to_replica()` is the only
    function that knows about deferring (#99).

177. **A `catch` does not stop an abort when a joinable thread is still in scope.** `main()` had no
    handler at all, so a failed bind left through the default terminate handler — SIGABRT, exit -6,
    a supervisor logging a crash for a taken port. Adding the handler is half the fix: the shutdown
    monitor is a local `std::thread`, and one that is still joinable when its destructor runs calls
    `std::terminate`, so the catch block would have returned **past** the destructor that aborts.
    Both mechanisms were live at once, and the second is #88's, whose message (`terminate called
    without an active exception`) sends readers looking for a missing `catch`. The mutation that
    says so is the useful one: with the catch present and the join guard removed, both refusal tests
    still fail. Pin the exit code, not the message — the message was correct throughout (#102).

178. **One flag serving two meanings makes the log claim something nobody asked for.** Winding the
    shutdown monitor down by setting `g_shutdown_requested` made a node that never started print
    `Shutdown requested — the epoll loop will drain and close` on its way out, which reads as an
    operator having signalled it. Two flags: one means "a shutdown was requested", the other means
    "this thread has no further reason to exist". Same defect as a line announcing a guarantee the
    code does not give (pitfall 112), and it is pinned by an assertion rather than left to the
    comment claiming the separation matters (#102).

179. **A wipe must forget the sequence frontier, or dedup empties the store.** Clearing the store
    clears `stores_`, `buffers_`, `pending_rows_` and every segment directory — and the frontier
    lives in `seq_tracker_`, which is none of those. Left standing it claims records the wipe has
    just deleted, so every record the primary sends below it is dropped as a duplicate and nothing
    refills the hole: measured, **0 rows where 1 was replayed**. Two instances, and the second was
    found only by going looking: `discard_local_data_for_resync()`, and `load_snapshot()`, which
    got away with it because the *mesh* calls `adopt_snapshot_sequence_state()` straight after and
    that resets — while the replication bootstrap calls only `load_snapshot()`. The mechanism was
    already written down in `SequenceTracker::reset()`'s own docstring. Closed as a class by a
    static test that derives the functions from the source, not by two fixes (#101).

180. **A question that carries a request cannot be asked of a peer that will not answer it.** The
    first design for #101 sent the stream identity **with** `REPLICATE`, and defended itself with an
    apply gate: hold every record until the primary either confirms the stream or refuses. Cheap on
    paper. But the position has gone out, so an older primary is **already streaming** by the time
    the replica learns there will be no answer — which means in-flight records, a second handshake
    on a busy socket, an ordering to reconcile, and a loop in which every reconnection wipes.
    `STREAMID?` carries no position, so an older primary ignores it and sends **nothing**: there is
    nothing to reconcile and no gate to write. Ask first, request second, when the answer decides
    whether the request is safe (#101).

181. **The decision belongs to whoever holds both facts, not to whoever noticed first.**
    `demote_to_replica()` discarded the store on all four of its call sites, and the condition it
    was standing in for — "is what I hold a prefix of this primary's stream?" — needs the primary's
    identity, which exists only after the connection. Three of those callers are role changes and
    one is process start; that list grows, and the fifth arrives with a comment about why it is
    different. A branch on the caller is **correct in every case somebody thought about while
    writing it**, so there is nothing behavioural to catch: the guard is a static test that the
    signature carries no discriminator and that exactly one place decides (#101).

182. **A restart timed from the harness measures the harness.** Two stores differing threefold gave
    6.32 s and 6.33 s — agreement to the third decimal is not a measurement, it is the polling
    quantum. Read timestamps the node prints, or assert the quantity that made the time grow: after
    #101 a restart re-streams **2 records at both a 33-record and a 97-record store**, and comparing
    the larger store's replay against the smaller store's size needs no threshold at all (#101).

183. **A guard that starts at zero on every fresh object is disarmed on the connection it exists
    for.** `ReplicationClient::local_epoch_` was raised only by what the primary sent, and
    `demote_to_replica()` builds a new client on every role change - so the first `REPLICATE` of
    every connection carried epoch 0, which is never greater than anything and so could not trip
    `ERR STALE_PRIMARY`. Measured: a node that held the role in epoch 9 **applied and stored** a
    record announced at epoch 5. The fix is not an initialiser: a number two objects both claim to
    hold is the defect, and the one that resets is the one the guards read (#103).

184. **Seeding a copy from a field that is itself zero fixes only the case somebody thought of.**
    The item said to seed the client from `engine_.current_epoch()`. That reaches a node which held
    the role, because promotions write an `EPOCH` record to its WAL - and misses a node that has
    only ever followed, because nothing raises the engine's epoch on the follow path. The commonest
    case, a replica re-pointed after a failover, was the one the named fix would have missed.
    Measure the case you are not thinking of before writing the fix the item describes (#103).

185. **A monotone number is the whole of a fence, so a wipe must not touch it.** #101's wipe
    discards the position, the store and the sequence frontier, so discarding the epoch alongside
    them looks consistent - and would leave every failover unfenced, since a failover always takes
    the wipe path (the new primary's stream identity differs from the saved one). The price of
    keeping it is named instead: a data directory carrying a higher epoch than the cluster it is
    pointed at refuses to follow it, in a log line with two readings (#103, `docs/operations.md`).

186. **On the wire, the line is now and the payload is history.** A catch-up forwards every record
    type but `ROTATE`, each on a line stamped with the primary's *current* epoch - so the `EPOCH`
    records of every past promotion arrive announced as current. Refusing on the payload would
    disconnect every replica replaying a failover out of the log; the payload may raise the epoch
    and must never refuse on it (#103).

187. **An assertion placed after the next message tests the recovery, not the damage.** The mutation
    that let the epoch be talked *down* survived, because the test checked the number after a later
    record had already raised it back. Nothing was wrong with the engine or with the mutation - the
    assertion stood one message too late. When a mutation survives, ask where the test is looking
    before you ask what the code does (#103).

188. **A field written at one site and read at none cannot have a behavioural test, so the check is
    static and its subject is the tree.** Six instances of this shape in this workspace
    (`provisional`, `basis`, `in_use`, `key_id`, `partition_by`, then #104), not one with a symptom,
    every one found while looking for something else. `tests/test_field_usage.cpp` surveys every
    member declared in `include/orderbook/` against every occurrence in `src/` and `include/` and
    fails when they are **all** plain writes with the value discarded. One finding in the engine
    before the fix, none after (#104).

189. **"A trailing-underscore name before a `;` with something in front of it" is also the shape of
    `x = y_;`.** What separates a declaration from an assignment is what the **prefix ends with** —
    a type for the first, `=` for the second. Six fields were reported dead because their only read
    looked like a declaration and was skipped as one, and `return message_;` hid the only read of
    `Result<void>::message_` the same way. The corollary is sharper than the rule: a checker that
    skips lines it *classifies* as declarations must instead skip **recorded sites**, or a misparse
    deletes a read rather than merely misfiling it (#104).

190. **A mutating expression whose value is consumed is a read.** `conn.conn_id = next_conn_id_++`
    and `if (!unknown_names_reported_.insert(x).second)` both mutate a member *and hand its value to
    somebody*; four fields read as dead because of it. The cheap rule that gets all of them right:
    for a pure write, the statement must **begin** with the name. And in the other direction, a
    constructor's initialiser list (`: pos_(0), end_(0) {}`) is the only write some members ever
    get, so it has to count as one (#104).

191. **A mutation table needs the verdict each mutation is *supposed* to produce, because some must
    survive.** A checker that flags everything passes every kill test, so the load-bearing mutation
    is the control: plant a field that is written **and read** and require silence. Two further
    traps met the same day — a mutation that drops a call leaves a static function unused and a
    mutation writing `count(...) >= 0` compares an unsigned value, so `-Werror` rejects both, and a
    mutation that does not build measures nothing (#104).

192. **An adapter that adds work the other adapters do not pay measures the harness, and it happens
    in both directions.** In the comparative benchmark a fresh `clickhouse-client` costs **80 ms**
    per query and a fresh `psql` **40-60 ms** against queries of a few milliseconds, so the first
    versions of those adapters charged the competitors forty and ten times the work asked of them.
    The mirror image was ours and read as an honest loss: `OrderbookEngine.query()` building 4000
    `OrderbookRow` dataclasses costs p50 **15.5 ms** against **5.3 ms** for the same bytes parsed
    into tuples, which is what every competitor's adapter does - so the first four-system run
    reported 18.2 ms against ClickHouse's 6.3. Every timed path now holds one connection open and
    parses text into tuples (#39 part two).

193. **Two defects in three lines, each hiding the other.** The comparative harness's
    `query_time_range()` selected on timestamps the server had never stored (#105), so it returned an
    empty list - and its row mapping read `r.timestamp` and `r.size`, which do not exist. The empty
    result hid the wrong attribute; the wrong attribute would have raised the moment the result
    stopped being empty. Part one's **published** noise floor was measured through those three lines,
    which means it describes the latency of a query matching no rows. When a number looks stable and
    cheap, check that the work it is timing happened.

194. **A parameter honoured in one mode and dropped in another is the cross-mode form of a field
    nobody reads.** `OrderbookEngine.insert(timestamp_ns=...)` is used in the embedded branch and
    never mentioned in the TCP or pool branches, because `INSERT` and `MINSERT` carry no timestamp
    field. Four integration call sites pass it and **none asserts on it**, which is the strongest
    evidence that it reads like the right thing to do. A client that cannot send a value must refuse
    rather than drop it (#105).

195. **The machine may already be running containerised copies of the systems you are about to
    benchmark natively.** ClickHouse on 58123 for the flagship product's test engines, PostgreSQL on
    5432 for the landing page - and a run reaching either would have measured a container while
    nothing in the numbers looked wrong. Refuse the container's port **by name before asking the
    server anything**, so the reason in the table names the container rather than whatever that
    server answers first, and let the version prove which one replied: native ClickHouse 26.8.x,
    container 24.8.x. The second consequence is quieter: `pg_createcluster` takes the next free port,
    so the native PostgreSQL came up on 5433 (#39 part two).

196. **Generate the artefact, then quote it.** Rewording a limitation meant the published results
    file no longer matched the code, so I regenerated it - and the README still cited the previous
    run's numbers and a file I had just deleted. A document quoting a number nothing produced is the
    defect this repository has recorded four times; the cheap guard is to regenerate last and copy
    the numbers from the file that will be committed (#39 part two).

197. **A drain with no bound is a shutdown decided by whoever happens to be connected.** On
    `SIGTERM` the listener closed at once and the loop then waited for every session to end -
    correct, and unbounded. Measured: **0.11 s** to exit with nothing connected and **still running
    after 60 s** with one *idle* client attached, because an idle client never leaves and a
    long-lived client is the normal case for a database (a pool, a `SUBSCRIBE` stream, a monitor).
    A supervisor's answer to a process that will not stop is `SIGKILL`, so the flush and checkpoint
    the graceful path exists for were exactly what did not run. Two rules fall out: **the default
    bound must be shorter than the supervisor's timeout** (10 s against systemd's 90 s), and
    "wait for ever" stays available but has to be asked for (#106).

198. **A decision that needs a clock is a unit test when the clock is an argument, and a sleep when
    it is not.** `drain_verdict(started, sessions, timeout, now)` has four cases - nothing
    connected, inside the deadline, on the deadline, and "0 means for ever" - and all four are
    microseconds of arithmetic instead of a test that waits. The other half of the same fix: the
    io_uring loop asked the same question in **two** more places and **no CI job builds that
    file**, so a bound written three times could not even be compiled on one of the two sides. One
    function plus a static test refusing any line that pairs `draining_` with `active_sessions` is
    what makes it a closed class rather than a fix applied where somebody looked (#106).

199. **A harness that escalates silently hides the defect it is escalating around.** `_stop_node()`
    is "SIGTERM, then SIGKILL after five seconds" with nothing said, so every integration node with
    a client attached had been hard-killed for as long as that helper existed - which is why a node
    that never exits went unnoticed by two hundred tests. And the server's new default of 10 s is
    *longer* than that escalation, so shipping the fix without touching the harness would have kept
    the whole battery on the killing path: the nodes now start with `--drain-timeout-ms 2000`, and
    from that change on the suite exercises the graceful path on the way out (#106).

200. **Assert that the connection ended, not that your own `send()` failed.** #101's ceiling test
    required the *sender* to notice the drop, and on this machine the drop lands after the send loop
    finishes: eight failures in a row against a binary identical to master's, in runs whose log said
    `disconnecting replica fd=7: not draining its stream-identity answers`. A probe settled it in
    one line — `replicas=0 hung_up=0`, the record gone and the sender unaware. `POLLRDHUP` is the
    property and it costs nothing else: **reading the answers would make the socket a well-behaved
    peer**, which is the one thing that test must not become. Pitfall 54's shape, in a test written
    by someone who knew about pitfall 54.

201. **A parser that reads the fields it knows and ignores the rest accepts typos forever.**
    Measured over a live server: of sixteen command shapes, **fourteen accepted a token nobody
    reads** and five stored a row for it — `INSERT AAA EX bid 100 5 1 notanumber` answered `OK`.
    This is pitfall 27 on the wire, the same class #36 closed for command-line flags, and it is
    worse here because the discarded token is sometimes the one that mattered: an upgraded client
    sending an event time to an older server got `OK` with the time dropped, so it could not tell a
    server that stored it from one that did not (#107, #105).

202. **Correct behaviour in one branch out of eighteen is indistinguishable from nobody having
    decided.** `AUTH` already refused a trailing token — an exact `tokens.size() != 3` written by
    whoever happened to think of it there — while seventeen other commands did not. So the fix is
    one table rather than eighteen habits, and it is **indexed by `CommandType`**, which makes a
    nineteenth command a *compile* error until it declares its arity. That is the same mechanism as
    the missing `default:` in `allowed_before_authentication()`: a static test can be forgotten in
    review, a `static_assert` cannot (#107).

203. **When you build the table, do not give it a field nothing reads.** The arity table carries
    only the **maximum**; each branch keeps its own minimum, because a branch that reads `tokens[5]`
    needs the guard that makes the read safe. A minimum in the table would have been read by
    nothing — a poor field to add in the same repository that spent #104 on the fifth instance of
    exactly that shape. Ask it while designing, not while auditing (#107).

204. **Measure the words, not only the behaviour.** Before writing the refusal I ran the same shape
    past the query parser, which has answered this situation for years:
    `ERR Parse error at line 1, col 26: unexpected token 'garbage'`. Borrowing those words costs
    nothing and stops one protocol from saying one thing two ways — and the same measurement is what
    turned `SELECT` and `SUBSCRIBE` from "not checked" into a genuine exemption from *counting*.
    The one place the rule inverts is `AUTH`: a refusal writes a log line, so its extra token is
    refused **without being repeated**, because a response echoed into a log is a response in a log
    (#107).

205. **A sentinel that already makes the comparison false turns its own guard into something that
    cannot fail.** "No maximum" was `kFreeForm = size_t(-1)`, guarded by
    `max_tokens != kFreeForm && tokens.size() > max_tokens` — and deleting the guard **changed
    nothing**, because no line carries `SIZE_MAX` tokens. The mutation survived, and the guard still
    read like protection to anybody reviewing it. Moving the emptiness into the type
    (`std::optional<size_t>`) makes the same deletion compare against `nullopt`, refuse every
    `SELECT`, and die. When a mutation survives, ask whether a sentinel is doing the work the guard
    claims — and prefer the type that cannot express the accident (#107).

206. **An argument accepted and discarded is worse than one refused, because the caller has no way
    to find out.** `insert(timestamp_ns=…)` was honoured embedded and dropped on the wire, so the
    same call meant two things depending on the mode — measured as **0 of 400 rows** inside the
    dataset's own span where two SQL systems returned 400. The fix is half protocol and half
    refusal: the field is optional and last (six fields still mean what they meant), and both
    clients **ask** whether the server can store one and fail before sending a byte. Sending the
    field is not a test for support: a server that predates it answers `OK` and drops the value,
    which is why #107 had to come first (#105).

207. **Zero is not absence when zero already means something.** `DeltaUpdate::sequence_number` uses
    0 for "unassigned", so accepting `event_time 0` as "stamp it on arrival" would have made "I have
    no time for this row" and "use yours" the same request — in the one place where telling them
    apart is the feature. Refused, naming the field. And deliberately **no sanity window**: a server
    refusing a timestamp from 2019 breaks backfill, which is the case the field exists for (#105).

208. **Before adding a field a client can choose, find what already reads the one it resembles.**
    Three mechanisms read a record's time and only one would have been a real risk: multi-master LWW
    compares the **HLC** (node clock), so a client cannot decide which conflicting write survives;
    segment pruning is a range-intersection test, so out-of-order times cost scan efficiency rather
    than correctness; TTL retention works on whole segments by their newest event time, so a
    **backfill arrives with its age** and one row dated in the future keeps its segment alive. The
    first made the change possible, the third is what an operator meets at 3 a.m. and is now in
    `docs/operations.md` (#105).

209. **A capability list is a published vocabulary, so a name in it is a promise — and a promise
    nothing reads is the shape this workspace has paid for five times.** `capabilities:` uses names
    rather than a version number (a number needs a semver parser in every client, and the question
    is "can you take this field", not "what are you called"), the absence of the line is an answer
    rather than an error, and a static test requires every announced name to have a **declared
    reader** in both directions. Adding a third name forces naming its reader (#105).

210. **CodeQL cannot see a `static_assert` as a use, so a helper written only for one looks dead.**
    `cpp/unused-static-function` flagged `grammar_is_indexed_by_type()` — correctly, from where it
    stands: nothing calls it at run time. The check belongs inside the assertion anyway, so it is an
    immediately-invoked constexpr lambda now and there is nothing left to mistake for dead code.
    Worth knowing before writing the next compile-time completeness check, because this repository
    writes a lot of them (#107).

211. **Verifying a guard against the wrong build target is the worst place for a stale artefact.**
    I swapped two table rows to prove the `static_assert` fires and built `orderbook_engine` — which
    does not compile `command_parser.cpp`. The build passed, the guard looked dead, and the next
    step would have been to "fix" something that works. Building `orderbook_tcp_server_lib` failed
    the assertion twice, as it should. Fourth instance of the stale-artefact class in this
    workspace, and the first one aimed at a mechanism rather than at a result: **when you mutate to
    prove a guard fires, name the target that compiles the file** (#107).

212. **A test that quotes a message pins the wording; the property is usually one clause of it.**
    My own #107 integration test asserted `"unexpected token 'surprise'" in reply`. Two days later
    #105 made the seventh token a field, so the same line is refused as an *invalid event time* —
    same rule, different sentence, and the test went red for a change that strengthened the thing
    it was guarding. It asserts `"'surprise'" in reply` now, which is the rule: **the refusal names
    the token**. Quote a phrase only where the phrase is the contract (`ERR STALE_PRIMARY`), and
    where it is prose, assert the clause that carries the guarantee (#105, #107).



213. **"The likeliest producer" is a guess with a costume on — sample it.** #86's server thread
    stayed open for a week on "an OOM kill fits every observation", which it did. Measured under
    ThreadSanitizer: the heaviest modules peak at **246 MiB resident** across every node and etcd,
    the largest single node ever at **92 MiB**, and `MemAvailable` never below **8.8 GiB** — against
    a 16 GB runner, not an out-of-memory condition. The hypothesis is now **refuted rather than
    unproven**, which is worth more than the guess was, and the sampling took twenty minutes
    (#86).

214. **Compose the harness with the defect before blaming the machine.** What actually fits every
    observation in #86 is `_stop_node()`'s silent "SIGTERM, then SIGKILL after five seconds" meeting
    #106's unbounded drain: a node closes its listener at once, never finishes draining while a
    client is attached, and is killed. From outside that is *nothing listening*, then `signal 9`, no
    race report, only sometimes — every line of the observation, and neither half is a defect in the
    server's logic. The report now says which of the two it is, because **the harness knows and
    never said so**: "this harness escalated SIGTERM to SIGKILL 5.0s ago" or "no SIGKILL came from
    this harness; MemAvailable is now N MiB" (#86, #106).

215. **A sanitizer that will not start reads as a node that died.** A TSan-instrumented server
    aborts with `FATAL: ThreadSanitizer: unexpected memory mapping` **at random** on this kernel —
    higher ASLR entropy than it can map around, `vm.mmap_rnd_bits=28` is the usual answer and this
    machine refuses to set it. It exits **66**, and until the report named that, a node that never
    started was indistinguishable from one that crashed. Eleven integration errors in my first
    measurement attempt were this and nothing else (#86).

216. **`git checkout <path>` deleted an uncommitted fix again — fourth time in this workspace, and
    this time in the same session that wrote the rule down for somebody else.** I reverted
    `tools/ob_cli.cpp` "to clean up" after a hand-applied mutation, and the #110 work went with it;
    the giveaway was seven tests failing at baseline with the *old* output, which reads exactly like
    a fix that does not work. Recovered because the patch was a script rather than an edit — which
    is the practical lesson beside the old one: **commit before mutating**, and when you must patch
    by hand, patch with something you can run twice.

217. **Coverage is not an oracle, and a green fuzzing campaign cannot tell the difference.** The
    multi-master frame harness reached the "frame longer than 64 MiB" branch on every single run —
    `above_max_length` is a committed seed that exists for exactly that — and deleting the ceiling
    check from the parser **survived 1.6 million executions**. Nothing asserted that the refusal had
    to happen: the verdict changed from −1 to 0 and every other property still held. Three of three
    planted defects in that file survived the first pass. The lesson is about order of work: **plant
    a defect before calling a harness finished**, because "the corpus reaches this shape" and "this
    shape is checked" are different claims and a fuzz run only demonstrates the first (#38).

218. **Invariance under fragmentation is blind to a systematic decoding error.** The prettiest
    property in that harness — one stream split into chunks yields the same frames — cannot see a
    decoder that reports *every* payload one byte early, because it shifts both sides of the
    comparison equally. The oracle that sees it needs no second parser: a round trip through the
    production encoder (`encode_frame` → `parse_frames`). Which surfaced something else — a harness
    named for frames had never once called the encoder, so `encode_frame` had no fuzz coverage at
    all. Frame coverage went 144 → 177 points when it did (#38).

219. **A canonical-text round trip cannot see a field the formatter does not emit.** Deleting the
    line in `format_command` that writes an INSERT's event time **passes** "format, reparse, compare
    the canonical text", because the field is then absent from both sides. That is precisely the
    defect #105 existed to fix: a write that asked to carry its own time and a write that asked for
    arrival time become the same write again. The property is `parse ∘ format == identity on
    commands`, so compare the **structures field by field**, with a `switch` that has no `default:`
    so a nineteenth command is a compile error rather than a silent pass (#38, #105, #107).

220. **A fuzzer's `-max_len` decides which branches are reachable at all, not just how fast.** A
    frame longer than 64 MiB cannot be *accepted* under a 64 KiB input limit — the payload never
    completes — so that branch has no positive test and its oracle has to be indirect: after a
    successful verdict, nothing left in the buffer may declare a length above the ceiling. Before
    asserting anything about a boundary, check whether a run exists in which the boundary is crossed
    the other way (#38).

221. **A mutation that does not compile is not a verdict about the harness. Reshape the mutation,
    never the code.** Turning `if (expected != base_hdr.checksum)` into `if (false)` leaves
    `expected` unused, and `-Werror=unused-variable` makes that a build failure; the table reported
    `DID_NOT_COMPILE`, which is honest and worth nothing. Appending `&& false` keeps the variable
    used and mutates the program. A table with one entry that is not a verdict still looks like a
    table (#38).

222. **A check that searches for `-fsanitize=undefined` as a substring never matches**, because
    clang is handed `-fsanitize=address,undefined` as a single argument. The first version of the
    instrumentation verifier reported **every** parser as uninstrumented in a build that was
    instrumented throughout — a failure that did not exist, and trusting it would have meant
    "fixing" healthy CMake. Sanitizer lists are parsed, not searched, and `-fno-sanitize-recover`
    has to be excluded: it changes what a finding does, not what is instrumented. The opposite
    blindness is worse — a missing file or an empty `compile_commands.json` reads as "no problems
    found" — so absence is a failure and the number of translation units actually inspected is
    printed (#38, #83).

223. **The count of required checks rotted for the third time, and only now has a mechanism.** The
    prose said "eleven" while twelve were required, then "twelve" while thirteen were, and #38 makes
    it fourteen. Nobody was careless on any of those days: **every** change added a context and
    **no** change recounted the sentence, which is the same one-directional rot as a status
    paragraph. `check_contexts.py` now derives the number from `master.json` and fails when the
    prose disagrees **or stops making the claim**. Anchored on the claim
    (`**N checks are required**`) rather than on the number word, because the same document says
    "the other thirteen are note-level tidiness" about CodeQL alerts — a word search would either
    fail on that sentence or be loosened until it failed on nothing (#38, #108).

224. **When a result is being discarded at seven call sites, the fix is `[[nodiscard]]`, not seven
    added `if`s.** Every `::fsync()` in `src/wal.cpp` threw its answer away, so `--fsync-policy
    every` acknowledged writes it had not synced — measured: eleven `EIO` returns and three
    `INSERT`s still answered `OK`. Making `flush()` and `sync()` return `bool` and marking them
    `[[nodiscard]]` turns ignoring the answer into a **compile error**, and each site then had to
    say what it does: the write paths and `FLUSH` throw, both snapshot paths throw, the flush tick
    throws into #112's boundary, and `close()` logs and completes — because a shutdown that throws
    is #112 from the other end. A comment asking callers to check would have been the same omission
    one layer up. The attribute also found **sixteen** sites nobody had looked at, five test files
    that flushed and dropped the result before reading the file back, so a failed flush made the
    *next* assertion report the wrong thing (#113).
225. **A failed `fsync` cannot be retried on Linux, so there is no recovery path to write.** The
    kernel reports the error once, to whichever caller happened to be there, and **marks the
    affected pages clean** — the next `fsync` on that descriptor returns 0 with the data gone. So
    the engine refuses the write rather than retrying it, and the counter and the ERROR line are
    sticky and monotone: they are the only lasting evidence that something acknowledged may not be
    on the disk. The second-order defect was in the old code's *bookkeeping*: it set
    `pending_sync_ = 0` after an unchecked `fsync`, so a writer coming out of a failed sync told
    the flush loop there was nothing left to do (#113).
226. **A counter published after the call that can throw stays flat in exactly the case it exists
    for.** `ob_wal_fsync_errors_total` was published after the WAL sync in the flush tick, under a
    comment reasoning that a tick which throws would publish on the next one. True of a transient
    failure. With `fsync` failing *every* time, every tick throws at the same line, so the number an
    operator reads never moved while the writes were being correctly refused. Publish before
    anything that can throw — and note which test found it: the regression test for the item itself,
    not review (#113).
227. **A checker can scan an API that does not exist, and that branch never matches.**
    `scripts/check_metrics.py` claims to prove every metric written by name is registered, and
    looked for `add_to_counter` — `MetricsRegistry` has `increment_counter` — while not looking for
    `increment_gauge`, which eight sites use. So a metric written only that way escaped the check
    entirely. Same shape as searching for `-fsanitize=undefined` as a substring (pitfall 222): the
    instrument answers "nothing wrong" in the same voice it uses when there is nothing wrong. A
    deliberately unregistered `increment_gauge` now fails it, which it did not before (#113).
228. **A document claiming a stronger default than the code is worse than one claiming a weaker
    one, and `docs/architecture.md` claimed it about durability.** "With `FsyncPolicy::EVERY` (the
    default)" stood in the paragraph headed *What the WAL guarantees after a crash*, while the code
    says `INTERVAL` in all four places it states a default. `docs/cli.md` opened the same section
    with an unqualified "is in a fsynced WAL record before the reply is sent" and put the caveat
    thirty lines below — a caveat under the claim is a caveat nobody reads. Both now name the
    policy in the sentence that makes the promise (#113).
229. **A mechanism written for one instance leaves the class open, and the rot moves to the
    document the mechanism does not read.** Pitfall 223 derived the required-check count so the
    prose could not drift — in `docs/github-security.md` only. One item later this file said
    "Thirteen" against a ruleset requiring fourteen: same rot, same week, in a file already in the
    tree. `check_contexts.py` reads **every** document that states the count now. And extending it
    hit use-versus-mention on the first run: the regex matched this file's own quotation of its
    anchor, inside backticks, in a checker whose docstring warns about exactly that. Code spans come
    out before matching, and the mutation table has a control that must **survive** — an added
    backticked mention (#113).

230. **A full filesystem reaches a growable memory mapping as `SIGBUS`, not as an `errno`, and that
    is why this engine does not memory-map a file for writing.** `ftruncate` extends a file
    **sparsely** and allocates nothing, so it succeeds on a nearly full filesystem; the allocation
    happens when the page is first touched, where there is no return value to check. Measured on an
    8 MB tmpfs: reserving 64 MB succeeded and left 67 108 864 apparent bytes, and writing into it
    died with `Bus error` — **exit 135**, against a control reserving 2 MB that completed. A signal
    is not an exception, so `run_thread_body()` (#112) cannot catch it and no `ERR` can carry it.
    #112 and #113 made a failing disk into a refusal the client is told about; adopting the mapped
    store would have reinstated process death for a full disk, in a strictly harder form. **The
    decision came from the failure mode, not from the throughput benchmark the item expected to
    need** (#114).
231. **A test suite on a component nothing calls describes the paths somebody thought about.**
    `MmapStore` had 165 lines of passing tests and was named as "the only thing keeping the
    component honest". Driven with #54's injector making the `ftruncate` inside `remap()` fail —
    skipping the one inside `open()`, which is the first the injector sees — its only growth path
    leaves the object broken three ways: `size()` still reports 4000 bytes into a mapping that is
    gone, `write_ptr()` returns **`0xfa0`** (`nullptr + 4000`, a pointer from a null base), and the
    next `advance()` **never returns**, because `remap()` zeroes `mapped_size_` and the growth loop
    doubles zero for ever (exit 124 at a 20 s timeout). None of the three is reachable without
    making a syscall fail, which is what the instrument is for (#114, #54 task A4.1).
232. **My probe had the defect twice before the measurement was worth anything, both times in the
    direction that reads like a finding.** First: the fault fired inside `open()` rather than
    `remap()`, because `open()` calls `ftruncate` too — so all three stages reported the same abort
    and none of them tested what they claimed (`OB_FAULT_SKIP=1` fixes it). Second, and worse:
    `advance()` returns the offset where the reserved bytes **start** and leaves the cursor past
    them, so `write_ptr()` afterwards points past the reservation — writing there is a **SIGSEGV**
    that is one digit away from the SIGBUS being hunted (139 against 135), and it killed the
    *control* too. The control failing is what said the probe was wrong rather than the code; a
    measurement whose control does not pass is not a measurement (#114).

233. **A duration that comes out exactly `0.00` is a measurement taken after the event, not a fast
    mechanism.** #54 stage B times how long a holder keeps the primary role after its coordinator
    vanishes. The first version measured it in a loop that ran *after* the loop watching the
    replica, so by the time it looked the step-down had happened twenty-five seconds earlier, and
    it reported `primary_step_down_without_coordinator_sec: 0.0`. Both observations are about the
    same window, so they belong in the same pass; the real figure is **2.28 s**, because the lease
    keepalive fails on the next tick. The guard is cheap and worth having: assert the duration is
    **greater than zero**, because a zero here can only mean the clock started too late.
234. **Counting lines in a test's assertion message measures the truncation, not the log.** I put
    `log[-2000:]` in a failure message — correct, a full node log in a traceback is unreadable —
    and then derived a WARN rate from that same captured output: **0.42 lines/s**, which is a
    property of the 2000 characters rather than of the engine. Measured properly, with a probe that
    reads the node's own log across the outage, it is **2.17 and 2.23 lines/s** on the two nodes.
    Five times the number. A rate computed from a buffer somebody chose the size of is not a rate
    (#116).
235. **An assertion on an `OB_LOG_DEBUG` line, against a server whose default level is INFO,
    matches nothing — and here the failing assertion *was* the finding.** Two of #54 stage B's
    three windows failed on `"rather than campaigning" in node_log`, the sentence in which a
    replica records that it is declining to stand for election. It is a DEBUG line and the default
    is INFO, which this file's own logging section warns about. The test was wrong; so is the
    engine, and for the same reason: if the line is invisible to the test at the default level it
    is invisible to an operator, which is #115. The fix on the test side is a harness that can ask
    for the level it needs (`ClusterManager(log_level="DEBUG")`, affecting one module rather than a
    shared session cluster), and on the engine side an item. **When an assertion cannot see
    something, ask whether the operator can.**

236. **Grepping for a path finds the comments that mention it, not the code that reads it.** Asking
    "which tests read `docs/operations.md`" with `grep -l 'docs/'` over `tests/*.cpp` returned five
    files, and I re-ran all 63 of their tests after editing that document — on the strength of a
    match inside sentences like *"the way `docs/operations.md` tells an operator to make one"*.
    **Exactly one** C++ test reads a document at runtime: `CliConfigStatic.`
    `EveryKnownFlagIsInTheCliReference`, over `docs/cli.md`. The derivation that answers the
    question is the file-reading call (`read_source(...)`, `OB_SOURCE_DIR`), not the string. Use
    versus mention, fifth time in this workspace, and this time it put a false clause in a commit
    message — corrected in #113's entry rather than rewritten, because the history was pushed.

237. **A check a cross-reference satisfies is worse than no check, because the next reader trusts
    it.** #115 is about the *level* a decision is logged at: the replica's refusal to campaign was
    `OB_LOG_DEBUG` against a default of INFO, so an operator saw it zero times. The test proving
    the engine can say that sentence counted `staying REPLICA rather than campaigning` — which is
    in the INFO line **and** in the per-tick DEBUG line beside it. A mutation rewording the INFO
    line **survived**, satisfied by the DEBUG one, so the check could not decide the thing it
    existed for. The counted phrase is now a clause belonging to the INFO line alone and the
    runtime count is exact rather than "at least one".
238. **A phrase a node writes on one log line does not exist contiguously in the source.** C++
    implicit concatenation breaks it: `"…so this node is staying REPLICA "` then
    `"rather than campaigning…"`. A grep for the whole phrase never matches, so a check asking
    "can the engine say this" passed by **finding nothing**. The reader joins adjacent literals the
    way the compiler does — `re.sub(r'"\s*"', "", source)` — with the limit named: it is not a
    preprocessor, it does not expand `%s`, and a quote inside a comment would fool it. This
    workspace had the trap recorded on the *mutating* side (a one-line edit inside a concatenated
    string is not a mutation); this is the first time on the **searching** side. `STEP_DOWN_REASONS`
    had passed the same check without the joining only because those phrases each fit in one
    literal, which is why the list that failed is the useful one.
239. **When a long background run dies under memory pressure, try a short one before blaming your
    workload.** This machine had ~2 GB genuinely free with 8.9 GB in page cache, and its guard
    killed the full integration battery (48 tests in), then a **single-test** run, then even the
    CI-polling loop — while `MemAvailable` read 8.5 GB throughout. The single-test log was **zero
    bytes**, so the process had not allocated anything: the kill was about the machine, not the
    test. `ctest` then passed in **five** `-I` ranges of about 100 s each (379 + 179 + 160 + 160 +
    146 = 1024), which is the same suite in the same order with `-j1` inside each range. Say
    plainly what did **not** run locally rather than leaving it to be inferred — the integration
    battery's verification for #115 and #116 is CI's, and the PR says so.

240. **A registered metric nobody writes reads as zero, and the check that exists walks the other
    way.** `scripts/check_metrics.py` proves every name handed to `increment_counter` and friends is
    registered; a registration with no write is invisible to that direction, and the script's own
    docstring said so. Five metrics were in that state, one of them
    `ob_mm_replication_lag_bytes` — described as "Replication lag in bytes (max across peers)",
    which is exactly the signal #54 stage C went looking for. An operator alerting on replication
    lag was alerting on a constant. The reverse scan is in that script now, with an allowlist
    naming #117. **It is deliberately looser than the forward one**: three subscription counters
    are written through a local `publish(name, …)` lambda, so a strict scan called them dead — for
    finding *dead* metrics, "the name appears as a literal anywhere in `src/`" is the right
    direction to be wrong in, because it can miss a mention but never a real write.
241. **`SELECT`'s column header is a line, and a filter that drops only the `OK` counts it as a
    row.** `rows()` in the stage C tests stripped the `OK` and kept
    `timestamp_ns<tab>price<tab>…` — so a node holding **nothing** returned a one-element list,
    which is truthy, so `assert rows(peer)` ("the mesh replicated something") could never fail, and
    every row count in the file was one too many. An `assert len(rows) >= 4` meant to check four
    writes was satisfied by three. Pitfall 110's shape in a new place, and the header is now pinned
    against `src/response_formatter.cpp` — **in the source's own spelling**, because a tab there is
    two characters and the first version of that check compared real tabs against a file with none.
242. **A fault injector's own control tests are what make the tests behind it mean anything, and
    mine found two defects before a single engine test ran.** The mesh proxy forwarded bytes across
    a partition, because the fault was checked **before** `recv` and the pump was already inside a
    0.25 s call when the partition arrived. And it delivered a byte-budget remainder on the very
    next iteration, because the flush condition asked only "not partitioned". Both are
    unexpressible now rather than fixed: everything read goes into the withheld buffer and only an
    allowance leaves it, so order is preserved by construction and one function decides how much
    may pass. Five mutations, four killed, one control surviving.
243. **A fault that depends on which end dialled is a fault a test cannot aim.** The proxy's byte
    budget was spent only on the client-to-target direction, and in a symmetric mesh the surviving
    link is whichever end connected first (#96), so the writes under test travelled the direction
    the budget did not cover and `held_bytes()` came back 0 — which reads as "the budget let
    everything through". The budget is a property of the **link** now. The cost is named rather
    than hidden: a heartbeat going the other way spends it too, so the cut arrives sooner than
    asked, which is the safe direction for a fault to be wrong in.

244. **A difference of two positions is a lag only when both index the same log and the subtrahend
    is kept current — and the same expression is correct one call site away.** `Engine::stats()`
    computes a replica's lag as `wal_.current_offset() - r.confirmed_offset`, which is right: the
    replica streams *our* WAL and acknowledges into that field on every `ACK`. Six lines later it
    computes a mesh peer's the same way, and `multi_master.hpp` says in the file that declares the
    field that those are positions in the peer's **own** WAL and that comparing them with ours was
    #61. It is also written in exactly one place, `process_handshake()`, so it is frozen at connect
    time. Measured (`scripts/measure_mesh_lag.py`): on a mesh converged by row content the printed
    `replication_lag_peer_<id>` equals this node's own WAL offset **to the byte**, three phases
    running. This is #79's `shutdown()`/`take_result()` shape — two functions, one shape, one of
    them broken — so the rule has to be unconditional rather than a note beside the good one.

245. **A field whose name states a quantity will be read as that quantity, and the name is the part
    that ships.** `MM_PEERS`' column is called `lag_bytes` and holds `peer.send_buf.size()`; the
    Python client parses it to an int and `docs/python.md` called it "replication lag in bytes".
    Nothing is wrong with the number — it is a correct queue depth, and a useful one — but every
    reader of that page writes the alert its name implies, and on a healthy link it reads zero
    right up to 4 MB of writes that have not left (#117). The same list documented `status` as
    `"active"/"joining"/"leaving"`, which is the peer **registry's** vocabulary; the column emits
    `connected`/`disconnected`, so a client testing for `"active"` tested for a value the server
    never sends. Both found by reading the code that fills the columns, not the page (#118).

246. **A sixteen-bit counter that breaks ties wraps, and wrapping it makes the clock go
    backwards.** The HLC's `logical` field is `uint16` because the wire format says so — three
    `static_assert`s pin that layout — and `static_cast<uint16_t>(last_.logical + 1)` was silent
    about it. Measured: with the physical component pinned an hour ahead, three regressions per
    200 000 local ticks, the first at tick 65 533, against a control of **zero** over the same
    200 000 on an unskewed clock (#119). The answer is to carry the overflow into the physical
    component, not to saturate the counter: saturating stops the reversal and **silently stops
    breaking ties**, which a mutation demonstrates — it kills the strict-inequality test and leaves
    the monotonicity test green. That pair of tests is the whole defence, and only the mutation
    shows the second one is load-bearing.

247. **`max()` over a value that arrives from the network is a value the network chooses, and
    `max()` is never undone.** `tick_receive()` stores `max({now, last_, remote})` and
    `src/multi_master.cpp` hands it the frame's timestamp **ten lines** after deserialising it,
    with nothing in between. So one record from a peer whose clock is ahead pins this node's clock
    ahead for the life of the process — and since `apply_delta_mm()` stamps our own writes from
    that clock, the value leaves on our records and every peer adopts it too. Consistent, and
    permanently wrong about real time (#121). The general form: when a remote value feeds a
    monotonic accumulator, ask what bounds it, and if the answer is nothing, ask whether the
    accumulator is supposed to mean anything.

248. **A guarantee whose state cannot be read from outside is a guarantee on someone's word, and a
    log line is not readable state.** #120's fix collapses a per-write WARN into two lines per
    excursion, and C++ tests have no log sink — so the property "many events, two lines" was not
    assertable at all until the clock exposed both numbers (`drift_excursions()` counts
    occurrences, `drift_episodes()` counts loud lines). The pair is also what an operator needs:
    `ob_mm_hlc_drift_ns` is a peak that never comes down, so a ten-second blip and an hour of skew
    read identically once the blip is over. When the fix is "say it less", the number of times you
    said it becomes part of the contract.

249. **A metric fed the wrong number is worse than one fed nothing, because a flat zero is visibly
    broken and a plausible number is a second opinion.** The io_uring loop incremented
    `ob_iouring_sqe_submitted` by the **completion** count, so it equalled
    `ob_iouring_cqe_processed` by construction: an operator comparing the two to find a backlog
    compared a number with itself, and both moved, and neither was wrong-looking. The real number
    was two lines away in a return value nobody captured — `io_uring_submit()`'s, whose negative
    case also means the prepared entries stayed in the ring and this transport made no progress
    (#113's shape, different file). When a metric has an obvious partner, check they cannot be the
    same expression.

250. **`total - published` is correct only while the source outlives the publisher, and that is a
    property of the object rather than of the pattern.** Three engine counters are owned by
    objects with no registry — the WAL writer, the hybrid logical clock, the replication client.
    The first two live as long as the engine, which is the only reason the subtraction was ever
    right; `repl_client_` is **rebuilt on every role change**, so its total restarts at zero and a
    bare subtraction reads that as "nothing happened". The metric then freezes from the restart
    until the new object passes the old total — the exact window an operator is watching, a node
    that has just become a replica and is catching up. One `counter_delta()` in `metrics.hpp` now,
    where a unit test can state the restart case without a cluster.

251. **When the instruction count of the function you changed goes *down*, look for the new
    symbol before you report a saving.** Extracting the WAL's accounting made `write_record` read
    220 → **212**, eight fewer, while the extracted function was a real 16-instruction call — so
    the true cost was **+8**, and the drop was relocation. Defining it in the header removed the
    call and left **+3**, which is the counter itself (a relaxed load, an add, a relaxed store),
    with the archive's `lock`-prefixed instruction count unchanged at 19. Second instance in this
    repo: `apply_delta` reads 503 → 3 because its body moved to `apply_delta_impl`. The tool warns
    about this in its own docstring and the warning is not enough — the check is
    `nm | grep <new symbol>`.

252. **A text assertion about a call site must read the arguments, not the statement, because the
    callee's name contains the words you are looking for.** The static test that stops
    `ob_iouring_sqe_submitted` being fed the completion count asked whether the statement mentioned
    `count` — and matched `increment_counter`. Same family as a denylist finding `rds` inside
    `records` and as `"disconnected".count("connected") == 1`, committed here **in the check
    written to catch it**. Take the text after the first comma, not the text of the call.

253. **A harness that refuses a mutation whose source did not move cannot see one whose output did
    not.** Proving that the WAL's record count belongs in the shared accounting needed a mutation
    that gives one write path its own copy without the counter. The first attempt guarded the
    counter on `total != sizeof(WALRecordV2)`, which is **always true** because `total` is header
    plus payload — a non-mutation that survived and read as a gap in the tests. The rule was
    already written down and the guard implements only half of it; the other half is asserting the
    *behaviour* changed, which no textual check can do for you.

254. **"Filed rather than fixed" stops being available once your own change makes the defect
    reachable.** Reading the code showed `repl_client_` read under `mtx_` by `stats()` and written
    without it by `promote_to_primary()`, and filing it (#122) was defensible on the grounds that a
    change about metrics should not touch a failover path. Then
    `sanitizers-integration (tsan)` went red on that very PR, naming
    `unique_ptr<ReplicationClient>::reset` **and** `operator delete` — so it is a use-after-free
    window rather than a torn read, which reading had not established, and the new publisher is
    what exposed it: `stats()` runs on a `STATUS` command, the flush tick runs every interval. The
    general form: when you add a **reader** to shared state, the frequency you add it at is part of
    the change, and a latent race is a race whose probability you just multiplied.

255. **Before deferring a fix, look for the same operation done correctly elsewhere in the file.**
    The answer to #122 was ten lines further down: `demote_to_replica()` already took ownership
    under the lock and destroyed through a local, with a comment saying "for the reason above" —
    about a reason the other site was breaking. Two call sites of one idiom, one of them wrong, and
    the correct one carrying the explanation. That is the same asymmetry as #79's
    `shutdown()`/`take_result()` pair and as #118's two subtractions, and it means the cheap first
    question is not "how do I fix this" but "does this file already do it right somewhere".

256. **A list of names you maintain by hand is a check in one direction only, and the silent
    direction is the one that matters.** `MetricsRegistry.EveryMetricTheEngineWritesIsRegistered`
    held the names the engine writes, with a comment saying the list-ness was deliberate: "so
    adding a metric means adding it here too". Nothing enforced that. #118 removed a registration
    and the test broke loudly, which is the easy direction; a metric the engine writes and nobody
    added would never have been noticed. Deleted rather than repaired, because
    `scripts/check_metrics.py` does the same job mechanically **in both directions** in a required
    job, and a second copy of a guarantee cannot be mutated separately from the first. Same lesson
    as #32, where a hand-written list of value-less flags produced a feature built on a false
    premise.

257. **Removing a field beats zeroing it, because the compiler finds the copy sites a grep
    misses.** Taking `mm_replication_lag_per_peer` out of both structs rather than leaving it empty
    turned the third assignment — in `tcp_server.cpp`, a file this change had no reason to open —
    into a build error. Two sites were where the search said they would be; the third was a copy
    between two structs of the same shape, which is exactly what a name-based search reads past.

258. **Before publishing a number, check whether *your own* new number has the defect you are
    removing.** #118's fix was three parts, and the third — publishing the replica lag, the one
    genuine number in the area — stopped at reading the expression: it ignores the WAL file index,
    and `rotate()` resets `current_offset`, so a replica more than a file behind reports **zero**.
    Publishing that in the change whose subject is a lag that reads the wrong thing would have been
    the same defect in a new name, so it is #123 and the gauge stayed unpublished. The general
    form: a fix that adds a metric should ask the question it just asked of the old one.

259. **When zero is a real answer, "cannot be measured" needs its own number — a sentinel says the
    opposite of the truth.** A replica that is caught up is zero bytes behind, so the same zero
    cannot also mean "a WAL file between its position and ours is gone". Three lags in this engine
    now report as a **pair** for that reason: `ob_mm_replication_lag_records` with
    `ob_mm_peers_position_unknown`, `ob_replication_lag_bytes` with `ob_replicas_lag_unknown`, and
    the drift gauge with its excursion count. In each case the second number names a condition the
    first cannot express, and in the replica case it is the **more urgent** of the two: retention
    keeps files back to the slowest connected replica, so a missing one means that replica cannot
    catch up from this log at all.

260. **A formula that is accurate only at production settings is a formula no test can check.**
    The cross-file WAL distance could have been estimated as `files * rotate_threshold`, and closed
    files really are at least the threshold — rotation waits for it, and a restart appends rather
    than starting a short file. At 512 MB the error is one record, 0.005%. At the 512-byte
    thresholds the tests use it is a quarter of a file. Asking the filesystem for each
    **intervening** file size is exact, costs nothing because there are normally none, and is the
    same answer at every threshold, which is what makes the unit tests mean something.

261. **A test written to reproduce a defect needs an assertion that it still does.**
    `WalDistance.APositionLateInTheEarlierFileReadsAsZeroBehindTheOldWay` computes the old
    expression beside the new answer and asserts **the old one says zero**. Its first version took
    the position of the *first* record in the earlier file, whose offset is 0 — the smallest
    possible subtrahend — so the old expression answered 136 and the test proved nothing while
    passing its real assertion. The clamp needs a position **late** in the earlier file. Without
    the control the test would have been green, named after a defect, and blind to it.

262. **`str.replace()` without asserting the match count is a silent no-op, and in a script that
    edits a mutation harness it produces a verdict about code you never wrote.** Two "corrections"
    to #123's table did nothing, so the table kept reporting `DID_NOT_BUILD` for a mutation whose
    text was still the original — and the original really did leave a variable unused under
    `-Werror`, so the message was true about a mutation I thought I had replaced. Half an hour went
    into reconciling that with a manual run that built cleanly, because the manual run used the
    replacement text and the harness used the old one. It is the same family as a mutation that
    does not change the program, one level up: **the tool that edits the tool needs the same
    assertion the tool has.**

263. **A harness that refuses a verdict has to print why, or the refusal costs more than it
    saves.** `DID_NOT_BUILD` with no compiler line is indistinguishable between a real `-Werror`
    and a transient failure this repository has hit before (two `cmake --build` in one directory,
    `Text file busy`). Adding four lines of the build output to the refusal turned the case above
    from a guess into a fact in one run. Same rule as the earlier lock-file guard: an assertion
    that declines to answer must name the evidence.

264. **How many WAL files are on disk is a retention fact, not a rotation fact.** The first
    assertion in #124's integration module was "fifteen hundred records must leave at least three
    WAL files", and it failed against a primary that had rotated three times: it held
    `['wal_000002.bin', 'wal_000003.bin']`, because the replica confirmed into file 2 while the
    writing was still going on and the flush tick freed what was below. For "did it rotate", read
    the highest index or `ob_wal_file_index`; for "which files are still here", list the directory.
    Two questions, two instruments.

265. **A replica refuses `FLUSH`, so its own flush interval decides when what replication delivered
    becomes queryable.** `FLUSH` is answered `ERR read-only replica`, and rows reach a `SELECT`
    through segments, so a replica's tick is the only thing that materialises them. The first
    version of #124's catch-up test set a ten-minute interval to stop retention from freeing the
    files it needed — and read **zero** rows out of a replica that had received every one of them.
    The fix was to stop retention with a mechanism instead of a timer: a second replica, stopped,
    pins `safe_truncate` in place. A configuration chosen to disable one thing in the flush tick
    disables everything in it.

266. **A floor that protects an operator does not belong in the component the operator is not
    typing at.** `--wal-rotate-bytes` refuses anything below one maximal record, because below that
    a single write fills a file and the directory grows one file per record. `WALWriter` deliberately
    does **not** enforce it: its unit tests drive a 512-byte threshold so rotation is reachable
    without writing megabytes, which is a reasonable thing for a component test to do. The
    distinction is who chose the number, and it is written next to both.

267. **A refusal is only the documented one if the boundary itself is accepted.** The floor and the
    ceiling are tested from both sides — one byte under refuses, the value itself parses — because a
    refusal that is off by one is a different refusal from the one `docs/cli.md` prints, and the
    error message quotes those two numbers.

268. **A stopped process does not act on `SIGTERM` until it runs again.** A test that leaves a node
    under `SIGSTOP` — including one that fails between the pause and its `finally` — would make
    teardown wait out its five-second grace and take the `SIGKILL` path, which is #106's shape from
    the other side: the harness silently stops exercising the graceful exit and the report gains a
    `signal 9` nobody asked for. `_stop_node()` sends `SIGCONT` first, unconditionally and with no
    record of which nodes are paused: `SIGCONT` to a running process is a no-op, and a set that is
    wrong in the direction of "not paused" costs exactly the five seconds this avoids.

269. **A checksum over a document the receiver cannot reconstruct is a checksum that can only
    fail.** `SNAPSHOT_END` carried `crc32c(manifest.to_json())`, and `to_json()` serialises
    `created_at_ns` and `total_rows` — two fields the wire never sends. The receiver rebuilds the
    manifest with both at zero, so against a real primary the comparison failed **every time** and
    the replica retried for ever (#125, fixed). The direction of the rule matters: before comparing
    two digests, ask whether both sides can produce the same bytes. The fix is one
    `transferred_digest()` both ends call, rather than two call sites each choosing which fields to
    include — which is how the two definitions came to disagree in the first place.

270. **A stub built to agree with the code under test proves that the code agrees with itself.**
    The only test of that bootstrap path uses a mock primary that constructs its manifest from
    exactly the four fields the wire carries, leaving the other two at their default zero — which is
    what the receiver reconstructs. Its own comment says it has to "build the same manifest to name
    it". It stands in for the *sender*, so it has to be built like the sender; the fields a real
    primary fills in are the whole defect.

271. **A pause long enough to observe anything outlives the socket timeout, so a `SIGSTOP` test is
    a disconnect test whether it means to be or not.** #124's retention test needed a flush tick and
    four scrapes while the replica was stopped; by the time it resumed, the replica's `read_line`
    had timed out, it reconnected, and in that window retention advanced past its saved position.
    Every clause the test was written for passed; what followed belonged to a different item. Do not
    shorten the pause to fit under the timeout — the margin is against a number you do not control,
    on a runner three times slower under a sanitizer.

272. **`shutil.copy2` preserves the mtime, so a source restored with it is older than the object
    file built from the mutant — and the build rebuilds nothing.** #124's mutation harness kept a
    pristine copy alongside (the rule that exists because `git checkout <path>` has eaten
    uncommitted work here four times) and restored with `copy2`. Every verdict after the first
    restore was therefore measured against a binary that still carried an earlier mutation, and the
    restored tree was reported as not green. Restore with `copyfile` plus an explicit `utime`, or
    `touch` afterwards. **What caught it was the control**: the one mutation in the table that had
    to survive came back KILLED, which is the whole argument for having it — a table where
    everything dies reports a broken instrument as diligence.

273. **A safety net registered after the thing it protects is not a safety net.**
    `ClusterManager.start()` ended with `atexit.register(self.shutdown)`, and `shutdown()` began
    with `if not self._started: return` — both halves conditional on the start having finished. So a
    cluster whose start failed halfway (a node refusing its arguments, a port taken, etcd slow to
    answer) leaked everything it had already launched. Measured twice: #124's mutation run left
    **four** orphan etcds holding ports and memory, and three more were found on 7 September with
    uptimes over a day, blamed then on a killed run rather than on this. Registered in the
    constructor now, and the guard is "is there anything here" rather than "did it start". Checked
    both ways with a probe that starts a cluster whose flags are refused: one leaked etcd before,
    none after.

274. **A fixture built for a test nobody then writes is a knob nothing turns.** `narrow_proxied_mesh`
    put a 4 kB `SO_RCVBUF` on the accepted sockets, on the expectation that it would bring the
    engine's own send queue within reach in kilobytes. It does not — TCP holds unsent data in the
    **sender's** buffer, which is the engine's socket and not anything a proxy can narrow — and the
    same session measured that. The fixture stayed for two more items with **no users**. Deleted
    with the measurement written where it was, because the next reader would otherwise reach for it
    for exactly the reason it does not work.

275. **A test whose precondition is a race is a flaky test, even when the race is usually won.**
    The #125 bootstrap test first paused its replica with `SIGSTOP`, which leaves it *connected* —
    so retention cannot pass its position, and the refusal the test needs depends on a flush tick
    landing in the gap between the resume and the reconnect. It passed, and it would have failed on
    a loaded runner. `kill_node` makes every step observable instead: the primary reports zero
    replicas connected, the file the replica confirmed is gone from the directory, and only then is
    the replica allowed back.

276. **A response here ends with a blank line, so reading one line per command reads the next
    command's reply as its own.** `format_ok()` returns `"OK\n\n"` and the banner is
    `"OK ob_tcp_server v0.1.0\n\n"`. A write helper that read a single line got `''` for its first
    `MINSERT` and reported it as a refused write. And the reason it was reading lines at all is
    worth keeping: the module's `raw()` reads until the socket goes **quiet**, which costs its full
    3 s read timeout per call — right for one command, and 129 s for the forty-three a volume test
    needs, which is how it hit pytest's limit before injecting any fault.

277. **A test about what replay can reach has to keep everything that writes a checkpoint away from
    it.** #126's measurement read **1** stranded record instead of 2 on its first run, because the
    node ran with `--flush-interval-ms 500` and a tick had moved a row into a segment and written a
    checkpoint — and replay begins after the last checkpoint, so the row was rescued by timing
    rather than by the WAL. The flush interval goes an hour out and the node is `SIGKILL`ed rather
    than stopped, because a clean stop ends in a checkpoint too.

278. **A pointer into a byte buffer is not a `const Level*`, and the difference is invisible until a
    sanitizer sees it.** Every receive path had
    `reinterpret_cast<const Level*>(payload + sizeof(DeltaUpdate))`, and a mesh frame puts the
    payload **42** bytes into the receive buffer (a 4-byte length, a 38-byte header) and the levels
    **130** bytes in — neither a multiple of `alignof(Level)`. UBSan: *member access within
    misaligned address … for type 'const struct Level'*. It works on x86, which is why it survived
    every mesh test this repository has; the standard lets a compiler assume the alignment it was
    promised. Fixed as a class through one `levels_from_payload()` that copies into a caller-owned
    scratch buffer (#127).

279. **Four sanitizer regimes can each cover most of a path and leave a hole between them.** The unit
    tests call the apply path with a real `Level` array, aligned by construction. The integration
    battery drives real frames through real sockets, but its sanitizer job is **TSan**, which does
    not check alignment. The ASan/UBSan job builds the C++ suite, and until #54's D3 nothing in that
    suite took a delta **off a socket**. The fuzzer reads arbitrary bytes under UBSan, but drives
    `parse_frames` rather than the apply path behind it. When asking whether something is covered,
    ask which *build* covers it, not which test.

280. **Measuring a fix's cost can contradict the reading of it in both directions at once.** The
    first version of #127's copy called `scratch.resize(n_levels)` per record, which
    value-initialises — `Level` has a `_pad{}` member initialiser — so it zero-fills bytes the
    `memcpy` overwrites. Growing only when the buffer is too small saved **three** instructions out
    of 205. The resize was not the cost; the copy was. And the apply path, which is where the reading
    expected a difference, came out **identical instruction for instruction**.

281. **A recovery that needs the thing that just broke is not a recovery.** #126's obvious fix was a
    "tear" record so replay could step over the abandoned bytes — and writing it means *another
    write to the file that just refused one*. The same objection killed it twice over: `rotate()`
    could not be reused either, because its first act is a ROTATE record **into the file just
    established as unwritable**. What works is the pair that needs nothing from the broken file: open
    the next one, and teach the reader that a mismatch in any file but the last is a tear.

282. **A fix's condition is as much of the fix as its action.** Abandoning a file on **every** failed
    write would give a disk that stays full one empty WAL file per refused write — a pathology the
    fix would have introduced. The tear condition is exactly `remaining < total` at the failure
    site, which the writer already knows, and the disk-stays-full test now asserts the file count so
    the condition is a tested property rather than a comment.

283. **A descriptor number is an identity only until it is closed, and an epoll set hands it back to
    you afterwards.** `MultiMasterManager::io_loop()` armed its registrations with `ev.data.fd` and
    dispatched on that number. `epoll_wait()` harvests before the loop takes `mtx_`, and three other
    threads close peer sockets under that lock — `handle_topology_change()`, `check_backpressure()`
    on the client write path, and the reconnect loop — so a harvested number can belong to anything
    by the time it is dispatched. It belonged, once, to **the client port's epoll instance**, created
    by `epoll_create1()` in `TcpServer::run()` right after `Engine::open()` started the mesh threads;
    the loop's branch for "a descriptor with no record behind it" closed it, and the node then refused
    every client connection. Registrations carry `conn_id` now (#128). The rule was already written
    down twice in this tree — about `pending_` and about subscriptions — and applied to a container
    both times.

284. **When a subsystem dies for no reason it can explain, ask who else could have closed its
    descriptors.** The symptom was nine integration tests failing with `Connection refused` against
    nodes whose mesh was up, and nothing in any node's log but a mesh warning about an unrecognised
    descriptor. The only reason it was diagnosable at all is that ThreadSanitizer tracks file
    descriptors and reports the **creation site**: without that line the report reads as a race on an
    address in a subsystem that never touches the other one's memory.

285. **A defensive close is a bet that the thing you cannot identify is still yours.** The branch
    closed an armed descriptor with no record behind it, on the argument that such a descriptor must
    be an orphan. Both halves of the argument were wrong: the number may not be ours any more, and
    under `EPOLLET` an orphan may produce no further event to be closed on — so it never was reliable
    leak protection. Removing the close trades a leak nobody has measured for a close of another
    subsystem's descriptor that has been.

286. **A helper that returns a pointer into a reference parameter is a trap the moment something
    nearby returns by value.** `MultiMasterManager::peer_states()` hands back a vector; the test
    helper beside it takes `const std::vector<PeerConnection>&` and returns a pointer into it. So
    `find_peer(mm->peer_states(), 2)` compiles, reads well, points into a vector that dies at the end
    of the expression — and **passes**, because the freed memory still holds the right values. Four
    local runs and five repetitions were green; `sanitizers (tsan)` called it a heap-use-after-free.
    The rvalue overload is deleted now, so the shape is a **compile error** instead of something a
    sanitizer has to be running to see.

287. **A `sleep_for` in a loop a `stop()` joins is shutdown latency, and it is the third time in
    this tree.** `PeerRegistry::lease_loop()` slept `max(1, lease_ttl/3)` **seconds** in one call, so
    shutdown waited out whatever remained of it: measured **2.94 s** at the default TTL and
    **4.02 s** at a 30 s one, against **0.22 s** for a node with no lease loop. The fix and its
    explanation were already in the tree — `Engine::flush_loop()` waits on a condition variable with
    the stop flag as the predicate, under a comment saying `join()` cannot interrupt a sleeping
    thread — and the mesh's `wakeup_fd_` comment records the second occurrence. Notify **after**
    storing the flag and **before** the join: the predicate reads the flag, so a notification that
    arrives first is one the waiter sleeps through.

288. **When two published numbers about the same thing disagree, the gap is the defect.** #106
    measured `SIGTERM` at 0.11 s for a node with nobody connected, and the integration harness
    escalates to `SIGKILL` after five seconds. Anything between those two is invisible: a mesh node
    taking three seconds to stop was inside the harness's tolerance and outside the published figure,
    and neither number was wrong. Go and measure the configuration that falls between them.

289. **A measured count and the run that measured it have to name the same tree.** The test-suite
    table cited PR #124's CI run beside a C++ count of **1083**, and #124's tree had **1082** — the
    count came in with #129 and the citation did not move with it. Nothing was wrong with either
    number; what was wrong is that together they describe a tree that never existed. The row exists
    to be a measurement, so the citation moves in the same commit as the count or the row is prose.

290. **An exception boundary belongs where the unit of work is, and for an epoll loop that is one
    event, not one pass.** Every mesh registration is `EPOLLET`, so an event the loop abandons is
    **not re-delivered** — taking the whole pass down on a throw would silently drop the rest of a
    batch that can hold 64 (#112).

291. **A "loud once" episode that closes in the same pass it opened is not an episode.** The
    recovery line ran after the event loop, so a pass containing a throw reported the failure and
    then announced recovery immediately: the log alternated ERROR / "handled again" for three
    failing records. Gate the recovery on a pass that had events **and** none of which threw. And
    do not over-suppress while fixing it: #95's shape is a line per *loop iteration* carrying no
    new information, whereas one line per failed record is information.

292. **Wrapping a long loop body in `try` is a whitespace commit, and it can be proved.** The mesh
    event body is 248 lines with five `continue`s and four `break`s, and the breaks belong to
    *nested* loops — so extracting a function would need each one classified by hand. Wrapping in
    place with a `catch` that rethrows makes `git diff -w` show **only** the lines added, which is
    a proof rather than an assurance; the handling then arrives as its own readable diff.

293. **Before writing a boundary, measure which thread actually does the work.** #112 predicted
    that an `ENOSPC` during promotion would end the monitor thread. For the **startup** promotion
    it does not: that runs on the main thread inside `TcpServer::run()`, so the throw reaches
    `main` and the process exits with the reason named — already the right shape. Only the
    promotion after a failover runs on the monitor thread, and that one leaves the node reporting
    `REPLICA <its own replication port>` for ever.

294. **A paragraph asking the next person to keep two numbers in step is not a mechanism — and I
    proved it in the commit that added one.** Pitfall 289 was written because the test-suite table
    cited one CI run beside a count from another. The commit that recorded that lesson then left
    the citation on PR #125's run beside an integration count of **265**, which #125's tree did not
    have. Nothing offline can verify counts measured on someone else's machine; what can be removed
    is the possibility of updating one half without the other, so both halves now come out of
    `scripts/test_table.py <pr>`. Whose own first version printed `17 in 0:00` for a job that ran
    265 tests, because `re.search` returns the **first** match and pytest's verdict is the last.

295. **A boundary that saves a thread does not save the state the thread was halfway through
    changing.** #112's per-iteration boundary made the failover monitor survive a refused `EPOCH`
    write — counter, one ERROR, a recovery line — and the node still answered `ROLE` with
    `REPLICA <its own replication port>`, because the promotion had already taken the leader key
    when the write was refused. Fixing the thread and fixing the half-finished transition are two
    defects; the second is #130, and writing failover semantics inside a `catch` would have hidden
    it in the least-reviewed code in the subsystem.

296. **A rewriting script must assert its own postcondition, not report its match count.** A regex
    pass over nine `make_unique<FailoverManager>` sites printed *rewrote 8* and changed none of
    them; the link error found it two builds later. Counting `->registry())` occurrences before and
    after, and then re-scanning for sites that still lack the argument, is what caught it. Same
    family as "assert the source **and** the output changed" from the mutation harness: a tool's
    own report of what it did is not evidence that it did it.

297. **Mutate past the logging, not before it, or the mutation kills the wrong assertion.** Putting
    `throw;` right after the counter made the boundary skip its own log lines, so the test failed on
    "the failed tick was not reported" — true, but it says nothing about whether the *thread*
    survived. Counting, logging and **then** rethrowing kills the assertion that carries the
    guarantee: *no tick ran after the one that threw*. Place the mutation so that only the property
    under test changes.

298. **A per-iteration boundary can create a busy-spin that the missing boundary hid.** The
    replication run loop's `epoll_wait` timeout is a variable that outlives one pass and is **zero**
    whenever a catch-up cursor has queue space. While an exception took the thread with it there was
    no second pass to spin; add a boundary and a repeatedly throwing pass becomes
    `epoll_wait(..., 0)` in a loop. Measured with a constructed throw, i3-7100U, two 5-second
    windows: **172 000–195 000 passes/s and a whole core** without the floor, **10/s and 0.01 s of
    CPU** with it. Ask what the loop's *pacing* inherits from the pass that failed, and make the
    failure path go through the same function as the idle path so it cannot inherit anything else.

299. **The list of loops that "cannot survive stopping" is not evidence about the code either.** A
    mutation deleting one row from that hand-written list survived: the rule covered less and stayed
    green. Derived from the tree, `src/` has thirteen loop functions and six of them guard an
    iteration — #112 had named four. Same shape as the thread-entry count that was wrong twice in
    the same item. Derive the set, keep the list only for what each member *is*, and check both
    directions so a row naming a function the tree no longer has fails too.

300. **Two components logging the same condition each look reasonable alone.** A lease etcd had
    forgotten produced eleven WARN lines from the peer registry and eleven from the coordinator
    client in 33 s — one per refresh interval each, and neither component was writing more than one
    line per attempt. Count the lines the *condition* produces, not the lines a function produces.

301. **`LogEpisode` is not thread-safe, and the fix is to hold a lock, not to write a second
    mechanism.** Its own header says it expects its users to already hold a lock over the decision
    that consults it. `CoordinatorClient` is used from more than one thread by design (#71), so its
    episode has its own mutex — deliberately not the one held across the HTTP round trip.

302. **Before filing "no measured throwing path", go and read every call the loop can reach.** For
    `run_loop` that was: every `throw` in the file is on the startup path or in the other class,
    `pread` short reads are handled by moving to the next file, the one allocation sized from disk
    is bounded by a `uint16_t`, and the TLS context is built before the thread starts. Four
    sentences, each checkable — which is a different claim from "I did not find one".

303. **A script that wraps a code region must be given the line *after* it, not its last line.** The
    end of a loop body is a run of closing braces, so an anchor on one of those matched the wrong
    `}`: the wrapper reported success after wrapping six lines of a ninety-five-line poll. The line
    that follows a region is unique prose; the line that ends it is punctuation shared with every
    other block in the file.

304. **A boundary is not a substitute for owning a resource.** `MetricsServer::handle_request`
    closed its accepted socket at each of its two `return`s and at neither of the two paths that
    throw, so wrapping the loop around it would have *counted* a descriptor leak rather than
    prevented one — one fd per failed request until EMFILE, which is the same metrics endpoint going
    dark that the boundary was being added to prevent. Ask what the abandoned iteration was holding.

305. **A test whose premise is "the server might not close the socket" must not read to EOF.** The
    first version of the leak test hung instead of failing: past ten minutes with no client
    deadline, then 250 s to fail with a five-second one. Reading a single response instead kills the
    same mutation in 6 s. Same family as the alarm on the SDK's silent-socket tests — a test that
    loses what it guards has to fail, not wait.

306. **A mutation harness must restore the sources *and rebuild*, or the next thing to use that
    build directory runs the last mutant.** The harness restored seven files and left the mutated
    `test_thread_boundaries` binary in place; the full `ctest` after it reported a failure naming a
    loop the source classifies correctly. Third variant of the stale-artefact pitfall in this
    repository, and the tell was the same: the message described a tree that does not exist.

307. **A metric written from a header is invisible to a checker that scans `.cpp` files.**
    `check_metrics.py` called `ob_loop_errors_total` dead because `LoopGuard` is header-only — its
    premise, "a write lives in a translation unit", was true of every writer until one of them was
    shared by seven. It scans `include/` now. The pair that says the forward half is load-bearing:
    a header writing an unregistered name is caught, and the same mutation with `include/` dropped
    from that scan survives.

308. **A test that asserts on a `DEBUG` line asserts on a line the process will never write.** The
    rewritten #133 test required the `Unavailable` branch's own sentence in the log, and that branch
    logs at `DEBUG` while the nodes run at the default `INFO` — so no amount of waiting could make
    it pass. The branch is quiet **by design**, which is the whole reason it does not become #133 in
    a new place; what is observable about it is the **absence** of the repair, and that is what the
    test states now. Before asserting a line is present, check the level it is written at against
    the level the process runs at.

309. **Two halves of one repair become true at different times, so poll both rather than sampling
    one at the moment the other fires.** #132's recovery writes the etcd key and *then* logs that it
    did; the first version of its test waited for the key and read the log window once, which lost
    the race and reported "the key is back but the node did not say it wrote it, so something else
    did" — a diagnosis pointing at a defect that does not exist. The loop waits for `key AND line`
    now. Same family as pitfall 302: an instrument that samples one side of a two-step change
    answers with the same voice it would use if the change were wrong.

310. **When a repair is added to a loop, the boundary around that loop becomes load-bearing for a
    second reason, and the comment justifying it goes stale in the direction that reads fine.**
    `lease_loop()`'s boundary was justified by "the registration expires with its lease and nothing
    ever puts it back — `register_self()` runs once, at start". #132 made the second clause false
    while making the conclusion *stronger*: the repair lives inside that loop, so a thread that ends
    takes the repair with it. A justification that has become wrong while its conclusion stayed
    right is the hardest kind to notice, because nothing it claims will ever fail.

311. **`MM_PEERS` and `replicas` list *connections*, so either can be one row short for a moment
    while a peer is being re-learned — assert the property, not the instant.** Third time in this
    repository: `replica_confirmed_file()` asked for `replicas[0]` while a replica reconnected
    after bootstrap and got `replicas: 0` seconds after that same replica answered a query with
    every row; #132's first test read `MM_PEERS` once at the moment the registry key returned and
    found no row for peer 2 at all. Neither was a defect, and both read exactly like one. What
    those tables promise is that the row **comes back**, so poll until it does and put the last
    read in the failure message.

312. **Two sentences about one set, only one of them checked, and the unchecked one is the one that
    rots.** `docs/roadmap.md` gained a mechanical `Open:` line — held in both directions by
    `scripts/check_roadmap.py` — while a **bold** paragraph five hundred lines below still said
    "there is no open defect on this page". That sentence was false **within two days**, falsified by
    the same session that wrote the checker, and nothing failed, because nothing was watching it.
    Adding a mechanism does not retire the prose it duplicates: go and find the other statements of
    the same fact and turn them into **pointers**. A document that never restates the set cannot
    disagree with it, which is the answer the flagship product's `docs/requirements.md` took for the
    same shape.

313. **A method that promises a side effect, does nothing, returns success and *logs that it
    succeeded* is worse than an absent one.** `PeerRegistry::update_status()` said "Update this
    node's status in etcd" in the header, wrote nothing, returned `true`, and emitted `Updating
    status for node N to 'x'` at INFO — so the first caller would have got the line they were going
    to grep for. Same family as #30's `cluster authentication enabled`, printed by a path that
    enforced nothing. An absent method fails on the first attempt to use it; this one would have
    failed silently and confirmed itself in the log. `update_position()` beside it was the one
    somebody would reach for, because #72 wants exactly that. Both deleted (#134).

314. **The checker that catches a dead *field* cannot catch a dead *function*, and the reason is
    why one was not written.** `tests/test_field_usage.cpp` compares every member declared in
    `include/orderbook/` against every occurrence, which works because a data member that nothing
    mentions is unambiguously dead. For methods it would need a hand-written list of what counts as
    internal rather than public API — `include/orderbook/` also holds the client API and the C API,
    where a callerless public method is entirely legitimate — and a list written by hand is not
    evidence about the code (#112's loops, #32's valueless flags, #117's metrics each paid for
    that). So this class is caught by reading here, and #134 records that rather than papering
    over it with a checker that would need an allowlist longer than the rule.

315. **A rule that passes by coincidence is indistinguishable from one that works — until the
    coincidence ends.** `scripts/check_roadmap.py` read every `#N` in the file as a reference to a
    roadmap item, including the `#N` inside the test table's citation of a **pull request**. Every
    such citation resolved to an unrelated item and the checker was satisfied, because PR numbers
    had always been *below* the item count. The first PR numbered above the last item is the first
    one that fails, which is how this was found: a legitimate citation reported as a broken
    reference. The rule now blanks markdown links whose target is `github.com`, with the pair that
    says it is load-bearing — a bare bogus `#9999` still fails, and a high-numbered PR citation
    passes. When two different kinds of thing share a notation, ask which one your checker is
    actually reading, and whether it has ever had to tell them apart.

316. **A mechanism written into a filed item is a hypothesis until it names the line, and #130's
    named a function that did the opposite.** The item said the monitor tick "reads the leader key,
    finds this node's address in it, and adopts it". `adopt_leader_if_present()` refuses a key
    naming this node and had done so for three weeks before the item was filed — so its first
    candidate answer was already in the tree, and three weeks of reasoning rested on a symptom
    explained backwards. The real cause was `attempt_promotion()` setting `role_` and
    `primary_address_` **before** calling the handler that throws. Before designing a fix, go and
    read the function the item blames; the cost of not doing it is choosing between candidates for
    a defect that does not exist.

317. **The condition to act on is the resource you hold, not the role you believe you have.**
    `FailoverManager::stop()` revoked the lease `if (role_ == PRIMARY && lid != 0)`, where the role
    was a proxy for holding a lease. #130's fix moves `role_` to after the promotion's handler
    returns, which breaks exactly that proxy: a shutdown in the new window finds `role_ == REPLICA`
    with a live lease and leaves the leader key alive until its TTL — a failover made *slower* by
    the change meant to make one honest. It revokes on `lid != 0` now. Same family as #131's
    descriptor guard: a guard around something you do not own counts the leak instead of preventing
    it.

318. **An answer assembled from several components' views can be false while every field in it is
    individually plausible.** `ROLE` prints the role from the engine, the address from the failover
    manager and the epoch from the engine. A half-finished promotion had already moved two of the
    three, so the reply was `REPLICA <this node's own replication port> 2` — a sentence that cannot
    be true of anything, and none of the three fields was corrupt. When a status string is built
    from more than one owner, ask which of them a partial transition has already touched.

319. **Where a bound goes is decided by which layer can also refuse the thing the bound is about.**
    #121 needed to stop a peer's clock becoming the mesh's clock. Putting the ceiling in
    `HybridLogicalClock` looks natural and is wrong: a clock that declines part of what it is told
    breaks the invariant it exists for, because if we accept a record we must stamp later writes
    above it or a causally later write can lose an LWW conflict to the record it followed. So the
    ceiling belongs where the **record** can be declined — and since a record refused under a live
    link is a silent hole, where the **peer** can be declined. The class is still uncapped on
    purpose, with the reason written beside the test that requires it.

320. **A rule decided per node against per-node state is not a rule, it is a divergence generator.**
    The obvious answer to #121 was to clamp what we absorb from a peer. Each node would clamp
    against *its own* wall clock, so two nodes stamp their later writes differently and
    `ConflictResolver::resolve()` picks different winners for the same pair — the divergence lands
    in the **data**. An untrue clock beats divergent values, and it is not close. Before adding a
    policy to a replicated system, ask what the policy reads: if it reads something each node has
    its own copy of, every node gets its own answer.

321. **"Unreachable in any reachable state" is a claim with a lifetime, and the lifetime ends where
    an input is unbounded.** `resolve_logical()` argued that the physical component cannot be
    `UINT64_MAX` because no real clock produces a 585-year-old timestamp — true of clocks, not of
    peers, and nothing bounded what a peer could send. Worse, the same comment said saturating
    there "keeps this function's promise"; it does not, because `{UINT64_MAX, 0}` follows
    `{UINT64_MAX, 65535}` and the clock runs **backwards by the whole counter**, which is what #119
    exists to prevent. When a comment says a state is unreachable, find the input that would reach
    it and check that something bounds it.

322. **A build directory that is behind under-reports the test count, and that count gets
    published.** A full local `ctest -j1` printed `1094/1094` for a tree whose suite is **1098**:
    the build had not registered four new tests, the run was green, and the number went into a
    sentence about the tree. Three measurements reconcile it — CI's 1098, `ctest -N` listing 1100
    (the two extra are the `DISABLED_` measurement harnesses, always), and a local rerun giving 1098
    in 218.95 s. Pitfall 149 says to read the build's exit status rather than its output; this is the
    case where the status is **zero** and the answer is still short. Any count that feeds the test
    table comes from the run that measured the tree carrying it, which is what
    `scripts/test_table.py` is for.

323. **A verdict that lives in an exit code does not survive a pipe.** A check-polling loop run as
    `watch.sh | head -3` died on SIGPIPE after three lines and the task reported **exit 0**, which
    reads as "the checks settled"; `python3 scripts/check_roadmap.py | tail -1` reported `rc=0`
    while the checker was in fact refusing a bare `#139` in new prose. Read the tool's own status
    (`${PIPESTATUS[0]}`) or do not pipe it. This has now cost a `ctest` verdict, a roadmap checker
    and a CI monitor, in that order.

324. **A policy constant nothing pins is not the same as one pinned to a literal — bound it from
    both sides instead.** Every expectation about #121's five-minute bound is written in terms of
    `MM_MAX_CLOCK_SKEW_NS` on purpose: pinning the number itself would fail on any legitimate
    retuning and teach a reader that the test is noise. What the fixtures do instead is bound it
    from both ends — a `static_assert` requires `4 min < bound < 6 min` because the test's two skews
    have to straddle it or it asserts nothing, two runtime guards require `1 min < bound < 1 h`, and
    a unit expectation puts it under a year. So a widening large enough to change an answer **does
    not compile**, and one small enough to compile changes nothing observable. That is why the
    mutation table carries "widened to five and a half minutes" as a control that must **survive**:
    a surviving mutation nobody explained is one the next reader assumes you missed.

325. **A machine's answer about itself can be absent rather than wrong, and a reader with a default
     turns that into a blank.** `/proc/cpuinfo` has no `model name` line and no `cpu MHz` line on
     aarch64, so `hardware.py` reported `cpu_model=unknown` and `mhz=0.0` on the first non-x86 run —
     in the module whose docstring says every field is read from the machine so that a reader can
     compare theirs to ours. The machine was willing to answer: implementer, part, variant and
     revision from MIDR_EL1, and `Amazon EC2 m9g.xlarge` from DMI. `BogoMIPS` is in the same file and
     is the architected timer's frequency, not the clock — a field holding a number that means
     something else is worse than an empty one. Google Benchmark makes the same mistake in its own
     context block: it reported `mhz_per_cpu: 2000` on this instance, which is the BogoMIPS value.

326. **`/tmp` is a tmpfs on a good share of machines, and a benchmark that puts one system's storage
     there has handed itself a win.** The comparative harness used `tempfile.mkdtemp()` for the
     engine's data directory and described the disk under the *build* directory. On the workstation
     it was written on, `/tmp` is part of an ext4 root, so the engine and both servers shared a disk
     and the published numbers are sound — by luck. On the first machine whose `/tmp` is a tmpfs the
     engine's WAL would have gone to memory while ClickHouse and PostgreSQL wrote to NVMe, and the
     report would have printed the NVMe next to all three numbers. Measured, same binary and core:
     989.62 ns/op with storage on tmpfs against 1061.04 ns/op on xfs.

327. **A "measured" figure written into generated prose is measured on whichever machine wrote the
     prose, and it travels.** `run.py` carried "Measured on this machine: the engine ingests 446,219
     updates/s in process (2552 ns/op)" as a string literal; the first aarch64 report printed it
     inside a header reading `Amazon EC2 m9g.xlarge`, where the same benchmark measures 995 ns/op.
     The same list claimed "about 4.8 ms of Python-side parsing is included in every figure in the
     query column" in a report whose fastest query median was 1.47 ms — a constant declared to be
     inside every figure while exceeding the smallest of them. Measure it in the run, or name the
     machine it came from; the third option is a contradiction a reader can see and the harness
     cannot.

328. **Two numbers in one sentence can be in different units and still read as a ratio.** "446,219
     updates/s in process against 4,012 updates/s through the wire — a factor of 111" compared
     `BM_IngestionThroughput`, which applies **one** level per call, with a harness that sends
     **twenty** levels per round trip. Most of the 111 was the 20. In one unit, on one machine:
     946,000 levels/s in process against 361,466 over the wire, a factor of 2.6 — and even that is
     an upper bound, because the harness's Python client spent 3.73 CPU-seconds of that run against
     the server's 1.11.

329. **A micro-benchmark of a component overstates what removing it buys on the path that contains
     it.** The 112-byte CRC32C fold costs 193.66 ns with the table and 6.68 ns with the ARMv8
     instruction — 187 ns apart in isolation. Inside `apply_delta` the same substitution is worth
     **25 ns**, measured over six interleaved rounds with non-overlapping ranges: the core overlaps
     the table's dependent-load chain with the rest of the operation. Publishing the isolated figure
     as an ingest improvement would have overstated it eightfold. Measure the path you claim about.

330. **`git cherry-pick <branch>` takes the tip commit only.** Assembling a measurement tree from two
     feature branches picked one commit from a two-commit branch and produced a tree that looked
     assembled and was missing the first half. It was caught by `argparse` refusing an unrecognised
     `--data-dir`, which is the behaviour to be grateful for. Use `origin/master..origin/<branch>`.

331. **`grep -c` exits 1 when it finds nothing.** A build step ending in `grep -c "error:"` reports
     **failure on success**, and the background task that ran it was summarised as "failed with exit
     code 1" over a clean build. The mirror image is worse: a step ending in a grep that *matches*
     reports success when it found the thing you were checking for.

332. **When two instruments disagree and the cause is not found, the number is not published.** Two
     binaries with materially identical timed loops measured single-level `apply_delta` at 646 ns and
     2690 ns on the same core, seconds apart. A third instrument agreed with the first, byte counts
     confirmed the same work in both, and the one hypothesis tested — a memory clobber in a sibling
     branch of the loop — was wrong. The twenty-level figure agreed across all three instruments, so
     that is the one on the page. 646 ns was the flattering number, which is the reason to be
     careful rather than the reason to pick it.

333. **`-Wno-error` in `CMAKE_CXX_FLAGS` loses to the project's own `-Werror`,** because CMake puts
     the cache variable first and GCC takes the last flag. To enumerate every diagnostic in one pass
     instead of fixing them one build at a time, keep `-Werror` and use `cmake --build . -- -k 0`:
     ninja carries on after a failed object and the full list arrives at once. One object out of 312
     failed on the first GCC 14 build of this tree.

334. **A guard's premise can be true when it is written and falsified by a feature added later, and
     nothing fails when it is.** `merge_segments()` refuses a segment directory it already holds,
     with a comment naming the cause — *"Two flush paths raced"*, #26 — and *"Defence in depth, not
     the fix."* A segment's directory name is its event-time range, and before #105 the wire dropped
     the client's timestamp, so two client writes could never produce the same range: the comment was
     correct. #105 put event time on the wire, which made a backfill re-run produce exactly that
     state from one client, sequentially. Measured: the first write's values silently replaced, or —
     when the second write has fewer rows — **zero rows for that symbol** after two acknowledged
     writes (#136). When adding a feature, grep for the guards whose reasoning your feature makes
     reachable; they will not fail, because their conclusion is still right.

335. **`getrusage(RUSAGE_SELF)` excludes subprocesses, so a "client CPU" column reads about zero for
     any adapter that shells out.** The comparative harness's three adapters do not agree about
     this and the difference is invisible in the code that times them: the engine's and
     ClickHouse's are in-process Python (a socket and `http.client`), and the PostgreSQL one drives
     `psql`. So TimescaleDB's client cost read **0.003 s** where the engine's read 3.731 — a
     comparison wrong in the direction that makes *us* look worse, which is not a reason to have
     left it. And the first version of the docstring explaining the fix claimed ClickHouse shelled
     out too; it does not. Check which of them actually forks before writing the sentence about why.
     `/proc/<pid>/stat`'s `cutime`/`cstime` are the same question for the **server** side, and they
     are what makes the number right for PostgreSQL, which forks a backend per connection.

336. **A probe linked against the project's libraries must use the compiler that built them.**
     `g++` on Amazon Linux 2023 is GCC 11 and the tree is built with `gcc14-g++`; linking a probe
     with the default compiler fails on `std::__cxx11::basic_string::_M_replace_cold`, a GCC 12+
     symbol. The error names a string function and says nothing about a version, so it reads as a
     mystery in the project's own code rather than as a toolchain mismatch.

337. **A cleanup step whose success is not checked is not a cleanup step, and the measurement
     afterwards is against whatever survived.** A probe runner began with
     `pkill -f 'ob_tcp_server --port 9191'` and went straight on; the old server — configured
     differently, and unresponsive — was still there, so the new one could not bind and the probe
     connected to the old one and hung. Assert the port is free before measuring against it. The
     consolation is that the engine said so precisely: `Error: bind() failed on port 9191: Address
     already in use`, which is #102's named refusal rather than the `SIGABRT` it used to be, on a
     machine that code had never run on.

338. **A service unit name is a fact about the distribution, not about the software.** A runbook
     written against Ubuntu says `postgresql@16-main`; on Amazon Linux the unit is `postgresql`, and
     `systemctl start postgresql@16-main` **creates a failed instance of a template** rather than
     failing to find anything. So the step reported `failed` for PostgreSQL while PostgreSQL was
     running the whole time, and the two comparative runs after it failed for an unrelated reason.
     Read `systemctl list-units` on the machine before writing the command that starts something.

339. **A number whose wall time is fifteen times its CPU time is a question, not a result.**
     `BM_IngestionThroughputBatched` reported 20,264 ns wall against 1,345 ns CPU per twenty-level
     update, and taken at face value the in-process figure would have been *slower* than the same
     work over a socket — which cannot be true and is exactly the shape that gets published when
     one number is read without its neighbour. Google Benchmark prints both columns for this
     reason. Find where the wall time goes before quoting either.

340. **A profile is a sample, so it has a sample size, and the small entries go first.** The pilot
     profile behind #138 ran 400 queries - about 0.2 s of server CPU, a few hundred samples at
     1999 Hz - and reported `format_query_response` at **40.11%** against `ColumnarStore::scan`
     at 2.74%. Re-run over 16,000 queries it is **22.96% against 2.70%**. The pilot was right
     about which function dominates, which is what a pilot is for, and wrong by nearly a factor
     of two about how much, because at that size a 2% entry is a handful of samples and a ratio
     built on it is arithmetic over noise. Print the sample or cycle count beside the shares, and
     do not publish a ratio whose denominator is small.

341. **A share that goes up while the total goes down is the expected shape, not a contradiction.**
     After #138 the formatter's share rose from 22.96% to 32.55% while the server spent 42% fewer
     cycles on the same 16,000 queries. Work that had been attributed to `_M_construct`, `malloc`
     and `memcpy` moved inside the function and got smaller. Compare **absolute** cycles across
     two profiles; percentages are of different totals and cannot be subtracted.

342. **A loop that picks a field by a value cannot match straight-line code that inlines, and the
     difference is worth measuring before you assume either way.** Replacing seven unrolled
     `put_field` calls with a loop over a column list cost `format_query_response` **24% more
     cycles** for `SELECT *` - 2.107 G to 2.621 G - with every other symbol flat. The general
     loop is still right for a narrowed answer; the canonical shape keeps its unrolled path. Add
     the specialised path **after** the measurement says so, which is what #139's own spec
     required, and never before.

343. **Two code paths producing one output need a test that reaches both, and a content-based
     dispatch makes that harder than it looks.** `format_query_response` picks the unrolled path
     when the column list *is* the canonical seven, so no call can be made to take the general
     path over that shape. The test formats each column **alone** - necessarily the loop - and
     compares it with that field cut out of the unrolled output. A test that only checked the
     fast path against literal bytes would pass while the two drifted.

344. **A buffer sized for "the widest row" stops being the widest row when the query can repeat a
     column.** The row buffer was a 105-byte stack array, correct for seven distinct columns and
     a stack overflow for `SELECT sequence_number, sequence_number, ...` at twenty repeats. The
     size has to come from the query's own list. The same reasoning retired `p + 24` as the end
     pointer handed to `to_chars`: fine while the buffer was always wider than any one field.

345. **Before refusing something, find out whether it is already refused and whether the form is
     in use.** #139 added a parse-time refusal for a select list mixing columns with aggregates,
     on the assumption that mixing was accepted. `execute()` had refused it for a long time with
     `AGG_WITH_COLUMNS`, naming the column; the new refusal changed the error code and broke that
     test, which is the only reason it was noticed. A second guarantee for one rule cannot be
     mutated separately. The same session then tried refusing a narrowed `SUBSCRIBE` and withdrew
     it: **three tests failed** when the refusal was tried, and the form appears five times across
     four test files - `SUBSCRIBE price ... WHERE price BETWEEN ...`, where the column names what
     the filter reads. Those two numbers are different questions and the smaller one is the
     measurement; counting occurrences afterwards is what showed the first version of this entry
     had used one to mean the other.

346. **A self-describing answer can be refused by a client; a positional one cannot.** A `SELECT`
     response carries a header, so narrowing it is safe in the sense that matters - a client that
     cannot read the new shape can say so by name. A `PUSH` line has no header, so narrowing it
     changes what field 2 means with no signal at all, and a subscriber reading positionally is
     simply wrong. That is why `SELECT` projects and `SUBSCRIBE` does not: the fix there is
     announcing the columns in `OK SUB <id>`, which is a protocol change.

347. **A guarantee can be stated three times inside one function, and the copy that holds is the
     one nothing can mutate.** `ColumnarStore::scan()` widens the caller's `ColumnSet` with
     `TimestampNs` because it filters on the row's timestamp and will not depend on the caller
     having remembered; `columns_to_read()` puts it there too, because the engine knows it filters
     on time; and two lines below the widening, `need(true, "ts.col", timestamps)` opened the file
     regardless of the set. The third made the first two unobservable, so a mutation deleting the
     widening **survived** -
     `ColumnarStoreProjection.TheTimestampIsReadEvenWhenTheSetLeavesItOut`, the test written for
     exactly that case, stayed green. Two statements of one rule are fine when each has a test at
     its own level; the one to delete is the one that makes the others impossible to falsify. The
     fix also moved the code back onto its own requirement, which says the scan opens
     **exclusively** the files the set names.

348. **`shutil.copy2` in a mutation harness restores the source with the backup's mtime, so the
     rebuild after it does nothing** - pitfall 272, committed again in a harness written by
     someone who had just written it down. The per-mutation restores used `write_text` and were
     fine; only the final one used `copy2`, which left the last mutant's object file in place. The
     tree afterwards had two red tests in `QueryProjection` with nothing wrong in the source, and
     `touch src/query_engine.cpp` plus a rebuild turned them green, which is the tell. Restore
     with `copyfile` and touch. The second half is what would have caught it: the harness checked
     its baseline **without building first**, so that check described whatever the previous run
     had left in `build/` rather than the tree it was about to mutate.

349. **An anchor on a literal dies with the literal, and the thing that replaced it is the new
     anchor.** `test_mesh_link_faults.py` pins the seven-column header it strips out of every
     query answer, and pinned it by looking for that string in `src/response_formatter.cpp` —
     right until #139 replaced the literal with a loop over the column table and the clang fix
     deleted the leftover. The test then failed saying the string no longer exists, which is the
     correct report from an anchor pointing at nothing. Two things follow. The replacement anchor
     is `src/query_columns.cpp`'s spellings table, **which is stronger than what it replaces**: a
     literal can agree with a header nothing prints, and a mutation swapping two rows of the table
     now fails the test where before it would not have. And the commit that removed the literal
     was verified with a **C++ build and `ctest`** — the only reader of that source text is a
     Python integration test, so the local verification could not have seen it. When a change
     deletes a string, grep the whole tree for it, tests in other languages included.

350. **A function-scoped fixture writing to one symbol on a session-scoped cluster turns every
     count assertion into an assertion about test order.** Storage is append-only, so
     `test_column_projection.py`'s `book` fixture added a row per test to one shared symbol: six
     of its seven tests read `lines[2]`, where the first row is the same however many follow it,
     and the seventh asserted `len(rows) == 1` and read **seven**. It is the order that decides,
     not the code, and it had never run to completion in CI before — the branch's previous run was
     cancelled, so the first completion was the first report. Give each test its own symbol; the
     count then says what it looks like it says, and the six that passed become exact rather than
     merely satisfied.

351. **Two correct mechanisms compose into a fixed timer, and the only client that can see it is
     one nobody benchmarked.** Nagle holds a small write while an earlier byte is unacknowledged;
     delayed ACK withholds the acknowledgement. Each is right. Together, on a server that had
     `TCP_NODELAY` on every socket it dialled and none it accepted, a client pipelining commands
     paid **52.75, 51.68 and 52.15 ms per round trip at batch 8, 64 and 512** — the same number at
     three batch sizes, which is what says timer rather than cost. The server's own CPU for those
     runs was 0.16, 0.12 and 0.14 seconds against 131.885 seconds of wall, which is pitfall 339's
     tell at a ratio of 800. A request/response client meets none of it, because with one response
     outstanding there is nothing unacknowledged when the next write happens — so the defect
     survived every published benchmark, all of which ask one question at a time. The clinching
     control is on the **client** side and changes nothing on the server: re-arming `TCP_QUICKACK`
     before every `recv` took the same 250 round trips from 12.963 s to 0.021 s (#140).

353. **An upstream bottleneck makes the one you are measuring invisible, and the tell is a
     figure that does not move with the variable you are changing.** #137's first before/after ran
     on two trees that both predate #140, and every one of eight rows came out at **20.1-20.5 s
     and ~198,000 levels/s** — the same at a 100 ms flush interval as at 1000, and the same on
     both trees. My first reading was that the shared box had been busy, and that was wrong:
     200,000 updates at batch 512 is 391 round trips, and 391 × the 52.15 ms delayed-ACK timer
     #140 measured is **20.4 s** against 20.13 measured. The ceiling under test was never reached.
     A quantity that refuses to move when you move its input is not noise, it is a different
     limit — and a fix measured against a baseline missing an earlier fix measures the earlier
     one. Re-run on a #140 base the same comparison is 1,196,745 → 2,209,501 levels/s.

354. **`wait_for` bounds the condition wait, not the reacquisition of the mutex after it.** #137's
     deadline is meant to refuse a write whose queue never frees, and a mutation shortening it
     from five seconds to one millisecond **survived**: the flush holds `mtx_` for the whole of
     its first phase — 1.2 s for a million rows, against 73 ms for the segment write that follows
     outside the lock — so the writer parked behind it is waiting for the mutex, not on the
     condition, and it is never refused. The outcome is right and the constant's name does not
     say so, which is why the header does. The case the deadline does bite is a flush that
     throws, releases `mtx_` and leaves the queue full, and that is a fault-injector test rather
     than a unit one.

355. **An assertion that something is absent is satisfied by a shape that never existed.** Two
     `/metrics` assertions in one new test were wrong in opposite directions, both from pitfall
     66: the exposition writes `name{node_role="standalone"} 3`, so a lookup for the name and a
     space finds nothing. One failed loudly. The other — `find("name 0") == npos`, meaning "the
     counter is not zero" — **passed**, because no line in that document has ever had the shape
     `name 0`. Before asserting that a string is absent, check that the string could have been
     present.

352. **A median that does not move can hide a tail that is an entire timer, so report the
     statistic that moved.** The same defect on the subscription path left the median push latency
     at 0.003 ms and the p99 at 0.006 ms — `SubscriptionHub` batches a drain, so most pushes go
     out with nothing outstanding — while the **maximum** in every one of three runs was a full
     delayed-ACK timer: 48.126, 40.884 and 43.330 ms, against 0.009, 0.006 and 0.015 after. Had
     only the median been quoted this would read as "no effect on subscribers", and for a feed
     sold on latency the 40 ms tail is the number a client quotes back. Roughly one update in two
     hundred at those rates, and which one depends on when the subscriber's acknowledgement
     happened to be due (#140).

356. **A function named for what its caller wants is a function nobody checks against what it
     does, and here the name was the whole defect.** `Engine::load_snapshot()` cleared the
     in-memory store and rebuilt the index from **whatever was on disk**; it never touched a file.
     Both installers renamed the received files in and removed nothing, on the reasonable reading
     that a function called *load snapshot* loads the snapshot — the mesh's own comment says
     applying a write now "would be applying it to contents that `load_snapshot()` is about to
     discard", and it discards memory. So a replica that had flushed a *prefix* of a symbol kept
     its own segment beside the arriving one: **122 rows against the primary's 100** (#142). The
     names overlap without matching, because a segment's directory name is its event-time range
     and a prefix ends earlier, so #136's duplicate-directory guard had nothing to refuse. Two
     rules fall out. When a function's name states an effect, grep for the syscall that would have
     it — here `remove`, and there was none. And split the name rather than the behaviour:
     `install_snapshot()` does the whole thing and `adopt_store_on_disk()` is the memory half,
     under a name that cannot be mistaken for the other.

357. **Clearing in the caller and rebuilding afterwards moves a window; clearing under the lock
     the readers take removes it.** The obvious fix for #142 — delete the directories, then
     rename, then reopen — leaves a gap in which a scan answers **short** rather than failing,
     because `ColumnarStore::close()` keeps the index and every read path opens its files per
     call with an `is_open()` test. That gap already existed as a rename over live files, which is
     why it is worth saying that the fix removes it rather than inherits it: the swap lives in
     `ColumnarStore` and holds `index_mtx_` exclusively for all four steps, so a concurrent scan
     blocks on the shared lock and then sees one store. The trap on the way: `open_existing()`
     takes that mutex itself and `std::shared_mutex` is not recursive, so the rebuild had to be
     split into an unlocked half first — the same shape as pitfalls 20 and 68, met a third time.

358. **Both snapshot staging directories live inside the data directory, so "clear the store"
     deletes the files you are about to install.** Replication stages at `<base>/snapshot_staging`
     and the mesh at `<base>/mm_snapshot_staging`. A clear written as "every top-level directory
     whose name does not begin with `wal_`" — which is what `discard_local_data_for_resync()`
     already says, correctly, for its own path — eats the snapshot. There is a unit test for
     exactly that, because it is the one mistake that turns this fix into a worse defect than the
     one it closes.

359. **An item's own estimate of what a fix would cost is an estimate, and this one blocked the
     correct answer for a week.** #136 said making segment identity unique was "the widest change:
     the snapshot manifest addresses segments by directory, retention orders them by name, and a
     replica bootstrapping from a snapshot indexes what it is sent." Two of the three are false.
     `segment_dir()` had **one caller**; every other use of `dir_path` is an opaque string;
     retention sorts with `segment_order_less` (`start_ts_ns`, then `end_ts_ns`, `dir_path` only
     as a tie-break), not by name; and `open_existing()` takes the timestamps from `meta.json`, so
     the name format is not part of any contract and the change is **backward compatible with
     every directory already on disk**. The cheap-looking candidate — replace the index entry —
     was the one that silently loses a write. Cost an item's candidates the way you would measure
     anything else: by reading who actually depends on the thing you would change.

360. **A comparator's comment can state the invariant a defect breaks, which makes it a defect
     report nobody filed.** `segment_order_less` says *"dir_path is unique per segment, so this is
     a total order"* and records that a TTL property test used to fail one run in three when it
     was not. Under #136 `dir_path` was **not** unique — that is the whole defect — so the
     comparator was not a total order in exactly the state the defect produces. Fixing identity
     restored a premise the code had been asserting for a year. When a comment says "X, so this
     holds", ask what happens where X is false, and whether anything stops it being false.

361. **Let the syscall arbitrate instead of testing first.** The disambiguating name for a segment
     directory is chosen with `fs::create_directory()`, which reports whether it created the
     directory or found one, rather than with an `exists()` followed by a create. That removes the
     race without a claim about which lock the caller holds — worth more than the claim, because
     the guard this narrows had asserted a locking fact about *its* callers and had been wrong
     about it since #105 shipped. The general form: when the question is "is this name free",
     prefer the call that takes the name and tells you, over the call that answers and leaves a
     gap.

362. **A test that quotes the shape of a rendered artefact has to read the renderer, and the word
     on its own is satisfied by the line below it.** My provenance test for the io profile asserted
     that `format_config()` contained `[profile]`; the renderer prints `(profile)`, so it failed
     for the right reason on its first run. The half worth keeping is that the bracketed version
     would have been the wrong check *anyway*: `profile` is a key on the next line of the same
     output, so a search for the word anywhere is satisfied by a cross-reference — pitfall 237's
     shape, and the third time a check in this repository has been satisfied by a mention of the
     thing rather than the thing. The assertion extracts the `io-spin-us` line and requires the
     value and the provenance *in that line*.
363. **A control mutation is what finds the literal you did not notice you had written, and this
     one found two.** #144's table needs a row that must **survive**: a retuning of `kBoostSpinUs`
     inside the window the tests state as legitimate, which is what says the constant is bounded
     rather than pinned. Drafting it before running it caught the first mistake — I copied "widened
     to 5.5 minutes" from #121, where the bound is measured in minutes, and here the tests state
     `0 < kBoostSpinUs <= 1000 µs`, so that widening is a **kill**. Both rows belong in the table
     and for different reasons: the retuning survives, the widening past the documented window
     dies, and one without the other reads as an accident.
     Then the corrected control — 200 µs, comfortably inside those bounds — **killed as well**, and
     the reason is the whole value of having it: a third test, the one asserting `--print-config`
     attributes the value to the profile, looked for the literal `"50"` in the rendered line. That
     is pitfall 324's mistake committed in the test written to honour it, it was invisible to
     reading, and nothing but a mutation that changes the constant can find it. The expectation is
     `std::to_string(ob::kBoostSpinUs)` now.

364. **A probe that assembles its command by appending to a default measures the default, and the
     two columns look like an answer.** The first version of the probe behind #145's table
     had `std::string command = "PING"` and then appended its arguments, so every round trip sent
     `PINGBOOK SYM EX` and both the baseline and the subject measured the server's
     `ERR unknown command` — **two plausible figures a hundred nanoseconds apart**, which reads
     precisely as "the book read is free" and would have been published as that. Nothing in the
     latency said otherwise. What said otherwise was the field the probe prints for exactly this
     reason: **twenty bytes of answer for a thousand levels per side.** A latency figure for a
     payload nobody states is a figure about nothing, so the probe reports the payload and the
     harness refuses a row whose answer is not the shape it asked for - both now in the
     repository, as `benchmarks/command_latency.cpp` and `scripts/measure_book_latency.py`. Fourth variant
     of "the instrument answers in the same voice it would use if the code were fine", and the
     first where the *difference* between two columns was the thing being fabricated.
365. **Match the symbol whose body you are claiming about, not the one with the name you expect.**
     The write-path diff for #145 first reported `ob::Engine::apply_delta` as **2 instructions**
     in both builds and "identical", which is true and says nothing: its body moved to
     `apply_delta_impl` long ago and what is left is a tail call. The claim needs
     `apply_delta_impl` (588), `apply_delta_mm` (647) and `WALWriter::append` (111). `mnemonic_diff`
     warns about this in its own docstring and the warning is not enough — the check is
     `nm -C <archive> | grep <name>` before choosing the symbol.

366. **Profile the thread you are about to multiply before you multiply it.** The plan for using
     every core started with more reactors, because one io thread sat at 0.88 of wall time and the
     rate was flat in connections. The profile of that thread said a third of it was sending
     answers one segment at a time — a per-batch cost every additional reactor would have paid
     again. Removing it first bought +55% on the same thread and made the per-reactor unit of work
     smaller before there were several of them (#146).
367. **A test that reads a counter the instant a client closes asserts an ordering between two
     processes, and flow control can hold that ordering up for years.** #143's ceiling tests passed
     for as long as the server read 4 KiB at a time: it drained slowly enough that the client
     blocked in `sendall()` until the server's reset arrived, so the refusal always landed before
     the client got to look. Reading 64 KiB let the client finish writing into kernel buffers and
     look first — **4 of 40** immediate reads missed a refusal 1.5–2.1 ms away, **0 of 40** once
     settled. Wait for the property with a deadline; the test changed and the engine did not.
368. **A premise a test states in a comment rather than an assertion is true until someone changes
     the thing it depends on, and then it is silently false.** "2000 commands are about 10 kB, so
     the server takes them in several reads" — they were 96 kB, which made "several" true by
     accident, and a 64 KiB read made it two. The size is asserted now, against the read size it
     is about.
369. **A check that lives in one harness does not protect the next script, and pitfall 326 was met
     again three days after it was written down.** The comparative harness refuses a data directory on
     tmpfs; the pipelined-ingest script written for #146 put each node's data under `tempfile`'s
     default, `/tmp`, which on Amazon Linux is a tmpfs, and read **11–15% high** on every row.
     Import the check (`benchmarks/comparative/hardware.py`) rather than remembering it.
370. **Count syscalls with tracepoints and attribute them with `strace`, because `strace` changes
     what it counts.** Under `strace` the read that finds the socket empty disappears: the traced
     server is slow enough that the client's next batch has arrived by the time it reads again. The
     tracepoint count (`perf stat -e syscalls:sys_enter_read`) is the number; `strace` says which
     descriptor each read was on — here, that one read per pass of the loop is the subscription
     hub's eventfd, drained whether or not it fired.

371. **A second copy of the server loop drifts in the direction nobody tests, and every feature
     added after it was written went to one of the two.** The io_uring transport constructed its
     engine with `FsyncPolicy::INTERVAL` and a 100 ms flush interval as literals, passed no
     multi-master configuration, handed subscriptions a null hub and refused TLS — so
     `--fsync-policy every` parsed, was accepted and did nothing: **2 fsyncs for 200 acknowledged
     writes**, against 201 on epoll (#147). Nothing failed, because no CI job ran that binary. When
     two loops must agree, one of them should not exist, and the proof that it could go was a
     latency table in which the other one's `boost` won.
372. **An asynchronous send borrows its buffer until the completion arrives, so a per-descriptor
     slot reused by the next submission is a use-after-free with the right length.**
     `pending_writes_[fd].assign(data, len)` ran once per answer while the sends prepared for the
     earlier answers still pointed into that string; a longer answer reallocated it. The client got
     exactly as many bytes as it asked for, which is why the byte count looked right — and they were
     **memory of the server process**. A sequential client never has two answers in flight, so only
     a pipeline could see it (#147).
373. **Writing "not measured here" is how the measurement gets made.** #146's closing paragraph was
     going to say the io_uring transport "sends one answer at a time and is untouched". Checking
     that sentence against the transport took ten minutes and found a heap disclosure and a
     durability flag that did nothing. Fourth time in this repository that writing for a reader
     tested the code (pitfalls 84, 112, 228).
374. **A guard written after a defect is checked against the defect as it was, not against the
     shape the guard looks for.** #90 removed `"ob_tcp_server v0.1.0 listening on port %u,
     data-dir: %s\n"` and added a rule refusing `"v?0\.1\.0"` — a literal made of the version and
     nothing else, which that line does not match. Restored verbatim, the removed line passes the
     rule written to keep it out, and two older copies inside longer literals — the welcome banner
     and the CLI's greeting, both from March — passed it for as long as it existed (#148). #90 says
     the guard was mutation-checked; whatever the mutation was, it was not the diff's own minus
     side, which is the first one to try: the defect as it was is the one input the guard must
     refuse.
375. **A chain of static checks is only as long as its last link, and the missing link is the one
     that reads.** #32 held the parser's branches against the known-flag list and the list against
     the help text and the CLI reference, and all of them agreed on `--workers` for six months:
     parsed, listed, documented as "Number of worker threads (default: 4)" — and read by nothing
     (#149). The check that finds it goes one step further than any list: a field the parser
     writes must be read by something other than the function that prints it back.
376. **A lock's cost across threads is its handoffs, not its critical section.** The multi-reactor
     stage predicted 1.6–2.4× for writes, bounded by Amdahl at 1.84× with 39% of the io thread under
     `Engine::mtx_`, and measured 0.89–0.93×. The bound counted the work under the lock; the lock is
     taken once per record, 768 000 times in three seconds by four threads, and each contended
     acquisition is a futex sleep and wake that costs more than the section it guards (#151).
     `perf stat -e syscalls:sys_enter_futex` before and after is a thirty-second measurement, and
     `perf record -g` on the same tracepoint names the lock: here, 100% on one address.
377. **Two builds compared are two compilers until shown otherwise.** The box's master build used
     `gcc14-g++` from an earlier configure and the branch the default `c++`, which is GCC 11, and the
     branch read 18% faster at one reactor on identical code (19.3 against 16.0 million levels a
     second). `grep CMAKE_CXX_COMPILER CMakeCache.txt` in both trees, before the first number.
378. **Connections dealt in turn put two on one loop whenever the order says so, and a measurement of
     isolation has to say which loop each landed on.** The first isolation run at two reactors dealt
     the writer to the scanning connection's reactor, because it was the fourth connection, and
     showed no benefit at all. Read the placement from the node's `Reactor N adopted` lines; do not
     assume it from the count.
379. **A scripted rename that rewrites every write can leave the read.** `STATUS`'s snapshot was made
     by replacing `stats.` with `snap.` in its body, which changed every field it wrote and left
     `format_status(stats, …)` - the one use not followed by a dot. It was caught by reading the diff,
     and the mutation that restores it survived every unit test until
     `StatusAnswersTheEnginesFiguresNotTheSharedDefaults` was written for it.
380. **Staging hunks by marker can commit a state that does not build.** The admin serialisation and
     the drain change touched the same four places; staging the hunks that named `admin_mtx` also
     removed a member whose use sat in a hunk left unstaged. Hunks are units of text, not of meaning;
     check that the committed tree builds, or build the intermediate file state instead of staging
     pieces of the final one.
381. **A static rule that asks for a word in a branch passes a branch that ignores it.** #152's rule
     asked for `format_error(` in the too-large branch and no `close_session(`, and a mutation that
     built the error, dropped it and set `queue_refused` satisfied both while closing the session
     exactly as before. The rule now asks for what the branch must *do* - queue the error, and give
     up only if even that does not fit - which is what the mutation row was for.
382. **A record that ends a file for every reader has to be followed by leaving the file, whatever
     else fails.** Replay and catch-up stop at a ROTATE record; `rotate()` synced the marker and threw
     on a failed sync before moving to the next file, so the next acknowledged write went in behind
     it and was gone after a restart (#153). The question to ask of any marker is not whether it was
     written but where the writer is afterwards.
383. **Housekeeping that throws after a write reports the write as failed, and the client sends it
     again.** A rotation that could not write its marker threw out of the `INSERT` whose record was
     already in the file - `ERR` and present, which a retry turns into two rows (#153). An exception
     from after the side effect belongs to the thing that failed, not to the one that succeeded.
384. **Closing before opening is the right order, and it leaves nothing to write to if the open
     fails.** `open_current()` closed the old descriptor first - correct, because that file must not
     be written to again - and a failed open left `fd_ = -1` with nothing that ever opened another,
     so every write was `Bad file descriptor` until a restart, after the directory was writable
     again (#154). A state with no resource needs something that retries getting one.
385. **A relative path handed to a child that runs somewhere else kills every row of a mutation
     table.** mut153 passed the server to pytest as `wt-2b/build/…` and pytest ran from `wt-2b`, so
     seven integration rows "died" of `FileNotFoundError`. Only the control, which has to survive,
     and the baseline after the last restore, which has to pass, said so - resolve paths before
     handing them over, and never drop either of those two.
386. **A second write path is the one that goes untested.** Stage 2b could have left `apply_delta()`
     alone and added `apply_deltas()` beside it: the server would then have used one and most of the
     tests the other. Carrying the GAP inside the batch, in front of its DELTA, is what let a single
     write become a batch of one - so every test the single path had now tests the batch (#155).
387. **A rule no test can tell from its absence is removed, not kept.** Writing #155's mutation
     table found two in code written the same day, before a row had run: a second duplicate check
     guarding a batch of replicated records that does not exist, and a rotation skipped after a
     failed sync only to match the bytes of a single-write path that no longer existed. Kept, each
     would have read as a guarantee and been none.
388. **A count of occurrences cannot see a call moved into a loop, and a rule satisfied by any
     occurrence is satisfied by the wrong one.** The rule for "one wait for room per batch" counted
     `await_pending_room(`, and moving the call into the loop over the writes keeps one occurrence.
     Its replacement read the blocks a `{` opens, and mutation row 7 - a loop with no braces - got
     past it; it now reads a loop header on the call's line or the one before. Row 14 got past a
     rule that asked for *a* `pending_writes_.clear()` before the loop, by leaving the other one:
     the rule now names which. Each control is that the rule still finds what it looks for.
389. **A verification that shares its build directory with the work verifies neither.** Twice in
     #155: four targets rebuilt in the tree whose full `ctest` was running, and then the run
     restarted with every other test binary still linked against the previous engine - `ctest`
     would have reported the old code in the voice it uses for the new. The script now builds
     first and refuses to test after a failed build, and nothing touches the tree until it ends.
     (And the script was edited while it ran - bash reads a script as it goes - a third time.)
390. **A query reads what has been flushed.** A test that wrote 640 rows and queried straight away
     read 192, the ones the flush timer had moved; a replica refuses `FLUSH` and flushes on its own
     interval. The flush belongs to the test's premise, not to what it is testing.
391. **Two engines compared record for record differ in the one field that holds the clock.** A
     CHECKPOINT carries the wall-clock time `close()` wrote it at. The comparison blanks that field
     and nothing else, so it still checks that both engines wrote one - dropping the record would
     have dropped that.
392. **A gate is tested where it is bypassed, not only where it stands.** The event loop now holds a
     read's writes and applies them through `execute_writes()`, which does not pass
     `execute_command()`'s authentication gate: `deferrable_write()` is that gate for a write. The
     test of an unauthenticated session checked `SELECT`, `STATUS` and `COMPRESS` and not one write,
     so a yes too many there would have stored writes without authentication with nothing failing.
393. **`TMPDIR=… sudo cmd` runs `cmd` without `TMPDIR`.** `sudo` resets the environment, so a
     benchmark meant for the disk wrote its WAL to `/tmp`, a tmpfs on the benchmark host, and read
     30% faster than every other run of itself. `bench_engine` says so on stderr, and the command
     had sent stderr to `/dev/null` (#155). Pass the variable inside (`sudo … env TMPDIR=… cmd`),
     keep stderr, and read it - a refusal nobody reads is a measurement of the wrong thing.
394. **A bound derived from the number it explains confirms nothing.** Taking the serial fraction
     from the measured speedup and then finding that Amdahl's bound matches the speedup is one
     number said twice. #155's first draft did exactly that; the claim now rests on two measurements
     that do not use it - eight connections giving what four gave, and the profile's time inside
     the lock (79% of the wall clock) - and that is the only form of it worth writing down.
395. **`nullopt` from a reader is two answers.** Stage 3's cgroup reader returned "no such file" and
     "a file that cannot be read" as the same `nullopt`, so the reason could not tell a level that
     limits nothing (no `cpu.max`: the CPU controller is not enabled there, and the root never has
     one) from a level that could hold a limit it did not read - and it said "no limit" beside a
     parent it had not read (#156). #82 met the shape one class over, a `nullopt` that meant four
     things. A reader returns what it knows in as many states as there are, and `open()`'s errno
     is where the difference was.
396. **A `grep` that found nothing is a claim about one commit.** Three pages quoted
     `grep … hardware_concurrency … src include` returning nothing; stage 3 made it return a line in
     `src/machine.cpp`, and none of the three would have noticed, because nobody re-runs a search
     whose answer they have already published (#156). Say when it was true, and when you add the
     thing a published search looked for, search the tree for the search.
397. **A thread's name is not the program's.** The rule measurement checked that the PID behind
     `taskset` and `systemd-run` was the server by reading `/proc/PID/comm` - the main thread's name -
     and the server's main thread runs the first client loop and calls itself `ob-io-0`, so the check
     refused the server as a wrapper (#158). `/proc/PID/exe` is the program.
398. **A log read from its tail cannot see what a process said first.** The new profile tests looked
     for the `machine:` and `io profile` lines through the module's `tail()`, the last 2000
     characters, and a node's startup is 3.5 KB: the lines that decide everything after them are the
     ones a tail misses (#158).
399. **A spin window's cost is the window.** With more busy threads than cores, a loop that is
     spinning keeps the core its client needs until the window closes, so the client's p99 grows by
     the window - 16.4 µs at 10, 24.2 at 20, 53.9 at 50 (#158). #144 chose 50 as a judgement about
     the gaps between requests; the gain was complete at 10 on the machine we publish from, and the
     other 40 were tail.
400. **An apostrophe in a comment ends a single-quoted `ssh` argument.** A script sent as
     `ssh host '... cat > f <<"EOF" ... the server's own ... EOF'` closed its own quoting at
     `server's`, and the rest ran in the local shell, stopping at a `cd … || exit 2` that was all
     that stood between it and a benchmark run on the wrong machine. Write the script locally and
     `scp` it.
401. **A record that states a boundary has to carry the position the boundary was true at.** The
     checkpoint meant "everything before me", and the lock was not held from the drain to the
     record, so "before me" included what writers appended in between (#159). Capture the position
     when the claim is true - at the drain, under the lock - and write that, not the record's own.
     The same wrong boundary had a second reader, WAL retention; grep for every reader of a boundary
     before fixing one.
402. **An injection point named by size moves when the record's size moves.** `OB_FAULT_SIZE=24`
     named the checkpoint until it gained an 8-byte payload. The test failed on its own "no fault
     fired" guard rather than passing over nothing, which is the only reason it was noticed. When a
     record's layout changes, grep the fault tests for its size.
403. **`FLUSH` runs on the connection's event loop.** A test that needs a write *during* a flush
     cannot get it from `FLUSH` on a one-loop node: the writes on another connection wait for the
     loop, that is for the whole flush - 1.99 s measured, and the `FLUSH` answered first. The flush
     tick has its own thread. The premise check is what said so; without it the test passes against
     the defect, because rows written after the flush survive on any build.
404. **A control run needs the fix's instruments and the defect's code.** The first run of #159's
     tests against the old server used the old build's injector, whose delay mode knew only syncs,
     so `OB_FAULT_DELAY_MS` on a write **failed** it instead - a different fault. The tests' own
     premise ("the injector never slowed segment write number 1", log `action=fail`) refused. Pair
     the old server with the new injector.
405. **A failed sync is not retried by the next one.** Linux reports a failed `fsync` or `syncfs`
     once and marks the pages it could not write clean, so the next sync succeeds **without writing
     them**. The first version of #160 let the next successful `syncfs()` cover a failed flush's
     segments, and a checkpoint after it then vouched for files the device may not have; the WAL
     had the same hole one record wide, a checkpoint whose sync failed promoted by the sync after.
     A failed sync of any kind now freezes the checkpoints until a restart rebuilds from the log
     (`ob_checkpoints_frozen`). This section's own entry on `fsync` already said the pages are
     marked clean - the design was written beside the sentence that refuted it.
406. **An injector that fails a call does not reproduce what the kernel does after a real
     failure.** `OB_FAULT_OP=syncfs` returns `EIO` and marks nothing clean, so the test asserting
     "the next flush claims both" passed while being false on real hardware: the instrument could
     not model the one fact that decided the design. Reading the design against the kernel found
     it, and then a test built on a consequence the injector *can* produce - retention moving after
     a failed WAL sync - measured it. When a fault test agrees with a design, ask whether the
     injector could have disagreed.
407. **`syncfs()` answers for the whole filesystem.** From Linux 5.8 it reports a writeback error
     anywhere on the superblock since the descriptor's last call - a WAL page, or another process's
     file on a shared volume - and before 5.8 it reported none at all. Whose pages failed cannot be
     told, so the only safe reading of a failure is the conservative one.
408. **A cut's premise is checked on the device, after the remount and before the restart.** The
     vouching test syncs each `meta.json` itself - a test process can `fsync` a file the node wrote,
     through a read-only descriptor - and then asserts on the remounted filesystem that a column
     came back empty. Without that check, a run in which writeback happened to reach every file
     would pass with the rule doing nothing, and read as the rule working.
409. **Writing the expected verdict before running a mutation table audits the tests.** #160's
     table named four mutations nothing would kill before a single row ran: the vouching rule was
     asserted only by its log line, `wal_identity`'s atomic write was covered in every existing test
     by the flush's own `syncfs()`, the `none` exemption had no test, and a refused segment's
     directory could stay. Each got a test first, and the run then matched 23 of 23 - including
     the control that says why the old power-cut test could not see the identity.
410. **Two renderings for three states lie about the third.** The replay line had "resuming at
     file N offset M" and "after the checkpoint record, which says nothing of what it covered", so a
     log with **no** checkpoint read as an older build's checkpoint. #160 made that state common -
     a first flush cut short, or a failed sync, leaves none - and it is the one in which startup
     removes every segment of this WAL. When an enum-shaped value goes into a log, count the states
     before the strings.
411. **`LD_PRELOAD` cannot see stdio's writes.** glibc's `fclose` flushes through its internal
     `__write`, not the `write` symbol, so the injector's log stayed empty over the save it was meant
     to hold - and the test's premise said so rather than passing. The kernel sees every write:
     `strace -P <path> -e inject=write:delay_enter=8000000` holds the one on a path however the
     process makes it, and it is what measured #162 on the code before the fix.
412. **Two missing syncs can cancel each other.** The build before #162 survived the snapshot cut by
     accident: its position file did not survive the cut either, so the replica bootstrapped again.
     Making only the position file durable - a durable pointer to data that is not - answered 0 of
     1100 rows. When a fix makes one of two files durable, test the state in which only that one
     survives, and ship both halves together.
413. **Wait for the state the test claims, not for the first line that looks like it.** The snapshot
     cut first waited for the state file to name a stream, which the replica writes at position zero
     before it asks for anything, so the cut landed before the install, a second bootstrap healed it,
     and the test passed with the install's sync removed. The fault test did the same with rows: the
     first install had already replaced the store, so every row was readable before any position was
     recorded. Both are now waited for as the position they are about.
414. **An open for writing is a claim about durability, so the claim sits next to it.**
     `OB_DURABLE: <what syncs it>` within four lines above every open in `src/`, `include/` and
     `tools/`, found from the tree in both directions (`tests/test_durable_writes.cpp`). `std::regex`
     over the tree took nine seconds a test in Debug; plain search with identifier boundaries takes
     0.4, and has to reject `fdopen` for `fopen` and `::create_directories` for `::creat`.
415. **A count from a clock carries nothing of the clock it came from.** The TTL sweep took
     `steady_clock::now().time_since_epoch()` - nanoseconds since boot - and compared it with event
     times, nanoseconds since 1970 (#163). Up for less than the retention, the cutoff wrapped past
     every timestamp and the first sweep deleted everything; up for longer, nothing ever expired.
     Every unit test passed, because each one handed `delete_expired_segments()` a cutoff it had
     computed itself. Take moments from `wall_clock_ns()`, keep monotonic readings as
     `time_point`s, and `tests/test_clock_use.cpp` refuses a count taken any other way.
416. **A test whose premise is the machine must read the machine.** The wipe needs a retention
     longer than the uptime, so the TTL tests take the retention from `/proc/uptime` - a CI runner
     has been up for minutes and a desk machine for a day, and a constant would hold the premise on
     one of them.
417. **A log number named for one quantity and computed as another survives every test.** The
     retention line printed `age=` for how far past the cutoff a segment was, which is not its age -
     and while the cutoff was a count from boot it said `age=4626822.4h` of a segment written a
     second earlier. It names what it computes now.
418. **Taking work out of a lock and then taking the lock once per piece of it can cost more than
     leaving it in.** Stage 5's drain first released the pending-row ceiling under `mtx_` once per
     4096-row chunk — about 250 acquisitions a tick — and at four pipelining writers each queued
     behind them, so the drain fell behind and the queue reached the ceiling: 10.6 → 9.0 M levels/s.
     Under `every`, where a writer holds `mtx_` through its own `fsync`, every give-back waited
     8.7 ms and the chunk pool filled (RSS 32 → 157 MiB). The count is an atomic now and the pool
     has a lock of its own (#164).
419. **Two containers swapped each tick each keep the capacity of the largest tick they held.** The
     first half of stage 5 did that, and resident memory went 163 → 270 MiB at one connection and
     158 → 321 at four — the million-row ceiling's memory promise doubled, with nothing failing.
     Measure peak RSS whenever the container behind a bounded queue changes.
420. **A diagnostic that did not run reads as one that found nothing.** `perf trace -e` with `unlink`
     in its list refuses the whole list on aarch64, which only has `unlinkat`, and with stderr sent
     to `/dev/null` the run traced nothing — which reads exactly like "no slow syscalls". Keep
     stderr, try the event list on its own first, and check the trace is non-empty before reading a
     zero out of it.
421. **When a change moves when something happens, every test that waited for it moves too.** After
     stage 5, #160's test under `none` expected the failed sync of a rotated file by the time the
     inserts returned, and it happens at the next tick now; the drain test read its premise — the
     rollover's log line — while the stalled write was still in progress, and the line is written
     after the write returns. Both reported a missing premise against a server doing what it should.
422. **An aggregate cannot count what is stored.** It is computed over the live book, and with a time
     filter it is refused; a test that has to count stored rows — a drain that may have lost or
     doubled some — reads them back.
423. **A test that fits in one chunk cannot see a chunk boundary.** #161's test drains three rows,
     all in one 4096-row chunk, so once the queue was chunked it could not tell "the rest of the
     batch" from "the rest of this chunk", and nothing killed a drain that dropped every chunk after
     the one it stopped in. Writing the mutation table's verdicts down before running it found the
     row nothing could kill; the multi-chunk test puts the refused rollover in the second of three.
424. **`pkill -f` with a pattern your own shell's command line contains kills that shell** — pitfall
     122 in its destructive form: exit 144, and the command meant to stop a hung probe took the
     session's shell with it. Stop a process by the PID it was started with.

## Current state and open problems

Roadmap phases 1-6 are complete; 7-11 are planned in [docs/roadmap.md](docs/roadmap.md). Item numbers
below refer to that file. **Those numbers are permanent ids — never renumber them.** A new item takes
the next free number wherever it sits on the page; `scripts/check_roadmap.py` (run in CI) checks ids,
references and ranges. The rule exists because three renumbering passes each broke something, and
because commit messages and specs cite these numbers.

**Where the suites stand:** the measured counts and runtimes are in the test-suite table in
[docs/roadmap.md](docs/roadmap.md). Both suites run in CI on every pull request, the whole integration
battery a second time under ThreadSanitizer, with an unexpected skip failing the job. The CLI and
C++ client harness are built alongside the selected server in both integration jobs. Clang builds
and tests the tree too. **Thirteen checks are required** on `master`: #38 added `fuzz`, and #147
took `io-uring-build` away with the transport it built. The exact contexts
live in `.github/rulesets/master.json`, and `check_contexts.py` now derives that number and checks
**this sentence** against it as well as the one in `docs/github-security.md` — it said "Thirteen"
for one item, which is pitfall 223 happening in the second document its own mechanism did not
read.

Read the sanitizer claims with #83 in mind: until it landed, `OB_ENABLE_ASAN`, `OB_ENABLE_TSAN` and
`OB_ENABLE_COVERAGE` instrumented the test binaries and the server but **none of the static
libraries**, because `add_compile_options()` only affects targets declared after it and those blocks
sat below all of them.

**The open set is held mechanically by the `Open:` line in `docs/roadmap.md`; read it there rather
than trusting any sentence here, which has been wrong about it before** - this one named an open P0
until the change that closed it. The items
below are the recent closures worth knowing because each changes what the engine promises; the list
carries no count, because the previous version of this sentence said "four" above a list of six and
omitted the newest one entirely - which is the rot pitfall 312 is about, in the paragraph that
warns about it.

**#164**: the flush tick syncs the WAL, drains its rows and deletes without the engine's lock, and
the file a rotation leaves is synced by the tick rather than by the writer that crossed the
threshold. One pipelining connection writes 8-9% more, with its once-a-tick stall halved under the
default and gone under `none` between rotations; four connections under the default are unchanged,
because the million-row ceiling binds them and what bounds them is the flush cycle's capacity, not
its lock.

**#139**: a row query answers the columns it names. `SELECT *` is byte for byte what it was; a
three-column question costs a fifth less.

**#140**: every accepted socket sets `TCP_NODELAY`. A client that pipelines paid a **52 ms kernel
timer per round trip** — the same figure at three batch sizes, which is what says timer rather
than cost — and a request/response client could never see it, which is why every published
benchmark missed it.

**#141**: `insert_batch()` in the Python client, so that measurement describes a client somebody
has. 1.59× at a batch of 64, and the single-row batch is **9% slower** than `insert()` — the row
that does not flatter is the control.

**#137**: a writer at the pending-row ceiling asks for a flush instead of waiting out
`--flush-interval-ms`, and its wait has a deadline after which the write is refused rather than
accepted. 1,196,745 → 2,209,501 levels/s at four million levels and a one-second interval,
unchanged at the 100 ms default the engine ships with.

**#162**: a replica's position file is replaced, never rewritten in place - a kill between the
truncate and the write left it empty, which reads as "wipe the store and stream everything again"
(measured with the write held open by `strace`: 0 bytes, `clearing local data`,
`REPLICATE 0 0 0`) - and an installed snapshot is synced before the position that names it is
saved: with only the first half, a power cut answered **0 of 1100** rows, and the build before
either half answered 1100 by accident, because its position file did not survive the cut either
(pitfall 412). Every open for writing now carries `OB_DURABLE:` or goes through
`write_file_atomically()`, derived from the tree by `tests/test_durable_writes.cpp`.

**#160 and #161**: a flush syncs its segment files - one `syncfs()` on the data directory, outside
`mtx_` - before the checkpoint that claims them, and startup removes every segment no surviving
checkpoint vouches for and rebuilds it from the WAL. Before it, a power cut after a flush lost the
rows a synced checkpoint claimed: measured with dm-flakey, **0 of 201**, now 201. Retention follows
only a checkpoint a WAL sync has covered, and **a failed sync of any kind freezes the checkpoints
until a restart** (`ob_checkpoints_frozen`), because the next sync after a failed one proves
nothing (pitfall 405). #161: a segment write the disk refuses is an error - it stored every price
as zero and answered `OK`.

**#159**: a flush's checkpoint claims the position its drain reached, not everything before
itself. The segments are written without `mtx_`, so records appended meanwhile precede the
checkpoint while their rows are still queued; replay skipped them, and a crash before the next flush
lost them under every fsync policy (measured: 3 of 3 acknowledged rows). The checkpoint's 8-byte
payload is that position, replay forwards everything after the last checkpoint plus what lies
between the position and it, and the position can only add records. WAL retention read the same
value (`Engine::drained_up_to_`) - the files before the drain's file, not before the current one,
which a rotation during the segment write had made the file holding those records (measured: 600
of 600 gone) - until #160 moved it one step further back, to the newest checkpoint known to be on
the device.

**#158**: `boost` is the command line's default - one client event loop per usable CPU, and a
10 µs spin window where the process may run on two CPUs or more and a cgroup limit leaves a CPU of
time beyond the loops. Measured with the binary (m9g.xlarge, four cores, 12 connections): reads
15.9 → **49.7 million levels a second**, writes 6.1 → **10.5 million**, `PING` 7.77 → 6.33 µs, and a
`PING` beside three pipelining writers 503 → **6.2 µs** p50, because it has a loop of its own. The
window is 10 µs rather than #144's 50 because under oversubscription the tail **is** the window
(p99 16.4 µs at 10, 53.9 at 50), and on one CPU the spin doubled a sharing client's p99, so boost
does not spin there. `eco` is the engine as it was; `ServerConfig{}` stays eco's values.

**#156**: the node works out how many CPUs it can use — the affinity mask and the tightest cgroup
CPU limit on the way from its cgroup to the root, rounded down — and says so at startup and in
`--print-config` (`machine: 1 usable CPU: affinity 4, cgroup v2 limit 1.50 CPUs (…)`). A cgroup file
it could not read is said rather than taken as "no limit"; a level with no `cpu.max` limits
nothing. Nothing is sized to it yet: that is stage 4 of #151, the default profile.

**#155**: the writes of one read are applied under one acquisition of `mtx_`, and their WAL records
reach the file with one `write()` per run - one `fsync` per run under `every`. One connection:
4 785 305 → **6 081 081** levels a second, `write()` per batch 64 → 1. Under `every`: 10 794 →
**543 260**, `fsync` per batch 64 → 1 (group commit). Four connections: **10.2 million at two loops
and 10.7 at four**, against 4.9 million on master at any count, and futex calls per batch 122.55 →
2.02. What is left is the lock itself, now saturated rather than contended - 43% of the server's
CPU inside it at four loops, 79% of the wall clock - and it holds the book, the rows queued for the
flush and the WAL's encode, of which the WAL's `write()` is a few per cent. `apply_delta()` and its
siblings are the batch with one write, so there is one write path.

**#151**: `--io-threads N` gives the server N client event loops, connections dealt to them in
turn and kept for life. Measured on the m9g.xlarge, every build with GCC 14, four connections:
**reads scale** — `BOOK` 16.0 → 30.3 → **49.2 million levels read a second** at one, two and four
loops — and **a query no longer stalls another loop's writes**: a writer beside a connection
looping a 134 ms scan went from 65 424 levels a second with a p99 of 136 ms to 4 995 672 and
241 µs. **Pipelined writes did not scale, and cost about 10% with a tripled p99**, because the
engine took `mtx_` once per record and held it through the WAL's `write()`: 1 128 futex calls at
one loop, 1 485 158 at four, every one of them in `apply_delta_impl` - which is what #155 fixed.
The default was 1 until stage 4 (#158) made it the profile's - one per usable CPU. Two things it fixed on the way
are their own items: #150 (a TLS handshake that could not start closed its descriptor twice) and
#152 (an answer above 64 MB closed the connection of a client that was reading).

**#147**: the io_uring transport is gone. Measured before it went, on the m9g.xlarge: a pipelined
batch of six commands came back as **177 bytes of the server's heap** — `submit_write()` reassigned
one per-descriptor string under every send already queued on it — `--fsync-policy every` gave **2
fsyncs for 200 acknowledged writes** against 201 on epoll, because its engine was built with
`FsyncPolicy::INTERVAL` as a literal, and it answered `PING` in 7,487 ns p50 against **6,295** for
epoll's `--profile boost`, at three times the CPU. With no measured advantage left, repairing two
defects of that consequence in a second copy of the server loop — and keeping that copy in step
with every feature, the multi-reactor stage included — lost to deleting it, the decision #114 made
about `MmapStore`. `-DOB_USE_IO_URING=ON` is refused at configure time with the item's number, and
its three flags are unknown to the parser, which refuses them by name.

**#146**: the answers to one read go out in **one** `send()`, and the read is 64 KiB, so one read
is one batch. A pipelined batch of 64 answers used to cost 64 sends and 64 segments — 32.4% of the
io thread, found by profiling the thread the next stage would parallelise, before parallelising it.
Measured on an m9g.xlarge with the node's data on the instance's disk, one connection:
**3.12 → 4.85 million levels/s**, batch p50 363.9 → 222.4 µs, the server's CPU for the same work
4.70 → 3.05 s, sends per batch 64 → 1. What it is not: more cores — the io thread is at 0.84 of wall
time afterwards and four connections still reach the rate of one, which is the next stage.
The bytes a client reads are unchanged, so the speed half is pinned by a static test over the loop's
shape (`ReadLoopStatic.EveryCommandFromOneReadIsAnsweredWithOneSend`), and the behaviour half by
`tests/integration/test_pipelined_answers.py`, including a failed `AUTH` ending its batch where it
stands. Reproduce with `scripts/measure_pipelined_ingest.py`, which refuses a data directory on
memory and a build that is not Release.

**#145**: `BOOK <symbol> <exchange> [depth]` returns the **live** book, in the Python client as
`book()` and in `ob_cli` as `book`. `SoABuffer` **is** the current book and `read_snapshot()` had
exactly one caller — the aggregate branch — so of eighteen wire commands none returned its levels,
and a client's two routes (`SUBSCRIBE` and rebuild, or `SELECT` history and replay) both made it
hold what the server already holds. It is the most orderbook-specific question there is and the one
where a column store is not slow but **structurally wrong**, which is also why the comparative table
said `NOT COMPARABLE` about the workload this engine is named after.
Measured on an m9g.xlarge over loopback, three rounds of 20,000 round trips per level count with a
`PING` baseline in every round: ten levels per side answers in **9.7 µs p50** of which 8.0 is the
round trip, and the marginal cost settles at **45–53 ns per level**. The honest reading is not the
flattering one — server CPU is 0.08 s for 20,000 `PING`s against **2.14 s** for 20,000 full
1000-per-side books, so a deep answer is TSV at about forty bytes a level rather than the seqlock,
and the read is not the thing to optimise. `apply_delta_impl`, `apply_delta_mm` and
`WALWriter::append` are identical instruction for instruction against master. The table is
re-measurable with `scripts/measure_book_latency.py`, which drives
`benchmarks/command_latency.cpp` and refuses an answer of the wrong shape.
Four things worth knowing before touching it. **One `read_snapshot()` per answer**, taken before any
row is emitted: reading a side as its rows are formatted composes the answer from two moments
separated by up to two thousand levels of formatting, and a book assembled that way can show a
crossed spread the market never had. It is **not** an atomic read of both sides, and `docs/cli.md`
says so — that needs one seqlock per buffer instead of one per side, which is a write-path change.
**Two of the seven columns come from the buffer**, so every row of one answer carries the same
`timestamp_ns` and `sequence_number`; that is the identity of the snapshot and the number to resume
a `SUBSCRIBE` from, not a per-level time. And **at capacity a side evicts its worst-priced level**
rather than refusing the new one, which is what makes "the best N levels" a question all three
systems can be asked — a design property nobody had reason to write down until the benchmark needed
it.

**#144**: `--profile eco|boost`, with `--io-spin-us` underneath. The io thread always blocked
between events, so every round trip paid a kernel wake-up. Measured **through the flag** on an
m9g.xlarge over loopback, seven interleaved rounds of 20,000 `PING` round trips: p50
**8074 → 6578 ns**, p99 **8369 → 6837**, for **+33%** server CPU — 91% of a core against 56% while
the probe saturates it. What says constant rather than tail is that **p50 and p99 fall by the same
absolute amount** (1496 and 1532 ns); a tail would take far more off p99. A third column settles
what the window costs: `--io-spin-us 100000000` never closes, gives the **same p50** for 98% of a
core, so the bounded window gives up no latency under traffic and buys back the idle cost — which
is the whole reason the public cost is "up to one core while traffic flows" rather than a core.
Two things not to overstate. It is **loopback on one machine**: across a real network 1.5 µs is
noise, so the mode is worth what it is worth to a *colocated* client. And a profile is a **named
set of the knobs, not a second code path** — nothing in the loop branches on its name, `eco`
resolves to the default `--io-spin-us 0`, and `--print-config` attributes a value the profile chose
to the profile, because a mode whose effect cannot be read is a mode on somebody's word.
**One claim from the gate measurement is withdrawn**: that run reported the minimum unchanged and
this one does not — `eco`'s is 6327-7689 ns against `boost`'s 5955-6076. It does not reproduce with
this probe and the roadmap says so rather than swapping the sentence. **Since #158** `boost` is the
default and its window is 10 µs, not the 50 these figures were measured with: the same gain, without
the 50 µs tail a spinning loop costs a client when the cores are oversubscribed.

**#143**: one session's unparsed input is bounded. A client that sent bytes and never sent a
newline took the server's resident memory to **257 MiB**, needing no authentication, and
`--max-line-length` could not bound it — that check runs on a line that has already been assembled,
and a client that never sends a newline never assembles one. The cap is one number over **both**
accumulations (`Session::unparsed_bytes()`), because a pending `MINSERT` collects payload lines that
never come back from `feed()` either, so a fix for the receive buffer alone would have left the
second route open.

**#142**: a snapshot install **replaces** the store. It used to rename the received files in and
remove nothing, so a replica that had flushed a *prefix* of a symbol kept its own segment beside
the arriving one and answered with both — 122 rows against the primary's 100. The green CI runs
were never "no defect": a replica that flushed the **whole** set produced a directory named
identically, so the rename overwrote it.

**#136**: a directory belongs to one segment rather than to one event-time span, so re-running a
backfill stores a second segment instead of destroying the first. Measured over the wire: **8000
rows where it read 4000**, and **5000 where it read nothing at all**, with zero `ERROR` lines
where there were 200. Two things worth carrying from it — the item's own costing said this was the
widest of three candidates and it is one function with one caller, because **nothing parses a
segment directory name**; and `segment_order_less` had been asserting in its own comment that
`dir_path` is unique per segment, which is precisely what the defect broke.


Things a newcomer should know, because they are real limits rather than bugs to file again:

- **The wire protocol authenticates and encrypts on all three surfaces, and every one of those
  flags is off by default.** Authentication came with #30 parts one and two — client sessions with
  `--auth-secret-file`, the replication link and the multi-master mesh with `--cluster-secret-file`
  — by challenge-response over HMAC-SHA256, so a secret never crosses the wire. Encryption came
  with #30 part three: `--tls-client` for client sessions (series C, PR #80) and
  `--tls-replication` / `--tls-multi-master` for the node links (series D, PR #81), TLS 1.3 with no
  configurable floor, and on the node links verification is mutual and cannot be configured
  otherwise. *(This bullet said "nothing is encrypted" until 7 September 2026, four weeks after
  that stopped being true. A limit that has been lifted and is still documented as a limit is the
  same defect as one that was never documented.)*
  What is still true: **an unconfigured node is plaintext and unauthenticated on all three
  surfaces**, and the startup log WARNs for each disabled surface rather than leaving "default open"
  in a document.
  Certificate rotation needs a restart. Three things to know before touching authentication: the
  client gate sits *before* `execute_command`'s switch and its classifier has no `default:` (pitfall
  109); the surface label is inside the HMAC input because replication and multi-master share one
  secret; and the two secret files must differ, which the start enforces, because a client holding
  the cluster secret can present itself as a replica and stream the whole write-ahead log.
- **The whole integration battery runs under ThreadSanitizer**, not a subset — since #85.
  Unexpected skips and sanitizer reports fail the job. Before that the job ran three multi-master
  modules, and the reason
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
- **A replica keeps its store across a restart and resumes; a failover is still a full re-sync**
  (#101). The primary announces the identity of the WAL it writes — `STREAMID?` answered with
  `STREAM <id>`, asked **before** `REPLICATE` so a primary that does not know the command answers
  nothing and streams nothing — and the replica saves that identity beside the position in
  `repl_state.txt`. A match resumes; anything else discards and replays from zero. Three things
  follow that are easy to expect otherwise. **A promotion is "anything else"**: the identity belongs
  to a data directory, so every surviving replica re-syncs in full after a failover — correct rather
  than unfinished, because the promoted node may be *behind* this one and dedup makes over-delivery
  safe without making a divergent suffix safe. **Restoring a primary from a backup draws a new
  identity**, so it costs every replica a full re-sync. And **the discard decision is not made by
  `demote_to_replica()`** — it needs the primary's identity, which exists only after the connection,
  so it lives in `ReplicationClient::resolve_stream_identity()` and a static test pins that nothing
  about which of the four callers demoted the node reaches it.
- **Nothing in this engine memory-maps a file for writing, and that is load-bearing rather than
  incidental** (#114). A growable mapping is the one shape in which a full filesystem arrives as
  **`SIGBUS`** instead of `ENOSPC`: `ftruncate` extends sparsely and succeeds, and the allocation
  fails later, on first touch, where there is no return value. Measured on an 8 MB tmpfs — 64 MB
  reserved, `Bus error`, exit 135, against a passing 2 MB control. A signal is not an exception, so
  `run_thread_body()` cannot catch it and no client can be told; mapping the segment write path
  would undo #112 and #113. `MmapStore` was deleted for this reason rather than benchmarked. What
  it does not settle: `ts.col`, `cnt.col` and `side.col` are raw fixed-width arrays, so a mapped
  *reader* could still skip a copy — the deleted class was an appender, so that question is open,
  and the argument "the columns are compressed so there is nothing to map" is false for those three.
- **`--fsync-policy every` means what it says, and a failed `fsync` is refused rather than
  acknowledged** (#113). `WALWriter::flush()` and `sync()` are `[[nodiscard]] bool`, so discarding
  the answer is a compile error; the write paths, `FLUSH`, both snapshot paths and the flush tick
  throw, and `close()` logs and completes. `ob_wal_fsync_errors_total` is separate from
  `ob_flush_errors_total` because a full disk is freed while a disk returning `EIO` is replaced, and
  it is **sticky**: a failed `fsync` cannot be retried on Linux, which reports the error once and
  marks the pages clean. Two consequences to expect rather than file: a client that resends after
  the refusal produces a **second row**, which nothing deduplicates (the sequence-number dedup is
  about a second *delivery* of one record), and the columnar segment files are still buffered stream
  I/O with no per-segment `fsync` — that is what WAL replay is for. See `docs/operations.md`, "When
  an fsync fails".
- **No thread in this engine can end the process by letting an exception escape** (#112). All
  seventeen `std::thread` constructions in `src/` wrap their body in `run_thread_body()`
  (`include/orderbook/thread_boundary.hpp`), and a test derives that count from the source rather
  than from a list — hand-counting gave eleven. A joinable thread whose body throws calls
  `std::terminate`, so an `ENOSPC` on the flush thread's WAL write used to abort the whole node in a
  crash loop while the client-facing path handled the same condition correctly.
- **Every loop in `src/` guards one iteration at a time, and the set is derived rather than listed**
  (#112, #131). Thirteen loop functions: six were closed one at a time — `Engine::flush_loop`,
  `FailoverManager::monitor_loop`, `MultiMasterManager::io_loop`, `ReplicationManager::run_loop`,
  `PeerRegistry::lease_loop`, and `ReplicationClient::run_loop` which had done so since before
  #112 — and the other seven together through `LoopGuard`
  (`include/orderbook/loop_guard.hpp`). A hand-written list of four survived a mutation that
  deleted a row from it, which is why `tests/test_thread_boundaries.cpp` derives the set from the
  tree and checks both directions; a fourteenth loop has to join a list or explain itself.
  Four things worth knowing before writing that loop. The unit is **one pass** unless an abandoned
  iteration loses something no later one redoes — which in this tree means `EPOLLET`, and is why
  the mesh and replication loops guard per *event*. The loop whose epoll timeout is **zero** while
  a catch-up cursor has queue space is `ReplicationManager::run_loop` and not the mesh `io_loop`
  (which waits 500 ms). Giving that loop a boundary **created** a spin — `wait_ms` outlives a pass,
  so the failure path goes through `replication_wait_ms()` like the idle path. And a boundary is not
  a substitute for owning a resource: `MetricsServer::handle_request` closed its socket at each
  `return` and at neither path that throws, so the guard around it would have counted a leak rather
  than prevented one; the descriptor is closed by scope now.
- **A mesh link is breakable from the harness, and the proxy that does it needs no engine change**
  (#54 stage C). `tests/integration/mesh_proxy.py` sits between two peers;
  `ClusterManager.redirect_peer()` puts it there by overwriting the address
  `PeerRegistry::register_self()` published, which works because nothing writes that key again
  while it **exists** — since #132 a node does re-register, but only on a key it has read back and
  found **absent**, which is why that fix keeps the one-shot property this fixture stands on rather
  than just being cheap. Four things to know before using it, each of which cost a wrong test
  first.
  **`partition()` buffers and stops reading** — buffers because a real partition delays bytes
  rather than deleting them (deleting them is `close_connections()`), and stops reading because a
  proxy that kept draining would hide the fault from the sender, which is the half that has to be
  visible. **`stall_after(n)` is a property of the link, not of a direction**: which way the writes
  travel depends on who dialled, and a symmetric mesh does not let a test choose.
  **Both peers must be redirected**, because the surviving link is whichever was dialled first
  (#96 measured zero double links), so redirecting one leaves the other direct — and the fixture
  therefore **asserts that a proxy carried bytes** before any test trusts it.
  **A restart undoes a redirect**, because registration is part of starting.
  What the engine promises, measured: after `heal()` the mesh converges **by row content**; a frame
  cut at byte 20 of a 38-byte header is **not applied in part**; and a reconnect's catch-up
  re-delivers records without storing any twice. What it does **not** promise today is #117.
- **Every lag this engine reports is a pair now, and the second half names what the first cannot
  say** (#123 and #124 closed). The replica lag was the last one wrong: `stats()` computed
  `current_offset - confirmed_offset` with the **file index ignored**, and `rotate()` resets the
  current offset, so a replica more than a file behind read **zero bytes behind**. It goes through
  one `WALWriter::bytes_since()` now — exact, across files, asking the filesystem for each
  intervening size rather than estimating from the rotation threshold, because an estimate is
  excellent at 512 MB and out by a quarter at the thresholds tests use. A missing file in between
  answers `nullopt`, which reaches an operator as `lag=unknown` in `STATUS` and as
  `ob_replicas_lag_unknown` — and that condition is **worse** than a large lag, because retention
  keeps files back to the slowest connected replica. The cross-file path **is** covered end to end
  since #124 made the rotation threshold a flag (`--wal-rotate-bytes`, refused below one maximal
  record and above 2 GiB): `tests/integration/test_wal_rotation.py` stops a replica with `SIGSTOP`
  — connected, counted, acknowledging nothing — and requires a lag bigger than a whole file beside
  a zero unknown count, which is the same fact from two sides, since the distance is measurable
  precisely because retention kept the files.
- **UBSan found undefined behaviour older than the mesh, and it took a test that reads a frame off
  a socket in that build to see it** (#127, closed). Every receive path cast a pointer into its byte
  buffer to `const Level*`; a mesh frame puts the levels 130 bytes in, which is not a multiple of
  eight. Fixed as a class through one `levels_from_payload()`, with the cost measured: the parsing
  function grows 188 → 393 instructions for one `memcpy`, and both apply paths are identical.
- **Fault injection (#54) is closed, and so is its last measurement, #126.** An acknowledged write
  that landed behind a **torn** WAL record did not survive a restart: the write that tore is
  refused, the ones after it were answered `OK`, and `WALReplayer::replay()` returned at the first
  CRC mismatch rather than breaking out of one file. Measured 2 of 2 stranded, then **0** after the
  fix — which is two halves, neither useful alone: the writer **abandons** a file whose record it
  tore (no ROTATE marker: its first act would be a write into the file just established as
  unwritable) and replay treats a mismatch in any file but the **last** as a tear. It fires only
  when the write left bytes behind, because abandoning on every failed write would give a full disk
  one empty WAL file per refusal. The stage that found it needed a composite fault the injector
  could not express — `OB_FAULT_SHORT_THEN_FAIL`, because a short write alone cannot tear a record
  `write_record()` resumes.
- **A snapshot's checksum covers what the wire carries, and before #125 it covered two fields the
  wire has never sent** (closed). `SNAPSHOT_END` named `crc32c(manifest.to_json())`, and that
  document includes `created_at_ns` and `total_rows`, which the primary fills and nothing
  transmits — so the receiver's reconstruction differed by construction and **no** bootstrap on the
  replication path ever verified. Measured on a live pair: 24 of 24 files arrived with every
  per-file CRC verified, the manifest CRC disagreed, and the replica asked again every five seconds
  holding zero rows. `SnapshotManifest::transferred_digest()` is now called by both ends. Reachable
  only since #124, because retention removes a file a replica needs only after a rotation — and the
  only unit test of the path used a mock primary built to agree with the **receiver**, which is why
  nothing caught it; that mock now fills in both fields, as a primary does.
- **A new reader of shared state changes how often a latent race fires, and that is part of the
  change** (#122, closed). `repl_client_` was read under `mtx_` by `stats()` and written without it
  by `promote_to_primary()`; the fix is the idiom `demote_to_replica()` ten lines down already
  used — take ownership under the lock, join and destroy through a local. It was filed rather than
  fixed until TSan went red on the PR that added a publisher reading that pointer **every flush
  interval** instead of on a `STATUS` command, and the report named `operator delete` as well as
  `reset`, so it is a use-after-free window rather than a torn pointer.
- **Four of #117's five dead metrics are fed; the fifth is #118's to rename, not ours to fill.**
  `ob_wal_records_written` counts **every** record type in one shared `advance_after_write()` —
  both write paths had the identical five-line accounting block, which is #94's shape, and a
  mutation giving the v2 path its own copy without the counter kills the record-type test while
  leaving the rotation test green. It reads higher than the append count on purpose: each rotation
  writes a `ROTATE` record, which is how the rotation test found out (it asserted forty and read
  fifty). Cost measured, not argued: **+3 instructions** per record in both write paths, no new
  `lock`-prefixed instruction, `apply_delta_mm` unchanged. `ob_repl_records_replayed` needed only a
  publisher — and pitfall 250 — because `STATUS` has printed it as `replayed=` all along. The
  io_uring pair went with its transport (#147).
- **The hybrid logical clock never goes backwards, and a peer's clock can no longer become the
  mesh's clock without limit** (#119, #120, #121 closed). The reversal was the `uint16` `logical`
  counter wrapping while the physical component was pinned above the wall clock — measured at tick
  65 533, against a control of zero regressions over the same 200 000 ticks — and the overflow now
  carries into the physical component, which is why
  `HLCSkew.CrossingTheLogicalPeriodProducesAStrictlyGreaterTimestamp` exists beside the
  monotonicity test: saturating the counter would keep the second green and stop breaking ties.
  Two numbers describe drift and you need both, because `ob_mm_hlc_drift_ns` is a **peak that never
  comes down**: `ob_mm_hlc_drift_excursions_total` says how often.
  The bound from #121 is **five minutes, at the mesh door, against the wall clock**, and it refuses
  the **peer** — `MultiMasterManager::drop_peer_if_clock_is_implausible()`. Not the record (a
  refused record under a live link is a silent hole), not a clamp (decided per node against that
  node's wall clock, so two nodes pick different LWW winners and the **data** diverges), and not in
  the clock class, which is still uncapped on purpose: a clock that declines part of what it is told
  breaks the invariant it exists for. The right way to say what the bound is for: the mesh's clock
  is the **maximum** of its members' clocks and nothing bounded the maximum. It does not buy a clock
  that means wall time — four minutes of skew is still absorbed — and it makes
  `resolve_logical()`'s saturation branch unreachable, which matters because that branch runs the
  clock backwards by the whole counter and its comment used to claim otherwise.
  A row's `timestamp_ns` was never this clock: it is the client's `event_time_ns` (#105) or the
  receiving node's `system_clock`, so retention and time-range queries are untouched by mesh skew.
  LWW still converges under opposite drift, measured from both sides.
- **The mesh reports its lag in records now, as a pair, and the replica lag is still unpublished
  on purpose** (#118 closed, #123 open). `STATUS`'s `replication_lag_peer_<id>` and `MM_PEERS`'
  `lag_bytes` are gone and renamed respectively — the first removed because the honest number is a
  different unit from a different mechanism, so keeping the field would have left readers parsing
  one line about another subject; the second is `send_queue_bytes`, which is what it always held.
  `ob_mm_replication_lag_records` comes from `compare_vectors()` in the anti-entropy pass, so it is
  as stale as `--anti-entropy-interval-seconds`, and it is **useless without
  `ob_mm_peers_position_unknown`**: the comparison reports a peer that has said nothing as holding
  nothing, so those peers are excluded and counted, and zero lag with a nonzero unknown count means
  "we do not know". That pair closed the second clause of #54's C3, which stage C had recorded as
  untestable because of #117. What is **not** published is the replica lag (#123): it is the one
  genuine number here and it ignores the WAL file index, so a replica more than a file behind reads
  zero.
- **The mesh had two fields named after a replication lag and neither was one** (#118, found while
  planning #117). `STATUS`'s
  `replication_lag_peer_<id>` subtracts a mesh peer's `confirmed_offset` — a position in the
  peer's **own** WAL, written once in `process_handshake()` — from ours, so on a mesh converged by
  row content it equals this node's own WAL offset **to the byte** — which is to say the engine
  believes a peer holding every row is at byte **zero** (`scripts/measure_mesh_lag.py` prints that
  subtraction as its own column, because two equal numbers read as a coincidence and a peer
  believed to be at zero does not). `MM_PEERS`'
  `lag_bytes` is `send_buf.size()`, which #117 measured as flat through 4 MB. The per-replica lag
  is genuine — live, refreshed on every `ACK` — and reaches an operator only by reading `STATUS`
  by hand. So before adding a lag number anywhere, ask the two questions in pitfall 244: same log,
  and is the subtrahend refreshed. The honest mesh answer is in **records**, from
  `compare_vectors()`, which is also what #54's requirement 1.4 asks for.
- **An unreachable coordinator is injectable too, and the node's promise is a refusal** (#54 stage
  B). `ClusterManager.stop_etcd()` / `start_etcd()` are public; the restart reuses the **same client
  port and data directory**, because a node is handed `--etcd-endpoint` on its command line at
  start, so a coordinator returning on a fresh port has not returned as far as that node is
  concerned. What the engine promises, measured across three windows: a node that cannot read the
  leader key **does not take the role** (#82's three-state answer), a holder whose lease keepalive
  fails **gives the role up in 2.28 s** rather than waiting out a TTL, reads keep being served, and
  nothing exits. **What the log says about it is one line per condition, since #115 and #116**: the
  refusal to campaign is INFO on the tick the episode opens, and the two position-publish failures
  say so once and once more when they recover rather than once a second. Measured, before and
  after: 2.17 and 2.23 lines/s per node became **0.40 and 0.45**, and the refusal went from **zero**
  mentions at the default level to exactly one, with the 2.28 s step-down unchanged as the control. The intent is recorded to **annotate** `unexplained_deaths()`, not to
  suppress it — a node that dies while the coordinator is away is the defect that stage hunts.
- **Storage faults are injectable, and the instrument is `tests/fault/obfault.c`** (#54 stage A) —
  an `LD_PRELOAD` shim over `write`, `pwrite`, `fsync`, `fdatasync` and `ftruncate`, armed by
  `OB_FAULT_PATH` and friends, matching by the path behind the descriptor. It found #112, #113 and
  #114 on its first run. Two things to know before using it: an empty `OB_FAULT_PATH` **disarms**
  it, and both integration CI jobs build it and assert the shared object exists, because the tests
  in `test_fault_injector.py` and `test_storage_faults.py` **fail rather than skip** when it is
  missing — deliberately, since everything they assert would otherwise pass without any fault being
  injected (pitfall 57). `OB_FAULT_DELAY_MS` makes the chosen write or sync **slow instead of
  failed** (#159): it holds a flush's segment write open for seconds, which is what made the window
  between a drain and its checkpoint testable. `OB_FAULT_OP=syncfs` is the flush's segment sync
  (#160), matched by the data directory its descriptor names. It fails the call in userspace and
  leaves every page as it was, where a real failure marks them clean - the one thing the injector
  cannot reproduce (pitfall 406).
- **A power cut is testable, and the instrument is `tests/integration/test_power_cut.py`** (#160):
  dm-flakey over a loop device, switched to drop every write, the way xfstests simulate one. It needs
  root and the module, so it runs with `OB_POWER_CUT_TESTS=1`, which both CI integration jobs set -
  and with it set a missing prerequisite **fails**, because a power-cut test that skips looks
  exactly like code that survives the cut. It is the only test here that is not a killed process:
  the page cache survives a kill.
- **A wire write can carry its event time** (#105). `INSERT` and `MINSERT` accept an optional
  trailing `event_time_ns`; absence asks for arrival time, while zero is refused. Both clients check
  the `insert_event_time` capability before sending a supplied time and refuse an older server
  rather than dropping the value. LWW still compares the node's HLC; TTL follows the stored event
  time, so backfill arrives with its age. See `docs/operations.md`.
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
- **CRC32C folds with a hardware instruction on x86_64 *and* on aarch64**, chosen at run time
  (`getauxval(AT_HWCAP) & HWCAP_CRC32`), with the table as the fallback for an ARMv8.0 part — one
  binary either way, and the startup log names which one is running rather than asserting SSE4.2
  as it used to. The ARM path was added because the first run on aarch64 measured the table:
  193.66 ns against 6.68 ns for a 112-byte WAL record, which is **2.4%** on the whole ingest path,
  not the 29× the isolated fold suggests. GCC 14 and clang 15 on this architecture disagree about
  every spelling of the intrinsic except inline asm carrying `.arch_extension crc`, which is
  measured to be no slower than either.
- **The whole suite has run on a weakly-ordered memory model**, which it never had before
  19 September 2026: 1102 tests pass on aarch64 in Release, again under ThreadSanitizer with zero
  reports, again under AddressSanitizer + UBSan with zero findings, and the **integration battery**
  there too — 273 passed with the two opt-in skips in 22:46, zero unexplained node deaths. That
  fourth one matters on its own: the C++ passes drive the apply path with aligned `Level` arrays,
  which is the gap #127 lived in, and the battery is the only thing that takes a delta off a
  socket. That is the strongest
  evidence available and it is not proof — a race is probabilistic and TSan reasons about
  synchronisation rather than about the hardware — but until that day every seqlock and every
  atomic in this engine had only ever executed on x86, which is TSO.
- Every FetchContent dependency, including `rapidcheck`, is pinned to a commit SHA.

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
