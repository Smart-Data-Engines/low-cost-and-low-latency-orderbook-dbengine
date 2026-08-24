# Roadmap — orderbook-dbengine

Development plan towards production quality, High Availability, and HFT readiness.

Two things drive the ordering below: what blocks someone from running this engine in production, and
what an engineer evaluating it needs in order to verify our claims themselves.

**The engine runs natively on the host. There is no containerised deployment path and there will not
be one.** Containers add a layer of overhead between the engine and the hardware, which defeats the
point of an engine built for specific hardware. Packaging, tooling and cluster setup target bare
metal: native binaries, systemd units, and configuration that exposes CPU pinning and memory tuning
rather than hiding it.

Effort scale: S = few days, M = week, L = 2-3 weeks, XL = month+.

**Item numbers are permanent identifiers.** A new item takes the next free number and is placed
wherever it reads best, so numbering is not ascending down the page and is not meant to be. Renumbering
to keep it tidy costs more than it buys: three passes over this file each damaged something. A `#47-48`
range became `#48-48`, because a rewrite only touches the bound carrying a `#`. References drifted onto
neighbouring items while still resolving to *an* item, which no existence check can see. Commit
messages, specs and `CLAUDE.md` cite these numbers, and every renumbering invalidates those citations
too. `scripts/check_roadmap.py` verifies what is verifiable: unique ids, resolving references, and
ranges that ascend. Whether a reference points at the item it means is on the reader.

---

## Phase 1 — Production Hardening ✅

### 1. Configurable fsync policy
- Status: **DONE** — `FsyncPolicy` enum (EVERY, INTERVAL, NONE)
- Worth knowing: for four months this bought nothing. Records were fsynced faithfully and then never
  read back, because replay applied an empty callback (#62). A durability knob is only as good as the
  recovery path behind it.

### 2. Graceful shutdown with drain
- Status: **DONE** — `draining_` state, rejects new connections, waits for in-flight

### 3. WAL truncation / compaction
- Status: **DONE** — `WALWriter::truncate_before()` + auto-truncation in `flush_loop()`

### 4. Backpressure on pending_rows_
- Status: **DONE** — `MAX_PENDING_ROWS=1M` + `condition_variable` backpressure

### 5. Monitoring endpoint
- Status: **DONE** — `Engine::stats()` + extended `ServerStats` (replication, failover, compression, TTL metrics)

## Phase 2 — Replication ✅

### 6. WAL streaming replication
- Status: **DONE** — `ReplicationManager` + `ReplicationClient`, CRC32C verify, exponential backoff

### 7. Read replicas
- Status: **DONE** — `--read-only` flag, replica replays via `apply_delta()`

### 8. Replica lag monitoring
- Status: **DONE** — per-replica `lag_bytes`, WAL truncation respects slowest replica

## Phase 3 — High Availability ✅

### 9. Automatic failover
- Status: **DONE** — `EpochManager`, `CoordinatorClient` (etcd v3 REST), `FailoverManager`

### 10. Client-side failover
- Status: **DONE** — `_ClientPool` with auto primary discovery, write routing, read fallback

### 11. Fencing / split-brain protection
- Status: **DONE** — Epoch in wire protocol, stale-epoch fencing, `ERR STALE_PRIMARY`

## Phase 4 — Performance & Features ✅

### 12. Snapshot-based replica bootstrap
- Status: **DONE** — `create_snapshot()`, chunked transfer, auto-bootstrap on WAL_TRUNCATED

### 13. Wire protocol compression (LZ4)
- Status: **DONE** — Replication stream + query session compression, `COMPRESS LZ4` handshake

### 14. TTL / data retention
- Status: **DONE** — `--ttl-hours`, `--ttl-scan-interval-seconds`, per-node retention

### 15. Incremental flush (non-blocking)
- Status: **DONE** — Two-phase flush (drain under mutex, write without), 82ms → 2ms

### 16. Batch INSERT (MINSERT wire protocol)
- Status: **DONE** — `MINSERT` command, single `apply_delta()`, Python auto-batch, 85ms → 3ms

### 17. Stress testing & load benchmarks
- Status: **DONE** — C++ (5 scenarios) + Python TCP (2 scenarios), 777k levels/s sustained

## Phase 5 — HFT Production Readiness ✅

### 18. Observability stack (Prometheus + structured logging)
- Status: **DONE** — Prometheus `/metrics` HTTP endpoint (counters/gauges/histograms), `StructuredLogger` (JSON, log levels), `MetricsServer`, latency histograms

### 19. Failover integration tests with real etcd
- Status: **DONE** — a local etcd instance, 13 C++ tests, 4 Python tests, full failover cycle verified

### 20. C++ native client library
- Status: **DONE** — `OrderbookClient` + `OrderbookPool`, zero-copy, pre-allocated buffers, ROLE discovery, failover

### 21. io_uring transport layer
- Status: **DONE** — `IoUringServer` with SQPOLL, registered buffers, `OB_USE_IO_URING` compile flag. PING 24µs (vs 45µs epoll)

### 22. Symbol-based sharding
- Status: **DONE** — `ShardMap` + `ConsistentHashRing` (MurmurHash3, virtual nodes), `ShardCoordinator` (etcd registration, rebalancing, migration), `ShardRouter` (C++ client routing), Python `_ClientPool` sharding mode, wire protocol (SHARD_MAP, SHARD_INFO, MIGRATE), 10 property-based tests

### 23. Integration test suite
- Status: **DONE** — the framework, and since #28 the tests too. Kept here for the history: the
  framework was DONE (`ClusterManager` auto-boots etcd plus two nodes, fixtures, colored console report, marker-based categories), but **the test files themselves are missing from the repository**. A `test_*` pattern in `.gitignore` silently excluded every `tests/integration/test_*.py`, so the ~37 tests across 9 categories were never committed. The `.gitignore` is fixed and the suite was rewritten from scratch — see #28.

## Phase 6 — Write Scalability ✅

### 24. Multi-master replication
- Status: **DONE** — HLC (Hybrid Logical Clock), WALRecordV2 (38B, carries `origin_node_id` for loop prevention), `ConflictResolver` (Last-Writer-Wins per cell), `PeerRegistry` (etcd peer discovery with topology watch), `AntiEntropyManager` (periodic reconciliation), `MultiMasterManager` with a full TCP networking layer: length-prefixed framing, 17-byte handshake with protocol negotiation, unified epoll io_loop, catch-up streaming from a peer's WAL position, backpressure to snapshot sync above 512MB, exponential-backoff reconnect with jitter. Wire protocol commands `MM_PEERS` / `MM_CONFLICTS`, CLI flags, metrics, failover integration. 97 spec tasks, property-based tests for framing, backoff and catch-up ordering.

---

## Phase 7 — Correctness and Deployability

**Why this phase is first.** Everything up to here is engine capability. What stands between this
engine and a production deployment is not another feature. A shipped feature still does not do what it
claims (#56, anti-entropy reconciles nothing), the wire protocol has no authentication, there is no
configuration file, and there is no packaging. Someone who reads the code and likes it still cannot run it. Fix the broken
promises first, then remove the deployment blockers.

### 25. Columnar segments lost the order side ✅
- Status: **DONE** — format version 1 stored only `ts`/`price`/`qty`/`cnt` and zeroed `side`,
  `level_index` and `sequence_number` on read. Since the default flush interval is 100ms and
  `ob_tcp_server` has no flag to change it, **every row came back as a bid at level 0 within a tenth
  of a second of being written**. For an L2 orderbook store that makes spread, mid-price and
  imbalance meaningless on stored data, all three of which the README advertises.

Found by the first integration test written for item #28, not by the 531 C++ tests, because
`make_row()` in `test_columnar_store.cpp` hard-coded `side = SIDE_BID`. A field that no test ever
varies is untested however many assertions mention it.

Format version 2 stores all seven columns. `seq.col` uses zigzag-delta followed by Simple8b, which
costs 0.27 bytes per row against 8.00 for zigzag alone. A segment with an unknown version, a missing
column or a short column is now skipped with an error rather than read with zeroed fields.

Second defect found while checking the first: `create_snapshot()` matched an allowlist of column
names, so it would have shipped replicas segments without the new columns, which the reader then
rejects as incomplete. Data loss two components away from the change that caused it. Now matched by
extension, with a test that fails if a column on disk is absent from the manifest.

Verified: 540 tests passing, mutation-verified per field, and benchmarks measured before and after in
the same conditions show no regression (p50 10576ns vs 10736ns at cv ~1.3%). Spec:
`kiro-workspace/specs/columnar-side-level-seq/`

### 26. Concurrent flush registered the same segment twice ✅

Two threads could flush at once — the 100ms background `flush_loop()` and a client `FLUSH` — and
`flush_write_and_merge()` ran outside `mtx_` by design so segment I/O would not block writers.
`ColumnarStore` had no lock over its active-segment state, so both callers saw the same active
segment, wrote the same directory and returned a valid `SegmentMeta`; `merge_segments()` appended both
without deduplicating. One segment on disk, two entries in the query index, and `SELECT` returned
every row in it twice. No error, no warning: aggregations over the doubled rows looked plausible.

It surfaced as a flaky integration test failing 1 run in 6 with `expected 3 rows, got 6`, duplicates
carrying identical timestamps. Sequential idempotence was already covered and passing — two `FLUSH`
calls in a row are fine, because the second finds empty buffers. Nothing tested two at once.

Three more defects sat in the same code, all found by writing the tests before the fix:

- `flush_write_and_merge()` iterated `stores_` unlocked, while `load_snapshot()` and the REPLICA
  transition call `stores_.clear()` under `mtx_`. That destroys the `ColumnarStore` objects Phase B is
  iterating: use-after-free on any node taking a snapshot or being demoted, presenting as a crash
  unrelated to its cause.
- `create_snapshot()` and `create_symbol_snapshot()` called `flush_segment()` and discarded the
  returned meta. `QueryEngine` reads `combined_store_` only, never the live SoA buffer, so a snapshot
  made the rows it had just persisted invisible to every query until the next `open_existing()`.
  Measured: 0 of 5 rows queryable after `SNAPSHOT`.
- `ColumnarStore::append()` discarded the meta of a segment closed by a rollover, so data older than
  one segment duration vanished from queries the same way. Measured: 3 of 6 rows.

Fixed with a single `flush_mtx_` serialising every path that writes segments or mutates `stores_`
(lock order `flush_mtx_` → `mtx_` → `index_mtx_`, with `demote_to_replica()` deliberately not holding
it across `repl_mgr_->stop()`, which would deadlock against a replication thread inside
`create_snapshot()`), Phase B working on a snapshot of the store list, `take_rolled_segments()` for
rollover metas, and both snapshot paths merging what they flush.

`merge_segments()` also refuses a directory already in the index and counts it in
`segment_merge_refused`, exposed in `STATUS`. That refusal is a backstop, not the fix — and it taught
us something worth recording: with it in place, deleting `flush_mtx_` no longer failed the
concurrency test, because the duplicate was caught after the fact and the row count stayed right. A
test that only checks the symptom cannot verify the cause. The test now asserts the counter is zero,
which fails within one round without the lock.

Verified: four tests written red first, then green; each of the four fixes mutation-verified
separately; 544 tests passing; the integration suite six consecutive clean runs where it previously
failed 1 in 6; `BM_UpdateLatency` p50 unchanged within noise (the lock is taken once per flush, not
per row). Spec: `kiro-workspace/specs/flush-race-duplicate-segments/`

### 27. Aggregations are unreachable over the wire protocol ✅

Every aggregate query answered `OK` plus a single row of zeros. `format_query_response()` had one
fixed row header and never read `QueryResult::agg_values` — one write site in the whole repository,
zero read sites. The engine computed the values correctly the entire time; nothing carried them to a
client. Not the Python client, not the C++ client, not `nc`; only code linking the engine directly.
The README advertised all four functions.

Worse than an error, because `OK` plus a number invites belief. For an orderbook store it meant
spread 0 and imbalance 0 — two values that read as a signal in a trading system.

Writing a test that runs an aggregate through `execute()` — which nothing had ever done — turned up
four more defects in the same feature:

- **`empty` was dropped**, so a spread on a book with only one side was indistinguishable from a
  spread of zero. It is now `NULL` on the wire, never `0`.
- **Scales were tribal knowledge.** `VWAP` and `MID_PRICE` are multiplied by 10⁶, `IMBALANCE` by 10⁹,
  the rest not at all, and this lived only in header comments. A client reading mid-price as a raw
  price is wrong by a million. The factor is now returned by the function that applies it
  (`AggResult::scale`) and travels in the response.
- **The argument was decoration, and it lied.** The dispatcher calls `sum_qty()` for `SUM` and
  `avg_price()` for `AVG` regardless of the argument, so `SUM(price)` returned a sum of quantities
  labelled `SUM(price)`, and `AVG(quantity)`/`MIN(quantity)` returned price statistics. Arguments are
  now validated against what each function actually aggregates.
- **`DEPTH_RANGE` could only ever answer `NULL`.** The parser rebuilds the expression text with `", "`
  between arguments, so the second bound always arrived as `" 101000"`; `std::from_chars` refuses a
  leading space, `parse_i64()` turned that failure into `0`, and `[lo, 0]` is an empty range.
  `test_aggregation.cpp` tests `depth_within_range()` directly, so it stayed green.

Aggregates read the live SoA book, so a timestamp or price filter cannot be honoured. Both used to be
accepted and ignored; both are now refused by name (`AGG_TIME_FILTER`, `AGG_PRICE_FILTER`), as is
mixing aggregates with plain columns (`AGG_WITH_COLUMNS`).

Response shape — one row per aggregate, self-describing:

```
OK
name	value	scale
SPREAD(*)	1000	1
MID_PRICE(*)	100500000000	1000000
IMBALANCE(10)	250000000	1000000000
```

Python gets `query_agg()` returning `AggValue` with a `real` property that applies the scale; C++ gets
`query_agg()` returning `AggEntry`; each parser refuses the other shape by naming the right method
instead of misparsing columns. The CLI renders an aggregate table with the value in natural units.

**Why 547 tests missed it.** Coverage came in two halves that never met: 14 tests for
`AggregationEngine`'s maths on hand-built `SoASide` inputs, and one parser test that calls `parse()`
rather than `execute()`. No test crossed from a query string to the bytes on the wire. The earlier
claim here that "the engine computes them correctly — `test_query_engine.cpp` proves it" was
overstated: what was proven was the arithmetic and the grammar, not the execution path.

Verified: 27 new tests (execution, arguments, formatter, C++ client, plus 11 integration tests over
the raw protocol and the Python client), all values hand-computed in the tests; 578 C++ tests and 55
integration tests green; four mutations confirmed red separately.

Performance: `BM_VwapLatency` measured 2645 → 1616 ns, and that is **not** a speedup this change
produced — do not quote it. The accumulation loop is byte-identical in both binaries (same 15
instructions, same registers, no SIMD in either). `AggResult` crossing 16 bytes moved the return to a
hidden pointer, which lengthened the prologue by two pushes and moved the loop from `0x41530`
(mod 32 = 16, spanning two 32-byte fetch boundaries) to `0x41600` (mod 64 = 0, spanning one). On this
CPU, with the loop stream detector disabled, that front-end difference is worth the ~2.5 cycles per
level observed. Control benchmarks the change cannot touch confirm the two build environments are
equivalent: ingestion −1.3%, `BM_UpdateLatency` +0.1%. Treat as no regression.

Spec: `kiro-workspace/specs/aggregations-over-wire/`

### 28. Restore the integration test suite ✅
- The framework survived (`tests/integration/conftest.py`, 691 lines), the tests did not
- **Restored: 108 passing tests across 12 modules** — smoke, replication, compression, edge cases,
  metrics, aggregations, pool, C++ client, stress, large_response, multi-master (convergence and
  failover), and the opt-in live Binance pair. Each one was written to assert values rather than
  absence of errors, and between them they found six defects
  that 578 unit tests did not: the columnar format losing `side`, the flush race, aggregations
  returning zeros, and the write path in #59
- **Failover category is back** (9 tests): graceful handover refusals, kill → promotion with the time
  published as `failover_time_sec`, acknowledged data surviving a kill, the promoted node accepting
  writes, and a pool client re-discovering the primary. One test is `xfail(strict=True)` because the
  behaviour it asserts is genuinely broken — see #60
- **Complete.** 108 passing, 2 skipped, 2 xfailed across 12 modules; 113 across 13 since #62 added the
  crash-recovery module, the only one that kills a server. The multi-master modules
  (`test_mm_convergence.py`, 9 tests; `test_mm_failover.py`, 6) run on their own three-node mesh, and
  the live Binance modules (`test_binance_live.py`, 5; `test_binance_failover_sync.py`, 2) are opt-in
  behind `OB_BINANCE_TESTS=1` and hard-skip with a named reason when the exchange is unreachable or
  `websockets` is missing — a third-party outage must never fail this suite. Both were verified against
  the live feed, twice
- The two xfails are `strict=True` and point at real defects the suite found: #60 and #61
- Two fixtures exist to keep a shared session cluster usable: `heavy_cluster` (module-scoped) for load
  modules that would otherwise leave the replica replaying half a million rows into the next module's
  timeouts, and `healthy_cluster`, which restarts nodes and verifies a single primary after any test
  that kills one
- Until the rest is done, `scripts/run_regression.sh --full` cannot pass
- Effort: M | Impact: Correctness confidence, credibility of a fresh clone

### 29. Graceful failover honours its target ✅
- Status: **DONE** — `FAILOVER <target_node_id>` used to ignore the target entirely, and the
  outgoing primary raced the intended successor and won roughly half the time.

**Caveat found later** (item #60): both mechanisms are verified by C++ tests against live etcd, and
those tests publish node positions by hand. The server never publishes them, so `FAILOVER <target>`
rejects every target with `ERR unknown_target` on a real cluster. The mechanisms below are correct; the
command that triggers them cannot currently get past target validation.

Fixed with two mechanisms that cover different cases:
- **Handover intent** (`<prefix>handover` in etcd, written without a lease so it survives the
  revocation that follows): while it is live, only the named target campaigns for the leader key and
  the other replicas stand aside. After its deadline the cluster returns to an ordinary election, so
  an unreachable target cannot deadlock it
- **Election cooldown** on the outgoing primary, so it cannot reclaim the role it just released once
  the intent expires

`initiate_graceful_failover()` now returns a result enum instead of a bool, and the wire protocol
distinguishes `unknown_target` and `invalid_target` from a generic failure. The target is validated
against the coordinator before anything is revoked, so a typo in a node id leaves the cluster
untouched instead of dropping it into an election.

Six integration tests against a real etcd, including a ten-iteration handover loop and a three-node
case. Mutation-verified: disabling the deferral turns the three-node test red, disabling the cooldown
turns the fallback test red. Worth recording that the two-node test catches *neither* on its own,
because the two mechanisms overlap there — which is why the three-node test exists. Spec:
`kiro-workspace/specs/graceful-failover-fix/`

### 30. Authentication and TLS on the wire protocol
- Token or mTLS authentication for client sessions, replication links and multi-master peers
- TLS termination in-process (OpenSSL) or a documented sidecar pattern, with a benchmark of the cost
- Per-connection identity in logs and metrics
- Documented in `SECURITY.md`, which currently states the absence of both as a deployment constraint
- Effort: L | Impact: **Unblocks production adoption**

### 31. Access control
- Read-only users, per-symbol and per-exchange ACLs, admin-only commands (`FAILOVER`, `MIGRATE`)
- Effort: M | Impact: Multi-tenant deployments, compliance conversations

### 32. Configuration file support
- YAML or TOML config, with CLI flags overriding file values. Twenty-plus flags is past the point
  where flags alone are reasonable for ops
- Config validation with clear error messages, `--print-config` for support
- Effort: S | Impact: Ops ergonomics, fewer misconfigurations

### 33. Native packaging and cluster bootstrap
- Distribution packages: `.deb` and `.rpm` built on tag, plus a static tarball for everything else.
  Binary, headers, default config, systemd unit, man page
- `systemd` units for `ob_tcp_server` with `LimitMEMLOCK`, `CPUAffinity`, `Restart=on-failure`, and
  ordering against a local etcd unit
- `scripts/bootstrap-cluster.sh`: brings up a three-node multi-master cluster on one host or across
  hosts over SSH, native processes only, one command
- Install docs that cover the tuning the engine actually cares about: CPU pinning, isolated cores,
  `vm.swappiness`, huge pages, NIC queue affinity, fsync policy per storage device
- Effort: M | Impact: Time-to-first-run drops from an hour to minutes, without a container layer
  between the engine and the hardware

### 34. Backup, restore, point-in-time recovery
- `ob_backup` / `ob_restore` tooling on top of existing snapshots plus WAL
- Documented recovery procedure with RPO/RTO numbers
- Effort: M | Impact: Nobody runs a database they cannot restore

### 35. Grafana dashboard and alert rules
- Shipped dashboard JSON and Prometheus alert rules (replica lag, failover events, backpressure,
  conflict rate, flush latency)
- Effort: S | Impact: High value relative to cost; makes the metrics already being exported usable
- **Prerequisite cleared.** Five of the gauges a dashboard would plot were dead: registered as
  `ob_segment_count`, `ob_pending_rows`, `ob_symbol_count`, `ob_wal_file_index` and `ob_current_epoch`,
  but written by the engine without the `ob_` prefix. `MetricsRegistry::set_gauge()` looks the name up
  and returns quietly when it misses, so every one of those writes updated nothing and `/metrics`
  served a flat zero while the engine worked correctly. `ob_mm_backpressure_snapshot_total` was
  incremented but never registered at all. Names fixed, the missing counter registered, and an
  unregistered write now logs `OB_LOG_ERROR` once per name and increments a counter that three unit
  tests assert is zero. Found by the first integration test that checked a metric's *value* rather
  than its presence

## Phase 8 — Verifiability

**Why.** Performance claims are worth nothing if a reader cannot reproduce them, and quality claims
are worth nothing without evidence in CI. Every item in this phase produces something a stranger can
run and check themselves.

### 36. The argument parser mutated its loop counter, and hid three defects behind it ✅

`parse_args()` consumed every flag's value with `argv[++i]` inside `for (int i = 1; i < argc; ++i)` —
29 instances of `cpp/loop-variable-changed`, and a CodeQL review thread on every PR that touched the
file. Three PRs in a row paid that toll before this was worth doing.

The static-analysis finding turned out to be the least of it. Measured on the built binary before the
rewrite:

```
ob_tcp_server --port abc     → terminate called after throwing std::invalid_argument
                                 what(): stoi
                               core dumped
ob_tcp_server --port         → server started, on the default port
ob_tcp_server --prot 5599    → server started, on the default port
```

A non-numeric value crashed with a C++ exception message rather than an error. A flag with no value
was **silently ignored**, because the guard read `arg == "--port" && i + 1 < argc` and a missing value
simply fell through. And there was no unknown-argument branch at all, so a typo in a flag name — and
the value after it — vanished, leaving an operator with a server on a port they did not ask for.

Now: an `ArgCursor` owns the index, so consuming a value is not a mutation of a loop variable; values
are parsed with `std::from_chars` and range-checked against the destination type; and every one of the
three cases above is an error naming the flag:

```
Error: --port expects a non-negative integer, got 'abc'
Error: --port requires a value
Error: unknown argument '--prot'
Error: --port expects a value in range, got '99999'
```

The range check matters on its own: `--port 99999` used to be `static_cast<uint16_t>` of 99999, which
is 34463. The server listened on a port nobody named.

**This is stricter than before**, deliberately: an invocation carrying an unknown flag used to start a
server and now refuses to. A correct invocation behaves exactly as it did.

`parse_cli_args()` also had no tests, which is how all of this survived. It has 15 now — six on parsing
(including the endpoint list dropping empty entries, and booleans not swallowing the next argument) and
nine death tests, one per refusal. Two of those pin down validations that already existed and had never
been exercised: multi-master without `--mm-node-id`, and multi-master without `--coordinator-endpoints`.
The second was found by writing the happy-path test without endpoints and watching it take the whole
test binary down.

- Spec: none; the roadmap entry was the spec

### 37. CI hardening — sanitizers ✅ (coverage and the compiler matrix are still open)

**Sanitizers are in CI and both are clean**, and each found a real defect on the way in — which is the
whole argument for the job, so it is worth recording what they were.

**UBSan: undefined behaviour in every in-memory use of a timestamp.** `HLCTimestamp` was
`#pragma pack(1)` so that its size would match its 12-byte wire form. That put `physical_ns` on a
4-byte boundary whenever the struct was embedded in another — inside a `std::vector<ConflictEntry>`,
for instance — and binding a `const uint64_t&` to it, as `EXPECT_EQ` does, is undefined behaviour:

```
runtime error: reference binding to misaligned address 0x516000005adc for type
'const long unsigned int', which requires 8 byte alignment
```

Not theoretical for an engine written for specific hardware: an unaligned 8-byte access faults on some
targets and takes a slower path on others. The packing was never needed — `serialize()` and
`deserialize()` copy field by field at fixed offsets, so the wire layout never depended on the struct
layout. The struct is naturally aligned now, `sizeof` is 16, the wire form is still 12, and three
`offsetof` assertions state what the wire form actually requires.

**TSan: shutdown closed descriptors the io thread was still using.** `MultiMasterManager::stop()`
closed `listen_fd_` and `epoll_fd_` under a comment saying it did so "to unblock threads", and only
then joined them. Closing an epoll descriptor does **not** wake a thread inside `epoll_wait()` on
Linux, so shutdown waited out the 500 ms timeout anyway — while the loop could call `epoll_wait()` on
a descriptor number the kernel had already handed to something else:

```
WARNING: ThreadSanitizer: data race
  Write of size 8 by main thread:   close ... MultiMasterManager::stop() multi_master.cpp:314
  Previous read of size 8 by T1:    epoll_wait ... MultiMasterManager::io_loop() multi_master.cpp:536
  Location is file descriptor 4 created by main thread at epoll_create1
```

Now an `eventfd` registered in the epoll set is written by `stop()`, the threads are joined, and only
then is anything closed. One fix, and all twelve failing tests went green — they were the same race
seen from twelve places. Shutdown also stopped waiting: the multi-master stats module went from
carrying a half-second teardown per node to finishing in 1.7 s overall.

Both jobs run the C++ unit suite, not the integration suite: the value is in the concurrency and
memory paths, and instrumenting a live etcd cluster would multiply the runtime without reaching
anything new. `detect_leaks=1` is on, because a leak in a long-lived server process is a defect.
ThreadSanitizer needs `vm.mmap_rnd_bits=28` on Ubuntu 24.04 or it refuses to start at all —
documented in the workflow next to the sysctl, since the failure mode ("unexpected memory mapping")
does not name its cause.

Still open under this item: the coverage report with a badge (`OB_ENABLE_COVERAGE` already exists),
and the GCC/Clang × Debug/Release matrix.

- Effort: S | Impact: 697 tests clean under ASan+UBSan and under TSan, checked on every push. Two
  defects found by turning them on, one of them undefined behaviour on the hot path's data type

### 38. Fuzzing
- libFuzzer harnesses for `command_parser`, the multi-master frame parser, and WAL record
  deserialization. These are the three places that read untrusted bytes
- Corpus in-repo, short fuzz run in CI, optional OSS-Fuzz submission
- Effort: M | Impact: Finds the class of bug that property tests miss; also a credibility signal

### 39. Reproducible comparative benchmarks
- `benchmarks/README.md` already holds equivalent workload definitions for ClickHouse, TimescaleDB
  and kdb+. Turn them into a **runnable harness**: native installation of each system from its
  official packages, one script, results table with hardware, versions and dataset recorded
- Every system compared runs natively. Benchmarking a native engine against containerised
  competitors would measure the container layer, not the engines
- Publish results in the README with the exact hardware, kernel, and dataset used
- Effort: L | Impact: Turns a performance claim into something a reader can verify on their own
  hardware in an afternoon

### 40. Documentation site
- MkDocs or Doxygen on GitHub Pages: architecture, wire protocol reference, operations guide,
  five-minute tutorial that ends with a real query
- Effort: M | Impact: Reduces evaluation friction; the docs currently require reading the repo

### 41. Client libraries: Rust and Go
- Thin bindings over the existing C API, published to crates.io and as a Go module
- Effort: M | Impact: Widens the audience beyond C++ and Python shops

### 42. Release engineering
- Semantic versioning, tagged releases with changelog, prebuilt PyPI wheels (`pyproject.toml` with
  scikit-build-core is already in place), signed tags
- Effort: S | Impact: `pip install orderbook-dbengine` is the shortest path to a first user

### 43. Worked example: live market data ingestion
- A runnable Binance (or Coinbase) websocket ingestor writing into the engine, with a Grafana
  dashboard showing it live. `scripts/binance_*.py` is the seed for this
- Effort: S | Impact: Turns an abstract engine into a visible demo

## Phase 9 — Query and Analytics Depth

**Why.** Ingestion is solved. What a trading firm actually asks next is analytical: bars, windows,
and getting data into their existing Python stack without a copy.

### 44. Time-bucketed aggregation in the query language
- `GROUP BY time_bucket(interval)`, OHLCV bar generation, time-weighted mid price, rolling windows
- Effort: L | Impact: This is what people build on top of orderbook data anyway

### 45. Streaming subscriptions
- `SUBSCRIBE 'SYM'.'EXCH'` pushing updates to the client; the README already advertises streaming
  subscriptions, so either implement or correct the claim
- Backpressure policy per subscriber, slow-consumer disconnect
- Effort: M | Impact: Real-time consumers stop polling

### 46. Apache Arrow output
- Arrow IPC / Flight result format, zero-copy into pandas, polars and DuckDB
- Effort: M | Impact: Drops the integration cost for analytics teams to near zero

### 47. Zone maps and columnar indexes
- Per-segment min/max and count for timestamp and price, so range scans skip segments
- Effort: M | Impact: Query latency on large ranges

### 48. Cost-based scan planning
- Decide live-buffer versus columnar scan versus both from segment statistics rather than a fixed rule
- Effort: M | Impact: Predictable query latency as data grows

## Phase 10 — Performance Frontier

**Why.** This is where the "custom engine for specific hardware" claim gets proven in our own
codebase. Each item is also a story we can sell as bespoke work.

### 49. SIMD codec
- AVX2/AVX-512 for delta, zigzag and Simple8b encode/decode. SIMD is currently only in aggregation
- Effort: M | Impact: Flush and scan throughput

### 50. NUMA awareness and thread pinning
- Per-socket allocation, pinned io threads, `--cpu-affinity` configuration
- Effort: M | Impact: Tail latency on multi-socket servers, which is where clients run

### 51. Huge pages
- `MADV_HUGEPAGE` / explicit hugetlb for mmap segments and SoA buffers
- Effort: S | Impact: TLB pressure at large working sets

### 52. Shared-memory transport for local clients
- Zero-copy ring buffer for co-located processes, bypassing TCP entirely
- Effort: L | Impact: Sub-microsecond local writes; a genuine HFT differentiator

### 53. Kernel-bypass experiment
- AF_XDP or DPDK prototype measured against the io_uring path, published as an engineering write-up
  even if we do not ship it
- Effort: L | Impact: Credibility on the low-latency claim; strong content

## Phase 11 — Reliability Engineering

### 54. Chaos and fault injection
- Network partitions between multi-master peers, packet loss and reorder, disk-full, fsync failure,
  clock skew (HLC correctness under skew is untested), etcd unavailability
- Effort: L | Impact: The failure modes that lose data in production

### 55. Multi-node cluster tests in CI
- Three native nodes plus etcd started by a script, multi-master convergence and failover verified
  on every PR
- Effort: M | Impact: Prevents regressions that unit tests structurally cannot catch

### 56. Rolling upgrade support
- Protocol version negotiation matrix, mixed-version cluster tests, documented upgrade path
- Effort: M | Impact: Required before anyone runs this longer than one release

### 57. Anti-entropy reconciles for real, and here is what that covers ✅

The class was a scheduler with two stubs: `detect_gaps()` returned an empty list unconditionally,
`repair_gap()` returned false, and `GapInfo` described a gap as a WAL file index and byte offset —
the model #61 measured as data loss. It was worse than that: the scheduler was never constructed
either, and asking a multi-master node for statistics dereferenced it (#68).

**Now** a pass is one thing: tell every connected peer what we hold. Receiving a vector already makes
a node stream what the peer lacks (#61), so reconciliation needs no protocol of its own — sending the
vector *is* the repair. `compare_vectors()` reports the difference in both directions in terms of
`(symbol, origin, sequence range)`, and the work is injected into `AntiEntropyManager` as a function
rather than reached for through a back-reference to `MultiMasterManager`, which owns it. That is what
makes a pass testable with no cluster, no etcd and no ports — and why this class had no tests before.

**A repair counts when the gap is gone, not when a vector was sent.** A pass remembers what it was
behind on and the next pass counts what disappeared. A metric that counted requests would measure
diligence; `ob_mm_anti_entropy_repairs_total` now measures closure, and
`ob_mm_reconcile_gaps_detected` alongside the run counter keeps "checked, nothing to repair" distinct
from "never ran" — the ambiguity that let this item look finished for months.

**What it actually covers, measured rather than claimed.** The obvious test — partition a node, write
elsewhere, restore the link — reconverges without any help from reconciliation, and a mutation that
disabled the pass entirely still passed it. An `iptables DROP` does not reset the connection, so TCP
retransmits the backlog once the rule is gone. In this architecture, most divergence is already
handled: a broken connection triggers reconnect, handshake and catch-up; a live connection delivers
in order. What is left for anti-entropy is divergence that persists while the connection is healthy:

- a record the receiver dropped rather than lost in transit — above the 4096-entry held set in
  `SequenceTracker`, or refused for another reason
- a peer whose vector was missing or stale when catch-up ran, so the filter had nothing to work from
- a backlog the sender discarded under backpressure, which nothing repairs today and which becomes
  the conclusive test for this item once #69 caps the live send buffer

**A regression this introduced, and the fix.** As first shipped, a vector arriving started a full scan
of the retained WAL — so a timer that sends a vector to every peer meant every node re-read its whole
log, per peer, per interval, on the io_loop thread that also carries live traffic. Measured in the
harness: `scanned=543 (9662010 bytes) sent=0`, over and over. At the 94 MB/s that scan runs at, a 1 GB
WAL would spend most of every interval reading itself to find nothing.

Receiving a vector now compares it first and scans only if the peer is actually missing a range. In a
four-cycle harness run that skipped 18 scans and left only scans that sent records. The comparison is
safe because every route to a peer missing data leaves evidence in it or forces a reconnect: a
disconnected peer returns through the handshake, a backlog dropped for not draining closes the
connection (#69), and a record the receiver refused leaves its own frontier behind. The code says so,
because a future change that can drop a record while both sides stay connected has to remove the
shortcut with it.

Verified: 6 unit tests on the pass (both directions, closure measured across passes, a persisting gap
not counted, the same symbol from two peers as two facts, and a run with no reconciler saying so), 7
on `compare_vectors` (including a key only the peer holds, and silence read as "holds nothing"), and
three mutations red — closure counted on dispatch, a silent peer read as holding everything, and a
missing reconciler reported as a clean pass. The fourth mutation, disabling the pass, is green against
the partition scenario for the reason above; that gap closes with #69.

- Spec: `kiro-workspace/specs/mm-version-vector-catchup/` (section 6a)

### 58. Distributed tracing
- OpenTelemetry spans across client, primary, replica and peers; trace a write end to end
- Effort: M | Impact: Debuggability in a real deployment

### 59. Responses larger than the socket buffer killed the session, and SIGPIPE killed the server ✅

Two defects on the same code path, both found by the first integration test that read a large result
set under load.

**A response above the socket send buffer was truncated.** Measured on a fresh server, one symbol,
reading immediately: 20 000 rows fine, 50 000 fine, 100 000 (~3.9 MB) closed the connection mid-stream.
Client sockets are non-blocking, and `Session::send_response()` looped on `::write()` treating anything
other than `EINTR` as failure — so a full kernel send buffer returned `EAGAIN`, the loop gave up, and
`tcp_server.cpp` read that as "client gone" and removed the session. There was **no log line at all**:
the last entry after such a disconnect was the server's startup message.

Ironically the correct pattern was already in the repository, in
`MultiMasterManager::flush_send_buffer()` — queue, arm `EPOLLOUT` on `EAGAIN`, disarm when drained —
and documented as pitfall 5. The TCP server did not use it.

**SIGPIPE killed the whole process.** `grep -rn SIGPIPE src/ tools/ include/` returned nothing, and
`Session` wrote with `::write()`, so a client disconnecting mid-response raised SIGPIPE whose default
action terminated the server — every other client's session with it. The unit test for this exited with
code 141 (128 + 13) before the fix. Every other socket writer in the repo already used
`::send(..., MSG_NOSIGNAL)`: metrics server, replication, multi-master, the C++ client. Only this one
did not, and buffering responses across event-loop turns widens the window, so both had to be fixed
together.

Now: a per-session send buffer with `EPOLLOUT` arming, `MSG_NOSIGNAL` on every write plus
`signal(SIGPIPE, SIG_IGN)` as a net, a 64 MB cap per session so a client that never reads cannot grow
the server without bound, one `close_session(fd, reason)` that logs instead of five silent copies, and
`ob_session_pending_bytes` in `/metrics` so an operator sees a slow client before it disappears.

**Why 578 tests missed it.** The largest response any test ever asked for was 1000 rows, about 40 KB —
three orders of magnitude below the threshold. `test_tcp_server.cpp` exercises
`format_query_response()` as a function, with no socket involved, so kernel buffers do not exist there.
This was not a forgotten case: no test had ever sent a response bigger than a socket buffer, and that
is the only condition under which either defect appears.

Verified: 8 integration tests (including a deliberately slow reader that fills the buffer every run,
and a client vanishing mid-response with `SO_LINGER 0`) plus 6 unit tests on `Session`; four mutations
confirmed red separately — EAGAIN as failure, no `EPOLLOUT` arming, no cap, no `MSG_NOSIGNAL`.
Spec: `kiro-workspace/specs/large-response-write-path/`

### 60. Graceful failover could not work outside its own tests, and two more links were missing ✅

`FAILOVER <target_node_id>` validated the target against
`CoordinatorClient::get_published_positions()`, and nothing in production ever published a position:
`publish_wal_position()` had one caller in `src/`, a connectivity check writing `(0, 0)`, and the rest
were in `tests/test_etcd_integration.cpp`, which publishes positions by hand before exercising the
feature. So every graceful handover on a real cluster answered `ERR unknown_target`.

`FailoverManager` already had everything it needed — `RoleTransitionHandler::get_wal_position()` exists
and `Engine` implements it — so the fix is a publish in the monitor loop, at most once a second, for
both roles. `FAILOVER <target>` works from the first run:

```
{"component":"failover","msg":"Graceful failover: handing role to node-1 (grace=5s cooldown=15s)"}
```

**And then the integration module fell over**, which is where this item earns its length. Two more
links in the same chain had never run, because the first link never worked:

- **The outgoing primary kept saying it was primary.** The handover moved `FailoverManager`'s own role
  and called `handler_.demote_to_replica()` **only if** the coordinator already showed a new leader at
  that instant — which it never does, because the target first has to notice the empty leader key. The
  comment said "monitor_loop() will pick it up on its next pass"; the replica branch of that loop only
  recorded the address. So the node that had just given the role away answered `ROLE` with `PRIMARY`
  and kept accepting writes. Measured: `{'node-0': 'PRIMARY 1', 'node-1': 'PRIMARY 2'}` for the full
  30-second convergence window, while the node's own `FailoverManager` said `role=2` (REPLICA).
- **A replica never followed a new primary.** The same branch recorded `primary_address_` and told the
  engine nothing, so a `ReplicationClient` stayed pointed at whoever was leader when it started. After
  a handover the demoted node followed nobody; after a promotion elsewhere, a surviving replica kept
  talking to the node that had lost the role. Uncovered by the tests because a two-node cluster has no
  third node to observe it.

Both are now in the monitor loop: the handover demotes the engine immediately (the lease is gone, so
the role is gone whatever the target does yet — an empty address is safe, `demote_to_replica()` only
starts a client when it can parse `host:port`), and the replica branch adopts a leader address when it
changes, once per change rather than once per poll.

`test_handover_lands_on_the_named_target` lost its `xfail(strict=True)` and gained the two assertions
this uncovered: the outgoing node must report `REPLICA`, and it must refuse writes.

Worth recording separately: `elect_winner()`, the "most advanced replica wins" policy, has **no callers
in `src/`** — election is a create-only CAS race on the leader key, so the winner is whoever gets there
first. Published positions now exist for it to compare, but nothing compares them. That is #70.

- Effort: S | Impact: Graceful handover works, an outgoing primary stops advertising a role it gave
  away, and a replica follows the current leader instead of a remembered one

### 77. Two metrics were written by name and never registered ✅

Found by reading a test's own log output instead of only its verdict. Every run of the dedup tests
printed:

```
ERROR metrics Write to unregistered counter 'ob_mm_duplicates_dropped': the value is discarded and
/metrics will report a flat zero.
```

The registry already said exactly what was wrong, on every run, for as long as receive-side dedup had
existed. A sweep for the class found a second one, `ob_sequence_gaps_detected` — so **both** numbers
that say whether multi-master deduplication is working were invisible on the dashboard, and both were
added together with the mechanisms they measure, which is precisely when nobody is watching a graph
yet.

Both registered, and `scripts/check_metrics.py` now runs in the `docs-integrity` job: it extracts every
string literal handed to `increment_counter()`, `add_to_counter()`, `set_gauge()` or
`observe_histogram()` in `src/` and `tools/`, and fails if one is missing from the registrations in
`src/metrics.cpp`. Verified by deleting a registration and watching it exit 1 with the file and metric
named.

What the script cannot prove, and no script can: that a registered metric is ever written, or that a
name is spelled the way a dashboard expects.

- Effort: S | Impact: The two counters that describe deduplication report real numbers, and a check in
  CI stops the class from coming back

### 76. Multi-master bootstrap is a stub, and its flag has no way out

Found while looking for the snapshot mechanism #67 says to build on. It is not there.

`MultiMasterManager::bootstrap_from_peer()` logs one progress line with every number set to zero and
returns:

```cpp
OB_LOG_INFO("mm", "Bootstrap progress: phase=%s bytes=%zu/%zu (%.1f%%) elapsed=%.1fs",
            "snapshot", size_t(0), size_t(0), 0.0, 0.0);
// Full implementation in task 12 — snapshot transfer + WAL catch-up.
```

It has **no callers**. `start_bootstrap()` has none either outside `tests/test_multi_master.cpp`. And
`bootstrapping_` is set to `true` in one place and **never set back to false anywhere in the tree**.

So a multi-master node never bootstraps: it starts empty and relies on catch-up from a peer's retained
WAL. That is survivable and is roughly what #67 describes. The trap is the flag: `INSERT`, `MINSERT`
and `DELETE` all answer `ERR BOOTSTRAPPING` while `is_bootstrapping()` is true, so **the day someone
wires `start_bootstrap()` into a real path, that node stops accepting writes for the rest of its
life** — the same shape as #73, waiting in a state machine with an entrance and no exit.

Two things to decide, and they are separate: whether to implement snapshot bootstrap (it is what #67
needs, and what `AntiEntropyManager::trigger_snapshot_repair()` is a stub for), and what to do
meanwhile about a progress log that reports progress it never makes. A feature that looks implemented
is worse than an absent one, especially in a repository read as a portfolio.

**The dead end is closed; the transfer is still missing.** `finish_bootstrap(bool succeeded)` now
pairs with `start_bootstrap()`, and a failure leaves the state rather than sitting in it: a node that
cannot bootstrap says so loudly and becomes usable, because refusing writes for ever is the worse
answer. `bootstrap_from_peer()` logs that it is not implemented instead of a progress line of zeros,
and clears the flag. Four unit tests cover entering, leaving, leaving after a failure, and leaving
without entering.

What remains is the snapshot transfer itself, and it needs a decision rather than an implementation,
because the obvious shortcut is not available: **`ReplicationManager` is deliberately not created in
multi-master mode** (`src/engine.cpp`: "NOTE: ReplicationManager is NOT created in MM mode"), so an MM
node serves no `SNAPSHOT_REQUEST` and a joining node has nobody to ask.

Two ways, and they differ in architecture rather than in effort:

1. **Extend the multi-master protocol** with snapshot request/file/chunk messages, mirroring what
   `replication.cpp` already does for primary→replica: sender-side streaming state per peer,
   receiver-side staging, CRC check and rename-into-place. Self-contained, no new listener, and it
   duplicates a protocol that already exists once.
2. **Run the replication server on multi-master nodes too**, and let a joining node act as a replica
   for the length of the bootstrap before switching to peering. Far less new code, and it reuses a
   tested path — at the cost of every MM node speaking two protocols on two ports, with overlapping
   responsibilities.

Either way the manifest should carry the sender's version vector, so the receiver may legitimately
declare frontiers from it — which is the remaining half of #67.

- Effort: S (done: the dead end) + L (the transfer) | Impact: The first caller of `start_bootstrap()`
  no longer bricks its node. Adding a node to a running cluster still misses data older than its
  peers' WAL retention

### 75. A restart forgot every out-of-order record it was holding ✅

The second consequence listed under #67, split out because the fix is a different mechanism and does
not need snapshot bootstrap.

`SequenceTracker` keeps, per (symbol, origin), a frontier — "everything up to here" — and a set of
numbers seen above it that cannot be merged yet because something below is missing. Only frontiers
were persisted. So a node holding 5 with 1-4 missing wrote down nothing, and after a restart it had
never seen 5. Catch-up over-delivers **on purpose** (#61 made that safe by dropping duplicates on
arrival), so the next redelivery was applied a second time into append-only storage: one duplicate row
per held record, on every restart.

Held numbers are now persisted as **inclusive ranges** in their own WAL record (`WAL_RECORD_HELD_SEQUENCES`,
type 8), written next to the version vector. Ranges because that is the shape the data has — catch-up
delivers runs, so four thousand held numbers above one gap is a single sixteen-byte range. A separate
record type rather than an extra section in the version vector, because the vector is also what peers
read, and catch-up forwards `WAL_RECORD_DELTA` and nothing else: **this changes no protocol**.

`SequenceTracker::fingerprint()` had to learn about the held set too. It summed frontiers only, and the
whole point of this state is that it changes while the frontier stands still — without that, a node
receiving nothing but out-of-order records would have written down none of them.

The regression test needed two attempts, and the second attempt is the interesting one. The first
version passed with the persistence **disabled**, because the re-flushed segment landed on the same
directory name and `ColumnarStore` refused the duplicate merge — #62's backstop, masking the very
thing under test. The file already warned about exactly this in the test above it, and I walked into it
anyway. With a later-timestamped record added so the segment path differs, the test reads 2 rows with
the fix and **3 without it**.

Truncation is bounded and honest: the WAL payload length is 16-bit, so at most 3000 ranges are written
per persist, and exceeding that logs a warning. A held set written in part still prevents every
duplicate it covers.

What this does **not** fix, and stays with #67: a late joiner still exports no vector entry for an
origin whose stream it joined mid-way, so peers keep sending it records it already has. That needs a
legitimately established base, which needs snapshot bootstrap — see #76.

- Effort: M | Impact: A restart no longer turns catch-up's deliberate over-delivery into duplicate rows

### 74. A keepalive for a forgotten lease answers 200, so the lease fenced nothing (P0) ✅

Found while working out whether a position key could be trusted for #72, by asking etcd what it
actually answers instead of assuming.

`refresh_lease()` ended with `return !resp.empty();`. Measured against etcd 3.5.17:

```
live lease, keepalive:
  {"result":{"header":{...},"ID":"563970515281197573","TTL":"30"}}
revoked lease, keepalive:
  {"result":{"header":{...},"ID":"563970515281197573"}}          ← same shape, HTTP 200, no TTL
```

So the function **could not fail**, and the lease fenced nothing. The leader key is written under the
lease precisely so that losing the lease loses the role, which made this the failure the whole
mechanism exists to prevent:

1. A primary loses contact with etcd. After the TTL the lease expires and **the leader key is
   deleted**.
2. A replica sees no leader, campaigns, wins the CAS under its own lease, and is primary.
3. Contact returns. The old primary's keepalive answers 200 with no TTL, it concludes all is well,
   and keeps answering `PRIMARY` — and `INSERT` refuses writes only for `NodeRole::REPLICA`, so it
   **keeps accepting them**.

Reproduced without building a partition, by revoking every lease in etcd — which is what a partition
longer than the TTL does to the leader key, minus the networking:

```
before the fix:  ['PRIMARY 1', 'PRIMARY 2']  for the full 24 s window, old primary logged nothing
after the fix:   ['PRIMARY 2', 'REPLICA …']  — "keepalive returned no TTL", demoted, re-elected
```

Three changes, in increasing order of how much they generalise:

1. `refresh_lease()` reads `TTL` from the keepalive response and fails when it is absent or `<= 0`.
2. `Impl::http_post()` reads `CURLINFO_RESPONSE_CODE` and returns nothing for `>= 400`, logging the
   code, URL and body. This fixes a class rather than a case: a `put` under an unknown lease answers
   **404** with `{"error":"etcdserver: requested lease not found"}`, and every caller that tested
   `!resp.empty()` read that refusal as a success.
3. The `PRIMARY` branch of `monitor_loop()` now reads cluster state each pass, reconciles the epoch,
   and steps down the moment the leader key names someone else — an independent guard, because a live
   lease is not proof that the key still belongs to us. It demotes on the first sighting: a spurious
   demotion costs seconds of unavailability, two primaries cost divergent data.

The regression test (`test_a_primary_whose_lease_etcd_forgot_stops_holding_the_role`) samples the
roles twice a second and fails on the first moment two nodes claim the role. It fails against the
pre-fix binary with `2 nodes held the PRIMARY role at once`.

- Effort: S | Impact: Closes a split-brain path in the component whose only job is to prevent split
  brain, and stops three other call sites from reading an HTTP 404 as a success

### 73. A node that loses the startup race is inert for the rest of its life (P0) ✅

Found while proving #70 on a real cluster: the deference log lines never appeared, and the reason was
not the new code. Two nodes started **simultaneously**, the way any `systemd`, Ansible or start-all
script starts them. One won the leader key. The other sat at `STANDALONE` for 54 seconds and did not
log a single election attempt. Then `kill -9` on the primary, and **no promotion in 40 seconds** — the
cluster simply had no primary any more.

The state machine has a dead state. `FailoverManager::start()` assigns `REPLICA` when it reads a leader
from etcd and calls `attempt_promotion()` when it does not — but `attempt_promotion()` returns on a lost
CAS **without touching the role**, so the loser stays `STANDALONE`. And `monitor_loop()` branches on
`PRIMARY` and on `REPLICA`, so `STANDALONE` matches neither: no lease refresh, no leader poll, no
campaign, no replication client. Forever.

Why no test caught it: the integration fixture starts nodes **one at a time and waits**, so the loser
always reads an existing leader and takes the `REPLICA` path. Simultaneous start — the realistic case —
was never exercised. The consequences compound: an inert node also never replicates, so it holds no data
to promote *with*, and the operator's instinctive fix (restart it) is the only thing that works.

Fix: `monitor_loop()` must have no dead state — a `STANDALONE` node with a reachable coordinator and
failover enabled either campaigns (no leader) or becomes a replica (leader present) — and a lost CAS
must re-read the state and demote to replica of the winner. Regression test: start the cluster
simultaneously, then kill the primary.

The state machine now has no dead ends. `adopt_leader_if_present()` follows whoever holds the leader
key, and it is called from the places that used to leave a node without a role: the lost CAS in
`attempt_promotion()` and a new `STANDALONE` branch in `monitor_loop()`. A startup that cannot reach a
configured coordinator starts the monitor thread and retries instead of returning, logging once per
thirty attempts because an unreachable coordinator is a condition, not an event. Single-node mode is
untouched: with no `--coordinator-endpoints` there is no thread and no retry log, since that is a
deployment choice rather than an outage.

Measured on both scenarios, before and after:

```
A: two nodes started simultaneously
   before: ['PRIMARY 1', 'STANDALONE']  → kill -9 primary → no promotion in 40s
   after:  ['REPLICA …', 'PRIMARY 1']   → kill -9 primary → promoted in 6.8s

B: a node booted while nothing listened on the coordinator port
   before: STANDALONE, and still STANDALONE after etcd came up — for ever
   after:  STANDALONE, then joined 0.3s after etcd answered
```

Three regression tests in `tests/integration/test_failover_dead_state.py`, which brings its own etcd
because the whole point is the *order* of startup. All three were run against the pre-fix binary and
all three failed, which is the only reason to believe they test anything:

```
AssertionError: a node holds no cluster role and will never campaign again: ['STANDALONE', 'PRIMARY 1']
AssertionError: node-0 never promoted within 45.0s; roles now ['STANDALONE', '<unreachable: ConnectionRefusedError>']
AssertionError: the node never joined after etcd came up; role is STANDALONE
```

Two things worth keeping from how this was found. The retry loop exposed a leak the one-shot code hid:
`connect()` called `curl_easy_init()` unconditionally and overwrote `impl_->curl_handle` without
freeing it — invisible when you connect once, a leak per second when you retry. And the fixture had
already documented the bug as a reason to avoid it: "This avoids a race condition where both nodes
start simultaneously and one fails to transition from STANDALONE." The defect was known and worked
around in the harness rather than fixed in the engine. **A workaround in the test harness is a bug
report nobody filed.**

- Effort: S | Impact: Automatic failover now works on a cluster whose nodes were started together,
  which is every real deployment, and on a node that boots while etcd is restarting. This was the
  failure mode HA exists to prevent

### 72. Deference cannot tell a further replica from a dead one ✅

`decide_election()` (#70) defers to whoever published the furthest position, and cannot ask whether that
node is still alive, because `PublishedPosition` carries neither a timestamp nor a lease. The bounded
window keeps that safe but blunt: in a two-node cluster the survivor always waits the full window for the
node that just died, which is where the +3 s of failover time measured in #70 goes.

Two ways to make it precise, and the choice matters:

1. **Timestamp in `PublishedPosition`** — cheap, but compares wall clocks across machines, so it trades a
   liveness question for an NTP assumption.
2. **Write the position keys under a per-node etcd lease** — a dead node's position disappears on its own,
   which is exactly what a lease is for, and needs no clock agreement. Costs each node its own lease plus
   a refresh in the monitor loop.

Recommendation: (2). Then deference applies only to peers that are both further ahead **and** currently
alive, the window shrinks to a backstop for a live-but-wedged node, and the common two-node failover pays
nothing.

Option (2) it is. Position keys are written under a **per-node lease**, so a node that stops
refreshing stops being visible to an election — the liveness question is answered by the mechanism
that exists for liveness, and no clock is compared to any other clock.

The lease TTL is the same as the leader lease's, and that is a choice rather than convenience: when a
process dies, **both** leases stop being refreshed at the same instant, so the position key expires at
about the moment the leader key does. The survivor starts its election and the corpse is already gone
from the list. The safety asymmetry says not to go shorter — a position key that lives slightly too
long costs a little failover time, while one that vanishes too early (a live node that missed a
refresh under load) costs **data**, because we would stop deferring to a replica that really does hold
more log. Refresh rides along with the position publish, once a second.

`decide_election()` is unchanged. It gets better input, not new logic, which is the best available
outcome for a change like this. `--election-deference-ms` stops being the main protection and becomes
the residual one: it now catches a node that is alive, refreshing its lease, and still not promoting —
stuck in I/O, looping, stopped in a debugger. The default stays at 3000 and is expected not to fire.

`stop()` revokes the position lease, so a node stopped on purpose leaves the list at once rather than
after the TTL.

Measured on a two-node cluster, `--coordinator-lease-ttl 5`, `kill -9` on the primary:

```
                 dead node's position    "Deferring election"   promotion
before (#70):    never expires           1                      8.5 s
after  (#72):    gone at +4.9 s          0                      5.6 s
```

So the three seconds #70 charged for a corpse are gone, and the win #70 was built for — preferring the
replica that lost the least — now applies only to replicas that are actually there.

This item had a prerequisite that was not visible when it was filed: **#74**. Publishing under a lease
is only sound if the code can tell that a lease has died, and `refresh_lease()` could not fail at all.
The re-grant path in `ensure_position_lease()` would have been unreachable, and a node whose lease
etcd had forgotten would have gone on publishing keys that were deleted on arrival — invisible to
itself, and invisible to every election.

Three regression tests, all of which fail against the pre-change binary: the dead node's position
disappears; the survivor holds the role only once that position is gone (an invariant rather than a
stopwatch, so a loaded machine cannot make it lie); and a node stopped with `SIGTERM` leaves the list
within four seconds, well inside the TTL, so expiry cannot be what removed it.

- Effort: M | Impact: Removes the failover time #70 added in the common case, and makes "prefer the most
  advanced replica" mean the most advanced *live* replica

### 71. The coordinator client shared one libcurl handle across threads ✅

Found by chasing a flaky test after #60, rather than by re-running it until it passed.

`CoordinatorClient::Impl` held a single `CURL* curl_handle` and there was no mutex anywhere in
`src/coordinator.cpp`. A libcurl easy handle must not be used from two threads at once, and this one
was: the failover monitor thread refreshes the lease and polls cluster state, while a session thread
running `FAILOVER` sets a handover intent and revokes a lease — on the same handle.

The window was narrow until #60 started publishing WAL positions every second. Then graceful handover
began failing intermittently:

```
AssertionError: handover refused: 'ERR failover_failed'
```

`failover_failed` is `COORDINATOR_ERROR`, which in that path means `revoke_lease()` returned false —
a request corrupted mid-flight by the concurrent publish. One run in three, and the cascade after it
(a cluster with no primary) also broke a smoke test two modules later with "No node with PRIMARY role
found".

Every request on the shared handle is now serialised. The watch loop is unaffected: it already creates
its own handle. Verified by repetition, since the symptom was intermittent: three consecutive runs of
the failover module and two of the failover-plus-smoke combination that had been failing, all green.

Worth noting what the flakiness was *not*: it was not test ordering, though it looked like it. The
control — the same combination on `master` — passed, which pointed at the change rather than the
tests, and the actual message (`failover_failed`, not `not_primary`) pointed at the coordinator rather
than at the role. Re-running until green would have hidden a data race in the component that decides
which node is primary.

- Effort: S | Impact: Removes a data race from lease management and leader election, and with it the
  intermittent handover failure it caused

### 70. The election policy has no callers ✅

`elect_winner()` in `src/failover.cpp` picks the most advanced replica from the published positions —
higher WAL file index, then higher byte offset, then lower node id as a tiebreak. It has unit tests in
`tests/test_failover_election.cpp`. It has **no callers in `src/`**: `grep -rn elect_winner src/` finds
the definition and nothing else.

What actually happens on failover is `attempt_promotion()`, whose comment says it plainly: "Grant a new
lease and try to acquire leadership via CAS. If the leader key doesn't exist, CAS succeeds and we become
primary. If it exists (another node promoted first), CAS fails and we stay replica." The role goes to
whoever wins the race, not to the replica with the least missing data.

Since #60 the positions the policy needs are actually published, so wiring it in is now possible: a
candidate reads the positions, and defers if another live replica is further ahead. The care is in the
edge cases — the most advanced replica being down, positions being stale, and two candidates deferring
to each other — which is why this is its own item rather than a rider on #60.

Wired in as `decide_election()`, a pure function over the published positions, called from
`attempt_promotion()` after the cooldown check and **before** `grant_lease()` — deferring must not
consume a lease it is about to abandon.

The edge case that shaped the design is the one the entry above predicted: **positions carry no lease**.
A dead node's position stays in etcd, so naive "step aside if someone is further" hands the cluster a
livelock — the survivor waits for a node that will never come back. So deference is bounded: defer while
another node is further ahead, and after `--election-deference-ms` promote anyway, logging that the
position we deferred to may be stale. Two candidates reading the same list never both defer, because at
most one of them is not the best.

Proven on a real two-node cluster, not just in unit tests (`scripts/`-style probe, staggered start so the
loser is a genuine REPLICA):

```
role before:  ['PRIMARY 1', 'REPLICA 127.0.0.1:50435 0']
kill -9 primary
16:04:09 Deferring election to node-0 (file=0 offset=260), window=3000ms — it holds more of the log than we do
16:04:12 Deference window expired after 3028ms and node-0 never promoted — promoting anyway
node1 promoted after 9.4s
```

The honest cost: **failover went from ~6.5 s to 9.4 s** whenever the node that died was the furthest
ahead — which in a two-node cluster is the common case, and there the wait buys nothing, since there is
no second replica to prefer. That is why the flag exists and why `0` restores the old race. The win
appears with two or more replicas at different positions, where the promotion now goes to the one that
lost the least.

Mutation-tested rather than assumed: disabling the window bound fails 2 tests, failing to recognise
ourselves in the list fails 3, and comparing byte offsets while ignoring the file index fails 1.
Removing the empty-list shortcut fails nothing — correctly, since `elect_winner({})` is null and the
next branch reaches the same decision, so that early return is documentation, not logic.

- Effort: M | Impact: A promotion now picks the node that lost the least instead of the quickest one,
  which is the whole point of publishing positions. Follow-ups it exposed: #72 and #73

### 61. Multi-master catch-up compared WAL offsets across independent WALs ✅

A node that rejoined after an outage caught up once and then silently stopped. Reproduced with a
purpose-built three-node harness that logs to files, because the integration fixture keeps node stdout
in a pipe and the catch-up decision is invisible there:

```
cycle 0: node2 has 3/3 — OK
cycle 1: node2 has 3/5 — LOSES [720000, 721000]
  writer has: [700000, 710000, 711000, 720000, 721000]
```

The logs gave the mechanism rather than the symptom:

```
cycle 0: Peer 3 is behind (peer: file=0 off=174, local: file=0 off=522) — starting catch-up
cycle 1: Peer 3 is behind (peer: file=0 off=846, local: file=0 off=870) — starting catch-up
```

In cycle 0 the stream started at byte 174 of the *local* WAL and happened to land on a record boundary,
so everything arrived by luck. In cycle 1 the peer reported 846 against a local 870, so the node
concluded "behind by 24 bytes" and shipped the last 24 — exactly one empty `CHECKPOINT` record from
#62 — while the rows written during the outage sat earlier in the log. The two offsets have no common
scale: every node writes its own records plus copies of foreign ones. #62 made the drift faster by
adding checkpoints, and `AntiEntropyManager` (#57), the only second line of defence, is a stub.

**The fix** replaces the position comparison with a version vector: for each
`(symbol, exchange, origin)`, the highest sequence number below which nothing is missing. Sequence
numbers exist since #64 and are dense within an origin's stream, so a hole is arithmetic — which is why
this could not be fixed before #64 landed. Details in [architecture.md](architecture.md); the parts
that took measuring:

- **The entry is a frontier, not a maximum.** A peer can receive live record 7 before catch-up delivers
  6, and a maximum would report 6 as delivered. Records above the frontier are applied but do not move
  it.
- **Over-delivery is not free, which the design assumed it was.** Streaming everything a peer might
  lack turned #61's data loss into #26's duplicate rows: four outage cycles stored 25 rows where 9 were
  written. Storage is append-only, and the two mechanisms that look like they would prevent this do
  not — Last-Writer-Wins keeps its HLC state in memory and loses it on restart, and the columnar
  store's refusal to merge a duplicate segment path only hides a duplicate while the re-flushed segment
  covers the same timestamp range. The receiver now drops a record it has already applied, by sequence
  number, before the WAL append.
- **The vector has to survive a restart**, or every restart triggers a redelivery the node cannot
  recognise. It is written to the WAL as record type 7 next to the checkpoint, only when a frontier
  moved, and without fsync — losing it means restoring a lower frontier, which asks for too much.
- **Reusing the `WALRecordV2` envelope** for the vector means a node on protocol 1 skips it as an
  unknown record type instead of disconnecting, and after a two-second grace window it is treated as
  holding nothing.

**Found on the way:** serving the vector from the tracker under the engine mutex deadlocked the flush
thread against itself — `persist_version_vector_if_changed()` runs inside the block that already holds
`mtx_`, and `std::mutex` is not recursive. The thread stacks showed the flush thread waiting on a mutex
it held while every client write queued behind it. `sudo gdb -p <pid> -batch -ex "thread apply all bt"`
is how that was found in two minutes instead of by guessing; ptrace_scope blocks it without sudo.

Verified: 4 outage cycles with exact row counts (no losses, no duplicates), the integration test
extended to two outages because one passed for months while the defect was live, 5 dedup tests, 12
tracker tests for the frontier, 7 serialisation tests. Four mutations red — frontier as a maximum,
catch-up filter off by one, no receive-side dedup, vector not restored at startup. The dedup mutation
took three attempts to catch: LWW masked it, then the segment-path refusal masked it, which is
pitfall 37 twice in one afternoon.

`test_a_restarted_node_catches_up_on_what_it_missed` lost its `xfail(strict=True)` marker: it went
XPASS the moment the fix landed, which is what strict was for.

- Spec: `kiro-workspace/specs/mm-version-vector-catchup/`

### 68. STATUS killed every multi-master node (P0) ✅

Found while starting #57, by reading the class that item is about rather than the item.

`Engine::stats()` in multi-master mode did this:

```cpp
s.mm_anti_entropy_runs = mm_mgr_->anti_entropy().total_runs();
```

`MultiMasterManager::anti_entropy()` is `return *anti_entropy_;`, and **nothing in the repository ever
constructed `anti_entropy_`** — `grep -rn "make_unique<AntiEntropyManager>" src/` returned nothing. So
the first caller to ask a multi-master node for its statistics dereferenced a null `unique_ptr`.

Measured, on a live node started by `scripts/mm_harness.py`:

```
node came up, port 57139
1) STATUS ...
   reply:
   process alive? NO, exit=-11
```

And with no server and no etcd at all, straight through the library API: `Engine::stats()` on an
MM-enabled engine dumped core. That is `STATUS` from an operator, and every `/metrics` scrape a
monitoring system makes, taking down the node — on the feature this engine advertises for write
scalability.

**Why 640 unit and 117 integration tests missed it.** The multi-master modules exercise `INSERT`,
`SELECT`, `ROLE` and `MM_PEERS` but never `STATUS`. The metrics module runs `STATUS` and `/metrics`,
but against the plain `cluster` fixture with multi-master off. Both paths were covered; their
*crossing* was not. Same family as #25 (a field no test varied) and #64 (a field nobody filled), and
the reason it stayed invisible is the same: nothing was missing from the list of things to test, only
from the list of combinations.

**The fix** is two separate corrections, because two separate mistakes had to meet:

- `MultiMasterManager::start()` now constructs the manager and starts it, when a peer registry exists.
  This is #57's scheduler, which the roadmap described as working: it had never run once.
- `anti_entropy()` returns a **pointer**, not a reference. The component is optional — a node without
  coordinator endpoints has no registry and no scheduler — and handing out a reference to something
  optional is what made the bad call look correct. `stop()` had checked the pointer since the
  beginning; the accessor did not.
- `Engine::stats()` fills the counters only when the manager exists, and now also reports
  `mm_anti_entropy_repairs`, which `tcp_server.cpp` had been hardcoding to 0 with a comment saying it
  was unavailable. Zero has to mean "no scheduler" and not "ran and found nothing" — the same
  ambiguity that let #57 look finished.

Verified: 3 unit tests (`stats()` on an MM engine, repeated scrapes, and the no-registry case
answering instead of crashing) and 3 integration tests with their own etcd — `STATUS` leaves the node
alive, `/metrics` leaves the node alive, and the scheduler records a run within twenty seconds at a
one-second interval. Mutation red: drop the pointer check in `stats()` and the process dumps core
again.

- Spec: `kiro-workspace/specs/mm-status-crash/`

### 69. Queued output for a peer had no ceiling, and discarding it corrupted the stream ✅

Found while trying to make a partition test prove something about anti-entropy (#57), and the first
version of this entry got the measurement wrong — corrected below, because the wrong number was
already committed.

**Two defects, one code path.**

`MultiMasterManager::enqueue_frame()` appends every broadcast frame to `peer.send_buf` and tries to
drain it. Nothing capped that buffer on the live path: `check_backpressure()`, the only thing that
looked at its size, was called from exactly one place — the catch-up loop. A peer that stops reading,
whether partitioned, paused or slow, grows the writer with no limit.

Worse, what `check_backpressure()` did about it was `send_buf.clear()` while keeping the socket open.
`try_drain_send_buf()` erases the sent prefix after a partial write, so the buffer can begin in the
middle of a frame — clearing it leaves the peer waiting for the rest of a frame nobody will send and
reading everything after it as that frame's tail. That is not freeing memory, it is silently
corrupting the peer's framing until some invented length exceeds `MM_MAX_FRAME_PAYLOAD`.

**What was measured, and what was not.** With a 256 KB ceiling and one of three nodes isolated by
iptables, the buffer crossed it and the peer was dropped after about 240k levels written — so the
growth is real. But the first version of this entry claimed "+17.8 MB per 120k levels, about
113 MB/s per unreachable peer", and that number was wrong: a control run with the same writes and
**no** partition grew by +17.4 MB against +17.7 MB with one. The RSS was the writer's own pending
rows and columnar buffers, which grow with any write; the peer buffer contributed about 0.2 MB,
because the kernel socket buffer absorbs the first few megabytes. The unbounded case was not measured
to saturation — the writer stalls before that — so the magnitude here is code-verified, not
benchmarked, and the entry says so rather than repeating a proxy measurement as if it were the thing.

**The fix.** `--mm-max-peer-send-buffer` (default 64 MB, the ceiling a client session has had since
#59) checked on the live path, and on overflow the connection is **dropped** rather than the buffer
cleared: a closed socket is the only answer that does not lie about the state of the stream, and the
existing reconnect path then catches the peer up. `check_backpressure()` on the catch-up path now does
the same thing for the same reason. `ob_mm_peer_send_buf_bytes` and `ob_mm_peer_dropped_slow_total`
make a peer that is not draining visible before it disappears, the way `ob_session_pending_bytes` does
for clients.

**Verification, split by what is actually verifiable.** The framing half is deterministic and now has
tests: feed `parse_frames()` a stream where one frame is cut in half and five good frames follow, and
**none of the five is delivered** — the parser keeps counting bytes towards the frame the sender
abandoned, reports no error, and stays desynced. That is the receiver's view of `send_buf.clear()`, and
it is the reason the fix drops the connection instead.

The ceiling's end-to-end trip is not deterministic and the entry says so rather than pretending. With a
256 KB ceiling it tripped after about 160k levels written to a peer stopped with `SIGSTOP`, and in
another run with the same shape it had not tripped by 320k, because the kernel socket buffers absorb a
few megabytes first and their size autotunes. `MMH_MODE=slowpeer` in `scripts/mm_harness.py` runs that
scenario and reports what it observed, including "the ceiling never tripped, so this proves nothing"
when it did not — a diagnostic, not a verdict. What the code guarantees is the bound; what the harness
can show is that the bound is reachable.

- Spec: `kiro-workspace/specs/mm-send-buffer-cap/`

### 67. A node that joins an origin's stream mid-way never establishes a frontier

Found while writing #61's dedup tests, and worth its own item because the fix is a different mechanism.

The frontier means "I have everything from this origin up to here", so it can only leave zero if the
node followed the stream from its first record. A node that joins a cluster later, or whose peer no
longer retains the early records, sees sequence 5000 before it ever sees 1 — and cannot honestly claim
1-4999. Consequences, both bounded: it exports no entry for that origin, so peers keep sending it
records it already has; and because only frontiers are persisted, a restart loses the held set above
the frontier, so those redeliveries are applied again and duplicate rows.

The mechanism that fits is the one already in the codebase for this: snapshot bootstrap. A snapshot
carries the sender's state, so the receiver may legitimately declare frontiers from it — which is also
what `AntiEntropyManager::trigger_snapshot_repair()` is a stub for today (#57).

Not urgent: every node in a cluster that grew together follows its peers' streams from the start, and
the duplicate window is bounded by the 4096-entry held set. It matters when a node is added to a
running cluster.

**Half of this is closed.** The duplicate-rows-after-a-restart consequence was a separate mechanism —
the held set simply was not persisted — and is fixed under #75. What remains is the frontier itself: a
node that joined mid-stream still cannot claim contiguity, so it exports no entry for that origin and
peers keep sending it records it already has. That still needs a base established by snapshot
bootstrap, and **#76** records that multi-master bootstrap does not exist yet: `bootstrap_from_peer()`
is a stub with no callers.

- Effort: M | Impact: A late-joining node keeps receiving redeliveries. It no longer stores duplicate
  rows after a restart — that half went with #75

### 62. The WAL was written, fsynced, and never read back ✅

Found while investigating #61, by asking a question no test had asked: what does a node hold after a
crash?

**Measured before the fix.** Five `INSERT`s to a fresh server, each acknowledged, sent in one write so
the 100 ms background flush could not intervene. No `*.col` file existed at the moment of the kill and
the WAL held 680 bytes. `kill -9`, restart, `SELECT`: **0 of 5 rows**. Not a corner case, not a race —
the ordinary path, every time, for as long as the engine had existed.

The cause was three lines in `Engine::open()`: `WALReplayer::replay()` was called with a callback that
counted records and discarded them. So `FsyncPolicy::EVERY` flushed each record to the platter, and
recovery threw all of them away. The WAL worked perfectly as a replication source and not at all as a
recovery log, which is why the failure was invisible in a repository whose replication tests all pass.

**Why 585 tests missed it.** Every one of them ended in `close()`, which drains the pending rows and
flushes segments, so the data came back from the columnar store and the replay path was never the thing
under test. No test had ever killed a process. That is the whole explanation, and it is the reason the
new tests do not call `close()`: `Engine::release()` in C++, a real `SIGKILL` in Python.

**The fix.** A `CHECKPOINT` record (type 6) appended after — never before — a flush has written its
segment files and merged their metadata; `WALReplayer::replay_after_checkpoint()` forwarding only the
records past the last checkpoint, in two passes over the same parser rather than a second one written
for the tail; `Engine::apply_delta_replayed()` applying them without re-appending to the WAL,
re-broadcasting to peers, or waking subscribers; and an immediate flush afterwards, because
`QueryEngine` reads segments and a recovered row left in the SoA buffer is invisible to every `SELECT`
(pitfall 13, met again from the other side).

**The window a checkpoint cannot describe**, and the part worth reading twice: a crash between writing
the segment files and appending the checkpoint. Those records are then replayed although their rows are
durable. Skipping them by timestamp looks like belt-and-braces until you measure what happens without
it — the re-flush lands on the same segment path (`<active_segment_start>_<end_ts>`, and the start stays
0), so `ColumnarStore` refuses the merge as a duplicate, **but the refusal comes after the files were
rewritten in place**. Since the WAL is truncated only up to the replica-confirmed position, its tail can
hold fewer rows than the segment it is overwriting. Measured with the guard removed: 8 durable rows
became **6**. The guard prevents data loss, not duplicate scans.

That same collision is why the first two attempts at this mutation came back green: a backstop lower in
the stack was hiding the defect, so the mutation looked covered while no test had touched the changed
line. Recorded as a pitfall, because it invalidates mutation testing generally.

**Performance.** Ingestion and update latency are untouched, as expected — nothing was added to the
per-update path. Machine B, three interleaved rounds: ingestion median 2543 → 2552 ns/op (+0.4%),
update p50 6033 → 6003 ns (-0.5%), both far inside a ±30% run-to-run spread.

The flush path is where the cost landed, and the first version of it was real: fsyncing the checkpoint
under `FsyncPolicy::EVERY` cost **+0.22 ms (+10.5%)** on `FLUSH` (median 2.04 → 2.26 ms, 150 samples per
arm, interleaved). The checkpoint does not need to be durable — it only ever claims that rows already
are, so losing it costs a replay the timestamp guard then skips. Written without fsync, the difference
falls below the noise floor (median 2.18 → 2.04 ms, i.e. the wrong sign). Worth stating plainly: the
first measurement of this was garbage. `FLUSH` answers `OK\n\n`, the harness read one line per command,
so it drifted a line per iteration and reported 0.03 ms for a flush the roadmap documents at 2-3 ms —
pitfall 35 again, one layer up.

Verified: 7 unit tests, 5 integration tests that actually `SIGKILL` a server (including two crashes in a
row, and writes made after a recovery surviving the next crash), three mutations red — empty replay,
checkpoint before the flush, timestamp guard removed.
Spec: `kiro-workspace/specs/wal-replay-recovery/`

- Also added: `--flush-interval-ms` on `ob_tcp_server`. The recovery tests need rows to stay in the WAL,
  and hardcoding 100 ms made the test race the server instead of measuring it.

### 63. The replay guard assumes timestamps for a symbol arrive in order ✅

Found while writing #62, by asking what the guard assumes rather than what it does.

`replay_wal_tail()` skips a record when its timestamp is at or below the highest `end_ts_ns` among the
segments for that symbol. `SegmentMeta::end_ts_ns` is the timestamp of the **last** row written into
the segment, not the highest one in it, so the comparison is exact only while timestamps for a symbol
increase monotonically.

A single node satisfies that: `ob_tcp_server` stamps every write on arrival. Multi-master does not. A
peer's record carries the origin's timestamp and is appended to the local WAL after whatever arrived
locally in the meantime, so the tail can hold a record with a timestamp below an existing segment's
`end_ts_ns`. Replayed inside the crash window between writing segments and appending the checkpoint,
that record would be skipped as already durable when it is not — one lost row on a rejoining node.

The intersection is narrow (multi-master, plus a crash in a window of microseconds, plus an
out-of-order timestamp for the same symbol), which is why #62 shipped with it rather than waiting.
Two ways out, and the second is the honest one:
- Record `max(ts)` alongside `end_ts_ns` in `SegmentMeta`, and compare against that. Cheap, but it
  only shrinks the assumption instead of removing it: a segment can still be missing rows whose
  timestamps fall inside its range.
- Have the checkpoint carry the WAL position (file index + offset) it certifies, so replay starts from
  a position rather than inferring one from timestamps. The timestamp comparison then narrows to the
  crash window alone, where a row-level identity check on the replayed rows can settle it exactly.

**Fixed by making the answer a fact.** Every segment now records the WAL position its rows came from
(`wal_file_index`, `wal_byte_offset` in `meta.json`), and recovery compares positions instead of
timestamps. The invariant that makes it exact: a record reaches the WAL *before* it reaches
`pending_rows_`, and a flush drains all pending rows before writing any segment — so every row in a
segment came from a record at or before the position taken at that drain. Per symbol, which matters:
a crash that left one symbol's segment written and another's not is described correctly, because each
segment carries its own position.

The third option, neither of the two the entry proposed: `wal_identity`. A snapshot transfer and a
shard migration ship whole segment directories, `meta.json` included, so a received segment carries
the **sender's** position — and skipping by a foreign position would drop records this node never
stored, which is the expensive direction. Segments therefore record which WAL the position belongs to,
recovery trusts it only on a match, and the identity file lives at `<data_dir>/wal_identity`,
deliberately outside every segment directory so that it cannot travel with one. Missing identity or
position means "written before this existed": recovery falls back to the timestamp comparison for that
symbol and logs a warning naming what it did.

Three tests, each with the fix disabled to prove it is measuring something:

```
out-of-order timestamp inside a segment's range   fix: 4 rows   timestamp guard: 3 rows (the lost row)
position from another node's WAL                  fix: 4 rows   identity check off: 3 rows
crash window (segments written, no checkpoint)    fix: 8 rows   — passes either way, by design
```

The crash-window test needed rewriting rather than re-running, and the reason is worth keeping. It
used to build its state by **re-appending copies** of durable records after the last checkpoint, which
puts them at positions *above* the segment that holds them — a state the engine cannot produce, since
a record reaches the WAL before the row it produces reaches a segment. It now cuts the log at the last
checkpoint instead, which is exactly what a crash between writing segment files and recording that
fact leaves behind. A test whose construction the mechanism cannot reach will contradict the correct
fix, and did.

- Effort: M | Impact: Recovery no longer rests on an assumption that multi-master breaks. The guard is
  a position comparison, exact per symbol, and a position from another node's log cannot be mistaken
  for one of ours

### 64. Nobody assigned the sequence numbers, so three mechanisms were switched off by a zero ✅

Found while working out what #61 needs in order to be fixable at all.

`src/tcp_server.cpp` set `delta.sequence_number = 0` with the comment
`// server-assigned; engine handles sequencing`. The engine assigned nothing: it copied the value into
the WAL header and the stored row. Every write that ever came in over the network carried **0**, and the
comment was worse than no comment, because it told the reader the layer below handled it.

What that silently disabled:

- **Gap detection never fired.** `apply_delta()` in `src/soa_buffer.cpp` tests
  `prev_seq != 0 && update.sequence_number != prev_seq + 1`, and `prev_seq` is whatever the last write
  stored, which was always 0.
- **`append_gap()` was dead code.** The `GAP` record is as old as the WAL format, has a unit test, and
  had never been produced by a running server.
- **The `sequence_number` column in every segment was zeros**, so it was space in the format rather than
  data — the same shape of defect as #25, where segments silently dropped the order side.
- **#61 had nothing to be fixed on.** A `(origin_node_id, sequence_number)` version vector needs a
  sequence number to exist.

The interesting part is that the missing assignment was a *symptom*. A single counter per SoA buffer
cannot express per-origin sequencing: in multi-master, records from two origins land in the same field,
so every interleave would be reported as a gap. That is why the mechanism was switched off by a zero
rather than merely forgotten, and why the fix is not "fill the field in".

**The fix.** `SequenceTracker` (`src/sequence_tracker.cpp`) holds, per symbol, a local counter and the
last number seen from each origin. `Engine::stamp_sequence()` assigns when the number is 0 and passes a
non-zero one through untouched — the discriminator has to be the value, not the caller, because the
replica path shares `apply_delta()` with client writes and must keep the primary's numbering. Gaps are
decided per origin, append a `GAP`, increment `ob_sequence_gaps_detected` and log symbol, origin and the
expected number. Counters are restored at startup from `SegmentMeta::max_sequence_number` (new field in
`meta.json`; absent means 0, which is the truth about older data) and from the WAL tail replayed by #62,
both of which only raise. Replay seeds the tracker instead of assigning, so a gap recorded once is not
re-reported on every restart.

**Found on the way:** `Engine::apply_remote_delta()` dereferenced `hlc_` and `mm_mgr_` with no null
check, so calling it on a node without multi-master dumped core. Unreachable through the server — only
`MultiMasterManager` calls it — but it is a public method on a library type, and a test that called it
found out the hard way. It now answers `OB_ERR_INVALID_ARG`.

Verified: 14 unit tests on the tracker, 9 on the engine (numbers in the WAL read back through a replayer
rather than a getter), 3 integration tests that read `meta.json` from a real server's data directory,
four mutations red — assigning over a supplied number, one high-water mark for all origins, no restore
from `meta.json`, and replay assigning instead of seeding. 615 C++ tests green, 142 s.

**Performance, and what the control benchmark actually caught.** Eight interleaved rounds on machine B
(order reversed each round, pinned with `taskset`), compared **pairwise** — the ratio within a round,
because slow thermal drift moves both arms together and cancels in a ratio. Medians: ingestion +3.0%
(7 of 8 rounds positive), update p50 +1.0% (signs random). Then the control: `BM_VwapLatency`, a read
path this change cannot touch, came out **-40.6% in 8 of 8 rounds**. `objdump` on both binaries puts
the benchmark function at the same address (`0xc02f`, mod 32 = 15) with the same 21 instructions,
differing only in the call offsets to `_M_dispose`. So that is binary layout, the pitfall `bench-guard`
documents at ±37% on this very benchmark — which means a few-percent signal from the engine benchmark
on this machine is not evidence of anything.

So the cost was measured directly instead, on the mechanism: `SequenceTracker::observe()` is
**54.4 ns** per call (one string-keyed lookup plus one origin-keyed lookup) and building a
`"SYMBOL.EXCHANGE"` key is **27.0 ns** (allocation plus concatenation). Net against master is
**~27 ns per write, about 1% of a 2883 ns ingestion op**, because the change also removed one of the
two key builds per write: `apply_delta()` built it once for the migrated-symbol check and again in
`stamp_sequence()`, and now builds it once and passes it down.

- Status: **DONE**
- Spec: `kiro-workspace/specs/wal-sequence-numbers/`

### 66. The write path built the same key string four times ✅

Measured while benchmarking #64, fixed now that the numbers were known.

Every write builds `"SYMBOL.EXCHANGE"`: once for the migrated-symbol check, once more inside
`get_or_create_buffer()`, which took its arguments as `std::string` **by value** — so calling it with
the `char` arrays from `DeltaUpdate` created two temporaries and then concatenated them, three
allocations of their own. Four in total on the hottest path in the engine.

The write path already has that key in hand: it needs it for the migrated-symbol check and, since
#64, for the sequence tracker. So `get_or_create_buffer()` now takes the prebuilt key plus the two
`char` pointers it needs when it actually creates a buffer, and every hot call site passes what it
already built. The old two-argument overload stays for callers that do not have a key.

Measured on machine B, the exact pattern the engine used against the same lookup with a key in hand:

| | ns per write |
|---|---|
| build the key, then look up | 53.6 |
| look up with the key already built | 8.8 |

**44.8 ns saved per write**, about 1.6% of a 2883 ns ingestion op. Measured on the pattern rather than
with `bench_engine`, for the reason #64 established: a few percent is below what the engine benchmark
can resolve on this machine, and the control benchmark proved it by moving 40% on an unrelated read
path.

- Effort: S | Impact: Three allocations off every write, on the path the engine's headline number
  measures

### 65. The sequence number is not visible to a client ✅

`format_query_response()` sends six columns — timestamp, price, quantity, order_count, side, level — and
the sequence number is not one of them, although `QueryEngine` fills it into `QueryResult`. After #64
these numbers are real and per-origin, so exposing them would let a client detect for itself that rows
it received have a hole in them.

Not free: it means a seventh column in the row format, `kQueryHeader`, the Python client's row parsing,
the C++ client, `docs/cli.md`, and the tests that assert response shape. Worth doing deliberately rather
than as a side effect of #64.

`sequence_number` is now the **seventh and last** column of a `SELECT` response. Last on purpose: a
client that reads columns by index keeps working unchanged, and one that reads by name finds the new
field. The same value reaches the three other readers the engine ships — the C++ client
(`QueryRow::sequence_number`), the Python client (`OrderbookRow.sequence_number`) and the interactive
CLI, which grew a `seq` column so the number is not visible on the wire and invisible in our own tool.

Compatibility went both ways and both are tested. A new client against a six-column server reads 0 —
"unknown" — instead of failing or shifting a field. A truncated or non-numeric seventh column is still
a parse error, because handing back half a number would let a caller believe it knows where it is in
the stream.

The C API needed a decision rather than an edit: `ob_result_next()` is a C entry point somebody may
have compiled against, so the extra out-parameter went into a new `ob_result_next_seq()` and the old
function delegates to it. The Python binding uses the new one when the loaded library exports it and
falls back otherwise, so an in-process query reports the same numbers a TCP query does. Without that
the field would have been a silent 0 in pool mode — the exact class of defect #64 was.

What 0 means is now written down in four places, because it means two different things and neither is
"the first row": the row predates sequencing, or the server predates the column.

Verified beyond "it compiles": the formatter round-trip property now covers the seventh column, three
new client unit tests cover the new/old/garbage cases, and three integration tests read the numbers
off a live server — that the header names the column last, that the highest number a client sees
equals `max_sequence_number` in `meta.json` for the same symbol, and that ten writes produce ten
consecutive numbers with no hole. The module's docstring used to say "a client cannot see these
numbers, so asserting on `SELECT` output would prove nothing"; it now says the opposite, and the
tests do the asserting.

**What the column costs.** Measured directly on the formatter rather than through `bench_engine`,
which cannot resolve this size of change on this machine: formatting 1000 rows, best of five
interleaved rounds, on the i3-7100U development machine. Seven columns cost **+41 ns per row (+23%)**
over six — one more `std::to_string` of a `uint64` and one more tab. A 1000-row response therefore
spends about 40 µs more in formatting. The upper end of the range is quoted on purpose: understating
a cost is the same mistake as overstating a speed.

The measurement needed two attempts, and the reason is worth more than the number. The first version
kept the six-column control in the **same translation unit** as `main`, where it could be inlined,
while the real function lives in `response_formatter.cpp` and cannot be. Moving the control into its
own translation unit — the same shape as the thing it is compared against — changed the *control* by
28 ns, which is most of the effect being measured. A control that is not built like the subject
measures the harness.

- Effort: S | Impact: A client can verify the completeness of what it received instead of trusting the
  server


---

## Recommended order

No P0 is open. The four that were — #60, #61, #62, #64 — are closed, and so is #73, which was found
while proving #70 on a real cluster rather than by reading the code.

| Priority | Item | Effort | Why now |
|----------|------|--------|---------|
| **P1** | Position freshness on election (#72) | M | Deference cannot tell a further replica from a dead one, so every two-node failover pays the full window for nothing |
| **P1** | A node joining mid-stream never establishes a frontier (#67) | M | It can catch up but cannot prove it has everything, which is the guarantee the version vector exists to give |
| **P1** | Deployment artifacts (#33) | M | Cheapest large jump in time-to-first-run |
| **P1** | Reproducible comparative benchmarks (#39) | L | Makes the performance claim verifiable by a reader instead of asserted |
| **P1** | Authentication and TLS (#30) | L | The single blocker to production adoption |
| **P2** | Argument parser refactor (#36) | S | Closes 27 static-analysis findings; without it every PR adding a CLI flag needs a manual review-thread resolution |
| **P2** | CI hardening with sanitizers (#37) | S | Strong quality signal, low cost, catches real concurrency bugs |
| **P2** | Configuration file (#32) | S | Ops ergonomics |
| **P2** | Documentation site (#40) | M | Lowers evaluation friction |
| **P2** | Release engineering + PyPI wheels (#42) | S | `pip install` is the shortest path to a first user |
| **P3** | Time-bucketed aggregation (#44) | L | The most-requested analytical capability for this data |
| **P3** | Arrow output (#46) | M | Near-zero integration cost for analytics teams |
| **P3** | Backup and restore (#34) | M | Table stakes for a database |
| **P4** | Chaos testing (#54) | L | Do this once there are users whose data can be lost |
| **P4** | Performance frontier (#49-50) | varies | Proves the bespoke-engine claim; pick one and write it up |

## Known gaps and honest caveats

Things a reviewer will notice, listed here so they do not look like oversights:

- **No authentication, no TLS.** Trusted-network deployment only (#30).
- **Neither suite kills a process except in one module.** Until #62 that was every module, and it hid
  total loss of acknowledged writes on crash. `tests/integration/test_crash_recovery.py` is the only
  place a server is `SIGKILL`ed; fault injection more broadly is still #54.
- **Anti-entropy is a scheduler with no reconciliation** (#57). The spec task is marked complete and
  the metrics report runs, but gap detection and repair are placeholders that return "nothing found"
  and "cannot repair". Reconnect catch-up is the only thing healing divergence today.
- **Benchmark baselines were recorded on one developer machine** with no hardware description. The
  table below fixes that going forward. Any published number needs its hardware next to it.
- **The README advertises streaming subscriptions** that are not verified to exist in the current
  wire protocol (#45): implement it or correct the claim.
- **Aggregation SIMD is opt-in and off by default** (`OB_ENABLE_AVX2=OFF`), so default builds do not
  show the SIMD numbers.

## Performance baselines

Baselines are hardware-specific. **Never quote a number without the machine it came from.**

### Reference machine A — recorded May 2026 (specification not captured; treat as indicative)

| Metric | Value | Notes |
|--------|-------|-------|
| Native ingestion | ~1.35M updates/s | C++ benchmark, single core |
| Native update latency | ~2.8 µs p50 | C++ benchmark |
| PING latency (epoll) | ~45 µs avg | Python client, loopback |
| PING latency (io_uring) | ~24 µs avg | Python client, loopback |
| Single INSERT (TCP) | ~0.3 ms | Python client |
| MINSERT 1000 levels (TCP) | ~3 ms | Python client, single round-trip |
| FLUSH (incremental) | ~2-3 ms | Non-blocking, two-phase |
| LZ4 INSERT (TCP) | ~1.6-2.9 ms | After Nagle fix |
| Sustained INSERT throughput | 29k/s | Python TCP, 60s stress test |
| Sustained MINSERT throughput | 777k levels/s | Python TCP, 60s stress test |
| Failover time | ~5-8 s | etcd lease TTL dependent |

### Machine B — Intel Core i3-7100U @ 2.40GHz, 2C/4T, August 2026

A 2017 ultra-low-voltage laptop CPU. Numbers here run roughly 3x below machine A on CPU-bound
benchmarks. **This is a hardware difference, not a regression** — Release build, `-O3 -DNDEBUG`,
verified. Do not use this machine for published figures.

| Metric | Value | Machine-A threshold | Ratio |
|--------|-------|---------------------|-------|
| `BM_IngestionThroughput` | 387k updates/s | ≥ 1.0M/s | 3.5x slower |
| `BM_UpdateLatency` p50 | 10.6 µs | ≤ 5 µs | 3.9x slower |
| `BM_UpdateLatency` p99 | 10.8 µs | — | — |
| `BM_VwapLatency` | 1577 ns (1000 levels) | ≤ 1000 ns | 1.6x slower |
| `BM_TimeRangeQuery` (10k / 100k rows) | 0.549 ms / 3.40 ms | ≤ 5 ms | inside |

The `BM_TimeRangeQuery` figures above replace an earlier `0.004 ms / 0.004 ms`, which was not a
measurement of anything. The benchmark issued `SELECT ... FROM orderbook WHERE symbol='...'`, a syntax
the parser does not accept; `execute()` returned an error string, the row callback never ran and
`benchmark::DoNotOptimize()` swallowed the error. What got published as scan latency for 100k rows was
the cost of rejecting a malformed query. The benchmark now uses the real grammar and aborts if the
scan returns zero rows, so it cannot silently measure nothing again. Honest scan throughput on this
machine is 19-29M rows/s.

Run-to-run variance on this machine is high (a first run under background load reported 298k/s
against 387k/s on an idle run). The earlier 8.6 µs figure for `BM_UpdateLatency` came from a run with
cv 21% and was optimistic; repeated measurement on an idle machine gives 10.6 µs at cv ~1.3%. Quote
the low-variance number. Thermally throttled laptop CPUs
are not benchmark hosts. Treat these figures as a smoke test that the engine works, nothing more.

### Regression thresholds

The thresholds used by `bench-guard` (IngestionThroughput ≥ 1.0M/s, UpdateLatency ≤ 5µs,
VwapLatency ≤ 1000ns, TimeRangeQuery ≤ 5ms) are **machine-A thresholds**. They are not meaningful on
slower hardware. Compare a run against the previous run **on the same machine**, and reserve
absolute thresholds for a designated benchmark host.

### Test suite

| Suite | Count | Status |
|-------|-------|--------|
| C++ (GTest + RapidCheck) | 673 | all passing, ~152s with `ctest -j1` on machine B |
| Python integration | 121 | passing, plus 2 skipped. **No xfails left**: #60's and #61's markers both fell with their fixes |
