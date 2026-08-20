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

### 36. Refactor the argument parser to stop mutating the loop counter
- `parse_args()` in `src/tcp_server.cpp` consumes flag values with `argv[++i]` inside
  `for (int i = 1; i < argc; ++i)`. That is 27 instances of `cpp/loop-variable-changed`, and a classic
  source of off-by-one bugs when a flag is added or reordered
- Individual lines cannot be fixed in isolation: advancing past a flag's value requires modifying `i`,
  so it takes restructuring the loop. A small helper that takes the flag name and returns its value,
  advancing an explicit index, removes the whole class at once
- Practical consequence beyond tidiness: `required_review_thread_resolution` is enabled on `master`,
  so CodeQL opens a review thread on any PR that touches one of these lines, and every such PR needs a
  manual resolution before it can merge. This came up while merging the graceful failover fix, whose
  only offence was adding two flags in the file's established style
- Effort: S | Impact: Closes 27 static-analysis findings and removes friction from every future PR
  that adds a CLI flag

### 37. CI hardening
- Sanitizer jobs: ASan + UBSan on the full test suite, TSan on the concurrency-heavy subset
  (SoA seqlock, multi-master io_loop, group commit)
- Coverage report with a badge; `OB_ENABLE_COVERAGE` already exists
- Matrix build: GCC and Clang, Debug and Release
- Effort: S | Impact: A sanitizer-clean concurrent C++ codebase is a strong quality signal

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

### 57. Complete the anti-entropy implementation
- `AntiEntropyManager` runs, logs and reports metrics, but its three working methods are
  placeholders: `detect_gaps()` always returns an empty list, `repair_gap()` returns false, and
  `trigger_snapshot_repair()` does nothing. The scheduler around them is real; the reconciliation
  is not
- `detect_gaps()` needs the Engine wiring flagged as `TODO(task-12)` in `src/anti_entropy.cpp`:
  compare local `wal().current_file_index()` / `current_offset()` against each peer's published
  position from `PeerRegistry`
- `repair_gap()` should issue a catch-up request over the existing `MultiMasterManager` path;
  `trigger_snapshot_repair()` should reuse the snapshot bootstrap machinery from item #12
- Until then, divergence is only healed by the reconnect catch-up path. Two peers that drift while
  staying connected stay drifted
- Effort: M | Impact: Closes the one component in the architecture that does not do what its name says

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

### 60. Graceful failover cannot work outside its own tests (P0)

`FAILOVER <target_node_id>` validates the target by looking for it in
`CoordinatorClient::get_published_positions()` (`src/failover.cpp:206`). Nothing in production ever
publishes a position:

```
$ grep -rn "publish_wal_position" src/
src/shard_coordinator.cpp:238:  (void)coordinator_->publish_wal_position(0, 0);  // verify connectivity
src/coordinator.cpp:536:        bool CoordinatorClient::publish_wal_position(...)
```

One connectivity check with `(0, 0)` and the definition itself. Every other caller is in
`tests/test_etcd_integration.cpp`, which publishes positions by hand before exercising the feature.

So on a real two-node cluster, **every graceful handover answers `ERR unknown_target`** — measured
against the integration cluster: `FAILOVER node-1` → `ERR unknown_target node-1`, with both nodes
healthy and `node-1` holding the replica role. `docs/cli.md` documents that error as "usually a typo in
a node id". In reality it means nobody publishes positions.

The same absence affects `elect_winner()` (`src/failover.cpp:521`), which picks the most advanced
replica from those positions. With no positions, election cannot prefer the node with the least data
loss — and automatic failover demonstrably still works, so it is choosing by some other route. That
needs establishing rather than assuming.

Item #29 recorded this feature as fixed. What was fixed was real — the handover intent and the
election cooldown, verified by C++ tests against live etcd — but those tests publish the positions the
server never publishes, so the fix has never run in the configuration it ships in. Sixth instance of
the same pattern in this repository: the layer works, the layer above never feeds it, and no test
crosses the boundary.

Scope: publish each node's WAL position periodically (the `FailoverManager` monitor loop already
ticks), under a lease so a dead node stops being "known", then confirm both target validation and
position-based election against a real cluster. `tests/integration/test_failover.py` already contains
the test, marked `xfail(strict=True)` so it turns red the moment the defect is fixed and the marker
becomes a lie.

- Effort: S-M | Impact: An advertised HA operation currently cannot be performed on a real deployment


### 61. Multi-master catch-up compares WAL offsets across independent WALs (P0)

A node that rejoins a multi-master cluster catches up on the first outage and silently stops catching
up on later ones. Measured on three nodes, killing and restarting the same node three times, writing
one row before and one row during each outage:

| Cycle | Rows on the restarted node | Expected |
|-------|----------------------------|----------|
| 0 | 2 | 2 |
| 1 | 4 | 4 |
| 2 | **5** | 6 |

The row written during the third outage never arrives. No error is reported anywhere, on either node.

The mechanism is in the handshake. `handle_handshake()` decides whether to stream:

```
Peer 3 is behind (peer: file=0 off=450, local: file=0 off=600) — starting catch-up
```

That comparison is between **byte offsets in two independent WALs**. In multi-master every node writes
its own local records *and* the remote records it applies, in whatever order they arrive, so the same
logical set of records produces different offsets on different nodes. After a couple of outages the
rejoining node's offset can equal or exceed the peer's while it is still missing records the peer has,
and the peer concludes there is nothing to send. Catch-up working at all on the first outage is
coincidence, not design: the offsets happened to line up in the useful direction.

Byte offsets cannot express "which records does this node not have". That needs per-record identity —
`(origin_node_id, sequence_number)` is already carried in `WALRecordV2`, or the HLC — compared as a set
rather than as a scalar position.

There is no second line of defence: `AntiEntropyManager` (#57) is supposed to detect and repair exactly
this, and `detect_gaps()` returns an empty list unconditionally. So the two defects compound —
reconciliation is the mechanism that would have caught the flawed comparison, and it does nothing.

`tests/integration/test_mm_failover.py::test_a_restarted_node_catches_up_on_what_it_missed` is marked
`xfail(strict=True)`: it fails on the current server and will turn the suite red as soon as the fix
lands, so the marker cannot outlive the defect.

- Effort: M | Impact: Silent data loss on a rejoining node, in the topology the engine advertises for
  write scalability

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

### 63. The replay guard assumes timestamps for a symbol arrive in order

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

- Effort: M | Impact: One lost row per occurrence on a multi-master node, in a window that only opens
  on an unclean stop. Correctness of a guard that currently rests on an unstated assumption

### 64. Nobody assigned the sequence numbers, so three mechanisms were switched off by a zero

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

### 66. The write path builds the same key string three times

Measured while benchmarking #64, and worth its own item because the numbers are known and the fix is
not free.

Every write builds `"SYMBOL.EXCHANGE"` and looks it up more than once: `apply_delta()` builds it for
the migrated-symbol check (and after #64 passes that one down), `get_or_create_buffer()` takes
`std::string` parameters by value and concatenates again — two temporaries plus the result — and
`SequenceTracker` keeps its own map keyed by the same string. Measured on machine B: 27.0 ns per key
construction, 54.4 ns for the tracker's `observe()`.

Unifying per-symbol state into a single map — buffer, sequence counter and origin high-water marks
under one key — would leave one lookup and no extra allocations per write, and would very likely come
out *ahead* of master rather than level with it. This was the original shape in #64's design and was
deliberately deferred: it changes shared `Engine` internals (7 uses of `buffers_`, 4 of `live_ptrs_`,
plus the flush, snapshot and TTL paths), so it deserves its own change with its own measurement rather
than riding along with a correctness fix.

Note for whoever does it: on machine B the engine benchmarks cannot resolve a few percent (see #64), so
the verification has to be a direct measurement of the mechanism, not a `bench_engine` A/B.

- Effort: S | Impact: Removes three allocations and one hash lookup from the hottest path in the engine

### 65. The sequence number is not visible to a client

`format_query_response()` sends six columns — timestamp, price, quantity, order_count, side, level — and
the sequence number is not one of them, although `QueryEngine` fills it into `QueryResult`. After #64
these numbers are real and per-origin, so exposing them would let a client detect for itself that rows
it received have a hole in them.

Not free: it means a seventh column in the row format, `kQueryHeader`, the Python client's row parsing,
the C++ client, `docs/cli.md`, and the tests that assert response shape. Worth doing deliberately rather
than as a side effect of #64.

- Effort: S | Impact: A client can verify the completeness of what it received instead of trusting the
  server


---

## Recommended order

| Priority | Item | Effort | Why now |
|----------|------|--------|---------|
| **P0** | Multi-master catch-up loses records (#61) | M | A rejoining node silently misses records after a second outage, and anti-entropy that should catch it is a stub |
| **P0** | Graceful failover unreachable (#60) | S | `FAILOVER <target>` always answers `ERR unknown_target` outside the C++ tests, because nothing publishes node positions |
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
| C++ (GTest + RapidCheck) | 615 | all passing, ~142s with `ctest -j1` on machine B (592 on master, 23 more with #64) |
| Python integration | 113 | passing, plus 2 skipped and 2 `xfail(strict=True)` for #60 and #61; ~3.7 min |
