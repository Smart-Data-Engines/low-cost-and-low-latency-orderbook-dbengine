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
- **Not its control plane** (#175, measured 25 September 2026): `ShardCoordinator`'s "etcd registration" writes nothing — no shard node, no `shard_map` — so each shard owns every symbol, two on one etcd become a primary and its replica, and neither client finds a shard. The parts above are real; what joins them is #175.

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

### 30. Authentication and TLS on the wire protocol ✅

**All three parts are done.** Client sessions authenticate and so do the node links (parts one and
two, spec in `kiro-workspace/specs/wire-authentication/`), the client port encrypts with both
shipped clients verifying the name, and the replication link and the mesh encrypt and verify each
other (part three, spec in `kiro-workspace/specs/wire-tls/`).

The item bundled two independent things with different risk profiles, so it was built in that order.
Authentication is a protocol concern: one place per surface, once per connection, off the hot path,
and testable deterministically. TLS is a transport concern that enters the I/O loops — and there are
four of them (epoll, io_uring, replication, multi-master), each with its own framing and its own
output-buffer machinery. Authentication is also the half with more value per line: without it anyone
who reaches the port is primary in the cluster, while TLS without authentication encrypts the traffic
of an unknown peer.

**Part one — client sessions ✅**
- Challenge-response over HMAC-SHA256 (`AUTH` → `OK CHALLENGE <nonce>` → `AUTH <identity> <hmac>`),
  **not a bearer token**: with no TLS yet, a token seen by a passive observer is replayable for ever
  and a response to a fresh 32-byte CSPRNG nonce is not. One round trip per connection.
- The gate sits **before** `execute_command`'s switch. A per-case check means the next command added
  without one is reachable unauthenticated and nothing fails; the classifier's switch has no
  `default:`, so `-Wswitch` makes a new `CommandType` a build failure, and a test refuses a
  `default:` being added.
- One seam covers both transports, because epoll and io_uring share `ob::Session` and
  `execute_command`. At the time no CI job built the io_uring file, so a static test refuses an
  `execute_command` call from a transport that passes no credential store.
- The surface label (`client` / `replication` / `mm`) is inside the HMAC input. Replication and
  multi-master share one cluster secret, so without domain separation a response captured on one of
  those links authenticates on the other.
- Secrets come from files only — `--auth-secret-file`, `--cluster-secret-file`. **No flag carries a
  secret value**: an argument is in `/proc/<pid>/cmdline` for every process on the machine, and
  `--print-config` exists to be pasted into a ticket, so `ServerConfig` holds paths.
- Eight refusals at startup, each fatal, including a file readable beyond its owner (the message
  prints the mode found) and **the cluster secret also being a client secret** — a client holding
  that can present itself as a replica and stream the whole write-ahead log.
- Only the line terminator is stripped from a secret file. A general trim makes two different files
  the same secret, and for a secret "silently the same" is a security property. The flagship
  product's `read_bytes().strip()` shortened a random salt in ~5% of files.
- Identity in logs and in `STATUS`; three unlabelled counters. **No identity label**, deliberately:
  per-identity attribution belongs to #31 where an identity gains permissions, and a label fed by
  the name a peer *claims* before authenticating is an unbounded label set an attacker controls.
- **Found by the integration test, not the unit test:** `request_close_after_flush()` was consulted
  only in the EPOLLOUT drain, so a response small enough to fit the socket buffer left the session
  open with the flag set and nothing reading it. `ERR auth_failed` is eighteen bytes, and closing on
  the first failure *is* the rate limit. The io_uring loop consulted the flag nowhere at all. The
  unit test had asserted the flag rather than the effect — pitfall 45, from the other side.
- Also `--metrics-bind`, because the metrics endpoint has no authentication and deliberately none: a
  Prometheus scraper cannot perform a challenge-response, so a bearer token would be the weaker
  mechanism that ends up used, and binding to a private interface is the stronger answer.

**Part two — cluster links ✅**
- Mutual challenge-response on the replication link and the multi-master mesh, under
  `--cluster-secret-file`. **Mutual on these and one-way on the client link**, because a shared
  secret only proves identity among holders who are equally trusted: nodes are, a client population
  is not. A client proving the *server's* identity is TLS's job, and pretending otherwise with a
  secret every client holds would be theatre — any client could impersonate the server to another.
- **One flag per side, not two.** This side sends no handshake until the peer has proved itself and
  the peer applies the same rule, so mutual authentication falls out of the symmetry; `peer_proved`
  is a property of the *connection* and resets on every reconnect.
- Replication is a text protocol, so the order is fixed to keep either side from ever handling an
  out-of-order message: the primary challenges on accept, the replica sends **its challenge before
  its response**, and the primary's two replies therefore arrive as `AUTH <hmac>` then `OK AUTH`.
  `ERR unauthenticated` goes on the wire *before* the close — a replica merely missing its secret
  would otherwise see a reconnect loop with no message.
- Multi-master gets two frame types (205, 206) before `HandshakeMessage`, and **no protocol version
  bump is needed** because framing disambiguates them: a handshake frame is exactly 17 bytes and an
  authentication frame carries a 38-byte `WALRecordV2` header. A 17-byte frame from an
  unauthenticated peer therefore means **a peer running without a cluster secret**, and is logged as
  that sentence — the fix is on the other node, and calling it a short or malformed frame would send
  an operator into this one's code. The handshake **is** the acceptance; there is no third message.
- No mixed mode, documented rather than left to be discovered.
- Every refusal is paired with the exchange that must succeed. A gate that refuses everything
  demonstrates nothing, so `ClusterAuthReplication` has three tests: same secret replicates, no
  secret replicates nothing, wrong secret replicates nothing.

**Part three — TLS on all three surfaces ✅**

Spec: `kiro-workspace/specs/wire-tls/`. Series C did the client port
(`--tls-client --tls-cert-file --tls-key-file`, TLS 1.3 minimum, both shipped clients verifying by
default); series D did the replication link and the mesh (`--tls-replication`,
`--tls-multi-master`, `--tls-ca-file`, `--tls-peer-names`), where TLS is **mutual** and mTLS is what
gives channel binding.

- **Verification is two checks, and only one of them is what `SSL_VERIFY_PEER` does.** Chain
  verification says the certificate was signed by a CA you trust; it says nothing about whether the
  certificate belongs to the host you dialled. So with a private CA that signs a whole cluster —
  which is how anyone actually deploys this — node B's certificate is perfectly acceptable for node
  A, the relay of the paragraph below works again between two holders of *legitimate* certificates,
  and every verification reports success. `tls_expect_host()` binds the name; an IP literal takes
  the other branch in both halves (no SNI, per RFC 6066, and matched against `iPAddress` rather than
  `dNSName`), and getting either wrong looks like working code.
- **The test that carries this is the one with a good chain and the wrong name.** A certificate
  handed to the client as its own trust anchor, issued for `10.0.0.2`, served on `127.0.0.1`.
  Deleting the name check makes it pass; the neighbouring trust test does not move, so the two
  failures discriminate rather than overlap. Both clients have it, at unit and integration level.
- **A protection an operator cannot see is a protection on our word**, so `tls_verify=False` is a
  named act in both clients: a startup WARN from the C++ context and a `warnings.warn` from Python,
  and the escape hatch has its own test where the certificate the other tests refuse is accepted.
- **The trust anchor loads before the socket.** A CA path that does not exist is permanent and
  knowable without a network; a refused connection is transient. In the other order an operator
  whose server is also down is told `connection refused` and goes to debug the network — which is
  what it said until the test for it was written.
- **Four client configurations are refused rather than interpreted**, each describing a caller who
  believes the connection is protected in a way it is not: a CA file without TLS, `tls_verify=False`
  without TLS, a CA file with verification off, and TLS in local mode. All four fire before a socket
  exists.
- **`OrderbookTlsError` is deliberately neither `OrderbookError` nor `OSError`.** The Python pool's
  retry paths catch both, and `ssl.SSLError` *is* an `OSError`, so a certificate that fails to
  verify would be retried against every node in the mesh — each failing identically, because the
  cause is the client's own configuration — and the operator would read `No primary available`
  instead of `certificate verify failed`. A peer that drops mid-handshake stays retryable.
- **A gap from part one closed on the way past:** `PoolConfig` and `ShardRouterConfig` carried
  neither credentials nor transport, so the C++ pool and the sharded client could not reach an
  authenticated node **at all** — `auth_identity` existed on `ClientConfig` and nothing put it
  there. Three sites hand-copied the fields each happened to know about. One
  `copy_client_access()` template now carries them, and a static test derives `ClientConfig`'s field
  list from the header and refuses a field that neither the template nor every construction site
  mentions, because the next field is the one that drifts (pitfall 79: a list you wrote yourself is
  not evidence about the code).
- **Both misconfigurations were measured, and they fail differently.** A forgotten `--tls-client` on
  the server fails the client at once with `wrong version number`: the plaintext banner arrives
  where a ServerHello was expected. A forgotten `tls=True` on the client **hangs until the client's
  timeout** and the server logs nothing — this protocol has the server speak first, so both sides
  wait, and until a byte arrives the server cannot tell a plaintext client from a slow one. Not
  fixable; the test is named after the behaviour so the hang is read as the right thing.
- **The harness had the same defect in miniature.** The TLS node fixture used a *verifying*
  connection as its readiness probe, so a node deliberately issued a certificate for another
  address reported `node never answered` while its own log said `listening`. A probe that answers
  two questions with one word — pitfall 92's shape, in a fixture.
- **Channel binding is the thing it buys beyond confidentiality, and part two cannot have it.**
  Challenge-response proves knowledge of the secret; nothing ties the exchange to the connection it
  happened on, so an attacker who can redirect a replica's connection relays both directions and
  both sides believe they are talking to each other. That is a limit of a shared secret without a
  channel identity rather than a defect — a relay can forward any value bound only to a nonce — and
  it is written into `SECURITY.md` as a limit rather than left for a reader to assume otherwise.
- **The shape is decided, and it was decided from a measurement** — `benchmarks/tls/`, run before
  any of this was designed. Eight interleaved rounds on i3-7100U over loopback, warm-up discarded,
  at sizes taken from the wire protocol: 5 B is a `PING`, 60 kB is a `MINSERT` of a thousand levels.

  | payload | plaintext | TLS 1.3 (OpenSSL) | TLS 1.3 + kTLS TX |
  |---|---|---|---|
  | 5 B | 31.94 µs (cv 2.7%) | 52.84 µs — **1.68×** (1.56–1.73) | 56.92 µs — 1.77× |
  | 60 kB | 59.84 µs (cv 4.7%) | 230.28 µs — **3.70×** (3.64–4.20) | 265.48 µs — 4.38× |

- **In-process, not a sidecar.** A sidecar pays the same record-layer cost plus a loopback hop, so
  it cannot be faster by construction. It stays a documented deployment option with its price
  named: the engine then sees `127.0.0.1` rather than the client's address, so part one's
  authentication log lines and #31's ACLs stop distinguishing clients.
- **No kTLS, and this killed the design that was about to be proposed.** It measured 1.08× and
  1.15× *slower* than plain OpenSSL, with the range's lower bound at 1.03 on the large size. Scoped
  honestly: loopback is the record layer's CPU cost with no NIC in the way, and kTLS exists to
  avoid a copy and to hand encryption to hardware that can do it — so this is evidence against kTLS
  **on this path** rather than evidence that kTLS is slow, and that assumption expires the day a NIC
  with TLS offload is in the picture.
- **TLS 1.3 minimum**, even though TLS 1.2 is what a full kernel data path would need: probed rather
  than assumed, this OpenSSL negotiates kTLS receive only on 1.2. A public database engine capped at
  1.2 in 2026 is a review finding, and the io_uring path that would protect is off by default and
  had no CI build at the time (#108 later added compilation coverage).
- Per-listener: client port, replication port, multi-master mesh, each enabled separately.
- The io_uring path either gets TLS or a **named refusal** — `--tls` together with io_uring must not
  silently mean plaintext, and the process must not start. Receive is in userspace regardless of
  kTLS, so that path needs memory BIOs, which is a rewrite of the fast path that exists to be fast.
**Series D — the node links, and one question the client port does not have.**

- **On a node link TLS is always mutual, and there is no flag for less.** Both ends present a
  certificate and both verify; `--tls-replication` and `--tls-multi-master` therefore **require**
  `--tls-ca-file` and the process refuses to start without one. "Encrypt but do not check who the
  peer is" leaves the relay above open while looking like protection, which is the configuration
  this part exists to remove. mTLS is not a separate switch: on a node link it *is* what TLS is, and
  it costs nothing extra to configure because every node already has a certificate for its listener.
- **The accepting end has no name to expect, and that is the whole design question.** After
  `accept()` the only fact about the peer is its source address. Matching the certificate against
  *that* sounds strong and breaks on the first `DNS:`-only certificate, behind NAT and behind a
  proxy — turning "TLS on" into "the cluster does not form" — and would put a reverse DNS lookup in
  the accept path. Chain-only is sufficient **when the CA signs nothing but this cluster**, because
  every holder of a signed certificate then already has the cluster secret and the whole WAL; with a
  corporate CA the same sentence means every host in the organisation may become a replica. So the
  constraint is a mechanism rather than a sentence in a document: `--tls-peer-names` is an identity
  allowlist an accepted certificate must satisfy, empty means chain-only, and **the startup log says
  which of the two is in force** — the mistake part one paid for was a line claiming a guarantee
  nothing enforced (pitfall 112).
- **The allowlist check happens inside the handshake, not after it.** Four call sites across two
  loops, and by the time a caller could check, OpenSSL has already buffered the peer's decrypted
  bytes — so one forgotten `if` means a peer whose certificate we rejected feeding frames to the
  parser. `TlsChannel::continue_handshake()` fails the handshake instead, which makes the gate
  impossible to forget rather than merely present. Same move as part one putting the client gate
  before the `switch` instead of in every `case`.
- **The cluster secret and mTLS compose by AND.** Configured both means required both. OR would let
  a failure of either be covered silently by the other, so nothing could observe that one had
  stopped working. mTLS *is* an alternative in the sense that a cluster can run on it alone.
- **`TlsChannel` is one object per connection, held by `shared_ptr`, and neither choice is taste.**
  Repeating three fields and a handshake state machine in `ReplicaInfo` and `PeerConnection` would
  mean two implementations of the four `IoWant` combinations, which are the only hard thing here.
  And `replicas_` is a `std::vector` whose `push_back` moves its elements while a `PeerConnection`
  **changes key** after the handshake by erase-and-move, so a by-value member holding any pointer
  into itself dangles from the first reallocation — a defect that would surface as corrupt bytes on
  the sixth replica.
- **The state of the guarantee is readable on a live node**, because a guarantee whose state cannot
  be read is a guarantee on our word: `ob_mm_peers_tls_verified` against `ob_mm_peers_connected`,
  `ob_replicas_tls_verified` against `ob_replicas_connected`, and one INFO line per connection
  naming the certificate identity. A count and not a label — a label fed by a peer is an unbounded
  label set (part one, #31). Both halves of both pairs are exported, and both are recomputed on
  every pass of the loop that owns the connections: publishing a count only where it goes up leaves
  a dropped link counted, which is the shape #94 had on the mesh side.
- **The certificate identity lands in a field, which these links did not have.** A node's identity
  used to be its `node_id`, arriving in a handshake that authentication precedes, so the cluster form
  of a secret file carries no name at all. `ReplicaInfo::identity` and `PeerConnection::identity`
  hold the certificate's common name, sanitised on the way to a log because a CN is a string the peer
  chose (pitfall 117). Verification matches SANs and the identity is the CN — the log line prints
  both, so the two cannot be mistaken for each other.
- **Every write to a replica now goes through one queue, and that fixed a defect older than TLS.**
  `send_to_replica()` — the only sender in the catch-up path — called a `send_all()` helper on a
  **non-blocking** socket, so the first `EAGAIN` was read as a dead replica and dropped it mid
  catch-up; it reconnected, asked for the same range, and was dropped again. Measured before the
  change: **17 270 of 40 000 records delivered**, then `send_to_replica failed`. It was found by
  asking where that code would put `SSL_ERROR_WANT_WRITE`, which has the same answer as where it
  puts `EAGAIN`: nowhere. The ceiling still drops a replica that is not draining, at 16 MB of queued
  output instead of one socket buffer, and it resumes from its confirmed position. Test
  mutation-checked in both directions; the reconnects that remained were #93, now closed.
- **The test for it needed a measured number, not a generous one.** With neither side setting a
  buffer size, the loopback pair absorbed **2.6 MB** before the sender first saw `EAGAIN`, so a 2 MB
  version of that test passed against the defect. Shrinking the receiver's window to 4 kB reproduced
  it reliably and made the test take 49 seconds; 8 MB of WAL and no window tricks reproduce it in
  0.66 s. Pitfall 123 again: a probe that does not reproduce the shape says "no defect" in the same
  voice as one under which there is none.
- **The io_uring refusal stays broad, and the reason is coverage rather than epoll.** The node links
  have their own loops, but encrypted links on this transport have no runtime tests. The
  `io-uring-build` job (#108) proves compilation and linking only; it does not justify narrowing
  the refusal. Said in the refusal message rather than implied.
- Cost published with named hardware, a percentile, and the floor of the range.
- Six things easy to miss because they are not about cryptography — starting with the TLS output
  buffer being a *second* place the 64 MB send cap has to hold — are in
  `kiro-workspace/specs/wire-tls/requirements.md` §3.

- Effort: L | Impact: **Unblocks production adoption**

### 31. Access control
- Read-only users, per-symbol and per-exchange ACLs, admin-only commands (`FAILOVER`, `MIGRATE`)
- Effort: M | Impact: Multi-tenant deployments, compliance conversations

### 32. Configuration file support ✅
- `--config <path>`, flat `key = value` with `#` comments, CLI flags overriding file values.
  It was **thirty-seven** flags, not "twenty-plus".
- Config validation with clear error messages, and `--print-config` which prints the resolved
  configuration **with the provenance of each value** — default, file, or command line — and exits
  without opening a port.
- **The file is rewritten into arguments and handed to the existing parser.** A config key *is* a
  flag name by construction rather than through a mapping table, there is one type validation and
  one error message, and precedence falls out of argument order because the parser assigns. The
  alternative considered first — a declarative option table — would have made the config key a
  second vocabulary maintained beside the `arg == "--x"` branches, and the symptom of those
  diverging is a key an operator wrote that does nothing.
- Two static tests hold the two lists this needs against the parser's own source: the valid keys, and
  the flags that take no value. A mutation dropping one flag from the list fails the first.
- **The static test deleted a feature built on a false premise.** A `--no-failover-enabled` negation
  was added because `failover_enabled` defaults to true and a valueless flag cannot express false —
  except `--failover-enabled` *takes a value*, so `--failover-enabled false` had always worked. The
  belief came from reading the default rather than the parser's branch, and the list check disagreed
  with the list I had written myself.
- Found in that branch and fixed: `--failover-enabled` mapped anything unrecognised to **false**, so
  `--failover-enabled tru` silently disabled failover. Same class as #36. The accepted spellings are
  unchanged; what is new is that a value outside them is refused.
- Effort: S | Impact: Ops ergonomics, fewer misconfigurations — and it unblocks #33, which packages a
  default config and a systemd unit

### 33. Native packaging and cluster bootstrap ✅

**Both halves are done and merged (PR #66).** Packages, unit and operations documentation, plus
`scripts/bootstrap-cluster.sh` for a single host; the multi-host procedure is written rather than
scripted, for the reason given below. Spec: `kiro-workspace/specs/native-packaging/`.

- `.deb` and a static tarball with **byte-identical relative layouts**, holding the binary, headers,
  `/etc/orderbook/ob.conf`, the systemd unit, a man page and the docs. Dependencies resolved by
  `dpkg-shlibdeps` rather than hand-listed, because a hand-written list goes stale at the first new
  link and the symptom is an install that succeeds and a binary that will not start.
- `.rpm` **conditional on `rpmbuild` existing**, so a machine without it configures DEB and TGZ
  rather than failing every `cpack`. Built and inspected in CI, which is the only place it can be.
- The `package` job runs **on tags and on every pull request**, not on tags alone: a job first
  exercised on a tag is a job first exercised at the moment it matters most, and CPack failures are
  configuration failures that appear only when a generator runs.
- `ExecStart` is the binary plus `--config`, which is the whole payoff of #32 — before it, that line
  would have carried up to 37 flags and changing one setting would have meant editing a unit file.

**Three things this item asked for are theatre for this engine, and establishing that was the first
value.** Checked by grepping the sources rather than assumed:
  `LimitMEMLOCK` — nothing calls `mlock`, `MAP_LOCKED` or `MAP_HUGETLB`, so the limit would be
  raised for nothing, and in a unit file it reads as knowledge about the engine's requirements;
  huge pages — `MADV_HUGEPAGE` does not appear, so any tuning claim would be an unmeasured one;
  a default `CPUAffinity` — pinning to particular cores on an unknown machine is a mistake rather
  than a tuning. All three are absent with the reason written down, and a test holds two of them
  absent so nobody "fixes" it.

**Four defects, each from reading the artefact rather than from anything failing:**
  the Python wheel's install rule leaked into the .deb, because CPack with component install off
  takes every rule regardless of `CPACK_COMPONENTS_ALL` — a `SKBUILD` guard removes it from the
  build instead;
  `${CMAKE_INSTALL_SYSCONFDIR}` is relative, so the config landed in `/usr/etc/orderbook/ob.conf`
  while `conffiles` declared `/etc/orderbook/ob.conf`, which would have marked nothing and let the
  first upgrade silently revert every local edit;
  making that path absolute fixed the .deb and **made the archive generator try to create
  /etc/orderbook on the build host** — it failed only for want of privileges, and a root or
  container build would have written into the host's /etc while producing a package;
  and `--fsync-policy` **did not exist**. Writing `docs/operations.md` asked an operator to choose
  durability per storage device, and the server hardcoded `INTERVAL`, so the most consequential
  setting in a database was unreachable. Added, with an unrecognised value refused rather than read
  as the default.

**Part two: `scripts/bootstrap-cluster.sh`, single host, verified.** Three multi-master nodes plus
etcd as native processes, a configuration file per node in the same shape as `/etc/orderbook/ob.conf`,
and a wait for **every node seeing both peers as connected** rather than for the ports to open — a
node that is merely listening can accept a write and have nobody to send it to.

- **The SSH half is deliberately not a script**, and this is a scope decision rather than an
  omission: it could not be verified here — `sshd` is installed but inactive and no key is set up,
  and standing one up is a change to a developer's machine rather than a test. A deployment script
  nobody has run is worse than a procedure someone has read, so `docs/operations.md` carries the
  multi-host procedure with the two things that bite (`mm-replication-port` is a different port from
  the client one, and etcd must be reachable from *every* node). Verifying a script would need a
  second host, which is a decision with an owner.
- **Three defects in it, each from running it rather than reading it.** The readiness check counted
  lines containing `node_id` and always got 1, because that appears in the header and never in a peer
  row — counting `connected` is also the stronger condition, since #84 made `MM_PEERS` list
  connections still in their handshake. `stop` printed "stopped" and returned while all three nodes
  were still draining, so it now waits and escalates with a message rather than reporting a state it
  has not confirmed. And `case "$1"` under `set -u` failed with no argument.
- **And it found a defect in the engine.** Every metric on a multi-master node carried
  `node_role="standalone"`: `set_node_role()` is called only from `promote_to_primary()` and
  `demote_to_replica()`, neither of which a multi-master node runs. An operator scraping a three-node
  mesh saw three nodes each claiming to be alone — the one thing that label exists to distinguish —
  while `ROLE` on the wire correctly answered `MULTI_MASTER`. Two operator-facing signals
  disagreeing, and the metric was the wrong one. Fixed, with an integration test.
- Effort: M, done except the SSH script | Impact: Time-to-first-run drops from an hour to minutes,
  without a container layer between the engine and the hardware

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

### 37. CI hardening — sanitizers, the compiler matrix and coverage ✅

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

**The compiler half is closed, and it found two things.** `README.md` and `CLAUDE.md` both claim
GCC ≥ 12 / Clang ≥ 15, and nothing had ever checked the second half of that sentence. It was nearly
true: the whole tree built with Clang 18 after two fixes, and all 735 tests passed.

What Clang caught that GCC does not, both `-Wunused-but-set-variable` and both the same shape — a
value computed and never read:

- `base64_decode()` counted trailing `=` characters into a `padding` variable that nothing used,
  under a comment saying the padding was stripped. It was not; the loop checks those two characters
  as it reads them, which is correct, so the variable and the comment were both fiction.
- `handle_catchup_request()` kept a `file_offset` counter, advanced by every record streamed and read
  by nothing. The primary does not need one — it streams sequentially, and where the replica has got
  to comes back in the replica's ACKs, which is the only account of it worth trusting. A counter
  nobody reads is the mirror image of pitfall 15, where a field nobody wrote disabled the mechanism
  that read it.

One job rather than a GCC/Clang × Debug/Release matrix: `build-and-test` already covers GCC Debug
with the full suite and `release-build` covers GCC Release, so a matrix would spend most of its time
re-running what is already required. The missing combination was Clang, and `clang-build` covers both
its configurations — Debug with the full `ctest`, and Release, because `-O2` turns on diagnostics that
`-O0` never reaches.

Also fixed on the way in, before it could bite: `-Wno-maybe-uninitialized` was added unconditionally
to sanitizer trees. That flag does not exist in Clang, where an unknown `-Wno-*` is itself a
diagnostic — which `-Werror` would turn into the build failure the line exists to prevent. It is
guarded on `CMAKE_CXX_COMPILER_ID STREQUAL "GNU"` now.

**The coverage half is closed too, without a badge.** A `coverage` job builds with
`OB_ENABLE_COVERAGE`, runs the suite, and prints line, function and branch coverage into the job
summary with a per-file breakdown attached as an artifact. Nothing leaves the repository: every badge
on offer means sending reports from a public repository to a third-party service and holding an
account there, which is a decision with an owner rather than a task. It is answered below.

**The first honest number**, and it is instructive next to the one it replaced:

| | lines | functions | branches | source files measured |
|---|---|---|---|---|
| Before #83 | 59.0% of **2387** | 66.5% | 36.2% | **6** of 34 |
| After | **61.0% of 11352** | 72.5% | 33.4% | **33** of 34 |

The percentage barely moved while the denominator grew almost fivefold. That is the shape of the
defect: the old figure was not measuring less of the tree, it was measuring an unrepresentative sixth
of it and landing on a plausible number anyway.

Gated at a **58% line floor** — three points of slack, so ordinary churn does not trip it and a real
drop does. Branches are deliberately not gated: 33% is too far from anything to be a useful ratchet,
and a floor nobody can raise is a floor nobody respects. The job also gates three things that are not
percentages — the tree builds with coverage, the suite passes under it, and the instrumentation still
reaches the libraries, which is the part that failed silently for as long as the option existed.

**The badge question, answered: no badge.** There are exactly two ways to put a percentage on
this page and both were rejected on what they cost rather than on taste.

A **third-party service** — Codecov, Coveralls, or any of them — means this repository uploading its
reports to somebody else's account on every push, a token for that account living in a public
repository's CI, and a badge on our front page that goes red when *their* infrastructure has a bad
day. This tree has already paid that bill twice with CodeQL, where an infrastructure failure blocked
merges exactly as effectively as a real finding. The second way is **CI writing the number itself**,
through a shields.io endpoint backed by a gist or an orphan branch, which needs a workflow with
write access — the one thing `docs/github-security.md` refuses everywhere else, and for a number
rather than for a capability.

What a badge would add is one figure. What already asserts the floor is the `coverage` job being
**required**: it fails below its **58% line floor**, so the green CI badge at the top of `README.md`
cannot be green on a tree below that floor. A percentage badge would therefore say *less* that is checkable than the
badge already there, and more that is decorative.

So the number is on the page instead, **with its denominator and a citation of the run that measured
it** — 66.2% of 14,562 lines, from run 34953853236 on `d2929e4`. The denominator is not
presentation: the table above is the argument for it, because "59.0%" was true of a tree where the
instrumentation reached 6 of 34 source files, and a badge renders exactly the half of that sentence
which was not the defect.

**And the claim has a mechanism, because otherwise it is the kind that rots in one direction.**
`scripts/check_coverage_claim.py` reads `FLOOR` out of the workflow and holds every floor stated in
prose against it — `README.md`, `docs/github-security.md`, this page and `CLAUDE.md` — in the same
both-directions shape as `check_contexts.py`, because a mechanism guarding one document is how the
second one quietly learns a different number. It also holds the quoted figure to its own arithmetic:
the percentage has to follow from the covered and total counts printed beside it, and it has to sit
at or above the floor, since a citation of a run that would have failed the gate cannot be the run
we cite. What it cannot prove is that the figure is current or that the cited run measured the cited
tree; both are facts about a GitHub runner, which is why the run is named rather than summarised.

Using it found two things reading it would not have. Anchored on the word "floor", its first run
reported `README.md`'s **noise** floor for the comparative benchmark — "7% apart against a 21.2%
floor" — as a false claim about coverage: two unrelated floors on one page, the same shape as `rds`
matching "records". And skipping fenced blocks, which is right for a pasted job log, made the one
page whose job is to be audited the one page not audited: `docs/github-security.md` states the floor
inside its **Checklist**, fenced for monospace rather than because it quotes anything. Floors are
therefore scanned everywhere and measured figures only outside fences, which is safe because neither
floor shape is a shape the gate itself prints.

Ten mutations, each with the verdict it is meant to give, and nine of them are refusals: the floor
raised in the workflow only; changed in one document only; changed inside the fenced checklist; a
percentage that no longer follows from its counts; a figure below the floor; the denominator
dropped, leaving a bare percentage; `README.md` no longer stating the floor while other pages still
do — the front page carries the CI badge, so it is the page that has to say what the badge refuses;
the floor phrase removed from every document; and every phrasing of it removed, which is the row
that proves the check cannot pass by finding nothing. The tenth is the control, and it survives: the
prose around the number reworded.

**Correction, from #83.** The line below said "697 tests clean under ASan+UBSan and under TSan", and
this entry said so from the day the jobs went in. It was true of the test binaries and the server and
**not of the twenty-eight static libraries** — `add_compile_options()` only affects targets created
after the call, and these blocks sat past every one of them. UBSan needs instrumentation to see
anything, so undefined behaviour in library code was not being checked at all. #83 has the evidence,
the fix and what survives of the original claim.

- Effort: S | Impact: 697 tests clean under ASan+UBSan and under TSan, checked on every push — with
  the qualification above until #83 made it true of the whole tree. Two defects found by turning them
  on, one of them undefined behaviour on the hot path's data type

### 38. Fuzzing ✅

Three libFuzzer harnesses, one for each place that reads bytes this engine did not produce: the wire
command parser (`parse_command`, `parse_minsert`), the multi-master frame codec (`encode_frame`,
`parse_frames`) and WAL record deserialisation (`replay_v2`, `replay_after_checkpoint`). Opt-in
through `OB_BUILD_FUZZERS`, which refuses GCC and refuses to run without `OB_ENABLE_ASAN`, so an
incomplete request fails at configure time rather than producing a harness that explores nothing.
`-fsanitize=fuzzer-no-link` is added **before** the libraries are created, which is the whole of
#83: `add_compile_options()` only reaches targets defined after the call, and the sanitizer blocks
once sat below all twenty-eight of them.

`src/mm_framing.cpp` is a pure move. `encode_frame` and `parse_frames` lived in `multi_master.cpp`
next to the peer networking state machine, so linking them pulled in `Engine`, the snapshot
machinery and the whole manager — a fuzz driver for a byte parser would have been fuzzing static
initialisation. The manager links the same new library, so there is one implementation rather than a
copy compiled twice.

Forty-eight seeds are committed (25 commands, 10 frames, 13 WAL records) plus a command dictionary.
Measured on an Intel i3-7100U, Debug with ASan and UBSan, 60 s per harness: **1,609,034 / 1,671,098 /
136,285 executions** at **26,377 / 27,395 / 2,234 exec/s**, reaching 863 / 144 / 333 coverage points.
No findings. The WAL harness is an order of magnitude slower because every input is written to a file
and read back through a real `WALReplayer`, which is the point of it.

**The result worth reporting is not the green run — it is what a mutation table said about these
harnesses.** Nine defects were planted in the three parsers, with one control mutation that changes
a log string and must survive:

| mutation | first pass | after |
|---|---|---|
| `cmd_side_formatted_as_number` | killed by seed `binary_bytes` | killed |
| `wal_checkpoint_replay_from_start` | killed by seed `checkpoint_tail` | killed |
| `wal_context_payload_len_overstated` | killed by seed `checkpoint_tail` | killed |
| `wal_accepts_a_bad_checksum` | did not compile | killed by seed `bad_checksum` |
| `cmd_format_drops_event_time` | **survived** | killed by seed `event_time` |
| `mm_oversize_length_accepted` | **survived** | killed by seed `above_max_length` |
| `mm_payload_offset_off_by_one` | **survived** | killed by seed `above_max_length` |
| `mm_erases_consumed_bytes_on_error` | **survived** | killed by seed `valid_then_invalid` |
| `mm_encode_length_overstates` | — (encoder had no coverage) | killed by seed `above_max_length` |
| `CONTROL_log_wording_only` | survived, as it must | survived |

The frame harness had the most elegant-looking property of the three — splitting a stream into
chunks must not change which frames come out — and **caught none of the three defects planted in its
parser**. The corpus reached every one of those branches. What was missing was an oracle looking at
them, which is the difference between coverage and a test. Fragmentation invariance cannot see a
*systematic* decoding error: a parser that reports every payload one byte early shifts both sides of
the comparison equally. And the refusal of an over-long frame had no assertion at all — an over-long
payload can never be completed inside the fuzzer's input limit, so deleting the ceiling check merely
turns −1 into 0 while every other property still holds.

Four oracles closed it, each derived from the contract of the function under test rather than from a
second implementation of it, which requirement 1 of the spec rules out:

- a successful verdict never leaves an over-long frame pending, because the incomplete-frame break
  is only reachable *after* the ceiling check
- a refusal leaves the buffer untouched, which is what lets the caller drop the connection instead of
  resynchronising onto a frame boundary that never existed
- a payload survives `encode_frame` followed by `parse_frames`. **`encode_frame` had no coverage
  whatsoever** until this was added: the stream harness only ever called the decoder. Frame coverage
  went 144 → 177 points
- a command means the same thing after `format_command` and a reparse, compared field by field. The
  canonical-text comparison could not see a dropped field — removing the line that emits an INSERT's
  event time round-trips cleanly, because the field is then absent from both sides, which is the
  exact defect #105 existed to fix

After that, **nine of nine planted defects are killed by committed seeds**, deterministically rather
than by a lucky campaign, and the control still survives. Every kill names the seed that caught it,
which is the evidence that the corpus is doing the work.

`fuzz/verify_instrumentation.py` replaces reading CMakeLists.txt with reading the compiler's own
command lines out of `compile_commands.json`, because what CMake says and what the compiler received
are two different claims. Its own first version was blind: it tested for the substring
`-fsanitize=undefined`, which never matches, because clang is handed `-fsanitize=address,undefined`
as a single argument — it reported every parser as uninstrumented in a fully instrumented build. So
sanitizer lists are parsed rather than searched, and absence is a failure: a missing file, an empty
database or an empty corpus each fail loudly instead of finding no problems in nothing.

**Scope, stated because each limit was measured rather than assumed:**

- these are parser harnesses, not server tests. Nothing opens a socket, starts etcd, binds a port or
  touches another process's data directory
- over-acceptance is not what the command harness measures. A parser that accepts a trailing token
  still round-trips cleanly, because `format_command` does not emit the token it ignored. The
  mechanism that refuses those is the arity table indexed by `CommandType` (#107)
- `parse_frames` cannot be shown to *accept* an over-long frame, only to refuse one: completing a
  64 MiB payload is three orders of magnitude past the input limit. The oracle is therefore indirect
- an unknown WAL header version is read as legacy rather than refused. Measured on the
  `unknown_version` seed: `version = 255` takes the 24-byte path, and what stops a future format
  from being misread into the engine is the checksum failing, not a version check
- the WAL harness drives one file; rotation and the manifest stay with the C++ suite
- **no OSS-Fuzz.** Submitting is an option and was never a condition of finishing this item; it
  would mean sending builds of a public repository to a third-party service, which is the same kind
  of decision as the coverage badge in #37

The `fuzz` job and `{"context": "fuzz"}` in `.github/rulesets/master.json` enter in one PR, because
`check_contexts.py` rejects a produced but unrequired context. The live ruleset is applied **after
merge** and read back. That brings it to fourteen contexts — and the sentence in
`docs/github-security.md` that states the number is no longer maintained by hand: the drift checker
derives it from the ruleset and fails if the prose disagrees or stops making the claim. It had been
wrong twice already, "eleven" against twelve and "twelve" against thirteen, and neither time was
anybody careless: every change added a context and no change recounted the sentence.

- Effort: M | Impact: finds the class of bug property tests miss, and the mutation table is the part
  that makes the harnesses worth trusting

### 39. Reproducible comparative benchmarks ✅

**Both parts are done.** Part one (PR #70) built the harness, the dataset, the resolution
measurement and our own adapter. Part two installed ClickHouse and TimescaleDB natively, wrote the
three competitor adapters, ran the multi-system checkpoint and published the table — and it found
more in our own code than in theirs. Spec: `kiro-workspace/specs/reproducible-benchmarks/`.

**The first published answer was that this engine loses all four comparable workloads**, and the
reasons were worth more than the numbers. i3-7100U, Release, 200 000 rows, twelve rounds, control
floor **15.7%** (`results/2026-09-11-a8b19196.md`):

| system | ingest (rows/s) | 4000-row query | not like-for-like because |
|---|---|---|---|
| orderbook | 78,151 | 10.09 ms (8.98–16.42) | one `MINSERT` round trip per book update |
| ClickHouse 26.8.2.7 | 433,779 | 5.96 ms (5.10–7.69) | the whole CSV in one request |
| TimescaleDB 2.30.0 / PG 16.15 | 106,874 | 6.06 ms (5.44–14.85) | `\copy` of the whole CSV, `timescaledb-tune` applied |
| kdb+ | NOT MEASURED | NOT MEASURED | a vendor registration, and a licence question about publishing its free edition's numbers |

**Recomputed on 12 September after #105 closed, by one run rather than by editing numbers** — and
this is the first table that compares the *same question*, because until then the engine could not
be given event time at all and the query column excluded the time column
(`results/2026-09-12-ece487a1.md`, control floor **21.2%**):

| system | ingest (rows/s) | time-range query (4000 rows) | verdict against orderbook |
|---|---|---|---|
| orderbook | 66,072 | 9.32 ms (8.46–13.28) | — |
| ClickHouse 26.8.2.7 | 428,842 | 5.26 ms (4.78–6.85) | **loses both**: 84.6% and 43.6% apart |
| TimescaleDB 2.30.0 / PG 16.15 | 116,438 | 8.67 ms (5.60–10.54) | **loses ingest** (43.3%); the query is **7% apart against a 21.2% floor**, so it is reported as indistinguishable rather than as a win |
| kdb+ | NOT MEASURED | NOT MEASURED | unchanged: a vendor registration and a licence question |

So the honest summary changed from "loses all four" to "**loses three, and one is inside the
floor**" — and the reason is that the workload became comparable, not that the engine got faster.
The ingest column reads 66,072 against the earlier 78,151, which is **15% apart against this run's
own 21.2% floor**: not separable from noise, and the harness's own note is why it is not presented
as a change — two consecutive runs of this table gave 9.69 ms and 10.97 ms for the same query.

The remaining loss is a limit of the **protocol** rather than of the storage engine, which is the
useful finding: the same engine ingests **446,219 updates/s in process** here against **4,012
through the wire** — a factor of 111, all of it the round trip.

**That last sentence is wrong in both of its halves, and the correction belongs here rather than in
place of it, because the run happened.** The two figures are in different units — the in-process
benchmark applied **one** level per call and the harness sends **twenty** per round trip — so a
factor of twenty of the 111 was the word "updates" meaning two things. And "all of it the round
trip" is false: measured on the aarch64 box at equal volume and with the client held fixed, the
engine's wall-clock ingest over a socket is within **about 4%** of its own in-process figure, the
round trip costs about eight times the storage path's CPU per level and almost nothing in
throughput, and the largest single term in that column is the harness's own Python client. The
measurements are in [`../benchmarks/on-a-bigger-machine.md`](../benchmarks/on-a-bigger-machine.md).

**Five findings came out of running it, and four of them are about our own code.**

**#105, filed rather than patched: `insert(timestamp_ns=…)` is silently dropped over TCP.** The
Python client accepts it, computes it, uses it in the embedded branch and never mentions it in the
TCP or pool branches, because `INSERT` and `MINSERT` carry no timestamp field. Measured: rows loaded
with the dataset's timestamps came back stamped `1789153200757060030`, and the dataset's own span
selected **0 of 400 rows** while the same load into ClickHouse and TimescaleDB selected 400. Four
integration call sites pass that argument and none asserts on it, which is the strongest evidence
that it reads like the right thing to do.

**Part one's published noise floor was measured on a query that returned nothing**, and the same
three lines held a second defect that hid the first. `query_time_range()` selected on timestamps the
server had never stored, so it returned an empty list — and its row mapping read `r.timestamp` and
`r.size`, which do not exist (`timestamp_ns`, `quantity`). The empty result hid the wrong attribute;
the wrong attribute would have exposed the empty result the moment it stopped being empty. The floor
published on 3 September (0.2341) therefore describes the latency of a query matching no rows.

**Two thirds of our engine's measured query time was our own Python client.** The first four-system
run reported 18.2 ms for 4000 rows against ClickHouse's 6.3, and the honest-looking loss was
`OrderbookEngine.query()` building 4000 `OrderbookRow` dataclasses: measured, p50 **15.5 ms** through
the client against **5.3 ms** for the same bytes parsed into tuples, which is what every competitor's
adapter does. The mirror image was on the other side and found first — a fresh `clickhouse-client`
costs **80 ms** per query and a fresh `psql` 40-60 ms, so the first versions of those adapters
charged the competitors forty and ten times the work they were asked to do. **An adapter must not
add work the other adapters do not pay**, in either direction.

**Two of the three competitors were already running on this machine, in containers** — ClickHouse on
58123 for the flagship product's test engines and PostgreSQL on 5432 for the landing page. A run that
reached either would have measured a container, which requirement 1.2 forbids, and nothing in the
numbers would have looked wrong. Each adapter now refuses the container's port **by name before
asking the server anything**, and each records the endpoint it used: the native ClickHouse answers
26.8.x and the container answers 24.8.x, so which server replied is a measurement rather than an
assumption. The second consequence is where PostgreSQL ended up: `pg_createcluster` took **5433**
because 5432 was busy.

**`classify()` and `verdict_for()` had no callers outside the tests** until this run — written in
part one for a comparison that could not happen with one system, which is the shape #104 is about.
And the first thing the multi-system driver did was trip the package's own static test: my summary
line spelled "faster" in `run.py`, and only `resolution.classify()` may say that. The words are
named constants in `resolution` now, so a caller counting verdicts refers to them instead of
repeating the claim.

**The checkpoint's own value was in reading the output** (task 7.4): the first four-system table
printed `75131.7815` rows per second, carried a 700-character tuning list inside a table cell, put a
200-character refusal where a number goes, and filed "every figure includes 4.8 ms of Python-side
parsing" under *What this engine cannot do* — where a property of the measurement reads as a
deficiency of the engine. All four are fixed in `report.py`, which now formats by unit, moves prose
below the table and keeps measurement notes in their own section.

Also measured, and it is the floor's own answer to how much this machine can be trusted: five runs
of the same table in one afternoon gave floors of **0.05, 0.07, 0.15, 0.16 and 0.35**. Twelve rounds
rather than six is the documented lever, and the published run uses it.

**The centre of it turned out to be a refusal rather than a feature.** This machine does not resolve
percentages, and now the harness says so with a number: it measures the same system against itself,
interleaved, and reports anything below that floor as `INDISTINGUISHABLE ON THIS HARDWARE`. Measured
floors across the first runs: **0.68** with one cold call included, **0.52** from a single scheduler
hiccup in eight rounds, and **0.12–0.23** once a warm-up and one discarded outlier were added — every
control ratio published beside them. The word "faster" exists in one function, held there by a static
test over the package.

**Three things came from running it that reading it had not shown**, and each is recorded in the
code: the generator gave every row its own timestamp, which a batched load cannot preserve because
the client takes one timestamp per call; hardware detection reported "unknown" for an ordinary NVMe
because `lsblk -d` on an LVM-over-LUKS mount source returns the mapper device; and my own glue passed
a cached number to `measure()`, which returns a floor of exactly 0.0 — so `classify()` called a 1%
difference faster. A control run whose ratios are all exactly 1.0 is now refused.

**It also produced #90.** The harness has to record the version of every system it measures, reads
ClickHouse's from `SELECT version()`, and ours could not be asked at all — so the results file said
"unreported (the server has no way to report its version)". That sentence was the argument for the
item, and after it merged the adapter reads `STATUS` instead. The first re-run *still* said
unreported, correctly: `build-release/` predated #90, so the measured binary genuinely had no version
field. A literal would have printed a version the binary did not have.
What kdb+ needs is not code: a vendor registration for the binary and its licence, and a reading of
that licence about publishing numbers from the free edition. `systems/kdb.py` is written to the same
interface as the others and refuses early with both reasons, so the day a licence exists this is a
flag rather than a file.

- Effort: L | Impact: Turns a performance claim into something a reader can verify on their own
  hardware in an afternoon — and it turned four of them into corrections

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

### 45. Streaming subscriptions ✅
- `SUBSCRIBE 'SYM'.'EXCH'` pushing updates to the client
- Backpressure policy per subscriber, slow-consumer disconnect
- **The claim has been corrected in the meantime, because it was the half that was free.** The
  README listed "streaming subscriptions" among the features, and a reader takes a feature list on a
  wire-protocol project as describing the wire protocol. What exists: the query language parses
  `SUBSCRIBE`, `Engine::subscribe()` and `ob_subscribe()` deliver rows to a callback, and
  `notify_subscribers()` is called on every write — so an **embedded** consumer really does stream.
  What does not exist: any way to ask for it over TCP. `CommandType` has no `SUBSCRIBE`, and
  `QueryEngine::execute()` says so outright ("SUBSCRIBE via execute() is not supported"). A network
  client polls. The README now says exactly that.
- **Done.** Spec: `kiro-workspace/specs/streaming-subscriptions/`.
  `SUBSCRIBE 'SYM'.'EXCH'` and `UNSUBSCRIBE [id]` are wire commands; the server pushes matching rows
  as `PUSH <id>` with the same seven columns as a query row. Bounded queue per subscriber
  (`--max-subscriber-queue-bytes`, 8 MB ≈ 140 000 rows), overflow closes the session, per-session
  limit on subscriptions, five metrics, and `subscribe()` / `poll()` in the Python client.
- **The subscription list had been a data race the whole time.** A bare `std::vector` shared between
  the epoll loop (`apply_delta`) and `MultiMasterManager::io_loop` (`apply_remote_delta`), latent
  only because the sole callers of `ob_subscribe()` were single-threaded tests. Now a `shared_mutex`
  with deferred removal, and the callback runs with **no lock held** — the first fix invoked it under
  the shared lock, which deadlocks, because marking an entry dead takes the exclusive lock and
  `std::shared_mutex` is not recursive.
- **Cost on the write path with nobody subscribed: none measurable.** `BM_IngestionThroughput`,
  i3-7100U, Release, six interleaved rounds: master 2600.6 ns/op (cv 0.99%), branch 2560.4 ns/op
  (cv 2.60%), median of per-round ratios 0.9848 over a 0.967-1.025 range. The difference is inside
  this machine's noise and is not claimed as a speed-up.
  Getting there cost a **measured 76% regression** first: the batch was collected into a
  `SnapshotRow subscriber_rows[MAX_LEVELS]` declared in `apply_delta`, and the declaration is
  unconditional while the type is *not* trivially default constructible — the `{}` on its three
  padding members gives it a real default constructor, so every write ran a thousand of them and
  touched 48 KB of stack whether anyone was subscribed or not. 2559 → 4511 ns/op, 6/6 rounds.
- **Closed by #85.** Task 6.3 was blocked on it: running the subscription module under TSan reported
  a pre-existing race in the WAL position accessors, which that module was simply the first thing to
  reach. With #85 fixed, `sanitizers-integration (tsan)` runs the whole battery — this module
  included.
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

### 54. Chaos and fault injection ✅
- Network partitions between multi-master peers, packet loss and reorder, disk-full, fsync failure,
  clock skew (HLC correctness under skew is untested), etcd unavailability
- Effort: L | Impact: The failure modes that lose data in production

**Four stages are in, and the item stays open with what is left named.** Stage A injects storage
faults with an `LD_PRELOAD` shim and found #112, #113 and #114 in its first run; stage B takes the
coordinator away and found #115 and #116; stage C breaks mesh links through a proxy and found #117
and #118; stage D skews clocks and found #119, #120 and the question that is #121.

**The last clause of stage C is closed, and the number is the interesting part.** "A peer that
stopped reading is dropped rather than buffered for ever" was recorded as blocked by **volume, not
visibility**, and that was right about the mechanism and pessimistic about the cost.
`ob_mm_peer_send_buf_bytes` is the engine's own queue and cannot grow until `send()` returns
`EAGAIN`, which needs the **sender's** socket buffer full — the engine sets no `SO_SNDBUF`, so that
is `tcp_wmem`'s maximum, and narrowing the *receiver's* buffer does not shorten it because TCP holds
unsent data on the sender's side. Measured: **3 015 750 bytes** of accepted writes before the queue
passed a 256 kB ceiling, so about 2.9 MB sits where no gauge can see it. The test costs **9.0 s**,
because a 1000-level `MINSERT` is one ~24 kB mesh frame and three megabytes is ~125 round trips. Its
control is a **draining** link: the same writes leave the drop counter at zero and the queue at 0
bytes, without which "the partition caused the drop" would be a claim about volume. Convergence
afterwards is read from `ob_mm_replication_lag_records` returning to zero rather than by comparing
half a million rows.

A fixture went with it: `narrow_proxied_mesh` put a 4 kB receive buffer on the accepted sockets on
the expectation that it would shorten that wait, the same session measured that it does not, and it
was left behind **with no users** — a fixture built for a test nobody then wrote, which is the shape
of a knob nothing turns.

`docs/operations.md` now has the mesh-link section that stage C left for last, because writing it
earlier would have meant describing the two numbers #117 and #118 were about to change: for the
first ~2.9 MB the engine reports nothing, then the records lag moves, then the peer is dropped.

**The last three are in, and two of them produced something.** The wire-level HLC test
(`tests/test_mm_wire_clock.cpp`) frames a DELTA record whose timestamp is an hour ahead and requires
it to become this node's clock and **stay** — which is what turns "those ten lines parse an HLC and
hand it on" from a claim about code into a measurement. It is paired with the other half, that the
record carrying the skew is still **applied**: a node that dropped it would keep its clock and lose a
write, which is the trade #121 would have to make explicit.

"A short write, then an error" was a hypothesis and became **#126**, since fixed: the write that is
cut is refused, the writes after it were acknowledged, and **none of them survived a restart** —
measured 2 of 2, because `WALReplayer::replay()` returned at the first CRC mismatch rather than
breaking out of one file. That was the central durability sentence in `docs/cli.md` with an exception
nobody could see from outside. Getting the number right needed the flush tick an hour out and a
`SIGKILL` rather than a stop: the first run read 1 of 2 because a tick had written a checkpoint and
rescued a row, which is timing rather than the WAL.

And the reproduction guide is `tests/fault/README.md`: the four failures worth reproducing, each as
one command line plus its environment, with the log that says the injection happened — because an
injector that matched nothing looks exactly like code that survives the fault.

**Mutations for the closing three: four for C4 and five for D3 and the injector, each with the
verdict it had to give.** For C4: the ceiling never firing, the ceiling ignoring its flag and using
the default, the drop going uncounted, and a reworded warning that survives. For D3: the wire's HLC
replaced by a zero one, and a clock that ignores the remote timestamp — both killed by the wire test,
which is what says it measures the path rather than the class. Two more are about the *instrument*,
because a composite fault that only does one half proves nothing: letting the torn record's remainder
through, and a short write that reports bytes it did not write. One did not build on the first
attempt (`false` in a C file with no `<stdbool.h>`), so the **mutation** was transformed rather than
the code.

**#54 is therefore closed.** What it leaves behind is the list of items it found: #112, #113, #114
from stage A, #115 and #116 from stage B, #117 and #118 from stage C, #119 and #120 from stage D,
#121 as a question for a maintainer, and #126 from its own last measurement.

### 55. Multi-node cluster tests in CI ✅
- Three native nodes plus etcd started by a script, multi-master convergence and failover verified
  on every PR

Two jobs, added in two steps and for two different reasons.

**`sanitizers-integration (tsan)`** came first, as a side effect of #80: it installs etcd, builds the
server under ThreadSanitizer and runs the integration battery against real clusters, failing on any
sanitizer report. It was the first CI job to run the pytest suite at all.

*Its scope used to be three multi-master modules, and the stated reason for that was a hypothesis
that turned out to be false.* The note here said the modules that kill nodes were excluded "because
their fixtures wait on timings that instrumentation makes unreliable". When #85 finally ran the whole
battery under TSan, all nineteen modules passed with zero reports — including `test_failover.py`,
`test_failover_dead_state.py` and `test_crash_recovery.py`, the three that `SIGKILL` a server. The
narrow scope had cost something concrete: none of those modules starts the failover monitor, so the
WAL position race in `publish_position_if_due()` was never on a TSan build, and it sat there for
months. **A comment justifying a gap in coverage is a hypothesis** — the same lesson as #80 itself,
and the third time this repository has paid for it.

**`integration-tests`** is the rest: the whole suite against a plain build, and therefore the half
that gates what the narrow job cannot — failover, crash recovery under `SIGKILL`, and the
position-lease invariant that #72 and #74 turned on. Kept as its own job rather than appended to
`build-and-test`, because that job is the C++ suite and a failure here means something different.

Both are required in the ruleset. The cost is stated plainly in `docs/github-security.md` next to
CodeQL's, because it is the same cost: **an infrastructure failure blocks merges exactly as
effectively as a real finding**, and here the infrastructure is etcd plus a live cluster. The
failover module is also the one place in the suite with a wall-clock dependency — it flaked once
locally when a benchmark was saturating a core in parallel — so if it turns out to flake on shared
runners, the fix is a measured timeout, not deletion.

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

### 76. Multi-master bootstrap is a stub, and its flag has no way out ✅

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
WAL. That is survivable and is roughly what #67 describes. The trap is the flag: `INSERT` and
`MINSERT` answer `ERR BOOTSTRAPPING` while `is_bootstrapping()` is true, so **the day someone
wires `start_bootstrap()` into a real path, that node stops accepting writes for the rest of its
life** — the same shape as #73, waiting in a state machine with an entrance and no exit.
*(This paragraph named `DELETE` as a third command until #76 was implemented and the guards were
audited: there is no `DELETE` in the client protocol. The two that exist were the two that mattered,
and — see below — they were also the only two that checked the multi-master flag at all.)*

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

**Closed by extending the multi-master protocol**, and the deciding argument was correctness rather
than effort. The other option on the table was to run the replication server on MM nodes too and let
a joiner act as a replica for the length of the bootstrap — far less new code, reusing a tested path.
It was rejected because an MM node's WAL holds records from several origins, each numbered by the
origin that minted it, while the primary→replica receive path applies records **without sequence
dedup and without LWW conflict resolution** — a replica has one source and needs neither. Serving
that protocol from an MM node puts two protocols with different correctness rules over one WAL,
where a single misconfiguration duplicates rows. Two listeners on every node was the smaller half of
the objection.

The extension turned out cheaper than this entry assumed, because **no new framing was needed**.
Frames after the handshake are untagged: each carries a `WALRecordV2` header whose `record_type` is
the only discriminator, `handle_frame()` branches on it, and an unknown value falls through to
`handle_remote_record()`, which skips it and stays connected. That is the door the version vector
(type 7) went through, and five snapshot messages went through it too — so a node running the older
build stays in the mesh. The numbers live in a reserved range from 200 up, documented next to the
`WAL_RECORD_*` constants, so adding a ninth WAL record type can never collide with a wire message.

What the transfer does:

- `SNAPSHOT_REQUEST` → the sender creates a snapshot **and captures its version vector and held set
  in the same critical section as the flush** (`create_snapshot_with_sequence_state()`). That
  boundary is the whole correctness argument and is one line wide: a vector exported afterwards can
  claim a number that landed after the flush and is therefore in no snapshot file, so the receiver
  would declare a frontier over a hole; exported before, it claims less than the files hold, and a
  redelivery of the difference appends those rows a second time. The held set closes what remains
  exactly — the numbers above the frontier that the sender does hold are listed, so a redelivery of
  any of them meets `has_seen()`.
- `SNAPSHOT_BEGIN` announces the metadata blob; the blob itself (manifest ++ vector ++ held) is
  streamed through the same chunk mechanism as the files, because a manifest for a few thousand
  segments passes 64 kB on its own and a frame cannot carry more (#78). Metadata carried in one
  frame would have imposed a store-size limit on the very case bootstrap exists for.
- `SNAPSHOT_CHUNK` frames are pushed only while the peer's send buffer is below a low watermark, and
  resume from the `EPOLLOUT` branch as the socket drains. So live deltas enqueued between chunks go
  out promptly, and the buffer never reaches the size that drops the peer for not draining (#69).
- `SNAPSHOT_END` carries nothing. Every byte is already covered by a per-file CRC from the manifest
  and by the metadata CRC from BEGIN; a third checksum could only fail where one of those already
  has.
- `SNAPSHOT_ABORT` carries a bounded, sanitised reason, so a peer cannot make us log an arbitrary
  amount of text or break a log line.

The receiver stages to a scratch directory, validates **every** path in the manifest before writing
a byte and refuses the whole snapshot if one is unsafe, checks each file's CRC as it completes, and
installs only after a pre-flight pass confirms every staged file exists at its manifest size. Then
`load_snapshot()`, then `adopt_snapshot_sequence_state()` — which **resets** the tracker before
importing, because `import_own_vector()` only raises, so a frontier from the discarded contents
would otherwise survive the discard and claim rows that are no longer on disk.

Three things this fixed that were not on the list when it started:

- **The file index meant different things on the two sides.** A chunk names its file by index into
  the manifest, and `to_json()` sorts entries by path for deterministic output — so index 0 on the
  sender was a different file from index 0 on the receiver, and the first chunk was rejected for
  exceeding a size belonging to another file. The sender now adopts the order it is about to
  transmit. Found by the first end-to-end test, on its first run.
- **`Engine::is_bootstrapping()` did not know about multi-master.** It consulted `repl_client_`,
  which does not exist in MM mode, so `SELECT` and `FLUSH` passed straight through an MM bootstrap
  while `INSERT` and `MINSERT` were stopped by their own duplicated check. `FLUSH` is the one that
  mattered: it writes segments into the directory an install is about to rename files into. One
  condition now covers all five commands, and the duplicated blocks — and the second spelling of
  the same error — are gone.
- **A record applied during bootstrap is worse than a record dropped.** `load_snapshot()` discards
  the in-memory buffers, so a delta applied mid-transfer can vanish while its number stays in the
  tracker: a frontier claiming a row that does not exist, which no later catch-up fills because
  nobody knows it is missing. Remote deltas are now refused **without being recorded as seen**, so
  the next vector exchange brings them back.

Triggered from a real path, which is what this entry was really about: after the handshake and the
peer's vector, a node that **holds nothing at all** asks that peer for a snapshot. Deliberately
stricter than "we are behind" — installing a snapshot discards local contents, and a node that wipes
its own rows because a peer looked further ahead is a worse failure than any amount of redundant
traffic. Repairing a node that *does* hold data stays with #57, where `trigger_snapshot_repair()`
already waits and where "discard and accept" has an owner.

Verified: 20 unit tests in `tests/test_mm_snapshot.cpp` (codecs, every refusal, a whole transfer
driven through a socketpair, and who may ask), plus an integration test that adds a fourth node to a
running three-node mesh — a case the harness could not express before, which is a large part of why
#67 went untested for so long. Six of the seven fixes were confirmed by disabling them and watching
the test fail. The seventh is recorded honestly below.

**One guard is redundant by design, and the first attempt to test it was measuring something else.**
`SNAPSHOT_END` checks that every file arrived, and `install_snapshot_files()` checks the same thing
again from the staging directory. Disabling only the first left the test passing — install caught it
— which is pitfall 37 exactly. The test now asserts the invariant rather than the branch: after an
incomplete transfer, **no `.col` file may appear in the data directory at all**. Disabling both
guards together fails it. Worth stating plainly: a half-installed snapshot leaves in-memory state
empty while the directory already holds another node's segments, so `holds_no_data()` — the first
thing the test asserted — reads "clean" for a directory that is anything but.

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

### 80. The integration suite had never been run under a sanitizer, and it reported a deadlock ✅

Found by pointing the existing pytest suite at a TSan build of `ob_tcp_server` — thirteen seconds of
`test_mm_convergence.py`, nine tests, three nodes. The CI sanitizer job (#37) runs `ctest` only, and
unit tests never start a server process with real clients, real peers and a signal-driven shutdown.
So this whole class was outside the reach of a job that exists to find exactly it.

**A lock-order inversion, reported on all three nodes.** TSan names
`execute_command()` (`tcp_server.cpp:954`) on one side and an internal thread on the other:

```
Cycle in lock order graph: M0 => M1 => M0
  Mutex M1 acquired here while holding mutex M0 in main thread:   TcpServer::run()
  Mutex M0 acquired here while holding mutex M1 in thread T5
```

The two mutexes are not named in the stacks — the intervening frames are inlined at `-O1` — but the
two code paths that acquire them in opposite orders are unambiguous:

- **Client write:** `Engine::apply_delta_mm()` takes `Engine::mtx_` and, still holding it, calls
  `MultiMasterManager::broadcast_local()`, which takes `MM::mtx_`.
- **Received delta:** `io_loop()` holds `MM::mtx_` across the whole peer-fd branch — including
  `process_recv_buf() → handle_frame() → handle_remote_record()` — and calls
  `Engine::apply_remote_delta()`, which takes `Engine::mtx_`.

So `Engine::mtx_ → MM::mtx_` on one thread and `MM::mtx_ → Engine::mtx_` on the other. Both are
ordinary operations on **every** multi-master node: one accepts a client write while the other
applies a peer's record. The window is microseconds, which is why the suite has never hung — a
cluster under sustained bidirectional load is a different matter.

Note what did *not* find it. `reconcile_with_peers()` carries a comment about this exact cycle
("does not touch the engine mutex while holding MM's — the cycle that deadlocked the flush thread
once already"), so the hazard was known in one place and not audited elsewhere. Pitfall 20 in
`CLAUDE.md` describes the same shape from the previous occurrence.

**Seventeen data races, all on the shutdown path.** `TcpServer::shutdown()` runs on the
signal-handling thread and reads members that `main` and `run()` are still writing:
`src/tcp_server.cpp:1062` (9 reports), `src/metrics_server.cpp:62` (3), `src/tcp_server.cpp:1009`
(2), plus three on the `metrics_server_` unique_ptr itself. Lower severity — the process is
exiting — but a crash while shutting down is a crash, and it makes CI flaky for reasons nobody can
reproduce locally.

Repeatable, and now run in CI. `OB_SERVER_BINARY` lets the whole suite run against any build:

```bash
sudo sysctl -w vm.mmap_rnd_bits=28
cmake --build build-tsan -j$(nproc) --target ob_tcp_server
PYTHONPATH=$PWD/python OB_INTEGRATION_TESTS=1 \
  OB_SERVER_BINARY=$PWD/build-tsan/ob_tcp_server \
  TSAN_OPTIONS="detect_deadlocks=1 second_deadlock_stack=1 halt_on_error=0 log_path=/tmp/tsan" \
  pytest tests/integration/ -q
```

Every site matters, not just the one TSan happened to name. On the io-loop side the calls into the
engine under `MM::mtx_` are `apply_remote_delta()`, `holds_no_data()` and — added by #76 —
`create_snapshot_with_sequence_state()`, which takes `Engine::flush_mtx_` **before** `Engine::mtx_`
and so widens the cycle by one mutex. `export_version_vector()` is deliberately not one of them: it
reads a cache under its own mutex, which is what that comment in `reconcile_with_peers()` is about.

**Fixed by removing one direction of the cycle entirely.** Of the two orders, `Engine::mtx_ →
MM::mtx_` was the smaller side — three call sites, all of which can gather what they need without
the lock — so it is gone, and `MM::mtx_ → Engine::mtx_` is now the only order in the tree:

- `apply_delta_mm()` broadcasts **after** releasing `mtx_`, as the last step rather than the
  fifth. The cost is that two concurrent writers can reach the wire in an order that differs from
  their WAL order; nothing on the receiving side reads arrival order as meaning anything, because
  catch-up already over-delivers out of order on purpose, records above the frontier are held
  rather than rejected, and conflicts are resolved by HLC.
- `stats()` asks multi-master for peer state **before** taking `mtx_` and copies it in afterwards.
  This was the second way in, and the more embarrassing one: `STATUS` and every `/metrics` scrape
  came through it.
- `open()` and `close()` call `mm_mgr_->start()`/`stop()` without holding `mtx_`, so they were
  never part of it. Checked rather than assumed.

The shutdown races went the same way as #41, and for the same reason: **the thread that owns a
descriptor is the thread that closes it.** `TcpServer::shutdown()` now only raises `draining_`; the
epoll loop sees it within its 100 ms wait and closes the listen socket and the metrics server
itself. `MetricsServer::stop()` joins its thread before closing anything, having previously closed
`listen_fd_` "to unblock epoll_wait" — which does not unblock it, and left a descriptor number the
loop was still comparing against events and the kernel was free to reassign.

Measured before and after, on the same command: **3 lock-order inversions and 17 data races → zero**,
with `test_mm_convergence.py`, `test_mm_stats.py`, `test_mm_snapshot_bootstrap.py` and
`test_metrics.py` all clean under TSan.

And the door is now open in CI: a `sanitizers-integration (tsan)` job builds the server under TSan
and runs the multi-master modules against it with the deadlock detector on, failing on **any** report
— TSan runs with `halt_on_error=0` so that one finding does not hide the next, which means pytest can
pass while the logs are full. The modules that kill nodes are left out for now: their fixtures wait
on timeouts that instrumentation makes unreliable, and a flaky required check teaches people to
ignore checks.

- Effort: M | Impact: A multi-master node under bidirectional load could deadlock, taking client
  writes and peer replication down together. P0 by consequence, never observed in the wild

### 176. A mesh node that holds 8 192 segments or more cannot send a snapshot, so no peer can join it **P1**

**Found reading the mesh's snapshot sender for #165 part 2b, and not yet measured.** A mesh
snapshot names each file by a 16-bit index in its chunk header, and 0xFFFF is the metadata blob's,
so `begin_snapshot_send()` refuses a manifest of 65 535 files or more — loudly, `too_many_files`,
counted in `ob_mm_snapshot_failed_total`. A segment is eight files. So a node holding 8 192
segments cannot bootstrap a peer that joins the mesh, or one too far behind to catch up, and that
is not a store the mesh reaches only by neglect: part 2b merges a symbol's segments, but not below
one a symbol an hour, so **8 192 instruments** — an options chain — is past the limit whatever
merging does, and before part 2b a node writing 256 symbols at the soak's rate reached it in about
five minutes. The replication link's snapshot names each file by its path, and has no such limit.

A fix widens the index — a 32-bit one in a versioned frame — or sends the file list in parts, and
its test is a mesh node past the limit and a peer that joins it, which would today be a strict
xfail beside #60 and #61.

- Effort: M | Impact: a mesh cannot take a new peer once one node holds a few thousand segments —
  a few thousand instruments, or a few hours of a few hundred

### 175. Sharding by symbol has no control plane: no shard writes itself or the shard map to etcd, and neither client finds a shard **P1**

**Found reading the shard coordinator for #172, then measured** against a native etcd, with two
servers started as `--shard-id s0` and `s1` and the same `--coordinator-endpoints`. Each logs
`Registered shard=… status=active`, and etcd then holds `/ob/leader` and the two `/ob/nodes/…` keys
of the failover registration and nothing of sharding: no `shard_map`, no shard node.
`ShardCoordinator::register_shard()` builds the node's JSON and calls `publish_wal_position(0, 0)`
"to verify connectivity", beside a comment saying a proper implementation needs a generic `put()`;
`start()` puts the shard alone in its own map "for now". What follows, measured on the same pair:

- **each shard's map holds one shard, so it owns every symbol**: `s0` accepted `INSERT` of `AAA`,
  `BBB`, `CCC` and `DDD`;
- **two shards on one etcd share one leader key**, because the prefix is `/ob/` and the server has
  no flag to change it: `s1` became `s0`'s replica and answered each of them `ERR read-only
  replica`;
- **the Python pool in shard mode fetched an empty map**, opened no shard connection, and refused
  the first insert with `Shard  not connected` — the empty string being the shard it resolved to;
- **the C++ `ShardRouter::refresh_shard_map()` reads nothing** and returns success.

Roadmap #22 says sharding is done, and its parts are: the hash ring, the map's format, ownership,
`SYMBOL_MIGRATED`, the migration commands. What joins them — the map in etcd, written by the shards
and read by the clients, and a failover group per shard — is not there, so the feature cannot be
used end to end, and nothing tests it that way: the C++ tests hand the router its map, and no
integration test starts a shard. A fix has to decide who writes the map and how two shards starting
together do not overwrite each other (a compare-and-swap on the key's revision), what a shard that
leaves does to it (its lease), how a shard's failover keys are its own, and how both clients read
the map.

- Effort: M–L | Impact: a documented feature that cannot be used, whose nodes say they registered; a
  shard that takes every symbol, and a second shard that silently becomes the first one's replica

### 174. A start reads the whole WAL twice, even when its last checkpoint covers every record **P2**

**Found measuring part 2a of #165**, splitting a warm start by the timestamps in its own log. Both
builds, after the same 90-second soak and a clean stop — whose last checkpoint covers every record,
so the replay forwards **none** of them: a first pass over the WAL to find the last checkpoint, and
a second to forward what it does not cover. On the m9g.xlarge, one 274 MB WAL file of about 463 000
records: **about 0.3 s** for the first pass and **0.9–0.96 s** for the second, warm, in both builds.
Cold, the WAL is most of what a start reads once part 2a has taken the segment count down: of the
292 MiB this build's cold start read, about 261 are the WAL.

What bounds it is the WAL on disk at start: the file being written, up to `--wal-rotate-bytes` (512
MB by default), and every file retention has not yet deleted — since part 2a, back to the oldest
record a waiting block still needs. So a node that writes a rotation's worth between restarts reads
about twice this at every start.

Why the second pass costs three times the first is not measured, and is not guessed at here. The
first question is what a start needs to read at all: the last checkpoint's position is known when it
is written, and a start that could find it without reading everything before it would read only the
tail it replays.

- Effort: S–M | Impact: after part 2a, the largest part of a start's cost for a node that writes,
  and it grows with the WAL between rotations rather than with what is left to replay

### 173. A storage-fault test read a replica once, in the milliseconds between its recorded position and the end of its bootstrap ✅

**Found verifying #170 and #171**: `test_a_snapshot_whose_install_could_not_sync_is_installed_again`
failed once in a full local battery — *the replica did not end with every row*, having read none —
and passed **10 of 10** alone. The branch under test changed no C++, and the test reads through a raw
socket rather than through the client that branch changed, so the failure was not that change's; it
was not a flake either until it was measured.

Measured with the same two nodes, the replica's state file polled every half millisecond and the
first line of every answer kept: right after the position appears, `SELECT` answers **`ERR
bootstrapping` for 0.6–1.4 ms**, and **every one of the 1100 rows by 4.9–6.1 ms**, three runs of
three. `ReplicationClient::install_snapshot()` saves the position once the install has synced,
removes its staging directory, and its caller clears the bootstrapping flag after that — so there
is a window in which the position says the store is complete and the node, correctly, still
refuses reads. The test polled every 200 ms and read once, and `select_prices()` read **every**
`ERR` as no rows, so an honest "not yet" read as a replica that had lost everything. The window
grows with the latency of the syncs around it, which a full battery supplies.

**A test defect, not lost data** — the rows were there a few milliseconds later, every time.

**Fixed in the test.** `select_prices()` reads only `ERR … not found` as no rows — its documented
case, a symbol with nothing left — and raises on any other refusal; the replica test asks again
while the answer is `ERR bootstrapping`, and reads **first**, before the assertions that read the
whole log, because reading the log takes as long as the window. The state file is polled every
millisecond instead of every 200, so the read lands in the window in most runs rather than once in
a few hundred — **in most, not all**: the mutation reading once was killed in **4 of 5** runs, and the
defect as it was, every refusal read as no rows, in **3 of 5**. Holding the window open would make it
deterministic, and the fault injector can delay a call — but it takes one rule per node, and this
node's rule is the failed sync the test is about.

Mutation table: rows re-run five times where the answer depends on landing in the window; the poll
back at 200 ms with one read **survives**, which is the reason the poll changed, and a wait that
asks again after any refusal survives because nothing here produces another one. The docstring
control survives.

- Effort: S | Impact: a required check could fail on a correct replica, once in a few hundred runs
  under load, with a message saying rows were lost

### 172. The Python client's sharded pool replaced its routing state under its callers, and nothing tested it ✅ **P1**

**Found fixing #171, by reading, and not measured**: no test constructs a pool with
`coordinator_endpoints`, so there is nothing yet to measure it with, and building that is the first
half of the fix. Three things, each in a function the health check runs while callers route:

- `_rebuild_hash_ring()` assigns an **empty** ring and then fills it, so a lookup in between routes by
  a partial one — for a symbol the shard map does not assign, to a shard chosen from the virtual
  nodes added so far. Whether that shard then refuses a symbol it does not own is what this entry
  cannot say without the fixture.
- `_connect_shards()` deletes from the dictionary that `_route_query()`'s fan-out iterates, which
  raises `RuntimeError: dictionary changed size during iteration` in the caller.
- A shard connection is replaced only when the shard map changes, so one that #171 closes stays
  closed until then. Before #171 it answered every later command with the previous reply instead,
  which is worse and is what a client without that change still does.

The fix is a sharded-pool fixture first — a shard map in the test etcd and nodes behind it — then
routing state built beside the live one and swapped in whole, and closed shard connections
replaced at every health check.

**Fixed, with the instrument first.** `tests/integration/test_sharded_pool.py` builds what nothing
had: a native etcd, two standalone nodes, and the shard map in `<prefix>shard_map`, written by the
test in the shape `ShardMap::to_json()` produces — because a node started as a shard writes neither
itself nor the map to etcd, and two of them on one etcd become a primary and its replica (#175,
filed from this). Against the pool before the fix, three of its tests fail, one per defect above:

- a refresh made to stop after adding `s0` to its ring, and a symbol the whole ring gives to `s1`
  written meanwhile: **it landed on `s0`** — routed by the half-built ring;
- a fan-out query held on `s0` while a refresh took `s1` out of the map: **`RuntimeError: dictionary
  changed size during iteration`** in the caller;
- a shard connection closed as #171 closes one after a timeout: **it never came back**, and every
  write to that shard said so — with #171's own message promising that a pool replaces the
  connection at its next health check.

The map, the ring and the connections are one `_ShardRouting` now, built beside the one in use under
a lock of its own and published by one assignment, so a caller takes it once and routes by one
version. A connection the map no longer names, names elsewhere, or #171 closed is replaced, and
closed only after the new routing is published; the health check refreshes on a new map **or** on a
closed connection. Two things the fix itself introduced have tests of their own: `close()` takes the
routing lock and marks the pool, so neither a refresh in flight nor one a health check starts after
it opens a connection nothing will close; and a `SYMBOL_MIGRATED` retry whose fetch comes back empty
— etcd unreachable — keeps the routing there is rather than routing by none.

**Mutation table: 10 rows, 10 with the verdict written down before the run**, each killed row killed
by the test it names and by no other, every test green before and after. Three of the tests were
written for rows 5, 6 and 8 before the run, when nothing there could see them.

| # | mutation | caught by |
|---|---|---|
| 1 | the ring published before it is built | the half-built-ring test |
| 2 | the refresh deletes retired shards from the published dictionary | the fan-out test |
| 3 | the health check replaces no closed connection | the timed-out-connection test |
| 4 | a refresh keeps a connection #171 closed | the timed-out-connection test |
| 5 | `close()` swaps the routing without the lock | the close-during-a-refresh test |
| 6 | `close()` leaves the pool unmarked | the refresh-after-close test |
| 7 | a refresh does not ask whether the pool closed | the refresh-after-close test |
| 8 | a `SYMBOL_MIGRATED` retry routes by an empty fetch | the migrated-symbol test |
| 9 | retired connections closed before the new routing is published | survives: a caller holding the old routing fails on a retired shard either way |
| 10 | control: a comment reworded | survives |

- Effort: S–M | Impact: a sharded pool can route by a half-built ring, raise in a caller during a map
  refresh, and not recover a shard connection after one timeout

### 171. A Python client connection whose reply timed out answered the next command with it, and every command after that one behind ✅ **P0**

**Found designing the fix for #170**, reading what `_TcpBackend` does when a read times out: it
raises and keeps the socket, with what it has read of the reply still in its buffer and the rest
still on its way. The next command is sent, and its read starts at the front of that buffer — the
previous command's reply.

Measured on the i3-7100U against one standalone node: `BIG` holding 100 000 rows, `SMALL` holding
one, and a client with a 20 ms timeout. The query for `BIG` timed out; the next query, for `SMALL`,
returned **100 000 rows, every one of them `BIG`'s**, with no error; the `PING` after it got `SMALL`'s
reply. The control, on a connection that did not time out, answered `SMALL` with its one row. Every
reply after a timeout was one behind for the life of the connection — in direct mode as much as in a
pool, with one thread as much as with two. The default timeout is ten seconds, so in production it
takes an answer slower than that: a large scan, a server behind a long flush, a network pause. And
it happens to the caller that does the reasonable thing: catches the documented `OrderbookError`
and carries on.

**Fixed.** An exchange that does not finish — a timeout, a reset, an interrupt, a reply the client
cannot read — closes the connection before anything else can use it, and every later call on that
client raises, naming the exchange and saying that nothing was retried. Nothing reopens it: this
client has never reopened a connection by itself, and the one this closes may carry subscriptions
and a negotiated compression that a silent reconnect would drop. A new client does; a pool drops
the connection at its next health check — a `ROLE` on a closed connection fails — and replaces it at
the one after. `insert_batch()` already said a transport failure part-way leaves the batch
indeterminate; it says now that the connection is closed as it raises, because the replies still on
their way are exactly what the next command would have read.

**Tests**: nine in `tests/integration/test_reply_attribution.py`, which holds #170 as well. The two
for this item freeze a node of their own with `SIGSTOP`, so a reply arrives late by construction
rather than by the speed of the machine: after a query or a pipelined batch times out, a query and
a `PING` on the same client are refused with the reason, while a connection that did not time out
gets the right row from the same server. Against the client before the fix both fail — the query
**returned**. A third holds the pool's half: once the connection that timed out has been replaced, a
query is never answered with anything but its own row. **That one passes against the old client**,
because the old pool also dropped a connection whose `ROLE` timed out; it pins the replacement, not
the defect. Against the old client the module gives **8 failed, 1 passed**, and the two tests that
hold a connection's lock cannot run there at all, having no lock to hold.

**Mutation table, #170 and #171 together** — twelve rows, the verdicts written before the run, the
source restored from saved bytes, a fresh bytecode cache for every run so a restored file is not
answered from a stale one, and a row counted as killed only when the test named for it failed.
**12 of 12 as written down**: ten killed, both controls surviving.

| # | Mutation | Verdict | Killed by |
|---|----------|---------|-----------|
| 1 | an exchange holds nothing — #170 as it was | killed | the two-thread race, the health check, the poll, `close()` |
| 2 | an exchange that does not finish leaves the connection open — #171 as it was | killed | both timeout cases |
| 3 | a closed connection is not refused before it is used | killed | both timeout cases: `AttributeError`, not the refusal |
| 4 | the health check asks under the pool's lock | killed | the held-connection write, and the replacement test, which then waits for the lock the health check holds |
| 5 | an answer about a replaced connection is written anyway | killed | the replacement test alone |
| 6 | a connection whose `ROLE` failed is kept | killed | the pool-replacement test alone |
| 7 | a poll holds nothing | killed | the poll and `close()` tests |
| 8 | a pipelined batch holds nothing and closes nothing | killed | the batch timeout case alone |
| 9 | `close()` waits its turn behind a poll | killed | the `close()` test alone |
| 10 | a call `close()` interrupted reports the raw failure | killed | the `close()` test alone |
| 11 | control: the warning's words changed | survives | — |
| 12 | control: an abandoned connection keeps its buffer | survives | nothing reads it again: the refusal comes first |

**What this does in the sharded pool is filed as #172, not fixed here**: shard connections are
replaced only when the shard map changes, so one this closes stays closed until then — where it used
to answer every later command with the previous reply — and nothing tests the sharded pool at all.

- Effort: S | Impact: after one timeout, every reply on a connection belonged to the command before
  it — wrong rows, returned normally, by the documented client in every mode

### 170. The Python pool client answered one thread's query with another's, and its own health check stole a write's reply ✅ **P0**

**Found verifying #167 and #168**: the local battery failed
`test_failover.py::test_a_pool_client_follows_the_new_primary` once, with `Pool FLUSH failed:
unexpected response: PRIMARY 6`, and passed it 3 of 3 on a rerun — a reply to `ROLE` read as the
reply to `FLUSH`. That is not a flicker; it is the pool using one socket from two threads.
`_ClientPool.execute_write()` and `execute_read()` release the pool's lock before
`backend.execute()`, `_TcpBackend` has no lock of its own, and the health-check thread sends `ROLE`
down every connection every `health_check_interval` — 2 s by default — holding a lock the caller no
longer holds. Two threads then write one socket and read one buffer.

Measured on the i3-7100U against one standalone node through `hosts=[...]`: with one caller and the
health check at 10 ms, the **first** `FLUSH` got `STANDALONE`. With two callers each asking only for
its own symbol, one row each, and the health check off, **10 216 of 26 104** answers to `PA`'s query
carried `PB`'s row — 39%, and **no error at all**. With the health check on as well: 32 of 82 foreign,
194 errors.

Three consequences, in the order they cost: a multi-threaded caller gets another query's rows and
nothing says so; a single-threaded caller at the default interval sometimes gets an error for a
write the server applied, and retrying it stores the rows twice, in storage that never removes one;
and the health check, reading a query's reply as a role, marks a healthy node unknown and sends the
pool to re-discover.

**Fixed**, with #171 in the same change. Each connection carries one exchange at a time — a command
and its reply, a pipelined batch and its replies, a poll, the handshake — under a lock of its own,
so the pool's lock decides where a command goes and the connection's decides when.
`tests/integration/test_reply_attribution.py` holds it: against the client before the fix the
two-thread race fails with **2224 of 5374** answers carrying the other symbol's row, and the
health-check test with `Pool FLUSH failed: unexpected response: PRIMARY 1`.

**Two consequences of that lock had to be fixed with it, and the first is one the lock created.**
The health check asked its `ROLE` questions holding the pool's lock. Once an exchange holds its
connection, a `ROLE` waits for whatever a caller has in flight there — so one long read on a replica
would have stalled **every** call through the pool, a write to the primary included, for as long as
the read lasted. It asks without the pool's lock now and writes the answers under it, and an answer
about a connection another thread has replaced meanwhile changes nothing: a write's retry reconnects
on its own thread, and dropping the replacement would leave the pool without a node it can reach.
The test holds a replica's connection the way a long read does and requires a write to the primary
to finish; with the questions asked under the lock, the write waits until the connection is let go.

The second is `close()`. Behind the lock it would wait its turn, and a poll's turn lasts as long as
the poll was asked to wait. It shuts the socket under an exchange in flight instead, which ends the
other thread's read at once — and that is also what makes closing safe, because closing a descriptor
a read is blocked on frees a number the read may still be using. Measured against the client before
the fix, `close()` under a poll returned at once and **the poll died with `AttributeError: 'NoneType'
object has no attribute 'settimeout'`**; after it, `close()` takes **0.101 s** — the tenth of a
second it waits for the connection to be free — and the poll raises `the connection … was closed
while this call was in flight`.

**A third symptom was on the connection all along and was not in this entry.** `poll()` waits by
setting the socket's timeout to what is left of its own wait, and a socket's timeout is every
thread's. A command whose reply took longer than a concurrent poll's wait failed as a timeout it
never had: with the server frozen for half a second, a client whose timeout is ten seconds and a
poll of 50 ms beside it, the query raised `TCP recv timeout`. **My prediction for that test was
wrong**, and the first version of it measured nothing: I expected a poll and a command to swap
replies, as two commands do. They do not — both take from the front of one buffer and a command has
one reply outstanding — so the race version passed against the old client, and the test was
rewritten around the shared timeout.

The mutation table for both items is under #171.

- Effort: S | Impact: silent wrong answers for a multi-threaded caller, and failed-but-applied writes
  for a single-threaded one, through the documented multi-host client

### 169. An instrument whose exchange has a dot shares its book, its sequence numbers and its stored rows with another **P1**

**Found writing part 2a of #165**, reading the key a per-symbol store is filed under. The engine
files a live book, a sequence counter and a columnar store under `symbol + "." + exchange`, built in
about fifteen places — so `"A.B"` on `"C"` and `"A"` on `"B.C"` are one key. Measured over the wire
on master: `INSERT A.B C bid 100 5 1`, `INSERT A B.C bid 200 7 1`, `FLUSH`. `BOOK A.B C` and
`BOOK A B.C` answer **the same two levels** — 200 at level 0 and 100 at level 1, both at sequence
number 2 — a `SELECT` of `'A.B'.'C'` returns both rows and one of `'A'.'B.C'` none, and the data
directory holds one segment, under `A.B/C`, with both rows in it, so the mix survives a restart.
Part 1 of #165 keyed its own index with a NUL for exactly this reason and did not reach the
engine's maps.

**The key is also a format.** `SequenceTracker::VectorEntry::key` is that string, and a version
vector is persisted in the WAL, sent in the mesh's handshake and carried by a mesh snapshot — so
a new separator would change what three formats carry. A dot in a symbol is ordinary (`BRK.B`); a
dot in an exchange is not, and the collision needs one: with none, the key splits at its last dot
into exactly one pair. So the fix is a refusal at every entry that names an exchange — the wire's
writes and queries, the C API, and what a primary or a peer sends — with the existing key kept.

- Effort: S–M | Impact: two instruments' books, sequence numbers and stored rows merge silently
  whenever an exchange name carries a dot

### 168. A `SNAPSHOT` keeps the last row a scan delivers for each level, not the latest one at or before its time ✅ **P1**

**Found writing part 1 of #165**, asking what depends on the order a scan hands rows to its callback.
`SNAPSHOT` does: it reconstructs the book at `T` by keeping, for each `(side, level)`, the row that
arrives last — `state[key] = row` — and a scan delivers segments by start and each segment's rows in
the order they were appended. That is time order only while rows arrive in it.

Measured with one level written twice in one flush, the later event time first — 5 s at price 100,
then 3 s at price 101 — and `SNAPSHOT` at 10 s. In local mode on the i3-7100U, on this tree: **the
3-second row**. Over the wire on the m9g.xlarge, on the tree before #139, the same: the row at
`1790000003000000000` with price `101`, while `SELECT *` returns both rows. The book at 10 s was the
one written at 5 s; a correction for an earlier instant, arriving after it, replaced it in the
answer. Reachable the ways #166 was — a client giving its own event times (#105), a mesh peer's
backlog applied after a partition — and there since the engine's first commit, invisible while every
row arrived in time order.

The answer is the row with the greatest timestamp at or before `T` for each level, with the order of
delivery only breaking a tie. That also makes the answer independent of the order the index hands
segments out in, which part 1 of #165 had to preserve for exactly this reader.

**Closed.** For each level the answer is the row with the latest timestamp at or before `T`, a tie
going to the later written — what every row got before — and the levels come bids first, then
asks, each by level, the order `BOOK` answers in, where a hash map made the order and with it what a
`LIMIT` kept. Against the code before the fix, four of the five new tests in
`tests/test_snapshot_query.cpp` fail on the assertions that name this item and #167 — the 3-second
row, the hash's order, no columns — and the tie test passes, which is what it is there for.

**One of those tests predicted the old code wrongly, and then the next change made it blind.** Its
comment said the correction arriving a flush later was answered right before the fix, by the order of
the segments; run against that code it failed with 101, because a store on its own handed its segments
out in the order it wrote them. Part 1 of #165 then made every index hand them out by start — which
puts the correction first and makes the old rule right by luck — so on the tree this merges into, the
same mutation **survives** that test. It pins the answer now, not the defect, and says so; the verdict
was changed before the run on the rebased tree, not after it.

Mutation table, on the rebased tree, 9 rows with the verdict written down before the run — 7 killed,
2 surviving, both explained, every instrument green before and after:

| # | mutation | caught by |
|---|---|---|
| 1 | the shape never assigned — #167 as it was | the shape test |
| 2 | the shape always all seven, whatever the select list names | the shape test |
| 3 | the last row delivered wins — #168 as it was | the latest-row test |
| 4 | a tie keeps the first delivered | the tie test |
| 5 | a hash map again | the order test |
| 6 | the key without the side | the order test |
| 7 | the latest by sequence number rather than by time | the latest-row test |
| 8 | the last row delivered wins, against the two-segment test | survives: since #165 part 1 the old rule is right there by luck |
| 9 | control: the `DEBUG` line reworded | survives |

- Effort: S | Impact: a `SNAPSHOT` answered with a book that never existed at its time whenever a
  level's updates reached a flush out of time order

### 167. A `SNAPSHOT` answers over the wire with no columns, since #139 ✅ **P1**

**Found measuring #168, which could not be measured over the wire at all.**
`SELECT * FROM 'SNP'.'EX' WHERE AT <T>` answers `OK`, an empty header, and one empty line per row:
the rows are there and every column is gone. The shape a row query is formatted with, which #139
introduced so that a row query answers the columns it names, is assigned after the `SNAPSHOT`
branch has returned — so the formatter receives an empty column list and prints what it was given.
Measured on the m9g.xlarge with the same probe on two trees: before #139 the answer has all seven
columns; at #139 it has none. Nothing caught it: the only test of `SNAPSHOT` parses the statement,
and nothing executes one over the wire. Local mode is unaffected, because `ob_query()` hands rows
to its caller without formatting them — which is where #168 was measured on this tree instead.

**Closed.** The shape is assigned at the top of the `SNAPSHOT` branch — the select list, or all seven
— so a `SNAPSHOT` answers the columns it names like every row query since #139. The new
`tests/integration/test_snapshot_query.py` asks over the raw protocol, because the shape is the thing
asked about and both clients read a row by position: against the server before the fix all three of
its tests fail on `['OK']`, after it all three pass. `raw_query()` moved into `conftest.py` to be
shared with `test_column_projection.py`, whose seven tests pass unchanged with it there.
`docs/query-language.md` says what the answer is: the latest row at or before the time for each
level, the order, the columns, and that it reads the store, so a row the tick has not written is not
in it.

- Effort: S | Impact: the documented way to reconstruct a book at a point in time answered nothing
  over TCP, and said `OK`

### 166. A segment's time range was its first row's hour and its last row, so a query skipped rows it held and retention deleted current ones ✅ **P0**

**Found while measuring #165**, asking why the same one-second query got slower the further back it
looked. `SegmentMeta` declares `start_ts_ns` as the "earliest timestamp in this segment" and
`end_ts_ns` as the "latest", and `flush_segment()` fills them with the start of the period its
first row fell in and the timestamp of its **last** row. The two agree only while rows arrive in
time order — the case when the server stamps every row on arrival — and not when a client gives its
own event times (#105), nor in multi-master when a peer's record, which keeps the time the peer gave
it, is applied after a later local row (read from the code, not measured). Measured on the
m9g.xlarge through the wire, identically on master and on #164's branch:

| rows written in one flush, in this order | the segment directory, `<start>_<end>` | `SELECT` of everything | `SELECT` of only the row out of place |
|---|---|---|---|
| T + 2 s, then T + 1 s | `1790182800000000000_1790183401000000000`: it ends at T + 1 s | both rows | **nothing** |
| 5 s into an hour, then a minute before it | `1790186400000000000_1790186340000000000`: it **starts after it ends** | both rows | **nothing** |

Both answers are `OK`, and nothing is logged.

**And retention deletes what it should keep.** `delete_expired_segments()` deletes a segment whose
`end_ts_ns` is older than the cutoff — the last row's time, not the newest's. Under `--ttl-hours 24`,
a row written now and then one dated 48 hours back, in one flush: **0 of 2 rows** after the first
sweep, and the log said `its newest row is 24.0 h past the retention` of a segment holding a row one
second old. In the other order the older row starts a period of its own, the current one rolls it
over, and only the old row goes — which is the control.

**It is also why a query gets slower the further back it looks.** Every segment of a period claims
to start at the period's boundary, so pruning keeps every segment of that hour written after the
window: a one-second `SELECT` of one symbol holding 1100 segments took **2.0 ms two seconds back and
7.0 ms 115 seconds back**, returning the same 400 rows each time.

**Four documents said otherwise**, because they read the declaration and not the function that
fills it: #105's item and pitfall 208 ("segment pruning is a range-intersection test, so
out-of-order times cost scan efficiency, not correctness"; "TTL retention works on whole segments by
their newest event time"), `docs/operations.md` ("rows arriving out of order widen segments"), and
the `--ttl-hours` row of `docs/cli.md` ("a batch whose oldest row is past the window is expired").
The intersection test is right; the range it intersects was not the rows'.

**The fix: the range is the rows', and the one number replay reads is kept.** `append()` tracks the
active segment's earliest and latest timestamps, and `flush_segment()` records them — and names the
directory by them — while the time of the last row goes into a field of its own, `last_row_ts_ns`.
That separation is the design decision. `end_ts_ns` had four readers: pruning and retention needed
the newest row, the index's order and the directory name did not care, and **replay's fallback
needed exactly the old number**. For a symbol with no trusted WAL position — segments older than
#63, and segments a snapshot brought in, which carry the sender's position — replay takes a record
at or before the recorded end to be stored already, documented as exact only while times rise.
Given the newest row instead of the last, its error with falling times would turn from a record
applied twice into a record never applied. It reads `last_row_ts_ns`, and one test says so: a
segment holding +2 s and then +1 s, made foreign by its WAL identity, and a +1.5 s record in the WAL
tail that nothing stores — it comes back after a `SIGKILL` before the fix and after it, and is lost
under the mutation that reads the newest row.

**Segments already on disk are repaired once, at the first start that finds them** — and whenever a
snapshot install brings one in, since a segment from an older primary is one of these. Such a
segment is known by what its `meta.json` lacks (`"time_range":"rows"`). Its `ts.col` is read, its
range corrected in the index always, and on disk in batches of 4096: every corrected file written
beside the old one, one `syncfs()`, and only then a `rename()` over it — so a crash or a power cut
anywhere leaves each segment with its old `meta.json` or its new one, never half of either, and the
next start finishes the job. Not `write_file_atomically()`: that is an `fsync` of the file and of
its directory per segment, for every segment a node ever wrote. A failed `syncfs()` publishes
nothing of its batch — tested through the injector: the corrected files are removed, the old
`meta.json` is byte for byte what it was, the query is answered from the corrected index, and the
next start writes it. The format version stays 2, so an older build still reads what this one
writes, and prunes and expires by the right range.

**Measured on the m9g.xlarge** (GCC 14 Release, data on EBS, the page cache dropped before every
start, `scripts/measure_segment_range_repair.py`): 106 752 segments written by the build before the
fix — 256 symbols for 90 seconds — then restarted three times on that directory.

| start | first answer after | what it did |
|---|---|---|
| the build before the fix | 82.31 s | read 106 752 `meta.json` files |
| this fix, first start | 170.02 s | the same, and the repair: 106 752 read and rewritten in **87.1 s**, none of them holding rows outside the range they recorded (the writes were server-stamped, so in time order) |
| this fix, next start | 82.34 s | nothing to repair |

So the repair costs roughly one more cold read of the whole index, once. What makes both numbers
large is the segment count, which is #165. And the slope from the top of this item is gone: the same
one-second `SELECT` of one symbol holding 1098 segments took **1.22, 1.19, 1.15 and 1.16 ms** at 10,
30, 60 and 115 seconds back — the same 400 rows each time, and flat, because every segment's range is
now the span of its own tick's rows (`scripts/measure_time_pruning.py`). What is left of that
millisecond is the index walk of #165.

**On the drain path the change costs nothing nine interleaved rounds can see.** Two compares a row
in `ColumnarStore::append()`, which is about 15 ns of drain work per row: 15.33 ns median before and
15.49 after, per-round ratios 0.978-1.042 with a median of 1.006 — a difference inside a spread
wider than it, so it is not reported as a cost either way. The function's instruction count went
692 → 485, and that is not a saving: GCC outlined the vectors' growth path
(`std::vector<unsigned long>::_M_realloc_append`, a symbol this build's archive has and the one before
it does not), which is where code lives, not how much of it runs.

**Tests.** Eleven in the new `tests/test_segment_time_range.cpp`: both shapes, rows in order, a
rollover, retention, the two new keys read back, a `meta.json` of the old format read with its
recorded end as its last row, the repair in the index and on disk and a second open that reads
nothing, a leftover of an unfinished repair removed, a repair that cannot write that still corrects
the index, and a segment installed from an older primary. Six in the new
`tests/integration/test_segment_time_range.py`, through the wire. Against the build before the fix
the wire tests fail as they should — both queries answered nothing for a row a `SELECT` of everything
returned, and retention left 0 of 2 rows — and the replay test passes, because it guards what must
not change. `test_flush_segment_returns_meta` pinned the old start ("rounded down" to the period) and
now expects the first row's time.

**Mutation table: fifteen rows, fifteen with the verdict written down before the run** — thirteen
killed and two controls surviving — against committed code, every instrument green before the first
row and after the last, and the sources restored byte for byte.

| # | Mutation | Instrument | Verdict |
|---|---|---|---|
| 1 | the start is the period's start again | a late row from the hour before (C++) | killed |
| 1i | the same | the same, through the wire | killed |
| 2 | the end is the last row again | rows out of order (C++) | killed |
| 2t | the same | retention, through the wire | killed: 0 of 2 rows |
| 3 | the range not widened by the rows after the first | rows out of order (C++) | killed |
| 4 | the range not restarted by a rollover | the rollover (C++) | killed |
| 5 | an old segment not corrected in the index | the repair (C++) | killed |
| 5i | the same | the upgrade, through the wire | killed |
| 6 | the repair kept in memory, never written | the second open (C++) | killed |
| 7 | the corrected files renamed without the sync before them | the failed `syncfs()`, through the wire | killed |
| 8 | what an unfinished repair left is not removed | the leftover (C++) | killed |
| 9 | replay compares with the newest row instead of the last | replay, through the wire | killed |
| 10 | an old `meta.json` read as a last row of zero | the old format (C++) | killed |
| 11 | control: the unreadable `ts.col` warning reworded | the upgrade, through the wire | survives |
| 12 | control: the segment-written debug line reworded | rows out of order (C++) | survives |

**The documents.** The four that said otherwise say since when it is true: #105's item, pitfall 208,
`docs/operations.md` — which also has a section on what the first start after the upgrade does and
what its log line means — and the `--ttl-hours` row of `docs/cli.md`. Spec:
`kiro-workspace/specs/segment-time-range/`.

- Effort: M | Impact: a time-range `SELECT` could answer `OK` without rows the node held, and
  `--ttl-hours` could delete rows younger than the retention, whenever one symbol's rows reached a
  flush out of time order

### 165. Nothing merges segments, so the index grows by one per active symbol per tick, and everything that reads it walks it whole ✅ **P0**

**Found while measuring stage 5 of #151 (#164).** A flush tick closes the active segment of every
symbol that received rows since the last tick, so a node gains **one segment per active symbol per
tick** — ten ticks a second at the default interval — and nothing merges them; TTL deletes them, if
a TTL is set, and that is all. Four things walk the whole index:

- **every tick's merge, under the engine's lock**: `merge_segments()` checks each new segment
  against every existing one before inserting it, then sorts the index;
- **every query**, which copies the index before pruning it;
- **startup**, which reads every segment's `meta.json`;
- and **the TTL sweep**, which sorts the whole index and deletes expired directories while holding
  the index's own lock, which every query takes — outside `mtx_` since #164, but not out of a
  query's way.

Measured on the m9g.xlarge (Release, the stage-5 server), with a probe that fills an index and times
the two calls:

| segments in the index | merge of 16 new, under `mtx_` | a scan that finds nothing |
|---|---|---|
| 1 000 | 0.055 ms | 0.034 ms |
| 10 000 | 0.61 | 0.32 |
| 50 000 | 3.7 | 1.6 |
| 100 000 | 7.7 | 3.3 |

Linear, and the merge's line is time every writer waits. **How fast a node gets there**: a soak
writing one 20-level `MINSERT` to each of 256 symbols twenty times a second — 102 400 levels a
second, a small fraction of what one connection can write — had **105 472 segment directories after
90 seconds**, and a writer's round grew with them:

| after | segments | a round of 256 writes, p50 | p99 | max | narrow `SELECT` | `PING` |
|---|---|---|---|---|---|---|
| 15 s | 21 248 | 0.61 ms | 14.7 ms | 16.4 ms | 2.85 ms | 0.08 ms |
| 30 s | 41 216 | 0.59 | 32.7 | 37.1 | 4.60 | 0.09 |
| 45 s | 59 392 | 0.60 | 53.8 | 60.2 | 6.52 | 0.06 |
| 60 s | 75 776 | 0.59 | 74.7 | 79.9 | 7.83 | 0.09 |
| 75 s | 91 136 | 0.60 | 89.4 | 93.6 | 9.96 | 0.07 |
| 90 s | 105 472 | 0.58 | 104.2 | 115.7 | 10.66 | 0.08 |

The median does not move and the p99 grows by about a millisecond per thousand segments, which is
the shape of a cost paid once a tick under the lock — and the only thing a tick does under it that
grows with the index is that merge, which by the end of the soak put 256 new segments into 100 000
every tick, where the probe's row puts 16. The ticks slowed as it grew — 1 416 segments a
second in the first fifteen seconds, 956 in the last — because each one spends longer merging. A
`PING` is untouched, because it takes no lock. Restarted with 105 728 segments, the node answered
its first request after **2.70 s**, and a narrow `SELECT` then took 19.6 ms. Nothing bounds any of
this but TTL: a node with a thousand active symbols and no retention degrades for as long as it
runs. A segment is nine inodes — its directory, seven column files and `meta.json` — so the soak
also created about 950 000 of them in 90 seconds. The m9g.xlarge's 100 GB xfs root reports 52.4 M
inodes, which that rate would use up in about an hour and a half: a projection, not a measurement.
And a query reads more of these segments than its range needs, which is #166.

**What a fix has to do, in two parts that can land separately.** The index must stop being walked
whole — a lookup per symbol rather than a scan, a duplicate check that does not compare against
every segment, nothing sorted under `mtx_`, and a query that does not copy what it will prune.
That removes the slope from the writers and the queries. And the count must stop growing with the
tick rate — a segment that stays open across ticks, or segments merged behind the writers — which
is what startup, the inodes and every walk that remains need. Which of those is a design question
with its own measurements, for its own spec.

**Part 1, the index, is done, and so are part 2a, the count at the tick, and part 2b, merging what
is written — both below.** Each symbol has its
own segments now, keyed with a NUL between symbol and exchange (a dot would make `"A.B"` on `"C"` and
`"A"` on `"B.C"` one key). A tick's merge is a set lookup and an insertion, at the end almost always;
a query finds its symbol and binary-searches its window; retention decides and takes the expired
segments out under the index's lock and measures and deletes them without it, so a query that
copied one before it went can find its files going — and says so on `DEBUG`, because those rows are
past the retention, while a missing file of a segment still indexed is the `ERROR` it always was.
And the engine's and the C API's per-symbol stores keep no index of their own (`OwnIndex::kNo`): each
had kept a copy of every segment it wrote, for the life of the process, that nothing read and
retention never pruned.

**The design had one window per symbol, and was wrong about what it cost.** The window was
`[s - widest, e]`, `widest` the widest segment the symbol ever had, and the spec said a window "a
little too wide costs comparisons, never a segment". Not a little. A flush that closes a batch
mixing current rows with old ones — a peer's backlog applied after a partition, a client's late
correction (#105) — writes a segment as wide as the gap between them, and from then on every query
of that symbol walked back over each of its segments within that width of its range. Measured with
`benchmarks/segment_index_cost`, a scan of a gap no segment holds, beside one segment half the
history wide: **0.0031 ms at 10 000 segments of the symbol, 0.020–0.025 at 50 000, 0.048–0.052 at
100 000** — linear, under the lock a tick's merge waits on. The index is in **width tiers** now, by
the bit width of `end - start`, each with its own window. In a tier every segment is more than half
the widest, so what a scan compares there and does not need contains one instant — at most the
tier's segments holding `s - 2^(bits-1)`, which is one or none when a symbol's segments follow each
other. The same measurement: **0.0001 ms at every size**. `scan()` returns what it compared and its
candidates, so the bound is asserted rather than argued: a property test holds the candidates to
exactly the segments whose range meets the query, and the excess to the tiers times the deepest
instant. Found before the pull request, by asking what "a little" was bounded by.

Measured on the m9g.xlarge, GCC 14 Release, five rounds with before and after alternating; the
before columns of the first two are the table at the top of this item:

| segments in the index | merge of 16 new | a scan that finds nothing | a scan past one wide segment: one window → tiers |
|---|---|---|---|
| 1 000 | 0.055 → **0.007–0.010 ms** | 0.034 → **0.0001 ms** | 0.0002 → 0.0001 ms |
| 10 000 | 0.61 → **0.003–0.005** | 0.32 → **0.0001** | 0.0031 → 0.0001 |
| 50 000 | 3.7 → **0.003** | 1.6 → **0.0001** | 0.020–0.025 → 0.0001 |
| 100 000 | 7.7 → **0.003** | 3.3 → **0.0001** | 0.048–0.052 → 0.0001 |

And the soak from the top of this item, the build before (#166's tree) and the build after back to
back on fresh data directories on the EBS volume, the disk quiet before the first:

| after | segments, before → after | a round, p99 | max | narrow `SELECT` | server RSS |
|---|---|---|---|---|---|
| 15 s | 22 784 → 24 576 | 6.07 → **0.76 ms** | 13.26 → 2.99 ms | 2.99 → **0.38 ms** | 40.0 → 36.6 MiB |
| 30 s | 42 496 → 47 872 | 26.87 → **0.71** | 36.99 → 0.74 | 4.97 → **0.30** | 55.1 → 48.9 |
| 45 s | 60 928 → 70 400 | 45.61 → **0.71** | 53.78 → 0.81 | 7.06 → **0.33** | 65.8 → 57.2 |
| 60 s | 78 080 → 93 952 | 65.10 → **0.74** | 68.33 → 0.89 | 8.21 → **0.31** | 81.7 → 71.1 |
| 75 s | 94 208 → 117 248 | 79.28 → **0.74** | 84.81 → 0.78 | 10.00 → **0.35** | 92.1 → 78.1 |
| 90 s | 109 056 → 139 520 | 94.66 → **0.74** | 105.38 → 0.80 | 11.13 → **0.31** | 100.8 → 88.1 |

A round's p50 is 0.58–0.62 ms in both, so the slope was all in the tail, where a tick's merge put
it. The p99 is flat now **with 28% more segments**, the narrow `SELECT` is flat, and the server is
smaller with more segments — about 250 bytes a segment at the same count, which is the copy the
per-symbol stores kept.

**The prediction written before the implementation** (spec §1.6), against that: a merge of 16 at
about 0.05 ms at 1 000 segments and no more than twice that at 100 000 — **0.003**, an order of
magnitude under it; a scan finding nothing in single microseconds — **0.1 µs**; the soak's p99 and
`SELECT` flat — **both**; RSS about 200 bytes a segment lower — **about 250**; the start unchanged —
**unchanged per segment**, below, which is part 2's number.

**What part 1 made worse, and why it is part 2's number.** The ticks no longer slow as the index
grows, so a node gains segments faster: **1 550 a second** over the soak against 1 212 —
one per active symbol per tick, at about six ticks a second, because what bounds a tick now is
writing 256 segments of nine files each. And a start reads every segment's directory. Warm, with
every inode cached, that is CPU: **2.85 s for 109 312 segments before, 3.37–3.41 s for 139 776 after**,
26 and 24 µs a segment. **Cold** — after `drop_caches`, which is a reboot or a new instance — it is
the disk: **84.2–84.6 s before, 107.2–107.5 s after, reading 1.19 and 1.44 GiB** from this gp3
volume, about 11 KiB and 0.77 ms a segment, and the two starts stand in the ratio of their segment
counts (1.27 and 1.28). So part 1 did not change what a segment costs a start, and at this soak's
rate every second of uptime adds about 1.2 s to a cold one. A start's time depends on what the page
cache kept, and the measurement said so on its own: the first restart of the run, warm by the
script's definition, read 63 MiB from storage and took 12.04 s. Part 2 is now more urgent than
this item was when it was filed, not less.

**Mutation table: 21 rows, 21 with the verdict written down before the run** — 19 killed, 2
controls surviving, every instrument green before and after. Reading the table against the tests,
still before any run, found that row 7's instrument could not see it: the retention test asked
`holds()` whether an emptied symbol was gone, and `holds()` answers by segments, so a kept empty
entry was invisible to the check whose message named it. The test was given `symbols_indexed()`.
That reading is also what led to the tiers, which rows 17–21 are about.

| # | mutation | caught by |
|---|---|---|
| 1 | a tier's window not widened by its widest segment | the property test |
| 2 | the widest segment never recorded | the property test |
| 3 | every segment appended at the end, whatever its start | the property test |
| 4 | no duplicate check | the duplicate test |
| 5 | the key without the exchange | `holds()`'s test |
| 6 | the key joined with a dot | `holds()`'s test |
| 7 | retention keeps a symbol it emptied | the retention test, by `symbols_indexed()` |
| 8 | a segment retention could not delete leaves the index | the undeletable-segment test |
| 9 | a segment going under a query is an `ERROR` again | the query-meets-retention test |
| 10 | a segment still indexed is taken as going | the missing-file test |
| 11 | the engine's per-symbol store indexes its own segments | the static test |
| 12 | a store whose segments go elsewhere indexes them anyway | the no-copy test |
| 13 | `index()` in no particular order | the order test |
| 14 | removing named segments does not count them out | the removal test |
| 15 | control: the quiet line reworded | survives |
| 16 | control: retention's failed-delete line reworded | survives |
| 17 | every segment in one tier — the one window this began with | the wide-segment test |
| 18 | the tiers' candidates not merged into one order | the delivery-order test |
| 19 | removing named segments keeps a symbol it emptied | the removal test |
| 20 | the scan counts nothing it compared | the property test |
| 21 | the scan says its candidates are everything it compared | the property test |

Row 18 has a reader behind it that the tests did not name until then: a `LIMIT` takes the first rows
a scan delivers and a `SNAPSHOT` keeps the last, so the order candidates reach the callback in is
part of the answer. Asking who else depends on it found **#168**, and measuring that found **#167**.

**Part 2a is done: a tick seals only the stores that are due, and about as many rows as it
drained.** A tick used to write a segment for every symbol that had received a row since the last
one — at 256 symbols about two thousand files and a `syncfs()` a tick, 84.7% of the flush thread's
CPU in the kernel creating them. Rows now wait, readable, in the blocks a drain publishes to the
query index, and a store is **sealed** — its blocks written into one segment and swapped for it in
one step — when it has 65 536 rows, when its oldest block is ten seconds old, or when every block
together is over four million rows. Below that budget a tick takes **its share**: at most 64 stores,
and after the first no more rows than a quarter more than it drained (or 65 536, if it drained
fewer), so stores that come due together are spread over ticks. `FLUSH`, `close()` and both
snapshots seal everything.

**The checkpoint was the hard part, and its first design lost rows before any test ran.** It said:
claim only what no block still holds, and nothing more is needed. Walking the kill test through it —
written for this part, not yet run — found the hole. A seal writes the blocks of several drains into
one segment, positioned at the last of them; with another symbol's older block still waiting, the
claim stands *before* that segment's position, so a start removes the segment as unvouched, and
replay from the claim rebuilds only its rows recorded *after* the claim. The earlier ones are gone,
and retention may already have deleted their records. And a position cannot vouch at all once a late
seal of old blocks writes a segment positioned before the checkpoint it came after.

So **every seal has an epoch.** Each `write_seals()` that writes anything bumps it, the segments it
writes carry it in `meta.json`, and a checkpoint written while blocks wait is **sixteen bytes**:
where replay starts — the oldest record a waiting block still needs — and the epoch the sync before
it covered. At start a local segment sealed after that epoch is removed and rebuilt; whatever the
replay brings back that a kept segment already holds is skipped by the per-symbol positions replay
has filtered by since #63. With nothing waiting the checkpoint is the eight bytes it always was. The
epoch continues at open from the highest the store or the last checkpoint names, because starting
again at zero would stamp new seals with epochs an old checkpoint already vouches for. Which
segments a checkpoint vouches for is a pure function, `Engine::segment_vouched_for()`, tested
without staging a crash.

**Going back to an older build is safe only from a clean stop.** `close()` seals everything, so the
last checkpoint is the eight-byte form every build reads. After a crash, a build before this one
reads the sixteen bytes as saying nothing and replays from the checkpoint record, which would skip
the records of the rows that were waiting: start the node once with this build first.

Measured on the m9g.xlarge against master (`4702cfa`; the same GCC 14 Release build of both, ABBA,
each run on a fresh data root on the gp3 volume), with the soak of part 1 — 256 symbols, one
20-level `MINSERT` each per round, 20 rounds a second — on `900ac7b`, whose C++ is this pull
request's:

| | master (`4702cfa`) | part 2a |
|---|---|---|
| segments on disk after 90 s and a clean stop | 141 312 – 143 104 | **2 304** |
| flush ticks a second | 5.9 – 6.3 | **8.9 – 9.3** |
| seals a second, per 15 s window | — | 17.1 or 34.1 — 25.6 on average, 256 symbols every ten seconds |
| writer round, p50 / p99 | 0.59 – 0.62 / 0.70 – 0.76 ms | 0.59 – 0.62 / 0.62 – 0.76 ms |
| narrow `SELECT` of the last second | 0.29 – 0.38 ms | 0.26 – 0.35 ms |
| RSS at 90 s | 87.9 – 90.2 MiB, growing with the segments | 99.4 – 99.9 MiB, within 3 MiB of that from the forty-fifth second |
| restart after a clean stop, warm | 3.25 – 3.33 s | **1.50 – 1.51 s** |
| restart after a clean stop, cold (`drop_caches`) | 107.54 – 109.45 s, 1 456 – 1 474 MiB read | **4.38 – 4.42 s**, 291 – 293 MiB read |
| restart after a kill, warm | 3.40 s | 1.70 s, replaying the rows that waited |

**The prediction held on every line the soak measures**, and the tick's own work is read from its
cadence rather than timed: the flush loop sleeps its interval and then ticks, so 8.9–9.3 ticks a
second at 100 ms is an average tick of **8–12 ms**, where master's 5.9–6.3 is **59–70 ms** — the ~70
ms the prediction started from. One cost it did not name is real: the rows waiting in blocks are
memory. RSS is about 100 MiB from the forty-fifth second on, against master's 88–90 MiB at the
ninetieth — which grows with master's segments, where this does not — and the budget bounds it at
four million rows. The first version of this part, before the share and the row pool below, held
170.7–171.9 MiB in the same soak.

**Measuring it corrected part 1's own number.** Part 1 put a warm start at "about 24 µs a segment":
the whole start divided by the segments. The start's own log splits it: on master, **2.12 s** to
open 142 592 segments together with a first pass over the WAL, **0.96 s** for a second pass, and
0.24 s to listening — about **13 µs a segment**, and roughly 1.2 s that both builds pay for reading
the WAL twice. Which is now most of what a start costs: of the 292 MiB this part's cold start read
in its first measurement, about 261 are the WAL. That is **#174**.

**Measuring it found a harness problem before an engine one.** The first ingest table put part 2a
10–11% behind master at four pipelining connections — medians of five rounds, 10.61 M levels a
second against 11.77 M — with the two equal at one connection. It did not survive the question of
what else was running: the harness started each run straight after the last, with the page cache
still holding what that run had written and deleted — a gigabyte of WAL, thousands of segment files
— for the next run's own syncs to flush, and in its rotation part 2a's runs mostly followed
master's. Six rounds of each at four connections, default policy, without and with a quiet disk
first:

| before each run | master | part 2a |
|---|---|---|
| nothing — straight after the last | 10.24 M (9.39–11.77) | 10.01 M (9.71–11.52) |
| `sync`, and a second of no writes on the data root's device | **11.84 M (11.80–11.86)** | **11.77 M (11.75–11.86)** |

The wait after the `sync` was hardly ever more than its one second: the `sync` is what does it. The
harness does both now (`--no-settle` measures without), and the pull request's table below was
measured with them.

**What the chase found on the way is real, and none of it is that gap.** The flush thread took 26%
more CPU than master's; collecting each block's range in the drain, reserving the block to its
store's last and appending a block at once (`ColumnarStore::append_block()`) took that to 17%. The
tick's own lines, on `DEBUG` since then, showed its seals **strictly alternating**: sixteen stores
taking about 62 500 rows a tick, just under the 65 536 that make a store due, came due together
every other tick, and that tick wrote ~1.45 M rows in 21 ms and synced them in 39 — about as long as
the four connections take to fill the pending queue's million rows. So a tick seals **its share**.
The first share, exactly what the tick drained, kept whatever backlog it found — what comes due each
tick is what was drained, so the stores the first wave deferred stayed deferred, four or five ticks
late with 2.5–3.2 M rows waiting — and the server's peak RSS went from 164 to 668–771 MiB; a quarter
more drains it. And `perf record -e page-faults` found the rest: the server faulted 5.5–5.7 pages a
batch against master's 1.2–1.5, 68% of them in `drain_batch()` writing into blocks it had just
allocated. A sealed block's rows go back to a pool the next drain takes from (`RowBufferPool`), and
every seal writes through one set of column buffers lent to its store in turn, where sixteen stores
had each kept buffers as big as their biggest seal. Six rounds of each, rotating, with two seconds
between runs — before the harness settled the disk, which is why master's own rounds spread here:

| build | levels a second, median (range) | peak RSS | minor faults | flush thread CPU |
|---|---|---|---|---|
| master | 11.56 M (11.18-11.73) | 162-165 MiB | 53 k | 1.26 s |
| the share, of what a tick drained | 11.69 M (10.84-12.13) | 668-771 MiB | 223 k | 1.47 s |
| + the row pool and the lent buffers | 11.94 M (11.15-12.07) | 442-500 MiB | 160 k | 1.40 s |
| + a quarter more | **11.78 M (11.72-11.83)** | **275-299 MiB** | 129 k | 1.38 s |

The last row's rounds are the only ones that did not spread, and it seals about half the segments
master writes at this load (409–423 against 832–848 a run). The pull request's table — the harness
of #164 on a quiet disk, five rounds on the same binary:

| connections, fsync policy | master | part 2a |
|---|---|---|
| 4, the default | 11.79 M (11.13–11.82) | 11.76 M (11.48–11.80) |
| 4, `every` | 586 k (583–588 k); p99 12.96 ms | **594 k (593–598 k); p99 11.13 ms** |
| 1, the default | 6.66 M (6.41–6.71) | 6.70 M (6.67–6.72) |
| 1, `every` | 563 k (562–567 k); p99 3.97 ms | **570 k (568–573 k); p99 3.48 ms** |
| peak RSS, 4 and 1, the default | 166 and 159 MiB | 290 and 245 MiB |

**Level with master under the default policy** — −0.3% and +0.5% on the medians, part 2a's ranges
inside master's — **and ahead under `every`**: 1.2–1.4% more levels a second, 12–14% off p99, and 8%
less server CPU at four connections. At that rate a store takes about eighteen ticks to reach its
65 536 rows, so where master wrote a segment of it every tick, part 2a writes one in eighteen. The
price is memory: the rows waiting in blocks, and the pool that keeps a drain's worth of their
storage, 290 MiB at four connections against 166.

**Four storage tests measured something else once a tick sealed only what was due, and one had never
measured what it said.** A symbol the disk refuses now waits in a readable block, so both symbols
answer after the failed `FLUSH`, each row once, and the disk tells them apart — one segment after
the first flush, one each after the second. The tests of a writer during the tick's segment write
and of rows written during it surviving a crash make their store due by rows (`make_due`); the first
had passed only because the age seal came at about fifteen seconds, inside its fifteen-second
patience. And **the retention test under `every` did not hold the path it was written for**: a tick
that finds nothing owed must move the floor (#160), but the file a rotation leaves makes a ticket
owed under every policy, and the test rotated the WAL about every other tick — with that promotion
removed it passed three runs of three on master. It now rotates the WAL with a store sealed by rows
and then writes single rows, one every 20 ms, into a file with room for all of them. Its first
rewrite failed two runs of three and said why: a block's replay starts where the drain before it
ended, so a row drained with the sealed store's last held the floor inside it.

**Mutation tables: 47 rows, 45 as written down before the run.** Every instrument was green before
and after each table, and each has a control that survives.

The seal epochs, 12 rows, 12 as written; row 5 first came back INVALID — without the stamp the epoch
is unused and `-Werror` refuses the build — so the row was rewritten to stamp 0 and keep the
variable used: the mutation changed, not the code.

| # | the epochs | caught by |
|---|---|---|
| 1 | the first design: the checkpoint claims less, in eight bytes | the epoch-form test, and **the kill test** — the one it was written for |
| 2 | at start a position judges an epoch checkpoint | the rule's tests, and the kill test |
| 3 | a segment sealed after the checkpoint is kept | the rule's test |
| 4 | the epoch starts again at zero after a reopen | the continuity test |
| 5 | a seal stamps no epoch | the epoch-form and continuity tests |
| 6 | `meta.json` keeps no epoch | both engine tests |
| 7 | `meta.json`'s epoch is not read back | the continuity test |
| 8 | the last checkpoint's epoch is dropped | the epoch-form test, and the kill test |
| 9 | the epoch read from the position's bytes | the payload test |
| 10 | the epoch form when nothing waits too | the eight-byte test |
| 11 | control: the epoch's INFO line reworded | survives |
| 12 | every `write_seals()` bumps the epoch, one with nothing to write included | survives: an epoch only has to rise, and a gap is harmless |

Row 3 is the one a kill cannot see: a killed process leaves the segment whole in the page cache, so
keeping it gives the right answer. Its test is the rule's.

The drain and the seal, 11 rows, 10 as written. Row 4's verdict was wrong, and the row is a finding:
`flush_segment()` records a segment's quantities as raw when the store's flag says so **or** the
encoder fell back, and the encoder falls back for exactly the quantities `append()` tests — so the
flag changes no byte, no test can kill the row, and the test came out of `append_block()`.

| # | the drain and the seal | caught by |
|---|---|---|
| 1 | the drain does not lower a block's minimum | the out-of-order range test |
| 2 | the drain does not raise a block's maximum | the out-of-order range test |
| 3 | `append_block()` never goes row by row, whatever period a row is of | the property test |
| 4 | `append_block()` does not test quantities for Simple8b's width | **survives — the verdict was wrong** |
| 5 | `append_block()` does not widen the segment's range by the block's | the property test, the range test |
| 6 | `append_block()` does not count the rows after the first | the property test, four engine tests |
| 7 | `RowBlock::make` ignores the range it is given | the property tests, the range test |
| 8 | control: `reserve_rows()` reserves nothing | survives |
| 9 | control: the drain does not reserve a block to its store's last one | survives |
| 10 | control: a reservation a quiet store no longer needs is kept | survives |
| 11 | the hints are not cleared with the stores | survives: a stale hint sizes a reservation |

The share, 9 of 9; row 1 rewritten once, because its first form left `share` unused and `-Werror`
refused the build. Row 7's test was written for it before the run, when no test there was could see
it.

| # | the share | caught by |
|---|---|---|
| 1 | not applied: every due store sealed in the tick | four policy tests, the spreading test |
| 2 | the oldest due store bound by it too | the oldest-always test, two engine tests, an epoch test |
| 3 | stops at the first store that does not fit | the younger-fits test |
| 4 | no floor: a quiet tick seals what it drained | the quiet-tick test |
| 5 | the rows picked not added up | four policy tests, the spreading test |
| 6 | the tick passes no limit | the spreading test |
| 7 | the tick passes nothing for what it drained | the drained-share test |
| 8 | control: the deferred count in the `DEBUG` line zero | survives |
| 9 | control: a comment reworded | survives |

The pool, the lent buffers and the quarter, 10 of 10. Rows 8 and 9 survive by what they are: memory,
not output — no test reads what a buffer's capacity is. Row 6 is the guard's reason: with it gone
the lent-buffer test's `EXPECT_THROW` fails and the `flush_segment()` after it dies on the buffers
swapped out from under its segment.

| # | the pool, the lent buffers, the quarter | caught by |
|---|---|---|
| 1 | a block's rows not given back | the pool's reuse and query-holds tests |
| 2 | `take()` never takes a spare | the reuse and far-bigger tests |
| 3 | `take()` takes a spare however much bigger than the ask | the far-bigger test |
| 4 | `give_back()` keeps spares past the bound | the bound test |
| 5 | `take(0)` takes a spare | the bound test — tightened for this row before the run |
| 6 | `swap_buffers()` swaps under an active segment | the lent-buffer test, and a crash after it |
| 7 | the share what the tick drained, not a quarter more | two policy tests |
| 8 | a successful seal keeps the lent buffers in its store | survives: memory |
| 9 | a quiet store's block keeps the big buffer it took | survives: memory |
| 10 | control: a comment reworded | survives |

The storage tests, 5 rows and one again: row 1 survived the first rewrite of the retention test,
which is how the test's old gap was found.

| # | mutation | meant for | caught by |
|---|---|---|---|
| 1 | #160: the floor does not move on a tick that finds nothing owed | retention under `every` | **survived the first rewrite**; the second: that test |
| 2 | #160: a failed seal skips the merge of the seals that were written | a symbol the disk refuses | that test |
| 3 | #159: the checkpoint claims the log's position, not the drain's | rows written during the segment write | that test |
| 4 | stage 5 of #151: the seal writes under the engine's lock | a writer during the segment write | that test |
| 5 | control: a comment reworded | — | survives |

**Part 2b is done: the flush tick merges a symbol's small segments, and a crash in any of a merge's
steps keeps each row once.** Eight segments of one merge level in one symbol's hour become one of
the next level, up to 262 144 rows; a segment of 65 536 rows or more — what a seal at the write
ceiling writes — merges with nothing; and an hour that ended a minute ago and has received nothing
since merges what is left into as few segments as fit. A merge moves one step a tick: written into
`<start>_<end>_<n>.compacting` beside its inputs; synced; published by a rename under the index's
lock in place of its inputs, in one step; synced again; and only then are the inputs removed, once
every query that copied them has finished. A tick that drained more than 65 536 rows merges nothing new
unless none has for ten seconds, and a lighter one merges for up to 10 ms. `--compaction off` is
the valve. In part 1's soak on the m9g.xlarge a node held **1 957 – 3 238 segments after twenty
minutes where master held 30 208 – 30 464**, and a cold start answered in **5.9 – 6.8 s rather
than 26.4 – 26.9 s**; the write ceiling and a writer's latency did not move, and what it costs is
a narrow query into history, which reads a bigger segment: 1.8 – 2.3 ms against 0.14 – 0.25.

**The size a merge stops at was measured before it was chosen.** A query reads a segment whole —
price and sequence are delta-encoded from its first row, quantities are Simple8b words that decode
from the first — so a bigger segment is a slower narrow query, and the cap is a trade between the
number of files and what one reads. `benchmarks/segment_size_cost` on the m9g.xlarge, p50 of each of
three runs:

| rows | bytes | one second, every column | ts + price | whole segment | merged from segments of 4 096 | `syncfs()` after |
|---|---|---|---|---|---|---|
| 4 096 | 101 095 | 0.059 ms | 0.013 ms | 0.07 – 0.08 ms | — | — |
| 16 384 | 403 384 | 0.19 ms | 0.057 – 0.059 ms | 0.25 – 0.26 ms | 0.80 – 0.84 ms (49 – 51 ns a row) | 2.5 – 3.4 ms |
| 65 536 | 1 612 521 | 0.79 ms | 0.34 – 0.35 ms | 1.05 – 1.11 ms | 3.1 – 3.2 ms (47 – 49 ns) | 3.7 – 4.5 ms |
| 262 144 | 6 449 083 | 3.2 – 3.5 ms | 1.5 ms | 4.2 – 4.7 ms | 12.3 – 13.4 ms (47 – 51 ns) | 7.5 – 9.3 ms |
| 1 048 576 | 25 795 300 | 14.3 – 14.5 ms | 6.2 – 6.5 ms | 18.8 – 19.8 ms | 52 – 55 ms (50 – 52 ns) | 15.9 – 17.3 ms |

A merge costs about 50 ns a row whatever its size, so the cap is chosen by the query: 262 144 rows
keep a narrow query into history under ~3.5 ms. A segment with chunks and a chunk index would have
both — merges by concatenation, narrow queries reading a chunk — and is a format change with its
own story for going back to an older build; it is not this part.

**A merge must not change the order a scan delivers rows in**, because a `SNAPSHOT` keeps the row
delivered last on a timestamp tie and a `LIMIT` the rows delivered first. So a merge takes only
segments consecutive in their symbol's delivery order — no segment of another hour lies between
them — and publishes only if its range sorts strictly between the segments before and after them,
checked when it is planned and again under the index's lock, because a seal in between can put a
segment in the middle.

**A query that copied an input reads it after the swap**, so its files stay until that query ends.
A scan holds the reader generation current when it copied the index; a publication starts a new
one, and each generation holds the one after it, so the generation a publication retired is gone
exactly when every scan that could have copied its inputs has finished — including scans of older
generations, which could have copied a later publication's inputs too. Retention still removes at
once: those rows are past the window.

**A merge takes a segment of this node's WAL only once a checkpoint on the device vouches for it.**
The merged segment keeps its inputs' highest epoch and position, and a start keeps a segment of
this WAL only if the last checkpoint vouches for its epoch — so merging one sealed after the last
checkpoint known synced would make a merged segment a power cut removes, with the inputs it
replaced already gone. The epoch the last synced checkpoint names moves with the retention floor.

**What a start finds after a crash, it cleans before anything reads it.** A working directory is
removed; a segment that a merged segment beside it names in `compacted_from` is removed — named by
its directory *and* what its own `meta.json` recorded, because a directory is named after its
range, and once an input is gone the next seal with rows over the same range takes its name. A
clean stop leaves neither, and a snapshot's walk leaves both out, because a build before part 2b
takes both for segments and holds their rows twice.

**While a snapshot is being made or sent, nothing merges and retention does not sweep.** The
snapshot pins the segment files from before its flush until its sender finishes the transfer: the
manifest names files, and a merge or a sweep removing one mid-transfer failed the transfer, and
the replica started again. That was true of the sweep before merges existed.

Measured on the m9g.xlarge against master (`3174ec0`, part 2a; the same GCC 14 Release build of
both, each run on a fresh data root on the gp3 volume and started only once the load was under 0.3
with no process not ours running, both read again at the end of every run), on
`16bb1ed`, whose C++ is this pull request's. Part 1's soak — 256 symbols, one 20-level `MINSERT`
each per round, 20 rounds a second — twenty minutes a run, ABBA:

| | master (`3174ec0`) | part 2b |
|---|---|---|
| segments on disk after 20 minutes | 30 208 – 30 464, 25.2 seals a second from the start | **1 957 – 3 238**, between 512 and 3 584 from the second minute on |
| after a clean stop | 30 464 – 30 720 | **2 213 – 3 509** |
| merges a second, over the run | — | 3.2 – 3.4 |
| writer round, p50 / p99, medians of the 80 windows | 0.60 / 0.63 ms | 0.59 – 0.60 / 0.63 – 0.64 ms |
| narrow `SELECT` of the last second, median | 0.26 ms | 0.26 ms |
| narrow `SELECT` of one second a minute into the run | 0.14 – 0.25 ms, from a segment of ten seconds | 0.25 – 0.55 ms to minute 11, from one of 80 seconds; **1.78 – 2.33 ms** after, from one of 640 seconds — 256 000 rows |
| RSS after 20 minutes | 115.5 – 116.3 MiB, 14.6 – 15.0 more than at the second minute | 117.3 – 118.0 MiB, flat from minute 11 |
| restart after a clean stop, warm | 2.77 – 2.81 s | **2.45 – 2.49 s** |
| restart after a clean stop, cold (`drop_caches`) | 26.41 – 26.91 s, 693 – 695 MiB read | **5.89 – 6.83 s**, 456 – 471 MiB read |
| of which the index, cold / warm | 22.47 – 22.96 s / 0.36 – 0.37 s | **1.90 – 2.87 s** / 0.05 – 0.08 s |

**The count follows the data now, not the uptime.** Master's grew by a segment per symbol every ten
seconds, and a start reads each one: 0.74 – 0.75 ms a segment cold, 22.5 – 23.0 s of its 26.4 –
26.9. Part 2b's rose and fell inside the hour — every 80 seconds eight seals of a symbol became one
of 32 000 rows, and at minute 11 eight of those became one of 256 000 — and fell again when the hour
ended: the fourth run crossed 13:00, and in the report window from about 13:01:01 to 13:01:16 the
count went from 3 476 to 768, at 32.7 merges a second — the hour's leftovers, merged once it had
been over for a minute. The index column is read from the start's own log, between the line before
`open_existing()` and the one after it; what is left of a start — 3.8 – 3.9 s cold and 2.4 s warm,
in both builds — is mostly two passes over the WAL, which is #174.

**Memory is within 2.5 MiB of master's at twenty minutes, and its shape is the other way round.**
Master's grows with its segments, about 560 bytes each. Part 2b's rose once, by 13.1 – 13.6 MiB at minute
11, with the first merges of 256 000 rows — a merge writes through the seal buffers and leaves them
the size it needed — and did not move again; a merge is 262 144 rows at most, so that is as far as
it goes.

**What it costs is the query into history.** One second of rows a minute into the run was read in
0.14 – 0.25 ms from master's ten-second segment and in 1.78 – 2.33 ms from part 2b's merged one,
because a query reads a segment whole: the trade `segment_size_cost` measured before the cap was
chosen, and the reason the cap is not higher.

**The write ceiling did not move.** At it a tick drains more than 65 536 rows, and such a tick
merges nothing new unless none has for ten seconds. Medians of five rounds, alternated, 40 000
batches of 64 twenty-level `MINSERT`s a run:

| connections, `--fsync-policy` | master (`3174ec0`) | part 2b |
|---|---|---|
| 4, `interval` | 11.80 M levels/s (11.77 – 11.89) | 11.81 M (11.78 – 11.84) |
| 4, `every` | 582 454 (582 250 – 582 908) | 582 595 (582 306 – 583 383) |
| 1, `interval` | 6.80 M (6.79 – 6.97) | 6.81 M (6.77 – 6.83) |
| 1, `every` | 557 886 (557 209 – 558 118) | 558 207 (558 037 – 558 344) |

A batch's p99 at four connections was 620.1 µs on master and 612.9 on part 2b, at one 197.5 and
197.2.

**An independent review found what the tests had not, twice.** A separate agent read the whole
branch, changing nothing, and reported four real defects: merges under `--fsync-policy none` went
through all five steps with nothing on the device, because the segment sync that policy skips was
counted as run; a symbol named like a working directory was removed with its rows at the next
start; a run whose merged range tied a neighbour was written, refused at publication and written
again every tick; and a start trusted a merged segment's `meta.json` alone. Writing the fixes
found a fifth by test — a partition whose run waited only for the checkpoint of its own tick was
left to its hour's settling. The same agent then reviewed the fixes and found what they introduced
or left: the vouching sync `none` needs still ran at the write ceiling through the heavy tick's
allowance; a failed sync under `none` froze checkpoints that policy never freezes; a partition
could be looked at every tick after a snapshot sealed without a checkpoint; the start removed a
short merged segment that was the only copy of its rows; and the destructor that released the
chain of generations a link at a time peeked at a count that does not order the read. Each has a
test or a row in the table below.

**Found on the way:**

- **A bound resting on a condition several lines away.** `plan()` took up to `kFanIn` segments
  after a check that at least `kFanIn` were there; the mutation that weakened the check made it
  read past the stretch, and the engine test binary died of it (signal 11). The bound stops at the
  stretch's end itself now, and the mutation is a clean kill.
- **A gauge set on one path.** `ob_segments_awaiting_removal` was set only where inputs were
  removed, so the tick that retired them left it at zero for a tick.
- **A look is not free.** A tick at the write ceiling that found nothing to merge looked again the
  next tick, copying a partition's segments each time; it looks once every ten seconds now.
- **A write acknowledged is not yet a row a `SELECT` returns** — measured writing the tests: a
  `SELECT` from another connection straight after a `MINSERT` returned nothing, and `BOOK` the five
  levels, five times of five. Documented in `docs/cli.md`; a test counts what was flushed.
- **#176**: a mesh snapshot cannot carry more than 65 534 files.

**Mutation table: 62 rows, 62 as written down before each run** — 41 killed, 21 survived where the
verdict said they would; one verdict (row 43) was written as uncertain, depending on the order the
directory walk meets two merged segments, and came out killed, and one row (52) first came back
INVALID — `-Werror` refused a mutation that left a parameter unused — and was rewritten to keep it
used: the mutation changed, not the code. The table ran in four passes, each written before it ran:
rows 1–32 against the first complete tree, rows 33–37 for the stop's cleanup and the snapshot's
exclusion that the first pass led to, rows 38–51 for the fixes an independent review of the branch
found, and rows 52–62 for the fixes the review of those fixes found. Every instrument was green
before and after each pass, and the restored tree too. Most of the 21 that survive say why in the
row: a power cut, the fault injector, or cost alone — each a property no test here can reach.

| # | mutation | caught by |
|---|---|---|
| 1 | the swap does not check that the inputs are consecutive | the store's refusal test |
| 2 | nor that the merged range sorts where the inputs were | the store's refusal test |
| 3 | the swap hands back a generation nobody holds | three reader tests |
| 4 | a generation does not hold the one after it | the younger-generation test |
| 5 | a scan takes no generation | three reader tests |
| 6 | a rebuild leaves a working directory | the rebuild test |
| 7 | a rebuild descends into one | the rebuild test |
| 8 | a rebuild takes an input by its name alone | the name-reuse test |
| 9 | a rebuild removes no input | the rebuild test |
| 10 | a merge's read pads a short column | the short-column test |
| 11 | a merged segment's last row is the row appended last | the lineage test |
| 12 | a partition's view holds only its members | the view test |
| 13 | runs ignore the level | the level test |
| 14 | one short of the fan-in merges | the fan-in test (and a read past the stretch: row 36) |
| 15 | a full segment merges | the full-segment test and the property |
| 16 | runs ignore the WAL identity | the identity test and the property |
| 17 | a segment of this WAL merges whatever the checkpoint on the device vouches for | the vouching test |
| 18 | a partition settles without being quiet | the settling test and three engine tests |
| 19 | the step ignores a snapshot's pin | the pin test |
| 20 | the retention sweep ignores a snapshot's pin | the pin test |
| 21 | inputs removed while a scan reads them | the engine's reader test |
| 22 | inputs removed before the sync after the publication | survives: a power cut only |
| 23 | a merge published before the sync after its write | survives: a power cut only |
| 24 | the step never syncs on its own | seven engine tests |
| 25 | a merged segment's level is its inputs' | the eight-seals test |
| 26 | a merged segment's position is its first input's | the latest-of-its-inputs test |
| 27 | its epoch is its first input's | the latest-of-its-inputs test |
| 28 | a discarded store's retired inputs are kept | the discard test |
| 29 | a seal does not note its partition | seven engine tests |
| 30 | a tick at the ceiling merges like a light one | survives: measured instead |
| 31 | the step goes on after the checkpoints froze | survives: needs the fault injector |
| 32 | control: a comment reworded | survives |
| 33 | close() leaves what merges wrote | the clean-stop test |
| 34 | a snapshot walks replaced inputs | the snapshot test |
| 35 | close() removes inputs before the sync after their publication | survives: a power cut only |
| 36 | row 14 again, with the bound at the stretch's end | the fan-in test and two engine tests, no crash |
| 37 | the bound without the stretch's end | survives: equivalent before a partition settles |
| 38 | a run taken whatever its merged range ties | the tie test and the property |
| 39 | a settled merge takes any number of inputs | the input-cap test |
| 40 | a working directory recognised at any depth | the name test |
| 41 | any name ending in `.compacting` is one | the name test |
| 42 | a start trusts a merged segment short of its rows | the torn-merge test |
| 43 | one fingerprint per path | the two-merges-one-path test (uncertain verdict; killed) |
| 44 | inputs told apart before the range repair | the repaired-range test |
| 45 | the chain of generations released recursively | survives: no chain long enough to exhaust a stack |
| 46 | a full seal wakes its partition | survives: cost only |
| 47 | a partition waiting only for vouching left to its settling | the fan-in engine test |
| 48 | under `none` a seal's sync is counted | survives: a power cut only |
| 49 | under `none` the appended checkpoint taken as durable | survives: a power cut only |
| 50 | working directories not numbered | survives: runs of one range are no longer planned |
| 51 | under `none` the vouching sync every tick | survives: cost only |
| 52 | a partition waits for vouching whatever checkpoint was appended | survives: cost only |
| 53 | ...whatever its segment's size | survives: cost only |
| 54 | under `none` a tick at the write ceiling runs the step | survives: cost only |
| 55 | under `none` the vouching sync without a look that wanted it | survives: cost only |
| 56 | a short merged segment removed whatever its inputs | the gone-inputs test |
| 57 | a short merged segment kept although every input is there | the torn-merge test |
| 58 | the chain of generations released by recursion | the million-link test (the binary dies) |
| 59 | a refused rename not backed off | survives: nothing here makes a rename fail |
| 60 | under `none` a failed WAL sync not seen by merges | survives: needs the fault injector |
| 61 | under `none` a failed merge sync freezes the checkpoints | survives: needs the fault injector |
| 62 | close() removes inputs after merging stopped | survives: needs a failed sync |

- Effort: L | Impact: part 1 closed the slope from writes and queries; part 2a took the count to
  about one segment per active symbol per ten seconds at a trickle and half of master's at the write
  ceiling, and a cold start from minutes to seconds; merging the segments that are written is part
  2b

### 164. The flush tick synced, drained and deleted under the engine's lock, and a rotation synced its whole file on a writer's thread ✅

**Stage 5 of #151: the flush tick's I/O out of the lock every writer needs.** Once stage 4 made a
node use every core by default (#158), the longest things the engine did while holding `mtx_` were
the flush thread's, once a tick: the WAL `fsync` — 7.9 ms at p50 and 25.8 ms at worst on the
m9g.xlarge, measured with `perf trace` — the drain of a tick's rows into their stores, about 640 000
of them at one pipelining connection, and the unlinks of WAL truncation and of the TTL sweep. One
more was a writer's own: the writer whose record crossed `--wal-rotate-bytes` synced the file it was
leaving, on its thread and under the lock — under `--fsync-policy none`, which syncs nothing else,
up to 512 MB at once, measured at 295 and 323 ms. A pipelining writer's batch waited 21-30 ms at
p99.9 once per tick while its p99 said 0.2-0.7 ms: a stall that comes once per interval touches one
batch in several hundred, so `benchmarks/pipelined_ingest.cpp` prints p99.9 and the maximum now.

**What the tick holds `mtx_` for now is bookkeeping** (`docs/architecture.md`, "The flush tick"):

- **The sync is a ticket in three steps** (`WALWriter::SyncTicket`). Under `mtx_` the tick takes it —
  a `dup()` of the current descriptor, the records a sync would cover, the position they end at,
  and the files a rotation has left since the last tick — together with every row queued so far.
  Every row taken has its record before that position, because a writer appends the record and
  queues the row under one hold of `mtx_`. Without the lock it performs the `fsync`s; under it
  again it accounts for them. The duplicate names the same open file, so a rotation in between does
  not move the sync to the wrong file or close its descriptor from under it (#128's shape); a `dup()`
  that fails syncs under the lock, as the tick used to. A sync that fails puts the rows back in
  front of what was written meanwhile and throws for #112's boundary to count: draining them would
  move them out of the only place that still knows they were never synced.
- **The queue is a FIFO of fixed-size chunks** (`ChunkedQueue<PendingRow, 4096>`, its own header
  and its own unit test). Appending never reallocates or moves a row, so nothing copies the queue
  under the lock when it outgrows a capacity, and taking every row queued so far is moving a vector
  of chunk pointers. The drain runs without `mtx_`, under `flush_mtx_`, which every mutator of the
  stores holds, and each chunk is cleared, given back and released from the ceiling as soon as its
  rows are in their stores — so a writer at the ceiling waits for a chunk, not for the tick. A store
  the drain creates is the one insertion into the store map and takes `mtx_` for it, because
  `holds_no_data()` reads the map under `mtx_` alone. Everything that counts the queue — the room a
  writer waits for, `STATUS`, `holds_no_data()` and `ob_pending_rows` — counts the rows being
  drained too, so the bound stays one ceiling and nothing reads as empty for the length of an
  `fsync`.
- **A rotation leaves its file to the tick.** `open_current()` puts the descriptor on a list the
  next ticket carries instead of syncing it; `flush()`, the destructor and a ticket dropped before it
  was performed sync and close what is on the list, under every policy, because a promise that the
  log is synced to here which skipped the file just left would skip exactly the records written last
  before the rotation. Past four files waiting, the writer syncs the oldest itself, as it always did:
  a node whose ticks are minutes apart would otherwise hold a descriptor and up to 512 MB of unsynced
  log per rotation in between.
- **WAL truncation and the TTL sweep decide under `mtx_` and delete without it.** A file the number
  allows cannot stop being allowed once the lock is released: the floor only rises, and only under
  `flush_mtx_`, which the tick holds throughout.

Only the tick moved. `FLUSH`, `close()` and snapshot creation drain under the lock as they did,
straight after a sync under the same hold.

**Measured with the binaries that ship it** — master after #163 against this branch, on the
m9g.xlarge (four Graviton cores shared with the load generator), loopback, GCC 14 Release, data on
EBS; five interleaved rounds of 40 000 batches of 64 twenty-level `MINSERT`s
(`scripts/measure_pipelined_ingest.py`), medians, and a batch's latency in µs:

| connections, policy | build | levels / s | p50 | p99 | p99.9 | max | peak RSS |
|---|---|---|---|---|---|---|---|
| 1, `interval` (default) | before | 6 191 693 | 171.2 | 194.6 | 22 714 | 31 523 | 163 MiB |
| | **after** | **6 730 445** | 174.4 | 197.6 | **10 417** | **16 538** | 158 MiB |
| 1, `every` | before | 559 198 | 2 253.5 | 4 497.6 | 7 412 | 20 316 | 25 MiB |
| | after | 563 394 | 2 258.3 | 3 949.2 | 6 960 | 15 354 | 24 MiB |
| 1, `none` | before | 6 240 030 | 171.8 | 195.0 | 9 969 | 308 917 | 138 MiB |
| | **after** | **6 738 413** | 174.1 | 197.5 | **219** | 299 210 | 155 MiB |
| 4, `interval` | before | 10 540 230 | 301.8 | 540.0 | 46 425 | 61 081 | 157 MiB |
| | after | 10 397 988 | 320.0 | 605.3 | 43 391 | 58 677 | 160 MiB |
| 4, `every` | before | 578 955 | 8 740.4 | 17 259.3 | 19 578 | 30 115 | 32 MiB |
| | after | 581 750 | 8 765.3 | 12 901.1 | 15 351 | 26 292 | 34 MiB |
| 4, `none` | before | 11 618 774 | 300.6 | 712.8 | 13 371 | 324 471 | 164 MiB |
| | **after** | **13 420 237** | 314.0 | 552.3 | **923** | 276 746 | 164 MiB |

It says a different thing per row, so read it that way:

- **One connection: +8.7% under the default and +8.0% under `none`**, with the once-a-tick stall
  halved under `interval` (22.7 ms at p99.9 → 10.4) and gone under `none` (10.0 ms → 0.22).
- **Four connections under the default: no change the rounds can tell apart** — −1.3% on the median,
  with the build before this one spread over 10.18-11.68 M levels a second and this one over
  10.31-10.49 M. Why is below, and it is the most useful thing this stage measured.
- **Four connections under `none`: +15.5%**, with p99.9 14× lower.
- **`every`: the same throughput**, as it must be — every write syncs itself, so nothing is owed at
  the tick — **and 12-25% off its p99.** Once writes flow the ticket is never owed under that
  policy, so the only things this change took out of the lock on that path are the drain and the
  deletes, which no longer sit between two writers' syncs. That is read from the code, not traced.
- **p50 is 2% higher at one connection and 6% at four, and the server's CPU 3% higher for the same
  work.** The drain now runs while writers write, on the same cores, where it used to stop them, and
  handing chunks back is work a swap did not do. That is the price of the tails above.
- **Resident memory is where it was.** The first half of this stage, which only took the sync out of
  the lock, swapped two vectors every tick, and each kept the capacity of the largest tick it had
  held: 163 → **270 MiB** at one connection and 158 → **321** at four, which doubles the memory the
  million-row ceiling promises. It was not merged. The chunked queue allocates only when no spare
  chunk is left, so the queue, the batch being drained and the spares together stay within the
  ceiling plus three chunks.

**The prediction was written before the second measurement, and three of its six lines missed:**

| predicted | measured |
|---|---|
| one connection, `interval`: p99.9 under 2 ms, max under 10 | 10.4 and 16.5 ms — **missed** |
| four connections, `interval`: p99.9 8-15 ms, throughput +0-10% | 43.4 ms and −1.3% — **missed** |
| `none`: max under 20 ms at both connection counts | 299 and 277 ms — **missed** |
| four connections, `none`: p99.9 under 5 ms | 0.92 ms |
| `every`: unchanged within noise | the same throughput, tails lower |
| peak RSS back to 158-169 MiB | 155-164 MiB |

**All three misses have one cause, and it is not the lock.** A diagnostic run at one connection
traced every server syscall longer than 2 ms, the kernel's dirty-page throttling
(`writeback:balance_dirty_pages`) and the page cache's dirty count every 100 ms:

- **Under `interval` the writer meets the ceiling once a flush cycle.** At 6.6 M levels a second it
  queues about 660 000 rows per 100 ms, and one cycle — sync, drain, segment writes, one `syncfs()` —
  took about 150 ms: the log counted **51 waits for room in the 7.8-second run, 153 ms apart**, one
  write each. No syscall on a writer's thread took over 2 ms; the long ones were all the flush
  thread's — WAL `fsync`s of up to 32.0 ms (a median of 12.2 among the 35 over 2 ms) and `syncfs()`s
  of up to 66.9 ms (21.3 among 53) — and there was **no dirty-page throttling at all**. The prediction assumed a writer at the
  ceiling waits for one chunk; it waits for the next drain to reach one, and that is behind the rest
  of the cycle.
- **At four connections the ceiling binds throughout**, and a third build says so: this branch with
  only its drain put back under the lock — row 2 of the table below, built as a server — measured
  **10 355 986** levels a second and 42.3 ms at p99.9, against 10 298 213 and 43.5 for this branch
  and 10 437 097 and 45.8 for master in the same five rounds. Three builds, one number. What bounds
  four writers is how many rows a flush cycle moves, not which lock it holds while it moves them.
- **Under `none` the maximum is the rotation's `fsync`, which this change moved and could not
  shorten**: 317 and 477 ms in the diagnostic run, one for each of its two rotations, now on the flush
  thread. The tick holds the batch it took through that sync, so the rest of the ceiling
  fills in tens of milliseconds and a writer waits out the remainder: **two waits for room in the
  run, one per rotation.** What this stage bought under `none` is everything between rotations.

So the next ceiling on the write path is the flush cycle's capacity — the drain and the segment I/O,
one after the other, on one thread — and it is a stage of its own rather than something to tune here.
Two things were found on the way: the segment count that cycle leaves behind it, **#165**, and the
time range each of those segments claims, **#166**.

**The second measurement found a regression of this stage's own.** Its first version released the
ceiling under `mtx_` once per chunk — about 250 acquisitions a tick — and at four writers each one
queued behind them, so the drain fell behind and the queue reached the ceiling: **10.6 → 9.0 M levels
a second**, p50 302 → 335 µs. Under `every`, where a writer holds `mtx_` through its own `fsync`
(8.7 ms at p50), each give-back waited that long and the queue filled the chunk pool: peak RSS
**32 → 157 MiB**. Taking work out of a lock and then taking the lock once per piece of it can cost
more than leaving it in. The spare chunks have a lock of their own now, which a writer takes once in
4096 rows; the detached count is an atomic lowered without `mtx_`; and the per-chunk notification
goes out unlocked — a writer that misses one between its check and its wait is woken by the next, or
by the last, which is sent after taking `mtx_`.

**And the first half found two things before it was set aside.** The crash test it asked for failed
against the build before it as well — the tick's checkpoint claimed every record written before it,
including those whose rows were queued after the drain — which is **#159**, older than this stage and
fixed on its own first. And promoting the retention floor only on the path that performed a sync
stopped retention under `every` while writes arrived, because under that policy the ticket is never
owed once they do; `test_retention_moves_under_every_while_writes_flow` holds that now.

**Tests.** Every change has its crash test in the same run, through #54's injector, because
reordering I/O against a lock is exactly what breaks only under a crash — and the injector gained a
mode that makes a chosen `fsync` or `write` **slow** rather than failing (`OB_FAULT_DELAY_MS`):

- a writer does not wait for the tick's sync — ten writes timed while that sync is made to last three
  seconds took **2.988 s** against master and **0.006 s** against this branch — nor for a segment the
  tick's drain rolls over, nor for the sync of a file a rotation left;
- rows written during a tick's sync, and during its drain, come back through a crash after it;
- a failed tick sync drains nothing, and the next tick drains it all;
- retention moves under `every` while writes flow;
- a failed sync of a file the WAL rotated away from costs no acknowledged write — #153's test, moved
  to where that sync happens now;
- a drain the disk stops part-way puts back every chunk after it: the refused rollover is the 5001st
  of 9200 rows, in the second chunk, with 1008 rows in a third.

Against master's server the timing tests fail as they should — ten writes waited 2.99-3.00 s behind
the tick's sync, a rollover in its drain and a rotated file's sync — and #153's test catches the
rotation syncing the file itself. In C++: nine `ChunkedQueue` tests, one of them a property test
that the queue behaves as a deque and stays within its bound; ten `WalSyncTicket` tests; two
static ones — performing a ticket touches nothing of the writer's but its failure count, which is
what makes it callable unlocked, and a ticket dropped unperformed reaches the writer only through
that count — and one that pins where the tick's sync, drain and deletes run.

**Mutation table: twenty-four rows, twenty-four with the verdict they were supposed to produce** —
nineteen killed and five surviving, two of those controls — against committed code, the eight
instruments green before the first row and after the last, and the sources restored byte for byte.

| # | Mutation | Instrument | Verdict |
|---|---|---|---|
| 1s | the tick's sync back under the lock | where the tick runs what (static) | killed |
| 1i | the same | a writer timed against a 3 s sync | killed: ten writes took 2.99 s |
| 2s | the tick's drain back under the lock | where the tick runs what | killed |
| 2i | the same | a writer timed against a 3 s rollover in the drain | killed: 2.99 s |
| 3 | the ceiling released at the end of the drain rather than per chunk | the same | survives, as written down |
| 4u | the rotation syncs and closes the file it leaves itself | the ticket's tests | killed |
| 4i | the same | a writer timed against a 3 s sync of the left file | killed: 3.00 s |
| 4r | the same | #153's test | killed: the left file was synced before anything asked |
| 5u | `flush()` leaves the files a rotation left alone | the ticket's tests | killed |
| 5r | the same | #153's test | killed: the `FLUSH` whose sync failed answered `OK` |
| 6u | a ticket does not carry the files a rotation left | the ticket's tests | killed |
| 6i | the same | a writer timed against the left file's sync | killed: nothing synced the file |
| 7 | no bound on the files left for a tick | the ticket's tests | killed |
| 8 | a dropped ticket closes its files without syncing them | the ticket's tests | killed |
| 9 | what a batch did not consume goes behind what was queued since | the queue's tests | killed |
| 10 | a chunk given back is kept whatever the bound | the queue's tests | killed |
| 10b | the spare chunks shared without their lock | the queue's tests | survives, as written down |
| 11 | the last elements start one late | the queue's tests | killed |
| 12 | a stopped drain puts back only the chunk it stopped in | the multi-chunk drain test | killed: 8192 rows of 9200 |
| 13 | a new store inserted without `mtx_` | a writer timed against a 3 s rollover in the drain | survives, as written down |
| 14 | WAL truncation back under the lock | where the tick runs what | killed |
| 15 | the TTL sweep back under the lock | where the tick runs what | killed |
| 16 | control: the stopped drain's warning reworded | the multi-chunk drain test | survives |
| 17 | control: the left file's debug line reworded | a writer timed against the left file's sync | survives |

**The three written down to survive, and why that verdict is right rather than a gap.** Row 3 moves
*when* room comes back, not whether, and the test that could kill it would be a gate on a clock at
the ceiling — it is what the second measurement above measured. Rows 10b and 13 are data races, one
between the drain handing a chunk back and a writer taking one, one between the drain creating a
store and `holds_no_data()` reading the map: no single-threaded test can see either, and the
instrument that can is ThreadSanitizer with both threads running, which is the TSan jobs' to run
rather than this table's. **Row 12 is the row writing the verdicts down first found**: #161's test
drains three rows, all in one chunk, so it could not tell "the rest of the batch" from "the rest of
this chunk", and nothing killed a drain that dropped every chunk after the one it stopped in. The
multi-chunk test exists because of it — and it counts rows by reading them back, because an aggregate
is computed over the live book, not over what is stored.

- Effort: L | Impact: the flush tick no longer stops every writer once an interval — one connection
  writes 8-9% more and waits half as long at p99.9 under the default, a node under `none` waits only
  at a rotation, and the ceiling's memory is what it was

### 163. `--ttl-hours` measured event times against the time since boot: it deleted every segment, or none ✅ **P0**

**Found while reading the flush tick for stage 5 of #151**, in the retention block under the drain.
The sweep's cutoff was `steady_clock::now()` minus the retention, and `steady_clock` counts from the
machine's boot, while a segment's times are event times - nanoseconds since the Unix epoch, whether
the server stamped a row on arrival or the client gave its time (#105). Two failures from one line,
and which one a node got depended on how long its machine had been up.

**Up for less than the retention, the subtraction wrapped** to a cutoff past every timestamp, and
the first sweep - which runs at the first tick - deleted every segment. Measured on the i3-7100U, up
21.5 hours: a node that had flushed 200 rows, restarted with `--ttl-hours 24`, held **0 of 200 rows
and none of their two segment directories** after its first sweep; the same restart without the
flag held all 200. Its log said `age=4626822.4h` of a segment written seconds before. **Up for
longer, the cutoff was a few hours into 1970** and nothing ever expired: rows dated two hours ago
under `--ttl-hours 1` were all still there after the sweeps. So a node restarted soon after its
machine booted lost its store, and one running for longer than its retention kept everything for
ever. Every unit test passed, because each one handed `delete_expired_segments()` a cutoff it had
computed itself; nothing ran the sweep through an engine.

**The cutoff is the wall clock now, and it saturates.** `ttl_cutoff_ns(wall_clock_ns(), ttl_hours)`
next to `TTLConfig`: 0 - nothing expired - for a retention of 0, which is the flag's "keep
everything", and for one reaching back past the epoch; and no product that can overflow, because the
flag takes any `uint64_t` and 5 124 096 hours is where its nanoseconds stop fitting. The sweep's
**cadence** stays on the monotonic clock, as a `time_point`, so the two cannot be compared again: a
stepped wall clock moves what has expired - it is the clock the rows are stamped with - and not how
often the sweep runs. The retention line now says how far past the retention a segment was, which is
what it had always computed and called the segment's age.

**The same shape three more times**, found by sweeping the tree for it rather than by reading:
an anti-entropy run's time, a conflict's time whose comment said "wall clock", and a snapshot
manifest's creation time. None of them decided anything, and each would have the day something read
it. `tests/test_clock_use.cpp` now finds every `time_since_epoch()` in `src/`, `include/` and
`tools/` - the one way to turn a clock reading into a count - and accepts it on a
`system_clock::now()` reading, or at one of two listed sites with the reason its number is an
interval (a log line's milliseconds, and the mesh's grace-window clock); checked both ways, with a
snippet in which it must flag four and pass four. It shares its scanner with #162's durability rule
(`tests/source_scan.hpp`), so the two cannot come to disagree about what counts as code.

**Tests.** Three engine tests in `test_ttl_retention.cpp` restart an engine on a directory that
holds rows older and younger than the retention: one with a retention taken from `/proc/uptime` so
it is longer than the machine has been up - the premise under which the old sweep deleted
everything, on any machine rather than on the one it was written on - one at an hour, and one with
an interval of 31 years, because the first sweep runs at the first tick and counting its interval
from boot instead would have delayed it by the rest of it. `test_ttl.py` does the measured restart
through the real flags. Against the build before this fix: **0 of 100** rows written a moment ago
survived a 23-hour retention on a machine up 21.8 hours.

**Mutation table: eighteen rows, eighteen with the verdict they were supposed to produce** -
fifteen killed, one refused by the compiler and two controls - against committed code, the three
instruments green before the first row and after the last, and the sources restored byte for byte.

| # | Mutation | Instrument | Verdict |
|---|---|---|---|
| 0u | the sweep exactly as it was: the boot clock and a subtraction that wraps | engine sweep tests | killed |
| 0n | the same | restarted node | killed: 0 of 100 fresh rows survived |
| 1u | the boot clock through today's saturating cutoff | engine sweep tests | killed: nothing expires |
| 1n | the same | restarted node | killed: no sweep deleted anything |
| 1s | the same | clock rule | killed |
| 2 | no saturation when the retention reaches past the epoch | cutoff tests | killed |
| 3 | a retention of 0 expires everything older than now | compiler | refused by a `static_assert` |
| 4 | the saturation tested with a product that can overflow | cutoff tests | killed |
| 5 | the first sweep waits an interval counted from boot | engine sweep tests | killed |
| 6 | an anti-entropy run's time back on the boot clock | clock rule | killed |
| 7 | a conflict's time back on the boot clock | clock rule | killed |
| 8 | a symbol snapshot manifest's time back on the boot clock | clock rule | killed |
| 9 | the rule's list loses the log line's site | clock rule | killed |
| 10 | the rule's list keeps a count the tree no longer has | clock rule | killed |
| 11 | the scanner takes any clock's `now()` for the wall clock | clock rule | killed |
| 12 | the shared scanner reads line comments as code | clock rule | killed |
| 13 | control: the sweep's debug line reworded | engine sweep tests | survives |
| 14 | control: the retention line reworded | restarted node | survives |

**Row 4 survived the first run, and that is the row worth reading.** The only overflowing
retention the tests used was `UINT64_MAX`, whose product happens to wrap to a number larger than any
wall clock - so a check that multiplied before comparing passed. The first retention whose
nanoseconds do not fit, 5 124 096 hours, wraps to about 25 minutes, and that check would read a
retention of 584 years as one of 25 minutes. It is pinned now, and the property test draws
retentions around that threshold instead of leaving them to an arbitrary 64-bit draw. **Row 5** is
the one writing the verdicts down first found: nothing killed it until the 31-year interval.

- Effort: S | Impact: a node restarted with `--ttl-hours` longer than its machine's uptime deleted
  its whole store at the first sweep, and one running longer than its retention never expired
  anything

### 162. A replica rewrites its state file in place, and nothing after a snapshot install is synced ✅

**Found while closing #160, by asking what else recovery reads that only the WAL was ever synced
beside.** Two halves, and they had to land together.

**The replica's position file was rewritten in place.** `ReplicationClient::save_state()` opened
`repl_state.txt` with `O_TRUNC` and wrote into it, so a replica killed between the truncate and the
write left an **empty** file - which `load_state()` reads as a position nothing can attribute, that
is as "wipe the store and stream the whole log again" (#101), and which forgets the epoch #103 made
that file the only keeper of. The save runs every ten seconds, so the window needs no power cut,
only a kill at the wrong moment. **Measured with the write held open at the kernel** (`strace -P
repl_state.txt -e inject=write:delay_enter=8000000` on a static primary and replica; the
`LD_PRELOAD` injector cannot see it, because glibc's `fclose` writes through its internal `__write`
and not the `write` symbol): killed inside the hold, the file came back **0 bytes**, and the restart
logged `clearing local data` and asked for `REPLICATE 0 0 0`. The same kill without the hold kept its
69 bytes and resumed. Now the file goes through `write_file_atomically()`, and the same measurement
lands the kill in the temporary's write: the file keeps its 69 bytes and the restart resumes.

**An installed snapshot was never synced before its position was recorded.** The install renames
the staged files into the data directory; the replica then saves the snapshot's WAL position and,
after a restart, resumes from it, so nothing asks for the snapshot's rows again. **Measured with a
real power cut** (a replica on dm-flakey bootstrapped by a snapshot because retention had deleted
the file it asked for, and cut as soon as its state file named the snapshot's position): with the
position made durable by the first half and the install unsynced, the replica answered **0 of 1100
rows**. The build before either half answered all 1100 - **by accident**: its position file did not
survive the cut either, so it bootstrapped again. Fixing only the first half would have been worse
than fixing nothing: a durable pointer to data that is not. `Engine::install_snapshot()` now ends
with one `syncfs()` on the data directory, without `mtx_`, and a failure there is a failed sync like
any other: it freezes the checkpoints (#160) and reports the install failed, so the position is not
recorded and the next bootstrap starts again. `snapshot_manifest.json` goes through
`write_file_atomically()` too, one writer at a time; its temporary and rename were never synced.

**Every open for writing now says what makes it durable** - `tests/test_durable_writes.cpp` finds
each one in `src/`, `include/` and `tools/` and requires, within four lines above it, an
`OB_DURABLE:` comment naming what syncs its bytes, or the file goes through
`write_file_atomically()` and opens nothing at its call site. Checked both ways, with comments and
string literals blanked, and a snippet in which the scan must find five sites and ignore four. Six
sites are marked: the WAL, segment files, the atomic writer itself, both snapshot stagings and the
cross-device install copy. The rule is about the next file somebody writes beside the WAL, which is
how all three of #160's and #162's were made.

**Tests.** `test_replica_restart.py`: a replica restarted with the injector holding its first save's
write, killed inside it, and restarted, keeps its position and its store; against the stdio writer
its premise fails, because the injector sees nothing to hold. `test_power_cut.py`: the snapshot cut
above. Its first version waited for the state file to name a stream - which the replica writes at
position zero **before** asking for anything - so the cut landed before the install, the second
bootstrap healed it, and the test passed with the install's sync removed. `test_storage_faults.py`:
a fresh replica's first `syncfs()` is its install's, because a node with nothing to flush syncs
nothing, so failing it is deterministic: the install is reported failed, the position is not saved,
the checkpoints freeze, and after the five-second reconnect backoff the snapshot is installed again.
Its first version waited for the rows and counted one install - the first install had already
replaced the store, so every row was readable before any position was recorded.

**Mutation table: eighteen rows, eighteen with the verdict they were supposed to produce** -
fifteen killed, two controls and one documented survivor - against committed code, the four
instruments green before the first row and after the last, and the sources restored byte for byte.

| # | Mutation | Instrument | Verdict |
|---|---|---|---|
| 1 | the state file rewritten in place, with raw writes (the defect's shape) | kill during the save | killed: the file came back empty |
| 1s | the same | static test | killed |
| 2 | the state file written through stdio in place (the code before #162) | kill during the save | killed at its premise: the injector sees nothing to hold |
| 2s | the same | static test | killed |
| 3 | no sync after a snapshot install | snapshot under a power cut | killed: 0 of 1100 rows |
| 3f | the same | failed install sync | killed: there is no sync to fail |
| 4 | an install whose sync failed reported as installed | failed install sync | killed |
| 5 | an install whose sync failed freezes nothing | failed install sync | killed |
| 6 | the manifest written in place through an `ofstream` | static test | killed |
| 7 | the manifest writer without its mutex | static test | survives, documented |
| 8 | the WAL's durability marker removed | static test | killed |
| 9 | a durability marker with no open under it | static test | killed |
| 10 | the scan reads string literals as code | static test | killed |
| 11 | the scan reads line comments as code | static test | killed |
| 12 | every `::open` counted, whatever its flags | static test | killed |
| 13 | every `fopen` counted, whatever its mode | static test | killed |
| 14 | control: the save's warning reworded | kill during the save | survives |
| 15 | control: the install's error reworded after the part the test reads | failed install sync | survives |

**Row 7 survives, and stays that way on purpose.** Two snapshot creations writing the manifest at
the same moment is a race of microseconds inside milliseconds of work, and a test that tried to
reach it would pass most of the time, which is worse than none. The mutex is there because
`write_file_atomically()`'s temporary has one name and this function's callers run on four threads -
an argument from the code, made where the code is. **Row 2 is the one worth reading twice**: the
regression test cannot show the stdio writer's defect, only that it cannot measure it, which is why
the defect itself was measured with `strace` and the number above comes from that.

- Effort: S | Impact: a replica killed at the wrong moment wiped its store and re-streamed the whole
  log; after a power cut, a replica could serve a snapshot the device never received

### 161. A full disk during a flush stored every price as zero, and answered `OK` ✅

**Found writing #160's tests, by injecting the failure #54's injector already could and nobody had
aimed at a segment.** `flush_segment()` wrote its seven column files and `meta.json` through
`std::ofstream`, and nothing read the stream's state. **Measured on the build before the fix**, with
`ENOSPC` on the first `price.col` write: `FLUSH` answered **`OK`**, `price.col` was **0 bytes**
beside a complete `meta.json`, and `SELECT` returned both rows **with price 0** - before a restart
and after one, because the checkpoint appended over them told replay they were stored. Silent,
permanent, and wrong in the one column a trader reads first.

**Now every segment file is a checked write** (`write_file_checked()`: a raw `write()` loop, and the
`close()` result read as well, since NFS defers write errors to it), and a refusal is an exception
the flush turns into a failed flush. Four things had to follow, or the fix would have moved the
damage rather than removed it:

- **a partly written segment removes its own directory**, so a later scan cannot mistake litter for
  data (`meta.json` is written last, so a directory with one is a directory with every column);
- **a store whose segment fails keeps its rows in memory and the others are written and merged**. An
  exception out of phase B's loop skipped the merge, so segments already written for other symbols
  were on disk and in no index - rows no query returned until a restart found them;
- **a drain stopped by a rollover's refused write keeps queued only the rows it had not appended.**
  A rollover writes the segment from inside `append()`, inside the drain, and without this the next
  drain appended the rows before the failure a second time, into storage that never removes one;
- **`close()` reports instead of throwing**, since it runs from the destructor.

The flush that failed writes no checkpoint (#160), so after a restart its rows come back from the
WAL: measured with the same injection, `FLUSH` answers `ERR ... price.col: No space left on device`,
and after a kill and restart `SELECT` returns 111 and 112.

- Effort: S | Impact: **P0** - a full disk corrupted acknowledged rows silently and permanently

### 160. Segment files were never synced, so a power cut after a flush lost the rows a synced checkpoint claimed ✅

**Found by reading while writing #159, then measured.** `src/columnar_store.cpp` wrote every segment
file and synced none of them, nor the directories holding them, while the checkpoint claiming them is
a WAL record, synced by the next write under `every` and the next tick under `interval`. So after a
power cut the log could say "these records are in segments" about files the disk never received.

**Measured with a power cut the kernel performs** (`scripts/power_cut.sh`, and now
`tests/integration/test_power_cut.py`: dm-flakey over a loop device, switched to drop every write
at the chosen moment, the way xfstests simulate one; ext4). One flush, one more synced write, the
cut: all eight segment files came back **zero bytes** under a whole WAL, and the node answered
**1 row of 201**. With a second flush after the last write - so every row is synced in the WAL under
either policy - the build before this change answered **0 of 201 under `every` and under
`interval`**: no segment survived, and the last checkpoint covered every record. The control, the
same kill, unmount and remount without the cut, answered 201 of 201 each time. `wal_identity` came
back empty as well, which makes every position the segments recorded foreign.

**This is not a process-crash problem**, which is why nothing caught it: a killed process leaves the
page cache to the kernel, and every other crash test in this repository kills. It broke `interval`'s
promise as well as `every`'s - that policy loses at most a flush interval of the WAL, and a synced
checkpoint over unsynced segments could lose any amount.

**The fix, and the measurement that chose it.** One `syncfs()` on the data directory per flush,
outside `mtx_`, before the checkpoint - chosen over an `fsync` per file on the m9g.xlarge (EBS gp3,
XFS, five interleaved rounds, pipelined 20-level `MINSERT` batches):

| `interval` | no sync | one `syncfs()` | `fsync` per file |
|---|---|---|---|
| 1 connection, 4 symbols | 6.08 M/s, p99 196 µs | 6.14 M/s, p99 195 µs | 5.96 M/s |
| 4 connections, 4 symbols each | 10.65 M/s, p99 **1791 µs** | **11.71** M/s, p99 **594 µs** | 3.31 M/s |
| 1 connection, 64 symbols | 6.47 M/s | 6.59 M/s | 0.97 M/s |
| 4 connections, 64 symbols each | 10.40 M/s, p99 **2255 µs** | 9.39 M/s (**−9.8%**), p99 **679 µs** | 0.26 M/s |

Per file is eight files, a directory and its parent per symbol, so its cost grows with the symbols a
tick touches: at sixteen symbols the tick took 250 ms of its 100 ms interval. `syncfs()` took 16-60
ms however many symbols, and under `every` - bound by the WAL's own sync per batch - it was within
noise: 548 → 558 thousand levels a second at one connection and 562 → 580 at four with four symbols
each, 518 → 524 and 446 → 439 with sixty-four, the last inside the no-sync run's own spread of
400-470, where an `fsync` per file lost 46% at 4.1 s a tick. **The tail at four
connections is three times shorter with it**, and why is not measured here; the hypothesis worth
testing is that `syncfs()` outside the lock writes back the WAL's dirty pages too, leaving less for
the tick's WAL sync, which runs under it. Under `none` nothing is synced, because that policy
promises nothing after a power cut.

**Four rules came with it, each because the obvious version was wrong:**

- **A flush whose sync fails writes no checkpoint, and no flush after it does either, until a
  restart.** The first version let the next successful `syncfs()` cover a failed flush's segments
  together with its own, and that is not what Linux does: a failed sync reports its error once and
  marks the pages it could not write clean, so the next one succeeds **without writing them**. The
  WAL had the same hole one record wide - a checkpoint is appended without a sync of its own, and
  when the sync after it failed, the tick after that synced successfully and took the checkpoint as
  durable. **Measured on this change's own first version**, with the fault injector: the second
  sync of `wal_000002.bin` failed, the third succeeded, and `wal_000001.bin` was gone. So the first
  failed sync of any kind - a flush's `syncfs()`, or any WAL `fsync` the writer counts - freezes the
  checkpoints for the rest of the process: one `ERROR` and `ob_checkpoints_frozen`, retention stops,
  the WAL keeps every record, and the restart replays from the last checkpoint synced before the
  failure and rebuilds every segment written since. Under `none` nothing is frozen, because that
  policy makes no promise a failed sync could break. The fault test that asserted the old rule - "the
  flush after it claims both" - passed because the injector fails a call and marks no page clean:
  it could not disagree with the design, and reading the design against the kernel is what did.
- **WAL retention follows the newest checkpoint known to be on the device** - one tick behind the
  one just appended, because a checkpoint is appended without a sync of its own, and deleting WAL
  files on its strength let a power cut keep the unlink and lose the checkpoint.
- **Startup removes the segments no surviving checkpoint vouches for**, and replay rebuilds their
  rows from the WAL (`ob_segments_rebuilt_from_wal_total`). A size or a checksum per file was the
  first design, and it was not enough: a segment's recorded position speaks for the **whole drain**
  that produced it, and a flush cut short can leave some of that drain's segments whole and others
  missing - each one passing its own check. The WAL holds every one of their rows, since retention
  deletes nothing a synced checkpoint does not vouch for. A log with no checkpoint at all vouches for
  no segment, when it starts at its first file; an older build's checkpoint says nothing of what it
  covered, and then every segment is taken as written, as before.
- **`wal_identity` is written through `write_file_atomically()`** - temporary, sync, rename,
  directory sync - because every flush's `syncfs()` covers it only from the first flush on, and a
  cut before that brought it back empty. `FLUSH` answers `ERR` when its segments could not be
  synced, and the replay line says when the log holds no checkpoint at all rather than rendering
  that as an older build's checkpoint.

**Tests.** `test_power_cut.py`, six cases, opt-in locally with `OB_POWER_CUT_TESTS=1` because they
need root and dm-flakey, and switched on in both CI integration jobs, whose skip gates accept no skip
but Binance's; with the variable set, a missing prerequisite **fails**. The cut after a flush and its
control, under `every` and `interval` - against the build before this change the two cuts fail with
0 of 201 and the controls pass. A cut **during** the first flush's `syncfs()`, held open by the
injector, after the test itself syncs each segment's `meta.json` and nothing else: the premise is
checked on the remounted device - the metadata there, a column empty - and the restart must bring
every row back, which is the vouching rule end to end rather than through its log line. And a cut
before the first flush, after which the identity must be the one it was. `test_storage_faults.py`
over the injector's new `OB_FAULT_OP=syncfs`: a flush whose sync failed claims nothing, under both
policies; a failed segment sync freezes the claim until a restart, and the restart ends it;
retention passes neither a segment that never synced nor a checkpoint whose WAL sync failed, and
under `none` a failed WAL sync freezes nothing; a refused segment write leaves the other symbols
written, merged and readable and no directory of its own behind; and a rollover refused mid-drain
appends no row twice.

**Mutation table: twenty-three rows, each with the verdict it was supposed to produce** - nineteen
killed and four controls that survive - against committed code, both batteries green before the
first row and after the last, and the sources restored byte for byte.

| # | Mutation | Instrument | Verdict |
|---|---|---|---|
| 1 | no segment sync at all (the defect) | cut after a flush | killed: 0 of 201 under both policies |
| 2 | the flush whose sync failed still appends its checkpoint | failed segment sync | killed |
| 3 | the first design: the next successful sync claims the failed flush's segments | freeze until a restart | killed |
| 3c | control: the same, against the test of one failed flush | failed segment sync | survives |
| 4 | the retention floor follows a sync that succeeded after one that failed | failed WAL sync | killed |
| 5 | a failed WAL sync freezes nothing | failed WAL sync | killed |
| 6 | under `none` a failed WAL sync freezes too | `none` | killed |
| 7 | retention follows a checkpoint before a WAL sync covers it | failed WAL sync | killed |
| 8 | the retention floor never rises | failed WAL sync, its restart half | killed |
| 9 | no segment is removed at startup | cut during the segment sync | killed: the rows are gone |
| 9f | the same | failed segment sync | killed |
| 10 | a log with no checkpoint vouches for every segment | cut during the segment sync | killed |
| 11 | a refused segment write ignored (#161) | refused symbol | killed |
| 12 | a refused segment's directory left behind | refused symbol | killed |
| 13 | the first refused store stops the flush before the merge | refused symbol | killed |
| 14 | a drain stopped by a refused rollover keeps its appended rows queued | refused rollover | killed |
| 15 | `wal_identity` written in place and not synced | cut before the first flush | killed: it came back empty |
| 15c | control: the same, against the cut after a flush | cut after a flush | survives |
| 16 | `FLUSH` answers `OK` over a failed segment sync | failed segment sync | killed |
| 17 | a failed segment sync is not counted | failed segment sync | killed |
| 18 | the freeze is not published as a gauge | failed WAL sync | killed |
| 19 | control: the freeze's `ERROR` reworded after the part the tests read | the three freeze tests | survives |
| 20 | control: the removal line's tail reworded | cut during the segment sync | survives |

Two rows are worth more than the others. **9**: without the vouching rule, a real cut that leaves a
segment's metadata and not its columns loses that segment's rows - the replay filter believes the
position the metadata records. **15c** is why the identity test exists at all: every other test's
flush syncs the identity together with its segments, so writing it in place and unsynced is
invisible to them. Writing the expected verdict before running is also what found the four
mutations nothing would kill - 9, 12, 15 and 6 - and each got its test first.

- Effort: M | Impact: **P0** - under `every`, writes acknowledged as synced were lost to a power cut
  once a flush claimed them; measured 0 of 201

### 159. A crash after a flush lost the rows written while it wrote its segments, and the tick could delete their WAL file ✅

**Found by stage 5 of #151, and older than it.** Stage 5 moves the flush tick's WAL sync out of the
engine's lock, and its crash test — rows written while the tick synced, a kill, a restart — failed
against the build **before** stage 5 as well. It did not need the sync outside the lock. It needed two
things this engine has had since #62 gave it a checkpoint: a flush that writes its segments without
`mtx_` (#15's two phases), and a checkpoint after that write which replay starts from (#62).

A flush drains the queued rows under the lock, writes their segments without it, and then appends a
checkpoint that replay starts from. Writers go on appending while the segments are written, so the log
holds records **between** the drain and the checkpoint whose rows are still queued. The checkpoint
had an empty payload and meant "everything before me", so replay skipped exactly those records, and a
crash before the next flush lost every one of them — each answered `OK`, and **under every fsync
policy**, `every` included, where the record was on the disk before the client heard anything.

**Measured before the fix**, with the fault injector holding a segment write open for two seconds:
row 300 before a tick, rows 301–303 while it writes, and a kill while the *next* tick writes them.
After the restart the node had **[300]** — three of three acknowledged rows gone, under `every` and
under `interval`. On a healthy disk the window is the segment write, milliseconds per tick; small for
any one crash, and there on every tick for as long as writes flow.

**The second reader of the same boundary was worse.** The tick's WAL retention deleted every file
before the **current** one once the segments were written. A rotation during the segment I/O leaves
the records written before it in a file before the current one, with their rows still queued — and
the tick deleted that file. The checkpoint fix alone cannot help: it gives back what is in the log,
and the log no longer has it. Measured: row 300, six hundred rows across a rotation during a slowed
tick, a kill during the next: **[300]**, six hundred of six hundred gone, and one WAL file left on
the disk where the fix leaves two.

**The fix is one value with two readers.** `Engine::drained_up_to_` is the position the last
completed drain reached — the one it already stamped into its stores for #63 — and

- **the checkpoint carries it**, as an eight-byte payload (file index, then offset, both
  little-endian). Replay forwards every record after the last checkpoint and **gives back** the ones
  before it that start at or after the position. The position can only add records: a payload no
  writer produces — a position past the checkpoint itself — is read as covering everything before
  it, which is the most a checkpoint ever claimed. One written by an older build has no payload and
  is read by ordinal, as it always was;
- **retention deletes only the files before the one the drain reached.**

It is set when the drain **completes**, not when it starts, so a drain that throws part-way leaves the
previous, smaller claim standing. Nothing else moved: the per-symbol positions that stop a durable
row being replayed twice (#63) were already right, because they are stamped at the drain, and they
are what makes giving records back safe — a record whose symbol's segment holds it is skipped there.

**Compatibility, both directions.** An older build reading a directory this one wrote reads the
checkpoint by ordinal — the payload is checksum-covered bytes it never looks at — so it has the
defect and nothing worse. This build reading an older directory reads its checkpoints the same way;
the records an older checkpoint wrongly covered are not recoverable, because the log's account of
itself does not say which they were.

**Why nothing caught it.** The tests that read a killed node's own data back — `test_crash_recovery.py`,
the torn-record test, #126's measurement — write, then kill, with the flush tick ten minutes or an hour
away, and a checkpoint written with nothing in flight covers exactly what it drained. The window needs
a write during a flush's segment I/O **and** a crash before the next flush, and a healthy disk makes
the first of those milliseconds wide. The injector's
new `OB_FAULT_DELAY_MS` is what makes it testable: the chosen write or sync sleeps and then succeeds,
which turns milliseconds into seconds a test can stand in.

**Three things the change moved, each caught by a guard doing its job.**
- The first version of the checkpoint test used `FLUSH`, and its own premise refused: `FLUSH` runs on
  the connection's event loop, so with one loop the three inserts on another connection waited out
  the whole segment write — **1.99 s** — and the `FLUSH` had answered before them. The tests use the
  tick, which has its own thread.
- #112's regression test named the checkpoint **by its size**, 24 bytes. It is 32 now, and the first
  run after the payload failed on "no fault fired, so this test measured nothing" — a record that
  changes size moves every injection point named by it, and the guard said so instead of passing over
  nothing. The ROTATE-marker test named its own target by the same size and said the checkpoint was
  the only other writer of it; the sentence is gone, and the premise is stronger.
- The fuzzer's WAL oracle said `replay_after_checkpoint` returns "exactly the suffix after the last
  checkpoint", which is no longer the contract. It now decodes the payload itself, from the layout
  rather than through the engine's function, so a wrong byte order is disagreed with instead of
  repeated; two seeds pin the two directions (`positioned_checkpoint`, `checkpoint_past_itself`), and
  a 90-second campaign ran 250,917 inputs through it clean.

`docs/architecture.md` called the checkpoint's payload empty and still described replay's guard as
the timestamp comparison #63 made a fallback; `docs/cli.md` said replay starts after the last
checkpoint. Both say what is true now. **Writing #160 down found three sentences it contradicts** — measuring it then made them wrong
rather than doubtful — and one of them was the guarantee itself: `docs/operations.md` said a segment lost to a power failure is
rebuilt by replaying the WAL — which holds only until the next checkpoint claims it — and
`architecture.md` and `cli.md` promised that under `every` an acknowledged write survives a power
cut. Each now says until when. The same page's device table recommended `--fsync-policy never` for
NVMe with power-loss protection, a value the server refuses to start on (`--fsync-policy expects
every, interval or none, got 'never'`), and argued that the capacitor makes a sync pointless: it
makes a sync **cheap**, since it protects what reached the device, and a write never synced is in
the kernel's page cache, which a power cut empties whatever the device has.

**Tests:** `CheckpointPayload.*` (the layout byte for byte, and every length but eight saying nothing),
`WalCheckpoint.*` (the records between the drain and the checkpoint given back and nothing else; a
quiet flush read as before; an older build's checkpoint; only the last checkpoint deciding, both
orders; a position past the checkpoint taking nothing away; the drain's position in an earlier file
than the checkpoint), the two integration tests over the injector — both **fail against the build
before the fix**, which is what makes them evidence — and one for the injector's delayed write,
whose control is the two writes in the same run that must not wait, and whose bytes must arrive.

**Mutation table: fifteen rows, each with the verdict it was supposed to produce** — twelve killed
and three surviving on purpose — against the committed code, with the WAL suite, the fuzzer's seeds
and both fault modules asserted green before the first row and after the last, and sources restored
from saved bytes. Three instruments, and a row names one, so the table says which a mutation needs.
Killed by the WAL suite: replay ignoring the position (the old rule); the position allowed to take
records **after** the checkpoint away, which was this change's own first version; the checkpoint
record itself forwarded; the first checkpoint's position kept instead of the last one's; the payload
written offset first; any payload of eight bytes **or more** read as a position. Killed by the
fuzzer's seeds: the old rule again, the first version again, and the payload **read** offset first —
which only the harness's own decode can see, since the engine's encoder and decoder would agree.
Killed by a live node: the checkpoint claiming the log's end (the defect), retention deleting every
file before the current one (the second defect — **only** the rotation test kills it, and the control
running the same mutation against the other test survives, which is what says that test is
load-bearing), and retention switched off (killed by #124's own control in `test_wal_rotation.py`,
"retention is not running in this cluster"). The third survivor is written down as one: recording
the drain's position when the drain starts rather than when it completes differs only for a drain
that throws part-way, which nothing in this suite makes happen. The control rewording the replay's
log line survives because the test reads the count, not the sentence.

- Effort: M | Impact: P0 by consequence — acknowledged writes lost to a crash, under every fsync
  policy, with no error anywhere; and a rotation at the wrong moment deleted the only copy

### 158. A node started without flags used one core of the machine it ran on ✅

**Stage 4 of #151: `boost` is the default, and it is sized to the machine.** Stages 1 to 2b made
more than one client event loop possible and made both halves of the traffic scale with them —
reads 3.08× at four loops (#151), pipelined writes 1.73× (#155) — and a node started without flags
still ran one loop, so every one of those numbers was behind a flag. The command line's default
profile is now `boost`: it takes the CPUs stage 3 found (#156) and gives **one client event loop per
usable CPU**, and a **10 µs spin window** where the process has a CPU to spare. `eco` is the engine as
it was — one loop, blocking between events — and a value the operator gives wins over either.
`ServerConfig`'s own default stays `eco`, like the two fields a profile sets: a struct cannot know
the machine, so a server built in code is today's engine and the command line starts from
`kDefaultProfile` instead.

**Measured with the binary that ships it** (m9g.xlarge, four Graviton cores shared with the load
generator, loopback, GCC 14 Release, data on EBS, five interleaved rounds, a fresh node per run;
`scripts/measure_profile_rule.py`, whose first half is a measurement and whose second is a record
of what it refuses):

| | `eco` | the default (`boost`) |
|---|---|---|
| `BOOK` reads, 12 connections, levels read / s | 15 900 281 (15.72–16.12 M) | **49 667 582** (48.89–50.32 M) |
| `MINSERT` writes, 12 connections, levels / s | 6 122 008 (6.06–6.13 M) | **10 546 970** (10.51–10.87 M) |
| read batch p99 | 1 942 µs | 225 µs |
| `PING` alone, p50 / p99 | 7 772 / 8 094 ns | **6 329 / 6 775** |
| `PING` beside three connections pipelining writes, p50 / p99 | 502 705 / 544 954 ns | **6 241 / 15 198** |

The last row is the one this stage buys that no earlier one did by itself: with one loop, a
latency-sensitive connection waits behind every batch the others send; with a loop per CPU it has
one of its own.

**The rule is a measurement, and the measurement changed two things the design had.** Eight series
on the m9g.xlarge and one on the development laptop, each a set of placements (the server's cores
and the probe's under `taskset`, a CPU limit through `systemd-run --user --scope -p CPUQuota=`)
against configurations against workloads, written down in full in
`kiro-workspace/specs/uses-the-whole-machine/design.md`:

- **Loops = usable CPUs, no fewer and no more.** Twelve connections — which one to four loops divide
  evenly — on the server's own cores: two cores read 28.7 million levels a second with two loops and
  29.5 with three, but three loops on two cores took the read batch's p99 from 1.05 ms to 10.7;
  three cores read 44.2 million with three loops against 29.9 with two; four cores shared with the
  probe read 49.1 with four loops against 40.2–42.2 with three. Writes saturate at two loops (the
  engine's lock, #155) and lose nothing past them.
- **A cgroup limit's floor, not its ceiling.** Two loops under a one-CPU limit wrote what one wrote,
  with a p99 of 25 ms against 0.93 and 1.4–1.6 s of every ~2 s run throttled; under 1.5 CPUs, two
  loops wrote more but spent 211–265 ms throttled with a p99 of 2.3 ms against 0.85. Rounding down
  in stage 3 is what keeps the loops inside the limit, and now it is measured.
- **The spin window: 10 µs, not #144's 50.** 50 µs was a judgement about the gaps between a client's
  requests. The gain saturates at 10 — `PING` p50 7 569 ns with no spin, 7 622 at 5 µs (too short to
  catch the next request), 6 279 at 10, 6 252 at 20, 6 306 at 50 — and when the machine has more
  busy threads than cores **the tail is the window**: a `PING` beside three writers on the same four
  cores had a p99 of 15 917 ns with no spin, 16 448 at 10 µs, 24 190 at 20 and 53 900 at 50, because
  a spinning loop keeps the core the client needs until the window closes. With the server's cores
  its own, every window from 10 µs up gave the same (7 664 → 6 203 ns p50).
- **No spin on one CPU, nor under a limit that binds** — the two places requirement 4.5 named, now
  with numbers. A client sharing the only CPU had a p99 of 5 859 ns with no spin, 13 706 with 10 µs
  and 23 704 with 20, in every one of five rounds; on a CPU of the server's own, and under a one-CPU
  limit on four cores, the same window took 1.4 µs off. Under a limit, spinning spends the limit, and
  a cgroup over it stops every thread until the next period — the throttling above was measured from
  loops past the limit rather than from the spin, but the spin's cost in CPU grows with an event rate
  the engine cannot see, and 1.4 µs of gain against a stop of up to 100 ms decides the direction.

**Where the 10 does not travel, measured rather than assumed.** On the development laptop
(i3-7100U, 2017, `PING` ~30 µs) 10 and 20 µs catch nothing — 29 875 → 29 836 and 28 759 ns — and
50 µs takes 18% off (24 360). The window has to cover the gap between an answer and the client's
next request, which belongs to the machine and the client; 10 µs is the choice for the class of
machine the published numbers come from, and `--io-spin-us` is the operator's for a client with a
longer gap. Adapting the window to the gaps a loop observes is the obvious next step and has a
failure mode worth naming before anyone builds it: a loop spinning on the core its client needs
*delays* the client's next request, so the gaps it observes grow with its own window, and a rule that
widens the window to fit them widens it without bound.

**`--print-config` and the log say what was chosen and why**, under the machine it was chosen for:

```
# machine: 1 usable CPU: affinity 1, no cgroup v2 CPU limit
# profile boost: 1 client event loop, one per usable CPU, and no spin: the only CPU this process may run on is shared with its flush, the kernel and any client on this machine
```

and an operator's value is said beside the profile's reason, so a line reading "4 client event
loops" above a node running 2 is not left to be noticed.

**Four things in the measuring harness, each of which would have measured something else under
the right name.** The check that the PID behind `taskset` and `systemd-run` was the server read
`/proc/PID/comm`, got `ob-io-0` and refused the server as a wrapper — the main thread runs the first
loop and carries its name, so the check reads `/proc/PID/exe`. The writer beside a `PING` probe is
given a hundred times what it can finish and stopped when the probe ends, so writes cover the whole
probe by construction rather than because a batch count happened to suffice; under a quota or on a
shared core the probe takes many times longer than alone. A placement under a quota is refused
unless the scope's `cpu.max` says what was asked for. And the first comparison of loop counts used
four connections, which three loops are dealt 2+1+1 — the loop with two was the bottleneck, and
three loops read as worse than two on writes (9.1 against 10.3 million) until twelve connections
took the imbalance out (10.7).

**The integration battery runs on the default.** Nothing in it is pinned to `eco`, so every node in
it runs a loop per CPU of its runner and spins where the rule says. Locally, on the development
laptop's four hardware threads — four loops per node, the spin on, three-node clusters and the test
client all on the same four — **340 passed and the 2 opt-in Binance tests skipped, in 22:55**,
against 338 and 23:04 for #155's tree on the one-loop default. Three tests are
new in `test_io_profile.py`: a node with no flags runs as many `ob-io-*` threads as its log says it
chose, counted by the kernel rather than read back from the sentence that claimed them, and as many
as the mask this test process has; a node under `taskset` on one CPU runs one loop and says why it
does not spin; and the idle test now measures the default node, since requirement 4.4 is that a
hundred per cent of the cores means under load, not heating an idle machine.

**Mutation table: fifteen rows, thirteen killed and both controls surviving**, against the committed
code, sources restored from saved bytes after each row, and both suites asserted green before the
first row as well as after the last. Killed: the loops sized to the affinity mask rather than to the
usable CPUs; no ceiling on the loops; a spin on the only CPU; a binding limit that does not stop the
spin; a limit of exactly loops + 1 refused; the profile overwriting the operator's loops; the
profile's loops reported as a default; the command line starting from the struct's `eco`; `eco`
quietly sizing the loops; `--print-config` without the profile's choice; an operator's loops not
said beside the profile's reason — and two only a live node can see: the server running one loop
whatever the configuration says (killed by the thread count, *"the log says 4 loops, the kernel lists
['ob-io-0']"*), and the spin not switched off for one CPU on a node under `taskset`. The controls — the
window retuned inside its bounds, and the startup line's cost clause reworded — survive, because
nothing pins either.

**What this does not do.** It sizes the client event loops and the spin, and nothing else: the
flush thread, replication and the mesh loop keep one thread each (stage 5 is the flush's lock, stage
6 a scan across cores). The rule is measured up to four CPUs; beyond them it is the reads' linear
scaling extrapolated, and the machine it runs on will say otherwise if it is wrong. And it does not
see where the client runs — the tail a spinning loop costs a client on its core is bounded by the
10 µs window, not removed.

- Effort: M | Impact: a node uses the machine it is given without a flag — on four cores 3.1× the
  reads and 1.7× the writes of the engine as it was, and a latency-sensitive connection no longer
  waits behind other connections' batches

### 157. The sustained-insert test read its whole run in one answer, which #152's ceiling refused on a faster runner ✅

**A required check failed on a pull request whose change could not have caused it** (#156, PR #171),
and the failure was the server doing what #152 made it do:
`answer of 77121881 bytes is larger than the 67108864 a session may have queued; narrow the query or
add LIMIT`. `test_stress.py::test_sustained_insert_throughput` inserts for a fixed time as fast as the
Python client can and then reads every row back with one `SELECT *`, so the size of that answer is
the runner's speed times `OB_STRESS_SECONDS` — and the 64 MB ceiling is a constant. The margin had
been thin for as long as #152 has existed: the same test passed on PR #170's run a few hours earlier.
With `OB_STRESS_SECONDS=30`, the documented long run, it could not have passed on any machine that
inserts two million levels in thirty seconds.

The test now counts the run's rows in answers the server will send: half-open windows of the
timestamp, and a window whose answer is refused split in two and each half counted. The server
stamps a row at arrival with the host's clock, so the run's rows sit in its wall-clock span; two outer
windows keep the count from depending on that. Checked with the long run on the development laptop:
**2 023 750 levels, about 120 MB as one answer, counted exactly**. The claim is unchanged — every
level sent comes back once — and it no longer has a speed above which it cannot be checked.

- Effort: S | Impact: a required check that failed on fast runners for a reason in the test, not in
  the engine

### 156. The engine could not tell how many CPUs it had, so nothing in it could be sized to the machine ✅

**Stage 3 of #151, and what stage 4 needs before it can exist.** Nothing in the engine asked how big
the machine is — every thread count is a constant or a flag, and before this change
`grep -rn 'hardware_concurrency\|_SC_NPROCESSORS\|sched_getaffinity' src include` found nothing —
so a default sized to the machine had nothing to be sized from. (Three pages said so in the present
tense: `benchmarks/before-a-bigger-machine.md`, `benchmarks/on-a-bigger-machine.md` and
`scripts/measure_cpu_cost.py`. Each now says when it was true and what changed.) And the
obvious answer is the wrong one in the two places an engine is most often run:
`std::thread::hardware_concurrency()` says how many CPUs the kernel has, which is neither how many a
process under `taskset` or a cpuset may be scheduled on, nor how much CPU **time** a container's
cgroup lets it use wherever it is scheduled. Four spinning loops sized from the first number in a
four-core container capped at one CPU would each get a quarter of one.

**`detect_machine()`** is a pure function of what the process can see — the affinity mask, the text
of `/proc/self/cgroup` and a reader for the cgroup files — so every case below is a unit test rather
than a machine to go and find, and the answer is `max(1, min(mask, floor(limit)))`:

- **Every ancestor's limit, up to the mount's root.** A parent's `cpu.max` binds its children, so
  the leaf alone says "no limit" under a slice capped at one CPU.
- **The mount's root as well as the named path**, which is where the limit is inside a container:
  without a cgroup namespace `/proc/self/cgroup` names the host's path, and that path does not exist
  in the container's view.
- **Rounded down.** A CPU and a half is one CPU the engine can keep busy; two loops on it would get
  three quarters each.
- **cgroup v1** as quota over period, `-1` as no limit, and on a hybrid host the controller is read
  in the hierarchy that holds it — the unified one has no `cpu.max` and would have said "no limit".
- **A file that cannot be read is no information**, not "no limit", and the reason says how many
  levels that was.

**The last rule is the one the first version got wrong, and writing this entry found it.** The
header promised it; the reason said it only when **no** level could be read, so a leaf saying `max`
beside a parent whose `cpu.max` could not be read came out as "no cgroup v2 CPU limit" — more than
was found, since a limit at the parent would bind. Two different things had one answer. A `cpu.max`
that does **not exist** is a level where the CPU controller is not enabled — the hierarchy's root
never has one, so on every real host at least one level was "unreadable" in the old sense — and it
limits nothing. A file that exists and cannot be read could hold a limit. The reader now tells them
apart by `open()`'s errno (`ENOENT` and `ENOTDIR` are "not there"), the reason counts the second kind
whether or not a limit was found at the others, and under cgroup v1, whose files exist in every
cgroup of the `cpu` hierarchy, none of them in view says where the hierarchy is not mounted rather
than that it limits nothing.

**Said where it is read**, in one line at startup and in `--print-config`:

```
machine: 1 usable CPU: affinity 4, cgroup v2 limit 1.50 CPUs (/sys/fs/cgroup/…/run-….scope/cpu.max)
```

**Checked on two real systems, not only on literals** — the development machine (Ubuntu 24.04,
systemd 255) and the m9g.xlarge (Amazon Linux 2023, systemd 252), both cgroup v2, each started under
the wrapper that produces the case:

| started under | development machine | m9g.xlarge |
|---|---|---|
| nothing | 4 usable CPUs: affinity 4, no cgroup v2 CPU limit | the same |
| `taskset -c 0,1` / `-c 0-2` | 2 usable CPUs: affinity 2, no cgroup v2 CPU limit | 3 usable CPUs: affinity 3, … |
| `systemd-run --user --scope -p CPUQuota=150%` | 1 usable CPU: affinity 4, cgroup v2 limit 1.50 CPUs | the same |
| `… -p CPUQuota=250%` | 2 usable CPUs: affinity 4, cgroup v2 limit 2.50 CPUs | the same |
| `taskset -c 0-1` around `CPUQuota=350%` | — | 2 usable CPUs: affinity 2, cgroup v2 limit 3.50 CPUs |

Nothing is sized to it yet. That is stage 4, whose rule for the default profile is to come out of a
measurement, not out of this paragraph.

**Mutation table: twenty-one rows, twenty killed and the control surviving**, against the committed
code, sources restored from saved bytes after each row and both suites green after the last. Killed:
only the process's own cgroup read, not its ancestors; the mount's root not read; a fractional limit
rounded up; the first limit found winning instead of the tightest; the controller matched by
substring (`cpuacct` is not `cpu`); the unified hierarchy preferred on a hybrid host; an absent
`cpu.max` read as unreadable; an unreadable one taken as absent; the unreadable levels left unsaid
beside the ones that were read; no readable level reported as no limit; a v1 hierarchy out of view
reported as no limit; the reader reporting a missing file as unreadable, `ENOTDIR` as unreadable,
and a failed `read()` as the file's text; a zero or negative quota read as a limit; v1's `-1` read
as a limit; the larger of the mask and the limit taken; the online count used when the mask is
known; the reader counting online CPUs instead of the mask; `--print-config` without the machine.
The control — the startup line reworded — survives, because no test pins a log line's words. The
first version of the substring row did not build (it left the helper unused) and was rewritten to
make the mistake inside the helper, where it would be made.

**CodeQL on the pull request found one more:** the reason printed "3" or "1.50" by testing the limit
against its floor — an exact comparison of doubles, which is a question about their representation.
It now prints two places and drops `.00` from the text, the same output for every limit a cgroup
can hold.

- Effort: S | Impact: the default profile can be sized to the machine the process actually has, and
  an operator can read what the node found and where it found it

### 155. A read's writes took the engine's lock once each, so writes from several reactors queued on it ✅

**Stage 2b of #151, and the answer to the half of it that did not scale.** #151 measured a convoy:
the engine took `mtx_` once per record and held it through the WAL's `write()`, so with more than
one reactor writing, every acquisition contended — futex calls **1 128 at one reactor, 1 485 158 at
four**, all of them in `Engine::apply_delta_impl`. The writes of one read are now applied under
**one** acquisition, and their WAL records reach the file with **one `write()` per run** between
rotations — and under `--fsync-policy every`, one `fsync` per run, which is group commit.

**What a batch keeps, and each of these is a test:** every write gets the answer it would have got
as a command of its own, in the order the commands came; a command after a write in the same read
sees it; a failed `AUTH` still ends the batch where it stands; each record goes to the replicas with
the position its own append returned (#98); in multi-master each write has its own HLC tick and the
peers are told after the lock is released, in order (#80); a GAP record goes into the WAL directly
in front of the DELTA it announces; and the metrics count per write, with one update per batch.

**One write path, not two.** `apply_delta()`, `apply_delta_replicated()` and `apply_delta_mm()` are
the batch with one write — which is what carrying the GAP inside the batch bought — so every test the
single-write path had now tests the batch, and the server answers a write through `execute_writes()`
whichever way it arrived.

**Measured on the m9g.xlarge** (four Graviton cores, Release, GCC 14 for both builds, loopback, data
on the instance's EBS volume, five interleaved rounds; `scripts/measure_pipelined_ingest.py` with the
kernel counting system calls per batch, and `benchmarks/pipelined_ingest`). The binaries measured were
built from this commit's sources — all 132 files in `src/`, `include/` and `benchmarks/` compared by
hash — and master is 4258643:

| one connection, batches of 64 `MINSERT`s of 20 levels | master | this change |
|---|---|---|
| levels / s | 4 785 305 (4 750 452 – 4 814 825) | **6 081 081** (6 033 171 – 6 173 847) |
| batch p50 / p99 | 225.7 / 255.0 µs | 171.6 / 195.7 µs |
| server CPU for the same work | 3.08 s | 2.41 s |
| `write()` calls per batch | 64.01 | **1.01** |
| the same under `--fsync-policy every`, levels / s | 10 794 (10 791 – 10 805) | **543 260** (541 623 – 549 251) |
| `fsync` calls per batch under `every` | 64.00 | **1.00** |
| batch p50 under `every` | 118.4 ms | 2.31 ms |

| four connections, batches of 64 × 20 levels | master | this change |
|---|---|---|
| 1 reactor, levels / s | 4 871 543 | 6 177 795 |
| 2 reactors | 4 362 367 | **10 192 567** |
| 4 reactors | 4 483 782 | **10 721 513** |
| batch p99 at 1 / 2 / 4 reactors | 1 412 / 5 120 / 4 154 µs | 848 / 416 / 1 573 µs |
| futex calls per batch at 4 reactors | 122.55 | **2.02** |

**Group commit is the largest number here and the design gave it no number.** Under `every` a
pipelining client paid one sync per write — 1.85 ms each on this volume — and now pays one per read:
**50×**, with `OK` still meaning on the disk, because the answers are sent after the sync.

**The convoy is gone — 1.47 million futex calls a run become 24 thousand — and four reactors are
1.73× one, which is below the prediction.** The design predicted 2.2–2.8× at four reactors against
one; against this build's one reactor it is 1.73×, against master's 2.20×, and two reactors already
reach 95% of four. **What is left is the lock, now saturated rather than contended**, and that was
measured twice, independently of the throughput it explains:
- **eight connections change nothing** — 10 934 247 levels a second at four reactors against
  10 721 513 with four connections, and 10 200 021 against 10 192 567 at two — so the ceiling is the
  server's, not the client's one batch in flight per connection;
- **the profile at four reactors puts 43% of the server's CPU inside the lock**: all of
  `Engine::apply_local_writes` (35.8%, inclusive) and the flush's first phase, which takes the same
  lock (`flush_drain_pending`, 7.5%). That is 1.36 s of 3.15 s of CPU in a 1.73 s run — the lock held
  **79%** of the wall time even with the profiler slowing everything down. Inside it: the book's
  `insert_level` (15.3% of all samples), the pending rows and the subscribers (`apply_in_memory`,
  7.8% of its own), and the WAL's encode and `write()` (`append_batch`, 4.9% inclusive, and the
  kernel's copy of the run into the page cache). The parse is outside it and now runs on every
  reactor (`parse_minsert`, 29.3% inclusive).

So the next ceiling is the work under `mtx_`, which the design's stage 5 (I/O out of the lock)
addresses only in part: the WAL's `write()` is a few per cent of it, and the book and the pending
rows are most of it. **CRC32C is not in the profile above 1%**, so the absence of a hardware CRC path
on ARM is not what limits this machine.

**What a batch of one costs, stated rather than averaged away.** Over the wire it is not
measurable: 104 701 against 103 118 one-write round trips a second, ranges overlapping, p50 8.9
against 9.1 µs. In process, `BM_IngestionThroughput` — one level on a one-level book, the cheapest
write this engine has — is **733.0 against 746.6 ns (+1.9%)** over six interleaved rounds, and
`BM_IngestionThroughputBatched`, twenty levels, the dataset's shape, is **1 849 against 1 723 ns
(−6.8%)**: the record is now encoded in place, with one copy of the levels where there were two.
Counted by the kernel, the whole process retires **2 723 user instructions per iteration of the
one-level benchmark on master and 3 066 on this change** — a figure about the process, estimation
runs and flush thread included, so it is stated as that and not as a cost per write. The first
version of this change retired 3 114; copying the update into the batch once instead of twice, and
building a run's error text only when a run fails, took 48 of those back and no measurable time. The
remaining cost is the price of one write path instead of two.

**One number above was nearly published from memory, and the instrument had said so.** The first
three-way comparison of those builds read 524.7, 536.1 and 531.0 ns — faster than every other run of
the same benchmark — because `TMPDIR=… sudo perf stat …` loses the variable to `sudo`, so the engine
wrote its WAL to `/tmp`, which on this instance is a tmpfs. `bench_engine` refuses that in so many
words on stderr, and the command had sent stderr to `/dev/null`. The figures above are from runs
that passed the variable through `sudo` and whose stderr was kept and checked for the refusal —
pitfall 326, met a third time.

**Five questions the design answered differently from the code**, each settled in the direction "the
answer a client gets is the one it got as a command of its own", and all recorded in the design: the
conditions (read-only, bootstrap, shard) are checked when the batch is applied, so the window between
check and apply is today's; one wait for room with today's predicate, so the queue can pass its
ceiling by the batch's rows the way one `MINSERT` passes it by its own; a `write()` that stops at a
record refuses **that** record and the ones behind it are tried again, as the next command after a
refused one was — the design said "refuse the rest", which would have changed the answer of every
write behind a transient failure — while records already in the file are never written twice and a
failed sync refuses its whole run; the GAP travels in the batch; and after a `write()` that stopped,
the rotation is not tried, so a disk that has just refused a write is not asked for a ROTATE marker
too (#153's rule).

**Two rules of the first version were removed rather than kept, because no test could tell them
from their absence**, and writing the mutation table is what found them, before a row ran: a second
duplicate check on the replication path that guarded a batch of replicated records nobody sends
(the replication client applies one record at a time), and a rotation skipped after a failed sync
only to match the bytes of a single-write path that no longer exists.

**A security gap found on the way, and it was in the tests, not the code.** A held write never
reaches `execute_command()`, so its authentication gate is `deferrable_write()` — and the test of an
unauthenticated session checked `SELECT`, `STATUS` and `COMPRESS` and not one write. A yes too many
there would have stored writes without authentication with nothing failing. Two tests now hold it:
the classification, and a pipeline over the wire (`INSERT`, `PING`, `MINSERT` in one read before
`AUTH`) that must be refused where it stands and store nothing — and mutation rows 13 and 13b are
that yes too many, killed by each.

**Three static tests had to follow the new shape and keep their claim.** #98's rule that the replicas
get the position the append returned now follows it through `append_batch()`'s outcomes into
`broadcast_to_replicas()`; #146's rule that a read's answers are queued, not sent, reads the
`enqueue` lambda the loop and the held writes share; and #152's rule for an answer too large to send
matches the branch's braces instead of the next `} else if`, which no longer exists. The first one's
guard fired exactly as written — "if the write path changed shape, this test has stopped checking
anything".

**Mutation table: twenty-three rows, each with the verdict it was supposed to produce** — twenty-one
killed and two surviving by design, committed before the first mutation, every source restored from
bytes kept beside the run, both baselines green after the last restore. Unit rows run the named
binary, integration rows `tests/integration/test_write_batches.py` unless the row says otherwise:

| | mutation | verdict | killed by |
|---|---|---|---|
| 1 | a run cut per WAL record: a GAP crossing the threshold ends its file before its DELTA | killed | `WalBatch.AGapThatCrossesTheThresholdStaysInTheFileOfTheRecordItPrecedes` |
| 2 | the records behind a refused write refused too (the design's first rule) | killed | every write of the batch answered `ERR` |
| 3 | the records that landed before a refused write written again | killed | the torn record acknowledged |
| 4 | a run whose sync failed counted as written | killed | eight `OK`s under `every` over a failed `fsync` |
| 5 | runs of one record: a `write()` and an `fsync` per write again | killed — **as the second version** | no write of the batch's size was made |
| 6 | the wait for room taken per write | killed | `WriteBatchStatic` |
| 7 | the WAL appended one record at a time, in a loop without braces | killed — **on the second run** | `WriteBatchStatic`, after it learned to see such a loop |
| 7b | the same, seen by the injector | killed | no write of the batch's size was made |
| 8 | one HLC tick for the whole batch | killed | `WriteBatch.MultiMasterWritesAreNumberedAndTickedInTheOrderGiven` |
| 9 | the peers told with the engine's lock held (#80) | killed | `WriteBatchStatic` |
| 10 | a record the WAL refused applied and answered anyway | killed | the refused record answered `OK` |
| 11 | the replicas told the WAL's current position (#98) | killed | `WalPositionWireStatic` |
| 12 | held writes applied only at the end of the read | killed | `ReadLoopStatic` |
| 12b | the same, seen by a client | killed | `test_pipelined_answers.py`: a `BOOK` that did not see the writes before it |
| 13 | a write held whether or not the session authenticated | killed | `AuthGateTest.AWriteIsHeldForItsReadOnlyOnceTheSessionHasAuthenticated` |
| 13b | the same, over the wire | killed | `test_auth.py`: `OK` for an `INSERT` before `AUTH` |
| 14 | the held writes not emptied at the start of a read | killed — **on the second run** | `ReadLoopStatic`, after it stopped accepting the wrong `clear()` |
| 15 | the latency of a batch observed once, not once per write | killed | 1 observation where 16 belong |
| 16 | the answers of the held writes queued in reverse | killed | the refused write's `ERR` in another write's place |
| 17 | a migrated symbol's write applied in a batch | killed | `WriteBatch.AMigratedSymbolIsRefusedAndTheWritesAroundItAreApplied` |
| 18 | a replicated duplicate applied again | killed | `ReplicationClientTest.ARecordDeliveredTwiceIsAppliedOnce` |
| 19 | the thread's scratch used even while in use | survived | nothing re-enters the write path today; this is the ratchet for the day something does |
| 20 | **control:** the batch's debug line reworded | survived | by design |

**Three rows needed a second run, and none of the three was the engine.** Row 5's first version,
`} while (false)`, left a variable unused and did not build under `-Werror` — invalid, never a kill,
so the mutation was rewritten rather than the code. Row 7 survived because the rule that the WAL call
is outside every loop read only the blocks a `{` opens, and the mutation's loop had no braces. Row 14
survived because the rule that the held writes are emptied at the start of a read found the *other*
`clear()` — inside the function that applies them, which an exception out of `execute_writes()`
skips, and that path is the one the first `clear()` is for: the reactor's next read may be another
session's. Both rules were fixed and both rows then died. And row 1's first draft never ran: it
stopped a run before a crossing GAP without rotating, which leaves the bytes as they were — a
mutation that "survives" without being one.

**Two process mistakes of mine, both about the tree a verification runs in.** I rebuilt four targets
in the worktree whose full `ctest` was running, and then restarted the run with every other test
binary still linked against the previous engine. Neither result was used: the verification is now a
script that builds first and refuses to run a test after a failed build, and nothing touched the tree
until it ended — on which it reported **1 193 of 1 193** C++ tests and **338 passed, 2 skipped** in
the integration battery, on 23:04.

- Effort: M | Impact: pipelined writes scale with the reactors instead of queueing on the engine's
  lock, and `--fsync-policy every` costs one sync per read rather than one per write

### 154. A WAL that could not open its next file never opened one again ✅

**Found with #153, in the same function.** `open_current()` closes the old descriptor before it
opens the new one — the right order, since the file being left ends in its ROTATE marker and must
not be written to again — and when the new file could not be created it threw with `fd_ = -1`, and
nothing ever opened a file again. **Measured before the fix, without the injector:** the data
directory made read-only for the insert that rotates and writable again straight after it. That
insert was answered `ERR WALWriter: cannot open …/wal_000001.bin: Permission denied` although its
record was written (#153), and **every write after it was refused with `WALWriter: write failed:
Bad file descriptor`** — six of six, with the directory writable again — until the process
restarted. A torn record whose next file could not be opened (#126's path) reached the same state,
logged "could not open the next WAL file" once, and then refused everything.

**Now a writer with no file opens one before its next write.** `ensure_open()` runs first in both
record writers. Until it can open the file, every write is refused with the reason — the file and
`Permission denied` — rather than a bad descriptor, and the outage is one line when it starts and
one when it ends, however many writes are refused in between (`LogEpisode`).

Held by `WalRotation.ANextFileThatCannotBeCreatedIsOpenedOnceItCan`, which needs only a directory
the writer cannot create a file in and so runs in every build, and by
`test_a_wal_that_could_not_open_its_next_file_opens_it_once_it_can`, which goes through **two**
outages at the end of two files, because an outage the log reported once has to be reported again
the next time it starts. Both refuse to run as root, where a read-only directory refuses nothing.

- Effort: S | Impact: a moment without a writable data directory at a rotation stopped every write
  until a restart

### 153. A failed sync of a ROTATE marker left the writer in the file it had just ended ✅

**Found designing the next stage of #151**, which writes several records with one `write()` and has
to say what a rotation that fails in the middle of a batch means — and the single-record path had no
answer to copy. Replay and catch-up both stop reading a file at its ROTATE record. `rotate()` wrote
that marker through `write_record()`, which syncs under `--fsync-policy every`, and a failed sync
threw **before** the writer moved to the next file. So the next record went into the same file,
behind the marker.

**Measured before the fix** (`--wal-rotate-bytes 65573`, one-level inserts of 136 bytes, the
injector failing exactly the marker's sync, `SIGKILL`, a restart without it): the insert that
crossed the threshold was answered `ERR WALWriter: fsync failed: Input/output error` although its
own record had been written and synced, **and the one after it was answered `OK` and was gone after
the restart** — 487 acknowledged, 487 back, and the one missing was acknowledged. The marker's write
refused with ENOSPC had the first half of that: the insert that asked for the rotation answered
`ERR` and present after a restart, so a client that sent it again stored it twice.

**Two rules, and each is what the other needs.** The marker is written **without a sync of its
own**: it holds no client's data, and `open_current()` syncs the file it leaves before closing it,
recording a failure there as every other sync does — so the writer leaves the file whatever that
sync says. And **a rotation that fails is not the failure of the record before it**: `rotate()` no
longer throws for the disk. When nothing of the marker reached the file, the file is readable to
its end, the next record goes into it and the rotation is tried again after that one — the one case
in which a file passes the threshold by more than one record, because the disk refused the record
that would have ended it. When the marker tore, #126's abandonment has already left the file.

Held by `test_a_failed_sync_while_rotating_costs_no_acknowledged_write` and
`test_a_rotation_that_cannot_write_its_marker_does_not_refuse_the_write_that_crossed_it`. The first
proves which sync it failed rather than trusting the arithmetic that chose it: under `every` a
client whose own sync fails is answered `ERR` (#113), so one fault fired and every insert answered
`OK` is what says it was the rotation's.

**Mutation table: eight rows, each with the verdict it was supposed to produce** — seven killed and
the control surviving, committed before the first mutation, every source restored from bytes kept
beside the run, and both baselines green after the last restore. The integration rows run
`tests/integration/test_storage_faults.py`, row 3 `WalRotation.*`:

| | mutation | verdict | killed by |
|---|---|---|---|
| 1 | the ROTATE marker synced on its own again (#153) | killed | 1484 acknowledged and gone after the restart — the loss this item measured, now by the test |
| 2 | a marker the disk refused thrown out of the insert that crossed the threshold (#153) | killed | 2483 answered `ERR WALWriter: write failed: No space left on device` |
| 3 | no retry: a writer with no file stays without one (#154) | killed | `WalRotation.ANextFileThatCannotBeCreatedIsOpenedOnceItCan` |
| 4 | the same, from the node (#154) | killed | `ERR WALWriter: write failed: Bad file descriptor` once the directory was writable again |
| 5 | the outage said on every refused write | killed | the outage's line counted, four where two belong |
| 6 | the outage never ended, so the next one is not said | killed | the second outage's line missing — which is why the test goes through two |
| 7 | the old descriptor kept when the next file cannot be opened | killed | the write that had to be refused answered `OK`, into the file behind its marker |
| 8 | **control:** the rotation's debug line reworded | survived | by design |

**The table's first run killed every integration row and the control with it, and nothing about
the engine was the reason.** The harness handed the tests the server as a relative path, pytest ran
from the tree it names, and every integration row died of `FileNotFoundError` — seven rows that
read as kills. The control, which has to survive, and the baseline after the last restore, which has to pass, both
said otherwise, and that is the whole case for keeping them. The run before that stopped on its
first row, because the anchor for row 1 appears twice in the file; the harness now checks every
anchor before it mutates anything.

- Effort: S | Impact: under `--fsync-policy every`, an acknowledged write lost per failed sync of a
  rotation; under any policy, a write refused and stored when the rotation after it failed

### 152. An answer larger than 64 MB closed the connection of a client that was reading ✅

**Found by the isolation measurement of #151**, whose scanning connection kept dying. The second
`SELECT timestamp, price` of one symbol, after more writes had arrived, was a 76 616 983-byte answer,
and the log said `Send buffer cap exceeded: fd=8 pending=0 adding=76616983 cap=67108864` and closed
the session. `pending=0` is the tell: nothing was queued, the client was reading as fast as it
could, and the answer alone was larger than everything a session may hold. **No client could ever
have been sent it**, and it was told so by EOF.

`docs/cli.md` described the cap as what happens to "a client that stops reading entirely while
asking for more" — true of the cap, and not of this: a query whose answer passes 64 MB failed for
every client, every time, and the reader was left to guess from a closed socket whether the server
had crashed.

**Now the two refusals are told apart.** `Session::queue_answer()` returns `TooLargeAlone` for an
answer larger than the whole cap on its own — compared after LZ4 framing on a compressed session,
where it is the frame that must fit — and `CapExceeded` for one that does not fit behind what is
already queued. The second is a client that has stopped reading and closes the session as before.
The first gets `ERR answer of N bytes is larger than the 67108864 a session may have queued; narrow
the query or add LIMIT`, and the session stays open.

**What this does not fix, stated rather than implied:** the answer is still built in full before
it is measured, so the query that found this allocated 76 MB to be told no. Streaming an answer as
the socket drains would remove both the allocation and the ceiling, and it is a change to how every
query produces rows, not to this check.

Held by `AnswerCeiling.AnAnswerLargerThanTheCapIsRefusedAloneAndNothingIsQueued` — the refusal, the
largest answer that still fits as its control, and the other refusal still the other one — and by a
static rule that the loop replaces such an answer with an error and does not close. The behavioural
half needs a store of about 2.5 million rows, so it was checked once on the m9g.xlarge instead:
the same query that found it, 76 437 783 bytes this time, answered `ERR answer of 76437783 bytes
is larger than the 67108864 a session may have queued; narrow the query or add LIMIT`, and the
session stayed open.

- Effort: S | Impact: every query whose answer passed 64 MB ended its session without a reason

### 151. One client event loop for every connection, so the engine used one core of four ✅

**`--io-threads N` gives the server N client event loops ("reactors").** The one holding the
listening socket accepts every connection and deals it to the next reactor in turn; a connection
stays on the reactor it was dealt to for its whole life, because a `Session` is not thread-safe and
the order of a connection's answers is the order its reactor wrote them. Default 1, which is the
loop this server always had — and measured to be it: at one reactor the new binary and master are
the same within the noise on both workloads below.

**Measured on the m9g.xlarge** (four Graviton cores, Release, loopback, every build with GCC 14,
five interleaved rounds, the probe on the same machine; `scripts/measure_pipelined_ingest.py` and
`benchmarks/pipelined_ingest`, which gained a `book` mode for the read half):

| four connections, batches of 64 | master | 1 reactor | 2 reactors | 4 reactors |
|---|---|---|---|---|
| **reads**: `BOOK`, levels read / s | 16 133 184 | 15 992 260 | **30 254 474** | **49 212 362** |
| reads, batch p99 | 649 µs | 655 µs | 353 µs | **222 µs** |
| **writes**: `MINSERT` × 20, levels / s | 4 856 227 | 4 878 479 | 4 327 641 | 4 505 432 |
| writes, batch p99 | 1 387 µs | 1 248 µs | 4 863 µs | 4 067 µs |

**Reads scale, 3.08× at four reactors** — and that is with the probe on the same four cores: its
own CPU is 0.45 s of 0.62 s wall at four reactors, so the machine is full rather than the server
done. **Writes do not scale, and get worse**, and that was not the prediction. The design said
1.6–2.4× for writes, bounded above by Amdahl at 1.84× with 39% of the io thread under the engine's
lock. The bound was computed on the work under the lock and ignored the lock itself.

**The kernel says why, and says it in one place.** `perf stat` on the futex tracepoint over the
same 768 000 `MINSERT`s: **1 128 calls at one reactor, 1 222 274 at two, 1 485 158 at four**, with
607 404 and 758 130 context switches. `perf record -g` on that tracepoint puts **100%** of them in
`Engine::apply_delta_impl` — half in `pthread_mutex_lock` waiting, half in `unlock` waking — on one
address. The engine takes `mtx_` once per record and holds it through the WAL's `write()`, so with
two writers every acquisition is contended, and a futex handoff (sleep, wake, reschedule) costs more
than the critical section it guards. That is a convoy, and more reactors feed it faster. The
next stage takes one lock and one WAL write per read rather than per record; until it does, more
than one reactor is for reads and for isolation, and the documentation says so.

**Isolation is the other half, and it is large.** One connection loops a `SELECT` of 1.5 million
rows (134 ms each) while the probe pipelines writes on another:

| writer beside a scanning connection | levels / s | batch p99 |
|---|---|---|
| 1 reactor | 65 424 | 136 145 µs |
| 2 reactors, writer on the other reactor | 4 995 672 | 241 µs |
| 4 reactors, writer on another reactor | 4 877 810 | 243 µs |
| control, no scan | 4 908 385 – 4 969 225 | 241 – 243 µs |

At one reactor the writer waits behind every scan: **76× less throughput and a p99 equal to the
scan**. On another reactor it does not notice. And the first run of this measurement showed what
"another reactor" means: at two reactors the writer was dealt, by the order connections arrived, to
the scanning connection's own reactor, and saw nothing — isolation holds between reactors, not
within one. The table comes from a run that opens one idle connection before the writer, which
moves it off the scanner's reactor at two; without it, the writer at two reactors read
**64 492 levels a second with a p99 of 138 ms** — the one-reactor figures.

**What had to become one thing for N reactors to be one server**, each held by a test in
`tests/integration/test_io_reactors.py` or by a static rule:
- `--max-sessions` is one limit, checked on the accepting reactor against one count before the
  connection is dealt anywhere — counted where it is adopted, a burst of accepts would be checked
  against a number that had not caught up;
- `conn_id` comes from the accepting reactor, so it stays unique;
- the drain has one deadline: each reactor starts its clock when it sees the request, the first to
  see it reaches the deadline first, and stopping `running_` stops them all — so a shared start,
  which the design called for, would only have moved the others' clocks to that same moment, and it
  was taken out again;
- the gauges every reactor contributes to are published as each reactor's change, because a gauge
  set from four loops is whichever wrote last;
- `STATUS` answers from a snapshot of its own — it wrote the engine's figures, a vector among them,
  into the one `ServerStats` all connections share, which is a data race the moment two reactors
  answer it (found by reading, before anything ran in parallel);
- `FAILOVER` and `MIGRATE` run alone across reactors, through a classifier with no `default:` like
  the authentication gate's: two concurrent `FAILOVER`s revoke one lease twice, and the loser clears
  the winner's handover intent, election block and in-handover flag while the handover runs;
- a reactor that ends on an exception stops the server and `run()` leaves with the error once the
  others are joined, as the single loop did — left running, the others would go on dealing
  connections to a loop that no longer serves them;
- threads are named `ob-io-0` … so `top -H` and a per-thread sample say which loop is which.

**One defect found in the path the reactors now share, and two in what they read:** #150 (a TLS
handshake that could not start closed its descriptor twice) is fixed in `Reactor::adopt()`; #152
(an answer larger than 64 MB closed the connection of a client that was reading) is fixed in the
loop. And the measurement's first version compared master built with GCC 14 against this branch
built with GCC 11, and reported this branch 18% faster at one reactor on reads — which was the
compiler (GCC 11's build reads 19.3 million levels a second, GCC 14's 16.0, on identical code). Every
number above is one compiler.

**Mutation table: thirteen rows, each with the verdict it was supposed to produce** — eleven
killed and two surviving by design, committed before the first mutation, every source restored
from bytes kept beside the run; unit rows run `test_tcp_server`, the others
`tests/integration/test_io_reactors.py`:

| | mutation | verdict | killed by |
|---|---|---|---|
| 1 | the admin lock not taken | killed | the rule that the loop consults the classifier |
| 2 | `FAILOVER` classified as a data command | killed | `AdminSerialisation.ExactlyFailoverAndMigrateRunAlone` |
| 3 | the TLS failure branch closing its descriptor again (#150) | killed | `SessionsStatic` |
| 4 | an answer too large to send closing the session (#152) | killed — **on the second run** | the rule, after it learned what to ask |
| 5 | the too-large check gone from the session (#152) | killed | `AnswerCeiling`'s unit test |
| 6 | `STATUS` formatted from the shared struct | killed — **on the second run** | `StatusAnswersTheEnginesFiguresNotTheSharedDefaults` |
| 7 | every connection kept by the accepting reactor | killed | the deal: eight connections went to `[0, 0, 0, 0, 0, 0, 0, 0]` |
| 8 | `--max-sessions` counted per reactor | killed | the fourth connection got the banner |
| 9 | a hand-off that does not wake its reactor | killed | the connection is never answered |
| 10 | a summed gauge set instead of changed | killed | `ob_subscriptions_active` stayed at 1, never 4 |
| 11 | the deadline reported by every reactor | killed | it was said four times |
| 12 | a reactor that dies not stopping the server | survived | no test can make a reactor die outside an event |
| 13 | **control:** a debug line reworded | survived | by design |

**Two rows survived the first run, and both were the tests' fault.** Row 4: the static rule for
#152 asked for `format_error(` in the branch and no `close_session(`, and a branch that built the
error, dropped it and set `queue_refused` satisfied both while closing the session exactly as
before — it now asks that the error be what is queued, and that the session end only if even the
error does not fit. Row 6: formatting `STATUS` from the shared struct, which `STATUS` no longer
writes, answered zeros for every engine figure and passed all seventy unit tests, because none of
them looked at a figure; now one does. Row 12 is stated rather than hidden: an exception can only
leave a reactor's loop outside an event, which nothing in a test can arrange.

- Effort: L | Impact: reads and queries use the machine's cores and no longer stall ingest on the
  loop they share; writes wait on the engine's lock per record until the next stage

### 150. A TLS handshake that could not start closed its connection's descriptor twice ✅

**Found reading the accept path before moving it into the reactors.** When `wrap()` refused to
start a handshake on a newly accepted connection, the branch did `remove_session(client_fd)` —
which closes the descriptor — and then `::close(client_fd)` again. Between the two closes the
kernel is free to hand that number to any file another thread opens, and this server has several
that do: the WAL writer rotating, the flush thread writing segments, replication and the mesh
accepting. The second close would then close **their** file, with no error on either side — #128's
class, on the client port.

**Reachable only when OpenSSL cannot allocate**, which is why nothing ever saw it: `wrap()` throws
when `SSL_new` or `SSL_set_fd` fails, and both fail only for want of memory. Not reproduced, and
said so; the defect is in the order of three lines, which a reading establishes.

**Fixed where the branch now lives.** The multi-reactor stage (#151) moved the accept path's second
half into `Reactor::adopt()`, which unregisters the descriptor from epoll first, while the number
is still this connection's, and then removes the session — one close. The class is held rather
than the line: `SessionsStatic.NoDescriptorIsClosedAgainAfterItsSessionIsRemoved` reads every
`remove_session(X)` in the file and refuses a `::close(X)` in the rest of its block, and it runs its
own two cases first — the shape this was, and the right one. Restoring the old branch fails it.

- Effort: S | Impact: under memory exhaustion only, a close of another thread's file

### 149. `--workers` was accepted, printed and documented for six months, and read by nothing ✅

**Found while planning the multi-reactor stage, which is about to add the knob this one looked
like.** `--workers <N>` was parsed into `ServerConfig::worker_threads`, printed back by
`--print-config`, and listed in `--help` and in `docs/cli.md` as "Number of worker threads
(default: 4)" — and read by nothing, from the server's first commit in March. #32 noticed, and
printed a note under it (`# workers is parsed and not used: client commands run inline on the epoll
loop`) on the argument that hiding it would leave an operator tuning a knob that does nothing. The
note was true; the knob stayed, so the operator could still tune it, and `--help` still said it made
threads. Next to a real knob for the client event loops, a flag that looks like one and does
nothing is a trap.

**Refused now, by the rule every unknown flag gets** — `unknown argument '--workers'`, from a config
file as much as from a command line. **BREAKING** for a command line that passes it, which is the
point: the alternative is a server that starts, and does something other than what the operator
believed it would.

**The mechanism is the link #32's static tests did not check.** Those hold the parser's branches
against the known-flag list, and the list against `--help` and `docs/cli.md`, and all of them agreed
the whole time — the parser was never what was missing. What was missing is the next link: a field
the parser writes must be read by something other than the function that prints it back.
`CliConfigStatic.EveryParsedValueIsReadByTheServer` takes every `ServerConfig` member from the
header, cuts the parser and the formatter out of `src/tcp_server.cpp`, and requires a `.field` or
`->field` for each of the rest somewhere in `src/`, `tools/` or `include/`. One field is exempt and
pinned in both directions: `profile`, which the parser resolves into the knobs it names, so its
value is only what `--print-config` reports. The rule runs its own cases first, through the same
functions — a header with a dead field, and a server source with the two writers in it — and
against the tree before this change it names `worker_threads` and nothing else.

What it cannot see is written beside it: a field read only by code the binary does not build.
#147's three io_uring flags were exactly that — read in `src/io_uring_server.cpp`, doing nothing in
the epoll binary — and a read in any source file counts.

**Mutation table: ten rows** — nine killed and the control surviving, run the same way:

| | mutation | verdict | killed by |
|---|---|---|---|
| 1 | `--workers` back, whole — field, branch, list, help, `--print-config` | killed | the new rule, naming `worker_threads` and nothing else |
| 2 | the same, against the refusal test | killed | `WorkersIsRefusedBecauseNothingEverReadIt` |
| 3 | a live field made dead: the subscription hub built with a literal instead of `--max-subscriber-queue-bytes` | killed | the new rule, naming `max_subscriber_queue_bytes` |
| 4 | the parser not cut | killed | the rule's own cases |
| 5 | the formatter not cut | killed | the rule's own cases |
| 6 | a longer identifier read as the field (`.deadline` as `.dead`) | killed | the rule's own cases |
| 7 | comments read as code | killed | the rule's own cases |
| 8 | a member without an initializer missed | killed | the rule's own cases |
| 9 | `profile`'s exemption dropped | killed | the tree, which really does leave it unread |
| 10 | **control:** the failure message reworded | survived | by design |

Row 3 is the one that says what the rule is for: it catches a field that stops being read, not
only the one field that never was.

- Effort: S | Impact: an operator tuning `--workers` was tuning nothing, and the multi-reactor stage
  would have added the real knob beside it

### 148. The welcome banner and the CLI carried the version as literals, and #90's guard could not see #90 ✅

**Found while reading the client loop before making it parallel**, at the line that greets every
connection: `s->send_response("OK ob_tcp_server v0.1.0\n\n")`. #90 took the version out of the
startup line so that it would come from `project(... VERSION)`, and this one — the first bytes every
client of every node reads — stayed a literal. So did `ob_cli`'s greeting,
`"orderbook-dbengine CLI v0.1.0\n"`. Both date from the server's first commit, in March. The first
version bump would have had every node greet its clients with the previous release's number, while
`STATUS` and `/metrics` told the truth.

**The guard #90 added could not have caught #90.** `VersionStatic.TheVersionIsNotRetypedInSources`
refused the pattern `"v?0\.1\.0"`: a string literal made of the version **and nothing else**. The
literal #90 removed was `"ob_tcp_server v0.1.0 listening on port %u, data-dir: %s\n"`, which that
pattern does not match. Measured rather than argued: that line restored verbatim, against #90's own
test, **passes** (row 4 below). And the files it read were four names written into the
test, so `tools/ob_cli.cpp` was never opened. #90's entry says both guards were mutation-checked;
whatever that mutation was, it was not #90's own diff reversed, which is the first one to try,
because the defect as it was is the one input a guard written after it must refuse.

**What changed.** Both greetings take `ob::version()`, like every other place a node reports its
version. The rule now finds the version anywhere inside one literal, as a whole number — `v10.1.0`
is not `0.1.0` — in every `.cpp` and `.hpp` under `src/`, `tools/`, `include/` and `benchmarks/`,
listed from the tree. It runs its own cases through the function it runs the tree through: the
banner, the CLI line, a comment quoting the version, the right form, and a longer number on each
side, of which exactly the first two must be reported. So a scan that stops seeing a literal fails
on its cases instead of passing a tree it no longer reads, and the four files copies were found in
must be among the files it read, so a walk that lost a directory says so.

**Mutation table: thirteen rows, each with the verdict it was supposed to produce** — eleven killed
and two surviving, committed before the first mutation and every source restored from bytes kept
beside the run:

| | mutation | verdict | killed by |
|---|---|---|---|
| 1 | the banner's literal back | killed | the tree scan, naming the line |
| 2 | the CLI's literal back | killed | the tree scan |
| 3 | #90's removed startup line back, against this rule | killed | the tree scan |
| 4 | **the same line, against #90's rule** | **survived** | nothing — the demonstration |
| 5 | a literal in a header | killed | the tree scan, which reads `include/` now |
| 6 | #90's pattern back, over the whole tree | killed | the rule's own cases |
| 7 | the leading word boundary dropped | killed | the case `v1` + version |
| 8 | the trailing word boundary dropped | killed | the case version + `1` |
| 9 | the prefilter inverted | killed | the rule's own cases |
| 10 | a comment quoting the version read as code | killed | the case that is a comment |
| 11 | the walk loses `tools/` | killed | the files that must be read |
| 12 | the walk loses `include/` | killed | the files that must be read |
| 13 | **control:** the failure message reworded | survived | by design |

Rows 6–10 are the rule's own cases doing their job: each mutation of the scanner leaves the tree
green, because the tree has no literal left for a weakened scanner to miss, and fails on the six
lines the scanner is run on first.

**Three sentences corrected beside it, all saying the same wrong thing.** `CMakeLists.txt`,
`include/orderbook/version.hpp` and the test's own header called the startup line **the only**
place the version appeared in the C++; #90's entry below says so too, and is left as written,
because it records what #90 believed. It was not the only place: it was the one somebody looked at.
`version.hpp` also cited the drift guard by a name no test has (`TheVersionIsNotRetyped`).

- Effort: S | Impact: the first version bump would have shipped a banner naming the previous
  release, under a guard that could not see the defect it was written after

### 147. The io_uring transport sent a pipelining client bytes of the server's heap, and ignored `--fsync-policy` ✅

**Closed by removing the transport.** `ob_tcp_server_iouring`, `OB_USE_IO_URING`,
`src/io_uring_server.cpp` and the flags only it read are gone, and so is the `io-uring-build`
required check, which built it and nothing else. Three measurements, and the last is the one that
settled it.

**What a pipelined batch received.** Six commands in one write — `PING`, two `INSERT`s, an unknown
command, a `BOOK`, `PING` — answered through each transport, with two controls: the same batch one
command at a time, and the same batch on the epoll transport. Measured on the m9g.xlarge:

| | one command at a time | the batch in one write |
|---|---|---|
| epoll | 177 bytes, correct | 177 bytes, identical |
| io_uring | 177 bytes, correct | **177 bytes, not the answers** |

The io_uring bytes began `\x8b\x00\x00\x00\x00\x8b\x00\x00\x00\xb6\xe0\x0c\t\x00…` and held
fragments of other answers — **memory of the server process, sent to a client**. The mechanism is
one line: `IoUringServer::submit_write()` did `pending_writes_[fd].assign(data, len)` and prepared a
send pointing at that string's buffer. Every answer of one read calls it for the same descriptor
before the ring is submitted, so every send of the read pointed into one string that each later
`assign` rewrote — and reallocated when a longer answer came along, which left the earlier sends
pointing at freed memory. The length of each send was right, so the byte count came out right.
Sequential clients never have two answers in flight, which is why nothing saw it: no test drove this
transport with a pipeline, and no CI job ran it at all (#108 built it, and said that was all).

**What `--fsync-policy every` did.** 200 acknowledged writes under that flag: **201 fsyncs** on the
epoll transport and **2** on io_uring. `IoUringServer` constructed its engine with
`FsyncPolicy::INTERVAL` and a 100 ms flush interval as literals, so the flag parsed, was accepted, and
did nothing — #113's defect (acknowledged writes that were not synced) in the transport #113 did not
look at, and without even the `EIO` to make it visible.

**What else it did not carry, read rather than measured:** no multi-master configuration reached its
engine, subscriptions were handed a null hub, sharding a null coordinator, and every `--tls-*` flag
was refused. Every server feature added since it was written had been added to one transport.

**And what it was for.** The transport existed to be the fast path — its own comment put `PING` at
24 µs against 45 µs on epoll, on the reference machine, before #144 gave epoll a spin window. Measured now,
m9g.xlarge, five interleaved rounds of 20,000 `PING` round trips through a bare socket:

| | p50 | p99 | server CPU for 20,000 |
|---|---|---|---|
| epoll, `--profile eco` | 7,733 ns | 8,053 ns | 0.070 s |
| io_uring | 7,487 ns | 7,876 ns | **0.200 s** |
| epoll, `--profile boost` | **6,295 ns** | **6,774 ns** | 0.060 s |

`boost` answers faster than io_uring did, for **a third of its CPU**. With no measured
advantage left, the choice was between fixing two defects of this severity in a second copy of the
server loop — and then keeping it in step with every feature, including the multi-reactor work that
is next — or deleting it. It is the same decision as #114, for the same kind of reason: the cost of
the component was a failure mode, and there was no measurement on the other side.

**What would bring it back** is a measurement, not a preference: an io_uring loop that shares
`Session`'s output buffer and the engine's construction with the epoll loop rather than copying
them, and that beats `boost` on the table above. The spec it was built from is still in
`kiro-workspace/specs/io-uring-transport/`, with a note at its head naming this item.

**What removing it took, and the order it had to happen in.** Four files deleted (the transport,
its header, its property tests, and #117's test reading its source), its build block, its metrics
and the one helper only it called, three flags, and every test that checked a rule "in both
transports" — which now check it in one, with the reason kept beside them, because the multi-reactor
stage adds loops and a rule written for two is exactly what a new one forgets. Two refusals stand in
its place: `-DOB_USE_IO_URING=ON` fails at configure time naming this item, rather than configuring
cleanly and leaving a build script to look for a binary that no longer exists; and `--ring-size`,
`--no-sqpoll` and `--sqpoll-idle-ms` are unknown to the parser, which refuses a flag it does not
know by name (#36). All three were accepted by the **epoll** binary too — `--print-config` reported
them back as set — and did nothing there, which is pitfall 27 in flags nobody would have thought to
try. And the required check had to go
**before** the merge: a pull request that deletes a job can never report it, so the live ruleset was
narrowed to thirteen first, read back, and only then was this merged.

- Effort: S | Impact: P0 in consequence on the builds that used it — heap memory to a client, and a
  durability flag that did nothing — and one server loop to make parallel instead of two

### 146. The io thread sent one segment per answer, so a pipelined batch of 64 cost 64 sends ✅

**Closed.** The answers to every command parsed from one read are queued into the session's buffer
and go out with **one** `send()` after the read's last command, and the read is 64 KiB rather than
4 KiB, so one read is one batch. What a client reads is byte for byte what it read before.

**Found by measuring what the next piece of work would parallelise, before parallelising it.** The
first step of making the engine use every core it is given
(`kiro-workspace/specs/uses-the-whole-machine/`) was a baseline, and it said two things. The rate
did not move with the number of writers — about 3.14 million levels a second at one connection and
at four — and one thread, the epoll loop, sat at 0.88 of wall time throughout. Flat in connections
with one thread saturated means that thread is the ceiling. A profile of it then said the thing
worth fixing before splitting it into several: **32.4% of it was sending answers**, one `send()`
per response and 64 per batch, each its own segment since #140 turned Nagle off, with the kernel
running the loopback receive path of every segment inside the sender's own call. More reactors
would have multiplied that cost rather than removed it.

**Measured, m9g.xlarge (4 cores, aarch64), loopback, a fresh node per run with its data on the
instance's EBS volume, five rounds with the order rotated so every build ran first, second and
third; `loadavg` 0.15–0.91.** Batches of 64 twenty-level `MINSERT`s, 12,000 batches a run, every
answer read before the next batch is sent; sends and reads counted by the kernel's syscall
tracepoints. `scripts/measure_pipelined_ingest.py` driving `benchmarks/pipelined_ingest.cpp`
reproduces it.

One connection:

| | levels/s | range | batch p50 | batch p99 | server CPU | sends per batch | reads per batch |
|---|---|---|---|---|---|---|---|
| before | 3,121,306 | 3.113–3.152 M | 363.9 µs | 395.1 µs | 4.70 s | 64.00 | 8.00 |
| one send per read | 4,591,504 | 4.585–4.625 M | 236.6 µs | 265.7 µs | 3.19 s | 6.00 | 8.00 |
| and a 64 KiB read | **4,845,612** | 4.833–4.865 M | **222.4 µs** | **251.3 µs** | **3.05 s** | **1.00** | **3.00** |

**+55% levels a second, −39% at p50 and −35% of the server's CPU for the same 15.36 million
levels.** The ranges do not overlap, and the same comparison through a second harness an hour
earlier gave 3.14, 4.59 and 4.85 million.

Four connections:

| | levels/s | batch p50 | batch p99 |
|---|---|---|---|
| before | 3,137,759 | 1,437.6 µs | 14,139.4 µs |
| one send per read | 4,623,883 | 929.0 µs | 2,059.2 µs |
| and a 64 KiB read | 4,895,226 | 877.8 µs | 1,980.3 µs |

**What the tables do not say, and it is the larger half.** This is the same one thread doing less
per batch: the io thread is at 0.84 of wall time afterwards, and four connections still reach the
rate of one. That is the next stage — more than one reactor — and this one came first because it is
a per-batch cost every reactor would have paid. The four-connection p99 falling from 14.1 ms to
2.1 ms is not a second effect: one thread serves four clients, so a batch queues behind the others,
and the queue is shorter when each batch costs less.

**The client pays less too.** The probe's own CPU for a run fell from 1.49 s to 0.06 s, because it
receives one segment per batch where it received 64.

**Every read in the counts is accounted for.** A batch is 20,736 bytes. Of the eight reads per
batch before, six carried data — a 4 KiB read takes that batch in six pieces — one found the socket
empty and ended the edge-triggered drain, and one was the subscription hub's eventfd, which the
loop drains on every pass whether or not it fired (attributed with `strace`, whose own slowness
also removes the empty read by letting the next batch arrive first — so the counts come from the
tracepoints and the attribution from `strace`, and neither alone). With the 64 KiB read that is one
of each. The eventfd read is a syscall on every pass of the loop for a feature most connections do
not use; it is a candidate for the latency work and is measured there rather than removed here.

**What did not change, and is held by tests rather than by this paragraph.**
`tests/integration/test_pipelined_answers.py`, eight tests: the bytes and their order are what one
command at a time produces, in the clear, compressed and over TLS; a batch several reads long is
answered whole and in order; `QUIT` delivers everything before it, **including while those answers
are still draining**; a line too long delivers the answers before it; `COMPRESS` inside a batch
frames exactly what follows its acknowledgement; and a failed `AUTH` ends the batch where it
stands, with a control proving that the same write is made when no failure precedes it — one
attempt per connection is #30's whole rate limit.

The half of this that matters for speed is invisible on the wire, because the bytes are the same,
so `ReadLoopStatic.EveryCommandFromOneReadIsAnsweredWithOneSend` pins the loop's shape: answers
queued inside the loop over a read's commands, no send and no flush there, and exactly one flush
after it.

**Mutation table: ten rows, each with the verdict it was supposed to produce** — eight killed and
two surviving, committed before the first mutation, every source restored from bytes kept beside the
run and touched so the build sees it, and each row's run made of the static test plus 59 wire tests
across seven integration modules:

| | mutation | verdict | killed by |
|---|---|---|---|
| 1 | a flush after every answer, inside the loop | killed | `ReadLoopStatic` only |
| 2 | `send_response` in place of `queue_response` | killed | `ReadLoopStatic` only |
| 3 | a second flush after the loop | killed | `ReadLoopStatic`: exactly one flush |
| 4 | no stop after a failed `AUTH` | killed | `test_a_failed_auth_ends_its_batch_where_it_stands` |
| 5 | compression switched on before its acknowledgement is queued | killed | `test_compression_negotiated_inside_a_batch_frames_everything_after_it` |
| 6 | a line too long closes without flushing what came before it | killed | `test_a_line_too_long_behind_a_batch_delivers_the_answers_before_it` |
| 7 | `QUIT` closes while answers are still draining | killed | `test_quit_behind_answers_still_draining_delivers_all_of_them`, and only it |
| 8 | the #143 ceiling disabled | killed | both ceiling tests, now that they wait for the refusal |
| 9 | **control:** the loop's two early exits in the other order | survived | a batch can set at most one of them |
| 10 | the read back to 4 KiB | survived | by design |

Rows 1–3 pass **all 59 wire tests**, which is the case for a static test made in one line: the
cost this item removes cannot be seen from the socket. Row 7 fails no test that existed before this
item — the old `QUIT` test survives it. Row 10 survives on purpose: the read size is throughput
rather than correctness, a test for it would be a gate on a clock, and the measurement above is its
evidence.

**Four things found on the way, every one of them in a test or an instrument rather than in the
engine.**

- **The #143 ceiling tests asserted an ordering between two processes.** They read their counter
  the instant the client closed, and the 64 KiB read made one of them fail. Measured
  on the i3-7100U with a probe repeating the test's own scenario forty times: with the 64 KiB read
  **4 of 40** immediate reads missed a refusal that landed 1.5–2.1 ms later, and 33 of the 40
  clients managed to send all 4 MiB; with the 4 KiB read the server drained more slowly, flow
  control held the client until the server's reset, and **0 of 40** missed. Settled, none missed in
  either build — the property held throughout and the instant was the thing that had been lucky.
  Both tests now wait for the counter, and disabling the ceiling still fails both, in 22 s.
- **`QUIT` defers its close while answers are still queued, and nothing reached that branch.** The
  test for it sends twenty `PING`s, whose answers fit the socket buffer, so the flush leaves nothing
  pending. A hundred full books behind a receive window clamped to 64 KiB do not — and the mutation
  that closes at once is now killed by that test alone.
- **A premise described instead of asserted.** The test for a batch spanning several reads said its
  2000 commands were "about 10 kB"; they were 96 kB, so "several reads" had been true by accident,
  and the 64 KiB read made it two. It sends 288 kB now and asserts that it is more than four reads.
- **The measurement script's first version wrote each node's data to `/tmp`, and `/tmp` on this
  instance is a tmpfs.** The same three builds read **3.46, 5.26 and 5.57** million levels a second
  there against 3.12, 4.59 and 4.85 on the disk: 11-15% of every figure was the WAL going to
  memory. This is pitfall 326, recorded three days earlier about the comparative harness
  and met again by a script written after it. The script now refuses a data directory on memory and a
  build that is not Release, with the comparative harness's own checks rather than a second copy of
  them.

**Not measured here, and said rather than implied.** Over TLS a read returns what `SSL_read` hands
back, which is bounded by the record rather than by this buffer, so the send count per batch over
TLS was not measured; the TLS test holds the bytes, not the count. And the io_uring transport has
its own read path and is untouched here.

- Effort: S | Impact: +55% pipelined ingest on the same thread, the per-batch cost every reactor of
  the next stage would otherwise pay

### 145. The engine's central data structure was not readable over the wire ✅

**Closed.** `BOOK <symbol> <exchange> [depth]`, on the wire, in the Python client and in `ob_cli`.

`SoABuffer` **is** the current book — per side a depth and its levels, updated in place by
`apply_delta`, read consistently under a seqlock — and the engine keeps one for every symbol it has
seen. Checked in the code rather than recalled, because #130 paid for a filed mechanism that named
the wrong function: **`read_snapshot()` had exactly one caller**, the aggregate branch of
`QueryEngine::execute()`, feeding `is_agg_expr` — VWAP, MID_PRICE, IMBALANCE and the rest. A row
`SELECT` goes to `store_.scan()`, which is the **columnar history**.

So a client could ask for VWAP *over* the book and could not ask for the book. Of the eighteen wire
commands, none returned its levels, and the two routes a client had were both indirect and both
made it hold what the server already holds: `SUBSCRIBE` and rebuild from the stream, or `SELECT`
history and replay it.

**Why this is worth more than its size.** It is the most orderbook-specific question there is, and
the one where a column store is not slow but **structurally wrong**: answering it from history means
finding the latest row per (side, price) over everything stored. Ours is a `memcpy` per side under a
seqlock. It is also why the comparative table said `NOT COMPARABLE` about exactly the workload this
engine exists for — the two comparable columns there, bulk ingest and a time-range scan, are generic,
and we lose both. With `BOOK` the same question becomes expressible for all three systems.

**A separate command rather than a form of `SELECT`.** In this protocol `SELECT` means history, and
a form that silently meant something else is the worst kind of addition: a client that writes
`SELECT * FROM 'BTC'.'EX' LIMIT 10` gets ten rows of history today and would get ten levels of the
book after such a change, with no signal. Two bare tokens rather than the query language's
`'SYM'.'EXCH'`, which is where the spec's first sketch changed: the quoted form belongs to the
commands this layer hands to the SQL parser, and `BOOK` is tokenised here like `INSERT` and
`MINSERT`, the other per-symbol tokenised commands. A quote parser for one command would be a second
syntax for one idea.

**One snapshot per answer, and the limit named rather than smoothed over.** A single
`read_snapshot()` fills both sides before any row is emitted. The alternative — reading a side as
its rows are formatted — composes the answer from two moments separated by the formatting of up to
two thousand levels, and a book assembled that way can show a crossed spread the market never had.
What `BOOK` is **not** is an atomic read of both sides: `read_snapshot()` spins per side, so the two
halves are microseconds apart. Making it atomic means one seqlock per buffer instead of one per
side, which is a change on the write path and not this item's to make.

**Two of the seven columns are properties of the read.** `timestamp_ns` and `sequence_number` come
from the buffer, so every row of one answer carries the same pair. That is stated in `docs/cli.md`
and in `docs/python.md` where a client reads it, because the column names suggest a per-level time —
and it is **useful** rather than filler: the pair says *as of which update* this book is, which is
the number to resume a `SUBSCRIBE` from.

**The lookup, not a second copy of it.** The buffer is resolved through the same
`LiveBufferLookup` the aggregate branch uses and held for the whole answer, which is #92: `buffers_`
owns these and a snapshot install replaces the store (#142), so a read still going through a
resolved raw pointer reads freed memory — measured then as `heap-use-after-free` in 3 of 3 ASan
runs. `read_book()` therefore lives in `QueryEngine`, which owns that lookup; putting it on `Engine`
would have made a **second supplier**, which is what the static test in
`tests/test_query_live_buffer_race.cpp` exists to refuse, because the C API had the identical race
one file away.

And the same sentence in the other direction, which #92's own first test paid for: a test of
that class must **dereference** the buffer. `SELECT *` resolves it for an existence check and never reads
through it, which is why it drove that race for three clean ASan runs, and why #91's test had to pick
VWAP deliberately. `read_book()` cannot do anything else — every row it emits comes out of the
snapshot it took through that pointer — so it is a **better** instrument for the class than the
driver that had to be chosen.

**Three things the grammar gives for free, and one number that had two copies.** The arity row makes
a nineteenth command a *compile* error until it declares itself; the authentication gate applies
because it sits before the `switch` whose classifier has no `default:`; and the refusals name the
token. The depth ceiling is `MAX_LEVELS`, and writing that line found that
`SoASide::MAX_LEVELS` and `ob::MAX_LEVELS` were **two constants of the same value with nothing
comparing them** — the first is the array length, the second is what the parser refuses a `MINSERT`
above and what sizes every WAL payload buffer. Diverging would not be memory-unsafe, because
`insert_level` answers `OB_ERR_FULL`, but a `MINSERT` accepted by the parser and refused level by
level is a refusal from the wrong layer. One `static_assert` now ties them, which is the version
number in three places (#107) caught before it cost anything.

**What it deliberately does not do.** No column projection — that is #139's and belongs to `SELECT`;
a second column list to keep in step is a second thing to get wrong before anybody asked. Nothing to
`SUBSCRIBE`, whose column header is a protocol change #139 recorded as open. No history: "how did
the book look at 9:30" is a range query and stays one. And **no C API entry point**, so the Python
client refuses `book()` in local mode with a message that says why — the C API is a published ABI
and extending it is separate work, not something to smuggle in behind a keyword argument.

**Measured on an m9g.xlarge over loopback, three interleaved rounds of 20,000 round trips per
level count, `loadavg` 0.07-0.34, with a `PING` in every round as the baseline** — because on
loopback the round trip is most of a small answer, so a `BOOK` latency quoted alone would be read
as the cost of the read when it is mostly the cost of the socket:

| levels per side | `PING` p50 | `BOOK` p50 | difference | `BOOK` p99 | answer | per level |
|---|---|---|---|---|---|---|
| 1 | 7941 ns | 8636 ns | **695 ns** | 8940 ns | 149 B | 348 ns |
| 10 | 8044 ns | 9653 ns | **1609 ns** | 10 021 ns | 851 B | 80 ns |
| 100 | 7919 ns | 16 978 ns | **9059 ns** | 17 611 ns | 8071 B | 45 ns |
| 1000 | 7974 ns | 114 359 ns | **106 385 ns** | 131 892 ns | 83 691 B | 53 ns |

Medians of the rounds; the `BOOK` p50 spread within a level count is under 0.2% and `PING`'s across
all twelve rounds is 7816-8111 ns, so every difference above the first is an order of magnitude
outside the floor, and the first is still four times it.

**The honest reading, which is not the flattering one: the book read is nearly free and the wire
format is not.** Ten levels per side answers in 9.7 µs, of which 8.0 is the round trip. The marginal
cost settles at **45-53 ns per level**, and server CPU says where it goes — 0.08 s for 20,000
`PING`s against 2.14 s for 20,000 full books, which is ~107 µs of CPU per 2000-level answer against
a snapshot that is two `memcpy`s of about 16 kB. So what a client pays for a **deep** book is TSV at
roughly forty bytes a level, not the seqlock. That is worth stating plainly because it is also the
answer to "should this have a binary form": the read is not the thing to optimise.

**And the probe had the defect first, which is the reason `answer_bytes` is in its output.** The
first version assembled its command as `std::string command = "PING"` and then *appended* the
arguments, so every round trip sent `PINGBOOK SYM EX` and both columns measured the server's
`ERR unknown command`: two plausible numbers **a hundred nanoseconds apart**, which reads exactly
like "the book read is free". Nothing in the latency said so. Twenty bytes of answer for a thousand
levels per side did. A measurement whose control does not pass is not a measurement, so the
instrument is in the repository now rather than in a scratch directory:
`benchmarks/command_latency.cpp` refuses an error answer instead of timing it and reports the size
of the answer it timed, and `scripts/measure_book_latency.py` refuses a book whose answer is not
the 2N + 3 lines N levels per side must be. Run once more on the development laptop, the committed
probe reproduces the answer sizes above to the byte - 149, 851 and 83,691 - and its latencies,
from a machine with a load average near three, are not this table and are not offered as one.

**The write path is untouched, and that is a diff rather than an assurance.**
`scripts/mnemonic_diff.py` against master, Release: `apply_delta_impl` **588 → 588**,
`apply_delta_mm` **647 → 647** and `WALWriter::append` **111 → 111**, mnemonic sequences identical
in all three. Measured on `apply_delta_impl` and not on `apply_delta`, which reads as two
instructions in both builds because its body moved to that function years ago — a symbol matched by
the name a reader expects can report a tail-call shim and call it the write path (pitfall 251).

**And two of its own tests came out of a surviving mutation.** Dropping the depth ceiling and
reading an explicit `0` as "no depth given" both **survived** the C++ suite, because the wire
refusals for this command were tested exclusively over a socket — so the unit job and the mutation
harness could not see either. Five parser tests in `tests/test_command_arity.cpp` now cover them,
each with the control that says what it is about: the ceiling accepts its own boundary, omitting the
argument still means "everything", and a valid line still parses.

**Mutations: ten, each with the verdict it was meant to give.** Baseline green before and after,
sources restored from bytes kept beside the harness rather than from `HEAD`.

| mutation | verdict |
|---|---|
| the two sides are read by two snapshots rather than one | **killed** |
| depth is ignored, so every answer is the whole book | **killed** |
| depth bounds the whole answer rather than each side | **killed** |
| asks are emitted before bids | **killed** |
| the snapshot identity is left at zero on every row | **killed** |
| the owning handle is dropped for a raw pointer | **killed** |
| a missing symbol is not refused | **killed** |
| the depth ceiling is dropped | **killed** — survived until the parser tests existed |
| an explicit depth of zero is read as "no depth given" | **killed** — survived until the parser tests existed |
| the not-found refusal is reworded | **survives** (control) |

Row five was reshaped once rather than the code changed for it: written as `r.timestamp_ns = 0`, it
left `ts` unused and `-Werror` refused to build it, which is a verdict about the compiler and not
about the tests. `ts * 0` is the same mutation that compiles.

- Effort: M | Impact: the one question this engine answers better than a column store by
  construction, expressible for the first time — and the reason the comparative table could not
  speak about the workload the product is named after

### 144. The io thread always blocked between events, so every round trip paid a kernel wake-up ✅

**Closed.** `--profile eco|boost`, with `--io-spin-us` as the knob underneath. A profile is a named
set of the knobs and not a second code path: nothing in the loop branches on its name, and at
`--io-spin-us 0` — the default when this closed, and what `eco` resolves to — `io_wait_ms()` returns
the blocking timeout the loop always used. (**Since #158** `boost` is the default, sized to the
machine, and its window is 10 µs rather than the 50 measured below: the gain was complete at 10,
and past it the window was tail.)

**The gate for building it at all was a measurement, and it corrected what the mode is for.** The
first version of the design said `boost` buys cores for throughput. That was wrong about the
customer: the client loop blocks in `epoll_wait`, so every request waits for the kernel to wake the
io thread, and a trading firm buys the latency tail rather than batch throughput.

**Measured through the flag, m9g.xlarge, seven interleaved rounds of 20,000 `PING` round trips
through a bare socket** (a C++ probe, so the number is about the server and the kernel rather than
about a client), the order alternated, `loadavg` 0.01-0.05 throughout:

| | p50 | p99 | minimum | server CPU for 20,000 | while the probe ran |
|---|---|---|---|---|---|
| `eco` | 8074 ns | 8369 ns | 7573 ns | 0.090 s | 56% of a core |
| `boost` | **6578 ns** | **6837 ns** | **5993 ns** | 0.120 s | 91% of a core |
| difference | **−18.5%** | **−18.3%** | −20.9% | **+33%** | |

Medians of the rounds. `eco`'s p50 spans 7978-8130 ns round to round and `boost`'s 6499-6640, so
the difference is an order of magnitude wider than the spread it is read against.

**What says this is a constant on every round trip rather than a tail: p50 and p99 fall by the same
absolute amount**, 1496 ns and 1532 ns. A tail would take far more off p99.

**The bounded window costs nothing against spinning for ever, and the third column is what makes
the window defensible rather than cautious.** `--io-spin-us 100000000` never closes, which is byte
for byte the `epoll_wait(..., 0)` variant the gate was measured on: three rounds gave p50
**6578 ns** — the same median as `boost` — for **0.130 s** of CPU, 98% of a core. Same latency,
lower idle cost, and on an idle node the difference is the whole of it: always-spin holds that core
indefinitely while `boost` returns to nothing, which is why the public cost is **"up to one core
while traffic flows"**. The integration test asserts the idle half, because it is the one claim
here that nothing else can check.

**And one claim from the gate measurement is withdrawn rather than quietly replaced.** That run
reported the **minimum unchanged** (5964 against 5915 ns) and I published "the minimum does not
move" as what distinguishes a wake-up from a tail. It does move: `eco`'s minimum is 6327-7689 ns
across seven rounds against `boost`'s 5955-6076, about the same 1.5 µs as the percentiles, with
more spread because a blocking loop is occasionally already awake when the next request arrives.
The gate's figure does not reproduce with this probe and I cannot explain it, so the honest course
is to say so and rest the claim on the two percentiles falling by the same amount, which is
stronger anyway. The gate was a hand-edited loop; this is the flag.

**What the measurement does not say, and it belongs next to the number rather than in a footnote:**
this is loopback on one machine. Across a real network a round trip is orders of magnitude larger
and 1.5 µs stops being 18% and becomes noise. The mode is worth exactly what it is worth to a
**colocated** client, and `docs/cli.md` says so where an operator will read it.

**Three refusals, each because the alternative is silent.** An unknown name is refused with the
name in the message (#27 and #36 on the wire's other side: a parser that ignores what it does not
understand hides operator mistakes, and here the mistake is a node started in `eco` that its
operator believes is in `boost`). The profile sets only what the operator did not, so a value from
the command line or the config file wins. And what the profile did set is attributed to the profile
(`Origin::Profile`), so `--print-config` answers "what is this node doing" rather than "which
switches were thrown" — a mode whose effect cannot be read is a mode on somebody's word.

`io_wait_ms()` is a pure function for the same reason `drain_verdict()` is (#106): the alternative
is a clock gate, and a test whose threshold is a duration fails on legitimate variation and teaches
its reader to re-run it. The static half is what the unit tests cannot reach — replacing `wait_ms`
with a literal at the one `epoll_wait` call site reinstates the blocking behaviour and leaves every
unit test green, because they measure the function rather than its use.

**And the integration module adds the two questions that need a process, which is the only reason it
exists.** A node with the mode on serves normally, with `eco` as the control in the same
parametrised test — the window lives inside the event loop, so a mistake there costs missed events
rather than a wrong answer, and a wrong answer is what a client sees. And the window **closes**:
three seconds of idling with a threshold three orders of magnitude clear of the value (#129's
shape), plus a `PING` afterwards, because a window that closed because the loop stopped looking
would satisfy a CPU assertion perfectly.

**Mutations: seven, each with the verdict it was meant to give — and the last row is the reason
the other six are worth reading.**

| mutation | verdict |
|---|---|
| the loop takes a literal timeout instead of `io_wait_ms()` | **killed** by the static test only |
| the profile sets the spin unconditionally, overwriting the operator's value | **killed** |
| the profile block accepts any name | **killed** |
| the value is attributed to `Origin::Default` rather than `Origin::Profile` | **killed** |
| the window boundary is `<=` rather than `<` | **killed** |
| `kBoostSpinUs` widened past the documented window, to 5 ms | **killed** |
| `kBoostSpinUs` retuned inside the documented window, to 200 µs | **survives** (control) |

**Row one is why the static test exists, and it is there because the first version of that test did
not kill it.** That version asked only whether `io_wait_ms(` appears in `src/tcp_server.cpp` —
which it does, as its own definition — so replacing the computed timeout with a literal **survived**
a check satisfied by a mention rather than by a use. It requires every assignment to `wait_ms` to
come through `io_wait_ms(` now, and counts them.

**Rows six and seven are one claim from two sides, and the control had to be fixed twice.** Every
expectation about the window is written in terms of `kBoostSpinUs` rather than pinned to 50, because
pinning the number fails on any legitimate retuning and teaches a reader that the test is noise
(#121's lesson). What the two rows say is that the bounds are load-bearing: zero makes `boost`
identical to `eco` and a window past a millisecond is a node that mostly spins, so a widening large
enough to change an answer **does not compile**, while one small enough to compile changes nothing
observable. The first draft of row seven said 5.5 minutes, copied from #121 where the bound is
measured in minutes — a **kill** here, caught by writing the table before running it. And the
corrected 200 µs killed as well, which is the whole value of keeping a control: the provenance test
was looking for the literal `"50"` in the rendered line, which is #121's mistake committed in the
test written to honour it, invisible to reading, and findable only by a mutation that moves the
constant.

Baseline green before and after; sources restored byte for byte, with the mtime touched, because
`copy2` restores the backup's timestamp and the rebuild after it then does nothing.

- Effort: S | Impact: the first knob in this engine that trades CPU for latency rather than for
  durability or throughput, and the only one whose public cost has to name the deployment it is
  worth anything in

### 143. One session could hold unbounded unparsed input, and the limit that exists cannot see it ✅

**Closed.** Found while profiling the ingest path for something else — reading `Session::feed()`
to see where its 9.97% of the server's CPU went, not looking for this.

**Measured, m9g.xlarge, one connection, no authentication:** a client that sends bytes and never
sends a newline took the server's resident memory from 1.9 MiB to **257 MiB** after 227 MiB on the
wire, with the server draining the socket as fast as it arrived. There is no cap: `feed()` appends
to `read_buffer_` and erases only up to the last **complete** line, so a stream with no line in it
is never erased.

**`--max-line-length` exists, is 256 KB, and cannot bound this.** The check is
`if (line.size() > config_.max_line_length)` inside the loop over the lines `feed()` returned — so
a client that never completes a line produces no lines, the loop body never runs, and the check
never executes. The limit is real and guards the wrong thing: the size of a command that already
exists, not the size of an accumulation that might never become one.

**There are two routes and a fix for one would have left the other.** Beside the receive buffer, a
pending `MINSERT` collects its payload lines into `minsert_lines_`, and those lines **do** end in
newlines — they simply never come back from `feed()`, so the per-line check cannot see them either.
A client that announces a thousand levels and then sends 32 KB payload lines accumulates just as
freely. That is why the ceiling is one number over both (`Session::unparsed_bytes()`) rather than a
check on the buffer: two accumulations with one cap have no gap between them.

**The bound is twice `max_line_length`**, because a session can legitimately hold one `MINSERT`
block being assembled *and* the start of the next command, each bounded by the line length.
Anything past that is not a command in progress. Deliberately with **no flag and no disabling
value**: its sibling `max_line_length` has no flag either, so claiming otherwise would promise
configurability that does not exist, and a value nothing could set would be an untestable branch
reading as an option this engine offers.

`ob_sessions_unparsed_overflow_total` is the only external sign, registered in the change that
writes it.

**The control is the test that matters.** A ceiling low enough to break real traffic would pass
both refusal tests and be a worse defect than the one it closes, so a 1000-level `MINSERT` — what
`max_line_length`'s own comment says the protocol supports — has to keep working, and is asserted
on the row count rather than on the absence of an error.

This is the input-side mirror of **#69**, which capped the send buffer at 64 MB per session after
clearing a partially-sent one corrupted the peer's framing. The output side was bounded two years
before the input side, which is the ordinary direction for this mistake: the bytes you send are
yours to count, and the bytes you receive arrive whether you counted them or not.

**Mutations: four, each with the verdict it was meant to give — per test, because the point of
three tests is that they answer differently.**

| mutation | no-newline | pending `MINSERT` | the control |
|---|---|---|---|
| no ceiling at all | **fails** | **fails** | passes |
| the ceiling counts the receive buffer only | passes | **fails** | passes |
| the ceiling is 1024 bytes | passes | passes | **fails** |
| the refusal is reworded | passes | passes | passes |

Row two is the design decision, demonstrated: a fix that bounded only the receive buffer leaves the
`MINSERT` route wide open, and one number over both accumulations is what closes it. Row three is
why the control exists — a ceiling below real traffic passes both refusals and would have shipped.
Row four is the control mutation and survives. Baseline green before and after; sources restored
byte for byte.

- Effort: S | Impact: P0 by consequence — a single unauthenticated connection exhausts the memory
  of a node that is otherwise healthy, and the refusal it needed was one comparison

### 142. A replica bootstrapped by snapshot keeps rows the primary does not have, for a symbol it already held ✅

**Closed.** The staged files replace the store now, in one exclusive operation, rather than being
renamed in beside it.

**The evidence, with both nodes' data directories preserved.** The replica ends with *three*
segment directories for a symbol the primary has two of, and the extra one is its own:

| node | segment directories for that symbol | rows |
|---|---|---|
| primary | `…483039636760`, `…483041495609` | 100 between them |
| replica | **those two, plus its own `…018354201021`** | **122** |

The extra 22 are the replica's own, flushed before it was killed. An earlier run of the same test
gave 88 + 100 = **188**: the number varies with how much the replica had flushed when it died,
which is the whole shape of the defect. After the fix the replica's directory list is the
primary's, name for name.

**The mechanism, and it is none of the three this item was filed with.** `Engine::load_snapshot()`
cleared `stores_`, `buffers_`, `pending_rows_` and the sequence frontier — **memory only** — and
then rebuilt the index from *whatever was on disk*. `ReplicationClient::install_snapshot()` renamed
each received file into place, overwriting a colliding path and touching nothing else. A segment's
directory name **is** its event-time range, so a replica killed mid-stream has flushed a *prefix*:
its directory ends earlier, the two names share a start and differ in the end, and that is an
**overlap rather than an equality** — nothing for #136's duplicate-directory guard to refuse. The
snapshot's segment was installed, no WAL record was re-applied, and the old segments did not
survive a clear: **there was no clear.** The function was named for the caller's intention, not for
what it did.

**Why CI was green, as a mechanism rather than as "timing".** If the replica flushes **all** of the
symbol's rows before it is killed, its directory is named by the same range as the primary's, the
rename overwrites it file by file, and the count is exactly right. The defect needs a *prefix*. So
the green runs were never "no defect"; they were "the stale directory happened to be the one being
overwritten".

**The fix is one method, on the component that owns the directory layout.**
`ColumnarStore::replace_from_staging()` holds `index_mtx_` **exclusively for the whole swap**:
drop the index, remove the segment directories, move the staged files in, re-read. A scan takes
that same mutex shared, so it waits and then sees the new store. That is why it did not go in the
caller: clearing there and rebuilding afterwards would have **moved** the window rather than
removed it — `ColumnarStore::close()` keeps the index and every read path opens files per call, so
a scan in the gap answers *short* rather than failing. The window already existed, as a rename over
live files; it is gone now.

It spares anything named `wal_*` (the WAL and `wal_identity`), plain files (`repl_state.txt`) and
**the staging directory**, which both callers put *inside* the data directory — a clear that did
not know that would delete the files it was about to install, and there is a test for exactly that.

`Engine::load_snapshot()` is two functions now, because one of them was a lie:
`install_snapshot(staging_dir, manifest)` does the whole thing, and `adopt_store_on_disk()` is the
memory half under a name that cannot be mistaken for an install. Both installers lost their rename
loops; an unsafe path now refuses the **whole** install rather than skipping one entry, because the
store is about to be replaced by exactly that list and an entry we decline is a hole in it.

**One thing only the rebase could show.** #137 gave `pending_rows_` a waiter with a five-second
deadline, and both halves of this change clear that vector under `mtx_` without waking it — which
on their own branches was nothing, because before #137 nobody was asleep there. Together it is a
writer that waits out the deadline and is **refused** after room had already been made. Narrow (a
node installing a store is bootstrapping, and a replica takes no client writes) and one line, but
it is the shape worth naming: two changes that are each correct alone, meeting for the first time
in a rebase. `pending_cv_.notify_all()` after the clear, in both.

**Mutations: five, each with the verdict it was meant to give.**

| mutation | verdict |
|---|---|
| the removal loop never runs | **killed** — 122 rows again, and the unit test for the overlapping name |
| the staging directory is not spared | **killed** — the clear eats the files it is about to install |
| `wal_*` is not spared | **killed** — the WAL and `wal_identity` go with the segments |
| a failed move reports success | **killed** — an install that installed nothing reads as done |
| the closing log line is reworded | **survives**, and it is the control |

The control is the row that says the other four mean something: a table in which everything dies
reports a broken harness as diligence. Baseline green before and after, and the source restored
byte for byte — with `copyfile` plus a touch rather than `copy2`, which keeps the backup's mtime
and leaves the rebuild with nothing to do (pitfall 272, twice in this repository).

- Effort: M | Impact: P0 by consequence — a replica answered `SELECT` with rows its primary never
  had, silently and durably, after the documented recovery from a truncated position

### 141. The engine could be pipelined and no client of ours could do it ✅

#140 measured **2,174,287 levels/s against 1,314,663** for a client asking one question at a time,
and the client that measured it was a forty-line C++ probe written for the occasion. Both of the
clients this repository ships send one command and wait for its answer, so that number described
a client nobody had — the mechanism present on one side of the wire and absent on the other, which
is the shape this workspace has filed seven times under other names.

Nothing on the wire changes. `Session::feed()` has always returned every complete command from one
read and the server has always answered them in order; what was missing is a client that sends
more than one before reading. `_TcpClient._recv_response()` already consumed exactly one response
and left the rest of the buffer alone — it has to, because a `PUSH` may arrive between a command
and its reply (#45) — so the transport half of this is a loop.

**Measured on one m9g.xlarge, 5000 updates of 20 levels, five rounds with the order alternating
between them, medians:**

| | levels/s | client CPU per level | against the loop |
|---|---|---|---|
| `insert()` in a loop | 969,204 | 623 ns | — |
| `insert_batch`, 1 per call | 883,923 | 725 ns | **0.91×** |
| `insert_batch`, 8 per call | 1,261,354 | 531 ns | 1.30× |
| `insert_batch`, 64 per call | 1,542,355 | 496 ns | **1.59×** |
| `insert_batch`, 512 per call | 1,542,096 | 502 ns | 1.59× |

**The row that does not flatter it is the control.** A batch of one is 9% *slower* than `insert()`,
reproducibly across five rounds, and the client CPU column says why: 725 ns per level against 623,
so about two microseconds of extra Python per update — a normalisation pass, an outcome object, a
size sum. For one update `insert()` is the right call and the documentation says so. The API pays
from about eight and stops improving after 64.

**The other thing that column says is where the remaining ceiling is.** At batch 64 the client
spends 0.050 s of its own CPU against 0.065 s of wall: **the Python client is three quarters of
what is left**, and the same work from C++ reaches 2,174,287 levels/s against the same server. So
the honest reading of 1.59× is "as much of #140 as Python can collect", not "what the engine can
do". A client in a compiled language gets the rest.

**It is not a transaction and the shape of the answer says so.** A batch is N independent writes
in one journey; some may land and others be refused. The result is a list of outcomes each
carrying its own index, rather than a return code or one exception, because a caller that filters
or regroups that list would otherwise lose which update bounced — and losing that is the failure
this API exists to prevent. Everything decidable *before* sending is decided for the whole batch,
since a partial send after rejecting update k is a write nobody can find afterwards.

**Three refusals, each because the honest answer is not the obvious one.** Pool and sharded mode:
the router picks a connection per symbol, so a batch spanning symbols is several batches on
several connections and which of them is one round trip is a decision nobody has measured. A
compressed connection: each command is its own LZ4 frame, and whether the server takes several
frames from one read has not been measured — "probably works" is the wrong thing to find out about
in production. And `MAX_BATCH_BYTES`, which is **not** a server limit and says so where it is
defined: the server answers a `MINSERT` in four bytes against a 64 MB per-session cap, so from
that side a batch could carry sixteen million commands. What eight megabytes bounds is the
caller's own memory.

**One definition of the wire spelling, which this change forced.** `insert()` spelled a write
twice — once for pool mode, once for TCP — and a third copy was the natural way to write this. Two
copies of a protocol's syntax is how two clients of one server begin saying different things; the
same rule already governs this engine's identifier quoting, its set of write-shaped operations and
its query header. Four parametrised cases pin the bytes, so the extraction is checked rather than
assumed.

**The C++ client did not get this and that is recorded rather than implied.** The harness that
publishes comparative numbers is in Python, so Python is where the measurement needed a client;
`OrderbookClient::minsert_batch()` is the same loop over a reader that already exists and is worth
doing when something needs it.

**Mutations: nine, each with the verdict it is meant to produce, and the first control is the one
worth reading.**

| mutation | wanted | got |
|---|---|---|
| the wire spelling drops the event time | KILLED | KILLED |
| every write is spelled `INSERT` | KILLED | KILLED |
| only the first outcome is returned | KILLED | KILLED |
| every outcome claims success | KILLED | KILLED |
| the batch ceiling is gone | KILLED | KILLED |
| the level lists need not line up | KILLED | KILLED |
| an event time the server cannot store is sent anyway | KILLED | KILLED |
| CONTROL: the batch is sent one command at a time | SURVIVES | survived |
| CONTROL: the pool refusal is reworded | SURVIVES | survived |

Replacing the single write with a loop of `execute()` — which is `insert()` again, one round trip
per command — **survives every test in this module**, and it should. The tests state what a batch
*stores* and what it *says about each update*, and neither of those changes when the same bytes
take nine hundred round trips instead of one. What the speed claim rests on is the measured table
above, which no test can be a substitute for; a test that gated on it would be a gate on a clock,
and this repository has one of those to point at already. Saying which of the two carries the
claim is the reason that row is in the table rather than left out.

- Effort: S | Impact: #140's measurement becomes reachable from the client this project ships,
  which is the difference between a protocol that allows something and a product that does it

### 140. No accepted socket turned Nagle off, so a client that pipelines paid a kernel timer per round trip ✅

Measured on one m9g.xlarge, quiet box, loopback, one connection, 20,000 updates of 20 levels each
through `MINSERT`. Before and after in the same run, alternating, so a neighbour's periodic load
cannot land on one side of the ratio:

| commands per round trip | before, wall | before, levels/s | after, wall | after, levels/s |
|---|---|---|---|---|
| 1 — the control | 0.306 s | 1,305,742 | 0.304 s | **1,314,663** |
| 8 | 131.885 s | 3,033 | 0.200 s | **1,997,199** |
| 64 | 16.150 s | 24,767 | 0.185 s | **2,159,195** |
| 512 | 2.037 s | 196,409 | 0.184 s | **2,174,287** |

**The server's CPU is the part that says what this was.** For the same 400,000 levels it spent
0.16, 0.12 and 0.14 seconds before, and 0.16, 0.15 and 0.15 after. The 131.885 seconds were not
work. Divided by round trips the before column is **52.75, 51.68 and 52.15 ms** at batch 8, 64 and
512 — the same figure at three batch sizes, which is a timer rather than any per-byte cost.

**Read those as the wire's rate, not as a sustained ingest rate, and the difference is a flush.**
400,000 levels at a 2000 ms flush interval means **no flush fell inside any of the eight runs** —
which is right for isolating the protocol, and wrong for quoting as throughput. The same client
against the same build, sweeping volume at `--flush-interval-ms 1000`:

| levels | wall | levels/s |
|---|---|---|
| 200,000 | 0.092 s | 2,172,964 |
| 500,000 | 0.224 s | 2,227,273 |
| 1,000,000 | 0.452 s | 2,214,381 |
| 2,000,000 | 1.208 s | 1,655,960 |
| 4,000,000 | 3.344 s | 1,196,162 |

Flat to **1,000,000 levels**, which is `MAX_PENDING_ROWS` exactly, and falling after it. That is
#137's slope, and a request/response client could not reach it, so until this change nothing
could. The before/after ratio above is unaffected: both sides ran the same volume at the same
interval.

**And the sweep is at `--flush-interval-ms 1000`, which is not what this engine ships.** The
default is 100 ms, and asking the same question there was worth doing before writing the sentence
this paragraph nearly carried. Four runs of each, alternating, four million levels:

| `--flush-interval-ms` | levels/s |
|---|---|
| 100 — the default | 2,175,674 / 2,122,961 / 2,028,399 / 2,225,517 |
| 1000 | 1,196,163 / 1,192,260 / 1,195,473 / 1,197,076 |

At the shipped default **the ceiling costs nothing measurable** — four million levels run at the
same rate as two hundred thousand. At one second it costs **1.8×**, and the 1000 ms column is so
tight across four runs because what it is measuring is a timer rather than work. So the honest
sustained figure depends on a setting: **~2.1M levels/s as shipped**, and 1.2M for an operator who
has lengthened the interval. That is a narrower claim than the one this paragraph started with,
and the measurement that narrowed it took four minutes.

**The diagnosis is the control on the other side.** `TCP_QUICKACK` re-armed before every `recv` in
the client — the server unchanged, not rebuilt, not restarted — took the same 250 round trips from
**12.963 s to 0.021 s**. So what the pipelining client was paying for was the server's *second*
response sitting in the server's kernel, waiting for an acknowledgement the client had no reason
to send until it had read the first.

Nagle holds a small write while an earlier byte on the connection is unacknowledged; the peer's
delayed-ACK timer releases it. Both are correct. Together they cost tens of milliseconds on any
exchange where one side writes twice before the other has a reason to answer, and every message
this engine sends is small. **A request/response client never meets it and never could** — with
one response outstanding there is nothing unacknowledged when the next write happens. That is why
this survived every benchmark this repository has published: all of them ask one question at a
time.

**What it cost the subscription path is a tail, not a median, and saying that precisely matters.**
Two hundred updates at each of three rates, one connection subscribing and never writing:

| updates | before: median / p99 / max | after: median / p99 / max |
|---|---|---|
| one per 50 ms | 0.003 / 0.006 / **48.126** ms | 0.003 / 0.006 / **0.009** ms |
| one per 5 ms | 0.003 / 0.007 / **40.884** ms | 0.004 / 0.005 / **0.006** ms |
| one per 1 ms | 0.004 / 0.007 / **43.330** ms | 0.004 / 0.007 / **0.015** ms |

The median did not move and the p99 did not move. `SubscriptionHub` accumulates a batch and the
drain writes it once, so most pushes go out with nothing outstanding. But the worst push in
**every** run was a full delayed-ACK timer — roughly one update in two hundred at these rates,
and which one depends on when the subscriber's acknowledgement happened to be due. For a feed
whose whole argument is latency, a tail at 40 ms is the number a client would quote back.

**The asymmetry.** The engine set `TCP_NODELAY` on every socket it *dialled* — both mesh
directions, the C++ client library — and on **no** socket it accepted. The replication link had it
on neither end, which is the same shape in the smaller: a live record and the `ACK` answering it
are two small writes with nothing else in flight.

**The fix is one definition** (`ob::set_tcp_nodelay`, `include/orderbook/socket_options.hpp`),
called from both client transports, both ends of the replication link, both mesh directions and
the client library. `src/metrics_server.cpp` is exempt and says so beside its own socket: it
writes the whole response in one `::send()` and closes, so there is never an earlier
unacknowledged byte to hold a second write behind, and setting the option there would be a line
that reads as caution and changes nothing.

Not done here, and the reason is a measurement rather than a preference: **coalescing the
responses of one read batch into a single write**. `send_response()` flushes per command, so a
batch of 64 leaves as 64 small segments. With Nagle off that costs syscalls and packets, not a
timer, and the after column above is already 1.65× the request/response client — so it is an
optimisation to measure on its own rather than a defect to fix under this number.

**Three checks, because none of them is sufficient alone.** A socket option cannot be read from
the other end of a connection, so no client can ask whether the server set it.
`tests/test_socket_options.cpp` reads it back with `getsockopt` against a control asserting it was
off beforehand, and — separately — derives the connection-holding sources from the tree (an
`accept` or `connect` syscall, matched with a leading non-identifier so `CoordinatorClient::
connect(` is not one of them) and requires each to set the option or carry a co-located
`OB_NO_TCP_NODELAY:` reason. It refuses a raw `setsockopt(..., TCP_NODELAY, ...)` anywhere in
`src/`, so a sixth site cannot open-code it. `tests/integration/test_wire_nodelay.py` observes the
consequence on a real server socket, which is the closest anything gets: measured against the
tree with the one line removed, **41.0 ms median, min 40.9, max 42.1** over 25 round trips on the
development machine, against sub-millisecond with it.

**What it exposes, now that the server is the bottleneck again.** A profile of the write path was
not worth taking before this: the server was idle almost all of the time, waiting for an
acknowledgement, so the samples would have landed in `epoll_wait`. Below the pending-row ceiling,
six rounds of 400,000 levels, 2K samples at 1999 Hz:

| | share |
|---|---|
| `ob::insert_level` | 15.1% |
| `malloc` + `_int_free` + `cfree` | 9.1% |
| `get_or_create_store` + the string-keyed hashtable `find` under it | 7.1% |
| `from_chars` + `parse_minsert` + `Session::feed` + `tokenize` + `memchr` | 11.9% |

The allocation share is the one with an obvious owner: `Session::feed()` returns
`std::vector<std::string>`, so a batch of 512 commands is 512 heap allocations and 512 frees that
live for the length of one loop. None of that is in this item — it is named here because this
change is what made it measurable.

**Mutations: nine, each with the verdict it is meant to produce, and two of them must survive.**

| mutation | wanted | got |
|---|---|---|
| the client port's accept forgets it | KILLED | static, and the integration test independently (41.0 ms median) |
| the io_uring accept forgets it | KILLED | static — nothing else can, since no CI job runs that loop |
| the replication accept forgets it, the dial keeps it | KILLED | static — **this is the one the per-file rule survived** |
| the replication dial forgets it, the accept keeps it | KILLED | static |
| the helper does nothing | KILLED | behavioural: `getsockopt` reads the option back |
| the metrics server drops its exemption reason | KILLED | static |
| a sixth site open-codes `setsockopt(..., TCP_NODELAY, ...)` | KILLED | static, two assertions |
| CONTROL: the exemption's reason is reworded | SURVIVES | survived |
| CONTROL: the log component string changes | SURVIVES | survived |

Two things about that run are worth more than the table. The open-coding mutation was first
planted in `src/metrics_server.cpp` and **did not build** — that file cannot name `TCP_NODELAY`
without a new include — so the *mutation* was reshaped and moved to a file that already has the
header, rather than the code being changed to accommodate it. And the harness scored both controls
as failures because it compared the word `SURVIVED` against the word `SURVIVES`: an instrument
disagreeing with itself about spelling, in the two rows whose whole job is to be the check on the
instrument.

- Effort: S | Impact: A pipelining client was capped at ~19 round trips per second whatever it
  batched, and the worst push to a subscriber waited out a 40 ms kernel timer. Neither is visible
  to a request/response client, which is every benchmark this engine has published

### 139. A select list was parsed, validated, and then ignored, so every row query was `SELECT *` ✅

Measured on a running server rather than read off the parser, because the parser's own behaviour
was the thing in question. One row inserted, then four queries:

| asked | answered, before |
|---|---|
| `SELECT * FROM 'AAA'.'EX'` | seven columns |
| `SELECT price FROM 'AAA'.'EX'` | **seven columns** |
| `SELECT price, quantity FROM 'AAA'.'EX'` | **seven columns** |
| `SELECT quantity, price FROM 'AAA'.'EX'` | **seven columns, in the other order** |

Every one answered `OK`. This is #107's class in the query language rather than in the command
parser: there the extra token was unread, here the select list was read, its names were checked
against the seven the lexer knows, aggregate calls in it were validated by name and argument — and
then the row path never looked at `ast.select_exprs` again. A client that asked for one column got
seven and had no way to tell, because the header it was handed named seven and was correct about
the bytes that followed it.

**Two lists, and keeping them apart is most of the change.** What has to be *answered* is ordered
and may repeat, because it answers the question literally: `SELECT price, price` is two columns.
What has to be *read* is a set, and wider — `SELECT price WHERE timestamp BETWEEN …` has to read
`ts.col` and must not answer it. Holding those in one type is the defect.
`include/orderbook/query_columns.hpp` holds both.

**One column had two names and they already disagreed.** The lexer took `timestamp`, the response
header said `timestamp_ns`, so a name copied out of a header was a syntax error. Both spellings
parse now, the header is generated from one table, and `SELECT *` is byte-identical by
construction — a test compares the generated header against the literal bytes clients already
parse, written out rather than referenced, because a generator compared against itself passes for
every spelling including a wrong one.

`ColumnarStore::scan` takes the set and opens only those files. Seven near-identical read blocks
became one helper — part of the change rather than tidying alongside it, because projection adds
a condition to each and seven conditions in seven copies is seven places to get it wrong
differently. The sequence number is the expensive column, Simple8b **and** zigzag-delta, so a
query without it skips two of a segment's four decode passes. A missing column file now refuses
the segment only for a query that needs that column; before, any one of the seven missing dropped
the segment from **every** query, including ones that would never have looked at it.

**A latent stack overflow came with it and is fixed here.** The row buffer was a 105-byte stack
array sized for seven distinct columns, which is the widest row **that list** can produce. A query
may name one column many times, and twenty repeats of `sequence_number` is 420 bytes - so the
first version of this change ran off the end of it for any list whose widths came to more than
105, which `SELECT *` cannot and a repeat easily can. The buffer is sized from the query's own
list, and the test uses the widest column with a value at its type's limit.

**Both clients read rows by position**, so a narrowed response would have come back as an empty
list from the Python one — it skips a row with fewer than six fields — and `bad timestamp_ns` from
the C++ one. A silent wrong answer in our own client is the same defect this removes from the
server. Both now refuse a header they cannot read and name the columns they were handed; reading
by name is the follow-up. A prefix of the canonical list is still accepted, because #65's seventh
column means this client still talks to a server that sends six.

### What it costs and what it buys

One m9g.xlarge (4 ARM64 cores, 16 GiB), Release, one probe binary against both servers - and the
probe parses nothing, which is also the only way to measure a server whose answer our C++ client
now refuses. The box is shared with another session's benchmark, and its load is not a constant:
**2.3 falling to 0.1** across the cycle run, **2.6 rising to 4.1** across the wall run, and 7.5
falling to 1.7 across a repetition an hour earlier, with its ClickHouse holding about a third of a
core whenever it was working. **Cycles counted against the server's own pid are therefore the
instrument**, because that is the number a neighbour cannot move; the wall clock is reported beside
them and each table says what the box was doing.

| 4,000 rounds of a 4,000-row query | before | after | ratio |
|---|---|---|---|
| `SELECT timestamp, price, quantity` | 3,045,211,432 cycles, **43.0 bytes/row** | 2,436,299,208 cycles, **33.0 bytes/row** | **0.800** |
| `SELECT *` - the control | 3,101,138,099 cycles | 3,030,782,086 cycles | 0.977 |

The same SQL to both servers, and they answer differently: before, 4,000 rows in **171,911
bytes**, because the list was ignored; after, 4,000 rows in **132,032 bytes**.

Two things about how to read that table. Each pair is adjacent in time, which matters because the
neighbour's load fell from 2.3 to 0.14 across the twenty minutes the four windows took - so the
control pair was measured busy and the claim pair quiet, and neither comparison crosses that. And
the same measurement on the previous head, an hour earlier under a load average of 7.5 falling to
1.7, gave **0.816** against this run's 0.800: two runs whose conditions differed that much
agreeing to within 2% is the argument for the instrument, and it is also the honest width of a
single cycle figure here.

Wall clock, eight rounds, **with the round order alternating** - odd rounds before-then-after,
even rounds after-then-before, because interleaving defends a ratio against slow drift and not
against a neighbour whose load has a period near the round:

| 8 rounds | before | after | ratio median | range | rounds faster |
|---|---|---|---|---|---|
| three columns | 0.305 ms | **0.242 ms** | **0.794** | 0.775-0.810 | 8 of 8 |
| `SELECT *` - the control | 0.305 ms | 0.303 ms | 0.993 | 0.981-1.000 | 7 of 8 |

The order made no difference: 0.795 before-first against 0.794 after-first on the three-column
question, which is what that experiment was for.

### And the profile says the saving is not mostly where it was expected

16,000 of the three-column query against each side, one m9g.xlarge at a load average under 1, no
call graph so the shares are flat and comparable. **The total agrees with the cycle table to four
digits** - 11.889 G against 9.506 G is 0.7995, where `perf stat` over 4,000 rounds gave 0.800 -
which is two instruments and one number.

Where it went, in **absolute** cycles rather than shares, because a share of a smaller total is
the trap #138 already paid for:

| symbol | before | after | saved |
|---|---|---|---|
| the row formatter | 4.247 G | 3.754 G | 0.49 G |
| `decode_prices` | 0.780 G | 0.380 G | **0.40 G** |
| `decode_simple8b` | 0.636 G | 0.402 G | **0.23 G** |
| `ColumnarStore::scan` | 0.633 G | 0.472 G | 0.16 G |
| everything below the 1% cut | 2.8 G | 1.6 G | 1.2 G |

**The two decoders together saved more than the formatter did**, which is the opposite of what the
work was aimed at: this item exists because the formatter dominated the profile, and the answer it
produced is mostly cheaper because the scan stopped decoding two columns nobody asked for -
`decode_prices` runs once instead of twice (price, and the sequence number's zigzag) and
`decode_simple8b` once instead of twice (quantity, and the sequence number's bit-packing), which is
the "skips two of a segment's four decode passes" sentence turning into a number. `memcpy` and the
row callback did not move by more than this profile can resolve; at 8-10K samples a 5% entry is a
few hundred samples, so read the four largest rows and not the small differences.

### The control failed twice before it held, and the two failures had different causes

The spec written before any of this said to write the general loop, measure `SELECT *` against
master, and only add a specialised path if it slowed - so the gate was there, and it fired.

*First attempt: a general loop for everything.* `SELECT *` came out 1.019-1.102 slower, 8 of 8
rounds in that direction. Small, but the wrong direction on the shape the published comparative
table measures. Profiling both sides over 16,000 of that query said where, rather than leaving it
to be argued: `format_query_response` went from **2.107 G cycles to 2.621 G, up 24%**, with every
other symbol flat. Seven straight-line calls inline; a loop that picks the field by a value
cannot, however cheap the switch is.

*Second attempt: unroll the row writer, keep one loop with a branch in it.* Barely moved -
0.991-1.085, still ~5% slow. `objdump` said the function had gone 919 to 1672 instructions with
both paths inlined, and the cause turned out not to be the row writer at all: the shared loop
wrote through a `char*` that might point at a stack array or at a heap one, so the compiler could
no longer treat the canonical buffer as a local nothing else aliases. **The row writer was the
suspect; the buffer was the cause**, and only reading the disassembly separated them.

*Third: two loops, and the narrow one out of line.* The profile named the real cost of the second
attempt - `std::__to_chars_i` had appeared as its own symbol at **6.71% of the server**, a symbol
absent from the build before projection. The function had outgrown GCC's inlining budget and
taken `to_chars` out of line with it. `[[gnu::noinline]]` on the narrow path brings it back:
**1027 instructions against master's 919, and zero out-of-line references to `to_chars`, the same
as master.** The canonical path gets back its own fixed local array and is the shape it was before
projection - a duplicate, added because three measurements said so.

Two paths need something holding them together, and it cannot be a call that picks between them,
because the choice is made from the column list. So the test formats each column **alone**, which
is necessarily the loop, and compares it with that field cut out of the unrolled output: a fast
path that wrote a field differently, or in the wrong order, fails on that column and names it.

### One guarantee said three times, and the copy that held was the one nothing could mutate

The mutation table found it rather than review. `ColumnarStore::scan()` widens the caller's set
with the timestamp because it filters on it and will not depend on the caller having remembered;
`columns_to_read()` puts it there too, because the engine knows it filters on time; and two lines
below the widening, `need(true, "ts.col", timestamps)` opened the file regardless of the set. So
the mutation deleting the widening **survived**, with
`ColumnarStoreProjection.TheTimestampIsReadEvenWhenTheSetLeavesItOut` - the test written for
exactly that case - still green. The file is opened through the set now, like every other one,
which is also what requirement 2.1 says: the scan opens **exclusively** the files the set names.

### What this does not do

`SUBSCRIBE` still pushes seven columns, and now says so once per subscription. Both obvious
alternatives are worse. Narrowing the push cannot be made safe, because **a `PUSH` line has no
header** — a `SELECT` response describes itself, so a client that cannot read a narrowed one can
say so, while a subscriber reading field 2 as the price has nothing to check against and would
simply read the wrong field; announcing the columns in `OK SUB <id>` is the fix and it is a
protocol change. And refusing breaks a form that works and is in use: `SUBSCRIBE price FROM …
WHERE price BETWEEN …` names the column it filters on. **Three tests failed** when the refusal
was tried, which is how it was found; counting afterwards, the form appears five times across four
test files.

The comparative harness still asks us `SELECT *` while asking ClickHouse and TimescaleDB for three
columns, then throws four of our seven away in Python. The adapter is ready for it - `_raw_rows()`
already finds its three columns **by name in the header**, which it does because #65 added a
column and the alternative was reading the wrong field - so that is one line of SQL. What makes it
the next commit rather than this one is the table: one run produces every number in it or none,
and `scripts/check_comparative_claim.py` has to agree with what that run wrote.

- Effort: M | Impact: the answer stops being a different question from the one asked; **a fifth
  off** the three-column query - 20% of the server's cycles, 21% of the wall clock - and **23%
  fewer bytes** on the wire

### 138. Formatting the answer cost eight times the read it came from ✅

Found by profiling the server while it answered the query the comparative table publishes, rather
than by reasoning about where a slow range scan spends its time. The arithmetic pointed here first —
an in-process scan is about 13 ns a row and the same rows over a socket were costing about 116 — but
arithmetic names an amount, not a function, so `perf` was asked. Over 16,000 of that query, both
sides profiled the same way:

| share of the server's profile | before | after |
|---|---|---|
| `format_query_response` | **22.96%** | 32.55% |
| `memcpy` | 24.27% | 6.18% |
| `basic_string::_M_construct` | 5.53% | *gone* |
| `malloc` / `_int_free` / `cfree` | 6.84% | 1.10% |
| `memset` | 1.82% | *gone* |
| `ColumnarStore::scan` | **2.70%** | 5.18% |
| **total cycles for the same 16,000 queries** | **20.56 G** | **11.90 G** |

The read was 2.70% and the answer was 22.96%, eight times as much. The old loop called
`std::to_string` seven times a row, once per column, so a 4,000-row response allocated 28,000
temporary strings and freed them again — and that is not a deduction, it is the four symbols beside
the formatter: constructing them, copying them, allocating them and freeing them. Three of those
four leave the profile entirely. `std::to_chars` writes into a caller's buffer and allocates
nothing: the loop now fills one 105-byte stack buffer per row — sized from every field at its
type's limit plus the six tabs and the newline — and appends it to a string reserved once.

The formatter's *share* goes **up** while the total goes down by 42%, and that is the expected
shape rather than a contradiction: work that used to be attributed to `_M_construct`, `malloc` and
`memcpy` is now done inside the function, on the stack, and there is much less of it.

**The first profile of this was under-sampled and I published its numbers before checking that.**
It ran 400 queries — about 0.2 s of server CPU, a few hundred samples at 1999 Hz — and said 40.11%
against 2.74%. It was right about which function dominates, which is what a pilot is for, and wrong
by nearly a factor of two about how much, because a 2% entry there is a handful of samples. The
numbers above come from 16,000 queries and about 16 s of CPU on each side. A profile is a sample,
so it has a sample size, and the entries small enough to matter for a ratio are the ones that
sample size ruins first.

`side` is `uint8_t`, which is a character type, so it is widened before `to_chars`. Without that its
digits would be written as a character, which is the one mistake in this change that produces a
well-formed response saying something else, so a test pins it by name.

Measured on machine C, Release, before and after interleaved on one idle box with a single probe
binary against both servers, so neither the client nor the build configuration is a variable:

| 4,000 rows, the published shape | before | after |
|---|---|---|
| server and wire, median of 8 rounds | 0.470 ms | 0.314 ms |
| through a C++ client, end to end | 0.614 ms | 0.450 ms |
| the formatter alone, `BM_FormatQueryResponse` | 258 µs | 95.5 µs |

**A 32% reduction on the server-and-wire path at the floor of the range** (0.6797), 33% at the
median, and 8 of 8 rounds in the same direction — well above this machine's own noise floor, which
ranged 0.89% to 5.37% over six runs. The control is the change itself: the response is meant to be
byte-identical, and both sides answered with 4,000 rows in 171,911 bytes.

Two instruments agree on it, which is worth more than either alone. Under `perf` — which slows both
sides — the same query went 0.505 ms to 0.338 ms, a ratio of 0.669 against the unprofiled run's
0.667. And the cycle counts say the same thing in a different unit: **42% less server CPU for the
same 16,000 queries**.

The two instruments nearly agree, and that is worth recording because usually they do not. The
formatter alone saves 162 µs a response; the path saved 156 µs. A 4% overstatement, where the
isolated CRC32C saving in #81 overstated its path by 8× — the difference is that formatting sits
alone on the response path with nothing overlapping it, so removing it removes the whole of it.

What this does **not** do: the comparative table is not regenerated here. One run produces every
number in it or none of them, and the table is pinned to the run it cites by
`scripts/check_comparative_claim.py`. The query row will move when that run happens, and it will
move for this reason. Nor does it touch the larger finding underneath: the harness asks us
`SELECT *` and asks ClickHouse and TimescaleDB for three columns, because at this commit the
engine could not express the narrower question — that is #139, and it is the bigger number.

- Effort: S | Impact: 32% off the published query path, measured on the path rather than on a
  micro-benchmark

### 137. A writer that hits the pending-row ceiling waits for a flush nothing asks for, and the writer is the epoll thread ✅

**Closed.** A writer that runs out of room asks for a flush before it sleeps, and its wait has a
deadline.

**What it was waiting for takes 73 ms.** Measured on an m9g.xlarge with the flush interval set to
an hour so that the only flush is the one being timed, at three sizes, under a load average of
2.95 from the neighbouring session — pessimistic, which is the right direction for choosing a
deadline:

| pending rows | write | **flush** |
|---|---|---|
| 250,000 | 0.086 s | **0.017 s** |
| 500,000 | 0.179 s | **0.034 s** |
| 1,000,000 — the full ceiling | 0.393 s | **0.073 s** |

Linear, 0.068 µs a row. So the writer was waiting out `--flush-interval-ms` for work that takes
73 ms: thirteenfold at one second, and at the hour an operator can set it, a node that cannot be
told apart from a hung one.

**Measured, and the prediction was written before the work.** One m9g.xlarge, 4,000,000 levels
through a pipelining client, alternating, with a load guard refusing to start while the
neighbouring session was busy:

| `--flush-interval-ms` | before | after |
|---|---|---|
| 1000 | 1,196,745 / 1,195,609 | **2,209,501 / 2,203,385** |
| 100 — the default | 2,217,558 / 2,212,487 | 2,225,466 / 2,187,635 |

**1.85× at one second, and the slope is gone rather than softened**: 2.21M is the rate the same
client gets *below* the ceiling, so four million levels now cost what two hundred thousand do.
The prediction recorded before the change was 1.9–2.2M and the measurement is 2.20–2.21M. The
100 ms row is the control and it does not move, which is the answer to "does this cost anything
at the setting the engine ships with": no.

**What the write path pays, counted rather than argued** (`scripts/mnemonic_diff.py`, Release,
aarch64, against the same two trees the table above came from):

| | before | after |
|---|---|---|
| `Engine::apply_delta_impl` | 588 | **571** |
| `Engine::apply_delta_mm` | 657 | **641** |
| `Engine::apply_delta` | 2 | 2 |

**Both callers got shorter, and that is not a saving** — it is pitfall 251's tell. The inline
condition-variable wait they used to carry moved into `await_pending_room`, which is **162
instructions, out of line, and called**. So the steady state trades seventeen inline instructions
for a call, a return and the episode's load-store-branch; the other 145 are the waiting path,
which a write that finds room never enters. The obvious next step if it ever matters is the one
#117 took with the WAL counter: put the fast-path check inline in the header and leave only the
wait out of line. Not done here, because a change that moves the same work behind a call is not
the place to claim a cycle either way.

**Two halves, and the control shows both are load-bearing.** Asking removes the dependency on the
interval; it does nothing when the flush itself cannot make progress, which is a full disk or
#113's `EIO`. So the wait has a five-second deadline and the write is **refused** after it rather
than accepted — sixty times the 73 ms, so a healthy flush never reaches it even an order of
magnitude slower. A deadline a healthy write can touch is a gate on a clock, and those teach
operators to ignore refusals. Measured with the ask removed and the deadline kept: the same test
takes the refusal path in 5 s and fails with a sentence, where before this item it would have
waited an hour and reported a stuck runner.

**Where the numbers to compare came from, and the run that did not count.** The first before/after
went on two trees that both predate #140 and produced eight rows at **20.1–20.5 s and ~198,000
levels/s** — the same at 100 ms and at 1000 ms, and the same on both trees. My first reading was
that the shared box had been busy. The arithmetic says otherwise: 200,000 updates at batch 512 is
391 round trips, and 391 × the 52.15 ms delayed-ACK timer #140 measured is **20.4 s** against
20.13 measured. The ceiling under test was never reached, because the wire capped the run an order
of magnitude below it. **A quantity that will not move when you move its input is not noise, it is
a different limit**, and a fix measured against a baseline missing an earlier fix measures the
earlier one.

- Effort: M | Impact: P0. The waiting thread is the epoll loop, so while it waits the node accepts
  nothing, answers nothing, logs nothing and — measured — does not observe `SIGTERM` for 273 s

**How it was found and what it cost before, kept below.**

Found on the aarch64 benchmark box while measuring the wire, by a probe that ran a node with
`--flush-interval-ms 3600000` so that no flush would perturb the timing. The node accepted 50,000
twenty-level updates and then stopped: no reply, **no line in its log**, and a new connection could
not read the banner. The process was alive.

**The arithmetic names it exactly.** `MAX_PENDING_ROWS` is `1'000'000`
(`include/orderbook/engine.hpp:498`) and 50,000 updates of twenty levels is 1,000,000 rows.
`apply_delta_impl()` blocks there:

```cpp
// Backpressure: wait until pending queue has room.
// This blocks the writer if the flush thread can't keep up.
pending_cv_.wait(lock, [this]() {
    return pending_rows_.size() < MAX_PENDING_ROWS || stop_flush_.load(...);
});
```

The only thing that empties `pending_rows_` is the flush, and `pending_cv_` is notified in exactly
two places: at the end of a flush, and in shutdown. **Nothing asks the flush loop to run because a
writer is waiting.** So the writer waits for the timer, and the timer is an operator's flag.

**Confirmed with a backtrace rather than inferred.** Both threads in `futex_do_wait`, 0.5 s of CPU
between them:

```
Thread 1  ob::Engine::apply_delta_impl  <- std::condition_variable::wait
          ob::execute_command <- ob::TcpServer::run <- main
Thread 2  ob::Engine::flush_loop        <- pthread_cond_clockwait
```

Thread 1 is the **epoll loop**. That is what turns a stalled writer into a stopped node: while it
sits in `apply_delta`, nothing accepts a connection, nothing answers `PING`, nothing writes a log
line, and — measured — **`SIGTERM` is not observed either**. The node logged `Shutdown requested`
(that line comes from the signal handler) and was still in the same two futexes **273 seconds
later**, at which point it had to be killed with `SIGKILL`. A supervisor does exactly that, and
whatever was pending is lost.

**It is a slope, not a cliff, and the slope is what makes the far end look like a hang.** Measured
at 4,000,000 levels through the wire, one flush interval per row, everything else held:

| `--flush-interval-ms` | wall | levels/s | against 1000 ms |
|---|---|---|---|
| 1000 | 3.70 s | **1,081,417** | — |
| 5000 | 15.71 s | 254,691 | 4.2× slower |
| 20000 | 60.68 s | 65,930 | 16.4× slower |
| 60000 | **did not finish in 120 s** | — | the extrapolated rate puts it near 180 s |

Throughput falls roughly in inverse proportion to the interval, which is exactly what "the writer
waits for the next flush" predicts, and none of those rows logged an error: backpressure is doing
what it says. At 3,600,000 ms the same slope reaches a node that cannot be distinguished from a
hung one, and that is where this was found.

**So the earlier version of this paragraph was wrong and the correction is the point.** It said
that at the 1000 ms the comparative harness sets, one interval is one ceiling — reasoned from
`MAX_PENDING_ROWS` and the ingest rate, and contradicted by a measurement taken the same afternoon:
the volume series pushed 20,000,000 levels at that interval with no stall. The shipped default of
100 ms is further still. What is defective is not the ceiling, it is that the wait is on a timer
nobody can shorten, that the thread doing the waiting is the one serving every client, and that
none of it is logged.

**Three candidate answers.**

1. **Let the writer ask.** A flush-now signal beside `flush_stop_cv_`, which already exists so that
   `join()` does not wait out the interval — the writer's wait becomes bounded by how long a flush
   takes rather than by the interval. Smallest change, and it keeps backpressure meaning what it
   says.
2. **Refuse instead of waiting.** Bound the wait and answer `ERR`, which is the answer this engine
   takes elsewhere: #113 refuses a write it cannot sync rather than acknowledging it. It turns a
   silent stop into a named refusal and does not make the writes land.
3. **Refuse the configuration.** Unavailable: whether an interval is long enough to reach the
   ceiling depends on the write rate, which is not knowable at startup.

Whichever is chosen, the log has nothing to say today and should: a writer that has been blocked on
backpressure for longer than an interval is the one line an operator needs, and this node emitted
none.

**Reproduction.** Start a node with `--flush-interval-ms 3600000`, send more than 1,000,000 rows —
50,000 `MINSERT`s of twenty levels will do — and then try `PING` from a second connection, and
`SIGTERM`. Note what the probe got wrong, because it matters for anyone repeating it: a crude
`/dev/tcp` banner check reported "stopped answering" two seconds in, while the node was still
starting and holding 7.5 MB. The evidence here is the backtrace and the 273 seconds, not that line.

- Effort: M | Impact: **P0 by consequence.** A node stops serving every client, logs nothing, and
  cannot be shut down gracefully, from a documented flag set to a value the project's own tuning
  note recommends for bulk loads. The margin at that recommended value is a factor of one on this
  hardware
### 136. Writing the same event-time span twice destroys a symbol's segment, and the only diagnosis names a race that did not happen ✅

**Closed.** A directory belongs to one segment rather than to one event-time span.

**The same two cases, measured over the wire against the fix**, with the node's own log counted
rather than the client's replies — the client always said `OK`:

| the second write | before | after |
|---|---|---|
| same span, different prices | 4000 of an expected 8000, 200 `ERROR` lines | **8000**, zero |
| same span, 5 levels instead of 20 | **0**, 200 `ERROR` lines | **5000**, zero |

5000 is 4000 + 1000 and not 8000, which is the point: the two writes are different sizes and both
are now stored, where before the shorter one made the whole symbol unreadable.

**The fix is one function with one caller, and the first segment of a span keeps its name
character for character.** `create_unique_segment_dir()` asks for `<start>_<end>` and, only if
that is taken, `_1`, `_2`, and so on — so the format differs exactly where the engine used to lose
data, and existing directories keep working because `open_existing()` reads the timestamps from
`meta.json` rather than from the name. No migration, no manifest version.

**`create_directory()` is the arbiter rather than an `exists()` before it.** It reports whether it
created the directory or found one, so two flushers racing for the same free name cannot both win
it — and that holds without a claim about which lock the caller holds, which is worth more here
than the claim would be: the guard this narrows asserted a locking fact about its callers and was
wrong about it from the day #105 shipped.

**The item's own costing of its candidates was wrong, and that is why the correct answer looked
expensive.** It said candidate 1 was "the widest change: the snapshot manifest addresses segments
by directory, retention orders them by name, and a replica bootstrapping from a snapshot indexes
what it is sent." Nothing parses a segment directory name: `segment_dir()` had exactly one caller
and every other use of `dir_path` is opaque. Retention does **not** order by name — it sorts with
`segment_order_less`, which is `start_ts_ns`, then `end_ts_ns`, with `dir_path` only as a
tie-break. The manifest carries opaque relative paths, and since #142 one function installs them.

**And a consequence nobody had written down.** `segment_order_less`'s own comment says *"dir_path
is unique per segment, so this is a total order."* Under this defect it was not unique — that is
the defect — so the comparator was not a total order in exactly the state the defect produces, and
the same comment records a TTL property test that used to fail one run in three on a non-total
order. This does not merely stop the data loss; it **restores a premise the code already
asserted**.

**What was rejected.** Candidate 2 (replace the index entry) makes the second write a silent
winner, which is the semantic #26's guard exists to prevent. Candidate 3 (refuse at write time) is
honest and leaves the backfill re-run broken; unique identity gets that honesty for free, because
the state it would refuse no longer arises. And the **WAL position** was the obvious
disambiguator until the docstring of the field ruled it out: a received segment carries the
*sender's* position, "meaningless here, and dangerous if believed" — a foreign number in a local
path reads as a claim about this node. An in-memory counter does not survive a restart; the
ordinal's state is the directory listing, which does.

**Mutations: four, each with the verdict it was meant to give.**

| mutation | verdict |
|---|---|
| identity is the span again | **killed** — the defect, and both integration cases with it |
| `exists()` before the create instead of letting it arbitrate | **killed** by the four-thread race test |
| the ordinal starts at 0 | **killed** — the second segment is named `_1`, and that is pinned |
| the log line is reworded | **survives**, and it is the control |

The second row is the one worth reading: it says the arbiter is load-bearing rather than a
stylistic preference, because a check-then-create hands two flushers the same free name. Baseline
green before and after, source restored byte for byte.

The guard's message no longer names a cause it cannot know.

**How it was found and what it cost, kept below.**

Found on the aarch64 benchmark box, by a wire probe that replayed the same twenty-level updates
against one server three times and produced **200 `ERROR` lines** — one per symbol per replay — in
a run whose client saw nothing but `OK`.

A segment's identity **is its time range**: the directory is
`<data-dir>/<symbol>/<exchange>/<start_ts>_<end_ts>`. Two flushes covering the same span therefore
write to the same directory, and `ColumnarStore::merge_segments()`
(`src/columnar_store.cpp:603`) refuses to index a `dir_path` it already holds. Its comment states
its premise in one line — *"Two flush paths raced"*, the defect **#26** fixed, *"Defence in depth,
not the fix"* — and that premise was **true when it was written**. Before **#105** the wire dropped
the client's `timestamp_ns` and the server stamped arrival time, so no two client writes could ever
produce the same span. #105 put event time on the wire so that backfills are expressible, and in
doing so made a second way to reach this state: **one client, sequentially, writing the same span
twice**, which is what re-running a backfill is.

**Measured, four cases, one symbol, 200 updates of 20 levels, `--flush-interval-ms 1000`.**
Every write was acknowledged and every `FLUSH` returned `OK`.

| the second write | rows readable after it | what happened |
|---|---|---|
| same span, same shape, **different prices** | 4000 of an expected 8000 | the first write's values are **gone**: the rows read back carry the second write's prices under the first write's index entry |
| same span, **5 levels instead of 20** | **0** | the index keeps `row_count=4000`, the directory now holds 1000 rows, and the reader refuses the whole segment: `Skipping segment …: short column(s) for row_count=4000` |
| span shifted by half (overlapping) | 8000 | **both readable** — a control |
| a disjoint later span | 8000 | **both readable** — a control |

The two controls are what make this a claim about **identical spans** rather than about overlap in
general, and they bound who is exposed: a backfill re-run with the same boundaries, not any
re-delivery.

**The severity is in the third column, not the second.** The first row is silent replacement of
acknowledged data. The second is worse and is the one to read: two acknowledged writes, and the
symbol then returns **nothing at all** — the index's count and the bytes on disk disagree, and the
second guard, which is correct, drops the segment rather than serve a short one. There is no error
to the client at any point in either case.

**Why no test catches it.** Nothing in the suite writes the same span twice; before #105 nothing
could. And the guard reports the state with a cause attached, so a reader who hits it goes looking
for a flush race. This repository's own rule from the flagship product applies here: **a wrong
diagnosis is worse than none.**

**Three candidate answers, and the cheapest is not obviously right.**

1. **Make segment identity unique** — an ordinal or a monotonic counter in the directory name, so
   identity stops being derived from content. Correct in principle and the widest change: the
   snapshot manifest addresses segments by directory, retention orders them by name, and a replica
   bootstrapping from a snapshot indexes what it is sent.
2. **Replace the index entry when the directory is rewritten** — smallest diff, and it makes the
   second write win silently, which is a semantic nobody asked for and which #26's guard exists to
   prevent.
3. **Refuse at write time, to the client** — the honest minimum: a flush that would produce a span
   already indexed fails the `FLUSH`, so the operator learns it from the call rather than from a
   log line naming something else. It does not make the backfill work; it stops the loss.

Whichever is chosen, the guard's message must stop asserting a cause it cannot know.

**Reproduction**, in full, because a pointer at a file nobody else has is not one. Start a node
with `--flush-interval-ms 1000` on an empty data directory, then from the Python client:

```python
prices = [5_000_000 - i * 100 for i in range(20)]
sizes  = [1_000 + i for i in range(20)]
for u in range(200):                       # one span: BASE .. BASE + 199_000
    engine.insert("SYMA", "EX", "bid", prices, sizes, timestamp_ns=BASE + u * 1000)
engine.flush(); time.sleep(2.5)
len(engine.query("SELECT * FROM 'SYMA'.'EX'"))      # 4000

prices = [9_000_000 - i * 100 for i in range(5)]    # same span, five levels
sizes  = [1_000 + i for i in range(5)]
for u in range(200):
    engine.insert("SYMA", "EX", "bid", prices, sizes, timestamp_ns=BASE + u * 1000)
engine.flush(); time.sleep(2.5)
len(engine.query("SELECT * FROM 'SYMA'.'EX'"))      # 0
```

Keep the second `range(20)` instead of `range(5)` and the count stays 4000 with the second write's
prices, which is the first row of the table. Shift the second loop's `BASE` by `100_000` and both
writes are readable, which is the control.

- Effort: M | Impact: **P0 by consequence.** Two acknowledged writes over one event-time span leave
  a symbol returning nothing, with no error to the client and a server line naming a different
  cause. Reachable from the public API by re-running a backfill, which is the operation #105 added
  event time on the wire to make possible

### 135. The mesh registry writes to `endpoints[0]` while its coordinator client talks to whichever endpoint answered ✅

Found while writing #132's `read_self_key()`, which became the **second** of three places to
hardcode the same index — and asking why there was an index at all.

`--coordinator-endpoints` takes a **comma-separated list**; `--help` says so, the parser splits it,
and `CliArgs.SplitsCoordinatorEndpointsAndDropsEmptyOnes` pins three endpoints plus the empties it
drops. `CoordinatorClient::connect()` then probes them **in order** and keeps the first that answers
`/v3/maintenance/status` as `active_endpoint`, which every lease call goes through.

`PeerRegistry` does not. All three of its own etcd calls address `config_.endpoints[0]` directly:

| line | call | what it is for |
|---|---|---|
| `src/peer_registry.cpp:393` | `PUT /v3/kv/put` | publishing this node's address (`register_self`) |
| `src/peer_registry.cpp:532` | `POST /v3/kv/range` | reading its own key back (#132) |
| `src/peer_registry.cpp:647` | `POST /v3/kv/range` | the **topology watch** — how every peer is learned |

**Measured, and the control is the same run with the order reversed.** A node given
`--coordinator-endpoints http://127.0.0.1:1,<live etcd>`:

| | dead first | live first (control) |
|---|---|---|
| registered within 25 s | **no** | **yes, in 0.5 s** |
| `Registered node` lines | 0 | 1 |
| `Failed to PUT PeerInfo` | **1** | 0 |
| published address | none | `127.0.0.1:48255` |

Reversing the order is what makes this a claim about **order** rather than about a dead endpoint
merely being configured. And three counts say exactly how far the node got: **zero** `Failed to
connect`, **zero** `Failed to grant`, so `connect()` and `grant_lease()` both succeeded through the
live endpoint — only the PUT to `endpoints[0]` failed. `Started watch` appears once, so the node
goes on serving, and its topology watch queries the dead endpoint too, so it never learns a peer
either. The mesh does not form.

**#132's recovery does not help here, and the measurement says why: zero `Lease refresh failed`
lines.** The lease is alive, because it was granted through the endpoint that answers — so the
failure path that re-registers is never entered. An unregistered node in this configuration stays
unregistered for the life of the process, which is the state #132 removed everywhere else.

Two halves of one subsystem disagreeing about which server they are talking to, and the half that
works is the one an operator would check first: leader election, lease keepalive and failover are
all fine, because those go through the client that failed over correctly.

**The reason the index exists is that there is nothing else to ask.** `active_endpoint` is private
to `CoordinatorClient::Impl` and the header exposes no accessor, so a class holding a client cannot
find out which endpoint answered. That is also why the fix is not a one-line change of subscript:
either the client grows a way to say where it is connected, or these three calls go **through** it
the way the lease calls already do. The second is better and is the same argument
`redirect_peer()`'s docstring makes about formats — a second copy of "how we talk to etcd" is how
the two of them come to disagree, and this item is what that looks like after it has happened.

**Why nobody has hit it**: every test and every document in this repository runs a single endpoint.
`ClusterManager` passes one URL, `test_cli_args.cpp` proves the *parser* splits a list and then
nothing starts a node with two. A configuration that is documented, parsed, and pinned by a test,
but never once exercised end to end — which is the same gap `sanitizers-integration (tsan)` was in
before #85 widened it.

Related but not the same, and worth recording next to it: `impl_->connected` is set false **only by
`disconnect()`**, so once a client has chosen an endpoint it never re-probes the others. A node
whose `active_endpoint` dies keeps trying that one for the life of the process even with two healthy
endpoints configured. Same root — the client's endpoint choice is made once and is invisible from
outside. That half is **read from the code, not measured**, and is stated that way.

**Fixed by making the client answer the question instead of guessing it.**
`CoordinatorClient::endpoint()` returns the endpoint that answered, and all three call sites ask it.
Empty means **not connected** and is treated as a refusal rather than as a reason to fall back to an
index: a fallback there would restore exactly this defect, silently. The third site — the topology
watch — needed one more thing, because it runs whether or not `register_self()` succeeded: with no
endpoint it now says so **once** and keeps polling, rather than either going quiet or writing a line
every 100 ms (#133's shape, in the loop most able to produce it).

**The test was the part that needed a decision, and it was deferred to here on purpose.** The
harness gave every node exactly one endpoint, and `--coordinator-endpoints` **appends** rather than
replaces, so `extra_node_args` cannot put the dead one first — the harness's own flag is already
ahead of it. The two ways out were a knob whose only user is one test (the shape
`narrow_proxied_mesh` was deleted for) and a test that starts its own node (the shape pitfall 77 is
about). Taken: two attributes feeding `_node_argv`, which is the **only** place a node's command
line is built — pitfall 77 is about *duplicating* that list, so a parameter to the single builder is
its opposite, and `restart_node()` inherits the order for free because it goes through the same
function.

`tests/integration/test_coordinator_endpoint_order.py` is two tests, and the second is a **control
rather than a second case**: the same unreachable endpoint in the harmless position, which passed
before this fix and has to keep passing. Without it, a harness that quietly stopped prepending
anything would leave the first test green and meaningless. Both assert their own premise from
`Popen.args` — the command line the node actually got — rather than from the attribute that put it
there, because an attribute is what the harness *meant* to say. And the first asserts the mesh forms
on top of both registrations, because that is the only assertion that reaches the third call site:
two registered nodes that never see each other is a topology watch still reading the wrong endpoint.

Measured: **2 passed in 4.6-5.1 s**. Against the **four** source files from the commit before the
fix — the two headers and the two units, reverted together so they stay consistent, with the harness
and both tests unchanged — the run is **1 failed, 1 passed in 50.1 s**: the regression test names
both nodes as never registered, the control passes, and the extra 45 s is its own patience window.
Restored from a copy kept alongside and **rebuilt**, because a restore without a rebuild leaves the
next consumer of that build directory running the previous tree.

- Effort: M | Impact: was measured — a documented multi-endpoint HA configuration in which the mesh
  **never** formed if the *first* endpoint was the one that was down, while every other etcd-backed
  mechanism in the node behaved correctly and #132's recovery could not see the condition

### 134. Two registry methods claim to write to etcd, write nothing, and return success ✅

Found while giving #132 its `registered_status_`, by asking the narrow question "what else ever
writes this node's status?" and getting the answer **nothing**.

```cpp
bool PeerRegistry::update_status(const std::string& new_status) {
    OB_LOG_INFO("peer_registry", "Updating status for node %u to '%s'", …);
    // In a full implementation this would PUT the updated PeerInfo to etcd.
    return true;
}
```

`update_position(hlc, wal_file, wal_offset)` had the same shape one function below, `return true`
and no etcd, and — worse — **no comment saying so**. Neither had a caller anywhere in the tree,
neither had a test, and neither was mentioned in any document (checked across every file, not three
extensions).

**The header is where this did its damage**, because that is what a caller reads: *"Update this
node's status in etcd"* and *"Update this node's HLC and WAL position in etcd"*, sitting directly
under `register_self()`, which says "with lease" and means it. Three promises, one kept.

**Why it was worse than an absent method, and this is the part worth keeping.** `update_status()`
**logged at INFO that it did the thing**. The first caller would therefore have got a line
confirming a change that did not happen — and this repository has already paid for that exact shape
once, in #30's series B, where `cluster authentication enabled` was printed by a path that enforced
nothing: an operator greps for precisely that line to confirm precisely that guarantee. A method
that returned `false`, or did not exist, is found on the first attempt to use it.

**And `update_position()` was the one somebody would have reached for.** #72 wants peer positions
published under a lease so election deference can tell a lagging replica from a dead one, and #118
measured what the registry actually holds: `wal_file_index` and `wal_byte_offset` are written
**once**, by `register_self()`, so every peer's view of every other peer's position is frozen at
handshake. A future implementer asking "is there a place to publish positions?" would have found a
method whose name, whose signature and whose documentation all said yes, and which returned `true`
without doing it.

This was the **seventh** instance in this workspace of a thing whose value never reaches anybody —
after `provisional`, `basis`, `in_use`, `key_id`, `partition_by` and #104's
`adopted_primary_address_` — and the second with no behavioural symptom at all, because there was no
caller to have one.

**Both are deleted, which is the answer #104 took for its field**: the day someone needs one is the
day it is written, and writing `update_position()` is #72's work rather than this item's. The
header's claim about etcd went with them, and so did the sentence in `registered_status_`'s comment
that cited `update_status()` as the reason one string is enough — the reason is now simply that
`register_self()` is the only writer there has ever been.

**No mutation table, and the reason is the interesting half: this change is a deletion, and the
mechanism that would catch a re-introduction is worse than the gap.** `tests/test_field_usage.cpp`
from #104 catches the *field* shape by comparing every member against every occurrence; it cannot
see this one, because these are functions and the unread thing was their **effect**. A checker for
"a method with no caller" cannot be turned on here without a hand-written list of what counts as
internal rather than public API — `include/orderbook/` also holds the client API and the C API,
where a callerless public method is entirely legitimate. A list written by hand is not evidence
about the code, which this repository has now paid for three times (#112's loops, #32's valueless
flags, #117's metrics), so the honest state is: **this class of defect is caught by reading here,
and that is recorded rather than papered over.**

**Two more things came out of the deletion, and the second is a fourth instance of the same shape
as the three #132 corrected.** First, `PeerInfo::status` can now hold exactly one value: `active`.
`register_self()` is its only writer and its only call site passes that string, so two client
documents naming `joining` and `leaving` beside it were describing a vocabulary nothing has ever
written and nothing can now — both say so. The field itself is **left alone deliberately**:
`PeerInfo::from_json` requires the key, so dropping it is a change to a document two nodes exchange,
for a constant that costs nothing. Written down so the next reader does not "finish the job"
unsafely.

Second, a comment beside `lease_refusals_` still argued that both loud-once conditions are usually
permanent "and `register_self()` runs once (#132)" — a sentence #132's own fix had made false, in the
file that fix edited. The reason changed rather than going away, which is why the comment is worth
having: of the three answers `read_self_key()` gives, two leave the refusal permanent and only
`Absent` ends it. **Four stale justifications from one change, every one of them naming a fact that
became false while its conclusion stayed right** — which is the whole difficulty, because nothing
they claim will ever fail.

- Effort: S | Impact: was latent — nothing called either, so it would have cost the first caller
  rather than the cluster, and it would have cost them a log line saying the write happened

### 133. A lease that etcd has forgotten is reported once per refresh interval, for ever ✅

Found while giving `lease_loop` its per-iteration boundary (#112), by revoking the lease that holds
a node's mesh registration open and reading what the node then says about it.

**Measured on a two-node mesh, before**: eleven `Lease refresh failed` and eleven
`keepalive returned no TTL` in 33 s — one of each per refresh interval, at the default TTL/3, for
the life of the process. The condition is permanent: a lease etcd has forgotten fails every
keepalive, and nothing re-registers (#132). **After**: one of each. Run against the commit before
the fix, the test that pins it counts **4 lines in its 14-second window where the fix gives 1**.

This is #95's shape — a permanent condition retried at loop frequency and logged at loop frequency
— and the third time this repository has answered it with `LogEpisode`, after #116's WAL-position
publisher (2.17 lines/s → 0.40) and #120's hybrid logical clock (one line per *write*).

**Two halves, because the two lines have different owners, and the second is the interesting one.**

`PeerRegistry::refresh_lease()` gets this object's own episode. The line that opens it now carries
the consequence — *"this node's mesh registration expires with the lease and nothing re-registers
it"* — which is what makes one line enough where a hundred were not information, and it names which
of the two failures it is, because they ask for different things: no lease at all means
`register_self()` failed or never ran; a refused keepalive means the lease existed and is gone.

`CoordinatorClient::refresh_lease()` gets one too, and it covers **all four owners** of that class
rather than this one caller — `FailoverManager`, `PeerRegistry`, `ShardRouter` and
`ShardCoordinator`. Under its own mutex, because `LogEpisode` is not thread-safe and says so in its
own header: it asks its users to hold a lock over the decision that consults it, and this class is
used from more than one thread by design, which is exactly what #71 was about. Deliberately **not**
`http_mtx`, which is held across a network round trip while this decision is taken after it is
released. One episode per client rather than per lease id: every owner in this tree keeps exactly
one lease, and the opening line names the id, so two leases sharing a client would be visible
rather than silently merged.

**The alternative that was considered and rejected**: give `refresh_lease()` three answers instead
of two, the way #82 gave `read_leader()` `Present` / `Absent` / `Unavailable` one function away in
the same class, and let each caller decide what to say. That is the better design and it is not this
change: it needs a decision per caller, and one of the four — `ShardCoordinator::watch_loop()` —
**discards the result entirely today**, so the line this change keeps is the only thing it emits
about a lost lease. Recorded here rather than quietly done differently.

**The control is inside the test, and it has to be.** "One line" is satisfied by a condition that
only happened once, so the registration's absence is asserted on each of fourteen seconds — four
refresh intervals and more — before the lines are counted.

- Effort: S | Impact: the log an operator greps during an etcd incident is the one that buries the
  finding under its own repetition, and the two lines came from two different components so neither
  looked like a flood on its own

### 132. A node whose registration lease is lost never comes back to the mesh registry ✅

Found by the same probe as #133, and it is the more serious half.

`PeerRegistry::register_self()` ran **once**, at start (`src/multi_master.cpp:351`), and was the
only writer of `lease_id_`. `lease_loop()` refreshes that lease every TTL/3 and **discarded the
answer** — which since #74 is an answer worth having, because a keepalive for a lease etcd has
forgotten now fails rather than silently succeeding.

**Measured before**: revoke the lease under a running two-node mesh and `<prefix>mm_peers/2` is
gone at once and **still gone 22.5 s later**, seven refresh intervals, while the node answers
`PONG` on every sample. `Registered node` appears exactly once in its log, at start. Recovery is a
restart.

**What survives, measured rather than assumed, and it narrows the defect usefully.** The mesh does
not fall apart: the existing TCP link was dialled before the key went away and keeps carrying
writes, and a node that joins *after* the revoke still ends up connected to the unregistered one —
because that node's own topology watch sees the newcomer's registration and dials **out**. A third
node started after the revoke received both records. So this was not a partition; it was a node
permanently absent from the one place the cluster's addresses are published, and whose row in every
peer's `MM_PEERS` carried an **empty address** for the rest of its life.

**Three answers were on the table, and the cheap one was wrong.** Re-registering on every refused
refresh has a cost this repository can name: `register_self()` running more than once would
overwrite the entry `ClusterManager.redirect_peer()` writes, and **ten integration tests** in #54's
stage C depend on that entry staying where the harness put it — a fixture whose insertion point is
"this registration happens exactly once", as its own docstring says. The second was to report the
condition and let an operator act. The third, taken here, is to write the entry again **only when
the key is confirmed absent**, which keeps the one-shot property for a key that exists.

**The read has three answers, not two, for the reason #82 gave `read_leader()` the same shape one
class away**: a read that failed and a key that is gone ask for opposite things, and a `bool` makes
them the same answer. `PeerRegistry::read_self_key()` returns `Absent` only when etcd's range
response carries no `kvs` member at all — an empty body is a transport that said nothing, which is
`Unavailable`. Gating on `Absent` rather than on the refusal buys three separate things:

- the entry the harness overwrites to redirect a peer is **not** overwritten back, so stage C's
  ten tests keep their premise (checked: `test_mesh_link_faults.py` + `test_mesh_proxy.py`,
  **12 passed in 217 s** with the fix in place);
- two nodes sharing a `node_id` cannot start a war over the key, because whichever wrote it last
  leaves it `Present` for both;
- while etcd is unreachable the answer is `Unavailable`, so nothing is attempted and **nothing is
  written at INFO** — the quiet branch is what keeps this recovery from becoming #133 in a new
  place. The loud WARN one branch up already carries the operator-facing sentence, which now says
  the entry will be written again once the key is confirmed gone instead of "nothing re-registers
  it".

**What it deliberately does not cover, and this is a decision rather than a gap**: a key an
operator **deleted** while the lease is still alive. That node is invisible too, but its refresh
still succeeds, so this branch never runs — and eviction by deleting the key is a thing someone may
be relying on.

`etcd_post()` came out of this because `read_self_key()` needed a third copy of the same twenty
lines of curl setup, and the two it replaced differed only in their timeout; a third copy is how the
three of them would have come to disagree about anything else. `registered_status_` is remembered so
the second registration says what the first one said rather than a hardcoded default —
`update_status()` writes nothing to etcd, so that string is the only status the registry has ever
held for this node.

**Measured after**, same harness: the key is absent at 0 s and 2.5 s and **present from 5.0 s
onward**, `Registered node` appears twice, the recovery line once, one WARN of each kind, and the
peer's `MM_PEERS` row carries a real address again instead of an empty one.

**The module has three tests now, and the third is the one that says the gate is real.** The two
that existed are green against the *cheap* version of this fix — rewrite on every refused refresh —
because with etcd reachable the key really is gone, and with etcd stopped the grant fails so no
line is written either. The state that tells the two apart is a refusal over a key that **exists**,
and nothing else in this battery produces it: `redirect_peer()` writes without a lease, which
detaches the key, so the test captures the lease id *before* redirecting and revokes it by id
afterwards. From then on every refresh fails while the entry sits where the harness put it, and the
address has to stay there. Its control is the **premise** rather than the outcome — without
asserting that a refusal reached the log, the test passes against a node whose refresh is
succeeding, in which case the branch under test never ran at all.

Both other tests gained a rate bound that is **independent of wording**: the count of registry
lines above `DEBUG` across the window, measured at one and two. That is what catches the flood a
*different* wrong answer would cause — reading the unreachable case as `Absent` attempts a grant
every interval, and each failure is its own WARN from a function neither test names. A count is
also the right shape for it, because #133 is about a rate, and anchoring on a phrase makes the
phrase load-bearing (#128's static test paid for that and had to be re-anchored on its condition).

**Both older tests were rewritten, and one of them could never have passed.** The #132 test
was written to fail on the day of the fix, so it flipped; it now polls *both* halves — the key and
the log line — because the key lands in etcd before the line lands in the log, and reading the log
once at the moment the key appears is a race the first version of the rewrite lost. The #133 test
needed a **new premise** (`stop_etcd()` instead of a revoke, which also exercises the `Unavailable`
branch), because after this fix a revoked lease is no longer a permanent condition. Its first
version then asserted on the `Unavailable` branch's own log line — which is **DEBUG**, while the
nodes run at the default level, so the assertion could not pass at any point. That branch is quiet
by design; what is observable about it is the **absence** of the rewrite, and that is what the test
states.

One red run in the rewrite paid for a lesson of its own: the #132 test read node 1's `MM_PEERS`
**once**, at the moment the key came back, and found no row for peer 2 at all. That list is per
*peer record*, and the re-registration moves node 1 through learning the address again — the same
shape as asking for `replicas[0]` in one instant while a replica reconnects, which this repository
has now hit three times. The property is that the address comes back, so the test waits for it.

**Mutation table: eight, seven with the verdict they were supposed to give, and the eighth is
recorded as surviving because the reason is worth more than the row.**

| mutation | wanted | verdict |
|---|---|---|
| the unreachable case reads as `Absent` | KILLED | KILLED — the flood the quiet branch prevents |
| rewrite on every refusal, not only on `Absent` | KILLED | KILLED — **by the third test alone** |
| the `kvs` test inverted | KILLED | KILLED |
| the `Present` branch logs at INFO | KILLED | KILLED — by the rate bound |
| the `Unavailable` branch logs at WARN | KILLED | KILLED — by the rate bound |
| `etcd_post()` returns success on a curl error | KILLED | **SURVIVED** |
| the status is never remembered | SURVIVES | SURVIVED |
| the WARN's wording is changed (control) | SURVIVES | SURVIVED |

**The survivor is a real gap and it is narrow in a way worth writing down.** The simple path is
masked by `read_self_key()`'s own `response.empty()` guard: with etcd unreachable the transport
fails, the body is empty either way, and the answer is still `Unavailable`. The only state where the
mutation changes behaviour is one where etcd answers the **range** and then fails the **PUT** — and
then `register_self()` would return `true` without having written anything, so the node would log
`has registered again` about a key that is still gone. That is the worst kind of wrong: the log
would be the thing that lies.

That state is constructible and **#135 is the item that constructs it** — measured there, a node
given a dead endpoint *first* has `connect()` and `grant_lease()` succeed through the live one while
the PUT to `endpoints[0]` fails. This battery cannot express it today because every node it starts
gets exactly one endpoint, so the row stays recorded rather than closed, and the shape needed to
close it is named.

- Effort: M, mostly the decision | Impact: a node that kept serving and kept its existing links
  while being invisible to the registry — so the cluster worked until the day something needed to
  look an address up

### 131. Seven more loops end on their first exception ✅

Found by a mutation that **survived** while #112's last loop was being closed, which is the part
worth recording.

#112's per-iteration rule was first written as a hand-list of the four loops that item names.
Deleting a row from that list survived: the rule covered less and stayed green. That is the
**third** time inside #112 that a list written by hand turned out not to be evidence about the code
— the count of thread entry points was wrong the same way twice, one then eleven then seventeen — so
the set is derived from the tree now and the list only says what each member *is*.

Derived: fourteen functions in `src/` match `void Class::…loop()`. One is a notifier with no loop in
it (`MultiMasterManager::wake_io_loop`) and is named, so a real loop cannot hide by having its
`while` rewritten. Of the thirteen that remain, **six** guard one iteration at a time — #112's four
plus `ReplicationClient::run_loop()`, which has done so since before that item, and
`ReplicationManager::run_loop()` — and **seven do not**:

| loop | what its death costs |
|---|---|
| `PeerRegistry::watch_loop()` | no new or moved peer is ever learned; the mesh stops growing. It also **calls the topology callback**, so `handle_topology_change()` — which dials peers — runs inside it |
| `MultiMasterManager::reconnect_loop()` | a dropped mesh link is never re-dialled; #95's and #97's work all lives here |
| `AntiEntropyManager::loop()` | reconciliation stops, so divergence between masters is never repaired (#57's whole mechanism) |
| `ShardRouter::watch_loop()` | the shard map goes stale and this client keeps routing by it |
| `ShardCoordinator::watch_loop()` | shard ownership is never re-read; it also discards `refresh_lease()`'s answer, which is #133's rejected alternative |
| `MetricsServer::run_loop()` | `/metrics` stops answering: monitoring goes dark while the engine is fine, which is the same "every outward signal disagrees with reality" problem from the other side |
| `OrderbookPool::health_check_loop()` | a client pool stops noticing dead connections; the only one of the seven outside the server |

**Closed, and the seven judgements came out as one shape plus one exception.** Reading all seven
gave the same answer six times: **one pass is the unit**, because none of them loses anything a
later iteration will not redo. That is the difference from #112's mesh and replication loops, whose
`EPOLLET` registrations made an abandoned event unrecoverable and forced a boundary per *event*.
Here the topology watch re-polls the same prefix, the reconnect loop re-dials with the backoff it
already claimed under the lock, anti-entropy re-compares the same version vectors, both shard
watches re-read, and the client pool recomputes `primary_idx_` from scratch. An abandoned pass costs
one interval, and that number is written beside each guard.

**`MetricsServer` was the exception this item predicted, and it found a leak.** Its *pass* is the
safest of the seven to abandon — the only registration is the listen socket and it is
**level-triggered**, so a dropped connection is offered again — and it has no nap at all, because
the 200 ms `epoll_wait` timeout is its pacing. But the unit *inside* the pass is one request, and
that is where the descriptor lives: `handle_request` closed the socket at its two `return`s and on
neither of the two paths that throw. `registry_.serialize()` builds a string of every metric and the
response concatenation builds another; on a box short of memory either is a `std::bad_alloc` through
a function holding an open socket, so the leak is one descriptor per failed request until EMFILE —
**which is a metrics endpoint that stops answering, this item's own failure arriving by a second
road**. Closed by scope now, which makes the class impossible rather than caught once.

**None of the seven has a throwing path today either, and that is established rather than assumed.**
`anti_entropy.cpp`, `shard_router.cpp`, `shard_coordinator.cpp` and `metrics_server.cpp` contain no
`throw` at all; the four in `multi_master.cpp` are all in `start()`; `PeerInfo::from_json` catches
nlohmann's `parse_error` and type-checks **every** field before `get<>`; and both `std::stoi` calls
in `shard_router.cpp` — the one place in the seven that parses a number out of an address the
registry handed it — already sit inside `try`/`catch (...)`. What is left is `std::bad_alloc` from
string growth and `std::system_error` from a mutex.

**One mechanism, not seven copies, and one counter, not seven.** `include/orderbook/loop_guard.hpp`
is the shape: `caught()` counts and says so loudly once, `ok()` closes the episode — and `ok()` is
**unreachable from an iteration that threw**, which is what the mesh loop paid for with a log
alternating ERROR / "handled again" for three failing records. Pacing stays with the caller, because
these seven wait in three different ways and because #112 measured that a boundary can *create* a
busy-spin. The four existing counters stay as they are: three were measured firing, and each asks an
operator for a different thing. These seven all ask for the same thing — read the line, it names the
loop — so `ob_loop_errors_total` covers them, and seven registered counters nothing can reach would
have been seven flat zeros dressed as coverage (#117).

**The registry is a pointer, and the two client-side loops pass null.** `ShardRouter` and
`OrderbookPool` run in the caller's process; the one that owns a metrics registry is the server. The
stated cost is that both libraries now link `orderbook_metrics`, because a header-only `caught()`
names `increment_counter` even where it never calls it — taken against a second hand-written
boundary, which is the drift `log_episode.hpp` exists to prevent. `orderbook_metrics` depends only
on `orderbook_core` and `orderbook_logger`, which both of those already link, so the graph does not
grow.

**The eighth is not invisible**: the checker requires every loop in `src/` to be classified, in both
directions, so a new loop has to join a list or explain itself and a row naming a function the tree
no longer has fails too. The `recorded` list is empty now and **stays in the file** — it is where
the next unguarded loop goes if it is not guarded on the day it is written.

**Two of this item's own instruments were wrong first, and both cost real time.** The script that
wrapped the seven bodies was given each region as its first and *last* line; the end of a loop body
is a run of closing braces, so it matched the wrong `}` and wrapped six lines of a ninety-five-line
poll while reporting success. It takes the line that *follows* the region now, which is unique prose
in every one of the seven. And the first version of the descriptor test read to EOF — with the leak
planted, the server never closed, so the test **hung** rather than failed: past ten minutes with no
deadline, 250 s once the client had one. It reads a single response now and the same mutation dies
in 6 s.

- Effort: M | Impact: seven subsystems that ended quietly on their first exception, each leaving a
  node that answers health checks — plus one descriptor leak the reading found on the way

### 130. A promotion that stops halfway leaves the node holding a leader key it cannot act on ✅

Found while measuring #112's `monitor_loop` half. The heading changed with the fix, and so did most
of this entry, because **the mechanism it was filed with was wrong** — that correction is worth more
than the fix.

**What it said, and why it was wrong.** "The next monitor tick reads the leader key, finds this
node's address in it, and adopts it — so the node is a replica of itself."
`FailoverManager::adopt_leader_if_present()` **refuses** a key that names us, and has since
**23 August 2026** (`183531e`, #73's fix), three weeks before this item was filed. So the first of
its three candidate answers — "refuse to adopt our own address" — was **already in the tree**, and
the sentence describing the defect pointed at a function that does the opposite. A mechanism in a
filed item is a hypothesis until it names the line; this one named a line that disproved it.

**The real one, read from the code and then measured.** `attempt_promotion()` ran, in order:
`lease_id_`, then `epoch_` and `primary_address_ = config_.replication_address` under the lock, then
`role_.store(PRIMARY)`, and **then** `handler_.promote_to_primary()` — which throws. So one
half-finished act was visible as three fields:

| | before the fix | after |
|---|---|---|
| `FailoverManager::role_` | **PRIMARY** — so the monitor's PRIMARY branch **renewed the lease** | REPLICA until the handler returns |
| `FailoverManager::primary_address_` | this node's own address | empty while unfinished |
| `Engine::node_role_` | REPLICA, read-only | unchanged |
| `Engine::current_epoch_` | 2, with no `EPOCH` record on disk | unchanged |
| `repl_client_`, `repl_state.txt` | destroyed, deleted | unchanged |

`ROLE` answered `REPLICA <this node's own replication port> 2` because
`Engine::handle_role_command()` assembles it from **three views**: the role from the engine, the
address from the failover manager, the epoch from the engine — and the promotion had already moved
two of them. Nothing adopted anything.

**And the consequence was worse than the item claimed.** Because `role_` said PRIMARY, the monitor
went on refreshing the lease, so the leader key stayed alive and **no peer would ever take the
role** — while the engine refused every write and had no replication manager. Having already
deleted `repl_state.txt`, the node could not fall back to following anyone either. A leader that
accepts nothing and never lets go, answering `PING` throughout.

**The second candidate is unsafe as it was written, and naming that is the other half of this
item.** "Release the key when the promotion fails" reads as obviously right. Epoch selection is

```cpp
EpochValue current = (fm_epoch.term > local_epoch.term) ? fm_epoch : local_epoch;
EpochValue new_epoch = current.incremented();
```

— `max(epoch known from etcd, engine epoch) + 1`. A node that took the key at epoch 2, stamped
`wal_.set_epoch(2)` and then released it leaves a peer which **never observed that key** computing
`max(1, 1) + 1 = 2` and winning the same epoch, against a node that already has it in its WAL.
Releasing the key erases the only record that epoch 2 was consumed, because etcd is where epochs are
agreed. Making it safe would mean adding a *third* durable artefact to an act that already fails on
the inconsistency of two. Rejected, with the cost of rejecting it written down: a permanently
refusing disk means the node holds the key and retries for ever, so the cluster has no primary —
but loudly and with a counter, rather than silently as before.

**The fix is the third candidate plus the two things it needs to be safe.**

- `role_` and `primary_address_` are set **after** `handler_.promote_to_primary()` returns. A
  promotion has two durable effects and this announced the second before it happened.
- The REPLICA arm of `monitor_tick()` gains the arm the state machine was missing: a node holding a
  key that **names it** while not being primary finishes the promotion on a later tick, **with the
  epoch from the key**. Not by re-running the CAS — that is create-only and the key exists, so it
  would fail for ever against our own entry. Idempotent by construction rather than by care:
  `repl_client_` is already gone, `repl_state.txt` already deleted, `current_epoch_` and
  `wal_.set_epoch()` receive the same value, and a second `EPOCH` record for the same epoch is
  harmless because replay only ever **raises** the epoch it reads.
- `stop()` revokes the lease when it **holds a lease**, not when it believes it is PRIMARY. The role
  was a proxy for holding one, and moving `role_` breaks exactly that proxy: a shutdown in the new
  window would have left the leader key alive until its TTL — a failover made slower by the change
  meant to make one honest. *The condition to act on is the resource you hold, not the role you
  think you have* — the same lesson as #131's descriptor guard, one subsystem along.

Rolling the promotion back inside the `catch` is still rejected, for the reason the item was filed
with: that is failover semantics in the least-reviewed code of any subsystem.

**What `ROLE` still cannot say, stated rather than left to be discovered.** There is no word in its
vocabulary for "holds the leader key and serves nothing". The address is **empty** while a won
promotion is unfinished, which is the only truthful thing available — not the node we stopped
following, and not ourselves — and the log carries the rest. Giving it a word is a protocol change
and is not smuggled in here.

**Measured**, with #54's injector refusing the 32-byte `EPOCH` record on the replica only, two nodes
on one etcd: across a twenty-second window at one sample a second, `ROLE` never names this node's
own port, the stalled promotion is reported **once** with what an operator can do about it,
`ob_monitor_errors_total` moves, and `PING` answers on every sample. The control in the same module,
at a size nothing writes, still reaches `PRIMARY`. **3 passed in 86.5 s**; against the two source
files from the commit before the fix, the new test fails and the other two pass.

- Effort: M, mostly the decision | Impact: was a node whose disk refused one 32-byte record holding
  and renewing the cluster's leader key while refusing every write, reporting a role that cannot
  exist. No data lost; availability lost, silently, with no counter moving after the first tick

### 129. A mesh node's shutdown waits out the lease loop's sleep ✅

Found while reading `PeerRegistry::lease_loop()` for #112's remaining half, and then measured,
because the two numbers that bracket it were already published and disagreed: #106 measured `SIGTERM`
at **0.11 s** for a node with nobody connected, and the integration harness escalates to `SIGKILL`
after five seconds — a window wide enough to hide seconds of waiting without anything ever saying so.

That loop slept `max(1, lease_ttl/3)` **seconds** in a plain `std::this_thread::sleep_for`, and
`stop_watch()` joins it. `join()` cannot interrupt a sleeping thread, so shutdown waited.

**Measured before the fix** (`build/ob_tcp_server` against native etcd, i3-7100U, node up ~6 s,
nothing connected):

| configuration | SIGTERM → exit | lease interval |
|---|---|---|
| standalone, no coordinator (**control**) | **0.22 s** | — |
| mesh node, `--coordinator-lease-ttl 3` | **1.05 s** | 1 s |
| mesh node, default `--coordinator-lease-ttl 10` | **2.94 s** | 3 s |
| mesh node, `--coordinator-lease-ttl 30` | **4.02 s** | 10 s |

The 30-second row is the one that establishes the mechanism rather than the default one. Ten seconds
of sleep, entered about six seconds before the signal, leaves about four — which is what was
measured. So the cost is **the remainder of the current sleep**, and only its bound is a function of
the TTL. A model that said "ttl/3" would have predicted 10 s and been wrong in the direction that
looks conservative.

**After**, same probe, same machine: **0.33 s** at the default TTL and **0.24 s** at 30 s, against a
standalone control that stayed at **0.24 s**. The number to read is not the drop; it is that a mesh
node now exits as fast as a node with no lease loop at all, which is the property.

**The fix was already in this tree, with a comment explaining it.** `Engine::flush_loop()` waits on a
condition variable with the stop flag as its predicate, under: *"A plain `sleep_for()` here made
`close()` block until the current interval elapsed, because `join()` cannot interrupt a sleeping
thread: shutdown took up to `flush_interval_ns_` for no reason, and tests that open and close an
Engine per case paid it every time."* This is the **third** place with the shape — the mesh's
`wakeup_fd_` comment records the second, where `stop()` used to wait out a 500 ms `epoll_wait`. The
notification is sent after the flag is stored and before either join, because the predicate reads the
flag: a notification that arrives first is one the waiter sleeps through.

`watch_loop()` was checked and left alone: it sleeps 100 ms per pass, so its contribution is bounded
at a tenth of a second and there is nothing to fix.

**The test is a property, not a duration, and that is deliberate.** It gives the registry a 3600-second
TTL — a 1200-second interval — and requires `stop_watch()` to return inside two seconds. Three orders
of magnitude, so it cannot fail on load; a gate that could is the kind that teaches people to re-run
until green (#10.7's own docstring). The control was run at a shortened interval so that it fails in
ten seconds rather than twenty minutes: with `sleep_for` restored the test fails at **10 000 ms**, and
with the fix back it passes in **305 ms**.

- Effort: S | Impact: every mesh node's shutdown paid up to `lease_ttl/3` seconds after the drain
  timeout #106 exists to bound, and a supervisor's stop timeout or a harness's `SIGKILL` escalation
  absorbed it silently

### 128. The mesh io loop closes descriptors by number, and a number it no longer owns can belong to anything ✅

Found by `sanitizers-integration (tsan)` on PR #123 — on a branch whose diff does not contain one
line of `src/multi_master.cpp`. The required check asked the question; the report answered it,
because it names the descriptor's **creation site**:

```
WARNING: ThreadSanitizer: data race (pid=6039)
  Write of size 8 at 0x72b000000160 by thread T5 (mutexes: write M0):
    #0 close
    #1 ob::MultiMasterManager::io_loop() src/multi_master.cpp:820
  Previous read of size 8 at 0x72b000000160 by main thread:
    #0 epoll_ctl
    #1 ob::TcpServer::run() src/tcp_server.cpp:1773
  Location is file descriptor 11 created by main thread at:
    #0 epoll_create1
    #1 ob::TcpServer::run() src/tcp_server.cpp:1743
```

Descriptor 11 was **the client port's epoll instance**, and the mesh closed it. Not a torn read of a
shared variable: one subsystem destroyed another subsystem's object, and the only reason it surfaces
as a race at all is that ThreadSanitizer tracks descriptors.

**The cost, measured in the same run.** `TcpServer::run()` does `break` on a failed `epoll_wait()`
under the comment "fatal epoll error", so the client-facing loop ends. Nine integration tests failed
with `Node node-2 (port 60727) not ready after 60.0s: [Errno 111] Connection refused` — a node whose
mesh was up and whose client port never served one connection. The read TSan caught is at line 1773,
the subscription hub's registration, so the close landed **while the client port was still assembling
its epoll set**: the node did not lose the ability to accept clients, it never had it.

**The mechanism, and every step is a line rather than a hypothesis.** `io_loop()` harvests events
with `epoll_wait()` and takes `mtx_` afterwards, per event. The registrations carried `ev.data.fd` —
a descriptor number. Between the harvest and the dispatch, three threads other than the loop close
peer sockets, each under the same `mtx_`, which orders nothing about descriptor numbers:
`handle_topology_change()` → `disconnect_peer()` on the peer-registry watch thread (its lock at
`src/multi_master.cpp:2359` is the M0 the report names), `check_backpressure()` on the **client write
path** through `broadcast_local()`, and the reconnect loop. A closed number goes to the first taker
in the whole process — and `TcpServer::run()` calls `epoll_create1()` at line 1743, *after*
`Engine::open()` at line 1675 has started the mesh threads. The startup window is therefore exactly
the window in which a mesh drops a duplicate link and a client port opens its epoll set.

The loop then reached its own defensive branch for "a descriptor in the epoll set with no record
behind it", wrote the warning that branch exists to write, and ran `epoll_ctl(DEL)` + `close(ev_fd)`
on a number that was no longer its own.

**The rule was already known, written down twice, and applied to a container both times.**
`include/orderbook/multi_master.hpp`, about `pending_`: *"Keyed by `conn_id` rather than by the
descriptor. Descriptor numbers are reused by the kernel and mean something to the epoll set; a
`conn_id` is minted once and means nothing to anyone else, which is the property a key needs."* And
`src/tcp_server.cpp`, about subscriptions: *"Descriptor numbers are reused, so a subscription pinned
to `fd` alone would outlive its connection and push rows to whoever inherits the number."* Both are
right and both came out of real defects (#96, #45). Neither reached the one place where a descriptor
number is not only a key but also the argument to `close()`. It is the **third** appearance of this
class in this file: the `wakeup_fd_` comment records a shutdown that called `epoll_wait()` on a
number the kernel had already reassigned, reported by TSan as a race on file descriptor 4.

**The fix is what an event says, and the second half is not the one the report is about.**
`ev.data.u64` carries the connection's `conn_id`, and the dispatch resolves that —
`find_connection_by_fd()` became `find_connection_by_conn_id()`, a lookup rather than a scan for
`pending_`, which #96 had already keyed that way. Two values are reserved for the two descriptors
the loop owns for its whole life and never closes mid-iteration, `listen_fd_` and `wakeup_fd_`, where
the number *is* the identity; `conn_id` is minted from 1 upwards, so neither can collide with a
connection. Every read of a descriptor inside the peer branch now comes from the **record**
(`peer_ptr->fd`) rather than from the event.

The half that is about correctness rather than about crashes: a stale event carrying only a number
can be attributed to a **live** connection that inherited it, and an `EPOLLHUP` from a peer that is
already gone would then drop a healthy link with nothing in the log. That is #96's shape, one layer
out, and `PendingPeers.AConnectionOnARecycledDescriptorIsItsOwnConnection` is what holds it — with
the descriptor number read back from both connections, so a run where the kernel did not hand the
number back says so instead of passing quietly.

**And the branch closes nothing now, which is the fix rather than an omission.** An event about a
connection that is gone is routine — the harvest precedes the lock — and closing the socket already
removed its registration, so there is nothing left to disarm. Nor can a skipped event repeat: every
registration in the mesh is `EPOLLET`, so ignoring one is not the busy loop that would make silence
expensive. What the old branch was defending against is an armed descriptor whose record was dropped
without closing its socket; against that, closing by number was never reliable either, because under
edge triggering there may be no next event at all. The trade is a descriptor leak that has never been
measured, against a close of another subsystem's descriptor that has.

**Accepting a connection now records it before arming it**, because the registration has to carry an
identity and the identity is minted under `mtx_`. That also closes a window this path used to open on
purpose: for a few instructions the descriptor was armed with no record behind it, which is the exact
state the old branch was written to complain about.

**Asked and answered about the neighbours, because "I did not check" and "I checked, it does not
apply" read the same afterwards.** In `src/tcp_server.cpp` every `close_session()` and `::close()` is
on the loop thread or after the loop has ended, so nothing else can free a number under it — today.
In `src/replication.cpp` the premise does not hold: `ReplicationManager::broadcast()` runs on the
client write path and calls `remove_replica_locked()`, which closes the socket, so stale events exist
there too. What is absent is this defect's teeth — that loop never closes a descriptor it cannot find
a record for, and the worst a stale event can do is a spurious drain or read on a new replica that
inherited the number, both of which the next real event would have done anyway. Named here rather
than filed, and rather than fixed in a change about the mesh.

**Cost on the write path, measured rather than argued.** The mesh write path reaches
`arm_epollout()` and `disarm_epollout()` through `broadcast_local()`, so the key is built on the path
a client write takes. `scripts/mnemonic_diff.py`, Release, master `c82f01e` against this commit:

| function | base | head | |
|---|---|---|---|
| `MultiMasterManager::arm_epollout` | 23 | 23 | one instruction chosen differently |
| `MultiMasterManager::disarm_epollout` | 23 | 23 | the same |
| `MultiMasterManager::broadcast_local` | 92 | 92 | identical |
| `Engine::apply_delta_mm` | 602 | 602 | identical |
| `WALWriter::append` | 90 | 90 | identical |

The one difference is worth reading, because it is smaller than "the same count" suggests. The old
form wrote a descriptor into the union and then cleared the padding — `mov %edx,0x10(%rsp)` followed
by `movl $0x0,0x14(%rsp)`. The new one loads the identity and stores all eight bytes at once —
`mov 0x30(%rsi),%rax` … `mov %rax,0x10(%rsp)`. Two four-byte stores became a load and one eight-byte
store, so the count is unchanged and there is no claim about time here: fewer or equal instructions
is not a speed-up, it is only not more work.

**Seven mutations, each with the verdict it had to give, and which test kills which is the point.**

| mutation | verdict | killed by |
|---|---|---|
| `arm_epollout` carries the descriptor again | KILLED | the static test alone |
| the accepted connection is armed with its descriptor | KILLED | the static test alone |
| the gone-connection branch closes the number again | KILLED | the static test alone |
| the lookup matches the descriptor instead of the identity | KILLED | the behavioural test alone |
| the listen sentinel collides with the first `conn_id` | KILLED | both |
| the wakeup sentinel collides with a `conn_id` | KILLED | both |
| control: the note about a connection that is gone is reworded | **SURVIVED** | — |

Three of the six are killed by the static test and by nothing else, which is what says that test
carries weight rather than decoration: reintroducing `data.fd` at one of six registration sites is
invisible to every behavioural test in this repository, because the harm needs a descriptor number to
be recycled *and* an event to be in flight across it. One is killed only by the behavioural test,
which is the other half — that the dispatch resolves an identity and not a number.

The control had to be moved to get here, and the reason is a rule in its own right: the static test's
first version anchored on the branch's **log phrase**, so rewording the message failed the test.
A check anchored on prose makes the prose load-bearing and hands the next person to improve the
wording a failure with no explanation. It anchors on the branch's condition now, and the control —
rewording that message — survives, as a control must.

**And the first version of that guard was itself a use-after-free, caught by a required check on
this very PR.** `mm->peer_states()` returns the vector **by value**, and the helper beside it takes a
reference and hands back a pointer into it — so `find_peer(mm->peer_states(), 2)` reads perfectly
well and points into a vector that dies at the end of the expression. It passed locally, four times
in a row and five out of five under repetition, because the freed memory still held the right values;
`sanitizers (tsan)` called it a **heap-use-after-free** at four lines of that one test, and nothing
else in the job reported anything.

The repair is not "hold the vector in a local", though it is that too. The rvalue overload of the
helper is now **deleted**, so the shape is a compile error rather than something a sanitizer has to
be running to catch — verified in both directions: the bad form fails to build with *use of deleted
function*, and the file builds and passes again once restored. That is the same move as #127's
`levels_from_payload()` and #92's `shared_ptr`: the class becomes impossible rather than fixed once.

**What no test here can do, said plainly.** `PendingPeers.AConnectionOnARecycledDescriptorIsItsOwnConnection`
is a regression guard, not a reproduction: it establishes that the second connection really does land
on the first one's descriptor number — read back and compared, so a run where the kernel did not hand
it back says so — but the harm needed an event in flight across that moment, and a test cannot
schedule the kernel. The evidence that the defect was real is the TSan report; the evidence that it
is gone is the absence of the shape, which is what the static test is for.

- Effort: M | Impact: a mesh node could destroy its own client port's epoll instance during startup
  and then refuse every client connection, with nothing in its log but a mesh warning about an
  unrecognised descriptor. Present since the mesh had a reconnect path; reachable whenever a
  duplicate link is resolved while the client port is still starting

### 120. A clock that is off writes one warning per write, not one per excursion ✅

Found by #54 stage D, in the same probe run as #119 — the log of one 200 000-tick run was **22.9 MB**.

`tick_local()` and `tick_receive()` both ended with an unconditional
`if (drift > 1s) OB_LOG_WARN(...)`, and `tick_local()` is on the client write path
(`apply_delta_mm`). **Measured: 200 002 lines for 200 000 ticks.** That is #95's shape — a permanent
condition retried at loop frequency and logged at loop frequency — at write frequency rather than
loop frequency, which is three orders of magnitude worse than the case #116 fixed.

The fix is #116's, applied at this site: one line when the excursion opens, one when it clears,
with the duration on the closing line. `LogEpisode` moved out of `FailoverManager` into
`include/orderbook/log_episode.hpp` to do it — **the second user is what turns a pattern into a
mechanism**, and two copies of "have I already said this" drift apart until the two log shapes an
operator greps for disagree.

**Every occurrence is still counted**, in `ob_mm_hlc_drift_excursions_total`, registered in the
same change that writes it (#117's discipline). The pair is the point and it is not tidiness:
`ob_mm_hlc_drift_ns` is a **peak that never comes down**, because nothing lowers the HLC's physical
component, so a single ten-second excursion and a clock that is permanently an hour out give the
same gauge for ever. The counter is what separates them, and it is the half that can be alerted on
(#94's lesson about `ob_replicas_connected`).

Two tests state the property in the only form a C++ test can, because these tests have no log sink
to read: an excursion lasting a thousand ticks is **one** episode with more than a thousand
occurrences, and an excursion that clears and returns is **two** episodes. The second one exists
because "one line per episode" is otherwise indistinguishable from "one line, ever" — the failure
mode of an episode whose `end()` is never reached, which a mutation confirms is a live risk.

- Effort: S | Impact: an hour of skew on one node's clock was megabytes of log per node and the one
  number that could have been alerted on did not exist

### 119. The HLC went backwards, which is the one property it exists to provide ✅

Found by #54 stage D. `tests/test_hlc_skew.cpp` presents the timestamps a **broken** cluster
produces; `tests/test_hlc.cpp` had always presented the ones a working cluster produces, and the
difference is where this was hiding.

`tick_local()` increments the logical counter on every tick whose physical component has not
moved, and that counter is **16 bits** — fixed by the wire format, which three `static_assert`s on
`HLCTimestamp`'s layout pin. `static_cast<uint16_t>(last_.logical + 1)` wraps silently.

**Measured** (probe against `liborderbook_hlc.a`, i3-7100U):

| run | ticks | regressions | first at |
|-----|-------|-------------|----------|
| control, no remote frame | 200 000 | **0** | — |
| after one remote frame an hour ahead | 200 000 | **3** | tick **65 533**, `…816.65535.1` → `…816.0.1` |

Three regressions per 200 000 ticks is one per 65 536, which is the arithmetic saying the same
thing. The control matters as much as the finding: without it a green skew test cannot be told
from a probe that is not looking.

**Reachable, and not slowly.** `Engine::apply_remote_delta()` hands every replicated record's
timestamp to `tick_receive()`, which stores `max({now, last_, remote})` with no ceiling — and
`src/multi_master.cpp` deserialises that timestamp from the frame header and passes it on **ten
lines later**, with nothing in between. So one frame from a peer whose clock is ahead pins our
physical component above the wall clock, and from then on every local tick increments the counter.
At this engine's published native ingestion rate a pinned window of 100 ms is some 135 000 events,
so the period is crossed twice inside a tenth of a second.

**The fix carries the overflow into the physical component** rather than wrapping it: the order
stays total for ever, and the cost is one nanosecond per 65 536 events, below the resolution of
anything that reads this clock. The alternative — saturating the counter — would stop the reversal
and **silently stop breaking ties**, which is worse than the reversal because nothing would show
it. A separate test therefore asserts that crossing the period produces a **strictly** greater
timestamp, and a mutation confirms that test is load-bearing: saturating kills that test and
**leaves the monotonicity test green**.

**What this deliberately does not do is put a ceiling on the drift a remote may introduce.** With
the counter fixed, a poisoned clock is wrong about real time but not incorrect: every node that
receives such a record adopts the same value, timestamps stay monotonic, and LWW still converges —
which stage D's other measurement confirms from the other side (two nodes with **opposite** drift
agree on the winner of the same conflict, and still agree after merging each other's clocks). A
ceiling would buy a clock that means something in exchange for losing causal order against a peer
we would be refusing to believe. That is a policy decision with a named cost rather than a bug fix,
so the current behaviour is **pinned by a test** — changing it has to be a decision, and the
question is recorded as #121 rather than settled quietly here.

- Effort: S | Impact: P0 by consequence. LWW conflict resolution is built on HLC ordering, so a
  clock that goes backwards is two nodes able to disagree permanently about a row's content, and
  nothing in the engine would notice — anti-entropy compares what each side holds, and two sides
  each holding a different winner look consistent to it. Never observed in the wild

### 127. Every receive path cast a pointer into a byte buffer to `const Level*`, which is undefined behaviour when that buffer is not aligned ✅

Found by #54's D3 on its first CI run, in the `sanitizers (asan)` job — the one place that builds
this suite with UBSan. Not by the test failing on what it asserts: the assertions passed, and UBSan
reported the engine underneath them.

```
src/engine.cpp:878:76: runtime error: member access within misaligned address 0x50e000007282
for type 'const struct Level', which requires 8 byte alignment
    #0 ob::Engine::apply_remote_delta(…) engine.cpp:878
    #1 ob::MultiMasterManager::handle_remote_record(…) multi_master.cpp:568
    #2 ob::MultiMasterManager::handle_frame(…) multi_master.cpp:1720
    #3 ob::MultiMasterManager::process_recv_buf(…) multi_master.cpp:1560
    #4 ob::MultiMasterManager::io_loop() multi_master.cpp:909
```

**The arithmetic is the whole defect.** A mesh frame is a 4-byte length and a 38-byte `WALRecordV2`,
so the payload starts **42** bytes into the receive buffer and the levels 42 + 88 = **130** bytes in.
Neither is a multiple of `alignof(Level)`, which is 8. `reinterpret_cast<const Level*>(payload +
sizeof(DeltaUpdate))` therefore produces a pointer the standard says may not be dereferenced, and
`levels[i].price` is the dereference. On x86 it compiles to an unaligned load and works, which is
exactly why it survived every mesh test this repository has: the machine forgives it and the standard
does not, and a compiler is entitled to assume the alignment it was promised.

**Why nothing caught it before, which is the reusable part.** Four layers had to line up. The unit
tests call `apply_remote_delta()` with a real `Level` array, so they are aligned by construction. The
integration battery drives real frames through real sockets, but the battery's sanitizer job is
**TSan**, and TSan does not check alignment. The ASan/UBSan job builds the C++ suite, and until D3
nothing in that suite took a delta **off a socket**. And the one harness that does read arbitrary
bytes under UBSan — the fuzzer — drives `parse_frames`, not the apply path behind it. The defect sat
in the gap between four things that each cover most of it.

**Fixed as a class, in one place.** `include/orderbook/level_payload.hpp` copies the levels into a
caller-owned scratch buffer and hands back an aligned pointer; the two hot receive paths keep that
buffer as a member, so it allocates once per process rather than once per record, and WAL replay uses
a local because it runs once per record at startup and never again. All **four** sites that had this
shape now go through it: the mesh, the two in the replication client, and WAL replay. Only the mesh
one was *observed* misaligned — the other three are the same construction and are said to be, rather
than claimed to have been measured.

**Cost, measured rather than argued** (`scripts/mnemonic_diff.py`, Release, against `origin/master`):
`handle_remote_record` **188 → 393** instructions, `ReplicationClient::receive_and_replay`
**732 → 743**, and the apply paths behind both — `apply_remote_delta` (800 plus a 47-instruction cold
clone) and `apply_delta_replicated` — **identical, instruction for instruction**. So the whole cost
is one `memcpy` of `n_levels × 24` bytes per received record, in the function that parses it, and
nothing changed in the function that applies it. No claim about time: the copy is one more pass over
bytes the same record already walks three times (the CRC over the payload, conflict resolution per
level, the WAL append).

One thing in that number is worth keeping. The first version called `scratch.resize(n_levels)` per
record, which **value-initialises** the new elements, because `Level` has a default member
initialiser (`_pad{}`) — zero-filling bytes the `memcpy` immediately overwrites. Growing only when
the buffer is too small saved **three** instructions of the 205, so the resize was not the cost; the
copy is. Measuring said that, and reading the code would have guessed wrong in both directions.

- Effort: S | Impact: undefined behaviour on every record the mesh and the replication stream
  receive. It works on x86 today, which is the only reason this is an S rather than a P0 — and the
  reason it needed a sanitizer to find rather than a bug report

### 126. An acknowledged write that landed behind a torn WAL record did not survive a restart ✅

Measured by #54's A2.2, which existed to settle a hypothesis from reading the code. It is confirmed.

**The shape.** `WALWriter::write_record()` loops `while (remaining > 0)`, so a short write is
resumed and the record completes — correct, and it means a torn record needs a short write **and** a
failed retry. Injected exactly that (`OB_FAULT_SHORT=20 OB_FAULT_SHORT_THEN_FAIL=1` with `ENOSPC`),
the file ends up as `[record][record][20 stranded bytes][record][record]`: the write that was cut is
**refused**, so nobody is misled about it, and the two writes after it are answered **`OK`**.

**What comes back after a `SIGKILL`: the two before the tear, and neither of the two after it.**
`WALReplayer::replay()` returns at the first CRC mismatch — `return last_good_seq`, not `break` — so
it stops for the whole *directory*, not for the file it is in. The 20 stranded bytes are a header's
first 20 bytes (sequence, timestamp, checksum), the next four are read from the following record, and
the payload that describes fails its checksum. Everything past that point is unreachable, however
many files it spans.

So the durability sentence in `docs/cli.md` — an acknowledged `INSERT` is in a WAL record before the
reply and survives a process kill — is **false for every write that follows a torn record**, and
nothing tells the client which of its writes those are.

**The measurement, and why the first run of it was wrong.** Five writes, the third cut: stranded
**2 of 2**. The first run read **1**, because `FaultNode` starts a node with
`--flush-interval-ms 500` and a tick had moved one row into a segment and written a checkpoint —
replay begins after the last checkpoint, so the row was rescued by timing rather than by the WAL.
The test now runs with the tick an hour out and kills the node rather than stopping it, because a
clean stop ends in a checkpoint too. A test of what replay can reach has to keep everything that
writes a checkpoint away from it.

**Fixed with the two halves, because neither is any use alone.** The writer **abandons** a file
whose record it tore — opening the next one and publishing that position — and replay treats a
checksum mismatch in a file that is **not the last** as a tear and continues with the next file. Do
only the first and replay still stops at the tear, because it stopped for the whole directory. Do
only the second and there is nothing behind the tear to find, because the writer kept appending into
the abandoned region.

**Two details in the writer's half are the whole of its correctness.** It is *not* `rotate()`: that
function's first act is to write a ROTATE record **into the file just established as unwritable**.
So there is no marker, and the replayer's rule is what replaces it. And it fires only when the write
left bytes behind — `remaining < total` — not on every failed write: a disk that stays full fails
atomically, and abandoning the file each time would produce **one empty WAL file per refused
write**, a pathology the fix would have introduced rather than removed. That condition now has its
own assertion in the disk-stays-full test.

`abandon_torn_file()` is `noexcept` and logs rather than throws if it cannot open the next file,
because the caller is about to report the write's own error — the one that says why the disk refused
— and it must not be replaced by a second error about the recovery.

**Measured after, by the test that measured before:** the same five writes with the third cut leave
**0** stranded where they left 2 of 2, everything else acknowledged comes back, and the evidence
that this is the new path is `wal_000001.bin` — the node's rotation threshold is 512 MB and the test
writes five records, so a second file exists only because the torn one was abandoned. Two unit tests
pin the replayer's rule from both sides: a mismatch in an earlier file yields the records from the
file behind it, and a mismatch in the **last** file still stops, because that one is a crash tail and
reading past it would hand the engine a record the process never finished writing.

**Alternatives, named because they were considered.** A tear record (its own type, or a
length-prefixed skip) would let replay step over exactly the abandoned bytes — and it needs *another
write to the file that just refused one*, so the fix would depend on the thing that broke. Refusing
all further writes to a torn file turns a lost tail into an outage. Doing nothing was what the
measurement was against.

**The replayer's half is a migration path, and finding that out took a failing assertion.** A file
the *new* writer abandons ends mid-record — 20 bytes of a 24-byte header — and every reader here has
always treated a short header as the end of that file, without comparing a checksum at all. So the
mismatch rule cannot be reached by anything this build writes. What it is for is a directory an
**older build** left behind, where records sit *behind* the tear and the reader assembles a parseable
header out of the stranded bytes and the record following them. That is now said out loud rather than
implied: `WALReplayer::tears_skipped()` counts those files, `Engine::open()` logs a line naming what
it means, and the integration test asserts the replay is **silent** about a checksum — because with
the writer's half in place, a compared checksum would mean something was written behind the tear.

**Six mutations, each with the verdict it had to give — and two of them rewrote the fix.**

*"The last file is identified by the wrong end of the list"* survived the first table, and chasing it
found the unit test passing for the wrong reason **twice**. With the file ending in the 20 stranded
bytes, the reader's header read is short and the replayer has always continued past that; with 136
bytes of `0x5A` behind them, the assembled header claims a 23 130-byte payload and the *payload* read
is short, which is the same path again. It takes a **real** record behind the tear — its first four
bytes are a small sequence number, so the garbled header claims a four-byte payload, the read
succeeds, and the checksum is finally compared. A synthetic corruption is not the corruption you get.

*"Replay reads past a mismatch in the last file too"* survived because it was **not a mutation**:
returning from the whole replay and ending the current file differ only when a later file exists, and
in the last file there is none. The `is_last` branch was therefore one guarantee stated twice, which
is the shape that cannot be mutated separately — so it is now one branch, and the distinction lives
where it is real: in `tears_skipped()` and in the message an operator reads.

The test for that case had the **same** defect as its neighbour and it took finding one to see the
other: `AMismatchInTheLastFileStillStopsTheReplay` appended 136 bytes of plausible garbage, so its
two assertions held whatever this rule did. It now appends a real record too, and a control says the
rewrite is load-bearing: with `is_last` forced to `false` in `replay_v2()`, so that nothing is ever
the last file, it fails — where before the rewrite it passed. Each test's output now carries the WARN
line for its own case, which is the cheapest proof that a checksum was compared at all.

- Effort: M | Impact: the engine's central durability claim had an exception nobody could see from
  outside. The window is narrow — it needs a write that fails *after* writing part of a record —
  but inside it every later acknowledgement was a promise the restart broke

### 125. A replica sent to the snapshot path could never bootstrap: the manifest checksum covered two fields the wire does not carry ✅

Found by #124's retention test, on the first run that ever reached this path.

`SNAPSHOT_BEGIN` carries four numbers — total bytes, the WAL file index, the WAL byte offset and the
file count — and then one `SNAPSHOT_FILE <path> <size> <crc>` header per file. The replica rebuilds a
`SnapshotManifest` from exactly that, and `SNAPSHOT_END <crc>` was checked against
`crc32c(manifest.to_json())`. But `to_json()` also serialises **`created_at_ns`** and
**`total_rows`**, which `Engine::create_snapshot()` fills in and **nothing puts on the wire**. The
replica's reconstruction leaves both at zero, so the two documents differed by construction and the
checksum could not match — not sometimes, not under load: **never**.

**Measured on a live pair** (i3-7100U, two nodes, `--wal-rotate-bytes 65573`): all 24 files arrived,
every per-file CRC verified — the receiver checks each file against its own header, and no mismatch
was logged for any of them — and the bootstrap was abandoned with
`primary said 2070107884 and the 24 file(s) received make 3121752028`. The replica then reconnected,
was refused again, asked for another snapshot and abandoned it again, **every five seconds, holding
zero rows**, against a primary that was healthy and had every row.

**Why nothing caught it, which is the more useful half.** Two reasons, and they compound.

The path was **unreachable in the battery without #124**: a replica is only sent here when the WAL
file its position names has been removed, retention only removes files *below* the current one, and
reaching a second file needed 512 MB of writes. So no integration test had ever seen
`ERR WAL_TRUNCATED` on the replication link.

And the one unit test of the receiving side was a **mock primary built to agree with it**.
`tests/test_replication.cpp` constructed `ob::SnapshotManifest expected;` and filled precisely the
four fields the wire carries, leaving `created_at_ns` and `total_rows` at their default zero — which
is exactly what the receiver reconstructs. Its own comment said why ("the mock primary has to build
the same manifest to name it"), and that was the defect: the stub was made to match the code under
test rather than the sender it stands in for. A stub that agrees with the receiver proves the
receiver agrees with itself. It now fills both fields in, as a primary does, which makes that test a
regression test for this item — and a mutation confirms it: with the old comparison restored, it
fails.

**The fix is a digest over what was transferred, not two more fields on the wire.**
`SnapshotManifest::transferred_digest()` renders the document with those two fields zeroed and
checksums that; both ends call **the same function**, so a field added to the struct later is
excluded by construction and has to be put on the wire to be checked. The alternative — extending
`SNAPSHOT_BEGIN` — is a protocol change that makes a new replica refuse an older primary, for two
numbers no receiver uses. And the general rule is worth stating in one line, because it is
direction-sensitive: **a checksum over a document the receiver cannot reconstruct is a checksum that
can only fail.**

**Four unit tests and one integration test.** The unit tests build the manifest a **primary**
produces and the one a receiver can rebuild, and require the digests to agree — with a control that
the two documents really do differ, or the test would pass against the defect. A second one requires
the digest to still notice everything that does travel (a file's CRC, its size, its name, a file
missing, the total, the WAL position) and to be unmoved by the two fields it excludes, which is
stated rather than left to be inferred. A third pins that arrival order does not matter, since the
receiver appends in wire order and `to_json()` sorts by path.

The integration test is the one #124 could not finish: a replica is killed, the primary writes past
three rotations, and only when the primary **reports zero replicas connected** and the file the
replica had confirmed is **gone from its directory** is the replica allowed back. Both preconditions
are observed rather than waited out, and that is why it kills rather than stops: a `SIGSTOP`ped
replica stays connected — the process is frozen, so it closes nothing — and retention cannot pass a
replica the primary still counts, which would leave the refusal depending on a flush tick landing in
the gap between the resume and the reconnect. It asserts `ERR WAL_TRUNCATED` in the primary's log
**window** (the log accumulates across the module), every row written during the outage, and a row
written **before** it — because a snapshot replaces the whole store while the rows the primary had
not yet flushed arrive afterwards as WAL records, from the position the snapshot carries.

**Seven mutations, and the one that survived says something about the fix.** Restoring the old
comparison on the **sender** is killed, including by the repaired mock, which is what makes that mock
a regression test. Keeping either excluded field in the digest is killed; so is a digest that stops
covering the file list, and a receiver that ignores the WAL position it was told. The control — a
reworded abandonment message — survives. And *"the receiver checks the whole document again"*
survives **because on that side the two expressions are the same value**: the receiver's manifest
carries those two fields at zero by construction, so zeroing them changes nothing. The defect was
entirely on the sender. The receiver calls the shared function anyway, so that the two definitions
cannot drift when a field is added to the struct — a mechanism no mutation can distinguish today,
recorded rather than removed, because a surviving mutation without a note is one the next reader
assumes was missed.

**Found on the way, and fixed in the same branch: a cluster whose `start()` failed leaked etcd.**
`ClusterManager.start()` ended with `atexit.register(self.shutdown)` and `shutdown()` began with
`if not self._started: return` — both halves of the protection conditional on the start having
finished. #124's mutation run, which deliberately makes a node refuse its arguments, left **four**
orphan etcds holding ports and memory; the three found on 7 September with uptimes over a day were
the same leak, blamed then on a killed run. The net is registered in the constructor now and the
guard asks whether there is anything to clean up. Verified both ways with a probe whose flags the
parser refuses: one leaked etcd before, none after.

- Effort: S | Impact: the documented recovery path for "your position is gone" could not complete,
  so a replica that fell behind far enough never returned. It retried for ever, which reads like a
  slow replica rather than a broken one

### 124. Nothing in the integration battery could cross a WAL file boundary, because the threshold was hardcoded ✅

`--wal-rotate-bytes` now decides where the WAL rotates, default 512 MB, and the battery crosses
boundaries: `tests/integration/test_wal_rotation.py` is the first thing in this repository to do it.

**The flag is defensible on its own merits and that was the condition for doing it at all.** A WAL
rotation threshold is an operator knob comparable systems expose, and it decides three things an
operator has to reason about: what a crash replays, what a reconnecting replica may have to scan,
and what retention can free — files are deleted whole, and only below the file the slowest
**connected** replica has acknowledged. `docs/operations.md` has the section and the table of those
two halves. Had the only argument been "a test cannot reach a rotation", the answer would have been
no; this repository has refused that trade before (pitfall 168 in `CLAUDE.md`) and should keep
refusing it.

**Refused at both ends rather than clamped**, from the constants the WAL itself declares so there is
one definition of each. Above `MAX_WAL_ROTATE_THRESHOLD` (2 GiB) because a WAL position is a file
index and a 32-bit offset read as one value (#85). Below `MIN_WAL_ROTATE_THRESHOLD` — 65573 bytes, a
38-byte header plus the 64 KiB payload limit — because a single write can then fill a file on its
own, so every write rotates and the directory grows one file per record. **The floor belongs to the
flag and deliberately not to `WALWriter`**: the unit tests drive that class with a 512-byte threshold
precisely so rotation is reachable without writing megabytes, which is a reasonable thing for a
component test to do. The distinction is who chose the number.

**The wiring has its own behavioural test, because a flag that goes nowhere is this workspace's
most-repeated defect** (`provisional`, `basis`, `in_use`, `key_id`, `partition_by`, #104). Measured:
forty 136-byte records against a 4096-byte threshold leave `get_wal_position().first` above zero,
and the control — the same records at the default — leaves it at zero. Without the control the test
also passes for a writer that rotates on every write regardless of its argument.

**Three integration tests, and what each one had to establish rather than assume.**

*A replica follows the primary across rotations.* Fifteen hundred single-level records, 136 bytes
each, against the 65573-byte floor: three rotations, every row on the replica. The control is the
rotation itself, read from the primary's directory **and** from `ob_wal_file_index`, because "the
replica has 1500 rows" passes just as well against a WAL that never rotated — which is what every
replication test in this battery measured until now.

*Retention keeps the file a stopped replica still needs.* `SIGSTOP` is the instrument, and it is the
right one: a killed replica leaves `replicas_`, so `safe_truncate` becomes the current file index and
everything below it is freed, while a **stopped** one is still connected and still counted with its
`confirmed_file` frozen. Three claims share that state — the file is kept, the lag reads **more than
a whole file** (the number the arithmetic before #123 could not produce), and
`ob_replicas_lag_unknown` is zero, which is the same fact from the other side since the distance is
measurable precisely because the files are there. The control is taken **before** the fault:
retention is watched actually freeing a file, with the replica keeping up.

*A reconnecting replica is caught up across file boundaries*, which is #98's arithmetic end to end.
Asserted on the line `handle_catchup()` writes — `from_file=0 … through_file=3` — and not only on the
row count, because a row count is also satisfied by a snapshot, by a scan that stayed inside one
file, and by a replica that never lost anything.

**Two things the first version of that module got wrong, both worth keeping.** *How many WAL files
are on disk is a retention fact, not a rotation fact*: after fifteen hundred records the primary had
rotated three times and held `['wal_000002.bin', 'wal_000003.bin']`, because the replica had
confirmed into file 2 while the writing was still going on. And *a replica refuses `FLUSH`* — it is
read-only — so a flush interval long enough to stop retention also stops the replica's rows from
ever becoming queryable: the first version read **zero** rows out of a replica that had received
every one of them. What holds the files for the catch-up test instead is a third replica, stopped —
`ClusterManager.add_replica()` exists for it, and until now no test in this battery had two
replicas, so `safe_truncate` had never been a minimum over more than one element.

**What it found, and why the retention test stops where it does.** A pause long enough to observe
anything outlives the replica's socket timeout, so it reconnects; in that window it is not connected,
retention advances, and its saved position is refused with `ERR WAL_TRUNCATED` — correct, and now
documented. What happens next is **#125**: the snapshot bootstrap that refusal sends it to cannot
succeed. The test asserts the other half of the promise instead — the primary still holds every row
it acknowledged — and #125 owns the assertion that the replica comes back.

**Eight mutations, each with the verdict it was expected to give, and the first run of the table
found a defect in the table.** The control — a reworded log line, which must **survive** — came back
KILLED, and the restored tree was reported as not green. The cause was `shutil.copy2` in the
harness's restore: it preserves the mtime, so a source put back from the pristine copy is *older*
than the object file built from the mutant and the build rebuilds nothing. Every verdict after the
first restore had been measured against a binary still carrying an earlier mutation. `copyfile` plus
an explicit `utime` fixes it, and the reason to keep a control in every such table is exactly this:
one that dies is the only thing that tells you the instrument is broken rather than the code
diligent. After the fix, all eight — two joints of the flag's plumbing (the engine's argument, and
the CLI call site, which different suites catch), three refusals, retention ignoring what replicas
have confirmed, retention deleting nothing, and the control.

- Effort: S for the flag, M for the tests | Impact: three behaviours that only happen at a rotation
  were covered by unit tests that cannot express a reconnect or a retention pass. Two of them are
  now covered by a running cluster, and reaching the third found a defect that made a replica
  unrecoverable

### 123. The replica lag that is genuine ignored the WAL file index, so a replica a file behind read zero ✅

Found while fixing #118, in the number that item called "the lag that *is* real" — and left open
there deliberately, because publishing a number that reads zero in the case it exists for, inside
the change whose subject was a lag that read the wrong thing, would have been the same defect in a
new name.

`Engine::stats()` computed each replica's lag as `current_offset - r.confirmed_offset`. The part
#118 was about is fine: a replica streams **our** WAL and refreshes `confirmed_offset` on every
`ACK <file> <offset>`, so the two positions index the same log and the subtrahend is kept current.
What the expression did not do is look at `confirmed_file`. `rotate()` publishes
`{next_index, next_offset}`, so the current offset **resets** — and a replica still acknowledging
into the previous file has the larger number, the clamp answers **zero**, and the lag reads zero
exactly when a replica is more than a file behind.

**Measured, and the measurement is in the test rather than in this paragraph.**
`WalDistance.APositionLateInTheEarlierFileReadsAsZeroBehindTheOldWay` computes the old expression
beside the new answer on the same two positions and **asserts the old one says zero**. That
assertion earned its place immediately: the first version of the test took the position of the
*first* record in the earlier file, whose offset is 0 — the smallest possible subtrahend — so the
old expression answered 136 and the test proved nothing. The clamp needs a position **late** in the
earlier file, which is the mechanism stated as a value.

**The decision the item was left open for: what to report when a file in between is gone.**

The distance is computed **exactly**, by asking the filesystem for the size of each intervening
file, and not estimated from the rotation threshold. Estimating was tempting and is wrong in a way
that would have been invisible in production: closed files are *at least* the threshold, because
that is what rotation waits for, and a restart appends (`O_APPEND`, continuing from the highest
existing index) rather than starting a short one — so `files * threshold` is excellent at 512 MB
and out by a quarter at the 512-byte thresholds the tests use. A formula that is accurate only at
production settings is a formula no test can check. Exactness costs one `file_size` per
**intervening** file, and there are normally none.

When a file is missing the answer is **`nullopt`**, and it reaches an operator as two distinct
things rather than a number: `lag=unknown` in `STATUS`'s `[replicas]` block, and a separate gauge
`ob_replicas_lag_unknown` counting the replicas it happened to. The alternatives were both worse.
Zero is a **real answer** here — a replica that is caught up is zero bytes behind — so a zero
standing in for "cannot be measured" says the opposite of the truth, which is #123 restated. And a
guess would hide the more serious of the two conditions: retention keeps WAL files back to the
slowest connected replica, so **a missing file says that replica can no longer catch up from this
log and needs a snapshot**. That is worse than a large lag, and it deserves its own number rather
than being averaged into one.

`ob_replication_lag_bytes` is the max across replicas whose distance is known, published from
`publish_replica_gauges()` — recomputed once per loop pass over the replicas that exist, rather
than maintained wherever a lag changes, which is #94's lesson and the reason `ob_replicas_connected`
is counted there too. `Engine::stats()` and that publisher both call one
`WALWriter::bytes_since()`: two ways of computing one quantity is how #118 produced a lag that was
not one.

**What was not covered here, and is now.** When this item closed, no integration test crossed a WAL
file boundary: the rotation threshold was a literal in `src/engine.cpp` and a real node would have
had to write 512 MB. That was filed as **#124** rather than worked around — making production code
configurable so a test can reach it is a test deciding the shape of the program, and the flag was
worth adding on its own merits or not at all. It was, and it is in; this number's cross-file
behaviour is pinned by a running cluster, which found that a lag bigger than a whole file is
reported as one.

**Ten mutations, each with the verdict it was expected to give — and the table paid for itself
twice before it got there.** Two of the ten survived the first run against real gaps in the tests,
not against the code. *"The current file's bytes are dropped"* survived because forty 136-byte
records against a 512-byte threshold rotate on the last one, so that test ended with
`now.offset == 0` and dropping it changed nothing; the test asserts the current file is non-empty
now, as a stated precondition. And *"a missing file is guessed at instead of unknown"* survived
because the only test for it removed an **intervening** file, so the error path for the file the
position itself sits in had no test at all — which is now `AMissingFirstFileIsUnknownToo`.

The deliberate survivor is *"stats reports every lag as known"*: the formatter's half is pinned by
a test that builds `ServerStats` directly, and nothing behavioural covers `stats()` wiring the flag,
because that needs a replica whose confirmed file has been removed. Same shape as #118's survivor,
recorded for the same reason — a surviving mutation without a note is one the next reader assumes
was missed.

- Effort: S | Impact: the only genuine replication lag in the engine read zero in the case an
  operator cares about, and it was visible only by reading `STATUS` by hand. It is now two gauges,
  and the second one names a condition the first cannot express
### 122. The replication client's pointer was written without the lock every reader holds ✅

Found while giving `ob_repl_records_replayed` a publisher (#117), because the number to publish
lives behind that pointer — and then **confirmed by TSan in the same PR**, because that publisher
is what made it fire.

`Engine::stats()` reads `repl_client_` with `mtx_` held. `Engine::promote_to_primary()` released
`mtx_` and *then* wrote the member:

```cpp
std::unique_lock<std::mutex> lock(mtx_);
if (repl_client_) {
    lock.unlock();
    repl_client_->stop();
    repl_client_.reset();      // the member is written here, unlocked
    lock.lock();
}
```

The unlock is not the defect and has to stay: `stop()` joins the client's threads, and holding
`mtx_` across a join the joined thread may need is #79's deadlock. What was wrong is only *when
the member is written*.

**The fix is the idiom already in this file, ten lines further down.** `demote_to_replica()` takes
ownership **under** the lock and destroys through a local, with a comment saying "for the reason
above" — about a reason this site was breaking:

```cpp
if (std::unique_ptr<ReplicationClient> client = std::move(repl_client_)) {
    lock.unlock();
    client->stop();
    client.reset();
    lock.lock();
}
```

The member write happens with the lock held; the join and the destruction happen on a local no
other thread can reach. The ordering `promote_to_primary()` depends on further down is unchanged —
`stop()` and the destructor still run before `discard_saved_replication_position()`, which matters
because `stop()` ends with `save_state()`.

**How it was found is the part worth keeping.** Reading the code said the reader was disciplined
and the writer was not, which is the shape #41, #49 and #83 all were. It was filed rather than
fixed on the argument that a change about metrics is the wrong place to touch a failover path —
and then `sanitizers-integration (tsan)` went red on that very PR, naming
`std::__uniq_ptr_impl<ob::ReplicationClient>::reset` and, separately, `operator delete`. Two things
follow. The race is a **use-after-free window** rather than a torn pointer read, which reading it
had not established. And the new publisher is what made it reachable: `stats()` runs on a `STATUS`
command or a scrape, while the flush tick runs every interval — so the frequency changed by orders
of magnitude even though the new reader took the correct lock.

Which retires the argument for deferring it. **"Filed rather than fixed" stops being available
once your own change makes a defect reachable**, and a PR that leaves a required check red is not
a PR.

**Measured on both sides by the same job, which is the cleanest form this kind of claim gets.**
Before, on the commit that added the publisher and filed the defect: `sanitizers-integration (tsan)`
red, naming `std::__uniq_ptr_impl<ob::ReplicationClient>::reset` and `operator delete`. After, on
the commit that carries this line: **256 integration tests passed under TSan in 24:37 with zero
data-race reports**, and 256 passed in 18:58 uninstrumented. Same runner, same job, one commit
apart.

- Effort: S | Impact: a use-after-free window on the ordinary promotion path, with `STATUS` and
  every `/metrics` scrape as the reader. Present since the replica path existed; reported the first
  time anything read that pointer often enough

### 121. Nothing bounds how far a peer's clock can move ours, and the move is permanent ✅

Named while fixing #119 and left open on purpose, because the honest answer was a decision. Taken
now: **the mesh refuses a peer whose physical clock is more than five minutes ahead of this node's
wall clock.** Not the record, not the clock, and not a clamp — and the three it is not are the
content of the decision.

`tick_receive()` stores `max({now, last_, remote})`, `src/multi_master.cpp` passed the frame's
timestamp to it unfiltered, and no code path lowers the physical component again. The right way to
say what that means is not "a peer can move our clock" but **the mesh's clock is the maximum of its
members' clocks, and it stays there for as long as that member keeps writing.** Nothing bounded the
maximum.

**First, the blast radius, measured rather than assumed — and it is smaller than the item was
filed with.** The filed text said `apply_delta_mm()` stamps our writes from that clock so the value
goes out on our records. It stamps the **record's HLC**; a row's `timestamp_ns` comes from
`Engine::stamp_for()`, which reads `system_clock` directly, or from the client since #105. So a
drifted clock does **not** corrupt row timestamps, TTL retention or time-range queries. It reaches
exactly two things: the order of records, and LWW's choice between concurrent writes. That
narrowing is what made the rest of the decision tractable, and it took reading one line
(`row.timestamp_ns = delta.timestamp_ns`) rather than reasoning about the design.

**Then the tail, which is the reason a bound had to exist at all rather than being a matter of
taste.** `resolve_logical()` carries a logical overflow into the physical component and its comment
said `physical` "cannot be UINT64_MAX here in any reachable state … If one ever arrives the
increment saturates the type rather than wrapping to zero, which is the direction that keeps this
function's promise." **That last clause is false.** At `{UINT64_MAX, 65535}` the next tick wants
65536, leaves `physical` alone and returns 0 — so `{UINT64_MAX, 0}` follows `{UINT64_MAX, 65535}`,
the clock goes **backwards by the whole counter**, which is the one property #119 exists to prevent,
and then oscillates there for ever. Unreachable from any real clock; reachable from a peer, because
nothing bounded what a peer could say. Saturating is the least-bad thing to do once you are in that
state. The fix is to make the state unreachable, and a bound at the door does that.

**Why the peer and not the record, the clock or a clamp.** Four answers were available and only two
of them leave every participating node holding the *same* clock, which is the property a
multi-master mesh cannot trade:

- **Clamp what we absorb.** The obvious answer, and it is the worst one. The verdict is taken
  per node against *that node's* wall clock, so two nodes clamp differently, stamp their later
  writes differently, and resolve the same LWW conflict differently — `ConflictResolver::resolve()`
  compares the record's HLC against the stored one, so the divergence lands in the **data**. An
  untrue clock beats divergent values, and it is not close.
- **Refuse the record, keep the link.** The write is lost while the peer believes it replicated: a
  silent hole, and the worst outcome on any list this repository keeps.
- **Absorb anything.** What the item measured. Correct today, and one misconfigured node owns the
  cluster's clock for as long as it runs.
- **Refuse the peer.** Every node still in the mesh shares one clock; the condition is visible on
  both sides (`MM_PEERS` keeps what the peer claimed, a counter moves, one line says what to do);
  and it is reversible — fix the clock and the link returns. Taken.

The verdict is against the **wall clock**, deliberately, not against our HLC. Every healthy node
agrees about wall time to within its own skew, so every healthy node reaches the same verdict about
the same peer, and the outcome is stable rather than a race between who absorbed first.

**Five minutes, and it is a judgement rather than a measurement — said so rather than dressed up.**
What the bound separates is not two magnitudes but two *kinds* of clock: one that is merely
unsynchronised, which lands in milliseconds under working NTP and in seconds after a VM suspend or a
long pause, and one that is **wrong** — a hand-set date, a dead RTC, a host that never had NTP. The
first heals as wall time catches up; the second does not, for the reason at the top of this entry.
Four orders of magnitude above the first class and far below the second is the whole of the choice,
and the exact value is not load-bearing: what is load-bearing is that a bound exists. A **constant
rather than a flag**, because the only thing an operator could tune is how badly a peer's clock may
lie, and tuning it *tighter* would start dropping peers that are merely unsynchronised — which is
the cost this decision exists to refuse. The mechanism is there if a user ever asks; a flag added
now would be a knob nothing turns, and one of those was deleted two items ago.

**What this deliberately does not buy: a clock that means wall time.** A peer four minutes ahead is
still absorbed and still pins the mesh four minutes ahead. That is accepted, because rows carry
their own time and because refusing at four minutes would cost data for a clock that is merely
unsynchronised. `ob_mm_hlc_drift_ns` is still a peak that never falls, and still the thing to alarm
on.

**And what it costs, asserted rather than described.** The refused peer's data does not arrive.
`MmWireClock.AnHourInTheFutureOnTheWireIsRefusedAndTheClockDoesNotMove` requires the record to be
absent from storage as well as absent from the clock — a test that checked only the clock would let
the next reader believe the data came too.

**The clock class is still uncapped, and that is the decision rather than the absence of one.** A
clock that silently declined part of what it was told would break the invariant it exists for: if
we accept a record we must stamp later writes above it, or a causally later write can lose an LWW
conflict to the record it followed. So the layer that says no must be the layer that can also
decline the record — and, since a record refused under a live link is a silent hole, the layer that
can decline the peer. `tests/test_hlc_skew.cpp` still requires the class to absorb an hour handed
to it directly, with the reason written beside it.

**No integration test, and the reason is the instrument rather than the effort.** Producing a real
node whose clock is five minutes off needs the host clock moved or a time namespace, which is not
something this battery can do to the machine it runs on. The wire test is the right instrument and
was already built for #54's stage D: a fake peer that frames one DELTA whose HLC says what no real
clock would. That is also what makes this measurable at all — the engine's own clock is never
skewed, only what arrives on the wire.

Five copies of one fact came out of this: `wall_clock_ns()` existed as a private static in
`HybridLogicalClock`, a free function in `src/failover.cpp`, and one in each of two test files —
written two different ways, `clock_gettime(CLOCK_REALTIME)` in two and `system_clock::now()` in the
other two. They agree on Linux, so this was never a defect; it was the shape that produces one
(#118 had two fields named after the same quantity and neither held it). #121 needed the wall clock
in a third production place, so instead of a fifth copy there is `include/orderbook/wall_clock.hpp`
and one definition. `Engine::stamp_for()` keeps its own expression on purpose: it answers a
different question — *whose* time a row carries — and #105 gave that one function of its own for the
same reason this one exists.

**The mutation table says one thing the tests do not say out loud: the policy number is pinned from
both sides without being pinned to a literal.** Eleven rows, each with the verdict it is meant to
give. **Eight are killed by a test** — the bound removed, moved by one nanosecond, the behind-us
clause dropped, the verdict taken against our own HLC, the peer refused but not dropped, the check
moved after dispatch, the counter not incremented, and the peer's claim stored only after the
verdict. **One is refused by the compiler.** And **two are controls that survive**: the WARN
reworded, and the bound widened from five minutes to five and a half.

That last one is the row worth having. Every expectation in both test files is written in terms of
`MM_MAX_CLOCK_SKEW_NS` rather than against a literal, deliberately — pinning the number itself would
fail on any legitimate retuning and teach a reader that the test is noise. What the fixtures do
instead is bound the constant from **both** sides: `tests/test_mm_wire_clock.cpp` carries a
`static_assert` requiring `4 min < bound < 6 min`, because its two skews have to straddle it or the
test asserts nothing, and two runtime guards in the same file require `1 min < bound < 1 h`, each
with its reason beside it; `tests/test_hlc_skew.cpp` puts it under a year. So a widening large enough
to change an answer **does not compile**, and one small enough to compile changes nothing anybody can
observe. The row that establishes the first half is `bound_widened_to_an_hour`, and its verdict is
DID-NOT-BUILD — a kill by the tightest mechanism available rather than an escape.

**Two rows had to be re-expressed rather than have the code changed for them**, which is this
repository's rule and was applied twice here. `return true;` leaves both parameters unused under
`-Werror=unused-parameter`, so the bound-removed row is a tautology that still reads them
(`remote_physical_ns >= local_wall_ns || remote_physical_ns < local_wall_ns`). And the
claim-stored-late row first introduced a local nothing read
(`-Werror=unused-but-set-variable`); it now leaves the verdict reading the true remote value — so the
refusal behaviour is untouched — and moves only *when* the peer's claim is stored, which leaves a
refused peer's `last_hlc` at zero. That is what makes it a kill rather than a restatement of the
refusal rows: what it breaks is the assertion keeping `MM_PEERS` showing what a refused peer claimed,
and since that assertion sits inside an `if` over the peer record, the row also establishes that the
record survives the drop and the `if` is entered.

- Effort: M, most of it the decision | Impact: one misconfigured node's clock was the whole mesh's
  clock, permanently, and a value no real clock produces could push the physical component into the
  one state where this clock runs backwards

### 118. Two mesh fields are named after a replication lag, neither is one, and the lag that is real is published nowhere ✅


Found while planning #117: the gauge nothing feeds turned out to have two siblings that something
does feed, carrying numbers that cannot mean what they are called.

`Engine::stats()` computes a per-peer mesh lag as `wal_.current_offset() - peer.confirmed_offset`,
and `STATUS` prints it as `replication_lag_peer_<id>`. `include/orderbook/multi_master.hpp` says
what those fields are, in the file that declares them:

> Reported by the peer in its handshake, kept for the MM_PEERS view only. Catch-up must not use
> them: they are positions in the peer's own WAL, and #61 was the consequence of comparing them
> with ours.

Two facts make the printed number meaningless rather than approximate. The offsets count
**different bytes** — every mesh node writes its own client writes as well as everything it
replicates — and the subtrahend is **written once**, in `process_handshake()`, which is the only
place in the tree that assigns it for a mesh peer. It is frozen at connect time.

**Measured** (`scripts/measure_mesh_lag.py`, i3-7100U, two-node mesh, native etcd, 120 writes per
phase, convergence established by row content rather than by a sleep):

| phase | rows node 0 | rows node 1 | node 0 WAL offset | `replication_lag_peer_2` | implied peer offset | `MM_PEERS` `lag_bytes` |
|-------|-------------|-------------|-------------------|--------------------------|---------------------|------------------------|
| 1 | 120 | 120 | 20 392 | **20 392** | **0** | 0 |
| 2 | 240 | 240 | 40 692 | **40 692** | **0** | 0 |
| 3 | 360 | 360 | 60 992 | **60 992** | **0** | 0 |

The reported lag equals this node's own WAL offset **to the byte**, in every phase, on a mesh
converged by content. The "implied peer offset" column is arithmetic on the two beside it rather
than a fourth measurement — the engine computes the lag as `ours - peer.confirmed_offset`, so
`ours - lag` is the position it believes that peer holds — and it is the decisive one: **zero,
while that peer's own WAL is 60 992 bytes long and holds all 360 rows.** Two numbers being equal
can be a coincidence of similar magnitudes; a node believing a fully caught-up peer sits at byte
zero cannot.

The absolute offsets move by 92 bytes between runs depending on whether an epoch record was
written, which is why the claim here is the **equality and the zero**, not the byte counts. The
expression also clamps at zero, so the number would fall back to zero when **our** WAL rotates.

**`MM_PEERS` carries a second field with the same name and a different wrong answer.** Its
`lag_bytes` column is `peer.send_buf.size()` — this node's own outbound queue, which #117 measured
as staying at zero until the *sender's* socket buffer fills, 4 MB on this machine. So the two
numbers an operator can read disagree with each other: one grows without bound on a healthy mesh,
the other stays flat through four megabytes of writes that have not left.

**The contrast is the lesson, and it is in the same function.** The identical expression for a
*replica* — `current_offset - r.confirmed_offset` — is correct, because a replica streams **our**
WAL and its `confirmed_offset` is refreshed on every `ACK <file> <offset>`. Same arithmetic, same
word in the output, one of them valid. So the rule cannot be "subtract the confirmed offset": a
difference of positions is a lag only when both index the same log **and** the subtrahend is kept
current. This is #79's shape — `shutdown()` and `take_result()` had the same shape and one of them
deadlocked — so the rule has to be unconditional rather than a note beside one call site.

**And the registry has it exactly the wrong way round.** `src/metrics.cpp` holds one metric with
"lag" in its name: `ob_mm_replication_lag_bytes`, the mesh one, which nothing writes (#117). The lag
that *is* real — per replica, live, already computed — reaches an operator only by reading `STATUS`
by hand, and a number a human has to read cannot be alerted on. That is #94's lesson about
`ob_replicas_connected`, which is the reason that gauge exists at all.

**Two client documents state the wrong thing as a consequence.** `docs/python.md` documents
`mm_peers()[i]["lag_bytes"]` as "replication lag in bytes", and `docs/cli.md` lists the column
without saying what it holds. The same list documents `status` as `"active"`, `"joining"` or
`"leaving"`, which is the **peer registry's** vocabulary (`include/orderbook/peer_registry.hpp`);
the column emits `connected` or `disconnected`, so a client testing for `"active"` is testing for a
value the server never sends.

The fix has three parts and none of them is "feed the gauge": stop reporting a byte lag for mesh
peers, publish the replica lag that is genuine, and give the mesh the lag it can state honestly.
`compare_vectors()` already returns `VectorDiff::peer_lacks` as (symbol, origin, from_seq, to_seq),
so the honest mesh answer is in **records** — which is what requirement 1.4 of #54 asks for, and
what #117's plan line asked for in bytes and could not have had.

**What the fix turned out to be, and it is not "feed the gauge".**

`STATUS`'s `replication_lag_peer_<id>` lines are **removed**, not re-pointed at the honest number:
that number is in a different unit from a different mechanism at a different freshness, so keeping
the field would have left every existing reader parsing the same line about a different subject.
The field disappeared from both structs rather than being zeroed, which is how the compiler found
the third copy site in `tcp_server.cpp` that a grep would have missed.

`MM_PEERS`' column is `send_queue_bytes` now. The value never changed — `peer.send_buf.size()` —
and only the name was wrong, which was the whole problem: it invited the alert the word "lag"
implies on a number that is zero precisely when a node has accepted writes it has not handed to
the kernel yet. The Python client builds its dicts from the answer's own header, so a client
written against the old key gets a `KeyError` rather than a silently different number.

**The mesh's honest lag is `ob_mm_replication_lag_records`, and it is a pair.** Records because
sequence numbers are per-origin and origin-stamped — the mechanism #61's fix introduced, and the
one position two nodes can compare. `AntiEntropyManager` already had the answer: its pass calls
`compare_vectors()` per peer and holds gaps carrying `from_seq` and `to_seq`. The second gauge,
`ob_mm_peers_position_unknown`, is not decoration: `compare_vectors()` reports a peer that has said
nothing as holding **nothing**, deliberately, because sending it everything is the safe direction
for a repair — so counting that as lag would make silence the largest lag in the mesh. Those peers
are excluded and counted separately, and the honest reading is the pair. Zero lag with a nonzero
unknown count means "we do not know", which is #84's defect again if the two share one number.

`ob_mm_replication_lag_bytes` is **removed from the registry** rather than fed, so #117's allowlist
is now empty and the checker's stale-allowlist branch got its first real exercise.

**Measured on the probe that found the defect** (`scripts/measure_mesh_lag.py`, two-node mesh,
convergence by row content): `lag_records` **0** with `unknown` **0** in all three phases, beside a
`wal 0` column reading 20 300 / 40 692 / 61 084 — the numbers the old field carried. And the
partitioned case is the half a converged mesh cannot show, so it is an integration test rather than
a probe reading: **it closes the second clause of #54's C3**, which stage C recorded as having no
passing test *because of #117*. Six writes across a partitioned link, the lag goes above zero with
the unknown count still zero, and it returns to zero on heal — three readings, the first of which
is the control, because a gauge only ever observed as zero is what #117 was about.

**Nine mutations, each with the verdict it was expected to give**, tree restored byte-identically.
Two of them are the interesting ones. The deliberate survivor is *"the reconciler never reports an
unknown position"*: the exclusion arithmetic is pinned by unit tests that pass an `unknown` list
directly, but nothing behavioural watches the reconciler **fill** that list, because a peer with no
vector exists only in the window between handshake and the first exchange and no test can land on
it deterministically. Recorded rather than dropped — a surviving mutation without a note is one the
next reader assumes was missed. And *"the records lag is always zero"* was run a second time
against the **integration** test, which failed in 1:12: that is what says the test closing C3 is
load-bearing rather than a test of a gauge that happens to read zero.

One mutation did not build at first, because removing the exclusion left a parameter unused under
`-Werror`. The **mutation** was transformed, not the code.

- Effort: S to stop reporting the two wrong numbers, M for the honest one | Impact: an operator
  watching a converged mesh read a lag that grew all day, a client reading `peer["lag_bytes"]` read
  a queue depth under a different name, and `peer["status"] == "active"` was never true. The
  replica lag is **still** unpublished and that is #123, not an omission: it ignores the WAL file
  index, so publishing it here would have repeated this defect in a new metric

### 117. Five metrics are registered and fed by nothing, and one of them is the only signal a partitioned mesh could give ✅

Four of the five are fed now. The fifth is `ob_mm_replication_lag_bytes`, and it is **not** fed
because #118 measured that it cannot be: the only per-peer position the mesh has is a byte offset
into the peer's *own* WAL, frozen at handshake. Its allowlist entry moved from this item to that
one and is waiting to be **removed** rather than filled — the honest mesh answer is in records,
which is a different metric with a different name.

**`ob_wal_records_written` had nowhere to be counted, so the counter went where the count was
already being lost.** Both write paths ended with the same five lines — advance the position,
increment the pending-sync count — identical down to their comment. That is #94's shape: a quantity
maintained at N call sites is a quantity the N+1st call site will not maintain. The two copies are
now one `advance_after_write()`, so a third record format gets the counter by calling it or does
not compile. A mutation confirms the placement is load-bearing rather than tidy: giving the v2 path
its own copy without the counter kills the test that walks every record type and leaves the
rotation test green.

It counts **every** record type, which is the description's own claim and what makes it worth
having beside `ob_inserts_total` instead of duplicating it: the difference is the bookkeeping the
WAL does on its own behalf. That is measurable and was measured by getting it wrong — the rotation
test asserted forty records for forty appends and read **fifty**, because each rotation writes a
`ROTATE` record of its own through the same function. The test asserts appends plus rotations now,
and the description says so.

**Cost on the write path, measured rather than argued, because `append` is on it**
(`scripts/mnemonic_diff.py`, Release, i3-7100U):

| function | base | head |
|----------|------|------|
| `WALWriter::write_record` | 220 | **223** |
| `WALWriter::write_record_v2` | 207 | **210** |
| `WALWriter::append` | 90 | 90 (identical) |
| `Engine::apply_delta_mm` | 602 | 602 (same instructions, 35 shifted operands) |

**+3 instructions per record** — a relaxed load, an add, a relaxed store — and
**`lock`-prefixed instructions in the whole WAL archive: 19 before, 19 after**, so the counter adds
none. Relaxed load-and-store rather than `fetch_add` for exactly that reason, which is the argument
`position_` beside it already makes.

The first measurement said something else and saying it would have been wrong: out of line,
`advance_after_write()` became a real 16-instruction call and `write_record` read **212**, eight
*fewer* than base. That drop is relocation, not saving — the tool's own documentation warns about
it (`apply_delta` reads 503 → 3 because the body moved to `apply_delta_impl`) — and the true cost
was +8. Defining it in the header took the call away and left the +3 that is the counter itself.

**`ob_repl_records_replayed` needed no counter — the number has existed all along** in
`ReplicationClient`, and `STATUS` has been printing it as `replayed=` while `/metrics` reported a
flat zero. What it needed was a publisher, and writing that found the thing worth remembering:
**`repl_client_` is rebuilt on every role change**, so its total restarts at zero. The
`total - published` form used for the fsync counter is correct only because `WALWriter` and
`HybridLogicalClock` live as long as the engine; applied here it freezes the metric from the
restart until the new client passes the old total — which is precisely the window an operator is
watching, a node that has just become a replica and is catching up. All three totals now go
through one `counter_delta()`, and that function is in `metrics.hpp` so a unit test can state the
restart case without a cluster. Reading the pointer at all turned out to need care, and that is
**#122**.

**The io_uring pair cost the most to make honest, because nothing runs that transport.** Since #108
a CI job compiles `src/io_uring_server.cpp` and checks the symbol reached the binary; that job's
own message says compiling is all it does. Three things were wrong in six lines of its loop:

- `io_uring_submit()`'s **return value was discarded**. It is the number of entries submitted, and
  a negative value means they stayed in the ring — the transport stops making progress and says
  nothing. #113's shape in a different file.
- `ob_iouring_sqe_submitted` was fed the **completion** count, making it equal to
  `ob_iouring_cqe_processed` by construction, so an operator comparing the two to find a backlog
  compared a number with itself. A metric fed the wrong number is worse than one fed nothing:
  a flat zero is visibly broken.
- `ob_iouring_cq_overflows` had nothing to be fed from that is an event.
  `io_uring_cq_has_overflow()` is a **flag the kernel leaves raised**, so a counter driven by it
  once per pass would count this loop's speed. It counts **episodes** now, through the
  `LogEpisode` #120 extracted — the third user, which is what turns a pattern into a mechanism —
  and the log says it twice per excursion rather than once per pass.

`ob_iouring_sq_utilization` is sampled **before** submitting, because submitting empties the queue
and a sample taken afterwards reads zero whatever the handlers queued. Its denominator is
`ready + space_left`, the ring's real capacity, not the configured depth: the kernel rounds the
request up to a power of two, so the configured number is the wrong denominator.

**Two answers to "nothing runs this file", and both are used.** The arithmetic moved into
`metrics.hpp` as `queue_utilization_percent()`, where the ordinary suite builds *and executes* it —
saturation, truncation and a zero capacity are four tests rather than a comment. What cannot move,
which expression is handed to which counter, is asserted against the source text in
`tests/test_iouring_instrumentation.cpp`, a file deliberately **not** behind `OB_USE_IO_URING`
because its whole purpose is to check a file this build does not compile. That is weaker than
running it and it is the strongest thing available; four mutations restoring each defect are killed
by it.

`ob_iouring_submit_errors_total` is new, registered in the same change that writes it. A log line
would have been the other option and a worse one: this one repeats while the condition holds, and
the count is the half that can be alerted on.

- Effort: S for the WAL and replication counters, M for the mesh gauge — and the M turned out to be
  **not our work**: it is #118, and the answer there is a different metric rather than a value for
  this one. The rest was S. | Impact: an operator watching WAL growth, replica catch-up or io_uring
  backpressure watched constants; the io_uring submission counter was worse than a constant,
  because it looked like a second opinion and was a copy of the first

### 116. A node whose coordinator is unreachable writes two WARN lines a second for ever, and one of them names a consequence that did not happen ✅

Found by #54 stage B, which set out to check refusals and had to read the logs to do it.

**Measured, default log level, a 30-second etcd outage on a two-node cluster (i3-7100U):**

| Node | Lines | Per second | Of which the two repeating WARNs |
|------|-------|-----------|----------------------------------|
| the holder | 65 | **2.17** | 60 |
| the replica | 67 | **2.23** | 60 |

`publish_position_if_due()` rate-limits itself to once a second. Both of its failure paths log at
**WARN** — `ensure_position_lease()` when the grant fails, and the publish itself — while its
success path logs at **DEBUG**. So a healthy node is silent about publishing its position and a
node that cannot reach the coordinator says the same two sentences every second until the outage
ends. That is #95's shape (pitfall 144): a permanent failure retried at loop frequency and logged
at loop frequency. Over an hour-long etcd outage it is about seven thousand lines carrying one
fact.

**And the wording of one of them is wrong in the case that produces it.** The line reads
*"could not grant a lease for the published position — publishing without one, so this position
will outlive this node and other nodes may defer to it after it dies"*, and the very next line is
`publish_wal_position failed`. Nothing was published, so nothing will outlive anything: the
sentence describes a consequence of a publish that did not occur, and sends an operator after a
data-integrity problem that is not there. It is pitfall 112's shape — a log line announcing
something the code did not do — with the polarity reversed, announcing a harm instead of a
guarantee.

**A third thing, reachable by reading and deliberately not claimed as measured.** When
`grant_lease()` fails the code publishes with `lease = 0` anyway. With the coordinator unreachable
that publish fails too, which is why the outage above is harmless. But if a coordinator refuses
lease grants while still accepting puts — a lease quota, an overloaded server, auth on the Lease
API — the position is written **without a lease and never expires**, and a dead node's position is
deferred to for ever. That is what #72 closed by putting positions under leases. **No run has been
observed in which grant fails and put succeeds**; the distinction between "reachable by reading"
and "measured" is the point of saying so.

The fix shape already exists in this tree: #113's flush loop logs the first failure of an episode
loudly, the rest at DEBUG, and a line when it recovers. Applied here that is one WARN when the
coordinator stops answering, silence while it stays that way, and one line when it comes back.

**Done. Measured before and after, and the rate is the comparable figure because the windows
differ (30 s before, 20 s after):**

| | Before | After |
|---|---|---|
| lines per second, per node | 2.17 and 2.23 | **0.40 and 0.45** |
| lines in the window | 65 and 67 | 8 and 9 |

A `LogEpisode` in `failover.hpp` gives each condition one line when it starts and one when it ends,
with the number of ticks it lasted — the shape #113's flush loop already used. **The idea was
already in this file, applied to one of the three places that needed it:** `standalone_polls_` was
written for exactly this and guards the STANDALONE branch alone, which is pitfall 172 — a rule
applied by hand is applied once too few.

**The misleading sentence moved rather than being reworded.** "publishing without one, so this
position will outlive this node and other nodes may defer to it after it dies" was on the *lease
grant failure* path, where the very next line said the publish had failed — nothing was written, so
nothing outlived anything. That harm happens only when the publish **succeeds** without a lease, so
the WARN is now on that tick, naming #72, which is the item that put positions under leases. The
third concern from this item's filing — a coordinator that refuses grants while accepting puts — is
therefore now *reported when it happens* rather than guessed at: still not measured, but no longer
silent.

- Effort: S | Impact: the log an operator reads during a coordinator outage went from ~92% two
  repeated sentences to one line per condition, and the one warning that was false where it stood
  is now true where it stands

### 115. The one decision an unreachable coordinator makes invisible is the decision that answers the operator's question ✅

Also found by #54 stage B, and it is the counterpart to #116: not that there is too little logging
during an outage, but that the part which is missing is the part being asked for.

A replica that cannot read the leader key **deliberately does not campaign** — that is #82's fix,
and the whole reason the coordinator's answer has three states instead of two. It is logged with
`OB_LOG_DEBUG`, and the server's default level is **INFO** (`TcpServerConfig::log_level`). So the
decision never reaches an operator's log.

**Measured on the same 30-second outage:** across 65 and 67 lines on the two nodes, the count of
lines mentioning the campaign decision is **0** and **0**.

What makes this a defect rather than a preference is the contrast on the *same* node in the *same*
window. The holder's step-down is logged **four times, once each, at WARN and INFO, naming the
mechanism**: `refresh_lease failed for lease=…`, `lease lost, demoting to REPLICA`, `no new primary
is published yet — demoting anyway`, `demoted to REPLICA`. So this is not a general gap in the
logging of coordinator faults. It is one decision, and it happens to be the one that answers the
question an operator asks first: *etcd is down and nothing has failed over — is that the engine
deciding, or the engine broken?* Today the honest answer is in the source, not in the log.

Per-tick INFO would be #116 again, so the shape is the same as the fix there: say it once when the
node starts declining, and once when it stops.

**Not a defect, recorded because #54 stage B's test was written against the wrong branch first.**
The holder does **not** wait out a TTL when the coordinator vanishes: its lease keepalive fails on
the next tick and it demotes immediately. Measured: **2.28 s** from `stop_etcd()` to the node no
longer answering `ROLE` with `PRIMARY`. The "could not confirm the leader key names us for N s"
branch is for a different fault — a coordinator that answers keepalives but not reads.

**Done, and the count is exact rather than "at least one".** The refusal is `OB_LOG_INFO` on the
tick the episode opens and `OB_LOG_DEBUG` on every tick after, so a 20-second outage produces
**one** line about it where a 30-second outage produced **zero**. The episode ends on *any* answer
from the coordinator, not only on a leader being present: a confirmed absence is information too,
and ending it only on `Present` would leave a node recovering into a genuine election reporting the
refusal for ever — an episode counter nobody resets is a flag nobody clears.

**The test for this found a defect in its own check, which is the part worth keeping.** The obvious
phrase to count, `staying REPLICA rather than campaigning`, is in the INFO line *and* in the
per-tick DEBUG line beside it — so a mutation rewording the INFO line **survived**, satisfied by
the DEBUG one, while this item is entirely about the level. The counted phrase is now a clause
belonging to the INFO line alone. A check a cross-reference satisfies is worse than no check,
because the next reader trusts it.

And what the fix did **not** change, which is the control: the holder still gives the role up
**2.28 s** after the coordinator vanishes, the same figure as before. This item touched what the
engine says, not what it does.

**A consequence outside the engine.** #54 stage B had added `ClusterManager(log_level=...)` so a
test could see the DEBUG line. With the line at INFO the module runs at the server's own default —
a stronger statement, because it now asserts on what an operator gets — and the parameter became a
knob nothing turns, so it is gone. Pitfall 31 running backwards: a workaround in the harness was a
bug report, and fixing the bug deleted the workaround.

- Effort: S | Impact: during a coordinator outage the engine's correct refusal to fail over is now
  the first thing the log says about it, instead of being absent from it

### 114. An `MmapStore` with tests and no caller, described by three documents as the storage path ✅

Found while #54's fault injector went looking for an mmap to fail, and found none.

**Measured:** `mmap(` appears in all of `src/` only inside `src/mmap_store.cpp`. `MmapStore` is
constructed only in `tests/test_mmap_store.cpp` — nothing in `src/`, `include/` or `tools/` uses it,
beyond an `#include "orderbook/mmap_store.hpp"` in `columnar_store.hpp` that uses no name from it.
`ColumnarStore` writes every `.col` with `std::ofstream` and reads with `std::ifstream`. A segment
is a directory `<symbol>/<exchange>/<start_ns>_<end_ns>/` holding seven column files —
`price`, `qty`, `cnt`, `ts`, `side`, `level`, `seq` — and a `meta.json`.

So `orderbook_mmap` is a compiled library with a test suite and no production caller, which is the
shape of #104 at component scale rather than field scale.

**Three documents described it as how this engine stores data**, and they are corrected in the same
change that files this, because a claim about the code is not a roadmap item:

- `README.md` listed "MMAP persistence with segment-based time partitioning" as a feature
- `docs/architecture.md` drew the columnar store as "segments on disk via MMAP"
- `docs/storage.md` had a whole **MMAP Store** section claiming column files are memory-mapped with
  `mmap(MAP_SHARED)`, extended with "`ftruncate` + `mremap`" and synced with `msync(MS_SYNC)` — and
  even that is not what the unused component does, since it remaps with `munmap` + `ftruncate` +
  `mmap` and never calls `mremap`

The same document also named WAL files `wal_NNNN.wal`; they are `wal_%06u.bin`, which the injector's
own decision log printed while nobody was looking for it.

**Decided: it goes. And the deciding fact is the failure mode, not the benchmark the item
expected to need.**

The question looked like "is mapping faster than `std::ofstream` for a write-once sequential file",
which needs a measurement. It is not that question, because of what a *growable* mapping does when
the filesystem fills. `ftruncate` extends a file **sparsely** and allocates no blocks, so it
succeeds; the allocation happens when the page is first touched, and there is no return value
there. **Measured on an 8 MB tmpfs**: reserving 64 MB succeeded, leaving a file of 67 108 864
apparent bytes with 8 MB allocated, and writing into it died with `Bus error` — **exit 135**. The
control, reserving 2 MB on the same filesystem, completed normally.

A signal is not an exception. `run_thread_body()` cannot catch it, no `ERR` can carry it, and the
node is gone. #112 and #113 spent this week making a failing disk into a refusal the client is told
about; adopting this component on the segment write path would have reinstated **process death for
a full disk**, in a form strictly harder to handle than the `ENOSPC` those two items closed. That
is not a trade a benchmark can win.

**Three more defects, measured with #54's injector making `ftruncate` fail, and they answer this
spec's own task A4.1.** With the `ftruncate` inside `open()` skipped so the one inside `remap()`
fails:

- `size()` still reports **4000** bytes written into a mapping that no longer exists
- `write_ptr()` returns **`0xfa0`** — `nullptr + 4000`, a pointer computed from a null base, so a
  caller that catches the exception and retries writes to address 4000
- the next `advance()` **never returns**: `remap()` zeroes `mapped_size_`, and the growth loop is
  `new_size = mapped_size_ * 2` followed by `while (cur + bytes > new_size) new_size *= 2`, which
  doubles zero for ever. Twenty-second timeout, exit 124

So the component was not merely uncalled: its only growth path leaves the object unusable in three
ways, and the test suite that was "keeping it honest" never reached that path. **A test suite that
passes on a component nothing calls says what the component does on the paths somebody thought
about.**

**What the deletion does not foreclose, because the distinction matters.** This class is an
**appender** (`write_ptr()`, `advance()`, `msync`) — not a reader. Three of the seven column files
(`ts.col` uint64, `cnt.col` uint32, `side.col` uint8) are raw fixed-width arrays read straight into
vectors, so a mapped *reader* could skip a copy and an allocation for those; `price.col`, `qty.col`
and `seq.col` are delta+zigzag and Simple8b encoded and must be decoded into a buffer however the
bytes arrive. That remains an open possibility about code nobody has written, and removing an
appender says nothing about it. The first reading of this item had that argument backwards — "the
columns are compressed, so there is nothing to map in place" — which is true of three of six and
false of the other three.

Gone: `include/orderbook/mmap_store.hpp`, `src/mmap_store.cpp`, `tests/test_mmap_store.cpp`, the
`orderbook_mmap` library, the link dependency `orderbook_columnar` had on it and never used, the
`ob_add_mmap_test` helper with its single caller, the entry in the Clang coverage list, and the
`#include` in `columnar_store.hpp` that named nothing.

**Said plainly because it is a shipped artefact: a public header went away.**
`install(DIRECTORY include/orderbook)` shipped `mmap_store.hpp`, so the package carried 43 headers
and now carries 42, and `#include <orderbook/columnar_store.hpp>` no longer pulls it in
transitively. Nothing in the tree or in `scripts/verify_package.sh` named it, and
`liborderbook_shared.so` exports no `MmapStore` symbol, so the only consumer this can reach is one
who included a header for a class the engine never used.

- Effort: S | Impact: 362 lines of a component nothing called, whose adoption would have undone
  #112 and #113 — and `docs/storage.md` now says why the engine does not memory-map, with the
  number behind it

- Effort: S to delete, L to adopt | Impact: the front page described a storage mechanism the engine
  does not use, and nothing in CI could notice

### 113. `fsync`'s return value is discarded, so the strongest durability policy acknowledges writes it did not sync ✅

Found by roadmap #54's fault injector, and visible by reading before it was measured: **all seven
`::fsync()` calls in `src/wal.cpp` throw their result away.**

Measured with `--fsync-policy every`, the strongest promise this engine makes — the one whose whole
point is that a write is on disk before the client is told `OK`: **eleven `fsync` calls returned
`EIO` and all three `INSERT`s were still answered `OK`.** The node stayed up and exited 0. A client
cannot tell a durable write from one whose sync failed, which is the only thing that policy sells.

**Why a retry is not the fix, and why this is worse than an unchecked return usually is.** On Linux
a failed `fsync` **marks the affected pages clean**: the error is reported once, to whoever happened
to call, and the next `fsync` on that descriptor returns 0 with the data gone. So the engine can
neither learn about it later nor repair it by trying again. Whatever is done here has to be done at
the first failure.

What the fix is not allowed to be is an abort — that is #112, in the same file, from the other
direction.

**Done, and the shape of the fix is `[[nodiscard]]` rather than seven added `if`s.**
`WALWriter::flush()` and `sync()` return `bool` and are marked `[[nodiscard]]`, so ignoring the
answer is a **compile error**. A comment asking callers to check would have been the same omission
one layer up; this made each of the seven call sites say what it does instead, and the answers are
genuinely different:

- the two write paths under `every` **throw**, which is how a failed `write` is already reported —
  the client gets `ERR …` and the node keeps serving, measured and tested since #54's stage A
- `FLUSH` throws, because answering `OK` over a failed sync is the same lie as `INSERT` doing it
- both snapshot paths throw: a snapshot means "everything up to here is on the disk", and this is
  the call that establishes it. The async worker turns that into a failed snapshot, so the peer
  retries rather than bootstrapping from an unchecked premise
- the flush tick throws, and #112's per-iteration boundary counts and retries it
- **`close()` logs and completes**, because a shutdown path that throws is #112 from the other end,
  and the records are in the WAL file either way

`fsync_or_record()` is the one place that calls `::fsync`. It counts the failure, logs what the sync
was *for* — "fsync failed" alone does not say whether a client was waiting — and says what Linux
does next, because the obvious reaction is the one thing that cannot work. It deliberately **does
not clear `pending_sync_`**: that was the second-order defect, since the old code zeroed the count
after an unchecked `fsync`, so the writer came out of a failed sync telling the flush loop there was
nothing left to do.

`ob_wal_fsync_errors_total` is separate from `ob_flush_errors_total` on purpose: a full disk is
freed, a disk returning `EIO` is replaced.

**The attribute found sixteen sites nobody had looked at.** Five test files flushed the WAL and
dropped the result, then read the file back expecting the record to be there — so a flush that
failed would have made the *next* assertion report the wrong thing. All sixteen are assertions now,
`ASSERT_TRUE` in test bodies, `RC_ASSERT` in a RapidCheck property, and `EXPECT_TRUE` in one helper
that returns a value, because `ASSERT_*` expands to a bare `return`.

**A defect in this change, found by its own regression test.** The counter was published *after* the
WAL sync in the flush tick, under a comment saying a tick that throws would publish on the next one.
True of a transient failure; false of the case that matters — with `fsync` failing every time, every
tick throws at the same line, so the number an operator reads stayed flat while the writes were
being correctly refused. It is published first now, before anything in the tick can throw.

**Found along the way, and fixed here because it is a claim about the code rather than an item.**
`scripts/check_metrics.py` says it proves every metric written by name is registered, and scanned
`add_to_counter`, which `MetricsRegistry` does not have — a branch that could never match — while
not scanning `increment_gauge`, which it does have and which eight sites use for
`ob_active_sessions`. A metric written only that way escaped the check entirely. With the scan
corrected it sees one more written name, and a deliberately unregistered `increment_gauge` now fails
it, which it did not before.

**What this does not cover, stated.** The columnar segment files are written with buffered stream
I/O and are not fsynced per segment, so this policy is about the WAL — a segment lost to a power cut
is rebuilt by replaying it, which is what the WAL is for.

**A correction to this item's own commit message, recorded rather than rewritten because the
history is pushed.** It says the verification included "the 63 tests that read
`docs/operations.md` re-run after the doc edit, because the full `ctest` predated the edit and five
test files read `docs/`". Both halves are wrong. **No C++ test reads `docs/operations.md`** — five
of them *mention* the path in comments, and the grep that found them matched the comments, which is
use-versus-mention in a new place. And the full `ctest` did **not** predate the edit: that document
is inside commit `6930589`, which is the commit the verification ran on. Exactly one C++ test reads
a document at all, `CliConfigStatic.EveryKnownFlagIsInTheCliReference` over `docs/cli.md`, and that
file is in the same commit. So the verification was *stronger* than the message describes rather
than weaker, and every figure it quotes is accurate; only the reason given for one extra run is
not.

- Effort: M | Impact: `--fsync-policy every` now means what it says, and the failure is visible to
  both the client that asked and the operator who has to replace the disk

### 112. An ENOSPC on the flush thread's WAL write aborts the whole node ✅

Found by roadmap #54's fault injector. **The client-facing half of this is correct**, which is what
makes the rest dangerous: an `ENOSPC` on a delta record written by a client's own session is
answered `ERR WALWriter: write failed: No space left on device`, the row is not stored, the node
keeps serving, and `SIGTERM` still exits 0. Measured, including a disk that stays full: every
`INSERT` refused with the reason named, `PING` still answered.

The WAL writes that are **not** on a session thread have no such handler — and the scope is wider
than this item said twice before. **Sixteen of seventeen `std::thread` constructions in `src/` have
no exception boundary.** Any exception escaping any of them ends the process, and this engine throws
`std::runtime_error` from the WAL, the mmap store, the columnar store and several parsers.

The number was wrong twice, and how it was wrong is the lesson. First it was "`flush_loop` has no
`try`", which is one instance. Then it was "eleven of eleven thread entry points" — counted over a
list of entry functions **I wrote by hand**, which silently omitted five threads: both `run_loop`
threads in `replication.cpp`, `ShardCoordinator`'s migration thread, `coordinator.cpp`'s watch
thread and `AsyncSnapshot`'s worker. Derived from the `std::thread` constructions instead, the set
is seventeen. *A list you wrote yourself is not evidence about the code* — the rule #32 paid for,
in a new place.

The seventeenth is `src/async_snapshot.cpp:54`, and it is the only one that thought about this: its
lambda wraps `produce()` in `try`/`catch (const std::exception&)`/`catch (...)`, under a comment
saying that a joinable `std::thread` calls `std::terminate` and that losing one snapshot beats
losing the process. **Its boundary still does not cover the whole body** — the statements before the
`try` and, more to the point, the mutex taken after the `catch` to publish the result, from which a
`std::system_error` would escape. So the one place that reasoned about it is also an argument for
putting the boundary at the construction site rather than inside each body.

So the measured abort is one **reachable instance of a class**, which makes the fix a class fix: a
boundary wrapping every thread body at the point it is constructed, logging what escaped, plus a
static test whose rule is one grep-able property — *every `std::thread` construction in `src/`
passes its body through that boundary*. Chosen over "there is a `try` before the loop in the entry
function" because the second needs a parser and the first needs a line. The rule's discrimination
was checked before anything was written: it separates the seventeen constructions from the four
moves and declarations (`std::thread stale = std::move(worker_);`, `std::thread victim;`) and from
`std::hash<std::thread::id>`, and an early version of it misfiled `async_snapshot.cpp:54` as a move
because `std::move` appears in that lambda's **capture list**. That case is a test of the checker
now.

An outer boundary alone is not the whole fix: it stops the process dying and leaves a subsystem that
exits on its first exception, which is the "guarantee absent in production" shape. So each loop
needs its own judgement about continuing, and the ones where stopping is not survivable —
`flush_loop`, `lease_loop`, `monitor_loop`, `io_loop` — get a per-iteration boundary with a metric
and a log that does not flood (#95's shape). Whatever is not done gets written down rather than
left to be discovered.

The instance that was measured: two records reach the WAL from `flush_loop`, a 24-byte checkpoint
from `flush_write_and_merge()` and a 68-byte version vector from
`persist_version_vector_if_changed()`. Failing either one:

```
terminate called after throwing an instance of 'std::runtime_error'
  what():  WALWriter: write failed: No space left on device
exit -6
```

Measured by naming the call rather than counting calls — the injector can fail "the 68-byte write",
which is the same call on every run, where "the fourth write" is not:

- fail only the 68-byte write → **SIGABRT**, one injection fired
- fail only the 24-byte write → **SIGABRT**, one injection fired
- control, fail a size nothing writes → zero injections, three `OK`s, exit 0

**It kills an idle node, and it does it in a loop.** With nobody connected at all the process died
**0.5 s into idleness**, and across three consecutive restarts on a disk that stays full it came up
every time and died unattended after 1.5–2.0 s. A supervisor restarts it for ever, and what the
operator is handed is `SIGABRT` rather than "no space left on device". That is exactly the contrast
#102 was about, one call site further in: a failed bind used to abort instead of refusing, and
`load_secrets_or_exit()` was named there as the shape to copy.

No acknowledged write is lost, which is worth stating because it bounds the damage: after the crash
and a restart, replay returned every row that had been answered `OK` and none that had been refused.
This is an availability defect, not a durability one.

**Done in two layers, and the mutation table is what says both are needed.**

`run_thread_body(component, name, body)` in `include/orderbook/thread_boundary.hpp` wraps **all
seventeen** thread bodies at the point each is constructed. Fifteen were a one-line wrap; the two
long lambdas — `coordinator.cpp`'s 54-line watch and `async_snapshot.cpp`'s 35-line worker — were
named first and wrapped second, because indenting their bodies would have produced a diff in which
the one changed thing is invisible. `tests/test_thread_boundaries.cpp` holds the rule: every
`std::thread` construction in `src/` has `run_thread_body` inside its parentheses. It asserts the
**count** of constructions as well, because a rule that stopped matching would report a clean tree.
Six of its seven tests are the rule's own cases, each one a mistake it made before it shipped.

An outer boundary alone stops the process dying and leaves a subsystem that ends on its first
exception. So `flush_loop` also guards **one tick at a time**: the tick moved into
`Engine::flush_tick()` (no `continue`, `break` or `return` in it, which is what made that
mechanical), a failure increments `ob_flush_errors_total`, and the log is loud once and then quiet —
a permanently full disk would otherwise write an ERROR every interval for the life of the process,
which is #95's shape. The episode is closed by a recovery line, so silence never means the problem
went away.

| mutation | verdict |
|---|---|
| both boundaries present | the regression test passes |
| the per-iteration boundary removed | fails: *the tick failed but nothing counted it, so an operator watching a full disk would see a node that looks healthy* |
| both removed | fails: *the node died on a failing flush tick, exit -6*, `what(): WALWriter: write failed: No space left on device` |

The middle row is the one worth having. The process survives on the outer boundary alone — and the
flush thread is gone for the life of that process while every client-facing symptom looks fine.

**What was not done, named rather than left to be found.** `lease_loop`, `monitor_loop` and
`io_loop` had the outer boundary only, so an exception there ends that subsystem rather than the
process: a node whose lease loop stopped disappears from the mesh when its registration expires,
one whose monitor loop stopped never learns about a role change (#82's shape), and one whose io
loop stopped is out of the mesh entirely.

**The three loops it named are measured now, and one of the three sentences above was wrong.**
`io_loop` is done, in this item rather than a new one, because it is the same defect in a second
place. The other two are measured and open, and the correction matters more than it looks: the
loop whose epoll timeout is **zero** whenever a catch-up cursor has queue space is
`ReplicationManager::run_loop()` (`src/replication.cpp:846`), not the mesh `io_loop`, which waits
**500 ms** (`src/multi_master.cpp:712`). So the bound belongs to the replication loop, the scope is
**four** loops rather than three, and the note above was sending the next reader to the wrong
function.

Three probes with #54's injector, each with a control, and the first one ruled a path out.

**The startup promotion already refuses correctly, so it needed nothing.** A single node with a
coordinator promotes itself on the **main thread**, inside `TcpServer::run()`, so an `ENOSPC` on
the 32-byte `EPOCH` record (24-byte header, 8-byte payload) reaches `main`: the process prints
`Error: WALWriter: write failed: No space left on device` and exits. One injection, zero
thread-boundary lines — a named refusal, which is #102's and #113's shape rather than a silent
thread death. "I checked and it does not apply" is a different sentence from "I did not check."

**`monitor_loop` is worse than this item predicted.** Two nodes on one etcd, the injector on the
second only. B is a healthy replica, A is killed, B waits out #82's election delay, stands, and the
`EPOCH` write is refused:

```
"msg":"monitor_loop ended on an exception and that thread is gone: WALWriter: write failed: No space left on device"
```

The process **lives**, answers `PONG`, and `ROLE` reports **`REPLICA 127.0.0.1:44613 2`** — 44613
is B's **own** replication port. A half-finished promotion left the node a replica of itself, and
the loop that would fix that is gone: across the 40 seconds observed the role never changes. The
control, the same run at a size nothing writes, reports zero injections and `PRIMARY 2` at twenty
seconds.

**`io_loop`, now fixed, was the quieter of the two.** A two-node mesh, the injector on the
receiver, an `ENOSPC` on the 150-byte record a mesh delta writes (a 38-byte V2 header and 112 bytes
of payload — the same payload a client's `INSERT` puts behind the 24-byte legacy header):

```
"msg":"io_loop ended on an exception and that thread is gone: WALWriter: write failed: No space left on device"
```

Three records written on the peer afterwards **never arrived** (A had two rows, B still one), while
B answered `PONG` and its own `MM_PEERS` called the link **`connected`**, with a fresh HLC and
`send_queue_bytes 0`. A node out of the mesh whose every outward signal says the mesh is fine.

The boundary is **inside** the event loop rather than around the pass, and that is not tidiness:
every registration here is `EPOLLET`, so an abandoned event is not re-delivered and taking the pass
down would silently drop the rest of a batch that can hold 64. `ob_mm_io_errors_total` is
registered in the same change that writes it, and it is the **only** outward sign — which is the
flush half's own argument for a counter over a log line.

**Two things the first version of that boundary got wrong, both from running it rather than reading
it.** The recovery line fired in the **same pass** as the ERROR it was meant to close, because
`end()` runs after the loop that opened the episode — measured as a log alternating ERROR /
"handled again" / ERROR for three failing records. It is gated now on a pass that had events and
none of which threw. And the episode deliberately does **not** suppress one line per failed record:
#95's shape is a line per *loop iteration* carrying no new information, while three lines for three
records a full disk refused is information, and the counter beside them is the thing to alarm on.

The test asserts on records arriving **after** the refusal rather than on the thread being alive,
because alive is exactly what the node looked like when the thread was gone. Its control is a
second test, because a fault-injection test that quietly failed to inject reads identically to an
engine that survived the fault. The mutation that says both carry weight: leaving the report and
the counter in place and letting the thread die anyway fails with the message the test was written
for — *"the second node holds 2 rows, so records written after the refused one never arrived"*.

**Asked while the probe was running, and answered: no record is lost.** `apply_remote_delta()`
applies to memory at step 4 and writes the WAL at step 5, so a refused write leaves the record
visible and absent from the log. Measured with the flush tick an hour out and `SIGKILL`, which is
the only way to see the window: after the restart the receiver replayed `records=1 applied=1` and
the refused record was gone — and so was its place in the dedup frontier, because the frontier is
derived from what persisted. Either exit therefore restores it: the next flush writes it to a
segment, or a crash forgets it was ever seen and anti-entropy asks the peer again. What is **not**
measured is that last leg — the anti-entropy pass itself — and it is named here rather than
implied.

**`monitor_loop` is done too, in three commits that would have hidden each other in one.** The
seven byte-identical nap blocks collapsed first, on a property a script established rather than a
reading: six `continue`s each immediately preceded by exactly that nap, plus the one at the bottom,
so every path napped exactly once and `while (running_) { monitor_tick(); nap(); }` with
`continue` → `return` changes nothing. Then the tick extraction, 267 lines and no `continue` left in
it. Then the boundary, which is the whole of the third diff.

`FailoverManager` takes a `MetricsRegistry&` — nine `make_unique` sites and sixteen direct ones in
`tests/test_etcd_integration.cpp`, each already holding an engine, plus the one in `Engine::open()`.
Passed rather than reached through `RoleTransitionHandler`, because incrementing a counter is not a
role transition; not defaulted, because a default would let every test leave the counter unfed,
which is #117 exactly and #117 is why the counter is here at all. `orderbook_failover` links
`orderbook_metrics` now, which is what the linker said about it.

The recovery line sits **inside** the `try`, so a tick that threw cannot claim its own recovery —
the mistake the mesh boundary made one commit earlier, paid once rather than twice.

**And this is the part worth reading: the boundary fixed the thread and the node is still stuck.**
After the fix, the same probe reports the thread alive, `ob_monitor_errors_total` at 1, the episode
opening and closing — and `ROLE` still answering `REPLICA 127.0.0.1:44613 2`, that node's own
replication port, for the forty seconds observed. That is **#130**, a separate defect with its own
decision to make, and the test for this change deliberately asserts the **thread** rather than the
role: the recovery line can only be written by a tick after the one that threw, while "the process
is alive" was true when the thread was gone.

The explanation offered here for that answer — "the next tick adopts what it finds in the key" —
was **wrong**, and #130 records the correction: `adopt_leader_if_present()` refuses a key naming
this node and has since #73. The string came from `attempt_promotion()` setting `role_` and
`primary_address_` before calling the handler, so one half-finished act was visible through three
fields at once. The wrong explanation is left standing here with this paragraph beside it, because
it is the observation this item actually made and the correction belongs where the investigation
happened.

Two mutations, and the second is the one worth having. Rethrowing straight after the counter kills
the test on its first assertion — the failure output carries the old `monitor_loop ended on an
exception and that thread is gone`, which is the pre-fix line. Counting, logging **and then**
rethrowing kills it on the assertion that matters: *no tick ran after the one that threw*. Restored
from a copy kept alongside both times, green after.

**`lease_loop` and `run_loop` are done, and both are ratchets — the prediction above was right
about the first and half wrong about the second.**

Neither has a throwing path in this tree, and that was established rather than assumed.
`src/coordinator.cpp` contains **no `throw` at all** and `refresh_lease()` answers failure with
`false`, so what is left under `lease_loop` is `std::bad_alloc` from the string building and
`std::system_error` from the wait. Under `run_loop`, every `throw` in `src/replication.cpp` is
either on the startup path or in `ReplicationClient`; `continue_catchup()` reads with `pread` and
answers a short read by moving to the next file; `payload_len` is a `uint16_t`, so the one
allocation sized from a byte on disk cannot ask for more than 64 KiB; and
`ReplicationConfig::tls_server` is built before `start()`, so no certificate is loaded from that
thread. Both counters are registered with that caveat in their own comment, because an increment
nobody registered is discarded in silence (#77) and the first such exception should not also be the
first thing nobody can alarm on.

**`run_loop` gets two boundaries, and the second one is what makes it different from the mesh.**
Per event, for the same reason: every replica registration is `EPOLLIN | EPOLLOUT | EPOLLET`, so an
abandoned event is not re-delivered and taking the pass down would strand whatever the other
descriptors of a batch of 32 had ready. **And per pass**, because unlike `io_loop` this one does
real work outside the dispatch — the snapshot poll, both replica gauges, every catch-up cursor's
next batch, the five-second heartbeat — all four of which are re-attempted next pass, so the pass
is a safe unit to abandon where the thread is not.

**The floor is needed because the boundary creates the hazard, not because anything throws today.**
That is the correction to the sentence above. `wait_ms` outlives one pass, so a pass that throws
before reaching the recompute keeps the previous pass's value — and that value is **zero** whenever
a catch-up had queue space. While an exception took the thread with it there was no second pass to
spin at all.

**Measured, because a bound argued for is a bound somebody will remove.** The premise is
constructed — nothing here throws, which is the whole point — so the probe makes every pass throw
and starts `wait_ms` at what a catch-up with queue space leaves behind. The pass count is exact,
because the boundary's own counter is incremented once per abandoned pass. i3-7100U, Debug, one
node with no replica and no client, two 5-second windows each:

| | passes/s | CPU in a 5.0 s window |
|---|---|---|
| without the floor | **172 000–195 000** | **5.01 s and 5.04 s** — one whole core |
| with the floor | **10.0** and 10.0 | 0.01 s and 0.00 s |

100 ms is not a new threshold — it is what this loop already waits with nothing in hand, so ten
passes a second is a failing loop paced exactly like an idle one. `replication_wait_ms()` is a free
pure function for the reason `drain_verdict()` is (#106), so the `catch` says "no work in hand" by
its argument rather than carrying a second copy of the number, and a static test refuses any other
way of assigning `wait_ms`.

**What measuring `lease_loop` found is worth more than its boundary**, and both halves are separate
items. Revoking the lease that holds a node's mesh registration open: the key is gone at once and
**still gone 22.5 s later** while the node answers `PONG`, because `register_self()` runs once and
is the only writer of `lease_id_` — that is **#132**. And the node says so eleven times in 33 s,
one line per refresh interval from each of two components — **#133**, fixed.

**The loop count was a hand-written list too, and a mutation said so.** The first version of the
per-iteration rule listed this item's four loops by hand, and deleting a row from it **survived**:
the rule covered less and stayed green — the third time inside this one item that a list written by
hand turned out not to be evidence about the code. Derived from the tree instead: fourteen
`void Class::…loop()` definitions in `src/`, one of them a notifier with no loop in it, **six** of
the remaining thirteen guarding one iteration at a time and **seven not**. Those seven are **#131**,
with what each one's death costs. Four is what somebody counted; thirteen is what the tree has.

The mechanism is in `tests/test_thread_boundaries.cpp` beside the outer rule, checked in both
directions as the metrics checker is: a loop in neither list fails, and a row naming a function the
tree no longer has fails too.


- Effort: M | Impact: the most ordinary disk condition there is no longer turns a refusal into a
  crash loop, and the sixteen other threads that shared the exit are closed by the same change

### 111. A dial test's premise was a claim about the machine, and the machine changed ✅

`PeerDial.AnUnreachablePeerAddressDoesNotStopTheNode` guards #97: a dial to an unreachable peer
must not be holding the mesh mutex, so an inbound connection and a client write both have to
complete while one is outstanding. It dialled the literal `10.9.9.7:7100`, under a comment stating
that the address "is not routed on this machine".

**That is a claim about the machine, not about the engine, and it stopped being true.** The host has
a default route whose gateway answers ICMP "network unreachable", so the dial returned in about a
second instead of hanging for `MM_CONNECT_TIMEOUT_MS`. Measured in the failing run:
`Dial to peer 7 at 10.9.9.7:7100 failed: Network is unreachable (attempt #1)` 1.1 s after start,
where the test's arithmetic assumes five.

The assertion that failed was several lines away from the premise that broke, which is the expensive
part. The bound on claimed attempts was `1 + dialling_ms / MM_CONNECT_TIMEOUT_MS` — one attempt per
connect deadline — and with the dial failing fast the attempts came at the **backoff's** pace
instead. Two attempts in a 1.7 s window is `ReconnectBackoff` working exactly as designed
(`initial_delay_s` 1.0 s, jitter ±25%, so the earliest a second attempt can be claimed is 750 ms).
The test called it a redial storm. **Measured, this is not a regression**: ten interleaved runs of
the test in isolation, five on `9c32464` and five on the fuzzing branch, all pass — the failure
needs the gateway to answer *and* the test to be slowed by a full suite so the second attempt lands
inside its window.

**Done, in two parts.** The premise is now **constructed rather than assumed**: a listening socket
whose accept queue is deliberately filled, which drops further SYNs while
`net.ipv4.tcp_abort_on_overflow` is 0, its default. No routing table participates, the port comes
from the kernel and is held for the whole test — an ephemeral port we own cannot be handed to
anybody else, which is what #109 was about. And the test **states the premise instead of relying on
it**: it probes its own blackhole first and fails with "this machine will not hold a TCP connection
open" rather than failing an assertion about attempt counts.

The bound is now derived from the backoff schedule, which is the mechanism that actually paces
attempts, so it holds whether a dial hangs or is refused outright. A storm is a redial every 100 ms,
an order of magnitude away either way.

`PeerDial.ADialRefusedOutrightDoesNotBecomeARedialStorm` is new and makes the broken case
deterministic: nothing can listen on port 1 without root, so the kernel refuses immediately and no
network cooperates. Against the bound it replaced, that test fails; against the new one it passes.

- Effort: S | Impact: a required check stops being intermittently red for a reason that has nothing
  to do with the code it gates — the class of block this repository has already paid for twice, in
  the CodeQL outage and in the unretried etcd download

### 110. The interactive CLI drops what it does not understand, and guesses the side ✅

Found while closing #107, on the surface where a human actually types. `tools/ob_cli.cpp` parses its
own arguments with `istringstream >>` and embeds the engine directly, so the wire's new refusal does
not reach it:

```
ob> insert AAA EX sideways 6500000 1500
OK  seq=1  sideways AAA@EX  price=6500000 qty=1500      <- the word is echoed back
ob> insert BBB EX bid 6400000 1400 1 1700000000000000000
OK  seq=2  bid BBB@EX  price=6400000 qty=1400            <- the event time is dropped

ob> query SELECT side, price, quantity FROM 'AAA'.'EX' ...
  1789194659676840286  | bid  | 0 | 6500000 | 1500 | 1 | 1     <- stored as a bid
```

Measured, not read: the confirmation line echoes **`sideways`** as though it were a side, and the
row is a **bid**. An echo that repeats the typo back is worse than silence, because it is the exact
place a human looks to check that the tool understood. The second line drops the event time the same
way the wire did before #107 — the row's `ts_ns` is arrival time.

The side is the sharper half and it is not about trailing tokens at all:
`side_str == "ask" || side_str == "ASK" ? SIDE_ASK : SIDE_BID` makes **every** word that is not
exactly `ask` or `ASK` a bid. `Ask`, `asks`, `sell`, `sideways` and a typo all store the opposite
side of the book from the one that was typed, with no message. The wire refuses that same word
(`ERR unexpected token`… since #107, and an invalid side was already refused before it) — so the
tool built for a human is the one that guesses. The same ternary appears three times, and two more
handlers (`bulk`, and the loader at line 264) read arguments the same way.

**A third silence in the same tool, found by running it:** `ob_cli --data-dir /tmp/x` prints
`Data directory: --data-dir` and opens a store in a directory of that name, because the whole of
argv handling is `if (argc > 1) data_dir = argv[1];`. An unknown flag becomes the data directory,
which is #36's `--prot 5599` with a filesystem attached — and the reason it went unnoticed is that
the tool's own usage line says `ob_cli [data_dir]`, so nobody who read the help would type a flag.

Why this was filed rather than folded into #107: it is a different surface with a different testing
question. Nothing in this repository exercised the interactive CLI — `tests/test_cli_args.cpp` and
`tests/test_cli_config.cpp` are about the *server's* flags — so the fix needed somewhere to prove
itself first, and that was the larger half of the work. #36 is the precedent for what "refuses what
it does not understand" should look like here, and it also warns what happens without a test: its
own negation table was built on a premise read from a default rather than from the parser, and a
static test deleted all three pieces.

**Done.** One `parse_side()` accepting `bid`/`ask` in **any case** and nothing else — so the parser
became *more* permissive about spelling while refusing nonsense — replacing the ternary at all three
sites. One `nothing_follows()` refusing a token the grammar has no place for, naming it, which is
#107's rule one layer in. Confirmations print the side that was **stored** rather than the word that
was typed. The CSV loader **counts** a mistyped side as an error instead of loading it as a bid, so
`Loaded 480 rows (20 errors)` is something an operator can act on. And argv refuses an unknown flag
instead of opening a store in a directory named after it, with `--help` as the one dash argument
that works — refusing every one of them would refuse the first thing a reader tries.

Measured after, by running it: `insert AAA EX sideways 6500000 1500` is refused naming the word, and
**the symbol does not exist afterwards** — which is a stronger statement than "it was not stored as a
bid". `insert CCC EX Ask 6400000 1400` is accepted and the row comes back on the **ask** side.

The first CI run exposed a missing half of this coverage: all seven CLI tests skipped because the
integration job built only the server and C++ client harness. Both integration jobs now also build
`ob_cli`, and its fixture derives the sibling binary from `OB_SERVER_BINARY`, so the TSan job cannot
quietly test an uninstrumented CLI from `build/`. The existing skip gate caught the omission.
The test helper also checks the process exit status, and the flag-refusal test owns both paths it
asserts were never created.

Seven integration tests, each refusal with a control beside it, driving the binary with a script on
stdin. Six mutations, five killed, and the survivor is the control — but not on the first run: the
control was killed for a reason that had nothing to do with what it changed, because **my own test
asserted on a path in the repository root** and an earlier mutation run had created it. Shared state
in an assertion, which is #109's class in a new place; the test owns its working directory now.

- Effort: S for the refusals, M with a harness that can drive the CLI | Impact: the tool a human
  types into stored the wrong side of the book for a mistyped word, in silence


### 109. Tests bound fixed ports inside the range the kernel hands to anybody ✅

Found while verifying #107, and the diagnosis is the point: **seven tests failed against a tree
whose only change was the command parser**, and the same seven passed when re-run on their own.

```
{"component":"mm","msg":"bind() failed on port 55400: Address already in use"}
C++ exception with description "connect to mesh port failed" thrown in the test body.
```

Not `TIME_WAIT` — the mesh listener does set `SO_REUSEADDR`, and that is the case it covers. The
squatter was an **active socket belonging to somebody else**: measured,
`/proc/sys/net/ipv4/ip_local_port_range` is **32768–60999** on this machine and on a GitHub runner,
and four test files named 47821, 54900, 55100 and 55400 — every one inside it. Any outgoing
connection on the machine may be given one of those numbers at any instant, and 25 ephemeral ports
were in use while that run was going. The odds changed with the **machine**, not with the code:
ClickHouse and PostgreSQL were installed here the day before for #39, and ClickHouse alone keeps
about a thousand threads.

**Why this is an item rather than a re-run.** A required check that goes red for a reason unrelated
to the code is indistinguishable from a finding until somebody reads the log — a bill this
repository has paid twice already, once for a CodeQL outage and once for an etcd download with no
retry. And the habit it teaches is the expensive part: a suite that needs luck trains its readers to
press re-run, which is exactly how the next real failure gets through. It also came within one step
of costing more than time: the first reading of those seven failures was "my parser change broke the
mesh", and the file it points at is the one #96 and #97 came out of.

The fix is one header, `tests/test_ports.hpp`, with a 100-wide block per test binary, all **below**
the ephemeral floor — so only our own tests can collide, and that collision is visible in one file
rather than spread across eleven. Three files were already safe (19876, 21876, 21987) and were moved
into it too, because a file that keeps its own base is the one the next author copies. `test_ports`
is no place for a fixed port to hide: `test_port_discipline` reads the floor **back from the
kernel** rather than trusting the constant beside it — the constant is precisely the thing that
would be wrong on the machine where this matters — and it fails, rather than skips, if the range
ever starts below our blocks.

What this deliberately does not do is convert these tests to OS-assigned ports. They hand a number
to a component that binds it later, so there is nothing to ask the OS on behalf of; the number has
to exist before the socket does. `test_mm_port_isolation` uses port 0 where that *is* possible, and
that stays.

- Effort: S | Impact: every network test on the suite was one unrelated outgoing connection away
  from a red required check, and the first reading of that red is always "my change broke it"


### 108. The io_uring transport was never built in CI ✅

No CI job compiled `src/io_uring_server.cpp`. The gap became visible while finishing #106, when
manual work on the drain fix exposed a problem in the second transport that the usual CI builds
could not reach. An optional transport can stop compiling while every required check stays green.

**Done: `io-uring-build` compiles the separate `ob_tcp_server_iouring` target** in Release with
`-DOB_USE_IO_URING=ON -DOB_BUILD_TESTS=OFF`. The option adds that target; it does not replace the
ordinary `ob_tcp_server`, which still uses epoll. The job therefore checks the linked binary for a
defined `ob::IoUringServer::run()` symbol as well as checking the build's exit status. Locally, the
symbol check accepted a fresh io_uring build and rejected a freshly built epoll control.

**The scope is compilation and linking.** The job starts no server and needs no running etcd. It
provides no evidence about io_uring runtime behaviour or data races. Those need execution on a
kernel that supports the transport. In particular, node-link TLS remains refused: its own epoll
loops do not establish that the encrypted links have been exercised on this transport. The refusal
and the operations guide now name the missing runtime coverage rather than a missing build job.

The workflow job and `{"context": "io-uring-build"}` in `.github/rulesets/master.json` enter in one
PR, because `check_contexts.py` rejects a produced but unrequired context. The live ruleset is
applied **after merge**, then read back to verify all thirteen contexts: requiring the new context
before an open branch can produce it blocks that branch indefinitely. The apply instructions use
the versioned JSON rather than an older, incomplete copy embedded in a document.

- Effort: S | Impact: every PR compiles and links the optional transport; runtime tests remain a
  separate piece of verification

### 107. The wire parser accepts trailing tokens on `INSERT` and `MINSERT` ✅

Found while measuring what an older server would do with the extra field #105 needs.

```
INSERT AAA EX bid 100 5 1 1700000000000000000   -> OK
INSERT AAA EX bid 100 5 1 notanumber            -> OK
MINSERT AAA EX bid 1 1700000000000000000        -> OK
```

All four were accepted and all four stored a row. The parser reads the fields it knows and ignores
the rest, which is **pitfall 27 on the wire protocol** — the same class #36 closed for command-line
flags, where `--prot 5599` was silently skipped and `--port 99999` was cast down to 34463. The CLI
got a parser that refuses what it does not understand; the wire never did.

Two consequences, and the second is why this is filed on its own rather than inside #105:

- an operator's typo is accepted. `INSERT SYM EX bid 100 5 1 extra` is a row stored with something
  the sender meant to matter, discarded without a word.
- **it decides #105's client design.** An upgraded client sending a timestamp to an older server
  gets `OK` and the value is dropped — which is exactly the defect #105 is about, one layer out. So
  the client cannot rely on the server refusing the extra field, and has to ask what it is talking
  to. Refusing trailing tokens is what makes a future field's absence loud instead of silent.

The refusal has to name the token rather than the count, because "too many arguments" sends an
operator counting spaces.

**The title undersold it: the scope was the whole command set.** Measured over a live server before
the change, sixteen shapes tried and **fourteen accepted a token nobody reads** — five of them
storing a row for it:

| sent | answered | stored |
|---|---|---|
| `INSERT AAA EX bid 100 5 1 1700000000000000000` | `OK` | 1 row |
| `INSERT AAA EX bid 100 5 1 notanumber` | `OK` | 1 row |
| `MINSERT DDD EX bid 1 1700000000000000000` + one level | `OK` | 1 row |
| `MINSERT EEE EX bid 1` + `100 5 1 notanumber` | `OK` | 1 row |
| `PING please` / `FLUSH now` / `ROLE primary` | `PONG` / `OK` / `STANDALONE` | — |
| `COMPRESS LZ4 level9` / `UNSUBSCRIBE 1 now` | `OK COMPRESS LZ4` / `OK 0` | — |
| `MM_CONFLICTS notanumber` | the default limit of **100** | — |

After the fix: all sixteen refuse, and **zero rows** where five were stored.

**The fix is a table, not two branches, and that is the whole difference between this and a patch.**
Seventeen of eighteen commands read the fields they knew and ignored the rest. `AUTH` was the
exception, through an exact `tokens.size() != 3` written by whoever happened to think of it there —
and correct behaviour present in one place out of eighteen is indistinguishable from nobody having
decided. `kGrammar` is **indexed by `CommandType`**, so a nineteenth command is a *compile* error
until it declares its arity: the same mechanism as the missing `default:` in
`allowed_before_authentication()`, which is what makes `-Wswitch` tell us about the next command.
Only the maximum lives there — each branch keeps its own minimum, because a branch that reads
`tokens[5]` needs the guard that makes the read safe, and a minimum in the table would be read by
nothing, which is a poor field to add in the repository that spent #104 on the fifth one.

Two things came along, both the same silence in another shape. A **level line** of a batch accepted
`100 5 1 notanumber` and stored the level; its refusal names the line number, because a batch is up
to `MAX_LEVELS` lines and "somewhere in there" is not an answer. And `MM_CONFLICTS notanumber`
**silently became 100**, four lines above an `UNSUBSCRIBE` branch that already refuses exactly this,
having learnt it from #36.

**Wording measured rather than chosen:** the query parser already refuses a trailing token by name
(`SELECT * FROM 'AAA'.'EX' garbage` → `ERR Parse error at line 1, col 26: unexpected token
'garbage'`), so the command layer borrows its words and the protocol says one thing one way. That
measurement is also what makes `SELECT` and `SUBSCRIBE` a genuine exemption from *counting* rather
than an exemption from refusing. `AUTH`'s extra token is refused **without being repeated**: every
refusal writes a log line, and a response echoed into a log is a response in a log.

**Where this rule deliberately does not reach.** The replication and multi-master line parsers use
`sscanf`, which ignores everything past the fields it names: `REPLICATE 0 0 0 junk` parses as
`REPLICATE 0 0 0`, and eight call sites behave that way. That is not the same defect. On those links
the **field count is the version negotiation**, and it is written down: `parsed >= 2`, `parsed == 3`
and `parsed == 4` are three meanings of one line, which is exactly what keeps a pre-epoch primary
readable (#103). Leniency there buys a compatibility that no client typo can buy. What it does not
buy is the *next* field — a node too old to know it accepts the line and drops the value in silence,
which is #105's shape one surface over, and the answer this repository has already chosen for that
twice is negotiation rather than tolerance (`STREAMID?` in #101, `capabilities:` in #105's spec).
Recorded here rather than filed as an item: there is no defect to reproduce today, and the fix for
the future one is a handshake, not a token count.

**The refusal's log line needed bounding, and that is part of the item rather than a footnote.** A
refused command is reachable **before authentication** — `allowed_before_authentication()` lets an
unparseable line through precisely because it is refused anyway — so a WARN per refused line is a
flood any peer who can reach the port can drive at line rate, which is #95's shape (a permanent
failure retried at loop frequency and logged with it). The halves are split by who knows what: the
parser says *what* is wrong at DEBUG, being pure and having no fd to name; `execute_command` says
*who* sent it at WARN, **once per connection**, naming the fd. Every refusal increments
`ob_refused_commands_total`, which is the alertable half — a working client produces none of them.

**Mutation corrected the design, not just the tests: fourteen mutations, thirteen killed, and the
survivor is the control.** The one that mattered was mine. The first version wrote "no maximum" as
`kFreeForm = size_t(-1)` and guarded with `max_tokens != kFreeForm && tokens.size() > max_tokens` —
and **deleting that guard changed nothing**, because no line has `SIZE_MAX` tokens, so the
comparison was already false. A guard that cannot fail is a guard nobody can check, and the next
reader trusts it. The emptiness now lives in the type (`std::optional<size_t>`), so dropping the
test compares against `nullopt`, refuses every `SELECT`, and dies. The rule worth keeping: when a
mutation survives, ask whether a sentinel is quietly doing the work the guard claims to do.

The survivor is the refusal's log line, which nothing asserts — it is for operators, and saying so
is more honest than a table in which everything dies.

Eleven unit tests and four integration tests, every refusal with a control beside it — a parser that
refuses everything passes every refusal test. The unit tests read the exported `command_grammar()`
rather than a second list of the same facts, which is the shape that cost #32 a flag, a negation
table and a test built on a premise read from a default instead of from the parser.

- Effort: S | Impact: a mistyped write is accepted in silence, and every future wire field inherits
  the same silence


### 106. A node with a connected client never exits on `SIGTERM` ✅

Measured, i3-7100U, Release, and the two halves are one command apart:

| | time to exit on `SIGTERM` |
|---|---|
| no client connected | **0.11 s**, code 0 |
| one **idle** client connected | **still running after 60 s** |

Not a hang, and the distinction is the item: on `SIGTERM` the listener closes at once — a new
connection is refused immediately — and the process then waits for **every existing session to
close**, exiting 0.00 s after the last client disconnects. The draining is deliberate and right; what
is missing is a **bound**. As written, one long-lived client keeps a node alive for ever, and a
long-lived client is the normal case for a database: a connection pool, a `SUBSCRIBE` stream (#45), a
monitoring client.

What that costs, in the order an operator meets it:

- **systemd turns the graceful stop into a hard kill.** `TimeoutStopSec` defaults to 90 s, after
  which the unit is `SIGKILL`ed — so the shutdown path that exists to flush and checkpoint is the
  one thing that does not get to run. That is #102's argument about exit modes, arriving from the
  other end: there the exit code lied about a configuration error, here the exit mode is decided by
  whoever happens to be connected.
- **the integration harness has been hiding it.** `_stop_node()` is "SIGTERM → wait 5 s → SIGKILL",
  silently, so every node with a client attached has been hard-killed for as long as that helper has
  existed. `scripts/bootstrap-cluster.sh` escalates too and at least says so (pitfall 88).
- **a rolling upgrade (#56) cannot be graceful** while any client holds a connection.

**The same defect is in both transports, which decides the shape of the fix.** `io_uring_server.cpp`
checks `draining_ && active_sessions <= 0` in **two** places, neither with a deadline — and **no CI
job builds that file**, so a fix written twice cannot even be compiled by CI on one of the two
sides. This repository has paid for "the fix exists and is used at one of two sites" often enough to
name it: `c_api.cpp` beside the server in #91, two wipe sites in #101, two entry points in #102. So
the deadline belongs in **one helper both loops call**, with a static test refusing a drain check
that does not go through it.

The fix is a deadline rather than a mode switch: `--drain-timeout-ms`, default **10 s**, after which
the loop closes what is left and exits — logging how long it waited and **how many sessions it
cut**, because that count is the difference between "the clients left" and "the node left". `0`
keeps the old behaviour and has to be asked for. PostgreSQL's three shutdown modes are the
reference; ours implemented only "smart", with the timeout at infinity.

**Measured after the fix**, same machine, same three cases:

| | before | after |
|---|---|---|
| nothing connected | 0.11 s, code 0 | **0.11 s**, code 0 — unchanged, which is the control |
| one idle client, default bound | still running after 60 s | **10.15 s**, code 0, `Drain deadline of 10000 ms reached with 1 session(s) still open` |
| one idle client, `--drain-timeout-ms 2000` | — | **2.12 s**, code 0 |
| one idle client, `--drain-timeout-ms 0` | — | still running after 4 s, and it leaves the moment the client does |

The exit is a clean **0** in every case: a node that cut sessions still flushed and checkpointed, so
the count in the WARN line is what tells the two apart rather than the exit status.

**The decision lives in one function both transports call**, because the io_uring half is in a file
no CI job built at the time, and a bound written three times is a bound that drifts.
`drain_verdict()` is pure
and takes the clock as an argument, so its four cases are unit tests rather than sleeps; a static
test over both sources requires each to consult it and refuses any line that pairs `draining_` with
`active_sessions` on its own.

**And the harness was hiding it, so the harness changed too.** `_stop_node()` is "SIGTERM, then
SIGKILL after five seconds", silently — and the server's new default of 10 s is *longer* than that,
so the default would have kept the whole battery on the killing path. Every node the integration
suite starts now carries `--drain-timeout-ms 2000`, which puts the drain inside the escalation
window: from this change on, two hundred tests exercise the graceful path on the way out instead of
being killed.

Three integration tests, each with its control: a node with a client attached leaves and **says what
it cut** (2.2 s measured), an empty node leaves well inside its budget (0.2 s — if it took the whole
budget the bound would be a sleep rather than a deadline), and `0` is still there for a deployment
that would rather hang than cut a session.

- Effort: S | Impact: every node with a client attached is `SIGKILL`ed by its supervisor instead of
  shutting down, which is exactly when the flush and checkpoint matter


### 105. Nothing can be written with its own event time over the wire ✅

Found by #39 part two, in the only way it could be found: by loading the same dataset into three
systems and asking each of them the same time-range question. Two answered with 400 rows. Ours
answered with none.

`OrderbookEngine.insert()` takes a `timestamp_ns` argument, computes it (`ts = timestamp_ns if
timestamp_ns is not None else time.time_ns()`), passes it in the **embedded** branch — and never
mentions it again in the TCP or pool branches, because `INSERT` and `MINSERT` carry no timestamp
field. The server stamps arrival time. So the same call means two different things depending on how
the client was constructed, and the one that loses the value is the one that goes over a network.

**Measured**: 200 000 rows loaded with timestamps from `1700000000000000000` came back stamped
`1789153200757060030`, and `SELECT … WHERE timestamp BETWEEN <dataset span>` returned **0 of 400
rows** for one symbol while the same CSV in ClickHouse and TimescaleDB returned 400.

The reach is wider than the benchmark, and each part of it is checked rather than assumed:

- **four integration call sites pass `timestamp_ns` over TCP** — two in
  `test_mm_snapshot_bootstrap.py`, two in `test_failover.py` — and **none of them asserts on the
  value**, so the drop is invisible to the whole battery. That they pass the argument at all is the
  evidence worth keeping: it reads like the right thing to do.
- **the comparative harness's time-range workload has returned zero rows since part one**, and
  part one's published noise floor was measured through it (see #39).
- `benchmarks/comparative/dataset.py` states the premise in its own docstring — "the client takes
  **one** `timestamp_ns` per batch, so a batched load would have stored timestamps that the
  time-range query then selects on" — which was false when it was written.

**Why this is a capability gap and not a cosmetic one.** This engine's central query is a time range
over market data. With arrival time as the only timestamp a record can have, replaying history into
it over the wire is impossible, a live feed's event time is lost at the door, and a `timestamp
BETWEEN` answers a question about when the engine heard rather than when the market moved. The
capability exists in the engine — `DeltaUpdate::timestamp_ns` is what the embedded path fills — and
stops at the protocol.

The fix is a protocol change, which is why it is filed rather than patched inside a benchmark item:
an optional trailing field on `INSERT` and `MINSERT`, both clients, `docs/cli.md`, and a decision
about what an older server should do with the extra token. The client must **refuse** rather than
drop — silently dropping is what this item is.

**Done, and measured in the shape the defect was measured in.** `INSERT … [event_time_ns]` and
`MINSERT … [event_time_ns]` on the header, optional and last — the only shape in which an old client
and a new server understand each other without negotiating, because six fields still mean exactly
what they meant. Over a live server:

| sent | rows inside the sender's own 2 ns window |
|---|---|
| `INSERT AAA EX bid 100 5 1 1700000000000000000` | **1 of 1** |
| `MINSERT CCC EX bid 2 1700000000000000000` + two levels | **2 of 2** |
| the same lines without the field (control) | **0**, and the rows are there at arrival time |

One time per batch rather than one per level, because a batch **is** one book update at one instant —
which is what `DeltaUpdate` already models, and why the comparative benchmark's generator groups its
rows by `(ts, symbol, side)`.

**Zero is refused rather than read as absence.** Zero is how "unassigned" is spelled everywhere else
here (`DeltaUpdate::sequence_number` uses it for exactly that), so accepting it would make "I have
no time for this row" and "stamp it on arrival" the same request — in the one place where telling
them apart is the whole feature. A non-numeric value is refused naming the token. There is
deliberately **no sanity window**: a server that refused a timestamp from 2019 would break backfill,
which is the case this field exists for.

**The client asks, and refuses rather than drops.** Sending the field is not a test for support —
measured on the release before #107, a server answers `OK` to a trailing token and stores the row
without it, so a client inferring support from a successful write would be inferring it from the
very defect it is avoiding. So `STATUS` carries `capabilities: insert_event_time,strict_args`, read
**once per connection and only by a caller who passes a time** (a read-only client pays no round
trip for a question it never asks). Names rather than a version number: a number needs a semver
parser in every client — two today, four planned — and the question is "can you take this field",
not "what are you called". **The absence of the line is an answer**, not an error.

Both shipped clients refuse before a byte goes out, and the proof is stronger than a row count: the
symbol does not exist afterwards. The Python client also **refuses `seq` over the wire** — a
sequence number belongs to the origin, and over TCP that is the server, which assigns one per symbol
(pitfall 16). It had been accepted and discarded, which is this item's own defect wearing another
argument's name; `python/stress_test.py` was passing it, which is how the assumption became visible.

**What a client-chosen time does and does not move, checked in the code rather than assumed:** LWW
in multi-master compares the **HLC**, which comes from the node's clock, so a client cannot decide
which of two conflicting writes survives — that was the single real risk and the reason this was
addable at all. Segment pruning is a range-intersection test, so out-of-order times cost scan
efficiency, not correctness. TTL retention works on whole segments by their newest event time, so a
**backfill arrives with its age** — a year of history into a node with a 24-hour TTL is expired on
the next sweep, and one row dated in the future keeps its whole segment. All three are now in
`docs/operations.md`, because the second and third are the kind of surprise an operator meets at
3 a.m. **The second and third were false until #166**, and they were checked against the wrong
thing: `SegmentMeta` declared the earliest and the latest timestamp, and `flush_segment()` filled
them with the start of the first row's hour and the last row's time — so a row written out of order
fell outside the range a query intersects, and retention judged a segment by its last-written row
rather than its newest. The intersection test was right; the range was not the rows'.

**A capability nothing reads is the shape this workspace has paid for five times**, so a static test
requires every announced name to have a declared reader, in both directions — `insert_event_time` is
read by the Python client's gate, `strict_args` by the tests of the refusal it names. Adding a third
name forces naming its reader.

Tests: five unit tests over `execute_command` (each with the control that makes it mean something —
a server stamping *every* row with the sender's time would pass the first and fail the second), four
property and unit tests over the C++ client's formatter including a **byte-for-byte** check that a
caller who passes no time produces exactly the line an older client produced, and four integration
tests over the wire — the event time, the capability answer, the refusal against an older server
(its capability set replaced with the empty one a pre-#105 server reports), and the `seq` refusal.

**The comparative table was recomputed by one run rather than edited**, which is what #39's own
entry now carries: with the field in place all three systems filter on the same range, so the table
compares the same question for the first time. The published summary went from "loses all four
comparable workloads" to **"loses three, one inside the floor"** — the time-range query against
TimescaleDB is 9.32 ms against 8.67 ms, 7% apart against a measured floor of 21.2%, so the harness
reports it as indistinguishable rather than as a win. The engine did not get faster; the comparison
got honest.

- Effort: M | Impact: the engine's main query selects on the wrong clock for every record written
  over a network, and the argument that looks like the fix is accepted and discarded

### 104. A field that claims a guarantee, written at one site and read at none ✅

`FailoverManager::adopted_primary_address_` was assigned in exactly one place, inside the
graceful-handover path, and **read nowhere**. Its docstring said what it was for: "the primary
address this node has told the engine to follow, so a leader change is adopted once and an unchanged
leader does not restart replication every second."

**The property is real; the field never provided it.** Checked rather than assumed: the monitor
loop's REPLICA branch records the address and calls nothing else, so a replica watching an unchanged
leader restarts nothing. `adopt_leader_if_present()` — which does call `demote_to_replica()` — is
reached only from the STANDALONE branch (#73) and from `handle_primary_lease_lost()`, both
transitions rather than per-tick work, and it stores `REPLICA` into `role_` so the next tick takes
the other branch. The "adopted once" guarantee comes from **where the calls are**. The field is gone
and the sentence now sits in the two places that produce it.

There was no behavioural symptom to reproduce, and that is why **the deliverable is the check, not
the deletion**. This was the sixth instance of the shape in this workspace — after `provisional`,
`basis`, `in_use`, `key_id` and `partition_by` in the flagship product — and all six were found
while looking for something else.

`tests/test_field_usage.cpp` surveys every member declared in `include/orderbook/` against every
occurrence in `src/` and `include/`, and fails on any field whose occurrences are **all** plain
writes with the value discarded. Measured before the deletion: **one** finding in the whole engine,
this one. After it: none. Three tests, 1.6 s each.

**Building it produced six false findings and five silent misses, and each is now a case in
`FieldUsage.TheRulesAreTriedOnTheCasesTheyGotWrong` — the cases come from the mistakes rather than
from imagination.**

| what the checker got wrong | why | fields affected |
|---|---|---|
| `confirmed = last_ownership_confirmed_;` read as a declaration | a name before `;` with something in front of it is *also* an assignment; what separates them is that a declaration's prefix ends in a **type** and an assignment's ends in `=` | six reported dead |
| a trailing comment containing `(` | the declaration rule refused parentheses after the name, and `// raw bytes (before compression)` has one | four skipped silently |
| a template's closing `>` | the member-access test skipped spaces, so `std::atomic<int> level_{...}` read as `ptr->level_` | one skipped |
| `return message_;` read as a declaration | same shape again; filing it as one hid the **only** read of `Result<void>::message_` | one reported dead |
| `conn_id = next_conn_id_++`, `insert(x).second` | a mutating expression whose value is consumed is a read; the rule is that the statement must **begin** with the name | four reported dead |
| a constructor's initialiser list | `: pos_(0), end_(0) {}` is the only write some members ever get, so it counts as one | would have hidden a dead field |

**Two tripwires, because a static test whose subject can quietly shrink is not a test.** The refined
enumeration is compared against a deliberately cruder scan, and a name only the crude one finds is a
failure — that comparison is what found the five misses. The crude scan itself must find at least
200 names, so the pair cannot both collapse to nothing.

**What it deliberately does not cover, named rather than left to be discovered:** a field filled
through a non-const reference (`read_into(x_)`), through `memcpy(&x_, …)`, or by a free
`std::swap(x_, y_)` reads as used, because the name is not the target of the statement. Tests and
`tools/` are not searched — a field whose only reader is a test is not a mechanism, and a tool
reaches a private member only through an accessor, which lives in a header and is searched.

Mutations: ten, each with the verdict it was **supposed** to produce, because one of them is a
control that has to survive — a checker that reports every field says nothing by saying everything.
All ten agreed. The two that matter: putting the deleted field back is caught, and planting a
brand-new write-only field in an unrelated class is caught. Two mutations had to be rephrased
because they did not compile (`-Werror=unused-function` on a dropped call, `-Werror=type-limits` on
`count(...) >= 0`), which measures nothing; and **one survived and the reason was in the test** —
the enumeration tripwire compares two scans that share a helper, so removing comment stripping
weakened both equally and they went on agreeing. The rule has its own case now.

- Effort: S | Impact: a docstring claiming a guarantee its field did not provide, in the file where
  role transitions are decided — and, for the class behind it, a check where there was none


### 103. A replica's epoch protection starts every connection at zero ✅

Found by an assertion in a #101 test that expected the engine's epoch on the wire and got a zero.

`ReplicationClient::local_epoch_` was initialised to 0 and **only ever raised by what the primary
sent** — a `WAL` line's epoch, an `EPOCH` record, a `HEARTBEAT`. Nothing seeded it, and a fresh
object is what every path produces: `demote_to_replica()` constructs a new `ReplicationClient`, and
so does a restart.

That disarmed two guards, in both directions, on exactly the path they exist for. The primary's is
`ERR STALE_PRIMARY`, refused when `replica_epoch > wal_.current_epoch()`; zero is never greater than
anything. The replica's own is the filter that skips a record whose epoch is below what it has seen —
also zero, so nothing was below it. Both came back to life only after a record or heartbeat had
arrived on *this* connection carrying a higher number, which is to say after the point where a stale
primary would already have been served.

**What the measurements said, and one of them moved the fix.**

| | before | after |
|---|---|---|
| a node that held the role in epoch 9, demoted, introduces itself | `REPLICATE 0 0 0` | `REPLICATE 0 0 9` |
| a node that learned epoch 9 from a heartbeat, then a role change | `REPLICATE 0 0 0` | `REPLICATE 0 0 9` |
| a record announced at epoch 5 reaching a node that knows 9 | applied and stored: `records_replayed=1`, one row, connection open | refused, connection ends, `count_rows=0` |
| the same node after a `SIGKILL` and restart, first handshake (live cluster) | epoch `0` | epoch `1`, matching its primary |
| a primary at epoch 5 answering `REPLICATE 0 0 9` | `ERR STALE_PRIMARY` — this half worked | unchanged |

The second row is the one that changed the design. **The fix this item described — seed the client
from `engine_.current_epoch()` when it starts — would not have reached it.** Following a primary
never raised the engine's epoch: `WAL_RECORD_EPOCH` records are written by promotions, so a node
that has only ever followed holds the number nowhere durable and nowhere shared. Seeding from a
field that is itself zero is a fix that measures as one only in the case somebody thought of.

So the number was **removed from the client rather than initialised there**. It is a fact about the
node, and the engine already holds one with that meaning; `Engine::note_primary_epoch()` raises it
and never lowers it, and the six read/write sites in the client collapse into one guard. #103 was
not a missing initialiser — it was a second copy of one number, and the copy was the one the guards
read.

**Three consequences, all deliberate.**

**The epoch is never forgotten**, including when #101 decides the stream is foreign and wipes the
store. That was a real choice and it went the other way first: a wipe already discards the position,
the store and the sequence frontier, so discarding the epoch with them is the consistent-looking
move. It is wrong, and the case that says so is the ordinary failover: our saved stream identity is
the *old* primary's, so every failover takes the wipe path — a replica that forgot its epoch there
would be unfenced exactly when a superseded primary is on the network. The price is named instead: a
data directory carrying a higher epoch than the cluster it is pointed at refuses to follow it, with
both numbers in the log line. `docs/operations.md` covers the two ways to read that line, because
the same line means "your primary is superseded" and "this directory belongs to another cluster",
and only one of them is fixed by emptying the directory.

**An EPOCH record raises without refusing, unlike the line that carries it.** A catch-up forwards
every record type but `ROTATE`, and `send_to_replica()` stamps each line with `wal_.current_epoch()`
— the primary's *now*. So a replica catching up is handed the EPOCH record of every past promotion
on lines announcing the current epoch: the payload is then, the line is now. Refusing on the payload
would disconnect every replica replaying a failover out of the log. Pinned by a test that sends
epoch 3's promotion on a line announcing 9 and then requires the next record to land — because a
test asserting the absence of a disconnect passes against a replica that has stopped reading.

**`repl_state.txt` carries `epoch=`, written the moment it changes** rather than on the ten-second
timer, since a crash between a failover and that tick is precisely when the number matters. This is
the only durable home a follower has: the WAL keeps it for a node that held the role, memory keeps
it across a role change, and nothing kept it across a restart. A file written before this change has
no such line and leaves the engine's epoch alone, which is the same downgrade property `stream_id`
has.

`ROLE` and `ob_current_epoch` on a replica now report the epoch it is following rather than 0. That
is a side effect of having one number instead of two, and it is an improvement on its own: a replica
answering `REPLICA <addr> 0` while following a primary in epoch 9 is misinformation an operator
reads during a failover.

**Write path unchanged, verified rather than asserted**: `apply_delta` (3 instructions),
`apply_delta_impl` (549, plus its 31-instruction cold clone) and `WALWriter::append` (90) are
instruction-for-instruction identical to master under `scripts/mnemonic_diff.py` in Release. The
tool refused the first run — "0 definitions in base — not a measurement" — because the base
worktree had been configured with `OB_BUILD_TESTS=OFF` and never built the engine archive. An
instrument that says "not a measurement" instead of "identical" is worth the line it costs.

Eight tests: the two halves of the guard with a control for each (a guard that refuses everything
passes the refusal test), the epoch outliving a role change and a crash-restart, the historical-record
asymmetry, the state file asserted **while the client still runs** so a build that only saves at
shutdown cannot pass, and a static test that the number has one home — because a re-introduced member
would be seeded correctly on the day it was written and go stale on the next role change somebody
adds.

Mutations: eight, all caught — but one of them survived its first pass and the reason was in the
test, not the engine. `epoch_lowerable` relaxes `while (epoch > known)` to `while (epoch != known)`,
which lets a historical record talk the number down; the test that should have caught it asserted
the epoch **after** an epoch-9 record that raised the value back, so it measured the recovery
instead of the damage. Moved between the two records, it kills the mutation and nothing else does.
Worth keeping as the general form: an assertion placed after the next message tests whether the
system repairs itself, which is a different question from whether it broke.

Noted while reading, not filed as an item because it is a test rather than a defect in the engine:
`WireProtocolEpoch.EpochFencingCorrectness` asserts `msg >= local` equals `msg >= local`. It is a
tautology, true of every implementation including one with no check at all, and it is why this
fencing had no behavioural coverage until now. The four new behavioural tests are its replacement in
substance; the property test is left where it is, since deleting it is a separate change to a file
this item did not otherwise touch.

- Effort: S | Impact: epoch fencing on the replication link was inert on the first connection, which
  is every connection after a restart or a role change



### 102. A node that cannot listen leaves by `terminate` rather than by a message and exit 1 ✅

Found in CI on PR #91, where a node whose client port a previous test still held printed

    terminate called after throwing an instance of 'std::runtime_error'
      what():  bind() failed on port 40739: Address already in use

and the harness reported `node exited with -6`.

The message was already right. The **exit mode** was wrong, and the exit mode is the half a
supervisor reads: systemd logs `Main process exited, code=dumped`, applies whatever its policy says
about crashes, and may write a core file — for a configuration mistake. An operator seeing that
starts debugging the engine rather than freeing the port. Exit `-6` is also indistinguishable from a
real crash, which is the confusion #88 was about from the other direction.

`TcpServer::run()` throws for **eleven** startup conditions across the two transports (socket,
setsockopt, bind, listen, `epoll_create1`, `epoll_ctl`, and `io_uring_queue_init` with its own four),
and `main()` caught none of them. **The contrast is the argument**: `load_secrets_or_exit()` and
`load_tls_or_exit()` are named for what they do, print `Error: <what>`, and exit 1. Eight refusals at
startup already behave this way. The listen path was the outlier.

**A `catch` alone would not have fixed it, and that is the part worth keeping.** The shutdown monitor
is a local `std::thread`, and one that is still joinable when its destructor runs calls
`std::terminate` — so a catch block returning 1 would have returned straight past the destructor
that aborts. Both mechanisms were live at once here: the missing catch, and #88's joinable thread.
`MonitorJoin` joins on every exit path including an unwind, and is declared after the thread so it is
destroyed before the server the thread refers to.

Its flag is **separate** from `g_shutdown_requested`, and the reason is what the log says: reusing
the shutdown flag made a failed bind print `Shutdown requested — the epoll loop will drain and close`
on its way out, which reads as an operator having signalled a node that never started. A line
announcing something nobody asked for is the same defect as a line announcing a guarantee the code
does not give.

**Measured after the fix**, on a port held open by the test rather than raced for: exit `1`, one
`Error: bind() failed on port N: Address already in use`, no `terminate`, and no shutdown line. The
ordinary path still exits `0`.

**Two throw sites are pinned rather than one**, because the fix is a contract about how the program
ends and not a catch around one call: the client port from `TcpServer::run()`, and the replication
port from `ReplicationManager::start()` inside `Engine::open()` inside `run()` — which unwinds a
partially opened engine, a state no destructor had ever run in, because the process used to abort
first. **The third test is the control**: `exit 1` plus a message about a port is also exactly what a
thoroughly broken server produces, so the same binary has to start on free ports, answer `PONG`, and
still exit `0` on `SIGTERM` — the half most easily broken by adding a catch.

Mutations: five, all caught — no `try`/`catch`; the catch with the join guard removed (this is the
one that says the catch alone is insufficient); the guard reusing the shutdown flag; the guard
joining without setting its flag, which hangs and dies on the test's own deadline; and narrowing
`ob_cli`'s handler to `std::runtime_error`, which the static guard names by filename.

**Both entry points, and a static test so it is a closed class rather than a fix applied twice.**
`ob_cli` had the same shape — `Engine::open()` throws when it cannot open the WAL, which an
unwritable data directory produces — and nothing supervises that tool, so the exit code matters less
there; what matters is that a repository which has paid three times for "the fix exists and is used
at one of two sites" does not do it again. `CliConfig.EveryEntryPointRefusesToStartRatherThanAborting`
enumerates `tools/` rather than naming the two files, and carries the pair that stops it passing by
finding nothing: at least two entry points must have been examined. Measured: `ob_cli /proc/nope`
exits `1` with `Error: filesystem error: cannot create directories…`, and an ordinary session still
exits `0`.

**Deliberately not changed:** a taken `--metrics-port` is logged as an error and the node starts
anyway, so a node whose monitoring is blind looks healthy. That is a different decision — which
conditions should refuse at all, rather than how a refusal exits — and it is named here rather than
folded in.

- Effort: S | Impact: A configuration mistake was reported to the supervisor as a crash, so restart
  policy and post-mortem both treated it as one

### 101. A replica that restarts wipes its store and re-syncs from zero ✅

Found while writing #98's integration test, which had asserted the opposite and passed anyway.

`demote_to_replica()` cleared `stores_`, `buffers_` and `pending_rows_`, deleted every columnar
segment directory on disk, deleted `repl_state.txt` and started a `ReplicationClient` at position
zero. The comment gave the reason, and the reason is sound for the case it names: "the node was
previously PRIMARY with its own data, and the new primary may have different data".

The case it did not name is the ordinary one. A failover-managed node restarts, its
`FailoverManager` reads etcd, finds a primary and calls the same function — so a node that was
already a replica of that same primary, holding exactly what that primary sent it, threw all of it
away and streamed the whole WAL again.

**The strongest number is not about time or bytes.** In the window the wipe creates, the replica
answers reads with `OK` and a partial result, and never refuses. Measured, 23 501 rows per symbol:

| t from restart | reply | rows |
|---|---|---|
| 0.72 s | `OK ob_tcp_server v0.1.0` | **1** |
| 3.92 s | `OK …` | 22 501 |
| 7.13 s | `OK …` | 23 501 (complete) |

Zero refusals, zero `ERR`, zero empty replies: for **6.4 s** the replica served reads missing
**99.996 %** of the data, reported as success — with all 23 501 rows on its disk a second earlier.
Not an outage a client notices; silent incompleteness. And the window is manufactured rather than
inherited: `engine_->open()`, WAL tail replay included, runs in `TcpServer::run()` **before**
`::bind()`, so a node that keeps its data is complete in its first reply.

**The second number is cumulative.** The replica appends every replayed record to its **own** WAL,
so each restart adds a full copy of the primary's log: ×2.00 in four runs out of four, N restarts
giving N+1 copies, bounded only by retention.

Before and after, same machine (i3-7100U, Debug, two-node `ClusterManager`, restart with no role
change):

| | before | after |
|---|---|---|
| columnar files | 2261 deleted | unchanged: 252 → 252, and 245 → 245 in the battery run |
| replica's own WAL | ×2.00 (a full copy added) | +0 B, against a 469 930 B primary log |
| first reply after restart | 1 row of 23 501 | 2401 of 2401 |
| records re-streamed | the whole store | **2**, at both a 33-record and a 97-record store |

That last row is the item in one line: the work after a restart is two records at either size —
the checkpoints the primary appended while the node was down — where it used to be everything. The
file count is reported as *unchanged* rather than as a figure, because the figure moves with flush
timing between runs and only the direction is the claim.

**The fix is not "skip the wipe on a restart", and the distinction this item originally proposed was
wrong.** The text here used to say the node has "a WAL identity (`wal_identity`) and a saved
position, which is exactly the pair that says this is the same stream I was following". It is not:
`wal_identity` names **this node's own** WAL, while the saved position indexes the **primary's**.
The pair says nothing. So the primary announces the identity of the WAL it writes, in answer to a
`STREAMID?` that carries no position, and the replica saves that next to the position and compares
them on every connection. A match resumes; anything else discards and replays from zero.

The question goes out **before** the position, and that ordering is the design. A pre-#101 primary
lands in its "unknown message — ignore" branch and sends nothing, so no record is in flight while
the replica decides — no apply gate, no re-handshake on a busy socket, no ordering to reconcile. The
decision also stops depending on which of the four callers demoted the node, which matters because
that list grows: the condition is a property of the data, and a static test pins that nothing about
the caller reaches it.

Resuming also needs over-delivery to be harmless, because a saved position is written on a timer
and a primary may re-send what a replica already applied. So the replication link drops a record
whose `(symbol, origin, sequence)` it has already seen — **gated by the caller, not by the value**:
the C API and the Python client pass sequence numbers of their own, so a guard keyed on "the number
is non-zero" would have silently deduplicated embedded writes. That coupling produced this item's
worst defect, in code this item added: the wipe cleared the store and **kept the sequence
frontier**, so every record replayed into the empty store was dropped as a duplicate. Measured, 0
rows where 1 was replayed, and closed as a class by a static test rather than by two fixes.

**What it costs the primary's write path is five instructions, and they have addresses.** The dedup
guard sits in a body both entry points share, so the policy argument is parked in the prologue
(`mov %ecx,%r12d`) and tested once (`cmp $0x1,%r12d; je`) — not taken for a client write, because
the guard's body is out of line and a client write never reaches the `has_seen()` call site. The
function grew 504 → 551 instructions in Release, of which one 33-instruction block is that
out-of-line body. `WALWriter::append` is identical, instruction for instruction;
`ReplicationManager::broadcast` has the same sequence with six shifted field offsets, because
`ReplicationConfig` grew by eight bytes. Measured with `scripts/mnemonic_diff.py`, which also showed
why the obvious comparison is wrong here: `apply_delta` reads 503 → 3, true and silent about the
work, because the body moved to `apply_delta_impl` and left a tail jump.

One more defect of this item's own making, found by reading it back rather than by a test:
`STREAMID?` is the first message on this link that a peer can repeat and be answered every time,
and `enqueue_send()` has no ceiling of its own. A peer that asked and never read grew that
connection's send buffer without bound — #69 in a new place, 10 bytes in for 29 bytes of memory
out. It now hangs up at the same ceiling as every other sender here.

**What this does not give, and the first one is the case most people will expect it to cover.**
**A failover is still a full re-sync for every replica, and that is correct rather than
unfinished.** Two reasons, and only the first is about bookkeeping: the identity belongs to a data
directory, so a promoted node's WAL is a different stream and the position a replica held in the old
primary's log indexes nothing in the new one — resuming across a promotion would need a *logical*
position, a sequence vector rather than a byte offset. But even with one, the replica could not keep
what it holds: the promoted node may be **behind** it (#70's election prefers the replica furthest
ahead precisely because the losers' extra records are lost), so a replica keeping its own suffix
would serve records the new primary does not have. Dedup makes over-delivery safe; it does not make
a divergent suffix safe. So the wipe on a role change is the same wipe as before, now reached
because the identity differs rather than because the function always did it — and this item buys
the restart, not the failover.
Then: the saved position is written on a ten-second timer, so even a restart can re-stream up to a
window of writes — bounded by time rather than by the store, which is the whole change, but not
zero. Cross-version upgrades cost one socket timeout per connection attempt against a primary that
does not know the command. A node that held PRIMARY and accepted writes discards everything, by
design and without being asked whether it did: `promote_to_primary()` deletes the position, so
there is nothing left to match. And restoring a primary from a backup draws a new identity, so it
costs every replica a full re-sync — correct, and worth knowing before the restore rather than
after.

- Effort: M | Impact: a restarted replica keeps its store, and the window in which it served
  incomplete reads reporting success is gone


**Correction, 11 September, to the ceiling test this item added.**
`APeerThatAsksAndNeverReadsIsDroppedRatherThanBuffered` asserted that the test's own `send()` fails
— and that is a **symptom**, not the property. The property is that the connection ends, and from
outside it shows three ways: a send that fails, a read that returns zero, or the peer's FIN arriving
while nobody is reading. On this machine the drop lands *after* the send loop finishes, so the test
failed **eight times in a row** against a binary byte-identical to master's, in runs whose own log
said `disconnecting replica fd=7: not draining its stream-identity answers`. It had passed three
times earlier the same day. A probe printed `replicas=0` with `hung_up=0`: the record was gone and
the sender had not noticed, which is the whole defect in one line.

It polls `POLLRDHUP` now, and that choice is load-bearing rather than stylistic: **reading the
answers would make this socket a well-behaved peer**, which is the one thing this test must not
become. Eight of eight afterwards. Same shape as pitfall 54 — an assertion on an ordering between
two independent things — and the engine was right throughout.

### 100. A record broadcast between `accept()` and the handshake is delivered twice ✅

Measured while writing #98's tests, which is how it was found: a live-path test that broadcast
records right after connecting saw fourteen where twelve were sent, and the two extra were not
noise.

`accept_replica()` pushes a `ReplicaInfo` into `replicas_` as soon as the socket is accepted, before
the `REPLICATE` line has been read. `broadcast()` walks every entry in `replicas_`, so a live write
in that window is queued to a replica that has not asked for anything yet. Then the handshake
arrives, `handle_catchup()` fixes the cursor's end at `wal_.current_position()` — which is *past*
those records, because the append preceded the broadcast — and the catch-up sends every one of them
again.

**Measured**: ten records broadcast between the accept and the handshake, and the stream carried
**twenty**, in the pattern 1..10 followed by 1..10. Exactly one non-increasing step in the sequence
numbers, which is what one clean repeat of the whole batch looks like.

The answer is a **drop, not a queue**, and that is what makes this a different defect from #99
rather than the same one twice. Every record broadcast before the handshake is necessarily inside
the catch-up range: the append happens before the broadcast, and the range ends at the WAL position
read when the handshake is processed. So the catch-up will deliver it, and holding a copy for later
is what produces the second one. (A handshake that asks from a position *ahead* of such a record is
also fine to drop: the replica already has it.) What #99 needs instead is a queue, because a
snapshot transfer's records are not in a range anybody is about to stream.

The replication path has no sequence check — `engine.cpp` says so about the multi-master path, where
there is one — so a duplicate delta is applied twice. Whether that produces duplicate rows depends
on flush timing, and that half is not measured.

**`live_record_is_needed()` is one question with the answer in one place**, because a replica can be
mid-transfer in more than one way and `broadcast()` should not learn them separately. It is
answerable at all only since **#98** gave the live path the record's own position — before that
there was nothing to compare, which is a dependency worth naming: the field that was wrong and
load-bearing nowhere turned out to be the field this fix is built on.

Its second condition covers a narrower case of the same defect that was not measured separately: a
record appended *before* a cursor was created but broadcast *after* it. That needs the handshake to
be processed between an append and its broadcast — one mutex hand-off wide, so narrow rather than
absent, and the comparison costs nothing. `wal_position_before()` is a named function rather than
`operator<` because positions from two different WAL directories are not comparable; that is what
**#61** established, having compared byte offsets across independent WALs and lost records.

**What the fix gives up, named:** `continue_catchup()` abandons its range if a WAL file in the
middle is missing (`stopping at file N`). A record dropped here would then not arrive by either
path — where before it would have arrived through `pending`. The records between it and the gap
would not have arrived either, so the replica has a hole in both worlds; it is not a regression in
kind, and the file it needs is one nothing else deletes while a replica is behind.

**The tests are a pair, and the second is what makes the first a fix rather than a silence:**
dropping the live copy is only correct while a catch-up is going to deliver it, so a live record
*after* the cursor finishes still has to arrive. The accept window is entered by polling
`replica_states()` rather than by sleeping — the protocol's own signal that the accept has happened
and the handshake has not. There is no integration test: the harness's replica sends `REPLICATE`
immediately on connecting, so the window is not reachable from outside the process, and a test that
cannot enter the window would be asserting about something else.

**Mutations: five, all caught — one only after the ordering got a test of its own.**

| # | mutation | caught by |
|---|---|---|
| 1 | the handshake flag is ignored | `ARecordBroadcastBeforeTheHandshakeArrivesOnce` |
| 2 | the flag is set at `accept()` instead of at `REPLICATE` | same test |
| 3 | every live record is dropped — the fix taken too far | 7 tests, led by `ARecordBroadcastAfterTheHandshakeStillArrives` |
| 4 | the range comparison is inverted | 2 tests |
| 5 | `wal_position_before()` drops the file index | **survived**; now `WalPosition.TheOrderIsFileFirstAndThenOffset` |

Mutation 5 is the reason that ordering has a direct test. It only shows up for a record in an
earlier file with a larger offset than the boundary it is compared against, and no behavioural test
reaches that shape — the cursor ranges they build do not straddle a boundary at the moment a live
record is compared against one. Nothing about the contract needs a socket, so it is checked as a
contract.

- Effort: S | Impact: a replica bootstrapping while the primary takes writes applies part of its
  catch-up twice


### 99. A live write during a snapshot transfer is spliced into the snapshot's byte stream ✅

Named by #93 rather than fixed by it: the mechanism #93 built for the catch-up stream is the one
this needs, but they are different streams and a fix that changes both in one commit is a fix whose
measurement covers neither.

`broadcast()` walks every entry in `replicas_` and there is no branch on
`snapshot_transfer.active`, so a replica being bootstrapped receives live `WAL ...` records
interleaved into a stream of `SNAPSHOT_FILE` headers and raw file bytes. On the replica side
`request_and_receive_snapshot()` reads a header with `read_line()` and then exactly `file_size`
bytes with `read_exact()` — so a record that lands between two files makes the next `read_line()`
return `WAL 3 0 4888 7` where `SNAPSHOT_FILE` was expected, and the bootstrap is abandoned; a record
that lands *inside* a file's bytes becomes file content, and the transfer fails its CRC instead.

**Measured**, which this item said had not been done. A mock primary splices one live record into
an otherwise valid two-file snapshot stream, and the outcome is read off the replica's data
directory rather than off a log line: **nothing is installed** — including the file that arrived
intact before the splice, because the install happens at `SNAPSHOT_END`. The abandonment now says
what arrived: `snapshot bootstrap abandoned after 1 of 2 file(s): expected a SNAPSHOT_FILE header,
got 'WAL 0 4888 136'`.

**The control test earned its place immediately.** Its first version failed too — `SNAPSHOT_END`
carries the CRC32C of the manifest the replica assembles from the headers it received, and the mock
primary had sent a bare `SNAPSHOT_END`. Without a control, the spliced test would have been passing
by not finding anything, which is the standard failure mode of a test that asserts an absence.

**The heartbeat is the same defect with a five-second timer instead of a write, and this item did
not name it.** That loop walks every replica with no branch on the transfer either, so any snapshot
transfer lasting longer than five seconds is spliced with `HEARTBEAT <epoch>` **without a single
client write**. The reason nobody had seen it: nothing in the integration battery bootstraps a
replica by snapshot over the replication protocol at all, and the unit tests that exist cover the
receiving side (paths, CRC) rather than the sending side.

**The decision became one pure function with three answers**, in the header so the cases a socket
cannot reach are checked directly:

| answer | when |
|---|---|
| `Drop` | no handshake yet; inside a catch-up range; or before the snapshot's own WAL position — already inside the files being installed |
| `Defer` | a transfer is streaming and the record is at or past its boundary |
| `Send` | nothing in progress |

`pending` moved off the catch-up cursor to `ReplicaInfo::deferred_live`, because it now holds bytes
waiting for either transfer: one buffer, one release function, two callers.

**The primary-side test is the first in this repo to drive a real `ReplicationManager` through a
snapshot transfer**, and it is deterministic rather than raced: its socket never reads, so the
transfer stalls with `active` still true, and `snapshot_active()` is the signal rather than a sleep.
Two numbers had to be measured to make that work. `continue_snapshot_transfer()` backs off at half
of `MAX_SEND_BUF_SIZE`, so at 12 symbols the **7.3 MB** snapshot streamed in one pass and the window
never existed; 24 symbols make **14.6 MB across 208 files** and it stalls. Then the assertion about
the deferred record failed for a reason worth recording: it used `recv_wire_records()`, which starts
from an empty buffer and reads the socket, while the record's bytes were already in the test's own
buffer — the "reader with a buffer of its own" mistake from the other side.

**Mutations: six, all caught — two only after the suite grew a mechanism, and one of those was a
comment of mine being untrue.**

| # | mutation | caught by |
|---|---|---|
| 1 | the snapshot is not a transfer, so live records go into its byte stream | 2 tests |
| 2 | the snapshot branch answers `Send` instead of `Defer` | the primary-side test |
| 3 | the snapshot's end never releases what waited | the primary-side test |
| 4 | the snapshot boundary comparison is inverted | 2 tests |
| 5 | the release happens before `active` is cleared | **survived** — and correctly: the comment claiming that ordering was load-bearing was wrong, because the release goes through `enqueue_send()` directly. The comment changed, not the code |
| 6 | the heartbeat is routed around `queue_to_replica()` | **survived**; now a structural guard — `run_loop()` may not call `enqueue_send()` or `enqueue_and_flush()` at all |

**Seven silent early returns in `request_and_receive_snapshot()` now log.** A bootstrap that is
abandoned means a replica with no data, and it could happen for seven different reasons without a
single line — including the one #99 takes.

**And the new test found a data race older than this item, which is the argument for writing it.**
`set_engine()` stored a plain pointer while `publish_replica_gauges()` read it on the epoll thread
under `mtx_`. Both production callers set the engine *before* `start()`, so nothing had reported it;
the first test to set it the other way round made ThreadSanitizer say so, on this machine and
independently in the `sanitizers (tsan)` check on the pull request. The setter takes `mtx_` now —
every reader of `engine_` was already holding it — and the fixture sets the engine before `start()`,
which is the order production uses.

- Effort: S | Impact: a replica can be bootstrapped from a primary that is taking writes, which is
  every primary worth bootstrapping from


### 98. The WAL position a replica is told is not the position of the record it is told about ✅

Named by #93, and larger than the item that found it. Its own text claimed a dropped replica
"resumes from the position it confirmed, so progress is monotonic". It does not.

`send_to_replica()` writes `WAL <replica.confirmed_file> <replica.confirmed_offset> ...` — the
position this replica last **acknowledged**, not the position of the record being sent. `broadcast()`
is worse: it writes `WAL <current_file> 0 ...`, a literal zero. The replica does
`confirmed_offset = byte_offset + total_len` and saves that to its state file, so:

- **During a catch-up** the field never moves, because the ACKs that would move it are read by
  `handle_replica_data()`, and that is the function the synchronous pass was running inside.
  **Measured**: every one of 112 records delivered before the drop carried `file=0 offset=0`. The
  replica therefore recorded `24112` — one record's worth — no matter how many it received, and a
  reconnect resumed one record along. The 16 MB steps this was assumed to make were one record.
- **In steady state** every record says offset `0`, so a replica that restarts asks for
  `total_len` bytes into the current WAL file and is re-sent nearly all of it. Storage is
  append-only, so a re-applied delta appends its rows a second time (the note at `engine.cpp:718`
  says this about the multi-master path, where the sequence check stops it; the replication path has
  no such check).
- **WAL retention is gated on the file index, and that is why this has been survivable.**
  `Engine`'s retention pass takes `min(current_file_index, every replica's confirmed_file)` and
  truncates below it — it never reads the offset. And the file index on the wire is *correct* on the
  live path: `broadcast()` sends `wal_.current_file_index()`. During a catch-up it is stale in the
  safe direction — every record claims the file the replica asked from, so a replica catching up
  across files 3 to 7 keeps retention pinned at 3 until it is done. So the field that is wrong is
  the one nothing depends on, and the field something depends on is right. That is the shape of a
  defect that survives four phases of work: it is load-bearing nowhere.

**Both halves had to change together.** Fixing the catch-up alone makes a replica's position jump
to 24 MB and then back to `total_len` on its first live record — worse than a field that is
uniformly meaningless, because a position that moves backwards is one a retention gate can act on.
The wire format did not change: the field was already there and already `uint32`/`size_t`; what
changed is what is written into it.

**Correction to the paragraph this item used to carry.** It said the live path could take the
position from `wal_.current_position()` immediately after the append, "because the append and the
broadcast happen under one engine lock with nothing between them". The lock is not the problem —
**rotation is.** `append()` checks the threshold *after* the write and `rotate()` publishes
`{next_index, next_offset}` in one store, so for the one record per WAL file that crosses the
threshold the current position is already in the next file while the record sits at the end of the
previous one. Subtracting the record's length from it names a file the record is not in, and can
underflow. So the answer is the #96 lesson applied to the WAL: **a function that writes returns
where it wrote.** `WALWriter::append()` and `append_with_origin()` return the `WalPosition` of the
record they wrote; `broadcast()` and `send_to_replica()` take it; the catch-up cursor passes
`cur.position()`.

The five internal `append_*` stay `void`. Only those two write records that travel with a position
on them, and `append_version_vector()` can *refuse* to write, so it has no position to give — a
returned value nobody reads is the shape this project has paid for five times (`provisional`,
`basis`, `in_use`, `key_id`, `partition_by`). `append()` is not `[[nodiscard]]` either: forty
callers are right to ignore the position, and what enforces the rule is that `broadcast()` cannot
be called without one.

**Measured before and after** (i3-7100U, Debug):

| | before | after |
|---|---|---|
| catch-up across WAL files 0–6, 300 records | every record announced `file=0 offset=0` | each record announced at its own position |
| replica's reported offset after 40 rows | **136** — one record — against a WAL of 5472 | **5472**, one record behind the primary's own file |
| replica's `replayed` at that point | 41 | 41 |
| position persisted to `repl_state.txt` | `byte_offset=136` | `byte_offset=5472` |

**The tests assert the property rather than checking the positions against each other**, and that
distinction is the whole of them: a column of zeroes is self-consistent. Each one reads the WAL
file the wire named, at the offset it named, and requires the record there to be the one that was
sent. The integration half checks the replica's offset against the size of the primary's WAL file,
because that file is the only thing that knows how long the stream is.

**Mutations: nine, all caught — but one of them only after the test suite grew a mechanism it did
not have.**

| # | mutation | caught by |
|---|---|---|
| 1 | `broadcast()` sends a literal zero offset again | 2 C++ tests + the integration test (`offset 136`, 5428 bytes behind) |
| 2 | the catch-up announces `replica.confirmed_*` again | 4 C++ tests |
| 3 | the engine derives the position as `current_position() - total_len` | **survived 951 tests**; now the static test |
| 3b | `broadcast()` derives it internally instead of using its parameter | only `ARotatingAppendAnnouncesTheFileTheRecordWentInto` |
| 3c | the append's return is captured but a derived value is broadcast | the static test, on two of its three assertions |
| 4 | `write_record()` returns the record's end instead of its first byte | 2 C++ tests |
| 5 | `append()` returns the position after the rotation | 2 C++ tests |
| 6 | the cursor announces `through_file` instead of the file it is reading | 1 C++ test |
| 7 | the cursor advances before announcing | 4 C++ tests |

**Mutation 3 is the one worth reading.** The behavioural tests drive `ReplicationManager` directly,
so they pin what the manager does with a position and say nothing about which position the *engine*
chooses — and the derivation is wrong only for the record whose append rotated, which no test
rotates, because the threshold `Engine` hardcodes is 512 MB. A behavioural test would need that
threshold to become configurable for a test's sake. The mechanism is a static test over
`Engine::apply_delta` instead, and for this claim it is the stronger one: the engine may not
*compute* a position at all. Mutation 3b then shows the rotation test is not redundant — it is the
only thing that catches a derivation made inside `broadcast()`.

**The integration test's first premise was wrong, and the node's own log said so.** It asserted
that a restarted replica resumes instead of replaying. A failover-managed replica **at the time**
cleared its local data and re-synced from zero whenever it was told its primary — **including on its
own restart** (that is #101, closed since, and the premise is true now):
measured, `clearing local data before starting replication from 127.0.0.1:58169` followed by
`REPLICATE 0 0 0`, on a plain restart of a node that was already a replica of that same primary.
So the restart measured the wipe rather than the resume and passed for an unrelated reason. What
replaced it asserts the position, which is the thing #98 makes true. (Whether that wipe should
happen on a restart at all is a separate question, named as #101.)

**What this does not claim.** The earlier text said a re-applied delta appends its rows a second
time, so a `SELECT` returns each of them twice. Measured under the defect: it does not, not at that
point — the re-applied deltas update the same price levels in the live buffer and enqueue rows that
are only distinguishable after a flush, and `FLUSH` on a replica is refused (`ERR read-only
replica`). Re-delivery is real and measured; the duplicate rows are a second-order consequence of
flush timing that was asserted without being measured, and is not asserted now.

- Effort: S | Impact: a replica's saved position becomes true, so a reconnect resumes where it
  stopped instead of asking for a WAL file it already has, and retention is gated on a real number


### 97. One unreachable peer address stops every write on the node ✅ **P0**

The multi-master reconnect loop holds `mtx_` across the whole of its pass — the prune, the dial and
the gauges — and the dial is a **blocking** `::connect()`: `set_nonblocking()` comes after it, not
before. `connect_to_peer()`, the copy of that logic on the topology-change path, does the same
thing from its first line. `mtx_` is the mutex the io loop takes for every peer event and the one
`broadcast_local()` takes on the client write path, so while that connect is outstanding the node
accepts no peer connection, reads no peer frame, and **finishes no client write**.

A refused connection returns at once, which is why a healthy cluster and every existing test miss
this: a peer that is merely down refuses. What hurts is a SYN that goes nowhere — a firewalled
peer, a host that has vanished, a registry entry pointing somewhere unrouted — where the kernel
retries for `tcp_syn_retries` doublings. That is the failure a multi-master cluster exists to
survive.

Measured on i3-7100U, Debug (the number is a kernel timeout, not code speed), with
`tcp_syn_retries = 6` and one peer record whose address was `10.9.9.7:7100`:

| what was blocked | measured |
|---|---|
| an inbound mesh connection waiting for the node's handshake | **132.5 s** (floor of 132.5 / 132.8 / 134.7 across three runs) |
| one client write through `apply_delta_mm()` | **135.7 s** — one write completed in the whole run |

Found by accident, and the accident is the useful part: a unit test for #96 installed a peer record
with a made-up address, and the file became flaky **3 runs in 12** because its own node stopped
answering for over two minutes. `getaddrinfo()` is inside the same critical section, so a peer
address written as a hostname adds DNS resolution to it.

**Two of my own measurements were wrong before this one was right, in different ways.** The first
ran the write and the dial *in sequence* and reported 0 ms, having issued every write after the
connect returned. The second ran them concurrently and still reported 1 ms, because it called
`Engine::apply_delta()` — and on a multi-master node the server calls `apply_delta_mm()`, which is
the overload that broadcasts. A measurement of the wrong entry point exonerates the code in the
same voice it would use if the code were fine.

Not the replication link: `ReplicationClient::connect_to_primary()` blocks in the same way, on its
own dedicated thread and holding no mutex, so it delays that replica's own reconnect and nothing
else. Worth stating, because the code looks identical.

**The fix is three phases and one dial.** Decide under the lock, dial with it released, install
under it again — and the two copies of the dial (the reconnect loop's inline one and
`connect_to_peer()`) became one `dial_address()` plus one `finish_dial()`. The socket is put in
non-blocking mode **before** `connect()` rather than after, so the wait is ours to bound:
`MM_CONNECT_TIMEOUT_MS` is 5 s, which allows two SYN retransmissions, and a peer that misses it is
retried by the backoff instead of waited on. `poll()` reporting POLLOUT is not the answer to "did it
connect" — a refusal is also writable — so `SO_ERROR` is read even on the ready path.

Three things the lock release made necessary, each of which is a way to get this wrong:

- **The attempt is claimed before the lock goes**, not after the dial returns. Otherwise
  `next_reconnect_time` stays in the past for the whole dial and the loop opens a fresh connection
  to the same peer every 100 ms. This is #95's rule — every failure branch moves the next-attempt
  time — applied to a branch that now spans a lock release.
- **The record may be gone**: a topology change during the dial means the descriptor is closed and
  dropped rather than installed.
- **The peer may already be connected** through a link somebody else opened — its own second dial,
  or the peer dialling us and its handshake being adopted (#96). One link per peer, and the one
  already carrying traffic keeps it.

**What this deliberately does not do**, so the limit is named rather than discovered: the dial is
bounded and off the lock, but its completion is not driven from the epoll set, so a pass with N
unreachable peers spends up to N × 5 s on the dialling thread. That delays those peers' own next
attempts and nothing else — no client write, no peer frame, no shutdown beyond one deadline — which
is why it is a cost rather than the defect. Driving the connect from the same EPOLLOUT machinery the
writes already use would remove it too.

Three mutations, three caught, and they fail through *different* tests, which is what says the two
tests are measuring different things: putting the dial back under the lock fails the responsiveness
test; removing the deadline fails only the shutdown test (a node stopped mid-dial waited out the
kernel — which is why every measurement run of this defect took 133 s); claiming the attempt after
the dial fails the "exactly one attempt in flight" assertion.

- Effort: M | Impact: a client write on a healthy node waits out a dead peer's TCP timeout. P0 by
  consequence: the node is up, answering `PING`, and accepting nothing

### 96. The temporary key for an accepted connection lives in the node-id space ✅

`peers_` is keyed by node id, and a connection this node accepted was inserted under
`static_cast<uint16_t>(client_fd)` until its handshake said who was behind it. The comment above
that line described a different design — "use a high node_id range (fd + 10000) as temp key" —
which is the one that would have been safe. The code did not do it.

So an inbound connection landing on descriptor N silently replaced the live record of peer N: the
assignment dropped that peer's send buffer, lost its backoff and its advertised address, and left
its descriptor in the epoll set with no record behind it — the next event on it took the "unknown
fd" branch and closed it, so the peer saw a truncation. Nothing logged anything, on either side;
that branch warns now, because a descriptor in the epoll set with nothing behind it is never
routine.

**Measured before it was fixed, and how it had to be measured is the interesting part.** A cluster
cannot be the instrument: the collision needs a node id equal to a descriptor number, the
integration fixture numbers its nodes 1..3, and `--mm-node-id` accepts any `uint16_t` with no range
check — so a mesh numbered 1..3 is safe by accident and one numbered by rack position is not. A
test can do what a cluster cannot: install a peer record for **every** descriptor number the
accepted socket might get, which makes the coincidence certain rather than lucky. The connection
arrived on descriptor **8**, and the record of peer 8 was gone.

Three more numbers, from live meshes, because *reachable* and *reached* are different claims:
**14** replacements of an existing peer record across three multi-master integration modules —
every one of a record with no live socket, so what they destroyed was the advertised address and not
a connection; **0** orphaned descriptors; and **0** duplicate links even with all three nodes
launched at once, because peer discovery goes through an etcd watch whose latency is orders of
magnitude above a loopback connect, so the second dialler always finds itself already connected.
That last one is a fact about this deployment rather than about the protocol.

The fix is not the reserved range — that is still a node id, still in the same space, and still one
arithmetic slip from a live record. It is a separate container, `pending_`, keyed by `conn_id`:
minted once, never reused, and meaningless to every other subsystem, which is what a key needs.
Nothing there is broadcast to, dialled, counted or reconciled, and that removed the six
`node_id == 0` tests standing in for "is this record real?" — the MM_PEERS skip from #84, the
reconnect-loop cleanup from #95, and four in the io loop, three of which collapsed into one
`connection_lost()` stating the difference once: an identified peer keeps its record and takes
backoff, an unidentified connection is gone for good.

**Two further defects came out of writing the fix, both older than it.** The re-key did
`peers_.erase(peer_key); peers_[real_id] = std::move(moved);` and the io loop went on using the
pointer it had taken into the erased record — the EPOLLOUT branch below reads `peer_ptr->connected`
and drains through it, so an event carrying both EPOLLIN and EPOLLOUT read freed memory. The same
class as #92, found by reading rather than by a sanitizer, and impossible now by construction:
adoption *returns* the new location, so the caller has nothing stale to use. And a handshake could
claim node id **0** or this node's own id — the first would have left the connection in the
unidentified container for ever, connected and never adoptable; the second keys a record as us,
which broadcast then sends our own records to. Both refused, with the reason in the log.

Two live links to one node now resolve to one, and to the *same* one at both ends: the surviving
link is the one the lower-numbered node dialled, which each end evaluates from its own id, the
peer's, and which of the two it accepted. A rule that is not a function of exactly those three lets
each end close the link the other kept, leaving the pair with none — so the two tests for it are
the same situation seen from both ends, and flipping the comparison fails both.

- Effort: M | Impact: a live peer link is silently replaced, with no log line

### 95. The reconnect loop retried a permanent failure ten times a second ✅

`Reconnect: invalid peer address: ` in the log every 100 ms, for the life of the process, on a node
that had refused an inbound connection. Older than the change that surfaced it (#30 part three).

Two causes, both worth naming. Every failure branch in that loop moves `next_reconnect_time` and
takes the backoff — except the unparseable address, which simply continued, so a failure that would
never clear was retried at loop frequency and said so at loop frequency. And the record being
retried could not be dialled at all: a connection this node *accepted* is stored with no node id and
no address, because the port it arrived on is the peer's ephemeral source port. Once such a
connection closes before its handshake names a node, there is nothing to dial and nothing for it to
become — so it was one dead entry in `peers_` per refused inbound connection, kept for the life of
the process.

Measured on a three-node mesh with one node outside `--tls-peer-names`, over fifteen seconds: **0**
`invalid peer address` lines where there had been about 150 per node, and 15 dead records dropped
per node. A peer that completed its handshake but is not in the registry keeps its record — it can
still dial us — and is now logged at DEBUG with backoff rather than at WARN with none.

- Effort: S | Impact: a log line at 10 Hz is a log an operator cannot read

### 94. `ob_mm_peers_connected` never counted a connection the node accepted ✅

Found by the integration test written for series D's own gauge, the one that asserts the two mesh
numbers agree: `ob_mm_peers_tls_verified` **2** against `ob_mm_peers_connected` **1**, on a
three-node mesh where every link was mutually verified. An operator reads that gap as a peer talking
plaintext — the exact opposite of what was true.

The count was recomputed inline at three sites — `connect_to_peer()`, `disconnect_peer()` and the
reconnect loop — and none of them is `accept()`. The number was therefore right for peers this node
dialled and short by one for every peer that dialled it, which in a three-node mesh is consistently
half of them.

Both gauges now come from one `publish_peer_gauges()` over `peers_`, and the correctness does not
come from its call sites: it also runs once per reconnect-loop pass, so no state change anywhere can
leave either gauge stale for more than 100 ms, whichever of the twenty-odd places that move a peer's
state made it. The denominator is the one MM_PEERS uses — a connection accepted but not yet named by
its handshake is not a peer (#84) — so the view and the gauge cannot disagree either. A static test
refuses a second write site for either name: three copies of a count is how the fourth site comes to
be missing.

- Effort: S | Impact: the mesh's own guarantee metric read as a violation of itself

### 93. Catch-up above the send-buffer ceiling costs one reconnect per 16 MB ✅

Named by #30 part three series D rather than fixed by it, because the fix there was the one that
belonged with the change: every write to a replica now goes through `enqueue_send()` and the
EPOLLOUT drain, which is the only shape in which a socket saying "come back later" has anywhere to
say it — and the only shape in which `SSL_ERROR_WANT_WRITE` does.

What that left: `handle_catchup()` streamed the whole requested WAL range in one synchronous pass,
so the weight of that range became the depth of the send queue.

**Measured before the fix** (i3-7100U, GCC 13.3, Debug, 1000 records of 1000 levels = **24.11 MB**
requested, one replica that reads nothing for a second): the connection was dropped **61 ms** in,
`send_buf=16780544 > 16777216`, having delivered **112 of 1000 records**. That is 2.70 MB, which is
what this loopback pair absorbs before the sender's first `EAGAIN` — series D measured 2.6 MB for
the same reason, on the same machine.

**The title understated it, and this item's own text was wrong about why.** It said the replica
"resumes from the position it confirmed, so progress is monotonic — but a replica a gigabyte behind
needs sixty-odd reconnects to get there". Measured: **every record of a catch-up carries
`file=0 offset=0`**, because `send_to_replica()` puts the replica's own last-confirmed position on
the wire rather than the record's, and the ACKs that would move it are not read until the pass
returns. So the replica saves `0 + total_len` whatever it received, and a reconnect resumes one
record along instead of 16 MB along. The reconnect loop was not slow progress; it was **no
progress**. Filed as #98, which is the larger half of what this item was pointing at.

The fix is the cursor this item asked for: `CatchupCursor` on `ReplicaInfo` — which WAL file, where
in it, and where the stream ends — which is the shape `SnapshotTransferState` already gives the
snapshot stream, one function away in the same class.

**The design content is not the resuming, it is what resuming gives up.** A synchronous pass held
`mtx_` from the first record to the last, so `broadcast()` could not interleave and ordering was
free. A pass that stops and resumes has to buy that back, and there are exactly two ways:

- **Chase the live WAL end.** Rejected, and the reason is measurable: a record is appended and then
  broadcast under the engine's write lock, so while the cursor holds `mtx_` that `broadcast()` call
  is *waiting*. `::write()` puts the record in the file immediately, so the cursor reads it and
  sends it — and then releases the lock and the waiting call sends it again. Not a rare race: with
  a live write during the pass it is the outcome of every handoff.
- **Fix the end when the cursor is created, and hold live records behind it.** Taken. A record
  appended afterwards sits at or past that point, so the cursor never reads it; its `broadcast()`
  goes into `pending` and is released, in arrival order, when the cursor reaches the end. Arrival
  order is WAL order because both happen under the engine's write lock.

Two bounds, and they are different promises. The **queue** bound is half the ceiling, so the queue
is never grown to the size of the range — that is the one this item is about. The **batch** bound
(`kCatchupBatchBytes`, 1 MB) is about the mutex: `broadcast()` needs it, so a pass holding it for
the length of a range stalls every client write for that long, which is the shape #97 measured at
135 s on the mesh side.

**What a client write waits, measured at the entry point the write path uses** — `broadcast()`, not
`handle_catchup()`, because #97's lesson is that measuring the wrong entry point acquits the code in
the same voice it would use if the code were fine. i3-7100U, Debug, 24.11 MB range, ~6700 samples
per window, floor of three or more runs, with a control window on the same run:

| window | p50 | p99 | p999 | longest | replica survives |
|---|---|---|---|---|---|
| control, nothing running | 0.009 ms | 0.020 ms | 0.031 ms | 0.080 ms | — |
| **before**, receiver reading nothing | 0.003 ms | 0.009 ms | — | **49.4 ms** | **no**, 2.70 of 24.11 MB |
| after, receiver reading nothing | 0.004 ms | 0.019 ms | 0.048 ms | 19.5 ms | yes, all 24.11 MB |
| after, receiver draining | 0.009 ms | 0.021 ms | 0.041 ms | 12.0 ms | yes, all 24.11 MB |

The before/after percentiles are not comparable and saying so is the point: **before**, the replica
is dropped 61 ms in, so the rest of that window has no catch-up running at all — its p99 is low
because the work stopped. The comparable numbers are the longest wait and whether the range
arrived.

The batch bound's own worth is the last row, and it is smaller than expected: removing it moves the
longest wait for a **draining** receiver from 12.0 ms to **25.3 ms** (floor of two runs each) and
moves the non-draining case not at all (28.7 against 28.7), because there the queue ceiling already
bounds the pass. A measured factor of two, on the case where nothing else bounds it.

Resumption is single-sited: the run loop's per-pass tick, after that pass's EPOLLOUT drains have
made room, and **not** also in the EPOLLOUT branch beside the snapshot transfer's. A cursor has to
be resumed from three situations — the socket drained, the batch budget ran out with the queue
already empty, and the first batch was queued by `handle_catchup()` — and only one of those arrives
as an event. The loop's own timeout carries the rest: zero while some cursor has room to queue more,
100 ms otherwise. A cursor whose queue is *full* is deliberately not "room", because polling that
would be the busy-spin of pitfall 5, and EPOLLOUT is what says the socket drained.

The `COMPRESS LZ4` directive travelled with the cursor. "After the last plain byte of the catch-up"
used to be a line in `handle_replica_data()` after a pass nothing could interrupt; it is a moment
the cursor decides now. The records in `pending` were framed at broadcast time and are already
compressed if the replica asked for that, which is the other half of the same seam.

**Mutations: nine, seven caught, and the two that survive are pacing rather than correctness.**

| mutation | caught by |
|---|---|
| a live record is queued straight away instead of waiting | order of 1001, and the seam |
| no back-off at half the ceiling | the 24.11 MB catch-up |
| the file index does not advance at a file boundary | the seven-file range |
| a ROTATE record is streamed instead of ending its file | the seven-file range |
| what waited is dropped rather than released | order of 1001 |
| the directive is sent after the frames it frames | the seam |
| the cursor chases the live WAL end | order of 1001 — **after** the reader was fixed |
| **no batch bound** | **survived**: changes only how long one pass holds `mtx_`, and the measurement above is its whole defence. A test would be a wall-clock gate, which is what #10.7's own docstring warns teaches people to re-run until green |
| **the loop always sleeps 100 ms** | **survived**: changes only how fast a catch-up advances (one batch per tick, so 24 MB takes 2.4 s instead of 0.2 s). Nothing arrives differently, and the test for it would again be a clock |

**The mutation that chased the live WAL end survived its first run, and the reason is worth more
than the mutation.** The test read until the *expected* count and stopped — so the duplicate record
was sitting in the reader's own buffer, discarded when the function returned, indistinguishable from
one that was never sent. The reader now reads until the socket goes quiet and the assertion is exact
in both directions. A test that stops when it has seen enough cannot see too many.

- Effort: S | Impact: a catch-up of any size completes on one connection; a live write during it
  arrives after the history it follows, exactly once


### 92. A query holds a raw `SoABuffer*` across a snapshot install ✅

Named by #91 rather than fixed by it, because it is a lifetime problem and not a locking one.

`buffers_` owns the `SoABuffer`s and `live_ptrs_` points into them. `load_snapshot()` and the
snapshot-install path both **clear** them under `flush_mtx_` + `mtx_`, which destroys every buffer.
A query resolves its pointer under `mtx_` (#91) and then reads through it after releasing the lock,
so a snapshot install during a query frees memory the query is reading.

Latent today: a snapshot install happens on bootstrap and on a full resync, neither of which
overlaps steady-state querying in any test — which is exactly the shape of pitfall 22 and the
`set_read_only_flag()` finding, a raw pointer whose lifetime is nested by convention rather than by
construction.

**Measured before being fixed, because "latent" and "unreachable" are different claims.** A reader
thread issuing `SELECT VWAP(price)` against a loop of 600 snapshot installs reports
`heap-use-after-free` under AddressSanitizer in **3 of 3 runs** — on the seqlock version load inside
`read_snapshot()`, from the query thread, with the free in `load_snapshot()`. Reachable through the
public API by any replica that serves reads while it bootstraps or resyncs.

The answer is the first of the three that were filed: `LiveBufferLookup` returns
`std::shared_ptr<SoABuffer>` and the query holds it for its own length, so a buffer cleared out of
the map mid-query stays alive until that query drops it. Such a query answers with the contents it
started with, which is what any query gets when a write lands after it began. The cost is one atomic
increment per query and **nothing on the write path**, which resolves its buffer under the same lock
it writes beneath — the accounting in the original note had this backwards. The third candidate,
holding `mtx_` across a whole query, stays the one to avoid.

`live_ptrs_` went with it. It was a raw-pointer index of the same keys as `buffers_`, populated and
cleared in the same three places; one map cannot disagree with itself.

The type change is what made the fix complete: the compiler required both suppliers of the lookup to
be visited, the server's in `src/engine.cpp` and the embedded path's in `src/c_api.cpp`. Nothing
clears the C API's map today, so the defect was the server's alone — but a type that is only
accidentally safe is the shape that produced this item in the first place.

The test that drives the race passed for the wrong reason first: `SELECT *` resolves the buffer for
an existence check and **never dereferences it**, so three clean ASan runs said nothing at all. The
aggregation branch is the one that reads through the pointer, which is also why #91's test picked
VWAP.

**The cost, by disassembly rather than by stopwatch** (`scripts/mnemonic_diff.py`, i3-7100U, GCC
13.3, Release, `9376d39` against the fix):

| Function | master | fix | reading |
|---|---|---|---|
| `Engine::apply_delta` | 501 | 501 | same instructions; 29 operands differ, all member offsets |
| `Engine::apply_delta_mm` | 597 | 597 | same, 34 offsets |
| `WALWriter::append` | 88 | 88 | identical |
| `QueryEngine::execute` | 2197 | 2224 | +27: the handle's stack slot and its lifetime |

Offsets moved because the map's value type grew from 8 bytes to 16. The atomic is countable and was
counted: **0 → 1 lock-prefixed instruction** in `QueryEngine::execute`, and none in the write path
or in either cold clone. No wall-clock number is quoted for the ingestion benchmark, because it
measures `apply_delta` and `WALWriter::append` — and an instruction-for-instruction identity is a
stronger statement about those than a timing on a machine that has produced ±40% for an unchanged
function.

- Effort: M | Impact: removes a reachable use-after-free on the read path


### 91. A `SELECT` racing the creation of a symbol's live buffer ✅

**Found by ThreadSanitizer on the integration battery, exposed by a test written for #30 and not by
that change.** The new authenticated multi-master test polls `SELECT` on all three nodes while a
record propagates, so on the two receiving nodes a query ran exactly while `apply_remote_delta`
created that symbol's buffer. Five reports in one run, all the same pair.

- `Engine` owns `live_ptrs_`, a `std::unordered_map<std::string, SoABuffer*>` inserted into under
  `mtx_` by **every** write path — a client write, the replication apply path, the multi-master io
  loop. `QueryEngine` held a **reference** to that map and read it with no lock at all: `count()` at
  `query_engine.cpp:670` and a second, independent `find()` in the aggregation branch. An
  `unordered_map` insertion rehashes, so a concurrent reader can follow a bucket that has moved.
- **Reachable from a plain client `SELECT`**, not only in multi-master: any node taking writes for a
  symbol it has not seen before is enough. It needed the query and the *first* write for a symbol to
  overlap, which is why the existing tests — which write, then read — never produced it.
- **The same defect was one file away, in the C API**, which is the embedded path the Python client
  uses locally: `ob_insert` creates buffers under `mtx` and `ob_query` read the map without it.
  Fixing only the server would have left it.
- Fixed by handing `QueryEngine` a **lookup callable** instead of the map. `Engine`'s implementation
  takes `mtx_` for the duration of one map read and releases it before the query runs — one
  uncontended lock per query rather than holding the write path's mutex across a scan, which in this
  engine would be the worse trade. It also collapses the two lookups into one, so a query can no
  longer see a symbol exist in one and not in the other.
- Two tests failing in different directions: one drives the race, and one **refuses the shape** —
  because a behavioural test for a rehash race is probabilistic and a shape test is not. The shape
  test also asserts that *both* suppliers of the lookup take a lock, by extracting the callable from
  each file by brace matching rather than by looking for the word "lock" somewhere in it.
- **Still open, and named rather than fixed here:** `live_ptrs_` and `buffers_` are *cleared* when a
  snapshot is installed, and `buffers_` owns the `SoABuffer`s. So a query holding a resolved pointer
  across a snapshot install would read freed memory. Today the window is narrow and this fix does
  not widen it, but it is a lifetime problem rather than a locking one — item **#92**.
- Effort: S | Impact: **removes undefined behaviour on the read path**


### 90. A running node could not be asked its version, and the banner that carried it lied twice ✅

- **There was no way to ask a running node what version it is.** Not `--print-config`, not `STATUS`,
  not `/metrics`. The only occurrence of the version anywhere in the C++ was a hardcoded literal in
  `tools/ob_tcp_server.cpp`. For a database an evaluator is deciding whether to trust, "which build
  is this node running" was a question with no answer.
- Found while writing the comparative harness for #39, which records the version of every system it
  measures beside the numbers — that is the whole point of its requirement 2.1. It can read
  ClickHouse's from `SELECT version()` and ours from nothing, so its results file said "unreported
  (the server has no way to report its version)". That sentence was the honest artefact and the
  argument for this item.
- **The banner was printed before the server bound.** `std::printf("... listening on port %u ...")`
  ran before the `TcpServer` was constructed, so it announced listening that had not happened; a
  bind that then failed for a taken port left the output claiming to listen with the error
  underneath it.
- **And it was never flushed.** It went to `stdout` via `printf`, which is block-buffered when
  redirected to a file, a pipe or a journal — so the line arrived at **process exit**. Every other
  line was on time because the logger writes to `stderr`, unbuffered. Measured: the banner was
  absent from a node's log file while the node was up and answering, and present after it stopped.
  The one line an operator greps to confirm a start was the last one to appear.
- **Fixed in all three parts, and verified on a live node rather than by reading.** The version
  reaches the binary from `project(... VERSION)` through a compile definition, so the C++ has one
  copy and `ob::version()` is the only way to get it. The startup line says **starting** and is
  flushed; the line reporting a working socket is logged by the server after the bind and the listen
  have both succeeded. And the version is askable three ways: `STATUS` gains a `version:` key/value
  line — not a column, so no client parsing the tab-separated table has to change — and `/metrics`
  gains `ob_build_info{version="…",node_role="…"} 1`, the conventional shape, which is what lets a
  monitoring system tell an old binary from a new one across a fleet.
- Checked by starting a node and asking it: the banner is in the redirected file **while the node is
  up** rather than after it exits, the logger's `listening on port …, version 0.1.0` line is there,
  `STATUS` answers `version: 0.1.0`, and `/metrics` answers `ob_build_info{version="0.1.0"} 1`.
- **Two drift guards, both mutation-checked.** `pyproject.toml` still carries its own version,
  because a wheel's metadata cannot be a C++ macro — a test holds the two in step, and another
  refuses the version as a literal in any of the four sources that report it. A literal which agrees
  today is one that drifts at the first bump, and the symptom is an operator told the wrong build is
  running, which is worse than being told nothing.
- Effort: S | Impact: an operator could not tell which build was running, and the line saying the
  server was up was neither true when printed nor visible when needed

### 89. A graceful handover demotes the outgoing primary twice ✅

- **A race window, not stale bookkeeping, and the first version of this entry got that wrong.** The
  handover does store `NodeRole::REPLICA` — `src/failover.cpp:346`. The problem is the order around
  it: `revoke_lease()` makes the leader key disappear *before* `role_.store(REPLICA)` runs, so a
  monitor pass landing between the two sees "we hold the PRIMARY role and the leader key is gone".
  That is true, and it is the unconditional demotion #82 added on purpose, so it demotes a node
  which is a line away from demoting itself. Read from the source rather than inferred from the log,
  which is what corrected it.
- Harmless since #88, because both demotions are now idempotent: the second finds the replication
  objects already gone and re-sets a role and a flag that already hold. Before #88 it was the second
  caller that aborted the process.
- It is still worth fixing, for two reasons that are not the crash. It is work done twice on a path
  where the point is a quick, clean handover. And the log of a **planned** operation reads like a
  fault: `WARN we hold the PRIMARY role but the leader key is gone`, then `WARN lease lost, demoting
  to REPLICA`, then `WARN no new primary is published yet`. An operator who runs `FAILOVER` and reads
  three warnings has been told something went wrong, and nothing did.
- The fix is to make the expected disappearance distinguishable from the unexpected one: the
  handover knows it is handing over, so a flag set before the revoke and cleared after the role
  store lets the monitor loop tell "the key is gone because I gave it away" from "the key is gone
  and I did not expect that". Only one of those is a fault.
  Reordering — storing the role before revoking — is the obvious alternative and is worse: the
  revoke has an explicit "staying primary" path on failure, so the role would have to be put back,
  which is the same window pointing the other way.
- **Fix: `handing_over_`, set through a scope guard, and the guard is the load-bearing part.** It is
  true only between revoking our own lease and recording the new role, and the monitor loop's
  "Absent" branch treats the key's disappearance as expected while it holds — logging at INFO that
  the handover demotes this node itself, and doing nothing. A flag that suppresses a safety check
  must be impossible to leave set, and `initiate_graceful_failover()` has **seven** return paths,
  one of which keeps the role when the revoke fails. So it is RAII rather than a pair of stores: if
  the handover dies after revoking, the guard clears on unwind and the next pass demotes, and the
  net #82 added is still there.
- **The flag alone was not enough, and reasoning about the mutation is what found that.** The
  monitor reads `role_` at the top of an iteration and reads the leader key later in the same one,
  with an etcd round trip in between — so a handover that starts *and finishes* inside that gap
  clears the flag before the Absent branch runs, and the branch then acts on a `current` that still
  says PRIMARY. Two windows, and neither guard covers the other: the flag is for a handover in
  flight, and a **re-read of `role_` immediately before stepping down** is for one that completed
  while we were asking etcd. Acting on a stale role is the actual defect; the flag was treating a
  symptom of the narrower half.
- The TTL clock rule alongside it needs no change: a handover completes in milliseconds, orders
  below a lease TTL, so it cannot reach that threshold. Checked rather than assumed.
- **The integration test catches it about one run in three, measured — so it is the backstop and
  not the proof.** Three runs against a build with both conditions disabled: one failure, two
  passes. A test that waits for a one-in-three race reads as flaky and gets a re-run instead of a
  reading, which is the lesson the probabilistic salt test taught in the sibling repository. So the
  decision moved into `decide_on_absent_key(role_now, handing_over)` — pure, next to
  `decide_election()` and for the same stated reason — and its six combinations are one assertion
  each with no cluster. Both mutations are caught deterministically and by *different* tests:
  disabling the flag fails `HandoverInFlightIsNotAFault`, disabling the role re-read fails
  `ARoleThatMovedOnLeavesNothingToStepDownFrom`. The third case, a genuinely lost lease, passes
  under both mutations, which is what shows the guards do not swallow the situation #82 exists for.
- **The integration test is an absence, and it is only possible because of #86.** Nodes log to a file in their
  own data directory now, so the integration test records the log offset before the `FAILOVER` and
  asserts that what follows contains the handover and **not** `lease lost, demoting to REPLICA` nor
  the lease-lost warning. Over the produced slice rather than the whole file, because a
  session-scoped cluster's log is mostly other tests' output and an assertion over all of it would
  pass or fail on history. It does *not* assert the new INFO line is present: whether a monitor pass
  lands inside that window is timing, and asserting it would be an assertion about the machine.
- Effort: S | Impact: three warnings during a healthy planned handover, which is how operators learn
  to discount warnings

### 88. A graceful `FAILOVER` could abort the outgoing primary ✅ **P0**

- **`FAILOVER <target>` killed the node that handed the role over.** The process died with
  `SIGABRT`, so its port stopped accepting connections — an operator issuing a *planned* handover
  lost the outgoing node entirely, and the tool they reach for when they want to be careful is the
  one that did it.
- Found by the diagnostics added for #86, on a **documentation-only** pull request — which is what
  settled that no code change was responsible. The test reported `alive=False exit=-6`, the fixture's
  own guard reported `node-0 ... is not running and no test killed it: signal 6`, and the node's log
  ended on `terminate called without an active exception`. That message is libstdc++ for a joinable
  `std::thread` being destroyed; it is not an uncaught exception, which is why the absence of any
  `catch` in `failover.cpp` was a red herring.
- **The mechanism was a guard whose early return meant the wrong thing.** `ReplicationManager::stop()`
  began `if (!running_) return;` and stored `false` **before** joining, so a second caller saw
  `false` and returned having joined nothing — while reading, at every call site, as *stopped*. Its
  next act was destroying the manager, whose destructor calls `stop()` and hits the same guard, so a
  joinable thread reached `~thread`. `ReplicationClient::stop()` had the identical shape one class
  away, and both were changed rather than only the one observed to abort.
- **Two callers is not exotic; it is what a graceful handover produces.** The outgoing primary
  revokes its *own* lease, so #82's unconditional lease-lost demotion fires while the handover's own
  demotion is still running. Both call `demote_to_replica()`, which read `repl_mgr_`, released `mtx_`
  to call `stop()`, relocked and reset — a window both callers entered, the second operating on an
  object the first was tearing down. The log shows the whole sequence in three lines: lease revoked
  and "now REPLICA", then "the leader key is gone — stepping down", then "lease lost, demoting".
- **Fix, in two places that are not duplicates.** `stop()` is serialised on its own mutex, held
  across the join, with the guard as an `exchange` — so the early return now means *finished*, and
  any pair of callers is safe, including `Engine::shutdown()` racing a demotion. And
  `demote_to_replica()` takes ownership of both objects **under the lock** (`std::move` out of the
  `unique_ptr`) before releasing it, so a second demotion sees `nullptr`. The mutex is not `mtx_`:
  the epoll thread takes that one, and holding it across a join is the deadlock pitfall 41 came from.
  `AsyncSnapshotBuilder::shutdown()` already used exactly this move-out-then-join pattern, from #79.
- **The regression test hangs under the defect rather than aborting, and that had to be measured.**
  Twelve runs against the reverted fix: twelve hangs, no aborts — one join succeeds and the other
  waits on a thread id that will never be signalled. A hanging test detects a defect and reports
  nothing, so the same change gives every test a `TIMEOUT` (300 s). CTest's default is 1500, which
  in CI reads as a stuck runner rather than as a failure.
- **Still open, and filed separately as #89:** the second demotion should not happen at all.
  Harmless now that both are idempotent, but it is work done twice and it prints three warnings
  during a healthy planned handover. Kept as its own item rather than a bullet here, because an open
  defect inside a closed item is one nobody scanning headings will find. It is a window between
  `revoke_lease()` and `role_.store(REPLICA)`, not the stale role I first assumed from the log.
- Effort: M | Impact: a planned, operator-initiated handover could take the outgoing node down. P0
  by consequence, and reachable by the safest-sounding command in the failover interface
### 87. `--help` listed six of forty flags, and the documents that promised the rest were incomplete too ✅

- `ob_tcp_server --help` printed **six** options. The parser accepts **forty**. `--help` is the
  first command anyone runs against an unfamiliar binary, so what it omits is what the engine does
  not appear to have — and this engine is a portfolio piece read by people deciding whether to talk
  to us.
- The three omissions that matter say why it is not cosmetic: **`--config` and `--print-config`**,
  which exist precisely so that forty flags are manageable, were undiscoverable from the one command
  that would show them; and **`--fsync-policy`**, the durability setting in a database, which #33 had
  already found missing altogether once.
- **Fix:** the text is generated from `known_flags()` — the parser's own list — rather than written
  beside it, so a flag added to the parser cannot be absent from the help. A flag with no
  description prints `(undocumented)`, which is visible rather than blank, and fails
  `CliConfigStatic.EveryKnownFlagIsDocumented`. Same reasoning as #32 feeding the config file
  through the existing parser instead of building a second dictionary of flag names.
- **Following the chain found two false statements in artefacts the package installs.** The man page
  points at `docs/cli.md` for the full set, which was the right design — except `cli.md` documented
  **21 of 40**, so the artefact that promised completeness was the incomplete one, and the promise
  is printed on every host. `cli.md` also said WAL durability was set "at build/config level" by
  `EVERY`, `INTERVAL`, `NEVER`: #33 made it a flag, and the parser compares `every`, `interval`,
  `none` — lower case, exactly — so two of the three documented spellings **refuse to start the
  server**. And the man page said the binary defaults to `DEBUG` logging; it defaults to `INFO`,
  and that sentence is what made a log-volume estimate wrong by an order of magnitude while chasing
  a hung node in #86.
- **A guard whose first run caught my own mistake, and whose first mutation caught the guard.**
  `DocumentedEnumValuesAreTheOnesTheParserAccepts` takes the values out of the parser's own branch
  and requires the help to name them: it fired immediately, because my generated text said
  `--fsync-policy` accepts ALWAYS and NEVER. It fired a second time on `--failover-enabled`, which
  accepts `true/1/yes` and `false/0/no` — the flag whose branch used to map **anything unrecognised
  to false**, so the spellings are the difference between failover on and silently off. Then the
  completeness test for `cli.md` **survived its own mutation**: it searched the whole file, and a
  row deleted from the table was still found in a paragraph above. It matches the table row now.
- Worth keeping: the two enum flags disagree on case, both as the shipped `ob.conf` writes them —
  `log-level = INFO` and `fsync-policy = interval` — so each description says which case it wants.
- Effort: S | Impact: the first command an evaluator runs described 15% of the binary, and two
  installed documents named values that do not work

### 86. A required check is flaky, and the assertion that flickers is asserting a race ✅

- `test_handover_lands_on_the_named_target` asserts that `FAILOVER <target>` answers `OK`. The
  handover is accepted, the node then stops being primary, and whether the acknowledgement reaches
  the client is a matter of which happens first — so the assertion is on an ordering between two
  independent things, which is pitfall 54's shape in a different place.
- **Measured across four CI job executions on three branches, it failed in three:** twice on a branch
  whose only functional change was the CLI flag parser, once on a branch carrying only that parser
  change *under ThreadSanitizer* while the plain integration job on the same commit passed, and it
  passed once on an empty commit off master. So it is neither a branch effect nor a load effect. It
  had been invisible because master's runs happened to be green.
- The diagnosis cost most of the time, and the reason is worth recording: `send_command()` slept
  0.3 s and took one `recv`, so **an orderly close and a reply that had not arrived yet both came
  back as `''`**. A failing assertion could not say which had happened, and `FAILOVER` legitimately
  takes seconds because it is etcd round-trips and a grace period.
- Partially addressed: the helper now reads until data or a real timeout and **raises** on an orderly
  close, so the two events are distinguishable; and the test keeps the protection the assertion was
  added for (#60 made every `FAILOVER` answer `ERR unknown_target`, so an `ERR` is still a failure)
  while no longer asserting when the acknowledgement arrives. The two assertions that follow check
  the property the test is named after.
- **The first fix was not enough, and the second occurrence said something new.** It failed again
  under ThreadSanitizer with a different assertion: `the outgoing primary reports 'UNREACHABLE'
  after handing the role over`. `role_of()` had the same defect `send_command()` had — it returned
  `"UNREACHABLE"` for anything raising `OSError`, which covers a refused connection *and* a
  `socket.timeout`, since that is an `OSError` subclass. A node that was merely slow read exactly
  like a node that was gone. One function away from the one that was fixed, which is pitfall 63's
  shape: two functions, the same mistake, and fixing one.
  So `role_of()` now answers `NO_ANSWER_YET`, `CLOSED_WITHOUT_REPLY` or `UNREACHABLE`, and the
  assertion **polls for thirty seconds** rather than sampling once — the property is that the
  outgoing node *ends up* a replica, and a single sample asserts it gets there within five seconds,
  which is an assertion about the machine.
- **The third occurrence said the most, and none of it was about the test.** It failed at 40.81 s —
  the thirty-second poll exhausted — reporting `UNREACHABLE`. That word now means something precise:
  `role_of()` returns it for an `OSError` that is *not* a `socket.timeout`, so it is a **refused
  connection**. A node that is slow, or blocked, keeps its listening socket and times out instead. A
  refusal means nothing is listening: the node is gone, or has closed its listener. That is a server
  finding, and the reason it took three runs to reach is that three separate layers were blind.
- **Layer one, and it is a CI defect worth its own line: the step that would have explained the
  failure only ran when there was nothing to explain.** `Fail on any ThreadSanitizer report` sits
  after the pytest step, and in GitHub Actions a step following a failed step is **skipped** —
  confirmed against the API, which reports `skipped` for it on the red run. So every race report
  ThreadSanitizer wrote on all three occurrences was deleted with the runner, unread. It now carries
  `if: always()`, with the rule written next to it: `always()` belongs on a step surfacing evidence
  that **exists only on the runner**. Checked `coverage` and `package` against that rule and left
  both alone — a coverage percentage from a failed suite is not a measurement, and a `.deb` rebuilds
  locally.
- **Layer two: the harness could not see a dead node.** Every node's stdout and stderr went to a
  `subprocess.PIPE` that nothing ever read, and both `healthy_cluster` and `healthy_mm_cluster`
  restart whatever is not running — with **no way to tell a deliberate `kill_node()` from a crash**.
  These modules kill nodes on purpose constantly, so a node that died of its own accord was repaired
  in silence while the suite stayed green. Not a workaround for a known defect: an inability to see
  one. Fixed three ways — nodes log to a file in their own data directory (**appended**, because
  `restart_node()` reuses the directory and `"w"` would delete the evidence in the act of repairing
  the cluster), `unexplained_deaths()` reports any node that is not running and was not killed by a
  test, and the handover assertion prints liveness, exit status and the node's own log tail.
  `unexplained_deaths()` was verified by mutation — a node killed behind the harness's back produces
  `node-1 (index 1, port 45999) is not running and no test killed it: signal 9` plus its log.
- **Layer three, measured, and it corrects a workspace note rather than confirming it.** I had
  written that the unread pipes fill because nodes log at DEBUG. The binary's default is **INFO**.
  Measured on i3-7100U, Release, default level: 2000 writes cost **153 bytes in total** — writes are
  not logged at INFO — but **each client connection costs ~153 bytes**, so the 64 KB pipe fills at
  roughly **418 connections per node**. The `cluster` fixture is session-scoped across 146 tests,
  each opening a connection per command, so the battery goes past that: a node blocking inside
  `write()` was reachable, and is now impossible. It is a real hazard removed, and it is **not** the
  cause of this failure — a blocked node refuses nothing.
- **The server side is answered, and the answer is no.** The question was whether a node closes a
  client session while stepping down, leaving an operator unable to tell success from a refusal.
  Nothing on the failover path touches a session or the listener: the session-closing sites are all
  in the epoll loop and all about the session's own state, and `draining_` - the only writer of
  `listen_fd_`'s closure - is reachable only from the `SIGINT`/`SIGTERM` handler. Measured rather
  than argued: `FAILOVER <target>` answers **`OK`** on the same connection that issued it.
  What *is* true is that the command runs inside `execute_command` on the epoll thread - three
  coordinator round-trips, `repl_mgr_->stop()` joining threads, and `demote_to_replica()` wiping
  every columnar segment directory - so every **other** client of that node waits for it.
  `scripts/measure_failover_stall.py` puts a number on that, i3-7100U with etcd on loopback:

  | columnar files | `FAILOVER` answered in | worst concurrent `PING` | `PING` baseline p50 |
  |---|---|---|---|
  | 280 | 69.3 ms | 19.4 ms | 0.053 ms |
  | 2800 | 75.1 ms | 73.2 ms | 0.057 ms |

  Ten times the segment files cost 6 ms more, so the stall is the coordinator round-trips and not
  the local wipe. Tens of milliseconds is a cost to write down, not a reason to move the handover
  off the io loop the way #79 moved snapshot creation - there the figure was 1.7 s and grew with the
  store. And it would change the command's meaning: an operator issuing `FAILOVER` wants the answer
  *after* the handover, not a receipt for having asked.
  This also does not explain the third occurrence's `UNREACHABLE`, which requires that nothing is
  listening; a busy epoll thread still has a listening socket and a kernel accept queue, so it
  produces a timeout rather than a refusal. The memory hypothesis below stands.
  The third occurrence sharpened it into something falsifiable: the node **stopped listening
  altogether** for the whole thirty seconds, which is a larger claim than closing one session. Two
  candidates remain and the exit status separates them. Reading the server narrowed it to those two
  and no further: `UNREACHABLE` requires that nothing is listening, and only two paths get there —
  the process is gone, or `draining_` is set, which closes `listen_fd_` and then ends the loop once
  sessions drain, so that path ends the process too. `draining_` has exactly **one** writer,
  `TcpServer::shutdown()`, reachable only from the `SIGINT`/`SIGTERM` handler; `SIGPIPE` is ignored
  and nothing on the failover path calls it. Nothing in `demote_to_replica()` touches `listen_fd_`,
  and while `failover.cpp` contains **zero `catch`** — so an exception on the monitor thread would
  call `std::terminate` — the manual etcd parser guards `npos` at all five `substr` sites, so that
  trigger is not present.
  **So the node was signalled or it died, and the memory hypothesis is now measured — and
  refuted.** ThreadSanitizer multiplies a process's footprint, this job runs three nodes plus etcd
  on one shared runner, and an OOM kill arrives as `SIGKILL` with no report of any kind, which fits
  every observation. It was the likeliest producer until somebody sampled it. Measured on
  12 September, TSan build, the battery's heaviest modules (`test_stress.py` and
  `test_large_response.py`, a half-million-row load among them):

  | | |
  |---|---|
  | peak resident, every node **and** every etcd at once | **246 MiB** |
  | largest single node ever (`VmHWM`) | **92 MiB** |
  | lowest `MemAvailable` seen during the run | **8.8 GiB** |

  Against a runner with 16 GB that is not an out-of-memory condition, and three nodes rather than
  two would add about 90 MiB. **The hypothesis stands refuted rather than unproven**, which is worth
  more than the guess was.

  **And the measurement pointed at something better, which is already closed: #106.** A node that
  receives `SIGTERM` closes its listening socket **immediately** and then waits for its sessions —
  and before #106 it waited forever if any client was attached. `_stop_node()` in the harness is
  "`SIGTERM`, then `SIGKILL` after five seconds", **silently**. Compose those and the outside
  observer sees: nothing listening (a refusal, not a timeout), the process alive for five more
  seconds, then `signal 9`, no race report, and only sometimes — which is every observation this
  item recorded, including the third occurrence's thirty seconds of `UNREACHABLE`. Since #106 the
  harness starts nodes with `--drain-timeout-ms 2000`, so that window is bounded at two seconds.

  **What keeps this readable next time**, because "the likeliest producer" is what cost a week here:
  `unexplained_deaths()` no longer prints one word. A `signal 9` is now reported as *either* "this
  harness escalated SIGTERM to SIGKILL 5.0s ago — #106's shape" — the harness knows, and it never
  said so — *or* "no SIGKILL came from this harness, so it came from outside it; MemAvailable is now
  N MiB", which is the number the OOM hypothesis needs and which nobody was recording. Four tests
  pin both branches plus a control that refuses to invent a theory for an ordinary exit code.

  **One more thing came out of measuring it, and it is an environment fact rather than a defect:**
  a TSan-instrumented server aborts at startup with
  `FATAL: ThreadSanitizer: unexpected memory mapping` **at random** on this kernel — the ASLR
  entropy is higher than TSan can map around, `vm.mmap_rnd_bits=28` is the usual answer and this
  machine will not let it be set. It exits **66**, so a node that never started reads as a node that
  died; the report now names that too.
- **After the diagnostic commit the check passed, and that is not evidence that it is fixed.** One
  green run is what this very item already recorded as an anecdote: it passed once on an empty
  commit off master while failing three times elsewhere, which is why the measurement was four
  executions across three branches rather than one. Nothing in that commit changed a line of the
  server, so if the cause is on the server it is still there. The server half stays open, and the
  next failure reports the exit status, the liveness and the node's own log instead of one word.
- **A third cause of the same flakiness, and it is not a defect in the engine: every wait in the
  integration suite was chosen against an uninstrumented build.** The job failed with `node-1 never
  accepted connections` on a branch whose only changes were Python files and documentation — a
  30-second startup budget for a node that starts in two, under ThreadSanitizer, on a shared runner
  that was also running the rest of the battery. ThreadSanitizer costs five to fifteen times the run
  time, so the numbers were never wrong for the machine they were written on and never right for
  this job.
  `patience()` in `conftest.py` triples every startup wait when `TSAN_OPTIONS` or `ASAN_OPTIONS` is
  set — read from the environment, because that is what makes it true. It is scaling rather than
  silencing: a node that cannot start inside the scaled window is still a failure, and still says
  so. Applied to the shared fixture and to the two modules that start their own nodes.
**The server half is closed as an explanation rather than as a patch**, and the distinction is the
point: nothing in the server had to change, because the producer was the harness's silent
escalation meeting #106's unbounded drain. Both are fixed — the bound in the server, the silence in
the report — and the memory theory is refuted by measurement rather than left hanging.

- Effort: S for the test half (done), S for the diagnostic half (done), M for the server half
  (**done**: measured, refuted, explained) | Impact: a required check that fails at random trains
  everyone to re-run it, which is how a real failure gets re-run too

### 85. The WAL position was read from four threads without synchronisation, as an inconsistent pair ✅

- `WALWriter::current_offset()` and `current_file_index()` returned plain members that
  `write_record()` mutates. TSan reported the read from `FailoverManager::publish_position_if_due()`
  against the write from the flush thread.
- **The atomicity was the smaller half.** The two were read as a *pair* by two separate loads, so a
  rotation between them yielded a position that never existed. Measured before the fix with a reader
  polling in a tight loop: **one incoherent pair in about 150 million reads, in two runs out of
  three.** So the coherence defect was real and rare — the window is two adjacent instructions —
  while the data race was on *every* concurrent read.
- It reached a decision, which is why rare was not the same as harmless: `get_wal_position()` feeds
  the published position that election deference compares to pick the replica furthest ahead
  (#70, #72). And the static test found more sites than the report did: **two snapshot manifests**
  composed the pair as well, which is the point a joining peer catches up from.
- **Fix:** one `std::atomic<WalPosition>` **replacing** the two members rather than published beside
  them, because a copy would need publishing at five mutation sites and a missed one gives a
  silently stale position — a worse symptom than the UB it replaces, which a sanitizer at least
  reports. `static_assert(is_always_lock_free)`, because an atomic that quietly takes a lock would
  put that lock on the WAL write path. The offset narrows to 32 bits, so the constructor **refuses**
  a rotate threshold above 2 GiB rather than clamping it.
- **My own implementation reintroduced the defect and the test caught it in one run.** The first
  rotation published `(N+1, previous file's offset)` as an intermediate state, because it
  incremented the index and let `open_current()` store the offset afterwards: 96 backwards
  observations in 4.3 million. `open_current(index)` now *returns* the offset and rotation is a
  single store.
- **Cost on the write path: none measurable.** `BM_IngestionThroughput`, i3-7100U, Release, six
  interleaved rounds against `eeb1698`: 2490.0 ns/op (cv 1.31%) against 2466.5 ns/op (cv 0.71%),
  median of per-round ratios 0.9905 over 0.973-1.008. That is inside this machine's resolution and
  is not claimed as a speed-up. `objdump` of `write_record` confirms the claim the design rests on:
  zero `lock`, `cmpxchg`, `mfence` or `xchg` — the relaxed load and store are plain moves.
- **And the gap that hid it is closed:** `sanitizers-integration (tsan)` runs the **whole** battery
  now, not three modules. This also unblocked task 6.3 of #45.
- **Widening the job found a second defect, and it is the worse one.** Four modules built their own
  `os.path.join(REPO, "build", "ob_tcp_server")` and ignored `OB_SERVER_BINARY` — they start their
  own nodes rather than using `ClusterManager`, so each grew the path and none grew the override.
  Consequence in CI: three of them **skipped** (14 tests, reported as skips in a summary nobody
  reads) and the fourth crashed on a missing file. `test_cpp_client.py` skipped another seven for the
  same reason with its own harness path. Consequence locally, which is worse: a stale
  `build/ob_tcp_server` was there to be found, so a per-module check of "clean under TSan" reported
  clean for runs in which **TSan was not present at all** — and `test_mm_stats.py` is one of the
  three modules this job had been running since it was created. *Part of a required check had been
  measuring an uninstrumented binary since the day it was written.*
  Fixed with one `server_binary_path()` in `conftest.py`, a derivation for the client harness, a
  static test in `test_smoke.py` that refuses a module building its own path, and a CI step that
  **fails the job on any skip** — the same shape as the SDE repository's step checking its PostgreSQL
  cross-section did not skip.
- Verified as CI will run it: **145 tests, zero skips, zero ThreadSanitizer reports**, 8m25s on the
  development machine. The earlier claim of "19 modules, 154 tests, zero reports" was made before
  this was found and was wrong for four of those modules.

### 84. MM_PEERS counted inbound connections that had not said who they were ✅

Found by the `integration-tests` job, which failed with `node-2 sees 3 peers` in a three-node
cluster. The third row was `0  (no address)  disconnected`.

An accepted connection is stored in `peers_` under a temporary key with `node_id = 0` until its
handshake identifies it. `handle_mm_peers_command()` printed every entry, so a connection
mid-handshake appeared as a peer — one an operator reads as a peer that has fallen over, and one that
anything comparing the row count against the cluster size reads as a node too many. Both readings are
wrong, and the second is what made it an intermittent test failure rather than a permanent one: the
row exists only for as long as a handshake is in flight.

Un-identified connections are skipped now, and their count goes to the log at DEBUG. Not to the wire:
these rows are parsed — by the integration harness among others — so a trailing summary line would be
counted as a peer by anything splitting on newlines. Dropping something silently is the failure this
whole class is about, hence the log line.

`MM_PEERS` is a command an operator has to trust, which is the argument #23 makes about a metric that
reads zero for two different reasons, and the one behind the `hlc_timestamp` column that showed
`0.0.0` for every peer because nothing ever wrote `last_hlc`.

- Effort: S | Impact: A diagnostic command reported a peer that did not exist, intermittently

### 83. The sanitizers and the coverage build instrumented a sixth of the tree ✅

Found while measuring coverage for the other half of #37. `gcovr` reported **59.0% of 2387 lines**,
which for a tree of this size is the wrong order of magnitude — and the per-file report named **6 of
34** source files. Everything else was missing, not at 0%.

`add_compile_options()` affects only targets created **after** the call. Every library in this
project is created between lines 85 and 213 of `CMakeLists.txt`; the `OB_ENABLE_ASAN`,
`OB_ENABLE_TSAN` and `OB_ENABLE_COVERAGE` blocks sat at 232-258, past all of them. So the
instrumentation reached `ob_tcp_server` and the test executables and **none of the twenty-eight
static libraries where the engine lives**.

One grep is the whole proof:

```
build-asan/CMakeFiles/orderbook_multi_master.dir/flags.make   -fsanitize: 0
build-asan/CMakeFiles/orderbook_engine.dir/flags.make         -fsanitize: 0
build-asan/CMakeFiles/ob_tcp_server.dir/flags.make            -fsanitize: 1
```

**What this means for what has been claimed.** #37 and #80 both reported suites "clean under
ASan+UBSan and TSan", and this repository has said so in a commit message, a pull request and its own
notes. That claim covered the test binaries and the server, not the libraries. UBSan needs
instrumentation to see anything, so undefined behaviour in library code was never checked. ASan still
catches heap errors through its allocator interposition, so that part held. The TSan findings in #80
were real — a lock-order inversion and races it sees through pthread interceptors regardless — but
races entirely inside uninstrumented library code were invisible to it.

Corrected rather than quietly restated, because the number of times this repository has been bitten
by a mechanism that looks present and is not is the reason it keeps a pitfall list.

The fix is placement: the three blocks now sit **after** FetchContent, so googletest, benchmark,
rapidcheck and nlohmann/json stay uninstrumented — they are not what these builds are asking about,
and a UBSan finding inside a dependency would fail the build under `-fno-sanitize-recover` — and
**before** the first `add_library`, so every target of ours is covered. Verified the same way it was
disproved: by grepping `flags.make`.

**And the first fully-instrumented run found two pieces of undefined behaviour, both in libraries
that had never been instrumented.** Two of 744 tests failed, which is the proof that this was not a
tidying exercise:

- **`encode_prices()` subtracted two `int64_t`** to form each delta. UBSan:
  `-5398869315210128419 - 3959960346406320104 cannot be represented in type 'long int'`. Real prices
  live nowhere near the ends of the range, but the property test generates the whole of it — and it
  was right to: signed overflow is undefined, and the round trip was therefore not total. The deltas
  are computed in unsigned arithmetic and reinterpreted now, which wraps by definition and, in C++20,
  converts back modularly rather than implementation-definedly. `decode_prices()` wraps to match, so
  the codec now inverts itself for **every** `int64_t` input rather than for the range prices happen
  to occupy.
- **`HybridLogicalClock::merge_remote()` computed drift as
  `int64_t(new_physical) - int64_t(now)`**, and then negated it if negative. Two undefined steps in
  three lines: the subtraction overflows for a large physical component, and `-INT64_MIN` is
  undefined on its own. This one is reachable **from the network** — `new_physical` derives from a
  peer's timestamp on the wire, so a node sending a nonsense value caused undefined behaviour on
  every node that received it, not merely a wrong drift figure. Unsigned difference, then clamped to
  `INT64_MAX`.

Neither was found by the sanitizer job that had been required on every pull request for a day,
because neither library was compiled with the sanitizer.

**And one library cannot be compiled with TSan at all**, which is the other thing the accident was
hiding. `orderbook_soa` builds the SoA buffer's seqlock on `std::atomic_thread_fence`, and GCC
refuses: *"'atomic_thread_fence' is not supported with '-fsanitize=thread' [-Werror=tsan]"*. TSan
models happens-before through atomic operations rather than standalone fences, so it could not reason
about a seqlock even if it compiled one. That translation unit is excluded from TSan explicitly now,
with the cost stated where the exclusion is: **races inside the seqlock are outside TSan's reach**, and
reports about the data it guards, raised from instrumented callers, have to be read against the
seqlock's design rather than taken at face value. Before #83 the file was not instrumented either —
along with the other twenty-seven — so the build succeeded by accident and nobody learned that the
tool and the engine's hottest data structure are incompatible.

- Effort: S | Impact: Two CI jobs and a coverage number that all looked like they covered the tree
  and covered a sixth of it. Turning the instrumentation on properly found undefined behaviour on
  the compression path and in the clock, one of it reachable from a peer


### 82. A revoked lease is noticed on the next refresh, and a candidate can win the key before then ✅

Found by the `integration-tests` job on its **first run** (#55). The suite has always passed on the
development machine; on a shared two-vCPU runner two failover tests failed, and only one of them was
a test defect.

The real one: `test_a_primary_whose_lease_etcd_forgot_stops_holding_the_role` reported **two nodes
holding PRIMARY at once**. That is not #74 coming back — #74 was about the holder never finding out
at all, and it does find out now. This is the window before it does.

Revoking the lease deletes the leader key **immediately**. The holder learns on its next refresh,
which runs every `lease_ttl_seconds / 3` — about 3.3 s at the default TTL of 10. A candidate polling
the leader key sees it vacant and can win it inside that window. So for up to ~3.3 s two nodes
believe they are primary, and **both accept writes**, because the write path checks the local role
rather than asking the coordinator per write. Writes landing on the one about to step down are in its
WAL and in nobody else's.

Bounded, and not the same class as #74's indefinite split brain — but it is real divergence, and it
only shows up when the two polls land in the unlucky order, which a loaded machine makes likely and
an idle one makes rare. That is why it went unseen: it needs a slow runner and it needs looking.

The fix is a lease that enforces itself locally rather than one that is checked when convenient.
Two halves, and they are independent:

1. **Self-fencing on the holder.** Stop accepting writes when the time since the last *successful*
   refresh exceeds a fraction of the TTL, rather than only when a refresh actively fails. A holder
   that cannot reach etcd is in the same position as one whose lease was revoked, and today neither
   stops until a call returns.
2. **A candidate waits out the remainder.** After observing a vacant leader key, wait long enough
   that the previous holder must have noticed — the TTL, less what is already known to have elapsed.
   This costs failover latency, which is the trade to state explicitly rather than assume: the same
   trade #70 made with `--election-deference-ms` and #72 then bought back.

Recorded as a test rather than as a paragraph:
`test_no_two_nodes_hold_the_role_at_the_same_instant`, marked `xfail(strict=False)` on purpose. Not
strict, because whether it reproduces depends on which poll lands first — it fails on a loaded runner
and passes on an idle laptop, and a strict marker would turn the idle case into a false failure. The
non-strict marker is the honest statement of a defect whose reproduction is probabilistic.

**The other failure was the test's fault, and worth recording next to it.**
`test_the_survivor_does_not_wait_for_a_dead_nodes_position` asserted that a killed node's published
position had already left etcd *by the time* the survivor was promoted. That is not an invariant: the
leader lease and the position lease share a TTL and have independent refresh phases, so which expires
first depends on which was refreshed more recently before the kill. It failed at 10.2 s with the
position still present — the mechanism working exactly as designed. It now asserts what #72 actually
guarantees, which is also the stronger regression guard: the position **does** disappear within the
TTL plus a margin, because it is written under a lease nobody is refreshing. Before #72 the key had
no lease and stayed there for ever.

**The window was the smaller half. Fixing it turned up the larger one.**

`handle_primary_lease_lost()` called `handler_.demote_to_replica()` **only when it could read a
leader key carrying a non-empty address**:

```cpp
auto state = coordinator_->get_cluster_state();
if (state.has_value() && !state->leader_address.empty()) {
    primary_address_ = state->leader_address;
    handler_.demote_to_replica(state->leader_address);   // ← the only call
}
```

In the case that matters there is no key: a revoked or expired lease deletes it, so there is no
address, so the Engine was **never told**. `FailoverManager::role_` went to `REPLICA` while
`Engine::node_role_` stayed `PRIMARY`, `read_only_flag_` stayed unset, `ROLE` kept answering
`PRIMARY <epoch>` — and the node went on accepting writes until it happened to stand for election
again, **indefinitely if it never did**. Not a 3.3-second window: an open-ended one.

That is pitfall 28 — "when a role moves, every component that answers questions about it has to be
told" — in the very path pitfall 28 was written about. Not knowing where to point the replication
client is a reason to start no client; it is not a reason to keep claiming a role. The demotion is
unconditional now, and the address is optional.

**What actually shipped**, in the order it was found:

1. **The coordinator says what it does not know.** `read_leader()` answers `Present` / `Absent` /
   `Unavailable`. `get_cluster_state()`'s `std::nullopt` meant not-connected *or* empty-response *or*
   key-absent *or* unparseable-body, so a primary could not act on a vacant key without also
   stepping down on every transient etcd error — which is why it acted on neither.
2. **The holder steps down on a confirmed-absent key**, within its one-second poll instead of within
   `lease_ttl/3`. And on a clock rule: no confirmation for a whole TTL means stepping down, because
   whatever the reason the lease has had time to expire. It never fires on a healthy node, which
   confirms every second.
3. **The demotion reaches the Engine unconditionally** — the larger half above.
4. **A candidate waits out the holder's step-down bound** (`--election-lease-wait-ms`, deriving from
   the lease TTL) before claiming a vacated key. A cold start does not wait: no leader has existed to
   wait for, read from the epoch, which is persisted. The residual hole is a brand-new node that has
   never seen this cluster's epoch and reconnects during the vacancy — narrower than what this
   closes, and named rather than left to be found.
5. **A replica no longer campaigns on a read that failed.** Same conflation, opposite direction: an
   unreachable coordinator used to look exactly like a vacant key.

**Measured cost, which is the trade that was chosen deliberately:**

| | before | after |
|---|---|---|
| Failover after `kill -9` | 10.2 s | **20.1 s** |
| Two nodes accepting writes after a revoke | open-ended | 0 |
| Old holder still answering `PRIMARY` after a revoke | until it re-promoted, or for ever | ≤ ~1 s |

Failover roughly doubles, every time and not only in the unlucky case. That was the decision: the
alternatives either leave the window open or make a primary read-only during a brief etcd hiccup,
which trades a latency cost for an availability one.

**Four tests, and three of them had to be rewritten first**, because they asserted transient states
that this change re-timed:

- `test_a_primary_that_lost_its_lease_refuses_writes` is the new one and the one that matters: it
  asserts a *refused write* rather than a reported role, because a role is what a node says and a
  refused write is what protects the data. Verified by mutation — restoring the `if
  (!new_primary.empty())` makes it fail.
- `test_no_two_nodes_hold_the_role_at_the_same_instant` was `xfail(strict=False)` from the day CI
  reproduced it; it is an ordinary test now.
- `..._stops_holding_the_role` used to wait for "exactly one primary", which is true before the
  transition as well as after — so its loop declared success while nothing had happened. It watches
  for the holder's claim to *change* now.
- `..._after_a_lease_scare` broke on the first sighting of one primary and then re-read, which
  straddles the gap between a prompt step-down and the next election. It confirms twice, a second
  apart.

- Effort: M | Impact: A node whose lease was revoked kept accepting writes indefinitely. Closed at
  the cost of doubling failover latency

### 81. CRC32C was a byte-at-a-time table lookup on a CPU that has a CRC32C instruction ✅

Found while sizing #79. Creating a snapshot ran at 148 MB/s, which for a flush plus a checksum pass
looked like the checksum, so the checksum got measured on its own: **295 MB/s, flat, at every size**.
That is a table walk one byte per iteration, and SSE4.2 has had a `crc32` instruction implementing
this exact reflected polynomial since 2008.

Measured on the development machine (i3-7100U, Release, `-O2`), buffer mutated on every iteration so
neither the call nor the loop can be hoisted:

| Payload | Table | Instruction | Speedup |
|---------|-------|-------------|---------|
| 112 B — a one-level `INSERT` | 361.7 ns | 23.8 ns | **15.2×** |
| 328 B — ten levels | 1094 ns | 45.0 ns | 24.3× |
| 24 088 B — `MINSERT` with 1000 levels | 81.7 µs | 3.78 µs | 21.6× |
| 4 MB — a columnar segment file | 14.23 ms | 0.70 ms | 20.4× |

The first attempt at this measurement reported **82 TB/s** and a 3.2× "speedup", because the input
was loop-invariant and the function pure, so the compiler hoisted both. Worth recording next to the
result: the numbers were absurd enough to notice, and a smaller error in the same direction would not
have been.

End to end, with the control built by changing one line in the same header so that the subject and
the control differ in nothing else (pitfall 33):

| `bench_engine` (5 repetitions, Release) | Table | Instruction |
|---|---|---|
| `BM_IngestionThroughput` | 2843 ns/op, 392.4k updates/s, cv 0.97% | **2455 ns/op, 461.7k updates/s**, cv 0.89% |

**+17.6% ingestion throughput**, from two tight and non-overlapping distributions. The 388 ns per
operation is the right order for the 338 ns the mechanism costs at this payload size; the remaining
50 ns is not accounted for line by line and was not chased, so it is reported as unexplained rather
than attributed. For scale: #66 was worth celebrating at 44.8 ns per write.

`BM_UpdateLatency` is not quoted, and that is deliberate. Its `manual_time` column moved the *wrong*
way by 3.7 µs while its CPU-time column moved the right way by 1.5 µs. This is the instrument the
roadmap already discredited on this machine — `BM_VwapLatency` once reported −40.6% in 8 of 8 rounds
for an identical function at the same address — so it is reported as unusable rather than quietly
dropped.

How it is chosen: `__attribute__((target("sse4.2")))` on the hardware fold, so the default build
keeps its baseline and no global `-msse4.2` is needed, plus `__builtin_cpu_supports("sse4.2")`
resolved once at static initialisation. No build-time assumption about the CPU, unlike
`OB_ENABLE_AVX2`, because there is nothing to opt into: the fallback is the old code, and a CPU
without the instruction gets exactly what it got before. `Engine::open()` logs which one ran, since
a factor of twenty on the write path deserves a line and is not otherwise visible from outside.

The property that matters more than the speed is that **you cannot tell which one ran**. These
checksums go into WAL record headers, snapshot manifests and every replication frame, so a build that
computed them differently would reject its own files and disconnect its own peers.
`tests/test_crc32c.cpp` compares the two at every length from 0 to 300 — every length, because the
hardware fold does eight bytes at a time and then finishes byte-wise, so the interesting cases sit
around each multiple of eight and a sampled test misses all of them — at every alignment from 0 to 15,
on buffers up to 1 MB, and across uneven splits of the running form. It also pins three published
CRC32C check values, because two implementations agreeing proves consistency, not correctness.

Knock-on, and it is **smaller than it looks** — which is why it was measured rather than asserted.
Snapshot creation went from 16.0-18.0 ms to **8.3-10.3 ms** for the same 2.37 MB across 184 files:
1.9×, not 20×. So the checksum was about half of that path and the other half is elsewhere. At
287 MB/s for 184 files that is roughly 45 µs per file, which is what an `fs::file_size`, an
`ifstream` open, a `std::vector` sized to the whole file, and a read cost when repeated per file.
**#79 stays open, and what it should fix has changed**: the dominant cost is now per-file syscalls
and allocation, not arithmetic.

- Effort: S | Impact: +17.6% ingestion throughput measured; every WAL record, every replication frame
  and every snapshot file checksummed 15-25× faster

### 79. Creating a snapshot blocks the multi-master io thread, and the measurement says how much ✅

Filed because #76 measured its own cost instead of estimating it, and the number does not scale.

`create_snapshot_with_sequence_state()` runs on whichever thread asked for it, and for multi-master
that is `io_loop()` — the thread that also carries live deltas, catch-up and peer handshakes. The
work is a flush plus a CRC32C pass over every columnar file, so it grows with the store.

Measured in a Release build on the development machine (i3-7100U), 100 000 rows across 20 symbols:

| Rows | Symbols | Files | Bytes | Time | Rate |
|------|---------|-------|-------|------|------|
| 100 000 | 20 | 184 | 2.37 MB | 16.0–18.0 ms | 132–148 MB/s |
| the same, after #81 | 20 | 184 | 2.37 MB | **8.3–10.3 ms** | 230–287 MB/s |

Three rounds each. The first assumption was that this path *was* the CRC pass, and #81 tested that by
making the CRC twenty times faster: the path got **1.9× faster**, not twenty. So the checksum was
about half of it, and the other half is per-file work — at 287 MB/s across 184 files that is roughly
45 µs per file, which is what an `fs::file_size`, an `ifstream` open, a `std::vector` sized to the
whole file and a read cost when repeated once per file.

That changes what this item should fix, and in a useful direction, because two candidates are now
independent:

1. **Stop paying per file.** One reused buffer read in fixed-size chunks instead of a vector sized to
   each file, and `open`/`read`/`close` instead of an `ifstream` per entry. Contained, measurable, and
   it helps every caller of `create_snapshot()`, including shard migration.
2. **Get it off the io thread.** The flush-and-checksum on a short-lived worker, handing the manifest
   back through the `wakeup_fd_` eventfd that `stop()` already uses. This is the one that bounds the
   worst case rather than shrinking it, and it adds the cross-thread state whose bug class TSan
   found twice (#37, #80) — so it wants doing carefully, not quickly.

**Candidate 1 turned out to be two things, and only one of them was the time.**

Replacing the per-file `std::vector` and `ifstream` with one reused buffer and `open`/`read`/`close`
moved the clock **not at all**: 8.2-10.2 ms against 8.3-10.3 ms. That is the third hypothesis about
this path to be wrong — the first said the checksum (half of it, #81), the second said the
allocation (none of it). The change stayed anyway, for two reasons that are not speed: a
hundred-megabyte segment no longer causes a hundred-megabyte transient allocation on the io thread,
and a failed open or a short read is now an ERROR line instead of a manifest entry silently
describing a file with `crc32c = 0`.

So the path got profiled properly instead of guessed at again. Each column below is a full
directory walk **plus** the named operation, three rounds:

| walk | + `file_size()` | + `fs::relative()` | + prefix strip | read + CRC of 2.37 MB |
|------|-----------------|--------------------|----------------|-----------------------|
| 1.03 ms | 1.50 ms | **4.85 ms** | 1.01 ms | 3.01 ms |
| 0.98 ms | 1.78 ms | **7.63 ms** | 1.58 ms | 3.49 ms |
| 1.06 ms | 1.58 ms | **4.97 ms** | 1.02 ms | 3.00 ms |

`fs::relative()` is **~21 µs per call**, about 3.9 ms across 184 files — roughly half of everything
the snapshot cost. libstdc++ implements it through `weakly_canonical()`, which resolves every path
component against the filesystem, for both arguments, on every call. Producing the same string by
stripping the base-directory prefix measures inside the noise of the bare walk.

It is sound to strip: `path` comes from `recursive_directory_iterator(base_dir_)`, so it always
begins with `base_dir_`. Checked rather than assumed, with `fs::relative()` as the fallback if it
ever does not.

**Where this leaves the item.** Snapshot creation on the same store:

| | Time (3 rounds) | Rate |
|---|---|---|
| Originally | 16.0 / 16.1 / 18.0 ms | 132-148 MB/s |
| After #81 (hardware CRC32C) | 8.3 / 8.3 / 10.3 ms | 230-287 MB/s |
| After the prefix strip | 6.4 / 4.3 / 4.1 ms | 372-577 MB/s |

**~4× on a warm store**, and the first round after writing the store is consistently the slow one
(6.4-7.1 ms) because the files are not in the page cache yet. What is left is 1 ms of directory walk
and 3 ms of reading and checksumming 2.37 MB, which is close to the floor for "read every file and
check it".

That puts a gigabyte at roughly **1.7 seconds** rather than 7, on the io thread. Better, and still
not nothing — so candidate 2 was done on its own merits.

**Candidate 2: off the io thread.** `AsyncSnapshotBuilder` runs the creation on a short-lived worker
and hands the result back through a notification whose only job is to wake the owner's loop; the
owner collects it from its own thread, so every field it owns still has exactly one owner at a time.
Both askers were converted, because both are io loops and the second one had the same defect with
nobody watching it:

Measured on the same store as every other number in this item — 100 000 rows, 20 symbols, 184 files,
2.37 MB, Release build on the development machine (i3-7100U), three rounds:

| What the io thread pays when a snapshot is requested | Time |
|---|---|
| Before: the whole creation, inline | 6.4 / 4.1 / 5.0 ms |
| After: starting a worker and returning | **0.146 / 0.099 / 0.060 ms** |

Roughly 40–70× on this store, but the ratio is not the point and quoting it alone would be
misleading. The first row grows with the store — the same table above puts a gigabyte at about 1.7
seconds — and the second row does not, because it is a thread creation. That is the difference
between shrinking the worst case and bounding it, which is what this half of the item was for.

| | Before | After |
|---|---|---|
| `MultiMasterManager::io_loop()` | ran the whole creation | starts a worker and goes back to `epoll_wait()` |
| `ReplicationManager::run_loop()` | ran it, and released `mtx_` mid-function to do so | starts a worker; nothing is released and nothing has to be re-found |

The replication side was not in this item's title and had the bug anyway. It also had a second one
worth naming: `handle_snapshot_request()` unlocked `mtx_`, created the snapshot, locked again and then
searched `replicas_` for the entry it had been holding a reference to, because that entry could have
been removed while the lock was down. None of that is needed once the wait happens elsewhere.

Three properties, each of them a refusal:

- **One at a time, no queue.** A second request during creation is answered `busy`. Two concurrent
  flush-and-checksum passes would double the cost the move exists to avoid.
- **A finished snapshot whose requester has gone is discarded.** Matched on a new
  `PeerConnection::conn_id` rather than `node_id` or descriptor, because the case that neither can
  see is the node that dropped and *came back*: the new connection asked for nothing, and installing
  a snapshot discards local contents. Sending it one would be a wipe it never requested.
- **The work is not cancellable.** A disconnect marks the request dead; the flush is not abandoned
  half-way. Price named: until that worker finishes, another peer is refused as busy. Once per node
  bootstrap, that beats both alternatives.

**One defect fixed on the way, older than this item.** `snapshot_manifest.json` was written straight
onto its own path with `trunc` and no synchronisation. The multi-master and replication loops have
always been separate threads, so two creators could already interleave their JSON and a reader could
already catch the file empty; #79 only added two more possible writers. It goes through a temporary
file and a rename now.

**What it cost to get right, in one sentence each.**

- The first `shutdown()` held the object's mutex across `join()`, and the worker's last act is to take
  that mutex to publish — deadlock, and the hang printed nothing, so gdb was the log. `take_result()`
  had the identical shape one function away and was *safe*, because it only joins once the result is
  published. Two functions, same shape, one deadlock: the rule is now blanket, no mutex held across
  any join in that class.
- The test for "publish before you notify" **survived its own mutation**: it woke a collector from a
  condition variable and raced it against the worker's very next line, which the worker won on every
  run. It now makes the notification sleep after announcing itself, which makes the check decisive.
- The test for the manifest race survived too, for a different reason: a two-file manifest fits in one
  stdio buffer and goes out in a single `write()`, so there is no partial state to catch. Thirty
  symbols and counting an empty read as a failure fixed it.

The repeatable seam is `MMSnapshotMeasurement.DISABLED_SnapshotCreationCost`, which prints the
breakdown, the total, and now the io-loop cost per request, because three wrong guesses in a row is an
argument for keeping the instrument rather than the conclusion.

**Hot-path control, and how it was settled.** Nothing here is on the ingestion path, but "the diff
says so" is not a measurement. `BM_IngestionThroughput` could not decide it: the machine, an hour into
this work, produced cv 7.95%, and a paired run in a quieter window gave medians of 2602 ns with the
change against 2607 ns on stashed `master` — identical, and both about 6% above the 2455 ns this
document records for #81, on **unmodified** code. So the absolute figure is a machine-state artefact
and only same-session pairs mean anything.

What did decide it was the machine code. `objdump` of `Engine::apply_delta`, with hex literals
normalised away, is **identical** between the two builds. Without that normalisation 34 lines differ,
and every one is a jump target displaced by four bytes because the function's `.cold` section moved —
which is the kind of difference that would have looked like a finding.

- Effort: M | Impact: A snapshot of a large store no longer stalls either io loop at all; the loop
  pays a thread creation and goes back to `epoll_wait()`

### 78. A payload larger than 65535 bytes produces a record header that understates it ✅

Found while sizing the snapshot chunk for #76, by reading the field the chunk would have to fit in.
It is not about snapshots at all.

`payload_len` is a `uint16_t` in both `WALRecord` and `WALRecordV2`, and two writers cast a `size_t`
into it without checking:

```cpp
hdr.payload_len = static_cast<uint16_t>(payload_len);   // append_version_vector, append_held_sequences
```

`write_record()` then writes the payload it was handed and the header the caller built. So a payload
above 65535 bytes produces a record whose header claims to be shorter than it is — and every replay
after that record reads the middle of this payload as the next header. **The WAL tail becomes
unreadable from that point**, which for a record written on every flush means crash recovery
silently stops there.

The same field is checked on the wire, with a different consequence. `handle_frame()` disconnects a
peer whose `payload_len` disagrees with the frame it arrived in, so an oversized version vector
drops the connection — on every reconnect, for ever.

Reachable at ordinary scale, not at an extreme: a version vector is `2 + 42n` bytes, so it passes
65535 at **1561 (symbol, origin) pairs** — 400 symbols across four origins. `MM_MAX_VV_ENTRIES` says
4096 is fine, and 4096 entries is 172 kB, which wraps to a header claiming 40962.

Fixed at the serialisers, which is the one place both paths share:

- A version vector that would not fit becomes the "send everything" marker instead. Partial is not
  an option here: the receiver has no way to know entries were left out, so it would never ask for
  them.
- Held ranges are trimmed to fit, entry by entry, with a warning naming what was dropped. Partial
  *is* sound here — every range that survives prevents a duplicate row, and the ones dropped cost
  only the duplicates they would have prevented, which is the trade #75 already accepts.
- Both WAL appenders refuse an oversized payload outright as a backstop, at ERROR. Losing a version
  vector costs duplicates; losing the WAL tail costs rows.

`MAX_LEVELS = 1000` keeps DELTA records well under the limit, so the exposure was exactly the two
unbounded payloads.

- Effort: S | Impact: A cluster above ~1560 (symbol, origin) pairs could not form, and each node
  corrupted its own WAL tail on the first flush

### 67. A node that joins an origin's stream mid-way never establishes a frontier ✅

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

**Closed in two halves, by two different mechanisms.** The duplicate-rows-after-a-restart
consequence was never about frontiers at all — the held set simply was not persisted — and went with
#75. The frontier itself is closed by #76: a snapshot carries the sender's version vector and held
set, captured in the same critical section as the flush that produced its files, and the receiver
resets its tracker and adopts them. A node that starts empty now ends up stating exactly what the
sender stated, which is a claim it is entitled to make because its contents *are* the sender's
contents.

Two details worth keeping, because both are ways to get this wrong while looking right:

- **A frontier for the node's own origin has to move the local counter with it.** A node whose data
  directory was wiped keeps its node id, so a peer can still hold records it minted before the wipe.
  Minting from 1 again hands out numbers the cluster has already seen, and every peer drops the new
  records as duplicates — of rows this node no longer has. `adopt_snapshot_sequence_state()` raises
  the counter past any adopted frontier for its own origin.
- **A sender that cannot state what it holds must refuse to be a bootstrap source.** If the version
  vector does not fit a frame, the receiver would install the files and then declare no frontier at
  all — so every peer resends the whole snapshot's worth of records into append-only storage. The
  sender refuses with a reason rather than sending a "send everything" marker, and the receiver
  refuses such a marker too, in case an older sender ever produces one.

- Effort: M | Impact: A node added to a running cluster can now prove what it holds, so peers stop
  resending it records it already has

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

**No P0 is open**, and **#169, #175 and #176 are open P1s** — the mechanical list is the `Open:`
line below; read it there rather than trusting this paragraph, which is prose and has been wrong
about this before. **#176** was found writing part 2b of #165: a mesh snapshot names each file by a
16-bit index, so a node of 8 192 segments — 8 192 instruments, whatever merging does — cannot
bootstrap a peer that joins it. **#175**: sharding by symbol has no control plane — no shard writes itself or
the shard map to etcd, each owns every symbol, and a second one on the same etcd becomes the first
one's replica — so neither client can find a shard, and roadmap #22's "done" is true only of its
parts. It was found fixing **#172, which is closed**: the Python pool's sharded mode replaced its
routing under its callers — a write routed by a half-built ring, a fan-out that raised, a shard
connection a timeout closed that never came back — and a test that builds a sharded pool against a
map in etcd holds all three. **#170 and #171 are closed, and both were the Python client's**: a pool
used one socket from two threads, so a multi-threaded caller got another query's rows — 39% of them
in the measurement, with no error — and a connection whose reply timed out answered the next command
with it, every reply after that one behind.
**#165 was a P0 and is closed**: the index is per symbol and in width
tiers, so a tick's merge and a query no longer walk it — a writer's p99 flat at 0.71–0.76 ms through
the soak where it grew to 94.66 ms — a tick seals only the stores that are due, and about the rows
it drained, so a node gains a segment per active symbol every ten seconds at a trickle and half of
master's at the write ceiling (part 2a) — and the flush tick merges a symbol's small segments, so
the count follows the data rather than the uptime: after twenty minutes of that soak a node holds
1 957 – 3 238 segments where part 2a's held 30 208 – 30 464, and a cold start answers in 5.9 – 6.8 s
rather than 26.4 – 26.9 (part 2b), at the price of a narrow query into history, 1.8 – 2.3 ms
against 0.14 – 0.25. **#174** is what measuring part 2a found next, and most of what is left of a
start after part 2b: a start reads the whole WAL twice even when its last checkpoint covers every
record.
**#169**: an exchange name with a dot makes two instruments one key, so they share a live book,
sequence numbers and stored rows. **#167 and #168 are closed**: a `SNAPSHOT` answers its columns
over the wire again — it had answered none since #139 — and keeps the latest row at or before its
time for each level rather than the last one a scan delivered. **#166 was a P0
and is closed**: a segment's time range was its first row's hour and its last row, so a query
skipped rows the segment held and `--ttl-hours` deleted rows younger than the retention — **0 of 2**
after one sweep, a row one second old among them — whenever one symbol's rows reached a flush out of
time order; segments written before the fix are repaired at the first start that finds them.
**#165** was filed as a slope: nothing merges segments, so a node gains one per
active symbol per tick, and the tick's merge, every query, startup and the TTL sweep all walked the
whole index — 256 symbols written at 102 400 levels a second, a small fraction of what one
connection can write, reached 105 472 segments in 90 seconds, with a writer's p99 grown from 14.7
to 104 ms along the way. Both were found measuring **#164**, stage 5 of #151, which is closed: the
flush tick syncs, drains and deletes without the engine's lock, and the file a rotation leaves is
synced by the tick rather than by the writer that crossed the threshold. **#163 was a P0
and is closed**: `--ttl-hours` measured event times against the time since boot, so a node
restarted with a retention longer than its machine had been up deleted its whole store at the first
sweep - 0 of 200 rows in the measurement - and one running longer than its retention never expired
anything. **#162 closed the one before it**: a replica's state file rewritten in place, which a kill at the wrong moment turned into
a full resync and a forgotten epoch, and a snapshot install nothing synced, which a power cut after
the replica recorded its position turned into 0 of 1100 rows. **#160 and #161 were P0s and are closed**: segment files were never synced, so a
power cut after a flush lost every row the checkpoint claimed - 0 of 201 in the test that now
performs the cut, under `every` and `interval` - and a full disk during a flush stored every price as
zero and answered `OK`. **#159 was a P0 and is
closed**: a flush's checkpoint claimed the rows written while it wrote its segments, and a crash
before the next flush lost them — three of three acknowledged rows in the measurement, under every
fsync policy — while the tick's retention could delete the WAL file that held them. It was found by
stage 5's crash test failing against the build **before** stage 5, which is the only reason it was not
filed as stage 5's own defect. Four were on this line before it and all four are closed. **#136**: a directory belongs to one segment
rather than to one event-time span, so a backfill re-run stores a second segment instead of
destroying the first — 8000 rows where the wire probe used to read 4000, and 5000 where it used
to read **nothing**. Its own costing of its candidates was wrong in the direction that made the
correct answer look expensive: nothing parses a segment directory name, so the change is one
function with one caller and is backward compatible with every directory already on disk.
**#137**: a writer at the pending-row ceiling asks for a flush
now, and four million levels at a one-second interval went 1,196,745 to 2,209,501 levels/s — the
rate the same client gets below the ceiling, so the slope is gone rather than softened. **#142**:
the snapshot install renamed the received files in beside the replica's own and removed nothing,
so a replica that had flushed a *prefix* of a symbol answered with both copies — 122 rows against
the primary's 100, and none of the three mechanisms this page filed it with was the one.
**#139 was on it too and is
closed**: the row path answers the columns a query names, `SELECT *` byte for byte unchanged and a
fifth off a three-column question. Every P0 raised before it —
#60, #61, #62, #64, #68, #73, #74, #80, #88 and #97 — is closed, and several were found by running a real cluster rather than by reading the code
(#73 while proving #70, #82's true cause while proving #82's smaller half, #97 from the flicker of
#96's own test).

**Open: #169, #174, #175, #176.** Every other item above #58 is marked closed, and
`scripts/check_roadmap.py` holds that in both directions — an item whose heading loses its tick has
to appear on this line in the same commit, and one that gains a tick has to leave it. Items #1 to
#58 are planned work nobody has built, not defects, which is what the floor in this line is for.

Read that as narrowly as it is written. It says every defect **that has been filed** above #58 and
is not named on it is closed. #121, the last question among them, was answered by bounding how far a peer's clock
may move this one, refusing the peer rather than the record, the clock or a clamp. It does not say the engine is finished; the capability table below is the list of what it is
not. **#37** is ticked as well, and the badge it used to leave open was **answered rather than
dropped**: the number is on the front page with its denominator and the run that measured it, and
the `coverage` job is required, so a tree below the floor cannot have a green CI badge. A percentage
badge would have said less that is checkable than the badge already there. This sentence said the
opposite for four days, which is the reason the table below is now checked — and the reason that
check would not have caught this one, because #37 is below its floor.

**#117**, **#118**, **#122** and **#123** are closed, and
together they are one investigation that started with five registered
metrics nothing wrote and ended four items later in the WAL's own arithmetic. Every lag this engine
reports is now measured against a position it can actually compare, and every one of them is a
**pair** — a value beside a count of the cases the value cannot describe — because in all three
places zero was a real answer that a sentinel would have contradicted.

Four of those five are fed. The fifth could not be: **#118** measured that the mesh has no byte
position two nodes can compare — `STATUS`'s `replication_lag_peer_<id>` equalled this node's own
WAL offset to the byte on a converged mesh, which is to say it reported a peer holding every row as
sitting at byte zero, and `MM_PEERS`' `lag_bytes` column was this node's send queue. Both are gone;
the mesh's lag is `ob_mm_replication_lag_records`, read beside `ob_mm_peers_position_unknown`
because the comparison reports a peer that has said nothing as holding nothing. That also **closed
the second clause of #54's C3**, which stage C had recorded as untestable *because* of #117.

**#122** is the one worth remembering for its own sake. Reading the code found `repl_client_` read
under `mtx_` by `stats()` and written without it by `promote_to_primary()`; it was filed rather than
fixed, on the argument that a change about metrics should not touch a failover path. Then TSan went
red on that PR, naming `unique_ptr::reset` **and** `operator delete` — a use-after-free window
rather than a torn read — because the new publisher reads that pointer every flush interval where
`stats()` reads it on demand. Adding a reader means adding it at a frequency, and that frequency is
part of the change.

**#123** is closed too, and it was the last of that chain: the replica lag — the one genuine number
in the area — ignored the WAL file index, so a replica more than a file behind read **zero**. It is
exact now, across files, and when a file in between is gone the answer is **`unknown`** rather than
a number, because zero is a real answer here and a missing file says something worse than a large
lag: retention keeps files back to the slowest connected replica, so that replica can no longer
catch up from this log.

**#124** is closed, and it is why #123 could finally be pinned by a running cluster rather than by
unit tests. The WAL rotation threshold was a literal in `src/engine.cpp`, so **no integration test
had ever crossed a WAL file boundary** — a real node would have had to write 512 MB.
`--wal-rotate-bytes` is an operator knob on its own merits, which was the condition for adding it,
and the battery now crosses boundaries in three tests: a replica streaming through three rotations,
retention keeping the file a stopped replica still needs while reporting a lag bigger than a whole
file, and a reconnecting replica caught up **across** files with the primary's own
`from_file=0 … through_file=3` as the evidence.

**#125** is closed, and that third state found it on the first run that ever reached it: a replica
sent to the snapshot path — the documented recovery for "your position is gone" — **could never
bootstrap**, because the manifest checksum covered `created_at_ns` and `total_rows`, two fields the
wire does not carry. Measured: 24 of 24 files arrived with every per-file CRC verified, the manifest
CRC disagreed, and the replica asked again every five seconds holding zero rows. The digest is now
one function both ends call, over what was transferred. Two things about how it hid are worth more
than the fix: the path was unreachable in the battery until #124, and the only unit test of it used a
**mock primary built to agree with the receiver** — four fields filled in, the other two left at the
zero the receiver reconstructs — so the stub proved the receiver agreed with itself.

**#126** and **#127** are closed, and **#128**, **#129**, **#132** and **#133** with them. **#132**
is the one with the most behind it for its size: a node whose registration lease etcd had forgotten
was out of the mesh registry for the life of the process, and the fix is a **three-state** read of
its own key rather than a rewrite on failure — because the cheap version passes both tests the
measurement produced and would quietly take ten of #54 stage C's tests away from the fixture they
stand on. **#112** is closed
too: all four of the loops it named now guard one iteration at a time, and closing it produced the
three defects listed above. #131 is the one to read first, because it is not a defect anybody
observed — it is a mutation that **survived**: the rule holding the other three loops honest was a
hand-written list, and deleting a row from it left the rule covering less while staying green.
Derived from the tree, `src/` has thirteen loop functions and **seven** still end on their first
exception.

**#134 is closed.** It was filed while #132 was being written, by asking the narrow question "what
else ever writes this node's status?" and getting the answer **nothing**: two `PeerRegistry` methods
whose headers promised etcd wrote nothing, returned `true`, and in one case **logged at INFO that
they had done it**. Both are deleted — the answer #104 took for its field — and the write-up records
why no checker guards the shape: one would need a hand-written list of what counts as internal
rather than public API, and a list written by hand is not evidence about the code.

**#135 is closed**, and it came out of the same question one step further on: #132's read was the
**second** of three places in `PeerRegistry` to hardcode `endpoints[0]`, while the coordinator client
beside it talked to whichever endpoint answered. Measured with the reversed order as its control —
dead endpoint first and the node never registered; live first and it registered in 0.5 s — and the
client now answers the question instead, with empty treated as a refusal rather than as a reason to
fall back to an index.

**#121** remains the question stage D left behind, filed rather than answered because a ceiling on
the drift a peer may introduce costs causal order against that peer.

#115 and #116 before it were also about what the log says rather than what the engine does, and
were found the same way — by #54 stage B, which set out to check refusals and had to read the logs
to do it. Measured across a coordinator outage, before and after: **2.17 and 2.23
lines per second per node became 0.40 and 0.45**, the replica's correct refusal to campaign went
from **zero** mentions at the default level to exactly one, and the one warning that was false
where it stood now stands where it is true. The holder still steps down in **2.28 s**, unchanged,
which is the control saying this touched the log and not the mechanism.

Everything about storage faults is closed. #114 went by deletion, and the deciding fact was not
the benchmark the item expected to need: on a full filesystem a growable mapping fails with
**`SIGBUS`** (measured, exit 135 on an 8 MB tmpfs), which is a signal no thread boundary can catch
and no client can be told about — so adopting it would have undone the two items beside it. #113 is
closed — `--fsync-policy every` no longer acknowledges writes it did not sync — and so is #112: an `ENOSPC` on the flush
thread's WAL write used to abort the node in a crash loop while the client-facing path handled the
same condition correctly, and all seventeen thread bodies now have an exception boundary.
Everything recorded before them is closed: #108's build job closed the last gap in compilation coverage, without claiming runtime
coverage of io_uring, and #111 closed a test whose premise was a claim about the machine rather
than about the engine. #110's first CI run also verified the
value of the skip gate: seven new CLI tests did not run until both integration jobs built the CLI
and the fixture selected the same build as the server.

**#126 is fixed** — the last defect #54's own closing measurement found: a file whose record was
torn is abandoned, and replay treats a mismatch in any file but the last as a tear rather than as
the end of the log.

**Which defects are open is stated in exactly one place on this page — the `Open:` line above**,
which `scripts/check_roadmap.py` holds in **both** directions. This paragraph used to say "there is
no open defect on this page" and was **bold and false** within two days of being written — the same
session that added the checker filed three items under it. A second sentence about a set the
checker already owns is a second sentence to keep true, and it is the one that rots, because
nothing fails when it stops being accurate. So this one points rather than restates, which is the
answer `docs/requirements.md` in the flagship product took for the same shape: a document that is
never meant to speak about status is easier to keep true than one meant to be current.

Both of the maintainer decisions that used to be listed here are made: #121 bounds the drift a
peer may introduce by refusing the peer, and #37 answered the coverage badge with a number and a
mechanism instead of a badge. This paragraph named them for one session after they were closed,
directly below the sentence above explaining why a second statement about the open set is the one
that rots — so `scripts/check_roadmap.py` now refuses any row of the table below whose **Item**
names a closed entry. Its first run found three of them, one more than reading the page had.
The capability items are in the table below.

| Priority | Item | Effort | Why now |
|----------|------|--------|---------|
| **P1** | An exchange name with a dot is refused, so no two instruments share a key (#169) | S–M | `A.B` on `C` and `A` on `B.C` share one live book, one sequence counter and one store, silently |
| **P1** | Sharding by symbol gains its control plane: the shards write the map, and both clients read it (#175) | M–L | A shard writes neither itself nor the map to etcd, owns every symbol, and a second one on the same etcd becomes the first one's replica; neither client can find a shard |
| **P1** | A mesh snapshot carries any number of files, so a peer can join a node of 8 192 segments or more (#176) | M | A mesh snapshot names a file by a 16-bit index, so a node of 8 192 segments - 8 192 instruments, whatever part 2b merges - cannot bootstrap a peer that joins it; found reading the sender, not yet measured |
| **P2** | A start finds its last checkpoint without reading the whole WAL twice (#174) | S–M | Since part 2b of #165 the index is 1.9 - 2.9 s of a cold start after a twenty-minute soak, 5.9 - 6.8 s, and the WAL most of the rest - and a start reads it twice even when the checkpoint covers every record |
| **P2** | Worked example on live market data (#43) | S | `scripts/binance_live_bootstrap.py` already runs the two-node case end to end on a live feed; what is missing is the write-up and a dashboard |
| **P2** | Grafana dashboard and alert rules (#35) | S | The metrics are already exported and the five dead gauges behind this are fixed; this is the cheapest step that makes them usable |
| **P2** | Documentation site (#40) | M | Lowers evaluation friction |
| **P2** | Release engineering + PyPI wheels (#42) | S | `pip install` is the shortest path to a first user |
| **P3** | Time-bucketed aggregation (#44) | L | The most-requested analytical capability for this data |
| **P3** | Arrow output (#46) | M | Near-zero integration cost for analytics teams |
| **P3** | Backup and restore (#34) | M | Table stakes for a database |
| **P3** | Access control (#31) | M | Multi-tenant deployments and the compliance conversation; authentication landed with #30, authorisation did not |
| **P4** | Rolling upgrade support (#56) | M | Required before anyone runs this longer than one release |
| **P4** | Performance frontier (#49-53) | varies | Proves the bespoke-engine claim; pick one and write it up |

## Known gaps and honest caveats

Things a reviewer will notice, listed here so they do not look like oversights:

- **Fuzzing covers three parsers, not the server.** #38 drives `parse_command`/`parse_minsert`, the
  multi-master frame codec and WAL replay, with a committed corpus and a bounded run on every pull
  request. Nothing in it opens a socket, starts etcd or binds a port, so a framing bug that needs
  two live nodes belongs to the integration battery instead. There is no OSS-Fuzz submission, and
  that was never a condition of the item: it would mean sending builds of a public repository to a
  third-party service, the same class of decision as the coverage badge in #37.

- **Every encrypted surface is off by default.** All
  three surfaces authenticate (`--auth-secret-file`, `--cluster-secret-file`) and all three can be
  encrypted since #30 part three — `--tls-client` for client sessions, `--tls-replication` and
  `--tls-multi-master` for the node links, TLS 1.3 with no configurable floor. Both shipped clients
  verify the chain *and* the name; on the node links verification is mutual and cannot be configured
  otherwise. Three things are still worth a reviewer's notice. **Every one of those flags defaults
  to off**, so a cluster that nobody configured is plaintext on all three surfaces and says so only
  in its startup log. **Certificate rotation needs a restart**, one node at a time. And **`--tls-peer-names` empty means chain-only verification**,
  which under a company-wide CA means any host it signs may join the cluster — the list is the
  mechanism that narrows that, and the startup log names which mode is in force.
  *(This bullet used to say the replication link and the mesh were plaintext. That was true until
  #30 part three, series D — and a caveats section that understates the engine is the same defect as
  one that overstates it, in the document a reader checks for honesty.)*
- **Event time is optional on wire writes** (#105). Without `event_time_ns`, the server stamps
  arrival time. Both clients refuse to send a supplied time to a server lacking the capability.
  TTL uses stored event time, so old backfills can expire at the next retention sweep, while a
  future-dated row keeps its entire segment alive. LWW conflict resolution still uses the node's
  HLC, not the client's chosen event time.
- **Process death is exercised in three modules, and nowhere else.** Until #62 no module killed
  anything, and that hid total loss of acknowledged writes on crash. Today `test_crash_recovery.py`,
  `test_failover.py` and `test_failover_dead_state.py` `SIGKILL` a server; the last of those also
  covers `SIGTERM` deliberately, because the difference between the two is the defect it was written
  for. What none of them do is fail a disk, drop a packet or stall a thread — fault injection more
  broadly is still #54.
- **Anti-entropy reconciles, but only what a peer still retains** (#57). Gap detection and repair are
  real; what neither covers is a gap whose records have left every peer's WAL. That needs a snapshot,
  and the reconciler has no path to one: `AntiEntropyManager` is a scheduler around a pluggable
  `ReconcileFn`, and the multi-master reconciler it drives never requests a snapshot. The transfer it
  would use exists (#76, #79), so what remains is the decision to discard a node's contents, which is
  a decision with an owner.
  *(This bullet has been wrong twice: it once said anti-entropy was a scheduler with two
  placeholders, true until #57; then it named `trigger_snapshot_repair()`, a function that no longer
  exists. The gap it describes is real, which is why the wrong names went unnoticed.)*
- **Snapshot bootstrap does not resume and does not compress** (#76). An interrupted transfer starts
  again from zero, and columnar files are already compressed, so a second pass would buy little.
  The third item that used to be on this list — creation running on the io thread — closed with #79:
  the loop now pays 0.060-0.146 ms to hand the work to a worker, and that figure does not grow with
  the store, where creation does. One request at a time, so a second peer arriving mid-creation is
  told `busy` rather than queued. And **nothing in the integration battery bootstraps a replica by
  snapshot over the replication protocol** — the coverage is unit-level on both halves now (#99
  added the sending side), which is why a live record and a heartbeat could both be spliced into
  that stream for as long as they were.
- **Benchmark baselines were recorded on one developer machine** with no hardware description. The
  table below fixes that going forward. Any published number needs its hardware next to it.
- **A subscriber that stops reading is disconnected, not throttled** (#45). Each subscription has an
  8 MB queue ceiling — roughly 140 000 rows — and past it the session is closed with
  `ob_subscription_overflow_disconnects_total` incremented. There is no flow control and no
  resumption: a consumer that needs continuity re-reads with `SELECT` from a known sequence number
  (#65). And a cancelled subscription may deliver one more row, because a notification already in
  flight is not recalled.
  *(This bullet used to say subscriptions worked embedded and not over TCP. That was true until #45
  closed.)*
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
| PING latency (io_uring) | ~24 µs avg | Python client, loopback. The transport was removed in #147: measured on an m9g.xlarge it answered `PING` in 7,487 ns p50 against 6,295 for the epoll server's `--profile boost` |
| Single INSERT (TCP) | ~0.3 ms | Python client |
| MINSERT 1000 levels (TCP) | ~3 ms | Python client, single round-trip |
| FLUSH (incremental) | ~2-3 ms | Non-blocking, two-phase |
| LZ4 INSERT (TCP) | ~1.6-2.9 ms | After Nagle fix |
| Sustained INSERT throughput | 29k/s | Python TCP, 60s stress test |
| Sustained MINSERT throughput | 777k levels/s | Python TCP, 60s stress test |
| Failover time | ~5-8 s | etcd lease TTL dependent. **Measured at 20.1 s since #82**, which added a deliberate wait of one lease TTL before a candidate claims a vacated key. The figure here predates that and predates the machine-B table below |

### Machine B — Intel Core i3-7100U @ 2.40GHz, 2C/4T, August 2026

A 2017 ultra-low-voltage laptop CPU. Numbers here run roughly 3x below machine A on CPU-bound
benchmarks. **This is a hardware difference, not a regression** — Release build, `-O3 -DNDEBUG`,
verified. Do not use this machine for published figures.

| Metric | Value | Machine-A threshold | Ratio |
|--------|-------|---------------------|-------|
| `BM_IngestionThroughput` | **462k updates/s** (387k before #81) | ≥ 1.0M/s | 2.2x slower |
| `BM_UpdateLatency` p50 | 10.6 µs | ≤ 5 µs | 3.9x slower |
| `BM_UpdateLatency` p99 | 10.8 µs | — | — |
| `BM_VwapLatency` | 1577 ns (1000 levels) | ≤ 1000 ns | 1.6x slower |
| `BM_TimeRangeQuery` (10k / 100k rows) | 0.549 ms / 3.40 ms | ≤ 5 ms | inside |

The ingestion figure moved for a reason worth stating rather than quietly restating: CRC32C now uses
the SSE4.2 instruction instead of a byte-at-a-time table, which is worth 388 ns per operation on this
machine (#81). Two runs of five repetitions, cv under 1% on both, with the control built by changing
one line in the same header. The machine-A thresholds in this table were recorded with the table
version, so the ratios against them are now flattering by that much until machine A is re-measured.

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

Verified by [the full CI run for PR #184](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/runs/36076977771),
on the tree whose flush tick seals only the stores that are due (#165, part 2a) and whose Python
client's sharded pool swaps its routing whole (#172).

**Against PR #183's run (1348 and 393) C++ is unchanged and integration +7, and both reconcile to
files.** #172 is the Python client's and added no C++ test; the seven integration tests are all in
the new `test_sharded_pool.py`.

**Before that, against PR #180's run (1301 and 381) C++ was +47 and integration +12, over two
merges, and both reconciled to files.** The forty-seven C++ tests are part 2a's: sixteen in
`tests/test_row_blocks.cpp`, seventeen in `tests/test_lazy_flush.cpp` and fourteen in
`tests/test_seal_epochs.cpp`, all three new. The twelve integration tests are nine in the new
`test_reply_attribution.py` (PR #181) and three in the new `test_lazy_flush.py`; the four storage
tests part 2a rewrote are the same four functions, and #173's fix (PR #182) changed a helper, not a
count.

**Before that, against PR #176's run (1249 and 364) C++ was +22 and integration +8, and both
reconciled to files.** The twenty-two C++ tests are #164's: nine `ChunkedQueue.*` in the new
`tests/test_chunked_queue.cpp` (one of them a property test), ten `WalSyncTicket.*` and two
`WalSyncStatic.*` in `tests/test_wal.cpp`, and `FlushTickStatic.*` in `tests/test_write_batch.cpp`.
The eight integration tests are nine new functions in `test_storage_faults.py` less one renamed
there — #153's test about a failed sync while rotating, which moved to where that sync happens now.

**Before that, against PR #174's run (1236 and 360) C++ was +13 and integration +4, over two
merges, and both reconciled to files.** #162 added three C++ tests, the `DurableWrites.*` rule in the new
`tests/test_durable_writes.cpp` that every open for writing says what makes it durable, and three
integration ones, one per file: a snapshot a replica installs survives a power cut
(`test_power_cut.py`), a replica killed while it saves its position keeps what it holds
(`test_replica_restart.py`), and a snapshot whose install could not sync is installed again
(`test_storage_faults.py`). #163 added ten C++ tests, five `TTLCutoff.*` and three `TTLSweep.*` in
`tests/test_ttl_retention.cpp` and two `ClockUse.*` in the new `tests/test_clock_use.cpp`, and one
integration test, the new `test_ttl.py`. `test_durable_writes.cpp` lost 128 lines to #163 and no
test: its scanner moved into `tests/source_scan.hpp`, so the two rules read the tree the same way.
PR #175's own run gave 1239 and 363, which is the step between.

**Before that, against PR #173's run (1236 and 345) C++ was unchanged and integration +15**, and both
reconciled to files: #160 and #161 added no C++ test, and the fifteen are the six in the new
`tests/integration/test_power_cut.py` - the cut after a flush and its control under both policies,
a cut during a flush's segment sync and a cut before the first flush - eight in
`test_storage_faults.py` - a flush whose segment sync failed under both policies, the freeze until a
restart, retention held back by an unsynced segment and by a failed WAL sync, `none` freezing
nothing, and the two refused segment writes - and one in `test_fault_injector.py`, a chosen `syncfs`
failing.

**Before that, against PR #172's run (1228 and 340) C++ was +8 and integration +5**, and both reconcile to
files: the eight are `CheckpointPayload.*` and `WalCheckpoint.*` in `tests/test_wal.cpp`, and the five
are the two crash tests in `tests/integration/test_storage_faults.py` (one of them under both fsync
policies) and the two delay-mode tests in `test_fault_injector.py` the fault injector gained with
#159.

**This row was ten merges behind, and both deltas reconcile to files.** Against PR #157's run
(1151 and 299) C++ is **+30**: fourteen in `tests/test_tcp_server.cpp`, six in the new
`tests/test_query_book.cpp`, five each in `tests/test_cli_config.cpp` and
`tests/test_command_arity.cpp`, four each in `tests/test_cli_args.cpp` and
`tests/test_columnar_store.cpp` and one in `tests/test_query_buffer_lifetime.cpp`, less four in
`tests/test_iouring_instrumentation.cpp`, four in `tests/test_metrics_registry.cpp` and one in
`tests/test_tls_context.cpp`, which #147 took out with the transport. **The six in
`tests/test_io_uring.cpp` went too and moved nothing**: that file was built only with
`OB_USE_IO_URING`, which this job does not set, so it had never been part of this count — the one
line of the reconciliation that a file-by-file diff of test macros gets wrong, because it counts a
file the job never compiled. Integration is **+28**: eight in `test_pipelined_answers.py` (#146),
seven in `test_io_reactors.py` (#151), five in `test_live_book.py` (#145), three in
`test_recv_buffer_ceiling.py` (#143), three in `test_io_profile.py` (#144; two functions, one of
them run under both profiles) and two in `test_segment_identity.py` (#136).

**This row was four merges behind — #140, #141, #137 and #142 — and both deltas reconcile to
files, which is the only check that means anything here.** Against PR #153's run (1140 and 280,
read from the run rather than from the cell, because the cell said 280 while its own prose said
273) C++ is **+11**: five in `tests/test_socket_options.cpp` (#140), two in
`tests/test_pending_backpressure.cpp` (#137) and four in `tests/test_columnar_store.cpp` (#142).
Integration is **+19**, and the number of *functions* added is 11 — the difference is
parametrisation, so it was counted by collecting the three new files rather than by grepping
`def test_`: `test_client_batch.py`, `test_pending_ceiling.py` and `test_wire_nodelay.py` collect
19 between them. `test_wal_rotation.py` changed and added nothing, because #142 strengthened an
assertion rather than adding a test.

**The row this replaces was two merges behind, and the arithmetic is the argument for the
script.** It cited PR #149 and said 1102, while master after
[PR #151](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/pull/151)
and
[PR #152](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/pull/152)
was **1109** — both of those merged without a table commit, so nothing was wrong with any number
in isolation and the pair had stopped describing a tree that exists. Against 1109 this branch is **+31**, which is
exactly the count of test macros it adds, and against 273 it is **+7**, which is exactly
`test_column_projection.py`. That both deltas reconcile to the file is the check worth having;
a delta measured against a stale baseline reconciles to nothing. Runtimes below are from GitHub's
`ubuntu-24.04` runners except where a row says otherwise, not the machine-B performance baseline
above.

**Both halves of this block come out of `scripts/test_table.py <pr>`, and that is the second
attempt at keeping them together.** The citation and the counts have to name the same tree, and
they have now drifted apart twice: with #129 the table cited PR #124's run beside a C++ count of
1083 when #124's tree had 1082, and the commit that *added a paragraph asking the next person not
to let that happen* left the citation on PR #125's run beside an integration count of 265 that
#125's tree did not have. A sentence asking a human to keep two numbers in step is the shape this
repository keeps finding rotten — a claim with nothing over it. Nothing offline can check these
numbers, because they are measured elsewhere; what the script removes is the possibility of
updating one half without the other, and its own first version printed `17 in 0:00` for a job that
ran 265 tests, because `re.search` returns the *first* match and pytest's verdict is the last.

| Suite | Count | Status |
|-------|-------|--------|
| C++ (GTest + RapidCheck) | 1348 | **1348 on PR #184's tree**, measured by CI — unchanged, because #172 added none — and locally `ctest -j1` passed on that branch. **Before that**, 1348 on PR #183's tree, measured by CI — forty-seven more than the row before, all of them part 2a of #165's: sixteen in the new `tests/test_row_blocks.cpp`, seventeen in the new `tests/test_lazy_flush.cpp` and fourteen in the new `tests/test_seal_epochs.cpp` — and locally `ctest -j1` gave **1348/1348 in 268.80 s** on the i3-7100U on that branch. **Before that**, 1301 on PR #180's tree, measured by CI — five more than the row before, all five in the new `tests/test_snapshot_query.cpp` (#167 and #168) — and locally `ctest -j1` gave **1301/1301 in 235.78 s** on the i3-7100U on that branch. **Before that**, 1296 on PR #179's tree, measured by CI — fourteen more than the row before, all fourteen in the new `tests/test_segment_index.cpp` (part 1 of #165) — and locally `ctest -j1` gave **1296/1296 in 226.59 s** on the i3-7100U on that branch. **Before that**, 1282 on PR #178's tree, measured by CI — eleven more than the row before, all eleven in the new `tests/test_segment_time_range.cpp` (#166) — and locally `ctest -j1` gave **1282/1282 in 231.86 s** on the i3-7100U on #166's branch. **Before that**, 1271 on PR #177's tree, and locally 1271/1271 in 256.94 s on #164's branch. **Before that**, 1249 on PR #176's tree, and locally 1249/1249 in 226.63 s on #163's branch. **Before that**, 1239 on PR #175's tree, and locally 1239/1239 in 223.84 s on #162's branch; and 1236 on PR #174's tree, and locally 1236/1236 in 223.5 s on #160's branch. **Before that**, 1236 on PR #173's tree, and locally 1236/1236 in 216.0 s on #159's branch. **Before that**, 1228 on PR #172's tree, and locally 1228/1228 in 226.7 s on #158's branch rebased onto PR #171's merge. **Before that**, 1182 on PR #169's tree. **Before that**, 1181, measured by CI on #151's tree; locally `ctest -j1` gave **1181/1181 in 262 s** on the i3-7100U on #151's branch, with the build clean and warning-free. **Before that**, 1151, and locally 1151 passed in a single `ctest -j1` run on the i3-7100U with the build clean and warning-free. **Before that**, all passing with `ctest -j1` on the i3-7100U, **205 s in a single run** — **thirty-one more than master, and the breakdown is one file per question** (#139). Nine are the new `tests/test_query_columns.cpp`: what a select list resolves to, that `columns_to_read()` is wider than the output list because a filter reads what the answer does not carry, and that the canonical order is the table's order. Eight in `tests/test_response_formatter.cpp` cover the narrowed path against the unrolled one **by formatting each column alone and cutting that field out of the `SELECT *` output** — a content-based dispatch means no call can be made to take the general path over the canonical shape, so the two are held together by construction rather than by a literal. Eight in `tests/test_query_engine.cpp`, three in `tests/test_columnar_store.cpp` — including the one that found a guarantee stated three times, where a hardcoded `true` made two widenings unobservable and the mutation for them survived — and three in `tests/test_client.cpp` for the two clients' refusals, because both read a row by position and neither can read a narrowed answer. **Before them**, **four more than the previous commit, and all four are about the machine this tree had never run on**: two in `tests/test_mm_snapshot.cpp` pin the ten header bytes of a snapshot chunk literally rather than through our own decoder, and a full-size chunk, because the frame is sized once now (PR #146); two in `tests/test_crc32c.cpp` are the pair that makes the rest of that file mean anything on this architecture — one skips with an explanation where there is only the table to compare against itself, which is what every agreement test had been doing off x86, and one requires the implementation the engine *reports* to be the one that runs (PR #147) across the four runs this tree and its two predecessors recorded, against 236-390 s three commits back when another session's containers were resident — the spread, not either end, is what the next number is read against. **Four more than the previous commit, and the arithmetic is worth writing down: six new and two removed** (#121). The six are three in `tests/test_hlc_skew.cpp` — the bound accepts up to itself and refuses one nanosecond past it, a peer behind us is plausible however far behind, and `UINT64_MAX` is refused so the logical carry can never saturate — and three in `tests/test_mm_wire_clock.cpp`, which is the only instrument that can reach this at all: a fake peer framing one DELTA whose HLC says what no real clock would. The two removed are the ones that **pinned the behaviour this decision reverses**, `AnHourInTheFutureOnTheWireBecomesThisNodesClockAndStays` and `ARecordFromAPeerWhoseClockIsWrongIsStillApplied`, both written by #54's stage D to state that nothing bounded the absorption. They were not deleted into a gap: the same file now asserts the opposite about the same wire shape, which is what makes a falling count readable rather than alarming. **And the local number that preceded this row was wrong by exactly those four.** A full local run on this branch printed `1094/1094` against a build directory that had not registered the four; the reconciliation is three measurements agreeing — CI's 1098, `ctest -N` listing 1100, and a local rerun after the merge giving **1098 passed in 218.95 s**. A stale build answers in the same voice it would use if it were right, which this repository has now paid for in five different shapes. **Before it**, unchanged by three commits: #134 deleted two methods no test referenced, and #135's tests are integration ones — three runs of the same suite on the same machine, the slowest with another session's containers resident and ~1 GB actually free. That spread, not any one of its ends, is what the next number is read against: it is wider than anything a commit in this repository has changed. **Seven more than the previous commit, and all seven are #131's.** Five are `LoopGuard`'s own, in the new `tests/test_loop_guard.cpp`: the counter counts every failing iteration rather than every episode, the episode counts consecutive failures and reopens after a recovery, two successes running report nothing (the observable half of "loud once" for a loop that polls ten times a second), a **null** registry still gets loud-once because two of the seven loops run in somebody else's process, and the name it writes is in the registry's own output. The other two are the descriptor `MetricsServer::handle_request` used to leak on the paths that throw — one behavioural, fifty requests against a live server with no growth in `/proc/self/fd`, and one static, because **the throwing path cannot be driven**: nothing in the process can make `serialize()` fail on demand and a knob to make it would be a knob nothing turns in production. **Before them**, four were #112's last two loops. Three are in the new `tests/test_replication_io_boundary.cpp`: that the pacing function returns zero only while a catch-up can progress, that nothing in `run_loop()` assigns `wait_ms` any other way (counted at **three** sites, because losing the one in the `catch` is the regression), and that both `try`s are where they have to be — anchored on the dispatch loop's own line rather than on a log phrase, which is the mistake #128's version of this test made. The fourth is in `tests/test_thread_boundaries.cpp` and is the one worth reading: the set of loops that guard an iteration is **derived from the tree** rather than listed, because a mutation deleting a row from the hand-written list **survived**. Fourteen `void Class::…loop()` definitions in `src/`, one a notifier with no loop in it and named, six of the remaining thirteen guarded and **seven not** — those seven are #131. Checked in both directions, so a loop in neither list fails and a row naming a function the tree no longer has fails too. **Before them**, the most recent addition was #129's: a registry given a 1200-second lease interval has to stop inside two seconds, which is a property stated three orders of magnitude clear of load rather than a duration. **Before it**, four were #128's, and they divide the way that defect does: three in `tests/test_mm_epoll_identity.cpp` are about the shape — that the two reserved event keys cannot collide with a connection, that closing a descriptor takes its registration with it (measured against `dup2`, which forces the reuse the defect needs instead of hoping for it), and that no registration in `src/multi_master.cpp` carries a bare descriptor number. The fourth is behavioural: a connection landing on the descriptor its predecessor gave back is its own connection, with both numbers read back so a run where the kernel did not recycle the number says so rather than passing quietly. Three of the six mutations in that item's table are killed by the static test **and by nothing else**, which is what says it carries weight. **Before them**, two were #126's, and they pin the replayer's rule from both sides: a checksum mismatch in an earlier WAL file yields the records from the file behind it, and one in the **last** file still stops replay — that one is a crash tail, and reading past it would hand the engine a record the process never finished writing. **Earlier**: two were #54's D3, three #125's, six #124's, seven #123's, six #118's, seven #117's. `tests/test_iouring_instrumentation.cpp` adds four that read a source file this build does not compile, which is the only check available for the rest of that transport. CTest lists **1085**: two are `DISABLED_` measurement harnesses (`MMSnapshotMeasurement.SnapshotCreationCost`, `ReplicationProtocolTest.TheWritePathWaitOfALargeCatchup`) that print measurements rather than assert them. The count that passes and the count CTest lists differ by exactly those two harnesses, always; a row two commits back gave one number for both. The runtimes are what this machine gave on the commit measured, not a budget |
| Python integration | 400 | **`400 passed, 2 skipped in 29:08`** on the GitHub runner for PR #184's tree — seven more than the row before, all in the new `test_sharded_pool.py` (#172) — and locally `400 passed, 2 skipped in 30:15` on the i3-7100U on that branch. **Before that**, `393 passed, 2 skipped in 28:56` on the GitHub runner for PR #183's tree — twelve more than the row before: nine in the new `test_reply_attribution.py` (#170 and #171, PR #181) and three in the new `test_lazy_flush.py` (part 2a of #165) — and locally `393 passed, 2 skipped in 29:36` on the i3-7100U on that branch. **Before that**, `381 passed, 2 skipped in 25:15` on the GitHub runner for PR #180's tree — three more, all in the new `test_snapshot_query.py` (#167 and #168) — and locally `380 passed, 1 failed, 2 skipped in 26:24` on the i3-7100U on that branch, the failure the one #170 is about. **Before that**, `378 passed, 2 skipped in 25:45` on PR #179's tree — unchanged, because part 1 of #165 added none; its wire measurement is the soak — and locally `378 passed, 2 skipped in 25:51` on the i3-7100U on that branch. **Before that**, `378 passed, 2 skipped in 25:05` on PR #178's tree — six more, all in the new `test_segment_time_range.py` (#166) — and locally `378 passed, 2 skipped in 26:01` on the i3-7100U on #166's branch, with `OB_POWER_CUT_TESTS=1`. **Before that**, `372 passed, 2 skipped in 25:25` on PR #177's tree, and locally 372 in 26:06 on #164's branch. **Before that**, `364 passed, 2 skipped in 25:05` on PR #176's tree, and locally 364 in 24:48 on #163's branch. **Before that**, `363 passed, 2 skipped in 24:34` on PR #175's tree, and locally 363 in 24:52 on #162's branch; and `360 passed, 2 skipped in 24:15` on PR #174's tree, the six power-cut tests among them, and locally 360 in 24:35 on #160's branch. **Before that**, `345 passed, 2 skipped in 23:55` on PR #173's tree, and locally 345 in 23:51 on #159's branch. **Before that**, `340 passed, 2 skipped in 23:25` on PR #172's tree, and locally 340 in 22:55 on #158's code before its rebase. **Before that**, `330 passed, 2 skipped in 22:38` on PR #169's tree, and before that `327 passed, 2 skipped in 22:56`, and before that `299 passed, 2 skipped in 22:44`, and before that all passing, plus the two collection-time Binance opt-in skips (`OB_BINANCE_TESTS=1`). Those skips are not part of the 280; count pytest's final result rather than the report plugin's progress characters. `280 passed, 2 skipped in 22:42` on the GitHub runner for this commit — **seven more than master, and all seven are the new `test_column_projection.py`**, which asks the question over the raw protocol on purpose: both of our clients read a row by position, so neither can read a narrowed answer and the refusals are at the bottom of that file. **Two of its tests had never run to completion before this run** — the branch's previous CI was cancelled — and both failed on the first one that did. One pinned the seven-column header as a literal in `src/response_formatter.cpp`, which this branch replaced with a generator and then deleted; it reads the column table now, which is stronger, because a dead literal can agree with a header nothing prints. The other wrote one row per test into one symbol on a session-scoped cluster, so with storage append-only the seventh test read seven rows where it had written one — and **29:20 for the same 273 under TSan**, which is the job that has to be read as well, because a battery that skips under instrumentation reads as green. Against `20:37` on the development machine (i3-7100U, native etcd) **for the 263-test tree seven commits back** — the figure is kept as the spread to expect between the two machines, and labelled with the tree it came from rather than silently paired with a count it never measured. **Unchanged by #121, and the reason is the instrument rather than the effort**: producing a real node whose physical clock is five minutes off needs the host clock moved or a time namespace, which is not something this battery can do to the machine it runs on — so the assertion lives at the wire instead, where #54's stage D already built the fake peer for it. **Before it, one more than the commit before, in the existing `test_failover_storage_faults.py` beside the control that was already there** (#130): what a node *says* while it holds a leader key it won and cannot act on. The assertion with teeth is sampled rather than read once — `ROLE` must never name this node's own replication port as the primary it follows, across a twenty-second window in which the pre-fix code answered exactly that on **every** sample. The second assertion is that the condition is reported **once**, with what an operator can do about it, because the loop runs every second and the storage that refused the record usually goes on refusing it. Measured against the two source files from the commit before that fix: **1 failed, 2 passed in 66.8 s**, the failure arriving on the **first** sample with `REPLICA 127.0.0.1:43273 2` and #112's two tests untouched. **Before it, two more, both in the new `test_coordinator_endpoint_order.py`, and the second is a control rather than a second case** (#135): the same unreachable coordinator endpoint in the harmless position, which passed before that fix and has to keep passing — without it, a harness that quietly stopped prepending anything would leave the first test green and meaningless. Both assert their premise from `Popen.args`, the command line the node actually got, rather than from the attribute that put it there, because an attribute is what the harness *meant* to say. The first also requires the mesh to form on top of both registrations, because that is the only assertion reaching the third of the three call sites: two registered nodes that never see each other is a topology watch still reading the wrong endpoint. Measured against the four source files from the commit before that fix, harness and tests unchanged: **1 failed, 1 passed in 50.1 s**, the failure naming both nodes. **Before them, one more than the commit before that, and all three in `test_peer_lease_lost.py` were rewritten**, because #132 turned the first one's premise inside out: it was written to assert that the registration **does not** come back, with a note saying that the day that loop learns to re-register is the day it fails. That day was this commit. It now polls **both** halves — the key and the log line — because the key lands in etcd before the line lands in the log, and reading the log once at the moment the key appears is a race the first rewrite lost. The #133 test needed a **new premise** as well (`stop_etcd()` rather than a revoke, which also exercises the branch that keeps this fix quiet), because after #132 a revoked lease is no longer a permanent condition, and counting log lines over a condition that repairs itself counts a condition that happened once. **The third is the one that says the gate is real**: a refusal over a key that **exists**, which nothing else in this battery produces — `redirect_peer()` writes without a lease, so the test captures the lease id before redirecting and revokes it by id afterwards. Its control is the *premise* rather than the outcome, because without asserting that a refusal reached the log it passes against a node whose refresh is succeeding. The other two gained a rate bound that is independent of wording: the count of registry lines above `DEBUG` across the window, measured at one and two. This tree's battery ran locally only as **the one module** (3 tests in **40 s**, and stage C's twelve in **3:37** as the regression check on the fixture this fix had to leave alone); the whole battery on this commit is CI's, and the 224-235 s `ctest` above is local. **Before them**, two were #112's `monitor_loop` half and both in the new `test_failover_storage_faults.py`: a replica whose data directory refuses the `EPOCH` record a role transition writes loses that monitor tick and not the thread, and its control at a size nothing writes, which must inject nothing. The assertion that carries the guarantee in the first of the two is the **recovery** line rather than the error line — only a later tick can write it, so a run in which the thread died would report the error and then say nothing, which is what a boundary is for. The pair costs **45.7 s** locally, and the module asserts its own premise: `OB_FAULT_PATH=ob_node1_` names the replica's data directory only because `ClusterManager.start()` waits for node-0 to hold PRIMARY before it starts node-1, and a change to that ordering would aim the injector at the **primary's** startup promotion, which exits the process. **Before them**, two were #112's `io_loop` half and both in the new `test_mesh_storage_faults.py`: a mesh receiver whose WAL refuses one record still receives the ones after it (**38.6 s**, because it waits for a mesh to form and for four records to cross it), and its control at a size nothing writes, which must inject nothing (**28.2 s**); that pair costs **67 s** locally. **Before those**: three were #54's A2.2 — the torn-record measurement behind #126, which costs 1.9 s — #125's — a killed replica whose confirmed WAL file retention has removed comes back with every row — and #54's C4, a mesh peer that stopped reading, which costs **9.0 s** and ~2.9 MB of writes because that is where the kernel stops absorbing them. The three before it were #124's — the first tests in this battery to cross a WAL file boundary — and the four together cost **23 s** locally, because the threshold they rotate at is 65573 bytes rather than 512 MB. The ten before them were #54 stage C, and they are most of the **16:24 → 19:18** change: each proxied-mesh test starts three nodes behind a proxy and converges on row content |
| Python integration under TSan | 400 | **`400 passed in 36:10`** on the GitHub runner for PR #184's tree, against **29:08** uninstrumented on the same runner. **Before that**, `393 passed in 35:50` on PR #183's tree, against 28:56 uninstrumented. **Before that**, `381 passed in 32:09` on PR #180's tree, against 25:15 uninstrumented. **Before that**, `378 passed in 32:41` on PR #179's tree, against 25:45 uninstrumented. **Before that**, `378 passed in 32:09` on PR #178's tree, against 25:05 uninstrumented. **Before that**, `372 passed in 32:04` on PR #177's tree, against 25:25 uninstrumented — instrumentation's cost is the difference between two runs on one machine. **Before that**, `364 passed in 31:35` on PR #176's tree, and `363 passed in 31:15` on PR #175's tree, and `360 passed in 30:48` on PR #174's tree, and `345 passed in 30:13` on PR #173's tree, and `340 passed in 29:39` on PR #172's tree, and `330 passed in 29:56` on PR #169's tree, and before that `327 passed in 29:28`, and before that `299 passed in 29:18`, and before that all passing, zero skips and zero sanitizer reports; the live Binance modules are excluded from this job. `280 passed in 29:06` on the GitHub runner for this commit. **This row was four behind, and the tool that exists to prevent that had already printed the right number**: the citation above it named [PR #139's run](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/runs/34947630036), which reported `273 in 29:06`, while the cell said `269 in 28:17` — a run on an older tree. `scripts/test_table.py` prints all three counts in one block precisely so that one edit carries them together, and the previous table commit carried two of the three. Reading it is the part a script cannot do — and this job is what closed #122: it turned **red** on the pull request for #117 with a race on `unique_ptr::reset`, which is the only reason that defect is closed rather than filed. Read it against the **22:02** the same runner gave the uninstrumented battery rather than against this machine's number: instrumentation's cost is the difference between two runs on one machine, and every wait in the stage B and stage C windows scales with `patience()` on top of it |

#54's nine — six for the fault injector and three for what the engine does with a refused WAL
write — run in both integration jobs, and both counts above are from the same CI run rather than
from a local one. That the TSan job reports **the same count as the row above it**, with zero
skips, is what establishes something the design left open: an injector compiled with
ThreadSanitizer preloads cleanly into a server compiled with it, measured instead of argued. The
count is not repeated here on purpose — a second copy of a number is the drift this section already
has two entries about.

The seven CLI tests run in both integration jobs. Both build `ob_cli`, and the fixture selects the
binary beside the server under test. No `xfail` remains.

`ctest -j1` is not a preference. The network tests bind ports, so a parallel run fails for a reason
that has nothing to do with the code under test.
