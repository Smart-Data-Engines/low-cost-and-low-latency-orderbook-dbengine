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
  `execute_command`. No CI job builds the io_uring file, so a static test refuses an
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
  built by no CI job.
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
  mutation-checked in both directions; the remaining reconnects are #93.
- **The test for it needed a measured number, not a generous one.** With neither side setting a
  buffer size, the loopback pair absorbed **2.6 MB** before the sender first saw `EAGAIN`, so a 2 MB
  version of that test passed against the defect. Shrinking the receiver's window to 4 kB reproduced
  it reliably and made the test take 49 seconds; 8 MB of WAL and no window tricks reproduce it in
  0.66 s. Pitfall 123 again: a probe that does not reproduce the shape says "no defect" in the same
  voice as one under which there is none.
- **The io_uring refusal stays broad, and the reason is coverage rather than epoll.** The node links
  have their own loops and would work in that build. No CI job builds that file, so a surface that
  "should work" there is a surface nobody has run, and `--tls-*` must never turn out to mean
  plaintext. Said in the refusal message rather than implied.
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
account there, which is a decision with an owner rather than a task, and it was decided against for
now.

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

**Correction, from #83.** The line below said "697 tests clean under ASan+UBSan and under TSan", and
this entry said so from the day the jobs went in. It was true of the test binaries and the server and
**not of the twenty-eight static libraries** — `add_compile_options()` only affects targets created
after the call, and these blocks sat past every one of them. UBSan needs instrumentation to see
anything, so undefined behaviour in library code was not being checked at all. #83 has the evidence,
the fix and what survives of the original claim.

- Effort: S | Impact: 697 tests clean under ASan+UBSan and under TSan, checked on every push — with
  the qualification above until #83 made it true of the whole tree. Two defects found by turning them
  on, one of them undefined behaviour on the hot path's data type

### 38. Fuzzing
- libFuzzer harnesses for `command_parser`, the multi-master frame parser, and WAL record
  deserialization. These are the three places that read untrusted bytes
- Corpus in-repo, short fuzz run in CI, optional OSS-Fuzz submission
- Effort: M | Impact: Finds the class of bug that property tests miss; also a credibility signal

### 39. Reproducible comparative benchmarks

**Part one is done and merged (PR #70): the harness, the dataset, the resolution measurement and our
own adapter.** What is left is the three competitor adapters, the publication step, and the
multi-system checkpoint — all of which need ClickHouse, TimescaleDB and kdb+ installed natively,
which is a decision about this machine rather than code. Spec:
`kiro-workspace/specs/reproducible-benchmarks/`.

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

### 54. Chaos and fault injection
- Network partitions between multi-master peers, packet loss and reorder, disk-full, fsync failure,
  clock skew (HLC correctness under skew is untested), etcd unavailability
- Effort: L | Impact: The failure modes that lose data in production

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

### 97. One unreachable peer address stops every write on the node **P0**

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

The fix has two halves and only the first is small: dial **outside** `mtx_` and re-check under it
before installing the record — which still leaves the reconnect thread, and therefore every other
peer's dial, stuck behind one dead address — and then make the connect non-blocking with a bounded
deadline, driven from the EPOLLOUT machinery that already exists for writes. The two copies of the
dial should become one on the way.

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

### 93. Catch-up above the send-buffer ceiling costs one reconnect per 16 MB

Named by #30 part three series D rather than fixed by it, because the fix there was the one that
belonged with the change: every write to a replica now goes through `enqueue_send()` and the
EPOLLOUT drain, which is the only shape in which a socket saying "come back later" has anywhere to
say it — and the only shape in which `SSL_ERROR_WANT_WRITE` does.

What that left: `handle_catchup()` still streams the whole requested WAL range in one synchronous
pass, so a replica that is not draining reaches the 16 MB queue ceiling and is dropped. It reconnects
and resumes from the position it confirmed, so progress is monotonic — but a replica a gigabyte
behind needs sixty-odd reconnects to get there. Before series D the same thing happened at **one
socket buffer** (measured: 2.6 MB on loopback with no buffer sizes set, ~208 kB with
`net.core.wmem_default`), so this is two orders of magnitude better and still not right.

The answer is a catch-up cursor on `ReplicaInfo` — the file index and offset reached, plus a flag —
resumed from the EPOLLOUT branch. That is not a new mechanism: `continue_snapshot_transfer()` in the
same class already does exactly this for snapshot streaming, so the shape is in the file, one
function away.

Worth stating what this is *not*: a replica far enough behind that the WAL no longer covers its
position already falls through to snapshot bootstrap (`ERR WAL_TRUNCATED`), which is the path for the
genuinely-far-behind case. This is about the band in between.

- Effort: S | Impact: removes the reconnect loop from a large catch-up


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

### 86. A required check is flaky, and the assertion that flickers is asserting a race

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
  **So the node was signalled or it died, and the likeliest producer is memory.** ThreadSanitizer
  multiplies a process's footprint several times, this job runs three nodes plus etcd on one shared
  runner, and an OOM kill arrives as `SIGKILL` with no report of any kind — which fits every
  observation: only under TSan, only sometimes, no race report, and a refusal rather than a timeout.
  The assertion now prints the exit status, so `signal 9` would settle it. Guessing beyond that is
  not worth it: the next red run reports which, because the three layers above no longer discard the
  answer.
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
- Effort: S for the test half (done), S for the diagnostic half (done), M for the server half |
  Impact: a required check that fails at random trains everyone to re-run it, which is how a real
  failure gets re-run too

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

No P0 is open, and none of the correctness work is. Every P0 that has been raised — #60, #61, #62,
#64, #68, #73, #74 — is closed, and two of those were found by running a real cluster rather than by
reading the code (#73 while proving #70, #82's true cause while proving #82's smaller half).

**What that changes about this table.** Every remaining item is a capability or a proof, not a
defect. The ordering below is therefore about who we want to be able to say yes to, and the first
three rows are one answer: a reader can build the engine and read its tests, but cannot deploy it,
cannot verify its numbers, and cannot put it on a network they do not fully control.

| Priority | Item | Effort | Why now |
|----------|------|--------|---------|
| **P1** | Deployment artifacts (#33) | M | Cheapest large jump in time-to-first-run; today a first run means reading CMake |
| **P1** | Reproducible comparative benchmarks (#39) | L | Makes the performance claim verifiable by a reader instead of asserted, and the claim is the reason the repo exists |
| **P2** | Worked example on live market data (#43) | S | `scripts/binance_live_bootstrap.py` already runs the two-node case end to end on a live feed; what is missing is the write-up and a dashboard |
| **P2** | Configuration file (#32) | S | Ops ergonomics; past twenty flags, flags alone are unreasonable |
| **P2** | Documentation site (#40) | M | Lowers evaluation friction |
| **P2** | Release engineering + PyPI wheels (#42) | S | `pip install` is the shortest path to a first user |
| **P2** | Streaming subscriptions on the wire (#45) | M | The embedded half works; the network half is the one a reader assumes from the feature list |
| **P3** | Time-bucketed aggregation (#44) | L | The most-requested analytical capability for this data |
| **P3** | Arrow output (#46) | M | Near-zero integration cost for analytics teams |
| **P3** | Backup and restore (#34) | M | Table stakes for a database |
| **P3** | Fuzzing (#38) | M | Finds the class of bug property tests miss, in the three places that read untrusted bytes |
| **P4** | Chaos testing (#54) | L | Do this once there are users whose data can be lost |
| **P4** | Performance frontier (#49-50) | varies | Proves the bespoke-engine claim; pick one and write it up |

## Known gaps and honest caveats

Things a reviewer will notice, listed here so they do not look like oversights:

- **TLS on client sessions only.** All three surfaces authenticate (`--auth-secret-file`,
  `--cluster-secret-file`), and since #30 part three the client port can be encrypted with
  `--tls-client` — TLS 1.3, and both shipped clients verify the chain *and* the name by default. The
  **replication link and the multi-master mesh are still plaintext**: they authenticate, and every
  record they carry is readable on the path. So a cluster still wants a network you trust between
  its nodes, while the clients talking to it no longer do.
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
  told `busy` rather than queued.
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
| PING latency (io_uring) | ~24 µs avg | Python client, loopback |
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

| Suite | Count | Status |
|-------|-------|--------|
| C++ (GTest + RapidCheck) | 673 | all passing, ~152s with `ctest -j1` on machine B |
| Python integration | 121 | passing, plus 2 skipped. **No xfails left**: #60's and #61's markers both fell with their fixes |
