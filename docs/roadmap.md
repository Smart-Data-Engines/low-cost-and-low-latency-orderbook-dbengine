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

### 137. A writer that hits the pending-row ceiling waits for a flush nothing asks for, and the writer is the epoll thread

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

### 136. Writing the same event-time span twice destroys a symbol's segment, and the only diagnosis names a race that did not happen

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
3 a.m.

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

**Two P0s are open: #136 and #137**, both found by the same afternoon on a machine this tree
had never run on, and both filed rather than fixed because each has candidate answers that
differ in what they cost. #136 changes an on-disk layout the snapshot manifest and retention
both address; #137 changes what backpressure means. Every P0 raised before it —
#60, #61, #62, #64, #68, #73, #74, #80, #88 and #97 — is closed, and several were found by running a real cluster rather than by reading the code
(#73 while proving #70, #82's true cause while proving #82's smaller half, #97 from the flicker of
#96's own test).

**Open: #136 and #137.** Every other item above #58 is marked closed, and
`scripts/check_roadmap.py` holds that in both directions — an item whose heading loses its tick has
to appear on this line in the same commit, and one that gains a tick has to leave it. Items #1 to
#58 are planned work nobody has built, not defects, which is what the floor in this line is for.

Read that as narrowly as it is written. It says every defect **that has been filed** above #58 is
closed, and #121, the last of them, was a question rather than a defect — answered by bounding how
far a peer's clock may move this one, refusing the peer rather than the record, the clock or a
clamp. It does not say the engine is finished; the capability table below is the list of what it is
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

- **Every encrypted surface is off by default, and one transport cannot have it at all.** All
  three surfaces authenticate (`--auth-secret-file`, `--cluster-secret-file`) and all three can be
  encrypted since #30 part three — `--tls-client` for client sessions, `--tls-replication` and
  `--tls-multi-master` for the node links, TLS 1.3 with no configurable floor. Both shipped clients
  verify the chain *and* the name; on the node links verification is mutual and cannot be configured
  otherwise. Four things are still worth a reviewer's notice. **Every one of those flags defaults
  to off**, so a cluster that nobody configured is plaintext on all three surfaces and says so only
  in its startup log. **The io_uring transport refuses every `--tls-*` flag** — for the client port
  because receive stays in userspace even with kernel TLS, so that loop needs a memory-BIO rewrite;
  for the node links because encrypted links on this transport have no runtime tests. The
  `io-uring-build` job (#108) verifies compilation and linking only. The transport remains
  plaintext-only. **Certificate rotation needs a restart**, one node at a time. And **`--tls-peer-names` empty means chain-only verification**,
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

Verified by [the full CI run for PR #149](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/runs/35435355356),
on the tree carrying the aarch64 write-up and the two P0s that run filed, **#136 and #137** — so
the page has open items again, and both were found by the tree being on a machine it had never run
on rather than by reading it. Runtimes below are from GitHub's
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
| C++ (GTest + RapidCheck) | 1102 | all passing with `ctest -j1` on the i3-7100U, **219-235 s in a single run** — **four more than the previous commit, and all four are about the machine this tree had never run on**: two in `tests/test_mm_snapshot.cpp` pin the ten header bytes of a snapshot chunk literally rather than through our own decoder, and a full-size chunk, because the frame is sized once now (PR #146); two in `tests/test_crc32c.cpp` are the pair that makes the rest of that file mean anything on this architecture — one skips with an explanation where there is only the table to compare against itself, which is what every agreement test had been doing off x86, and one requires the implementation the engine *reports* to be the one that runs (PR #147) across the four runs this tree and its two predecessors recorded, against 236-390 s three commits back when another session's containers were resident — the spread, not either end, is what the next number is read against. **Four more than the previous commit, and the arithmetic is worth writing down: six new and two removed** (#121). The six are three in `tests/test_hlc_skew.cpp` — the bound accepts up to itself and refuses one nanosecond past it, a peer behind us is plausible however far behind, and `UINT64_MAX` is refused so the logical carry can never saturate — and three in `tests/test_mm_wire_clock.cpp`, which is the only instrument that can reach this at all: a fake peer framing one DELTA whose HLC says what no real clock would. The two removed are the ones that **pinned the behaviour this decision reverses**, `AnHourInTheFutureOnTheWireBecomesThisNodesClockAndStays` and `ARecordFromAPeerWhoseClockIsWrongIsStillApplied`, both written by #54's stage D to state that nothing bounded the absorption. They were not deleted into a gap: the same file now asserts the opposite about the same wire shape, which is what makes a falling count readable rather than alarming. **And the local number that preceded this row was wrong by exactly those four.** A full local run on this branch printed `1094/1094` against a build directory that had not registered the four; the reconciliation is three measurements agreeing — CI's 1098, `ctest -N` listing 1100, and a local rerun after the merge giving **1098 passed in 218.95 s**. A stale build answers in the same voice it would use if it were right, which this repository has now paid for in five different shapes. **Before it**, unchanged by three commits: #134 deleted two methods no test referenced, and #135's tests are integration ones — three runs of the same suite on the same machine, the slowest with another session's containers resident and ~1 GB actually free. That spread, not any one of its ends, is what the next number is read against: it is wider than anything a commit in this repository has changed. **Seven more than the previous commit, and all seven are #131's.** Five are `LoopGuard`'s own, in the new `tests/test_loop_guard.cpp`: the counter counts every failing iteration rather than every episode, the episode counts consecutive failures and reopens after a recovery, two successes running report nothing (the observable half of "loud once" for a loop that polls ten times a second), a **null** registry still gets loud-once because two of the seven loops run in somebody else's process, and the name it writes is in the registry's own output. The other two are the descriptor `MetricsServer::handle_request` used to leak on the paths that throw — one behavioural, fifty requests against a live server with no growth in `/proc/self/fd`, and one static, because **the throwing path cannot be driven**: nothing in the process can make `serialize()` fail on demand and a knob to make it would be a knob nothing turns in production. **Before them**, four were #112's last two loops. Three are in the new `tests/test_replication_io_boundary.cpp`: that the pacing function returns zero only while a catch-up can progress, that nothing in `run_loop()` assigns `wait_ms` any other way (counted at **three** sites, because losing the one in the `catch` is the regression), and that both `try`s are where they have to be — anchored on the dispatch loop's own line rather than on a log phrase, which is the mistake #128's version of this test made. The fourth is in `tests/test_thread_boundaries.cpp` and is the one worth reading: the set of loops that guard an iteration is **derived from the tree** rather than listed, because a mutation deleting a row from the hand-written list **survived**. Fourteen `void Class::…loop()` definitions in `src/`, one a notifier with no loop in it and named, six of the remaining thirteen guarded and **seven not** — those seven are #131. Checked in both directions, so a loop in neither list fails and a row naming a function the tree no longer has fails too. **Before them**, the most recent addition was #129's: a registry given a 1200-second lease interval has to stop inside two seconds, which is a property stated three orders of magnitude clear of load rather than a duration. **Before it**, four were #128's, and they divide the way that defect does: three in `tests/test_mm_epoll_identity.cpp` are about the shape — that the two reserved event keys cannot collide with a connection, that closing a descriptor takes its registration with it (measured against `dup2`, which forces the reuse the defect needs instead of hoping for it), and that no registration in `src/multi_master.cpp` carries a bare descriptor number. The fourth is behavioural: a connection landing on the descriptor its predecessor gave back is its own connection, with both numbers read back so a run where the kernel did not recycle the number says so rather than passing quietly. Three of the six mutations in that item's table are killed by the static test **and by nothing else**, which is what says it carries weight. **Before them**, two were #126's, and they pin the replayer's rule from both sides: a checksum mismatch in an earlier WAL file yields the records from the file behind it, and one in the **last** file still stops replay — that one is a crash tail, and reading past it would hand the engine a record the process never finished writing. **Earlier**: two were #54's D3, three #125's, six #124's, seven #123's, six #118's, seven #117's. `tests/test_iouring_instrumentation.cpp` adds four that read a source file this build does not compile, which is the only check available for the rest of that transport. CTest lists **1085**: two are `DISABLED_` measurement harnesses (`MMSnapshotMeasurement.SnapshotCreationCost`, `ReplicationProtocolTest.TheWritePathWaitOfALargeCatchup`) that print measurements rather than assert them. The count that passes and the count CTest lists differ by exactly those two harnesses, always; a row two commits back gave one number for both. The runtimes are what this machine gave on the commit measured, not a budget |
| Python integration | 273 | all passing, plus the two collection-time Binance opt-in skips (`OB_BINANCE_TESTS=1`). Those skips are not part of the 273; count pytest's final result rather than the report plugin's progress characters. `273 passed, 2 skipped in 22:14` on the GitHub runner for this commit — and **29:20 for the same 273 under TSan**, which is the job that has to be read as well, because a battery that skips under instrumentation reads as green. Against `20:37` on the development machine (i3-7100U, native etcd) **for the 263-test tree seven commits back** — the figure is kept as the spread to expect between the two machines, and labelled with the tree it came from rather than silently paired with a count it never measured. **Unchanged by #121, and the reason is the instrument rather than the effort**: producing a real node whose physical clock is five minutes off needs the host clock moved or a time namespace, which is not something this battery can do to the machine it runs on — so the assertion lives at the wire instead, where #54's stage D already built the fake peer for it. **Before it, one more than the commit before, in the existing `test_failover_storage_faults.py` beside the control that was already there** (#130): what a node *says* while it holds a leader key it won and cannot act on. The assertion with teeth is sampled rather than read once — `ROLE` must never name this node's own replication port as the primary it follows, across a twenty-second window in which the pre-fix code answered exactly that on **every** sample. The second assertion is that the condition is reported **once**, with what an operator can do about it, because the loop runs every second and the storage that refused the record usually goes on refusing it. Measured against the two source files from the commit before that fix: **1 failed, 2 passed in 66.8 s**, the failure arriving on the **first** sample with `REPLICA 127.0.0.1:43273 2` and #112's two tests untouched. **Before it, two more, both in the new `test_coordinator_endpoint_order.py`, and the second is a control rather than a second case** (#135): the same unreachable coordinator endpoint in the harmless position, which passed before that fix and has to keep passing — without it, a harness that quietly stopped prepending anything would leave the first test green and meaningless. Both assert their premise from `Popen.args`, the command line the node actually got, rather than from the attribute that put it there, because an attribute is what the harness *meant* to say. The first also requires the mesh to form on top of both registrations, because that is the only assertion reaching the third of the three call sites: two registered nodes that never see each other is a topology watch still reading the wrong endpoint. Measured against the four source files from the commit before that fix, harness and tests unchanged: **1 failed, 1 passed in 50.1 s**, the failure naming both nodes. **Before them, one more than the commit before that, and all three in `test_peer_lease_lost.py` were rewritten**, because #132 turned the first one's premise inside out: it was written to assert that the registration **does not** come back, with a note saying that the day that loop learns to re-register is the day it fails. That day was this commit. It now polls **both** halves — the key and the log line — because the key lands in etcd before the line lands in the log, and reading the log once at the moment the key appears is a race the first rewrite lost. The #133 test needed a **new premise** as well (`stop_etcd()` rather than a revoke, which also exercises the branch that keeps this fix quiet), because after #132 a revoked lease is no longer a permanent condition, and counting log lines over a condition that repairs itself counts a condition that happened once. **The third is the one that says the gate is real**: a refusal over a key that **exists**, which nothing else in this battery produces — `redirect_peer()` writes without a lease, so the test captures the lease id before redirecting and revokes it by id afterwards. Its control is the *premise* rather than the outcome, because without asserting that a refusal reached the log it passes against a node whose refresh is succeeding. The other two gained a rate bound that is independent of wording: the count of registry lines above `DEBUG` across the window, measured at one and two. This tree's battery ran locally only as **the one module** (3 tests in **40 s**, and stage C's twelve in **3:37** as the regression check on the fixture this fix had to leave alone); the whole battery on this commit is CI's, and the 224-235 s `ctest` above is local. **Before them**, two were #112's `monitor_loop` half and both in the new `test_failover_storage_faults.py`: a replica whose data directory refuses the `EPOCH` record a role transition writes loses that monitor tick and not the thread, and its control at a size nothing writes, which must inject nothing. The assertion that carries the guarantee in the first of the two is the **recovery** line rather than the error line — only a later tick can write it, so a run in which the thread died would report the error and then say nothing, which is what a boundary is for. The pair costs **45.7 s** locally, and the module asserts its own premise: `OB_FAULT_PATH=ob_node1_` names the replica's data directory only because `ClusterManager.start()` waits for node-0 to hold PRIMARY before it starts node-1, and a change to that ordering would aim the injector at the **primary's** startup promotion, which exits the process. **Before them**, two were #112's `io_loop` half and both in the new `test_mesh_storage_faults.py`: a mesh receiver whose WAL refuses one record still receives the ones after it (**38.6 s**, because it waits for a mesh to form and for four records to cross it), and its control at a size nothing writes, which must inject nothing (**28.2 s**); that pair costs **67 s** locally. **Before those**: three were #54's A2.2 — the torn-record measurement behind #126, which costs 1.9 s — #125's — a killed replica whose confirmed WAL file retention has removed comes back with every row — and #54's C4, a mesh peer that stopped reading, which costs **9.0 s** and ~2.9 MB of writes because that is where the kernel stops absorbing them. The three before it were #124's — the first tests in this battery to cross a WAL file boundary — and the four together cost **23 s** locally, because the threshold they rotate at is 65573 bytes rather than 512 MB. The ten before them were #54 stage C, and they are most of the **16:24 → 19:18** change: each proxied-mesh test starts three nodes behind a proxy and converges on row content |
| Python integration under TSan | 273 | all passing, zero skips and zero sanitizer reports; the live Binance modules are excluded from this job. `273 passed in 29:18` on the GitHub runner for this commit. **This row was four behind, and the tool that exists to prevent that had already printed the right number**: the citation above it named [PR #139's run](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/runs/34947630036), which reported `273 in 29:06`, while the cell said `269 in 28:17` — a run on an older tree. `scripts/test_table.py` prints all three counts in one block precisely so that one edit carries them together, and the previous table commit carried two of the three. Reading it is the part a script cannot do — and this job is what closed #122: it turned **red** on the pull request for #117 with a race on `unique_ptr::reset`, which is the only reason that defect is closed rather than filed. Read it against the **22:02** the same runner gave the uninstrumented battery rather than against this machine's number: instrumentation's cost is the difference between two runs on one machine, and every wait in the stage B and stage C windows scales with `patience()` on top of it |

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
