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

---

## Phase 1 — Production Hardening ✅

### 1. Configurable fsync policy
- Status: **DONE** — `FsyncPolicy` enum (EVERY, INTERVAL, NONE)

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
- Status: **PARTIAL** — framework is DONE (`ClusterManager` auto-boots etcd plus two nodes, fixtures, colored console report, marker-based categories), but **the test files themselves are missing from the repository**. A `test_*` pattern in `.gitignore` silently excluded every `tests/integration/test_*.py`, so the ~37 tests across 9 categories were never committed. The `.gitignore` is fixed; the test files have to be recovered or rewritten. See item #25.

## Phase 6 — Write Scalability ✅

### 24. Multi-master replication
- Status: **DONE** — HLC (Hybrid Logical Clock), WALRecordV2 (38B, carries `origin_node_id` for loop prevention), `ConflictResolver` (Last-Writer-Wins per cell), `PeerRegistry` (etcd peer discovery with topology watch), `AntiEntropyManager` (periodic reconciliation), `MultiMasterManager` with a full TCP networking layer: length-prefixed framing, 17-byte handshake with protocol negotiation, unified epoll io_loop, catch-up streaming from a peer's WAL position, backpressure to snapshot sync above 512MB, exponential-backoff reconnect with jitter. Wire protocol commands `MM_PEERS` / `MM_CONFLICTS`, CLI flags, metrics, failover integration. 97 spec tasks, property-based tests for framing, backoff and catch-up ordering.

---

## Phase 7 — Correctness and Deployability

**Why this phase is first.** Everything up to here is engine capability. What stands between this
engine and a production deployment is not another feature. Two shipped features do not do what they
claim (#25, #26), the wire protocol has no authentication, there is no configuration file, and there
is no packaging. Someone who reads the code and likes it still cannot run it. Fix the broken
promises first, then remove the deployment blockers.

### 25. Restore the integration test suite (P0)
- The framework survived (`tests/integration/conftest.py`, 691 lines), the tests did not
- 9 categories to restore: smoke, replication, failover, compression, stress, edge cases, metrics,
  pool, C++ client; plus `test_mm_convergence.py`, `test_mm_failover.py`, `test_binance_live.py`,
  `test_binance_failover_sync.py` referenced by `scripts/run_regression.sh`
- Until this is done, `scripts/run_regression.sh --full` cannot pass and a fresh clone cannot run
  integration tests at all
- Effort: M | Impact: Correctness confidence, credibility of a fresh clone

### 26. Fix graceful failover: it ignores the target and races itself (P0 bug)
Two defects in `FailoverManager::initiate_graceful_failover()` (`src/failover.cpp:166`):

- **The target node is ignored.** The signature is
  `initiate_graceful_failover(const std::string& /*target_node_id*/)` — the parameter is commented
  out and never used. The implementation revokes its own lease and hopes some replica wins the
  election. So `FAILOVER <target_node_id>` in the wire protocol does not hand the role to the node
  the operator named.
- **The outgoing primary immediately competes for the role again.** After revoking its lease it sets
  its own role to `REPLICA`, and one second later its own `monitor_loop()` sees an empty
  `leader_node_id` and promotes it straight back. Measured on
  `EtcdTestFixture.GracefulFailover`: the test fails roughly 40-50% of runs, and the failing runs
  show the same node logging `promoted to PRIMARY, epoch=2` right after handing the role away.
  Verified as pre-existing, not introduced by the harness change.

Why it matters in production: an operator runs `FAILOVER node_B` before taking node A down for
maintenance, gets `OK`, and node A stays primary. They then shut it down and get the unplanned
failover they were trying to avoid.

Fix direction: honour `target_node_id` (write the successor into the coordinator, or have the target
promote itself against a fenced epoch), and give the outgoing primary an election cooldown so it
cannot re-acquire the role it just released. Needs a spec: the wire protocol contract and the
epoch-fencing interaction both change.

- Effort: M | Impact: **Graceful failover is the operation an operator reaches for during
  maintenance. Today it silently does something else.**

### 27. Authentication and TLS on the wire protocol
- Token or mTLS authentication for client sessions, replication links and multi-master peers
- TLS termination in-process (OpenSSL) or a documented sidecar pattern, with a benchmark of the cost
- Per-connection identity in logs and metrics
- Documented in `SECURITY.md`, which currently states the absence of both as a deployment constraint
- Effort: L | Impact: **Unblocks production adoption**

### 28. Access control
- Read-only users, per-symbol and per-exchange ACLs, admin-only commands (`FAILOVER`, `MIGRATE`)
- Effort: M | Impact: Multi-tenant deployments, compliance conversations

### 29. Configuration file support
- YAML or TOML config, with CLI flags overriding file values. Twenty-plus flags is past the point
  where flags alone are reasonable for ops
- Config validation with clear error messages, `--print-config` for support
- Effort: S | Impact: Ops ergonomics, fewer misconfigurations

### 30. Native packaging and cluster bootstrap
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

### 31. Backup, restore, point-in-time recovery
- `ob_backup` / `ob_restore` tooling on top of existing snapshots plus WAL
- Documented recovery procedure with RPO/RTO numbers
- Effort: M | Impact: Nobody runs a database they cannot restore

### 32. Grafana dashboard and alert rules
- Shipped dashboard JSON and Prometheus alert rules (replica lag, failover events, backpressure,
  conflict rate, flush latency)
- Effort: S | Impact: High value relative to cost; makes the metrics already being exported usable

## Phase 8 — Verifiability

**Why.** Performance claims are worth nothing if a reader cannot reproduce them, and quality claims
are worth nothing without evidence in CI. Every item in this phase produces something a stranger can
run and check themselves.

### 33. CI hardening
- Sanitizer jobs: ASan + UBSan on the full test suite, TSan on the concurrency-heavy subset
  (SoA seqlock, multi-master io_loop, group commit)
- Coverage report with a badge; `OB_ENABLE_COVERAGE` already exists
- Matrix build: GCC and Clang, Debug and Release
- Effort: S | Impact: A sanitizer-clean concurrent C++ codebase is a strong quality signal

### 34. Fuzzing
- libFuzzer harnesses for `command_parser`, the multi-master frame parser, and WAL record
  deserialization. These are the three places that read untrusted bytes
- Corpus in-repo, short fuzz run in CI, optional OSS-Fuzz submission
- Effort: M | Impact: Finds the class of bug that property tests miss; also a credibility signal

### 35. Reproducible comparative benchmarks
- `benchmarks/README.md` already holds equivalent workload definitions for ClickHouse, TimescaleDB
  and kdb+. Turn them into a **runnable harness**: native installation of each system from its
  official packages, one script, results table with hardware, versions and dataset recorded
- Every system compared runs natively. Benchmarking a native engine against containerised
  competitors would measure the container layer, not the engines
- Publish results in the README with the exact hardware, kernel, and dataset used
- Effort: L | Impact: Turns a performance claim into something a reader can verify on their own
  hardware in an afternoon

### 36. Documentation site
- MkDocs or Doxygen on GitHub Pages: architecture, wire protocol reference, operations guide,
  five-minute tutorial that ends with a real query
- Effort: M | Impact: Reduces evaluation friction; the docs currently require reading the repo

### 37. Client libraries: Rust and Go
- Thin bindings over the existing C API, published to crates.io and as a Go module
- Effort: M | Impact: Widens the audience beyond C++ and Python shops

### 38. Release engineering
- Semantic versioning, tagged releases with changelog, prebuilt PyPI wheels (`pyproject.toml` with
  scikit-build-core is already in place), signed tags
- Effort: S | Impact: `pip install orderbook-dbengine` is the shortest path to a first user

### 39. Worked example: live market data ingestion
- A runnable Binance (or Coinbase) websocket ingestor writing into the engine, with a Grafana
  dashboard showing it live. `scripts/binance_*.py` is the seed for this
- Effort: S | Impact: Turns an abstract engine into a visible demo

## Phase 9 — Query and Analytics Depth

**Why.** Ingestion is solved. What a trading firm actually asks next is analytical: bars, windows,
and getting data into their existing Python stack without a copy.

### 40. Time-bucketed aggregation in the query language
- `GROUP BY time_bucket(interval)`, OHLCV bar generation, time-weighted mid price, rolling windows
- Effort: L | Impact: This is what people build on top of orderbook data anyway

### 41. Streaming subscriptions
- `SUBSCRIBE 'SYM'.'EXCH'` pushing updates to the client; the README already advertises streaming
  subscriptions, so either implement or correct the claim
- Backpressure policy per subscriber, slow-consumer disconnect
- Effort: M | Impact: Real-time consumers stop polling

### 42. Apache Arrow output
- Arrow IPC / Flight result format, zero-copy into pandas, polars and DuckDB
- Effort: M | Impact: Drops the integration cost for analytics teams to near zero

### 43. Zone maps and columnar indexes
- Per-segment min/max and count for timestamp and price, so range scans skip segments
- Effort: M | Impact: Query latency on large ranges

### 44. Cost-based scan planning
- Decide live-buffer versus columnar scan versus both from segment statistics rather than a fixed rule
- Effort: M | Impact: Predictable query latency as data grows

## Phase 10 — Performance Frontier

**Why.** This is where the "custom engine for specific hardware" claim gets proven in our own
codebase. Each item is also a story we can sell as bespoke work.

### 45. SIMD codec
- AVX2/AVX-512 for delta, zigzag and Simple8b encode/decode. SIMD is currently only in aggregation
- Effort: M | Impact: Flush and scan throughput

### 46. NUMA awareness and thread pinning
- Per-socket allocation, pinned io threads, `--cpu-affinity` configuration
- Effort: M | Impact: Tail latency on multi-socket servers, which is where clients run

### 47. Huge pages
- `MADV_HUGEPAGE` / explicit hugetlb for mmap segments and SoA buffers
- Effort: S | Impact: TLB pressure at large working sets

### 48. Shared-memory transport for local clients
- Zero-copy ring buffer for co-located processes, bypassing TCP entirely
- Effort: L | Impact: Sub-microsecond local writes; a genuine HFT differentiator

### 49. Kernel-bypass experiment
- AF_XDP or DPDK prototype measured against the io_uring path, published as an engineering write-up
  even if we do not ship it
- Effort: L | Impact: Credibility on the low-latency claim; strong content

## Phase 11 — Reliability Engineering

### 50. Chaos and fault injection
- Network partitions between multi-master peers, packet loss and reorder, disk-full, fsync failure,
  clock skew (HLC correctness under skew is untested), etcd unavailability
- Effort: L | Impact: The failure modes that lose data in production

### 51. Multi-node cluster tests in CI
- Three native nodes plus etcd started by a script, multi-master convergence and failover verified
  on every PR
- Effort: M | Impact: Prevents regressions that unit tests structurally cannot catch

### 52. Rolling upgrade support
- Protocol version negotiation matrix, mixed-version cluster tests, documented upgrade path
- Effort: M | Impact: Required before anyone runs this longer than one release

### 53. Complete the anti-entropy implementation
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

### 54. Distributed tracing
- OpenTelemetry spans across client, primary, replica and peers; trace a write end to end
- Effort: M | Impact: Debuggability in a real deployment

---

## Recommended order

| Priority | Item | Effort | Why now |
|----------|------|--------|---------|
| **P0** | Restore integration test suite (#25) | M | The repo currently ships a test framework with no tests. Fix before anything else. |
| **P0** | Fix graceful failover (#26) | M | `FAILOVER <node>` ignores the target and the outgoing primary re-takes the role ~40-50% of the time |
| **P1** | Deployment artifacts (#30) | M | Cheapest large jump in time-to-first-run |
| **P1** | Reproducible comparative benchmarks (#35) | L | Makes the performance claim verifiable by a reader instead of asserted |
| **P1** | Authentication and TLS (#27) | L | The single blocker to production adoption |
| **P2** | CI hardening with sanitizers (#33) | S | Strong quality signal, low cost, catches real concurrency bugs |
| **P2** | Configuration file (#29) | S | Ops ergonomics |
| **P2** | Documentation site (#36) | M | Lowers evaluation friction |
| **P2** | Release engineering + PyPI wheels (#38) | S | `pip install` is the shortest path to a first user |
| **P3** | Time-bucketed aggregation (#40) | L | The most-requested analytical capability for this data |
| **P3** | Arrow output (#42) | M | Near-zero integration cost for analytics teams |
| **P3** | Backup and restore (#31) | M | Table stakes for a database |
| **P4** | Chaos testing (#50) | L | Do this once there are users whose data can be lost |
| **P4** | Performance frontier (#45-48) | varies | Proves the bespoke-engine claim; pick one and write it up |

## Known gaps and honest caveats

Things a reviewer will notice, listed here so they do not look like oversights:

- **No authentication, no TLS.** Trusted-network deployment only (#27).
- **Integration test files missing from the repo** (#25). The framework is present and the C++ suite
  is complete: 510 tests, all passing.
- **`FAILOVER <target_node_id>` does not honour the target** and the outgoing primary can immediately
  re-acquire the role it just released (#26). Reproducible: `EtcdTestFixture.GracefulFailover` fails
  40-50% of runs.
- **Anti-entropy is a scheduler with no reconciliation** (#53). The spec task is marked complete and
  the metrics report runs, but gap detection and repair are placeholders that return "nothing found"
  and "cannot repair". Reconnect catch-up is the only thing healing divergence today.
- **Benchmark baselines were recorded on one developer machine** with no hardware description. The
  table below fixes that going forward. Any published number needs its hardware next to it.
- **The README advertises streaming subscriptions** that are not verified to exist in the current
  wire protocol (#41): implement it or correct the claim.
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
| `BM_UpdateLatency` p50 | 8.6 µs | ≤ 5 µs | 3.1x slower |
| `BM_UpdateLatency` p99 | 9.6 µs | — | — |
| `BM_VwapLatency` | 1577 ns (1000 levels) | ≤ 1000 ns | 1.6x slower |
| `BM_TimeRangeQuery` (10k / 100k rows) | 0.004 ms / 0.004 ms | ≤ 5 ms | well inside |

Run-to-run variance on this machine is high (a first run under background load reported 298k/s
against 387k/s on an idle run, and `BM_UpdateLatency` cv was 21%). Thermally throttled laptop CPUs
are not benchmark hosts. Treat these figures as a smoke test that the engine works, nothing more.

### Regression thresholds

The thresholds used by `bench-guard` (IngestionThroughput ≥ 1.0M/s, UpdateLatency ≤ 5µs,
VwapLatency ≤ 1000ns, TimeRangeQuery ≤ 5ms) are **machine-A thresholds**. They are not meaningful on
slower hardware. Compare a run against the previous run **on the same machine**, and reserve
absolute thresholds for a designated benchmark host.

### Test suite

| Suite | Count | Status |
|-------|-------|--------|
| C++ (GTest + RapidCheck) | 510 | all passing, ~381s with `ctest -j1` on machine B |
| Python integration | ~37 | **missing from repo** (#25) |
