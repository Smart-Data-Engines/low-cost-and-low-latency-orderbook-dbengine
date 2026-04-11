# Roadmap — orderbook-dbengine

Development plan towards production quality, High Availability, and HFT readiness.

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

## Phase 5 — HFT Production Readiness

### 18. Observability stack (Prometheus + structured logging)
- Prometheus `/metrics` HTTP endpoint with counters/gauges/histograms
- Structured JSON logging (replace fprintf(stderr) with structured logger)
- Distributed tracing headers (optional, for multi-node debugging)
- Effort: M | Impact: Critical for production ops

### 19. Failover integration tests with real etcd
- Docker-based test environment with etcd cluster
- Test full failover cycle: primary crash → replica promotion → client reconnect
- Test split-brain recovery with real network partitions
- Gate behind `OB_INTEGRATION_TESTS` env var
- Effort: M | Impact: Confidence in HA correctness

### 20. C++ native client library
- Zero-copy TCP client with connection pooling
- Binary wire protocol option (avoid text parsing overhead on hot path)
- Header-only or static library, no external dependencies
- Effort: L | Impact: 10-100x lower latency than Python client

### 21. io_uring transport layer
- Replace epoll with io_uring for async I/O (Linux 5.1+)
- Submission queue batching for multiple concurrent connections
- Zero-copy receive with registered buffers
- Effort: L | Impact: ~30-50% latency reduction on modern kernels

### 22. Symbol-based sharding
- Different symbols on different nodes, routing layer in client
- Shard map stored in etcd, auto-rebalancing on node add/remove
- Prerequisite: C++ client (#20) for efficient cross-shard routing
- Effort: XL | Impact: Horizontal scalability

### 23. Multi-master replication
- Any node accepts writes, data propagated to all other nodes
- Conflict resolution via vector clocks or CRDTs
- Prerequisite: sharding (#22) and solid failover (#9-11)
- Effort: XL | Impact: Write scalability, geo-distribution

## Recommended Order

| Priority | Item | Effort | Why next |
|----------|------|--------|----------|
| P1 | Observability (#18) | M | Can't run production without metrics/logging |
| P1 | etcd integration tests (#19) | M | Validates HA before real deployment |
| P2 | C++ client (#20) | L | Unlocks HFT hot path, prerequisite for sharding |
| P2 | io_uring transport (#21) | L | Major latency win on modern Linux |
| P3 | Symbol sharding (#22) | XL | Horizontal scale when single node isn't enough |
| P4 | Multi-master (#23) | XL | Research-grade, only if geo-distribution needed |

S = few days, M = week, L = 2-3 weeks, XL = month+

## Performance Baselines (current)

| Metric | Value | Notes |
|--------|-------|-------|
| PING latency (TCP) | ~60 µs avg | Python client, loopback |
| Single INSERT (TCP) | ~0.3 ms | Python client |
| MINSERT 1000 levels (TCP) | ~3 ms | Python client, single round-trip |
| FLUSH (incremental) | ~2-3 ms | Non-blocking, two-phase |
| Sustained INSERT throughput | 29k/s | Python TCP, 60s stress test |
| Sustained MINSERT throughput | 777k levels/s | Python TCP, 60s stress test |
| Native update latency | ~2.8 µs p50 | C++ benchmark |
| Native ingestion | ~1.35M updates/s | C++ benchmark |
| C++ tests | 281 | GTest + RapidCheck |
| Python tests | 14 | unittest + Hypothesis |
