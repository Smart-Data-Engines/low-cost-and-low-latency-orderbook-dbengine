# Roadmap — orderbook-dbengine

Development plan towards production quality and High Availability.

## Phase 1 — Production Hardening (HA foundation)

### 1. Configurable fsync policy
- Modes: `fsync_every` (full durability), `fsync_interval` (current ~100ms), `fsync_none` (max throughput)
- User chooses the durability vs throughput tradeoff
- Status: **DONE** — `FsyncPolicy` enum, propagated through `WALWriter` → `Engine` → `TcpServer`

### 2. Graceful shutdown with drain
- Reject new connections, finish in-flight commands
- Flush pending rows, then shut down
- Currently has `atomic<bool> running_` but no drain logic
- Status: **DONE** — `draining_` state in `TcpServer`, rejects new connections during drain, waits for active sessions to finish

### 3. WAL truncation / compaction ← P0
- WAL rotates at 512MB but old files are never removed
- After data is confirmed in the columnar store, old WAL files can be safely deleted
- Status: **DONE** — `WALWriter::truncate_before()` + auto-truncation in `Engine::flush_loop()`

### 4. Backpressure on pending_rows_ ← P0
- `std::vector<PendingRow>` grows without limit
- Under high ingestion rate with slow flush → OOM
- Bounded queue with backpressure (block writer when full)
- Status: **DONE** — `MAX_PENDING_ROWS=1M` + `condition_variable` backpressure in `apply_delta()`

### 5. Monitoring endpoint ← P0
- `STATUS` returns basic stats but lacks:
  - WAL size, pending rows count, segment count
  - Memory usage, uptime
- Critical for production operations
- Status: **DONE** — `Engine::stats()` + extended `ServerStats` with `pending_rows`, `wal_file`, `segments`, `symbols`

## Phase 2 — Replication (HA foundation)

### 6. WAL streaming replication
- Primary sends WAL records to replica(s) via a dedicated TCP stream
- Replica replays WAL and maintains its own SoA buffer + columnar store
- Protocol: `REPLICATE <file_index> <byte_offset>\n` / `WAL <fi> <off> <len>\n<binary>` / `ACK <fi> <off>\n`
- Status: **DONE** — `ReplicationManager` (epoll, non-blocking send buffers, EPOLLOUT drain), `ReplicationClient` (BufferedReader, CRC32C verify, exponential backoff reconnect), shared `crc32c.hpp` header

### 7. Read replicas
- Replica accepts SELECT/aggregation, rejects INSERT/FLUSH
- Client connects to replica for reads, to primary for writes
- Status: **DONE** — `--read-only` flag in TcpServer rejects INSERT/FLUSH, replica Engine replays via `apply_delta()`

### 8. Replica lag monitoring
- Primary tracks confirmed offset per replica
- Exposed in `STATUS` and as a metric, alert when lag > threshold
- Status: **DONE** — per-replica `lag_bytes` in `Engine::Stats`, exposed in STATUS response, WAL truncation respects slowest replica

## Phase 3 — High Availability

### 9. Automatic failover
- Replica with the highest confirmed WAL offset promotes itself to primary
- Option A: External coordinator (etcd/consul) — simpler, proven
- Option B: Built-in Raft — zero external deps, more work
- Recommendation: start with option A

### 10. Client-side failover
- Python bindings: `OrderbookEngine(hosts=["primary:5555", "replica1:5556"])`
- Automatic switchover to new primary after failover

### 11. Fencing / split-brain protection
- WAL epoch counter: each new primary increments the epoch
- Replicas reject writes from the old epoch

## Phase 4 — Scalability and features

### 12. Symbol-based sharding
- Different symbols on different nodes, routing layer

### 13. Snapshot-based replica bootstrap
- Consistent snapshot (columnar store + WAL offset) instead of full WAL replay
- Status: **DONE** — `Engine::create_snapshot()` (flush + CRC32C), `Engine::load_snapshot()`, `ReplicationManager` chunked transfer (SNAPSHOT_BEGIN/FILE/END), `ReplicationClient` auto-bootstrap on WAL_TRUNCATED, staging dir for crash safety, manifest CRC verification, bootstrapping state in STATUS

### 14. Wire protocol compression
- Optional LZ4 frame compression on TCP for replication and large query results

### 15. TTL / data retention
- Automatic deletion of segments older than N days

### 16. Multi-master replication
- Any node accepts writes, data propagated to all other nodes
- Requires conflict resolution (sequence number vector clocks or CRDTs)
- Prerequisite: automatic failover (#9-11) and snapshot bootstrap (#13) must be solid first

## Prioritization

| Priority | Item | Effort | Impact | Status |
|----------|------|--------|--------|--------|
| P0 | WAL truncation (#3) | S | Without this, disk fills up | ✅ Done |
| P0 | Backpressure (#4) | S | Without this, OOM in production | ✅ Done |
| P0 | Monitoring (#5) | M | Without this, blind ops | ✅ Done |
| P1 | Graceful shutdown (#2) | S | Data loss on restart | ✅ Done |
| P1 | Fsync policy (#1) | S | Configurability | ✅ Done |
| P1 | WAL streaming (#6) | L | Foundation for everything | ✅ Done |
| P2 | Read replicas (#7) | M | First HA benefit | ✅ Done |
| P2 | Replica lag (#8) | S | Operational necessity | ✅ Done |
| P3 | Failover (#9-11) | XL | Full HA | |
| P3 | Snapshot bootstrap (#13) | L | Replica catch-up | ✅ Done |
| P4 | Sharding, TTL (#12-15) | XL | Scale | |

S = few days, M = week, L = 2-3 weeks, XL = month+
