# Architecture Overview

## System Design

The engine is composed of six subsystems, each responsible for a specific concern:

```
                    ┌─────────────────────────────────────────┐
                    │              Engine Facade               │
                    │  (owns and coordinates all subsystems)   │
                    └──────────┬──────────────────┬───────────┘
                               │                  │
              ┌────────────────▼──┐          ┌────▼────────────┐
              │    WAL Writer     │          │   Query Engine   │
              │  (crash recovery) │          │  (SQL parser +   │
              │                   │          │   execution)     │
              └────────┬──────────┘          └────┬────────────┘
                       │                          │
              ┌────────▼──────────┐          ┌────▼────────────┐
              │   SoA Buffer      │          │  Aggregation    │
              │  (in-memory L2    │          │  Engine         │
              │   orderbook)      │          │  (VWAP, spread, │
              │                   │          │   imbalance...) │
              └────────┬──────────┘          └─────────────────┘
                       │
              ┌────────▼──────────┐
              │  Columnar Store   │
              │  (time-partitioned│
              │   segments on     │
              │   disk via MMAP)  │
              └───────────────────┘
```

## Data Flow

### Write Path (apply_delta)

1. **WAL write** — The delta update is serialized and appended to the WAL file with a CRC32C checksum. `fsync` ensures durability before any state mutation.

2. **SoA buffer update** — The seqlock writer protocol increments the version to odd, writes the new price levels, then increments to even. Readers spin-wait on even versions for consistent snapshots.

3. **Gap detection** — If the sequence number is not consecutive (`seq != prev_seq + 1`), a gap event is recorded in the WAL.

4. **Columnar enqueue** — SnapshotRows are enqueued for the background flush thread, which periodically drains them into the columnar store.

5. **Subscriber notification** — Streaming query callbacks are invoked synchronously within the apply_delta call.

### Read Path (query)

1. **Parse** — The SQL string is parsed by a hand-written recursive-descent parser into a `QueryAST`.

2. **Plan** — The query engine determines whether this is a scan, aggregation, or snapshot query.

3. **Scan** — For time-range queries, the columnar store's segment index is consulted. Segments outside the time range are pruned. Matching segments are decoded (delta+zigzag for prices, Simple8b for volumes) and filtered.

4. **Aggregate** — For aggregation queries (VWAP, spread, etc.), the live SoA buffer is read using the seqlock reader protocol, and the aggregation engine computes the result.

5. **Return** — Results are delivered via a callback function, one row at a time.

### Startup (open)

1. Scan the columnar store directory and rebuild the segment index from `meta.json` files. This
   happens **first**, because recovery needs to know what is already durable.
2. Replay the WAL records written after the last `CHECKPOINT` record and apply them to the SoA
   buffer and the pending-row queue. A record whose timestamp is at or below the highest `end_ts_ns`
   of an existing segment for its symbol is skipped: those rows are already durable.
3. If anything was replayed, flush it into a segment immediately. `QueryEngine` reads segments, not
   the live SoA buffer, so a recovered row that stays in memory is invisible to every `SELECT`.
4. Read the epoch record, if any, and restore the fencing epoch.
5. Start the background flush thread.

### Shutdown (close)

1. Stop the background flush thread.
2. Flush all pending rows to the columnar store.
3. Flush each columnar segment's metadata.
4. Flush the WAL to disk.

### What the WAL guarantees after a crash

With `FsyncPolicy::EVERY` (the default), a write that has been acknowledged is in a fsynced WAL
record before the acknowledgement leaves the server, and it comes back after a `SIGKILL`, a power
cut, or any other end that skips `close()`. That is the whole point of the log, and until August
2026 it did not hold: `Engine::open()` called replay with a callback that did nothing, so every
acknowledged write not yet flushed to a segment was lost. Nothing in 585 tests noticed, because
every one of them ended in `close()`, which drains and flushes.

A `CHECKPOINT` record (type 6, empty payload) marks how far the log has been made redundant by the
columnar store. It is appended **after** the segment files are written and their metadata merged,
never before: a checkpoint that claims more than is durable turns a crash into data loss, while one
that claims less costs a replay that is skipped anyway.

The checkpoint is deliberately **not** fsynced, even under `FsyncPolicy::EVERY`. It can only ever
claim that rows are already durable, so losing it in a crash makes the next `open()` replay records
the timestamp comparison below then skips. Fsyncing it measured +0.22 ms (+10.5%) on every `FLUSH`,
paid to protect a record whose loss is harmless; without the fsync the cost is not measurable above
run-to-run spread. The next WAL write fsyncs the file anyway, so in practice the checkpoint reaches
the platter moments later.

The remaining window is a crash between writing the segment files and appending the checkpoint. The
records are then replayed even though their rows are durable, which is what the timestamp comparison
in step 2 above exists to catch — without it, replay rewrites an existing segment with whatever the
WAL tail still holds, and since the WAL is truncated only up to the replica-confirmed position, that
tail can hold fewer rows than the segment does. Measured with the comparison removed: eight durable
rows became six.

That comparison uses each segment's `end_ts_ns`, which is the timestamp of the **last** row written
into it rather than the highest, so it assumes timestamps for one symbol arrive in order. A single
node satisfies that, because the server stamps each write on arrival. Multi-master does not: a peer's
record carries the origin's timestamp and can be appended after newer local rows, and a record like
that, replayed inside the crash window, would be skipped as already durable. Narrow, but real, and
tracked as roadmap #63.

`FLUSH` and a clean `close()` both end in a checkpoint, so a restart after either replays nothing.

## Key Design Decisions

**Append-only storage** — No in-place updates or deletes. This simplifies crash recovery and enables lock-free reads. Orderbook data is naturally time-series: you rarely need to modify historical snapshots.

**SoA layout** — Prices, quantities, and order counts are stored in separate arrays (Struct-of-Arrays) rather than an array of structs. This improves cache utilization for aggregation queries that scan a single column.

**Seqlock concurrency** — A single writer can update the orderbook while multiple readers get consistent snapshots without locks. The version counter (odd = writing, even = stable) lets readers detect torn reads and retry.

**Segment-based partitioning** — The columnar store splits data into time-bounded segments. Each segment is a directory containing column files (`price.col`, `qty.col`, `ts.col`, `cnt.col`) and a `meta.json` descriptor. This enables efficient time-range pruning.

**Delta+zigzag compression** — Orderbook prices are highly correlated between consecutive levels. Delta encoding followed by zigzag encoding produces small integers that compress well with Simple8b bit-packing.


## Multi-Master Replication

Multi-master replication extends the engine to accept writes on multiple nodes simultaneously. Each node in a multi-master cluster holds a full copy of the data and can serve both reads and writes.

### Core Concepts

**Hybrid Logical Clock (HLC)** — Each write is stamped with a 12-byte HLC timestamp combining physical wall-clock time (uint64 nanoseconds), a logical counter (uint16), and the node ID (uint16). HLC preserves causal ordering without requiring synchronized clocks across nodes.

**Last-Writer-Wins (LWW)** — Conflicts (concurrent writes to the same price level on different nodes) are resolved deterministically: the write with the higher HLC timestamp wins. For orderbook L2 data, this is semantically correct — the most recent price level update is always the most current.

**Full-Mesh Topology** — For clusters up to 5 nodes, every node connects directly to every other node. WAL records are propagated in a single hop (no re-broadcast), eliminating cascading delays.

**Anti-Entropy** — A background process periodically compares WAL positions across nodes and repairs any gaps. This guarantees eventual consistency even after network partitions or temporary failures.

### Write Path (Multi-Master Mode)

```
                    ┌─────────────────────────────────────────┐
                    │           Client INSERT                  │
                    └──────────────────┬──────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────┐
                    │         HLC tick_local()                 │
                    │  physical = max(wall_clock, last_hlc)    │
                    │  logical = reset or increment            │
                    └──────────────────┬──────────────────────┘
                                       │
              ┌────────────────────────▼────────────────────────┐
              │  WAL append_with_origin(delta, origin, hlc)     │
              │  (38-byte header: seq + ts + crc + origin + hlc)│
              └────────────────────────┬────────────────────────┘
                                       │
              ┌────────────────────────▼────────────────────────┐
              │  ConflictResolver.update_hlc(key, hlc, origin)  │
              │  (track per-level HLC for future conflict check)│
              └────────────────────────┬────────────────────────┘
                                       │
              ┌────────────────────────▼────────────────────────┐
              │  Apply to SoA buffer (same as single-node)      │
              └────────────────────────┬────────────────────────┘
                                       │
              ┌────────────────────────▼────────────────────────┐
              │  MultiMasterManager.broadcast_local()            │
              │  (send WAL record to all connected peers)        │
              └────────────────────────┬────────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────┐
                    │           Return OK to client            │
                    └─────────────────────────────────────────┘
```

### Receive Path (Remote WAL Record)

When a node receives a WAL record from a peer:

1. **Loop prevention** — Check `origin_node_id`. If it matches the local node, discard (prevents infinite loops in full-mesh).
2. **HLC merge** — Call `tick_receive(remote_hlc)` to advance the local clock.
3. **Conflict resolution** — For each price level in the delta, compare `remote_hlc` with the locally tracked HLC for that key. Apply only levels where remote wins (LWW).
4. **WAL append** — Write the record to local WAL preserving the original `origin_node_id`.
5. **No re-broadcast** — The record is NOT forwarded to other peers (single-hop propagation).

### Conflict Resolution Algorithm

```
resolve(key, remote_hlc, remote_origin):
    local_state = level_states[key]
    if not found → NO_CONFLICT (first write)
    if remote_hlc > local_state.hlc → APPLY_REMOTE
    if remote_hlc < local_state.hlc → REJECT_REMOTE
    if equal physical + logical → tie-break by node_id (higher wins)
```

### Anti-Entropy Protocol

Every `--anti-entropy-interval-seconds` (default 30s):

1. Read peer WAL positions from etcd (Peer Registry)
2. Compare with local WAL position
3. For each detected gap: request missing records from the source peer
4. If WAL is truncated (gap too large): trigger full snapshot repair

### Per-Shard Multi-Master

When combined with sharding, each shard operates as an independent multi-master cluster:

- Peer Registry keys are namespaced: `<prefix>shards/<shard_id>/mm_peers/<node_id>`
- Replication streams are isolated per shard
- ShardRouter distributes writes across MM nodes in a shard using round-robin
