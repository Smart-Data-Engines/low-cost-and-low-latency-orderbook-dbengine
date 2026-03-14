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

1. Replay WAL records to recover any updates not yet persisted to the columnar store.
2. Scan the columnar store directory and rebuild the segment index from `meta.json` files.
3. Start the background flush thread.

### Shutdown (close)

1. Stop the background flush thread.
2. Flush all pending rows to the columnar store.
3. Flush each columnar segment's metadata.
4. Flush the WAL to disk.

## Key Design Decisions

**Append-only storage** — No in-place updates or deletes. This simplifies crash recovery and enables lock-free reads. Orderbook data is naturally time-series: you rarely need to modify historical snapshots.

**SoA layout** — Prices, quantities, and order counts are stored in separate arrays (Struct-of-Arrays) rather than an array of structs. This improves cache utilization for aggregation queries that scan a single column.

**Seqlock concurrency** — A single writer can update the orderbook while multiple readers get consistent snapshots without locks. The version counter (odd = writing, even = stable) lets readers detect torn reads and retry.

**Segment-based partitioning** — The columnar store splits data into time-bounded segments. Each segment is a directory containing column files (`price.col`, `qty.col`, `ts.col`, `cnt.col`) and a `meta.json` descriptor. This enables efficient time-range pruning.

**Delta+zigzag compression** — Orderbook prices are highly correlated between consecutive levels. Delta encoding followed by zigzag encoding produces small integers that compress well with Simple8b bit-packing.
