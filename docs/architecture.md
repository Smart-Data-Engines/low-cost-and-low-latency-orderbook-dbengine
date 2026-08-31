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

### Sequence numbers and who assigns them

A sequence number belongs to the **origin** that produced an update, not to the node storing it. A
node numbers only the writes it accepted from a client, per `(symbol, exchange)`, and never renumbers
anything that arrived from elsewhere: a replica renumbering its primary's stream, or a multi-master
node renumbering a peer's, would make catch-up compare numbers minted by different nodes.

`0` in `DeltaUpdate::sequence_number` means "unassigned". `Engine::stamp_sequence()` fills it in from
the counter for that symbol; a non-zero number passes through untouched, which is how the replica path
keeps the primary's numbering while sharing `apply_delta()` with client writes. The caller's struct is
never modified — the engine works on a copy.

The state lives in `SequenceTracker` (`src/sequence_tracker.cpp`): a local counter plus the last number
seen from each origin, per symbol. Gap detection is per origin, because a single counter cannot tell a
gap from two origins interleaving — in multi-master every interleave would look like a hole. A number
that is not exactly one past the previous one **for that origin** appends a `GAP` record, increments
`ob_sequence_gaps_detected` and logs the symbol, origin and expected number. The first record from an
origin is never a gap.

Counters are restored at startup from two places, both of which only ever raise them: the highest
number in each segment (`SegmentMeta::max_sequence_number`, published in `meta.json`) and every record
replayed from the WAL tail. Replay *seeds* the tracker rather than assigning, so a gap recorded when
the records were first written is not reported again on every restart. A `meta.json` without the field
was written before numbers existed, and 0 is then the truth about that data rather than a fallback.

Until August 2026 none of this happened: `tcp_server.cpp` set the field to 0 with a comment saying the
engine assigned it, and the engine copied the zero into the WAL header and the stored row. So every
production write carried 0, the `sequence_number` column in every segment was zeros, and the gap check
in `soa_buffer.cpp` — which requires a non-zero previous number — could never fire, leaving `GAP` a
record type that had a unit test and had never been produced by a running server. That flag is still
returned for the C API, whose caller supplies its own numbers and owns a single stream; the engine
ignores it.

Not exposed on the wire: `format_query_response()` sends six columns and the sequence number is not
among them, so a client cannot see it. Adding a column means changing both clients, the header line and
the docs, which is roadmap #65.

### How a node catches up after an outage

A node that reconnects states **what it holds** and the peer sends the complement. It states it as a
*version vector*: for each `(symbol, exchange, origin)`, the highest sequence number below which
nothing is missing. Sequence numbers are minted per origin (see above), so they mean the same thing on
every node that received them, and a hole in them is arithmetic.

The entry is a **frontier, not a maximum**. A reconnecting peer can receive live record 7 before
catch-up delivers 6, and a maximum would report 6 as delivered and never ask for it again. A record
above the frontier is applied — data is data — but the frontier stays where it is, so the next
exchange asks for that range again.

One principle decides every edge case: **when unsure, ask for too much.** A missing entry, a peer that
sent no vector, and a peer that could not fit its vector on the wire all mean the same thing to a
sender: send everything retained. Over-delivery costs bandwidth; under-delivery is silent data loss.

That principle only holds because the receiver drops what it already applied, by sequence number,
before the WAL append. Two other mechanisms look like they would do it and do not: Last-Writer-Wins
refuses a record whose HLC is not newer, but its state is in memory and does not survive a restart;
and `ColumnarStore` refuses to merge a segment path already in the index, which hides a duplicate only
while the re-flushed segment happens to cover the same timestamp range. Measured without the sequence
check: four outage cycles storing 25 rows where 9 were written.

The vector travels in a `WALRecordV2` envelope with `record_type = WAL_RECORD_VERSION_VECTOR`, which is
also how it is written to the WAL so a restarted node knows what it holds. Reusing the record envelope
means a node on the older protocol skips it as an unknown type instead of disconnecting; it then sends
no vector of its own, and after a two-second grace window it is treated as holding nothing.

What this replaced: catch-up used to compare the peer's WAL byte offset with the local one and stream
from that offset in the *local* log. Every node writes its own records plus copies of foreign ones, so
the same data yields different offsets — the two numbers had no common scale. Measured: a node
reporting offset 846 against a local 870 was judged "behind by 24 bytes" and sent one empty checkpoint
record, while the rows it had missed sat earlier in the log (roadmap #61).

One limit worth knowing, and it is about size. The vector travels in a record header whose
`payload_len` is a `uint16_t`, so a vector above 65535 bytes — 1561 entries at 42 bytes each — cannot
be described by the header that carries it. It is not sent and not written down; the node falls back
to asking for everything, which costs bandwidth and never costs data.

A node that joined an origin's stream in the middle used to be the other limit: it saw sequence 5000
before it ever saw 1, so it could not claim "everything up to here" for that origin and kept
receiving redeliveries for ever. That is what snapshot bootstrap is for, and it now exists — see
below.

### Snapshot bootstrap: how a node that holds nothing gets a base

A frontier can only leave zero if the node followed an origin's stream from its first record. A node
added to a running cluster never did, so no amount of catch-up lets it state what it holds. A
snapshot solves this by carrying the sender's own frontiers alongside its files: the receiver's
contents *become* the sender's contents, so the sender's claims become claims the receiver is
entitled to make.

The transfer rides the multi-master frame envelope, tagged by `record_type` in the reserved wire-only
range from 200 up, the same way the version vector rides `record_type = 7`. A node that does not know
these types skips them and stays connected.

```
joiner                                            peer
  │  handshake + version vector  ───────────────►  │
  │  ◄──────────────  handshake + version vector   │
  │                                                │
  │  SNAPSHOT_REQUEST  ──────────────────────────► │   (only if the joiner holds nothing at all)
  │  ◄──────────  SNAPSHOT_BEGIN (lengths + CRC)   │   flush + checksum, vector captured with it
  │  ◄──────────  SNAPSHOT_CHUNK × n  (metadata)   │   manifest ++ version vector ++ held set
  │  ◄──────────  SNAPSHOT_CHUNK × n  (file data)  │   pushed as the socket drains
  │  ◄──────────  SNAPSHOT_END                     │
  │                                                │
  │  stage → verify → install → adopt frontiers    │
```

Four properties are worth stating, because each is a way to get this wrong:

**The vector is captured in the same critical section as the flush.** Exported a line later, it could
claim a number that landed after the flush and is therefore in no snapshot file — and the receiver
would declare a frontier over a hole. Exported before, it claims less than the files hold, and a
redelivery of the difference appends those rows a second time into append-only storage. The held set
closes what remains exactly: the numbers above the frontier that the sender does hold are listed, so
a redelivery of any of them is recognised.

**The metadata is streamed, not carried in one frame.** A manifest for a few thousand segments passes
65535 bytes on its own, and so does a version vector of 1561 entries. Metadata in a single frame
would have put a store-size limit on the one case bootstrap exists for.

**Chunks are pushed only while the peer's send buffer has room**, and resume from the `EPOLLOUT`
branch of the io loop as the socket drains. So live deltas enqueued between chunks go out promptly,
and the buffer never reaches the size at which a peer is dropped for not draining.

**Installation is all or nothing.** Files are staged to a scratch directory; every path in the
manifest is validated before a byte is written and one unsafe path refuses the whole snapshot; each
file's CRC32C is checked as it completes; and a pre-flight pass confirms every staged file exists at
its manifest size before the first rename. An abandoned transfer leaves the data directory exactly as
it was — and leaves the node **usable**, because refusing writes for ever is a worse answer than
admitting the bootstrap failed.

Two things the joiner must not do while a snapshot is in flight. It must not serve reads or accept
writes, including `FLUSH`, which would write segments into the directory an install is about to
rename files into. And it must not *record* the remote deltas that keep arriving: applying one is
harmless — the install discards it — but remembering its number leaves a frontier claiming a row that
no longer exists, which no later catch-up will fill because nobody knows it is missing. Such records
are dropped unmarked, so the next vector exchange brings them back.

The gate on asking is deliberately strict: a node requests a snapshot only when it holds **nothing at
all**, not merely when it is behind. Installing one discards local contents, and a node that wipes
its own rows because a peer looked further ahead is a worse failure than any amount of redundant
traffic. Repairing a node that does hold data is anti-entropy's job.

#### Who creates the snapshot, and on which thread

Creating one is a flush plus a CRC32C pass over every columnar file, so the work grows with the
store: 2.37 MB across 184 files measured 16–18 ms in a Release build on the development machine, and
a gigabyte would be several seconds. It used to run on whichever thread asked, and both askers are
io loops — `MultiMasterManager::io_loop()`, which also carries live deltas, catch-up and handshakes,
and `ReplicationManager::run_loop()`. A loop stopped for seconds answers nothing while it waits.

Since #79 the request only starts an `AsyncSnapshotBuilder`, which runs the creation on a short-lived
worker and hands the result back through a notification whose only job is to wake the loop. The loop
collects the result from its own thread, so every field it owns still has exactly one owner at a
time.

Three consequences are worth stating because each is a refusal:

- **One at a time, no queue.** A second request while one is being created is answered
  `SNAPSHOT_ABORT busy`. Two concurrent flush-and-checksum passes would double the cost the move
  exists to avoid.
- **A finished snapshot is discarded if its requester is gone.** The target is matched on a
  connection id, not on `node_id` or descriptor: the node that asked may have dropped and come back,
  and the new connection asked for nothing. Sending it a snapshot would hand it a wipe of its local
  contents that it never requested.
- **The work is not cancellable.** A disconnect marks the request dead and the result is thrown away
  when it lands; the flush is not abandoned half-way. The price is named: until that worker finishes,
  another peer's request is refused as busy. For an operation that happens once per node bootstrap,
  that beats both alternatives.

One side effect landed with it. `snapshot_manifest.json` is written by whoever creates a snapshot, and
was written straight onto its own path with `trunc` and no synchronisation — so two creators could
interleave their JSON, and a reader could catch the file empty. It now goes to a temporary file and is
renamed into place. The window predates #79, because the two managers have always had separate
threads; #79 only made it easier to reach.

### Anti-entropy: what reconciliation is for, and what it is not

A background pass every `--anti-entropy-interval-seconds` tells every connected peer what this node
holds. Receiving a vector already makes a node stream what the sender lacks, so reconciliation needs
no protocol of its own: sending the vector is the repair. The pass reports the difference in both
directions as `(symbol, origin, sequence range)`, and counts a repair only when a gap it was behind on
has disappeared by the following pass — a count of requests sent would measure diligence rather than
convergence.

It is worth being precise about what this adds, because most divergence in this architecture is
already handled elsewhere. A broken connection triggers reconnect, handshake, and catch-up. A healthy
connection delivers in order, and an `iptables DROP` does not reset it — TCP retransmits the backlog
once traffic is allowed again, so a partitioned node reconverges without reconciliation doing
anything. What is left for anti-entropy is divergence that outlives a healthy connection: a record the
receiver dropped rather than lost (above the held-set cap in `SequenceTracker`, or refused), a peer
whose vector was missing or stale when catch-up ran, and a backlog the sender discarded under
backpressure.

`ob_mm_anti_entropy_runs_total` and `ob_mm_reconcile_gaps_detected` are reported separately on
purpose: a zero in the second one means "checked, nothing to repair" only if the first one is moving.

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

### Who holds the role, and how a node stops holding it

The leader key in etcd is written under a lease, so losing the lease loses the role. Three things
make that more than a slogan, and each of them was a defect first.

**A lease that is alive is not proof that the role is yours.** The key can be gone while the lease
lives. So a primary re-reads the leader key every second and steps down if it does not name itself —
independently of whether its lease refresh succeeded. (Before that guard, a keepalive for a lease
etcd had forgotten answered HTTP 200 with the id echoed back and no `TTL` field, and the code tested
only that the response was non-empty, so the refresh could not fail at all.)

**A read has three possible answers, not two.** `Present`, `Absent`, `Unavailable`. The distinction
is load-bearing in both directions: a primary must step down on a key that is confirmed gone, and
must *not* step down because a read failed; a replica must campaign when there is confirmed no
leader, and must not campaign because it could not find out. Both used to see the same
`std::nullopt`.

**A holder that cannot confirm ownership for a whole lease TTL steps down anyway.** Whatever the
reason — unreachable coordinator, a stalled poll — the lease has had time to expire, so continuing
to answer `PRIMARY` is a claim without support. A healthy node confirms every second, ten times more
often than the threshold.

And on the other side of the handover, a candidate does **not** take a vacated key immediately:

```
leader key vanishes (lease expired or revoked)
  │
  ├─ holder notices within ~1 s and demotes: read-only, ROLE stops saying PRIMARY
  │
  └─ candidate waits --election-lease-wait-ms (default: the lease TTL) before its CAS
```

The wait exists because the two events above are not ordered by anything. A revoke deletes the key
at once while the holder learns on its next poll, so a candidate that claims immediately can coexist
with a node that still believes it is primary — and both accept writes while both believe it. The
wait costs failover latency, measured at 10.2 s → 20.1 s on the development machine, and that trade
was made deliberately: the alternative that costs no latency makes a primary read-only during a
brief etcd hiccup instead.

A cold start does not wait. No leader has existed to wait for, which the node reads from the epoch —
persisted, so a node that was ever part of this cluster comes back knowing one.

Demotion is unconditional. It used to depend on being able to read the *new* primary's address,
which after a revoke does not exist yet — so the Engine was never told, and a node kept accepting
writes while the failover component privately considered it a replica. Not knowing where to point the
replication client is a reason to start no client, not a reason to keep the role.

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
