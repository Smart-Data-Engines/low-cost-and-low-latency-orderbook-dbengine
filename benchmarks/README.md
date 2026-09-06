# Benchmark Suite

This directory contains the Google Benchmark suite for `orderbook-dbengine` and
configuration stubs for equivalent workloads on ClickHouse, TimescaleDB, and kdb+.

## Running the native benchmarks

```bash
cmake -S .. -B ../build -DCMAKE_BUILD_TYPE=Release
cmake --build ../build --target bench_engine
../build/benchmarks/bench_engine \
    --benchmark_format=json \
    --benchmark_out=results.json \
    --benchmark_report_aggregates_only=true
```

The JSON output includes the engine version, host hardware description, and
timestamp fields emitted by Google Benchmark automatically.

## Benchmark descriptions

| Benchmark              | Metric                          | Requirement |
|------------------------|---------------------------------|-------------|
| `BM_UpdateLatency`     | p50/p99/p99.9 per `apply_delta` | 12.1        |
| `BM_IngestionThroughput` | updates/second, single core   | 12.2        |
| `BM_VwapLatency`       | VWAP over 1000 levels           | 12.3        |
| `BM_TimeRangeQuery`    | query latency over N snapshots  | 12.4        |

---

## Comparing two builds instead of two timings

`scripts/mnemonic_diff.py` disassembles named functions in two Release builds and compares the
instruction sequences. It exists because the question "what does this change cost the path that
does not use it?" is not answerable with a stopwatch on this hardware: a control experiment on
i3-7100U produced −40.6% in 8 of 8 rounds for a function that had not changed, and the suite above
measures `apply_delta` and `WALWriter::append`, which a change to the network write path never
touches.

```bash
git worktree add /tmp/ob_base origin/master
cmake -S /tmp/ob_base -B /tmp/ob_base/build-release -DCMAKE_BUILD_TYPE=Release -DOB_BUILD_TESTS=OFF
cmake --build /tmp/ob_base/build-release -j$(nproc)
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DOB_BUILD_TESTS=OFF
cmake --build build-release -j$(nproc)

scripts/mnemonic_diff.py --base /tmp/ob_base/build-release --head build-release \
    --symbol 'ob::WALWriter::append(ob::DeltaUpdate const&, ob::Level const*)' \
    --symbol 'ob::ReplicationManager::drain_send_buffer(ob::ReplicaInfo&) => ob::ReplicationManager::drain_send_buffer_plain(ob::ReplicaInfo&)'
```

Symbols are matched on the **exact** demangled signature, `A => B` compares a function that was
renamed or split, alignment NOPs are dropped, and clones (`[clone .cold]`) are reported as rows of
their own. Every one of those is a lesson rather than a feature: matching by substring once summed
`flush_output` with `flush_output_tls` and reported 310 instructions for a function that has 148;
padding once made `broadcast` read as 173 against 170; and a change that adds only a cold path
leaves the hot count identical.

### What TLS on the node links cost the plaintext path (#30 part three, series D)

i3-7100U, GCC 13.3, Release, `be1bd1b` against the series D branch:

| Function | master | branch | reading |
|---|---|---|---|
| `WALWriter::append` | 88 | 88 | identical, instruction for instruction |
| `Engine::apply_delta` | 501 | 501 | same instructions; 27 operands differ, all of them member offsets |
| `ReplicationManager::enqueue_send` | 166 | 166 | same, 11 offsets moved |
| `MultiMasterManager::broadcast_local` | 92 | 92 | same, 5 offsets moved |
| `ReplicationManager::broadcast` | 166 | 163 | changed on purpose: the blocking send is gone |
| `drain_send_buffer` → `drain_send_buffer_plain` | 83 | 79 | the plaintext drain, four instructions shorter |
| `try_drain_send_buf` → `try_drain_send_buf_plain` | 140 | 144 | the plaintext drain, four instructions longer |

The offsets moved because `ReplicaInfo` and `PeerConnection` gained a channel pointer and an
identity string (+48 bytes) and `Engine` grew with them: the same instructions in the same order,
reading fields that sit further along. `Engine::apply_delta_mm` went 602 → 597 for the same reason
and is **not** reported as a speedup — `src/engine.cpp` is byte-identical between the two commits,
so five fewer instructions is the optimiser choosing `lea` over `add` for an offset that moved.

What the plaintext path actually pays is the dispatcher, and it is worth quoting in full:

```
endbr64
cmpq   $0x0,0x68(%rsi)      # replica.tls == nullptr?
je     <drain_send_buffer_plain>
jmp    <drain_send_buffer_tls>
```

One compare against null and a tail jump, on a function that then does 79 instructions of work.

## Equivalent workload: ClickHouse

**Version tested**: 23.x (Community Edition)

### Schema

```sql
CREATE TABLE orderbook (
    timestamp_ns  UInt64,
    sequence_number UInt64,
    symbol        LowCardinality(String),
    exchange      LowCardinality(String),
    side          UInt8,
    level_index   UInt16,
    price         Int64,
    quantity      UInt64,
    order_count   UInt32
) ENGINE = MergeTree()
ORDER BY (symbol, exchange, timestamp_ns)
PARTITION BY toYYYYMMDD(fromUnixTimestamp64Nano(timestamp_ns));
```

### Ingestion workload

```bash
# Generate 1M rows and insert via clickhouse-client
clickhouse-client --query="INSERT INTO orderbook FORMAT RowBinary" < data.bin
```

### Query workload (equivalent to BM_TimeRangeQuery)

```sql
SELECT timestamp_ns, price, quantity
FROM orderbook
WHERE symbol = 'BTC-USD'
  AND exchange = 'BENCH'
  AND timestamp_ns BETWEEN 1700000000000000000 AND 1700000001000000000;
```

### VWAP workload (equivalent to BM_VwapLatency)

```sql
SELECT sumIf(price * quantity, level_index < 1000) /
       sumIf(quantity,         level_index < 1000) AS vwap
FROM orderbook
WHERE symbol = 'BTC-USD' AND exchange = 'BENCH'
  AND timestamp_ns = (SELECT max(timestamp_ns) FROM orderbook
                      WHERE symbol = 'BTC-USD' AND exchange = 'BENCH');
```

---

## Equivalent workload: TimescaleDB

**Version tested**: 2.x on PostgreSQL 15

### Schema

```sql
CREATE TABLE orderbook (
    timestamp_ns  BIGINT        NOT NULL,
    sequence_number BIGINT      NOT NULL,
    symbol        TEXT          NOT NULL,
    exchange      TEXT          NOT NULL,
    side          SMALLINT      NOT NULL,
    level_index   SMALLINT      NOT NULL,
    price         BIGINT        NOT NULL,
    quantity      BIGINT        NOT NULL,
    order_count   INTEGER       NOT NULL
);

SELECT create_hypertable('orderbook', 'timestamp_ns',
    chunk_time_interval => 3600000000000);  -- 1-hour chunks in nanoseconds

CREATE INDEX ON orderbook (symbol, exchange, timestamp_ns DESC);
```

### Ingestion workload

```bash
# Use COPY for bulk ingestion
psql -c "\COPY orderbook FROM 'data.csv' CSV HEADER"
```

### Query workload (equivalent to BM_TimeRangeQuery)

```sql
SELECT timestamp_ns, price, quantity
FROM orderbook
WHERE symbol = 'BTC-USD'
  AND exchange = 'BENCH'
  AND timestamp_ns BETWEEN 1700000000000000000 AND 1700000001000000000;
```

### VWAP workload (equivalent to BM_VwapLatency)

```sql
SELECT SUM(price::NUMERIC * quantity) / NULLIF(SUM(quantity), 0) AS vwap
FROM orderbook
WHERE symbol = 'BTC-USD'
  AND exchange = 'BENCH'
  AND timestamp_ns = (SELECT MAX(timestamp_ns) FROM orderbook
                      WHERE symbol = 'BTC-USD' AND exchange = 'BENCH')
  AND level_index < 1000;
```

---

## Equivalent workload: kdb+/q

**Version tested**: kdb+ 4.0 (64-bit on-demand)

### Schema and ingestion

```q
// Define the orderbook table (splayed on disk for large datasets)
orderbook:([]
    timestamp_ns:`long$();
    sequence_number:`long$();
    symbol:`symbol$();
    exchange:`symbol$();
    side:`short$();
    level_index:`short$();
    price:`long$();
    quantity:`long$();
    order_count:`int$()
)

// Ingest 1M rows
`orderbook insert (1000000#1700000000000000000j;
                   til 1000000;
                   1000000#`$"BTC-USD";
                   1000000#`$"BENCH";
                   1000000#0h;
                   1000000#0h;
                   1000000#5000000j;
                   1000000#1000j;
                   1000000#1i)
```

### Query workload (equivalent to BM_TimeRangeQuery)

```q
// Time-range scan
select timestamp_ns, price, quantity
from orderbook
where symbol=`$"BTC-USD",
      exchange=`$"BENCH",
      timestamp_ns within (1700000000000000000j; 1700000001000000000j)
```

### VWAP workload (equivalent to BM_VwapLatency)

```q
// VWAP over top 1000 levels at latest snapshot
t: select from orderbook
   where symbol=`$"BTC-USD",
         exchange=`$"BENCH",
         timestamp_ns=max timestamp_ns,
         level_index<1000;

// Compute VWAP
(sum t[`price] * t[`quantity]) % sum t[`quantity]
```

---

## Interpreting results

Compare the `items_per_second` (throughput) and `real_time` (latency) fields in
`results.json` against the equivalent queries on each external system.  All
measurements should be taken on the same hardware with the OS page cache warm.
