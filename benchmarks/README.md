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
