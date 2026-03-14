# Storage Format

## Overview

The engine uses three storage layers:

1. **WAL (Write-Ahead Log)** — append-only journal for crash recovery
2. **SoA Buffer** — in-memory orderbook state (not persisted directly)
3. **Columnar Store** — time-partitioned segments on disk via MMAP

## WAL Format

WAL files are stored in the data directory as `wal_NNNN.wal`.

Each record:

```
┌──────────────────────────────────────────────────┐
│ sequence_number  : uint64  (8 bytes)             │
│ timestamp_ns     : uint64  (8 bytes)             │
│ checksum         : uint32  (CRC32C of payload)   │
│ payload_len      : uint32  (4 bytes)             │
│ record_type      : uint8   (1=DELTA, 2=SNAPSHOT, │
│                              3=GAP, 4=ROTATE)    │
│ padding          : 3 bytes                       │
│ payload          : [payload_len bytes]            │
└──────────────────────────────────────────────────┘
```

- Records are written sequentially, never modified
- CRC32C checksum covers the payload bytes
- Replay stops at the first checksum mismatch (torn write detection)
- Rotation: when file size exceeds 512 MB, a new WAL file is opened

## Columnar Store Layout

```
<data_dir>/
  <symbol>/
    <exchange>/
      <start_ts>_<end_ts>/
        price.col          Delta+zigzag encoded int64 prices
        qty.col            Simple8b packed uint64 quantities
        ts.col             Raw uint64 timestamps
        cnt.col            Raw uint32 order counts
        meta.json          Segment metadata
```

### meta.json

```json
{
  "symbol": "BTC-USD",
  "exchange": "BINANCE",
  "start_ts_ns": 1700000000000000000,
  "end_ts_ns":   1700000001000000000,
  "row_count": 5000,
  "min_price": 6490000,
  "max_price": 6510000
}
```

### Segment Rollover

A new segment is created when the timestamp of an incoming row exceeds
`active_segment_start + segment_duration_ns` (default: 1 hour).

## Compression

### Price Compression (Delta + Zigzag)

Orderbook prices are highly correlated between consecutive levels. The codec:

1. **Delta encoding** — stores the difference between consecutive prices. The first value is stored absolute.
   ```
   Input:  [65000, 64990, 64980, 64970]
   Deltas: [65000, -10, -10, -10]
   ```

2. **Zigzag encoding** — maps signed integers to unsigned (so small negatives become small positives).
   ```
   zigzag(0) = 0, zigzag(-1) = 1, zigzag(1) = 2, zigzag(-2) = 3, ...
   ```

### Volume Compression (Simple8b)

Quantities are packed using Simple8b bit-packing:

- Each 64-bit word has a 4-bit selector (16 modes) indicating how many values are packed and their bit width
- Up to 240 values per word for small integers
- Values exceeding 2⁶⁰−1 fall back to raw uint64 storage

## MMAP Store

Column files are memory-mapped using `mmap(MAP_SHARED)`:

- Initial size is configurable; files are extended with `ftruncate` + `mremap` when capacity is exceeded
- `msync(MS_SYNC)` is called on flush for durability
- Supports segment files up to 16 GB on 64-bit systems

## SoA Buffer (In-Memory)

The live orderbook state per symbol/exchange:

```
SoASide (alignas(64)):
  prices[1000]       : int64_t
  quantities[1000]   : uint64_t
  order_counts[1000] : uint32_t
  depth              : uint32_t
  version            : atomic<uint64_t>  (seqlock counter)

SoABuffer:
  bid    : SoASide
  ask    : SoASide
  sequence_number : atomic<uint64_t>
  last_timestamp_ns : uint64_t
  symbol[32]   : char
  exchange[32] : char
```

- Bids are sorted descending by price (best bid first)
- Asks are sorted ascending by price (best ask first)
- Maximum 1000 levels per side
- Seqlock protocol: version odd = write in progress, even = stable
