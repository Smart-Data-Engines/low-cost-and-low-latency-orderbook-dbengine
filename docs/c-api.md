# C API Reference

The C API (`include/orderbook/c_api.h`) provides an `extern "C"` interface for FFI integration. No C++ types or exceptions cross the boundary.

## Header

```c
#include "orderbook/c_api.h"
```

## Status Codes

| Code | Value | Meaning |
|------|-------|---------|
| `OB_C_OK` | 0 | Success |
| `OB_C_ERR_INVALID_ARG` | -1 | Invalid argument (null pointer, bad side, etc.) |
| `OB_C_ERR_NOT_FOUND` | -2 | Symbol/exchange not found |
| `OB_C_ERR_PARSE` | -3 | SQL parse error |
| `OB_C_ERR_IO` | -4 | I/O error |
| `OB_C_ERR_MMAP_FAILED` | -5 | Memory-mapped file operation failed |
| `OB_C_ERR_INTERNAL` | -99 | Internal error (caught C++ exception) |

## Engine Lifecycle

### ob_engine_create

```c
ob_engine_t* ob_engine_create(const char* base_dir);
```

Create and open an engine instance. The directory is created if needed. Returns `NULL` on failure.

Opening a directory that holds data composes each symbol's live book from its history by price, as
the server does at a start (#235), so that a side replacement removes what the side held. The
embedded engine writes no book summaries, so that reads each symbol's rows from the first: 112-290 ms
at a million rows of one symbol on an i3-7100U, Release. Open an engine once and flush it with
`ob_flush()`, rather than destroying and creating it again to make writes readable.

An open also replays the WAL after its last checkpoint (#245). The writes of a process that ended
without `ob_flush()` or `ob_engine_destroy()` are applied again, into the live book and into the
symbol's segment, and flushed, so the next open replays none of them.
- A write a segment already holds is skipped, by the WAL position the segment records.
- A segment of a build before #245 records none, and is judged by the time of its last row. That
  is exact only while a symbol's times rise.

Measured on an i3-7100U, Release, ext4 over LUKS on NVMe, one symbol of 100-level updates, the
process ended before a flush (`evidence/2026-10-23-embedded-wal-replay/` in the workspace):

| rows written | the open before #245 | the open now |
|---|---|---|
| 10 000 | 2.4-3.5 ms, and no update found | 21.7-35.4 ms, and every update found |
| 100 000 | 2.6-2.8 ms, and no update found | 54.5-60.2 ms, and every update found |
| 1 000 000 | 2.4-3.8 ms, and no update found | 662-708 ms, and every update found |

A directory a build before #245 wrote holds no checkpoint, so the first open reads its whole WAL,
once: the first flush or close after it appends the first checkpoint. At a million rows written and
closed by such a build, the first open took 177-221 ms and the next 165-172 ms, and each found every
update once.

### ob_engine_destroy

```c
void ob_engine_destroy(ob_engine_t* engine);
```

Flush as `ob_flush()` does, close the engine, and free memory. Its checkpoint ends the WAL, so the
next open replays nothing. Safe to call with `NULL`.

### ob_flush

```c
ob_status_t ob_flush(ob_engine_t* engine);
```

Make every write so far readable by `ob_query()`, keeping the engine open (#244): each symbol's
open segment is written and indexed, with any segment a write rolled over into, as
`ob_engine_destroy()` writes them. The live books stay in memory. `OB_C_ERR_INVALID_ARG` for `NULL`,
`OB_C_ERR_INTERNAL` when a segment could not be written.

When anything was written since the last checkpoint, a flush then does three things (#245):
1. It takes the segments and the WAL to the device with one `syncfs()`.
2. It appends a checkpoint, where the next open starts replaying.
3. It removes the WAL files wholly before the checkpoint.

The sync comes before the checkpoint, as in the server's flush: a checkpoint claims only rows that
are on the device (#160). A flush with nothing new waits for no device.

`OB_C_ERR_IO` when the sync failed. The segments are written and readable, and no checkpoint claims
them. Linux reports a failed sync once, and may have dropped what it could not write. From then on,
until the engine is created again, no checkpoint is appended and no WAL file removed. The next open
replays from the last checkpoint before the failure.

What the sync costs, measured as above: a flush after one update of 100 levels took 2.79-3.03 ms
(median of 300, three runs) and 4.99-6.16 ms at the 90th percentile, where it took 0.11-0.19 ms
and 0.15-0.26 ms before #245. The SDK's adapter flushes before a read that follows a write, so a
read after a write pays it in local mode, as a `FLUSH` pays the server's sync over TCP.

Before it a caller flushed by destroying the engine and creating it again, as the Python client did
in local mode. That costs whatever an open costs, and since #235 an open composes every book from
history: a flush with nothing new took 94-146 ms at a million rows of one symbol, against 0.02 ms
or less with `ob_flush()` (i3-7100U, Release; `evidence/2026-10-18-incremental-books/` in the
workspace).

## Data Ingestion

### ob_apply_delta

```c
ob_status_t ob_apply_delta(
    ob_engine_t*    engine,
    const char*     symbol,      // e.g. "BTC-USD"
    const char*     exchange,    // e.g. "BINANCE"
    uint64_t        seq,         // sequence number
    uint64_t        ts_ns,       // nanosecond timestamp
    const int64_t*  prices,      // array of prices
    const uint64_t* qtys,        // array of quantities
    const uint32_t* cnts,        // array of order counts
    uint32_t        n_levels,    // array length
    int             side         // 0=bid, 1=ask
);
```

Insert one or more price levels. The update is:
1. Written to the WAL, at once and without a sync: `ob_flush()` and `ob_engine_destroy()` sync it
2. Applied to the in-memory SoA buffer
3. Appended to its symbol's open segment, which `ob_flush()` or `ob_engine_destroy()` writes. There
   is no background flush.

A process that ends without either loses none of these writes: the next `ob_engine_create()` replays
them from the WAL (#245). **A power cut or a kernel crash can lose them**, since the last flush or
close. There is no setting that syncs each write, as the server's `--fsync-policy every` does.

### ob_replace_side

```c
ob_status_t ob_replace_side(/* the arguments of ob_apply_delta */);
```

The same write for an update that is the whole side (`MREPLACE`, #233): every price the side holds and
the update does not name is removed, by a level of quantity 0 put in front of the update, before it is
logged - so the WAL holds an ordinary update. `n_levels` may be 0, which empties the side. Above
`MAX_LEVELS` (1000) it is refused with `OB_C_ERR_INVALID_ARG`, where `ob_apply_delta()` cuts a write to
the ceiling: a replacement cut short would remove prices it named.

An embedded engine writes its segments in segment format 3 with a seal's encodings - LZ4 or nothing
(`docs/operations.md`, "Segment format 3"). Nothing merges an embedded store, so its segments keep
those encodings rather than a merge's ZSTD, and there is no setting for format 2 yet.

## Querying

### ob_query

```c
ob_result_t* ob_query(ob_engine_t* engine, const char* sql);
```

Execute a SQL query. Returns a result set iterator, or `NULL` on error.

### ob_result_next

```c
ob_status_t ob_result_next(
    ob_result_t* result,
    uint64_t*    out_timestamp_ns,  // may be NULL
    int64_t*     out_price,         // may be NULL
    uint64_t*    out_quantity,      // may be NULL
    uint32_t*    out_order_count,   // may be NULL
    uint8_t*     out_side,          // may be NULL
    uint16_t*    out_level          // may be NULL
);
```

Advance to the next row. Returns `OB_C_OK` while rows remain, `OB_C_ERR_NOT_FOUND` when exhausted. Any output pointer may be `NULL` to skip that field.

### ob_result_next_seq

```c
ob_status_t ob_result_next_seq(
    ob_result_t* result,
    uint64_t*    out_timestamp_ns,     // may be NULL
    int64_t*     out_price,            // may be NULL
    uint64_t*    out_quantity,         // may be NULL
    uint32_t*    out_order_count,      // may be NULL
    uint8_t*     out_side,             // may be NULL
    uint16_t*    out_level,            // may be NULL
    uint64_t*    out_sequence_number   // may be NULL
);
```

The same cursor with one more field: the per-origin sequence number of the update that produced the
row, or `0` when the row predates sequence numbering. Gaps in this sequence for one symbol are what
let a reader verify for itself that it received everything.

It is a separate function rather than an extra parameter on `ob_result_next()` because that one is a C
entry point callers may already have compiled against. Both advance the same cursor — call one or the
other, not both.

### ob_result_free

```c
void ob_result_free(ob_result_t* result);
```

Free the result set. Safe to call with `NULL`.

## Subscriptions

### ob_subscribe

```c
uint64_t ob_subscribe(
    ob_engine_t* engine,
    const char*  sql,
    void (*callback)(const char* json_row, void* userdata),
    void*        userdata
);
```

Register a streaming subscription. The callback is invoked synchronously within `ob_apply_delta` for each matching row. Returns a subscription ID (0 on failure).

The `json_row` parameter contains a JSON object with fields: `timestamp_ns`, `price`, `quantity`, `order_count`, `side`, `level`.

### ob_unsubscribe

```c
void ob_unsubscribe(ob_engine_t* engine, uint64_t sub_id);
```

Remove a subscription. Subsequent deltas will not trigger the callback.

## Example

```c
#include "orderbook/c_api.h"
#include <stdio.h>

int main() {
    ob_engine_t* engine = ob_engine_create("/tmp/ob_c_example");

    // Insert bid levels
    int64_t  prices[] = {6500000, 6499000};
    uint64_t qtys[]   = {150, 200};
    uint32_t cnts[]   = {3, 5};

    ob_apply_delta(engine, "BTC-USD", "BINANCE",
                   1, 1700000000000000000ULL,
                   prices, qtys, cnts, 2, 0);

    // Make the write readable, keeping the engine open
    ob_flush(engine);

    // Query
    ob_result_t* result = ob_query(engine,
        "SELECT * FROM 'BTC-USD'.'BINANCE' "
        "WHERE timestamp BETWEEN 0 AND 9999999999999999999");

    uint64_t ts; int64_t price; uint64_t qty;
    uint32_t cnt; uint8_t side; uint16_t level;

    while (ob_result_next(result, &ts, &price, &qty, &cnt, &side, &level) == OB_C_OK) {
        printf("price=%ld qty=%lu side=%s\n", price, qty, side == 0 ? "bid" : "ask");
    }

    ob_result_free(result);
    ob_engine_destroy(engine);
    return 0;
}
```
