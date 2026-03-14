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

### ob_engine_destroy

```c
void ob_engine_destroy(ob_engine_t* engine);
```

Flush all pending data, close the engine, and free memory. Safe to call with `NULL`.

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
1. Written to the WAL (fsync'd)
2. Applied to the in-memory SoA buffer
3. Enqueued for background columnar flush

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

    // Flush (destroy + recreate)
    ob_engine_destroy(engine);
    engine = ob_engine_create("/tmp/ob_c_example");

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
