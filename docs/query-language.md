# Query Language

The engine supports a SQL-like query language for scanning, aggregating, and subscribing to orderbook data.

## Grammar (EBNF)

```
query       = select_query | subscribe_query ;
select_query = "SELECT" select_list "FROM" symbol_ref
               [ "WHERE" where_clause ]
               [ "LIMIT" integer ] ;
subscribe_query = "SUBSCRIBE" select_list "FROM" symbol_ref
                  [ "WHERE" where_clause ] ;

select_list = "*" | column { "," column } ;
column      = identifier | agg_call ;
agg_call    = identifier "(" [ identifier ] ")" ;

symbol_ref  = "'" symbol_name "'" "." "'" exchange_name "'" ;

where_clause = condition { "AND" condition } ;
condition    = "timestamp" "BETWEEN" integer "AND" integer
             | "price" "BETWEEN" integer "AND" integer
             | "AT" integer ;
```

## SELECT Queries

### Scan all rows

```sql
SELECT * FROM 'BTC-USD'.'BINANCE'
  WHERE timestamp BETWEEN 0 AND 9999999999999999999
```

### Scan with time range

```sql
SELECT price, quantity FROM 'BTC-USD'.'BINANCE'
  WHERE timestamp BETWEEN 1700000000000000000 AND 1700000001000000000
```

### Scan with price filter

```sql
SELECT * FROM 'BTC-USD'.'BINANCE'
  WHERE timestamp BETWEEN 0 AND 9999999999999999999
  AND price BETWEEN 6490000 AND 6510000
```

### With LIMIT

```sql
SELECT * FROM 'BTC-USD'.'BINANCE'
  WHERE timestamp BETWEEN 0 AND 9999999999999999999
  LIMIT 100
```

## Aggregation Queries

Aggregation functions operate on the live SoA buffer — the current in-memory orderbook state, not
the stored segments. That has two consequences worth stating plainly, because both used to be
silently ignored:

- **A timestamp filter is refused**, not applied. There is nothing to filter: the aggregate reads
  the book as it is now. Aggregation over a time range is a separate feature (roadmap #43).
- **A price filter is refused.** Use `DEPTH_RANGE(lo, hi)`, which does what a price filter on an
  aggregate would be expected to do.

### Available functions

| Function | Description | Scale |
|----------|-------------|-------|
| `SUM(quantity)` | Sum of quantities over the first n levels | raw |
| `AVG(price)` | Average price | raw |
| `MIN(price)` | Minimum price | raw |
| `MAX(price)` | Maximum price | raw |
| `VWAP(price)` | Volume-weighted average price | × 10⁶ |
| `SPREAD(*)` | Best ask − best bid | raw |
| `MID_PRICE(*)` | (best ask + best bid) / 2 | × 10⁶ |
| `IMBALANCE(n)` | (bid_vol − ask_vol) / (bid_vol + ask_vol) over n levels | × 10⁹ |
| `DEPTH(price)` | Quantity at exactly that price | raw |
| `DEPTH_RANGE(lo, hi)` | Sum of quantities for levels priced in [lo, hi] | raw |
| `CUMULATIVE_VOLUME(n)` | Sum of quantities over the first n levels | raw |

Function names are case-insensitive. The scale column is not documentation you have to remember —
every response carries the scale with the value.

### Example

```sql
SELECT SPREAD(*), MID_PRICE(*), IMBALANCE(10) FROM 'BTC-USD'.'BINANCE'
```

### Response format

Aggregates use their own response shape: one row per requested expression, three columns.

```
OK
name	value	scale
SPREAD(*)	1000	1
MID_PRICE(*)	100500000000	1000000
IMBALANCE(10)	250000000	1000000000

```

Divide `value` by `scale` to get natural units — 100500000000 / 10⁶ = 100500. The values are integers
on the wire, so nothing is rounded on the way out.

`value` is `NULL` when there was nothing to aggregate: a spread on a book with only one side is
absent, and reporting it as `0` would read as a market with no spread at all. Clients expose this as
`None` (Python `AggValue.value`) or `AggEntry::empty` (C++).

The header is what distinguishes the two response shapes. A client that asks for aggregates through
the row API gets an error naming the right method rather than a misparsed row:

```python
aggs = engine.query_agg("BTC-USD", "BINANCE", "SPREAD(*)", "MID_PRICE(*)")
aggs["MID_PRICE(*)"].real     # 100500.0, already divided by the scale
aggs["SPREAD(*)"].is_empty    # False
```

### Refusals

| Error | Meaning |
|-------|---------|
| `AGG_WITH_COLUMNS` | Aggregates mixed with plain columns (`SELECT price, SPREAD(*)`). There is no `GROUP BY`, so the column would have to be dropped |
| `AGG_TIME_FILTER` | A timestamp predicate combined with an aggregate |
| `AGG_PRICE_FILTER` | A price predicate combined with an aggregate; use `DEPTH_RANGE(lo, hi)` |
| `OB_ERR_PARSE: undefined aggregation function` | Unknown function name |

## SNAPSHOT Queries

Reconstruct the orderbook state at a specific timestamp:

```sql
SELECT * FROM 'BTC-USD'.'BINANCE' WHERE AT 1700000000000000000
```

## SUBSCRIBE Queries

Register a streaming callback that fires on every matching delta update:

```sql
SUBSCRIBE price FROM 'BTC-USD'.'BINANCE'
  WHERE price BETWEEN 6490000 AND 6510000
```

Subscriptions are used programmatically via the C API (`ob_subscribe`) or Python bindings. The CLI does not support interactive subscriptions.

## Column Names

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` / `timestamp_ns` | uint64 | Nanosecond Unix timestamp |
| `price` | int64 | Price in smallest sub-unit |
| `quantity` | uint64 | Quantity |
| `order_count` | uint32 | Number of orders at this level |
| `side` | uint8 | 0 = bid, 1 = ask |
| `level` | uint16 | 0-based level index (0 = best) |
| `sequence_number` | uint64 | Per-origin sequence number of the update that produced the row; last column, and 0 when unknown |

## Error Handling

- Unknown symbol/exchange: returns `OB_ERR_NOT_FOUND` with a descriptive message
- Parse errors: returns `OB_ERR_PARSE` with line number, column, and description
- `LIMIT 0`: returns an empty result set (not an error)
