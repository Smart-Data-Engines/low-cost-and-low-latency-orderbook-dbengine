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

Aggregation functions operate on the live SoA buffer (in-memory orderbook state).

### Available functions

| Function | Description | Scale |
|----------|-------------|-------|
| `sum(quantity)` | Sum of quantities | raw |
| `avg(price)` | Average price | raw |
| `min(price)` | Minimum price | raw |
| `max(price)` | Maximum price | raw |
| `vwap()` | Volume-weighted average price | × 10⁶ |
| `spread()` | Best ask − best bid | raw |
| `mid_price()` | (best ask + best bid) / 2 | × 10⁶ |
| `imbalance()` | (bid_vol − ask_vol) / (bid_vol + ask_vol) | × 10⁹ |

### Example

```sql
SELECT vwap(), spread() FROM 'BTC-USD'.'BINANCE'
  WHERE timestamp BETWEEN 0 AND 9999999999999999999
```

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

## Error Handling

- Unknown symbol/exchange: returns `OB_ERR_NOT_FOUND` with a descriptive message
- Parse errors: returns `OB_ERR_PARSE` with line number, column, and description
- `LIMIT 0`: returns an empty result set (not an error)
