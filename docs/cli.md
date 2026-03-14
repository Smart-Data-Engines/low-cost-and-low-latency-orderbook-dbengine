# CLI Reference

## Starting the CLI

```bash
./build/ob_cli [data_directory]
```

If no directory is given, defaults to `/tmp/ob_cli_data`. The directory is created if it doesn't exist. Data persists across sessions.

## Commands

### insert

Insert a single price level.

```
insert <symbol> <exchange> <bid|ask> <price> <qty> [count]
```

- `price` — integer in smallest sub-units (e.g. cents: 6500000 = $65,000.00)
- `qty` — quantity (unsigned integer)
- `count` — order count (optional, default: 1)

```
ob> insert BTC-USD BINANCE bid 6500000 150
OK  seq=1  bid BTC-USD@BINANCE  price=6500000 qty=150

ob> insert BTC-USD BINANCE ask 6510000 80 5
OK  seq=2  ask BTC-USD@BINANCE  price=6510000 qty=80
```

### bulk

Insert multiple levels at once. Prices step by 100 per level (descending for bids, ascending for asks).

```
bulk <symbol> <exchange> <bid|ask> <n_levels> <base_price> <base_qty>
```

```
ob> bulk BTC-USD BINANCE bid 10 6500000 100
OK  seq=3  10 bid levels for BTC-USD@BINANCE  base_price=6500000
```

### load

Import rows from a CSV file.

```
load <csv_file>
```

CSV format (header required):

```csv
symbol,exchange,side,price,qty,count,timestamp_ns
BTC-USD,BINANCE,bid,6500000,150,3,
BTC-USD,BINANCE,ask,6510000,80,2,
ETH-USD,COINBASE,bid,420000,500,,
```

- `count` and `timestamp_ns` are optional (default: 1 and current time)
- `side` is `bid` or `ask`

```
ob> load /tmp/orderbook_data.csv
  Loaded 3 rows from /tmp/orderbook_data.csv
  Run 'flush' to make them queryable.
```

### generate

Generate synthetic orderbook data for testing.

```
generate <symbol> <exchange> <n_rows>
```

```
ob> generate BTC-USD BINANCE 10000
  Generated 10000 rows for BTC-USD@BINANCE in 245.3 ms (40766 rows/sec)
  Run 'flush' to make them queryable.
```

### flush

Force-flush all pending data from the in-memory buffer to the columnar store. Required before data is visible to queries.

```
ob> flush
  Flushing...
  Done. Data is now queryable.
```

### query

Execute a SQL query against the columnar store.

```
query <SQL>
```

```
ob> query SELECT * FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999
  ts_ns               | side | level | price        | qty          | orders
  ────────────────────┼──────┼───────┼──────────────┼──────────────┼────────
  1773478813946338657  | bid  |     0 |      6500000 |          150 |      3
  1773478813948808615  | bid  |     0 |      6499000 |          200 |      5
  ── 2 row(s) in 0.11 ms
```

See [Query Language](query-language.md) for full SQL syntax.

### status

Show engine statistics.

```
ob> status
  sequence: 42  inserts: 42  queries: 5
```

### help

Show the help message.

### quit / exit

Shut down the engine (flushes all data) and exit.

## Typical Session

```
ob> bulk BTC-USD BINANCE bid 20 6500000 100
ob> bulk BTC-USD BINANCE ask 20 6510000 50
ob> flush
ob> query SELECT * FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999 LIMIT 5
ob> status
ob> quit
```
