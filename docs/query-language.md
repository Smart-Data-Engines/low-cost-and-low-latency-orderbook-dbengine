# Query Language

The engine supports a SQL-like query language for scanning, aggregating, and subscribing to orderbook data.

## Grammar (EBNF)

```
query       = select_query | subscribe_query ;
select_query = "SELECT" select_list "FROM" symbol_ref
               [ "WHERE" where_clause ]
               [ "GROUP" "BY" bucket ]
               [ "LIMIT" integer ] ;
bucket      = "TIME_BUCKET" "(" integer unit ")" ;
unit        = "ns" | "us" | "ms" | "s" | "m" | "h" | "d" ;
subscribe_query = "SUBSCRIBE" select_list "FROM" symbol_ref
                  [ "WHERE" where_clause ] ;

select_list = "*" | column { "," column } ;
column      = identifier | agg_call ;
agg_call    = identifier "(" [ agg_args ] ")" ;
agg_args    = side | "*" | identifier | [ side "," ] integer [ "," integer ] ;
side        = "bid" | "ask" ;

symbol_ref  = "'" symbol_name "'" "." "'" exchange_name "'" ;

where_clause = condition { "AND" condition } ;
condition    = "timestamp" range
             | "price" range
             | "side" range
             | "level" range
             | "AT" integer ;
range        = "BETWEEN" integer "AND" integer
             | comparison integer ;
comparison   = "=" | "<" | "<=" | ">" | ">=" ;
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

### Conditions

`BETWEEN` includes both of its ends, and the comparisons mean what they mean in SQL: `price = 200`
is one price, and `timestamp > t` leaves `t` out - which is how to ask for what came after the last
row read. Conditions joined by `AND` narrow each other, on one column as on two: `price >= 100 AND
price <= 200` is `price BETWEEN 100 AND 200`. A range nothing satisfies - `price BETWEEN 300 AND 100`,
`price = 1 AND price = 2`, `timestamp > 18446744073709551615` - answers no rows.

```sql
SELECT * FROM 'BTC-USD'.'BINANCE'
  WHERE timestamp > 1700000000000000000
  AND price >= 6490000 AND price < 6510000
```

`side` (0 for the bids, 1 for the asks) and `level` (0 the best) are conditions as the time and
the price are, with the same rules (#200): `side = 0 AND level = 0` is the top of the bids, and a
value past the column's type - `side = 256`, `level = 65536` - does not parse rather than wrap.

Before #199 in the roadmap every comparison set one end of the range and nothing else: `=` meant
`>=`, `>` and `<` kept the value they exclude, and a second condition on a column replaced the
first instead of narrowing it - all of it answered `OK`. A subscription's conditions are read the
same way, so the same was true of what it pushed.

### With LIMIT

```sql
SELECT * FROM 'BTC-USD'.'BINANCE'
  WHERE timestamp BETWEEN 0 AND 9999999999999999999
  LIMIT 100
```

### Which columns come back

The ones named, in the order named. `SELECT quantity, price` answers quantity first; a column named
twice is answered twice; `SELECT *` is the seven below, in the order of the table. The response
header names them, so a client can tell what it was handed — which is the only reason narrowing a
`SELECT` is safe and narrowing a `PUSH` is not.

Two consequences worth knowing before writing a query. The server reads only the column files it
needs, so a narrower question is a cheaper one — including for a column a predicate uses but the
answer does not carry: `SELECT quantity ... WHERE price BETWEEN ...` reads `price` and does not
answer it. And the bundled Python and C++ clients read a row **by position**, so they accept the
seven and refuse anything narrower by name rather than misreading it; a narrowed query goes over
the raw protocol until they read columns by name.

## Aggregation Queries

Aggregation functions operate on the live SoA buffer — the current in-memory orderbook state, not
the stored segments. That has two consequences worth stating plainly, because both used to be
silently ignored:

- **A timestamp filter is refused**, not applied. There is nothing to filter: the aggregate reads
  the book as it is now. Aggregation over a time range is a separate feature (roadmap #44).
- **A price filter is refused.** Use `DEPTH_RANGE(lo, hi)`, which does what a price filter on an
  aggregate would be expected to do.
- **So are side and level conditions** (#200): a function names the side it reads in its argument,
  and how deep in its own argument where it takes one.

**A function of one side of the book names the side** - `bid` or `ask`, in any case, the expression
in the answer in lower case - so one query can answer both sides and the spread:

```sql
SELECT VWAP(bid), VWAP(ask), SPREAD(*), DEPTH_RANGE(ask, 6490000, 6510000) FROM 'BTC-USD'.'BINANCE'
```

Until #200 every one of them read the bids whatever the query said, and nothing could ask for the
asks: on a book of bids 100×5, 99×6 and asks 101×7, 102×8, `MAX(price)` answered 100, `DEPTH(101)`
answered 0 and `DEPTH_RANGE(99, 102)` 11 of the 26 in the range. The spellings that meant the bids -
`SUM(quantity)`, `SUM(*)`, `AVG(price)`, `VWAP(*)`, `CUMULATIVE_VOLUME(n)` and the rest - are refused
with `AGG_NEEDS_SIDE`, which names the spelling to use, rather than answered with a number whose
meaning changed.

### Available functions

| Function | Reads | Description | Scale |
|----------|-------|-------------|-------|
| `SUM(bid)`, `SUM(ask)` | the side named | Sum of the side's quantities | raw |
| `AVG(bid)`, `AVG(ask)` | the side named | Average of the side's prices, truncated | raw |
| `MIN(bid)`, `MIN(ask)` | the side named | Lowest price of the side | raw |
| `MAX(bid)`, `MAX(ask)` | the side named | Highest price of the side | raw |
| `VWAP(bid)`, `VWAP(ask)` | the side named | The side's prices weighted by their quantities | × 10⁶ |
| `CUMULATIVE_VOLUME(bid, n)`, `(ask, n)` | the side named | Sum of the side's quantities over its first n levels | raw |
| `SPREAD(*)` | both | Best ask − best bid | raw |
| `MID_PRICE(*)` | both | (best ask + best bid) / 2 | × 10⁶ |
| `IMBALANCE(n)` | both | (bid_vol − ask_vol) / (bid_vol + ask_vol) over n levels | × 10⁹ |
| `DEPTH(price)`, `DEPTH(bid, price)`, `DEPTH(ask, price)` | both, or the side named | Quantity at exactly that price | raw |
| `DEPTH_RANGE(lo, hi)`, `DEPTH_RANGE(bid, lo, hi)`, `DEPTH_RANGE(ask, lo, hi)` | both, or the side named | Sum of quantities for levels priced in [lo, hi] | raw |

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
| `AGG_WITH_COLUMNS` | Aggregates mixed with plain columns (`SELECT price, SPREAD(*)`): the column would have to be dropped. For aggregates of rows over time, see [Time buckets](#time-buckets) |
| `AGG_TIME_FILTER` | A timestamp predicate, or `AT`, combined with an aggregate |
| `AGG_PRICE_FILTER` | A price predicate combined with an aggregate; use `DEPTH_RANGE(lo, hi)` |
| `AGG_SIDE_FILTER`, `AGG_LEVEL_FILTER` | A side or level condition combined with an aggregate: name the side in the function, `VWAP(bid)` |
| `AGG_NEEDS_SIDE` | A function of one side that names none - `SUM(quantity)`, `VWAP(*)`, `CUMULATIVE_VOLUME(5)`: write `SUM(bid)`, `VWAP(ask)`, `CUMULATIVE_VOLUME(bid, 5)` |
| `AGG_NEEDS_BUCKET` | `COUNT`, `FIRST` or `LAST` without `GROUP BY`: they aggregate a time bucket's rows, and the live book has none |
| `OB_ERR_PARSE: undefined aggregation function` | Unknown function name |

## Time buckets

Aggregates over the **stored rows** of each interval, rather than over the live book (#44):

```sql
-- One-minute bars of the best bid
SELECT FIRST(price), MAX(price), MIN(price), LAST(price), COUNT(*)
FROM 'BTC-USD'.'BINANCE'
WHERE side = 0 AND level = 0
GROUP BY TIME_BUCKET(1m)

-- Updates a second, and the volume-weighted price of each, over an hour
SELECT COUNT(*), VWAP(price) FROM 'BTC-USD'.'BINANCE'
WHERE timestamp BETWEEN 1700000000000000000 AND 1700003600000000000
GROUP BY TIME_BUCKET(1s)
```

- **The interval** is a positive integer and a unit, `ns`, `us`, `ms`, `s`, `m`, `h` or `d` (a day is
  86 400 s), at most 366 d. `1m` and `1 m` read the same; units are lower case.
- **A bucket** is `t - (t mod interval)` of each row's event time: on the Unix epoch, in UTC, the
  same for every query of that interval. Only buckets holding a row that meets the conditions are
  answered, in time order; `LIMIT n` answers the first `n`.
- **The conditions narrow the rows** - time, price, side, level - instead of being refused as they
  are beside a function of the live book. `AT` and `GROUP BY` are not one query.
- Rows are what a `SELECT` reads, so a row is in a bucket once it has been flushed, as it is in a
  `SELECT`.

| Function | Of | Value |
|---|---|---|
| `COUNT(*)` | - | the rows in the bucket |
| `FIRST(c)`, `LAST(c)` | `price`, `quantity` | of the row with the earliest, the latest event time; a tie goes to the row stored first for `FIRST` and last for `LAST`, as in a snapshot |
| `MIN(c)`, `MAX(c)` | `price`, `quantity` | |
| `SUM(quantity)` | `quantity` | |
| `AVG(c)` | `price`, `quantity` | scaled by 10^6 |
| `VWAP(price)` | `price`, weighted by `quantity` | Σ(price × quantity) / Σ quantity, scaled by 10^6; `NULL` when every row of the bucket has quantity 0 |

The same function names mean something else without `GROUP BY` - `SUM(bid)` is the live book's -
and a side is chosen with `WHERE side = 0`, not in the argument. Sums are kept in 128 bits; a value
that does not fit a 64-bit integer once scaled is refused, not wrapped.

### Response format

```
OK
bucket_ns	COUNT(*)/1	FIRST(price)/1	VWAP(price)/1000000
1700000040000000000	42	100	100454545
1700000100000000000	17	101	NULL

```

The first column is each bucket's start. Every other column names its aggregate as the query wrote
it and, after the last `/`, its scale: divide by it for the natural value. The scale depends only on
the function, so it is in the header, written before the first row; a query with no bucket answers
the header alone. The Python client's `query_buckets(sql)` returns `Bucket(start_ns, values)` with
an `AggValue` per aggregate (`.real` divides by the scale), the C++ client's `query_buckets(sql)`
`BucketRow`s; `query()` refuses this shape by name. The local library (`ob_query`) refuses
`GROUP BY`: its rows are the seven columns.

### Refusals

| Error | Meaning |
|-------|---------|
| `Parse error ... interval` | An interval that is zero, negative, without a unit, in a unit not listed, or past 366 d - said where it stands |
| `Parse error ... is not one` | A column or `*` in the list of a `GROUP BY` query, which answers aggregates of each bucket |
| `Parse error ... aggregates the live book` | `SPREAD`, `DEPTH` and the other functions of the live book under `GROUP BY` |
| `Parse error ... takes` | A function given a column it does not aggregate - `COUNT(price)`, `SUM(price)`, `VWAP(quantity)` - or a side, `SUM(bid)`, which a bucket takes from `WHERE side = …` |
| `BUCKETS_TOO_MANY` | The answer would have more buckets than the server allows (`--max-query-buckets`, 100 000 by default): narrow the time range or widen the interval. Refused rather than cut short |
| `BUCKET_OVERFLOW` | An aggregate of one bucket that does not fit a 64-bit integer, named with the bucket |

## SNAPSHOT Queries

Reconstruct the orderbook state at a specific timestamp:

```sql
SELECT * FROM 'BTC-USD'.'BINANCE' WHERE AT 1700000000000000000
```

For each side and level, the answer is the row with the **latest event time at or before** the
one asked for; two rows of one level at one instant resolve to the one written later. Levels come
bids first, then asks, each by level — the order `BOOK` answers in — so `LIMIT n` keeps the first
`n` of them. It answers the columns it names, like any row query (`SELECT price, quantity … WHERE
AT …`), and reads the columnar store, so a row the flush tick has not yet written is not in it.

A price condition keeps the levels of that book priced within it - `WHERE AT t AND price BETWEEN lo
AND hi` is the book at `t` inside a band - and it is applied to the book, so a level whose latest
price is outside the band is left out even when an earlier row of it was inside. Side and level
conditions keep that side and those levels of it the same way (#200): `AT t AND level = 0` is the
best bid and the best ask at `t`. `LIMIT` counts the
levels answered. A timestamp condition beside `AT` is refused with `SNAPSHOT_TIME_FILTER`, since `AT`
names the moment, and a second `AT` does not parse. Before #199 the price and the timestamp
conditions were accepted and ignored, and a second `AT` replaced the first.

Two things this answered differently before, and both are fixed (#167, #168 in the roadmap): over
the wire, from #139 on, it answered `OK` with an empty header and empty rows; and it kept the last
row a scan *delivered* for each level rather than the latest, so a correction for an earlier
instant that arrived later — a client's own event time, a mesh peer's backlog — replaced the book it
came after. Its order was a hash map's.

## SUBSCRIBE Queries

Register a streaming callback that fires on every matching delta update:

```sql
SUBSCRIBE price FROM 'BTC-USD'.'BINANCE'
  WHERE price BETWEEN 6490000 AND 6510000
```

A subscription takes timestamp, price, side and level conditions as a `SELECT` does (see
Conditions above).
`AT` is refused: it names a moment of the stored book, and a subscription is what is written from
now on. It used to be accepted and dropped, which subscribed to every row.

### Over the wire

Send the same statement as a command and the server pushes matching rows to that connection until
the subscription is cancelled or the connection closes:

```
C: SUBSCRIBE 'BTC-USD'.'BINANCE'
S: OK SUB 1

S: PUSH 1	1756640400000000000	7845812	1500	3	0	0	91823
S: PUSH 1	1756640400000000123	7845813	900	1	0	0	91824

C: UNSUBSCRIBE 1
S: OK 1
```

`PUSH <id>` is followed by **all seven columns, in the order of the table below**, tab separated —
the same shape as `SELECT *`, and the same shape whatever select list the `SUBSCRIBE` named. A
`PUSH` line carries no header, so a narrowed push would change what field 2 means with no signal at
all; announcing the columns in `OK SUB <id>` is the fix and it is a protocol change. The server logs
once per subscription when a list was named and ignored. A client that already parses `SELECT *`
output adds one branch on the prefix rather than a second format.

Four things worth knowing before writing a client:

- **A push can arrive between your command and its reply.** The protocol allows it, so a reader that
  matches on the front of its buffer has to take complete `PUSH` lines off first. The bundled Python
  client does this; a client that does not will block waiting for a reply that is already in its
  buffer.
- **A cancelled subscription may still deliver one more row**, if a notification was already running
  when the cancellation landed. Waiting for notifications to quiesce inside `UNSUBSCRIBE` would
  block one server thread on another, so the cost is pushed here instead.
- **A slow consumer is disconnected, not throttled.** Each subscription has a queue ceiling
  (`--max-subscriber-queue-bytes`, 8 MB by default — roughly 140 000 rows). Past it the session is
  closed and `ob_subscription_overflow_disconnects_total` is incremented. The queue is not discarded
  first: taking back bytes the client has partly read would truncate its input instead of
  disconnecting it.
- **`UNSUBSCRIBE` with no id cancels every subscription of that connection** and answers with the
  count. A malformed id is rejected as an unknown command rather than widened into "all of them".

There is a limit per session (`--max-subscriptions-per-session`, 16 by default), because without one
a single connection can order an unbounded amount of work onto every other client's write path.

### Embedded

The same statement through the C API (`ob_subscribe`) or `Engine::subscribe()` delivers rows to a
callback in-process. The callback runs on the thread that performed the write — which for a
multi-master node may be the replication io thread — so it must not block. It may cancel its own
subscription: cancellation marks the entry and the removal happens later, so no lock is held across
the callback.

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
