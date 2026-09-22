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

The interactive CLI has no event-time argument: it embeds the engine rather than speaking the wire
protocol, and the embedded API has taken `timestamp_ns` since the first release (see
`docs/c-api.md`). The wire field documented below is what closed the gap between the two (#105).

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
  ts_ns               | side | level | price        | qty          | orders |      seq
  ────────────────────┼──────┼───────┼──────────────┼──────────────┼────────┼─────────
  1773478813946338657  | bid  |     0 |      6500000 |          150 |      3 |        1
  1773478813948808615  | bid  |     0 |      6499000 |          200 |      5 |        2
  ── 2 row(s) in 0.11 ms

A select list narrows the answer. `SELECT price, quantity FROM 'BTC-USD'.'BINANCE' WHERE ...`
returns two columns, in that order, and the server reads only the column files it needs to answer
it — `SELECT quantity, price` answers in the order asked, and a column named twice is answered
twice. Naming a column the engine does not have is refused, as it always was.

The column is spelt `timestamp` or `timestamp_ns` in a query; the response header always calls it
`timestamp_ns`. Both spellings parse, so a name copied out of a header works.

**`query` above renders the seven-column shape and refuses anything else**, and so does the Python
client: both read a row by position, so they name the columns they were handed rather than read the
wrong field. A narrowed query goes over the raw protocol until they read columns by name.

`SUBSCRIBE` takes a select list and **does not** narrow what it pushes — a `PUSH` line carries no
header, so a narrowed push would change what field 2 means with no way for a client to tell. The
server logs that once per subscription.

`seq` is the per-origin sequence number of the update that produced the row, and the wire protocol
carries it as the seventh column of a `SELECT *` response. Consecutive numbers for one symbol mean
nothing is missing between them; a hole means a row did not arrive. `0` means the number is unknown —
the row was stored before sequencing existed, or the server predates the column and sent six fields.
```

Aggregates render as their own table, with the value in natural units in the last column, so the
scale factor does not have to be applied by eye:

```
ob> query SELECT SPREAD(*), MID_PRICE(*), IMBALANCE(10) FROM 'BTC-USD'.'BINANCE'
  aggregate            | value                |      scale | in units
  ─────────────────────┼──────────────────────┼────────────┼──────────────
  SPREAD(*)            |                 1000 |          1 | 1000
  MID_PRICE(*)         |         100500000000 |    1000000 | 100500
  IMBALANCE(10)        |            250000000 | 1000000000 | 0.25
  ── 3 aggregate(s) in 0.12 ms
```

An empty aggregate prints `NULL`, not `0`. Aggregates read the live book, so they need no `WHERE`
clause and reject one.

See [Query Language](query-language.md) for full SQL syntax.

### book

    BOOK <symbol> <exchange> [depth]

The **live** book for one symbol: the current levels of both sides, bids first and then asks, each
side in its own order (bids descending, asks ascending). `depth` is per side and counts from the
best level; omit it for everything the side has. Rows carry the same seven columns as a `SELECT`
row, so a client that parses one parses the other.

This is a different question from `SELECT`, which reads **history** — the columnar store — and will
answer with every version of a level it has. `BOOK` reads the engine's own current state under a
seqlock and is one `memcpy` per side.

**Two of those seven columns come from the buffer rather than from a level, so every row of one
answer carries the same `timestamp_ns` and `sequence_number`.** That is the identity of the
snapshot — *as of which update* this book is — not the time a level changed. It is the number to
resume a `SUBSCRIBE` from: take the book, then subscribe, and discard anything at or below that
sequence number.

`BOOK` is not an atomic read of both sides. Each side is read under its own seqlock, one after the
other, so in principle the two halves are microseconds apart. Making it atomic means one seqlock
per buffer instead of one per side, which is a change on the write path; the alternative that was
rejected is worse — reading each side while emitting its rows puts the formatting of up to two
thousand levels between the two halves.

Refusals, all naming the token: a symbol with no live buffer is `OB_ERR_NOT_FOUND`, a non-numeric
depth, a depth of `0` (omit the argument to ask for everything — an empty answer on request is
indistinguishable from a book that is not there), and a depth above 1000, which is the most levels
per side this engine stores.

    ob> book BTC-USD BINANCE 2
    OK
    timestamp_ns	price	quantity	order_count	side	level	sequence_number
    1758614400000000000	6500000	100	1	bid	0	42
    1758614400000000000	6499000	250	3	bid	1	42
    1758614400000000000	6501000	80	2	ask	0	42
    1758614400000000000	6502000	140	1	ask	1	42

### status

Show engine statistics.

```
ob> status
  sequence: 42  inserts: 42  queries: 5
```

Over the wire, `STATUS` also reports which build is answering:

```
$ echo "STATUS" | nc localhost 9090 | grep '^version:'
version: 0.1.0
```

That question had no answer before roadmap #90 — not here, not in `--print-config`, not in
`/metrics`. It is a key/value line rather than a column in the tab-separated table above, so no
client parsing that table has to change. `/metrics` carries the same fact as
`ob_build_info{version="0.1.0",node_role="..."} 1`, which is what lets a monitoring system tell a
node running an old binary from one running the new one.

The number comes from `project(... VERSION)` in `CMakeLists.txt` through a compile definition, so
there is one copy in the C++ and a test holds `pyproject.toml` in step with it. It used to be a
literal in `tools/ob_tcp_server.cpp`, which was also the only place it appeared.

### help

Show the help message.

### quit / exit

Shut down the engine (flushes all data) and exit.

### AUTH

Only when the server runs with `--auth-secret-file`. An unauthenticated session may run `AUTH`,
`PING` and `QUIT`, and nothing else — every other command answers `ERR unauthenticated`.

```
C: AUTH
S: OK CHALLENGE 3f1c...9ab2          (64 hex characters)
C: AUTH grafana 7d40...c18e          (HMAC-SHA256, 64 hex characters)
S: OK AUTH grafana
```

The response is

```
HMAC-SHA256(secret, "ob-auth-v1\0client\0initiator\0<identity>\0<challenge>")
```

hex-encoded lower case, with **NUL separators** between the five fields. Challenge-response rather
than sending the secret, because there is no TLS yet: a secret in the first packet is replayable by
anyone who saw it, and a response to a fresh nonce is not.

`client` is the surface — the cluster links use `replication` and `mm`, so a response captured on
one surface is useless on another. `initiator` is the end that opened the connection, and on this
surface it is the only value that ever appears: the server never proves itself to a client. It is in
the input because on the **cluster** links both ends hold the same key and both answer challenges,
so without a role an attacker could reflect a node's own challenge back at it, be handed the answer,
and replay it.

| Response | Means |
|----------|-------|
| `OK CHALLENGE <hex>` | answer this, and only this — a new `AUTH` replaces the outstanding challenge |
| `OK AUTH <identity>` | the session is authenticated |
| `ERR auth_failed` | wrong response **or** unknown identity, deliberately indistinguishable; the connection is then closed |
| `ERR auth_no_challenge` | a response arrived with no challenge outstanding |
| `ERR already_authenticated` | this session has already authenticated |
| `ERR auth_disabled` | the server is not running with `--auth-secret-file` |

`AUTH` does not count as the session's first command, so `AUTH` followed by `COMPRESS LZ4` works.

A failed attempt closes the connection, which is also the rate limit: one attempt per connection,
and connections are bounded by `--max-sessions`.

## Large responses and slow clients

A response is queued per session and written as the socket accepts it, so a result set larger than the
kernel's send buffer is delivered across several event-loop turns rather than in one write. Two
consequences worth knowing:

- **A slow client is not disconnected.** Reading a 100 000-row result a few kilobytes at a time works;
  the server keeps the remainder queued and sends it as the client drains. Before this was buffered,
  any response above roughly 2 MB closed the connection mid-stream, with nothing in the log.
- **Queued output is capped at 64 MB per session** (about 1.7 million rows of response). A client that
  stops reading entirely while asking for more hits that cap and has its session closed, with the
  reason logged. This bounds server memory: without a cap, one client that never reads would grow the
  process without limit.
- **One answer larger than the cap is refused, not sent** — to any client, however fast it reads.
  The session gets `ERR answer of N bytes is larger than the 67108864 a session may have queued;
  narrow the query or add LIMIT` in its place and stays open (#152). Before that, the connection
  was closed with nothing but EOF to say why.

`ob_session_pending_bytes` in `/metrics` reports the bytes queued across all sessions. It is the
signal that a client is not keeping up, and it should sit at zero in a healthy system.

Use `LIMIT` when you do not need the whole scan — it is cheaper on both sides than transferring rows
you will discard.

## Pipelining

A connection may carry several commands before their answers are read. The server executes every
complete command from one read and answers them in order, so a client that keeps a window open
spends one round trip on a batch rather than one per command. Measured on an m9g.xlarge over
loopback, 20 levels per `MINSERT`: **1,314,663 levels/s** asking one at a time, **2,174,287** at
512 commands in flight — 1.65× for a change on the client's side only. Both of those are the
wire's rate with no flush due. Sustained over four million levels the same client measures
**~2.1M levels/s at the default `--flush-interval-ms 100`**, and 1.2M at one second: past a
million pending rows a writer waits for the next flush, so a longer interval costs throughput
(#137).

Until #140 this was not worth doing, and the reason was not the engine's parsing. No socket the
server *accepted* turned Nagle off, so the second answer of a batch waited in the server's kernel
for the client's delayed acknowledgement of the first: a fixed **~52 ms per round trip whatever
the batch size**, which capped a pipelining client at about nineteen round trips a second. If you
are reading numbers from before that change, that is what they measured.

Two things follow for a client. Responses are self-delimiting — `OK` bodies end in a blank line,
`ERR` and `PONG` are one line each — so a batched reader must count answers rather than read
"the next line"; and a terminator can straddle two reads, so a buffer cleared between reads loses
it. The subscription stream is unaffected either way: `PUSH` lines arrive as the server produces
them, whether or not anything else is in flight.

## Latency profiles: eco and boost

`--profile` names a set of the knobs below rather than a second code path. `eco` is the default and
is byte for byte what the server always did; `boost` sets `--io-spin-us`, so the io thread keeps
polling for a while after an event instead of blocking immediately.

What that buys is the **kernel wake-up**, and it is a constant on every round trip rather than a
tail. Measured on an m9g.xlarge over loopback, seven interleaved rounds of 20,000 `PING` round
trips through a bare socket, the order alternated, `loadavg` 0.01-0.05 throughout — and measured
**through the flag**, so the figures describe what ships rather than a hand-edited loop:

| | p50 | p99 | minimum | server CPU for 20,000 | while the probe ran |
|---|---|---|---|---|---|
| `eco` | 8074 ns | 8369 ns | 7573 ns | 0.090 s | 56% of a core |
| `boost` | **6578 ns** | **6837 ns** | **5993 ns** | 0.120 s | 91% of a core |
| difference | **−18.5%** | **−18.3%** | −20.9% | **+33%** | |

Medians of the rounds. Round to round, `eco`'s p50 spans 7978-8130 ns and `boost`'s 6499-6640, so
the difference is an order of magnitude wider than the spread it is read against.

**p50 and p99 fall by the same absolute amount** — 1496 ns and 1532 ns — which is what says a
constant on every round trip; a tail would take far more off p99 than off p50. The minimum moves
too, and by about the same amount, with more spread (`eco`'s ranges 6327-7689 across the rounds)
because a blocking loop is occasionally already awake when the next request arrives.

**The bounded window costs nothing against spinning for ever, and that is measured rather than
assumed.** `--io-spin-us 100000000` is a window long enough never to close, which is byte for byte
"always spin": three rounds gave p50 **6578 ns** — the same median as `boost` — for **0.130 s** of
CPU, 98% of a core. So the window buys back the idle cost and gives up no latency under continuous
traffic, which is the whole reason the mode has one: with no window an idle node holds a core
indefinitely, and the honest public cost is **"up to one core while traffic flows"** rather than
"one core".

**One thing this measurement does not say.** It is loopback on one machine: across a real network a
round trip is orders of magnitude larger and 1.5 µs stops being 18% and becomes noise. The mode is
worth exactly what it is worth to a **colocated** client.

`--io-spin-us` is the knob underneath and can be set on its own — a value the operator gave wins
over the profile, and `--print-config` says which of the two a value came from. `--profile`
refuses a name it does not know, so `--profile bost` does not start a node in `eco` that looks
like it started in `boost`.

## Client event loops: `--io-threads`

`--io-threads N` gives the server N client event loops. The one that holds the listening socket
accepts every connection and deals it to the next loop in turn; a connection stays on the loop it
was dealt to for its whole life, so the order of its answers is unchanged. `1` is the default and
is the single loop this server always had. The log names where each connection went —
`Reactor 2 adopted fd=14 conn_id=7 from 10.0.0.5:51234` — and the threads are named `ob-io-0` …
`ob-io-N-1`, so `top -H` says which loop is busy.

What more loops buy depends on what the connections ask, and both halves are measured (m9g.xlarge,
four cores, loopback, four connections pipelining batches of 64; roadmap #151):

| | 1 loop | 2 loops | 4 loops |
|---|---|---|---|
| `BOOK` reads, levels read / s | 15 992 260 | 30 254 474 | **49 212 362** |
| `MINSERT` writes, levels / s | 4 878 479 | 4 327 641 | 4 505 432 |
| a writer beside a connection scanning 1.5 M rows, levels / s | 65 424 | **4 995 672** | 4 877 810 |

- **Reads scale with the loops**, 3.08× at four here, with the load generator on the same cores.
- **A query no longer stalls the writes of another connection** — if that connection is on another
  loop. At one loop a writer beside a connection looping a 134 ms scan waited behind every scan;
  on another loop it did not notice. Connections are dealt in turn, so two can share a loop, and a
  heavy query holds its own loop for as long as it runs.
- **Pipelined writes from several connections do not scale yet, and cost about 10% with a tripled
  p99.** The engine takes its write lock once per record, and with several loops writing every
  acquisition contends. Until the engine applies a read's writes under one acquisition, a
  write-only workload is best served by the default.

`--max-sessions` is one limit for the server, not one per loop, and `--drain-timeout-ms` is one
deadline. `FAILOVER` and `MIGRATE` run alone across the loops, as they did when one loop ran every
command. Values from 1 to 64 are accepted, and 0 is refused rather than guessed at.

## Typical Session

```
ob> bulk BTC-USD BINANCE bid 20 6500000 100
ob> bulk BTC-USD BINANCE ask 20 6510000 50
ob> flush
ob> query SELECT * FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999 LIMIT 5
ob> status
ob> quit
```

## Durability and crash recovery

An acknowledged `INSERT` or `MINSERT` is in a WAL record before the reply is sent, and it survives a
process kill: on the next start, `Engine::open()` replays every WAL record written after the last
checkpoint, applies it, and flushes it into a segment so queries can see it. Whether it also
survives a **power cut** is `--fsync-policy`, described under Parameters below — the default,
`interval`, syncs within the flush interval rather than before the reply, so that sentence is about
a process ending, not about the platter. Under `every` the reply waits for the `fsync`, and since
#113 a failed `fsync` is answered `ERR` rather than `OK`.
The startup log states what happened, and it is worth reading after an unclean stop:

```
{"component":"wal","msg":"Replay after checkpoint: records=15 last_checkpoint_ordinal=11 forwarded=4"}
{"component":"engine","msg":"WAL replay: records=4 applied=4 skipped_already_flushed=0"}
```

`skipped_already_flushed` counts records whose rows a segment already holds. It is normally 0, and
non-zero after a crash that landed between writing the segment files and recording that fact.

`FLUSH` and a clean shutdown both end in a checkpoint, so a restart after either replays nothing.

On a **replica** the same restart also keeps what replication delivered. It saves how far it got in
the primary's log and, on reconnecting, asks the primary which stream it serves before asking to
resume — so it continues from that position instead of streaming the whole log again, and it starts
over only when the answer says the primary's WAL is not the one that position indexes. Which of the
two happened is a line in the replica's log, and the three forms it takes are in
`docs/operations.md`, under "A replica that restarted, and which stream it decided it was on".

### Parameters

| Flag | Default | Meaning |
|------|---------|---------|
| `--drain-timeout-ms <N>` | 10000 | On `SIGTERM`, how long to wait for open client sessions before closing them and exiting. `0` waits indefinitely, which is what the server did before #106 — and with a long-lived client that meant it never exited, so a supervisor's own timeout turned every graceful stop into a `SIGKILL`. Measured on an i3-7100U: 0.11 s to exit with nothing connected, still running after 60 s with one idle client attached. The exit is a clean 0 either way, and the node logs how many sessions it cut |
| `--flush-interval-ms <N>` | 100 | How often the background thread moves pending rows into columnar segments. Lower means less to replay after a crash and more segment churn; higher means the opposite. A long interval is also how the recovery tests keep rows in the WAL instead of racing the flush |

Durability of the WAL write itself is `--fsync-policy`, which takes `every`, `interval` or `none`
— lower case, compared exactly, and an unrecognised value is refused rather than read as the
default. With anything other than `every`, an acknowledged write can be lost on a power cut: the
replay described above cannot recover a record that never reached the platter.

How large the WAL grows before it starts a new file is `--wal-rotate-bytes`, and it is a **trigger
rather than a file size**: rotation is checked after a write, so a file may exceed the threshold by
one record — by more only while the disk refuses the ROTATE record that would end it, which
`docs/operations.md` shows in the log. Three things follow from the number, which is why it is a knob at all. It bounds what a
crash replays, together with `--flush-interval-ms`. It bounds what a reconnecting replica may have
to scan before the primary decides a snapshot is cheaper. And it is the granularity retention frees,
because WAL files are deleted whole and only below the file the slowest **connected** replica has
confirmed — so a large threshold means a lagging replica pins more bytes on disk, and a small one
means more files to walk.

It is refused at both ends rather than clamped. The ceiling is 2 GiB: a WAL position is a file index
and a 32-bit offset read as one value, and a larger file would let the offset wrap and report a
position inside the wrong part of the file. The floor is 65573 bytes — a 38-byte header plus the
64 KiB payload limit — because below one maximal record a single write can fill a file on its own
and every write rotates, which costs a WAL file per record. The engine's own unit tests do use a
512-byte threshold, on a `WALWriter` constructed directly, and that is the difference the refusal
draws: a component test that wants one file per record may ask for it, an operator who typed a
plausible small number is told what it would do.

This paragraph used to say the policy was set "at build/config level" and name the values `EVERY`,
`INTERVAL` and `NEVER`. Roadmap #33 made it a flag, and two of those three spellings are refused by
the parser — so the installed CLI reference told an operator that the most consequential setting in
a database was unreachable, and named values that will not start the server. Note the two enum flags
disagree on case, both as the shipped `ob.conf` writes them: `log-level = INFO` and
`fsync-policy = interval`.

## Full flag reference

Every flag the parser accepts, which is also every key the configuration file accepts —
the file is rewritten into arguments and handed to this same parser, so the two lists cannot
drift apart. `ob_tcp_server --help` prints this same set, generated from the same source.

This section exists because the man page promises it. `--help` used to list six of forty,
the man page said the full set was here, and this file had twenty-one — so the artefact that
promised completeness was the incomplete one, and that promise is printed on every host the
package is installed on. `CliConfigStatic.EveryKnownFlagIsInTheCliReference` holds it now.

| Flag | Argument | Meaning |
|------|----------|---------|
| `--anti-entropy-interval-seconds` | `<N>` | Multi-master reconciliation interval (default: 60) |
| `--auth-secret-file` | `<PATH>` | Client credentials, `<identity> <secret>` per line; mode 600. Empty disables client authentication |
| `--cluster-secret-file` | `<PATH>` | Shared secret for replication and multi-master links, one line; mode 600 |
| `--config` | `<FILE>` | Read `key = value` settings from FILE; command line wins |
| `--coordinator-endpoints` | `<URLS>` | Comma-separated etcd endpoints for HA and failover |
| `--coordinator-lease-ttl` | `<N>` | Leader lease TTL in seconds (default: 10) |
| `--data-dir` | `<DIR>` | Data directory for the engine (default: /tmp/ob_data) |
| `--election-deference-ms` | `<N>` | Wait for a replica further ahead in the log; 0 disables |
| `--election-lease-wait-ms` | `<N>` | Wait after the leader key vanishes before standing |
| `--failover-enabled` | `<BOOL>` | Participate in automatic failover: true/1/yes or false/0/no (default: true) |
| `--drain-timeout-ms` | `<N>` | On shutdown, how long to wait for open client sessions before closing them (default: 10000; 0 waits indefinitely) |
| `--flush-interval-ms` | `<N>` | Background flush interval in ms (default: 100) |
| `--fsync-policy` | `<POLICY>` | WAL durability: every, interval or none (lower case; default: interval) |
| `--io-spin-us` | `<N>` | Keep polling for this many microseconds after the last event before blocking again (default: 0, always block). Costs up to one core while traffic flows and takes ~20% off the loopback round trip |
| `--io-threads` | `<N>` | Client event loops, 1 to 64 (default: 1). The loop that accepts deals connections to them in turn, and a connection stays on the loop it was dealt to for its whole life |
| `--profile` | `<NAME>` | `eco` (default, blocking io) or `boost` (sets `io-spin-us`). A named set of the knobs, not a second code path; an unknown name is refused |
| `--handover-cooldown-seconds` | `<N>` | How long a node that handed the role over abstains |
| `--handover-grace-seconds` | `<N>` | Grace period granted to a handover target |
| `--log-level` | `<LEVEL>` | ERROR, WARN, INFO or DEBUG (upper case; default: INFO) |
| `--max-sessions` | `<N>` | Maximum concurrent client sessions (default: 64) |
| `--max-subscriber-queue-bytes` | `<N>` | Per-subscriber queue ceiling; past it the session closes |
| `--max-subscriptions-per-session` | `<N>` | Subscription limit per session (default: 16) |
| `--metrics-bind` | `<ADDR>` | Address the metrics listener binds to (default: every interface) |
| `--metrics-port` | `<PORT>` | Prometheus metrics port; 0 disables the endpoint |
| `--mm-max-catchup-bytes` | `<N>` | WAL bytes a peer may scan before a snapshot is used |
| `--mm-max-peer-send-buffer` | `<N>` | Per-peer send buffer ceiling; past it the peer is dropped |
| `--mm-node-id` | `<N>` | Multi-master node id, unique in the mesh |
| `--mm-replication-port` | `<PORT>` | Multi-master peer port |
| `--multi-master` | — (boolean) | Run as a multi-master node instead of primary/replica |
| `--node-id` | `<ID>` | This node's name, as it appears to the coordinator |
| `--port` | `<PORT>` | TCP port to listen on (default: 9090) |
| `--primary-host` | `<HOST>` | Primary to replicate from, when starting as a replica |
| `--primary-port` | `<PORT>` | Primary's replication port |
| `--print-config` | — (boolean) | Print every setting with its origin and exit; opens no port |
| `--read-only` | — (boolean) | Refuse writes regardless of role |
| `--replication-compress` | — (boolean) | Compress the replication stream with LZ4 |
| `--replication-port` | `<PORT>` | Port replicas connect to on this node |
| `--shard-id` | `<N>` | This node's shard, when sharding by symbol |
| `--shard-vnodes` | `<N>` | Virtual nodes per shard in the consistent hash ring |
| `--snapshot-chunk-size` | `<N>` | Bytes per snapshot transfer chunk |
| `--snapshot-staging-dir` | `<DIR>` | Where an incoming snapshot is staged before install |
| `--tls-ca-file` | `<PATH>` | Trust anchor (PEM) for verifying peer certificates on node links; required by `--tls-replication` and `--tls-multi-master` |
| `--tls-cert-file` | `<PATH>` | This node's certificate chain (PEM), used on every TLS surface and in both roles |
| `--tls-client` | — (boolean) | TLS on the client port; needs --tls-cert-file and --tls-key-file |
| `--tls-key-file` | `<PATH>` | This node's private key (PEM); mode 600 |
| `--tls-multi-master` | — (boolean) | TLS with mutual certificate verification on the multi-master mesh; needs `--tls-ca-file` |
| `--tls-peer-names` | `<NAMES>` | Comma-separated identities an accepted peer's certificate may carry; empty accepts any name this CA signed |
| `--tls-replication` | — (boolean) | TLS with mutual certificate verification on the replication link, in both roles; needs `--tls-ca-file` |
| `--ttl-hours` | `<N>` | Retention in hours; 0 keeps everything. Counted from **the record's own event time**, per segment — so a backfill written with `[event_time_ns]` arrives with its age, and a batch whose oldest row is past the window is expired on the next sweep. One row dated in the future keeps its whole segment |
| `--ttl-scan-interval-seconds` | `<N>` | How often retention scans for expired rows |
| `--wal-rotate-bytes` | `<N>` | WAL bytes before the next file is opened (default: 536870912). A **trigger**, not a file size: rotation is checked after a write, so a file may exceed it by one record. Refused below 65573 (one maximal record) and above 2 GiB |

## Argument handling

The server refuses a command line it does not fully understand, rather than starting with defaults:

```
$ ob_tcp_server --prot 5599
Error: unknown argument '--prot'

$ ob_tcp_server --port
Error: --port requires a value

$ ob_tcp_server --port abc
Error: --port expects a non-negative integer, got 'abc'

$ ob_tcp_server --port 99999
Error: --port expects a value in range, got '99999'
```

All four used to be accepted in some form. A typo in a flag name was ignored along with its value, a
flag with no value fell through, a non-numeric value threw an uncaught `std::invalid_argument` from
`stoi`, and an out-of-range port was cast into range — `99999` became `34463`, so the server listened
on a port nobody had named. If you have scripts passing flags this binary does not know, they will now
fail instead of starting a server with a configuration you did not intend.

## Wire commands: the same rule, since #107

The wire protocol now refuses a command line carrying a token its grammar has no place for, and the
refusal names the token:

```
C: INSERT BTC-USD BINANCE bid 6500000 150 1 surprise
S: ERR unexpected token 'surprise'; INSERT takes: INSERT <symbol> <exchange> <bid|ask> <price> <qty> [count] [event_time_ns]

C: MINSERT BTC-USD BINANCE bid 1 yesterday
S: ERR invalid event time 'yesterday'; MINSERT takes nanoseconds since the epoch

C: PING please
S: ERR unexpected token 'please'; PING takes: PING
```

**All of those used to answer `OK`** — and the first two stored a row for a value the server
discarded. Measured before the change: fourteen command shapes accepted a token nobody reads, five
of them wrote it away, and `MM_CONFLICTS notanumber` quietly became `MM_CONFLICTS 100`. It is the
same defect as the flag section above, one layer out: the parser read the fields it knew and ignored
the rest.

Three things are worth knowing if you write against this protocol:

- **the count is not in the message, the token is.** "Too many arguments" sends you counting spaces.
- **`SELECT` and `SUBSCRIBE` are not counted here**, because their tail is a query and the query
  parser already refuses a trailing token by name (`ERR Parse error at line 1, col 26: unexpected
  token 'garbage'`). The exemption is from counting, not from refusing.
- **an `AUTH` line's extra token is refused without being repeated.** Every refusal writes a log
  line, and a response echoed into a log is a response in a log.

A level line of a `MINSERT` batch follows the same rule and the refusal says which line:
`ERR unexpected token 'x' on level line 2; a level line takes: <price> <qty> [count]`.

If you have a client sending a field this server does not know, it will now be told so instead of
being answered `OK`. That is the point, and it is what made the event time below expressible: an
upgraded client sending one to an older server used to get `OK` with the time dropped, so it could
not tell a server that stored it from one that did not.

## Writing with your own event time

`INSERT` and `MINSERT` take an **optional last field**: the time the update happened, in nanoseconds
since the epoch.

```
C: INSERT BTC-USD BINANCE bid 6500000 150 1 1700000000000000000
S: OK

C: MINSERT BTC-USD BINANCE bid 2 1700000000000000000
C: 6500000 150 1
C: 6499900 200 1
S: OK
```

Omit it and the server stamps arrival time, which is what every release before this one did with
every write. That is still the right answer for a live feed; the field is for **backfill**, where
arrival time is the time of the import rather than the time of the market.

```
C: STATUS
S: ...
S: capabilities: insert_event_time,strict_args
```

**Ask before you send.** A server that predates this field answers `OK` and stores the row with
arrival time — measured, on the release before it — so a client cannot infer support from a
successful write. Both shipped clients ask once per connection and **refuse rather than drop**:
`OrderbookEngine.insert(..., timestamp_ns=T)` raises, and `OrderbookClient::insert(..., T)` returns
an error, before a byte goes out. The absence of the `capabilities:` line is an answer too, not a
failure.

Three refusals, each naming the token rather than counting arguments:

```
C: INSERT BTC-USD BINANCE bid 6500000 150 1 0
S: ERR event time 0 means "unassigned"; omit the field to get arrival time

C: INSERT BTC-USD BINANCE bid 6500000 150 1 yesterday
S: ERR invalid event time 'yesterday'; INSERT takes nanoseconds since the epoch

C: INSERT BTC-USD BINANCE bid 6500000 150 1 1700000000000000000 extra
S: ERR unexpected token 'extra'; INSERT takes: ...
```

There is **no sanity window** and that is deliberate: a server that refused a timestamp from 2019
would break backfill, which is the case this field exists for. What the time does affect is written
down in `docs/operations.md` — retention counts by the record's own time, so loading history loads
its age along with it.

One field per batch, not per level: a batch **is** one book update at one instant, which is what the
engine's own `DeltaUpdate` models.

## Configuration file

Thirty-seven flags is past the point where a command line is a reasonable way to configure a
service, and a systemd unit carrying them all means an operator changing one setting edits the unit.
So `--config` reads them from a file:

```ini
# /etc/orderbook/ob.conf — keys are flag names without the dashes.
port          = 9090
data-dir      = /var/lib/orderbook
max-sessions  = 256          # a comment may follow a value
log-level     = INFO

# Booleans take true or false.
multi-master  = true
mm-node-id    = 1
read-only     = false
```

```
$ ob_tcp_server --config /etc/orderbook/ob.conf
```

**A key is a flag name.** Not a parallel vocabulary with a mapping table — the file is rewritten into
command-line arguments and handed to the same parser, so a new flag is a valid key the moment it
exists, and a value is validated by the same code with the same message whether it came from a file
or a flag. Two static tests hold that: the list of valid keys is checked against the parser's own
branches, and so is the list of flags that take no value.

**A flag overrides the file; the file overrides the default.** There is no merge step, because the
file's arguments simply come first and the parser assigns.

### Seeing what the server resolved

```
$ ob_tcp_server --config /etc/orderbook/ob.conf --port 9191 --print-config
# Resolved configuration. Provenance in brackets: a list of values does not say which
# of them you chose, and that is the question this flag exists to answer.
  data-dir                         /var/lib/orderbook  (file)
  log-level                        INFO  (file)
  max-sessions                     256  (file)
  port                             9191  (command line)
  read-only                        false  (default)
  ...
```

`--print-config` prints and exits **without opening a port**, so it still works when the port is
taken — which is one of the situations you reach for it in.

Every value it prints is one the server reads, except `profile`, which is the name of the set of
values it decided. `--workers` used to be the other exception — parsed, printed here with a note
that nothing used it, and read by nothing — and it is refused now (#149), with
`CliConfigStatic.EveryParsedValueIsReadByTheServer` holding the rule for the rest.

### Refusals

A configuration file with a mistake in it does not start a server. Same rule as a mistyped flag:

| What | Message |
|---|---|
| unknown key | `unknown key 'prot'. Closest known keys: port, ...` |
| missing file | `cannot open config file '...'` |
| line without `=` | `<path>:12: expected 'key = value', got '...'` |
| the same key twice | `<path>:8: 'port' is set more than once` |
| non-boolean for a boolean key | `'read-only' takes true or false, got 'yes'` |
| empty value | `'data-dir' has no value` |
| `config` inside a config file | `'config' cannot be set from inside a config file` |

Duplicate keys are refused rather than resolved last-wins, because last-wins is a silent choice
between two things you wrote. A chain of config files is refused outright rather than depth-limited,
because a depth limit answers "how deep" when the question is "why".

## Multi-Master Replication

Multi-master mode allows multiple nodes to accept writes simultaneously. All nodes in the cluster replicate data to each other via WAL streaming in a full-mesh topology. Conflicts (concurrent writes to the same price level) are resolved automatically using Last-Writer-Wins (LWW) based on Hybrid Logical Clock (HLC).

### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--multi-master` | — | off | Enable multi-master mode |
| `--mm-node-id <uint16>` | yes (in MM mode) | — | Unique node identifier in the cluster (1–65535) |
| `--mm-replication-port <port>` | yes (in MM mode) | — | TCP port for inter-node WAL replication |
| `--anti-entropy-interval-seconds <N>` | no | 30 | Interval for anti-entropy consistency checks |
| `--mm-max-catchup-bytes <N>` | no | 536870912 (512MB) | Max catch-up buffer before the peer is dropped and re-synced |
| `--mm-max-peer-send-buffer <N>` | no | 67108864 (64MB) | Queued output one peer may hold before its connection is dropped. A peer that stops reading — partitioned, paused or merely slow — otherwise grows the writer without bound: measured at about 113 MB/s per unreachable peer before this ceiling existed. Same ceiling a client session gets |

Multi-master mode also requires:
- `--coordinator-endpoints` — etcd endpoint(s) for peer discovery
- `--replication-port` — standard replication port (used for catch-up)

Multi-master mode is incompatible with:
- `--read-only` — all MM nodes accept writes
- `--primary-host` / `--primary-port` — single-primary replication

### Example: 3-Node Multi-Master Cluster

Start etcd (if not already running). Install it natively, the same way the engine itself runs.
There is deliberately no container in this path:

```bash
ETCD_VER=v3.5.17
curl -L https://github.com/etcd-io/etcd/releases/download/$ETCD_VER/etcd-$ETCD_VER-linux-amd64.tar.gz | tar xz
sudo install -m755 etcd-$ETCD_VER-linux-amd64/etcd etcd-$ETCD_VER-linux-amd64/etcdctl /usr/local/bin/

etcd --name node-etcd --data-dir /var/lib/ob-etcd \
  --advertise-client-urls http://127.0.0.1:2379 \
  --listen-client-urls http://127.0.0.1:2379
```

On a permanent deployment, run etcd from a systemd unit and order `ob_tcp_server` after it.

Start three multi-master nodes:

```bash
# Node 1
./build/ob_tcp_server \
  --port 5555 --data-dir /tmp/mm_node1 \
  --coordinator-endpoints http://127.0.0.1:2379 \
  --node-id mm_node_1 \
  --multi-master --mm-node-id 1 \
  --mm-replication-port 6001 \
  --replication-port 6001

# Node 2
./build/ob_tcp_server \
  --port 5556 --data-dir /tmp/mm_node2 \
  --coordinator-endpoints http://127.0.0.1:2379 \
  --node-id mm_node_2 \
  --multi-master --mm-node-id 2 \
  --mm-replication-port 6002 \
  --replication-port 6002

# Node 3
./build/ob_tcp_server \
  --port 5557 --data-dir /tmp/mm_node3 \
  --coordinator-endpoints http://127.0.0.1:2379 \
  --node-id mm_node_3 \
  --multi-master --mm-node-id 3 \
  --mm-replication-port 6003 \
  --replication-port 6003
```

All three nodes accept writes. Data written to any node is automatically replicated to the others:

```bash
# Write to node 1
echo "INSERT BTC-USD BINANCE bid 6500000 150 3" | nc localhost 5555

# Read from node 2 (data is replicated)
echo "SELECT * FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999" | nc localhost 5556

# Check cluster status
echo "MM_PEERS" | nc localhost 5555
echo "MM_CONFLICTS" | nc localhost 5555
```

`MM_PEERS` answers a header line and then one line per peer: `node_id`, `address`, `status`,
`hlc_timestamp`, `send_queue_bytes`. Two of those are worth reading carefully. `status` is
`connected` or `disconnected` — the state of the link, not the `status` a node publishes about
itself in the peer registry. That registry field only ever holds **`active`**: `register_self()` is
its only writer and is called with that one string. Earlier revisions of this page named
`joining` and `leaving` beside it; nothing has ever written either, and since the two stub methods
that looked as though they might were deleted (#134), nothing can. And `send_queue_bytes` is what this node has queued to
send that peer: zero on a healthy link, and still zero through the few megabytes the sender's
socket buffer absorbs, so it is a backpressure signal rather than a measure of how far behind the
peer is.

That column was called `lag_bytes` until #118 and the value has not changed — only the name, which
was the whole problem: it invited the alert the word "lag" implies on a number that is zero exactly
when a node has accepted writes it has not yet handed to the kernel. `STATUS` also printed
`replication_lag_peer_<id>`, which subtracted a position in the peer's **own** WAL, recorded once
at handshake, from this node's offset — on a converged mesh that equals this node's own WAL size,
which is to say it reported a peer holding every row as sitting at byte zero. Those lines are
**gone**, not renamed: the honest number is in records and comes from a different mechanism, so
keeping the field would have left every reader parsing the same line about a different subject.

**How far behind a mesh peer is, today:** `ob_mm_replication_lag_records` in `/metrics`, read
beside `ob_mm_peers_position_unknown`. See "When a mesh peer falls behind" in
[operations.md](operations.md), which says what the pair means and how stale it is.

The command lists **peers**, meaning connections whose handshake has said who they are — an
inbound connection that has not got that far is not listed, because it used to appear as
`0  (no address)  disconnected`, which reads as a peer that has fallen over and counts as one
node too many. The number of such connections is logged at DEBUG rather than put on the wire,
since these rows are parsed.

## High Availability: graceful failover

With `--coordinator-endpoints` set, one node holds the primary role under an etcd lease and the
others follow as replicas. Before taking the primary down for maintenance, hand the role over
deliberately rather than letting the cluster discover the outage:

```bash
echo "FAILOVER node_B" | nc localhost 5555
```

### What happens

1. The primary validates that `node_B` is a node the coordinator knows about
2. It publishes a **handover intent** naming `node_B`, with a deadline
3. It blocks itself from standing for election for the cooldown period
4. It revokes its lease, so the leader key disappears

While the intent is live, only `node_B` campaigns for the leader key; the other replicas stand
aside. This is what makes the role land where you sent it rather than with whichever replica polls
first.

If `node_B` never takes over, the intent expires at its deadline and the cluster falls back to an
ordinary election, so an unreachable target cannot leave you without a primary.

### Responses

| Response | Meaning |
|----------|---------|
| `OK` | Handover **initiated**. Not the same as finished, see below |
| `ERR not_primary` | This node is not the primary |
| `ERR failover_not_configured` | No coordinator configured |
| `ERR invalid_target <id>` | Target was empty, or named this node itself |
| `ERR unknown_target <id>` | Target is not known to the coordinator, usually a typo in a node id |
| `ERR failover_failed` | Coordinator error; the node kept its role and its lease |

**The connection may close without any reply, and that is not in the table above because it is not
an answer.** The outgoing node is tearing down its primary machinery while your session is open, and
it can drop the session before the acknowledgement reaches you. Treat a closed connection as
*unknown* rather than as failure: the handover has usually happened. Ask the target, as below.

Roadmap #86 holds this open as an interface question rather than a bug — an operator should not have
to infer the outcome of a deliberate operation. What is fixed is worse and was real: until #88 the
outgoing node could **abort** during a graceful handover, so the closed session was sometimes a dead
process. It stays up and becomes a replica now, which the two checks below confirm.

**`OK` means initiated, not completed.** Confirm the outcome by asking the target:

```bash
echo "ROLE" | nc localhost 5556     # expect: PRIMARY <epoch>
```

And confirm the node you handed it away from is a replica rather than gone:

```bash
echo "ROLE" | nc localhost 5555     # expect: REPLICA <primary address> <epoch>
```

The epoch a replica reports is the one it is **following**, and it is the same number both epoch
guards on the replication link stand on. It survives a restart and a role change, and it never goes
backwards — so a node whose data directory belongs to a cluster that got further will refuse to
follow a primary behind it, saying so with both numbers. `docs/operations.md` covers that line, which
has two readings (#103). Until #103 a replica answered `0` here no matter whose stream it was
replaying.

Anything other than `OK` leaves the node primary with its lease intact, so a rejected handover is
never a partial one.

### Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--handover-grace-seconds` | 5 | How long the named target gets before the cluster falls back to an ordinary election. Keep it below the lease TTL, so a handover completes faster than a failure is detected |
| `--handover-cooldown-seconds` | 15 | How long the outgoing primary refrains from standing for election. Must be >= the grace window, otherwise it could win the race it just announced. Keep it above the lease TTL, so it does not return before the new primary settles |
| `--election-deference-ms` | 3000 | How long a candidate waits when another node has published a further WAL position, so the most advanced replica gets first refusal. Bounded on purpose: unbounded deference would leave the cluster with no primary at all if the node it waits for never comes back. `0` disables deference and restores the pre-#70 race. Since #72 the positions carry a per-node lease, so a dead node drops off the list on its own and this window is a **backstop** — it now fires only for a node that is alive, refreshing its lease, and still not promoting |

| `--election-lease-wait-ms` | 0 (derive from the lease TTL) | How long a candidate waits after **first seeing the leader key absent**, before standing for election. This is what closes the window in #82: a revoked or expired lease deletes the leader key immediately, while the previous holder learns on its next poll, so a candidate that claims the vacated key at once can coexist with a node that still believes it is primary — and both accept writes. The default equals the lease TTL, which is the bound within which the previous holder is guaranteed to have stepped down. It costs failover latency every time, roughly the TTL on top of what failover took before. A smaller explicit value narrows the margin in proportion; a value below the holder's own step-down bound reopens the window. A cold start does not wait, because no leader has existed to wait for |

The server refuses to start if the cooldown is shorter than the grace window.
