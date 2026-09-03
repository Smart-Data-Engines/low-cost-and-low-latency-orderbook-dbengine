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
  ts_ns               | side | level | price        | qty          | orders |      seq
  ────────────────────┼──────┼───────┼──────────────┼──────────────┼────────┼─────────
  1773478813946338657  | bid  |     0 |      6500000 |          150 |      3 |        1
  1773478813948808615  | bid  |     0 |      6499000 |          200 |      5 |        2
  ── 2 row(s) in 0.11 ms

`seq` is the per-origin sequence number of the update that produced the row, and the wire protocol
carries it as the seventh column of a `SELECT` response. Consecutive numbers for one symbol mean
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

`ob_session_pending_bytes` in `/metrics` reports the bytes queued across all sessions. It is the
signal that a client is not keeping up, and it should sit at zero in a healthy system.

Use `LIMIT` when you do not need the whole scan — it is cheaper on both sides than transferring rows
you will discard.

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

An acknowledged `INSERT` or `MINSERT` is in a fsynced WAL record before the reply is sent, and it
survives a process kill or a power cut: on the next start, `Engine::open()` replays every WAL record
written after the last checkpoint, applies it, and flushes it into a segment so queries can see it.
The startup log states what happened, and it is worth reading after an unclean stop:

```
{"component":"wal","msg":"Replay after checkpoint: records=15 last_checkpoint_ordinal=11 forwarded=4"}
{"component":"engine","msg":"WAL replay: records=4 applied=4 skipped_already_flushed=0"}
```

`skipped_already_flushed` counts records whose rows a segment already holds. It is normally 0, and
non-zero after a crash that landed between writing the segment files and recording that fact.

`FLUSH` and a clean shutdown both end in a checkpoint, so a restart after either replays nothing.

### Parameters

| Flag | Default | Meaning |
|------|---------|---------|
| `--flush-interval-ms <N>` | 100 | How often the background thread moves pending rows into columnar segments. Lower means less to replay after a crash and more segment churn; higher means the opposite. A long interval is also how the recovery tests keep rows in the WAL instead of racing the flush |

Durability of the WAL write itself is `--fsync-policy`, which takes `every`, `interval` or `none`
— lower case, compared exactly, and an unrecognised value is refused rather than read as the
default. With anything other than `every`, an acknowledged write can be lost on a power cut: the
replay described above cannot recover a record that never reached the platter.

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
| `--config` | `<FILE>` | Read `key = value` settings from FILE; command line wins |
| `--coordinator-endpoints` | `<URLS>` | Comma-separated etcd endpoints for HA and failover |
| `--coordinator-lease-ttl` | `<N>` | Leader lease TTL in seconds (default: 10) |
| `--data-dir` | `<DIR>` | Data directory for the engine (default: /tmp/ob_data) |
| `--election-deference-ms` | `<N>` | Wait for a replica further ahead in the log; 0 disables |
| `--election-lease-wait-ms` | `<N>` | Wait after the leader key vanishes before standing |
| `--failover-enabled` | `<BOOL>` | Participate in automatic failover: true/1/yes or false/0/no (default: true) |
| `--flush-interval-ms` | `<N>` | Background flush interval in ms (default: 100) |
| `--fsync-policy` | `<POLICY>` | WAL durability: every, interval or none (lower case; default: interval) |
| `--handover-cooldown-seconds` | `<N>` | How long a node that handed the role over abstains |
| `--handover-grace-seconds` | `<N>` | Grace period granted to a handover target |
| `--log-level` | `<LEVEL>` | ERROR, WARN, INFO or DEBUG (upper case; default: INFO) |
| `--max-sessions` | `<N>` | Maximum concurrent client sessions (default: 64) |
| `--max-subscriber-queue-bytes` | `<N>` | Per-subscriber queue ceiling; past it the session closes |
| `--max-subscriptions-per-session` | `<N>` | Subscription limit per session (default: 16) |
| `--metrics-port` | `<PORT>` | Prometheus metrics port; 0 disables the endpoint |
| `--mm-max-catchup-bytes` | `<N>` | WAL bytes a peer may scan before a snapshot is used |
| `--mm-max-peer-send-buffer` | `<N>` | Per-peer send buffer ceiling; past it the peer is dropped |
| `--mm-node-id` | `<N>` | Multi-master node id, unique in the mesh |
| `--mm-replication-port` | `<PORT>` | Multi-master peer port |
| `--multi-master` | — (boolean) | Run as a multi-master node instead of primary/replica |
| `--no-sqpoll` | — (boolean) | Disable io_uring SQPOLL even where it is available |
| `--node-id` | `<ID>` | This node's name, as it appears to the coordinator |
| `--port` | `<PORT>` | TCP port to listen on (default: 9090) |
| `--primary-host` | `<HOST>` | Primary to replicate from, when starting as a replica |
| `--primary-port` | `<PORT>` | Primary's replication port |
| `--print-config` | — (boolean) | Print every setting with its origin and exit; opens no port |
| `--read-only` | — (boolean) | Refuse writes regardless of role |
| `--replication-compress` | — (boolean) | Compress the replication stream with LZ4 |
| `--replication-port` | `<PORT>` | Port replicas connect to on this node |
| `--ring-size` | `<N>` | io_uring submission queue size |
| `--shard-id` | `<N>` | This node's shard, when sharding by symbol |
| `--shard-vnodes` | `<N>` | Virtual nodes per shard in the consistent hash ring |
| `--snapshot-chunk-size` | `<N>` | Bytes per snapshot transfer chunk |
| `--snapshot-staging-dir` | `<DIR>` | Where an incoming snapshot is staged before install |
| `--sqpoll-idle-ms` | `<N>` | io_uring SQPOLL idle timeout in ms |
| `--ttl-hours` | `<N>` | Retention in hours; 0 keeps everything |
| `--ttl-scan-interval-seconds` | `<N>` | How often retention scans for expired rows |
| `--workers` | `<N>` | Number of worker threads (default: 4) |

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

The output includes `workers`, which is parsed and not used: client commands run inline on the epoll
loop. It is printed rather than hidden, because hiding it would leave an operator tuning a knob that
does nothing.

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
`hlc_timestamp`, `lag_bytes`. It lists **peers**, meaning connections whose handshake has said who
they are — an inbound connection that has not got that far is not listed, because it used to appear
as `0  (no address)  disconnected`, which reads as a peer that has fallen over and counts as one node
too many. The number of such connections is logged at DEBUG rather than put on the wire, since these
rows are parsed.

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
echo "ROLE" | nc localhost 5555     # expect: REPLICA
```

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
