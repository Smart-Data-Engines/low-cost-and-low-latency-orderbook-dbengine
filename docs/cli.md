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

## Multi-Master Replication

Multi-master mode allows multiple nodes to accept writes simultaneously. All nodes in the cluster replicate data to each other via WAL streaming in a full-mesh topology. Conflicts (concurrent writes to the same price level) are resolved automatically using Last-Writer-Wins (LWW) based on Hybrid Logical Clock (HLC).

### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--multi-master` | — | off | Enable multi-master mode |
| `--mm-node-id <uint16>` | yes (in MM mode) | — | Unique node identifier in the cluster (1–65535) |
| `--mm-replication-port <port>` | yes (in MM mode) | — | TCP port for inter-node WAL replication |
| `--anti-entropy-interval-seconds <N>` | no | 30 | Interval for anti-entropy consistency checks |
| `--mm-max-catchup-bytes <N>` | no | 536870912 (512MB) | Max catch-up buffer before switching to snapshot sync |

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

**`OK` means initiated, not completed.** Confirm the outcome by asking the target:

```bash
echo "ROLE" | nc localhost 5556     # expect: PRIMARY <epoch>
```

Anything other than `OK` leaves the node primary with its lease intact, so a rejected handover is
never a partial one.

### Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--handover-grace-seconds` | 5 | How long the named target gets before the cluster falls back to an ordinary election. Keep it below the lease TTL, so a handover completes faster than a failure is detected |
| `--handover-cooldown-seconds` | 15 | How long the outgoing primary refrains from standing for election. Must be >= the grace window, otherwise it could win the race it just announced. Keep it above the lease TTL, so it does not return before the new primary settles |

The server refuses to start if the cooldown is shorter than the grace window.
