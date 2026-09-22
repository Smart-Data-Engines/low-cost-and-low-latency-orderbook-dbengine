# Python Bindings

Zero-dependency Python package with two modes of operation:
- **Local** — direct in-process access via ctypes (requires shared library)
- **TCP** — connect to a running `ob_tcp_server` over the network (no native deps needed)

## Installation

```bash
# From the project root — builds the C++ shared library and installs the package
pip install .

# Or with uv
uv pip install .
```

For TCP-only usage, you can also just copy `python/orderbook_engine/` — it has no native dependencies in TCP mode.

## Quick Start

```python
from orderbook_engine import OrderbookEngine

# Local mode
engine = OrderbookEngine("/tmp/ob_data")

# TCP mode (connect to running ob_tcp_server)
engine = OrderbookEngine(host="10.0.0.1", port=5555)
```

Both modes expose the same API.

## API Reference

### OrderbookEngine

```python
# Local mode
engine = OrderbookEngine(data_dir="/tmp/ob_data")

# TCP mode
engine = OrderbookEngine(host="127.0.0.1", port=5555, timeout=10.0)
```

Supports context manager:

```python
with OrderbookEngine(host="localhost", port=5555) as engine:
    engine.insert(...)
```

#### engine.mode → str

Returns `"local"` or `"tcp"`.

#### engine.insert(symbol, exchange, side, prices, qtys, counts=None, timestamp_ns=None, seq=None) → int

Insert one or more price levels.

```python
engine.insert(
    symbol="BTC-USD",
    exchange="BINANCE",
    side="bid",
    prices=[6_500_000, 6_499_000],
    qtys=[150, 200],
    counts=[3, 5],           # optional, default: [1, 1, ...]
    timestamp_ns=None,       # optional; None means the server stamps arrival time
)
```

Returns the sequence number used.

**`timestamp_ns` is the time the update happened**, and since #105 it reaches the server over TCP as
well as in embedded mode. Before that it was honoured embedded and **silently dropped on the wire** —
measured: a dataset's own span selected 0 of 400 rows through TCP while the same load into ClickHouse
and TimescaleDB selected 400.

Two things follow, and both are refusals rather than surprises:

- **against a server that cannot store one, this raises and sends nothing.** The client asks
  `STATUS` once per connection (`engine.server_capabilities()` → `{"insert_event_time", ...}`) and
  fails before a byte goes out, because a write that went out without the time its caller chose is a
  row nobody can find afterwards. Sending the field is not a test for support: a server that predates
  the field answers `OK` and discards it.
- **`seq` cannot be chosen over the wire.** A sequence number belongs to the origin, and over TCP
  that is the server, which assigns one per symbol. The argument used to be accepted and discarded;
  it is now refused. In embedded mode you still choose it.

#### engine.insert_batch(updates) → List[BatchOutcome]

Several updates in one round trip, with a word about each of them.

```python
from orderbook_engine import BookUpdate

outcomes = engine.insert_batch([
    BookUpdate("BTC-USD", "BINANCE", "bid", [6_500_000], [150]),
    BookUpdate("ETH-USD", "BINANCE", "ask", [3_100_000, 3_100_500], [40, 60]),
])
for o in outcomes:
    if not o.ok:
        print(f"update {o.index} refused: {o.message}")
```

The server has always executed every complete command it finds in one read; until #140 no client
gained anything by sending several, because the second answer waited out a delayed-ACK timer.
Measured on an m9g.xlarge, 5000 updates of 20 levels, five rounds with alternating order, medians:

| | levels/s | client CPU per level | against the loop |
|---|---|---|---|
| `insert()` in a loop | 969,204 | 623 ns | — |
| `insert_batch`, 1 per call | 883,923 | 725 ns | **0.91×** |
| `insert_batch`, 8 per call | 1,261,354 | 531 ns | 1.30× |
| `insert_batch`, 64 per call | 1,542,355 | 496 ns | **1.59×** |
| `insert_batch`, 512 per call | 1,542,096 | 502 ns | 1.59× |

**A batch of one is 9% slower than `insert()`, reproducibly** — the extra 102 ns per level is this
method's own bookkeeping, and for one update `insert()` is the right call. It pays from about
eight, and stops improving after 64.

Two things that table says about the client rather than the engine. At batch 64 it spends 0.050 s
of its own CPU on 0.065 s of wall, so **the Python client is most of what is left**: the same work
from the C++ client reaches 2,174,287 levels/s against the same server. And these are the wire's
rate with no flush due — sustained over four million levels the server settles near 1.2M
levels/s, which is a property of the engine rather than of the client.

**It is not a transaction.** A batch is N independent writes in one journey: some may land and
others be refused, which is why the result is a list rather than a return code. Everything that
can be refused *before* sending is refused for the whole batch, because a partial send after
rejecting one update is a write nobody can find afterwards.

Three named refusals, each because the honest answer is not obvious:

- **pool and sharded mode**: the router picks a connection per symbol, so a batch spanning symbols
  is several batches on several connections, and which of them is one round trip is a decision
  nobody has measured.
- **a compressed connection**: each command is its own LZ4 frame, and whether the server takes
  several frames from one read has not been measured.
- **`MAX_BATCH_BYTES` (8 MB)**: not a server limit — it bounds the caller's own memory. Split and
  send twice; two batches are two round trips, not a failure.

In embedded mode this is a loop over `insert()`, which is exactly what it is, and the docstring
says so rather than implying a round trip that does not exist.

**A transport failure part-way through raises, and the outcomes already read are lost.** The
server answers in order, so when the connection drops some of the batch has been acknowledged and
the rest has not, and this method cannot hand back both a list and an exception. Treat a raise as
*indeterminate* — the batch may have landed in full, in part, or not at all — and read to find
out. It is where a single `insert()` leaves you when the connection drops on its reply, widened
to the size of the batch, which is a reason to keep batches at a size whose re-examination you
can afford.

#### engine.flush()

Force-flush pending data so it becomes queryable.

#### engine.query(sql) → List[OrderbookRow]

Execute a SQL query.

```python
rows = engine.query(
    "SELECT * FROM 'BTC-USD'.'BINANCE' "
    "WHERE timestamp BETWEEN 0 AND 9999999999999999999"
)
```

#### engine.query_all(symbol, exchange, limit=None) → List[OrderbookRow]

Convenience method to query all rows for a symbol/exchange pair.

#### engine.book(symbol, exchange, depth=None) → List[OrderbookRow]

The **live** book: the current levels of both sides. TCP and pool mode only.

A different question from `query()`, which reads history and answers with every version of a level
it has stored. This reads the structure the engine updates in place, so there is one row per level,
and it is one `memcpy` per side on the server.

```python
rows = engine.book("BTC-USD", "BINANCE", depth=5)   # five levels per side
best_bid = next(r for r in rows if r.side == "bid")
as_of    = rows[0].sequence_number                  # resume a subscribe() from here
```

Bids come first, then asks, each side in its own order (bids descending, asks ascending), and
`level` restarts at 0 for the asks because a level index is a position within a side.

**Two of the seven fields are properties of the read rather than of a level**: every row carries
the same `timestamp_ns` and `sequence_number`, which say *as of which update* this book is. They
are not the time a level changed.

`depth` is per side and counts from the best level; `None` asks for everything the side has. A
`depth` below 1 is refused by the client before it reaches the wire, and one above 1000 is refused
by the server, which is the most levels per side it stores.

Refused in local mode, and the message says why: the embedded path goes through the C API, which
has no entry point for a live-book read. The aggregate functions do reach the live buffer in local
mode.

#### engine.query_agg(symbol, exchange, *exprs) → Dict[str, AggValue]

Run aggregate expressions against the live orderbook. TCP and pool mode only.

```python
aggs = engine.query_agg("BTC-USD", "BINANCE", "SPREAD(*)", "MID_PRICE(*)", "IMBALANCE(10)")

aggs["MID_PRICE(*)"].real       # 100500.0  — already divided by the scale
aggs["MID_PRICE(*)"].value      # 100500000000  — raw, scaled by 10^6
aggs["MID_PRICE(*)"].scale      # 1000000
aggs["SPREAD(*)"].is_empty      # False
```

No timestamp range is sent: aggregates are computed over the current book, and the server rejects a
timestamp or price filter rather than accepting one and ignoring it.

`query()` raises `OrderbookError` if the query turns out to return aggregates, because the row parser
would silently discard all three columns and hand back an empty list.

It raises for the same reason on a **narrowed** response. Since #139 the server answers the columns
a query names — `SELECT price, quantity FROM ...` returns two — and this client reads a row by
position, so it would read the price as a timestamp or skip the row as too short. It checks the
header instead and names the columns it was handed:

```python
engine.query("SELECT price FROM 'BTC-USD'.'BINANCE' WHERE ...")
# OrderbookError: this client reads the standard row columns by position and the server
# answered with ['price']. Ask for `SELECT *`, or read the response with a client that
# reads columns by name.
```

A narrowed query is worth asking for — it is fewer bytes on the wire and less work in the server —
but until this client reads columns by name it has to be sent over the raw protocol. `SELECT *` is
unchanged and is what every method here sends.

#### engine.status() → dict

Returns server statistics, parsed from every `key: value` field the server sends: `sessions`,
`queries`, `inserts`, `role`, `epoch`, `replicas`, `primary_address`, `lease_ttl_remaining`, the
`ttl_*` counters and `segment_merge_refused`.

#### engine.ping() → str

Returns `"PONG"`. Useful for connection health checks in TCP mode, and it works before
authentication so a health check needs no credentials.

#### auth=(identity, secret)

For a server running with `--auth-secret-file`:

```python
eng = OrderbookEngine(host="10.0.0.1", port=9090, auth=("grafana", secret))
```

Authentication happens on connect, after the banner and before compression negotiation, and again on
every reconnect in pool mode. The secret never crosses the wire — the client answers a challenge with
HMAC-SHA256 — and it is not rendered by any `repr` in the library.

Against a server that is **not** authenticating, this raises `OrderbookError`. Deliberately: a client
that believes it authenticated while the server authenticates nobody has a deployment problem, and
continuing silently would hide it.

#### tls=True, tls_ca_file=..., tls_verify=True

For a server running with `--tls-client`:

```python
eng = OrderbookEngine(host="db1.internal", port=9090,
                      tls=True, tls_ca_file="/etc/ssl/certs/internal-ca.pem",
                      auth=("grafana", secret))
```

TLS 1.3 minimum, matching the server's floor. `tls_ca_file` defaults to the system trust store.

`tls_verify` is on by default, and it checks two separate things: that the certificate chains to a
trusted CA, **and** that it covers the address or hostname you dialled. The second is the one that
is easy to omit and impossible to notice — with a private CA that signs a whole cluster, chain-only
verification accepts node B's certificate for node A and reports success. Connecting by IP therefore
needs an `IP:` entry in the certificate's `subjectAltName`; an address is never matched against a
`DNS:` entry.

Turning verification off warns, once, through `warnings.warn`:

```python
eng = OrderbookEngine(host="10.0.0.1", port=9090, tls=True, tls_verify=False)
# UserWarning: tls_verify=False - the connection is encrypted against a passive observer and
# unprotected against a man in the middle, which is the half a shared secret already covers
```

Failures raise **`OrderbookTlsError`**, which is deliberately neither an `OrderbookError` nor an
`OSError`. In pool mode the retry paths catch both — and `ssl.SSLError` *is* an `OSError` — so a
certificate that does not verify would otherwise be retried against every node in the mesh, each
failing identically because the cause is your configuration, and the report would read
`No primary available` instead of `certificate verify failed`. A peer that drops mid-handshake stays
an `OSError` and stays retryable.

Four configurations are refused at construction, before any socket exists, because each one
describes a caller who believes the connection is protected in a way it is not:

| Passed | Refusal |
|---|---|
| `tls_ca_file=...` without `tls=True` | it verifies nothing and the connection would be plain text |
| `tls_verify=False` without `tls=True` | there is no certificate to decline to check |
| `tls_ca_file=...` with `tls_verify=False` | a trust anchor nothing consults |
| `tls=True` with `data_dir=...` | local mode opens no socket |

**If you forget `tls=True` against a TLS port, the connection hangs until `timeout` and then
raises.** Not a defect and not fixable: this protocol has the server speak first, so your client
waits for the banner while the server waits for a ClientHello, and until a byte arrives the server
cannot tell a plaintext client from a slow one. The opposite mistake — `tls=True` against a
plaintext port — fails immediately with `wrong version number`, because the banner arrives where a
ServerHello was expected.

#### engine.close()

Shut down / disconnect. Called automatically by context manager.

### OrderbookRow

```python
@dataclass
class OrderbookRow:
    timestamp_ns: int
    price: int
    quantity: int
    order_count: int
    side: str            # "bid" or "ask"
    level: int
    sequence_number: int = 0   # per-origin sequence of the update; 0 = unknown

    @property
    def price_float(self) -> float: ...
```

### AggValue

```python
@dataclass
class AggValue:
    name: str
    value: Optional[int]   # None when the server reported NULL
    scale: int

    @property
    def real(self) -> Optional[float]: ...   # value / scale
    @property
    def is_empty(self) -> bool: ...          # value is None
```

`value` is scaled by the server: × 10⁶ for `VWAP` and `MID_PRICE`, × 10⁹ for `IMBALANCE`, raw for
everything else. Prefer `real` unless you want the integer. `value` is `None` when there was nothing
to aggregate — a spread on a one-sided book is absent, not zero.

### OrderbookError

```python
from orderbook_engine import OrderbookError

try:
    engine.query("INVALID SQL")
except OrderbookError as e:
    print(e.status, str(e))
```

## Benchmark

```bash
# Local mode
python python/benchmark.py --rows 500000 --symbols 10

# TCP mode
python python/benchmark.py --mode tcp --host 127.0.0.1 --port 5555

# All options
python python/benchmark.py --help
```


## Multi-Master Support

The Python client supports multi-master clusters with automatic peer discovery and round-robin write distribution.

### Connecting to a Multi-Master Cluster

```python
from orderbook_engine import OrderbookEngine

# Connect to multiple MM nodes — client auto-detects multi-master mode
engine = OrderbookEngine(
    hosts=["10.0.0.1:5555", "10.0.0.2:5555", "10.0.0.3:5555"],
    timeout=10.0,
)

# Writes are automatically distributed across all MM nodes (round-robin)
engine.insert("BTC-USD", "BINANCE", "bid",
              prices=[6_500_000], qtys=[150])

# Reads can go to any node
rows = engine.query_all("BTC-USD", "BINANCE")
```

### Multi-Master API

#### engine.mm_peers() → List[dict]

Query the list of known multi-master peers.

```python
peers = engine.mm_peers()
for peer in peers:
    print(f"Node {peer['node_id']}: {peer['address']} "
          f"status={peer['status']} queued={peer['send_queue_bytes']}B")
```

Returns a list of dicts with keys:
- `node_id` (int) — peer node identifier
- `address` (str) — replication address (host:port)
- `status` (str) — `"connected"` or `"disconnected"`, the state of the link to that peer. Earlier
  releases of this page documented `"active"`, `"joining"` and `"leaving"` here; those belong to the
  peer registry in etcd and this column has never carried them, so a test for `"active"` is a test
  for a value the server does not send (#118). And of those three the registry only ever holds
  `"active"` — the two methods that looked as though they could set the others wrote nothing and
  are deleted (#134)
- `hlc_timestamp` (str) — last known HLC timestamp
- `send_queue_bytes` (int) — bytes this node currently has queued to send to that peer. Zero on a
  healthy link, and still zero through the few megabytes the sender's socket buffer absorbs, so it
  is a backpressure signal rather than a measure of how far behind the peer is. **This key was
  called `lag_bytes` before #118**; the value is unchanged and only the name was wrong. A client
  written against the old name gets a `KeyError` rather than a silently different number, because
  this method builds its dicts from the server's own header row

**How far behind a mesh peer is** is not in this answer and deliberately so: the mesh's honest lag
is in **records**, from the per-origin sequence vectors, and it is a metric —
`ob_mm_replication_lag_records`, read beside `ob_mm_peers_position_unknown`. A number an operator
has to read by hand cannot be alerted on.

#### engine.mm_conflicts(limit=100) → List[dict]

Query the conflict log (last N resolved conflicts).

```python
conflicts = engine.mm_conflicts(limit=50)
for c in conflicts:
    print(f"{c['symbol']}@{c['exchange']} {c['side']} price={c['price']} "
          f"→ {c['result']}")
```

Returns a list of dicts with keys:
- `timestamp` (int) — when the conflict was detected
- `symbol`, `exchange`, `side`, `price` — the conflicting key
- `local_hlc`, `remote_hlc` — HLC timestamps of both sides
- `local_origin`, `remote_origin` (int) — node IDs
- `result` (str) — "local_wins" or "remote_wins"

#### engine.status() → dict (extended)

In multi-master mode, `status()` includes a `multi_master` key:

```python
st = engine.status()
if "multi_master" in st:
    mm = st["multi_master"]
    print(f"Node ID: {mm['node_id']}")
    print(f"Peers: {mm['connected_peers']}/{mm['peer_count']}")
    print(f"Conflicts: {mm['mm_conflicts_total']}")
    print(f"Anti-entropy runs: {mm['anti_entropy_runs']}")
    print(f"HLC drift: {mm['hlc_drift_ns']}ns")
```

The `multi_master` dict contains:
- `node_id` (int) — this node's identifier
- `peer_count` (int) — total known peers
- `connected_peers` (int) — currently connected peers
- `mm_conflicts_total` (int) — total conflicts resolved
- `anti_entropy_runs` (int) — anti-entropy cycles completed
- `hlc_physical_ns` (int) — current HLC physical time
- `hlc_logical` (int) — current HLC logical counter
- `hlc_drift_ns` (int) — max observed HLC drift

### Client Pool Behavior in Multi-Master Mode

When the `_ClientPool` detects that nodes report `MULTI_MASTER` via the `ROLE` command, it switches to multi-master routing:

- **Writes** are distributed across all connected MM nodes using round-robin
- **Reads** can go to any available node
- If a node becomes unreachable, it is removed from the rotation
- The pool periodically re-checks node roles via health checks
