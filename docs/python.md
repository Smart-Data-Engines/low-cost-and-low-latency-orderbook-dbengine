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
    timestamp_ns=None,       # optional, default: now
)
```

Returns the sequence number used.

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

#### engine.ping() → str

Returns `"PONG"`. Useful for connection health checks in TCP mode.

#### engine.status() → dict

Returns server statistics. In TCP mode: `{"mode": "tcp", "sessions": 1, "queries": 5, "inserts": 100}`.

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

    @property
    def price_float(self) -> float: ...
```

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
          f"status={peer['status']} lag={peer['lag_bytes']}B")
```

Returns a list of dicts with keys:
- `node_id` (int) — peer node identifier
- `address` (str) — replication address (host:port)
- `status` (str) — "active", "joining", or "leaving"
- `hlc_timestamp` (str) — last known HLC timestamp
- `lag_bytes` (int) — replication lag in bytes

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
