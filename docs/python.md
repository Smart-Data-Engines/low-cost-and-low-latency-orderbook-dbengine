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
