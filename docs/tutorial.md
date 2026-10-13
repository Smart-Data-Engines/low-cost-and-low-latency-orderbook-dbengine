# Tutorial

Five minutes from a release to a query. The steps:
- install the server;
- start a node;
- write two levels of a book, flush them and read them back, over raw TCP and then from Python.

The two exchanges below are replayed against the server on every change to this repository
(`tests/integration/test_docs_transcripts.py`). What this page shows is what the server answers.

## 1. Install

The packages of a release are on its [GitHub release page](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/releases/tag/v0.1.0),
for x86-64 and ARM64, with a `SHA256SUMS` file. On Ubuntu, x86-64:

```bash
curl -LO https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/releases/download/v0.1.0/orderbook-dbengine_0.1.0_amd64.deb
curl -LO https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/releases/download/v0.1.0/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
sudo apt install ./orderbook-dbengine_0.1.0_amd64.deb
```

The other packages of the release are installed the same way:
- **ARM64:** `orderbook-dbengine_0.1.0_arm64.deb`;
- **Fedora, RHEL and Amazon Linux:** `orderbook-dbengine-0.1.0-1.x86_64.rpm` or the `aarch64` one,
  with `sudo dnf install ./<file>`.

The packages need glibc 2.34 or newer. On anything else, build from source as the
[README](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine#build)
describes.

The package installs `/usr/bin/ob_tcp_server` and a service, `ob_tcp_server`, which it does not
start. This tutorial runs a node of its own instead.

## 2. Start a node

```bash
ob_tcp_server --port 9090 --data-dir ~/ob-tutorial
```

It runs in the foreground and logs to the terminal. The data directory is created if it is not
there, and it holds everything the node writes. Stop the node with Ctrl-C; a restart on the same
directory comes back with the data.

## 3. Talk to it over TCP

In a second terminal, `nc localhost 9090`. Below:
- `S:` is a line from the node, and `S:` with nothing after it is an empty line;
- `C:` is a line you type, without the `C: `.

<!-- transcript: checked -->
```
S: OK ob_tcp_server v<version>
S:
C: INSERT BTC-USD BINANCE bid 6500000 150 3 1700000000000000000
S: OK
S:
C: INSERT BTC-USD BINANCE ask 6500100 80 2 1700000000000000000
S: OK
S:
C: FLUSH
S: OK
S:
C: SELECT price, quantity, side FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999
S: OK
S: price	quantity	side
S: 6500000	150	0
S: 6500100	80	1
S:
C: QUIT
```

Each `INSERT` is one side of the book: symbol, exchange, side, price, quantity, the number of
orders at that price, and the time the update happened, in nanoseconds since the epoch. Prices and
quantities are integers, so a price of 65 000.00 is written in cents. Leave the time out and the
node stamps arrival time.

`FLUSH` moves what was written into the column store, which is what `SELECT` reads. A node also
flushes on its own, every 100 ms by default. In a row, `side` is 0 for a bid and 1 for an ask.

Every answer ends where its first word says. `OK` runs to an empty line and `ERR` is one line. The
[wire protocol](protocol.md) has the rest.

## 4. From Python

The client is pure Python. It will be on PyPI as `orderbook-dbengine` once v0.1.0 is published
there. Until then, install it from the release:

```bash
pip install https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/releases/download/v0.1.0/orderbook_dbengine-0.1.0-py3-none-any.whl
```

<!-- tutorial: python -->
```python
from orderbook_engine import OrderbookEngine

engine = OrderbookEngine(host="127.0.0.1", port=9090)
engine.insert("ETH-USD", "BINANCE", "bid", [310000, 309900], [40, 25],
              timestamp_ns=1700000000000000000)
engine.flush()
answer = engine.query_rows("SELECT price, quantity, level FROM 'ETH-USD'.'BINANCE' "
                           "WHERE timestamp BETWEEN 0 AND 9999999999999999999")
for row in answer.rows:
    print(row)
engine.close()
```

It prints the two levels of the bid side: their price, quantity and position from the best.

<!-- tutorial: python output -->
```
(310000, 40, 0)
(309900, 25, 1)
```

## Where next

- [Query language](query-language.md): time ranges, conditions, aggregations, time buckets,
  snapshots of the book at an instant, and subscriptions.
- [Python client](python.md): batches, pools and sharded clusters.
- [Operations](operations.md): replicas and failover, backups, TLS and authentication, metrics.
- [`examples/binance-live`](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/tree/master/examples/binance-live):
  a live market written into a node and drawn on a dashboard.
