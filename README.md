# orderbook-dbengine

[![CI](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/workflows/ci.yml/badge.svg)](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/workflows/codeql.yml/badge.svg)](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/workflows/codeql.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)

A purpose-built C++20 database engine for Level 2 orderbook data in high-frequency trading environments. Designed for sub-microsecond update latency and millions of updates per second on a single core.

Built by [Smart Data Engines](https://smartdataengines.com), who build custom database engines for specific hardware and workloads.

> **Deployment notes:** the engine runs natively on the host; there is no containerised deployment path by design. The wire protocol authenticates client sessions, replication links and multi-master peers by challenge-response (`--auth-secret-file`, `--cluster-secret-file`), and **encrypts nothing** — so run it on a trusted network only. See [SECURITY.md](SECURITY.md).

## Features

- **SoA (Struct-of-Arrays) buffer** with seqlock for lock-free concurrent reads
- **Write-Ahead Log (WAL)** with CRC32C checksums and crash recovery
- **Columnar storage** with delta+zigzag price compression and Simple8b volume packing
- **MMAP persistence** with segment-based time partitioning
- **Aggregation engine** (VWAP, spread, mid-price, imbalance, etc.) with optional AVX2/AVX-512 SIMD,
  reachable over the wire protocol: every result carries its scale factor and distinguishes an empty
  aggregate from a zero
- **SQL-like query language** with time-range filters and aggregations
- **Streaming subscriptions, pushed** — `SUBSCRIBE 'SYM'.'EXCH'` over the wire and the server sends
  rows as they are written, prefixed `PUSH <id>` with the same seven columns as a query row. A
  bounded queue per subscriber, and a consumer that stops reading is disconnected rather than
  allowed to grow the server's memory. Also available embedded (`Engine::subscribe()`,
  `ob_subscribe()`) and from the Python client (`subscribe()` / `poll()`)
- **TCP server** — connect remotely via telnet/nc, like PostgreSQL or ClickHouse
- **Multi-master replication** — write to any node, automatic conflict resolution via HLC + LWW
- **C API** for FFI integration (Python, Rust, Go, etc.)
- **Python bindings** — local (ctypes) or remote (TCP), same API

## Quick Start

### Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### Run the interactive CLI

```bash
./build/ob_cli /tmp/my_orderbook
```

```
ob> insert BTC-USD BINANCE bid 6500000 150
ob> insert BTC-USD BINANCE ask 6510000 80
ob> flush
ob> query SELECT * FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999
ob> quit
```

### Run the TCP server

```bash
./build/ob_tcp_server --port 5555 --data-dir /tmp/ob_data
```

Connect with any TCP client:

```bash
$ nc localhost 5555
OK ob_tcp_server v0.1.0
PING
PONG
INSERT BTC-USD BINANCE bid 6500000 150 3
OK
FLUSH
OK
SELECT * FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999
OK
timestamp_ns	price	quantity	order_count	side	level	sequence_number
1700000000000	6500000	150	3	0	0	1

QUIT
```

### Use from Python

```bash
pip install .
```

```python
from orderbook_engine import OrderbookEngine

# Local mode (in-process, ctypes)
engine = OrderbookEngine("/tmp/ob_data")

# Or TCP mode (connect to running ob_tcp_server)
engine = OrderbookEngine(host="192.168.1.10", port=5555)

engine.insert("BTC-USD", "BINANCE", "bid",
              prices=[6_500_000, 6_499_000],
              qtys=[150, 200])
engine.flush()

rows = engine.query_all("BTC-USD", "BINANCE")
for row in rows:
    print(row.price, row.quantity, row.side)

engine.close()
```

### Run a Multi-Master Cluster

Start three nodes that all accept writes with automatic replication:

```bash
# Requires etcd running on localhost:2379
# Node 1
./build/ob_tcp_server --port 5555 --data-dir /tmp/mm1 \
  --coordinator-endpoints http://127.0.0.1:2379 --node-id mm1 \
  --multi-master --mm-node-id 1 --mm-replication-port 6001 --replication-port 6001

# Node 2
./build/ob_tcp_server --port 5556 --data-dir /tmp/mm2 \
  --coordinator-endpoints http://127.0.0.1:2379 --node-id mm2 \
  --multi-master --mm-node-id 2 --mm-replication-port 6002 --replication-port 6002

# Node 3
./build/ob_tcp_server --port 5557 --data-dir /tmp/mm3 \
  --coordinator-endpoints http://127.0.0.1:2379 --node-id mm3 \
  --multi-master --mm-node-id 3 --mm-replication-port 6003 --replication-port 6003
```

Write to any node — data replicates automatically:

```python
from orderbook_engine import OrderbookEngine

engine = OrderbookEngine(hosts=["localhost:5555", "localhost:5556", "localhost:5557"])
engine.insert("BTC-USD", "BINANCE", "bid", prices=[6_500_000], qtys=[150])
engine.close()
```

### Run tests

```bash
# -j1 is required: network tests bind fixed ports and fail under parallel execution.
ctest --test-dir build --output-on-failure -j1
```

510 tests (GTest + RapidCheck), roughly 6 minutes. Integration tests additionally need a native
`etcd` binary — see [tests/integration/README.md](tests/integration/README.md).

### Run benchmarks

```bash
# C++ (native)
./build/benchmarks/bench_engine

# Python
python python/benchmark.py
python python/benchmark.py --mode tcp --host 127.0.0.1 --port 5555
```

## Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `OB_BUILD_TESTS` | ON | Build tests and fetch gtest/rapidcheck |
| `OB_ENABLE_AVX2` | OFF | Enable AVX2 SIMD for aggregation |
| `OB_ENABLE_AVX512` | OFF | Enable AVX-512 SIMD for aggregation |
| `OB_ENABLE_COVERAGE` | OFF | Enable gcov/llvm-cov instrumentation |
| `OB_ENABLE_ASAN` | OFF | AddressSanitizer + UndefinedBehaviorSanitizer. Build in a separate tree |
| `OB_ENABLE_TSAN` | OFF | ThreadSanitizer. Cannot be combined with `OB_ENABLE_ASAN` |

## Project Structure

```
include/orderbook/     C++ headers (public API)
src/                   Implementation files
tests/                 Unit + property-based tests (510 tests)
benchmarks/            Google Benchmark suite
tools/                 CLI tool (ob_cli) and TCP server (ob_tcp_server)
python/                Python bindings and benchmark script
docs/                  Documentation
```

## Documentation

See the [docs/](docs/) directory:

- [Architecture Overview](docs/architecture.md)
- [CLI Reference](docs/cli.md)
- [Query Language](docs/query-language.md)
- [Python Bindings](docs/python.md)
- [C API Reference](docs/c-api.md)
- [Storage Format](docs/storage.md)
- [Benchmarks](benchmarks/README.md)
- [Roadmap](docs/roadmap.md)
- [Repository security](docs/github-security.md)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
