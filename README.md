# orderbook-dbengine

[![CI](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/workflows/ci.yml/badge.svg)](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/workflows/codeql.yml/badge.svg)](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/workflows/codeql.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)

A purpose-built C++20 database engine for Level 2 orderbook data in high-frequency trading environments. Designed for sub-microsecond update latency and millions of updates per second on a single core — and the measured numbers, including the workloads where it **loses**, are in [How it compares](#how-it-compares) rather than in a sentence.

Built by [Smart Data Engines](https://smartdataengines.com), who build custom database engines for specific hardware and workloads.

> **Deployment notes:** the engine runs natively on the host; there is no containerised deployment path by design. The wire protocol authenticates client sessions, replication links and multi-master peers by challenge-response (`--auth-secret-file`, `--cluster-secret-file`). **All three surfaces can be encrypted** with `--tls-client`, `--tls-replication` and `--tls-multi-master` (TLS 1.3, no configurable floor). Both shipped clients verify the certificate chain *and* the name by default; on the node links TLS is **mutual** and cannot be configured otherwise, which is what binds the exchange to the connection and closes the relay that authentication alone cannot. Every surface is off by default. See [SECURITY.md](SECURITY.md).

## How it compares

Measured against natively installed competitors on one machine, by
`python -m benchmarks.comparative.run --rows 200000 --rounds 12`. **Control floor 2.26%** over
twelve interleaved rounds: this machine does not separate differences smaller than that, so anything
below it is reported as indistinguishable rather than as a win. That floor was **21.2%** on the
machine the previous table was measured on, which is most of why this one exists.

Amazon EC2 m9g.xlarge, aarch64 implementer 0x41 part 0xd84 r0p1, 4 cores, 15.3 GiB, Amazon Elastic Block Store on nvme0n1p1 → nvme0n1, xfs, kernel 6.18.48, gcc14-g++ 14.2.1, Release. Clock: not published by this platform.
200,000 rows, 50 symbols, 20 levels, seed 7.

| System | Version | Ingest (rows/s) | Time-range query (4000 rows) | Why it is not a like-for-like number |
|---|---|---|---|---|
| **orderbook-dbengine** | 0.1.0 | **350,509** | **3.02 ms** (2.99–3.08) | one `MINSERT` round trip per book update: there is no bulk-load path over the wire |
| ClickHouse | 26.8.2.7 | 1,151,732 | 1.49 ms (1.38–1.59) | the whole CSV in one request |
| TimescaleDB | 2.30.0 / PG 16.15 | 416,715 | 1.96 ms (1.94–2.03) | `\copy` of the whole CSV; `timescaledb-tune` applied |
| kdb+ | — | NOT MEASURED | NOT MEASURED | needs a vendor registration, and whether its free edition's numbers may be published here is a licence question rather than a technical one |

**All four comparable pairs are losses**, classified by the harness against its own measured floor rather than by
inspection:

- **ingest** against ClickHouse — **69.6% apart** against a 2.26% floor. A loss.
- **ingest** against TimescaleDB — **15.9% apart** against a 2.26% floor. A loss.
- **the time-range query** against ClickHouse — **50.7% apart** against a 2.26% floor. A loss.
- **the time-range query** against TimescaleDB — **35.3% apart** against a 2.26% floor. A loss.

One row of this dataset is one book level, so rows and levels are the same count in this table. They
are **not** the same count in the paragraph below about the wire, and that difference used to be
published as a ratio.

The ingest column is a limit of the *protocol* rather than of the storage engine: **there is no
bulk-load path over the wire**, so this harness sends one round trip per book update while the SQL
systems receive the whole CSV in one request.

It used to say the round trip was the whole of the difference — "446,219 updates/s in process
against 4,012 updates/s through the wire, a factor of 111" — and that was wrong twice. Both numbers
said "updates" while one meant a single level and the other twenty, so a factor of twenty of the
111 was the word. And the remainder is not the round trip. Holding the volume and the client fixed
and changing only the path, at 20,000,000 levels on this machine:

| | wall ns per level | server CPU ns per level |
|---|---|---|
| in process (`bench_engine BM_IngestionThroughputBatched`) | **1014** | **67** |
| the same update over a socket, C++ client | **1059** | 525 |

**Wall-clock throughput is the same to within about 4%.** The protocol costs about eight times the
storage path's CPU per level and almost nothing in throughput, because throughput is bounded by
neither: server CPU per level is flat at 525–530 ns across a tenfold change in volume while wall per
level climbs and then stops. The full series, the caveats and what is still unexplained are in
[`benchmarks/on-a-bigger-machine.md`](benchmarks/on-a-bigger-machine.md).

What that does not excuse is the ingest column itself, and the gap **widens with volume**: at five
times the rows ClickHouse loads 2.76× faster than it did and this engine loads 1.01× — flat, because
a round trip per update does not amortise — so 69.6% apart becomes 89.2% apart.

### What it costs, which is a different question from who finishes first

Wall-clock on a four-core box conflates "faster per core" with "uses more cores", and this engine's
name contains a claim about cost. Measured over 2,000,000 levels by
[`scripts/measure_cpu_cost.py`](scripts/measure_cpu_cost.py), counting each server's
`utime+stime+cutime+cstime` and each client's own CPU including its live children:

| system | wall | levels/s | server CPU | client CPU | **levels per server CPU-second** | server cores |
|---|---|---|---|---|---|---|
| orderbook | 5.574 s | 358,829 | 1.130 s | 4.537 s | **1,769,912** | **0.20** |
| clickhouse | 0.469 s | 4,265,060 | 1.120 s | 0.055 s | **1,785,714** | **2.39** |
| timescaledb | 5.241 s | 381,626 | 4.250 s | 0.083 s | 470,588 | 0.81 |

Per server CPU-second the engine and ClickHouse are **1,769,912 against 1,785,714** — inside every
floor this machine has measured, and one 10 ms clock tick apart, so indistinguishable twice over.
The two figures are given rather than their quotient, because the quotient is smaller than the
resolution of the clock that produced them. ClickHouse wins the
clock by spending **2.39 cores** where the engine spends **0.20**; TimescaleDB costs 3.8× the CPU
per level of either.

Three things belong beside that rather than after it. ClickHouse is doing **more** work per level —
parsing text and building compressed parts where the engine receives binary frames and appends — so
parity per CPU-second is not a flattering result for us. **Our client burns four times what our
server burns**, and a C++ client sending the same 100,000 round trips does it with 0.44 s and
reaches 1,319,261 levels/s, so this ingest column measures the harness's Python as much as the
protocol: the loss to ClickHouse is 4.1× rather than 11.9×, and the smaller number is the honest one
to argue against. And two CPU figures one 10 ms tick apart agree to within the measurement's own
resolution and nothing more should be read into them.

Every figure in the query column also includes Python-side parsing for 4000 rows, identical for all
three systems, because each adapter turns text into tuples. It is **measured in the run** and stated
rather than subtracted — an earlier version of this page carried a parsing constant larger than the
smallest query median in the same table, because the constant had been measured on a different
machine and written into the report as prose.

Full run, with every tuning declaration and every refusal: [`benchmarks/comparative/results/2026-09-19-55fc0e74-5.md`](benchmarks/comparative/results/2026-09-19-55fc0e74-5.md). The write-up of
what this machine found, including the eight defects it exposed and the prediction registered before
it booted: [`benchmarks/on-a-bigger-machine.md`](benchmarks/on-a-bigger-machine.md).
To reproduce it, install the competitors natively first —
[`benchmarks/install_competitors.md`](benchmarks/install_competitors.md); nothing in the harness
installs anything, and a containerised competitor would measure the container.
## Features

- **SoA (Struct-of-Arrays) buffer** with seqlock for lock-free concurrent reads
- **Write-Ahead Log (WAL)** with CRC32C checksums and crash recovery
- **Columnar storage** with delta+zigzag price compression and Simple8b volume packing
- **Columnar segments on disk** — time-partitioned, one file per column, written whole when a
  flush completes
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
- **Fuzzed parsers** — libFuzzer harnesses over wire command parsing, multi-master framing and WAL
  deserialization, with an in-repo corpus and a bounded run on every pull request. Each asserts a
  property rather than merely surviving, and the harnesses themselves are checked by planting
  defects in the parsers: see [fuzz/README.md](fuzz/README.md)
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

GTest and RapidCheck. The suite's size and its measured runtime live in
[docs/roadmap.md](docs/roadmap.md), recorded against the commit that measured them rather than
repeated here — this page said "510 tests" for long enough that the number had halved against
reality. Integration tests additionally need a native `etcd` binary — see
[tests/integration/README.md](tests/integration/README.md).

### Coverage

The required `coverage` job builds the tree with instrumentation, runs the whole suite under it and
**fails below a 58% line floor** — so the green CI badge at the top of this page already asserts
that floor. It also gates three things that are not percentages: that the tree builds with
coverage, that the suite passes under it, and that the instrumentation still **reaches the
libraries**, which is the check that failed silently for as long as the option existed.

Last measured: **66.2% of 14,562 lines (9,640 covered)**, functions 78.5%, branches 36.3%, on
commit `d2929e4` —
[run 34953853236](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/actions/runs/34953853236).
The per-file breakdown is in that job's summary and attached to it as an artifact.

**There is deliberately no coverage badge**, and the reasoning is in
[docs/roadmap.md](docs/roadmap.md) under item 37: a percentage badge needs either a third-party
account with this repository's reports flowing to it or write access for CI to push the number
somewhere, and neither buys anything the floor above does not already assert. The figure is quoted
with its denominator for the same reason: before the instrumentation was fixed this repository could
have published "59.0%" truthfully while measuring 6 of 34 source files.

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
| `OB_BUILD_FUZZERS` | OFF | Build libFuzzer harnesses over the parsers. Requires Clang and `OB_ENABLE_ASAN`; see [fuzz/README.md](fuzz/README.md) |
| `OB_ENABLE_AVX2` | OFF | Enable AVX2 SIMD for aggregation |
| `OB_ENABLE_AVX512` | OFF | Enable AVX-512 SIMD for aggregation |
| `OB_ENABLE_COVERAGE` | OFF | Enable gcov/llvm-cov instrumentation |
| `OB_ENABLE_ASAN` | OFF | AddressSanitizer + UndefinedBehaviorSanitizer. Build in a separate tree |
| `OB_ENABLE_TSAN` | OFF | ThreadSanitizer. Cannot be combined with `OB_ENABLE_ASAN` |

## Project Structure

```
include/orderbook/     C++ headers (public API)
src/                   Implementation files
tests/                 Unit + property-based tests
benchmarks/            Google Benchmark suite
fuzz/                  libFuzzer harnesses over the parsers, with their seed corpus
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
