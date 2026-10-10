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
`python -m benchmarks.comparative.run --rows 200000 --rounds 12`. **Control floor 1.92%** over
twelve interleaved rounds: this machine does not separate differences smaller than that, so anything
below it is reported as indistinguishable rather than as a win. That floor was **21.2%** on the
machine an earlier table was measured on, which is most of why the harness measures its own.

Amazon EC2 m8a.xlarge, AMD EPYC 9R45, 4 cores, 15.3 GiB, Amazon Elastic Block Store on nvme0n1p1 → nvme0n1, ext4, kernel 7.0.0, Ubuntu 26.04, GCC 15.2, Release, at `4b92718`. Clock: what `/proc/cpuinfo` read at the start, a moment's figure rather than a rated one.
200,000 rows, 50 symbols, 20 levels, seed 7.

| System | Version | Ingest (rows/s) | Time-range query (4000 rows) | Why it is not a like-for-like number |
|---|---|---|---|---|
| **orderbook-dbengine** | 0.1.0 | **480,898** | **0.678 ms** (0.664–0.744) | 64 updates per round trip: there is still no bulk-load path over the wire, so this sends rows/64 requests. The answer is text, read into rows by the Python client's `query_rows()` |
| ClickHouse | 26.9.12.8 | 1,307,364 | 1.342 ms (1.267–1.528) | the whole CSV in one request; the query through clickhouse-driver, with the uncompressed cache on |
| TimescaleDB | 2.30.2 / PG 16.15 | 511,001 | 0.676 ms (0.658–0.881) | `\copy` of the whole CSV; `timescaledb-tune` applied; the query through psycopg, its results in binary |
| kdb+ | — | NOT MEASURED | NOT MEASURED | needs a vendor registration, and whether its free edition's numbers may be published here is a licence question rather than a technical one |

**One of the four comparable pairs is a win, one a tie and two are losses**, classified by the
harness against its own measured floor rather than by inspection:

- **the time-range query** against ClickHouse — **49.5% apart** against a 1.92% floor. A win.
- **the time-range query** against TimescaleDB — **0.4% apart, inside the floor**. Indistinguishable,
  which is the harness's word for it and not a win.
- **ingest** against ClickHouse — **63.2% apart** against a 1.92% floor. A loss.
- **ingest** against TimescaleDB — **5.9% apart** against a 1.92% floor. A loss.

**What the query column measures.** Every figure runs from the request to the answer as rows of
Python ints, and each system is asked through the fastest Python client measured for it: psycopg
with binary results for TimescaleDB, clickhouse-driver for ClickHouse, the client's own
`query_rows()` for this engine (#226, #229). Of the engine's 0.678 ms its reply takes 0.190 ms to
arrive, read and not parsed; the rest is Python turning its text into ints, where psycopg and
clickhouse-driver build their rows from binary in compiled code.

**What changed since the previous table, and what it is safe to say about it.** The previous table,
of 19 September, had this engine losing the time-range query to both, 35.6% and 21.9% apart. That
comparison was unequal four ways, found together (#226):
- the competitors' clocks stopped before they parsed their answers, and the engine's after it;
- the competitors were asked through their slowest Python clients, `psql` and HTTP;
- the engine's adapter parsed its answer the slowest of three ways measured;
- Python's garbage collector fell inside timed calls, every system's.

Each is corrected here. The machine changed too - the m9g.xlarge the previous table ran on has since
been replaced, and this is the host the competitors are installed on - and so did the engine, with segment
format 3 (#219), decoded columns held between queries (#220) and conditions that narrow the read
(#47). So no figure here is compared with that table's.

One row of this dataset is one book level, so rows and levels are the same count in this table. They
are **not** the same count in the paragraph below about the wire, and that difference used to be
published as a ratio.

The ingest column is a limit of the *protocol* rather than of the storage engine: **there is no
bulk-load path over the wire**, so this harness sends a round trip per 64 book updates (#141) while
the SQL systems receive the whole CSV in one request.

It used to say the round trip was the whole of the difference — "446,219 updates/s in process
against 4,012 updates/s through the wire, a factor of 111" — and that was wrong twice. Both numbers
said "updates" while one meant a single level and the other twenty, so a factor of twenty of the
111 was the word. And the remainder is not the round trip. Holding the volume and the client fixed
and changing only the path, at 20,000,000 levels on the m9g.xlarge an earlier table was measured on:

| | wall ns per level | server CPU ns per level |
|---|---|---|
| in process (`bench_engine BM_IngestionThroughputBatched`) | **1014** | **67** |
| the same update over a socket, C++ client | **1059** | 525 |

**Wall-clock throughput is the same to within about 4%.** The protocol costs about eight times the
storage path's CPU per level and almost nothing in throughput, because throughput is bounded by
neither: server CPU per level is flat at 525–530 ns across a tenfold change in volume while wall per
level climbs and then stops. The full series, the caveats and what is still unexplained are in
[`benchmarks/on-a-bigger-machine.md`](benchmarks/on-a-bigger-machine.md).

What that does not excuse is the ingest column itself, and against ClickHouse the gap **still
widens with volume**: at five times the rows (1,000,000, six rounds, floor 0.66%,
[`…-672503ce.md`](benchmarks/comparative/results/2026-10-10-672503ce.md)) ClickHouse loads **3.04×**
what it does at 200,000 and this engine **1.30×**, so 63.2% apart becomes **84.3% apart**.

**At five times the rows this engine is faster than TimescaleDB on ingest** — 624,447 rows/s against
480,363, **23.1% apart against a 0.66% floor** — where at 200,000 rows it is 5.9% slower. This engine
loads **1.30×** what it did and TimescaleDB **0.94×**, so the crossing is mostly this engine gaining
from volume. It is a cross-run comparison, which the in-run floor does not govern.

**And with 20,000 rows an answer the time-range query is ClickHouse's.** In the same run this engine
answered in 4.049 ms against ClickHouse's 1.908, **52.9% apart**, a loss, and TimescaleDB's 4.584,
**11.7% apart**, a win. Its reply took 0.908 ms of the 4.049. The rest is the Python client parsing
20,000 rows of text, where clickhouse-driver decodes ClickHouse's native column blocks in compiled
code. In Python, then, reading the engine's answer costs more than the engine takes to give it, and
the more rows, the more.

### What it costs, which is a different question from who finishes first

Wall-clock on a four-core box conflates "faster per core" with "uses more cores", and this engine's
name contains a claim about cost. Measured over 2,000,000 levels by
[`scripts/measure_cpu_cost.py`](scripts/measure_cpu_cost.py), counting each server's
`utime+stime+cutime+cstime` and each client's own CPU including its live children, in five runs on an
**Amazon EC2 m8a.xlarge** (4 vCPU, AMD EPYC 9R45, Ubuntu 26.04, GCC 15.2, ext4) on **7 October 2026**,
master `96d1dd9`, against ClickHouse 26.9.12.8 and TimescaleDB 2.30.2 on PostgreSQL 16.15, installed
natively and tuned as [`benchmarks/install_competitors.md`](benchmarks/install_competitors.md) says.
Each cell is the range of the five:

| system | wall | levels/s | server CPU | client CPU | **levels per server CPU-second** | server cores |
|---|---|---|---|---|---|---|
| orderbook | 3.149 - 3.223 s | 620,633 - 635,186 | 0.400 - 0.410 s | 3.158 - 3.229 s | **4,878,049 - 5,000,000** | **0.13** |
| clickhouse | 0.357 - 0.410 s | 4,875,006 - 5,601,321 | 0.890 - 1.220 s | 0.026 - 0.028 s | **1,639,344 - 2,247,191** | **2.44 - 2.97** |
| timescaledb | 4.064 - 4.523 s | 442,143 - 492,164 | 3.160 - 3.200 s | 0.043 - 0.063 s | 625,000 - 632,911 | 0.71 - 0.79 |

The engine's server CPU was 0.400 or 0.410 s in every run, which the kernel counts in 10 ms ticks: the
figures stand for 0.395 - 0.415 s, a 2.5% resolution, and its levels per CPU-second for 4.82 - 5.06
million. Its lowest against each competitor's highest, per server CPU-second the engine ingests
**2.17 times** what ClickHouse does and **7.71 times** what TimescaleDB does. This is a different
script from the comparative run above and carries no floor of its own, so it is given as the figures
and their ratio rather than borrowed into the vocabulary that run owns.

**Where it was before.** On 6 October, at `3157ce7`, the same script on the same machine put the
engine's server CPU at 0.290 s in all five runs - 6,896,552 levels per CPU-second, 3.07 times
ClickHouse's best - in segment format 2. Format 3 (#219) spends the difference at a seal, which
searches for each column's smallest encoding: a seal with no encodings to reuse searches all of them,
and in this three-second run each of the 50 symbols seals only a few times, so that cost is nearly
all of it - 37 - 46% more server CPU than format 2, run for run. Over long runs, which reuse what a
symbol's last seal chose, the server spent 3.5 - 7.5% more CPU than format 2 for the same levels and
ingested them faster, with a batch's p999 a sixth of format 2's (#219). On 19 September, on an
m9g.xlarge, the script had put the engine at 2,272,727 against ClickHouse's 1,754,386; it read the
engine's CPU as zero from #151 until #218.

ClickHouse still wins the clock, by spending **2.44 - 2.97 cores** where the engine spends **0.13**:
about twenty times the parallelism for eight to nine times the throughput. TimescaleDB costs **7.7 -
8.0 times** the CPU per level of the engine.

Three things belong beside that rather than after it. ClickHouse is doing **more** work per level -
parsing text and building compressed parts where the engine receives binary frames and appends - so
what this compares is the cost of loading the data each way it is loaded, not of the same work.
**Our client burns eight times what our server burns**: this ingest column measures the harness's
Python as much as the protocol, and a C++ client sends the same updates with a fraction of it - see
`benchmarks/wire_load.cpp` and [`benchmarks/on-a-bigger-machine.md`](benchmarks/on-a-bigger-machine.md).
And the bytes it keeps: by [`scripts/measure_storage.py`](scripts/measure_storage.py) on the same
machine, versions and day, each dataset loaded, flushed and left 90 s for the merges, bytes on disk a
row - the engine's segments, ClickHouse's table as the harness creates it and with five codec sets
an expert might choose, TimescaleDB's hypertable compressed, in one chunk spanning the data:

| dataset | rows | **orderbook** | ClickHouse as installed | ClickHouse, the best of five codec sets | TimescaleDB, compressed |
|---|---|---|---|---|---|
| Binance, diff stream: 10 pairs, 10 minutes | 266,717 | **3.78** | 7.04 | 4.31 - `GCD` before `Delta` or `T64`, `DoubleDelta` on the time, `ZSTD(3)` | 17.29 |
| Binance, top 20 snapshots: the same pairs and minutes | 1,347,400 | **0.53** | 1.17 | 0.87 - `ZSTD(1)`, `DoubleDelta` on the time | 12.45 |
| the comparative dataset: 50 synthetic symbols | 200,000 | **1.89** | 4.23 | 1.87 - `Delta` or `T64`, `DoubleDelta` on the time, `ZSTD(9)` | 7.86 |

On the two recordings of real books the engine keeps **45 - 54%** of what ClickHouse keeps as
installed, and **61 - 88%** of what it keeps under the best of the five codec sets measured for each;
on the comparative dataset's random walks it is level with that best (1.89 against 1.87) and keeps
45% of ClickHouse as installed. These are a seal's bytes: each symbol's data is one segment here, so
nothing merges, and a merge's ZSTD tier takes the recordings to about 2.94 and 0.33 bytes a row (#219).
As the file system allocates them, the comparative dataset's fifty segments of 4,000 rows pay a 4 KiB
block twice each - 3.07 bytes a row against ClickHouse's best 2.09 - where the recordings' larger
segments take 3.99 and 0.61 against 4.47 and 0.95. The figures come with their reading cost: a format-3
read decodes every column format 2 read raw. A segment's decoded columns are then held between
queries (`--decoded-cache-mb`, #220). A query asked again on the recordings takes 0.20 - 0.85 of format
2's time from the page cache on the m9g.xlarge and the m8a.xlarge (medians of three rounds' p50).
A segment's first read takes 1.25 - 1.55 times format 2's (#223).

The engine's share of the query column, its reply against its parse, is **measured in each run** and
stated in its results file rather than subtracted. An earlier version of this page carried a parsing
constant larger than the smallest query median in the same table, because the constant had been
measured on a different machine and written into the report as prose. The one after it said the
parse was in every system's figure, when the clocks put it in the engine's alone (#226).

Full run, with every tuning declaration and every refusal: [`benchmarks/comparative/results/2026-10-10-287a4e15.md`](benchmarks/comparative/results/2026-10-10-287a4e15.md). The write-up of
what the m9g.xlarge the earlier tables ran on found, including the nine defects it exposed and the
prediction registered before it booted: [`benchmarks/on-a-bigger-machine.md`](benchmarks/on-a-bigger-machine.md).
To reproduce it, install the competitors natively first, and the two Python clients the harness
times them through — [`benchmarks/install_competitors.md`](benchmarks/install_competitors.md); nothing
in the harness installs anything, and a containerised competitor would measure the container.
## Features

- **SoA (Struct-of-Arrays) buffer** with seqlock for lock-free concurrent reads
- **Write-Ahead Log (WAL)** with CRC32C checksums and crash recovery
- **Columnar segments on disk** — time-partitioned, every column of a segment in one checksummed
  file, each in whichever of its candidate encodings is smallest for it (segment format 3, #219);
  written whole when a symbol has enough rows or its oldest are ten seconds old, and merged in the
  background into segments of up to 262 144 rows, so their number follows the data rather than
  uptime
- **Aggregation engine** (VWAP, spread, mid-price, imbalance, etc.) with optional AVX2/AVX-512 SIMD,
  reachable over the wire protocol: every result carries its scale factor and distinguishes an empty
  aggregate from a zero
- **SQL-like query language** with time-range filters and aggregations. A `SELECT` answers the
  columns it names, in the order it names them, and the server decodes only the columns needed
  to answer it — including a column a predicate reads and the answer does not carry
- **The live book, on the wire** — `BOOK <symbol> <exchange> [depth]` returns the current levels of
  both sides from the structure the engine updates in place, under one seqlock read, bids first and
  each side in its own order. Distinct from `SELECT`, which reads history and answers with every
  version of a level it has stored: this answers with one row per level. Measured on an m9g.xlarge
  over loopback, ten levels per side is **9.7 µs p50** of which 8.0 µs is the round trip, and the
  marginal cost is 45–53 ns per level. Two of the seven columns are properties of the read rather
  than of a level, so every row carries the same `timestamp_ns` and `sequence_number` — which is
  the number to resume a `SUBSCRIBE` from. Also in the Python client (`book()`) and in `ob_cli`
- **Streaming subscriptions, pushed** — `SUBSCRIBE 'SYM'.'EXCH'` over the wire and the server sends
  rows as they are written, prefixed `PUSH <id>` with all seven columns, whatever select list the
  subscription named: a `PUSH` line has no header, so a narrowed push would change what a field
  means with nothing for a client to check against. A
  bounded queue per subscriber, and a consumer that stops reading is disconnected rather than
  allowed to grow the server's memory. Also available embedded (`Engine::subscribe()`,
  `ob_subscribe()`) and from the Python client (`subscribe()` / `poll()`)
- **TCP server** — connect remotely via telnet/nc, like PostgreSQL or ClickHouse
- **Backup and restore** — `BACKUP` takes a cut at one moment into the server's `--backup-dir`
  (hard links on the data directory's filesystem, a checksummed copy on another), `ob_backup` asks
  for one from cron, and `ob_restore` checks a backup whole before it writes anything and restores
  it into an empty directory. No restore to a moment between two backups yet: see
  [docs/operations.md](docs/operations.md), "Backing up and restoring a node"
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

It needs GCC 12 or newer, or clang 15, and the development packages of liblz4, libzstd, libcurl and
OpenSSL 3; CMake refuses an older compiler, saying so (#213), and CI builds the packages with GCC 12
(#221). On Ubuntu 24.04:
`sudo apt install build-essential cmake pkg-config liblz4-dev libzstd-dev libcurl4-openssl-dev libssl-dev` - what CI
installs, with OpenSSL named rather than taken as libcurl's dependency. On Amazon Linux
2023, whose default compiler is GCC 11:
`sudo dnf install gcc14-c++ cmake lz4-devel libzstd-devel libcurl-devel openssl-devel`, then configure with
`CC=gcc14-gcc CXX=gcc14-g++` in front of the first command.

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
pip install .     # the client, pure Python
# local mode needs the engine's C API library: a release package installs it, or build it -
cmake --build build --target orderbook_shared
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

# Or several updates in one round trip, with an outcome for each of them. Measured on an
# m9g.xlarge: 1,542,355 levels/s at 64 per call against 969,204 one at a time.
from orderbook_engine import BookUpdate
engine.insert_batch([BookUpdate("BTC-USD", "BINANCE", "bid", [6_500_000], [150]),
                     BookUpdate("ETH-USD", "BINANCE", "ask", [3_100_000], [40])])
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
- [Operations](docs/operations.md) - backups, failover, the dashboard and alert rules
- [Upgrading without stopping](docs/upgrading.md)
- [Releasing](docs/releasing.md) - what a release contains, how one is cut and verified; [Changelog](CHANGELOG.md)
- [Benchmarks](benchmarks/README.md)
- [Roadmap](docs/roadmap.md)
- [Repository security](docs/github-security.md)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
