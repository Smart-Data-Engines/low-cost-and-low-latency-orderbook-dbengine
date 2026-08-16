# Integration Test Suite

Automated integration tests for orderbook-dbengine. The framework manages the full cluster lifecycle (a native etcd process + 2 ob_tcp_server nodes), runs 9 test categories, and produces a colored console report.

## Prerequisites

1. **etcd, installed natively** — the harness starts it as a plain process. No containers: the
   engine has no containerised deployment path, so its tests do not depend on one either.
   ```bash
   ETCD_VER=v3.5.17
   curl -L https://github.com/etcd-io/etcd/releases/download/$ETCD_VER/etcd-$ETCD_VER-linux-amd64.tar.gz | tar xz
   sudo install -m755 etcd-$ETCD_VER-linux-amd64/etcd etcd-$ETCD_VER-linux-amd64/etcdctl /usr/local/bin/
   ```
   Verify: `etcd --version`. A binary outside PATH can be pointed to with `OB_ETCD_BINARY`.
2. **Compiled `ob_tcp_server`** — binary at `build/ob_tcp_server`. Build with:
   ```bash
   mkdir -p build && cd build && cmake .. && make -j$(nproc)
   ```
3. **Python with `orderbook_engine` and the test extras** — install from project root:
   ```bash
   pip install -e ".[test]"
   ```
   The extras matter: without `lz4` the compression module errors out at fixture setup, and without
   `pytest-timeout` the `timeout = 120` in `pytest.ini` is silently ignored. On a PEP 668 system
   (Debian/Ubuntu ≥ 23), do this inside a virtualenv.

## Running Tests

All commands are run from the repository root (`low-cost-and-low-latency-orderbook-dbengine/`).

**Full run:**
```bash
OB_INTEGRATION_TESTS=1 pytest tests/integration/ -v
```

**Filtered by categories (comma-separated):**
```bash
OB_INTEGRATION_TESTS=1 OB_INTEGRATION_FILTER=smoke,replication pytest tests/integration/ -v
```

**With short traceback:**
```bash
OB_INTEGRATION_TESTS=1 pytest tests/integration/ -v --tb=short
```

**Single category via pytest marker:**
```bash
OB_INTEGRATION_TESTS=1 pytest tests/integration/ -m smoke -v
```

## Directory Structure

```
tests/integration/
├── conftest.py              # ClusterManager, fixtures, report plugin, env gate
├── pytest.ini               # Markers, testpaths, timeout config
├── test_smoke.py            # Basic single-node operations
├── test_replication.py      # WAL replication primary → replica
├── test_failover.py         # Automatic failover after primary kill
├── test_compression.py      # LZ4 compression negotiation and data integrity
├── test_stress.py           # Sustained throughput and concurrent read/write
├── test_edge_cases.py       # Invalid inputs, oversized lines, read-only writes
├── test_metrics.py          # Prometheus /metrics endpoint and STATUS command
├── test_pool.py             # _ClientPool discovery, routing, failover
├── test_cpp_client.py       # Native C++ client binary (optional)
├── ob_integration_test.cpp  # C++ test binary source (compiled by CMake)
└── README.md                # This file
```

## Test Categories

| Category | Marker | Description |
|---|---|---|
| smoke | `@pytest.mark.smoke` | PING, INSERT/query roundtrip, STATUS fields, ROLE check |
| replication | `@pytest.mark.replication` | WAL streaming, bulk replication lag, data consistency |
| failover | `@pytest.mark.failover` | Primary kill → replica promotion, data survival, timing |
| compression | `@pytest.mark.compression` | LZ4 negotiation, compressed data integrity, bulk insert |
| stress | `@pytest.mark.stress` | 30s sustained throughput, concurrent R/W, large MINSERT |
| edge_cases | `@pytest.mark.edge_cases` | Nonexistent symbol, oversized line, malformed INSERT, read-only write |
| metrics | `@pytest.mark.metrics` | Prometheus text exposition, STATUS fields on primary/replica |
| pool | `@pytest.mark.pool` | Pool primary discovery, write routing, failover re-discovery |
| cpp_client | `@pytest.mark.cpp_client` | C++ binary ping, insert/query, minsert, aggregates (skipped if not compiled) |
| aggregations | `@pytest.mark.aggregations` | Aggregates over the wire: values, scale factors, NULL for empty, refusals |
| large_response | `@pytest.mark.large_response` | Responses above the socket send buffer, slow readers, clients vanishing mid-response |
| multi_master | `@pytest.mark.multi_master` | Three-node mesh: convergence, LWW, node loss and rejoin (own cluster) |
| binance | `@pytest.mark.binance` | Live Binance depth feed — **opt-in**, see below |

## Adding New Tests

1. Create `tests/integration/test_<category>.py`
2. Add the marker at module level:
   ```python
   import pytest
   pytestmark = pytest.mark.<category>
   ```
3. (Optional) Register the marker in `pytest.ini`

pytest auto-discovers `test_*.py` files — no registry or config changes required.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OB_INTEGRATION_TESTS` | Yes | Set to `1` to enable tests. Without it, all tests are skipped. |
| `OB_INTEGRATION_FILTER` | No | Comma-separated category names (e.g. `smoke,replication`). Runs only matching categories. Empty = run all. |
| `OB_BINANCE_TESTS` | No | Set to `1` to run the live Binance modules. Without it they hard-skip: a third-party exchange being unreachable must never fail this suite. |
| `OB_STRESS_SECONDS` | No | Duration of the sustained-load window. Defaults to `5`; the documented long run is `30`. The duration used is printed in the report. |

## Console Report

After all tests finish, the framework prints a colored report to stdout:

- **Green** (✓) — passed tests
- **Red** (✗) — failed tests with error messages
- **Yellow** (⚠) — skipped tests

The report includes per-category sections, total passed/failed/skipped counts, execution time, and environment info (server version, ports, paths). Failover tests report measured failover time; stress tests report throughput (levels/sec) and error count.

## Cluster fixtures

Three cluster fixtures, because one shared cluster cannot serve every kind of test:

| Fixture | Scope | For |
|---|---|---|
| `cluster` | session | Ordinary tests. Shared, so nothing here may kill a node or push half a million rows |
| `heavy_cluster` | module | Load modules (`stress`, `large_response`). Half a million rows leave the replica replaying a backlog, and later modules then fail on timeouts that have nothing to do with them |
| `failover_cluster` / `healthy_cluster` | module / function | Tests that kill nodes or move the primary. Restoring the shared cluster is not enough: lease TTLs and the election cooldown keep roles moving after any single check says they have settled |
| `mm_cluster` / `healthy_mm_cluster` | module / function | Three-node multi-master mesh. A different topology, not a variation on primary/replica |

`healthy_cluster` and `healthy_mm_cluster` restart dead nodes on teardown and refuse to finish unless
the topology is back, because a half-restored cluster makes the next test's failure point at the wrong
code.

## Live Binance Tests

`test_binance_live.py` and `test_binance_failover_sync.py` stream real BTC/USDT depth data through the
engine. They are opt-in (`OB_BINANCE_TESTS=1`) and skip at module level with a reason naming the
missing precondition: the opt-in itself, the `websockets` package, or reachability of
`stream.binance.com:9443`.

They exist because synthetic data cannot check what real data can: that live prices survive the round
trip at the right scale, that both sides of a real book arrive, and that a node rejoining mid-feed ends
up agreeing with the node that stayed up.

## C++ Client Tests

The `test_cpp_client.py` module runs the `ob_integration_test` binary (built by CMake with `-DOB_BUILD_TESTS=ON`). If the binary is not found in `build/tests/`, all C++ tests are automatically skipped with an informational message — no error is raised.
