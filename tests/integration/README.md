# Integration Test Suite

Automated integration tests for orderbook-dbengine. The framework manages the full cluster lifecycle (etcd Docker + 2 ob_tcp_server nodes), runs 9 test categories, and produces a colored console report.

## Prerequisites

1. **Docker** — etcd runs as a Docker container. Verify: `docker info`
2. **Compiled `ob_tcp_server`** — binary at `build/ob_tcp_server`. Build with:
   ```bash
   mkdir -p build && cd build && cmake .. && make -j$(nproc)
   ```
3. **Python with `orderbook_engine`** — install from project root:
   ```bash
   pip install -e .
   ```

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
| cpp_client | `@pytest.mark.cpp_client` | C++ binary ping, insert/query, minsert (skipped if not compiled) |

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

## Console Report

After all tests finish, the framework prints a colored report to stdout:

- **Green** (✓) — passed tests
- **Red** (✗) — failed tests with error messages
- **Yellow** (⚠) — skipped tests

The report includes per-category sections, total passed/failed/skipped counts, execution time, and environment info (server version, ports, paths). Failover tests report measured failover time; stress tests report throughput (levels/sec) and error count.

## C++ Client Tests

The `test_cpp_client.py` module runs the `ob_integration_test` binary (built by CMake with `-DOB_BUILD_TESTS=ON`). If the binary is not found in `build/tests/`, all C++ tests are automatically skipped with an informational message — no error is raised.
