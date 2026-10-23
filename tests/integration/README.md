# Integration Test Suite

Automated integration tests for orderbook-dbengine. The framework manages the clusters' lifecycle (a
native etcd process and the nodes a fixture asks for), reports the tests by the categories
`conftest.py` registers, and produces a colored console report.

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

Every file of this directory, by what it tests. `scripts/check_integration_readme.py` holds the list
to the directory in both directions, in the `docs-integrity` job: the list used to say it was
complete as of #101, and by #281 it named 31 of 93 files.

```
tests/integration/
├── conftest.py                       # ClusterManager, the fixtures, the report plugin, the env gate
├── pytest.ini                        # markers, testpaths, the timeout
├── binance_support.py                # the gate and helpers of the live Binance modules
├── mesh_proxy.py                     # a TCP proxy between two mesh peers, to break a link
├── ob_integration_test.cpp           # the C++ client harness test_cpp_client.py runs
├── README.md                         # this file
│
│   # One node: start, stop, the wire, its clients
├── test_smoke.py                     # the basics, before anything else is worth checking
├── test_startup_refusal.py           # how a node refuses to start (#102)
├── test_shutdown.py                  # a node stops when asked, with a client attached (#106)
├── test_bind_addresses.py            # the address each listener binds, read from the kernel (#203)
├── test_ipv6.py                      # IPv6 on every listener and every dialler of a node
├── test_unix_socket.py               # the Unix socket, through the binary and the clients (#52)
├── test_rings.py                     # a Unix-socket session moved to shared-memory rings (#52)
├── test_io_profile.py                # the boost and eco profiles on a live node (#144, #151)
├── test_io_reactors.py               # more than one client event loop, --io-threads N
├── test_pipelined_answers.py         # every command of one read answered with one send (#146)
├── test_wire_nodelay.py              # a pipelining client pays no delayed-ACK timer (#140)
├── test_reply_attribution.py         # a reply goes to the command that asked for it (#170, #171)
├── test_recv_buffer_ceiling.py       # one session may not hold unbounded unparsed input (#143)
├── test_edge_cases.py                # input the server should refuse
├── test_large_response.py            # responses larger than the socket send buffer
├── test_compression.py               # LZ4 session compression, negotiated and lossless
├── test_cli_tool.py                  # the interactive CLI refuses what it does not understand (#110)
├── test_cpp_client.py                # the native C++ client against a running server
├── test_pool.py                      # the Python client's pool: discovery and routing
├── test_client_batch.py              # insert_batch(): several updates in one round trip (#141)
├── test_capabilities_are_read.py     # every capability announced has something that reads it (#105)
├── test_docs_transcripts.py          # what the documentation shows a server answer, it answers (#40)
├── test_mixed_versions.py            # the previous version and this one, side by side (#56)
│
│   # Writes and what they become
├── test_write_batches.py             # the writes of one read are one batch (#151, #155)
├── test_event_time.py                # a write keeps the time its sender gave it (#105)
├── test_sequence_numbers.py          # sequence numbers, off disk and off the wire
├── test_live_book.py                 # the live book, through the client somebody has (#145)
├── test_replace_side.py              # MREPLACE replaces one side of a book (#233)
├── test_book_huge_pages.py           # books on huge pages (#51)
├── test_pending_ceiling.py           # crossing the pending-row ceiling costs a flush (#137)
├── test_lazy_flush.py                # a tick seals a store when it is due; a crash loses nothing (#165)
├── test_compaction.py                # a symbol's segments merged; a crash in any step keeps each row once
├── test_segment_identity.py          # the same event-time span written twice (#136)
├── test_segment_time_range.py        # a segment's time range is its rows' (#166)
├── test_ttl.py                       # --ttl-hours expires what is older, and nothing else (#163)
├── test_wal_rotation.py              # writes that cross a WAL file boundary (#124)
│
│   # Queries
├── test_aggregations.py              # aggregates over the wire, including refusals
├── test_column_projection.py         # the response carries the columns the query asked for (#139)
├── test_where_conditions.py          # what a WHERE condition answers (#199)
├── test_time_buckets.py              # aggregates over time buckets, server and Python client (#44)
├── test_snapshot_query.py            # what a SNAPSHOT answers (#167, #168)
├── test_scan_pool.py                 # a node reading on the scan pool answers the same bytes (#246)
├── test_subscriptions.py             # pushed streaming subscriptions (#45)
│
│   # Durability, faults and recovery
├── test_crash_recovery.py            # SIGKILL, restart, count the rows
├── test_power_cut.py                 # what an acknowledged write survives when the power goes (#160)
├── test_storage_faults.py            # the disk refusing a WAL write (#54)
├── test_fault_injector.py            # the fault injector's own tests
├── test_death_report.py              # what the harness says when a node dies unasked (#86)
├── test_backup.py                    # a backup on a running server, restored offline (#34)
├── test_pitr.py                      # a restore to a moment after the backup (#34)
│
│   # Replication and failover
├── test_replication.py               # what the primary accepts, the replica ends up holding
├── test_replica_restart.py           # a restarted replica keeps what it holds (#101)
├── test_replica_lineage.py           # a replica promoted after a snapshot gives every row (#197)
├── test_joining_replica.py           # a joining replica does not stand for election (#215)
├── test_read_only.py                 # --read-only refuses writes from the first one (#241)
├── test_failover.py                  # the primary role handed over, and lost unexpectedly
├── test_failover_dead_state.py       # failover on a cluster nobody started politely
├── test_failover_follows_successor.py  # a primary that gives the role up follows its successor (#201)
├── test_failover_storage_faults.py   # the disk refusing the record a promotion writes (#112)
├── test_handover.py                  # a planned FAILOVER loses no acknowledged write (#204)
├── test_coordinator_endpoint_order.py  # which coordinator endpoint the peer registry talks to (#135)
├── test_coordinator_unavailable.py   # a node whose coordinator is unreachable (#54)
├── test_peer_lease_lost.py           # a node whose mesh registration lease is gone (#132, #133)
│
│   # The multi-master mesh
├── test_mm_convergence.py            # three nodes that all accept writes, and converge
├── test_mm_failover.py               # a mesh with a node missing
├── test_mm_restart.py                # what a restarted mesh node holds, and how soon (#179, #180)
├── test_mm_multi_writer.py           # a symbol several nodes write (#184, #185)
├── test_mm_catchup_ceiling.py        # a node that missed more than a catch-up round reads (#178)
├── test_mm_snapshot_bootstrap.py     # a node added to a running mesh bootstraps from a snapshot
├── test_mm_snapshot_many_files.py    # more files than a 16-bit index can name (#176)
├── test_mm_version_vector_scale.py   # a version vector past what one frame carries (#177)
├── test_mm_stats.py                  # STATUS and /metrics on a mesh node
├── test_mesh_link_faults.py          # the link to a peer breaking under a node (#54)
├── test_mesh_storage_faults.py       # the disk refusing the WAL write a peer's record needs (#112)
├── test_mesh_proxy.py                # mesh_proxy.py's own tests
│
│   # Shards
├── test_shard_control_plane.py       # shards that find each other in etcd, and clients (#175)
├── test_shard_migration.py           # a symbol moved between shards with its rows (#196)
├── test_sharded_pool.py              # the Python client's sharded pool on a real shard map (#172)
│
│   # Security
├── test_auth.py                      # challenge-response authentication on a live server (#30)
├── test_access_control.py            # permissions per identity (#31)
├── test_tls.py                       # TLS on the client port, chain and name (#30)
├── test_tls_cluster.py               # TLS on the replication link and the mesh (#30)
├── test_tls_reload.py                # SIGHUP reads the certificate again, on every surface (#238)
│
│   # Observability
├── test_metrics.py                   # /metrics and STATUS: what an operator would page on
├── test_stress.py                    # sustained throughput, concurrency, what breaks under both
│
│   # Opt-in, over the network
├── test_binance_live.py              # live Binance depth data through the engine
├── test_binance_failover_sync.py     # the live feed through a mesh losing and regaining a node
└── test_example_binance_live.py      # examples/binance-live/, without the network (#43)
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
| crash_recovery | `@pytest.mark.crash_recovery` | SIGKILL and restart: the WAL replayed on its own node |
| subscriptions | `@pytest.mark.subscriptions` | Pushed streaming subscriptions over the wire (#45) |
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
| `OB_KEEP_FAILED_NODE_LOGS` | No | A directory. When a test fails, every cluster that saw it copies its nodes' and etcd's logs there on shutdown, under the failed test's id, before the temporary directories go. Both CI integration jobs set it and upload the directory (`failed-node-logs`, `failed-node-logs-tsan`). A green run writes nothing. |

## Console Report

After all tests finish, the framework prints a colored report to stdout:

- **Green** (✓) — passed tests
- **Red** (✗) — failed tests with error messages
- **Yellow** (⚠) — skipped tests

The report includes per-category sections, total passed/failed/skipped counts, execution time, and environment info (server version, ports, paths). Failover tests report measured failover time; stress tests report throughput (levels/sec) and error count.

## Cluster fixtures

Four kinds of cluster fixture, because one shared cluster cannot serve every kind of test:

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


## When the suite says rows are missing but not why

`scripts/mm_harness.py` runs three multi-master nodes with each node's log on disk, kills one in a
loop, and checks exact row counts after every restart. The fixtures here write each node's output to
`node.log` in its data directory, which `shutdown()` removes - so the line where the engine decided what
to send is gone with a passing test, and kept for a failing one only with `OB_KEEP_FAILED_NODE_LOGS` -
and a single outage can pass by luck: that is how roadmap #61 stayed hidden.

```bash
MMH_CYCLES=4 python3 scripts/mm_harness.py     # logs under /tmp/ob_mm_harness
```

It repeats the outage on purpose: #61 recovered on the first reconnect by luck and lost rows from the
second one on. It also counts duplicates, because the first attempt at fixing that defect replaced
missing rows with duplicated ones, and a check that only looks for absent values calls that a pass.
On a hang it dumps thread stacks with `sudo gdb`, which has twice named a lock problem in seconds.
