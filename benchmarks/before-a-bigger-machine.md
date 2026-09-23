# Before booting a bigger machine for the comparative benchmark

**This is preparation, not results.** The comparative numbers this repository publishes are in
[`benchmarks/README.md`](README.md), measured on the development machine named there. This page is
the survey that has to happen *before* the same four numbers are taken on a larger box, so that what
gets published is a number about the engine rather than a number about its defaults.

It is a read-only survey: nothing in it changes the engine, and every claim in it was checked
against the tree rather than remembered.

**The run happened. The results are in [`on-a-bigger-machine.md`](on-a-bigger-machine.md)**, on an
`m9g.xlarge` — which is aarch64, where this page asked for x86_64, and the one reason it asked is
answered there. **Nothing below this line has been edited since it was written**, including the
prediction near the end and including the parts the measurement contradicted: a prediction rewritten
after the run is not a prediction, and every one of these was too conservative.


## The finding that matters most: the engine does not size itself to the machine at all

`grep -rn 'hardware_concurrency\|sysconf(_SC_NPROCESSORS' src include` → **zero hits** when this
page was written. (Since #156 `src/machine.cpp` reads how many CPUs the process can use, and since
#158 the default profile gives one client event loop per usable CPU — so the paragraph below
describes the engine this page was written for, whose client loop was one thread whatever the
machine, and the flush, replication and mesh threads keep the fixed roles it lists.) Every
thread in this engine has a fixed role — flush loop, lease loop, monitor loop, io loop, replication,
mesh, snapshot worker — and not one of them is a pool sized to cores. Roadmap #112 counted the
entry points while giving each an exception boundary: **seventeen**, derived from the
`std::thread` constructions rather than from a list somebody wrote, which is worth repeating
because the hand count that preceded it said eleven.

ClickHouse sizes its pools to the machine, and `timescaledb-tune` does the same for PostgreSQL.
So on an 8-vCPU box the comparison is **a fixed-thread engine against two self-tuning ones**. That
is a real property of an engine whose thesis is "built for a known machine", and publishing it
without saying so invites the reader to conclude we are slow when we are unconfigured. Two honest
ways out, and they are not the same:

1. Tune the engine for the box too, from the flags below, and say which values were used.
2. Publish the defaults and state plainly that the engine does not auto-scale while both
   competitors do.

(2) is cheaper and weaker. (1) is the one worth doing, because the numbers it produces are the ones
a reader could reproduce against their own box.

## Flags that can change a benchmark number

Grouped by what they would move. None of them are tuned for anything today; the defaults were
chosen on a two-core laptop.

| Flag | Why it matters on a bigger box |
|------|-------------------------------|
| `--flush-interval-ms` | how often the columnar flush runs; the ingest path's back-pressure comes from `MAX_PENDING_ROWS` ahead of it |
| `--fsync-policy` | `interval` is the default (**not** `every` — `docs/architecture.md` claimed otherwise until #113). On instance-store NVMe versus gp3 this is the single biggest storage knob |
| `--max-sessions` | the benchmark harness opens one connection per client; the protocol round trip is where we lose (111× measured), so client count is a first-class variable |
| `--snapshot-chunk-size` | irrelevant to ingest, relevant if the run includes a bootstrap |
| `--mm-max-catchup-bytes`, `--mm-max-peer-send-buffer` | mesh only; not in the comparative workload |
| `--replication-compress` | not in the comparative workload either |

## Constants that are not flags and would need a code change

| Constant | Value | Note |
|----------|-------|------|
| `Session::kMaxSendBuffer` | 64 MiB | per-session output ceiling; large query responses |
| `ReplicationClient::DEFAULT_BUF_SIZE` | 4096 | read buffer |
| `IoUringServer::BUFFER_SIZE` | 4096 | registered buffer size |
| `MAX_PENDING_ROWS` | 1 000 000 | the ingest path's back-pressure point (`engine.hpp`) |

## Rules for the run itself, from what this repo has already paid for

- **Measure the noise floor first and publish nothing before it.** The harness does this inside the
  run it governs: the last two published runs recorded control floors of **0.2341** and **0.2119**
  (`benchmarks/comparative/results/`), and a difference smaller than the floor is reported as
  indistinguishable rather than as a win. On shared tenancy it can be worse, and if the floor comes
  out at a quarter then a fifteen-percent difference is not a difference.
- **One run produces all four numbers or none.** `benchmarks/` already works this way
  (`2026-09-12-ece487a1` recomputed every number rather than editing one); numbers from two
  machines in one table is the defect that shape prevents.
- **No burstable instance.** `t3`/`t4g` CPU credits run out *during* a sustained ingest, so early
  and late rounds measure different machines. `c7i`/`m7i` for fixed performance.
- **Storage is the other half.** On gp2/gp3 the published number is a number about the volume that
  was bought; either use local NVMe (`i4i`, or any `i`-family instance) or state the provisioned
  IOPS and throughput. Not `c7gd`, which an earlier version of this line suggested: it is Graviton,
  and the section below is why that would measure the wrong thing.
- **No Docker.** The engine does not use it and a container between the engine and the hardware
  contradicts the point of the engine; native installs per `benchmarks/install_competitors.md`.
- **Publish the in-process number beside the wire number.** The measured loss is protocol-bound:
  **446 219** updates/s in process against **4 012** over the wire on the development machine — 111×,
  all of it round trips (roadmap #39 part two). A faster box multiplies everyone by roughly the same
  factor, so the engine's actual claim lives in the first number and a table with only the second
  one describes a protocol.
- **Expect the gap to widen, not narrow.** ClickHouse parallelises where our hot path is
  per-connection, so more cores should help it more. That is the honest prediction to write down
  *before* the run, so that reading it afterwards is a check rather than a rationalisation — and the
  last section of this page writes it down as numbers rather than as a direction.


## The instance: x86_64, fixed performance, storage that is named

**x86_64, not Graviton.** `include/orderbook/crc32c.hpp` guards its hardware path with
`#if (defined(__x86_64__) || defined(_M_X64)) && (defined(__GNUC__) || defined(__clang__))` and
computes the checksum with `_mm_crc32_u64`; there is no ARM path, so on Graviton every WAL record
falls back to the byte-at-a-time table. Roadmap #81 measured exactly that difference when it added
the intrinsic: **361.7 ns → 23.8 ns** per 112-byte record, and **+17.6%** on
`BM_IngestionThroughput` from the one change. A comparative table produced on `c7g`/`c7gd` would be
publishing the cost of a missing intrinsic as a property of the engine. `c7i`, `m7i` or an `i`-family
instance.

**Fixed performance, for the reason already in the rules above** — no `t3`/`t4g`.

**Suggested: 8 vCPU x86_64, either local NVMe or gp3 with the provisioned IOPS and throughput
written into the published table.** Twice the development machine's thread count and a comparable
class of core. Larger is not better here: the engine has no pool sized to cores (the first section
of this page), so above a handful of vCPUs the extra ones are measuring the competitors' tuning.
The harness already records `cpu_model`, `cores`, `mhz`, `ram_mib`, `disk_model`, `disk_stack` and
`filesystem` into its result JSON, so the only fact a human has to supply is the number the volume
was bought with.


## The prediction, registered before the run

Written down first, so that reading the result is a check rather than a rationalisation. Against
the last published run — `benchmarks/comparative/results/2026-09-12-ece487a1.json`, i3-7100U,
4 cores, 2399.989 MHz, 15,896 MiB, control floor **21.2%** over twelve rounds:

| | measured, i3-7100U | predicted, 8 vCPU x86_64 |
|---|---|---|
| orderbook ingest | 66,072 rows/s | **80,000 – 95,000** |
| ClickHouse ingest | 428,842 rows/s | **650,000 – 850,000** |
| TimescaleDB ingest | 116,438 rows/s | **140,000 – 170,000** |
| ClickHouse ÷ orderbook, ingest | 6.5× | **7.5× – 10×**, i.e. wider |
| orderbook `time_range` p50 | 9.32 ms | **6.5 – 8.0 ms** |
| ClickHouse `time_range` p50 | 5.26 ms | **3.7 – 4.5 ms** |
| TimescaleDB `time_range` p50 | 8.67 ms | **6.0 – 7.5 ms** |
| orderbook in process | 446,219 updates/s | **530,000 – 650,000** |
| control floor | 21.2% | **under 10%** |

**Why ingest should widen.** The harness holds one connection and sends one `MINSERT` round trip per
book update, while the SQL systems receive the whole CSV in one request. Our figure is therefore
bound by single-connection round-trip latency, which improves with core speed and not with core
count; ClickHouse's insert path parallelises and sizes itself to the machine. Doubling the cores
helps it and barely helps us. That is a true statement about this engine — it has no bulk-load path
over the wire — and it is already the first line of `ENGINE_LIMITATIONS`.

**Why the query column is the part worth booting a quiet machine for, and it is not the ratio.**
Every query figure contains a measured p50 of about **4.8 ms of Python-side parsing** for 4000 rows,
identical for all three systems. Subtract it and what is left is 4.5 ms for the engine against about
0.46 ms for ClickHouse — but *that subtraction is not publishable from this machine*, because 21.2%
of 9.32 ms is ±2.0 ms, so ClickHouse's remainder is not even separable from zero at this resolution.
The published 1.77× is therefore a ratio between two numbers dominated by a constant neither system
controls. A floor under ten percent is what makes the residuals separable, and that — not a bigger
number for us — is the result worth having.

It also sets up the one reading to be careful with. If the query ratio comes out **wider** on the
new box, that does not mean the engine got relatively slower: it means the Python constant scaled
better than the server-side work, and the table has started to show what was always underneath.
If it comes out **narrower**, the opposite. Either way the number to quote about the engine is the
residual and the floor beside it, not the ratio alone.

**The floor is a gate, not a statistic.** Today's control ratios include a 0.643 and a 1.212 on a
laptop with other work on it. A dedicated instance should bring the floor under ten percent; if it
does not, the instance is noisy and **nothing is published from that run**, because a difference
smaller than the floor is not a difference. The harness measures the floor inside the run it
governs, so this is a decision the run makes for itself rather than one a human makes afterwards.


## The runbook

```bash
# 1. Competitors, natively - no Docker (benchmarks/install_competitors.md)
sudo systemctl start clickhouse-server postgresql@16-main

# 2. The comparative table: one run produces every number in it
python3 -m benchmarks.comparative.run --rows 200000 --symbols 50 --levels 20 --seed 7 --rounds 12

# 3. The in-process number, which is where this engine's own claim lives. Release only
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release && cmake --build build-release -j$(nproc)
./build-release/benchmarks/bench_engine --benchmark_filter=BM_IngestionThroughput
```

**The command is unchanged and its meaning is not.** Google Benchmark's filter is a regex *search*,
so this one name now selects two benchmarks - verified by running `--benchmark_filter=BM_Ingestion`
against a build that has only the first, which listed it. The difference matters because
`BM_IngestionThroughput` applies **one** level per call, which is this engine's most cache-friendly
shape and was the source of the published in-process figure, while the harness above sends
**twenty** levels per round trip: the two halves of the published ratio were in different units.
`BM_IngestionThroughputBatched` is the twenty-level one, added beside the first rather than
replacing it, and this line now runs both.

Both numbers go into the same write-up or neither does: a table carrying only the wire figure
describes a protocol, and one carrying only the in-process figure describes a library nobody can
reach over a socket.
