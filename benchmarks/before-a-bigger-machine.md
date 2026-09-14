# Before booting a bigger machine for the comparative benchmark

**This is preparation, not results.** The comparative numbers this repository publishes are in
[`benchmarks/README.md`](README.md), measured on the development machine named there. This page is
the survey that has to happen *before* the same four numbers are taken on a larger box, so that what
gets published is a number about the engine rather than a number about its defaults.

It is a read-only survey: nothing in it changes the engine, and every claim in it was checked
against the tree rather than remembered.


## The finding that matters most: the engine does not size itself to the machine at all

`grep -rn 'hardware_concurrency\|sysconf(_SC_NPROCESSORS' src include` → **zero hits.** Every
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
| `--ring-size`, `--no-sqpoll`, `--sqpoll-idle-ms` | io_uring only, and that transport is compiled but not run by CI (#108). If the AWS run uses it, it is the first time anything does |
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
  was bought; either use local NVMe (`c7gd`, `i4i`) or state the provisioned IOPS and throughput.
- **No Docker.** The engine does not use it and a container between the engine and the hardware
  contradicts the point of the engine; native installs per `benchmarks/install_competitors.md`.
- **Publish the in-process number beside the wire number.** The measured loss is protocol-bound:
  **446 219** updates/s in process against **4 012** over the wire on the development machine — 111×,
  all of it round trips (roadmap #39 part two). A faster box multiplies everyone by roughly the same
  factor, so the engine's actual claim lives in the first number and a table with only the second
  one describes a protocol.
- **Expect the gap to widen, not narrow.** ClickHouse parallelises where our hot path is
  per-connection, so more cores should help it more. That is the honest prediction to write down
  *before* the run, so that reading it afterwards is a check rather than a rationalisation.
