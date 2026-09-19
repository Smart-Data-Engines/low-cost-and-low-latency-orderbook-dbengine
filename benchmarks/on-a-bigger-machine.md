# On a bigger machine

[`before-a-bigger-machine.md`](before-a-bigger-machine.md) registered what to measure and what to
expect. This page is what the machine said. **Nothing on that page has been edited**, including the
prediction and including the parts this run contradicted: a prediction rewritten after the run is
not a prediction.

The short version is that the machine was worth it, and not for the reason it was asked for. The
noise floor fell from 21.2% to **2.26%** in the published run, and to 0.89% at best — which is what
it was asked for. What it bought was **nine defects**, none of them found by reading code (each was
found by the tree being on a machine it had never been on) and a published claim that turned out to
be wrong in both of its halves.

## The machine

| | |
|---|---|
| Instance | Amazon EC2 **m9g.xlarge**, eu-central-1 |
| CPU | aarch64, ARM implementer `0x41` part `0xd84` r0p1, 4 vCPU |
| Cache | L1d 64 KiB/core, L2 2 MiB/core, **L3 48 MiB shared by all four** (`/sys/devices/system/cpu/cpu0/cache`) |
| Memory | 15,640 MiB, **no swap** |
| Storage | 100 GB gp3, 6000 IOPS, 500 MB/s, **xfs** on `nvme0n1p1` |
| Clock | **not published**: no `cpu MHz` line and no cpufreq driver, so the platform is fixed-performance at the hardware level |
| OS | Amazon Linux 2023, kernel 6.18.48 |
| Compiler | GCC 14.2.1 (`gcc14-g++`), Release |

**It is aarch64, and the prep page asked for x86_64.** That request had exactly one reason: CRC32C
had no hardware path outside x86, so a comparative table measured here would have published the cost
of a missing instruction as a property of the engine. The instruction exists on this CPU — `crc32`
is in the feature flags and has been mandatory since ARMv8.1 — and what was missing was our use of
it. It was added and measured before any comparison ran.

## What the engine did on it, before any competitor

| | measured | spread |
|---|---|---|
| Full C++ suite, Release | **1102 tests, 100% passed** | 113.7 s |
| Full C++ suite, ThreadSanitizer | **1102 tests, 100% passed, zero reports** | 190.9 s |
| Full C++ suite, AddressSanitizer + UBSan | **1102 tests, 100% passed, zero findings** | 1873.1 s |
| Integration battery (pytest, real clusters) | **273 passed, 2 opt-in skips** | 1366.0 s |
| `BM_IngestionThroughput` (one level) | 1072 ns wall, 655 ns CPU | cv 0.82% / **0.11%** |
| `BM_IngestionThroughputBatched` (twenty levels) | 20,284 ns wall, 1336 ns CPU | cv **0.14%** |
| `BM_UpdateLatency` | 10,289.86 ns | cv 0.34% |
| `BM_VwapLatency` | 620.94 ns | cv **0.02%** |
| `BM_TimeRangeQuery/100000` | 1.34 ms | cv 1.04% |

The first four lines are the result worth having most, and they are about correctness rather than
speed: **this engine had never been compiled or run on a weakly-ordered memory model.** Every
seqlock, every atomic and every lock-free path in it had only ever executed on x86, which is TSO — a
missing acquire or release is invisible there and real here. Four full passes, four clean — and the fourth is the one that had to be run rather than assumed.
The three C++ passes drive the apply path with real, aligned `Level` arrays, and that is exactly the
gap **#127** lived in: a pointer into a byte buffer cast to `const Level*`, invisible to
ThreadSanitizer (which does not check alignment) and unreachable from the C++ suite (in which
nothing took a delta **off a socket**) while a mesh frame puts the levels 130 bytes in. The
integration battery is the only thing here that drives real clusters, real sockets, replication and
the multi-master mesh, and it has only ever run on GitHub's x86_64 runners. On this machine: **273
passed, 2 opt-in skips, 22:46**, the same 273 the runners report, zero unexplained node deaths and
nothing in any node's log.

That is evidence and not proof, and the difference is worth stating: a race is probabilistic, and
ThreadSanitizer reasons about synchronisation rather than about the hardware, so a build that is
clean under it can still be relying on x86's ordering in a path no test drove. What can be said is
that the strongest check available was run and reported nothing, which was not true of this engine
before today.

The coefficients of variation are the second result. On the development machine a control experiment
on an **unchanged** function once produced −40.6% in 8 of 8 rounds. Here the same suite's benchmarks
vary by hundredths of a percent.

## The comparative table

ClickHouse **26.8.2.7** and TimescaleDB **2.30.0 on PostgreSQL 16.15** — the same versions as the
previous published run, so the architecture is the only thing that moved. Both installed natively,
no Docker. TimescaleDB is **not installed from a package here** — `rpm -q
timescaledb-2-postgresql-16` reports none — because the vendor's repository had no aarch64 build
to install, so it was built from source at that tag, and its own tuner was built from source and
run as well, so the tuning is still the vendor's decision rather than ours. That is a difference
in the competitor and it is the first reason a second machine is worth having.

One row of this dataset is one book level, so rows and levels are the same count in this section.
They are **not** the same count in the section after it, and that difference was a published claim.

| System | Ingest (rows/s) | Time-range (4000 rows) | VWAP |
|---|---|---|---|
| **orderbook-dbengine** 0.1.0 | **350,509** | **3.02 ms** (2.99–3.08) | **0.014 ms** |
| ClickHouse 26.8.2.7 | 1,151,732 | 1.49 ms (1.38–1.59) | not comparable |
| TimescaleDB 2.30.0 / PG 16.15 | 416,715 | 1.96 ms (1.94–2.03) | not comparable |
| kdb+ | NOT MEASURED | NOT MEASURED | NOT MEASURED |

**Control noise floor: 2.26%**, against 21.2% on the development machine. That is the number this
machine was for, and everything below is a difference the run can now resolve.

The floor is itself a measurement and it moved: **six runs on this box gave 2.31%, 1.41%, 3.06%,
5.37%, 2.26% and 0.89%**. So the honest claim is a range — this machine resolves somewhere between
about 0.9% and 5.4%, and the run being published resolved 2.26% — rather than the best of six.
Every pair below is outside the worst of them as well as the published one.

- **ingest** against ClickHouse — **69.6% apart**. A loss.
- **ingest** against TimescaleDB — **15.9% apart**. A loss.
- **the time-range query** against ClickHouse — **50.7% apart**. A loss.
- **the time-range query** against TimescaleDB — **35.3% apart**. A loss.

Four losses, none inside the floor. The VWAP refusal is unchanged from the previous run, down to the
digits: ours answers `100241.527539` and both SQL systems answer `100235.3567614876…`. It is a
difference in the question — our aggregate runs over the live book — and not in the arithmetic, and
our value is **bit-identical across the two architectures**, which is worth a line on its own,
because floating-point reassociation between compilers and instruction sets is exactly the sort of
thing that would have shown up here.

## Does the gap move with volume? Yes, against us

The same harness at five times the rows, as a separate run and a separate file:

| | 200,000 rows | 1,000,000 rows | |
|---|---|---|---|
| control floor | 2.26% | 0.89% | |
| orderbook ingest | 350,509 | 356,547 | **flat (1.02×)** |
| clickhouse ingest | 1,151,732 | 3,307,113 | **2.87×** better |
| timescaledb ingest | 416,715 | 417,515 | flat (1.00×) |
| orderbook time-range | 3.02 ms | 15.32 ms | **5.07×** for 5× the rows |
| clickhouse time-range | 1.49 ms | 2.44 ms | **1.64×** |
| timescaledb time-range | 1.96 ms | 9.79 ms | **5.01×** |

Two things follow and neither flatters us. **Our ingest does not improve with volume and
ClickHouse's does**, because our wire costs one round trip per update however much data there is
while a single bulk request amortises better the more it carries — so the ingest distance widens
from **69.6%** to **89.2%**. And **our scan is linear**, 5.07× the time for 5× the rows, as is
TimescaleDB's at 5.01×; ClickHouse's 1.64× is its columnar scan and its parallelism, and that is the
gap that grows fastest.

## The factor of 111 was wrong twice

The README said the engine "ingests 446,219 updates/s in process against 4,012 updates/s through
the wire — a factor of 111, and the round trip is all of it". Both halves were wrong, and the second
is the one worth reading.

**The units.** Both numbers said "updates". `BM_IngestionThroughput` applies **one** level per call
and the harness sends **twenty** per round trip, so a factor of twenty of that 111 was the word
meaning two different things. `BM_IngestionThroughputBatched` was added beside the single-level
benchmark — not replacing it, because the one-level shape is the most cache-friendly this engine has
and that is worth knowing — so the two halves are now in one unit.

**"The round trip is all of it."** It is not, and the way to see it is to hold the volume and change
only the path. A C++ client sending the same twenty-level `MINSERT`s to the same server, on the same
storage, at the same one-second flush interval:

| levels | wall | levels/s | server CPU | **server ns/level** | **wall ns/level** |
|---|---|---|---|---|---|
| 2,000,000 | 1.516 s | 1,319,261 | 1.050 s | 525 | 758 |
| 10,000,000 | 10.658 s | 938,262 | 5.300 s | 530 | 1066 |
| 20,000,000 | 21.179 s | 944,332 | 10.500 s | **525** | **1059** |

and the in-process benchmark, at that last volume — 1,000,000 iterations of twenty levels is
20,000,000 rows:

| | wall ns/level | CPU ns/level |
|---|---|---|
| in process (`BM_IngestionThroughputBatched`) | **1014** | **67** |
| over the wire, same volume | **1059** | 525 |

The in-process row is the three-repetition run above (20,284 ns per twenty-level iteration, cv
0.14%). The published report's own invocation of the same benchmark gave 991,008 levels/s, which is
1009 ns per level — 0.5% from this one, and quoted here rather than reconciled away, because two
artefacts in one pull request giving two numbers for one quantity is the shape this page is about.

**Wall-clock throughput is the same to within about 4% whether the levels arrive over a socket or
are applied in process.** The round trip costs about 460 ns of server CPU per level — roughly eight
times what applying the level costs — and costs almost nothing in throughput, because throughput is
bounded by something neither of them is: server CPU per level is **flat at 525–530 ns across a
tenfold change in volume** while wall per level climbs from 758 to ~1060 and then stops, which is
the signature of the storage path rather than of the protocol.

Two caveats, because this is a comparison between two instruments. The in-process benchmark writes
one symbol and the wire probe writes fifty, so they are not the same workload; and its iteration
count is fixed by `->MinTime(2.0)` in the benchmark's own registration, which **overrides**
`--benchmark_min_time`, so the volume could not be varied on that side at all — which is why the
volume series above is on the wire, where it can be.

What this does not say is that the protocol is free. It costs eight times the storage path's CPU per
level, and on a machine with cores to spare that is invisible in the wall clock and very visible in
the bill. Which is the next section.

## What it costs, which is a different question from who finishes first

Wall clock on a four-core box conflates "faster per core" with "uses more cores", and this engine's
name contains a claim about cost. Measured over 2,000,000 levels with
`scripts/measure_cpu_cost.py`, counting each server's `utime+stime+cutime+cstime` and each client's
own CPU including its live children:

| system | wall | levels/s | server CPU | client CPU | **levels per server CPU-second** | server cores |
|---|---|---|---|---|---|---|
| orderbook | 5.574 s | 358,829 | 1.130 s | **4.537 s** | **1,769,912** | **0.20** |
| clickhouse | 0.469 s | 4,265,060 | 1.120 s | 0.055 s | **1,785,714** | **2.39** |
| timescaledb | 5.241 s | 381,626 | 4.250 s | 0.083 s | 470,588 | 0.81 |

Per server CPU-second the engine and ClickHouse are **0.9% apart** — inside every floor this machine
has measured, and one 10 ms clock tick apart, so indistinguishable twice over. ClickHouse wins the
clock by spending **2.39 cores** where the engine spends **0.20**. TimescaleDB costs 3.8× the CPU
per level of either.

Three things belong next to that rather than after it.

ClickHouse is doing **more** work per level — parsing text and building compressed parts where the
engine receives binary frames and appends — so parity per CPU-second is not a flattering result for
us.

**Our client burns four times what our server burns**, and the C++ probe above did the same
2,000,000 levels with **0.44 s** of client CPU against this harness's 4.537. So the ingest column
measures the harness's Python at least as much as it measures the protocol: with an efficient
client the same server reaches 1,319,261 levels/s where the harness reads 358,829. The loss to
ClickHouse is real either way — 4.1× rather than 11.9× — and the smaller number is the honest one
to argue against.

And the two CPU figures being one tick apart is the measurement's resolution, not a coincidence:
`/proc` reports these in 10 ms units, which at 1.13 s is 0.9%. Two numbers that agree to within
their own resolution agree, and nothing more than that should be read into them.

## CRC32C on this architecture

The fold in isolation, per buffer:

| size | table | ARMv8 `crc32c*` | speedup |
|---|---|---|---|
| 64 B | 93.9 ns | 5.9 ns | 16× |
| 112 B (a WAL record) | 193.66 ns | 6.68 ns | 29× |
| 1 KB | 2.14 µs | 40.4 ns | 53× |
| 4 MB | 8.97 ms | 160.5 µs | 56× |

On the ingest path, six interleaved rounds against a control build differing by one line:
**2.4% faster** — per-round ratio median 1.0237, range 1.0166–1.0279, non-overlapping. With the
engine's storage on tmpfs the same comparison does not resolve at all (0.9972, range
0.9895–1.0064), which is the storage ceiling above showing up in a second place.

So **187 ns of isolated saving is worth 25 ns on the operation that contains it**. That gap is the
lesson: the core overlaps the table's dependent-load chain with the rest of `apply_delta`, and
publishing the isolated figure as an ingest improvement would have overstated it eightfold.

## The prediction, against the measurement

| quantity | i3-7100U | predicted before the run | measured, m9g.xlarge | verdict |
|---|---|---|---|---|
| orderbook ingest | 66,072 rows/s | 80,000 – 95,000 | **350,509** | 3.7× above the top |
| ClickHouse ingest | 428,842 rows/s | 650,000 – 850,000 | **1,151,732** | 1.4× above |
| TimescaleDB ingest | 116,438 rows/s | 140,000 – 170,000 | **416,715** | 2.5× above |
| ClickHouse ÷ orderbook | 6.5× | **7.5× – 10×, wider** | **3.3×** | **wrong direction** |
| orderbook `time_range` | 9.32 ms | 6.5 – 8.0 ms | **3.02 ms** | 2.2× better than the best case |
| ClickHouse `time_range` | 5.26 ms | 3.7 – 4.5 ms | **1.49 ms** | 2.5× better |
| TimescaleDB `time_range` | 8.67 ms | 6.0 – 7.5 ms | **1.96 ms** | 3.1× better |
| orderbook in process | 446,219 updates/s | 530,000 – 650,000 | **933,000** | 1.4× above |
| control floor | 21.2% | under 10% | **2.26%** | met, and beaten fourfold |

Eight of nine were too conservative. The ninth is the one to read, because it is the only one whose
**sign** is wrong, and the reason it is wrong is written on the prep page in plain sight.

**The prediction was for an 8 vCPU box. This one has four** — the same count as the development
machine. So the variable that moved was not core count but core *speed* and cache: 48 MiB of L3
against 3 MiB, 2 MiB of L2 per core against 256 KiB, and `BM_IngestionThroughput` at 1072 ns against
2506. The registered reasoning — "our figure is bound by single-connection round-trip latency, which
improves with core speed and not with core count; ClickHouse's insert path parallelises and sizes
itself to the machine; doubling the cores helps it and barely helps us" — is **correct, and it
predicts the opposite of what happened on a machine whose cores did not double**. Our ingest
improved 5.3×; ClickHouse's 2.7×.

And the prediction comes right again as soon as the dataset grows: at 1,000,000 rows the ratio is
**9.3×**, inside the predicted band. So the honest summary is that the mechanism was understood and
applied to the wrong axis, and the run that shows it is the second one rather than the first.

**Two further reasons the old numbers flattered the comparison less than they looked.** The 9.32 ms
time-range figure had a round-to-round range of 8.46–13.28 ms against a 21.2% floor; here the same
workload ranges 2.99–3.08. Part of what reads as a speed-up is the earlier number having been
measured through noise. And the development machine's storage stack is
`vgubuntu-root → nvme0n1p3_crypt → nvme0n1p3 → nvme0n1` — dm-crypt is in the write path, where this
instance is plain xfs on EBS. Some of the ingest gain is the absence of encryption rather than the
presence of a faster core, which is what the `disk_stack` field in the report exists to make
visible.

## What the machine found, in the order it found it

Nothing on this list came from reading code. Each one came from the tree being on a machine it had
never been on.

1. **The tree did not build.** One object out of 312 failed under GCC 14, which neither CI's
   `ubuntu-24.04` runner nor the development machine has — both are on GCC 13, so "GCC ≥ 12" was a
   promise nobody had checked above 13. A `reserve()` whose argument contains a runtime length
   defeats GCC 14's analysis of `push_back`'s growth branch. Fixed by sizing the frame once, which
   is also the shape that does not copy 32 KiB per chunk (#146).
2. **CRC32C ran the table** on a CPU that has the instruction. Added, measured, and the two
   compilers on this architecture disagree about every spelling of the intrinsic except one (#147).
3. **The harness could not describe this machine.** CPU "unknown", clock 0.0, compiler "unknown" —
   three fields read in a way only x86 answers, in the module whose stated purpose is that a reader
   can compare their hardware to ours (#148).
4. **The engine would have been timed against RAM.** `/tmp` is a tmpfs here; the harness put the
   engine's storage there and described the disk under the build directory, while ClickHouse and
   PostgreSQL wrote to their own directories on NVMe. Refused now, before the run (#148).
5. **The report carried another machine's measurements as prose**, including a Python-parsing
   constant of 4.8 ms in a report whose smallest query median was 1.49 ms — a constant declared to
   be *inside* every figure while exceeding the smallest of them. Measured in the run now: 1.435 ms
   (#148).
6. **The published factor of 111 was wrong in both halves** — the section above.
7. **`BM_IngestionThroughput` rewrites one price on a one-level book**, which is the most
   cache-friendly shape this engine has, and it was the source of the headline in-process number. A
   twenty-level benchmark now sits beside it (#148).
8. **Writing the same event-time span twice destroys a symbol's segment** (#136, open). A segment's
   identity is its time range, so a backfill re-run produces a directory already in the index; the
   guard that refuses it was written for #26's flush race and says so, and that premise stopped
   being the only way in when #105 put event time on the wire. Measured: the first write's values
   silently replaced, or — when the second write has fewer rows — **zero rows for that symbol**
   after two acknowledged writes. Filed rather than fixed, because the three candidate answers
   differ in what they cost and one of them changes an on-disk layout the snapshot manifest and
   retention both address.

9. **A writer that hits the pending-row ceiling waits for a flush nothing asks for, and the writer
   is the epoll thread** (#137, open). `MAX_PENDING_ROWS` is 1,000,000 and `apply_delta_impl()`
   blocks there until a flush drains it; the flush runs on `--flush-interval-ms` and nothing
   signals it because a writer is waiting. So the node stops accepting, stops answering, logs
   nothing, and — measured — does not observe `SIGTERM` either: still in the same two futexes
   **273 seconds** after it, with 0.5 s of CPU between both threads, killed with `SIGKILL`. It is a
   slope rather than a cliff — 1,081,417 levels/s at a 1000 ms interval, 254,691 at 5000, 65,930 at
   20,000, roughly inverse — and an earlier draft of this line said 1000 ms was one interval from
   the ceiling, which the sweep contradicted.

One thing worked exactly as designed on a machine it had never run on, and it is worth the line
because the failure it replaced was a `SIGABRT`: a second node on a taken port printed
`Error: bind() failed on port 9191: Address already in use` and exited, which is #102.

## What is not published from this run, and why

- **Two of the four comparative runs.** Four were taken; two are committed. The first two were
  produced by a harness whose in-process sentence compared a CPU-time rate with a wall-clock rate,
  and a report carrying a sentence known to be in the wrong unit is the defect this page is
  largely about. Their floors are quoted above, because that is a fact about the machine rather
  than about the sentence.
- **The single-level in-process figure, as a headline.** Two instruments with materially identical
  timed loops measured `apply_delta` at 646 ns and 2690 ns on the same core, minutes apart; a third
  agreed with the first, byte counts confirmed identical work, and the one hypothesis tested was
  wrong. The twenty-level figure agrees across all three, so that is the one used; the single-level
  benchmark's own numbers are in the table above with both of its columns.
- **kdb+.** Unchanged: the binary and its licence both come from a vendor registration, and whether
  the free edition's terms permit publishing numbers is a licensing question rather than a
  technical one.
- **Any claim about the io_uring transport.** It builds here, as CI requires, and nothing in this
  run executed it.
- **Any per-core comparison against ClickHouse.** Its parallelism was not constrained, so "levels
  per CPU-second" is what was measured and "levels per core" is not.
## Does this need a different machine?

**Not for the engine's own numbers.** The one thing the previous machine could not do was resolve
small differences, and that is fixed: the control floor went from 21.2% to **2.26%** in the
published run and 0.89% at best, the benchmark suite's round-to-round variation is hundredths of a
percent, and every pair in the comparative table is now classified rather than swallowed. Nothing
bigger is needed to publish this table, and a faster box would not make any of these numbers more
trustworthy.

**One more machine is worth having, and it is one machine rather than two.** Two questions are open
and a single box answers both if it is booted twice:

### 1. Architecture is currently a confound, and one of the competitors is not the same build

The published table before this run was x86_64 (i3-7100U); this one is aarch64. Two things moved at
once - the architecture and the machine class - so no difference between the two tables can be
attributed to either. Within one table that does not matter, and the harness refuses cross-run
comparisons for exactly this reason. Three things do make it matter:

- **TimescaleDB is not the same build on the two architectures.** `rpm -q
  timescaledb-2-postgresql-16` reports nothing here, so this run compares against a **source build**
  at that tag where the previous run compared against the vendor's package. That difference is ours
  by necessity rather than by choice, and it is a difference in the competitor.
- **The x86_64 CRC32C path has never been measured on a quiet machine.** #81 measured it at 21.2%
  floor; the ARM path was measured here against a floor between 0.9% and 5.4% and is worth 2.4% on
  the ingest path. Whether the SSE4.2 path is worth the same, more, or less is unmeasured.
- Most prospective readers run x86_64, so a table on comparable x86_64 hardware is the more useful
  artefact regardless of what it says.

### 2. The whole wall-clock loss to ClickHouse is cores-used, and nothing has measured how that scales

Measured here over two million levels: **per server CPU-second the engine and ClickHouse are 0.9%
apart** - inside every floor this machine has measured, and one 10 ms clock tick apart - while
ClickHouse wins the clock by spending **2.39 cores** where the engine spends **0.20**. That is the
whole of the wall-clock difference on this box, and the section above shows that the round trip is
not: at equal volume the engine's wall-clock ingest is within 4% of its own in-process figure.

`grep -rn 'hardware_concurrency\|_SC_NPROCESSORS\|sched_getaffinity' src include` returns
**nothing**: every thread in this engine has a fixed role, and none of them is a pool sized to the
machine. ClickHouse sizes itself to the cores it can see, and `timescaledb-tune` sizes PostgreSQL
to the machine. So the prediction, registered here before any such box exists: **on 8 or 16 cores
the engine's ingest wall clock stays roughly where it is, ClickHouse's grows with the cores, and
the per-CPU-second parity holds.** If per-CPU-second parity *breaks* in our favour on a bigger
box, the engine scales better per unit of work; if it breaks against us, the fixed thread count is
costing something a pool would not. Either answer is worth more than the wall-clock number it
explains.

### The machine, and how to boot it twice

One **x86_64, non-burstable, general-purpose or compute instance with at least 8 vCPU**, same
storage as this one (100 GB gp3, 6000 IOPS, 500 MB/s), Amazon Linux 2023. The `c7i`/`m7i` families
fit and were current when this was written - check the console rather than trusting that sentence.

Then measure it **twice**, and the second boot is the point:

1. **All cores.** This answers question 2 and produces the table a reader on x86_64 wants.
2. **Rebooted with `nr_cpus=4` on the kernel command line.** Four x86_64 cores against four aarch64
   cores, everything else held: same storage class, same OS, same competitor versions - except
   TimescaleDB, which is now the vendor's package, and that difference is named rather than hidden.
   This answers question 1.

**`nr_cpus=4` rather than `taskset`, and the reason is the competitor.** ClickHouse decides its own
thread count from the number of CPUs it can see. Under `taskset` it would still *see* eight and be
confined to four, which measures a misconfigured ClickHouse rather than a four-core one. `nr_cpus=`
caps what the kernel will ever bring up, so every process on the box agrees about how big it is.
(`maxcpus=` only limits what is onlined at boot and permits hotplug afterwards, which is a weaker
guarantee than a measurement wants.)

**If only one boot is possible, take the four-core one.** Question 1 removes a confound from numbers
we already publish; question 2 adds a number we do not yet claim.

**What another machine would not fix**, and this paragraph used to say the opposite of what was
measured. The ingest column is *not* simply a protocol limit: at equal volume the protocol costs
about 4% of throughput, the throughput ceiling is the storage path, and the largest single term in
that column is the harness's own Python client — a C++ client doing the same 100,000 round trips
reaches 1,319,261 levels/s where the harness reads 358,829 for the same 2,000,000 levels. Against
ClickHouse's 4,265,060 at that volume the loss is still 3.2×, so the gap is real and it is smaller
than the table shows. None of those three terms is a hardware problem: a faster box multiplies every
system by its own factor. The work is a bulk-load path over the wire, a client that does not cost
four times the server, and whatever the storage ceiling turns out to be — and the third of those is
not yet understood well enough to name.
