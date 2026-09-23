# Reproducing a storage fault, one command at a time

`libobfault.so` is an `LD_PRELOAD` shim that makes a chosen `write`, `fsync`, `fdatasync` or
`ftruncate` fail on a chosen file, after a chosen number of successful calls. It exists because the
failures that lose data in production are the ones a healthy machine never produces: `ENOSPC`
mid-record, `EIO` from `fsync`, a short write that leaves a torn record behind. Roadmap #54.

The battery drives it from `tests/integration/test_storage_faults.py`. This page is for the other
case — reproducing one failure by hand, on a node you can then poke at.

## Build

```bash
cmake -S . -B build && cmake --build build -j"$(nproc)" --target obfault ob_tcp_server
# -> build/tests/libobfault.so
```

`test_fault_injector.py` **fails** rather than skips when that file is missing, and so should you
trust nothing without it: a fault-injection test that injects nothing is indistinguishable from an
engine that survives the fault.

## One command

Every knob is an environment variable, so the reproduction is the command line plus the environment.
A full disk on the fourth WAL record, against a node in `/tmp/faulty`:

```bash
LD_PRELOAD=$PWD/build/tests/libobfault.so \
OB_FAULT_PATH=wal_000000.bin \
OB_FAULT_OP=write \
OB_FAULT_ERRNO=ENOSPC \
OB_FAULT_SIZE=136 \
OB_FAULT_SKIP=3 \
OB_FAULT_COUNT=1 \
OB_FAULT_LOG=/tmp/faulty/fault.log \
./build/ob_tcp_server --port 9090 --data-dir /tmp/faulty --fsync-policy every
```

Then, from another shell:

```bash
printf 'INSERT SYM EX bid 100 1 1\n' | nc 127.0.0.1 9090     # repeat four times
cat /tmp/faulty/fault.log
# obfault op=write fd=7 path=/tmp/faulty/wal_000000.bin arg=136 seen=3 action=fail errno=28
```

**The log is the point.** An injector that matched nothing looks exactly like code that survives the
fault, so `OB_FAULT_PATH` is required — an empty one disarms the shim completely — and the log is
what says the injection happened. Read it before you believe anything about the run.

## The knobs

| Variable | Meaning |
|---|---|
| `OB_FAULT_PATH` | substring the file's path must contain. **Required**; empty disarms everything |
| `OB_FAULT_OP` | `write`, `fsync`, `fdatasync` or `ftruncate` |
| `OB_FAULT_ERRNO` | which errno to report. The shim knows exactly three — `ENOSPC`, `EIO` and `EDQUOT` — and reads anything else, including a typo, as `EIO` |
| `OB_FAULT_SIZE` | only calls whose byte count equals this. `-1` (default) means any |
| `OB_FAULT_SKIP` | let this many matching calls succeed first |
| `OB_FAULT_COUNT` | fail this many, then let the rest through. Default: all of them |
| `OB_FAULT_SHORT` | `write`: return this many bytes **and write them**, instead of failing |
| `OB_FAULT_SHORT_THEN_FAIL` | `1`: after the short write, fail the caller's retry |
| `OB_FAULT_DELAY_MS` | make the chosen call **slow instead of failed**: sleep this long, then do it for real. `write`, `fsync` and `fdatasync` |
| `OB_FAULT_LOG` | where the decisions go. Without it, nothing is recorded |

**`OB_FAULT_SIZE` matters more than it looks.** This engine's WAL takes a 136-byte delta record from
the session thread and a 32-byte checkpoint plus a 68-byte version vector from the flush loop, so
"the fourth write" is a different call on every run while "the fourth 136-byte write" is the same one
every time. Requirement 2.2 of the fault-injection spec asks for a *named* injection point, and the
size filter is what makes one available.

**`OB_FAULT_DELAY_MS` holds a window open rather than breaking anything.** The fault it models is
time: a segment write or a sync that takes seconds. That is what exposed #159 - rows written while a
flush writes its segments sit between its drain and its checkpoint, a window of milliseconds on a
healthy disk - and it is the instrument for anything asking whether a writer waits for the flush.
The call still succeeds, so the log line says `action=delay` and the test's premise is that it did.

**`OB_FAULT_SHORT_THEN_FAIL` is one fault, not two.** A short write on its own cannot tear a record
this engine wrote: `WALWriter::write_record()` loops `while (remaining > 0)` and resumes, which is
correct. The shape that strands a WAL tail needs the cut *and* a failed retry, and the retry is a
different size from the record — so the remainder is failed whatever its size, outside the size
filter that named the first call.

## The five failures worth reproducing

| Want | Environment |
|---|---|
| A full disk the client is told about | `OP=write ERRNO=ENOSPC SIZE=136 SKIP=3 COUNT=1` |
| A disk that stays full | `OP=write ERRNO=ENOSPC SIZE=136` (no `COUNT`) |
| A failing `fsync` under `--fsync-policy every` | `OP=fsync ERRNO=EIO` |
| A torn record, and the writes stranded behind it | `OP=write ERRNO=ENOSPC SIZE=136 SKIP=2 COUNT=1 SHORT=20 SHORT_THEN_FAIL=1` |
| A flush tick that takes its time over the segments (#159) | `PATH=price.col OP=write DELAY_MS=2000 COUNT=2` |

The last one needs the flush tick out of the way to be *about* the WAL — a tick moves rows into
segments and writes a checkpoint covering them, and replay skips what the last checkpoint covers. Add
`--flush-interval-ms 3600000`, kill the node with `SIGKILL` rather than stopping it (a clean stop
ends in a checkpoint too), and then count what comes back. Roadmap #126 is what that measurement
found.

## What the shim will not do

- **It does not fail a syscall it cannot attribute to a path.** The path comes from
  `/proc/self/fd/<fd>` at decision time rather than from an intercepted `open`, which costs one
  `readlink` per *matching* call and removes an fd table, variadic mode arguments and a bootstrap
  window in which libc's own initialisation reaches an unresolved interposer.
- **It does not go through `dlsym`.** Pass-through is `syscall(SYS_write, …)` directly, so there is
  no recursion risk and no resolution order to get right — the log itself is written that way, so it
  cannot re-enter the `write` interposer. The cost: these wrappers are not cancellation points the
  way libc's are, which matters to a thread being cancelled and not to this engine, which cancels
  none.
- **It does not survive `exec` into something you did not mean to instrument.** `LD_PRELOAD` is
  inherited, so set it on the one process you are testing rather than exporting it in your shell.
