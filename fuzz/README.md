# Fuzzing the parsers

Three harnesses, one for each place in this engine that reads bytes it did not produce:

| Harness | Production code it drives | Reached from |
|---|---|---|
| `fuzz_command_parser` | `parse_command`, `parse_minsert` | any client socket, before authentication on the `AUTH` path |
| `fuzz_mm_frames` | `encode_frame`, `parse_frames` | the multi-master mesh, from a peer |
| `fuzz_wal_replay` | `WALReplayer::replay_v2`, `replay_after_checkpoint` | the data directory on startup, after a crash |

A parser refusing malformed input is a **correct** result and never a finding. A finding is a crash,
a sanitizer report, a timeout, or one of the harness's own property assertions failing.

## Build

Opt-in, and it refuses an incomplete request rather than building something that explores nothing:
Clang is required, and so is `OB_ENABLE_ASAN` (which turns on ASan *and* UBSan).

```bash
cmake -S . -B build-fuzz \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_BUILD_TYPE=Debug \
  -DOB_BUILD_FUZZERS=ON -DOB_ENABLE_ASAN=ON -DOB_BUILD_TESTS=OFF
cmake --build build-fuzz -j$(nproc) \
  --target fuzz_command_parser fuzz_mm_frames fuzz_wal_replay
```

`OB_BUILD_FUZZERS=OFF` is the default and changes nothing about the ordinary build.

### Check the instrumentation, not the CMake file

```bash
python3 fuzz/verify_instrumentation.py build-fuzz
```

Coverage instrumentation has to reach the **libraries**, not just `LLVMFuzzerTestOneInput`. It
already failed to once here, in the ordinary sanitizer build (#83): `add_compile_options()` only
affects targets created after the call, so the flags reached the server and the tests and none of
the engine, and every job stayed green. This script reads the compiler's own command lines out of
`compile_commands.json`, because what CMake says and what the compiler received are two different
claims.

## Run the committed seeds

Deterministic, a few seconds, and the half worth running after any parser change:

```bash
for h in command_parser mm_frames wal_replay; do
  ./build-fuzz/fuzz/fuzz_$h fuzz/corpus/$h/*
done
```

## A bounded run — what CI does on every pull request

```bash
./build-fuzz/fuzz/fuzz_command_parser build-fuzz/fuzz-corpus/command_parser \
  fuzz/corpus/command_parser -dict=fuzz/command_parser.dict \
  -max_total_time=60 -timeout=10 -rss_limit_mb=1024 -max_len=65536
```

The first directory is where new inputs are written; the second is the read-only committed corpus.
Generated inputs stay out of the repository on purpose — a corpus that grows with every run stops
being a set of cases somebody chose.

## A real campaign

The CI run is a regression gate, not a campaign. An actual campaign is hours, and wants its own
corpus directory kept between runs:

```bash
mkdir -p /var/tmp/ob-fuzz/command_parser
./build-fuzz/fuzz/fuzz_command_parser /var/tmp/ob-fuzz/command_parser \
  fuzz/corpus/command_parser -dict=fuzz/command_parser.dict \
  -max_len=65536 -timeout=10 -rss_limit_mb=2048 -jobs=4
```

Measured throughput, Intel i3-7100U, Debug with ASan and UBSan, single process: **26,400 exec/s**
for commands, **27,400** for frames, **2,234** for the WAL. The WAL harness is an order of
magnitude slower because every input is written to a file and read back through a real
`WALReplayer` — which is the point of it, and the reason its budget buys fewer executions.

## Reproducing a crash

libFuzzer writes the offending input next to the run and names it in the log. One input, one
command, no fuzzing:

```bash
./build-fuzz/fuzz/fuzz_mm_frames build-fuzz/artifacts/mm_frames-crash-<hash>
```

CI keeps both the logs and the artefacts, on failure as well as success, so a red `fuzz` job is
reproducible from its uploaded `fuzz` artefact without re-running anything.

## What the corpus holds

`fuzz/corpus/command_parser/` is text, one command per file, and the names say which branch each
one is for: every command keyword, plus `bad_side`, `overflow` (numbers past their type),
`trailing_token` (the shape #107 refuses), `short_batch` (a `MINSERT` header promising more levels
than follow) and `binary_bytes` (NUL and non-ASCII where a keyword belongs).
`fuzz/command_parser.dict` gives the mutator the keywords, so it reaches recognised branches
instead of spending its budget rediscovering the word `INSERT`.

`fuzz/corpus/mm_frames/` is binary. A frame is a 4-byte little-endian payload length followed by
that many bytes; the ceiling is `MM_MAX_FRAME_PAYLOAD`, 64 MiB.

| Seed | Declared length | Why |
|---|---|---|
| `zero_length` | 0 | an empty frame is legal |
| `one_frame`, `two_frames` | 150, and 32 then 136 | the ordinary cases, and frame order |
| `short_length` | — (3 bytes total) | a header that is not there yet |
| `valid_then_partial` | 3, then a cut header | the common real case: a read boundary mid-frame |
| `valid_then_invalid` | 3, then `uint32_max` | a refusal *after* frames were already accepted |
| `max_length` | 67,108,864 (exactly 64 MiB) | at the ceiling, so it must not be refused |
| `above_max_length` | 67,108,865 | one byte over, so it must be refused |
| `uint32_max` | 4,294,967,295 | the largest value the field can hold |

`fuzz/corpus/wal_replay/` is binary. A record is a 24-byte header — `sequence_number` u64,
`timestamp_ns` u64, `checksum` u32 (CRC32C of the payload), `payload_len` u16, `record_type` u8,
`version` u8 — followed, when `version == 1`, by 14 more bytes (origin node id u16, HLC 12B), then
`payload_len` payload bytes. Every seed that claims to be valid **has a correct CRC32C**; the ones
that do not are named for it.

| Seed | Why |
|---|---|
| `legacy_delta`, `legacy_epoch` | the 24-byte header, both record types that carry a payload |
| `extended_delta`, `extended_epoch` | the 38-byte header |
| `bad_checksum` | a stored checksum that does not match the payload |
| `truncated_base`, `truncated_extended`, `truncated_payload` | cut inside each of the three structures |
| `oversized_payload_claim` | a header promising 65,535 bytes with none present |
| `rotate_then_tail` | a `ROTATE` record with content after it, which replay must stop at |
| `checkpoint_tail` | one record after a checkpoint |
| `two_checkpoints` | two checkpoints, so the suffix is measured from the **last** one |
| `unknown_version` | `version = 255` |

## What these harnesses check, and what they cannot

Each one asserts a property rather than merely surviving:

- **commands** — a command the parser accepts means the same thing after `format_command` and a
  reparse: same type, and every field the grammar carries compared one by one.
- **frames** — splitting one stream into chunks changes nothing about which frames come out, in
  what order, with what bytes, or whether the stream was refused; a payload survives
  `encode_frame` followed by `parse_frames`; a refusal leaves the buffer untouched; and a
  successful verdict never leaves an over-long frame waiting.
- **WAL** — `replay_after_checkpoint` returns exactly the suffix of `replay_v2` that follows the
  last checkpoint; every payload handed to a callback is the bytes on disk at the offset the
  context names; and the checksum is recomputed independently of the engine's own check.

The limits are worth stating, because each was measured with a deliberately broken parser rather
than reasoned about:

- **These are parser harnesses, not server tests.** Nothing here opens a socket, starts etcd, binds
  a port or touches another process's data directory. Framing bugs that need two live nodes belong
  to the integration battery.
- **Over-acceptance is not what the command harness measures.** A parser that accepts a trailing
  token still round-trips cleanly, because `format_command` does not emit the token it ignored. The
  mechanism that refuses those is the arity table indexed by `CommandType` and the tests that came
  with it (#107).
- **`parse_frames` cannot be shown to accept an over-long frame**, only to refuse one: completing a
  64 MiB payload is past the fuzzer's input limit by three orders of magnitude. The oracle is
  therefore indirect — after a successful verdict, anything still pending must be within the
  ceiling.
- **An unknown WAL header version is read as legacy, not refused.** Measured on `unknown_version`:
  `version = 255` takes the 24-byte path. What stops a future format from being misread into the
  engine is the checksum failing, not a version check — replay stops at the first mismatch.
- **The WAL harness drives one file.** Rotation across files, and the manifest, are covered by the
  C++ suite instead.

## When a harness stops detecting anything

A green fuzz run is evidence only if the assertions can fail. Three of the frame harness's
mutations survived the first time this was measured — the corpus reached the branch and no oracle
looked at it — and the three oracles named above exist because of that. If you change a parser or
a harness, break the parser on purpose and confirm the harness goes red, with one control mutation
that must stay green. A table in which everything dies is reporting a broken suite as thoroughness.
