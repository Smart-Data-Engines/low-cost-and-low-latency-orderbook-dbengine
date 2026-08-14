## What and why

<!-- What does this change do, and what problem does it solve? -->

## How it was verified

- [ ] `cmake --build build` passes with no warnings (`-Werror` is on)
- [ ] `ctest --test-dir build --output-on-failure -j1` is green (`-j1` is mandatory: network tests bind fixed ports)
- [ ] New behaviour is covered by a test
- [ ] Logging added for new functions (`OB_LOG_*`, with context: fd, node_id, epoch, sizes)

## Performance

<!-- Required if this touches the hot path: WAL, SoA buffer, columnar store, codec,
     aggregation, query engine, engine facade. Benchmarks are only meaningful in Release. -->

- [ ] Not applicable
- [ ] `bench_engine` run in Release, no regression against baselines
      (IngestionThroughput ≥ 1.0M/s, UpdateLatency ≤ 5µs, VwapLatency ≤ 1000ns, TimeRangeQuery ≤ 5ms)

Numbers:

## Compatibility

- [ ] No wire protocol change
- [ ] Wire protocol changed — version negotiation and backward compatibility described below

## Notes for the reviewer

<!-- Anything worth a second pair of eyes: tricky edge case, a decision you are unsure about. -->
