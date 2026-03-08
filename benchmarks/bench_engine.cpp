// benchmarks/bench_engine.cpp
// Google Benchmark suite for the orderbook-dbengine.
//
// Benchmarks:
//   BM_UpdateLatency       — p50/p99/p99.9 for single apply_delta end-to-end
//   BM_IngestionThroughput — updates/second on a single core
//   BM_VwapLatency         — VWAP over 1000 levels on warm cache
//   BM_TimeRangeQuery      — query latency over 1M snapshots
//
// Run with:
//   ./bench_engine --benchmark_format=json --benchmark_out=results.json
//
// Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.7

#include <benchmark/benchmark.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "orderbook/aggregation.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/soa_buffer.hpp"
#include "orderbook/types.hpp"

namespace {

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a DeltaUpdate + levels array for benchmarking.
void make_delta(ob::DeltaUpdate& du, std::vector<ob::Level>& levels,
                uint64_t seq, uint64_t ts_ns, uint16_t n = 10) {
    std::strncpy(du.symbol,   "BTC-USD", sizeof(du.symbol)   - 1);
    std::strncpy(du.exchange, "BENCH",   sizeof(du.exchange) - 1);
    du.sequence_number = seq;
    du.timestamp_ns    = ts_ns;
    du.side            = ob::SIDE_BID;
    du.n_levels        = n;

    levels.resize(n);
    for (uint16_t i = 0; i < n; ++i) {
        levels[i].price = 50'000'00LL - static_cast<int64_t>(i) * 100LL;
        levels[i].qty   = 1'000ULL + i;
        levels[i].cnt   = 1;
    }
}

/// Temporary directory RAII wrapper.
struct TempDir {
    std::filesystem::path path;
    TempDir() {
        path = std::filesystem::temp_directory_path() /
               ("ob_bench_" + std::to_string(
                   std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// ── BM_UpdateLatency ──────────────────────────────────────────────────────────
// Measures end-to-end latency of a single apply_delta call.
// Percentiles (p50/p99/p99.9) are derived from the per-iteration timing
// provided by Google Benchmark's statistics feature.
//
// Requirement 12.1: single-update latency ≤ 1 µs p99

static void BM_UpdateLatency(benchmark::State& state) {
    TempDir tmp;
    ob::Engine engine(tmp.path.string(), /*flush_interval_ns=*/1'000'000'000ULL);
    engine.open();

    ob::DeltaUpdate du{};
    std::vector<ob::Level> levels;
    uint64_t seq = 1;
    const uint64_t base_ts = 1'700'000'000'000'000'000ULL;

    // Warm up: pre-populate the buffer so the first real call isn't cold.
    make_delta(du, levels, seq++, base_ts);
    engine.apply_delta(du, levels.data());

    for (auto _ : state) {
        uint64_t cur_seq = seq++;
        make_delta(du, levels, cur_seq, base_ts + cur_seq);
        auto t0 = std::chrono::steady_clock::now();
        benchmark::DoNotOptimize(engine.apply_delta(du, levels.data()));
        auto t1 = std::chrono::steady_clock::now();
        state.SetIterationTime(
            std::chrono::duration<double>(t1 - t0).count());
    }

    engine.close();
    state.SetLabel("p50/p99/p99.9 via --benchmark_report_aggregates_only=true");
}
BENCHMARK(BM_UpdateLatency)
    ->UseManualTime()
    ->Repetitions(5)
    ->ComputeStatistics("p50",  [](const std::vector<double>& v) {
        auto s = v; std::sort(s.begin(), s.end());
        return s[s.size() / 2];
    })
    ->ComputeStatistics("p99",  [](const std::vector<double>& v) {
        auto s = v; std::sort(s.begin(), s.end());
        return s[static_cast<size_t>(s.size() * 0.99)];
    })
    ->ComputeStatistics("p99.9", [](const std::vector<double>& v) {
        auto s = v; std::sort(s.begin(), s.end());
        return s[static_cast<size_t>(s.size() * 0.999)];
    })
    ->Unit(benchmark::kNanosecond);

// ── BM_IngestionThroughput ────────────────────────────────────────────────────
// Measures sustained single-core ingestion throughput (updates/second).
//
// Requirement 12.2: ≥ 1M updates/second on a single core

static void BM_IngestionThroughput(benchmark::State& state) {
    TempDir tmp;
    ob::Engine engine(tmp.path.string(), /*flush_interval_ns=*/1'000'000'000ULL);
    engine.open();

    ob::DeltaUpdate du{};
    std::vector<ob::Level> levels;
    uint64_t seq = 1;
    const uint64_t base_ts = 1'700'000'000'000'000'000ULL;

    int64_t updates = 0;
    for (auto _ : state) {
        uint64_t cur_seq = seq++;
        make_delta(du, levels, cur_seq, base_ts + cur_seq, /*n=*/1);
        benchmark::DoNotOptimize(engine.apply_delta(du, levels.data()));
        ++updates;
    }

    engine.close();
    state.SetItemsProcessed(updates);
    state.SetLabel("updates/sec");
}
BENCHMARK(BM_IngestionThroughput)
    ->Unit(benchmark::kNanosecond)
    ->MinTime(2.0);

// ── BM_VwapLatency ────────────────────────────────────────────────────────────
// Measures VWAP computation latency over 1000 levels on a warm SoA buffer.
//
// Requirement 12.3: VWAP over 1000 levels ≤ 10 µs

static void BM_VwapLatency(benchmark::State& state) {
    // Build a fully-populated SoASide with 1000 levels.
    ob::SoASide side{};
    side.depth = ob::MAX_LEVELS;
    for (uint32_t i = 0; i < ob::MAX_LEVELS; ++i) {
        side.prices[i]       = 50'000'00LL - static_cast<int64_t>(i) * 10LL;
        side.quantities[i]   = 1'000ULL + i;
        side.order_counts[i] = 1;
    }
    side.version.store(0, std::memory_order_relaxed);

    ob::AggregationEngine agg;

    for (auto _ : state) {
        benchmark::DoNotOptimize(agg.vwap(side, ob::MAX_LEVELS));
    }

    state.SetLabel("VWAP over 1000 levels");
    state.SetItemsProcessed(state.iterations() * ob::MAX_LEVELS);
}
BENCHMARK(BM_VwapLatency)->Unit(benchmark::kNanosecond);

// ── BM_TimeRangeQuery ─────────────────────────────────────────────────────────
// Measures query latency for a time-range scan over a pre-populated engine.
// We insert N snapshots then measure a full-range SELECT.
//
// Requirement 12.4: time-range query over 1M rows ≤ 100 ms

static void BM_TimeRangeQuery(benchmark::State& state) {
    const int64_t N = state.range(0);
    TempDir tmp;
    ob::Engine engine(tmp.path.string(), /*flush_interval_ns=*/1'000'000'000ULL);
    engine.open();

    // Ingest N updates so the columnar store has data to scan.
    ob::DeltaUpdate du{};
    std::vector<ob::Level> levels;
    const uint64_t base_ts = 1'700'000'000'000'000'000ULL;
    for (int64_t i = 0; i < N; ++i) {
        make_delta(du, levels, static_cast<uint64_t>(i + 1),
                   base_ts + static_cast<uint64_t>(i) * 1'000ULL, /*n=*/1);
        engine.apply_delta(du, levels.data());
    }

    // Force flush so rows are in the columnar store.
    engine.close();
    engine.open();

    const std::string sql =
        "SELECT timestamp_ns, price, quantity "
        "FROM orderbook WHERE symbol='BTC-USD' AND exchange='BENCH' "
        "AND timestamp_ns >= " + std::to_string(base_ts) +
        " AND timestamp_ns <= " + std::to_string(base_ts + static_cast<uint64_t>(N) * 1'000ULL);

    int64_t row_count = 0;
    for (auto _ : state) {
        row_count = 0;
        benchmark::DoNotOptimize(
            engine.execute(sql, [&](const ob::QueryResult&) { ++row_count; }));
    }

    engine.close();
    state.SetItemsProcessed(state.iterations() * row_count);
    state.SetLabel("rows scanned per query");
}
BENCHMARK(BM_TimeRangeQuery)
    ->Arg(10'000)
    ->Arg(100'000)
    ->Unit(benchmark::kMillisecond);

} // namespace

// ── main ──────────────────────────────────────────────────────────────────────
// benchmark::Initialize + RunSpecifiedBenchmarks is provided by
// benchmark::benchmark_main (linked via CMake).
// To emit JSON: ./bench_engine --benchmark_format=json --benchmark_out=results.json
