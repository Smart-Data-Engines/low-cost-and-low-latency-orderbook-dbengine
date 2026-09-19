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
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <sys/statfs.h>

#include "orderbook/aggregation.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/response_formatter.hpp"
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

// ── BM_IngestionThroughputBatched ────────────────────────────────────────────
//
// The same path with twenty levels per update instead of one, and it exists so that the in-process
// figure can be compared with the one over the wire.
//
// The comparative harness sends one MINSERT per book update and the dataset's updates carry twenty
// levels, so its ingest row is levels per second at twenty levels an update. The benchmark above
// applies **one** level per call, and calling both of them "updates/s" made a factor-of-twenty unit
// difference invisible: the published claim that the round trip costs a factor of 111 was one
// number in each unit. This one counts levels and uses the wire's shape, so the two are subtractable.
//
// It is also a less flattering measurement, which is the other reason it is here: the single-level
// benchmark rewrites one price on a one-level book, which is the most cache-friendly shape this
// engine has.
static void BM_IngestionThroughputBatched(benchmark::State& state) {
    constexpr uint16_t kLevels = 20;

    TempDir tmp;
    ob::Engine engine(tmp.path.string(), /*flush_interval_ns=*/1'000'000'000ULL);
    engine.open();

    ob::DeltaUpdate du{};
    std::vector<ob::Level> levels;
    uint64_t seq = 1;
    const uint64_t base_ts = 1'700'000'000'000'000'000ULL;

    int64_t applied_levels = 0;
    for (auto _ : state) {
        uint64_t cur_seq = seq++;
        make_delta(du, levels, cur_seq, base_ts + cur_seq, kLevels);
        benchmark::DoNotOptimize(engine.apply_delta(du, levels.data()));
        applied_levels += kLevels;
    }

    engine.close();
    state.SetItemsProcessed(applied_levels);
    state.SetLabel("levels/sec, twenty per update - the shape the wire carries");
}
BENCHMARK(BM_IngestionThroughputBatched)
    ->Unit(benchmark::kNanosecond)
    ->MinTime(2.0);

// ── BM_FormatQueryResponse ───────────────────────────────────────────────────
//
// The response, on its own. It is here because `perf` put `format_query_response` at **22.96%** of
// the server's profile over 16,000 of the comparative benchmark's time-range query, against
// **2.70%** for `ColumnarStore::scan` - the answer cost eight times the read it came from.
//
// Read it beside the path rather than instead of it: an isolated component overstates what
// removing it buys on the path containing it, which this repository has measured (187 ns of CRC32C
// saving was worth 25 ns inside `apply_delta`). The claim about the query belongs to a query.
//
// 4,000 rows is the size the comparative harness asks for - one symbol of a 200,000-row dataset -
// and the values are the shape that dataset carries, because the cost is digits and a benchmark
// over single-digit fields would measure a row a third the width.
//
// Two column lists, because they answer different questions. Seven is what `SELECT *` costs and
// is the figure comparable with every measurement taken before projection existed. Three is the
// question the comparative harness actually asks ClickHouse and TimescaleDB, and since #139 it is
// the one it asks this engine too.
namespace {

std::vector<ob::QueryResult> benchmark_rows(size_t n) {
    std::vector<ob::QueryResult> rows(n);
    const uint64_t base_ts = 1'700'000'000'000'000'000ULL;
    for (size_t i = 0; i < n; ++i) {
        rows[i].timestamp_ns    = base_ts + i * 1'000ULL;
        rows[i].price           = 5'000'000LL - static_cast<int64_t>(i % 20) * 100LL;
        rows[i].quantity        = 1'000ULL + (i % 20);
        rows[i].order_count     = 1;
        rows[i].side            = static_cast<uint8_t>(i & 1U);
        rows[i].level           = static_cast<uint16_t>(i % 20);
        rows[i].sequence_number = i + 1;
    }
    return rows;
}

void run_format_benchmark(benchmark::State& state, const std::vector<ob::QueryColumn>& columns) {
    const size_t n = static_cast<size_t>(state.range(0));
    const std::vector<ob::QueryResult> rows = benchmark_rows(n);

    size_t bytes = 0;
    for (auto _ : state) {
        std::string out = ob::format_query_response(rows, columns);
        bytes = out.size();
        benchmark::DoNotOptimize(out.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * n));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * bytes));
    state.SetLabel(std::to_string(columns.size()) + " columns; " +
                   std::to_string(bytes / (n ? n : 1)) + " bytes/row");
}

}  // namespace

static void BM_FormatQueryResponse(benchmark::State& state) {
    run_format_benchmark(state, ob::all_query_columns());
}
BENCHMARK(BM_FormatQueryResponse)->Arg(4000)->Unit(benchmark::kMicrosecond);

/// The harness's three columns. Comparable with the seven above only as a ratio: it is a cheaper
/// answer to a narrower question, not the same work done faster.
static void BM_FormatQueryResponseProjected(benchmark::State& state) {
    run_format_benchmark(state, {ob::QueryColumn::TimestampNs,
                                 ob::QueryColumn::Price,
                                 ob::QueryColumn::Quantity});
}
BENCHMARK(BM_FormatQueryResponseProjected)->Arg(4000)->Unit(benchmark::kMicrosecond);

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

    // The engine's SELECT grammar is FROM '<symbol>'.'<exchange>' with a
    // BETWEEN range. The earlier query here used a symbol='...' WHERE clause the
    // parser does not accept, so execute() returned an error string, the callback
    // never ran and this benchmark timed the parser rejecting a query — 4 µs that
    // was published as the scan latency for 100k rows.
    const std::string sql =
        "SELECT * FROM 'BTC-USD'.'BENCH' WHERE timestamp BETWEEN " +
        std::to_string(base_ts) + " AND " +
        std::to_string(base_ts + static_cast<uint64_t>(N) * 1'000ULL);

    // Fail loudly rather than report a fast empty scan.
    {
        int64_t probe = 0;
        const std::string err =
            engine.execute(sql, [&](const ob::QueryResult&) { ++probe; });
        if (!err.empty()) {
            state.SkipWithError(("query rejected: " + err).c_str());
            engine.close();
            return;
        }
        if (probe == 0) {
            state.SkipWithError("query returned no rows: nothing would be measured");
            engine.close();
            return;
        }
    }

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
//
// Hand-written rather than benchmark_main's, for one reason: the filesystem the engine's storage
// lands on belongs beside these numbers, and the only way into --benchmark_out is to call
// AddCustomContext before the run.
//
// `temp_directory_path()` is $TMPDIR, or /tmp. On a machine whose /tmp is a tmpfs - the default on
// a good share of them, including the AWS instance where this was first noticed - the WAL and the
// segments this benchmark writes go to **memory**, and BM_IngestionThroughput becomes a measurement
// of the engine against RAM. That is a legitimate number, and a different one from the same
// benchmark on a disk: measured on one such machine, the two differ by more than the change this
// file was being used to evaluate. What is not legitimate is publishing either without saying which.
//
// Reported rather than refused, unlike the comparative harness, and the difference is who is being
// compared: there one system would have been on RAM while two were on disk, which is a false
// comparison; here there is one system and both storage choices are real questions about it. Point
// $TMPDIR at a disk to ask the other one.
//
// To emit JSON: ./bench_engine --benchmark_format=json --benchmark_out=results.json

namespace {

/// The filesystem under `path`, from statfs rather than from a subprocess.
///
/// The magic numbers are the kernel's, and the ones not listed fall through to hex rather than to
/// "unknown": a number a reader can look up is worth more than a word that says nothing.
std::string filesystem_name(const std::filesystem::path& path) {
    struct statfs info {};
    if (::statfs(path.c_str(), &info) != 0) {
        return "unknown (statfs failed)";
    }
    switch (static_cast<unsigned long>(info.f_type)) {
        case 0x01021994UL: return "tmpfs";
        case 0x858458F6UL: return "ramfs";
        case 0x0000EF53UL: return "ext2/3/4";
        case 0x58465342UL: return "xfs";
        case 0x9123683EUL: return "btrfs";
        case 0x2FC12FC1UL: return "zfs";
        case 0x794C7630UL: return "overlayfs";
        case 0x65735546UL: return "fuse";
        default: {
            char buf[32];
            std::snprintf(buf, sizeof buf, "0x%lx", static_cast<unsigned long>(info.f_type));
            return buf;
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }

    const std::filesystem::path storage = std::filesystem::temp_directory_path();
    const std::string fs = filesystem_name(storage);
    benchmark::AddCustomContext("engine_storage_path", storage.string());
    benchmark::AddCustomContext("engine_storage_fs", fs);

    if (fs == "tmpfs" || fs == "ramfs") {
        // Said out loud as well as recorded, because the person watching the run is the one who can
        // still point $TMPDIR somewhere else.
        std::fprintf(stderr,
                     "bench_engine: engine storage is %s at %s - the WAL and the segments are in "
                     "memory, not on a disk. Set TMPDIR to measure the other thing.\n",
                     fs.c_str(), storage.c_str());
    }

    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
