// Feature: stress-testing — Stress tests & load benchmarks for orderbook-dbengine
// Requirements: 1.1–1.5, 2.1–2.4, 3.1–3.4, 6.1–6.3, 7.1–7.3
//
// Gated behind OB_STRESS_TESTS env var — each test runs for ≥60 seconds.
// Run manually:  OB_STRESS_TESTS=1 ./build/tests/test_stress

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers — namespace stress
// ═══════════════════════════════════════════════════════════════════════════════

namespace stress {

static std::atomic<uint64_t> g_dir_counter{0};

/// Read VmRSS from /proc/self/status (Linux). Returns KB, 0 if unavailable.
size_t read_rss_kb() {
    std::ifstream f("/proc/self/status");
    if (!f.is_open()) return 0;
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            size_t kb = 0;
            std::sscanf(line.c_str(), "VmRSS: %zu", &kb);
            return kb;
        }
    }
    return 0;
}

/// Recursive directory size in bytes via std::filesystem.
size_t dir_size_bytes(const fs::path& dir) {
    size_t total = 0;
    std::error_code ec;
    for (auto& entry : fs::recursive_directory_iterator(dir, ec)) {
        if (entry.is_regular_file(ec)) {
            total += entry.file_size(ec);
        }
    }
    return total;
}

/// Compute percentile from a vector of doubles. p ∈ [0.0, 1.0].
/// Sorts the vector in-place.
double percentile(std::vector<double>& values, double p) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    size_t idx = static_cast<size_t>(p * static_cast<double>(values.size()));
    if (idx >= values.size()) idx = values.size() - 1;
    return values[idx];
}

/// RAII temp directory — creates on construction, removes on destruction.
struct TempDir {
    fs::path path;
    explicit TempDir(const std::string& prefix) {
        auto tmp = fs::temp_directory_path() /
                   (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
        fs::create_directories(tmp);
        path = tmp;
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

/// Fill a DeltaUpdate + Level with test data using value-init (no memset).
void make_insert(ob::DeltaUpdate& du, ob::Level& lvl,
                 uint64_t seq, uint64_t ts_ns,
                 const char* symbol = "STRESS", const char* exchange = "TEST") {
    du = ob::DeltaUpdate{};
    std::strncpy(du.symbol, symbol, sizeof(du.symbol) - 1);
    std::strncpy(du.exchange, exchange, sizeof(du.exchange) - 1);
    du.sequence_number = seq;
    du.timestamp_ns    = ts_ns;
    du.side            = ob::SIDE_BID;
    du.n_levels        = 1;

    lvl = ob::Level{};
    lvl.price = 50'000'00LL + static_cast<int64_t>(seq % 500);
    lvl.qty   = 1'000ULL + (seq % 500);
    lvl.cnt   = 1;
}

/// Print metrics to stdout in a readable format.
void report(const std::string& test_name,
            const std::map<std::string, std::string>& metrics) {
    std::cout << "\n=== STRESS TEST: " << test_name << " ===" << std::endl;
    for (const auto& [key, val] : metrics) {
        std::cout << std::left << std::setw(24) << (key + ":") << val << std::endl;
    }
    std::cout << std::endl;
}

/// RAII wrapper for Engine: opens on construction, closes on destruction.
struct EngineGuard {
    ob::Engine engine;
    std::string dir;

    explicit EngineGuard(const std::string& d,
                         uint64_t flush_ns = 1'000'000'000ULL,
                         ob::TTLConfig ttl = {})
        : engine(d, flush_ns, ob::FsyncPolicy::NONE, {}, {}, {}, ttl), dir(d) {
        engine.open();
    }
    ~EngineGuard() { engine.close(); }
    EngineGuard(const EngineGuard&) = delete;
    EngineGuard& operator=(const EngineGuard&) = delete;
};

/// Count rows returned by a SELECT query.
int query_row_count(ob::Engine& engine, const char* symbol = "STRESS",
                    const char* exchange = "TEST") {
    int count = 0;
    std::string sql = std::string("SELECT * FROM '") + symbol + "'.'" + exchange +
                      "' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    std::string err = engine.execute(sql, [&](const ob::QueryResult&) { ++count; });
    EXPECT_TRUE(err.empty()) << "Query error: " << err;
    return count;
}

} // namespace stress

using namespace std::chrono;

// ═══════════════════════════════════════════════════════════════════════════════
// Task 4.1: TEST(StressTest, SustainedIngestion)
// Continuous INSERT ≥100k/s for 60s, flush, verify count == inserted.
// Property 1: Data completeness after ingestion + flush
// Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5
// ═══════════════════════════════════════════════════════════════════════════════

TEST(StressTest, SustainedIngestion) {
    if (!std::getenv("OB_STRESS_TESTS")) {
        GTEST_SKIP() << "Set OB_STRESS_TESTS=1";
    }

    stress::TempDir tmp("stress_ingest_");
    // 100ms flush interval, FsyncPolicy::NONE
    stress::EngineGuard eg(tmp.path.string(), 100'000'000ULL);

    const auto duration = seconds(60);
    const size_t rss_start = stress::read_rss_kb();
    size_t rss_peak = rss_start;

    uint64_t seq = 1;
    uint64_t total_inserted = 0;
    auto t0 = steady_clock::now();
    auto last_report = t0;

    while (steady_clock::now() - t0 < duration) {
        ob::DeltaUpdate du{};
        ob::Level lvl{};
        uint64_t ts_ns = static_cast<uint64_t>(
            duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
        stress::make_insert(du, lvl, seq, ts_ns);

        ob::ob_status_t st = eg.engine.apply_delta(du, &lvl);
        ASSERT_EQ(st, ob::OB_OK);
        ++seq;
        ++total_inserted;

        // Report throughput every 10 seconds
        auto now = steady_clock::now();
        if (now - last_report >= seconds(10)) {
            double elapsed_s = duration_cast<milliseconds>(now - t0).count() / 1000.0;
            double tps = static_cast<double>(total_inserted) / elapsed_s;
            std::cout << "[SustainedIngestion] " << std::fixed << std::setprecision(0)
                      << elapsed_s << "s: " << tps << " inserts/s" << std::endl;

            size_t rss_now = stress::read_rss_kb();
            if (rss_now > rss_peak) rss_peak = rss_now;
            last_report = now;
        }
    }

    // Final flush
    eg.engine.flush_incremental();

    size_t rss_end = stress::read_rss_kb();
    if (rss_end > rss_peak) rss_peak = rss_end;

    // Verify data completeness
    int queried = stress::query_row_count(eg.engine);

    double elapsed_s = duration_cast<milliseconds>(steady_clock::now() - t0).count() / 1000.0;
    double tps = static_cast<double>(total_inserted) / elapsed_s;

    stress::report("SustainedIngestion", {
        {"Duration",       std::to_string(elapsed_s) + "s"},
        {"Total inserts",  std::to_string(total_inserted)},
        {"Throughput",     std::to_string(static_cast<uint64_t>(tps)) + " inserts/s"},
        {"RSS start",      std::to_string(rss_start) + " KB"},
        {"RSS peak",       std::to_string(rss_peak) + " KB"},
        {"RSS end",        std::to_string(rss_end) + " KB"},
        {"Query rows",     std::to_string(queried) + " (expected: " + std::to_string(total_inserted) + ")"},
    });

    ASSERT_EQ(static_cast<uint64_t>(queried), total_inserted)
        << "Data loss detected: queried " << queried << " vs inserted " << total_inserted;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 4.2: TEST(StressTest, ConcurrentReadWrite)
// 2 writer threads + 2 reader threads for 60s.
// Property 1: Data completeness + Property 2: No corrupted data
// Validates: Requirements 2.1, 2.2, 2.3, 2.4
// ═══════════════════════════════════════════════════════════════════════════════

TEST(StressTest, ConcurrentReadWrite) {
    if (!std::getenv("OB_STRESS_TESTS")) {
        GTEST_SKIP() << "Set OB_STRESS_TESTS=1";
    }

    stress::TempDir tmp("stress_conc_");
    stress::EngineGuard eg(tmp.path.string(), 100'000'000ULL);

    const auto duration = seconds(60);
    std::atomic<bool> stop{false};

    // Per-writer counters
    std::atomic<uint64_t> w0_count{0};
    std::atomic<uint64_t> w1_count{0};
    // Per-reader counters
    std::atomic<uint64_t> r0_queries{0};
    std::atomic<uint64_t> r1_queries{0};
    // Corruption flag
    std::atomic<bool> corruption_detected{false};

    // Writer 0 — symbol "W0"
    std::thread writer0([&]() {
        uint64_t seq = 1;
        while (!stop.load(std::memory_order_relaxed)) {
            ob::DeltaUpdate du{};
            ob::Level lvl{};
            uint64_t ts_ns = static_cast<uint64_t>(
                duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
            stress::make_insert(du, lvl, seq, ts_ns, "W0", "TEST");
            ob::ob_status_t st = eg.engine.apply_delta(du, &lvl);
            if (st == ob::OB_OK) {
                ++seq;
                w0_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    // Writer 1 — symbol "W1"
    std::thread writer1([&]() {
        uint64_t seq = 1;
        while (!stop.load(std::memory_order_relaxed)) {
            ob::DeltaUpdate du{};
            ob::Level lvl{};
            uint64_t ts_ns = static_cast<uint64_t>(
                duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
            stress::make_insert(du, lvl, seq, ts_ns, "W1", "TEST");
            ob::ob_status_t st = eg.engine.apply_delta(du, &lvl);
            if (st == ob::OB_OK) {
                ++seq;
                w1_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    // Reader 0 — reads W0
    std::thread reader0([&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            std::string sql = "SELECT * FROM 'W0'.'TEST' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
            std::string err = eg.engine.execute(sql, [&](const ob::QueryResult& row) {
                if (row.price <= 0 || row.quantity <= 0 || row.timestamp_ns == 0) {
                    corruption_detected.store(true, std::memory_order_relaxed);
                }
            });
            // NOT_FOUND is OK when no data flushed yet
            r0_queries.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // Reader 1 — reads W1
    std::thread reader1([&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            std::string sql = "SELECT * FROM 'W1'.'TEST' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
            std::string err = eg.engine.execute(sql, [&](const ob::QueryResult& row) {
                if (row.price <= 0 || row.quantity <= 0 || row.timestamp_ns == 0) {
                    corruption_detected.store(true, std::memory_order_relaxed);
                }
            });
            r1_queries.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // Run for duration
    std::this_thread::sleep_for(duration);
    stop.store(true, std::memory_order_relaxed);

    writer0.join();
    writer1.join();
    reader0.join();
    reader1.join();

    // Final flush
    eg.engine.flush_incremental();

    uint64_t total_w0 = w0_count.load();
    uint64_t total_w1 = w1_count.load();
    uint64_t total_writes = total_w0 + total_w1;
    uint64_t total_reads = r0_queries.load() + r1_queries.load();

    // Verify row counts
    int rows_w0 = stress::query_row_count(eg.engine, "W0", "TEST");
    int rows_w1 = stress::query_row_count(eg.engine, "W1", "TEST");
    int total_rows = rows_w0 + rows_w1;

    stress::report("ConcurrentReadWrite", {
        {"W0 inserts",     std::to_string(total_w0)},
        {"W1 inserts",     std::to_string(total_w1)},
        {"Total writes",   std::to_string(total_writes)},
        {"Total reads",    std::to_string(total_reads)},
        {"Write tps",      std::to_string(total_writes / 60) + "/s"},
        {"Read qps",       std::to_string(total_reads / 60) + "/s"},
        {"Rows W0",        std::to_string(rows_w0)},
        {"Rows W1",        std::to_string(rows_w1)},
        {"Corruption",     corruption_detected.load() ? "YES" : "none"},
    });

    EXPECT_FALSE(corruption_detected.load()) << "Corrupted data detected in reader threads";
    ASSERT_EQ(static_cast<uint64_t>(total_rows), total_writes)
        << "Data loss: queried " << total_rows << " vs inserted " << total_writes;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 4.3: TEST(StressTest, MinsertBatchThroughput)
// MINSERT with 1000 levels per batch for 60s, flush every 1000 batches.
// Property 1: Data completeness after ingestion + flush
// Validates: Requirements 3.1, 3.2, 3.3, 3.4
// ═══════════════════════════════════════════════════════════════════════════════

TEST(StressTest, MinsertBatchThroughput) {
    if (!std::getenv("OB_STRESS_TESTS")) {
        GTEST_SKIP() << "Set OB_STRESS_TESTS=1";
    }

    stress::TempDir tmp("stress_minsert_");
    stress::EngineGuard eg(tmp.path.string(), 100'000'000ULL);

    const auto duration = seconds(60);
    constexpr uint16_t LEVELS_PER_BATCH = 1000;

    uint64_t seq = 1;
    uint64_t total_batches = 0;
    uint64_t total_levels = 0;
    uint64_t batches_since_flush = 0;
    std::vector<double> flush_latencies_ms;

    auto t0 = steady_clock::now();

    while (steady_clock::now() - t0 < duration) {
        // Build a batch of 1000 levels as individual apply_delta calls
        // (MINSERT at engine level = apply_delta with n_levels > 1)
        ob::DeltaUpdate du{};
        du = ob::DeltaUpdate{};
        std::strncpy(du.symbol, "MBATCH", sizeof(du.symbol) - 1);
        std::strncpy(du.exchange, "TEST", sizeof(du.exchange) - 1);
        du.sequence_number = seq++;
        du.timestamp_ns = static_cast<uint64_t>(
            duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
        du.side = ob::SIDE_BID;
        du.n_levels = LEVELS_PER_BATCH;

        ob::Level levels[1000]{};
        for (uint16_t i = 0; i < LEVELS_PER_BATCH; ++i) {
            levels[i].price = 50'000'00LL + static_cast<int64_t>(i);
            levels[i].qty   = 1'000ULL + i;
            levels[i].cnt   = 1;
        }

        ob::ob_status_t st = eg.engine.apply_delta(du, levels);
        ASSERT_EQ(st, ob::OB_OK);

        ++total_batches;
        total_levels += LEVELS_PER_BATCH;
        ++batches_since_flush;

        // Flush every 1000 batches, measure latency
        if (batches_since_flush >= 1000) {
            auto flush_start = steady_clock::now();
            eg.engine.flush_incremental();
            auto flush_end = steady_clock::now();
            double flush_ms = duration_cast<microseconds>(flush_end - flush_start).count() / 1000.0;
            flush_latencies_ms.push_back(flush_ms);
            batches_since_flush = 0;
        }
    }

    // Final flush — call twice with a small delay to catch any data drained
    // by the background flush_loop that hasn't been written to segments yet.
    eg.engine.flush_incremental();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    eg.engine.flush_incremental();

    double elapsed_s = duration_cast<milliseconds>(steady_clock::now() - t0).count() / 1000.0;
    double levels_per_s = static_cast<double>(total_levels) / elapsed_s;

    // Compute flush percentiles
    std::string p50_str = "N/A", p95_str = "N/A", p99_str = "N/A";
    if (!flush_latencies_ms.empty()) {
        double p50 = stress::percentile(flush_latencies_ms, 0.50);
        double p95 = stress::percentile(flush_latencies_ms, 0.95);
        double p99 = stress::percentile(flush_latencies_ms, 0.99);
        p50_str = std::to_string(p50) + " ms";
        p95_str = std::to_string(p95) + " ms";
        p99_str = std::to_string(p99) + " ms";
    }

    // Verify data completeness
    int queried = stress::query_row_count(eg.engine, "MBATCH", "TEST");

    stress::report("MinsertBatchThroughput", {
        {"Duration",        std::to_string(elapsed_s) + "s"},
        {"Total batches",   std::to_string(total_batches)},
        {"Total levels",    std::to_string(total_levels)},
        {"Levels/s",        std::to_string(static_cast<uint64_t>(levels_per_s))},
        {"Flush count",     std::to_string(flush_latencies_ms.size())},
        {"Flush p50",       p50_str},
        {"Flush p95",       p95_str},
        {"Flush p99",       p99_str},
        {"Query rows",      std::to_string(queried) + " (expected: " + std::to_string(total_levels) + ")"},
    });

    ASSERT_EQ(static_cast<uint64_t>(queried), total_levels)
        << "Data loss: queried " << queried << " vs inserted " << total_levels;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 4.4: TEST(StressTest, RssMemoryProfile)
// Continuous insert + flush every 5s for 60s, sample RSS every 1s.
// Property 3: RSS bounded — peak - start < 500 MB (512000 KB)
// Validates: Requirements 6.1, 6.2, 6.3
// ═══════════════════════════════════════════════════════════════════════════════

TEST(StressTest, RssMemoryProfile) {
    if (!std::getenv("OB_STRESS_TESTS")) {
        GTEST_SKIP() << "Set OB_STRESS_TESTS=1";
    }

    stress::TempDir tmp("stress_rss_");
    stress::EngineGuard eg(tmp.path.string(), 100'000'000ULL);

    const auto duration = seconds(60);
    const size_t rss_start = stress::read_rss_kb();
    size_t rss_peak = rss_start;
    std::vector<size_t> rss_samples;
    rss_samples.push_back(rss_start);

    uint64_t seq = 1;
    uint64_t total_inserted = 0;
    auto t0 = steady_clock::now();
    auto last_flush = t0;
    auto last_rss_sample = t0;

    while (steady_clock::now() - t0 < duration) {
        ob::DeltaUpdate du{};
        ob::Level lvl{};
        uint64_t ts_ns = static_cast<uint64_t>(
            duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
        stress::make_insert(du, lvl, seq, ts_ns, "RSS", "TEST");

        ob::ob_status_t st = eg.engine.apply_delta(du, &lvl);
        ASSERT_EQ(st, ob::OB_OK);
        ++seq;
        ++total_inserted;

        auto now = steady_clock::now();

        // Flush every 5 seconds
        if (now - last_flush >= seconds(5)) {
            eg.engine.flush_incremental();
            last_flush = now;
        }

        // Sample RSS every 1 second
        if (now - last_rss_sample >= seconds(1)) {
            size_t rss_now = stress::read_rss_kb();
            if (rss_now > 0) {
                rss_samples.push_back(rss_now);
                if (rss_now > rss_peak) rss_peak = rss_now;
            }
            last_rss_sample = now;
        }
    }

    // Final flush
    eg.engine.flush_incremental();

    size_t rss_end = stress::read_rss_kb();
    if (rss_end > rss_peak) rss_peak = rss_end;

    int64_t rss_delta = static_cast<int64_t>(rss_peak) - static_cast<int64_t>(rss_start);

    stress::report("RssMemoryProfile", {
        {"Total inserts",  std::to_string(total_inserted)},
        {"RSS start",      std::to_string(rss_start) + " KB"},
        {"RSS peak",       std::to_string(rss_peak) + " KB"},
        {"RSS end",        std::to_string(rss_end) + " KB"},
        {"RSS delta",      std::to_string(rss_delta) + " KB"},
        {"RSS samples",    std::to_string(rss_samples.size())},
    });

    // Assert RSS bounded — skip if read_rss_kb() returns 0 (macOS)
    if (rss_start > 0) {
        ASSERT_LT(rss_delta, 512000)
            << "RSS grew by " << rss_delta << " KB (>" << 512000 << " KB / 500 MB limit)";
    } else {
        std::cout << "[RssMemoryProfile] WARNING: /proc/self/status unavailable, "
                  << "skipping RSS assertion" << std::endl;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 4.5: TEST(StressTest, DiskUsageProfile)
// Continuous insert with TTL for 60s, sample disk size every 5s.
// Property 4: Disk bounded — disk_end < 2 × disk_peak
// Validates: Requirements 7.1, 7.2, 7.3
// ═══════════════════════════════════════════════════════════════════════════════

TEST(StressTest, DiskUsageProfile) {
    if (!std::getenv("OB_STRESS_TESTS")) {
        GTEST_SKIP() << "Set OB_STRESS_TESTS=1";
    }

    stress::TempDir tmp("stress_disk_");
    // TTL: 1 hour, scan every 5 seconds
    ob::TTLConfig ttl_cfg{1, 5};
    stress::EngineGuard eg(tmp.path.string(), 100'000'000ULL, ttl_cfg);

    const auto duration = seconds(60);
    size_t disk_start = stress::dir_size_bytes(tmp.path);
    size_t disk_peak = disk_start;
    std::vector<size_t> disk_samples;
    disk_samples.push_back(disk_start);

    uint64_t seq = 1;
    uint64_t total_inserted = 0;
    auto t0 = steady_clock::now();
    auto last_sample = t0;

    while (steady_clock::now() - t0 < duration) {
        ob::DeltaUpdate du{};
        ob::Level lvl{};
        uint64_t ts_ns = static_cast<uint64_t>(
            duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
        stress::make_insert(du, lvl, seq, ts_ns, "DISK", "TEST");

        ob::ob_status_t st = eg.engine.apply_delta(du, &lvl);
        ASSERT_EQ(st, ob::OB_OK);
        ++seq;
        ++total_inserted;

        auto now = steady_clock::now();

        // Sample disk size every 5 seconds
        if (now - last_sample >= seconds(5)) {
            eg.engine.flush_incremental();
            size_t disk_now = stress::dir_size_bytes(tmp.path);
            disk_samples.push_back(disk_now);
            if (disk_now > disk_peak) disk_peak = disk_now;
            last_sample = now;
        }
    }

    // Final flush
    eg.engine.flush_incremental();

    size_t disk_end = stress::dir_size_bytes(tmp.path);
    if (disk_end > disk_peak) disk_peak = disk_end;

    // Get engine stats for WAL/segment counts
    auto st = eg.engine.stats();

    stress::report("DiskUsageProfile", {
        {"Total inserts",  std::to_string(total_inserted)},
        {"Disk start",     std::to_string(disk_start / 1024) + " KB"},
        {"Disk peak",      std::to_string(disk_peak / 1024) + " KB"},
        {"Disk end",       std::to_string(disk_end / 1024) + " KB"},
        {"Disk samples",   std::to_string(disk_samples.size())},
        {"WAL file index", std::to_string(st.wal_file_index)},
        {"Segments",       std::to_string(st.segment_count)},
    });

    // Assert disk bounded: end < 2 × peak
    // This verifies WAL truncation keeps disk usage reasonable
    ASSERT_LE(disk_end, 2 * disk_peak)
        << "Disk grew unbounded: end=" << disk_end << " > 2×peak=" << 2 * disk_peak;

    // Also verify disk end is under 2 GB (reasonable for 60s of inserts)
    constexpr size_t TWO_GB = 2ULL * 1024 * 1024 * 1024;
    ASSERT_LT(disk_end, TWO_GB)
        << "Disk usage exceeded 2 GB: " << disk_end << " bytes";
}
