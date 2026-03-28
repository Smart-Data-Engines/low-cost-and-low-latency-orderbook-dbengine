// Feature: incremental-flush — Tests for two-phase incremental flush
// Requirements: 1.1, 1.3, 1.4, 3.1, 3.3, 4.1, 4.2, 6.1, 6.2, 6.3, 8.3, 8.4, 9.4

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"
#include "orderbook/replication.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// ── Helpers ───────────────────────────────────────────────────────────────────

namespace {

static std::atomic<uint64_t> g_dir_counter{0};

std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() /
               (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
    fs::create_directories(tmp);
    return tmp.string();
}

/// RAII wrapper for Engine: opens on construction, closes on destruction.
struct EngineGuard {
    ob::Engine engine;
    std::string dir;

    explicit EngineGuard(const std::string& d,
                         uint64_t flush_ns = 1'000'000'000ULL)
        : engine(d, flush_ns, ob::FsyncPolicy::NONE), dir(d) {
        engine.open();
    }
    ~EngineGuard() {
        engine.close();
    }
    EngineGuard(const EngineGuard&) = delete;
    EngineGuard& operator=(const EngineGuard&) = delete;
};

/// RAII temp directory cleanup.
struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix)
        : path(make_temp_dir(prefix)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

/// Insert N rows into engine via apply_delta. Each row gets a unique timestamp.
void insert_rows(ob::Engine& engine, int count, uint64_t ts_base = 1'000'000'000ULL,
                 uint64_t seq_base = 1, int64_t price_base = 10000,
                 const char* symbol = "SYM", const char* exchange = "EXC") {
    for (int i = 0; i < count; ++i) {
        ob::DeltaUpdate delta{};
        std::memset(delta.symbol, 0, sizeof(delta.symbol));
        std::memset(delta.exchange, 0, sizeof(delta.exchange));
        std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, exchange, sizeof(delta.exchange) - 1);
        delta.sequence_number = seq_base + static_cast<uint64_t>(i);
        delta.timestamp_ns    = ts_base + static_cast<uint64_t>(i) * 1'000'000ULL;
        delta.side            = ob::SIDE_BID;
        delta.n_levels        = 1;

        ob::Level lvl{};
        lvl.price = price_base + i;
        lvl.qty   = 100 + static_cast<uint64_t>(i);
        lvl.cnt   = 1;
        lvl._pad  = 0;

        ob::ob_status_t st = engine.apply_delta(delta, &lvl);
        ASSERT_EQ(st, ob::OB_OK);
    }
}

/// Count rows returned by a SELECT query on the given symbol.exchange.
int query_row_count(ob::Engine& engine, const char* symbol = "SYM",
                    const char* exchange = "EXC") {
    int count = 0;
    std::string sql = std::string("SELECT * FROM '") + symbol + "'.'" + exchange +
                      "' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    std::string err = engine.execute(sql, [&](const ob::QueryResult&) { ++count; });
    EXPECT_TRUE(err.empty()) << "Query error: " << err;
    return count;
}

/// Collect prices returned by a SELECT query.
std::vector<int64_t> query_prices(ob::Engine& engine, const char* symbol = "SYM",
                                   const char* exchange = "EXC") {
    std::vector<int64_t> prices;
    std::string sql = std::string("SELECT * FROM '") + symbol + "'.'" + exchange +
                      "' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    std::string err = engine.execute(sql, [&](const ob::QueryResult& r) {
        prices.push_back(r.price);
    });
    EXPECT_TRUE(err.empty()) << "Query error: " << err;
    return prices;
}

} // anonymous namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Task 6.2: Unit test — test_flush_incremental_basic
// Insert 100 rows via apply_delta, flush_incremental(), SELECT returns 100 rows
// Validates: Requirement 1.1
// ═══════════════════════════════════════════════════════════════════════════════

TEST(IncrementalFlush, test_flush_incremental_basic) {
    TempDir tmp("iflush_basic_");
    EngineGuard eg(tmp.path);

    insert_rows(eg.engine, 100);
    eg.engine.flush_incremental();

    int count = query_row_count(eg.engine);
    EXPECT_EQ(count, 100);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 6.3: Unit test — test_flush_incremental_empty
// flush_incremental() with no pending rows, no error
// Validates: Requirement 1.4
// ═══════════════════════════════════════════════════════════════════════════════

TEST(IncrementalFlush, test_flush_incremental_empty) {
    TempDir tmp("iflush_empty_");
    EngineGuard eg(tmp.path);

    // Should not throw or error
    EXPECT_NO_THROW(eg.engine.flush_incremental());

    // Double flush — still no error
    EXPECT_NO_THROW(eg.engine.flush_incremental());

    // Query on non-existent symbol returns NOT_FOUND — that's expected for empty engine
    int count = 0;
    std::string err = eg.engine.execute(
        "SELECT * FROM 'SYM'.'EXC' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult&) { ++count; });
    // Either empty result or NOT_FOUND is acceptable for empty engine
    EXPECT_EQ(count, 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 6.4: Unit test — test_close_still_flushes_all
// Insert rows, close(), open(), query returns all rows
// Validates: Requirement 8.3
// ═══════════════════════════════════════════════════════════════════════════════

TEST(IncrementalFlush, test_close_still_flushes_all) {
    TempDir tmp("iflush_close_");

    {
        EngineGuard eg(tmp.path);
        insert_rows(eg.engine, 50);
        // EngineGuard destructor calls close()
    }

    // Reopen and verify all rows are present
    {
        EngineGuard eg2(tmp.path);
        int count = query_row_count(eg2.engine);
        EXPECT_EQ(count, 50);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
// Task 6.5: Property 1 — Flush round-trip
// Feature: incremental-flush, Property 1: for any set of rows, after
// flush_incremental(), SELECT returns all rows with correct values
// **Validates: Requirements 1.1, 2.3, 4.2**
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IncrementalFlushProp, prop_flush_roundtrip, ()) {
    const auto row_count = *rc::gen::inRange(1, 200);
    const auto price_base = *rc::gen::inRange<int64_t>(1000, 100000);

    TempDir tmp("iflush_prop1_");
    EngineGuard eg(tmp.path);

    // Insert rows
    for (int i = 0; i < row_count; ++i) {
        ob::DeltaUpdate delta{};
        std::memset(delta.symbol, 0, sizeof(delta.symbol));
        std::memset(delta.exchange, 0, sizeof(delta.exchange));
        std::strncpy(delta.symbol, "SYM", sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "EXC", sizeof(delta.exchange) - 1);
        delta.sequence_number = static_cast<uint64_t>(i + 1);
        delta.timestamp_ns    = 1'000'000'000ULL + static_cast<uint64_t>(i) * 1'000'000ULL;
        delta.side            = ob::SIDE_BID;
        delta.n_levels        = 1;

        ob::Level lvl{};
        lvl.price = price_base + i;
        lvl.qty   = 100 + static_cast<uint64_t>(i);
        lvl.cnt   = 1;
        lvl._pad  = 0;

        ob::ob_status_t st = eg.engine.apply_delta(delta, &lvl);
        RC_ASSERT(st == ob::OB_OK);
    }

    eg.engine.flush_incremental();

    // Verify all rows are returned
    std::vector<int64_t> got_prices;
    std::string err = eg.engine.execute(
        "SELECT * FROM 'SYM'.'EXC' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult& r) { got_prices.push_back(r.price); });
    RC_ASSERT(err.empty());
    RC_ASSERT(static_cast<int>(got_prices.size()) == row_count);

    // Verify prices match
    std::sort(got_prices.begin(), got_prices.end());
    for (int i = 0; i < row_count; ++i) {
        RC_ASSERT(got_prices[static_cast<size_t>(i)] == price_base + i);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 6.6: Property 4 — No data loss between phases
// Feature: incremental-flush, Property 4: insert A → flush → insert B → flush
// → SELECT returns A ∪ B
// **Validates: Requirements 3.3, 9.4**
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IncrementalFlushProp, prop_no_data_loss_between_phases, ()) {
    const auto count_a = *rc::gen::inRange(1, 100);
    const auto count_b = *rc::gen::inRange(1, 100);

    TempDir tmp("iflush_prop4_");
    EngineGuard eg(tmp.path);

    // Insert batch A
    insert_rows(eg.engine, count_a, 1'000'000'000ULL, 1, 10000);

    // Flush A
    eg.engine.flush_incremental();

    // Insert batch B (different timestamps/sequences)
    insert_rows(eg.engine, count_b,
                2'000'000'000ULL,
                static_cast<uint64_t>(count_a + 1),
                20000);

    // Flush B
    eg.engine.flush_incremental();

    // Verify A ∪ B
    int total = query_row_count(eg.engine);
    RC_ASSERT(total == count_a + count_b);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 6.7: Property 5 — Crash recovery
// Feature: incremental-flush, Property 5: insert → flush → close → open →
// query = insert → flush → query
// **Validates: Requirements 1.3, 6.1, 6.2**
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IncrementalFlushProp, prop_crash_recovery_roundtrip, ()) {
    const auto row_count = *rc::gen::inRange(1, 50);
    const auto price_base = *rc::gen::inRange<int64_t>(1000, 50000);

    TempDir tmp("iflush_prop5_");

    // Insert → flush → close
    {
        EngineGuard eg(tmp.path);
        for (int i = 0; i < row_count; ++i) {
            ob::DeltaUpdate delta{};
            std::memset(delta.symbol, 0, sizeof(delta.symbol));
            std::memset(delta.exchange, 0, sizeof(delta.exchange));
            std::strncpy(delta.symbol, "SYM", sizeof(delta.symbol) - 1);
            std::strncpy(delta.exchange, "EXC", sizeof(delta.exchange) - 1);
            delta.sequence_number = static_cast<uint64_t>(i + 1);
            delta.timestamp_ns    = 1'000'000'000ULL + static_cast<uint64_t>(i) * 1'000'000ULL;
            delta.side            = ob::SIDE_BID;
            delta.n_levels        = 1;

            ob::Level lvl{};
            lvl.price = price_base + i;
            lvl.qty   = 100 + static_cast<uint64_t>(i);
            lvl.cnt   = 1;
            lvl._pad  = 0;

            ob::ob_status_t st = eg.engine.apply_delta(delta, &lvl);
            RC_ASSERT(st == ob::OB_OK);
        }
        eg.engine.flush_incremental();
        // EngineGuard destructor calls close()
    }

    // Reopen → query
    {
        EngineGuard eg2(tmp.path);
        int count = query_row_count(eg2.engine);
        RC_ASSERT(count == row_count);

        // Verify prices
        auto prices = query_prices(eg2.engine);
        std::sort(prices.begin(), prices.end());
        for (int i = 0; i < row_count; ++i) {
            RC_ASSERT(prices[static_cast<size_t>(i)] == price_base + i);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 6.8: Property 7 — Backward compat close()
// Feature: incremental-flush, Property 7: insert → close → open → query
// returns all rows
// **Validates: Requirements 8.3**
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IncrementalFlushProp, prop_close_full_flush, ()) {
    const auto row_count = *rc::gen::inRange(1, 50);
    const auto price_base = *rc::gen::inRange<int64_t>(1000, 50000);

    TempDir tmp("iflush_prop7_");

    // Insert → close (no explicit flush_incremental)
    {
        EngineGuard eg(tmp.path);
        for (int i = 0; i < row_count; ++i) {
            ob::DeltaUpdate delta{};
            std::memset(delta.symbol, 0, sizeof(delta.symbol));
            std::memset(delta.exchange, 0, sizeof(delta.exchange));
            std::strncpy(delta.symbol, "SYM", sizeof(delta.symbol) - 1);
            std::strncpy(delta.exchange, "EXC", sizeof(delta.exchange) - 1);
            delta.sequence_number = static_cast<uint64_t>(i + 1);
            delta.timestamp_ns    = 1'000'000'000ULL + static_cast<uint64_t>(i) * 1'000'000ULL;
            delta.side            = ob::SIDE_BID;
            delta.n_levels        = 1;

            ob::Level lvl{};
            lvl.price = price_base + i;
            lvl.qty   = 100 + static_cast<uint64_t>(i);
            lvl.cnt   = 1;
            lvl._pad  = 0;

            ob::ob_status_t st = eg.engine.apply_delta(delta, &lvl);
            RC_ASSERT(st == ob::OB_OK);
        }
        // close() via destructor — should flush all pending rows
    }

    // Reopen → query
    {
        EngineGuard eg2(tmp.path);
        int count = query_row_count(eg2.engine);
        RC_ASSERT(count == row_count);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
// Task 7.1: Property 3 — Concurrent INSERT and SELECT during flush
// Feature: incremental-flush, Property 3: INSERT and SELECT concurrent with
// flush_incremental() succeed without errors or crashes
// **Validates: Requirements 3.1, 4.1**
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IncrementalFlushProp, prop_concurrent_insert_select_during_flush, ()) {
    const auto insert_count = *rc::gen::inRange(10, 100);
    const auto flush_count  = *rc::gen::inRange(1, 5);

    TempDir tmp("iflush_prop3_");
    // Use short flush interval so background flush thread is active
    EngineGuard eg(tmp.path, 50'000'000ULL); // 50ms

    // Pre-insert some rows so SELECT has data to read
    insert_rows(eg.engine, 10, 500'000'000ULL, 1, 5000);
    eg.engine.flush_incremental();

    std::atomic<bool> done{false};
    std::atomic<int> insert_ok{0};
    std::atomic<int> select_ok{0};
    std::atomic<int> flush_ok{0};

    // INSERT thread
    std::thread inserter([&]() {
        for (int i = 0; i < insert_count && !done.load(std::memory_order_relaxed); ++i) {
            ob::DeltaUpdate delta{};
            std::memset(delta.symbol, 0, sizeof(delta.symbol));
            std::memset(delta.exchange, 0, sizeof(delta.exchange));
            std::strncpy(delta.symbol, "SYM", sizeof(delta.symbol) - 1);
            std::strncpy(delta.exchange, "EXC", sizeof(delta.exchange) - 1);
            delta.sequence_number = 100 + static_cast<uint64_t>(i);
            delta.timestamp_ns    = 3'000'000'000ULL + static_cast<uint64_t>(i) * 1'000'000ULL;
            delta.side            = ob::SIDE_BID;
            delta.n_levels        = 1;

            ob::Level lvl{};
            lvl.price = 30000 + i;
            lvl.qty   = 50;
            lvl.cnt   = 1;
            lvl._pad  = 0;

            ob::ob_status_t st = eg.engine.apply_delta(delta, &lvl);
            if (st == ob::OB_OK) {
                insert_ok.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    // SELECT thread
    std::thread selector([&]() {
        for (int i = 0; i < insert_count && !done.load(std::memory_order_relaxed); ++i) {
            int cnt = 0;
            std::string err = eg.engine.execute(
                "SELECT * FROM 'SYM'.'EXC' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
                [&](const ob::QueryResult&) { ++cnt; });
            if (err.empty()) {
                select_ok.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    // FLUSH thread
    std::thread flusher([&]() {
        for (int i = 0; i < flush_count; ++i) {
            eg.engine.flush_incremental();
            flush_ok.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    inserter.join();
    selector.join();
    flusher.join();
    done.store(true, std::memory_order_relaxed);

    // All INSERTs should succeed
    RC_ASSERT(insert_ok.load() == insert_count);
    // All SELECTs should succeed (no errors)
    RC_ASSERT(select_ok.load() == insert_count);
    // All flushes should succeed
    RC_ASSERT(flush_ok.load() == flush_count);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 7.2: Property 6 — WAL sequence monotonicity
// Feature: incremental-flush, Property 6: apply_delta interleaved with
// flush_incremental() → WAL replay has monotonic sequence_number
// **Validates: Requirements 6.3**
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IncrementalFlushProp, prop_wal_sequence_monotonic, ()) {
    const auto total_ops = *rc::gen::inRange(5, 80);
    // Decide how many flushes to interleave (at least 1)
    const auto flush_every = *rc::gen::inRange(3, 15);

    TempDir tmp("iflush_prop6_");

    {
        EngineGuard eg(tmp.path);

        uint64_t seq = 1;
        for (int i = 0; i < total_ops; ++i) {
            ob::DeltaUpdate delta{};
            std::memset(delta.symbol, 0, sizeof(delta.symbol));
            std::memset(delta.exchange, 0, sizeof(delta.exchange));
            std::strncpy(delta.symbol, "SYM", sizeof(delta.symbol) - 1);
            std::strncpy(delta.exchange, "EXC", sizeof(delta.exchange) - 1);
            delta.sequence_number = seq++;
            delta.timestamp_ns    = 1'000'000'000ULL + static_cast<uint64_t>(i) * 1'000'000ULL;
            delta.side            = ob::SIDE_BID;
            delta.n_levels        = 1;

            ob::Level lvl{};
            lvl.price = 10000 + i;
            lvl.qty   = 100;
            lvl.cnt   = 1;
            lvl._pad  = 0;

            ob::ob_status_t st = eg.engine.apply_delta(delta, &lvl);
            RC_ASSERT(st == ob::OB_OK);

            // Interleave flush
            if ((i + 1) % flush_every == 0) {
                eg.engine.flush_incremental();
            }
        }
        // Final flush
        eg.engine.flush_incremental();
        // close() via destructor
    }

    // Replay WAL and verify monotonic sequence numbers
    ob::WALReplayer replayer(tmp.path);
    std::vector<uint64_t> sequences;
    replayer.replay([&](const ob::WALRecord& rec, const uint8_t*) {
        if (rec.record_type == ob::WAL_RECORD_DELTA) {
            sequences.push_back(rec.sequence_number);
        }
    });

    RC_ASSERT(!sequences.empty());

    // Verify monotonically increasing
    for (size_t i = 1; i < sequences.size(); ++i) {
        RC_ASSERT(sequences[i] > sequences[i - 1]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 7.3: Property 8 — Snapshot completeness
// Feature: incremental-flush, Property 8: insert → flush_incremental →
// create_snapshot → sum of row_count = number of inserted rows
// **Validates: Requirements 8.4**
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IncrementalFlushProp, prop_snapshot_completeness, ()) {
    const auto row_count = *rc::gen::inRange(1, 50);

    TempDir tmp("iflush_prop8_");
    EngineGuard eg(tmp.path);

    // Insert rows
    insert_rows(eg.engine, row_count);

    // Flush incrementally
    eg.engine.flush_incremental();

    // Create snapshot
    ob::SnapshotManifest manifest = eg.engine.create_snapshot();

    // Verify total_rows matches inserted count
    RC_ASSERT(manifest.total_rows == static_cast<size_t>(row_count));
}
