// Feature: flush-race-duplicate-segments — flush paths must not duplicate or lose segments
//
// Every test here covers the same class of defect: a segment written to disk whose
// SegmentMeta either reaches combined_store_ twice (rows read double) or never
// reaches it at all (rows invisible to SELECT, since QueryEngine reads only the
// combined store and never the live SoA buffer).

#include "orderbook/engine.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

static std::atomic<uint64_t> g_dir_counter{0};

std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() /
               (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
    fs::create_directories(tmp);
    return tmp.string();
}

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix) : path(make_temp_dir(prefix)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

struct EngineGuard {
    ob::Engine engine;
    explicit EngineGuard(const std::string& d, uint64_t flush_ns)
        : engine(d, flush_ns, ob::FsyncPolicy::NONE) {
        engine.open();
    }
    ~EngineGuard() { engine.close(); }
    EngineGuard(const EngineGuard&) = delete;
    EngineGuard& operator=(const EngineGuard&) = delete;
};

/// One row, with a caller-chosen timestamp. Segment rollover is driven by the
/// timestamp, not by wall-clock time, so a test can cross an hour boundary
/// without waiting for one.
void insert_row(ob::Engine& engine, uint64_t ts_ns, uint64_t seq, int64_t price,
                const char* symbol = "RACE", const char* exchange = "EXC") {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, exchange, sizeof(delta.exchange) - 1);
    delta.sequence_number = seq;
    delta.timestamp_ns    = ts_ns;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = 1;

    ob::Level lvl{};
    lvl.price = price;
    lvl.qty   = 100;
    lvl.cnt   = 1;
    lvl._pad  = 0;

    ASSERT_EQ(engine.apply_delta(delta, &lvl), ob::OB_OK);
}

int query_row_count(ob::Engine& engine, const char* symbol = "RACE",
                    const char* exchange = "EXC") {
    int count = 0;
    std::string sql = std::string("SELECT * FROM '") + symbol + "'.'" + exchange +
                      "' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    std::string err = engine.execute(sql, [&](const ob::QueryResult&) { ++count; });
    EXPECT_TRUE(err.empty()) << "Query error: " << err;
    return count;
}

constexpr uint64_t kHourNs = 3600ULL * 1'000'000'000ULL;

} // anonymous namespace

// ═══════════════════════════════════════════════════════════════════════════════
// D1: two threads flushing at once must not register the same segment twice.
//
// flush_write_and_merge() runs without mtx_ by design, and ColumnarStore has no
// lock over its active-segment state, so both callers could see the same active
// segment, both write the same directory and both merge the same meta.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(FlushRace, ConcurrentFlushDoesNotDuplicateRows) {
    TempDir tmp("flush_race_dup_");
    // 1ms flush interval: the background thread flushes constantly, so an
    // explicit flush has every chance of landing in the same window.
    EngineGuard eg(tmp.path, 1'000'000ULL);

    constexpr int kRounds        = 60;
    constexpr int kRowsPerRound  = 3;
    uint64_t seq = 1;

    for (int round = 0; round < kRounds; ++round) {
        const uint64_t ts_base = 1'000'000'000ULL + static_cast<uint64_t>(round) * 1'000'000ULL;
        for (int i = 0; i < kRowsPerRound; ++i) {
            insert_row(eg.engine, ts_base + static_cast<uint64_t>(i) * 1000ULL, seq++,
                       10'000 + i);
        }

        // Two explicit flushes, released together, plus the background thread.
        std::atomic<bool> go{false};
        auto flusher = [&]() {
            while (!go.load(std::memory_order_acquire)) { /* spin to align */ }
            eg.engine.flush_incremental();
        };
        std::thread a(flusher);
        std::thread b(flusher);
        go.store(true, std::memory_order_release);
        a.join();
        b.join();

        const int expected = (round + 1) * kRowsPerRound;
        const int got      = query_row_count(eg.engine);
        ASSERT_EQ(got, expected)
            << "round " << round << ": concurrent flush changed the row count. "
            << "More than expected means one segment was merged twice; fewer means "
            << "a segment was written but never merged.";

        // The row count alone cannot verify the lock: merge_segments() refuses a
        // segment whose directory is already indexed, so a race that did happen
        // still yields the right count. This counter is what distinguishes
        // "prevented" from "caught after the fact".
        ASSERT_EQ(eg.engine.stats().segment_merge_refused, 0u)
            << "round " << round << ": two flush paths produced the same segment. "
            << "The index check kept the rows correct, but flush_mtx_ should have "
            << "made the duplicate impossible in the first place.";
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// D4: a segment rolled over inside append() must stay visible to SELECT.
//
// ColumnarStore::append() calls flush_segment() when a row crosses the segment
// duration and drops the returned meta on the floor.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(FlushRace, RolledOverSegmentStaysVisibleToQueries) {
    TempDir tmp("flush_race_roll_");
    // 10s interval: the background thread must not flush between the two batches,
    // because the rollover only happens if both are drained in one pass.
    EngineGuard eg(tmp.path, 10'000'000'000ULL);

    // apply_delta() only parks rows in pending_rows_; append() runs during the
    // drain. So a rollover needs both batches pending at the same time, with
    // timestamps more than the 1h segment duration apart — flushing in between
    // would empty the buffers and leave the rollover nothing to discard.
    const uint64_t t0 = 2 * kHourNs;
    for (int i = 0; i < 3; ++i) {
        insert_row(eg.engine, t0 + static_cast<uint64_t>(i) * 1'000'000ULL, 1 + i, 10'000 + i);
    }
    const uint64_t t1 = t0 + 2 * kHourNs;
    for (int i = 0; i < 3; ++i) {
        insert_row(eg.engine, t1 + static_cast<uint64_t>(i) * 1'000'000ULL, 10 + i, 20'000 + i);
    }

    eg.engine.flush_incremental();

    EXPECT_EQ(query_row_count(eg.engine), 6)
        << "rows from the rolled-over segment are missing: append() flushed it to "
           "disk and discarded the SegmentMeta, so combined_store_ never learned of it";
}

// ═══════════════════════════════════════════════════════════════════════════════
// D3: SNAPSHOT must not hide rows it flushed on the way.
//
// create_snapshot() drains pending rows, calls flush_segment() and ignores the
// returned meta.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(FlushRace, SnapshotKeepsFlushedRowsQueryable) {
    TempDir tmp("flush_race_snap_");
    EngineGuard eg(tmp.path, 10'000'000'000ULL);

    for (int i = 0; i < 5; ++i) {
        insert_row(eg.engine, 5'000'000'000ULL + static_cast<uint64_t>(i) * 1'000'000ULL,
                   1 + i, 30'000 + i);
    }

    // No explicit flush: the snapshot itself is what pushes these rows to a segment.
    (void)eg.engine.create_snapshot();

    EXPECT_EQ(query_row_count(eg.engine), 5)
        << "SNAPSHOT wrote the segment but dropped its SegmentMeta, so the rows it "
           "just persisted became invisible to SELECT";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Defence in depth: merging the same segment twice must not double its rows.
//
// This is not the fix — the fix is serialising the flush paths — but if the race
// ever returns by another route, we want an error in the log rather than doubled
// rows at the client.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(FlushRace, MergeSegmentsIgnoresASegmentAlreadyInTheIndex) {
    TempDir tmp("flush_race_merge_");

    ob::ColumnarStore writer(tmp.path);
    writer.set_symbol_exchange("MERGE", "EXC");
    for (int i = 0; i < 4; ++i) {
        ob::SnapshotRow row{};
        row.timestamp_ns    = 7'000'000'000ULL + static_cast<uint64_t>(i) * 1000ULL;
        row.price           = 40'000 + i;
        row.quantity        = 10;
        row.order_count     = 1;
        row.side            = ob::SIDE_BID;
        row.level_index     = static_cast<uint16_t>(i);
        row.sequence_number = 1 + static_cast<uint64_t>(i);
        writer.append(row);
    }
    auto meta = writer.flush_segment();
    ASSERT_TRUE(meta.has_value());

    ob::ColumnarStore index(tmp.path);
    index.merge_segments({meta.value()});
    const size_t after_first = index.segment_count();
    index.merge_segments({meta.value()});

    EXPECT_EQ(index.segment_count(), after_first)
        << "the same segment directory was registered twice, so every row in it "
           "would be scanned twice";
}
