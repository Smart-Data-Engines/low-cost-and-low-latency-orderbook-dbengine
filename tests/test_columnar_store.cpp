// Tests for ColumnarStore: property-based tests (Properties 7–9, 13) and unit tests.
// Feature: orderbook-dbengine

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "orderbook/column_codec.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"

namespace fs = std::filesystem;

// ── Test helpers ──────────────────────────────────────────────────────────────

namespace {

static std::atomic<uint64_t> g_dir_counter{0};

static fs::path make_temp_dir(const std::string& suffix = "") {
    auto base = fs::temp_directory_path() /
                ("ob_col_test_" + suffix + "_" +
                 std::to_string(g_dir_counter.fetch_add(1)));
    fs::create_directories(base);
    return base;
}

struct TempDir {
    fs::path path;
    explicit TempDir(const std::string& suffix = "")
        : path(make_temp_dir(suffix)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

// side and level_index are parameters, not constants. They used to be hard-coded
// to SIDE_BID and 0 here, which meant no test in this file ever changed them —
// and the read path zeroed both without a single failure to show for it.
static ob::SnapshotRow make_row(uint64_t ts_ns, int64_t price = 10000,
                                 uint64_t qty = 100, uint32_t cnt = 1,
                                 uint8_t side = ob::SIDE_BID,
                                 uint16_t level = 0) {
    ob::SnapshotRow row{};
    row.timestamp_ns    = ts_ns;
    row.sequence_number = ts_ns / 1000;
    row.side            = side;
    row.level_index     = level;
    row.price           = price;
    row.quantity        = qty;
    row.order_count     = cnt;
    return row;
}

/// Collect every row a store returns for the full time range.
static std::vector<ob::SnapshotRow> scan_all(const ob::ColumnarStore& store) {
    std::vector<ob::SnapshotRow> out;
    store.scan(0, UINT64_MAX, "", "", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { out.push_back(r); });
    return out;
}

/// Write segments in `version` (segment format v3): what a test of format 2's files asks for.
static void use_format(ob::ColumnarStore& store, uint32_t version) {
    auto format = store.segment_format();
    format.version = version;
    store.set_segment_format(format);
}

// Build a ColumnarStore with a known symbol/exchange by using a subdir approach.
// Since SnapshotRow doesn't carry symbol/exchange, we set them on the store directly
// by using a base_dir that already encodes symbol/exchange.
// The store's segment_dir uses symbol_ and exchange_ which default to "".
// For tests we use the store with empty symbol/exchange (the default).

} // anonymous namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Property 7: Segment time boundary partitioning
// Feature: orderbook-dbengine, Property 7: Segment time boundary partitioning
// For any sequence of rows spanning more than one segment duration, the columnar
// store should create a separate segment for each time interval, with each
// segment's metadata recording the correct min and max timestamps.
// Validates: Requirements 4.2, 4.4, 4.5
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ColumnarStoreProperty, prop_segment_partitioning, ()) {
    TempDir tmp("seg_part");

    // Use a small segment duration so we can easily span multiple segments.
    const uint64_t seg_dur = 1000ULL; // 1000 ns per segment
    const auto n_segments  = *rc::gen::inRange<int>(2, 5);
    const auto rows_per_seg = *rc::gen::inRange<int>(1, 10);

    ob::ColumnarStore store(tmp.str(), seg_dur);

    // Generate rows: each segment gets rows_per_seg rows with timestamps
    // within that segment's window.
    std::vector<ob::SnapshotRow> all_rows;
    for (int seg = 0; seg < n_segments; ++seg) {
        uint64_t seg_base = static_cast<uint64_t>(seg) * seg_dur;
        for (int r = 0; r < rows_per_seg; ++r) {
            uint64_t ts = seg_base + static_cast<uint64_t>(r);
            all_rows.push_back(make_row(ts, 10000 + seg * 100 + r));
        }
    }

    for (const auto& row : all_rows) {
        store.append(row);
    }
    store.flush_segment();

    // Should have exactly n_segments segments
    RC_ASSERT(static_cast<int>(store.segment_count()) == n_segments);

    // Each segment's start_ts_ns and end_ts_ns must be within its window
    const auto idx = store.index();
    for (int seg = 0; seg < n_segments; ++seg) {
        uint64_t expected_start = static_cast<uint64_t>(seg) * seg_dur;
        uint64_t expected_end   = expected_start + static_cast<uint64_t>(rows_per_seg - 1);

        // Find the segment that covers this window
        bool found = false;
        for (const auto& meta : idx) {
            if (meta.start_ts_ns == expected_start) {
                RC_ASSERT(meta.end_ts_ns == expected_end);
                RC_ASSERT(meta.row_count == static_cast<uint64_t>(rows_per_seg));
                found = true;
                break;
            }
        }
        RC_ASSERT(found);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 8: Append-only segment growth
// Feature: orderbook-dbengine, Property 8: Append-only segment growth
// For any sequence of appends to an active segment, the segment file sizes
// should be monotonically non-decreasing.
// Validates: Requirements 4.3
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ColumnarStoreProperty, prop_segment_append_only, ()) {
    TempDir tmp("append_only");

    // Use a large segment duration so all rows go into one segment.
    const uint64_t seg_dur = 1ULL << 60; // effectively infinite
    const auto n = *rc::gen::inRange<int>(2, 50);

    ob::ColumnarStore store(tmp.str(), seg_dur);
    // Format 2's files, one a column: what grows with the rows. Format 3's one file is held to what
    // it says of itself further down.
    use_format(store, ob::kColumnarFormatV2);

    // Track file sizes after each flush
    std::vector<uintmax_t> price_sizes;
    std::vector<uintmax_t> qty_sizes;
    std::vector<uintmax_t> ts_sizes;
    std::vector<uintmax_t> cnt_sizes;

    // Append rows one at a time, flushing after each to observe growth.
    // We use a fresh store each time to simulate incremental flushes.
    // Actually, flush_segment() resets the active segment, so we need to
    // accumulate and check the final file sizes grow with more rows.
    // Instead: append all rows, flush once, check sizes > 0.
    for (int i = 0; i < n; ++i) {
        store.append(make_row(static_cast<uint64_t>(i) * 100ULL,
                              10000 + i, static_cast<uint64_t>(i + 1)));
    }
    store.flush_segment();

    RC_ASSERT(store.segment_count() == 1u);

    const auto meta = store.index()[0];
    const std::string& dir = meta.dir_path;

    // All column files must exist and be non-empty
    auto file_size = [&](const std::string& name) -> uintmax_t {
        std::error_code ec;
        auto sz = fs::file_size(dir + "/" + name, ec);
        return ec ? 0 : sz;
    };

    RC_ASSERT(file_size("price.col") > uintmax_t{0});
    RC_ASSERT(file_size("qty.col")   > uintmax_t{0});
    RC_ASSERT(file_size("ts.col")    > uintmax_t{0});
    RC_ASSERT(file_size("cnt.col")   > uintmax_t{0});

    // Verify that appending more rows to a new store with same base produces
    // a larger (or equal) file — monotonically non-decreasing.
    uintmax_t ts_size_n = file_size("ts.col");

    // Create a second store with 2*n rows
    TempDir tmp2("append_only2");
    ob::ColumnarStore store2(tmp2.str(), seg_dur);
    use_format(store2, ob::kColumnarFormatV2);
    for (int i = 0; i < n * 2; ++i) {
        store2.append(make_row(static_cast<uint64_t>(i) * 100ULL,
                               10000 + i, static_cast<uint64_t>(i + 1)));
    }
    store2.flush_segment();

    RC_ASSERT(store2.segment_count() == 1u);
    const auto meta2 = store2.index()[0];
    uintmax_t ts_size_2n = fs::file_size(meta2.dir_path + "/ts.col");

    // More rows → larger or equal file
    RC_ASSERT(ts_size_2n >= ts_size_n);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 9: Columnar insertion order preservation
// Feature: orderbook-dbengine, Property 9: Columnar insertion order preservation
// For any sequence of N rows written to a segment, reading back those rows
// should return them in the same insertion order.
// Validates: Requirements 4.6
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ColumnarStoreProperty, prop_insertion_order, ()) {
    TempDir tmp("ins_order");

    const uint64_t seg_dur = 1ULL << 60;
    const auto n = *rc::gen::inRange<int>(1, 50);

    ob::ColumnarStore store(tmp.str(), seg_dur);

    // Generate rows with strictly increasing timestamps and distinct prices
    std::vector<ob::SnapshotRow> written;
    for (int i = 0; i < n; ++i) {
        auto row = make_row(static_cast<uint64_t>(i) * 1000ULL,
                            10000 + i * 7,
                            static_cast<uint64_t>(i + 1) * 5,
                            static_cast<uint32_t>(i + 1));
        written.push_back(row);
        store.append(row);
    }
    store.flush_segment();

    // Scan back all rows
    std::vector<ob::SnapshotRow> read_back;
    store.scan(0, UINT64_MAX, "", "", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { read_back.push_back(r); });

    RC_ASSERT(static_cast<int>(read_back.size()) == n);

    // Verify insertion order: timestamps must match in order
    for (int i = 0; i < n; ++i) {
        RC_ASSERT(read_back[static_cast<size_t>(i)].timestamp_ns ==
                  written[static_cast<size_t>(i)].timestamp_ns);
        RC_ASSERT(read_back[static_cast<size_t>(i)].price ==
                  written[static_cast<size_t>(i)].price);
        RC_ASSERT(read_back[static_cast<size_t>(i)].quantity ==
                  written[static_cast<size_t>(i)].quantity);
        RC_ASSERT(read_back[static_cast<size_t>(i)].order_count ==
                  written[static_cast<size_t>(i)].order_count);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 13: Restart data durability
// Feature: orderbook-dbengine, Property 13: Restart data durability
// For any set of rows written and flushed before a simulated restart, all rows
// should be accessible after reopening the store with the segment index rebuilt.
// Validates: Requirements 7.3, 7.4
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ColumnarStoreProperty, prop_restart_durability, ()) {
    TempDir tmp("restart");

    const uint64_t seg_dur = 1ULL << 60;
    const auto n = *rc::gen::inRange<int>(1, 30);

    std::vector<ob::SnapshotRow> written;

    // Write and flush
    {
        ob::ColumnarStore store(tmp.str(), seg_dur);
        for (int i = 0; i < n; ++i) {
            auto row = make_row(static_cast<uint64_t>(i) * 1000ULL,
                                10000 + i * 3,
                                static_cast<uint64_t>(i + 1),
                                static_cast<uint32_t>(i + 1));
            written.push_back(row);
            store.append(row);
        }
        store.flush_segment();
        // store goes out of scope — simulates restart
    }

    // Reopen and rebuild index
    ob::ColumnarStore store2(tmp.str(), seg_dur);
    store2.open_existing();

    RC_ASSERT(store2.segment_count() == 1u);

    // Scan and verify all rows present
    std::vector<ob::SnapshotRow> recovered;
    store2.scan(0, UINT64_MAX, "", "", ob::ColumnSet::all(),
                [&](const ob::SnapshotRow& r) { recovered.push_back(r); });

    RC_ASSERT(static_cast<int>(recovered.size()) == n);
    for (int i = 0; i < n; ++i) {
        RC_ASSERT(recovered[static_cast<size_t>(i)].timestamp_ns ==
                  written[static_cast<size_t>(i)].timestamp_ns);
        RC_ASSERT(recovered[static_cast<size_t>(i)].price ==
                  written[static_cast<size_t>(i)].price);
        RC_ASSERT(recovered[static_cast<size_t>(i)].quantity ==
                  written[static_cast<size_t>(i)].quantity);
        RC_ASSERT(recovered[static_cast<size_t>(i)].order_count ==
                  written[static_cast<size_t>(i)].order_count);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests
// ═══════════════════════════════════════════════════════════════════════════════

// Single segment: append rows, scan back, verify round-trip.
TEST(ColumnarStore, SingleSegmentAppendScan) {
    TempDir tmp("ut_single");
    ob::ColumnarStore store(tmp.str(), 1ULL << 60);

    std::vector<ob::SnapshotRow> rows;
    for (int i = 0; i < 5; ++i) {
        auto r = make_row(static_cast<uint64_t>(i) * 1000ULL,
                          10000 + i * 100,
                          static_cast<uint64_t>(i + 1) * 10,
                          static_cast<uint32_t>(i + 1));
        rows.push_back(r);
        store.append(r);
    }
    store.flush_segment();

    ASSERT_EQ(store.segment_count(), 1u);

    std::vector<ob::SnapshotRow> out;
    store.scan(0, UINT64_MAX, "", "", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { out.push_back(r); });

    ASSERT_EQ(out.size(), rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(out[i].timestamp_ns, rows[i].timestamp_ns);
        EXPECT_EQ(out[i].price,        rows[i].price);
        EXPECT_EQ(out[i].quantity,     rows[i].quantity);
        EXPECT_EQ(out[i].order_count,  rows[i].order_count);
    }
}

// Segment rollover: rows spanning two segment durations → two segments.
TEST(ColumnarStore, SegmentRolloverAtTimeBoundary) {
    TempDir tmp("ut_rollover");
    const uint64_t seg_dur = 1000ULL;
    ob::ColumnarStore store(tmp.str(), seg_dur);

    // 3 rows in segment 0 (ts 0, 100, 200)
    store.append(make_row(0,   10000, 1));
    store.append(make_row(100, 10001, 2));
    store.append(make_row(200, 10002, 3));

    // 2 rows in segment 1 (ts 1000, 1100)
    store.append(make_row(1000, 20000, 4));
    store.append(make_row(1100, 20001, 5));

    store.flush_segment();

    ASSERT_EQ(store.segment_count(), 2u);

    const auto idx = store.index();
    EXPECT_EQ(idx[0].row_count, 3u);
    EXPECT_EQ(idx[0].start_ts_ns, 0u);
    EXPECT_EQ(idx[0].end_ts_ns, 200u);

    EXPECT_EQ(idx[1].row_count, 2u);
    EXPECT_EQ(idx[1].start_ts_ns, 1000u);
    EXPECT_EQ(idx[1].end_ts_ns, 1100u);
}

// open_existing: write, close, reopen, verify index rebuilt.
TEST(ColumnarStore, OpenExistingRebuildsIndex) {
    TempDir tmp("ut_reopen");
    const uint64_t seg_dur = 1ULL << 60;

    {
        ob::ColumnarStore store(tmp.str(), seg_dur);
        store.append(make_row(100, 50000, 10, 2));
        store.append(make_row(200, 50001, 11, 3));
        store.flush_segment();
    }

    ob::ColumnarStore store2(tmp.str(), seg_dur);
    store2.open_existing();

    ASSERT_EQ(store2.segment_count(), 1u);
    EXPECT_EQ(store2.index()[0].row_count, 2u);
    // start_ts_ns is the segment boundary (rounded down), not the first row ts
    EXPECT_EQ(store2.index()[0].end_ts_ns, 200u);

    // Scan and verify data
    std::vector<ob::SnapshotRow> out;
    store2.scan(0, UINT64_MAX, "", "", ob::ColumnSet::all(),
                [&](const ob::SnapshotRow& r) { out.push_back(r); });

    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].price, 50000);
    EXPECT_EQ(out[1].price, 50001);
}

// Scan with time-range pruning: only rows in [start, end] returned.
TEST(ColumnarStore, ScanWithTimeRangePruning) {
    TempDir tmp("ut_pruning");
    const uint64_t seg_dur = 1000ULL;
    ob::ColumnarStore store(tmp.str(), seg_dur);

    // Segment 0: ts 0..200
    store.append(make_row(0,   10000, 1));
    store.append(make_row(100, 10001, 2));
    store.append(make_row(200, 10002, 3));

    // Segment 1: ts 1000..1200
    store.append(make_row(1000, 20000, 4));
    store.append(make_row(1100, 20001, 5));
    store.append(make_row(1200, 20002, 6));

    store.flush_segment();
    ASSERT_EQ(store.segment_count(), 2u);

    // Scan only segment 0 range
    {
        std::vector<ob::SnapshotRow> out;
        store.scan(0, 500, "", "", ob::ColumnSet::all(),
                   [&](const ob::SnapshotRow& r) { out.push_back(r); });
        ASSERT_EQ(out.size(), 3u);
        for (const auto& r : out) {
            EXPECT_LE(r.timestamp_ns, 500u);
        }
    }

    // Scan only segment 1 range
    {
        std::vector<ob::SnapshotRow> out;
        store.scan(1000, 2000, "", "", ob::ColumnSet::all(),
                   [&](const ob::SnapshotRow& r) { out.push_back(r); });
        ASSERT_EQ(out.size(), 3u);
        for (const auto& r : out) {
            EXPECT_GE(r.timestamp_ns, 1000u);
        }
    }

    // Scan a range that covers neither segment
    {
        std::vector<ob::SnapshotRow> out;
        store.scan(500, 999, "", "", ob::ColumnSet::all(),
                   [&](const ob::SnapshotRow& r) { out.push_back(r); });
        EXPECT_EQ(out.size(), 0u);
    }
}

// Corrupted meta.json: open_existing should skip invalid entries gracefully.
TEST(ColumnarStore, CorruptedMetaJsonHandling) {
    TempDir tmp("ut_corrupt");
    const uint64_t seg_dur = 1ULL << 60;

    // Write a valid segment first
    {
        ob::ColumnarStore store(tmp.str(), seg_dur);
        store.append(make_row(100, 10000, 5, 1));
        store.flush_segment();
    }

    // Find the meta.json and corrupt it
    for (auto& entry : fs::recursive_directory_iterator(tmp.path)) {
        if (entry.path().filename() == "meta.json") {
            std::ofstream f(entry.path().string(), std::ios::out | std::ios::trunc);
            f << "NOT VALID JSON {{{";
            break;
        }
    }

    // open_existing should not throw; it should skip the corrupted entry
    ob::ColumnarStore store2(tmp.str(), seg_dur);
    EXPECT_NO_THROW(store2.open_existing());
    // The corrupted segment should be skipped (row_count=0 → parse returns false)
    EXPECT_EQ(store2.segment_count(), 0u);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 1.5: test_flush_segment_returns_meta
// Feature: incremental-flush
// flush_segment() returns SegmentMeta with correct fields; returns std::nullopt
// when no active segment.
// Validates: Requirements 2.1
// ═══════════════════════════════════════════════════════════════════════════════

TEST(ColumnarStore, test_flush_segment_returns_meta) {
    TempDir tmp("ut_flush_meta");
    const uint64_t seg_dur = 1ULL << 60;
    ob::ColumnarStore store(tmp.str(), seg_dur);

    // --- No active segment → std::nullopt ---
    {
        auto result = store.flush_segment();
        EXPECT_FALSE(result.has_value());
    }

    // --- Append rows with known symbol/exchange, flush, verify meta ---
    store.set_symbol_exchange("BTCUSD", "BINANCE");

    store.append(make_row(1000, 50000, 10, 2));
    store.append(make_row(2000, 50100, 20, 3));
    store.append(make_row(3000, 50200, 30, 4));

    auto result = store.flush_segment();
    ASSERT_TRUE(result.has_value());

    const ob::SegmentMeta& meta = result.value();
    // The first row's time. This said 0, "rounded down" to the segment's period, until #166:
    // that start and a last-row end put a row written out of time order outside the range
    // queries prune by. test_segment_time_range.cpp holds what the range is now.
    EXPECT_EQ(meta.start_ts_ns, 1000u);
    EXPECT_EQ(meta.end_ts_ns, 3000u);
    EXPECT_EQ(meta.row_count, 3u);
    EXPECT_EQ(meta.symbol, "BTCUSD");
    EXPECT_EQ(meta.exchange, "BINANCE");
    EXPECT_FALSE(meta.dir_path.empty());
    EXPECT_TRUE(fs::exists(meta.dir_path));

    // --- After flush, no active segment → std::nullopt again ---
    {
        auto result2 = store.flush_segment();
        EXPECT_FALSE(result2.has_value());
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 1.6: test_merge_segments_empty
// Feature: incremental-flush
// merge_segments({}) does not change the index.
// Validates: Requirements 2.4
// ═══════════════════════════════════════════════════════════════════════════════

TEST(ColumnarStore, test_merge_segments_empty) {
    TempDir tmp("ut_merge_empty");
    const uint64_t seg_dur = 1ULL << 60;
    ob::ColumnarStore store(tmp.str(), seg_dur);

    // Append and flush to create one segment in the index
    store.set_symbol_exchange("ETHUSD", "KRAKEN");
    store.append(make_row(500, 30000, 5, 1));
    store.flush_segment();

    ASSERT_EQ(store.segment_count(), 1u);
    const auto idx_before = store.index();  // copy

    // merge_segments with empty vector — index must not change
    store.merge_segments({});

    ASSERT_EQ(store.segment_count(), 1u);
    const auto idx_after = store.index();
    EXPECT_EQ(idx_after[0].start_ts_ns, idx_before[0].start_ts_ns);
    EXPECT_EQ(idx_after[0].end_ts_ns,   idx_before[0].end_ts_ns);
    EXPECT_EQ(idx_after[0].row_count,   idx_before[0].row_count);
    EXPECT_EQ(idx_after[0].symbol,      idx_before[0].symbol);
    EXPECT_EQ(idx_after[0].exchange,    idx_before[0].exchange);
    EXPECT_EQ(idx_after[0].dir_path,    idx_before[0].dir_path);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 2: Index sort invariant
// Feature: incremental-flush, Property 2: Index sort invariant
// For any sequence of flush_segment() and merge_segments(), index_ is always
// sorted by start_ts_ns.
// Validates: Requirements 2.2
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ColumnarStoreProperty, prop_index_sorted_invariant, ()) {
    TempDir tmp("prop_idx_sort");
    const uint64_t seg_dur = 1000ULL;
    ob::ColumnarStore store(tmp.str(), seg_dur);
    store.set_symbol_exchange("SYM", "EXC");

    // Generate segments via append + flush with monotonically increasing
    // timestamps (the realistic scenario — market data arrives in time order).
    const auto n_flush_rounds = *rc::gen::inRange<int>(1, 6);
    uint64_t next_seg = 0;
    for (int round = 0; round < n_flush_rounds; ++round) {
        const auto seg_offset = *rc::gen::inRange<uint64_t>(0, 10);
        uint64_t seg_base = next_seg + seg_offset;
        next_seg = seg_base + 1;
        const auto n_rows = *rc::gen::inRange<int>(1, 5);
        for (int r = 0; r < n_rows; ++r) {
            uint64_t ts = seg_base * seg_dur + static_cast<uint64_t>(r);
            store.append(make_row(ts, 10000 + round * 100 + r));
        }
        store.flush_segment();

        // Invariant: index must be sorted after each flush
        const auto idx = store.index();
        for (size_t i = 1; i < idx.size(); ++i) {
            RC_ASSERT(idx[i - 1].start_ts_ns <= idx[i].start_ts_ns);
        }
    }

    // Now merge a random batch of external segments with arbitrary timestamps.
    // merge_segments() must re-sort the index.
    const auto n_merge = *rc::gen::inRange<int>(0, 5);
    std::vector<ob::SegmentMeta> external;
    for (int m = 0; m < n_merge; ++m) {
        const auto ts = *rc::gen::inRange<uint64_t>(0, 100000);
        ob::SegmentMeta seg{};
        seg.start_ts_ns = ts;
        seg.end_ts_ns   = ts + 100;
        seg.row_count   = 1;
        seg.first_price = 0;
        seg.has_raw_qty = false;
        seg.symbol      = "SYM";
        seg.exchange    = "EXC";
        seg.dir_path    = "/tmp/fake";
        external.push_back(seg);
    }
    store.merge_segments(external);

    // Invariant: index must still be sorted after merge
    const auto idx = store.index();
    for (size_t i = 1; i < idx.size(); ++i) {
        RC_ASSERT(idx[i - 1].start_ts_ns <= idx[i].start_ts_ns);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Side, level_index and sequence_number survive a flush
//
// Spec: kiro-workspace/specs/columnar-side-level-seq. Format version 1 stored
// only ts/price/qty/cnt and zeroed the other three fields on read, so every row
// that passed a flush came back as a bid at level 0 with sequence 0. The whole
// suite missed it because make_row() pinned side to SIDE_BID.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(ColumnarStoreFields, SideIsPreservedThroughFlush) {
    TempDir tmp("side_pres");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);

    store.append(make_row(1000, 100'000, 10, 1, ob::SIDE_BID, 0));
    store.append(make_row(2000, 101'000, 20, 1, ob::SIDE_ASK, 0));
    store.flush_segment();

    const auto rows = scan_all(store);
    ASSERT_EQ(rows.size(), 2u);

    // Match by price, because scan order is not part of the contract.
    const ob::SnapshotRow* bid = nullptr;
    const ob::SnapshotRow* ask = nullptr;
    for (const auto& r : rows) {
        if (r.price == 100'000) bid = &r;
        if (r.price == 101'000) ask = &r;
    }
    ASSERT_NE(bid, nullptr);
    ASSERT_NE(ask, nullptr);

    EXPECT_EQ(bid->side, ob::SIDE_BID);
    EXPECT_EQ(ask->side, ob::SIDE_ASK)
        << "the ask came back as side=" << static_cast<int>(ask->side)
        << "; order side is being lost on the way through the segment";
}

TEST(ColumnarStoreFields, LevelIndexIsPreserved) {
    TempDir tmp("level_pres");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);

    constexpr uint16_t kDepth = 5;
    for (uint16_t lvl = 0; lvl < kDepth; ++lvl) {
        store.append(make_row(1000 + lvl, 100'000 - lvl * 100, 10, 1,
                              ob::SIDE_BID, lvl));
    }
    store.flush_segment();

    const auto rows = scan_all(store);
    ASSERT_EQ(rows.size(), kDepth);

    std::vector<uint16_t> levels;
    for (const auto& r : rows) levels.push_back(r.level_index);
    std::sort(levels.begin(), levels.end());

    for (uint16_t lvl = 0; lvl < kDepth; ++lvl) {
        EXPECT_EQ(levels[lvl], lvl) << "book depth is not preserved";
    }
}

TEST(ColumnarStoreFields, SequenceNumberIsPreserved) {
    TempDir tmp("seq_pres");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);

    std::vector<uint64_t> expected;
    for (int i = 0; i < 10; ++i) {
        auto row = make_row(1000 + static_cast<uint64_t>(i));
        row.sequence_number = 5'000'000'000ULL + static_cast<uint64_t>(i);
        expected.push_back(row.sequence_number);
        store.append(row);
    }
    store.flush_segment();

    auto rows = scan_all(store);
    ASSERT_EQ(rows.size(), expected.size());

    std::vector<uint64_t> got;
    for (const auto& r : rows) got.push_back(r.sequence_number);
    std::sort(got.begin(), got.end());

    EXPECT_EQ(got, expected)
        << "sequence numbers are lost, so gaps in the update stream become undetectable";
}

TEST(ColumnarStoreFields, SequenceNumberHandlesNonMonotonic) {
    // Multi-master writes from different nodes can land in one segment with a
    // falling sequence. Simple8b cannot represent that, which is why the column
    // uses the zigzag-delta codec.
    TempDir tmp("seq_nonmono");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);

    const std::vector<uint64_t> seqs = {900, 100, 500, 42, 10'000, 7};
    for (size_t i = 0; i < seqs.size(); ++i) {
        auto row = make_row(1000 + static_cast<uint64_t>(i));
        row.sequence_number = seqs[i];
        store.append(row);
    }
    store.flush_segment();

    auto rows = scan_all(store);
    ASSERT_EQ(rows.size(), seqs.size());

    std::vector<uint64_t> got;
    for (const auto& r : rows) got.push_back(r.sequence_number);
    auto want = seqs;
    std::sort(got.begin(), got.end());
    std::sort(want.begin(), want.end());
    EXPECT_EQ(got, want);
}

// #198: a quantity of 2^60 - 1 was written as an ordinary Simple8b word, and that word is the
// codec's fallback marker, so the read took the next word for the value and every quantity after it
// in the segment moved. The same for a sequence number falling by 2^59 from one row to the next,
// whose zigzag delta is that value.
TEST(ColumnarStoreFields, TheCodecsMarkerValueIsPreserved) {
    TempDir tmp("marker_value");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);

    const uint64_t marker = (1ULL << 60) - 1;
    const std::vector<uint64_t> qtys = {marker, 5, 7, marker, 1};
    const std::vector<uint64_t> seqs = {(1ULL << 59) + 10, 10, 11, 12, 13};
    for (size_t i = 0; i < qtys.size(); ++i) {
        auto row = make_row(1000 + static_cast<uint64_t>(i), 10000, qtys[i]);
        row.sequence_number = seqs[i];
        store.append(row);
    }
    const auto meta = store.flush_segment();
    ASSERT_TRUE(meta.has_value());
    EXPECT_TRUE(meta->has_raw_qty);

    auto rows = scan_all(store);
    ASSERT_EQ(rows.size(), qtys.size());
    std::sort(rows.begin(), rows.end(), [](const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
        return a.timestamp_ns < b.timestamp_ns;
    });
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows[i].quantity, qtys[i]) << "row " << i;
        EXPECT_EQ(rows[i].sequence_number, seqs[i]) << "row " << i;
    }
}

TEST(ColumnarStoreFields, SegmentWithUnknownFormatVersionIsRejected) {
    // An older segment has no side/level/seq columns. Reading it anyway would
    // hand back zeroed fields, which is exactly the defect being fixed, so the
    // segment has to be skipped rather than partially trusted.
    TempDir tmp("bad_version");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);
    store.append(make_row(1000, 100'000, 10, 1, ob::SIDE_ASK, 3));
    store.flush_segment();

    ASSERT_EQ(scan_all(store).size(), 1u) << "sanity: the segment reads before tampering";

    // Rewrite meta.json with a version this build does not know.
    const auto dir = store.index().at(0).dir_path;
    {
        std::ifstream in(dir + "/meta.json");
        std::string meta((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
        in.close();
        const std::string from = "\"format_version\":" + std::to_string(ob::kColumnarFormatVersion);
        const auto pos = meta.find(from);
        ASSERT_NE(pos, std::string::npos) << "meta.json is missing format_version";
        meta.replace(pos, from.size(), "\"format_version\":99");
        std::ofstream out(dir + "/meta.json", std::ios::trunc);
        out << meta;
    }

    // Reopen so the tampered meta is what the index holds.
    ob::ColumnarStore reopened(tmp.str(), 1'000'000'000ULL);
    reopened.open_existing();
    EXPECT_TRUE(scan_all(reopened).empty())
        << "a segment with an unsupported format version must be skipped, not read";
}

TEST(ColumnarStoreFields, SegmentWithMissingColumnIsRejected) {
    TempDir tmp("missing_col");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);
    use_format(store, ob::kColumnarFormatV2);
    store.append(make_row(1000, 100'000, 10, 1, ob::SIDE_ASK, 2));
    store.flush_segment();

    const auto dir = store.index().at(0).dir_path;
    std::error_code ec;
    ASSERT_TRUE(fs::remove(dir + "/side.col", ec)) << "side.col should exist";

    ob::ColumnarStore reopened(tmp.str(), 1'000'000'000ULL);
    reopened.open_existing();
    EXPECT_TRUE(scan_all(reopened).empty())
        << "a segment missing a column must be skipped, not read with zeros";
}

TEST(ColumnarStoreFields, AFormat3SegmentWithoutItsColumnsFileIsRejected) {
    TempDir tmp("missing_col_v3");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);
    store.append(make_row(1000, 100'000, 10, 1, ob::SIDE_ASK, 2));
    store.flush_segment();

    const auto dir = store.index().at(0).dir_path;
    std::error_code ec;
    ASSERT_TRUE(fs::remove(dir + "/" + ob::kColumnsV3File, ec)) << "columns.v3 should exist";

    ob::ColumnarStore reopened(tmp.str(), 1'000'000'000ULL);
    reopened.open_existing();
    EXPECT_TRUE(scan_all(reopened).empty()) << "a segment without its columns was read";
}

TEST(ColumnarStoreFields, TruncatedColumnIsRejected) {
    TempDir tmp("truncated_col");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);
    use_format(store, ob::kColumnarFormatV2);
    for (int i = 0; i < 8; ++i) {
        store.append(make_row(1000 + static_cast<uint64_t>(i), 100'000 + i, 10, 1,
                              (i % 2 == 0) ? ob::SIDE_BID : ob::SIDE_ASK,
                              static_cast<uint16_t>(i)));
    }
    store.flush_segment();

    const auto dir = store.index().at(0).dir_path;
    // Keep only the first two of eight entries.
    fs::resize_file(dir + "/side.col", 2);

    ob::ColumnarStore reopened(tmp.str(), 1'000'000'000ULL);
    reopened.open_existing();
    EXPECT_TRUE(scan_all(reopened).empty())
        << "a truncated column must invalidate the segment; padding with zeros is "
           "how the lost-side defect behaved";
}

TEST(ColumnarStoreFields, AFormat3SegmentCutShortOrChangedIsRejectedAndTheOthersAreRead) {
    // Each a segment of its own, one row apart: the damaged one is skipped, the others read.
    for (const std::string damage : {"cut", "flip"}) {
        SCOPED_TRACE(damage);
        TempDir tmp("damaged_v3");
        ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);
        for (uint64_t s = 0; s < 3; ++s) {
            for (int i = 0; i < 8; ++i) {
                store.append(make_row(s * 2'000'000'000ULL + 1000 + static_cast<uint64_t>(i),
                                      100'000 + i, 10, 1, ob::SIDE_BID, static_cast<uint16_t>(i)));
            }
            store.flush_segment();
        }
        ASSERT_EQ(store.segment_count(), 3u);
        const std::string file = store.index().at(1).dir_path + "/" + ob::kColumnsV3File;
        const auto size = fs::file_size(file);
        if (damage == "cut") {
            fs::resize_file(file, size - 1);
        } else {
            // The last byte is the sequence numbers' block's: its checksum no longer holds.
            std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
            f.seekg(static_cast<std::streamoff>(size - 1));
            char c = 0;
            f.read(&c, 1);
            c = static_cast<char>(c ^ 0x5A);
            f.seekp(static_cast<std::streamoff>(size - 1));
            f.write(&c, 1);
        }
        ob::ColumnarStore reopened(tmp.str(), 1'000'000'000ULL);
        reopened.open_existing();
        const auto rows = scan_all(reopened);
        EXPECT_EQ(rows.size(), 16u) << "the damaged segment was read, or another one with it was not";
        for (const auto& r : rows) {
            EXPECT_TRUE(r.timestamp_ns < 2'000'000'000ULL || r.timestamp_ns >= 4'000'000'000ULL)
                << "a row of the damaged segment came back";
        }
    }
}

RC_GTEST_PROP(ColumnarStoreFieldsProperty,
              prop_round_trip_preserves_side_level_and_sequence,
              ()) {
    // The test that would have caught the original defect: the generator produces
    // both sides on its own, so the fields cannot stay at their defaults.
    const auto n = *rc::gen::inRange<int>(1, 40);

    TempDir tmp("prop_fields");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);

    struct Expected {
        uint8_t  side;
        uint16_t level;
        uint64_t seq;
        int64_t  price;
    };
    std::vector<Expected> expected;

    for (int i = 0; i < n; ++i) {
        const auto side  = static_cast<uint8_t>(*rc::gen::element(ob::SIDE_BID, ob::SIDE_ASK));
        const auto level = static_cast<uint16_t>(*rc::gen::inRange<int>(0, 64));
        const auto seq   = static_cast<uint64_t>(*rc::gen::inRange<int64_t>(0, 1'000'000));

        auto row = make_row(1000 + static_cast<uint64_t>(i),
                            /*price=*/100'000 + i, 10, 1, side, level);
        row.sequence_number = seq;
        store.append(row);
        expected.push_back({side, level, seq, row.price});
    }
    store.flush_segment();

    const auto rows = scan_all(store);
    RC_ASSERT(rows.size() == expected.size());

    // Price is unique per row here, so it identifies the row across the scan.
    for (const auto& want : expected) {
        const ob::SnapshotRow* got = nullptr;
        for (const auto& r : rows) {
            if (r.price == want.price) { got = &r; break; }
        }
        RC_ASSERT(got != nullptr);
        RC_ASSERT(got->side == want.side);
        RC_ASSERT(got->level_index == want.level);
        RC_ASSERT(got->sequence_number == want.seq);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #139: a scan reads the columns it was asked for and no others
//
// The interesting part of these is how they are written rather than what they assert. A test that
// checks "the field I did not ask for came back as zero" passes just as happily against a store
// that read the column, if the stored value happens to be zero. So the values here are chosen so
// that zero is impossible to produce by accident.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(ColumnarStoreProjection, AColumnLeftOutOfTheSetIsNotRead) {
    TempDir tmp("ut_projection_skips");
    ob::ColumnarStore store(tmp.str());

    // Every sequence number here is non-zero, and `make_row` derives it from the timestamp, so a
    // store that read `seq.col` cannot hand back a zero. Same for side, level and order count.
    for (uint64_t i = 1; i <= 8; ++i) {
        store.append(make_row(i * 1'000'000ULL, 10'000 + static_cast<int64_t>(i),
                              100 + i, static_cast<uint32_t>(i + 1),
                              ob::SIDE_ASK, static_cast<uint16_t>(i)));
    }
    store.flush_segment();

    // The control first: with everything in the set, none of those fields is zero.
    const auto full = scan_all(store);
    ASSERT_EQ(full.size(), 8u);
    for (const auto& r : full) {
        ASSERT_NE(r.sequence_number, 0u) << "the fixture has to make zero impossible";
        ASSERT_NE(r.order_count, 0u);
        ASSERT_NE(r.side, 0u);
        ASSERT_NE(r.level_index, 0u);
    }

    // Now ask for two columns, and the rest must come back at their defaults.
    std::vector<ob::SnapshotRow> narrow;
    ob::ColumnSet set;
    set.add(ob::QueryColumn::Price).add(ob::QueryColumn::Quantity);
    store.scan(0, UINT64_MAX, "", "", set,
               [&](const ob::SnapshotRow& r) { narrow.push_back(r); });

    ASSERT_EQ(narrow.size(), full.size()) << "narrowing changes the fields, not the rows";
    for (size_t i = 0; i < narrow.size(); ++i) {
        EXPECT_EQ(narrow[i].price,    full[i].price);
        EXPECT_EQ(narrow[i].quantity, full[i].quantity);
        // The timestamp is read whatever the caller asked for, because the scan filters on it.
        EXPECT_EQ(narrow[i].timestamp_ns, full[i].timestamp_ns);

        EXPECT_EQ(narrow[i].sequence_number, 0u) << "seq.col was not in the set";
        EXPECT_EQ(narrow[i].order_count,     0u) << "cnt.col was not in the set";
        EXPECT_EQ(narrow[i].side,            0u) << "side.col was not in the set";
        EXPECT_EQ(narrow[i].level_index,     0u) << "level.col was not in the set";
    }
}

TEST(ColumnarStoreProjection, TheTimestampIsReadEvenWhenTheSetLeavesItOut) {
    // A set built by hand can omit it; the scan compares every row against the time range, so it
    // adds the column itself rather than filtering against a zero it never loaded.
    TempDir tmp("ut_projection_ts");
    ob::ColumnarStore store(tmp.str());
    for (uint64_t i = 1; i <= 4; ++i) store.append(make_row(i * 1'000'000ULL));
    store.flush_segment();

    ob::ColumnSet price_only;
    price_only.add(ob::QueryColumn::Price);
    std::vector<ob::SnapshotRow> out;
    store.scan(2'000'000ULL, 3'000'000ULL, "", "", price_only,
               [&](const ob::SnapshotRow& r) { out.push_back(r); });

    ASSERT_EQ(out.size(), 2u) << "the range filter has to work without the caller asking for ts";
    EXPECT_EQ(out[0].timestamp_ns, 2'000'000ULL);
    EXPECT_EQ(out[1].timestamp_ns, 3'000'000ULL);
}

TEST(ColumnarStoreProjection, AMissingColumnFileOnlyRefusesTheSegmentForQueriesThatNeedIt) {
    TempDir tmp("ut_projection_missing");
    ob::ColumnarStore store(tmp.str());
    use_format(store, ob::kColumnarFormatV2);
    for (uint64_t i = 1; i <= 5; ++i) store.append(make_row(i * 1'000'000ULL));
    store.flush_segment();
    ASSERT_EQ(scan_all(store).size(), 5u);

    // Delete one column file. Before the read set, this dropped the whole segment from every
    // query, including the ones that would never have looked at it.
    size_t removed = 0;
    for (auto& e : fs::recursive_directory_iterator(tmp.path)) {
        if (e.is_regular_file() && e.path().filename() == "seq.col") {
            fs::remove(e.path());
            ++removed;
        }
    }
    ASSERT_EQ(removed, 1u) << "the fixture has to actually remove the file it is about";

    // A query that does not need it is answered.
    ob::ColumnSet without_seq;
    without_seq.add(ob::QueryColumn::Price).add(ob::QueryColumn::Quantity);
    std::vector<ob::SnapshotRow> ok;
    store.scan(0, UINT64_MAX, "", "", without_seq,
               [&](const ob::SnapshotRow& r) { ok.push_back(r); });
    EXPECT_EQ(ok.size(), 5u) << "a column nobody asked for cannot make a segment unreadable";

    // A query that needs it is still refused the segment, as before.
    ob::ColumnSet with_seq;
    with_seq.add(ob::QueryColumn::SequenceNumber);
    std::vector<ob::SnapshotRow> refused;
    store.scan(0, UINT64_MAX, "", "", with_seq,
               [&](const ob::SnapshotRow& r) { refused.push_back(r); });
    EXPECT_TRUE(refused.empty()) << "narrowing must not weaken the check for a column in the set";
}

TEST(ColumnarStoreProjection, AFormat3BlockThatFailsItsChecksumOnlyRefusesQueriesThatReadIt) {
    // Format 3's counterpart of the one above: the columns are blocks of one file, each with its own
    // checksum, checked only when the column is read.
    TempDir tmp("ut_projection_crc");
    ob::ColumnarStore store(tmp.str());
    for (uint64_t i = 1; i <= 5; ++i) store.append(make_row(i * 1'000'000ULL));
    store.flush_segment();
    ASSERT_EQ(scan_all(store).size(), 5u);
    const std::string file = store.index().at(0).dir_path + "/" + ob::kColumnsV3File;
    const auto size = fs::file_size(file);
    {
        // The last byte is the sequence numbers' block's.
        std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
        f.seekg(static_cast<std::streamoff>(size - 1));
        char c = 0;
        f.read(&c, 1);
        c = static_cast<char>(c ^ 0x5A);
        f.seekp(static_cast<std::streamoff>(size - 1));
        f.write(&c, 1);
    }
    ob::ColumnSet without_seq;
    without_seq.add(ob::QueryColumn::Price).add(ob::QueryColumn::Quantity);
    std::vector<ob::SnapshotRow> ok;
    store.scan(0, UINT64_MAX, "", "", without_seq, [&](const ob::SnapshotRow& r) { ok.push_back(r); });
    EXPECT_EQ(ok.size(), 5u) << "a block nobody asked for made the segment unreadable";
    ob::ColumnSet with_seq;
    with_seq.add(ob::QueryColumn::SequenceNumber);
    std::vector<ob::SnapshotRow> refused;
    store.scan(0, UINT64_MAX, "", "", with_seq, [&](const ob::SnapshotRow& r) { refused.push_back(r); });
    EXPECT_TRUE(refused.empty()) << "a block whose checksum fails was decoded";
}

// ── Segment format 3: the files, and both formats in one store ───────────────────────────────

TEST(ColumnarStoreFormat, AFormat3SegmentIsItsMetaAndOneColumnsFile) {
    TempDir tmp("format3_files");
    ob::ColumnarStore store(tmp.str());
    // Levels 0 - 19 of the bids, then of the asks.
    for (uint64_t i = 0; i < 40; ++i) {
        store.append(make_row(1'000'000ULL + i, 100'000 + static_cast<int64_t>(i), i + 1, 1,
                              i < 20 ? ob::SIDE_BID : ob::SIDE_ASK, static_cast<uint16_t>(i % 20)));
    }
    const auto meta = store.flush_segment();
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(meta->format_version, ob::kColumnarFormatV3);
    std::set<std::string> files;
    for (const auto& e : fs::directory_iterator(meta->dir_path)) files.insert(e.path().filename().string());
    EXPECT_EQ(files, (std::set<std::string>{"meta.json", ob::kColumnsV3File}));
    std::ifstream in(meta->dir_path + "/meta.json");
    const std::string json((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_NE(json.find("\"format_version\":3"), std::string::npos) << json;
    // Levels 0 - 19 of each side: five hex digits, not 250.
    EXPECT_NE(json.find("\"bid_levels\":\"fffff\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"ask_levels\":\"fffff\""), std::string::npos) << json;
    // And it reads back, the level sets included.
    ob::ColumnarStore reopened(tmp.str());
    reopened.open_existing();
    ASSERT_EQ(reopened.index().size(), 1u);
    ASSERT_NE(reopened.index()[0].levels, nullptr) << "the short level set was not read";
    EXPECT_EQ(*reopened.index()[0].levels, *meta->levels);
    EXPECT_EQ(scan_all(reopened).size(), 40u);
}

TEST(ColumnarStoreFormat, SegmentFormat2WritesFormat2) {
    TempDir tmp("format2_files");
    ob::ColumnarStore store(tmp.str());
    use_format(store, ob::kColumnarFormatV2);
    store.append(make_row(1'000'000ULL, 100'000, 10, 1, ob::SIDE_BID, 3));
    const auto meta = store.flush_segment();
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(meta->format_version, ob::kColumnarFormatV2);
    for (const char* f : {"ts.col", "price.col", "qty.col", "cnt.col", "side.col", "level.col", "seq.col"}) {
        EXPECT_TRUE(fs::exists(meta->dir_path + "/" + f)) << f;
    }
    EXPECT_FALSE(fs::exists(meta->dir_path + "/" + ob::kColumnsV3File));
    std::ifstream in(meta->dir_path + "/meta.json");
    const std::string json((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    // Every digit of the level sets: a build before format 3 reads this meta.json.
    const auto at = json.find("\"bid_levels\":\"");
    ASSERT_NE(at, std::string::npos);
    EXPECT_EQ(json.find('"', at + 14) - (at + 14), ob::LevelSet::kLevels / 4) << json;
}

namespace {

/// The encoding byte of each block of a format-3 segment, in the order of its file's directory.
std::vector<uint8_t> block_encodings(const std::string& segment_dir) {
    std::ifstream in(segment_dir + "/" + ob::kColumnsV3File, std::ios::binary);
    const std::string file((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    const std::span<const char> bytes(file.data(), file.size());
    EXPECT_EQ(file.substr(0, 4), "OBS3");
    size_t at = 4;
    uint64_t rows = 0;
    EXPECT_TRUE(ob::column_codec::get_varint(bytes, at, rows));
    const size_t columns = static_cast<uint8_t>(file.at(at++));
    std::vector<uint64_t> lengths;
    for (size_t c = 0; c < columns; ++c) {
        ++at;   // the column's id
        uint64_t length = 0;
        EXPECT_TRUE(ob::column_codec::get_varint(bytes, at, length));
        at += 4;   // the block's CRC32C
        lengths.push_back(length);
    }
    std::vector<uint8_t> encodings;
    for (const uint64_t length : lengths) {
        encodings.push_back(static_cast<uint8_t>(file.at(at)));
        at += static_cast<size_t>(length);
    }
    EXPECT_EQ(at, file.size());
    return encodings;
}

constexpr uint8_t kZstdBit = 0x20;
constexpr uint8_t kLz4Bit = 0x40;
constexpr size_t kQuantityColumn = 2;

/// Two hundred snapshots of a book's twenty levels whose quantities repeat from one to the next:
/// ZSTD keeps the quantities in half of LZ4's bytes, the case on which the two tiers part.
void fill_repeating_snapshots(ob::ColumnarStore& store) {
    for (uint64_t s = 0; s < 200; ++s) {
        for (uint64_t l = 0; l < 20; ++l) {
            store.append(make_row(1'000'000'000ULL + s * 100'000'000ULL,
                                  6'000'000'000LL - static_cast<int64_t>(l) * 1'000'000,
                                  100'000 + l * 7'919 % 5'003, 1, ob::SIDE_BID, static_cast<uint16_t>(l)));
        }
    }
}

}  // namespace

TEST(ColumnarStoreFormat, AStoreNotToldOtherwiseSealsAsTheServerDoesWithoutZstd) {
    // An application embedding the engine makes its stores without a format, and nothing merges
    // what they seal: they seal as a server's stores do - LZ4 or nothing, the last encodings reused.
    TempDir tmp("format_seal_default");
    ob::ColumnarStore store(tmp.str());
    EXPECT_EQ(store.segment_format().search.zstd_level, 0);
    EXPECT_EQ(store.segment_format().search_every, 16u);
    fill_repeating_snapshots(store);
    const auto meta = store.flush_segment();
    ASSERT_TRUE(meta.has_value());
    const auto encodings = block_encodings(meta->dir_path);
    ASSERT_EQ(encodings.size(), 7u);
    for (size_t c = 0; c < encodings.size(); ++c) {
        EXPECT_EQ(encodings[c] & kZstdBit, 0) << "column " << c;
    }
    EXPECT_NE(encodings[kQuantityColumn] & kLz4Bit, 0) << "the repeating quantities are LZ4's";
}

TEST(ColumnarStoreFormat, TheMergesFormatTakesZstdWhereItSavesItsMargin) {
    TempDir tmp("format_merge_policy");
    ob::ColumnarStore store(tmp.str());
    store.set_segment_format(ob::ColumnarStore::SegmentFormat::merge());
    fill_repeating_snapshots(store);
    const auto meta = store.flush_segment();
    ASSERT_TRUE(meta.has_value());
    const auto encodings = block_encodings(meta->dir_path);
    ASSERT_EQ(encodings.size(), 7u);
    EXPECT_NE(encodings[kQuantityColumn] & kZstdBit, 0) << "half of LZ4's bytes passes a margin of 10%";
    const auto rows = scan_all(store);
    ASSERT_EQ(rows.size(), 4000u);
    EXPECT_EQ(rows[3999].quantity, 100'000 + 19 * 7'919 % 5'003);
}

TEST(ColumnarStoreReadBuffers, AFormat3ReadsDecodingScratchIsCountedInWhatThePoolKeeps) {
    // The decoder's own buffers - here the quantities unpacked from fixed-width blocks before their
    // inverse - belong to the read's pooled set, so the budget bounds them as it bounds the columns.
    TempDir tmp("read_buffers_scratch");
    ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);
    constexpr uint64_t kRows = 4000;
    uint64_t x = 0x9E3779B97F4A7C15ULL;
    for (uint64_t i = 0; i < kRows; ++i) {
        // Quantities of sixteen random bits: blocks of 128 at sixteen bits beat anything LZ4 makes.
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        store.append(make_row(1000 + i, 100'000 + static_cast<int64_t>(i), 1 + (x & 0xFFFF)));
    }
    const auto meta = store.flush_segment();
    ASSERT_TRUE(meta.has_value());
    ASSERT_EQ(meta->format_version, ob::kColumnarFormatV3);
    const auto encodings = block_encodings(meta->dir_path);
    ASSERT_EQ(encodings.size(), 7u);
    ASSERT_EQ((encodings[kQuantityColumn] >> 3) & 0x3, 2) << "the quantities are in fixed-width blocks";

    constexpr size_t kBudget = size_t{128} << 20;
    struct RestoreBudget {
        ~RestoreBudget() { ob::ColumnarStore::set_read_buffers_limit_for_test(kBudget); }
    } restore;
    ob::ColumnarStore::set_read_buffers_limit_for_test(0);   // a fresh set for the read below
    ob::ColumnarStore::set_read_buffers_limit_for_test(kBudget);
    ASSERT_EQ(scan_all(store).size(), kRows);
    // The seven columns decoded, the file as read, and at least the quantities' values in the
    // decode's scratch.
    const size_t file_bytes = fs::file_size(meta->dir_path + "/" + ob::kColumnsV3File);
    EXPECT_GE(ob::ColumnarStore::read_buffers_held(),
              kRows * (8 + 8 + 8 + 8 + 4 + 1 + 2) + file_bytes + kRows * sizeof(uint64_t));
}

TEST(ColumnarStoreFormat, SegmentsOfBothFormatsInOneStoreReadAsTheyWereWritten) {
    TempDir tmp("format_both");
    std::vector<ob::SnapshotRow> written;
    {
        ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);
        for (uint32_t version : {ob::kColumnarFormatV2, ob::kColumnarFormatV3, ob::kColumnarFormatV2,
                                 ob::kColumnarFormatV3}) {
            use_format(store, version);
            const uint64_t base = written.size() * 1'000'000ULL;
            for (uint64_t i = 0; i < 50; ++i) {
                auto r = make_row(base + i * 1000, -5'000 + static_cast<int64_t>(i * 37 % 101),
                                  1 + i * 7919 % 5000, static_cast<uint32_t>(i % 9),
                                  i % 3 ? ob::SIDE_ASK : ob::SIDE_BID, static_cast<uint16_t>(i % 25));
                r.sequence_number = 1'000'000'000ULL + base + i;
                store.append(r);
                written.push_back(r);
            }
            const auto meta = store.flush_segment();
            ASSERT_TRUE(meta.has_value());
            ASSERT_EQ(meta->format_version, version);
        }
    }
    ob::ColumnarStore reopened(tmp.str(), 1'000'000'000ULL);
    reopened.open_existing();
    const auto rows = scan_all(reopened);
    ASSERT_EQ(rows.size(), written.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows[i].timestamp_ns, written[i].timestamp_ns) << i;
        EXPECT_EQ(rows[i].price, written[i].price) << i;
        EXPECT_EQ(rows[i].quantity, written[i].quantity) << i;
        EXPECT_EQ(rows[i].order_count, written[i].order_count) << i;
        EXPECT_EQ(rows[i].side, written[i].side) << i;
        EXPECT_EQ(rows[i].level_index, written[i].level_index) << i;
        EXPECT_EQ(rows[i].sequence_number, written[i].sequence_number) << i;
    }
}

// ── replace_from_staging: a snapshot replaces the store, it does not join it (#142) ──────────

namespace {

/// Write a minimal segment directory: enough `meta.json` for `open_existing()` to index it.
static void plant_segment(const fs::path& base, const std::string& symbol,
                          const std::string& exchange, uint64_t start_ns, uint64_t end_ns) {
    const fs::path dir = base / symbol / exchange /
                         (std::to_string(start_ns) + "_" + std::to_string(end_ns));
    fs::create_directories(dir);
    std::ofstream meta(dir / "meta.json");
    meta << "{\"symbol\":\"" << symbol << "\",\"exchange\":\"" << exchange
         << "\",\"start_ts_ns\":" << start_ns << ",\"end_ts_ns\":" << end_ns
         << ",\"row_count\":1,\"wal_identity\":0,\"wal_file_index\":0,\"wal_byte_offset\":0}";
}

static std::string segment_relpath(const std::string& symbol, const std::string& exchange,
                                   uint64_t start_ns, uint64_t end_ns) {
    return symbol + "/" + exchange + "/" + std::to_string(start_ns) + "_" +
           std::to_string(end_ns) + "/meta.json";
}

} // namespace

// The shape #142 was: a replica killed mid-stream has flushed a **prefix**, so its directory ends
// earlier than the one arriving. The ranges overlap and the names differ, which is why the
// duplicate-directory guard had nothing to refuse and the rows were returned twice.
TEST(ColumnarStoreReplace, AStaleSegmentWhoseRangeOverlapsButDoesNotMatchIsRemoved) {
    TempDir data("replace_overlap");
    TempDir staging("replace_overlap_stage");

    plant_segment(data.path, "SYM", "EX", 1000, 1088);      // ours, a prefix
    plant_segment(staging.path, "SYM", "EX", 1000, 1100);   // theirs, the whole thing

    ob::ColumnarStore store(data.path.string());
    store.open_existing();
    ASSERT_EQ(store.segment_count(), 1u) << "the stale segment was not indexed, so this test "
                                            "cannot show that it is removed";

    ASSERT_TRUE(store.replace_from_staging(
        staging.path.string(), {segment_relpath("SYM", "EX", 1000, 1100)}));

    EXPECT_EQ(store.segment_count(), 1u)
        << "the store holds more than the snapshot named, so the install joined rather than "
           "replaced";
    EXPECT_FALSE(fs::exists(data.path / "SYM" / "EX" / "1000_1088"))
        << "the stale directory is still on disk; open_existing() will index it again";
    EXPECT_TRUE(fs::exists(data.path / "SYM" / "EX" / "1000_1100" / "meta.json"));
}

// The WAL is not the store's to delete, and `wal_identity` says which stream this directory
// belongs to — losing it turns a resumable replica into one that re-syncs from zero (#101).
TEST(ColumnarStoreReplace, TheWalAndTheIdentityAndPlainFilesSurvive) {
    TempDir data("replace_spares");
    TempDir staging("replace_spares_stage");

    plant_segment(data.path, "SYM", "EX", 1000, 1088);
    { std::ofstream(data.path / "wal_000000.bin") << "wal"; }
    { std::ofstream(data.path / "wal_identity") << "7"; }
    { std::ofstream(data.path / "repl_state.txt") << "pos"; }
    fs::create_directories(data.path / "wal_archive");          // a directory, but WAL's
    plant_segment(staging.path, "SYM", "EX", 1000, 1100);

    ob::ColumnarStore store(data.path.string());
    store.open_existing();
    ASSERT_TRUE(store.replace_from_staging(
        staging.path.string(), {segment_relpath("SYM", "EX", 1000, 1100)}));

    EXPECT_TRUE(fs::exists(data.path / "wal_000000.bin"));
    EXPECT_TRUE(fs::exists(data.path / "wal_identity"));
    EXPECT_TRUE(fs::exists(data.path / "repl_state.txt"));
    EXPECT_TRUE(fs::exists(data.path / "wal_archive"));
}

// Both callers stage **inside** the data directory, so a clear that does not know about staging
// deletes the files it is about to install. This is the one that turns the fix into the defect.
TEST(ColumnarStoreReplace, TheStagingDirectoryInsideTheDataDirectoryIsNotDeleted) {
    TempDir data("replace_staging_inside");
    const fs::path staging = data.path / "snapshot_staging";

    plant_segment(data.path, "SYM", "EX", 1000, 1088);
    plant_segment(staging, "SYM", "EX", 1000, 1100);

    ob::ColumnarStore store(data.path.string());
    store.open_existing();
    ASSERT_TRUE(store.replace_from_staging(
        staging.string(), {segment_relpath("SYM", "EX", 1000, 1100)}))
        << "the install failed, which is what happens when the clear ate the staged files";

    EXPECT_TRUE(fs::exists(data.path / "SYM" / "EX" / "1000_1100" / "meta.json"));
    EXPECT_FALSE(fs::exists(data.path / "SYM" / "EX" / "1000_1088"));
}

// A store that refuses has to leave an index describing what is actually there, because the node
// stays up and serves reads until it bootstraps again.
TEST(ColumnarStoreReplace, AMissingStagedFileRefusesAndLeavesACoherentIndex) {
    TempDir data("replace_missing");
    TempDir staging("replace_missing_stage");

    plant_segment(data.path, "SYM", "EX", 1000, 1088);
    ob::ColumnarStore store(data.path.string());
    store.open_existing();

    EXPECT_FALSE(store.replace_from_staging(
        staging.path.string(), {segment_relpath("SYM", "EX", 1000, 1100)}))
        << "a staged file that is not there has to be refused rather than installed as nothing";

    EXPECT_EQ(store.segment_count(), 0u)
        << "the index still names the directory the clear removed, so a scan would open files "
           "that are gone and answer short";
}

// ── #136: a directory belongs to one segment, not to one event-time span ──────────────────────

namespace {

/// Write `rows` rows into the span starting at `start_ts` and flush them, returning the segment.
static ob::SegmentMeta flush_span(ob::ColumnarStore& store, uint64_t start_ts, int rows,
                                  int64_t price) {
    store.set_symbol_exchange("SYM", "EX");
    for (int i = 0; i < rows; ++i) {
        store.append(make_row(start_ts + static_cast<uint64_t>(i), price + i));
    }
    auto meta = store.flush_segment();
    EXPECT_TRUE(meta.has_value()) << "nothing was flushed, so this test has no segment to talk "
                                     "about";
    return meta.value();
}

} // namespace

// The common case has to be byte-for-byte what it was, or this change is a migration rather than
// a fix: the name only differs where the engine used to lose data.
TEST(SegmentIdentity, TheFirstSegmentOfASpanKeepsTheNameItAlwaysHad) {
    TempDir tmp("ident_first");
    ob::ColumnarStore store(tmp.str(), 1000ULL);

    const auto meta = flush_span(store, 1000, 4, 500);

    const std::string expected = tmp.str() + "/SYM/EX/1000_1003";
    EXPECT_EQ(meta.dir_path, expected)
        << "the unchanged case changed, which would make every existing directory a special case";
}

// The defect: two flushes covering one span wrote to one directory, and the second destroyed the
// first. Both are readable now, and the second is named for being second.
TEST(SegmentIdentity, ASecondFlushOfTheSameSpanIsASecondSegment) {
    TempDir tmp("ident_second");
    ob::ColumnarStore store(tmp.str(), 1000ULL);

    const auto first  = flush_span(store, 1000, 4, 500);
    const auto second = flush_span(store, 1000, 4, 900);

    EXPECT_NE(first.dir_path, second.dir_path)
        << "both flushes claimed one directory, which is the whole of #136";
    EXPECT_EQ(second.dir_path, first.dir_path + "_1");
    EXPECT_TRUE(fs::exists(first.dir_path + "/meta.json"));
    EXPECT_TRUE(fs::exists(second.dir_path + "/meta.json"));

    // `flush_segment()` indexes what it wrote, so the question is whether the index holds both.
    // Before #136 the second flush reused the first's directory and the index held one entry
    // whose row count described bytes that were no longer there.
    EXPECT_EQ(store.segment_count(), 2u)
        << "the index holds one entry for two flushes, so one flush's rows are unreachable";
    EXPECT_EQ(store.merge_segments({second}), 1u)
        << "a segment already indexed by the flush that wrote it must still be refused a second "
           "time — that guard is what this change narrows, not what it removes";
}

// The worse half of #136, and the one that returned **nothing**: a shorter second write left the
// index claiming the first write's row count over the second write's bytes, and the reader
// refused the whole segment.
TEST(SegmentIdentity, AShorterSecondWriteDoesNotStrandTheFirst) {
    TempDir tmp("ident_shorter");
    ob::ColumnarStore store(tmp.str(), 1000ULL);

    const auto first  = flush_span(store, 2000, 8, 500);
    const auto second = flush_span(store, 2000, 2, 900);

    ASSERT_NE(first.dir_path, second.dir_path);
    EXPECT_EQ(first.row_count, 8u);
    EXPECT_EQ(second.row_count, 2u);

    // Re-read from disk: what the index says and what the bytes hold have to agree for both, and
    // before #136 the shorter write made them disagree for the surviving entry.
    ob::ColumnarStore reopened(tmp.str(), 1000ULL);
    reopened.open_existing();
    EXPECT_EQ(reopened.segment_count(), 2u)
        << "a segment was dropped on re-open, which is how this defect returned zero rows";
}

// `create_directory()` is the arbiter rather than an `exists()` before it, so this holds without
// any claim about which lock the caller holds — which matters, because the guard this replaces
// asserted a locking fact about its callers and was wrong about it.
TEST(SegmentIdentity, TwoStoresRacingForOneSpanGetDifferentDirectories) {
    TempDir tmp("ident_race");

    constexpr int kThreads = 4;
    std::vector<std::string> dirs(kThreads);
    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            ob::ColumnarStore store(tmp.str(), 1000ULL);
            dirs[static_cast<size_t>(i)] = flush_span(store, 3000, 3, 100 * (i + 1)).dir_path;
        });
    }
    for (auto& th : threads) th.join();

    std::set<std::string> unique(dirs.begin(), dirs.end());
    EXPECT_EQ(unique.size(), static_cast<size_t>(kThreads))
        << "two flushers were handed the same directory, so one overwrote the other";
}

// ═══════════════════════════════════════════════════════════════════════════════
// #49 step 2: a thread keeps its segment-read buffers from one read to the next
//
// The buffers hold the last read's columns. What must not follow from that: a read handing back a
// value an earlier read left - in a shorter segment, in a run of zeros, or while an outer read on
// the same thread is still handing rows out of them.
// ═══════════════════════════════════════════════════════════════════════════════

namespace {

// Every field a function of the store's tag and the row's index, so that a value from the other
// store's segment, or from another row, cannot pass for the right one. The quantities hold runs of
// zeros - Simple8b's all-zero words - where the other tag's hold values.
uint64_t tagged_qty(uint64_t tag, uint64_t i) {
    const bool zero = (tag % 2 == 1) ? (i % 500 >= 300) : (i % 500 < 300);
    return zero ? 0 : tag * 1000 + i;
}

void fill_tagged(ob::ColumnarStore& store, uint64_t tag, uint64_t rows) {
    for (uint64_t i = 0; i < rows; ++i) {
        auto row = make_row(1000 + i, static_cast<int64_t>(tag * 1'000'000 + i), tagged_qty(tag, i),
                            static_cast<uint32_t>(tag + i % 7),
                            tag % 2 == 1 ? ob::SIDE_ASK : ob::SIDE_BID,
                            static_cast<uint16_t>(tag + i % 3));
        row.sequence_number = tag * 1'000'000 + i;
        store.append(row);
    }
    ASSERT_TRUE(store.flush_segment().has_value());
}

// The same check, as a value, for a thread that cannot use the test's assertions to stop.
bool tagged_rows_match(std::vector<ob::SnapshotRow> rows, uint64_t tag, uint64_t count) {
    if (rows.size() != count) return false;
    std::sort(rows.begin(), rows.end(), [](const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
        return a.timestamp_ns < b.timestamp_ns;
    });
    for (uint64_t i = 0; i < count; ++i) {
        const auto& r = rows[i];
        if (r.timestamp_ns != 1000 + i || r.price != static_cast<int64_t>(tag * 1'000'000 + i) ||
            r.quantity != tagged_qty(tag, i) || r.order_count != tag + i % 7 ||
            r.side != (tag % 2 == 1 ? ob::SIDE_ASK : ob::SIDE_BID) ||
            r.level_index != tag + i % 3 || r.sequence_number != tag * 1'000'000 + i) {
            return false;
        }
    }
    return true;
}

void expect_tagged(std::vector<ob::SnapshotRow> rows, uint64_t tag, uint64_t count) {
    ASSERT_EQ(rows.size(), count) << "store " << tag;
    std::sort(rows.begin(), rows.end(), [](const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
        return a.timestamp_ns < b.timestamp_ns;
    });
    for (uint64_t i = 0; i < count; ++i) {
        const auto& r = rows[i];
        ASSERT_EQ(r.timestamp_ns, 1000 + i) << "store " << tag << " row " << i;
        ASSERT_EQ(r.price, static_cast<int64_t>(tag * 1'000'000 + i)) << "store " << tag << " row " << i;
        ASSERT_EQ(r.quantity, tagged_qty(tag, i)) << "store " << tag << " row " << i;
        ASSERT_EQ(r.order_count, tag + i % 7) << "store " << tag << " row " << i;
        ASSERT_EQ(r.side, tag % 2 == 1 ? ob::SIDE_ASK : ob::SIDE_BID) << "store " << tag << " row " << i;
        ASSERT_EQ(r.level_index, tag + i % 3) << "store " << tag << " row " << i;
        ASSERT_EQ(r.sequence_number, tag * 1'000'000 + i) << "store " << tag << " row " << i;
    }
}

}  // namespace

TEST(ColumnarStoreReadBuffers, ASegmentReadAfterALongerOneHandsBackOnlyItsOwnValues) {
    TempDir tmp("read_buffers_lengths");
    ob::ColumnarStore longer((tmp.path / "a").string(), 1'000'000'000ULL);
    ob::ColumnarStore shorter((tmp.path / "b").string(), 1'000'000'000ULL);
    fill_tagged(longer, 1, 3000);
    fill_tagged(shorter, 2, 700);

    expect_tagged(scan_all(longer), 1, 3000);
    expect_tagged(scan_all(shorter), 2, 700);
    expect_tagged(scan_all(longer), 1, 3000);
    EXPECT_GT(ob::ColumnarStore::read_buffers_held(), 0u) << "the pool keeps what the reads filled";
}

TEST(ColumnarStoreReadBuffers, AReadInsideAnotherOnTheSameThreadGetsBuffersOfItsOwn) {
    TempDir tmp("read_buffers_nested");
    ob::ColumnarStore outer_store((tmp.path / "a").string(), 1'000'000'000ULL);
    ob::ColumnarStore inner_store((tmp.path / "b").string(), 1'000'000'000ULL);
    fill_tagged(outer_store, 3, 1200);
    fill_tagged(inner_store, 4, 800);

    std::vector<ob::SnapshotRow> outer, inner;
    outer_store.scan(0, UINT64_MAX, "", "", ob::ColumnSet::all(), [&](const ob::SnapshotRow& r) {
        if (outer.empty()) inner = scan_all(inner_store);
        outer.push_back(r);
    });
    expect_tagged(inner, 4, 800);
    expect_tagged(outer, 3, 1200);
}

TEST(ColumnarStoreReadBuffers, BuffersPastTheBudgetAreFreedRatherThanKept) {
    for (const uint32_t format : {ob::kColumnarFormatV2, ob::kColumnarFormatV3}) {
        SCOPED_TRACE(format);
        TempDir tmp("read_buffers_budget");
        ob::ColumnarStore store(tmp.str(), 1'000'000'000ULL);
        use_format(store, format);
        fill_tagged(store, 5, 2000);

        constexpr size_t kBudget = size_t{128} << 20;
        struct RestoreBudget {
            ~RestoreBudget() { ob::ColumnarStore::set_read_buffers_limit_for_test(kBudget); }
        } restore;

        // From nothing, whatever the tests before this one left in the pool.
        ob::ColumnarStore::set_read_buffers_limit_for_test(0);
        expect_tagged(scan_all(store), 5, 2000);
        ASSERT_EQ(ob::ColumnarStore::read_buffers_held(), 0u) << "a set past the budget is freed";

        ob::ColumnarStore::set_read_buffers_limit_for_test(kBudget);
        expect_tagged(scan_all(store), 5, 2000);
        const size_t held = ob::ColumnarStore::read_buffers_held();
        // At least every column of 2000 rows as read and as decoded. Format 2: the timestamps, the
        // prices both ways, the quantities and sequence numbers decoded (and the sequence numbers'
        // zigzag deltas), the counts, sides and levels - all but the Simple8b words, whose number the
        // codec decides. A floor of the decoded columns alone let a count that left one buffer out pass
        // (the mutation table of #49's step 2). Format 3: the seven columns decoded, and its file as
        // read - the largest of the store's.
        size_t every_column = size_t{2000} * (8 + 8 + 8 + 8 + 8 + 8 + 4 + 1 + 2);
        if (format == ob::kColumnarFormatV3) {
            size_t largest_file = 0;
            for (const auto& m : store.index()) {
                largest_file = std::max<size_t>(largest_file, fs::file_size(m.dir_path + "/" + ob::kColumnsV3File));
            }
            every_column = size_t{2000} * (8 + 8 + 8 + 8 + 4 + 1 + 2) + largest_file;
        }
        EXPECT_GE(held, every_column);
        expect_tagged(scan_all(store), 5, 2000);
        EXPECT_EQ(ob::ColumnarStore::read_buffers_held(), held)
            << "the next read takes the same set back and needs no more";

        ob::ColumnarStore::set_read_buffers_limit_for_test(held - 1);
        EXPECT_EQ(ob::ColumnarStore::read_buffers_held(), 0u) << "a lower budget frees what it cannot hold";
        expect_tagged(scan_all(store), 5, 2000);
        EXPECT_EQ(ob::ColumnarStore::read_buffers_held(), 0u) << "and a read's set that would pass it";
        ob::ColumnarStore::set_read_buffers_limit_for_test(kBudget);
    }
}

TEST(ColumnarStoreReadBuffers, ThreadsReadingAtOnceEachHaveASetOfTheirOwn) {
    TempDir tmp("read_buffers_threads");
    ob::ColumnarStore odd((tmp.path / "a").string(), 1'000'000'000ULL);
    ob::ColumnarStore even((tmp.path / "b").string(), 1'000'000'000ULL);
    fill_tagged(odd, 7, 1500);
    fill_tagged(even, 8, 900);

    std::atomic<int> wrong{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < 25; ++i) {
                const bool pick_odd = (t + i) % 2 == 1;
                if (!tagged_rows_match(scan_all(pick_odd ? odd : even), pick_odd ? 7 : 8,
                                       pick_odd ? 1500 : 900)) {
                    wrong.fetch_add(1);
                }
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(wrong.load(), 0) << "reads of one thread saw another's columns";
}
