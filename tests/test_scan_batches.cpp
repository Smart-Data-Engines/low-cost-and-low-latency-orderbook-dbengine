// OB-12, stage 1: a scan hands its rows over a batch at a time.
//
// The batch is what every read emits through - a segment's pooled read, its held columns (#220) and
// the blocks no seal has written yet - and the row scan is a reader over it. What this file holds:
//
// - a batch's shape: the read's columns, whole, and no more than a chunk of rows, increasing;
// - the rows across the batches against an oracle that is not the scan: rows written in time order,
//   a segment per flush and a block last, so the order the scan hands them over in is the order they
//   were written - for any range, filter, set of columns and reader that stops early;
// - a reader stopping inside a batch, counted as the row-by-row loop counted it;
// - a format-2 column cut short, padded to the segment's rows before it is handed over.

#include "orderbook/columnar_store.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 20'718ULL * 86'400ULL * kSec;   // a whole UTC day: one hour's segment

std::atomic<uint64_t> g_counter{0};

std::string made(const fs::path& dir) {
    fs::create_directories(dir);
    return dir.string();
}

ob::SnapshotRow row(uint64_t ts, uint8_t side, uint16_t level, int64_t price, uint64_t seq) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = seq;
    r.side            = side;
    r.level_index     = level;
    r.price           = price;
    r.quantity        = static_cast<uint64_t>(price % 97 + 1);
    r.order_count     = static_cast<uint32_t>(level % 5 + 1);
    return r;
}

struct Store {
    fs::path dir;
    ob::ColumnarStore store;

    explicit Store(uint32_t format = ob::kColumnarFormatV3)
        : dir(fs::temp_directory_path() / ("ob_scan_batches_" + std::to_string(::getpid()) + "_" +
                                           std::to_string(g_counter.fetch_add(1))))
        , store(made(dir)) {
        auto f = store.segment_format();
        f.version = format;
        store.set_segment_format(f);
        store.set_symbol_exchange("BK", "EX");
    }
    ~Store() {
        store.close();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    ob::SegmentMeta segment(const std::vector<ob::SnapshotRow>& rows) {
        store.set_symbol_exchange("BK", "EX");
        for (const auto& r : rows) store.append(r);
        auto meta = store.flush_segment();
        EXPECT_TRUE(meta.has_value());
        return meta.value_or(ob::SegmentMeta{});
    }
    void block(const std::vector<ob::SnapshotRow>& rows) {
        store.publish_blocks({ob::RowBlock::make("BK", "EX", rows)});
    }
};

bool same_row(const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
    return a.timestamp_ns == b.timestamp_ns && a.sequence_number == b.sequence_number &&
           a.side == b.side && a.level_index == b.level_index && a.price == b.price &&
           a.quantity == b.quantity && a.order_count == b.order_count;
}

/// What the row scan hands over of `r` when it reads `columns`: the fields of those, zero elsewhere.
ob::SnapshotRow projected(const ob::SnapshotRow& r, ob::ColumnSet columns) {
    ob::SnapshotRow out{};
    out.timestamp_ns = r.timestamp_ns;
    if (columns.has(ob::QueryColumn::SequenceNumber)) out.sequence_number = r.sequence_number;
    if (columns.has(ob::QueryColumn::Side))           out.side            = r.side;
    if (columns.has(ob::QueryColumn::Level))          out.level_index     = r.level_index;
    if (columns.has(ob::QueryColumn::Price))          out.price           = r.price;
    if (columns.has(ob::QueryColumn::Quantity))       out.quantity        = r.quantity;
    if (columns.has(ob::QueryColumn::OrderCount))     out.order_count     = r.order_count;
    return out;
}

struct Collected {
    std::vector<ob::SnapshotRow> rows;
    std::vector<size_t> batch_sizes;
    ob::ColumnarStore::ScanCost cost;
    bool shapes_ok{true};
    std::string why;
};

/// The rows of a batch scan, collected through `row_of()`, stopping once `stop_after` are taken -
/// and every batch's shape checked against the columns the read asked for.
Collected collect(const ob::ColumnarStore& store, uint64_t from, uint64_t to, ob::ColumnSet columns,
                  const ob::RowFilter& filter, size_t stop_after = SIZE_MAX) {
    Collected c;
    ob::ColumnSet read = columns;
    read.add(ob::QueryColumn::TimestampNs);
    if (filter.has_price()) read.add(ob::QueryColumn::Price);
    if (filter.has_side()) read.add(ob::QueryColumn::Side);
    if (filter.has_level()) read.add(ob::QueryColumn::Level);
    c.cost = store.scan_batches(from, to, "BK", "EX", columns, filter,
                                [&](const ob::RowBatch& b) -> ob::BatchVerdict {
        c.batch_sizes.push_back(b.rows.size());
        const auto bad = [&](const std::string& why) {
            if (c.shapes_ok) c.why = why;
            c.shapes_ok = false;
        };
        if (b.rows.empty() || b.rows.size() > ob::kScanBatchRows) bad("a batch of " + std::to_string(b.rows.size()) + " rows");
        if (!std::is_sorted(b.rows.begin(), b.rows.end()) ||
            std::adjacent_find(b.rows.begin(), b.rows.end()) != b.rows.end()) {
            bad("a batch whose rows do not increase");
        }
        const size_t needed = b.rows.empty() ? 0 : static_cast<size_t>(b.rows.back()) + 1;
        const auto whole = [&](size_t size, ob::QueryColumn col, const char* name) {
            if (read.has(col) ? size < needed : size != 0) {
                bad(std::string("column ") + name + " holds " + std::to_string(size) + " values");
            }
        };
        whole(b.ts.size(), ob::QueryColumn::TimestampNs, "ts");
        whole(b.price.size(), ob::QueryColumn::Price, "price");
        whole(b.qty.size(), ob::QueryColumn::Quantity, "qty");
        whole(b.cnt.size(), ob::QueryColumn::OrderCount, "cnt");
        whole(b.side.size(), ob::QueryColumn::Side, "side");
        whole(b.level.size(), ob::QueryColumn::Level, "level");
        whole(b.seq.size(), ob::QueryColumn::SequenceNumber, "seq");
        for (size_t k = 0; k < b.rows.size(); ++k) {
            c.rows.push_back(ob::row_of(b, b.rows[k]));
            if (c.rows.size() >= stop_after) return {true, k};
        }
        return {};
    });
    return c;
}

} // namespace

// ── A batch's shape ────────────────────────────────────────────────────────────────────────────

TEST(ScanBatches, ABatchHoldsTheReadsColumnsWholeAndAChunkOfIncreasingRowsAtMost) {
    Store s;
    std::vector<ob::SnapshotRow> seg, blk;
    for (uint64_t i = 0; i < 5000; ++i) seg.push_back(row(kBase + i * kSec / 10, 0, 0, 100 + static_cast<int64_t>(i), i));
    for (uint64_t i = 0; i < 3000; ++i) blk.push_back(row(kBase + (5000 + i) * kSec / 10, 1, 2, 50, 5000 + i));
    s.segment(seg);
    s.block(blk);
    ob::ColumnSet price_only;
    price_only.add(ob::QueryColumn::Price);
    const Collected c = collect(s.store, 0, UINT64_MAX, price_only, ob::RowFilter{});
    EXPECT_TRUE(c.shapes_ok) << c.why;
    ASSERT_EQ(c.rows.size(), 8000u);
    // The segment's 5 000 rows in chunks of 2 048, then the block's 3 000 the same way.
    EXPECT_EQ(c.batch_sizes, (std::vector<size_t>{2048, 2048, 904, 2048, 952}));
    EXPECT_EQ(c.rows[4999].price, 5099);
    EXPECT_EQ(c.rows[5000].price, 50) << "the block's rows come after the segment's";
    EXPECT_EQ(c.rows[0].quantity, 0u) << "a column the read did not ask for reads as zero";
    EXPECT_EQ(c.rows[0].sequence_number, 0u);
}

// ── The rows across batches, against the rows as written ──────────────────────────────────────

RC_GTEST_PROP(ScanBatchesProperty, TheRowsAcrossBatchesAreTheWrittenRowsInRangeKeptAndInOrder, ()) {
    const bool v2 = *rc::gen::arbitrary<bool>();
    Store s(v2 ? ob::kColumnarFormatV2 : ob::kColumnarFormatV3);
    // Segments one after another in time, a flush each, and maybe a block after them: the order
    // the scan hands rows over in is then the order they were written in.
    const int segments = *rc::gen::inRange(1, 4);
    std::vector<ob::SnapshotRow> written;
    uint64_t t = kBase;
    for (int g = 0; g < segments + 1; ++g) {
        const bool is_block = g == segments;
        if (is_block && *rc::gen::arbitrary<bool>()) break;
        const int n = *rc::gen::inRange(1, 4500);
        t += 1000;   // each segment starts after the last one's end, so the index orders them as written
        std::vector<ob::SnapshotRow> rows;
        for (int i = 0; i < n; ++i) {
            t += static_cast<uint64_t>(*rc::gen::inRange(0, 3)) * 1000;   // ties too
            const auto side = static_cast<uint8_t>(*rc::gen::inRange(0, 2));
            const auto level = static_cast<uint16_t>(*rc::gen::inRange(0, 6));
            const int64_t price = *rc::gen::inRange<int64_t>(-50, 400);
            rows.push_back(row(t, side, level, price, static_cast<uint64_t>(written.size())));
            written.push_back(rows.back());
        }
        if (is_block) {
            s.block(rows);
        } else {
            s.segment(rows);
        }
    }
    const uint64_t first = written.front().timestamp_ns;
    const uint64_t last = written.back().timestamp_ns;
    const auto moment = [&]() {
        switch (*rc::gen::inRange(0, 4)) {
        case 0: return uint64_t{0};
        case 1: return UINT64_MAX;
        default: return first + (last - first) * static_cast<uint64_t>(*rc::gen::inRange(0, 101)) / 100;
        }
    };
    uint64_t from = moment(), to = moment();
    if (from > to) std::swap(from, to);

    ob::RowFilter filter;
    if (*rc::gen::arbitrary<bool>()) filter.price_lo = *rc::gen::inRange<int64_t>(-60, 410);
    if (*rc::gen::arbitrary<bool>()) filter.price_hi = *rc::gen::inRange<int64_t>(-60, 410);
    if (*rc::gen::arbitrary<bool>()) filter.side_lo = filter.side_hi = static_cast<uint8_t>(*rc::gen::inRange(0, 2));
    if (*rc::gen::arbitrary<bool>()) {
        filter.level_lo = static_cast<uint16_t>(*rc::gen::inRange(0, 4));
        filter.level_hi = static_cast<uint16_t>(*filter.level_lo + *rc::gen::inRange(0, 3));
    }
    ob::ColumnSet columns;
    for (const ob::QueryColumn c : ob::all_query_columns()) {
        if (*rc::gen::arbitrary<bool>()) columns.add(c);
    }
    const size_t stop_after = *rc::gen::arbitrary<bool>()
                                  ? SIZE_MAX
                                  : static_cast<size_t>(*rc::gen::inRange(1, 3000));

    ob::ColumnSet read = columns;
    if (filter.has_price()) read.add(ob::QueryColumn::Price);
    if (filter.has_side()) read.add(ob::QueryColumn::Side);
    if (filter.has_level()) read.add(ob::QueryColumn::Level);
    std::vector<ob::SnapshotRow> expected;
    for (const auto& r : written) {
        if (expected.size() >= stop_after) break;
        if (r.timestamp_ns < from || r.timestamp_ns > to) continue;
        if (!filter.keeps(r.price, r.side, r.level_index)) continue;
        expected.push_back(projected(r, read));
    }

    const Collected got = collect(s.store, from, to, columns, filter, stop_after);
    RC_ASSERT(got.shapes_ok);
    RC_ASSERT(got.rows.size() == expected.size());
    for (size_t i = 0; i < expected.size(); ++i) RC_ASSERT(same_row(got.rows[i], expected[i]));
    RC_ASSERT(got.cost.stopped == (expected.size() >= stop_after));
}

// ── A stop inside a batch ──────────────────────────────────────────────────────────────────────

TEST(ScanBatches, AReaderStoppingInsideABatchCountsTheRowsFilteredBeforeItsRowOnly) {
    // 5 000 rows, a third of them level 0: the 1 000th of those is row 2 997 of the segment, in its
    // second chunk, and 1 998 rows of the range failed the filter before it.
    Store s;
    std::vector<ob::SnapshotRow> rows;
    for (uint64_t i = 0; i < 5000; ++i) {
        rows.push_back(row(kBase + i * kSec / 10, 0, static_cast<uint16_t>(i % 3), static_cast<int64_t>(i), i));
    }
    s.segment(rows);
    ob::RowFilter level0;
    level0.level_lo = level0.level_hi = 0;
    const Collected c = collect(s.store, 0, UINT64_MAX, ob::ColumnSet::all(), level0, 1000);
    ASSERT_EQ(c.rows.size(), 1000u);
    EXPECT_EQ(c.rows.back().price, 2997);
    EXPECT_TRUE(c.cost.stopped);
    EXPECT_EQ(c.cost.rows_filtered, 1998u);
    EXPECT_EQ(c.batch_sizes.size(), 2u) << "nothing is read past the batch the reader stopped in";
    // And a reader that stops on a batch's first row without taking it: the rows the filter failed
    // before that row, which is none here, and the row itself not counted as filtered.
    uint64_t filtered = UINT64_MAX;
    const auto cost = s.store.scan_batches(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), level0,
                                           [&](const ob::RowBatch&) -> ob::BatchVerdict {
                                               return {true, 0};
                                           });
    filtered = cost.rows_filtered;
    EXPECT_TRUE(cost.stopped);
    EXPECT_EQ(filtered, 0u);
}

// ── A format-2 column cut short ────────────────────────────────────────────────────────────────

TEST(ScanBatches, AFormat2ColumnCutShortIsPaddedToTheSegmentsRowsWithZeros) {
    Store s(ob::kColumnarFormatV2);
    std::vector<ob::SnapshotRow> rows;
    for (uint64_t i = 0; i < 300; ++i) rows.push_back(row(kBase + i * kSec, 1, 0, 100 + static_cast<int64_t>(i), i));
    const auto meta = s.segment(rows);
    ASSERT_EQ(meta.format_version, ob::kColumnarFormatV2);
    const std::string qty = meta.dir_path + "/qty.col";
    fs::resize_file(qty, fs::file_size(qty) / 3 / 8 * 8);   // whole words, a third of them
    size_t qty_values = 0;
    std::vector<uint64_t> quantities;
    s.store.scan_batches(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), ob::RowFilter{},
                         [&](const ob::RowBatch& b) -> ob::BatchVerdict {
                             qty_values = b.qty.size();
                             for (const uint32_t i : b.rows) quantities.push_back(b.qty[i]);
                             return {};
                         });
    EXPECT_EQ(qty_values, 300u) << "the column is handed over whole";
    ASSERT_EQ(quantities.size(), 300u);
    EXPECT_EQ(quantities.front(), rows.front().quantity);
    EXPECT_EQ(quantities.back(), 0u) << "past the file's end a quantity reads as zero, as it always did";
}
