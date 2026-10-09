// Tests for #47 step 2: a query's conditions narrow the read, not only the answer.
//
// A scan given a `RowFilter` leaves unread a segment whose metadata - its price range, its levels -
// proves no row of it meets the filter, does not build a row that fails it, and ends when its reader
// says so. None of that may change an answer: the property below holds a filtered scan to the scan
// without it, filtered afterwards, row for row and in the same order, over segments whose metadata
// is known and segments whose metadata is not, and over blocks no seal has written yet.

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <regex>
#include <sstream>
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

ob::SnapshotRow row(uint64_t ts, uint8_t side, uint16_t level, int64_t price) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = static_cast<uint64_t>(price + 1000) * 7 + ts % 5;
    r.side            = side;
    r.level_index     = level;
    r.price           = price;
    r.quantity        = static_cast<uint64_t>(price + 1000) % 9 + 1;
    r.order_count     = static_cast<uint32_t>(level % 3 + 1);
    return r;
}

struct Fixture {
    fs::path dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    ob::QueryEngine engine;

    Fixture()
        : dir(fs::temp_directory_path() / ("ob_pushdown_" + std::to_string(::getpid()) + "_" +
                                           std::to_string(g_counter.fetch_add(1))))
        , store(made(dir))
        , engine(store, [](const std::string&) -> std::shared_ptr<ob::SoABuffer> { return nullptr; },
                 agg) {}
    ~Fixture() {
        store.close();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    /// One segment of BK.EX holding these rows, sealed by the store: its metadata as written.
    ob::SegmentMeta segment(const std::vector<ob::SnapshotRow>& rows) {
        store.set_symbol_exchange("BK", "EX");
        for (const auto& r : rows) store.append(r);
        auto meta = store.flush_segment();
        EXPECT_TRUE(meta.has_value());
        return meta.value_or(ob::SegmentMeta{});
    }
    /// One segment written beside the index and put in it with less metadata than it has, as a
    /// build before #47 step 2 - or before #47 step 1 - left it: the price range unknown, and with
    /// `no_levels` the level set too.
    void segment_unknown(const std::vector<ob::SnapshotRow>& rows, bool no_levels) {
        ob::ColumnarStore writer(dir.string(), ob::ColumnarStore::kDefaultSegmentDurationNs,
                                 ob::ColumnarStore::OwnIndex::kNo);
        writer.set_symbol_exchange("BK", "EX");
        for (const auto& r : rows) writer.append(r);
        auto meta = writer.flush_segment();
        ASSERT_TRUE(meta.has_value());
        meta->has_price_range = false;
        if (no_levels) meta->levels = nullptr;
        store.merge_segments({*meta});
    }
    void block(const std::vector<ob::SnapshotRow>& rows) {
        store.publish_blocks({ob::RowBlock::make("BK", "EX", rows)});
    }

    std::vector<ob::SnapshotRow> scanned(uint64_t from, uint64_t to) const {
        std::vector<ob::SnapshotRow> out;
        store.scan(from, to, "BK", "EX", ob::ColumnSet::all(),
                   [&](const ob::SnapshotRow& r) { out.push_back(r); });
        return out;
    }
    std::vector<ob::SnapshotRow> filtered(uint64_t from, uint64_t to, const ob::RowFilter& filter,
                                          size_t stop_after = SIZE_MAX,
                                          ob::ColumnarStore::ScanCost* cost = nullptr) const {
        std::vector<ob::SnapshotRow> out;
        const auto c = store.scan(from, to, "BK", "EX", ob::ColumnSet::all(), filter,
                                  [&](const ob::SnapshotRow& r) {
                                      out.push_back(r);
                                      return out.size() < stop_after;
                                  });
        if (cost != nullptr) *cost = c;
        return out;
    }

    struct Answer {
        std::string error;
        ob::QueryShape shape;
        std::vector<ob::QueryResult> rows;
    };
    Answer run(const std::string& sql) {
        Answer a;
        a.error = engine.execute(sql, [&](const ob::QueryResult& r) { a.rows.push_back(r); }, a.shape);
        return a;
    }
};

bool same_row(const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
    return a.timestamp_ns == b.timestamp_ns && a.sequence_number == b.sequence_number &&
           a.side == b.side && a.level_index == b.level_index && a.price == b.price &&
           a.quantity == b.quantity && a.order_count == b.order_count;
}

bool same_rows(const std::vector<ob::SnapshotRow>& a, const std::vector<ob::SnapshotRow>& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (!same_row(a[i], b[i])) return false;
    }
    return true;
}

std::vector<ob::SnapshotRow> kept(const std::vector<ob::SnapshotRow>& rows, const ob::RowFilter& f) {
    std::vector<ob::SnapshotRow> out;
    for (const auto& r : rows) {
        if (f.keeps(r.price, r.side, r.level_index)) out.push_back(r);
    }
    return out;
}

} // namespace

// ── The property: a filtered scan is the scan, filtered after ────────────────────────────────

RC_GTEST_PROP(ScanPushdownProperty, AFilteredScanIsTheScanFilteredAfterRowForRowInOrder, ()) {
    Fixture f;
    const auto rows_of = [&](int n) {
        std::vector<ob::SnapshotRow> rows;
        for (int i = 0; i < n; ++i) {
            // Now and then a level past the set's, which leaves the segment without one.
            const auto level = static_cast<uint16_t>(*rc::gen::inRange(0, 6));
            const bool past = level == 5 && *rc::gen::inRange(0, 4) == 0;
            rows.push_back(row(kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 60)) * (kSec / 2),
                               static_cast<uint8_t>(*rc::gen::inRange(0, 2)),
                               past ? uint16_t{1500} : level, *rc::gen::inRange<int64_t>(-50, 51)));
        }
        return rows;
    };
    const int segments = *rc::gen::inRange(1, 7);
    for (int s = 0; s < segments; ++s) {
        switch (*rc::gen::inRange(0, 3)) {
        case 0:  f.segment(rows_of(*rc::gen::inRange(1, 12))); break;
        case 1:  f.segment_unknown(rows_of(*rc::gen::inRange(1, 12)), false); break;
        default: f.segment_unknown(rows_of(*rc::gen::inRange(1, 12)), true); break;
        }
    }
    const int blocks = *rc::gen::inRange(0, 3);
    for (int b = 0; b < blocks; ++b) f.block(rows_of(*rc::gen::inRange(1, 6)));

    ob::RowFilter filter;
    const auto maybe = [] { return *rc::gen::inRange(0, 3) == 0; };
    if (maybe()) filter.price_lo = *rc::gen::inRange<int64_t>(-60, 61);
    if (maybe()) filter.price_hi = *rc::gen::inRange<int64_t>(-60, 61);
    if (maybe()) filter.side_lo = static_cast<uint8_t>(*rc::gen::inRange(0, 3));
    if (maybe()) filter.side_hi = static_cast<uint8_t>(*rc::gen::inRange(0, 3));
    if (maybe()) filter.level_lo = static_cast<uint16_t>(*rc::gen::element(0, 1, 3, 5, 1500, 2000));
    if (maybe()) filter.level_hi = static_cast<uint16_t>(*rc::gen::element(0, 1, 3, 5, 1500, 2000));
    const uint64_t from = kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 40)) * (kSec / 2);
    const uint64_t to = from + static_cast<uint64_t>(*rc::gen::inRange(0, 50)) * (kSec / 2);

    const auto expected = kept(f.scanned(from, to), filter);
    ob::ColumnarStore::ScanCost cost;
    const auto got = f.filtered(from, to, filter, SIZE_MAX, &cost);
    RC_ASSERT(same_rows(got, expected));
    RC_ASSERT(!cost.stopped);

    // Ended by the reader after k rows: the first k of the same rows.
    const size_t k = static_cast<size_t>(*rc::gen::inRange<int>(1, static_cast<int>(expected.size()) + 2));
    const auto first = f.filtered(from, to, filter, k);
    const std::vector<ob::SnapshotRow> prefix(expected.begin(),
                                              expected.begin() + static_cast<std::ptrdiff_t>(std::min(k, expected.size())));
    RC_ASSERT(same_rows(first, prefix));
}

// ── Segments left unread ───────────────────────────────────────────────────────────────

TEST(ScanPushdown, ASegmentOutsideThePriceBandIsLeftUnread) {
    Fixture f;
    f.segment({row(kBase + 1 * kSec, ob::SIDE_BID, 0, 100), row(kBase + 2 * kSec, ob::SIDE_BID, 1, 110)});
    f.segment({row(kBase + 3 * kSec, ob::SIDE_ASK, 0, 200), row(kBase + 4 * kSec, ob::SIDE_ASK, 1, 210)});
    ob::RowFilter band;
    band.price_lo = 150;
    band.price_hi = 250;
    ob::ColumnarStore::ScanCost cost;
    const auto rows = f.filtered(0, UINT64_MAX, band, SIZE_MAX, &cost);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].price, 200);
    EXPECT_EQ(cost.skipped_by_price, 1u) << "the segment of prices 100 - 110 was read";
    EXPECT_EQ(cost.segments_read, 1u);
    // The control: a band holding both reads both.
    band.price_lo = 0;
    const auto all = f.filtered(0, UINT64_MAX, band, SIZE_MAX, &cost);
    EXPECT_EQ(all.size(), 4u);
    EXPECT_EQ(cost.skipped_by_price, 0u);
    EXPECT_EQ(cost.segments_read, 2u);
}

TEST(ScanPushdown, ASegmentWithoutAPairTheConditionsAllowIsLeftUnread) {
    Fixture f;
    f.segment({row(kBase + 1 * kSec, ob::SIDE_BID, 0, 10), row(kBase + 2 * kSec, ob::SIDE_BID, 2, 11)});
    f.segment({row(kBase + 3 * kSec, ob::SIDE_ASK, 5, 12), row(kBase + 4 * kSec, ob::SIDE_ASK, 7, 13)});
    ob::RowFilter ask5;
    ask5.side_lo = ask5.side_hi = ob::SIDE_ASK;
    ask5.level_lo = ask5.level_hi = 5;
    ob::ColumnarStore::ScanCost cost;
    const auto rows = f.filtered(0, UINT64_MAX, ask5, SIZE_MAX, &cost);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].price, 12);
    EXPECT_EQ(cost.skipped_by_levels, 1u) << "the segment of bid levels 0 and 2 was read";
    // A level the set cannot hold: every row of a segment with a set is below it.
    ob::RowFilter past;
    past.level_lo = 1500;
    const auto none = f.filtered(0, UINT64_MAX, past, SIZE_MAX, &cost);
    EXPECT_TRUE(none.empty());
    EXPECT_EQ(cost.skipped_by_levels, 2u);
    EXPECT_EQ(cost.segments_read, 0u);
}

TEST(ScanPushdown, ASegmentWhoseMetadataIsUnknownIsRead) {
    Fixture f;
    f.segment_unknown({row(kBase + 1 * kSec, ob::SIDE_BID, 0, 100)}, true);
    ob::RowFilter elsewhere;
    elsewhere.price_lo = 500;
    elsewhere.level_lo = elsewhere.level_hi = 9;
    ob::ColumnarStore::ScanCost cost;
    const auto rows = f.filtered(0, UINT64_MAX, elsewhere, SIZE_MAX, &cost);
    EXPECT_TRUE(rows.empty());
    EXPECT_EQ(cost.segments_read, 1u) << "a segment whose metadata proves nothing was left unread";
    EXPECT_EQ(cost.skipped_by_price + cost.skipped_by_levels, 0u);
    EXPECT_EQ(cost.rows_filtered, 1u) << "its row was not filtered before it was built";
}

TEST(ScanPushdown, AScanEndedByItsReaderOpensNoFurtherSegment) {
    Fixture f;
    for (int s = 0; s < 5; ++s) {
        f.segment({row(kBase + static_cast<uint64_t>(s * 2 + 1) * kSec, ob::SIDE_BID, 0, s),
                   row(kBase + static_cast<uint64_t>(s * 2 + 2) * kSec, ob::SIDE_BID, 1, s)});
    }
    ob::ColumnarStore::ScanCost cost;
    const auto rows = f.filtered(0, UINT64_MAX, ob::RowFilter{}, 1, &cost);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_TRUE(cost.stopped);
    EXPECT_EQ(cost.segments_read, 1u) << "the scan went on reading after its reader had what it wanted";
    // And blocks: one ended inside them reads no further row of them.
    f.block({row(kBase + 20 * kSec, ob::SIDE_BID, 0, 99), row(kBase + 21 * kSec, ob::SIDE_BID, 0, 98)});
    const auto eleven = f.filtered(0, UINT64_MAX, ob::RowFilter{}, 11, &cost);
    ASSERT_EQ(eleven.size(), 11u);
    EXPECT_EQ(eleven.back().price, 99);
    EXPECT_TRUE(cost.stopped);
}

TEST(ScanPushdown, ATimeOrderedScanLeavesASegmentWithoutItsLevelsUnread) {
    // The series' read (#44 step 2): level 0 of either side, by time.
    Fixture f;
    f.segment({row(kBase + 1 * kSec, ob::SIDE_BID, 3, 10), row(kBase + 2 * kSec, ob::SIDE_ASK, 4, 11)});
    f.segment({row(kBase + 3 * kSec, ob::SIDE_BID, 0, 12), row(kBase + 4 * kSec, ob::SIDE_ASK, 1, 13)});
    ob::RowFilter level0;
    level0.level_lo = level0.level_hi = 0;
    std::vector<ob::SnapshotRow> got;
    const auto cost = f.store.scan_by_time(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), level0,
                                           [&](const ob::SnapshotRow& r) {
                                               got.push_back(r);
                                               return true;
                                           });
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].price, 12);
    EXPECT_EQ(cost.skipped, 1u) << "the segment of levels 3 and 4 was read";
    EXPECT_EQ(cost.rows_filtered, 1u) << "the level-1 row was built and then dropped";
    EXPECT_EQ(cost.kept, 1u);
}

// ── The selection pass (#224) ──────────────────────────────────────────────────────────

TEST(ScanPushdown, ASelectionAcrossChunksStopsAndCountsAsTheRowByRowLoopDid) {
    // 5 000 rows - the selection's chunks are 2 048 - level 0 and level 1 by turns.
    Fixture f;
    std::vector<ob::SnapshotRow> rows;
    for (int i = 0; i < 5000; ++i) {
        rows.push_back(row(kBase + static_cast<uint64_t>(i) * (kSec / 10), ob::SIDE_BID,
                           static_cast<uint16_t>(i % 2), i));
    }
    f.segment(rows);
    ob::RowFilter level0;
    level0.level_lo = level0.level_hi = 0;
    ob::ColumnarStore::ScanCost cost;
    const auto all = f.filtered(0, UINT64_MAX, level0, SIZE_MAX, &cost);
    ASSERT_EQ(all.size(), 2500u);
    EXPECT_EQ(all[1249].price, 2498);
    EXPECT_EQ(all[1250].price, 2500) << "a row was lost or doubled at a chunk's edge";
    EXPECT_EQ(cost.rows_filtered, 2500u);
    // Stopped by the reader in the second chunk: the rows the filter failed before it, no more.
    const auto first = f.filtered(0, UINT64_MAX, level0, 1500, &cost);
    ASSERT_EQ(first.size(), 1500u);
    EXPECT_EQ(first.back().price, 2998);
    EXPECT_TRUE(cost.stopped);
    EXPECT_EQ(cost.rows_filtered, 1499u);
    // And a time range that cuts the chunks: the rows of [1000, 3999] the filter keeps.
    const auto middle = f.filtered(kBase + 1000 * (kSec / 10), kBase + 3999 * (kSec / 10), level0);
    ASSERT_EQ(middle.size(), 1500u);
    EXPECT_EQ(middle.front().price, 1000);
    EXPECT_EQ(middle.back().price, 3998);
}

TEST(ScanPushdown, AFormat2SegmentWithItsPriceCutShortIsFilteredRowByRowAsBefore) {
    // A price column a short file left short is padded with zeros, and the selection pass needs
    // whole columns: this read goes through the loop it falls back to, and answers as the scan
    // filtered after does.
    Fixture f;
    auto format = f.store.segment_format();
    format.version = ob::kColumnarFormatV2;
    f.store.set_segment_format(format);
    std::vector<ob::SnapshotRow> rows;
    for (int i = 0; i < 300; ++i) {
        rows.push_back(row(kBase + static_cast<uint64_t>(i) * kSec, ob::SIDE_ASK, 0, 100 + i));
    }
    const auto meta = f.segment(rows);
    ASSERT_EQ(meta.format_version, ob::kColumnarFormatV2);
    const std::string price = meta.dir_path + "/price.col";
    fs::resize_file(price, fs::file_size(price) / 2);
    ob::RowFilter low;
    low.price_hi = 150;   // keeps the padded zeros as well as the prices up to 150
    ob::ColumnarStore::ScanCost cost;
    const auto got = f.filtered(0, UINT64_MAX, low, SIZE_MAX, &cost);
    const auto expected = kept(f.scanned(0, UINT64_MAX), low);
    ASSERT_FALSE(expected.empty());
    EXPECT_TRUE(same_rows(got, expected));
    EXPECT_EQ(cost.rows_filtered, f.scanned(0, UINT64_MAX).size() - expected.size());
}

// ── The price range in meta.json ──────────────────────────────────────────────────────

TEST(ScanPushdown, ASegmentsPriceRangeGoesToMetaJsonAndComesBack) {
    Fixture f;
    const auto meta = f.segment({row(kBase + 1 * kSec, ob::SIDE_BID, 0, -7), row(kBase + 2 * kSec, ob::SIDE_ASK, 3, 42),
                                 row(kBase + 3 * kSec, ob::SIDE_BID, 1, 5)});
    ASSERT_TRUE(meta.has_price_range);
    EXPECT_EQ(meta.min_price, -7);
    EXPECT_EQ(meta.max_price, 42);
    ob::ColumnarStore reopened(f.dir.string());
    reopened.open_existing();
    ASSERT_EQ(reopened.index().size(), 1u);
    const auto read = reopened.index()[0];
    EXPECT_TRUE(read.has_price_range);
    EXPECT_EQ(read.min_price, -7);
    EXPECT_EQ(read.max_price, 42);
}

TEST(ScanPushdown, AMetaJsonWithoutAPriceRangeReadsAsUnknown) {
    Fixture f;
    const auto meta = f.segment({row(kBase + 1 * kSec, ob::SIDE_BID, 0, 100)});
    const std::string path = meta.dir_path + "/meta.json";
    std::ifstream in(path);
    std::string json((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();
    ASSERT_NE(json.find("\"min_price\":100"), std::string::npos) << json;
    // What a build before #47 step 2 wrote; and a range the wrong way round, which nothing writes.
    for (const std::string& variant :
         {std::regex_replace(json, std::regex(",\"min_price\":-?[0-9]+,\"max_price\":-?[0-9]+"), ""),
          std::regex_replace(json, std::regex("\"min_price\":100"), "\"min_price\":101,\"x\":0"),
          std::regex_replace(json, std::regex("\"max_price\":100"), "\"max_price\":99")}) {
        std::ofstream(path, std::ios::trunc) << variant;
        ob::ColumnarStore reopened(f.dir.string());
        reopened.open_existing();
        ASSERT_EQ(reopened.index().size(), 1u) << variant;
        EXPECT_FALSE(reopened.index()[0].has_price_range) << variant;
    }
}

// ── The query engine ───────────────────────────────────────────────────────────────────

TEST(ScanPushdown, ALimitOfZeroAnswersNothingAndAConditionAnswersAsBefore) {
    Fixture f;
    f.segment({row(kBase + 1 * kSec, ob::SIDE_BID, 0, 100), row(kBase + 2 * kSec, ob::SIDE_ASK, 1, 150)});
    f.segment({row(kBase + 3 * kSec, ob::SIDE_BID, 0, 300), row(kBase + 4 * kSec, ob::SIDE_ASK, 2, 350)});
    const auto zero = f.run("SELECT * FROM 'BK'.'EX' LIMIT 0");
    EXPECT_TRUE(zero.error.empty()) << zero.error;
    EXPECT_TRUE(zero.rows.empty());
    EXPECT_FALSE(zero.shape.columns.empty()) << "an answer of no rows still has its header's columns";

    const auto band = f.run("SELECT price FROM 'BK'.'EX' WHERE price BETWEEN 120 AND 320");
    ASSERT_TRUE(band.error.empty()) << band.error;
    ASSERT_EQ(band.rows.size(), 2u);
    EXPECT_EQ(band.rows[0].price, 150);
    EXPECT_EQ(band.rows[1].price, 300);

    const auto first = f.run("SELECT price FROM 'BK'.'EX' WHERE side = 0 LIMIT 1");
    ASSERT_TRUE(first.error.empty()) << first.error;
    ASSERT_EQ(first.rows.size(), 1u);
    EXPECT_EQ(first.rows[0].price, 100);
}
