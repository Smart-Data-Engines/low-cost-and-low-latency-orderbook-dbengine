// Tests for #220: a format-3 segment's decoded columns held between queries.
//
// The first half holds the budget and its CLOCK to what they promise on their own. The second
// holds the store to the three things that make holding columns safe to have at all: a read
// answers the same with and without it, a column held is read without the file, and a segment
// that leaves the index - by a merge, retention, a drop, a snapshot installed under the same
// paths, a rebuild - takes its columns with it. Every refusal-shaped test has the control that
// would pass against a store that held nothing.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/decoded_columns.hpp"

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 1'790'000'000ULL * kSec;
constexpr size_t kMiB    = size_t{1} << 20;

std::atomic<uint64_t> g_counter{0};

struct TempDir {
    fs::path path;
    TempDir()
        : path(fs::temp_directory_path() / ("ob_decoded_columns_" + std::to_string(::getpid()) +
                                            "_" + std::to_string(g_counter.fetch_add(1)))) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

/// A row whose every field says where it came from, so a row of the wrong segment cannot pass for
/// the right one: the price is the caller's, and the rest follow from it and the time.
ob::SnapshotRow row_at(uint64_t ts, int64_t price, uint16_t level = 1) {
    ob::SnapshotRow row{};
    row.timestamp_ns    = ts;
    row.sequence_number = static_cast<uint64_t>(price) * 11 + ts % 7;
    row.side            = (price % 2 == 0) ? ob::SIDE_BID : ob::SIDE_ASK;
    row.level_index     = level;
    row.price           = price;
    row.quantity        = static_cast<uint64_t>(price) * 3;
    row.order_count     = static_cast<uint32_t>(price % 7);
    return row;
}

/// A segment as a seal writes it, by a store whose segments go elsewhere.
ob::SegmentMeta written(const std::string& base, const std::vector<ob::SnapshotRow>& rows,
                        const std::string& symbol = "A") {
    ob::ColumnarStore writer(base, ob::ColumnarStore::kDefaultSegmentDurationNs,
                             ob::ColumnarStore::OwnIndex::kNo);
    writer.set_symbol_exchange(symbol, "EX");
    writer.set_wal_position(77, 1, 0);
    for (const auto& r : rows) writer.append(r);
    auto meta = writer.flush_segment();
    EXPECT_TRUE(meta.has_value());
    EXPECT_EQ(meta.value_or(ob::SegmentMeta{}).format_version, ob::kColumnarFormatV3);
    return meta.value_or(ob::SegmentMeta{});
}

/// `n` rows from `t0`, a second apart, priced from `price0` - out of time order where `shuffled`
/// says, as #166's segments are.
std::vector<ob::SnapshotRow> rows_from(uint64_t t0, size_t n, int64_t price0, bool shuffled = false) {
    std::vector<ob::SnapshotRow> rows;
    rows.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        rows.push_back(row_at(t0 + i * kSec, price0 + static_cast<int64_t>(i),
                              static_cast<uint16_t>(i % 20)));
    }
    if (shuffled) {
        for (size_t i = 0; i + 3 < rows.size(); i += 5) std::swap(rows[i], rows[i + 3]);
    }
    return rows;
}

std::vector<ob::SnapshotRow> read(const ob::ColumnarStore& store, ob::ColumnSet columns,
                                  const std::string& symbol = "A", uint64_t lo = 0,
                                  uint64_t hi = UINT64_MAX) {
    std::vector<ob::SnapshotRow> out;
    store.scan(lo, hi, symbol, "EX", columns, [&](const ob::SnapshotRow& r) { out.push_back(r); });
    return out;
}

bool same_rows(const std::vector<ob::SnapshotRow>& a, const std::vector<ob::SnapshotRow>& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i].timestamp_ns != b[i].timestamp_ns || a[i].price != b[i].price ||
            a[i].quantity != b[i].quantity || a[i].order_count != b[i].order_count ||
            a[i].side != b[i].side || a[i].level_index != b[i].level_index ||
            a[i].sequence_number != b[i].sequence_number) {
            return false;
        }
    }
    return true;
}

ob::ColumnSet set_of(std::initializer_list<ob::QueryColumn> columns) {
    ob::ColumnSet s;
    for (auto c : columns) s.add(c);
    return s;
}

/// A column of `n` 64-bit values, for the budget's own tests.
ob::DecodedColumns::Column column_of(size_t n, uint64_t first = 1) {
    auto values = std::make_shared<ob::ColumnValues>();
    auto& v = values->values.emplace<std::vector<uint64_t>>(n);
    for (size_t i = 0; i < n; ++i) v[i] = first + i;
    return values;
}

} // namespace

// ── The budget and its CLOCK, on their own ─────────────────────────────────────────────

TEST(DecodedColumnsBudget, HoldsWhatFitsAndCountsHitsAndMisses) {
    auto budget = std::make_shared<ob::DecodedColumnsBudget>(kMiB);
    auto seg = std::make_shared<ob::DecodedColumns>(budget);
    EXPECT_EQ(seg->get(0), nullptr);
    const auto col = column_of(1000);
    const auto held = seg->put(0, col, "seg");
    EXPECT_EQ(held, col);
    EXPECT_EQ(seg->get(0), col) << "a column that fits was not held";
    EXPECT_EQ(budget->held(), col->bytes());
    EXPECT_EQ(seg->bytes(), col->bytes());
    const auto stats = budget->stats();
    EXPECT_EQ(stats.misses, 1u);
    EXPECT_EQ(stats.hits, 1u);
    EXPECT_EQ(stats.limit_bytes, kMiB);
}

TEST(DecodedColumnsBudget, TheFirstColumnPutIsTheOneHeld) {
    auto budget = std::make_shared<ob::DecodedColumnsBudget>(kMiB);
    auto seg = std::make_shared<ob::DecodedColumns>(budget);
    const auto first = column_of(100, 1);
    const auto second = column_of(100, 1000);
    EXPECT_EQ(seg->put(3, first, "seg"), first);
    EXPECT_EQ(seg->put(3, second, "seg"), first) << "a second read's decode replaced the first";
    EXPECT_EQ(budget->held(), first->bytes()) << "the column was counted twice";
}

TEST(DecodedColumnsBudget, AColumnLargerThanTheBudgetIsNotHeldButAnswersItsRead) {
    auto budget = std::make_shared<ob::DecodedColumnsBudget>(1000);
    auto seg = std::make_shared<ob::DecodedColumns>(budget);
    const auto big = column_of(10'000);
    EXPECT_EQ(seg->put(0, big, "seg"), big) << "the read lost the column it decoded";
    EXPECT_EQ(seg->get(0), nullptr);
    EXPECT_EQ(budget->held(), 0u);
    // The control: a column that fits is held by the same budget.
    const auto small = column_of(10);
    seg->put(1, small, "seg");
    EXPECT_EQ(seg->get(1), small);
}

TEST(DecodedColumnsBudget, TheHandEvictsASegmentNotReadAgainBeforeOneThatWas) {
    const auto col_a = column_of(1000, 1);
    const auto col_b = column_of(1000, 2);
    const auto col_c = column_of(1000, 3);
    // Room for two columns and not three.
    auto budget = std::make_shared<ob::DecodedColumnsBudget>(col_a->bytes() * 2 + col_a->bytes() / 2);
    auto a = std::make_shared<ob::DecodedColumns>(budget);
    auto b = std::make_shared<ob::DecodedColumns>(budget);
    auto c = std::make_shared<ob::DecodedColumns>(budget);
    a->put(0, col_a, "a");
    b->put(0, col_b, "b");
    ASSERT_EQ(a->get(0), col_a);   // read again: the hand passes it over once
    c->put(0, col_c, "c");
    EXPECT_EQ(a->get(0), col_a) << "a segment read again was evicted before one that was not";
    EXPECT_EQ(b->get(0), nullptr) << "the segment not read again was kept";
    EXPECT_EQ(c->get(0), col_c);
    EXPECT_EQ(budget->stats().evictions, 1u);
    EXPECT_LE(budget->held(), budget->limit());
}

TEST(DecodedColumnsBudget, AReadKeepsAColumnEvictedUnderIt) {
    const auto col_a = column_of(1000, 7);
    auto budget = std::make_shared<ob::DecodedColumnsBudget>(col_a->bytes() + col_a->bytes() / 2);
    auto a = std::make_shared<ob::DecodedColumns>(budget);
    auto b = std::make_shared<ob::DecodedColumns>(budget);
    a->put(0, col_a, "a");
    const auto reading = a->get(0);   // a read in progress
    b->put(0, column_of(1000, 9), "b");
    ASSERT_EQ(a->get(0), nullptr) << "nothing was evicted, so this test shows nothing";
    ASSERT_NE(reading, nullptr);
    EXPECT_EQ(reading->as<uint64_t>()[999], 7u + 999u) << "the read's column changed under it";
}

TEST(DecodedColumnsBudget, WhatIsHeldGoesBackWhenTheSegmentsGo) {
    auto budget = std::make_shared<ob::DecodedColumnsBudget>(kMiB);
    {
        auto a = std::make_shared<ob::DecodedColumns>(budget);
        auto b = std::make_shared<ob::DecodedColumns>(budget);
        a->put(0, column_of(1000), "a");
        b->put(2, column_of(500), "b");
        ASSERT_GT(budget->held(), 0u);
    }
    EXPECT_EQ(budget->held(), 0u) << "segments that are gone still count against the budget";
}

TEST(DecodedColumnsBudget, ASegmentIsNotEvictedToMakeRoomForItself) {
    const auto first = column_of(1000, 1);
    auto budget = std::make_shared<ob::DecodedColumnsBudget>(first->bytes() + first->bytes() / 2);
    auto seg = std::make_shared<ob::DecodedColumns>(budget);
    seg->put(0, first, "seg");
    const auto second = column_of(1000, 2);
    EXPECT_EQ(seg->put(1, second, "seg"), second) << "the read lost the column it decoded";
    EXPECT_EQ(seg->get(0), first) << "the segment evicted its own column for another of its own";
    EXPECT_EQ(seg->get(1), nullptr);
}

// ── The store ──────────────────────────────────────────────────────────────────────────

TEST(DecodedColumnsStore, ASecondReadOfAHeldSegmentReadsNoFile) {
    TempDir dir;
    const auto seg = written(dir.str(), rows_from(kBase, 1000, 100));
    ob::ColumnarStore store(dir.str());
    store.set_decoded_columns_budget(64 * kMiB);
    store.open_existing();
    const auto ts_price = set_of({ob::QueryColumn::TimestampNs, ob::QueryColumn::Price});
    const auto first = read(store, ts_price);
    ASSERT_EQ(first.size(), 1000u);

    // The file gone - and a read of what is held still answers, and the same.
    const std::string file = seg.dir_path + "/" + ob::kColumnsV3File;
    fs::rename(file, file + ".away");
    const auto again = read(store, ts_price);
    EXPECT_TRUE(same_rows(again, first)) << "a held column was read from the file, or read wrong";
    EXPECT_GE(store.decoded_columns_stats().hits, 2u);

    // The control: a column never read is not held, and its read answers as it did before - the
    // segment refused, its file missing while it is indexed.
    const auto seq = read(store, set_of({ob::QueryColumn::TimestampNs, ob::QueryColumn::SequenceNumber}));
    EXPECT_TRUE(seq.empty()) << "a column never read was answered without the file";
    fs::rename(file + ".away", file);
}

TEST(DecodedColumnsStore, ReadsWithAndWithoutHoldingAnswerTheSame) {
    TempDir dir;
    // Three segments: one row, a thousand out of time order, and ten thousand.
    written(dir.str(), rows_from(kBase, 1, 7));
    written(dir.str(), rows_from(kBase + 10'000 * kSec, 1000, 1000, /*shuffled=*/true));
    written(dir.str(), rows_from(kBase + 100'000 * kSec, 10'000, 50'000));
    ob::ColumnarStore plain(dir.str());
    plain.set_decoded_columns_budget(0);
    plain.open_existing();
    ob::ColumnarStore holding(dir.str());
    holding.set_decoded_columns_budget(64 * kMiB);
    holding.open_existing();

    const std::vector<ob::ColumnSet> sets = {
        ob::ColumnSet::all(),
        set_of({ob::QueryColumn::TimestampNs, ob::QueryColumn::Price}),
        set_of({ob::QueryColumn::TimestampNs, ob::QueryColumn::Side, ob::QueryColumn::Level}),
        set_of({ob::QueryColumn::TimestampNs, ob::QueryColumn::SequenceNumber,
                ob::QueryColumn::Quantity, ob::QueryColumn::OrderCount}),
    };
    for (const auto& columns : sets) {
        const auto expected = read(plain, columns);
        ASSERT_EQ(expected.size(), 11'001u);
        // Twice: the first decodes and holds, the second answers from what is held.
        EXPECT_TRUE(same_rows(read(holding, columns), expected));
        EXPECT_TRUE(same_rows(read(holding, columns), expected));
        // And a range inside one segment, across the out-of-order rows.
        const uint64_t lo = kBase + 10'100 * kSec, hi = kBase + 10'400 * kSec;
        EXPECT_TRUE(same_rows(read(holding, columns, "A", lo, hi), read(plain, columns, "A", lo, hi)));
    }
    // The book at an instant, and the rows in time order.
    const uint64_t at = kBase + 10'500 * kSec;
    for (int pass = 0; pass < 2; ++pass) {
        EXPECT_TRUE(same_rows(holding.latest_per_level(at, "A", "EX").rows,
                              plain.latest_per_level(at, "A", "EX").rows));
        std::vector<ob::SnapshotRow> a, b;
        const auto keep = [](const ob::SnapshotRow&) { return true; };
        holding.scan_by_time(0, UINT64_MAX, "A", "EX", ob::ColumnSet::all(), keep,
                             [&](const ob::SnapshotRow& r) { a.push_back(r); return true; });
        plain.scan_by_time(0, UINT64_MAX, "A", "EX", ob::ColumnSet::all(), keep,
                           [&](const ob::SnapshotRow& r) { b.push_back(r); return true; });
        EXPECT_TRUE(same_rows(a, b));
    }
    const auto stats = holding.decoded_columns_stats();
    EXPECT_GT(stats.hits, 0u) << "nothing was held, so the comparison above compared two plain reads";
    EXPECT_EQ(plain.decoded_columns_stats().held_bytes, 0u);
}

TEST(DecodedColumnsStore, AMergeTakesItsInputsColumnsWithIt) {
    TempDir dir;
    const auto a = written(dir.str(), rows_from(kBase, 100, 10));
    const auto b = written(dir.str(), rows_from(kBase + 200 * kSec, 100, 500));
    ob::ColumnarStore store(dir.str());
    store.set_decoded_columns_budget(64 * kMiB);
    store.open_existing();
    const auto before = read(store, ob::ColumnSet::all());
    ASSERT_EQ(before.size(), 200u);
    ASSERT_GT(store.decoded_columns_stats().held_bytes, 0u);

    // The merge, as the engine writes and publishes one: its output read from the inputs.
    ob::ColumnarStore writer(dir.str(), UINT64_MAX, ob::ColumnarStore::OwnIndex::kNo);
    writer.set_symbol_exchange("A", "EX");
    writer.set_wal_position(77, 1, 0);
    for (const auto& in : {a, b}) {
        ASSERT_TRUE(store.read_segment(in, [&](const ob::SnapshotRow& r) { writer.append(r); }));
    }
    writer.set_lineage(1, {ob::SegmentInput::of(a), ob::SegmentInput::of(b)}, b.last_row_ts_ns);
    const std::string staging = (fs::path(a.dir_path).parent_path() /
                                 ("1_1_1" + std::string(ob::ColumnarStore::kCompactingSuffix))).string();
    auto out = writer.flush_segment_into(staging);
    ASSERT_TRUE(out.has_value());
    const auto result = store.replace_segments({a, b}, *out, [](ob::SegmentMeta& m) {
        const fs::path to = fs::path(m.dir_path).parent_path() /
                            (std::to_string(m.start_ts_ns) + "_" + std::to_string(m.end_ts_ns) + "_m");
        std::error_code ec;
        fs::rename(m.dir_path, to, ec);
        m.dir_path = to.string();
        return !ec;
    });
    ASSERT_EQ(result.outcome, ob::ColumnarStore::Replaced::kYes);
    EXPECT_EQ(store.decoded_columns_stats().held_bytes, 0u)
        << "the inputs' columns outlived their segments, with no read holding them";
    EXPECT_TRUE(same_rows(read(store, ob::ColumnSet::all()), before));
    EXPECT_GT(store.decoded_columns_stats().held_bytes, 0u) << "the merged segment's columns not held";
}

TEST(DecodedColumnsStore, RetentionAndADropTakeTheirSegmentsColumnsWithThem) {
    TempDir dir;
    const auto old_seg = written(dir.str(), rows_from(kBase, 100, 10));
    const auto mid_seg = written(dir.str(), rows_from(kBase + 1000 * kSec, 100, 20));
    const auto new_seg = written(dir.str(), rows_from(kBase + 2000 * kSec, 100, 30));
    ob::ColumnarStore store(dir.str());
    store.set_decoded_columns_budget(64 * kMiB);
    store.open_existing();
    ASSERT_EQ(read(store, ob::ColumnSet::all()).size(), 300u);
    const size_t all_three = store.decoded_columns_stats().held_bytes;
    ASSERT_GT(all_three, 0u);

    const auto [removed, reclaimed] = store.delete_expired_segments(kBase + 500 * kSec);
    ASSERT_EQ(removed, 1u);
    (void)reclaimed;
    const size_t two = store.decoded_columns_stats().held_bytes;
    EXPECT_LT(two, all_three) << "retention's segment kept its columns";
    EXPECT_GT(two, 0u) << "the segments retention kept lost theirs";

    ASSERT_EQ(store.remove_segments({mid_seg.dir_path}), 1u);
    EXPECT_LT(store.decoded_columns_stats().held_bytes, two) << "a dropped segment kept its columns";
    EXPECT_EQ(read(store, ob::ColumnSet::all()).size(), 100u);
    (void)old_seg;
    (void)new_seg;
}

TEST(DecodedColumnsStore, ASnapshotInstalledUnderTheSamePathsIsReadNotTheColumnsBefore) {
    TempDir data;
    TempDir staging;
    // The same range - so the same directory name - and other prices.
    const auto ours = written(data.str(), rows_from(kBase, 50, 100), "SYM");
    const auto theirs = written(staging.str(), rows_from(kBase, 50, 9000), "SYM");
    const fs::path rel = fs::relative(fs::path(theirs.dir_path), staging.path);
    ASSERT_EQ(fs::relative(fs::path(ours.dir_path), data.path), rel) << "not the same path";

    ob::ColumnarStore store(data.str());
    store.set_decoded_columns_budget(64 * kMiB);
    store.open_existing();
    const auto before = read(store, ob::ColumnSet::all(), "SYM");
    ASSERT_EQ(before.size(), 50u);
    ASSERT_EQ(before.front().price, 100);

    ASSERT_TRUE(store.replace_from_staging(
        staging.str(), {(rel / "meta.json").string(), (rel / ob::kColumnsV3File).string()}));
    const auto after = read(store, ob::ColumnSet::all(), "SYM");
    ASSERT_EQ(after.size(), 50u);
    EXPECT_EQ(after.front().price, 9000) << "the columns of the segment the snapshot replaced were "
                                            "answered for the one it installed under the same path";
    EXPECT_EQ(after.back().price, 9049);
}

TEST(DecodedColumnsStore, ARebuildStartsEverySegmentEmpty) {
    TempDir dir;
    written(dir.str(), rows_from(kBase, 100, 10));
    ob::ColumnarStore store(dir.str());
    store.set_decoded_columns_budget(64 * kMiB);
    store.open_existing();
    ASSERT_EQ(read(store, ob::ColumnSet::all()).size(), 100u);
    ASSERT_GT(store.decoded_columns_stats().held_bytes, 0u);
    store.open_existing();
    EXPECT_EQ(store.decoded_columns_stats().held_bytes, 0u) << "a rebuilt index kept the old columns";
    const uint64_t misses_before = store.decoded_columns_stats().misses;
    EXPECT_EQ(read(store, ob::ColumnSet::all()).size(), 100u);
    EXPECT_GT(store.decoded_columns_stats().misses, misses_before) << "the rebuilt entry was not read";
}

TEST(DecodedColumnsStore, AFileThatFailsACheckLeavesNothingHeld) {
    TempDir dir;
    const auto seg = written(dir.str(), rows_from(kBase, 50, 10));
    ob::ColumnarStore store(dir.str());
    store.set_decoded_columns_budget(64 * kMiB);
    store.open_existing();
    {
        // The last byte is the sequence numbers' block's.
        const std::string file = seg.dir_path + "/" + ob::kColumnsV3File;
        const auto size = fs::file_size(file);
        std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
        f.seekg(static_cast<std::streamoff>(size - 1));
        char c = 0;
        f.read(&c, 1);
        c = static_cast<char>(c ^ 0x5A);
        f.seekp(static_cast<std::streamoff>(size - 1));
        f.write(&c, 1);
    }
    const auto with_seq = set_of({ob::QueryColumn::TimestampNs, ob::QueryColumn::SequenceNumber});
    EXPECT_TRUE(read(store, with_seq).empty()) << "a block whose checksum fails was decoded";
    EXPECT_EQ(store.decoded_columns_stats().held_bytes, 0u) << "a file that failed a check left columns";
    EXPECT_TRUE(read(store, with_seq).empty()) << "the second read was answered from memory";
    // The control: the columns whose blocks hold are read, and held.
    const auto ts_price = set_of({ob::QueryColumn::TimestampNs, ob::QueryColumn::Price});
    EXPECT_EQ(read(store, ts_price).size(), 50u);
    EXPECT_GT(store.decoded_columns_stats().held_bytes, 0u);
    EXPECT_TRUE(read(store, with_seq).empty());
}

TEST(DecodedColumnsStore, AMergesReadNeitherTakesNorLeavesColumns) {
    TempDir dir;
    const auto seg = written(dir.str(), rows_from(kBase, 100, 10));
    ob::ColumnarStore store(dir.str());
    store.set_decoded_columns_budget(64 * kMiB);
    store.open_existing();
    const auto meta = store.index().at(0);
    size_t rows = 0;
    ASSERT_TRUE(store.read_segment(meta, [&](const ob::SnapshotRow&) { ++rows; }));
    EXPECT_EQ(rows, 100u);
    const auto stats = store.decoded_columns_stats();
    EXPECT_EQ(stats.held_bytes, 0u) << "a merge's read held what it read";
    EXPECT_EQ(stats.hits + stats.misses, 0u) << "a merge's read asked the slot";
    // The control: a query's read of the same segment holds.
    EXPECT_EQ(read(store, ob::ColumnSet::all()).size(), 100u);
    EXPECT_GT(store.decoded_columns_stats().held_bytes, 0u);
    (void)seg;
}

TEST(DecodedColumnsStore, TheBudgetBoundsWhatIsHeldOnceReadsFinish) {
    TempDir dir;
    for (int s = 0; s < 6; ++s) {
        written(dir.str(), rows_from(kBase + static_cast<uint64_t>(s) * 100'000 * kSec, 5000,
                                     static_cast<int64_t>(s) * 10'000));
    }
    ob::ColumnarStore probe(dir.str());
    probe.set_decoded_columns_budget(64 * kMiB);
    probe.open_existing();
    read(probe, ob::ColumnSet::all(), "A", kBase, kBase + 50'000 * kSec);
    const size_t one_segment = probe.decoded_columns_stats().held_bytes;
    ASSERT_GT(one_segment, 0u);

    ob::ColumnarStore store(dir.str());
    store.set_decoded_columns_budget(one_segment * 2 + one_segment / 2);
    store.open_existing();
    for (int round = 0; round < 3; ++round) {
        EXPECT_EQ(read(store, ob::ColumnSet::all()).size(), 30'000u);
        const auto stats = store.decoded_columns_stats();
        EXPECT_LE(stats.held_bytes, stats.limit_bytes) << "round " << round;
        EXPECT_GT(stats.held_bytes, 0u) << "round " << round;
    }
    EXPECT_GT(store.decoded_columns_stats().evictions, 0u) << "six segments fit a budget of two and a half";
}

TEST(DecodedColumnsStore, ThreadsReadingTheSameSegmentsUnderEvictionAnswerTheSame) {
    TempDir dir;
    for (int s = 0; s < 16; ++s) {
        written(dir.str(), rows_from(kBase + static_cast<uint64_t>(s) * 10'000 * kSec, 500,
                                     static_cast<int64_t>(s) * 1000, s % 3 == 0));
    }
    ob::ColumnarStore plain(dir.str());
    plain.set_decoded_columns_budget(0);
    plain.open_existing();
    const auto expected_all = read(plain, ob::ColumnSet::all());
    const auto ts_price = set_of({ob::QueryColumn::TimestampNs, ob::QueryColumn::Price});
    const auto expected_price = read(plain, ts_price);
    ASSERT_EQ(expected_all.size(), 8000u);

    ob::ColumnarStore store(dir.str());
    store.open_existing();
    // Room for about four segments of sixteen: every round evicts.
    ob::ColumnarStore probe(dir.str());
    probe.set_decoded_columns_budget(64 * kMiB);
    probe.open_existing();
    read(probe, ob::ColumnSet::all(), "A", kBase, kBase + 5000 * kSec);
    store.set_decoded_columns_budget(probe.decoded_columns_stats().held_bytes * 4);

    std::atomic<int> wrong{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < 20; ++i) {
                const bool all = (t + i) % 2 == 0;
                const auto rows = read(store, all ? ob::ColumnSet::all() : ts_price);
                if (!same_rows(rows, all ? expected_all : expected_price)) wrong.fetch_add(1);
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(wrong.load(), 0) << "a read under eviction answered other rows";
    const auto stats = store.decoded_columns_stats();
    EXPECT_GT(stats.evictions, 0u) << "nothing was evicted, so the threads never raced one";
    EXPECT_GT(stats.hits, 0u);
    EXPECT_LE(stats.held_bytes, stats.limit_bytes);
}

TEST(DecodedColumnsStore, HoldingOffReadsAsBeforeAndTakesTheSlotsAway) {
    TempDir dir;
    written(dir.str(), rows_from(kBase, 100, 10));
    ob::ColumnarStore store(dir.str());
    store.open_existing();
    EXPECT_EQ(read(store, ob::ColumnSet::all()).size(), 100u);
    auto stats = store.decoded_columns_stats();
    EXPECT_EQ(stats.hits + stats.misses + stats.held_bytes + stats.limit_bytes, 0u)
        << "a store not given a budget held columns";
    EXPECT_EQ(store.index().at(0).decoded, nullptr);

    store.set_decoded_columns_budget(64 * kMiB);
    EXPECT_NE(store.index().at(0).decoded, nullptr) << "a segment indexed before the budget got no slot";
    EXPECT_EQ(read(store, ob::ColumnSet::all()).size(), 100u);
    EXPECT_GT(store.decoded_columns_stats().held_bytes, 0u);

    store.set_decoded_columns_budget(0);
    EXPECT_EQ(store.index().at(0).decoded, nullptr);
    EXPECT_EQ(store.decoded_columns_stats().held_bytes, 0u);
    EXPECT_EQ(read(store, ob::ColumnSet::all()).size(), 100u);
}
