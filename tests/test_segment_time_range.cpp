// #166: a segment's time range is the range of its rows.
//
// Until #166 `flush_segment()` recorded the start of the period a segment's first row fell in and
// the timestamp of its LAST row, while `SegmentMeta` declared "earliest" and "latest". Queries prune
// by that range and retention deletes by its end, so a row that reached a flush out of time order
// was outside it: skipped by a query that asked for it, and deleted with a segment retention judged
// by an older row. These pin the new range from the store's side, the repair of segments written
// before it, and the one number the change must not move - the last row's time, which replay's
// fallback reads. The wire-level measurements are in tests/integration/test_segment_time_range.py.

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/query_columns.hpp"

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kHour = 3600 * kSec;
// Ten minutes into an hour, so nothing is near a period's boundary unless a test puts it there.
constexpr uint64_t kBase = 1'790'000'000ULL * kSec / kHour * kHour + 600 * kSec;

std::atomic<uint64_t> g_counter{0};

struct TempDir {
    fs::path path;
    TempDir()
        : path(fs::temp_directory_path() /
               ("ob_time_range_" + std::to_string(::getpid()) + "_" +
                std::to_string(g_counter.fetch_add(1)))) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::permissions(path, fs::perms::owner_all, fs::perm_options::add, ec);
        for (auto& e : fs::recursive_directory_iterator(path, ec)) {
            fs::permissions(e.path(), fs::perms::owner_all, fs::perm_options::add, ec);
        }
        fs::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

ob::SnapshotRow row_at(uint64_t ts, int64_t price) {
    ob::SnapshotRow row{};
    row.timestamp_ns    = ts;
    row.sequence_number = 1;
    row.side            = ob::SIDE_BID;
    row.price           = price;
    row.quantity        = 1;
    row.order_count     = 1;
    return row;
}

std::vector<int64_t> prices_between(const ob::ColumnarStore& store, uint64_t lo, uint64_t hi) {
    std::vector<int64_t> out;
    store.scan(lo, hi, "SYM", "EX", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { out.push_back(r.price); });
    return out;
}

std::string read_file(const fs::path& p) {
    std::ifstream f(p);
    std::ostringstream s;
    s << f.rdbuf();
    return s.str();
}

void write_file(const fs::path& p, const std::string& content) {
    std::ofstream f(p, std::ios::trunc);
    f << content;
}

/// The one segment a store wrote, and its `meta.json`.
fs::path only_segment(const TempDir& dir) {
    std::vector<fs::path> segments;
    for (auto& e : fs::directory_iterator(dir.path / "SYM" / "EX")) segments.push_back(e.path());
    EXPECT_EQ(segments.size(), 1u);
    return segments.empty() ? fs::path() : segments.front();
}

/// Rewrite a segment's `meta.json` into what the writer before #166 produced: without
/// `last_row_ts_ns` and `time_range`, and with the range it recorded.
void as_written_before_the_fix(const fs::path& segment, uint64_t start, uint64_t end) {
    std::string meta = read_file(segment / "meta.json");
    ASSERT_NE(meta.find("\"time_range\":\"rows\""), std::string::npos)
        << "premise: the segment was written with its rows' range: " << meta;
    meta = std::regex_replace(meta, std::regex(",\"last_row_ts_ns\":[0-9]+"), "");
    meta = std::regex_replace(meta, std::regex(",\"time_range\":\"rows\""), "");
    meta = std::regex_replace(meta, std::regex("\"start_ts_ns\":[0-9]+"),
                              "\"start_ts_ns\":" + std::to_string(start));
    meta = std::regex_replace(meta, std::regex("\"end_ts_ns\":[0-9]+"),
                              "\"end_ts_ns\":" + std::to_string(end));
    write_file(segment / "meta.json", meta);
}

/// A store for SYM.EX with the two rows appended in the order given, flushed.
ob::SegmentMeta written(ob::ColumnarStore& store, std::vector<std::pair<uint64_t, int64_t>> rows) {
    store.set_symbol_exchange("SYM", "EX");
    for (auto [ts, price] : rows) store.append(row_at(ts, price));
    auto meta = store.flush_segment();
    EXPECT_TRUE(meta.has_value());
    return meta.value_or(ob::SegmentMeta{});
}

}  // namespace

TEST(SegmentTimeRange, RowsOutOfOrderGiveTheRowsRange) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto meta = written(store, {{kBase + 2 * kSec, 100}, {kBase + 1 * kSec, 101}});
    EXPECT_EQ(meta.start_ts_ns, kBase + 1 * kSec);
    EXPECT_EQ(meta.end_ts_ns, kBase + 2 * kSec) << "the end was the last row's time";
    EXPECT_EQ(meta.last_row_ts_ns, kBase + 1 * kSec);
    EXPECT_TRUE(meta.time_range_is_rows);
    EXPECT_EQ(prices_between(store, kBase + kSec + kSec / 2, kBase + 3 * kSec),
              (std::vector<int64_t>{100}));
}

TEST(SegmentTimeRange, ALateRowFromTheHourBeforeIsInsideItsSegment) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const uint64_t boundary = kBase / kHour * kHour + kHour;
    const auto meta = written(store, {{boundary + 5 * kSec, 200}, {boundary - 60 * kSec, 201}});
    EXPECT_EQ(meta.start_ts_ns, boundary - 60 * kSec) << "the start was the period's boundary";
    EXPECT_EQ(meta.end_ts_ns, boundary + 5 * kSec);
    EXPECT_LE(meta.start_ts_ns, meta.end_ts_ns);
    EXPECT_EQ(prices_between(store, boundary - 61 * kSec, boundary - 1),
              (std::vector<int64_t>{201}));
}

TEST(SegmentTimeRange, RowsInOrderKeepTheirEndAndStartAtTheFirstRow) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto meta = written(store, {{kBase + 1, 1}, {kBase + 2, 2}, {kBase + 3, 3}});
    EXPECT_EQ(meta.start_ts_ns, kBase + 1) << "the first row, not the start of its hour";
    EXPECT_EQ(meta.end_ts_ns, kBase + 3);
    EXPECT_EQ(meta.last_row_ts_ns, kBase + 3);
    const std::string name = only_segment(dir).filename().string();
    EXPECT_EQ(name, std::to_string(kBase + 1) + "_" + std::to_string(kBase + 3));
}

TEST(SegmentTimeRange, ARolloverStartsTheNextSegmentsRangeAtItsOwnFirstRow) {
    // The period decides when a segment rolls over; what the closed one records is its own rows.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    store.set_symbol_exchange("SYM", "EX");
    const uint64_t next_hour = kBase / kHour * kHour + kHour;
    store.append(row_at(kBase + 2 * kSec, 1));
    store.append(row_at(kBase + 1 * kSec, 2));
    store.append(row_at(next_hour + 7 * kSec, 3));   // rolls the first segment over
    store.append(row_at(next_hour + 3 * kSec, 4));
    auto rolled = store.take_rolled_segments();
    ASSERT_EQ(rolled.size(), 1u);
    EXPECT_EQ(rolled[0].start_ts_ns, kBase + 1 * kSec);
    EXPECT_EQ(rolled[0].end_ts_ns, kBase + 2 * kSec);
    const auto second = store.flush_segment();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->start_ts_ns, next_hour + 3 * kSec);
    EXPECT_EQ(second->end_ts_ns, next_hour + 7 * kSec);
}

TEST(SegmentTimeRange, RetentionKeepsASegmentWhoseNewestRowIsInsideTheWindow) {
    // Measured through the wire before the fix: 0 of 2 rows after the first sweep, with the one
    // written a second earlier among them.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const uint64_t now = kBase + 30 * 24 * kHour;
    written(store, {{now, 100}, {now - 48 * kHour, 101}});
    EXPECT_EQ(store.delete_expired_segments(now - 24 * kHour).first, 0u);
    EXPECT_EQ(prices_between(store, 0, UINT64_MAX).size(), 2u);
    // And retention still works: past the newest row, the segment goes.
    EXPECT_EQ(store.delete_expired_segments(now + 1).first, 1u);
}

TEST(SegmentTimeRange, TheRangeAndTheLastRowAreInMetaJsonAndReadBack) {
    TempDir dir;
    {
        ob::ColumnarStore store(dir.str());
        written(store, {{kBase + 2 * kSec, 100}, {kBase + 1 * kSec, 101}});
    }
    const std::string text = read_file(only_segment(dir) / "meta.json");
    EXPECT_NE(text.find("\"time_range\":\"rows\""), std::string::npos) << text;
    EXPECT_NE(text.find("\"last_row_ts_ns\":" + std::to_string(kBase + 1 * kSec)),
              std::string::npos) << text;

    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.last_rebuild_ranges_read(), 0u) << "a segment of the new format was read";
    const auto index = reopened.index();
    ASSERT_EQ(index.size(), 1u);
    EXPECT_EQ(index[0].start_ts_ns, kBase + 1 * kSec);
    EXPECT_EQ(index[0].end_ts_ns, kBase + 2 * kSec);
    EXPECT_EQ(index[0].last_row_ts_ns, kBase + 1 * kSec);
    EXPECT_TRUE(index[0].time_range_is_rows);
}

TEST(SegmentTimeRange, ASegmentWrittenBeforeTheFixCarriesItsRecordedEndAsTheLastRow) {
    // Replay's fallback has always compared with the recorded end, which was the last row's time;
    // reading a file without `last_row_ts_ns` as 0 would make it skip nothing.
    TempDir dir;
    {
        ob::ColumnarStore store(dir.str());
        written(store, {{kBase + 1, 1}, {kBase + 2, 2}});
    }
    as_written_before_the_fix(only_segment(dir), kBase / kHour * kHour, kBase + 2);

    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.last_rebuild_ranges_read(), 1u) << "the old format was not recognised";
    const auto index = reopened.index();
    ASSERT_EQ(index.size(), 1u);
    EXPECT_EQ(index[0].last_row_ts_ns, kBase + 2);
    EXPECT_EQ(index[0].start_ts_ns, kBase + 1) << "the repair did not give it its rows' start";
}

TEST(SegmentTimeRange, ASegmentWrittenBeforeTheFixIsRepairedInTheIndexAndOnDisk) {
    TempDir dir;
    {
        ob::ColumnarStore store(dir.str());
        written(store, {{kBase + 2 * kSec, 100}, {kBase + 1 * kSec, 101}});
    }
    const fs::path segment = only_segment(dir);
    // What the old writer recorded for these two rows: the hour, and the last row.
    as_written_before_the_fix(segment, kBase / kHour * kHour, kBase + 1 * kSec);

    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.last_rebuild_ranges_read(), 1u);
    EXPECT_EQ(prices_between(reopened, kBase + kSec + kSec / 2, kBase + 3 * kSec),
              (std::vector<int64_t>{100}));
    const std::string text = read_file(segment / "meta.json");
    EXPECT_NE(text.find("\"time_range\":\"rows\""), std::string::npos) << text;
    EXPECT_NE(text.find("\"end_ts_ns\":" + std::to_string(kBase + 2 * kSec)), std::string::npos)
        << text;
    EXPECT_NE(text.find("\"last_row_ts_ns\":" + std::to_string(kBase + 1 * kSec)),
              std::string::npos) << "the repair moved the number replay reads: " << text;
    EXPECT_FALSE(fs::exists(segment / "meta.json.range"));

    ob::ColumnarStore again(dir.str());
    again.open_existing();
    EXPECT_EQ(again.last_rebuild_ranges_read(), 0u) << "the repair did not reach the disk";
    EXPECT_EQ(prices_between(again, kBase + kSec + kSec / 2, kBase + 3 * kSec),
              (std::vector<int64_t>{100}));
}

TEST(SegmentTimeRange, WhatAnUnfinishedRepairLeftIsRemovedAndTheSegmentStaysReadable) {
    TempDir dir;
    {
        ob::ColumnarStore store(dir.str());
        written(store, {{kBase + 1, 1}, {kBase + 2, 2}});
    }
    const fs::path segment = only_segment(dir);
    write_file(segment / "meta.json.range", "{\"half\":");   // a repair killed mid-write

    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_FALSE(fs::exists(segment / "meta.json.range"));
    EXPECT_EQ(prices_between(reopened, 0, UINT64_MAX), (std::vector<int64_t>{1, 2}));
}

TEST(SegmentTimeRange, ARepairThatCannotBeWrittenStillCorrectsTheIndex) {
    ASSERT_NE(::geteuid(), 0u) << "the premise is a directory this process cannot write, which "
                                  "root always can";
    TempDir dir;
    {
        ob::ColumnarStore store(dir.str());
        written(store, {{kBase + 2 * kSec, 100}, {kBase + 1 * kSec, 101}});
    }
    const fs::path segment = only_segment(dir);
    as_written_before_the_fix(segment, kBase / kHour * kHour, kBase + 1 * kSec);
    const std::string before = read_file(segment / "meta.json");
    fs::permissions(segment, fs::perms::owner_read | fs::perms::owner_exec);
    ASSERT_NE(::access(segment.c_str(), W_OK), 0) << "premise: the segment is still writable";

    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    fs::permissions(segment, fs::perms::owner_all);
    EXPECT_EQ(prices_between(reopened, kBase + kSec + kSec / 2, kBase + 3 * kSec),
              (std::vector<int64_t>{100}))
        << "the index was not corrected because the disk could not be";
    EXPECT_EQ(read_file(segment / "meta.json"), before);
}

TEST(SegmentTimeRange, ASegmentInstalledFromAnOlderPrimaryIsRepairedToo) {
    // A snapshot from a node running a build before #166 ships meta.json files of the old format;
    // replace_from_staging() rebuilds the index through the same repair as open_existing().
    TempDir source;
    {
        ob::ColumnarStore store(source.str());
        written(store, {{kBase + 2 * kSec, 100}, {kBase + 1 * kSec, 101}});
    }
    const fs::path segment = only_segment(source);
    as_written_before_the_fix(segment, kBase / kHour * kHour, kBase + 1 * kSec);

    TempDir target;
    const fs::path staging = target.path / "staging";
    const std::string rel = (fs::path("SYM") / "EX" / segment.filename()).string();
    std::vector<std::string> files;
    for (auto& e : fs::directory_iterator(segment)) {
        const std::string name = e.path().filename().string();
        fs::create_directories(staging / rel);
        fs::copy_file(e.path(), staging / rel / name);
        files.push_back(rel + "/" + name);
    }
    ob::ColumnarStore store(target.str());
    ASSERT_TRUE(store.replace_from_staging(staging.string(), files));
    EXPECT_EQ(store.last_rebuild_ranges_read(), 1u);
    EXPECT_EQ(prices_between(store, kBase + kSec + kSec / 2, kBase + 3 * kSec),
              (std::vector<int64_t>{100}));
}
