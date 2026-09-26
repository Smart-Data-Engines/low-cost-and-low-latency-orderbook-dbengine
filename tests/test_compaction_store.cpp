// #165 part 2b, the store's half: what a merge reads, writes and publishes.
//
// A merge reads a run of a symbol's segments whole, writes their rows as one segment into a working
// directory no reader takes for a segment, and publishes it by swapping it for its inputs under the
// index's lock. What these hold is what makes that safe to do under a query: the swap is one step,
// it refuses inputs another segment lies between and an output that would sort elsewhere - either
// would change the order a scan delivers rows in, which a SNAPSHOT tie and a LIMIT depend on - a
// scan that copied an input keeps it readable until it ends, and a rebuild after a crash keeps each
// row once: it removes a merge nothing published, and the inputs found beside the merged segment
// that replaced them, told from a later segment that took an input's name by what each recorded.

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/query_columns.hpp"

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 1'790'000'000ULL * kSec;

std::atomic<uint64_t> g_counter{0};

struct TempDir {
    fs::path path;
    TempDir()
        : path(fs::temp_directory_path() /
               ("ob_compaction_store_" + std::to_string(::getpid()) + "_" +
                std::to_string(g_counter.fetch_add(1)))) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

ob::SnapshotRow row_at(uint64_t ts, int64_t price, uint16_t level = 1) {
    ob::SnapshotRow row{};
    row.timestamp_ns    = ts;
    row.sequence_number = static_cast<uint64_t>(price);
    row.side            = ob::SIDE_ASK;
    row.level_index     = level;
    row.price           = price;
    row.quantity        = static_cast<uint64_t>(price) * 3;
    row.order_count     = static_cast<uint32_t>(price % 7);
    return row;
}

struct Stamp {
    uint64_t identity{77};
    uint32_t file{1};
    uint64_t offset{0};
    uint64_t epoch{0};
};

/// A segment as a seal writes it, by a store whose segments go elsewhere.
ob::SegmentMeta written(const std::string& base, const std::string& symbol,
                        const std::vector<std::pair<uint64_t, int64_t>>& rows, Stamp stamp = {}) {
    ob::ColumnarStore writer(base, ob::ColumnarStore::kDefaultSegmentDurationNs,
                             ob::ColumnarStore::OwnIndex::kNo);
    writer.set_symbol_exchange(symbol, "EX");
    writer.set_wal_position(stamp.identity, stamp.file, stamp.offset);
    writer.set_seal_epoch(stamp.epoch);
    for (const auto& [ts, price] : rows) writer.append(row_at(ts, price));
    auto meta = writer.flush_segment();
    EXPECT_TRUE(meta.has_value());
    return meta.value_or(ob::SegmentMeta{});
}

/// The merge of `inputs`, read from `store` and written into its working directory beside them.
ob::SegmentMeta merged(const ob::ColumnarStore& store, const std::string& base,
                       const std::vector<ob::SegmentMeta>& inputs) {
    ob::ColumnarStore writer(base, UINT64_MAX, ob::ColumnarStore::OwnIndex::kNo);
    writer.set_symbol_exchange(inputs.front().symbol, inputs.front().exchange);
    uint64_t last_row = 0, epoch = 0;
    uint32_t level = 0, file = 0;
    uint64_t offset = 0;
    std::vector<ob::SegmentInput> named;
    for (const auto& in : inputs) {
        EXPECT_TRUE(store.read_segment(in, [&](const ob::SnapshotRow& r) { writer.append(r); }));
        last_row = std::max(last_row, in.last_row_ts_ns);
        epoch = std::max(epoch, in.seal_epoch);
        level = std::max(level, in.merge_level);
        if (in.wal_file_index > file || (in.wal_file_index == file && in.wal_byte_offset > offset)) {
            file = in.wal_file_index;
            offset = in.wal_byte_offset;
        }
        named.push_back(ob::SegmentInput::of(in));
    }
    writer.set_wal_position(inputs.front().wal_identity, file, offset);
    writer.set_seal_epoch(epoch);
    writer.set_lineage(level + 1, std::move(named), last_row);
    const fs::path parent = fs::path(inputs.front().dir_path).parent_path();
    static int seq = 0;
    const std::string dir =
        (parent / ("1_1_" + std::to_string(++seq) + std::string(ob::ColumnarStore::kCompactingSuffix)))
            .string();
    auto meta = writer.flush_segment_into(dir);
    EXPECT_TRUE(meta.has_value());
    return meta.value_or(ob::SegmentMeta{});
}

/// Publish a working directory by renaming it to a segment's name, as the engine does.
bool rename_to_segment(ob::SegmentMeta& meta) {
    const fs::path from(meta.dir_path);
    const fs::path to = from.parent_path() / (std::to_string(meta.start_ts_ns) + "_" +
                                              std::to_string(meta.end_ts_ns) + "_merged");
    std::error_code ec;
    fs::rename(from, to, ec);
    if (ec) return false;
    meta.dir_path = to.string();
    return true;
}

std::vector<ob::SnapshotRow> delivered(const ob::ColumnarStore& store, const std::string& symbol) {
    std::vector<ob::SnapshotRow> out;
    store.scan(0, UINT64_MAX, symbol, "EX", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { out.push_back(r); });
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

}  // namespace

TEST(CompactionStore, AMergeReadsASegmentWholeInTheOrderItHoldsItsRows) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    // Out of time order on purpose: a merge keeps the order rows were written in, not their times.
    const auto seg = written(dir.str(), "A", {{kBase + 3 * kSec, 30}, {kBase + 1 * kSec, 10},
                                              {kBase + 2 * kSec, 20}});
    store.merge_segments({seg});
    std::vector<ob::SnapshotRow> rows;
    ASSERT_TRUE(store.read_segment(seg, [&](const ob::SnapshotRow& r) { rows.push_back(r); }));
    EXPECT_TRUE(same_rows(rows, {row_at(kBase + 3 * kSec, 30), row_at(kBase + 1 * kSec, 10),
                                 row_at(kBase + 2 * kSec, 20)}));
}

TEST(CompactionStore, AMergeDoesNotTakeASegmentWithAShortColumn) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto seg = written(dir.str(), "A", {{kBase + 1 * kSec, 10}, {kBase + 2 * kSec, 20}});
    store.merge_segments({seg});
    // One timestamp of two: a query pads the second row's with zero, and a merge must not write it.
    fs::resize_file(seg.dir_path + "/ts.col", sizeof(uint64_t));
    size_t rows = 0;
    EXPECT_FALSE(store.read_segment(seg, [&](const ob::SnapshotRow&) { ++rows; }));
    EXPECT_EQ(rows, 0u) << "a segment that cannot be merged whole handed over rows";
}

TEST(CompactionStore, AMergedSegmentIsWrittenWhereItIsToldAndSaysWhatItReplaced) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 10}, {kBase + 5 * kSec, 50}},
                           {77, 1, 100, 4});
    const auto b = written(dir.str(), "A", {{kBase + 6 * kSec, 60}, {kBase + 2 * kSec, 20}},
                           {77, 1, 200, 5});
    store.merge_segments({a, b});
    const auto out = merged(store, dir.str(), {a, b});
    EXPECT_TRUE(ob::ColumnarStore::is_compacting_dir(fs::path(out.dir_path).filename().string()));
    EXPECT_EQ(out.row_count, 4u);
    EXPECT_EQ(out.start_ts_ns, kBase + 1 * kSec);
    EXPECT_EQ(out.end_ts_ns, kBase + 6 * kSec);
    EXPECT_EQ(out.merge_level, 1u);
    EXPECT_EQ(out.seal_epoch, 5u);
    EXPECT_EQ(out.wal_byte_offset, 200u);
    // The later of the inputs' last rows, not the last row appended, which is b's second.
    EXPECT_EQ(out.last_row_ts_ns, kBase + 5 * kSec);
    EXPECT_EQ(b.last_row_ts_ns, kBase + 2 * kSec);
    EXPECT_EQ(a.last_row_ts_ns, kBase + 5 * kSec);
    EXPECT_EQ(out.last_row_ts_ns, std::max(a.last_row_ts_ns, b.last_row_ts_ns));

    std::ifstream f(out.dir_path + "/meta.json");
    const std::string json((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    // The merge's keys come after every key an older build reads, which finds a key by its first
    // occurrence: an input's row count must not be what it reads as the segment's.
    const auto list = json.find("\"compacted_from\":[");
    ASSERT_NE(list, std::string::npos) << json;
    EXPECT_LT(json.find("\"row_count\":4"), list) << json;
    EXPECT_EQ(json.find("\"row_count\":", list), std::string::npos) << json;
    EXPECT_NE(json.find("\"dir\":\"" + fs::path(a.dir_path).filename().string() + "\"", list),
              std::string::npos) << json;

    // And a working directory is written once: a second merge into it is refused, and leaves it.
    ob::ColumnarStore again(dir.str(), UINT64_MAX, ob::ColumnarStore::OwnIndex::kNo);
    again.set_symbol_exchange("A", "EX");
    again.append(row_at(kBase + 9 * kSec, 90));
    EXPECT_THROW(again.flush_segment_into(out.dir_path), std::runtime_error);
    EXPECT_TRUE(fs::exists(out.dir_path + "/meta.json"));
    // Its rows are still active, and a store destroyed with active rows writes them as a segment
    // of its own: whoever merges has to abandon them.
    again.abandon_active();
}

TEST(CompactionStore, APartitionViewHoldsWhatLiesBetweenItsMembers) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    constexpr uint64_t kHour = 3600 * kSec;
    const uint64_t p = (kBase / kHour) * kHour;
    const auto a = written(dir.str(), "A", {{p + 10 * kSec, 1}, {p + 20 * kSec, 2}});
    // Starts between a and c and ends in the next hour: in the order a scan delivers, and not the
    // partition's.
    const auto wide = written(dir.str(), "A", {{p + kHour + 5 * kSec, 3}, {p + 30 * kSec, 4}});
    const auto c = written(dir.str(), "A", {{p + 40 * kSec, 5}});
    const auto other = written(dir.str(), "B", {{p + 25 * kSec, 6}});
    store.merge_segments({a, wide, c, other});
    const auto view = store.partition_view("A", "EX", p, p + kHour);
    ASSERT_EQ(view.segments.size(), 3u);
    EXPECT_EQ(view.segments[0].dir_path, a.dir_path);
    EXPECT_EQ(view.segments[1].dir_path, wide.dir_path);
    EXPECT_EQ(view.segments[2].dir_path, c.dir_path);
    EXPECT_EQ(view.member, (std::vector<bool>{true, false, true}));
    EXPECT_TRUE(store.partition_view("A", "EX", p + 2 * kHour, p + 3 * kHour).segments.empty());
}

TEST(CompactionStore, AReplacementSwapsInOneStepAndDeliversTheSameRowsInTheSameOrder) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto before_seg = written(dir.str(), "A", {{kBase + 1 * kSec, 1}});
    const auto a = written(dir.str(), "A", {{kBase + 2 * kSec, 2}, {kBase + 3 * kSec, 3}});
    // A tie with a's second row - same time, same level: which a SNAPSHOT keeps is which comes last.
    const auto b = written(dir.str(), "A", {{kBase + 3 * kSec, 33}, {kBase + 4 * kSec, 4}});
    const auto c = written(dir.str(), "A", {{kBase + 5 * kSec, 5}});
    const auto after_seg = written(dir.str(), "A", {{kBase + 9 * kSec, 9}});
    store.merge_segments({before_seg, a, b, c, after_seg});
    const auto rows_before = delivered(store, "A");

    auto out = merged(store, dir.str(), {a, b, c});
    int published = 0;
    const auto result = store.replace_segments({a, b, c}, out, [&](ob::SegmentMeta& m) {
        ++published;
        return rename_to_segment(m);
    });
    ASSERT_EQ(result.outcome, ob::ColumnarStore::Replaced::kYes);
    EXPECT_EQ(published, 1);
    EXPECT_EQ(store.segment_count(), 3u);
    EXPECT_TRUE(same_rows(delivered(store, "A"), rows_before))
        << "the merge changed what a scan delivers, or the order it delivers it in";
    // Nothing reads the inputs any more, and nothing held the generation they were copied under.
    EXPECT_TRUE(result.readers_before.expired());
}

TEST(CompactionStore, AReplacementRefusesAnInputThatIsGone) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}});
    const auto b = written(dir.str(), "A", {{kBase + 2 * kSec, 2}});
    store.merge_segments({a, b});
    const auto out = merged(store, dir.str(), {a, b});
    ASSERT_EQ(store.remove_segments({b.dir_path}), 1u);   // what retention's sweep would do
    bool called = false;
    const auto result = store.replace_segments({a, b}, out, [&](ob::SegmentMeta&) {
        called = true;
        return true;
    });
    EXPECT_EQ(result.outcome, ob::ColumnarStore::Replaced::kInputGone);
    EXPECT_FALSE(called) << "a merge of a segment no longer indexed was published";
    EXPECT_EQ(store.segment_count(), 1u);
}

TEST(CompactionStore, AReplacementRefusesInputsAnotherSegmentLiesBetween) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}});
    const auto between = written(dir.str(), "A", {{kBase + 2 * kSec, 2}});
    const auto c = written(dir.str(), "A", {{kBase + 3 * kSec, 3}});
    store.merge_segments({a, between, c});
    const auto rows_before = delivered(store, "A");
    const auto out = merged(store, dir.str(), {a, c});
    bool called = false;
    const auto result = store.replace_segments({a, c}, out, [&](ob::SegmentMeta&) {
        called = true;
        return true;
    });
    EXPECT_EQ(result.outcome, ob::ColumnarStore::Replaced::kNotConsecutive);
    EXPECT_FALSE(called);
    EXPECT_TRUE(same_rows(delivered(store, "A"), rows_before));
}

TEST(CompactionStore, AReplacementRefusesAnOutputThatWouldSortElsewhere) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    // Three segments of one range: their order is their directories' names, which a merged
    // segment's is not chosen to keep.
    const std::vector<std::pair<uint64_t, int64_t>> rows = {{kBase + 1 * kSec, 1},
                                                            {kBase + 2 * kSec, 2}};
    const auto a = written(dir.str(), "A", rows);
    const auto b = written(dir.str(), "A", rows);
    const auto next = written(dir.str(), "A", rows);
    store.merge_segments({a, b, next});
    const auto view = store.partition_view("A", "EX", 0, UINT64_MAX);
    ASSERT_EQ(view.segments.size(), 3u);
    const auto out = merged(store, dir.str(), {view.segments[0], view.segments[1]});
    bool called = false;
    const auto result = store.replace_segments({view.segments[0], view.segments[1]}, out,
                                               [&](ob::SegmentMeta&) {
                                                   called = true;
                                                   return true;
                                               });
    EXPECT_EQ(result.outcome, ob::ColumnarStore::Replaced::kWouldMove);
    EXPECT_FALSE(called);
}

TEST(CompactionStore, APublicationThatFailsLeavesTheIndexAsItWas) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}});
    const auto b = written(dir.str(), "A", {{kBase + 2 * kSec, 2}});
    store.merge_segments({a, b});
    const auto rows_before = delivered(store, "A");
    const auto out = merged(store, dir.str(), {a, b});
    const auto result = store.replace_segments({a, b}, out, [](ob::SegmentMeta&) { return false; });
    EXPECT_EQ(result.outcome, ob::ColumnarStore::Replaced::kPublishFailed);
    EXPECT_EQ(store.segment_count(), 2u);
    EXPECT_TRUE(same_rows(delivered(store, "A"), rows_before));
}

TEST(CompactionStore, AScanThatCopiedAnInputKeepsItReadableUntilItEnds) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}, {kBase + 2 * kSec, 2}});
    const auto b = written(dir.str(), "A", {{kBase + 3 * kSec, 3}});
    store.merge_segments({a, b});
    const auto out = merged(store, dir.str(), {a, b});

    std::mutex m;
    std::condition_variable cv;
    bool in_scan = false, go_on = false;
    std::vector<int64_t> seen;
    std::thread reader([&] {
        store.scan(0, UINT64_MAX, "A", "EX", ob::ColumnSet::all(), [&](const ob::SnapshotRow& r) {
            std::unique_lock<std::mutex> lock(m);
            seen.push_back(r.price);
            if (seen.size() == 1) {
                in_scan = true;
                cv.notify_all();
                cv.wait(lock, [&] { return go_on; });
            }
        });
    });
    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock, [&] { return in_scan; });
    }
    const auto result = store.replace_segments({a, b}, out, rename_to_segment);
    ASSERT_EQ(result.outcome, ob::ColumnarStore::Replaced::kYes);
    // A scan that starts now reads the merged segment and holds a later generation, which is not
    // what keeps the inputs.
    EXPECT_EQ(delivered(store, "A").size(), 3u);
    EXPECT_FALSE(result.readers_before.expired())
        << "the inputs' files could have been removed under a scan that is reading them";
    {
        std::lock_guard<std::mutex> lock(m);
        go_on = true;
    }
    cv.notify_all();
    reader.join();
    EXPECT_EQ(seen, (std::vector<int64_t>{1, 2, 3})) << "the scan that copied the inputs lost rows";
    EXPECT_TRUE(result.readers_before.expired());
}

TEST(CompactionStore, AGenerationKeepsEveryYoungerOneAlive) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}});
    const auto b = written(dir.str(), "A", {{kBase + 2 * kSec, 2}});
    const auto c = written(dir.str(), "A", {{kBase + 5 * kSec, 5}});
    const auto d = written(dir.str(), "A", {{kBase + 6 * kSec, 6}});
    store.merge_segments({a, b, c, d});

    std::mutex m;
    std::condition_variable cv;
    bool in_scan = false, go_on = false;
    std::thread reader([&] {
        size_t n = 0;
        store.scan(0, UINT64_MAX, "A", "EX", ob::ColumnSet::all(), [&](const ob::SnapshotRow&) {
            if (++n > 1) return;
            std::unique_lock<std::mutex> lock(m);
            in_scan = true;
            cv.notify_all();
            cv.wait(lock, [&] { return go_on; });
        });
    });
    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock, [&] { return in_scan; });
    }
    // The scan copied all four; the second replacement's inputs are among them, though its
    // generation began after the scan did.
    const auto first = store.replace_segments({a, b}, merged(store, dir.str(), {a, b}),
                                              rename_to_segment);
    ASSERT_EQ(first.outcome, ob::ColumnarStore::Replaced::kYes);
    const auto second = store.replace_segments({c, d}, merged(store, dir.str(), {c, d}),
                                               rename_to_segment);
    ASSERT_EQ(second.outcome, ob::ColumnarStore::Replaced::kYes);
    EXPECT_FALSE(first.readers_before.expired());
    EXPECT_FALSE(second.readers_before.expired())
        << "a younger generation expired while a scan from an older one still read its inputs";
    {
        std::lock_guard<std::mutex> lock(m);
        go_on = true;
    }
    cv.notify_all();
    reader.join();
    EXPECT_TRUE(first.readers_before.expired());
    EXPECT_TRUE(second.readers_before.expired());
}

TEST(CompactionStore, ARebuildRemovesAMergeNothingPublished) {
    TempDir dir;
    {
        ob::ColumnarStore store(dir.str());
        const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}});
        const auto b = written(dir.str(), "A", {{kBase + 2 * kSec, 2}});
        store.merge_segments({a, b});
        const auto out = merged(store, dir.str(), {a, b});
        ASSERT_TRUE(fs::exists(out.dir_path + "/meta.json"));
    }
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.segment_count(), 2u) << "a merge nothing published was indexed";
    EXPECT_EQ(reopened.last_rebuild_removed().staging, 1u);
    EXPECT_EQ(reopened.last_rebuild_removed().superseded, 0u);
    for (const auto& e : fs::recursive_directory_iterator(dir.path)) {
        EXPECT_FALSE(ob::ColumnarStore::is_compacting_dir(e.path().filename().string()))
            << e.path();
    }
    EXPECT_EQ(delivered(reopened, "A").size(), 2u);
}

TEST(CompactionStore, ARebuildRemovesTheInputsItFindsBesideTheirMergedSegment) {
    TempDir dir;
    std::vector<ob::SnapshotRow> rows_before;
    {
        ob::ColumnarStore store(dir.str());
        const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}}, {77, 1, 10, 1});
        const auto b = written(dir.str(), "A", {{kBase + 2 * kSec, 2}}, {77, 1, 20, 2});
        const auto keep = written(dir.str(), "A", {{kBase + 7 * kSec, 7}}, {77, 1, 70, 3});
        store.merge_segments({a, b, keep});
        rows_before = delivered(store, "A");
        auto out = merged(store, dir.str(), {a, b});
        // Published, and the process gone before it removed the inputs.
        ASSERT_TRUE(rename_to_segment(out));
    }
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.segment_count(), 2u);
    EXPECT_EQ(reopened.last_rebuild_removed().superseded, 2u);
    EXPECT_TRUE(same_rows(delivered(reopened, "A"), rows_before))
        << "a row the merge and its inputs both held was delivered twice, or not at all";
    // And the merged segment is read back as written: its own counts, not an input's.
    const auto index = reopened.index();
    ASSERT_EQ(index.size(), 2u);
    EXPECT_EQ(index[0].row_count, 2u);
    EXPECT_EQ(index[0].merge_level, 1u);
    EXPECT_EQ(index[0].seal_epoch, 2u);
    EXPECT_EQ(index[0].wal_byte_offset, 20u);
}

TEST(CompactionStore, ARebuildKeepsASegmentThatTookAnInputsName) {
    TempDir dir;
    std::string name;
    {
        ob::ColumnarStore store(dir.str());
        const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}}, {77, 1, 10, 1});
        const auto b = written(dir.str(), "A", {{kBase + 2 * kSec, 2}}, {77, 1, 20, 2});
        store.merge_segments({a, b});
        auto out = merged(store, dir.str(), {a, b});
        ASSERT_TRUE(rename_to_segment(out));
        // The inputs removed, as the merge does - and then a later seal whose rows span what a's
        // did, so its directory has a's name and its own epoch.
        fs::remove_all(a.dir_path);
        fs::remove_all(b.dir_path);
        const auto later = written(dir.str(), "A", {{kBase + 1 * kSec, 100}}, {77, 2, 5, 9});
        ASSERT_EQ(later.dir_path, a.dir_path);
        name = later.dir_path;
    }
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.last_rebuild_removed().superseded, 0u)
        << "a segment written after the merge was removed for the name it shares with an input";
    EXPECT_TRUE(fs::exists(name + "/meta.json"));
    EXPECT_EQ(reopened.segment_count(), 2u);
    EXPECT_EQ(delivered(reopened, "A").size(), 3u);
}

TEST(CompactionStore, OnlyAMergesOwnNameAtASegmentsDepthIsAWorkingDirectory) {
    using ob::ColumnarStore;
    EXPECT_TRUE(ColumnarStore::is_compacting_dir("1790000000_1790000001_7.compacting"));
    EXPECT_FALSE(ColumnarStore::is_compacting_dir("X.compacting"));
    EXPECT_FALSE(ColumnarStore::is_compacting_dir("1_2.compacting"));
    EXPECT_FALSE(ColumnarStore::is_compacting_dir("1_2_x.compacting"));
    EXPECT_FALSE(ColumnarStore::is_compacting_dir("1__2.compacting"));
    EXPECT_FALSE(ColumnarStore::is_compacting_dir(".compacting"));

    // A symbol and an exchange a client named like one: their rows stay through a rebuild.
    TempDir dir;
    {
        ob::ColumnarStore store(dir.str());
        ob::ColumnarStore writer(dir.str(), ob::ColumnarStore::kDefaultSegmentDurationNs,
                                 ob::ColumnarStore::OwnIndex::kNo);
        writer.set_symbol_exchange("X.compacting", "1_2_3.compacting");
        writer.append(row_at(kBase + 1 * kSec, 1));
        ASSERT_TRUE(writer.flush_segment().has_value());
    }
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.segment_count(), 1u)
        << "a symbol whose name ends like a working directory was removed with its rows";
    EXPECT_EQ(reopened.last_rebuild_removed().staging, 0u);
}

TEST(CompactionStore, ARebuildKeepsTheInputsOfAMergedSegmentWhoseColumnsAreShort) {
    TempDir dir;
    std::vector<ob::SnapshotRow> rows_before;
    std::string merged_dir;
    {
        ob::ColumnarStore store(dir.str());
        const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}}, {77, 1, 10, 1});
        const auto b = written(dir.str(), "A", {{kBase + 2 * kSec, 2}}, {77, 1, 20, 2});
        store.merge_segments({a, b});
        rows_before = delivered(store, "A");
        auto out = merged(store, dir.str(), {a, b});
        ASSERT_TRUE(rename_to_segment(out));
        merged_dir = out.dir_path;
    }
    // What a storage that did not keep what it acknowledged leaves: the merged segment's timestamps
    // short of its rows.
    fs::resize_file(merged_dir + "/ts.col", sizeof(uint64_t));
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.last_rebuild_removed().superseded, 0u)
        << "the inputs of a merged segment short of its rows were removed";
    EXPECT_EQ(reopened.last_rebuild_removed().short_merges, 1u);
    EXPECT_FALSE(fs::exists(merged_dir)) << "the short merged segment was kept";
    EXPECT_TRUE(same_rows(delivered(reopened, "A"), rows_before));
}

TEST(CompactionStore, AShortMergedSegmentWhoseInputsAreGoneIsKept) {
    TempDir dir;
    std::string merged_dir;
    {
        ob::ColumnarStore store(dir.str());
        const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}}, {77, 1, 10, 1});
        const auto b = written(dir.str(), "A", {{kBase + 2 * kSec, 2}}, {77, 1, 20, 2});
        store.merge_segments({a, b});
        auto out = merged(store, dir.str(), {a, b});
        ASSERT_TRUE(rename_to_segment(out));
        merged_dir = out.dir_path;
        // A merge that finished: its inputs removed.
        fs::remove_all(a.dir_path);
        fs::remove_all(b.dir_path);
    }
    fs::resize_file(merged_dir + "/ts.col", sizeof(uint64_t));
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_TRUE(fs::exists(merged_dir + "/price.col"))
        << "the only copy of its rows was removed for one short column";
    EXPECT_EQ(reopened.last_rebuild_removed().short_merges, 0u);
    EXPECT_EQ(reopened.segment_count(), 1u);
}

TEST(CompactionStore, ARebuildTellsAnInputFromEveryMergedSegmentThatNamesItsPath) {
    TempDir dir;
    std::vector<ob::SnapshotRow> rows_before;
    {
        ob::ColumnarStore store(dir.str());
        // A first merge, its inputs gone as a merge leaves them...
        const auto a = written(dir.str(), "A", {{kBase + 1 * kSec, 1}}, {77, 1, 10, 1});
        const auto b = written(dir.str(), "A", {{kBase + 2 * kSec, 2}}, {77, 1, 20, 2});
        store.merge_segments({a, b});
        auto first = merged(store, dir.str(), {a, b});
        ASSERT_TRUE(rename_to_segment(first));
        store.replace_segments({a, b}, first, [](ob::SegmentMeta&) { return true; });
        fs::remove_all(a.dir_path);
        fs::remove_all(b.dir_path);
        // ...then later segments that took the same names, merged in turn, and the process gone
        // before their removal.
        const auto a2 = written(dir.str(), "A", {{kBase + 1 * kSec, 100}}, {77, 2, 30, 3});
        const auto b2 = written(dir.str(), "A", {{kBase + 2 * kSec, 200}}, {77, 2, 40, 4});
        ASSERT_EQ(a2.dir_path, a.dir_path);
        store.merge_segments({a2, b2});
        auto second = merged(store, dir.str(), {a2, b2});
        const fs::path from(second.dir_path);
        const fs::path to = from.parent_path() / (std::to_string(second.start_ts_ns) + "_" +
                                                  std::to_string(second.end_ts_ns) + "_merged2");
        fs::rename(from, to);
        rows_before = delivered(store, "A");   // a2 and b2, indexed; the first merge
    }
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.last_rebuild_removed().superseded, 2u)
        << "an input one merged segment names was kept because another names its path too";
    EXPECT_EQ(delivered(reopened, "A").size(), rows_before.size());
}

TEST(CompactionStore, AnInputIsComparedWithTheRangeItWasMergedWith) {
    TempDir dir;
    std::string input_dir;
    {
        ob::ColumnarStore store(dir.str());
        const auto a = written(dir.str(), "A", {{kBase + 5 * kSec, 1}}, {77, 1, 10, 1});
        const auto b = written(dir.str(), "A", {{kBase + 6 * kSec, 2}}, {77, 1, 20, 2});
        store.merge_segments({a, b});
        auto out = merged(store, dir.str(), {a, b});
        ASSERT_TRUE(rename_to_segment(out));
        input_dir = a.dir_path;
    }
    // The input's meta.json as a build before #166 left it, whose repair the merge's process made
    // in memory only: a range from the start of its hour, and no word that it is the rows'.
    {
        std::ifstream f(input_dir + "/meta.json");
        std::string json((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        const std::string rows = ",\"time_range\":\"rows\"";
        ASSERT_NE(json.find(rows), std::string::npos) << json;
        json.erase(json.find(rows), rows.size());
        const std::string start = "\"start_ts_ns\":" + std::to_string(kBase + 5 * kSec);
        const uint64_t hour = (kBase / (3600 * kSec)) * (3600 * kSec);
        ASSERT_NE(json.find(start), std::string::npos) << json;
        json.replace(json.find(start), start.size(), "\"start_ts_ns\":" + std::to_string(hour));
        std::ofstream(input_dir + "/meta.json", std::ios::trunc) << json;
    }
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.last_rebuild_removed().superseded, 2u)
        << "an input compared with the range its old meta.json says, rather than its rows', stayed";
    EXPECT_FALSE(fs::exists(input_dir));
    EXPECT_EQ(delivered(reopened, "A").size(), 2u);
}

TEST(CompactionStore, AChainOfManyGenerationsIsReleasedWithoutRecursion) {
    // What a scan that outlived a million publications holds: the start of a chain of every
    // generation since. Released by recursion, this is a million nested destructors on that scan's
    // thread; released a link at a time, it is one.
    auto head = std::make_shared<ob::detail::ReaderGeneration>();
    auto tail = head;
    for (int i = 0; i < 1'000'000; ++i) {
        auto link = std::make_shared<ob::detail::ReaderGeneration>();
        tail->next = link;
        tail = std::move(link);
    }
    std::weak_ptr<ob::detail::ReaderGeneration> last = tail;
    tail.reset();
    head.reset();
    EXPECT_TRUE(last.expired()) << "a generation at the end of the chain outlived its release";
}
