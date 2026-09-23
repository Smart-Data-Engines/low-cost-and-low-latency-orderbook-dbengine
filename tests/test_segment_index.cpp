// #165 part 1: the segment index is per symbol, and nothing walks it whole on a tick's or a query's
// path.
//
// The index was one vector of every segment of every symbol: each tick's merge compared every new
// segment with every indexed one and sorted all of them under the engine's lock, and each query
// copied all of them before filtering one symbol out. What these hold is that the per-symbol index
// answers what the flat one answered - the same rows for every query, the same order from index(),
// the same refusals - and the things the change added: a scan compares only what its width tiers'
// windows reach, so one wide segment does not make every query walk its symbol; retention deletes
// without the lock and a query that meets a segment going says so quietly; and a store whose
// segments go to a combined store keeps no index of its own.

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/query_columns.hpp"
#include "source_scan.hpp"

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 1'790'000'000ULL * kSec;

std::atomic<uint64_t> g_counter{0};

struct TempDir {
    fs::path path;
    TempDir()
        : path(fs::temp_directory_path() /
               ("ob_segment_index_" + std::to_string(::getpid()) + "_" +
                std::to_string(g_counter.fetch_add(1)))) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
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

/// One segment of `symbol` holding a row at each of `times`, written by `store`, which indexes it.
ob::SegmentMeta segment(ob::ColumnarStore& store, const std::string& symbol,
                        const std::vector<uint64_t>& times, int64_t first_price) {
    store.set_symbol_exchange(symbol, "EX");
    int64_t price = first_price;
    for (uint64_t ts : times) store.append(row_at(ts, price++));
    auto meta = store.flush_segment();
    EXPECT_TRUE(meta.has_value());
    return meta.value_or(ob::SegmentMeta{});
}

std::vector<int64_t> prices(const ob::ColumnarStore& store, const std::string& symbol,
                            uint64_t lo, uint64_t hi, ob::ColumnarStore::ScanCost* cost = nullptr) {
    std::vector<int64_t> out;
    const auto c = store.scan(lo, hi, symbol, "EX", ob::ColumnSet::all(),
                              [&](const ob::SnapshotRow& r) { out.push_back(r.price); });
    if (cost != nullptr) *cost = c;
    std::sort(out.begin(), out.end());
    return out;
}

class StderrCapture {
public:
    StderrCapture() {
        std::fflush(stderr);
        saved_ = ::dup(STDERR_FILENO);
        std::strncpy(name_, "/tmp/ob_segment_index_log_XXXXXX", sizeof(name_) - 1);
        fd_ = ::mkstemp(name_);
        ::dup2(fd_, STDERR_FILENO);
    }
    std::string text() {
        std::fflush(stderr);
        ::dup2(saved_, STDERR_FILENO);
        ::close(saved_);
        saved_ = -1;
        std::ifstream f(name_);
        std::ostringstream s;
        s << f.rdbuf();
        return s.str();
    }
    ~StderrCapture() {
        if (saved_ >= 0) {
            ::dup2(saved_, STDERR_FILENO);
            ::close(saved_);
        }
        ::close(fd_);
        ::unlink(name_);
    }

private:
    char name_[64]{};
    int saved_{-1};
    int fd_{-1};
};

}  // namespace

// A scan answers with the rows of the segments of its symbol that its range can reach, and the
// candidate windows are searches on sorted starts, one per width tier, each widened by its tier's
// widest segment. Drawn here: symbols sharing one directory tree, segments in any order and of any
// width, including one row far from the rest (the shape #166 made representable), and queries of
// any range. The expected answer is a walk over every row written, which is what the flat index
// amounted to.
//
// And what the scan says it looked at: its candidates are exactly the segments whose range meets
// the query, and what it compared and did not need is bounded - per tier, the segments holding one
// instant, so here at most the tiers there are times the most segments any instant is in.
RC_GTEST_PROP(SegmentIndexProperty, AScanReturnsWhatAWalkOfEveryRowReturns, ()) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const std::vector<std::string> symbols = {"A", "B", "C.D"};
    struct Written { std::string symbol; uint64_t ts; int64_t price; };
    struct Range { std::string symbol; uint64_t lo; uint64_t hi; };
    std::vector<Written> written;
    std::vector<Range> ranges;
    const int segments = *rc::gen::inRange(1, 12);
    int64_t price = 1;
    for (int s = 0; s < segments; ++s) {
        const std::string sym = symbols[static_cast<size_t>(*rc::gen::inRange(0, 3))];
        const int rows = *rc::gen::inRange(1, 5);
        std::vector<uint64_t> times;
        for (int r = 0; r < rows; ++r) {
            times.push_back(kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 1000)) * kSec);
        }
        segment(store, sym, times, price);
        for (uint64_t ts : times) written.push_back({sym, ts, price++});
        ranges.push_back({sym, *std::min_element(times.begin(), times.end()),
                          *std::max_element(times.begin(), times.end())});
    }
    for (int q = 0; q < 8; ++q) {
        const std::string sym = symbols[static_cast<size_t>(*rc::gen::inRange(0, 3))];
        uint64_t lo = kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 1001)) * kSec;
        uint64_t hi = kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 1001)) * kSec;
        if (lo > hi) std::swap(lo, hi);
        std::vector<int64_t> expected;
        for (const auto& w : written) {
            if (w.symbol == sym && w.ts >= lo && w.ts <= hi) expected.push_back(w.price);
        }
        std::sort(expected.begin(), expected.end());
        ob::ColumnarStore::ScanCost cost;
        RC_ASSERT(prices(store, sym, lo, hi, &cost) == expected);

        size_t meeting = 0;
        size_t deepest = 0;
        std::set<unsigned> widths;
        for (const auto& r : ranges) {
            if (r.symbol != sym) continue;
            if (r.lo <= hi && r.hi >= lo) ++meeting;
            widths.insert(static_cast<unsigned>(std::bit_width(r.hi - r.lo)));
            size_t depth = 0;   // the deepest instant is some segment's start
            for (const auto& o : ranges) {
                if (o.symbol == sym && o.lo <= r.lo && r.lo <= o.hi) ++depth;
            }
            deepest = std::max(deepest, depth);
        }
        RC_ASSERT(cost.candidates == meeting);
        RC_ASSERT(cost.compared >= cost.candidates);
        RC_ASSERT(cost.compared - cost.candidates <= widths.size() * deepest);
    }
}

TEST(SegmentIndex, AWideSegmentDoesNotWidenTheWindowOfTheNarrowOnes) {
    // One wide segment among a symbol's narrow ones - what a flush writes when a batch mixes
    // current rows with old ones: a peer's backlog after a partition, a late correction (#105).
    // With one window per symbol, every scan of it walked back over every segment that starts
    // within that width of its range.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    constexpr uint64_t kStep = 10 * kSec;
    constexpr size_t kNarrow = 200;
    for (size_t i = 0; i < kNarrow; ++i) {
        segment(store, "A", {kBase + i * kStep}, static_cast<int64_t>(i) + 1);
    }
    segment(store, "A", {kBase, kBase + 100 * kStep}, 1000);          // wide: [0, 100) steps

    // A gap near the end, which no segment holds.
    ob::ColumnarStore::ScanCost cost;
    const uint64_t gap = kBase + 198 * kStep + kSec;
    EXPECT_TRUE(prices(store, "A", gap, gap + kSec, &cost).empty());
    EXPECT_EQ(cost.candidates, 0u);
    EXPECT_LE(cost.compared, 2u) << "the wide segment's width is the narrow ones' window";

    // The wide segment is still found where it is: the middle of its range, which holds none of
    // its two rows but meets it.
    const uint64_t middle = kBase + 50 * kStep + kSec;
    EXPECT_TRUE(prices(store, "A", middle, middle + kSec, &cost).empty());
    EXPECT_EQ(cost.candidates, 1u) << "the wide segment was not a candidate for its own range";
    EXPECT_LE(cost.compared, 2u);
    EXPECT_EQ(prices(store, "A", kBase + 100 * kStep, kBase + 100 * kStep),
              (std::vector<int64_t>{101, 1001}));
}

TEST(SegmentIndex, IndexIsOneOrderAcrossSymbolsAsItAlwaysWas) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    segment(store, "B", {kBase + 5 * kSec}, 1);
    segment(store, "A", {kBase + 3 * kSec, kBase + 9 * kSec}, 2);
    segment(store, "B", {kBase + 1 * kSec}, 4);
    segment(store, "A", {kBase + 1 * kSec}, 5);
    const auto index = store.index();
    ASSERT_EQ(index.size(), 4u);
    EXPECT_EQ(store.segment_count(), 4u);
    for (size_t i = 1; i < index.size(); ++i) {
        const auto& a = index[i - 1];
        const auto& b = index[i];
        EXPECT_TRUE(std::tie(a.start_ts_ns, a.end_ts_ns, a.dir_path) <
                    std::tie(b.start_ts_ns, b.end_ts_ns, b.dir_path))
            << "not in segment_order_less order at " << i;
    }
}

TEST(SegmentIndex, RowsReachTheCallbackInTheOrderTheFlatIndexGaveThem) {
    // Segments of four widths, so four tiers, interleaved by start. A scan reads its candidates in
    // segment_order_less order, the order the flat index gave them, and rows reach the callback in
    // it - which a LIMIT and a SNAPSHOT read, since they take the first rows or keep the last.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    segment(store, "A", {kBase + 1 * kSec}, 1);                      // width 0
    segment(store, "A", {kBase + 2 * kSec, kBase + 500 * kSec}, 2);  // 498 s
    segment(store, "A", {kBase + 3 * kSec}, 4);                      // width 0
    segment(store, "A", {kBase + 4 * kSec, kBase + 6 * kSec}, 5);    // 2 s
    std::vector<int64_t> order;
    const auto cost = store.scan(0, UINT64_MAX, "A", "EX", ob::ColumnSet::all(),
                                 [&](const ob::SnapshotRow& r) { order.push_back(r.price); });
    EXPECT_EQ(cost.candidates, 4u);
    EXPECT_EQ(order, (std::vector<int64_t>{1, 2, 3, 4, 5, 6}));
}

TEST(SegmentIndex, ADuplicateDirectoryIsRefusedAndCountedOnce) {
    TempDir dir;
    ob::ColumnarStore writer(dir.str(), ob::ColumnarStore::kDefaultSegmentDurationNs,
                             ob::ColumnarStore::OwnIndex::kNo);
    const auto meta = segment(writer, "A", {kBase + 1}, 1);
    ob::ColumnarStore combined(dir.str());
    EXPECT_EQ(combined.merge_segments({meta}), 0u);
    EXPECT_EQ(combined.merge_segments({meta}), 1u) << "the same directory was indexed twice";
    EXPECT_EQ(combined.segment_count(), 1u);
    EXPECT_EQ(prices(combined, "A", 0, UINT64_MAX), (std::vector<int64_t>{1}));
}

TEST(SegmentIndex, HoldsAnswersForItsOwnSymbolAndExchangeOnly) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    store.set_symbol_exchange("A.B", "C");
    store.append(row_at(kBase, 1));
    ASSERT_TRUE(store.flush_segment().has_value());
    EXPECT_TRUE(store.holds("A.B", "C"));
    // One key with a dot between symbol and exchange; two with the separator the index uses.
    EXPECT_FALSE(store.holds("A", "B.C"));
    EXPECT_FALSE(store.holds("A.B", "D"));
    EXPECT_TRUE(prices(store, "A", 0, UINT64_MAX).empty());
}

TEST(SegmentIndex, RetentionTakesWhatIsPastTheCutoffAndLeavesTheRestInOrder) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    segment(store, "A", {kBase + 1 * kSec}, 1);                      // expires
    segment(store, "A", {kBase + 2 * kSec, kBase + 50 * kSec}, 2);  // wide: its newest row stays
    segment(store, "A", {kBase + 3 * kSec}, 4);                      // expires
    segment(store, "B", {kBase + 4 * kSec}, 5);                      // expires, and B with it
    segment(store, "A", {kBase + 20 * kSec}, 6);                     // stays
    const auto [deleted, bytes] = store.delete_expired_segments(kBase + 10 * kSec);
    EXPECT_EQ(deleted, 3u);
    EXPECT_GT(bytes, 0u);
    EXPECT_EQ(store.segment_count(), 2u);
    EXPECT_EQ(store.symbols_indexed(), 1u) << "a symbol left without segments is still indexed";
    EXPECT_FALSE(store.holds("B", "EX"));
    EXPECT_EQ(prices(store, "A", 0, UINT64_MAX), (std::vector<int64_t>{2, 3, 6}));
    const auto index = store.index();
    ASSERT_EQ(index.size(), 2u);
    EXPECT_LT(index[0].start_ts_ns, index[1].start_ts_ns);
    for (const auto& e : fs::recursive_directory_iterator(dir.path)) {
        if (e.path().filename() == "meta.json") {
            const auto d = e.path().parent_path().string();
            EXPECT_TRUE(d == index[0].dir_path || d == index[1].dir_path) << d << " is on disk";
        }
    }
}

TEST(SegmentIndex, ASegmentRetentionCannotDeleteStaysIndexedForTheNextSweep) {
    ASSERT_NE(::geteuid(), 0u) << "the premise is a directory this process cannot change";
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto meta = segment(store, "A", {kBase + 1 * kSec}, 1);
    // The segment's own directory, so remove_all() fails on its first file instead of deleting
    // them all and then failing on the directory.
    fs::permissions(meta.dir_path, fs::perms::owner_read | fs::perms::owner_exec);
    const auto [deleted, bytes] = store.delete_expired_segments(kBase + 10 * kSec);
    fs::permissions(meta.dir_path, fs::perms::owner_all);
    EXPECT_EQ(deleted, 0u);
    (void)bytes;
    EXPECT_EQ(store.segment_count(), 1u) << "a segment still on disk left the index";
    EXPECT_EQ(prices(store, "A", 0, UINT64_MAX), (std::vector<int64_t>{1}));
    EXPECT_EQ(store.delete_expired_segments(kBase + 10 * kSec).first, 1u);
}

TEST(SegmentIndex, AQueryThatMeetsASegmentRetentionIsDeletingLeavesItOutQuietly) {
    // Retention takes segments out of the index under the lock and deletes their directories
    // without it, so a scan that copied a segment before it was taken out can reach it after its
    // files are gone. Made deterministic here: the scan's own callback, which runs without the
    // index's lock, runs the sweep while the scan is between two segments it already copied.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    segment(store, "A", {kBase + 1 * kSec, kBase + 90 * kSec}, 1);   // read first, and stays
    segment(store, "A", {kBase + 2 * kSec}, 3);                      // read second, and expires
    std::vector<int64_t> seen;
    bool swept = false;
    StderrCapture log;
    store.scan(0, UINT64_MAX, "A", "EX", ob::ColumnSet::all(), [&](const ob::SnapshotRow& r) {
        seen.push_back(r.price);
        if (!swept) {
            swept = true;
            EXPECT_EQ(store.delete_expired_segments(kBase + 10 * kSec).first, 1u);
        }
    });
    const std::string text = log.text();
    std::sort(seen.begin(), seen.end());
    EXPECT_EQ(seen, (std::vector<int64_t>{1, 2})) << "the deleted segment's rows were returned";
    EXPECT_EQ(text.find("\"level\":\"ERROR\""), std::string::npos) << text;
}

TEST(SegmentIndex, AMissingFileOfASegmentStillIndexedIsStillAnError) {
    // The quiet path above is for a segment the index no longer has. One it still has and whose
    // file is gone is what it always was: a segment the query cannot read.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto meta = segment(store, "A", {kBase + 1 * kSec}, 1);
    fs::remove(fs::path(meta.dir_path) / "price.col");
    StderrCapture log;
    EXPECT_TRUE(prices(store, "A", 0, UINT64_MAX).empty());
    const std::string text = log.text();
    EXPECT_NE(text.find("missing column price.col"), std::string::npos) << text;
}

TEST(SegmentIndex, AStoreWhoseSegmentsGoElsewhereKeepsNoIndex) {
    TempDir dir;
    ob::ColumnarStore store(dir.str(), ob::ColumnarStore::kDefaultSegmentDurationNs,
                            ob::ColumnarStore::OwnIndex::kNo);
    const auto meta = segment(store, "A", {kBase + 1 * kSec}, 1);
    EXPECT_FALSE(meta.dir_path.empty());
    EXPECT_EQ(store.segment_count(), 0u) << "the per-symbol store kept a copy nothing reads";
    EXPECT_FALSE(store.holds("A", "EX"));
    ob::ColumnarStore combined(dir.str());
    combined.merge_segments({meta});
    EXPECT_EQ(prices(combined, "A", 0, UINT64_MAX), (std::vector<int64_t>{1}));
}

TEST(SegmentIndex, RemovingNamedSegmentsLeavesTheOthers) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const auto a = segment(store, "A", {kBase + 1 * kSec}, 1);
    segment(store, "A", {kBase + 2 * kSec}, 2);
    const auto b = segment(store, "B", {kBase + 3 * kSec}, 3);
    EXPECT_EQ(store.remove_segments({a.dir_path, b.dir_path, "/no/such/segment"}), 2u);
    EXPECT_EQ(store.segment_count(), 1u);
    EXPECT_EQ(store.symbols_indexed(), 1u) << "a symbol left without segments is still indexed";
    EXPECT_FALSE(store.holds("B", "EX"));
    EXPECT_FALSE(fs::exists(a.dir_path));
    EXPECT_EQ(prices(store, "A", 0, UINT64_MAX), (std::vector<int64_t>{2}));
}

TEST(SegmentIndex, AReopenedStoreIndexesWhatIsOnDiskPerSymbol) {
    TempDir dir;
    {
        ob::ColumnarStore store(dir.str());
        segment(store, "A", {kBase + 2 * kSec}, 1);
        segment(store, "B", {kBase + 1 * kSec}, 2);
        segment(store, "A", {kBase + 1 * kSec}, 3);
    }
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    EXPECT_EQ(reopened.segment_count(), 3u);
    EXPECT_EQ(prices(reopened, "A", 0, UINT64_MAX), (std::vector<int64_t>{1, 3}));
    EXPECT_EQ(prices(reopened, "B", 0, UINT64_MAX), (std::vector<int64_t>{2}));
    EXPECT_TRUE(reopened.holds("A", "EX"));
}

TEST(SegmentIndexStatic, EveryPerSymbolStoreKeepsNoIndexOfItsOwn) {
    // A per-symbol store is made with make_unique, the combined store as a member: so every
    // make_unique of a ColumnarStore in the engine's sources is a store whose segments go to a
    // combined store, and has to say OwnIndex::kNo - the default is kYes, which is right for a
    // store on its own and is the copy nothing read (#165).
    using namespace ob::source_scan;
    size_t sites = 0;
    for (const auto& path : engine_sources()) {
        const Views v = blank(read_file(path));
        for (const char* name : {"make_unique<ColumnarStore>", "make_unique<ob::ColumnarStore>"}) {
            for (const size_t paren : calls_of(v.bare, name)) {
                ++sites;
                EXPECT_NE(call_args(v.code, paren).find("OwnIndex::kNo"), std::string::npos)
                    << rel(path) << ":" << line_of(v.bare, paren)
                    << " makes a per-symbol store that indexes its own segments";
            }
        }
    }
    // The engine's and the C API's; fewer means this stopped finding them, which would make every
    // check above pass by finding nothing.
    EXPECT_GE(sites, 2u);
}
