// #167 and #168: what a SNAPSHOT answers.
//
// A SNAPSHOT reconstructs the book at a time: for each (side, level), the row at or before it. It
// kept the last row a scan delivered, which is the latest only while rows arrive in time order -
// and they do not when a client gives its own event times (#105) or a mesh peer's backlog is applied
// after a partition, so a correction for an earlier instant replaced the book it came after (#168).
// It answered in the order of a hash map. And over the wire it answered no columns at all: the
// shape the formatter prints was assigned below its return (#167), which these check through the
// shape the engine hands back, since that is all the formatter reads.

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/query_columns.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 1'790'000'000ULL * kSec;
constexpr uint8_t  kBid  = 0;
constexpr uint8_t  kAsk  = 1;

std::atomic<uint64_t> g_counter{0};

std::string made(const fs::path& dir) {
    fs::create_directories(dir);
    return dir.string();
}

struct Fixture {
    fs::path dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    std::unordered_map<std::string, std::shared_ptr<ob::SoABuffer>> live;
    ob::QueryEngine engine;

    Fixture()
        : dir(fs::temp_directory_path() / ("ob_snapshot_query_" + std::to_string(::getpid()) +
                                           "_" + std::to_string(g_counter.fetch_add(1))))
        , store(made(dir))
        , engine(store,
                 [this](const std::string& key) -> std::shared_ptr<ob::SoABuffer> {
                     auto it = live.find(key);
                     return it == live.end() ? nullptr : it->second;
                 },
                 agg) {}
    ~Fixture() {
        store.close();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    /// One segment of SNP.EX holding these rows, in this order.
    void segment(const std::vector<ob::SnapshotRow>& rows) {
        store.set_symbol_exchange("SNP", "EX");
        for (const auto& r : rows) store.append(r);
        ASSERT_TRUE(store.flush_segment().has_value());
    }

    std::vector<ob::QueryResult> snapshot(const std::string& sql,
                                          ob::QueryShape* shape_out = nullptr) {
        std::vector<ob::QueryResult> out;
        ob::QueryShape shape;
        const std::string err =
            engine.execute(sql, [&](const ob::QueryResult& r) { out.push_back(r); }, shape);
        EXPECT_TRUE(err.empty()) << err;
        if (shape_out != nullptr) *shape_out = shape;
        return out;
    }
};

ob::SnapshotRow level(uint64_t ts, uint8_t side, uint16_t level_index, int64_t price) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = 1;
    r.side            = side;
    r.level_index     = level_index;
    r.price           = price;
    r.quantity        = 5;
    r.order_count     = 1;
    return r;
}

std::string at(uint64_t ts) {
    return "SELECT * FROM 'SNP'.'EX' WHERE AT " + std::to_string(ts);
}

std::vector<int64_t> prices(const std::vector<ob::QueryResult>& rows) {
    std::vector<int64_t> out;
    for (const auto& r : rows) out.push_back(r.price);
    return out;
}

}  // namespace

TEST(SnapshotQuery, ALevelsLatestRowAtOrBeforeTheTimeIsTheAnswer) {
    // The shape #168 was measured with: one level written twice in one flush, the later event
    // time first. The book at 10 s is the one written at 5 s; the one at 4 s is the correction.
    Fixture f;
    f.segment({level(kBase + 5 * kSec, kBid, 0, 100), level(kBase + 3 * kSec, kBid, 0, 101)});
    EXPECT_EQ(prices(f.snapshot(at(kBase + 10 * kSec))), (std::vector<int64_t>{100}))
        << "the correction for 3 s replaced the book written at 5 s";
    EXPECT_EQ(prices(f.snapshot(at(kBase + 4 * kSec))), (std::vector<int64_t>{101}));
    EXPECT_TRUE(f.snapshot(at(kBase + 2 * kSec)).empty());
}

TEST(SnapshotQuery, ACorrectionInALaterSegmentIsTheAnswerOnlyAtItsOwnTime) {
    // The same two rows a flush apart. Whether this was right before the fix depended on the order
    // the store handed its segments out in: the engine's combined store sorted them by start, which
    // put the correction first and made the answer right by luck, and a store on its own - this
    // fixture - handed them out in the order it wrote them, which made it wrong. The comment above
    // this test first said it passed before the fix; run against that code, it failed with 101.
    Fixture f;
    f.segment({level(kBase + 5 * kSec, kBid, 0, 100)});
    f.segment({level(kBase + 3 * kSec, kBid, 0, 101)});
    EXPECT_EQ(prices(f.snapshot(at(kBase + 10 * kSec))), (std::vector<int64_t>{100}));
    EXPECT_EQ(prices(f.snapshot(at(kBase + 4 * kSec))), (std::vector<int64_t>{101}));
}

TEST(SnapshotQuery, ATieOnTheTimestampKeepsTheLaterWritten) {
    // What every row got before: the last one delivered. Two writes of one level at one instant
    // arrive in the order they were written.
    Fixture f;
    f.segment({level(kBase + 5 * kSec, kBid, 0, 100), level(kBase + 5 * kSec, kBid, 0, 102)});
    EXPECT_EQ(prices(f.snapshot(at(kBase + 10 * kSec))), (std::vector<int64_t>{102}));
}

TEST(SnapshotQuery, TheBookComesBidsFirstThenAsksEachByLevel) {
    // The order BOOK answers in. A hash map made it a property of the hash, and a LIMIT kept
    // whichever levels the hash put first.
    Fixture f;
    f.segment({level(kBase + 1 * kSec, kAsk, 1, 211), level(kBase + 1 * kSec, kBid, 2, 102),
               level(kBase + 1 * kSec, kAsk, 0, 210), level(kBase + 1 * kSec, kBid, 0, 100),
               level(kBase + 1 * kSec, kBid, 1, 101)});
    const auto rows = f.snapshot(at(kBase + 10 * kSec));
    EXPECT_EQ(prices(rows), (std::vector<int64_t>{100, 101, 102, 210, 211}));
    ASSERT_EQ(rows.size(), 5u);
    EXPECT_EQ(rows[3].side, kAsk);
    EXPECT_EQ(rows[3].level, 0u);
    EXPECT_EQ(prices(f.snapshot(at(kBase + 10 * kSec) + " LIMIT 2")),
              (std::vector<int64_t>{100, 101}));
}

TEST(SnapshotQuery, ASnapshotSaysWhichColumnsItAnswers) {
    // The shape is what the wire formatter prints. Empty, it printed an empty header and an empty
    // line per row (#167) - the rows were there and every column was gone.
    Fixture f;
    f.segment({level(kBase + 1 * kSec, kBid, 0, 100)});
    ob::QueryShape shape;
    ASSERT_EQ(f.snapshot(at(kBase + 10 * kSec), &shape).size(), 1u);
    EXPECT_EQ(shape.columns, ob::all_query_columns());
    EXPECT_FALSE(shape.is_aggregate);
    ASSERT_EQ(f.snapshot("SELECT price, side FROM 'SNP'.'EX' WHERE AT " +
                             std::to_string(kBase + 10 * kSec),
                         &shape)
                  .size(),
              1u);
    EXPECT_EQ(shape.columns,
              (std::vector<ob::QueryColumn>{ob::QueryColumn::Price, ob::QueryColumn::Side}));
}
