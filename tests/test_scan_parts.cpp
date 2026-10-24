// #246, stage 2: a query's candidates read on a pool, folded in their order.
//
// The promise is that the pool changes where the work is done and nothing else, so every test here
// holds an answer made with a pool - of one, two and four threads - to the answer made without one:
//
// - a row answer, byte for byte, for any columns, conditions and LIMIT, over segments and a block;
// - the buckets of GROUP BY TIME_BUCKET, every aggregate, with ties on a time across two segments -
//   FIRST and LAST break a tie by the order the scan delivers rows in, and the parts are merged in it;
// - a sum of price times quantity whose overflow a merge cannot tell, which is read again on one
//   thread and so answers - or refuses - as one thread would;
//
// and what the parts do around the answer: a LIMIT of one reads one candidate, a part that throws
// reaches the query with every other part settled, and with nothing to divide no part is made.

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/loop_load.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/scan_pool.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 20'718ULL * 86'400ULL * kSec;

std::atomic<uint64_t> g_counter{0};

std::string made(const fs::path& dir) {
    fs::create_directories(dir);
    return dir.string();
}

struct Store {
    fs::path dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    ob::QueryEngine engine;

    Store()
        : dir(fs::temp_directory_path() / ("ob_scan_parts_" + std::to_string(::getpid()) + "_" +
                                           std::to_string(g_counter.fetch_add(1))))
        , store(made(dir))
        , engine(store, [](const std::string&) -> std::shared_ptr<ob::SoABuffer> { return nullptr; },
                 agg) {
        store.set_symbol_exchange("BK", "EX");
    }
    ~Store() {
        store.close();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    void segment(const std::vector<ob::SnapshotRow>& rows) {
        store.set_symbol_exchange("BK", "EX");
        for (const auto& r : rows) store.append(r);
        EXPECT_TRUE(store.flush_segment().has_value());
    }
    void block(const std::vector<ob::SnapshotRow>& rows) {
        store.publish_blocks({ob::RowBlock::make("BK", "EX", rows)});
    }

    /// The reply the server would send, built as the server builds it: rows a batch or a part at a
    /// time into a `RowAnswer`, aggregates and buckets through the callback.
    std::string reply(const std::string& sql, ob::ScanPool* pool) {
        // Divided whenever there is a pool, small stores included: the parts' path is the one held
        // to the answer without a pool here, and these stores are below where a query divides.
        engine.set_scan_pool(pool, /*divide_small=*/true);
        ob::QueryShape shape;
        ob::QueryResponseBuilder builder(shape);
        const std::string err = engine.execute(sql, [&](const ob::QueryResult& r) { builder.add(r); },
                                               shape, &builder);
        engine.set_scan_pool(nullptr);
        return err.empty() ? builder.finish() : "ERR " + err;
    }
};

ob::SnapshotRow row(uint64_t ts, uint8_t side, uint16_t level, int64_t price, uint64_t qty,
                    uint64_t seq) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = seq;
    r.side            = side;
    r.level_index     = level;
    r.price           = price;
    r.quantity        = qty;
    r.order_count     = static_cast<uint32_t>(level % 4 + 1);
    return r;
}

/// Segments one after another in time, each a flush, the next one starting at the instant the last
/// ended - a tie on a time across two segments - and maybe a block after them.
uint64_t fill(Store& s, int segments, bool with_block) {
    uint64_t t = kBase;
    uint64_t seq = 0;
    for (int g = 0; g < segments + (with_block ? 1 : 0); ++g) {
        const int n = *rc::gen::inRange(1, 4000);
        std::vector<ob::SnapshotRow> rows;
        for (int i = 0; i < n; ++i) {
            if (i > 0) t += static_cast<uint64_t>(*rc::gen::inRange(0, 3)) * 7'000'000;
            rows.push_back(row(t, static_cast<uint8_t>(*rc::gen::inRange(0, 2)),
                               static_cast<uint16_t>(*rc::gen::inRange(0, 5)),
                               *rc::gen::inRange<int64_t>(-200, 2000),
                               static_cast<uint64_t>(*rc::gen::inRange(0, 50)), ++seq));
        }
        if (g == segments) {
            s.block(rows);
        } else {
            s.segment(rows);
        }
    }
    return t;
}

std::string conditions(uint64_t last) {
    std::string where = " WHERE timestamp BETWEEN ";
    const uint64_t span = last - kBase + 1;
    const uint64_t lo = *rc::gen::arbitrary<bool>() ? 0 : kBase + span * static_cast<uint64_t>(*rc::gen::inRange(0, 50)) / 100;
    const uint64_t hi = *rc::gen::arbitrary<bool>() ? UINT64_MAX : kBase + span * static_cast<uint64_t>(*rc::gen::inRange(50, 101)) / 100;
    where += std::to_string(lo) + " AND " + std::to_string(hi);
    if (*rc::gen::arbitrary<bool>()) {
        where += " AND price BETWEEN " + std::to_string(*rc::gen::inRange(-200, 900)) + " AND " +
                 std::to_string(*rc::gen::inRange(900, 2000));
    }
    if (*rc::gen::arbitrary<bool>()) where += " AND side = " + std::to_string(*rc::gen::inRange(0, 2));
    if (*rc::gen::arbitrary<bool>()) where += " AND level < " + std::to_string(*rc::gen::inRange(1, 5));
    return where;
}

}  // namespace

// ── The answers ───────────────────────────────────────────────────────────────────────────────

RC_GTEST_PROP(ScanPartsProperty, ARowAnswerOnThePoolIsTheBytesOfOneThreads, ()) {
    Store s;
    const uint64_t last = fill(s, *rc::gen::inRange(2, 6), *rc::gen::arbitrary<bool>());
    const auto column = rc::gen::element<std::string>("timestamp", "price", "quantity",
                                                      "order_count", "side", "level",
                                                      "sequence_number");
    std::string select = "SELECT *";
    if (*rc::gen::arbitrary<bool>()) {
        const auto names = *rc::gen::nonEmpty(rc::gen::container<std::vector<std::string>>(column));
        select = "SELECT ";
        for (size_t i = 0; i < names.size(); ++i) select += (i ? ", " : "") + names[i];
    }
    std::string sql = select + " FROM 'BK'.'EX'" + conditions(last);
    if (*rc::gen::arbitrary<bool>()) sql += " LIMIT " + std::to_string(*rc::gen::inRange(1, 9000));

    ob::ScanPool one(1, nullptr), two(2, nullptr), four(4, nullptr);
    const std::string alone = s.reply(sql, nullptr);
    RC_ASSERT(alone.compare(0, 3, "ERR") != 0);
    RC_ASSERT(s.reply(sql, &one) == alone);
    RC_ASSERT(s.reply(sql, &two) == alone);
    RC_ASSERT(s.reply(sql, &four) == alone);
}

RC_GTEST_PROP(ScanPartsProperty, BucketsOnThePoolAreTheBucketsOfOneThread, ()) {
    Store s;
    const uint64_t last = fill(s, *rc::gen::inRange(2, 6), *rc::gen::arbitrary<bool>());
    const auto width = *rc::gen::element<std::string>("7ms", "50ms", "1s", "1m");
    const std::string sql =
        "SELECT COUNT(*), FIRST(price), LAST(price), FIRST(quantity), LAST(quantity), MIN(price), "
        "MAX(price), MIN(quantity), MAX(quantity), SUM(quantity), AVG(price), AVG(quantity), "
        "VWAP(price) FROM 'BK'.'EX'" + conditions(last) + " GROUP BY TIME_BUCKET(" + width + ")" +
        (*rc::gen::arbitrary<bool>() ? " LIMIT " + std::to_string(*rc::gen::inRange(1, 50)) : "");
    ob::ScanPool one(1, nullptr), four(4, nullptr);
    const std::string alone = s.reply(sql, nullptr);
    RC_ASSERT(alone.compare(0, 3, "ERR") != 0);
    RC_ASSERT(s.reply(sql, &one) == alone);
    RC_ASSERT(s.reply(sql, &four) == alone);
}

TEST(ScanParts, ARowAnswerWhosePartsSpanManyChunksIsTheBytesOfOneThreads) {
    // Three segments of 30 000 rows of every column: some 2 MB of text a part, so several of a row
    // part's 256 KiB chunks each, and a LIMIT that ends in a later chunk of a later part is cut there
    // (#248). The property above draws parts of at most 4 000 rows, which a chunk mostly holds whole.
    Store s;
    uint64_t t = kBase;
    uint64_t seq = 0;
    for (int g = 0; g < 3; ++g) {
        std::vector<ob::SnapshotRow> rows;
        for (int i = 0; i < 30000; ++i) {
            t += 7'000'000;
            rows.push_back(row(t, static_cast<uint8_t>(i % 2), static_cast<uint16_t>(i % 5),
                               1'000'000 + (i * 37) % 100'000, static_cast<uint64_t>(i % 977 + 1),
                               ++seq));
        }
        s.segment(rows);
    }
    ob::ScanPool two(2, nullptr);
    for (const char* limit : {"", " LIMIT 1", " LIMIT 30000", " LIMIT 45001", " LIMIT 89999"}) {
        const std::string sql = std::string("SELECT * FROM 'BK'.'EX'") + limit;
        const std::string alone = s.reply(sql, nullptr);
        ASSERT_NE(alone.compare(0, 3, "ERR"), 0) << sql << ": " << alone.substr(0, 80);
        EXPECT_EQ(s.reply(sql, &two), alone) << sql;
    }
}

TEST(ScanParts, ATieOnATimeAcrossTwoSegmentsGoesToTheOrderTheScanDeliversIn) {
    // Two segments sharing their boundary instant: FIRST of the later segment's rows at that time
    // must not replace the earlier one's, and LAST must take the later one's.
    Store s;
    const uint64_t t = kBase + 5 * kSec;
    s.segment({row(t - kSec, 0, 0, 10, 1, 1), row(t, 0, 0, 20, 2, 2)});
    s.segment({row(t, 0, 0, 30, 3, 3), row(t + kSec, 0, 0, 40, 4, 4)});
    s.segment({row(t + kSec, 0, 0, 50, 5, 5)});
    const std::string sql = "SELECT FIRST(price), LAST(price), COUNT(*) FROM 'BK'.'EX' "
                            "WHERE timestamp BETWEEN " + std::to_string(t) + " AND " +
                            std::to_string(t + kSec) + " GROUP BY TIME_BUCKET(1m)";
    ob::ScanPool pool(4, nullptr);
    const std::string alone = s.reply(sql, nullptr);
    EXPECT_NE(alone.find("\t20\t50\t4\n"), std::string::npos) << alone;
    EXPECT_EQ(s.reply(sql, &pool), alone);
}

TEST(ScanParts, ASumTooLargeForTheMergeIsReadAgainOnOneThread) {
    // Prices and quantities near 2^63 and 2^64: one product P is about 2^127, so whether a sum part
    // way left 128 bits depends on where it fell. Across two segments of one bucket, both ending at
    // a sum of 1, whose VWAP would be answered:
    // - P, then -P and 1: the sums are P, 0, 1, and none left 128 bits - VWAP is answered;
    // - P, then 2^100, -P, -2^100 and 1: the sums are P, P + 2^100 - which left them - and on to 1,
    //   and VWAP is refused. The sizes of the second part's products stay inside 128 bits, so it is
    //   the bound that says the merge cannot tell, not their overflow.
    // A merge cannot tell either from the parts' sums, so both are read again on one thread.
    constexpr int64_t kP = std::numeric_limits<int64_t>::max();
    constexpr uint64_t kQ = std::numeric_limits<uint64_t>::max();
    constexpr int64_t k40 = int64_t{1} << 40;
    constexpr uint64_t k60 = uint64_t{1} << 60;
    for (const bool overflows : {false, true}) {
        Store s;
        s.segment({row(kBase + 1, 0, 0, kP, kQ, 1)});
        if (overflows) {
            s.segment({row(kBase + 2, 0, 0, k40, k60, 2), row(kBase + 3, 0, 0, -kP, kQ, 3),
                       row(kBase + 4, 0, 0, -k40, k60, 4), row(kBase + 5, 0, 0, 1, 1, 5)});
        } else {
            s.segment({row(kBase + 2, 0, 0, -kP, kQ, 2), row(kBase + 3, 0, 0, 1, 1, 3)});
        }
        const std::string sql = "SELECT VWAP(price), COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)";
        ob::ScanPool pool(2, nullptr);
        const std::string alone = s.reply(sql, nullptr);
        EXPECT_EQ(alone.rfind("ERR BUCKET_OVERFLOW", 0) == 0, overflows) << alone;
        EXPECT_EQ(s.reply(sql, &pool), alone) << (overflows ? "overflows" : "does not overflow");
    }
}

// ── Around the answer ─────────────────────────────────────────────────────────────────────────

namespace {

/// A part that counts its rows and, on the candidate `throw_at` among the parts made, throws.
std::atomic<int> g_parts_alive{0};

class CountingPart final : public ob::ScanPart {
public:
    explicit CountingPart(bool throws) : throws_(throws) { ++g_parts_alive; }
    ~CountingPart() override { --g_parts_alive; }
    ob::BatchVerdict take(const ob::RowBatch& b) override {
        if (throws_) throw std::runtime_error("a part failed");
        rows_ += b.rows.size();
        return {};
    }
    uint64_t rows() const override { return rows_; }

private:
    bool throws_;
    uint64_t rows_{0};
};

/// A part that records the rows of each step it is handed and stops at the batch's row `stop_at`.
class SteppedPart final : public ob::ScanPart {
public:
    explicit SteppedPart(uint64_t stop_at) : stop_at_(stop_at) {}
    ob::BatchVerdict take(const ob::RowBatch& b) override {
        steps.push_back(b.rows.size());
        for (size_t k = 0; k < b.rows.size(); ++k) {
            seen.push_back(b.rows[k]);
            if (rows_++ == stop_at_) return {true, k};
        }
        return {};
    }
    uint64_t rows() const override { return rows_; }

    std::vector<size_t> steps;
    std::vector<uint32_t> seen;

private:
    uint64_t stop_at_;
    uint64_t rows_{0};
};

void ten_segments(Store& s) {
    for (uint64_t g = 0; g < 10; ++g) {
        std::vector<ob::SnapshotRow> rows;
        for (uint64_t i = 0; i < 500; ++i) rows.push_back(row(kBase + (g * 1000 + i) * 1000, 0, 0, 7, 1, g * 1000 + i));
        s.segment(rows);
    }
}

}  // namespace

TEST(ScanParts, APoolThreadHandsAPartItsBatchAStepAtATimeAndKeepsItsStop) {
    // A batch of 1 000 chosen rows, every third row of the candidate's: on a pool thread the part is
    // handed 128 rows at a time (#248), and a stop at the batch's row 700 - row 60 of the sixth step -
    // is the batch's row 700, after which nothing more of the batch is handed over.
    std::vector<uint32_t> chosen;
    for (uint32_t i = 0; i < 1000; ++i) chosen.push_back(3 * i);
    ob::RowBatch batch{};
    batch.rows = chosen;

    SteppedPart whole(UINT64_MAX);
    EXPECT_FALSE(ob::take_in_steps(whole, batch).stop);
    EXPECT_EQ(whole.steps, (std::vector<size_t>{128, 128, 128, 128, 128, 128, 128, 104}));
    EXPECT_EQ(whole.seen, chosen);

    SteppedPart stopping(700);
    const ob::BatchVerdict v = ob::take_in_steps(stopping, batch);
    EXPECT_TRUE(v.stop);
    EXPECT_EQ(v.at, 700u) << "a stop in a later step was not said of the batch";
    EXPECT_EQ(stopping.steps, (std::vector<size_t>{128, 128, 128, 128, 128, 128}));
    ASSERT_EQ(stopping.seen.size(), 701u);
    EXPECT_EQ(stopping.seen.back(), chosen[700]);
}

TEST(ScanParts, ALimitReadsNoCandidateItWillNotNeed) {
    // Ten segments of 500 rows. A LIMIT the first one's rows may meet is read on the query's
    // thread, through the direct reader, with no part made; one of 600 has two candidates handed
    // out - the first may fall short, the two cannot - where the pool of four would read five.
    Store s;
    ten_segments(s);
    ob::ScanPool pool(4, nullptr);
    for (const uint64_t limit : {uint64_t{1}, uint64_t{500}, uint64_t{600}}) {
        std::atomic<int> made{0};
        uint64_t folded = 0;
        uint64_t direct = 0;
        ob::PartsScan how;
        how.pool = &pool;
        how.divide_small = true;
        how.limit_rows = limit;
        how.direct = [&direct, limit](const ob::RowBatch& b) -> ob::BatchVerdict {
            const uint64_t room = limit - direct;
            if (b.rows.size() >= room) {
                direct = limit;
                return {true, static_cast<size_t>(room - 1)};
            }
            direct += b.rows.size();
            return {};
        };
        how.make_part = [&made]() -> std::unique_ptr<ob::ScanPart> {
            ++made;
            return std::make_unique<CountingPart>(false);
        };
        how.fold = [&folded, limit](ob::ScanPart& p) {
            folded += p.rows();
            return folded < limit;
        };
        const auto cost = s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(),
                                             ob::RowFilter{}, how);
        EXPECT_TRUE(cost.stopped) << "LIMIT " << limit;
        if (limit <= 500) {
            EXPECT_EQ(made.load(), 0) << "LIMIT " << limit << " was divided";
            EXPECT_EQ(direct, limit);
            EXPECT_EQ(cost.segments_read, 1u);
        } else {
            EXPECT_EQ(made.load(), 2) << "LIMIT " << limit << ": a candidate it did not need was read";
            EXPECT_EQ(cost.segments_read, 2u);
        }
        EXPECT_EQ(g_parts_alive.load(), 0);
    }
}

TEST(ScanParts, APartThatThrowsReachesTheQueryWithEveryOtherPartSettled) {
    Store s;
    ten_segments(s);
    ob::ScanPool pool(3, nullptr);
    std::atomic<int> made{0};
    ob::PartsScan how;
    how.pool = &pool;
    how.divide_small = true;
    how.make_part = [&made]() -> std::unique_ptr<ob::ScanPart> {
        return std::make_unique<CountingPart>(made.fetch_add(1) == 3);
    };
    how.fold = [](ob::ScanPart&) { return true; };
    EXPECT_THROW(s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(),
                                    ob::RowFilter{}, how),
                 std::runtime_error);
    EXPECT_EQ(g_parts_alive.load(), 0) << "a part outlived the scan that made it";
    // And the pool reads the next query as if nothing had happened.
    made = 100;
    uint64_t rows = 0;
    how.fold = [&rows](ob::ScanPart& p) {
        rows += p.rows();
        return true;
    };
    s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), ob::RowFilter{}, how);
    EXPECT_EQ(rows, 5000u);
}

TEST(ScanParts, WithNothingToDivideTheScanIsTheDirectReaderAndNoPartIsMade) {
    Store s;
    s.segment({row(kBase, 0, 0, 7, 1, 1), row(kBase + 1, 0, 0, 8, 1, 2)});
    ob::ScanPool pool(2, nullptr), none(0, nullptr);
    for (ob::ScanPool* p : {static_cast<ob::ScanPool*>(nullptr), &none, &pool}) {
        std::atomic<int> made{0};
        uint64_t direct = 0;
        ob::PartsScan how;
        how.pool = p;
        how.divide_small = true;
        how.direct = [&direct](const ob::RowBatch& b) -> ob::BatchVerdict {
            direct += b.rows.size();
            return {};
        };
        how.make_part = [&made]() -> std::unique_ptr<ob::ScanPart> {
            ++made;
            return std::make_unique<CountingPart>(false);
        };
        how.fold = [](ob::ScanPart&) { return true; };
        s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), ob::RowFilter{}, how);
        EXPECT_EQ(made.load(), 0) << "one candidate was divided";
        EXPECT_EQ(direct, 2u);
    }
}

TEST(ScanParts, NoMorePartsAreHeldThanThePoolsThreadsAndOne) {
    // What bounds the memory of a query on the pool: a part is made as its candidate is handed out
    // and let go as it is folded, and no more are handed out ahead of the fold than the pool reads.
    Store s;
    ten_segments(s);
    for (const unsigned threads : {1u, 2u, 4u}) {
        ob::ScanPool pool(threads, nullptr);
        std::atomic<int> most{0};
        ob::PartsScan how;
        how.pool = &pool;
        how.divide_small = true;
        how.make_part = [&most]() -> std::unique_ptr<ob::ScanPart> {
            auto part = std::make_unique<CountingPart>(false);
            int alive = g_parts_alive.load();
            int seen = most.load();
            while (alive > seen && !most.compare_exchange_weak(seen, alive)) {
            }
            return part;
        };
        uint64_t rows = 0;
        how.fold = [&rows](ob::ScanPart& p) {
            rows += p.rows();
            return true;
        };
        s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), ob::RowFilter{}, how);
        EXPECT_EQ(rows, 5000u);
        EXPECT_LE(most.load(), static_cast<int>(threads) + 1) << threads << " thread(s)";
        EXPECT_EQ(g_parts_alive.load(), 0);
    }
}

TEST(ScanParts, BucketsPastTheCeilingOnlyTogetherAreRefusedAsOneThreadRefusesThem) {
    // Each segment's buckets are under the ceiling and the two together are not: the refusal is
    // the union's, which only the merge sees.
    Store s;
    for (uint64_t g = 0; g < 2; ++g) {
        std::vector<ob::SnapshotRow> rows;
        for (uint64_t b = 0; b < 40; ++b) rows.push_back(row(kBase + (g * 40 + b) * kSec, 0, 0, 5, 1, g * 40 + b + 1));
        s.segment(rows);
    }
    s.engine.set_max_query_buckets(60);
    const std::string sql = "SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)";
    ob::ScanPool pool(2, nullptr);
    const std::string alone = s.reply(sql, nullptr);
    EXPECT_EQ(alone.rfind("ERR BUCKETS_TOO_MANY", 0), 0u) << alone;
    EXPECT_EQ(s.reply(sql, &pool), alone);
}

TEST(ScanParts, AScanItsFoldEndsCountsTheSegmentsLeftUnreadBeforeItsPartAsOneThreadDoes) {
    // Six segments, every other one priced outside the condition and left unread by its metadata:
    // ended at the third part read, the scan has read three and left two unread - not three.
    Store s;
    uint64_t t = kBase;
    for (int g = 0; g < 6; ++g) {
        const int64_t price = g % 2 == 0 ? 100 : 5;
        s.segment({row(t, 0, 0, price, 1, static_cast<uint64_t>(2 * g + 1)),
                   row(t + 1, 0, 0, price + 1, 1, static_cast<uint64_t>(2 * g + 2))});
        t += kSec;
    }
    ob::RowFilter priced;
    priced.price_lo = 100;
    priced.price_hi = 110;
    int read = 0;
    const auto one_thread = s.store.scan_batches(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), priced,
                                                 [&read](const ob::RowBatch&) -> ob::BatchVerdict {
                                                     return {++read == 3, 0};
                                                 });
    ob::ScanPool pool(2, nullptr);
    int folded = 0;
    ob::PartsScan how;
    how.pool = &pool;
    how.divide_small = true;
    how.make_part = []() -> std::unique_ptr<ob::ScanPart> { return std::make_unique<CountingPart>(false); };
    how.fold = [&folded](ob::ScanPart&) { return ++folded < 3; };
    const auto parts = s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), priced, how);
    ASSERT_TRUE(one_thread.stopped);
    EXPECT_TRUE(parts.stopped);
    EXPECT_EQ(parts.segments_read, one_thread.segments_read);
    EXPECT_EQ(parts.skipped_by_price, one_thread.skipped_by_price);
    EXPECT_EQ(one_thread.segments_read, 3u);
    EXPECT_EQ(one_thread.skipped_by_price, 2u);
}

TEST(ScanParts, AScanAPoolCannotSpeedUpIsReadOnItsQuerysThread) {
    // A segment of 40 000 rows, one of 2 000, then eight of 5 000, one row a microsecond in that
    // order. Divided: the eight of 5 000, 40 000 rows spread over eight candidates. Not divided:
    // 4 000 rows over two of them - too few - nor the 42 000 of the first two, most of them in one.
    Store s;
    uint64_t next = 0;
    std::vector<uint64_t> first_of;   // the index of each segment's first row
    for (const uint64_t n : {40'000u, 2'000u, 5'000u, 5'000u, 5'000u, 5'000u, 5'000u, 5'000u, 5'000u, 5'000u}) {
        first_of.push_back(next);
        std::vector<ob::SnapshotRow> rows;
        for (uint64_t i = 0; i < n; ++i, ++next) rows.push_back(row(kBase + next * 1000, 0, 0, 7, 1, next + 1));
        s.segment(rows);
    }
    const auto at = [](uint64_t index) { return kBase + index * 1000; };
    ob::ScanPool pool(2, nullptr);
    const auto parts_made = [&](uint64_t from, uint64_t to) {
        std::atomic<int> made{0};
        ob::PartsScan how;
        how.pool = &pool;
        how.direct = [](const ob::RowBatch&) -> ob::BatchVerdict { return {}; };
        how.make_part = [&made]() -> std::unique_ptr<ob::ScanPart> {
            ++made;
            return std::make_unique<CountingPart>(false);
        };
        how.fold = [](ob::ScanPart&) { return true; };
        s.store.scan_parts(from, to, "BK", "EX", ob::ColumnSet::all(), ob::RowFilter{}, how);
        return made.load();
    };
    EXPECT_EQ(parts_made(at(first_of[2]), at(next - 1)), 8);
    EXPECT_EQ(parts_made(at(first_of[2] - 2000), at(first_of[2] + 1999)), 0)
        << "4 000 rows in range were divided";
    EXPECT_EQ(parts_made(at(0), at(first_of[2] - 1)), 0) << "a range mostly in one segment was divided";
    EXPECT_EQ(g_parts_alive.load(), 0);
}

TEST(ScanParts, AScanFindingThePoolDividedAlreadyReadsOnItsOwnThread) {
    // One scan divides at a time: a second query that came while the first held the pool reads on
    // its own thread rather than queueing behind the first one's parts.
    Store s;
    ten_segments(s);
    ob::ScanPool pool(2, nullptr);
    std::atomic<bool> first_out{false};
    std::atomic<bool> release{false};

    class HeldPart final : public ob::ScanPart {
    public:
        HeldPart(std::atomic<bool>& out, std::atomic<bool>& release) : out_(out), release_(release) {}
        ob::BatchVerdict take(const ob::RowBatch& b) override {
            out_ = true;
            while (!release_) std::this_thread::sleep_for(std::chrono::milliseconds(1));
            rows_ += b.rows.size();
            return {};
        }
        uint64_t rows() const override { return rows_; }

    private:
        std::atomic<bool>& out_;
        std::atomic<bool>& release_;
        uint64_t rows_{0};
    };

    uint64_t first_rows = 0;
    std::thread first([&] {
        ob::PartsScan how;
        how.pool = &pool;
        how.divide_small = true;
        how.make_part = [&]() -> std::unique_ptr<ob::ScanPart> {
            return std::make_unique<HeldPart>(first_out, release);
        };
        how.fold = [&first_rows](ob::ScanPart& p) {
            first_rows += p.rows();
            return true;
        };
        s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), ob::RowFilter{}, how);
    });
    for (int i = 0; i < 5000 && !first_out; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(first_out.load()) << "the first scan never handed a part out";

    std::atomic<int> made{0};
    uint64_t direct = 0;
    ob::PartsScan how;
    how.pool = &pool;
    how.divide_small = true;
    how.direct = [&direct](const ob::RowBatch& b) -> ob::BatchVerdict {
        direct += b.rows.size();
        return {};
    };
    how.make_part = [&made]() -> std::unique_ptr<ob::ScanPart> {
        ++made;
        return std::make_unique<CountingPart>(false);
    };
    how.fold = [](ob::ScanPart&) { return true; };
    s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), ob::RowFilter{}, how);
    release = true;
    first.join();
    EXPECT_EQ(made.load(), 0) << "the second scan divided while the first held the pool";
    EXPECT_EQ(direct, 5000u);
    EXPECT_EQ(first_rows, 5000u);
    EXPECT_TRUE(pool.claim()) << "the first scan kept the pool after it ended";
    pool.release();
}

TEST(ScanParts, AScanWithNoCpuToSpareFromTheClientLoopsIsReadOnItsQuerysThread) {
    // #249: with every CPU's client loop serving, a scan the pool could divide is read on its own
    // thread, and makes no part; with a CPU left over, it divides.
    Store s;
    ten_segments(s);
    ob::ScanPool pool(2, nullptr);
    ob::LoopLoad load(2);
    pool.set_loop_load(&load, 2);
    load.serving(0);
    load.serving(1);
    std::atomic<int> made{0};
    ob::PartsScan how;
    how.pool = &pool;
    how.divide_small = true;
    how.direct = [](const ob::RowBatch&) -> ob::BatchVerdict { return {}; };
    how.make_part = [&made]() -> std::unique_ptr<ob::ScanPart> {
        ++made;
        return std::make_unique<CountingPart>(false);
    };
    how.fold = [](ob::ScanPart&) { return true; };
    const auto kept = s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), ob::RowFilter{}, how);
    EXPECT_EQ(made.load(), 0) << "a scan divided with every CPU's client loop serving";
    EXPECT_EQ(kept.segments_read, 10u);
    EXPECT_EQ(pool.undivided(), 1u);
    load.stopped_at(1, ob::LoopLoad::Clock::now() - 2 * ob::ScanPool::kLoopRecent);
    s.store.scan_parts(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(), ob::RowFilter{}, how);
    EXPECT_GT(made.load(), 0) << "a scan with a CPU to spare did not divide";
    EXPECT_EQ(pool.undivided(), 1u);
    pool.set_loop_load(nullptr, 0);
}
