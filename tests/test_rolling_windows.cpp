// #44, step 3: rolling windows - `GROUP BY TIME_BUCKET(1m) ROLLING 15m`.
//
// A bucket answers what happened in its minute. What a trading desk asks more often is what
// happened in the last fifteen, every minute: a rolling VWAP, a rolling volume, the highest bid of
// the last hour. Each bucket answered carries the aggregates of the window that ends with it.
//
// The oracle is the requirement itself (9.4): a window that starts on a multiple of its own width is
// a bucket of that width, so its every column must equal `GROUP BY TIME_BUCKET(<window>)` over the
// same stretch - for the rows' aggregates and for the series of the book alike. The property at the
// end holds that over random books, and the counts and sums of every window, aligned or not, against
// the rows themselves.

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
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec = 1'000'000'000ULL;
constexpr uint64_t kMin = 60 * kSec;
constexpr uint64_t kBase = 20'718ULL * 86'400ULL * kSec;   // a whole UTC day, so buckets start on it

std::atomic<uint64_t> g_counter{0};

std::string made(const fs::path& dir) {
    fs::create_directories(dir);
    return dir.string();
}

ob::SnapshotRow row(uint64_t ts, int64_t price, uint64_t qty, uint8_t side = 0, uint16_t level = 0) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = 1;
    r.side            = side;
    r.level_index     = level;
    r.price           = price;
    r.quantity        = qty;
    r.order_count     = 1;
    return r;
}

struct Fixture {
    fs::path dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    std::unordered_map<std::string, std::shared_ptr<ob::SoABuffer>> live;
    ob::QueryEngine engine;

    Fixture()
        : dir(fs::temp_directory_path() / ("ob_rolling_" + std::to_string(::getpid()) + "_" +
                                           std::to_string(g_counter.fetch_add(1))))
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

    /// One segment of BK.EX holding these rows, in this order.
    void segment(const std::vector<ob::SnapshotRow>& rows) {
        store.set_symbol_exchange("BK", "EX");
        for (const auto& r : rows) store.append(r);
        ASSERT_TRUE(store.flush_segment().has_value());
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
    /// The answer of a bucket query that must succeed: bucket start -> its values, NULL as INT64_MIN.
    std::map<uint64_t, std::vector<int64_t>> buckets(const std::string& sql) {
        const Answer a = run(sql);
        EXPECT_TRUE(a.error.empty()) << sql << ": " << a.error;
        std::map<uint64_t, std::vector<int64_t>> out;
        for (const auto& r : a.rows) {
            std::vector<int64_t> v;
            for (const auto& x : r.agg_values) v.push_back(x.empty ? INT64_MIN : x.value);
            out[r.timestamp_ns] = v;
        }
        return out;
    }
};

std::string parse_error(Fixture& f, const std::string& sql) {
    ob::QueryAST ast;
    return f.engine.parse(sql, ast);
}

std::string range(uint64_t lo, uint64_t hi) {
    return " WHERE timestamp BETWEEN " + std::to_string(lo) + " AND " + std::to_string(hi);
}

constexpr int64_t kNull = INT64_MIN;

}  // namespace

// ── Parser ────────────────────────────────────────────────────────────────────

TEST(RollingParser, AWindowOfBucketsIsReadAndWrittenBack) {
    Fixture f;
    ob::QueryAST ast;
    ASSERT_EQ(f.engine.parse("SELECT COUNT(*), VWAP(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m) "
                             "rolling 900s LIMIT 3", ast), "");
    ASSERT_TRUE(ast.rolling_ns.has_value());
    EXPECT_EQ(*ast.rolling_ns, 15 * kMin);
    // Written back in the largest unit that divides it, which reads back to the same window.
    const std::string canonical = f.engine.format(ast);
    EXPECT_EQ(canonical, "SELECT COUNT(*), VWAP(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m) "
                         "ROLLING 15m LIMIT 3");
    ob::QueryAST again;
    ASSERT_EQ(f.engine.parse(canonical, again), "");
    EXPECT_EQ(again.rolling_ns, ast.rolling_ns);
    EXPECT_EQ(again.bucket_aggs, ast.bucket_aggs);

    ob::QueryAST plain;
    ASSERT_EQ(f.engine.parse("SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)", plain), "");
    EXPECT_FALSE(plain.rolling_ns.has_value()) << "a bucket query without ROLLING is step 1's";
}

TEST(RollingParser, AWindowIsAWholeNumberOfBucketsAndSaysSo) {
    Fixture f;
    const std::string e = parse_error(f, "SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m) "
                                         "ROLLING 90s");
    EXPECT_NE(e.find("a window is a whole number of buckets, and 90s is not one of 1m"),
              std::string::npos) << e;
    const std::string many = parse_error(f, "SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s) "
                                            "ROLLING 3h");
    EXPECT_NE(many.find("a window spans at most 10000 buckets, and 3h spans 10800 of 1s"),
              std::string::npos) << many;
    EXPECT_EQ(parse_error(f, "SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s) ROLLING 10000s"),
              "") << "exactly the limit is a window";
}

TEST(RollingParser, RefusesWhatIsNotAWindowByItsToken) {
    Fixture f;
    const std::string group = "SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1d) ";
    const struct { std::string tail; const char* says; } cases[] = {
        {"ROLLING", "expected the window after ROLLING"},
        {"ROLLING 0d", "a window is a positive integer and a unit, got '0'"},
        {"ROLLING 15", "expected the window's unit"},
        {"ROLLING 15x", "unknown unit 'x' - a window's is ns, us, ms, s, m, h or d"},
        {"ROLLING 367d", "a window is at most 366 d, got 367d"},
    };
    for (const auto& c : cases) {
        const std::string e = parse_error(f, group + c.tail);
        EXPECT_NE(e.find(c.says), std::string::npos) << c.tail << ": " << e;
    }
    const std::string alone = parse_error(f, "SELECT COUNT(*) FROM 'BK'.'EX' ROLLING 5m");
    EXPECT_NE(alone.find("ROLLING is a window of time buckets - it follows GROUP BY TIME_BUCKET(...)"),
              std::string::npos) << alone;
}

// ── The window of each bucket ─────────────────────────────────────────────────

TEST(Rolling, EachBucketCarriesTheWindowThatEndsWithIt) {
    Fixture f;
    // One row a minute, minute m at quantity m + 1 and price 100 + m.
    std::vector<ob::SnapshotRow> rows;
    for (uint64_t m = 0; m < 10; ++m) rows.push_back(row(kBase + m * kMin + kSec, 100 + static_cast<int64_t>(m), m + 1));
    f.segment(rows);

    const auto got = f.buckets("SELECT COUNT(*), SUM(quantity), FIRST(quantity), LAST(quantity), "
                               "MIN(price), MAX(price) FROM 'BK'.'EX'" +
                               range(kBase + 5 * kMin, kBase + 10 * kMin - 1) +
                               " GROUP BY TIME_BUCKET(1m) ROLLING 3m");
    ASSERT_EQ(got.size(), 5u) << "every bucket of the range, minutes 5 to 9";
    // Minute 5's window is minutes 3, 4 and 5 - two of them before the range the query names.
    EXPECT_EQ(got.at(kBase + 5 * kMin), (std::vector<int64_t>{3, 4 + 5 + 6, 4, 6, 103, 105}));
    EXPECT_EQ(got.at(kBase + 9 * kMin), (std::vector<int64_t>{3, 8 + 9 + 10, 8, 10, 107, 109}));
}

TEST(Rolling, AWindowWithNoRowIsAnsweredWithACountOfZero) {
    Fixture f;
    f.segment({row(kBase + kSec, 100, 1), row(kBase + 9 * kMin + kSec, 109, 10)});
    const auto got = f.buckets("SELECT COUNT(*), SUM(quantity), VWAP(price) FROM 'BK'.'EX'" +
                               range(kBase, kBase + 10 * kMin - 1) +
                               " GROUP BY TIME_BUCKET(1m) ROLLING 2m");
    ASSERT_EQ(got.size(), 10u) << "a window answers whether or not its own bucket holds a row";
    EXPECT_EQ(got.at(kBase + 1 * kMin), (std::vector<int64_t>{1, 1, 100'000'000}))
        << "minute 1's window holds minute 0's row";
    EXPECT_EQ(got.at(kBase + 5 * kMin), (std::vector<int64_t>{0, kNull, kNull}));
}

TEST(Rolling, AWindowOfOneBucketIsTheBucket) {
    Fixture f;
    f.segment({row(kBase + kSec, 100, 1), row(kBase + 2 * kSec, 104, 3), row(kBase + 3 * kMin, 90, 7)});
    const std::string select = "SELECT COUNT(*), SUM(quantity), AVG(price), VWAP(price), FIRST(price), "
                               "LAST(price) FROM 'BK'.'EX'" + range(kBase, kBase + 4 * kMin - 1) +
                               " GROUP BY TIME_BUCKET(1m)";
    const auto plain = f.buckets(select);
    const auto rolled = f.buckets(select + " ROLLING 1m");
    ASSERT_EQ(rolled.size(), 4u);
    for (const auto& [start, values] : plain) EXPECT_EQ(rolled.at(start), values) << start;
}

TEST(Rolling, TheCeilingCountsTheBucketsBeforeTheRange) {
    Fixture f;
    f.segment({row(kBase + kSec, 100, 1)});
    f.engine.set_max_query_buckets(10);
    // Five buckets answered, six more before them for the first window: eleven, one too many.
    const auto refused = f.run("SELECT COUNT(*) FROM 'BK'.'EX'" +
                               range(kBase + 10 * kMin, kBase + 15 * kMin - 1) +
                               " GROUP BY TIME_BUCKET(1m) ROLLING 7m");
    EXPECT_EQ(refused.error.rfind("BUCKETS_TOO_MANY", 0), 0u) << refused.error;
    EXPECT_TRUE(refused.rows.empty());
    const auto fits = f.run("SELECT COUNT(*) FROM 'BK'.'EX'" +
                            range(kBase + 10 * kMin, kBase + 15 * kMin - 1) +
                            " GROUP BY TIME_BUCKET(1m) ROLLING 6m");
    EXPECT_TRUE(fits.error.empty()) << fits.error;
    EXPECT_EQ(fits.rows.size(), 5u);
}

TEST(Rolling, LimitCountsTheBucketsAnswered) {
    Fixture f;
    f.segment({row(kBase + kSec, 100, 1), row(kBase + 5 * kMin, 100, 1)});
    const auto got = f.buckets("SELECT COUNT(*) FROM 'BK'.'EX'" + range(kBase, kBase + 10 * kMin - 1) +
                               " GROUP BY TIME_BUCKET(1m) ROLLING 5m LIMIT 3");
    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got.begin()->first, kBase);
}

TEST(Rolling, ASumPastSixtyFourBitsIsRefusedNamingItsBucket) {
    Fixture f;
    // Each minute's sum fits; the window of two does not.
    const uint64_t big = (1ULL << 62) + 1;
    f.segment({row(kBase + kSec, 100, big), row(kBase + kMin + kSec, 100, big)});
    const auto refused = f.run("SELECT SUM(quantity) FROM 'BK'.'EX'" + range(kBase, kBase + 2 * kMin - 1) +
                               " GROUP BY TIME_BUCKET(1m) ROLLING 2m");
    EXPECT_EQ(refused.error, "BUCKET_OVERFLOW: SUM(quantity) of the bucket at " +
                                 std::to_string(kBase + kMin) + " does not fit a 64-bit integer");
    EXPECT_TRUE(f.run("SELECT SUM(quantity) FROM 'BK'.'EX'" + range(kBase, kBase + 2 * kMin - 1) +
                      " GROUP BY TIME_BUCKET(1m)").error.empty())
        << "the control: without the window, each bucket fits";
}

TEST(Rolling, NearTheEpochTheWindowReachesNoFurtherThanZero) {
    Fixture f;
    f.segment({row(kSec, 100, 1), row(2 * kSec, 101, 2)});
    const auto got = f.buckets("SELECT COUNT(*), SUM(quantity) FROM 'BK'.'EX'" + range(0, 3 * kSec - 1) +
                               " GROUP BY TIME_BUCKET(1s) ROLLING 5s");
    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got.at(0), (std::vector<int64_t>{0, kNull}));
    EXPECT_EQ(got.at(2 * kSec), (std::vector<int64_t>{2, 3}));
}

TEST(Rolling, ASeriesWindowIsTheBookThroughTheWholeWindow) {
    Fixture f;
    // The bid: 100 from the start of minute 0, 110 from minute 1 on, 90 for the last half of minute 2.
    f.segment({row(kBase, 100, 1, 0), row(kBase, 200, 1, 1), row(kBase + kMin, 110, 1, 0),
               row(kBase + 2 * kMin + 30 * kSec, 90, 1, 0)});
    const auto got = f.buckets("SELECT OPEN(bid), HIGH(bid), LOW(bid), CLOSE(bid), TWAP(bid) FROM 'BK'.'EX'" +
                               range(kBase, kBase + 3 * kMin - 1) + " GROUP BY TIME_BUCKET(1m) ROLLING 3m");
    // Minute 2's window is the three minutes: 100 for one, 110 for one and a half, 90 for a half.
    EXPECT_EQ(got.at(kBase + 2 * kMin),
              (std::vector<int64_t>{100, 110, 90, 90, (100'000'000LL * 60 + 110'000'000LL * 90 +
                                                       90'000'000LL * 30) / 180}));
    // And the oracle: the same three minutes as one bucket of three.
    const auto whole = f.buckets("SELECT OPEN(bid), HIGH(bid), LOW(bid), CLOSE(bid), TWAP(bid) FROM 'BK'.'EX'" +
                                 range(kBase, kBase + 3 * kMin - 1) + " GROUP BY TIME_BUCKET(3m)");
    EXPECT_EQ(got.at(kBase + 2 * kMin), whole.at(kBase));
}

TEST(Rolling, ASeriesWindowReachesBeforeTheRangeToo) {
    Fixture f;
    f.segment({row(kBase, 100, 1, 0), row(kBase, 200, 1, 1), row(kBase + kMin, 110, 1, 0),
               row(kBase + 2 * kMin + 30 * kSec, 90, 1, 0)});
    // Only minute 2 answered, and its window is the same three minutes as above: the book of the
    // two before the range is read, not cut away as a step-2 bucket's first window is cut at lo.
    const auto got = f.buckets("SELECT OPEN(bid), HIGH(bid), LOW(bid), CLOSE(bid), TWAP(bid) FROM 'BK'.'EX'" +
                               range(kBase + 2 * kMin, kBase + 3 * kMin - 1) +
                               " GROUP BY TIME_BUCKET(1m) ROLLING 3m");
    ASSERT_EQ(got.size(), 1u);
    const auto whole = f.buckets("SELECT OPEN(bid), HIGH(bid), LOW(bid), CLOSE(bid), TWAP(bid) FROM 'BK'.'EX'" +
                                 range(kBase, kBase + 3 * kMin - 1) + " GROUP BY TIME_BUCKET(3m)");
    EXPECT_EQ(got.at(kBase + 2 * kMin), whole.at(kBase));
}

TEST(Rolling, ASeriesBesideAConditionIsRefusedAsStepTwoRefusesIt) {
    Fixture f;
    f.segment({row(kBase, 100, 1)});
    const auto refused = f.run("SELECT TWAP(mid) FROM 'BK'.'EX' WHERE price > 0 GROUP BY TIME_BUCKET(1m) "
                               "ROLLING 5m");
    EXPECT_EQ(refused.error.rfind("SERIES_FILTER", 0), 0u) << refused.error;
    EXPECT_TRUE(f.run("SELECT COUNT(*) FROM 'BK'.'EX' WHERE price > 0 GROUP BY TIME_BUCKET(1m) ROLLING 5m")
                    .error.empty())
        << "the rows' aggregates take conditions, rolling or not";
}

// ── The oracle (requirement 9.4) ──────────────────────────────────────────────

RC_GTEST_PROP(RollingProperty, AnAlignedWindowIsTheBucketOfItsWidthAndEveryWindowCountsItsRows, ()) {
    Fixture f;
    const uint64_t step = kSec * static_cast<uint64_t>(*rc::gen::inRange(1, 4));
    const uint64_t k = static_cast<uint64_t>(*rc::gen::inRange(1, 6));
    const uint64_t window = step * k;

    // Rows on both sides, level 0 and deeper, over sixty seconds in half seconds, in several segments.
    std::vector<ob::SnapshotRow> all;
    const int segments = *rc::gen::inRange(1, 4);
    for (int s = 0; s < segments; ++s) {
        std::vector<ob::SnapshotRow> rows;
        const int n = *rc::gen::inRange(1, 25);
        for (int i = 0; i < n; ++i) {
            rows.push_back(row(kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 120)) * (kSec / 2),
                               *rc::gen::inRange<int64_t>(90, 111),
                               static_cast<uint64_t>(*rc::gen::inRange(0, 9)),
                               static_cast<uint8_t>(*rc::gen::inRange(0, 2)),
                               static_cast<uint16_t>(*rc::gen::inRange(0, 2))));
        }
        f.segment(rows);
        all.insert(all.end(), rows.begin(), rows.end());
    }

    // The range: starting on a multiple of the window, so the plain query's first bucket is uncut.
    const uint64_t lo = kBase + window * static_cast<uint64_t>(*rc::gen::inRange(1, 4));
    const uint64_t hi = lo + window * static_cast<uint64_t>(*rc::gen::inRange(1, 5)) -
                        static_cast<uint64_t>(*rc::gen::inRange(1, 3)) * (kSec / 2);
    const std::string select = "SELECT COUNT(*), SUM(quantity), FIRST(price), LAST(quantity), MIN(price), "
                               "MAX(quantity), AVG(price), VWAP(price), OPEN(mid), HIGH(bid), LOW(ask), "
                               "CLOSE(spread), TWAP(mid) FROM 'BK'.'EX'";
    const auto rolled = f.buckets(select + range(lo, hi) + " GROUP BY TIME_BUCKET(" +
                                  std::to_string(step / kSec) + "s) ROLLING " +
                                  std::to_string(window / kSec) + "s");
    const auto plain = f.buckets(select + range(lo - window, hi) + " GROUP BY TIME_BUCKET(" +
                                 std::to_string(window / kSec) + "s)");

    RC_ASSERT(rolled.size() == (hi - hi % step - lo) / step + 1);
    for (const auto& [b, values] : rolled) {
        // Every window, aligned or not: its count and its sum from the rows themselves.
        const uint64_t ws = b + step - window;
        uint64_t count = 0;
        int64_t sum = 0;
        for (const auto& r : all) {
            if (r.timestamp_ns >= ws && r.timestamp_ns < b + step && r.timestamp_ns <= hi) {
                ++count;
                sum += static_cast<int64_t>(r.quantity);
            }
        }
        RC_ASSERT(values[0] == static_cast<int64_t>(count));
        RC_ASSERT(values[1] == (count == 0 ? kNull : sum));
        // A window that starts on a multiple of its width is the bucket of that width.
        if (ws % window == 0) {
            RC_ASSERT(plain.count(ws) == 1u);
            RC_ASSERT(values == plain.at(ws));
        }
    }
}
