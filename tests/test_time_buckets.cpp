// #44, step 1: aggregates over time buckets - `SELECT <aggregates> FROM ... GROUP BY TIME_BUCKET(1m)`.
//
// An aggregate with a time condition was refused (`AGG_TIME_FILTER`): the aggregates read the live
// book. A minute's bars of the top of the book, updates a second, the average quantity at the best
// level - the first thing a trading firm builds on L2 data - meant fetching every row and counting
// on the client. These run the engine over a store, as the server does, and the property at the end
// compares every bucket with an oracle computed from the rows a plain SELECT with the same conditions
// answers, in the order it answers them.

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec = 1'000'000'000ULL;
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
        : dir(fs::temp_directory_path() / ("ob_time_buckets_" + std::to_string(::getpid()) + "_" +
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
    /// The answer of a bucket query that must succeed: bucket start -> its values.
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

}  // namespace

// ── Parser ────────────────────────────────────────────────────────────────────

TEST(TimeBucketParse, EveryUnitIsItsNanoseconds) {
    Fixture f;
    const std::pair<const char*, uint64_t> cases[] = {
        {"7ns", 7}, {"7us", 7'000}, {"7ms", 7'000'000}, {"7s", 7 * kSec}, {"7m", 420 * kSec},
        {"7h", 7 * 3600 * kSec}, {"7d", 7 * 86'400 * kSec},
    };
    for (const auto& [text, ns] : cases) {
        ob::QueryAST ast;
        const std::string err = f.engine.parse(
            std::string("SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(") + text + ")", ast);
        ASSERT_TRUE(err.empty()) << text << ": " << err;
        ASSERT_TRUE(ast.bucket_ns.has_value());
        EXPECT_EQ(*ast.bucket_ns, ns) << text;
    }
}

TEST(TimeBucketParse, AnIntervalAndItsUnitMayBeApart) {
    Fixture f;
    ob::QueryAST a, b;
    ASSERT_TRUE(f.engine.parse("SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)", a).empty());
    ASSERT_TRUE(f.engine.parse("SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1 m)", b).empty());
    EXPECT_EQ(a.bucket_ns, b.bucket_ns);
}

TEST(TimeBucketParse, AnIntervalThatIsNoIntervalIsRefusedWhereItStands) {
    Fixture f;
    const char* bad[] = {
        "TIME_BUCKET(0s)",     // zero
        "TIME_BUCKET(-1s)",    // negative
        "TIME_BUCKET(5)",      // no unit
        "TIME_BUCKET(5x)",     // no such unit
        "TIME_BUCKET(5M)",     // units are lower case: M is not minutes
        "TIME_BUCKET(367d)",   // past a leap year
        "TIME_BUCKET(18446744073709551615ns)",   // fits u64, past the limit
        "TIME_BUCKET(99999999999999999999s)",    // does not fit u64
        "TIME_BUCKET()",
        "TIME_BUCKET(1s",
        "TIME(1s)",
    };
    for (const char* b : bad) {
        const std::string err = parse_error(f, std::string("SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY ") + b);
        EXPECT_FALSE(err.empty()) << b << " parsed";
        EXPECT_NE(err.find("Parse error at line 1, col "), std::string::npos) << b << ": " << err;
    }
    EXPECT_TRUE(parse_error(f, "SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(366d)").empty());
}

TEST(TimeBucketParse, ASelectListOfAnythingButBucketAggregatesIsRefusedByName) {
    Fixture f;
    const std::pair<const char*, const char*> cases[] = {
        {"price", "'price' is not one"},
        {"*", "'*' is not one"},
        {"COUNT(*), price", "'price' is not one"},
        {"SPREAD(*)", "aggregates the live book"},
        {"DEPTH(100)", "aggregates the live book"},
        {"SUM(bid)", "WHERE side = 0"},
        {"VWAP(ask)", "WHERE side = 1"},
        {"COUNT(price)", "COUNT takes '*'"},
        {"SUM(price)", "SUM of a bucket takes quantity"},
        {"VWAP(quantity)", "VWAP of a bucket takes price"},
        {"AVG(order_count)", "AVG of a bucket takes price or quantity"},
        {"FIRST(*)", "FIRST of a bucket takes price or quantity"},
    };
    for (const auto& [list, says] : cases) {
        const std::string err =
            parse_error(f, std::string("SELECT ") + list + " FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)");
        EXPECT_NE(err.find(says), std::string::npos) << list << ": " << err;
    }
}

TEST(TimeBucketParse, AtAndGroupByAreNotOneQuery) {
    Fixture f;
    const std::string err = parse_error(
        f, "SELECT COUNT(*) FROM 'BK'.'EX' WHERE AT " + std::to_string(kBase) + " GROUP BY TIME_BUCKET(1m)");
    EXPECT_NE(err.find("not both"), std::string::npos) << err;
}

TEST(TimeBucketParse, TheCanonicalFormReadsBackToTheSameQuery) {
    Fixture f;
    const std::pair<const char*, const char*> cases[] = {
        {"TIME_BUCKET(90s)", "TIME_BUCKET(90s)"},     // not a whole minute
        {"TIME_BUCKET(120s)", "TIME_BUCKET(2m)"},     // the largest unit that divides it
        {"TIME_BUCKET(1500ms)", "TIME_BUCKET(1500ms)"},
        {"TIME_BUCKET(24h)", "TIME_BUCKET(1d)"},
        {"TIME_BUCKET(1 ns)", "TIME_BUCKET(1ns)"},
    };
    for (const auto& [in, canonical] : cases) {
        ob::QueryAST a;
        ASSERT_TRUE(f.engine.parse(std::string("SELECT COUNT(*), VWAP(price) FROM 'BK'.'EX' WHERE price >= 5 "
                                               "GROUP BY ") + in + " LIMIT 3", a).empty()) << in;
        const std::string text = f.engine.format(a);
        EXPECT_NE(text.find(std::string("GROUP BY ") + canonical), std::string::npos) << in << ": " << text;
        ob::QueryAST b;
        ASSERT_TRUE(f.engine.parse(text, b).empty()) << text;
        EXPECT_EQ(a.bucket_ns, b.bucket_ns) << text;
        EXPECT_EQ(a.bucket_aggs, b.bucket_aggs) << text;
        EXPECT_EQ(a.limit, b.limit) << text;
        EXPECT_EQ(a.price_lo, b.price_lo) << text;
    }
}

// ── Execution ─────────────────────────────────────────────────────────────────

TEST(TimeBuckets, EachBucketAnswersItsOwnRows) {
    // Ten rows a second apart from kBase + 1 s, prices 100.., quantities 1..: buckets of 5 s hold
    // seconds 1-4, 5-9 and 10.
    Fixture f;
    std::vector<ob::SnapshotRow> rows;
    for (int i = 0; i < 10; ++i) rows.push_back(row(kBase + (i + 1) * kSec, 100 + i, i + 1));
    f.segment(rows);
    const auto b = f.buckets("SELECT COUNT(*), FIRST(price), LAST(price), MIN(quantity), MAX(quantity), "
                             "SUM(quantity), AVG(price), VWAP(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(5s)");
    ASSERT_EQ(b.size(), 3u);
    // 100x1 + 101x2 + 102x3 + 103x4 = 1020 over 10; the average of 100-103 is 101.5.
    EXPECT_EQ(b.at(kBase), (std::vector<int64_t>{4, 100, 103, 1, 4, 10, 101'500'000, 102'000'000}));
    // 104x5 + ... + 108x9 = 3720 over 35 = 106.2857142..., truncated at the scale.
    EXPECT_EQ(b.at(kBase + 5 * kSec), (std::vector<int64_t>{5, 104, 108, 5, 9, 35, 106'000'000, 106'285'714}));
    EXPECT_EQ(b.at(kBase + 10 * kSec), (std::vector<int64_t>{1, 109, 109, 10, 10, 10, 109'000'000, 109'000'000}));
}

TEST(TimeBuckets, ARowWithinOneIntervalOfTheEpochIsInTheFirstBucket) {
    // Before any row has a bucket the last one is none, though its start reads 0 - and a row within
    // one interval of the epoch is that far from 0: asked about first, it must find no bucket.
    Fixture f;
    f.segment({row(1 * kSec, 100, 1), row(2 * kSec, 101, 1), row(70 * kSec, 102, 1)});
    const auto b = f.buckets("SELECT COUNT(*), LAST(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)");
    ASSERT_EQ(b.size(), 2u);
    EXPECT_EQ(b.at(0), (std::vector<int64_t>{2, 101}));
    EXPECT_EQ(b.at(60 * kSec), (std::vector<int64_t>{1, 102}));
}

TEST(TimeBuckets, TheAnswerSaysEachColumnsScaleBeforeAnyRow) {
    Fixture f;
    f.segment({row(kBase, 100, 1)});
    const auto a = f.run("SELECT COUNT(*), VWAP(price), AVG(quantity) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)");
    ASSERT_TRUE(a.error.empty()) << a.error;
    EXPECT_TRUE(a.shape.is_buckets);
    ASSERT_EQ(a.shape.bucket_columns.size(), 3u);
    EXPECT_EQ(a.shape.bucket_columns[0].text, "COUNT(*)");
    EXPECT_EQ(a.shape.bucket_columns[0].scale, 1);
    EXPECT_EQ(a.shape.bucket_columns[1].scale, 1'000'000);
    EXPECT_EQ(a.shape.bucket_columns[2].scale, 1'000'000);
}

TEST(TimeBuckets, FirstAndLastAreByEventTimeWhateverTheOrderRowsArriveIn) {
    // A segment's rows as appended, and a correction for an earlier instant appended later (#168):
    // FIRST and LAST are the earliest and the latest event time, not the first and last delivered.
    Fixture f;
    f.segment({row(kBase + 3 * kSec, 300, 1), row(kBase + 1 * kSec, 100, 1), row(kBase + 2 * kSec, 200, 1)});
    f.segment({row(kBase + 4 * kSec, 400, 1), row(kBase + 500'000'000, 50, 1)});
    const auto b = f.buckets("SELECT FIRST(price), LAST(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)");
    ASSERT_EQ(b.size(), 1u);
    EXPECT_EQ(b.at(kBase), (std::vector<int64_t>{50, 400}));
}

TEST(TimeBuckets, ATieOnTheTimeIsBrokenByTheOrderOfDelivery) {
    // The rule SNAPSHOT keeps (#168): of two rows at one instant, LAST is the later delivered and
    // FIRST the earlier.
    Fixture f;
    f.segment({row(kBase + kSec, 111, 1), row(kBase + kSec, 222, 1), row(kBase + kSec, 333, 1)});
    const auto b = f.buckets("SELECT FIRST(price), LAST(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)");
    EXPECT_EQ(b.at(kBase), (std::vector<int64_t>{111, 333}));
}

TEST(TimeBuckets, AVwapOfNothingButZeroQuantitiesIsNullNotZero) {
    Fixture f;
    f.segment({row(kBase, 100, 0), row(kBase + 1, 200, 0)});
    const auto a = f.run("SELECT VWAP(price), COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)");
    ASSERT_TRUE(a.error.empty()) << a.error;
    ASSERT_EQ(a.rows.size(), 1u);
    EXPECT_TRUE(a.rows[0].agg_values[0].empty);
    EXPECT_EQ(a.rows[0].agg_values[1].value, 2);
}

TEST(TimeBuckets, ConditionsNarrowTheRowsTheyWereRefusedBesideTheBook) {
    // Beside a function of the live book a time, price, side or level condition is refused
    // (AGG_TIME_FILTER and the rest, #199, #200); beside a bucket it says which rows to aggregate.
    Fixture f;
    f.segment({row(kBase, 100, 1, 0, 0), row(kBase + 1, 101, 2, 1, 0), row(kBase + 2, 102, 3, 0, 1),
               row(kBase + 3 * kSec, 103, 4, 0, 0)});
    EXPECT_EQ(f.buckets("SELECT SUM(quantity) FROM 'BK'.'EX' WHERE side = 0 AND level = 0 "
                        "GROUP BY TIME_BUCKET(1s)"),
              (std::map<uint64_t, std::vector<int64_t>>{{kBase, {1}}, {kBase + 3 * kSec, {4}}}));
    EXPECT_EQ(f.buckets("SELECT COUNT(*) FROM 'BK'.'EX' WHERE price BETWEEN 101 AND 102 AND timestamp < " +
                        std::to_string(kBase + kSec) + " GROUP BY TIME_BUCKET(1s)"),
              (std::map<uint64_t, std::vector<int64_t>>{{kBase, {2}}}));
}

TEST(TimeBuckets, NoRowsIsNoBucketsAndStillAShape) {
    Fixture f;
    f.segment({row(kBase, 100, 1)});
    const auto a = f.run("SELECT COUNT(*) FROM 'BK'.'EX' WHERE price > 1000 GROUP BY TIME_BUCKET(1s)");
    ASSERT_TRUE(a.error.empty()) << a.error;
    EXPECT_TRUE(a.rows.empty());
    EXPECT_TRUE(a.shape.is_buckets);
    EXPECT_EQ(a.shape.bucket_columns.size(), 1u);
}

TEST(TimeBuckets, LimitKeepsTheFirstBucketsInTimeOrder) {
    // Delivered latest first: the limit is on buckets after they are ordered, not on rows as they come.
    Fixture f;
    f.segment({row(kBase + 9 * kSec, 9, 1)});
    f.segment({row(kBase + 1 * kSec, 1, 1), row(kBase + 5 * kSec, 5, 1), row(kBase + 3 * kSec, 3, 1)});
    const auto b = f.buckets("SELECT FIRST(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s) LIMIT 2");
    EXPECT_EQ(b, (std::map<uint64_t, std::vector<int64_t>>{{kBase + kSec, {1}}, {kBase + 3 * kSec, {3}}}));
}

TEST(TimeBuckets, PastTheCeilingTheQueryIsRefusedNotCutShort) {
    Fixture f;
    std::vector<ob::SnapshotRow> rows;
    for (int i = 0; i < 6; ++i) rows.push_back(row(kBase + i * kSec, 100, 1));
    f.segment(rows);
    f.engine.set_max_query_buckets(6);
    EXPECT_TRUE(f.run("SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)").error.empty());
    f.engine.set_max_query_buckets(5);
    const auto a = f.run("SELECT COUNT(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)");
    EXPECT_EQ(a.error.rfind("BUCKETS_TOO_MANY: more than 5 buckets", 0), 0u) << a.error;
    EXPECT_TRUE(a.rows.empty()) << "a refusal handed out rows";
}

TEST(TimeBuckets, AValueThatDoesNotFitIsARefusalNamingItAndItsBucket) {
    // Two quantities of 2^62 + 2^62 + 2^62 sum past INT64_MAX: SUM cannot answer, and wrapping it
    // would answer a negative volume.
    Fixture f;
    const uint64_t big = 1ULL << 62;
    f.segment({row(kBase, 1, big), row(kBase + 1, 1, big), row(kBase + 2, 1, big)});
    const auto a = f.run("SELECT COUNT(*), SUM(quantity) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)");
    EXPECT_EQ(a.error, "BUCKET_OVERFLOW: SUM(quantity) of the bucket at " + std::to_string(kBase) +
                           " does not fit a 64-bit integer");
    EXPECT_TRUE(a.rows.empty());
    // Their volume-weighted price is still 1: weighed by those quantities and divided before it is
    // scaled. Their average quantity, scaled by 10^6, is not a 64-bit number - refused the same way.
    const auto vwap = f.buckets("SELECT VWAP(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)");
    EXPECT_EQ(vwap.at(kBase), (std::vector<int64_t>{1'000'000}));
    EXPECT_EQ(f.run("SELECT AVG(quantity) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)").error.rfind(
                  "BUCKET_OVERFLOW: AVG(quantity)", 0),
              0u);
}

TEST(TimeBuckets, ABucketFunctionWithoutGroupByIsRefusedWithWhatItNeeds) {
    Fixture f;
    f.segment({row(kBase, 100, 1)});
    const auto a = f.run("SELECT COUNT(*) FROM 'BK'.'EX'");
    EXPECT_EQ(a.error.rfind("AGG_NEEDS_BUCKET: COUNT", 0), 0u) << a.error;
}

TEST(TimeBuckets, VwapReadsTheQuantitiesItIsNotAskedFor) {
    // VWAP(price) names one column and weighs by another: without a quantity aggregate beside it,
    // the quantities have to be read all the same - or every bucket's VWAP is NULL.
    Fixture f;
    f.segment({row(kBase, 100, 1), row(kBase + 1, 200, 3)});
    EXPECT_EQ(f.buckets("SELECT VWAP(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)").at(kBase),
              (std::vector<int64_t>{175'000'000}));
}

TEST(TimeBuckets, ANegativePriceAveragesTowardZeroLikeEveryScaledValue) {
    Fixture f;
    f.segment({row(kBase, -7, 1), row(kBase + 1, 0, 1)});
    const auto b = f.buckets("SELECT AVG(price), VWAP(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)");
    EXPECT_EQ(b.at(kBase), (std::vector<int64_t>{-3'500'000, -3'500'000}));
}

// ── The answer on the wire ─────────────────────────────────────────────────────

TEST(TimeBucketResponse, TheHeaderCarriesEachColumnsScaleAndAnEmptyValueIsNull) {
    ob::QueryShape shape;
    shape.is_buckets = true;
    shape.bucket_columns = {{"COUNT(*)", 1}, {"VWAP(price)", 1'000'000}};
    ob::QueryResponseBuilder reply(shape);
    ob::QueryResult r{};
    r.timestamp_ns = 60 * kSec;
    r.agg_values = {{"COUNT(*)", 3, false, 1}, {"VWAP(price)", 0, true, 1'000'000}};
    reply.add(r);
    EXPECT_EQ(reply.finish(), "OK\nbucket_ns\tCOUNT(*)/1\tVWAP(price)/1000000\n60000000000\t3\tNULL\n\n");
}

TEST(TimeBucketResponse, NoBucketsIsTheHeaderAlone) {
    ob::QueryShape shape;
    shape.is_buckets = true;
    shape.bucket_columns = {{"COUNT(*)", 1}};
    ob::QueryResponseBuilder reply(shape);
    EXPECT_EQ(reply.finish(), "OK\nbucket_ns\tCOUNT(*)/1\n\n");
}

// ── Against an oracle ─────────────────────────────────────────────────────────

RC_GTEST_PROP(TimeBucketProps, prop_every_bucket_is_what_its_rows_make, ()) {
    Fixture f;
    // Up to three segments of up to twenty rows, times anywhere in ten seconds - ties included -
    // and out of order, prices either side of zero, quantities from zero.
    const int segments = *rc::gen::inRange(1, 4);
    for (int s = 0; s < segments; ++s) {
        const auto n = *rc::gen::inRange(1, 21);
        std::vector<ob::SnapshotRow> rows;
        for (int i = 0; i < n; ++i) {
            rows.push_back(row(kBase + *rc::gen::inRange<uint64_t>(0, 40) * 250'000'000ULL,
                               *rc::gen::inRange<int64_t>(-50, 200), *rc::gen::inRange<uint64_t>(0, 9),
                               static_cast<uint8_t>(*rc::gen::inRange(0, 2)),
                               static_cast<uint16_t>(*rc::gen::inRange(0, 3))));
        }
        f.segment(rows);
    }
    const uint64_t width = *rc::gen::element<uint64_t>(250'000'000ULL, kSec, 3 * kSec, 7 * kSec, 60 * kSec);
    const std::string where = *rc::gen::element<std::string>("", " WHERE side = 1", " WHERE price >= 0",
                                                             " WHERE level <= 1 AND price < 150");
    // A random part of the list: a column an aggregate reads without naming it is read only if the
    // engine says so, and a list that always held a quantity aggregate hid VWAP's need for one.
    const std::vector<std::string> all = {"COUNT(*)", "FIRST(price)", "LAST(quantity)", "MIN(price)",
                                          "MAX(quantity)", "SUM(quantity)", "AVG(price)", "VWAP(price)"};
    std::vector<size_t> asked;
    for (size_t i = 0; i < all.size(); ++i) {
        if (*rc::gen::arbitrary<bool>()) asked.push_back(i);
    }
    if (asked.empty()) asked.push_back(*rc::gen::inRange<size_t>(0, all.size()));
    std::string list;
    for (size_t i : asked) list += (list.empty() ? "" : ", ") + all[i];

    // The oracle: the rows a plain SELECT answers under the same conditions, in its order.
    std::vector<ob::QueryResult> seen;
    RC_ASSERT(f.engine.execute("SELECT * FROM 'BK'.'EX'" + where,
                               [&](const ob::QueryResult& r) { seen.push_back(r); }).empty());
    struct Expect {
        uint64_t count = 0, first_ts = 0, last_ts = 0;
        int64_t first_price = 0, min_price = 0, sum_price = 0, sum_px_qty = 0;
        uint64_t last_qty = 0, max_qty = 0, sum_qty = 0;
    };
    std::map<uint64_t, Expect> want;
    for (const auto& r : seen) {
        Expect& e = want[r.timestamp_ns - r.timestamp_ns % width];
        if (e.count == 0 || r.timestamp_ns < e.first_ts) { e.first_ts = r.timestamp_ns; e.first_price = r.price; }
        if (e.count == 0 || r.timestamp_ns >= e.last_ts) { e.last_ts = r.timestamp_ns; e.last_qty = r.quantity; }
        e.min_price = e.count == 0 ? r.price : std::min(e.min_price, r.price);
        e.max_qty = e.count == 0 ? r.quantity : std::max(e.max_qty, r.quantity);
        ++e.count;
        e.sum_qty += r.quantity;
        e.sum_price += r.price;
        e.sum_px_qty += r.price * static_cast<int64_t>(r.quantity);
    }

    const auto got = f.buckets("SELECT " + list + " FROM 'BK'.'EX'" + where +
                               " GROUP BY TIME_BUCKET(" + std::to_string(width) + "ns)");
    RC_ASSERT(got.size() == want.size());
    for (const auto& [start, e] : want) {
        RC_ASSERT(got.count(start) == 1u);
        const auto& v = got.at(start);
        // Truncated toward zero, as the engine scales: (sum * 10^6) / divisor in exact arithmetic.
        const auto scaled = [](int64_t sum, int64_t divisor) {
            return static_cast<int64_t>(static_cast<__int128>(sum) * 1'000'000 / divisor);
        };
        const int64_t vwap = e.sum_qty == 0 ? INT64_MIN : scaled(e.sum_px_qty, static_cast<int64_t>(e.sum_qty));
        const std::vector<int64_t> every = {static_cast<int64_t>(e.count), e.first_price,
                                            static_cast<int64_t>(e.last_qty), e.min_price,
                                            static_cast<int64_t>(e.max_qty), static_cast<int64_t>(e.sum_qty),
                                            scaled(e.sum_price, static_cast<int64_t>(e.count)), vwap};
        std::vector<int64_t> expected;
        for (size_t i : asked) expected.push_back(every[i]);
        RC_ASSERT(v == expected);
    }
}
