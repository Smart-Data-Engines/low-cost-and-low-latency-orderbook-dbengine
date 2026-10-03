// #199: what a WHERE condition answers.
//
// Every comparison set one end of a range and nothing else. `=` set the lower bound, so
// `price = 200` answered every price from 200 up; `>` and `<` set the bound itself, so each kept the
// one value it excludes; and a second condition on a column replaced the first instead of narrowing
// it. A SNAPSHOT read neither its price nor its time conditions, a second AT replaced the first, an
// aggregate with AT answered the book as rows, and a subscription with AT pushed every row. All of
// it was answered `OK`. Measured on master (`14d5095`) through the library before the fix.
//
// These run the engine over a store, as the server does, and compare what comes back with what the
// conditions allow - the property below by an oracle that evaluates each condition on each row.

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/query_engine.hpp"
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
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kBase = 1'790'000'000'000'000'000ULL;
constexpr uint8_t  kBid  = 0;
constexpr uint8_t  kAsk  = 1;
constexpr uint64_t kU64Max = std::numeric_limits<uint64_t>::max();
constexpr int64_t  kI64Min = std::numeric_limits<int64_t>::min();
constexpr int64_t  kI64Max = std::numeric_limits<int64_t>::max();

std::atomic<uint64_t> g_counter{0};

std::string made(const fs::path& dir) {
    fs::create_directories(dir);
    return dir.string();
}

ob::SnapshotRow row_at(uint64_t ts, int64_t price, uint8_t side = kBid, uint16_t level = 0) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = 1;
    r.side            = side;
    r.level_index     = level;
    r.price           = price;
    r.quantity        = 5;
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
        : dir(fs::temp_directory_path() / ("ob_query_conditions_" + std::to_string(::getpid()) +
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

    /// One segment of CND.EX holding these rows, in this order.
    void segment(const std::vector<ob::SnapshotRow>& rows) {
        store.set_symbol_exchange("CND", "EX");
        for (const auto& r : rows) store.append(r);
        ASSERT_TRUE(store.flush_segment().has_value());
    }

    /// Three rows, one a second apart in event time and 100 apart in price.
    void three() { segment({row_at(kBase + 1000, 100), row_at(kBase + 2000, 200), row_at(kBase + 3000, 300)}); }

    /// The prices a `SELECT * ... WHERE <where>` answers, in the order answered.
    std::vector<int64_t> prices(const std::string& where) {
        std::vector<int64_t> out;
        const std::string err = engine.execute("SELECT * FROM 'CND'.'EX' WHERE " + where,
                                               [&](const ob::QueryResult& r) { out.push_back(r.price); });
        EXPECT_TRUE(err.empty()) << where << ": " << err;
        return out;
    }

    /// What `execute()` refuses `sql` with, or "" when it answers.
    std::string refusal(const std::string& sql) {
        return engine.execute(sql, [](const ob::QueryResult&) {});
    }
};

using Prices = std::vector<int64_t>;

}  // namespace

TEST(QueryConditions, EqualsAsksForOneValue) {
    Fixture f;
    f.three();
    EXPECT_EQ(f.prices("price = 200"), (Prices{200})) << "= answered as >=";
    EXPECT_EQ(f.prices("timestamp = " + std::to_string(kBase + 2000)), (Prices{200}));
}

TEST(QueryConditions, AStrictComparisonLeavesItsBoundOut) {
    // `timestamp > t` is how a client asks for what came after the last row it read; keeping t
    // handed that row back on every poll.
    Fixture f;
    f.three();
    EXPECT_EQ(f.prices("price > 200"), (Prices{300}));
    EXPECT_EQ(f.prices("price < 200"), (Prices{100}));
    EXPECT_EQ(f.prices("timestamp > " + std::to_string(kBase + 2000)), (Prices{300}));
    EXPECT_EQ(f.prices("timestamp < " + std::to_string(kBase + 2000)), (Prices{100}));
}

TEST(QueryConditions, AnInclusiveComparisonKeepsItsBound) {
    // What these answered before as well: the strict ones are the inclusive ones a step inside, so
    // a fix that moved these too would show here.
    Fixture f;
    f.three();
    EXPECT_EQ(f.prices("price >= 200"), (Prices{200, 300}));
    EXPECT_EQ(f.prices("price <= 200"), (Prices{100, 200}));
    EXPECT_EQ(f.prices("timestamp >= " + std::to_string(kBase + 2000)), (Prices{200, 300}));
    EXPECT_EQ(f.prices("timestamp <= " + std::to_string(kBase + 2000)), (Prices{100, 200}));
}

TEST(QueryConditions, ConditionsOnOneColumnNarrowEachOther) {
    Fixture f;
    f.three();
    EXPECT_EQ(f.prices("price >= 300 AND price >= 100"), (Prices{300}))
        << "the second lower bound replaced the first";
    EXPECT_EQ(f.prices("price <= 100 AND price <= 300"), (Prices{100}))
        << "the second upper bound replaced the first";
    EXPECT_EQ(f.prices("price BETWEEN 100 AND 200 AND price BETWEEN 200 AND 300"), (Prices{200}));
    EXPECT_EQ(f.prices("timestamp BETWEEN " + std::to_string(kBase + 1000) + " AND " +
                       std::to_string(kBase + 3000) + " AND timestamp > " + std::to_string(kBase + 1000)),
              (Prices{200, 300}));
    EXPECT_EQ(f.prices("price > 100 AND timestamp < " + std::to_string(kBase + 3000)), (Prices{200}));
}

TEST(QueryConditions, ARangeNothingSatisfiesAnswersNoRows) {
    // Past the end of the type there is no step inside a strict bound: nothing is greater than the
    // largest value or less than the smallest. A step taken anyway wraps, and the range it makes is
    // the whole column.
    Fixture f;
    f.three();
    EXPECT_TRUE(f.prices("timestamp > 18446744073709551615").empty());
    EXPECT_TRUE(f.prices("timestamp < 0").empty());
    EXPECT_TRUE(f.prices("price > 9223372036854775807").empty());
    EXPECT_TRUE(f.prices("price < -9223372036854775808").empty());
    EXPECT_TRUE(f.prices("price BETWEEN 300 AND 100").empty());
    EXPECT_TRUE(f.prices("price = 200 AND price = 300").empty());

    ob::QueryAST ast;
    ASSERT_TRUE(f.engine.parse("SELECT * FROM 'CND'.'EX' WHERE timestamp > 18446744073709551615", ast).empty());
    ASSERT_TRUE(ast.ts_start_ns.has_value() && ast.ts_end_ns.has_value());
    EXPECT_GT(*ast.ts_start_ns, *ast.ts_end_ns);
}

TEST(QueryConditions, TheCanonicalFormReadsBackAsTheSameRange) {
    Fixture f;
    const std::vector<std::string> wheres = {
        "price > 200",
        "price = 200",
        "timestamp < 5",
        "price >= 300 AND price >= 100 AND price < 400",
        "timestamp > 18446744073709551615",
        "price < -9223372036854775808",
        "timestamp BETWEEN 10 AND 20 AND timestamp BETWEEN 15 AND 30",
        "AT 5000 AND price BETWEEN 95 AND 160",
        "AT 5000 AND price > 100",
    };
    for (const auto& where : wheres) {
        ob::QueryAST first, again;
        ASSERT_TRUE(f.engine.parse("SELECT * FROM 'CND'.'EX' WHERE " + where, first).empty()) << where;
        const std::string canonical = f.engine.format(first);
        ASSERT_TRUE(f.engine.parse(canonical, again).empty()) << canonical;
        EXPECT_EQ(again.ts_start_ns, first.ts_start_ns) << where << " -> " << canonical;
        EXPECT_EQ(again.ts_end_ns, first.ts_end_ns) << where << " -> " << canonical;
        EXPECT_EQ(again.price_lo, first.price_lo) << where << " -> " << canonical;
        EXPECT_EQ(again.price_hi, first.price_hi) << where << " -> " << canonical;
        EXPECT_EQ(again.snapshot_ts_ns, first.snapshot_ts_ns) << where << " -> " << canonical;
    }
    ob::QueryAST ast;
    ASSERT_TRUE(f.engine.parse("SELECT * FROM 'CND'.'EX' WHERE price > 200", ast).empty());
    EXPECT_EQ(f.engine.format(ast), "SELECT * FROM 'CND'.'EX' WHERE price >= 201");
}

TEST(QueryConditions, ASnapshotKeepsTheLevelsOfItsBookPricedInRange) {
    // The bid's best level moved from 100 to 150; its second level is at 90, the ask at 200.
    Fixture f;
    f.segment({row_at(kBase + 1000, 100, kBid, 0), row_at(kBase + 1000, 90, kBid, 1),
               row_at(kBase + 1000, 200, kAsk, 0), row_at(kBase + 2000, 150, kBid, 0)});
    const std::string at = "AT " + std::to_string(kBase + 5000);
    EXPECT_EQ(f.prices(at), (Prices{150, 90, 200}));
    EXPECT_EQ(f.prices(at + " AND price BETWEEN 95 AND 160"), (Prices{150}))
        << "the price condition was ignored";
    // The level's latest price is out of range, and an older row of it is in: the book at that
    // moment has no level in range, so nothing - not the older row.
    EXPECT_TRUE(f.prices(at + " AND price BETWEEN 95 AND 120").empty());
    // LIMIT counts what is answered, after the condition.
    EXPECT_EQ(f.prices(at + " AND price >= 160 LIMIT 1"), (Prices{200}));
}

TEST(QueryConditions, ASnapshotRefusesATimestampCondition) {
    Fixture f;
    f.three();
    const std::string err = f.refusal("SELECT * FROM 'CND'.'EX' WHERE AT " + std::to_string(kBase + 5000) +
                                      " AND timestamp <= " + std::to_string(kBase + 1500));
    EXPECT_EQ(err.rfind("SNAPSHOT_TIME_FILTER", 0), 0u) << err;
}

TEST(QueryConditions, ASecondAtIsRefused) {
    Fixture f;
    ob::QueryAST ast;
    const std::string err = f.engine.parse("SELECT * FROM 'CND'.'EX' WHERE AT 5 AND AT 7", ast);
    EXPECT_NE(err.find("a second AT"), std::string::npos) << err;
}

TEST(QueryConditions, AnAggregateRefusesAt) {
    Fixture f;
    f.three();
    const std::string err = f.refusal("SELECT SPREAD(*) FROM 'CND'.'EX' WHERE AT " + std::to_string(kBase + 5000));
    EXPECT_EQ(err.rfind("AGG_TIME_FILTER", 0), 0u) << "an aggregate with AT answered: '" << err << "'";
}

TEST(QueryConditions, ASubscriptionPushesOnlyWhatItsConditionsAllow) {
    Fixture f;
    const std::vector<ob::SnapshotRow> rows = {row_at(kBase + 1000, 100), row_at(kBase + 2000, 200),
                                               row_at(kBase + 3000, 300)};
    const std::vector<std::pair<std::string, Prices>> cases = {
        {"price > 200", {300}},
        {"price = 200", {200}},
        {"timestamp < " + std::to_string(kBase + 2000), {100}},
        {"price >= 100 AND price >= 300", {300}},
    };
    for (const auto& [where, want] : cases) {
        Prices pushed;
        const uint64_t id = f.engine.subscribe("SUBSCRIBE * FROM 'CND'.'EX' WHERE " + where,
                                               [&](const ob::QueryResult& r) { pushed.push_back(r.price); });
        ASSERT_NE(id, 0u) << where;
        f.engine.notify_subscribers("CND", "EX", rows);
        f.engine.unsubscribe(id);
        EXPECT_EQ(pushed, want) << where;
    }
}

TEST(QueryConditions, ASubscriptionRefusesAt) {
    Fixture f;
    EXPECT_EQ(f.engine.subscribe("SUBSCRIBE * FROM 'CND'.'EX' WHERE AT 5", [](const ob::QueryResult&) {}), 0u)
        << "a subscription with AT pushed every row";
}

// #200: `side` and `level` are conditions as time and price are, with #199's rules.

TEST(QueryConditions, SideAndLevelNarrowRowsAsTheOtherColumnsDo) {
    Fixture f;
    f.segment({row_at(kBase + 1000, 100, kBid, 0), row_at(kBase + 1000, 99, kBid, 1),
               row_at(kBase + 1000, 101, kAsk, 0), row_at(kBase + 1000, 102, kAsk, 1)});
    const auto sorted = [](Prices p) { std::sort(p.begin(), p.end()); return p; };
    EXPECT_EQ(sorted(f.prices("side = 1")), (Prices{101, 102}));
    EXPECT_EQ(sorted(f.prices("side < 1")), (Prices{99, 100}));
    EXPECT_EQ(sorted(f.prices("side = 0 AND level = 0")), (Prices{100})) << "the top of the bids";
    EXPECT_EQ(sorted(f.prices("level >= 1")), (Prices{99, 102}));
    EXPECT_EQ(sorted(f.prices("level BETWEEN 0 AND 0 AND side > 0")), (Prices{101}));
    EXPECT_TRUE(f.prices("side = 2").empty());
    EXPECT_TRUE(f.prices("side > 255").empty());
}

TEST(QueryConditions, ANarrowedSelectStillReadsTheColumnsItsConditionsAreOn) {
    // `SELECT price` answers one column and its side condition needs another: a scan that read only
    // what it answers would see every row's side as 0 and keep none of the asks.
    Fixture f;
    f.segment({row_at(kBase + 1000, 100, kBid, 0), row_at(kBase + 1000, 99, kBid, 1),
               row_at(kBase + 1000, 101, kAsk, 0), row_at(kBase + 1000, 102, kAsk, 1)});
    Prices got;
    const std::string err = f.engine.execute("SELECT price FROM 'CND'.'EX' WHERE side = 1 AND level = 1",
                                             [&](const ob::QueryResult& r) { got.push_back(r.price); });
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(got, (Prices{102}));
}

TEST(QueryConditions, ASnapshotKeepsTheSideAndTheLevelsItIsAskedFor) {
    Fixture f;
    f.segment({row_at(kBase + 1000, 100, kBid, 0), row_at(kBase + 1000, 99, kBid, 1),
               row_at(kBase + 1000, 101, kAsk, 0), row_at(kBase + 1000, 102, kAsk, 1)});
    const std::string at = "AT " + std::to_string(kBase + 5000);
    EXPECT_EQ(f.prices(at + " AND side = 1"), (Prices{101, 102}));
    EXPECT_EQ(f.prices(at + " AND level = 0"), (Prices{100, 101})) << "the best of each side";
}

TEST(QueryConditions, ASubscriptionPushesOnlyTheSideItAsksFor) {
    Fixture f;
    const std::vector<ob::SnapshotRow> rows = {row_at(kBase + 1000, 100, kBid, 0),
                                               row_at(kBase + 1000, 101, kAsk, 0)};
    Prices pushed;
    const uint64_t id = f.engine.subscribe("SUBSCRIBE * FROM 'CND'.'EX' WHERE side = 1",
                                           [&](const ob::QueryResult& r) { pushed.push_back(r.price); });
    ASSERT_NE(id, 0u);
    f.engine.notify_subscribers("CND", "EX", rows);
    f.engine.unsubscribe(id);
    EXPECT_EQ(pushed, (Prices{101}));
}

TEST(QueryConditions, ASideOrALevelPastItsColumnsTypeIsAParseError) {
    Fixture f;
    ob::QueryAST ast;
    std::string err = f.engine.parse("SELECT * FROM 'CND'.'EX' WHERE side = 256", ast);
    EXPECT_NE(err.find("out of range for side"), std::string::npos) << err;
    err = f.engine.parse("SELECT * FROM 'CND'.'EX' WHERE level = 65536", ast);
    EXPECT_NE(err.find("out of range for level"), std::string::npos) << err;
    EXPECT_TRUE(f.engine.parse("SELECT * FROM 'CND'.'EX' WHERE level = 65535", ast).empty());
}

TEST(QueryConditions, SideAndLevelReadBackFromTheCanonicalForm) {
    Fixture f;
    for (const std::string where : {"side = 1", "level > 3", "side >= 0 AND level <= 9",
                                    "AT 5000 AND side = 0 AND level = 0"}) {
        ob::QueryAST first, again;
        ASSERT_TRUE(f.engine.parse("SELECT * FROM 'CND'.'EX' WHERE " + where, first).empty()) << where;
        const std::string canonical = f.engine.format(first);
        ASSERT_TRUE(f.engine.parse(canonical, again).empty()) << canonical;
        EXPECT_EQ(again.side_lo, first.side_lo) << canonical;
        EXPECT_EQ(again.side_hi, first.side_hi) << canonical;
        EXPECT_EQ(again.level_lo, first.level_lo) << canonical;
        EXPECT_EQ(again.level_hi, first.level_hi) << canonical;
    }
    ob::QueryAST ast;
    ASSERT_TRUE(f.engine.parse("SELECT * FROM 'CND'.'EX' WHERE side = 1", ast).empty());
    EXPECT_EQ(f.engine.format(ast), "SELECT * FROM 'CND'.'EX' WHERE side BETWEEN 1 AND 1")
        << "a side printed as the character it also is";
}

namespace {

enum class Op { Eq, Lt, Le, Gt, Ge, Between };

struct Condition {
    bool     on_price;
    Op       op;
    uint64_t ts_a, ts_b;   // for a timestamp condition
    int64_t  px_a, px_b;   // for a price condition
};

const char* op_text(Op op) {
    switch (op) {
        case Op::Eq: return "=";
        case Op::Lt: return "<";
        case Op::Le: return "<=";
        case Op::Gt: return ">";
        case Op::Ge: return ">=";
        case Op::Between: return "BETWEEN";
    }
    return "?";
}

template <typename T>
bool holds(T x, Op op, T a, T b) {
    switch (op) {
        case Op::Eq: return x == a;
        case Op::Lt: return x < a;
        case Op::Le: return x <= a;
        case Op::Gt: return x > a;
        case Op::Ge: return x >= a;
        case Op::Between: return a <= x && x <= b;
    }
    return false;
}

std::string text(const Condition& c) {
    const std::string col = c.on_price ? "price" : "timestamp";
    const std::string a = c.on_price ? std::to_string(c.px_a) : std::to_string(c.ts_a);
    const std::string b = c.on_price ? std::to_string(c.px_b) : std::to_string(c.ts_b);
    if (c.op == Op::Between) return col + " BETWEEN " + a + " AND " + b;
    return col + " " + op_text(c.op) + " " + a;
}

}  // namespace

RC_GTEST_PROP(QueryConditionsProps, prop_the_rows_are_those_every_condition_allows, ()) {
    // Values from a small domain, so that conditions land on rows, beside each other and between
    // them, with the ends of both types among them.
    std::vector<uint64_t> ts_values{0, kBase - 1, kU64Max};
    for (uint64_t i = 0; i <= 13; ++i) ts_values.push_back(kBase + i);
    std::vector<int64_t> px_values{kI64Min, kI64Max};
    for (int64_t p = -7; p <= 7; ++p) px_values.push_back(p);

    // Every draw at full size: RapidCheck scales a range by the run's size, so the early runs of 25
    // would draw only the first value of each list (pitfall 531). `inRange` excludes its maximum.
    const auto full = [](auto gen) { return rc::gen::resize(100, std::move(gen)); };
    const auto n_rows = *full(rc::gen::inRange<size_t>(1, 25));
    std::vector<ob::SnapshotRow> rows;
    for (size_t i = 0; i < n_rows; ++i) {
        rows.push_back(row_at(kBase + *full(rc::gen::inRange<uint64_t>(0, 14)),
                              *full(rc::gen::inRange<int64_t>(-6, 7)), kBid, static_cast<uint16_t>(i)));
    }

    const auto n_conds = *full(rc::gen::inRange<size_t>(1, 5));
    std::vector<Condition> conds;
    for (size_t i = 0; i < n_conds; ++i) {
        Condition c{};
        c.on_price = *full(rc::gen::element(false, true));
        c.op       = *full(rc::gen::element(Op::Eq, Op::Lt, Op::Le, Op::Gt, Op::Ge, Op::Between));
        c.ts_a     = *full(rc::gen::elementOf(ts_values));
        c.ts_b     = *full(rc::gen::elementOf(ts_values));
        c.px_a     = *full(rc::gen::elementOf(px_values));
        c.px_b     = *full(rc::gen::elementOf(px_values));
        conds.push_back(c);
    }

    std::string where;
    for (const auto& c : conds) where += (where.empty() ? "" : " AND ") + text(c);

    std::vector<std::pair<uint64_t, int64_t>> want;
    for (const auto& r : rows) {
        bool all = true;
        for (const auto& c : conds) {
            all = all && (c.on_price ? holds<int64_t>(r.price, c.op, c.px_a, c.px_b)
                                     : holds<uint64_t>(r.timestamp_ns, c.op, c.ts_a, c.ts_b));
        }
        if (all) want.emplace_back(r.timestamp_ns, r.price);
    }

    Fixture f;
    f.segment(rows);
    std::vector<std::pair<uint64_t, int64_t>> got;
    const std::string err = f.engine.execute("SELECT * FROM 'CND'.'EX' WHERE " + where,
                                             [&](const ob::QueryResult& r) { got.emplace_back(r.timestamp_ns, r.price); });
    RC_ASSERT(err.empty());
    std::sort(want.begin(), want.end());
    std::sort(got.begin(), got.end());
    RC_LOG() << "WHERE " << where << ": want " << want.size() << " row(s), got " << got.size() << "\n";
    RC_ASSERT(got == want);
}
