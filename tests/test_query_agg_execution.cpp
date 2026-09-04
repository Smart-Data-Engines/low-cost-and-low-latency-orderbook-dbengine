// Feature: aggregations-over-wire — aggregate results must survive execute()
//
// The gap this file fills: test_aggregation.cpp checks AggregationEngine's maths on
// hand-built SoASide inputs, and test_query_engine.cpp checks that the parser fills
// ast.select_exprs. Neither ever ran an aggregate through QueryEngine::execute() and
// looked at the value that came out, which is how the result reached clients as a
// row of zeros for as long as the feature existed.
//
// Every expected value below is computed by hand in the test, never copied from the
// code's output.

#include "orderbook/query_engine.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/aggregation.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace {

static std::atomic<uint64_t> g_dir_counter{0};

std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() /
               (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
    fs::create_directories(tmp);
    return tmp.string();
}

/// A QueryEngine with one live book, so aggregates have something to read.
/// Aggregates run over the live SoA buffers, not over stored segments.
struct AggFixture {
    std::string dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    ob::SoABuffer buffer{};
    std::unordered_map<std::string, ob::SoABuffer*> live;
    ob::QueryEngine engine;

    AggFixture()
        : dir(make_temp_dir("qagg_test_"))
        , store(dir)
        , engine(store,
                 // A lookup rather than the map: QueryEngine no longer holds a reference to a
                 // live-buffer map, because reading it while a write path inserted into it was a
                 // data race (#91). One thread here, so no lock is needed.
                 [this](const std::string& key) -> ob::SoABuffer* {
                     auto it = live.find(key);
                     return (it == live.end()) ? nullptr : it->second;
                 },
                 agg) {
        std::strncpy(buffer.symbol, "BTC-USD", sizeof(buffer.symbol) - 1);
        std::strncpy(buffer.exchange, "BINANCE", sizeof(buffer.exchange) - 1);
        buffer.sequence_number.store(7, std::memory_order_relaxed);
        buffer.last_timestamp_ns = 1'700'000'000'000'000'000ULL;
        live["BTC-USD.BINANCE"] = &buffer;
    }

    ~AggFixture() {
        store.close();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    void add_bid(int64_t price, uint64_t qty, uint32_t cnt = 1) {
        ASSERT_EQ(ob::insert_level(buffer.bid, price, qty, cnt, /*descending=*/true), ob::OB_OK);
    }
    void add_ask(int64_t price, uint64_t qty, uint32_t cnt = 1) {
        ASSERT_EQ(ob::insert_level(buffer.ask, price, qty, cnt, /*descending=*/false), ob::OB_OK);
    }

    /// Run a query and return the single QueryResult an aggregate query produces.
    ob::QueryResult run(const std::string& sql, std::string* error = nullptr) {
        std::vector<ob::QueryResult> results;
        std::string err = engine.execute(sql, [&](const ob::QueryResult& r) {
            results.push_back(r);
        });
        if (error) *error = err;
        if (!err.empty() || results.empty()) return {};
        return results.front();
    }
};

/// Look up one aggregate by the expression that produced it.
const ob::AggValue* find_agg(const ob::QueryResult& r, const std::string& name) {
    for (const auto& v : r.agg_values) {
        if (v.name == name) return &v;
    }
    return nullptr;
}

} // anonymous namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Values must arrive, and arrive correct.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(QueryAggExecution, SpreadReachesTheCaller) {
    AggFixture fix;
    fix.add_bid(100'000, 50);
    fix.add_ask(101'000, 30);

    std::string err;
    auto result = fix.run("SELECT SPREAD(*) FROM 'BTC-USD'.'BINANCE'", &err);
    ASSERT_TRUE(err.empty()) << err;

    const auto* spread = find_agg(result, "SPREAD(*)");
    ASSERT_NE(spread, nullptr) << "no aggregate named SPREAD(*) came back from execute()";
    EXPECT_FALSE(spread->empty);
    EXPECT_EQ(spread->value, 1'000) << "best ask 101000 minus best bid 100000";
    EXPECT_EQ(spread->scale, 1) << "spread is in raw price sub-units";
}

TEST(QueryAggExecution, MidPriceCarriesItsScale) {
    AggFixture fix;
    fix.add_bid(100'000, 50);
    fix.add_ask(101'000, 30);

    auto result = fix.run("SELECT MID_PRICE(*) FROM 'BTC-USD'.'BINANCE'");
    const auto* mid = find_agg(result, "MID_PRICE(*)");
    ASSERT_NE(mid, nullptr);

    // (100000 + 101000) / 2 = 100500, scaled by 10^6 by mid_price().
    EXPECT_EQ(mid->value, 100'500 * 1'000'000LL);
    EXPECT_EQ(mid->scale, 1'000'000)
        << "without the scale a client reads mid-price a million times too large";
    EXPECT_EQ(mid->value / mid->scale, 100'500);
}

TEST(QueryAggExecution, ImbalanceCarriesItsScale) {
    AggFixture fix;
    fix.add_bid(100'000, 60);
    fix.add_ask(101'000, 40);

    auto result = fix.run("SELECT IMBALANCE(10) FROM 'BTC-USD'.'BINANCE'");
    const auto* imb = find_agg(result, "IMBALANCE(10)");
    ASSERT_NE(imb, nullptr);

    // (60 - 40) * 10^9 / (60 + 40) = 200000000
    EXPECT_EQ(imb->value, 200'000'000LL);
    EXPECT_EQ(imb->scale, 1'000'000'000);
}

TEST(QueryAggExecution, VwapCarriesItsScale) {
    AggFixture fix;
    fix.add_bid(100'000, 10);
    fix.add_bid(99'000, 30);

    auto result = fix.run("SELECT VWAP(price) FROM 'BTC-USD'.'BINANCE'");
    const auto* vwap = find_agg(result, "VWAP(price)");
    ASSERT_NE(vwap, nullptr);

    // (100000*10 + 99000*30) / 40 = 3970000/40 = 99250, scaled by 10^6.
    EXPECT_EQ(vwap->value, 99'250 * 1'000'000LL);
    EXPECT_EQ(vwap->scale, 1'000'000);
}

TEST(QueryAggExecution, SeveralAggregatesComeBackInQueryOrder) {
    AggFixture fix;
    fix.add_bid(100'000, 50);
    fix.add_ask(101'000, 30);

    auto result = fix.run(
        "SELECT SPREAD(*), MID_PRICE(*), IMBALANCE(10) FROM 'BTC-USD'.'BINANCE'");

    ASSERT_EQ(result.agg_values.size(), 3u)
        << "a query asking for three aggregates must return three values";
    EXPECT_EQ(result.agg_values[0].name, "SPREAD(*)");
    EXPECT_EQ(result.agg_values[1].name, "MID_PRICE(*)");
    EXPECT_EQ(result.agg_values[2].name, "IMBALANCE(10)");
}

TEST(QueryAggExecution, SameFunctionTwiceWithDifferentArgumentsStaysDistinct) {
    AggFixture fix;
    fix.add_bid(100'000, 60);
    fix.add_ask(101'000, 40);

    auto result = fix.run(
        "SELECT IMBALANCE(1), IMBALANCE(10) FROM 'BTC-USD'.'BINANCE'");

    ASSERT_EQ(result.agg_values.size(), 2u);
    EXPECT_EQ(result.agg_values[0].name, "IMBALANCE(1)");
    EXPECT_EQ(result.agg_values[1].name, "IMBALANCE(10)");
}

TEST(QueryAggExecution, EmptyBookIsReportedAsEmptyNotAsZero) {
    AggFixture fix;
    fix.add_bid(100'000, 50);
    // No ask side at all, so spread and mid-price have nothing to work with.

    auto result = fix.run("SELECT SPREAD(*), MID_PRICE(*) FROM 'BTC-USD'.'BINANCE'");

    const auto* spread = find_agg(result, "SPREAD(*)");
    ASSERT_NE(spread, nullptr);
    EXPECT_TRUE(spread->empty)
        << "a spread with no ask side is not a spread of zero, and a trading client "
           "cannot tell the difference unless the flag survives";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Input the engine cannot honour must be refused, not quietly dropped.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(QueryAggExecution, MixingAggregatesWithColumnsIsRefused) {
    AggFixture fix;
    fix.add_bid(100'000, 50);
    fix.add_ask(101'000, 30);

    std::string err;
    fix.run("SELECT price, SPREAD(*) FROM 'BTC-USD'.'BINANCE'", &err);

    ASSERT_FALSE(err.empty())
        << "'price' was silently dropped: no GROUP BY semantics exist for this query";
    EXPECT_NE(err.find("AGG_WITH_COLUMNS"), std::string::npos) << err;
}

TEST(QueryAggExecution, TimestampFilterWithAnAggregateIsRefused) {
    AggFixture fix;
    fix.add_bid(100'000, 50);
    fix.add_ask(101'000, 30);

    std::string err;
    fix.run("SELECT SPREAD(*) FROM 'BTC-USD'.'BINANCE' "
            "WHERE timestamp BETWEEN 0 AND 9999999999999999999", &err);

    ASSERT_FALSE(err.empty())
        << "aggregates read the live book, so a timestamp range cannot be honoured "
           "and must not be ignored in silence";
    EXPECT_NE(err.find("AGG_TIME_FILTER"), std::string::npos) << err;
}

// ═══════════════════════════════════════════════════════════════════════════════
// The argument has to name what the function actually aggregates.
//
// The dispatcher calls sum_qty() for SUM and avg_price() for AVG regardless of the
// argument, so SUM(price) returned a sum of quantities labelled SUM(price), and
// AVG(quantity) returned an average price. The argument was parsed, echoed back in
// the result name, and thrown away.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(QueryAggArguments, SumRejectsAColumnItDoesNotSum) {
    AggFixture fix;
    fix.add_bid(100'000, 50);

    std::string err;
    fix.run("SELECT SUM(price) FROM 'BTC-USD'.'BINANCE'", &err);

    ASSERT_FALSE(err.empty())
        << "SUM(price) returned a quantity under the name SUM(price)";
    EXPECT_NE(err.find("AGG_BAD_ARGUMENT"), std::string::npos) << err;
}

TEST(QueryAggArguments, PriceFunctionsRejectAQuantityArgument) {
    AggFixture fix;
    fix.add_bid(100'000, 50);

    for (const char* sql : {"SELECT AVG(quantity) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT MIN(quantity) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT MAX(quantity) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT VWAP(quantity) FROM 'BTC-USD'.'BINANCE'"}) {
        std::string err;
        fix.run(sql, &err);
        EXPECT_FALSE(err.empty()) << sql << " was accepted and the argument ignored";
        EXPECT_NE(err.find("AGG_BAD_ARGUMENT"), std::string::npos) << sql << ": " << err;
    }
}

TEST(QueryAggArguments, TwoSidedFunctionsTakeNoColumn) {
    AggFixture fix;
    fix.add_bid(100'000, 50);
    fix.add_ask(101'000, 30);

    for (const char* sql : {"SELECT SPREAD(price) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT MID_PRICE(quantity) FROM 'BTC-USD'.'BINANCE'"}) {
        std::string err;
        fix.run(sql, &err);
        EXPECT_FALSE(err.empty()) << sql << " ignored its argument";
        EXPECT_NE(err.find("AGG_BAD_ARGUMENT"), std::string::npos) << sql << ": " << err;
    }
}

TEST(QueryAggArguments, ValidFormsStillWork) {
    AggFixture fix;
    fix.add_bid(100'000, 50);
    fix.add_ask(101'000, 30);

    for (const char* sql : {"SELECT SUM(quantity) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT SUM(*) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT AVG(price) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT VWAP(*) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT SPREAD(*) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT CUMULATIVE_VOLUME(5) FROM 'BTC-USD'.'BINANCE'",
                            "SELECT DEPTH(100000) FROM 'BTC-USD'.'BINANCE'"}) {
        std::string err;
        auto result = fix.run(sql, &err);
        EXPECT_TRUE(err.empty()) << sql << ": " << err;
        EXPECT_EQ(result.agg_values.size(), 1u) << sql;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DEPTH_RANGE could only ever answer NULL.
//
// The parser rebuilds the expression text with ", " between arguments, so the
// second bound always arrived as " 101000". std::from_chars refuses a leading
// space, parse_i64() turned that failure into 0, and [lo, 0] is an empty range.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(QueryAggArguments, DepthRangeFindsLevelsInsideTheRange) {
    AggFixture fix;
    fix.add_bid(100'000, 50);
    fix.add_bid(99'000, 20);
    fix.add_bid(90'000, 7);   // outside the range below

    std::string err;
    auto result = fix.run(
        "SELECT DEPTH_RANGE(99000, 101000) FROM 'BTC-USD'.'BINANCE'", &err);
    ASSERT_TRUE(err.empty()) << err;

    ASSERT_EQ(result.agg_values.size(), 1u);
    const auto& v = result.agg_values[0];
    EXPECT_FALSE(v.empty) << "DEPTH_RANGE answered NULL for a range containing two levels";
    EXPECT_EQ(v.value, 70) << "50 at 100000 plus 20 at 99000, and not the 7 at 90000";
}

TEST(QueryAggArguments, DepthRangeIgnoresTheSpaceTheParserInserts) {
    AggFixture fix;
    fix.add_bid(100'000, 50);

    // Written without a space; the parser reconstructs it with one either way.
    std::string err;
    auto result = fix.run(
        "SELECT DEPTH_RANGE(99000,101000) FROM 'BTC-USD'.'BINANCE'", &err);
    ASSERT_TRUE(err.empty()) << err;
    ASSERT_EQ(result.agg_values.size(), 1u);
    EXPECT_EQ(result.agg_values[0].value, 50);
}

TEST(QueryAggArguments, DepthRangeOutsideAnyLevelIsEmptyNotZero) {
    AggFixture fix;
    fix.add_bid(100'000, 50);

    auto result = fix.run("SELECT DEPTH_RANGE(1, 2) FROM 'BTC-USD'.'BINANCE'");
    ASSERT_EQ(result.agg_values.size(), 1u);
    EXPECT_TRUE(result.agg_values[0].empty)
        << "no levels in range is an empty result, not a depth of zero";
}
