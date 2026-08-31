// Feature: orderbook-dbengine — Query Engine tests
// Tasks 10.5–10.11: property-based tests (Properties 28–33) and unit tests.

#include "orderbook/query_engine.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/aggregation.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <filesystem>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdlib>
#include <atomic>
#include <span>
#include <thread>

namespace fs = std::filesystem;

// ── Helpers ───────────────────────────────────────────────────────────────────

static std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(tmp);
    return tmp.string();
}

// Fixture: QueryEngine backed by an empty ColumnarStore.
struct QEFixture {
    std::string dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    std::unordered_map<std::string, ob::SoABuffer*> live;
    ob::QueryEngine engine;

    QEFixture()
        : dir(make_temp_dir("qe_test_"))
        , store(dir)
        , engine(store, live, agg)
    {}

    ~QEFixture() {
        store.close();
        fs::remove_all(dir);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Property 32: Query parse-format-parse round-trip
// Feature: orderbook-dbengine, Property 32: Query parse-format-parse round-trip
// Validates: Requirements 10.11, 11.4
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(QueryEngineProps, prop_query_roundtrip, ()) {
    auto symbol   = *rc::gen::element(std::string{"AAPL"}, std::string{"BTC-USD"}, std::string{"ETH-USD"});
    auto exchange = *rc::gen::element(std::string{"NYSE"}, std::string{"COINBASE"}, std::string{"BINANCE"});
    auto ts_start = *rc::gen::inRange<uint64_t>(1000000000ULL, 2000000000000000000ULL);
    auto ts_end   = ts_start + *rc::gen::inRange<uint64_t>(1, 1000000000ULL);
    auto limit    = *rc::gen::inRange<uint64_t>(1, 10000ULL);

    std::string sql =
        "SELECT timestamp, price, quantity FROM '" + symbol + "'.'" + exchange +
        "' WHERE timestamp BETWEEN " + std::to_string(ts_start) +
        " AND " + std::to_string(ts_end) +
        " LIMIT " + std::to_string(limit);

    QEFixture fix;
    ob::QueryAST ast1, ast2;

    auto err1 = fix.engine.parse(sql, ast1);
    RC_ASSERT(err1.empty());

    std::string formatted = fix.engine.format(ast1);

    auto err2 = fix.engine.parse(formatted, ast2);
    RC_ASSERT(err2.empty());

    RC_ASSERT(ast1.type     == ast2.type);
    RC_ASSERT(ast1.symbol   == ast2.symbol);
    RC_ASSERT(ast1.exchange == ast2.exchange);
    RC_ASSERT(ast1.ts_start_ns == ast2.ts_start_ns);
    RC_ASSERT(ast1.ts_end_ns   == ast2.ts_end_ns);
    RC_ASSERT(ast1.limit       == ast2.limit);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 33: Invalid query parse error with location
// Feature: orderbook-dbengine, Property 33: Invalid query parse error with location
// Validates: Requirements 11.2
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(QueryEngineProps, prop_invalid_query_error, ()) {
    auto invalid_sql = *rc::gen::element(
        std::string{"SELECT"},
        std::string{"SELECT * FROM"},
        std::string{"SELECT * FROM 'AAPL'"},
        std::string{"SELECT * FROM 'AAPL'.'NYSE' WHERE timestamp"},
        std::string{"GARBAGE QUERY HERE"},
        std::string{"SELECT * FROM 'AAPL'.'NYSE' LIMIT"}
    );

    QEFixture fix;
    ob::QueryAST ast;
    std::string err = fix.engine.parse(invalid_sql, ast);
    RC_ASSERT(!err.empty());
    RC_ASSERT(err.find("line") != std::string::npos);
    RC_ASSERT(err.find("col")  != std::string::npos);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 28: Query timestamp filter correctness
// Feature: orderbook-dbengine, Property 28: Query timestamp filter correctness
// Validates: Requirements 10.3
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(QueryEngineProps, prop_ts_filter, ()) {
    auto ts_start = *rc::gen::inRange<uint64_t>(1000ULL, 1000000000000000000ULL);
    auto ts_end   = ts_start + *rc::gen::inRange<uint64_t>(1, 1000000000ULL);

    std::string sql =
        "SELECT timestamp FROM 'SYM'.'EX' WHERE timestamp BETWEEN " +
        std::to_string(ts_start) + " AND " + std::to_string(ts_end);

    QEFixture fix;
    ob::QueryAST ast;
    RC_ASSERT(fix.engine.parse(sql, ast).empty());
    RC_ASSERT(ast.ts_start_ns.has_value());
    RC_ASSERT(ast.ts_end_ns.has_value());
    RC_ASSERT(ast.ts_start_ns.value() == ts_start);
    RC_ASSERT(ast.ts_end_ns.value()   == ts_end);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 29: Query price filter correctness
// Feature: orderbook-dbengine, Property 29: Query price filter correctness
// Validates: Requirements 10.4
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(QueryEngineProps, prop_price_filter, ()) {
    auto price_lo = *rc::gen::inRange<int64_t>(0, 1000000LL);
    auto price_hi = price_lo + *rc::gen::inRange<int64_t>(1, 100000LL);

    std::string sql =
        "SELECT price FROM 'SYM'.'EX' WHERE price BETWEEN " +
        std::to_string(price_lo) + " AND " + std::to_string(price_hi);

    QEFixture fix;
    ob::QueryAST ast;
    RC_ASSERT(fix.engine.parse(sql, ast).empty());
    RC_ASSERT(ast.price_lo.has_value());
    RC_ASSERT(ast.price_hi.has_value());
    RC_ASSERT(ast.price_lo.value() == price_lo);
    RC_ASSERT(ast.price_hi.value() == price_hi);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 30: LIMIT clause correctness
// Feature: orderbook-dbengine, Property 30: LIMIT clause correctness
// Validates: Requirements 10.6
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(QueryEngineProps, prop_limit_clause, ()) {
    auto limit = *rc::gen::inRange<uint64_t>(1, 10000ULL);

    std::string sql =
        "SELECT * FROM 'SYM'.'EX' LIMIT " + std::to_string(limit);

    QEFixture fix;
    ob::QueryAST ast;
    RC_ASSERT(fix.engine.parse(sql, ast).empty());
    RC_ASSERT(ast.limit.has_value());
    RC_ASSERT(ast.limit.value() == limit);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 31: Unknown symbol/exchange error
// Feature: orderbook-dbengine, Property 31: Unknown symbol/exchange error
// Validates: Requirements 10.10
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(QueryEngineProps, prop_unknown_symbol_error, ()) {
    auto sym = *rc::gen::element(std::string{"UNKNOWN1"}, std::string{"UNKNOWN2"}, std::string{"NOSUCHSYM"});
    auto ex  = *rc::gen::element(std::string{"NOEX1"}, std::string{"NOEX2"}, std::string{"NOSUCHEX"});

    std::string sql = "SELECT * FROM '" + sym + "'.'" + ex + "'";

    QEFixture fix;
    std::string err = fix.engine.execute(sql, [](const ob::QueryResult&){});
    RC_ASSERT(!err.empty());
    RC_ASSERT(err.find("NOT_FOUND") != std::string::npos);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests — Task 10.11
// ═══════════════════════════════════════════════════════════════════════════════

TEST(QueryEngineUnit, ParseTimeRangeQuery) {
    QEFixture fix;
    ob::QueryAST ast;
    std::string sql =
        "SELECT timestamp, price, quantity FROM 'AAPL'.'NYSE' "
        "WHERE timestamp BETWEEN 1700000000000000000 AND 1700003600000000000 LIMIT 1000";
    ASSERT_TRUE(fix.engine.parse(sql, ast).empty());
    EXPECT_EQ(ast.type,     ob::QueryType::SELECT);
    EXPECT_EQ(ast.symbol,   "AAPL");
    EXPECT_EQ(ast.exchange, "NYSE");
    ASSERT_TRUE(ast.ts_start_ns.has_value());
    ASSERT_TRUE(ast.ts_end_ns.has_value());
    EXPECT_EQ(ast.ts_start_ns.value(), 1700000000000000000ULL);
    EXPECT_EQ(ast.ts_end_ns.value(),   1700003600000000000ULL);
    ASSERT_TRUE(ast.limit.has_value());
    EXPECT_EQ(ast.limit.value(), 1000ULL);
    EXPECT_EQ(ast.select_exprs.size(), 3u);
}

TEST(QueryEngineUnit, ParseAggregationQuery) {
    QEFixture fix;
    ob::QueryAST ast;
    std::string sql =
        "SELECT VWAP(price), SPREAD(*), IMBALANCE(10) FROM 'BTC-USD'.'COINBASE' "
        "WHERE timestamp >= 1700000000000000000";
    ASSERT_TRUE(fix.engine.parse(sql, ast).empty());
    EXPECT_EQ(ast.symbol,   "BTC-USD");
    EXPECT_EQ(ast.exchange, "COINBASE");
    ASSERT_EQ(ast.select_exprs.size(), 3u);
    EXPECT_EQ(ast.select_exprs[0], "VWAP(price)");
    EXPECT_EQ(ast.select_exprs[1], "SPREAD(*)");
    EXPECT_EQ(ast.select_exprs[2], "IMBALANCE(10)");
    ASSERT_TRUE(ast.ts_start_ns.has_value());
    EXPECT_EQ(ast.ts_start_ns.value(), 1700000000000000000ULL);
}

TEST(QueryEngineUnit, ParseSnapshotQuery) {
    QEFixture fix;
    ob::QueryAST ast;
    std::string sql = "SELECT * FROM 'AAPL'.'NYSE' WHERE AT 1700001234567890123";
    ASSERT_TRUE(fix.engine.parse(sql, ast).empty());
    EXPECT_EQ(ast.type, ob::QueryType::SNAPSHOT);
    ASSERT_TRUE(ast.snapshot_ts_ns.has_value());
    EXPECT_EQ(ast.snapshot_ts_ns.value(), 1700001234567890123ULL);
}

TEST(QueryEngineUnit, ParseSubscribeQuery) {
    QEFixture fix;
    ob::QueryAST ast;
    std::string sql =
        "SUBSCRIBE timestamp, price, quantity FROM 'AAPL'.'NYSE' "
        "WHERE price BETWEEN 15000 AND 16000";
    ASSERT_TRUE(fix.engine.parse(sql, ast).empty());
    EXPECT_EQ(ast.type, ob::QueryType::SUBSCRIBE);
    ASSERT_TRUE(ast.price_lo.has_value());
    ASSERT_TRUE(ast.price_hi.has_value());
    EXPECT_EQ(ast.price_lo.value(), 15000LL);
    EXPECT_EQ(ast.price_hi.value(), 16000LL);
}

TEST(QueryEngineUnit, ParseErrorHasLineCol) {
    QEFixture fix;
    ob::QueryAST ast;
    std::string err = fix.engine.parse("SELECT * FROM", ast);
    ASSERT_FALSE(err.empty());
    EXPECT_NE(err.find("line"), std::string::npos);
    EXPECT_NE(err.find("col"),  std::string::npos);
}

TEST(QueryEngineUnit, LimitZeroParsesOk) {
    QEFixture fix;
    ob::QueryAST ast;
    std::string err = fix.engine.parse("SELECT * FROM 'SYM'.'EX' LIMIT 0", ast);
    ASSERT_TRUE(err.empty());
    ASSERT_TRUE(ast.limit.has_value());
    EXPECT_EQ(ast.limit.value(), 0ULL);
}

TEST(QueryEngineUnit, ExecuteUnknownSymbolReturnsError) {
    QEFixture fix;
    std::string err = fix.engine.execute(
        "SELECT * FROM 'NOSYM'.'NOEX'",
        [](const ob::QueryResult&){}
    );
    ASSERT_FALSE(err.empty());
    EXPECT_NE(err.find("NOT_FOUND"), std::string::npos);
}

TEST(QueryEngineUnit, ExecuteSubscribeReturnsError) {
    QEFixture fix;
    std::string err = fix.engine.execute(
        "SUBSCRIBE * FROM 'SYM'.'EX'",
        [](const ob::QueryResult&){}
    );
    ASSERT_FALSE(err.empty());
}

TEST(QueryEngineUnit, SubscribeCallbackInvokedOnMatch) {
    QEFixture fix;
    int called = 0;

    uint64_t sub_id = fix.engine.subscribe(
        "SUBSCRIBE price FROM 'AAPL'.'NYSE' WHERE price BETWEEN 10000 AND 20000",
        [&](const ob::QueryResult& r) {
            ++called;
            EXPECT_GE(r.price, 10000LL);
            EXPECT_LE(r.price, 20000LL);
        }
    );
    ASSERT_NE(sub_id, 0u);

    ob::SnapshotRow row{};
    row.timestamp_ns    = 1000000000ULL;
    row.sequence_number = 1;
    row.side            = 0;
    row.level_index     = 0;
    row.price           = 15000;
    row.quantity        = 100;
    row.order_count     = 1;

    fix.engine.notify_subscribers("AAPL", "NYSE", std::span<const ob::SnapshotRow>{&row, 1});
    EXPECT_EQ(called, 1);

    // Row outside price range — should not trigger callback
    row.price = 25000;
    fix.engine.notify_subscribers("AAPL", "NYSE", std::span<const ob::SnapshotRow>{&row, 1});
    EXPECT_EQ(called, 1);

    // Unsubscribe — no more callbacks
    fix.engine.unsubscribe(sub_id);
    row.price = 15000;
    fix.engine.notify_subscribers("AAPL", "NYSE", std::span<const ob::SnapshotRow>{&row, 1});
    EXPECT_EQ(called, 1);
}

// ── The subscription list is shared across threads, and was not synchronised ──────────────────
//
// `notify_subscribers()` is called from the write path, and the write path is not one thread:
//
//   Engine::apply_delta        -> the server's epoll loop (a client sent INSERT/MINSERT)
//   Engine::apply_delta_mm     -> the same loop
//   Engine::apply_remote_delta -> MultiMasterManager::io_loop (multi_master.cpp:499)
//
// `subscriptions_` was a plain std::vector with no lock: subscribe() push_back'd, unsubscribe()
// erased, notify_subscribers() iterated. Nothing made those safe together.
//
// The race was latent rather than absent. The only callers of ob_subscribe() are single-threaded
// tests, so nobody had two threads on this list — but an embedded consumer on a multi-master node
// that subscribes and then receives a peer's delta has it immediately, and exposing SUBSCRIBE over
// the wire (#45) makes it certain, because registration then arrives on the epoll loop while
// notification can arrive from io_loop.
//
// This test was written expecting to need TSan to show anything, on the reasoning that a data race
// has no deterministic observable behaviour. The measurement disagreed and the note is worth
// keeping: on the unfixed tree it aborts on **every** run, in a plain Debug build, with
// `std::bad_function_call`. The notifier holds a reference into the vector, `push_back` reallocates
// and moves the element, and the callback it then invokes is a moved-from `std::function`. So the
// gate is ordinary `ctest`, not the sanitizer - which matters, because a test that only fails under
// a sanitizer only runs in the jobs that build one.
//
// The assertion below is still weak, and that is now the only honest form: what is being proven is
// that nothing aborts, and an abort is not something an EXPECT can see.
TEST(QueryEngineUnit, SubscribingWhileNotifyingIsSafeAcrossThreads) {
    QEFixture fix;
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> notifications{0};

    ob::SnapshotRow row{};
    row.timestamp_ns    = 1000000000ULL;
    row.sequence_number = 1;
    row.price           = 15000;
    row.quantity        = 100;
    row.order_count     = 1;

    // The notifier: what the write path does, as fast as it can.
    std::thread notifier([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            fix.engine.notify_subscribers("AAPL", "NYSE", std::span<const ob::SnapshotRow>{&row, 1});
            notifications.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // The registrar: what a session does when SUBSCRIBE arrives and when it disconnects. Enough
    // churn to force the vector to reallocate repeatedly, which is what moves the element a
    // concurrent iteration is holding.
    std::vector<uint64_t> ids;
    for (int round = 0; round < 400; ++round) {
        uint64_t id = fix.engine.subscribe(
            "SUBSCRIBE price FROM 'AAPL'.'NYSE'",
            [](const ob::QueryResult&) {}
        );
        ASSERT_NE(id, 0u);
        ids.push_back(id);
        if (ids.size() > 8) {
            fix.engine.unsubscribe(ids.front());
            ids.erase(ids.begin());
        }
    }

    stop.store(true, std::memory_order_relaxed);
    notifier.join();

    for (uint64_t id : ids) fix.engine.unsubscribe(id);

    // Weak on purpose, per the comment above: what is being proven is the absence of a report from
    // the sanitizer, not a value. This only asserts the threads actually ran, because a test whose
    // notifier never got scheduled proves nothing and would pass forever.
    EXPECT_GT(notifications.load(), 0u);
}

TEST(QueryEngineUnit, FormatIsReparseable) {
    QEFixture fix;
    std::string original =
        "SELECT timestamp, price FROM 'AAPL'.'NYSE' "
        "WHERE timestamp BETWEEN 1000000 AND 2000000 LIMIT 100";
    ob::QueryAST ast1;
    ASSERT_TRUE(fix.engine.parse(original, ast1).empty());

    std::string formatted = fix.engine.format(ast1);
    ob::QueryAST ast2;
    ASSERT_TRUE(fix.engine.parse(formatted, ast2).empty());

    EXPECT_EQ(ast1.type,       ast2.type);
    EXPECT_EQ(ast1.symbol,     ast2.symbol);
    EXPECT_EQ(ast1.exchange,   ast2.exchange);
    EXPECT_EQ(ast1.ts_start_ns, ast2.ts_start_ns);
    EXPECT_EQ(ast1.ts_end_ns,   ast2.ts_end_ns);
    EXPECT_EQ(ast1.limit,       ast2.limit);
}
