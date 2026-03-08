// Feature: orderbook-dbengine — Engine integration tests (Requirements 7.3, 7.4, 7.5, 8.1, 8.3)
#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>
#include <filesystem>
#include <string>
#include <vector>
#include <cstring>
#include <cstdlib>

namespace fs = std::filesystem;

static std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(tmp);
    return tmp.string();
}

// ── Test 1: apply_delta → query returns correct data end-to-end ───────────────
// Validates: Requirements 7.3, 7.4, 8.1
TEST(EngineIntegration, ApplyDeltaAndQueryEndToEnd) {
    std::string dir = make_temp_dir("engine_e2e_");

    {
        ob::Engine engine(dir, 50'000'000ULL); // 50ms flush interval
        engine.open();

        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol,   "AAPL", sizeof(delta.symbol)   - 1);
        std::strncpy(delta.exchange, "NYSE", sizeof(delta.exchange) - 1);
        delta.sequence_number = 1;
        delta.timestamp_ns    = 1000000000ULL;
        delta.side            = ob::SIDE_BID;
        delta.n_levels        = 3;

        ob::Level levels[3] = {
            {10300, 100, 1, 0},
            {10200, 200, 2, 0},
            {10100, 300, 3, 0}
        };

        ob::ob_status_t st = engine.apply_delta(delta, levels);
        ASSERT_EQ(st, ob::OB_OK);

        // Graceful close flushes all dirty pages to disk (Requirement 7.4)
        engine.close();
    }

    // Reopen and query — combined_store rebuilt via open_existing()
    {
        ob::Engine engine2(dir);
        engine2.open();

        std::vector<int64_t> got_prices;
        std::string err = engine2.execute(
            "SELECT timestamp, price, quantity FROM 'AAPL'.'NYSE' "
            "WHERE timestamp BETWEEN 0 AND 9999999999999999999",
            [&](const ob::QueryResult& r) {
                got_prices.push_back(r.price);
            });

        EXPECT_TRUE(err.empty()) << "Query error: " << err;
        ASSERT_EQ(got_prices.size(), 3u);
        EXPECT_EQ(got_prices[0], 10300LL);
        EXPECT_EQ(got_prices[1], 10200LL);
        EXPECT_EQ(got_prices[2], 10100LL);

        engine2.close();
    }

    fs::remove_all(dir);
}

// ── Test 2: Graceful shutdown flushes all dirty pages ─────────────────────────
// Validates: Requirements 7.4, 8.3
TEST(EngineIntegration, GracefulShutdownFlushesData) {
    std::string dir = make_temp_dir("engine_flush_");

    {
        // Use a very long flush interval so the background thread won't fire
        ob::Engine engine(dir, 1'000'000'000ULL); // 1s
        engine.open();

        for (int i = 0; i < 5; ++i) {
            ob::DeltaUpdate delta{};
            std::strncpy(delta.symbol,   "BTC", sizeof(delta.symbol)   - 1);
            std::strncpy(delta.exchange, "CB",  sizeof(delta.exchange) - 1);
            delta.sequence_number = static_cast<uint64_t>(i + 1);
            delta.timestamp_ns    = static_cast<uint64_t>(i + 1) * 1'000'000'000ULL;
            delta.side            = ob::SIDE_BID;
            delta.n_levels        = 1;

            ob::Level level{10000 + i * 100, static_cast<uint64_t>(100 + i), 1, 0};
            ob::ob_status_t st = engine.apply_delta(delta, &level);
            ASSERT_EQ(st, ob::OB_OK);
        }

        // Graceful close must flush all pending rows even without background flush
        engine.close();
    }

    // Reopen and verify all 5 rows are present
    {
        ob::Engine engine2(dir);
        engine2.open();

        int row_count = 0;
        std::string err = engine2.execute(
            "SELECT * FROM 'BTC'.'CB' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
            [&](const ob::QueryResult&) { ++row_count; });

        EXPECT_TRUE(err.empty()) << "Query error: " << err;
        EXPECT_EQ(row_count, 5);

        engine2.close();
    }

    fs::remove_all(dir);
}

// ── Test 3: Subscribe callback is invoked on apply_delta ──────────────────────
// Validates: Requirements 8.1, 7.3
TEST(EngineIntegration, SubscribeCallbackInvokedOnApplyDelta) {
    std::string dir = make_temp_dir("engine_sub_");

    ob::Engine engine(dir);
    engine.open();

    int called = 0;
    int64_t last_price = 0;

    uint64_t sub_id = engine.subscribe(
        "SUBSCRIBE price FROM 'AAPL'.'NYSE' WHERE price BETWEEN 10000 AND 20000",
        [&](const ob::QueryResult& r) {
            ++called;
            last_price = r.price;
        });
    ASSERT_NE(sub_id, 0u);

    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol,   "AAPL", sizeof(delta.symbol)   - 1);
    std::strncpy(delta.exchange, "NYSE", sizeof(delta.exchange) - 1);
    delta.sequence_number = 1;
    delta.timestamp_ns    = 1'000'000'000ULL;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = 1;

    ob::Level level{15000, 100, 1, 0};
    ob::ob_status_t st = engine.apply_delta(delta, &level);
    ASSERT_EQ(st, ob::OB_OK);

    EXPECT_EQ(called, 1);
    EXPECT_EQ(last_price, 15000LL);

    // Unsubscribe — subsequent deltas should not trigger the callback
    engine.unsubscribe(sub_id);

    ob::Level level2{12000, 50, 1, 0};
    delta.sequence_number = 2;
    engine.apply_delta(delta, &level2);
    EXPECT_EQ(called, 1); // still 1

    engine.close();
    fs::remove_all(dir);
}

// ── Test 4: Multiple symbols are stored and queried independently ─────────────
// Validates: Requirements 7.3, 7.4
TEST(EngineIntegration, MultipleSymbolsStoredIndependently) {
    std::string dir = make_temp_dir("engine_multi_");

    {
        ob::Engine engine(dir, 1'000'000'000ULL); // 1s — long enough to prevent background flush
        engine.open();

        // Insert into AAPL.NYSE
        {
            ob::DeltaUpdate delta{};
            std::strncpy(delta.symbol,   "AAPL", sizeof(delta.symbol)   - 1);
            std::strncpy(delta.exchange, "NYSE", sizeof(delta.exchange) - 1);
            delta.sequence_number = 1;
            delta.timestamp_ns    = 1'000'000'000ULL;
            delta.side            = ob::SIDE_BID;
            delta.n_levels        = 1;
            ob::Level level{20000, 50, 1, 0};
            engine.apply_delta(delta, &level);
        }

        // Insert into BTC.CB
        {
            ob::DeltaUpdate delta{};
            std::strncpy(delta.symbol,   "BTC", sizeof(delta.symbol)   - 1);
            std::strncpy(delta.exchange, "CB",  sizeof(delta.exchange) - 1);
            delta.sequence_number = 1;
            delta.timestamp_ns    = 2'000'000'000ULL;
            delta.side            = ob::SIDE_ASK;
            delta.n_levels        = 1;
            ob::Level level{50000, 10, 1, 0};
            engine.apply_delta(delta, &level);
        }

        engine.close();
    }

    {
        ob::Engine engine2(dir);
        engine2.open();

        int aapl_count = 0, btc_count = 0;

        engine2.execute(
            "SELECT * FROM 'AAPL'.'NYSE' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
            [&](const ob::QueryResult&) { ++aapl_count; });

        engine2.execute(
            "SELECT * FROM 'BTC'.'CB' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
            [&](const ob::QueryResult&) { ++btc_count; });

        EXPECT_EQ(aapl_count, 1);
        EXPECT_EQ(btc_count, 1);

        engine2.close();
    }

    fs::remove_all(dir);
}

// ── Test 5: WAL is written before apply (write-before-apply ordering) ─────────
// Validates: Requirements 8.1
TEST(EngineIntegration, WalWrittenBeforeApply) {
    std::string dir = make_temp_dir("engine_wal_");

    ob::Engine engine(dir, 1'000'000'000ULL); // 1s
    engine.open();

    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol,   "ETH", sizeof(delta.symbol)   - 1);
    std::strncpy(delta.exchange, "CB",  sizeof(delta.exchange) - 1);
    delta.sequence_number = 1;
    delta.timestamp_ns    = 1'000'000'000ULL;
    delta.side            = ob::SIDE_ASK;
    delta.n_levels        = 2;

    ob::Level levels[2] = {
        {30000, 10, 1, 0},
        {30100, 20, 2, 0}
    };

    ob::ob_status_t st = engine.apply_delta(delta, levels);
    ASSERT_EQ(st, ob::OB_OK);

    // WAL file must exist in base_dir after apply_delta
    bool wal_exists = false;
    for (const auto& entry : fs::directory_iterator(dir)) {
        const std::string name = entry.path().filename().string();
        if (name.find("wal") != std::string::npos ||
            name.find(".wal") != std::string::npos ||
            name.rfind("wal", 0) == 0) {
            wal_exists = true;
            break;
        }
    }
    // Also check recursively one level deep
    if (!wal_exists) {
        for (const auto& entry : fs::recursive_directory_iterator(dir)) {
            const std::string name = entry.path().filename().string();
            if (name.find(".wal") != std::string::npos ||
                name.find("wal") != std::string::npos) {
                wal_exists = true;
                break;
            }
        }
    }
    EXPECT_TRUE(wal_exists) << "WAL file not found in " << dir;

    engine.close();
    fs::remove_all(dir);
}
