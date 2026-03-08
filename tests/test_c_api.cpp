// Feature: orderbook-dbengine — C API unit tests (Requirements 13.5)
#include "orderbook/c_api.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <string>
#include <vector>
#include <cstring>
#include <atomic>
#include <chrono>

namespace fs = std::filesystem;

// Helper: create a temp dir
static std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(tmp);
    return tmp.string();
}

// Fixture
class CApiTest : public ::testing::Test {
protected:
    std::string dir;
    ob_engine_t* engine = nullptr;

    void SetUp() override {
        dir = make_temp_dir("capi_test_");
        engine = ob_engine_create(dir.c_str());
        ASSERT_NE(engine, nullptr);
    }

    void TearDown() override {
        ob_engine_destroy(engine);
        fs::remove_all(dir);
    }
};

// ── Test 1: ob_apply_delta + ob_query round-trip ──────────────────────────────

TEST_F(CApiTest, ApplyDeltaAndQueryRoundTrip) {
    // Apply a delta with 3 bid levels
    int64_t  prices[] = {10300, 10200, 10100};
    uint64_t qtys[]   = {100, 200, 300};
    uint32_t cnts[]   = {1, 2, 3};

    ob_status_t st = ob_apply_delta(engine, "AAPL", "NYSE",
                                     1, 1000000000ULL,
                                     prices, qtys, cnts, 3, 0 /* bid */);
    ASSERT_EQ(st, OB_C_OK);

    // Flush to disk by destroying and recreating the engine (destructor flushes
    // the active segment; open_existing rebuilds the index on next create).
    ob_engine_destroy(engine);
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);

    // Query back the data
    ob_result_t* result = ob_query(engine,
        "SELECT timestamp, price, quantity FROM 'AAPL'.'NYSE' "
        "WHERE timestamp BETWEEN 0 AND 9999999999999999999");
    ASSERT_NE(result, nullptr);

    // Collect rows
    std::vector<int64_t> got_prices;
    uint64_t ts; int64_t price; uint64_t qty; uint32_t cnt; uint8_t side; uint16_t level;
    while (ob_result_next(result, &ts, &price, &qty, &cnt, &side, &level) == OB_C_OK) {
        got_prices.push_back(price);
    }
    ob_result_free(result);

    ASSERT_EQ(got_prices.size(), 3u);
    // Prices should match what we inserted
    EXPECT_EQ(got_prices[0], 10300LL);
    EXPECT_EQ(got_prices[1], 10200LL);
    EXPECT_EQ(got_prices[2], 10100LL);
}

// ── Test 2: ob_subscribe callback receives JSON row ───────────────────────────

TEST_F(CApiTest, SubscribeCallbackReceivesJsonRow) {
    std::atomic<int> call_count{0};
    std::string last_json;

    // Subscribe to price range
    auto callback = [](const char* json_row, void* userdata) {
        auto* data = static_cast<std::pair<std::atomic<int>*, std::string*>*>(userdata);
        data->first->fetch_add(1);
        *data->second = json_row;
    };

    std::pair<std::atomic<int>*, std::string*> userdata{&call_count, &last_json};

    uint64_t sub_id = ob_subscribe(engine,
        "SUBSCRIBE price FROM 'AAPL'.'NYSE' WHERE price BETWEEN 10000 AND 20000",
        callback, &userdata);
    ASSERT_NE(sub_id, 0u);

    // Apply a delta that matches the subscription
    int64_t  prices[] = {15000};
    uint64_t qtys[]   = {100};
    uint32_t cnts[]   = {1};
    ob_apply_delta(engine, "AAPL", "NYSE", 1, 1000000000ULL, prices, qtys, cnts, 1, 0);

    EXPECT_EQ(call_count.load(), 1);
    EXPECT_FALSE(last_json.empty());
    // JSON should contain price field
    EXPECT_NE(last_json.find("price"), std::string::npos);
    EXPECT_NE(last_json.find("15000"), std::string::npos);

    // Unsubscribe
    ob_unsubscribe(engine, sub_id);

    // Apply another delta — should not trigger callback
    ob_apply_delta(engine, "AAPL", "NYSE", 2, 2000000000ULL, prices, qtys, cnts, 1, 0);
    EXPECT_EQ(call_count.load(), 1);
}

// ── Test 3: Error codes for invalid inputs ────────────────────────────────────

TEST_F(CApiTest, NullEngineReturnsError) {
    int64_t  prices[] = {10000};
    uint64_t qtys[]   = {100};
    uint32_t cnts[]   = {1};
    ob_status_t st = ob_apply_delta(nullptr, "SYM", "EX", 1, 1000ULL, prices, qtys, cnts, 1, 0);
    EXPECT_EQ(st, OB_C_ERR_INVALID_ARG);
}

TEST_F(CApiTest, NullSymbolReturnsError) {
    int64_t  prices[] = {10000};
    uint64_t qtys[]   = {100};
    uint32_t cnts[]   = {1};
    ob_status_t st = ob_apply_delta(engine, nullptr, "EX", 1, 1000ULL, prices, qtys, cnts, 1, 0);
    EXPECT_EQ(st, OB_C_ERR_INVALID_ARG);
}

TEST_F(CApiTest, InvalidSideReturnsError) {
    int64_t  prices[] = {10000};
    uint64_t qtys[]   = {100};
    uint32_t cnts[]   = {1};
    ob_status_t st = ob_apply_delta(engine, "SYM", "EX", 1, 1000ULL, prices, qtys, cnts, 1, 2 /* invalid */);
    EXPECT_EQ(st, OB_C_ERR_INVALID_ARG);
}

TEST_F(CApiTest, QueryNullEngineReturnsNull) {
    ob_result_t* result = ob_query(nullptr, "SELECT * FROM 'SYM'.'EX'");
    EXPECT_EQ(result, nullptr);
}

TEST_F(CApiTest, QueryUnknownSymbolReturnsNull) {
    ob_result_t* result = ob_query(engine, "SELECT * FROM 'NOSYM'.'NOEX'");
    EXPECT_EQ(result, nullptr);
}

TEST_F(CApiTest, ResultNextOnNullReturnsError) {
    ob_status_t st = ob_result_next(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
    EXPECT_EQ(st, OB_C_ERR_INVALID_ARG);
}

TEST_F(CApiTest, ResultNextExhaustedReturnsNotFound) {
    // Apply a delta first so the symbol exists
    int64_t  prices[] = {10000};
    uint64_t qtys[]   = {100};
    uint32_t cnts[]   = {1};
    ob_apply_delta(engine, "SYM", "EX", 1, 1000ULL, prices, qtys, cnts, 1, 0);

    ob_result_t* result = ob_query(engine,
        "SELECT * FROM 'SYM'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999");
    ASSERT_NE(result, nullptr);

    // Drain all rows
    while (ob_result_next(result, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr) == OB_C_OK) {}

    // Next call should return NOT_FOUND
    ob_status_t st = ob_result_next(result, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
    EXPECT_EQ(st, OB_C_ERR_NOT_FOUND);

    ob_result_free(result);
}
