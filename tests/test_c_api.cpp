// Feature: orderbook-dbengine — C API unit tests (Requirements 13.5)
#include "orderbook/c_api.h"
#include "orderbook/level_payload.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>
#include <cstring>
#include <algorithm>
#include <utility>
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

// ── GROUP BY (#44): refused, not answered as rows ─────────────────────────────

TEST_F(CApiTest, AGroupByQueryIsRefusedRatherThanAnsweredAsRowsOfZeros) {
    // A time-bucket answer is buckets of aggregates, and this result's rows are the seven columns:
    // answered, it would be one row a bucket with the start as its time and zeros for the rest.
    int64_t  prices[] = {10300};
    uint64_t qtys[]   = {100};
    uint32_t cnts[]   = {1};
    ASSERT_EQ(ob_apply_delta(engine, "AAPL", "NYSE", 1, 1000000000ULL, prices, qtys, cnts, 1, 0), OB_C_OK);
    ob_engine_destroy(engine);
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);

    EXPECT_EQ(ob_query(engine, "SELECT COUNT(*) FROM 'AAPL'.'NYSE' GROUP BY TIME_BUCKET(1s)"), nullptr);
    // And the same rows, as rows, still are.
    ob_result_t* rows = ob_query(engine, "SELECT * FROM 'AAPL'.'NYSE'");
    ASSERT_NE(rows, nullptr);
    ob_result_free(rows);
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

// ── ob_replace_side (MREPLACE, #233) ──────────────────────────────────────────
//
// The embedded engine's side replacement: the WAL record and the rows are the update followed by
// a level of quantity 0 for each price the side held and the update did not name, best first.

namespace {

using PriceQty = std::pair<int64_t, uint64_t>;

/// The levels of every DELTA record of `symbol` in the WAL under `dir`, in file order.
std::vector<std::vector<PriceQty>> capi_delta_records(const std::string& dir, const char* symbol) {
    std::vector<std::vector<PriceQty>> out;
    std::vector<ob::Level> scratch;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_DELTA) return;
        ob::DeltaUpdate update{};
        std::memcpy(&update, ctx.payload, sizeof(update));
        if (std::strcmp(update.symbol, symbol) != 0) return;
        const ob::Level* levels = ob::levels_from_payload(ctx.payload, update.n_levels, scratch);
        std::vector<PriceQty> record;
        for (uint16_t i = 0; i < update.n_levels; ++i) record.emplace_back(levels[i].price, levels[i].qty);
        out.push_back(std::move(record));
    });
    return out;
}

/// Every row of `symbol` in history, in the order the query returns them.
std::vector<PriceQty> capi_history(ob_engine_t* engine, const char* symbol) {
    const std::string sql = std::string("SELECT * FROM '") + symbol +
                            "'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    std::vector<PriceQty> rows;
    ob_result_t* result = ob_query(engine, sql.c_str());
    if (result == nullptr) return rows;
    uint64_t ts; int64_t price; uint64_t qty; uint32_t cnt; uint8_t side; uint16_t level;
    while (ob_result_next(result, &ts, &price, &qty, &cnt, &side, &level) == OB_C_OK) {
        rows.emplace_back(price, qty);
    }
    ob_result_free(result);
    return rows;
}

}  // namespace

TEST_F(CApiTest, ReplaceSideLogsAndStoresTheUpdateThenItsRemovals) {
    int64_t  held_p[] = {10300, 10200, 10100};
    uint64_t held_q[] = {100, 200, 300};
    uint32_t held_c[] = {1, 1, 1};
    ASSERT_EQ(ob_apply_delta(engine, "RPL", "EX", 1, 1000ULL, held_p, held_q, held_c, 3, 0), OB_C_OK);
    int64_t  ask_p[] = {10400};
    uint64_t ask_q[] = {50};
    uint32_t ask_c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "RPL", "EX", 2, 1001ULL, ask_p, ask_q, ask_c, 1, 1), OB_C_OK);

    int64_t  new_p[] = {10350, 10200};
    uint64_t new_q[] = {70, 250};
    uint32_t new_c[] = {2, 2};
    ASSERT_EQ(ob_replace_side(engine, "RPL", "EX", 3, 1002ULL, new_p, new_q, new_c, 2, 0), OB_C_OK);

    ob_engine_destroy(engine);
    const auto records = capi_delta_records(dir, "RPL");
    ASSERT_EQ(records.size(), 3u);
    EXPECT_EQ(records[2], (std::vector<PriceQty>{{10300, 0}, {10100, 0}, {10350, 70}, {10200, 250}}))
        << "the record of a replacement is not the bids it removed, highest first, and then its "
           "levels - and the asks are not the bids' business";

    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
    const auto rows = capi_history(engine, "RPL");
    const std::vector<PriceQty> replaced(rows.end() - std::min<size_t>(rows.size(), 4), rows.end());
    EXPECT_EQ(replaced, (std::vector<PriceQty>{{10300, 0}, {10100, 0}, {10350, 70}, {10200, 250}}))
        << "history does not hold the replacement as applied";
}

TEST_F(CApiTest, ReplaceSideOfAFullSideByAnotherFullSideLeavesTheReplacement) {
    // A full side has no room for a price it does not hold until the removals are applied, so they
    // go first. The live book is not readable through the C API; what a second replacement removes
    // is: the 999 prices the first one wrote and the second does not name.
    const uint32_t n = static_cast<uint32_t>(ob::MAX_LEVELS);
    std::vector<int64_t>  held(n), fresh(n);
    std::vector<uint64_t> q(n, 1);
    std::vector<uint32_t> c(n, 1);
    for (uint32_t i = 0; i < n; ++i) {
        held[i]  = 100000 - static_cast<int64_t>(i);
        fresh[i] = 50000 - static_cast<int64_t>(i);
    }
    ASSERT_EQ(ob_apply_delta(engine, "FULL", "EX", 1, 1000ULL, held.data(), q.data(), c.data(), n, 0), OB_C_OK);
    ASSERT_EQ(ob_replace_side(engine, "FULL", "EX", 2, 1001ULL, fresh.data(), q.data(), c.data(), n, 0), OB_C_OK);
    ASSERT_EQ(ob_replace_side(engine, "FULL", "EX", 3, 1002ULL, fresh.data(), q.data(), c.data(), 1, 0), OB_C_OK);
    ob_engine_destroy(engine);
    const auto records = capi_delta_records(dir, "FULL");
    ASSERT_EQ(records.size(), 3u);
    ASSERT_EQ(records[1].size(), 2 * static_cast<size_t>(n));
    EXPECT_EQ(records[1].front(), (PriceQty{100000, 0}));
    EXPECT_EQ(records[1][n], (PriceQty{50000, 1}));
    ASSERT_EQ(records[2].size(), static_cast<size_t>(n)) << "the side after the first replacement was "
                                                            "not the replacement";
    EXPECT_EQ(records[2].front(), (PriceQty{49999, 0}));
    EXPECT_EQ(records[2].back(), (PriceQty{50000, 1}));
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
}

TEST_F(CApiTest, AReplacementAfterAReopenRemovesWhatTheSideHeldBefore) {
    // The Python client's local flush() reopens the engine. A reopened engine composes its live
    // books from history (#235); without that a replacement computed its removals against an empty
    // side, wrote none, and history kept every price the replacement meant to remove.
    int64_t  held_p[] = {100, 99, 98};
    uint64_t held_q[] = {1, 1, 1};
    uint32_t held_c[] = {1, 1, 1};
    ASSERT_EQ(ob_replace_side(engine, "RO", "EX", 1, 1000ULL, held_p, held_q, held_c, 3, 0), OB_C_OK);
    ob_engine_destroy(engine);
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);

    int64_t  new_p[] = {101, 99};
    uint64_t new_q[] = {2, 2};
    uint32_t new_c[] = {1, 1};
    ASSERT_EQ(ob_replace_side(engine, "RO", "EX", 2, 1001ULL, new_p, new_q, new_c, 2, 0), OB_C_OK);
    ob_engine_destroy(engine);
    const auto records = capi_delta_records(dir, "RO");
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[1], (std::vector<PriceQty>{{100, 0}, {98, 0}, {101, 2}, {99, 2}}))
        << "a replacement after a reopen did not remove the prices the side held before it";

    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
    std::vector<PriceQty> book;
    ob_result_t* result = ob_query(engine, "SELECT * FROM 'RO'.'EX' WHERE AT 1001");
    ASSERT_NE(result, nullptr);
    uint64_t ts; int64_t price; uint64_t qty; uint32_t cnt; uint8_t side; uint16_t level;
    while (ob_result_next(result, &ts, &price, &qty, &cnt, &side, &level) == OB_C_OK) {
        book.emplace_back(price, qty);
    }
    ob_result_free(result);
    EXPECT_EQ(book, (std::vector<PriceQty>{{101, 2}, {99, 2}}));
}

TEST_F(CApiTest, TheFirstWriteAfterAReopenIsNotAGap) {
    // The restored book carries no sequence number: the caller's numbering is its own, and a
    // client in a new process that numbers from 1 again has not skipped anything.
    int64_t  p[] = {100};
    uint64_t q[] = {1};
    uint32_t c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "GAP", "EX", 41, 1000ULL, p, q, c, 1, 0), OB_C_OK);
    ob_engine_destroy(engine);
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
    ASSERT_EQ(ob_apply_delta(engine, "GAP", "EX", 1, 1001ULL, p, q, c, 1, 0), OB_C_OK);
    ob_engine_destroy(engine);
    size_t gaps = 0;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type == ob::WAL_RECORD_GAP) ++gaps;
    });
    EXPECT_EQ(gaps, 0u);
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
}

// ── ob_flush (#244) ────────────────────────────────────────────────────────────
//
// A flush writes and indexes each symbol's open segment and keeps the engine. Before it the Python
// client flushed by closing and opening the engine, and an open composes every book from history.

TEST_F(CApiTest, AFlushMakesTheWritesReadableWithoutClosingTheEngine) {
    int64_t  p[] = {100, 99};
    uint64_t q[] = {1, 2};
    uint32_t c[] = {1, 1};
    ASSERT_EQ(ob_apply_delta(engine, "FL", "EX", 1, 1000ULL, p, q, c, 2, 0), OB_C_OK);
    ASSERT_TRUE(capi_history(engine, "FL").empty())
        << "a write was readable before any flush, so nothing here would show what a flush does";
    ASSERT_EQ(ob_flush(engine), OB_C_OK);
    EXPECT_EQ(capi_history(engine, "FL"), (std::vector<PriceQty>{{100, 1}, {99, 2}}));

    // A flush with nothing new changes nothing, and a write after it is read after the next one.
    ASSERT_EQ(ob_flush(engine), OB_C_OK);
    int64_t  later_p[] = {101};
    uint64_t later_q[] = {3};
    uint32_t later_c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "FL", "EX", 2, 1001ULL, later_p, later_q, later_c, 1, 0), OB_C_OK);
    ASSERT_EQ(ob_flush(engine), OB_C_OK);
    EXPECT_EQ(capi_history(engine, "FL"), (std::vector<PriceQty>{{100, 1}, {99, 2}, {101, 3}}));
    EXPECT_EQ(ob_flush(nullptr), OB_C_ERR_INVALID_ARG);
}

TEST_F(CApiTest, AFlushIndexesTheSegmentsAWriteRolledOver) {
    // A segment covers an hour, so the second write closes the first hour's segment inside append(),
    // which cannot reach the index: the flush has to collect it.
    const uint64_t hour = 3600ULL * 1'000'000'000ULL;
    int64_t  p[] = {100};
    uint64_t q[] = {1};
    uint32_t c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "ROLL", "EX", 1, 1000ULL, p, q, c, 1, 0), OB_C_OK);
    q[0] = 2;
    ASSERT_EQ(ob_apply_delta(engine, "ROLL", "EX", 2, hour + 1000ULL, p, q, c, 1, 0), OB_C_OK);
    ASSERT_EQ(ob_flush(engine), OB_C_OK);
    EXPECT_EQ(capi_history(engine, "ROLL"), (std::vector<PriceQty>{{100, 1}, {100, 2}}))
        << "the first hour's segment, closed by the write that began the second, is not read";
}

TEST_F(CApiTest, AFlushKeepsTheLiveBookAndItsLabel) {
    // The engine is not reopened, so the book a write is applied to is the one before the flush,
    // label included: a write numbered past the next one after a flush is a gap, as without one.
    // An engine opened again would hold its books with no number, and see no gap.
    int64_t  p[] = {100, 99};
    uint64_t q[] = {1, 1};
    uint32_t c[] = {1, 1};
    ASSERT_EQ(ob_apply_delta(engine, "LB", "EX", 5, 1000ULL, p, q, c, 2, 0), OB_C_OK);
    ASSERT_EQ(ob_flush(engine), OB_C_OK);
    int64_t  next_p[] = {101};
    uint64_t next_q[] = {2};
    uint32_t next_c[] = {1};
    ASSERT_EQ(ob_replace_side(engine, "LB", "EX", 7, 1001ULL, next_p, next_q, next_c, 1, 0), OB_C_OK);
    ob_engine_destroy(engine);
    size_t gaps = 0;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type == ob::WAL_RECORD_GAP) ++gaps;
    });
    EXPECT_EQ(gaps, 1u) << "the write numbered 7 after 5 was not a gap: the flush lost the book's label";
    const auto records = capi_delta_records(dir, "LB");
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[1], (std::vector<PriceQty>{{100, 0}, {99, 0}, {101, 2}}))
        << "the replacement after the flush did not remove what the side held before it";
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
}

// ── The WAL replayed at an open (#245) ───────────────────────────────────────────
//
// A process that dies leaves its WAL and what it flushed, and nothing else. Each test takes the
// directory as such a process would leave it - a copy made while nothing writes (pitfall 475) - and
// opens an engine on the copy.

namespace {

/// A copy of `dir`, removed with this object.
struct CrashImage {
    std::string path;
    CrashImage(const std::string& dir, const std::string& tag) : path(dir + "_" + tag) {
        fs::remove_all(path);
        fs::copy(dir, path, fs::copy_options::recursive);
    }
    ~CrashImage() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

/// The WAL files under `dir`, by name.
std::vector<std::string> wal_files(const std::string& dir) {
    std::vector<std::string> out;
    for (const auto& entry : fs::directory_iterator(dir)) {
        const std::string name = entry.path().filename().string();
        if (name.rfind("wal_", 0) == 0 && entry.path().extension() == ".bin") out.push_back(entry.path().string());
    }
    std::sort(out.begin(), out.end());
    return out;
}

/// Every segment's WAL position under `dir` taken away, as a build before #245 wrote them.
void strip_positions(const std::string& dir) {
    for (const auto& entry : fs::recursive_directory_iterator(dir)) {
        if (entry.path().filename() != "meta.json") continue;
        std::ifstream in(entry.path());
        std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        in.close();
        for (const char* key : {"\"wal_identity\":", "\"wal_file_index\":", "\"wal_byte_offset\":"}) {
            const auto at = text.find(key);
            ASSERT_NE(at, std::string::npos) << key << " is not in " << entry.path();
            const auto from = at + std::string(key).size();
            const auto to = text.find_first_not_of("0123456789", from);
            text.replace(from, to - from, "0");
        }
        std::ofstream(entry.path(), std::ios::trunc) << text;
    }
}

std::vector<PriceQty> sorted(std::vector<PriceQty> rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

}  // namespace

TEST_F(CApiTest, WritesNotFlushedAreReplayedWhenTheEngineOpensAgain) {
    int64_t  p1[] = {100, 99};
    uint64_t q1[] = {1, 2};
    uint32_t c1[] = {1, 1};
    ASSERT_EQ(ob_apply_delta(engine, "RP", "EX", 1, 1000ULL, p1, q1, c1, 2, 0), OB_C_OK);
    int64_t  p2[] = {200};
    uint64_t q2[] = {3};
    uint32_t c2[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "RQ", "EX", 1, 1001ULL, p2, q2, c2, 1, 1), OB_C_OK);
    const CrashImage image(dir, "killed");
    ob_engine_t* reopened = ob_engine_create(image.path.c_str());
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(sorted(capi_history(reopened, "RP")), (std::vector<PriceQty>{{99, 2}, {100, 1}}));
    EXPECT_EQ(capi_history(reopened, "RQ"), (std::vector<PriceQty>{{200, 3}}));
    ob_engine_destroy(reopened);
    // The open flushed what it replayed and left a checkpoint after it, so the next one replays
    // none of it again.
    reopened = ob_engine_create(image.path.c_str());
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(sorted(capi_history(reopened, "RP")), (std::vector<PriceQty>{{99, 2}, {100, 1}}));
    ob_engine_destroy(reopened);
}

TEST_F(CApiTest, AWriteAfterAFlushIsReplayedAndTheFlushedOnesAreNot) {
    int64_t  p[] = {100};
    uint64_t q[] = {1};
    uint32_t c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "AF", "EX", 1, 1000ULL, p, q, c, 1, 0), OB_C_OK);
    ASSERT_EQ(ob_flush(engine), OB_C_OK);
    p[0] = 101;
    q[0] = 2;
    ASSERT_EQ(ob_apply_delta(engine, "AF", "EX", 2, 1001ULL, p, q, c, 1, 0), OB_C_OK);
    const CrashImage image(dir, "after_flush");
    ob_engine_t* reopened = ob_engine_create(image.path.c_str());
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(sorted(capi_history(reopened, "AF")), (std::vector<PriceQty>{{100, 1}, {101, 2}}));
    ob_engine_destroy(reopened);
}

TEST_F(CApiTest, ADeathBetweenAFlushsSegmentsAndItsCheckpointStoresEachRowOnce) {
    // A flush writes the segments and then the checkpoint. Dying between them leaves segments that
    // hold records no checkpoint covers, and applying those again would store their rows twice: a
    // segment records where its last record starts, and the replay skips what it holds. The times
    // run backwards on purpose, because a segment without a position is judged by the time of its
    // last row, and that rule would replay the first of these.
    int64_t  p[] = {100};
    uint64_t q[] = {1};
    uint32_t c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "CW", "EX", 1, 3000ULL, p, q, c, 1, 0), OB_C_OK);
    p[0] = 101;
    q[0] = 2;
    ASSERT_EQ(ob_apply_delta(engine, "CW", "EX", 2, 1000ULL, p, q, c, 1, 0), OB_C_OK);
    const auto files = wal_files(dir);
    ASSERT_EQ(files.size(), 1u);
    const auto before_flush = fs::file_size(files[0]);
    ASSERT_EQ(ob_flush(engine), OB_C_OK);
    ASSERT_GT(fs::file_size(files[0]), before_flush) << "the flush appended no checkpoint";
    const CrashImage image(dir, "between");
    fs::resize_file(fs::path(image.path) / fs::path(files[0]).filename(), before_flush);
    ob_engine_t* reopened = ob_engine_create(image.path.c_str());
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(sorted(capi_history(reopened, "CW")), (std::vector<PriceQty>{{100, 1}, {101, 2}}));
    ob_engine_destroy(reopened);
}

TEST_F(CApiTest, AClosedEngineEndsItsWalWithACheckpoint) {
    int64_t  p[] = {100};
    uint64_t q[] = {1};
    uint32_t c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "CL", "EX", 1, 1000ULL, p, q, c, 1, 0), OB_C_OK);
    ob_engine_destroy(engine);
    engine = nullptr;
    uint8_t last = 0;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) { last = ctx.header.record_type; });
    EXPECT_EQ(last, ob::WAL_RECORD_CHECKPOINT);
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
    EXPECT_EQ(capi_history(engine, "CL"), (std::vector<PriceQty>{{100, 1}}));
}

namespace {

/// What one `ob_flush()` logs, at DEBUG.
std::string flush_logged(ob_engine_t* engine, ob_status_t& status) {
    auto& logger = ob::StructuredLogger::instance();
    const ob::LogLevel was = logger.level();
    logger.set_level(ob::LogLevel::DEBUG);
    testing::internal::CaptureStderr();
    status = ob_flush(engine);
    const std::string said = testing::internal::GetCapturedStderr();
    logger.set_level(was);
    return said;
}

}  // namespace

TEST_F(CApiTest, AFlushTakesItsSegmentsToTheDeviceBeforeItsCheckpointClaimsThem) {
    // #160's rule, which the server's flush keeps: a checkpoint claims only rows on the device. One
    // that claimed rows still in the page cache - and a close syncs the WAL, so it would be on the
    // device - would leave a power cut nothing to replay them from. What a sync does shows only when
    // the power fails, so this reads the order in which a flush reports its steps.
    int64_t  p[] = {100};
    uint64_t q[] = {1};
    uint32_t c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "SY", "EX", 1, 1000ULL, p, q, c, 1, 0), OB_C_OK);
    ob_status_t flushed = OB_C_ERR_INTERNAL;
    const std::string said = flush_logged(engine, flushed);
    // With nothing new a flush claims nothing, and it waits for no device: what #244 measured.
    ob_status_t again = OB_C_ERR_INTERNAL;
    const std::string said_again = flush_logged(engine, again);
    ASSERT_EQ(flushed, OB_C_OK);
    ASSERT_EQ(again, OB_C_OK);
    const auto synced = said.find("the segments and the WAL synced in");
    const auto claimed = said.find("Checkpoint appended");
    ASSERT_NE(synced, std::string::npos) << "the flush did not sync:\n" << said;
    ASSERT_NE(claimed, std::string::npos) << "the flush appended no checkpoint:\n" << said;
    EXPECT_LT(synced, claimed) << "the checkpoint came before the sync:\n" << said;
    EXPECT_EQ(said_again.find("synced in"), std::string::npos)
        << "a flush with nothing new synced:\n" << said_again;
    EXPECT_EQ(said_again.find("Checkpoint appended"), std::string::npos)
        << "a flush with nothing new appended a checkpoint:\n" << said_again;
}

TEST_F(CApiTest, TheFirstFlushAfterAnOpenSyncsOnlyWhatNoCheckpointClaims) {
    // A fresh directory, and one a close ended with a checkpoint, hold nothing a checkpoint has to
    // claim: the first flush after the open, with nothing written, waits for no device.
    ob_status_t status = OB_C_ERR_INTERNAL;
    std::string said = flush_logged(engine, status);
    ASSERT_EQ(status, OB_C_OK);
    EXPECT_EQ(said.find("synced in"), std::string::npos) << "a fresh directory:\n" << said;
    int64_t  p[] = {100};
    uint64_t q[] = {1};
    uint32_t c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "FF", "EX", 1, 1000ULL, p, q, c, 1, 0), OB_C_OK);
    ob_engine_destroy(engine);
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
    said = flush_logged(engine, status);
    ASSERT_EQ(status, OB_C_OK);
    EXPECT_EQ(said.find("synced in"), std::string::npos) << "after a close:\n" << said;
    EXPECT_EQ(said.find("Checkpoint appended"), std::string::npos) << "after a close:\n" << said;
    EXPECT_EQ(capi_history(engine, "FF"), (std::vector<PriceQty>{{100, 1}}));

    // Records after the last checkpoint that the segments already hold are another matter: the
    // first flush checkpoints them, so that the next open does not read them again.
    p[0] = 101;
    ASSERT_EQ(ob_apply_delta(engine, "FF", "EX", 2, 2000ULL, p, q, c, 1, 0), OB_C_OK);
    const auto files = wal_files(dir);
    ASSERT_FALSE(files.empty());
    const auto before_flush = fs::file_size(files.back());
    ASSERT_EQ(ob_flush(engine), OB_C_OK);
    const CrashImage image(dir, "held");
    fs::resize_file(fs::path(image.path) / fs::path(files.back()).filename(), before_flush);
    ob_engine_t* reopened = ob_engine_create(image.path.c_str());
    ASSERT_NE(reopened, nullptr);
    said = flush_logged(reopened, status);
    EXPECT_EQ(status, OB_C_OK);
    EXPECT_NE(said.find("Checkpoint appended"), std::string::npos)
        << "records the segments hold were left for the next open to read again:\n" << said;
    EXPECT_EQ(sorted(capi_history(reopened, "FF")), (std::vector<PriceQty>{{100, 1}, {101, 1}}));
    ob_engine_destroy(reopened);
}

TEST_F(CApiTest, ASegmentOfAnOlderBuildIsJudgedByTheTimeOfItsLastRow) {
    // A build before #245 recorded no position in a segment and wrote no checkpoint, so its WAL holds
    // every record it ever wrote. Replayed whole, every row it stored would be stored twice. A
    // segment without a position is judged as the server judged one before #63: a record no later
    // than its last row is in it.
    int64_t  p[] = {100};
    uint64_t q[] = {1};
    uint32_t c[] = {1};
    ASSERT_EQ(ob_apply_delta(engine, "OB", "EX", 1, 1000ULL, p, q, c, 1, 0), OB_C_OK);
    p[0] = 101;
    q[0] = 2;
    ASSERT_EQ(ob_apply_delta(engine, "OB", "EX", 2, 2000ULL, p, q, c, 1, 0), OB_C_OK);
    const auto files = wal_files(dir);
    ASSERT_EQ(files.size(), 1u);
    const auto before_flush = fs::file_size(files[0]);
    ASSERT_EQ(ob_flush(engine), OB_C_OK);
    const CrashImage image(dir, "older");
    fs::resize_file(fs::path(image.path) / fs::path(files[0]).filename(), before_flush);
    strip_positions(image.path);
    ob_engine_t* reopened = ob_engine_create(image.path.c_str());
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(sorted(capi_history(reopened, "OB")), (std::vector<PriceQty>{{100, 1}, {101, 2}}));
    ob_engine_destroy(reopened);
}

TEST_F(CApiTest, ReplaceSideWithNoLevelsEmptiesTheSide) {
    int64_t  p[] = {10300, 10200};
    uint64_t q[] = {100, 200};
    uint32_t c[] = {1, 1};
    ASSERT_EQ(ob_apply_delta(engine, "EMP", "EX", 1, 1000ULL, p, q, c, 2, 1), OB_C_OK);
    ASSERT_EQ(ob_replace_side(engine, "EMP", "EX", 2, 1001ULL, nullptr, nullptr, nullptr, 0, 1), OB_C_OK);
    ob_engine_destroy(engine);
    const auto records = capi_delta_records(dir, "EMP");
    ASSERT_EQ(records.size(), 2u);
    // Asks, so lowest first.
    EXPECT_EQ(records[1], (std::vector<PriceQty>{{10200, 0}, {10300, 0}}));
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
}

TEST_F(CApiTest, ReplaceSideRefusesMoreLevelsThanTheBookHoldsRatherThanCuttingThem) {
    // ob_apply_delta() cuts a write to MAX_LEVELS; a replacement cut short would remove prices it
    // named, so it is refused - and refused before anything is logged.
    const uint32_t n = static_cast<uint32_t>(ob::MAX_LEVELS) + 1;
    std::vector<int64_t>  p(n);
    std::vector<uint64_t> q(n, 1);
    std::vector<uint32_t> c(n, 1);
    for (uint32_t i = 0; i < n; ++i) p[i] = 100000 - static_cast<int64_t>(i);
    EXPECT_EQ(ob_replace_side(engine, "BIG", "EX", 1, 1000ULL, p.data(), q.data(), c.data(), n, 0),
              OB_C_ERR_INVALID_ARG);
    EXPECT_EQ(ob_replace_side(engine, "BIG", "EX", 1, 1000ULL, p.data(), q.data(), c.data(), n - 1, 0),
              OB_C_OK);
    ob_engine_destroy(engine);
    const auto records = capi_delta_records(dir, "BIG");
    ASSERT_EQ(records.size(), 1u) << "the refused replacement was logged";
    EXPECT_EQ(records[0].size(), static_cast<size_t>(ob::MAX_LEVELS));
    engine = ob_engine_create(dir.c_str());
    ASSERT_NE(engine, nullptr);
}

TEST_F(CApiTest, ReplaceSideRefusesWhatApplyDeltaRefuses) {
    int64_t  p[] = {1};
    uint64_t q[] = {1};
    uint32_t c[] = {1};
    EXPECT_EQ(ob_replace_side(nullptr, "S", "EX", 1, 1ULL, p, q, c, 1, 0), OB_C_ERR_INVALID_ARG);
    EXPECT_EQ(ob_replace_side(engine, nullptr, "EX", 1, 1ULL, p, q, c, 1, 0), OB_C_ERR_INVALID_ARG);
    EXPECT_EQ(ob_replace_side(engine, "S", "EX", 1, 1ULL, nullptr, q, c, 1, 0), OB_C_ERR_INVALID_ARG);
    EXPECT_EQ(ob_replace_side(engine, "S", "EX", 1, 1ULL, p, q, c, 1, 2), OB_C_ERR_INVALID_ARG);
}
