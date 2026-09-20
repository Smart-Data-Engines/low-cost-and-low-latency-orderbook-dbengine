// The live book over the wire - #145.
//
// `SoABuffer` is the current book, and until this item `read_snapshot()` had exactly one caller:
// the aggregate branch of `QueryEngine::execute()`. So a client could ask for VWAP over the book
// and could not ask for the book. Both routes it had were indirect and made it hold what the
// server already holds: `SUBSCRIBE` and rebuild from the stream, or `SELECT` history and replay.
//
// These tests drive `Engine::read_book()` rather than the wire, because what the wire adds is
// parsing and formatting, both of which have their own tests. What is interesting here is which
// rows come out and in what order.

#include "orderbook/engine.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::string temp_dir(const std::string& prefix) {
    auto p = fs::temp_directory_path() / (prefix + std::to_string(::getpid()) + "_" +
                                         std::to_string(::gettid()));
    fs::create_directories(p);
    return p.string();
}

/// One delta, applied through the public write path.
void write_level(ob::Engine& engine, uint64_t seq, uint8_t side, int64_t price, uint64_t qty,
                 uint32_t count = 1) {
    ob::DeltaUpdate d{};
    std::strncpy(d.symbol,   "BOOKT", sizeof(d.symbol)   - 1);
    std::strncpy(d.exchange, "EX",    sizeof(d.exchange) - 1);
    d.timestamp_ns    = 1'700'000'000'000'000'000ULL + seq;
    d.sequence_number = seq;
    d.side            = side;
    d.n_levels        = 1;
    ob::Level level{};
    level.price = price;
    level.qty   = static_cast<int64_t>(qty);
    level.cnt   = count;
    level._pad  = 0;
    engine.apply_delta(d, &level);
}

std::vector<ob::QueryResult> book_of(ob::Engine& engine, uint32_t depth,
                                     const char* symbol = "BOOKT") {
    std::vector<ob::QueryResult> rows;
    const std::string err = engine.read_book(symbol, "EX", depth,
                                             [&](const ob::QueryResult& r) { rows.push_back(r); });
    EXPECT_TRUE(err.empty()) << err;
    return rows;
}

}  // namespace

TEST(QueryBook, BothSidesComeBackBidsFirstAndEachInItsOwnOrder) {
    const std::string dir = temp_dir("ob_book_order_");
    {
        ob::Engine engine(dir, 60'000'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        // Written out of order on purpose: the order in the answer is a property of `SoASide`,
        // which `insert_level()` maintains, not of the order somebody wrote.
        write_level(engine, 1, ob::SIDE_BID, 100, 5);
        write_level(engine, 2, ob::SIDE_BID, 102, 7);
        write_level(engine, 3, ob::SIDE_BID, 101, 6);
        write_level(engine, 4, ob::SIDE_ASK, 105, 8);
        write_level(engine, 5, ob::SIDE_ASK, 103, 9);

        const auto rows = book_of(engine, 0);
        ASSERT_EQ(rows.size(), 5u);

        // Bids first, descending.
        EXPECT_EQ(rows[0].side, ob::SIDE_BID);
        EXPECT_EQ(rows[0].price, 102);
        EXPECT_EQ(rows[0].level, 0);
        EXPECT_EQ(rows[1].price, 101);
        EXPECT_EQ(rows[2].price, 100);
        // Then asks, ascending, with `level` restarting at the best of that side - because a level
        // index is a position within a side and a client reading `level` as a row number would put
        // the best ask below three bids.
        EXPECT_EQ(rows[3].side, ob::SIDE_ASK);
        EXPECT_EQ(rows[3].price, 103);
        EXPECT_EQ(rows[3].level, 0);
        EXPECT_EQ(rows[4].price, 105);
        EXPECT_EQ(rows[4].level, 1);

        // And the payload is the level's, not a placeholder.
        EXPECT_EQ(rows[0].quantity, 7u);
        EXPECT_EQ(rows[0].order_count, 1u);
        engine.close();
    }
    fs::remove_all(dir);
}

TEST(QueryBook, DepthCountsPerSideFromTheBest) {
    const std::string dir = temp_dir("ob_book_depth_");
    {
        ob::Engine engine(dir, 60'000'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        for (int i = 0; i < 5; ++i) write_level(engine, 1 + i, ob::SIDE_BID, 100 - i, 10);
        for (int i = 0; i < 5; ++i) write_level(engine, 10 + i, ob::SIDE_ASK, 110 + i, 10);

        const auto two = book_of(engine, 2);
        ASSERT_EQ(two.size(), 4u) << "depth is per side, so two means two bids and two asks";
        EXPECT_EQ(two[0].price, 100);
        EXPECT_EQ(two[1].price, 99);
        EXPECT_EQ(two[2].price, 110);
        EXPECT_EQ(two[3].price, 111);

        // Asking for more than the side holds is not an error and does not pad.
        EXPECT_EQ(book_of(engine, 50).size(), 10u);
        // Zero, which the parser refuses on the wire, means "everything" to this layer - the two
        // decisions are deliberately separate, because a caller inside the process has no token to
        // be told about.
        EXPECT_EQ(book_of(engine, 0).size(), 10u);
        engine.close();
    }
    fs::remove_all(dir);
}

TEST(QueryBook, EveryRowCarriesTheSameSnapshotIdentity) {
    // The two columns that come from the buffer rather than from a level. Worth a test of its own
    // because it is the part a client will misread: a per-level timestamp is what the column name
    // suggests, and this is the identity of the read - as of which update this book is.
    const std::string dir = temp_dir("ob_book_ident_");
    {
        ob::Engine engine(dir, 60'000'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        write_level(engine, 1, ob::SIDE_BID, 100, 5);
        write_level(engine, 2, ob::SIDE_BID, 99, 5);
        write_level(engine, 3, ob::SIDE_ASK, 101, 5);

        const auto rows = book_of(engine, 0);
        ASSERT_EQ(rows.size(), 3u);
        for (const auto& r : rows) {
            EXPECT_EQ(r.timestamp_ns, rows.front().timestamp_ns);
            EXPECT_EQ(r.sequence_number, rows.front().sequence_number);
        }
        // And it is the **latest** update rather than the first, so a client can resume a
        // SUBSCRIBE from it.
        EXPECT_EQ(rows.front().sequence_number, 3u);
        engine.close();
    }
    fs::remove_all(dir);
}

TEST(QueryBook, ASymbolWithNoLiveBufferIsNotFound) {
    const std::string dir = temp_dir("ob_book_absent_");
    {
        ob::Engine engine(dir, 60'000'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        write_level(engine, 1, ob::SIDE_BID, 100, 5);

        std::vector<ob::QueryResult> rows;
        const std::string err = engine.read_book("NOSUCH", "EX", 0,
                                                 [&](const ob::QueryResult& r) { rows.push_back(r); });
        // The same refusal the aggregate path already gives for this situation, deliberately: one
        // answer for one condition rather than a second spelling of it.
        EXPECT_NE(err.find("OB_ERR_NOT_FOUND"), std::string::npos) << err;
        EXPECT_NE(err.find("NOSUCH"), std::string::npos) << "the refusal does not name the symbol";
        EXPECT_TRUE(rows.empty()) << "a refused read emitted rows";
        engine.close();
    }
    fs::remove_all(dir);
}

TEST(QueryBook, AOneSidedBookAnswersWithThatSide) {
    // The control for the ordering test: with both sides present, an implementation that emitted
    // one side twice would pass a row count and the first three prices.
    const std::string dir = temp_dir("ob_book_oneside_");
    {
        ob::Engine engine(dir, 60'000'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        write_level(engine, 1, ob::SIDE_ASK, 200, 3);

        const auto rows = book_of(engine, 0);
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(rows[0].side, ob::SIDE_ASK);
        EXPECT_EQ(rows[0].price, 200);
        engine.close();
    }
    fs::remove_all(dir);
}
