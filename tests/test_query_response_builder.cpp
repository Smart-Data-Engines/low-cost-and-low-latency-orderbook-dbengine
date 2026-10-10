// #49 step 3: a SELECT's reply built row by row, as the engine hands the rows over.
//
// The server collected every row of a SELECT in a vector before it formatted one. It formats each
// row as it arrives now, through QueryResponseBuilder - which has to write the bytes
// format_query_response() writes, for every list of columns, because the reply is a wire format.
// And it can write the header at the first row only because the engine fills a query's shape
// before it hands over one: the second half of this file holds the engine to that, for each kind
// of query that hands over rows.

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/command_parser.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/query_columns.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/soa_buffer.hpp"
#include "orderbook/tcp_server.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::string built(const std::vector<ob::QueryResult>& rows, const std::vector<ob::QueryColumn>& columns) {
    ob::QueryShape shape;
    shape.columns = columns;
    ob::QueryResponseBuilder reply(shape);
    for (const auto& r : rows) reply.add(r);
    return reply.finish();
}

ob::QueryResult a_row(uint64_t ts, int64_t price, uint64_t qty, uint32_t cnt, uint8_t side,
                      uint16_t level, uint64_t seq) {
    ob::QueryResult r{};
    r.timestamp_ns = ts;
    r.price = price;
    r.quantity = qty;
    r.order_count = cnt;
    r.side = side;
    r.level = level;
    r.sequence_number = seq;
    return r;
}

}  // namespace

// ── The bytes ─────────────────────────────────────────────────────────────────

// Any rows, any list of columns - the seven, a narrowing, repeats, none - and the bytes are
// format_query_response()'s. Fields are drawn at full size (pitfall 531): a draw scaled by the
// case's size would leave the widest numbers, the ones the row buffer is sized for, nearly unseen.
RC_GTEST_PROP(QueryResponseBuilderProperty, WritesTheBytesFormatQueryResponseWrites, ()) {
    const auto column = rc::gen::element(ob::QueryColumn::TimestampNs, ob::QueryColumn::Price,
                                         ob::QueryColumn::Quantity, ob::QueryColumn::OrderCount,
                                         ob::QueryColumn::Side, ob::QueryColumn::Level,
                                         ob::QueryColumn::SequenceNumber);
    const auto columns = *rc::gen::weightedOneOf<std::vector<ob::QueryColumn>>({
        {2, rc::gen::just(ob::all_query_columns())},
        {3, rc::gen::container<std::vector<ob::QueryColumn>>(column)},
    });
    const auto n = *rc::gen::inRange<size_t>(0, 200);
    std::vector<ob::QueryResult> rows;
    rows.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        rows.push_back(a_row(*rc::gen::resize(100, rc::gen::arbitrary<uint64_t>()),
                             *rc::gen::resize(100, rc::gen::arbitrary<int64_t>()),
                             *rc::gen::resize(100, rc::gen::arbitrary<uint64_t>()),
                             *rc::gen::resize(100, rc::gen::arbitrary<uint32_t>()),
                             *rc::gen::resize(100, rc::gen::arbitrary<uint8_t>()),
                             *rc::gen::resize(100, rc::gen::arbitrary<uint16_t>()),
                             *rc::gen::resize(100, rc::gen::arbitrary<uint64_t>())));
    }
    RC_ASSERT(built(rows, columns) == ob::format_query_response(rows, columns));
}

TEST(QueryResponseBuilder, NoRowIsTheHeaderAndTheTerminator) {
    for (const auto& columns : std::vector<std::vector<ob::QueryColumn>>{
             ob::all_query_columns(), {ob::QueryColumn::Price}, {}}) {
        EXPECT_EQ(built({}, columns), ob::format_query_response({}, columns));
    }
}

TEST(QueryResponseBuilder, EveryFieldAtItsTypesLimit) {
    const std::vector<ob::QueryResult> rows = {
        a_row(UINT64_MAX, INT64_MIN, UINT64_MAX, UINT32_MAX, UINT8_MAX, UINT16_MAX, UINT64_MAX),
        a_row(0, INT64_MAX, 0, 0, 0, 0, 0)};
    EXPECT_EQ(built(rows, ob::all_query_columns()), ob::format_query_response(rows, ob::all_query_columns()));
    const std::vector<ob::QueryColumn> twice(20, ob::QueryColumn::SequenceNumber);
    EXPECT_EQ(built(rows, twice), ob::format_query_response(rows, twice));
}

// #227 writes the rows into the reply itself, which is sized ahead of them and cut to them at the
// end. Replies many times the size the reply starts at, of rows as wide as a row can be and as
// narrow, grow over and over - and must still be format_query_response()'s bytes, with nothing of
// the room ahead of the rows answered.
TEST(QueryResponseBuilder, AReplyGrownManyTimesOverIsTheSameBytes) {
    std::vector<ob::QueryResult> rows;
    for (uint64_t i = 0; i < 20'000; ++i) {
        rows.push_back(i % 2 ? a_row(UINT64_MAX - i, INT64_MIN + static_cast<int64_t>(i), UINT64_MAX,
                                     UINT32_MAX, UINT8_MAX, UINT16_MAX, UINT64_MAX - i)
                             : a_row(i, static_cast<int64_t>(i), i, 0, 0, 0, i));
    }
    for (const auto& columns : std::vector<std::vector<ob::QueryColumn>>{
             ob::all_query_columns(),
             {ob::QueryColumn::TimestampNs, ob::QueryColumn::Price, ob::QueryColumn::Quantity},
             std::vector<ob::QueryColumn>(20, ob::QueryColumn::SequenceNumber)}) {
        for (size_t n : {size_t{1}, size_t{97}, size_t{2'000}, rows.size()}) {
            const std::vector<ob::QueryResult> some(rows.begin(),
                                                    rows.begin() + static_cast<std::ptrdiff_t>(n));
            const std::string reply = built(some, columns);
            EXPECT_EQ(reply, ob::format_query_response(some, columns))
                << n << " rows of " << columns.size() << " columns";
            EXPECT_EQ(reply.find('\0'), std::string::npos)
                << "a byte of the room ahead of the rows was answered";
        }
    }
}

// An aggregate query hands over one result with its aggregates set and its row fields zero; the
// reply is the aggregates', as the server answered when it looked at the first row it collected.
TEST(QueryResponseBuilder, AFirstResultWithAggregatesIsAnsweredAsAggregates) {
    ob::QueryResult agg{};
    agg.agg_values.push_back(ob::AggValue{"SPREAD(*)", 1000, false, 1});
    agg.agg_values.push_back(ob::AggValue{"MID_PRICE(*)", 0, true, 1'000'000});
    ob::QueryShape shape;
    shape.is_aggregate = true;
    ob::QueryResponseBuilder reply(shape);
    reply.add(agg);
    EXPECT_EQ(reply.finish(), ob::format_agg_response(agg.agg_values));
}

// The builder reads the shape it was given when the first row comes, not when it is made: the
// server makes it before the engine has parsed the query.
TEST(QueryResponseBuilder, TheHeaderIsTheShapeWhenTheFirstRowComes) {
    ob::QueryShape shape;
    ob::QueryResponseBuilder reply(shape);
    shape.columns = {ob::QueryColumn::Quantity, ob::QueryColumn::Price};
    reply.add(a_row(1, -2, 3, 4, 1, 5, 6));
    EXPECT_EQ(reply.finish(), "OK\nquantity\tprice\n3\t-2\n\n");
}

// ── What the builder needs of the engine ──────────────────────────────────────

namespace {

std::atomic<uint64_t> g_counter{0};

std::string made(const fs::path& dir) {
    fs::create_directories(dir);
    return dir.string();
}

struct Fixture {
    fs::path dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    std::shared_ptr<ob::SoABuffer> book{std::make_shared<ob::SoABuffer>()};
    std::unordered_map<std::string, std::shared_ptr<ob::SoABuffer>> live;
    ob::QueryEngine engine;

    Fixture()
        : dir(fs::temp_directory_path() / ("ob_reply_builder_" + std::to_string(::getpid()) + "_" +
                                           std::to_string(g_counter.fetch_add(1))))
        , store(made(dir))
        , engine(store,
                 [this](const std::string& key) -> std::shared_ptr<ob::SoABuffer> {
                     auto it = live.find(key);
                     return it == live.end() ? nullptr : it->second;
                 },
                 agg) {
        std::strncpy(book->symbol, "RB", sizeof(book->symbol) - 1);
        std::strncpy(book->exchange, "EX", sizeof(book->exchange) - 1);
        book->last_timestamp_ns = 1'790'000'000'000'000'000ULL;
        EXPECT_EQ(ob::insert_level(book->bid, 100, 5, 1, /*descending=*/true), ob::OB_OK);
        EXPECT_EQ(ob::insert_level(book->ask, 101, 7, 1, /*descending=*/false), ob::OB_OK);
        live["RB.EX"] = book;

        store.set_symbol_exchange("RB", "EX");
        for (uint64_t i = 0; i < 50; ++i) {
            ob::SnapshotRow r{};
            r.timestamp_ns = 1'790'000'000'000'000'000ULL + i * 1'000;
            r.sequence_number = i + 1;
            r.side = static_cast<uint8_t>(i % 2);
            r.level_index = static_cast<uint16_t>(i % 5);
            r.price = 10'000 + static_cast<int64_t>(i);
            r.quantity = 1 + i;
            r.order_count = 1;
            store.append(r);
        }
        EXPECT_TRUE(store.flush_segment().has_value());
    }
    ~Fixture() {
        store.close();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
};

struct Seen {
    size_t results = 0;
    std::vector<ob::QueryColumn> columns_at_first;
    bool aggregate_at_first = false;
};

Seen run(Fixture& fx, const std::string& sql, ob::QueryShape& shape) {
    Seen seen;
    const std::string err = fx.engine.execute(sql, [&](const ob::QueryResult&) {
        if (seen.results++ == 0) {
            seen.columns_at_first = shape.columns;
            seen.aggregate_at_first = shape.is_aggregate;
        }
    }, shape);
    EXPECT_TRUE(err.empty()) << sql << ": " << err;
    return seen;
}

}  // namespace

TEST(QueryShapeBeforeRows, ARowScanHasItsColumnsBeforeItsFirstRow) {
    Fixture fx;
    ob::QueryShape shape;
    const Seen seen = run(fx, "SELECT price, quantity FROM 'RB'.'EX' WHERE timestamp BETWEEN 0 AND "
                              "9999999999999999999", shape);
    ASSERT_EQ(seen.results, 50u);
    EXPECT_EQ(seen.columns_at_first,
              (std::vector<ob::QueryColumn>{ob::QueryColumn::Price, ob::QueryColumn::Quantity}));
}

TEST(QueryShapeBeforeRows, ASnapshotHasItsColumnsBeforeItsFirstRow) {
    Fixture fx;
    ob::QueryShape shape;
    const Seen seen = run(fx, "SELECT * FROM 'RB'.'EX' WHERE AT 9999999999999999999", shape);
    ASSERT_GT(seen.results, 0u);
    EXPECT_EQ(seen.columns_at_first, ob::all_query_columns());
}

TEST(QueryShapeBeforeRows, AnAggregateIsMarkedBeforeItsResult) {
    Fixture fx;
    ob::QueryShape shape;
    const Seen seen = run(fx, "SELECT SPREAD(*) FROM 'RB'.'EX'", shape);
    ASSERT_EQ(seen.results, 1u);
    EXPECT_TRUE(seen.aggregate_at_first);
}

// The two ways the server could compose a reply, on the same engine: collected and formatted, or
// built as the rows come. The same bytes, for a scan, a narrowing, a snapshot and an aggregate.
TEST(QueryShapeBeforeRows, TheBuiltReplyIsTheCollectedOnes) {
    Fixture fx;
    for (const std::string& sql : {
             std::string{"SELECT * FROM 'RB'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999"},
             std::string{"SELECT quantity, price, quantity FROM 'RB'.'EX' WHERE timestamp BETWEEN 0 AND "
                         "9999999999999999999 LIMIT 7"},
             std::string{"SELECT * FROM 'RB'.'EX' WHERE AT 9999999999999999999"},
             std::string{"SELECT SPREAD(*), MID_PRICE(*) FROM 'RB'.'EX'"}}) {
        std::vector<ob::QueryResult> rows;
        ob::QueryShape collected_shape;
        ASSERT_TRUE(fx.engine.execute(sql, [&](const ob::QueryResult& r) { rows.push_back(r); },
                                      collected_shape).empty());
        const std::string collected =
            (!rows.empty() && !rows.front().agg_values.empty())
                ? ob::format_agg_response(rows.front().agg_values)
                : ob::format_query_response(rows, collected_shape.columns);

        ob::QueryShape shape;
        ob::QueryResponseBuilder reply(shape);
        ASSERT_TRUE(fx.engine.execute(sql, [&](const ob::QueryResult& r) { reply.add(r); }, shape)
                        .empty());
        EXPECT_EQ(reply.finish(), collected) << sql;
    }
}

// ── On the server ─────────────────────────────────────────────────────────────

// What the server's handler answers a SELECT and a BOOK with, on a real engine, against the rows
// the engine hands over collected and formatted as the handler did before #49's step 3.
TEST(QueryResponseOnTheServer, SelectAndBookAnswerWhatTheirCollectedRowsDid) {
    const fs::path dir = fs::temp_directory_path() /
                         ("ob_reply_server_" + std::to_string(::getpid()) + "_" +
                          std::to_string(g_counter.fetch_add(1)));
    fs::create_directories(dir);
    {
        ob::Engine engine(dir.string(), 1'000'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        for (uint64_t u = 0; u < 4; ++u) {
            ob::DeltaUpdate du{};
            std::strncpy(du.symbol, "RBS", sizeof(du.symbol) - 1);
            std::strncpy(du.exchange, "EX", sizeof(du.exchange) - 1);
            du.sequence_number = u + 1;
            du.timestamp_ns = 1'790'000'000'000'000'000ULL + u * 1'000;
            du.side = u % 2 == 0 ? ob::SIDE_BID : ob::SIDE_ASK;
            du.n_levels = 5;
            std::vector<ob::Level> levels(5);
            for (uint16_t i = 0; i < 5; ++i) {
                levels[i].price = (u % 2 == 0 ? 10'000 - i : 10'010 + i) + static_cast<int64_t>(u);
                levels[i].qty = 10 + i + u;
                levels[i].cnt = 1 + i;
            }
            ASSERT_EQ(engine.apply_delta(du, levels.data()), ob::OB_OK);
        }
        engine.flush_incremental();

        ob::Session session(-1);
        ob::ServerStats stats;

        const std::string sql = "SELECT * FROM 'RBS'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
        std::vector<ob::QueryResult> rows;
        ob::QueryShape shape;
        ASSERT_TRUE(engine.execute(sql, [&](const ob::QueryResult& r) { rows.push_back(r); }, shape).empty());
        ASSERT_EQ(rows.size(), 20u);
        EXPECT_EQ(ob::execute_command(ob::parse_command(sql), engine, session, stats, false),
                  ob::format_query_response(rows, shape.columns));

        std::vector<ob::QueryResult> book;
        ASSERT_TRUE(engine.read_book("RBS", "EX", 0, [&](const ob::QueryResult& r) { book.push_back(r); })
                        .empty());
        ASSERT_FALSE(book.empty());
        EXPECT_EQ(ob::execute_command(ob::parse_command("BOOK RBS EX"), engine, session, stats, false),
                  ob::format_query_response(book, ob::all_query_columns()));
        engine.close();
    }
    std::error_code ec;
    fs::remove_all(dir, ec);
}
