// One book live and in history (#233, decision B'), and the live book back after a start (#235).
//
// The live book applies an update by price: each level sets its price's quantity, and a quantity of
// 0 removes the price. History used to read a book at an instant by position - the latest row of
// each level - which is the live book only for a writer whose every update is the whole side from
// its best level. It reads by price now, so for any writes in time order `WHERE AT t` is the book
// the live one held at t: the property below writes random diffs and side replacements, records the
// live book after each, and asks history for every one of them, with flushes in between so the
// answers come from segments, blocks and both.
//
// What `BOOK` and `WHERE AT` cannot agree on is the label: every row of `BOOK` carries the update the
// read is as of, and a row of `WHERE AT` the update that set its price. The comparison leaves those
// two fields out and keeps the rest - side, level, price, quantity, count.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <unistd.h>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

/// One hour, so no tick but the ones a test asks for moves rows.
constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;
constexpr uint64_t kT0 = 1'700'000'000'000'000'000ULL;

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix) {
        auto p = fs::temp_directory_path() /
                 (prefix + std::to_string(::getpid()) + "_" +
                  std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
        fs::create_directories(p);
        path = p.string();
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

/// A level as both reads can answer it: side, level, price, quantity, count.
using Level = std::tuple<uint8_t, uint16_t, int64_t, uint64_t, uint32_t>;

std::unique_ptr<ob::Engine> open_engine(const std::string& dir) {
    auto engine = std::make_unique<ob::Engine>(dir, kNoAutoFlush);
    engine->open();
    return engine;
}

ob::ob_status_t write(ob::Engine& engine, uint8_t side, const std::vector<ob::Level>& levels,
                      uint64_t ts, bool replace) {
    ob::DeltaUpdate update{};
    std::strncpy(update.symbol, "BBP", sizeof(update.symbol) - 1);
    std::strncpy(update.exchange, "EX", sizeof(update.exchange) - 1);
    update.timestamp_ns = ts;
    update.side         = side;
    update.n_levels     = static_cast<uint16_t>(levels.size());
    return replace ? engine.replace_side(update, levels.data())
                   : engine.apply_delta(update, levels.data());
}

std::vector<Level> live_book(ob::Engine& engine) {
    std::vector<Level> out;
    const std::string err = engine.read_book("BBP", "EX", 2000, [&](const ob::QueryResult& r) {
        out.emplace_back(r.side, r.level, r.price, r.quantity, r.order_count);
    });
    // A symbol the node has not seen yet has no book: an empty one, for this comparison.
    if (!err.empty() && err.find("not found") == std::string::npos) ADD_FAILURE() << err;
    return out;
}

std::vector<Level> book_at(ob::Engine& engine, uint64_t at, std::string* err = nullptr) {
    std::vector<Level> out;
    const std::string e = engine.execute("SELECT * FROM 'BBP'.'EX' WHERE AT " + std::to_string(at),
                                         [&](const ob::QueryResult& r) {
                                             out.emplace_back(r.side, r.level, r.price, r.quantity,
                                                              r.order_count);
                                         });
    if (err != nullptr) *err = e;
    return out;
}

ob::Level level(int64_t price, uint64_t qty, uint32_t cnt = 1) {
    ob::Level l{};
    l.price = price;
    l.qty   = qty;
    l.cnt   = cnt;
    return l;
}

}  // namespace

// ── The writers #233 was found with ───────────────────────────────────────────

TEST(BookByPrice, AWriterOfTopLevelsReadsTheLiveBookInHistory) {
    // Each update is the side's best two levels, as a depth stream sends them: the live book keeps
    // the prices that dropped out, and so does history now.
    TempDir dir("ob_bbp_top_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(100, 1), level(99, 1)}, kT0, false), ob::OB_OK);
    ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(98, 2), level(97, 2)}, kT0 + 1000, false), ob::OB_OK);
    const auto live = live_book(*engine);
    engine->flush_incremental();
    EXPECT_EQ(book_at(*engine, kT0 + 1000), live);
    EXPECT_EQ(std::get<2>(book_at(*engine, kT0 + 1000).front()), 100)
        << "history's best bid is not the live book's";
    engine->close();
}

TEST(BookByPrice, AWriterOfChangedLevelsReadsTheLiveBookInHistory) {
    // The first update is the book; the second changes one level behind the best, as a diff does.
    TempDir dir("ob_bbp_diff_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(1000, 1), level(990, 1)}, kT0, false), ob::OB_OK);
    ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(995, 5)}, kT0 + 1000, false), ob::OB_OK);
    const auto live = live_book(*engine);
    engine->flush_incremental();
    const auto then = book_at(*engine, kT0 + 1000);
    EXPECT_EQ(then, live);
    ASSERT_EQ(then.size(), 3u);
    EXPECT_EQ(std::get<2>(then[0]), 1000) << "the diff's level 0 was read as the best bid";
    EXPECT_EQ(std::get<1>(then[1]), 1u) << "a row's level is not its position in the book";
}

TEST(BookByPrice, AReplacementIsReadAsTheSideItLeft) {
    TempDir dir("ob_bbp_replace_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(write(*engine, ob::SIDE_ASK, {level(101, 1), level(102, 1), level(103, 1)}, kT0, false),
              ob::OB_OK);
    engine->flush_incremental();   // the book in one segment, the replacement in the next
    ASSERT_EQ(write(*engine, ob::SIDE_ASK, {level(102, 7)}, kT0 + 1000, true), ob::OB_OK);
    const auto live = live_book(*engine);
    engine->flush_incremental();
    EXPECT_EQ(book_at(*engine, kT0 + 1000), live);
    EXPECT_EQ(book_at(*engine, kT0 + 1000), (std::vector<Level>{{ob::SIDE_ASK, 0, 102, 7, 1}}));
    // Before the replacement, the book it replaced.
    EXPECT_EQ(book_at(*engine, kT0 + 999).size(), 3u);
    engine->close();
}

// ── The property ──────────────────────────────────────────────────────────────

RC_GTEST_PROP(BookByPriceProperty, HistoryAtEachWriteIsTheLiveBookAfterIt, ()) {
    TempDir dir("ob_bbp_prop_");
    auto engine = open_engine(dir.path);
    const size_t writes = *rc::gen::inRange<size_t>(1, 40);
    std::vector<std::pair<uint64_t, std::vector<Level>>> seen;
    uint64_t ts = kT0;
    for (size_t w = 0; w < writes; ++w) {
        ts += *rc::gen::inRange<uint64_t>(1, 1000);
        const uint8_t side = *rc::gen::inRange(0, 2) == 0 ? ob::SIDE_BID : ob::SIDE_ASK;
        const bool replace = *rc::gen::inRange(0, 4) == 0;
        // Prices from a small range, so updates meet the prices earlier ones set, and a quantity
        // of 0 a quarter of the time: removals, of prices held and of prices not.
        const size_t n = *rc::gen::inRange<size_t>(replace ? 0 : 1, 8);
        std::vector<ob::Level> levels;
        std::map<int64_t, bool> named;
        for (size_t i = 0; i < n; ++i) {
            const int64_t price = *rc::gen::inRange<int64_t>(1, 30);
            if (named.count(price) != 0) continue;   // one level a price, as the SDK's diff sends
            named[price] = true;
            const uint64_t qty = *rc::gen::inRange(0, 4) == 0 ? 0 : *rc::gen::inRange<uint64_t>(1, 100);
            levels.push_back(level(price, qty, *rc::gen::inRange<uint32_t>(1, 5)));
        }
        RC_ASSERT(write(*engine, side, levels, ts, replace) == ob::OB_OK);
        seen.emplace_back(ts, live_book(*engine));
        if (*rc::gen::inRange(0, 6) == 0) engine->flush_incremental();
    }
    // History holds what a flush has taken from the queue; the last writes are in it after this one.
    engine->flush_incremental();
    for (const auto& [at, live] : seen) {
        std::string err;
        const auto then = book_at(*engine, at, &err);
        RC_ASSERT(err.empty() || live.empty());
        RC_ASSERT(then == live);
    }
    engine->close();
}

// ── The live book after a start (#235) ────────────────────────────────────────

TEST(LiveBookAfterAStart, ACleanStopBringsTheBookBack) {
    TempDir dir("ob_bbp_stop_");
    std::vector<Level> before;
    {
        auto engine = open_engine(dir.path);
        ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(100, 1), level(99, 2)}, kT0, false), ob::OB_OK);
        ASSERT_EQ(write(*engine, ob::SIDE_ASK, {level(101, 3)}, kT0 + 1, false), ob::OB_OK);
        engine->flush_incremental();   // a checkpoint: these are in a segment
        ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(98, 4), level(99, 0)}, kT0 + 2, false), ob::OB_OK);
        before = live_book(*engine);
        engine->close();   // a clean stop: its flush leaves no tail
    }
    ASSERT_EQ(before.size(), 3u);
    auto reopened = open_engine(dir.path);
    EXPECT_EQ(live_book(*reopened), before) << "the live book did not come back with the node";
    reopened->close();
}

TEST(LiveBookAfterAStart, ACrashBringsTheBookBackFromHistoryAndTheTail) {
    TempDir dir("ob_bbp_crash_");
    TempDir copy("ob_bbp_crash_copy_");
    std::vector<Level> before;
    {
        auto engine = open_engine(dir.path);
        ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(100, 1), level(99, 2)}, kT0, false), ob::OB_OK);
        engine->flush_incremental();   // in a segment
        ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(98, 4)}, kT0 + 1, true), ob::OB_OK);   // in the tail
        before = live_book(*engine);
        // What a crash leaves: the directory as it is, read by a new engine.
        fs::copy(dir.path, copy.path, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
        engine->close();
    }
    ASSERT_EQ(before, (std::vector<Level>{{ob::SIDE_BID, 0, 98, 4, 1}}));
    auto reopened = open_engine(copy.path);
    EXPECT_EQ(live_book(*reopened), before);
    reopened->close();
}

TEST(LiveBookAfterAStart, TheBookAfterAStartIsTheBookAtTheLastInstant) {
    // The rule a start holds to (#235): `BOOK` after it is `WHERE AT` the last instant.
    TempDir dir("ob_bbp_last_");
    {
        auto engine = open_engine(dir.path);
        for (int i = 0; i < 20; ++i) {
            const uint8_t side = (i % 2) ? ob::SIDE_ASK : ob::SIDE_BID;
            const int64_t base = side == ob::SIDE_BID ? 100 : 200;
            ASSERT_EQ(write(*engine, side, {level(base + i % 5, static_cast<uint64_t>(i % 3)),
                                            level(base - 1 - i % 4, 7)},
                            kT0 + static_cast<uint64_t>(i), i % 7 == 6),
                      ob::OB_OK);
            if (i % 6 == 5) engine->flush_incremental();
        }
        engine->close();
    }
    auto reopened = open_engine(dir.path);
    EXPECT_EQ(live_book(*reopened), book_at(*reopened, UINT64_MAX));
    reopened->close();
}

// ── With the summaries a seal writes ──────────────────────────────────────────

TEST(BookByPriceWithSummaries, ASealWritesThemAndAReadStartsFromTheNewest) {
    TempDir dir("ob_bbp_sum_");
    auto engine = open_engine(dir.path);
    engine->set_book_summary_rows(4);
    uint64_t ts = kT0;
    for (int i = 0; i < 6; ++i) {
        ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(100 - i, 1), level(90 - i, 2)}, ++ts, false),
                  ob::OB_OK);
        ASSERT_EQ(write(*engine, ob::SIDE_ASK, {level(110 + i, 3)}, ++ts, i % 2 == 0), ob::OB_OK);
        engine->flush_incremental();
    }
    const auto summaries = engine->store().book_summaries("BBP", "EX");
    ASSERT_FALSE(summaries.empty()) << "six seals of 3 rows each, a summary every 4 rows: none written";
    const auto book = engine->store().book_at_by_price(UINT64_MAX, "BBP", "EX");
    ASSERT_TRUE(book.from_summary.has_value());
    EXPECT_EQ(*book.from_summary, summaries.back());
    EXPECT_EQ(book_at(*engine, ts), live_book(*engine));
    engine->close();
}

TEST(BookByPriceWithSummaries, AStartRebuildsTheLiveBookFromTheNewestSummary) {
    TempDir dir("ob_bbp_sum_start_");
    std::vector<Level> before;
    {
        auto engine = open_engine(dir.path);
        engine->set_book_summary_rows(2);
        uint64_t ts = kT0;
        for (int i = 0; i < 5; ++i) {
            ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(100 + i, 1), level(99 + i, 0)}, ++ts, false),
                      ob::OB_OK);
            engine->flush_incremental();
        }
        before = live_book(*engine);
        engine->close();
    }
    auto reopened = open_engine(dir.path);
    EXPECT_FALSE(reopened->store().book_summaries("BBP", "EX").empty());
    EXPECT_TRUE(reopened->store().book_at_by_price(UINT64_MAX, "BBP", "EX").from_summary.has_value())
        << "a start composed the book from the first row with a summary beside the segments";
    EXPECT_EQ(live_book(*reopened), before);
    reopened->close();
}

TEST(BookByPriceWithSummaries, AnEventTimeBeforeASummaryFalsifiesIt) {
    TempDir dir("ob_bbp_sum_late_");
    auto engine = open_engine(dir.path);
    engine->set_book_summary_rows(1);
    ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(100, 1)}, kT0 + 100, false), ob::OB_OK);
    engine->flush_incremental();
    ASSERT_EQ(engine->store().book_summaries("BBP", "EX"), (std::vector<uint64_t>{kT0 + 100}));
    // A correction for an earlier instant: history at 100 holds it, which the summary does not.
    ASSERT_EQ(write(*engine, ob::SIDE_BID, {level(99, 4)}, kT0 + 50, false), ob::OB_OK);
    engine->flush_incremental();
    for (uint64_t at : {kT0 + 50, kT0 + 100}) {
        const auto then = book_at(*engine, at);
        ASSERT_FALSE(then.empty());
        EXPECT_EQ(std::get<2>(then.back()), 99) << "the correction at 50 is missing at " << at - kT0;
    }
    engine->close();
}

RC_GTEST_PROP(BookByPriceProperty, WithSummariesHistoryAtEachWriteIsStillTheLiveBook, ()) {
    TempDir dir("ob_bbp_prop_sum_");
    auto engine = open_engine(dir.path);
    engine->set_book_summary_rows(*rc::gen::inRange<uint64_t>(1, 12));
    const size_t writes = *rc::gen::inRange<size_t>(1, 40);
    std::vector<std::pair<uint64_t, std::vector<Level>>> seen;
    uint64_t ts = kT0;
    for (size_t w = 0; w < writes; ++w) {
        ts += *rc::gen::inRange<uint64_t>(1, 1000);
        const uint8_t side = *rc::gen::inRange(0, 2) == 0 ? ob::SIDE_BID : ob::SIDE_ASK;
        const bool replace = *rc::gen::inRange(0, 4) == 0;
        const size_t n = *rc::gen::inRange<size_t>(replace ? 0 : 1, 8);
        std::vector<ob::Level> levels;
        std::map<int64_t, bool> named;
        for (size_t i = 0; i < n; ++i) {
            const int64_t price = *rc::gen::inRange<int64_t>(1, 30);
            if (named.count(price) != 0) continue;
            named[price] = true;
            const uint64_t qty = *rc::gen::inRange(0, 4) == 0 ? 0 : *rc::gen::inRange<uint64_t>(1, 100);
            levels.push_back(level(price, qty, *rc::gen::inRange<uint32_t>(1, 5)));
        }
        RC_ASSERT(write(*engine, side, levels, ts, replace) == ob::OB_OK);
        seen.emplace_back(ts, live_book(*engine));
        if (*rc::gen::inRange(0, 3) == 0) engine->flush_incremental();
    }
    engine->flush_incremental();
    for (const auto& [at, live] : seen) RC_ASSERT(book_at(*engine, at) == live);
    engine->close();
}
