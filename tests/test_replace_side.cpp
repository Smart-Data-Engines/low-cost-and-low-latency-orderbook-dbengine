// A side replacement (MREPLACE; the incremental books of #233): an update that is the whole side.
// Every price the side holds and the update does not name is removed, by a level of quantity 0 the
// engine adds to the update before numbering and logging it - so what the WAL, a replica, a peer and
// history see is an ordinary update, and no record format changes.
//
// The tests read what those consumers read: the live book through `read_book()`, and the records
// back out of the WAL, because a record that held the update without its removals would leave every
// replica and every restart with the prices the replace removed.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/level_payload.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

/// One hour, so no tick but the ones a test asks for moves rows.
constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;

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

/// A price and its quantity; a count of 1 throughout, which the tests do not vary.
using Px = std::pair<int64_t, uint64_t>;

/// One write: its update and the levels it points at.
struct Write {
    ob::DeltaUpdate        update{};
    std::vector<ob::Level> levels;
};

Write make_write(const char* symbol, uint8_t side, const std::vector<Px>& levels, uint64_t ts) {
    Write w;
    std::strncpy(w.update.symbol, symbol, sizeof(w.update.symbol) - 1);
    std::strncpy(w.update.exchange, "EX", sizeof(w.update.exchange) - 1);
    w.update.timestamp_ns = ts;
    w.update.side         = side;
    w.update.n_levels     = static_cast<uint16_t>(levels.size());
    for (const auto& [price, qty] : levels) {
        ob::Level level{};
        level.price = price;
        level.qty   = qty;
        level.cnt   = 1;
        w.levels.push_back(level);
    }
    return w;
}

std::unique_ptr<ob::Engine> open_engine(const std::string& dir) {
    auto engine = std::make_unique<ob::Engine>(dir, kNoAutoFlush);
    engine->open();
    return engine;
}

ob::ob_status_t write(ob::Engine& engine, const char* symbol, uint8_t side,
                  const std::vector<Px>& levels, uint64_t ts) {
    const Write w = make_write(symbol, side, levels, ts);
    return engine.apply_delta(w.update, w.levels.data());
}

ob::ob_status_t replace(ob::Engine& engine, const char* symbol, uint8_t side,
                    const std::vector<Px>& levels, uint64_t ts) {
    const Write w = make_write(symbol, side, levels, ts);
    return engine.replace_side(w.update, w.levels.data());
}

/// One side of the live book, best first, as (price, quantity).
std::vector<Px> side_of(ob::Engine& engine, const char* symbol, uint8_t side) {
    std::vector<Px> out;
    const std::string err = engine.read_book(symbol, "EX", 2000, [&](const ob::QueryResult& r) {
        if (r.side == side) out.emplace_back(r.price, r.quantity);
    });
    if (!err.empty()) ADD_FAILURE() << "read_book: " << err;
    return out;
}

/// The levels of every DELTA record of `symbol` in the WAL, in file order, as (price, quantity).
std::vector<std::vector<Px>> delta_records(const std::string& dir, const char* symbol) {
    std::vector<std::vector<Px>> out;
    std::vector<ob::Level> scratch;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_DELTA) return;
        ob::DeltaUpdate update{};
        std::memcpy(&update, ctx.payload, sizeof(update));
        if (std::strcmp(update.symbol, symbol) != 0) return;
        const ob::Level* levels = ob::levels_from_payload(ctx.payload, update.n_levels, scratch);
        std::vector<Px> record;
        for (uint16_t i = 0; i < update.n_levels; ++i) record.emplace_back(levels[i].price, levels[i].qty);
        out.push_back(std::move(record));
    });
    return out;
}

/// The value of one counter, out of the exposition (pitfall 66).
std::optional<uint64_t> counter_value(const std::string& exposition, const std::string& name) {
    for (size_t at = exposition.find("\n" + name); at != std::string::npos;
         at = exposition.find("\n" + name, at + 1)) {
        const size_t after = at + 1 + name.size();
        if (after >= exposition.size()) continue;
        if (exposition[after] != '{' && exposition[after] != ' ') continue;
        const size_t space = exposition.find(' ', after);
        const size_t eol   = exposition.find('\n', after);
        if (space == std::string::npos || space > eol) continue;
        return std::strtoull(exposition.c_str() + space + 1, nullptr, 10);
    }
    return std::nullopt;
}

constexpr uint64_t kT0 = 1'700'000'000'000'000'000ULL;

}  // namespace

TEST(ReplaceSide, TheSideIsExactlyTheUpdateAndTheOtherSideIsLeftAlone) {
    TempDir dir("ob_replace_side_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(write(*engine, "AAA", ob::SIDE_BID, {{100, 1}, {99, 2}, {98, 3}}, kT0 + 1), ob::OB_OK);
    ASSERT_EQ(write(*engine, "AAA", ob::SIDE_ASK, {{102, 4}, {103, 5}}, kT0 + 2), ob::OB_OK);

    ASSERT_EQ(replace(*engine, "AAA", ob::SIDE_BID, {{101, 5}, {99, 7}}, kT0 + 3), ob::OB_OK);

    EXPECT_EQ(side_of(*engine, "AAA", ob::SIDE_BID), (std::vector<Px>{{101, 5}, {99, 7}}))
        << "the bids are not the replacement: a price it did not name was kept, or one it named lost";
    EXPECT_EQ(side_of(*engine, "AAA", ob::SIDE_ASK), (std::vector<Px>{{102, 4}, {103, 5}}))
        << "replacing the bids touched the asks";
    engine->close();
}

TEST(ReplaceSide, TheRecordIsTheRemovalsBestFirstThenTheUpdate) {
    TempDir dir("ob_replace_record_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(write(*engine, "AAA", ob::SIDE_BID, {{100, 1}, {99, 2}, {98, 3}}, kT0 + 1), ob::OB_OK);
    ASSERT_EQ(write(*engine, "AAA", ob::SIDE_ASK, {{102, 4}, {104, 6}, {103, 5}}, kT0 + 2), ob::OB_OK);
    ASSERT_EQ(replace(*engine, "AAA", ob::SIDE_BID, {{101, 5}, {99, 7}}, kT0 + 3), ob::OB_OK);
    ASSERT_EQ(replace(*engine, "AAA", ob::SIDE_ASK, {{105, 1}}, kT0 + 4), ob::OB_OK);
    engine->close();

    const auto records = delta_records(dir.path, "AAA");
    ASSERT_EQ(records.size(), 4u);
    // The bids' removals best first, which for bids is the highest: 100, then 98.
    EXPECT_EQ(records[2], (std::vector<Px>{{100, 0}, {98, 0}, {101, 5}, {99, 7}}))
        << "the WAL record of a bid replacement is not its removals, highest first, and then its "
           "levels - a replica or a restart would be left with the prices it removed";
    // The asks' best first is the lowest: 102, 103, 104.
    EXPECT_EQ(records[3], (std::vector<Px>{{102, 0}, {103, 0}, {104, 0}, {105, 1}}))
        << "the WAL record of an ask replacement is not its removals, lowest first, and then its levels";
}

TEST(ReplaceSide, NoLevelsEmptiesTheSide) {
    TempDir dir("ob_replace_empty_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(write(*engine, "AAA", ob::SIDE_BID, {{100, 1}, {99, 2}}, kT0 + 1), ob::OB_OK);
    ASSERT_EQ(replace(*engine, "AAA", ob::SIDE_BID, {}, kT0 + 2), ob::OB_OK);
    EXPECT_TRUE(side_of(*engine, "AAA", ob::SIDE_BID).empty()) << "an empty replacement left bids";
    engine->close();
    const auto records = delta_records(dir.path, "AAA");
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[1], (std::vector<Px>{{100, 0}, {99, 0}}));
}

TEST(ReplaceSide, ABookNothingHoldsTakesTheUpdateAsItIs) {
    TempDir dir("ob_replace_fresh_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(replace(*engine, "NEW", ob::SIDE_ASK, {{10, 1}, {11, 2}}, kT0 + 1), ob::OB_OK);
    EXPECT_EQ(side_of(*engine, "NEW", ob::SIDE_ASK), (std::vector<Px>{{10, 1}, {11, 2}}));
    engine->close();
    const auto records = delta_records(dir.path, "NEW");
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0], (std::vector<Px>{{10, 1}, {11, 2}})) << "removals were added to an empty side";
}

TEST(ReplaceSide, AQuantityZeroItNamesIsARemovalItAlreadyCarries) {
    TempDir dir("ob_replace_zero_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(write(*engine, "AAA", ob::SIDE_BID, {{100, 1}, {99, 2}}, kT0 + 1), ob::OB_OK);
    ASSERT_EQ(replace(*engine, "AAA", ob::SIDE_BID, {{101, 5}, {100, 0}}, kT0 + 2), ob::OB_OK);
    EXPECT_EQ(side_of(*engine, "AAA", ob::SIDE_BID), (std::vector<Px>{{101, 5}}));
    engine->close();
    const auto records = delta_records(dir.path, "AAA");
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[1], (std::vector<Px>{{99, 0}, {101, 5}, {100, 0}}))
        << "a price the replacement named with quantity 0 was removed twice, or 99 not at all";
}

TEST(ReplaceSide, InABatchItIsPlannedAfterTheWritesBeforeIt) {
    // The batch is applied in memory after its WAL write, so the replacement has to count the
    // write before it in the same batch: 97 is not in the book when the batch is planned.
    TempDir dir("ob_replace_batch_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(write(*engine, "AAA", ob::SIDE_BID, {{100, 1}}, kT0 + 1), ob::OB_OK);

    const Write before = make_write("AAA", ob::SIDE_BID, {{97, 3}}, kT0 + 2);
    const Write replacement = make_write("AAA", ob::SIDE_BID, {{101, 5}}, kT0 + 3);
    const Write after = make_write("AAA", ob::SIDE_BID, {{95, 9}}, kT0 + 4);
    std::vector<ob::ClientWrite> batch;
    batch.push_back(ob::ClientWrite{&before.update, before.levels.data()});
    ob::ClientWrite r{&replacement.update, replacement.levels.data()};
    r.replace_side = true;
    batch.push_back(r);
    batch.push_back(ob::ClientWrite{&after.update, after.levels.data()});
    std::vector<ob::WriteOutcome> outcomes(batch.size());
    engine->apply_deltas(batch, outcomes);
    for (const auto& o : outcomes) {
        ASSERT_EQ(o.status, ob::OB_OK);
        ASSERT_TRUE(o.error.empty()) << o.error;
    }

    EXPECT_EQ(side_of(*engine, "AAA", ob::SIDE_BID), (std::vector<Px>{{101, 5}, {95, 9}}))
        << "97, written earlier in the batch, survived the replacement - or 95, written after it, "
           "was lost";
    engine->close();
    const auto records = delta_records(dir.path, "AAA");
    ASSERT_EQ(records.size(), 4u);
    EXPECT_EQ(records[2], (std::vector<Px>{{100, 0}, {97, 0}, {101, 5}}));
}

TEST(ReplaceSide, TheCounterCountsTheRemovals) {
    TempDir dir("ob_replace_counter_");
    auto engine = open_engine(dir.path);
    ASSERT_EQ(write(*engine, "AAA", ob::SIDE_BID, {{100, 1}, {99, 2}, {98, 3}}, kT0 + 1), ob::OB_OK);
    ASSERT_EQ(replace(*engine, "AAA", ob::SIDE_BID, {{99, 2}}, kT0 + 2), ob::OB_OK);
    const auto removed =
        counter_value(engine->registry().serialize(), "ob_replace_side_removed_levels_total");
    ASSERT_TRUE(removed.has_value()) << "ob_replace_side_removed_levels_total is not in the exposition";
    EXPECT_EQ(*removed, 2u);
    engine->close();
}

TEST(ReplaceSide, AFullSideReplacedByAnotherFullSideIsOneRecordOfBoth) {
    // The largest record a replacement makes: the book's 1000 prices removed and 1000 new ones
    // written, 2000 levels in one DELTA - inside the record's 16-bit length (pitfall 44), which
    // 2000 levels of 24 bytes and the update's header are.
    TempDir dir("ob_replace_full_");
    auto engine = open_engine(dir.path);
    std::vector<Px> held, fresh;
    for (int64_t i = 0; i < static_cast<int64_t>(ob::MAX_LEVELS); ++i) {
        held.emplace_back(100'000 - i, 1);
        fresh.emplace_back(50'000 - i, 2);
    }
    ASSERT_EQ(write(*engine, "BIG", ob::SIDE_BID, held, kT0 + 1), ob::OB_OK);
    ASSERT_EQ(side_of(*engine, "BIG", ob::SIDE_BID).size(), ob::MAX_LEVELS);
    ASSERT_EQ(replace(*engine, "BIG", ob::SIDE_BID, fresh, kT0 + 2), ob::OB_OK);
    EXPECT_EQ(side_of(*engine, "BIG", ob::SIDE_BID), fresh)
        << "the side is not exactly the replacement after a full side replaced a full side";
    engine->close();

    const auto records = delta_records(dir.path, "BIG");
    ASSERT_EQ(records.size(), 2u);
    ASSERT_EQ(records[1].size(), 2 * ob::MAX_LEVELS);
    EXPECT_EQ(records[1].front(), (Px{100'000, 0})) << "the removals do not start at the best";
    EXPECT_EQ(records[1][ob::MAX_LEVELS - 1], (Px{100'000 - static_cast<int64_t>(ob::MAX_LEVELS) + 1, 0}));
    EXPECT_EQ(records[1][ob::MAX_LEVELS], fresh.front()) << "the update does not follow its removals";
    EXPECT_EQ(records[1].back(), fresh.back());
}

TEST(ReplaceSide, ARestartFromTheWalHoldsTheSameBook) {
    // What a crash leaves: the directory as it is, read by a new engine - which replays the WAL's
    // records into the live book, removals included.
    TempDir dir("ob_replace_restart_");
    TempDir copy("ob_replace_restart_copy_");
    std::vector<Px> bids_before;
    {
        auto engine = open_engine(dir.path);
        ASSERT_EQ(write(*engine, "AAA", ob::SIDE_BID, {{100, 1}, {99, 2}, {98, 3}}, kT0 + 1), ob::OB_OK);
        ASSERT_EQ(replace(*engine, "AAA", ob::SIDE_BID, {{101, 5}, {98, 4}}, kT0 + 2), ob::OB_OK);
        bids_before = side_of(*engine, "AAA", ob::SIDE_BID);
        fs::copy(dir.path, copy.path, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
        engine->close();
    }
    ASSERT_EQ(bids_before, (std::vector<Px>{{101, 5}, {98, 4}}));
    auto reopened = open_engine(copy.path);
    EXPECT_EQ(side_of(*reopened, "AAA", ob::SIDE_BID), bids_before)
        << "a restart from the WAL brought back a price the replacement removed";
    reopened->close();
}
