// #165 part 2a: a flush tick writes a segment for a store only when it is due.
//
// A tick used to write a segment for every store that had received a row: at 256 symbols about two
// thousand files and a syncfs() every tick, 84.7% of the flush thread's CPU in the kernel, and a node
// gained a segment per active symbol per tick. The rows now wait in published blocks - readable -
// until the store has enough of them, or they are old enough, or every store together is over a
// budget. These hold the policy as a function of its inputs, and the engine's wiring of it: a tick
// seals only what is due, FLUSH and close() seal everything, and a query reads rows either way.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    TempDir()
        : path((fs::temp_directory_path() /
                ("ob_lazy_flush_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1)))).string()) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

ob::Engine::SealCandidate candidate(size_t rows, Clock::duration age, Clock::time_point now) {
    return ob::Engine::SealCandidate{rows, now - age};
}

std::vector<size_t> indices(const std::vector<ob::Engine::SealPick>& picks) {
    std::vector<size_t> out;
    for (const auto& p : picks) out.push_back(p.index);
    return out;
}

void insert(ob::Engine& engine, const char* symbol, uint64_t first_seq, size_t levels) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.sequence_number = first_seq;
    delta.timestamp_ns    = 1'790'000'000'000'000'000ULL + first_seq;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = static_cast<uint16_t>(levels);
    std::vector<ob::Level> lv(levels);
    for (size_t i = 0; i < levels; ++i) {
        lv[i] = ob::Level{};
        lv[i].price = static_cast<int64_t>(1000 + i);
        lv[i].qty   = 1;
        lv[i].cnt   = 1;
    }
    ASSERT_EQ(engine.apply_delta(delta, lv.data()), ob::OB_OK);
}

size_t segments_on_disk(const std::string& dir, const std::string& symbol = "") {
    size_t n = 0;
    std::error_code ec;
    for (auto& e : fs::recursive_directory_iterator(dir, ec)) {
        if (e.path().filename() != "meta.json") continue;
        if (!symbol.empty() && e.path().string().find("/" + symbol + "/") == std::string::npos) continue;
        ++n;
    }
    return n;
}

size_t rows_answered(ob::Engine& engine, const char* symbol) {
    size_t n = 0;
    const std::string err = engine.execute(
        std::string("SELECT * FROM '") + symbol + "'.'EX'",
        [&](const ob::QueryResult&) { ++n; });
    EXPECT_TRUE(err.empty()) << err;
    return n;
}

/// Poll until the predicate holds or the deadline passes; the ticks run on their own thread.
template <typename F>
bool eventually(F&& done, std::chrono::milliseconds within = 5000ms) {
    const auto deadline = Clock::now() + within;
    while (Clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return done();
}

}  // namespace

// ── The policy ───────────────────────────────────────────────────────────────

TEST(SealPolicy, NothingIsDueBelowTheRowsAndTheAge) {
    const auto now = Clock::now();
    EXPECT_TRUE(ob::Engine::pick_seals({candidate(ob::Engine::kSealRows - 1, 1s, now)}, now, false)
                    .empty());
}

TEST(SealPolicy, EnoughRowsOrEnoughAgeIsDue) {
    const auto now = Clock::now();
    const auto picks = ob::Engine::pick_seals(
        {candidate(ob::Engine::kSealRows, 0s, now), candidate(1, ob::Engine::kSealAge, now),
         candidate(1, ob::Engine::kSealAge - 1ms, now)},
        now, false);
    ASSERT_EQ(indices(picks), (std::vector<size_t>{0, 1}));
    EXPECT_EQ(picks[0].why, ob::Engine::SealReason::kRows);
    EXPECT_EQ(picks[1].why, ob::Engine::SealReason::kAge);
}

TEST(SealPolicy, AtMostTheLimitATickOldestFirst) {
    // A thousand symbols that started together would otherwise all seal in one tick.
    const auto now = Clock::now();
    std::vector<ob::Engine::SealCandidate> c;
    for (size_t i = 0; i < ob::Engine::kSealsPerTick + 10; ++i) {
        c.push_back(candidate(1, ob::Engine::kSealAge + std::chrono::seconds(100 - i), now));
    }
    const auto picks = ob::Engine::pick_seals(c, now, false);
    ASSERT_EQ(picks.size(), ob::Engine::kSealsPerTick);
    for (size_t i = 0; i < picks.size(); ++i) EXPECT_EQ(picks[i].index, i);
}

TEST(SealPolicy, OverTheBudgetTheOldestAreSealedPastTheLimit) {
    // Neither due by rows nor by age, and together over the budget: sealed oldest first until they
    // are under it, however many that is - memory is what the budget bounds.
    const auto now = Clock::now();
    constexpr size_t kEach = 30'000;
    const size_t stores = ob::Engine::kUnsealedRowsBudget / kEach * 3 / 2;
    std::vector<ob::Engine::SealCandidate> c;
    for (size_t i = 0; i < stores; ++i) c.push_back(candidate(kEach, 1s, now));
    const auto picks = ob::Engine::pick_seals(c, now, false);
    const size_t left = (stores - picks.size()) * kEach;
    EXPECT_LE(left, ob::Engine::kUnsealedRowsBudget);
    EXPECT_GT(left + kEach, ob::Engine::kUnsealedRowsBudget) << "sealed more than the budget asked";
    EXPECT_GT(picks.size(), ob::Engine::kSealsPerTick) << "the per-tick limit held the budget back";
    for (size_t i = 0; i < picks.size(); ++i) {
        EXPECT_EQ(picks[i].index, i);
        EXPECT_EQ(picks[i].why, ob::Engine::SealReason::kBudget);
    }
}

TEST(SealPolicy, SealAllTakesEveryOne) {
    const auto now = Clock::now();
    const auto picks = ob::Engine::pick_seals(
        {candidate(1, 0s, now), candidate(2, 0s, now), candidate(3, 0s, now)}, now, true);
    ASSERT_EQ(indices(picks), (std::vector<size_t>{0, 1, 2}));
    for (const auto& p : picks) EXPECT_EQ(p.why, ob::Engine::SealReason::kAll);
}

// ── The engine's wiring ──────────────────────────────────────────────────────

TEST(LazyFlush, ATickSealsNothingThatIsNotDueAndAQueryReadsTheRows) {
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    insert(engine, "A", 1, 10);
    ASSERT_TRUE(eventually([&] { return engine.registry().gauge_value("ob_unsealed_rows") == 10; }))
        << "no tick drained the rows into a block";
    EXPECT_EQ(segments_on_disk(dir.path), 0u) << "a tick wrote a segment nothing made due";
    EXPECT_EQ(rows_answered(engine, "A"), 10u);
    engine.close();
}

TEST(LazyFlush, FlushSealsEverythingAndTheRowsAnswerOnce) {
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    insert(engine, "A", 1, 10);
    insert(engine, "B", 1, 5);
    ASSERT_TRUE(eventually([&] { return engine.registry().gauge_value("ob_unsealed_rows") == 15; }));
    engine.flush_incremental();
    EXPECT_EQ(engine.registry().gauge_value("ob_unsealed_rows"), 0);
    EXPECT_EQ(segments_on_disk(dir.path, "A"), 1u);
    EXPECT_EQ(segments_on_disk(dir.path, "B"), 1u);
    EXPECT_EQ(rows_answered(engine, "A"), 10u) << "a sealed row was answered twice or not at all";
    EXPECT_EQ(rows_answered(engine, "B"), 5u);
    engine.close();
}

TEST(LazyFlush, AStoreWithEnoughRowsIsSealedByATickAndTheOthersWait) {
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    size_t a_rows = 0;
    for (uint64_t seq = 1; a_rows < ob::Engine::kSealRows; ++seq) {
        insert(engine, "A", seq, 1000);
        a_rows += 1000;
    }
    insert(engine, "B", 1, 7);
    ASSERT_TRUE(eventually([&] { return segments_on_disk(dir.path, "A") > 0; }))
        << "a store over the rows threshold was not sealed";
    ASSERT_TRUE(eventually([&] { return engine.registry().gauge_value("ob_unsealed_rows") == 7; }));
    EXPECT_EQ(segments_on_disk(dir.path, "B"), 0u);
    EXPECT_GE(engine.registry().counter_value("ob_seals_total"), 1u);
    EXPECT_EQ(rows_answered(engine, "A"), a_rows);
    EXPECT_EQ(rows_answered(engine, "B"), 7u);
    engine.close();
}

TEST(LazyFlush, CloseSealsEverythingAndAReopenedNodeHasEveryRow) {
    TempDir dir;
    {
        ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        insert(engine, "A", 1, 10);
        ASSERT_TRUE(eventually([&] { return engine.registry().gauge_value("ob_unsealed_rows") == 10; }));
        engine.close();
    }
    EXPECT_EQ(segments_on_disk(dir.path, "A"), 1u);
    ob::Engine reopened(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    reopened.open();
    EXPECT_EQ(rows_answered(reopened, "A"), 10u);
    EXPECT_EQ(reopened.registry().gauge_value("ob_unsealed_rows"), 0);
    reopened.close();
}

TEST(LazyFlush, ABlocksRangeHoldsRowsThatArrivedOutOfTimeOrder) {
    // The drain computes a block's range while it collects the rows, rather than walking them again
    // afterwards (#165 part 2a). A row that is neither the drain's first nor in time order has to
    // be inside it, or a query of its time skips the block - and, once sealed, the segment, whose
    // range the seal takes from the block.
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    constexpr uint64_t kT = 1'790'000'000'000'000'000ULL;
    const std::vector<uint64_t> times = {kT + 5'000'000'000ULL, kT, kT + 10'000'000'000ULL};
    constexpr size_t kLevels = 4;
    std::vector<ob::DeltaUpdate> deltas(times.size());
    std::vector<std::vector<ob::Level>> levels(times.size(), std::vector<ob::Level>(kLevels));
    std::vector<ob::ClientWrite> writes;
    for (size_t i = 0; i < times.size(); ++i) {
        deltas[i] = ob::DeltaUpdate{};
        std::strncpy(deltas[i].symbol, "A", sizeof(deltas[i].symbol) - 1);
        std::strncpy(deltas[i].exchange, "EX", sizeof(deltas[i].exchange) - 1);
        deltas[i].sequence_number = i + 1;
        deltas[i].timestamp_ns    = times[i];
        deltas[i].side            = ob::SIDE_BID;
        deltas[i].n_levels        = static_cast<uint16_t>(kLevels);
        for (size_t l = 0; l < kLevels; ++l) {
            levels[i][l] = ob::Level{};
            levels[i][l].price = static_cast<int64_t>(1000 + l);
            levels[i][l].qty   = 1;
            levels[i][l].cnt   = 1;
        }
        writes.push_back(ob::ClientWrite{&deltas[i], levels[i].data()});
    }
    // One batch is queued under one hold of the engine's lock, so one drain takes all of it: one
    // block, its rows out of time order.
    std::vector<ob::WriteOutcome> outcomes(writes.size());
    engine.apply_deltas(writes, outcomes);
    for (const auto& o : outcomes) ASSERT_EQ(o.status, ob::OB_OK) << o.error;
    ASSERT_TRUE(eventually([&] {
        return engine.registry().gauge_value("ob_unsealed_rows") ==
               static_cast<int64_t>(times.size() * kLevels);
    }));
    const auto rows_at = [&](uint64_t ts) {
        size_t n = 0;
        const std::string err = engine.execute(
            "SELECT * FROM 'A'.'EX' WHERE timestamp BETWEEN " + std::to_string(ts) + " AND " +
                std::to_string(ts),
            [&](const ob::QueryResult&) { ++n; });
        EXPECT_TRUE(err.empty()) << err;
        return n;
    };
    for (uint64_t ts : times) EXPECT_EQ(rows_at(ts), kLevels) << "in a block, at " << ts;
    engine.flush_incremental();
    EXPECT_EQ(engine.registry().gauge_value("ob_unsealed_rows"), 0);
    for (uint64_t ts : times) EXPECT_EQ(rows_at(ts), kLevels) << "sealed, at " << ts;
    engine.close();
}

TEST(LazyFlush, ANodeWithRowsOnlyInBlocksHoldsData) {
    // holds_no_data() decides whether a replica may be bootstrapped over what it has; a node whose
    // rows wait in blocks has them.
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    EXPECT_TRUE(engine.holds_no_data());
    insert(engine, "A", 1, 3);
    ASSERT_TRUE(eventually([&] { return engine.registry().gauge_value("ob_unsealed_rows") == 3; }));
    EXPECT_FALSE(engine.holds_no_data());
    engine.close();
}
