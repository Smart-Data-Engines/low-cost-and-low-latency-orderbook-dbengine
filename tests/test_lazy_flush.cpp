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
    EXPECT_TRUE(ob::Engine::pick_seals({candidate(ob::Engine::kSealRows - 1, 1s, now)}, now, false,
                                       ob::Engine::kNoRowLimit)
                    .empty());
}

TEST(SealPolicy, EnoughRowsOrEnoughAgeIsDue) {
    const auto now = Clock::now();
    const auto picks = ob::Engine::pick_seals(
        {candidate(ob::Engine::kSealRows, 0s, now), candidate(1, ob::Engine::kSealAge, now),
         candidate(1, ob::Engine::kSealAge - 1ms, now)},
        now, false, ob::Engine::kNoRowLimit);
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
    const auto picks = ob::Engine::pick_seals(c, now, false, ob::Engine::kNoRowLimit);
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
    // A tick that drained nothing, whose share is kSealRows - a couple of these stores: the budget
    // is memory, and no share holds it back.
    const auto picks = ob::Engine::pick_seals(c, now, false, 0);
    const size_t left = (stores - picks.size()) * kEach;
    EXPECT_LE(left, ob::Engine::kUnsealedRowsBudget);
    EXPECT_GT(left + kEach, ob::Engine::kUnsealedRowsBudget) << "sealed more than the budget asked";
    EXPECT_GT(picks.size(), ob::Engine::kSealsPerTick) << "the per-tick limit held the budget back";
    for (size_t i = 0; i < picks.size(); ++i) {
        EXPECT_EQ(picks[i].index, i);
        EXPECT_EQ(picks[i].why, ob::Engine::SealReason::kBudget);
    }
}

TEST(SealPolicy, ATickTakesItsShareOfWhatIsDueOldestFirst) {
    // Sixteen stores that came due together, and a tick that drained a million rows: it seals about
    // that many, and the rest wait for the next tick rather than doubling this one.
    const auto now = Clock::now();
    std::vector<ob::Engine::SealCandidate> c;
    for (size_t i = 0; i < 16; ++i) c.push_back(candidate(125'000, std::chrono::milliseconds(100 - i), now));
    const auto picks = ob::Engine::pick_seals(c, now, false, 1'000'000);
    ASSERT_EQ(indices(picks), (std::vector<size_t>{0, 1, 2, 3, 4, 5, 6, 7}));
    for (const auto& p : picks) EXPECT_EQ(p.why, ob::Engine::SealReason::kRows);
}

TEST(SealPolicy, ATickThatDrainedLittleStillSealsASealsWorthOfSmallStoresDueByAge) {
    // Its share is kSealRows, not the rows it drained: 256 symbols trickling rows came due by age
    // together in the soak, and a share of what a quiet tick drains would seal two of them a tick.
    const auto now = Clock::now();
    constexpr size_t kEach = 4'000;
    std::vector<ob::Engine::SealCandidate> c;
    for (size_t i = 0; i < 20; ++i) c.push_back(candidate(kEach, ob::Engine::kSealAge + 1s, now));
    const auto picks = ob::Engine::pick_seals(c, now, false, 1'000);
    EXPECT_EQ(picks.size(), ob::Engine::kSealRows / kEach);
    for (const auto& p : picks) EXPECT_EQ(p.why, ob::Engine::SealReason::kAge);
}

TEST(SealPolicy, TheOldestDueStoreIsTakenWhateverItsRows) {
    // Bigger than the share on its own - and taken, or it would wait behind a limit forever.
    const auto now = Clock::now();
    const auto picks = ob::Engine::pick_seals(
        {candidate(3'000'000, 2s, now), candidate(ob::Engine::kSealRows, 1s, now)}, now, false,
        1'000'000);
    ASSERT_EQ(indices(picks), (std::vector<size_t>{0}));
}

TEST(SealPolicy, AYoungerStoreThatFitsTheShareIsTakenPastAnOlderOneThatDoesNot) {
    const auto now = Clock::now();
    const auto picks = ob::Engine::pick_seals(
        {candidate(600'000, 3s, now), candidate(600'000, 2s, now), candidate(300'000, 1s, now)}, now,
        false, 1'000'000);
    ASSERT_EQ(indices(picks), (std::vector<size_t>{0, 2}));
}

TEST(SealPolicy, SealAllTakesEveryOne) {
    const auto now = Clock::now();
    const auto picks = ob::Engine::pick_seals(
        {candidate(1, 0s, now), candidate(2, 0s, now), candidate(3, 0s, now)}, now, true, 1);
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

/// `writes` updates of `levels` levels to each of `symbols`, as one batch - queued under one hold of
/// the engine's lock, so one drain takes all of it.
void write_batch(ob::Engine& engine, const std::vector<const char*>& symbols, size_t writes,
                 size_t levels, uint64_t& seq) {
    std::vector<ob::Level> lv(levels);
    for (size_t l = 0; l < levels; ++l) {
        lv[l] = ob::Level{};
        lv[l].price = static_cast<int64_t>(1000 + l);
        lv[l].qty   = 1;
        lv[l].cnt   = 1;
    }
    std::vector<ob::DeltaUpdate> deltas;
    deltas.reserve(symbols.size() * writes);
    for (const char* s : symbols) {
        for (size_t w = 0; w < writes; ++w) {
            ob::DeltaUpdate d{};
            std::strncpy(d.symbol, s, sizeof(d.symbol) - 1);
            std::strncpy(d.exchange, "EX", sizeof(d.exchange) - 1);
            d.sequence_number = ++seq;
            d.timestamp_ns    = 1'790'000'000'000'000'000ULL + seq;
            d.side            = ob::SIDE_BID;
            d.n_levels        = static_cast<uint16_t>(levels);
            deltas.push_back(d);
        }
    }
    std::vector<ob::ClientWrite> batch;
    for (const auto& d : deltas) batch.push_back(ob::ClientWrite{&d, lv.data()});
    std::vector<ob::WriteOutcome> outcomes(batch.size());
    engine.apply_deltas(batch, outcomes);
    for (const auto& o : outcomes) ASSERT_EQ(o.status, ob::OB_OK) << o.error;
}

/// Every value `ob_seals_total` takes until it reaches `until`, read every 2 ms - for ticks a fifth
/// of a second apart, which is every value a tick leaves.
std::vector<uint64_t> seal_counts_until(ob::Engine& engine, uint64_t until) {
    std::vector<uint64_t> seen{engine.registry().counter_value("ob_seals_total")};
    const auto deadline = Clock::now() + 10s;
    while (seen.back() < until && Clock::now() < deadline) {
        const uint64_t now_sealed = engine.registry().counter_value("ob_seals_total");
        if (now_sealed != seen.back()) seen.push_back(now_sealed);
        std::this_thread::sleep_for(2ms);
    }
    return seen;
}

TEST(LazyFlush, StoresThatComeDueTogetherAreSealedOverSeveralTicks) {
    // Three stores take 40 000 rows each in one tick and as many in the next, so all three come due
    // in the second with 80 000 rows each. That tick drained 120 000 rows, so it seals one store and
    // leaves two for later ticks - one each, since a tick that drains nothing still seals
    // `kSealRows` - rather than 240 000 rows at once.
    TempDir dir;
    ob::Engine engine(dir.path, 200'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    constexpr size_t kLevels = 1000;
    const std::vector<const char*> symbols = {"A", "B", "C"};
    uint64_t seq = 0;
    write_batch(engine, symbols, 40, kLevels, seq);
    ASSERT_TRUE(eventually([&] {
        return engine.registry().gauge_value("ob_unsealed_rows") ==
               static_cast<int64_t>(3 * 40 * kLevels);
    })) << "the first batch was not drained into blocks";
    ASSERT_EQ(engine.registry().counter_value("ob_seals_total"), 0u) << "a store was due too soon";
    write_batch(engine, symbols, 40, kLevels, seq);
    EXPECT_EQ(seal_counts_until(engine, 3), (std::vector<uint64_t>{0, 1, 2, 3}))
        << "stores that came due together were not spread over ticks";
    for (const char* s : symbols) {
        EXPECT_EQ(segments_on_disk(dir.path, s), 1u) << s;
        EXPECT_EQ(rows_answered(engine, s), 80 * kLevels) << s;
    }
    engine.close();
}

TEST(LazyFlush, ATickSealsAsManyDueRowsAsItDrained) {
    // The share is what the tick drained: three stores due with 80 000 rows each, all from the one
    // tick that drained 240 000, are sealed by it together - a tick that sealed a store and waited
    // would fall behind the writers it drains.
    TempDir dir;
    ob::Engine engine(dir.path, 200'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    constexpr size_t kLevels = 1000;
    const std::vector<const char*> symbols = {"A", "B", "C"};
    uint64_t seq = 0;
    write_batch(engine, symbols, 80, kLevels, seq);
    EXPECT_EQ(seal_counts_until(engine, 3), (std::vector<uint64_t>{0, 3}))
        << "a tick sealed less than it drained";
    for (const char* s : symbols) EXPECT_EQ(segments_on_disk(dir.path, s), 1u) << s;
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
