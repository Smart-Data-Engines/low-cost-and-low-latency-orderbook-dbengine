// The seal's sync in the background (#190).
//
// At the write ceiling a flush tick waited for the syncfs() of what it sealed - 1.2 - 2.7 s at the
// median on a device slower than the ingest, 9.6 s at worst - before it drained the queue again, and
// the queue lasts about half a second there, so writers waited seconds and were refused past five.
// The tick hands the sync to a thread of its own now, with the checkpoint's claim frozen when it asks,
// and the next tick appends that checkpoint once the sync is done. These tests hold a sync on their
// own schedule and check what the tick does meanwhile, what the checkpoint claims, that a FLUSH waits
// for it, and that a restart before it came keeps every row once.

#include "orderbook/data_model.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    TempDir()
        : path((fs::temp_directory_path() /
                ("ob_seal_sync_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1)))).string()) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;
/// Updates of 1000 levels that make a store due for its seal: past kSealRows.
constexpr uint64_t kDueUpdates = ob::Engine::kSealRows / 1000 + 1;

void insert(ob::Engine& engine, const char* symbol, uint64_t seq, size_t levels) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.timestamp_ns = 1'790'000'000'000'000'000ULL + seq;
    delta.side         = ob::SIDE_BID;
    delta.n_levels     = static_cast<uint16_t>(levels);
    std::vector<ob::Level> lv(levels);
    for (size_t i = 0; i < levels; ++i) {
        lv[i] = ob::Level{};
        lv[i].price = static_cast<int64_t>(1000 + i);
        lv[i].qty   = 1;
        lv[i].cnt   = 1;
    }
    ASSERT_EQ(engine.apply_delta(delta, lv.data()), ob::OB_OK);
}

void due(ob::Engine& engine, const char* symbol, uint64_t first_seq = 1) {
    for (uint64_t i = 0; i < kDueUpdates; ++i) insert(engine, symbol, first_seq + i, 1000);
}

int rows(ob::Engine& engine, const char* symbol) {
    int n = 0;
    const std::string err = engine.execute(std::string("SELECT * FROM '") + symbol + "'.'EX'",
                                           [&n](const ob::QueryResult&) { ++n; });
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) ADD_FAILURE() << err;
    return n;
}

template <typename F>
bool eventually(F&& done, std::chrono::milliseconds within = 10000ms) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return done();
}

std::vector<uint64_t> epochs_on_disk(const std::string& dir, const std::string& symbol) {
    std::vector<uint64_t> out;
    const std::regex field("\"seal_epoch\":([0-9]+)");
    std::error_code ec;
    for (auto& e : fs::recursive_directory_iterator(dir, ec)) {
        if (e.path().filename() != "meta.json") continue;
        if (e.path().string().find("/" + symbol + "/") == std::string::npos) continue;
        std::ifstream in(e.path());
        std::stringstream ss;
        ss << in.rdbuf();
        std::smatch m;
        const std::string text = ss.str();
        if (std::regex_search(text, m, field)) out.push_back(std::stoull(m[1].str()));
    }
    return out;
}

ob::WALReplayer::LastCheckpoint last_checkpoint(const std::string& dir) {
    return ob::WALReplayer(dir).find_last_checkpoint();
}

std::unique_ptr<ob::Engine> engine_at(const std::string& dir) {
    auto engine = std::make_unique<ob::Engine>(dir, kNoAutoFlush, ob::FsyncPolicy::INTERVAL);
    engine->open();
    return engine;
}

}  // namespace

TEST(SealSyncBackground, ATickDrainsTheQueueWhileTheLastSealsSyncRuns) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    engine->hold_seal_syncs_from_for_test(1);   // the device takes its time over every one
    due(*engine, "A");
    engine->flush_tick_leaving_the_sync_for_test();
    ASSERT_TRUE(engine->seal_sync_busy_for_test()) << "the tick sealed nothing, or synced it itself";
    for (uint64_t i = 1; i <= 50; ++i) insert(*engine, "B", i, 10);

    // The next tick drains B while A's sync is held - it waited for that sync until #190.
    auto tick = std::async(std::launch::async, [&] { engine->flush_tick_leaving_the_sync_for_test(); });
    const bool finished = tick.wait_for(10s) == std::future_status::ready;
    engine->hold_seal_syncs_from_for_test(UINT64_MAX);
    tick.wait();
    ASSERT_TRUE(finished) << "the tick waited for the last tick's seal sync";
    EXPECT_EQ(engine->registry().gauge_value("ob_pending_rows"), 0) << "B was not drained";
    EXPECT_EQ(rows(*engine, "B"), 500);

    ASSERT_TRUE(eventually([&] { return !engine->seal_sync_busy_for_test(); }));
    engine->flush_tick_leaving_the_sync_for_test();   // takes the finished sync in
    EXPECT_GT(last_checkpoint(dir.path).ordinal, 0u) << "the finished sync's checkpoint never came";
    engine->close();
}

TEST(SealSyncBackground, NoCheckpointClaimsWhatItsSyncHasNotCovered) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    engine->hold_seal_syncs_from_for_test(1);
    for (uint64_t i = 1; i <= 7; ++i) insert(*engine, "B", i, 1);   // waiting throughout: epoch form
    due(*engine, "A");
    engine->flush_tick_leaving_the_sync_for_test();   // A sealed; its sync held
    due(*engine, "C", 1000);
    engine->flush_tick_leaving_the_sync_for_test();   // C sealed while A's sync runs; its own asked
    EXPECT_EQ(last_checkpoint(dir.path).ordinal, 0u)
        << "a checkpoint was appended before any seal's sync was done";

    // A's sync done, C's held: the checkpoint that follows is A's, and says nothing of C.
    engine->hold_seal_syncs_from_for_test(2);
    ASSERT_TRUE(eventually([&] { return engine->seal_syncs_finished_for_test() >= 1; }));
    engine->flush_tick_leaving_the_sync_for_test();
    auto last = last_checkpoint(dir.path);
    ASSERT_TRUE(last.seal_epoch.has_value()) << "no epoch checkpoint after A's sync";
    const auto a = epochs_on_disk(dir.path, "A");
    const auto c = epochs_on_disk(dir.path, "C");
    ASSERT_FALSE(a.empty());
    ASSERT_FALSE(c.empty());
    for (uint64_t e : a) EXPECT_LE(e, *last.seal_epoch) << "A's seal is past the checkpoint of its sync";
    for (uint64_t e : c) {
        EXPECT_GT(e, *last.seal_epoch) << "the checkpoint of A's sync vouches for C, sealed after it began";
    }

    // And C's once its own sync is done.
    engine->hold_seal_syncs_from_for_test(UINT64_MAX);
    ASSERT_TRUE(eventually([&] { return engine->seal_syncs_finished_for_test() >= 2; }));
    engine->flush_tick_leaving_the_sync_for_test();
    last = last_checkpoint(dir.path);
    ASSERT_TRUE(last.seal_epoch.has_value());
    for (uint64_t e : c) EXPECT_LE(e, *last.seal_epoch) << "C's sync came and its checkpoint did not";
    engine->close();
}

TEST(SealSyncBackground, AFlushWaitsForTheBackgroundSyncAndItsCheckpointIsTheLast) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    engine->hold_seal_syncs_from_for_test(1);
    for (uint64_t i = 1; i <= 7; ++i) insert(*engine, "B", i, 1);
    due(*engine, "A");
    engine->flush_tick_leaving_the_sync_for_test();

    auto flush = std::async(std::launch::async, [&] { engine->flush_incremental(); });
    const bool waited = flush.wait_for(300ms) == std::future_status::timeout;
    engine->hold_seal_syncs_from_for_test(UINT64_MAX);
    flush.get();
    EXPECT_TRUE(waited) << "FLUSH answered before the background sync it has to come after";
    const auto last = last_checkpoint(dir.path);
    EXPECT_FALSE(last.seal_epoch.has_value())
        << "the last checkpoint is not the FLUSH's, which leaves nothing waiting";
    EXPECT_TRUE(last.covered.has_value());
    engine->close();
}

TEST(SealSyncBackground, ARestartBeforeTheSyncCameKeepsEveryRowOnce) {
    // A seal written and its sync never done: nothing claims its segments, so a start removes them
    // and replays their rows from the WAL (#160) - once each.
    TempDir dir;
    TempDir copy;
    {
        auto engine = engine_at(dir.path);
        engine->hold_seal_syncs_from_for_test(1);
        for (uint64_t i = 1; i <= 7; ++i) insert(*engine, "B", i, 1);
        due(*engine, "A");
        engine->flush_tick_leaving_the_sync_for_test();
        ASSERT_FALSE(epochs_on_disk(dir.path, "A").empty()) << "A was not sealed";
        // What a crash here would leave: the files as they are, no checkpoint for A's seal.
        fs::copy(dir.path, copy.path, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
        engine->hold_seal_syncs_from_for_test(UINT64_MAX);
        engine->close();
    }
    auto reopened = engine_at(copy.path);
    EXPECT_EQ(rows(*reopened, "A"), static_cast<int>(kDueUpdates * 1000)) << "rows lost or doubled";
    EXPECT_EQ(rows(*reopened, "B"), 7);
    reopened->close();
}
