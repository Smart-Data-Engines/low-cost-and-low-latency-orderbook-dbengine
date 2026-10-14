// #190, step 9: the engine's half of the WAL budget - a writer waits for a sync once the WAL waiting
// for one reaches the budget, under `--fsync-policy interval` only, and every sync tells the budget
// what the device took.
//
// The budget's own arithmetic is `tests/test_sync_budget.cpp`. Here, without a slow device: a sync
// held open by the test seam, and the bytes a writer has to append to reach the initial budget. What
// the budget does on a device slower than the ingest is measured, not tested
// (evidence/2026-10-14-write-ceiling-slower/ before it).

#include "orderbook/engine.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

using std::chrono::milliseconds;

constexpr uint64_t kAnHour = 3'600'000'000'000ULL;   // no tick but the ones asked for
constexpr uint64_t MiB = 1ull << 20;

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

uint64_t budget_waits(ob::Engine& engine) {
    const auto v = counter_value(engine.registry().serialize(), "ob_writer_wal_budget_waits_total");
    EXPECT_TRUE(v.has_value()) << "ob_writer_wal_budget_waits_total is not in the exposition";
    return v.value_or(0);
}

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix)
        : path((fs::temp_directory_path() / (prefix + std::to_string(std::rand()))).string()) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

/// One delta of a thousand levels on BUD.EX: about 24 kB of WAL.
ob::ob_status_t write(ob::Engine& engine, uint64_t n) {
    std::vector<ob::Level> lv(1000);
    for (size_t l = 0; l < lv.size(); ++l) {
        lv[l] = {10'000'000 - static_cast<int64_t>(l), 100 + l, 1, 0};
    }
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol,   "BUD", sizeof(delta.symbol)   - 1);
    std::strncpy(delta.exchange, "EX",  sizeof(delta.exchange) - 1);
    delta.timestamp_ns = 1'700'000'000'000'000'000ULL + n;
    delta.side         = ob::SIDE_BID;
    delta.n_levels     = static_cast<uint16_t>(lv.size());
    return engine.apply_delta(delta, lv.data());
}

/// Enough deltas to take the WAL waiting for a sync well past the initial 4 MiB.
constexpr uint64_t kPastTheBudget = 400;   // ~9.8 MB

}  // namespace

TEST(WalBudget, AWriterPastTheBudgetWaitsForASyncAndGoesOnOnceItIsDone) {
    TempDir dir("ob_wal_budget_wait_");
    ob::Engine engine(dir.path, kAnHour, ob::FsyncPolicy::INTERVAL);
    engine.open();
    ASSERT_EQ(engine.sync_budget_for_test().bytes(), 4 * MiB);

    // The first sync is held until the test lets it go; the ones after it run.
    std::mutex m;
    std::condition_variable cv;
    bool entered = false, released = false;
    std::atomic<int> syncs{0};
    engine.while_syncing_wal_for_test([&] {
        if (syncs.fetch_add(1) != 0) return;
        std::unique_lock<std::mutex> lock(m);
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return released; });
    });

    std::atomic<uint64_t> written{0};
    std::atomic<int> refused{0};
    std::thread writer([&] {
        for (uint64_t i = 0; i < kPastTheBudget; ++i) {
            if (write(engine, i) != ob::OB_OK) refused.fetch_add(1);
            written.fetch_add(1);
        }
    });

    {
        std::unique_lock<std::mutex> lock(m);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] { return entered; }))
            << "no sync began: the writer never waited for one, and the flush is an hour away";
    }
    // The writer asked for the sync it is waiting for, and waits while it is held.
    const uint64_t held_at = written.load();
    std::this_thread::sleep_for(milliseconds(300));
    EXPECT_EQ(written.load(), held_at) << "the writer went on while the sync it waits for was held";
    EXPECT_LT(held_at, kPastTheBudget);
    EXPECT_GE(budget_waits(engine), 1u);

    {
        std::lock_guard<std::mutex> lock(m);
        released = true;
    }
    cv.notify_all();
    writer.join();
    EXPECT_EQ(written.load(), kPastTheBudget);
    EXPECT_EQ(refused.load(), 0) << "a write held for a 300 ms sync was refused";
    engine.while_syncing_wal_for_test({});
    engine.close();
}

TEST(WalBudget, NoWriterWaitsForItUnderEveryOrNone) {
    // `every` syncs each write on the writer's own thread, and `none` never syncs - under `none` a
    // writer waiting for a sync would wait out the deadline and be refused.
    for (const auto policy : {ob::FsyncPolicy::EVERY, ob::FsyncPolicy::NONE}) {
        SCOPED_TRACE(policy == ob::FsyncPolicy::EVERY ? "every" : "none");
        TempDir dir("ob_wal_budget_policy_");
        ob::Engine engine(dir.path, kAnHour, policy);
        engine.open();
        for (uint64_t i = 0; i < 250; ++i) ASSERT_EQ(write(engine, i), ob::OB_OK) << "write " << i;
        EXPECT_EQ(budget_waits(engine), 0u);
        engine.close();
    }
}

TEST(WalBudget, ASyncTellsTheBudgetWhatItWrote) {
    // 130 deltas are about 3.2 MB: more than half the 4 MiB budget, so whatever the sync takes, the
    // budget moves - doubled if it was quick, or set from its rate if it was not.
    TempDir dir("ob_wal_budget_tell_");
    ob::Engine engine(dir.path, kAnHour, ob::FsyncPolicy::INTERVAL);
    engine.open();
    for (uint64_t i = 0; i < 130; ++i) ASSERT_EQ(write(engine, i), ob::OB_OK);
    engine.flush_incremental();
    EXPECT_NE(engine.sync_budget_for_test().bytes(), 4 * MiB)
        << "FLUSH synced 3 MB of WAL and the budget did not hear of it";
    engine.close();
}

TEST(WalBudget, ARefusalWithASyncRunningSaysTheDeviceIsSlowerThanTheWrites) {
    const std::string slow = ob::Engine::room_refusal_reason(milliseconds(5200), 34'000'000);
    EXPECT_NE(slow.find("has run for 5200 ms"), std::string::npos) << slow;
    EXPECT_NE(slow.find("34.0 MB"), std::string::npos) << slow;
    EXPECT_EQ(slow.find("cannot make progress"), std::string::npos)
        << "a flush syncing at the device's rate is making progress: " << slow;

    const std::string stuck = ob::Engine::room_refusal_reason(std::nullopt, 0);
    EXPECT_NE(stuck.find("cannot make progress"), std::string::npos) << stuck;
}
