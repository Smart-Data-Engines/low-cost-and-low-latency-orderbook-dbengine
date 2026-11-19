// The WAL files retention lets go are removed off the flush thread (#277).
//
// A tick decides, under the engine's lock, which WAL files no checkpoint, replica or archive still needs,
// and until #277 removed them itself: the unlink of a 512 MB file took the flush thread 34-37 ms on ext4 and
// 16-17 ms on XFS under #190's ingest, twice a round, while the writers filled the other half of the pending
// queue in 32-36 ms - and most of the steady state's writes that waited at the ceiling waited during one.
// The tick now asks a thread of its own, and the next tick does not wait for the removal.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kOneHourNs = 3'600'000'000'000ULL;

/// A WAL file every few deltas: a delta of a thousand levels is about 24 kB of WAL.
constexpr size_t kSmallWalFiles = 64 * 1024;

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

std::unique_ptr<ob::Engine> engine_with_small_wal_files(const std::string& dir) {
    return std::make_unique<ob::Engine>(dir, kOneHourNs, ob::FsyncPolicy::INTERVAL, ob::ReplicationConfig{},
                                        ob::ReplicationClientConfig{}, ob::FailoverConfig{}, ob::TTLConfig{},
                                        ob::MultiMasterConfig{}, kSmallWalFiles);
}

ob::ob_status_t write(ob::Engine& engine, uint64_t n) {
    std::vector<ob::Level> lv(1000);
    for (size_t l = 0; l < lv.size(); ++l) lv[l] = {10'000'000 - static_cast<int64_t>(l), 100 + l + n, 1, 0};
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol,   "RM", sizeof(delta.symbol)   - 1);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.timestamp_ns = 1'700'000'000'000'000'000ULL + n;
    delta.side         = ob::SIDE_BID;
    delta.n_levels     = static_cast<uint16_t>(lv.size());
    return engine.apply_delta(delta, lv.data());
}

/// The WAL files in `dir`, by index.
std::vector<uint32_t> wal_files(const std::string& dir) {
    std::vector<uint32_t> out;
    for (const auto& entry : fs::directory_iterator(dir)) {
        uint32_t index = 0;
        if (ob::parse_wal_file_basename(entry.path().filename().string(), index)) out.push_back(index);
    }
    std::sort(out.begin(), out.end());
    return out;
}

/// Writes enough for a dozen WAL files and flushes them into segments, with the checkpoint that lets
/// retention take the files before it. Returns the files there are then.
std::vector<uint32_t> files_retention_may_take(ob::Engine& engine, const std::string& dir) {
    for (uint64_t n = 0; n < 36; ++n) EXPECT_EQ(write(engine, n), ob::OB_OK);
    engine.flush_incremental();
    return wal_files(dir);
}

}  // namespace

TEST(WalRemoval, ATickLeavesTheRemovalOfTheFilesItLetsGoToTheirOwnThread) {
    TempDir dir("ob_wal_removal_tick_");
    auto engine = engine_with_small_wal_files(dir.path);
    engine->open();
    const std::vector<uint32_t> before = files_retention_may_take(*engine, dir.path);
    ASSERT_GE(before.size(), 4u) << "the writes did not make the WAL files this test needs";

    {
        // Held where a removal is about to begin: a tick that removed its files itself would remove
        // them anyway, and one that waited for the removal would not return.
        auto hold = engine->hold_wal_removals_for_test();
        auto tick = std::async(std::launch::async, [&engine] { engine->flush_tick_leaving_the_sync_for_test(); });
        ASSERT_EQ(tick.wait_for(std::chrono::seconds(10)), std::future_status::ready)
            << "the tick did not return while the removal it asked for was held: it waits for it";
        EXPECT_EQ(wal_files(dir.path), before)
            << "the tick removed the WAL files itself, on the flush thread";
    }
    engine->await_wal_removals_for_test();
    const std::vector<uint32_t> after = wal_files(dir.path);
    ASSERT_FALSE(after.empty()) << "the file the writer is on was removed";
    EXPECT_LT(after.size(), before.size()) << "no WAL file was removed once the removal was let go";
    EXPECT_EQ(after.back(), before.back()) << "the newest WAL file was removed";
    engine->close();
}

TEST(WalRemoval, ATestTickReturnsOnceItsFilesAreRemoved) {
    // What every test of retention that reads the WAL directory after flush_tick_for_test() stands on.
    TempDir dir("ob_wal_removal_test_tick_");
    auto engine = engine_with_small_wal_files(dir.path);
    engine->open();
    const std::vector<uint32_t> before = files_retention_may_take(*engine, dir.path);
    ASSERT_GE(before.size(), 4u);
    engine->flush_tick_for_test();
    EXPECT_LT(wal_files(dir.path).size(), before.size())
        << "flush_tick_for_test() returned before the files its tick let go were removed";
    engine->close();
}

TEST(WalRemoval, CloseRemovesTheFilesTheLastTickLetGoBeforeItReturns) {
    TempDir dir("ob_wal_removal_close_");
    auto engine = engine_with_small_wal_files(dir.path);
    engine->open();
    const std::vector<uint32_t> before = files_retention_may_take(*engine, dir.path);
    ASSERT_GE(before.size(), 4u);

    std::future<void> closed;
    {
        auto hold = engine->hold_wal_removals_for_test();
        engine->flush_tick_leaving_the_sync_for_test();
        closed = std::async(std::launch::async, [&engine] { engine->close(); });
        // close() cannot finish while the removal it waits for is held.
        EXPECT_EQ(closed.wait_for(std::chrono::milliseconds(300)), std::future_status::timeout)
            << "close() returned with a removal its last tick asked for not done";
    }
    ASSERT_EQ(closed.wait_for(std::chrono::seconds(30)), std::future_status::ready);
    const std::vector<uint32_t> after = wal_files(dir.path);
    EXPECT_LT(after.size(), before.size())
        << "close() returned and left the files the last tick let go: the thread stopped without them";
}
