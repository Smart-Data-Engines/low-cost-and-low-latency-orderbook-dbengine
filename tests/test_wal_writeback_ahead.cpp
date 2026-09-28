// The WAL written back ahead of its sync (#190).
//
// Writers append the WAL to the page cache, and until #190 only the flush tick's sync handed it to
// the device - so at the write ceiling, ~63 MB/s of WAL on the development machine, each sync found
// everything written since the last one and took 0.9 - 2.4 s while writers waited for the room the
// tick's drain frees. A thread off every writer's path hands each megabyte appended to the kernel's
// writeback. These tests hold it to running where a sync is owed and nowhere else, and to following
// the WAL from file to file without leaving a descriptor behind; what it buys is measured, not
// tested (`evidence/2026-09-28-wal-writeback-ahead/`).

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    TempDir() {
        path = (fs::temp_directory_path() /
                ("ob_wal_writeback_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed))))
                   .string();
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;   // no tick syncs anything under the test

std::unique_ptr<ob::Engine> engine_at(const std::string& dir, ob::FsyncPolicy policy,
                                      size_t rotate_bytes = 512ULL << 20) {
    auto engine = std::make_unique<ob::Engine>(dir, kNoAutoFlush, policy, ob::ReplicationConfig{},
                                               ob::ReplicationClientConfig{}, ob::FailoverConfig{},
                                               ob::TTLConfig{}, ob::MultiMasterConfig{}, rotate_bytes);
    engine->open();
    return engine;
}

/// About `mb` megabytes of WAL: records of 100 levels, 2.4 kB each.
void write_megabytes(ob::Engine& engine, int mb, uint64_t first_ts) {
    std::vector<ob::Level> levels(100);
    for (size_t i = 0; i < levels.size(); ++i) {
        levels[i].price = static_cast<int64_t>(1'000 + i);
        levels[i].qty   = 1;
        levels[i].cnt   = 1;
        levels[i]._pad  = 0;
    }
    const int records = mb * 1024 * 1024 / 2500;
    for (int i = 0; i < records; ++i) {
        ob::DeltaUpdate d{};
        std::strncpy(d.symbol, "WB", sizeof(d.symbol) - 1);
        std::strncpy(d.exchange, "EX", sizeof(d.exchange) - 1);
        d.timestamp_ns = first_ts + static_cast<uint64_t>(i);
        d.side         = ob::SIDE_BID;
        d.n_levels     = static_cast<uint16_t>(levels.size());
        ASSERT_EQ(engine.apply_delta(d, levels.data()), ob::OB_OK);
    }
}

bool eventually(const std::function<bool()>& done) {
    for (int i = 0; i < 400; ++i) {
        if (done()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return done();
}

size_t open_descriptors() {
    size_t n = 0;
    for (const auto& e : fs::directory_iterator("/proc/self/fd")) {
        (void)e;
        ++n;
    }
    return n;
}

size_t wal_files(const std::string& dir) {
    size_t n = 0;
    for (const auto& e : fs::directory_iterator(dir)) {
        const std::string name = e.path().filename().string();
        if (name.rfind("wal_", 0) == 0 && e.path().extension() == ".bin") ++n;
    }
    return n;
}

}  // namespace

TEST(WalWritebackAhead, AWalUnderIntervalIsHandedToWritebackAsItGrows) {
    TempDir dir;
    auto engine = engine_at(dir.path, ob::FsyncPolicy::INTERVAL);
    write_megabytes(*engine, 6, 1'000'000'000ULL);
    ASSERT_TRUE(eventually([&] { return engine->wal_writeback_requests_for_test() >= 1; }))
        << "six megabytes appended and none handed to writeback";
    const uint64_t first = engine->wal_writeback_requests_for_test();
    write_megabytes(*engine, 4, 2'000'000'000ULL);
    EXPECT_TRUE(eventually([&] { return engine->wal_writeback_requests_for_test() > first; }))
        << "what was appended later was not handed to writeback";
    engine->close();
}

TEST(WalWritebackAhead, EveryAndNoneHandNothingToWriteback) {
    // `every` syncs each write before it is answered, and `none` promises nothing: neither has a sync
    // that writing back ahead would shorten.
    for (const auto policy : {ob::FsyncPolicy::EVERY, ob::FsyncPolicy::NONE}) {
        TempDir dir;
        auto engine = engine_at(dir.path, policy);
        write_megabytes(*engine, policy == ob::FsyncPolicy::EVERY ? 2 : 6, 1'000'000'000ULL);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(engine->wal_writeback_requests_for_test(), 0u)
            << "policy " << static_cast<int>(policy) << " wrote the WAL back ahead";
        engine->close();
    }
}

TEST(WalWritebackAhead, ItFollowsTheWalFromFileToFileAndLeavesNoDescriptor) {
    TempDir dir;
    const size_t before = open_descriptors();
    {
        auto engine = engine_at(dir.path, ob::FsyncPolicy::INTERVAL, 3u << 20);
        write_megabytes(*engine, 13, 1'000'000'000ULL);
        ASSERT_GE(wal_files(dir.path), 4u) << "the premise: the WAL rotated";
        EXPECT_TRUE(eventually([&] { return engine->wal_writeback_requests_for_test() >= 3; }))
            << engine->wal_writeback_requests_for_test() << " request(s) across the files";
        engine->close();
    }
    EXPECT_EQ(open_descriptors(), before) << "a descriptor outlived the engine";
}
