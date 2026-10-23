// The WAL written back ahead of its sync, and the flush loop while admission is in force (#242).
//
// Writers append the WAL to the page cache, and only a sync hands it to the device. Under the WAL's
// budget (#190 step 9) a device slower than the writes gets a sync at half the budget, each forcing
// the filesystem's journal: on the m9g.xlarge at 7 and 10 MB/s that took 1.7% and 0.8% of the levels
// a second, against the master before the budget. A thread off every writer's path hands each
// megabyte appended to the kernel's writeback, and while admission is in force the flush loop ticks
// when a writer asks, or after a second, rather than at every interval. These tests hold the thread
// to running where a sync is owed and nowhere else, to following the WAL from file to file without
// leaving a descriptor behind, and the loop to its two waits; what it buys is measured, not tested
// (`evidence/2026-10-23-wal-budget-syncs/` in the workspace).

#include "orderbook/admission.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <linux/mman.h>
#include <linux/magic.h>
#include <sys/syscall.h>
#include <sys/vfs.h>
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
    /// Under `base`, the system's temporary directory by default.
    explicit TempDir(const fs::path& base = fs::temp_directory_path()) {
        path = (base /
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
                                      size_t rotate_bytes = 512ULL << 20,
                                      uint64_t flush_interval_ns = kNoAutoFlush) {
    auto engine = std::make_unique<ob::Engine>(dir, flush_interval_ns, policy, ob::ReplicationConfig{},
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

uint64_t requests(ob::Engine& engine) {
    return engine.registry().counter_value("ob_wal_writeback_requests_total");
}

uint64_t ticks(ob::Engine& engine) { return engine.registry().counter_value("ob_flush_ticks_total"); }

/// Pages of the file `fd` names that are dirty in the page cache, from the kernel (cachestat(2),
/// Linux 6.5): what writeback has not taken yet.
uint64_t dirty_pages(int fd) {
    struct cachestat_range range{0, 0};   // the whole file
    struct cachestat cs{};
    if (::syscall(__NR_cachestat, fd, &range, &cs, 0) != 0) {
        ADD_FAILURE() << "cachestat: " << std::strerror(errno);
        return UINT64_MAX;
    }
    return cs.nr_dirty;
}

/// `records` records of 100 levels, 2.4 kB each.
void write_records(ob::Engine& engine, int records, uint64_t first_ts) {
    std::vector<ob::Level> levels(100);
    for (size_t i = 0; i < levels.size(); ++i) {
        levels[i].price = static_cast<int64_t>(1'000 + i);
        levels[i].qty   = 1;
        levels[i].cnt   = 1;
        levels[i]._pad  = 0;
    }
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
    ASSERT_TRUE(eventually([&] { return requests(*engine) >= 1; }))
        << "six megabytes appended and none handed to writeback";
    const uint64_t first = requests(*engine);
    write_megabytes(*engine, 4, 2'000'000'000ULL);
    EXPECT_TRUE(eventually([&] { return requests(*engine) > first; }))
        << "what was appended later was not handed to writeback";
    engine->close();
}

TEST(WalWritebackAhead, WhatIsHandedToWritebackIsDirtyNoLonger) {
    // Nothing syncs the WAL under this test: no tick at the interval, and under half the budget -
    // 4 MB before any sync has measured the device - no writer asks for one. Below the kernel's
    // background threshold its pages stay dirty for dirty_expire_centisecs, 30 s by default, unless
    // something asks for their writeback. Of about 1.86 MB the thread asks for the first megabyte;
    // the tail under a megabyte waits for the next.
    //
    // On the build's own disk rather than in /tmp: a tmpfs has no backing store, so its pages are
    // never dirty, and on one this test passed without a single request (the development machine's
    // /tmp is one).
    TempDir dir(OB_TEST_DISK_DIR);
    struct statfs fs_info{};
    ASSERT_EQ(::statfs(dir.path.c_str(), &fs_info), 0);
    ASSERT_NE(static_cast<long>(fs_info.f_type), static_cast<long>(TMPFS_MAGIC))
        << dir.path << " is on a tmpfs, where no page is ever dirty: this test would test nothing";
    auto engine = engine_at(dir.path, ob::FsyncPolicy::INTERVAL);
    write_records(*engine, 760, 1'000'000'000ULL);
    const std::string wal = dir.path + "/wal_000000.bin";
    const int fd = ::open(wal.c_str(), O_RDONLY | O_CLOEXEC);
    ASSERT_GE(fd, 0) << wal;
    ASSERT_GT(std::filesystem::file_size(wal), (3u << 20) / 2) << "the premise: more than 1.5 MB";
    ASSERT_LT(std::filesystem::file_size(wal), 2u << 20) << "the premise: under half the budget";
    const uint64_t page = static_cast<uint64_t>(::sysconf(_SC_PAGESIZE));
    uint64_t dirty = 0;
    EXPECT_TRUE(eventually([&] {
        dirty = dirty_pages(fd);
        return dirty * page <= (1u << 20);
    })) << dirty << " page(s) of " << page << " bytes of the WAL still dirty";
    EXPECT_EQ(engine->registry().counter_value("ob_flush_ticks_total"), 0u)
        << "the premise: no tick, so no sync, cleaned these pages";
    ::close(fd);
    engine->close();
}

TEST(WalWritebackAhead, OneRequestAMegabyteHoweverTheWritesArrive) {
    // Sixty small writes a few milliseconds apart, so the thread sees the WAL grow at nearly every
    // pass: it asks once a megabyte, not once a pass.
    TempDir dir;
    auto engine = engine_at(dir.path, ob::FsyncPolicy::INTERVAL);
    for (int i = 0; i < 60; ++i) {
        write_records(*engine, 43, 1'000'000'000ULL + static_cast<uint64_t>(i) * 1000);   // ~0.1 MB
        std::this_thread::sleep_for(std::chrono::milliseconds(6));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_GE(requests(*engine), 4u);
    EXPECT_LE(requests(*engine), 7u) << "about 6 MB was asked for in more than one request a megabyte";
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
        EXPECT_EQ(requests(*engine), 0u)
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
        EXPECT_TRUE(eventually([&] { return requests(*engine) >= 3; }))
            << requests(*engine) << " request(s) across the files";
        engine->close();
    }
    EXPECT_EQ(open_descriptors(), before) << "a descriptor outlived the engine";
}

TEST(WalBudgetFewerSyncs, WhileAdmissionIsInForceTheLoopTicksWhenAskedOrAfterASecond) {
    TempDir dir;
    auto engine = engine_at(dir.path, ob::FsyncPolicy::INTERVAL, 512ULL << 20, 50'000'000ULL);

    // Control: without admission, a tick every 50 ms.
    uint64_t from = ticks(*engine);
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    EXPECT_GE(ticks(*engine) - from, 6u) << "the loop did not tick at its interval";

    // A slow tick begins admission - at a rate no write here waits for.
    ASSERT_EQ(engine->admission_for_test().on_tick(10'000'000, std::chrono::milliseconds(200)),
              ob::AdmissionController::Change::Began);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));   // the wait in progress ends
    from = ticks(*engine);
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    EXPECT_LE(ticks(*engine) - from, 1u) << "the loop ticked at its interval while admission was in force";

    // A writer past half the WAL's budget - 4 MB before any sync has measured the device - asks for
    // a tick, and gets it at once rather than at the second.
    from = ticks(*engine);
    const auto asked = std::chrono::steady_clock::now();
    write_megabytes(*engine, 3, 5'000'000'000ULL);
    while (ticks(*engine) == from &&
           std::chrono::steady_clock::now() - asked < std::chrono::milliseconds(900)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_GT(ticks(*engine), from) << "a writer at half the budget got no tick before the second";
    EXPECT_TRUE(engine->admission_for_test().active());
    engine->close();
}
