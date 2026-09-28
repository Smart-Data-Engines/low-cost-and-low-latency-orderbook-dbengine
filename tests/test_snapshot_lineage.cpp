// A store that holds rows no record of its WAL holds begins a new WAL lineage (#197).
//
// Measured before the fix: a replica bootstrapped from a snapshot and then made a primary gave a
// new replica of its own 0 of its 18 000 rows - its WAL began at file 0, which existed and held
// none of them, and a primary sends a snapshot only when the file a replica asks from is gone. After
// `install_snapshot()` and after `discard_local_data_for_resync()` the WAL now has a new identity
// and begins at a file after that moment, with the files before it removed. What these hold, each
// beside a control: the identity changes and is on the disk, no file of the old lineage is left, a
// write after goes into the new one, a restart keeps the new identity and the installed rows and
// none of the replaced store's, and a node that installs nothing keeps its identity.

#include "orderbook/data_model.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    TempDir() {
        path = (fs::temp_directory_path() /
                ("ob_lineage_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed))))
                   .string();
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;
constexpr uint64_t kBase        = 1'790'000'000'000'000'000ULL;

std::unique_ptr<ob::Engine> engine_at(const std::string& dir) {
    auto engine = std::make_unique<ob::Engine>(dir, kNoAutoFlush, ob::FsyncPolicy::INTERVAL);
    engine->open();
    return engine;
}

void write(ob::Engine& engine, const char* symbol, uint64_t n) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.timestamp_ns = kBase + n * 1000;
    delta.side         = ob::SIDE_BID;
    delta.n_levels     = 2;
    ob::Level levels[2]{};
    for (int i = 0; i < 2; ++i) {
        levels[i].price = static_cast<int64_t>(n * 10 + i);
        levels[i].qty   = 1;
        levels[i].cnt   = 1;
    }
    ASSERT_EQ(engine.apply_delta(delta, levels), ob::OB_OK);
}

std::vector<int64_t> prices(ob::Engine& engine, const char* symbol) {
    std::vector<int64_t> out;
    const std::string err = engine.execute(std::string("SELECT * FROM '") + symbol + "'.'EX'",
                                           [&](const ob::QueryResult& r) { out.push_back(r.price); });
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) ADD_FAILURE() << err;
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<uint32_t> wal_files(const std::string& dir) {
    std::vector<uint32_t> out;
    for (const auto& e : fs::directory_iterator(dir)) {
        const std::string n = e.path().filename().string();
        if (n.size() == 14 && n.rfind("wal_", 0) == 0 && n.substr(10) == ".bin") {
            out.push_back(static_cast<uint32_t>(std::stoul(n.substr(4, 6))));
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

uint64_t identity_on_disk(const std::string& dir) {
    std::ifstream in(dir + "/wal_identity");
    uint64_t v = 0;
    in >> v;
    return v;
}

/// A snapshot of `from`, staged in `to`'s data directory the way a replica stages one.
ob::SnapshotManifest stage_snapshot(ob::Engine& from, const std::string& from_dir,
                                    const std::string& staging) {
    ob::SnapshotManifest m = from.create_snapshot();
    for (const auto& f : m.files) {
        const fs::path dst = fs::path(staging) / f.path;
        fs::create_directories(dst.parent_path());
        fs::copy_file(fs::path(from_dir) / f.path, dst);
    }
    return m;
}

}  // namespace

TEST(SnapshotLineage, AnInstallBeginsANewLineageAndKeepsIt) {
    TempDir a_dir, b_dir;
    auto a = engine_at(a_dir.path);
    for (uint64_t n = 0; n < 20; ++n) write(*a, "A", n);
    a->flush_incremental();
    const auto a_rows = prices(*a, "A");

    auto b = engine_at(b_dir.path);
    for (uint64_t n = 100; n < 110; ++n) write(*b, "OLD", n);   // the store the install replaces
    b->flush_incremental();
    for (uint64_t n = 110; n < 115; ++n) write(*b, "OLD", n);   // and records still only in the WAL
    const uint64_t before = b->wal_identity();
    const auto files_before = wal_files(b_dir.path);
    ASSERT_FALSE(files_before.empty());

    const std::string staging = b_dir.path + "/snapshot_staging";
    const ob::SnapshotManifest m = stage_snapshot(*a, a_dir.path, staging);
    ASSERT_TRUE(b->install_snapshot(staging, m));

    const uint64_t after = b->wal_identity();
    EXPECT_NE(after, before) << "the store is not what the old lineage's records describe";
    EXPECT_NE(after, 0u);
    EXPECT_EQ(identity_on_disk(b_dir.path), after) << "a restart would come back in the old lineage";
    const auto files_after = wal_files(b_dir.path);
    ASSERT_FALSE(files_after.empty());
    EXPECT_GT(files_after.front(), files_before.back())
        << "a file of the old lineage is left, and a replica asking for the start is streamed it";
    EXPECT_EQ(prices(*b, "A"), a_rows);
    EXPECT_TRUE(prices(*b, "OLD").empty());

    // A write after the install is in the new lineage, and a restart keeps both.
    write(*b, "A", 1000);
    b->flush_incremental();
    b->close();
    b.reset();
    auto again = engine_at(b_dir.path);
    EXPECT_EQ(again->wal_identity(), after);
    auto want = a_rows;
    want.push_back(10000);
    want.push_back(10001);
    std::sort(want.begin(), want.end());
    EXPECT_EQ(prices(*again, "A"), want);
    EXPECT_TRUE(prices(*again, "OLD").empty()) << "a restart replayed the replaced store's records";
    again->close();
    a->close();
}

TEST(SnapshotLineage, DiscardingTheStoreToReplayFromZeroBeginsANewLineage) {
    TempDir dir;
    auto e = engine_at(dir.path);
    for (uint64_t n = 0; n < 10; ++n) write(*e, "A", n);
    e->flush_incremental();
    for (uint64_t n = 10; n < 15; ++n) write(*e, "A", n);
    const uint64_t before = e->wal_identity();
    const auto files_before = wal_files(dir.path);

    e->discard_local_data_for_resync();

    EXPECT_NE(e->wal_identity(), before);
    EXPECT_EQ(identity_on_disk(dir.path), e->wal_identity());
    EXPECT_GT(wal_files(dir.path).front(), files_before.back());
    EXPECT_TRUE(prices(*e, "A").empty());
    const uint64_t after = e->wal_identity();
    e->close();
    e.reset();
    auto again = engine_at(dir.path);
    EXPECT_EQ(again->wal_identity(), after);
    EXPECT_TRUE(prices(*again, "A").empty()) << "a restart replayed the discarded store's records";
    again->close();
}

TEST(SnapshotLineage, ANodeThatInstallsNothingKeepsItsLineage) {
    // The control: writes, flushes and restarts are not a new lineage.
    TempDir dir;
    auto e = engine_at(dir.path);
    const uint64_t identity = e->wal_identity();
    for (uint64_t n = 0; n < 10; ++n) write(*e, "A", n);
    e->flush_incremental();
    e->close();
    e.reset();
    auto again = engine_at(dir.path);
    EXPECT_EQ(again->wal_identity(), identity);
    EXPECT_EQ(wal_files(dir.path).front(), 0u);
    EXPECT_EQ(prices(*again, "A").size(), 20u);
    again->close();
}
