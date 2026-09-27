// The WAL on a directory of its own (#186).
//
// On ext4 a write() of the WAL waits for the journal, and a data directory of thousands of
// instruments keeps the journal busy with its segments' files: a one-level INSERT answered 1.1 s
// late, all of it in the WAL append, and with the WAL on an ext4 of its own the same run's slowest
// was 11 ms. These tests are what the option has to keep true besides that: the WAL is where it is
// said to be and comes back from there, and no start that would begin an empty WAL beside the
// real one - which loses every row not yet in a segment - gets past open().

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix)
        : path((fs::temp_directory_path() /
                (prefix + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed))))
                   .string()) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

/// The rows stay in the WAL: no flush tick seals them in the length of a test.
constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;

std::unique_ptr<ob::Engine> engine_at(const std::string& data, const std::string& wal) {
    return std::make_unique<ob::Engine>(data, kNoAutoFlush, ob::FsyncPolicy::EVERY,
                                        ob::ReplicationConfig{}, ob::ReplicationClientConfig{},
                                        ob::FailoverConfig{}, ob::TTLConfig{},
                                        ob::MultiMasterConfig{}, 512ULL << 20, wal);
}

void insert_rows(ob::Engine& engine, int count) {
    for (int i = 0; i < count; ++i) {
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, "WALDIR", sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
        delta.timestamp_ns = 1'000'000'000ULL + static_cast<uint64_t>(i) * 1'000'000ULL;
        delta.side         = ob::SIDE_BID;
        delta.n_levels     = 1;
        ob::Level level{};
        level.price = 10'000 + i;
        level.qty   = 1;
        level.cnt   = 1;
        level._pad  = 0;
        ASSERT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);
    }
}

int query_count(ob::Engine& engine) {
    int count = 0;
    const std::string err = engine.execute(
        "SELECT * FROM 'WALDIR'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult&) { ++count; });
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) {
        ADD_FAILURE() << "query error: " << err;
    }
    return count;
}

bool has_wal_files(const std::string& dir) {
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(dir, ec)) {
        const std::string name = entry.path().filename().string();
        if (name.rfind("wal_", 0) == 0 && entry.path().extension() == ".bin") return true;
    }
    return false;
}

std::string open_refusal(const std::string& data, const std::string& wal) {
    try {
        auto engine = engine_at(data, wal);
        engine->open();
        engine->close();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

}  // namespace

TEST(WalDir, TheWalAndItsIdentityLiveInTheDirectoryNamed) {
    TempDir data("waldir_data_"), wal("waldir_wal_");
    auto engine = engine_at(data.path, wal.path);
    engine->open();
    insert_rows(*engine, 3);
    engine->close();

    EXPECT_TRUE(has_wal_files(wal.path)) << "no WAL file in the directory --wal-dir named";
    EXPECT_TRUE(fs::exists(fs::path(wal.path) / "wal_identity"));
    EXPECT_FALSE(has_wal_files(data.path)) << "a WAL file in the data directory all the same";
    EXPECT_FALSE(fs::exists(fs::path(data.path) / "wal_identity"))
        << "the identity is the WAL's, and stayed behind";
    EXPECT_EQ(fs::path(engine->wal_dir()), fs::weakly_canonical(wal.path));
}

TEST(WalDir, WithoutTheOptionTheWalIsInTheDataDirectory) {
    TempDir data("waldir_default_");
    auto engine = engine_at(data.path, "");
    engine->open();
    insert_rows(*engine, 1);
    engine->close();
    EXPECT_EQ(engine->wal_dir(), engine->base_dir());
    EXPECT_TRUE(has_wal_files(data.path));
    EXPECT_FALSE(fs::exists(fs::path(data.path) / "wal_location"))
        << "a data directory whose WAL is in it notes a location all the same";
}

TEST(WalDir, AnAbandonedEngineComesBackWithItsRowsFromThatWal) {
    TempDir data("waldir_crash_data_"), wal("waldir_crash_wal_");
    {
        auto engine = engine_at(data.path, wal.path);
        engine->open();
        insert_rows(*engine, 5);
        engine.release();   // deliberately leaked: no close(), no flush - what a crash leaves
    }
    auto reopened = engine_at(data.path, wal.path);
    reopened->open();
    EXPECT_EQ(query_count(*reopened), 5) << "the tail was not replayed from the WAL's directory";
    reopened->close();
}

TEST(WalDir, AWalInsideTheDataDirectoryIsRefused) {
    TempDir data("waldir_nested_");
    const std::string nested = data.path + "/wal";
    const std::string why = open_refusal(data.path, nested);
    EXPECT_NE(why.find("inside --data-dir"), std::string::npos) << why;
    EXPECT_FALSE(fs::exists(nested)) << "a refused start made the directory anyway";
}

TEST(WalDir, AWalAlreadyInTheDataDirectoryIsNotLeftBehind) {
    TempDir data("waldir_moved_data_"), wal("waldir_moved_wal_");
    {
        auto engine = engine_at(data.path, "");
        engine->open();
        insert_rows(*engine, 2);
        engine.release();   // the tail is in the data directory's WAL only
    }
    const std::string why = open_refusal(data.path, wal.path);
    EXPECT_NE(why.find("move its wal_*.bin files"), std::string::npos) << why;
    EXPECT_FALSE(has_wal_files(wal.path)) << "a refused start began a WAL there";
}

TEST(WalDir, AStartWithoutTheOptionDoesNotBeginAnEmptyWalBesideTheRealOne) {
    TempDir data("waldir_dropped_data_"), wal("waldir_dropped_wal_");
    {
        auto engine = engine_at(data.path, wal.path);
        engine->open();
        insert_rows(*engine, 4);
        engine.release();
    }
    const std::string why = open_refusal(data.path, "");
    EXPECT_NE(why.find("start with --wal-dir"), std::string::npos) << why;
    EXPECT_FALSE(has_wal_files(data.path)) << "a refused start began a WAL in the data directory";

    // And one that names another, empty, directory is refused the same way ...
    TempDir other("waldir_dropped_other_");
    const std::string why_other = open_refusal(data.path, other.path);
    EXPECT_NE(why_other.find("holds none"), std::string::npos) << why_other;

    // ... while the WAL moved there, files and identity, is the WAL.
    for (const auto& entry : fs::directory_iterator(wal.path)) {
        fs::rename(entry.path(), fs::path(other.path) / entry.path().filename());
    }
    auto moved = engine_at(data.path, other.path);
    moved->open();
    EXPECT_EQ(query_count(*moved), 4) << "the moved WAL's tail was not replayed";
    moved->close();
    std::ifstream location(fs::path(data.path) / "wal_location");
    std::string noted;
    std::getline(location, noted);
    EXPECT_EQ(fs::path(noted), fs::weakly_canonical(other.path)) << "the note still names the old place";
}
