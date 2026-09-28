// A symbol moved to another shard with its rows (#196): the pieces a migration is made of.
//
// `MIGRATE` marked a symbol migrated on its shard and moved none of its rows - three stubs - and is
// refused until it does. These tests hold the pieces it is built from. On the shard a symbol moves
// from: its writes refused while it moves, under the lock the WAL append takes, so none is answered
// OK after the last copy; every row of it sealed into segments that can be listed and read. On the
// shard it moves to: an adoption, begun only where none of its rows is, whose writes are taken from
// the connection that began it alone, and only while it stands - checked under the engine's lock
// too, because the server's check is made without it; and an abandoned adoption's rows dropped for
// good, the merges of them included - a restart brings none back.

#include "orderbook/engine.hpp"
#include "orderbook/command_parser.hpp"
#include "orderbook/compaction.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/session.hpp"
#include "orderbook/shard_coordinator.hpp"
#include "orderbook/shard_map.hpp"
#include "orderbook/tcp_server.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    TempDir() {
        path = (fs::temp_directory_path() /
                ("ob_symbol_migration_" + std::to_string(::getpid()) + "_" +
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

std::unique_ptr<ob::Engine> engine_at(const std::string& dir, uint64_t flush_ns = kNoAutoFlush) {
    auto engine = std::make_unique<ob::Engine>(dir, flush_ns, ob::FsyncPolicy::INTERVAL);
    engine->open();
    return engine;
}

/// One update of `levels` levels at `ts`, each level at its own price.
struct Update {
    ob::DeltaUpdate        delta{};
    std::vector<ob::Level> levels;
};

Update update_of(const char* symbol, uint64_t ts, uint16_t levels) {
    Update u;
    std::strncpy(u.delta.symbol, symbol, sizeof(u.delta.symbol) - 1);
    std::strncpy(u.delta.exchange, "EX", sizeof(u.delta.exchange) - 1);
    u.delta.timestamp_ns = ts;
    u.delta.side         = ob::SIDE_BID;
    u.delta.n_levels     = levels;
    u.levels.resize(levels);
    for (uint16_t i = 0; i < levels; ++i) {
        u.levels[i].price = static_cast<int64_t>(ts % 1'000'000) * 100 + i;
        u.levels[i].qty   = 7;
        u.levels[i].cnt   = 1;
    }
    return u;
}

ob::ob_status_t write(ob::Engine& engine, const char* symbol, uint64_t ts, uint16_t levels = 3) {
    const Update u = update_of(symbol, ts, levels);
    return engine.apply_delta(u.delta, u.levels.data());
}

/// A write as the server hands it to the engine, taken on `adoption` (0 for none).
ob::ob_status_t write_on(ob::Engine& engine, const char* symbol, uint64_t ts, uint64_t adoption) {
    const Update u = update_of(symbol, ts, 3);
    ob::ClientWrite w{};
    w.update   = &u.delta;
    w.levels   = u.levels.data();
    w.adoption = adoption;
    std::vector<ob::WriteOutcome> outcomes(1);
    engine.apply_deltas(std::span<const ob::ClientWrite>(&w, 1), outcomes);
    EXPECT_TRUE(outcomes[0].error.empty()) << outcomes[0].error;
    return outcomes[0].status;
}

int rows(ob::Engine& engine, const char* symbol) {
    int n = 0;
    const std::string sql = std::string("SELECT * FROM '") + symbol + "'.'EX'";
    const std::string err = engine.execute(sql, [&n](const ob::QueryResult&) { ++n; });
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) ADD_FAILURE() << err;
    return n;
}

/// The segment directories of `symbol` on the disk under `base`, whatever the index says.
size_t dirs_of(const std::string& base, const char* symbol) {
    size_t n = 0;
    std::error_code ec;
    for (fs::recursive_directory_iterator it(base, ec), end; !ec && it != end; it.increment(ec)) {
        if (it->path().filename() == "meta.json" &&
            it->path().string().find(std::string("/") + symbol + "/") != std::string::npos) {
            ++n;
        }
    }
    return n;
}

template <typename F>
bool eventually(F&& done, std::chrono::milliseconds within = 10000ms) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return done();
}

}  // namespace

// ── The shard a symbol moves from ─────────────────────────────────────────────

TEST(SymbolMigration, AMovingSymbolsWritesAreRefusedAndTheOthersTaken) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    ASSERT_EQ(write(*engine, "MOV", 1'000), ob::OB_OK);
    engine->freeze_symbol("MOV.EX");
    EXPECT_TRUE(engine->is_symbol_frozen("MOV.EX"));
    EXPECT_EQ(write(*engine, "MOV", 2'000), ob::OB_ERR_MOVING);
    EXPECT_EQ(write(*engine, "STAY", 2'000), ob::OB_OK) << "another symbol's write was refused";
    engine->thaw_symbol("MOV.EX");
    EXPECT_FALSE(engine->is_symbol_frozen("MOV.EX"));
    EXPECT_EQ(write(*engine, "MOV", 3'000), ob::OB_OK);
    engine->flush_incremental();
    EXPECT_EQ(rows(*engine, "MOV"), 2 * 3) << "a refused write was stored";
    engine->close();
}

TEST(SymbolMigration, SealingASymbolListsEveryRowOfIt) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    for (uint64_t i = 1; i <= 40; ++i) ASSERT_EQ(write(*engine, "SEAL", i * 1'000), ob::OB_OK);
    engine->flush_incremental();                  // some of it in a segment already
    for (uint64_t i = 41; i <= 70; ++i) ASSERT_EQ(write(*engine, "SEAL", i * 1'000), ob::OB_OK);
    ASSERT_EQ(write(*engine, "OTHER", 5'000), ob::OB_OK);

    const auto segments = engine->seal_symbol("SEAL.EX");
    ASSERT_GE(segments.size(), 2u);
    uint64_t listed = 0;
    std::vector<uint64_t> timestamps;
    for (const auto& meta : segments) {
        EXPECT_EQ(meta.symbol, "SEAL");
        listed += meta.row_count;
        ASSERT_TRUE(engine->read_symbol_segment(meta, [&](const ob::SnapshotRow& r) {
            timestamps.push_back(r.timestamp_ns);
        }));
    }
    EXPECT_EQ(listed, 70u * 3) << "the segments listed do not hold every row written";
    ASSERT_EQ(timestamps.size(), 70u * 3);
    std::sort(timestamps.begin(), timestamps.end());
    EXPECT_EQ(timestamps.front(), 1'000u);
    EXPECT_EQ(timestamps.back(), 70'000u);
    EXPECT_EQ(rows(*engine, "SEAL"), 70 * 3);
    EXPECT_EQ(rows(*engine, "OTHER"), 3) << "the other symbol's row was lost";
    engine->close();
    auto reopened = engine_at(dir.path);
    EXPECT_EQ(rows(*reopened, "SEAL"), 70 * 3) << "a restart after the seal lost or doubled rows";
    reopened->close();
}

// ── The shard a symbol moves to: the engine ───────────────────────────────────

TEST(SymbolMigration, ASymbolIsHeldFromItsFirstWrite) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    EXPECT_FALSE(engine->holds_symbol("HELD.EX"));
    ASSERT_EQ(write(*engine, "HELD", 1'000), ob::OB_OK);
    EXPECT_TRUE(engine->holds_symbol("HELD.EX")) << "a row only queued";
    engine->flush_incremental();
    EXPECT_TRUE(engine->holds_symbol("HELD.EX")) << "a row in a segment";
    EXPECT_FALSE(engine->holds_symbol("HELD.XX"));
    EXPECT_FALSE(engine->holds_symbol("HELDxEX")) << "the dot is where the key splits";
    engine->close();
    auto reopened = engine_at(dir.path);
    EXPECT_TRUE(reopened->holds_symbol("HELD.EX")) << "a row a restart found on the disk";
    reopened->close();
}

TEST(SymbolMigration, AnAdoptionBeginsOnlyWhereNoneOfItsRowsAre) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    ASSERT_EQ(write(*engine, "LEFT", 1'000), ob::OB_OK);
    EXPECT_EQ(engine->begin_adoption("LEFT.EX"), 0u) << "adopted over rows a migration would copy again";
    const uint64_t a = engine->begin_adoption("NEW.EX");
    EXPECT_NE(a, 0u);
    EXPECT_EQ(write_on(*engine, "NEW", 1'000, a), ob::OB_OK);
    engine->close();
}

TEST(SymbolMigration, AWriteTakenOnAnAdoptionIsStoredOnlyWhileItStands) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    const uint64_t first = engine->begin_adoption("ADP.EX");
    ASSERT_NE(first, 0u);
    ASSERT_EQ(write_on(*engine, "ADP", 1'000, first), ob::OB_OK);
    EXPECT_GT(engine->abandon_adoption("ADP.EX"), 0u);
    EXPECT_FALSE(engine->holds_symbol("ADP.EX"));
    // One the server let through before the abandon, reaching the log after it.
    EXPECT_EQ(write_on(*engine, "ADP", 2'000, first), ob::OB_ERR_NOT_OWNER);
    EXPECT_FALSE(engine->holds_symbol("ADP.EX")) << "a write taken on an abandoned adoption was stored";

    // Begun again: the old one's write is still refused, and the new one's taken.
    const uint64_t second = engine->begin_adoption("ADP.EX");
    ASSERT_NE(second, 0u);
    EXPECT_NE(second, first);
    EXPECT_EQ(write_on(*engine, "ADP", 3'000, first), ob::OB_ERR_NOT_OWNER);
    EXPECT_EQ(write_on(*engine, "ADP", 4'000, second), ob::OB_OK);
    engine->end_adoption("ADP.EX");
    EXPECT_EQ(write_on(*engine, "ADP", 5'000, second), ob::OB_ERR_NOT_OWNER)
        << "a write taken on an ended adoption: after END the shard takes it as an owner, unnumbered";
    EXPECT_EQ(write_on(*engine, "ADP", 6'000, 0), ob::OB_OK);
    engine->flush_incremental();
    EXPECT_EQ(rows(*engine, "ADP"), 2 * 3);
    engine->close();
}

TEST(SymbolMigration, ASymbolMigratedAwayIsTakenBackByAnAdoption) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    engine->mark_symbol_migrated("BACK.EX");
    EXPECT_EQ(write(*engine, "BACK", 1'000), ob::OB_ERR_MIGRATED);
    const uint64_t a = engine->begin_adoption("BACK.EX");
    ASSERT_NE(a, 0u);
    EXPECT_FALSE(engine->is_symbol_migrated("BACK.EX"));
    EXPECT_EQ(write_on(*engine, "BACK", 2'000, a), ob::OB_OK)
        << "the adoption's copy refused as migrated: the symbol could never come back";
    engine->close();
}

TEST(SymbolMigration, ADroppedSymbolIsGoneAndARestartDoesNotBringItBack) {
    TempDir dir;
    {
        auto engine = engine_at(dir.path);
        for (uint64_t i = 1; i <= 30; ++i) {
            ASSERT_EQ(write(*engine, "DROP", i * 1'000), ob::OB_OK);
            ASSERT_EQ(write(*engine, "KEEP", i * 1'000), ob::OB_OK);
            if (i == 10) engine->flush_incremental();
        }
        EXPECT_GE(engine->drop_symbol("DROP.EX"), 2u);
        EXPECT_FALSE(engine->holds_symbol("DROP.EX"));
        EXPECT_EQ(rows(*engine, "DROP"), 0);
        EXPECT_EQ(dirs_of(dir.path, "DROP"), 0u) << "a segment of it stayed on the disk";
        EXPECT_EQ(rows(*engine, "KEEP"), 30 * 3) << "the other symbol lost rows";
        engine->close();
    }
    auto reopened = engine_at(dir.path);
    EXPECT_EQ(rows(*reopened, "DROP"), 0) << "a restart replayed the dropped symbol's records";
    EXPECT_FALSE(reopened->holds_symbol("DROP.EX"));
    EXPECT_EQ(rows(*reopened, "KEEP"), 30 * 3);
    reopened->close();
}

TEST(SymbolMigration, ADropAfterASealThatClaimedNothingStaysDropped) {
    // seal_symbol() writes segments and no checkpoint, so the log's last one - here, none at all -
    // is older than every record of the symbol: the drop has to append one of its own, or a restart
    // replays them back with no segment to say they are stored.
    TempDir dir;
    {
        auto engine = engine_at(dir.path);
        for (uint64_t i = 1; i <= 20; ++i) ASSERT_EQ(write(*engine, "GONE", i * 1'000), ob::OB_OK);
        ASSERT_FALSE(engine->seal_symbol("GONE.EX").empty());
        EXPECT_GT(engine->drop_symbol("GONE.EX"), 0u);
        engine->close();
    }
    auto reopened = engine_at(dir.path);
    EXPECT_EQ(rows(*reopened, "GONE"), 0) << "a restart replayed the dropped symbol back";
    reopened->close();
}

TEST(SymbolMigration, ADropTakesTheInputsOfItsMergesThatWaitToBeRemoved) {
    // A merge published while a scan reads its inputs leaves them on the disk until the scan ends,
    // and a start keeps an input it finds without the merged segment that names it - which the drop
    // removes. Left there, the restart would bring every row of them back.
    TempDir dir;
    auto engine = engine_at(dir.path, 20'000'000ULL);
    for (uint64_t i = 1; i <= ob::compaction::kFanIn; ++i) {
        ASSERT_EQ(write(*engine, "MRG", i * 1'000, 50), ob::OB_OK);
        engine->flush_incremental();
    }
    std::mutex m;
    std::condition_variable cv;
    bool in_scan = false;
    bool go_on = false;
    std::thread reader([&] {
        size_t seen = 0;
        (void)engine->execute("SELECT * FROM 'MRG'.'EX'", [&](const ob::QueryResult&) {
            std::unique_lock<std::mutex> lock(m);
            if (++seen == 1) {
                in_scan = true;
                cv.notify_all();
                cv.wait(lock, [&] { return go_on; });
            }
        });
    });
    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock, [&] { return in_scan; });
    }
    // EXPECT, not ASSERT, until the join: a test that returned with the scan held would end the run.
    EXPECT_TRUE(eventually([&] { return engine->registry().counter_value("ob_compactions_total") == 1; }));
    EXPECT_TRUE(eventually([&] {
        return engine->registry().gauge_value("ob_segments_awaiting_removal") ==
               static_cast<int64_t>(ob::compaction::kFanIn);
    })) << "the merge's inputs did not wait for the scan";
    EXPECT_EQ(engine->drop_symbol("MRG.EX"), 1u) << "the merged segment";
    EXPECT_EQ(dirs_of(dir.path, "MRG"), 0u) << "the inputs of its merge stayed on the disk";
    {
        std::lock_guard<std::mutex> lock(m);
        go_on = true;
    }
    cv.notify_all();
    reader.join();
    engine->close();
    auto reopened = engine_at(dir.path);
    EXPECT_EQ(rows(*reopened, "MRG"), 0) << "a restart took the merge's inputs back";
    reopened->close();
}

TEST(SymbolMigration, NothingIsDroppedWhileASnapshotPinHoldsTheFiles) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    ASSERT_EQ(write(*engine, "PIN", 1'000), ob::OB_OK);
    {
        const auto pin = engine->pin_segment_files();
        EXPECT_THROW(engine->drop_symbol("PIN.EX"), std::runtime_error);
        EXPECT_TRUE(engine->holds_symbol("PIN.EX"));
        EXPECT_EQ(rows(*engine, "PIN"), 3) << "a refused drop removed rows";
    }
    EXPECT_EQ(engine->drop_symbol("PIN.EX"), 1u);
    EXPECT_FALSE(engine->holds_symbol("PIN.EX"));
    engine->close();
}

// ── The server's answers, and the coordinator's adoption ──────────────────────

namespace {

/// A map of two shards in which `owner` is assigned `symbol`.
ob::ShardMap two_shards(const std::string& symbol, const std::string& owner) {
    ob::ShardMap map;
    map.version = 1;
    for (const char* id : {"shard-0", "shard-1"}) {
        ob::ShardNode node;
        node.shard_id = id;
        node.address  = std::string("127.0.0.1:") + (id[6] == '0' ? "9090" : "9091");
        node.vnodes   = 150;
        node.status   = ob::ShardStatus::ACTIVE;
        map.shards[id] = node;
    }
    map.assignments[symbol] = owner;
    return map;
}

}  // namespace

class SymbolMigrationServer : public ::testing::Test {
protected:
    void SetUp() override {
        engine_ = engine_at(dir_.path);
        int fds[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        fd_server_ = fds[0];
        fd_client_ = fds[1];
        source_ = std::make_unique<ob::Session>(fd_server_, 1);
        other_  = std::make_unique<ob::Session>(fd_server_, 2);
        ob::ShardCoordinatorConfig cfg;
        cfg.shard_id = "shard-1";
        cfg.vnodes   = 150;
        coord_ = std::make_unique<ob::ShardCoordinator>(cfg, *engine_);
    }
    void TearDown() override {
        coord_.reset();
        engine_->close();
        ::close(fd_server_);
        ::close(fd_client_);
    }
    std::string run(ob::Session& session, const std::string& line, bool sharded = true) {
        return ob::execute_command(ob::parse_command(line), *engine_, session, stats_, false,
                                   nullptr, sharded ? coord_.get() : nullptr);
    }

    TempDir                               dir_;
    std::unique_ptr<ob::Engine>           engine_;
    ob::ServerStats                       stats_;
    int                                   fd_server_ = -1;
    int                                   fd_client_ = -1;
    std::unique_ptr<ob::Session>          source_;   ///< the migration's connection
    std::unique_ptr<ob::Session>          other_;    ///< any other client's
    std::unique_ptr<ob::ShardCoordinator> coord_;    ///< shard-1's
};

TEST_F(SymbolMigrationServer, AWriteOfAMovingSymbolIsAnsweredSymbolMoving) {
    engine_->freeze_symbol("MOV.EX");
    EXPECT_EQ(run(*other_, "INSERT MOV EX bid 100 1 1", false), "ERR SYMBOL_MOVING\n");
    engine_->thaw_symbol("MOV.EX");
    EXPECT_EQ(run(*other_, "INSERT MOV EX bid 100 1 1", false), "OK\n\n");
}

TEST_F(SymbolMigrationServer, AWriteOfAMigratedSymbolIsAnsweredSymbolMigrated) {
    // The engine refuses it under its lock; the answer used to be `apply_delta failed with code -9`
    // there, which no client reads - a check before the lock answered it, racing the migration.
    engine_->mark_symbol_migrated("GONE.EX");
    EXPECT_EQ(run(*other_, "INSERT GONE EX bid 100 1 1", false), "ERR SYMBOL_MIGRATED\n");
}

TEST_F(SymbolMigrationServer, AnAdoptedSymbolsWritesAreTakenFromTheConnectionThatBeganItAlone) {
    coord_->adopt_map_for_test(two_shards("ADP.EX", "shard-0"));
    EXPECT_EQ(run(*source_, "INSERT ADP EX bid 100 1 1"), "ERR NOT_OWNER ADP.EX\n");

    EXPECT_EQ(run(*source_, "ADOPT ADP.EX BEGIN shard-0"), "OK\n\n");
    EXPECT_TRUE(coord_->is_adopting("ADP.EX"));
    EXPECT_FALSE(coord_->owns_symbol("ADP.EX")) << "the map names shard-0 until the migration ends";
    EXPECT_EQ(run(*source_, "INSERT ADP EX bid 100 1 1"), "OK\n\n") << "the migration's copy";
    EXPECT_EQ(run(*other_, "INSERT ADP EX bid 101 1 1"), "ERR NOT_OWNER ADP.EX\n")
        << "a client the map sends to shard-0 wrote here, where an abandoned migration drops it";
    EXPECT_EQ(run(*other_, "ADOPT ADP.EX BEGIN shard-0").rfind("ERR already adopting", 0), 0u);

    // END before the map names this shard: taken from anyone, until a map that does.
    EXPECT_EQ(run(*source_, "ADOPT ADP.EX END"), "OK\n\n");
    EXPECT_TRUE(coord_->is_adopting("ADP.EX"));
    EXPECT_TRUE(coord_->owns_symbol("ADP.EX"));
    EXPECT_EQ(run(*other_, "INSERT ADP EX bid 102 1 1"), "OK\n\n") << "the map in etcd names this shard";
    EXPECT_EQ(run(*other_, "ADOPT ADP.EX ABANDON").rfind("ERR the adoption of ADP.EX has ended", 0), 0u);
    coord_->adopt_map_for_test(two_shards("ADP.EX", "shard-1"));
    EXPECT_FALSE(coord_->is_adopting("ADP.EX")) << "the map named this shard and the adoption stood";
    EXPECT_TRUE(coord_->owns_symbol("ADP.EX"));
    EXPECT_EQ(run(*other_, "INSERT ADP EX bid 103 1 1"), "OK\n\n");
    engine_->flush_incremental();
    EXPECT_EQ(rows(*engine_, "ADP"), 3);
}

TEST_F(SymbolMigrationServer, AWriteLetThroughBeforeAnAbandonIsAnsweredNotOwner) {
    // The server checks the adoption without the engine's lock; the engine checks it again under it.
    // The abandon here reaches the engine alone, so the server's check still passes - the window
    // between the two, held open.
    coord_->adopt_map_for_test(two_shards("RACE.EX", "shard-0"));
    ASSERT_EQ(run(*source_, "ADOPT RACE.EX BEGIN shard-0"), "OK\n\n");
    ASSERT_EQ(run(*source_, "INSERT RACE EX bid 100 1 1"), "OK\n\n");
    ASSERT_GT(engine_->abandon_adoption("RACE.EX"), 0u);
    EXPECT_EQ(run(*source_, "INSERT RACE EX bid 101 1 1"), "ERR NOT_OWNER RACE.EX\n");
    EXPECT_FALSE(engine_->holds_symbol("RACE.EX"));
}

TEST_F(SymbolMigrationServer, AnAbandonedAdoptionLeavesNothingAndIsBegunAgainOverNothing) {
    coord_->adopt_map_for_test(two_shards("LEFT.EX", "shard-0"));
    ASSERT_EQ(run(*source_, "ADOPT LEFT.EX BEGIN shard-0"), "OK\n\n");
    ASSERT_EQ(run(*source_, "INSERT LEFT EX bid 100 1 1"), "OK\n\n");
    EXPECT_EQ(run(*other_, "ADOPT LEFT.EX ABANDON"), "OK 1\n\n");
    EXPECT_FALSE(coord_->is_adopting("LEFT.EX"));
    EXPECT_FALSE(engine_->holds_symbol("LEFT.EX")) << "ABANDON left the adopted rows";
    EXPECT_EQ(run(*source_, "INSERT LEFT EX bid 100 1 1"), "ERR NOT_OWNER LEFT.EX\n")
        << "the abandoned adoption's connection still wrote";

    // Rows of it written some other way: a shard that holds them does not adopt it until ABANDON
    // drops them.
    ASSERT_EQ(write(*engine_, "LEFT", 9'000), ob::OB_OK);
    EXPECT_EQ(run(*source_, "ADOPT LEFT.EX BEGIN shard-0").rfind("ERR shard shard-1 holds rows of LEFT.EX", 0),
              0u);
    EXPECT_FALSE(coord_->is_adopting("LEFT.EX"));
    EXPECT_EQ(run(*other_, "ADOPT LEFT.EX ABANDON"), "OK 1\n\n");
    EXPECT_EQ(run(*source_, "ADOPT LEFT.EX BEGIN shard-0"), "OK\n\n");
}

TEST_F(SymbolMigrationServer, AShardDoesNotAdoptOrDropWhatItOwns) {
    coord_->adopt_map_for_test(two_shards("MINE.EX", "shard-1"));
    ASSERT_EQ(run(*other_, "INSERT MINE EX bid 100 1 1"), "OK\n\n");
    EXPECT_EQ(run(*source_, "ADOPT MINE.EX BEGIN shard-0").rfind("ERR shard shard-1 owns", 0), 0u);
    EXPECT_EQ(run(*source_, "ADOPT MINE.EX ABANDON").rfind("ERR shard shard-1 owns", 0), 0u);
    EXPECT_TRUE(engine_->holds_symbol("MINE.EX")) << "ABANDON dropped a symbol this shard owns";
}

TEST_F(SymbolMigrationServer, ADropRefusedIsAnsweredWithWhatStays) {
    coord_->adopt_map_for_test(two_shards("HELD.EX", "shard-0"));
    ASSERT_EQ(run(*source_, "ADOPT HELD.EX BEGIN shard-0"), "OK\n\n");
    ASSERT_EQ(run(*source_, "INSERT HELD EX bid 100 1 1"), "OK\n\n");
    {
        const auto pin = engine_->pin_segment_files();
        EXPECT_EQ(run(*other_, "ADOPT HELD.EX ABANDON").rfind("ERR the rows of HELD.EX stay", 0), 0u);
    }
    EXPECT_FALSE(coord_->is_adopting("HELD.EX")) << "the adoption is over either way";
    EXPECT_EQ(run(*source_, "INSERT HELD EX bid 101 1 1"), "ERR NOT_OWNER HELD.EX\n");
    EXPECT_EQ(run(*other_, "ADOPT HELD.EX ABANDON"), "OK 1\n\n");
    EXPECT_FALSE(engine_->holds_symbol("HELD.EX"));
}

TEST_F(SymbolMigrationServer, AMapThatNamesThisShardEndsAnAdoptionWhoseEndWasLost) {
    // The source writes the map once the rows are here and then sends END. An END lost on the way
    // left the adoption standing, and ABANDON then dropped what it adopted - the symbol's only copy
    // that takes writes, since the map names this shard for it.
    coord_->adopt_map_for_test(two_shards("LOST.EX", "shard-0"));
    ASSERT_EQ(run(*source_, "ADOPT LOST.EX BEGIN shard-0"), "OK\n\n");
    ASSERT_EQ(run(*source_, "INSERT LOST EX bid 100 1 1"), "OK\n\n");
    coord_->adopt_map_for_test(two_shards("LOST.EX", "shard-1"));
    EXPECT_FALSE(coord_->is_adopting("LOST.EX")) << "the map named this shard and the adoption stood";
    EXPECT_EQ(run(*other_, "INSERT LOST EX bid 101 1 1"), "OK\n\n");
    EXPECT_EQ(run(*other_, "ADOPT LOST.EX ABANDON").rfind("ERR shard shard-1 owns", 0), 0u);
    engine_->flush_incremental();
    EXPECT_EQ(rows(*engine_, "LOST"), 2) << "ABANDON dropped the symbol the map names this shard for";
}

TEST_F(SymbolMigrationServer, ASymbolTheRingGivesThisShardIsNotDroppedWhileAdopted) {
    // Owned by the ring rather than an assignment, which ends no adoption: ABANDON asks who owns the
    // symbol before it asks whether it is adopted.
    coord_->adopt_map_for_test(two_shards("RING.EX", "shard-0"));
    ASSERT_EQ(run(*source_, "ADOPT RING.EX BEGIN shard-0"), "OK\n\n");
    ASSERT_EQ(run(*source_, "INSERT RING EX bid 100 1 1"), "OK\n\n");
    ob::ShardMap alone = two_shards("OTHER.EX", "shard-1");   // RING.EX assigned to nobody
    alone.shards.erase("shard-0");                             // and the ring is shard-1's alone
    coord_->adopt_map_for_test(alone);
    ASSERT_TRUE(coord_->is_adopting("RING.EX"));
    ASSERT_TRUE(coord_->owns_symbol("RING.EX"));
    EXPECT_EQ(run(*other_, "ADOPT RING.EX ABANDON").rfind("ERR shard shard-1 owns", 0), 0u);
    EXPECT_TRUE(engine_->holds_symbol("RING.EX")) << "ABANDON dropped a symbol the ring gives this shard";
}

TEST_F(SymbolMigrationServer, AnAdoptionNeedsAnActiveShard) {
    // A shard the map does not name is joining, and owns and adopts nothing.
    ob::ShardMap map = two_shards("JOIN.EX", "shard-0");
    map.shards.erase("shard-1");
    coord_->adopt_map_for_test(map);
    EXPECT_EQ(run(*source_, "ADOPT JOIN.EX BEGIN shard-0"), "ERR shard shard-1 is not active\n");
    EXPECT_EQ(run(*source_, "ADOPT JOIN.EX END"), "ERR not adopting JOIN.EX\n");
}
