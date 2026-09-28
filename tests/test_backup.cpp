// A node's backup, taken and restored (#34).
//
// The engine's half - a cut without checksums - and the backup library's: the runner that takes a
// backup into a directory, the description it writes, the check of a backup against it, and the
// restore into an empty data directory through the engine's own start. What these hold, each beside
// a control that must pass: a backup holds exactly the cut's files, linked or copied, and verifies;
// the cut is one moment; merges after a linked backup do not change it; one backup at a time; a
// failure leaves nothing that looks like a backup; a restore answers exactly the backup's rows and
// numbers on from them, carries a mesh node's frontiers and the closed numbering, and refuses a
// target that is not empty, a backup that does not verify and a description it cannot trust, having
// written nothing.

#include "orderbook/backup.hpp"
#include "orderbook/compaction.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <nlohmann/json.hpp>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

std::atomic<uint64_t> g_dir_counter{0};
std::atomic<uint16_t> g_mm_port{47600};

struct TempDir {
    std::string path;
    TempDir() {
        path = (fs::temp_directory_path() /
                ("ob_backup_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed))))
                   .string();
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        // A test that took a directory's write permission gives it back here, or nothing under it
        // can be removed.
        for (auto it = fs::recursive_directory_iterator(path, ec);
             !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
            if (it->is_directory(ec)) fs::permissions(it->path(), fs::perms::owner_all,
                                                      fs::perm_options::add, ec);
        }
        fs::permissions(path, fs::perms::owner_all, fs::perm_options::add, ec);
        fs::remove_all(path, ec);
    }
};

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;
constexpr uint64_t kBase        = 1'790'000'000'000'000'000ULL;   // event times in 2026

std::unique_ptr<ob::Engine> engine_at(const std::string& dir, uint64_t flush_ns = kNoAutoFlush,
                                      ob::MultiMasterConfig mm = {}) {
    auto engine = std::make_unique<ob::Engine>(dir, flush_ns, ob::FsyncPolicy::INTERVAL,
                                               ob::ReplicationConfig{}, ob::ReplicationClientConfig{},
                                               ob::FailoverConfig{}, ob::TTLConfig{}, mm);
    engine->open();
    return engine;
}

ob::MultiMasterConfig mesh_node(uint16_t node_id) {
    ob::MultiMasterConfig mm{};
    mm.enabled                   = true;
    mm.node_id                   = node_id;
    mm.replication_port          = g_mm_port.fetch_add(1, std::memory_order_relaxed);
    mm.anti_entropy_interval_sec = 3600;
    return mm;
}

/// One update of three levels, at `kBase + n` microseconds.
void write(ob::Engine& engine, const char* symbol, uint64_t n) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.timestamp_ns = kBase + n * 1000;
    delta.side         = ob::SIDE_BID;
    delta.n_levels     = 3;
    ob::Level levels[3]{};
    for (int i = 0; i < 3; ++i) {
        levels[i].price = static_cast<int64_t>(n * 100 + i);
        levels[i].qty   = n + 1;
        levels[i].cnt   = 1;
    }
    ASSERT_EQ(engine.apply_delta(delta, levels), ob::OB_OK);
}

struct Row {
    uint64_t ts, seq;
    int64_t  price;
    uint64_t qty;
    uint16_t level;
    bool operator==(const Row& o) const {
        return ts == o.ts && seq == o.seq && price == o.price && qty == o.qty && level == o.level;
    }
};

std::vector<Row> rows_of(ob::Engine& engine, const char* symbol) {
    std::vector<Row> out;
    const std::string err = engine.execute(
        std::string("SELECT * FROM '") + symbol + "'.'EX'", [&](const ob::QueryResult& r) {
            out.push_back(Row{r.timestamp_ns, r.sequence_number, r.price, r.quantity, r.level});
        });
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) ADD_FAILURE() << err;
    return out;
}

/// Each symbol written `per_symbol` times, flushed after every `flush_every` updates.
void fill(ob::Engine& engine, const std::vector<const char*>& symbols, uint64_t per_symbol,
          uint64_t flush_every = 5) {
    for (uint64_t n = 0; n < per_symbol; ++n) {
        for (const char* s : symbols) write(engine, s, n);
        if ((n + 1) % flush_every == 0) engine.flush_incremental();
    }
}

template <typename F>
bool eventually(F&& done, std::chrono::milliseconds within = 20000ms) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return done();
}

ob::BackupProgress wait_for(const ob::BackupRunner& runner) {
    ob::BackupProgress p;
    EXPECT_TRUE(eventually([&] {
        p = runner.progress();
        return p.state != ob::BackupProgress::State::Running;
    })) << "the backup did not end";
    return p;
}

std::vector<std::string> entries(const std::string& dir) {
    std::vector<std::string> out;
    std::error_code ec;
    for (const auto& e : fs::directory_iterator(dir, ec)) out.push_back(e.path().filename().string());
    std::sort(out.begin(), out.end());
    return out;
}

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

void write_file(const std::string& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << content;
}

/// Take one backup and return its directory.
std::string take_backup(ob::Engine& engine, const std::string& backups, bool copy = false) {
    ob::BackupRunner runner(engine, backups, engine.registry());
    runner.force_copy_for_test(copy);
    std::string name;
    EXPECT_EQ(runner.start(name), ob::BackupRunner::Start::Started);
    const ob::BackupProgress p = wait_for(runner);
    EXPECT_EQ(p.state, ob::BackupProgress::State::Done) << p.error;
    return backups + "/" + name;
}

bool same_files(const std::vector<ob::SnapshotFileEntry>& a, std::vector<ob::SnapshotFileEntry> b) {
    auto by_path = [](const ob::SnapshotFileEntry& x, const ob::SnapshotFileEntry& y) {
        return x.path < y.path;
    };
    std::vector<ob::SnapshotFileEntry> sa = a;
    std::sort(sa.begin(), sa.end(), by_path);
    std::sort(b.begin(), b.end(), by_path);
    if (sa.size() != b.size()) return false;
    for (size_t i = 0; i < sa.size(); ++i) {
        if (sa[i].path != b[i].path || sa[i].size != b[i].size || sa[i].crc32c != b[i].crc32c) {
            ADD_FAILURE() << sa[i].path << " " << sa[i].size << " " << sa[i].crc32c << " vs " << b[i].path
                          << " " << b[i].size << " " << b[i].crc32c;
            return false;
        }
    }
    return true;
}

}  // namespace

// ── The cut without checksums ────────────────────────────────────────────────

TEST(BackupCut, WithoutChecksumsItListsTheSameFilesAndReadsNone) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A", "B"}, 20);

    const auto computed = engine->create_snapshot_with_sequence_state();
    fs::remove(dir.path + "/snapshot_manifest.json");
    const auto skipped =
        engine->create_snapshot_with_sequence_state(ob::SnapshotChecksums::Skip);

    ASSERT_FALSE(computed.manifest.files.empty());
    ASSERT_EQ(skipped.manifest.files.size(), computed.manifest.files.size());
    std::map<std::string, size_t> sizes;
    for (const auto& f : computed.manifest.files) sizes[f.path] = f.size;
    for (const auto& f : skipped.manifest.files) {
        EXPECT_EQ(f.crc32c, 0u) << f.path << " was read for its checksum";
        ASSERT_TRUE(sizes.count(f.path)) << f.path;
        EXPECT_EQ(f.size, sizes[f.path]) << f.path;
    }
    EXPECT_EQ(skipped.manifest.total_rows, computed.manifest.total_rows);
    EXPECT_EQ(skipped.manifest.total_bytes, computed.manifest.total_bytes);
    EXPECT_FALSE(fs::exists(dir.path + "/snapshot_manifest.json"))
        << "a manifest whose every CRC is zero was written as the data directory's snapshot manifest";
    // Control: the snapshot a replica is sent still checksums every file.
    EXPECT_TRUE(std::all_of(computed.manifest.files.begin(), computed.manifest.files.end(),
                            [](const auto& f) { return f.crc32c != 0; }));
    engine->close();
}

TEST(BackupCut, TheSealsAreWrittenWithoutTheEnginesLockAndHoldOnlyWhatTheCutDrained) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A"}, 10);
    for (uint64_t n = 10; n < 20; ++n) write(*engine, "A", n);   // waiting: the cut seals them

    // Inside the snapshot's seal writes, another thread writes. With the engine's lock held there it
    // could not until the seals were done - and they would be waiting for it.
    std::thread writer;
    std::atomic<bool> wrote{false};
    bool wrote_while_sealing = false;
    engine->while_sealing_for_test([&] {
        writer = std::thread([&] {
            write(*engine, "B", 1);
            wrote = true;
        });
        wrote_while_sealing = eventually([&] { return wrote.load(); }, 2000ms);
    });
    auto snap = engine->create_snapshot_with_sequence_state(ob::SnapshotChecksums::Skip);
    engine->while_sealing_for_test(nullptr);
    if (writer.joinable()) writer.join();
    EXPECT_TRUE(wrote_while_sealing) << "a writer waited for a snapshot's seals";
    // The cut is the moment before the seals: B, written during them, is in none of its files.
    for (const auto& f : snap.manifest.files) {
        EXPECT_NE(f.path.rfind("B/", 0), 0u) << f.path << " is a write taken after the cut";
    }
    EXPECT_EQ(snap.manifest.total_rows, 20u * 3u);
    snap.pin.reset();
    engine->flush_incremental();
    EXPECT_EQ(rows_of(*engine, "B").size(), 3u) << "the write taken during the seals was lost";
    engine->close();
}

// ── Names, paths, the directory ──────────────────────────────────────────────

TEST(BackupName, ReadsBackAsItsTimeAndSortsInTime) {
    const uint64_t t = 1'790'589'487'447'655'443ULL;   // 2026-09-28T09:58:07.447Z
    const std::string name = ob::backup_name_for(t);
    EXPECT_EQ(name, "20260928T095807.447Z");
    uint64_t back = 0;
    ASSERT_TRUE(ob::parse_backup_name(name, &back));
    EXPECT_EQ(back, t / 1'000'000ULL * 1'000'000ULL);
    EXPECT_LT(ob::backup_name_for(t), ob::backup_name_for(t + 1'000'000ULL));
    EXPECT_LT(ob::backup_name_for(t), ob::backup_name_for(t + 86'400'000'000'000ULL));
    for (const char* bad : {"", "20260928T095807.447", "20260928T095807447Z", "2026O928T095807.447Z",
                            ".partial-20260928T095807.447Z", "20261328T095807.447Z",
                            "20260928T255807.447Z"}) {
        EXPECT_FALSE(ob::parse_backup_name(bad, nullptr)) << bad;
    }
}

TEST(BackupPath, OnlyARelativePathThatStaysInsideIsAccepted) {
    EXPECT_TRUE(ob::backup_path_is_contained("BTC/BIN/1_2/ts.col"));
    EXPECT_TRUE(ob::backup_path_is_contained("meta.json"));
    for (const char* bad : {"", "/etc/passwd", "a//b", "./a", "a/./b", "a/../b", "..", "../a", "a/..",
                            "a/"}) {
        EXPECT_FALSE(ob::backup_path_is_contained(bad)) << bad;
    }
}

TEST(BackupDir, TheDataAndWalDirectoriesAndEverythingNestedEitherWayAreRefused) {
    TempDir root;
    const std::string data = root.path + "/data", wal = root.path + "/wal";
    fs::create_directories(data);
    fs::create_directories(wal);
    EXPECT_FALSE(ob::backup_dir_problem(data, data, "").empty());
    EXPECT_FALSE(ob::backup_dir_problem(data + "/backups", data, "").empty());
    EXPECT_FALSE(ob::backup_dir_problem(root.path, data, "").empty()) << "a backup dir holding the data";
    EXPECT_FALSE(ob::backup_dir_problem(wal + "/b", data, wal).empty());
    fs::create_directory_symlink(data, root.path + "/link");
    EXPECT_FALSE(ob::backup_dir_problem(root.path + "/link/b", data, "").empty())
        << "a symbolic link let the backup directory into the data directory";
    write_file(root.path + "/file", "x");
    EXPECT_FALSE(ob::backup_dir_problem(root.path + "/file", data, "").empty());
    fs::create_directories(root.path + "/ro");
    fs::permissions(root.path + "/ro", fs::perms::owner_read | fs::perms::owner_exec);
    if (::geteuid() != 0) {
        EXPECT_FALSE(ob::backup_dir_problem(root.path + "/ro", data, "").empty());
    }
    // Control: a sibling of both, created when missing.
    EXPECT_EQ(ob::backup_dir_problem(root.path + "/backups/node-1", data, wal), "");
    EXPECT_TRUE(fs::is_directory(root.path + "/backups/node-1"));
}

// ── Taking one ───────────────────────────────────────────────────────────────

TEST(BackupRunner, ALinkedBackupHoldsEveryFileOfTheCutAndVerifies) {
    TempDir dir, backups;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A", "B", "C"}, 30);

    const std::string backup = take_backup(*engine, backups.path);
    ob::BackupDescription d;
    std::string error;
    ASSERT_TRUE(ob::read_backup_description(backup, d, error)) << error;
    EXPECT_EQ(d.method, "linked");
    EXPECT_EQ(d.role, "standalone");
    EXPECT_EQ(d.wal_identity, engine->wal_identity());
    EXPECT_EQ(d.total_rows, 3u * 30u * 3u);
    EXPECT_TRUE(ob::verify_backup(backup, d).empty());
    // The same files, sizes and checksums a snapshot sent to a replica would list now.
    EXPECT_TRUE(same_files(d.files, engine->create_snapshot_with_sequence_state().manifest.files));
    EXPECT_EQ(entries(backups.path), std::vector<std::string>{d.name})
        << "a backup that is complete left its partial directory, or another entry, beside it";
    EXPECT_EQ(engine->registry().counter_value("ob_backups_total"), 1u);
    EXPECT_EQ(engine->registry().gauge_value("ob_backup_running"), 0);
    EXPECT_EQ(engine->registry().gauge_value("ob_backup_last_success_timestamp_seconds"),
              static_cast<int64_t>(d.cut_at_ns / 1'000'000'000ULL));
    struct stat st{};
    ASSERT_EQ(::stat((backup + "/store/" + d.files[0].path).c_str(), &st), 0);
    EXPECT_GE(st.st_nlink, 2u) << "a linked backup whose file is not a hard link";
    engine->close();
}

TEST(BackupRunner, ACopiedBackupVerifiesAndSharesNoInodeWithTheData) {
    TempDir dir, backups;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A", "B"}, 25);

    const std::string backup = take_backup(*engine, backups.path, /*copy=*/true);
    ob::BackupDescription d;
    std::string error;
    ASSERT_TRUE(ob::read_backup_description(backup, d, error)) << error;
    EXPECT_EQ(d.method, "copied");
    EXPECT_TRUE(ob::verify_backup(backup, d).empty());
    EXPECT_TRUE(same_files(d.files, engine->create_snapshot_with_sequence_state().manifest.files));
    for (const auto& f : d.files) {
        struct stat st{};
        ASSERT_EQ(::stat((backup + "/store/" + f.path).c_str(), &st), 0);
        EXPECT_EQ(st.st_nlink, 1u) << f.path;
    }
    engine->close();
}

TEST(BackupRunner, TheCutIsOneMomentAndWritesAfterItAreNotInTheBackup) {
    TempDir dir, backups, restored;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A", "B"}, 10);
    const auto a_at_cut = rows_of(*engine, "A");
    const auto b_at_cut = rows_of(*engine, "B");

    ob::BackupRunner runner(*engine, backups.path, engine->registry());
    // After the cut, with the files still pinned: every write here lands after it.
    runner.hold_after_cut_for_test([&] {
        for (uint64_t n = 10; n < 20; ++n) write(*engine, "A", n);
        write(*engine, "D", 1);
        engine->flush_incremental();
    });
    std::string name;
    ASSERT_EQ(runner.start(name), ob::BackupRunner::Start::Started);
    ASSERT_EQ(wait_for(runner).state, ob::BackupProgress::State::Done);
    ASSERT_EQ(rows_of(*engine, "A").size(), 20u * 3u) << "the writes after the cut were not taken";

    ob::RestoreReport report;
    std::string error;
    ASSERT_TRUE(ob::restore_backup(backups.path + "/" + name, restored.path + "/data", "", report,
                                   error))
        << error;
    auto back = engine_at(restored.path + "/data");
    EXPECT_EQ(rows_of(*back, "A"), a_at_cut);
    EXPECT_EQ(rows_of(*back, "B"), b_at_cut);
    EXPECT_TRUE(rows_of(*back, "D").empty());
    back->close();
    engine->close();
}

TEST(BackupRunner, MergesAfterALinkedBackupDoNotChangeIt) {
    TempDir dir, backups;
    // A tick every 20 ms, so the eighth seal of A is merged with the seven before it.
    auto engine = std::make_unique<ob::Engine>(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine->open();
    for (uint64_t n = 0; n + 1 < ob::compaction::kFanIn; ++n) {
        write(*engine, "A", n);
        engine->flush_incremental();
    }
    const std::string backup = take_backup(*engine, backups.path);
    ob::BackupDescription d;
    std::string error;
    ASSERT_TRUE(ob::read_backup_description(backup, d, error)) << error;
    const size_t segments_in_backup = std::count_if(d.files.begin(), d.files.end(), [](const auto& f) {
        return fs::path(f.path).filename() == "meta.json";
    });
    ASSERT_EQ(segments_in_backup, ob::compaction::kFanIn - 1);

    write(*engine, "A", 100);
    engine->flush_incremental();
    ASSERT_TRUE(eventually([&] {
        return engine->registry().counter_value("ob_compactions_total") >= 1 &&
               engine->registry().gauge_value("ob_segments_awaiting_removal") == 0;
    })) << "the seals were not merged, so this test did not test what it says";
    // The data directory's names of the seven are gone; the backup's links still hold them.
    size_t still_named = 0;
    for (const auto& f : d.files) still_named += fs::exists(dir.path + "/" + f.path) ? 1 : 0;
    EXPECT_EQ(still_named, 0u);
    EXPECT_TRUE(ob::verify_backup(backup, d).empty());
    engine->close();
}

TEST(BackupRunner, AMergeWaitsForTheBackupsPinAndGoesOnAfterIt) {
    TempDir dir, backups;
    auto engine = std::make_unique<ob::Engine>(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine->open();
    for (uint64_t n = 0; n + 1 < ob::compaction::kFanIn; ++n) {
        write(*engine, "A", n);
        engine->flush_incremental();
    }
    ob::BackupRunner runner(*engine, backups.path, engine->registry());
    bool merged_under_the_pin = false;
    runner.hold_after_cut_for_test([&] {
        // The eighth seal, after the cut: a merge of all eight is due, and would remove the seven
        // segments the backup is about to link. A second of ticks to take it in.
        write(*engine, "A", 100);
        engine->flush_incremental();
        merged_under_the_pin = eventually(
            [&] { return engine->registry().counter_value("ob_compactions_total") >= 1; }, 1000ms);
    });
    std::string name;
    ASSERT_EQ(runner.start(name), ob::BackupRunner::Start::Started);
    const ob::BackupProgress p = wait_for(runner);
    EXPECT_FALSE(merged_under_the_pin) << "a tick merged the segments the backup had listed";
    EXPECT_EQ(p.state, ob::BackupProgress::State::Done) << p.error;
    ob::BackupDescription d;
    std::string error;
    ASSERT_TRUE(ob::read_backup_description(backups.path + "/" + name, d, error)) << error;
    EXPECT_EQ(std::count_if(d.files.begin(), d.files.end(),
                            [](const auto& f) { return fs::path(f.path).filename() == "meta.json"; }),
              static_cast<long>(ob::compaction::kFanIn - 1));
    // Released: the merge goes on.
    EXPECT_TRUE(eventually([&] {
        return engine->registry().counter_value("ob_compactions_total") >= 1;
    })) << "the backup's pin was never released";
    engine->close();
}

TEST(BackupRunner, OneAtATimeAndTheSecondIsToldWhichIsRunning) {
    TempDir dir, backups;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A"}, 10);
    ob::BackupRunner runner(*engine, backups.path, engine->registry());
    std::mutex m;
    std::condition_variable cv;
    bool release = false;
    runner.hold_after_cut_for_test([&] {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock, [&] { return release; });
    });
    std::string first, second;
    ASSERT_EQ(runner.start(first), ob::BackupRunner::Start::Started);
    EXPECT_EQ(runner.start(second), ob::BackupRunner::Start::Running);
    EXPECT_EQ(second, first);
    {
        std::lock_guard<std::mutex> lock(m);
        release = true;
    }
    cv.notify_all();
    EXPECT_EQ(wait_for(runner).state, ob::BackupProgress::State::Done);
    // Control: once it is done, the next one begins.
    runner.hold_after_cut_for_test(nullptr);
    std::this_thread::sleep_for(2ms);   // a name is the time to the millisecond
    std::string third;
    EXPECT_EQ(runner.start(third), ob::BackupRunner::Start::Started);
    EXPECT_NE(third, first);
    EXPECT_EQ(wait_for(runner).state, ob::BackupProgress::State::Done);
    engine->close();
}

TEST(BackupRunner, ADirectoryItCannotWriteFailsTheBackupAndLeavesNothing) {
    TempDir dir, backups;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A"}, 10);
    if (::geteuid() == 0) GTEST_SKIP() << "root writes to a read-only directory";
    ob::BackupRunner runner(*engine, backups.path, engine->registry());
    fs::permissions(backups.path, fs::perms::owner_read | fs::perms::owner_exec);
    std::string name;
    ASSERT_EQ(runner.start(name), ob::BackupRunner::Start::Started);
    const ob::BackupProgress p = wait_for(runner);
    EXPECT_EQ(p.state, ob::BackupProgress::State::Failed);
    EXPECT_NE(p.error.find("cannot create"), std::string::npos) << p.error;
    fs::permissions(backups.path, fs::perms::owner_all);
    EXPECT_TRUE(entries(backups.path).empty());
    EXPECT_EQ(engine->registry().counter_value("ob_backup_failures_total"), 1u);
    EXPECT_EQ(engine->registry().counter_value("ob_backups_total"), 0u);
    engine->close();
}

TEST(BackupRunner, AStopDuringTheBackupFailsItAndRemovesWhatItWrote) {
    TempDir dir, backups;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A", "B"}, 10);
    ob::BackupRunner runner(*engine, backups.path, engine->registry());
    runner.hold_after_cut_for_test([&] {
        EXPECT_EQ(entries(backups.path).size(), 1u);   // its partial directory, while it runs
        runner.request_stop();
    });
    std::string name;
    ASSERT_EQ(runner.start(name), ob::BackupRunner::Start::Started);
    const ob::BackupProgress p = wait_for(runner);
    EXPECT_EQ(p.state, ob::BackupProgress::State::Failed);
    EXPECT_EQ(p.error, "stopped");
    EXPECT_TRUE(entries(backups.path).empty()) << "a stopped backup left " << entries(backups.path)[0];
    engine->close();
}

TEST(BackupRunner, ANameAlreadyTakenFailsWithoutReplacingWhatHasIt) {
    TempDir dir, backups;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A"}, 5);
    ob::BackupRunner runner(*engine, backups.path, engine->registry());
    fs::create_directories(backups.path + "/20260928T100000.000Z");
    write_file(backups.path + "/20260928T100000.000Z/keep", "not the runner's");
    runner.set_next_name_for_test("20260928T100000.000Z");
    std::string name;
    ASSERT_EQ(runner.start(name), ob::BackupRunner::Start::Started);
    const ob::BackupProgress p = wait_for(runner);
    EXPECT_EQ(p.state, ob::BackupProgress::State::Failed);
    EXPECT_EQ(read_file(backups.path + "/20260928T100000.000Z/keep"), "not the runner's");
    EXPECT_EQ(entries(backups.path), std::vector<std::string>{"20260928T100000.000Z"});
    engine->close();
}

TEST(BackupRunner, AtStartTheNewestCompleteBackupIsTheLastSuccess) {
    TempDir dir, backups;
    auto engine = engine_at(dir.path);
    for (const char* name : {"20260927T100000.000Z", "20260928T090000.500Z"}) {
        fs::create_directories(backups.path + "/" + name);
        write_file(backups.path + "/" + name + "/backup.json", "{}");
    }
    // Newer, and not a backup: no description, or not complete.
    fs::create_directories(backups.path + "/20260928T100000.000Z");
    fs::create_directories(backups.path + "/.partial-20260928T110000.000Z");
    ob::BackupRunner runner(*engine, backups.path, engine->registry());
    uint64_t ns = 0;
    ASSERT_TRUE(ob::parse_backup_name("20260928T090000.500Z", &ns));
    EXPECT_EQ(engine->registry().gauge_value("ob_backup_last_success_timestamp_seconds"),
              static_cast<int64_t>(ns / 1'000'000'000ULL));
    EXPECT_EQ(runner.progress().state, ob::BackupProgress::State::Idle);
    engine->close();
}

TEST(BackupStatus, EveryFieldOnALineOfItsOwnAndNoLineBreakInsideOne) {
    const std::string idle = ob::format_backup_status(ob::BackupProgress{});
    EXPECT_EQ(idle,
              "OK\nstate: idle\nname: -\nphase: -\nmethod: -\nfiles: 0/0\nbytes: 0/0\ncut_ms: 0\n"
              "pinned_ms: 0\nelapsed_ms: 0\nerror: -\n\n");
    ob::BackupProgress failed;
    failed.state = ob::BackupProgress::State::Failed;
    failed.name  = "20260928T100000.000Z";
    failed.error = "cannot write\nERR injected";
    const std::string text = ob::format_backup_status(failed);
    EXPECT_NE(text.find("error: cannot write ERR injected\n"), std::string::npos) << text;
    EXPECT_EQ(text.find("\nERR"), std::string::npos) << text;
}

// ── Restoring one ────────────────────────────────────────────────────────────

TEST(Restore, TheRestoredNodeAnswersExactlyTheBackupsRowsAndNumbersOnFromThem) {
    TempDir dir, backups, target;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A", "B", "C"}, 30, 7);
    write(*engine, "A", 30);   // in no segment yet: the cut seals it
    const std::string backup = take_backup(*engine, backups.path);
    std::map<std::string, std::vector<Row>> before;
    for (const char* s : {"A", "B", "C"}) before[s] = rows_of(*engine, s);
    engine->close();

    ob::RestoreReport report;
    std::string error;
    ASSERT_TRUE(ob::restore_backup(backup, target.path + "/data", "", report, error)) << error;
    EXPECT_TRUE(report.sequence_adopted);
    EXPECT_NE(report.wal_identity, 0u);
    // The restored WAL begins at its second file: a replica asking for the log from the start is
    // sent a snapshot, which holds the rows this WAL does not.
    EXPECT_FALSE(fs::exists(target.path + "/data/wal_000000.bin"));
    EXPECT_TRUE(fs::exists(target.path + "/data/wal_000001.bin"));
    auto back = engine_at(target.path + "/data");
    EXPECT_NE(back->wal_identity(), engine->wal_identity()) << "the restored node kept the old WAL's identity";
    for (const char* s : {"A", "B", "C"}) EXPECT_EQ(rows_of(*back, s), before[s]) << s;
    // The next write is numbered after everything the backup holds.
    uint64_t highest = 0;
    for (const auto& r : before["A"]) highest = std::max(highest, r.seq);
    write(*back, "A", 31);
    back->flush_incremental();
    const auto after = rows_of(*back, "A");
    ASSERT_EQ(after.size(), before["A"].size() + 3);
    EXPECT_GT(after.back().seq, highest);
    back->close();
}

TEST(Restore, ARestoreWithItsOwnWalDirectoryPutsTheWalThere) {
    TempDir dir, backups, target;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A"}, 10);
    const std::string backup = take_backup(*engine, backups.path);
    const auto before = rows_of(*engine, "A");
    engine->close();

    ob::RestoreReport report;
    std::string error;
    ASSERT_TRUE(ob::restore_backup(backup, target.path + "/data", target.path + "/wal", report, error))
        << error;
    bool wal_there = false;
    for (const auto& e : entries(target.path + "/wal")) wal_there |= e.rfind("wal_", 0) == 0;
    EXPECT_TRUE(wal_there) << "no WAL in the WAL directory";
    auto back = std::make_unique<ob::Engine>(target.path + "/data", kNoAutoFlush, ob::FsyncPolicy::INTERVAL,
                                             ob::ReplicationConfig{}, ob::ReplicationClientConfig{},
                                             ob::FailoverConfig{}, ob::TTLConfig{}, ob::MultiMasterConfig{},
                                             512ULL << 20, target.path + "/wal");
    back->open();
    EXPECT_EQ(rows_of(*back, "A"), before);
    back->close();
}

TEST(Restore, ATargetThatIsNotEmptyIsRefusedAndLeftAsItWas) {
    TempDir dir, backups, target;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A"}, 5);
    const std::string backup = take_backup(*engine, backups.path);
    engine->close();

    fs::create_directories(target.path + "/data");
    write_file(target.path + "/data/somebody's", "x");
    ob::RestoreReport report;
    std::string error;
    EXPECT_FALSE(ob::restore_backup(backup, target.path + "/data", "", report, error));
    EXPECT_NE(error.find("not empty"), std::string::npos) << error;
    EXPECT_EQ(entries(target.path + "/data"), std::vector<std::string>{"somebody's"});

    fs::create_directories(target.path + "/wal");
    write_file(target.path + "/wal/wal_000000.bin", "old");
    EXPECT_FALSE(ob::restore_backup(backup, target.path + "/data2", target.path + "/wal", report, error));
    EXPECT_NE(error.find("not empty"), std::string::npos) << error;
    EXPECT_FALSE(fs::exists(target.path + "/data2"));
    // Control: an empty one that exists is a target.
    fs::create_directories(target.path + "/data3");
    EXPECT_TRUE(ob::restore_backup(backup, target.path + "/data3", "", report, error)) << error;
}

TEST(Restore, ABackupThatDoesNotVerifyWritesNothing) {
    TempDir dir, backups, target;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A", "B"}, 10);
    const std::string backup = take_backup(*engine, backups.path, /*copy=*/true);
    engine->close();
    ob::BackupDescription d;
    std::string error;
    ASSERT_TRUE(ob::read_backup_description(backup, d, error)) << error;

    // One byte of one file changed.
    const std::string victim = backup + "/store/" + d.files.back().path;
    std::string bytes = read_file(victim);
    ASSERT_FALSE(bytes.empty());
    bytes[bytes.size() / 2] = static_cast<char>(bytes[bytes.size() / 2] ^ 0x01);
    write_file(victim, bytes);
    auto problems = ob::verify_backup(backup, d);
    ASSERT_EQ(problems.size(), 1u);
    EXPECT_NE(problems[0].find("CRC32C"), std::string::npos) << problems[0];
    ob::RestoreReport report;
    EXPECT_FALSE(ob::restore_backup(backup, target.path + "/data", "", report, error));
    EXPECT_FALSE(fs::exists(target.path + "/data")) << "a backup that does not verify created the target";

    // And one missing.
    fs::remove(backup + "/store/" + d.files.front().path);
    problems = ob::verify_backup(backup, d);
    EXPECT_EQ(problems.size(), 2u);
    EXPECT_NE(problems[0].find("missing"), std::string::npos) << problems[0];
    EXPECT_FALSE(ob::restore_backup(backup, target.path + "/data", "", report, error));
    EXPECT_FALSE(fs::exists(target.path + "/data"));
}

TEST(Restore, ADescriptionItCannotTrustIsRefused) {
    TempDir dir, backups;
    auto engine = engine_at(dir.path);
    fill(*engine, {"A"}, 5);
    const std::string backup = take_backup(*engine, backups.path);
    engine->close();
    const std::string original = read_file(backup + "/backup.json");
    ob::BackupDescription d;
    std::string error;
    ASSERT_TRUE(ob::BackupDescription::from_json(original, d, error)) << error;
    const std::string first = d.files.front().path;

    auto refused = [&](const std::string& text, const char* expect) {
        ob::BackupDescription out;
        std::string why;
        EXPECT_FALSE(ob::BackupDescription::from_json(text, out, why)) << expect;
        EXPECT_NE(why.find(expect), std::string::npos) << why;
    };
    auto replaced = [&](const std::string& from, const std::string& to) {
        std::string t = original;
        const auto at = t.find(from);
        EXPECT_NE(at, std::string::npos) << from;
        return at == std::string::npos ? t : t.replace(at, from.size(), to);
    };
    refused(replaced("\"format_version\": 1", "\"format_version\": 2"), "format version 2");
    refused(replaced("\"orderbook-backup\"", "\"something-else\""), "\"format\"");
    refused(replaced("\"" + first + "\"", "\"../../elsewhere\""), "leaves the directory");
    refused(replaced("\"" + first + "\"", "\"/etc/passwd\""), "leaves the directory");
    refused(replaced("\"total_bytes\": ", "\"total_bytes\": 1"), "add up to");
    refused(replaced("\"method\": \"linked\"", "\"method\": \"moved\""), "neither linked nor copied");
    refused("[1, 2]", "not a JSON object");
    refused("{\"format\": ", "not JSON");
    {
        // The same path twice.
        nlohmann::json j = nlohmann::json::parse(original);
        j["files"].push_back(j["files"][0]);
        j["total_bytes"] = j["total_bytes"].get<uint64_t>() + j["files"][0]["size"].get<uint64_t>();
        refused(j.dump(), "listed twice");
    }
    // Control: the description as written reads back as what was written.
    ob::BackupDescription again;
    EXPECT_TRUE(ob::BackupDescription::from_json(d.to_json(), again, error)) << error;
    EXPECT_EQ(again.to_json(), d.to_json());
}

TEST(Restore, AMeshNodesBackupCarriesItsFrontiersAndTheClosedNumbering) {
    TempDir dir, backups, target;
    const uint16_t node = 5;
    std::vector<ob::SequenceTracker::VectorEntry> vector_at_cut;
    std::vector<std::pair<std::string, uint64_t>> rows_at_cut;
    std::string backup;
    {
        auto engine = engine_at(dir.path, kNoAutoFlush, mesh_node(node));
        fill(*engine, {"A", "B"}, 10);
        // What a peer's records leave in the tracker: a frontier of origin 7 for B, with no
        // segment of origin 7 anywhere - so only the carried state can give it back.
        auto state = engine->create_snapshot_with_sequence_state(ob::SnapshotChecksums::Skip);
        state.vector.push_back({"B.EX", 7, 42});
        state.pin.reset();
        engine->adopt_snapshot_sequence_state(state.vector, state.held);
        write_file(dir.path + "/" + ob::kNumberingClosedFile, "closed\n");
        backup = take_backup(*engine, backups.path);
        vector_at_cut = engine->create_snapshot_with_sequence_state(ob::SnapshotChecksums::Skip).vector;
        engine->close();
    }
    ob::BackupDescription d;
    std::string error;
    ASSERT_TRUE(ob::read_backup_description(backup, d, error)) << error;
    EXPECT_EQ(d.role, "multi_master");
    EXPECT_EQ(d.mm_node_id, node);
    EXPECT_TRUE(d.numbering_closed);

    ob::RestoreReport report;
    ASSERT_TRUE(ob::restore_backup(backup, target.path + "/data", "", report, error)) << error;
    EXPECT_TRUE(fs::exists(target.path + "/data/" + ob::kNumberingClosedFile));
    auto back = engine_at(target.path + "/data", kNoAutoFlush, mesh_node(node));
    auto restored = back->create_snapshot_with_sequence_state(ob::SnapshotChecksums::Skip).vector;
    auto by_key = [](const auto& a, const auto& b) {
        return std::tie(a.key, a.origin) < std::tie(b.key, b.origin);
    };
    std::sort(vector_at_cut.begin(), vector_at_cut.end(), by_key);
    std::sort(restored.begin(), restored.end(), by_key);
    ASSERT_EQ(restored.size(), vector_at_cut.size());
    for (size_t i = 0; i < restored.size(); ++i) {
        EXPECT_EQ(restored[i].key, vector_at_cut[i].key);
        EXPECT_EQ(restored[i].origin, vector_at_cut[i].origin);
        EXPECT_EQ(restored[i].frontier, vector_at_cut[i].frontier) << restored[i].key << " origin "
                                                                     << restored[i].origin;
    }
    const bool has_seven = std::any_of(restored.begin(), restored.end(), [](const auto& e) {
        return e.origin == 7 && e.frontier == 42;
    });
    EXPECT_TRUE(has_seven) << "the frontier of a peer's records was not carried";
    back->close();
}

TEST(Restore, AMeshBackupWithATruncatedVectorIsRefused) {
    TempDir dir, backups, target;
    std::string backup;
    {
        auto engine = engine_at(dir.path, kNoAutoFlush, mesh_node(6));
        fill(*engine, {"A"}, 5);
        backup = take_backup(*engine, backups.path);
        engine->close();
    }
    std::string text = read_file(backup + "/backup.json");
    const std::string from = "\"vector_truncated\": false";
    ASSERT_NE(text.find(from), std::string::npos);
    text.replace(text.find(from), from.size(), "\"vector_truncated\": true");
    write_file(backup + "/backup.json", text);
    ob::RestoreReport report;
    std::string error;
    EXPECT_FALSE(ob::restore_backup(backup, target.path + "/data", "", report, error));
    EXPECT_NE(error.find("bootstrap the node from a peer"), std::string::npos) << error;
    EXPECT_FALSE(fs::exists(target.path + "/data"));
}
