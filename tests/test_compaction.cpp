// #165 part 2b, the engine's half: the flush tick merges a symbol's small segments.
//
// The store's tests hold what a merge reads, writes and swaps; these hold the engine's wiring of it:
// eight seals of a symbol become one segment within a few ticks and every row answers once and in
// the order it did, fewer than eight wait while the partition may still grow, a scan reading the
// inputs keeps their files until it ends, a snapshot's pin stops merges and the retention sweep, the
// valve turns merging off, a restart after a merge answers the same, and an idle node under the
// default policy finishes a merge with syncs of its own.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
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
                ("ob_compaction_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1)))).string()) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

constexpr uint64_t kBase = 1'790'000'000'000'000'000ULL;
constexpr size_t kLevels = 50;

void insert(ob::Engine& engine, const char* symbol, uint64_t seq, size_t levels = kLevels) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.sequence_number = seq;
    delta.timestamp_ns    = kBase + seq * 1'000'000ULL;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = static_cast<uint16_t>(levels);
    std::vector<ob::Level> lv(levels);
    for (size_t i = 0; i < levels; ++i) {
        lv[i] = ob::Level{};
        lv[i].price = static_cast<int64_t>(seq * 1000 + i);
        lv[i].qty   = seq;
        lv[i].cnt   = 1;
    }
    ASSERT_EQ(engine.apply_delta(delta, lv.data()), ob::OB_OK);
}

/// `n` seals of one symbol, each a FLUSH of one update: `n` segments of `kLevels` rows.
void seal_n(ob::Engine& engine, const char* symbol, uint64_t first_seq, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        insert(engine, symbol, first_seq + i);
        engine.flush_incremental();
    }
}

/// Every meta.json under `dir`, read while the flush thread removes directories: a walk that meets a
/// directory going away starts again rather than throwing - which a range-for over the iterator
/// does, from its increment.
std::vector<fs::path> meta_files(const std::string& dir) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        std::vector<fs::path> out;
        std::error_code ec;
        fs::recursive_directory_iterator it(dir, ec), end;
        while (!ec && it != end) {
            if (it->path().filename() == "meta.json") out.push_back(it->path());
            it.increment(ec);
        }
        if (!ec) return out;
    }
    ADD_FAILURE() << "could not walk " << dir;
    return {};
}

size_t segments_on_disk(const std::string& dir, const std::string& symbol = "") {
    size_t n = 0;
    for (const auto& path : meta_files(dir)) {
        if (!symbol.empty() && path.string().find("/" + symbol + "/") == std::string::npos) continue;
        ++n;
    }
    return n;
}

struct Row {
    uint64_t ts;
    int64_t price;
    uint16_t level;
    bool operator==(const Row& o) const {
        return ts == o.ts && price == o.price && level == o.level;
    }
};

std::vector<Row> rows_answered(ob::Engine& engine, const char* symbol) {
    std::vector<Row> out;
    const std::string err = engine.execute(
        std::string("SELECT * FROM '") + symbol + "'.'EX'",
        [&](const ob::QueryResult& r) { out.push_back(Row{r.timestamp_ns, r.price, r.level}); });
    EXPECT_TRUE(err.empty()) << err;
    return out;
}

/// What `seal_n()` wrote, in the order it was written.
std::vector<Row> rows_written(uint64_t first_seq, size_t n) {
    std::vector<Row> out;
    for (uint64_t seq = first_seq; seq < first_seq + n; ++seq) {
        for (size_t i = 0; i < kLevels; ++i) {
            out.push_back(Row{kBase + seq * 1'000'000ULL, static_cast<int64_t>(seq * 1000 + i),
                              static_cast<uint16_t>(i)});
        }
    }
    return out;
}

template <typename F>
bool eventually(F&& done, std::chrono::milliseconds within = 10000ms) {
    const auto deadline = Clock::now() + within;
    while (Clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return done();
}

std::string merged_meta(const std::string& dir) {
    for (const auto& path : meta_files(dir)) {
        std::ifstream f(path);
        const std::string json((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        if (json.find("\"merge_level\":") != std::string::npos) return json;
    }
    return {};
}

}  // namespace

TEST(Compaction, EightSealsOfASymbolMergeIntoOneAndEveryRowAnswersInOrder) {
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    seal_n(engine, "A", 1, ob::compaction::kFanIn);
    seal_n(engine, "B", 1, 2);   // another symbol's two, which stay as they are
    const auto answered_before = rows_answered(engine, "A");
    ASSERT_EQ(answered_before, rows_written(1, ob::compaction::kFanIn));

    ASSERT_TRUE(eventually([&] { return segments_on_disk(dir.path, "A") == 1; }))
        << segments_on_disk(dir.path, "A") << " segment(s) of A: the eight were not merged, or their "
                                                "files were not removed";
    EXPECT_EQ(rows_answered(engine, "A"), rows_written(1, ob::compaction::kFanIn))
        << "the merge changed what a query answers, or the order it answers it in";
    EXPECT_EQ(segments_on_disk(dir.path, "B"), 2u);
    EXPECT_EQ(engine.registry().counter_value("ob_compactions_total"), 1u);
    EXPECT_EQ(engine.registry().counter_value("ob_compaction_inputs_total"), ob::compaction::kFanIn);
    EXPECT_EQ(engine.registry().counter_value("ob_compaction_rows_total"),
              ob::compaction::kFanIn * kLevels);
    EXPECT_TRUE(eventually([&] {
        return engine.registry().gauge_value("ob_segments_awaiting_removal") == 0;
    }));
    const std::string json = merged_meta(dir.path);
    EXPECT_NE(json.find("\"merge_level\":1"), std::string::npos) << json;
    EXPECT_NE(json.find("\"compacted_from\":["), std::string::npos) << json;
    engine.close();
}

TEST(Compaction, FewerThanTheFanInWaitWhileThePartitionMayGrow) {
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    seal_n(engine, "A", 1, ob::compaction::kFanIn - 1);
    // Many ticks: nothing settles a partition sealed into within kSettle.
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(segments_on_disk(dir.path, "A"), ob::compaction::kFanIn - 1);
    EXPECT_EQ(engine.registry().counter_value("ob_compactions_total"), 0u);
    // The eighth makes a merge.
    seal_n(engine, "A", ob::compaction::kFanIn, 1);
    EXPECT_TRUE(eventually([&] { return segments_on_disk(dir.path, "A") == 1; }));
    engine.close();
}

TEST(Compaction, AScanReadingTheInputsKeepsTheirFilesUntilItEnds) {
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    seal_n(engine, "A", 1, ob::compaction::kFanIn);

    std::mutex m;
    std::condition_variable cv;
    bool in_scan = false;
    bool go_on = false;
    std::vector<Row> seen;
    std::thread reader([&] {
        const std::string err = engine.execute("SELECT * FROM 'A'.'EX'", [&](const ob::QueryResult& r) {
            std::unique_lock<std::mutex> lock(m);
            seen.push_back(Row{r.timestamp_ns, r.price, r.level});
            if (seen.size() == 1) {
                in_scan = true;
                cv.notify_all();
                cv.wait(lock, [&] { return go_on; });
            }
        });
        EXPECT_TRUE(err.empty()) << err;
    });
    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock, [&] { return in_scan; });
    }
    // The merge is published under the scan, and its inputs wait for it.
    ASSERT_TRUE(eventually([&] { return engine.registry().counter_value("ob_compactions_total") == 1; }));
    std::this_thread::sleep_for(200ms);   // ticks enough to have removed them, were nothing reading
    EXPECT_EQ(segments_on_disk(dir.path, "A"), ob::compaction::kFanIn + 1)
        << "a merge removed the files of segments a running scan had copied";
    EXPECT_EQ(engine.registry().gauge_value("ob_segments_awaiting_removal"),
              static_cast<int64_t>(ob::compaction::kFanIn));
    {
        std::lock_guard<std::mutex> lock(m);
        go_on = true;
    }
    cv.notify_all();
    reader.join();
    EXPECT_EQ(seen, rows_written(1, ob::compaction::kFanIn))
        << "the scan that began before the merge lost rows, or read some twice";
    EXPECT_TRUE(eventually([&] { return segments_on_disk(dir.path, "A") == 1; }))
        << "the inputs were not removed once the scan ended";
    engine.close();
}

TEST(Compaction, ASnapshotsPinStopsMergingAndTheRetentionSweep) {
    TempDir dir;
    // One hour of retention, swept every tick: every row here is days older than that.
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE, {}, {}, {},
                      ob::TTLConfig{1, 0});
    auto pin = engine.pin_segment_files();
    engine.open();
    seal_n(engine, "A", 1, ob::compaction::kFanIn);
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(segments_on_disk(dir.path, "A"), ob::compaction::kFanIn)
        << "a merge or the retention sweep moved files a snapshot's manifest may name";
    EXPECT_EQ(engine.registry().counter_value("ob_compactions_total"), 0u);
    pin.reset();
    // Unpinned, the sweep deletes them - it runs before the merges in a tick.
    EXPECT_TRUE(eventually([&] { return segments_on_disk(dir.path, "A") == 0; }))
        << segments_on_disk(dir.path, "A") << " segment(s) left after the pin went";
    engine.close();
}

TEST(Compaction, TheValveTurnsMergingOff) {
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    engine.set_compaction_enabled(false);
    engine.open();
    seal_n(engine, "A", 1, ob::compaction::kFanIn);
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(segments_on_disk(dir.path, "A"), ob::compaction::kFanIn);
    EXPECT_EQ(engine.registry().counter_value("ob_compactions_total"), 0u);
    engine.close();
}

TEST(Compaction, ARestartAfterAMergeAnswersEveryRowOnce) {
    TempDir dir;
    {
        ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        seal_n(engine, "A", 1, ob::compaction::kFanIn);
        ASSERT_TRUE(eventually([&] { return segments_on_disk(dir.path, "A") == 1; }));
        engine.close();
    }
    ob::Engine reopened(dir.path, 20'000'000ULL, ob::FsyncPolicy::NONE);
    reopened.open();
    EXPECT_EQ(rows_answered(reopened, "A"), rows_written(1, ob::compaction::kFanIn));
    // A write after the restart seals a segment of its own, and a sequence number past the merged
    // segment's: the merge kept the highest of its inputs'.
    insert(reopened, "A", ob::compaction::kFanIn + 1);
    reopened.flush_incremental();
    EXPECT_EQ(rows_answered(reopened, "A").size(), (ob::compaction::kFanIn + 1) * kLevels);
    reopened.close();
}

TEST(Compaction, AnIdleNodeFinishesAMergeWithSyncsOfItsOwn) {
    TempDir dir;
    // The default policy: every step waits for a sync, and an idle tick seals nothing to sync.
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::INTERVAL);
    engine.open();
    seal_n(engine, "A", 1, ob::compaction::kFanIn);
    EXPECT_TRUE(eventually([&] { return segments_on_disk(dir.path, "A") == 1; }))
        << segments_on_disk(dir.path, "A") << " segment(s) of A on an idle node";
    EXPECT_EQ(rows_answered(engine, "A"), rows_written(1, ob::compaction::kFanIn));
    engine.close();
}
