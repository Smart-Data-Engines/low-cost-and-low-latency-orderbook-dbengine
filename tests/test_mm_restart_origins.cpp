// What a mesh node knows after a restart, and from whom (#179), and what it tells its peers between
// two seals (#180).
//
// A sequence number means something only together with the origin that minted it: the receive path
// drops a record when `has_seen(key, origin, seq)`, and a peer's catch-up sends what the vector says
// is missing per (symbol, origin). Replay used to remember every record of the WAL tail as this
// node's own, so after a restart a record a peer had sent was unseen for that peer - the peer sent
// it again and it was stored twice: 200 rows where the writer held 100, measured three nodes deep in
// `tests/integration/test_mm_restart.py`. These tests take the same restart apart on one engine.
//
// A crash is a copy of the data directory taken while nothing writes to it - the flush loop's
// interval is an hour and every tick is the test's own - so the copy is the directory at one
// instant, which is what `kill -9` leaves. The original is then closed normally, so no thread of it
// outlives the test.
//
// Multi-master is enabled without etcd, as in test_mm_dedup.cpp: peer discovery fails and logs,
// which is irrelevant here, and it keeps this a unit test instead of a cluster.

#include "orderbook/engine.hpp"
#include "test_ports.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"
#include "orderbook/version_vector.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <regex>
#include <string>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};
std::atomic<uint16_t> g_port{ob::test::kPortsMmRestartOrigins};

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix) {
        auto p = fs::temp_directory_path() /
                 (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
        fs::create_directories(p);
        path = p.string();
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;   // every tick here is the test's own
constexpr uint16_t kSelf = 1;
constexpr uint16_t kPeer = 2;

ob::MultiMasterConfig mm_config() {
    ob::MultiMasterConfig mm{};
    mm.enabled                   = true;
    mm.node_id                   = kSelf;
    mm.replication_port          = g_port.fetch_add(1, std::memory_order_relaxed);
    mm.compress                  = false;
    mm.max_catchup_bytes         = 1 << 20;
    mm.anti_entropy_interval_sec = 3600;   // out of the way: nothing here is about the timer
    return mm;
}

std::unique_ptr<ob::Engine> open_node(const std::string& dir,
                                      ob::FsyncPolicy policy = ob::FsyncPolicy::EVERY) {
    auto engine = std::make_unique<ob::Engine>(dir, kNoAutoFlush, policy, ob::ReplicationConfig{},
                                               ob::ReplicationClientConfig{},
                                               ob::FailoverConfig{}, ob::TTLConfig{}, mm_config());
    engine->open();
    return engine;
}

/// One record of `levels` levels, each at its own price so that no level loses to another under
/// Last-Writer-Wins: every level is a row, and a row count is what these tests read.
struct Record {
    ob::DeltaUpdate        delta{};
    std::vector<ob::Level> levels;
};

Record record(const char* symbol, uint64_t seq, uint64_t ts, uint16_t levels = 1) {
    Record r;
    std::strncpy(r.delta.symbol, symbol, sizeof(r.delta.symbol) - 1);
    std::strncpy(r.delta.exchange, "EX", sizeof(r.delta.exchange) - 1);
    r.delta.sequence_number = seq;
    r.delta.timestamp_ns    = ts;
    r.delta.side            = ob::SIDE_BID;
    r.delta.n_levels        = levels;
    r.levels.resize(levels);
    for (uint16_t i = 0; i < levels; ++i) {
        r.levels[i].price = static_cast<int64_t>(ts % 1'000'000'000ULL) * 1000 + i;
        r.levels[i].qty   = 5;
        r.levels[i].cnt   = 1;
        r.levels[i]._pad  = 0;
    }
    return r;
}

/// A record `origin` wrote, as the mesh delivers it.
ob::ob_status_t deliver(ob::Engine& engine, const Record& r, uint16_t origin) {
    ob::HLCTimestamp hlc{};
    hlc.physical_ns = r.delta.timestamp_ns;
    hlc.logical     = 0;
    hlc.node_id     = origin;
    return engine.apply_remote_delta(r.delta, r.levels.data(), origin, hlc);
}

/// A record this node's client wrote, numbered by the engine.
void write_own(ob::Engine& engine, const Record& r) {
    ASSERT_EQ(engine.apply_delta_mm(r.delta, r.levels.data()), ob::OB_OK);
}

int rows(ob::Engine& engine, const char* symbol) {
    int n = 0;
    const std::string sql = std::string("SELECT * FROM '") + symbol +
                            "'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    const std::string err = engine.execute(sql, [&n](const ob::QueryResult&) { ++n; });
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) {
        ADD_FAILURE() << "query error: " << err;
    }
    return n;
}

size_t segments_on_disk(const std::string& dir) {
    size_t n = 0;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(dir, ec);
         it != fs::recursive_directory_iterator(); ++it) {
        if (it->is_regular_file(ec) && it->path().filename() == "meta.json") ++n;
    }
    return n;
}

/// The data directory as a crash leaves it; see the top of this file for why a copy is one.
void crash_image(const std::string& from, const std::string& to) {
    fs::remove_all(to);
    fs::copy(from, to, fs::copy_options::recursive);
}

/// The frontier this node would state to a peer now for (key, origin), if it states one.
std::optional<uint64_t> told(ob::Engine& engine, const std::string& key, uint16_t origin) {
    bool truncated = false;
    const auto entries = engine.export_version_vector(1u << 20, truncated);
    for (const auto& e : entries) {
        if (e.key == key && e.origin == origin) return e.frontier;
    }
    return std::nullopt;
}

/// Every (key, origin) -> frontier this node would state to a peer now.
std::map<std::pair<std::string, uint16_t>, uint64_t> told_all(ob::Engine& engine) {
    bool truncated = false;
    std::map<std::pair<std::string, uint16_t>, uint64_t> out;
    for (const auto& e : engine.export_version_vector(1u << 20, truncated)) {
        out[{e.key, e.origin}] = e.frontier;
    }
    return out;
}

/// The last version vector the WAL holds - what a flush wrote down, from a whole export.
std::map<std::pair<std::string, uint16_t>, uint64_t> written_down(const std::string& dir) {
    std::vector<uint8_t> last;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_VERSION_VECTOR) return;
        last.assign(ctx.payload, ctx.payload + ctx.payload_len);
    });
    std::map<std::pair<std::string, uint16_t>, uint64_t> out;
    ob::PeerVector vector;
    if (last.empty() || !vector.deserialize(last.data(), last.size()) || vector.truncated()) {
        ADD_FAILURE() << "no usable version vector in the WAL";
        return out;
    }
    for (const auto& e : vector.entries()) out[{e.key, e.origin}] = e.frontier;
    return out;
}

/// The number and the origin of every DELTA record for `symbol` in the WAL, in file order.
std::vector<std::pair<uint64_t, uint16_t>> wal_numbers(const std::string& dir,
                                                       const std::string& symbol) {
    std::vector<std::pair<uint64_t, uint16_t>> out;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_DELTA) return;
        if (ctx.payload_len < sizeof(ob::DeltaUpdate)) return;
        ob::DeltaUpdate d{};
        std::memcpy(&d, ctx.payload, sizeof(d));
        if (symbol != d.symbol) return;
        out.emplace_back(ctx.header.sequence_number, ctx.origin_node_id);
    });
    return out;
}

/// Drop whatever the WAL holds after its last checkpoint: the state a crash leaves when it lands
/// straight after the checkpoint reached the file. Cut at the next record's start, as the replayer
/// reports it, so no header size is assumed. Returns whether anything was removed.
bool cut_after_last_checkpoint(const std::string& dir) {
    struct At {
        uint32_t file;
        uint64_t offset;
        uint8_t  type;
    };
    std::vector<At> records;
    {
        ob::WALReplayer replayer(dir);
        replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
            records.push_back({ctx.wal_file_index, ctx.wal_byte_offset, ctx.header.record_type});
        });
    }
    size_t last = records.size();
    for (size_t i = 0; i < records.size(); ++i) {
        if (records[i].type == ob::WAL_RECORD_CHECKPOINT) last = i;
    }
    if (last == records.size() || last + 1 == records.size()) return false;

    const At& cut = records[last + 1];
    for (const auto& entry : fs::directory_iterator(dir)) {
        const std::string name = entry.path().filename().string();
        unsigned index = 0;
        if (name.size() != 14 || std::sscanf(name.c_str(), "wal_%6u.bin", &index) != 1) continue;
        if (index == cut.file) fs::resize_file(entry.path(), cut.offset);
        if (index > cut.file) fs::remove(entry.path());
    }
    return true;
}

}  // namespace

// ═══════════════════════════════════════════════════════════════════════════════
// #179 - a replayed record is remembered as seen from the origin that wrote it
// ═══════════════════════════════════════════════════════════════════════════════

TEST(MeshRestartOrigins, APeersRecordsReplayedFromTheWalAreRememberedAsThePeers) {
    // The integration test's restart on one engine: records a peer sent and records this node's
    // clients wrote, all of them only in the WAL - nothing sealed, so no vector written either.
    TempDir live("mm_origins_live_");
    TempDir crashed("mm_origins_crash_");

    std::vector<Record> theirs;
    for (uint64_t seq = 1; seq <= 5; ++seq) {
        theirs.push_back(record("THEIRS", seq, 5'000'000'000ULL + seq));
    }
    {
        auto node = open_node(live.path);
        for (const auto& r : theirs) ASSERT_EQ(deliver(*node, r, kPeer), ob::OB_OK);
        for (uint64_t i = 1; i <= 3; ++i) write_own(*node, record("MINE", 0, 6'000'000'000ULL + i));
        ASSERT_EQ(segments_on_disk(live.path), 0u)
            << "a row reached a segment, so the restart below would not rest on the WAL alone";
        crash_image(live.path, crashed.path);
        node->close();
    }

    auto node = open_node(crashed.path);
    ASSERT_EQ(rows(*node, "THEIRS"), 5);
    ASSERT_EQ(rows(*node, "MINE"), 3);

    // The peer, asked what this node holds, sends its records again: catch-up over-delivers on
    // purpose, and the receive path is what must refuse them.
    for (const auto& r : theirs) EXPECT_EQ(deliver(*node, r, kPeer), ob::OB_OK);
    EXPECT_EQ(rows(*node, "THEIRS"), 5)
        << "the peer's records were stored a second time: replay remembered them as this node's "
           "own, so for the peer's origin their numbers were unseen - #179, 200 rows where the "
           "writer held 100";

    // The next record a client writes continues this node's own numbers.
    write_own(*node, record("MINE", 0, 6'000'000'010ULL));

    // And what a peer is told, one tick later.
    node->flush_tick_for_test();
    EXPECT_EQ(told(*node, "THEIRS.EX", kPeer), std::optional<uint64_t>(5))
        << "the node does not say it holds the peer's records, so the peer sends them again at "
           "every reconnection";
    EXPECT_EQ(told(*node, "THEIRS.EX", kSelf), std::nullopt)
        << "the node claims to have written numbers only the peer wrote; a peer lacking them is "
           "then a range this node can never send (#178's unfillable-range warning)";
    EXPECT_EQ(told(*node, "MINE.EX", kSelf), std::optional<uint64_t>(4));
    node->close();

    const auto mine = wal_numbers(crashed.path, "MINE");
    ASSERT_EQ(mine.size(), 4u);
    EXPECT_EQ(mine.back().first, 4u)
        << "the write after the restart did not continue this node's numbers";
    EXPECT_EQ(mine.back().second, kSelf);
}

TEST(MeshRestartOrigins, ARecordWrittenBeforeTheMeshIsThisNodesOwn) {
    // A record without an origin in its header - written by a node that was not in a mesh - is
    // this node's own: nobody else could have written it into this WAL.
    TempDir live("mm_origins_legacy_live_");
    TempDir crashed("mm_origins_legacy_crash_");
    {
        ob::Engine plain(live.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
        plain.open();
        for (uint64_t i = 1; i <= 3; ++i) {
            const Record r = record("MINE", 0, 6'000'000'000ULL + i);
            ASSERT_EQ(plain.apply_delta(r.delta, r.levels.data()), ob::OB_OK);
        }
        crash_image(live.path, crashed.path);
        plain.close();
    }
    const auto before = wal_numbers(crashed.path, "MINE");
    ASSERT_EQ(before.size(), 3u);
    ASSERT_EQ(before.back().second, 0u) << "the record names an origin, so it is not the case here";

    auto node = open_node(crashed.path);
    ASSERT_EQ(rows(*node, "MINE"), 3);
    write_own(*node, record("MINE", 0, 6'000'000'010ULL));
    node->flush_tick_for_test();
    EXPECT_EQ(told(*node, "MINE.EX", kSelf), std::optional<uint64_t>(4))
        << "the records from before the mesh were not remembered as this node's own, so the "
           "node's own frontier cannot reach past them";
    node->close();
}

TEST(MeshRestartOrigins, APeersRecordsAfterTheLastVectorAreRememberedToo) {
    // A vector in the WAL and a tail after it: the vector names what was held at the last seal, the
    // tail is what arrived since, and both are the peer's.
    TempDir live("mm_origins_tail_live_");
    TempDir crashed("mm_origins_tail_crash_");

    std::vector<Record> theirs;
    for (uint64_t seq = 1; seq <= 6; ++seq) {
        theirs.push_back(record("THEIRS", seq, 5'000'000'000ULL + seq));
    }
    {
        auto node = open_node(live.path);
        for (size_t i = 0; i < 3; ++i) ASSERT_EQ(deliver(*node, theirs[i], kPeer), ob::OB_OK);
        node->flush_incremental();   // a segment, the vector, a checkpoint
        for (size_t i = 3; i < 6; ++i) ASSERT_EQ(deliver(*node, theirs[i], kPeer), ob::OB_OK);
        crash_image(live.path, crashed.path);
        node->close();
    }

    auto node = open_node(crashed.path);
    ASSERT_EQ(rows(*node, "THEIRS"), 6);
    for (const auto& r : theirs) EXPECT_EQ(deliver(*node, r, kPeer), ob::OB_OK);
    EXPECT_EQ(rows(*node, "THEIRS"), 6)
        << "the records after the vector were stored again: the vector covered the first three, "
           "and replay remembered the rest as this node's own";
    node->flush_tick_for_test();
    EXPECT_EQ(told(*node, "THEIRS.EX", kPeer), std::optional<uint64_t>(6));
    node->close();
}

TEST(MeshRestartOrigins, RecordsASegmentAlreadyHoldsAreRememberedFromTheirOrigin) {
    // Since part 2a of #165 a checkpoint's replay starts at the oldest record a row still waiting in
    // a store needs, so a restart reads again the records of every store sealed after it, and skips
    // them as stored. Their numbers were known only to the vector - and a vector of more than 4 096
    // entries is never written (#177). Here one symbol carries records of 4 097 origins, which makes
    // it that large and is the row still waiting; a store the tick seals comes after it.
    TempDir live("mm_origins_skip_live_");
    TempDir crashed("mm_origins_skip_crash_");

    constexpr uint16_t kWideOrigins    = 4097;
    constexpr uint16_t kFirstWide      = 3;
    constexpr uint16_t kLevels         = 1000;
    constexpr uint64_t kSealedRecords  = 66;   // 66 000 rows: past kSealRows, so the tick seals it
    const auto sealed_record = [&](uint64_t seq) {
        return record("SEALED", seq, 2'000'000'000ULL + seq, kLevels);
    };
    {
        auto node = open_node(live.path, ob::FsyncPolicy::INTERVAL);
        for (uint16_t k = 0; k < kWideOrigins; ++k) {
            ASSERT_EQ(deliver(*node, record("WIDE", 1, 1'000'000'000ULL + k),
                              static_cast<uint16_t>(kFirstWide + k)),
                      ob::OB_OK);
        }
        for (uint64_t seq = 1; seq <= kSealedRecords; ++seq) {
            ASSERT_EQ(deliver(*node, sealed_record(seq), kPeer), ob::OB_OK);
        }
        node->flush_tick_for_test();
        ASSERT_EQ(segments_on_disk(live.path), 1u)
            << "the tick was to seal SEALED and nothing else, leaving WIDE's rows waiting";
        bool truncated = false;
        (void)node->export_version_vector(1u << 20, truncated);
        ASSERT_TRUE(truncated)
            << "the vector fits, so it was written, and the restart would learn from it what this "
               "test is about the replay learning";
        crash_image(live.path, crashed.path);
        node->close();
    }

    testing::internal::CaptureStderr();
    auto node = open_node(crashed.path, ob::FsyncPolicy::INTERVAL);
    const std::string said = testing::internal::GetCapturedStderr();

    std::smatch m;
    ASSERT_TRUE(std::regex_search(said, m, std::regex("skipped_by_position=(\\d+)"))) << said;
    ASSERT_EQ(std::stoull(m[1].str()), kSealedRecords)
        << "SEALED's records were not read again and skipped as stored, so this test is not about "
           "them";
    ASSERT_TRUE(std::regex_search(said, m, std::regex("other_origins=(\\d+)"))) << said;
    EXPECT_EQ(std::stoull(m[1].str()), kWideOrigins + kSealedRecords);
    ASSERT_EQ(rows(*node, "SEALED"), static_cast<int>(kSealedRecords * kLevels));
    ASSERT_EQ(rows(*node, "WIDE"), static_cast<int>(kWideOrigins));

    EXPECT_EQ(deliver(*node, sealed_record(1), kPeer), ob::OB_OK);
    EXPECT_EQ(rows(*node, "SEALED"), static_cast<int>(kSealedRecords * kLevels))
        << "a record a segment already held was stored again: replay skipped it as stored and "
           "remembered nothing about it, and there was no vector to remember it either";
    EXPECT_EQ(deliver(*node, record("WIDE", 1, 1'000'000'000ULL), kFirstWide), ob::OB_OK);
    EXPECT_EQ(rows(*node, "WIDE"), static_cast<int>(kWideOrigins));
    node->close();
}

TEST(MeshRestartOrigins, ACrashRightAfterACheckpointKeepsTheVectorThatCoversIt) {
    // The vector is written **before** the checkpoint: the checkpoint cuts the replay, so whatever
    // it cuts off is known only to a vector - and a crash landing between the two appends, with the
    // checkpoint first, left the records before it known to nothing.
    TempDir live("mm_origins_ckpt_live_");
    TempDir crashed("mm_origins_ckpt_crash_");

    std::vector<Record> theirs;
    for (uint64_t seq = 1; seq <= 3; ++seq) {
        theirs.push_back(record("THEIRS", seq, 5'000'000'000ULL + seq));
    }
    {
        auto node = open_node(live.path);
        for (const auto& r : theirs) ASSERT_EQ(deliver(*node, r, kPeer), ob::OB_OK);
        node->flush_incremental();
        crash_image(live.path, crashed.path);
        node->close();
    }
    // Whatever followed the checkpoint did not reach the file. Nothing, in this order.
    (void)cut_after_last_checkpoint(crashed.path);

    auto node = open_node(crashed.path);
    ASSERT_EQ(rows(*node, "THEIRS"), 3);
    for (const auto& r : theirs) EXPECT_EQ(deliver(*node, r, kPeer), ob::OB_OK);
    EXPECT_EQ(rows(*node, "THEIRS"), 3)
        << "the records before the checkpoint were stored again: the vector covering them came "
           "after it and was lost with the crash, and the replay starts at the checkpoint";
    node->close();
}

// ═══════════════════════════════════════════════════════════════════════════════
// #180 - what a peer is told is at most one tick old, whether the tick seals or not
// ═══════════════════════════════════════════════════════════════════════════════

TEST(MeshRestartOrigins, APeerIsToldWhatArrivedAfterOneTickThatSealsNothing) {
    // The vector a peer is compared against is a cache, and it was refreshed only where the vector
    // is written down - a checkpoint. Since part 2a of #165 a tick that seals nothing writes none,
    // for up to ten seconds, so a peer returning inside that window was told this node lacked what
    // it held - 312 985 records sent a second time in #178's measurement - and a node returning
    // after a restart was judged to hold what it missed: 238 of 2 100 rows three seconds on.
    TempDir dir("mm_origins_tick_");
    auto node = open_node(dir.path);
    for (uint64_t seq = 1; seq <= 3; ++seq) {
        ASSERT_EQ(deliver(*node, record("THEIRS", seq, 5'000'000'000ULL + seq), kPeer), ob::OB_OK);
    }
    ASSERT_EQ(told(*node, "THEIRS.EX", kPeer), std::nullopt)
        << "the vector moved before any tick, so this test would not see what the tick does";

    node->flush_tick_for_test();
    ASSERT_EQ(segments_on_disk(dir.path), 0u)
        << "the tick sealed something, and a seal writes the vector down anyway";
    EXPECT_EQ(told(*node, "THEIRS.EX", kPeer), std::optional<uint64_t>(3))
        << "a tick that sealed nothing left the vector a peer is told where the last seal left it";

    // And one that has nothing new leaves it as it is.
    node->flush_tick_for_test();
    EXPECT_EQ(told(*node, "THEIRS.EX", kPeer), std::optional<uint64_t>(3));
    node->close();
}

TEST(MeshRestartOrigins, WhatAPeerIsToldIsWhatAWholeExportSays) {
    // A tick does not export the vector again: it brings its copy up to date with the frontiers
    // that moved, which is what makes a refresh at every tick affordable. So the copy is checked
    // against a whole export - the one a flush writes into the WAL - after records from several
    // origins arriving in order, out of it, twice, and with holes, between ticks.
    TempDir dir("mm_origins_copy_");
    auto node = open_node(dir.path, ob::FsyncPolicy::INTERVAL);
    std::mt19937 rng(179);
    std::map<std::pair<int, uint16_t>, uint64_t> next;   // (symbol, origin) -> next number to send
    uint64_t ts = 1'000'000'000ULL;
    for (int step = 0; step < 3000; ++step) {
        const int sym = static_cast<int>(rng() % 40);
        const auto origin = static_cast<uint16_t>(2 + rng() % 4);
        const std::string symbol = "COPY" + std::to_string(sym);
        uint64_t& n = next[{sym, origin}];
        if (n == 0) n = 1;
        uint64_t seq = n;
        switch (rng() % 6) {
            case 0: seq = n + 1 + rng() % 3; break;              // ahead of a hole
            case 1: seq = n > 1 ? 1 + rng() % (n - 1) : 1; break; // a redelivery
            default: ++n; break;                                  // in order
        }
        ASSERT_EQ(deliver(*node, record(symbol.c_str(), seq, ++ts), origin), ob::OB_OK);
        if (rng() % 3 == 0) write_own(*node, record(symbol.c_str(), 0, ++ts));
        if (step % 97 == 0) node->flush_tick_for_test();
    }
    node->flush_tick_for_test();
    const auto told_before = told_all(*node);
    node->flush_incremental();                    // writes the whole export down
    EXPECT_EQ(told_before, written_down(dir.path))
        << "the copy kept from the frontiers that moved is not the vector a whole export gives";
    EXPECT_EQ(told_all(*node), told_before) << "the flush found something the ticks had not";
    EXPECT_GT(told_before.size(), 100u) << "the case is too small to say anything";
    node->close();
}
