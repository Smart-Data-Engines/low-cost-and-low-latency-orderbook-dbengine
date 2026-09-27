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
#include "mm_engine_node.hpp"
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

using namespace mm_engine;

std::atomic<uint16_t> g_port{ob::test::kPortsMmRestartOrigins};

std::unique_ptr<ob::Engine> open_node(const std::string& dir,
                                      ob::FsyncPolicy policy = ob::FsyncPolicy::EVERY) {
    return mm_engine::open_node(g_port, dir, policy);
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
        // A tick before anything arrives, so the cache is built while it is empty and goes past
        // 4 096 entries by updates: the first update after open is a whole rebuild, which would
        // otherwise be the only thing this test's vector ever went through.
        node->flush_tick_for_test();
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
