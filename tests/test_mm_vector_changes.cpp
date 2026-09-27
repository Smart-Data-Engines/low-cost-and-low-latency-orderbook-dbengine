// A mesh node writes down what moved in its version vector, not the whole vector (#189).
//
// Since #177 a vector of any size went into the WAL before every checkpoint that followed a frontier
// moving, whole and under the engine's lock: at 50 000 (symbol, origin) entries 8.7 - 9 ms and 2 MB
// of WAL at every checkpoint. A checkpoint writes the entries that moved since the last vector now,
// and a whole vector where a restart needs one to put them on: the first in every WAL file, the first
// after a start with no usable vector, after one a rotation cut across, and the last before a clean
// stop that wrote changes - and retention keeps the file the last whole vector begins in.
//
// These tests read the WAL as a restart does (`written_down()`, `VectorFromWal`) and hold it to what
// the node would tell a peer (`told_all()`), and read the records themselves for what was written.

#include "orderbook/engine.hpp"
#include "mm_engine_node.hpp"
#include "test_ports.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/version_vector.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

using namespace mm_engine;

std::atomic<uint16_t> g_port{ob::test::kPortsMmVectorChanges};

std::unique_ptr<ob::Engine> mesh_node(const std::string& dir, size_t rotate_bytes = 512ULL << 20) {
    auto engine = std::make_unique<ob::Engine>(dir, kNoAutoFlush, ob::FsyncPolicy::INTERVAL,
                                               ob::ReplicationConfig{}, ob::ReplicationClientConfig{},
                                               ob::FailoverConfig{}, ob::TTLConfig{},
                                               mm_config(g_port), rotate_bytes);
    engine->open();
    return engine;
}

/// One record of this node's own for each of `count` symbols, `first` on.
void write_symbols(ob::Engine& node, size_t first, size_t count, uint64_t ts) {
    for (size_t i = first; i < first + count; ++i) {
        write_own(node, record(("S" + std::to_string(i)).c_str(), 0, ts + i));
    }
}

/// What one vector record in the WAL is: its type, its file, and for the part format its header.
struct VectorWrite {
    uint8_t  type{0};
    uint32_t file{0};
    uint16_t part{0};
    uint16_t parts{1};
    uint16_t count{0};
};

std::vector<VectorWrite> vector_writes(const std::string& dir) {
    std::vector<VectorWrite> out;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        const uint8_t type = ctx.header.record_type;
        if (type != ob::WAL_RECORD_VERSION_VECTOR && type != ob::WAL_RECORD_VERSION_VECTOR_PART &&
            type != ob::WAL_RECORD_VERSION_VECTOR_CHANGES) {
            return;
        }
        VectorWrite w;
        w.type = type;
        w.file = ctx.wal_file_index;
        if (type == ob::WAL_RECORD_VERSION_VECTOR) {
            std::memcpy(&w.count, ctx.payload, sizeof(w.count));
        } else {
            std::memcpy(&w.part, ctx.payload + 4, sizeof(w.part));
            std::memcpy(&w.parts, ctx.payload + 6, sizeof(w.parts));
            std::memcpy(&w.count, ctx.payload + 8, sizeof(w.count));
        }
        out.push_back(w);
    });
    return out;
}

bool begins_whole(const VectorWrite& w) {
    return w.type == ob::WAL_RECORD_VERSION_VECTOR ||
           (w.type == ob::WAL_RECORD_VERSION_VECTOR_PART && w.part == 0);
}

size_t held_records(const std::string& dir) {
    size_t n = 0;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type == ob::WAL_RECORD_HELD_SEQUENCES) ++n;
    });
    return n;
}

}  // namespace

TEST(MeshVectorChanges, ACheckpointAfterAFewMovedWritesThoseAndARestartPutsThemOn) {
    TempDir live("mm_vchanges_live_");
    TempDir crashed("mm_vchanges_crash_");
    std::map<std::pair<std::string, uint16_t>, uint64_t> before;
    {
        auto node = mesh_node(live.path);
        write_symbols(*node, 0, 2000, 1'000'000'000ULL);
        node->flush_incremental();
        const auto first = vector_writes(live.path);
        ASSERT_FALSE(first.empty());
        ASSERT_TRUE(begins_whole(first.front())) << "a fresh node's first vector was not whole";

        write_symbols(*node, 5, 3, 2'000'000'000ULL);   // three frontiers move
        node->flush_incremental();
        const auto all = vector_writes(live.path);
        ASSERT_GT(all.size(), first.size()) << "the checkpoint after three moved wrote no vector";
        const VectorWrite& last = all.back();
        EXPECT_EQ(last.type, ob::WAL_RECORD_VERSION_VECTOR_CHANGES) << "a whole vector for three moves";
        EXPECT_EQ(last.parts, 1u);
        EXPECT_EQ(last.count, 3u);

        before = told_all(*node);
        EXPECT_EQ(written_down(live.path), before);
        crash_image(live.path, crashed.path);
        node->close();
    }
    testing::internal::CaptureStderr();
    auto node = mesh_node(crashed.path);
    const std::string said = testing::internal::GetCapturedStderr();
    EXPECT_EQ(told_all(*node), before) << "the restart did not put the changes on the whole vector";
    EXPECT_NE(said.find("1 set(s) of changes on the last whole one"), std::string::npos) << said;
    node->close();
}

TEST(MeshVectorChanges, ACheckpointWithNothingMovedWritesNoVector) {
    TempDir dir("mm_vchanges_still_");
    auto node = mesh_node(dir.path);
    write_symbols(*node, 0, 50, 1'000'000'000ULL);
    node->flush_incremental();
    const size_t written = vector_writes(dir.path).size();
    ASSERT_GT(written, 0u);
    node->flush_incremental();
    node->flush_incremental();
    EXPECT_EQ(vector_writes(dir.path).size(), written) << "a checkpoint with nothing moved wrote a vector";
    node->close();
}

TEST(MeshVectorChanges, ACleanStopLeavesAWholeVectorLast) {
    // So that a build before #189, which reads whole vectors only, starts from the node's vector.
    TempDir dir("mm_vchanges_stop_");
    std::map<std::pair<std::string, uint16_t>, uint64_t> before;
    {
        auto node = mesh_node(dir.path);
        write_symbols(*node, 0, 100, 1'000'000'000ULL);
        node->flush_incremental();
        write_symbols(*node, 0, 2, 2'000'000'000ULL);
        node->flush_incremental();
        ASSERT_EQ(vector_writes(dir.path).back().type, ob::WAL_RECORD_VERSION_VECTOR_CHANGES)
            << "the premise: changes were written";
        before = told_all(*node);
        node->close();
    }
    const auto all = vector_writes(dir.path);
    EXPECT_NE(all.back().type, ob::WAL_RECORD_VERSION_VECTOR_CHANGES) << "a clean stop ended on changes";
    // And the last whole vector alone is the node's: nothing after it to put on.
    ob::VectorFromWal whole_only;
    ob::WALReplayer replayer(dir.path);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_VERSION_VECTOR_CHANGES) whole_only.add(ctx);
    });
    ASSERT_TRUE(whole_only.vector().has_value());
    std::map<std::pair<std::string, uint16_t>, uint64_t> whole;
    for (const auto& e : *whole_only.vector()) whole[{e.key, e.origin}] = e.frontier;
    EXPECT_EQ(whole, before);
}

TEST(MeshVectorChanges, ChangesInAWalFileStandOnAWholeVectorThatBeginsInIt) {
    // Retention removes whole files, so the changes in a file may not stand on a whole vector in an
    // older one: the first vector each file gets is whole.
    TempDir dir("mm_vchanges_files_");
    auto node = mesh_node(dir.path, 96u << 10);
    for (int round = 0; round < 12; ++round) {
        write_symbols(*node, static_cast<size_t>(round) * 7, 300, 1'000'000'000ULL * (round + 1));
        node->flush_incremental();
        write_symbols(*node, 3, 2, 50'000'000'000ULL + static_cast<uint64_t>(round) * 1000);
        node->flush_incremental();
    }
    const auto all = vector_writes(dir.path);
    std::set<uint32_t> files;
    std::map<uint32_t, bool> whole_begun;
    size_t changes = 0;
    for (const auto& w : all) {
        files.insert(w.file);
        if (begins_whole(w)) whole_begun[w.file] = true;
        if (w.type == ob::WAL_RECORD_VERSION_VECTOR_CHANGES && w.part == 0) {
            ++changes;
            EXPECT_TRUE(whole_begun[w.file]) << "changes in file " << w.file
                                             << " before any whole vector begins in it";
        }
    }
    ASSERT_GT(files.size(), 3u) << "the premise: vectors in several files";
    ASSERT_GT(changes, 3u) << "the premise: changes written";
    EXPECT_EQ(written_down(dir.path), told_all(*node));
    node->close();
}

TEST(MeshVectorChanges, RetentionKeepsTheFileTheLastWholeVectorBeginsIn) {
    // WAL files of 32 kB and a whole vector of 1 500 entries, one record of 63 kB: every whole vector
    // ends its file, the checkpoint after it is in the next one, and retention - which deletes the
    // files the last checkpoint is past - would take the vector a restart reads with it.
    TempDir live("mm_vchanges_ret_live_");
    TempDir crashed("mm_vchanges_ret_crash_");
    std::map<std::pair<std::string, uint16_t>, uint64_t> before;
    {
        auto node = mesh_node(live.path, 32u << 10);
        write_symbols(*node, 0, 1500, 1'000'000'000ULL);
        for (int round = 0; round < 4; ++round) {
            write_symbols(*node, static_cast<size_t>(round) * 11, 5, 9'000'000'000ULL + round * 100ULL);
            node->flush_tick_for_test();
            node->flush_incremental();
            node->flush_tick_for_test();
        }
        ASSERT_FALSE(fs::exists(fs::path(live.path) / "wal_000000.bin"))
            << "the premise: retention removed files";
        before = told_all(*node);
        crash_image(live.path, crashed.path);
        node->close();
    }
    auto node = mesh_node(crashed.path);
    EXPECT_EQ(told_all(*node), before) << "the restart found no whole vector to start from";
    node->close();
}

TEST(MeshVectorChanges, HeldNumbersAreWrittenDownWhenTheyChange) {
    TempDir live("mm_vchanges_held_live_");
    TempDir crashed("mm_vchanges_held_crash_");
    {
        auto node = mesh_node(live.path);
        ASSERT_EQ(deliver(*node, record("HELD", 1, 5'000'000'001ULL), kPeer), ob::OB_OK);
        ASSERT_EQ(deliver(*node, record("HELD", 3, 5'000'000'003ULL), kPeer), ob::OB_OK);   // held
        node->flush_incremental();
        const size_t held = held_records(live.path);
        ASSERT_GE(held, 1u) << "the held number was not written down";

        write_own(*node, record("OTHER", 0, 6'000'000'001ULL));   // a frontier moves, nothing held
        node->flush_incremental();
        EXPECT_EQ(held_records(live.path), held) << "an unchanged held set was written again";
        crash_image(live.path, crashed.path);
        node->close();
    }
    auto node = mesh_node(crashed.path);
    EXPECT_EQ(deliver(*node, record("HELD", 3, 5'000'000'003ULL), kPeer), ob::OB_OK);
    EXPECT_EQ(rows(*node, "HELD"), 2) << "the held number was not restored, and its redelivery stored";
    node->close();
}
