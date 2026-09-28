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

#include <algorithm>
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


constexpr uint16_t kThird = 3;

/// The file of the last CHECKPOINT record.
uint32_t last_checkpoint_file(const std::string& dir) {
    uint32_t file = 0;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type == ob::WAL_RECORD_CHECKPOINT) file = ctx.wal_file_index;
    });
    return file;
}

/// A peer's rows and this node's own, and the whole vector their checkpoint writes; then `held` of a
/// third origin's numbers above a hole - rows that move no frontier - through several WAL files, and
/// a checkpoint after them, which writes the held set and no vector. Returns the whole vector's file.
uint32_t whole_vector_then_held_only(ob::Engine& node, const std::string& dir, uint64_t held) {
    for (uint64_t seq = 1; seq <= 5; ++seq) {
        EXPECT_EQ(deliver(node, record("PEERS", seq, 7'000'000'000ULL + seq), kPeer), ob::OB_OK);
    }
    write_symbols(node, 0, 20, 1'000'000'000ULL);
    node.flush_incremental();
    const auto whole = vector_writes(dir);
    EXPECT_FALSE(whole.empty());
    const uint32_t base = whole.empty() ? 0 : whole.back().file;
    for (uint64_t seq = 100; seq < 100 + held; ++seq) {
        EXPECT_EQ(deliver(node, record("HELDQ", seq, 8'000'000'000ULL + seq), kThird), ob::OB_OK);
    }
    node.flush_incremental();
    EXPECT_EQ(vector_writes(dir).size(), whole.size()) << "the premise: no vector after the held rows";
    return base;
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
    // A vector's records do not rotate the WAL - only data does - so a whole vector is in the file of
    // the checkpoint after it. What leaves it behind is data that moves no frontier: a peer's numbers
    // held above a hole. A checkpoint after them writes the held set and no vector, in a later file,
    // and retention retires the files that checkpoint is past - the vector's too.
    TempDir live("mm_vchanges_ret_live_");
    TempDir crashed("mm_vchanges_ret_crash_");
    std::map<std::pair<std::string, uint16_t>, uint64_t> before;
    {
        auto node = mesh_node(live.path, 8u << 10);
        const uint32_t base = whole_vector_then_held_only(*node, live.path, 100);
        node->flush_tick_for_test();
        node->flush_tick_for_test();
        ASSERT_GT(last_checkpoint_file(live.path), base) << "the premise: the checkpoint is past the vector";
        before = told_all(*node);
        crash_image(live.path, crashed.path);
        node->close();
    }
    auto node = mesh_node(crashed.path, 8u << 10);
    EXPECT_EQ(told_all(*node), before) << "the restart found no whole vector to start from";
    node->close();
}

TEST(MeshVectorChanges, ARestartKeepsTheFileItsWholeVectorBeginsInUntilItWritesOne) {
    // The same after a restart, before it writes a vector of its own: the one it read is in a file
    // that the first checkpoint it writes - after held numbers alone - is past.
    TempDir live("mm_vchanges_rs_live_");
    TempDir first("mm_vchanges_rs_first_");
    TempDir second("mm_vchanges_rs_second_");
    std::map<std::pair<std::string, uint16_t>, uint64_t> before;
    uint32_t base = 0;
    {
        auto node = mesh_node(live.path, 8u << 10);
        base = whole_vector_then_held_only(*node, live.path, 100);
        crash_image(live.path, first.path);
        node->close();
    }
    {
        auto node = mesh_node(first.path, 8u << 10);
        for (uint64_t seq = 600; seq < 900; ++seq) {
            ASSERT_EQ(deliver(*node, record("HELDQ", seq, 8'000'000'000ULL + seq), kThird), ob::OB_OK);
        }
        node->flush_incremental();
        node->flush_tick_for_test();
        node->flush_tick_for_test();
        ASSERT_GT(last_checkpoint_file(first.path), base) << "the premise: the checkpoint is past the vector";
        before = told_all(*node);
        crash_image(first.path, second.path);
        node->close();
    }
    auto node = mesh_node(second.path, 8u << 10);
    EXPECT_EQ(told_all(*node), before) << "the second restart found no whole vector to start from";
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

TEST(MeshVectorChanges, AWholeVectorIsWrittenAgainOnceTheChangesSinceOutgrowIt) {
    // So that neither a restart nor the WAL pays for changes without end: 20 entries whole, and five
    // moved at every checkpoint - the fourth set takes the changes past the whole vector's size.
    TempDir dir("mm_vchanges_grow_");
    auto node = mesh_node(dir.path);
    write_symbols(*node, 0, 20, 1'000'000'000ULL);
    node->flush_incremental();
    std::vector<uint8_t> kinds;   // one per vector written after the first: whole or changes
    size_t seen = vector_writes(dir.path).size();
    for (int round = 1; round <= 6; ++round) {
        write_symbols(*node, 0, 5, 1'000'000'000ULL * (round + 1));
        node->flush_incremental();
        const auto all = vector_writes(dir.path);
        ASSERT_GT(all.size(), seen);
        kinds.push_back(all.back().type);
        seen = all.size();
    }
    const auto first_whole = std::find_if(kinds.begin(), kinds.end(), [](uint8_t t) {
        return t != ob::WAL_RECORD_VERSION_VECTOR_CHANGES;
    });
    ASSERT_NE(first_whole, kinds.begin()) << "the first moves after a whole vector were not changes";
    EXPECT_NE(first_whole, kinds.end()) << "six sets of changes and never a whole vector again";
    EXPECT_EQ(written_down(dir.path), told_all(*node));
    node->close();
}

TEST(MeshVectorChanges, WhenMostOfTheVectorMovedItIsWrittenWhole) {
    TempDir dir("mm_vchanges_most_");
    auto node = mesh_node(dir.path);
    write_symbols(*node, 0, 20, 1'000'000'000ULL);
    node->flush_incremental();
    write_symbols(*node, 0, 15, 2'000'000'000ULL);   // 15 of 20
    node->flush_incremental();
    EXPECT_NE(vector_writes(dir.path).back().type, ob::WAL_RECORD_VERSION_VECTOR_CHANGES);
    node->close();
}

TEST(MeshVectorChanges, ARestartGoesOnWithChangesOnTheWholeVectorItRead) {
    // The writer goes on in the last WAL file, where the whole vector a restart read begins: what
    // moves after it is written as changes on it, not as another whole vector.
    TempDir live("mm_vchanges_on_live_");
    TempDir crashed("mm_vchanges_on_crash_");
    {
        auto node = mesh_node(live.path);
        write_symbols(*node, 0, 100, 1'000'000'000ULL);
        node->flush_incremental();
        write_symbols(*node, 0, 2, 2'000'000'000ULL);
        node->flush_incremental();
        crash_image(live.path, crashed.path);
        node->close();
    }
    auto node = mesh_node(crashed.path);
    const size_t before = vector_writes(crashed.path).size();
    write_symbols(*node, 10, 2, 3'000'000'000ULL);
    node->flush_incremental();
    const auto all = vector_writes(crashed.path);
    ASSERT_GT(all.size(), before);
    EXPECT_EQ(all.back().type, ob::WAL_RECORD_VERSION_VECTOR_CHANGES)
        << "a restart wrote its vector whole again";
    EXPECT_EQ(written_down(crashed.path), told_all(*node));
    node->close();
}
