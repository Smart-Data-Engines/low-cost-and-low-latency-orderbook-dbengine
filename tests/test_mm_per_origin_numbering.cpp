// Each origin numbers its own records of a symbol, and a restart continues from its own highest
// number (#184), and declares only what it wrote (#185).
//
// A sequence number means something only with the origin that minted it, and a frontier is
// "everything up to here from that origin" - so an origin's numbers for a symbol must not have
// holes. The counter that mints them is one per symbol, and it used to be raised by every origin's
// numbers: when two mesh nodes took turns writing one symbol, each origin's stream got holes where
// the other's numbers were, every receiver's frontier stopped at the first, and a node that missed
// rows was compared against a vector that said it lacked nothing - 10 000 of 11 000 rows after an
// outage, for good (`tests/integration/test_mm_multi_writer.py`). A restart made its own hole: it
// continued the counter from the highest number in a segment, which is every origin's.

#include "orderbook/columnar_store.hpp"
#include "orderbook/compaction.hpp"
#include "orderbook/engine.hpp"
#include "mm_engine_node.hpp"
#include "test_ports.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

using namespace mm_engine;

std::atomic<uint16_t> g_port{ob::test::kPortsMmPerOriginNumbering};

std::unique_ptr<ob::Engine> open_node(const std::string& dir,
                                      ob::FsyncPolicy policy = ob::FsyncPolicy::EVERY) {
    return mm_engine::open_node(g_port, dir, policy);
}

/// This node's own numbers for `symbol`, in WAL order.
std::vector<uint64_t> own_numbers(const std::string& dir, const std::string& symbol) {
    std::vector<uint64_t> out;
    for (const auto& [seq, origin] : wal_numbers(dir, symbol)) {
        if (origin == kSelf) out.push_back(seq);
    }
    return out;
}

/// Every meta.json under `dir` whose segment is `symbol`'s, as text.
std::vector<std::string> metas_of(const std::string& dir, const std::string& symbol) {
    std::vector<std::string> out;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(dir, ec); it != fs::recursive_directory_iterator();
         ++it) {
        if (!it->is_regular_file(ec) || it->path().filename() != "meta.json") continue;
        std::ifstream in(it->path());
        std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        if (text.find("\"symbol\":\"" + symbol + "\"") != std::string::npos) out.push_back(text);
    }
    return out;
}

/// Remove #184's keys from every meta.json under `dir`: a segment as a build before it wrote it.
void make_segments_older(const std::string& dir) {
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(dir, ec); it != fs::recursive_directory_iterator();
         ++it) {
        if (!it->is_regular_file(ec) || it->path().filename() != "meta.json") continue;
        std::string text;
        {
            std::ifstream in(it->path());
            text.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
        const auto at = text.find(",\"own_origin\":");
        if (at == std::string::npos) continue;
        text = text.substr(0, at) + "}";
        std::ofstream(it->path(), std::ios::trunc) << text;
    }
}

}  // namespace

TEST(MeshPerOriginNumbering, OwnNumbersStayContiguousWhateverAPeerWrote) {
    TempDir dir("mm_numbering_contiguous_");
    auto node = open_node(dir.path);
    for (uint64_t seq = 1; seq <= 5; ++seq) {
        ASSERT_EQ(deliver(*node, record("S", seq, 1'000'000'000ULL + seq), kPeer), ob::OB_OK);
    }
    write_own(*node, record("S", 0, 2'000'000'000ULL));
    for (uint64_t seq = 6; seq <= 10; ++seq) {
        ASSERT_EQ(deliver(*node, record("S", seq, 3'000'000'000ULL + seq), kPeer), ob::OB_OK);
    }
    write_own(*node, record("S", 0, 4'000'000'000ULL));
    node->flush_tick_for_test();
    EXPECT_EQ(told(*node, "S.EX", kSelf), std::optional<uint64_t>(2))
        << "this node's own stream has a hole where the peer's numbers are";
    node->close();
    EXPECT_EQ(own_numbers(dir.path, "S"), (std::vector<uint64_t>{1, 2}))
        << "the peer's numbers raised this node's counter";
}

TEST(MeshPerOriginNumbering, ARestartContinuesFromThisNodesOwnHighestNotEveryOrigins) {
    // The segment is the only source here: no vector reached the WAL (skip_vector_persistence_for_
    // test(); until #177 this test made one of 4 097 origins, too large to write), and a flush leaves
    // nothing for the replay. So what the restart continues this node's counter from is what the
    // segment says about its own rows - where every origin's highest, which is all it used to say,
    // is the peer's 100.
    TempDir live("mm_numbering_restart_live_");
    TempDir crashed("mm_numbering_restart_crash_");
    constexpr uint16_t kWideOrigins = 4097;
    {
        auto node = open_node(live.path, ob::FsyncPolicy::INTERVAL);
        node->skip_vector_persistence_for_test();
        for (uint16_t k = 0; k < kWideOrigins; ++k) {
            ASSERT_EQ(deliver(*node, record("WIDE", 1, 1'000'000'000ULL + k),
                              static_cast<uint16_t>(3 + k)), ob::OB_OK);
        }
        for (uint64_t seq = 1; seq <= 100; ++seq) {
            ASSERT_EQ(deliver(*node, record("S", seq, 2'000'000'000ULL + seq), kPeer), ob::OB_OK);
        }
        for (uint64_t i = 1; i <= 3; ++i) write_own(*node, record("S", 0, 3'000'000'000ULL + i));
        node->flush_incremental();
        ASSERT_FALSE(vector_in_wal(live.path)) << "a vector was written, and it would say how far "
                                                   "this node's own went instead of the segment";
        const auto metas = metas_of(live.path, "S");
        ASSERT_FALSE(metas.empty());
        EXPECT_NE(metas.front().find("\"own_origin\":1,\"own_max_sequence\":3"), std::string::npos)
            << metas.front();
        crash_image(live.path, crashed.path);
        node->close();
    }

    auto node = open_node(crashed.path, ob::FsyncPolicy::INTERVAL);
    write_own(*node, record("S", 0, 4'000'000'000ULL));
    node->close();
    const auto own = own_numbers(crashed.path, "S");
    ASSERT_FALSE(own.empty());
    EXPECT_EQ(own.back(), 4u)
        << "the counter continued from every origin's highest in the segment, leaving a hole a "
           "peer's frontier for this node never passes";
}

TEST(MeshPerOriginNumbering, ASymbolOnlyPeersWriteIsNotClaimedAfterARestart) {
    // #185: a start declared this node's own frontier up to the highest number in a segment, which
    // for a symbol only its peers write is theirs - so it claimed records nobody wrote, and every
    // reconciliation scanned its WAL for them.
    TempDir dir("mm_numbering_claim_");
    {
        auto node = open_node(dir.path);
        for (uint64_t seq = 1; seq <= 50; ++seq) {
            ASSERT_EQ(deliver(*node, record("T", seq, 1'000'000'000ULL + seq), kPeer), ob::OB_OK);
            ASSERT_EQ(deliver(*node, record("U", seq, 2'000'000'000ULL + seq), kPeer), ob::OB_OK);
        }
        for (uint64_t i = 1; i <= 2; ++i) write_own(*node, record("U", 0, 3'000'000'000ULL + i));
        node->flush_incremental();
        node->close();
    }
    auto node = open_node(dir.path);
    node->flush_tick_for_test();
    EXPECT_EQ(told(*node, "T.EX", kSelf), std::nullopt)
        << "the node claims to have written the numbers of a symbol only its peer writes";
    EXPECT_EQ(told(*node, "T.EX", kPeer), std::optional<uint64_t>(50));
    // And of a symbol both write, as far as it wrote - not as far as every origin's highest.
    EXPECT_EQ(told(*node, "U.EX", kSelf), std::optional<uint64_t>(2))
        << "the node claims numbers of a symbol both write that only its peer wrote";
    node->close();
}

TEST(MeshPerOriginNumbering, ASegmentFromBeforeThisIsReadAsItAlwaysWas) {
    // The control: a segment that does not say whose its rows are - one an older build wrote - is
    // read the way it was, the counter continued from every origin's highest.
    TempDir dir("mm_numbering_legacy_");
    {
        auto node = open_node(dir.path);
        for (uint64_t seq = 1; seq <= 50; ++seq) {
            ASSERT_EQ(deliver(*node, record("L", seq, 1'000'000'000ULL + seq), kPeer), ob::OB_OK);
        }
        for (uint64_t i = 1; i <= 2; ++i) write_own(*node, record("L", 0, 2'000'000'000ULL + i));
        node->flush_incremental();
        node->close();
    }
    make_segments_older(dir.path);
    ASSERT_EQ(metas_of(dir.path, "L").front().find("own_origin"), std::string::npos);
    auto node = open_node(dir.path);
    write_own(*node, record("L", 0, 3'000'000'000ULL));
    node->close();
    EXPECT_EQ(own_numbers(dir.path, "L").back(), 51u)
        << "a segment from before #184 is not read the way it always was";
}

TEST(MeshPerOriginNumbering, AMergeRecordsItsInputsOwnHighestAndWhetherItTookInAPeers) {
    // What a merge's segment says is its inputs' (set_own_max()), not its rows': they are rewritten
    // rows and say nothing about whose they are.
    TempDir dir("mm_numbering_merge_");
    ob::ColumnarStore store(dir.path, 3'600'000'000'000ULL, ob::ColumnarStore::OwnIndex::kNo);
    store.set_symbol_exchange("M", "EX");
    store.set_own_origin(7);

    std::vector<ob::SnapshotRow> rows;
    for (uint64_t seq = 1; seq <= 10; ++seq) {
        ob::SnapshotRow r{};
        r.timestamp_ns    = 1'000'000'000ULL + seq;
        r.sequence_number = seq;
        r.price           = static_cast<int64_t>(seq);
        r.quantity        = 1;
        rows.push_back(r);
    }
    const auto block = ob::RowBlock::make("M", "EX", rows, rows.front().timestamp_ns,
                                          rows.back().timestamp_ns, nullptr, /*max_own_seq=*/5,
                                          /*all_own=*/false);
    store.append_block(*block);
    const auto sealed = store.flush_segment();
    ASSERT_TRUE(sealed.has_value());
    EXPECT_TRUE(sealed->has_own_max);
    EXPECT_EQ(sealed->own_origin, 7u);
    EXPECT_EQ(sealed->own_max_sequence, 5u) << "a seal records its own rows' highest, not every row's";
    EXPECT_FALSE(sealed->has_received_rows);

    for (const auto& r : rows) store.append(r);
    store.set_own_max(true, 7, 42, /*received_rows=*/true);
    const auto merged = store.flush_segment();
    ASSERT_TRUE(merged.has_value());
    EXPECT_EQ(merged->own_max_sequence, 42u) << "a merge's answer is its inputs', not its rows'";
    EXPECT_TRUE(merged->has_received_rows);

    for (const auto& r : rows) store.append(r);
    store.set_own_max(false, 7, 0, false);
    const auto unknown = store.flush_segment();
    ASSERT_TRUE(unknown.has_value());
    EXPECT_FALSE(unknown->has_own_max) << "a merge of an input from before #184 claims to know";

    const auto on_disk = metas_of(dir.path, "M");
    ASSERT_EQ(on_disk.size(), 3u);
    size_t with_received = 0, without_own = 0;
    for (const auto& m : on_disk) {
        if (m.find("\"received_rows\":true") != std::string::npos) ++with_received;
        if (m.find("\"own_origin\"") == std::string::npos) ++without_own;
    }
    EXPECT_EQ(with_received, 1u);
    EXPECT_EQ(without_own, 1u);
}

TEST(MeshPerOriginNumbering, AFrontierOfThisNodesOwnFromASnapshotSurvivesARestart) {
    // After a wipe and a snapshot, this node's earlier records are in segments a peer sent, which
    // name the peer as whose rows they are - so a restart has only the vector for how far this
    // node's own numbers went, and continues its counter from it (restore_version_vector()).
    // Minting from 1 again hands out numbers the cluster has seen from this node, and every peer
    // drops the new records as the old ones.
    TempDir dir("mm_numbering_adopted_");
    {
        auto node = open_node(dir.path);
        node->adopt_snapshot_sequence_state({{"A.EX", kSelf, 50}}, {});
        node->close();
    }
    auto node = open_node(dir.path);
    write_own(*node, record("A", 0, 1'000'000'000ULL));
    node->close();
    EXPECT_EQ(own_numbers(dir.path, "A"), (std::vector<uint64_t>{51}))
        << "a restart minted numbers again that this node's vector says it had used";
}

TEST(MeshPerOriginNumbering, AMergeOfThisNodesSealsSaysHowFarItsOwnNumbersWent) {
    // The engine's merge (part 2b of #165) - not the store's override the test above drives - reads
    // its inputs' own highest and whether any took in a peer's segment: a merged segment that said
    // nothing would send a restart back to every origin's highest, which is the hole #184 is.
    TempDir dir("mm_numbering_merged_");
    auto node = open_node(dir.path);   // no flush thread of its own: every tick is the test's
    const size_t fan_in = ob::compaction::kFanIn;
    for (size_t i = 0; i < fan_in; ++i) {
        if (i % 2 == 0) {
            write_own(*node, record("M", 0, 1'000'000'000ULL + i));
        } else {
            ASSERT_EQ(deliver(*node, record("M", 100 + i, 1'000'000'000ULL + i), kPeer), ob::OB_OK);
        }
        node->flush_incremental();
    }
    ASSERT_EQ(metas_of(dir.path, "M").size(), fan_in) << "the premise: a seal each";
    for (int i = 0; i < 200 && metas_of(dir.path, "M").size() != 1; ++i) node->flush_tick_for_test();
    const auto metas = metas_of(dir.path, "M");
    ASSERT_EQ(metas.size(), 1u) << "the seals were not merged into one";
    EXPECT_NE(metas.front().find("\"merge_level\":1"), std::string::npos) << metas.front();
    EXPECT_NE(metas.front().find("\"own_origin\":1,\"own_max_sequence\":" +
                                 std::to_string(fan_in / 2)),
              std::string::npos)
        << "the merge did not say how far this node's own numbers went: " << metas.front();
    EXPECT_EQ(metas.front().find("\"received_rows\":true"), std::string::npos)
        << "a merge of this node's own seals said it took in a peer's segment: " << metas.front();
    node->close();
}

TEST(MeshPerOriginNumbering, APeersRecordsReplayedFromTheWalNeitherRaiseTheCounterNorBecomeOwn) {
    // A crash leaves the rows in the WAL alone. The replay seeds each record under its origin and a
    // peer's must not raise this node's counter (seed()); it puts the rows back in the pending
    // queue, and the segment they reach says whose they are as the replay knew it - a second
    // restart, when that segment is what speaks, is where a peer's rows counted as own would show.
    TempDir live("mm_numbering_replayed_live_");
    TempDir crashed("mm_numbering_replayed_crash_");
    {
        auto node = open_node(live.path);
        for (uint64_t i = 1; i <= 2; ++i) write_own(*node, record("R", 0, 1'000'000'000ULL + i));
        for (uint64_t seq = 1; seq <= 40; ++seq) {
            ASSERT_EQ(deliver(*node, record("R", seq, 2'000'000'000ULL + seq), kPeer), ob::OB_OK);
        }
        ASSERT_EQ(segments_on_disk(live.path), 0u) << "the premise: the WAL is all there is";
        crash_image(live.path, crashed.path);
        node->close();
    }
    {
        auto node = open_node(crashed.path);
        write_own(*node, record("R", 0, 3'000'000'000ULL));
        node->close();   // the replayed rows and this one reach a segment
    }
    auto node = open_node(crashed.path);
    write_own(*node, record("R", 0, 4'000'000'000ULL));
    node->close();
    EXPECT_EQ(own_numbers(crashed.path, "R"), (std::vector<uint64_t>{1, 2, 3, 4}))
        << "a peer's numbers raised this node's counter after a replay, or its replayed rows were "
           "recorded as this node's own";
}

TEST(MeshPerOriginNumbering, AMergeThatTakesInAPeersSegmentSaysSoAndCountsOnlyItsOwn) {
    // A segment a peer sealed - a snapshot's - names the peer as whose rows are its own, and its
    // own highest is the peer's. Merged with this node's seals, the result counts only this node's
    // own inputs and says it took in rows it cannot account for (`received_rows`), which a start
    // without a vector answers with every origin's highest. One of eight seals is made the peer's by
    // its meta.json, before a restart loads it: the only way a unit test has to a peer's segment.
    TempDir dir("mm_numbering_merged_peer_");
    const size_t fan_in = ob::compaction::kFanIn;
    {
        auto node = open_node(dir.path);
        for (size_t i = 0; i < fan_in; ++i) {
            write_own(*node, record("P", 0, 1'000'000'000ULL + i));
            node->flush_incremental();
        }
        node->close();
    }
    ASSERT_EQ(metas_of(dir.path, "P").size(), fan_in) << "the premise: a seal each";
    bool made = false;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(dir.path, ec);
         it != fs::recursive_directory_iterator() && !made; ++it) {
        if (!it->is_regular_file(ec) || it->path().filename() != "meta.json") continue;
        std::string text;
        {
            std::ifstream in(it->path());
            text.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
        const std::string mine = "\"own_origin\":1,\"own_max_sequence\":" + std::to_string(fan_in);
        const auto at = text.find(mine);
        if (at == std::string::npos) continue;
        text.replace(at, mine.size(), "\"own_origin\":2,\"own_max_sequence\":900");
        std::ofstream(it->path(), std::ios::trunc) << text;
        made = true;
    }
    ASSERT_TRUE(made) << "the premise: the last seal's meta.json names this node's own highest";

    auto node = open_node(dir.path);
    for (int i = 0; i < 200 && metas_of(dir.path, "P").size() != 1; ++i) node->flush_tick_for_test();
    const auto metas = metas_of(dir.path, "P");
    ASSERT_EQ(metas.size(), 1u) << "the seals were not merged into one";
    EXPECT_NE(metas.front().find("\"own_origin\":1,\"own_max_sequence\":" +
                                 std::to_string(fan_in - 1)),
              std::string::npos)
        << "the merge counted the peer's own numbers as this node's: " << metas.front();
    EXPECT_NE(metas.front().find("\"received_rows\":true"), std::string::npos)
        << "the merge did not say it took in a peer's segment: " << metas.front();
    node->close();
}
