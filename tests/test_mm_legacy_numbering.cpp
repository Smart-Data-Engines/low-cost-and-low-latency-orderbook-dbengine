// A mesh node's numbering from before per-origin numbers, closed once at the upgrade (#187).
//
// Before #184 a mesh node numbered a symbol with one counter that every origin's numbers raised, so
// the numbers of each origin of a symbol two nodes wrote had holes where the other's were. Measured on
// a mesh upgraded across #184 with such a symbol: after an outage the writers held 11 904 and 11 404
// rows, and the node back 16 232, where 11 000 were written - every frontier stopped at a hole, the
// numbers above it filled the held set to its cap, and a redelivery past the set was stored again.
// And every start declared this node's own frontier from those segments' highest number, which claimed
// the numbers its peers wrote (#185, for data from before #184) and started the catch-ups.
//
// A store from before #184 is made here the way that build left one: the peers' records applied with
// the old numbering - one counter across both - flushed, and every segment's meta.json without the
// fields #184 added.

#include "orderbook/engine.hpp"
#include "mm_engine_node.hpp"
#include "test_ports.hpp"
#include "orderbook/sequence_tracker.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <regex>
#include <sstream>
#include <string>

namespace fs = std::filesystem;
using mm_engine::deliver;
using mm_engine::record;
using mm_engine::rows;
using mm_engine::told;
using mm_engine::write_own;

namespace {

std::atomic<uint16_t> g_port{ob::test::kPortsMmLegacyNumbering};

constexpr uint64_t kBase = ob::kClosedNumberingBase;
constexpr uint16_t kSelf = mm_engine::kSelf;           // 1
constexpr uint16_t kPeerA = 2;
constexpr uint16_t kPeerB = 3;
const std::string kKey = "LG01.EX";                    // the peers' symbol, in turns
const std::string kOwnKey = "LG02.EX";                 // this node's own, alone
constexpr uint64_t kTs = 1'700'000'000'000'000'000ULL;

/// Without syncs: nothing here is about durability, and thousands of records are applied one by one.
std::unique_ptr<ob::Engine> open_node(const std::string& dir) {
    return mm_engine::open_node(g_port, dir, ob::FsyncPolicy::NONE);
}

/// Every segment's meta.json as a build before #184 wrote it: without own_origin, own_max_sequence
/// and received_rows.
size_t strip_own_fields(const std::string& dir) {
    static const std::regex own(R"(,"own_origin":[0-9]+,"own_max_sequence":[0-9]+(,"received_rows":true)?)");
    size_t n = 0;
    for (const auto& e : fs::recursive_directory_iterator(dir)) {
        if (!e.is_regular_file() || e.path().filename() != "meta.json") continue;
        std::string text;
        {
            std::ifstream in(e.path());
            text.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
        const std::string stripped = std::regex_replace(text, own, "");
        if (stripped != text) ++n;
        std::ofstream(e.path(), std::ios::trunc) << stripped;
    }
    return n;
}

/// `rounds` of `per` records of LG01 from the two peers in turns, numbered as before #184: one counter
/// across both, so peer A holds 1-per, peer B per+1-2per, peer A again, and so on. Returns the last.
uint64_t old_numbering(ob::Engine& engine, int rounds, int per, uint64_t& ts) {
    uint64_t seq = 0;
    for (int r = 0; r < rounds; ++r) {
        const uint16_t origin = (r % 2 == 0) ? kPeerA : kPeerB;
        for (int k = 0; k < per; ++k) {
            EXPECT_EQ(deliver(engine, record("LG01", ++seq, ts++), origin), ob::OB_OK);
        }
    }
    return seq;
}

/// What a peer is told, after the tick that brings the copy it is told from up to date (#180).
std::optional<uint64_t> told_now(ob::Engine& engine, const std::string& key, uint16_t origin) {
    engine.flush_incremental();
    return told(engine, key, origin);
}

bool closed_note(const std::string& dir) {
    return fs::exists(fs::path(dir) / "numbering_closed");
}

/// A store as a mesh node of a build before #184 leaves it: the peers' gapped streams of LG01, this
/// node's own LG02, flushed, and the segments without #184's fields.
struct LegacyStore {
    mm_engine::TempDir dir{"legacy_numbering_"};
    uint64_t ts{kTs};
    uint64_t last{0};
    int own_rows{0};

    LegacyStore(int rounds, int per) {
        auto engine = open_node(dir.path);
        last = old_numbering(*engine, rounds, per, ts);
        for (int k = 0; k < 5; ++k) write_own(*engine, record("LG02", 0, ts++));
        own_rows = 5;
        engine->flush_incremental();
        engine->close();
        EXPECT_GT(strip_own_fields(dir.path), 0u) << "no segment to make from before #184";
    }
};

}  // namespace

TEST(MeshLegacyNumbering, TheFirstStartClosesEveryOriginsNumberingAndNotes) {
    LegacyStore store(/*rounds=*/4, /*per=*/5);
    auto engine = open_node(store.dir.path);
    EXPECT_EQ(told(*engine, kKey, kPeerA), kBase - 1) << "peer A's frontier stayed at its first hole";
    EXPECT_EQ(told(*engine, kKey, kPeerB), kBase - 1) << "peer B's frontier stayed at its first hole";
    EXPECT_EQ(told(*engine, kOwnKey, kSelf), kBase - 1);
    EXPECT_TRUE(closed_note(store.dir.path));
    engine->close();
}

TEST(MeshLegacyNumbering, ANodeClaimsNoNumbersOfItsOwnForASymbolItDidNotWrite) {
    // #185 for data from before #184: a start declared this node's own frontier from the segment's
    // highest number - every origin's - and so claimed its peers' numbers as its own.
    LegacyStore store(4, 5);
    auto engine = open_node(store.dir.path);
    EXPECT_FALSE(told_now(*engine, kKey, kSelf).has_value())
        << "a node that never wrote LG01 states a frontier of its own for it";
    engine->close();
}

TEST(MeshLegacyNumbering, ThisNodesOwnNumbersGoOnFromTheBase) {
    LegacyStore store(4, 5);
    auto engine = open_node(store.dir.path);
    write_own(*engine, record("LG02", 0, store.ts++));
    EXPECT_EQ(told_now(*engine, kOwnKey, kSelf), kBase) << "the own record was not numbered from the base";
    write_own(*engine, record("LG01", 0, store.ts++));
    EXPECT_EQ(told_now(*engine, kKey, kSelf), kBase)
        << "a first own record of a closed symbol did not take the base, in order";
    engine->close();
}

TEST(MeshLegacyNumbering, ARedeliveryOfTheOldNumberingIsNotStoredAgain) {
    // What the mesh measured: past the held set's cap every redelivered record was stored twice.
    // Three rounds of 4 100 put 4 100 of each peer's numbers above its first hole - four past the
    // 4 096 a held set keeps.
    constexpr int kRounds = 3, kPer = 4'100;
    LegacyStore store(kRounds, kPer);
    auto engine = open_node(store.dir.path);
    const int before = rows(*engine, "LG01");
    ASSERT_EQ(before, kRounds * kPer);
    uint64_t seq = 0, ts = kTs;
    for (int r = 0; r < kRounds; ++r) {
        const uint16_t origin = (r % 2 == 0) ? kPeerA : kPeerB;
        for (int k = 0; k < kPer; ++k) {
            ASSERT_EQ(deliver(*engine, record("LG01", ++seq, ts++), origin), ob::OB_OK);
        }
    }
    EXPECT_EQ(rows(*engine, "LG01"), before) << "the old numbering's records were stored again";
    engine->close();
}

TEST(MeshLegacyNumbering, APeersRecordsFromTheBaseAreTakenInOrder) {
    LegacyStore store(4, 5);
    auto engine = open_node(store.dir.path);
    const int before = rows(*engine, "LG01");
    ASSERT_EQ(deliver(*engine, record("LG01", kBase, store.ts++), kPeerB), ob::OB_OK);
    ASSERT_EQ(deliver(*engine, record("LG01", kBase + 1, store.ts++), kPeerB), ob::OB_OK);
    EXPECT_EQ(told_now(*engine, kKey, kPeerB), kBase + 1);
    EXPECT_EQ(rows(*engine, "LG01"), before + 2);
    engine->close();
}

TEST(MeshLegacyNumbering, ALaterStartDoesNotCloseAgain) {
    // After the close a node that joined numbers the symbol from 1; closing again would take its
    // records below the base for ones this node has - the next ones dropped as duplicates.
    LegacyStore store(4, 5);
    {
        auto engine = open_node(store.dir.path);
        ASSERT_TRUE(closed_note(store.dir.path));
        for (uint64_t s = 1; s <= 3; ++s) {
            ASSERT_EQ(deliver(*engine, record("LG01", s, store.ts++), /*origin=*/4), ob::OB_OK);
        }
        engine->flush_incremental();
        engine->close();
    }
    auto engine = open_node(store.dir.path);
    EXPECT_EQ(told(*engine, kKey, 4), 3u) << "a later start closed the numbering again";
    ASSERT_EQ(deliver(*engine, record("LG01", 4, store.ts++), 4), ob::OB_OK);
    EXPECT_EQ(told_now(*engine, kKey, 4), 4u) << "the joined node's next record was not taken";
    EXPECT_EQ(told(*engine, kKey, kPeerA), kBase - 1);
    engine->close();
}

TEST(MeshLegacyNumbering, AStoreWrittenSincePerOriginNumbersIsLeftAlone) {
    mm_engine::TempDir dir("legacy_numbering_none_");
    uint64_t ts = kTs;
    {
        auto engine = open_node(dir.path);
        old_numbering(*engine, 2, 5, ts);
        engine->flush_incremental();
        engine->close();
    }
    auto engine = open_node(dir.path);
    EXPECT_FALSE(closed_note(dir.path)) << "a store with no segment from before #184 was closed";
    EXPECT_EQ(told(*engine, kKey, kPeerA), 5u);
    engine->close();
}

TEST(MeshLegacyNumbering, AnInstalledSnapshotCarriesWhetherItsNumberingIsClosed) {
    // The note does not travel with a snapshot, and the closed frontiers do: a node that installs one
    // and holds its segments from before #184 would otherwise close again at its next start.
    mm_engine::TempDir dir("legacy_numbering_snapshot_");
    auto engine = open_node(dir.path);
    engine->adopt_snapshot_sequence_state({{kKey, kPeerA, kBase - 1}}, {});
    EXPECT_TRUE(closed_note(dir.path)) << "a closed snapshot's numbering was not noted";
    engine->adopt_snapshot_sequence_state({{kKey, kPeerA, 12}}, {});
    EXPECT_FALSE(closed_note(dir.path)) << "an open snapshot's numbering was noted closed";
    engine->close();
}

TEST(MeshLegacyNumbering, AStoreOutsideAMeshIsNotClosed) {
    // Outside a mesh every row is origin 0 and a segment's highest number is its frontier, as it
    // always was: there are no other origins' numbers to have left holes.
    mm_engine::TempDir dir("legacy_numbering_standalone_");
    uint64_t ts = kTs;
    const auto standalone = [&dir] {
        auto e = std::make_unique<ob::Engine>(dir.path, mm_engine::kNoAutoFlush, ob::FsyncPolicy::NONE);
        e->open();
        return e;
    };
    {
        auto engine = standalone();
        for (int k = 0; k < 7; ++k) {
            auto r = record("LG03", 0, ts++);
            ASSERT_EQ(engine->apply_delta(r.delta, r.levels.data()), ob::OB_OK);
        }
        engine->flush_incremental();
        engine->close();
    }
    ASSERT_GT(strip_own_fields(dir.path), 0u);
    auto engine = standalone();
    EXPECT_FALSE(closed_note(dir.path)) << "a store outside a mesh was closed";
    engine->flush_incremental();
    EXPECT_EQ(told(*engine, "LG03.EX", 0), 7u)
        << "origin 0's frontier is not the segment's highest number";
    engine->close();
}
