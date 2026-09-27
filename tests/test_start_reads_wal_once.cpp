// A start reads its WAL once (#174).
//
// Each reader a start had - the last checkpoint, the version vector, the held numbers, the tail,
// the epoch - replayed the whole WAL on its own, a record at a time: 23 s to start on a WAL of
// 422 MB, which is how long a node that crashed stays out. The replay reads blocks now, and a start
// makes two passes: the last checkpoint's, which gathers everything else the start takes from the
// whole log, and the tail's. The first test holds the start to that number.
//
// The epoch was read by `replay()`, which had a parser of its own and read a mesh node's 38-byte
// headers as 24-byte ones: every restart of a mesh node logged that a record had been cut short by
// the process stopping, when none had. The second test is that restart.
//
// What the one pass restores - the vector, the held numbers, the epoch - is held to what the five
// did by the tests that restart a node for them: test_mm_restart_origins.cpp, test_wal_dir.cpp,
// test_mm_legacy_numbering.cpp.

#include "orderbook/engine.hpp"
#include "mm_engine_node.hpp"
#include "test_ports.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>

namespace {

using namespace mm_engine;

std::atomic<uint16_t> g_port{ob::test::kPortsStartReadsWalOnce};

std::unique_ptr<ob::Engine> single_node(const std::string& dir) {
    return std::make_unique<ob::Engine>(dir, kNoAutoFlush, ob::FsyncPolicy::INTERVAL,
                                        ob::ReplicationConfig{}, ob::ReplicationClientConfig{},
                                        ob::FailoverConfig{}, ob::TTLConfig{},
                                        ob::MultiMasterConfig{});
}

void write_rows(ob::Engine& engine, const char* symbol, uint64_t first_ts, int count) {
    for (int i = 0; i < count; ++i) {
        const Record r = record(symbol, 0, first_ts + static_cast<uint64_t>(i));
        ASSERT_EQ(engine.apply_delta(r.delta, r.levels.data()), ob::OB_OK);
    }
}

}  // namespace

TEST(StartReadsWalOnce, AStartPassesOverTheWalTwice) {
    // A WAL with everything a start reads from it: sealed rows and the checkpoint that covers them,
    // a vector beside it, a promotion's epoch, and a tail no checkpoint covers - the directory a
    // crash leaves.
    TempDir live("start_once_live_");
    TempDir crashed("start_once_crash_");
    {
        auto engine = single_node(live.path);
        engine->open();
        write_rows(*engine, "ONCE", 1'000'000'000ULL, 20);
        engine->flush_incremental();
        engine->promote_to_primary(ob::EpochValue{5});
        write_rows(*engine, "ONCE", 2'000'000'000ULL, 7);
        crash_image(live.path, crashed.path);
        engine->close();
    }

    auto engine = single_node(crashed.path);
    const uint64_t before = ob::WALReplayer::passes_for_test();
    engine->open();
    const uint64_t passes = ob::WALReplayer::passes_for_test() - before;

    EXPECT_EQ(passes, 2u) << "a start read the whole WAL " << passes << " times";
    EXPECT_EQ(rows(*engine, "ONCE"), 27) << "the tail was not replayed";
    EXPECT_EQ(engine->current_epoch(), 5u) << "the epoch was not read from the one pass";
    engine->close();
}

TEST(StartReadsWalOnce, AMeshNodeRestartsWithoutReportingARecordCutShort) {
    // A mesh node's own records and a peer's, all with 38-byte headers, and a clean stop: nothing
    // in its WAL was cut short, and its restart says nothing of the kind.
    TempDir dir("start_once_mesh_");
    {
        auto node = mm_engine::open_node(g_port, dir.path);
        for (uint64_t i = 1; i <= 4; ++i) write_own(*node, record("MESH", 0, 3'000'000'000ULL + i));
        for (uint64_t seq = 1; seq <= 3; ++seq) {   // priced apart from this node's own levels
            ASSERT_EQ(deliver(*node, record("MESH", seq, 4'000'000'100ULL + seq), kPeer), ob::OB_OK);
        }
        node->close();
    }

    testing::internal::CaptureStderr();
    auto node = mm_engine::open_node(g_port, dir.path);
    const std::string said = testing::internal::GetCapturedStderr();

    EXPECT_EQ(said.find("checksum mismatch"), std::string::npos)
        << "a restart reported a record cut short in a WAL that holds none:\n" << said;
    EXPECT_EQ(rows(*node, "MESH"), 7);
    node->close();
}
