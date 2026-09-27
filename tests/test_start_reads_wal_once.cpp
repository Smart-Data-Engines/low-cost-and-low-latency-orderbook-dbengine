// A start reads its WAL once (#174).
//
// Each reader a start had - the last checkpoint, the version vector, the held numbers, the tail,
// the epoch - replayed the whole WAL on its own, a record at a time: 23 s to start on a WAL of
// 422 MB, which is how long a node that crashed stays out. The replay reads blocks now, and a start
// reads the WAL once - the last checkpoint's pass, which gathers everything else the start takes
// from the whole log - and then what that checkpoint does not cover, from the pass's last mark
// before it. The first tests hold a start to that, by the bytes it reads.
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
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace {

using namespace mm_engine;

std::atomic<uint16_t> g_port{ob::test::kPortsStartReadsWalOnce};

std::unique_ptr<ob::Engine> single_node(const std::string& dir, size_t rotate_bytes = 512ULL << 20) {
    return std::make_unique<ob::Engine>(dir, kNoAutoFlush, ob::FsyncPolicy::INTERVAL,
                                        ob::ReplicationConfig{}, ob::ReplicationClientConfig{},
                                        ob::FailoverConfig{}, ob::TTLConfig{},
                                        ob::MultiMasterConfig{}, rotate_bytes);
}

/// `count` records of `levels` levels each, one row a level.
void write_rows(ob::Engine& engine, const char* symbol, uint64_t first_ts, int count,
                uint16_t levels = 1) {
    for (int i = 0; i < count; ++i) {
        const Record r = record(symbol, 0, first_ts + static_cast<uint64_t>(i), levels);
        ASSERT_EQ(engine.apply_delta(r.delta, r.levels.data()), ob::OB_OK);
    }
}

uint64_t wal_bytes(const std::string& dir) {
    uint64_t n = 0;
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        const std::string name = e.path().filename().string();
        if (name.rfind("wal_", 0) == 0 && e.path().extension() == ".bin") n += e.file_size();
    }
    return n;
}

/// The most a start reads past one reading of its WAL when nothing is left to replay: from the
/// last mark before its checkpoint - a block apart - to the end.
constexpr uint64_t kSlack = 2 * ob::WALReplayer::kDefaultReadBlockBytes;

/// What open() read from the WAL.
uint64_t bytes_to_open(ob::Engine& engine) {
    const uint64_t before = ob::WALReplayer::bytes_read_for_test();
    engine.open();
    return ob::WALReplayer::bytes_read_for_test() - before;
}

}  // namespace

TEST(StartReadsWalOnce, ACleanStopsStartReadsItsWalOnce) {
    // Eight megabytes of WAL, every record of it covered by the last checkpoint: a start read it
    // five times whole, then twice in blocks, and reads it once now.
    TempDir dir("start_once_clean_");
    {
        auto engine = single_node(dir.path);
        engine->open();
        write_rows(*engine, "ONCE", 1'000'000'000ULL, 8000, 40);
        engine->close();
    }
    const uint64_t wal = wal_bytes(dir.path);
    ASSERT_GT(wal, 4 * kSlack) << "the premise: a WAL much longer than the slack";

    auto engine = single_node(dir.path);
    const uint64_t read = bytes_to_open(*engine);
    EXPECT_GE(read, wal) << "a start that did not read its whole WAL cannot have found its last "
                            "checkpoint";
    EXPECT_LE(read, wal + kSlack) << "a start read " << read << " bytes of a WAL of " << wal;
    EXPECT_EQ(rows(*engine, "ONCE"), 8000 * 40);
    engine->close();
}

TEST(StartReadsWalOnce, ACrashedNodesStartReadsItsWalOnceAndThenTheTail) {
    // A WAL with everything a start takes from it - sealed rows and the checkpoint that covers them,
    // a vector beside it, a promotion's epoch - and a tail no checkpoint covers: the directory a
    // crash leaves. The tail is read twice, once in the first pass and once to replay it.
    TempDir live("start_once_live_");
    TempDir crashed("start_once_crash_");
    uint64_t tail_from = 0;
    {
        auto engine = single_node(live.path);
        engine->open();
        write_rows(*engine, "ONCE", 1'000'000'000ULL, 8000, 40);
        engine->flush_incremental();
        engine->promote_to_primary(ob::EpochValue{5});
        tail_from = wal_bytes(live.path);
        write_rows(*engine, "ONCE", 2'000'000'000ULL, 700, 40);
        crash_image(live.path, crashed.path);
        engine->close();
    }
    const uint64_t wal  = wal_bytes(crashed.path);
    const uint64_t tail = wal - tail_from;
    ASSERT_GT(wal, 4 * kSlack) << "the premise: a WAL much longer than the slack";

    auto engine = single_node(crashed.path);
    const uint64_t read = bytes_to_open(*engine);
    EXPECT_GE(read, wal + tail);
    EXPECT_LE(read, wal + tail + kSlack) << "a start read " << read << " bytes of a WAL of " << wal
                                         << " with a tail of " << tail;
    EXPECT_EQ(rows(*engine, "ONCE"), 8700 * 40) << "the tail was not replayed";
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

TEST(StartReadsWalOnce, AStartReportsTheTornFilesItsFirstPassSteppedOver) {
    // A file before the last torn in its middle, as a build before #126 left one, and a clean stop
    // after it: the tail pass begins in the last file and reads none of the first, so what the start
    // says it stepped over is the first pass's count.
    TempDir dir("start_once_torn_");
    {
        auto engine = single_node(dir.path, 1u << 20);
        engine->open();
        write_rows(*engine, "TORN", 1'000'000'000ULL, 3000, 40);
        engine->close();
    }
    const std::string first = dir.path + "/wal_000000.bin";
    ASSERT_TRUE(std::filesystem::exists(dir.path + "/wal_000002.bin")) << "the premise: three files";
    ASSERT_TRUE(std::filesystem::exists(first)) << "the premise: the first file is still there";
    {
        // A byte of the payload of a record half way through the file, changed: its checksum no
        // longer holds. The checksum covers the payload alone, so the byte is found by a replay.
        const uint64_t half = std::filesystem::file_size(first) / 2;
        uint64_t payload_at = 0;
        ob::WALReplayer replayer(dir.path);
        replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
            if (payload_at == 0 && ctx.wal_file_index == 0 && ctx.wal_byte_offset > half &&
                ctx.header.record_type == ob::WAL_RECORD_DELTA) {
                payload_at = ctx.wal_byte_offset + sizeof(ob::WALRecord) + ctx.payload_len / 2;
            }
        });
        ASSERT_GT(payload_at, 0u);
        std::fstream f(first, std::ios::in | std::ios::out | std::ios::binary);
        char byte = 0;
        f.seekg(static_cast<std::streamoff>(payload_at));
        f.read(&byte, 1);
        byte = static_cast<char>(byte ^ 0x5a);
        f.seekp(static_cast<std::streamoff>(payload_at));
        f.write(&byte, 1);
    }

    auto engine = single_node(dir.path, 1u << 20);
    testing::internal::CaptureStderr();
    engine->open();
    const std::string said = testing::internal::GetCapturedStderr();
    EXPECT_NE(said.find("1 WAL file(s) ended in a torn record and were stepped over"), std::string::npos)
        << said;
    engine->close();
}
