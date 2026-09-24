// #165 part 2a: which segments a checkpoint vouches for while rows wait in blocks.
//
// A seal writes the blocks of several drains into one segment positioned at the last of them, and
// a late seal of old blocks gives a segment positioned *before* the checkpoint it was written after
// - so a position cannot say which segments are durable once rows wait. The first design had the
// checkpoint claim less and nothing more, and walking a crash through it found the rows it lost:
// a segment positioned past the claim was removed at start, and replay from the claim rebuilt only
// its rows recorded after it. Segments now carry the epoch of the flush that sealed them, and a
// checkpoint written while rows wait names the epoch it vouches for.

#include "orderbook/engine.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <regex>
#include <sstream>
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
                ("ob_seal_epochs_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1)))).string()) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

void insert(ob::Engine& engine, const char* symbol, uint64_t first_seq, size_t levels) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.sequence_number = first_seq;
    delta.timestamp_ns    = 1'790'000'000'000'000'000ULL + first_seq;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = static_cast<uint16_t>(levels);
    std::vector<ob::Level> lv(levels);
    for (size_t i = 0; i < levels; ++i) {
        lv[i] = ob::Level{};
        lv[i].price = static_cast<int64_t>(1000 + i);
        lv[i].qty   = 1;
        lv[i].cnt   = 1;
    }
    ASSERT_EQ(engine.apply_delta(delta, lv.data()), ob::OB_OK);
}

template <typename F>
bool eventually(F&& done, std::chrono::milliseconds within = 5000ms) {
    const auto deadline = Clock::now() + within;
    while (Clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return done();
}

/// The seal epoch every segment of `symbol` on disk carries, read from its meta.json.
std::vector<uint64_t> epochs_on_disk(const std::string& dir, const std::string& symbol) {
    std::vector<uint64_t> out;
    const std::regex field("\"seal_epoch\":([0-9]+)");
    std::error_code ec;
    for (auto& e : fs::recursive_directory_iterator(dir, ec)) {
        if (e.path().filename() != "meta.json") continue;
        if (e.path().string().find("/" + symbol + "/") == std::string::npos) continue;
        std::ifstream in(e.path());
        std::stringstream ss;
        ss << in.rdbuf();
        std::smatch m;
        const std::string text = ss.str();
        if (std::regex_search(text, m, field)) out.push_back(std::stoull(m[1].str()));
    }
    return out;
}

ob::SegmentMeta local_segment(uint64_t identity, uint32_t file, uint64_t offset, uint64_t epoch) {
    ob::SegmentMeta meta{};
    meta.wal_identity    = identity;
    meta.wal_file_index  = file;
    meta.wal_byte_offset = offset;
    meta.seal_epoch      = epoch;
    return meta;
}

constexpr uint64_t kLocal = 0xABCDEF;

ob::WALReplayer::LastCheckpoint epoch_checkpoint(ob::WalPosition from, uint64_t epoch) {
    ob::WALReplayer::LastCheckpoint last{};
    last.ordinal    = 7;
    last.covered    = from;
    last.seal_epoch = epoch;
    last.any_record = true;
    return last;
}

ob::WALReplayer::LastCheckpoint position_checkpoint(ob::WalPosition covered) {
    ob::WALReplayer::LastCheckpoint last{};
    last.ordinal    = 7;
    last.covered    = covered;
    last.any_record = true;
    return last;
}

}  // namespace

// ── The payload ─────────────────────────────────────────────────────────────

TEST(CheckpointClaim, TheEpochFormCarriesWhereReplayStartsAndTheEpoch) {
    uint8_t payload[ob::CHECKPOINT_EPOCH_PAYLOAD_BYTES];
    ob::checkpoint_epoch_payload(ob::WalPosition{3, 4096}, 0x0102030405060708ULL, payload);
    const auto claim = ob::checkpoint_claim(payload, sizeof(payload));
    ASSERT_TRUE(claim.has_value());
    EXPECT_EQ(claim->replay_from.file_index, 3u);
    EXPECT_EQ(claim->replay_from.offset, 4096u);
    ASSERT_TRUE(claim->seal_epoch.has_value());
    EXPECT_EQ(*claim->seal_epoch, 0x0102030405060708ULL);
}

TEST(CheckpointClaim, TheEightByteFormNamesNoEpoch) {
    uint8_t payload[ob::CHECKPOINT_PAYLOAD_BYTES];
    ob::checkpoint_payload(ob::WalPosition{2, 77}, payload);
    const auto claim = ob::checkpoint_claim(payload, sizeof(payload));
    ASSERT_TRUE(claim.has_value());
    EXPECT_EQ(claim->replay_from.file_index, 2u);
    EXPECT_EQ(claim->replay_from.offset, 77u);
    EXPECT_FALSE(claim->seal_epoch.has_value());
}

TEST(CheckpointClaim, AnOlderReaderTakesTheEpochFormAsSayingNothing) {
    // What a build before this one does with it - the reason a node is downgraded only from a clean
    // stop, whose last checkpoint has the eight bytes.
    uint8_t payload[ob::CHECKPOINT_EPOCH_PAYLOAD_BYTES];
    ob::checkpoint_epoch_payload(ob::WalPosition{1, 1}, 9, payload);
    EXPECT_FALSE(ob::checkpoint_covered(payload, sizeof(payload)).has_value());
}

TEST(CheckpointClaim, AnyOtherLengthSaysNothing) {
    uint8_t payload[ob::CHECKPOINT_EPOCH_PAYLOAD_BYTES]{};
    EXPECT_FALSE(ob::checkpoint_claim(payload, 0).has_value());
    EXPECT_FALSE(ob::checkpoint_claim(payload, 12).has_value());
    EXPECT_FALSE(ob::checkpoint_claim(nullptr, ob::CHECKPOINT_PAYLOAD_BYTES).has_value());
}

// ── Which segments a checkpoint vouches for ─────────────────────────────────

TEST(SegmentVouchedFor, AnEpochCheckpointVouchesForTheSegmentsSealedUpToItsEpoch) {
    const auto last = epoch_checkpoint(ob::WalPosition{1, 100}, 5);
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal, 1, 50, 4), last, kLocal));
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal, 1, 50, 5), last, kLocal));
    EXPECT_FALSE(ob::Engine::segment_vouched_for(local_segment(kLocal, 1, 50, 6), last, kLocal))
        << "a segment sealed after the checkpoint was kept although no sync vouched for it";
}

TEST(SegmentVouchedFor, UnderAnEpochCheckpointAPositionPastWhereReplayStartsIsNoReason) {
    // The case the position rule gets wrong: a segment sealed at or before the epoch, holding rows
    // of records past the oldest waiting block. Removed, its earlier rows would be lost - replay
    // starts at that block and rebuilds only what comes after it.
    const auto last = epoch_checkpoint(ob::WalPosition{1, 100}, 5);
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal, 1, 900, 5), last, kLocal));
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal, 4, 0, 3), last, kLocal));
}

TEST(SegmentVouchedFor, ASegmentSealedBeforeEpochsExistedIsVouchedForByAnEpochCheckpoint) {
    const auto last = epoch_checkpoint(ob::WalPosition{1, 100}, 5);
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal, 9, 9, 0), last, kLocal));
}

TEST(SegmentVouchedFor, AnotherWalsSegmentIsNotThisLogsToJudge) {
    const auto by_epoch    = epoch_checkpoint(ob::WalPosition{1, 100}, 5);
    const auto by_position = position_checkpoint(ob::WalPosition{1, 100});
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal + 1, 8, 8, 99), by_epoch, kLocal));
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal + 1, 8, 8, 99), by_position,
                                                kLocal));
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(0, 8, 8, 99), by_epoch, kLocal));
}

TEST(SegmentVouchedFor, APositionCheckpointVouchesForTheSegmentsAtOrBeforeIt) {
    const auto last = position_checkpoint(ob::WalPosition{1, 100});
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal, 1, 50, 7), last, kLocal));
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal, 1, 100, 7), last, kLocal));
    EXPECT_FALSE(ob::Engine::segment_vouched_for(local_segment(kLocal, 1, 150, 7), last, kLocal));
    EXPECT_FALSE(ob::Engine::segment_vouched_for(local_segment(kLocal, 2, 0, 7), last, kLocal));
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal, 0, 0, 7), last, kLocal))
        << "a segment with no position was judged by one";
}

TEST(SegmentVouchedFor, NoCheckpointInALogFromItsFirstFileVouchesForNothingPositioned) {
    ob::WALReplayer::LastCheckpoint none{};
    none.any_record       = true;
    none.first_file_index = 0;
    EXPECT_FALSE(ob::Engine::segment_vouched_for(local_segment(kLocal, 0, 10, 0), none, kLocal));
}

TEST(SegmentVouchedFor, ACheckpointThatSaysNothingVouchesForEverySegment) {
    ob::WALReplayer::LastCheckpoint older{};
    older.ordinal    = 3;                    // a checkpoint, with no claim it can read
    older.any_record = true;
    EXPECT_TRUE(ob::Engine::segment_vouched_for(local_segment(kLocal, 5, 5, 5), older, kLocal));
}

// ── The engine's checkpoints and epochs ─────────────────────────────────────

TEST(SealEpochs, ACheckpointWrittenWhileABlockWaitsNamesTheEpochItsSyncCovered) {
    TempDir dir;
    ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::INTERVAL);
    engine.open();
    for (uint64_t seq = 1; seq <= ob::Engine::kSealRows / 1000 + 1; ++seq) insert(engine, "A", seq, 1000);
    insert(engine, "B", 1, 7);
    ASSERT_TRUE(eventually([&] { return !epochs_on_disk(dir.path, "A").empty(); }))
        << "the store over the rows threshold was never sealed";
    ASSERT_TRUE(eventually([&] {
        return engine.registry().gauge_value("ob_unsealed_rows") == 7;
    })) << "B's rows were not left waiting, so this proves nothing about the epoch form";

    ob::WALReplayer replayer(dir.path);
    const auto last = replayer.find_last_checkpoint();
    ASSERT_TRUE(last.seal_epoch.has_value()) << "a checkpoint written while B waited named no epoch";
    const auto a_epochs = epochs_on_disk(dir.path, "A");
    for (uint64_t e : a_epochs) {
        EXPECT_LE(e, *last.seal_epoch) << "a sealed segment is past the epoch the checkpoint vouches for";
        EXPECT_GE(e, 1u) << "a sealed segment carries no epoch";
    }
    engine.close();
}

TEST(SealEpochs, AFlushThatLeavesNothingWaitingWritesTheEightBytes) {
    // Which is what makes a clean stop's last checkpoint one a build before epochs can read.
    TempDir dir;
    {
        ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::INTERVAL);
        engine.open();
        insert(engine, "A", 1, 10);
        insert(engine, "B", 1, 10);
        ASSERT_TRUE(eventually([&] { return engine.registry().gauge_value("ob_unsealed_rows") == 20; }));
        engine.close();
    }
    ob::WALReplayer replayer(dir.path);
    const auto last = replayer.find_last_checkpoint();
    ASSERT_GT(last.ordinal, 0u) << "close() wrote no checkpoint";
    EXPECT_FALSE(last.seal_epoch.has_value()) << "a stop that left nothing waiting named an epoch";
    EXPECT_TRUE(last.covered.has_value());
}

TEST(SealEpochs, TheEpochContinuesAfterAReopen) {
    // Starting again at zero would stamp the next seals with epochs an old checkpoint vouches for.
    TempDir dir;
    uint64_t before = 0;
    {
        ob::Engine engine(dir.path, 20'000'000ULL, ob::FsyncPolicy::INTERVAL);
        engine.open();
        insert(engine, "A", 1, 10);
        engine.flush_incremental();
        insert(engine, "A", 2, 10);
        engine.flush_incremental();
        engine.close();
        for (uint64_t e : epochs_on_disk(dir.path, "A")) before = std::max(before, e);
    }
    ASSERT_GE(before, 2u) << "two FLUSHes that each sealed did not give two epochs";
    ob::Engine reopened(dir.path, 20'000'000ULL, ob::FsyncPolicy::INTERVAL);
    reopened.open();
    insert(reopened, "C", 1, 10);
    reopened.flush_incremental();
    const auto c = epochs_on_disk(dir.path, "C");
    ASSERT_EQ(c.size(), 1u);
    EXPECT_GT(c[0], before) << "the epoch started again after the reopen";
    reopened.close();
}
