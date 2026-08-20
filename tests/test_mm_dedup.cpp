// Receive-side deduplication: catch-up over-delivers on purpose, so the receiver has to drop
// what it already applied.
//
// The order of discovery matters here. Fixing #61 by streaming everything a peer might be
// missing turned data loss into duplicate rows — measured as four outage cycles storing 25
// rows where 9 were written — because storage is append-only and LWW resolves price levels in
// the SoA buffer, not appends to the columnar store. Over-delivery is only harmless if the
// receiver refuses a record it has seen.
//
// Multi-master is enabled without etcd on purpose: peer discovery fails and logs, which is
// irrelevant to apply_remote_delta(), and it keeps this a unit test instead of a cluster.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

namespace {

static std::atomic<uint64_t> g_dir_counter{0};
static std::atomic<uint16_t> g_port{54900};

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

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;

ob::MultiMasterConfig mm_config(uint16_t node_id) {
    ob::MultiMasterConfig mm{};
    mm.enabled                   = true;
    mm.node_id                   = node_id;
    mm.replication_port          = g_port.fetch_add(1, std::memory_order_relaxed);
    mm.compress                  = false;
    mm.max_catchup_bytes         = 1 << 20;
    mm.anti_entropy_interval_sec = 3600;   // out of the way: this test is not about the timer
    return mm;
}

ob::DeltaUpdate remote_delta(const char* symbol, uint64_t seq, uint64_t ts) {
    ob::DeltaUpdate d{};
    std::strncpy(d.symbol, symbol, sizeof(d.symbol) - 1);
    std::strncpy(d.exchange, "EX", sizeof(d.exchange) - 1);
    d.sequence_number = seq;
    d.timestamp_ns    = ts;
    d.side            = ob::SIDE_BID;
    d.n_levels        = 1;
    return d;
}

ob::HLCTimestamp hlc_for(const ob::DeltaUpdate& d, uint16_t origin) {
    ob::HLCTimestamp h{};
    h.physical_ns = d.timestamp_ns;
    h.logical     = 0;
    h.node_id     = origin;
    return h;
}

int row_count(ob::Engine& engine, const char* symbol) {
    int rows = 0;
    const std::string sql = std::string("SELECT * FROM '") + symbol +
                            "'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    const std::string err = engine.execute(sql, [&rows](const ob::QueryResult&) { ++rows; });
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) {
        ADD_FAILURE() << "query error: " << err;
    }
    return rows;
}

}  // namespace

TEST(MultiMasterDedup, TheSameRemoteRecordTwiceStoresOneRow) {
    TempDir tmp("mm_dedup_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY, {}, {}, {}, {},
                      mm_config(1));
    engine.open();

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    auto delta = remote_delta("DEDUP", /*seq=*/5, 1'000'000'000ULL);
    const auto hlc = hlc_for(delta, /*origin=*/2);

    EXPECT_EQ(engine.apply_remote_delta(delta, &level, 2, hlc), ob::OB_OK);
    EXPECT_EQ(engine.apply_remote_delta(delta, &level, 2, hlc), ob::OB_OK)
        << "a duplicate must be accepted and ignored, not refused: catch-up sending it again "
           "is correct behaviour, not an error";
    engine.flush_incremental();

    EXPECT_EQ(row_count(engine, "DEDUP"), 1)
        << "the record was applied twice, so append-only storage now holds it twice";
    engine.close();
}

TEST(MultiMasterDedup, DifferentSequenceNumbersFromOneOriginAreBothStored) {
    TempDir tmp("mm_dedup_two_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY, {}, {}, {}, {},
                      mm_config(1));
    engine.open();

    ob::Level level{};
    level.price = 200'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    for (uint64_t seq : {1ULL, 2ULL}) {
        auto delta = remote_delta("DEDUP2", seq, 2'000'000'000ULL + seq);
        level.price = static_cast<int64_t>(200'000 + seq);
        EXPECT_EQ(engine.apply_remote_delta(delta, &level, 3, hlc_for(delta, 3)), ob::OB_OK);
    }
    engine.flush_incremental();

    EXPECT_EQ(row_count(engine, "DEDUP2"), 2)
        << "dedup went too far and dropped a record that had not been seen";
    engine.close();
}

TEST(MultiMasterDedup, TheSameNumberFromTwoOriginsIsTwoDifferentRecords) {
    TempDir tmp("mm_dedup_origins_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY, {}, {}, {}, {},
                      mm_config(1));
    engine.open();

    ob::Level level{};
    level.qty  = 5;
    level.cnt  = 1;
    level._pad = 0;

    // Sequence numbers are minted per origin, so origin 2's number 9 and origin 3's number 9
    // are unrelated records. Deduplicating on the number alone would silently drop one.
    for (uint16_t origin : {uint16_t{2}, uint16_t{3}}) {
        auto delta = remote_delta("DEDUP3", /*seq=*/9, 3'000'000'000ULL + origin);
        level.price = 300'000 + origin;
        EXPECT_EQ(engine.apply_remote_delta(delta, &level, origin, hlc_for(delta, origin)),
                  ob::OB_OK);
    }
    engine.flush_incremental();

    EXPECT_EQ(row_count(engine, "DEDUP3"), 2)
        << "one of the two origins' records was dropped as a duplicate of the other";
    engine.close();
}

TEST(MultiMasterDedup, ARemoteRecordWithNoNumberIsNeverTreatedAsADuplicate) {
    TempDir tmp("mm_dedup_zero_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY, {}, {}, {}, {},
                      mm_config(1));
    engine.open();

    ob::Level level{};
    level.qty  = 5;
    level.cnt  = 1;
    level._pad = 0;

    // A peer running a build from before #64 sends 0. Two such records are two writes, and
    // reading 0 as "already seen" would drop every one of them.
    for (int i = 0; i < 2; ++i) {
        auto delta = remote_delta("DEDUP0", /*seq=*/0, 4'000'000'000ULL + static_cast<uint64_t>(i));
        level.price = 400'000 + i;
        EXPECT_EQ(engine.apply_remote_delta(delta, &level, 4, hlc_for(delta, 4)), ob::OB_OK);
    }
    engine.flush_incremental();

    EXPECT_EQ(row_count(engine, "DEDUP0"), 2)
        << "records from a node that does not assign sequence numbers were dropped as "
           "duplicates of each other";
    engine.close();
}

TEST(MultiMasterDedup, ARedeliveryAfterARestartIsStillDroppedWhenLwwCannotHelp) {
    TempDir tmp("mm_dedup_restart_");

    ob::Level level{};
    level.price = 500'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    // Sequence 1, not an arbitrary number: the frontier means "everything up to here", so it
    // can only leave zero when the stream is followed from its start. A node that joined an
    // origin's stream in the middle keeps a frontier of 0 for it, exports nothing for it, and
    // relies on the held set above the frontier — which is capped and not persisted. Closing
    // that case needs a base established by snapshot bootstrap, and it is roadmap #67.
    auto delta = remote_delta("DEDUPR", /*seq=*/1, 5'000'000'000ULL);
    const auto hlc = hlc_for(delta, /*origin=*/2);

    {
        ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY, {}, {}, {}, {},
                          mm_config(1));
        engine.open();
        ASSERT_EQ(engine.apply_remote_delta(delta, &level, 2, hlc), ob::OB_OK);
        engine.flush_incremental();
        ASSERT_EQ(row_count(engine, "DEDUPR"), 1);
        engine.close();
    }

    // The same record again after a restart. Within one process, Last-Writer-Wins already
    // refuses this: the HLC is not newer than what the conflict resolver holds for that price.
    // But the resolver's state is in memory, so after a restart it holds nothing and accepts
    // the record — which is how over-delivery turned into duplicate rows before this check
    // existed. The sequence number is the only thing that survives the restart and can say
    // "already applied".
    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY, {}, {}, {}, {},
                        mm_config(1));
    reopened.open();
    EXPECT_EQ(reopened.apply_remote_delta(delta, &level, 2, hlc), ob::OB_OK);

    // A genuinely new record after it, with a later timestamp. Without this the duplicate is
    // invisible for a reason that has nothing to do with dedup: the re-flushed segment would
    // land on the same directory name (start and end timestamp unchanged), and ColumnarStore
    // refuses to merge a path already in the index. That refusal is #62's backstop, and it
    // masked this mutation until the segment was made to differ.
    auto next = remote_delta("DEDUPR", /*seq=*/2, 5'000'001'000ULL);
    ob::Level next_level{};
    next_level.price = 500'001;
    next_level.qty   = 5;
    next_level.cnt   = 1;
    next_level._pad  = 0;
    EXPECT_EQ(reopened.apply_remote_delta(next, &next_level, 2, hlc_for(next, 2)), ob::OB_OK);
    reopened.flush_incremental();

    EXPECT_EQ(row_count(reopened, "DEDUPR"), 2)
        << "expected the original row plus one new one; a third means the redelivered record "
           "was applied again after the restart — Last-Writer-Wins cannot catch it, because "
           "its HLC state does not survive a restart";
    reopened.close();
}
