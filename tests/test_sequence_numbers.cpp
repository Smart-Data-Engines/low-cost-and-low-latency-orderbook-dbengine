// Feature: wal-sequence-numbers — the engine assigns them, and nothing else renumbers
//
// tcp_server.cpp sets delta.sequence_number = 0 and says "engine handles sequencing".
// It did not: the value was copied into the WAL header and the stored row unchanged, so
// every production write carried 0, gap detection could never fire (it needs a non-zero
// previous number), and the GAP record type had never been produced by a running server.
//
// These tests read the numbers back out of the WAL rather than trusting a getter, because
// the WAL is what replication, catch-up and recovery actually consume.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

static std::atomic<uint64_t> g_dir_counter{0};

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

/// One hour, so the background flush never intervenes in the middle of a measurement.
constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;

ob::DeltaUpdate make_delta(const char* symbol, const char* exchange, uint64_t ts,
                           uint64_t seq = 0) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, exchange, sizeof(delta.exchange) - 1);
    delta.sequence_number = seq;
    delta.timestamp_ns    = ts;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = 1;
    return delta;
}

/// Sequence numbers of every DELTA record in the WAL, in file order.
std::vector<uint64_t> wal_sequences(const std::string& dir,
                                    const std::string& symbol = "") {
    std::vector<uint64_t> out;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_DELTA) return;
        if (ctx.payload_len < sizeof(ob::DeltaUpdate)) return;
        ob::DeltaUpdate delta{};
        std::memcpy(&delta, ctx.payload, sizeof(ob::DeltaUpdate));
        if (!symbol.empty() && symbol != delta.symbol) return;
        out.push_back(ctx.header.sequence_number);
    });
    return out;
}

/// Record types present in the WAL, in file order — to see GAP records appear.
std::vector<uint8_t> wal_record_types(const std::string& dir) {
    std::vector<uint8_t> out;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        out.push_back(ctx.header.record_type);
    });
    return out;
}

int count_type(const std::vector<uint8_t>& types, uint8_t type) {
    int n = 0;
    for (uint8_t t : types) {
        if (t == type) ++n;
    }
    return n;
}

}  // namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Assignment
// ═══════════════════════════════════════════════════════════════════════════════

TEST(SequenceNumbers, LocalWritesAreNumberedConsecutively) {
    TempDir tmp("seq_local_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    engine.open();

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    for (int i = 0; i < 3; ++i) {
        auto delta = make_delta("SEQ", "EX", 1'000'000'000ULL + static_cast<uint64_t>(i));
        ASSERT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);
    }
    engine.close();

    const auto seqs = wal_sequences(tmp.path, "SEQ");
    ASSERT_EQ(seqs.size(), 3u);
    EXPECT_EQ(seqs[0], 1u) << "the first write of a symbol must be numbered 1, not 0: 0 is "
                              "reserved for 'nobody assigned one'";
    EXPECT_EQ(seqs[1], 2u);
    EXPECT_EQ(seqs[2], 3u);
}

TEST(SequenceNumbers, TwoSymbolsAreNumberedIndependently) {
    TempDir tmp("seq_two_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    engine.open();

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    // Interleaved, because a single global counter would still pass a sequential test.
    for (int i = 0; i < 3; ++i) {
        auto a = make_delta("SEQ-A", "EX", 2'000'000'000ULL + static_cast<uint64_t>(i) * 2);
        auto b = make_delta("SEQ-B", "EX", 2'000'000'001ULL + static_cast<uint64_t>(i) * 2);
        ASSERT_EQ(engine.apply_delta(a, &level), ob::OB_OK);
        ASSERT_EQ(engine.apply_delta(b, &level), ob::OB_OK);
    }
    engine.close();

    const std::vector<uint64_t> expected{1, 2, 3};
    EXPECT_EQ(wal_sequences(tmp.path, "SEQ-A"), expected)
        << "interleaving with another symbol left holes in this symbol's stream";
    EXPECT_EQ(wal_sequences(tmp.path, "SEQ-B"), expected);
}

TEST(SequenceNumbers, OneDeltaWithManyLevelsGetsOneNumber) {
    TempDir tmp("seq_minsert_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    engine.open();

    constexpr uint16_t kLevels = 100;
    std::vector<ob::Level> levels(kLevels);
    for (uint16_t i = 0; i < kLevels; ++i) {
        levels[i].price = 300'000 + i;
        levels[i].qty   = 3;
        levels[i].cnt   = 1;
        levels[i]._pad  = 0;
    }

    auto delta = make_delta("SEQ-BATCH", "EX", 3'000'000'000ULL);
    delta.n_levels = kLevels;
    ASSERT_EQ(engine.apply_delta(delta, levels.data()), ob::OB_OK);

    auto second = make_delta("SEQ-BATCH", "EX", 3'000'000'001ULL);
    second.n_levels = kLevels;
    ASSERT_EQ(engine.apply_delta(second, levels.data()), ob::OB_OK);
    engine.close();

    const std::vector<uint64_t> expected{1, 2};
    EXPECT_EQ(wal_sequences(tmp.path, "SEQ-BATCH"), expected)
        << "a MINSERT of 100 levels is one update and must consume one number, not 100";
}

TEST(SequenceNumbers, ACallerSuppliedNumberIsNotOverwritten) {
    TempDir tmp("seq_supplied_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    engine.open();

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    // This is the replica path: records streamed from a primary already carry its numbers
    // and arrive through the same apply_delta() a client write uses. Renumbering them
    // would make the replica's data disagree with the primary it is copying.
    auto delta = make_delta("SEQ-REPL", "EX", 4'000'000'000ULL, /*seq=*/7);
    ASSERT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);
    engine.close();

    const auto seqs = wal_sequences(tmp.path, "SEQ-REPL");
    ASSERT_EQ(seqs.size(), 1u);
    EXPECT_EQ(seqs[0], 7u) << "a non-zero number came from whoever originated the record "
                              "and must survive being stored here";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Durability of the counters
// ═══════════════════════════════════════════════════════════════════════════════

TEST(SequenceNumbers, NumbersKeepRisingAcrossARestart) {
    TempDir tmp("seq_restart_");

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    {
        ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
        engine.open();
        for (int i = 0; i < 2; ++i) {
            auto delta = make_delta("SEQ-RESTART", "EX",
                                    5'000'000'000ULL + static_cast<uint64_t>(i));
            ASSERT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);
        }
        engine.close();      // flushes, so the numbers live in a segment
    }

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();
    auto delta = make_delta("SEQ-RESTART", "EX", 5'000'000'010ULL);
    ASSERT_EQ(reopened.apply_delta(delta, &level), ob::OB_OK);
    reopened.close();

    const auto seqs = wal_sequences(tmp.path, "SEQ-RESTART");
    ASSERT_GE(seqs.size(), 3u);
    EXPECT_EQ(seqs.back(), 3u)
        << "the write after the restart reused a number: the counter was not restored from "
           "what is already durable, so two rows now claim the same position in the stream";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Gap detection, which has never once fired in this engine
// ═══════════════════════════════════════════════════════════════════════════════

TEST(SequenceNumbers, AHoleInOneOriginsStreamIsRecordedAsAGap) {
    TempDir tmp("seq_gap_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    engine.open();

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    // Explicit numbers with 2 missing: this is what a replica sees when the stream drops a
    // record, and what append_gap() exists to record.
    for (uint64_t seq : {1ULL, 3ULL}) {
        auto delta = make_delta("SEQ-GAP", "EX", 6'000'000'000ULL + seq, seq);
        ASSERT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);
    }
    engine.close();

    EXPECT_EQ(count_type(wal_record_types(tmp.path), ob::WAL_RECORD_GAP), 1)
        << "sequence 3 arrived after 1 and no GAP record was written. The record type has "
           "existed since the first WAL format and has never been produced by the engine";
}

TEST(SequenceNumbers, ARemoteDeltaWithoutMultiMasterIsRefusedNotFatal) {
    TempDir tmp("seq_remote_guard_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    engine.open();

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    // Only MultiMasterManager calls this, and it exists only in multi-master mode, so this
    // is unreachable through the server. It is still a public method on a library type, and
    // hlc_ and mm_mgr_ are null here: before the guard, this call dumped core.
    ob::HLCTimestamp hlc{};
    hlc.physical_ns = 8'000'000'000ULL;
    hlc.logical     = 0;
    hlc.node_id     = 3;

    auto delta = make_delta("SEQ-NO-MM", "EX", 8'000'000'000ULL, /*seq=*/1);
    EXPECT_EQ(engine.apply_remote_delta(delta, &level, /*origin=*/3, hlc), ob::OB_ERR_INVALID_ARG)
        << "a remote delta on a node without multi-master must be answered with an error; "
           "taking the process down is the worst available response";
    engine.close();

    EXPECT_TRUE(wal_sequences(tmp.path, "SEQ-NO-MM").empty())
        << "the refused record was written to the WAL anyway";
}

// Per-origin gap detection lives in SequenceTracker and is tested in
// test_sequence_tracker.cpp: interleaving two origins here would need a running
// multi-master node, etcd watch and all, to exercise arithmetic on two integers.

TEST(SequenceNumbers, ASegmentFromBeforeNumbersExistedDoesNotDisturbTheCounter) {
    TempDir tmp("seq_old_meta_");

    // An existing deployment's data: a segment whose meta.json predates
    // max_sequence_number, holding rows that were all written with a zero. open_existing()
    // builds its index from meta.json alone, so this is enough to be indexed.
    const std::string dir = tmp.path + "/SEQ-OLD/EX/0_9000000000";
    fs::create_directories(dir);
    {
        std::ofstream f(dir + "/meta.json");
        ASSERT_TRUE(f.is_open());
        f << R"({"format_version":2,"start_ts_ns":0,"end_ts_ns":9000000000,"row_count":4,)"
          << R"("first_price":0,"has_raw_qty":false,"symbol":"SEQ-OLD","exchange":"EX"})";
    }

    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    engine.open();

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    auto delta = make_delta("SEQ-OLD", "EX", 9'500'000'000ULL);
    ASSERT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);
    engine.close();

    const auto seqs = wal_sequences(tmp.path, "SEQ-OLD");
    ASSERT_EQ(seqs.size(), 1u);
    EXPECT_EQ(seqs[0], 1u)
        << "a missing field read as something other than 'no numbers in there' pushed the "
           "counter somewhere arbitrary; the rows in that segment carry 0, so 1 is free";
    EXPECT_EQ(count_type(wal_record_types(tmp.path), ob::WAL_RECORD_GAP), 0)
        << "the first write after an upgrade was reported as a gap";
}

TEST(SequenceNumbers, AGapRecordInTheWalDoesNotDisturbRecovery) {
    TempDir tmp("seq_gap_replay_");

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 5;
    level.cnt   = 1;
    level._pad  = 0;

    {
        // Abandoned without close(), so the WAL tail is replayed on the next open — and that
        // tail now contains a GAP record, which no engine had ever produced before, so this
        // interaction has never happened until both changes existed together.
        auto engine = std::make_unique<ob::Engine>(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
        engine->open();
        for (uint64_t seq : {1ULL, 3ULL}) {
            auto delta = make_delta("SEQ-GAP-REPLAY", "EX", 9'900'000'000ULL + seq, seq);
            ASSERT_EQ(engine->apply_delta(delta, &level), ob::OB_OK);
        }
        ASSERT_EQ(count_type(wal_record_types(tmp.path), ob::WAL_RECORD_GAP), 1);
        engine.release();     // leak deliberately: no close(), no flush, like a crash
    }

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();

    int rows = 0;
    const std::string sql =
        "SELECT * FROM 'SEQ-GAP-REPLAY'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    const std::string err = reopened.execute(sql, [&rows](const ob::QueryResult&) { ++rows; });
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) {
        ADD_FAILURE() << "query error: " << err;
    }

    auto next = make_delta("SEQ-GAP-REPLAY", "EX", 9'900'000'100ULL);
    ASSERT_EQ(reopened.apply_delta(next, &level), ob::OB_OK);
    reopened.close();

    EXPECT_EQ(rows, 2) << "the two written rows did not both come back through a WAL tail that "
                          "also held a GAP record";

    const auto seqs = wal_sequences(tmp.path, "SEQ-GAP-REPLAY");
    ASSERT_FALSE(seqs.empty());
    EXPECT_EQ(seqs.back(), 4u)
        << "the write after recovery was numbered " << seqs.back()
        << ": the replayed tail's highest number was 3, so the counter must continue past it";
    EXPECT_EQ(count_type(wal_record_types(tmp.path), ob::WAL_RECORD_GAP), 1)
        << "replay re-reported a gap that was already recorded when it happened, so every "
           "restart would add another GAP record for the same hole";
}
