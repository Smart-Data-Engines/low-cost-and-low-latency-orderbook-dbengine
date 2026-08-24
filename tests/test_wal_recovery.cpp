// Feature: wal-replay-recovery — writes acknowledged before a crash must come back
//
// Every other test in this repository shuts the engine down with close(), which
// drains and flushes, so the data comes back from columnar segments and the WAL is
// never exercised as a recovery log. These tests deliberately do not close: they
// abandon the engine the way a crash does, and then open the same directory again.
//
// The distinction matters because WAL replay applied nothing at all, and no test
// noticed for as long as the engine existed.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <fstream>
#include <memory>
#include <algorithm>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

static std::atomic<uint64_t> g_dir_counter{0};

std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() /
               (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
    fs::create_directories(tmp);
    return tmp.string();
}

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix) : path(make_temp_dir(prefix)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

/// A long flush interval, so the background thread does not quietly persist the rows
/// and turn a recovery test into a test of the columnar store.
constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;  // 1 hour

/// Cut the WAL at the last CHECKPOINT record, dropping it and anything after it.
///
/// This is the on-disk state a crash between writing segment files and recording that fact leaves
/// behind: the records are in the log at their original positions, their rows are already in a
/// segment, and nothing says so. Re-appending copies of them instead — which an earlier version of
/// this test did — produces a state the engine cannot produce, because a record reaches the WAL
/// before it reaches `pending_rows_`, so a durable row's record is always *below* the position its
/// segment recorded.
///
/// Returns the number of bytes removed.
size_t cut_wal_at_last_checkpoint(const std::string& dir) {
    std::string newest;
    for (const auto& entry : fs::directory_iterator(dir)) {
        const std::string name = entry.path().filename().string();
        if (name.size() == 14 && name.compare(0, 4, "wal_") == 0 &&
            name.compare(10, 4, ".bin") == 0) {
            if (newest.empty() || name > fs::path(newest).filename().string()) {
                newest = entry.path().string();
            }
        }
    }
    if (newest.empty()) return 0;

    std::vector<uint8_t> bytes;
    {
        std::ifstream in(newest, std::ios::binary);
        bytes.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }

    // Records are a 24-byte header plus payload_len bytes; walk them and remember the last
    // checkpoint's own offset.
    size_t off = 0;
    size_t last_checkpoint = std::string::npos;
    while (off + sizeof(ob::WALRecord) <= bytes.size()) {
        ob::WALRecord hdr{};
        std::memcpy(&hdr, bytes.data() + off, sizeof(hdr));
        if (hdr.record_type == ob::WAL_RECORD_CHECKPOINT) last_checkpoint = off;
        const size_t total = sizeof(ob::WALRecord) + hdr.payload_len;
        if (total == 0 || off + total > bytes.size()) break;
        off += total;
    }
    if (last_checkpoint == std::string::npos) return 0;

    const size_t removed = bytes.size() - last_checkpoint;
    fs::resize_file(newest, last_checkpoint);
    return removed;
}

void insert_rows(ob::Engine& engine, int count, uint64_t ts_base,
                 int64_t price_base, const char* symbol = "CRASH",
                 const char* exchange = "EX") {
    for (int i = 0; i < count; ++i) {
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, exchange, sizeof(delta.exchange) - 1);
        delta.sequence_number = static_cast<uint64_t>(i + 1);
        delta.timestamp_ns    = ts_base + static_cast<uint64_t>(i) * 1'000'000ULL;
        delta.side            = ob::SIDE_BID;
        delta.n_levels        = 1;

        ob::Level level{};
        level.price = price_base + i;
        level.qty   = 100 + static_cast<uint64_t>(i);
        level.cnt   = 1;
        level._pad  = 0;

        ASSERT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);
    }
}

int query_count(ob::Engine& engine, const char* symbol = "CRASH",
                const char* exchange = "EX") {
    int count = 0;
    std::string sql = std::string("SELECT * FROM '") + symbol + "'.'" + exchange +
                      "' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    std::string err = engine.execute(sql, [&](const ob::QueryResult&) { ++count; });
    // A symbol with neither segments nor a live buffer is reported as NOT_FOUND,
    // which for counting purposes is zero rows rather than a failure. Anything else
    // is a real error and should be seen.
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) {
        ADD_FAILURE() << "query error: " << err;
    }
    return count;
}

/// Count the segment directories under a data dir — how much is actually durable.
size_t segment_count_on_disk(const std::string& dir) {
    size_t count = 0;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(dir, ec);
         it != fs::recursive_directory_iterator(); ++it) {
        if (it->is_regular_file(ec) && it->path().filename() == "meta.json") ++count;
    }
    return count;
}

} // anonymous namespace

// ═══════════════════════════════════════════════════════════════════════════════
// The core promise: an acknowledged write survives a crash.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(WalRecovery, RowsSurviveAnAbandonedEngine) {
    TempDir tmp("wal_recovery_");

    {
        // No close(): the engine is abandoned, which is what a crash looks like to
        // the next process to open this directory.
        auto engine = std::make_unique<ob::Engine>(tmp.path, kNoAutoFlush,
                                                   ob::FsyncPolicy::EVERY);
        engine->open();
        insert_rows(*engine, 5, 1'000'000'000ULL, 10'000);

        ASSERT_EQ(segment_count_on_disk(tmp.path), 0u)
            << "rows reached a segment before the crash, so this test would pass "
               "without the WAL being read at all";

        engine.release();  // deliberately leaked: no destructor, no close(), no flush
    }

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();
    const int recovered = query_count(reopened);
    reopened.close();

    EXPECT_EQ(recovered, 5)
        << "five acknowledged writes were in the WAL and " << recovered
        << " came back; the WAL is written, fsynced and never replayed";
}

TEST(WalRecovery, RecoveredRowsKeepTheirValues) {
    TempDir tmp("wal_recovery_values_");

    {
        auto engine = std::make_unique<ob::Engine>(tmp.path, kNoAutoFlush,
                                                   ob::FsyncPolicy::EVERY);
        engine->open();
        insert_rows(*engine, 3, 2'000'000'000ULL, 50'000);
        engine.release();
    }

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();

    std::vector<int64_t> prices;
    std::vector<uint64_t> qtys;
    std::string err = reopened.execute(
        "SELECT * FROM 'CRASH'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult& r) {
            prices.push_back(r.price);
            qtys.push_back(r.quantity);
        });
    reopened.close();

    ASSERT_TRUE(err.empty()) << err;
    std::sort(prices.begin(), prices.end());
    std::sort(qtys.begin(), qtys.end());
    EXPECT_EQ(prices, (std::vector<int64_t>{50'000, 50'001, 50'002}));
    EXPECT_EQ(qtys, (std::vector<uint64_t>{100, 101, 102}));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Recovery must not double anything. The WAL is truncated only up to what replicas
// confirmed, not up to what was flushed, so after a restart it normally contains
// records that are already in segments.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(WalRecovery, FlushedRowsAreNotReplayedTwice) {
    TempDir tmp("wal_recovery_nodup_");

    {
        auto engine = std::make_unique<ob::Engine>(tmp.path, kNoAutoFlush,
                                                   ob::FsyncPolicy::EVERY);
        engine->open();
        insert_rows(*engine, 4, 3'000'000'000ULL, 70'000);
        engine->flush_incremental();     // first four are now durable in a segment

        ASSERT_GT(segment_count_on_disk(tmp.path), 0u) << "flush wrote no segment";

        // Four more, not flushed: only these should come back from the WAL.
        insert_rows(*engine, 4, 3'100'000'000ULL, 80'000);
        engine.release();
    }

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();
    const int total = query_count(reopened);
    reopened.close();

    EXPECT_EQ(total, 8)
        << "expected the four flushed rows plus the four recovered ones; " << total
        << " means the flushed rows were replayed on top of the segment that already "
           "held them";
}

TEST(WalRecovery, RestartingTwiceDoesNotMultiplyRows) {
    TempDir tmp("wal_recovery_twice_");

    {
        auto engine = std::make_unique<ob::Engine>(tmp.path, kNoAutoFlush,
                                                   ob::FsyncPolicy::EVERY);
        engine->open();
        insert_rows(*engine, 3, 4'000'000'000ULL, 90'000);
        engine.release();
    }

    int counts[2] = {0, 0};
    for (int attempt = 0; attempt < 2; ++attempt) {
        ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
        engine.open();
        counts[attempt] = query_count(engine);
        engine.close();   // flushes, so the second attempt reads segments + WAL
    }

    EXPECT_EQ(counts[0], 3);
    EXPECT_EQ(counts[1], 3)
        << "the second open saw " << counts[1]
        << " rows: recovery is not idempotent across restarts";
}

TEST(WalRecovery, CleanCloseLeavesNothingToReplay) {
    TempDir tmp("wal_recovery_clean_");

    {
        // close() drains and flushes, so every row is durable in a segment and the
        // WAL tail holds nothing that still needs applying.
        ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
        engine.open();
        insert_rows(engine, 3, 5'000'000'000ULL, 110'000);
        engine.close();
    }

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();
    const int count = query_count(reopened);
    reopened.close();

    EXPECT_EQ(count, 3)
        << "after a clean shutdown the rows are in segments; " << count
        << " means replay applied them again on top";
}

// ═══════════════════════════════════════════════════════════════════════════════
// A checkpoint must never precede the durability it claims.
//
// The timestamp guard in replay_wal_tail() hides the ordering in the happy path: with
// the checkpoint written before the flush, the tests above still pass, because nothing
// was lost — the guard only ever *skips* records. The failure that ordering protects
// against is a flush that does not complete, and that needs constructing.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(WalRecovery, AFailedFlushDoesNotClaimDurability) {
    TempDir tmp("wal_recovery_failed_flush_");

    {
        auto engine = std::make_unique<ob::Engine>(tmp.path, kNoAutoFlush,
                                                   ob::FsyncPolicy::EVERY);
        engine->open();
        insert_rows(*engine, 4, 6'000'000'000ULL, 120'000);

        // Make the segment write fail: the store cannot create its column files under
        // a directory it may not write to.
        std::error_code ec;
        fs::permissions(tmp.path, fs::perms::owner_read | fs::perms::owner_exec,
                        fs::perm_options::replace, ec);
        ASSERT_FALSE(ec) << "could not make the data dir read-only: " << ec.message();

        bool threw = false;
        try {
            engine->flush_incremental();
        } catch (const std::exception&) {
            threw = true;
        }

        // Restore permissions so the next open() can work.
        fs::permissions(tmp.path,
                        fs::perms::owner_read | fs::perms::owner_write |
                            fs::perms::owner_exec,
                        fs::perm_options::replace, ec);

        ASSERT_TRUE(threw)
            << "the flush was expected to fail with the directory read-only; without a "
               "failed flush this test cannot check what a checkpoint claims";

        ASSERT_EQ(segment_count_on_disk(tmp.path), 0u)
            << "a segment was written despite the read-only directory";

        engine.release();   // crash with the flush having failed
    }

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();
    const int recovered = query_count(reopened, "CRASH", "EX");
    reopened.close();

    EXPECT_EQ(recovered, 4)
        << "the flush failed, so nothing was durable, and the WAL still held all four "
           "records — " << recovered << " came back. A checkpoint appended before the "
           "flush would have told recovery to skip them";
}

// ═══════════════════════════════════════════════════════════════════════════════
// The window the checkpoint cannot cover: a crash after the segments were written
// and before the checkpoint recorded that fact.
//
// The on-disk state that leaves behind is WAL records whose rows are already in a
// segment, with no checkpoint after them. Constructed directly here, because no
// ordinary sequence of engine calls produces it — and without a test the timestamp
// guard that handles it is code nobody checks.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(WalRecovery, RecordsAlreadyCoveredByASegmentAreNotReplayed) {
    // The crash window no checkpoint can describe: segment files written, checkpoint not yet
    // appended. Built by cutting the log at the last checkpoint, so the records sit where the
    // engine put them and their rows are already durable — which is what makes the answer a
    // position comparison rather than a guess about timestamps (#63).
    //
    // An earlier version of this test re-appended copies of those records instead. That produces a
    // state the engine cannot produce: a record reaches the WAL before it reaches pending_rows_,
    // so a durable row's record is always *below* the position its segment recorded.
    TempDir tmp("wal_recovery_window_");

    constexpr uint64_t kFirst  = 7'000'000'000ULL;
    constexpr uint64_t kSecond = 8'000'000'000ULL;

    {
        ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
        engine.open();
        insert_rows(engine, 4, kFirst, 130'000);
        engine.flush_incremental();                  // segment + checkpoint
        insert_rows(engine, 4, kSecond, 140'000);
        engine.flush_incremental();                  // second segment + checkpoint
        engine.close();
    }

    ASSERT_GT(cut_wal_at_last_checkpoint(tmp.path), 0u)
        << "nothing was cut, so this test is not exercising the crash window";

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();
    const int after_recovery = query_count(reopened);
    reopened.close();

    // A second, clean restart: a re-flushed segment's merge is refused, but the refusal comes
    // after its files were rewritten in place, so damage from replaying over a durable segment
    // only surfaces one restart later.
    ob::Engine again(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    again.open();
    const int after_restart = query_count(again);
    again.close();

    EXPECT_EQ(after_recovery, 8)
        << after_recovery << " rows after recovery: records a segment already covers were "
           "replayed on top of it, rewriting a durable segment from whatever the WAL tail held";
    EXPECT_EQ(after_restart, 8)
        << after_restart << " of 8 rows after a further clean restart: replaying over a durable "
           "segment rewrote it with fewer rows than it held, which is data loss, not duplication";
}

TEST(WalRecovery, ARecordWithAnOutOfOrderTimestampIsNotMistakenForOneAlreadyStored) {
    // The defect #63 named. The old guard skipped a replayed record whose timestamp was at or
    // below the highest `end_ts_ns` among that symbol's segments. `end_ts_ns` is the *last* row's
    // timestamp, not the highest one, so the comparison is exact only while timestamps for a
    // symbol increase — true on one node, false in multi-master, where a peer's record carries the
    // origin's clock and is appended locally after whatever arrived in the meantime.
    //
    // Here the record is genuinely new and its timestamp falls inside the flushed segment's range.
    // Skipping it loses a row; comparing positions applies it, because it sits above the position
    // that segment recorded.
    TempDir tmp("wal_recovery_ooo_");

    {
        ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
        engine.open();
        insert_rows(engine, 3, 5'000'000'000ULL, 150'000);
        engine.flush_incremental();
        engine.close();
    }

    // Appended straight to the log, the way a peer's record arrives: after everything durable, but
    // carrying a timestamp from inside the segment's range.
    {
        ob::WALWriter writer(tmp.path, 512ULL << 20, ob::FsyncPolicy::EVERY);
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, "CRASH", sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
        delta.sequence_number = 99;
        delta.timestamp_ns    = 5'000'000'500ULL;    // inside the segment's [start, end]
        delta.side            = ob::SIDE_BID;
        delta.n_levels        = 1;

        ob::Level level{};
        level.price = 999'000;
        level.qty   = 42;
        level.cnt   = 1;
        level._pad  = 0;
        writer.append(delta, &level);
        writer.flush();
    }

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();
    const int rows = query_count(reopened);
    reopened.close();

    EXPECT_EQ(rows, 4)
        << rows << " rows: the out-of-order record was dropped as already stored, which is the "
           "lost row #63 described — its timestamp falls inside a segment's range, but that "
           "segment does not contain it";
}

TEST(WalRecovery, APositionFromAnotherNodesWalIsNotUsedToSkipAnything) {
    // A snapshot and a shard migration ship whole segment directories, meta.json included, so a
    // received segment carries the *sender's* WAL position. Believing it would skip records this
    // node holds and never stored — the expensive direction. Segments therefore record which WAL
    // the position belongs to, and recovery compares positions only when that matches.
    //
    // Simulated by rewriting a segment's meta.json with a foreign identity and an implausibly large
    // position, then appending a record that sits far below it.
    TempDir tmp("wal_recovery_foreign_");

    {
        ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
        engine.open();
        insert_rows(engine, 3, 5'000'000'000ULL, 160'000);
        engine.flush_incremental();
        engine.close();
    }

    // Find the segment's meta.json and give it someone else's WAL.
    std::string meta_path;
    for (auto& entry : fs::recursive_directory_iterator(tmp.path)) {
        if (entry.path().filename() == "meta.json") meta_path = entry.path().string();
    }
    ASSERT_FALSE(meta_path.empty()) << "no segment was written, so there is nothing to test";
    {
        std::string json;
        {
            std::ifstream in(meta_path);
            json.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
        const auto replace_field = [&json](const std::string& key, const std::string& value) {
            const std::string needle = "\"" + key + "\":";
            const auto at = json.find(needle);
            ASSERT_NE(at, std::string::npos) << "meta.json has no " << key;
            const auto start = at + needle.size();
            const auto end = json.find_first_of(",}", start);
            json.replace(start, end - start, value);
        };
        replace_field("wal_identity", "424242");          // not this node's
        replace_field("wal_file_index", "9999");          // far beyond anything local
        replace_field("wal_byte_offset", "999999999");
        std::ofstream out(meta_path, std::ios::trunc);
        out << json;
    }

    // A genuinely new record, well above the segment's timestamps so the fallback cannot skip it,
    // and at a WAL position far below the foreign one.
    {
        ob::WALWriter writer(tmp.path, 512ULL << 20, ob::FsyncPolicy::EVERY);
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, "CRASH", sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
        delta.sequence_number = 77;
        delta.timestamp_ns    = 9'000'000'000ULL;
        delta.side            = ob::SIDE_BID;
        delta.n_levels        = 1;

        ob::Level level{};
        level.price = 777'000;
        level.qty   = 7;
        level.cnt   = 1;
        level._pad  = 0;
        writer.append(delta, &level);
        writer.flush();
    }

    ob::Engine reopened(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY);
    reopened.open();
    const int rows = query_count(reopened);
    reopened.close();

    EXPECT_EQ(rows, 4)
        << rows << " rows: a position written against another node's WAL was used to decide this "
           "one's recovery, and a record that was never stored here was dropped";
}
