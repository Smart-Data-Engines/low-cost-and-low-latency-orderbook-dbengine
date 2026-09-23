// Stage 2b of using the whole machine (#155): the writes of one read are applied under one
// acquisition of the engine's lock, with their WAL records written by one write() per run.
//
// The claim that makes that safe is that nothing anyone can read differs from the same writes
// applied one call at a time: the WAL records, the numbers they carry, the answers, the book. So
// the behavioural tests here apply the same writes both ways, into two engines, and compare what
// each engine holds afterwards - read back out of the WAL and the live book rather than through a
// getter, because those are what replication, recovery and a client consume.
//
// What they cannot see is the one thing the stage changes - how many times the lock is taken -
// because one acquisition and sixty-four produce the same state. That is what the static test at
// the end of this file is for.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"
#include "test_ports.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <tuple>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};
std::atomic<uint16_t> g_port{ob::test::kPortsWriteBatch};

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

/// One hour, so the background flush never moves rows while a comparison is being made.
constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;

/// One write: its update and the levels it points at.
struct Write {
    ob::DeltaUpdate        update{};
    std::vector<ob::Level> levels;
};

Write make_write(const char* symbol, uint8_t side, uint16_t width, uint64_t ts, uint64_t seq = 0) {
    Write w;
    std::strncpy(w.update.symbol, symbol, sizeof(w.update.symbol) - 1);
    std::strncpy(w.update.exchange, "EX", sizeof(w.update.exchange) - 1);
    w.update.sequence_number = seq;
    w.update.timestamp_ns    = ts;
    w.update.side            = side;
    w.update.n_levels        = width;
    for (uint16_t i = 0; i < width; ++i) {
        ob::Level level{};
        level.price = static_cast<int64_t>(100'000 + (side == ob::SIDE_BID ? -1 : 1) * (i + 1) * 10 +
                                           static_cast<int64_t>(ts % 7));
        level.qty   = 10 + i + ts % 5;
        level.cnt   = 1 + i % 3;
        w.levels.push_back(level);
    }
    return w;
}

/// Writes of the sizes a client sends - one level, a MINSERT's twenty, a full thousand - on three
/// symbols and both sides, and on a third symbol two writes numbered by the caller with a hole
/// between them, so the second is a GAP against its stream and the batch has to put the GAP record
/// in front of it.
std::vector<Write> mixed_writes() {
    std::vector<Write> writes;
    const uint16_t widths[] = {1, 20, 1, 1000, 3, 20, 1};
    uint64_t ts = 1'700'000'000'000'000'000ULL;
    for (int round = 0; round < 3; ++round) {
        for (const uint16_t width : widths) {
            ++ts;
            writes.push_back(make_write(round % 2 ? "BBB" : "AAA",
                                        (ts % 2) ? ob::SIDE_BID : ob::SIDE_ASK, width, ts));
        }
    }
    writes.push_back(make_write("CCC", ob::SIDE_BID, 2, ++ts, /*seq=*/5));
    writes.push_back(make_write("CCC", ob::SIDE_BID, 2, ++ts, /*seq=*/9));
    writes.push_back(make_write("AAA", ob::SIDE_BID, 1, ++ts));
    return writes;
}

std::vector<ob::ClientWrite> as_client_writes(const std::vector<Write>& writes) {
    std::vector<ob::ClientWrite> out;
    for (const Write& w : writes) out.push_back(ob::ClientWrite{w.update, w.levels.data()});
    return out;
}

/// Every record of the WAL, in file order, as (type, sequence, time, origin, payload): the bytes a
/// replay, a replica and a catch-up read. Positions are left out on purpose - they are compared by
/// the WAL writer's own test, and here they would only restate the payload sizes.
///
/// One field is blanked, and only one: the time of a CHECKPOINT, which `close()` writes from the
/// wall clock. Two engines closed a few milliseconds apart differ there and nowhere else - the
/// first run of this test found exactly that, as record 25 of 26 - and dropping the record instead
/// would have dropped the check that both engines wrote one.
using Record = std::tuple<uint8_t, uint64_t, uint64_t, uint16_t, std::string>;

std::vector<Record> wal_records(const std::string& dir) {
    std::vector<Record> out;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        const bool clock = ctx.header.record_type == ob::WAL_RECORD_CHECKPOINT;
        out.emplace_back(ctx.header.record_type, ctx.header.sequence_number,
                         clock ? uint64_t{0} : ctx.header.timestamp_ns, ctx.origin_node_id,
                         std::string(reinterpret_cast<const char*>(ctx.payload), ctx.payload_len));
    });
    return out;
}

/// The live book of one symbol, both sides, as the rows `BOOK` would send.
std::vector<std::string> book(ob::Engine& engine, const std::string& symbol) {
    std::vector<std::string> rows;
    const std::string err =
        engine.read_book(symbol, "EX", 2000, [&](const ob::QueryResult& r) {
            rows.push_back(std::to_string(r.side) + ":" + std::to_string(r.level) + ":" +
                           std::to_string(r.price) + ":" + std::to_string(r.quantity) + ":" +
                           std::to_string(r.order_count) + ":" +
                           std::to_string(r.sequence_number));
        });
    if (!err.empty()) rows.push_back("error: " + err);
    return rows;
}

ob::MultiMasterConfig mm_config(uint16_t node_id) {
    ob::MultiMasterConfig mm{};
    mm.enabled                   = true;
    mm.node_id                   = node_id;
    mm.replication_port          = g_port.fetch_add(1, std::memory_order_relaxed);
    mm.compress                  = false;
    mm.max_catchup_bytes         = 1 << 20;
    mm.anti_entropy_interval_sec = 3600;
    return mm;
}

size_t count_type(const std::vector<Record>& records, uint8_t type) {
    size_t n = 0;
    for (const Record& r : records) n += std::get<0>(r) == type ? 1 : 0;
    return n;
}

} // namespace

TEST(WriteBatch, WhatABatchWritesIsWhatTheSameWritesOneAtATimeWrite) {
    const std::vector<Write> writes = mixed_writes();

    TempDir one_dir("wb_single_");
    std::vector<ob::ob_status_t> single_status;
    std::vector<std::string> single_books;
    size_t single_pending = 0;
    {
        ob::Engine engine(one_dir.path, kNoAutoFlush);
        engine.open();
        for (const Write& w : writes) single_status.push_back(engine.apply_delta(w.update, w.levels.data()));
        for (const char* symbol : {"AAA", "BBB", "CCC"}) {
            for (const std::string& row : book(engine, symbol)) single_books.push_back(row);
        }
        single_pending = engine.stats().pending_rows;
        engine.close();
    }

    TempDir batch_dir("wb_batch_");
    std::vector<ob::WriteOutcome> outcomes(writes.size());
    std::vector<std::string> batch_books;
    size_t batch_pending = 0;
    {
        ob::Engine engine(batch_dir.path, kNoAutoFlush);
        engine.open();
        const std::vector<ob::ClientWrite> batch = as_client_writes(writes);
        engine.apply_deltas(batch, outcomes);
        for (const char* symbol : {"AAA", "BBB", "CCC"}) {
            for (const std::string& row : book(engine, symbol)) batch_books.push_back(row);
        }
        batch_pending = engine.stats().pending_rows;
        engine.close();
    }

    for (size_t i = 0; i < writes.size(); ++i) {
        EXPECT_TRUE(outcomes[i].error.empty()) << "write " << i << ": " << outcomes[i].error;
        EXPECT_EQ(outcomes[i].status, single_status[i]) << "write " << i;
    }
    EXPECT_EQ(batch_books, single_books);
    EXPECT_EQ(batch_pending, single_pending);

    const std::vector<Record> single_wal = wal_records(one_dir.path);
    const std::vector<Record> batch_wal  = wal_records(batch_dir.path);
    ASSERT_EQ(batch_wal.size(), single_wal.size());
    for (size_t i = 0; i < single_wal.size(); ++i) {
        EXPECT_EQ(batch_wal[i], single_wal[i]) << "WAL record " << i << " differs";
    }
    // The premises, so a change to the writes above cannot quietly stop testing them: the batch
    // wrote every write, and one of them was a GAP against its stream.
    EXPECT_EQ(count_type(single_wal, ob::WAL_RECORD_DELTA), writes.size());
    EXPECT_EQ(count_type(single_wal, ob::WAL_RECORD_GAP), 1u)
        << "the writes no longer skip a number, so the GAP a batch has to place is not tested";
}

TEST(WriteBatch, AMigratedSymbolIsRefusedAndTheWritesAroundItAreApplied) {
    TempDir dir("wb_migrated_");
    ob::Engine engine(dir.path, kNoAutoFlush);
    engine.open();
    engine.mark_symbol_migrated("MMM.EX");

    const std::vector<Write> writes = {
        make_write("AAA", ob::SIDE_BID, 1, 1'000),
        make_write("MMM", ob::SIDE_BID, 1, 2'000),
        make_write("AAA", ob::SIDE_ASK, 3, 3'000),
    };
    std::vector<ob::WriteOutcome> outcomes(writes.size());
    engine.apply_deltas(as_client_writes(writes), outcomes);

    EXPECT_EQ(outcomes[0].status, ob::OB_OK);
    EXPECT_EQ(outcomes[1].status, ob::OB_ERR_MIGRATED);
    EXPECT_EQ(outcomes[2].status, ob::OB_OK);
    for (const ob::WriteOutcome& o : outcomes) EXPECT_TRUE(o.error.empty()) << o.error;

    // Refused before it was numbered: the two writes of AAA are 1 and 2, and nothing of MMM is in
    // the WAL.
    std::vector<uint64_t> numbers;
    for (const Record& r : wal_records(dir.path)) {
        if (std::get<0>(r) != ob::WAL_RECORD_DELTA) continue;
        ob::DeltaUpdate d{};
        std::memcpy(&d, std::get<4>(r).data(), sizeof(d));
        EXPECT_STREQ(d.symbol, "AAA");
        numbers.push_back(std::get<1>(r));
    }
    EXPECT_EQ(numbers, (std::vector<uint64_t>{1, 2}));
    engine.close();
}

TEST(WriteBatch, MultiMasterWritesAreNumberedAndTickedInTheOrderGiven) {
    // Multi-master cannot be compared byte for byte - the HLC is the wall clock - so what is
    // compared is everything else, and what the HLC must do is asserted: each write its own tick,
    // in the order of the writes, as one call per write gave them.
    const std::vector<Write> writes = {
        make_write("AAA", ob::SIDE_BID, 1, 1'000),
        make_write("AAA", ob::SIDE_ASK, 20, 2'000),
        make_write("BBB", ob::SIDE_BID, 3, 3'000),
        make_write("AAA", ob::SIDE_BID, 1, 4'000),
    };

    TempDir one_dir("wb_mm_single_");
    std::vector<Record> single_wal;
    {
        ob::Engine engine(one_dir.path, kNoAutoFlush, ob::FsyncPolicy::INTERVAL, {}, {}, {}, {},
                          mm_config(1));
        engine.open();
        for (const Write& w : writes) {
            EXPECT_EQ(engine.apply_delta_mm(w.update, w.levels.data()), ob::OB_OK);
        }
        engine.close();
        single_wal = wal_records(one_dir.path);
    }

    TempDir batch_dir("wb_mm_batch_");
    std::vector<ob::HLCTimestamp> ticks;
    std::vector<Record> batch_wal;
    {
        ob::Engine engine(batch_dir.path, kNoAutoFlush, ob::FsyncPolicy::INTERVAL, {}, {}, {}, {},
                          mm_config(1));
        engine.open();
        std::vector<ob::WriteOutcome> outcomes(writes.size());
        engine.apply_deltas_mm(as_client_writes(writes), outcomes);
        for (const ob::WriteOutcome& o : outcomes) {
            EXPECT_EQ(o.status, ob::OB_OK);
            EXPECT_TRUE(o.error.empty()) << o.error;
        }
        engine.close();
        ob::WALReplayer replayer(batch_dir.path);
        replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
            if (ctx.header.record_type == ob::WAL_RECORD_DELTA) ticks.push_back(ctx.hlc);
        });
        batch_wal = wal_records(batch_dir.path);
    }

    ASSERT_EQ(batch_wal.size(), single_wal.size());
    for (size_t i = 0; i < single_wal.size(); ++i) {
        EXPECT_EQ(batch_wal[i], single_wal[i]) << "WAL record " << i << " differs";
    }
    ASSERT_EQ(ticks.size(), writes.size());
    for (size_t i = 1; i < ticks.size(); ++i) {
        EXPECT_TRUE(ticks[i - 1] < ticks[i]) << "write " << i << " was not ticked after write "
                                             << i - 1;
    }
    for (const ob::HLCTimestamp& t : ticks) EXPECT_EQ(t.node_id, 1u);
}

TEST(WriteBatch, AnEmptyBatchTouchesNothing) {
    TempDir dir("wb_empty_");
    ob::Engine engine(dir.path, kNoAutoFlush);
    engine.open();
    const size_t before = wal_records(dir.path).size();
    engine.apply_deltas({}, {});
    EXPECT_EQ(wal_records(dir.path).size(), before);
    engine.close();
}

TEST(WriteBatch, TooFewOutcomesIsRefusedBeforeAnythingIsWritten) {
    // A write that could not be answered must not be applied: the outcome is the answer.
    TempDir dir("wb_outcomes_");
    ob::Engine engine(dir.path, kNoAutoFlush);
    engine.open();
    const std::vector<Write> writes = {make_write("AAA", ob::SIDE_BID, 1, 1'000),
                                       make_write("AAA", ob::SIDE_BID, 1, 2'000)};
    std::vector<ob::WriteOutcome> only_one(1);
    const size_t before = wal_records(dir.path).size();
    EXPECT_THROW(engine.apply_deltas(as_client_writes(writes), only_one), std::invalid_argument);
    EXPECT_EQ(wal_records(dir.path).size(), before);
    engine.close();
}

// ── One acquisition per batch ─────────────────────────────────────────────────

namespace {

std::string read_source(const std::string& rel) {
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + rel);
    if (!in) return {};
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

/// `text` without its line comments. The bodies checked below explain themselves in prose that
/// names the lock they are about, and a count that includes the prose counts the explanation.
std::string without_comments(const std::string& text) {
    std::string out;
    std::size_t at = 0;
    while (at < text.size()) {
        const std::size_t comment = text.find("//", at);
        if (comment == std::string::npos) {
            out.append(text, at, std::string::npos);
            break;
        }
        out.append(text, at, comment - at);
        const std::size_t eol = text.find('\n', comment);
        if (eol == std::string::npos) break;
        at = eol;
    }
    return out;
}

/// The braced body of the definition whose signature begins with `signature`, comments removed,
/// or empty.
std::string definition_body(const std::string& src, const std::string& signature) {
    const std::size_t at = src.find("\n" + signature);
    if (at == std::string::npos) return {};
    std::size_t pos = src.find('{', at);
    if (pos == std::string::npos) return {};
    int depth = 0;
    const std::size_t start = pos;
    for (; pos < src.size(); ++pos) {
        if (src[pos] == '{') ++depth;
        if (src[pos] == '}' && --depth == 0) {
            return without_comments(src.substr(start, pos - start + 1));
        }
    }
    return {};
}

std::size_t occurrences(const std::string& text, const std::string& needle) {
    std::size_t n = 0;
    for (std::size_t at = text.find(needle); at != std::string::npos;
         at = text.find(needle, at + needle.size())) {
        ++n;
    }
    return n;
}

} // namespace

TEST(WriteBatchStatic, TheWritesOfABatchTakeTheLockOnceAndReachTheWalInOneCall) {
    // The behavioural tests above pass for a batch that takes the lock per write and appends per
    // write - that produces the same state, and it is exactly the convoy this stage removes
    // (futex calls 1 128 at one reactor, 1 485 158 at four, all in the write path). So the shape is
    // asserted where the state cannot say it.
    const std::string src = read_source("src/engine.cpp");
    ASSERT_FALSE(src.empty()) << "cannot read src/engine.cpp, so this test checks nothing";

    const std::string batch = definition_body(src, "void Engine::apply_local_writes(");
    ASSERT_FALSE(batch.empty()) << "Engine::apply_local_writes moved; this test would check nothing";

    EXPECT_EQ(occurrences(batch, "std::unique_lock<std::mutex> lock(mtx_);"), 1u)
        << "the batch takes the engine's lock more than once, or not in the one place it names";
    EXPECT_EQ(occurrences(batch, "mtx_"), 1u)
        << "something other than the one acquisition names mtx_ in the batch";
    EXPECT_EQ(occurrences(batch, "wal_.append_batch("), 1u)
        << "the batch does not reach the WAL in one call";
    EXPECT_EQ(occurrences(batch, "wal_.append("), 0u) << "a record of the batch is appended alone";
    EXPECT_EQ(occurrences(batch, "append_with_origin("), 0u)
        << "a record of the batch is appended alone";
    EXPECT_EQ(occurrences(batch, "append_gap("), 0u)
        << "a GAP is written ahead of the batch rather than in front of its record";
    EXPECT_EQ(occurrences(batch, "stamp_sequence("), 0u)
        << "stamp_sequence() writes its GAP record itself; a batch has to place it";
    EXPECT_EQ(occurrences(batch, "await_pending_room("), 1u)
        << "the batch waits for room more than once";

    // The single-write entry points are this with one write, so there is one path to keep right.
    for (const char* entry : {"ob_status_t Engine::apply_delta(",
                              "ob_status_t Engine::apply_delta_replicated(",
                              "ob_status_t Engine::apply_delta_mm("}) {
        const std::string body = definition_body(src, entry);
        ASSERT_FALSE(body.empty()) << entry << " moved";
        EXPECT_NE(body.find("apply_delta_impl("), std::string::npos) << entry;
        EXPECT_EQ(body.find("mtx_"), std::string::npos) << entry << " takes the lock itself";
    }
    const std::string impl = definition_body(src, "ob_status_t Engine::apply_delta_impl(");
    ASSERT_FALSE(impl.empty());
    EXPECT_NE(impl.find("apply_local_writes("), std::string::npos)
        << "a single write no longer goes through the batch";
    EXPECT_EQ(impl.find("mtx_"), std::string::npos);

    // The steps a written record takes run under the batch's lock and must not take it again.
    for (const char* step : {"void Engine::broadcast_to_replicas(",
                             "ob_status_t Engine::apply_in_memory(",
                             "void Engine::note_local_hlcs(",
                             "void Engine::broadcast_to_peers("}) {
        const std::string body = definition_body(src, step);
        ASSERT_FALSE(body.empty()) << step << " moved";
        EXPECT_EQ(body.find("mtx_"), std::string::npos) << step << " names the engine's lock";
    }
}
