// The WAL replay read in blocks, and the one pass a start makes over the whole of it (#174).
//
// Every pass a start makes over the WAL read each record with a lseek() and three read()s and gave
// its payload a vector of its own: 4 us a record, and 23 s to start on a WAL of 422 MB. The
// replayer reads a file a block at a time now and hands records out of memory. What it decides
// about a record must not change with that, and where it could change is the edge of a block: a
// header or a payload begun in one block and finished in the next, and a file that ends inside
// either. These tests read with the least block the replayer allows, so that records cross blocks
// at every alignment, and hold what comes back to what was written - the position append() said,
// the levels it was given - and a file cut short anywhere to the records wholly before the cut.
//
// The last tests are the reader's other half of the fix: `replay()` is `replay_v2()` seen without
// origins, where it had a parser of its own that read a 38-byte header as a 24-byte one, and the
// pass that finds the last checkpoint hands every record on, so a start needs no pass of its own
// for anything else it takes from the whole log.

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "orderbook/data_model.hpp"
#include "orderbook/epoch.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/wal.hpp"

namespace fs = std::filesystem;

namespace {

fs::path make_temp_dir(const std::string& suffix) {
    static int n = 0;
    const auto base = fs::temp_directory_path() /
                      ("ob_wal_blocks_" + suffix + "_" + std::to_string(::getpid()) + "_" +
                       std::to_string(++n) + "_" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(base);
    return base;
}

struct TempDir {
    fs::path path;
    explicit TempDir(const std::string& suffix) : path(make_temp_dir(suffix)) {}
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
    std::string str() const { return path.string(); }
};

/// The least block a replayer reads with: one record of the largest size a header describes.
constexpr size_t kLeastBlock = sizeof(ob::WALRecordV2) + ob::WAL_MAX_PAYLOAD_LEN;

/// The most levels one record holds.
constexpr uint16_t kMostLevels =
    static_cast<uint16_t>((ob::WAL_MAX_PAYLOAD_LEN - sizeof(ob::DeltaUpdate)) / sizeof(ob::Level));

/// No rotation in the length of a test: one file, so every position append() returns is in it.
constexpr size_t kOneFile = 1ULL << 30;

/// A writer for a test's WAL. Nothing is synced: the replay reads the page cache all the same, and
/// a rotation every few blocks would otherwise sync each file on the writer's thread and say so.
struct TestWriter {
    ob::WALWriter writer;
    TestWriter(const std::string& dir, size_t rotate_bytes)
        : writer(dir, rotate_bytes, ob::FsyncPolicy::NONE) {}
};

std::string wal_name(uint32_t index) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "wal_%06u.bin", index);
    return buf;
}

/// One DELTA record as it was written: where, which, and the levels it carried.
struct Written {
    ob::WalPosition at;
    uint64_t        seq{0};
    uint16_t        origin{0};   ///< 0 for a record written without one
    uint16_t        levels{0};
    uint64_t        end{0};      ///< one past its last byte, in its file
};

/// A record's levels are told apart by their prices: level i of record seq is priced seq*10000+i.
std::vector<ob::Level> levels_of(uint64_t seq, uint16_t count) {
    std::vector<ob::Level> levels(count);
    for (uint16_t i = 0; i < count; ++i) {
        levels[i].price = static_cast<int64_t>(seq * 10'000 + i);
        levels[i].qty   = static_cast<int64_t>(seq);
        levels[i].cnt   = 1;
    }
    return levels;
}

/// Append DELTA records of the given level counts - every third one with an origin unless
/// `origins` is false, so both header sizes cross blocks - and return what was written, in order.
std::vector<Written> write_records(ob::WALWriter& writer, const std::vector<uint16_t>& counts,
                                   bool origins = true, uint64_t first_seq = 1) {
    ob::HybridLogicalClock clock(7);
    std::vector<Written> out;
    uint64_t seq = first_seq;
    for (const uint16_t count : counts) {
        ob::DeltaUpdate d{};
        std::strncpy(d.symbol, "BLOCKS", sizeof(d.symbol) - 1);
        std::strncpy(d.exchange, "EX", sizeof(d.exchange) - 1);
        d.sequence_number = seq;
        d.timestamp_ns    = 1'000'000'000ULL + seq;
        d.side            = ob::SIDE_BID;
        d.n_levels        = count;
        const auto levels = levels_of(seq, count);
        Written w;
        w.seq    = seq;
        w.levels = count;
        if (origins && seq % 3 == 0) {
            w.origin = static_cast<uint16_t>(1 + seq % 5);
            w.at     = writer.append_with_origin(d, levels.data(), w.origin, clock.tick_local());
        } else {
            w.at = writer.append(d, levels.data());
        }
        const uint64_t header = w.origin != 0 ? sizeof(ob::WALRecordV2) : sizeof(ob::WALRecord);
        w.end = w.at.offset + header + sizeof(ob::DeltaUpdate) + count * sizeof(ob::Level);
        out.push_back(w);
        ++seq;
    }
    EXPECT_TRUE(writer.sync());
    return out;
}

/// Whether a replayed record is the one written: the same place, number, origin and levels.
::testing::AssertionResult same_record(const ob::WALReplayContext& ctx, const Written& w) {
    if (ctx.header.record_type != ob::WAL_RECORD_DELTA) {
        return ::testing::AssertionFailure() << "record type " << int(ctx.header.record_type);
    }
    if (ctx.wal_file_index != w.at.file_index || ctx.wal_byte_offset != w.at.offset) {
        return ::testing::AssertionFailure()
               << "seq " << w.seq << " read at " << ctx.wal_file_index << ":" << ctx.wal_byte_offset
               << ", written at " << w.at.file_index << ":" << w.at.offset;
    }
    if (ctx.header.sequence_number != w.seq || ctx.origin_node_id != w.origin) {
        return ::testing::AssertionFailure() << "seq " << ctx.header.sequence_number << " origin "
                                             << ctx.origin_node_id << ", written seq " << w.seq
                                             << " origin " << w.origin;
    }
    const size_t expected_len = sizeof(ob::DeltaUpdate) + w.levels * sizeof(ob::Level);
    if (ctx.payload_len != expected_len || ctx.payload == nullptr) {
        return ::testing::AssertionFailure() << "seq " << w.seq << " payload of " << ctx.payload_len
                                             << " bytes, written " << expected_len;
    }
    ob::DeltaUpdate d{};
    std::memcpy(&d, ctx.payload, sizeof(d));
    if (d.sequence_number != w.seq || d.n_levels != w.levels) {
        return ::testing::AssertionFailure() << "seq " << w.seq << " carries the update of seq "
                                             << d.sequence_number;
    }
    const auto levels = levels_of(w.seq, w.levels);
    if (std::memcmp(ctx.payload + sizeof(d), levels.data(), levels.size() * sizeof(ob::Level)) != 0) {
        return ::testing::AssertionFailure() << "seq " << w.seq << ": levels differ";
    }
    return ::testing::AssertionSuccess();
}

/// The DELTA records a replay_v2() with `block` bytes delivers, checked against `written` in order.
/// Returns how many were delivered; the first mismatch fails the test.
size_t replay_and_check(const std::string& dir, size_t block, const std::vector<Written>& written) {
    ob::WALReplayer replayer(dir, block);
    size_t i = 0;
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_DELTA) return;
        if (i >= written.size()) {
            ADD_FAILURE() << "a record past the " << written.size() << " written";
            return;
        }
        EXPECT_TRUE(same_record(ctx, written[i]));
        ++i;
    });
    return i;
}

/// Level counts from 1 to the most one record holds, in an order that puts every size next to
/// every other: records of a few dozen bytes to 64 KB, headers and payloads across block edges.
std::vector<uint16_t> varied_counts(size_t n) {
    std::vector<uint16_t> counts;
    uint32_t x = 12345;
    for (size_t i = 0; i < n; ++i) {
        x = x * 1103515245u + 12345u;
        const uint32_t r = (x >> 8) % 100;
        counts.push_back(r < 5 ? kMostLevels : r < 60 ? static_cast<uint16_t>(1 + (x >> 4) % 8)
                                                      : static_cast<uint16_t>(1 + (x >> 4) % 400));
    }
    return counts;
}

uint64_t file_size(const std::string& path) {
    return static_cast<uint64_t>(fs::file_size(path));
}

}  // namespace

TEST(WalReplayBlocks, RecordsCrossingBlocksComeBackWhereAndAsTheyWereWritten) {
    TempDir dir("cross");
    std::vector<Written> written;
    {
        TestWriter w(dir.str(), kOneFile);
        written = write_records(w.writer, varied_counts(1500));
    }
    ASSERT_GT(file_size((dir.path / wal_name(0)).string()), 20 * kLeastBlock)
        << "the premise: the file is many blocks long";

    // The least block, one of an odd size, and the default: the same records, in the same places.
    for (const size_t block : {kLeastBlock, kLeastBlock + 4099, ob::WALReplayer::kDefaultReadBlockBytes}) {
        EXPECT_EQ(replay_and_check(dir.str(), block, written), written.size()) << "block " << block;
    }
}

TEST(WalReplayBlocks, ABlockAskedForBelowTheLargestRecordIsTheLargestRecord) {
    // A record of the most levels there are, between small ones, read with a block of one byte
    // asked for: the replayer takes the least block that holds it instead.
    TempDir dir("floor");
    std::vector<Written> written;
    {
        TestWriter w(dir.str(), kOneFile);
        written = write_records(w.writer, {3, kMostLevels, 1, kMostLevels, kMostLevels, 2});
    }
    EXPECT_EQ(replay_and_check(dir.str(), 1, written), written.size());
}

TEST(WalReplayBlocks, AFileCutShortAnywhereEndsAtTheLastWholeRecord) {
    // Enough records that the file is a block and some more, the cut then moved back one byte at a
    // time across the block's edge: whatever the cut splits - a header, the rest of a 38-byte one,
    // a payload - the replay delivers exactly the records that end at or before it.
    TempDir dir("cut");
    std::vector<Written> written;
    {
        TestWriter w(dir.str(), kOneFile);
        std::vector<uint16_t> counts;
        uint64_t bytes = 0;
        for (uint16_t i = 0; bytes < kLeastBlock + 3000; ++i) {
            const uint16_t c = static_cast<uint16_t>(1 + (i * 37) % 23);
            counts.push_back(c);
            bytes += sizeof(ob::WALRecordV2) + sizeof(ob::DeltaUpdate) + c * sizeof(ob::Level);
        }
        written = write_records(w.writer, counts);
    }
    const std::string path = (dir.path / wal_name(0)).string();
    const uint64_t full = file_size(path);
    ASSERT_GT(full, kLeastBlock) << "the premise: the cuts are past the first block";

    const int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    size_t cuts = 0;
    for (uint64_t cut = full; cut + 6000 >= full && cut > 0; --cut) {
        ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(cut)), 0);
        const size_t whole = static_cast<size_t>(
            std::count_if(written.begin(), written.end(), [&](const Written& w) { return w.end <= cut; }));
        const std::vector<Written> expected(written.begin(), written.begin() + static_cast<long>(whole));
        ASSERT_EQ(replay_and_check(dir.str(), kLeastBlock, expected), whole) << "cut at " << cut;
        ++cuts;
    }
    ::close(fd);
    EXPECT_GE(cuts, 6000u);
}

TEST(WalReplayBlocks, ReplayOfOldRecordsCutShortAnywhereEndsAtTheLastWholeRecord) {
    // The same for replay(), which reads records written without an origin: 24-byte headers.
    TempDir dir("cut_v1");
    std::vector<Written> written;
    {
        TestWriter w(dir.str(), kOneFile);
        std::vector<uint16_t> counts;
        uint64_t bytes = 0;
        for (uint16_t i = 0; bytes < kLeastBlock + 3000; ++i) {
            const uint16_t c = static_cast<uint16_t>(1 + (i * 37) % 23);
            counts.push_back(c);
            bytes += sizeof(ob::WALRecord) + sizeof(ob::DeltaUpdate) + c * sizeof(ob::Level);
        }
        written = write_records(w.writer, counts, /*origins=*/false);
    }
    const std::string path = (dir.path / wal_name(0)).string();
    const uint64_t full = file_size(path);
    ASSERT_GT(full, kLeastBlock) << "the premise: the cuts are past the first block";
    const int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    for (uint64_t cut = full; cut + 4000 >= full; --cut) {
        ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(cut)), 0);
        const size_t whole = static_cast<size_t>(
            std::count_if(written.begin(), written.end(), [&](const Written& w) { return w.end <= cut; }));
        size_t delivered = 0;
        ob::WALReplayer replayer(dir.str(), kLeastBlock);
        replayer.replay([&](const ob::WALRecord& hdr, const uint8_t* payload) {
            if (hdr.record_type != ob::WAL_RECORD_DELTA) return;
            ASSERT_LT(delivered, whole) << "cut at " << cut;
            const Written& w = written[delivered];
            ob::DeltaUpdate d{};
            std::memcpy(&d, payload, sizeof(d));
            const auto levels = levels_of(w.seq, w.levels);
            EXPECT_EQ(hdr.sequence_number, w.seq) << "cut at " << cut;
            EXPECT_EQ(d.n_levels, w.levels) << "cut at " << cut;
            EXPECT_EQ(std::memcmp(payload + sizeof(d), levels.data(), levels.size() * sizeof(ob::Level)), 0)
                << "cut at " << cut << ": the levels of seq " << w.seq;
            ++delivered;
        });
        ASSERT_EQ(delivered, whole) << "cut at " << cut;
    }
    ::close(fd);
}

TEST(WalReplayBlocks, ATornRecordPastABlockEndsOnlyItsOwnFile) {
    // A file that is not the last, torn in a record read after the block was refilled: the records
    // before the tear, then every record of the next file (#126), and the tear counted once.
    TempDir dir("torn");
    std::vector<Written> written;
    {
        TestWriter w(dir.str(), 3 * kLeastBlock);
        written = write_records(w.writer, varied_counts(400));
    }
    ASSERT_TRUE(fs::exists(dir.path / wal_name(1))) << "the premise: the log rotated";
    auto torn = std::find_if(written.begin(), written.end(), [](const Written& w) {
        return w.at.file_index == 0 && w.at.offset > kLeastBlock + 1000;
    });
    ASSERT_NE(torn, written.end());
    {
        // One byte of the payload, well inside it, changed: the checksum no longer holds.
        std::fstream f((dir.path / wal_name(0)).string(), std::ios::in | std::ios::out | std::ios::binary);
        const uint64_t header = torn->origin != 0 ? sizeof(ob::WALRecordV2) : sizeof(ob::WALRecord);
        f.seekg(static_cast<std::streamoff>(torn->at.offset + header + sizeof(ob::DeltaUpdate)));
        char byte = 0;
        f.read(&byte, 1);
        byte = static_cast<char>(byte ^ 0x5a);
        f.seekp(static_cast<std::streamoff>(torn->at.offset + header + sizeof(ob::DeltaUpdate)));
        f.write(&byte, 1);
    }
    std::vector<Written> expected(written.begin(), torn);
    for (const auto& w : written) {
        if (w.at.file_index > 0) expected.push_back(w);
    }
    ob::WALReplayer replayer(dir.str(), kLeastBlock);
    size_t i = 0;
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_DELTA) return;
        ASSERT_LT(i, expected.size());
        EXPECT_TRUE(same_record(ctx, expected[i]));
        ++i;
    });
    EXPECT_EQ(i, expected.size());
    EXPECT_EQ(replayer.tears_skipped(), 1u);
}

RC_GTEST_PROP(WalReplayBlocksProperty, AnyRecordsAnyBlockReadBackAsWritten,
              (unsigned salt, unsigned extra_block)) {
    TempDir dir("prop");
    std::vector<uint16_t> counts;
    uint32_t x = salt | 1u;
    const size_t n = 50 + salt % 300;
    for (size_t i = 0; i < n; ++i) {
        x = x * 1664525u + 1013904223u;
        counts.push_back(x % 11 == 0 ? kMostLevels : static_cast<uint16_t>(1 + (x >> 8) % 600));
    }
    std::vector<Written> written;
    {
        TestWriter w(dir.str(), 2 * kLeastBlock + salt % kLeastBlock);
        written = write_records(w.writer, counts);
    }
    const size_t block = kLeastBlock + extra_block % (2 * kLeastBlock);
    ob::WALReplayer replayer(dir.str(), block);
    size_t i = 0;
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_DELTA) return;
        RC_ASSERT(i < written.size());
        const auto same = same_record(ctx, written[i]);
        if (!same) RC_FAIL(same.message());
        ++i;
    });
    RC_ASSERT(i == written.size());
}

TEST(WalReplayBlocks, ReplayReadsRecordsWithAnOriginAsReplayV2Does) {
    // A mesh node's records have 38-byte headers. replay() read them as 24-byte ones - the origin
    // and clock as the payload's first bytes - so the first failed its checksum and ended the file,
    // and nothing after it was read: here, the epoch.
    TempDir dir("origins");
    std::vector<Written> written;
    {
        TestWriter w(dir.str(), kOneFile);
        written = write_records(w.writer, {4, 1, 9, 2, 7, 3});
        w.writer.append_epoch(ob::EpochValue{7});
        ASSERT_TRUE(w.writer.sync());
    }
    ASSERT_TRUE(std::any_of(written.begin(), written.end(), [](const Written& w) { return w.origin != 0; }))
        << "the premise: records with an origin";

    ob::WALReplayer replayer(dir.str(), kLeastBlock);
    std::vector<uint64_t> seqs;
    replayer.replay([&](const ob::WALRecord& hdr, const uint8_t* payload) {
        if (hdr.record_type != ob::WAL_RECORD_DELTA) return;
        const Written& w = written.at(seqs.size());
        ob::DeltaUpdate d{};
        std::memcpy(&d, payload, sizeof(d));
        EXPECT_EQ(d.sequence_number, w.seq) << "the payload of seq " << w.seq << " starts elsewhere";
        seqs.push_back(hdr.sequence_number);
    });
    EXPECT_EQ(seqs.size(), written.size());
    EXPECT_EQ(replayer.last_epoch(), 7u) << "the epoch after the records was not read";
    EXPECT_EQ(replayer.tears_skipped(), 0u);
}

TEST(WalReplayBlocks, TheCheckpointPassHandsOnEveryRecordItReads) {
    // What a start needs from the whole log besides the last checkpoint - the vector, the held
    // numbers, the epoch - it takes from this pass: every record, in order, as replay_v2() reads
    // them, and the pass's epoch after it.
    TempDir dir("also");
    {
        TestWriter w(dir.str(), 3 * kLeastBlock);
        (void)write_records(w.writer, varied_counts(200));
        w.writer.append_checkpoint(1, w.writer.current_position());
        w.writer.append_epoch(ob::EpochValue{11});
        (void)write_records(w.writer, {5, 6});
        ASSERT_TRUE(w.writer.sync());
    }
    struct Seen {
        uint8_t  type;
        uint64_t seq;
        uint32_t file;
        uint64_t offset;
        bool operator==(const Seen&) const = default;
    };
    std::vector<Seen> expected;
    ob::WALReplayer first(dir.str(), kLeastBlock);
    first.replay_v2([&](const ob::WALReplayContext& ctx) {
        expected.push_back({ctx.header.record_type, ctx.header.sequence_number, ctx.wal_file_index,
                            ctx.wal_byte_offset});
    });

    std::vector<Seen> also;
    ob::WALReplayer scan(dir.str(), kLeastBlock);
    const auto last = scan.find_last_checkpoint([&](const ob::WALReplayContext& ctx) {
        also.push_back({ctx.header.record_type, ctx.header.sequence_number, ctx.wal_file_index,
                        ctx.wal_byte_offset});
    });
    EXPECT_TRUE(also == expected) << also.size() << " records handed on of " << expected.size();
    EXPECT_EQ(last.records, expected.size());
    EXPECT_GT(last.ordinal, 0u);
    EXPECT_EQ(scan.last_epoch(), 11u);
}

// ── The tail pass from a mark (#174) ─────────────────────────────────────────

namespace {

/// Everything a replay delivers, as (type, seq, file, offset): enough to tell the records apart.
struct Delivered {
    uint8_t  type;
    uint64_t seq;
    uint32_t file;
    uint64_t offset;
    bool operator==(const Delivered&) const = default;
};

Delivered delivered(const ob::WALReplayContext& ctx) {
    return {ctx.header.record_type, ctx.header.sequence_number, ctx.wal_file_index, ctx.wal_byte_offset};
}

/// A WAL of several files with checkpoints between the records - the eight-byte form, and the
/// sixteen-byte one whose position is a record some way back, as a flush with a block waiting
/// writes it - so the last checkpoint gives records back from before itself.
std::vector<Written> write_with_checkpoints(const std::string& dir, unsigned salt, size_t records) {
    TestWriter w(dir, 3 * kLeastBlock + salt % kLeastBlock);
    std::vector<Written> written;
    uint32_t x = salt | 1u;
    for (size_t i = 0; i < records; ++i) {
        x = x * 1664525u + 1013904223u;
        const uint16_t count = x % 13 == 0 ? kMostLevels : static_cast<uint16_t>(1 + (x >> 8) % 300);
        written.push_back(write_records(w.writer, {count}, true, written.size() + 1).front());
        if ((x >> 4) % 23 == 0) {
            if ((x >> 12) % 2 == 0 || written.size() < 3) {
                w.writer.append_checkpoint(1, w.writer.current_position());
            } else {
                // Back to a record some way before this one, as a block that still waits needs it.
                const size_t back = 1 + (x >> 16) % std::min<size_t>(written.size() - 1, 40);
                w.writer.append_checkpoint(1, written[written.size() - 1 - back].at, 3);
            }
        }
    }
    EXPECT_TRUE(w.writer.sync());
    return written;
}

}  // namespace

TEST(WalReplayBlocks, AReplayFromAMarkReadsWhatTheWholeReplayReadsFromThere) {
    TempDir dir("from_mark");
    (void)write_with_checkpoints(dir.str(), 17, 900);
    std::vector<Delivered> whole;
    ob::WALReplayer all(dir.str(), kLeastBlock);
    all.replay_v2([&](const ob::WALReplayContext& ctx) { whole.push_back(delivered(ctx)); });

    ob::WALReplayer scan(dir.str(), kLeastBlock);
    const auto last = scan.find_last_checkpoint();
    ASSERT_GT(last.marks.size(), 20u) << "the premise: marks in many files and blocks";
    ASSERT_GT(last.marks.back().at.file_index, 2u);
    for (const auto& mark : last.marks) {
        std::vector<Delivered> from_mark;
        ob::WALReplayer tail(dir.str(), kLeastBlock);
        tail.replay_v2_from(mark.at, [&](const ob::WALReplayContext& ctx) { from_mark.push_back(delivered(ctx)); });
        ASSERT_LE(mark.ordinal, whole.size());
        const std::vector<Delivered> expected(whole.begin() + static_cast<long>(mark.ordinal - 1), whole.end());
        ASSERT_TRUE(from_mark == expected)
            << "from the mark at " << mark.at.file_index << ":" << mark.at.offset << " (ordinal "
            << mark.ordinal << "): " << from_mark.size() << " records, " << expected.size() << " expected";
    }
}

RC_GTEST_PROP(WalReplayBlocksProperty, TheTailPassForwardsWhatAReadFromTheStartForwards,
              (unsigned salt)) {
    // The tail pass from the last mark against the same pass without marks, which reads from the
    // start of the log as it always did: the same records forwarded, in the same order.
    TempDir dir("tail_prop");
    const size_t n = 100 + salt % 700;
    (void)write_with_checkpoints(dir.str(), salt, n);

    ob::WALReplayer scan(dir.str(), kLeastBlock);
    const auto last = scan.find_last_checkpoint();
    auto without_marks = last;
    without_marks.marks.clear();

    // What the runs cover, said with the result: a checkpoint that gives records back, and a pass
    // that begins past the start of the log.
    const bool gives_back = last.covered && ob::wal_position_before(*last.covered, last.at);
    const bool seeks = last.ordinal > 0 && last.marks.size() > 1 &&
                       !ob::wal_position_before(last.at, last.marks[1].at);
    RC_TAG(gives_back ? "gives records back" : "covers all before it");
    RC_TAG(seeks ? "begins past the first mark" : "begins at the start");

    std::vector<Delivered> from_mark, from_start;
    ob::WALReplayer a(dir.str(), kLeastBlock);
    a.replay_after(last, [&](const ob::WALReplayContext& ctx) { from_mark.push_back(delivered(ctx)); });
    ob::WALReplayer b(dir.str(), kLeastBlock);
    b.replay_after(without_marks, [&](const ob::WALReplayContext& ctx) { from_start.push_back(delivered(ctx)); });
    RC_ASSERT(from_mark == from_start);
}

TEST(WalReplayBlocks, TheTailPassReadsFromTheLastMarkBeforeWhatItForwards) {
    // A long log whose last checkpoint covers all but its last records: the tail pass reads those
    // and at most a block before them, where it read the whole log.
    TempDir dir("tail_bytes");
    uint64_t tail_from = 0;
    {
        TestWriter w(dir.str(), kOneFile);
        (void)write_records(w.writer, varied_counts(1500));
        w.writer.append_checkpoint(1, w.writer.current_position());
        tail_from = w.writer.current_position().offset;
        (void)write_records(w.writer, {3, 9, 27});
        ASSERT_TRUE(w.writer.sync());
    }
    const uint64_t size = file_size((dir.path / wal_name(0)).string());
    ASSERT_GT(size, 40 * kLeastBlock);

    ob::WALReplayer scan(dir.str(), kLeastBlock);
    const auto last = scan.find_last_checkpoint();
    size_t forwarded = 0;
    ob::WALReplayer tail(dir.str(), kLeastBlock);
    const uint64_t before = ob::WALReplayer::bytes_read_for_test();
    tail.replay_after(last, [&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type == ob::WAL_RECORD_DELTA) ++forwarded;
    });
    const uint64_t read = ob::WALReplayer::bytes_read_for_test() - before;
    EXPECT_EQ(forwarded, 3u);
    EXPECT_LE(read, (size - tail_from) + 2 * kLeastBlock) << "the tail pass read " << read << " of " << size;
}
