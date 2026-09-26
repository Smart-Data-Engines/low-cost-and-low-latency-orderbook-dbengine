// WALRecordCursor (#178): a WAL read forward from a position, a record at a time, while it is
// still being written.
//
// A multi-master catch-up is rounds, each continuing where the last one stopped. What a record is
// has one answer in this tree - `WALReplayer::replay_v2()`'s - so the first tests hold the cursor
// to the sequence replay_v2() delivers, on the same directories, including when the cursor is
// stopped anywhere and a new one continues from where it stood. The rest are the places the two
// must differ, because the last file is being written: a record not all there yet is a wait, not
// an end, and a rotation whose next file does not exist yet is too.

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <tuple>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "orderbook/data_model.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/wal.hpp"

namespace fs = std::filesystem;

namespace {

fs::path make_temp_dir(const std::string& suffix) {
    static int n = 0;
    const auto base = fs::temp_directory_path() /
                      ("ob_wal_cursor_" + suffix + "_" + std::to_string(::getpid()) + "_" +
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

ob::DeltaUpdate make_delta(uint64_t seq, uint16_t levels) {
    ob::DeltaUpdate upd{};
    std::strncpy(upd.symbol, "CURSOR", sizeof(upd.symbol) - 1);
    std::strncpy(upd.exchange, "EX", sizeof(upd.exchange) - 1);
    upd.sequence_number = seq;
    upd.timestamp_ns    = 1'000'000'000ULL + seq;
    upd.side            = ob::SIDE_BID;
    upd.n_levels        = levels;
    return upd;
}

/// What a reader saw of one record: enough to tell any two records of a test apart.
using Seen = std::tuple<uint8_t, uint64_t, uint16_t, std::vector<uint8_t>, uint32_t, uint64_t>;

Seen seen(const ob::WALReplayContext& ctx) {
    return Seen{ctx.header.record_type, ctx.header.sequence_number, ctx.origin_node_id,
                std::vector<uint8_t>(ctx.payload, ctx.payload + ctx.payload_len),
                ctx.wal_file_index, ctx.wal_byte_offset};
}

std::vector<Seen> replayed(const std::string& dir) {
    std::vector<Seen> out;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) { out.push_back(seen(ctx)); });
    return out;
}

/// Everything a cursor delivers until it says End.
std::vector<Seen> drained(ob::WALRecordCursor& cursor) {
    std::vector<Seen> out;
    ob::WALReplayContext ctx;
    while (cursor.next(ctx) == ob::WALRecordCursor::Step::Record) out.push_back(seen(ctx));
    return out;
}

/// A WAL of `n` records across several files: DELTA records with and without an origin, of
/// several sizes, and the other kinds a WAL holds between them.
void write_mixed(const std::string& dir, int n, size_t rotate_bytes, unsigned salt = 0) {
    ob::WALWriter writer(dir, rotate_bytes);
    std::vector<ob::Level> levels(64);
    for (size_t i = 0; i < levels.size(); ++i) {
        levels[i].price = 1000 + static_cast<int64_t>(i);
        levels[i].qty   = 5;
        levels[i].cnt   = 1;
    }
    ob::HybridLogicalClock clock(3);
    for (int i = 1; i <= n; ++i) {
        const uint16_t count = static_cast<uint16_t>(1 + (i * 7 + salt) % 64);
        const ob::DeltaUpdate d = make_delta(static_cast<uint64_t>(i), count);
        if ((i + salt) % 3 == 0) {
            writer.append_with_origin(d, levels.data(), static_cast<uint16_t>(1 + i % 4), clock.tick_local());
        } else {
            writer.append(d, levels.data());
        }
        if ((i + salt) % 17 == 0) writer.append_gap(static_cast<uint64_t>(i), d.timestamp_ns);
        if ((i + salt) % 29 == 0) {
            writer.append_checkpoint(d.timestamp_ns, writer.current_position(), static_cast<uint64_t>(i));
        }
    }
}

std::string wal_name(uint32_t index) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "wal_%06u.bin", index);
    return buf;
}

size_t wal_files(const std::string& dir) {
    size_t n = 0;
    for (const auto& e : fs::directory_iterator(dir)) {
        const std::string name = e.path().filename().string();
        if (name.rfind("wal_", 0) == 0 && name.size() == 14) ++n;
    }
    return n;
}

}  // namespace

TEST(WALRecordCursor, DeliversWhatReplayV2DeliversAcrossRotationsAndBothHeaders) {
    TempDir dir("same");
    write_mixed(dir.str(), 600, 8192);
    ASSERT_GT(wal_files(dir.str()), 3u) << "the premise: several files, so rotations are crossed";

    const std::vector<Seen> expected = replayed(dir.str());
    ASSERT_GT(expected.size(), 600u) << "the premise: DELTA records and the others between them";

    ob::WALRecordCursor cursor(dir.str());
    cursor.seek(ob::WalPosition{0, 0});
    EXPECT_EQ(drained(cursor), expected);
}

TEST(WALRecordCursor, AStoppedCursorContinuesWhereItStood) {
    TempDir dir("resume");
    write_mixed(dir.str(), 400, 4096, 5);
    const std::vector<Seen> expected = replayed(dir.str());

    // Every stopping point, each with a fresh cursor: what a round of a catch-up does.
    for (size_t stop = 0; stop <= expected.size(); stop += 37) {
        std::vector<Seen> got;
        ob::WalPosition at{};
        {
            ob::WALRecordCursor first(dir.str());
            first.seek(ob::WalPosition{0, 0});
            ob::WALReplayContext ctx;
            for (size_t i = 0; i < stop; ++i) {
                ASSERT_EQ(first.next(ctx), ob::WALRecordCursor::Step::Record) << "stop " << stop;
                got.push_back(seen(ctx));
            }
            at = first.position();
        }
        ob::WALRecordCursor second(dir.str());
        second.seek(at);
        for (auto& s : drained(second)) got.push_back(std::move(s));
        ASSERT_EQ(got, expected) << "stopped after " << stop << " record(s)";
    }
}

RC_GTEST_PROP(WALRecordCursorProperty, AnySequenceOfStopsReadsTheLogOnce,
              (unsigned salt, unsigned records, unsigned rotate_kib)) {
    TempDir dir("prop");
    write_mixed(dir.str(), 1 + static_cast<int>(records % 300), 4096 + (rotate_kib % 16) * 1024, salt);
    const std::vector<Seen> expected = replayed(dir.str());

    const auto stops = *rc::gen::container<std::vector<unsigned>>(rc::gen::inRange(0u, 50u));
    std::vector<Seen> got;
    ob::WalPosition at{};
    size_t s = 0;
    for (;;) {
        ob::WALRecordCursor cursor(dir.str());
        cursor.seek(at);
        const size_t take = s < stops.size() ? stops[s++] : SIZE_MAX;
        ob::WALReplayContext ctx;
        size_t taken = 0;
        bool ended = false;
        while (taken < take) {
            if (cursor.next(ctx) != ob::WALRecordCursor::Step::Record) { ended = true; break; }
            got.push_back(seen(ctx));
            ++taken;
        }
        at = cursor.position();
        if (ended) break;
    }
    RC_ASSERT(got == expected);
}

TEST(WALRecordCursor, ARecordNotAllWrittenIsAWaitAndThenARecord) {
    TempDir dir("tail");
    write_mixed(dir.str(), 20, 1 << 20);
    ASSERT_EQ(wal_files(dir.str()), 1u);
    const std::vector<Seen> before = replayed(dir.str());

    // The record the writer is in the middle of: its bytes, cut after the header and again in
    // the payload, appended the way a write in progress leaves them.
    TempDir scratch("tail_src");
    {
        ob::WALWriter one(scratch.str(), 1 << 20);
        std::vector<ob::Level> lv(40);
        one.append(make_delta(999, 40), lv.data());
    }
    std::ifstream src(scratch.path / "wal_000000.bin", std::ios::binary);
    const std::vector<char> record((std::istreambuf_iterator<char>(src)), {});
    ASSERT_GT(record.size(), 100u);

    const std::string path = (dir.path / "wal_000000.bin").string();
    const auto append = [&](size_t from, size_t to) {
        std::ofstream out(path, std::ios::binary | std::ios::app);
        out.write(record.data() + from, static_cast<std::streamsize>(to - from));
    };

    ob::WALRecordCursor cursor(dir.str());
    cursor.seek(ob::WalPosition{0, 0});
    EXPECT_EQ(drained(cursor), before);
    const ob::WalPosition tail = cursor.position();

    ob::WALReplayContext ctx;
    append(0, 10);                                    // part of the header
    EXPECT_EQ(cursor.next(ctx), ob::WALRecordCursor::Step::End);
    EXPECT_EQ(cursor.position().offset, tail.offset) << "a wait must not move the position";
    append(10, 60);                                   // the header and part of the payload
    EXPECT_EQ(cursor.next(ctx), ob::WALRecordCursor::Step::End);
    EXPECT_EQ(cursor.position().offset, tail.offset);
    append(60, record.size());                        // the rest
    ASSERT_EQ(cursor.next(ctx), ob::WALRecordCursor::Step::Record);
    EXPECT_EQ(ctx.header.sequence_number, 999u);
    EXPECT_EQ(ctx.wal_byte_offset, tail.offset);
    EXPECT_EQ(cursor.next(ctx), ob::WALRecordCursor::Step::End);
}

TEST(WALRecordCursor, ARotationWhoseNextFileIsNotThereYetIsAWait) {
    TempDir dir("rotate");
    write_mixed(dir.str(), 200, 4096);
    const size_t files = wal_files(dir.str());
    ASSERT_GE(files, 3u);
    const std::string last = (dir.path / wal_name(static_cast<uint32_t>(files - 1))).string();

    // Move the last file away: the one before it now ends with a ROTATE and nothing after it,
    // which is the moment between a rotation's ROTATE and the next file's creation.
    const std::string aside = (dir.path / "aside.bin").string();
    fs::rename(last, aside);
    const std::vector<Seen> without_last = replayed(dir.str());

    ob::WALRecordCursor cursor(dir.str());
    cursor.seek(ob::WalPosition{0, 0});
    EXPECT_EQ(drained(cursor), without_last);
    ob::WALReplayContext ctx;
    EXPECT_EQ(cursor.next(ctx), ob::WALRecordCursor::Step::End);

    fs::rename(aside, last);
    const std::vector<Seen> all = replayed(dir.str());
    std::vector<Seen> rest = drained(cursor);
    std::vector<Seen> joined = without_last;
    joined.insert(joined.end(), rest.begin(), rest.end());
    EXPECT_EQ(joined, all) << "the records of the file that appeared, and nothing twice";
}

TEST(WALRecordCursor, ATornRecordEndsAFileThatIsNotTheLastAsReplayV2Does) {
    TempDir dir("torn");
    write_mixed(dir.str(), 300, 4096, 2);
    ASSERT_GE(wal_files(dir.str()), 3u);

    // One byte of the payload of a record in the middle of the first file: its checksum no longer
    // matches. Found through the replayer, so the byte is a payload's and not a header's.
    std::vector<std::pair<uint64_t, size_t>> in_first;   // offset of the payload, its length
    {
        ob::WALReplayer scan(dir.str());
        scan.replay_v2([&](const ob::WALReplayContext& ctx) {
            if (ctx.wal_file_index != 0 || ctx.payload_len < 8) return;
            const size_t header = ctx.header._pad == 1 ? sizeof(ob::WALRecordV2) : sizeof(ob::WALRecord);
            in_first.emplace_back(ctx.wal_byte_offset + header, ctx.payload_len);
        });
    }
    ASSERT_GE(in_first.size(), 3u) << "the premise: records with payloads in the first file";
    const std::string first = (dir.path / "wal_000000.bin").string();
    {
        const int fd = ::open(first.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        const off_t at = static_cast<off_t>(in_first[in_first.size() / 2].first + 4);
        uint8_t byte = 0;
        ASSERT_EQ(::pread(fd, &byte, 1, at), 1);
        byte ^= 0xFF;
        ASSERT_EQ(::pwrite(fd, &byte, 1, at), 1);
        ::close(fd);
    }

    ob::WALReplayer replayer(dir.str());
    std::vector<Seen> expected;
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) { expected.push_back(seen(ctx)); });
    ASSERT_EQ(replayer.tears_skipped(), 1u) << "the premise: replay_v2 skipped the file's remainder";

    ob::WALRecordCursor cursor(dir.str());
    cursor.seek(ob::WalPosition{0, 0});
    EXPECT_EQ(drained(cursor), expected);
    EXPECT_EQ(cursor.tears_skipped(), 1u);
}

TEST(WALRecordCursor, FilesRetentionRemovedArePassedOver) {
    TempDir dir("gone");
    write_mixed(dir.str(), 300, 4096, 1);
    ASSERT_GE(wal_files(dir.str()), 4u);

    ob::WALRecordCursor cursor(dir.str());
    cursor.seek(ob::WalPosition{0, 0});
    ob::WALReplayContext ctx;
    ASSERT_EQ(cursor.next(ctx), ob::WALRecordCursor::Step::Record);
    const ob::WalPosition stood = cursor.position();

    fs::remove(dir.path / "wal_000000.bin");
    fs::remove(dir.path / "wal_000001.bin");
    const std::vector<Seen> left = replayed(dir.str());

    ob::WALRecordCursor again(dir.str());
    EXPECT_EQ(again.seek(stood), 2u) << "two file indexes passed over";
    EXPECT_EQ(again.position().file_index, 2u);
    EXPECT_EQ(again.position().offset, 0u);
    EXPECT_EQ(drained(again), left);

    // And one already reading when the files go: it has the first file open, finishes it, and
    // goes on with the first one that is still there.
    ob::WALRecordCursor reading(dir.str());
    reading.seek(ob::WalPosition{2, 0});
    EXPECT_EQ(drained(reading), left);
}

TEST(WALRecordCursor, AnEmptyOrMissingDirectoryIsAnEndThatLaterHasRecords) {
    TempDir dir("empty");
    ob::WALRecordCursor cursor(dir.str());
    EXPECT_EQ(cursor.seek(ob::WalPosition{0, 0}), 0u);
    ob::WALReplayContext ctx;
    EXPECT_EQ(cursor.next(ctx), ob::WALRecordCursor::Step::End);
    write_mixed(dir.str(), 5, 1 << 20);
    EXPECT_EQ(drained(cursor), replayed(dir.str()));
}
