// Tests for WAL Writer and Replayer: property-based tests (Properties 14–16)
// and unit tests.
// Feature: orderbook-dbengine

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "orderbook/data_model.hpp"
#include "orderbook/soa_buffer.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/wal.hpp"

// ── Test helpers ──────────────────────────────────────────────────────────────

namespace {

// Create a unique temporary directory for each test.
static std::filesystem::path make_temp_dir(const std::string& suffix = "") {
    static std::atomic<uint64_t> counter{0};
    auto base = std::filesystem::temp_directory_path() /
                ("ob_wal_test_" + suffix + "_" +
                 std::to_string(counter.fetch_add(1, std::memory_order_relaxed)));
    std::filesystem::create_directories(base);
    return base;
}

// Build a DeltaUpdate with a given sequence number and side.
static ob::DeltaUpdate make_delta(uint64_t seq, uint8_t side = ob::SIDE_BID,
                                   uint16_t n_levels = 1) {
    ob::DeltaUpdate upd{};
    std::strncpy(upd.symbol,   "TEST", sizeof(upd.symbol)   - 1);
    std::strncpy(upd.exchange, "EX",   sizeof(upd.exchange) - 1);
    upd.sequence_number = seq;
    upd.timestamp_ns    = seq * 1000ULL;
    upd.side            = side;
    upd.n_levels        = n_levels;
    return upd;
}

// Build a single Level.
static ob::Level make_level(int64_t price = 10000LL, uint64_t qty = 100ULL,
                             uint32_t cnt = 1U) {
    ob::Level lv{};
    lv.price = price;
    lv.qty   = qty;
    lv.cnt   = cnt;
    return lv;
}

// Count wal_*.bin files in a directory.
static size_t count_wal_files(const std::filesystem::path& dir) {
    size_t count = 0;
    for (auto& entry : std::filesystem::directory_iterator(dir)) {
        const std::string name = entry.path().filename().string();
        if (name.size() == 14 &&
            name.substr(0, 4) == "wal_" &&
            name.substr(10) == ".bin") {
            ++count;
        }
    }
    return count;
}

// RAII helper to remove a temp directory on destruction.
struct TempDir {
    std::filesystem::path path;
    explicit TempDir(const std::string& suffix = "")
        : path(make_temp_dir(suffix)) {}
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

} // anonymous namespace

// ── Property 15: WAL write-before-apply ordering ─────────────────────────────
// Feature: orderbook-dbengine, Property 15: WAL write-before-apply ordering
// For any delta update, the WAL record should be durably written before the
// update is visible in the SoA buffer.
// Validates: Requirements 8.1
RC_GTEST_PROP(WALProperty, prop_wal_write_before_apply, ()) {
    TempDir tmp("wba");

    const auto seq = *rc::gen::inRange<uint64_t>(1, 10000ULL);
    const auto price = *rc::gen::inRange<int64_t>(1, 1000000LL);
    const auto qty   = *rc::gen::inRange<uint64_t>(1, 1000000ULL);

    ob::DeltaUpdate upd = make_delta(seq, ob::SIDE_BID, 1);
    ob::Level lv = make_level(price, qty, 1U);

    // Step 1: write to WAL.
    {
        ob::WALWriter writer(tmp.str());
        writer.append(upd, &lv);
        // Asserted rather than cast away: this test then reads the file expecting the record to be
        // there, so a flush that failed would make the next assertion report the wrong thing.
        ASSERT_TRUE(writer.flush());
    }

    // Step 2: verify the WAL file contains the record BEFORE we apply to SoA.
    bool found_in_wal = false;
    {
        ob::WALReplayer replayer(tmp.str());
        replayer.replay([&](const ob::WALRecord& hdr, const uint8_t* /*payload*/) {
            if (hdr.record_type == ob::WAL_RECORD_DELTA &&
                hdr.sequence_number == seq) {
                found_in_wal = true;
            }
        });
    }
    RC_ASSERT(found_in_wal);

    // Step 3: now apply to SoA buffer.
    ob::SoABuffer buf{};
    buf.sequence_number.store(0, std::memory_order_relaxed);
    buf.bid.version.store(0, std::memory_order_relaxed);
    buf.ask.version.store(0, std::memory_order_relaxed);
    buf.bid.depth = 0;
    buf.ask.depth = 0;

    bool gap = false;
    ob::ob_status_t st = ob::apply_delta(buf, upd, &lv, gap);
    RC_ASSERT(st == ob::OB_OK);

    // The WAL record was confirmed present before the apply — ordering holds.
    RC_ASSERT(found_in_wal);
}

// ── Property 14: WAL crash recovery ──────────────────────────────────────────
// Feature: orderbook-dbengine, Property 14: WAL crash recovery
// For any set of delta updates written to the WAL before simulated termination,
// replaying the WAL should restore all those updates.
// Validates: Requirements 7.5, 8.3
RC_GTEST_PROP(WALProperty, prop_wal_crash_recovery, ()) {
    TempDir tmp("cr");

    const auto n = *rc::gen::inRange<int>(1, 20);

    // Generate n distinct sequence numbers and prices.
    std::vector<uint64_t> seqs;
    seqs.reserve(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
        seqs.push_back(static_cast<uint64_t>(i + 1));
    }

    std::vector<ob::Level> levels;
    levels.reserve(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
        levels.push_back(make_level(
            static_cast<int64_t>(i + 1) * 100LL,
            static_cast<uint64_t>(i + 1) * 10ULL,
            static_cast<uint32_t>(i + 1)));
    }

    // Write N updates to WAL (simulating normal operation before "crash").
    {
        ob::WALWriter writer(tmp.str());
        for (int i = 0; i < n; ++i) {
            ob::DeltaUpdate upd = make_delta(seqs[static_cast<size_t>(i)],
                                             ob::SIDE_BID, 1);
            writer.append(upd, &levels[static_cast<size_t>(i)]);
        }
        // Destructor calls fsync — simulates clean file close before crash.
    }

    // Replay and collect recovered sequence numbers.
    std::vector<uint64_t> recovered_seqs;
    {
        ob::WALReplayer replayer(tmp.str());
        replayer.replay([&](const ob::WALRecord& hdr, const uint8_t* /*payload*/) {
            if (hdr.record_type == ob::WAL_RECORD_DELTA) {
                recovered_seqs.push_back(hdr.sequence_number);
            }
        });
    }

    // All N updates must be recovered.
    RC_ASSERT(static_cast<int>(recovered_seqs.size()) == n);
    for (int i = 0; i < n; ++i) {
        RC_ASSERT(std::find(recovered_seqs.begin(), recovered_seqs.end(),
                            seqs[static_cast<size_t>(i)]) != recovered_seqs.end());
    }
}

// ── Property 16: WAL rotation ─────────────────────────────────────────────────
// Feature: orderbook-dbengine, Property 16: WAL rotation
// When WAL file exceeds rotation threshold, a new file is opened and old file
// remains intact.
// Validates: Requirements 8.4
RC_GTEST_PROP(WALProperty, prop_wal_rotation, ()) {
    TempDir tmp("rot");

    // Use a small threshold so rotation triggers quickly.
    // Each DELTA record is sizeof(WALRecord) + sizeof(DeltaUpdate) + sizeof(Level)
    // = 24 + 88 + 20 = 132 bytes.  Threshold of 200 bytes → rotates after ~1 record.
    const size_t threshold = 200;
    const auto n_records = *rc::gen::inRange<int>(3, 10);

    {
        ob::WALWriter writer(tmp.str(), threshold);
        for (int i = 0; i < n_records; ++i) {
            ob::DeltaUpdate upd = make_delta(static_cast<uint64_t>(i + 1),
                                             ob::SIDE_BID, 1);
            ob::Level lv = make_level(static_cast<int64_t>(i + 1) * 100LL,
                                      static_cast<uint64_t>(i + 1), 1U);
            writer.append(upd, &lv);
        }
    }

    // With threshold=200 and records of ~132 bytes, we expect multiple files.
    const size_t file_count = count_wal_files(tmp.path);
    RC_ASSERT(file_count >= size_t{2});

    // All files must be non-empty (old files remain intact).
    // The last file may be empty if it was just opened after rotation.
    // Collect files sorted by name; all except the last must be non-empty.
    std::vector<std::pair<std::string, uintmax_t>> file_sizes;
    for (auto& entry : std::filesystem::directory_iterator(tmp.path)) {
        const std::string name = entry.path().filename().string();
        if (name.size() == 14 &&
            name.substr(0, 4) == "wal_" &&
            name.substr(10) == ".bin") {
            file_sizes.emplace_back(name,
                std::filesystem::file_size(entry.path()));
        }
    }
    std::sort(file_sizes.begin(), file_sizes.end());
    // All files except the last must be non-empty (contain at least a ROTATE record).
    for (size_t i = 0; i + 1 < file_sizes.size(); ++i) {
        RC_ASSERT(file_sizes[i].second > uintmax_t{0});
    }

    // Replay must recover all DELTA records.
    int recovered = 0;
    {
        ob::WALReplayer replayer(tmp.str());
        replayer.replay([&](const ob::WALRecord& hdr, const uint8_t* /*payload*/) {
            if (hdr.record_type == ob::WAL_RECORD_DELTA) {
                ++recovered;
            }
        });
    }
    RC_ASSERT(recovered == n_records);
}

// ── Unit tests ────────────────────────────────────────────────────────────────

TEST(WAL, AppendReplayDelta) {
    TempDir tmp("ut_delta");

    ob::DeltaUpdate upd = make_delta(42, ob::SIDE_BID, 1);
    ob::Level lv = make_level(50000LL, 200ULL, 3U);

    {
        ob::WALWriter writer(tmp.str());
        writer.append(upd, &lv);
    }

    int delta_count = 0;
    uint64_t last_seq = 0;
    ob::WALReplayer replayer(tmp.str());
    last_seq = replayer.replay([&](const ob::WALRecord& hdr,
                                   const uint8_t* payload) {
        EXPECT_EQ(hdr.record_type, ob::WAL_RECORD_DELTA);
        EXPECT_EQ(hdr.sequence_number, 42ULL);
        EXPECT_GT(hdr.payload_len, 0);
        ASSERT_NE(payload, nullptr);

        // Deserialise DeltaUpdate from payload.
        ob::DeltaUpdate recovered_upd{};
        std::memcpy(&recovered_upd, payload, sizeof(ob::DeltaUpdate));
        EXPECT_EQ(recovered_upd.sequence_number, 42ULL);
        EXPECT_EQ(recovered_upd.side, ob::SIDE_BID);
        EXPECT_EQ(recovered_upd.n_levels, 1);

        // Deserialise Level.
        ob::Level recovered_lv{};
        std::memcpy(&recovered_lv,
                    payload + sizeof(ob::DeltaUpdate),
                    sizeof(ob::Level));
        EXPECT_EQ(recovered_lv.price, 50000LL);
        EXPECT_EQ(recovered_lv.qty,   200ULL);
        EXPECT_EQ(recovered_lv.cnt,   3U);

        ++delta_count;
    });

    EXPECT_EQ(delta_count, 1);
    EXPECT_EQ(last_seq, 42ULL);
}

TEST(WAL, AppendReplayGap) {
    TempDir tmp("ut_gap");

    {
        ob::WALWriter writer(tmp.str());
        writer.append_gap(99, 99000ULL);
    }

    int gap_count = 0;
    ob::WALReplayer replayer(tmp.str());
    replayer.replay([&](const ob::WALRecord& hdr, const uint8_t* /*payload*/) {
        if (hdr.record_type == ob::WAL_RECORD_GAP) {
            EXPECT_EQ(hdr.sequence_number, 99ULL);
            EXPECT_EQ(hdr.payload_len, 0);
            ++gap_count;
        }
    });

    EXPECT_EQ(gap_count, 1);
}

TEST(WAL, AppendReplayRotate) {
    TempDir tmp("ut_rotate");

    // Use a tiny threshold to force rotation after the first record.
    {
        ob::WALWriter writer(tmp.str(), 1); // threshold = 1 byte → always rotate
        ob::DeltaUpdate upd = make_delta(1, ob::SIDE_ASK, 1);
        ob::Level lv = make_level(20000LL, 50ULL, 1U);
        writer.append(upd, &lv);
        // After append, written_ >= threshold → rotate() is called automatically.
    }

    // There should be at least 2 WAL files.
    EXPECT_GE(count_wal_files(tmp.path), 2);

    // The first file must contain a ROTATE record.
    bool found_rotate = false;
    {
        // Read wal_000000.bin directly.
        const std::string first_file = tmp.str() + "/wal_000000.bin";
        int fd = ::open(first_file.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);

        ob::WALRecord hdr{};
        while (::read(fd, &hdr, sizeof(ob::WALRecord)) ==
               static_cast<ssize_t>(sizeof(ob::WALRecord))) {
            if (hdr.payload_len > 0) {
                // Skip payload.
                std::vector<uint8_t> payload(hdr.payload_len);
                ssize_t r = ::read(fd, payload.data(), hdr.payload_len);
                (void)r;
            }
            if (hdr.record_type == ob::WAL_RECORD_ROTATE) {
                found_rotate = true;
            }
        }
        ::close(fd);
    }
    EXPECT_TRUE(found_rotate);
}

TEST(WAL, ChecksumMismatchStopsReplay) {
    TempDir tmp("ut_csum");

    // Write two valid records.
    {
        ob::WALWriter writer(tmp.str());
        ob::Level lv1 = make_level(1000LL, 10ULL, 1U);
        ob::Level lv2 = make_level(2000LL, 20ULL, 1U);
        writer.append(make_delta(1), &lv1);
        writer.append(make_delta(2), &lv2);
    }

    // Corrupt the second record's checksum by flipping bytes in the file.
    {
        const std::string path = tmp.str() + "/wal_000000.bin";
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.is_open());

        // The second WALRecord starts at offset sizeof(WALRecord) + payload_of_first.
        // payload_of_first = sizeof(DeltaUpdate) + 1*sizeof(Level).
        const size_t first_payload = sizeof(ob::DeltaUpdate) + sizeof(ob::Level);
        const size_t second_hdr_offset = sizeof(ob::WALRecord) + first_payload;

        // Seek to the checksum field of the second header (offset 16 within WALRecord).
        f.seekp(static_cast<std::streamoff>(second_hdr_offset + 16));
        uint32_t bad_checksum = 0xDEADBEEFu;
        f.write(reinterpret_cast<const char*>(&bad_checksum), sizeof(bad_checksum));
    }

    // Replay should stop after the first valid record.
    int recovered = 0;
    uint64_t last_seq = 0;
    ob::WALReplayer replayer(tmp.str());
    last_seq = replayer.replay([&](const ob::WALRecord& hdr,
                                   const uint8_t* /*payload*/) {
        if (hdr.record_type == ob::WAL_RECORD_DELTA) {
            ++recovered;
        }
    });

    EXPECT_EQ(recovered, 1);
    EXPECT_EQ(last_seq, 1ULL);
}

TEST(WAL, RotationThresholdTriggersNewFile) {
    TempDir tmp("ut_rotthresh");

    // Each record is sizeof(WALRecord) + sizeof(DeltaUpdate) + sizeof(Level)
    // = 24 + 88 + 20 = 132 bytes.
    // Set threshold to 150 bytes → first record fits, second triggers rotation.
    const size_t threshold = 150;

    {
        ob::WALWriter writer(tmp.str(), threshold);
        ob::Level lv = make_level(1000LL, 10ULL, 1U);
        writer.append(make_delta(1), &lv);
        writer.append(make_delta(2), &lv);
        writer.append(make_delta(3), &lv);
    }

    // Expect at least 2 files.
    EXPECT_GE(count_wal_files(tmp.path), 2);

    // All DELTA records must be recoverable.
    int recovered = 0;
    ob::WALReplayer replayer(tmp.str());
    replayer.replay([&](const ob::WALRecord& hdr, const uint8_t* /*payload*/) {
        if (hdr.record_type == ob::WAL_RECORD_DELTA) ++recovered;
    });
    EXPECT_EQ(recovered, 3);
}

TEST(WAL, TruncateBeforeRemovesOldFiles) {
    TempDir tmp("ut_truncate");

    // Threshold of 150 bytes forces rotation after every 2nd record.
    // Record size = 24 (header) + 88 (DeltaUpdate) + 20 (Level) = 132 bytes.
    // After append: 132 bytes < 150 → no rotation.
    // After 2nd append: 264 bytes >= 150 → rotation → new file.
    const size_t threshold = 150;

    {
        ob::WALWriter writer(tmp.str(), threshold);
        ob::Level lv = make_level(1000LL, 10ULL, 1U);

        // Write 5 records to ensure the last file has at least one DELTA record.
        // File 0: record1 (132) + record2 (264 >= 150 → rotate) + ROTATE
        // File 1: record3 (132) + record4 (264 >= 150 → rotate) + ROTATE
        // File 2: record5 (132) — current file, has data
        writer.append(make_delta(1), &lv);
        writer.append(make_delta(2), &lv);
        writer.append(make_delta(3), &lv);
        writer.append(make_delta(4), &lv);
        writer.append(make_delta(5), &lv);

        const size_t files_before = count_wal_files(tmp.path);
        ASSERT_GE(files_before, 3) << "Need at least 3 WAL files for truncation test";

        const uint32_t current = writer.current_file_index();
        ASSERT_GT(current, 0) << "Current file index should be > 0 after rotations";

        // Truncate all files before the current one.
        size_t removed = writer.truncate_before(current);
        EXPECT_GT(removed, 0);

        const size_t files_after = count_wal_files(tmp.path);
        EXPECT_EQ(files_after, files_before - removed);
        EXPECT_GE(files_after, 1);
    }

    // Replay should recover record5 from the remaining current file.
    int recovered = 0;
    ob::WALReplayer replayer(tmp.str());
    replayer.replay([&](const ob::WALRecord& hdr, const uint8_t* /*payload*/) {
        if (hdr.record_type == ob::WAL_RECORD_DELTA) ++recovered;
    });
    EXPECT_GE(recovered, 1) << "At least the last record should be recoverable";
}

TEST(WAL, TruncateBeforeZeroRemovesNothing) {
    TempDir tmp("ut_truncate_zero");

    {
        ob::WALWriter writer(tmp.str(), 150);
        ob::Level lv = make_level();
        writer.append(make_delta(1), &lv);
        writer.append(make_delta(2), &lv);

        const size_t files_before = count_wal_files(tmp.path);
        size_t removed = writer.truncate_before(0);
        EXPECT_EQ(removed, 0);
        EXPECT_EQ(count_wal_files(tmp.path), files_before);
    }
}

// ── The WAL's own record count (#117) ────────────────────────────────────────
//
// `ob_wal_records_written` was registered from the beginning and written nowhere, so `/metrics`
// reported a flat zero for it while the WAL filled up. The counter lives in the one function both
// write paths now share, which is the part worth testing: a count kept in each path separately is
// a count the third path will not keep, and that is what #94 was.

TEST(WAL, EveryRecordTypeIsCounted) {
    TempDir tmp("rec_count");
    ob::DeltaUpdate upd = make_delta(1);
    const ob::Level lvl = make_level();

    ob::WALWriter writer(tmp.str());
    EXPECT_EQ(writer.records_written(), 0u) << "a fresh writer has written nothing";

    writer.append(upd, &lvl);
    EXPECT_EQ(writer.records_written(), 1u);

    // The bookkeeping records go through the other code paths — a v1 header without a payload, a
    // v1 header with one, and the v2 header the multi-master path uses. Each is one record, and
    // the point of asserting them individually is that they are separate functions: a count added
    // to `append()` alone would read 1 here and for ever.
    writer.append_gap(7, 1234);
    EXPECT_EQ(writer.records_written(), 2u);

    writer.append_checkpoint(2345, ob::WalPosition{});
    EXPECT_EQ(writer.records_written(), 3u);

    const uint8_t payload[] = {1, 2, 3, 4};
    writer.append_version_vector(payload, sizeof(payload));
    EXPECT_EQ(writer.records_written(), 4u);

    writer.append_held_sequences(payload, sizeof(payload));
    EXPECT_EQ(writer.records_written(), 5u);

    ob::DeltaUpdate remote = make_delta(2);
    writer.append_with_origin(remote, &lvl, /*origin_node_id=*/3, ob::HLCTimestamp{9, 0, 3});
    EXPECT_EQ(writer.records_written(), 6u)
        << "the v2 write path is a second function and must count through the same one";
}

// Rotation must not restart the count. The position does — it is a position *within a file* — and
// before this change the two were maintained by the same three lines in each write path, so the
// obvious way to write the counter leaves it sharing that reset.
//
// The exact total is asserted rather than a bound, and it is **not** the number of appends: each
// rotation writes a ROTATE record of its own, through the same function, so it is counted. That is
// the documented meaning of this metric and the reason it is worth having beside
// `ob_inserts_total` rather than being a duplicate of it — the difference is the bookkeeping the
// WAL does on its own behalf. The first version of this test asserted 40 and read 50, which is how
// the ROTATE records got into the description.
TEST(WAL, RotationDoesNotResetTheRecordCount) {
    TempDir tmp("rec_rotate");
    const ob::Level lvl = make_level();
    constexpr int kAppends = 40;

    // A threshold small enough that the file rotates several times inside the loop.
    ob::WALWriter writer(tmp.str(), /*rotate_threshold=*/512);
    uint64_t previous = 0;
    for (int i = 0; i < kAppends; ++i) {
        ob::DeltaUpdate upd = make_delta(static_cast<uint64_t>(i + 1));
        writer.append(upd, &lvl);
        const uint64_t now = writer.records_written();
        ASSERT_GT(now, previous) << "the count went backwards or stood still at append " << i;
        previous = now;
    }

    const uint32_t rotations = writer.current_file_index();
    ASSERT_GT(rotations, 0u) << "the threshold was too high for this test to have rotated at all";
    EXPECT_EQ(writer.records_written(), static_cast<uint64_t>(kAppends) + rotations)
        << "expected one record per append plus one ROTATE record per rotation";
}

// ── How far behind a position is, across files (#123) ───────────────────────
//
// `Engine::stats()` reported each replica's lag as `current_offset - confirmed_offset`. For a
// replica that is genuine arithmetic — a replica streams *our* WAL and refreshes its confirmed
// position on every ACK, so the two index the same log — and it ignores the file index. `rotate()`
// publishes `{next_index, next_offset}`, so the current offset **resets**: a replica still
// acknowledging into the previous file has the larger number, the expression clamps, and the lag
// reads **zero exactly when a replica is more than a file behind**.
//
// These tests measure that rather than describing it: each one computes the old expression beside
// the new answer, so the contrast is in the assertion instead of in a comment.

namespace {

/// What `Engine::stats()` used to report, kept here so the defect is a value rather than prose.
size_t old_lag_expression(size_t current_offset, size_t confirmed_offset) {
    return current_offset > confirmed_offset ? current_offset - confirmed_offset : 0;
}

} // namespace

TEST(WalDistance, WithinOneFileItIsTheDifference) {
    TempDir tmp("dist_same");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str(), /*rotate_threshold=*/1 << 20);

    ob::DeltaUpdate first = make_delta(1);
    const ob::WalPosition at = writer.append(first, &lvl);
    for (int i = 0; i < 5; ++i) {
        ob::DeltaUpdate upd = make_delta(static_cast<uint64_t>(i + 2));
        writer.append(upd, &lvl);
    }

    const auto distance = writer.bytes_since(at);
    ASSERT_TRUE(distance.has_value());
    EXPECT_EQ(*distance, writer.current_offset() - at.offset);
    // The control for the tests below: inside one file the old expression was right, which is why
    // the defect survived — every test that ever looked at this number stayed in one file.
    EXPECT_EQ(*distance, old_lag_expression(writer.current_offset(), at.offset));
}

// The defect, as a number.
//
// The position has to be **near the end** of the earlier file for the old expression to clamp,
// and that is the whole mechanism: after a rotation the current offset is small, so it is smaller
// than a confirmed offset from late in the previous file and the clamp answers zero. A position at
// the *start* of the earlier file does not reproduce it — the first version of this test used the
// first record, read 136 instead of 0, and its own control caught that it was proving nothing.
TEST(WalDistance, APositionLateInTheEarlierFileReadsAsZeroBehindTheOldWay) {
    TempDir tmp("dist_rotate");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str(), /*rotate_threshold=*/512);

    // Write until the last position still in file 0 is as late as it gets.
    ob::WalPosition late_in_first{};
    uint64_t seq = 1;
    while (writer.current_position().file_index == 0) {
        ob::DeltaUpdate upd = make_delta(seq++);
        const ob::WalPosition at = writer.append(upd, &lvl);
        if (at.file_index == 0) late_in_first = at;
    }
    ASSERT_GT(late_in_first.offset, 0u) << "no record landed late in the first file";

    // One more, so the current file has something in it and is clearly a different file.
    ob::DeltaUpdate upd = make_delta(seq++);
    writer.append(upd, &lvl);
    ASSERT_GT(writer.current_position().file_index, late_in_first.file_index);

    // The old expression, on exactly these two positions.
    const size_t old_answer = old_lag_expression(writer.current_offset(), late_in_first.offset);
    ASSERT_EQ(old_answer, 0u)
        << "this test no longer reproduces the defect it was written for, so the assertion below "
           "proves less than it claims: current_offset=" << writer.current_offset()
        << " confirmed_offset=" << late_in_first.offset;

    // And the answer.
    const auto distance = writer.bytes_since(late_in_first);
    ASSERT_TRUE(distance.has_value());
    EXPECT_GT(*distance, 0u) << "a position a whole file back is not zero bytes behind";
}

TEST(WalDistance, ItAccumulatesAcrossSeveralFiles) {
    TempDir tmp("dist_many");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str(), /*rotate_threshold=*/512);

    const ob::WalPosition start = writer.current_position();
    uint64_t seq = 1;
    for (int i = 0; i < 40; ++i) {
        ob::DeltaUpdate upd = make_delta(seq++);
        writer.append(upd, &lvl);
    }
    ASSERT_GT(writer.current_position().file_index, 2u) << "not enough rotations to be a test";

    // The current file must hold something, and this is asserted rather than assumed because the
    // first version of this test did not: forty 136-byte records against a 512-byte threshold
    // rotate on the last one, so the run ended with `now.offset == 0` and dropping the current
    // file's bytes from the sum changed **nothing**. A mutation doing exactly that survived, which
    // is how the gap was found.
    while (writer.current_position().offset == 0) {
        ob::DeltaUpdate upd = make_delta(seq++);
        writer.append(upd, &lvl);
    }
    ASSERT_GT(writer.current_position().offset, 0u);

    const auto distance = writer.bytes_since(start);
    ASSERT_TRUE(distance.has_value());

    // Checked against the files on disk rather than against the same arithmetic: summing the
    // sizes here the way the implementation does would make this a test of nothing.
    uint64_t on_disk = 0;
    for (const auto& entry : std::filesystem::directory_iterator(tmp.path)) {
        const std::string name = entry.path().filename().string();
        if (name.rfind("wal_", 0) == 0) on_disk += std::filesystem::file_size(entry.path());
    }
    EXPECT_EQ(*distance, on_disk) << "the distance from the beginning is the whole log";
}

// A position ahead of ours is not a negative distance. An ACK can be read after the value it
// acknowledges has been superseded, and a replica cannot be ahead of the log it follows.
TEST(WalDistance, APositionAheadOfUsIsZeroRatherThanUnderflow) {
    TempDir tmp("dist_ahead");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str());
    ob::DeltaUpdate upd = make_delta(1);
    writer.append(upd, &lvl);

    const ob::WalPosition current = writer.current_position();
    EXPECT_EQ(writer.bytes_since(ob::WalPosition{current.file_index + 1, 0}).value_or(1u), 0u);
    EXPECT_EQ(writer.bytes_since(ob::WalPosition{current.file_index,
                                                 current.offset + 100}).value_or(1u), 0u);
}

// A file that is gone makes the distance unknown, and that is a stronger statement than a large
// number: retention keeps files back to the slowest connected replica, so a missing one says that
// replica cannot catch up from this WAL any more.
TEST(WalDistance, AMissingFileIsUnknownRatherThanAGuess) {
    TempDir tmp("dist_gone");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str(), /*rotate_threshold=*/512);

    const ob::WalPosition start = writer.current_position();
    for (int i = 0; i < 40; ++i) {
        ob::DeltaUpdate upd = make_delta(static_cast<uint64_t>(i + 1));
        writer.append(upd, &lvl);
    }
    ASSERT_TRUE(writer.bytes_since(start).has_value()) << "the control: measurable before removal";

    // Remove one file in the middle, which is what retention would do if it ran past a replica.
    std::filesystem::remove(tmp.path / "wal_000001.bin");
    EXPECT_FALSE(writer.bytes_since(start).has_value());

    // And a position after the hole is still measurable, so "unknown" is about the span rather
    // than about the writer having given up.
    const ob::WalPosition after_hole{writer.current_position().file_index, 0};
    EXPECT_TRUE(writer.bytes_since(after_hole).has_value());
}

// The file the position itself sits in, which is a **different** code path from the files in
// between: the tail of the first one is measured before the loop starts. A mutation that made the
// first file's error path return zero instead of `nullopt` survived the test above, because that
// test removes an intervening file and never exercises the tail.
TEST(WalDistance, AMissingFirstFileIsUnknownToo) {
    TempDir tmp("dist_gone_first");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str(), /*rotate_threshold=*/512);

    ob::WalPosition in_first{};
    uint64_t seq = 1;
    while (writer.current_position().file_index == 0) {
        ob::DeltaUpdate upd = make_delta(seq++);
        const ob::WalPosition at = writer.append(upd, &lvl);
        if (at.file_index == 0) in_first = at;
    }
    for (int i = 0; i < 8; ++i) {
        ob::DeltaUpdate upd = make_delta(seq++);
        writer.append(upd, &lvl);
    }
    ASSERT_TRUE(writer.bytes_since(in_first).has_value()) << "the control: measurable before removal";

    std::filesystem::remove(tmp.path / "wal_000000.bin");
    EXPECT_FALSE(writer.bytes_since(in_first).has_value())
        << "the file the position sits in is gone and the distance was answered anyway";
}

// ── #126: a torn record in one file does not cost the files behind it ────────
//
// The writer abandons a file whose record it tore, without a ROTATE marker - it has just
// established that the file cannot be written to. What replaces the marker is the rule these tests
// pin: a checksum mismatch in a file that is **not** the last one is a tear, and replay continues
// with the next file; in the last file it is a crash tail, and replay stops.
//
// Both directions matter and the second is the control. Before this rule, `replay()` returned at
// the first mismatch **for the whole directory**, so the two writes acknowledged after a torn
// record did not survive a restart (measured 2 of 2). After it, a mismatch that really is a crash
// tail must still stop - a replayer that reads past one would hand the engine a record the process
// never finished writing.

namespace {

/// Append bytes to a WAL file by hand, which is the only way to produce a torn one on demand.
void append_raw(const std::string& dir, uint32_t index, const std::string& bytes) {
    char name[32];
    std::snprintf(name, sizeof(name), "wal_%06u.bin", index);
    std::ofstream out(dir + "/" + name, std::ios::binary | std::ios::app);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

/// The first twenty bytes of a record: a sequence number, a timestamp and a checksum, and then
/// nothing. Exactly what a write that failed after `20` bytes leaves behind, which is what the
/// fault injector produces and what #126 measured.
std::string twenty_stranded_bytes() {
    std::string bytes(20, '\0');
    for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = static_cast<char>(0xA5);
    return bytes;
}

/// Every record replay hands back, of any type.
///
/// Counting only `WAL_RECORD_DELTA` hid a mutation: a replayer that read **past** a checksum
/// mismatch hands on the garbled record, whose type byte is whatever the stranded bytes happened to
/// be, so a delta-only count stayed at the expected number and the mutation survived. These files
/// contain nothing but deltas, so any other type in the count is something the reader invented.
struct ReplayOutcome {
    size_t records = 0;
    size_t tears   = 0;
};

ReplayOutcome replay_outcome(const std::string& dir) {
    ReplayOutcome out;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext&) { ++out.records; });
    out.tears = replayer.tears_skipped();
    return out;
}

}  // namespace

TEST(WalTornRecord, AMismatchInAnEarlierFileDoesNotStopTheReplay) {
    TempDir tmp("torn_earlier");
    const ob::Level lvl = make_level();

    // File 0: two records, then the bytes a torn write leaves.
    {
        ob::WALWriter writer(tmp.str());
        ob::DeltaUpdate a = make_delta(1);
        ob::DeltaUpdate b = make_delta(2);
        writer.append(a, &lvl);
        writer.append(b, &lvl);
        ASSERT_TRUE(writer.flush());
    }
    // The stranded prefix, and then **a real record behind it** - which is what the engine wrote
    // before #126 and the only shape that reaches the checksum at all.
    //
    // Two earlier versions of this test passed without the rule it is for. With the file ending in
    // the 20 stranded bytes, the reader's next header read is short and the replayer has always
    // treated that as the end of *this* file and continued past it. With 136 bytes of `0x5A` behind
    // them, the assembled header claims a payload of 23 130 bytes, the payload read is short, and
    // the same path is taken. It takes a **real** record: its first four bytes are a small sequence
    // number, so the garbled header claims a four-byte payload, the read succeeds, and the checksum
    // is finally compared. A surviving mutation in the table said the test was passing for the
    // wrong reason; the control - running it with the rule removed - said so precisely.
    append_raw(tmp.str(), 0, twenty_stranded_bytes());
    {
        ob::WALWriter writer(tmp.str());   // continues from the highest index: file 0
        ob::DeltaUpdate stranded = make_delta(3);
        writer.append(stranded, &lvl);
        ASSERT_TRUE(writer.flush());
    }

    // File 1: the records the writer appended after abandoning file 0, produced the way the engine
    // produces them rather than fabricated. A `WALWriter` continues from the **highest existing
    // index**, so an empty `wal_000001.bin` is all it takes to put the next records there - and
    // deliberately no ROTATE record in file 0, because `rotate()` would write one and the replayer
    // would then stop at the marker rather than at the mismatch, which is a different case from the
    // one this test is about.
    append_raw(tmp.str(), 1, "");
    {
        ob::WALWriter writer(tmp.str());
        ob::DeltaUpdate c = make_delta(4);
        ob::DeltaUpdate d = make_delta(5);
        writer.append(c, &lvl);
        writer.append(d, &lvl);
        ASSERT_TRUE(writer.flush());
    }

    // Four: the two before the tear and the two in file 1. **Not five** - the record written into
    // file 0 behind the stranded bytes is unreachable, and that is not what this rule fixes: the
    // writer's half of #126 is what stops anything being put there in the first place.
    const ReplayOutcome out = replay_outcome(tmp.str());
    EXPECT_EQ(out.records, 4u)
        << "replay stopped at the torn record in file 0, so the two records in file 1 - which were "
           "acknowledged - did not come back";
    EXPECT_EQ(out.tears, 1u)
        << "replay did not report stepping over a tear, so it reached file 1 for some other reason "
           "than the rule this test is for";
}

TEST(WalTornRecord, AMismatchInTheLastFileStillStopsTheReplay) {
    // The control. Without it the rule above could be "ignore every mismatch", which would hand the
    // engine the tail of a record the process never finished writing.
    TempDir tmp("torn_last");
    const ob::Level lvl = make_level();
    {
        ob::WALWriter writer(tmp.str());
        ob::DeltaUpdate a = make_delta(1);
        ob::DeltaUpdate b = make_delta(2);
        writer.append(a, &lvl);
        writer.append(b, &lvl);
        ASSERT_TRUE(writer.flush());
    }
    // The same shape as the test above, for the same reason: only a **real** record behind the
    // stranded bytes makes the reader assemble a header it can parse, and only then is a checksum
    // compared at all. With 136 bytes of plausible garbage instead - what this test used to append -
    // the payload read runs short and the replayer ends the file without ever looking at a checksum,
    // so both assertions below held no matter what this rule did.
    append_raw(tmp.str(), 0, twenty_stranded_bytes());
    {
        ob::WALWriter writer(tmp.str());
        ob::DeltaUpdate stranded = make_delta(3);
        writer.append(stranded, &lvl);
        ASSERT_TRUE(writer.flush());
    }

    const ReplayOutcome out = replay_outcome(tmp.str());
    EXPECT_EQ(out.records, 2u)
        << "replay read past a checksum mismatch in the last WAL file, which is the crash tail: "
           "the record was being written when the process died";
    EXPECT_EQ(out.tears, 0u)
        << "the last file's mismatch was counted as a tear. It is not one: nothing follows it, and "
           "calling it a tear would tell an operator an older build had stranded a file";
}

// ── #153 and #154: a rotation that fails is not the failure of the record before it ──
//
// The integration battery holds the halves that need a failing disk (`test_storage_faults.py`);
// this one needs only a directory the writer cannot create a file in, so it runs everywhere.

namespace {

/// Takes the write bits off a directory for as long as it lives, and puts them back even when an
/// assertion leaves the scope early - a directory left read-only is one `TempDir` cannot remove.
struct ReadOnlyDir {
    std::filesystem::path path;
    std::filesystem::perms before;
    explicit ReadOnlyDir(const std::filesystem::path& p)
        : path(p), before(std::filesystem::status(p).permissions()) {
        std::filesystem::permissions(path,
                                     std::filesystem::perms::owner_write |
                                         std::filesystem::perms::group_write |
                                         std::filesystem::perms::others_write,
                                     std::filesystem::perm_options::remove);
    }
    ~ReadOnlyDir() {
        std::error_code ec;
        std::filesystem::permissions(path, before, ec);
    }
};

} // namespace

TEST(WalRotation, ANextFileThatCannotBeCreatedIsOpenedOnceItCan) {
    // Run as root, a read-only directory refuses nothing and this would measure nothing.
    ASSERT_NE(::geteuid(), 0u) << "this test cannot make the next WAL file uncreatable as root";

    TempDir tmp("ut_rot_open");
    const size_t threshold = 150;   // the first record fits, the second carries the file past it
    ob::Level lv = make_level(1000LL, 10ULL, 1U);
    {
        ob::WALWriter writer(tmp.str(), threshold);
        writer.append(make_delta(1), &lv);
        {
            ReadOnlyDir read_only(tmp.path);

            // Its record is written; the rotation after it cannot create wal_000001.bin, and that
            // is not this record's failure (#153).
            EXPECT_NO_THROW(writer.append(make_delta(2), &lv));

            // Nothing to write to now. The refusal names the file and the reason - before #154 it
            // was `write failed: Bad file descriptor`, for the rest of the process.
            try {
                writer.append(make_delta(3), &lv);
                ADD_FAILURE() << "a write with no WAL file to go to was accepted";
            } catch (const std::runtime_error& e) {
                const std::string what = e.what();
                EXPECT_NE(what.find("wal_000001.bin"), std::string::npos) << what;
                EXPECT_EQ(what.find("Bad file descriptor"), std::string::npos) << what;
            }
        }

        // The directory is writable again, so the next write opens the file the rotation could not.
        EXPECT_NO_THROW(writer.append(make_delta(4), &lv))
            << "the writer did not open its next file once it could (#154)";
        EXPECT_EQ(writer.current_file_index(), 1u);
    }

    // Replay reaches every record that was written, in order, across the file that ended with
    // its marker and the one opened late - and not the refused one.
    std::vector<uint64_t> seqs;
    ob::WALReplayer replayer(tmp.str());
    replayer.replay([&](const ob::WALRecord& hdr, const uint8_t* /*payload*/) {
        if (hdr.record_type == ob::WAL_RECORD_DELTA) seqs.push_back(hdr.sequence_number);
    });
    EXPECT_EQ(seqs, (std::vector<uint64_t>{1, 2, 4}));
}

// ── Stage 2b: a batch writes what the same records written one at a time wrote ──
//
// `append_batch()` exists to put a read's worth of records into the file with one `write()` per run
// instead of one per record. The claim it has to keep is that nothing else changes: the bytes, the
// positions, the rotations and the counts are the ones the single-record path produces, so a
// replayer, a replica streaming the file and a reader of the metrics cannot tell which path wrote
// it. That is asserted here the only way it can be - by writing the same records both ways into
// two directories and comparing them.

namespace {

/// One record of the comparison: its levels, whether it is a multi-master (V2) record, and whether
/// a GAP record goes in front of it, as the engine writes one when a stream skips a number.
struct BatchCase {
    ob::DeltaUpdate           update;
    std::vector<ob::Level>    levels;
    bool                      v2{false};
    ob::HLCTimestamp          hlc{};
    bool                      gap{false};
};

/// Records of every size the engine writes - one level, a MINSERT's twenty, a full thousand that
/// grows the batch buffer mid-run - with V1 and V2 interleaved, so the encoder is chosen per record,
/// and a GAP in front of five of them, one a thousand-level record. Where a GAP is itself the record
/// that crosses the threshold is a test of its own below, because only a threshold chosen to the
/// byte puts one there.
std::vector<BatchCase> mixed_cases() {
    std::vector<BatchCase> cases;
    const uint16_t widths[] = {1, 20, 1, 1000, 3, 20, 1, 1, 20, 1};
    uint64_t seq = 1;
    for (int round = 0; round < 3; ++round) {
        for (const uint16_t width : widths) {
            BatchCase c;
            c.update = make_delta(seq, (seq % 2) ? ob::SIDE_BID : ob::SIDE_ASK, width);
            for (uint16_t i = 0; i < width; ++i) {
                c.levels.push_back(make_level(10000LL + static_cast<int64_t>(seq) * 7 + i,
                                              100ULL + i, 1U + (i % 3)));
            }
            c.v2 = (seq % 3) == 0;
            c.hlc = ob::HLCTimestamp{1'700'000'000'000'000'000ULL + seq, static_cast<uint16_t>(seq),
                                     7};
            c.gap = (seq % 7) == 0 || seq == 14 + 10;   // seq 24 is a thousand-level record
            cases.push_back(std::move(c));
            ++seq;
        }
    }
    return cases;
}

/// Every WAL file in `dir`, by name, with its bytes.
std::map<std::string, std::string> wal_bytes(const std::filesystem::path& dir) {
    std::map<std::string, std::string> files;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        const std::string name = entry.path().filename().string();
        if (name.rfind("wal_", 0) != 0) continue;
        std::ifstream in(entry.path(), std::ios::binary);
        files[name] = std::string(std::istreambuf_iterator<char>(in), {});
    }
    return files;
}

} // namespace

TEST(WalBatch, TheBytesArePreciselyThoseOfTheSameRecordsWrittenOneAtATime) {
    // A threshold narrower than one thousand-level record (24 112 bytes), so each of those ends its
    // file: the batch has to cut a run there and rotate, the run it cuts holds several smaller
    // records before it, and the buffer grows in the middle of that run. 30 000 looked a few records
    // wide and gave two files, because a whole round of widths is 26 752 bytes.
    const size_t threshold = 10'000;
    const std::vector<BatchCase> cases = mixed_cases();

    TempDir one_at_a_time("ut_batch_single");
    TempDir batched("ut_batch_batched");

    std::vector<ob::WalPosition> single_positions;
    uint64_t single_counted = 0;
    {
        ob::WALWriter writer(one_at_a_time.str(), threshold);
        for (const BatchCase& c : cases) {
            if (c.gap) writer.append_gap(c.update.sequence_number, c.update.timestamp_ns);
            single_positions.push_back(
                c.v2 ? writer.append_with_origin(c.update, c.levels.data(), 7, c.hlc)
                     : writer.append(c.update, c.levels.data()));
        }
        single_counted = writer.records_written();
    }

    std::vector<ob::WalBatchOutcome> outcomes(cases.size());
    uint64_t batch_counted = 0;
    {
        ob::WALWriter writer(batched.str(), threshold);
        std::vector<ob::WalDelta> records;
        for (const BatchCase& c : cases) {
            records.push_back(ob::WalDelta{&c.update, c.levels.data(), c.v2 ? &c.hlc : nullptr,
                                           static_cast<uint16_t>(c.v2 ? 7 : 0), c.gap});
        }
        EXPECT_EQ(writer.append_batch(records, outcomes), cases.size());
        for (size_t i = 0; i < outcomes.size(); ++i) {
            EXPECT_TRUE(outcomes[i].error.empty()) << "record " << i << ": " << outcomes[i].error;
        }
        batch_counted = writer.records_written();
    }

    const auto single_files = wal_bytes(one_at_a_time.path);
    const auto batch_files  = wal_bytes(batched.path);
    ASSERT_GE(single_files.size(), 3u)
        << "the threshold did not make the records rotate, so the batch never had to cut a run";
    ASSERT_EQ(batch_files.size(), single_files.size());
    for (const auto& [name, bytes] : single_files) {
        ASSERT_TRUE(batch_files.count(name)) << name << " was written one at a time and not batched";
        EXPECT_EQ(batch_files.at(name), bytes) << name << " differs between the two paths";
    }

    ASSERT_EQ(outcomes.size(), single_positions.size());
    for (size_t i = 0; i < cases.size(); ++i) {
        EXPECT_EQ(outcomes[i].position.file_index, single_positions[i].file_index) << "record " << i;
        EXPECT_EQ(outcomes[i].position.offset, single_positions[i].offset) << "record " << i;
    }
    // The ROTATE markers and the GAP records are counted too, on both paths, so the totals agree
    // only if every run counted each of its records once.
    EXPECT_EQ(batch_counted, single_counted);
    size_t gaps = 0;
    for (const BatchCase& c : cases) gaps += c.gap ? 1 : 0;
    ASSERT_GE(gaps, 4u) << "the cases no longer put a GAP in front of any record";
}

TEST(WalBatch, AGapThatCrossesTheThresholdStaysInTheFileOfTheRecordItPrecedes) {
    // `append_gap()` does not check the rotation and the append after it does, so a GAP that
    // carries the file past the threshold is followed by its DELTA in the same file, and the file
    // rotates after that. A batch that cut its runs per WAL record rather than per batch record
    // would put the ROTATE marker between the two - a file that ends in a GAP, and a DELTA in the
    // next file that no longer follows the record announcing it.
    const ob::DeltaUpdate first  = make_delta(1);
    const ob::DeltaUpdate second = make_delta(5);
    const ob::Level level = make_level();
    const size_t one_record = sizeof(ob::WALRecord) + sizeof(ob::DeltaUpdate) + sizeof(ob::Level);
    const size_t threshold  = one_record + 10;   // after the first record, inside the GAP

    TempDir single("ut_batch_gap_single");
    {
        ob::WALWriter writer(single.str(), threshold);
        (void)writer.append(first, &level);
        ASSERT_LT(writer.current_position().offset, threshold);
        writer.append_gap(second.sequence_number, second.timestamp_ns);
        ASSERT_GE(writer.current_position().offset, threshold)
            << "the GAP did not cross the threshold, so this test would not be testing that";
        (void)writer.append(second, &level);
    }

    TempDir batched("ut_batch_gap_batched");
    {
        ob::WALWriter writer(batched.str(), threshold);
        const ob::WalDelta records[2] = {{&first, &level, nullptr, 0, false},
                                         {&second, &level, nullptr, 0, true}};
        ob::WalBatchOutcome outcomes[2]{};
        EXPECT_EQ(writer.append_batch(records, outcomes), 2u);
        EXPECT_EQ(outcomes[1].position.file_index, 0u) << "the DELTA went to the next file";
        EXPECT_EQ(outcomes[1].position.offset, one_record + sizeof(ob::WALRecord));
    }

    const auto single_files  = wal_bytes(single.path);
    const auto batched_files = wal_bytes(batched.path);
    ASSERT_EQ(single_files.size(), 2u);
    EXPECT_EQ(batched_files, single_files);
}

TEST(WalBatch, AnEmptyBatchWritesNothingAndIsNotAFailure) {
    TempDir tmp("ut_batch_empty");
    ob::WALWriter writer(tmp.str(), 1 << 20);
    EXPECT_EQ(writer.append_batch({}, {}), 0u);
    EXPECT_EQ(writer.current_position().offset, 0u);
    EXPECT_EQ(writer.records_written(), 0u);
}

TEST(WalBatch, TooFewOutcomesIsRefusedBeforeAnythingReachesTheFile) {
    // A caller that cannot be told where a record went must not have it written: the position is
    // what the replication wire sends with it (#98).
    TempDir tmp("ut_batch_outcomes");
    ob::WALWriter writer(tmp.str(), 1 << 20);
    const ob::DeltaUpdate update = make_delta(1);
    const ob::Level level = make_level();
    const ob::WalDelta records[2] = {{&update, &level, nullptr, 0, false},
                                     {&update, &level, nullptr, 0, false}};
    ob::WalBatchOutcome only_one[1]{};
    EXPECT_THROW(writer.append_batch(records, only_one), std::invalid_argument);
    EXPECT_EQ(writer.current_position().offset, 0u);
}

// ── #159: a checkpoint claims what its flush drained, not what the log held when it was written ──
//
// A flush drains the queued rows under the engine's lock, writes their segments without it, and
// only then appends the checkpoint. Writers go on appending while the segments are written, so
// the log holds records **between** the drain and the checkpoint whose rows are still queued.
// A checkpoint that meant "everything before me" had replay skip them, and a crash before the next
// flush lost every one, each answered `OK`. The checkpoint now carries the position its flush
// drained up to, and replay gives back what lies between that position and the checkpoint.
//
// The last test here is the one that decides whether the position can ever cost a record: a
// payload no writer produces - a position past the checkpoint - must take nothing away.

namespace {

/// Every record `replay_after_checkpoint` forwards, as (type, sequence), in order - of any type, so
/// a replayer that forwarded the checkpoint itself or skipped a bookkeeping record would show it.
std::vector<std::pair<uint8_t, uint64_t>> forwarded_after_checkpoint(const std::string& dir) {
    std::vector<std::pair<uint8_t, uint64_t>> out;
    ob::WALReplayer replayer(dir);
    replayer.replay_after_checkpoint([&](const ob::WALReplayContext& ctx) {
        out.emplace_back(ctx.header.record_type, ctx.header.sequence_number);
    });
    return out;
}

std::vector<std::pair<uint8_t, uint64_t>> deltas(std::initializer_list<uint64_t> seqs) {
    std::vector<std::pair<uint8_t, uint64_t>> out;
    for (uint64_t s : seqs) out.emplace_back(ob::WAL_RECORD_DELTA, s);
    return out;
}

/// The CHECKPOINT a build before #159 wrote: a 24-byte header and no payload, so it says nothing
/// about what it covered. Written by hand, because the writer no longer produces one.
std::string checkpoint_from_an_older_build(uint64_t timestamp_ns) {
    ob::WALRecord hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = timestamp_ns;
    hdr.checksum        = ob::crc32c(nullptr, 0);
    hdr.payload_len     = 0;
    hdr.record_type     = ob::WAL_RECORD_CHECKPOINT;
    hdr._pad            = 0;
    return std::string(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
}

}  // namespace

TEST(CheckpointPayload, APositionIsEightBytesTheFileIndexThenTheOffsetBothLittleEndian) {
    // The layout is asserted byte for byte, not only round-tripped: a payload read back by the
    // function that wrote it would agree with itself in any byte order, and the order is what an
    // older or newer build reading this directory depends on.
    uint8_t bytes[ob::CHECKPOINT_PAYLOAD_BYTES]{};
    ob::checkpoint_payload(ob::WalPosition{0x01020304u, 0x0A0B0C0Du}, bytes);
    const uint8_t expected[] = {0x04, 0x03, 0x02, 0x01, 0x0D, 0x0C, 0x0B, 0x0A};
    EXPECT_EQ(0, std::memcmp(bytes, expected, sizeof(expected)));

    const auto covered = ob::checkpoint_covered(bytes, sizeof(bytes));
    ASSERT_TRUE(covered.has_value());
    EXPECT_EQ(covered->file_index, 0x01020304u);
    EXPECT_EQ(covered->offset, 0x0A0B0C0Du);
}

TEST(CheckpointPayload, AnythingButEightBytesSaysNothing) {
    // The empty payload of an older build, and any other length, is "nothing said" - not a position
    // decoded from whatever bytes happen to be there, which could be anywhere in the log.
    const uint8_t bytes[9] = {1, 0, 0, 0, 2, 0, 0, 0, 3};
    EXPECT_FALSE(ob::checkpoint_covered(nullptr, 0).has_value());
    EXPECT_FALSE(ob::checkpoint_covered(bytes, 0).has_value());
    EXPECT_FALSE(ob::checkpoint_covered(bytes, 7).has_value());
    EXPECT_FALSE(ob::checkpoint_covered(bytes, 9).has_value());
    EXPECT_FALSE(ob::checkpoint_covered(nullptr, 8).has_value());
    EXPECT_TRUE(ob::checkpoint_covered(bytes, 8).has_value());
}

TEST(WalCheckpoint, TheRecordsBetweenTheDrainAndTheCheckpointAreGivenBack) {
    TempDir tmp("ckpt_given_back");
    const ob::Level lvl = make_level();
    {
        ob::WALWriter writer(tmp.str());
        writer.append(make_delta(1), &lvl);                 // drained: its rows are in segments
        const ob::WalPosition drained = writer.current_position();
        writer.append(make_delta(2), &lvl);                 // appended while the segments were
        writer.append(make_delta(3), &lvl);                 // being written: rows still queued
        writer.append_checkpoint(1'000, drained);
        writer.append(make_delta(4), &lvl);
    }
    EXPECT_EQ(forwarded_after_checkpoint(tmp.str()), deltas({2, 3, 4}))
        << "the records the checkpoint's flush did not drain must be replayed, and nothing else - "
           "neither the one it drained nor the checkpoint itself";
}

TEST(WalCheckpoint, ACheckpointWrittenWithNothingAfterTheDrainCoversEverythingBeforeIt) {
    // The control for the test above: a flush during which nobody wrote is the old case, and it
    // must read exactly as it always did.
    TempDir tmp("ckpt_quiet");
    const ob::Level lvl = make_level();
    {
        ob::WALWriter writer(tmp.str());
        writer.append(make_delta(1), &lvl);
        writer.append(make_delta(2), &lvl);
        writer.append_checkpoint(1'000, writer.current_position());
        writer.append(make_delta(3), &lvl);
    }
    EXPECT_EQ(forwarded_after_checkpoint(tmp.str()), deltas({3}));
}

TEST(WalCheckpoint, ACheckpointFromAnOlderBuildIsReadAsCoveringEverythingBeforeIt) {
    // Compatibility, and the limit of it: the records such a checkpoint wrongly covered are not in
    // the log's account of itself, so they cannot be told apart and are not replayed.
    TempDir tmp("ckpt_legacy");
    const ob::Level lvl = make_level();
    {
        ob::WALWriter writer(tmp.str());
        writer.append(make_delta(1), &lvl);
        writer.append(make_delta(2), &lvl);
    }
    append_raw(tmp.str(), 0, checkpoint_from_an_older_build(1'000));
    {
        ob::WALWriter writer(tmp.str());
        writer.append(make_delta(3), &lvl);
    }
    EXPECT_EQ(forwarded_after_checkpoint(tmp.str()), deltas({3}));
}

TEST(WalCheckpoint, OnlyTheLastCheckpointDecides) {
    const ob::Level lvl = make_level();

    // A positioned checkpoint followed by an older build's: the last one says nothing, so it is
    // read by ordinal - the first one's position must not reach past it and give records back.
    TempDir older_last("ckpt_older_last");
    {
        ob::WALWriter writer(older_last.str());
        const ob::WalPosition at_start = writer.current_position();
        writer.append(make_delta(1), &lvl);
        writer.append_checkpoint(1'000, at_start);
        writer.append(make_delta(2), &lvl);
    }
    append_raw(older_last.str(), 0, checkpoint_from_an_older_build(2'000));
    {
        ob::WALWriter writer(older_last.str());
        writer.append(make_delta(3), &lvl);
    }
    EXPECT_EQ(forwarded_after_checkpoint(older_last.str()), deltas({3}));

    // And the other way round: an older build's checkpoint, then a positioned one.
    TempDir positioned_last("ckpt_positioned_last");
    {
        ob::WALWriter writer(positioned_last.str());
        writer.append(make_delta(1), &lvl);
    }
    append_raw(positioned_last.str(), 0, checkpoint_from_an_older_build(1'000));
    {
        ob::WALWriter writer(positioned_last.str());
        writer.append(make_delta(2), &lvl);
        const ob::WalPosition drained = writer.current_position();
        writer.append(make_delta(3), &lvl);
        writer.append_checkpoint(2'000, drained);
        writer.append(make_delta(4), &lvl);
    }
    EXPECT_EQ(forwarded_after_checkpoint(positioned_last.str()), deltas({3, 4}));
}

TEST(WalCheckpoint, APositionPastTheCheckpointTakesNothingAway) {
    // No writer produces this - the drain always precedes its checkpoint - but a payload is bytes
    // on a disk, and the rule is that a position can only give records back. Read as covering
    // everything before the checkpoint, which is the most any checkpoint ever claimed.
    TempDir tmp("ckpt_future");
    const ob::Level lvl = make_level();
    {
        ob::WALWriter writer(tmp.str());
        writer.append(make_delta(1), &lvl);
        writer.append_checkpoint(1'000, ob::WalPosition{7, 0});
        writer.append(make_delta(2), &lvl);
        writer.append(make_delta(3), &lvl);
    }
    EXPECT_EQ(forwarded_after_checkpoint(tmp.str()), deltas({2, 3}));
}

TEST(WalCheckpoint, TheDrainsPositionMayBeInAnEarlierFileThanTheCheckpoint) {
    // A rotation between the drain and the checkpoint puts them in different files, and the
    // records given back span both. This is also the shape in which the flush tick's retention
    // used to delete the earlier file - see Engine::flush_loop and roadmap #159.
    TempDir tmp("ckpt_rotation");
    const ob::Level lvl = make_level();
    std::vector<uint64_t> expected;
    {
        ob::WALWriter writer(tmp.str(), /*rotate_threshold=*/512);
        writer.append(make_delta(1), &lvl);
        const ob::WalPosition drained = writer.current_position();
        uint64_t seq = 2;
        while (writer.current_position().file_index == drained.file_index) {
            writer.append(make_delta(seq), &lvl);
            expected.push_back(seq++);
        }
        writer.append(make_delta(seq), &lvl);                // one in the next file, before it
        expected.push_back(seq++);
        writer.append_checkpoint(1'000, drained);
        writer.append(make_delta(seq), &lvl);
        expected.push_back(seq);
        ASSERT_GT(writer.current_position().file_index, drained.file_index)
            << "the premise: the checkpoint is in a later file than the drain's position";
    }
    std::vector<std::pair<uint8_t, uint64_t>> want;
    for (uint64_t s : expected) want.emplace_back(ob::WAL_RECORD_DELTA, s);
    EXPECT_EQ(forwarded_after_checkpoint(tmp.str()), want);
}

// ── A sync performed without the writer's lock (stage 5 of #151) ─────────────
//
// The flush tick used to sync the WAL while holding the engine's lock, and every writer waited for
// the `fsync` - 7.9 ms at p50, 25.8 at worst on the m9g.xlarge. The ticket splits it: taken under
// the lock, performed without it, accounted for under it again. What these pin is the arithmetic of
// that split and the descriptor it lives on; that the tick really runs the middle step unlocked is
// `FlushTickStatic` in test_write_batch.cpp, and what a writer sees is test_storage_faults.py.

namespace {

/// Descriptors this process has open, so a ticket that forgets to close its `dup()` is a number
/// that moved rather than a leak nobody sees until the table is full.
std::size_t open_descriptors() {
    std::size_t n = 0;
    for ([[maybe_unused]] const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        ++n;
    }
    return n;
}

} // namespace

TEST(WalSyncTicket, ItCoversWhatWasWrittenBeforeItAndLeavesTheRestOwed) {
    TempDir tmp("ticket_cover");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str(), 1 << 20, ob::FsyncPolicy::INTERVAL);
    for (uint64_t s = 1; s <= 3; ++s) {
        ob::DeltaUpdate upd = make_delta(s);
        writer.append(upd, &lvl);
    }
    const ob::WalPosition before = writer.current_position();

    auto [ticket, err] = writer.prepare_sync();
    ASSERT_EQ(err, 0);
    ASSERT_TRUE(ticket.owed());
    EXPECT_EQ(ticket.position().file_index, before.file_index);
    EXPECT_EQ(ticket.position().offset, before.offset);

    // Written while the sync would be running: after the ticket's position, so not its to pay.
    for (uint64_t s = 4; s <= 5; ++s) {
        ob::DeltaUpdate upd = make_delta(s);
        writer.append(upd, &lvl);
    }
    const int performed = writer.perform_sync(ticket);
    ASSERT_EQ(performed, 0);
    EXPECT_FALSE(ticket.owed()) << "a performed ticket still holds its descriptor";
    writer.complete_sync(ticket, performed);
    EXPECT_EQ(writer.pending_sync_count(), 2u)
        << "the records written after the ticket must still be owed a sync";
}

TEST(WalSyncTicket, NothingIsOwedWhereThePolicyOrTheLogOwesNothing) {
    const ob::Level lvl = make_level();
    {
        TempDir tmp("ticket_every");
        ob::WALWriter writer(tmp.str(), 1 << 20, ob::FsyncPolicy::EVERY);
        ob::DeltaUpdate upd = make_delta(1);
        writer.append(upd, &lvl);
        EXPECT_FALSE(writer.prepare_sync().first.owed()) << "`every` has synced the run already";
    }
    {
        TempDir tmp("ticket_none");
        ob::WALWriter writer(tmp.str(), 1 << 20, ob::FsyncPolicy::NONE);
        ob::DeltaUpdate upd = make_delta(1);
        writer.append(upd, &lvl);
        EXPECT_FALSE(writer.prepare_sync().first.owed()) << "`none` is never synced by the tick";
    }
    {
        TempDir tmp("ticket_empty");
        ob::WALWriter writer(tmp.str(), 1 << 20, ob::FsyncPolicy::INTERVAL);
        EXPECT_FALSE(writer.prepare_sync().first.owed()) << "nothing written, nothing owed";
    }
    // The control: the same writer owes one as soon as something is written.
    TempDir tmp("ticket_control");
    ob::WALWriter writer(tmp.str(), 1 << 20, ob::FsyncPolicy::INTERVAL);
    ob::DeltaUpdate upd = make_delta(1);
    writer.append(upd, &lvl);
    EXPECT_TRUE(writer.prepare_sync().first.owed());
}

TEST(WalSyncTicket, ARotationBetweenTakingAndPerformingItClosesNothingOfTheTickets) {
    // The writer closes its own descriptor when it rotates. The ticket's is a duplicate of it, so it
    // still names the file the covered records are in - and it is the ticket's to close.
    TempDir tmp("ticket_rotate");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str(), /*rotate_threshold=*/512, ob::FsyncPolicy::INTERVAL);
    ob::DeltaUpdate first = make_delta(1);
    writer.append(first, &lvl);

    const std::size_t descriptors = open_descriptors();
    auto [ticket, err] = writer.prepare_sync();
    ASSERT_EQ(err, 0);
    ASSERT_TRUE(ticket.owed());
    const uint32_t file = ticket.position().file_index;
    for (uint64_t s = 2; writer.current_position().file_index == file; ++s) {
        ob::DeltaUpdate upd = make_delta(s);
        writer.append(upd, &lvl);
        ASSERT_LT(s, 100u) << "the writer never rotated, so this test measured nothing";
    }
    EXPECT_EQ(writer.perform_sync(ticket), 0) << "the ticket's descriptor went with the rotation";
    EXPECT_EQ(open_descriptors(), descriptors) << "the ticket leaked its duplicate descriptor";
}

TEST(WalSyncTicket, ADroppedTicketClosesItsDescriptorAndOwesStay) {
    TempDir tmp("ticket_drop");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str(), 1 << 20, ob::FsyncPolicy::INTERVAL);
    ob::DeltaUpdate upd = make_delta(1);
    writer.append(upd, &lvl);
    const std::size_t descriptors = open_descriptors();
    {
        auto prepared = writer.prepare_sync();
        ASSERT_TRUE(prepared.first.owed());
        EXPECT_EQ(open_descriptors(), descriptors + 1) << "a ticket that is owed holds a descriptor";
    }
    EXPECT_EQ(open_descriptors(), descriptors) << "a ticket dropped unperformed kept its descriptor";
    EXPECT_EQ(writer.pending_sync_count(), 1u) << "a dropped ticket paid for a sync it never did";
}

TEST(WalSyncTicket, AFailedSyncLeavesItsRecordsOwed) {
    // `perform_sync()` failing needs the injector, which test_storage_faults.py loads; what is
    // pinned here is the accounting that follows it, which is the half that decides whether the
    // flush tick drains rows whose records never reached the disk.
    TempDir tmp("ticket_fail");
    const ob::Level lvl = make_level();
    ob::WALWriter writer(tmp.str(), 1 << 20, ob::FsyncPolicy::INTERVAL);
    for (uint64_t s = 1; s <= 3; ++s) {
        ob::DeltaUpdate upd = make_delta(s);
        writer.append(upd, &lvl);
    }
    auto [ticket, err] = writer.prepare_sync();
    ASSERT_EQ(err, 0);
    writer.complete_sync(ticket, EIO);
    EXPECT_EQ(writer.pending_sync_count(), 3u) << "a failed sync was accounted as a paid one";
    writer.complete_sync(ticket, 0);
    EXPECT_EQ(writer.pending_sync_count(), 0u) << "the control: success pays what the ticket covered";
}

TEST(WalSyncStatic, PerformingATicketTouchesNothingOfTheWritersButTheFailureCount) {
    // `perform_sync()` runs without the lock that serialises this writer, so everything it reads
    // or writes of the writer has to be safe to touch from another thread: its failure counter is
    // an atomic, and nothing else may be named. Read from the source because a race here is one
    // TSan sees only on the interleaving that produces it.
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/src/wal.cpp");
    const std::string src((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_FALSE(src.empty()) << "cannot read src/wal.cpp";
    for (const char* signature : {"int WALWriter::perform_sync(", "int WALWriter::fsync_fd_or_record("}) {
        const std::size_t at = src.find(signature);
        ASSERT_NE(at, std::string::npos) << signature << " moved";
        std::size_t pos = src.find('{', at);
        int depth = 0;
        const std::size_t start = pos;
        for (; pos < src.size(); ++pos) {
            if (src[pos] == '{') ++depth;
            if (src[pos] == '}' && --depth == 0) break;
        }
        std::string body = src.substr(start, pos - start + 1);
        // Line comments out: the bodies explain themselves in prose that names the members.
        for (std::size_t c = body.find("//"); c != std::string::npos; c = body.find("//", c)) {
            body.erase(c, body.find('\n', c) - c);
        }
        for (const char* member : {"fd_", "pending_sync_", "position_", "fsync_policy_", "dir_"}) {
            const std::string m(member);
            for (std::size_t hit = body.find(m); hit != std::string::npos; hit = body.find(m, hit + 1)) {
                const char before = hit == 0 ? ' ' : body[hit - 1];
                const bool bare = before != '.' && before != '_' && !std::isalnum(static_cast<unsigned char>(before));
                const char after = hit + m.size() < body.size() ? body[hit + m.size()] : ' ';
                const bool whole = after != '_' && !std::isalnum(static_cast<unsigned char>(after));
                EXPECT_FALSE(bare && whole)
                    << signature << " touches the writer's `" << member << "` without its lock";
            }
        }
    }
}
