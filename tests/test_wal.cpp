// Tests for WAL Writer and Replayer: property-based tests (Properties 14–16)
// and unit tests.
// Feature: orderbook-dbengine

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "orderbook/data_model.hpp"
#include "orderbook/soa_buffer.hpp"
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

    writer.append_checkpoint(2345);
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
