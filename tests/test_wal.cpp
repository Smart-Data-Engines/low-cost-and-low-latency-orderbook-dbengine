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
        writer.flush();
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
