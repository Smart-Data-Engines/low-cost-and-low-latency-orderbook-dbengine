// Tests for WALRecordV2: property-based test (Property 7) and unit tests
// for backward compatibility, mixed records, and corrupted extended headers.
// Feature: multi-master-replication

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
#include "orderbook/hlc.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/wal.hpp"

// ── RapidCheck Arbitrary for HLCTimestamp ──────────────────────────────────────

namespace rc {
template <>
struct Arbitrary<ob::HLCTimestamp> {
    static Gen<ob::HLCTimestamp> arbitrary() {
        return gen::build<ob::HLCTimestamp>(
            gen::set(&ob::HLCTimestamp::physical_ns, gen::arbitrary<uint64_t>()),
            gen::set(&ob::HLCTimestamp::logical, gen::arbitrary<uint16_t>()),
            gen::set(&ob::HLCTimestamp::node_id, gen::arbitrary<uint16_t>()));
    }
};
} // namespace rc

// ── Test helpers ──────────────────────────────────────────────────────────────

namespace {

// Create a unique temporary directory for each test.
static std::filesystem::path make_temp_dir(const std::string& suffix = "") {
    static std::atomic<uint64_t> counter{0};
    auto base = std::filesystem::temp_directory_path() /
                ("ob_wal_v2_test_" + suffix + "_" +
                 std::to_string(counter.fetch_add(1, std::memory_order_relaxed)));
    std::filesystem::create_directories(base);
    return base;
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

} // anonymous namespace

// ── Property 7: WAL record round-trip (origin + HLC) ─────────────────────────
// Feature: multi-master-replication, Property 7: WAL record round-trip (origin + HLC)
// For any DeltaUpdate, HLCTimestamp, and origin_node_id, append_with_origin
// followed by replay_v2 SHALL recover identical origin_node_id and HLCTimestamp.
// **Validates: Requirements 2.1, 2.2, 2.6, 4.2**
RC_GTEST_PROP(WALv2Property, prop_wal_record_roundtrip_origin_hlc, ()) {
    TempDir tmp("rt");

    const auto seq = *rc::gen::inRange<uint64_t>(1, 100000ULL);
    const auto price = *rc::gen::inRange<int64_t>(1, 1000000LL);
    const auto qty = *rc::gen::inRange<uint64_t>(1, 1000000ULL);
    const auto origin = *rc::gen::arbitrary<uint16_t>();
    const auto hlc_ts = *rc::gen::arbitrary<ob::HLCTimestamp>();

    ob::DeltaUpdate upd = make_delta(seq, ob::SIDE_BID, 1);
    ob::Level lv = make_level(price, qty, 1U);

    // Write with origin and HLC.
    {
        ob::WALWriter writer(tmp.str());
        writer.append_with_origin(upd, &lv, origin, hlc_ts);
        writer.flush();
    }

    // Replay with replay_v2 and verify origin + HLC.
    bool found = false;
    uint16_t recovered_origin = 0;
    ob::HLCTimestamp recovered_hlc{};

    {
        ob::WALReplayer replayer(tmp.str());
        replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
            if (ctx.header.record_type == ob::WAL_RECORD_DELTA &&
                ctx.header.sequence_number == seq) {
                found = true;
                recovered_origin = ctx.origin_node_id;
                recovered_hlc = ctx.hlc;
            }
        });
    }

    RC_ASSERT(found);
    RC_ASSERT(recovered_origin == origin);
    RC_ASSERT(recovered_hlc == hlc_ts);
}

// ── Unit test: backward compatibility ─────────────────────────────────────────
// Records written with old append() (version=0) should be readable by replay_v2
// with origin_node_id=0 and hlc=zero.
TEST(WALv2, BackwardCompatibility) {
    TempDir tmp("compat");

    // Write records using legacy append() (version=0).
    {
        ob::WALWriter writer(tmp.str());
        ob::Level lv = make_level(50000LL, 200ULL, 3U);
        writer.append(make_delta(1), &lv);
        writer.append(make_delta(2), &lv);
        writer.flush();
    }

    // Replay with replay_v2.
    int count = 0;
    ob::WALReplayer replayer(tmp.str());
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type == ob::WAL_RECORD_DELTA) {
            // Legacy records should have origin=0 and hlc=zero.
            EXPECT_EQ(ctx.origin_node_id, 0);
            EXPECT_TRUE(ctx.hlc.is_zero());
            EXPECT_EQ(ctx.header.sequence_number, static_cast<uint64_t>(count + 1));
            ++count;
        }
    });

    EXPECT_EQ(count, 2);
}

// ── Unit test: mixed records ──────────────────────────────────────────────────
// Mix of old (version=0) and new (version=1) records in the same WAL file.
TEST(WALv2, MixedRecords) {
    TempDir tmp("mixed");

    ob::HLCTimestamp hlc1{};
    hlc1.physical_ns = 1700000000000000000ULL;
    hlc1.logical = 42;
    hlc1.node_id = 3;

    ob::HLCTimestamp hlc2{};
    hlc2.physical_ns = 1700000000000000001ULL;
    hlc2.logical = 0;
    hlc2.node_id = 5;

    // Write a mix of legacy and extended records.
    {
        ob::WALWriter writer(tmp.str());
        ob::Level lv = make_level(10000LL, 100ULL, 1U);

        // Legacy record (version=0).
        writer.append(make_delta(1), &lv);

        // Extended record (version=1).
        writer.append_with_origin(make_delta(2), &lv, 7, hlc1);

        // Another legacy record.
        writer.append(make_delta(3), &lv);

        // Another extended record.
        writer.append_with_origin(make_delta(4), &lv, 12, hlc2);

        writer.flush();
    }

    // Replay with replay_v2 and verify each record.
    struct RecordInfo {
        uint64_t seq;
        uint16_t origin;
        ob::HLCTimestamp hlc;
    };
    std::vector<RecordInfo> records;

    ob::WALReplayer replayer(tmp.str());
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type == ob::WAL_RECORD_DELTA) {
            records.push_back({ctx.header.sequence_number,
                               ctx.origin_node_id, ctx.hlc});
        }
    });

    ASSERT_EQ(records.size(), 4);

    // Record 1: legacy (version=0) → origin=0, hlc=zero.
    EXPECT_EQ(records[0].seq, 1);
    EXPECT_EQ(records[0].origin, 0);
    EXPECT_TRUE(records[0].hlc.is_zero());

    // Record 2: extended (version=1) → origin=7, hlc=hlc1.
    EXPECT_EQ(records[1].seq, 2);
    EXPECT_EQ(records[1].origin, 7);
    EXPECT_EQ(records[1].hlc, hlc1);

    // Record 3: legacy (version=0) → origin=0, hlc=zero.
    EXPECT_EQ(records[2].seq, 3);
    EXPECT_EQ(records[2].origin, 0);
    EXPECT_TRUE(records[2].hlc.is_zero());

    // Record 4: extended (version=1) → origin=12, hlc=hlc2.
    EXPECT_EQ(records[3].seq, 4);
    EXPECT_EQ(records[3].origin, 12);
    EXPECT_EQ(records[3].hlc, hlc2);
}

// ── Unit test: corrupted extended header ──────────────────────────────────────
// Simulate a corrupted extended header (version=1 but truncated extended data).
// replay_v2 should skip the corrupted record and continue.
TEST(WALv2, CorruptedExtendedHeader) {
    TempDir tmp("corrupt");

    // First, write a valid extended record to get the file format right.
    ob::HLCTimestamp hlc1{};
    hlc1.physical_ns = 1700000000000000000ULL;
    hlc1.logical = 10;
    hlc1.node_id = 1;

    {
        ob::WALWriter writer(tmp.str());
        ob::Level lv = make_level(10000LL, 100ULL, 1U);

        // Write a valid extended record.
        writer.append_with_origin(make_delta(1), &lv, 5, hlc1);

        // Write another valid legacy record after it.
        writer.append(make_delta(2), &lv);

        writer.flush();
    }

    // Now corrupt the file: overwrite the first record's version byte to 1
    // but truncate the extended header data by overwriting part of it with
    // the payload start. We'll do this by creating a hand-crafted file.
    {
        // Create a new WAL file with a corrupted record.
        const std::string corrupt_dir = tmp.str() + "_corrupt";
        std::filesystem::create_directories(corrupt_dir);
        const std::string corrupt_path = corrupt_dir + "/wal_000000.bin";

        int fd = ::open(corrupt_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0);

        // Write a 24-byte header with version=1 (indicating extended format).
        ob::WALRecord hdr{};
        hdr.sequence_number = 99;
        hdr.timestamp_ns    = 99000;
        hdr.payload_len     = 0;
        hdr.record_type     = ob::WAL_RECORD_DELTA;
        hdr._pad            = 1; // version=1

        // Compute CRC for empty payload.
        hdr.checksum = ob::crc32c(nullptr, 0);

        ::write(fd, &hdr, sizeof(ob::WALRecord));
        // Do NOT write the 14 extended bytes — this simulates truncation.
        // The file ends here, so replay_v2 can't read the 14B extension.

        ::fsync(fd);
        ::close(fd);

        // Replay the corrupted file — should skip the corrupted record.
        int count = 0;
        ob::WALReplayer replayer(corrupt_dir);
        replayer.replay_v2([&](const ob::WALReplayContext& /*ctx*/) {
            ++count;
        });

        // The corrupted record should be skipped (and there's nothing after it).
        EXPECT_EQ(count, 0);

        std::filesystem::remove_all(corrupt_dir);
    }
}

// ── Unit test: replay_v2 returns correct last sequence number ─────────────────
TEST(WALv2, ReplayV2ReturnsLastSeq) {
    TempDir tmp("lastseq");

    ob::HLCTimestamp hlc1{};
    hlc1.physical_ns = 1000000000ULL;
    hlc1.logical = 1;
    hlc1.node_id = 1;

    {
        ob::WALWriter writer(tmp.str());
        ob::Level lv = make_level(10000LL, 100ULL, 1U);
        writer.append_with_origin(make_delta(10), &lv, 1, hlc1);
        writer.append_with_origin(make_delta(20), &lv, 2, hlc1);
        writer.append_with_origin(make_delta(30), &lv, 3, hlc1);
        writer.flush();
    }

    ob::WALReplayer replayer(tmp.str());
    uint64_t last_seq = replayer.replay_v2([](const ob::WALReplayContext& /*ctx*/) {});

    EXPECT_EQ(last_seq, 30);
}

// ── Unit test: set_origin_node_id / origin_node_id ────────────────────────────
TEST(WALv2, SetOriginNodeId) {
    TempDir tmp("origin");

    ob::WALWriter writer(tmp.str());
    EXPECT_EQ(writer.origin_node_id(), 0);

    writer.set_origin_node_id(42);
    EXPECT_EQ(writer.origin_node_id(), 42);
}
