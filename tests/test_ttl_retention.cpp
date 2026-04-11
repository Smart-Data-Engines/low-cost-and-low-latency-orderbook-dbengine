// Tests for TTL configuration parsing (Task 7.2) and segment deletion (Tasks 8.3–8.5)
// Feature: compression-and-ttl
// Requirements: 5.1, 5.2, 5.3, 6.2, 6.3, 6.4, 6.5

#include "orderbook/columnar_store.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/tcp_server.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

// ── TTLConfigParsing: parse --ttl-hours 24 --ttl-scan-interval-seconds 60 ────
// Validates: Requirements 5.1, 5.3
TEST(TTLConfig, TTLConfigParsing) {
    std::vector<std::string> args_storage = {
        "ob_tcp_server",
        "--ttl-hours", "24",
        "--ttl-scan-interval-seconds", "60"
    };
    std::vector<char*> argv;
    for (auto& s : args_storage) argv.push_back(s.data());

    auto config = ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());

    EXPECT_EQ(config.ttl_hours, 24u);
    EXPECT_EQ(config.ttl_scan_interval_seconds, 60u);
}

// ── TTLConfigDefault: parse empty args, verify ttl_hours=0 ───────────────────
// Validates: Requirement 5.2
TEST(TTLConfig, TTLConfigDefault) {
    std::vector<std::string> args_storage = {"ob_tcp_server"};
    std::vector<char*> argv;
    for (auto& s : args_storage) argv.push_back(s.data());

    auto config = ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());

    EXPECT_EQ(config.ttl_hours, 0u);
    EXPECT_EQ(config.ttl_scan_interval_seconds, 300u);
}

// ── Helper: create a fake segment directory with meta.json ───────────────────
static void create_test_segment(const std::string& dir,
                                uint64_t start_ts, uint64_t end_ts,
                                const std::string& symbol = "SYM",
                                const std::string& exchange = "EX") {
    fs::create_directories(dir);
    // Write a minimal meta.json
    std::ofstream f(dir + "/meta.json", std::ios::out | std::ios::trunc);
    f << "{\"start_ts_ns\":" << start_ts
      << ",\"end_ts_ns\":" << end_ts
      << ",\"row_count\":10"
      << ",\"first_price\":100"
      << ",\"has_raw_qty\":false"
      << ",\"symbol\":\"" << symbol << "\""
      << ",\"exchange\":\"" << exchange << "\""
      << "}";
    f.flush();
    // Write a small dummy column file so directory has some size
    std::ofstream col(dir + "/price.col", std::ios::binary);
    uint64_t dummy[4] = {1, 2, 3, 4};
    col.write(reinterpret_cast<const char*>(dummy), sizeof(dummy));
}

// ── TTLDeleteExpired: create segments with known timestamps, delete with cutoff
// Validates: Requirements 6.2, 6.5
TEST(TTLDeletion, TTLDeleteExpired) {
    auto tmp = fs::temp_directory_path() / "ttl_delete_expired_test";
    fs::remove_all(tmp);

    ob::ColumnarStore store(tmp.string());

    // Create 3 segments: ts 100-200, 300-400, 500-600
    std::string dir1 = tmp.string() + "/SYM/EX/100_200";
    std::string dir2 = tmp.string() + "/SYM/EX/300_400";
    std::string dir3 = tmp.string() + "/SYM/EX/500_600";
    create_test_segment(dir1, 100, 200);
    create_test_segment(dir2, 300, 400);
    create_test_segment(dir3, 500, 600);

    store.open_existing();
    ASSERT_EQ(store.segment_count(), 3u);

    // Delete segments with end_ts_ns < 350 → should delete segment 100-200 only
    auto [deleted, reclaimed] = store.delete_expired_segments(350);
    EXPECT_EQ(deleted, 1u);
    EXPECT_GT(reclaimed, 0u);
    EXPECT_EQ(store.segment_count(), 2u);

    // Verify remaining segments
    const auto idx = store.index();
    EXPECT_EQ(idx[0].start_ts_ns, 300u);
    EXPECT_EQ(idx[1].start_ts_ns, 500u);

    // Verify directory was removed
    EXPECT_FALSE(fs::exists(dir1));
    EXPECT_TRUE(fs::exists(dir2));
    EXPECT_TRUE(fs::exists(dir3));

    fs::remove_all(tmp);
}

// ── TTLDeleteOldestFirst: create segments out of order, verify chronological deletion
// Validates: Requirement 6.3
TEST(TTLDeletion, TTLDeleteOldestFirst) {
    auto tmp = fs::temp_directory_path() / "ttl_delete_oldest_test";
    fs::remove_all(tmp);

    ob::ColumnarStore store(tmp.string());

    // Create segments out of order
    std::string dir3 = tmp.string() + "/SYM/EX/500_600";
    std::string dir1 = tmp.string() + "/SYM/EX/100_200";
    std::string dir2 = tmp.string() + "/SYM/EX/300_400";
    create_test_segment(dir3, 500, 600);
    create_test_segment(dir1, 100, 200);
    create_test_segment(dir2, 300, 400);

    store.open_existing();
    ASSERT_EQ(store.segment_count(), 3u);

    // Delete all segments with end_ts_ns < 700
    auto [deleted, reclaimed] = store.delete_expired_segments(700);
    EXPECT_EQ(deleted, 3u);
    EXPECT_EQ(store.segment_count(), 0u);

    // All directories should be gone
    EXPECT_FALSE(fs::exists(dir1));
    EXPECT_FALSE(fs::exists(dir2));
    EXPECT_FALSE(fs::exists(dir3));

    fs::remove_all(tmp);
}

// ── TTLDeleteFailure: make segment dir read-only, verify scanner continues
// Validates: Requirement 6.4
TEST(TTLDeletion, TTLDeleteFailure) {
    // Skip on root (permissions don't apply)
    if (geteuid() == 0) GTEST_SKIP() << "Skipping permission test as root";

    auto tmp = fs::temp_directory_path() / "ttl_delete_failure_test";
    fs::remove_all(tmp);

    ob::ColumnarStore store(tmp.string());

    std::string dir1 = tmp.string() + "/SYM/EX/100_200";
    std::string dir2 = tmp.string() + "/SYM/EX/300_400";
    create_test_segment(dir1, 100, 200);
    create_test_segment(dir2, 300, 400);

    store.open_existing();
    ASSERT_EQ(store.segment_count(), 2u);

    // Make dir1's parent non-writable so remove_all fails
    std::string parent = tmp.string() + "/SYM/EX";
    fs::permissions(parent, fs::perms::owner_read | fs::perms::owner_exec,
                    fs::perm_options::replace);

    // Delete with cutoff that covers both segments
    auto [deleted, reclaimed] = store.delete_expired_segments(500);

    // Both deletions should fail, segments remain in index
    EXPECT_EQ(deleted, 0u);
    EXPECT_EQ(store.segment_count(), 2u);

    // Restore permissions for cleanup
    fs::permissions(parent, fs::perms::owner_all, fs::perm_options::replace);
    fs::remove_all(tmp);
}

// ── TTLDisabledNoDelete: run with ttl_hours=0, verify no segments deleted
// Validates: Requirement 5.2
TEST(TTLDeletion, TTLDisabledNoDelete) {
    auto tmp = fs::temp_directory_path() / "ttl_disabled_test";
    fs::remove_all(tmp);

    ob::ColumnarStore store(tmp.string());

    std::string dir1 = tmp.string() + "/SYM/EX/100_200";
    std::string dir2 = tmp.string() + "/SYM/EX/300_400";
    create_test_segment(dir1, 100, 200);
    create_test_segment(dir2, 300, 400);

    store.open_existing();
    ASSERT_EQ(store.segment_count(), 2u);

    // cutoff_ns = 0 means TTL disabled → no deletions
    auto [deleted, reclaimed] = store.delete_expired_segments(0);
    EXPECT_EQ(deleted, 0u);
    EXPECT_EQ(reclaimed, 0u);
    EXPECT_EQ(store.segment_count(), 2u);

    fs::remove_all(tmp);
}

// ── Property 2: Expired segment deletion correctness ─────────────────────────
// Feature: compression-and-ttl, Property 2: Expired segment deletion correctness
// **Validates: Requirements 6.2, 6.3, 6.5**
RC_GTEST_PROP(TTLProperty, ExpiredSegmentDeletionCorrectness, ()) {
    // Generate 1–20 segments with random timestamps
    auto seg_count = *rc::gen::inRange(1, 21);
    struct SegSpec {
        uint64_t start_ts;
        uint64_t end_ts;
    };
    std::vector<SegSpec> specs;
    specs.reserve(static_cast<size_t>(seg_count));
    for (int i = 0; i < seg_count; ++i) {
        uint64_t s = *rc::gen::inRange<uint64_t>(1, 10000);
        uint64_t e = *rc::gen::inRange<uint64_t>(s, s + 5000);
        specs.push_back({s, e});
    }

    // Generate random cutoff
    uint64_t cutoff = *rc::gen::inRange<uint64_t>(1, 15000);

    // Create temp directory and segments
    static std::atomic<uint64_t> counter{0};
    auto unique_tmp = fs::temp_directory_path() / ("ttl_prop2_" + std::to_string(counter.fetch_add(1)));
    fs::remove_all(unique_tmp);

    ob::ColumnarStore store(unique_tmp.string());

    for (size_t i = 0; i < specs.size(); ++i) {
        std::string dir = unique_tmp.string() + "/SYM/EX/"
            + std::to_string(specs[i].start_ts) + "_"
            + std::to_string(specs[i].end_ts) + "_" + std::to_string(i);
        create_test_segment(dir, specs[i].start_ts, specs[i].end_ts);
    }

    store.open_existing();
    RC_ASSERT(store.segment_count() == static_cast<size_t>(seg_count));

    // Compute expected remaining segments
    std::vector<SegSpec> expected_remaining;
    for (const auto& sp : specs) {
        if (sp.end_ts >= cutoff) {
            expected_remaining.push_back(sp);
        }
    }
    std::sort(expected_remaining.begin(), expected_remaining.end(),
              [](const SegSpec& a, const SegSpec& b) {
                  return a.start_ts < b.start_ts;
              });

    auto [deleted, reclaimed] = store.delete_expired_segments(cutoff);

    // Assert correct count
    RC_ASSERT(store.segment_count() == expected_remaining.size());
    RC_ASSERT(deleted == static_cast<size_t>(seg_count) - expected_remaining.size());

    // Assert remaining segments match expected, sorted by start_ts_ns
    const auto idx = store.index();
    for (size_t i = 0; i < expected_remaining.size(); ++i) {
        RC_ASSERT(idx[i].start_ts_ns == expected_remaining[i].start_ts);
        RC_ASSERT(idx[i].end_ts_ns == expected_remaining[i].end_ts);
    }

    fs::remove_all(unique_tmp);
}

// ── Property 5: TTL=0 preserves all segments ────────────────────────────────
// Feature: compression-and-ttl, Property 5: TTL=0 preserves all segments
// **Validates: Requirements 5.2, 6.1**
RC_GTEST_PROP(TTLProperty, TTLZeroPreservesAllSegments, ()) {
    // Generate 1–20 segments with random timestamps
    auto seg_count = *rc::gen::inRange(1, 21);

    static std::atomic<uint64_t> counter5{0};
    auto unique_tmp = fs::temp_directory_path() / ("ttl_prop5_" + std::to_string(counter5.fetch_add(1)));
    fs::remove_all(unique_tmp);

    ob::ColumnarStore store(unique_tmp.string());

    for (int i = 0; i < seg_count; ++i) {
        uint64_t s = *rc::gen::inRange<uint64_t>(1, 10000);
        uint64_t e = *rc::gen::inRange<uint64_t>(s, s + 5000);
        std::string dir = unique_tmp.string() + "/SYM/EX/"
            + std::to_string(s) + "_" + std::to_string(e) + "_" + std::to_string(i);
        create_test_segment(dir, s, e);
    }

    store.open_existing();
    size_t original_count = store.segment_count();
    RC_ASSERT(original_count == static_cast<size_t>(seg_count));

    // Capture original index for comparison
    std::vector<ob::SegmentMeta> original_index = store.index();

    // cutoff = 0 → no deletions
    auto [deleted, reclaimed] = store.delete_expired_segments(0);

    RC_ASSERT(deleted == 0u);
    RC_ASSERT(reclaimed == 0u);
    RC_ASSERT(store.segment_count() == original_count);

    // Index should be unchanged
    const auto idx = store.index();
    for (size_t i = 0; i < original_count; ++i) {
        RC_ASSERT(idx[i].start_ts_ns == original_index[i].start_ts_ns);
        RC_ASSERT(idx[i].end_ts_ns == original_index[i].end_ts_ns);
    }

    fs::remove_all(unique_tmp);
}

// ── TTLStatusMetrics: verify STATUS includes ttl_hours, ttl_segments_deleted, ttl_bytes_reclaimed
// Validates: Requirement 8.1
TEST(TTLDeletion, TTLStatusMetrics) {
    // Create a ServerStats with TTL metrics populated
    ob::ServerStats stats;
    stats.ttl_hours            = 72;
    stats.ttl_segments_deleted = 5;
    stats.ttl_bytes_reclaimed  = 1048576;

    std::string response = ob::format_status(stats);

    // Verify TTL fields are present in the STATUS output
    EXPECT_NE(response.find("ttl_hours: 72"), std::string::npos);
    EXPECT_NE(response.find("ttl_segments_deleted: 5"), std::string::npos);
    EXPECT_NE(response.find("ttl_bytes_reclaimed: 1048576"), std::string::npos);
}

// ── TTLStatusMetricsDisabled: verify STATUS shows ttl_hours=0 when disabled
// Validates: Requirement 8.1
TEST(TTLDeletion, TTLStatusMetricsDisabled) {
    ob::ServerStats stats;
    // TTL disabled (defaults: ttl_hours=0, segments_deleted=0, bytes_reclaimed=0)

    std::string response = ob::format_status(stats);

    EXPECT_NE(response.find("ttl_hours: 0"), std::string::npos);
    EXPECT_NE(response.find("ttl_segments_deleted: 0"), std::string::npos);
    EXPECT_NE(response.find("ttl_bytes_reclaimed: 0"), std::string::npos);
}

} // namespace
