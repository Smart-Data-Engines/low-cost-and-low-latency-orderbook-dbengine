// Tests for TTL configuration parsing (Task 7.2) and segment deletion (Tasks 8.3–8.5)
// Feature: compression-and-ttl
// Requirements: 5.1, 5.2, 5.3, 6.2, 6.3, 6.4, 6.5

#include "orderbook/columnar_store.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/tcp_server.hpp"
#include "orderbook/wall_clock.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
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
    // Same total order the store uses. Comparing element-wise against a sort keyed
    // on start_ts alone made this test fail about one run in three: two segments can
    // share a start timestamp, std::sort is not stable, and each side broke the tie
    // its own way — reported as `304 == 303`.
    std::sort(expected_remaining.begin(), expected_remaining.end(),
              [](const SegSpec& a, const SegSpec& b) {
                  if (a.start_ts != b.start_ts) return a.start_ts < b.start_ts;
                  return a.end_ts < b.end_ts;
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

// ── The cutoff and the clock it is on (#163) ─────────────────────────────────
//
// The sweep compared event times with a count from this machine's boot. `ttl_cutoff_ns()` takes the
// wall clock as its argument, which is what these pin, and the sweep tests below are what show the
// engine hands it the wall clock rather than anything else.

constexpr uint64_t kNsPerHour = 3600ULL * 1'000'000'000ULL;

static_assert(ob::ttl_cutoff_ns(10 * kNsPerHour, 1) == 9 * kNsPerHour);
static_assert(ob::ttl_cutoff_ns(10 * kNsPerHour, 0) == 0, "0 keeps everything, as the flag says");

TEST(TTLCutoff, TheCutoffIsTheWallClockMinusTheRetention) {
    const uint64_t now = 1'790'000'000'000'000'000ULL;   // September 2026
    EXPECT_EQ(ob::ttl_cutoff_ns(now, 24), now - 24 * kNsPerHour);
    EXPECT_EQ(ob::ttl_cutoff_ns(now, 1), now - kNsPerHour);
}

TEST(TTLCutoff, ARetentionOfZeroExpiresNothing) {
    EXPECT_EQ(ob::ttl_cutoff_ns(1'790'000'000'000'000'000ULL, 0), 0u);
}

TEST(TTLCutoff, ARetentionReachingPastTheEpochExpiresNothingRatherThanWrapping) {
    // The shape of #163 itself: a clock that has counted less than the retention. The subtraction
    // this replaced wrapped to a cutoff past every timestamp.
    EXPECT_EQ(ob::ttl_cutoff_ns(5 * kNsPerHour, 6), 0u);
    // ...and exactly at the boundary the answer is the epoch, not a wrap.
    EXPECT_EQ(ob::ttl_cutoff_ns(6 * kNsPerHour, 6), 0u);
    EXPECT_EQ(ob::ttl_cutoff_ns(6 * kNsPerHour + 7, 6), 7u);
}

TEST(TTLCutoff, ARetentionTooLongToCountInNanosecondsDoesNotOverflow) {
    // `--ttl-hours` takes any uint64_t, and hours times nanoseconds per hour overflows from
    // 5 124 096 hours up.
    EXPECT_EQ(ob::ttl_cutoff_ns(1'790'000'000'000'000'000ULL, UINT64_MAX), 0u);
    EXPECT_EQ(ob::ttl_cutoff_ns(UINT64_MAX, UINT64_MAX / kNsPerHour),
              UINT64_MAX - (UINT64_MAX / kNsPerHour) * kNsPerHour);
}

RC_GTEST_PROP(TTLCutoff, TheCutoffIsNeverAfterNowAndIsExactWheneverItIsNotZero, ()) {
    const uint64_t now = *rc::gen::arbitrary<uint64_t>();
    const uint64_t ttl = *rc::gen::oneOf(rc::gen::inRange<uint64_t>(0, 100'000),
                                         rc::gen::arbitrary<uint64_t>());
    const uint64_t cutoff = ob::ttl_cutoff_ns(now, ttl);
    const unsigned __int128 span = static_cast<unsigned __int128>(ttl) * kNsPerHour;
    RC_ASSERT(cutoff <= now);
    if (ttl == 0 || span > now) {
        RC_ASSERT(cutoff == 0u);
    } else {
        RC_ASSERT(static_cast<unsigned __int128>(now - cutoff) == span);
    }
}

// ── The sweep, through an engine (#163) ──────────────────────────────────────

double machine_uptime_seconds() {
    std::ifstream in("/proc/uptime");
    double up = 0;
    in >> up;
    return up;
}

void write_rows(ob::Engine& engine, const char* symbol, uint64_t first_event_time_ns, int rows) {
    for (int i = 0; i < rows; ++i) {
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
        delta.timestamp_ns = first_event_time_ns + static_cast<uint64_t>(i);
        delta.side         = ob::SIDE_BID;
        delta.n_levels     = 1;
        ob::Level level{static_cast<int64_t>(1000 + i), 1, 1, 0};
        ASSERT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);
    }
}

/// Rows the engine returns for `symbol`. A symbol whose every segment has expired is one the
/// engine no longer knows, which it answers with "not found" - zero rows, not an error here.
int rows_of(ob::Engine& engine, const char* symbol) {
    int n = 0;
    const std::string err = engine.execute(
        std::string("SELECT * FROM '") + symbol +
            "'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult&) { ++n; });
    if (err.rfind("OB_ERR_NOT_FOUND", 0) == 0) return 0;
    EXPECT_TRUE(err.empty()) << err;
    return n;
}

/// A first engine, without a retention, writes `rows` rows of OLD dated `old_age_hours` ago and
/// `rows` of FRESH dated now, and closes - so both are in segments on disk, as they are for a node
/// restarted with `--ttl-hours` turned on. A second engine on that directory sweeps once a second.
/// Returns once the sweep has deleted something or ten seconds have passed.
struct SweepOutcome {
    int old_rows;
    int fresh_rows;
    uint64_t segments_deleted;
};

SweepOutcome sweep_after_restart(const fs::path& dir, uint64_t ttl_hours, uint64_t old_age_hours,
                                 int rows, uint64_t scan_interval_seconds = 1) {
    fs::remove_all(dir);
    fs::create_directories(dir);
    {
        ob::Engine first(dir.string(), 20'000'000ULL);
        first.open();
        const uint64_t now = ob::wall_clock_ns();
        write_rows(first, "OLD", now - old_age_hours * kNsPerHour, rows);
        write_rows(first, "FRESH", now, rows);
        first.close();
    }
    ob::Engine second(dir.string(), 20'000'000ULL, ob::FsyncPolicy::INTERVAL, {}, {}, {},
                      ob::TTLConfig{ttl_hours, scan_interval_seconds});
    second.open();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (second.stats().ttl_segments_deleted == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    SweepOutcome out{rows_of(second, "OLD"), rows_of(second, "FRESH"),
                     second.stats().ttl_segments_deleted};
    second.close();
    fs::remove_all(dir);
    return out;
}

TEST(TTLSweep, ARestartWithARetentionLongerThanTheMachineHasBeenUpKeepsWhatIsNewer) {
    // The premise under which the sweep this replaced deleted everything: a retention longer than
    // the count its clock had reached. Taken from this machine's own uptime, so the test holds that
    // premise on any machine rather than on the one it was written on - where it was a restart with
    // `--ttl-hours 24` after 21.5 hours up, and every row went.
    const double up = machine_uptime_seconds();
    ASSERT_GT(up, 0.0) << "/proc/uptime could not be read, so the premise cannot be set";
    const uint64_t ttl_hours = static_cast<uint64_t>(std::ceil(up / 3600.0)) + 1;
    ASSERT_GT(static_cast<double>(ttl_hours) * 3600.0, up);

    const SweepOutcome o = sweep_after_restart(fs::temp_directory_path() / "ttl_sweep_uptime",
                                               ttl_hours, ttl_hours + 2, 50);
    // The sweep ran: the rows older than the retention are the ones it was asked to expire...
    EXPECT_GE(o.segments_deleted, 1u);
    EXPECT_EQ(o.old_rows, 0) << "rows dated past the retention were not expired";
    // ...and it expired nothing else.
    EXPECT_EQ(o.fresh_rows, 50) << "the sweep deleted rows younger than the retention - with a "
                                   "retention of " << ttl_hours << " h, on a machine up "
                                << static_cast<uint64_t>(up / 3600.0) << " h";
}

TEST(TTLSweep, TheFirstSweepRunsAtTheFirstTickWhateverTheInterval) {
    // An interval of 31 years. The sweep's cadence counts from the last sweep, and before the first
    // one there is none: counted from the monotonic clock's zero - this machine's boot - instead,
    // the first sweep of a node on a machine up for less than its interval would wait out the rest
    // of it, and a node restarted after a long stop would keep its expired rows that long.
    const SweepOutcome o = sweep_after_restart(fs::temp_directory_path() / "ttl_sweep_first_tick", 1,
                                               3, 50, 1'000'000'000ULL);
    EXPECT_GE(o.segments_deleted, 1u) << "no sweep ran within ten seconds of starting";
    EXPECT_EQ(o.old_rows, 0);
    EXPECT_EQ(o.fresh_rows, 50);
}

TEST(TTLSweep, RowsOlderThanTheRetentionExpireAndNewerOnesStay) {
    // Against the sweep this replaced this fails whatever the machine's uptime: up for more than
    // an hour, its cutoff was an hour into 1970 and nothing expired; up for less, it wrapped and
    // everything did.
    const SweepOutcome o =
        sweep_after_restart(fs::temp_directory_path() / "ttl_sweep_one_hour", 1, 3, 50);
    EXPECT_GE(o.segments_deleted, 1u) << "no sweep deleted anything within ten seconds";
    EXPECT_EQ(o.old_rows, 0);
    EXPECT_EQ(o.fresh_rows, 50);
}

} // namespace
