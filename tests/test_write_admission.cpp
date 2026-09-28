// Admission at the rate the flush drains (#190).
//
// On a device slower than the ingest the flush tick takes seconds, and the pending queue lasts about
// half a second at the write ceiling: every writer stopped once it was full, for up to the five
// seconds after which a write is refused. A batch written with the queue past half waits now, after
// its write and outside the engine's lock, in proportion to its rows and to how far past half the
// queue is, so writers slow down with the device instead of stopping.

#include "orderbook/data_model.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    TempDir()
        : path((fs::temp_directory_path() /
                ("ob_admission_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1)))).string()) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;
constexpr size_t kHalf = ob::Engine::MAX_PENDING_ROWS / 2;

/// `updates` writes of `levels` levels each, as one batch, the way a pipelining client's read is.
double write_batch_ms(ob::Engine& engine, size_t updates, uint16_t levels, uint64_t first_ts) {
    std::vector<ob::DeltaUpdate> deltas(updates);
    std::vector<ob::Level> lv(levels);
    for (uint16_t i = 0; i < levels; ++i) {
        lv[i] = ob::Level{};
        lv[i].price = 1000 + i;
        lv[i].qty   = 1;
        lv[i].cnt   = 1;
    }
    std::vector<ob::ClientWrite> writes(updates);
    for (size_t i = 0; i < updates; ++i) {
        std::strncpy(deltas[i].symbol, "ADM", sizeof(deltas[i].symbol) - 1);
        std::strncpy(deltas[i].exchange, "EX", sizeof(deltas[i].exchange) - 1);
        deltas[i].timestamp_ns = first_ts + i;
        deltas[i].side         = ob::SIDE_BID;
        deltas[i].n_levels     = levels;
        writes[i].update = &deltas[i];
        writes[i].levels = lv.data();
    }
    std::vector<ob::WriteOutcome> outcomes(updates);
    const auto started = Clock::now();
    engine.apply_deltas(std::span<const ob::ClientWrite>(writes), outcomes);
    const double ms = std::chrono::duration<double, std::milli>(Clock::now() - started).count();
    for (const auto& o : outcomes) {
        EXPECT_EQ(o.status, ob::OB_OK);
        EXPECT_TRUE(o.error.empty()) << o.error;
    }
    return ms;
}

}  // namespace

TEST(WriteAdmission, NothingWaitsBelowHalfTheQueue) {
    EXPECT_EQ(ob::Engine::admission_delay_ns(0, 1000), 0u);
    EXPECT_EQ(ob::Engine::admission_delay_ns(kHalf, 1000), 0u) << "half is the threshold, not past it";
    EXPECT_EQ(ob::Engine::admission_delay_ns(ob::Engine::MAX_PENDING_ROWS, 0), 0u)
        << "a batch that queued nothing has nothing to wait for";
}

TEST(WriteAdmission, TheWaitGrowsWithTheQueueAndTheRowsAndStopsAtItsCeiling) {
    const size_t three_quarters = kHalf + kHalf / 2;
    // Half-way from half to full, 1000 rows: half of 100 us a row.
    EXPECT_EQ(ob::Engine::admission_delay_ns(three_quarters, 1000), 50'000'000u);
    EXPECT_EQ(ob::Engine::admission_delay_ns(three_quarters, 10), 500'000u)
        << "a small batch waits in proportion to its rows, not as long as a big one";
    EXPECT_LT(ob::Engine::admission_delay_ns(kHalf + 1000, 1000),
              ob::Engine::admission_delay_ns(three_quarters, 1000));
    EXPECT_EQ(ob::Engine::admission_delay_ns(ob::Engine::MAX_PENDING_ROWS, 1000),
              ob::Engine::kAdmissionFullDelayPerRowNs * 1000);
    EXPECT_EQ(ob::Engine::admission_delay_ns(ob::Engine::MAX_PENDING_ROWS, 1'000'000),
              ob::Engine::kAdmissionMaxDelayNs) << "a huge batch waits the ceiling, no longer";
    EXPECT_EQ(ob::Engine::admission_delay_ns(ob::Engine::MAX_PENDING_ROWS * 2, 1000),
              ob::Engine::kAdmissionFullDelayPerRowNs * 1000)
        << "past full is full";
}

TEST(WriteAdmission, ABatchWrittenPastHalfWaitsAfterItAndOneAfterTheDrainDoesNot) {
    TempDir dir;
    ob::Engine engine(dir.path, kNoAutoFlush, ob::FsyncPolicy::NONE);
    engine.open();
    // Up to half: no wait. Written as one batch, so what it queued is what the next one sees.
    const double to_half = write_batch_ms(engine, kHalf / 1000, 1000, 1'000'000);
    EXPECT_EQ(engine.registry().counter_value("ob_writer_admission_delays_total"), 0u);

    // A quarter more in one batch: the queue three quarters full after it, and the batch waits the
    // ceiling - its rows at half of 100 us are far past it.
    const double past_half = write_batch_ms(engine, kHalf / 2000, 1000, 2'000'000);
    EXPECT_GE(past_half, 0.95 * static_cast<double>(ob::Engine::kAdmissionMaxDelayNs) / 1e6)
        << "a batch written with the queue three quarters full did not wait";
    EXPECT_EQ(engine.registry().counter_value("ob_writer_admission_delays_total"), 1u);
    EXPECT_GE(engine.registry().counter_value("ob_writer_admission_delay_us_total"),
              ob::Engine::kAdmissionMaxDelayNs / 1000);

    // Drained: taken at full speed again.
    engine.flush_incremental();
    const double after = write_batch_ms(engine, 1, 1000, 3'000'000);
    EXPECT_LT(after, 50.0) << "a write after the drain still waited";
    EXPECT_EQ(engine.registry().counter_value("ob_writer_admission_delays_total"), 1u);
    (void)to_half;
    engine.close();
}
