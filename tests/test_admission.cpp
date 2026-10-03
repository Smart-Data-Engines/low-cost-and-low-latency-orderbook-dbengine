// #190, step 5: the admission controller on its own, with the time handed in.
//
// What it has to do, from the EC2 runs (evidence/2026-10-03-write-ceiling-ec2/): nothing at all
// while the device keeps up - the prototype that admitted from the queue's occupancy halved the
// throughput of a fast device - and, from a flush tick that shows the device behind, pace every
// writer together at the rate that tick measured, until nobody has had to wait for a while.

#include "orderbook/admission.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

namespace {

using ob::AdmissionController;
using Clock = AdmissionController::Clock;
using std::chrono::milliseconds;
using std::chrono::seconds;

// Far from the clock's epoch, so that `now - burst` is a time like any other.
const Clock::time_point kT0 = Clock::time_point{} + std::chrono::hours(1);

AdmissionController::Config config() {
    AdmissionController::Config c;
    c.interval = milliseconds(100);
    c.slow_factor = 2.0;
    c.max_delay = milliseconds(500);
    c.burst = milliseconds(100);
    c.raise = 1.1;
    c.idle_ticks_to_end = 10;
    c.floor_rows_per_s = 1000.0;
    return c;
}

}  // namespace

TEST(Admission, NothingWaitsUntilASlowTick) {
    AdmissionController a(config());
    EXPECT_EQ(a.admit(100'000, kT0), Clock::duration::zero());
    // A tick of a million rows in 150 ms: past its interval, short of slow - a fast device at the
    // ceiling, which is what the prototype took for a slow one.
    EXPECT_EQ(a.on_tick(1'000'000, milliseconds(150)), AdmissionController::Change::None);
    EXPECT_FALSE(a.active());
    EXPECT_EQ(a.rate(), 0.0);
    EXPECT_EQ(a.admit(100'000, kT0), Clock::duration::zero());
}

TEST(Admission, ATickThatTookNoRowsIsNotSlowHoweverLong) {
    AdmissionController a(config());
    EXPECT_EQ(a.on_tick(0, seconds(3)), AdmissionController::Change::None);
    EXPECT_FALSE(a.active());
}

TEST(Admission, ASlowTickBeginsAtTheRateItMeasured) {
    AdmissionController a(config());
    EXPECT_EQ(a.on_tick(600'000, seconds(2)), AdmissionController::Change::Began);
    EXPECT_TRUE(a.active());
    EXPECT_DOUBLE_EQ(a.rate(), 300'000.0);
    const auto m = a.last_slow_tick();
    EXPECT_EQ(m.rows, 600'000u);
    EXPECT_EQ(m.took, seconds(2));
    EXPECT_DOUBLE_EQ(m.rows_per_s, 300'000.0);
}

TEST(Admission, AnotherSlowTickLowersTheRateAndNeverRaisesIt) {
    AdmissionController a(config());
    a.on_tick(1'000'000, seconds(1));
    EXPECT_EQ(a.on_tick(300'000, seconds(1)), AdmissionController::Change::None);
    EXPECT_DOUBLE_EQ(a.rate(), 300'000.0);
    a.on_tick(2'000'000, seconds(1));   // a slow tick is the device behind, whatever it measured
    EXPECT_DOUBLE_EQ(a.rate(), 300'000.0);
}

TEST(Admission, ATickThatIsNotSlowRaisesTheRateByAStep) {
    AdmissionController a(config());
    a.on_tick(1'000'000, seconds(1));
    a.on_tick(50'000, milliseconds(40));
    EXPECT_DOUBLE_EQ(a.rate(), 1'100'000.0);
}

TEST(Admission, TwoWritersTogetherWriteAtTheRate) {
    AdmissionController a(config());
    a.on_tick(1'000'000, seconds(1));   // 1 M rows a second
    // Two writers, 10 000 rows a batch, each writing its next batch as soon as it is let go.
    Clock::time_point at[2] = {kT0, kT0};
    uint64_t rows = 0;
    const Clock::time_point end = kT0 + seconds(1);
    while (true) {
        const int w = at[0] <= at[1] ? 0 : 1;
        if (at[w] >= end) break;
        at[w] += a.admit(10'000, at[w]);
        rows += 10'000;
    }
    // A second's worth at the rate, plus the burst's credit (100 ms of it) and a batch apiece.
    EXPECT_GE(rows, 1'000'000u);
    EXPECT_LE(rows, 1'000'000u + 100'000u + 2 * 10'000u);
    EXPECT_GT(a.delayed_batches(), 0u);
}

TEST(Admission, AWriterBelowTheRateDoesNotWait) {
    AdmissionController a(config());
    a.on_tick(1'000'000, seconds(1));
    // Half the rate: 10 000 rows every 20 ms.
    for (int i = 0; i < 200; ++i) {
        EXPECT_EQ(a.admit(10'000, kT0 + milliseconds(20 * i)), Clock::duration::zero()) << "batch " << i;
    }
    EXPECT_EQ(a.delayed_batches(), 0u);
}

TEST(Admission, ItEndsOnlyAfterARunOfTicksInWhichNobodyWaited) {
    AdmissionController a(config());
    a.on_tick(1'000'000, seconds(1));
    Clock::time_point now = kT0;
    // Writers still paced: ticks that are not slow raise the rate, and admission stays.
    for (int tick = 0; tick < 30; ++tick) {
        for (int b = 0; b < 40; ++b) now += a.admit(50'000, now);   // above the rate
        EXPECT_EQ(a.on_tick(100'000, milliseconds(50)), AdmissionController::Change::None)
            << "ended at tick " << tick << " while writers were still waiting";
    }
    EXPECT_TRUE(a.active());
    // Then nobody waits: ten ticks of it end admission, and not one fewer.
    for (int tick = 0; tick < 9; ++tick) {
        EXPECT_EQ(a.on_tick(1'000, milliseconds(50)), AdmissionController::Change::None);
    }
    EXPECT_EQ(a.on_tick(1'000, milliseconds(50)), AdmissionController::Change::Ended);
    EXPECT_FALSE(a.active());
    EXPECT_EQ(a.rate(), 0.0);
    EXPECT_EQ(a.admit(1'000'000, now), Clock::duration::zero());
}

TEST(Admission, ADelayIsCappedAndWhatIsLeftForgiven) {
    AdmissionController a(config());
    a.on_tick(1, seconds(10));   // 0.1 rows a second, held up by the floor
    EXPECT_DOUBLE_EQ(a.rate(), 1000.0);
    // 100 000 rows at 1 000 a second is 100 s; the batch waits the cap.
    EXPECT_EQ(a.admit(100'000, kT0), milliseconds(500));
    // And the next one, once the first is let go, waits its own 100 ms from there, not the 99.5 s
    // left of the first.
    EXPECT_EQ(a.admit(100, kT0 + milliseconds(500)), milliseconds(100));
}

TEST(Admission, OffMeansOff) {
    AdmissionController a(config());
    a.set_enabled(false);
    EXPECT_EQ(a.on_tick(1'000'000, seconds(5)), AdmissionController::Change::None);
    EXPECT_FALSE(a.active());
    EXPECT_EQ(a.admit(1'000'000, kT0), Clock::duration::zero());

    AdmissionController b(config());
    b.on_tick(1'000'000, seconds(5));
    ASSERT_TRUE(b.active());
    b.set_enabled(false);
    EXPECT_FALSE(b.active());
    EXPECT_EQ(b.admit(1'000'000, kT0), Clock::duration::zero()) << "turned off and still pacing";
}

TEST(Admission, WritersAndTheFlushThreadAtOnce) {
    // For ThreadSanitizer: admit() from writers while on_tick() runs on another thread.
    AdmissionController a(config());
    std::atomic<bool> stop{false};
    std::vector<std::thread> writers;
    for (int w = 0; w < 3; ++w) {
        writers.emplace_back([&] {
            while (!stop.load()) (void)a.admit(1'000, Clock::now());
        });
    }
    for (int tick = 0; tick < 200; ++tick) {
        a.on_tick(tick % 3 == 0 ? 500'000 : 1'000, tick % 3 == 0 ? milliseconds(300) : milliseconds(10));
        (void)a.rate();
    }
    stop.store(true);
    for (auto& t : writers) t.join();
    SUCCEED();
}
