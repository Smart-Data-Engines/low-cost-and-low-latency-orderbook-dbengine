// OB-12, stage 2: the pool a scan's candidates are read on.
//
// What a query needs from it, each held here: a task no thread has taken runs on the query's own
// thread, so a query never waits on the queue; a task a thread has taken is waited for; what a
// task's work throws reaches the query that waits for it; a task the query no longer needs is
// dropped without running; and a pool that stops leaves its queued tasks to their queries, which run
// them, rather than stranding a query. A pool of no threads runs everything where it is waited for.
//
// And when its threads enter the kernel between the steps of their work (#248): `ScanPace` held to
// its rule, and a pool thread that works between steps entering it.

#include "orderbook/loop_load.hpp"
#include "orderbook/scan_pool.hpp"

#include <gtest/gtest.h>

#include <sched.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

}  // namespace

TEST(ScanPool, APoolOfNoThreadsRunsEveryTaskWhereItIsWaitedFor) {
    ob::ScanPool pool(0, nullptr);
    EXPECT_EQ(pool.threads(), 0u);
    std::thread::id ran_on;
    auto task = pool.submit([&] { ran_on = std::this_thread::get_id(); });
    EXPECT_FALSE(task->done());
    pool.run_or_wait(*task);
    EXPECT_TRUE(task->done());
    EXPECT_EQ(ran_on, std::this_thread::get_id());
}

TEST(ScanPool, ATaskNoThreadHasTakenRunsOnTheQuerysThread) {
    // One thread, held busy by a first task until the second is waited for: the second is still
    // queued then, and the query runs it itself rather than waiting behind the first.
    ob::ScanPool pool(1, nullptr);
    std::atomic<bool> release{false};
    std::atomic<bool> first_started{false};
    auto first = pool.submit([&] {
        first_started = true;
        while (!release) std::this_thread::sleep_for(1ms);
    });
    while (!first_started) std::this_thread::sleep_for(1ms);
    std::thread::id ran_on;
    auto second = pool.submit([&] { ran_on = std::this_thread::get_id(); });
    pool.run_or_wait(*second);
    EXPECT_EQ(ran_on, std::this_thread::get_id()) << "the query waited on the queue";
    release = true;
    pool.run_or_wait(*first);
    EXPECT_TRUE(first->done());
}

TEST(ScanPool, ATaskAThreadHasTakenIsWaitedFor) {
    ob::ScanPool pool(2, nullptr);
    std::atomic<bool> started{false};
    std::thread::id ran_on;
    auto task = pool.submit([&] {
        started = true;
        std::this_thread::sleep_for(50ms);
        ran_on = std::this_thread::get_id();
    });
    while (!started) std::this_thread::sleep_for(1ms);
    pool.run_or_wait(*task);
    EXPECT_TRUE(task->done());
    EXPECT_NE(ran_on, std::this_thread::get_id());
}

TEST(ScanPool, WhatATaskThrowsReachesTheQueryThatWaitsForIt) {
    for (const unsigned threads : {0u, 2u}) {
        ob::ScanPool pool(threads, nullptr);
        auto task = pool.submit([] { throw std::runtime_error("a part failed"); });
        try {
            pool.run_or_wait(*task);
            ADD_FAILURE() << "nothing was thrown, with " << threads << " thread(s)";
        } catch (const std::runtime_error& e) {
            EXPECT_STREQ(e.what(), "a part failed");
        }
        EXPECT_TRUE(task->done());
    }
}

TEST(ScanPool, ATaskTheQueryNoLongerNeedsIsDroppedWithoutRunning) {
    ob::ScanPool pool(1, nullptr);
    std::atomic<bool> release{false};
    std::atomic<bool> first_started{false};
    auto first = pool.submit([&] {
        first_started = true;
        while (!release) std::this_thread::sleep_for(1ms);
    });
    while (!first_started) std::this_thread::sleep_for(1ms);
    std::atomic<bool> second_ran{false};
    auto second = pool.submit([&] { second_ran = true; });
    pool.drop_or_wait(*second);
    EXPECT_TRUE(second->done());
    release = true;
    pool.run_or_wait(*first);
    // The thread finds the dropped task in its queue afterwards and passes over it.
    std::this_thread::sleep_for(20ms);
    EXPECT_FALSE(second_ran);
}

TEST(ScanPool, AStopLeavesItsQueuedTasksToTheirQueriesWhichRunThem) {
    std::vector<std::shared_ptr<ob::ScanPool::Task>> tasks;
    std::atomic<int> ran{0};
    std::atomic<bool> release{false};
    std::atomic<bool> first_started{false};
    {
        ob::ScanPool pool(1, nullptr);
        tasks.push_back(pool.submit([&] {
            first_started = true;
            while (!release) std::this_thread::sleep_for(1ms);
            ++ran;
        }));
        while (!first_started) std::this_thread::sleep_for(1ms);
        for (int i = 0; i < 3; ++i) tasks.push_back(pool.submit([&] { ++ran; }));
        // The stop waits for the task its thread is running and no longer: released while the stop
        // is under way, the thread may or may not reach another queued task first, and either way
        // every task is run exactly once, the rest by the query that waits for them.
        std::thread stopper([&] { pool.stop(); });
        std::this_thread::sleep_for(20ms);
        release = true;
        stopper.join();
        for (auto& t : tasks) pool.run_or_wait(*t);
        EXPECT_EQ(ran.load(), 4);
    }
    for (auto& t : tasks) EXPECT_TRUE(t->done());
}

TEST(ScanPool, ManyQueriesAtOnceEachGetEveryTaskDone) {
    // Four threads submitting and waiting at once, as four reactors' queries share the pool: every
    // task done, each exactly once.
    ob::ScanPool pool(3, nullptr);
    std::atomic<uint64_t> sum{0};
    std::vector<std::thread> queries;
    for (int q = 0; q < 4; ++q) {
        queries.emplace_back([&, q] {
            for (int round = 0; round < 50; ++round) {
                std::vector<std::shared_ptr<ob::ScanPool::Task>> mine;
                for (int k = 0; k < 8; ++k) {
                    mine.push_back(pool.submit([&sum, q, k] { sum += static_cast<uint64_t>(q * 100 + k); }));
                }
                for (auto& t : mine) pool.run_or_wait(*t);
            }
        });
    }
    for (auto& t : queries) t.join();
    uint64_t expected = 0;
    for (int q = 0; q < 4; ++q) {
        for (int k = 0; k < 8; ++k) expected += static_cast<uint64_t>(q * 100 + k) * 50;
    }
    EXPECT_EQ(sum.load(), expected);
}

TEST(ScanPool, ItsThreadsReadUnderSchedIdleBelowTheClientLoops) {
    // A task only a pool thread can run - nothing here runs or waits for it until it is done - says
    // which policy it ran under.
    ob::ScanPool pool(1, nullptr);
    std::atomic<int> policy{-1};
    auto task = pool.submit([&] { policy = ::sched_getscheduler(0); });
    for (int i = 0; i < 5000 && !task->done(); ++i) std::this_thread::sleep_for(1ms);
    ASSERT_TRUE(task->done()) << "no pool thread ran the task";
    pool.run_or_wait(*task);
    EXPECT_EQ(policy.load(), SCHED_IDLE);
}

TEST(ScanPool, OneScanAtATimeHoldsTheClaim) {
    ob::ScanPool pool(2, nullptr);
    ASSERT_TRUE(pool.claim());
    EXPECT_FALSE(pool.claim()) << "a second scan divided while one held the pool";
    pool.release();
    EXPECT_TRUE(pool.claim());
    pool.release();
}

// ── When a pool thread enters the kernel (#248) ───────────────────────────────────────────────────

namespace {

using Pace = ob::ScanPace;
const Pace::Clock::time_point kT{std::chrono::seconds{1}};   // a clock a second past its epoch
constexpr std::chrono::nanoseconds kNs{1};

}  // namespace

TEST(ScanPace, AThreadNobodyWaitsForEntersOnceAQuietIntervalHasPassed) {
    Pace pace;
    EXPECT_TRUE(pace.due(kT)) << "a thread's first step did not enter the kernel";
    EXPECT_FALSE(pace.entered(kT, kT + 100 * kNs)) << "an entry of 100 ns gave the CPU away";
    const auto e = kT + 100 * kNs;
    EXPECT_FALSE(pace.due(e + Pace::kBusy));
    EXPECT_FALSE(pace.due(e + Pace::kQuiet - kNs));
    EXPECT_TRUE(pace.due(e + Pace::kQuiet));
}

TEST(ScanPace, AfterAnEntryThatGaveTheCpuAwayItEntersEveryBusyIntervalForAWindow) {
    Pace pace;
    ASSERT_TRUE(pace.entered(kT, kT + Pace::kGaveUp)) << "an entry at the threshold kept the CPU";
    const auto g = kT + Pace::kGaveUp;
    EXPECT_FALSE(pace.due(g + Pace::kBusy - kNs));
    EXPECT_TRUE(pace.due(g + Pace::kBusy));
    // Entries that give nothing away keep the busy interval until the window from the one that did
    // has passed.
    auto at = g;
    while (at + 2 * Pace::kBusy < g + Pace::kWindow) {
        at += Pace::kBusy;
        ASSERT_TRUE(pace.due(at));
        ASSERT_FALSE(pace.entered(at, at + 50 * kNs));
        at += 50 * kNs;
    }
    const auto past = g + Pace::kWindow;
    ASSERT_FALSE(pace.entered(past, past + 50 * kNs));
    EXPECT_FALSE(pace.due(past + 50 * kNs + Pace::kBusy)) << "the busy interval outlived its window";
    EXPECT_TRUE(pace.due(past + 50 * kNs + Pace::kQuiet));
}

TEST(ScanPace, AnEntryJustShortOfTheThresholdGaveNothingAway) {
    Pace pace;
    EXPECT_FALSE(pace.entered(kT, kT + Pace::kGaveUp - kNs));
    EXPECT_FALSE(pace.due(kT + Pace::kGaveUp - kNs + Pace::kBusy));
}

TEST(ScanPool, AThreadThatWorksBetweenStepsEntersTheKernel) {
    // A task only a pool thread runs, stepping as a part's reader steps through a batch, a step a
    // fraction of a microsecond long. Before each `pace()` it reads the clock the rule reads and
    // counts the entries the rule owes: one at the first step, and one at each step a quiet
    // interval after the step that followed the last owed one - by when that entry, if the thread
    // made it, had ended. Every interval of the rule is at most the quiet one, so the thread entered
    // at least as often as it owed, however long a step took and however long the scheduler kept it
    // off a CPU. The thread's own CPU time was the measure until a CI runner gave 20 quiet intervals
    // of it and 5 entries (pitfall 627).
    ob::ScanPool pool(1, nullptr);
    std::atomic<uint64_t> spun{0};
    std::atomic<unsigned> owed{0};
    std::atomic<uint64_t> steps{0};
    std::atomic<int64_t> span_us{0};
    auto task = pool.submit([&] {
        using Clock = ob::ScanPace::Clock;
        const auto from = Clock::now();
        auto at = from;
        Clock::time_point ended{};
        bool ending = false;
        unsigned due = 0;
        uint64_t n = 0;
        uint64_t x = 0;
        while (due < 20 && at - from < 5s) {
            for (int i = 0; i < 200; ++i) x = x * 6364136223846793005ULL + 1442695040888963407ULL;
            at = Clock::now();
            if (ending) {   // the step after an owed entry: the entry, if made, had ended by now
                ended = at;
                ending = false;
            }
            if (due == 0 || at - ended >= ob::ScanPace::kQuiet) {
                ++due;
                ending = true;
            }
            ob::ScanPool::pace();
            ++n;
        }
        spun = x | 1;
        owed = due;
        steps = n;
        span_us = std::chrono::duration_cast<std::chrono::microseconds>(at - from).count();
    });
    for (int i = 0; i < 10000 && !task->done(); ++i) std::this_thread::sleep_for(1ms);
    ASSERT_TRUE(task->done()) << "no pool thread ran the task";
    pool.run_or_wait(*task);
    pool.stop();   // the thread joined, so what it counted is in the pool's counts
    EXPECT_NE(spun.load(), 0u);
    EXPECT_GE(pool.entries(), owed.load())
        << "a pool thread whose " << steps.load() << " steps over " << span_us.load() << " us owed "
        << owed.load() << " entries entered the kernel " << pool.entries() << " time(s)";
    EXPECT_EQ(owed.load(), 20u) << "a pool thread did not step through 20 quiet intervals in 5 s";
    EXPECT_LE(pool.gave_up(), pool.entries());
}

// ── Whether a CPU is left over from the client loops (#249) ───────────────────────────────────────

TEST(LoopLoad, ALoopCountsAsServingWhileItServesAndForAMomentAfter) {
    ob::LoopLoad load(3);
    const auto t0 = ob::LoopLoad::Clock::now();
    EXPECT_EQ(load.serving_within(t0, 2ms), 0u);
    // A loop that never served did not serve a moment ago, however long the moment - longer than
    // the clock has run since the machine booted included.
    EXPECT_EQ(load.serving_within(t0, std::chrono::hours(24 * 365 * 100)), 0u);
    load.serving(0);
    load.serving(2);
    EXPECT_EQ(load.serving_within(t0, 2ms), 2u);
    load.stopped_at(0, t0);
    EXPECT_EQ(load.serving_within(t0 + 1ms, 2ms), 2u) << "a loop that stopped a moment ago counted as idle";
    EXPECT_EQ(load.serving_within(t0 + 2ms, 2ms), 1u) << "a loop counted as serving past the moment";
    load.stopped(1);   // a loop that never served: nothing to end
    EXPECT_EQ(load.serving_within(t0 + 2ms, 2ms), 1u);
    load.stopped(2);
    EXPECT_EQ(load.serving_within(ob::LoopLoad::Clock::now(), 1h), 2u)
        << "loops 0 and 2 stopped within the hour; one of them did not record when";
    EXPECT_EQ(load.serving_within(ob::LoopLoad::Clock::now() + 2h, 1h), 0u);
}

TEST(ScanPool, AScanDividesOnlyWhileACpuIsLeftOverFromTheClientLoops) {
    ob::ScanPool pool(2, nullptr);
    const auto t0 = ob::LoopLoad::Clock::now();
    EXPECT_TRUE(pool.cpu_to_spare(t0)) << "with no loops to count, a scan divides on its own rules";
    ob::LoopLoad load(4);
    pool.set_loop_load(&load, 4);
    load.serving(0);   // the scan's own loop
    EXPECT_TRUE(pool.cpu_to_spare(t0));
    load.serving(1);
    load.serving(2);
    EXPECT_TRUE(pool.cpu_to_spare(t0)) << "three loops of four serving leave a CPU";
    load.serving(3);
    EXPECT_FALSE(pool.cpu_to_spare(t0));
    EXPECT_EQ(pool.undivided(), 1u);
    load.stopped_at(3, t0);
    EXPECT_FALSE(pool.cpu_to_spare(t0 + ob::ScanPool::kLoopRecent - 1us))
        << "a loop between two of its connection's requests counted as a CPU to spare";
    EXPECT_TRUE(pool.cpu_to_spare(t0 + ob::ScanPool::kLoopRecent));
    EXPECT_EQ(pool.undivided(), 2u);
    // On fewer CPUs than loops, as under a cgroup limit, the CPUs are what a scan divides against.
    pool.set_loop_load(&load, 3);
    EXPECT_FALSE(pool.cpu_to_spare(t0 + ob::ScanPool::kLoopRecent));
    pool.set_loop_load(nullptr, 0);
    EXPECT_TRUE(pool.cpu_to_spare(t0));
}
