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

#include "orderbook/scan_pool.hpp"

#include <gtest/gtest.h>

#include <sched.h>
#include <time.h>

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

constexpr uint64_t kT = 1'000'000'000ULL;   // a clock a second past its epoch

}  // namespace

TEST(ScanPace, AThreadNobodyWaitsForEntersOnceAQuietIntervalHasPassed) {
    ob::ScanPace pace;
    EXPECT_TRUE(pace.due(kT)) << "a thread's first step did not enter the kernel";
    EXPECT_FALSE(pace.entered(kT, kT + 100)) << "an entry of 100 ns gave the CPU away";
    const uint64_t e = kT + 100;
    EXPECT_FALSE(pace.due(e + ob::ScanPace::kBusyNs));
    EXPECT_FALSE(pace.due(e + ob::ScanPace::kQuietNs - 1));
    EXPECT_TRUE(pace.due(e + ob::ScanPace::kQuietNs));
}

TEST(ScanPace, AfterAnEntryThatGaveTheCpuAwayItEntersEveryBusyIntervalForAWindow) {
    ob::ScanPace pace;
    ASSERT_TRUE(pace.entered(kT, kT + ob::ScanPace::kGaveUpNs)) << "an entry at the threshold kept the CPU";
    const uint64_t g = kT + ob::ScanPace::kGaveUpNs;
    EXPECT_FALSE(pace.due(g + ob::ScanPace::kBusyNs - 1));
    EXPECT_TRUE(pace.due(g + ob::ScanPace::kBusyNs));
    // Entries that give nothing away keep the busy interval until the window from the one that did
    // has passed.
    uint64_t at = g;
    while (at + 2 * ob::ScanPace::kBusyNs < g + ob::ScanPace::kWindowNs) {
        at += ob::ScanPace::kBusyNs;
        ASSERT_TRUE(pace.due(at));
        ASSERT_FALSE(pace.entered(at, at + 50));
        at += 50;
    }
    const uint64_t past = g + ob::ScanPace::kWindowNs;
    ASSERT_FALSE(pace.entered(past, past + 50));
    EXPECT_FALSE(pace.due(past + 50 + ob::ScanPace::kBusyNs)) << "the busy interval outlived its window";
    EXPECT_TRUE(pace.due(past + 50 + ob::ScanPace::kQuietNs));
}

TEST(ScanPace, AnEntryJustShortOfTheThresholdGaveNothingAway) {
    ob::ScanPace pace;
    EXPECT_FALSE(pace.entered(kT, kT + ob::ScanPace::kGaveUpNs - 1));
    EXPECT_FALSE(pace.due(kT + ob::ScanPace::kGaveUpNs - 1 + ob::ScanPace::kBusyNs));
}

TEST(ScanPool, AThreadThatWorksBetweenStepsEntersTheKernel) {
    // A task only a pool thread runs, working 20 quiet intervals of its own CPU time - so however
    // long the scheduler keeps it off a CPU - with a step a fraction of a microsecond long, as a
    // part's reader steps through a batch.
    ob::ScanPool pool(1, nullptr);
    std::atomic<uint64_t> spun{0};
    auto task = pool.submit([&] {
        const auto cpu_ns = [] {
            timespec ts{};
            ::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
            return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(ts.tv_nsec);
        };
        const uint64_t from = cpu_ns();
        uint64_t x = 0;
        while (cpu_ns() - from < 20 * ob::ScanPace::kQuietNs) {
            for (int i = 0; i < 200; ++i) x = x * 6364136223846793005ULL + 1442695040888963407ULL;
            ob::ScanPool::pace();
        }
        spun = x | 1;
    });
    for (int i = 0; i < 5000 && !task->done(); ++i) std::this_thread::sleep_for(1ms);
    ASSERT_TRUE(task->done()) << "no pool thread ran the task";
    pool.run_or_wait(*task);
    pool.stop();   // the thread joined, so what it counted is in the pool's counts
    EXPECT_NE(spun.load(), 0u);
    EXPECT_GE(pool.entries(), 10u) << "a pool thread worked 20 quiet intervals and entered the kernel "
                                   << pool.entries() << " time(s)";
    EXPECT_LE(pool.gave_up(), pool.entries());
}
