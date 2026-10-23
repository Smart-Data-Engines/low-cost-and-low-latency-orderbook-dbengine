// OB-12, stage 2: the pool a scan's candidates are read on.
//
// What a query needs from it, each held here: a task no thread has taken runs on the query's own
// thread, so a query never waits on the queue; a task a thread has taken is waited for; what a
// task's work throws reaches the query that waits for it; a task the query no longer needs is
// dropped without running; and a pool that stops leaves its queued tasks to their queries, which run
// them, rather than stranding a query. A pool of no threads runs everything where it is waited for.

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
