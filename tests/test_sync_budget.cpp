// SyncBudget: how much WAL may wait for a sync before a writer waits for one (#190, step 9).
//
// Pure arithmetic over the syncs it is told about, so no device and no clock: each test hands it
// a sync's bytes and duration and reads the budget back.

#include "orderbook/sync_budget.hpp"

#include <gtest/gtest.h>

#include <chrono>

namespace {

using ob::SyncBudget;
using std::chrono::milliseconds;

constexpr uint64_t MiB = 1ull << 20;

SyncBudget::Config config() {
    SyncBudget::Config c;
    c.initial_bytes = 4 * MiB;
    c.floor_bytes   = 1 * MiB;
    c.ceiling_bytes = 64 * MiB;
    c.target        = milliseconds(1000);
    return c;
}

}  // namespace

TEST(SyncBudget, BeginsAtTheInitialBudgetBecauseBeforeASyncNothingIsKnown) {
    SyncBudget budget(config());
    EXPECT_EQ(budget.bytes(), 4 * MiB);
}

TEST(SyncBudget, AQuickSyncOfHalfTheBudgetDoublesIt) {
    SyncBudget budget(config());
    EXPECT_EQ(budget.on_sync(2 * MiB, milliseconds(20)), SyncBudget::Change::Raised);
    EXPECT_EQ(budget.bytes(), 8 * MiB);
}

TEST(SyncBudget, AQuickSyncOfLessThanHalfDoesNotRaiseIt) {
    // Light traffic says nothing about what the device takes: grown in a quiet hour, the budget
    // would hand the next burst the whole pending queue.
    SyncBudget budget(config());
    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(budget.on_sync(64 * 1024, milliseconds(1)), SyncBudget::Change::None);
    }
    EXPECT_EQ(budget.bytes(), 4 * MiB);
}

TEST(SyncBudget, ASlowSyncSetsTheBudgetToWhatTheDeviceWritesInTheTarget) {
    // 4 MiB in 800 ms is 5 MiB/s: what the device writes in the one-second target.
    SyncBudget budget(config());
    ASSERT_EQ(budget.on_sync(4 * MiB, milliseconds(800)), SyncBudget::Change::Raised);
    EXPECT_NEAR(static_cast<double>(budget.bytes()), 5.0 * MiB, 2.0);
    // Then the device slows: 5 MiB in 2.5 s is 2 MiB/s, and the budget comes down at once.
    ASSERT_EQ(budget.on_sync(5 * MiB, milliseconds(2500)), SyncBudget::Change::Lowered);
    EXPECT_NEAR(static_cast<double>(budget.bytes()), 2.0 * MiB, 2.0);
}

TEST(SyncBudget, ASlowSyncOfLittleStillLowersIt) {
    // A sync slow for its size is the device busy, whatever it was asked for.
    SyncBudget budget(config());
    ASSERT_EQ(budget.on_sync(1 * MiB, milliseconds(500)), SyncBudget::Change::Lowered);
    EXPECT_EQ(budget.bytes(), 2 * MiB) << "1 MiB in 0.5 s is 2 MiB a second, below the 4 in force";
}

TEST(SyncBudget, NoSyncRaisesItMoreThanTwofold) {
    // 4 MiB in 260 ms - just past a quick sync - is about 15 MiB a second; doubled is the most.
    SyncBudget budget(config());
    ASSERT_EQ(budget.on_sync(4 * MiB, milliseconds(260)), SyncBudget::Change::Raised);
    EXPECT_EQ(budget.bytes(), 8 * MiB);
}

TEST(SyncBudget, ASlowSyncOfLessThanHalfDoesNotRaiseIt) {
    // 3 MiB in 300 ms is 10 MiB a second, which would allow 10 of an 8 MiB budget - but 3 of 8 is
    // less than half, and only a sync that used the budget may raise it.
    SyncBudget budget(config());
    ASSERT_EQ(budget.on_sync(2 * MiB, milliseconds(20)), SyncBudget::Change::Raised);
    ASSERT_EQ(budget.bytes(), 8 * MiB);
    EXPECT_EQ(budget.on_sync(3 * MiB, milliseconds(300)), SyncBudget::Change::None);
    EXPECT_EQ(budget.bytes(), 8 * MiB);
}

TEST(SyncBudget, TheTwoRulesMeetWhereASyncTookHalfTheTarget) {
    // A sync of the whole budget in half the target measures twice the budget, which is what a
    // quick sync's doubling gives too: no step in the budget as a sync crosses the quick bound.
    SyncBudget quick(config());
    SyncBudget slow(config());
    ASSERT_EQ(quick.on_sync(4 * MiB, milliseconds(250)), SyncBudget::Change::Raised);
    ASSERT_EQ(slow.on_sync(4 * MiB, milliseconds(500)), SyncBudget::Change::Raised);
    EXPECT_EQ(quick.bytes(), slow.bytes());
}

TEST(SyncBudget, StaysBetweenItsFloorAndItsCeiling) {
    SyncBudget budget(config());
    ASSERT_EQ(budget.on_sync(4 * MiB, milliseconds(60'000)), SyncBudget::Change::Lowered);
    EXPECT_EQ(budget.bytes(), 1 * MiB) << "one bad measurement cannot stop writes";
    for (int i = 0; i < 20; ++i) (void)budget.on_sync(budget.bytes(), milliseconds(5));
    EXPECT_EQ(budget.bytes(), 64 * MiB);
    EXPECT_EQ(budget.on_sync(64 * MiB, milliseconds(5)), SyncBudget::Change::None);
}

TEST(SyncBudget, ASyncOfNothingSaysNothing) {
    SyncBudget budget(config());
    EXPECT_EQ(budget.on_sync(0, milliseconds(3000)), SyncBudget::Change::None);
    EXPECT_EQ(budget.bytes(), 4 * MiB);
}

TEST(SyncBudget, TheInitialBudgetIsHeldToTheFloorAndTheCeiling) {
    SyncBudget::Config c = config();
    c.initial_bytes = 1024;
    EXPECT_EQ(SyncBudget(c).bytes(), 1 * MiB);
    c.initial_bytes = 1ull << 40;
    EXPECT_EQ(SyncBudget(c).bytes(), 64 * MiB);
}
