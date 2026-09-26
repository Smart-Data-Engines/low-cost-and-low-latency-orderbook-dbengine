// #165 part 2b: which of a partition's segments merge, as a function of what is known of them.
//
// The policy decides how often each row is written again and how many segments a partition keeps,
// so it is a pure function and is held here without a clock or a disk: eight segments of one level
// make a merge, a full segment - what the write ceiling seals - never merges, a segment of another
// partition lying between two members, one not yet vouched for and one of another WAL each end a
// run, and a settled partition merges what is left into as few segments as fit.

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <cstdint>
#include <vector>

#include "orderbook/compaction.hpp"

using ob::compaction::Candidate;
using ob::compaction::kFanIn;
using ob::compaction::kFullRows;
using ob::compaction::kMaxRows;
using ob::compaction::plan;
using Merge = ob::compaction::Run;   // `Run` inside a TEST is testing::Test::Run()

namespace {

Candidate small(uint64_t rows = 4000, uint32_t level = 0) {
    return Candidate{rows, level, /*member=*/true, /*eligible=*/true, /*wal_identity=*/7};
}

std::vector<Candidate> repeat(size_t n, Candidate c) { return std::vector<Candidate>(n, c); }

std::vector<Candidate> operator+(std::vector<Candidate> a, const std::vector<Candidate>& b) {
    a.insert(a.end(), b.begin(), b.end());
    return a;
}

}  // namespace

TEST(CompactionPolicy, EightSegmentsOfOneLevelMakeAMerge) {
    EXPECT_EQ(plan(repeat(kFanIn, small()), false), (std::vector<Merge>{{0, kFanIn}}));
    EXPECT_TRUE(plan(repeat(kFanIn - 1, small()), false).empty())
        << "a partition still receiving rows merged fewer than the fan-in";
    EXPECT_EQ(plan(repeat(2 * kFanIn + 1, small()), false),
              (std::vector<Merge>{{0, kFanIn}, {kFanIn, kFanIn}}));
}

TEST(CompactionPolicy, ASettledPartitionMergesWhatIsLeftIntoAsFewAsFit) {
    EXPECT_EQ(plan(repeat(kFanIn - 1, small()), true), (std::vector<Merge>{{0, kFanIn - 1}}));
    // Levels do not matter once nothing more arrives.
    EXPECT_EQ(plan(repeat(3, small(32'000, 1)) + repeat(2, small()), true),
              (std::vector<Merge>{{0, 5}}));
    // And a single segment is not a merge.
    EXPECT_TRUE(plan({small()}, true).empty());
    // As many as fit, then again: five of 60 000 rows are two segments, the second alone.
    EXPECT_EQ(plan(repeat(5, small(60'000)), true), (std::vector<Merge>{{0, 4}}));
}

TEST(CompactionPolicy, AFullSegmentMergesWithNothing) {
    const Candidate full{kFullRows, 0, true, true, 7};
    EXPECT_TRUE(plan(repeat(kFanIn, full), false).empty());
    EXPECT_TRUE(plan(repeat(kFanIn, full), true).empty());
    // And it ends a stretch: four small ones on either side are not eight.
    EXPECT_TRUE(plan(repeat(4, small()) + std::vector<Candidate>{full} + repeat(4, small()), false)
                    .empty());
    EXPECT_EQ(plan(repeat(4, small()) + std::vector<Candidate>{full} + repeat(4, small()), true),
              (std::vector<Merge>{{0, 4}, {5, 4}}));
}

TEST(CompactionPolicy, WhatLiesBetweenTwoMembersEndsARun) {
    Candidate other = small();
    other.member = false;   // another partition's segment, in the order a scan delivers them
    Candidate pending = small();
    pending.eligible = false;   // not yet vouched for by a checkpoint on the device
    Candidate foreign = small();
    foreign.wal_identity = 9;   // a snapshot's
    for (const Candidate& gap : {other, pending, foreign}) {
        const auto split = repeat(4, small()) + std::vector<Candidate>{gap} + repeat(4, small());
        EXPECT_EQ(plan(split, true), (std::vector<Merge>{{0, 4}, {5, 4}}));
        EXPECT_TRUE(plan(split, false).empty());
    }
    // Two WALs' segments merge, each with their own.
    EXPECT_EQ(plan(repeat(kFanIn, small()) + repeat(kFanIn, foreign), false),
              (std::vector<Merge>{{0, kFanIn}, {kFanIn, kFanIn}}));
}

TEST(CompactionPolicy, EachLevelMergesWithItsOwn) {
    // What a partition holds a while into its period: merged segments before the newest sealed ones.
    EXPECT_EQ(plan(repeat(kFanIn, small(32'000, 1)) + repeat(3, small()), false),
              (std::vector<Merge>{{0, kFanIn}}));
    EXPECT_EQ(plan(repeat(3, small(32'000, 1)) + repeat(kFanIn, small()), false),
              (std::vector<Merge>{{3, kFanIn}}));
}

TEST(CompactionPolicy, AMergeStopsAtTheRowCap) {
    // Eight of 40 000 rows would be 320 000: six are what fit, and two wait for more of their level.
    EXPECT_EQ(plan(repeat(kFanIn, small(40'000, 1)), false), (std::vector<Merge>{{0, 6}}));
    EXPECT_EQ(plan(repeat(kFanIn, small(32'768, 1)), false), (std::vector<Merge>{{0, kFanIn}}))
        << "eight that make exactly the cap are one merge";
}

TEST(CompactionPolicy, ASegmentOfThisWalMergesOnlyOnceACheckpointOnTheDeviceVouchesForIt) {
    using ob::compaction::may_merge;
    using ob::compaction::SegmentFacts;
    const uint64_t local = 7;
    EXPECT_TRUE(may_merge(SegmentFacts{local, true, true, 5}, local, 5));
    EXPECT_FALSE(may_merge(SegmentFacts{local, true, true, 6}, local, 5))
        << "a segment sealed after the last checkpoint on the device was merged: a power cut would "
           "remove the merged segment, whose inputs were already gone";
    // Another WAL's - a snapshot's - is vouched for as it stands, whatever its epoch says.
    EXPECT_TRUE(may_merge(SegmentFacts{9, true, true, 1000}, local, 5));
    // What a merge never takes: a segment of no known WAL (before #63), one whose range was not its
    // rows' (before #166), one of another format.
    EXPECT_FALSE(may_merge(SegmentFacts{0, true, true, 0}, local, 5));
    EXPECT_FALSE(may_merge(SegmentFacts{local, false, true, 1}, local, 5));
    EXPECT_FALSE(may_merge(SegmentFacts{local, true, false, 1}, local, 5));
}

TEST(CompactionPolicy, APartitionSettlesOnceItsPeriodIsOverAndNothingIsAddedToIt) {
    using ob::compaction::kSettle;
    using ob::compaction::settled;
    using ob::compaction::until_settled;
    using std::chrono::nanoseconds;
    const uint64_t end = 1'790'000'000ULL * 1'000'000'000ULL;
    const uint64_t settle = static_cast<uint64_t>(nanoseconds(kSettle).count());
    const nanoseconds quiet = nanoseconds(kSettle);
    EXPECT_TRUE(settled(end, end + settle, quiet));
    EXPECT_FALSE(settled(end, end + settle - 1, quiet)) << "a period not over by kSettle settled";
    EXPECT_FALSE(settled(end, end + 10 * settle, quiet - nanoseconds(1)))
        << "a partition a seal added to within kSettle settled";
    // The wait is the longer of the two, and none once both have passed.
    EXPECT_EQ(until_settled(end, end, quiet), nanoseconds(kSettle));
    EXPECT_EQ(until_settled(end, end + settle, nanoseconds::zero()), nanoseconds(kSettle));
    EXPECT_EQ(until_settled(end, end + settle / 2, quiet - nanoseconds(kSettle) / 4),
              nanoseconds(kSettle) / 2);
    EXPECT_EQ(until_settled(end, end + settle, quiet), nanoseconds::zero());
    // And a period at the end of time does not wrap into one long over.
    EXPECT_FALSE(settled(UINT64_MAX - 1, UINT64_MAX - 1, quiet));
}

RC_GTEST_PROP(CompactionPolicy, RunsAreDisjointOrderedAndWithinTheirBounds, ()) {
    const auto n = *rc::gen::inRange<size_t>(0, 40);
    std::vector<Candidate> segs;
    for (size_t i = 0; i < n; ++i) {
        Candidate c;
        c.rows = *rc::gen::element<uint64_t>(uint64_t{1}, uint64_t{4000}, uint64_t{32'000},
                                              uint64_t{40'000}, uint64_t{65'535},
                                              uint64_t{65'536}, uint64_t{200'000});
        c.level = *rc::gen::inRange<uint32_t>(0, 3);
        c.member = *rc::gen::weightedElement<bool>({{9, true}, {1, false}});
        c.eligible = *rc::gen::weightedElement<bool>({{9, true}, {1, false}});
        c.wal_identity = *rc::gen::element<uint64_t>(uint64_t{7}, uint64_t{9});
        segs.push_back(c);
    }
    const bool settled = *rc::gen::arbitrary<bool>();
    const auto runs = plan(segs, settled);
    size_t after = 0;
    for (const Merge& r : runs) {
        RC_ASSERT(r.first >= after);
        RC_ASSERT(r.count >= 2u);
        RC_ASSERT(r.first + r.count <= segs.size());
        uint64_t rows = 0;
        for (size_t i = r.first; i < r.first + r.count; ++i) {
            const Candidate& c = segs[i];
            RC_ASSERT(c.member);
            RC_ASSERT(c.eligible);
            RC_ASSERT(c.rows < kFullRows);
            RC_ASSERT(c.wal_identity == segs[r.first].wal_identity);
            if (!settled) RC_ASSERT(c.level == segs[r.first].level);
            rows += c.rows;
        }
        RC_ASSERT(rows <= kMaxRows);
        if (!settled) RC_ASSERT(r.count <= kFanIn);
        after = r.first + r.count;
    }
}
