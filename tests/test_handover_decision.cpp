// #204: a handover's successor stands without the election wait, and only on a conjunction.
//
// A planned FAILOVER used to leave the cluster without a primary for a whole lease TTL - the
// target, like every candidate since #82, waited for "the previous holder to have certainly
// stepped down" - and to lose the acknowledged writes its target had not received when the stream
// stopped. The outgoing primary now steps down first, keeps streaming, and says so in its intent;
// the target stands at once when that statement is live, names it, is at the term it knows, and
// its own position is at the end of the stream the statement names. Each conjunct is one test
// here: weakening any of them lets a successor stand while the old holder might still take writes,
// or without writes it acknowledged.

#include "orderbook/coordinator.hpp"
#include "orderbook/failover.hpp"
#include "orderbook/stream_position.hpp"

#include <gtest/gtest.h>

#include <optional>
#include <string>

namespace {

using ob::HandoverIntent;
using ob::StreamPosition;
using ob::VacantLeaderAction;
using ob::decide_on_vacant_leader;

constexpr uint64_t kNow = 1'000'000;
constexpr uint64_t kTerm = 7;
constexpr uint64_t kStream = 0xABCDEF;

/// A live intent from A to B, with A's statement that it stepped down at `kTerm` and its stream
/// ends at 3:5000.
HandoverIntent stated_intent() {
    HandoverIntent i;
    i.target_node_id    = "node_B";
    i.from_node_id      = "node_A";
    i.deadline_ns       = kNow + 5'000;
    i.stepped_down_term = kTerm;
    i.stream_id         = kStream;
    i.stream_file       = 3;
    i.stream_offset     = 5000;
    return i;
}

StreamPosition at(uint32_t file, uint64_t offset, uint64_t stream = kStream) {
    return StreamPosition{stream, file, offset};
}

VacantLeaderAction decide(const std::optional<HandoverIntent>& intent,
                          const std::optional<StreamPosition>& have, bool wait_elapsed = false,
                          const std::string& self = "node_B", uint64_t known_term = kTerm) {
    return decide_on_vacant_leader(intent, self, kNow, known_term, have, wait_elapsed);
}

// ── stream_covers ─────────────────────────────────────────────────────────────

TEST(StreamCovers, TheEndItselfIsCovered) {
    EXPECT_TRUE(ob::stream_covers(at(3, 5000), at(3, 5000)));
}

TEST(StreamCovers, OneByteShortIsNot) {
    EXPECT_FALSE(ob::stream_covers(at(3, 4999), at(3, 5000)));
}

TEST(StreamCovers, ALaterFileIsPastAnyOffsetOfAnEarlierOne) {
    EXPECT_TRUE(ob::stream_covers(at(4, 0), at(3, 5000)));
    EXPECT_FALSE(ob::stream_covers(at(2, 9'000'000), at(3, 5000)));
}

TEST(StreamCovers, AnotherStreamIsNeverCoveredWhateverItsNumbers) {
    // Two WALs give the same offsets to different records (#61): a position in another stream says
    // nothing about this one.
    EXPECT_FALSE(ob::stream_covers(at(9, 9'000'000, kStream + 1), at(3, 5000)));
}

TEST(StreamCovers, AnUnknownStreamIsNotCovered) {
    EXPECT_FALSE(ob::stream_covers(at(9, 9'000'000, 0), at(3, 5000, 0)));
}

// ── The statement in the intent ───────────────────────────────────────────────

TEST(HandoverStatement, RoundTripsThroughTheIntent) {
    const HandoverIntent in = stated_intent();
    HandoverIntent out;
    ASSERT_TRUE(HandoverIntent::from_json(in.to_json(), out));
    EXPECT_EQ(out.stepped_down_term, kTerm);
    EXPECT_EQ(out.stream_id, kStream);
    EXPECT_EQ(out.stream_file, 3u);
    EXPECT_EQ(out.stream_offset, 5000u);
    EXPECT_TRUE(out.has_stream_end());
}

TEST(HandoverStatement, TheFirstPublicationIsWhatEveryVersionWrote) {
    // Byte for byte the pre-#204 JSON, which is what lets an older node read the first publication
    // as it always did.
    HandoverIntent first;
    first.target_node_id = "node_B";
    first.from_node_id   = "node_A";
    first.deadline_ns    = 42;
    EXPECT_EQ(first.to_json(), R"({"deadline_ns":42,"from_node_id":"node_A","target_node_id":"node_B"})");
}

TEST(HandoverStatement, AnIntentWithoutOneReadsAsNoStatement) {
    HandoverIntent out;
    ASSERT_TRUE(HandoverIntent::from_json(
        R"({"deadline_ns":42,"from_node_id":"node_A","target_node_id":"node_B"})", out));
    EXPECT_EQ(out.stepped_down_term, 0u);
    EXPECT_FALSE(out.has_stream_end());
}

TEST(HandoverStatement, AStepDownWithoutAStreamCarriesNoEnd) {
    HandoverIntent in = stated_intent();
    in.stream_id = 0;
    in.stream_file = 0;
    in.stream_offset = 0;
    HandoverIntent out;
    ASSERT_TRUE(HandoverIntent::from_json(in.to_json(), out));
    EXPECT_EQ(out.stepped_down_term, kTerm);
    EXPECT_FALSE(out.has_stream_end());
}

TEST(HandoverStatement, APartialStreamEndIsNoEnd) {
    // A successor waits for the end it was told; one it was half told is none it could reach.
    HandoverIntent out;
    ASSERT_TRUE(HandoverIntent::from_json(
        R"({"deadline_ns":42,"from_node_id":"node_A","stepped_down_term":7,"stream_id":9,)"
        R"("target_node_id":"node_B"})", out));
    EXPECT_EQ(out.stepped_down_term, 7u);
    EXPECT_FALSE(out.has_stream_end());
}

// ── decide_on_vacant_leader ───────────────────────────────────────────────────

TEST(VacantLeader, TheTargetHoldingTheWholeStreamStandsWithoutTheWait) {
    EXPECT_EQ(decide(stated_intent(), at(3, 5000)), VacantLeaderAction::StandForHandover);
    EXPECT_EQ(decide(stated_intent(), at(4, 100)), VacantLeaderAction::StandForHandover);
}

TEST(VacantLeader, TheTargetShortOfTheEndWaitsForTheRest) {
    EXPECT_EQ(decide(stated_intent(), at(3, 4999)), VacantLeaderAction::AwaitStream);
}

TEST(VacantLeader, TheTargetShortOfTheEndStandsOnceTheElectionWaitIsOver) {
    // What it did before #204: stand with what it has. Waiting for the stream never makes a
    // handover slower than it was.
    EXPECT_EQ(decide(stated_intent(), at(3, 4999), /*wait_elapsed=*/true),
              VacantLeaderAction::Stand);
}

TEST(VacantLeader, ATargetFollowingNoStreamWaitsForTheRest) {
    EXPECT_EQ(decide(stated_intent(), std::nullopt), VacantLeaderAction::AwaitStream);
}

TEST(VacantLeader, ATargetInAnotherStreamIsNotAtItsEnd) {
    EXPECT_EQ(decide(stated_intent(), at(9, 9'000'000, kStream + 1)),
              VacantLeaderAction::AwaitStream);
}

TEST(VacantLeader, AStatementWithoutAStreamLeavesNothingToWaitFor) {
    HandoverIntent i = stated_intent();
    i.stream_id = 0;
    EXPECT_EQ(decide(i, std::nullopt), VacantLeaderAction::StandForHandover);
}

TEST(VacantLeader, AStatementAtAnotherTermSaysNothingAboutThisLeader) {
    // An intent that outlived its handover - its target won and could not clear it, say - names a
    // term that is not the last one this node knows. The leader of that later term made no statement.
    EXPECT_EQ(decide(stated_intent(), at(3, 5000), false, "node_B", kTerm + 1),
              VacantLeaderAction::Wait);
    EXPECT_EQ(decide(stated_intent(), at(3, 5000), false, "node_B", kTerm - 1),
              VacantLeaderAction::Wait);
}

TEST(VacantLeader, WithoutAStatementTheTargetWaitsAsBefore) {
    // The first publication, or an intent from a node older than #204.
    HandoverIntent i = stated_intent();
    i.stepped_down_term = 0;
    EXPECT_EQ(decide(i, at(3, 5000)), VacantLeaderAction::Wait);
    EXPECT_EQ(decide(i, at(3, 5000), /*wait_elapsed=*/true), VacantLeaderAction::Stand);
}

TEST(VacantLeader, AnExpiredIntentIsAnOrdinaryElection) {
    HandoverIntent i = stated_intent();
    i.deadline_ns = kNow;   // is_active() is deadline > now
    EXPECT_EQ(decide(i, at(3, 5000)), VacantLeaderAction::Wait);
    EXPECT_EQ(decide(i, at(3, 5000), /*wait_elapsed=*/true), VacantLeaderAction::Stand);
}

TEST(VacantLeader, AnotherNodeDefersToTheTargetWhateverItHolds) {
    EXPECT_EQ(decide(stated_intent(), at(3, 5000), false, "node_C"), VacantLeaderAction::Defer);
    EXPECT_EQ(decide(stated_intent(), at(3, 5000), true, "node_C"), VacantLeaderAction::Defer);
}

TEST(VacantLeader, TheNodeThatHandedOverDefersToo) {
    EXPECT_EQ(decide(stated_intent(), std::nullopt, true, "node_A"), VacantLeaderAction::Defer);
}

TEST(VacantLeader, NoIntentIsAnOrdinaryElection) {
    EXPECT_EQ(decide(std::nullopt, at(3, 5000)), VacantLeaderAction::Wait);
    EXPECT_EQ(decide(std::nullopt, at(3, 5000), /*wait_elapsed=*/true), VacantLeaderAction::Stand);
}

}  // namespace
