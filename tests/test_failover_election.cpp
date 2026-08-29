// Feature: ha-automatic-failover
// Property 9: Election winner determinism
//
// For any set of candidate replicas with published WAL positions, the election
// winner should be the replica with the highest WAL position (file_index,
// byte_offset compared lexicographically). If WAL positions are equal, the
// replica with the lexicographically lowest node_id should win. This ordering
// should be deterministic and total.

#include "orderbook/coordinator.hpp"
#include "orderbook/failover.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <random>
#include <string>
#include <chrono>
#include <optional>
#include <vector>

namespace {

using ob::PublishedPosition;
using ob::elect_winner;

// ── RapidCheck property test ─────────────────────────────────────────────────

TEST(FailoverElection, Property9_WinnerDeterminism) {
    rc::check("election winner is highest WAL position, tie-break lowest node_id",
              []() {
        // Generate 2..10 candidates.
        const auto n = *rc::gen::inRange(2, 11);
        std::vector<PublishedPosition> positions;
        positions.reserve(static_cast<size_t>(n));

        for (int i = 0; i < n; ++i) {
            PublishedPosition p;
            p.node_id = "node_" + std::to_string(*rc::gen::inRange(0, 1000));
            p.wal_file_index = *rc::gen::inRange<uint32_t>(0, 100);
            p.wal_byte_offset = *rc::gen::inRange<size_t>(0, 1'000'000);
            positions.push_back(std::move(p));
        }

        // Compute winner.
        const auto* winner = elect_winner(positions);
        RC_ASSERT(winner != nullptr);

        // Verify: no other candidate has a strictly better position.
        for (const auto& p : positions) {
            if (&p == winner) continue;

            if (p.wal_file_index > winner->wal_file_index) {
                RC_FAIL("candidate has higher file_index than winner");
            }
            if (p.wal_file_index == winner->wal_file_index) {
                if (p.wal_byte_offset > winner->wal_byte_offset) {
                    RC_FAIL("candidate has higher byte_offset than winner");
                }
                if (p.wal_byte_offset == winner->wal_byte_offset) {
                    RC_ASSERT(winner->node_id <= p.node_id);
                }
            }
        }

        // Verify determinism: shuffling should produce the same winner.
        auto shuffled = positions;
        std::shuffle(shuffled.begin(), shuffled.end(),
                     std::mt19937{42});
        const auto* winner2 = elect_winner(shuffled);
        RC_ASSERT(winner2 != nullptr);
        RC_ASSERT(winner2->node_id == winner->node_id);
        RC_ASSERT(winner2->wal_file_index == winner->wal_file_index);
        RC_ASSERT(winner2->wal_byte_offset == winner->wal_byte_offset);
    });
}

// ── Boundary tests ───────────────────────────────────────────────────────────

TEST(FailoverElection, EmptyPositions) {
    std::vector<PublishedPosition> empty;
    EXPECT_EQ(elect_winner(empty), nullptr);
}

TEST(FailoverElection, SingleCandidate) {
    std::vector<PublishedPosition> positions;
    positions.push_back({"nodeA", 5, 8192});
    const auto* w = elect_winner(positions);
    ASSERT_NE(w, nullptr);
    EXPECT_EQ(w->node_id, "nodeA");
}

TEST(FailoverElection, TieBreakByNodeId) {
    std::vector<PublishedPosition> positions;
    positions.push_back({"nodeC", 5, 8192});
    positions.push_back({"nodeA", 5, 8192});
    positions.push_back({"nodeB", 5, 8192});

    const auto* w = elect_winner(positions);
    ASSERT_NE(w, nullptr);
    EXPECT_EQ(w->node_id, "nodeA");
}

TEST(FailoverElection, HigherFileIndexWins) {
    std::vector<PublishedPosition> positions;
    positions.push_back({"nodeA", 3, 999999});
    positions.push_back({"nodeB", 5, 100});

    const auto* w = elect_winner(positions);
    ASSERT_NE(w, nullptr);
    EXPECT_EQ(w->node_id, "nodeB");
}

TEST(FailoverElection, HigherOffsetWins) {
    std::vector<PublishedPosition> positions;
    positions.push_back({"nodeA", 5, 4096});
    positions.push_back({"nodeB", 5, 8192});

    const auto* w = elect_winner(positions);
    ASSERT_NE(w, nullptr);
    EXPECT_EQ(w->node_id, "nodeB");
}

// ── HandoverIntent ────────────────────────────────────────────────────────────

TEST(HandoverIntent, JsonRoundTrip) {
    ob::HandoverIntent in;
    in.target_node_id = "node_B";
    in.from_node_id   = "node_A";
    in.deadline_ns    = 1755164400000000000ULL;

    ob::HandoverIntent out;
    ASSERT_TRUE(ob::HandoverIntent::from_json(in.to_json(), out));
    EXPECT_EQ(out.target_node_id, in.target_node_id);
    EXPECT_EQ(out.from_node_id, in.from_node_id);
    EXPECT_EQ(out.deadline_ns, in.deadline_ns);
}

TEST(HandoverIntent, RejectsMalformedJson) {
    ob::HandoverIntent out;

    // Not JSON at all.
    EXPECT_FALSE(ob::HandoverIntent::from_json("", out));
    EXPECT_FALSE(ob::HandoverIntent::from_json("not json", out));
    EXPECT_FALSE(ob::HandoverIntent::from_json("{}", out));

    // Missing target: the intent would say nothing.
    EXPECT_FALSE(ob::HandoverIntent::from_json(
        R"({"deadline_ns":1,"from_node_id":"node_A"})", out));

    // Missing origin.
    EXPECT_FALSE(ob::HandoverIntent::from_json(
        R"({"deadline_ns":1,"target_node_id":"node_B"})", out));

    // Missing deadline: the intent would never expire, which is exactly the
    // deadlock the deadline exists to prevent.
    EXPECT_FALSE(ob::HandoverIntent::from_json(
        R"({"from_node_id":"node_A","target_node_id":"node_B"})", out));

    // Deadline present but not a number.
    EXPECT_FALSE(ob::HandoverIntent::from_json(
        R"({"deadline_ns":"soon","from_node_id":"node_A","target_node_id":"node_B"})", out));
}

TEST(HandoverIntent, ExpiresAtDeadline) {
    ob::HandoverIntent intent;
    intent.target_node_id = "node_B";
    intent.from_node_id   = "node_A";
    intent.deadline_ns    = 1000;

    EXPECT_TRUE(intent.is_active(999));
    EXPECT_FALSE(intent.is_active(1000));   // deadline reached: no longer active
    EXPECT_FALSE(intent.is_active(1001));
}

TEST(HandoverIntent, KeyPath) {
    EXPECT_EQ(ob::coordinator_handover_key("/ob/"), "/ob/handover");
    EXPECT_EQ(ob::coordinator_handover_key("/cluster1/"), "/cluster1/handover");
}

RC_GTEST_PROP(HandoverIntentProperty, prop_json_round_trip_preserves_fields,
              (const std::string& raw_target, const std::string& raw_from,
               uint64_t deadline)) {
    // Node ids come from configuration and etcd keys, so restrict to the
    // characters those actually allow.
    auto clean = [](const std::string& in) {
        std::string out;
        for (char c : in) {
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '_' || c == '-') {
                out.push_back(c);
            }
        }
        return out;
    };

    ob::HandoverIntent in;
    in.target_node_id = clean(raw_target);
    in.from_node_id   = clean(raw_from);
    in.deadline_ns    = deadline;

    RC_PRE(!in.target_node_id.empty());
    RC_PRE(!in.from_node_id.empty());
    RC_PRE(in.deadline_ns > 0ULL);

    ob::HandoverIntent out;
    RC_ASSERT(ob::HandoverIntent::from_json(in.to_json(), out));
    RC_ASSERT(out.target_node_id == in.target_node_id);
    RC_ASSERT(out.from_node_id == in.from_node_id);
    RC_ASSERT(out.deadline_ns == in.deadline_ns);
}

} // namespace

// ═══════════════════════════════════════════════════════════════════════════════
// decide_election: whether to take the role now, or wait for a better-placed replica
// ═══════════════════════════════════════════════════════════════════════════════
//
// elect_winner() above has had tests since it was written, and had no callers in src/ until #70:
// promotion was a create-only CAS race, so the role went to whoever polled first rather than to the
// replica that lost the least. These cover the decision that finally uses it — including the one
// that stops the preference from becoming an outage.

namespace {

ob::PublishedPosition at(const std::string& node, uint32_t file, size_t offset) {
    ob::PublishedPosition p;
    p.node_id         = node;
    p.wal_file_index  = file;
    p.wal_byte_offset = offset;
    return p;
}

constexpr auto kWindow = std::chrono::milliseconds(3000);
constexpr auto kZero   = std::chrono::milliseconds(0);

}  // namespace

TEST(DecideElection, PromotesImmediatelyWhenNothingIsPublished) {
    EXPECT_EQ(ob::decide_election({}, "node-0", kZero, kWindow),
              ob::ElectionDecision::PromoteNow)
        << "with no positions this has to behave as it did before #70: race for the key. A cluster "
           "whose nodes predate position publishing must still elect";
}

TEST(DecideElection, PromotesImmediatelyWhenWeAreTheMostAdvanced) {
    const std::vector<ob::PublishedPosition> positions{
        at("node-0", 3, 900), at("node-1", 3, 100)};

    EXPECT_EQ(ob::decide_election(positions, "node-0", kZero, kWindow),
              ob::ElectionDecision::PromoteNow)
        << "nobody holds more of the log than we do, so there is nothing to wait for";
}

TEST(DecideElection, DefersToAReplicaThatHoldsMore) {
    const std::vector<ob::PublishedPosition> positions{
        at("node-0", 3, 100), at("node-1", 3, 900)};

    EXPECT_EQ(ob::decide_election(positions, "node-0", kZero, kWindow),
              ob::ElectionDecision::Defer)
        << "node-1 lost less, so it should have the role — this is the whole point of the item";
}

TEST(DecideElection, AFurtherFileBeatsAFurtherOffset) {
    const std::vector<ob::PublishedPosition> positions{
        at("node-0", 4, 0), at("node-1", 3, 999999)};

    EXPECT_EQ(ob::decide_election(positions, "node-0", kZero, kWindow),
              ob::ElectionDecision::PromoteNow)
        << "a later WAL file is further along than a large offset in an earlier one";
}

TEST(DecideElection, PromotesAfterTheWindowWhenTheBetterNodeNeverDoes) {
    // The case that keeps this preference from becoming an outage. Positions are written to etcd
    // without a lease, so a node that died leaves its position behind for ever: deferring to it
    // without a deadline would leave the cluster with no primary at all, which is worse than the
    // defect being fixed.
    const std::vector<ob::PublishedPosition> positions{
        at("node-0", 3, 100), at("node-1", 9, 900)};

    EXPECT_EQ(ob::decide_election(positions, "node-0", std::chrono::milliseconds(2999), kWindow),
              ob::ElectionDecision::Defer);
    EXPECT_EQ(ob::decide_election(positions, "node-0", kWindow, kWindow),
              ob::ElectionDecision::PromoteAfterWindow)
        << "a dead node's stale position must not block promotion for ever";
}

TEST(DecideElection, TwoCandidatesReadingTheSamePositionsDoNotBothDefer) {
    // Equal positions: elect_winner breaks the tie on the lower node id, deterministically, so
    // exactly one of the two sees itself as the winner. Both deferring to each other would be the
    // same outage in a different shape.
    const std::vector<ob::PublishedPosition> positions{
        at("node-0", 3, 500), at("node-1", 3, 500)};

    const auto for_zero = ob::decide_election(positions, "node-0", kZero, kWindow);
    const auto for_one  = ob::decide_election(positions, "node-1", kZero, kWindow);

    EXPECT_EQ(for_zero, ob::ElectionDecision::PromoteNow);
    EXPECT_EQ(for_one, ob::ElectionDecision::Defer);
    EXPECT_NE(for_zero, for_one) << "if both sides read the same list, exactly one may promote";
}

TEST(DecideElection, ANodeMissingFromTheListStillDefersOnlyForTheWindow) {
    // Right after a start, before the first publish, our own position is not in etcd yet. Deferring
    // is right — someone else is demonstrably ahead — but only until the window runs out.
    const std::vector<ob::PublishedPosition> positions{at("node-1", 5, 10)};

    EXPECT_EQ(ob::decide_election(positions, "node-0", kZero, kWindow),
              ob::ElectionDecision::Defer);
    EXPECT_EQ(ob::decide_election(positions, "node-0", std::chrono::milliseconds(3001), kWindow),
              ob::ElectionDecision::PromoteAfterWindow);
}

TEST(DecideElection, AZeroWindowMeansNeverDefer) {
    const std::vector<ob::PublishedPosition> positions{
        at("node-0", 1, 0), at("node-1", 9, 900)};

    EXPECT_EQ(ob::decide_election(positions, "node-0", kZero, std::chrono::milliseconds(0)),
              ob::ElectionDecision::PromoteAfterWindow)
        << "--election-deference-ms=0 has to switch the preference off rather than deadlock it";
}

// ── The election wait after the leader key goes absent (#82) ──────────────────
//
// A revoked or expired lease deletes the leader key immediately, while the previous holder learns on
// its next poll. A candidate that claims the vacated key at once can therefore coexist with a node
// that still believes it is primary — and both accept writes while both believe it. CI reproduced
// exactly that on a shared runner.
//
// The wait is the fix, and this is the decision it turns on, tested as a function of time rather
// than through a cluster.

TEST(ElectionWait, NoAbsenceObservedMeansNoCampaign) {
    // The case that matters most, and the one that used to be indistinguishable from an absent key:
    // a read that failed records nothing, so nothing has elapsed.
    const auto now = std::chrono::steady_clock::now();
    EXPECT_FALSE(ob::election_wait_elapsed(std::nullopt, now, 10'000));
    EXPECT_FALSE(ob::election_wait_elapsed(std::nullopt, now, 0))
        << "not even with the wait switched off: there is nothing to have waited for";
}

TEST(ElectionWait, TheWindowIsMeasuredFromTheFirstSighting) {
    const auto seen = std::chrono::steady_clock::now();

    EXPECT_FALSE(ob::election_wait_elapsed(seen, seen, 10'000));
    EXPECT_FALSE(ob::election_wait_elapsed(seen, seen + std::chrono::milliseconds(9'999), 10'000));
    EXPECT_TRUE(ob::election_wait_elapsed(seen, seen + std::chrono::milliseconds(10'000), 10'000))
        << "the boundary is inclusive: waiting exactly the window is waiting the window";
    EXPECT_TRUE(ob::election_wait_elapsed(seen, seen + std::chrono::seconds(30), 10'000));
}

TEST(ElectionWait, AZeroWindowIsNoWait) {
    // Reachable only from a caller that asks for it — the configuration reads 0 as "derive from the
    // lease TTL" and rejects a negative value at startup.
    const auto seen = std::chrono::steady_clock::now();
    EXPECT_TRUE(ob::election_wait_elapsed(seen, seen, 0));
}

TEST(ElectionWait, ClockGoingBackwardsDoesNotGrantAnEarlyCampaign) {
    // steady_clock should not go backwards, but the arithmetic is on a duration that could be
    // negative if it ever did, and "negative elapsed" must not compare as "waited long enough".
    const auto seen = std::chrono::steady_clock::now();
    EXPECT_FALSE(ob::election_wait_elapsed(seen, seen - std::chrono::seconds(5), 10'000));
}
