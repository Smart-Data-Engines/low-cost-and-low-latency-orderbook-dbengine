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
