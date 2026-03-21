// Feature: ha-automatic-failover
// Property 9: Election winner determinism
//
// For any set of candidate replicas with published WAL positions, the election
// winner should be the replica with the highest WAL position (file_index,
// byte_offset compared lexicographically). If WAL positions are equal, the
// replica with the lexicographically lowest node_id should win. This ordering
// should be deterministic and total.

#include "orderbook/failover.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>

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

} // namespace
