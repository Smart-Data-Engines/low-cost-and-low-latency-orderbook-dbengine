// Unit tests for SequenceTracker: assignment, per-origin gap detection, counter recovery.
//
// These deliberately need no Engine, no etcd and no ports. Gap detection only means
// anything with several origins, and the only Engine entry point for a foreign origin is
// apply_remote_delta(), which requires multi-master to be running — so keeping this state
// in Engine would have made the interesting cases cost half a cluster to test.

#include "orderbook/sequence_tracker.hpp"

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace {

constexpr uint16_t kLocal = 0;   // origin id outside multi-master

}  // namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Assignment
// ═══════════════════════════════════════════════════════════════════════════════

TEST(SequenceTracker, AssignsFromOneForAnUnseenSymbol) {
    ob::SequenceTracker t;

    auto first = t.observe("BTC.BINANCE", kLocal, 0);
    EXPECT_EQ(first.sequence_number, 1u) << "0 is reserved for 'nobody assigned one', so the "
                                            "first real number must be 1";
    EXPECT_TRUE(first.assigned);
    EXPECT_FALSE(first.gap) << "the first record of a stream cannot be a gap";

    auto second = t.observe("BTC.BINANCE", kLocal, 0);
    EXPECT_EQ(second.sequence_number, 2u);
    EXPECT_FALSE(second.gap);
}

TEST(SequenceTracker, CountsSymbolsIndependently) {
    ob::SequenceTracker t;

    // Interleaved, because one shared counter would pass a sequential check.
    EXPECT_EQ(t.observe("A.EX", kLocal, 0).sequence_number, 1u);
    EXPECT_EQ(t.observe("B.EX", kLocal, 0).sequence_number, 1u);
    EXPECT_EQ(t.observe("A.EX", kLocal, 0).sequence_number, 2u);
    EXPECT_EQ(t.observe("B.EX", kLocal, 0).sequence_number, 2u);
    EXPECT_EQ(t.symbol_count(), 2u);
}

TEST(SequenceTracker, PassesASuppliedNumberThroughUntouched) {
    ob::SequenceTracker t;

    auto d = t.observe("REPL.EX", kLocal, 7);
    EXPECT_EQ(d.sequence_number, 7u) << "a non-zero number came from whoever originated the "
                                        "record; renumbering it here would make this node "
                                        "disagree with the stream it is copying";
    EXPECT_FALSE(d.assigned);
}

TEST(SequenceTracker, ASuppliedNumberStillAdvancesTheLocalCounter) {
    ob::SequenceTracker t;

    t.observe("MIXED.EX", kLocal, 9);
    auto next = t.observe("MIXED.EX", kLocal, 0);
    EXPECT_EQ(next.sequence_number, 10u)
        << "a node that both accepts client writes and receives a stream handed out a number "
           "already in use";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Gaps, per origin
// ═══════════════════════════════════════════════════════════════════════════════

TEST(SequenceTracker, AHoleInAnOriginsStreamIsAGap) {
    ob::SequenceTracker t;

    t.observe("GAP.EX", 1, 1);
    auto d = t.observe("GAP.EX", 1, 3);

    EXPECT_TRUE(d.gap);
    EXPECT_EQ(d.expected, 2u) << "the gap must say what was missing, or the log cannot tell an "
                                 "operator which records to go looking for";
}

TEST(SequenceTracker, TwoOriginsInterleavingIsNotAGap) {
    ob::SequenceTracker t;

    // This is the case a single counter per symbol cannot express, and the reason gap
    // detection has never run in this engine.
    for (uint64_t seq = 1; seq <= 3; ++seq) {
        for (uint16_t origin : {uint16_t{1}, uint16_t{2}}) {
            auto d = t.observe("MM.EX", origin, seq);
            EXPECT_FALSE(d.gap) << "origin " << origin << " seq " << seq
                                << " was read as a gap, but each origin numbers its own stream";
        }
    }
    EXPECT_EQ(t.high_water("MM.EX", 1), 3u);
    EXPECT_EQ(t.high_water("MM.EX", 2), 3u);
}

TEST(SequenceTracker, AGapInOneOriginDoesNotImplicateAnother) {
    ob::SequenceTracker t;

    t.observe("MM.EX", 1, 1);
    t.observe("MM.EX", 2, 1);
    EXPECT_TRUE(t.observe("MM.EX", 1, 5).gap) << "origin 1 skipped 2-4";
    EXPECT_FALSE(t.observe("MM.EX", 2, 2).gap) << "origin 2 is intact and must stay unaccused";
}

TEST(SequenceTracker, TheFirstRecordFromAnOriginIsNotAGap) {
    ob::SequenceTracker t;

    // A number far from 1: this is a peer that has been writing for a while and whose
    // earlier records this node never saw. Nothing was lost here that this node could name.
    auto d = t.observe("LATE.EX", 7, 5000);
    EXPECT_FALSE(d.gap);
    EXPECT_EQ(t.high_water("LATE.EX", 7), 5000u);
}

TEST(SequenceTracker, ARedeliveredRecordIsNotAGap) {
    ob::SequenceTracker t;

    t.observe("OOO.EX", 1, 1);
    t.observe("OOO.EX", 1, 2);
    t.observe("OOO.EX", 1, 3);

    // Catch-up redelivers on purpose whenever it cannot prove the peer already has a record
    // (#61's design principle: over-deliver rather than lose). Reporting those as gaps would
    // put a GAP record in the WAL for every redelivered row and make the metric noise.
    EXPECT_FALSE(t.observe("OOO.EX", 1, 2).gap)
        << "a record at or below the frontier is a duplicate, not a hole";
    EXPECT_EQ(t.high_water("OOO.EX", 1), 3u)
        << "the maximum went backwards, so the next record would look like a gap";
    EXPECT_EQ(t.frontier("OOO.EX", 1), 3u) << "a duplicate cannot move the frontier either";
    EXPECT_FALSE(t.observe("OOO.EX", 1, 4).gap);
}

TEST(SequenceTracker, TheRecordThatFillsAHoleIsNotItselfAGap) {
    ob::SequenceTracker t;

    t.observe("FILL.EX", 1, 1);
    EXPECT_TRUE(t.observe("FILL.EX", 1, 3).gap) << "2 is missing, so this is a gap";
    EXPECT_FALSE(t.observe("FILL.EX", 1, 2).gap)
        << "2 is exactly what was missing; measuring gaps against the maximum instead of the "
           "frontier would report the repair as a new hole";
    EXPECT_EQ(t.frontier("FILL.EX", 1), 3u);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Counter recovery
// ═══════════════════════════════════════════════════════════════════════════════

TEST(SequenceTracker, RaiseLocalMovesTheCounterUp) {
    ob::SequenceTracker t;

    t.raise_local("R.EX", 41);
    EXPECT_EQ(t.peek_next_local("R.EX"), 42u);
    EXPECT_EQ(t.observe("R.EX", kLocal, 0).sequence_number, 42u);
}

TEST(SequenceTracker, RaiseLocalNeverMovesTheCounterDown) {
    ob::SequenceTracker t;

    t.raise_local("R.EX", 100);
    t.raise_local("R.EX", 5);      // an older segment, read after a newer one
    EXPECT_EQ(t.peek_next_local("R.EX"), 101u)
        << "open() raises the counter from several sources in whatever order it finds them, "
           "so a lower one must not undo a higher one";
}

TEST(SequenceTracker, RaiseLocalOnAnUnseenSymbolIsTheSameAsStartingThere) {
    ob::SequenceTracker t;

    EXPECT_EQ(t.peek_next_local("NEW.EX"), 1u);
    t.raise_local("NEW.EX", 0);    // an old segment written before numbers existed
    EXPECT_EQ(t.peek_next_local("NEW.EX"), 1u)
        << "a segment full of zeros must not push the counter to 1 by accident and must not "
           "push it anywhere else either";
}

TEST(SequenceTracker, SeedLeavesAConservativeFrontierAcrossAHoleInTheTail) {
    ob::SequenceTracker t;
    t.set_local_origin(1);   // the replay of this node's own records, which raise its counter (#184)

    // Replay of a WAL tail with a hole. The hole may be real, or records 2-3 may be sitting
    // in a segment where replay cannot see them — the tail only reaches back to the last
    // checkpoint. The tracker cannot tell the difference, so it takes the conservative side:
    // the frontier stops below the hole, the next catch-up asks for that range again, and a
    // redelivery costs bandwidth. Claiming the records instead would be #61: a node stating
    // it holds what it never received.
    t.seed("S.EX", 1, 1);
    t.seed("S.EX", 1, 4);

    EXPECT_EQ(t.high_water("S.EX", 1), 4u);
    EXPECT_EQ(t.peek_next_local("S.EX"), 5u);
    EXPECT_EQ(t.frontier("S.EX", 1), 1u)
        << "the frontier crossed a hole it has no evidence for";
    EXPECT_TRUE(t.observe("S.EX", 1, 5).gap)
        << "a received record five past a frontier of one is a hole, and saying so is what "
           "makes the range get requested again";
}

TEST(SequenceTracker, ANumberThisNodeAssignedIsNeverAGap) {
    ob::SequenceTracker t;

    // A remote hole holds the frontier down for that origin. The node's own writes must not
    // be dragged into it: they are minted in order in the same critical section, so a GAP
    // record per local insert would be noise. Origin 0 is the local one here.
    t.observe("L.EX", 7, 1);
    t.observe("L.EX", 7, 9);          // remote hole: frontier for origin 7 stays at 1
    ASSERT_EQ(t.frontier("L.EX", 7), 1u);

    for (int i = 0; i < 3; ++i) {
        auto d = t.observe("L.EX", 0, 0);     // 0 = unassigned, so the tracker mints it
        EXPECT_TRUE(d.assigned);
        EXPECT_FALSE(d.gap) << "local write " << d.sequence_number << " was called a gap";
    }
}

TEST(SequenceTracker, DeclareFrontierIsHowARestartedNodeStopsAccusingItself) {
    ob::SequenceTracker t;

    // What Engine::open() does: the counter comes back from the segments, and the node
    // declares that its own records up to it are held — sound only for the local origin,
    // which cannot be missing a record it minted and applied itself.
    t.raise_local("D.EX", 40);
    t.declare_frontier("D.EX", /*origin=*/0, 40);

    EXPECT_EQ(t.frontier("D.EX", 0), 40u);
    auto d = t.observe("D.EX", 0, 0);
    EXPECT_EQ(d.sequence_number, 41u);
    EXPECT_FALSE(d.gap);
}

TEST(SequenceTracker, DeclareFrontierDrainsWhatWasHeldAboveIt) {
    ob::SequenceTracker t;

    t.seed("DR.EX", 5, 10);
    t.seed("DR.EX", 5, 11);
    ASSERT_EQ(t.frontier("DR.EX", 5), 0u) << "nothing contiguous from 1 yet";

    t.declare_frontier("DR.EX", 5, 9);
    EXPECT_EQ(t.frontier("DR.EX", 5), 11u)
        << "declaring up to 9 must absorb the 10 and 11 already held, or the frontier would "
           "understate what is provably there";
    EXPECT_EQ(t.above_frontier_size("DR.EX", 5), 0u);
}

TEST(SequenceTracker, SeedIgnoresRecordsFromBeforeNumbersExisted) {
    ob::SequenceTracker t;

    t.seed("OLD.EX", 0, 0);
    EXPECT_EQ(t.peek_next_local("OLD.EX"), 1u);
    EXPECT_EQ(t.high_water("OLD.EX", 0), 0u)
        << "a zero is the absence of a number, not the number zero, and must not become an "
           "origin's high-water mark — the next real record would look like a gap";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Contiguous frontier (#61): "the highest number I saw" is not "everything up to here"
// ═══════════════════════════════════════════════════════════════════════════════

TEST(SequenceTracker, TheFrontierOnlyMovesThroughContiguousRecords) {
    ob::SequenceTracker t;

    t.observe("F.EX", 1, 1);
    EXPECT_EQ(t.frontier("F.EX", 1), 1u);

    // A live record arriving before catch-up delivers the one behind it. A maximum would
    // call 2 delivered here, and 2 would never be asked for again — which is #61.
    t.observe("F.EX", 1, 3);
    EXPECT_EQ(t.frontier("F.EX", 1), 1u)
        << "the frontier moved past a hole, so the missing record would never be requested";
    EXPECT_EQ(t.high_water("F.EX", 1), 3u) << "the maximum is still tracked, for gap detection";

    t.observe("F.EX", 1, 2);
    EXPECT_EQ(t.frontier("F.EX", 1), 3u)
        << "filling the hole must drain what was already held above the frontier, not just "
           "advance by one";
}

TEST(SequenceTracker, AFrontierIsPerOriginAndPerSymbol) {
    ob::SequenceTracker t;

    t.observe("A.EX", 1, 1);
    t.observe("A.EX", 2, 1);
    t.observe("B.EX", 1, 1);
    t.observe("A.EX", 1, 2);

    EXPECT_EQ(t.frontier("A.EX", 1), 2u);
    EXPECT_EQ(t.frontier("A.EX", 2), 1u);
    EXPECT_EQ(t.frontier("B.EX", 1), 1u);
    EXPECT_EQ(t.frontier("B.EX", 2), 0u) << "nothing was ever seen here, so nothing is held";
}

TEST(SequenceTracker, AnUnboundedHoleDoesNotGrowMemoryWithoutLimit) {
    ob::SequenceTracker t;

    // A peer that has been away for a long time can deliver a very long run above the
    // frontier. Holding all of it is an optimisation, not a correctness requirement, so it
    // is capped — and the frontier stays put, which only means asking for too much later.
    for (uint64_t seq = 2; seq <= 6000; ++seq) t.observe("CAP.EX", 1, seq);

    EXPECT_EQ(t.frontier("CAP.EX", 1), 0u) << "record 1 never arrived, so nothing is contiguous";
    EXPECT_LE(t.above_frontier_size("CAP.EX", 1), 4096u)
        << "the held set grew past its cap, so a long outage would grow memory unbounded";
}

TEST(SequenceTracker, SeedRestoresTheFrontierTooNotJustTheMaximum) {
    ob::SequenceTracker t;

    // Replay of a WAL tail after a restart: this is where the frontier has to come back
    // from, because segments carry no origin and cannot answer "what do I have from whom".
    t.seed("S.EX", 1, 1);
    t.seed("S.EX", 1, 2);
    t.seed("S.EX", 1, 4);

    EXPECT_EQ(t.frontier("S.EX", 1), 2u)
        << "the frontier came back as the maximum, so the hole at 3 would be treated as "
           "delivered and never requested again";
    EXPECT_EQ(t.high_water("S.EX", 1), 4u);
}

TEST(SequenceTracker, AFrontierStartsAtZeroMeaningNothingHeld) {
    ob::SequenceTracker t;
    EXPECT_EQ(t.frontier("NEW.EX", 7), 0u)
        << "0 has to mean 'I have nothing from this origin', because that is what a peer "
           "sends when it has never heard of it, and the answer must be 'send everything'";
}

TEST(SequenceTracker, HasSeenIsWhatMakesRedeliveryIdempotent) {
    ob::SequenceTracker t;

    t.observe("H.EX", 1, 1);
    t.observe("H.EX", 1, 2);
    t.observe("H.EX", 1, 5);        // out of order, held above the frontier

    EXPECT_TRUE(t.has_seen("H.EX", 1, 1));
    EXPECT_TRUE(t.has_seen("H.EX", 1, 2)) << "below the frontier";
    EXPECT_FALSE(t.has_seen("H.EX", 1, 3)) << "the hole must read as not seen, or it is lost";
    EXPECT_TRUE(t.has_seen("H.EX", 1, 5)) << "held above the frontier still counts as applied";
    EXPECT_FALSE(t.has_seen("H.EX", 1, 6));
    EXPECT_FALSE(t.has_seen("H.EX", 2, 1)) << "a different origin's numbering is unrelated";
    EXPECT_FALSE(t.has_seen("OTHER.EX", 1, 1));
}

TEST(SequenceTracker, HasSeenSaysNoForAnUnassignedNumber) {
    ob::SequenceTracker t;
    EXPECT_FALSE(t.has_seen("H.EX", 1, 0))
        << "0 means nobody assigned one, so it cannot have been seen; answering yes would "
           "silently drop writes from an older node";
}

TEST(SequenceTracker, OriginsWithNothingHeldDoNotCountTowardsTheVectorLimit) {
    ob::SequenceTracker t;

    // 50 origins seen mid-stream, so every frontier is 0 and none of them is exportable, plus
    // two that are. Counting all the pairs before filtering would call this vector 52 entries
    // long and refuse to state a position that fits in two.
    for (uint16_t origin = 1; origin <= 50; ++origin) {
        t.observe("Z.EX", origin, 5000);          // no contiguity from 1: frontier stays 0
    }
    t.declare_frontier("A.EX", 1, 10);
    t.declare_frontier("B.EX", 1, 20);

    bool truncated = true;
    const auto entries = t.export_vector(/*limit=*/4, truncated);
    EXPECT_FALSE(truncated) << "a vector of two entries was refused as too large";
    EXPECT_EQ(entries.size(), 2u);
}

TEST(SequenceTracker, AVectorOverTheLimitIsRefusedWholeNotInPart) {
    ob::SequenceTracker t;
    for (int i = 0; i < 10; ++i) t.declare_frontier("S" + std::to_string(i) + ".EX", 1, 5);

    bool truncated = false;
    const auto entries = t.export_vector(/*limit=*/4, truncated);
    EXPECT_TRUE(truncated);
    EXPECT_TRUE(entries.empty())
        << "a partial vector looks complete to the receiver, so the entries left out would "
           "never be asked for";
}

// ── reset() ───────────────────────────────────────────────────────────────────
//
// The one caller entitled to this is a snapshot install, which replaces the node's contents
// wholesale. What matters is that nothing survives: a frontier that outlives the contents it
// described claims records that are no longer on disk.

TEST(SequenceTracker, ResetForgetsFrontiersHeldNumbersAndLocalCounters) {
    ob::SequenceTracker t;
    t.observe("A.EX", 1, 1);
    t.observe("A.EX", 1, 2);
    t.observe("A.EX", 1, 9);          // held above the frontier
    t.observe("A.EX", 0, 0);          // mints a local number, moving next_local

    ASSERT_EQ(t.frontier("A.EX", 1), 2u);
    ASSERT_TRUE(t.has_seen("A.EX", 1, 9));
    ASSERT_GT(t.peek_next_local("A.EX"), 1u);

    t.reset();

    EXPECT_EQ(t.symbol_count(), 0u);
    EXPECT_EQ(t.frontier("A.EX", 1), 0u);
    EXPECT_FALSE(t.has_seen("A.EX", 1, 9));
    EXPECT_FALSE(t.has_seen("A.EX", 1, 1));
    EXPECT_EQ(t.peek_next_local("A.EX"), 1u);

    bool truncated = false;
    EXPECT_TRUE(t.export_vector(4096, truncated).empty());
    EXPECT_FALSE(truncated);
}

TEST(SequenceTracker, ImportAfterResetDoesNotResurrectTheOldFrontier) {
    // The reason reset() exists. import_own_vector() only ever raises, so adopting a snapshot
    // whose frontier is *lower* than ours would keep ours — and ours describes contents that
    // load_snapshot() just discarded.
    ob::SequenceTracker t;
    for (uint64_t s = 1; s <= 100; ++s) t.observe("A.EX", 1, s);
    ASSERT_EQ(t.frontier("A.EX", 1), 100u);

    t.reset();
    t.import_own_vector({{"A.EX", 1, 10}});

    EXPECT_EQ(t.frontier("A.EX", 1), 10u)
        << "the adopted frontier must replace ours, not lose to it";
    EXPECT_FALSE(t.has_seen("A.EX", 1, 50))
        << "50 was in the discarded contents; claiming it is a hole that never gets filled";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Moved frontiers: what the flush tick brings the vector peers are told up to date with (#180)
// ═══════════════════════════════════════════════════════════════════════════════

namespace {

using Frontiers = std::map<std::pair<std::string, uint16_t>, uint64_t>;

Frontiers as_map(const std::vector<ob::SequenceTracker::VectorEntry>& entries) {
    Frontiers out;
    for (const auto& e : entries) out[{e.key, e.origin}] = e.frontier;
    return out;
}

Frontiers moved_now(ob::SequenceTracker& t) {
    const auto moved = t.take_moved_frontiers();
    EXPECT_FALSE(moved.all);
    return as_map(moved.moved);
}

}  // namespace

TEST(SequenceTracker, TheMovedFrontiersAreEachThatMovedOnceAndNothingElse) {
    // Both halves matter. A frontier that moved and is not listed leaves a peer told a stale
    // vector until something else moves it; a listing of what did not move costs the tick a copy,
    // and a redelivery - which catch-up produces on purpose - is the common case of that.
    ob::SequenceTracker t;
    EXPECT_TRUE(t.take_moved_frontiers().all) << "a copy kept from nothing must start by rebuilding";
    EXPECT_TRUE(moved_now(t).empty());

    (void)t.observe("A.EX", 1, 0);                 // assigned: frontier 1
    (void)t.observe("A.EX", 1, 0);                 // and 2: listed once, with where it is now
    (void)t.observe("A.EX", 2, 1);                 // the first number from an origin
    EXPECT_EQ(moved_now(t), (Frontiers{{{"A.EX", 1}, 2}, {{"A.EX", 2}, 1}}));

    (void)t.observe("A.EX", 2, 1);                 // a redelivery
    (void)t.observe("A.EX", 2, 5);                 // held above the frontier: not in the vector
    EXPECT_TRUE(moved_now(t).empty()) << "a redelivery or a held number was listed as a move";
    (void)t.observe("A.EX", 2, 5);                 // the held number again
    EXPECT_TRUE(moved_now(t).empty());

    t.seed("A.EX", 2, 2);                          // fills part of the hole
    EXPECT_EQ(moved_now(t), (Frontiers{{{"A.EX", 2}, 2}}));
    t.seed("A.EX", 2, 2);
    t.raise_local("A.EX", 100);                    // the local counter is not in the vector
    t.declare_frontier("A.EX", 1, 1);              // below it: nothing to declare
    EXPECT_TRUE(moved_now(t).empty());

    t.seed("A.EX", 2, 4);                          // held, then the hole closes over it
    t.seed("A.EX", 2, 3);
    EXPECT_EQ(moved_now(t), (Frontiers{{{"A.EX", 2}, 5}}))
        << "the frontier drained the held numbers, so it is listed where the drain left it";

    t.declare_frontier("B.EX", 1, 10);
    t.import_held({ob::SequenceTracker::HeldRanges{"C.EX", 3, {{1, 2}}}});
    t.import_own_vector({ob::SequenceTracker::VectorEntry{"D.EX", 4, 6}});
    EXPECT_EQ(moved_now(t), (Frontiers{{{"B.EX", 1}, 10}, {{"C.EX", 3}, 2}, {{"D.EX", 4}, 6}}));

    (void)t.observe("A.EX", 1, 0);
    t.reset();
    const auto after_reset = t.take_moved_frontiers();
    EXPECT_TRUE(after_reset.all) << "after a reset a copy holds frontiers the tracker no longer does";
    EXPECT_TRUE(after_reset.moved.empty());
}

TEST(SequenceTracker, TheListingsSayWhetherACopyFromTheLastTakeIsExact) {
    // What the mesh manager asks before it tells a peer it lacks nothing, from under a lock that
    // keeps it out of the tracker (#180 part D): has any frontier moved since the copy was brought
    // up to date? A pair counts once a take, so the answer costs nothing a record.
    ob::SequenceTracker t;
    const auto first = t.take_moved_frontiers();
    EXPECT_EQ(first.listings, t.listings()) << "a take does not reach the listing it hands out";
    (void)t.observe("A.EX", 1, 0);                 // frontier 1: listed
    (void)t.observe("A.EX", 1, 0);                 // 2: listed already
    EXPECT_EQ(t.listings(), first.listings + 1) << "a pair was counted once a record";
    (void)t.observe("B.EX", 2, 5);                 // held above the frontier: nothing moved
    EXPECT_EQ(t.listings(), first.listings + 1) << "a held number was counted as a move";

    const auto second = t.take_moved_frontiers();
    EXPECT_EQ(second.listings, t.listings());
    (void)t.observe("A.EX", 1, 1);                 // a redelivery
    EXPECT_EQ(t.listings(), second.listings) << "a redelivery was counted as a move";
    (void)t.observe("A.EX", 1, 0);                 // 3: moved again since the take
    EXPECT_EQ(t.listings(), second.listings + 1) << "a move after a take was not counted";

    const uint64_t before_reset = t.listings();
    t.reset();
    EXPECT_GT(t.listings(), before_reset)
        << "a copy from before a reset holds frontiers the tracker no longer does";
}

// A copy kept only from `take_moved_frontiers()` - rebuilt from `export_vector()` when told to -
// is the export, whatever the tracker was asked in between: the property the engine's cache of the
// vector rests on, since it never exports the whole vector again while nothing is reset.
RC_GTEST_PROP(SequenceTrackerProperty, ACopyKeptFromTheMovedFrontiersIsTheExport, ()) {
    ob::SequenceTracker t;
    Frontiers copy;
    const auto sync = [&] {
        auto moved = t.take_moved_frontiers();
        if (moved.all) {
            bool truncated = false;
            copy = as_map(t.export_vector(1u << 20, truncated));
            return;
        }
        for (const auto& e : moved.moved) copy[{e.key, e.origin}] = e.frontier;
    };
    const auto steps = *rc::gen::inRange<int>(1, 200);
    for (int i = 0; i < steps; ++i) {
        const std::string key = std::string(1, static_cast<char>('A' + *rc::gen::inRange(0, 4))) + ".EX";
        const auto origin = static_cast<uint16_t>(*rc::gen::inRange(1, 4));
        const auto seq = static_cast<uint64_t>(*rc::gen::inRange(0, 12));
        switch (*rc::gen::inRange(0, 9)) {
            case 0: case 1: case 2: (void)t.observe(key, origin, seq); break;
            case 3: case 4: t.seed(key, origin, seq); break;
            case 5: t.declare_frontier(key, origin, seq); break;
            case 6: t.import_held({ob::SequenceTracker::HeldRanges{key, origin, {{seq + 1, seq + 3}}}}); break;
            case 7: if (*rc::gen::inRange(0, 10) == 0) t.reset(); break;
            default: sync(); break;
        }
    }
    sync();
    bool truncated = false;
    RC_ASSERT(copy == as_map(t.export_vector(1u << 20, truncated)));
}

// The half of #180 part D the tracker owns: a copy whose last take reached the tracker's listing is
// the export, whatever the tracker was asked in between - so "the copy is exact" is never said of
// one that is not. (The other way round need not hold: a reset of an empty tracker changes nothing
// a copy holds, and still says the copy is behind, which costs a wait and never a record.)
RC_GTEST_PROP(SequenceTrackerProperty, ACopyWhoseListingIsCurrentIsTheExport, ()) {
    ob::SequenceTracker t;
    Frontiers copy;
    uint64_t covers = 0;
    const auto sync = [&] {
        auto moved = t.take_moved_frontiers();
        covers = moved.listings;
        if (moved.all) {
            bool truncated = false;
            copy = as_map(t.export_vector(1u << 20, truncated));
            return;
        }
        for (const auto& e : moved.moved) copy[{e.key, e.origin}] = e.frontier;
    };
    const auto steps = *rc::gen::inRange<int>(1, 200);
    for (int i = 0; i < steps; ++i) {
        const std::string key = std::string(1, static_cast<char>('A' + *rc::gen::inRange(0, 4))) + ".EX";
        const auto origin = static_cast<uint16_t>(*rc::gen::inRange(1, 4));
        const auto seq = static_cast<uint64_t>(*rc::gen::inRange(0, 12));
        switch (*rc::gen::inRange(0, 9)) {
            case 0: case 1: case 2: (void)t.observe(key, origin, seq); break;
            case 3: case 4: t.seed(key, origin, seq); break;
            case 5: t.declare_frontier(key, origin, seq); break;
            case 6: t.import_held({ob::SequenceTracker::HeldRanges{key, origin, {{seq + 1, seq + 3}}}}); break;
            case 7: if (*rc::gen::inRange(0, 10) == 0) t.reset(); break;
            default: sync(); break;
        }
        RC_ASSERT(t.listings() >= covers);
        if (t.listings() == covers) {
            bool truncated = false;
            RC_ASSERT(copy == as_map(t.export_vector(1u << 20, truncated)));
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Whose numbers raise the local counter (#184)
// ═══════════════════════════════════════════════════════════════════════════════

TEST(SequenceTracker, ANumberAnotherOriginMintedDoesNotRaiseTheLocalCounter) {
    // One counter per symbol, raised by every origin's numbers, gave each origin's stream holes
    // when two mesh nodes wrote one symbol - node 0 1-500, node 1 501-1 000, node 0 1 001-1 500 -
    // and a frontier is "everything up to here", so every receiver's stopped at the first hole.
    ob::SequenceTracker t;
    t.set_local_origin(1);
    for (uint64_t seq = 1; seq <= 500; ++seq) (void)t.observe("S.EX", 2, seq);
    t.seed("S.EX", 2, 501);
    EXPECT_EQ(t.peek_next_local("S.EX"), 1u) << "another origin's numbers raised this node's counter";
    EXPECT_EQ(t.observe("S.EX", 1, 0).sequence_number, 1u);
    EXPECT_EQ(t.observe("S.EX", 1, 0).sequence_number, 2u);
    EXPECT_EQ(t.frontier("S.EX", 1), 2u) << "this node's own stream has a hole";

    // Its own numbers, received or replayed, still do: they are its counter's.
    t.seed("S.EX", 1, 10);
    EXPECT_EQ(t.peek_next_local("S.EX"), 11u);
    (void)t.observe("S.EX", 1, 20);
    EXPECT_EQ(t.peek_next_local("S.EX"), 21u);
}

TEST(SequenceTracker, WithoutAMeshEveryNumberIsTheLocalOrigins) {
    // The control: origin 0 is everything a node without a mesh holds, a replica's included, so a
    // promoted replica numbers on from its primary's numbers as it always did.
    ob::SequenceTracker t;
    (void)t.observe("R.EX", 0, 41);
    t.seed("R.EX", 0, 42);
    EXPECT_EQ(t.peek_next_local("R.EX"), 43u);
}

TEST(SequenceTracker, AHeldSetAtItsCapIsSaidOnceUntilTheFrontierMoves) {
    // The diagnostic for a hole nothing fills (#184): a held set at its cap. Once, not per record -
    // every record above the hole reaches it - and again only after the frontier has moved.
    ob::SequenceTracker t;
    t.set_local_origin(1);
    (void)t.observe("H.EX", 2, 1);
    testing::internal::CaptureStderr();
    for (uint64_t seq = 3; seq < 3 + ob::SequenceTracker::kMaxAboveFrontier + 10; ++seq) {
        (void)t.observe("H.EX", 2, seq);
    }
    const std::string said = testing::internal::GetCapturedStderr();
    size_t warnings = 0;
    for (size_t at = said.find("Held set full"); at != std::string::npos;
         at = said.find("Held set full", at + 1)) {
        ++warnings;
    }
    EXPECT_EQ(warnings, 1u) << "a held set at its cap is said once, not per record past it";
    EXPECT_EQ(t.above_frontier_size("H.EX", 2), ob::SequenceTracker::kMaxAboveFrontier);
    EXPECT_EQ(t.frontier("H.EX", 2), 1u);
    (void)t.observe("H.EX", 2, 2);   // the hole filled: the held numbers drain
    EXPECT_GT(t.frontier("H.EX", 2), 1u);
    EXPECT_LT(t.above_frontier_size("H.EX", 2), ob::SequenceTracker::kMaxAboveFrontier);
}

// ── The numbering from before per-origin numbers, closed (#187) ───────────────────────────────

namespace {

/// One symbol as a mesh node numbered it before #184: origins 2 and 3 in turns, one counter.
void old_numbering(ob::SequenceTracker& t, const std::string& key) {
    for (uint64_t s = 1; s <= 5; ++s) t.observe(key, 2, s);
    for (uint64_t s = 6; s <= 10; ++s) t.observe(key, 3, s);
    for (uint64_t s = 11; s <= 15; ++s) t.observe(key, 2, s);
}

}  // namespace

TEST(ClosedNumbering, EveryKnownOriginIsClosedAndTheCounterGoesOnFromTheBase) {
    ob::SequenceTracker t;
    t.set_local_origin(1);
    old_numbering(t, "K.EX");
    ASSERT_EQ(t.frontier("K.EX", 2), 5u);
    ASSERT_EQ(t.frontier("K.EX", 3), 0u);
    ASSERT_EQ(t.above_frontier_size("K.EX", 2), 5u);

    EXPECT_EQ(t.close_numbering("K.EX"), 2u);
    EXPECT_EQ(t.frontier("K.EX", 2), ob::kClosedNumberingBase - 1);
    EXPECT_EQ(t.frontier("K.EX", 3), ob::kClosedNumberingBase - 1);
    EXPECT_EQ(t.above_frontier_size("K.EX", 2), 0u);
    EXPECT_EQ(t.above_frontier_size("K.EX", 3), 0u);
    EXPECT_EQ(t.peek_next_local("K.EX"), ob::kClosedNumberingBase);
    EXPECT_EQ(t.frontier("K.EX", 1), 0u) << "an origin the symbol never had was closed";
    EXPECT_EQ(t.close_numbering("K.EX"), 0u) << "a second close moved something";
}

TEST(ClosedNumbering, ARecordFromTheBaseClosesItsOwnOriginAndIsNoGap) {
    ob::SequenceTracker t;
    t.set_local_origin(1);
    old_numbering(t, "K.EX");
    t.observe("K.EX", 4, 1);
    const auto d = t.observe("K.EX", 3, ob::kClosedNumberingBase);
    EXPECT_FALSE(d.gap) << "the first number of a closed origin was judged a gap";
    EXPECT_EQ(t.frontier("K.EX", 3), ob::kClosedNumberingBase);
    EXPECT_EQ(t.frontier("K.EX", 2), 5u) << "another origin was closed by it";
    EXPECT_EQ(t.frontier("K.EX", 4), 1u);
}

TEST(ClosedNumbering, ARecordBelowTheBaseClosesNothing) {
    ob::SequenceTracker t;
    old_numbering(t, "K.EX");
    const auto d = t.observe("K.EX", 3, ob::kClosedNumberingBase - 1);
    EXPECT_TRUE(d.gap);
    EXPECT_EQ(t.frontier("K.EX", 3), 0u);
}

TEST(ClosedNumbering, AReplayedOrOwnRecordFromTheBaseClosesItsOriginToo) {
    ob::SequenceTracker t;
    t.set_local_origin(1);
    old_numbering(t, "K.EX");
    t.seed("K.EX", 2, ob::kClosedNumberingBase);
    EXPECT_EQ(t.frontier("K.EX", 2), ob::kClosedNumberingBase) << "a replayed record did not close";

    // This node never wrote K; its first own number after the close is the base, and in order.
    t.close_numbering("K.EX");
    const auto own = t.observe("K.EX", 1, 0);
    EXPECT_EQ(own.sequence_number, ob::kClosedNumberingBase);
    EXPECT_EQ(t.frontier("K.EX", 1), ob::kClosedNumberingBase);
}

TEST(ClosedNumbering, ASymbolFirstSeenAfterTheCloseNumbersFromOne) {
    ob::SequenceTracker t;
    t.set_local_origin(1);
    old_numbering(t, "K.EX");
    for (const auto& key : t.keys()) t.close_numbering(key);
    EXPECT_EQ(t.observe("NEW.EX", 1, 0).sequence_number, 1u);
    EXPECT_EQ(t.observe("NEW.EX", 2, 1).gap, false);
    EXPECT_EQ(t.frontier("NEW.EX", 2), 1u);
}

// ═══════════════════════════════════════════════════════════════════════════════
// What a writer asks before writing the held set down (#189)
// ═══════════════════════════════════════════════════════════════════════════════

TEST(HeldVersion, CountsChangesToTheHeldNumbersAndNothingElse) {
    ob::SequenceTracker t;
    const uint16_t peer = 2;
    const uint64_t before = t.held_version();
    (void)t.observe("A.EX", peer, 1);
    (void)t.observe("A.EX", peer, 2);
    EXPECT_EQ(t.held_version(), before) << "records in order hold nothing, and changed nothing held";
    EXPECT_EQ(t.holding_count(), 0u);

    (void)t.observe("A.EX", peer, 5);   // held above the hole at 3
    EXPECT_EQ(t.held_version(), before + 1);
    EXPECT_EQ(t.holding_count(), 1u);
    (void)t.observe("A.EX", peer, 5);   // a redelivery of what is held
    EXPECT_EQ(t.held_version(), before + 1) << "a redelivery changed the held set";

    (void)t.observe("A.EX", peer, 3);
    (void)t.observe("A.EX", peer, 4);   // fills the hole: 5 drains into the frontier
    EXPECT_EQ(t.frontier("A.EX", peer), 5u);
    EXPECT_EQ(t.held_version(), before + 2);
    EXPECT_EQ(t.holding_count(), 0u) << "a state that holds nothing is still in the index";

    t.declare_frontier("B.EX", peer, 10);
    (void)t.observe("B.EX", peer, 13);
    t.declare_frontier("B.EX", peer, 20);   // covers what B held
    EXPECT_EQ(t.holding_count(), 0u);
    EXPECT_EQ(t.held_version(), before + 4);

    (void)t.observe("C.EX", peer, 9);
    ASSERT_EQ(t.holding_count(), 1u);
    t.reset();
    EXPECT_EQ(t.holding_count(), 0u);
    EXPECT_EQ(t.held_version(), before + 6) << "a reset that dropped held numbers changed nothing";
}

RC_GTEST_PROP(HeldVersionProperty, TheHeldExportIsWhatWasHeldInKeyAndOriginOrder, ()) {
    // Any records from a few origins in any order, and the numbers each holds above its frontier -
    // computed here from what arrived - are what export_held() hands out, sorted, from its index.
    ob::SequenceTracker t;
    std::map<std::pair<std::string, uint16_t>, std::set<uint64_t>> seen;
    const auto n = *rc::gen::inRange<size_t>(0, 400);
    for (size_t i = 0; i < n; ++i) {
        const auto key = "K" + std::to_string(*rc::gen::inRange(0, 6)) + ".EX";
        const auto origin = static_cast<uint16_t>(*rc::gen::inRange(1, 4));
        const auto seq = static_cast<uint64_t>(*rc::gen::inRange(1, 40));
        (void)t.observe(key, origin, seq);
        seen[{key, origin}].insert(seq);
    }
    std::vector<ob::SequenceTracker::HeldRanges> expected;
    for (const auto& [k, seqs] : seen) {
        uint64_t frontier = 0;
        while (seqs.count(frontier + 1) != 0) ++frontier;
        ob::SequenceTracker::HeldRanges h;
        h.key = k.first;
        h.origin = k.second;
        for (uint64_t s : seqs) {
            if (s <= frontier) continue;
            if (!h.ranges.empty() && h.ranges.back().second + 1 == s) {
                h.ranges.back().second = s;
            } else {
                h.ranges.emplace_back(s, s);
            }
        }
        if (!h.ranges.empty()) expected.push_back(std::move(h));
    }
    bool truncated = false;
    const auto held = t.export_held(1u << 20, truncated);
    RC_ASSERT(!truncated);
    RC_ASSERT(held.size() == expected.size());
    RC_ASSERT(t.holding_count() == expected.size());
    for (size_t i = 0; i < held.size(); ++i) {
        RC_ASSERT(held[i].key == expected[i].key);
        RC_ASSERT(held[i].origin == expected[i].origin);
        RC_ASSERT(held[i].ranges == expected[i].ranges);
    }
}
