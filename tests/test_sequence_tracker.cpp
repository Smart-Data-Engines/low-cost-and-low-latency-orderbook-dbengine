// Unit tests for SequenceTracker: assignment, per-origin gap detection, counter recovery.
//
// These deliberately need no Engine, no etcd and no ports. Gap detection only means
// anything with several origins, and the only Engine entry point for a foreign origin is
// apply_remote_delta(), which requires multi-master to be running — so keeping this state
// in Engine would have made the interesting cases cost half a cluster to test.

#include "orderbook/sequence_tracker.hpp"

#include <gtest/gtest.h>

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
