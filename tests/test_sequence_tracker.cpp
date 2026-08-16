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

TEST(SequenceTracker, ALateOutOfOrderRecordDoesNotDragTheHighWaterBack) {
    ob::SequenceTracker t;

    t.observe("OOO.EX", 1, 1);
    t.observe("OOO.EX", 1, 2);
    t.observe("OOO.EX", 1, 3);

    EXPECT_TRUE(t.observe("OOO.EX", 1, 2).gap) << "a repeat of 2 is not what was expected";
    EXPECT_EQ(t.high_water("OOO.EX", 1), 3u)
        << "the high-water mark went backwards, so every following record would be reported "
           "as a gap for as long as the stream continues";
    EXPECT_FALSE(t.observe("OOO.EX", 1, 4).gap);
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

TEST(SequenceTracker, SeedRestoresStateWithoutReportingGaps) {
    ob::SequenceTracker t;

    // Replay of a WAL tail whose records already have numbers, including a hole that was
    // recorded as a GAP when it first happened.
    t.seed("S.EX", 1, 1);
    t.seed("S.EX", 1, 4);

    EXPECT_EQ(t.high_water("S.EX", 1), 4u);
    EXPECT_EQ(t.peek_next_local("S.EX"), 5u);
    EXPECT_FALSE(t.observe("S.EX", 1, 5).gap)
        << "the record following a replayed tail was reported as a gap, so every restart "
           "would invent one";
}

TEST(SequenceTracker, SeedIgnoresRecordsFromBeforeNumbersExisted) {
    ob::SequenceTracker t;

    t.seed("OLD.EX", 0, 0);
    EXPECT_EQ(t.peek_next_local("OLD.EX"), 1u);
    EXPECT_EQ(t.high_water("OLD.EX", 0), 0u)
        << "a zero is the absence of a number, not the number zero, and must not become an "
           "origin's high-water mark — the next real record would look like a gap";
}
