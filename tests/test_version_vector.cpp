// Serialisation of the version vector, and what a sender concludes from it.
//
// The asymmetry that matters: a missing entry, a peer that sent nothing, and a peer that said
// "I cannot state what I have" must all mean the same thing to a sender — send everything.
// Any of them meaning "the peer has it" would be roadmap #61 again.

#include "orderbook/version_vector.hpp"

#include <gtest/gtest.h>

#include <cstring>

TEST(VersionVector, RoundTripsEntries) {
    std::vector<ob::SequenceTracker::VectorEntry> entries{
        {"BTC-USD.BINANCE", 1, 42},
        {"ETH-USD.BINANCE", 1, 7},
        {"BTC-USD.BINANCE", 2, 5},
    };

    const auto payload = ob::serialize_version_vector(entries, /*truncated=*/false);
    ASSERT_EQ(payload.size(), ob::VV_HEADER_SIZE + 3 * ob::VV_ENTRY_SIZE);

    ob::PeerVector pv;
    ASSERT_TRUE(pv.deserialize(payload.data(), payload.size()));
    EXPECT_EQ(pv.entry_count(), 3u);
    EXPECT_FALSE(pv.truncated());
    EXPECT_FALSE(pv.wants_everything());

    EXPECT_EQ(pv.frontier_for("BTC-USD.BINANCE", 1), 42u);
    EXPECT_EQ(pv.frontier_for("ETH-USD.BINANCE", 1), 7u);
    EXPECT_EQ(pv.frontier_for("BTC-USD.BINANCE", 2), 5u)
        << "the same symbol from a different origin is a different entry";
}

TEST(VersionVector, AMissingEntryMeansThePeerHasNothingThere) {
    const auto payload = ob::serialize_version_vector({{"A.EX", 1, 9}}, false);
    ob::PeerVector pv;
    ASSERT_TRUE(pv.deserialize(payload.data(), payload.size()));

    EXPECT_EQ(pv.frontier_for("B.EX", 1), 0u) << "an unknown symbol must read as 'send it all'";
    EXPECT_EQ(pv.frontier_for("A.EX", 4), 0u) << "an unknown origin must read the same way";
}

TEST(VersionVector, ATruncatedVectorAsksForEverything) {
    const auto payload = ob::serialize_version_vector({{"A.EX", 1, 9}}, /*truncated=*/true);
    ASSERT_EQ(payload.size(), ob::VV_HEADER_SIZE) << "nothing but the marker is sent";

    ob::PeerVector pv;
    ASSERT_TRUE(pv.deserialize(payload.data(), payload.size()));
    EXPECT_TRUE(pv.truncated());
    EXPECT_TRUE(pv.wants_everything());
    EXPECT_EQ(pv.frontier_for("A.EX", 1), 0u)
        << "a peer that could not state what it has must not appear to hold anything";
}

TEST(VersionVector, ASilentPeerAlsoWantsEverything) {
    ob::PeerVector pv;
    EXPECT_FALSE(pv.received());
    EXPECT_TRUE(pv.wants_everything())
        << "a peer on the older protocol sends no vector at all, and the safe reading of "
           "silence is that it holds nothing";
}

TEST(VersionVector, AShortPayloadIsRefusedRatherThanPartlyBelieved) {
    auto payload = ob::serialize_version_vector({{"A.EX", 1, 9}, {"B.EX", 1, 3}}, false);
    payload.resize(payload.size() - 10);          // a frame cut short

    ob::PeerVector pv;
    EXPECT_FALSE(pv.deserialize(payload.data(), payload.size()));
    EXPECT_TRUE(pv.wants_everything())
        << "half a vector read as a whole one would leave the dropped entries looking "
           "delivered";
}

TEST(VersionVector, AnEmptyVectorIsUsableAndSaysThePeerHoldsNothing) {
    // Two different things that lead to the same traffic and must not be conflated:
    // "no usable vector" (silence, or a peer that could not state its position) versus a
    // perfectly good vector that happens to be empty. The first is wants_everything(); the
    // second is handled by every frontier being 0, which the per-record filter reads as
    // "send it".
    const auto payload = ob::serialize_version_vector({}, false);
    ob::PeerVector pv;
    ASSERT_TRUE(pv.deserialize(payload.data(), payload.size()));

    EXPECT_TRUE(pv.received()) << "the peer did answer; it simply holds nothing yet";
    EXPECT_FALSE(pv.truncated());
    EXPECT_FALSE(pv.wants_everything()) << "the vector is usable, so no blanket send is needed";
    EXPECT_EQ(pv.frontier_for("ANY.EX", 1), 0u)
        << "and every lookup in it still says 'send that record'";
}

TEST(VersionVector, AKeyAtTheWireLimitSurvivesTheRoundTrip) {
    // char[16] symbol + '.' + char[16] exchange, both filled to 15 characters.
    const std::string key = "123456789012345.123456789012345";
    ASSERT_EQ(key.size(), 31u);

    const auto payload = ob::serialize_version_vector({{key, 3, 11}}, false);
    ob::PeerVector pv;
    ASSERT_TRUE(pv.deserialize(payload.data(), payload.size()));
    EXPECT_EQ(pv.frontier_for(key, 3), 11u) << "the longest possible key was clipped";
}

// ═══════════════════════════════════════════════════════════════════════════════
// compare_vectors: the arithmetic a reconciliation pass runs on
// ═══════════════════════════════════════════════════════════════════════════════

namespace {

ob::PeerVector make_peer_vector(const std::vector<ob::SequenceTracker::VectorEntry>& entries,
                                bool truncated = false) {
    const auto payload = ob::serialize_version_vector(entries, truncated);
    ob::PeerVector pv;
    pv.deserialize(payload.data(), payload.size());
    return pv;
}

}  // namespace

TEST(CompareVectors, FindsWhatEachSideIsMissing) {
    const std::vector<ob::SequenceTracker::VectorEntry> ours{
        {"A.EX", 1, 10},   // we are ahead
        {"B.EX", 1, 5},    // we are behind
    };
    const auto theirs = make_peer_vector({{"A.EX", 1, 4}, {"B.EX", 1, 9}});

    const auto diff = ob::compare_vectors(ours, theirs, /*peer=*/7);

    ASSERT_EQ(diff.peer_lacks.size(), 1u);
    EXPECT_EQ(diff.peer_lacks[0].key, "A.EX");
    EXPECT_EQ(diff.peer_lacks[0].from_seq, 5u) << "the first record the peer is missing";
    EXPECT_EQ(diff.peer_lacks[0].to_seq, 10u);
    EXPECT_EQ(diff.peer_lacks[0].peer_node_id, 7u);

    ASSERT_EQ(diff.we_lack.size(), 1u);
    EXPECT_EQ(diff.we_lack[0].key, "B.EX");
    EXPECT_EQ(diff.we_lack[0].from_seq, 6u);
    EXPECT_EQ(diff.we_lack[0].to_seq, 9u);
}

TEST(CompareVectors, EqualFrontiersAreNotAGap) {
    const std::vector<ob::SequenceTracker::VectorEntry> ours{{"A.EX", 1, 10}};
    const auto theirs = make_peer_vector({{"A.EX", 1, 10}});

    const auto diff = ob::compare_vectors(ours, theirs, 7);
    EXPECT_TRUE(diff.we_lack.empty());
    EXPECT_TRUE(diff.peer_lacks.empty());
}

TEST(CompareVectors, AKeyOnlyTheyHoldIsSomethingWeLack) {
    // The case that matters most for anti-entropy: a symbol whose records only reached the peer.
    // Iterating our own entries alone would never see it, and the difference would be invisible
    // for as long as nobody wrote to that symbol here.
    const std::vector<ob::SequenceTracker::VectorEntry> ours{{"A.EX", 1, 3}};
    const auto theirs = make_peer_vector({{"A.EX", 1, 3}, {"ONLY-THEIRS.EX", 2, 8}});

    const auto diff = ob::compare_vectors(ours, theirs, 7);

    ASSERT_EQ(diff.we_lack.size(), 1u);
    EXPECT_EQ(diff.we_lack[0].key, "ONLY-THEIRS.EX");
    EXPECT_EQ(diff.we_lack[0].origin, 2u);
    EXPECT_EQ(diff.we_lack[0].from_seq, 1u) << "we hold nothing there, so we lack from 1";
    EXPECT_EQ(diff.we_lack[0].to_seq, 8u);
}

TEST(CompareVectors, AKeyOnlyWeHoldIsSomethingTheyLack) {
    const std::vector<ob::SequenceTracker::VectorEntry> ours{{"ONLY-OURS.EX", 1, 4}};
    const auto theirs = make_peer_vector({});

    const auto diff = ob::compare_vectors(ours, theirs, 7);
    ASSERT_EQ(diff.peer_lacks.size(), 1u);
    EXPECT_EQ(diff.peer_lacks[0].from_seq, 1u);
    EXPECT_EQ(diff.peer_lacks[0].to_seq, 4u);
    EXPECT_TRUE(diff.we_lack.empty());
}

TEST(CompareVectors, TheSameSymbolFromDifferentOriginsIsComparedSeparately) {
    const std::vector<ob::SequenceTracker::VectorEntry> ours{{"A.EX", 1, 9}, {"A.EX", 2, 1}};
    const auto theirs = make_peer_vector({{"A.EX", 1, 2}, {"A.EX", 2, 6}});

    const auto diff = ob::compare_vectors(ours, theirs, 7);

    ASSERT_EQ(diff.peer_lacks.size(), 1u);
    EXPECT_EQ(diff.peer_lacks[0].origin, 1u) << "we are ahead on origin 1";
    ASSERT_EQ(diff.we_lack.size(), 1u);
    EXPECT_EQ(diff.we_lack[0].origin, 2u) << "and behind on origin 2, in the same symbol";
}

TEST(CompareVectors, APeerThatSaidNothingIsTreatedAsHoldingNothing) {
    const std::vector<ob::SequenceTracker::VectorEntry> ours{{"A.EX", 1, 10}};
    ob::PeerVector silent;   // never received

    const auto diff = ob::compare_vectors(ours, silent, 7);

    ASSERT_EQ(diff.peer_lacks.size(), 1u);
    EXPECT_EQ(diff.peer_lacks[0].from_seq, 1u) << "send it everything";
    EXPECT_TRUE(diff.we_lack.empty())
        << "a silent peer says nothing about our own gaps, and inventing zeros for them would be "
           "a claim rather than an observation";
}

TEST(CompareVectors, ATruncatedVectorIsTreatedTheSameAsSilence) {
    const std::vector<ob::SequenceTracker::VectorEntry> ours{{"A.EX", 1, 10}};
    const auto theirs = make_peer_vector({{"A.EX", 1, 99}}, /*truncated=*/true);

    const auto diff = ob::compare_vectors(ours, theirs, 7);
    ASSERT_EQ(diff.peer_lacks.size(), 1u);
    EXPECT_EQ(diff.peer_lacks[0].from_seq, 1u);
    EXPECT_TRUE(diff.we_lack.empty())
        << "a vector the peer could not state must not be read as data about what it holds";
}

// ── Held sequence ranges (#75) ────────────────────────────────────────────────
//
// The frontier describes a node that followed every origin's stream from its first record.
// Everything that arrived out of order sits above it, and that set is what a restart used to
// forget — turning catch-up's deliberate over-delivery back into duplicate rows.

TEST(HeldRanges, RoundTripsThroughTheWireFormat) {
    std::vector<ob::SequenceTracker::HeldRanges> in;
    in.push_back({"BTC-USD.BINANCE", 2, {{5, 9}, {20, 20}, {100, 4099}}});
    in.push_back({"ETH-USD.KRAKEN", 7, {{1, 1}}});

    const auto payload = ob::serialize_held_ranges(in);
    ASSERT_FALSE(payload.empty());

    std::vector<ob::SequenceTracker::HeldRanges> out;
    ASSERT_TRUE(ob::deserialize_held_ranges(payload.data(), payload.size(), out));
    ASSERT_EQ(out.size(), in.size());
    for (size_t i = 0; i < in.size(); ++i) {
        EXPECT_EQ(out[i].key, in[i].key);
        EXPECT_EQ(out[i].origin, in[i].origin);
        EXPECT_EQ(out[i].ranges, in[i].ranges);
    }
}

TEST(HeldRanges, AnEmptySetSerialisesToNothingAtAll) {
    // Not an empty record: nothing held means nothing to write, and a zero-entry payload would
    // cost a WAL record per flush for no information.
    EXPECT_TRUE(ob::serialize_held_ranges({}).empty());
}

TEST(HeldRanges, ATruncatedPayloadIsRefusedRatherThanPartlyBelieved) {
    std::vector<ob::SequenceTracker::HeldRanges> in;
    in.push_back({"BTC-USD.BINANCE", 2, {{5, 9}, {20, 30}}});
    const auto payload = ob::serialize_held_ranges(in);

    std::vector<ob::SequenceTracker::HeldRanges> out;
    // Every prefix short of the whole thing must be refused. Believing half of it would mean
    // claiming to have seen numbers this node never did, which loses records instead of
    // duplicating them — the expensive direction.
    for (size_t len = 0; len < payload.size(); ++len) {
        out.assign(1, {});   // make sure the function clears it
        EXPECT_FALSE(ob::deserialize_held_ranges(payload.data(), len, out))
            << "accepted a payload truncated to " << len << " of " << payload.size() << " bytes";
        EXPECT_TRUE(out.empty()) << "left entries behind after refusing a payload";
    }
}

TEST(HeldRanges, ABackwardsRangeIsRefused) {
    // 36-byte entry header with one range whose end precedes its start.
    std::vector<uint8_t> payload(ob::HS_HEADER_SIZE + ob::HS_ENTRY_HEADER_SIZE + ob::HS_RANGE_SIZE, 0);
    const uint16_t one = 1;
    std::memcpy(payload.data(), &one, sizeof(one));
    std::memcpy(payload.data() + ob::HS_HEADER_SIZE, "SYM.EX", 6);
    std::memcpy(payload.data() + ob::HS_HEADER_SIZE + 34, &one, sizeof(one));  // range_count
    const uint64_t first = 90, last = 10;
    std::memcpy(payload.data() + ob::HS_HEADER_SIZE + ob::HS_ENTRY_HEADER_SIZE, &first, sizeof(first));
    std::memcpy(payload.data() + ob::HS_HEADER_SIZE + ob::HS_ENTRY_HEADER_SIZE + 8, &last, sizeof(last));

    std::vector<ob::SequenceTracker::HeldRanges> out;
    EXPECT_FALSE(ob::deserialize_held_ranges(payload.data(), payload.size(), out));
}

TEST(HeldRanges, ExportCollapsesConsecutiveNumbersIntoOneRange) {
    ob::SequenceTracker tracker;
    // A run above a gap: 5..8 and 20, with 1-4 never seen, so the frontier stays at 0.
    for (uint64_t seq : {5ULL, 6ULL, 7ULL, 8ULL, 20ULL}) {
        tracker.observe("SYM.EX", 3, seq);
    }
    ASSERT_EQ(tracker.frontier("SYM.EX", 3), 0u);

    bool truncated = false;
    const auto held = tracker.export_held(100, truncated);
    ASSERT_FALSE(truncated);
    ASSERT_EQ(held.size(), 1u);
    EXPECT_EQ(held[0].origin, 3);
    ASSERT_EQ(held[0].ranges.size(), 2u) << "a run of four should be one range, not four";
    const std::pair<uint64_t, uint64_t> run{5, 8};
    const std::pair<uint64_t, uint64_t> single{20, 20};
    EXPECT_EQ(held[0].ranges[0], run);
    EXPECT_EQ(held[0].ranges[1], single);
}

TEST(HeldRanges, ImportRestoresWhatHasBeenSeenWithoutMovingTheFrontier) {
    ob::SequenceTracker tracker;
    tracker.import_held({{"SYM.EX", 3, {{5, 8}}}});

    EXPECT_TRUE(tracker.has_seen("SYM.EX", 3, 5));
    EXPECT_TRUE(tracker.has_seen("SYM.EX", 3, 8));
    EXPECT_FALSE(tracker.has_seen("SYM.EX", 3, 4)) << "importing held numbers must not claim the gap";
    EXPECT_EQ(tracker.frontier("SYM.EX", 3), 0u);
}

TEST(HeldRanges, ExportTruncatesAndSaysSoRatherThanDroppingSilently) {
    ob::SequenceTracker tracker;
    // Every other number, so each one is its own range and the budget is reached quickly.
    for (uint64_t seq = 10; seq < 10 + 20 * 2; seq += 2) {
        tracker.observe("SYM.EX", 4, seq);
    }

    bool truncated = false;
    const auto held = tracker.export_held(5, truncated);
    EXPECT_TRUE(truncated);
    ASSERT_EQ(held.size(), 1u);
    EXPECT_EQ(held[0].ranges.size(), 5u);
}

// ── #78: a payload larger than the header can describe ────────────────────────
//
// `payload_len` is a uint16_t in both the WAL record header and the multi-master frame header,
// and the writers cast a size_t into it. A version vector of 4096 entries — the documented
// maximum — is 172 kB, so the cast wrapped and produced a header claiming 40962 bytes for a
// 172034-byte payload. In the WAL that makes every later record unreadable, because replay takes
// the header at its word and lands in the middle of this payload. On the wire the peer compares
// `payload_len` against the frame it received, disagrees, and disconnects — again on every
// reconnect. Both were reachable with roughly 400 symbols across 4 origins.

TEST(VersionVectorLimits, AVectorTooLargeForAHeaderBecomesTheSendEverythingMarker) {
    std::vector<ob::SequenceTracker::VectorEntry> entries;
    for (int i = 0; i < 1600; ++i) {       // 1600 * 42 + 2 = 67202 bytes
        entries.push_back({"SYM" + std::to_string(i) + ".EX", 1, 100});
    }
    ASSERT_GT(ob::VV_HEADER_SIZE + entries.size() * ob::VV_ENTRY_SIZE, ob::WAL_MAX_PAYLOAD_LEN);

    const auto payload = ob::serialize_version_vector(entries, /*truncated=*/false);

    ASSERT_EQ(payload.size(), ob::VV_HEADER_SIZE)
        << "a payload a header cannot describe must not be produced at all";
    uint16_t marker = 0;
    std::memcpy(&marker, payload.data(), sizeof(marker));
    EXPECT_EQ(marker, ob::VV_TRUNCATED)
        << "the receiver has to be told to send everything, not handed a short read";
}

TEST(VersionVectorLimits, AVectorThatStillFitsIsUnaffected) {
    std::vector<ob::SequenceTracker::VectorEntry> entries;
    for (int i = 0; i < 1500; ++i) {       // 1500 * 42 + 2 = 63002 bytes
        entries.push_back({"SYM" + std::to_string(i) + ".EX", 1, 100});
    }
    const auto payload = ob::serialize_version_vector(entries, /*truncated=*/false);
    ASSERT_LE(payload.size(), ob::WAL_MAX_PAYLOAD_LEN);
    EXPECT_EQ(payload.size(), ob::VV_HEADER_SIZE + entries.size() * ob::VV_ENTRY_SIZE);

    ob::PeerVector pv;
    ASSERT_TRUE(pv.deserialize(payload.data(), payload.size()));
    EXPECT_EQ(pv.entry_count(), 1500u);
    EXPECT_FALSE(pv.truncated());
}

TEST(VersionVectorLimits, HeldRangesAreTrimmedToFitRatherThanRefused) {
    // Unlike the vector, a partial held set is sound: every range that survives prevents a
    // duplicate row, and the ones dropped cost only the duplicates they would have prevented.
    std::vector<ob::SequenceTracker::HeldRanges> entries;
    for (int i = 0; i < 2000; ++i) {
        entries.push_back({"SYM" + std::to_string(i) + ".EX", 1, {{10, 12}, {20, 22}}});
    }
    const auto payload = ob::serialize_held_ranges(entries);

    ASSERT_FALSE(payload.empty()) << "trimming, not refusing";
    ASSERT_LE(payload.size(), ob::WAL_MAX_PAYLOAD_LEN);

    std::vector<ob::SequenceTracker::HeldRanges> parsed;
    ASSERT_TRUE(ob::deserialize_held_ranges(payload.data(), payload.size(), parsed));
    EXPECT_GT(parsed.size(), 0u);
    EXPECT_LT(parsed.size(), entries.size()) << "some entries must have been dropped";
    // And what survived is intact — a trimmed payload that parses into damaged entries would be
    // worse than none.
    for (const auto& e : parsed) {
        ASSERT_EQ(e.ranges.size(), 2u);
        EXPECT_EQ(e.ranges[0].first, 10u);
        EXPECT_EQ(e.ranges[1].second, 22u);
    }
}
