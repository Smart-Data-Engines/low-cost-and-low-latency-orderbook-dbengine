// Serialisation of the version vector, and what a sender concludes from it.
//
// The asymmetry that matters: a missing entry, a peer that sent nothing, and a peer that said
// "I cannot state what I have" must all mean the same thing to a sender — send everything.
// Any of them meaning "the peer has it" would be roadmap #61 again.

#include "orderbook/version_vector.hpp"

#include <gtest/gtest.h>

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
