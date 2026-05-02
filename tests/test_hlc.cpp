// Tests for HybridLogicalClock: property-based tests (Properties 1–6)
// and unit tests (pretty-print, deserialization errors, is_zero).
// Feature: multi-master-replication

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "orderbook/hlc.hpp"

// ── RapidCheck Arbitrary for HLCTimestamp ──────────────────────────────────────

namespace rc {
template <>
struct Arbitrary<ob::HLCTimestamp> {
    static Gen<ob::HLCTimestamp> arbitrary() {
        return gen::build<ob::HLCTimestamp>(
            gen::set(&ob::HLCTimestamp::physical_ns, gen::arbitrary<uint64_t>()),
            gen::set(&ob::HLCTimestamp::logical, gen::arbitrary<uint16_t>()),
            gen::set(&ob::HLCTimestamp::node_id, gen::arbitrary<uint16_t>()));
    }
};
} // namespace rc

// ── Property 1: HLC binary serialization round-trip ───────────────────────────
// Feature: multi-master-replication, Property 1: HLC binary serialization round-trip
// For any valid HLCTimestamp, serialize to 12 bytes then deserialize SHALL
// return an identical value.
// **Validates: Requirements 1.2, 1.6, 12.1, 12.4**
RC_GTEST_PROP(HLCProperty, prop_binary_serialization_roundtrip, ()) {
    const auto ts = *rc::gen::arbitrary<ob::HLCTimestamp>();

    uint8_t buf[12]{};
    ts.serialize(buf);
    const auto recovered = ob::HLCTimestamp::deserialize(buf);

    RC_ASSERT(recovered == ts);
}

// ── Property 2: HLC text serialization round-trip ─────────────────────────────
// Feature: multi-master-replication, Property 2: HLC text serialization round-trip
// For any valid HLCTimestamp, to_string() then from_string() SHALL return an
// identical value.
// **Validates: Requirements 12.2, 12.5**
RC_GTEST_PROP(HLCProperty, prop_text_serialization_roundtrip, ()) {
    const auto ts = *rc::gen::arbitrary<ob::HLCTimestamp>();

    const std::string text = ts.to_string();
    const auto recovered = ob::HLCTimestamp::from_string(text);

    RC_ASSERT(recovered.has_value());
    RC_ASSERT(*recovered == ts);
}

// ── Property 3: HLC comparison order preserved through serialization ──────────
// Feature: multi-master-replication, Property 3: HLC comparison order preserved
// through serialization
// For any pair of HLCTimestamp (a, b), the comparison result SHALL be identical
// before and after binary serialization + deserialization.
// **Validates: Requirements 1.7**
RC_GTEST_PROP(HLCProperty, prop_comparison_order_preserved, ()) {
    const auto a = *rc::gen::arbitrary<ob::HLCTimestamp>();
    const auto b = *rc::gen::arbitrary<ob::HLCTimestamp>();

    uint8_t buf_a[12]{};
    uint8_t buf_b[12]{};
    a.serialize(buf_a);
    b.serialize(buf_b);

    const auto a2 = ob::HLCTimestamp::deserialize(buf_a);
    const auto b2 = ob::HLCTimestamp::deserialize(buf_b);

    RC_ASSERT((a < b) == (a2 < b2));
    RC_ASSERT((a > b) == (a2 > b2));
    RC_ASSERT((a == b) == (a2 == b2));
}

// ── Property 4: HLC tick_local monotonicity ───────────────────────────────────
// Feature: multi-master-replication, Property 4: HLC tick_local monotonicity
// For any sequence of tick_local() calls (including with wall clock going
// backwards), each successive result SHALL be strictly greater than the
// previous one.
// **Validates: Requirements 1.3**
RC_GTEST_PROP(HLCProperty, prop_tick_local_monotonicity, ()) {
    // Generate a count of tick_local calls to perform.
    const auto count = *rc::gen::inRange(2, 20);

    ob::HybridLogicalClock hlc(1);

    ob::HLCTimestamp prev{};
    bool first = true;
    for (int i = 0; i < count; ++i) {
        // tick_local() guarantees monotonicity regardless of wall clock
        // behavior. The HLC algorithm ensures each successive call produces
        // a strictly increasing result even if the wall clock stands still
        // or goes backwards.
        auto ts = hlc.tick_local();
        if (!first) {
            RC_ASSERT(ts > prev);
        }
        prev = ts;
        first = false;
    }
}

// ── Property 5: HLC tick_receive merge correctness ────────────────────────────
// Feature: multi-master-replication, Property 5: HLC tick_receive merge
// correctness
// For any local HLC state and any remote HLCTimestamp, the result of
// tick_receive(remote) SHALL be strictly greater than both the old local
// state and the remote timestamp.
// **Validates: Requirements 1.4, 1.5**
RC_GTEST_PROP(HLCProperty, prop_tick_receive_merge_correctness, ()) {
    const auto remote = *rc::gen::arbitrary<ob::HLCTimestamp>();

    // Use a fresh HLC clock for each test iteration.
    const auto node_id = *rc::gen::inRange<uint16_t>(1, 1000);
    ob::HybridLogicalClock hlc(node_id);

    // Establish a local state by calling tick_local first.
    hlc.tick_local();
    const auto old_local = hlc.current();

    // Now merge with the remote timestamp.
    const auto result = hlc.tick_receive(remote);

    RC_ASSERT(result > old_local);
    RC_ASSERT(result > remote);
}

// ── Property 6: HLC causal ordering ──────────────────────────────────────────
// Feature: multi-master-replication, Property 6: HLC causal ordering
// For any chain of causal events (tick_local / tick_receive), if event A
// happened-before event B, then HLC(A) < HLC(B).
// **Validates: Requirements 1.5**

RC_GTEST_PROP(HLCProperty, prop_causal_ordering, ()) {
    const auto count = *rc::gen::inRange(2, 15);

    ob::HybridLogicalClock hlc(42);

    // Execute events and collect timestamps in causal order.
    std::vector<ob::HLCTimestamp> timestamps;
    timestamps.reserve(static_cast<size_t>(count));

    for (int i = 0; i < count; ++i) {
        // Randomly choose between local tick and receive.
        const auto is_receive = *rc::gen::arbitrary<bool>();
        ob::HLCTimestamp ts{};
        if (is_receive) {
            const auto remote = *rc::gen::arbitrary<ob::HLCTimestamp>();
            ts = hlc.tick_receive(remote);
        } else {
            ts = hlc.tick_local();
        }
        timestamps.push_back(ts);
    }

    // All events are in a single causal chain (sequential on one clock),
    // so each successive timestamp must be strictly greater.
    for (size_t i = 1; i < timestamps.size(); ++i) {
        RC_ASSERT(timestamps[i] > timestamps[i - 1]);
    }
}

// ── Unit tests: HLC pretty-print ─────────────────────────────────────────────

TEST(HLCUnit, PrettyPrintFormat) {
    // Use a known timestamp: 1705312200 seconds = 2024-01-15T09:50:00Z
    const uint64_t secs = 1705312200ULL;
    const uint64_t nanos = 123456789ULL;
    const uint64_t physical_ns = secs * 1'000'000'000ULL + nanos;

    ob::HLCTimestamp ts{physical_ns, 42, 3};
    const std::string pretty = ts.pretty_print();

    // Verify format: "YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ L=X N=Y"
    EXPECT_NE(pretty.find("2024-01-15T09:50:00"), std::string::npos)
        << "pretty_print should contain ISO date, got: " << pretty;
    EXPECT_NE(pretty.find(".123456789Z"), std::string::npos)
        << "pretty_print should contain nanoseconds, got: " << pretty;
    EXPECT_NE(pretty.find("L=42"), std::string::npos)
        << "pretty_print should contain logical counter, got: " << pretty;
    EXPECT_NE(pretty.find("N=3"), std::string::npos)
        << "pretty_print should contain node_id, got: " << pretty;
}

TEST(HLCUnit, PrettyPrintZero) {
    ob::HLCTimestamp ts{0, 0, 0};
    const std::string pretty = ts.pretty_print();

    // Epoch zero = 1970-01-01T00:00:00.000000000Z L=0 N=0
    EXPECT_NE(pretty.find("1970-01-01T00:00:00"), std::string::npos)
        << "Zero timestamp should be epoch, got: " << pretty;
    EXPECT_NE(pretty.find("L=0"), std::string::npos);
    EXPECT_NE(pretty.find("N=0"), std::string::npos);
}

// ── Unit tests: from_string error handling ────────────────────────────────────

TEST(HLCUnit, FromStringEmpty) {
    std::string error;
    auto result = ob::HLCTimestamp::from_string("", error);
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(error.find("empty"), std::string::npos)
        << "Error should mention 'empty', got: " << error;
}

TEST(HLCUnit, FromStringMissingFirstDot) {
    std::string error;
    auto result = ob::HLCTimestamp::from_string("12345", error);
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(error.find("missing"), std::string::npos)
        << "Error should mention 'missing', got: " << error;
}

TEST(HLCUnit, FromStringMissingSecondDot) {
    std::string error;
    auto result = ob::HLCTimestamp::from_string("12345.67", error);
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(error.find("missing"), std::string::npos)
        << "Error should mention 'missing', got: " << error;
}

TEST(HLCUnit, FromStringOverflowLogical) {
    // logical > 65535
    std::string error;
    auto result = ob::HLCTimestamp::from_string("100.70000.1", error);
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(error.find("overflow"), std::string::npos)
        << "Error should mention 'overflow', got: " << error;
}

TEST(HLCUnit, FromStringOverflowNodeId) {
    // node_id > 65535
    std::string error;
    auto result = ob::HLCTimestamp::from_string("100.1.70000", error);
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(error.find("overflow"), std::string::npos)
        << "Error should mention 'overflow', got: " << error;
}

TEST(HLCUnit, FromStringInvalidChars) {
    std::string error;
    auto result = ob::HLCTimestamp::from_string("abc.1.2", error);
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(error.find("invalid"), std::string::npos)
        << "Error should mention 'invalid', got: " << error;
}

TEST(HLCUnit, FromStringExtraDots) {
    std::string error;
    auto result = ob::HLCTimestamp::from_string("1.2.3.4", error);
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(error.find("extra"), std::string::npos)
        << "Error should mention 'extra', got: " << error;
}

TEST(HLCUnit, FromStringEmptyFields) {
    std::string error;
    auto result = ob::HLCTimestamp::from_string(".1.2", error);
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(error.find("empty"), std::string::npos)
        << "Error should mention 'empty', got: " << error;
}

// ── Unit test: is_zero() ──────────────────────────────────────────────────────

TEST(HLCUnit, IsZeroTrue) {
    ob::HLCTimestamp ts{0, 0, 0};
    EXPECT_TRUE(ts.is_zero());
}

TEST(HLCUnit, IsZeroFalsePhysical) {
    ob::HLCTimestamp ts{1, 0, 0};
    EXPECT_FALSE(ts.is_zero());
}

TEST(HLCUnit, IsZeroFalseLogical) {
    ob::HLCTimestamp ts{0, 1, 0};
    EXPECT_FALSE(ts.is_zero());
}

TEST(HLCUnit, IsZeroFalseNodeId) {
    ob::HLCTimestamp ts{0, 0, 1};
    EXPECT_FALSE(ts.is_zero());
}
