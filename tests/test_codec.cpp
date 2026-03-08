// Tests for codec: property-based tests (Properties 10–12) and unit tests.
// Feature: orderbook-dbengine

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <vector>

#include "orderbook/codec.hpp"

// ═══════════════════════════════════════════════════════════════════════════════
// Property 10: Price compression round-trip
// Feature: orderbook-dbengine, Property 10: Price compression round-trip
// For any sequence of valid int64 price values, applying delta encoding
// followed by zigzag encoding and then reversing both operations should
// produce a sequence identical to the original.
// Validates: Requirements 5.3, 5.4
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(CodecProperty, prop_price_compression_roundtrip, ()) {
    const auto n = *rc::gen::inRange<size_t>(1, 101);
    std::vector<int64_t> prices(n);
    for (size_t i = 0; i < n; ++i) {
        prices[i] = *rc::gen::arbitrary<int64_t>();
    }

    auto encoded = ob::encode_prices(prices);
    RC_ASSERT(encoded.size() == prices.size());

    auto decoded = ob::decode_prices(encoded);
    RC_ASSERT(decoded.size() == prices.size());

    for (size_t i = 0; i < n; ++i) {
        RC_ASSERT(decoded[i] == prices[i]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 11: Volume compression round-trip
// Feature: orderbook-dbengine, Property 11: Volume compression round-trip
// For any sequence of valid uint64 quantity values where each value is
// ≤ 2^60 − 1, applying Simple8b encoding and then decoding should produce
// a sequence identical to the original.
// Validates: Requirements 6.2, 6.3
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(CodecProperty, prop_volume_compression_roundtrip, ()) {
    const auto n = *rc::gen::inRange<size_t>(1, 101);
    static constexpr uint64_t kMax = (1ULL << 60) - 1;
    std::vector<uint64_t> values(n);
    for (size_t i = 0; i < n; ++i) {
        values[i] = *rc::gen::inRange<uint64_t>(0, kMax + 1);
    }

    auto result  = ob::encode_simple8b(values);
    auto decoded = ob::decode_simple8b(result.words, n);

    RC_ASSERT(decoded.size() == n);
    for (size_t i = 0; i < n; ++i) {
        RC_ASSERT(decoded[i] == values[i]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 12: Volume compression fallback correctness
// Feature: orderbook-dbengine, Property 12: Volume compression fallback correctness
// For any quantity value exceeding 2^60 − 1, the columnar store should store
// it as a raw uint64 and decode it back to the original value without loss.
// Validates: Requirements 6.4
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(CodecProperty, prop_volume_fallback, ()) {
    static constexpr uint64_t kMax = (1ULL << 60) - 1;

    // Generate a mix: some normal values and at least one fallback value
    const auto n_normal   = *rc::gen::inRange<size_t>(0, 10);
    const auto n_fallback = *rc::gen::inRange<size_t>(1, 5);

    std::vector<uint64_t> values;
    values.reserve(n_normal + n_fallback);

    for (size_t i = 0; i < n_normal; ++i) {
        values.push_back(*rc::gen::inRange<uint64_t>(0, kMax + 1));
    }
    for (size_t i = 0; i < n_fallback; ++i) {
        // Generate values strictly > kMax
        uint64_t v = *rc::gen::inRange<uint64_t>(kMax + 1, UINT64_MAX);
        values.push_back(v);
    }

    // Shuffle so fallback values are not always at the end
    // (use a simple deterministic interleave via index)
    std::vector<uint64_t> shuffled;
    shuffled.reserve(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        shuffled.push_back(values[i]);
    }

    auto result = ob::encode_simple8b(shuffled);
    RC_ASSERT(result.has_fallback == true);

    auto decoded = ob::decode_simple8b(result.words, shuffled.size());
    RC_ASSERT(decoded.size() == shuffled.size());

    for (size_t i = 0; i < shuffled.size(); ++i) {
        RC_ASSERT(decoded[i] == shuffled[i]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests
// ═══════════════════════════════════════════════════════════════════════════════

// ── Delta encoding: ascending prices ─────────────────────────────────────────
TEST(Codec, DeltaEncodingAscending) {
    std::vector<int64_t> prices = {100, 101, 102, 103, 104};
    auto enc = ob::encode_prices(prices);
    auto dec = ob::decode_prices(enc);
    ASSERT_EQ(dec, prices);

    // First value is absolute (zigzag of 100)
    // delta[1..] = 1, so zigzag(1) = 2
    EXPECT_EQ(enc[0], (static_cast<uint64_t>(100) << 1) ^ 0ULL); // zigzag(100) = 200
    for (size_t i = 1; i < enc.size(); ++i) {
        EXPECT_EQ(enc[i], 2ULL); // zigzag(1) = 2
    }
}

// ── Delta encoding: descending prices ────────────────────────────────────────
TEST(Codec, DeltaEncodingDescending) {
    std::vector<int64_t> prices = {500, 499, 498, 497};
    auto enc = ob::encode_prices(prices);
    auto dec = ob::decode_prices(enc);
    ASSERT_EQ(dec, prices);

    // delta = -1 each step; zigzag(-1) = 1
    for (size_t i = 1; i < enc.size(); ++i) {
        EXPECT_EQ(enc[i], 1ULL); // zigzag(-1) = 1
    }
}

// ── Delta encoding: flat prices ───────────────────────────────────────────────
TEST(Codec, DeltaEncodingFlat) {
    std::vector<int64_t> prices = {1000, 1000, 1000, 1000};
    auto enc = ob::encode_prices(prices);
    auto dec = ob::decode_prices(enc);
    ASSERT_EQ(dec, prices);

    // delta = 0 for i>0; zigzag(0) = 0
    for (size_t i = 1; i < enc.size(); ++i) {
        EXPECT_EQ(enc[i], 0ULL);
    }
}

// ── Delta encoding: mixed prices ─────────────────────────────────────────────
TEST(Codec, DeltaEncodingMixed) {
    std::vector<int64_t> prices = {100, 200, 150, 300, -50};
    auto enc = ob::encode_prices(prices);
    auto dec = ob::decode_prices(enc);
    ASSERT_EQ(dec, prices);
}

// ── Zigzag boundary values ────────────────────────────────────────────────────
TEST(Codec, ZigzagBoundaryValues) {
    // Test INT64_MIN, INT64_MAX, 0, -1, 1 as single-element sequences
    auto check = [](int64_t v) {
        std::vector<int64_t> in = {v};
        auto enc = ob::encode_prices(in);
        auto dec = ob::decode_prices(enc);
        ASSERT_EQ(dec.size(), 1u);
        EXPECT_EQ(dec[0], v) << "Failed for value " << v;
    };

    check(0);
    check(1);
    check(-1);
    check(INT64_MAX);
    check(INT64_MIN);

    // Also test as second element (delta encoding path)
    auto check2 = [](int64_t first, int64_t second) {
        std::vector<int64_t> in = {first, second};
        auto enc = ob::encode_prices(in);
        auto dec = ob::decode_prices(enc);
        ASSERT_EQ(dec.size(), 2u);
        EXPECT_EQ(dec[0], first);
        EXPECT_EQ(dec[1], second);
    };

    check2(0, INT64_MAX);
    check2(0, INT64_MIN);
    check2(INT64_MAX, INT64_MAX);
    check2(INT64_MIN, INT64_MIN);
    check2(-1, 1);
    check2(1, -1);
}

// ── Simple8b: all selectors ───────────────────────────────────────────────────
TEST(Codec, Simple8bAllSelectors) {
    // Selector 0/1: all zeros (240 or 120 values)
    {
        std::vector<uint64_t> vals(240, 0);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 240);
        EXPECT_EQ(d, vals);
        EXPECT_FALSE(r.has_fallback);
    }
    // Selector 2: 60 × 1-bit values (0 or 1)
    {
        std::vector<uint64_t> vals(60, 1);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 60);
        EXPECT_EQ(d, vals);
    }
    // Selector 3: 30 × 2-bit values (max=3)
    {
        std::vector<uint64_t> vals(30, 3);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 30);
        EXPECT_EQ(d, vals);
    }
    // Selector 4: 20 × 3-bit values (max=7)
    {
        std::vector<uint64_t> vals(20, 7);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 20);
        EXPECT_EQ(d, vals);
    }
    // Selector 5: 15 × 4-bit values (max=15)
    {
        std::vector<uint64_t> vals(15, 15);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 15);
        EXPECT_EQ(d, vals);
    }
    // Selector 6: 12 × 5-bit values (max=31)
    {
        std::vector<uint64_t> vals(12, 31);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 12);
        EXPECT_EQ(d, vals);
    }
    // Selector 7: 10 × 6-bit values (max=63)
    {
        std::vector<uint64_t> vals(10, 63);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 10);
        EXPECT_EQ(d, vals);
    }
    // Selector 8: 8 × 7-bit values (max=127)
    {
        std::vector<uint64_t> vals(8, 127);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 8);
        EXPECT_EQ(d, vals);
    }
    // Selector 9: 7 × 8-bit values (max=255)
    {
        std::vector<uint64_t> vals(7, 255);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 7);
        EXPECT_EQ(d, vals);
    }
    // Selector 10: 6 × 10-bit values (max=1023)
    {
        std::vector<uint64_t> vals(6, 1023);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 6);
        EXPECT_EQ(d, vals);
    }
    // Selector 11: 5 × 12-bit values (max=4095)
    {
        std::vector<uint64_t> vals(5, 4095);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 5);
        EXPECT_EQ(d, vals);
    }
    // Selector 12: 4 × 15-bit values (max=32767)
    {
        std::vector<uint64_t> vals(4, 32767);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 4);
        EXPECT_EQ(d, vals);
    }
    // Selector 13: 3 × 20-bit values (max=1048575)
    {
        std::vector<uint64_t> vals(3, 1048575);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 3);
        EXPECT_EQ(d, vals);
    }
    // Selector 14: 2 × 30-bit values (max=1073741823)
    {
        std::vector<uint64_t> vals(2, 1073741823ULL);
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 2);
        EXPECT_EQ(d, vals);
    }
    // Selector 15: 1 × 60-bit value (max = 2^60-1)
    {
        uint64_t max60 = (1ULL << 60) - 1;
        // Use max60 - 1 to avoid triggering the fallback marker
        std::vector<uint64_t> vals = {max60 - 1};
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 1);
        EXPECT_EQ(d, vals);
        EXPECT_FALSE(r.has_fallback);
    }
}

// ── Simple8b: boundary values ─────────────────────────────────────────────────
TEST(Codec, Simple8bBoundaryValues) {
    // Zero
    {
        std::vector<uint64_t> vals = {0};
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 1);
        ASSERT_EQ(d.size(), 1u);
        EXPECT_EQ(d[0], 0u);
        EXPECT_FALSE(r.has_fallback);
    }
    // Max encodable without fallback: (1<<60)-1
    {
        uint64_t max60 = (1ULL << 60) - 2; // avoid marker
        std::vector<uint64_t> vals = {max60};
        auto r = ob::encode_simple8b(vals);
        auto d = ob::decode_simple8b(r.words, 1);
        ASSERT_EQ(d.size(), 1u);
        EXPECT_EQ(d[0], max60);
        EXPECT_FALSE(r.has_fallback);
    }
    // Value requiring fallback: (1<<60)
    {
        uint64_t over = 1ULL << 60;
        std::vector<uint64_t> vals = {over};
        auto r = ob::encode_simple8b(vals);
        EXPECT_TRUE(r.has_fallback);
        auto d = ob::decode_simple8b(r.words, 1);
        ASSERT_EQ(d.size(), 1u);
        EXPECT_EQ(d[0], over);
    }
    // UINT64_MAX fallback
    {
        std::vector<uint64_t> vals = {UINT64_MAX};
        auto r = ob::encode_simple8b(vals);
        EXPECT_TRUE(r.has_fallback);
        auto d = ob::decode_simple8b(r.words, 1);
        ASSERT_EQ(d.size(), 1u);
        EXPECT_EQ(d[0], UINT64_MAX);
    }
    // Mixed: normal + fallback + normal
    {
        std::vector<uint64_t> vals = {42, UINT64_MAX, 99};
        auto r = ob::encode_simple8b(vals);
        EXPECT_TRUE(r.has_fallback);
        auto d = ob::decode_simple8b(r.words, 3);
        ASSERT_EQ(d.size(), 3u);
        EXPECT_EQ(d[0], 42u);
        EXPECT_EQ(d[1], UINT64_MAX);
        EXPECT_EQ(d[2], 99u);
    }
    // Empty input
    {
        std::vector<uint64_t> vals;
        auto r = ob::encode_simple8b(vals);
        EXPECT_TRUE(r.words.empty());
        EXPECT_FALSE(r.has_fallback);
        auto d = ob::decode_simple8b(r.words, 0);
        EXPECT_TRUE(d.empty());
    }
}
