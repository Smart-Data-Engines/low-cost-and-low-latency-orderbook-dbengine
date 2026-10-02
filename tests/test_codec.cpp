// Tests for codec: property-based tests (Properties 10–12) and unit tests.
// Feature: orderbook-dbengine

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "orderbook/codec.hpp"

namespace {

struct Selector { uint32_t count; uint32_t bits; };
constexpr Selector kSelectors[16] = {
    {240, 0}, {120, 0}, {60, 1}, {30, 2}, {20, 3}, {15, 4}, {12, 5}, {10, 6},
    {8, 7},   {7, 8},   {6, 10}, {5, 12}, {4, 15}, {3, 20}, {2, 30}, {1, 60},
};

// The Simple8b decoder as it was before #49 unrolled it, kept as it was: the rewrite changes speed
// and nothing else, so it has to answer as this did for every input, words the encoder wrote or
// not. It is also the previous version's decoder, which is what reads a segment on a downgrade.
std::vector<uint64_t> reference_decode_simple8b(std::span<const uint64_t> words, size_t count) {
    static constexpr uint64_t kFallbackMarker = (1ULL << 60) - 1;

    std::vector<uint64_t> out;
    out.reserve(count);

    size_t wi = 0;
    while (wi < words.size() && out.size() < count) {
        uint64_t word = words[wi++];
        uint32_t sel  = static_cast<uint32_t>(word >> 60);

        // Check for fallback marker
        if (sel == 15) {
            uint64_t payload = word & kFallbackMarker;
            if (payload == kFallbackMarker && wi < words.size()) {
                // Raw uint64 fallback
                out.push_back(words[wi++]);
                continue;
            }
            // Normal selector 15: 1 × 60-bit value
            out.push_back(payload);
            continue;
        }

        uint32_t cnt  = kSelectors[sel].count;
        uint32_t bits = kSelectors[sel].bits;

        if (bits == 0) {
            // All zeros
            uint32_t emit = static_cast<uint32_t>(
                cnt < static_cast<uint32_t>(count - out.size()) ? cnt : (count - out.size()));
            for (uint32_t k = 0; k < emit; ++k) {
                out.push_back(0ULL);
            }
        } else {
            uint64_t mask = (bits == 64) ? UINT64_MAX : ((1ULL << bits) - 1);
            uint32_t emit = static_cast<uint32_t>(
                cnt < static_cast<uint32_t>(count - out.size()) ? cnt : (count - out.size()));
            for (uint32_t k = 0; k < emit; ++k) {
                out.push_back((word >> (k * bits)) & mask);
            }
        }
    }

    return out;
}

}  // namespace

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
    // The top of the range by name as well as by draw (#198): 2^60 - 1 is the one value whose
    // ordinary word is the fallback marker, and a uniform draw over 2^60 values never lands on it.
    const auto value = rc::gen::weightedOneOf<uint64_t>({
        {4, rc::gen::inRange<uint64_t>(0, kMax + 1)},
        {1, rc::gen::element(kMax, kMax - 1, uint64_t{0}, uint64_t{1})},
    });
    std::vector<uint64_t> values(n);
    for (size_t i = 0; i < n; ++i) {
        values[i] = *value;
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
// Since #198 the fallback also takes 2^60 − 1 itself, whose ordinary word is the marker.
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
        // At or past the marker's value, the ends by name (#198)
        uint64_t v = *rc::gen::weightedOneOf<uint64_t>({
            {3, rc::gen::inRange<uint64_t>(kMax + 1, UINT64_MAX)},
            {1, rc::gen::element(kMax, kMax + 1, uint64_t{UINT64_MAX})},
        });
        values.push_back(v);
    }

    // Shuffled, so that fallback values are not always at the end: the value after one is where
    // #198 showed. This said "shuffle" and copied the values in order until then.
    std::vector<uint64_t> shuffled = values;
    for (size_t i = shuffled.size(); i > 1; --i) {
        std::swap(shuffled[i - 1], shuffled[*rc::gen::inRange<size_t>(0, i)]);
    }

    auto result = ob::encode_simple8b(shuffled);
    RC_ASSERT(result.has_fallback == true);

    auto decoded = ob::decode_simple8b(result.words, shuffled.size());
    RC_ASSERT(decoded.size() == shuffled.size());

    for (size_t i = 0; i < shuffled.size(); ++i) {
        RC_ASSERT(decoded[i] == shuffled[i]);
    }
}

// Runs of one width each, so that the encoder chooses every selector and the fallback's values sit
// anywhere in the sequence: a draw uniform over 2^60 values, as Property 11 makes, puts nearly every
// value in a selector-15 word of its own.
//
// Widths and values at full size whatever the case's size: RapidCheck scales an integer's range by
// it, and at the small sizes a run begins with every "60-bit" value would be a few bits wide.
RC_GTEST_PROP(CodecProperty, prop_volume_roundtrip_in_runs_of_widths, ()) {
    static constexpr uint64_t kMarker = (1ULL << 60) - 1;
    std::vector<uint64_t> values;
    const auto runs = *rc::gen::inRange<size_t>(1, 9);
    for (size_t r = 0; r < runs; ++r) {
        const auto bits = *rc::gen::weightedOneOf<unsigned>({
            {8, rc::gen::resize(100, rc::gen::inRange<unsigned>(0, 61))},
            {1, rc::gen::just(61u)},                   // the marker's value and past it
        });
        const auto len  = *rc::gen::inRange<size_t>(1, 300);
        const auto value = rc::gen::resize(100, bits <= 60
            ? rc::gen::inRange<uint64_t>(0, 1ULL << bits)
            : rc::gen::element(kMarker, kMarker + 1, uint64_t{UINT64_MAX}));
        for (size_t k = 0; k < len; ++k) values.push_back(*value);
    }

    const auto result = ob::encode_simple8b(values);
    RC_ASSERT(ob::decode_simple8b(result.words, values.size()) == values);
}

// The unrolled decoder answers as the one before it for any words and any count - including words
// no encoder writes, a fallback marker with nothing after it, and a count that ends inside a word or
// past what the words hold.
//
// Selector and payload uniform whatever the case's size. The first version drew
// `arbitrary<uint64_t>()`, which RapidCheck makes 64 * size / 100 bits wide: up to size 94 no
// selector bit is set, and two of 25 cases are larger, so nearly every word was 240 zeros and
// dropping the last value of a partly filled word survived it (the mutation table of #49's step 1).
RC_GTEST_PROP(CodecProperty, prop_simple8b_decode_matches_the_previous_decoder, ()) {
    static constexpr uint64_t kMarkerWord = (15ULL << 60) | ((1ULL << 60) - 1);
    const auto word = rc::gen::resize(100, rc::gen::weightedOneOf<uint64_t>({
        {6, rc::gen::apply([](uint64_t sel, uint64_t payload) { return (sel << 60) | payload; },
                           rc::gen::inRange<uint64_t>(0, 16),
                           rc::gen::inRange<uint64_t>(0, 1ULL << 60))},
        {1, rc::gen::just(kMarkerWord)},           // a fallback's first word
    }));
    const auto words = *rc::gen::container<std::vector<uint64_t>>(word);

    // Up to just past the slots the words have, so that the count ends inside a word as often as not.
    size_t slots = 0;
    for (const uint64_t w : words) slots += kSelectors[w >> 60].count;
    const auto count = *rc::gen::resize(100, rc::gen::inRange<size_t>(0, slots + 2));
    RC_ASSERT(ob::decode_simple8b(words, count) == reference_decode_simple8b(words, count));
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
    // Max encodable without fallback: (1<<60)-2. (1<<60)-1 would be the marker (#198).
    {
        uint64_t max60 = (1ULL << 60) - 2;
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

// ── Simple8b: the value the fallback marker spells (#198) ────────────────────
// 2^60 - 1 fits a selector-15 word, and that word's payload is then the fallback marker, so the
// decoder took the word after it for a raw value. Before the fix [2^60 - 1, 5, 7] came back as the
// one value 4611686018427387965, and [3, 2^60 - 1, 9] as [3, 5764607523034234889]. The encoder
// spells the value as the fallback, which the previous decoder reads the same way: a segment
// written after the fix reads right on a build from before it.
TEST(Codec, Simple8bTheMarkersOwnValueTakesTheFallback) {
    const uint64_t marker = (1ULL << 60) - 1;
    const std::vector<std::vector<uint64_t>> cases = {
        {marker, 5, 7}, {3, marker, 9}, {marker, marker}, {marker}, {0, 0, marker, 1ULL << 60, 0},
    };
    for (const auto& vals : cases) {
        const auto r = ob::encode_simple8b(vals);
        EXPECT_TRUE(r.has_fallback);
        EXPECT_EQ(ob::decode_simple8b(r.words, vals.size()), vals);
        EXPECT_EQ(reference_decode_simple8b(r.words, vals.size()), vals);
    }
}

// ── Simple8b: how many values a decode returns ───────────────────────────────
// Up to the count, and no more than the words hold: the unused slots of a word the count ends in
// are not values, and words that run out are not padded.
TEST(Codec, Simple8bDecodeStopsAtTheCountAndAtTheWords) {
    const std::vector<uint64_t> zeros = {0};   // selector 0: 240 zeros
    EXPECT_EQ(ob::decode_simple8b(zeros, 5), std::vector<uint64_t>(5, 0));
    EXPECT_EQ(ob::decode_simple8b(zeros, 1000), std::vector<uint64_t>(240, 0));

    std::vector<uint64_t> vals(500);
    for (size_t i = 0; i < vals.size(); ++i) vals[i] = i % 8;   // 3 bits: 25 words of 20
    const auto r = ob::encode_simple8b(vals);
    ASSERT_EQ(r.words.size(), 25u);
    EXPECT_EQ(ob::decode_simple8b(r.words, 500), vals);
    EXPECT_EQ(ob::decode_simple8b(r.words, 10'000), vals);
    EXPECT_EQ(ob::decode_simple8b(r.words, 487),
              std::vector<uint64_t>(vals.begin(), vals.begin() + 487));
    EXPECT_TRUE(ob::decode_simple8b(r.words, 0).empty());
    EXPECT_TRUE(ob::decode_simple8b({}, 100).empty());
}
