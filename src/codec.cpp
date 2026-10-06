#include "orderbook/codec.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>

namespace ob {

// ── Zigzag helpers ────────────────────────────────────────────────────────────

static inline uint64_t zigzag_encode(int64_t v) noexcept {
    return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
}

static inline int64_t zigzag_decode(uint64_t v) noexcept {
    return static_cast<int64_t>((v >> 1) ^ -(v & 1ULL));
}

// ── Delta + Zigzag price codec ────────────────────────────────────────────────

// The deltas are computed in unsigned arithmetic and reinterpreted, not subtracted as int64_t.
//
// `prices[i] - prev` on two int64_t is undefined when it overflows, and a property test over the
// full int64 range finds that in a handful of cases — UBSan reported
// "-5398869315210128419 - 3959960346406320104 cannot be represented in type 'long int'". Unsigned
// subtraction wraps by definition, and in C++20 the conversion back to a signed type is modular
// rather than implementation-defined, so this is exact for **every** int64 input rather than for the
// range real prices happen to occupy. Which also makes the codec's round trip total: with wrapping
// both ways, decode inverts encode even for values that overflow.
//
// It went unnoticed because the sanitizer builds did not instrument this library at all (#83).
std::vector<uint64_t> encode_prices(std::span<const int64_t> prices) {
    std::vector<uint64_t> out;
    out.reserve(prices.size());
    uint64_t prev = 0;
    for (size_t i = 0; i < prices.size(); ++i) {
        const uint64_t cur = static_cast<uint64_t>(prices[i]);
        const int64_t delta = static_cast<int64_t>(cur - prev);
        out.push_back(zigzag_encode(delta));
        prev = cur;
    }
    return out;
}

std::vector<int64_t> decode_prices(std::span<const uint64_t> encoded) {
    std::vector<int64_t> out;
    decode_prices_into(encoded, out);
    return out;
}

void decode_prices_into(std::span<const uint64_t> encoded, std::vector<int64_t>& out) {
    // Appended rather than sized and written through a pointer: for a new vector, zeroing it first
    // cost more than the push_back saves (#49's step 1 measured 1.39 against 1.52 ns a value).
    out.clear();
    out.reserve(encoded.size());
    uint64_t prev = 0;
    for (size_t i = 0; i < encoded.size(); ++i) {
        const uint64_t delta = static_cast<uint64_t>(zigzag_decode(encoded[i]));
        const uint64_t price = prev + delta;      // wraps, matching encode_prices() exactly
        out.push_back(static_cast<int64_t>(price));
        prev = price;
    }
}

// ── Simple8b codec ────────────────────────────────────────────────────────────

// Selector table: {values_per_word, bits_per_value}
struct S8bSelector {
    uint32_t count;
    uint32_t bits;
};

static constexpr S8bSelector kSelectors[16] = {
    {240, 0},   // 0
    {120, 0},   // 1
    { 60, 1},   // 2
    { 30, 2},   // 3
    { 20, 3},   // 4
    { 15, 4},   // 5
    { 12, 5},   // 6
    { 10, 6},   // 7
    {  8, 7},   // 8
    {  7, 8},   // 9
    {  6, 10},  // 10
    {  5, 12},  // 11
    {  4, 15},  // 12
    {  3, 20},  // 13
    {  2, 30},  // 14
    {  1, 60},  // 15
};

static constexpr uint64_t kFallbackMarker = (1ULL << 60) - 1; // all 60 bits set

// The first selector, in the table's order, whose values all fit - a value fits when it is no
// wider than the selector's bits, and a selector of 0 bits takes only zeros. The widths' running
// maximum is extended only as far as a selector needs, and no further once it is wider than that
// selector takes, since a longer prefix is no narrower. The version before this scanned the values
// again for every selector it tried, up to 240 of them for the first: on a column of wide values
// that scan was most of what encoding cost (segment format v3 encodes each column several ways).
// It picks what that version picked, which `test_codec` holds it to.
static int best_selector(const uint64_t* begin, size_t available) {
    uint8_t widest[241];   // widest[k]: the widest of the first k values, as far as computed
    widest[0] = 0;
    size_t known = 0;
    for (int sel = 0; sel <= 15; ++sel) {
        const size_t use = std::min<size_t>(kSelectors[sel].count, available);
        const unsigned bits = kSelectors[sel].bits;
        while (known < use && widest[known] <= bits) {
            const uint64_t v = begin[known];
            const auto w = static_cast<uint8_t>(v == 0 ? 0 : 64 - __builtin_clzll(v));
            widest[known + 1] = std::max(widest[known], w);
            ++known;
        }
        // Stopped short of `use`: the values so far are already wider than this selector takes.
        if (known >= use && widest[use] <= bits) return sel;
    }
    return -1; // unreachable for valid inputs
}

Simple8bResult encode_simple8b(std::span<const uint64_t> values) {
    Simple8bResult result;
    result.has_fallback = false;

    size_t i = 0;
    while (i < values.size()) {
        // Check for fallback: a value past the 60 bits a word holds, **or equal to the marker**
        // (#198). A selector-15 word whose payload is all ones is read as the fallback marker, so
        // 2^60 - 1 stored as an ordinary value made the decoder take the next word for a raw value:
        // measured, [2^60 - 1, 5, 7] came back as one value, 4611686018427387965. It is the one
        // value both encodings would spell the same, and the fallback is the unambiguous one.
        if (values[i] >= kFallbackMarker) {
            // Emit fallback: two words
            // Word 0: selector=15 in top 4 bits, value = kFallbackMarker
            uint64_t word0 = (static_cast<uint64_t>(15) << 60) | kFallbackMarker;
            result.words.push_back(word0);
            result.words.push_back(values[i]);
            result.has_fallback = true;
            ++i;
            continue;
        }

        // Find best selector for values starting at i
        int sel = best_selector(values.data() + i, values.size() - i);
        if (sel < 0) {
            // Should not happen; treat as fallback
            uint64_t word0 = (static_cast<uint64_t>(15) << 60) | kFallbackMarker;
            result.words.push_back(word0);
            result.words.push_back(values[i]);
            result.has_fallback = true;
            ++i;
            continue;
        }

        uint32_t cnt  = kSelectors[sel].count;
        uint32_t bits = kSelectors[sel].bits;
        uint32_t use  = static_cast<uint32_t>(
            cnt < static_cast<uint32_t>(values.size() - i) ? cnt : (values.size() - i));

        uint64_t word = static_cast<uint64_t>(sel) << 60;

        if (bits > 0) {
            for (uint32_t k = 0; k < use; ++k) {
                word |= (values[i + k] << (k * bits));
            }
        }
        // For bits==0 (selectors 0,1): word is just the selector, values are all 0

        result.words.push_back(word);
        i += use;
    }

    return result;
}

namespace {

/// One whole word of `Count` values of `Bits` bits: every shift a constant and every value its own
/// expression, so there is no loop left to keep (#49). The table-driven loop this replaces read
/// both numbers from `kSelectors` per word and bounded every value by the count; GCC left the
/// widest words' loops rolled even with the numbers constant, which is why this is a fold and not
/// a `for`.
template <unsigned Bits, unsigned... K>
inline void unpack_values(uint64_t word, uint64_t* out, std::integer_sequence<unsigned, K...>) noexcept {
    constexpr uint64_t mask = (1ULL << Bits) - 1;
    ((out[K] = (word >> (K * Bits)) & mask), ...);
}

template <unsigned Bits, unsigned Count>
inline void unpack(uint64_t word, uint64_t* out) noexcept {
    unpack_values<Bits>(word, out, std::make_integer_sequence<unsigned, Count>{});
}

}  // namespace

namespace {

/// The words' values into `o`, which has room for `want`; how many it wrote. `kZeroed`: whether `o`
/// holds zeros already - a new vector's - so that the two all-zero selectors only move the cursor;
/// a vector used before holds an earlier decode's values, and they write theirs.
///
/// The words are read through a pointer and a count held in locals, not through the span: measured
/// (#49's step 2), that is the faster loop for words of several values - 2.21 against 2.42 ns a
/// value on the i3-7100U, level on the ARM host - which is what a column of quantities is made of,
/// at the price of words holding one 60-bit value each on the ARM host.
template <bool kZeroed>
size_t decode_simple8b_words(std::span<const uint64_t> words, size_t want, uint64_t* o,
                             size_t* used = nullptr) noexcept {
    const uint64_t* w = words.data();
    const size_t nw = words.size();
    size_t n = 0;
    size_t wi = 0;
    while (wi < nw && n < want) {
        const uint64_t word = w[wi++];
        const uint32_t sel  = static_cast<uint32_t>(word >> 60);
        const size_t   room = want - n;
        if (kSelectors[sel].count > room) {
            // More slots than values remain - the encoder's last word, partly filled. Never
            // selector 15, whose one slot always fits.
            const uint32_t bits = kSelectors[sel].bits;
            if (bits != 0) {
                const uint64_t mask = (1ULL << bits) - 1;
                for (size_t k = 0; k < room; ++k) o[n + k] = (word >> (k * bits)) & mask;
            } else if (!kZeroed) {
                std::fill_n(o + n, room, uint64_t{0});
            }
            if (used != nullptr) *used = wi;
            return want;
        }
        switch (sel) {
        case 0:  if (!kZeroed) std::fill_n(o + n, 240, uint64_t{0}); n += 240; break;
        case 1:  if (!kZeroed) std::fill_n(o + n, 120, uint64_t{0}); n += 120; break;
        case 2:  unpack<1, 60>(word, o + n);  n += 60; break;
        case 3:  unpack<2, 30>(word, o + n);  n += 30; break;
        case 4:  unpack<3, 20>(word, o + n);  n += 20; break;
        case 5:  unpack<4, 15>(word, o + n);  n += 15; break;
        case 6:  unpack<5, 12>(word, o + n);  n += 12; break;
        case 7:  unpack<6, 10>(word, o + n);  n += 10; break;
        case 8:  unpack<7, 8>(word, o + n);   n += 8;  break;
        case 9:  unpack<8, 7>(word, o + n);   n += 7;  break;
        case 10: unpack<10, 6>(word, o + n);  n += 6;  break;
        case 11: unpack<12, 5>(word, o + n);  n += 5;  break;
        case 12: unpack<15, 4>(word, o + n);  n += 4;  break;
        case 13: unpack<20, 3>(word, o + n);  n += 3;  break;
        case 14: unpack<30, 2>(word, o + n);  n += 2;  break;
        default: {
            // One 60-bit value - or, with the marker for a payload and a word after it, the
            // fallback, whose value is that word.
            const uint64_t payload = word & kFallbackMarker;
            if (payload == kFallbackMarker && wi < nw) {
                o[n++] = w[wi++];
            } else {
                o[n++] = payload;
            }
            break;
        }
        }
    }
    if (used != nullptr) *used = wi;
    return n;
}

}  // namespace

std::vector<uint64_t> decode_simple8b(std::span<const uint64_t> words, size_t count) {
    // Never larger than the words can hold, 240 values each: the push_back version only reserved
    // `count`, and sizing touches the pages. Trimmed to what the words held.
    const size_t want = std::min(count, words.size() * 240);
    std::vector<uint64_t> out(want);
    out.resize(decode_simple8b_words<true>(words, want, out.data()));
    return out;
}

void decode_simple8b_into(std::span<const uint64_t> words, size_t count, std::vector<uint64_t>& out) {
    const size_t want = std::min(count, words.size() * 240);
    out.resize(want);   // capacity kept; what an earlier decode left is written over or cut off
    out.resize(decode_simple8b_words<false>(words, want, out.data()));
}

bool decode_simple8b_exact(std::span<const uint64_t> words, size_t count, std::vector<uint64_t>& out) {
    out.resize(count);
    size_t used = 0;
    const size_t n = decode_simple8b_words<false>(words, count, out.data(), &used);
    return n == count && used == words.size();
}

size_t simple8b_words_used(std::span<const uint64_t> words, size_t count) {
    // The walk decode_simple8b_words() makes, counting instead of unpacking.
    size_t n = 0;
    size_t wi = 0;
    while (n < count) {
        if (wi == words.size()) return words.size() + 1;
        const uint64_t word = words[wi++];
        const uint32_t sel = static_cast<uint32_t>(word >> 60);
        if (sel == 15 && (word & kFallbackMarker) == kFallbackMarker && wi < words.size()) ++wi;
        n += std::min<size_t>(kSelectors[sel].count, count - n);
    }
    return wi;
}

} // namespace ob
