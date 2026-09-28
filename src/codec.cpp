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

// Find the best selector that fits all values in [begin, begin+count).
// Returns selector index, or -1 if no selector fits (shouldn't happen for sel>=2).
static int best_selector(const uint64_t* begin, size_t available) {
    for (int sel = 0; sel <= 15; ++sel) {
        uint32_t cnt  = kSelectors[sel].count;
        uint32_t bits = kSelectors[sel].bits;

        if (cnt > static_cast<uint32_t>(available)) {
            // Can't fill a full word; only use this selector if it's the last
            // group and we have fewer values than cnt.
            // We'll handle partial words by padding with zeros.
        }

        uint32_t use = static_cast<uint32_t>(
            cnt < static_cast<uint32_t>(available) ? cnt : available);

        if (bits == 0) {
            // All values must be 0
            bool ok = true;
            for (uint32_t i = 0; i < use; ++i) {
                if (begin[i] != 0) { ok = false; break; }
            }
            if (ok) return sel;
            continue;
        }

        uint64_t max_val = (bits == 64) ? UINT64_MAX : ((1ULL << bits) - 1);
        bool ok = true;
        for (uint32_t i = 0; i < use; ++i) {
            if (begin[i] > max_val) { ok = false; break; }
        }
        if (ok) return sel;
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

std::vector<uint64_t> decode_simple8b(std::span<const uint64_t> words, size_t count) {
    std::vector<uint64_t> out;
    decode_simple8b_into(words, count, out);
    return out;
}

void decode_simple8b_into(std::span<const uint64_t> words, size_t count, std::vector<uint64_t>& out) {
    // Sized once and trimmed at the end to what the words held. Never larger than they can hold,
    // 240 values each: the push_back version only reserved `count`, and sizing touches the pages.
    // A vector used before still holds its last values, so the all-zero selectors write theirs.
    const size_t want = std::min(count, words.size() * 240);
    out.resize(want);
    uint64_t* o = out.data();
    size_t n = 0;
    size_t wi = 0;
    while (wi < words.size() && n < want) {
        const uint64_t word = words[wi++];
        const uint32_t sel  = static_cast<uint32_t>(word >> 60);
        const size_t   room = want - n;
        if (kSelectors[sel].count > room) {
            // More slots than values remain - the encoder's last word, partly filled. Never
            // selector 15, whose one slot always fits.
            const uint32_t bits = kSelectors[sel].bits;
            if (bits != 0) {
                const uint64_t mask = (1ULL << bits) - 1;
                for (size_t k = 0; k < room; ++k) o[n + k] = (word >> (k * bits)) & mask;
            } else {
                std::fill_n(o + n, room, uint64_t{0});
            }
            n = want;
            break;
        }
        switch (sel) {
        case 0:  std::fill_n(o + n, 240, uint64_t{0}); n += 240; break;
        case 1:  std::fill_n(o + n, 120, uint64_t{0}); n += 120; break;
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
            if (payload == kFallbackMarker && wi < words.size()) {
                o[n++] = words[wi++];
            } else {
                o[n++] = payload;
            }
            break;
        }
        }
    }
    out.resize(n);
}

} // namespace ob
