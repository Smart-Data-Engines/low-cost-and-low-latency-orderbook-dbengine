#include "orderbook/codec.hpp"

#include <cassert>
#include <cstdint>
#include <limits>
#include <stdexcept>

namespace ob {

// ── Zigzag helpers ────────────────────────────────────────────────────────────

static inline uint64_t zigzag_encode(int64_t v) noexcept {
    return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
}

static inline int64_t zigzag_decode(uint64_t v) noexcept {
    return static_cast<int64_t>((v >> 1) ^ -(v & 1ULL));
}

// ── Delta + Zigzag price codec ────────────────────────────────────────────────

std::vector<uint64_t> encode_prices(std::span<const int64_t> prices) {
    std::vector<uint64_t> out;
    out.reserve(prices.size());
    int64_t prev = 0;
    for (size_t i = 0; i < prices.size(); ++i) {
        int64_t delta = (i == 0) ? prices[0] : (prices[i] - prev);
        out.push_back(zigzag_encode(delta));
        prev = prices[i];
    }
    return out;
}

std::vector<int64_t> decode_prices(std::span<const uint64_t> encoded) {
    std::vector<int64_t> out;
    out.reserve(encoded.size());
    int64_t prev = 0;
    for (size_t i = 0; i < encoded.size(); ++i) {
        int64_t delta = zigzag_decode(encoded[i]);
        int64_t price = (i == 0) ? delta : (prev + delta);
        out.push_back(price);
        prev = price;
    }
    return out;
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
static constexpr uint64_t kMaxSimple8b    = (1ULL << 60) - 1;

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
        // Check for fallback: value > max Simple8b
        if (values[i] > kMaxSimple8b) {
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

std::vector<uint64_t> decode_simple8b(std::span<const uint64_t> words, size_t count) {
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

} // namespace ob
