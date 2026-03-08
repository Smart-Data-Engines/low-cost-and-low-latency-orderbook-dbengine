#pragma once

#include <cstdint>
#include <span>
#include <vector>

namespace ob {

// ── Delta + Zigzag price codec ────────────────────────────────────────────────
//
// Encoding:
//   delta[0]  = price[0]                        (absolute anchor)
//   delta[i]  = price[i] - price[i-1]           (for i > 0)
//   zigzag[i] = (delta[i] << 1) ^ (delta[i] >> 63)
//
// Decoding:
//   delta[i]  = (zigzag[i] >> 1) ^ -(zigzag[i] & 1)
//   price[i]  = price[i-1] + delta[i]           (prefix sum)

/// Encode a sequence of int64 prices to uint64 zigzag(delta) values.
/// prices[0] is stored as absolute (zigzag-encoded).
std::vector<uint64_t> encode_prices(std::span<const int64_t> prices);

/// Decode a sequence of uint64 zigzag(delta) values back to int64 prices.
std::vector<int64_t> decode_prices(std::span<const uint64_t> encoded);

// ── Simple8b volume codec ─────────────────────────────────────────────────────
//
// Selector table (4-bit selector, values per word, bits per value):
//   0:  240 × 0 bits  (all zeros)
//   1:  120 × 0 bits  (all zeros)
//   2:   60 × 1 bit
//   3:   30 × 2 bits
//   4:   20 × 3 bits
//   5:   15 × 4 bits
//   6:   12 × 5 bits
//   7:   10 × 6 bits
//   8:    8 × 7 bits
//   9:    7 × 8 bits
//  10:    6 × 10 bits
//  11:    5 × 12 bits
//  12:    4 × 15 bits
//  13:    3 × 20 bits
//  14:    2 × 30 bits
//  15:    1 × 60 bits  (or raw uint64 fallback)
//
// Fallback: values > (1ULL<<60)-1 are stored as two words:
//   word 0: selector=15, value=0xFFFFFFFFFFFFFFF (all 60 bits set = marker)
//   word 1: raw uint64 value

struct Simple8bResult {
    std::vector<uint64_t> words;  ///< encoded 64-bit words
    bool has_fallback;            ///< true if any value used raw uint64 fallback
};

/// Encode uint64 quantities using Simple8b.
Simple8bResult encode_simple8b(std::span<const uint64_t> values);

/// Decode Simple8b-encoded words back to uint64 values.
/// count: expected number of output values.
std::vector<uint64_t> decode_simple8b(std::span<const uint64_t> words, size_t count);

} // namespace ob
