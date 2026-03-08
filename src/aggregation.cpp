// Aggregation Engine implementation.
// SIMD-accelerated paths for sum_qty, vwap, min_price, max_price using
// AVX2/AVX-512 intrinsics with scalar fallbacks.
// Requirements: 9.1–9.11, 13.6

#include "orderbook/aggregation.hpp"

#include <algorithm>
#include <climits>
#include <cstdint>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __AVX512F__
#include <immintrin.h>
#endif

namespace ob {

namespace {

// Effective level count: min(n_levels, side.depth)
inline uint32_t effective_n(const SoASide& side, uint32_t n_levels) {
    return std::min(n_levels, side.depth);
}

// ── Scalar helpers ────────────────────────────────────────────────────────────

static int64_t scalar_sum_qty(const SoASide& side, uint32_t n) {
    int64_t sum = 0;
    for (uint32_t i = 0; i < n; ++i) {
        sum += static_cast<int64_t>(side.quantities[i]);
    }
    return sum;
}

static int64_t scalar_min_price(const SoASide& side, uint32_t n) {
    int64_t mn = side.prices[0];
    for (uint32_t i = 1; i < n; ++i) {
        if (side.prices[i] < mn) mn = side.prices[i];
    }
    return mn;
}

static int64_t scalar_max_price(const SoASide& side, uint32_t n) {
    int64_t mx = side.prices[0];
    for (uint32_t i = 1; i < n; ++i) {
        if (side.prices[i] > mx) mx = side.prices[i];
    }
    return mx;
}

// VWAP numerator: SUM(prices[i] * quantities[i])
// Returns {numerator, total_qty}
static std::pair<__int128, int64_t> scalar_vwap_parts(const SoASide& side, uint32_t n) {
    __int128 num = 0;
    int64_t  denom = 0;
    for (uint32_t i = 0; i < n; ++i) {
        num   += static_cast<__int128>(side.prices[i]) *
                 static_cast<__int128>(side.quantities[i]);
        denom += static_cast<int64_t>(side.quantities[i]);
    }
    return {num, denom};
}

// ── AVX2 accelerated helpers ──────────────────────────────────────────────────

#ifdef __AVX2__

static int64_t avx2_sum_qty(const SoASide& side, uint32_t n) {
    const uint64_t* q = side.quantities;
    __m256i acc = _mm256_setzero_si256();
    uint32_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q + i));
        acc = _mm256_add_epi64(acc, v);
    }
    // Horizontal sum of 4 × int64
    __m128i lo  = _mm256_castsi256_si128(acc);
    __m128i hi  = _mm256_extracti128_si256(acc, 1);
    __m128i sum = _mm_add_epi64(lo, hi);
    int64_t result = _mm_cvtsi128_si64(sum) +
                     _mm_cvtsi128_si64(_mm_unpackhi_epi64(sum, sum));
    // Scalar tail
    for (; i < n; ++i) result += static_cast<int64_t>(q[i]);
    return result;
}

static int64_t avx2_min_price(const SoASide& side, uint32_t n) {
    const int64_t* p = side.prices;
    // _mm256_min_epi64 requires AVX-512VL; use scalar for min/max under pure AVX2
    // (AVX2 does not have 64-bit integer min/max intrinsics)
    return scalar_min_price(side, n);
}

static int64_t avx2_max_price(const SoASide& side, uint32_t n) {
    return scalar_max_price(side, n);
}

static std::pair<__int128, int64_t> avx2_vwap_parts(const SoASide& side, uint32_t n) {
    // AVX2 doesn't have 64-bit multiply accumulate; fall back to scalar for correctness
    return scalar_vwap_parts(side, n);
}

#endif // __AVX2__

// ── AVX-512 accelerated helpers ───────────────────────────────────────────────

#ifdef __AVX512F__

static int64_t avx512_sum_qty(const SoASide& side, uint32_t n) {
    const uint64_t* q = side.quantities;
    __m512i acc = _mm512_setzero_si512();
    uint32_t i = 0;
    for (; i + 8 <= n; i += 8) {
        __m512i v = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(q + i));
        acc = _mm512_add_epi64(acc, v);
    }
    int64_t result = static_cast<int64_t>(_mm512_reduce_add_epi64(acc));
    for (; i < n; ++i) result += static_cast<int64_t>(q[i]);
    return result;
}

static int64_t avx512_min_price(const SoASide& side, uint32_t n) {
    const int64_t* p = side.prices;
    __m512i acc = _mm512_set1_epi64(INT64_MAX);
    uint32_t i = 0;
    for (; i + 8 <= n; i += 8) {
        __m512i v = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(p + i));
        acc = _mm512_min_epi64(acc, v);
    }
    int64_t result = _mm512_reduce_min_epi64(acc);
    for (; i < n; ++i) result = std::min(result, p[i]);
    return result;
}

static int64_t avx512_max_price(const SoASide& side, uint32_t n) {
    const int64_t* p = side.prices;
    __m512i acc = _mm512_set1_epi64(INT64_MIN);
    uint32_t i = 0;
    for (; i + 8 <= n; i += 8) {
        __m512i v = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(p + i));
        acc = _mm512_max_epi64(acc, v);
    }
    int64_t result = _mm512_reduce_max_epi64(acc);
    for (; i < n; ++i) result = std::max(result, p[i]);
    return result;
}

#endif // __AVX512F__

// ── Dispatch wrappers ─────────────────────────────────────────────────────────

static int64_t dispatch_sum_qty(const SoASide& side, uint32_t n) {
#ifdef __AVX512F__
    return avx512_sum_qty(side, n);
#elif defined(__AVX2__)
    return avx2_sum_qty(side, n);
#else
    return scalar_sum_qty(side, n);
#endif
}

static int64_t dispatch_min_price(const SoASide& side, uint32_t n) {
#ifdef __AVX512F__
    return avx512_min_price(side, n);
#elif defined(__AVX2__)
    return avx2_min_price(side, n);
#else
    return scalar_min_price(side, n);
#endif
}

static int64_t dispatch_max_price(const SoASide& side, uint32_t n) {
#ifdef __AVX512F__
    return avx512_max_price(side, n);
#elif defined(__AVX2__)
    return avx2_max_price(side, n);
#else
    return scalar_max_price(side, n);
#endif
}

static std::pair<__int128, int64_t> dispatch_vwap_parts(const SoASide& side, uint32_t n) {
#ifdef __AVX2__
    return avx2_vwap_parts(side, n);
#else
    return scalar_vwap_parts(side, n);
#endif
}

} // anonymous namespace

// ── AggregationEngine method implementations ──────────────────────────────────

AggResult AggregationEngine::sum_qty(const SoASide& side, uint32_t n_levels) const {
    uint32_t n = effective_n(side, n_levels);
    if (n == 0) return {0, true};
    return {dispatch_sum_qty(side, n), false};
}

AggResult AggregationEngine::avg_price(const SoASide& side, uint32_t n_levels) const {
    uint32_t n = effective_n(side, n_levels);
    if (n == 0) return {0, true};
    int64_t sum = 0;
    for (uint32_t i = 0; i < n; ++i) sum += side.prices[i];
    return {sum / static_cast<int64_t>(n), false};
}

AggResult AggregationEngine::min_price(const SoASide& side, uint32_t n_levels) const {
    uint32_t n = effective_n(side, n_levels);
    if (n == 0) return {0, true};
    return {dispatch_min_price(side, n), false};
}

AggResult AggregationEngine::max_price(const SoASide& side, uint32_t n_levels) const {
    uint32_t n = effective_n(side, n_levels);
    if (n == 0) return {0, true};
    return {dispatch_max_price(side, n), false};
}

AggResult AggregationEngine::vwap(const SoASide& side, uint32_t n_levels) const {
    uint32_t n = effective_n(side, n_levels);
    if (n == 0) return {0, true};
    auto [num, denom] = dispatch_vwap_parts(side, n);
    if (denom == 0) return {0, true};
    // Scale by 10^6
    int64_t result = static_cast<int64_t>((num * 1'000'000LL) / denom);
    return {result, false};
}

AggResult AggregationEngine::spread(const SoASide& bid, const SoASide& ask) const {
    if (bid.depth == 0 || ask.depth == 0) return {0, true};
    return {ask.prices[0] - bid.prices[0], false};
}

AggResult AggregationEngine::mid_price(const SoASide& bid, const SoASide& ask) const {
    if (bid.depth == 0 || ask.depth == 0) return {0, true};
    // (ask + bid) / 2 scaled by 10^6
    int64_t sum = ask.prices[0] + bid.prices[0];
    // Multiply by 10^6 first to preserve precision, then divide by 2
    int64_t result = (sum * 1'000'000LL) / 2LL;
    return {result, false};
}

AggResult AggregationEngine::imbalance(const SoASide& bid, const SoASide& ask,
                                        uint32_t n_levels) const {
    uint32_t nb = effective_n(bid, n_levels);
    uint32_t na = effective_n(ask, n_levels);

    int64_t bid_vol = 0;
    for (uint32_t i = 0; i < nb; ++i) bid_vol += static_cast<int64_t>(bid.quantities[i]);

    int64_t ask_vol = 0;
    for (uint32_t i = 0; i < na; ++i) ask_vol += static_cast<int64_t>(ask.quantities[i]);

    int64_t total = bid_vol + ask_vol;
    if (total == 0) return {0, true};

    // (bid_vol - ask_vol) * 10^9 / (bid_vol + ask_vol)
    __int128 num = static_cast<__int128>(bid_vol - ask_vol) * 1'000'000'000LL;
    int64_t result = static_cast<int64_t>(num / total);
    return {result, false};
}

AggResult AggregationEngine::depth_at_price(const SoASide& side, int64_t price) const {
    for (uint32_t i = 0; i < side.depth; ++i) {
        if (side.prices[i] == price) {
            return {static_cast<int64_t>(side.quantities[i]), false};
        }
    }
    return {0, false};
}

AggResult AggregationEngine::depth_within_range(const SoASide& side,
                                                  int64_t lo, int64_t hi) const {
    int64_t sum = 0;
    bool found = false;
    for (uint32_t i = 0; i < side.depth; ++i) {
        if (side.prices[i] >= lo && side.prices[i] <= hi) {
            sum += static_cast<int64_t>(side.quantities[i]);
            found = true;
        }
    }
    return {sum, !found};
}

AggResult AggregationEngine::cumulative_volume(const SoASide& side, uint32_t n_levels) const {
    uint32_t n = effective_n(side, n_levels);
    if (n == 0) return {0, true};
    int64_t sum = 0;
    for (uint32_t i = 0; i < n; ++i) sum += static_cast<int64_t>(side.quantities[i]);
    return {sum, false};
}

} // namespace ob
