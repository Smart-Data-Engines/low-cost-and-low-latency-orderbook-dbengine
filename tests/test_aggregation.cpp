// Tests for AggregationEngine: property-based tests (Properties 17–27) and unit tests.
// Feature: orderbook-dbengine

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <vector>

#include "orderbook/aggregation.hpp"
#include "orderbook/soa_buffer.hpp"

// ── Test helpers ──────────────────────────────────────────────────────────────

// Fill a SoASide with levels from given prices and quantities vectors.
static void fill_side(ob::SoASide& side,
                      const std::vector<int64_t>& prices,
                      const std::vector<uint64_t>& qtys) {
    side.depth = 0;
    side.version.store(0, std::memory_order_relaxed);
    uint32_t n = static_cast<uint32_t>(std::min(prices.size(), qtys.size()));
    n = std::min(n, static_cast<uint32_t>(ob::SoASide::MAX_LEVELS));
    for (uint32_t i = 0; i < n; ++i) {
        side.prices[i]       = prices[i];
        side.quantities[i]   = qtys[i];
        side.order_counts[i] = 1;
    }
    side.depth = n;
}

// ── Reference implementations ─────────────────────────────────────────────────

static int64_t ref_sum_qty(const std::vector<uint64_t>& qtys, uint32_t n) {
    int64_t s = 0;
    for (uint32_t i = 0; i < n; ++i) s += static_cast<int64_t>(qtys[i]);
    return s;
}

static int64_t ref_avg_price(const std::vector<int64_t>& prices, uint32_t n) {
    int64_t s = 0;
    for (uint32_t i = 0; i < n; ++i) s += prices[i];
    return s / static_cast<int64_t>(n);
}

static int64_t ref_min_price(const std::vector<int64_t>& prices, uint32_t n) {
    int64_t mn = prices[0];
    for (uint32_t i = 1; i < n; ++i) mn = std::min(mn, prices[i]);
    return mn;
}

static int64_t ref_max_price(const std::vector<int64_t>& prices, uint32_t n) {
    int64_t mx = prices[0];
    for (uint32_t i = 1; i < n; ++i) mx = std::max(mx, prices[i]);
    return mx;
}

static int64_t ref_vwap(const std::vector<int64_t>& prices,
                        const std::vector<uint64_t>& qtys, uint32_t n) {
    __int128 num   = 0;
    int64_t  denom = 0;
    for (uint32_t i = 0; i < n; ++i) {
        num   += static_cast<__int128>(prices[i]) * static_cast<__int128>(qtys[i]);
        denom += static_cast<int64_t>(qtys[i]);
    }
    if (denom == 0) return 0;
    return static_cast<int64_t>((num * 1'000'000LL) / denom);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 17: SUM aggregation correctness
// Feature: orderbook-dbengine, Property 17: SUM aggregation correctness
// Validates: Requirements 9.1
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_sum, ()) {
    const auto n = *rc::gen::inRange<uint32_t>(1, 101);
    std::vector<int64_t>  prices(n);
    std::vector<uint64_t> qtys(n);
    for (uint32_t i = 0; i < n; ++i) {
        prices[i] = *rc::gen::inRange<int64_t>(1, 1'000'000LL);
        qtys[i]   = *rc::gen::inRange<uint64_t>(1, 1'000'000ULL);
    }

    ob::SoASide side{};
    fill_side(side, prices, qtys);
    ob::AggregationEngine eng;

    auto res = eng.sum_qty(side, n);
    RC_ASSERT(!res.empty);
    RC_ASSERT(res.value == ref_sum_qty(qtys, n));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 18: AVG aggregation correctness
// Feature: orderbook-dbengine, Property 18: AVG aggregation correctness
// Validates: Requirements 9.2
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_avg, ()) {
    const auto n = *rc::gen::inRange<uint32_t>(1, 101);
    std::vector<int64_t>  prices(n);
    std::vector<uint64_t> qtys(n, 1ULL);
    for (uint32_t i = 0; i < n; ++i) {
        prices[i] = *rc::gen::inRange<int64_t>(1, 1'000'000LL);
    }

    ob::SoASide side{};
    fill_side(side, prices, qtys);
    ob::AggregationEngine eng;

    auto res = eng.avg_price(side, n);
    RC_ASSERT(!res.empty);
    RC_ASSERT(res.value == ref_avg_price(prices, n));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 19: MIN/MAX aggregation correctness
// Feature: orderbook-dbengine, Property 19: MIN/MAX aggregation correctness
// Validates: Requirements 9.3
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_minmax, ()) {
    const auto n = *rc::gen::inRange<uint32_t>(1, 101);
    std::vector<int64_t>  prices(n);
    std::vector<uint64_t> qtys(n, 1ULL);
    for (uint32_t i = 0; i < n; ++i) {
        prices[i] = *rc::gen::inRange<int64_t>(-1'000'000LL, 1'000'001LL);
    }

    ob::SoASide side{};
    fill_side(side, prices, qtys);
    ob::AggregationEngine eng;

    auto mn = eng.min_price(side, n);
    auto mx = eng.max_price(side, n);

    RC_ASSERT(!mn.empty);
    RC_ASSERT(!mx.empty);
    RC_ASSERT(mn.value == ref_min_price(prices, n));
    RC_ASSERT(mx.value == ref_max_price(prices, n));
    RC_ASSERT(mn.value <= mx.value);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 20: VWAP correctness
// Feature: orderbook-dbengine, Property 20: VWAP correctness
// Validates: Requirements 9.4
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_vwap, ()) {
    const auto n = *rc::gen::inRange<uint32_t>(1, 51);
    std::vector<int64_t>  prices(n);
    std::vector<uint64_t> qtys(n);
    for (uint32_t i = 0; i < n; ++i) {
        prices[i] = *rc::gen::inRange<int64_t>(1, 100'000LL);
        qtys[i]   = *rc::gen::inRange<uint64_t>(1, 100'000ULL);
    }

    ob::SoASide side{};
    fill_side(side, prices, qtys);
    ob::AggregationEngine eng;

    auto res = eng.vwap(side, n);
    RC_ASSERT(!res.empty);
    RC_ASSERT(res.value == ref_vwap(prices, qtys, n));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 21: Spread correctness
// Feature: orderbook-dbengine, Property 21: Spread correctness
// Validates: Requirements 9.5
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_spread, ()) {
    const int64_t bid_best = *rc::gen::inRange<int64_t>(1, 100'000LL);
    const int64_t ask_best = *rc::gen::inRange<int64_t>(bid_best, bid_best + 10'000LL);

    ob::SoASide bid{}, ask{};
    bid.prices[0] = bid_best; bid.quantities[0] = 100; bid.depth = 1;
    ask.prices[0] = ask_best; ask.quantities[0] = 100; ask.depth = 1;

    ob::AggregationEngine eng;
    auto res = eng.spread(bid, ask);
    RC_ASSERT(!res.empty);
    RC_ASSERT(res.value == ask_best - bid_best);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 22: Mid-price correctness
// Feature: orderbook-dbengine, Property 22: Mid-price correctness
// Validates: Requirements 9.6
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_mid_price, ()) {
    const int64_t bid_best = *rc::gen::inRange<int64_t>(1, 100'000LL);
    const int64_t ask_best = *rc::gen::inRange<int64_t>(bid_best, bid_best + 10'000LL);

    ob::SoASide bid{}, ask{};
    bid.prices[0] = bid_best; bid.quantities[0] = 100; bid.depth = 1;
    ask.prices[0] = ask_best; ask.quantities[0] = 100; ask.depth = 1;

    ob::AggregationEngine eng;
    auto res = eng.mid_price(bid, ask);
    RC_ASSERT(!res.empty);
    int64_t expected = ((bid_best + ask_best) * 1'000'000LL) / 2LL;
    RC_ASSERT(res.value == expected);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 23: Imbalance correctness
// Feature: orderbook-dbengine, Property 23: Imbalance correctness
// Validates: Requirements 9.7
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_imbalance, ()) {
    const auto n = *rc::gen::inRange<uint32_t>(1, 21);
    std::vector<int64_t>  bid_prices(n), ask_prices(n);
    std::vector<uint64_t> bid_qtys(n), ask_qtys(n);
    for (uint32_t i = 0; i < n; ++i) {
        bid_prices[i] = static_cast<int64_t>(i + 1);
        ask_prices[i] = static_cast<int64_t>(i + 1001);
        bid_qtys[i]   = *rc::gen::inRange<uint64_t>(1, 10'000ULL);
        ask_qtys[i]   = *rc::gen::inRange<uint64_t>(1, 10'000ULL);
    }

    ob::SoASide bid{}, ask{};
    fill_side(bid, bid_prices, bid_qtys);
    fill_side(ask, ask_prices, ask_qtys);
    ob::AggregationEngine eng;

    auto res = eng.imbalance(bid, ask, n);

    int64_t bv = 0, av = 0;
    for (uint32_t i = 0; i < n; ++i) {
        bv += static_cast<int64_t>(bid_qtys[i]);
        av += static_cast<int64_t>(ask_qtys[i]);
    }
    int64_t total = bv + av;
    RC_ASSERT(total > 0);
    RC_ASSERT(!res.empty);

    __int128 num = static_cast<__int128>(bv - av) * 1'000'000'000LL;
    int64_t expected = static_cast<int64_t>(num / total);
    RC_ASSERT(res.value == expected);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 24: Depth-at-price correctness
// Feature: orderbook-dbengine, Property 24: Depth-at-price correctness
// Validates: Requirements 9.8
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_depth_at_price, ()) {
    const auto n = *rc::gen::inRange<uint32_t>(1, 21);
    std::vector<int64_t>  prices(n);
    std::vector<uint64_t> qtys(n);
    for (uint32_t i = 0; i < n; ++i) {
        prices[i] = static_cast<int64_t>(i + 1) * 100LL;
        qtys[i]   = *rc::gen::inRange<uint64_t>(1, 10'000ULL);
    }

    ob::SoASide side{};
    fill_side(side, prices, qtys);
    ob::AggregationEngine eng;

    // Pick a random index to query
    const auto idx = *rc::gen::inRange<uint32_t>(0, n);
    auto res = eng.depth_at_price(side, prices[idx]);
    RC_ASSERT(!res.empty);
    RC_ASSERT(res.value == static_cast<int64_t>(qtys[idx]));

    // Query a price not in the side
    auto res_miss = eng.depth_at_price(side, -999LL);
    RC_ASSERT(!res_miss.empty);
    RC_ASSERT(res_miss.value == 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 25: Depth-within-range correctness
// Feature: orderbook-dbengine, Property 25: Depth-within-range correctness
// Validates: Requirements 9.9
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_depth_within_range, ()) {
    const auto n = *rc::gen::inRange<uint32_t>(1, 21);
    std::vector<int64_t>  prices(n);
    std::vector<uint64_t> qtys(n);
    for (uint32_t i = 0; i < n; ++i) {
        prices[i] = static_cast<int64_t>(i + 1) * 100LL;
        qtys[i]   = *rc::gen::inRange<uint64_t>(1, 10'000ULL);
    }

    ob::SoASide side{};
    fill_side(side, prices, qtys);
    ob::AggregationEngine eng;

    // Random range covering some levels
    const auto lo_idx = *rc::gen::inRange<uint32_t>(0, n);
    const auto hi_idx = *rc::gen::inRange<uint32_t>(lo_idx, n);
    int64_t lo = prices[lo_idx];
    int64_t hi = prices[hi_idx];

    auto res = eng.depth_within_range(side, lo, hi);

    // Reference: sum qtys where price in [lo, hi]
    int64_t expected = 0;
    bool any = false;
    for (uint32_t i = 0; i < n; ++i) {
        if (prices[i] >= lo && prices[i] <= hi) {
            expected += static_cast<int64_t>(qtys[i]);
            any = true;
        }
    }
    RC_ASSERT(res.empty == !any);
    RC_ASSERT(res.value == expected);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 26: Cumulative volume correctness
// Feature: orderbook-dbengine, Property 26: Cumulative volume correctness
// Validates: Requirements 9.10
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_agg_cumulative_volume, ()) {
    const auto n = *rc::gen::inRange<uint32_t>(1, 101);
    std::vector<int64_t>  prices(n);
    std::vector<uint64_t> qtys(n);
    for (uint32_t i = 0; i < n; ++i) {
        prices[i] = static_cast<int64_t>(i + 1);
        qtys[i]   = *rc::gen::inRange<uint64_t>(1, 10'000ULL);
    }

    ob::SoASide side{};
    fill_side(side, prices, qtys);
    ob::AggregationEngine eng;

    const auto k = *rc::gen::inRange<uint32_t>(1, n + 1);
    auto res = eng.cumulative_volume(side, k);
    RC_ASSERT(!res.empty);
    RC_ASSERT(res.value == ref_sum_qty(qtys, std::min(k, n)));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 27: Empty aggregation sentinel
// Feature: orderbook-dbengine, Property 27: Empty aggregation sentinel
// Validates: Requirements 9.11
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(AggProperty, prop_empty_agg_sentinel, ()) {
    ob::SoASide empty_side{};
    empty_side.depth = 0;

    ob::SoASide one_level{};
    one_level.prices[0] = 1000; one_level.quantities[0] = 100; one_level.depth = 1;

    ob::AggregationEngine eng;

    // All single-side aggregations on empty side return empty=true, value=0
    auto r1 = eng.sum_qty(empty_side, 10);
    RC_ASSERT(r1.empty && r1.value == 0);

    auto r2 = eng.avg_price(empty_side, 10);
    RC_ASSERT(r2.empty && r2.value == 0);

    auto r3 = eng.min_price(empty_side, 10);
    RC_ASSERT(r3.empty && r3.value == 0);

    auto r4 = eng.max_price(empty_side, 10);
    RC_ASSERT(r4.empty && r4.value == 0);

    auto r5 = eng.vwap(empty_side, 10);
    RC_ASSERT(r5.empty && r5.value == 0);

    auto r6 = eng.cumulative_volume(empty_side, 10);
    RC_ASSERT(r6.empty && r6.value == 0);

    // N=0 also returns empty
    auto r7 = eng.sum_qty(one_level, 0);
    RC_ASSERT(r7.empty && r7.value == 0);

    auto r8 = eng.avg_price(one_level, 0);
    RC_ASSERT(r8.empty && r8.value == 0);

    // spread/mid_price with empty side
    auto r9 = eng.spread(empty_side, one_level);
    RC_ASSERT(r9.empty && r9.value == 0);

    auto r10 = eng.mid_price(empty_side, one_level);
    RC_ASSERT(r10.empty && r10.value == 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests
// ═══════════════════════════════════════════════════════════════════════════════

// Helper: build a SoASide in-place from an initializer list.
// Returns by pointer to avoid copying non-copyable atomic member.
// We use a static local to keep it simple in tests.
#define MAKE_SIDE(var, ...)                                                 \
    ob::SoASide var{};                                                      \
    do {                                                                    \
        std::initializer_list<std::pair<int64_t, uint64_t>> _lvls = {__VA_ARGS__}; \
        uint32_t _i = 0;                                                    \
        for (auto& [_p, _q] : _lvls) {                                     \
            var.prices[_i] = _p; var.quantities[_i] = _q;                  \
            var.order_counts[_i] = 1; ++_i;                                \
        }                                                                   \
        var.depth = _i;                                                     \
    } while (false)

TEST(AggEngine, SumQtyKnownValues) {
    MAKE_SIDE(side, {100, 10}, {200, 20}, {300, 30});
    ob::AggregationEngine eng;
    auto r = eng.sum_qty(side, 3);
    EXPECT_FALSE(r.empty);
    EXPECT_EQ(r.value, 60);
}

TEST(AggEngine, AvgPriceKnownValues) {
    MAKE_SIDE(side, {100, 1}, {200, 1}, {300, 1});
    ob::AggregationEngine eng;
    auto r = eng.avg_price(side, 3);
    EXPECT_FALSE(r.empty);
    EXPECT_EQ(r.value, 200);  // (100+200+300)/3 = 200
}

TEST(AggEngine, MinMaxKnownValues) {
    MAKE_SIDE(side, {500, 1}, {100, 1}, {300, 1});
    ob::AggregationEngine eng;
    auto mn = eng.min_price(side, 3);
    auto mx = eng.max_price(side, 3);
    EXPECT_FALSE(mn.empty);
    EXPECT_FALSE(mx.empty);
    EXPECT_EQ(mn.value, 100);
    EXPECT_EQ(mx.value, 500);
}

TEST(AggEngine, VwapKnownValues) {
    // prices: 100, 200; qtys: 1, 1
    // VWAP = (100*1 + 200*1) / (1+1) * 10^6 = 150 * 10^6 = 150000000
    MAKE_SIDE(side, {100, 1}, {200, 1});
    ob::AggregationEngine eng;
    auto r = eng.vwap(side, 2);
    EXPECT_FALSE(r.empty);
    EXPECT_EQ(r.value, 150'000'000LL);
}

TEST(AggEngine, VwapZeroQuantity) {
    ob::SoASide side{};
    side.prices[0] = 100; side.quantities[0] = 0; side.depth = 1;
    ob::AggregationEngine eng;
    auto r = eng.vwap(side, 1);
    EXPECT_TRUE(r.empty);
    EXPECT_EQ(r.value, 0);
}

TEST(AggEngine, SpreadKnownValues) {
    ob::SoASide bid{}, ask{};
    bid.prices[0] = 9900;  bid.quantities[0] = 10; bid.depth = 1;
    ask.prices[0] = 10100; ask.quantities[0] = 10; ask.depth = 1;
    ob::AggregationEngine eng;
    auto r = eng.spread(bid, ask);
    EXPECT_FALSE(r.empty);
    EXPECT_EQ(r.value, 200);
}

TEST(AggEngine, MidPriceKnownValues) {
    ob::SoASide bid{}, ask{};
    bid.prices[0] = 9900;  bid.quantities[0] = 10; bid.depth = 1;
    ask.prices[0] = 10100; ask.quantities[0] = 10; ask.depth = 1;
    ob::AggregationEngine eng;
    auto r = eng.mid_price(bid, ask);
    EXPECT_FALSE(r.empty);
    // (9900 + 10100) * 10^6 / 2 = 20000 * 10^6 / 2 = 10000 * 10^6
    EXPECT_EQ(r.value, 10'000'000'000LL);
}

TEST(AggEngine, ImbalanceEqualVolumes) {
    ob::SoASide bid{}, ask{};
    bid.prices[0] = 100; bid.quantities[0] = 500; bid.depth = 1;
    ask.prices[0] = 101; ask.quantities[0] = 500; ask.depth = 1;
    ob::AggregationEngine eng;
    auto r = eng.imbalance(bid, ask, 1);
    EXPECT_FALSE(r.empty);
    EXPECT_EQ(r.value, 0);
}

TEST(AggEngine, DepthAtPriceFound) {
    MAKE_SIDE(side, {100, 50}, {200, 75}, {300, 25});
    ob::AggregationEngine eng;
    auto r = eng.depth_at_price(side, 200);
    EXPECT_FALSE(r.empty);
    EXPECT_EQ(r.value, 75);
}

TEST(AggEngine, DepthAtPriceNotFound) {
    MAKE_SIDE(side, {100, 50}, {200, 75});
    ob::AggregationEngine eng;
    auto r = eng.depth_at_price(side, 999);
    EXPECT_FALSE(r.empty);  // empty=false always for depth_at_price
    EXPECT_EQ(r.value, 0);
}

TEST(AggEngine, DepthWithinRange) {
    MAKE_SIDE(side, {100, 10}, {200, 20}, {300, 30}, {400, 40});
    ob::AggregationEngine eng;
    // Range [150, 350] covers prices 200 and 300
    auto r = eng.depth_within_range(side, 150, 350);
    EXPECT_FALSE(r.empty);
    EXPECT_EQ(r.value, 50);  // 20 + 30
}

TEST(AggEngine, CumulativeVolume) {
    MAKE_SIDE(side, {100, 10}, {200, 20}, {300, 30});
    ob::AggregationEngine eng;
    auto r = eng.cumulative_volume(side, 2);
    EXPECT_FALSE(r.empty);
    EXPECT_EQ(r.value, 30);  // 10 + 20
}

TEST(AggEngine, N1Boundary) {
    MAKE_SIDE(side, {500, 99});
    ob::AggregationEngine eng;

    EXPECT_EQ(eng.sum_qty(side, 1).value, 99);
    EXPECT_EQ(eng.avg_price(side, 1).value, 500);
    EXPECT_EQ(eng.min_price(side, 1).value, 500);
    EXPECT_EQ(eng.max_price(side, 1).value, 500);
    EXPECT_EQ(eng.vwap(side, 1).value, 500'000'000LL);
    EXPECT_EQ(eng.cumulative_volume(side, 1).value, 99);
}

TEST(AggEngine, N1000Boundary) {
    ob::SoASide side{};
    side.depth = 0;
    for (uint32_t i = 0; i < 1000; ++i) {
        side.prices[i]       = static_cast<int64_t>(i + 1);
        side.quantities[i]   = 1;
        side.order_counts[i] = 1;
        ++side.depth;
    }
    ob::AggregationEngine eng;

    auto r_sum = eng.sum_qty(side, 1000);
    EXPECT_FALSE(r_sum.empty);
    EXPECT_EQ(r_sum.value, 1000);

    auto r_min = eng.min_price(side, 1000);
    EXPECT_EQ(r_min.value, 1);

    auto r_max = eng.max_price(side, 1000);
    EXPECT_EQ(r_max.value, 1000);

    auto r_cum = eng.cumulative_volume(side, 1000);
    EXPECT_EQ(r_cum.value, 1000);
}
