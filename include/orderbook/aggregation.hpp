#pragma once

#include <cstdint>

#include "orderbook/soa_buffer.hpp"

namespace ob {

// Fixed-point scale: all prices in sub-unit integers.
// VWAP and mid_price return values scaled by 10^6.
// Imbalance returns value scaled by 10^9.

struct AggResult {
    int64_t value;  // primary result
    bool    empty;  // true when input set was empty
};

class AggregationEngine {
public:
    // sum_qty: sum of quantities[0..min(n,depth)-1]; empty if n==0 or depth==0
    AggResult sum_qty(const SoASide& side, uint32_t n_levels) const;

    // avg_price: sum(prices[0..n-1]) / n; empty if n==0 or depth==0
    AggResult avg_price(const SoASide& side, uint32_t n_levels) const;

    // min_price: minimum of prices[0..n-1]; empty if n==0 or depth==0
    AggResult min_price(const SoASide& side, uint32_t n_levels) const;

    // max_price: maximum of prices[0..n-1]; empty if n==0 or depth==0
    AggResult max_price(const SoASide& side, uint32_t n_levels) const;

    // vwap: SUM(prices[i]*quantities[i]) / SUM(quantities[i]) scaled by 10^6;
    //       empty if n==0 or depth==0 or total_qty==0
    AggResult vwap(const SoASide& side, uint32_t n_levels) const;

    // spread: ask.prices[0] - bid.prices[0]; empty if either side has depth==0
    AggResult spread(const SoASide& bid, const SoASide& ask) const;

    // mid_price: (ask.prices[0] + bid.prices[0]) / 2 scaled by 10^6;
    //            empty if either side has depth==0
    AggResult mid_price(const SoASide& bid, const SoASide& ask) const;

    // imbalance: (bid_vol - ask_vol) * 10^9 / (bid_vol + ask_vol);
    //            empty if bid_vol + ask_vol == 0
    AggResult imbalance(const SoASide& bid, const SoASide& ask, uint32_t n_levels) const;

    // depth_at_price: quantity at exact price level, or 0 if not found; empty=false always
    AggResult depth_at_price(const SoASide& side, int64_t price) const;

    // depth_within_range: sum of quantities for levels with price in [lo, hi];
    //                     empty if no levels in range
    AggResult depth_within_range(const SoASide& side, int64_t lo, int64_t hi) const;

    // cumulative_volume: sum of quantities[0..min(n,depth)-1]; empty if n==0 or depth==0
    AggResult cumulative_volume(const SoASide& side, uint32_t n_levels) const;
};

} // namespace ob
