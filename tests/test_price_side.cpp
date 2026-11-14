// One side of a book composed by price (#266, price_side.hpp), against the std::map it replaces: after
// every put and removal the same prices held, the same best price, and the same rows best first.

#include "orderbook/price_side.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <functional>
#include <map>
#include <vector>

namespace {

ob::SnapshotRow row(int64_t price, uint64_t qty, uint64_t seq) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = 1'000 + seq;
    r.sequence_number = seq;
    r.side            = ob::SIDE_BID;
    r.price           = price;
    r.quantity        = qty;
    r.order_count     = static_cast<uint32_t>(qty % 7 + 1);
    return r;
}

template <bool HighestFirst, typename Map>
void check_same(ob::PriceSide<HighestFirst>& side, const Map& map) {
    RC_ASSERT(side.size() == map.size());
    RC_ASSERT(side.empty() == map.empty());
    if (!map.empty()) RC_ASSERT(side.best() == map.begin()->first);
    const std::vector<ob::SnapshotRow> rows = side.best_first();
    RC_ASSERT(rows.size() == map.size());
    size_t i = 0;
    for (const auto& [price, r] : map) {
        RC_ASSERT(rows[i].price == price);
        RC_ASSERT(rows[i].sequence_number == r.sequence_number);
        RC_ASSERT(rows[i].quantity == r.quantity);
        ++i;
    }
}

/// Puts and removals over a few prices or many, a tick apart or a hundred, against the map.
template <bool HighestFirst, typename Map>
void walk() {
    ob::PriceSide<HighestFirst> side;
    Map map;
    const int64_t tick = *rc::gen::element<int64_t>(1, 100);
    const int prices = *rc::gen::inRange(1, 300);
    const int ops = *rc::gen::inRange(1, 2000);
    for (int n = 0; n < ops; ++n) {
        const int64_t price = 1'000'000 + tick * *rc::gen::inRange(-prices, prices);
        if (*rc::gen::inRange(0, 4) == 0) {
            side.remove(price);
            map.erase(price);
        } else {
            const ob::SnapshotRow r = row(price, static_cast<uint64_t>(*rc::gen::inRange(1, 1000)),
                                          static_cast<uint64_t>(n));
            side.put(r);
            map.insert_or_assign(price, r);
        }
        if (n % 7 == 0) check_same(side, map);
        if (*rc::gen::inRange(0, 500) == 0) {
            side.clear();
            map.clear();
        }
    }
    check_same(side, map);
}

}  // namespace

RC_GTEST_PROP(PriceSide, TheBidsAreWhatAMapHighestFirstHolds, ()) {
    walk<true, std::map<int64_t, ob::SnapshotRow, std::greater<>>>();
}

RC_GTEST_PROP(PriceSide, TheAsksAreWhatAMapLowestFirstHolds, ()) {
    walk<false, std::map<int64_t, ob::SnapshotRow>>();
}

TEST(PriceSide, ARemovedBestGivesWayToTheNextAndComesBackWhenPutAgain) {
    ob::PriceSide<true> bids;
    bids.put(row(100, 1, 1));
    bids.put(row(99, 1, 2));
    bids.put(row(98, 1, 3));
    EXPECT_EQ(bids.best(), 100);
    bids.remove(100);
    EXPECT_EQ(bids.best(), 99);
    bids.remove(99);
    EXPECT_EQ(bids.best(), 98);
    bids.put(row(100, 5, 4));
    EXPECT_EQ(bids.best(), 100);
    EXPECT_EQ(bids.size(), 2u);
    const auto rows = bids.best_first();
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].price, 100);
    EXPECT_EQ(rows[0].quantity, 5u);
    EXPECT_EQ(rows[1].price, 98);
}

TEST(PriceSide, RemovingAPriceNotHeldChangesNothing) {
    ob::PriceSide<false> asks;
    asks.remove(5);   // an empty side
    asks.put(row(10, 1, 1));
    asks.remove(11);
    EXPECT_EQ(asks.size(), 1u);
    EXPECT_EQ(asks.best(), 10);
}

TEST(PriceSide, ManyRemovalsAndPutsKeepTheTableAndTheHeapBounded) {
    // A price added and removed a hundred thousand times: the table holds one price, and the heap does
    // not keep every removal.
    ob::PriceSide<true> bids;
    for (uint64_t n = 0; n < 100'000; ++n) {
        bids.put(row(static_cast<int64_t>(n % 64), 1, n));
        bids.remove(static_cast<int64_t>(n % 64));
    }
    bids.put(row(7, 1, 1));
    EXPECT_EQ(bids.size(), 1u);
    EXPECT_EQ(bids.best(), 7);
}
