// A book side's levels as insert_level() keeps them, against the insert_level() before #268, after every
// update.
//
// #268 replaced the two linear searches of insert_level() - for the price, then for where a new one goes -
// with one search by halves. A side holds its prices in its order with no price twice, so the slot the
// search finds is the one the first search found when the price is held, and the one the second found
// when it is not. This holds it to that: any sequence of updates to either side - prices that collide,
// prices that fill the side past MAX_LEVELS and push its worst out, removals - leaves the side's prices,
// quantities, order counts and depth as the function before it left them, update by update.
#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "orderbook/data_model.hpp"
#include "orderbook/soa_buffer.hpp"
#include "orderbook/types.hpp"

namespace {

// insert_level() as it was at 0ed158b, before #268 - copied, so that a difference is from what was
// shipped.
ob::ob_status_t reference_insert_level(ob::SoASide& side, int64_t price, uint64_t qty, uint32_t cnt,
                                       bool descending) {
    const uint32_t depth = side.depth;

    // Search for an existing level at this price.
    uint32_t found_idx = depth; // sentinel: not found
    for (uint32_t i = 0; i < depth; ++i) {
        if (side.prices[i] == price) {
            found_idx = i;
            break;
        }
    }

    // ── Remove (qty == 0) ──────────────────────────────────────────────────
    if (qty == 0) {
        if (found_idx == depth) {
            return ob::OB_OK; // nothing to remove
        }
        // Shift elements left to fill the gap.
        const uint32_t tail = depth - found_idx - 1;
        if (tail > 0) {
            std::memmove(&side.prices[found_idx],
                         &side.prices[found_idx + 1],
                         tail * sizeof(int64_t));
            std::memmove(&side.quantities[found_idx],
                         &side.quantities[found_idx + 1],
                         tail * sizeof(uint64_t));
            std::memmove(&side.order_counts[found_idx],
                         &side.order_counts[found_idx + 1],
                         tail * sizeof(uint32_t));
        }
        side.depth = depth - 1;
        return ob::OB_OK;
    }

    // ── Update existing level ──────────────────────────────────────────────
    if (found_idx < depth) {
        side.quantities[found_idx]   = qty;
        side.order_counts[found_idx] = cnt;
        // Price unchanged; sort order is preserved.
        return ob::OB_OK;
    }

    // ── Insert new level ───────────────────────────────────────────────────
    if (depth >= ob::SoASide::MAX_LEVELS) {
        const int64_t worst = side.prices[depth - 1];
        const bool new_is_worse = descending
            ? (price <= worst)   // new bid price ≤ lowest existing bid
            : (price >= worst);  // new ask price ≥ highest existing ask
        if (new_is_worse) {
            return ob::OB_OK; // silently discard — not relevant for top of book
        }
        // Evict the worst level (last element) to make room.
        side.depth = depth - 1;
        // Fall through to the normal sorted-insert path below.
    }

    // Find insertion position to maintain sort order.
    const uint32_t cur_depth = side.depth; // re-read after possible eviction
    uint32_t insert_pos = cur_depth;
    for (uint32_t i = 0; i < cur_depth; ++i) {
        bool should_insert_before = descending
            ? (price > side.prices[i])
            : (price < side.prices[i]);
        if (should_insert_before) {
            insert_pos = i;
            break;
        }
    }

    // Shift elements right to make room.
    const uint32_t tail = cur_depth - insert_pos;
    if (tail > 0) {
        std::memmove(&side.prices[insert_pos + 1],
                     &side.prices[insert_pos],
                     tail * sizeof(int64_t));
        std::memmove(&side.quantities[insert_pos + 1],
                     &side.quantities[insert_pos],
                     tail * sizeof(uint64_t));
        std::memmove(&side.order_counts[insert_pos + 1],
                     &side.order_counts[insert_pos],
                     tail * sizeof(uint32_t));
    }

    side.prices[insert_pos]       = price;
    side.quantities[insert_pos]   = qty;
    side.order_counts[insert_pos] = cnt;
    side.depth = cur_depth + 1;
    return ob::OB_OK;
}

std::unique_ptr<ob::SoABuffer> make_buffer() {
    auto buf = std::make_unique<ob::SoABuffer>();
    std::strncpy(buf->symbol, "TEST", sizeof(buf->symbol) - 1);
    std::strncpy(buf->exchange, "EX", sizeof(buf->exchange) - 1);
    buf->sequence_number.store(0, std::memory_order_relaxed);
    buf->bid.version.store(0, std::memory_order_relaxed);
    buf->ask.version.store(0, std::memory_order_relaxed);
    buf->bid.depth = 0;
    buf->ask.depth = 0;
    return buf;
}

/// The first place two sides differ, or nothing.
std::string difference(const ob::SoASide& ours, const ob::SoASide& theirs) {
    if (ours.depth != theirs.depth) {
        return "depth " + std::to_string(ours.depth) + " against the reference's " + std::to_string(theirs.depth);
    }
    for (uint32_t i = 0; i < ours.depth; ++i) {
        if (ours.prices[i] != theirs.prices[i] || ours.quantities[i] != theirs.quantities[i] ||
            ours.order_counts[i] != theirs.order_counts[i]) {
            return "level " + std::to_string(i) + ": " + std::to_string(ours.prices[i]) + " x " +
                   std::to_string(ours.quantities[i]) + " against the reference's " +
                   std::to_string(theirs.prices[i]) + " x " + std::to_string(theirs.quantities[i]);
        }
    }
    return {};
}

struct Step {
    uint8_t side;
    std::vector<ob::Level> levels;
};

/// An update of one side: one to twenty levels, their prices from a band that is narrow - prices held
/// again and again, changed and removed - or wide enough to fill a side past MAX_LEVELS, a removal
/// one level in `remove_in` (a zero quantity).
rc::Gen<Step> step(int64_t band, unsigned remove_in) {
    return rc::gen::exec([band, remove_in] {
        Step s;
        s.side = *rc::gen::inRange<int>(0, 2) == 0 ? ob::SIDE_BID : ob::SIDE_ASK;
        const auto n = *rc::gen::inRange<size_t>(1, 21);
        for (size_t k = 0; k < n; ++k) {
            ob::Level l{};
            l.price = 1'000'000 + *rc::gen::inRange<int64_t>(-band, band + 1) * 100;
            l.qty = *rc::gen::inRange<unsigned>(0, remove_in) == 0 ? 0 : *rc::gen::inRange<uint64_t>(1, 1'000'000);
            l.cnt = *rc::gen::inRange<uint32_t>(0, 50);
            s.levels.push_back(l);
        }
        return s;
    });
}

}  // namespace

RC_GTEST_PROP(BookLevelSearch, EverySideIsTheReferencesAfterEveryUpdate, ()) {
    // At full size whatever the case's: RapidCheck scales `inRange` with it, and a band drawn small
    // never fills a side.
    const auto band = *rc::gen::resize(100, rc::gen::element<int64_t>(int64_t{3}, int64_t{40}, int64_t{400}, int64_t{3000}));
    const auto remove_in = *rc::gen::resize(100, rc::gen::element<unsigned>(2u, 5u, 50u));
    const auto updates = *rc::gen::resize(100, rc::gen::inRange<size_t>(1, 600));
    RC_CLASSIFY(band >= 400 && updates > 200, "a side can fill past MAX_LEVELS");

    auto buf = make_buffer();
    auto reference = make_buffer();
    bool reached_max = false;
    for (size_t u = 0; u < updates; ++u) {
        const Step s = *rc::gen::resize(100, step(band, remove_in));
        ob::DeltaUpdate d{};
        std::strncpy(d.symbol, "TEST", sizeof(d.symbol) - 1);
        std::strncpy(d.exchange, "EX", sizeof(d.exchange) - 1);
        d.side = s.side;
        d.sequence_number = u + 1;
        d.timestamp_ns = (u + 1) * 1000;
        d.n_levels = static_cast<uint16_t>(s.levels.size());
        bool gap = false;
        RC_ASSERT(ob::apply_delta(*buf, d, s.levels.data(), gap) == ob::OB_OK);

        ob::SoASide& theirs = s.side == ob::SIDE_BID ? reference->bid : reference->ask;
        for (const ob::Level& l : s.levels) {
            reference_insert_level(theirs, l.price, l.qty, l.cnt, s.side == ob::SIDE_BID);
        }
        const ob::SoASide& ours = s.side == ob::SIDE_BID ? buf->bid : buf->ask;
        const std::string why = difference(ours, theirs);
        RC_LOG() << "update " << u << ", side " << int{s.side} << ": " << why << "\n";
        RC_ASSERT(why.empty());
        reached_max = reached_max || ours.depth == ob::SoASide::MAX_LEVELS;
    }
    RC_CLASSIFY(reached_max, "a side reached MAX_LEVELS");
}

// A side filled to MAX_LEVELS and then given prices above, below and between its own, on both sides:
// the evictions and the refusals of a full side, which a random walk reaches only now and then.
TEST(BookLevelSearch, AFullSideTakesAndRefusesAsTheReferenceDoes) {
    for (const uint8_t side_id : {ob::SIDE_BID, ob::SIDE_ASK}) {
        auto buf = make_buffer();
        auto reference = make_buffer();
        ob::SoASide& ours = side_id == ob::SIDE_BID ? buf->bid : buf->ask;
        ob::SoASide& theirs = side_id == ob::SIDE_BID ? reference->bid : reference->ask;
        const bool descending = side_id == ob::SIDE_BID;
        uint64_t seq = 1;
        const auto apply = [&](int64_t price, uint64_t qty) {
            ob::Level l{price, qty, 1, 0};
            ob::DeltaUpdate d{};
            std::strncpy(d.symbol, "TEST", sizeof(d.symbol) - 1);
            d.side = side_id;
            d.sequence_number = seq++;
            d.n_levels = 1;
            bool gap = false;
            ASSERT_EQ(ob::apply_delta(*buf, d, &l, gap), ob::OB_OK);
            reference_insert_level(theirs, price, qty, 1, descending);
            ASSERT_EQ(difference(ours, theirs), "") << "price " << price << " x " << qty;
        };
        for (int64_t p = 0; p < static_cast<int64_t>(ob::SoASide::MAX_LEVELS); ++p) apply(10'000 + p * 10, 5);
        ASSERT_EQ(ours.depth, ob::SoASide::MAX_LEVELS);
        // Below and above every price, on a price held and between two held, the edges of the band.
        for (const int64_t p : {int64_t{1}, int64_t{9'990}, int64_t{10'000}, int64_t{10'005}, int64_t{15'000},
                                int64_t{19'990}, int64_t{19'995}, int64_t{20'000}, int64_t{30'000}}) {
            apply(p, 7);
        }
        // Removals at the front, the back and the middle, then the side refilled past its limit.
        for (const int64_t p : {int64_t{10'000}, int64_t{19'990}, int64_t{15'000}, int64_t{12'345}}) apply(p, 0);
        for (int64_t p = 0; p < 50; ++p) apply(5'000 + p * 300, 3);
    }
}

// remove_level() takes a price out of either side, wherever it is: it has no caller in the engine, and
// it named the bids' order for every side, which a search by halves would have taken at its word.
TEST(BookLevelSearch, RemoveLevelTakesAPriceOutOfEitherSide) {
    for (const uint8_t side_id : {ob::SIDE_BID, ob::SIDE_ASK}) {
        auto buf = make_buffer();
        ob::SoASide& side = side_id == ob::SIDE_BID ? buf->bid : buf->ask;
        for (int64_t p = 1; p <= 9; ++p) {
            ob::Level l{p * 100, 10, 1, 0};
            ob::DeltaUpdate d{};
            d.side = side_id;
            d.sequence_number = static_cast<uint64_t>(p);
            d.n_levels = 1;
            bool gap = false;
            ASSERT_EQ(ob::apply_delta(*buf, d, &l, gap), ob::OB_OK);
        }
        for (const int64_t p : {int64_t{300}, int64_t{100}, int64_t{900}, int64_t{555}}) {
            ASSERT_EQ(ob::remove_level(side, p), ob::OB_OK);
        }
        ASSERT_EQ(side.depth, 6u) << "side " << int{side_id};
        for (uint32_t i = 0; i < side.depth; ++i) {
            EXPECT_NE(side.prices[i], 300);
            EXPECT_NE(side.prices[i], 100);
            EXPECT_NE(side.prices[i], 900);
            if (i > 0) {
                EXPECT_TRUE(side_id == ob::SIDE_BID ? side.prices[i - 1] > side.prices[i]
                                                    : side.prices[i - 1] < side.prices[i]);
            }
        }
    }
}

// install_book() - a start's rebuild of a live book from history - is the one other writer of a side.
// A book composed by price is in each side's order with each price once, and is put in as it is; levels
// that are not are put in one by one, as updates would put them, so that the side keeps the order the
// search by halves relies on, and the caller is told.
TEST(BookLevelSearch, InstallBookKeepsASidesOrderWhateverItIsGiven) {
    const auto side_of = [](const std::vector<ob::Level>& levels, bool descending) {
        auto buf = make_buffer();
        ob::SoASide& side = descending ? buf->bid : buf->ask;
        for (const ob::Level& l : levels) reference_insert_level(side, l.price, l.qty, l.cnt, descending);
        return buf;
    };
    const std::vector<ob::Level> bids_ordered = {{300, 1, 1, 0}, {200, 2, 1, 0}, {100, 3, 1, 0}};
    const std::vector<ob::Level> asks_ordered = {{400, 4, 1, 0}, {500, 5, 1, 0}, {600, 6, 1, 0}};
    {
        auto buf = make_buffer();
        EXPECT_TRUE(ob::install_book(*buf, bids_ordered.data(), bids_ordered.size(), asks_ordered.data(),
                                     asks_ordered.size(), 7, 70));
        EXPECT_EQ(difference(buf->bid, side_of(bids_ordered, true)->bid), "");
        EXPECT_EQ(difference(buf->ask, side_of(asks_ordered, false)->ask), "");
        EXPECT_EQ(buf->sequence_number.load(), 7u);
    }
    // Out of order, a price twice, and on each side the other side's order.
    const std::vector<ob::Level> bids_mixed = {{100, 1, 1, 0}, {300, 2, 1, 0}, {200, 3, 1, 0}, {300, 9, 2, 0}};
    const std::vector<ob::Level> asks_mixed = {{600, 4, 1, 0}, {500, 5, 1, 0}, {400, 6, 1, 0}};
    {
        auto buf = make_buffer();
        EXPECT_FALSE(ob::install_book(*buf, bids_mixed.data(), bids_mixed.size(), asks_mixed.data(),
                                      asks_mixed.size(), 8, 80));
        EXPECT_EQ(difference(buf->bid, side_of(bids_mixed, true)->bid), "");
        EXPECT_EQ(difference(buf->ask, side_of(asks_mixed, false)->ask), "");
        EXPECT_EQ(buf->bid.depth, 3u);
        // And a price held is found by an update afterwards, not taken a second time.
        ob::Level again{300, 11, 1, 0};
        ob::DeltaUpdate d{};
        d.side = ob::SIDE_BID;
        d.sequence_number = 9;
        d.n_levels = 1;
        bool gap = false;
        ASSERT_EQ(ob::apply_delta(*buf, d, &again, gap), ob::OB_OK);
        EXPECT_EQ(buf->bid.depth, 3u);
        EXPECT_EQ(buf->bid.prices[0], 300);
        EXPECT_EQ(buf->bid.quantities[0], 11u);
    }
}
