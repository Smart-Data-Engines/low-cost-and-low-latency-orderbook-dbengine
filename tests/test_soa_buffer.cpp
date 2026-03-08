// Tests for SoA Buffer: property-based tests (Properties 1-6) and unit tests.
// Feature: orderbook-dbengine

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "orderbook/data_model.hpp"
#include "orderbook/soa_buffer.hpp"
#include "orderbook/types.hpp"

// Helpers

static std::unique_ptr<ob::SoABuffer> make_buffer(const char* symbol = "TEST",
                                                   const char* exchange = "EX") {
    auto buf = std::make_unique<ob::SoABuffer>();

    std::strncpy(buf->symbol,   symbol,   sizeof(buf->symbol)   - 1);
    std::strncpy(buf->exchange, exchange, sizeof(buf->exchange) - 1);
    buf->sequence_number.store(0, std::memory_order_relaxed);
    buf->bid.version.store(0, std::memory_order_relaxed);
    buf->ask.version.store(0, std::memory_order_relaxed);
    buf->bid.depth = 0;
    buf->ask.depth = 0;
    return buf;
}

static ob::DeltaUpdate make_update(uint8_t side, uint64_t seq,
                                   uint16_t n_levels = 0) {
    ob::DeltaUpdate upd{};
    std::strncpy(upd.symbol,   "TEST", sizeof(upd.symbol)   - 1);
    std::strncpy(upd.exchange, "EX",   sizeof(upd.exchange) - 1);
    upd.side            = side;
    upd.sequence_number = seq;
    upd.timestamp_ns    = seq * 1000ULL;
    upd.n_levels        = n_levels;
    return upd;
}

// Property 1: Orderbook data round-trip
// Feature: orderbook-dbengine, Property 1: Orderbook data round-trip
// Validates: Requirements 1.3, 1.6, 1.7, 1.8
RC_GTEST_PROP(SoABufferProperty, prop_data_round_trip, ()) {
    const auto n = *rc::gen::inRange<uint16_t>(1, 21);

    std::vector<int64_t> prices;
    prices.reserve(n);
    auto price_gen = rc::gen::inRange<int64_t>(1, 1000000LL);
    while (static_cast<uint16_t>(prices.size()) < n) {
        int64_t p = *price_gen;
        if (std::find(prices.begin(), prices.end(), p) == prices.end()) {
            prices.push_back(p);
        }
    }

    std::vector<ob::Level> levels(n);
    for (uint16_t i = 0; i < n; ++i) {
        levels[i].price = prices[i];
        levels[i].qty   = *rc::gen::inRange<uint64_t>(1, 1000000ULL);
        levels[i].cnt   = *rc::gen::inRange<uint32_t>(1, 1000U);
    }

    auto buf = make_buffer();
    ob::DeltaUpdate upd = make_update(ob::SIDE_BID, 1, n);
    bool gap = false;
    ob::ob_status_t st = ob::apply_delta(*buf, upd, levels.data(), gap);
    RC_ASSERT(st == ob::OB_OK);

    ob::SoASide out_bid{}, out_ask{};
    ob::read_snapshot(*buf, out_bid, out_ask);

    RC_ASSERT(out_bid.depth == static_cast<uint32_t>(n));

    for (uint16_t i = 0; i < n; ++i) {
        bool found = false;
        for (uint32_t j = 0; j < out_bid.depth; ++j) {
            if (out_bid.prices[j] == levels[i].price) {
                RC_ASSERT(out_bid.quantities[j]   == levels[i].qty);
                RC_ASSERT(out_bid.order_counts[j] == levels[i].cnt);
                found = true;
                break;
            }
        }
        RC_ASSERT(found);
    }
}

// Property 2: Sequence number gap detection
// Feature: orderbook-dbengine, Property 2: Sequence number gap detection
// Validates: Requirements 1.5
RC_GTEST_PROP(SoABufferProperty, prop_gap_detection, ()) {
    const auto seq1 = *rc::gen::inRange<uint64_t>(1, 1000ULL);
    const auto gap_offset = *rc::gen::inRange<uint64_t>(2, 100ULL);
    const auto seq2_gap = seq1 + gap_offset;

    ob::Level lv{100LL, 10ULL, 1U, 0U};

    // Non-consecutive: gap_detected must be true
    {
        auto buf = make_buffer();
        ob::DeltaUpdate upd1 = make_update(ob::SIDE_BID, seq1, 1);
        bool gap = false;
        ob::apply_delta(*buf, upd1, &lv, gap);

        ob::Level lv2{200LL, 5ULL, 1U, 0U};
        ob::DeltaUpdate upd2 = make_update(ob::SIDE_BID, seq2_gap, 1);
        bool gap2 = false;
        ob::apply_delta(*buf, upd2, &lv2, gap2);
        RC_ASSERT(gap2 == true);
    }

    // Consecutive: gap_detected must be false
    {
        auto buf = make_buffer();
        ob::DeltaUpdate upd1 = make_update(ob::SIDE_BID, seq1, 1);
        bool gap = false;
        ob::apply_delta(*buf, upd1, &lv, gap);

        ob::Level lv2{200LL, 5ULL, 1U, 0U};
        ob::DeltaUpdate upd2 = make_update(ob::SIDE_BID, seq1 + 1, 1);
        bool gap2 = false;
        ob::apply_delta(*buf, upd2, &lv2, gap2);
        RC_ASSERT(gap2 == false);
    }
}

// Property 3: Bid/ask sort invariant
// Feature: orderbook-dbengine, Property 3: Bid/ask sort invariant
// Validates: Requirements 2.5
RC_GTEST_PROP(SoABufferProperty, prop_sort_invariant, ()) {
    const auto n_updates = *rc::gen::inRange<int>(1, 11);

    auto buf = make_buffer();
    uint64_t seq = 1;

    for (int u = 0; u < n_updates; ++u) {
        const auto n_levels = *rc::gen::inRange<uint16_t>(1, 6);
        std::vector<ob::Level> levels(n_levels);
        for (uint16_t i = 0; i < n_levels; ++i) {
            levels[i].price = *rc::gen::inRange<int64_t>(1, 10000LL);
            levels[i].qty   = *rc::gen::inRange<uint64_t>(1, 1000ULL);
            levels[i].cnt   = 1U;
        }

        uint8_t side = (u % 2 == 0) ? ob::SIDE_BID : ob::SIDE_ASK;
        ob::DeltaUpdate upd = make_update(side, seq++, n_levels);
        bool gap = false;
        ob::apply_delta(*buf, upd, levels.data(), gap);
    }

    ob::SoASide out_bid{}, out_ask{};
    ob::read_snapshot(*buf, out_bid, out_ask);

    for (uint32_t i = 1; i < out_bid.depth; ++i) {
        RC_ASSERT(out_bid.prices[i - 1] >= out_bid.prices[i]);
    }

    for (uint32_t i = 1; i < out_ask.depth; ++i) {
        RC_ASSERT(out_ask.prices[i - 1] <= out_ask.prices[i]);
    }
}

// Property 4: Maximum depth invariant
// Feature: orderbook-dbengine, Property 4: Maximum depth invariant
// Validates: Requirements 1.2
RC_GTEST_PROP(SoABufferProperty, prop_max_depth_invariant, ()) {
    const auto n_updates = *rc::gen::inRange<int>(1, 50);

    auto buf = make_buffer();
    uint64_t seq = 1;

    for (int u = 0; u < n_updates; ++u) {
        const auto n_levels = *rc::gen::inRange<uint16_t>(1, 21);
        std::vector<ob::Level> levels(n_levels);
        for (uint16_t i = 0; i < n_levels; ++i) {
            levels[i].price = *rc::gen::inRange<int64_t>(1, 2000LL);
            levels[i].qty   = *rc::gen::inRange<uint64_t>(1, 100ULL);
            levels[i].cnt   = 1U;
        }

        ob::DeltaUpdate upd = make_update(ob::SIDE_BID, seq++, n_levels);
        bool gap = false;
        ob::apply_delta(*buf, upd, levels.data(), gap);

        RC_ASSERT(buf->bid.depth <= ob::SoASide::MAX_LEVELS);
        RC_ASSERT(buf->ask.depth <= ob::SoASide::MAX_LEVELS);
    }
}

// Property 5: Seqlock read consistency
// Feature: orderbook-dbengine, Property 5: Seqlock read consistency
// Validates: Requirements 3.2, 3.5
RC_GTEST_PROP(SoABufferProperty, prop_seqlock_consistency, ()) {
    const auto n_writes = *rc::gen::inRange<int>(10, 50);

    auto buf = make_buffer();

    {
        ob::Level lv{1000LL, 100ULL, 1U, 0U};
        ob::DeltaUpdate upd = make_update(ob::SIDE_BID, 1, 1);
        bool gap = false;
        ob::apply_delta(*buf, upd, &lv, gap);
    }

    std::atomic<bool> stop{false};
    std::atomic<int>  torn_reads{0};

    std::thread writer([&, n_writes]() {
        uint64_t seq = 2;
        for (int i = 0; i < n_writes; ++i) {
            ob::Level lv{1000LL,
                         static_cast<uint64_t>(seq) * 100ULL,
                         static_cast<uint32_t>(seq),
                         0U};
            ob::DeltaUpdate upd = make_update(ob::SIDE_BID, seq, 1);
            bool gap = false;
            ob::apply_delta(*buf, upd, &lv, gap);
            ++seq;
        }
        stop.store(true, std::memory_order_release);
    });

    std::thread reader([&]() {
        while (!stop.load(std::memory_order_acquire)) {
            ob::SoASide out_bid{}, out_ask{};
            ob::read_snapshot(*buf, out_bid, out_ask);

            for (uint32_t j = 0; j < out_bid.depth; ++j) {
                if (out_bid.prices[j] == 1000LL) {
                    uint64_t qty = out_bid.quantities[j];
                    uint32_t cnt = out_bid.order_counts[j];
                    if (cnt > 0 && qty != static_cast<uint64_t>(cnt) * 100ULL) {
                        torn_reads.fetch_add(1, std::memory_order_relaxed);
                    }
                    break;
                }
            }
        }
    });

    writer.join();
    reader.join();

    RC_ASSERT(torn_reads.load() == 0);
}

// Property 6: Concurrent update convergence
// Feature: orderbook-dbengine, Property 6: Concurrent update convergence
// Validates: Requirements 3.4
RC_GTEST_PROP(SoABufferProperty, prop_concurrent_convergence, ()) {
    const auto n_updates = *rc::gen::inRange<int>(5, 30);

    auto buf = make_buffer();

    for (int i = 1; i <= n_updates; ++i) {
        ob::Level lv{static_cast<int64_t>(i) * 10LL,
                     static_cast<uint64_t>(i),
                     static_cast<uint32_t>(i),
                     0U};
        ob::DeltaUpdate upd = make_update(ob::SIDE_BID, static_cast<uint64_t>(i), 1);
        bool gap = false;
        ob::apply_delta(*buf, upd, &lv, gap);
    }

    RC_ASSERT(buf->sequence_number.load(std::memory_order_relaxed) ==
              static_cast<uint64_t>(n_updates));
}

// Unit tests

TEST(SoABuffer, SingleLevelInsertBid) {
    auto buf = make_buffer();
    ob::Level lv{10000LL, 500ULL, 3U, 0U};
    ob::DeltaUpdate upd = make_update(ob::SIDE_BID, 1, 1);
    bool gap = false;
    ASSERT_EQ(ob::apply_delta(*buf, upd, &lv, gap), ob::OB_OK);

    ob::SoASide out_bid{}, out_ask{};
    ob::read_snapshot(*buf, out_bid, out_ask);

    EXPECT_EQ(out_bid.depth, 1u);
    EXPECT_EQ(out_bid.prices[0],       10000LL);
    EXPECT_EQ(out_bid.quantities[0],   500ULL);
    EXPECT_EQ(out_bid.order_counts[0], 3U);
    EXPECT_EQ(out_ask.depth, 0u);
}

TEST(SoABuffer, SingleLevelInsertAsk) {
    auto buf = make_buffer();
    ob::Level lv{20000LL, 200ULL, 1U, 0U};
    ob::DeltaUpdate upd = make_update(ob::SIDE_ASK, 1, 1);
    bool gap = false;
    ASSERT_EQ(ob::apply_delta(*buf, upd, &lv, gap), ob::OB_OK);

    ob::SoASide out_bid{}, out_ask{};
    ob::read_snapshot(*buf, out_bid, out_ask);

    EXPECT_EQ(out_ask.depth, 1u);
    EXPECT_EQ(out_ask.prices[0],       20000LL);
    EXPECT_EQ(out_ask.quantities[0],   200ULL);
    EXPECT_EQ(out_ask.order_counts[0], 1U);
    EXPECT_EQ(out_bid.depth, 0u);
}

TEST(SoABuffer, SingleLevelModify) {
    auto buf = make_buffer();
    bool gap = false;

    ob::Level lv1{5000LL, 100ULL, 2U, 0U};
    ob::DeltaUpdate upd1 = make_update(ob::SIDE_BID, 1, 1);
    ob::apply_delta(*buf, upd1, &lv1, gap);

    ob::Level lv2{5000LL, 999ULL, 7U, 0U};
    ob::DeltaUpdate upd2 = make_update(ob::SIDE_BID, 2, 1);
    ob::apply_delta(*buf, upd2, &lv2, gap);

    ob::SoASide out_bid{}, out_ask{};
    ob::read_snapshot(*buf, out_bid, out_ask);

    EXPECT_EQ(out_bid.depth, 1u);
    EXPECT_EQ(out_bid.prices[0],       5000LL);
    EXPECT_EQ(out_bid.quantities[0],   999ULL);
    EXPECT_EQ(out_bid.order_counts[0], 7U);
}

TEST(SoABuffer, SingleLevelRemove) {
    auto buf = make_buffer();
    bool gap = false;

    ob::Level lv1{3000LL, 50ULL, 1U, 0U};
    ob::DeltaUpdate upd1 = make_update(ob::SIDE_ASK, 1, 1);
    ob::apply_delta(*buf, upd1, &lv1, gap);

    ob::Level lv2{3000LL, 0ULL, 0U, 0U};
    ob::DeltaUpdate upd2 = make_update(ob::SIDE_ASK, 2, 1);
    ob::apply_delta(*buf, upd2, &lv2, gap);

    ob::SoASide out_bid{}, out_ask{};
    ob::read_snapshot(*buf, out_bid, out_ask);

    EXPECT_EQ(out_ask.depth, 0u);
}

TEST(SoABuffer, ExactlyMaxLevels) {
    auto buf = make_buffer();
    uint64_t seq = 1;

    for (uint32_t i = 0; i < ob::MAX_LEVELS; ++i) {
        ob::Level lv{static_cast<int64_t>(i + 1), 1ULL, 1U, 0U};
        ob::DeltaUpdate upd = make_update(ob::SIDE_BID, seq++, 1);
        bool gap = false;
        ob::ob_status_t st = ob::apply_delta(*buf, upd, &lv, gap);
        ASSERT_EQ(st, ob::OB_OK) << "Failed at level " << i;
    }

    ob::SoASide out_bid{}, out_ask{};
    ob::read_snapshot(*buf, out_bid, out_ask);
    EXPECT_EQ(out_bid.depth, ob::MAX_LEVELS);
}

TEST(SoABuffer, OverflowRejected) {
    auto buf = make_buffer();
    uint64_t seq = 1;

    for (uint32_t i = 0; i < ob::MAX_LEVELS; ++i) {
        ob::Level lv{static_cast<int64_t>(i + 1), 1ULL, 1U, 0U};
        ob::DeltaUpdate upd = make_update(ob::SIDE_BID, seq++, 1);
        bool gap = false;
        ob::apply_delta(*buf, upd, &lv, gap);
    }

    ob::Level lv_extra{static_cast<int64_t>(ob::MAX_LEVELS + 1), 1ULL, 1U, 0U};
    ob::DeltaUpdate upd_extra = make_update(ob::SIDE_BID, seq, 1);
    bool gap = false;
    ob::ob_status_t st = ob::apply_delta(*buf, upd_extra, &lv_extra, gap);
    EXPECT_EQ(st, ob::OB_ERR_FULL);
}

TEST(SoABuffer, GapDetectionNonConsecutive) {
    auto buf = make_buffer();

    ob::Level lv{100LL, 10ULL, 1U, 0U};
    ob::DeltaUpdate upd1 = make_update(ob::SIDE_BID, 5, 1);
    bool gap1 = false;
    ob::apply_delta(*buf, upd1, &lv, gap1);

    ob::Level lv2{200LL, 20ULL, 1U, 0U};
    ob::DeltaUpdate upd2 = make_update(ob::SIDE_BID, 10, 1);
    bool gap2 = false;
    ob::apply_delta(*buf, upd2, &lv2, gap2);

    EXPECT_TRUE(gap2);
}

TEST(SoABuffer, GapDetectionConsecutive) {
    auto buf = make_buffer();

    ob::Level lv{100LL, 10ULL, 1U, 0U};
    ob::DeltaUpdate upd1 = make_update(ob::SIDE_BID, 5, 1);
    bool gap1 = false;
    ob::apply_delta(*buf, upd1, &lv, gap1);

    ob::Level lv2{200LL, 20ULL, 1U, 0U};
    ob::DeltaUpdate upd2 = make_update(ob::SIDE_BID, 6, 1);
    bool gap2 = false;
    ob::apply_delta(*buf, upd2, &lv2, gap2);

    EXPECT_FALSE(gap2);
}
