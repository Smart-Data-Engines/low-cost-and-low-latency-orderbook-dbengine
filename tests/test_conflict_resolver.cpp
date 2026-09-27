// Tests for ConflictResolver: property-based test (Property 8) and unit tests.
// Feature: multi-master-replication

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "orderbook/conflict_resolver.hpp"
#include "orderbook/hlc.hpp"

// ── RapidCheck generators ─────────────────────────────────────────────────────

namespace rc {

template <>
struct Arbitrary<ob::HLCTimestamp> {
    static Gen<ob::HLCTimestamp> arbitrary() {
        return gen::build<ob::HLCTimestamp>(
            gen::set(&ob::HLCTimestamp::physical_ns, gen::arbitrary<uint64_t>()),
            gen::set(&ob::HLCTimestamp::logical, gen::arbitrary<uint16_t>()),
            gen::set(&ob::HLCTimestamp::node_id, gen::arbitrary<uint16_t>()));
    }
};

template <>
struct Arbitrary<ob::ConflictKey> {
    static Gen<ob::ConflictKey> arbitrary() {
        return gen::build<ob::ConflictKey>(
            gen::set(&ob::ConflictKey::symbol,
                     gen::map(gen::container<std::string>(
                                  gen::inRange('A', 'Z')),
                              [](std::string s) {
                                  if (s.empty()) s = "S";
                                  return s;
                              })),
            gen::set(&ob::ConflictKey::exchange,
                     gen::map(gen::container<std::string>(
                                  gen::inRange('A', 'Z')),
                              [](std::string s) {
                                  if (s.empty()) s = "E";
                                  return s;
                              })),
            gen::set(&ob::ConflictKey::side,
                     gen::element(static_cast<uint8_t>(0),
                                  static_cast<uint8_t>(1))),
            gen::set(&ob::ConflictKey::price, gen::arbitrary<int64_t>()));
    }
};

} // namespace rc

// ═══════════════════════════════════════════════════════════════════════════════
// Property 8: LWW conflict resolution determinism
// **Validates: Requirements 5.1, 5.2, 5.3**
// ═══════════════════════════════════════════════════════════════════════════════

// Assertion 1: if remote > local → APPLY_REMOTE; if remote < local → REJECT_REMOTE
RC_GTEST_PROP(ConflictResolverProperty,
              prop_lww_determinism_remote_vs_local, ()) {
    const auto conflict_key = *rc::gen::arbitrary<ob::ConflictKey>();
    const auto local_hlc = *rc::gen::arbitrary<ob::HLCTimestamp>();
    const auto remote_hlc = *rc::gen::arbitrary<ob::HLCTimestamp>();

    // Skip when physical_ns and logical are equal — that's the tie-break case
    // tested separately.
    RC_PRE(local_hlc.physical_ns != remote_hlc.physical_ns ||
           local_hlc.logical != remote_hlc.logical);
    // And when one origin wrote both: that is not a conflict (#182), and has its own property.
    RC_PRE(local_hlc.node_id != remote_hlc.node_id);

    ob::ConflictResolver resolver;
    resolver.update_hlc(conflict_key, local_hlc, local_hlc.node_id);

    const auto result = resolver.resolve(conflict_key, remote_hlc,
                                         remote_hlc.node_id);

    // Compare only physical_ns and logical (not node_id) for strict ordering.
    const bool remote_newer =
        (remote_hlc.physical_ns > local_hlc.physical_ns) ||
        (remote_hlc.physical_ns == local_hlc.physical_ns &&
         remote_hlc.logical > local_hlc.logical);

    if (remote_newer) {
        RC_ASSERT(result == ob::ConflictResolution::APPLY_REMOTE);
    } else {
        RC_ASSERT(result == ob::ConflictResolution::REJECT_REMOTE);
    }
}

// Assertion 2: two independent resolvers with the same data produce the same result
RC_GTEST_PROP(ConflictResolverProperty,
              prop_lww_convergence, ()) {
    const auto conflict_key = *rc::gen::arbitrary<ob::ConflictKey>();
    const auto local_hlc = *rc::gen::arbitrary<ob::HLCTimestamp>();
    const auto remote_hlc = *rc::gen::arbitrary<ob::HLCTimestamp>();

    ob::ConflictResolver resolver1;
    ob::ConflictResolver resolver2;

    resolver1.update_hlc(conflict_key, local_hlc, local_hlc.node_id);
    resolver2.update_hlc(conflict_key, local_hlc, local_hlc.node_id);

    const auto result1 = resolver1.resolve(conflict_key, remote_hlc,
                                           remote_hlc.node_id);
    const auto result2 = resolver2.resolve(conflict_key, remote_hlc,
                                           remote_hlc.node_id);

    RC_ASSERT(result1 == result2);
}

// Assertion 3: tie-break — equal physical+logical → higher node_id wins
RC_GTEST_PROP(ConflictResolverProperty,
              prop_lww_tiebreak_higher_node_wins, ()) {
    const auto conflict_key = *rc::gen::arbitrary<ob::ConflictKey>();
    const auto phys = *rc::gen::arbitrary<uint64_t>();
    const auto logical_val = *rc::gen::arbitrary<uint16_t>();
    const auto node_a = *rc::gen::arbitrary<uint16_t>();
    const auto node_b = *rc::gen::arbitrary<uint16_t>();

    // We need distinct node_ids for a meaningful tie-break test.
    RC_PRE(node_a != node_b);

    ob::HLCTimestamp local_ts{phys, logical_val, node_a};
    ob::HLCTimestamp remote_ts{phys, logical_val, node_b};

    ob::ConflictResolver resolver;
    resolver.update_hlc(conflict_key, local_ts, node_a);

    const auto result = resolver.resolve(conflict_key, remote_ts, node_b);

    if (node_b > node_a) {
        RC_ASSERT(result == ob::ConflictResolution::APPLY_REMOTE);
    } else {
        RC_ASSERT(result == ob::ConflictResolution::REJECT_REMOTE);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests: ConflictEntry, per-level detection, log ring buffer
// Requirements: 5.4, 5.5, 5.6
// ═══════════════════════════════════════════════════════════════════════════════

// ── ConflictEntry has all required fields after resolve ───────────────────────

TEST(ConflictResolverUnit, ConflictEntryFieldsPopulated) {
    ob::ConflictResolver resolver;

    ob::ConflictKey key{"BTCUSD", "BINANCE", 0, 50000};
    ob::HLCTimestamp local_ts{1000, 1, 1};
    ob::HLCTimestamp remote_ts{2000, 0, 2};

    resolver.update_hlc(key, local_ts, 1);
    const auto result = resolver.resolve(key, remote_ts, 2);

    EXPECT_EQ(result, ob::ConflictResolution::APPLY_REMOTE);
    EXPECT_EQ(resolver.total_conflicts(), 1u);

    const auto entries = resolver.get_log(10);
    ASSERT_EQ(entries.size(), 1u);

    const auto& entry = entries[0];
    EXPECT_EQ(entry.key.symbol, "BTCUSD");
    EXPECT_EQ(entry.key.exchange, "BINANCE");
    EXPECT_EQ(entry.key.side, 0);
    EXPECT_EQ(entry.key.price, 50000);
    EXPECT_EQ(entry.local_hlc, local_ts);
    EXPECT_EQ(entry.remote_hlc, remote_ts);
    EXPECT_EQ(entry.local_origin, 1);
    EXPECT_EQ(entry.remote_origin, 2);
    EXPECT_EQ(entry.result, ob::ConflictEntry::REMOTE_WINS);
    EXPECT_GT(entry.detected_at_ns, 0u);
}

// ── Per-level conflict detection — different price levels don't interfere ─────

TEST(ConflictResolverUnit, PerLevelIsolation) {
    ob::ConflictResolver resolver;

    ob::ConflictKey key_a{"BTCUSD", "BINANCE", 0, 50000};
    ob::ConflictKey key_b{"BTCUSD", "BINANCE", 0, 51000};

    ob::HLCTimestamp ts_a{1000, 0, 1};
    ob::HLCTimestamp ts_b{2000, 0, 1};

    resolver.update_hlc(key_a, ts_a, 1);
    resolver.update_hlc(key_b, ts_b, 1);

    // Remote with ts=1500 should win against key_a (1000) but lose against key_b (2000).
    ob::HLCTimestamp remote_ts{1500, 0, 2};

    const auto result_a = resolver.resolve(key_a, remote_ts, 2);
    const auto result_b = resolver.resolve(key_b, remote_ts, 2);

    EXPECT_EQ(result_a, ob::ConflictResolution::APPLY_REMOTE);
    EXPECT_EQ(result_b, ob::ConflictResolution::REJECT_REMOTE);
}

// ── No local state → NO_CONFLICT ──────────────────────────────────────────────

TEST(ConflictResolverUnit, NoLocalStateReturnsNoConflict) {
    ob::ConflictResolver resolver;

    ob::ConflictKey key{"ETHUSD", "KRAKEN", 1, 3000};
    ob::HLCTimestamp remote_ts{5000, 0, 2};

    const auto result = resolver.resolve(key, remote_ts, 2);
    EXPECT_EQ(result, ob::ConflictResolution::NO_CONFLICT);
    EXPECT_EQ(resolver.total_conflicts(), 0u);
}

// ── Ring buffer — oldest entries are evicted after max_log_entries ─────────────

TEST(ConflictResolverUnit, RingBufferEviction) {
    constexpr size_t max_entries = 5;
    ob::ConflictResolver resolver(max_entries);

    ob::ConflictKey key{"SYM", "EXC", 0, 100};
    ob::HLCTimestamp local_ts{1000, 0, 1};
    resolver.update_hlc(key, local_ts, 1);

    // Generate max_entries + 3 conflicts to overflow the ring buffer - each from the origin that did
    // not write the level last, which is what makes it one (#182: origin 2 writing again after
    // itself was counted, and this test counted it).
    for (size_t i = 0; i < max_entries + 3; ++i) {
        const uint16_t origin = (i % 2 == 0) ? 2 : 3;
        ob::HLCTimestamp remote_ts{2000 + i, 0, origin};
        resolver.resolve(key, remote_ts, origin);
        resolver.update_hlc(key, remote_ts, origin);
    }

    const auto log = resolver.get_log(max_entries + 10);
    EXPECT_EQ(log.size(), max_entries);

    // Total conflicts should count all, not just those in the buffer.
    EXPECT_EQ(resolver.total_conflicts(), max_entries + 3);

    // The oldest entries should have been evicted — the first entry in the log
    // should correspond to the 4th conflict (index 3), i.e. remote physical_ns = 2003.
    EXPECT_EQ(log[0].remote_hlc.physical_ns, 2003u);
}

// ── per_symbol_conflicts() returns correct counters ───────────────────────────

TEST(ConflictResolverUnit, PerSymbolConflictCounters) {
    ob::ConflictResolver resolver;

    ob::ConflictKey key_btc{"BTCUSD", "BINANCE", 0, 50000};
    ob::ConflictKey key_eth{"ETHUSD", "BINANCE", 0, 3000};

    resolver.update_hlc(key_btc, {1000, 0, 1}, 1);
    resolver.update_hlc(key_eth, {1000, 0, 1}, 1);

    // 2 BTC conflicts
    resolver.resolve(key_btc, {2000, 0, 2}, 2);
    resolver.update_hlc(key_btc, {2000, 0, 2}, 2);
    resolver.resolve(key_btc, {3000, 0, 3}, 3);

    // 1 ETH conflict
    resolver.resolve(key_eth, {2000, 0, 2}, 2);

    const auto counts = resolver.per_symbol_conflicts();
    EXPECT_EQ(counts.at("BTCUSD"), 2u);
    EXPECT_EQ(counts.at("ETHUSD"), 1u);
    EXPECT_EQ(resolver.total_conflicts(), 3u);
}

// ── clear_log() clears log but NOT level_states_ ──────────────────────────────

TEST(ConflictResolverUnit, ClearLogPreservesLevelStates) {
    ob::ConflictResolver resolver;

    ob::ConflictKey key{"BTCUSD", "BINANCE", 0, 50000};
    ob::HLCTimestamp local_ts{1000, 0, 1};
    resolver.update_hlc(key, local_ts, 1);

    // Create a conflict.
    resolver.resolve(key, {2000, 0, 2}, 2);
    EXPECT_EQ(resolver.total_conflicts(), 1u);
    EXPECT_EQ(resolver.get_log(10).size(), 1u);

    // Clear the log.
    resolver.clear_log();
    EXPECT_EQ(resolver.total_conflicts(), 0u);
    EXPECT_TRUE(resolver.get_log(10).empty());

    // Level states should still be present — resolving against the same key
    // should still detect a conflict (not NO_CONFLICT).
    const auto result = resolver.resolve(key, {3000, 0, 3}, 3);
    EXPECT_NE(result, ob::ConflictResolution::NO_CONFLICT);
}

// ═══════════════════════════════════════════════════════════════════════════════
// #182: the origin that wrote a level last, writing it again, is not a conflict
// ═══════════════════════════════════════════════════════════════════════════════

// Whatever the two timestamps: newer applies, anything else is a late copy, and neither is logged
// or counted as a conflict - a book updating a level it holds is its ordinary life.
RC_GTEST_PROP(ConflictResolverProperty, OneOriginWritingALevelAgainIsNeverAConflict, ()) {
    const auto conflict_key = *rc::gen::arbitrary<ob::ConflictKey>();
    const auto local_hlc = *rc::gen::arbitrary<ob::HLCTimestamp>();
    const auto remote_hlc = *rc::gen::arbitrary<ob::HLCTimestamp>();
    const auto origin = *rc::gen::arbitrary<uint16_t>();

    ob::ConflictResolver resolver;
    resolver.update_hlc(conflict_key, local_hlc, origin);
    const auto result = resolver.resolve(conflict_key, remote_hlc, origin);

    const bool remote_newer =
        (remote_hlc.physical_ns > local_hlc.physical_ns) ||
        (remote_hlc.physical_ns == local_hlc.physical_ns &&
         remote_hlc.logical > local_hlc.logical);
    RC_ASSERT(result == (remote_newer ? ob::ConflictResolution::NO_CONFLICT
                                      : ob::ConflictResolution::REJECT_STALE));
    RC_ASSERT(resolver.total_conflicts() == 0u);
    RC_ASSERT(resolver.get_log(10).empty());
}

TEST(ConflictResolverUnit, TheOriginThatWroteALevelLastWritingItAgainIsNotAConflict) {
    // #178's measurement: 300 000 single-level writes from one node, 301 985 lines of "Conflict
    // detected" on the node that received them - one for nearly every replicated update.
    ob::ConflictResolver resolver;
    const ob::ConflictKey key{"BTCUSD", "BINANCE", 0, 50000};
    resolver.update_hlc(key, {1000, 0, 1}, 1);

    EXPECT_EQ(resolver.resolve(key, {2000, 0, 1}, 1), ob::ConflictResolution::NO_CONFLICT)
        << "the next update from the origin that wrote the level is not applied as a plain update";
    EXPECT_EQ(resolver.resolve(key, {500, 0, 1}, 1), ob::ConflictResolution::REJECT_STALE)
        << "a late copy of an older update from the same origin is not refused as stale";
    EXPECT_EQ(resolver.resolve(key, {1000, 0, 1}, 1), ob::ConflictResolution::REJECT_STALE)
        << "the same update again is not refused as stale";
    EXPECT_EQ(resolver.total_conflicts(), 0u);
    EXPECT_TRUE(resolver.get_log(10).empty());
    EXPECT_TRUE(resolver.per_symbol_conflicts().empty());

    // And another origin's is one, as it always was.
    EXPECT_EQ(resolver.resolve(key, {3000, 0, 2}, 2), ob::ConflictResolution::APPLY_REMOTE);
    EXPECT_EQ(resolver.total_conflicts(), 1u);
}

TEST(ConflictResolverUnit, ConflictsBetweenOriginsAreLoggedAsAWindowNotALineEach) {
    // The first opens a window and is said with its details; the ones inside it are counted, and
    // said as one line with the first after it. The counter and MM_CONFLICTS keep every one.
    constexpr uint64_t kSecond = 1'000'000'000ULL;
    const uint64_t t0 = 1'700'000'000ULL * kSecond;
    ob::ConflictResolver resolver;
    const ob::ConflictKey key{"ETHUSD", "BINANCE", 1, 3000};
    testing::internal::CaptureStderr();
    for (uint64_t i = 0; i < 6; ++i) {
        const uint16_t origin = (i % 2 == 0) ? 2 : 3;
        resolver.update_hlc(key, {1000 + i, 0, static_cast<uint16_t>(5 - origin)},
                            static_cast<uint16_t>(5 - origin));      // the other origin wrote it
        resolver.resolve(key, {2000 + i, 0, origin}, origin, t0 + i * kSecond);
    }
    resolver.update_hlc(key, {9000, 0, 3}, 3);
    resolver.resolve(key, {9500, 0, 2}, 2, t0 + 11 * kSecond);   // past the window
    for (uint64_t i = 0; i < 2; ++i) {                             // two inside the next one
        resolver.update_hlc(key, {10000 + i, 0, 3}, 3);
        resolver.resolve(key, {11000 + i, 0, 2}, 2, t0 + (12 + i) * kSecond);
    }
    resolver.update_hlc(key, {20000, 0, 3}, 3);
    resolver.resolve(key, {21000, 0, 2}, 2, t0 + 25 * kSecond);   // and past that one
    const std::string said = testing::internal::GetCapturedStderr();

    size_t info = 0, first = 0, summary = 0, second = 0;
    for (size_t at = said.find('\n'), from = 0; at != std::string::npos;
         from = at + 1, at = said.find('\n', from)) {
        const std::string line = said.substr(from, at - from);
        if (line.find("\"component\":\"conflict\"") == std::string::npos) continue;
        if (line.find("\"level\":\"INFO\"") == std::string::npos) continue;
        ++info;
        if (line.find("Conflict detected: REMOTE wins for ETHUSD/BINANCE/1/3000") != std::string::npos) {
            ++first;
        }
        if (line.find("5 more conflict(s) between origins in the 11 s since the last line") !=
            std::string::npos) {
            ++summary;
        }
        if (line.find("2 more conflict(s) between origins in the 14 s since the last line") !=
            std::string::npos) {
            ++second;
        }
    }
    EXPECT_EQ(info, 3u) << said;
    EXPECT_EQ(first, 1u) << "the window's first conflict was not said with its details";
    EXPECT_EQ(summary, 1u) << "the conflicts inside the window were not said as one count";
    EXPECT_EQ(second, 1u) << "the next window did not count from its own start";
    EXPECT_EQ(resolver.total_conflicts(), 10u) << "a conflict inside a window was not counted";
}
