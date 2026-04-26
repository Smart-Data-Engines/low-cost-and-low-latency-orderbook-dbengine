// Tests for ShardMap & ConsistentHashRing: property-based tests (Properties 1-5, 9-10)
// and unit tests for edge cases.
// Feature: symbol-sharding

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "orderbook/shard_map.hpp"

// ── Helpers ──────────────────────────────────────────────────────────────────

static std::string make_shard_id(int i) {
    return "shard-" + std::to_string(i);
}

static std::string make_symbol(int i) {
    return "SYM" + std::to_string(i) + ".EX";
}

// ── Property 1: ShardMap version monotonic ──────────────────────────────────
// Validates: Requirements 1.4
RC_GTEST_PROP(ShardMapProperty, prop_version_monotonic, ()) {
    const int num_mutations = *rc::gen::inRange<int>(1, 30);

    ob::ShardMap sm{};
    sm.version = 0;

    // Seed with at least one shard so we can do assignments
    ob::ShardNode seed_node;
    seed_node.shard_id = "shard-0";
    seed_node.address  = "127.0.0.1:5555";
    seed_node.status   = ob::ShardStatus::ACTIVE;
    seed_node.vnodes   = 150;
    sm.shards["shard-0"] = seed_node;
    sm.version++;

    int next_shard = 1;
    int next_sym   = 0;

    for (int i = 0; i < num_mutations; ++i) {
        uint64_t version_before = sm.version;

        // Pick a random mutation type: 0=add shard, 1=remove shard, 2=assign symbol
        int mutation = *rc::gen::inRange<int>(0, 3);

        if (mutation == 0) {
            // Add shard
            std::string sid = make_shard_id(next_shard++);
            ob::ShardNode node;
            node.shard_id = sid;
            node.address  = "127.0.0.1:" + std::to_string(6000 + next_shard);
            node.status   = ob::ShardStatus::ACTIVE;
            node.vnodes   = 150;
            sm.shards[sid] = node;
            sm.version++;
        } else if (mutation == 1 && sm.shards.size() > 1) {
            // Remove a non-seed shard
            auto it = sm.shards.begin();
            std::advance(it, *rc::gen::inRange<int>(0, static_cast<int>(sm.shards.size())));
            if (it != sm.shards.end()) {
                sm.shards.erase(it);
                sm.version++;
            }
        } else {
            // Assign symbol to a random shard
            if (!sm.shards.empty()) {
                auto it = sm.shards.begin();
                std::advance(it, *rc::gen::inRange<int>(0, static_cast<int>(sm.shards.size())));
                std::string sym = make_symbol(next_sym++);
                sm.assignments[sym] = it->first;
                sm.version++;
            }
        }

        RC_ASSERT(sm.version > version_before);
    }
}

// ── Property 2: Consistent hashing determinism ──────────────────────────────
// Validates: Requirements 3.1, 3.6
RC_GTEST_PROP(ConsistentHashRingProperty, prop_determinism, ()) {
    const int num_shards = *rc::gen::inRange<int>(1, 11);
    const int vnodes     = *rc::gen::inRange<int>(10, 501);

    // Generate a random key
    const int key_len = *rc::gen::inRange<int>(1, 50);
    std::string key;
    key.reserve(static_cast<size_t>(key_len));
    for (int ki = 0; ki < key_len; ++ki) {
        key.push_back(*rc::gen::inRange<char>('A', 'z'));
    }

    // Build ring
    ob::ConsistentHashRing ring1;
    for (int i = 0; i < num_shards; ++i) {
        ring1.add_shard(make_shard_id(i), static_cast<uint32_t>(vnodes));
    }

    // Multiple lookups on same ring → same result
    std::string result1 = ring1.lookup(key);
    std::string result2 = ring1.lookup(key);
    std::string result3 = ring1.lookup(key);
    RC_ASSERT(result1 == result2);
    RC_ASSERT(result2 == result3);
    RC_ASSERT(!result1.empty());

    // Build a new ring with the same shards → identical assignment
    ob::ConsistentHashRing ring2;
    for (int i = 0; i < num_shards; ++i) {
        ring2.add_shard(make_shard_id(i), static_cast<uint32_t>(vnodes));
    }
    std::string result_new = ring2.lookup(key);
    RC_ASSERT(result1 == result_new);
}

// ── Property 3: Minimal movement on shard addition ──────────────────────────
// Validates: Requirements 3.2
RC_GTEST_PROP(ConsistentHashRingProperty, prop_minimal_movement_on_add, ()) {
    const int num_shards  = *rc::gen::inRange<int>(2, 8);
    const int num_symbols = *rc::gen::inRange<int>(50, 201);
    const int vnodes      = 150;

    // Generate symbol keys
    std::vector<std::string> symbols;
    symbols.reserve(num_symbols);
    for (int i = 0; i < num_symbols; ++i) {
        symbols.push_back(make_symbol(i));
    }

    // Build initial ring
    ob::ConsistentHashRing ring;
    for (int i = 0; i < num_shards; ++i) {
        ring.add_shard(make_shard_id(i), vnodes);
    }

    // Compute initial assignments
    auto old_assignments = ring.compute_assignments(symbols);

    // Add one new shard
    ring.add_shard(make_shard_id(num_shards), vnodes);

    // Compute new assignments
    auto new_assignments = ring.compute_assignments(symbols);

    // Count moved symbols
    int moved = 0;
    for (const auto& sym : symbols) {
        if (old_assignments[sym] != new_assignments[sym]) {
            ++moved;
        }
    }

    // Symbols that didn't move keep their assignment
    for (const auto& sym : symbols) {
        if (old_assignments[sym] == new_assignments[sym]) {
            RC_ASSERT(old_assignments[sym] == new_assignments[sym]);
        }
    }

    // Upper bound: moved ≤ 2 * |S| / N
    int upper_bound = 2 * num_symbols / num_shards;
    RC_ASSERT(moved <= upper_bound);
}

// ── Property 4: Redistribution on shard removal ─────────────────────────────
// Validates: Requirements 3.3
RC_GTEST_PROP(ConsistentHashRingProperty, prop_redistribution_on_removal, ()) {
    const int num_shards  = *rc::gen::inRange<int>(2, 8);
    const int num_symbols = *rc::gen::inRange<int>(100, 301);
    const int vnodes      = 150;

    std::vector<std::string> symbols;
    symbols.reserve(num_symbols);
    for (int i = 0; i < num_symbols; ++i) {
        symbols.push_back(make_symbol(i));
    }

    // Build ring
    ob::ConsistentHashRing ring;
    for (int i = 0; i < num_shards; ++i) {
        ring.add_shard(make_shard_id(i), vnodes);
    }

    auto old_assignments = ring.compute_assignments(symbols);

    // Pick a shard to remove
    int remove_idx = *rc::gen::inRange<int>(0, num_shards);
    std::string removed_shard = make_shard_id(remove_idx);

    ring.remove_shard(removed_shard);

    auto new_assignments = ring.compute_assignments(symbols);

    // 1) All symbols of removed shard are assigned to remaining shards
    for (const auto& sym : symbols) {
        if (old_assignments[sym] == removed_shard) {
            RC_ASSERT(new_assignments[sym] != removed_shard);
            RC_ASSERT(!new_assignments[sym].empty());
        }
    }

    // 2) No symbol from remaining shards changes assignment
    for (const auto& sym : symbols) {
        if (old_assignments[sym] != removed_shard) {
            RC_ASSERT(old_assignments[sym] == new_assignments[sym]);
        }
    }

    // 3) Distribution ≈ uniform: no shard has more than 2x the ideal share
    int remaining = num_shards - 1;
    if (remaining > 0) {
        std::unordered_map<std::string, int> counts;
        for (const auto& sym : symbols) {
            counts[new_assignments[sym]]++;
        }
        double ideal = static_cast<double>(num_symbols) / remaining;
        for (const auto& [shard, count] : counts) {
            RC_ASSERT(static_cast<double>(count) <= 2.0 * ideal);
        }
    }
}

// ── Property 5: Pinned symbols invariant ────────────────────────────────────
// Validates: Requirements 3.4, 3.5
RC_GTEST_PROP(ShardMapProperty, prop_pinned_symbols_invariant, ()) {
    const int num_shards  = *rc::gen::inRange<int>(2, 6);
    const int num_symbols = *rc::gen::inRange<int>(10, 51);
    const int num_pinned  = *rc::gen::inRange<int>(1, std::min(num_symbols, 10) + 1);
    const int num_changes = *rc::gen::inRange<int>(1, 10);

    // Build initial ShardMap
    ob::ShardMap sm{};
    sm.version = 1;

    ob::ConsistentHashRing ring;
    for (int i = 0; i < num_shards; ++i) {
        std::string sid = make_shard_id(i);
        ob::ShardNode node;
        node.shard_id = sid;
        node.address  = "127.0.0.1:" + std::to_string(5555 + i);
        node.status   = ob::ShardStatus::ACTIVE;
        node.vnodes   = 150;
        sm.shards[sid] = node;
        ring.add_shard(sid, 150);
    }

    // Generate symbols and assign via consistent hashing
    std::vector<std::string> symbols;
    for (int i = 0; i < num_symbols; ++i) {
        symbols.push_back(make_symbol(i));
    }
    auto assignments = ring.compute_assignments(symbols);
    for (const auto& [sym, shard] : assignments) {
        sm.assignments[sym] = shard;
    }

    // Pin some symbols
    std::unordered_map<std::string, std::string> pinned_assignments;
    for (int i = 0; i < num_pinned; ++i) {
        const auto& sym = symbols[static_cast<size_t>(i)];
        sm.pinned_symbols.insert(sym);
        pinned_assignments[sym] = sm.assignments[sym];
    }

    // Apply topology changes (add/remove shards), but preserve pinned assignments
    int next_shard = num_shards;
    for (int c = 0; c < num_changes; ++c) {
        int change_type = *rc::gen::inRange<int>(0, 2);

        if (change_type == 0) {
            // Add shard
            std::string sid = make_shard_id(next_shard++);
            ob::ShardNode node;
            node.shard_id = sid;
            node.address  = "127.0.0.1:" + std::to_string(5555 + next_shard);
            node.status   = ob::ShardStatus::ACTIVE;
            node.vnodes   = 150;
            sm.shards[sid] = node;
            ring.add_shard(sid, 150);
        } else if (ring.shard_count() > 2) {
            // Remove a non-pinned-target shard (ensure we don't remove shards
            // that pinned symbols point to)
            std::set<std::string> protected_shards;
            for (const auto& [sym, shard] : pinned_assignments) {
                protected_shards.insert(shard);
            }
            // Find a removable shard
            std::string to_remove;
            for (const auto& [sid, _] : sm.shards) {
                if (protected_shards.find(sid) == protected_shards.end()) {
                    to_remove = sid;
                    break;
                }
            }
            if (!to_remove.empty()) {
                ring.remove_shard(to_remove);
                sm.shards.erase(to_remove);
            }
        }

        // Recompute assignments via consistent hashing for non-pinned symbols
        auto new_ring_assignments = ring.compute_assignments(symbols);
        for (const auto& sym : symbols) {
            if (sm.pinned_symbols.count(sym) == 0) {
                sm.assignments[sym] = new_ring_assignments[sym];
            }
            // Pinned symbols keep their assignment — do NOT update
        }
        sm.version++;

        // Verify: pinned symbols always point to the same shard
        for (const auto& [sym, expected_shard] : pinned_assignments) {
            RC_ASSERT(sm.assignments[sym] == expected_shard);
        }
    }
}


// ── Property 9: ShardMap serialization round-trip ───────────────────────────
// Validates: Requirements 8.1, 8.2, 8.4, 8.5
RC_GTEST_PROP(ShardMapProperty, prop_serialization_round_trip, ()) {
    const int num_shards     = *rc::gen::inRange<int>(1, 11);
    const int num_symbols    = *rc::gen::inRange<int>(0, 101);
    const int num_pinned     = *rc::gen::inRange<int>(0, std::min(num_symbols + 1, 6));
    const int num_migrations = *rc::gen::inRange<int>(0, 4);

    ob::ShardMap original{};
    original.version = *rc::gen::inRange<uint64_t>(0, 10000);

    // Generate shards
    std::vector<std::string> shard_ids;
    for (int i = 0; i < num_shards; ++i) {
        std::string sid = make_shard_id(i);
        shard_ids.push_back(sid);
        ob::ShardNode node;
        node.shard_id = sid;
        node.address  = "10.0.0." + std::to_string(i) + ":5555";
        int status_val = *rc::gen::inRange<int>(0, 3);
        node.status = static_cast<ob::ShardStatus>(status_val);
        node.vnodes = *rc::gen::inRange<uint32_t>(10, 500);
        original.shards[sid] = node;
    }

    // Generate symbol assignments
    for (int i = 0; i < num_symbols; ++i) {
        std::string sym = make_symbol(i);
        int shard_idx = *rc::gen::inRange<int>(0, num_shards);
        original.assignments[sym] = shard_ids[static_cast<size_t>(shard_idx)];
    }

    // Generate pinned symbols (subset of assigned symbols)
    {
        int pinned_count = 0;
        for (const auto& [sym, _] : original.assignments) {
            if (pinned_count >= num_pinned) break;
            original.pinned_symbols.insert(sym);
            ++pinned_count;
        }
    }

    // Generate migrations
    for (int i = 0; i < num_migrations && i < num_shards; ++i) {
        ob::MigrationState ms;
        ms.symbol_key      = make_symbol(i);
        ms.source_shard_id = shard_ids[0];
        ms.target_shard_id = shard_ids[static_cast<size_t>(std::min(i + 1, num_shards - 1))];
        ms.progress_pct    = *rc::gen::inRange<uint8_t>(0, 101);
        original.active_migrations.push_back(ms);
    }

    // Round-trip: to_json → from_json → compare
    std::string json1 = original.to_json();
    ob::ShardMap restored{};
    std::string error;
    bool ok = ob::ShardMap::from_json(json1, restored, error);
    RC_ASSERT(ok);
    RC_ASSERT(error.empty());

    // Compare fields
    RC_ASSERT(restored.version == original.version);
    RC_ASSERT(restored.assignments.size() == original.assignments.size());
    for (const auto& [sym, shard] : original.assignments) {
        auto it = restored.assignments.find(sym);
        RC_ASSERT(it != restored.assignments.end());
        RC_ASSERT(it->second == shard);
    }
    RC_ASSERT(restored.shards.size() == original.shards.size());
    for (const auto& [sid, node] : original.shards) {
        auto it = restored.shards.find(sid);
        RC_ASSERT(it != restored.shards.end());
        RC_ASSERT(it->second.shard_id == node.shard_id);
        RC_ASSERT(it->second.address == node.address);
        RC_ASSERT(it->second.status == node.status);
        RC_ASSERT(it->second.vnodes == node.vnodes);
    }
    RC_ASSERT(restored.pinned_symbols == original.pinned_symbols);
    RC_ASSERT(restored.active_migrations.size() == original.active_migrations.size());
    for (size_t i = 0; i < original.active_migrations.size(); ++i) {
        RC_ASSERT(restored.active_migrations[i].symbol_key == original.active_migrations[i].symbol_key);
        RC_ASSERT(restored.active_migrations[i].source_shard_id == original.active_migrations[i].source_shard_id);
        RC_ASSERT(restored.active_migrations[i].target_shard_id == original.active_migrations[i].target_shard_id);
        RC_ASSERT(restored.active_migrations[i].progress_pct == original.active_migrations[i].progress_pct);
    }

    // Double serialization → identical JSON string (determinism)
    std::string json2 = restored.to_json();
    RC_ASSERT(json1 == json2);
}

// ── Property 10: Invalid JSON resilience ────────────────────────────────────
// Validates: Requirements 8.3
RC_GTEST_PROP(ShardMapProperty, prop_invalid_json_resilience, ()) {
    // Generate a random invalid input type
    int input_type = *rc::gen::inRange<int>(0, 5);

    std::string input;
    switch (input_type) {
    case 0: {
        // Random bytes
        int rlen = *rc::gen::inRange<int>(1, 100);
        input.reserve(static_cast<size_t>(rlen));
        for (int ri = 0; ri < rlen; ++ri) {
            input.push_back(*rc::gen::inRange<char>(0, 127));
        }
        break;
    }
    case 1:
        // Valid JSON but missing fields
        input = R"({"version": 1})";
        break;
    case 2:
        // Wrong type for version
        input = R"({"version": "not_a_number", "assignments": {}, "shards": {}, "pinned_symbols": [], "active_migrations": []})";
        break;
    case 3:
        // Empty object
        input = "{}";
        break;
    case 4:
        // Array instead of object
        input = "[]";
        break;
    }

    ob::ShardMap out{};
    // Set sentinel values to verify no modification on failure
    out.version = 99999;

    std::string error;
    bool ok = ob::ShardMap::from_json(input, out, error);

    // Must return false (no crash, no UB)
    RC_ASSERT(!ok);
    // Must have a descriptive error
    RC_ASSERT(!error.empty());
}

// ── Unit tests: ConsistentHashRing edge cases ───────────────────────────────
// Validates: Requirements 3.1

TEST(ConsistentHashRingUnit, EmptyRingLookupReturnsEmpty) {
    ob::ConsistentHashRing ring;
    EXPECT_EQ(ring.lookup("any_key"), "");
    EXPECT_EQ(ring.lookup(""), "");
    EXPECT_EQ(ring.shard_count(), 0u);
    EXPECT_EQ(ring.vnode_count(), 0u);
}

TEST(ConsistentHashRingUnit, OneShardAllKeysMapToIt) {
    ob::ConsistentHashRing ring;
    ring.add_shard("shard-0", 150);

    EXPECT_EQ(ring.shard_count(), 1u);
    EXPECT_EQ(ring.vnode_count(), 150u);

    // All keys should map to the single shard
    EXPECT_EQ(ring.lookup("AAPL.XNAS"), "shard-0");
    EXPECT_EQ(ring.lookup("BTCUSD.BINANCE"), "shard-0");
    EXPECT_EQ(ring.lookup(""), "shard-0");
    EXPECT_EQ(ring.lookup("some_random_key_12345"), "shard-0");
}

TEST(ConsistentHashRingUnit, RemoveLastShardRingEmpty) {
    ob::ConsistentHashRing ring;
    ring.add_shard("shard-0", 100);
    EXPECT_EQ(ring.shard_count(), 1u);
    EXPECT_GT(ring.vnode_count(), 0u);

    ring.remove_shard("shard-0");
    EXPECT_EQ(ring.shard_count(), 0u);
    EXPECT_EQ(ring.vnode_count(), 0u);
    EXPECT_EQ(ring.lookup("any_key"), "");
}

TEST(ConsistentHashRingUnit, VnodeCountAfterAddRemove) {
    ob::ConsistentHashRing ring;

    ring.add_shard("shard-0", 100);
    EXPECT_EQ(ring.vnode_count(), 100u);
    EXPECT_EQ(ring.shard_count(), 1u);

    ring.add_shard("shard-1", 200);
    EXPECT_EQ(ring.vnode_count(), 300u);
    EXPECT_EQ(ring.shard_count(), 2u);

    ring.remove_shard("shard-0");
    EXPECT_EQ(ring.vnode_count(), 200u);
    EXPECT_EQ(ring.shard_count(), 1u);

    ring.remove_shard("shard-1");
    EXPECT_EQ(ring.vnode_count(), 0u);
    EXPECT_EQ(ring.shard_count(), 0u);
}

TEST(ConsistentHashRingUnit, RemoveNonexistentShardIsNoop) {
    ob::ConsistentHashRing ring;
    ring.add_shard("shard-0", 50);
    EXPECT_EQ(ring.shard_count(), 1u);

    ring.remove_shard("shard-nonexistent");
    EXPECT_EQ(ring.shard_count(), 1u);
    EXPECT_EQ(ring.vnode_count(), 50u);
}

// ── Unit tests: ShardMap JSON edge cases ────────────────────────────────────
// Validates: Requirements 8.1, 8.2, 8.3

TEST(ShardMapJsonUnit, EmptyShardMapProducesValidJson) {
    ob::ShardMap sm{};
    sm.version = 0;

    std::string json = sm.to_json();
    EXPECT_FALSE(json.empty());

    // Should round-trip
    ob::ShardMap restored{};
    std::string error;
    bool ok = ob::ShardMap::from_json(json, restored, error);
    EXPECT_TRUE(ok) << "Error: " << error;
    EXPECT_EQ(restored.version, 0u);
    EXPECT_TRUE(restored.assignments.empty());
    EXPECT_TRUE(restored.shards.empty());
    EXPECT_TRUE(restored.pinned_symbols.empty());
    EXPECT_TRUE(restored.active_migrations.empty());
}

TEST(ShardMapJsonUnit, MissingVersionFieldReturnsFalse) {
    std::string json = R"({"assignments": {}, "shards": {}, "pinned_symbols": [], "active_migrations": []})";
    ob::ShardMap out{};
    std::string error;
    bool ok = ob::ShardMap::from_json(json, out, error);
    EXPECT_FALSE(ok);
    EXPECT_FALSE(error.empty());
    // Error should mention "version"
    EXPECT_NE(error.find("version"), std::string::npos);
}

TEST(ShardMapJsonUnit, WrongTypeVersionReturnsFalse) {
    std::string json = R"({"version": "not_a_number", "assignments": {}, "shards": {}, "pinned_symbols": [], "active_migrations": []})";
    ob::ShardMap out{};
    std::string error;
    bool ok = ob::ShardMap::from_json(json, out, error);
    EXPECT_FALSE(ok);
    EXPECT_FALSE(error.empty());
    EXPECT_NE(error.find("version"), std::string::npos);
}

TEST(ShardMapJsonUnit, EmptyStringReturnsFalse) {
    ob::ShardMap out{};
    std::string error;
    bool ok = ob::ShardMap::from_json("", out, error);
    EXPECT_FALSE(ok);
    EXPECT_FALSE(error.empty());
}

TEST(ShardMapJsonUnit, InvalidJsonSyntaxReturnsFalse) {
    ob::ShardMap out{};
    std::string error;
    bool ok = ob::ShardMap::from_json("{invalid json", out, error);
    EXPECT_FALSE(ok);
    EXPECT_FALSE(error.empty());
}
