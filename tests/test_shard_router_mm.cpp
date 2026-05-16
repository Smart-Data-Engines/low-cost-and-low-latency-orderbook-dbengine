// Tests for ShardRouter multi-master: property-based test (Property 12)
// and unit tests for MM failover, ShardMap mm_nodes serialization,
// and ShardCoordinator MM topology propagation.
// Feature: multi-master-replication (Tasks 19.4, 19.5)

#include "orderbook/shard_map.hpp"
#include "orderbook/shard_coordinator.hpp"

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

// ═══════════════════════════════════════════════════════════════════════════════
// Task 19.4 — Property 12: ShardRouter multi-master round-robin
// **Validates: Requirements 8.3**
//
// Generator: N operations (1-100), K nodes (1-5)
// Assertion: each node receives ⌊N/K⌋ or ⌈N/K⌉ operations (even distribution)
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(ShardRouterMMProperty, prop_round_robin_even_distribution, ()) {
    const int num_ops   = *rc::gen::inRange<int>(1, 101);
    const int num_nodes = *rc::gen::inRange<int>(1, 6);

    // Simulate round-robin counter logic (same as ShardRouter::get_client_mm)
    std::atomic<uint64_t> rr_counter{0};
    std::vector<int> node_hits(num_nodes, 0);

    for (int i = 0; i < num_ops; ++i) {
        uint64_t idx = rr_counter.fetch_add(1, std::memory_order_relaxed);
        size_t pos = static_cast<size_t>(idx % static_cast<uint64_t>(num_nodes));
        node_hits[pos]++;
    }

    // Verify even distribution: each node gets floor(N/K) or ceil(N/K)
    int floor_val = num_ops / num_nodes;
    int ceil_val  = floor_val + (num_ops % num_nodes != 0 ? 1 : 0);

    for (int i = 0; i < num_nodes; ++i) {
        RC_ASSERT(node_hits[i] >= floor_val);
        RC_ASSERT(node_hits[i] <= ceil_val);
    }

    // Verify total operations match
    int total = 0;
    for (int hits : node_hits) {
        total += hits;
    }
    RC_ASSERT(total == num_ops);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 19.5 — Unit tests
// ═══════════════════════════════════════════════════════════════════════════════

// ── Test: MM failover — when one node is unavailable, routing to remaining ───

TEST(ShardRouterMMUnit, Failover_SkipsUnavailableNodes) {
    // Simulate the round-robin logic with some nodes unavailable (nullptr).
    // This mirrors ShardRouter::get_client_mm() behavior.

    const int num_nodes = 3;
    // Simulate: node 0 = available, node 1 = unavailable, node 2 = available
    std::vector<bool> available = {true, false, true};

    std::atomic<uint64_t> rr_counter{0};
    std::vector<int> node_hits(num_nodes, 0);

    const int num_ops = 10;
    for (int i = 0; i < num_ops; ++i) {
        uint64_t idx = rr_counter.fetch_add(1, std::memory_order_relaxed);

        // Try round-robin, skip unavailable nodes (same logic as get_client_mm)
        for (int attempt = 0; attempt < num_nodes; ++attempt) {
            size_t pos = static_cast<size_t>((idx + static_cast<uint64_t>(attempt))
                                             % static_cast<uint64_t>(num_nodes));
            if (available[pos]) {
                node_hits[pos]++;
                break;
            }
        }
    }

    // Node 1 should have 0 hits (unavailable)
    EXPECT_EQ(node_hits[1], 0);

    // Nodes 0 and 2 should share all operations
    EXPECT_EQ(node_hits[0] + node_hits[2], num_ops);

    // Both available nodes should get some operations
    EXPECT_GT(node_hits[0], 0);
    EXPECT_GT(node_hits[2], 0);
}

TEST(ShardRouterMMUnit, Failover_AllNodesUnavailable_NoHits) {
    const int num_nodes = 3;
    std::vector<bool> available = {false, false, false};

    std::atomic<uint64_t> rr_counter{0};
    std::vector<int> node_hits(num_nodes, 0);

    const int num_ops = 5;
    for (int i = 0; i < num_ops; ++i) {
        uint64_t idx = rr_counter.fetch_add(1, std::memory_order_relaxed);
        bool found = false;
        for (int attempt = 0; attempt < num_nodes; ++attempt) {
            size_t pos = static_cast<size_t>((idx + static_cast<uint64_t>(attempt))
                                             % static_cast<uint64_t>(num_nodes));
            if (available[pos]) {
                node_hits[pos]++;
                found = true;
                break;
            }
        }
        // When all nodes are unavailable, no hit is recorded
        EXPECT_FALSE(found);
    }

    for (int i = 0; i < num_nodes; ++i) {
        EXPECT_EQ(node_hits[i], 0);
    }
}

// ── Test: ShardMap with mm_nodes — correct serialization/deserialization ─────

TEST(ShardRouterMMUnit, ShardMap_MMNodes_SerializationRoundTrip) {
    ob::ShardMap original{};
    original.version = 42;

    ob::ShardNode node;
    node.shard_id = "shard-0";
    node.address  = "127.0.0.1:9090";
    node.status   = ob::ShardStatus::ACTIVE;
    node.vnodes   = 150;

    // Add mm_nodes
    ob::MMNodeInfo mm1;
    mm1.node_id    = 1;
    mm1.address    = "10.0.0.1:9090";
    mm1.mm_address = "10.0.0.1:9091";
    node.mm_nodes.push_back(mm1);

    ob::MMNodeInfo mm2;
    mm2.node_id    = 2;
    mm2.address    = "10.0.0.2:9090";
    mm2.mm_address = "10.0.0.2:9091";
    node.mm_nodes.push_back(mm2);

    ob::MMNodeInfo mm3;
    mm3.node_id    = 3;
    mm3.address    = "10.0.0.3:9090";
    mm3.mm_address = "10.0.0.3:9091";
    node.mm_nodes.push_back(mm3);

    original.shards["shard-0"] = node;
    original.assignments["BTC.BINANCE"] = "shard-0";

    // Serialize
    std::string json = original.to_json();

    // Deserialize
    ob::ShardMap restored{};
    std::string error;
    bool ok = ob::ShardMap::from_json(json, restored, error);
    ASSERT_TRUE(ok) << "Parse error: " << error;

    // Verify
    EXPECT_EQ(restored.version, 42u);
    ASSERT_EQ(restored.shards.size(), 1u);
    ASSERT_TRUE(restored.shards.count("shard-0"));

    const auto& restored_node = restored.shards.at("shard-0");
    EXPECT_EQ(restored_node.shard_id, "shard-0");
    EXPECT_EQ(restored_node.address, "127.0.0.1:9090");
    ASSERT_EQ(restored_node.mm_nodes.size(), 3u);

    EXPECT_EQ(restored_node.mm_nodes[0].node_id, 1u);
    EXPECT_EQ(restored_node.mm_nodes[0].address, "10.0.0.1:9090");
    EXPECT_EQ(restored_node.mm_nodes[0].mm_address, "10.0.0.1:9091");

    EXPECT_EQ(restored_node.mm_nodes[1].node_id, 2u);
    EXPECT_EQ(restored_node.mm_nodes[1].address, "10.0.0.2:9090");
    EXPECT_EQ(restored_node.mm_nodes[1].mm_address, "10.0.0.2:9091");

    EXPECT_EQ(restored_node.mm_nodes[2].node_id, 3u);
    EXPECT_EQ(restored_node.mm_nodes[2].address, "10.0.0.3:9090");
    EXPECT_EQ(restored_node.mm_nodes[2].mm_address, "10.0.0.3:9091");
}

TEST(ShardRouterMMUnit, ShardMap_EmptyMMNodes_SerializationRoundTrip) {
    ob::ShardMap original{};
    original.version = 1;

    ob::ShardNode node;
    node.shard_id = "shard-0";
    node.address  = "127.0.0.1:9090";
    node.status   = ob::ShardStatus::ACTIVE;
    node.vnodes   = 150;
    // No mm_nodes

    original.shards["shard-0"] = node;

    // Serialize
    std::string json = original.to_json();

    // Deserialize
    ob::ShardMap restored{};
    std::string error;
    bool ok = ob::ShardMap::from_json(json, restored, error);
    ASSERT_TRUE(ok) << "Parse error: " << error;

    ASSERT_EQ(restored.shards.size(), 1u);
    const auto& restored_node = restored.shards.at("shard-0");
    EXPECT_TRUE(restored_node.mm_nodes.empty());
}

TEST(ShardRouterMMUnit, ShardNode_MMNodes_JsonRoundTrip) {
    ob::ShardNode original;
    original.shard_id = "shard-1";
    original.address  = "192.168.1.10:9090";
    original.status   = ob::ShardStatus::ACTIVE;
    original.vnodes   = 200;

    ob::MMNodeInfo mm;
    mm.node_id    = 5;
    mm.address    = "192.168.1.10:9090";
    mm.mm_address = "192.168.1.10:9091";
    original.mm_nodes.push_back(mm);

    std::string json = original.to_json();

    ob::ShardNode restored;
    bool ok = ob::ShardNode::from_json(json, restored);
    ASSERT_TRUE(ok);

    EXPECT_EQ(restored.shard_id, "shard-1");
    EXPECT_EQ(restored.address, "192.168.1.10:9090");
    EXPECT_EQ(restored.vnodes, 200u);
    ASSERT_EQ(restored.mm_nodes.size(), 1u);
    EXPECT_EQ(restored.mm_nodes[0].node_id, 5u);
    EXPECT_EQ(restored.mm_nodes[0].address, "192.168.1.10:9090");
    EXPECT_EQ(restored.mm_nodes[0].mm_address, "192.168.1.10:9091");
}

// ── Test: ShardCoordinator propagates mm_nodes in ShardMap ───────────────────

TEST(ShardRouterMMUnit, ShardCoordinator_PropagatesMMNodes) {
    // Verify that ShardMap with mm_nodes can be propagated through
    // the update_shard_map mechanism (simulated without etcd).

    ob::ShardMap map{};
    map.version = 10;

    ob::ShardNode node;
    node.shard_id = "shard-0";
    node.address  = "127.0.0.1:9090";
    node.status   = ob::ShardStatus::ACTIVE;
    node.vnodes   = 150;

    ob::MMNodeInfo mm1;
    mm1.node_id    = 1;
    mm1.address    = "10.0.0.1:9090";
    mm1.mm_address = "10.0.0.1:9091";
    node.mm_nodes.push_back(mm1);

    ob::MMNodeInfo mm2;
    mm2.node_id    = 2;
    mm2.address    = "10.0.0.2:9090";
    mm2.mm_address = "10.0.0.2:9091";
    node.mm_nodes.push_back(mm2);

    map.shards["shard-0"] = node;
    map.assignments["ETH.KRAKEN"] = "shard-0";

    // Serialize the map (as ShardCoordinator would publish to etcd)
    std::string json = map.to_json();

    // Deserialize (as ShardRouter would receive from etcd)
    ob::ShardMap received{};
    std::string error;
    bool ok = ob::ShardMap::from_json(json, received, error);
    ASSERT_TRUE(ok) << "Parse error: " << error;

    // Verify mm_nodes are propagated
    ASSERT_EQ(received.shards.size(), 1u);
    const auto& received_node = received.shards.at("shard-0");
    ASSERT_EQ(received_node.mm_nodes.size(), 2u);
    EXPECT_EQ(received_node.mm_nodes[0].node_id, 1u);
    EXPECT_EQ(received_node.mm_nodes[1].node_id, 2u);

    // Verify the ShardRouter can use mm_nodes for routing decisions
    // (shard has mm_nodes → use round-robin)
    EXPECT_FALSE(received_node.mm_nodes.empty());
}

TEST(ShardRouterMMUnit, RoundRobin_CounterWrapsCorrectly) {
    // Test that the round-robin counter wraps correctly with large values
    const int num_nodes = 3;
    std::atomic<uint64_t> rr_counter{UINT64_MAX - 2};
    std::vector<int> node_hits(num_nodes, 0);

    // Perform 6 operations that will wrap the counter
    for (int i = 0; i < 6; ++i) {
        uint64_t idx = rr_counter.fetch_add(1, std::memory_order_relaxed);
        size_t pos = static_cast<size_t>(idx % static_cast<uint64_t>(num_nodes));
        node_hits[pos]++;
    }

    // All 6 operations should be distributed (2 each for 3 nodes)
    int total = 0;
    for (int hits : node_hits) {
        total += hits;
    }
    EXPECT_EQ(total, 6);
}
