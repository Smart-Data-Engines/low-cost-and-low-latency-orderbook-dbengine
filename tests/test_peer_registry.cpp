// Tests for PeerRegistry: property-based tests (Properties 10, 11) and unit tests.
// Feature: multi-master-replication

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "orderbook/peer_registry.hpp"

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
struct Arbitrary<ob::PeerInfo> {
    static Gen<ob::PeerInfo> arbitrary() {
        return gen::build<ob::PeerInfo>(
            gen::set(&ob::PeerInfo::node_id,
                     gen::inRange<uint16_t>(1, 1000)),
            gen::set(&ob::PeerInfo::address,
                     gen::map(
                         gen::tuple(gen::inRange(1, 255), gen::inRange(1, 255),
                                    gen::inRange(1, 255), gen::inRange(1, 255),
                                    gen::inRange(1024, 65535)),
                         [](const std::tuple<int, int, int, int, int>& t) {
                             return std::to_string(std::get<0>(t)) + "." +
                                    std::to_string(std::get<1>(t)) + "." +
                                    std::to_string(std::get<2>(t)) + "." +
                                    std::to_string(std::get<3>(t)) + ":" +
                                    std::to_string(std::get<4>(t));
                         })),
            gen::set(&ob::PeerInfo::status,
                     gen::element(std::string("active"),
                                  std::string("joining"),
                                  std::string("leaving"))),
            gen::set(&ob::PeerInfo::last_hlc,
                     gen::arbitrary<ob::HLCTimestamp>()),
            gen::set(&ob::PeerInfo::wal_file_index,
                     gen::arbitrary<uint32_t>()),
            gen::set(&ob::PeerInfo::wal_byte_offset,
                     gen::inRange<size_t>(0, 1ULL << 40)));
    }
};

template <>
struct Arbitrary<ob::PeerRegistryData> {
    static Gen<ob::PeerRegistryData> arbitrary() {
        return gen::mapcat(
            gen::inRange(0, 6),
            [](int peer_count) {
                return gen::map(
                    gen::tuple(
                        gen::arbitrary<uint64_t>(),
                        gen::container<std::vector<ob::PeerInfo>>(
                            static_cast<std::size_t>(peer_count),
                            gen::arbitrary<ob::PeerInfo>())),
                    [](const std::tuple<uint64_t,
                                        std::vector<ob::PeerInfo>>& t) {
                        ob::PeerRegistryData data;
                        data.version  = std::get<0>(t);
                        data.topology = "full-mesh";
                        for (const auto& peer : std::get<1>(t)) {
                            data.peers[peer.node_id] = peer;
                        }
                        return data;
                    });
            });
    }
};

} // namespace rc

// ═══════════════════════════════════════════════════════════════════════════════
// Property 10: Peer_Registry JSON round-trip
// **Validates: Requirements 13.1, 13.3, 13.5**
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(PeerRegistryProperty,
              prop_json_roundtrip, ()) {
    const auto data = *rc::gen::arbitrary<ob::PeerRegistryData>();

    const std::string json_str = data.to_json();

    ob::PeerRegistryData parsed;
    std::string parse_error;
    bool ok = ob::PeerRegistryData::from_json(json_str, parsed, parse_error);

    RC_ASSERT(ok);
    RC_ASSERT(parsed == data);
}

// Assertion: keys in JSON are sorted alphabetically
RC_GTEST_PROP(PeerRegistryProperty,
              prop_json_keys_sorted, ()) {
    const auto data = *rc::gen::arbitrary<ob::PeerRegistryData>();

    const std::string json_str = data.to_json();

    // Parse the JSON and verify top-level keys are sorted.
    auto j = nlohmann::json::parse(json_str);
    RC_ASSERT(j.is_object());

    std::vector<std::string> keys;
    for (auto it = j.begin(); it != j.end(); ++it) {
        keys.push_back(it.key());
    }

    std::vector<std::string> sorted_keys = keys;
    std::sort(sorted_keys.begin(), sorted_keys.end());
    RC_ASSERT(keys == sorted_keys);

    // Also verify peer sub-object keys are sorted.
    if (j.contains("peers") && j["peers"].is_object()) {
        for (auto& [peer_key, peer_val] : j["peers"].items()) {
            if (!peer_val.is_object()) continue;
            std::vector<std::string> peer_keys;
            for (auto it = peer_val.begin(); it != peer_val.end(); ++it) {
                peer_keys.push_back(it.key());
            }
            std::vector<std::string> peer_sorted = peer_keys;
            std::sort(peer_sorted.begin(), peer_sorted.end());
            RC_ASSERT(peer_keys == peer_sorted);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 11: Peer_Registry etcd key format
// **Validates: Requirements 8.2**
// ═══════════════════════════════════════════════════════════════════════════════

// Generator for safe prefix strings (alphanumeric + '/')
static rc::Gen<std::string> gen_prefix() {
    return rc::gen::map(
        rc::gen::container<std::string>(
            rc::gen::element('a', 'b', 'c', '/', 'x', 'y')),
        [](std::string s) {
            // Ensure non-empty and ends with '/'
            if (s.empty()) s = "/";
            if (s.back() != '/') s += '/';
            return s;
        });
}

static rc::Gen<std::string> gen_shard_id() {
    return rc::gen::map(
        rc::gen::container<std::string>(
            rc::gen::element('a', 'b', 'c', '-', '0', '1', '2')),
        [](std::string s) {
            if (s.empty()) s = "shard-0";
            return s;
        });
}

// Without sharding: mm_peer_key(prefix, node_id) == "<prefix>mm_peers/<node_id>"
RC_GTEST_PROP(PeerRegistryProperty,
              prop_etcd_key_no_shard, ()) {
    const auto prefix  = *gen_prefix();
    const auto node_id = *rc::gen::inRange<uint16_t>(0, 65535);

    const std::string key = ob::mm_peer_key(prefix, node_id);
    const std::string expected = prefix + "mm_peers/" + std::to_string(node_id);

    RC_ASSERT(key == expected);
}

// With sharding: mm_peer_key(prefix, shard_id, node_id) == "<prefix>shards/<shard_id>/mm_peers/<node_id>"
RC_GTEST_PROP(PeerRegistryProperty,
              prop_etcd_key_with_shard, ()) {
    const auto prefix   = *gen_prefix();
    const auto shard_id = *gen_shard_id();
    const auto node_id  = *rc::gen::inRange<uint16_t>(0, 65535);

    const std::string key = ob::mm_peer_key(prefix, shard_id, node_id);
    const std::string expected = prefix + "shards/" + shard_id +
                                 "/mm_peers/" + std::to_string(node_id);

    RC_ASSERT(key == expected);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests: PeerInfo JSON errors, PeerRegistryData pretty-print, range-end
// Requirements: 13.2, 13.4
// ═══════════════════════════════════════════════════════════════════════════════

// ── PeerInfo::from_json with invalid JSON → descriptive error ─────────────────

TEST(PeerRegistryUnit, PeerInfoFromJsonInvalidJson) {
    ob::PeerInfo out;
    std::string error;
    bool ok = ob::PeerInfo::from_json("not json at all", out, error);
    EXPECT_FALSE(ok);
    EXPECT_FALSE(error.empty());
    EXPECT_NE(error.find("invalid JSON"), std::string::npos);
}

TEST(PeerRegistryUnit, PeerInfoFromJsonNotObject) {
    ob::PeerInfo out;
    std::string error;
    bool ok = ob::PeerInfo::from_json("[1,2,3]", out, error);
    EXPECT_FALSE(ok);
    EXPECT_NE(error.find("expected JSON object"), std::string::npos);
}

TEST(PeerRegistryUnit, PeerInfoFromJsonMissingNodeId) {
    ob::PeerInfo out;
    std::string error;
    bool ok = ob::PeerInfo::from_json(
        R"({"address":"1.2.3.4:5000","status":"active","last_hlc":"0.0.0","wal_file_index":0,"wal_byte_offset":0})",
        out, error);
    EXPECT_FALSE(ok);
    EXPECT_NE(error.find("node_id"), std::string::npos);
}

TEST(PeerRegistryUnit, PeerInfoFromJsonMissingAddress) {
    ob::PeerInfo out;
    std::string error;
    bool ok = ob::PeerInfo::from_json(
        R"({"node_id":1,"status":"active","last_hlc":"0.0.0","wal_file_index":0,"wal_byte_offset":0})",
        out, error);
    EXPECT_FALSE(ok);
    EXPECT_NE(error.find("address"), std::string::npos);
}

TEST(PeerRegistryUnit, PeerInfoFromJsonMissingStatus) {
    ob::PeerInfo out;
    std::string error;
    bool ok = ob::PeerInfo::from_json(
        R"({"node_id":1,"address":"1.2.3.4:5000","last_hlc":"0.0.0","wal_file_index":0,"wal_byte_offset":0})",
        out, error);
    EXPECT_FALSE(ok);
    EXPECT_NE(error.find("status"), std::string::npos);
}

TEST(PeerRegistryUnit, PeerInfoFromJsonInvalidHlc) {
    ob::PeerInfo out;
    std::string error;
    bool ok = ob::PeerInfo::from_json(
        R"({"node_id":1,"address":"1.2.3.4:5000","status":"active","last_hlc":"bad","wal_file_index":0,"wal_byte_offset":0})",
        out, error);
    EXPECT_FALSE(ok);
    EXPECT_NE(error.find("last_hlc"), std::string::npos);
}

// ── PeerRegistryData::from_json with missing fields → descriptive error ───────

TEST(PeerRegistryUnit, PeerRegistryDataFromJsonMissingVersion) {
    ob::PeerRegistryData out;
    std::string error;
    bool ok = ob::PeerRegistryData::from_json(
        R"({"peers":{},"topology":"full-mesh"})", out, error);
    EXPECT_FALSE(ok);
    EXPECT_NE(error.find("version"), std::string::npos);
}

TEST(PeerRegistryUnit, PeerRegistryDataFromJsonMissingPeers) {
    ob::PeerRegistryData out;
    std::string error;
    bool ok = ob::PeerRegistryData::from_json(
        R"({"version":1,"topology":"full-mesh"})", out, error);
    EXPECT_FALSE(ok);
    EXPECT_NE(error.find("peers"), std::string::npos);
}

TEST(PeerRegistryUnit, PeerRegistryDataFromJsonMissingTopology) {
    ob::PeerRegistryData out;
    std::string error;
    bool ok = ob::PeerRegistryData::from_json(
        R"({"version":1,"peers":{}})", out, error);
    EXPECT_FALSE(ok);
    EXPECT_NE(error.find("topology"), std::string::npos);
}

TEST(PeerRegistryUnit, PeerRegistryDataFromJsonInvalidPeerEntry) {
    ob::PeerRegistryData out;
    std::string error;
    bool ok = ob::PeerRegistryData::from_json(
        R"({"version":1,"peers":{"1":{"node_id":1}},"topology":"full-mesh"})",
        out, error);
    EXPECT_FALSE(ok);
    // Should mention the problematic peer key.
    EXPECT_NE(error.find("peers['1']"), std::string::npos);
}

// ── to_json_pretty() produces readable JSON with indentation ──────────────────

TEST(PeerRegistryUnit, PrettyPrintHasIndentation) {
    ob::PeerRegistryData data;
    data.version  = 42;
    data.topology = "full-mesh";

    ob::PeerInfo peer;
    peer.node_id         = 1;
    peer.address         = "10.0.0.1:7001";
    peer.status          = "active";
    peer.last_hlc        = ob::HLCTimestamp{1700000000000000000ULL, 5, 1};
    peer.wal_file_index  = 3;
    peer.wal_byte_offset = 1024;
    data.peers[1] = peer;

    const std::string pretty = data.to_json_pretty();

    // Pretty-printed JSON should contain newlines and spaces for indentation.
    EXPECT_NE(pretty.find('\n'), std::string::npos);
    EXPECT_NE(pretty.find("    "), std::string::npos);

    // Should still be valid JSON that round-trips.
    ob::PeerRegistryData parsed;
    std::string parse_error;
    EXPECT_TRUE(ob::PeerRegistryData::from_json(pretty, parsed, parse_error))
        << parse_error;
    EXPECT_EQ(parsed, data);
}

// ── mm_peers_range_end() generates correct range-end for etcd watch ───────────

TEST(PeerRegistryUnit, RangeEndNoShard) {
    const std::string prefix = "/ob/";
    const std::string range_end = ob::mm_peers_range_end(prefix);

    // The range-end should be the prefix with the last byte incremented.
    // "/ob/mm_peers/" → "/ob/mm_peers0" (since '/' + 1 == '0')
    EXPECT_EQ(range_end, "/ob/mm_peers0");

    // The range-end should be strictly greater than any key under mm_peers/.
    const std::string key_0 = ob::mm_peer_key(prefix, 0);
    const std::string key_max = ob::mm_peer_key(prefix, 65535);
    EXPECT_LT(key_0, range_end);
    EXPECT_LT(key_max, range_end);
}

TEST(PeerRegistryUnit, RangeEndWithShard) {
    const std::string prefix   = "/ob/";
    const std::string shard_id = "shard-a";
    const std::string range_end = ob::mm_peers_range_end(prefix, shard_id);

    EXPECT_EQ(range_end, "/ob/shards/shard-a/mm_peers0");

    const std::string key_0 = ob::mm_peer_key(prefix, shard_id, 0);
    const std::string key_max = ob::mm_peer_key(prefix, shard_id, 65535);
    EXPECT_LT(key_0, range_end);
    EXPECT_LT(key_max, range_end);
}

// ── PeerInfo round-trip (single peer) ─────────────────────────────────────────

TEST(PeerRegistryUnit, PeerInfoRoundTrip) {
    ob::PeerInfo original;
    original.node_id         = 42;
    original.address         = "192.168.1.100:7001";
    original.status          = "joining";
    original.last_hlc        = ob::HLCTimestamp{1700000000000000000ULL, 10, 42};
    original.wal_file_index  = 7;
    original.wal_byte_offset = 524288;

    const std::string json_str = original.to_json();

    ob::PeerInfo parsed;
    std::string error;
    ASSERT_TRUE(ob::PeerInfo::from_json(json_str, parsed, error)) << error;
    EXPECT_EQ(parsed, original);
}
