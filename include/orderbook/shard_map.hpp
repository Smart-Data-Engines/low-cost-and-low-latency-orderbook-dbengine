#pragma once

// ── ShardMap, ConsistentHashRing, ShardNode ──────────────────────────────────
//
// Core data structures for symbol-based sharding.  ShardMap holds the
// authoritative mapping of symbol keys ("symbol.exchange") to shard IDs,
// stored centrally in etcd.  ConsistentHashRing provides the hashing layer
// that distributes symbols across shards with minimal movement on topology
// changes (virtual-node consistent hashing, MurmurHash3_x86_32, ring 2^32).

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace ob {

// ── Shard node status ─────────────────────────────────────────────────────────

enum class ShardStatus : uint8_t {
    ACTIVE   = 0,  // normal operating mode
    JOINING  = 1,  // shard joining cluster, waiting for rebalancing
    DRAINING = 2,  // shard leaving cluster, migration in progress
};

// ── Shard node descriptor ─────────────────────────────────────────────────────

struct ShardNode {
    std::string shard_id;                       // e.g. "shard-0"
    std::string address;                        // "host:port" — client TCP address
    ShardStatus status{ShardStatus::ACTIVE};
    uint32_t    vnodes{150};                    // virtual node count in hash ring

    /// Serialize to deterministic JSON (sorted keys).
    std::string to_json() const;

    /// Deserialize from JSON.  Returns true on success.
    static bool from_json(std::string_view json, ShardNode& out);
};

// ── Symbol migration state ────────────────────────────────────────────────────

struct MigrationState {
    std::string symbol_key;         // "symbol.exchange"
    std::string source_shard_id;
    std::string target_shard_id;
    uint8_t     progress_pct{0};    // 0-100
};

// ── Shard Map ─────────────────────────────────────────────────────────────────

/// Central symbol-to-shard assignment map.
/// Stored in etcd under key <prefix>shard_map.
/// Versioned — every change increments version.
struct ShardMap {
    uint64_t version{0};

    /// Mapping symbol_key ("symbol.exchange") → shard_id
    std::map<std::string, std::string> assignments;

    /// Registered shards
    std::map<std::string, ShardNode> shards;

    /// Symbols manually pinned to shards (excluded from rebalancing)
    std::unordered_set<std::string> pinned_symbols;

    /// Active migrations
    std::vector<MigrationState> active_migrations;

    /// Serialize to JSON (deterministic — sorted keys).
    std::string to_json() const;

    /// Deserialize from JSON.  Returns true on success.
    static bool from_json(std::string_view json, ShardMap& out);

    /// Deserialize from JSON with descriptive error message.
    static bool from_json(std::string_view json, ShardMap& out, std::string& error);
};

// ── Consistent Hash Ring ──────────────────────────────────────────────────────

/// Consistent hashing ring with virtual nodes.
/// Algorithm: MurmurHash3_x86_32 on key, ring 2^32.
/// Each shard has `vnodes` virtual nodes distributed on the ring.
class ConsistentHashRing {
public:
    ConsistentHashRing() = default;

    /// Add a shard to the ring with the given number of virtual nodes.
    void add_shard(const std::string& shard_id, uint32_t vnodes = 150);

    /// Remove a shard from the ring.
    void remove_shard(const std::string& shard_id);

    /// Find the shard responsible for the given key.
    /// Returns empty string if the ring is empty.
    std::string lookup(std::string_view key) const;

    /// Return the number of shards in the ring.
    size_t shard_count() const;

    /// Return the total number of virtual nodes in the ring.
    size_t vnode_count() const;

    /// Compute assignments for a set of symbol keys.
    /// Returns map symbol_key → shard_id.
    std::unordered_map<std::string, std::string>
    compute_assignments(const std::vector<std::string>& symbol_keys) const;

    /// Compute assignment delta after topology change.
    /// Returns symbols that changed shard.
    std::vector<std::pair<std::string, std::string>>
    compute_reassignments(const std::unordered_map<std::string, std::string>& old_assignments,
                          const std::vector<std::string>& symbol_keys) const;

private:
    /// Ring: sorted map hash_value → shard_id
    std::map<uint32_t, std::string> ring_;

    /// Track vnodes per shard (for removal)
    std::unordered_map<std::string, std::vector<uint32_t>> shard_vnodes_;

    /// MurmurHash3_x86_32
    static uint32_t hash(std::string_view key);
};

// ── Shard Map diff ────────────────────────────────────────────────────────────

/// Diff between two ShardMaps.
/// Lists symbols that were added, removed, or moved between shards.
struct ShardMapDiff {
    std::vector<std::pair<std::string, std::string>> added;    // symbol → new_shard
    std::vector<std::pair<std::string, std::string>> removed;  // symbol → old_shard
    std::vector<std::tuple<std::string, std::string, std::string>> moved; // symbol, old_shard, new_shard
};

/// Compute the diff between two ShardMaps.
ShardMapDiff compute_shard_map_diff(const ShardMap& old_map, const ShardMap& new_map);

} // namespace ob
