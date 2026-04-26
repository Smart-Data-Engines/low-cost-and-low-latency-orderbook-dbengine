#include "orderbook/shard_map.hpp"
#include "orderbook/logger.hpp"

#include <nlohmann/json.hpp>

#include <cstring>

namespace ob {

// ── MurmurHash3_x86_32 ───────────────────────────────────────────────────────
//
// Public-domain reference implementation by Austin Appleby.
// Produces a 32-bit hash suitable for the consistent-hashing ring (2^32).

static inline uint32_t rotl32(uint32_t x, int8_t r) noexcept {
    return (x << r) | (x >> (32 - r));
}

static inline uint32_t fmix32(uint32_t h) noexcept {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

uint32_t ConsistentHashRing::hash(std::string_view key) {
    const auto* data = reinterpret_cast<const uint8_t*>(key.data());
    const auto  len  = static_cast<int>(key.size());

    const int      nblocks = len / 4;
    const uint32_t seed    = 0;  // fixed seed for determinism

    uint32_t h1 = seed;

    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    // ── body ──────────────────────────────────────────────────────────
    const auto* blocks = reinterpret_cast<const uint32_t*>(data + nblocks * 4);
    // Walk backwards from the end of the aligned portion.
    for (int i = -nblocks; i; ++i) {
        uint32_t k1;
        std::memcpy(&k1, &blocks[i], sizeof(k1));

        k1 *= c1;
        k1  = rotl32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1  = rotl32(h1, 13);
        h1  = h1 * 5 + 0xe6546b64;
    }

    // ── tail ──────────────────────────────────────────────────────────
    const auto* tail = data + nblocks * 4;
    uint32_t k1 = 0;

    switch (len & 3) {
    case 3: k1 ^= static_cast<uint32_t>(tail[2]) << 16; [[fallthrough]];
    case 2: k1 ^= static_cast<uint32_t>(tail[1]) << 8;  [[fallthrough]];
    case 1: k1 ^= static_cast<uint32_t>(tail[0]);
            k1 *= c1;
            k1  = rotl32(k1, 15);
            k1 *= c2;
            h1 ^= k1;
    }

    // ── finalization ──────────────────────────────────────────────────
    h1 ^= static_cast<uint32_t>(len);
    h1  = fmix32(h1);
    return h1;
}

// ── ConsistentHashRing ────────────────────────────────────────────────────────

void ConsistentHashRing::add_shard(const std::string& shard_id, uint32_t vnodes) {
    OB_LOG_INFO("shard_map", "Adding shard %s with %u vnodes",
                shard_id.c_str(), vnodes);

    auto& vnode_hashes = shard_vnodes_[shard_id];
    vnode_hashes.reserve(vnodes);

    for (uint32_t i = 0; i < vnodes; ++i) {
        // Virtual node key: "shard_id#i"
        std::string vnode_key = shard_id + "#" + std::to_string(i);
        uint32_t h = hash(vnode_key);
        ring_[h] = shard_id;
        vnode_hashes.push_back(h);
    }
}

void ConsistentHashRing::remove_shard(const std::string& shard_id) {
    OB_LOG_INFO("shard_map", "Removing shard %s from ring", shard_id.c_str());

    auto it = shard_vnodes_.find(shard_id);
    if (it == shard_vnodes_.end()) return;

    for (uint32_t h : it->second) {
        ring_.erase(h);
    }
    shard_vnodes_.erase(it);
}

std::string ConsistentHashRing::lookup(std::string_view key) const {
    if (ring_.empty()) {
        return {};
    }

    uint32_t h = hash(key);

    // Find the first node with hash >= h (clockwise walk).
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) {
        // Wrap around to the first node on the ring.
        it = ring_.begin();
    }

    const std::string& result = it->second;
    OB_LOG_DEBUG("shard_map", "Lookup key=%.*s -> shard=%s",
                 static_cast<int>(key.size()), key.data(), result.c_str());
    return result;
}

size_t ConsistentHashRing::shard_count() const {
    return shard_vnodes_.size();
}

size_t ConsistentHashRing::vnode_count() const {
    return ring_.size();
}

std::unordered_map<std::string, std::string>
ConsistentHashRing::compute_assignments(const std::vector<std::string>& symbol_keys) const {
    std::unordered_map<std::string, std::string> assignments;
    assignments.reserve(symbol_keys.size());

    for (const auto& key : symbol_keys) {
        assignments[key] = lookup(key);
    }
    return assignments;
}

std::vector<std::pair<std::string, std::string>>
ConsistentHashRing::compute_reassignments(
    const std::unordered_map<std::string, std::string>& old_assignments,
    const std::vector<std::string>& symbol_keys) const
{
    std::vector<std::pair<std::string, std::string>> moved;

    for (const auto& key : symbol_keys) {
        std::string new_shard = lookup(key);
        auto it = old_assignments.find(key);
        if (it != old_assignments.end() && it->second != new_shard) {
            // Symbol changed shard: pair is (symbol_key, new_shard_id)
            moved.emplace_back(key, new_shard);
        }
    }
    return moved;
}

// ── ShardStatus string conversion ─────────────────────────────────────────────

static const char* shard_status_to_string(ShardStatus s) {
    switch (s) {
    case ShardStatus::ACTIVE:   return "active";
    case ShardStatus::JOINING:  return "joining";
    case ShardStatus::DRAINING: return "draining";
    }
    return "active";
}

static ShardStatus shard_status_from_string(std::string_view s) {
    if (s == "joining")  return ShardStatus::JOINING;
    if (s == "draining") return ShardStatus::DRAINING;
    return ShardStatus::ACTIVE;
}

// ── ShardNode JSON serialization ──────────────────────────────────────────────

std::string ShardNode::to_json() const {
    // Use nlohmann::json with sorted keys (std::map-backed object)
    nlohmann::json j;
    j["address"]  = address;
    j["shard_id"] = shard_id;
    j["status"]   = shard_status_to_string(status);
    j["vnodes"]   = vnodes;
    return j.dump();
}

bool ShardNode::from_json(std::string_view json, ShardNode& out) {
    try {
        auto j = nlohmann::json::parse(json);
        if (!j.is_object()) return false;
        if (!j.contains("shard_id") || !j["shard_id"].is_string()) return false;
        if (!j.contains("address")  || !j["address"].is_string())  return false;

        out.shard_id = j["shard_id"].get<std::string>();
        out.address  = j["address"].get<std::string>();
        out.status   = j.contains("status") && j["status"].is_string()
                           ? shard_status_from_string(j["status"].get<std::string>())
                           : ShardStatus::ACTIVE;
        out.vnodes   = j.contains("vnodes") && j["vnodes"].is_number_unsigned()
                           ? j["vnodes"].get<uint32_t>()
                           : 150;
        return true;
    } catch (...) {
        return false;
    }
}

// ── ShardMap JSON serialization ───────────────────────────────────────────────

std::string ShardMap::to_json() const {
    OB_LOG_DEBUG("shard_map", "Serializing ShardMap version=%lu shards=%zu symbols=%zu",
                 static_cast<unsigned long>(version), shards.size(), assignments.size());

    // Build JSON with deterministic key order.
    // nlohmann::json uses std::map internally for objects → keys are sorted.
    nlohmann::json j;

    // active_migrations
    nlohmann::json mig_arr = nlohmann::json::array();
    for (const auto& m : active_migrations) {
        nlohmann::json mj;
        mj["progress_pct"]     = m.progress_pct;
        mj["source_shard_id"]  = m.source_shard_id;
        mj["symbol_key"]       = m.symbol_key;
        mj["target_shard_id"]  = m.target_shard_id;
        mig_arr.push_back(std::move(mj));
    }
    j["active_migrations"] = std::move(mig_arr);

    // assignments (std::map → already sorted)
    nlohmann::json asgn = nlohmann::json::object();
    for (const auto& [sym, shard] : assignments) {
        asgn[sym] = shard;
    }
    j["assignments"] = std::move(asgn);

    // pinned_symbols — sorted for determinism
    std::vector<std::string> pinned_sorted(pinned_symbols.begin(), pinned_symbols.end());
    std::sort(pinned_sorted.begin(), pinned_sorted.end());
    j["pinned_symbols"] = std::move(pinned_sorted);

    // shards (std::map → already sorted)
    nlohmann::json sh = nlohmann::json::object();
    for (const auto& [id, node] : shards) {
        nlohmann::json nj;
        nj["address"]  = node.address;
        nj["shard_id"] = node.shard_id;
        nj["status"]   = shard_status_to_string(node.status);
        nj["vnodes"]   = node.vnodes;
        sh[id] = std::move(nj);
    }
    j["shards"] = std::move(sh);

    // version
    j["version"] = version;

    return j.dump();
}

bool ShardMap::from_json(std::string_view json, ShardMap& out) {
    std::string error;
    return from_json(json, out, error);
}

bool ShardMap::from_json(std::string_view json, ShardMap& out, std::string& error) {
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(json);
    } catch (const nlohmann::json::parse_error& e) {
        error = std::string("invalid JSON: ") + e.what();
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }

    if (!j.is_object()) {
        error = "expected JSON object at root";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }

    // version (required)
    if (!j.contains("version")) {
        error = "missing field 'version'";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    if (!j["version"].is_number()) {
        error = "invalid type for 'version': expected number";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    out.version = j["version"].get<uint64_t>();

    // assignments (required)
    if (!j.contains("assignments")) {
        error = "missing field 'assignments'";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    if (!j["assignments"].is_object()) {
        error = "invalid type for 'assignments': expected object";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    out.assignments.clear();
    for (auto& [key, val] : j["assignments"].items()) {
        if (!val.is_string()) {
            error = "invalid type for assignments['" + key + "']: expected string";
            OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
            return false;
        }
        out.assignments[key] = val.get<std::string>();
    }

    // shards (required)
    if (!j.contains("shards")) {
        error = "missing field 'shards'";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    if (!j["shards"].is_object()) {
        error = "invalid type for 'shards': expected object";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    out.shards.clear();
    for (auto& [key, val] : j["shards"].items()) {
        if (!val.is_object()) {
            error = "invalid type for shards['" + key + "']: expected object";
            OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
            return false;
        }
        ShardNode node;
        if (!val.contains("shard_id") || !val["shard_id"].is_string()) {
            error = "missing or invalid 'shard_id' in shards['" + key + "']";
            OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
            return false;
        }
        node.shard_id = val["shard_id"].get<std::string>();
        if (!val.contains("address") || !val["address"].is_string()) {
            error = "missing or invalid 'address' in shards['" + key + "']";
            OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
            return false;
        }
        node.address = val["address"].get<std::string>();
        node.status  = val.contains("status") && val["status"].is_string()
                           ? shard_status_from_string(val["status"].get<std::string>())
                           : ShardStatus::ACTIVE;
        node.vnodes  = val.contains("vnodes") && val["vnodes"].is_number_unsigned()
                           ? val["vnodes"].get<uint32_t>()
                           : 150;
        out.shards[key] = std::move(node);
    }

    // pinned_symbols (required)
    if (!j.contains("pinned_symbols")) {
        error = "missing field 'pinned_symbols'";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    if (!j["pinned_symbols"].is_array()) {
        error = "invalid type for 'pinned_symbols': expected array";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    out.pinned_symbols.clear();
    for (const auto& elem : j["pinned_symbols"]) {
        if (!elem.is_string()) {
            error = "invalid type in 'pinned_symbols': expected string";
            OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
            return false;
        }
        out.pinned_symbols.insert(elem.get<std::string>());
    }

    // active_migrations (required)
    if (!j.contains("active_migrations")) {
        error = "missing field 'active_migrations'";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    if (!j["active_migrations"].is_array()) {
        error = "invalid type for 'active_migrations': expected array";
        OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
        return false;
    }
    out.active_migrations.clear();
    for (const auto& elem : j["active_migrations"]) {
        if (!elem.is_object()) {
            error = "invalid type in 'active_migrations': expected object";
            OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
            return false;
        }
        MigrationState ms;
        if (!elem.contains("symbol_key") || !elem["symbol_key"].is_string()) {
            error = "missing or invalid 'symbol_key' in active_migrations entry";
            OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
            return false;
        }
        ms.symbol_key = elem["symbol_key"].get<std::string>();
        if (!elem.contains("source_shard_id") || !elem["source_shard_id"].is_string()) {
            error = "missing or invalid 'source_shard_id' in active_migrations entry";
            OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
            return false;
        }
        ms.source_shard_id = elem["source_shard_id"].get<std::string>();
        if (!elem.contains("target_shard_id") || !elem["target_shard_id"].is_string()) {
            error = "missing or invalid 'target_shard_id' in active_migrations entry";
            OB_LOG_WARN("shard_map", "Failed to parse ShardMap JSON: %s", error.c_str());
            return false;
        }
        ms.target_shard_id = elem["target_shard_id"].get<std::string>();
        ms.progress_pct = elem.contains("progress_pct") && elem["progress_pct"].is_number()
                              ? elem["progress_pct"].get<uint8_t>()
                              : 0;
        out.active_migrations.push_back(std::move(ms));
    }

    return true;
}

// ── ShardMap diff ──────────────────────────────────────────────────────────────

ShardMapDiff compute_shard_map_diff(const ShardMap& old_map, const ShardMap& new_map) {
    ShardMapDiff diff;

    // Added: symbols in new_map but not in old_map
    for (const auto& [sym, new_shard] : new_map.assignments) {
        auto it = old_map.assignments.find(sym);
        if (it == old_map.assignments.end()) {
            diff.added.emplace_back(sym, new_shard);
        } else if (it->second != new_shard) {
            // Moved: symbol exists in both but shard changed
            diff.moved.emplace_back(sym, it->second, new_shard);
        }
    }

    // Removed: symbols in old_map but not in new_map
    for (const auto& [sym, old_shard] : old_map.assignments) {
        if (new_map.assignments.find(sym) == new_map.assignments.end()) {
            diff.removed.emplace_back(sym, old_shard);
        }
    }

    OB_LOG_DEBUG("shard_map", "ShardMap diff: added=%zu removed=%zu moved=%zu",
                 diff.added.size(), diff.removed.size(), diff.moved.size());

    return diff;
}

} // namespace ob
