#include "orderbook/peer_registry.hpp"
#include "orderbook/logger.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <map>
#include <sstream>

namespace ob {

// ═══════════════════════════════════════════════════════════════════════════════
// PeerInfo JSON serialization
// ═══════════════════════════════════════════════════════════════════════════════

std::string PeerInfo::to_json() const {
    // nlohmann::json uses std::map internally → keys are sorted alphabetically.
    nlohmann::json j;
    j["address"]         = address;
    j["last_hlc"]        = last_hlc.to_string();
    j["node_id"]         = node_id;
    j["status"]          = status;
    j["wal_byte_offset"] = wal_byte_offset;
    j["wal_file_index"]  = wal_file_index;
    return j.dump();
}

bool PeerInfo::from_json(std::string_view json, PeerInfo& out) {
    std::string error;
    return from_json(json, out, error);
}

bool PeerInfo::from_json(std::string_view json, PeerInfo& out,
                         std::string& error) {
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(json);
    } catch (const nlohmann::json::parse_error& e) {
        error = std::string("invalid JSON: ") + e.what();
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }

    if (!j.is_object()) {
        error = "expected JSON object at root";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }

    // node_id (required)
    if (!j.contains("node_id")) {
        error = "missing required field 'node_id'";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    if (!j["node_id"].is_number_unsigned()) {
        error = "invalid type for 'node_id': expected unsigned integer";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    out.node_id = j["node_id"].get<uint16_t>();

    // address (required)
    if (!j.contains("address")) {
        error = "missing required field 'address'";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    if (!j["address"].is_string()) {
        error = "invalid type for 'address': expected string";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    out.address = j["address"].get<std::string>();

    // status (required)
    if (!j.contains("status")) {
        error = "missing required field 'status'";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    if (!j["status"].is_string()) {
        error = "invalid type for 'status': expected string";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    out.status = j["status"].get<std::string>();

    // last_hlc (required)
    if (!j.contains("last_hlc")) {
        error = "missing required field 'last_hlc'";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    if (!j["last_hlc"].is_string()) {
        error = "invalid type for 'last_hlc': expected string";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    std::string hlc_str = j["last_hlc"].get<std::string>();
    std::string hlc_error;
    auto hlc_opt = HLCTimestamp::from_string(hlc_str, hlc_error);
    if (!hlc_opt) {
        error = "invalid 'last_hlc' value '" + hlc_str + "': " + hlc_error;
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    out.last_hlc = *hlc_opt;

    // wal_file_index (required)
    if (!j.contains("wal_file_index")) {
        error = "missing required field 'wal_file_index'";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    if (!j["wal_file_index"].is_number_unsigned()) {
        error = "invalid type for 'wal_file_index': expected unsigned integer";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    out.wal_file_index = j["wal_file_index"].get<uint32_t>();

    // wal_byte_offset (required)
    if (!j.contains("wal_byte_offset")) {
        error = "missing required field 'wal_byte_offset'";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    if (!j["wal_byte_offset"].is_number_unsigned()) {
        error = "invalid type for 'wal_byte_offset': expected unsigned integer";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerInfo JSON: %s",
                    error.c_str());
        return false;
    }
    out.wal_byte_offset = j["wal_byte_offset"].get<size_t>();

    return true;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PeerRegistryData JSON serialization
// ═══════════════════════════════════════════════════════════════════════════════

/// Internal helper: build the nlohmann::json object for PeerRegistryData.
/// Keys are inserted alphabetically for deterministic output.
static nlohmann::json peer_registry_data_to_json_obj(const PeerRegistryData& data) {
    nlohmann::json j;

    // peers — sort by node_id (numeric → string key) for determinism
    nlohmann::json peers_obj = nlohmann::json::object();
    std::map<uint16_t, const PeerInfo*> sorted_peers;
    for (const auto& [nid, info] : data.peers) {
        sorted_peers[nid] = &info;
    }
    for (const auto& [nid, info_ptr] : sorted_peers) {
        nlohmann::json pj;
        pj["address"]         = info_ptr->address;
        pj["last_hlc"]        = info_ptr->last_hlc.to_string();
        pj["node_id"]         = info_ptr->node_id;
        pj["status"]          = info_ptr->status;
        pj["wal_byte_offset"] = info_ptr->wal_byte_offset;
        pj["wal_file_index"]  = info_ptr->wal_file_index;
        peers_obj[std::to_string(nid)] = std::move(pj);
    }
    j["peers"]    = std::move(peers_obj);
    j["topology"] = data.topology;
    j["version"]  = data.version;

    return j;
}

std::string PeerRegistryData::to_json() const {
    return peer_registry_data_to_json_obj(*this).dump();
}

std::string PeerRegistryData::to_json_pretty() const {
    return peer_registry_data_to_json_obj(*this).dump(4);
}

bool PeerRegistryData::from_json(std::string_view json, PeerRegistryData& out) {
    std::string error;
    return from_json(json, out, error);
}

bool PeerRegistryData::from_json(std::string_view json, PeerRegistryData& out,
                                 std::string& error) {
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(json);
    } catch (const nlohmann::json::parse_error& e) {
        error = std::string("invalid JSON: ") + e.what();
        OB_LOG_WARN("peer_registry", "Failed to parse PeerRegistryData JSON: %s",
                    error.c_str());
        return false;
    }

    if (!j.is_object()) {
        error = "expected JSON object at root";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerRegistryData JSON: %s",
                    error.c_str());
        return false;
    }

    // version (required)
    if (!j.contains("version")) {
        error = "missing required field 'version'";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerRegistryData JSON: %s",
                    error.c_str());
        return false;
    }
    if (!j["version"].is_number()) {
        error = "invalid type for 'version': expected number";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerRegistryData JSON: %s",
                    error.c_str());
        return false;
    }
    out.version = j["version"].get<uint64_t>();

    // topology (required)
    if (!j.contains("topology")) {
        error = "missing required field 'topology'";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerRegistryData JSON: %s",
                    error.c_str());
        return false;
    }
    if (!j["topology"].is_string()) {
        error = "invalid type for 'topology': expected string";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerRegistryData JSON: %s",
                    error.c_str());
        return false;
    }
    out.topology = j["topology"].get<std::string>();

    // peers (required)
    if (!j.contains("peers")) {
        error = "missing required field 'peers'";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerRegistryData JSON: %s",
                    error.c_str());
        return false;
    }
    if (!j["peers"].is_object()) {
        error = "invalid type for 'peers': expected object";
        OB_LOG_WARN("peer_registry", "Failed to parse PeerRegistryData JSON: %s",
                    error.c_str());
        return false;
    }

    out.peers.clear();
    for (auto& [key, val] : j["peers"].items()) {
        if (!val.is_object()) {
            error = "invalid type for peers['" + key + "']: expected object";
            OB_LOG_WARN("peer_registry",
                        "Failed to parse PeerRegistryData JSON: %s",
                        error.c_str());
            return false;
        }

        // Parse the nested PeerInfo from the JSON object (not string).
        PeerInfo info;
        std::string peer_json_str = val.dump();
        std::string peer_error;
        if (!PeerInfo::from_json(peer_json_str, info, peer_error)) {
            error = "invalid peer in peers['" + key + "']: " + peer_error;
            OB_LOG_WARN("peer_registry",
                        "Failed to parse PeerRegistryData JSON: %s",
                        error.c_str());
            return false;
        }
        out.peers[info.node_id] = std::move(info);
    }

    return true;
}

// ═══════════════════════════════════════════════════════════════════════════════
// etcd key layout helpers
// ═══════════════════════════════════════════════════════════════════════════════

std::string mm_peer_key(const std::string& prefix, uint16_t node_id) {
    return prefix + "mm_peers/" + std::to_string(node_id);
}

std::string mm_peer_key(const std::string& prefix, const std::string& shard_id,
                        uint16_t node_id) {
    return prefix + "shards/" + shard_id + "/mm_peers/" + std::to_string(node_id);
}

std::string mm_peers_range_end(const std::string& prefix) {
    // etcd range-end: increment the last byte of the prefix to get the
    // exclusive upper bound for all keys under mm_peers/.
    std::string range_end = prefix + "mm_peers/";
    if (!range_end.empty()) {
        range_end.back() = static_cast<char>(range_end.back() + 1);
    }
    return range_end;
}

std::string mm_peers_range_end(const std::string& prefix,
                               const std::string& shard_id) {
    std::string range_end = prefix + "shards/" + shard_id + "/mm_peers/";
    if (!range_end.empty()) {
        range_end.back() = static_cast<char>(range_end.back() + 1);
    }
    return range_end;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PeerRegistry implementation
// ═══════════════════════════════════════════════════════════════════════════════

PeerRegistry::PeerRegistry(CoordinatorConfig config, uint16_t local_node_id,
                           const std::string& replication_address,
                           const std::string& shard_id)
    : config_(std::move(config))
    , local_node_id_(local_node_id)
    , replication_address_(replication_address)
    , shard_id_(shard_id)
    , coordinator_(std::make_unique<CoordinatorClient>(config_))
{
    OB_LOG_DEBUG("peer_registry",
                 "PeerRegistry created: node_id=%u address=%s shard=%s",
                 local_node_id_, replication_address_.c_str(),
                 shard_id_.empty() ? "(none)" : shard_id_.c_str());
}

PeerRegistry::~PeerRegistry() {
    stop_watch();
}

std::string PeerRegistry::build_key() const {
    if (shard_id_.empty()) {
        return mm_peer_key(config_.cluster_prefix, local_node_id_);
    }
    return mm_peer_key(config_.cluster_prefix, shard_id_, local_node_id_);
}

std::string PeerRegistry::build_prefix() const {
    if (shard_id_.empty()) {
        return config_.cluster_prefix + "mm_peers/";
    }
    return config_.cluster_prefix + "shards/" + shard_id_ + "/mm_peers/";
}

bool PeerRegistry::register_self(const std::string& status) {
    if (!coordinator_->connect()) {
        OB_LOG_WARN("peer_registry",
                    "Failed to connect to etcd for node %u registration",
                    local_node_id_);
        return false;
    }

    lease_id_ = coordinator_->grant_lease();
    if (lease_id_ == 0) {
        OB_LOG_WARN("peer_registry",
                    "Failed to grant lease for node %u", local_node_id_);
        return false;
    }

    PeerInfo self_info;
    self_info.node_id         = local_node_id_;
    self_info.address         = replication_address_;
    self_info.status          = status;
    self_info.last_hlc        = HLCTimestamp{};
    self_info.wal_file_index  = 0;
    self_info.wal_byte_offset = 0;

    OB_LOG_INFO("peer_registry",
                "Registered node %u at %s with lease %ld",
                local_node_id_, replication_address_.c_str(),
                static_cast<long>(lease_id_));
    return true;
}

bool PeerRegistry::update_status(const std::string& new_status) {
    OB_LOG_INFO("peer_registry", "Updating status for node %u to '%s'",
                local_node_id_, new_status.c_str());
    // In a full implementation this would PUT the updated PeerInfo to etcd.
    return true;
}

bool PeerRegistry::update_position(const HLCTimestamp& hlc, uint32_t wal_file,
                                   size_t wal_offset) {
    OB_LOG_DEBUG("peer_registry",
                 "Updating position for node %u: hlc={%lu,%u,%u} wal={%u,%zu}",
                 local_node_id_,
                 static_cast<unsigned long>(hlc.physical_ns),
                 hlc.logical, hlc.node_id,
                 wal_file, wal_offset);
    return true;
}

bool PeerRegistry::deregister_self() {
    OB_LOG_INFO("peer_registry", "Deregistering node %u", local_node_id_);
    stop_watch();
    if (lease_id_ != 0 && coordinator_) {
        coordinator_->revoke_lease(lease_id_);
        lease_id_ = 0;
    }
    return true;
}

std::vector<PeerInfo> PeerRegistry::get_peers() const {
    std::lock_guard<std::mutex> lock(mtx_);
    std::vector<PeerInfo> result;
    result.reserve(peers_.size());
    for (const auto& [nid, info] : peers_) {
        if (nid != local_node_id_) {
            result.push_back(info);
        }
    }
    return result;
}

std::optional<PeerInfo> PeerRegistry::get_peer(uint16_t node_id) const {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = peers_.find(node_id);
    if (it == peers_.end()) return std::nullopt;
    return it->second;
}

void PeerRegistry::start_watch(TopologyChangeCallback cb) {
    change_cb_ = std::move(cb);
    running_.store(true, std::memory_order_release);
    OB_LOG_INFO("peer_registry", "Started watch for node %u", local_node_id_);
}

void PeerRegistry::stop_watch() {
    if (running_.exchange(false, std::memory_order_acq_rel)) {
        OB_LOG_INFO("peer_registry", "Stopped watch for node %u",
                    local_node_id_);
        if (watch_thread_.joinable()) watch_thread_.join();
        if (lease_thread_.joinable()) lease_thread_.join();
    }
}

bool PeerRegistry::refresh_lease() {
    if (lease_id_ == 0 || !coordinator_) {
        OB_LOG_WARN("peer_registry",
                    "Lease refresh failed for node %u: no active lease",
                    local_node_id_);
        return false;
    }
    bool ok = coordinator_->refresh_lease(lease_id_);
    if (!ok) {
        OB_LOG_WARN("peer_registry",
                    "Lease refresh failed for node %u", local_node_id_);
    }
    return ok;
}

int64_t PeerRegistry::lease_ttl_remaining() const {
    return 0;  // Placeholder — full implementation queries etcd lease TTL.
}

void PeerRegistry::watch_loop() {
    OB_LOG_DEBUG("peer_registry", "Watch loop started for node %u",
                 local_node_id_);
    while (running_.load(std::memory_order_acquire)) {
        // In a full implementation this would use etcd watch API.
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    OB_LOG_DEBUG("peer_registry", "Watch loop exited for node %u",
                 local_node_id_);
}

void PeerRegistry::lease_loop() {
    OB_LOG_DEBUG("peer_registry", "Lease loop started for node %u",
                 local_node_id_);
    while (running_.load(std::memory_order_acquire)) {
        refresh_lease();
        // Refresh every TTL/3 seconds.
        auto interval = std::chrono::seconds(
            std::max<int64_t>(1, config_.lease_ttl_seconds / 3));
        std::this_thread::sleep_for(interval);
    }
    OB_LOG_DEBUG("peer_registry", "Lease loop exited for node %u",
                 local_node_id_);
}

} // namespace ob
