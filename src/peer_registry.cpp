#include "orderbook/peer_registry.hpp"
#include "orderbook/thread_boundary.hpp"
#include "orderbook/logger.hpp"

#include <nlohmann/json.hpp>
#include <curl/curl.h>

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
                           MetricsRegistry& registry,
                           const std::string& shard_id)
    : config_(std::move(config))
    , local_node_id_(local_node_id)
    , replication_address_(replication_address)
    , shard_id_(shard_id)
    , coordinator_(std::make_unique<CoordinatorClient>(config_))
    , registry_(registry)
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

    // PUT PeerInfo JSON to etcd with lease.
    std::string key = build_key();
    std::string value = self_info.to_json();
    std::string key_b64 = base64_encode(key);
    std::string value_b64 = base64_encode(value);

    std::string url = config_.endpoints[0] + "/v3/kv/put";
    std::string body = "{\"key\":\"" + key_b64 +
                       "\",\"value\":\"" + value_b64 +
                       "\",\"lease\":\"" + std::to_string(lease_id_) + "\"}";

    // Use a simple curl request to PUT.
    CURL* curl = curl_easy_init();
    if (curl) {
        std::string response;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
            +[](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
                auto* resp = static_cast<std::string*>(userdata);
                resp->append(ptr, size * nmemb);
                return size * nmemb;
            });
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        CURLcode res = curl_easy_perform(curl);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            OB_LOG_WARN("peer_registry",
                        "Failed to PUT PeerInfo to etcd for node %u: %s",
                        local_node_id_, curl_easy_strerror(res));
            return false;
        }
    }

    // Store self in local peers map.
    {
        std::lock_guard<std::mutex> lock(mtx_);
        peers_[local_node_id_] = self_info;
    }

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

    // Start watch thread that polls etcd for peer changes.
    watch_thread_ = std::thread([this] {
        run_thread_body("peer_registry", "watch_loop", [this] { watch_loop(); });
    });

    // Start lease keep-alive thread.
    lease_thread_ = std::thread([this] {
        run_thread_body("peer_registry", "lease_loop", [this] { lease_loop(); });
    });

    OB_LOG_INFO("peer_registry", "Started watch for node %u", local_node_id_);
}

void PeerRegistry::stop_watch() {
    if (running_.exchange(false, std::memory_order_acq_rel)) {
        OB_LOG_INFO("peer_registry", "Stopped watch for node %u",
                    local_node_id_);
        // Notified after the flag is stored and before either join, because the predicate reads the
        // flag: a notification that arrives first is a notification the waiter sleeps through.
        lease_stop_cv_.notify_all();
        if (watch_thread_.joinable()) watch_thread_.join();
        if (lease_thread_.joinable()) lease_thread_.join();
    }
}

bool PeerRegistry::refresh_lease() {
    // Loud once and then quiet, for both ways this can fail. The lease loop calls this every
    // TTL/3 and neither failure is transient in practice: a lease etcd has forgotten stays
    // forgotten, and a registration that never happened leaves `lease_id_` at 0 for the life of
    // the process. Measured before this episode existed, with the lease revoked under a running
    // two-node mesh: eleven of these in 33 s, one per interval, for ever - #95's shape, and the
    // same defect #116 closed for the position publisher and #120 for the clock (#133).
    //
    // The episode is this object's, so a caller other than the lease loop still gets the first
    // line. It is not atomic because the lease thread is the only caller in this tree, which is
    // what `LogEpisode` asks of its users.
    const bool ok = (lease_id_ != 0 && coordinator_) && coordinator_->refresh_lease(lease_id_);
    if (!ok) {
        // Which of the two it is, in the line that opens the episode - they ask for different
        // things. No lease at all means `register_self()` failed or was never called; a refused
        // keepalive means the lease existed and is gone, and the node is out of the mesh registry
        // until something re-registers it, which nothing does (#132).
        if (lease_refusals_.begin()) {
            OB_LOG_WARN("peer_registry",
                        "Lease refresh failed for node %u%s - this node's mesh registration "
                        "expires with the lease and nothing re-registers it", local_node_id_,
                        lease_id_ == 0 ? ": no active lease" : "");
        } else {
            OB_LOG_DEBUG("peer_registry",
                         "Lease refresh for node %u still failing (%llu consecutive)",
                         local_node_id_,
                         static_cast<unsigned long long>(lease_refusals_.ticks()));
        }
        return false;
    }
    if (const uint64_t held = lease_refusals_.end()) {
        OB_LOG_INFO("peer_registry",
                    "node %u's lease is being refreshed again after %llu refusal(s)",
                    local_node_id_, static_cast<unsigned long long>(held));
    }
    return true;
}

int64_t PeerRegistry::lease_ttl_remaining() const {
    return 0;  // Placeholder — full implementation queries etcd lease TTL.
}

void PeerRegistry::watch_loop() {
    OB_LOG_DEBUG("peer_registry", "Watch loop started for node %u",
                 local_node_id_);

    while (running_.load(std::memory_order_acquire)) {
        // Poll etcd for all peer keys under our prefix.
        std::string prefix = build_prefix();
        std::string range_end = prefix;
        if (!range_end.empty()) {
            range_end.back() = static_cast<char>(range_end.back() + 1);
        }

        std::string key_b64 = base64_encode(prefix);
        std::string end_b64 = base64_encode(range_end);

        std::string url = config_.endpoints[0] + "/v3/kv/range";
        std::string body = "{\"key\":\"" + key_b64 +
                           "\",\"range_end\":\"" + end_b64 + "\"}";

        CURL* curl = curl_easy_init();
        std::string response;
        bool success = false;

        if (curl) {
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                +[](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
                    auto* resp = static_cast<std::string*>(userdata);
                    resp->append(ptr, size * nmemb);
                    return size * nmemb;
                });
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3L);
            struct curl_slist* headers = nullptr;
            headers = curl_slist_append(headers, "Content-Type: application/json");
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

            CURLcode res = curl_easy_perform(curl);
            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);

            success = (res == CURLE_OK && !response.empty());
        }

        if (success) {
            // Parse the range response to extract peer info.
            // etcd v3 range response format:
            // {"header":{...},"kvs":[{"key":"<b64>","value":"<b64>","..."},...],"count":"N"}
            std::vector<PeerInfo> discovered_peers;

            // Simple parsing: find all "value":"<base64>" entries.
            size_t pos = 0;
            while (true) {
                pos = response.find("\"value\":\"", pos);
                if (pos == std::string::npos) break;
                pos += 9; // skip "value":"
                size_t end = response.find('"', pos);
                if (end == std::string::npos) break;

                std::string value_b64 = response.substr(pos, end - pos);
                std::string value_json = base64_decode(value_b64);

                PeerInfo info;
                if (PeerInfo::from_json(value_json, info)) {
                    discovered_peers.push_back(info);
                }
                pos = end + 1;
            }

            // Update local peers map and notify callback if changed.
            bool changed = false;
            {
                std::lock_guard<std::mutex> lock(mtx_);
                std::unordered_map<uint16_t, PeerInfo> new_peers;
                for (auto& p : discovered_peers) {
                    new_peers[p.node_id] = p;
                }

                if (new_peers != peers_) {
                    peers_ = std::move(new_peers);
                    changed = true;
                }
            }

            if (changed && change_cb_) {
                // Build peer list excluding self.
                std::vector<PeerInfo> peer_list;
                for (const auto& p : discovered_peers) {
                    if (p.node_id != local_node_id_) {
                        peer_list.push_back(p);
                    }
                }
                OB_LOG_INFO("peer_registry",
                            "Topology change detected: %zu peers (excluding self)",
                            peer_list.size());
                change_cb_(peer_list);
            }
        }

        // Poll every 1 second.
        for (int i = 0; i < 10 && running_.load(std::memory_order_acquire); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    OB_LOG_DEBUG("peer_registry", "Watch loop exited for node %u",
                 local_node_id_);
}

void PeerRegistry::lease_loop() {
    OB_LOG_DEBUG("peer_registry", "Lease loop started for node %u",
                 local_node_id_);
    while (running_.load(std::memory_order_acquire)) {
        // One refresh at a time under its own boundary (#112). The outer `run_thread_body` keeps
        // the *process* alive; without this, one exception here ends the thread that holds this
        // node's mesh registration open, and what follows is measured rather than argued: the
        // registration expires with its lease, the key is gone from etcd for good, and nothing
        // ever puts it back — `register_self()` runs once, at start, and is the only writer of
        // `lease_id_`. The node keeps answering `PING` throughout.
        //
        // A **ratchet**, and it says so rather than pretending to close something measured. No
        // path in this tree throws here: `CoordinatorClient` contains no `throw` at all and
        // `refresh_lease()` answers failure with `false`, so what is left is `std::bad_alloc`
        // from the string building underneath and `std::system_error` from the wait below.
        try {
            refresh_lease();
        } catch (const std::exception& e) {
            registry_.increment_counter("ob_peer_lease_errors_total");
            if (lease_errors_.begin()) {
                OB_LOG_ERROR("peer_registry",
                             "refreshing node %u's lease threw and the next interval will be "
                             "attempted; if this keeps failing the lease expires and this node "
                             "leaves the mesh registry: %s", local_node_id_, e.what());
            } else {
                OB_LOG_DEBUG("peer_registry",
                             "refreshing node %u's lease threw again (%llu consecutive): %s",
                             local_node_id_,
                             static_cast<unsigned long long>(lease_errors_.ticks()), e.what());
            }
        }

        // Inside the loop and after the boundary, so a refresh that threw cannot claim its own
        // recovery — the mistake the mesh boundary made one commit earlier, where `end()` ran
        // after the code that opened the episode and the log alternated ERROR / "again".
        if (const uint64_t held = lease_errors_.end()) {
            OB_LOG_INFO("peer_registry",
                        "node %u's lease is being refreshed again after %llu attempt(s) that "
                        "threw", local_node_id_, static_cast<unsigned long long>(held));
        }

        // Refresh every TTL/3 seconds, on a wait `stop_watch()` can end. A plain `sleep_for()` here
        // made shutdown wait out the rest of the current interval, because `join()` cannot
        // interrupt a sleeping thread — measured at 2.94 s for the default TTL against 0.22 s for a
        // node with no lease loop at all (#129).
        const auto interval = std::chrono::seconds(
            std::max<int64_t>(1, config_.lease_ttl_seconds / 3));
        std::unique_lock<std::mutex> lock(lease_stop_mtx_);
        if (lease_stop_cv_.wait_for(lock, interval, [this] {
                return !running_.load(std::memory_order_acquire);
            })) {
            OB_LOG_DEBUG("peer_registry", "Lease loop woken to stop for node %u", local_node_id_);
            break;
        }
    }
    OB_LOG_DEBUG("peer_registry", "Lease loop exited for node %u",
                 local_node_id_);
}

} // namespace ob
