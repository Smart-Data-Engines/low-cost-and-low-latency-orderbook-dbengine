// ── CoordinatorClient — etcd v3 REST integration ─────────────────────────────

#include "orderbook/coordinator.hpp"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>

#include <curl/curl.h>

namespace ob {

// ── Base64 encode / decode ───────────────────────────────────────────────────
// etcd v3 REST API requires keys and values to be base64-encoded.

static constexpr char kBase64Table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64_encode(const std::string& input) {
    std::string out;
    out.reserve(((input.size() + 2) / 3) * 4);

    const auto* src = reinterpret_cast<const uint8_t*>(input.data());
    size_t len = input.size();
    size_t i = 0;

    while (i + 2 < len) {
        uint32_t triple = (static_cast<uint32_t>(src[i]) << 16) |
                          (static_cast<uint32_t>(src[i + 1]) << 8) |
                          static_cast<uint32_t>(src[i + 2]);
        out += kBase64Table[(triple >> 18) & 0x3F];
        out += kBase64Table[(triple >> 12) & 0x3F];
        out += kBase64Table[(triple >> 6) & 0x3F];
        out += kBase64Table[triple & 0x3F];
        i += 3;
    }

    if (i + 1 == len) {
        uint32_t val = static_cast<uint32_t>(src[i]) << 16;
        out += kBase64Table[(val >> 18) & 0x3F];
        out += kBase64Table[(val >> 12) & 0x3F];
        out += '=';
        out += '=';
    } else if (i + 2 == len) {
        uint32_t val = (static_cast<uint32_t>(src[i]) << 16) |
                       (static_cast<uint32_t>(src[i + 1]) << 8);
        out += kBase64Table[(val >> 18) & 0x3F];
        out += kBase64Table[(val >> 12) & 0x3F];
        out += kBase64Table[(val >> 6) & 0x3F];
        out += '=';
    }

    return out;
}

static uint8_t base64_char_value(char c) {
    if (c >= 'A' && c <= 'Z') return static_cast<uint8_t>(c - 'A');
    if (c >= 'a' && c <= 'z') return static_cast<uint8_t>(c - 'a' + 26);
    if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0' + 52);
    if (c == '+') return 62;
    if (c == '/') return 63;
    return 0;
}

std::string base64_decode(const std::string& input) {
    std::string out;
    if (input.empty()) return out;

    // Strip trailing '=' for length calculation.
    size_t len = input.size();
    size_t padding = 0;
    if (len >= 1 && input[len - 1] == '=') ++padding;
    if (len >= 2 && input[len - 2] == '=') ++padding;

    out.reserve((len / 4) * 3);

    for (size_t i = 0; i + 3 < len; i += 4) {
        uint32_t sextet =
            (static_cast<uint32_t>(base64_char_value(input[i])) << 18) |
            (static_cast<uint32_t>(base64_char_value(input[i + 1])) << 12) |
            (static_cast<uint32_t>(base64_char_value(input[i + 2])) << 6) |
            static_cast<uint32_t>(base64_char_value(input[i + 3]));

        out += static_cast<char>((sextet >> 16) & 0xFF);
        if (input[i + 2] != '=')
            out += static_cast<char>((sextet >> 8) & 0xFF);
        if (input[i + 3] != '=')
            out += static_cast<char>(sextet & 0xFF);
    }

    return out;
}

// ── etcd key layout helpers ──────────────────────────────────────────────────

std::string coordinator_leader_key(const std::string& prefix) {
    return prefix + "leader";
}

std::string coordinator_epoch_key(const std::string& prefix) {
    return prefix + "epoch";
}

std::string coordinator_node_key(const std::string& prefix,
                                 const std::string& node_id) {
    return prefix + "nodes/" + node_id;
}

std::string coordinator_nodes_range_end(const std::string& prefix) {
    // Range end for all keys under <prefix>nodes/ — increment last byte.
    std::string key = prefix + "nodes/";
    if (!key.empty()) {
        key.back() = static_cast<char>(key.back() + 1);
    }
    return key;
}

// ── JSON serialization — ClusterState ────────────────────────────────────────

std::string ClusterState::to_json() const {
    // Deterministic alphabetical field ordering.
    std::string out;
    out.reserve(128);

    out += "{\"address\":\"";
    out += leader_address;
    out += "\",\"epoch\":";
    out += std::to_string(epoch.term);
    out += ",\"node_id\":\"";
    out += leader_node_id;
    out += "\"}";

    return out;
}

bool ClusterState::from_json(std::string_view json, ClusterState& out) {
    out = {};

    auto extract_string = [&](const char* key) -> std::string {
        std::string search = std::string("\"") + key + "\":\"";
        auto pos = json.find(search);
        if (pos == std::string_view::npos) return {};
        pos += search.size();
        auto end = json.find('"', pos);
        if (end == std::string_view::npos) return {};
        return std::string(json.substr(pos, end - pos));
    };

    auto extract_uint64 = [&](const char* key) -> uint64_t {
        std::string search = std::string("\"") + key + "\":";
        auto pos = json.find(search);
        if (pos == std::string_view::npos) return 0;
        pos += search.size();
        uint64_t val = 0;
        while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
            val = val * 10 + static_cast<uint64_t>(json[pos] - '0');
            ++pos;
        }
        return val;
    };

    out.leader_node_id  = extract_string("node_id");
    out.leader_address  = extract_string("address");
    out.epoch.term      = extract_uint64("epoch");

    return !out.leader_node_id.empty();
}

// ── JSON serialization — PublishedPosition ───────────────────────────────────

std::string PublishedPosition::to_json() const {
    std::string out;
    out.reserve(96);

    out += "{\"node_id\":\"";
    out += node_id;
    out += "\",\"wal_file\":";
    out += std::to_string(wal_file_index);
    out += ",\"wal_offset\":";
    out += std::to_string(wal_byte_offset);
    out += '}';

    return out;
}

bool PublishedPosition::from_json(std::string_view json, PublishedPosition& out) {
    out = {};

    auto extract_string = [&](const char* key) -> std::string {
        std::string search = std::string("\"") + key + "\":\"";
        auto pos = json.find(search);
        if (pos == std::string_view::npos) return {};
        pos += search.size();
        auto end = json.find('"', pos);
        if (end == std::string_view::npos) return {};
        return std::string(json.substr(pos, end - pos));
    };

    auto extract_uint64 = [&](const char* key) -> uint64_t {
        std::string search = std::string("\"") + key + "\":";
        auto pos = json.find(search);
        if (pos == std::string_view::npos) return 0;
        pos += search.size();
        uint64_t val = 0;
        while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
            val = val * 10 + static_cast<uint64_t>(json[pos] - '0');
            ++pos;
        }
        return val;
    };

    out.node_id         = extract_string("node_id");
    out.wal_file_index  = static_cast<uint32_t>(extract_uint64("wal_file"));
    out.wal_byte_offset = static_cast<size_t>(extract_uint64("wal_offset"));

    return !out.node_id.empty();
}

// ── JSON response helpers (extract fields from etcd v3 REST responses) ───────

/// Extract a string value for a given key from a JSON fragment.
static std::string json_extract_string(std::string_view json, const char* key) {
    std::string search = std::string("\"") + key + "\":\"";
    auto pos = json.find(search);
    if (pos == std::string_view::npos) return {};
    pos += search.size();
    auto end = json.find('"', pos);
    if (end == std::string_view::npos) return {};
    return std::string(json.substr(pos, end - pos));
}

/// Extract a numeric value for a given key from a JSON fragment.
/// etcd v3 REST returns some numbers as quoted strings (e.g. lease ID).
static int64_t json_extract_int64(std::string_view json, const char* key) {
    std::string search = std::string("\"") + key + "\":";
    auto pos = json.find(search);
    if (pos == std::string_view::npos) return 0;
    pos += search.size();

    // Skip optional quote (etcd returns some numbers as strings).
    bool quoted = false;
    if (pos < json.size() && json[pos] == '"') {
        quoted = true;
        ++pos;
    }

    int64_t val = 0;
    bool negative = false;
    if (pos < json.size() && json[pos] == '-') {
        negative = true;
        ++pos;
    }
    while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
        val = val * 10 + static_cast<int64_t>(json[pos] - '0');
        ++pos;
    }

    (void)quoted; // consumed for skipping
    return negative ? -val : val;
}

/// Check if a JSON response contains "succeeded":true (for txn responses).
static bool json_extract_succeeded(std::string_view json) {
    return json.find("\"succeeded\":true") != std::string_view::npos;
}

// ── Pimpl ────────────────────────────────────────────────────────────────────

struct CoordinatorClient::Impl {
    CURL*               curl_handle{nullptr};
    bool                connected{false};
    std::string         active_endpoint;
    std::thread         watch_thread;
    std::atomic<bool>   watching{false};
    LeaseEventCallback  watch_cb;

    /// libcurl write callback — appends data to a std::string.
    static size_t write_callback(char* ptr, size_t size, size_t nmemb,
                                 void* userdata) {
        auto* response = static_cast<std::string*>(userdata);
        size_t total = size * nmemb;
        response->append(ptr, total);
        return total;
    }

    /// POST JSON to an etcd endpoint.  Returns response body or empty on error.
    std::string http_post(const std::string& url, const std::string& json_body) {
        if (!curl_handle) return {};

        std::string response;
        response.reserve(512);

        curl_easy_reset(curl_handle);
        curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, json_body.c_str());
        curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE,
                         static_cast<long>(json_body.size()));
        curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 2L);

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers);

        CURLcode res = curl_easy_perform(curl_handle);
        curl_slist_free_all(headers);

        if (res != CURLE_OK) return {};
        return response;
    }
};

// ── CoordinatorClient lifetime ───────────────────────────────────────────────

CoordinatorClient::CoordinatorClient(CoordinatorConfig config)
    : config_(std::move(config))
    , impl_(std::make_unique<Impl>())
{}

CoordinatorClient::~CoordinatorClient() {
    stop_watch();
    disconnect();
}

// ── connect / disconnect / is_connected ──────────────────────────────────────

bool CoordinatorClient::connect() {
    if (impl_->connected) return true;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    impl_->curl_handle = curl_easy_init();
    if (!impl_->curl_handle) return false;

    // Try each endpoint in order — first one that responds to /v3/maintenance/status wins.
    for (const auto& ep : config_.endpoints) {
        std::string url = ep + "/v3/maintenance/status";
        std::string body = "{}";
        std::string resp = impl_->http_post(url, body);
        if (!resp.empty()) {
            impl_->active_endpoint = ep;
            impl_->connected = true;
            return true;
        }
    }

    // No endpoint responded — clean up.
    curl_easy_cleanup(impl_->curl_handle);
    impl_->curl_handle = nullptr;
    return false;
}

void CoordinatorClient::disconnect() {
    stop_watch();
    if (impl_->curl_handle) {
        curl_easy_cleanup(impl_->curl_handle);
        impl_->curl_handle = nullptr;
    }
    impl_->connected = false;
    impl_->active_endpoint.clear();
}

bool CoordinatorClient::is_connected() const {
    return impl_->connected;
}

// ── Lease operations ─────────────────────────────────────────────────────────

int64_t CoordinatorClient::grant_lease() {
    if (!impl_->connected) return 0;

    std::string url = impl_->active_endpoint + "/v3/lease/grant";
    std::string body = "{\"TTL\":" + std::to_string(config_.lease_ttl_seconds) +
                       ",\"ID\":0}";

    std::string resp = impl_->http_post(url, body);
    if (resp.empty()) return 0;

    return json_extract_int64(resp, "ID");
}

bool CoordinatorClient::refresh_lease(int64_t lease_id) {
    if (!impl_->connected) return false;

    std::string url = impl_->active_endpoint + "/v3/lease/keepalive";
    std::string body = "{\"ID\":" + std::to_string(lease_id) + "}";

    std::string resp = impl_->http_post(url, body);
    return !resp.empty();
}

bool CoordinatorClient::revoke_lease(int64_t lease_id) {
    if (!impl_->connected) return false;

    std::string url = impl_->active_endpoint + "/v3/lease/revoke";
    std::string body = "{\"ID\":" + std::to_string(lease_id) + "}";

    std::string resp = impl_->http_post(url, body);
    return !resp.empty();
}

// ── Leadership acquisition (CAS transaction) ─────────────────────────────────

bool CoordinatorClient::try_acquire_leadership(int64_t lease_id,
                                               const EpochValue& epoch,
                                               const std::string& address) {
    if (!impl_->connected) return false;

    std::string leader_key = coordinator_leader_key(config_.cluster_prefix);
    std::string key_b64 = base64_encode(leader_key);

    // Build the value: ClusterState JSON.
    ClusterState state;
    state.leader_node_id = config_.node_id;
    state.leader_address = address;
    state.epoch          = epoch;
    state.lease_id       = lease_id;
    std::string value_b64 = base64_encode(state.to_json());

    // CAS: create-only (key must not exist / create_revision == 0).
    std::string url = impl_->active_endpoint + "/v3/kv/txn";
    std::string body =
        "{\"compare\":[{\"key\":\"" + key_b64 +
        "\",\"target\":\"CREATE\",\"create_revision\":\"0\"}],"
        "\"success\":[{\"request_put\":{\"key\":\"" + key_b64 +
        "\",\"value\":\"" + value_b64 +
        "\",\"lease\":" + std::to_string(lease_id) + "}}],"
        "\"failure\":[]}";

    std::string resp = impl_->http_post(url, body);
    if (resp.empty()) return false;

    return json_extract_succeeded(resp);
}

// ── Cluster state read ───────────────────────────────────────────────────────

std::optional<ClusterState> CoordinatorClient::get_cluster_state() {
    if (!impl_->connected) return std::nullopt;

    std::string leader_key = coordinator_leader_key(config_.cluster_prefix);
    std::string key_b64 = base64_encode(leader_key);

    std::string url = impl_->active_endpoint + "/v3/kv/range";
    std::string body = "{\"key\":\"" + key_b64 + "\"}";

    std::string resp = impl_->http_post(url, body);
    if (resp.empty()) return std::nullopt;

    // Extract the value from the first kv in the response.
    // etcd response: {"kvs":[{"key":"...","value":"<base64>", ...}]}
    std::string value_b64 = json_extract_string(resp, "value");
    if (value_b64.empty()) return std::nullopt;

    std::string value_json = base64_decode(value_b64);
    ClusterState state;
    if (!ClusterState::from_json(value_json, state)) return std::nullopt;

    return state;
}

// ── WAL position publish / read ──────────────────────────────────────────────

bool CoordinatorClient::publish_wal_position(uint32_t file_index,
                                             size_t byte_offset) {
    if (!impl_->connected) return false;

    std::string node_key = coordinator_node_key(config_.cluster_prefix,
                                                config_.node_id);
    std::string key_b64 = base64_encode(node_key);

    PublishedPosition pos;
    pos.node_id         = config_.node_id;
    pos.wal_file_index  = file_index;
    pos.wal_byte_offset = byte_offset;
    std::string value_b64 = base64_encode(pos.to_json());

    std::string url = impl_->active_endpoint + "/v3/kv/put";
    std::string body = "{\"key\":\"" + key_b64 +
                       "\",\"value\":\"" + value_b64 + "\"}";

    std::string resp = impl_->http_post(url, body);
    return !resp.empty();
}

std::vector<PublishedPosition> CoordinatorClient::get_published_positions() {
    std::vector<PublishedPosition> result;
    if (!impl_->connected) return result;

    std::string range_start = coordinator_node_key(config_.cluster_prefix, "");
    std::string range_end   = coordinator_nodes_range_end(config_.cluster_prefix);

    std::string key_b64     = base64_encode(range_start);
    std::string end_b64     = base64_encode(range_end);

    std::string url = impl_->active_endpoint + "/v3/kv/range";
    std::string body = "{\"key\":\"" + key_b64 +
                       "\",\"range_end\":\"" + end_b64 + "\"}";

    std::string resp = impl_->http_post(url, body);
    if (resp.empty()) return result;

    // Parse all "value" fields from the kvs array.
    // Simple approach: find each "value":"..." occurrence.
    std::string_view sv(resp);
    size_t search_pos = 0;
    while (search_pos < sv.size()) {
        std::string_view needle = "\"value\":\"";
        auto pos = sv.find(needle, search_pos);
        if (pos == std::string_view::npos) break;
        pos += needle.size();
        auto end = sv.find('"', pos);
        if (end == std::string_view::npos) break;

        std::string val_b64(sv.substr(pos, end - pos));
        std::string val_json = base64_decode(val_b64);

        PublishedPosition pp;
        if (PublishedPosition::from_json(val_json, pp)) {
            result.push_back(std::move(pp));
        }

        search_pos = end + 1;
    }

    return result;
}

// ── Watch leader key ─────────────────────────────────────────────────────────

void CoordinatorClient::watch_leader(LeaseEventCallback cb) {
    if (!impl_->connected) return;
    stop_watch();

    impl_->watch_cb = std::move(cb);
    impl_->watching.store(true, std::memory_order_release);

    std::string endpoint = impl_->active_endpoint;
    std::string prefix   = config_.cluster_prefix;
    auto* impl_ptr       = impl_.get();

    impl_->watch_thread = std::thread([endpoint, prefix, impl_ptr]() {
        // The watch thread uses its own curl handle for the long-poll.
        CURL* watch_curl = curl_easy_init();
        if (!watch_curl) return;

        std::string leader_key = coordinator_leader_key(prefix);
        std::string key_b64    = base64_encode(leader_key);

        std::string url  = endpoint + "/v3/watch";
        std::string body = "{\"create_request\":{\"key\":\"" + key_b64 + "\"}}";

        while (impl_ptr->watching.load(std::memory_order_acquire)) {
            std::string response;
            response.reserve(512);

            curl_easy_reset(watch_curl);
            curl_easy_setopt(watch_curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(watch_curl, CURLOPT_POSTFIELDS, body.c_str());
            curl_easy_setopt(watch_curl, CURLOPT_POSTFIELDSIZE,
                             static_cast<long>(body.size()));
            curl_easy_setopt(watch_curl, CURLOPT_WRITEFUNCTION,
                             Impl::write_callback);
            curl_easy_setopt(watch_curl, CURLOPT_WRITEDATA, &response);
            // Long-poll timeout — reconnect periodically to check watching flag.
            curl_easy_setopt(watch_curl, CURLOPT_TIMEOUT, 30L);

            struct curl_slist* headers = nullptr;
            headers = curl_slist_append(headers, "Content-Type: application/json");
            curl_easy_setopt(watch_curl, CURLOPT_HTTPHEADER, headers);

            CURLcode res = curl_easy_perform(watch_curl);
            curl_slist_free_all(headers);

            if (!impl_ptr->watching.load(std::memory_order_acquire)) break;

            if (res == CURLE_OK && !response.empty() && impl_ptr->watch_cb) {
                // Try to extract the new value from the watch event.
                std::string value_b64 = json_extract_string(response, "value");
                if (!value_b64.empty()) {
                    std::string value_json = base64_decode(value_b64);
                    ClusterState state;
                    if (ClusterState::from_json(value_json, state)) {
                        impl_ptr->watch_cb(state);
                    }
                } else {
                    // Key deleted (lease expired) — notify with empty state.
                    ClusterState empty_state;
                    impl_ptr->watch_cb(empty_state);
                }
            }
        }

        curl_easy_cleanup(watch_curl);
    });
}

void CoordinatorClient::stop_watch() {
    impl_->watching.store(false, std::memory_order_release);
    if (impl_->watch_thread.joinable()) {
        impl_->watch_thread.join();
    }
    impl_->watch_cb = nullptr;
}

} // namespace ob
