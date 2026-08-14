#pragma once

#include "orderbook/query_engine.hpp"

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ob {

// ── Server statistics (thread-safe) ───────────────────────────────────────────

struct ServerStats {
    std::atomic<uint64_t> total_queries{0};
    std::atomic<uint64_t> total_inserts{0};
    std::atomic<int>      active_sessions{0};

    // Engine-level metrics (populated on STATUS request).
    struct EngineMetrics {
        size_t pending_rows{0};
        size_t wal_file_index{0};
        size_t segment_count{0};
        size_t symbol_count{0};
    };
    EngineMetrics engine_metrics;

    // Replication metrics (populated from Engine::Stats on STATUS request)
    struct ReplicaMetrics {
        std::string address;
        uint32_t    confirmed_file;
        size_t      confirmed_offset;
        size_t      lag_bytes;
    };
    std::vector<ReplicaMetrics> replicas;

    bool     is_replica{false};
    uint32_t repl_confirmed_file{0};
    size_t   repl_confirmed_offset{0};
    uint64_t repl_records_replayed{0};
    bool     repl_connected{false};

    // Snapshot bootstrap state
    bool     bootstrapping{false};
    size_t   snapshot_bytes_received{0};
    size_t   snapshot_bytes_total{0};
    bool     snapshot_active{false};  // primary: snapshot transfer in progress

    // Failover state
    uint8_t  node_role{0};            // NodeRole enum value
    uint64_t current_epoch{0};
    std::string primary_address;
    int64_t  lease_ttl_remaining{0};

    // Compression metrics
    uint64_t compress_bytes_in{0};    // total pre-compression bytes
    uint64_t compress_bytes_out{0};   // total post-compression bytes

    // TTL / data retention metrics
    uint64_t ttl_hours{0};              // configured TTL (0 = disabled)
    uint64_t ttl_segments_deleted{0};   // cumulative segments deleted
    uint64_t ttl_bytes_reclaimed{0};    // cumulative bytes reclaimed

    // Flush integrity: segments refused because their directory was already in the
    // index. Anything but 0 means two flush paths raced.
    uint64_t segment_merge_refused{0};

    // Sharding metrics
    std::string shard_id;                // empty = non-sharded
    std::string shard_status;            // "active", "joining", "draining"
    size_t      shard_symbols_count{0};
    uint64_t    shard_map_version{0};

    // Migration metrics
    bool        migration_in_progress{false};
    std::string migration_symbol;
    std::string migration_target_shard;
    uint8_t     migration_progress_pct{0};

    // Routing errors
    uint64_t    shard_routing_errors{0};

    // Multi-master metrics
    uint8_t     mm_node_role{0};          // NodeRole enum value (3 = MULTI_MASTER)
    uint16_t    mm_node_id{0};
    size_t      mm_peer_count{0};
    size_t      mm_connected_peers{0};
    uint64_t    mm_conflicts_total{0};
    uint64_t    mm_anti_entropy_runs{0};
    uint64_t    mm_anti_entropy_repairs{0};
    uint64_t    mm_hlc_physical_ns{0};
    uint16_t    mm_hlc_logical{0};
    int64_t     mm_hlc_drift_ns{0};
    std::vector<std::pair<uint16_t, size_t>> mm_replication_lag_per_peer;
};

// ── Parsed response (for round-trip testing) ──────────────────────────────────

struct ParsedResponse {
    bool        is_error;
    std::string error_message;
    std::vector<std::string>              header_columns;
    std::vector<std::vector<std::string>> rows;
};

// ── Formatting functions ──────────────────────────────────────────────────────

/// Format a successful query result as TSV with headers.
/// Returns "OK\n<header>\n<row1>\n...<rowN>\n\n"
std::string format_query_response(const std::vector<QueryResult>& rows);

/// Format an error response.
/// Returns "ERR <message>\n"
std::string format_error(std::string_view message);

/// Format OK with no body.
/// Returns "OK\n\n"
std::string format_ok();

/// Format PONG response.
/// Returns "PONG\n"
std::string format_pong();

/// Format STATUS response with server statistics.
std::string format_status(const ServerStats& stats);

/// Parse a response string back into structured form (for round-trip testing).
ParsedResponse parse_response(std::string_view response);

} // namespace ob
