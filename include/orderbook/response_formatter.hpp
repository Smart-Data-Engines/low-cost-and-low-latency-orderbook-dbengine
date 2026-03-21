#pragma once

#include "orderbook/query_engine.hpp"

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
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
