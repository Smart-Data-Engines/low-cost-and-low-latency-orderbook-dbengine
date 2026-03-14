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
