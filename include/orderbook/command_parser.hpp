#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace ob {

// ── Command types ─────────────────────────────────────────────────────────────

enum class CommandType {
    SELECT,
    INSERT,
    FLUSH,
    PING,
    STATUS,
    ROLE,
    FAILOVER,
    QUIT,
    UNKNOWN
};

// ── INSERT arguments ──────────────────────────────────────────────────────────

struct InsertArgs {
    std::string symbol;
    std::string exchange;
    uint8_t     side;       // 0=bid, 1=ask
    int64_t     price;
    uint64_t    qty;
    uint32_t    count{1};
};

// ── Parsed command ────────────────────────────────────────────────────────────

struct Command {
    CommandType type;
    std::string raw_sql;        // for SELECT
    InsertArgs  insert_args;    // for INSERT
    std::string target_node_id; // for FAILOVER
};

// ── Free functions ────────────────────────────────────────────────────────────

/// Parse a single command line. Returns Command with type=UNKNOWN on failure.
Command parse_command(std::string_view line);

/// Format a Command back to its wire representation (trailing \n included).
std::string format_command(const Command& cmd);

} // namespace ob
