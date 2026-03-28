#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace ob {

// ── Command types ─────────────────────────────────────────────────────────────

enum class CommandType {
    SELECT,
    INSERT,
    MINSERT,
    FLUSH,
    PING,
    STATUS,
    ROLE,
    FAILOVER,
    QUIT,
    COMPRESS,
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

// ── MINSERT arguments ─────────────────────────────────────────────────────────

struct MinsertArgs {
    std::string symbol;
    std::string exchange;
    uint8_t     side;        // 0=bid, 1=ask
    uint16_t    n_levels;    // number of levels in the batch
    struct Level {
        int64_t  price;
        uint64_t qty;
        uint32_t count{1};
    };
    std::vector<Level> levels;
};

// ── Parsed command ────────────────────────────────────────────────────────────

struct Command {
    CommandType type;
    std::string raw_sql;        // for SELECT
    InsertArgs  insert_args;    // for INSERT
    MinsertArgs minsert_args;   // for MINSERT
    std::string target_node_id; // for FAILOVER
};

// ── Free functions ────────────────────────────────────────────────────────────

/// Parse a single command line. Returns Command with type=UNKNOWN on failure.
Command parse_command(std::string_view line);

/// Parse a multi-line MINSERT block. Returns Command with type=UNKNOWN on failure.
Command parse_minsert(std::string_view block);

/// Format a Command back to its wire representation (trailing \n included).
std::string format_command(const Command& cmd);

} // namespace ob
