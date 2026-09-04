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
    SHARD_MAP,
    SHARD_INFO,
    MIGRATE,
    MM_PEERS,
    MM_CONFLICTS,
    SUBSCRIBE,
    UNSUBSCRIBE,
    AUTH,
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
    std::string migrate_symbol;       // "symbol.exchange" for MIGRATE
    std::string migrate_target_shard; // target shard_id for MIGRATE
    size_t      mm_conflicts_limit{100}; // for MM_CONFLICTS

    /// The whole SUBSCRIBE line, handed to the query engine unparsed.
    ///
    /// Not decomposed here, deliberately. `QueryEngine::parse()` already accepts the full grammar -
    /// symbol, exchange, timestamp and price filters - and re-implementing a subset of it in the
    /// command parser would make two languages with one name. So this layer decides only *which*
    /// command arrived.
    std::string subscribe_sql;

    /// The id for UNSUBSCRIBE, or 0 meaning "every subscription of this session".
    uint64_t    unsubscribe_id{0};

    /// AUTH: the claimed identity, and the response to the outstanding challenge.
    ///
    /// Both empty for a bare `AUTH`, which is the request for a challenge. Parsed here rather than
    /// in the gate so that a malformed response never reaches a comparison: the response must be
    /// exactly 64 lower-case hex characters and the identity must be within the identity charset,
    /// or the line is UNKNOWN.
    std::string auth_identity;
    std::string auth_response;
};

// ── Free functions ────────────────────────────────────────────────────────────

/// Parse a single command line. Returns Command with type=UNKNOWN on failure.
Command parse_command(std::string_view line);

/// Parse a multi-line MINSERT block. Returns Command with type=UNKNOWN on failure.
Command parse_minsert(std::string_view block);

/// Format a Command back to its wire representation (trailing \n included).
std::string format_command(const Command& cmd);

} // namespace ob
