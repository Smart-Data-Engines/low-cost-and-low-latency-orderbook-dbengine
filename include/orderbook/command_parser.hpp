#pragma once

#include <cstdint>
#include <optional>
#include <span>
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

    /// Why the line was refused, when the parser has something specific to say about it.
    ///
    /// Empty for a line whose first token is not a command at all - that one is `unknown command`
    /// and nothing more can honestly be said. Non-empty when the command *was* recognised and the
    /// line still is not it: a trailing token, or an argument the grammar has no place for. Those
    /// two answers used to be the same answer, and the difference matters to whoever typed it -
    /// `unknown command` about `INSERT ... typo` sends the reader looking for a missing feature.
    ///
    /// The message names the offending **token**, not how many there were: "too many arguments"
    /// sends an operator counting spaces (#107).
    std::string error;

    /// AUTH: the claimed identity, and the response to the outstanding challenge.
    ///
    /// Both empty for a bare `AUTH`, which is the request for a challenge. Parsed here rather than
    /// in the gate so that a malformed response never reaches a comparison: the response must be
    /// exactly 64 lower-case hex characters and the identity must be within the identity charset,
    /// or the line is UNKNOWN.
    std::string auth_identity;
    std::string auth_response;
};

// ── Command grammar ───────────────────────────────────────────────────────────

/// The most tokens one command line may carry, keyword included, and what it accepts instead.
///
/// Exported so that a test can be written against the parser's **own** table rather than against a
/// second list of the same facts. That shape has cost this repository once already: #32 grew a flag,
/// a negation table and a test from a list of value-less flags I wrote by reading a default instead
/// of the parser, and all three were wrong. A list you wrote yourself is not evidence about code.
struct CommandGrammar {
    CommandType      type;
    std::string_view keyword;

    /// Empty where the tail belongs to the query parser (`SELECT`, `SUBSCRIBE`), which refuses a
    /// trailing token by name itself.
    ///
    /// An `optional` rather than a sentinel, and that is a correction made by a mutation. The first
    /// version had `kFreeForm = size_t(-1)` and a guard reading
    /// `max_tokens != kFreeForm && tokens.size() > max_tokens` — and **deleting that guard changed
    /// nothing**, because no line has `SIZE_MAX` tokens, so the comparison was already false. A
    /// guard that cannot fail is a guard that cannot be checked, and the next reader would trust it.
    /// With the emptiness in the type, dropping the test compares against `nullopt`, refuses every
    /// `SELECT`, and a test says so.
    std::optional<size_t> max_tokens;

    std::string_view usage;        ///< quoted in the refusal, so the answer says what is accepted

    /// False where a token on this line can be a credential (`AUTH`). The refusal then says *that*
    /// there is an extra token without repeating it, because a refusal writes a log line and a
    /// response echoed into a log is a response in a log.
    bool             may_quote_token;
};

/// Every command's grammar, in `CommandType` order. One row per enumerator except `UNKNOWN`, which
/// the implementation enforces at compile time.
std::span<const CommandGrammar> command_grammar();

// ── Free functions ────────────────────────────────────────────────────────────

/// Parse a single command line. Returns Command with type=UNKNOWN on failure.
Command parse_command(std::string_view line);

/// Parse a multi-line MINSERT block. Returns Command with type=UNKNOWN on failure.
Command parse_minsert(std::string_view block);

/// Format a Command back to its wire representation (trailing \n included).
std::string format_command(const Command& cmd);

} // namespace ob
