#pragma once

#include "orderbook/query_columns.hpp"
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
        /// Valid only when `lag_known`; see the note on `Engine::Stats::ReplicaMetrics`.
        uint64_t    lag_bytes;
        bool        lag_known;
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
///
/// `columns` names what to emit and in what order, which is what the query asked for: a repeated
/// column is emitted twice and `SELECT quantity, price` answers in that order. Pass
/// `all_query_columns()` for `SELECT *`.
///
/// **Required rather than defaulted.** A default would let a caller not decide, and the value it
/// would have to default to - all seven - is the wrong answer for every narrowed query, returned
/// without complaint. There is one caller in the server and it has the list from the parser.
///
/// Row scans only. An aggregate result does not fit this shape — it has no price,
/// quantity or level — and passing one here is what made every aggregate query
/// answer a network client with a row of zeros. Use format_agg_response().
std::string format_query_response(const std::vector<QueryResult>& rows,
                                  const std::vector<QueryColumn>& columns);

/// A row scan's reply built as the engine hands the rows over, rather than from a vector of them
/// (#49 step 3). The server collected every row of a `SELECT` before it formatted one - 64 bytes a
/// row, copied again each time the vector grew - and measured on a server whose heap turned over,
/// that collection took three quarters of its page faults. The bytes are format_query_response()'s,
/// and a test holds the two to each other; a first result that carries aggregates makes the reply
/// format_agg_response()'s, the choice the server made by looking at the first row it collected.
///
/// `shape` is read at the first result and at finish(): the engine fills a query's shape before it
/// hands over a row - a test in `test_query_engine.cpp` holds it to that - and the reply's header is
/// written from it then.
class QueryResponseBuilder {
public:
    explicit QueryResponseBuilder(const QueryShape& shape) : shape_(shape) {}
    QueryResponseBuilder(const QueryResponseBuilder&) = delete;
    QueryResponseBuilder& operator=(const QueryResponseBuilder&) = delete;

    /// One result, formatted now: a row, or the aggregates an aggregate query hands over once.
    void add(const QueryResult& r);

    /// The reply, header and terminator included; the builder is spent after it.
    std::string finish();

private:
    void start(const QueryResult& first);
    void start_buckets();
    void add_narrow(const QueryResult& r);
    void add_bucket(const QueryResult& r);

    const QueryShape&     shape_;
    std::string           out_;
    std::vector<char>     narrow_row_;   // a narrowed row's buffer, sized from the columns
    std::vector<AggValue> aggregates_;
    bool started_   = false;
    bool aggregate_ = false;
    bool all_seven_ = false;
    bool buckets_   = false;   ///< a GROUP BY answer (#44), told by the shape, not by a row
};

/// The header of a `GROUP BY TIME_BUCKET(...)` answer (#44), without its line end:
///
///     bucket_ns	COUNT(*)/1	FIRST(price)/1	VWAP(price)/1000000
///
/// Each aggregate's scale follows its expression after a `/`, divide by it for the natural value:
/// the scale depends on the function alone, and the header is written before the first row. No
/// expression contains a `/`, so a client splits each name at its last one.
std::string format_bucket_header(const QueryShape& shape);

/// Format aggregate results as TSV: one row per aggregate, three columns.
///
///     OK
///     name	value	scale
///     SPREAD(*)	1000	1
///     MID_PRICE(*)	100500000000	1000000
///
/// The value column carries NULL when there was nothing to aggregate, which is not
/// the same as zero: a spread with no ask side is absent, not tight. The scale
/// column is what the value is multiplied by, so a client divides by it instead of
/// hardcoding that mid-price happens to be scaled by a million.
std::string format_agg_response(const std::vector<AggValue>& values);

/// Format one pushed subscription row.
/// Returns "PUSH <id>\t<timestamp>\t<price>\t<quantity>\t<order_count>\t<side>\t<level>\t<seq>\n"
///
/// The seven columns are the seven columns of a `SELECT` row, in the same order (#65 put the
/// sequence number last), so a client that already parses query results adds one branch on the
/// prefix rather than a second format.
///
/// `PUSH` rather than a second connection for pushed data. A separate channel would be cleaner to
/// parse and would require agreeing on the identity of two connections, which is the problem
/// `conn_id` solves internally — exposed to the client this time.
///
/// One line, complete, and that is why the subscriber queue holds formatted bytes rather than rows:
/// a partial socket write then resumes mid-line instead of mid-row, and a row is never split across
/// two framings.
std::string format_push(uint64_t subscription_id, const QueryResult& row);

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
/// Render STATUS.
///
/// `identity` is the authenticated identity of the session asking, or empty when client
/// authentication is off - rendered as a key/value line rather than a column, so no client parsing
/// the tab-separated table has to change. Per-session rather than in ServerStats because that is
/// what it is: two sessions on one node answer this differently.
std::string format_status(const ServerStats& stats, std::string_view identity = {});

/// Parse a response string back into structured form (for round-trip testing).
ParsedResponse parse_response(std::string_view response);

} // namespace ob
