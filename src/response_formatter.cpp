#include "orderbook/response_formatter.hpp"
#include "orderbook/capabilities.hpp"
#include "orderbook/version.hpp"

#include <algorithm>
#include <charconv>
#include <iterator>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace ob {

// ── Helpers ───────────────────────────────────────────────────────────────────

// sequence_number goes last on purpose: a client that reads columns by index keeps working, and
// one that reads by name finds the new field. 0 means "unassigned" — rows written before #64
// carry no number, and there is no way to invent one for them after the fact.
static constexpr std::string_view kQueryHeader =
    "timestamp_ns\tprice\tquantity\torder_count\tside\tlevel\tsequence_number";

static constexpr std::string_view kAggHeader = "name\tvalue\tscale";

static constexpr std::string_view kStatusHeader =
    "sessions\tqueries\tinserts\tpending_rows\twal_file\tsegments\tsymbols";

/// Split a string_view by a single-character delimiter.
static std::vector<std::string_view> split(std::string_view sv, char delim) {
    std::vector<std::string_view> parts;
    size_t start = 0;
    while (start <= sv.size()) {
        auto pos = sv.find(delim, start);
        if (pos == std::string_view::npos) {
            parts.push_back(sv.substr(start));
            break;
        }
        parts.push_back(sv.substr(start, pos - start));
        start = pos + 1;
    }
    return parts;
}

// ── format_query_response ─────────────────────────────────────────────────────

/// Widest a `SELECT *` row can be: every field at its type's limit, six tabs and a newline.
///
///   timestamp_ns 20 + price 20 (19 digits and a sign) + quantity 20 + order_count 10
///   + side 3 + level 5 + sequence_number 20  =  98, plus 7 separators.
///
/// It is the size of the stack buffer and **not** the worst case any more: a query may name the
/// same column more than once, so `SELECT sequence_number, sequence_number, ...` needs 21 bytes
/// per repeat and would run off the end of a buffer sized for seven distinct columns. The row
/// buffer is sized from the query's own column list, and this is the point at which it stops
/// fitting on the stack.
static constexpr size_t kMaxQueryRowBytes = 105;

/// Append `value` and then `sep`, without constructing a string for either.
///
/// `std::to_chars` writes into a caller's buffer and does not allocate, which is the whole reason
/// this is here: the version before it called `std::to_string` seven times a row, so a 4,000-row
/// answer built 28,000 temporary strings. Measured with `perf` on an m9g.xlarge over 16,000 of the
/// comparative benchmark's time-range query, `format_query_response` was **22.96%** of the
/// profile against **2.70%** for `ColumnarStore::scan` - the response cost eight times the read it
/// came from - and the temporaries were visible beside it as `basic_string::_M_construct` at
/// 5.53%, `memcpy` at 24.27%, and malloc/free at 6.84%. After this, three of those five symbols
/// are not in the profile at all.
template <typename T>
static char* put_field(char* p, char* end, T value, char sep) {
    // `end` is the buffer's real end rather than `p + 24`. The fixed window was correct only
    // because the buffer was always wider than any one field; handing `to_chars` a pointer past
    // the allocation is the kind of detail that is fine until a caller sizes the buffer tightly.
    //
    // `side` is a `uint8_t`, which is a character type: handing it to `to_chars` unwidened invites
    // an overload that would write a byte rather than a number. Widen every integer to the type
    // `to_chars` cannot misread.
    if constexpr (std::is_signed_v<T>) {
        const auto res = std::to_chars(p, end, static_cast<long long>(value));
        p = res.ptr;
    } else {
        const auto res = std::to_chars(p, end, static_cast<unsigned long long>(value));
        p = res.ptr;
    }
    *p++ = sep;
    return p;
}

/// One field of one row, chosen by column. Separated from the loop so that the seven-column path
/// and a narrowed one cannot disagree about what a column means.
static char* put_column(char* p, char* end, const QueryResult& r, QueryColumn c, char sep) {
    switch (c) {
        case QueryColumn::TimestampNs:    return put_field(p, end, r.timestamp_ns,    sep);
        case QueryColumn::Price:          return put_field(p, end, r.price,           sep);
        case QueryColumn::Quantity:       return put_field(p, end, r.quantity,        sep);
        case QueryColumn::OrderCount:     return put_field(p, end, r.order_count,     sep);
        case QueryColumn::Side:           return put_field(p, end, r.side,            sep);
        case QueryColumn::Level:          return put_field(p, end, r.level,           sep);
        case QueryColumn::SequenceNumber: return put_field(p, end, r.sequence_number, sep);
    }
    // No `default`, so a column added to the enum and not to this switch is a build error rather
    // than a field that silently goes missing from every response.
    return p;
}

/// The seven-column row, unrolled.
///
/// A duplicate of what the loop below does, and it is here because measuring said so rather than
/// because it looked faster. With only the general loop, `SELECT *` - the shape the published
/// comparative table measures - cost **24% more cycles inside this function** over 16,000 of that
/// query on an m9g.xlarge (2.107 G to 2.621 G), while every other symbol stayed flat. Seven
/// straight-line calls inline; a loop that picks the field by a value cannot, however cheap the
/// switch is.
///
/// The two paths are pinned against each other by a test that formats each column alone through
/// the loop and compares it with the same field cut out of this one's output.
static char* put_seven(char* p, char* end, const QueryResult& r) {
    p = put_field(p, end, r.timestamp_ns,    '\t');
    p = put_field(p, end, r.price,           '\t');
    p = put_field(p, end, r.quantity,        '\t');
    p = put_field(p, end, r.order_count,     '\t');
    p = put_field(p, end, r.side,            '\t');
    p = put_field(p, end, r.level,           '\t');
    p = put_field(p, end, r.sequence_number, '\n');
    return p;
}

std::string format_query_response(const std::vector<QueryResult>& rows,
                                  const std::vector<QueryColumn>& columns) {
    std::string out;
    // 64 rather than `kMaxQueryRowBytes`: measured on the comparative benchmark's dataset a row is
    // **43 bytes** for all seven columns, so 64 leaves half again and reserves 1.6x what is used
    // rather than 2.4x. A narrower answer over-reserves, which costs one allocation of unused
    // bytes and no copying; under-reserving costs a copy of everything written so far, and
    // growing is what the 24.27% of that profile spent in `memcpy` was.
    out.reserve(96 + rows.size() * 64);

    out += "OK\n";
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i != 0) out += '\t';
        out += column_name(columns[i]);
    }
    out += '\n';

    // One buffer for the row and one append for it: thirteen appends a row was thirteen chances
    // to grow the string and thirteen calls into its bookkeeping.
    //
    // Two loops rather than one with a branch in it, and the second attempt at this is why. A
    // single loop writing through a `char*` that might point at either a stack array or a heap
    // one left the compiler unable to treat the canonical path's buffer as a known local, and
    // `SELECT *` stayed ~5% slower than before projection even with the row writer unrolled.
    // Split, the canonical path is the same shape it was: a fixed local array nothing else can
    // alias.
    if (columns == all_query_columns()) {
        char line[kMaxQueryRowBytes];
        for (const auto& r : rows) {
            char* p = put_seven(line, line + sizeof(line), r);
            out.append(line, static_cast<size_t>(p - line));
        }
    } else if (!columns.empty()) {
        // One allocation for the whole response, sized from this query's own list because a
        // repeated column repeats its width - `SELECT sequence_number, sequence_number, ...` at
        // twenty repeats needs 420 bytes where seven distinct columns need 105.
        std::vector<char> buf(max_row_bytes(columns));
        char* const buf_end = buf.data() + buf.size();
        const size_t last = columns.size() - 1;
        for (const auto& r : rows) {
            char* p = buf.data();
            for (size_t i = 0; i < columns.size(); ++i) {
                p = put_column(p, buf_end, r, columns[i], i == last ? '\n' : '\t');
            }
            out.append(buf.data(), static_cast<size_t>(p - buf.data()));
        }
    }
    // An empty column list emits a header of nothing and a row of nothing, which is what asking
    // for no columns means. Not reachable from the parser, which requires at least one item.

    out += '\n'; // empty line terminator
    return out;
}

// ── format_agg_response ───────────────────────────────────────────────────────

std::string format_agg_response(const std::vector<AggValue>& values) {
    std::string out;
    out.reserve(64 + values.size() * 48);

    out += "OK\n";
    out += kAggHeader;
    out += '\n';

    for (const auto& v : values) {
        out += v.name;
        out += '\t';
        // NULL rather than 0: a client that reads 0 here believes a number the
        // engine never computed. One that chokes on NULL at least knows.
        if (v.empty) {
            out += "NULL";
        } else {
            out += std::to_string(v.value);
        }
        out += '\t';
        out += std::to_string(v.scale);
        out += '\n';
    }

    out += '\n'; // empty line terminator, same contract as a row response
    return out;
}

// ── format_error ──────────────────────────────────────────────────────────────

// ── format_push ───────────────────────────────────────────────────────────────

std::string format_push(uint64_t subscription_id, const QueryResult& row) {
    std::string out;
    out.reserve(96);
    out += "PUSH ";
    out += std::to_string(subscription_id);
    out += '\t';
    out += std::to_string(row.timestamp_ns);
    out += '\t';
    out += std::to_string(row.price);
    out += '\t';
    out += std::to_string(row.quantity);
    out += '\t';
    out += std::to_string(row.order_count);
    out += '\t';
    out += std::to_string(row.side);
    out += '\t';
    out += std::to_string(row.level);
    out += '\t';
    out += std::to_string(row.sequence_number);
    out += '\n';
    return out;
}

std::string format_error(std::string_view message) {
    std::string out;
    out.reserve(5 + message.size());
    out += "ERR ";
    out += message;
    out += '\n';
    return out;
}

// ── format_ok ─────────────────────────────────────────────────────────────────

std::string format_ok() {
    return "OK\n\n";
}

// ── format_pong ───────────────────────────────────────────────────────────────

std::string format_pong() {
    return "PONG\n";
}

// ── format_status ─────────────────────────────────────────────────────────────

std::string format_status(const ServerStats& stats, std::string_view identity) {
    std::string out;
    out += "OK\n";
    out += kStatusHeader;
    out += '\n';
    out += std::to_string(stats.active_sessions.load(std::memory_order_relaxed));
    out += '\t';
    out += std::to_string(stats.total_queries.load(std::memory_order_relaxed));
    out += '\t';
    out += std::to_string(stats.total_inserts.load(std::memory_order_relaxed));
    out += '\t';
    out += std::to_string(stats.engine_metrics.pending_rows);
    out += '\t';
    out += std::to_string(stats.engine_metrics.wal_file_index);
    out += '\t';
    out += std::to_string(stats.engine_metrics.segment_count);
    out += '\t';
    out += std::to_string(stats.engine_metrics.symbol_count);
    out += '\n';

    // What this build can do, by name (#105). Unconditional and always in the same place, for the
    // same reason the replica count below is unconditional: a consumer that cannot tell "none of
    // them" from "the field is missing" is a consumer that has to guess, and here the guess decides
    // whether a client sends an event time or refuses to.
    out += "capabilities: ";
    for (size_t i = 0; i < std::size(kCapabilities); ++i) {
        if (i) out += ',';
        out += kCapabilities[i];
    }
    out += '\n';

    // Replication info (primary mode): per-replica lag.
    //
    // The count is unconditional. Emitting it only when replicas exist means a
    // monitoring consumer cannot tell "zero replicas" from "field missing", which is
    // the difference between a healthy standalone node and a parser that broke.
    out += "replicas: ";
    out += std::to_string(stats.replicas.size());
    out += '\n';
    if (!stats.replicas.empty()) {
        for (size_t i = 0; i < stats.replicas.size(); ++i) {
            const auto& r = stats.replicas[i];
            out += "replica[";
            out += std::to_string(i);
            out += "]: ";
            out += r.address;
            out += " file=";
            out += std::to_string(r.confirmed_file);
            out += " offset=";
            out += std::to_string(r.confirmed_offset);
            out += " lag=";
            // `unknown` rather than a number, because zero is a real answer here: a replica that
            // is caught up is zero bytes behind. Printing zero for "cannot be measured" is the
            // defect #123 was, said a second way — and this case means something worse than a
            // large lag, since a WAL file the replica still needs is gone.
            if (r.lag_known) {
                out += std::to_string(r.lag_bytes);
            } else {
                out += "unknown";
            }
            out += '\n';
        }
    }

    // Replication info (replica mode): local replay offset, connection status
    if (stats.is_replica) {
        out += "replication: connected=";
        out += (stats.repl_connected ? "yes" : "no");
        out += " file=";
        out += std::to_string(stats.repl_confirmed_file);
        out += " offset=";
        out += std::to_string(stats.repl_confirmed_offset);
        out += " replayed=";
        out += std::to_string(stats.repl_records_replayed);
        out += '\n';
    }

    // Snapshot bootstrap progress (replica)
    if (stats.bootstrapping) {
        out += "snapshot: bootstrapping bytes_received=";
        out += std::to_string(stats.snapshot_bytes_received);
        out += " bytes_total=";
        out += std::to_string(stats.snapshot_bytes_total);
        out += '\n';
    }

    // Snapshot transfer active (primary)
    if (stats.snapshot_active) {
        out += "snapshot: transfer_active\n";
    }

    // Failover state
    {
        out += "role: ";
        switch (stats.node_role) {
        case 1:  out += "primary"; break;
        case 2:  out += "replica"; break;
        default: out += "standalone"; break;
        }
        out += '\n';
        out += "epoch: ";
        out += std::to_string(stats.current_epoch);
        out += '\n';
        if (!stats.primary_address.empty()) {
            out += "primary_address: ";
            out += stats.primary_address;
            out += '\n';
        }
        if (stats.lease_ttl_remaining > 0) {
            out += "lease_ttl_remaining: ";
            out += std::to_string(stats.lease_ttl_remaining);
            out += '\n';
        }
    }

    // Compression metrics
    if (stats.compress_bytes_in > 0 || stats.compress_bytes_out > 0) {
        out += "compress_bytes_in: ";
        out += std::to_string(stats.compress_bytes_in);
        out += '\n';
        out += "compress_bytes_out: ";
        out += std::to_string(stats.compress_bytes_out);
        out += '\n';
    }

    // TTL / data retention metrics
    out += "ttl_hours: ";
    out += std::to_string(stats.ttl_hours);
    out += '\n';
    out += "ttl_segments_deleted: ";
    out += std::to_string(stats.ttl_segments_deleted);
    out += '\n';
    out += "ttl_bytes_reclaimed: ";
    out += std::to_string(stats.ttl_bytes_reclaimed);
    out += '\n';

    // The build this node is running, which nothing could be asked before #90: not this command,
    // not `--print-config`, not `/metrics`. A key/value line rather than a column, so no client
    // parsing the tab-separated table above has to change.
    out += "version: ";
    out += version();
    out += '\n';

    // Who is asking (#30). A dash when client authentication is disabled, which is a different
    // statement from an empty value: it says the server is not authenticating anyone.
    out += "identity: ";
    out += identity.empty() ? std::string_view{"-"} : identity;
    out += '\n';

    // Flush integrity
    out += "segment_merge_refused: ";
    out += std::to_string(stats.segment_merge_refused);
    out += '\n';

    // Sharding metrics (only when shard_id is non-empty)
    if (!stats.shard_id.empty()) {
        out += "shard_id: ";
        out += stats.shard_id;
        out += '\n';
        out += "shard_status: ";
        out += stats.shard_status;
        out += '\n';
        out += "shard_symbols_count: ";
        out += std::to_string(stats.shard_symbols_count);
        out += '\n';
        out += "shard_map_version: ";
        out += std::to_string(stats.shard_map_version);
        out += '\n';

        // Migration metrics (when migration is in progress)
        if (stats.migration_in_progress) {
            out += "migration_in_progress: 1\n";
            out += "migration_symbol: ";
            out += stats.migration_symbol;
            out += '\n';
            out += "migration_target_shard: ";
            out += stats.migration_target_shard;
            out += '\n';
            out += "migration_progress_pct: ";
            out += std::to_string(stats.migration_progress_pct);
            out += '\n';
        }

        out += "shard_routing_errors: ";
        out += std::to_string(stats.shard_routing_errors);
        out += '\n';
    }

    // Multi-master metrics (only when node_role == MULTI_MASTER, i.e. mm_node_role == 3)
    if (stats.mm_node_role == 3) {
        out += "[multi_master]\n";
        out += "node_id: ";
        out += std::to_string(stats.mm_node_id);
        out += '\n';
        out += "peer_count: ";
        out += std::to_string(stats.mm_peer_count);
        out += '\n';
        out += "connected_peers: ";
        out += std::to_string(stats.mm_connected_peers);
        out += '\n';
        out += "mm_conflicts_total: ";
        out += std::to_string(stats.mm_conflicts_total);
        out += '\n';
        out += "anti_entropy_runs: ";
        out += std::to_string(stats.mm_anti_entropy_runs);
        out += '\n';
        out += "anti_entropy_repairs: ";
        out += std::to_string(stats.mm_anti_entropy_repairs);
        out += '\n';
        out += "hlc_physical_ns: ";
        out += std::to_string(stats.mm_hlc_physical_ns);
        out += '\n';
        out += "hlc_logical: ";
        out += std::to_string(stats.mm_hlc_logical);
        out += '\n';
        out += "hlc_drift_ns: ";
        out += std::to_string(stats.mm_hlc_drift_ns);
        out += '\n';
        // `replication_lag_peer_<id>` used to be printed here, and it was not a lag: it was this
        // node's own WAL offset minus a byte position in the peer's **own** WAL, recorded once at
        // handshake and never updated. On a mesh converged by row content it therefore equalled
        // this node's WAL size to the byte, which is to say the engine reported a peer holding
        // every row as sitting at byte zero (#118).
        //
        // Removed rather than re-pointed at the honest number. The honest number is in **records**
        // and comes from a different mechanism at a different freshness, so keeping the field name
        // and changing what it means would leave every existing reader parsing the same line and
        // silently changing subject. `ob_mm_replication_lag_records` is the replacement and it is
        // a metric, because a number an operator has to read by hand cannot be alerted on (#94).
    }

    out += '\n'; // empty line terminator
    return out;
}

// ── parse_response ────────────────────────────────────────────────────────────

ParsedResponse parse_response(std::string_view response) {
    ParsedResponse parsed{};
    parsed.is_error = false;

    if (response.empty()) return parsed;

    // Error response: "ERR <message>\n"
    if (response.size() >= 4 && response.substr(0, 4) == "ERR ") {
        parsed.is_error = true;
        auto msg = response.substr(4);
        // Strip trailing newline
        if (!msg.empty() && msg.back() == '\n') {
            msg.remove_suffix(1);
        }
        parsed.error_message = std::string(msg);
        return parsed;
    }

    // PONG response: "PONG\n"
    if (response.size() >= 4 && response.substr(0, 4) == "PONG") {
        // No headers, no rows — just a pong
        return parsed;
    }

    // OK response: "OK\n..." — parse TSV header and data rows
    if (response.size() >= 3 && response.substr(0, 3) == "OK\n") {
        auto body = response.substr(3);

        // Split body into lines
        auto lines = split(body, '\n');

        // First non-empty line is the header
        size_t idx = 0;

        // Find header line
        while (idx < lines.size() && lines[idx].empty()) ++idx;
        if (idx >= lines.size()) return parsed;

        // Parse header columns (tab-separated)
        auto header_line = lines[idx];
        parsed.header_columns = [&]() {
            auto cols = split(header_line, '\t');
            std::vector<std::string> result;
            result.reserve(cols.size());
            for (auto c : cols) result.emplace_back(c);
            return result;
        }();
        ++idx;

        // Parse data rows until empty line or end
        for (; idx < lines.size(); ++idx) {
            if (lines[idx].empty()) break; // terminator
            auto cells = split(lines[idx], '\t');
            std::vector<std::string> row;
            row.reserve(cells.size());
            for (auto c : cells) row.emplace_back(c);
            parsed.rows.push_back(std::move(row));
        }

        return parsed;
    }

    return parsed;
}

} // namespace ob
