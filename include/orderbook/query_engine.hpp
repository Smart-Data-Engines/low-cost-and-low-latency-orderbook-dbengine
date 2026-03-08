#pragma once

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/soa_buffer.hpp"

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ob {

// ── QueryResult ───────────────────────────────────────────────────────────────
// A single row returned by a query execution.

struct QueryResult {
    uint64_t timestamp_ns;
    uint64_t sequence_number;
    int64_t  price;
    uint64_t quantity;
    uint32_t order_count;
    uint8_t  side;    // 0=bid, 1=ask
    uint16_t level;
    // For aggregation results: name -> value pairs
    std::vector<std::pair<std::string, int64_t>> agg_values;
};

// ── QueryType ─────────────────────────────────────────────────────────────────

enum class QueryType { SELECT, SUBSCRIBE, SNAPSHOT };

// ── QueryAST ──────────────────────────────────────────────────────────────────
// Internal representation of a parsed query.

struct QueryAST {
    QueryType type;
    std::string symbol;
    std::string exchange;
    std::optional<uint64_t> ts_start_ns;
    std::optional<uint64_t> ts_end_ns;
    std::optional<int64_t>  price_lo;
    std::optional<int64_t>  price_hi;
    std::vector<std::string> select_exprs;  // column names or agg calls
    std::optional<uint64_t> limit;
    std::optional<uint64_t> snapshot_ts_ns;
};

// ── RowCallback ───────────────────────────────────────────────────────────────

using RowCallback = std::function<void(const QueryResult&)>;

// ── QueryEngine ───────────────────────────────────────────────────────────────

class QueryEngine {
public:
    explicit QueryEngine(const ColumnarStore& store,
                         const std::unordered_map<std::string, SoABuffer*>& live_buffers,
                         const AggregationEngine& agg);

    ~QueryEngine();

    // Non-copyable
    QueryEngine(const QueryEngine&)            = delete;
    QueryEngine& operator=(const QueryEngine&) = delete;

    /// Execute a SQL query; returns error string on failure, empty on success.
    std::string execute(std::string_view sql, RowCallback cb);

    /// Parse only; returns error string on failure, empty on success.
    std::string parse(std::string_view sql, QueryAST& out);

    /// Pretty-print AST back to canonical SQL string.
    std::string format(const QueryAST& ast);

    /// Register streaming subscription; returns subscription id.
    uint64_t subscribe(std::string_view sql, RowCallback cb);

    /// Unregister a streaming subscription.
    void unsubscribe(uint64_t id);

    /// Called by the Engine when a new delta is committed to the SoA buffer.
    /// Notifies all matching subscriptions synchronously (within 1µs budget).
    void notify_subscribers(const std::string& symbol, const std::string& exchange,
                            const SnapshotRow& row);

private:
    const ColumnarStore& store_;
    const std::unordered_map<std::string, SoABuffer*>& live_buffers_;
    const AggregationEngine& agg_;

    // Subscription tracking
    struct Subscription {
        uint64_t   id;
        QueryAST   ast;
        RowCallback cb;
    };
    std::vector<Subscription> subscriptions_;
    uint64_t next_sub_id_{1};
};

} // namespace ob
