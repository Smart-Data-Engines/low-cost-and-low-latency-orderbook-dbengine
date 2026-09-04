#pragma once

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/soa_buffer.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ob {

// ── QueryResult ───────────────────────────────────────────────────────────────
// A single row returned by a query execution.

/// One aggregate result on its way to a client: what was asked for, what came out,
/// whether there was anything to aggregate, and in what units.
///
/// `empty` and `scale` are not decoration. Without `empty`, a spread computed on a
/// book with no ask side is indistinguishable from a spread of zero. Without
/// `scale`, a client reads MID_PRICE a million times too large, because vwap and
/// mid_price are scaled by 10^6 and imbalance by 10^9. Both used to be dropped
/// here, and the whole vector used to be dropped again by the response formatter.
struct AggValue {
    std::string name;   ///< the expression exactly as written, e.g. "MID_PRICE(*)"
    int64_t     value;
    bool        empty;
    int64_t     scale;
};

struct QueryResult {
    uint64_t timestamp_ns;
    uint64_t sequence_number;
    int64_t  price;
    uint64_t quantity;
    uint32_t order_count;
    uint8_t  side;    // 0=bid, 1=ask
    uint16_t level;
    // Populated for aggregation queries only; empty for a row scan.
    std::vector<AggValue> agg_values;
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

/// Resolve the live SoA buffer for a `"symbol.exchange"` key, or nullptr.
///
/// A callable rather than a reference to Engine's map, and the difference is a data race.
/// `live_ptrs_` is inserted into by every thread that applies a write - a client, the replication
/// apply path, the multi-master io loop - while a query reads it. `unordered_map` insertion
/// rehashes, so a concurrent reader can follow a bucket that has moved: undefined behaviour, and
/// ThreadSanitizer reported it five times on the first integration run that issued a `SELECT` while
/// a new symbol's buffer was being created on another thread (#91).
///
/// Engine's implementation takes `mtx_` for the duration of **one map lookup** and releases it
/// before the query runs, so this costs one uncontended lock per query rather than holding the
/// write path's mutex across a scan.
using LiveBufferLookup = std::function<SoABuffer*(const std::string& key)>;

// ── QueryEngine ───────────────────────────────────────────────────────────────

class QueryEngine {
public:
    explicit QueryEngine(const ColumnarStore& store,
                         LiveBufferLookup live_buffer,
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

    /// Register streaming subscription; returns subscription id, or 0 if the SQL does not parse.
    uint64_t subscribe(std::string_view sql, RowCallback cb);

    /// Unregister a streaming subscription.
    ///
    /// Marks it dead and returns; the entry is removed later, under the exclusive lock, when no
    /// notification is in flight. So a cancelled subscription may still receive one more row if a
    /// notification was already running - stated here because a client assuming "nothing after
    /// unsubscribe" is a client with a race.
    ///
    /// The alternative, waiting here for notifications to quiesce, would block whichever thread
    /// cancels on whichever thread notifies. Those are the epoll loop and `io_loop`.
    void unsubscribe(uint64_t id);

    /// Whether anything is subscribed at all. One relaxed atomic read.
    ///
    /// The write path calls this before building a batch, because zero subscriptions is the case in
    /// every deployment that does not use them and in almost every test, and it must not pay for a
    /// lock. May read high for a moment - a subscription marked dead and not yet compacted still
    /// counts - and never low. That direction is the safe one: high costs one pointless lock
    /// acquisition, low drops a row.
    bool has_subscribers() const {
        return live_.load(std::memory_order_relaxed) > 0;
    }

    /// Called by the Engine when a new delta is committed to the SoA buffer.
    ///
    /// Takes the lock **once per delta**, not once per row. It used to be called from inside the
    /// per-level loop in `apply_delta`, so a 1000-level MINSERT meant a thousand calls; adding a
    /// lock to that shape would have meant a thousand acquisitions on the hot path.
    ///
    /// Called from whichever thread owns the write path - the server's epoll loop for client
    /// writes, `MultiMasterManager::io_loop` for a peer's delta. It may not touch anything owned by
    /// one of those in particular, which is why a callback that wants to reach a socket has to
    /// enqueue rather than write.
    void notify_subscribers(const std::string& symbol, const std::string& exchange,
                            std::span<const SnapshotRow> rows);

private:
    const ColumnarStore& store_;
    LiveBufferLookup     live_buffer_;
    const AggregationEngine& agg_;

    // ── Subscription tracking ────────────────────────────────────────────────────────────────
    //
    // Shared between the write path and whatever registers subscriptions, which are different
    // threads. Before this it was a bare vector: subscribe() push_back'd, unsubscribe() erased, and
    // notify_subscribers() iterated, with nothing making those safe together. On the unfixed tree
    // `QueryEngineUnit.SubscribingWhileNotifyingIsSafeAcrossThreads` aborts on every run.
    struct Subscription {
        uint64_t          id;
        QueryAST          ast;
        RowCallback       cb;
        std::atomic<bool> dead{false};
    };

    /// Held shared while notifying, exclusive while registering or compacting.
    ///
    /// `shared_mutex` rather than `mutex` because notification is frequent and read-only while
    /// registration is rare and writes. It is the more expensive of the two at zero contention,
    /// which is affordable only because `has_subscribers()` keeps the no-subscriber path away from
    /// it entirely.
    mutable std::shared_mutex subs_mtx_;

    /// `unique_ptr` rather than the object, so an entry's address does not move.
    ///
    /// A vector of objects relocates its elements on `push_back`, and a notification in progress
    /// holds a reference into one of them. The shared lock does not save that on its own, since
    /// registration takes the exclusive lock and could therefore be the only thing running - the
    /// indirection makes the address stable regardless, instead of resting on an argument about
    /// when the vector does and does not reallocate.
    std::vector<std::unique_ptr<Subscription>> subscriptions_;

    /// Notifications currently running. Compaction waits for zero.
    std::atomic<uint32_t> notifying_{0};

    /// Entries not marked dead, for `has_subscribers()`.
    std::atomic<size_t> live_{0};

    uint64_t next_sub_id_{1};

    /// Drop dead entries. Caller holds the exclusive lock; does nothing while a notification runs.
    void compact_locked();

    /// Count entries not marked dead. Caller holds the lock in either mode.
    ///
    /// Recounted rather than incremented and decremented, so the counter cannot drift away from
    /// what the vector actually holds across a compaction. It is read on the hot path through
    /// `has_subscribers()`, so being wrong there is worse than being slightly slower here - and
    /// this runs on registration, which happens once per subscription.
    size_t count_live_locked() const;
};

} // namespace ob
