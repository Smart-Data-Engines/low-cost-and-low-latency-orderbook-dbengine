// Feature: orderbook-dbengine — C API wrapper (Requirements 13.1, 13.5)
//
// All extern "C" functions catch C++ exceptions and convert them to
// ob_status_t error codes so that no exception crosses the ABI boundary.

#include "orderbook/c_api.h"
#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/soa_buffer.hpp"
#include "orderbook/types.hpp"
#include "orderbook/wal.hpp"

#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

// ── ob_engine (C++ side) ──────────────────────────────────────────────────────
// Owns all subsystems.  The QueryEngine holds const-refs to store_, live_ptrs_,
// and agg_, so those members must be constructed before query_engine_.
//
// We use one ColumnarStore per (symbol, exchange) pair so that segment metadata
// carries the correct symbol/exchange strings required by the query engine's
// scan filter.  A "combined" store is used by the QueryEngine; it is rebuilt
// from all persisted segments on startup via open_existing().
struct ob_engine {
    std::string base_dir;
    ob::WALWriter     wal;
    ob::AggregationEngine agg;

    // One SoABuffer per "symbol.exchange" key.
    std::unordered_map<std::string, std::unique_ptr<ob::SoABuffer>> buffers;
    // Raw-pointer view used by QueryEngine (stable because unique_ptr owns storage).
    std::unordered_map<std::string, ob::SoABuffer*> live_ptrs;

    // One ColumnarStore per "symbol.exchange" key.
    std::unordered_map<std::string, std::unique_ptr<ob::ColumnarStore>> stores;

    // Combined store used by QueryEngine for scanning (rebuilt on startup).
    ob::ColumnarStore combined_store;

    std::unique_ptr<ob::QueryEngine> query_engine;
    std::mutex mtx;

    explicit ob_engine(const char* dir)
        : base_dir(dir)
        , wal(dir)
        , combined_store(dir)
        , query_engine(std::make_unique<ob::QueryEngine>(combined_store, live_ptrs, agg))
    {
        // Rebuild segment index from any previously persisted segments.
        combined_store.open_existing();
    }

    // Get or create the per-symbol ColumnarStore.
    ob::ColumnarStore& get_store(const std::string& symbol, const std::string& exchange) {
        const std::string key = symbol + "." + exchange;
        auto it = stores.find(key);
        if (it != stores.end()) return *it->second;

        // Create a new store rooted at base_dir.
        // We set symbol/exchange on it so that segment metadata is correct.
        auto store = std::make_unique<ob::ColumnarStore>(base_dir);
        store->set_symbol_exchange(symbol, exchange);
        auto& ref = *store;
        stores[key] = std::move(store);
        return ref;
    }
};

// ── ob_result (C++ side) ──────────────────────────────────────────────────────
struct ob_result {
    std::vector<ob::QueryResult> rows;
    size_t pos{0};
};

// ── Helpers ───────────────────────────────────────────────────────────────────

// Map ob:: status codes to the C API status codes (they share the same values,
// but the C header defines them as plain macros so we cast explicitly).
static inline ob_status_t to_c_status(ob::ob_status_t s) {
    return static_cast<ob_status_t>(s);
}

// ── Engine lifecycle ──────────────────────────────────────────────────────────

extern "C" ob_engine_t* ob_engine_create(const char* base_dir) {
    if (!base_dir) return nullptr;
    try {
        return new ob_engine(base_dir);
    } catch (...) {
        return nullptr;
    }
}

extern "C" void ob_engine_destroy(ob_engine_t* engine) {
    delete engine;
}

// ── Delta ingestion ───────────────────────────────────────────────────────────

extern "C" ob_status_t ob_apply_delta(ob_engine_t*    engine,
                                       const char*     symbol,
                                       const char*     exchange,
                                       uint64_t        seq,
                                       uint64_t        ts_ns,
                                       const int64_t*  prices,
                                       const uint64_t* qtys,
                                       const uint32_t* cnts,
                                       uint32_t        n_levels,
                                       int             side)
{
    if (!engine || !symbol || !exchange)              return OB_C_ERR_INVALID_ARG;
    if (n_levels > 0 && (!prices || !qtys || !cnts)) return OB_C_ERR_INVALID_ARG;
    if (side != 0 && side != 1)                       return OB_C_ERR_INVALID_ARG;

    try {
        std::lock_guard<std::mutex> lock(engine->mtx);

        // ── Build DeltaUpdate for WAL ─────────────────────────────────────────
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol,   symbol,   sizeof(delta.symbol)   - 1);
        std::strncpy(delta.exchange, exchange, sizeof(delta.exchange) - 1);
        delta.sequence_number = seq;
        delta.timestamp_ns    = ts_ns;
        delta.side            = static_cast<uint8_t>(side);
        delta.n_levels        = static_cast<uint16_t>(
            std::min(n_levels, static_cast<uint32_t>(ob::MAX_LEVELS)));

        // Build Level array for WAL and SoA apply.
        std::vector<ob::Level> levels(delta.n_levels);
        for (uint16_t i = 0; i < delta.n_levels; ++i) {
            levels[i].price = prices[i];
            levels[i].qty   = qtys[i];
            levels[i].cnt   = cnts[i];
        }

        // ── Write to WAL first (write-before-apply, Requirement 8.1) ─────────
        engine->wal.append(delta, levels.data());

        // ── Get or create SoABuffer for this symbol.exchange ──────────────────
        const std::string key = std::string(symbol) + "." + exchange;
        if (engine->buffers.find(key) == engine->buffers.end()) {
            auto buf = std::make_unique<ob::SoABuffer>();
            // Zero-initialise plain data fields; atomics are default-constructed.
            buf->bid.depth = 0;
            buf->ask.depth = 0;
            buf->bid.version.store(0, std::memory_order_relaxed);
            buf->ask.version.store(0, std::memory_order_relaxed);
            buf->sequence_number.store(0, std::memory_order_relaxed);
            buf->last_timestamp_ns = 0;
            std::strncpy(buf->symbol,   symbol,   sizeof(buf->symbol)   - 1);
            std::strncpy(buf->exchange, exchange, sizeof(buf->exchange) - 1);
            engine->live_ptrs[key] = buf.get();
            engine->buffers[key]   = std::move(buf);
        }

        ob::SoABuffer* buf = engine->buffers[key].get();

        // ── Apply to SoA buffer (seqlock protocol inside apply_delta) ─────────
        bool gap_detected = false;
        ob::ob_status_t apply_status = ob::apply_delta(*buf, delta, levels.data(), gap_detected);

        if (gap_detected) {
            engine->wal.append_gap(seq, ts_ns);
        }

        // ── Flush to columnar store (one SnapshotRow per level) ───────────────
        // Use the per-symbol store so that segment metadata carries the correct
        // symbol/exchange strings (needed for query engine scan filtering).
        ob::ColumnarStore& sym_store = engine->get_store(symbol, exchange);
        for (uint16_t i = 0; i < delta.n_levels; ++i) {
            ob::SnapshotRow row{};
            row.timestamp_ns    = ts_ns;
            row.sequence_number = seq;
            row.side            = static_cast<uint8_t>(side);
            row.level_index     = i;
            row.price           = prices[i];
            row.quantity        = qtys[i];
            row.order_count     = cnts[i];
            sym_store.append(row);
        }

        // ── Notify subscribers ────────────────────────────────────────────────
        for (uint16_t i = 0; i < delta.n_levels; ++i) {
            ob::SnapshotRow row{};
            row.timestamp_ns    = ts_ns;
            row.sequence_number = seq;
            row.side            = static_cast<uint8_t>(side);
            row.level_index     = i;
            row.price           = prices[i];
            row.quantity        = qtys[i];
            row.order_count     = cnts[i];
            engine->query_engine->notify_subscribers(symbol, exchange, row);
        }

        return to_c_status(apply_status);
    } catch (...) {
        return OB_C_ERR_INTERNAL;
    }
}

// ── Query ─────────────────────────────────────────────────────────────────────

extern "C" ob_result_t* ob_query(ob_engine_t* engine, const char* sql) {
    if (!engine || !sql) return nullptr;
    try {
        auto* result = new ob_result{};
        std::string err = engine->query_engine->execute(
            sql,
            [result](const ob::QueryResult& r) {
                result->rows.push_back(r);
            });
        if (!err.empty()) {
            delete result;
            return nullptr;
        }
        return result;
    } catch (...) {
        return nullptr;
    }
}

extern "C" void ob_result_free(ob_result_t* result) {
    delete result;
}

extern "C" ob_status_t ob_result_next(ob_result_t* result,
                                       uint64_t*    out_timestamp_ns,
                                       int64_t*     out_price,
                                       uint64_t*    out_quantity,
                                       uint32_t*    out_order_count,
                                       uint8_t*     out_side,
                                       uint16_t*    out_level)
{
    if (!result) return OB_C_ERR_INVALID_ARG;
    if (result->pos >= result->rows.size()) return OB_C_ERR_NOT_FOUND;

    const auto& r = result->rows[result->pos++];
    if (out_timestamp_ns) *out_timestamp_ns = r.timestamp_ns;
    if (out_price)        *out_price        = r.price;
    if (out_quantity)     *out_quantity     = r.quantity;
    if (out_order_count)  *out_order_count  = r.order_count;
    if (out_side)         *out_side         = r.side;
    if (out_level)        *out_level        = r.level;
    return OB_C_OK;
}

// ── Subscriptions ─────────────────────────────────────────────────────────────

extern "C" uint64_t ob_subscribe(ob_engine_t* engine,
                                  const char*  sql,
                                  void (*callback)(const char* json_row, void* userdata),
                                  void*        userdata)
{
    if (!engine || !sql || !callback) return 0;
    try {
        // Wrap the C callback in a RowCallback that serialises the result to JSON.
        auto cb = [callback, userdata](const ob::QueryResult& r) {
            std::ostringstream os;
            os << "{\"ts\":"    << r.timestamp_ns
               << ",\"price\":" << r.price
               << ",\"qty\":"   << r.quantity
               << ",\"cnt\":"   << r.order_count
               << ",\"side\":"  << static_cast<int>(r.side)
               << ",\"level\":" << r.level << "}";
            const std::string json = os.str();
            callback(json.c_str(), userdata);
        };
        return engine->query_engine->subscribe(sql, std::move(cb));
    } catch (...) {
        return 0;
    }
}

extern "C" void ob_unsubscribe(ob_engine_t* engine, uint64_t sub_id) {
    if (!engine) return;
    try {
        engine->query_engine->unsubscribe(sub_id);
    } catch (...) {}
}
