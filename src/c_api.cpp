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
#include "orderbook/logger.hpp"
#include "orderbook/durable_file.hpp"
#include "orderbook/level_payload.hpp"
#include "orderbook/wall_clock.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ── ob_engine (C++ side) ──────────────────────────────────────────────────────
// Owns all subsystems.  The QueryEngine holds a reference to the store and the aggregation
// engine, and a callable that resolves a live buffer, so those members must be constructed before
// query_engine_.
//
// We use one ColumnarStore per (symbol, exchange) pair so that segment metadata
// carries the correct symbol/exchange strings required by the query engine's
// scan filter.  A "combined" store is used by the QueryEngine; it is rebuilt
// from all persisted segments on startup via open_existing().
struct ob_engine {
    std::string base_dir;
    ob::WALWriter     wal;
    ob::AggregationEngine agg;

    // One SoABuffer per "symbol.exchange" key. `shared_ptr` for the same reason as the server's
    // map (#92): what a query holds has to keep the buffer alive. Nothing clears this map today,
    // so the lifetime defect was the server's alone - but a type that only happens to be safe is
    // the shape that produced #92 in the first place. The raw-pointer index that used to sit here
    // is gone; one map cannot disagree with itself.
    std::unordered_map<std::string, std::shared_ptr<ob::SoABuffer>> buffers;

    // One ColumnarStore per "symbol.exchange" key.
    std::unordered_map<std::string, std::unique_ptr<ob::ColumnarStore>> stores;

    // Combined store used by QueryEngine for scanning (rebuilt on startup).
    ob::ColumnarStore combined_store;

    std::unique_ptr<ob::QueryEngine> query_engine;
    std::mutex mtx;

    // The identity of this WAL, which a segment records beside its position, so that a replay
    // trusts only the positions written against this WAL (#245, as the server does: #63, pitfall 39).
    uint64_t wal_identity{0};
    // Where the WAL ended once the last checkpoint was appended: a flush with nothing new since then
    // appends none.
    ob::WalPosition checkpointed_at{};
    bool has_checkpointed{false};

    explicit ob_engine(const char* dir)
        : base_dir(dir)
        , wal(dir)
        , combined_store(dir)
        // A lookup taking `mtx`, not a reference to the map. The same race the embedded path
        // had: `ob_insert` creates buffers under `mtx` while `ob_query` read the map without it,
        // and an unordered_map insertion rehashes under a concurrent reader (#91). `ob_query` does
        // not hold `mtx`, so taking it here cannot self-deadlock. It returns an owning handle for
        // the same reason the server's does (#92).
        , query_engine(std::make_unique<ob::QueryEngine>(
              combined_store,
              [this](const std::string& key) -> std::shared_ptr<ob::SoABuffer> {
                  std::lock_guard<std::mutex> lock(mtx);
                  auto it = buffers.find(key);
                  return (it == buffers.end()) ? nullptr : it->second;
              },
              agg))
    {
        load_or_create_wal_identity();
        // Rebuild segment index from any previously persisted segments.
        combined_store.open_existing();
        restore_live_books();
        replay_wal_tail();
    }

    // A close is a flush too, so that it leaves a checkpoint behind it and the next open replays
    // nothing it wrote (#245). The members' own destructors then find nothing left to write.
    ~ob_engine() {
        try {
            flush_locked();
        } catch (const std::exception& e) {
            OB_LOG_ERROR("c_api", "close: the last flush failed: %s - the next open replays the WAL "
                                  "from the checkpoint before it", e.what());
        } catch (...) {
            OB_LOG_ERROR("c_api", "close: the last flush failed with an exception that is not a "
                                  "std::exception");
        }
    }

    // The identity of the WAL beside this engine's segments: read from `wal_identity`, or drawn and
    // written there before any segment records it, as the server does it (#63, #160). A directory
    // the server wrote holds the same file, and it means the same thing.
    void load_or_create_wal_identity() {
        const std::string path = base_dir + "/wal_identity";
        {
            std::ifstream in(path);
            uint64_t value = 0;
            if (in.is_open() && (in >> value) && value != 0) {
                wal_identity = value;
                OB_LOG_INFO("c_api", "WAL identity %llu", static_cast<unsigned long long>(value));
                return;
            }
            if (in.is_open()) {
                OB_LOG_WARN("c_api", "%s is present and unusable - drawing a new identity, so a "
                                     "replay trusts no position the segments recorded and compares "
                                     "times instead", path.c_str());
            }
        }
        // Random rather than derived from the path or the clock: two directories restored from one
        // backup must not agree, or one would trust the other's positions.
        std::random_device rd;
        uint64_t value = (static_cast<uint64_t>(rd()) << 32) ^ rd();
        if (value == 0) value = 1;   // 0 means "unknown" everywhere else
        wal_identity = value;
        if (const int err = ob::write_file_atomically(path, std::to_string(value)); err != 0) {
            OB_LOG_WARN("c_api", "cannot write %s (%s) - a replay will compare times rather than "
                                 "positions, which is exact only while a symbol's times rise",
                        path.c_str(), std::strerror(err));
            return;
        }
        OB_LOG_INFO("c_api", "WAL identity %llu, new", static_cast<unsigned long long>(value));
    }

    // One row a level of `update` into `store`, as a write stores them.
    static void append_rows(ob::ColumnarStore& store, const ob::DeltaUpdate& update,
                            const ob::Level* levels) {
        for (uint16_t i = 0; i < update.n_levels; ++i) {
            ob::SnapshotRow row{};
            row.timestamp_ns    = update.timestamp_ns;
            row.sequence_number = update.sequence_number;
            row.side            = update.side;
            row.level_index     = i;
            row.price           = levels[i].price;
            row.quantity        = levels[i].qty;
            row.order_count     = levels[i].cnt;
            store.append(row);
        }
    }

    // The segment these rows end up in holds every record of the symbol up to this one, which starts
    // at `at`: a replay skips a record that starts before the position a segment recorded (#245, as
    // the server's #63). Called after the rows are appended, so that a rollover inside append()
    // stamps the segment it closes with the record before, whose rows are the last it holds.
    void stamp_position(ob::ColumnarStore& store, ob::WalPosition at) {
        store.set_wal_position(wal_identity, at.file_index, static_cast<uint64_t>(at.offset) + 1);
    }

    // Write each store's open segment, with any its writes rolled over into, index them, and append
    // a checkpoint saying every record so far is in a segment (#245); then remove the WAL files
    // wholly before it. ob_flush() and a close are this. `mtx` held, or the engine reachable by
    // nobody else.
    size_t flush_locked() {
        // What the next open would have indexed: the segments the writes rolled over into as well,
        // which append() parks because it cannot reach the index (pitfall 13).
        std::vector<ob::SegmentMeta> metas;
        for (auto& [key, store] : stores) {
            for (ob::SegmentMeta& meta : store->take_rolled_segments()) metas.push_back(std::move(meta));
            if (std::optional<ob::SegmentMeta> meta = store->flush_segment()) {
                metas.push_back(std::move(*meta));
            }
        }
        const size_t refused = combined_store.merge_segments(metas);
        if (refused != 0) {
            // Their rows are in the segments the index already holds under those directories, so
            // a query misses nothing; that two writes produced one directory is the defect to find.
            OB_LOG_ERROR("c_api", "flush: the index refused %zu of %zu segment(s) as already held",
                         refused, metas.size());
        }
        // Every record so far has its rows in a segment now. A checkpoint says so, and the next open
        // replays only what comes after it.
        const ob::WalPosition covered = wal.current_position();
        const bool moved = !has_checkpointed || covered.file_index != checkpointed_at.file_index ||
                           covered.offset != checkpointed_at.offset;
        size_t removed = 0;
        if (moved) {
            wal.append_checkpoint(ob::wall_clock_ns(), covered);
            checkpointed_at  = wal.current_position();
            has_checkpointed = true;
            removed = wal.truncate_before(covered.file_index);
        }
        OB_LOG_DEBUG("c_api", "flush: %zu segment(s) written and indexed from %zu store(s); %s at file "
                              "%u offset %u; %zu WAL file(s) before it removed",
                     metas.size(), stores.size(), moved ? "a checkpoint" : "no checkpoint, the WAL ends",
                     covered.file_index, covered.offset, removed);
        return metas.size();
    }

    // The records the last checkpoint does not cover, applied as a write applies them - the update
    // into the live book, its rows into their store - and not written to the WAL again, which holds
    // them (#245). Before this the embedded engine read none of its WAL, so a process that ended
    // without a flush or a close lost every write since the last one.
    //
    // A record a segment already holds is skipped: by the position the segment recorded, which
    // closes the window between a flush's segments and its checkpoint; or, for a segment of an
    // older build, which recorded none and wrote no checkpoint, by the time of its last row - the
    // server's rule before #63, exact only while a symbol's times rise. Then a flush, so the next
    // open replays none of them again.
    void replay_wal_tail() {
        const auto started = std::chrono::steady_clock::now();
        struct DurableUpTo {
            ob::WalPosition position{};
            bool            has_position{false};
            uint64_t        last_row_ts_ns{0};
        };
        std::unordered_map<std::string, DurableUpTo> durable;
        for (const ob::SegmentMeta& meta : combined_store.index()) {
            DurableUpTo& d = durable[meta.symbol + "." + meta.exchange];
            if (meta.last_row_ts_ns > d.last_row_ts_ns) d.last_row_ts_ns = meta.last_row_ts_ns;
            // A position written against another WAL - a directory copied from another node - is
            // not this WAL's, and a zero one is a segment that predates them.
            if (meta.wal_identity == 0 || meta.wal_identity != wal_identity ||
                (meta.wal_file_index == 0 && meta.wal_byte_offset == 0)) {
                continue;
            }
            const ob::WalPosition at{meta.wal_file_index, static_cast<uint32_t>(meta.wal_byte_offset)};
            if (!d.has_position || ob::wal_position_before(d.position, at)) {
                d.position     = at;
                d.has_position = true;
            }
        }

        uint64_t records = 0, applied = 0, rows = 0, by_position = 0, by_time = 0;
        std::vector<ob::Level> scratch;
        ob::WALReplayer replayer(base_dir);
        replayer.replay_after_checkpoint([&](const ob::WALReplayContext& ctx) {
            ++records;
            if (ctx.header.record_type != ob::WAL_RECORD_DELTA) return;
            if (ctx.payload_len < sizeof(ob::DeltaUpdate)) {
                OB_LOG_WARN("c_api", "WAL replay: DELTA payload too short (%zu < %zu), skipping",
                            ctx.payload_len, sizeof(ob::DeltaUpdate));
                return;
            }
            ob::DeltaUpdate update{};
            std::memcpy(&update, ctx.payload, sizeof(update));
            const size_t level_bytes = static_cast<size_t>(update.n_levels) * sizeof(ob::Level);
            if (sizeof(ob::DeltaUpdate) + level_bytes > ctx.payload_len) {
                OB_LOG_WARN("c_api", "WAL replay: payload holds %zu bytes but %u levels need %zu, "
                                     "skipping", ctx.payload_len, update.n_levels,
                            sizeof(ob::DeltaUpdate) + level_bytes);
                return;
            }
            const std::string symbol(update.symbol, strnlen(update.symbol, sizeof(update.symbol)));
            const std::string exchange(update.exchange, strnlen(update.exchange, sizeof(update.exchange)));
            const ob::WalPosition at{ctx.wal_file_index, static_cast<uint32_t>(ctx.wal_byte_offset)};
            if (const auto it = durable.find(symbol + "." + exchange); it != durable.end()) {
                const DurableUpTo& d = it->second;
                if (d.has_position) {
                    if (ob::wal_position_before(at, d.position)) {
                        ++by_position;
                        return;
                    }
                } else if (update.timestamp_ns <= d.last_row_ts_ns) {
                    ++by_time;
                    return;
                }
            }
            // Copied, not cast: see level_payload.hpp and #127.
            const ob::Level* levels = ob::levels_from_payload(ctx.payload, update.n_levels, scratch);
            // A gap the update leaves was written to the WAL when the write was made.
            bool gap_detected = false;
            ob::apply_delta(buffer(symbol, exchange), update, levels, gap_detected);
            ob::ColumnarStore& store = get_store(symbol, exchange);
            append_rows(store, update, levels);
            stamp_position(store, at);
            ++applied;
            rows += update.n_levels;
        });
        if (applied > 0) flush_locked();
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - started).count();
        OB_LOG_INFO("c_api", "WAL replay: %llu record(s) after the last checkpoint, %llu update(s) "
                             "applied (%llu row(s)), %llu in a segment by its position, %llu by its "
                             "last row's time, in %lld ms",
                    static_cast<unsigned long long>(records), static_cast<unsigned long long>(applied),
                    static_cast<unsigned long long>(rows), static_cast<unsigned long long>(by_position),
                    static_cast<unsigned long long>(by_time), static_cast<long long>(ms));
    }

    // Get or create the live buffer of "symbol.exchange", zero-initialised. `mtx` held, or the
    // engine not yet reachable by anyone else.
    ob::SoABuffer& buffer(const std::string& symbol, const std::string& exchange) {
        const std::string key = symbol + "." + exchange;
        auto it = buffers.find(key);
        if (it == buffers.end()) {
            auto buf = std::make_shared<ob::SoABuffer>();
            // Zero-initialise plain data fields; atomics are default-constructed.
            buf->bid.depth = 0;
            buf->ask.depth = 0;
            buf->bid.version.store(0, std::memory_order_relaxed);
            buf->ask.version.store(0, std::memory_order_relaxed);
            buf->sequence_number.store(0, std::memory_order_relaxed);
            buf->last_timestamp_ns.store(0, std::memory_order_relaxed);
            std::strncpy(buf->symbol,   symbol.c_str(),   sizeof(buf->symbol)   - 1);
            std::strncpy(buf->exchange, exchange.c_str(), sizeof(buf->exchange) - 1);
            it = buffers.emplace(key, std::move(buf)).first;
        }
        return *it->second;
    }

    // The live book of every symbol history holds, composed by price, as the server's engine
    // restores it at a start (#235). A side replacement computes its removals from the live book,
    // and a reopened engine without this held none - the Python client's local flush() reopens
    // one - so a replacement after a reopen removed nothing it should have.
    //
    // The sequence label stays 0, unknown, as for a book no write has reached: this engine decides
    // a gap from its caller's own numbering (apply_delta), which a reopen does not restore, and a
    // client in a new process numbering from 1 again has not skipped anything.
    void restore_live_books() {
        const auto started = std::chrono::steady_clock::now();
        size_t restored = 0;
        size_t levels = 0;
        for (const auto& [symbol, exchange] : combined_store.symbols()) {
            const ob::ColumnarStore::PricedBook book =
                combined_store.book_at_by_price(UINT64_MAX, symbol, exchange, ob::MAX_LEVELS);
            if (!book.as_of) continue;
            std::vector<ob::Level> bids, asks;
            for (const ob::SnapshotRow& r : book.rows) {
                ob::Level level{};
                level.price = r.price;
                level.qty   = r.quantity;
                level.cnt   = r.order_count;
                (r.side == ob::SIDE_BID ? bids : asks).push_back(level);
            }
            ob::install_book(buffer(symbol, exchange), bids.data(), bids.size(), asks.data(),
                             asks.size(), /*sequence_number=*/0, book.as_of->timestamp_ns);
            ++restored;
            levels += book.rows.size();
            OB_LOG_DEBUG("c_api", "live book of %s.%s restored from history: %zu bid and %zu ask "
                                  "price(s), as of %llu",
                         symbol.c_str(), exchange.c_str(), bids.size(), asks.size(),
                         static_cast<unsigned long long>(book.as_of->timestamp_ns));
        }
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - started).count();
        OB_LOG_INFO("c_api", "live books restored from history: %zu symbol(s), %zu level(s), in %lld ms",
                    restored, levels, static_cast<long long>(ms));
    }

    // Get or create the per-symbol ColumnarStore.
    ob::ColumnarStore& get_store(const std::string& symbol, const std::string& exchange) {
        const std::string key = symbol + "." + exchange;
        auto it = stores.find(key);
        if (it != stores.end()) return *it->second;

        // Create a new store rooted at base_dir.
        // We set symbol/exchange on it so that segment metadata is correct.
        // What this store writes is read through combined_store, so it keeps no index of its own
        // (#165) - one that nothing read and nothing pruned.
        auto store = std::make_unique<ob::ColumnarStore>(
            base_dir, ob::ColumnarStore::kDefaultSegmentDurationNs, ob::ColumnarStore::OwnIndex::kNo);
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

extern "C" ob_status_t ob_flush(ob_engine_t* engine) {
    if (!engine) return OB_C_ERR_INVALID_ARG;
    try {
        std::lock_guard<std::mutex> lock(engine->mtx);
        engine->flush_locked();
        return OB_C_OK;
    } catch (const std::exception& e) {
        OB_LOG_ERROR("c_api", "flush failed: %s", e.what());
        return OB_C_ERR_INTERNAL;
    } catch (...) {
        OB_LOG_ERROR("c_api", "flush failed with an exception that is not a std::exception");
        return OB_C_ERR_INTERNAL;
    }
}

// ── Delta ingestion ───────────────────────────────────────────────────────────

namespace {

/// One write of the embedded engine, its lock held: the WAL first, then the book, the rows and the
/// subscribers. `ob_apply_delta()` and `ob_replace_side()` are both this, the second with the
/// removals it adds - so there is one write path here, as in the server's engine.
ob_status_t apply_locked(ob_engine_t*    engine,
                         const char*     symbol,
                         const char*     exchange,
                         uint64_t        seq,
                         uint64_t        ts_ns,
                         const int64_t*  prices,
                         const uint64_t* qtys,
                         const uint32_t* cnts,
                         uint16_t        n_levels,
                         int             side)
{

    // ── Build DeltaUpdate for WAL ─────────────────────────────────────────
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol,   symbol,   sizeof(delta.symbol)   - 1);
    std::strncpy(delta.exchange, exchange, sizeof(delta.exchange) - 1);
    delta.sequence_number = seq;
    delta.timestamp_ns    = ts_ns;
    delta.side            = static_cast<uint8_t>(side);
    delta.n_levels        = n_levels;

    // Build Level array for WAL and SoA apply.
    std::vector<ob::Level> levels(delta.n_levels);
    for (uint16_t i = 0; i < delta.n_levels; ++i) {
        levels[i].price = prices[i];
        levels[i].qty   = qtys[i];
        levels[i].cnt   = cnts[i];
    }

    // ── Write to WAL first (write-before-apply, Requirement 8.1) ─────────
    const ob::WalPosition at = engine->wal.append(delta, levels.data());

    // ── Get or create SoABuffer for this symbol.exchange ──────────────────
    ob::SoABuffer* buf = &engine->buffer(symbol, exchange);

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
    ob_engine::append_rows(sym_store, delta, levels.data());
    engine->stamp_position(sym_store, at);

    // ── Notify subscribers ────────────────────────────────────────────────
    // One call for the whole delta. Guarded, because a caller with no subscriptions - the
    // common case, and every test that is not about subscriptions - should not build the array.
    if (engine->query_engine->has_subscribers()) {
        std::vector<ob::SnapshotRow> rows;
        rows.reserve(delta.n_levels);
        for (uint16_t i = 0; i < delta.n_levels; ++i) {
            ob::SnapshotRow row{};
            row.timestamp_ns    = ts_ns;
            row.sequence_number = seq;
            row.side            = static_cast<uint8_t>(side);
            row.level_index     = i;
            row.price           = prices[i];
            row.quantity        = qtys[i];
            row.order_count     = cnts[i];
            rows.push_back(row);
        }
        engine->query_engine->notify_subscribers(symbol, exchange, rows);
    }

    return to_c_status(apply_status);
}

}  // namespace

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
        return apply_locked(engine, symbol, exchange, seq, ts_ns, prices, qtys, cnts,
                            static_cast<uint16_t>(
                                std::min(n_levels, static_cast<uint32_t>(ob::MAX_LEVELS))),
                            side);
    } catch (...) {
        return OB_C_ERR_INTERNAL;
    }
}

extern "C" ob_status_t ob_replace_side(ob_engine_t*    engine,
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
    // Refused rather than cut to the ceiling, as `ob_apply_delta()` cuts a write: a replacement cut
    // short would remove prices it named.
    if (n_levels > ob::MAX_LEVELS)                    return OB_C_ERR_INVALID_ARG;

    try {
        std::lock_guard<std::mutex> lock(engine->mtx);
        // A level of quantity 0 for every price the side holds and the update does not name - in
        // the side's own order, so best first - then the update, under the lock the write takes.
        // The removals first, as in the server's engine: a full side has no room for a new price
        // until they are applied.
        std::vector<int64_t>  p;
        std::vector<uint64_t> q;
        std::vector<uint32_t> c;
        const std::string key = std::string(symbol) + "." + exchange;
        size_t removed = 0;
        if (const auto it = engine->buffers.find(key); it != engine->buffers.end() && it->second) {
            const ob::SoASide& held = side == 0 ? it->second->bid : it->second->ask;
            const std::unordered_set<int64_t> named(prices, prices + n_levels);
            for (uint32_t k = 0; k < held.depth; ++k) {
                if (named.count(held.prices[k]) != 0) continue;
                p.push_back(held.prices[k]);
                q.push_back(0);
                c.push_back(0);
                ++removed;
            }
        }
        p.insert(p.end(), prices, prices + n_levels);
        q.insert(q.end(), qtys, qtys + n_levels);
        c.insert(c.end(), cnts, cnts + n_levels);
        OB_LOG_DEBUG("c_api", "replace side %s %s: %u level(s) written, %zu removed", key.c_str(),
                     side == 0 ? "bid" : "ask", n_levels, removed);
        return apply_locked(engine, symbol, exchange, seq, ts_ns, p.data(), q.data(), c.data(),
                            static_cast<uint16_t>(p.size()), side);
    } catch (...) {
        return OB_C_ERR_INTERNAL;
    }
}

// ── Query ─────────────────────────────────────────────────────────────────────

extern "C" ob_result_t* ob_query(ob_engine_t* engine, const char* sql) {
    if (!engine || !sql) return nullptr;
    try {
        // A GROUP BY answer is buckets of aggregates (#44), which this result cannot carry - its rows
        // are the seven columns - so it is refused rather than answered as rows of zeros.
        {
            ob::QueryAST ast;
            if (engine->query_engine->parse(sql, ast).empty() && ast.bucket_ns.has_value()) {
                OB_LOG_WARN("c_api", "GROUP BY TIME_BUCKET is not answered by the local library; "
                                     "query a server, whose clients read time buckets");
                return nullptr;
            }
        }
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

extern "C" ob_status_t ob_result_next_seq(ob_result_t* result,
                                       uint64_t*    out_timestamp_ns,
                                       int64_t*     out_price,
                                       uint64_t*    out_quantity,
                                       uint32_t*    out_order_count,
                                       uint8_t*     out_side,
                                       uint16_t*    out_level,
                                       uint64_t*    out_sequence_number)
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
    if (out_sequence_number) *out_sequence_number = r.sequence_number;
    return OB_C_OK;
}

extern "C" ob_status_t ob_result_next(ob_result_t* result,
                                       uint64_t*    out_timestamp_ns,
                                       int64_t*     out_price,
                                       uint64_t*    out_quantity,
                                       uint32_t*    out_order_count,
                                       uint8_t*     out_side,
                                       uint16_t*    out_level)
{
    // Delegating keeps one implementation of the cursor. The sequence number is dropped rather
    // than silently zeroed into a caller's struct: this signature has no field for it.
    return ob_result_next_seq(result, out_timestamp_ns, out_price, out_quantity,
                              out_order_count, out_side, out_level, nullptr);
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
