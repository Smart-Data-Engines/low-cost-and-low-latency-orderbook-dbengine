// Feature: orderbook-dbengine — Engine facade (Requirements 7.3, 7.4, 7.5, 8.1, 8.3)
//
// The Engine owns and coordinates all subsystems:
//   WALWriter, SoABuffer map, ColumnarStore map, AggregationEngine, QueryEngine.
//
// apply_delta flow: WAL write → SoA buffer apply (gap detection) → enqueue for columnar flush.
// open():  replay WAL + rebuild columnar index + start background flush thread.
// close(): stop flush thread + final flush + flush_segment + WAL flush.

#include "orderbook/engine.hpp"

#include <chrono>
#include <cstring>

namespace ob {

Engine::Engine(std::string_view base_dir, uint64_t flush_interval_ns)
    : base_dir_(base_dir)
    , flush_interval_ns_(flush_interval_ns)
    , wal_(base_dir)
    , combined_store_(base_dir)
    , query_engine_(std::make_unique<QueryEngine>(combined_store_, live_ptrs_, agg_))
{}

Engine::~Engine() {
    close();
}

void Engine::open() {
    // Replay WAL to restore any updates not yet in the columnar store.
    // (Simplified: we rebuild the columnar index from persisted segments;
    //  a full implementation would re-apply WAL records to the SoA buffers.)
    WALReplayer replayer(base_dir_);
    replayer.replay([this](const WALRecord& /*rec*/, const uint8_t* /*payload*/) {
        // WAL replay: in a full implementation, reconstruct DeltaUpdate from
        // the payload and re-apply to the SoA buffer / columnar store.
        // For now we rely on the columnar store's persisted segments.
    });

    // Rebuild columnar segment index from persisted meta.json files.
    combined_store_.open_existing();

    // Start background flush thread.
    stop_flush_.store(false, std::memory_order_relaxed);
    flush_thread_ = std::thread([this]() { flush_loop(); });
}

void Engine::close() {
    // Stop background flush thread first.
    stop_flush_.store(true, std::memory_order_relaxed);
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }

    // Final flush of all pending rows under the lock.
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // Group commit: sync any remaining WAL records.
        wal_.sync();
        flush_pending();
    }

    // Flush each per-symbol columnar store.
    for (auto& [key, store] : stores_) {
        store->flush_segment();
    }

    // Flush WAL to disk.
    wal_.flush();
}

ob_status_t Engine::apply_delta(const DeltaUpdate& delta, const Level* levels) {
    std::lock_guard<std::mutex> lock(mtx_);

    // 1. Write to WAL before any state mutation (Requirement 8.1).
    //    No fsync here — group commit via flush_loop() or close().
    wal_.append(delta, levels);

    // 2. Apply to SoA buffer using seqlock writer protocol.
    SoABuffer& buf = get_or_create_buffer(delta.symbol, delta.exchange);
    bool gap_detected = false;
    ob_status_t status = ob::apply_delta(buf, delta, levels, gap_detected);

    // 3. Record gap event in WAL if sequence number was non-consecutive (Requirement 1.5).
    if (gap_detected) {
        wal_.append_gap(delta.sequence_number, delta.timestamp_ns);
    }

    // 4. Enqueue SnapshotRows for background columnar flush + notify subscribers.
    for (uint16_t i = 0; i < delta.n_levels; ++i) {
        SnapshotRow row{};
        row.timestamp_ns    = delta.timestamp_ns;
        row.sequence_number = delta.sequence_number;
        row.side            = delta.side;
        row.level_index     = i;
        row.price           = levels[i].price;
        row.quantity        = levels[i].qty;
        row.order_count     = levels[i].cnt;

        pending_rows_.push_back({delta.symbol, delta.exchange, row});

        // 5. Notify streaming subscribers synchronously (within 1 µs budget, Requirement 10.9).
        query_engine_->notify_subscribers(delta.symbol, delta.exchange, row);
    }

    return status;
}

std::string Engine::execute(std::string_view sql, RowCallback cb) {
    return query_engine_->execute(sql, std::move(cb));
}

std::string Engine::parse(std::string_view sql, QueryAST& out) {
    return query_engine_->parse(sql, out);
}

std::string Engine::format(const QueryAST& ast) {
    return query_engine_->format(ast);
}

uint64_t Engine::subscribe(std::string_view sql, RowCallback cb) {
    return query_engine_->subscribe(sql, std::move(cb));
}

void Engine::unsubscribe(uint64_t id) {
    query_engine_->unsubscribe(id);
}

// ── Private helpers ───────────────────────────────────────────────────────────

SoABuffer& Engine::get_or_create_buffer(const std::string& symbol,
                                         const std::string& exchange) {
    const std::string key = symbol + "." + exchange;
    auto it = buffers_.find(key);
    if (it != buffers_.end()) return *it->second;

    auto buf = std::make_unique<SoABuffer>();
    buf->bid.depth = 0;
    buf->ask.depth = 0;
    buf->bid.version.store(0, std::memory_order_relaxed);
    buf->ask.version.store(0, std::memory_order_relaxed);
    buf->sequence_number.store(0, std::memory_order_relaxed);
    buf->last_timestamp_ns = 0;
    std::strncpy(buf->symbol,   symbol.c_str(),   sizeof(buf->symbol)   - 1);
    std::strncpy(buf->exchange, exchange.c_str(), sizeof(buf->exchange) - 1);

    live_ptrs_[key] = buf.get();
    auto& ref = *buf;
    buffers_[key] = std::move(buf);
    return ref;
}

ColumnarStore& Engine::get_or_create_store(const std::string& symbol,
                                            const std::string& exchange) {
    const std::string key = symbol + "." + exchange;
    auto it = stores_.find(key);
    if (it != stores_.end()) return *it->second;

    auto store = std::make_unique<ColumnarStore>(base_dir_);
    store->set_symbol_exchange(symbol, exchange);
    auto& ref = *store;
    stores_[key] = std::move(store);
    return ref;
}

void Engine::flush_loop() {
    const auto interval = std::chrono::nanoseconds(flush_interval_ns_);
    while (!stop_flush_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(interval);
        std::lock_guard<std::mutex> lock(mtx_);
        // Group commit: sync WAL to disk periodically instead of per-record.
        if (wal_.pending_sync_count() > 0) {
            wal_.sync();
        }
        flush_pending();
    }
}

void Engine::flush_pending() {
    // Drain pending_rows_ into per-symbol columnar stores.
    for (const auto& pr : pending_rows_) {
        ColumnarStore& store = get_or_create_store(pr.symbol, pr.exchange);
        store.append(pr.row);
    }
    pending_rows_.clear();
}

} // namespace ob
