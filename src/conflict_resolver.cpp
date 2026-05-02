// ── ConflictResolver implementation ──────────────────────────────────────────
//
// LWW conflict resolution for multi-master replication.
// Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6

#include "orderbook/conflict_resolver.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>
#include <chrono>

namespace ob {

// ── Constructor ───────────────────────────────────────────────────────────────

ConflictResolver::ConflictResolver(size_t max_log_entries)
    : max_log_entries_(max_log_entries) {
    OB_LOG_DEBUG("conflict", "ConflictResolver created: max_log_entries=%zu",
                 max_log_entries);
}

// ── resolve ───────────────────────────────────────────────────────────────────

ConflictResolution ConflictResolver::resolve(const ConflictKey& key,
                                             const HLCTimestamp& remote_hlc,
                                             uint16_t remote_origin) {
    std::lock_guard<std::mutex> lock(mtx_);

    auto it = level_states_.find(key);
    if (it == level_states_.end()) {
        OB_LOG_DEBUG("conflict",
                     "resolve: key=%s/%s/%u/%ld no local state → NO_CONFLICT",
                     key.symbol.c_str(), key.exchange.c_str(),
                     static_cast<unsigned>(key.side), static_cast<long>(key.price));
        return ConflictResolution::NO_CONFLICT;
    }

    const auto& local_state = it->second;
    const auto& local_hlc = local_state.hlc;

    // Get wall-clock time for the conflict entry.
    const auto now_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());

    // Compare using HLC total order (physical_ns → logical → node_id).
    // But for tie-break we only compare physical_ns and logical first,
    // then use node_id as the tie-breaker (higher node_id wins).

    if (remote_hlc.physical_ns > local_hlc.physical_ns ||
        (remote_hlc.physical_ns == local_hlc.physical_ns &&
         remote_hlc.logical > local_hlc.logical)) {
        // Remote is strictly newer (ignoring node_id).
        ConflictEntry entry{};
        entry.key = key;
        entry.local_hlc = local_hlc;
        entry.remote_hlc = remote_hlc;
        entry.local_origin = local_state.origin;
        entry.remote_origin = remote_origin;
        entry.result = ConflictEntry::REMOTE_WINS;
        entry.detected_at_ns = now_ns;
        log_conflict(entry);

        OB_LOG_INFO("conflict",
                    "Conflict detected: REMOTE wins for %s/%s/%u/%ld "
                    "(remote_hlc={%lu,%u,%u} > local_hlc={%lu,%u,%u})",
                    key.symbol.c_str(), key.exchange.c_str(),
                    static_cast<unsigned>(key.side), static_cast<long>(key.price),
                    static_cast<unsigned long>(remote_hlc.physical_ns),
                    static_cast<unsigned>(remote_hlc.logical),
                    static_cast<unsigned>(remote_hlc.node_id),
                    static_cast<unsigned long>(local_hlc.physical_ns),
                    static_cast<unsigned>(local_hlc.logical),
                    static_cast<unsigned>(local_hlc.node_id));

        return ConflictResolution::APPLY_REMOTE;
    }

    if (remote_hlc.physical_ns < local_hlc.physical_ns ||
        (remote_hlc.physical_ns == local_hlc.physical_ns &&
         remote_hlc.logical < local_hlc.logical)) {
        // Local is strictly newer (ignoring node_id).
        ConflictEntry entry{};
        entry.key = key;
        entry.local_hlc = local_hlc;
        entry.remote_hlc = remote_hlc;
        entry.local_origin = local_state.origin;
        entry.remote_origin = remote_origin;
        entry.result = ConflictEntry::LOCAL_WINS;
        entry.detected_at_ns = now_ns;
        log_conflict(entry);

        OB_LOG_INFO("conflict",
                    "Conflict detected: LOCAL wins for %s/%s/%u/%ld "
                    "(local_hlc={%lu,%u,%u} > remote_hlc={%lu,%u,%u})",
                    key.symbol.c_str(), key.exchange.c_str(),
                    static_cast<unsigned>(key.side), static_cast<long>(key.price),
                    static_cast<unsigned long>(local_hlc.physical_ns),
                    static_cast<unsigned>(local_hlc.logical),
                    static_cast<unsigned>(local_hlc.node_id),
                    static_cast<unsigned long>(remote_hlc.physical_ns),
                    static_cast<unsigned>(remote_hlc.logical),
                    static_cast<unsigned>(remote_hlc.node_id));

        return ConflictResolution::REJECT_REMOTE;
    }

    // Equal physical_ns and logical — tie-break by node_id (higher wins).
    if (remote_hlc.node_id > local_hlc.node_id) {
        ConflictEntry entry{};
        entry.key = key;
        entry.local_hlc = local_hlc;
        entry.remote_hlc = remote_hlc;
        entry.local_origin = local_state.origin;
        entry.remote_origin = remote_origin;
        entry.result = ConflictEntry::REMOTE_WINS;
        entry.detected_at_ns = now_ns;
        log_conflict(entry);

        OB_LOG_INFO("conflict",
                    "Conflict detected: REMOTE wins (tie-break) for %s/%s/%u/%ld "
                    "(remote_node=%u > local_node=%u)",
                    key.symbol.c_str(), key.exchange.c_str(),
                    static_cast<unsigned>(key.side), static_cast<long>(key.price),
                    static_cast<unsigned>(remote_hlc.node_id),
                    static_cast<unsigned>(local_hlc.node_id));

        return ConflictResolution::APPLY_REMOTE;
    }

    // local node_id >= remote node_id → local wins.
    ConflictEntry entry{};
    entry.key = key;
    entry.local_hlc = local_hlc;
    entry.remote_hlc = remote_hlc;
    entry.local_origin = local_state.origin;
    entry.remote_origin = remote_origin;
    entry.result = ConflictEntry::LOCAL_WINS;
    entry.detected_at_ns = now_ns;
    log_conflict(entry);

    OB_LOG_INFO("conflict",
                "Conflict detected: LOCAL wins (tie-break) for %s/%s/%u/%ld "
                "(local_node=%u >= remote_node=%u)",
                key.symbol.c_str(), key.exchange.c_str(),
                static_cast<unsigned>(key.side), static_cast<long>(key.price),
                static_cast<unsigned>(local_hlc.node_id),
                static_cast<unsigned>(remote_hlc.node_id));

    return ConflictResolution::REJECT_REMOTE;
}

// ── update_hlc ────────────────────────────────────────────────────────────────

void ConflictResolver::update_hlc(const ConflictKey& key,
                                  const HLCTimestamp& hlc,
                                  uint16_t origin) {
    std::lock_guard<std::mutex> lock(mtx_);
    level_states_[key] = LevelState{hlc, origin};
    OB_LOG_DEBUG("conflict",
                 "update_hlc: key=%s/%s/%u/%ld hlc={%lu,%u,%u} origin=%u",
                 key.symbol.c_str(), key.exchange.c_str(),
                 static_cast<unsigned>(key.side), static_cast<long>(key.price),
                 static_cast<unsigned long>(hlc.physical_ns),
                 static_cast<unsigned>(hlc.logical),
                 static_cast<unsigned>(hlc.node_id),
                 static_cast<unsigned>(origin));
}

// ── log_conflict ──────────────────────────────────────────────────────────────

void ConflictResolver::log_conflict(const ConflictEntry& entry) {
    // Caller already holds mtx_.
    log_.push_back(entry);
    if (log_.size() > max_log_entries_) {
        log_.pop_front();
    }
    total_conflicts_.fetch_add(1, std::memory_order_relaxed);
    per_symbol_conflicts_[entry.key.symbol]++;
}

// ── get_log ───────────────────────────────────────────────────────────────────

std::vector<ConflictEntry> ConflictResolver::get_log(size_t limit) const {
    std::lock_guard<std::mutex> lock(mtx_);
    const size_t count = std::min(limit, log_.size());
    // Return the most recent `count` entries.
    return {log_.end() - static_cast<std::ptrdiff_t>(count), log_.end()};
}

// ── per_symbol_conflicts ──────────────────────────────────────────────────────

std::unordered_map<std::string, uint64_t>
ConflictResolver::per_symbol_conflicts() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return per_symbol_conflicts_;
}

// ── clear_log ─────────────────────────────────────────────────────────────────

void ConflictResolver::clear_log() {
    std::lock_guard<std::mutex> lock(mtx_);
    log_.clear();
    total_conflicts_.store(0, std::memory_order_relaxed);
    per_symbol_conflicts_.clear();
    OB_LOG_DEBUG("conflict", "Conflict log cleared");
}

} // namespace ob
