// ── ConflictResolver implementation ──────────────────────────────────────────
//
// LWW conflict resolution for multi-master replication.
// Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6

#include "orderbook/conflict_resolver.hpp"
#include "orderbook/wall_clock.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>

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
    // Wall-clock time for the conflict entry - which this comment promised while the line under it
    // read `steady_clock`, a count from this machine's boot (#163).
    return resolve(key, remote_hlc, remote_origin, wall_clock_ns());
}

ConflictResolution ConflictResolver::resolve(const ConflictKey& key,
                                             const HLCTimestamp& remote_hlc,
                                             uint16_t remote_origin,
                                             uint64_t now_ns) {
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

    // Compare using HLC total order (physical_ns → logical → node_id).
    // But for tie-break we only compare physical_ns and logical first,
    // then use node_id as the tie-breaker (higher node_id wins).
    const bool remote_newer = remote_hlc.physical_ns > local_hlc.physical_ns ||
                              (remote_hlc.physical_ns == local_hlc.physical_ns &&
                               remote_hlc.logical > local_hlc.logical);
    const bool remote_older = remote_hlc.physical_ns < local_hlc.physical_ns ||
                              (remote_hlc.physical_ns == local_hlc.physical_ns &&
                               remote_hlc.logical < local_hlc.logical);

    // The origin that wrote this level last, writing it again: in the order its own clock gives,
    // the next update of a level is the ordinary life of a book, not two writers disagreeing - and
    // counting it as a conflict logged one for nearly every replicated update (#182). Newer applies;
    // anything else from it is a late copy of what it already said.
    if (local_state.origin == remote_origin) {
        OB_LOG_DEBUG("conflict", "resolve: key=%s/%s/%u/%ld origin %u again, %s",
                     key.symbol.c_str(), key.exchange.c_str(),
                     static_cast<unsigned>(key.side), static_cast<long>(key.price),
                     static_cast<unsigned>(remote_origin), remote_newer ? "newer" : "not newer");
        return remote_newer ? ConflictResolution::NO_CONFLICT : ConflictResolution::REJECT_STALE;
    }

    // Two origins wrote this level: a conflict, decided last-writer-wins, with the higher node id
    // winning a tie.
    ConflictEntry entry{};
    entry.key = key;
    entry.local_hlc = local_hlc;
    entry.remote_hlc = remote_hlc;
    entry.local_origin = local_state.origin;
    entry.remote_origin = remote_origin;
    entry.detected_at_ns = now_ns;
    const bool remote_wins = remote_newer || (!remote_older && remote_hlc.node_id > local_hlc.node_id);
    entry.result = remote_wins ? ConflictEntry::REMOTE_WINS : ConflictEntry::LOCAL_WINS;
    log_conflict(entry);
    say(entry, now_ns);
    return remote_wins ? ConflictResolution::APPLY_REMOTE : ConflictResolution::REJECT_REMOTE;
}

// ── say ───────────────────────────────────────────────────────────────────────

void ConflictResolver::say(const ConflictEntry& e, uint64_t now_ns) {
    // Caller holds mtx_.
    const char* winner = e.result == ConflictEntry::REMOTE_WINS ? "REMOTE" : "LOCAL";
    const bool tie = e.remote_hlc.physical_ns == e.local_hlc.physical_ns &&
                     e.remote_hlc.logical == e.local_hlc.logical;
    if (window_open_ && now_ns - window_started_ns_ < kLogWindowNs) {
        ++unsaid_;
        OB_LOG_DEBUG("conflict",
                     "Conflict: %s wins%s for %s/%s/%u/%ld (origin %u against %u, "
                     "remote_hlc={%lu,%u,%u} local_hlc={%lu,%u,%u})",
                     winner, tie ? " (tie-break)" : "", e.key.symbol.c_str(),
                     e.key.exchange.c_str(), static_cast<unsigned>(e.key.side),
                     static_cast<long>(e.key.price), static_cast<unsigned>(e.remote_origin),
                     static_cast<unsigned>(e.local_origin),
                     static_cast<unsigned long>(e.remote_hlc.physical_ns),
                     static_cast<unsigned>(e.remote_hlc.logical),
                     static_cast<unsigned>(e.remote_hlc.node_id),
                     static_cast<unsigned long>(e.local_hlc.physical_ns),
                     static_cast<unsigned>(e.local_hlc.logical),
                     static_cast<unsigned>(e.local_hlc.node_id));
        return;
    }
    if (unsaid_ > 0) {
        // The window's count, said with the conflict that ends it; the counter and MM_CONFLICTS
        // have every one.
        OB_LOG_INFO("conflict",
                    "%llu more conflict(s) between origins in the %.0f s since the last line, and "
                    "now: %s wins%s for %s/%s/%u/%ld (origin %u against %u)",
                    static_cast<unsigned long long>(unsaid_),
                    static_cast<double>(now_ns - window_started_ns_) / 1e9, winner,
                    tie ? " (tie-break)" : "", e.key.symbol.c_str(), e.key.exchange.c_str(),
                    static_cast<unsigned>(e.key.side), static_cast<long>(e.key.price),
                    static_cast<unsigned>(e.remote_origin), static_cast<unsigned>(e.local_origin));
    } else {
        OB_LOG_INFO("conflict",
                    "Conflict detected: %s wins%s for %s/%s/%u/%ld (origin %u against %u, "
                    "remote_hlc={%lu,%u,%u} local_hlc={%lu,%u,%u}); more within %.0f s are "
                    "counted, not logged",
                    winner, tie ? " (tie-break)" : "", e.key.symbol.c_str(),
                    e.key.exchange.c_str(), static_cast<unsigned>(e.key.side),
                    static_cast<long>(e.key.price), static_cast<unsigned>(e.remote_origin),
                    static_cast<unsigned>(e.local_origin),
                    static_cast<unsigned long>(e.remote_hlc.physical_ns),
                    static_cast<unsigned>(e.remote_hlc.logical),
                    static_cast<unsigned>(e.remote_hlc.node_id),
                    static_cast<unsigned long>(e.local_hlc.physical_ns),
                    static_cast<unsigned>(e.local_hlc.logical),
                    static_cast<unsigned>(e.local_hlc.node_id),
                    static_cast<double>(kLogWindowNs) / 1e9);
    }
    window_open_       = true;
    window_started_ns_ = now_ns;
    unsaid_            = 0;
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
