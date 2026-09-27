#pragma once

// ── ConflictResolver — LWW conflict resolution for multi-master replication ──
//
// Detects and resolves conflicts when two nodes write to the same price level
// (symbol, exchange, side, price) concurrently.  Uses Last-Writer-Wins (LWW)
// based on HLC timestamps with deterministic tie-break by node_id.
//
// Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6

#include "orderbook/hlc.hpp"

#include <atomic>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace ob {

// ── Conflict key (per-level) ──────────────────────────────────────────────────

struct ConflictKey {
    std::string symbol;
    std::string exchange;
    uint8_t     side{0};      // 0=bid, 1=ask
    int64_t     price{0};

    bool operator==(const ConflictKey& o) const {
        return symbol == o.symbol &&
               exchange == o.exchange &&
               side == o.side &&
               price == o.price;
    }
};

struct ConflictKeyHash {
    size_t operator()(const ConflictKey& k) const {
        // Combine hashes of all four fields.
        size_t h = std::hash<std::string>{}(k.symbol);
        h ^= std::hash<std::string>{}(k.exchange) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<uint8_t>{}(k.side) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int64_t>{}(k.price) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

// ── Conflict log entry ────────────────────────────────────────────────────────

struct ConflictEntry {
    ConflictKey  key;
    HLCTimestamp local_hlc;
    HLCTimestamp remote_hlc;
    uint16_t     local_origin{0};
    uint16_t     remote_origin{0};
    enum Result : uint8_t { LOCAL_WINS = 0, REMOTE_WINS = 1 } result{LOCAL_WINS};
    uint64_t     detected_at_ns{0};  // wall clock when conflict was detected
};

// ── Conflict resolution outcome ───────────────────────────────────────────────

enum class ConflictResolution {
    APPLY_REMOTE,   // remote HLC is newer → apply remote write
    REJECT_REMOTE,  // local HLC is newer → reject remote write
    NO_CONFLICT,    // no existing local state for this key, or the origin that wrote it last writes it again
    REJECT_STALE,   // the origin that wrote it last, with an update not newer than that: a late copy (#182)
};

// ── ConflictResolver ──────────────────────────────────────────────────────────

class ConflictResolver {
public:
    explicit ConflictResolver(size_t max_log_entries = 10000);
    ~ConflictResolver() = default;

    /// Resolve a conflict for a single price level.
    /// Compares remote_hlc with the last known HLC for the given key.
    /// Returns APPLY_REMOTE if remote is newer, REJECT_REMOTE if local is newer,
    /// NO_CONFLICT if no local state exists for this key.
    ///
    /// A conflict is two origins writing one level (#182). The origin that wrote the level last
    /// writing it again is a book's ordinary life - NO_CONFLICT when newer, REJECT_STALE when not -
    /// and neither enters the log, the counts or a line. Counting those, a mesh logged a conflict for
    /// nearly every replicated update: 301 985 lines, 61 MB, for 300 000 writes.
    ConflictResolution resolve(const ConflictKey& key,
                               const HLCTimestamp& remote_hlc,
                               uint16_t remote_origin);
    /// The same, at `now_ns` on the wall clock: what the log's window is measured against. A seam
    /// for the window's test; `resolve()` above passes `wall_clock_ns()`.
    ConflictResolution resolve(const ConflictKey& key,
                               const HLCTimestamp& remote_hlc,
                               uint16_t remote_origin,
                               uint64_t now_ns);

    /// A conflict opens a window of this length: it is logged at INFO with its details, and the ones
    /// inside the window are counted, and said as one line with the next conflict after it (#182).
    static constexpr uint64_t kLogWindowNs = 10'000'000'000ULL;

    /// Update the last known HLC for a key (called after successful apply).
    void update_hlc(const ConflictKey& key, const HLCTimestamp& hlc,
                    uint16_t origin);

    /// Get the last N conflict log entries (most recent last).
    std::vector<ConflictEntry> get_log(size_t limit = 100) const;

    /// Get total conflict count.
    uint64_t total_conflicts() const { return total_conflicts_.load(std::memory_order_relaxed); }

    /// Get per-symbol conflict counts.
    std::unordered_map<std::string, uint64_t> per_symbol_conflicts() const;

    /// Clear the conflict log (but NOT level_states_).
    void clear_log();

private:
    mutable std::mutex mtx_;

    // Per-level HLC tracking: key → (last_hlc, last_origin)
    struct LevelState {
        HLCTimestamp hlc;
        uint16_t     origin{0};
    };
    std::unordered_map<ConflictKey, LevelState, ConflictKeyHash> level_states_;

    // Conflict log (ring buffer)
    std::deque<ConflictEntry> log_;
    size_t max_log_entries_;

    // Metrics
    std::atomic<uint64_t> total_conflicts_{0};
    std::unordered_map<std::string, uint64_t> per_symbol_conflicts_;

    /// Append an entry to the ring buffer, trimming if necessary.
    void log_conflict(const ConflictEntry& entry);

    /// Say a conflict: at INFO when no window is open or the open one has passed - with the count of
    /// the ones inside it, if any - and at DEBUG inside the window. Caller holds `mtx_`.
    void say(const ConflictEntry& entry, uint64_t now_ns);
    bool     window_open_{false};
    uint64_t window_started_ns_{0};   ///< when the last INFO line was said
    uint64_t unsaid_{0};              ///< conflicts inside the window since that line
};

} // namespace ob
