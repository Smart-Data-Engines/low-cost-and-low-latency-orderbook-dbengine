#pragma once

// Sequence numbers, assigned per (symbol, exchange) and tracked per origin.
//
// A sequence number belongs to the *origin* that produced the update, not to the node
// storing it. A node numbers only the writes it accepted from a client, and never
// renumbers anything that arrived from somewhere else — a replica renumbering its
// primary's stream, or a multi-master node renumbering a peer's, would make catch-up and
// reconciliation compare numbers minted by different nodes, which is the same class of
// error as comparing byte offsets across independent WALs (roadmap #61).
//
// Why gap detection lives here rather than in SoABuffer: that buffer holds one sequence
// number, so it cannot tell a gap from two origins interleaving. In multi-master every
// interleave would look like a hole. That is the real reason the mechanism was switched
// off by a zero rather than merely forgotten — nothing filled the field in, so
// `prev_seq != 0` never held and the check never ran.

#include <cstdint>
#include <string>
#include <unordered_map>

namespace ob {

/// Per-symbol sequence state: the local counter, plus the last number seen from each origin.
class SequenceTracker {
public:
    /// What observe() decided about one update.
    struct Decision {
        uint64_t sequence_number{0};  ///< assigned, or passed through unchanged
        bool     assigned{false};     ///< true when this tracker minted the number
        bool     gap{false};          ///< a hole in this origin's stream
        uint64_t expected{0};         ///< the number a gap was expecting (0 when no gap)
    };

    /// Observe an update for `key` ("SYMBOL.EXCHANGE") from `origin`.
    ///
    /// `sequence_number == 0` means nobody has assigned one, so the local counter does. A
    /// non-zero number came from whoever originated the record and passes through
    /// untouched. Either way the origin's high-water mark is advanced, and a number that
    /// is not exactly one past the previous one for that origin is reported as a gap.
    ///
    /// The first record seen from an origin is never a gap: there is no previous number to
    /// be one past.
    Decision observe(const std::string& key, uint16_t origin, uint64_t sequence_number);

    /// Raise the local counter so the next assigned number is greater than `seq`.
    ///
    /// Called with what is already durable — the highest number in each segment, and every
    /// number replayed from the WAL tail — so a restart cannot hand out a number twice.
    /// Only ever raises; a lower value is ignored, which is what makes it safe to call
    /// from several sources in any order.
    void raise_local(const std::string& key, uint64_t seq);

    /// Record that `seq` from `origin` was already seen, without reporting a gap.
    ///
    /// For WAL replay: those records were written once already, and any gap between them
    /// was recorded then. Replay re-reporting it would append a second GAP record for the
    /// same hole on every restart.
    void seed(const std::string& key, uint16_t origin, uint64_t seq);

    /// Next number the local counter would assign for `key` (1 when unseen). Test seam.
    uint64_t peek_next_local(const std::string& key) const;

    /// Last number seen from `origin` for `key`, or 0 if none. Test seam.
    uint64_t high_water(const std::string& key, uint16_t origin) const;

    /// Number of symbols with any state. Logged at startup.
    std::size_t symbol_count() const { return symbols_.size(); }

private:
    struct SymbolState {
        uint64_t next_local{1};   ///< 0 is reserved for "nobody assigned one"
        std::unordered_map<uint16_t, uint64_t> origin_high_water;
    };

    std::unordered_map<std::string, SymbolState> symbols_;
};

}  // namespace ob
