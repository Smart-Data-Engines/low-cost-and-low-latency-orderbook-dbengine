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

#include <cstddef>
#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

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

    /// Highest number N such that every number up to N has been seen from `origin`.
    ///
    /// This is what catch-up asks for and what anti-entropy compares, and it is deliberately
    /// not the maximum: a peer can receive live record 7 before catch-up delivers 6, and a
    /// maximum would report 6 as delivered — which is exactly how roadmap #61 lost rows. 0
    /// means nothing is held, so a peer sending 0 is asking for everything.
    uint64_t frontier(const std::string& key, uint16_t origin) const;

    /// Has this exact number already been seen from `origin` for `key`?
    ///
    /// The receive path needs this, not the frontier alone: catch-up deliberately
    /// over-delivers, and storage is append-only, so applying a record twice appends its rows
    /// twice. Measured before this existed: four outage cycles turned 9 written rows into 25
    /// stored ones. "Over-delivery is harmless" was an assumption, and it was wrong.
    bool has_seen(const std::string& key, uint16_t origin, uint64_t seq) const;

    /// How many out-of-order numbers are held above the frontier. Test seam.
    std::size_t above_frontier_size(const std::string& key, uint16_t origin) const;

    /// One entry of a version vector: what this node holds for one (symbol, origin).
    struct VectorEntry {
        std::string key;       ///< "SYMBOL.EXCHANGE"
        uint16_t    origin{0};
        uint64_t    frontier{0};
    };

    /// Everything this node holds, for the handshake's version vector.
    ///
    /// Returns an empty vector and sets `truncated` when there are more entries than
    /// `limit`: a peer that cannot state what it has is treated as having nothing, which
    /// costs bandwidth and never costs data.
    std::vector<VectorEntry> export_vector(std::size_t limit, bool& truncated) const;

    /// Declare that everything up to `seq` from `origin` is held, without proof.
    ///
    /// Sound for the **local** origin only, and only from the restored counter: a node mints
    /// its own numbers and applies them in the same critical section, so it cannot be missing
    /// one of its own records. Using this for a remote origin would claim records that were
    /// never received, which is the failure #61 is about.
    void declare_frontier(const std::string& key, uint16_t origin, uint64_t seq);

    /// Restore frontiers from this node's own persisted vector.
    ///
    /// Authoritative for every origin, unlike a peer's vector: this is what *we* recorded
    /// about our own contents. It is also the only way a restarted node learns what it holds
    /// from a remote origin, because segments carry no origin field and the WAL tail only
    /// reaches back to the last checkpoint.
    void import_own_vector(const std::vector<VectorEntry>& entries);

    /// A cheap fingerprint of every frontier, for "has anything changed since we last wrote
    /// the vector down" without serialising it each time.
    uint64_t fingerprint() const;

    /// Records above the frontier held per (key, origin) before the set stops growing.
    ///
    /// Holding them is an optimisation — it lets a filled hole drain in one step. Dropping
    /// them only means the frontier stays put and the next catch-up asks for more.
    static constexpr std::size_t kMaxAboveFrontier = 4096;

    /// Number of symbols with any state. Logged at startup.
    std::size_t symbol_count() const { return symbols_.size(); }

private:
    struct OriginState {
        uint64_t           high_water{0};      ///< largest number seen; drives gap detection
        uint64_t           frontier{0};        ///< everything up to here has been seen
        std::set<uint64_t> above_frontier;     ///< seen but not contiguous yet
    };

    struct SymbolState {
        uint64_t next_local{1};   ///< 0 is reserved for "nobody assigned one"
        std::unordered_map<uint16_t, OriginState> origins;
    };

    /// Record `seq` as seen from `origin` and advance the frontier as far as it now reaches.
    static void note_seen(OriginState& st, uint64_t seq);

    std::unordered_map<std::string, SymbolState> symbols_;
};

}  // namespace ob
