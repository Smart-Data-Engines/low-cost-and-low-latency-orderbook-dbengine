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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace ob {

/// Where a symbol's numbers go on from once its numbering from before per-origin numbers is closed
/// (#187): every number below it is that numbering's, declared held for every origin of the symbol at
/// the close, and each origin numbers its records from here.
///
/// A constant, the same on every node, and not the highest old number each node holds: an origin
/// numbers on from its own highest, so a node that held more of the old numbers than the origin did
/// would take the origin's next records for ones it had, and one that held fewer would wait for
/// numbers nothing sends. Nodes that were not in step at the close agree on this without asking.
inline constexpr uint64_t kClosedNumberingBase = 1ULL << 48;

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

    /// Which origin this tracker's own numbers are (#184): the mesh node's id, or 0 without a mesh.
    ///
    /// A number another origin minted does not raise the local counter. The counter is one per
    /// symbol, and a sequence number means something only with its origin - so when two mesh nodes
    /// write one symbol, raising it by each other's numbers left holes in each origin's stream, every
    /// receiver's frontier stopped at the first, and a node that missed rows was judged to hold
    /// them. Without a mesh every record is origin 0, a replica's included, so a promoted replica
    /// still numbers on from its primary's numbers.
    void set_local_origin(uint16_t origin) { local_origin_ = origin; }

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
    /// never received, which is the failure #61 is about - and the one exception is
    /// `close_numbering()`, below, which names its cost.
    void declare_frontier(const std::string& key, uint16_t origin, uint64_t seq);

    /// Close `key`'s numbering from before per-origin numbers (#187): every origin this tracker knows
    /// for it holds every number below kClosedNumberingBase, and this node's own numbers of it go on
    /// from there. Returns how many origins' frontiers moved.
    ///
    /// The holes that numbering left in each origin's stream are other origins' numbers, not records
    /// that never arrived, and nothing else can say so: a frontier stopped at the first one stays
    /// there, the numbers above it fill the held set, and a redelivery past the set is stored twice
    /// (measured: 11 904 rows where 11 000 were written). The cost is a record this node really lacks
    /// below the base, which is not asked for again.
    std::size_t close_numbering(const std::string& key);

    /// Every symbol this tracker holds anything for.
    std::vector<std::string> keys() const;

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

    /// Every (symbol, origin) whose frontier moved since the last call, with the frontier it has now
    /// - each once, however often it moved - or `all` when a copy kept from these must be rebuilt
    /// from `export_vector()` instead: before the first call, and after `reset()`.
    ///
    /// For the copy of the vector peers are told (#180), which the flush tick keeps up to date at
    /// every tick under the engine's lock. Exporting the whole vector there cost 286 us at 4 000
    /// entries (p50, Release, i3-7100U) - a stall of every write, every tick anything moved; this
    /// costs what moved. A held number is not in the vector, so only a frontier counts.
    struct MovedFrontiers {
        bool                     all{false};
        std::vector<VectorEntry> moved;
        /// `listings()` as this take left it: a copy built from it holds every frontier that moved
        /// up to that listing, and whether one moved since is `listings()` being past it.
        uint64_t                 listings{0};
    };
    MovedFrontiers take_moved_frontiers();

    /// How many times a (symbol, origin) has gone onto the list `take_moved_frontiers()` hands out,
    /// and `reset()`s: monotonic, and readable **without** the engine's lock - the one thing here
    /// that is (#180 part D).
    ///
    /// After a take, the first frontier of any pair to move lists that pair, so "nothing listed
    /// since the take" is "no frontier moved since the take": a copy of the vector kept from the
    /// takes knows it is exact when this still equals the `listings` of its last one. The mesh
    /// manager asks that before it tells a peer it lacks nothing, from under its own lock, which
    /// the write path takes after the engine's - so it cannot ask the tracker itself. It rises at
    /// most once a pair a take, not once a record.
    uint64_t listings() const { return listings_.load(std::memory_order_acquire); }

    /// Records above the frontier held per (key, origin) before the set stops growing.
    ///
    /// Holding them is an optimisation — it lets a filled hole drain in one step. Dropping
    /// them only means the frontier stays put and the next catch-up asks for more.
    static constexpr std::size_t kMaxAboveFrontier = 4096;

    /// One (symbol, origin) pair's held-but-not-contiguous numbers, as inclusive ranges.
    ///
    /// Ranges rather than individual numbers because that is the shape the data has: catch-up
    /// delivers runs, so a held set of four thousand numbers above one gap is a single range.
    struct HeldRanges {
        std::string key;
        uint16_t    origin{0};
        /// Inclusive [first, last] pairs, ascending and non-adjacent.
        std::vector<std::pair<uint64_t, uint64_t>> ranges;
    };

    /// Everything held above the frontiers, for persistence.
    ///
    /// `max_ranges` bounds the total across all entries, because the WAL record that carries this
    /// has a 16-bit length. When it is hit, `truncated` is set and the entries that fit are still
    /// returned: every range that survives prevents a duplicate row after a restart, and the ones
    /// dropped only cost the duplicates they would have prevented.
    std::vector<HeldRanges> export_held(std::size_t max_ranges, bool& truncated) const;

    /// Restore held numbers from a previous run. Only raises: a number already known stays known,
    /// and a frontier is never moved by this, because these numbers are precisely the ones the
    /// frontier cannot cover.
    void import_held(const std::vector<HeldRanges>& held);

    /// Forget everything: every frontier, every held number and every local counter.
    ///
    /// For the one caller that is entitled to it — installing a snapshot, which replaces this
    /// node's contents wholesale. `import_own_vector()` only ever raises a frontier, so without
    /// this a frontier from the discarded contents would survive the discard and claim records
    /// that are no longer on disk.
    void reset();

    /// Number of symbols with any state. Logged at startup.
    std::size_t symbol_count() const { return symbols_.size(); }

    /// Changes to the numbers held above any frontier so far (#189). A writer that compares it with
    /// the count at its last write knows whether the held set moved without walking every frontier,
    /// which is what `fingerprint()` does.
    uint64_t held_version() const { return held_version_; }
    /// How many (symbol, origin) hold numbers above their frontier.
    std::size_t holding_count() const { return holding_.size(); }

private:
    struct OriginState {
        uint64_t           high_water{0};      ///< largest number seen; drives gap detection
        uint64_t           frontier{0};        ///< everything up to here has been seen
        std::set<uint64_t> above_frontier;     ///< seen but not contiguous yet
        bool               listed{false};      ///< in `moved_` since the last take
        bool               held_full{false};   ///< said so once; cleared when the frontier moves
    };

    struct SymbolState {
        uint64_t next_local{1};   ///< 0 is reserved for "nobody assigned one"
        std::unordered_map<uint16_t, OriginState> origins;
    };

    /// Record `seq` as seen from `origin` and advance the frontier as far as it now reaches.
    /// Returns whether the frontier moved - a redelivery, and a number held above it, do not.
    static bool note_seen(OriginState& st, uint64_t seq);

    /// Raise `st`'s frontier to `seq`: what is held below it is covered, and what is held just above
    /// joins it. Returns whether it moved. What declare_frontier() and the closes share.
    static bool raise_frontier(OriginState& st, uint64_t seq);

    /// A number at or past kClosedNumberingBase from an origin whose frontier is below it: that
    /// origin closed its numbering from before per-origin numbers where it is written (#187), so it is
    /// closed here too - for an origin this node did not know when it closed its own, or a node that
    /// holds no segment from before. Before anything is judged a gap. `key` is the map's own key.
    void close_if_past_base(const std::string& key, uint16_t origin, OriginState& st,
                            uint64_t seq);

    /// After anything that may have changed `st.above_frontier`, whose size was `held_before`: the
    /// count held_version() says, and the index export_held() reads (#189). `key` is the map's own
    /// key, as for mark_moved().
    void note_held(const std::string& key, uint16_t origin, OriginState& st, std::size_t held_before);

    /// List `st` for the next `take_moved_frontiers()`, once. `key` is the map's own key: the
    /// maps are node-based, so it and `st` stay where they are until `reset()`, which drops the
    /// list with them.
    void mark_moved(const std::string& key, uint16_t origin, OriginState& st);

    struct Moved {
        const std::string* key;
        uint16_t           origin;
        OriginState*       state;
    };

    /// One more listing; see listings(). Its only writer holds the engine's lock, so a load and a
    /// store, not a read-modify-write.
    void count_listing() {
        listings_.store(listings_.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }

    std::unordered_map<std::string, SymbolState> symbols_;
    uint16_t           local_origin_{0};   ///< see set_local_origin()
    std::vector<Moved> moved_;         ///< see take_moved_frontiers()
    bool               moved_all_{true};
    std::atomic<uint64_t> listings_{0};   ///< see listings()

    struct Holding {
        const std::string* key;
        uint16_t           origin;
    };
    /// Every state whose above_frontier is not empty, and whose it is (#189). The maps are
    /// node-based, so a state stays where it is until `reset()`, which empties this with them.
    std::unordered_map<const OriginState*, Holding> holding_;
    uint64_t held_version_{0};   ///< see held_version()
};

}  // namespace ob
