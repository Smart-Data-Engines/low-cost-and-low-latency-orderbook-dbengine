#pragma once

// Version vector: what a node holds, per (symbol, exchange, origin).
//
// A node asks a peer for what it is missing by stating what it already has, in terms the
// peer can interpret in its own log. Byte offsets cannot do that — two independent WALs have
// no common scale, which is roadbook #61 — but a sequence number minted by an origin means
// the same thing on every node that received it.
//
// The entry carries a *frontier*, not a maximum: "I have everything from this origin up to
// here". See SequenceTracker::frontier() for why the distinction is the whole point.
//
// One serialisation serves two purposes: the frame sent to a peer, and the record written to
// the WAL so a restarted node knows what it holds. Both use the WALRecordV2 envelope with
// record_type = WAL_RECORD_VERSION_VECTOR, which means a node running the older protocol
// skips it as an unknown record type instead of disconnecting.

#include "orderbook/sequence_tracker.hpp"
#include "orderbook/wal.hpp"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace ob {

/// Wire size of one entry: char[32] key + uint16 origin + uint64 frontier.
inline constexpr size_t VV_ENTRY_SIZE = 42;
/// Payload header: uint16 entry_count.
inline constexpr size_t VV_HEADER_SIZE = 2;
/// entry_count == this means "I cannot state what I have — send everything".
inline constexpr uint16_t VV_TRUNCATED = 0xFFFF;

/// Serialise entries into a payload. `truncated` sends the "send everything" marker.
std::vector<uint8_t> serialize_version_vector(const std::vector<SequenceTracker::VectorEntry>& entries,
                                             bool truncated);

// ── Held sequence numbers ────────────────────────────────────────────────────
//
// The frontier says "everything up to here"; these are the numbers above it that arrived out of
// order. They live in their own WAL record rather than in the version vector, for one reason: the
// vector is also what peers read, and this is only ever read by the node that wrote it. Catch-up
// forwards `WAL_RECORD_DELTA` and nothing else, so a new record type changes no protocol.

/// Fixed part of one held entry: char[32] key + uint16 origin + uint16 range_count.
inline constexpr size_t HS_ENTRY_HEADER_SIZE = 36;
/// One inclusive range: uint64 first + uint64 last.
inline constexpr size_t HS_RANGE_SIZE = 16;
/// Payload header: uint16 entry_count.
inline constexpr size_t HS_HEADER_SIZE = 2;

/// Serialise held ranges for the WAL. Returns an empty payload when there is nothing to write.
std::vector<uint8_t> serialize_held_ranges(
        const std::vector<SequenceTracker::HeldRanges>& entries);

/// Parse a held-ranges payload. False on a malformed or truncated buffer, in which case `out` is
/// left empty — losing this state costs duplicate rows after a restart, never wrong data, so
/// refusing the whole payload is the safe answer to a byte that does not parse.
bool deserialize_held_ranges(const uint8_t* data, size_t len,
                            std::vector<SequenceTracker::HeldRanges>& out);

/// A peer's vector, ready to be asked "does it have this record?".
class PeerVector {
public:
    /// Empty (and therefore "has nothing") until deserialize() succeeds.
    bool deserialize(const uint8_t* data, size_t len);

    /// Everything the peer holds from `origin` for `key`; 0 means nothing.
    uint64_t frontier_for(const std::string& key, uint16_t origin) const;

    /// The entries as read, for a node restoring its own persisted vector.
    std::vector<SequenceTracker::VectorEntry> entries() const;

    bool   truncated() const { return truncated_; }
    size_t entry_count() const { return entries_.size(); }
    bool   received() const { return received_; }

    /// True when the peer said nothing, or said it cannot state what it has. Both answers
    /// mean the same thing to a sender: send everything you have.
    bool wants_everything() const { return !received_ || truncated_; }

private:
    struct Key {
        std::string key;
        uint16_t    origin;
        bool operator==(const Key& o) const { return origin == o.origin && key == o.key; }
    };
    struct KeyHash {
        size_t operator()(const Key& k) const {
            return std::hash<std::string>{}(k.key) ^ (static_cast<size_t>(k.origin) << 1);
        }
    };

    std::unordered_map<Key, uint64_t, KeyHash> entries_;
    bool truncated_{false};
    bool received_{false};
};

/// One (symbol, origin) pair where two nodes disagree about what they hold.
struct VectorGap {
    uint16_t    peer_node_id{0};
    std::string key;            ///< "SYMBOL.EXCHANGE"
    uint16_t    origin{0};
    uint64_t    from_seq{0};    ///< first sequence number the lagging side is missing
    uint64_t    to_seq{0};      ///< last sequence number the other side holds
};

/// Both directions of a comparison: what we lack, and what the peer lacks.
struct VectorDiff {
    std::vector<VectorGap> we_lack;
    std::vector<VectorGap> peer_lacks;
};

/// Compare our frontiers against a peer's.
///
/// A missing entry means "holds nothing here" on whichever side it is missing from, never
/// "holds everything" — the same asymmetry the catch-up filter relies on. Getting that backwards
/// is how a reconciliation pass would conclude there is nothing to repair while a peer sits on
/// data nobody else has.
VectorDiff compare_vectors(const std::vector<SequenceTracker::VectorEntry>& ours,
                           const PeerVector& theirs, uint16_t peer_node_id);

}  // namespace ob
