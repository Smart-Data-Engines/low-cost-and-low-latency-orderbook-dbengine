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
#include <string_view>
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

// ── A vector of any size, in parts (#177) ────────────────────────────────────
//
// One record's length is a uint16_t, so the single-record format above holds 1 560 entries, and past
// them a vector used to be the "send everything" marker - every reconciliation resent the whole WAL,
// and a joiner never asked for a snapshot. A vector that does not fit one record goes as parts of one
// generation, a record type of their own (WAL_RECORD_VERSION_VECTOR_PART), which a build that does not
// know it skips - as it skips nothing more than the vector it could not have read anyway.

/// Entries the single-record format holds.
inline constexpr size_t VV_MAX_SINGLE_ENTRIES = (WAL_MAX_PAYLOAD_LEN - VV_HEADER_SIZE) / VV_ENTRY_SIZE;
/// A part's header: uint32 generation, uint16 part, uint16 parts, uint16 entry_count.
inline constexpr size_t VV_PART_HEADER_SIZE = 10;
/// Entries one part holds.
inline constexpr size_t VV_MAX_PART_ENTRIES = (WAL_MAX_PAYLOAD_LEN - VV_PART_HEADER_SIZE) / VV_ENTRY_SIZE;
/// The largest vector stated at all - a memory bound, 42 MB on the wire, not a record's. Past it, the
/// "send everything" marker, as every vector past one record used to be.
inline constexpr size_t VV_MAX_ENTRIES = 1'000'000;

/// One record of a vector: its type (WAL_RECORD_VERSION_VECTOR or WAL_RECORD_VERSION_VECTOR_PART) and
/// its payload.
struct VectorRecord {
    uint8_t              record_type{0};
    std::vector<uint8_t> payload;
};

/// The records that carry `entries`: one of the single-record format when they fit it - so a mesh below
/// the old limit sees nothing new - else parts 0..N-1 of `generation`, in order. `truncated`, or more
/// than VV_MAX_ENTRIES: one single-format record with the "send everything" marker.
std::vector<VectorRecord> serialize_version_vector_records(
    const std::vector<SequenceTracker::VectorEntry>& entries, bool truncated, uint32_t generation);

/// The vector as a mesh snapshot's metadata carries it, in a block whose length is a uint32 (#177): the
/// single-record format up to VV_MAX_SINGLE_ENTRIES, and past it a block of parts - the count
/// VV_PARTS_BLOB, which the single-record format never uses, a uint32 number of parts, and each part
/// behind its uint32 length. A receiver of a build before this reads the marker as an entry count, does
/// not find the entries, and refuses the snapshot - as it refused one whose vector said "send
/// everything", which is what a vector that size was before.
inline constexpr uint16_t VV_PARTS_BLOB = 0xFFFE;
std::vector<uint8_t> serialize_version_vector_blob(
    const std::vector<SequenceTracker::VectorEntry>& entries);
/// Parse a block written by serialize_version_vector_blob(). False when it is not one; true with
/// `says_send_everything` when it is the single-record format's marker.
bool deserialize_version_vector_blob(const uint8_t* data, size_t len,
                                     std::vector<SequenceTracker::VectorEntry>& out,
                                     bool& says_send_everything);

/// Puts a vector sent in parts back together. Part 0 opens an assembly; every next part has to continue
/// it - the same generation and part count, the next number - and the last completes it. Parts of one
/// vector are written and sent together and in order, so anything else means what was being assembled
/// will not complete: it is dropped, and the vector before it stands.
class VectorAssembler {
public:
    enum class Step {
        Incomplete,   ///< a part taken; more to come
        Complete,     ///< the last part: take() hands the vector out
        Dropped,      ///< out of sequence: what was being assembled is gone (this part may open anew)
        Malformed,    ///< not a part: its header or its length does not add up
    };
    Step add(const uint8_t* data, size_t len);
    /// The vector the last Complete assembled, moved out.
    std::vector<SequenceTracker::VectorEntry> take() { return std::move(complete_); }
    bool assembling() const { return open_; }

private:
    bool     open_{false};
    uint32_t generation_{0};
    uint16_t parts_{0};
    uint16_t next_{0};
    std::vector<SequenceTracker::VectorEntry> building_;
    std::vector<SequenceTracker::VectorEntry> complete_;
};

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

/// The hash of one (key, origin) pair, of a key held as a string or looked up through a view of one
/// (std::hash gives both the same value).
inline size_t hash_vector_key(std::string_view key, uint16_t origin) {
    return std::hash<std::string_view>{}(key) ^ (static_cast<size_t>(origin) << 1);
}

/// A peer's vector, ready to be asked "does it have this record?".
class PeerVector {
public:
    /// Empty (and therefore "has nothing") until deserialize() succeeds.
    bool deserialize(const uint8_t* data, size_t len);
    /// One part of a vector sent in parts (#177). True when it completed a vector, which then replaces
    /// what the peer said before; until then, and when the assembly is dropped, that stands.
    bool deserialize_part(const uint8_t* data, size_t len);

    /// Everything the peer holds from `origin` for `key`; 0 means nothing.
    uint64_t frontier_for(const std::string& key, uint16_t origin) const;
    /// The frontier the peer listed for (`key`, `origin`), or nullptr when it did not list the
    /// pair - which frontier_for() reads as 0, and a comparison needs told apart (#177).
    const uint64_t* find(std::string_view key, uint16_t origin) const;

    /// The entries as read, for a node restoring its own persisted vector.
    std::vector<SequenceTracker::VectorEntry> entries() const;
    /// Every (key, origin, frontier), without copying them out: what a comparison walks (#177 - at
    /// 50 000 entries the copy entries() makes was a malloc per key on the mesh's io loop).
    template <typename F>
    void for_each(F&& f) const {
        for (const auto& [k, frontier] : entries_) f(k.key, k.origin, frontier);
    }

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
    };
    /// A key looked up without building a string for it (#177): frontier_for() made a copy of the
    /// key, and a malloc, for every entry of ours at every comparison.
    struct KeyView {
        std::string_view key;
        uint16_t         origin;
    };
    struct KeyHash {
        using is_transparent = void;
        size_t operator()(const Key& k) const { return hash_vector_key(k.key, k.origin); }
        size_t operator()(const KeyView& k) const { return hash_vector_key(k.key, k.origin); }
    };
    struct KeyEq {
        using is_transparent = void;
        template <typename A, typename B>
        bool operator()(const A& a, const B& b) const {
            return a.origin == b.origin && std::string_view(a.key) == std::string_view(b.key);
        }
    };

    std::unordered_map<Key, uint64_t, KeyHash, KeyEq> entries_;
    bool truncated_{false};
    bool received_{false};
    VectorAssembler assembler_;
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
///
/// `ours` lists each (key, origin) at most once, as SequenceTracker::export_vector() and the
/// engine's copy of it do: a peer whose every pair was found among ours has no pair left to walk.
VectorDiff compare_vectors(const std::vector<SequenceTracker::VectorEntry>& ours,
                           const PeerVector& theirs, uint16_t peer_node_id);

}  // namespace ob
