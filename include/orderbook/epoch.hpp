#pragma once

// ── Epoch subsystem — WAL_RECORD_EPOCH and EpochValue ────────────────────────
//
// The epoch is a monotonically increasing uint64 counter incremented on every
// primary promotion.  It is stored in the WAL as an Epoch_Record (type=5) and
// attached to every broadcast WAL message as a Fence_Token.
//
// EpochValue is a struct (not a bare uint64) so that it can be extended to a
// vector clock for multi-master replication (#16) without breaking the WAL
// format — the payload length in the WALRecord header will simply grow.

#include <cstdint>

namespace ob {

// ── Record type ──────────────────────────────────────────────────────────────
inline constexpr uint8_t WAL_RECORD_EPOCH = 5;

// ── EpochValue ───────────────────────────────────────────────────────────────
/// Epoch value — scalar today, extensible to vector clock for multi-master.
/// The WAL payload stores this as a little-endian uint64.
struct EpochValue {
    uint64_t term{0};  // monotonically increasing per-cluster epoch

    bool operator<(const EpochValue& o)  const { return term < o.term; }
    bool operator<=(const EpochValue& o) const { return term <= o.term; }
    bool operator==(const EpochValue& o) const { return term == o.term; }
    bool operator!=(const EpochValue& o) const { return term != o.term; }
    bool operator>(const EpochValue& o)  const { return term > o.term; }
    bool operator>=(const EpochValue& o) const { return term >= o.term; }

    /// Return a new EpochValue with term incremented by 1.
    EpochValue incremented() const { return EpochValue{term + 1}; }
};

// ── Serialization ────────────────────────────────────────────────────────────
/// Serialize epoch to 8-byte little-endian payload for WAL_RECORD_EPOCH.
void epoch_to_payload(const EpochValue& epoch, uint8_t out[8]);

/// Deserialize epoch from 8-byte little-endian payload.
EpochValue epoch_from_payload(const uint8_t data[8]);

} // namespace ob
