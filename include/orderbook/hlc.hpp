#pragma once

// ── Hybrid Logical Clock — HLCTimestamp and HybridLogicalClock ───────────────
//
// HLCTimestamp is a 12-byte timestamp combining physical wall-clock time
// (nanoseconds), a logical counter, and a node identifier.  It provides a
// total order suitable for Last-Writer-Wins conflict resolution in
// multi-master replication.
//
// HybridLogicalClock is a thread-safe clock that generates monotonically
// increasing HLCTimestamp values, merging local and remote events to
// preserve causal ordering.
//
// Requirements: 1.2, 1.6, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6

#include "orderbook/logger.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace ob {

// ── HLC Timestamp ─────────────────────────────────────────────────────────────
// 12 bytes: physical_ns (8) + logical (2) + node_id (2)
// Comparison order: physical_ns → logical → node_id

/// Bytes an HLCTimestamp occupies on the wire. Not `sizeof(HLCTimestamp)`: the struct is laid out
/// for the CPU and written to the wire field by field, which are two different jobs.
inline constexpr size_t HLC_WIRE_SIZE = 12;

struct HLCTimestamp {
    uint64_t physical_ns{0};   // physical time in nanoseconds (wall clock)
    uint16_t logical{0};       // logical counter (incremented when physical is equal)
    uint16_t node_id{0};       // node identifier (tie-break in LWW)

    // ── Comparison (total order) ──────────────────────────────────────────────
    // Order: physical_ns → logical → node_id
    // Guarantees deterministic ordering for any two HLCTimestamp values.

    bool operator<(const HLCTimestamp& o) const {
        if (physical_ns != o.physical_ns) return physical_ns < o.physical_ns;
        if (logical != o.logical) return logical < o.logical;
        return node_id < o.node_id;
    }

    bool operator>(const HLCTimestamp& o) const { return o < *this; }

    bool operator<=(const HLCTimestamp& o) const { return !(o < *this); }

    bool operator>=(const HLCTimestamp& o) const { return !(*this < o); }

    bool operator==(const HLCTimestamp& o) const {
        return physical_ns == o.physical_ns &&
               logical == o.logical &&
               node_id == o.node_id;
    }

    bool operator!=(const HLCTimestamp& o) const { return !(*this == o); }

    // ── Binary serialization (12 bytes, little-endian) ────────────────────────

    /// Serialize to 12 bytes in little-endian format.
    void serialize(uint8_t out[12]) const {
        std::memcpy(out, &physical_ns, 8);
        std::memcpy(out + 8, &logical, 2);
        std::memcpy(out + 10, &node_id, 2);
    }

    /// Deserialize from 12 bytes in little-endian format.
    static HLCTimestamp deserialize(const uint8_t data[12]) {
        HLCTimestamp ts{};
        std::memcpy(&ts.physical_ns, data, 8);
        std::memcpy(&ts.logical, data + 8, 2);
        std::memcpy(&ts.node_id, data + 10, 2);
        return ts;
    }

    // ── Text serialization ────────────────────────────────────────────────────
    // Format: "<physical_ns>.<logical>.<node_id>"
    // Example: "1700000000000000000.42.3"

    /// Serialize to text: "<physical_ns>.<logical>.<node_id>"
    std::string to_string() const;

    /// Parse from text. Returns nullopt on failure.
    static std::optional<HLCTimestamp> from_string(std::string_view str);

    /// Parse from text with descriptive error message on failure.
    static std::optional<HLCTimestamp> from_string(std::string_view str,
                                                    std::string& error);

    // ── Pretty-print ──────────────────────────────────────────────────────────
    // Format: "2024-01-15T10:30:00.123456789Z L=42 N=3"

    /// Format as human-readable string with ISO 8601 date.
    std::string pretty_print() const;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Returns true if all fields are zero (default-constructed).
    bool is_zero() const { return physical_ns == 0 && logical == 0 && node_id == 0; }
};

// The struct used to be `#pragma pack(1)` so that its size matched the wire form. That made every
// in-memory use of it misaligned — an HLCTimestamp inside a `std::vector<ConflictEntry>` puts
// `physical_ns` on a 4-byte boundary, and binding a `const uint64_t&` to it is undefined behaviour.
// UBSan reported exactly that, and it is not theoretical: unaligned 8-byte access is a fault on
// some targets and a silently slower path on others, in an engine written for specific hardware.
//
// Packing was never needed. `serialize()` and `deserialize()` copy field by field at fixed offsets,
// so the wire layout does not depend on the struct layout at all. What matters is asserted below:
// the field order and offsets the wire form is built from. `sizeof` is now 16 and nothing reads it.
static_assert(offsetof(HLCTimestamp, physical_ns) == 0, "wire layout: physical_ns first");
static_assert(offsetof(HLCTimestamp, logical) == 8,     "wire layout: logical at offset 8");
static_assert(offsetof(HLCTimestamp, node_id) == 10,    "wire layout: node_id at offset 10");
static_assert(alignof(HLCTimestamp) == alignof(uint64_t),
              "HLCTimestamp must be naturally aligned: it is compared and copied in hot paths");

// ── HybridLogicalClock ────────────────────────────────────────────────────────
// Thread-safe HLC implementation.
// Guarantees causal ordering: if A → B (A causally precedes B),
// then HLC(A) < HLC(B).

class HybridLogicalClock {
public:
    explicit HybridLogicalClock(uint16_t node_id);
    ~HybridLogicalClock() = default;

    HybridLogicalClock(const HybridLogicalClock&) = delete;
    HybridLogicalClock& operator=(const HybridLogicalClock&) = delete;

    /// Generate a new HLC timestamp for a local event.
    /// Algorithm:
    ///   new_physical = max(wall_clock_ns(), last_.physical_ns)
    ///   if new_physical > last_.physical_ns:
    ///       logical = 0
    ///   else:
    ///       logical = last_.logical + 1
    ///   last_ = {new_physical, logical, node_id_}
    ///   return last_
    HLCTimestamp tick_local();

    /// Update HLC after receiving a remote timestamp.
    /// Algorithm:
    ///   new_physical = max(wall_clock_ns(), last_.physical_ns, remote.physical_ns)
    ///   if new_physical > last_.physical_ns && new_physical > remote.physical_ns:
    ///       logical = 0
    ///   elif new_physical == last_.physical_ns && new_physical == remote.physical_ns:
    ///       logical = max(last_.logical, remote.logical) + 1
    ///   elif new_physical == last_.physical_ns:
    ///       logical = last_.logical + 1
    ///   elif new_physical == remote.physical_ns:
    ///       logical = remote.logical + 1
    ///   last_ = {new_physical, logical, node_id_}
    ///   return last_
    HLCTimestamp tick_receive(const HLCTimestamp& remote);

    /// Get the current (last generated) HLC timestamp.
    HLCTimestamp current() const;

    /// Get the node_id of this clock.
    uint16_t node_id() const { return node_id_; }

    /// Get the maximum observed drift (difference between wall clock and HLC physical).
    /// Used for the ob_mm_hlc_drift_ns metric.
    int64_t max_drift_ns() const;

    /// Reset the drift tracker (after logging a warning).
    void reset_drift();

private:
    uint16_t node_id_;
    mutable std::mutex mtx_;
    HLCTimestamp last_{};
    int64_t max_drift_ns_{0};

    /// Get the current physical time (wall clock) in nanoseconds.
    static uint64_t wall_clock_ns();
};

} // namespace ob
