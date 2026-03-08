#pragma once

#include <cstdint>

namespace ob {

// ── DeltaUpdate ───────────────────────────────────────────────────────────────
// Wire/WAL format for an incremental orderbook update.
// Carries one or more price-level changes for a single side of a single symbol.

struct Level {
    int64_t  price;   // price in smallest currency sub-unit (e.g. tenths of a cent)
    uint64_t qty;     // quantity (unsigned 64-bit)
    uint32_t cnt;     // order count (unsigned 32-bit)
    uint32_t _pad{};  // explicit padding for alignment
};

struct DeltaUpdate {
    char     symbol[32];          // null-terminated symbol identifier
    char     exchange[32];        // null-terminated exchange identifier
    uint64_t sequence_number;     // monotonically increasing per (symbol, exchange)
    uint64_t timestamp_ns;        // nanosecond-precision Unix timestamp
    uint8_t  side;                // 0 = bid, 1 = ask
    uint8_t  _pad0[1]{};
    uint16_t n_levels;            // number of entries in levels[]
    uint8_t  _pad1[4]{};
    // Variable-length levels array follows in serialised form.
    // In-memory callers pass a pointer separately; see apply_delta helpers.
};

// Maximum levels per side (Requirement 1.2)
inline constexpr uint32_t MAX_LEVELS = 1000;

// Side constants
inline constexpr uint8_t SIDE_BID = 0;
inline constexpr uint8_t SIDE_ASK = 1;

// ── SnapshotRow ───────────────────────────────────────────────────────────────
// A single row in the columnar store, representing one price level at one
// point in time.  On disk, price is delta+zigzag encoded and qty is Simple8b
// encoded; this struct holds the decoded (logical) values.

struct SnapshotRow {
    uint64_t timestamp_ns;      // nanosecond-precision Unix timestamp
    uint64_t sequence_number;   // sequence number of the update that produced this row
    uint8_t  side;              // 0 = bid, 1 = ask
    uint8_t  _pad0[1]{};
    uint16_t level_index;       // 0-based index within the side (0 = best)
    uint8_t  _pad1[4]{};
    int64_t  price;             // decoded price in smallest currency sub-unit
    uint64_t quantity;          // decoded quantity
    uint32_t order_count;       // order count
    uint32_t _pad2{};           // pad to 8-byte multiple
};

static_assert(sizeof(SnapshotRow) == 48, "SnapshotRow size mismatch");

} // namespace ob
