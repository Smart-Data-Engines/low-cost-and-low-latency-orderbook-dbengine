#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>

#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

namespace ob {

// SoASide: Structure-of-Arrays layout for one side (bid or ask) of an orderbook.
//
// Aligned to 64 bytes (cache-line boundary) to enable SIMD vectorization and
// maximize cache-line utilization (Requirements 2.1, 2.2, 2.3).
//
// Seqlock version counter protocol (Requirements 3.2, 3.5):
//   Writer: increment version to odd → write data → increment version to even.
//   Reader: read version (must be even) → read data → read version again → retry if changed.
struct alignas(64) SoASide {
    static constexpr size_t MAX_LEVELS = 1000;

    // Prices stored as 64-bit signed integers in smallest currency sub-unit (Requirement 1.6).
    // Bid side: descending order. Ask side: ascending order (Requirement 2.5).
    int64_t  prices[MAX_LEVELS];        // 8000 bytes

    // Quantities as 64-bit unsigned integers (Requirement 1.7).
    uint64_t quantities[MAX_LEVELS];    // 8000 bytes

    // Order counts as 32-bit unsigned integers (Requirement 1.8).
    uint32_t order_counts[MAX_LEVELS];  // 4000 bytes
    uint32_t _pad[MAX_LEVELS];          // 4000 bytes — pad to 64-byte multiple

    uint32_t depth;                     // active level count (≤ MAX_LEVELS, Requirement 1.2)
    uint32_t _pad2;                     // padding for alignment

    // Seqlock version counter: even = stable, odd = writing (Requirement 3.2).
    std::atomic<uint64_t> version;
};

// Verify SoASide is 64-byte aligned.
static_assert(alignof(SoASide) == 64, "SoASide must be 64-byte aligned");

// SoABuffer: one per (symbol, exchange) pair (Requirement 1.1).
// Holds bid and ask sides plus metadata.
struct SoABuffer {
    SoASide bid;                            // bid side (Requirement 2.4)
    SoASide ask;                            // ask side (Requirement 2.4)

    std::atomic<uint64_t> sequence_number;  // monotonically increasing (Requirement 1.4, 1.5)
    uint64_t last_timestamp_ns;             // nanosecond-precision Unix timestamp (Requirement 1.4)

    char symbol[32];                        // null-terminated symbol string (Requirement 1.1)
    char exchange[32];                      // null-terminated exchange string (Requirement 1.1)
};

// ── Free functions ────────────────────────────────────────────────────────────

/// Insert or update a level in a side, maintaining sort order.
/// @param side       The SoASide to modify (must already be under seqlock write).
/// @param price      Price of the level.
/// @param qty        Quantity; if 0 the level is removed.
/// @param cnt        Order count.
/// @param descending true for bid (descending), false for ask (ascending).
/// @return OB_OK, OB_ERR_FULL if depth would exceed MAX_LEVELS on a new insert.
ob_status_t insert_level(SoASide& side, int64_t price, uint64_t qty,
                         uint32_t cnt, bool descending);

/// Remove a level by price from a side.
/// @return OB_OK if found and removed, OB_ERR_NOT_FOUND otherwise.
ob_status_t remove_level(SoASide& side, int64_t price);

/// Apply a delta update to buf using the seqlock writer protocol.
/// Enforces bid-descending / ask-ascending sort order.
/// Sets gap_detected=true when update.sequence_number != buf.sequence_number + 1
/// (or buf.sequence_number == 0 for the very first update).
/// @return OB_OK on success, OB_ERR_FULL if a level insert would overflow.
ob_status_t apply_delta(SoABuffer& buf, const DeltaUpdate& update,
                        const Level* levels, bool& gap_detected);

/// Read a consistent snapshot of both sides using the seqlock reader protocol.
/// Spins until a stable (even-versioned) read is obtained for each side.
void read_snapshot(const SoABuffer& buf, SoASide& out_bid, SoASide& out_ask);

} // namespace ob
