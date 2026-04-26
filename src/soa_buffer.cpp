#include "orderbook/soa_buffer.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>

namespace ob {

// ── insert_level ──────────────────────────────────────────────────────────────
// Inserts, updates, or removes a level in a side while maintaining sort order.
// Called from within the seqlock write section (version already odd).
//
// Sort invariant:
//   bid (descending=true):  prices[0] >= prices[1] >= ... >= prices[depth-1]
//   ask (descending=false): prices[0] <= prices[1] <= ... <= prices[depth-1]
//
// If qty == 0 the level is treated as a removal.
ob_status_t insert_level(SoASide& side, int64_t price, uint64_t qty,
                         uint32_t cnt, bool descending)
{
    const uint32_t depth = side.depth;

    // Search for an existing level at this price.
    uint32_t found_idx = depth; // sentinel: not found
    for (uint32_t i = 0; i < depth; ++i) {
        if (side.prices[i] == price) {
            found_idx = i;
            break;
        }
    }

    // ── Remove (qty == 0) ──────────────────────────────────────────────────
    if (qty == 0) {
        if (found_idx == depth) {
            return OB_OK; // nothing to remove
        }
        // Shift elements left to fill the gap.
        const uint32_t tail = depth - found_idx - 1;
        if (tail > 0) {
            std::memmove(&side.prices[found_idx],
                         &side.prices[found_idx + 1],
                         tail * sizeof(int64_t));
            std::memmove(&side.quantities[found_idx],
                         &side.quantities[found_idx + 1],
                         tail * sizeof(uint64_t));
            std::memmove(&side.order_counts[found_idx],
                         &side.order_counts[found_idx + 1],
                         tail * sizeof(uint32_t));
        }
        side.depth = depth - 1;
        return OB_OK;
    }

    // ── Update existing level ──────────────────────────────────────────────
    if (found_idx < depth) {
        side.quantities[found_idx]   = qty;
        side.order_counts[found_idx] = cnt;
        // Price unchanged; sort order is preserved.
        return OB_OK;
    }

    // ── Insert new level ───────────────────────────────────────────────────
    if (depth >= SoASide::MAX_LEVELS) {
        // Buffer at capacity — evict worst-priced level or discard if new price
        // is worse than all existing levels.
        //   Bid (descending): worst = prices[depth-1] (lowest price)
        //   Ask (ascending):  worst = prices[depth-1] (highest price)
        const int64_t worst = side.prices[depth - 1];
        const bool new_is_worse = descending
            ? (price <= worst)   // new bid price ≤ lowest existing bid
            : (price >= worst);  // new ask price ≥ highest existing ask
        if (new_is_worse) {
            return OB_OK; // silently discard — not relevant for top of book
        }
        // Evict the worst level (last element) to make room.
        side.depth = depth - 1;
        // Fall through to the normal sorted-insert path below.
    }

    // Find insertion position to maintain sort order.
    // For descending (bid): insert before the first element that is < price.
    // For ascending  (ask): insert before the first element that is > price.
    const uint32_t cur_depth = side.depth; // re-read after possible eviction
    uint32_t insert_pos = cur_depth;
    for (uint32_t i = 0; i < cur_depth; ++i) {
        bool should_insert_before = descending
            ? (price > side.prices[i])
            : (price < side.prices[i]);
        if (should_insert_before) {
            insert_pos = i;
            break;
        }
    }

    // Shift elements right to make room.
    const uint32_t tail = cur_depth - insert_pos;
    if (tail > 0) {
        std::memmove(&side.prices[insert_pos + 1],
                     &side.prices[insert_pos],
                     tail * sizeof(int64_t));
        std::memmove(&side.quantities[insert_pos + 1],
                     &side.quantities[insert_pos],
                     tail * sizeof(uint64_t));
        std::memmove(&side.order_counts[insert_pos + 1],
                     &side.order_counts[insert_pos],
                     tail * sizeof(uint32_t));
    }

    side.prices[insert_pos]       = price;
    side.quantities[insert_pos]   = qty;
    side.order_counts[insert_pos] = cnt;
    side.depth = cur_depth + 1;
    return OB_OK;
}

// ── remove_level ──────────────────────────────────────────────────────────────
ob_status_t remove_level(SoASide& side, int64_t price)
{
    return insert_level(side, price, /*qty=*/0, /*cnt=*/0, /*descending=*/true);
}

// ── apply_delta ───────────────────────────────────────────────────────────────
// Applies a DeltaUpdate to buf using the seqlock writer protocol:
//   1. Increment side.version to odd  (begin write)
//   2. Write all fields
//   3. Increment side.version to even (end write)
//
// Gap detection: if buf.sequence_number != 0 and
//   update.sequence_number != buf.sequence_number + 1, gap_detected is set.
ob_status_t apply_delta(SoABuffer& buf, const DeltaUpdate& update,
                        const Level* levels, bool& gap_detected)
{
    gap_detected = false;

    // ── Gap detection ──────────────────────────────────────────────────────
    const uint64_t prev_seq = buf.sequence_number.load(std::memory_order_relaxed);
    if (prev_seq != 0 && update.sequence_number != prev_seq + 1) {
        gap_detected = true;
    }

    // ── Select the side to update ──────────────────────────────────────────
    SoASide& side = (update.side == SIDE_BID) ? buf.bid : buf.ask;
    const bool descending = (update.side == SIDE_BID);

    // ── Seqlock: begin write (version → odd) ──────────────────────────────
    uint64_t ver = side.version.load(std::memory_order_relaxed);
    side.version.store(ver + 1, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);

    // ── Apply all level changes ────────────────────────────────────────────
    ob_status_t status = OB_OK;
    for (uint16_t i = 0; i < update.n_levels; ++i) {
        ob_status_t s = insert_level(side, levels[i].price, levels[i].qty,
                                     levels[i].cnt, descending);
        if (s != OB_OK && status == OB_OK) {
            status = s; // record first error but continue applying remaining levels
        }
    }

    // Update buffer metadata.
    buf.sequence_number.store(update.sequence_number, std::memory_order_relaxed);
    buf.last_timestamp_ns = update.timestamp_ns;

    // ── Seqlock: end write (version → even) ───────────────────────────────
    std::atomic_thread_fence(std::memory_order_release);
    side.version.store(ver + 2, std::memory_order_relaxed);

    return status;
}

// ── read_snapshot ─────────────────────────────────────────────────────────────
// Reads a consistent snapshot of both sides using the seqlock reader protocol:
//   For each side:
//     1. Spin until version is even (no write in progress).
//     2. Copy all data.
//     3. Re-read version; if changed, retry.
void read_snapshot(const SoABuffer& buf, SoASide& out_bid, SoASide& out_ask)
{
    // Helper lambda for one side.
    auto read_side = [](const SoASide& src, SoASide& dst) {
        while (true) {
            // Step 1: wait for even version (stable).
            uint64_t v1;
            do {
                v1 = src.version.load(std::memory_order_acquire);
            } while (v1 & 1u);

            // Step 2: copy data.
            std::atomic_thread_fence(std::memory_order_acquire);
            const uint32_t depth = src.depth;
            dst.depth = depth;
            std::memcpy(dst.prices,       src.prices,       depth * sizeof(int64_t));
            std::memcpy(dst.quantities,   src.quantities,   depth * sizeof(uint64_t));
            std::memcpy(dst.order_counts, src.order_counts, depth * sizeof(uint32_t));

            // Step 3: re-read version; retry if changed.
            std::atomic_thread_fence(std::memory_order_acquire);
            const uint64_t v2 = src.version.load(std::memory_order_acquire);
            if (v1 == v2) {
                break; // consistent read
            }
        }
    };

    read_side(buf.bid, out_bid);
    read_side(buf.ask, out_ask);
}

} // namespace ob
