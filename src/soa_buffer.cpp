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
//   bid (descending=true):  prices[0] > prices[1] > ... > prices[depth-1]
//   ask (descending=false): prices[0] < prices[1] < ... < prices[depth-1]
// No price twice: only this function writes a side, and it changes a level it holds rather than adding
// another.
//
// One search by halves (#268) finds the first slot whose price does not come before `price` in the
// side's order: the price's own slot when the side holds it, and where it goes when the side does not.
// It replaced two searches from the front, one for the price and one for where a new price goes, which
// on a book of the depth a busy symbol's reaches - a side of Binance's diff stream holds 269 levels at
// the median and the whole thousand at the 90th percentile - was most of what applying a level cost:
// 109 ns a level against 22 on the m8a.xlarge, 141 against 26 on the m9g.xlarge.
//
// If qty == 0 the level is treated as a removal.
ob_status_t insert_level(SoASide& side, int64_t price, uint64_t qty,
                         uint32_t cnt, bool descending)
{
    const uint32_t depth = side.depth;

    // The first slot in [0, depth) whose price does not come before `price`: each step halves the
    // range with a conditional move rather than a branch, since which half is taken cannot be predicted.
    uint32_t pos = 0;
    for (uint32_t len = depth; len > 0;) {
        const uint32_t half = len / 2;
        const int64_t at = side.prices[pos + half];
        const bool before = descending ? at > price : at < price;
        pos = before ? pos + half + 1 : pos;
        len = before ? len - half - 1 : half;
    }
    const bool held = pos < depth && side.prices[pos] == price;

    // ── Remove (qty == 0) ──────────────────────────────────────────────────
    if (qty == 0) {
        if (!held) {
            return OB_OK; // nothing to remove
        }
        // Shift elements left to fill the gap.
        const uint32_t tail = depth - pos - 1;
        if (tail > 0) {
            std::memmove(&side.prices[pos],
                         &side.prices[pos + 1],
                         tail * sizeof(int64_t));
            std::memmove(&side.quantities[pos],
                         &side.quantities[pos + 1],
                         tail * sizeof(uint64_t));
            std::memmove(&side.order_counts[pos],
                         &side.order_counts[pos + 1],
                         tail * sizeof(uint32_t));
        }
        side.depth = depth - 1;
        return OB_OK;
    }

    // ── Update existing level ──────────────────────────────────────────────
    if (held) {
        side.quantities[pos]   = qty;
        side.order_counts[pos] = cnt;
        // Price unchanged; sort order is preserved.
        return OB_OK;
    }

    // ── Insert new level ───────────────────────────────────────────────────
    uint32_t cur_depth = depth;
    if (depth >= SoASide::MAX_LEVELS) {
        // Buffer at capacity. A new price that goes after every level held is worse than the worst
        // (`pos == depth`): silently discard - not relevant for top of book. Any other evicts the worst
        // level, the last, which the shift below then writes over.
        if (pos >= depth) {
            return OB_OK;
        }
        cur_depth = depth - 1;
    }

    // Shift elements right to make room.
    const uint32_t tail = cur_depth - pos;
    if (tail > 0) {
        std::memmove(&side.prices[pos + 1],
                     &side.prices[pos],
                     tail * sizeof(int64_t));
        std::memmove(&side.quantities[pos + 1],
                     &side.quantities[pos],
                     tail * sizeof(uint64_t));
        std::memmove(&side.order_counts[pos + 1],
                     &side.order_counts[pos],
                     tail * sizeof(uint32_t));
    }

    side.prices[pos]       = price;
    side.quantities[pos]   = qty;
    side.order_counts[pos] = cnt;
    side.depth = cur_depth + 1;
    return OB_OK;
}

// ── remove_level ──────────────────────────────────────────────────────────────
// Takes `price` out of either side, wherever it is. It named the bids' order for every side when it called
// insert_level(), whose search from the front found a price either way; a search by halves would take the
// order at its word and miss an ask (#268), so it looks for the price itself.
ob_status_t remove_level(SoASide& side, int64_t price)
{
    const uint32_t depth = side.depth;
    for (uint32_t i = 0; i < depth; ++i) {
        if (side.prices[i] != price) continue;
        const uint32_t tail = depth - i - 1;
        if (tail > 0) {
            std::memmove(&side.prices[i], &side.prices[i + 1], tail * sizeof(int64_t));
            std::memmove(&side.quantities[i], &side.quantities[i + 1], tail * sizeof(uint64_t));
            std::memmove(&side.order_counts[i], &side.order_counts[i + 1], tail * sizeof(uint32_t));
        }
        side.depth = depth - 1;
        return OB_OK;
    }
    return OB_OK;
}

// ── apply_delta ───────────────────────────────────────────────────────────────
// Applies a DeltaUpdate to buf using the seqlock writer protocol:
//   1. Increment side.version to odd  (begin write)
//   2. Write all fields
//   3. Increment side.version to even (end write)
//
// Gap detection: if buf.sequence_number != 0 and
//   update.sequence_number != buf.sequence_number + 1, gap_detected is set.
//
// Engine no longer reads that flag. One counter per buffer cannot tell a gap from two
// origins interleaving, which in multi-master would report every interleave as a hole, so
// the engine decides gaps per origin in SequenceTracker. The flag stays for the C API,
// whose caller supplies its own sequence numbers and owns a single stream.
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

    // Update buffer metadata, inside this side's seqlock: `read_snapshot()` reads the pair under
    // both sides' versions, so a pair it accepts is one update's.
    buf.sequence_number.store(update.sequence_number, std::memory_order_relaxed);
    buf.last_timestamp_ns.store(update.timestamp_ns, std::memory_order_relaxed);

    // ── Seqlock: end write (version → even) ───────────────────────────────
    std::atomic_thread_fence(std::memory_order_release);
    side.version.store(ver + 2, std::memory_order_relaxed);

    return status;
}

// ── read_snapshot ─────────────────────────────────────────────────────────────
// Reads the label, then a consistent snapshot of both sides, using the seqlock reader protocol:
//   For the label, under both sides' versions; then for each side:
//     1. Spin until version is even (no write in progress).
//     2. Copy all data.
//     3. Re-read version; if changed, retry.
SnapshotLabel read_snapshot(const SoABuffer& buf, SoASide& out_bid, SoASide& out_ask)
{
    // The label first (#239). Its callers used to read it after the sides, and then it could name
    // an update applied after a side was copied: a book labelled newer than what it holds. A client
    // that resumes a subscription from that number drops the push of the update in between, which
    // the book does not hold. Read before the sides, it is a lower bound instead: the update it
    // names was complete when the label was read, so every update up to it is in each side's copy.
    //
    // Under both sides' versions, because an update writes the pair inside its own side's seqlock
    // and nothing says which side the last one was. Both even and both unchanged around the two
    // loads: no update wrote either in between, so the two are one update's. Every update moves
    // one version, so the pair cannot change with both versions standing still.
    SnapshotLabel label{};
    while (true) {
        const uint64_t bid1 = buf.bid.version.load(std::memory_order_acquire);
        const uint64_t ask1 = buf.ask.version.load(std::memory_order_acquire);
        if (((bid1 | ask1) & 1u) != 0) continue;   // an update under way on one side
        label.sequence_number = buf.sequence_number.load(std::memory_order_relaxed);
        label.timestamp_ns    = buf.last_timestamp_ns.load(std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_acquire);
        if (buf.bid.version.load(std::memory_order_relaxed) == bid1 &&
            buf.ask.version.load(std::memory_order_relaxed) == ask1) {
            break;
        }
    }

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
    return label;
}

bool install_book(SoABuffer& buf, const Level* bids, size_t n_bids, const Level* asks, size_t n_asks,
                  uint64_t sequence_number, uint64_t timestamp_ns) {
    const auto install = [&](SoASide& side, const Level* levels, size_t n, bool label, bool descending) {
        // ── Seqlock: begin write (version → odd) ──────────────────────────────
        const uint64_t ver = side.version.load(std::memory_order_relaxed);
        side.version.store(ver + 1, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);
        // The levels as given when they are in the side's order with no price twice - a book composed
        // by price always is - and otherwise one by one through insert_level(), which keeps that order:
        // insert_level() finds a price by halves (#268), so a side out of order would hide a price it
        // holds and take it a second time.
        const size_t depth = std::min(n, SoASide::MAX_LEVELS);
        bool ordered = true;
        for (size_t i = 1; i < depth && ordered; ++i) {
            ordered = descending ? levels[i - 1].price > levels[i].price : levels[i - 1].price < levels[i].price;
        }
        if (ordered) {
            for (size_t i = 0; i < depth; ++i) {
                side.prices[i]       = levels[i].price;
                side.quantities[i]   = levels[i].qty;
                side.order_counts[i] = levels[i].cnt;
            }
            side.depth = static_cast<uint32_t>(depth);
        } else {
            side.depth = 0;
            for (size_t i = 0; i < n; ++i) {
                insert_level(side, levels[i].price, levels[i].qty, levels[i].cnt, descending);
            }
        }
        if (label) {
            buf.sequence_number.store(sequence_number, std::memory_order_relaxed);
            buf.last_timestamp_ns.store(timestamp_ns, std::memory_order_relaxed);
        }
        // ── Seqlock: end write (version → even) ───────────────────────────────
        std::atomic_thread_fence(std::memory_order_release);
        side.version.store(ver + 2, std::memory_order_relaxed);
        return ordered;
    };
    const bool bids_ordered = install(buf.bid, bids, n_bids, /*label=*/true, /*descending=*/true);
    const bool asks_ordered = install(buf.ask, asks, n_asks, /*label=*/false, /*descending=*/false);
    // The engine logs each book it restores, and what this says; this library has no logger to say it with.
    return bids_ordered && asks_ordered;
}

} // namespace ob
