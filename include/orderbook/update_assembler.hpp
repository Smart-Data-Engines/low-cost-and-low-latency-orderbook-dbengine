#pragma once

// ── A segment's rows read back as the writes that stored them (#196) ─────────
//
// A shard moving a symbol sends the target its rows as the updates that wrote them, so that the
// target stores the same rows and builds the same book: a write of n levels stored n rows, levels
// 0 to n-1, with its time, number and side, and a symbol's rows are appended in the order its writes
// came. So a row of level 0, or one of another time, number or side, begins the next update - the
// level is what tells two writes apart where neither the time nor a number does (rows written before
// numbers existed carry 0).

#include "orderbook/data_model.hpp"
#include "orderbook/symbol_mover.hpp"

#include <cstdint>
#include <utility>
#include <vector>

namespace ob {

/// One update as a migration sends it: the rows one write stored, in level order.
struct MovedUpdate {
    uint64_t                timestamp_ns{0};
    uint64_t                sequence_number{0};
    uint8_t                 side{0};
    std::vector<MovedLevel> levels;
};

class UpdateAssembler {
public:
    /// Take the next row. True when it began an update and so completed the one before it, which is
    /// then in `done`.
    bool add(const SnapshotRow& row, MovedUpdate& done) {
        const bool begins = !open_ || row.level_index == 0 || row.timestamp_ns != update_.timestamp_ns ||
                            row.sequence_number != update_.sequence_number || row.side != update_.side;
        bool completed = false;
        if (begins) {
            if (open_) {
                done      = std::move(update_);
                completed = true;
            }
            update_                 = MovedUpdate{};
            update_.timestamp_ns    = row.timestamp_ns;
            update_.sequence_number = row.sequence_number;
            update_.side            = row.side;
            open_                   = true;
        }
        update_.levels.push_back(MovedLevel{row.price, row.quantity, row.order_count});
        return completed;
    }

    /// The update still open after the last row, if there is one: true when `done` holds it.
    bool finish(MovedUpdate& done) {
        if (!open_) return false;
        done  = std::move(update_);
        open_ = false;
        return true;
    }

private:
    MovedUpdate update_;
    bool        open_{false};
};

}  // namespace ob
