#pragma once

// ── A segment's rows read back as the writes that stored them (#196) ─────────
//
// A shard moving a symbol sends the target its rows as the updates that wrote them, so that the
// target stores the same rows and builds the same book: a write of n levels stored n rows, levels
// 0 to n-1, with its time, number and side, one after another - a symbol's rows are appended in the
// order its writes came, and a merge keeps that order. So a row of level 0 begins the next update,
// and nothing else does: two writes can share a time, and rows written before numbers existed share
// the number 0, but every write has a level 0. (A first version also began one at a change of time,
// number or side; a mutation that dropped the side survived, because no write begins anywhere else.)

#include "orderbook/data_model.hpp"
#include "orderbook/symbol_mover.hpp"

#include <cstdint>
#include <utility>
#include <vector>

namespace ob {

/// One update as a migration sends it: the rows one write stored, in level order.
struct MovedUpdate {
    uint64_t                timestamp_ns{0};
    uint8_t                 side{0};
    std::vector<MovedLevel> levels;
};

class UpdateAssembler {
public:
    /// Take the next row. True when it began an update and so completed the one before it, which is
    /// then in `done`.
    bool add(const SnapshotRow& row, MovedUpdate& done) {
        const bool begins = !open_ || row.level_index == 0;
        bool completed = false;
        if (begins) {
            if (open_) {
                done      = std::move(update_);
                completed = true;
            }
            update_              = MovedUpdate{};
            update_.timestamp_ns = row.timestamp_ns;
            update_.side         = row.side;
            open_                = true;
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
