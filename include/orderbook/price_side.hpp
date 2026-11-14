#pragma once
// One side of a book composed by price (#266): every price the side holds with the row that set it
// last, the best price at hand after any change, and the rows in price order at the end.
//
// A composition applies every row since the summary it starts from - 65 536 at the engine's default -
// and a std::map with a node a price, a copy of the row in it and a pointer chased a level was half of
// what composing a summary cost (book_summary_cost, #266). Here a price finds its slot in a table with
// open addressing, its row is kept in a vector the table points into, and the best price waits at the
// top of a heap: a removal leaves its price in the heap, and best() takes out the ones it finds gone.

#include "orderbook/data_model.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <vector>

namespace ob {

/// `HighestFirst`: a side whose best price is its highest - the bids; the asks' best is their lowest.
template <bool HighestFirst>
class PriceSide {
public:
    /// The row that sets `r.price` now: the price added, or its row replaced.
    void put(const SnapshotRow& r) {
        if ((used_ + 1) * 4 > table_.size() * 3) rehash();
        size_t i = home(r.price);
        size_t reuse = kNone;
        for (;; i = (i + 1) & mask_) {
            Slot& s = table_[i];
            if (s.state == kEmpty) break;
            if (s.state == kRemoved) {
                if (reuse == kNone) reuse = i;
                continue;
            }
            if (s.price == r.price) {
                rows_[s.row] = r;
                return;
            }
        }
        uint32_t row;
        if (!free_.empty()) {
            row = free_.back();
            free_.pop_back();
            rows_[row] = r;
        } else {
            row = static_cast<uint32_t>(rows_.size());
            rows_.push_back(r);
        }
        if (reuse == kNone) {
            reuse = i;
            ++used_;
        }
        table_[reuse] = Slot{r.price, row, kFull};
        ++live_;
        // The best known stays known: a new price better than it is the best now.
        if (best_known_ && Worse{}(best_, r.price)) best_ = r.price;
        heap_.push_back(r.price);
        std::push_heap(heap_.begin(), heap_.end(), Worse{});
        // A heap of prices most of which are gone is rebuilt from the ones held.
        if (heap_.size() > 2 * live_ + 64) rebuild_heap();
    }

    /// The price no longer held; nothing when it was not.
    void remove(int64_t price) {
        if (table_.empty()) return;
        for (size_t i = home(price);; i = (i + 1) & mask_) {
            Slot& s = table_[i];
            if (s.state == kEmpty) return;
            if (s.state == kFull && s.price == price) {
                s.state = kRemoved;
                free_.push_back(s.row);
                --live_;
                if (best_known_ && price == best_) best_known_ = false;
                return;
            }
        }
    }

    bool empty() const { return live_ == 0; }
    size_t size() const { return live_; }

    /// The best price held. The side is not empty. Known without a look at the table until the best is
    /// removed: a composition asks after every instant, and most instants change a price below it.
    int64_t best() {
        if (best_known_) return best_;
        while (find(heap_.front()) == nullptr) {
            std::pop_heap(heap_.begin(), heap_.end(), Worse{});
            heap_.pop_back();
        }
        best_ = heap_.front();
        best_known_ = true;
        return best_;
    }

    /// Every row held, best price first.
    std::vector<SnapshotRow> best_first() const {
        std::vector<SnapshotRow> out;
        out.reserve(live_);
        for (const Slot& s : table_) {
            if (s.state == kFull) out.push_back(rows_[s.row]);
        }
        std::sort(out.begin(), out.end(), [](const SnapshotRow& a, const SnapshotRow& b) {
            return HighestFirst ? a.price > b.price : a.price < b.price;
        });
        return out;
    }

    void clear() {
        table_.clear();
        rows_.clear();
        free_.clear();
        heap_.clear();
        live_ = used_ = 0;
        mask_ = 0;
        shift_ = 64;
        best_known_ = false;
    }

private:
    static constexpr uint8_t kEmpty   = 0;
    static constexpr uint8_t kFull    = 1;
    static constexpr uint8_t kRemoved = 2;
    static constexpr size_t  kNone    = SIZE_MAX;

    struct Slot {
        int64_t  price{0};
        uint32_t row{0};
        uint8_t  state{kEmpty};
    };
    /// The heap's order: its front is the best price.
    using Worse = std::conditional_t<HighestFirst, std::less<int64_t>, std::greater<int64_t>>;

    /// Fibonacci hashing: prices a tick apart, or a hundred, spread over the table rather than
    /// landing on one residue of its size.
    size_t home(int64_t price) const {
        return static_cast<size_t>((static_cast<uint64_t>(price) * 0x9E3779B97F4A7C15ULL) >> shift_);
    }

    const SnapshotRow* find(int64_t price) const {
        if (table_.empty()) return nullptr;
        for (size_t i = home(price);; i = (i + 1) & mask_) {
            const Slot& s = table_[i];
            if (s.state == kEmpty) return nullptr;
            if (s.state == kFull && s.price == price) return &rows_[s.row];
        }
    }

    /// A table of twice the prices held, at least 16 slots, without the removed ones.
    void rehash() {
        const size_t capacity = std::bit_ceil(std::max<size_t>(16, (live_ + 1) * 2));
        std::vector<Slot> old;
        old.swap(table_);
        table_.assign(capacity, Slot{});
        mask_  = capacity - 1;
        shift_ = 64 - static_cast<unsigned>(std::countr_zero(capacity));
        used_  = 0;
        for (const Slot& s : old) {
            if (s.state != kFull) continue;
            size_t i = home(s.price);
            while (table_[i].state != kEmpty) i = (i + 1) & mask_;
            table_[i] = s;
            ++used_;
        }
    }

    void rebuild_heap() {
        heap_.clear();
        for (const Slot& s : table_) {
            if (s.state == kFull) heap_.push_back(s.price);
        }
        std::make_heap(heap_.begin(), heap_.end(), Worse{});
    }

    std::vector<Slot>        table_;
    std::vector<SnapshotRow> rows_;
    std::vector<uint32_t>    free_;    ///< slots of `rows_` a removal left
    std::vector<int64_t>     heap_;    ///< prices, the best at the front; removed ones until reached
    size_t                   live_{0};
    size_t                   used_{0};   ///< table slots not empty: held and removed
    size_t                   mask_{0};
    unsigned                 shift_{64};
    int64_t                  best_{0};
    bool                     best_known_{false};   ///< `best_` is the best held
};

}  // namespace ob
