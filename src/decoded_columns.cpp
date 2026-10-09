#include "orderbook/decoded_columns.hpp"

#include "orderbook/logger.hpp"

#include <type_traits>
#include <utility>

namespace ob {

namespace {

constexpr const char* kSlotNames[DecodedColumns::kSlots] = {"ts",   "price", "qty", "cnt",
                                                             "side", "level", "seq"};

} // namespace

size_t ColumnValues::bytes() const {
    const size_t held = std::visit(
        [](const auto& v) {
            using T = typename std::decay_t<decltype(v)>::value_type;
            return v.capacity() * sizeof(T);
        },
        values);
    return sizeof(*this) + held;
}

// ── DecodedColumns ──────────────────────────────────────────────────────────────

DecodedColumns::DecodedColumns(std::shared_ptr<DecodedColumnsBudget> budget)
    : budget_(std::move(budget)) {}

DecodedColumns::~DecodedColumns() {
    // No mutex of the budget's: a segment can die inside `charge()`, when the hand's lock on it was
    // its last reference. Nothing else can reach a segment whose last reference is going.
    if (budget_ && bytes_ > 0) budget_->release(bytes_);
}

DecodedColumns::Column DecodedColumns::get(size_t slot) const {
    Column held;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        held = columns_[slot];
    }
    if (held) {
        referenced_.store(true, std::memory_order_relaxed);
        budget_->count_hit();
    } else {
        budget_->count_miss();
    }
    return held;
}

DecodedColumns::Column DecodedColumns::put(size_t slot, Column values, std::string_view segment) {
    if (!values) return values;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (columns_[slot]) return columns_[slot];   // another read held it first
    }
    const size_t bytes = values->bytes();
    // Room first, without this segment's mutex: making room locks the segments it evicts, after
    // the budget's mutex, and this order is the only one anything takes them in.
    if (!budget_->charge(bytes, this)) return values;   // this read's alone
    size_t held_now = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (columns_[slot]) {
            // Lost to a read that decoded the same column at the same time: its copy is held, and
            // this one is dropped with the read.
            budget_->release(bytes);
            return columns_[slot];
        }
        columns_[slot] = values;
        bytes_ += bytes;
        held_now = bytes_;
    }
    // Not marked as used: a segment read once - a scan over a month - is the first the hand evicts,
    // and one read again since it was held is passed over once. A one-off scan cannot push out what
    // queries keep coming back to.
    // Into the ring only now that it holds the column, so the hand never finds it between the
    // charge and the column - and if it is evicted between these two lines, the ring just holds
    // a segment of nothing until the hand passes.
    budget_->track(shared_from_this());
    OB_LOG_DEBUG("decoded", "segment %.*s: column %s held, %zu bytes (%zu for the segment, %zu of %zu "
                            "held)",
                 static_cast<int>(segment.size()), segment.data(), kSlotNames[slot], bytes, held_now,
                 budget_->held(), budget_->limit());
    return values;
}

size_t DecodedColumns::bytes() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return bytes_;
}

size_t DecodedColumns::evict() {
    std::array<Column, kSlots> dropped{};
    size_t freed = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        dropped.swap(columns_);
        freed = bytes_;
        bytes_ = 0;
    }
    // `dropped` goes here, outside this segment's mutex: a read that copied a column keeps it, and
    // one nobody holds is freed now.
    return freed;
}

// ── DecodedColumnsBudget ────────────────────────────────────────────────────────

DecodedColumnsBudget::DecodedColumnsBudget(size_t limit_bytes) : limit_(limit_bytes) {}

DecodedColumnsBudget::Stats DecodedColumnsBudget::stats() const {
    return Stats{hits_.load(std::memory_order_relaxed), misses_.load(std::memory_order_relaxed),
                 evictions_.load(std::memory_order_relaxed), held(), limit_};
}

bool DecodedColumnsBudget::charge(size_t bytes, const DecodedColumns* charging) {
    if (bytes > limit_) {
        OB_LOG_DEBUG("decoded", "a column of %zu bytes is larger than the budget of %zu: not held",
                     bytes, limit_);
        return false;
    }
    // Segments the hand locked and evicted die, if it was their last reference, once the mutex is
    // released - their destructors take no mutex, but nothing is gained by running them under it.
    std::vector<std::shared_ptr<DecodedColumns>> passed;
    size_t freed = 0;
    size_t evicted = 0;
    bool fits = false;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // Two turns at most: the first may only clear the bits of segments read since the hand last
        // passed, and the second evicts what was not read again meanwhile.
        const size_t max_steps = 2 * ring_.size();
        for (size_t step = 0; held() + bytes > limit_ && !ring_.empty() && step < max_steps; ++step) {
            if (hand_ >= ring_.size()) hand_ = 0;
            std::shared_ptr<DecodedColumns> seg = ring_[hand_].lock();
            if (!seg) {
                ring_[hand_] = std::move(ring_.back());   // dead: its bytes went with it
                ring_.pop_back();
                continue;
            }
            if (seg.get() == charging) {
                ++hand_;
                passed.push_back(std::move(seg));
                continue;
            }
            const bool read_again = seg->referenced_.exchange(false, std::memory_order_relaxed);
            if (read_again) {   // passed over once, its bit cleared
                ++hand_;
                passed.push_back(std::move(seg));
                continue;
            }
            const size_t got = seg->evict();
            held_.fetch_sub(got, std::memory_order_relaxed);
            freed += got;
            ++evicted;
            seg->in_ring_ = false;
            ring_[hand_] = std::move(ring_.back());
            ring_.pop_back();
            evictions_.fetch_add(1, std::memory_order_relaxed);
            passed.push_back(std::move(seg));
        }
        fits = held() + bytes <= limit_;
        if (fits) held_.fetch_add(bytes, std::memory_order_relaxed);
    }
    if (evicted > 0) {
        OB_LOG_DEBUG("decoded", "evicted the columns of %zu segment(s), %zu bytes, for %zu more: %zu "
                                "of %zu held",
                     evicted, freed, bytes, held(), limit_);
    }
    if (!fits) {
        // The rest is held by segments read again as the hand passed, or by the charging segment
        // itself: normal for a segment near the size of the budget, so not a warning.
        OB_LOG_DEBUG("decoded", "two turns of the ring freed %zu of the %zu bytes asked for (%zu of %zu "
                                "held): the column is not held",
                     freed, bytes, held(), limit_);
    }
    return fits;
}

void DecodedColumnsBudget::track(const std::shared_ptr<DecodedColumns>& segment) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (segment->in_ring_) return;
    segment->in_ring_ = true;
    ring_.push_back(segment);
}

} // namespace ob
