#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <variant>
#include <vector>

namespace ob {

/// One column of a format-3 segment, decoded (#220): the values `column_codec::decode_as()`
/// decodes a block into, in the type the read path reads them as.
struct ColumnValues {
    std::variant<std::vector<uint64_t>,   // timestamp_ns, quantity
                 std::vector<int64_t>,    // price, sequence_number
                 std::vector<uint32_t>,   // order_count
                 std::vector<uint8_t>,    // side
                 std::vector<uint16_t>>   // level_index
        values;

    /// What holding it costs: the vector's allocation and this object.
    size_t bytes() const;

    template <typename T>
    std::span<const T> as() const {
        return std::get<std::vector<T>>(values);
    }
};

class DecodedColumnsBudget;

/// One segment's decoded columns, held between the reads that decode them (#220).
///
/// Owned by the segment's entry in the index and by the reads that copied that entry, so it lives
/// exactly as long as the last of them: a segment a merge replaces, retention or a drop removes, or
/// a snapshot install wipes takes its columns with it once its readers finish, and a segment that
/// later takes the same path - a directory is named after its range - starts with an empty one.
/// Nothing is keyed by a path, so nothing can hand one segment another's rows.
class DecodedColumns : public std::enable_shared_from_this<DecodedColumns> {
public:
    /// A segment's columns, in format 3's order: ts, price, qty, cnt, side, level, seq.
    static constexpr size_t kSlots = 7;
    using Column = std::shared_ptr<const ColumnValues>;

    explicit DecodedColumns(std::shared_ptr<DecodedColumnsBudget> budget);
    ~DecodedColumns();
    DecodedColumns(const DecodedColumns&) = delete;
    DecodedColumns& operator=(const DecodedColumns&) = delete;

    /// The column held as `slot`, or null - counted as a hit or a miss. A hit marks the segment as
    /// used, for the budget's CLOCK.
    Column get(size_t slot) const;

    /// Hold `values` as column `slot` if the budget makes room for it and no other read held one
    /// first, and return what the read should use: the column held, or `values` itself when it is
    /// not held. `segment` names it in the log.
    Column put(size_t slot, Column values, std::string_view segment);

    /// What this segment's held columns cost.
    size_t bytes() const;

private:
    friend class DecodedColumnsBudget;

    /// Drop every held column and return what they cost. Called by the budget, with its mutex
    /// held: the order is always the budget's mutex, then a segment's, never the other way.
    size_t evict();

    mutable std::mutex mtx_;
    std::array<Column, kSlots> columns_{};
    size_t bytes_{0};
    /// CLOCK's bit: set by a hit and by a put, cleared by the hand passing over it.
    mutable std::atomic<bool> referenced_{false};
    /// Whether the budget's ring holds this segment. The budget's mutex guards it.
    bool in_ring_{false};
    std::shared_ptr<DecodedColumnsBudget> budget_;
};

/// What the decoded columns of a store may hold, and the CLOCK that keeps them within it (#220).
///
/// A segment that holds a column is in the ring; making room walks the hand around it, passing
/// over - and clearing the bit of - a segment read since the hand last passed, and evicting one
/// that was not. Eviction takes the columns from the segment, not from a read that copied them:
/// such a read keeps them until it finishes, so what is held can pass the limit by the columns of
/// the reads in progress, and never by more.
class DecodedColumnsBudget {
public:
    explicit DecodedColumnsBudget(size_t limit_bytes);

    struct Stats {
        uint64_t hits;        ///< columns a read took from memory
        uint64_t misses;      ///< columns a read had to read and decode
        uint64_t evictions;   ///< segments whose columns were evicted to make room
        size_t   held_bytes;
        size_t   limit_bytes;
    };
    Stats stats() const;
    size_t held() const { return held_.load(std::memory_order_relaxed); }
    size_t limit() const { return limit_; }

    void count_hit()  { hits_.fetch_add(1, std::memory_order_relaxed); }
    void count_miss() { misses_.fetch_add(1, std::memory_order_relaxed); }

private:
    friend class DecodedColumns;

    /// Room for `bytes` more, counted as held: evicts other segments' columns, CLOCK-wise, never
    /// `charging`'s. False, with nothing counted, when `bytes` alone passes the limit or two turns of
    /// the hand freed too little.
    bool charge(size_t bytes, const DecodedColumns* charging);
    /// Into the ring, if it is not there: a segment that holds a column now.
    void track(const std::shared_ptr<DecodedColumns>& segment);
    /// Bytes no longer held: a put that lost its race, or a segment's destructor. No mutex - a
    /// segment can die inside `charge()`, when the hand's lock on it was its last reference.
    void release(size_t bytes) { held_.fetch_sub(bytes, std::memory_order_relaxed); }

    mutable std::mutex mtx_;   ///< the ring, the hand and every segment's `in_ring_`
    std::vector<std::weak_ptr<DecodedColumns>> ring_;
    size_t hand_{0};
    std::atomic<size_t> held_{0};
    const size_t limit_;
    std::atomic<uint64_t> hits_{0}, misses_{0}, evictions_{0};
};

} // namespace ob
