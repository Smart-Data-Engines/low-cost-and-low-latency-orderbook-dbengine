#pragma once

#include "orderbook/column_arena.hpp"

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

/// One column of a format-3 segment, decoded (#220), in the type the read path reads it as: in a
/// slot of the store's column arena (#223), or - a column too small for the arena's classes, or one
/// that found no room under its cap - in a vector of its own.
struct ColumnValues {
    /// The type, always; the values, when they are on the heap. A column in the arena keeps the
    /// alternative of its type here, empty, so that as<T>() checks the type the same way for both.
    std::variant<std::vector<uint64_t>,   // timestamp_ns, quantity
                 std::vector<int64_t>,    // price, sequence_number
                 std::vector<uint32_t>,   // order_count
                 std::vector<uint8_t>,    // side
                 std::vector<uint16_t>>   // level_index
        values;
    /// The slot the values are in, when they are the arena's, and how many there are.
    ArenaBlock block;
    size_t count{0};

    /// What holding it costs: the slot's class or the vector's allocation, and this object.
    size_t bytes() const;

    /// std::bad_variant_access for a type the column is not.
    template <typename T>
    std::span<const T> as() const {
        const std::vector<T>& v = std::get<std::vector<T>>(values);
        if (block) return {static_cast<const T*>(block.data()), count};
        return v;
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

    /// The arena a column of this segment is decoded into to be held (#223).
    ColumnArena* arena() const;

private:
    friend class DecodedColumnsBudget;

    /// Drop every held column and return what they cost. Called by the budget, with its mutex
    /// held: the order is always the budget's mutex, then a segment's, never the other way.
    size_t evict();

    mutable std::mutex mtx_;
    std::array<Column, kSlots> columns_{};
    size_t bytes_{0};
    /// CLOCK's bit: set by a hit - not by a put, so a segment read once goes first - and cleared by
    /// the hand passing over it.
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
    /// Its arena maps at most `limit_bytes` and a slack, arena_slack(), so that a read can take its
    /// slots before its put() evicts to make room, and keeps that slack free for the next read.
    explicit DecodedColumnsBudget(size_t limit_bytes);

    /// An eighth of the budget, and never under kArenaSlackFloor: the columns of one segment at its
    /// most rows, 262 144, are 9.75 MiB, and a miss past the first reads must find them free, or it
    /// decodes on the heap what the eviction its put() makes would have given it.
    static constexpr size_t kArenaSlackFloor = size_t{16} << 20;
    static size_t arena_slack(size_t limit_bytes);

    ColumnArena* arena() const { return arena_.get(); }

    struct Stats {
        uint64_t hits;        ///< columns a read took from memory
        uint64_t misses;      ///< columns a read had to read and decode
        uint64_t evictions;   ///< segments whose columns were evicted to make room
        size_t   held_bytes;
        size_t   limit_bytes;
        ColumnArena::Stats arena;   ///< the memory the columns are held in (#223)
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
    std::shared_ptr<ColumnArena> arena_;
};

} // namespace ob
