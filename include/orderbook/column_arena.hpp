#pragma once

#include "orderbook/huge_region.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace ob {

class ColumnArena;

/// One slot of a ColumnArena: what a held column is decoded into (#223). It gives the slot back
/// when it goes, and holds the arena until then.
class ArenaBlock {
public:
    ArenaBlock() = default;
    ArenaBlock(ArenaBlock&& other) noexcept;
    ArenaBlock& operator=(ArenaBlock&& other) noexcept;
    ArenaBlock(const ArenaBlock&) = delete;
    ArenaBlock& operator=(const ArenaBlock&) = delete;
    ~ArenaBlock();

    explicit operator bool() const { return data_ != nullptr; }
    void* data() const { return data_; }
    /// The slot's size: the class the column took, not the column's own bytes.
    size_t bytes() const { return bytes_; }

private:
    friend class ColumnArena;
    ArenaBlock(std::shared_ptr<ColumnArena> arena, void* data, size_t bytes);
    void release() noexcept;

    std::shared_ptr<ColumnArena> arena_;
    void* data_{nullptr};
    size_t bytes_{0};
};

/// The memory of the decoded columns a store holds between queries (#223).
///
/// A held column was a vector of its own: at 262 144 rows, 2 MiB of fresh pages - 512 page faults
/// on the read that decoded it, and as many again for the next column after an eviction, since
/// glibc gives a vector that large back to the kernel. Here a column takes a slot of its size class
/// - a power of two from 64 KiB to 2 MiB - in a region of 2 MiB, aligned, and for the classes from
/// kHugeClassBytes advised for transparent huge pages (map_huge_region()); the slot comes back to
/// the arena when the column goes. A node past its first reads decodes into memory it has.
///
/// A region is carved for one class; one wholly free is carved again for whichever class needs it.
/// The regions mapped stay within `cap_bytes`: where a column finds no slot under it, allocate()
/// returns an empty block and the column is decoded on the heap, as before. What is free beyond
/// `slack_bytes` goes back to the kernel. Made by std::make_shared: a block holds the arena.
class ColumnArena : public std::enable_shared_from_this<ColumnArena> {
public:
    static constexpr size_t kMinClassBytes = size_t{64} << 10;
    static constexpr size_t kMaxClassBytes = kHugeRegionBytes;
    static constexpr size_t kClasses = 6;   ///< 64 KiB, 128 KiB, ... 2 MiB
    /// The smallest class whose regions are advised for huge pages. A huge page is zeroed whole on
    /// its first touch: on the EPYC host 2 MiB took about 80 µs, where a column's own 4 KiB faults
    /// cost about 0.74 µs each, so a region first touched by a column under about 430 KiB paid more
    /// as one huge page than as the pages it used - which made a small recording's first query
    /// slower. Smaller classes take 4 KiB pages, and are given back and taken again the same.
    static constexpr size_t kHugeClassBytes = size_t{512} << 10;

    ColumnArena(size_t cap_bytes, size_t slack_bytes, bool huge_pages);
    ~ColumnArena();
    ColumnArena(const ColumnArena&) = delete;
    ColumnArena& operator=(const ColumnArena&) = delete;

    /// The class a column of `bytes` takes, or 0 for one that is not the arena's: smaller than
    /// kMinClassBytes, whose few pages cost little, or larger than kMaxClassBytes.
    static size_t class_bytes(size_t bytes);

    /// A slot for a column of `bytes`, or an empty block: `bytes` not the arena's, or no room under
    /// the cap.
    ArenaBlock allocate(size_t bytes);

    struct Stats {
        size_t   mapped_bytes;   ///< the regions mapped
        size_t   free_bytes;     ///< of them, in slots no column holds
        size_t   regions;
        uint64_t fallbacks;      ///< columns of the arena's sizes that found no room and went to the heap
    };
    Stats stats() const;

private:
    friend class ArenaBlock;

    struct Region {
        char*    base;
        size_t   class_index;   ///< the class it is carved for
        uint32_t slots;         ///< kHugeRegionBytes / the class's bytes, 1 to 32
        uint32_t free_mask;     ///< bit i set: slot i is free
        uint32_t full_mask() const { return slots == 32 ? ~uint32_t{0} : (uint32_t{1} << slots) - 1; }
    };

    void release(void* p, size_t class_bytes) noexcept;
    /// A region carved for class `k` with a free slot - a wholly free one carved again, or a new
    /// one - or null at the cap. Under mtx_.
    Region* carve(size_t k);
    /// Unmap wholly free regions while more than `slack_bytes_` is free. Under mtx_.
    void give_back_beyond_slack();
    /// Take `base` off the list of class `k`'s regions with a free slot. Under mtx_.
    void unlist(size_t k, char* base);

    const size_t cap_bytes_;
    const size_t slack_bytes_;
    const bool huge_pages_;
    mutable std::mutex mtx_;
    std::unordered_map<uintptr_t, Region> regions_;          ///< by base
    std::array<std::vector<char*>, kClasses> with_free_{};   ///< regions of each class with a free slot
    size_t mapped_bytes_{0};
    size_t free_bytes_{0};
    uint64_t fallbacks_{0};
};

}  // namespace ob
