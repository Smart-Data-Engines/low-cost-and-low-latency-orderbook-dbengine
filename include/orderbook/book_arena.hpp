#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

namespace ob {

/// Memory for the live books (#51).
///
/// A book is two sides of 24 KB, and an update to the top of one side touches four of its 4 KiB
/// pages - the prices, the quantities, the order counts and the seqlock's version. A node of
/// thousands of books touches more pages than a core's TLB maps, so an update to a book written a
/// while ago walks the page table for each of them.
///
/// So the books are carved out of regions of 2 MiB, aligned to 2 MiB, which the kernel is asked to
/// back with transparent huge pages (`MADV_HUGEPAGE`): forty-odd books to a region, and one TLB
/// entry for all of them. Asked, not told - where THP is `never` the advice does nothing, and the
/// arena is a heap of slots like any other. A freed slot is reused; a region goes back to the
/// kernel only with the arena.
class BookArena {
public:
    static constexpr size_t kRegionBytes = size_t{2} << 20;
    static constexpr size_t kSlotAlign = 64;   ///< a cache line, and SoASide's alignment

    /// `huge_pages` false gives the same arena without the advice.
    explicit BookArena(bool huge_pages);
    ~BookArena();
    BookArena(const BookArena&) = delete;
    BookArena& operator=(const BookArena&) = delete;

    /// A slot of `bytes`, aligned to `align` (at most kSlotAlign). The arena holds one kind of
    /// object, so every call after the first must ask for the same size: another one throws
    /// std::logic_error, and so does an alignment above kSlotAlign. std::bad_alloc when the kernel
    /// has no region to give.
    void* allocate(size_t bytes, size_t align);
    void deallocate(void* p, size_t bytes) noexcept;

    bool huge_pages() const { return huge_pages_; }
    size_t regions() const;
    size_t slots_in_use() const;

private:
    void add_region();   ///< called under mtx_

    const bool huge_pages_;
    mutable std::mutex mtx_;
    size_t slot_bytes_{0};
    std::vector<char*> regions_;
    std::vector<void*> free_;
    char* next_{nullptr};
    char* end_{nullptr};
    size_t in_use_{0};
};

/// The allocator std::allocate_shared takes. It holds the arena, so a book a reader still holds
/// keeps its memory after the engine has let go of both.
template <class T>
class BookArenaAllocator {
public:
    using value_type = T;

    explicit BookArenaAllocator(std::shared_ptr<BookArena> arena) : arena_(std::move(arena)) {}
    template <class U>
    BookArenaAllocator(const BookArenaAllocator<U>& other) : arena_(other.arena()) {}

    T* allocate(size_t n) { return static_cast<T*>(arena_->allocate(n * sizeof(T), alignof(T))); }
    void deallocate(T* p, size_t n) noexcept { arena_->deallocate(p, n * sizeof(T)); }

    const std::shared_ptr<BookArena>& arena() const { return arena_; }

    template <class U>
    bool operator==(const BookArenaAllocator<U>& other) const { return arena_ == other.arena(); }

private:
    std::shared_ptr<BookArena> arena_;
};

}  // namespace ob
