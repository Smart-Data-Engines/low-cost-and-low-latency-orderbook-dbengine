#include "orderbook/book_arena.hpp"

#include "orderbook/logger.hpp"

#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>

namespace ob {

BookArena::BookArena(bool huge_pages) : huge_pages_(huge_pages) {}

BookArena::~BookArena() {
    for (char* region : regions_) ::munmap(region, kRegionBytes);
}

size_t BookArena::regions() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return regions_.size();
}

size_t BookArena::slots_in_use() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return in_use_;
}

void BookArena::add_region() {
    // A page short of twice the size, which holds an aligned region wherever it starts, and the
    // unaligned ends given back: mmap aligns to a page, not to 2 MiB, and a region straddling two
    // huge pages could be backed by neither. Not twice exactly: a kernel since 6.12 aligns an
    // anonymous mapping whose length is a multiple of 2 MiB itself, which would leave the code
    // below unexercised on the machines it is tested on and needed on the ones it is not.
    const size_t span = 2 * kRegionBytes - static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    void* mapped = ::mmap(nullptr, span, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapped == MAP_FAILED) {
        OB_LOG_ERROR("book_arena", "mmap of %zu bytes for a region of books failed: %s", span,
                     std::strerror(errno));
        throw std::bad_alloc();
    }
    char* base = static_cast<char*>(mapped);
    const auto aligned = (reinterpret_cast<uintptr_t>(base) + kRegionBytes - 1) &
                         ~(uintptr_t{kRegionBytes} - 1);
    char* region = reinterpret_cast<char*>(aligned);
    char* tail = region + kRegionBytes;
    if (region > base) ::munmap(base, static_cast<size_t>(region - base));
    if (base + span > tail) ::munmap(tail, static_cast<size_t>(base + span - tail));

    const char* advice = "not asked for";
    if (huge_pages_) {
        if (::madvise(region, kRegionBytes, MADV_HUGEPAGE) == 0) {
            advice = "asked for";
        } else {
            // EINVAL without THP in the kernel. The region works the same, on 4 KiB pages.
            OB_LOG_WARN("book_arena", "madvise(MADV_HUGEPAGE) on the region at %p refused: %s; its "
                                      "books are on 4 KiB pages", static_cast<void*>(region),
                        std::strerror(errno));
            advice = "refused";
        }
    }
    try {
        regions_.push_back(region);
    } catch (...) {
        ::munmap(region, kRegionBytes);
        throw;
    }
    next_ = region;
    end_ = tail;
    OB_LOG_INFO("book_arena", "region %zu at %p: %zu books of %zu bytes, transparent huge pages %s",
                regions_.size(), static_cast<void*>(region), kRegionBytes / slot_bytes_, slot_bytes_,
                advice);
}

void* BookArena::allocate(size_t bytes, size_t align) {
    if (align > kSlotAlign) {
        throw std::logic_error("a book arena aligns its slots to " + std::to_string(kSlotAlign) +
                               " bytes, and " + std::to_string(align) + " were asked for");
    }
    const size_t slot = (bytes + kSlotAlign - 1) & ~(kSlotAlign - 1);
    std::lock_guard<std::mutex> lock(mtx_);
    if (slot_bytes_ == 0) {
        if (slot == 0 || slot > kRegionBytes) {
            throw std::logic_error("a book arena slot of " + std::to_string(bytes) + " bytes");
        }
        slot_bytes_ = slot;
    } else if (slot != slot_bytes_) {
        throw std::logic_error("a book arena holds slots of " + std::to_string(slot_bytes_) +
                               " bytes, and " + std::to_string(bytes) + " were asked for");
    }
    void* p;
    if (!free_.empty()) {
        p = free_.back();
        free_.pop_back();
    } else {
        if (next_ == nullptr || static_cast<size_t>(end_ - next_) < slot_bytes_) add_region();
        p = next_;
        next_ += slot_bytes_;
    }
    ++in_use_;
    OB_LOG_DEBUG("book_arena", "slot %p taken: %zu in use, %zu free, %zu regions", p, in_use_,
                 free_.size(), regions_.size());
    return p;
}

void BookArena::deallocate(void* p, size_t /*bytes*/) noexcept {
    std::lock_guard<std::mutex> lock(mtx_);
    --in_use_;
    try {
        free_.push_back(p);
    } catch (...) {
        // Out of memory for the list of free slots: this slot is lost until the arena goes, which
        // is better than ending a process for a book it no longer needs.
        OB_LOG_WARN("book_arena", "slot %p not reusable: no memory to remember it", p);
        return;
    }
    OB_LOG_DEBUG("book_arena", "slot %p given back: %zu in use, %zu free", p, in_use_, free_.size());
}

}  // namespace ob
