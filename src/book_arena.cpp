#include "orderbook/book_arena.hpp"

#include "orderbook/huge_region.hpp"
#include "orderbook/logger.hpp"

#include <stdexcept>
#include <string>

namespace ob {

BookArena::BookArena(bool huge_pages) : huge_pages_(huge_pages) {}

BookArena::~BookArena() {
    for (char* region : regions_) unmap_huge_region(region);
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
    const HugeRegion r = map_huge_region(huge_pages_, "book_arena");
    try {
        regions_.push_back(r.base);
    } catch (...) {
        unmap_huge_region(r.base);
        throw;
    }
    next_ = r.base;
    end_ = r.base + kRegionBytes;
    OB_LOG_INFO("book_arena", "region %zu at %p: %zu books of %zu bytes, transparent huge pages %s",
                regions_.size(), static_cast<void*>(r.base), kRegionBytes / slot_bytes_, slot_bytes_,
                advice_name(r.advice));
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
