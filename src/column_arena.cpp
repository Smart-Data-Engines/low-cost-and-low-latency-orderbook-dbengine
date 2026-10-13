#include "orderbook/column_arena.hpp"

#include "orderbook/logger.hpp"

#include <algorithm>
#include <new>

namespace ob {

// ── ArenaBlock ────────────────────────────────────────────────────────────────

ArenaBlock::ArenaBlock(std::shared_ptr<ColumnArena> arena, void* data, size_t bytes)
    : arena_(std::move(arena)), data_(data), bytes_(bytes) {}

ArenaBlock::ArenaBlock(ArenaBlock&& other) noexcept
    : arena_(std::move(other.arena_)), data_(other.data_), bytes_(other.bytes_) {
    other.data_ = nullptr;
    other.bytes_ = 0;
}

ArenaBlock& ArenaBlock::operator=(ArenaBlock&& other) noexcept {
    if (this != &other) {
        release();
        arena_ = std::move(other.arena_);
        data_ = other.data_;
        bytes_ = other.bytes_;
        other.data_ = nullptr;
        other.bytes_ = 0;
    }
    return *this;
}

ArenaBlock::~ArenaBlock() { release(); }

void ArenaBlock::release() noexcept {
    if (data_ != nullptr) arena_->release(data_, bytes_);
    data_ = nullptr;
    bytes_ = 0;
    arena_.reset();
}

// ── ColumnArena ───────────────────────────────────────────────────────────────

ColumnArena::ColumnArena(size_t cap_bytes, size_t slack_bytes, bool huge_pages)
    : cap_bytes_(cap_bytes), slack_bytes_(slack_bytes), huge_pages_(huge_pages) {}

ColumnArena::~ColumnArena() {
    // Every block holds the arena, so none is left: what remains is free.
    for (const auto& [base, region] : regions_) unmap_huge_region(region.base);
}

size_t ColumnArena::class_bytes(size_t bytes) {
    if (bytes < kMinClassBytes || bytes > kMaxClassBytes) return 0;
    size_t c = kMinClassBytes;
    while (c < bytes) c <<= 1;
    return c;
}

ColumnArena::Stats ColumnArena::stats() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return Stats{mapped_bytes_, free_bytes_, regions_.size(), fallbacks_};
}

void ColumnArena::unlist(size_t k, char* base) {
    auto& list = with_free_[k];
    const auto it = std::find(list.begin(), list.end(), base);
    if (it != list.end()) list.erase(it);
}

ColumnArena::Region* ColumnArena::carve(size_t k) {
    const size_t cls = kMinClassBytes << k;
    const auto slots = static_cast<uint32_t>(kHugeRegionBytes / cls);
    // A wholly free region of another class first: it costs no new memory. Its free bytes stay
    // free - every class divides 2 MiB - only the slots they are cut into change.
    for (auto& [key, r] : regions_) {
        if (r.free_mask != r.full_mask()) continue;
        unlist(r.class_index, r.base);
        const size_t was = kMinClassBytes << r.class_index;
        r.class_index = k;
        r.slots = slots;
        r.free_mask = r.full_mask();
        with_free_[k].push_back(r.base);
        OB_LOG_DEBUG("column_arena", "region at %p carved again: %u slots of %zu bytes, where it had "
                                     "%zu of %zu", static_cast<void*>(r.base), slots, cls,
                     kHugeRegionBytes / was, was);
        return &r;
    }
    if (mapped_bytes_ + kHugeRegionBytes > cap_bytes_) return nullptr;
    HugeRegion mapped{};
    try {
        mapped = map_huge_region(huge_pages_ && cls >= kHugeClassBytes, "column_arena");
    } catch (const std::bad_alloc&) {
        return nullptr;   // logged where the mmap failed; the column goes to the heap
    }
    Region r{mapped.base, k, slots, 0};
    r.free_mask = r.full_mask();
    try {
        with_free_[k].push_back(r.base);
        auto [it, inserted] = regions_.emplace(reinterpret_cast<uintptr_t>(r.base), r);
        (void)inserted;
        mapped_bytes_ += kHugeRegionBytes;
        free_bytes_ += kHugeRegionBytes;
        OB_LOG_INFO("column_arena", "region %zu at %p: %u slots of %zu bytes, transparent huge pages "
                                    "%s; %zu of a cap of %zu mapped",
                    regions_.size(), static_cast<void*>(r.base), slots, cls,
                    advice_name(mapped.advice), mapped_bytes_, cap_bytes_);
        return &it->second;
    } catch (...) {
        unlist(k, r.base);
        unmap_huge_region(r.base);
        return nullptr;
    }
}

ArenaBlock ColumnArena::allocate(size_t bytes) {
    const size_t cls = class_bytes(bytes);
    if (cls == 0) return {};
    // Before a slot is taken: a block holds the arena, and without one to hold the slot is lost.
    std::shared_ptr<ColumnArena> self = shared_from_this();
    size_t k = 0;
    while ((kMinClassBytes << k) < cls) ++k;
    void* p = nullptr;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        Region* r = nullptr;
        if (!with_free_[k].empty()) {
            r = &regions_.at(reinterpret_cast<uintptr_t>(with_free_[k].back()));
        } else {
            r = carve(k);
        }
        if (r == nullptr) {
            ++fallbacks_;
            OB_LOG_DEBUG("column_arena", "a column of %zu bytes goes to the heap: no slot of %zu bytes "
                                         "under the cap of %zu (%zu mapped, %zu free)",
                         bytes, cls, cap_bytes_, mapped_bytes_, free_bytes_);
            return {};
        }
        const auto i = static_cast<unsigned>(__builtin_ctz(r->free_mask));
        r->free_mask &= ~(uint32_t{1} << i);
        if (r->free_mask == 0) unlist(k, r->base);
        free_bytes_ -= cls;
        p = r->base + i * cls;
        OB_LOG_DEBUG("column_arena", "slot %p of %zu bytes taken for %zu: %zu of %zu mapped free", p, cls,
                     bytes, free_bytes_, mapped_bytes_);
    }
    return ArenaBlock(std::move(self), p, cls);
}

void ColumnArena::release(void* p, size_t cls) noexcept {
    std::lock_guard<std::mutex> lock(mtx_);
    const auto base = reinterpret_cast<uintptr_t>(p) & ~(uintptr_t{kHugeRegionBytes} - 1);
    const auto it = regions_.find(base);
    if (it == regions_.end()) {
        OB_LOG_ERROR("column_arena", "slot %p of %zu bytes is in no region of this arena", p, cls);
        return;
    }
    Region& r = it->second;
    const auto i = static_cast<unsigned>((reinterpret_cast<uintptr_t>(p) - base) / cls);
    const bool was_full = r.free_mask == 0;
    r.free_mask |= uint32_t{1} << i;
    free_bytes_ += cls;
    if (was_full) {
        try {
            with_free_[r.class_index].push_back(r.base);
        } catch (...) {
            // No memory to list it: its free slots wait until it is wholly free and carved again.
            OB_LOG_WARN("column_arena", "region at %p not listed with a free slot: no memory for it",
                        static_cast<void*>(r.base));
        }
    }
    OB_LOG_DEBUG("column_arena", "slot %p of %zu bytes given back: %zu of %zu mapped free", p, cls,
                 free_bytes_, mapped_bytes_);
    give_back_beyond_slack();
}

void ColumnArena::give_back_beyond_slack() {
    while (free_bytes_ > slack_bytes_) {
        auto it = std::find_if(regions_.begin(), regions_.end(),
                               [](const auto& kv) { return kv.second.free_mask == kv.second.full_mask(); });
        if (it == regions_.end()) return;
        Region r = it->second;
        unlist(r.class_index, r.base);
        regions_.erase(it);
        unmap_huge_region(r.base);
        mapped_bytes_ -= kHugeRegionBytes;
        free_bytes_ -= kHugeRegionBytes;
        OB_LOG_INFO("column_arena", "region at %p given back to the kernel: %zu free was beyond the "
                                    "slack of %zu; %zu mapped", static_cast<void*>(r.base),
                    free_bytes_ + kHugeRegionBytes, slack_bytes_, mapped_bytes_);
    }
}

}  // namespace ob
