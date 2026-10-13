#pragma once

#include <cstddef>

namespace ob {

/// A transparent huge page's size with 4 KiB base pages, and the size of every region below.
inline constexpr size_t kHugeRegionBytes = size_t{2} << 20;

/// What became of a region's request for huge pages.
enum class HugeAdvice { NotAsked, Asked, Refused };

/// "not asked for", "asked for" or "refused", for a log line.
const char* advice_name(HugeAdvice advice);

/// A region of kHugeRegionBytes, aligned to kHugeRegionBytes, which a huge page can back whole.
struct HugeRegion {
    char* base;
    HugeAdvice advice;
};

/// Map a region, private and anonymous, and with `advise` ask the kernel to back it with transparent
/// huge pages (`MADV_HUGEPAGE`). Where the kernel refuses - no THP in it - the region works the same
/// on 4 KiB pages, and a `WARN` under `component` says so. std::bad_alloc when mmap fails, after an
/// `ERROR` under `component`. Used by the books' arena (#51) and the held columns' (#223).
HugeRegion map_huge_region(bool advise, const char* component);

/// Give a region map_huge_region() returned back to the kernel.
void unmap_huge_region(char* base);

}  // namespace ob
