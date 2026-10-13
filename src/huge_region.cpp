#include "orderbook/huge_region.hpp"

#include "orderbook/logger.hpp"

#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <new>

namespace ob {

const char* advice_name(HugeAdvice advice) {
    switch (advice) {
    case HugeAdvice::NotAsked: return "not asked for";
    case HugeAdvice::Asked:    return "asked for";
    case HugeAdvice::Refused:  return "refused";
    }
    return "unknown";
}

HugeRegion map_huge_region(bool advise, const char* component) {
    // A page short of twice the size, which holds an aligned region wherever it starts, and the
    // unaligned ends given back: mmap aligns to a page, not to 2 MiB, and a region straddling two
    // huge pages could be backed by neither. Not twice exactly: a kernel since 6.12 aligns an
    // anonymous mapping whose length is a multiple of 2 MiB itself, which would leave the code
    // below unexercised on the machines it is tested on and needed on the ones it is not.
    const size_t span = 2 * kHugeRegionBytes - static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    void* mapped = ::mmap(nullptr, span, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapped == MAP_FAILED) {
        OB_LOG_ERROR(component, "mmap of %zu bytes for a region of 2 MiB failed: %s", span,
                     std::strerror(errno));
        throw std::bad_alloc();
    }
    char* base = static_cast<char*>(mapped);
    const auto aligned = (reinterpret_cast<uintptr_t>(base) + kHugeRegionBytes - 1) &
                         ~(uintptr_t{kHugeRegionBytes} - 1);
    char* region = reinterpret_cast<char*>(aligned);
    char* tail = region + kHugeRegionBytes;
    if (region > base) ::munmap(base, static_cast<size_t>(region - base));
    if (base + span > tail) ::munmap(tail, static_cast<size_t>(base + span - tail));

    HugeAdvice advice = HugeAdvice::NotAsked;
    if (advise) {
        if (::madvise(region, kHugeRegionBytes, MADV_HUGEPAGE) == 0) {
            advice = HugeAdvice::Asked;
        } else {
            // EINVAL without THP in the kernel. The region works the same, on 4 KiB pages.
            OB_LOG_WARN(component, "madvise(MADV_HUGEPAGE) on the region at %p refused: %s; it is "
                                   "on 4 KiB pages", static_cast<void*>(region), std::strerror(errno));
            advice = HugeAdvice::Refused;
        }
    }
    return HugeRegion{region, advice};
}

void unmap_huge_region(char* base) {
    ::munmap(base, kHugeRegionBytes);
}

}  // namespace ob
