// The books' memory (#51): slots carved out of 2 MiB regions, advised for transparent huge pages.
//
// Whether the kernel then backs a region with a huge page depends on the machine - its THP mode,
// its free memory - so these tests do not ask that. They ask what the arena controls: where the
// slots are, that they come back, and that a region carries the advice, which /proc/self/smaps
// shows as `hg` among a mapping's VmFlags whether or not a huge page was found for it.

#include "orderbook/book_arena.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr size_t kSlot = 48'000;   // about a book's, and a multiple of 64

/// The VmFlags of the mapping holding `p`, from /proc/self/smaps; "" when no mapping holds it.
std::string vm_flags_of(const void* p) {
    const auto at = reinterpret_cast<uintptr_t>(p);
    std::ifstream in("/proc/self/smaps");
    std::string line;
    bool inside = false;
    while (std::getline(in, line)) {
        unsigned long lo = 0, hi = 0;
        if (std::sscanf(line.c_str(), "%lx-%lx ", &lo, &hi) == 2) {
            inside = at >= lo && at < hi;
        } else if (inside && line.rfind("VmFlags:", 0) == 0) {
            return line + " ";
        }
    }
    return "";
}

bool kernel_has_thp() {
    return std::filesystem::exists("/sys/kernel/mm/transparent_hugepage/enabled");
}

} // namespace

TEST(BookArena, ARegionIsTwoMiBAlignedAndItsSlotsFollowEachOther) {
    ob::BookArena arena(/*huge_pages=*/true);
    void* a = arena.allocate(kSlot, 64);
    void* b = arena.allocate(kSlot, 64);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(a) % ob::BookArena::kRegionBytes, 0u)
        << "the first slot is the region's start, and a region is aligned to its size";
    EXPECT_EQ(static_cast<char*>(b) - static_cast<char*>(a), static_cast<ptrdiff_t>(kSlot));
    EXPECT_EQ(arena.regions(), 1u);
    EXPECT_EQ(arena.slots_in_use(), 2u);
}

TEST(BookArena, ASlotIsRoundedToACacheLine) {
    ob::BookArena arena(true);
    char* a = static_cast<char*>(arena.allocate(kSlot + 1, 8));
    char* b = static_cast<char*>(arena.allocate(kSlot + 1, 8));
    EXPECT_EQ(b - a, static_cast<ptrdiff_t>(kSlot + 64));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(b) % 64, 0u);
}

TEST(BookArena, ARegionHoldsTheSlotsThatFitAndTheNextOpensAnother) {
    ob::BookArena arena(true);
    const size_t fit = ob::BookArena::kRegionBytes / kSlot;
    std::vector<char*> slots;
    for (size_t i = 0; i < fit; ++i) slots.push_back(static_cast<char*>(arena.allocate(kSlot, 64)));
    EXPECT_EQ(arena.regions(), 1u);
    EXPECT_LE(slots.back() + kSlot, slots.front() + ob::BookArena::kRegionBytes) << "no slot crosses its region";
    char* next = static_cast<char*>(arena.allocate(kSlot, 64));
    EXPECT_EQ(arena.regions(), 2u);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(next) % ob::BookArena::kRegionBytes, 0u);
}

TEST(BookArena, AFreedSlotIsTheNextOneGiven) {
    ob::BookArena arena(true);
    void* a = arena.allocate(kSlot, 64);
    void* b = arena.allocate(kSlot, 64);
    arena.deallocate(a, kSlot);
    EXPECT_EQ(arena.slots_in_use(), 1u);
    EXPECT_EQ(arena.allocate(kSlot, 64), a);
    EXPECT_NE(arena.allocate(kSlot, 64), b) << "the control: a slot in use is not given twice";
    EXPECT_EQ(arena.regions(), 1u);
}

TEST(BookArena, ItHoldsOneSizeAndAlignsToACacheLineAtMost) {
    ob::BookArena arena(true);
    arena.allocate(kSlot, 64);
    EXPECT_THROW(arena.allocate(kSlot + 64, 64), std::logic_error);
    EXPECT_THROW(arena.allocate(kSlot, 128), std::logic_error);
    EXPECT_NO_THROW(arena.allocate(kSlot - 63, 64)) << "the same slot once rounded";
}

TEST(BookArena, ARegionCarriesTheAdviceOnlyWhenAskedTo) {
    if (!kernel_has_thp()) GTEST_SKIP() << "a kernel without transparent huge pages refuses the advice";
    ob::BookArena huge(true);
    ob::BookArena plain(false);
    void* h = huge.allocate(kSlot, 64);
    void* p = plain.allocate(kSlot, 64);
    EXPECT_NE(vm_flags_of(h).find(" hg "), std::string::npos) << vm_flags_of(h);
    EXPECT_EQ(vm_flags_of(p).find(" hg "), std::string::npos) << vm_flags_of(p);
    EXPECT_NE(vm_flags_of(p), "") << "the control: the plain region is a mapping too";
}

TEST(BookArena, ABookHeldByAReaderOutlivesTheArenaItCameFrom) {
    auto arena = std::make_shared<ob::BookArena>(true);
    auto book = std::allocate_shared<ob::SoABuffer>(ob::BookArenaAllocator<ob::SoABuffer>(arena));
    EXPECT_EQ(arena->slots_in_use(), 1u);
    EXPECT_EQ(book->bid.depth, 0u) << "made as make_shared makes it: value-initialised";
    const std::weak_ptr<ob::BookArena> watch = arena;
    arena.reset();   // the engine lets go
    ASSERT_FALSE(watch.expired()) << "the book's allocator holds the arena";
    book->bid.depth = 7;   // still writable memory: under ASan, a use after free would stop here
    book.reset();
    EXPECT_TRUE(watch.expired()) << "and lets it go with the book";
}

TEST(BookArena, TheEngineMakesItsBooksInTheArenaUnlessToldNotTo) {
    const auto dir = std::filesystem::temp_directory_path() / "ob_test_book_arena_engine";
    std::filesystem::remove_all(dir);
    ob::DeltaUpdate du{};
    std::snprintf(du.symbol, sizeof(du.symbol), "BTC-USD");
    std::snprintf(du.exchange, sizeof(du.exchange), "EX");
    du.timestamp_ns = 1'700'000'000'000'000'000ULL;
    du.side = ob::SIDE_BID;
    du.n_levels = 1;
    const ob::Level level{100, 1, 1, 0};
    {
        ob::Engine engine(dir.string(), 1'000'000'000ULL);
        EXPECT_TRUE(engine.book_huge_pages()) << "on by default";
        engine.open();
        EXPECT_EQ(engine.book_arena_regions(), 0u);
        ASSERT_EQ(engine.apply_delta(du, &level), ob::OB_OK);
        EXPECT_EQ(engine.book_arena_regions(), 1u) << "the first book opened the arena's first region";
        EXPECT_EQ(engine.registry().gauge_value("ob_book_arena_regions"), 1);
        EXPECT_THROW(engine.set_book_huge_pages(false), std::logic_error)
            << "a book already made stays where it is";
        engine.close();
    }
    std::filesystem::remove_all(dir);
    {
        ob::Engine engine(dir.string(), 1'000'000'000ULL);
        engine.set_book_huge_pages(false);
        engine.open();
        ASSERT_EQ(engine.apply_delta(du, &level), ob::OB_OK);
        EXPECT_EQ(engine.book_arena_regions(), 0u) << "off: the book is the heap's";
        engine.close();
    }
    std::filesystem::remove_all(dir);
}
