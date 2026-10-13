// The held columns' memory (#223): slots of power-of-two classes in 2 MiB regions advised for
// transparent huge pages, given back when a column goes and taken again by the next.
//
// Whether the kernel backs a region with a huge page depends on the machine, so these do not ask
// it. They ask what the arena controls: where a slot is, that it comes back, what the cap and the
// slack allow, and that a region carries the advice - `hg` among its VmFlags in /proc/self/smaps.

#include "orderbook/column_arena.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr size_t kKiB = size_t{1} << 10;
constexpr size_t kMiB = size_t{1} << 20;

std::shared_ptr<ob::ColumnArena> arena_of(size_t cap, size_t slack = 2 * kMiB, bool huge = true) {
    return std::make_shared<ob::ColumnArena>(cap, slack, huge);
}

uintptr_t at(const ob::ArenaBlock& b) { return reinterpret_cast<uintptr_t>(b.data()); }

/// The VmFlags of the mapping holding `p`, from /proc/self/smaps; "" when no mapping holds it.
std::string vm_flags_of(const void* p) {
    const auto a = reinterpret_cast<uintptr_t>(p);
    std::ifstream in("/proc/self/smaps");
    std::string line;
    bool inside = false;
    while (std::getline(in, line)) {
        unsigned long lo = 0, hi = 0;
        if (std::sscanf(line.c_str(), "%lx-%lx ", &lo, &hi) == 2) {
            inside = a >= lo && a < hi;
        } else if (inside && line.rfind("VmFlags:", 0) == 0) {
            return line + " ";
        }
    }
    return "";
}

} // namespace

TEST(ColumnArena, ClassesArePowersOfTwoFrom64KiBTo2MiB) {
    using A = ob::ColumnArena;
    EXPECT_EQ(A::class_bytes(0), 0u);
    EXPECT_EQ(A::class_bytes(64 * kKiB - 1), 0u) << "a few pages: the heap's";
    EXPECT_EQ(A::class_bytes(64 * kKiB), 64 * kKiB);
    EXPECT_EQ(A::class_bytes(64 * kKiB + 1), 128 * kKiB);
    EXPECT_EQ(A::class_bytes(80'000), 128 * kKiB) << "ten thousand 8-byte values";
    EXPECT_EQ(A::class_bytes(300 * kKiB), 512 * kKiB) << "a power of two, not the next 64 KiB";
    EXPECT_EQ(A::class_bytes(2 * kMiB), 2 * kMiB) << "262 144 8-byte values: a segment's most";
    EXPECT_EQ(A::class_bytes(2 * kMiB + 1), 0u);
}

TEST(ColumnArena, ASlotIsInAnAlignedRegionAndTheSlotsOfAClassFollowEachOther) {
    auto arena = arena_of(64 * kMiB);
    ob::ArenaBlock a = arena->allocate(100 * kKiB);
    ob::ArenaBlock b = arena->allocate(128 * kKiB);
    ASSERT_TRUE(a && b);
    EXPECT_EQ(a.bytes(), 128 * kKiB);
    EXPECT_EQ(at(a) % ob::kHugeRegionBytes, 0u) << "the first slot is the region's start";
    EXPECT_EQ(at(b) - at(a), 128 * kKiB);
    const auto s = arena->stats();
    EXPECT_EQ(s.regions, 1u);
    EXPECT_EQ(s.mapped_bytes, 2 * kMiB);
    EXPECT_EQ(s.free_bytes, 2 * kMiB - 2 * 128 * kKiB);
    EXPECT_EQ(s.fallbacks, 0u);
}

TEST(ColumnArena, AColumnTooSmallOrTooLargeIsNotTheArenas) {
    auto arena = arena_of(64 * kMiB);
    EXPECT_FALSE(arena->allocate(1000));
    EXPECT_FALSE(arena->allocate(3 * kMiB));
    EXPECT_EQ(arena->stats().regions, 0u);
    EXPECT_EQ(arena->stats().fallbacks, 0u) << "not the arena's is not a fallback: there was no room to want";
}

TEST(ColumnArena, AFreedSlotIsTakenAgainWithoutANewRegion) {
    auto arena = arena_of(64 * kMiB);
    ob::ArenaBlock a = arena->allocate(1 * kMiB);
    ob::ArenaBlock b = arena->allocate(1 * kMiB);
    const uintptr_t where = at(a);
    a = ob::ArenaBlock{};   // the column goes
    EXPECT_EQ(arena->stats().free_bytes, 1 * kMiB);
    ob::ArenaBlock c = arena->allocate(700 * kKiB);
    EXPECT_EQ(at(c), where) << "the slot given back is the next column's";
    EXPECT_NE(at(c), at(b)) << "the control: a slot in use is not given twice";
    EXPECT_EQ(arena->stats().regions, 1u);
}

TEST(ColumnArena, AWhollyFreeRegionIsCarvedForWhicheverClassNeedsIt) {
    auto arena = arena_of(2 * kMiB);   // room for one region
    {
        ob::ArenaBlock whole = arena->allocate(2 * kMiB);
        ASSERT_TRUE(whole);
    }
    ob::ArenaBlock small = arena->allocate(64 * kKiB);
    ASSERT_TRUE(small) << "the region, wholly free, carved again for 64 KiB";
    ob::ArenaBlock next = arena->allocate(64 * kKiB);
    EXPECT_EQ(at(next) - at(small), 64 * kKiB);
    EXPECT_EQ(arena->stats().regions, 1u);
    EXPECT_EQ(arena->stats().free_bytes, 2 * kMiB - 2 * 64 * kKiB);
}

TEST(ColumnArena, PastTheCapAColumnGoesToTheHeap) {
    auto arena = arena_of(2 * kMiB);
    ob::ArenaBlock a = arena->allocate(2 * kMiB);
    ASSERT_TRUE(a);
    EXPECT_FALSE(arena->allocate(2 * kMiB)) << "a second region would pass the cap";
    EXPECT_FALSE(arena->allocate(64 * kKiB)) << "and the one region holds a column of another class";
    EXPECT_EQ(arena->stats().fallbacks, 2u);
    EXPECT_EQ(arena->stats().mapped_bytes, 2 * kMiB);
}

TEST(ColumnArena, WhatIsFreeBeyondTheSlackGoesBackToTheKernel) {
    auto arena = arena_of(16 * kMiB, /*slack=*/2 * kMiB);
    {
        std::vector<ob::ArenaBlock> held;
        for (int i = 0; i < 4; ++i) held.push_back(arena->allocate(2 * kMiB));
        EXPECT_EQ(arena->stats().regions, 4u);
    }
    const auto s = arena->stats();
    EXPECT_EQ(s.regions, 1u) << "three regions given back, one kept for the next column";
    EXPECT_EQ(s.mapped_bytes, 2 * kMiB);
    EXPECT_EQ(s.free_bytes, 2 * kMiB);
}

TEST(ColumnArena, ARegionCarriesTheAdviceOnlyWhenAskedTo) {
    if (!std::filesystem::exists("/sys/kernel/mm/transparent_hugepage/enabled")) {
        GTEST_SKIP() << "a kernel without transparent huge pages refuses the advice";
    }
    auto huge = arena_of(64 * kMiB, 2 * kMiB, true);
    auto plain = arena_of(64 * kMiB, 2 * kMiB, false);
    ob::ArenaBlock h = huge->allocate(2 * kMiB);
    ob::ArenaBlock p = plain->allocate(2 * kMiB);
    EXPECT_NE(vm_flags_of(h.data()).find(" hg "), std::string::npos) << vm_flags_of(h.data());
    EXPECT_EQ(vm_flags_of(p.data()).find(" hg "), std::string::npos) << vm_flags_of(p.data());
    EXPECT_NE(vm_flags_of(p.data()), "") << "the control: the plain region is a mapping too";
    // A region carved for a class under kHugeClassBytes is not advised, even in an arena that asks:
    // its first column would pay for zeroing 2 MiB to use a fraction of them.
    ob::ArenaBlock small = huge->allocate(256 * kKiB);
    ob::ArenaBlock large = huge->allocate(ob::ColumnArena::kHugeClassBytes);
    EXPECT_EQ(vm_flags_of(small.data()).find(" hg "), std::string::npos) << vm_flags_of(small.data());
    EXPECT_NE(vm_flags_of(large.data()).find(" hg "), std::string::npos) << vm_flags_of(large.data());
}

TEST(ColumnArena, ABlockHoldsItsArenaAndMovesWithItsSlot) {
    auto arena = arena_of(64 * kMiB);
    const std::weak_ptr<ob::ColumnArena> watch = arena;
    ob::ArenaBlock a = arena->allocate(256 * kKiB);
    arena.reset();   // the budget lets go
    ASSERT_FALSE(watch.expired()) << "a column a read still holds keeps the arena";
    static_cast<char*>(a.data())[0] = 1;   // still writable: under ASan a use after free stops here
    ob::ArenaBlock b = std::move(a);
    EXPECT_FALSE(a) << "moved from: no slot of its own to give back";
    EXPECT_TRUE(b);
    ob::ArenaBlock c;
    c = std::move(b);
    EXPECT_FALSE(watch.expired());
    c = ob::ArenaBlock{};
    EXPECT_TRUE(watch.expired()) << "and lets it go with the last slot";
}

TEST(ColumnArena, ThreadsTakingAndGivingBackLeaveEverySlotFree) {
    auto arena = arena_of(64 * kMiB, /*slack=*/64 * kMiB);
    std::atomic<int> wrong{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937 rng(static_cast<unsigned>(t + 1));
            std::vector<ob::ArenaBlock> held;
            for (int i = 0; i < 2000; ++i) {
                if (held.size() < 6 && rng() % 2 == 0) {
                    const size_t bytes = (64 * kKiB) << (rng() % 6);
                    ob::ArenaBlock b = arena->allocate(bytes);
                    if (b) {
                        // Owned alone: no other thread's slot overlaps it.
                        static_cast<unsigned char*>(b.data())[0] = static_cast<unsigned char>(t);
                        static_cast<unsigned char*>(b.data())[b.bytes() - 1] = static_cast<unsigned char>(t);
                        held.push_back(std::move(b));
                    }
                } else if (!held.empty()) {
                    const auto& b = held.back();
                    if (static_cast<unsigned char*>(b.data())[0] != t ||
                        static_cast<unsigned char*>(b.data())[b.bytes() - 1] != t) {
                        wrong.fetch_add(1);
                    }
                    held.pop_back();
                }
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(wrong.load(), 0) << "a slot two threads held at once";
    const auto s = arena->stats();
    EXPECT_EQ(s.free_bytes, s.mapped_bytes) << "every slot back";
}
