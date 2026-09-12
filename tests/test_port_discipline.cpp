// Every fixed port a test binds must be one the kernel will never hand to somebody else (#109).
//
// On 12 September 2026 seven tests failed with `bind() failed on port 55400: Address already in
// use` against a tree whose only change was the command parser, and the same seven passed on their
// own. The cause was not a defect in the engine and not a `TIME_WAIT`: `/proc/sys/net/ipv4/
// ip_local_port_range` is 32768-60999, four test files named ports inside it, and any outgoing
// connection on the machine may be given one of those numbers at any instant.
//
// The floor is read back from the kernel rather than trusted from a constant, because the constant
// is the thing that would be wrong on the machine where this matters.

#include "test_ports.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <string>
#include <vector>

namespace {

/// `32768\t60999` - the kernel's own answer, or 0 when it will not say.
uint16_t ephemeral_floor_from_kernel() {
    std::ifstream in("/proc/sys/net/ipv4/ip_local_port_range");
    if (!in) return 0;
    unsigned low = 0, high = 0;
    in >> low >> high;
    if (!in || low == 0 || low > 65535) return 0;
    return static_cast<uint16_t>(low);
}

} // namespace

TEST(PortDiscipline, EveryFixedBlockIsBelowTheRangeTheKernelHandsOut) {
    const uint16_t floor_from_kernel = ephemeral_floor_from_kernel();
    ASSERT_GT(floor_from_kernel, 0u)
        << "could not read /proc/sys/net/ipv4/ip_local_port_range. The assumption this file exists "
        << "to check is unverifiable on this machine, and an unverifiable assumption asserted as "
        << "true is worse than one that is stated";

    // A machine configured with a lower floor is not a reason to pass: it is the reason to move the
    // blocks, and the number to move them below is printed right here.
    for (const uint16_t block : ob::test::kAllPortBlocks) {
        EXPECT_LT(block + ob::test::kPortBlockWidth, floor_from_kernel)
            << "port block " << block << "-" << (block + ob::test::kPortBlockWidth)
            << " reaches into the range this kernel hands to outgoing connections (from "
            << floor_from_kernel << "), so a test binding it can fail for a reason that has nothing "
            << "to do with the engine";
    }

    // And the assumed constant has to keep matching, or the comment in test_ports.hpp is describing
    // somebody else's machine.
    EXPECT_EQ(floor_from_kernel, ob::test::kEphemeralFloorAssumed)
        << "this kernel's ephemeral range starts at " << floor_from_kernel
        << ", not at the documented " << ob::test::kEphemeralFloorAssumed
        << " - the blocks are still safe, but test_ports.hpp now states a measurement that is not "
        << "this machine's";
}

TEST(PortDiscipline, NoTwoBlocksOverlap) {
    // The blocks are the only collision left once they are out of the kernel's range, so they are
    // the one thing this file can check that nothing else would notice: two tests sharing a number
    // fail only when they run close enough together, which is the flake that reads as a re-run.
    std::vector<uint16_t> blocks(std::begin(ob::test::kAllPortBlocks),
                                 std::end(ob::test::kAllPortBlocks));
    std::sort(blocks.begin(), blocks.end());
    for (size_t i = 1; i < blocks.size(); ++i) {
        EXPECT_GE(blocks[i] - blocks[i - 1], ob::test::kPortBlockWidth)
            << "blocks " << blocks[i - 1] << " and " << blocks[i] << " are closer than "
            << ob::test::kPortBlockWidth << " ports apart";
    }
    EXPECT_EQ(blocks.size(), 7u) << "a block was added or removed without this count being read";
}
