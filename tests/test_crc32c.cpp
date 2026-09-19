// CRC32C has two implementations, and the only interesting property is that you cannot tell which
// one ran.
//
// These checksums go into WAL record headers, snapshot manifests and every replication frame, so a
// build that computed them differently would reject its own files and disconnect its own peers. The
// hardware path is the SSE4.2 `crc32` instruction on x86-64 and the ARMv8 CRC32 extension's
// `crc32c*` on aarch64, both of which implement this exact reflected polynomial; that is a claim
// worth testing at every length and alignment rather than trusting.
//
// Read the first test before the rest: on a build with no hardware path, or on a CPU without the
// instruction, every "both implementations agree" test below compares the table with itself and
// passes without asserting anything. That was the state of this file on every non-x86 platform
// until aarch64 got a fold of its own.

#include "orderbook/crc32c.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <numeric>
#include <string>
#include <vector>

namespace {

std::vector<uint8_t> pattern(size_t n, uint8_t seed = 0) {
    std::vector<uint8_t> v(n);
    for (size_t i = 0; i < n; ++i) {
        v[i] = static_cast<uint8_t>(i * 31u + 7u + seed);
    }
    return v;
}

}  // namespace

TEST(Crc32c, TheComparisonsBelowHaveTwoImplementationsToCompare) {
    // The agreement tests call crc32c() and crc32c_table_only(). Where crc32c() *is* the table -
    // an architecture with no fold of its own, or a CPU that does not advertise the instruction -
    // those tests compare one function with itself: green, and vacuous.
    //
    // This test is the only thing that makes that visible from a passing run. It does not fail on
    // such a platform, because the table is the correct answer there and a red suite would be a
    // lie about the code; it skips, and says so, which is the difference between a suite that
    // reports its own scope and one that lets a reader assume it.
    if (!ob::crc32c_has_hardware()) {
        GTEST_SKIP() << "this build has no hardware CRC32C fold, or this CPU does not advertise "
                        "the instruction, so every agreement test in this file compares the table "
                        "implementation with itself";
    }
    SUCCEED() << "agreement tested against: " << ob::crc32c_implementation();
}

TEST(Crc32c, TheReportedImplementationIsTheOneThatRuns) {
    // Two facts that are printed once at startup and never again: whether a hardware fold was
    // found, and what it is called. They are decided in the same header but by different
    // expressions, so nothing but this stops them disagreeing - and the startup line is the only
    // answer an operator has to "which fold is this process running".
    const std::string named = ob::crc32c_implementation();
    if (ob::crc32c_has_hardware()) {
        EXPECT_NE(named, "lookup table");
    } else {
        EXPECT_EQ(named, "lookup table");
    }
}

TEST(Crc32c, BothImplementationsAgreeAtEveryLengthUpToAFewHundred) {
    // Every length, not a sample: the hardware path processes eight bytes at a time and then
    // finishes byte-wise, so the interesting cases are exactly the ones around each multiple of
    // eight, and a sampled test can miss all of them.
    for (size_t n = 0; n <= 300; ++n) {
        const auto data = pattern(n);
        const void* p = data.empty() ? nullptr : data.data();
        EXPECT_EQ(ob::crc32c(p, n), ob::crc32c_table_only(p, n)) << "length " << n;
    }
}

TEST(Crc32c, BothImplementationsAgreeAtEveryAlignment) {
    // The 8-byte form reads through memcpy rather than a cast, so an unaligned buffer must be
    // handled without a fault and without a different answer.
    const auto backing = pattern(512);
    for (size_t offset = 0; offset < 16; ++offset) {
        const uint8_t* p = backing.data() + offset;
        const size_t n = backing.size() - offset;
        EXPECT_EQ(ob::crc32c(p, n), ob::crc32c_table_only(p, n)) << "offset " << offset;
    }
}

TEST(Crc32c, BothImplementationsAgreeOnLargeBuffers) {
    for (size_t n : {size_t(4096), size_t(65536), size_t(1u << 20)}) {
        const auto data = pattern(n, 3);
        EXPECT_EQ(ob::crc32c(data.data(), n), ob::crc32c_table_only(data.data(), n))
            << "length " << n;
    }
}

TEST(Crc32c, KnownValuesFromTheStandard) {
    // Fixed values rather than a comparison between our own two paths: if both were wrong in the
    // same way, every test above would still pass. These are the published CRC32C check values.
    const std::string check = "123456789";
    EXPECT_EQ(ob::crc32c(check.data(), check.size()), 0xE3069283u);

    const std::string zeros(32, '\0');
    EXPECT_EQ(ob::crc32c(zeros.data(), zeros.size()), 0x8A9136AAu);

    const std::string ffs(32, '\xFF');
    EXPECT_EQ(ob::crc32c(ffs.data(), ffs.size()), 0x62A8AB43u);

    // And the empty input, which the header documents as the masked identity.
    EXPECT_EQ(ob::crc32c(nullptr, 0), 0x00000000u);
}

TEST(Crc32c, FoldingInPiecesMatchesOneCallOnWhicheverPathRuns) {
    const auto data = pattern(4096, 9);
    const uint32_t whole = ob::crc32c(data.data(), data.size());

    // Uneven splits on purpose, including the two boundaries: an implementation that only worked
    // on multiples of its block size would pass a two-halves test and fail here.
    for (size_t first : {size_t(0), size_t(1), size_t(7), size_t(8), size_t(9), size_t(64),
                         size_t(4095), size_t(4096)}) {
        uint32_t crc = ob::crc32c_init;
        crc = ob::crc32c_update(crc, data.data(), first);
        crc = ob::crc32c_update(crc, data.data() + first, data.size() - first);
        EXPECT_EQ(ob::crc32c_finish(crc), whole) << "split at " << first;
    }

    // Byte at a time, which is what a chunked reader degenerates to at the end of a file.
    uint32_t crc = ob::crc32c_init;
    for (size_t i = 0; i < 64; ++i) {
        crc = ob::crc32c_update(crc, data.data() + i, 1);
    }
    crc = ob::crc32c_update(crc, data.data() + 64, data.size() - 64);
    EXPECT_EQ(ob::crc32c_finish(crc), whole);
}

TEST(Crc32c, AnEmptyUpdateChangesNothing) {
    const auto data = pattern(100);
    uint32_t crc = ob::crc32c_update(ob::crc32c_init, data.data(), data.size());
    const uint32_t before = crc;
    crc = ob::crc32c_update(crc, nullptr, 0);
    crc = ob::crc32c_update(crc, data.data(), 0);
    EXPECT_EQ(crc, before);
}
