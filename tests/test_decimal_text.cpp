// The decimal text of an integer, eight digits at a time (#251): every write is held to the bytes
// `std::to_chars` writes for the same value, which is what a reply's rows were written by before.
//
// The values are drawn length by length - so many of each count of digits from 1 to 20 - from a
// generator with a fixed seed, rather than by RapidCheck's `arbitrary<uint64_t>()`, which draws
// from a width that grows with its size and so makes few long numbers (#49's step 1 found that out
// with a property that saw nearly nothing but small words).
#include "orderbook/decimal_text.hpp"

#include <gtest/gtest.h>

#include <array>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace dt = ob::decimal_text;

namespace {

template <typename T>
std::string to_chars_text(T v) {
    char buf[32];
    const auto res = std::to_chars(buf, buf + sizeof(buf), v);
    return std::string(buf, res.ptr);
}

/// What write() wrote for `v`, checked on the way to have touched nothing outside the room it
/// promises: a buffer of guards, the write in the middle of it.
template <typename T>
std::string written(T v) {
    constexpr size_t kGuard = 32;
    std::array<char, kGuard + dt::kWriteRoom + kGuard> buf;
    buf.fill('\x5A');
    char* const p = buf.data() + kGuard;
    char* const end = dt::write(p, v);
    EXPECT_GE(end, p);
    EXPECT_LE(end, p + dt::kWriteRoom);
    bool before = true;
    bool past = true;
    for (size_t i = 0; i < kGuard; ++i) {
        before = before && buf[i] == '\x5A';
        past = past && buf[kGuard + dt::kWriteRoom + i] == '\x5A';
    }
    EXPECT_TRUE(before) << "a byte before the write was touched, value " << v;
    EXPECT_TRUE(past) << "a byte past the room was touched, value " << v;
    return std::string(p, end);
}

constexpr uint64_t pow10(int k) {
    uint64_t v = 1;
    for (int i = 0; i < k; ++i) v *= 10;
    return v;
}

}  // namespace

TEST(DecimalText, EveryValueBelowTwoMillionIsToCharsBytes) {
    for (uint64_t v = 0; v < 2'000'000; ++v) {
        char buf[dt::kWriteRoom];
        char* const end = dt::write(buf, v);
        ASSERT_EQ(std::string(buf, end), to_chars_text(v)) << v;
    }
    for (int64_t v = -2'000'000; v < 2'000'000; ++v) {
        char buf[dt::kWriteRoom];
        char* const end = dt::write(buf, v);
        ASSERT_EQ(std::string(buf, end), to_chars_text(v)) << v;
    }
}

TEST(DecimalText, EveryPowerOfTwoAndOfTenAndTheirNeighbours) {
    std::vector<uint64_t> values{0, 1, std::numeric_limits<uint64_t>::max(),
                                 std::numeric_limits<uint64_t>::max() - 1};
    for (int k = 0; k < 64; ++k) {
        const uint64_t v = uint64_t{1} << k;
        values.insert(values.end(), {v - 1, v, v + 1});
    }
    for (int k = 0; k <= 19; ++k) {
        const uint64_t v = pow10(k);
        values.insert(values.end(), {v - 1, v, v + 1});
        for (uint64_t m = 2; m <= 9 && (k < 19 || m == 1); ++m) values.push_back(v * m - 1);
    }
    for (const uint64_t v : values) {
        EXPECT_EQ(written(v), to_chars_text(v)) << v;
        // The same magnitudes signed, either sign, as far as an int64_t holds them.
        if (v <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            const auto s = static_cast<int64_t>(v);
            EXPECT_EQ(written(s), to_chars_text(s)) << s;
            EXPECT_EQ(written(-s), to_chars_text(-s)) << -s;
        }
    }
    for (const int64_t s : {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min() + 1,
                            std::numeric_limits<int64_t>::max(), int64_t{-1}}) {
        EXPECT_EQ(written(s), to_chars_text(s)) << s;
    }
}

TEST(DecimalText, ManyValuesOfEveryLengthAreToCharsBytes) {
    std::mt19937_64 gen(251);
    for (int digits = 1; digits <= 20; ++digits) {
        const uint64_t lo = digits == 1 ? 0 : pow10(digits - 1);
        const uint64_t hi = digits == 20 ? std::numeric_limits<uint64_t>::max() : pow10(digits) - 1;
        std::uniform_int_distribution<uint64_t> in(lo, hi);
        for (int i = 0; i < 20'000; ++i) {
            const uint64_t v = in(gen);
            ASSERT_EQ(written(v), to_chars_text(v)) << v;
            if (v <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                const auto s = static_cast<int64_t>(v);
                ASSERT_EQ(written(-s), to_chars_text(-s)) << -s;
                ASSERT_EQ(written(s), to_chars_text(s)) << s;
            }
        }
    }
}

// put() writes by write() only where it has the room write() touches, and by to_chars where it has
// less: the same text either way, and nothing written at or past `end` - each buffer here is an
// allocation of exactly its size, so an address sanitizer build sees a byte past it.
TEST(DecimalText, PutWritesTheSameTextWithAnyRoomFromJustEnoughUp) {
    const std::vector<uint64_t> values{0, 7, 42, 99'999'999, 100'000'000, 1'234'567'890'123'456'789ULL,
                                       std::numeric_limits<uint64_t>::max()};
    for (const uint64_t v : values) {
        const std::string want = to_chars_text(v) + "\t";
        for (size_t room = want.size(); room <= dt::kWriteRoom + 8; ++room) {
            auto buf = std::make_unique<char[]>(room);
            char* const end = dt::put(buf.get(), buf.get() + room, v, '\t');
            ASSERT_EQ(std::string(buf.get(), end), want) << v << " in " << room << " bytes";
        }
    }
    for (const int64_t s : {int64_t{-1}, int64_t{-42}, std::numeric_limits<int64_t>::min(),
                            std::numeric_limits<int64_t>::max()}) {
        const std::string want = to_chars_text(s) + "\n";
        for (size_t room = want.size(); room <= dt::kWriteRoom + 8; ++room) {
            auto buf = std::make_unique<char[]>(room);
            char* const end = dt::put(buf.get(), buf.get() + room, s, '\n');
            ASSERT_EQ(std::string(buf.get(), end), want) << s << " in " << room << " bytes";
        }
    }
}

// The narrow types a row carries are widened, so a side of 1 is the digit 1 and not the byte 1.
TEST(DecimalText, PutWritesANarrowIntegerAsANumber) {
    char buf[dt::kWriteRoom];
    EXPECT_EQ(std::string(buf, dt::put(buf, buf + sizeof(buf), uint8_t{1}, '\t')), "1\t");
    EXPECT_EQ(std::string(buf, dt::put(buf, buf + sizeof(buf), uint8_t{255}, '\t')), "255\t");
    EXPECT_EQ(std::string(buf, dt::put(buf, buf + sizeof(buf), uint16_t{65535}, '\t')), "65535\t");
    EXPECT_EQ(std::string(buf, dt::put(buf, buf + sizeof(buf), uint32_t{4294967295U}, '\n')), "4294967295\n");
}
