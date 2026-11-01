#pragma once

// The decimal text of an integer, eight digits at a time (#251).
//
// A `SELECT`'s reply is text, seven integers a row, and answering one spent seven tenths of the
// server's time writing those integers: `std::to_chars` finds a number's length by dividing by ten
// thousand until it runs out, then writes it two digits at a time from the end, each pair a division
// by a hundred that waits for the one before. A 19-digit timestamp is a chain of about fourteen.
//
// This writes eight digits with no chain: the number is split into two halves of four, each half
// into two pairs and each pair into two digits by multiplications that work on all of the lanes of a
// 64-bit word at once, and the word is stored whole. A number of more than eight digits is cut into
// eights first, by one or two divisions by 10^8 that the compiler makes multiplications.
//
// The bytes are `std::to_chars`'s, which `tests/test_decimal_text.cpp` holds every write to. A write
// stores whole words, so it may touch bytes past the digits it writes: `kWriteRoom` is how many a
// caller must have from where it starts, and anything past the returned pointer is not text.

#include <bit>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace ob::decimal_text {

/// Bytes a write may touch from where it starts. The most it touches is twenty - a uint64_t's twenty
/// digits, or a sign and an int64_t's nineteen - and a field's separator goes after them; a word
/// more than that, so a caller's check need not count.
inline constexpr std::size_t kWriteRoom = 24;

namespace detail {

inline constexpr uint64_t kPow10[20] = {
    1ULL,
    10ULL,
    100ULL,
    1000ULL,
    10000ULL,
    100000ULL,
    1000000ULL,
    10000000ULL,
    100000000ULL,
    1000000000ULL,
    10000000000ULL,
    100000000000ULL,
    1000000000000ULL,
    10000000000000ULL,
    100000000000000ULL,
    1000000000000000ULL,
    10000000000000000ULL,
    100000000000000000ULL,
    1000000000000000000ULL,
    10000000000000000000ULL,
};

/// How many decimal digits `v` has, 1 for 0: the bits it needs times log10(2), which 1233 / 4096
/// approximates from below closely enough that one comparison with a power of ten corrects it.
/// Counted for `v | 1`, which has as many digits as `v` - it adds one only to an even number, and an
/// even number plus one is no power of ten past 1 - and is not below 10^0, as 0 is.
inline unsigned digits(uint64_t v) {
    const uint64_t w = v | 1;
    const unsigned t = (static_cast<unsigned>(std::bit_width(w)) * 1233U) >> 12;
    return t + 1 - static_cast<unsigned>(w < kPow10[t]);
}

/// The eight digits of `n` < 10^8, leading zeros included, as the bytes of a little-endian word:
/// the most significant digit in its lowest byte, which is the first in memory.
///
/// Each step divides every lane of the word at once by a multiplication and a shift that is exact
/// for the lane's range - x * 10486 >> 20 is x / 100 for x < 10^4, x * 103 >> 10 is x / 10 for
/// x < 100 - and no lane's product reaches the next lane, so the lanes never carry into each other.
inline uint64_t eight_digits(uint32_t n) {
    const uint64_t hi = n / 10000U;
    uint64_t x = hi | ((static_cast<uint64_t>(n) - hi * 10000U) << 32);       // two lanes of 4 digits
    const uint64_t hundreds = ((x * 10486U) >> 20) & 0x0000007F0000007FULL;
    x = hundreds | ((x - hundreds * 100U) << 16);                              // four lanes of 2
    const uint64_t tens = ((x * 103U) >> 10) & 0x000F000F000F000FULL;
    x = tens | ((x - tens * 10U) << 8);                                        // eight lanes of 1
    return x + 0x3030303030303030ULL;
}

/// The last `k` of a word's eight digits (1 <= k <= 8) at `p`, the whole word stored.
inline char* put_leading(char* p, uint64_t word, unsigned k) {
    word >>= 8 * (8 - k);
    std::memcpy(p, &word, 8);
    return p + k;
}

inline char* put_eight(char* p, uint64_t word) {
    std::memcpy(p, &word, 8);
    return p + 8;
}

}  // namespace detail

/// `v` in decimal at `p`, which has `kWriteRoom` bytes; returns the end of its digits.
inline char* write(char* p, uint64_t v) {
    if constexpr (std::endian::native != std::endian::little) {
        return std::to_chars(p, p + kWriteRoom, v).ptr;
    }
    using namespace detail;
    if (v < kPow10[8]) {
        return put_leading(p, eight_digits(static_cast<uint32_t>(v)), digits(v));
    }
    if (v < kPow10[16]) {
        const uint64_t hi = v / kPow10[8];
        p = put_leading(p, eight_digits(static_cast<uint32_t>(hi)), digits(hi));
        return put_eight(p, eight_digits(static_cast<uint32_t>(v - hi * kPow10[8])));
    }
    const uint64_t top  = v / kPow10[16];                  // at most 1844
    const uint64_t rest = v - top * kPow10[16];
    const uint64_t mid  = rest / kPow10[8];
    p = put_leading(p, eight_digits(static_cast<uint32_t>(top)), digits(top));
    p = put_eight(p, eight_digits(static_cast<uint32_t>(mid)));
    return put_eight(p, eight_digits(static_cast<uint32_t>(rest - mid * kPow10[8])));
}

/// `v` in decimal at `p`, a minus sign first if it is negative, as `std::to_chars` writes it.
inline char* write(char* p, int64_t v) {
    uint64_t magnitude = static_cast<uint64_t>(v);
    if (v < 0) {
        *p++ = '-';
        magnitude = 0 - magnitude;   // INT64_MIN's magnitude too, which no int64_t holds
    }
    return write(p, magnitude);
}

/// `value` in decimal and then `sep` at `p`, nothing at or past `end`: by write() where `kWriteRoom`
/// bytes are left, by `std::to_chars` where fewer are - the same bytes either way, so whether the
/// words fit never rests on how a caller sized its buffer. Returns the end of what it wrote.
template <typename T>
inline char* put(char* p, char* end, T value, char sep) {
    static_assert(std::is_integral_v<T>, "a field is an integer");
    // `side` is a `uint8_t`, which is a character type: handing it to `to_chars` unwidened invites
    // an overload that would write a byte rather than a number. Widen every integer to the type
    // `to_chars` cannot misread - and write() has only the two.
    if (end - p >= static_cast<std::ptrdiff_t>(kWriteRoom)) {
        if constexpr (std::is_signed_v<T>) {
            p = write(p, static_cast<int64_t>(value));
        } else {
            p = write(p, static_cast<uint64_t>(value));
        }
        *p++ = sep;
        return p;
    }
    // `end` is the buffer's real end rather than `p + 24`. The fixed window was correct only
    // because the buffer was always wider than any one field; handing `to_chars` a pointer past
    // the allocation is the kind of detail that is fine until a caller sizes the buffer tightly.
    //
    // The separator's byte is held back from `to_chars` rather than assumed to be spare. The
    // callers size their buffers so that it is, but that is an argument made two functions away,
    // and GCC 11 says so out loud: `writing 1 byte into a region of size 0` on the store below.
    // Reserving it here makes the guarantee local and provable instead of true-by-inspection.
    char* const digits_end = end - 1;
    if constexpr (std::is_signed_v<T>) {
        p = std::to_chars(p, digits_end, static_cast<long long>(value)).ptr;
    } else {
        p = std::to_chars(p, digits_end, static_cast<unsigned long long>(value)).ptr;
    }
    *p++ = sep;
    return p;
}

}  // namespace ob::decimal_text
