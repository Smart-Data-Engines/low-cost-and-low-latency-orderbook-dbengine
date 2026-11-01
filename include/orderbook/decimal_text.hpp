#pragma once

// The decimal text of an integer, eight digits at a time (#251).
//
// A `SELECT`'s reply is text, seven integers a row, and answering one spent seven tenths of the
// server's time writing those integers: `std::to_chars` finds a number's length by dividing by ten
// thousand until it runs out, then writes it two digits at a time from the end, each pair a division
// by a hundred that waits for the one before. A 19-digit timestamp is a chain of about fourteen.
//
// Here a number below 100 is a pair from a table, and a longer one is cut into eights by one or two
// divisions by 10^8, which the compiler makes multiplications. Each eight is written with no chain:
// split into two halves of four, each half into two pairs and each pair into two digits by
// multiplications that work on every lane of a 64-bit word at once, and the word is stored whole,
// its leading zeros counted from the word itself and shifted out.
//
// The bytes are `std::to_chars`'s, which `tests/test_decimal_text.cpp` holds every write to. A write
// stores whole words, so it may touch bytes past the digits it writes: `kWriteRoom` is how many a
// caller must have from where it starts, and anything past the returned pointer is not text.

#include <array>
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

inline constexpr uint64_t k1e8  = 100'000'000ULL;
inline constexpr uint64_t k1e16 = 10'000'000'000'000'000ULL;
inline constexpr uint64_t kAsciiZeros = 0x3030303030303030ULL;

/// "00" to "99": the two digits of each number below 100, in its order.
inline constexpr std::array<char, 200> kPairs = [] {
    std::array<char, 200> t{};
    for (int i = 0; i < 100; ++i) {
        t[static_cast<std::size_t>(2 * i)]     = static_cast<char>('0' + i / 10);
        t[static_cast<std::size_t>(2 * i + 1)] = static_cast<char>('0' + i % 10);
    }
    return t;
}();

/// The eight digits of `n` < 10^8, leading zeros included, as the bytes of a little-endian word, each
/// byte a digit's value 0-9: the most significant in its lowest byte, which is the first in memory.
///
/// Each step divides every lane of the word at once by a multiplication and a shift that is exact
/// for the lane's range - x * 10486 >> 20 is x / 100 for x < 10^4, x * 103 >> 10 is x / 10 for
/// x < 100 - and no lane's product reaches the next lane, so the lanes never carry into each other.
[[gnu::always_inline]] inline uint64_t eight_digits(uint32_t n) {
    const uint64_t hi = n / 10000U;
    uint64_t x = hi | ((static_cast<uint64_t>(n) - hi * 10000U) << 32);       // two lanes of 4 digits
    const uint64_t hundreds = ((x * 10486U) >> 20) & 0x0000007F0000007FULL;
    x = hundreds | ((x - hundreds * 100U) << 16);                              // four lanes of 2
    const uint64_t tens = ((x * 103U) >> 10) & 0x000F000F000F000FULL;
    return tens | ((x - tens * 10U) << 8);                                     // eight lanes of 1
}

/// The digits of a word of them that is not 0, from its first that is not a leading zero, at `p`,
/// the whole word stored. A leading zero is a zero byte below the first digit that is not, so the
/// word's trailing zero bits, rounded down to whole bytes, are the leading zeros' bits.
[[gnu::always_inline]] inline char* put_leading(char* p, uint64_t digits) {
    const unsigned zeros = static_cast<unsigned>(std::countr_zero(digits)) & ~7U;
    const uint64_t word = (digits + kAsciiZeros) >> zeros;
    std::memcpy(p, &word, 8);
    return p + 8 - zeros / 8;
}

/// All eight digits of a word of them at `p`.
[[gnu::always_inline]] inline char* put_eight(char* p, uint64_t digits) {
    const uint64_t word = digits + kAsciiZeros;
    std::memcpy(p, &word, 8);
    return p + 8;
}

}  // namespace detail

/// `v` in decimal at `p`, which has `kWriteRoom` bytes; returns the end of its digits.
[[gnu::always_inline]] inline char* write(char* p, uint64_t v) {
    if constexpr (std::endian::native != std::endian::little) {
        return std::to_chars(p, p + kWriteRoom, v).ptr;
    }
    using namespace detail;
    if (v < 100) {
        // One digit or two, without a branch on which: a single digit d is the second byte of "0d",
        // copied with the byte after it, which the next write or nobody reads.
        const auto one = static_cast<std::size_t>(v < 10);
        std::memcpy(p, kPairs.data() + 2 * v + one, 2);
        return p + 2 - one;
    }
    if (v < k1e8) {
        return put_leading(p, eight_digits(static_cast<uint32_t>(v)));
    }
    if (v < k1e16) {
        const uint64_t hi = v / k1e8;
        p = put_leading(p, eight_digits(static_cast<uint32_t>(hi)));
        return put_eight(p, eight_digits(static_cast<uint32_t>(v - hi * k1e8)));
    }
    const uint64_t top  = v / k1e16;                       // at most 1844
    const uint64_t rest = v - top * k1e16;
    const uint64_t mid  = rest / k1e8;
    p = put_leading(p, eight_digits(static_cast<uint32_t>(top)));
    p = put_eight(p, eight_digits(static_cast<uint32_t>(mid)));
    return put_eight(p, eight_digits(static_cast<uint32_t>(rest - mid * k1e8)));
}

/// `v` in decimal at `p`, a minus sign first if it is negative, as `std::to_chars` writes it.
[[gnu::always_inline]] inline char* write(char* p, int64_t v) {
    uint64_t magnitude = static_cast<uint64_t>(v);
    if (v < 0) {
        *p++ = '-';
        magnitude = 0 - magnitude;   // INT64_MIN's magnitude too, which no int64_t holds
    }
    return write(p, magnitude);
}

/// `value` of any integer type in decimal and then `sep` at `p`, which has `kWriteRoom` bytes.
///
/// `side` is a `uint8_t`, which is a character type: handed to `to_chars` unwidened it would invite
/// an overload that writes a byte rather than a number, and to write() it would be ambiguous. Every
/// integer is widened to the one of write()'s two types its signedness says.
template <typename T>
[[gnu::always_inline]] inline char* put_in_room(char* p, T value, char sep) {
    static_assert(std::is_integral_v<T>, "a field is an integer");
    if constexpr (std::is_signed_v<T>) {
        p = write(p, static_cast<int64_t>(value));
    } else {
        p = write(p, static_cast<uint64_t>(value));
    }
    *p++ = sep;
    return p;
}

namespace detail {

/// put()'s way where fewer than `kWriteRoom` bytes are left, out of line: `std::to_chars` up to the
/// buffer's real end.
template <typename T>
[[gnu::noinline]] char* put_by_to_chars(char* p, char* end, T value, char sep) {
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

}  // namespace detail

/// `value` in decimal and then `sep` at `p`, nothing at or past `end`: by write() where `kWriteRoom`
/// bytes are left, by `std::to_chars` where fewer are - the same bytes either way, so whether the
/// words fit never rests on how a caller sized its buffer. Returns the end of what it wrote.
template <typename T>
[[gnu::always_inline]] inline char* put(char* p, char* end, T value, char sep) {
    if (end - p >= static_cast<std::ptrdiff_t>(kWriteRoom)) [[likely]] {
        return put_in_room(p, value, sep);
    }
    return detail::put_by_to_chars(p, end, value, sep);
}

}  // namespace ob::decimal_text
