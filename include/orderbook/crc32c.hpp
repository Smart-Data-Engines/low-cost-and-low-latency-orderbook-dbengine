#pragma once

// ── CRC32C (Castagnoli) — header-only, thread-safe ───────────────────────────
//
// Polynomial 0x1EDC6F41 (reflected: 0x82F63B78). Two implementations of one function:
//
//   - the SSE4.2 `crc32` instruction, which computes exactly this polynomial, used when the CPU
//     has it. Byte-identical to the table version at every length and alignment, which is a
//     requirement rather than a hope: these checksums are written into WAL records, snapshot
//     manifests and every replication frame, so a build that computed them differently would
//     reject its own files.
//   - a lookup table computed at compile time via consteval, as the portable path and the
//     fallback. Zero runtime initialisation, no data races.
//
// Measured on the development machine (i3-7100U, Release), buffer mutated per iteration so the
// call cannot be hoisted — the first attempt at this measurement reported 82 TB/s because it was:
//
//   size        table        hardware     speedup
//   64 B        197 ns       20 ns        10x
//   128 B       414 ns       25 ns        17x
//   1 KB        3.4 us       139 ns       25x
//   64 KB       222 us       10.4 us      21x
//   4 MB        14.3 ms      700 us       20x
//
// The table version runs at ~295 MB/s regardless of size, which for a checksum on the write path
// of an engine that advertises microsecond latency was the thing worth noticing: at 64 bytes it
// cost 197 ns per WAL record, four times the biggest hot-path saving previously recorded in the
// roadmap.
//
// Usage:
//   #include "orderbook/crc32c.hpp"
//   uint32_t checksum = ob::crc32c(data_ptr, data_len);

#include <array>
#include <cstddef>
#include <cstdint>

// x86-64 with a GNU-compatible compiler: the hardware path needs both the instruction and
// `__attribute__((target(...)))`, which is how it is compiled without a global -msse4.2. Anything
// else — another architecture, another compiler — gets the table, which is the same code as before.
#if (defined(__x86_64__) || defined(_M_X64)) && (defined(__GNUC__) || defined(__clang__))
#define OB_CRC32C_X86 1
#include <nmmintrin.h>
#else
#define OB_CRC32C_X86 0
#endif

namespace ob {
namespace detail {

inline constexpr uint32_t CRC32C_POLY = 0x82F63B78u;

/// Build the 256-entry lookup table at compile time.
consteval std::array<uint32_t, 256> make_crc32c_table() {
    std::array<uint32_t, 256> tbl{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ ((crc & 1u) ? CRC32C_POLY : 0u);
        }
        tbl[i] = crc;
    }
    return tbl;
}

inline constexpr auto crc32c_table = make_crc32c_table();

} // namespace detail

// ── Running form ──────────────────────────────────────────────────────────────
//
// For data that arrives in pieces: a file streamed in chunks, a payload checksummed while it is
// still being written. The three pieces below are what crc32c() is made of, exposed so that a
// caller with a stream does not have to buffer the whole thing to checksum it — and so that there
// is one copy of the table walk in the tree rather than one per streaming caller.

/// Initial state of a running CRC32C. Not a valid checksum on its own.
inline constexpr uint32_t crc32c_init = 0xFFFFFFFFu;

namespace detail {

/// Table fold. The portable path, and the fallback on a CPU without the instruction.
inline uint32_t crc32c_update_table(uint32_t crc, const uint8_t* p, size_t len) noexcept {
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ crc32c_table[(crc ^ p[i]) & 0xFFu];
    }
    return crc;
}

#if OB_CRC32C_X86

/// Hardware fold.
///
/// `target("sse4.2")` rather than a global `-msse4.2`, so the default build keeps its baseline
/// and this one function is compiled with the instruction available. Dispatch below decides
/// whether to call it.
///
/// The 8-byte form takes and returns a 64-bit accumulator whose upper half is always zero; that
/// is how the instruction is specified, not an accident worth "tidying" into uint32_t.
__attribute__((target("sse4.2")))
inline uint32_t crc32c_update_hw(uint32_t crc, const uint8_t* p, size_t len) noexcept {
    uint64_t acc = crc;
    while (len >= 8) {
        uint64_t chunk;
        __builtin_memcpy(&chunk, p, sizeof(chunk));   // no alignment requirement, unlike a cast
        acc = _mm_crc32_u64(acc, chunk);
        p += 8;
        len -= 8;
    }
    uint32_t c = static_cast<uint32_t>(acc);
    while (len-- > 0) {
        c = _mm_crc32_u8(c, *p++);
    }
    return c;
}

/// Decided once, at static initialisation, so the fast path costs a load and a branch rather
/// than a guard-variable check on every call.
///
/// If something calls crc32c() during another translation unit's static initialisation and wins
/// the race, this reads false and the table runs. Slower, never wrong — which is the only
/// property that matters for a checksum.
inline const bool crc32c_has_hw = __builtin_cpu_supports("sse4.2");

#endif  // OB_CRC32C_X86

}  // namespace detail

/// Fold `len` bytes into a running CRC32C state.
///
/// Splitting a buffer anywhere and folding the pieces gives the same state as one call, for both
/// implementations — the instruction is defined on the same reflected polynomial as the table.
inline uint32_t crc32c_update(uint32_t crc, const void* data, size_t len) noexcept {
    const auto* p = static_cast<const uint8_t*>(data);
#if OB_CRC32C_X86
    if (detail::crc32c_has_hw) {
        return detail::crc32c_update_hw(crc, p, len);
    }
#endif
    return detail::crc32c_update_table(crc, p, len);
}

/// Turn a running state into the checksum. Must be applied exactly once, at the end.
inline constexpr uint32_t crc32c_finish(uint32_t crc) noexcept { return crc ^ 0xFFFFFFFFu; }

/// Compute CRC32C over `len` bytes starting at `data`.
/// Returns 0x00000000 for zero-length input (consistent with the masked identity).
inline uint32_t crc32c(const void* data, size_t len) noexcept {
    return crc32c_finish(crc32c_update(crc32c_init, data, len));
}

/// Whether this process will use the CRC32C instruction.
///
/// Worth logging at startup: the difference is a factor of twenty on the write path, and "which
/// implementation am I running" is not otherwise answerable from outside.
inline bool crc32c_has_hardware() noexcept {
#if OB_CRC32C_X86
    return detail::crc32c_has_hw;
#else
    return false;
#endif
}

/// The table implementation, exposed so a test can compare the two.
///
/// Not for production callers: they get whichever is faster on this CPU, and the point of the
/// test is that the choice cannot be observed in the output.
inline uint32_t crc32c_table_only(const void* data, size_t len) noexcept {
    return crc32c_finish(
        detail::crc32c_update_table(crc32c_init, static_cast<const uint8_t*>(data), len));
}

} // namespace ob
