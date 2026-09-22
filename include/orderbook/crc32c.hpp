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
// Measured again on aarch64 (Neoverse-V3, 4 vCPU, Amazon Linux 2023, GCC 14, Release), same
// harness and the same per-iteration mutation, when the second hardware path was added:
//
//   size        table        hardware     speedup
//   64 B        93.9 ns      5.9 ns       16x
//   128 B       229.5 ns     9.9 ns       23x
//   1 KB        2.14 us      40.4 ns      53x
//   64 KB       140.2 us     2.50 us      56x
//   4 MB        8.97 ms      160.5 us     56x
//
// The table is faster on this core than on the older x86 one (~470-680 MB/s against ~295 MB/s) and
// the instruction is faster still, so the ratio is wider, not narrower. The number that matters is
// the middle row: a 112-byte WAL record costs 220 ns of table fold that the instruction does not
// charge, on a machine where the whole ingestion path is measured in microseconds.
//
// Usage:
//   #include "orderbook/crc32c.hpp"
//   uint32_t checksum = ob::crc32c(data_ptr, data_len);

#include <array>
#include <cstddef>
#include <cstdint>

// Two architectures have an instruction for exactly this polynomial, and both are used when the
// CPU running the process has it: SSE4.2's `crc32` on x86-64, and the ARMv8 CRC32 extension's
// `crc32c*` on aarch64. Anything else — another architecture, another compiler — gets the table,
// which is the same code as before.
//
// On x86-64 the hardware path needs both the instruction and `__attribute__((target(...)))`, which
// is how it is compiled without a global -msse4.2. On aarch64 the equivalent lives in the assembly
// template; the long comment above that fold says why, and why it is not the intrinsic.
#if (defined(__x86_64__) || defined(_M_X64)) && (defined(__GNUC__) || defined(__clang__))
#define OB_CRC32C_X86 1
#include <nmmintrin.h>
#else
#define OB_CRC32C_X86 0
#endif

// aarch64 detection reads the Linux auxiliary vector, so this asks for Linux as well as the
// architecture. The engine needs epoll, so there is no aarch64 target here that is not Linux;
// naming it keeps the condition honest rather than lucky.
#if defined(__aarch64__) && defined(__linux__) && (defined(__GNUC__) || defined(__clang__))
#define OB_CRC32C_ARM 1
#include <asm/hwcap.h>
#include <sys/auxv.h>
#else
#define OB_CRC32C_ARM 0
#endif

// One condition for "this build has a second implementation to dispatch to", so that the call site
// below has one branch rather than one per architecture.
#define OB_CRC32C_HW (OB_CRC32C_X86 || OB_CRC32C_ARM)

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

#if OB_CRC32C_ARM

/// Hardware fold, aarch64.
///
/// Written as assembly rather than the `__crc32cd`/`__crc32cb` intrinsics, and that is a
/// measurement rather than a preference. The intrinsics live in <arm_acle.h> behind
/// `__ARM_FEATURE_CRC32`, which a per-function target attribute does not define, so each compiler
/// wants a different spelling and rejects the other's: GCC 14 exposes them when the include sits
/// inside `#pragma GCC target("+crc")` and clang 15 does not, while clang's own
/// `__builtin_arm_crc32cd` under `target("crc")` is refused by GCC, which wants `+crc`. Two
/// spellings of one instruction, each working on one compiler, is the shape this header exists to
/// avoid having.
///
/// The `.arch_extension` directive travels with the instruction inside the template, so one form
/// satisfies both compilers, and it costs nothing: measured on a Neoverse-V3, best of five runs,
/// the assembly form came out at 0.95x, 0.99x and 1.00x of the GCC intrinsic at 64 B, 1 KiB and
/// 4 MiB. The first attempt at that comparison reported 1.82 ns for 64 bytes *and* for four
/// megabytes, because the accumulator was cast to void and the whole fold was deleted — the same
/// class of non-measurement as the 82 TB/s above, and caught the same way, by the number being
/// absurd rather than by reading the code.
///
/// The alternative to selecting per function is building the whole engine for `armv8-a+crc`. CRC32
/// is mandatory from ARMv8.1, so that is true of every server-class part, and it would drop the
/// ARMv8.0 CPUs that this one binary still runs on. Runtime dispatch is the reason this header has
/// the shape it has, so the architecture that arrived second does not get to change it.
inline uint32_t crc32c_arm_u64(uint32_t crc, uint64_t v) noexcept {
    uint32_t out;
    __asm__(".arch_extension crc\n\tcrc32cx %w0, %w1, %x2" : "=r"(out) : "r"(crc), "r"(v));
    return out;
}

inline uint32_t crc32c_arm_u8(uint32_t crc, uint8_t v) noexcept {
    uint32_t out;
    __asm__(".arch_extension crc\n\tcrc32cb %w0, %w1, %w2" : "=r"(out) : "r"(crc), "r"(v));
    return out;
}

inline uint32_t crc32c_update_hw(uint32_t crc, const uint8_t* p, size_t len) noexcept {
    uint32_t c = crc;
    while (len >= 8) {
        uint64_t chunk;
        __builtin_memcpy(&chunk, p, sizeof(chunk));   // no alignment requirement, unlike a cast
        c = crc32c_arm_u64(c, chunk);
        p += 8;
        len -= 8;
    }
    while (len-- > 0) {
        c = crc32c_arm_u8(c, *p++);
    }
    return c;
}

/// Decided once, from the Linux auxiliary vector, which is the interface the kernel documents for
/// this feature. `__builtin_cpu_supports` does not cover it on both compilers at the versions this
/// tree supports.
///
/// Same failure mode as the x86 flag: lose the static-initialisation race and this reads false and
/// the table runs. Slower, never wrong.
inline const bool crc32c_has_hw = (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;

#endif  // OB_CRC32C_ARM

}  // namespace detail

/// Fold `len` bytes into a running CRC32C state.
///
/// Splitting a buffer anywhere and folding the pieces gives the same state as one call, for both
/// implementations — the instruction is defined on the same reflected polynomial as the table.
inline uint32_t crc32c_update(uint32_t crc, const void* data, size_t len) noexcept {
    const auto* p = static_cast<const uint8_t*>(data);
#if OB_CRC32C_HW
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
#if OB_CRC32C_HW
    return detail::crc32c_has_hw;
#else
    return false;
#endif
}

/// Which implementation this process will use, named rather than described.
///
/// The startup line printed "SSE4.2 instruction" on the hardware branch, which was true while
/// exactly one architecture had one. With two, the string is a claim about the CPU underneath and
/// has to be decided in the same place the dispatch is — otherwise a node on one architecture
/// reports the other one's instruction, and the log is the only thing that answers "which fold is
/// this process running" from outside.
inline const char* crc32c_implementation() noexcept {
#if OB_CRC32C_X86
    return detail::crc32c_has_hw ? "SSE4.2 crc32 instruction" : "lookup table";
#elif OB_CRC32C_ARM
    return detail::crc32c_has_hw ? "ARMv8 crc32c instruction" : "lookup table";
#else
    return "lookup table";
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
