#pragma once

// ── Portable CRC32C (Castagnoli) — header-only, thread-safe ──────────────────
//
// Software lookup-table implementation. Polynomial: 0x1EDC6F41 (reflected: 0x82F63B78).
// The table is computed at compile time via consteval, so there is zero runtime
// initialization cost and no data races.
//
// Usage:
//   #include "orderbook/crc32c.hpp"
//   uint32_t checksum = ob::crc32c(data_ptr, data_len);

#include <array>
#include <cstddef>
#include <cstdint>

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

/// Fold `len` bytes into a running CRC32C state.
inline uint32_t crc32c_update(uint32_t crc, const void* data, size_t len) noexcept {
    const auto* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ detail::crc32c_table[(crc ^ p[i]) & 0xFFu];
    }
    return crc;
}

/// Turn a running state into the checksum. Must be applied exactly once, at the end.
inline constexpr uint32_t crc32c_finish(uint32_t crc) noexcept { return crc ^ 0xFFFFFFFFu; }

/// Compute CRC32C over `len` bytes starting at `data`.
/// Returns 0x00000000 for zero-length input (consistent with the masked identity).
inline uint32_t crc32c(const void* data, size_t len) noexcept {
    return crc32c_finish(crc32c_update(crc32c_init, data, len));
}

} // namespace ob
