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

/// Compute CRC32C over `len` bytes starting at `data`.
/// Returns 0x00000000 for zero-length input (consistent with the masked identity).
inline uint32_t crc32c(const void* data, size_t len) noexcept {
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ detail::crc32c_table[(crc ^ p[i]) & 0xFFu];
    }
    return crc ^ 0xFFFFFFFFu;
}

} // namespace ob
