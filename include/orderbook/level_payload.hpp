#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "orderbook/data_model.hpp"

namespace ob {

/// The levels of a serialised delta, copied out of a buffer whose alignment nothing guarantees.
///
/// **Why a copy rather than a cast.** Every path that receives a delta — the mesh, the replication
/// stream, WAL replay — has a `DeltaUpdate` followed by `n_levels` `Level`s inside a byte buffer it
/// read from a socket or a file, and the obvious `reinterpret_cast<const Level*>(payload +
/// sizeof(DeltaUpdate))` is **undefined behaviour** unless that address happens to be
/// `alignof(Level)`-aligned. It usually does not: a mesh frame is a 4-byte length and a 38-byte
/// `WALRecordV2`, so the payload starts 42 bytes into the receive buffer and the levels 130 bytes
/// in, neither of which is a multiple of eight.
///
/// Measured, on the tree that added a wire-level test able to reach it: UBSan reports
/// `member access within misaligned address … for type 'const struct Level'` at
/// `engine.cpp`'s first read of `levels[i].price`, from `handle_remote_record()` through
/// `process_recv_buf()` on the mesh io loop (#127). It compiles to working code on x86 today —
/// unaligned loads are permitted there — which is exactly why it went unnoticed: the standard lets
/// a compiler assume the alignment it was promised, and nothing about that promise was true.
///
/// **The scratch buffer belongs to the caller**, so the hot paths keep one and pay no allocation
/// after the first record of their process. `resize()` on a vector that is already large enough
/// does not reallocate, and it does not shrink.
///
/// Returns `nullptr` for `n_levels == 0`, which is what the callers pass on to an apply path that
/// treats an empty delta as a valid record.
inline const Level* levels_from_payload(const void* payload, uint16_t n_levels,
                                        std::vector<Level>& scratch) {
    if (n_levels == 0) return nullptr;
    // Grown, never resized down, and measured rather than assumed: `resize(n)` on a vector of a
    // type with a default member initialiser - `Level::_pad{}` - **value-initialises** the new
    // elements, so calling it per record zero-fills bytes the `memcpy` below immediately
    // overwrites. Asking first makes the steady state one comparison and one copy.
    if (scratch.size() < n_levels) scratch.resize(n_levels);
    std::memcpy(scratch.data(),
                static_cast<const uint8_t*>(payload) + sizeof(DeltaUpdate),
                static_cast<size_t>(n_levels) * sizeof(Level));
    return scratch.data();
}

}  // namespace ob
