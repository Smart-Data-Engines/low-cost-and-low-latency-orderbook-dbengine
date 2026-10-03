#pragma once

#include <cstdint>

namespace ob {

/// A point in one primary's replication stream: whose stream it is (#101) and a position in that
/// primary's WAL, in the terms a replica confirms in (#98) - just past the last record applied.
///
/// What a handover is decided on: the outgoing primary announces where its stream ends, and its
/// successor stands without the election wait once its own position is there.
struct StreamPosition {
    uint64_t stream_id{0};   ///< 0: not known
    uint32_t file_index{0};
    uint64_t offset{0};
};

/// Whether `have` holds everything up to `end`: the same stream, known, and not before its end.
///
/// Positions in two streams are not comparable - two WALs give the same offsets to different
/// records, which is #61 - so a different or unknown stream is "no", whatever the numbers say.
inline bool stream_covers(const StreamPosition& have, const StreamPosition& end) {
    if (have.stream_id == 0 || have.stream_id != end.stream_id) return false;
    if (have.file_index != end.file_index) return have.file_index > end.file_index;
    return have.offset >= end.offset;
}

}  // namespace ob
