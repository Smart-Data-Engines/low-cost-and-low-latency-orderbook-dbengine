#pragma once

// ── Merging the segments seals write (#165 part 2b) ───────────────────────────
//
// Part 2a took the segment count from one per active symbol per tick to one per `kSealAge` at a
// trickle: 2 304 after a 90-second soak at 256 symbols, ~92 000 an hour, ~2.2 million a day - and a
// cold start pays ~0.77 ms for each, an index entry ~250 bytes. Compaction merges a symbol's small
// segments into bigger ones, in the flush tick, so the count follows the data rather than uptime.
//
// The size a merge stops at is a trade, and it was measured before it was chosen: a query reads a
// segment whole - price and sequence are delta-encoded from its first row, quantities are Simple8b
// words that decode from the first - so a one-second query in a segment of 65 536 rows took 0.79 ms
// on the m9g.xlarge, 3.2-3.5 ms at 262 144 and 14.3-14.5 at a million, while a merge costs about
// 50 ns a row whatever its size (benchmarks/segment_size_cost). The design is in
// kiro-workspace/specs/segment-growth/design.md §2.5.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace ob::compaction {

/// Merged segments stop at this many rows: a narrow query in one reads it whole.
inline constexpr size_t kMaxRows = 262'144;
/// A segment of at least this many rows is full and merges with nothing. It is what a seal at the
/// write ceiling writes (`Engine::kSealRows`), so the ceiling gives compaction no work.
inline constexpr size_t kFullRows = 65'536;
/// How many segments of one level make a merge: each row is written once per level it climbs,
/// log8 of the ratio between a merged segment and a sealed one.
inline constexpr size_t kFanIn = 8;
/// A partition whose period ended this long ago, and which has received nothing for as long, is
/// settled: its segments that are not full merge whatever their levels, into as few as fit.
inline constexpr std::chrono::seconds kSettle{60};
/// A flush tick that drained no more than `Engine::kSealRows` rows merges for up to this long; a
/// heavier one - the write ceiling - merges nothing, unless none has for `kMaxDelay`, and then one.
inline constexpr std::chrono::milliseconds kTickBudget{10};
inline constexpr std::chrono::seconds kMaxDelay{10};
/// How many partitions one tick looks at, so ten thousand symbols' partitions are not all read
/// in one tick; the next tick resumes after the last.
inline constexpr size_t kLooksPerTick = 128;

/// One segment of a partition's view, in the order a scan delivers them, with what the engine knows
/// of it.
struct Candidate {
    uint64_t rows{0};
    uint32_t level{0};
    /// The segment's end is in the partition's period. The others are in the view because they lie
    /// between two members, and a merge may take only segments nothing lies between.
    bool member{false};
    /// It may be merged now: readable, its range its rows', of a known WAL, and - if this node's
    /// WAL - vouched for by a checkpoint that is on the device.
    bool eligible{false};
    /// Which WAL its position refers to: a merge's inputs share one, which the merged segment keeps.
    uint64_t wal_identity{0};
};

/// What decides whether one segment may be a merge's input.
struct SegmentFacts {
    uint64_t wal_identity{0};
    bool     range_is_rows{false};    ///< its range is its rows' (#166), not repaired from a guess
    bool     current_format{false};   ///< the format this build writes
    uint64_t seal_epoch{0};
};

/// Whether a segment may be merged now: of a known WAL, its range its rows', of this build's format -
/// and, if it is of this node's WAL, vouched for by a checkpoint known to be on the device
/// (`vouched_epoch`). A merged segment takes its inputs' highest epoch, and a start keeps a segment
/// of this WAL only if the last checkpoint vouches for its epoch: merging one no checkpoint on the
/// device vouches for would make the merged segment one a power cut removes, with the inputs it
/// replaced already gone. A segment of another WAL - a snapshot's - is vouched for as it stands.
bool may_merge(const SegmentFacts& segment, uint64_t local_wal_identity, uint64_t vouched_epoch);

/// Whether a partition is settled: its period ended `kSettle` ago by the wall clock rows are stamped
/// with, and nothing has been added to it for `kSettle`.
bool settled(uint64_t period_end_ns, uint64_t wall_now_ns, std::chrono::nanoseconds since_added);
/// How long until an unsettled partition settles, if nothing is added to it meanwhile; zero once it
/// has.
std::chrono::nanoseconds until_settled(uint64_t period_end_ns, uint64_t wall_now_ns,
                                       std::chrono::nanoseconds since_added);

/// Consecutive candidates to merge into one segment: [first, first + count).
struct Run {
    size_t first{0};
    size_t count{0};
    bool operator==(const Run& o) const { return first == o.first && count == o.count; }
};

/// The merges a partition calls for, in order and disjoint. A run is at least two consecutive
/// members that are eligible, not full and of one WAL, at most `kMaxRows` rows together; and, until
/// the partition is `settled`, of one level and taken `kFanIn` at a time - fewer only when that many
/// would pass `kMaxRows`. A settled partition's runs ignore the level and take as many as fit.
std::vector<Run> plan(const std::vector<Candidate>& segments, bool settled);

}  // namespace ob::compaction
