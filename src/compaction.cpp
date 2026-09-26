#include "orderbook/compaction.hpp"

#include <algorithm>
#include <cstdint>

namespace ob::compaction {

namespace {

bool mergeable(const Candidate& c) {
    return c.member && c.eligible && c.rows < kFullRows;
}

uint64_t settle_ns() {
    return static_cast<uint64_t>(std::chrono::nanoseconds(kSettle).count());
}

/// (start, end) of `a` before (start, end) of `b`, strictly.
bool range_before(uint64_t a_start, uint64_t a_end, uint64_t b_start, uint64_t b_end) {
    return a_start < b_start || (a_start == b_start && a_end < b_end);
}

/// Whether merging [first, first + count) keeps every segment of the view where it was in the order
/// a scan delivers them: the merged range after the segment before the run and before the one after
/// it, both strictly - the directory's name, which breaks a tie, is the publication's to choose.
bool sorts_where_it_was(const std::vector<Candidate>& s, size_t first, size_t count) {
    const uint64_t start = s[first].start_ts_ns;
    uint64_t end = 0;
    for (size_t i = first; i < first + count; ++i) end = std::max(end, s[i].end_ts_ns);
    if (first > 0 && !range_before(s[first - 1].start_ts_ns, s[first - 1].end_ts_ns, start, end)) {
        return false;
    }
    const size_t after = first + count;
    return after >= s.size() || range_before(start, end, s[after].start_ts_ns, s[after].end_ts_ns);
}

}  // namespace

bool may_merge(const SegmentFacts& segment, uint64_t local_wal_identity, uint64_t vouched_epoch) {
    if (segment.wal_identity == 0 || !segment.range_is_rows || !segment.current_format) return false;
    return segment.wal_identity != local_wal_identity || segment.seal_epoch <= vouched_epoch;
}

bool settled(uint64_t period_end_ns, uint64_t wall_now_ns, std::chrono::nanoseconds since_added) {
    return until_settled(period_end_ns, wall_now_ns, since_added) == std::chrono::nanoseconds::zero();
}

std::chrono::nanoseconds until_settled(uint64_t period_end_ns, uint64_t wall_now_ns,
                                       std::chrono::nanoseconds since_added) {
    // In wall-clock nanoseconds, compared without an overflow for a period at the end of time.
    const uint64_t settles_at =
        period_end_ns > UINT64_MAX - settle_ns() ? UINT64_MAX : period_end_ns + settle_ns();
    const std::chrono::nanoseconds until_over =
        wall_now_ns >= settles_at
            ? std::chrono::nanoseconds::zero()
            : std::chrono::nanoseconds(static_cast<int64_t>(settles_at - wall_now_ns));
    const std::chrono::nanoseconds until_quiet =
        since_added >= std::chrono::nanoseconds(kSettle) ? std::chrono::nanoseconds::zero()
                                                         : std::chrono::nanoseconds(kSettle) - since_added;
    return std::max(until_over, until_quiet);
}

std::vector<Run> plan(const std::vector<Candidate>& segments, bool settled) {
    std::vector<Run> runs;
    const size_t n = segments.size();
    size_t i = 0;
    while (i < n) {
        if (!mergeable(segments[i])) {
            ++i;
            continue;
        }
        // A stretch of mergeable segments of one WAL - and, until the partition settles, of one
        // level: the unit the fan-in counts.
        size_t j = i + 1;
        while (j < n && mergeable(segments[j]) &&
               segments[j].wal_identity == segments[i].wal_identity &&
               (settled || segments[j].level == segments[i].level)) {
            ++j;
        }
        size_t k = i;
        while (k < j) {
            if (!settled && j - k < kFanIn) break;
            // Never past the stretch: the break above keeps k + kFanIn inside it before a partition
            // settles, and a bound that rests on a condition several lines away is one edit from a
            // read past the segments (a mutation of that condition found it).
            const size_t limit = std::min(j, k + (settled ? kMaxInputs : kFanIn));
            uint64_t rows = 0;
            size_t m = k;
            while (m < limit && rows + segments[m].rows <= kMaxRows) {
                rows += segments[m].rows;
                ++m;
            }
            // A settled run gives up its last inputs until it sorts where they were; a run of a
            // level is taken whole or not at all, the fan-in being what it waits for.
            while (settled && m - k > 2 && !sorts_where_it_was(segments, k, m - k)) --m;
            if (m - k >= 2 && sorts_where_it_was(segments, k, m - k)) {
                runs.push_back(Run{k, m - k});
                k = m;
            } else {
                // One that fits with nothing after it, or a run whose range would move: the first
                // stays, and the next may start a run.
                k = k + 1;
            }
        }
        i = j;
    }
    return runs;
}

}  // namespace ob::compaction
