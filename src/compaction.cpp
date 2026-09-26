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
            const size_t limit = settled ? j : std::min(j, k + kFanIn);
            uint64_t rows = 0;
            size_t m = k;
            while (m < limit && rows + segments[m].rows <= kMaxRows) {
                rows += segments[m].rows;
                ++m;
            }
            if (m - k >= 2) {
                runs.push_back(Run{k, m - k});
                k = m;
            } else {
                // One that fits with nothing after it: it stays, and the next may start a run.
                k = m > k ? m : k + 1;
            }
        }
        i = j;
    }
    return runs;
}

}  // namespace ob::compaction
