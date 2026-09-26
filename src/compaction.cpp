#include "orderbook/compaction.hpp"

namespace ob::compaction {

namespace {

bool mergeable(const Candidate& c) {
    return c.member && c.eligible && c.rows < kFullRows;
}

}  // namespace

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
            const size_t limit = settled ? j : k + kFanIn;
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
