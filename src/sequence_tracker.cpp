#include "orderbook/sequence_tracker.hpp"

#include "orderbook/logger.hpp"

#include <algorithm>

namespace ob {

void SequenceTracker::note_seen(OriginState& st, uint64_t seq) {
    st.high_water = std::max(st.high_water, seq);

    if (seq <= st.frontier) {
        // Already covered: a redelivery, which catch-up produces on purpose whenever it is
        // unsure. Nothing to do, and nothing to complain about.
        return;
    }

    if (seq == st.frontier + 1) {
        st.frontier = seq;
        // Drain whatever arrived early and is now contiguous. Without this, filling one hole
        // would advance the frontier by one and leave a run of already-delivered records
        // looking undelivered.
        auto it = st.above_frontier.begin();
        while (it != st.above_frontier.end() && *it == st.frontier + 1) {
            st.frontier = *it;
            it = st.above_frontier.erase(it);
        }
        return;
    }

    // Out of order. Hold it so the frontier can jump when the hole is filled, but only up to
    // a cap: holding is an optimisation, and a long outage must not grow memory without
    // bound. Dropping it means the frontier stays put and the next catch-up asks again.
    if (st.above_frontier.size() < kMaxAboveFrontier) {
        st.above_frontier.insert(seq);
    }
}

SequenceTracker::Decision SequenceTracker::observe(const std::string& key, uint16_t origin,
                                                   uint64_t sequence_number) {
    SymbolState& st = symbols_[key];

    Decision d{};
    d.sequence_number = sequence_number;

    if (sequence_number == 0) {
        d.sequence_number = st.next_local++;
        d.assigned        = true;
        OB_LOG_DEBUG("sequence", "Assigned: key=%s origin=%u seq=%llu",
                     key.c_str(), static_cast<unsigned>(origin),
                     static_cast<unsigned long long>(d.sequence_number));
    } else {
        // A number minted elsewhere still has to keep the local counter ahead of it, or a
        // node that both accepts client writes and receives a stream would hand out a
        // number already in use.
        st.next_local = std::max(st.next_local, sequence_number + 1);
    }

    if (d.assigned) {
        // A number this node minted cannot be a gap: it was assigned in order, in this
        // critical section. Checking it against the frontier would report a gap on every
        // local write whenever a remote hole is holding the frontier down — a GAP record per
        // insert, which is noise, not signal.
        note_seen(st.origins[origin], d.sequence_number);
        return d;
    }

    auto it = st.origins.find(origin);
    if (it == st.origins.end()) {
        // First record from this origin. Not a gap: there is nothing to be one past.
        OriginState fresh{};
        note_seen(fresh, d.sequence_number);
        st.origins.emplace(origin, std::move(fresh));
        OB_LOG_DEBUG("sequence", "First record from origin: key=%s origin=%u seq=%llu",
                     key.c_str(), static_cast<unsigned>(origin),
                     static_cast<unsigned long long>(d.sequence_number));
        return d;
    }

    // A gap is measured against the frontier, not the maximum. Three cases, and only the
    // third is a gap:
    //   seq <= frontier      a redelivery. Catch-up produces these on purpose whenever it is
    //                        unsure, so calling them gaps would fill the WAL with GAP records
    //                        and make the metric meaningless.
    //   seq == frontier + 1  in order, even when the maximum is higher: this is the record
    //                        that fills a known hole, which is the opposite of a gap.
    //   seq >  frontier + 1  something between the frontier and this record is missing.
    const uint64_t expected = it->second.frontier + 1;
    if (d.sequence_number > expected) {
        d.gap      = true;
        d.expected = expected;
        OB_LOG_WARN("sequence", "Gap: key=%s origin=%u expected=%llu got=%llu high_water=%llu",
                    key.c_str(), static_cast<unsigned>(origin),
                    static_cast<unsigned long long>(expected),
                    static_cast<unsigned long long>(d.sequence_number),
                    static_cast<unsigned long long>(it->second.high_water));
    }

    note_seen(it->second, d.sequence_number);
    return d;
}

void SequenceTracker::seed(const std::string& key, uint16_t origin, uint64_t seq) {
    if (seq == 0) return;          // a record from before numbers existed says nothing
    SymbolState& st = symbols_[key];
    st.next_local = std::max(st.next_local, seq + 1);
    note_seen(st.origins[origin], seq);
}

void SequenceTracker::declare_frontier(const std::string& key, uint16_t origin, uint64_t seq) {
    if (seq == 0) return;
    OriginState& st = symbols_[key].origins[origin];
    if (seq <= st.frontier) return;

    st.frontier   = seq;
    st.high_water = std::max(st.high_water, seq);
    // Anything held below the declared frontier is now covered.
    while (!st.above_frontier.empty() && *st.above_frontier.begin() <= st.frontier) {
        st.above_frontier.erase(st.above_frontier.begin());
    }
    // And anything immediately above it becomes contiguous.
    auto it = st.above_frontier.begin();
    while (it != st.above_frontier.end() && *it == st.frontier + 1) {
        st.frontier = *it;
        it = st.above_frontier.erase(it);
    }
    OB_LOG_DEBUG("sequence", "Declared frontier: key=%s origin=%u frontier=%llu",
                 key.c_str(), static_cast<unsigned>(origin),
                 static_cast<unsigned long long>(st.frontier));
}

void SequenceTracker::raise_local(const std::string& key, uint64_t seq) {
    SymbolState& st = symbols_[key];
    const uint64_t candidate = seq + 1;
    if (candidate > st.next_local) {
        OB_LOG_DEBUG("sequence", "Raised local counter: key=%s from=%llu to=%llu",
                     key.c_str(), static_cast<unsigned long long>(st.next_local),
                     static_cast<unsigned long long>(candidate));
        st.next_local = candidate;
    }
}

uint64_t SequenceTracker::peek_next_local(const std::string& key) const {
    auto it = symbols_.find(key);
    return it == symbols_.end() ? 1 : it->second.next_local;
}

uint64_t SequenceTracker::high_water(const std::string& key, uint16_t origin) const {
    auto it = symbols_.find(key);
    if (it == symbols_.end()) return 0;
    auto oit = it->second.origins.find(origin);
    return oit == it->second.origins.end() ? 0 : oit->second.high_water;
}

uint64_t SequenceTracker::frontier(const std::string& key, uint16_t origin) const {
    auto it = symbols_.find(key);
    if (it == symbols_.end()) return 0;
    auto oit = it->second.origins.find(origin);
    return oit == it->second.origins.end() ? 0 : oit->second.frontier;
}

bool SequenceTracker::has_seen(const std::string& key, uint16_t origin, uint64_t seq) const {
    if (seq == 0) return false;          // unassigned: nothing to have seen
    auto it = symbols_.find(key);
    if (it == symbols_.end()) return false;
    auto oit = it->second.origins.find(origin);
    if (oit == it->second.origins.end()) return false;

    const OriginState& st = oit->second;
    if (seq <= st.frontier) return true;
    // Above the frontier, only what is actually held counts as seen. Anything dropped by the
    // kMaxAboveFrontier cap reads as unseen, so it gets applied again — a duplicate row is
    // the price of a bounded set, and it is bounded on purpose.
    return st.above_frontier.count(seq) > 0;
}

std::size_t SequenceTracker::above_frontier_size(const std::string& key, uint16_t origin) const {
    auto it = symbols_.find(key);
    if (it == symbols_.end()) return 0;
    auto oit = it->second.origins.find(origin);
    return oit == it->second.origins.end() ? 0 : oit->second.above_frontier.size();
}

void SequenceTracker::import_own_vector(const std::vector<VectorEntry>& entries) {
    for (const auto& e : entries) {
        declare_frontier(e.key, e.origin, e.frontier);
    }
    OB_LOG_INFO("sequence", "Restored own version vector: entries=%zu symbols=%zu",
                entries.size(), symbols_.size());
}

void SequenceTracker::reset() {
    const std::size_t had = symbols_.size();
    symbols_.clear();
    OB_LOG_INFO("sequence", "Reset: dropped state for %zu symbols", had);
}

uint64_t SequenceTracker::fingerprint() const {
    // Order-independent on purpose: unordered_map iteration order is not stable, and this
    // only has to answer "did anything move".
    uint64_t fp = 0;
    for (const auto& [key, st] : symbols_) {
        for (const auto& [origin, ost] : st.origins) {
            fp += ost.frontier * 1000003ULL + origin;
            // The held set is persisted too, and it changes while the frontier stands still —
            // that is its whole purpose. Without mixing it in, a node that received nothing but
            // out-of-order records would never write any of them down.
            if (!ost.above_frontier.empty()) {
                fp += ost.above_frontier.size() * 31ULL +
                      *ost.above_frontier.begin() * 7ULL +
                      *ost.above_frontier.rbegin() * 13ULL;
            }
        }
    }
    return fp;
}

std::vector<SequenceTracker::HeldRanges> SequenceTracker::export_held(std::size_t max_ranges,
                                                                     bool& truncated) const {
    truncated = false;
    std::vector<HeldRanges> out;
    std::size_t budget = max_ranges;

    for (const auto& [key, st] : symbols_) {
        for (const auto& [origin, ost] : st.origins) {
            if (ost.above_frontier.empty()) continue;

            HeldRanges entry;
            entry.key    = key;
            entry.origin = origin;

            // Collapse consecutive numbers into one range as we go: the set is ordered, so this
            // is a single pass and no intermediate list of numbers is built.
            uint64_t first = 0, last = 0;
            bool open_range = false;
            for (uint64_t seq : ost.above_frontier) {
                if (open_range && seq == last + 1) {
                    last = seq;
                    continue;
                }
                if (open_range) entry.ranges.emplace_back(first, last);
                first = last = seq;
                open_range = true;
            }
            if (open_range) entry.ranges.emplace_back(first, last);

            if (entry.ranges.size() > budget) {
                // Keep what fits rather than dropping the entry: a partial held set is still
                // fewer duplicates than none.
                entry.ranges.resize(budget);
                truncated = true;
            }
            budget -= entry.ranges.size();
            if (!entry.ranges.empty()) out.push_back(std::move(entry));
            if (budget == 0) {
                // Anything not visited yet is dropped, and the caller must be told.
                truncated = truncated || out.size() < symbols_.size();
                return out;
            }
        }
    }
    return out;
}

void SequenceTracker::import_held(const std::vector<HeldRanges>& held) {
    for (const auto& entry : held) {
        auto& st  = symbols_[entry.key];
        auto& ost = st.origins[entry.origin];
        for (const auto& [first, last] : entry.ranges) {
            for (uint64_t seq = first; seq <= last; ++seq) {
                // note_seen() also advances the frontier when a range turns out to close a hole,
                // which is correct: if the gap below was filled in a previous run and recorded in
                // the frontier, these numbers now sit right above it.
                note_seen(ost, seq);
            }
        }
    }
}

std::vector<SequenceTracker::VectorEntry> SequenceTracker::export_vector(std::size_t limit,
                                                                        bool& truncated) const {
    truncated = false;
    std::vector<VectorEntry> out;

    // One pass, and it counts only what would actually go on the wire. A separate counting
    // pass over every (symbol, origin) pair overstated the size, because an origin whose
    // frontier is still 0 is skipped below — a node tracking many origins it has no
    // contiguous history for would have declared itself too large to state its position while
    // the real vector fit comfortably.
    for (const auto& [key, st] : symbols_) {
        for (const auto& [origin, ost] : st.origins) {
            if (ost.frontier == 0) continue;   // nothing held; the default already says so

            if (out.size() >= limit) {
                // Say "I cannot state what I have" rather than state part of it: a partial
                // vector looks complete to the sender, and the entries left out would never
                // be asked for. An empty vector means "send everything" — bandwidth, not data.
                truncated = true;
                out.clear();
                OB_LOG_WARN("sequence",
                            "Version vector exceeds %zu entries — asking for everything instead",
                            limit);
                return out;
            }
            out.push_back(VectorEntry{key, origin, ost.frontier});
        }
    }
    return out;
}

}  // namespace ob
