#include "orderbook/sequence_tracker.hpp"

#include "orderbook/logger.hpp"

#include <algorithm>

namespace ob {

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

    auto it = st.origin_high_water.find(origin);
    if (it == st.origin_high_water.end()) {
        // First record from this origin. Not a gap: there is nothing to be one past.
        st.origin_high_water.emplace(origin, d.sequence_number);
        OB_LOG_DEBUG("sequence", "First record from origin: key=%s origin=%u seq=%llu",
                     key.c_str(), static_cast<unsigned>(origin),
                     static_cast<unsigned long long>(d.sequence_number));
        return d;
    }

    const uint64_t expected = it->second + 1;
    if (d.sequence_number != expected) {
        d.gap      = true;
        d.expected = expected;
        OB_LOG_WARN("sequence", "Gap: key=%s origin=%u expected=%llu got=%llu",
                    key.c_str(), static_cast<unsigned>(origin),
                    static_cast<unsigned long long>(expected),
                    static_cast<unsigned long long>(d.sequence_number));
    }

    // Advance on the highest seen, so a late record arriving out of order does not drag the
    // high-water mark backwards and report every later record as a gap.
    it->second = std::max(it->second, d.sequence_number);
    return d;
}

void SequenceTracker::seed(const std::string& key, uint16_t origin, uint64_t seq) {
    if (seq == 0) return;          // a record from before numbers existed says nothing
    SymbolState& st = symbols_[key];
    st.next_local = std::max(st.next_local, seq + 1);
    uint64_t& hw  = st.origin_high_water[origin];
    hw = std::max(hw, seq);
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
    auto oit = it->second.origin_high_water.find(origin);
    return oit == it->second.origin_high_water.end() ? 0 : oit->second;
}

}  // namespace ob
