#include "orderbook/version_vector.hpp"

#include "orderbook/logger.hpp"

#include <algorithm>
#include <cstring>

namespace ob {

std::vector<uint8_t> serialize_version_vector(
        const std::vector<SequenceTracker::VectorEntry>& entries, bool truncated) {
    std::vector<uint8_t> out;

    if (truncated || entries.size() >= VV_TRUNCATED) {
        out.resize(VV_HEADER_SIZE);
        const uint16_t marker = VV_TRUNCATED;
        std::memcpy(out.data(), &marker, sizeof(marker));
        return out;
    }

    const uint16_t count = static_cast<uint16_t>(entries.size());
    out.resize(VV_HEADER_SIZE + static_cast<size_t>(count) * VV_ENTRY_SIZE, 0);
    std::memcpy(out.data(), &count, sizeof(count));

    size_t off = VV_HEADER_SIZE;
    for (const auto& e : entries) {
        // char[32] holds "SYMBOL.EXCHANGE": both fields are char[16] in DeltaUpdate, so the
        // joined key is at most 31 characters plus a terminator.
        std::memcpy(out.data() + off, e.key.data(), std::min<size_t>(e.key.size(), 31));
        off += 32;
        std::memcpy(out.data() + off, &e.origin, sizeof(e.origin));
        off += sizeof(e.origin);
        std::memcpy(out.data() + off, &e.frontier, sizeof(e.frontier));
        off += sizeof(e.frontier);
    }
    return out;
}

std::vector<uint8_t> serialize_held_ranges(
        const std::vector<SequenceTracker::HeldRanges>& entries) {
    if (entries.empty()) return {};

    size_t total = HS_HEADER_SIZE;
    for (const auto& e : entries) {
        total += HS_ENTRY_HEADER_SIZE + e.ranges.size() * HS_RANGE_SIZE;
    }

    std::vector<uint8_t> out(total, 0);
    const uint16_t count = static_cast<uint16_t>(entries.size());
    std::memcpy(out.data(), &count, sizeof(count));

    size_t off = HS_HEADER_SIZE;
    for (const auto& e : entries) {
        std::memcpy(out.data() + off, e.key.data(), std::min<size_t>(e.key.size(), 31));
        off += 32;
        std::memcpy(out.data() + off, &e.origin, sizeof(e.origin));
        off += sizeof(e.origin);
        const uint16_t range_count = static_cast<uint16_t>(e.ranges.size());
        std::memcpy(out.data() + off, &range_count, sizeof(range_count));
        off += sizeof(range_count);
        for (const auto& [first, last] : e.ranges) {
            std::memcpy(out.data() + off, &first, sizeof(first));
            off += sizeof(first);
            std::memcpy(out.data() + off, &last, sizeof(last));
            off += sizeof(last);
        }
    }
    return out;
}

bool deserialize_held_ranges(const uint8_t* data, size_t len,
                            std::vector<SequenceTracker::HeldRanges>& out) {
    out.clear();
    if (!data || len < HS_HEADER_SIZE) return false;

    uint16_t count = 0;
    std::memcpy(&count, data, sizeof(count));

    size_t off = HS_HEADER_SIZE;
    out.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        if (off + HS_ENTRY_HEADER_SIZE > len) { out.clear(); return false; }

        SequenceTracker::HeldRanges entry;
        const char* key_bytes = reinterpret_cast<const char*>(data + off);
        // The key was written into a fixed 32-byte field and zero-padded, so stop at the first
        // NUL rather than trusting the whole field to be text.
        entry.key.assign(key_bytes, std::find(key_bytes, key_bytes + 32, '\0'));
        off += 32;
        std::memcpy(&entry.origin, data + off, sizeof(entry.origin));
        off += sizeof(entry.origin);
        uint16_t range_count = 0;
        std::memcpy(&range_count, data + off, sizeof(range_count));
        off += sizeof(range_count);

        if (off + static_cast<size_t>(range_count) * HS_RANGE_SIZE > len) { out.clear(); return false; }
        entry.ranges.reserve(range_count);
        for (uint16_t r = 0; r < range_count; ++r) {
            uint64_t first = 0, last = 0;
            std::memcpy(&first, data + off, sizeof(first));
            off += sizeof(first);
            std::memcpy(&last, data + off, sizeof(last));
            off += sizeof(last);
            if (last < first) { out.clear(); return false; }   // not a range
            entry.ranges.emplace_back(first, last);
        }
        out.push_back(std::move(entry));
    }
    return true;
}

bool PeerVector::deserialize(const uint8_t* data, size_t len) {
    if (len < VV_HEADER_SIZE) {
        OB_LOG_WARN("mm", "Version vector payload too short: %zu bytes", len);
        return false;
    }

    uint16_t count = 0;
    std::memcpy(&count, data, sizeof(count));

    received_  = true;
    truncated_ = (count == VV_TRUNCATED);
    entries_.clear();

    if (truncated_) {
        OB_LOG_INFO("mm", "Peer cannot state what it holds — treating as empty (send everything)");
        return true;
    }

    const size_t needed = VV_HEADER_SIZE + static_cast<size_t>(count) * VV_ENTRY_SIZE;
    if (len < needed) {
        // A short payload would silently drop entries, and a dropped entry reads as "the peer
        // has nothing there" — which over-delivers rather than loses, but it is still a
        // protocol error worth refusing.
        OB_LOG_ERROR("mm", "Version vector truncated on the wire: %zu bytes for %u entries",
                     len, count);
        received_ = false;
        return false;
    }

    entries_.reserve(count);
    size_t off = VV_HEADER_SIZE;
    for (uint16_t i = 0; i < count; ++i) {
        char key_buf[33] = {};
        std::memcpy(key_buf, data + off, 32);
        off += 32;
        uint16_t origin = 0;
        std::memcpy(&origin, data + off, sizeof(origin));
        off += sizeof(origin);
        uint64_t frontier = 0;
        std::memcpy(&frontier, data + off, sizeof(frontier));
        off += sizeof(frontier);

        entries_[Key{std::string(key_buf), origin}] = frontier;
    }

    OB_LOG_INFO("mm", "Version vector received: entries=%u", count);
    return true;
}

std::vector<SequenceTracker::VectorEntry> PeerVector::entries() const {
    std::vector<SequenceTracker::VectorEntry> out;
    out.reserve(entries_.size());
    for (const auto& [k, frontier] : entries_) {
        out.push_back(SequenceTracker::VectorEntry{k.key, k.origin, frontier});
    }
    return out;
}

uint64_t PeerVector::frontier_for(const std::string& key, uint16_t origin) const {
    auto it = entries_.find(Key{key, origin});
    return it == entries_.end() ? 0 : it->second;
}

VectorDiff compare_vectors(const std::vector<SequenceTracker::VectorEntry>& ours,
                           const PeerVector& theirs, uint16_t peer_node_id) {
    VectorDiff diff{};

    // A peer that could not state its position, or has not stated it yet, is treated as holding
    // nothing: everything we have is something it lacks. Over-stating what it needs costs
    // bandwidth; under-stating it loses data.
    const bool peer_unknown = theirs.wants_everything();

    for (const auto& e : ours) {
        const uint64_t theirs_frontier = peer_unknown ? 0 : theirs.frontier_for(e.key, e.origin);
        if (theirs_frontier < e.frontier) {
            diff.peer_lacks.push_back(VectorGap{peer_node_id, e.key, e.origin,
                                                theirs_frontier + 1, e.frontier});
        }
    }

    if (peer_unknown) {
        // Nothing to learn about our own gaps from a peer that said nothing. Reporting them as
        // zero would be a claim; leaving them out is the truth.
        return diff;
    }

    // The other direction needs the peer's entries, including keys we have never heard of: a
    // symbol only it holds is exactly the gap worth finding.
    for (const auto& e : theirs.entries()) {
        uint64_t ours_frontier = 0;
        for (const auto& o : ours) {
            if (o.origin == e.origin && o.key == e.key) {
                ours_frontier = o.frontier;
                break;
            }
        }
        if (ours_frontier < e.frontier) {
            diff.we_lack.push_back(VectorGap{peer_node_id, e.key, e.origin,
                                             ours_frontier + 1, e.frontier});
        }
    }

    return diff;
}

}  // namespace ob
