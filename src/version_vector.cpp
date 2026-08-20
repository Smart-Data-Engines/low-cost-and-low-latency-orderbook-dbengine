#include "orderbook/version_vector.hpp"

#include "orderbook/logger.hpp"

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

}  // namespace ob
